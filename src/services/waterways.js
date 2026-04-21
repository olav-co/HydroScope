/**
 * Waterway geometry via USGS NLDI (National Linked Data Index).
 *
 * For each connection pair:
 *   1. Look up the NHDPlus comid for both sites
 *   2. Fetch downstream (DM) flowlines from the upstream site
 *   3. Trim the result at the comid of the downstream site so we only
 *      keep the segment between the two nodes — not the whole river to the ocean
 *
 * API base: https://api.water.usgs.gov/nldi/
 * One pair every 4 minutes via the scheduler.
 */

'use strict';

const https = require('https');
const db    = require('../db/database');
const { fetchComidForLatLon } = require('./usgs');

const NLDI_HOST = 'api.water.usgs.gov';
const NLDI_BASE = '/nldi/linked-data/nwissite';
const NLDI_COMID_BASE = '/nldi/linked-data/comid';

let _busy = false;

// comid cache — avoids re-fetching the same site on every pair
const _comidCache = {};

// ── HTTP helper ───────────────────────────────────────────────────────────────

function httpsGet(host, path, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      { hostname: host, path, method: 'GET',
        headers: { 'User-Agent': 'HydroScope/1.0 (hydrology monitoring dashboard)' },
        timeout: timeoutMs },
      (res) => {
        if (res.statusCode === 404) { res.resume(); return resolve(null); }
        if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode)); }
        let raw = '';
        res.on('data', c => raw += c);
        res.on('end', () => {
          try { resolve(JSON.parse(raw)); }
          catch(e) { reject(new Error('JSON parse: ' + e.message)); }
        });
      }
    );
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── NLDI comid lookup ─────────────────────────────────────────────────────────

/** Detect whether a site_id is USGS (all-numeric) or CWMS (alphanumeric). */
function isUsgsId(siteId) { return /^\d+$/.test(siteId); }

/**
 * Get the NHDPlus comid string for any site (USGS or CWMS).
 * USGS: uses NLDI nwissite metadata endpoint.
 * CWMS: looks up lat/lon from the DB then snaps via NLDI position endpoint.
 * Results are cached in memory for the lifetime of the process.
 */
async function getSiteComid(siteId) {
  if (_comidCache[siteId]) return _comidCache[siteId];

  let comid = null;

  if (isUsgsId(siteId)) {
    // USGS — NLDI nwissite lookup
    const data = await httpsGet(NLDI_HOST, `${NLDI_BASE}/USGS-${siteId}`);
    const raw = data?.features?.[0]?.properties?.comid;
    if (raw) comid = String(raw);
  } else {
    // CWMS — snap lat/lon to nearest NHD comid
    const site = db.getSiteById(siteId);
    if (site?.latitude && site?.longitude) {
      comid = await fetchComidForLatLon(site.latitude, site.longitude);
    }
  }

  if (comid) {
    _comidCache[siteId] = comid;
    return comid;
  }
  return null;
}

// ── Trim flowline features ────────────────────────────────────────────────────

/**
 * Trim a flowline feature array at the first feature matching targetComid.
 * Returns { trimmed, found }.
 */
function trimAtComid(features, targetComid) {
  if (!targetComid || !features.length) return { trimmed: features, found: false };
  const target = String(targetComid);
  const idx = features.findIndex(f =>
    String(f.id) === target ||
    String(f.properties?.nhdplus_comid) === target
  );
  if (idx === -1) return { trimmed: features, found: false };
  return { trimmed: features.slice(0, idx + 1), found: true };
}

/**
 * Trim a flowline feature array at the feature whose geometry is closest to
 * (targetLat, targetLon).  Used as a fallback when the exact comid of a CWMS
 * dam is not on the main NHD navigation path (e.g. snap landed on a canal).
 *
 * Returns the trimmed feature array, or null if features have no geometry.
 */
function trimAtLatLon(features, targetLat, targetLon) {
  if (!features.length || targetLat == null || targetLon == null) return null;

  let bestIdx  = -1;
  let bestDist = Infinity;

  features.forEach((f, fi) => {
    if (!f.geometry) return;
    const coords = f.geometry.type === 'LineString'
      ? f.geometry.coordinates
      : (f.geometry.coordinates || []).flat();  // MultiLineString

    for (const [lon, lat] of coords) {
      const d = (lat - targetLat) ** 2 + (lon - targetLon) ** 2;
      if (d < bestDist) { bestDist = d; bestIdx = fi; }
    }
  });

  if (bestIdx === -1) return null;
  return features.slice(0, bestIdx + 1);
}

// ── NLDI flowline fetch ───────────────────────────────────────────────────────

/**
 * Fetch the NHDPlus flowline path between fromSiteId and toSiteId.
 *
 * Strategy:
 *   1. Look up comids for both sites
 *   2. Fetch DM (downstream main) from fromSiteId
 *   3. Trim at toSiteId's comid → gives only the segment between the two gauges
 *   4. If toSiteId's comid isn't found in DM (tributary→mainstem connection),
 *      fall back to UM (upstream main) from toSiteId, trimmed at fromSiteId's comid
 *
 * Returns [[lat,lon],...] or null.
 */
/**
 * Fetch the NHDPlus flowline path between two sites.
 * toLat/toLon and fromLat/fromLon enable geographic trimming as a fallback
 * for CWMS dams whose snap comids land on canals/tailraces rather than the
 * main river channel.
 */
async function fetchFlowlinePath(fromSiteId, toSiteId, toLat, toLon, fromLat, fromLon) {
  const distanceKm = 999;

  // Fetch comids for both sites (cached after first call)
  const [fromComid, toComid] = await Promise.all([
    getSiteComid(fromSiteId),
    getSiteComid(toSiteId),
  ]);

  console.log(`[Waterways] comids: ${fromSiteId}=${fromComid||'?'} ${toSiteId}=${toComid||'?'}`);

  // Choose navigation base: USGS nwissite when possible; comid-based for CWMS
  const fromIsUsgs = isUsgsId(fromSiteId);
  const toIsUsgs   = isUsgsId(toSiteId);

  const dmNavBase = fromIsUsgs
    ? `${NLDI_BASE}/USGS-${fromSiteId}`
    : (fromComid ? `${NLDI_COMID_BASE}/${fromComid}` : null);

  const umNavBase = toIsUsgs
    ? `${NLDI_BASE}/USGS-${toSiteId}`
    : (toComid ? `${NLDI_COMID_BASE}/${toComid}` : null);

  // ── Try DM from upstream site ─────────────────────────────────────────────
  if (dmNavBase) {
    const dmData = await httpsGet(NLDI_HOST,
      `${dmNavBase}/navigation/DM/flowlines?distance=${distanceKm}`);

    if (dmData?.features?.length) {
      // 1. Exact comid trim
      const { trimmed, found } = trimAtComid(dmData.features, toComid);
      if (found) {
        const coords = extractCoords(trimmed);
        if (coords.length >= 2) {
          console.log(`[Waterways] DM comid-trim ${fromSiteId}→${toSiteId}: ${trimmed.length}/${dmData.features.length} flowlines, ${coords.length} pts`);
          return coords;
        }
      }

      // 2. Geographic trim — for CWMS dams whose snap comid isn't on main channel
      if (!found && toLat != null && toLon != null) {
        const geoFeatures = trimAtLatLon(dmData.features, toLat, toLon);
        if (geoFeatures && geoFeatures.length >= 2) {
          const coords = extractCoords(geoFeatures);
          if (coords.length >= 2) {
            console.log(`[Waterways] DM geo-trim ${fromSiteId}→${toSiteId}: ${geoFeatures.length}/${dmData.features.length} flowlines, ${coords.length} pts`);
            return coords;
          }
        }
      }

      // 3. Short path — use as-is (direct connection, no intermediate sites)
      if (dmData.features.length <= 5) {
        const coords = extractCoords(dmData.features);
        if (coords.length >= 2) {
          console.log(`[Waterways] DM short ${fromSiteId}→${toSiteId}: ${dmData.features.length} flowlines`);
          return coords;
        }
      }
    }
  }

  // ── Fallback: UM from downstream site ────────────────────────────────────
  if (umNavBase) {
    const umData = await httpsGet(NLDI_HOST,
      `${umNavBase}/navigation/UM/flowlines?distance=${distanceKm}`);

    if (umData?.features?.length) {
      // 1. Exact comid trim
      const { trimmed: umTrimmed, found: umFound } = trimAtComid(umData.features, fromComid);

      // 2. Geographic trim fallback
      let features = umFound ? umTrimmed : null;
      if (!features && fromLat != null && fromLon != null) {
        features = trimAtLatLon(umData.features, fromLat, fromLon);
      }
      if (!features) features = umData.features;

      const coords = extractCoords(features);
      coords.reverse();  // UM goes upstream → reverse for from→to direction
      if (coords.length >= 2) {
        console.log(`[Waterways] UM ${fromSiteId}→${toSiteId}: ${features.length} flowlines (comidTrim=${umFound}), ${coords.length} pts`);
        return coords;
      }
    }
  }

  return null;
}

/**
 * Extract a single ordered coordinate array from a GeoJSON FeatureCollection
 * of NHDPlus flowline LineStrings.
 * Coordinates are [lon, lat] — we convert to [lat, lon] for Leaflet.
 */
function extractCoords(features) {
  const coords = [];
  for (const f of features) {
    if (!f.geometry) continue;
    const geom = f.geometry;
    if (geom.type === 'LineString') {
      for (const c of geom.coordinates) {
        coords.push([c[1], c[0]]);
      }
    } else if (geom.type === 'MultiLineString') {
      for (const line of geom.coordinates) {
        for (const c of line) {
          coords.push([c[1], c[0]]);
        }
      }
    }
  }
  return coords;
}

// ── scheduler-driven pair processing ─────────────────────────────────────────

async function processNextPair() {
  if (_busy) return false;

  const pending = db.getPendingWaterwayPairs();
  if (!pending.length) return false;

  const pair = pending[0];
  _busy = true;
  console.log(`[Waterways] fetching NLDI path ${pair.from_site_id} → ${pair.to_site_id}`);

  db.upsertWaterwayPath({
    from_site_id: pair.from_site_id, to_site_id: pair.to_site_id,
    path_json: null, status: 'pending'
  });

  try {
    const coords = await fetchFlowlinePath(
      pair.from_site_id, pair.to_site_id,
      pair.to_lat,   pair.to_lon,
      pair.from_lat, pair.from_lon
    );

    if (coords && coords.length >= 2) {
      db.upsertWaterwayPath({
        from_site_id: pair.from_site_id, to_site_id: pair.to_site_id,
        path_json: JSON.stringify(coords), status: 'ok'
      });
      console.log(`[Waterways] ✓ ${pair.from_site_id} → ${pair.to_site_id}: ${coords.length} pts`);
    } else {
      db.upsertWaterwayPath({
        from_site_id: pair.from_site_id, to_site_id: pair.to_site_id,
        path_json: null, status: 'none'
      });
      console.log(`[Waterways] – ${pair.from_site_id} → ${pair.to_site_id}: no NLDI path found`);
    }
  } catch(err) {
    db.upsertWaterwayPath({
      from_site_id: pair.from_site_id, to_site_id: pair.to_site_id,
      path_json: null, status: 'error'
    });
    console.error(`[Waterways] ✗ ${pair.from_site_id} → ${pair.to_site_id}: ${err.message}`);
  } finally {
    _busy = false;
  }
  return true;
}

function resetAllPairs() {
  const pairs = db.getAllWaterwayConnectionPairs();
  for (const p of pairs) {
    db.upsertWaterwayPath({
      from_site_id: p.from_site_id, to_site_id: p.to_site_id,
      path_json: null, status: 'pending'
    });
  }
  return pairs.length;
}

async function refreshAllWaterways() {
  const n = resetAllPairs();
  await processNextPair();
  return { ok: true, queued: n };
}

function isRunning() { return _busy; }

function runBackgroundCrawl() {
  processNextPair()
    .then(ran => { if (!ran) console.log('[Waterways] all pairs already cached'); })
    .catch(e  => console.error('[Waterways] startup error:', e.message));
}

module.exports = { runBackgroundCrawl, processNextPair, refreshAllWaterways, resetAllPairs, isRunning };
