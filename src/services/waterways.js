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

const NLDI_HOST = 'api.water.usgs.gov';
const NLDI_BASE = '/nldi/linked-data/nwissite';

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

/**
 * Get the NHDPlus comid string for a USGS site.
 * Result is cached in memory for the lifetime of the process.
 */
async function getSiteComid(siteId) {
  if (_comidCache[siteId]) return _comidCache[siteId];
  const data = await httpsGet(NLDI_HOST, `${NLDI_BASE}/USGS-${siteId}`);
  const comid = data?.features?.[0]?.properties?.comid;
  if (comid) {
    _comidCache[siteId] = String(comid);
    return _comidCache[siteId];
  }
  return null;
}

// ── Trim flowline features at a target comid ──────────────────────────────────

/**
 * Given an ordered array of NLDI flowline features and a target comid,
 * return only the features up to and including the one matching targetComid.
 * If targetComid is not found, return the full array (fall through to caller).
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
async function fetchFlowlinePath(fromSiteId, toSiteId) {
  const distanceKm = 999;

  // Fetch comids for both sites (cached after first call)
  const [fromComid, toComid] = await Promise.all([
    getSiteComid(fromSiteId),
    getSiteComid(toSiteId),
  ]);

  console.log(`[Waterways] comids: ${fromSiteId}=${fromComid||'?'} ${toSiteId}=${toComid||'?'}`);

  // ── Try DM from upstream site, trimmed at downstream site's comid ──────────
  const dmPath = `${NLDI_BASE}/USGS-${fromSiteId}/navigation/DM/flowlines?distance=${distanceKm}`;
  const dmData = await httpsGet(NLDI_HOST, dmPath);

  if (dmData?.features?.length) {
    const { trimmed, found } = trimAtComid(dmData.features, toComid);
    if (found) {
      const coords = extractCoords(trimmed);
      if (coords.length >= 2) {
        console.log(`[Waterways] DM ${fromSiteId}→${toSiteId}: ${trimmed.length} flowlines (of ${dmData.features.length}), ${coords.length} pts`);
        return coords;
      }
    }
    // toComid not in DM results — try without trimming as a last resort only if
    // the full DM path is short (direct connection, no intermediate sites)
    if (dmData.features.length <= 5) {
      const coords = extractCoords(dmData.features);
      if (coords.length >= 2) {
        console.log(`[Waterways] DM ${fromSiteId}→${toSiteId}: short path (${dmData.features.length} flowlines), using as-is`);
        return coords;
      }
    }
  }

  // ── Fallback: UM from downstream site, trimmed at upstream site's comid ────
  const umPath = `${NLDI_BASE}/USGS-${toSiteId}/navigation/UM/flowlines?distance=${distanceKm}`;
  const umData = await httpsGet(NLDI_HOST, umPath);

  if (umData?.features?.length) {
    const { trimmed, found } = trimAtComid(umData.features, fromComid);
    const features = found ? trimmed : umData.features;
    const coords = extractCoords(features);
    // UM goes upstream → reverse to get from→to direction
    coords.reverse();
    if (coords.length >= 2) {
      console.log(`[Waterways] UM ${fromSiteId}→${toSiteId}: ${features.length} flowlines (trimmed=${found}), ${coords.length} pts`);
      return coords;
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
    const coords = await fetchFlowlinePath(pair.from_site_id, pair.to_site_id);

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
