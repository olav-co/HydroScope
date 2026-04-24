'use strict';

const express = require('express');
const router  = express.Router();
const https   = require('https');
const http    = require('http');
const db      = require('../../db/database');
const { getProvider } = require('../../services/ai/index');

const WBD_BASE = 'https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer';
// Layer 4 = HUC8  (2 = HUC4, 6 = HUC10, 8 = HUC12)
const HUC8_LAYER = 4;

function fetchJson(url, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, { headers: { Accept: 'application/json' } }, res => {
      let data = '';
      res.on('data', d => { data += d; });
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode} from ${url.slice(0, 80)}`));
          return;
        }
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error(`JSON parse: ${e.message}`)); }
      });
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => { req.destroy(); reject(new Error('Timeout')); });
  });
}

// ── Local point-in-polygon ────────────────────────────────────────────────────
// Ray-casting algorithm on a single GeoJSON ring.
function pointInRing(lon, lat, ring) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i];
    const [xj, yj] = ring[j];
    if (((yi > lat) !== (yj > lat)) && lon < (xj - xi) * (lat - yi) / (yj - yi) + xi) {
      inside = !inside;
    }
  }
  return inside;
}

function pointInGeoJSON(lon, lat, geometry) {
  if (!geometry) return false;
  if (geometry.type === 'Polygon') {
    // Outer ring must contain point; holes (inner rings) must not
    return pointInRing(lon, lat, geometry.coordinates[0]) &&
      !geometry.coordinates.slice(1).some(hole => pointInRing(lon, lat, hole));
  }
  if (geometry.type === 'MultiPolygon') {
    return geometry.coordinates.some(poly =>
      pointInRing(lon, lat, poly[0]) &&
      !poly.slice(1).some(hole => pointInRing(lon, lat, hole))
    );
  }
  return false;
}

// Look up which HUC8 basin contains a point — fully local, no remote calls.
// Requires the basin catalog to have been synced (Settings → Basin Catalog).
function lookupHuc8ByPoint(lat, lon) {
  const candidates = db.getBasinCandidatesForPoint(lon, lat);
  if (!candidates.length) {
    // No bbox match at all — either catalog not synced, or point outside CONUS
    const total = db.getBasinCount().total;
    if (total === 0) {
      console.warn('[Basins] Basin catalog is empty — run the full sync from Settings first.');
    }
    return null;
  }
  for (const basin of candidates) {
    try {
      const feature = JSON.parse(basin.polygon_json);
      if (pointInGeoJSON(lon, lat, feature.geometry)) {
        return { huc8_code: basin.huc8_code, huc8_name: basin.huc8_name };
      }
    } catch (_) {}
  }
  return null; // point in bbox of a basin but not actually inside any polygon (rare edge case)
}


// ── GET /api/basins ───────────────────────────────────────────────────────────
// Returns distinct HUC8 units that have active sites, with is_favorite per user.
// Used by dashboard, flow network, and other pages that filter by active-site basins.
router.get('/', (req, res) => {
  try {
    const userId = req.user ? req.user.id : null;
    res.json(db.getDistinctBasinsForUser(userId));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/basins/all ───────────────────────────────────────────────────────
// Returns all HUC8 entries cached in the basins table (not limited to active sites).
router.get('/all', (req, res) => {
  try {
    res.json(db.getAllCachedBasins());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const NOTABLE_NAMES = [
  'Columbia', 'Willamette', 'Mississippi', 'Colorado', 'Missouri', 'Tennessee',
];

// ── GET /api/basins/notable ───────────────────────────────────────────────────
// Returns a handful of recognisable basins from the local DB to seed the dropdown
// at national zoom.  No remote calls.
router.get('/notable', (req, res) => {
  try {
    res.json(db.getNotableBasinsLocal(NOTABLE_NAMES));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/basins/in-bbox ───────────────────────────────────────────────────
// Returns up to 20 HUC8 basins whose polygons intersect the given bounding box.
// ?minLon=&minLat=&maxLon=&maxLat=
router.get('/in-bbox', (req, res) => {
  const { minLon, minLat, maxLon, maxLat } = req.query;
  const coords = [minLon, minLat, maxLon, maxLat].map(Number);
  if (coords.some(isNaN)) return res.status(400).json({ error: 'minLon/minLat/maxLon/maxLat required' });
  try {
    res.json(db.getBasinsInBbox(...coords));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/basins/search?q= ─────────────────────────────────────────────────
// Searches the local basins table by name or HUC8 code prefix.
// No remote calls — requires the catalog full-sync to have been run.
router.get('/search', (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q) return res.json([]);
  try {
    res.json(db.searchBasinsLocal(q));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/basins/:huc8/polygon ─────────────────────────────────────────────
// Returns (and caches) the GeoJSON Feature polygon for a HUC8 code.
router.get('/:huc8/polygon', async (req, res) => {
  const code = req.params.huc8;
  if (!/^\d{8}$/.test(code)) return res.status(400).json({ error: 'huc8 must be 8 digits' });

  try {
    const row = db.getCachedBasinPolygon(code);
    if (row && row.polygon_json) {
      return res.json({ ok: true, feature: JSON.parse(row.polygon_json) });
    }
    // Polygon not in local DB — user needs to run the full sync from Settings.
    res.status(404).json({ error: 'Basin polygon not in local database. Run the full sync from Settings → Basin Catalog to download all basin boundaries.' });
  } catch (err) {
    console.error('[Basins] polygon error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/basins/lookup ────────────────────────────────────────────────────
// Lookup HUC8 for a lat/lon.  ?lat=&lon=
router.get('/lookup', (req, res) => {
  const lat = parseFloat(req.query.lat);
  const lon = parseFloat(req.query.lon);
  if (isNaN(lat) || isNaN(lon)) return res.status(400).json({ error: 'lat and lon required' });
  try {
    const result = lookupHuc8ByPoint(lat, lon);
    if (!result) return res.status(404).json({ error: 'No basin found for that point. Make sure the basin catalog has been downloaded.' });
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/basins/sync ─────────────────────────────────────────────────────
// Resolve HUC8 for all sites missing basin geometry.
// Runs async — responds immediately with count, logs progress.
router.post('/sync', (req, res) => {
  const sites = db.getSitesMissingHuc();
  let done = 0, failed = 0;
  for (const site of sites) {
    const huc = lookupHuc8ByPoint(site.latitude, site.longitude);
    if (huc) { db.updateSiteHuc(site.site_id, huc.huc8_code, huc.huc8_name); done++; }
    else failed++;
  }
  console.log(`[Basins] basin sync complete — ${done} updated, ${failed} not found`);
  res.json({ ok: true, updated: done, not_found: failed });
});

// ── POST /api/basins/recommendations ─────────────────────────────────────────
// Local HUC-hierarchy suggestions.
// Body: { active: [...], exclude: [...] }
router.post('/recommendations', (req, res) => {
  const active  = Array.isArray(req.body.active)  ? req.body.active  : (req.body.active  || '').split(',').map(s => s.trim()).filter(Boolean);
  const exclude = Array.isArray(req.body.exclude) ? req.body.exclude : (req.body.exclude || '').split(',').map(s => s.trim()).filter(Boolean);
  try {
    res.json(db.getBasinRecommendations(active, exclude));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/basins/ai-suggestions ──────────────────────────────────────────
// AI-picked basin suggestions. Cached server-side per active-basin fingerprint.
// Body: { active: [...], exclude: [...], viewport: "..." }
const _aiSuggestCache = new Map(); // fingerprint → { basins, ts }
const AI_CACHE_TTL_MS = 15 * 60 * 1000; // 15 minutes

router.post('/ai-suggestions', async (req, res) => {
  const active   = Array.isArray(req.body.active)  ? req.body.active  : (req.body.active  || '').split(',').map(s => s.trim()).filter(Boolean);
  const exclude  = new Set(Array.isArray(req.body.exclude) ? req.body.exclude : (req.body.exclude || '').split(',').map(s => s.trim()).filter(Boolean));
  const viewport = (req.body.viewport || '').trim();

  if (!active.length) return res.json([]);

  const fingerprint = active.slice().sort().join(',');
  const cached = _aiSuggestCache.get(fingerprint);
  if (cached && Date.now() - cached.ts < AI_CACHE_TTL_MS) {
    return res.json(cached.basins.filter(b => !exclude.has(b.huc8_code)));
  }

  try {
    const provider    = getProvider();
    const activeNames = db.getBasinsByCodes(active).map(b => `${b.huc8_name} (${b.huc8_code})`).join(', ');
    // Give the AI a pool of candidates from the same broad region to pick from
    const candidates  = db.getBasinRecommendations(active, [...active], 40)
      .map(b => `${b.huc8_code}: ${b.huc8_name}`).join('\n');

    const systemPrompt = `You are a hydrology expert helping a user discover relevant watersheds.
Return ONLY a JSON array of HUC8 codes (8-digit strings). No explanation, no markdown.`;
    const userPrompt   = `The user is actively monitoring these HUC8 watersheds: ${activeNames}.
${viewport ? `They are currently viewing the map around: ${viewport}.` : ''}

From the candidate watersheds below, pick the 4 most interesting or relevant ones to suggest — consider hydrological connectivity, shared river systems, or regional significance.

Candidates:
${candidates}

Return a JSON array of exactly 4 HUC8 codes, e.g.: ["14166000","14167000","14168000","14169000"]`;

    const result = await provider.generateJSON(systemPrompt, userPrompt);
    const codes  = Array.isArray(result) ? result.filter(c => /^\d{8}$/.test(c)) : [];
    const basins = db.getBasinsByCodes(codes);

    _aiSuggestCache.set(fingerprint, { basins, ts: Date.now() });
    res.json(basins.filter(b => !exclude.has(b.huc8_code)));
  } catch (err) {
    console.warn('[Basins] AI suggestions failed:', err.message);
    res.json([]); // graceful degradation — caller shows local recs instead
  }
});

// ── Full catalog sync ─────────────────────────────────────────────────────────
let _fullSyncRunning = false;
let _fullSyncProgress = { saved: 0, done: false, error: null };

// Extract axis-aligned bounding box from an ESRI polygon geometry (rings format, outSR=4326).
// Also computes centroid as a fallback for legacy queries.
function bboxFromEsriRings(geometry) {
  if (!geometry || !geometry.rings) return null;
  let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity;
  let sumX = 0, sumY = 0, n = 0;
  for (const ring of geometry.rings) {
    for (const [x, y] of ring) {
      if (x < minLon) minLon = x; if (x > maxLon) maxLon = x;
      if (y < minLat) minLat = y; if (y > maxLat) maxLat = y;
      sumX += x; sumY += y; n++;
    }
  }
  if (minLon === Infinity) return null;
  return {
    bbox_minlon: minLon, bbox_minlat: minLat, bbox_maxlon: maxLon, bbox_maxlat: maxLat,
    centroid_lat: n ? sumY / n : null,
    centroid_lon: n ? sumX / n : null,
  };
}

// Extract bbox from a GeoJSON geometry (used when on-demand polygon is fetched as geojson).
function bboxFromGeoJSON(geometry) {
  if (!geometry) return null;
  let minLon = Infinity, minLat = Infinity, maxLon = -Infinity, maxLat = -Infinity;
  let sumX = 0, sumY = 0, n = 0;
  function processRing(ring) {
    for (const [lon, lat] of ring) {
      if (lon < minLon) minLon = lon; if (lon > maxLon) maxLon = lon;
      if (lat < minLat) minLat = lat; if (lat > maxLat) maxLat = lat;
      sumX += lon; sumY += lat; n++;
    }
  }
  if (geometry.type === 'Polygon')      geometry.coordinates.forEach(processRing);
  else if (geometry.type === 'MultiPolygon') geometry.coordinates.forEach(p => p.forEach(processRing));
  if (minLon === Infinity) return null;
  return { bbox_minlon: minLon, bbox_minlat: minLat, bbox_maxlon: maxLon, bbox_maxlat: maxLat };
}

async function runFullCatalogSync() {
  // Pages are smaller because we carry simplified polygon geometry per row.
  // maxAllowableOffset=0.01° (~1km) gives clean visual polygons at regional zoom
  // without the massive byte cost of full survey-grade boundaries.
  const PAGE = 200;
  let offset = 0;
  let saved  = 0;
  _fullSyncProgress = { saved: 0, done: false, error: null };

  while (true) {
    const url = `${WBD_BASE}/${HUC8_LAYER}/query` +
      `?where=1%3D1` +
      `&outFields=huc8%2Cname` +
      `&returnGeometry=true` +
      `&maxAllowableOffset=0.01` +
      `&resultRecordCount=${PAGE}` +
      `&resultOffset=${offset}` +
      `&orderByFields=huc8+ASC` +
      `&f=geojson`;

    const data = await fetchJson(url, 90000);
    if (data.error) throw new Error(`WBD: ${data.error.message}`);

    const features = data.features || [];
    if (!features.length) break;

    const rows = features.map(f => {
      const b = bboxFromGeoJSON(f.geometry);
      return {
        huc8_code:    f.properties.huc8 || f.properties.HUC8,
        huc8_name:    f.properties.name || f.properties.Name,
        polygon_json: JSON.stringify(f),
        centroid_lat: b ? (b.bbox_minlat + b.bbox_maxlat) / 2 : null,
        centroid_lon: b ? (b.bbox_minlon + b.bbox_maxlon) / 2 : null,
        bbox_minlon:  b ? b.bbox_minlon : null,
        bbox_minlat:  b ? b.bbox_minlat : null,
        bbox_maxlon:  b ? b.bbox_maxlon : null,
        bbox_maxlat:  b ? b.bbox_maxlat : null,
      };
    });
    db.bulkUpsertBasins(rows);
    saved  += rows.length;
    offset += rows.length;
    _fullSyncProgress.saved = saved;
    console.log(`[Basins] full-sync ... ${saved} basins saved`);

    if (!data.exceededTransferLimit) break;
    await new Promise(r => setTimeout(r, 400));
  }
  return saved;
}

// ── GET /api/basins/catalog-status ───────────────────────────────────────────
router.get('/catalog-status', (req, res) => {
  const counts = db.getBasinCount();
  res.json({
    count:     counts.total,
    with_bbox: counts.with_bbox,
    running:   _fullSyncRunning,
    progress:  _fullSyncProgress,
  });
});

// ── POST /api/basins/full-sync ────────────────────────────────────────────────
// Downloads all ~2,267 HUC8 basin names+codes from USGS WBD and saves locally.
// Responds immediately; sync runs in background.
router.post('/full-sync', (req, res) => {
  if (_fullSyncRunning) return res.json({ ok: false, message: 'Already running' });
  _fullSyncRunning = true;
  res.json({ ok: true });

  runFullCatalogSync()
    .then(total => {
      console.log(`[Basins] full-sync complete — ${total} basins in local DB`);
    })
    .catch(err => {
      _fullSyncProgress.error = err.message;
      console.error('[Basins] full-sync failed:', err.message);
    })
    .finally(() => {
      _fullSyncProgress.done = true;
      _fullSyncRunning = false;
    });
});

module.exports = { router, lookupHuc8ByPoint };
