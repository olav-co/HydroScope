'use strict';

const express = require('express');
const router  = express.Router();
const https   = require('https');
const http    = require('http');
const db      = require('../../db/database');

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

// Fetch HUC8 record (code + name) for a lat/lon from USGS WBD.
async function lookupHuc8ByPoint(lat, lon) {
  const url = `${WBD_BASE}/${HUC8_LAYER}/query` +
    `?geometry=${encodeURIComponent(`${lon},${lat}`)}` +
    `&geometryType=esriGeometryPoint` +
    `&inSR=4326` +
    `&spatialRel=esriSpatialRelIntersects` +
    `&outFields=huc8%2Cname` +
    `&returnGeometry=false` +
    `&f=json`;
  const data = await fetchJson(url);
  if (data.error) throw new Error(`WBD error ${data.error.code}: ${data.error.message}`);
  const feat = (data.features || [])[0];
  if (!feat) return null;
  return { huc8_code: feat.attributes.huc8, huc8_name: feat.attributes.name };
}

// Fetch the polygon GeoJSON for a HUC8 code from USGS WBD.
async function fetchHuc8Polygon(huc8Code) {
  const url = `${WBD_BASE}/${HUC8_LAYER}/query` +
    `?where=${encodeURIComponent(`huc8='${huc8Code}'`)}` +
    `&outFields=huc8%2Cname` +
    `&returnGeometry=true` +
    `&f=geojson`;
  const data = await fetchJson(url, 30000);
  const feat = (data.features || [])[0];
  if (!feat) return null;
  return feat; // GeoJSON Feature
}

// ── GET /api/basins ───────────────────────────────────────────────────────────
// Returns distinct HUC8 units from sites already in the DB.
router.get('/', (req, res) => {
  try {
    res.json(db.getDistinctBasins());
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
    // Serve from cache first
    const cached = db.getCachedBasinPolygon(code);
    if (cached && cached.polygon_json) {
      return res.json({ ok: true, feature: JSON.parse(cached.polygon_json), cached: true });
    }

    const feature = await fetchHuc8Polygon(code);
    if (!feature) return res.status(404).json({ error: 'HUC8 not found' });

    const name = feature.properties?.name || code;
    db.upsertBasinPolygon({ huc8_code: code, huc8_name: name, polygon_json: JSON.stringify(feature) });
    res.json({ ok: true, feature, cached: false });
  } catch (err) {
    console.error('[Basins] polygon fetch error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/basins/lookup ────────────────────────────────────────────────────
// Lookup HUC8 for a lat/lon.  ?lat=&lon=
router.get('/lookup', async (req, res) => {
  const lat = parseFloat(req.query.lat);
  const lon = parseFloat(req.query.lon);
  if (isNaN(lat) || isNaN(lon)) return res.status(400).json({ error: 'lat and lon required' });
  try {
    const result = await lookupHuc8ByPoint(lat, lon);
    if (!result) return res.status(404).json({ error: 'No HUC8 found for that point' });
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
  res.json({ ok: true, queued: sites.length });

  // Fire and forget
  (async () => {
    let done = 0, failed = 0;
    for (const site of sites) {
      try {
        const huc = await lookupHuc8ByPoint(site.latitude, site.longitude);
        if (huc) {
          db.updateSiteHuc(site.site_id, huc.huc8_code, huc.huc8_name);
          done++;
        } else {
          failed++;
        }
      } catch (e) {
        console.warn(`[Basins] basin sync failed for ${site.site_id}:`, e.message);
        failed++;
      }
      // Polite delay to avoid hammering WBD
      await new Promise(r => setTimeout(r, 150));
    }
    console.log(`[Basins] basin sync complete — ${done} updated, ${failed} failed`);
  })();
});

module.exports = { router, lookupHuc8ByPoint };
