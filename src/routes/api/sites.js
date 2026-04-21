'use strict';

const express = require('express');
const router  = express.Router();
const https   = require('https');
const http    = require('http');
const { getDatasourcesConfig } = require('../../services/config');
const db = require('../../db/database');

// ── Helpers ───────────────────────────────────────────────────────────────────

function fetchJson(url, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, { headers: { 'Accept': 'application/json' } }, res => {
      let data = '';
      res.on('data', d => { data += d; });
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode} from ${url.slice(0, 80)}`));
          return;
        }
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error(`JSON parse error: ${e.message}`)); }
      });
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => { req.destroy(); reject(new Error('Request timed out')); });
  });
}

function fetchText(url, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    const req = client.get(url, { headers: { 'Accept': 'text/plain' } }, res => {
      let data = '';
      res.on('data', d => { data += d; });
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) {
          reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 200).replace(/\s+/g, ' ').trim()}`));
        } else {
          resolve(data);
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs, () => { req.destroy(); reject(new Error('Request timed out')); });
  });
}

// Scan USGS RDB comment header for embedded error messages
function extractUsgsRdbError(text) {
  for (const line of text.split('\n')) {
    if (!line.startsWith('#')) break;
    const msg = line.slice(1).trim();
    if (msg && /error|no sites|invalid|exceed|limit|bad request/i.test(msg)) return msg;
  }
  return null;
}

// Split a bbox into a grid of chunks no larger than maxDeg per side
function splitBbox(minLon, minLat, maxLon, maxLat, maxDeg) {
  const cols = Math.ceil((maxLon - minLon) / maxDeg);
  const rows = Math.ceil((maxLat - minLat) / maxDeg);
  const dLon = (maxLon - minLon) / cols;
  const dLat = (maxLat - minLat) / rows;
  const chunks = [];
  for (let c = 0; c < cols; c++) {
    for (let r = 0; r < rows; r++) {
      chunks.push([
        +(minLon + c       * dLon).toFixed(5),
        +(minLat + r       * dLat).toFixed(5),
        +(minLon + (c + 1) * dLon).toFixed(5),
        +(minLat + (r + 1) * dLat).toFixed(5),
      ]);
    }
  }
  return chunks;
}

// Parse USGS RDB tab-delimited response into array of objects
function parseUsgsRdb(text) {
  const lines = text.split('\n').filter(l => l.trim() && !l.startsWith('#'));
  if (lines.length < 2) return [];
  const headers = lines[0].split('\t');
  // lines[1] is the type row (5s, 15s, etc.) — skip it
  return lines.slice(2).map(line => {
    const vals = line.split('\t');
    const obj = {};
    headers.forEach((h, i) => { obj[h.trim()] = (vals[i] || '').trim(); });
    return obj;
  }).filter(r => r.site_no);
}

function usgsRowToSite(row) {
  return {
    source:     'usgs',
    id:         row.site_no,
    name:       row.station_nm || row.site_no,
    type:       guessUsgsType(row.site_tp_cd),
    lat:        parseFloat(row.dec_lat_va) || null,
    lon:        parseFloat(row.dec_long_va) || null,
  };
}

function guessUsgsType(code) {
  if (!code) return 'river';
  const c = code.toUpperCase();
  if (c === 'LK' || c === 'ES') return 'reservoir';
  if (c === 'WE')                return 'wetland';
  if (c === 'SP')                return 'spring';
  if (c === 'AT')                return 'tidal';
  return 'river';
}

// ── GET /api/sites ────────────────────────────────────────────────────────────

router.get('/', (req, res) => {
  try {
    const sites = db.getAllSitesWithTimeseries();
    res.json(sites);
  } catch (err) {
    console.error('[Sites API] GET error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/sites ───────────────────────────────────────────────────────────

router.post('/', (req, res) => {
  try {
    const { source, id, locationId, name, type, lat, lon, timeseries, enabled } = req.body;
    const siteId = id || locationId;
    if (!source || !siteId) return res.status(400).json({ error: 'source and id are required' });

    db.upsertSite({
      source,
      site_id:   siteId,
      name:      name || siteId,
      type:      type || 'river',
      latitude:  lat  != null ? lat  : null,
      longitude: lon  != null ? lon  : null,
    });

    if (Array.isArray(timeseries)) {
      db.replaceSiteTimeseries(siteId, timeseries);
    }

    if (enabled === false) db.setSiteEnabled(siteId, false);

    const site = db.getAllSitesWithTimeseries().find(s => s.site_id === siteId);
    res.json({ ok: true, site });
  } catch (err) {
    console.error('[Sites API] POST error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /api/sites/:id ────────────────────────────────────────────────────────

router.put('/:id', (req, res) => {
  try {
    const siteId = req.params.id;
    const existing = db.getAllSitesWithTimeseries().find(s => s.site_id === siteId);
    if (!existing) return res.status(404).json({ error: 'Site not found' });

    const { name, type, lat, lon, timeseries, enabled } = req.body;

    db.upsertSite({
      source:    existing.source,
      site_id:   siteId,
      name:      name != null ? name : existing.name,
      type:      type != null ? type : existing.type,
      latitude:  lat  != null ? lat  : existing.latitude,
      longitude: lon  != null ? lon  : existing.longitude,
    });

    if (Array.isArray(timeseries)) {
      db.replaceSiteTimeseries(siteId, timeseries);
    }

    if (enabled !== undefined) db.setSiteEnabled(siteId, enabled ? 1 : 0);

    const site = db.getAllSitesWithTimeseries().find(s => s.site_id === siteId);
    res.json({ ok: true, site });
  } catch (err) {
    console.error('[Sites API] PUT error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE /api/sites/:id ─────────────────────────────────────────────────────

router.delete('/:id', (req, res) => {
  try {
    db.deleteSite(req.params.id);
    res.json({ ok: true });
  } catch (err) {
    console.error('[Sites API] DELETE error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/sites/export ─────────────────────────────────────────────────────

router.get('/export', (req, res) => {
  try {
    const sites = db.getAllSitesWithTimeseries();
    const out = sites.map(s => {
      const entry = {
        source: s.source,
        id:     s.site_id,
        name:   s.name,
        type:   s.type,
        lat:    s.latitude,
        lon:    s.longitude,
      };
      if (s.source === 'cwms') {
        entry.locationId = s.site_id;
        delete entry.id;
        if (s.timeseries && s.timeseries.length) entry.timeseries = s.timeseries;
      }
      return entry;
    });
    res.setHeader('Content-Disposition', 'attachment; filename="sites-export.json"');
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(out, null, 2));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/sites/import ────────────────────────────────────────────────────

router.post('/import', (req, res) => {
  try {
    const { sites, preview } = req.body;
    if (!Array.isArray(sites)) return res.status(400).json({ error: 'sites must be an array' });

    const results = { added: 0, skipped: 0, errors: [] };

    for (const entry of sites) {
      try {
        const siteId = entry.id || entry.locationId || entry.site_id;
        if (!siteId || !entry.source) { results.skipped++; continue; }
        if (!preview) {
          db.upsertSite({
            source:    entry.source,
            site_id:   siteId,
            name:      entry.name || siteId,
            type:      entry.type || 'river',
            latitude:  entry.lat  != null ? entry.lat  : (entry.latitude  != null ? entry.latitude  : null),
            longitude: entry.lon  != null ? entry.lon  : (entry.longitude != null ? entry.longitude : null),
          });
          if (Array.isArray(entry.timeseries)) {
            db.replaceSiteTimeseries(siteId, entry.timeseries);
          }
        }
        results.added++;
      } catch (e) {
        results.errors.push({ entry, error: e.message });
      }
    }

    res.json({ ok: true, preview: !!preview, ...results });
  } catch (err) {
    console.error('[Sites API] import error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/sites/discover/usgs ──────────────────────────────────────────────
// bbox = "minLon,minLat,maxLon,maxLat"
// USGS documents a 25° per-side limit. If the drawn box exceeds that we split
// into chunks and query sequentially (not parallel — USGS rate-limits hammering).
// Commas are kept literal in the bBox value; USGS rejects %2C.

const USGS_BASE     = 'https://waterservices.usgs.gov/nwis/site/?format=rdb&siteStatus=active';
const USGS_MAX_DEG  = 25;

async function usgsQuery(wLon, sLat, eLon, nLat) {
  const url  = `${USGS_BASE}&bBox=${wLon},${sLat},${eLon},${nLat}`;
  console.log('[USGS] GET', url);
  const text = await fetchText(url, 45000);
  const warn = extractUsgsRdbError(text);
  if (warn) console.warn('[USGS] RDB warning:', warn);
  return parseUsgsRdb(text).map(usgsRowToSite).filter(s => s.lat && s.lon);
}

router.get('/discover/usgs', async (req, res) => {
  try {
    const { bbox, id, name } = req.query;

    if (id) {
      const url   = `https://waterservices.usgs.gov/nwis/site/?format=rdb&siteStatus=all&sites=${encodeURIComponent(id)}`;
      const text  = await fetchText(url, 20000);
      const sites = parseUsgsRdb(text).map(usgsRowToSite).filter(s => s.lat && s.lon);
      return res.json({ ok: true, sites, count: sites.length });
    }
    if (name) {
      // Query each state group with no server-side name filter, then filter in our backend.
      // Avoids all USGS wildcard/encoding quirks — simple case-insensitive includes on our side.
      const STATE_GROUPS = [
        'al,ak,az,ar,ca', 'co,ct,de,fl,ga', 'hi,id,il,in,ia',
        'ks,ky,la,me,md',  'ma,mi,mn,ms,mo', 'mt,ne,nv,nh,nj',
        'nm,ny,nc,nd,oh',  'ok,or,pa,ri,sc', 'sd,tn,tx,ut,vt',
        'va,wa,wv,wi,wy,dc,pr,vi',
      ];
      const usgsBase = 'https://waterservices.usgs.gov/nwis/site/?format=rdb&siteStatus=active';
      const results  = await Promise.allSettled(STATE_GROUPS.map(states =>
        fetchText(`${usgsBase}&stateCd=${states}`, 20000)
          .then(text => parseUsgsRdb(text).map(usgsRowToSite).filter(s => s.lat && s.lon))
      ));
      const q        = name.toLowerCase();
      const seen     = new Set();
      const allSites = [];
      for (const r of results) {
        if (r.status !== 'fulfilled') continue;
        for (const s of r.value) {
          if (seen.has(s.id)) continue;
          seen.add(s.id);
          if (s.name.toLowerCase().includes(q) || s.id.includes(q)) allSites.push(s);
        }
      }
      return res.json({ ok: true, sites: allSites, count: allSites.length });
    }

    if (!bbox) return res.status(400).json({ error: 'Provide bbox, id, or name' });

    const parts = bbox.split(',').map(Number);
    if (parts.length !== 4 || parts.some(isNaN))
      return res.status(400).json({ error: 'bbox must be minLon,minLat,maxLon,maxLat' });

    const [minLon, minLat, maxLon, maxLat] = parts;
    const lonSpan = maxLon - minLon;
    const latSpan = maxLat - minLat;

    let sites = [];

    if (lonSpan <= USGS_MAX_DEG && latSpan <= USGS_MAX_DEG) {
      // Single request — fits within USGS limit
      sites = await usgsQuery(minLon, minLat, maxLon, maxLat);
    } else {
      // Split and query chunks sequentially to avoid hammering USGS
      const chunks = splitBbox(minLon, minLat, maxLon, maxLat, USGS_MAX_DEG);
      console.log(`[USGS] splitting into ${chunks.length} chunks`);
      const seen = new Set();
      for (const [wLon, sLat, eLon, nLat] of chunks) {
        try {
          const rows = await usgsQuery(wLon, sLat, eLon, nLat);
          for (const s of rows) {
            if (!seen.has(s.id)) { seen.add(s.id); sites.push(s); }
          }
        } catch (e) {
          console.warn('[USGS] chunk failed:', e.message);
        }
      }
    }

    res.json({ ok: true, sites, count: sites.length });
  } catch (err) {
    console.error('[Sites API] USGS discover error:', err.message);
    res.json({ ok: false, sites: [], count: 0, warning: err.message });
  }
});

// ── GET /api/sites/discover/cwms/bbox ─────────────────────────────────────────
// Uses the CDA catalog/LOCATIONS bounding-box parameter directly — no hardcoded
// district matching. Paginates automatically until all entries are fetched.
// Query params: bbox = "minLon,minLat,maxLon,maxLat"

router.get('/discover/cwms/bbox', async (req, res) => {
  try {
    const { bbox } = req.query;
    if (!bbox) return res.status(400).json({ error: 'bbox is required' });

    const parts = bbox.split(',').map(Number);
    if (parts.length !== 4 || parts.some(isNaN))
      return res.status(400).json({ error: 'bbox must be minLon,minLat,maxLon,maxLat' });

    const [minLon, minLat, maxLon, maxLat] = parts;

    const dsCfg = getDatasourcesConfig();
    const cwms  = dsCfg.cwms || {};
    const base  = cwms.baseUrl || 'https://cwms-data.usace.army.mil/cwms-data';

    // CDA accepts bounding-box as "minLon,minLat,maxLon,maxLat"
    const bbParam = encodeURIComponent(bbox);
    const allEntries = [];
    let nextPage = null;

    do {
      const url = `${base}/catalog/LOCATIONS?bounding-box=${bbParam}&page-size=500`
        + (nextPage ? `&page=${encodeURIComponent(nextPage)}` : '');
      const data = await fetchJson(url);
      const entries = data.entries || (Array.isArray(data) ? data : []);
      allEntries.push(...entries);
      nextPage = data['next-page'] || null;
      console.log(`[CWMS] fetched ${entries.length} entries, nextPage=${!!nextPage}`);
    } while (nextPage);

    const seen  = new Set();
    const sites = allEntries
      .filter(e => e.latitude != null && e.longitude != null)
      // CDA bounding-box param doesn't actually filter geographically — do it ourselves
      .filter(e =>
        e.latitude  >= minLat && e.latitude  <= maxLat &&
        e.longitude >= minLon && e.longitude <= maxLon
      )
      .map(e => ({
        id:     e['location-id'] || e.name,
        name:   e['location-id'] || e.name,
        office: e.office,
        type:   (e['location-kind'] || 'dam').toLowerCase(),
        lat:    e.latitude,
        lon:    e.longitude,
        source: 'cwms',
      }))
      .filter(s => s.id && !seen.has(s.id) && seen.add(s.id));

    const offices = [...new Set(sites.map(s => s.office).filter(Boolean))];
    res.json({ ok: true, sites, offices, count: sites.length });
  } catch (err) {
    console.error('[Sites API] CWMS bbox error:', err.message);
    res.json({ ok: false, sites: [], offices: [], count: 0, warning: err.message });
  }
});

// ── GET /api/sites/discover/cwms/locations ─────────────────────────────────────
// Query params:
//   office = "LRN"  (Corps district office code)
//   like   = "partial name filter" (optional)

router.get('/discover/cwms/locations', async (req, res) => {
  try {
    const dsCfg  = getDatasourcesConfig();
    const cwms   = dsCfg.cwms || {};
    const base   = cwms.baseUrl || 'https://cwms-data.usace.army.mil/cwms-data';
    const office = req.query.office || cwms.office || 'LRN';
    const like   = req.query.like || '';

    let url = `${base}/catalog/LOCATIONS?office=${encodeURIComponent(office)}&pageSize=500`;
    if (like) url += `&like=${encodeURIComponent(like)}`;

    const data = await fetchJson(url);
    res.json({ ok: true, data });
  } catch (err) {
    console.error('[Sites API] CWMS locations error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/sites/discover/cwms/timeseries ─────────────────────────────────────
// Query params:
//   office     = "LRN"
//   locationId = "CEHT1-CENTER_HILL"

router.get('/discover/cwms/timeseries', async (req, res) => {
  try {
    const dsCfg      = getDatasourcesConfig();
    const cwms       = dsCfg.cwms || {};
    const base       = cwms.baseUrl || 'https://cwms-data.usace.army.mil/cwms-data';
    const office     = req.query.office     || cwms.office || 'LRN';
    const locationId = req.query.locationId || '';

    if (!locationId) return res.status(400).json({ error: 'locationId is required' });

    const like = `${locationId}.*`;
    const url  = `${base}/catalog/TIMESERIES?office=${encodeURIComponent(office)}&like=${encodeURIComponent(like)}&pageSize=500`;

    const data = await fetchJson(url);
    res.json({ ok: true, data });
  } catch (err) {
    console.error('[Sites API] CWMS timeseries error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/sites/discover/cwms/search ───────────────────────────────────────
// Full-text search across all CWMS offices (no bbox required).
// Query params:
//   name   = search term (matched with SQL LIKE wildcards on both ends)
//   office = optional Corps district code to restrict scope
router.get('/discover/cwms/search', async (req, res) => {
  try {
    const dsCfg  = getDatasourcesConfig();
    const cwms   = dsCfg.cwms || {};
    const base   = cwms.baseUrl || 'https://cwms-data.usace.army.mil/cwms-data';
    const { name, office } = req.query;
    if (!name) return res.status(400).json({ error: 'name is required' });

    // Fetch all CWMS locations (optionally restricted to one office), then filter in our
    // backend — avoids CDA wildcard encoding issues and matches on both id and public-name.
    let url = `${base}/catalog/LOCATIONS?page-size=5000`;
    if (office) url += `&office=${encodeURIComponent(office)}`;

    const data = await fetchJson(url);
    const q    = name.toLowerCase();
    const seen = new Set();
    const sites = (data.entries || [])
      .filter(e => {
        const id    = (e['location-id'] || e.name || '').toLowerCase();
        const pname = (e['public-name'] || e['long-name'] || '').toLowerCase();
        return id.includes(q) || pname.includes(q);
      })
      .filter(e => e.latitude != null && e.longitude != null)
      .map(e => ({
        id:     e['location-id'] || e.name,
        name:   e['public-name'] || e['long-name'] || e['location-id'] || e.name,
        office: e.office,
        type:   (e['location-kind'] || 'dam').toLowerCase(),
        lat:    e.latitude,
        lon:    e.longitude,
        source: 'cwms',
      }))
      .filter(s => s.id && !seen.has(s.id) && seen.add(s.id));

    res.json({ ok: true, sites, count: sites.length });
  } catch (err) {
    console.error('[Sites API] CWMS search error:', err.message);
    res.json({ ok: false, sites: [], count: 0, warning: err.message });
  }
});

// ── GET /api/sites/discover/test ─────────────────────────────────────────────
// Browser-accessible diagnostic: ?name=det  tries multiple USGS URL formats.
router.get('/discover/test', async (req, res) => {
  const name = req.query.name || 'det';
  const out  = { name, variants: [] };

  async function tryUsgs(label, url) {
    const r = { label, url };
    try {
      const text = await fetchText(url, 15000);
      const sites = parseUsgsRdb(text).map(usgsRowToSite).filter(s => s.lat && s.lon);
      r.status = 200; r.count = sites.length;
      r.preview = text.slice(0, 300).replace(/\n/g, '\\n');
      r.sample  = sites.slice(0, 3).map(s => s.name);
    } catch (e) {
      r.status = 'error'; r.error = e.message.slice(0, 200);
    }
    out.variants.push(r);
  }

  const enc  = encodeURIComponent(name);
  const base = 'https://waterservices.usgs.gov/nwis/site/?format=rdb&siteStatus=all';

  // Variant A: stateCd=mi, no wildcards
  await tryUsgs('stateCd=mi no-wildcard',       `${base}&stateCd=mi&stationNm=${enc}`);
  // Variant B: stateCd=mi, %25 encoded wildcards
  await tryUsgs('stateCd=mi %25 wildcards',     `${base}&stateCd=mi&stationNm=%25${enc}%25`);
  // Variant C: bBox over Michigan, no wildcards
  await tryUsgs('bBox=MI no-wildcard',          `${base}&bBox=-90,41,-82,48&stationNm=${enc}`);
  // Variant D: bBox over Michigan, %25 wildcards
  await tryUsgs('bBox=MI %25 wildcards',        `${base}&bBox=-90,41,-82,48&stationNm=%25${enc}%25`);
  // Variant E: all states, no wildcards
  const ALL = 'al,ak,az,ar,ca,co,ct,de,fl,ga,hi,id,il,in,ia,ks,ky,la,me,md,ma,mi,mn,ms,mo,mt,ne,nv,nh,nj,nm,ny,nc,nd,oh,ok,or,pa,ri,sc,sd,tn,tx,ut,vt,va,wa,wv,wi,wy,dc';
  await tryUsgs('allStates no-wildcard',        `${base}&stateCd=${ALL}&stationNm=${enc}`);

  res.json(out);
});

module.exports = router;
