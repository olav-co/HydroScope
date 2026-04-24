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
    lat:        parseFloat(row.dec_lat_va)  || null,
    lon:        parseFloat(row.dec_long_va) || null,
    huc8_code:  row.huc_cd   || null,   // 8-digit HUC from USGS RDB
    huc8_name:  null,                   // name resolved separately via WBD
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

router.post('/', async (req, res) => {
  try {
    const { source, id, locationId, name, type, lat, lon, timeseries, enabled,
            huc8_code, huc8_name, office } = req.body;
    const siteId = id || locationId;
    if (!source || !siteId) return res.status(400).json({ error: 'source and id are required' });

    db.upsertSite({
      source,
      site_id:   siteId,
      name:      name || siteId,
      type:      type || 'river',
      latitude:  lat  != null ? lat  : null,
      longitude: lon  != null ? lon  : null,
      huc8_code: huc8_code || null,
      huc8_name: huc8_name || null,
      office:    office || null,
    });

    if (Array.isArray(timeseries)) db.replaceSiteTimeseries(siteId, timeseries);
    if (enabled === false) db.setSiteEnabled(siteId, false);

    // Auto-lookup HUC8 in the background if not supplied and coords are known
    if (!huc8_code && lat != null && lon != null) {
      try {
        const { lookupHuc8ByPoint } = require('./basins');
        lookupHuc8ByPoint(lat, lon).then(huc => {
          if (huc) db.updateSiteHuc(siteId, huc.huc8_code, huc.huc8_name);
        }).catch(() => {});
      } catch (_) {}
    }

    // Ensure this office's CDA aliases are cached before pairing runs.
    if (source === 'cwms' && office) {
      try {
        const { ensureOfficeAliases } = require('../../services/cwmsAliases');
        const dsCfg  = getDatasourcesConfig();
        const cwmsCfg = dsCfg.cwms || {};
        const base   = cwmsCfg.baseUrl || 'https://cwms-data.usace.army.mil/cwms-data';
        await ensureOfficeAliases(office, base);
      } catch (aliasErr) {
        console.warn('[Sites API] alias fetch error:', aliasErr.message);
      }
    }

    // Run pairing now so the combined site exists before the response is returned.
    try {
      const { detectAndPairSites } = require('../../services/pairing');
      await detectAndPairSites();
    } catch (pairErr) {
      console.error('[Sites API] pairing error after add:', pairErr.message);
    }

    const site = db.getAllSitesWithTimeseries().find(s => s.site_id === siteId);
    res.json({ ok: true, site });
  } catch (err) {
    console.error('[Sites API] POST error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /api/sites/:id ────────────────────────────────────────────────────────

router.put('/:id', async (req, res) => {
  try {
    const siteId = req.params.id;
    const existing = db.getAllSitesWithTimeseries().find(s => s.site_id === siteId);
    if (!existing) return res.status(404).json({ error: 'Site not found' });

    const { name, type, lat, lon, timeseries, enabled } = req.body;

    const coordsChanged = (lat != null && lat !== existing.latitude) ||
                          (lon != null && lon !== existing.longitude);

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

    // Coord change invalidates the snapped COMID — clear it so next pairing re-fetches.
    if (coordsChanged) {
      db.setSiteComid(siteId, null);
      try {
        const { detectAndPairSites } = require('../../services/pairing');
        await detectAndPairSites();
      } catch (pairErr) {
        console.error('[Sites API] pairing error after coord update:', pairErr.message);
      }
    }

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

// ── GET /api/sites/:id/sources — list children of a combined site ─────────────
router.get('/:id/sources', (req, res) => {
  try {
    const children = db.getSiteChildren(req.params.id);
    res.json({ ok: true, sources: children });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── PATCH /api/sites/:id/sources/:childId — toggle a source on/off ────────────
router.patch('/:id/sources/:childId', (req, res) => {
  try {
    const { enabled } = req.body;
    if (enabled === undefined) return res.status(400).json({ error: 'enabled is required' });
    db.setSiteSourceEnabled(req.params.id, req.params.childId, enabled ? 1 : 0);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/sites/discover/match ───────────────────────────────────────────
// Pure-geometry co-location match for discovered (not-yet-saved) sites.
// No NLDI calls — those happen later when a site is actually added to DB.
// A Corps dam and its tailwater USGS gauge are almost always ≤5 km apart;
// COMID-based matching isn't viable here because adjacent NHD reaches have
// different COMIDs, and firing hundreds of NLDI requests would rate-limit.
// Body: { usgs: [{id, lat, lon}], cwms: [{id, lat, lon}] }
// Response: { pairs: [{cwmsId, usgsId, reason}] }
router.post('/discover/match', (req, res) => {
  try {
    const usgs = Array.isArray(req.body.usgs) ? req.body.usgs : [];
    const cwms = Array.isArray(req.body.cwms) ? req.body.cwms : [];
    if (!usgs.length || !cwms.length) return res.json({ ok: true, pairs: [] });

    function haversineKm(lat1, lon1, lat2, lon2) {
      const R = 6371;
      const dLat = (lat2 - lat1) * Math.PI / 180;
      const dLon = (lon2 - lon1) * Math.PI / 180;
      const a = Math.sin(dLat / 2) ** 2
        + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180)
        * Math.sin(dLon / 2) ** 2;
      return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    const { buildAliasMap } = require('../../services/cwmsAliases');
    const aliasMap  = buildAliasMap(); // Map<cwmsId, usgsId> from DB
    const usgsIndex = new Map(usgs.map(u => [u.id, u]));

    const PROXIMITY_KM = 5.0;
    const pairs    = [];
    const usedUsgs = new Set();

    for (const c of cwms) {
      // Priority 1: CDA alias
      const aliasUsgsId = aliasMap.get(c.id)
        ?? aliasMap.get(c.id.replace(/-[^-]+$/, ''));
      if (aliasUsgsId && usgsIndex.has(aliasUsgsId) && !usedUsgs.has(aliasUsgsId)) {
        pairs.push({ cwmsId: c.id, usgsId: aliasUsgsId, reason: `CDA alias → ${aliasUsgsId}` });
        usedUsgs.add(aliasUsgsId);
        continue;
      }

      // Priority 2: proximity ≤5 km
      if (c.lat == null || c.lon == null) continue;
      let bestDist = PROXIMITY_KM, bestUsgs = null;
      for (const u of usgs) {
        if (usedUsgs.has(u.id) || u.lat == null || u.lon == null) continue;
        const d = haversineKm(c.lat, c.lon, u.lat, u.lon);
        if (d < bestDist) { bestDist = d; bestUsgs = u; }
      }
      if (bestUsgs) {
        pairs.push({ cwmsId: c.id, usgsId: bestUsgs.id,
                     reason: `proximity ${bestDist.toFixed(2)} km` });
        usedUsgs.add(bestUsgs.id);
      }
    }

    console.log(`[Match] ${pairs.length} pair(s) from ${cwms.length} CWMS + ${usgs.length} USGS`);
    res.json({ ok: true, pairs });
  } catch (err) {
    console.error('[Sites API] /discover/match error:', err.message);
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

async function usgsQuery(wLon, sLat, eLon, nLat, attempt = 0) {
  const url = `${USGS_BASE}&bBox=${wLon},${sLat},${eLon},${nLat}`;
  if (attempt === 0) console.log('[USGS] GET', url);
  try {
    const text = await fetchText(url, 45000);
    const warn = extractUsgsRdbError(text);
    if (warn) console.warn('[USGS] RDB warning:', warn);
    return parseUsgsRdb(text).map(usgsRowToSite).filter(s => s.lat && s.lon);
  } catch (err) {
    if (attempt < 3) {
      const delay = (attempt + 1) * 2000;
      console.warn(`[USGS] attempt ${attempt + 1} failed (${err.message}), retrying in ${delay}ms`);
      await new Promise(r => setTimeout(r, delay));
      return usgsQuery(wLon, sLat, eLon, nLat, attempt + 1);
    }
    throw err;
  }
}

router.get('/discover/usgs', async (req, res) => {
  try {
    const { bbox, id, name, huc } = req.query;

    if (huc) {
      // HUC is a native USGS major filter — single clean request, no chunking needed.
      const url   = `https://waterservices.usgs.gov/nwis/site/?format=rdb&siteStatus=all&huc=${encodeURIComponent(huc)}`;
      const text  = await fetchText(url, 30000);
      const sites = parseUsgsRdb(text).map(usgsRowToSite).filter(s => s.lat && s.lon);
      return res.json({ ok: true, sites, count: sites.length });
    }

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

    const dsCfg   = getDatasourcesConfig();
    const cwms    = dsCfg.cwms || {};
    const base    = cwms.baseUrl || 'https://cwms-data.usace.army.mil/cwms-data';
    const office  = cwms.office || '';

    // Strategy 1: ask CDA to filter by bounding-box (supported on national CDA and newer local)
    // Strategy 2: if bbox not supported, fall back to fetching all locations for the configured office
    const bbParam  = encodeURIComponent(bbox);
    const officeQ  = office ? `&office=${encodeURIComponent(office)}` : '';
    const allEntries = [];
    let   nextPage   = null;
    let   usedBbox   = true;

    try {
      do {
        const url = `${base}/catalog/LOCATIONS?format=json&bounding-box=${bbParam}&page-size=500${officeQ}`
          + (nextPage ? `&page=${encodeURIComponent(nextPage)}` : '');
        const data = await fetchJson(url);
        const entries = _normCdaLocEntries(data);
        allEntries.push(...entries);
        nextPage = (data && data['next-page']) || null;
        console.log(`[CWMS bbox] fetched ${entries.length} entries, nextPage=${!!nextPage}`);
      } while (nextPage);
    } catch (bboxErr) {
      // bounding-box not supported — fall back to all locations for the office
      console.warn('[CWMS bbox] bounding-box param failed, falling back to office-wide fetch:', bboxErr.message);
      usedBbox = false;
      const tryUrls = [
        `${base}/catalog/LOCATIONS?format=json&page-size=1000${officeQ}`,
        `${base}/catalog/LOCATIONS?page-size=1000${officeQ}`,
      ];
      let fallbackData;
      for (const url of tryUrls) {
        try { fallbackData = await fetchJson(url); break; } catch (_) {}
      }
      if (fallbackData) allEntries.push(..._normCdaLocEntries(fallbackData));
    }

    // Check whether entries include coordinates at all
    const hasCoords = allEntries.some(e => e.latitude != null && e.longitude != null);

    const seen  = new Set();
    const sites = allEntries
      // Only geo-filter if entries actually carry coordinates AND we have reliable bbox coverage
      .filter(e => {
        if (!hasCoords) return true; // CDA didn't return coords — trust its own bbox filter / show all
        if (e.latitude == null || e.longitude == null) return false;
        if (!usedBbox) return true;  // no bbox applied server-side — show all with coords
        // Server-side bbox confirmation (national CDA bbox param is approximate)
        return e.latitude  >= minLat && e.latitude  <= maxLat &&
               e.longitude >= minLon && e.longitude <= maxLon;
      })
      .map(e => ({
        id:     e['location-id'] || e.name || e.id,
        name:   e['public-name'] || e['long-name'] || e['location-id'] || e.name || e.id,
        office: e.office,
        type:   (e['location-kind'] || e.kind || 'site').toLowerCase(),
        lat:    e.latitude  != null ? e.latitude  : null,
        lon:    e.longitude != null ? e.longitude : null,
        source: 'cwms',
      }))
      .filter(s => s.id && !seen.has(s.id) && seen.add(s.id));

    const offices = [...new Set(sites.map(s => s.office).filter(Boolean))];
    console.log(`[CWMS bbox] returning ${sites.length} sites (hasCoords=${hasCoords}, usedBbox=${usedBbox})`);
    res.json({ ok: true, sites, offices, count: sites.length, coordsMissing: !hasCoords });
  } catch (err) {
    console.error('[Sites API] CWMS bbox error:', err.message);
    res.json({ ok: false, sites: [], offices: [], count: 0, warning: err.message });
  }
});

// ── GET /api/sites/discover/cwms/probe ────────────────────────────────────────
// Tests the configured CDA connection. Returns offices list + raw response for debugging.

router.get('/discover/cwms/probe', async (req, res) => {
  const dsCfg = getDatasourcesConfig();
  const cwms  = dsCfg.cwms || {};
  const base  = cwms.baseUrl || 'https://cwms-data.usace.army.mil/cwms-data';
  const office = cwms.office || '';
  const results = { baseUrl: base, office, offices: null, officesError: null, locSample: null, locSampleError: null, locRawPreview: null };

  // 1. Fetch offices list — some CDA versions return XML; capture raw text too
  try {
    results.offices = await fetchJson(`${base}/offices`);
  } catch (e) {
    results.officesError = e.message;
    // Try fetching raw text to see what actually came back
    try {
      const raw = await fetchText(`${base}/offices`);
      results.officesRawPreview = raw.slice(0, 300).replace(/\s+/g, ' ').trim();
    } catch (_) {}
  }

  // 2. Try catalog/LOCATIONS with multiple param styles + format=json
  const probeUrls = [
    `${base}/catalog/LOCATIONS?format=json&page-size=5` + (office ? `&office=${encodeURIComponent(office)}` : ''),
    `${base}/catalog/LOCATIONS?page-size=5`            + (office ? `&office=${encodeURIComponent(office)}` : ''),
    `${base}/catalog/LOCATIONS?format=json&pageSize=5` + (office ? `&office=${encodeURIComponent(office)}` : ''),
    `${base}/catalog/LOCATIONS?pageSize=5`             + (office ? `&office=${encodeURIComponent(office)}` : ''),
  ];

  let probeOk = false;
  for (const url of probeUrls) {
    try {
      results.locSample = await fetchJson(url);
      results.locUrlUsed = url;
      probeOk = true;
      break;
    } catch (e) {
      results.locSampleError = e.message;
      // If it's a JSON parse error, capture raw so we can see the actual format
      if (e.message.startsWith('JSON parse')) {
        try {
          const raw = await fetchText(url);
          results.locRawPreview = raw.slice(0, 500).replace(/\s+/g, ' ').trim();
        } catch (_) {}
      }
    }
  }

  res.json({ ok: true, probeSuccess: probeOk, ...results });
});

// ── GET /api/sites/discover/cwms/locations ─────────────────────────────────────
// Query params:
//   office = "LRN"  (Corps district office code)
//   like   = "partial name filter" (optional)

// Normalise a raw CDA locations response into a flat entries array,
// handling multiple response shapes across CDA versions.
function _normCdaLocEntries(data) {
  if (!data) return [];
  if (Array.isArray(data))          return data;           // bare array
  if (Array.isArray(data.entries))  return data.entries;   // { entries: [...] }
  if (Array.isArray(data.locations)) return data.locations; // { locations: [...] }
  // Some versions nest under the office name
  for (const key of Object.keys(data)) {
    if (Array.isArray(data[key])) return data[key];
  }
  return [];
}

router.get('/discover/cwms/locations', async (req, res) => {
  try {
    const dsCfg  = getDatasourcesConfig();
    const cwms   = dsCfg.cwms || {};
    const base   = cwms.baseUrl || 'https://cwms-data.usace.army.mil/cwms-data';
    const office = req.query.office || cwms.office || '';
    const like   = req.query.like || '';

    // Try format=json + kebab-case first, then without format param, then camelCase, then /locations path
    let data;
    const officeQ = office ? `&office=${encodeURIComponent(office)}` : '';
    const likeQ   = like   ? `&like=${encodeURIComponent(like)}`     : '';
    const tryUrls = [
      `${base}/catalog/LOCATIONS?format=json&page-size=1000${officeQ}${likeQ}`,
      `${base}/catalog/LOCATIONS?page-size=1000${officeQ}${likeQ}`,
      `${base}/catalog/LOCATIONS?format=json&pageSize=1000${officeQ}${likeQ}`,
      `${base}/catalog/LOCATIONS?pageSize=1000${officeQ}${likeQ}`,
      `${base}/locations?format=json&page-size=1000${officeQ}`,
      `${base}/locations?page-size=1000${officeQ}`,
    ];

    let lastErr;
    for (const url of tryUrls) {
      try { data = await fetchJson(url); break; }
      catch (e) { lastErr = e; }
    }
    if (!data) throw lastErr;

    // Normalise and log raw shape for debugging
    const entries = _normCdaLocEntries(data);
    console.log(`[CWMS Catalog] Got ${entries.length} locations from ${base} (office=${office||'*'})`);
    if (entries.length) console.log('[CWMS Catalog] Sample entry keys:', Object.keys(entries[0]).join(', '));

    res.json({ ok: true, data: { entries } });
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
    const office     = req.query.office     || cwms.office || '';
    const locationId = req.query.locationId || '';

    if (!locationId) return res.status(400).json({ error: 'locationId is required' });

    const like = `${locationId}.*`;
    let data, lastErr;
    const officeQ = office ? `&office=${encodeURIComponent(office)}` : '';
    const tryUrls = [
      `${base}/catalog/TIMESERIES?format=json&page-size=500${officeQ}&like=${encodeURIComponent(like)}`,
      `${base}/catalog/TIMESERIES?page-size=500${officeQ}&like=${encodeURIComponent(like)}`,
      `${base}/catalog/TIMESERIES?format=json&pageSize=500${officeQ}&like=${encodeURIComponent(like)}`,
      `${base}/catalog/TIMESERIES?pageSize=500${officeQ}&like=${encodeURIComponent(like)}`,
      `${base}/timeseries?format=json&page-size=500${officeQ}&name=${encodeURIComponent(like)}`,
      `${base}/timeseries?page-size=500${officeQ}&name=${encodeURIComponent(like)}`,
    ];
    for (const url of tryUrls) {
      try { data = await fetchJson(url); break; }
      catch (e) { lastErr = e; }
    }
    if (!data) throw lastErr;

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
