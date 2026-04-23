const express = require('express');
const router = express.Router();
const db = require('../../db/database');
const { runFetch, runWeatherFetch, runCwmsFetch } = require('../../services/scheduler');

// GET /api/data/latest?sites=14211720,14142500
router.get('/latest', (req, res) => {
  const siteIds = req.query.sites ? req.query.sites.split(',') : null;
  const readings = db.getLatestReadings(siteIds);
  res.json({ ok: true, data: readings });
});

// GET /api/data/timeseries?site=14211720&param=00060&hours=168
// GET /api/data/timeseries?site=14211720&param=00060&hours=168
// If site is a combined parent, unions measurements from all enabled child sources.
router.get('/timeseries', (req, res) => {
  const { site, param, hours = 168 } = req.query;
  if (!site || !param) return res.status(400).json({ error: 'site and param are required' });

  const siteRow = db.getSiteById(site);
  if (siteRow && siteRow.source === 'combined') {
    const children = db.getSiteChildren(site);
    const enabled  = children.filter(c => c.enabled).map(c => c.child_site_id);
    const data     = enabled.length ? db.getTimeSeriesForSites(enabled, param, parseInt(hours)) : [];
    return res.json({
      ok: true, site, param, data, combined: true,
      sources: children.map(c => ({
        site_id: c.child_site_id, source: c.source, name: c.name, enabled: !!c.enabled,
      })),
    });
  }

  const data = db.getTimeSeriesForSite(site, param, parseInt(hours));
  res.json({ ok: true, site, param, data });
});

// GET /api/data/compare?sites=14211720,14211010&param=00060&hours=336
router.get('/compare', (req, res) => {
  const { sites, param, hours = 336 } = req.query;
  if (!sites || !param) return res.status(400).json({ error: 'sites and param are required' });
  const siteIds = sites.split(',');
  const data = db.getCompareData(siteIds, param, parseInt(hours));
  res.json({ ok: true, siteIds, param, data });
});

// GET /api/data/sites
router.get('/sites', (req, res) => {
  res.json({ ok: true, data: db.getAllSites() });
});

// GET /api/data/status
router.get('/status', (req, res) => {
  const lastFetch = db.getLastFetch();
  res.json({ ok: true, lastFetch });
});

// POST /api/data/refresh — manually trigger a USGS fetch
router.post('/refresh', async (req, res) => {
  try {
    await runFetch();
    res.json({ ok: true, message: 'USGS fetch triggered.' });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /api/data/cwms/refresh — manually trigger a CWMS fetch
router.post('/cwms/refresh', async (req, res) => {
  try {
    await runCwmsFetch();
    res.json({ ok: true, message: 'CWMS fetch triggered.' });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/data/weather?location=portland&param=precipitation&hours=72
router.get('/weather', (req, res) => {
  const { location, param = 'precipitation', hours = 72 } = req.query;
  if (location) {
    const data = db.getWeatherForChart(location, param, parseInt(hours));
    res.json({ ok: true, location, param, data });
  } else {
    const data = db.getRecentWeather(parseInt(hours));
    res.json({ ok: true, data });
  }
});

// GET /api/data/weather/locations — distinct locations that have weather data in DB
router.get('/weather/locations', (req, res) => {
  try {
    const locs = db.getDistinctWeatherLocations();
    res.json({ ok: true, locations: locs });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/data/weather/forecast?location=...
router.get('/weather/forecast', (req, res) => {
  const { location } = req.query;
  if (!location) return res.status(400).json({ error: 'location required' });
  const data = db.getWeatherForecast(location, 7);
  res.json({ ok: true, location, data });
});

// POST /api/data/weather/refresh
router.post('/weather/refresh', async (req, res) => {
  try {
    await runWeatherFetch();
    res.json({ ok: true, message: 'Weather fetch triggered.' });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/data/records?source=usgs&group_id=14211720&param_code=00060
// GET /api/data/site-params?site=...
router.get('/site-params', (req, res) => {
  const { site } = req.query;
  if (!site) return res.status(400).json({ error: 'site required' });
  const params = db.getSiteParamCodes(site);
  res.json({ ok: true, params });
});

// GET /api/data/records?source=cwms&group_id=CEHT1-CENTER_HILL&param_code=Elev-Pool
// GET /api/data/records?source=weather&group_id=portland&parameter=precipitation&interval=hourly
router.get('/records', (req, res) => {
  const { source, group_id, param_code, parameter, interval, limit = 5000 } = req.query;
  if (!source || !group_id) return res.status(400).json({ error: 'source and group_id are required' });

  try {
    if (source === 'usgs' || source === 'cwms') {
      if (!param_code) return res.status(400).json({ error: 'param_code required for ' + source });
      const data = db.getMeasurementRecords(group_id, param_code, parseInt(limit), source);
      res.json({ ok: true, data });
    } else if (source === 'weather') {
      if (!parameter || !interval) return res.status(400).json({ error: 'parameter and interval required for weather' });
      const data = db.getWeatherRecords(group_id, parameter, interval, parseInt(limit));
      res.json({ ok: true, data });
    } else {
      res.status(400).json({ error: 'source must be usgs, cwms, or weather' });
    }
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ── helpers ───────────────────────────────────────────────────────────────────
function httpsGetJSON(url, timeoutMs) {
  const https = require('https');
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: { 'User-Agent': 'HydroScope/1.0' } }, (res) => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode)); }
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { reject(e); } });
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs || 8000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function httpsGetText(url, timeoutMs) {
  const https = require('https');
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: { 'User-Agent': 'HydroScope/1.0' } }, (res) => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode)); }
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => resolve(raw));
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs || 8000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

// Parses USGS Statistics RDB text, returns seasonal percentiles for a given month+day.
function parseStatsRdb(text, month, day) {
  const lines = text.split('\n').filter(l => l.trim() && !l.startsWith('#'));
  if (lines.length < 3) return null;
  const headers = lines[0].split('\t');
  // lines[1] = data-type row — skip it
  const monthIdx = headers.indexOf('month_nu');
  const dayIdx   = headers.indexOf('day_nu');
  const countIdx = headers.indexOf('count_nu');
  const p10Idx   = headers.indexOf('p10_va');
  const p25Idx   = headers.indexOf('p25_va');
  const p50Idx   = headers.indexOf('p50_va');
  const p75Idx   = headers.indexOf('p75_va');
  const p90Idx   = headers.indexOf('p90_va');
  if (monthIdx < 0 || dayIdx < 0) return null;

  for (const line of lines.slice(2)) {
    const c = line.split('\t');
    if (+c[monthIdx] !== month || +c[dayIdx] !== day) continue;
    const pick = (i) => (i >= 0 && c[i] && c[i] !== '' && c[i] !== 'e') ? parseFloat(c[i]) : null;
    return {
      p10: pick(p10Idx), p25: pick(p25Idx), p50: pick(p50Idx),
      p75: pick(p75Idx), p90: pick(p90Idx),
      count: countIdx >= 0 ? +c[countIdx] || 0 : 0,
    };
  }
  return null;
}

// GET /api/data/flood-stages?sites=14211720,14142500
// Pulls action/flood/moderate/major stage thresholds from USGS WaterWatch.
router.get('/flood-stages', async (req, res) => {
  const sites = req.query.sites ? req.query.sites.split(',').map(s => s.trim()).filter(Boolean) : [];
  if (!sites.length) return res.json({ ok: true, data: {} });
  try {
    const url = `https://waterwatch.usgs.gov/webservices/realtime?format=json&site_no=${sites.join(',')}`;
    const json = await httpsGetJSON(url, 10000);
    const result = {};
    (json.sites || []).forEach(s => {
      result[s.site_no] = {
        action:   s.action_stage   != null ? +s.action_stage   : null,
        flood:    s.flood_stage    != null ? +s.flood_stage    : null,
        moderate: s.moderate_stage != null ? +s.moderate_stage : null,
        major:    s.major_stage    != null ? +s.major_stage    : null,
        class:    s.class || 'normal',
      };
    });
    res.json({ ok: true, data: result });
  } catch (err) {
    console.error('[flood-stages]', err.message);
    res.json({ ok: true, data: {} });   // degrade gracefully
  }
});

// GET /api/data/official-thresholds?site_id=14211720&param_code=00065
// Returns official thresholds (USGS WaterWatch flood stages, USGS Statistics seasonal
// percentiles) cached in the DB for 24 h. On cache miss, fetches live and stores results.
router.get('/official-thresholds', async (req, res) => {
  const { site_id, param_code } = req.query;
  if (!site_id || !param_code) return res.status(400).json({ error: 'site_id and param_code required' });

  try {
    // Return cached data if < 24 h old
    const cached = db.getOfficialThresholds(site_id, param_code);
    if (cached.length) {
      const age = Date.now() - new Date(cached[0].fetched_at).getTime();
      if (age < 24 * 3600 * 1000) return res.json({ ok: true, thresholds: cached });
    }

    const site = db.getSiteById(site_id);
    const isUsgs = site && site.source === 'usgs';
    const thresholds = [];

    // ── 1. USGS WaterWatch flood stages (gage height only) ──────────────────
    if (param_code === '00065') {
      try {
        const wUrl = `https://waterwatch.usgs.gov/webservices/realtime?format=json&site_no=${site_id}`;
        const wJson = await httpsGetJSON(wUrl, 10000);
        const s = (wJson.sites || []).find(x => x.site_no === site_id);
        if (s) {
          const stages = [
            { id: 'action_stage',   label: 'Action Stage',         value: s.action_stage,   color: '#f59e0b', type: 'caution'      },
            { id: 'flood_stage',    label: 'Flood Stage',          value: s.flood_stage,    color: '#f87171', type: 'max_advisory' },
            { id: 'moderate_flood', label: 'Moderate Flood Stage', value: s.moderate_stage, color: '#ef4444', type: 'max_advisory' },
            { id: 'major_flood',    label: 'Major Flood Stage',    value: s.major_stage,    color: '#dc2626', type: 'max_advisory' },
          ];
          stages.forEach(st => {
            if (st.value == null) return;
            thresholds.push({
              threshold_id: st.id,
              label:        st.label,
              value:        +st.value,
              unit:         'ft',
              source:       'USGS WaterWatch',
              source_label: 'Official NWS/USGS flood classification stage for this gage',
              type:         st.type,
              color:        st.color,
              category:     'official',
            });
          });
        }
      } catch (e) {
        console.warn('[official-thresholds] WaterWatch:', e.message);
      }
    }

    // ── 2. USGS Statistics seasonal percentiles (USGS sites only) ───────────
    if (isUsgs) {
      try {
        const today = new Date();
        const mm    = today.getMonth() + 1;
        const dd    = today.getDate();
        const sUrl  = `https://waterservices.usgs.gov/nwis/stat/?format=rdb&sites=${site_id}` +
                      `&statReportType=daily&statType=all&parameterCd=${param_code}`;
        const rdb   = await httpsGetText(sUrl, 14000);
        const pcts  = parseStatsRdb(rdb, mm, dd);

        if (pcts) {
          const mmdd  = String(mm).padStart(2,'0') + String(dd).padStart(2,'0');
          const yrs   = pcts.count ? ` (${pcts.count} yrs of record)` : '';
          const seasonNote = `Historical seasonal percentile for this calendar date${yrs}. Source: USGS National Water Information System.`;
          const pMap = [
            { key: 'p10', label: 'Seasonal Low (10th pct)',   color: '#38bdf8', type: 'min_advisory' },
            { key: 'p25', label: 'Below Normal (25th pct)',   color: '#7dd3fc', type: 'min_advisory' },
            { key: 'p75', label: 'Above Normal (75th pct)',   color: '#fb923c', type: 'max_advisory' },
            { key: 'p90', label: 'Seasonal High (90th pct)',  color: '#f87171', type: 'max_advisory' },
          ];
          pMap.forEach(p => {
            if (pcts[p.key] == null || isNaN(pcts[p.key])) return;
            thresholds.push({
              threshold_id: `stats_${p.key}_${mmdd}`,
              label:        p.label,
              value:        pcts[p.key],
              unit:         null,
              source:       'USGS Statistics',
              source_label: seasonNote,
              type:         p.type,
              color:        p.color,
              category:     'seasonal',
            });
          });
        }
      } catch (e) {
        console.warn('[official-thresholds] Statistics:', e.message);
      }
    }

    // Persist to DB (upsert keeps old rows if fetch returned nothing new)
    thresholds.forEach(t => db.upsertOfficialThreshold(site_id, param_code, t));

    // Re-read from DB so fetched_at and ids are consistent
    res.json({ ok: true, thresholds: db.getOfficialThresholds(site_id, param_code) });
  } catch (err) {
    console.error('[official-thresholds]', err.message);
    res.json({ ok: true, thresholds: [] });
  }
});

// GET /api/data/snotel?lat=45.4&lon=-121.9&radius=75
// Returns up to 4 nearby NRCS SNOTEL stations with current SWE (inches).
router.get('/snotel', async (req, res) => {
  const { lat, lon, radius = 75 } = req.query;
  if (!lat || !lon) return res.status(400).json({ error: 'lat and lon required' });
  try {
    // Step 1: nearest SNOTEL stations
    const stUrl = `https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/stations?activeOnly=true`
      + `&maxDistanceKilometers=${radius}&latitude=${lat}&longitude=${lon}&networkCds=SNTL&sortBy=distance&pageSize=4`;
    const stations = await httpsGetJSON(stUrl, 10000);
    const list = Array.isArray(stations) ? stations.slice(0, 4) : [];
    if (!list.length) return res.json({ ok: true, data: [] });

    // Step 2: last 7 days of SWE for those stations
    const triplets = encodeURIComponent(list.map(s => s.stationTriplet).join(','));
    const today    = new Date().toISOString().split('T')[0];
    const weekAgo  = new Date(Date.now() - 7*86400000).toISOString().split('T')[0];
    const dataUrl  = `https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/data`
      + `?stationTriplets=${triplets}&elementCd=WTEQ&duration=DAILY&beginDate=${weekAgo}&endDate=${today}`;
    const sweRows = await httpsGetJSON(dataUrl, 10000).catch(() => []);
    const sweMap  = {};
    (Array.isArray(sweRows) ? sweRows : []).forEach(r => { sweMap[r.stationTriplet] = r; });

    const data = list.map(st => {
      const row    = sweMap[st.stationTriplet];
      const values = row?.data?.[0]?.values || [];
      const latest = [...values].reverse().find(v => v.value != null);
      return {
        triplet:   st.stationTriplet,
        name:      st.name,
        elevation: st.elevation,
        lat:       st.latitude,
        lon:       st.longitude,
        swe_in:    latest?.value ?? null,
        date:      latest?.date  ?? null,
      };
    });
    res.json({ ok: true, data });
  } catch (err) {
    console.error('[snotel]', err.message);
    res.json({ ok: true, data: [] });
  }
});

module.exports = router;
