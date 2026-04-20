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
router.get('/timeseries', (req, res) => {
  const { site, param, hours = 168 } = req.query;
  if (!site || !param) return res.status(400).json({ error: 'site and param are required' });
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

// GET /api/data/weather/forecast?location=portland
router.get('/weather/forecast', (req, res) => {
  const { location = 'portland' } = req.query;
  const db = require('../../db/database');
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

// ── helper: simple https GET → parsed JSON ────────────────────────────────────
function httpsGetJSON(url, timeoutMs) {
  const https = require('https');
  return new Promise((resolve, reject) => {
    const req = https.get(url, (res) => {
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode)); }
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch(e) { reject(e); } });
    });
    req.on('error', reject);
    req.setTimeout(timeoutMs || 8000, () => { req.destroy(); reject(new Error('timeout')); });
  });
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
