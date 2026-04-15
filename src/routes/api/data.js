const express = require('express');
const router = express.Router();
const db = require('../../db/database');
const { runFetch } = require('../../services/scheduler');

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

// POST /api/data/refresh — manually trigger a fetch
router.post('/refresh', async (req, res) => {
  try {
    await runFetch();
    res.json({ ok: true, message: 'Fetch triggered successfully.' });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

module.exports = router;
