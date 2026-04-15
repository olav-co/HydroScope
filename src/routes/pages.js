const express = require('express');
const router = express.Router();
const db = require('../db/database');

// Shared view locals
function baseLocals(req) {
  const profile = db.getProfile();
  const lastFetch = db.getLastFetch();
  return { profile, lastFetch, activePage: '' };
}

router.get('/', (req, res) => {
  try {
    const sites = db.getAllSites();
    const latest = db.getLatestReadings();
    const profile = db.getProfile();
    const lastFetch = db.getLastFetch();

    // Group latest readings by site
    const bysite = {};
    for (const r of latest) {
      if (!bysite[r.site_id]) bysite[r.site_id] = { site_name: r.site_name, readings: {} };
      bysite[r.site_id].readings[r.parameter_code] = r;
    }

    res.render('index', { sites, bysite, profile, lastFetch, activePage: 'dashboard' });
  } catch (err) {
    res.status(500).render('error', { message: err.message });
  }
});

router.get('/visualize', (req, res) => {
  const sites = db.getAllSites();
  const { ...locals } = baseLocals(req);
  res.render('visualize', { ...locals, sites, activePage: 'visualize' });
});

router.get('/forecast', (req, res) => {
  const sites = db.getAllSites();
  const { ...locals } = baseLocals(req);
  res.render('forecast', { ...locals, sites, activePage: 'forecast' });
});

router.get('/compare', (req, res) => {
  const sites = db.getAllSites();
  const { ...locals } = baseLocals(req);
  res.render('compare', { ...locals, sites, activePage: 'compare' });
});

router.get('/insights', (req, res) => {
  const sites = db.getAllSites();
  const recentInsights = db.getRecentInsights(8);
  const { ...locals } = baseLocals(req);
  res.render('insights', { ...locals, sites, recentInsights, activePage: 'insights' });
});

router.get('/profile', (req, res) => {
  const sites = db.getAllSites();
  const { ...locals } = baseLocals(req);
  res.render('profile', { ...locals, sites, activePage: 'profile' });
});

module.exports = router;
