const express = require('express');
const router = express.Router();
const db = require('../db/database');

// Shared view locals
function baseLocals(req) {
  const userId = req.user ? req.user.id : null;
  const profile = db.getProfile(userId);
  const lastFetch = db.getLastFetch();
  return { profile, lastFetch, activePage: '' };
}

router.get('/', (req, res) => {
  try {
    const sites = db.getAllSites();
    const latest = db.getLatestReadings();
    const profile = db.getProfile(req.user ? req.user.id : null);
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

router.get('/weather', (req, res) => {
  const { ...locals } = baseLocals(req);
  res.render('weather', { ...locals, activePage: 'weather' });
});

router.get('/data-explorer', (req, res) => {
  const measurementSeries = db.getMeasurementSeries();
  const weatherSeries     = db.getWeatherSeries();
  const { ...locals } = baseLocals(req);
  res.render('data-explorer', { ...locals, measurementSeries, weatherSeries, activePage: 'data-explorer' });
});

router.get('/flow-network', (req, res) => {
  const sites = db.getAllSites();
  const connections = db.getSiteConnections(null);
  const { ...locals } = baseLocals(req);
  res.render('flow-network', { ...locals, sites, connections, activePage: 'flow-network' });
});

router.get('/water-quality', (req, res) => {
  const sites = db.getAllSites();
  const { ...locals } = baseLocals(req);
  res.render('water-quality', { ...locals, sites, activePage: 'water-quality' });
});

router.get('/annotations', (req, res) => {
  const sites = db.getAllSites();
  const { ...locals } = baseLocals(req);
  res.render('annotations', { ...locals, sites, activePage: 'annotations' });
});

router.get('/profile', (req, res) => {
  const sites = db.getAllSites();
  const { ...locals } = baseLocals(req);
  res.render('profile', { ...locals, sites, activePage: 'profile' });
});

router.get('/settings', (req, res) => {
  const { ...locals } = baseLocals(req);
  res.render('settings', { ...locals, activePage: 'settings' });
});

router.get('/sites', (req, res) => {
  const { ...locals } = baseLocals(req);
  res.render('sites', { ...locals, activePage: 'sites' });
});

router.get('/radar', (req, res) => {
  const sites = db.getAllSites();
  const { ...locals } = baseLocals(req);
  res.render('radar', { ...locals, sites, activePage: 'radar' });
});

router.get('/groups', (req, res) => {
  const sites = db.getAllSites();
  const { ...locals } = baseLocals(req);
  res.render('groups', { ...locals, sites, activePage: 'groups' });
});

module.exports = router;
