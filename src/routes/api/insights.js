const express = require('express');
const router = express.Router();
const db = require('../../db/database');
const { generateInsight } = require('../../services/gemini');

// POST /api/insights/ask
router.post('/ask', async (req, res) => {
  const { query, site_ids, bypass_cache } = req.body;

  if (!query || !query.trim()) {
    return res.status(400).json({ error: 'query is required' });
  }

  const profile = db.getProfile();
  const siteIds = Array.isArray(site_ids) && site_ids.length ? site_ids : null;

  try {
    const result = await generateInsight(profile, query.trim(), {
      siteIds,
      useCache: !bypass_cache,
    });
    res.json({ ok: true, response: result.response, cached: result.cached });
  } catch (err) {
    console.error('[Insights] Gemini error:', err.message);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/insights/recent
router.get('/recent', (req, res) => {
  const limit = parseInt(req.query.limit) || 10;
  const data = db.getRecentInsights(limit);
  res.json({ ok: true, data });
});

// POST /api/insights/forecast
// Uses AI to project near-term conditions for a specific site
router.post('/forecast', async (req, res) => {
  const { site_id, days_ahead = 3 } = req.body;
  if (!site_id) return res.status(400).json({ error: 'site_id is required' });

  const site = db.getSiteById(site_id);
  if (!site) return res.status(404).json({ error: 'Site not found' });

  const profile = db.getProfile();
  const query = `Based on the current and recent trend data for ${site.name}, provide a ${days_ahead}-day outlook.
    Include: expected flow direction (rising/falling/stable), any conditions of concern, and confidence level.
    Format as: Summary, Trend Analysis, ${days_ahead}-Day Outlook, Confidence & Caveats.`;

  try {
    const result = await generateInsight(profile, query, { siteIds: [site_id], useCache: true });
    res.json({ ok: true, site, response: result.response, cached: result.cached });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

module.exports = router;
