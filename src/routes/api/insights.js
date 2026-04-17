const express = require('express');
const router = express.Router();
const db = require('../../db/database');
const { generateInsight, generateEnrichment, generateChat, generateNetworkAnalysis } = require('../../services/gemini');

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
    res.status(500).json({ ok: false, error: 'Something went wrong. Please try again.' });
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
    console.error('[Insights/forecast] Error:', err.message);
    res.status(500).json({ ok: false, error: 'Something went wrong. Please try again.' });
  }
});

// POST /api/insights/enrich
// Returns structured JSON enrichments for a specific page context.
// Body: { page: 'dashboard'|'visualize'|'compare'|'forecast', context: { ... } }
router.post('/enrich', async (req, res) => {
  const { page, context } = req.body;
  if (!page) return res.status(400).json({ error: 'page is required' });

  const profile = db.getProfile();

  try {
    const result = await generateEnrichment(profile, page, context || {});
    res.json({ ok: true, data: result.data, cached: result.cached });
  } catch (err) {
    console.error('[Insights/enrich] Error:', err.message);
    res.status(500).json({ ok: false, error: 'Something went wrong. Please try again.' });
  }
});

// POST /api/insights/chat
// Body: { history: [{role, text}], query: string, mode: string, page_context: object }
// Map URL slugs to human-readable page names used in the wiki KB
const PAGE_NAMES = {
  'dashboard':      'Dashboard',
  'visualize':      'Data Visualization',
  'compare':        'Compare',
  'forecast':       'Forecast',
  'weather':        'Weather Data',
  'data-explorer':  'Data Explorer',
  'flow-network':   'Flow Network',
  'water-quality':  'Water Quality',
  'annotations':    'Annotations',
  'insights':       'Ask AI',
  'profile':        'Profile',
};

router.post('/chat', async (req, res) => {
  const { history = [], query, mode: requestedMode = 'general', page_context = null } = req.body;
  if (!query || !query.trim()) return res.status(400).json({ error: 'query is required' });

  const q = query.trim();
  const profile = db.getProfile();

  // Normalize page slug to readable name before passing to the pipeline
  const enrichedPageCtx = page_context ? { ...page_context } : null;
  if (enrichedPageCtx && enrichedPageCtx.current_page) {
    enrichedPageCtx.current_page_name = PAGE_NAMES[enrichedPageCtx.current_page] || enrichedPageCtx.current_page;
  }

  try {
    // Pipeline: local pre-filter → (optional) AI planner → data fulfillment → AI response
    // All routing, blocking, context-gap detection, and clarification live inside generateChat.
    const result = await generateChat(profile, history, q, enrichedPageCtx, requestedMode);

    if (result.blocked)       return res.json({ ok: true, blocked: true });
    if (result.needs_context) return res.json({ ok: true, needs_context: true, fields: result.fields });
    if (result.clarification) return res.json({ ok: true, clarification: result.clarification, mode: result.mode, modeChanged: result.modeChanged });

    res.json({
      ok: true,
      blocked: false,
      response: result.response,
      mode: result.mode,
      modeChanged: result.modeChanged,
      dataIncluded: result.dataIncluded,
    });
  } catch (err) {
    console.error('[Insights/chat] Error:', err.message);
    res.status(500).json({ ok: false, error: 'Something went wrong. Please try again.' });
  }
});

// POST /api/insights/network
// Body: { nodes: [{site_id,site_name,readings:[{...}]}], edges: [{from_site_id,to_site_id,label}], hours }
router.post('/network', async (req, res) => {
  const { nodes, edges, hours = 72 } = req.body;
  if (!nodes || !nodes.length) return res.status(400).json({ error: 'nodes array is required.' });
  const profile = db.getProfile();
  try {
    const result = await generateNetworkAnalysis(profile, nodes, edges || [], hours);
    res.json({ ok: true, data: result.data, cached: result.cached });
  } catch (err) {
    console.error('[Insights/network] Error:', err.message);
    res.status(500).json({ ok: false, error: 'Something went wrong. Please try again.' });
  }
});

module.exports = router;
