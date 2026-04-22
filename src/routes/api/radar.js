const express = require('express');
const router = express.Router();
const db = require('../../db/database');
const { generateInsight } = require('../../services/gemini');

// POST /api/radar/analyze
router.post('/analyze', async (req, res) => {
  const { radar_note, site_ids } = req.body;

  const profile = db.getProfile(req.user ? req.user.id : null);

  const radarCtx = radar_note
    ? `Current radar observation: ${radar_note}\n\n`
    : '';

  const query = `${radarCtx}Based on the current Doppler precipitation radar and the stream gauge readings below, provide a concise radar-hydrology analysis covering:
1. Precipitation intensity and spatial coverage over the watershed right now
2. Expected stream response over the next 1–3 hours (rainfall-to-runoff lag)
3. Sites most likely to see elevated flows given precipitation location
4. Any flood risk indicators or operational notes
5. Overall watershed status: dry / light / moderate / heavy / severe

Keep the analysis under 280 words. Be specific about site names and flow numbers where data supports it.`;

  try {
    const result = await generateInsight(profile, query, {
      siteIds: Array.isArray(site_ids) && site_ids.length ? site_ids : null,
      useCache: false,
    });
    res.json({ ok: true, response: result.response });
  } catch (err) {
    console.error('[Radar] AI error:', err.message);
    res.status(500).json({ ok: false, error: 'Analysis failed. Please try again.' });
  }
});

module.exports = router;
