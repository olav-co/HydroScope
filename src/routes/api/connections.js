const express = require('express');
const router  = express.Router();
const db      = require('../../db/database');
const { discoverNetworkTopology } = require('../../services/usgs');
const waterways = require('../../services/waterways');

// GET /api/connections — return all stored connections
router.get('/', (req, res) => {
  try {
    const siteIds = req.query.site_ids
      ? req.query.site_ids.split(',').map(s => s.trim()).filter(Boolean)
      : null;
    res.json({ data: db.getSiteConnections(siteIds) });
  } catch (err) {
    console.error('[connections GET]', err.message);
    res.status(500).json({ error: 'Failed to load connections.' });
  }
});

// POST /api/connections/sync — re-run NLDI topology discovery
router.post('/sync', async (req, res) => {
  try {
    const allSites = db.getAllSites();
    if (!allSites.length) return res.status(400).json({ error: 'No sites configured.' });
    const siteIds = allSites.map(s => s.site_id);
    const discovered = await discoverNetworkTopology(siteIds);
    db.syncNLDIConnections(discovered);
    const all = db.getSiteConnections(null);
    const syncInfo = db.getLastNLDISync();
    res.json({ ok: true, discovered: discovered.length, connections: all, synced_at: syncInfo.synced_at });
  } catch (err) {
    console.error('[connections/sync]', err.message);
    res.status(500).json({ error: 'NLDI topology discovery failed: ' + err.message });
  }
});

// GET /api/connections/sync-status
router.get('/sync-status', (req, res) => {
  try {
    const info = db.getLastNLDISync();
    res.json({ ok: true, synced_at: info.synced_at, count: info.count });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/connections/waterways?sites=id1,id2
// Returns cached waterway paths for active connection pairs.
// Query by site list so the client only gets paths it actually needs.
router.get('/waterways', (req, res) => {
  try {
    const siteIds = req.query.sites
      ? req.query.sites.split(',').map(s => s.trim()).filter(Boolean)
      : null;

    // Get relevant connections
    const conns = db.getSiteConnections(siteIds);
    if (!conns.length) return res.json({ ok: true, data: [] });

    // Fetch cached paths for those pairs
    const pairs = conns.map(c => ({ from_site_id: c.from_site_id, to_site_id: c.to_site_id }));
    const rows  = db.getWaterwayPaths(pairs);

    // Build a lookup map and annotate each connection
    const pathMap = {};
    for (const r of rows) pathMap[r.from_site_id + '->' + r.to_site_id] = r;

    const data = conns.map(c => {
      const cached = pathMap[c.from_site_id + '->' + c.to_site_id];
      return {
        from_site_id: c.from_site_id,
        to_site_id:   c.to_site_id,
        status:       cached ? cached.status : 'pending',
        path:         (cached && cached.path_json) ? JSON.parse(cached.path_json) : null,
        fetched_at:   cached ? cached.fetched_at : null,
      };
    });

    res.json({ ok: true, data, running: waterways.isRunning() });
  } catch(err) {
    console.error('[connections/waterways GET]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/connections/waterways/refresh
// Reset all pairs to pending so the scheduler re-crawls them over the next N*4 minutes.
router.post('/waterways/refresh', (req, res) => {
  try {
    const n = waterways.resetAllPairs();
    // Kick off first pair immediately rather than waiting for next tick
    waterways.processNextPair().catch(e => console.error('[Waterways] refresh first pair:', e.message));
    res.json({ ok: true, message: `Queued ${n} pair(s) for re-crawl. Processing one every 4 minutes.`, queued: n });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
