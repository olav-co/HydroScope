const express = require('express');
const router  = express.Router();
const db      = require('../../db/database');

// ── GET /api/wq/limits ────────────────────────────────────────────────────────
// ?site_id=&param_code=
router.get('/limits', (req, res) => {
  try {
    const { site_id, param_code } = req.query;
    const rows = db.getWQPermitLimits(site_id || null, param_code || null);
    res.json({ data: rows });
  } catch (err) {
    console.error('[wq/limits GET]', err.message);
    res.status(500).json({ error: 'Failed to load permit limits.' });
  }
});

// ── POST /api/wq/limits ───────────────────────────────────────────────────────
router.post('/limits', (req, res) => {
  try {
    const { site_id, parameter_code, limit_type, value, label, color } = req.body;
    if (!site_id || !parameter_code || !limit_type || value == null) {
      return res.status(400).json({ error: 'site_id, parameter_code, limit_type, and value are required.' });
    }
    const id = db.upsertWQPermitLimit({ site_id, parameter_code, limit_type, value: Number(value), label, color });
    res.status(201).json({ data: { id } });
  } catch (err) {
    console.error('[wq/limits POST]', err.message);
    res.status(500).json({ error: 'Failed to save permit limit.' });
  }
});

// ── PUT /api/wq/limits/:id ────────────────────────────────────────────────────
router.put('/limits/:id', (req, res) => {
  try {
    const { site_id, parameter_code, limit_type, value, label, color } = req.body;
    const id = db.upsertWQPermitLimit({ id: parseInt(req.params.id, 10), site_id, parameter_code, limit_type, value: Number(value), label, color });
    res.json({ data: { id } });
  } catch (err) {
    console.error('[wq/limits PUT]', err.message);
    res.status(500).json({ error: 'Failed to update permit limit.' });
  }
});

// ── DELETE /api/wq/limits/:id ─────────────────────────────────────────────────
router.delete('/limits/:id', (req, res) => {
  try {
    const changes = db.deleteWQPermitLimit(parseInt(req.params.id, 10));
    if (!changes) return res.status(404).json({ error: 'Limit not found.' });
    res.json({ data: { deleted: parseInt(req.params.id, 10) } });
  } catch (err) {
    console.error('[wq/limits DELETE]', err.message);
    res.status(500).json({ error: 'Failed to delete permit limit.' });
  }
});

module.exports = router;
