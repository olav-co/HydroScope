const express = require('express');
const router  = express.Router();
const db      = require('../../db/database');

// ── GET /api/annotations ──────────────────────────────────────────────────────
// ?site_id=&param_code=&start_at=&end_at=
router.get('/', (req, res) => {
  try {
    const { site_id, param_code, start_at, end_at } = req.query;
    const rows = db.getAnnotations({
      siteId:        site_id   || null,
      parameterCode: param_code || null,
      startAt:       start_at  || null,
      endAt:         end_at    || null,
    });
    res.json({ data: rows });
  } catch (err) {
    console.error('[annotations GET]', err.message);
    res.status(500).json({ error: 'Failed to load annotations.' });
  }
});

// ── POST /api/annotations ─────────────────────────────────────────────────────
router.post('/', (req, res) => {
  try {
    const { site_id, parameter_code, category, label, note, annotated_at } = req.body;
    if (!label || !annotated_at) {
      return res.status(400).json({ error: 'label and annotated_at are required.' });
    }
    const id = db.createAnnotation({ site_id, parameter_code, category, label, note, annotated_at });
    res.status(201).json({ data: { id } });
  } catch (err) {
    console.error('[annotations POST]', err.message);
    res.status(500).json({ error: 'Failed to create annotation.' });
  }
});

// ── PUT /api/annotations/:id ──────────────────────────────────────────────────
router.put('/:id', (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const { category, label, note, annotated_at } = req.body;
    const changes = db.updateAnnotation(id, { category, label, note, annotated_at });
    if (!changes) return res.status(404).json({ error: 'Annotation not found.' });
    res.json({ data: { id } });
  } catch (err) {
    console.error('[annotations PUT]', err.message);
    res.status(500).json({ error: 'Failed to update annotation.' });
  }
});

// ── DELETE /api/annotations/:id ───────────────────────────────────────────────
router.delete('/:id', (req, res) => {
  try {
    const id = parseInt(req.params.id, 10);
    const changes = db.deleteAnnotation(id);
    if (!changes) return res.status(404).json({ error: 'Annotation not found.' });
    res.json({ data: { deleted: id } });
  } catch (err) {
    console.error('[annotations DELETE]', err.message);
    res.status(500).json({ error: 'Failed to delete annotation.' });
  }
});

module.exports = router;
