'use strict';

const express = require('express');
const router  = express.Router();
const db      = require('../../db/database');

// ── GET /api/users/me ─────────────────────────────────────────────────────────
router.get('/me', (req, res) => {
  const aiSettings = db.getUserAiSettings(req.user.id);
  const favorites  = db.getUserFavoriteBasins(req.user.id);
  res.json({ user: req.user, aiSettings, favorites });
});

// ── GET /api/users/favorites ──────────────────────────────────────────────────
router.get('/favorites', (req, res) => {
  res.json(db.getUserFavoriteBasins(req.user.id));
});

// ── POST /api/users/favorites/:huc8 ──────────────────────────────────────────
router.post('/favorites/:huc8', (req, res) => {
  const { huc8 } = req.params;
  if (!/^\d{8}$/.test(huc8)) return res.status(400).json({ error: 'huc8 must be 8 digits' });
  const name = req.body.huc8_name || null;
  db.addUserFavoriteBasin(req.user.id, huc8, name);
  res.json({ ok: true });
});

// ── DELETE /api/users/favorites/:huc8 ────────────────────────────────────────
router.delete('/favorites/:huc8', (req, res) => {
  const { huc8 } = req.params;
  db.removeUserFavoriteBasin(req.user.id, huc8);
  res.json({ ok: true });
});

module.exports = router;
