'use strict';

const express = require('express');
const router  = express.Router();
const db      = require('../../db/database');

function uid(req) { return req.user ? req.user.id : null; }

// GET /api/groups
router.get('/', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    res.json(db.getGroupsForUser(userId));
  } catch (err) {
    console.error('[groups GET /] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/groups
router.post('/', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    const { name, color } = req.body;
    if (!name || !name.trim()) return res.status(400).json({ error: 'name is required' });
    const id = db.createGroup(userId, name.trim(), color || '#3b82f6');
    res.json({ ok: true, id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/groups/:id
router.patch('/:id', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    const { name, color } = req.body;
    const changes = db.updateGroup(parseInt(req.params.id), userId, { name, color });
    if (!changes) return res.status(404).json({ error: 'Group not found' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/groups/:id
router.delete('/:id', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    const changes = db.deleteGroup(parseInt(req.params.id), userId);
    if (!changes) return res.status(404).json({ error: 'Group not found' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/groups/:id/members
router.get('/:id/members', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    const group = db.getGroupById(parseInt(req.params.id), userId);
    if (!group) return res.status(404).json({ error: 'Group not found' });
    res.json(db.getGroupMembers(parseInt(req.params.id)));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/groups/:id/members  (bulk replace)
router.put('/:id/members', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    const { siteIds } = req.body;
    if (!Array.isArray(siteIds)) return res.status(400).json({ error: 'siteIds must be an array' });
    const ok = db.setGroupMembers(parseInt(req.params.id), userId, siteIds);
    if (!ok) return res.status(404).json({ error: 'Group not found' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/groups/:id/publish  — { published: true|false }
router.post('/:id/publish', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    const changes = db.publishGroup(parseInt(req.params.id), userId, !!req.body.published);
    if (!changes) return res.status(404).json({ error: 'Group not found' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/groups/shared  — published groups from other users
router.get('/shared', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    const rows = db.getPublishedGroups(userId);
    // Parse site_names_raw → trimmed array of up to 5 names
    const groups = rows.map(g => ({
      ...g,
      site_names: g.site_names_raw
        ? [...new Set(g.site_names_raw.split('||'))].filter(Boolean).slice(0, 5)
        : [],
      site_names_raw: undefined,
    }));
    res.json({ ok: true, groups });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/groups/shared/:id/save  — save a copy to my groups
router.post('/shared/:id/save', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    const localId = db.saveGroupCopy(userId, parseInt(req.params.id));
    if (!localId) return res.status(404).json({ error: 'Group not found or no longer shared' });
    res.json({ ok: true, id: localId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/groups/:id/sync  — pull latest from source group
router.post('/:id/sync', (req, res) => {
  try {
    const userId = uid(req);
    if (!userId) return res.status(401).json({ error: 'Not logged in' });
    const ok = db.syncGroupFromSource(parseInt(req.params.id), userId);
    if (!ok) return res.status(400).json({ error: 'Cannot sync — source is no longer available' });
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
