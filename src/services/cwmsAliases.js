'use strict';

/**
 * CWMS ↔ USGS alias resolution via CDA Agency Aliases location group.
 *
 * Corps districts configure USGS station numbers as aliases on CWMS locations
 * inside CWMS-Vue (Location Groups → Agency Aliases → USGS Station Number).
 * CDA exposes these through the location/group endpoint.
 *
 * This service fetches and caches those mappings so pairing can use them
 * as the authoritative first-pass before falling back to COMID or proximity.
 */

const https  = require('https');
const db     = require('../db/database');

const ALIAS_TTL_MS  = 24 * 60 * 60 * 1000; // re-fetch after 24 h
const CATEGORY_ID   = 'Agency Aliases';
const GROUP_ID      = 'USGS Station Number';

// In-flight fetches keyed by office — prevents duplicate concurrent requests.
const _inFlight = new Map();

// ── HTTP helper ───────────────────────────────────────────────────────────────

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, {
      method: 'GET',
      headers: {
        'Accept':     'application/json;version=2',
        'User-Agent': 'HydroScope/1.0 (hydrology monitoring dashboard)',
      },
      timeout: 20000,
    }, res => {
      if (res.statusCode === 404) { res.resume(); return resolve(null); }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error(`HTTP ${res.statusCode}`)); }
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error('JSON parse: ' + e.message)); }
      });
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.end();
  });
}

// ── Fetch one office's aliases from CDA ──────────────────────────────────────

async function _fetchOffice(office, baseUrl) {
  const url = `${baseUrl}/location/group?`
    + `category-id=${encodeURIComponent(CATEGORY_ID)}`
    + `&group-id=${encodeURIComponent(GROUP_ID)}`
    + `&office=${encodeURIComponent(office)}`;

  console.log(`[Aliases] fetching ${office}: ${url}`);
  const data = await fetchJson(url);

  // CDA may return a single group object or an array; normalise.
  const groups = Array.isArray(data) ? data : (data ? [data] : []);

  const rows = [];
  for (const group of groups) {
    const locs = group['assigned-locations'] || [];
    for (const loc of locs) {
      const cwmsId = loc['location-id'];
      const usgsId = String(loc['alias-id'] || '').trim();
      if (cwmsId && usgsId && /^\d{7,15}$/.test(usgsId)) {
        rows.push({ cwmsId, usgsId, office: loc['office-id'] || office });
      }
    }
  }

  if (rows.length) {
    db.upsertCwmsUsgsAliases(rows);
  }
  db.markAliasFetched(office, rows.length);
  console.log(`[Aliases] ${office}: ${rows.length} alias(es) stored`);
  return rows.length;
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Ensure aliases for `office` are fresh (fetched within TTL).
 * Safe to call concurrently — deduplicates in-flight fetches.
 */
async function ensureOfficeAliases(office, baseUrl) {
  if (!office) return;

  // Already fresh?
  const fetchedAt = db.getAliasFetchedAt(office);
  if (fetchedAt && (Date.now() - new Date(fetchedAt).getTime()) < ALIAS_TTL_MS) return;

  // Deduplicate concurrent callers for the same office.
  if (_inFlight.has(office)) return _inFlight.get(office);

  const p = _fetchOffice(office, baseUrl).catch(err => {
    console.warn(`[Aliases] ${office} fetch failed: ${err.message}`);
  }).finally(() => _inFlight.delete(office));

  _inFlight.set(office, p);
  return p;
}

/**
 * Boot-time: refresh aliases for every CWMS office already in DB.
 * Runs in the background — does not block server start.
 */
async function refreshAllOfficeAliases(baseUrl) {
  // Collect offices from two sources:
  //   1. CWMS sites currently in DB (office column)
  //   2. Offices previously fetched into the alias table
  const siteOffices = db.getAllSites()
    .filter(s => s.source === 'cwms' && s.office)
    .map(s => s.office)
    .filter(Boolean);

  const knownOffices = db.getDistinctCwmsOffices();

  const offices = [...new Set([...siteOffices, ...knownOffices])];
  if (!offices.length) {
    console.log('[Aliases] no CWMS offices in DB — skipping boot fetch');
    return;
  }

  console.log(`[Aliases] boot refresh for offices: ${offices.join(', ')}`);
  // Sequential with a small gap to avoid hammering CDA.
  for (const office of offices) {
    try { await ensureOfficeAliases(office, baseUrl); }
    catch (_) {}
    await new Promise(r => setTimeout(r, 300));
  }
}

/**
 * Return the USGS station number for a CWMS location ID, or null.
 * Checks exact match first, then strips sub-location suffix
 * (e.g. "Cheatham-TW" → "Cheatham") as a fallback.
 */
function resolveAlias(cwmsId) {
  if (!cwmsId) return null;
  const direct = db.getCwmsUsgsAlias(cwmsId);
  if (direct) return direct;

  // Sub-location fallback: "Cheatham-TW" → try "Cheatham"
  const dashIdx = cwmsId.lastIndexOf('-');
  if (dashIdx > 0) {
    const parent = cwmsId.slice(0, dashIdx);
    return db.getCwmsUsgsAlias(parent);
  }
  return null;
}

/**
 * Build a Map<cwmsId, usgsId> from all stored aliases, including sub-location
 * expansion so "Cheatham-TW" resolves if only "Cheatham" is stored.
 */
function buildAliasMap() {
  const rows = db.getAllCwmsUsgsAliases();
  const map  = new Map();
  for (const r of rows) map.set(r.cwms_id, r.usgs_id);
  return map;
}

module.exports = { ensureOfficeAliases, refreshAllOfficeAliases, resolveAlias, buildAliasMap };
