'use strict';

/**
 * HydroScope config loader
 *
 * Reads from split config files (preferred):
 *   config/ai.json          — AI provider keys & settings
 *   config/datasources.json — server, scheduler, USGS params, CWMS connection
 *
 * Falls back to legacy config/config.json automatically.
 *
 * On first boot with only config.json present, ensureMigrated() will:
 *   1. Write ai.json and datasources.json from the monolithic file
 *   2. Seed sites into the DB from the legacy sites arrays
 *   3. Mark seed as applied so it never runs again
 *
 * config.json is never deleted — it stays as a read-only reference.
 */

const fs   = require('fs');
const path = require('path');

const CONFIG_DIR  = path.join(__dirname, '../../config');
const AI_PATH     = path.join(CONFIG_DIR, 'ai.json');
const DS_PATH     = path.join(CONFIG_DIR, 'datasources.json');
const LEGACY_PATH = path.join(CONFIG_DIR, 'config.json');
const SEED_PATH   = path.join(CONFIG_DIR, 'sites-seed.json');

let _aiCache = null;
let _dsCache = null;

// ── Helpers ───────────────────────────────────────────────────────────────────

function readJSON(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function writeJSON(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + '\n', 'utf8');
}

// ── AI config ─────────────────────────────────────────────────────────────────

function getAiConfig() {
  if (_aiCache) return _aiCache;

  if (fs.existsSync(AI_PATH)) {
    _aiCache = readJSON(AI_PATH);
    return _aiCache;
  }

  if (fs.existsSync(LEGACY_PATH)) {
    const cfg  = readJSON(LEGACY_PATH);
    const data = cfg.ai || { activeProvider: 'gemini', providers: {} };
    // Absorb legacy top-level gemini key
    if (cfg.gemini && !(data.providers && data.providers.gemini)) {
      data.providers = data.providers || {};
      data.providers.gemini = cfg.gemini;
    }
    _aiCache = data;
    return _aiCache;
  }

  return { activeProvider: 'gemini', providers: {} };
}

function reloadAiConfig() {
  _aiCache = null;
}

// ── Datasources config ────────────────────────────────────────────────────────

function getDatasourcesConfig() {
  if (_dsCache) return _dsCache;

  if (fs.existsSync(DS_PATH)) {
    _dsCache = readJSON(DS_PATH);
    return _dsCache;
  }

  if (fs.existsSync(LEGACY_PATH)) {
    const cfg = readJSON(LEGACY_PATH);
    _dsCache = {
      server:    cfg.server    || { port: 3000, host: '0.0.0.0' },
      scheduler: cfg.scheduler || { enabled: true, services: {} },
      usgs: {
        parameters: (cfg.usgs && cfg.usgs.parameters) || ['00060', '00065', '00010', '00300', '00400'],
      },
      cwms: {
        baseUrl:      (cfg.cwms && cfg.cwms.baseUrl)       || 'https://cwms-data.usace.army.mil/cwms-data',
        office:       (cfg.cwms && cfg.cwms.office)        || 'LRN',
        lookbackHours:(cfg.cwms && cfg.cwms.lookbackHours) || 48,
      },
    };
    return _dsCache;
  }

  return {
    server:    { port: 3000, host: '0.0.0.0' },
    scheduler: { enabled: true, services: {} },
    usgs:      { parameters: ['00060', '00065', '00010', '00300', '00400'] },
    cwms:      { baseUrl: 'https://cwms-data.usace.army.mil/cwms-data', office: 'LRN', lookbackHours: 48 },
  };
}

function reloadDatasourcesConfig() {
  _dsCache = null;
}

// ── Write helpers (used by settings API) ─────────────────────────────────────

function saveAiConfig(data) {
  writeJSON(AI_PATH, data);
  reloadAiConfig();
}

function saveDatasourcesConfig(data) {
  writeJSON(DS_PATH, data);
  reloadDatasourcesConfig();
}

// ── Migration: config.json → split files + seed DB ───────────────────────────

function ensureMigrated(db) {
  // Already split — nothing to do
  if (fs.existsSync(AI_PATH) && fs.existsSync(DS_PATH)) return;

  if (!fs.existsSync(LEGACY_PATH)) return;

  let cfg;
  try { cfg = readJSON(LEGACY_PATH); }
  catch (e) { console.error('[Config] Failed to read legacy config.json:', e.message); return; }

  // Write ai.json
  if (!fs.existsSync(AI_PATH)) {
    const aiData = Object.assign({ activeProvider: 'gemini', providers: {} }, cfg.ai || {});
    if (cfg.gemini && !(aiData.providers && aiData.providers.gemini)) {
      aiData.providers = aiData.providers || {};
      aiData.providers.gemini = cfg.gemini;
    }
    writeJSON(AI_PATH, aiData);
    console.log('[Config] Wrote config/ai.json');
  }

  // Write datasources.json
  if (!fs.existsSync(DS_PATH)) {
    const dsData = {
      server:    cfg.server    || { port: 3000, host: '0.0.0.0' },
      scheduler: cfg.scheduler || { enabled: true, services: {} },
      usgs: {
        parameters: (cfg.usgs && cfg.usgs.parameters) || ['00060', '00065', '00010', '00300', '00400'],
      },
      cwms: {
        baseUrl:      (cfg.cwms && cfg.cwms.baseUrl)       || 'https://cwms-data.usace.army.mil/cwms-data',
        office:       (cfg.cwms && cfg.cwms.office)        || 'LRN',
        lookbackHours:(cfg.cwms && cfg.cwms.lookbackHours) || 48,
      },
    };
    writeJSON(DS_PATH, dsData);
    console.log('[Config] Wrote config/datasources.json');
  }

  // Seed sites from legacy config (only once)
  if (!db.getSeedApplied()) {
    let seeded = 0;
    if (cfg.usgs && Array.isArray(cfg.usgs.sites)) {
      for (const s of cfg.usgs.sites) {
        db.upsertSite({
          site_id: s.id, name: s.name, type: s.type || 'river',
          source: 'usgs', latitude: s.lat, longitude: s.lon,
        });
        seeded++;
      }
    }
    if (cfg.cwms && Array.isArray(cfg.cwms.sites)) {
      for (const s of cfg.cwms.sites) {
        db.upsertSite({
          site_id: s.locationId, name: s.name, type: s.type || 'dam',
          source: 'cwms', latitude: s.lat, longitude: s.lon,
        });
        if (s.timeseries && s.timeseries.length) {
          db.replaceSiteTimeseries(s.locationId, s.timeseries);
        }
        seeded++;
      }
    }
    db.markSeedApplied();
    console.log(`[Config] Seeded ${seeded} site(s) from legacy config.json into DB`);
  }
}

// ── Apply sites-seed.json (fresh install only) ────────────────────────────────

function applySeedFile(db) {
  if (db.getSeedApplied()) return;
  if (!fs.existsSync(SEED_PATH)) return;

  let sites;
  try { sites = readJSON(SEED_PATH); }
  catch (e) { console.warn('[Config] sites-seed.json parse error:', e.message); return; }

  if (!Array.isArray(sites)) {
    console.warn('[Config] sites-seed.json must be an array — skipping');
    return;
  }

  let seeded = 0;
  for (const s of sites) {
    const site_id = s.id || s.locationId || s.site_id;
    if (!site_id) continue;
    db.upsertSite({
      site_id,
      name:      s.name || site_id,
      type:      s.type || 'river',
      source:    s.source || 'usgs',
      latitude:  s.lat  !== undefined ? s.lat  : s.latitude,
      longitude: s.lon  !== undefined ? s.lon  : s.longitude,
      description: s.description || null,
    });
    if (s.timeseries && s.timeseries.length) {
      db.replaceSiteTimeseries(site_id, s.timeseries);
    }
    seeded++;
  }
  db.markSeedApplied();
  console.log(`[Config] Seeded ${seeded} site(s) from sites-seed.json`);
}

module.exports = {
  getAiConfig, reloadAiConfig, saveAiConfig,
  getDatasourcesConfig, reloadDatasourcesConfig, saveDatasourcesConfig,
  ensureMigrated, applySeedFile,
};
