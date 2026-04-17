'use strict';

const express = require('express');
const router  = express.Router();
const path    = require('path');
const fs      = require('fs');

const {
  stopScheduler, startScheduler, reloadConfig,
  runFetch, runWeatherFetch, runTopologyDiscovery, runWaterwayTick,
  restartService, getRunningState, getAllServiceConfigs, SERVICE_META,
} = require('../../services/scheduler');
const { resetProvider } = require('../../services/ai/index');
const db = require('../../db/database');

const CONFIG_PATH = path.join(__dirname, '../../../config/config.json');

// Known models per provider type — used as the picker list in the UI.
// Providers not listed here fall back to a single entry for whatever model is in config.
const PROVIDER_MODELS = {
  gemini: [
    { id: 'gemini-2.5-flash',      label: 'Gemini 2.5 Flash (recommended)' },
    { id: 'gemini-2.5-pro',        label: 'Gemini 2.5 Pro' },
    { id: 'gemini-2.0-flash',      label: 'Gemini 2.0 Flash' },
    { id: 'gemini-2.0-flash-lite', label: 'Gemini 2.0 Flash Lite' },
    { id: 'gemini-1.5-flash',      label: 'Gemini 1.5 Flash' },
    { id: 'gemini-1.5-pro',        label: 'Gemini 1.5 Pro' },
  ],
  openai: [
    { id: 'gpt-4o-mini',   label: 'GPT-4o Mini (recommended)' },
    { id: 'gpt-4o',        label: 'GPT-4o' },
    { id: 'gpt-4-turbo',   label: 'GPT-4 Turbo' },
    { id: 'gpt-3.5-turbo', label: 'GPT-3.5 Turbo' },
    { id: 'o4-mini',       label: 'o4 Mini' },
    { id: 'o3-mini',       label: 'o3 Mini' },
    { id: 'o1-mini',       label: 'o1 Mini' },
  ],
  anthropic: [
    { id: 'claude-opus-4-5',              label: 'Claude Opus 4.5' },
    { id: 'claude-sonnet-4-5',            label: 'Claude Sonnet 4.5 (recommended)' },
    { id: 'claude-haiku-4-5-20251001',    label: 'Claude Haiku 4.5' },
    { id: 'claude-3-5-sonnet-20241022',   label: 'Claude 3.5 Sonnet' },
    { id: 'claude-3-5-haiku-20241022',    label: 'Claude 3.5 Haiku' },
    { id: 'claude-3-opus-20240229',       label: 'Claude 3 Opus' },
  ],
};
const PROVIDER_LABELS = {
  gemini:    'Google Gemini',
  openai:    'OpenAI',
  anthropic: 'Anthropic Claude',
};

function readConfig() {
  const p = require.resolve('../../../config/config.json');
  if (require.cache[p]) delete require.cache[p];
  return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
}

// ── GET /api/settings ─────────────────────────────────────────────────────────

router.get('/', (req, res) => {
  try {
    const cfg = readConfig();

    // ── AI providers ───────────────────────────────────────────────────────────
    let activeProvider = 'gemini';
    let activeModel    = null;

    if (cfg.ai && cfg.ai.activeProvider) {
      activeProvider = cfg.ai.activeProvider;
      const pCfg = cfg.ai.providers && cfg.ai.providers[activeProvider];
      if (pCfg) activeModel = pCfg.model;
    } else if (cfg.gemini) {
      activeModel = cfg.gemini.model;
    }

    const PLACEHOLDER_RE = /^YOUR_.+_KEY_HERE$|^$/;
    const providerState  = {};

    // Walk every provider entry in config — no hard-coded list required
    const cfgProviders = (cfg.ai && cfg.ai.providers) ? cfg.ai.providers : {};

    // Also expose the legacy gemini key as a virtual provider entry
    if (!cfgProviders.gemini && cfg.gemini && cfg.gemini.apiKey) {
      cfgProviders.gemini = { apiKey: cfg.gemini.apiKey, model: cfg.gemini.model };
    }

    for (const [id, pCfg] of Object.entries(cfgProviders)) {
      const apiKey = (pCfg.apiKey || '').trim();
      if (PLACEHOLDER_RE.test(apiKey)) continue;   // skip unconfigured

      // Determine the effective type (explicit type field, or fall back to provider id)
      const type = pCfg.type || id;

      // Known model list for this type, or build a single-item list from what's in config
      const knownModels = PROVIDER_MODELS[type];
      const configModel = pCfg.model || (knownModels && knownModels[0].id) || 'unknown';
      const models = knownModels
        ? knownModels
        : [{ id: configModel, label: configModel }];

      // If the configured model isn't in the known list, prepend it
      const modelInList = models.some(m => m.id === configModel);
      const finalModels = modelInList
        ? models
        : [{ id: configModel, label: configModel + ' (configured)' }, ...models];

      providerState[id] = {
        model:   configModel,
        models:  finalModels,
        label:   pCfg.label || PROVIDER_LABELS[type] || pCfg.label || id,
        baseUrl: pCfg.baseUrl || null,
      };
    }

    const configuredIds = Object.keys(providerState);
    if (!providerState[activeProvider] && configuredIds.length) {
      activeProvider = configuredIds[0];
    }

    // ── Per-service scheduler config ───────────────────────────────────────────
    const globalEnabled = (cfg.scheduler && cfg.scheduler.enabled) !== false;
    const serviceConfigs = getAllServiceConfigs();

    // ── Status snapshot ────────────────────────────────────────────────────────
    let status = null;
    try { status = db.getSystemStatus(); } catch (_) {}

    res.json({
      scheduler: {
        enabled: globalEnabled,
        services: serviceConfigs,
      },
      ai: {
        activeProvider,
        activeModel,
        providers: providerState,
        providerIds: configuredIds,
      },
      status,
      runningState: getRunningState(),
    });
  } catch (err) {
    console.error('[Settings] GET error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── PUT /api/settings ─────────────────────────────────────────────────────────

router.put('/', (req, res) => {
  try {
    const cfg = readConfig();
    const { scheduler, ai } = req.body;

    // ── Global scheduler enabled ───────────────────────────────────────────────
    if (scheduler) {
      if (!cfg.scheduler) cfg.scheduler = {};

      if (scheduler.enabled !== undefined) {
        cfg.scheduler.enabled = !!scheduler.enabled;
      }

      // Legacy key — keep in sync for backwards compat
      if (scheduler.services && scheduler.services.usgs && scheduler.services.usgs.intervalMinutes) {
        cfg.scheduler.fetchIntervalMinutes = scheduler.services.usgs.intervalMinutes;
      }

      // Per-service configs
      if (scheduler.services) {
        if (!cfg.scheduler.services) cfg.scheduler.services = {};
        for (const [name, svcData] of Object.entries(scheduler.services)) {
          if (!SERVICE_META[name]) continue;
          if (!cfg.scheduler.services[name]) cfg.scheduler.services[name] = {};
          const target = cfg.scheduler.services[name];
          const key    = SERVICE_META[name].intervalKey;
          if (svcData.enabled !== undefined)   target.enabled  = !!svcData.enabled;
          if (svcData[key]    !== undefined)    target[key]     = svcData[key];
        }
      }
    }

    // ── AI settings ────────────────────────────────────────────────────────────
    if (ai) {
      if (!cfg.ai) cfg.ai = { providers: {} };
      if (!cfg.ai.providers) cfg.ai.providers = {};

      if (ai.activeProvider) cfg.ai.activeProvider = ai.activeProvider;

      if (ai.providers) {
        for (const [id, pData] of Object.entries(ai.providers)) {
          if (pData.model && cfg.ai.providers[id]) {
            cfg.ai.providers[id].model = pData.model;
          }
        }
      }

      // Sync legacy gemini key
      const gNew = cfg.ai.providers && cfg.ai.providers.gemini;
      if (gNew) {
        if (!cfg.gemini) cfg.gemini = {};
        if (gNew.model) cfg.gemini.model = gNew.model;
      }
    }

    // ── Write + hot-reload ─────────────────────────────────────────────────────
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2));

    resetProvider();
    reloadConfig();
    stopScheduler();
    startScheduler();

    res.json({ ok: true });
  } catch (err) {
    console.error('[Settings] PUT error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/settings/run/:service ──────────────────────────────────────────
// Manually trigger a single service run. Returns immediately after launch
// (the run is async; poll GET /api/settings for updated status).

router.post('/run/:service', async (req, res) => {
  const { service } = req.params;

  const runners = {
    usgs:      runFetch,
    weather:   runWeatherFetch,
    topology:  runTopologyDiscovery,
    waterways: runWaterwayTick,
  };

  const fn = runners[service];
  if (!fn) return res.status(400).json({ error: `Unknown service: ${service}` });

  // Check if already running
  const state = getRunningState();
  if (state[service]) return res.json({ ok: false, skipped: true, reason: 'already running' });

  // Fire and respond immediately — client polls for status update
  fn().catch(e => console.error(`[Settings] run/${service} error:`, e.message));

  res.json({ ok: true, started: true });
});

module.exports = router;
