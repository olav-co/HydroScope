'use strict';

const express = require('express');
const router  = express.Router();

const {
  stopScheduler, startScheduler, reloadConfig,
  runFetch, runWeatherFetch, runTopologyDiscovery, runWaterwayTick, runCwmsFetch, runBasinSync,
  restartService, getRunningState, getAllServiceConfigs, SERVICE_META,
} = require('../../services/scheduler');
const { resetProvider } = require('../../services/ai/index');
const {
  getAiConfig, reloadAiConfig, saveAiConfig,
  getDatasourcesConfig, reloadDatasourcesConfig, saveDatasourcesConfig,
} = require('../../services/config');
const db = require('../../db/database');

// Known models per provider type — used as the picker list in the UI.
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

// ── GET /api/settings ─────────────────────────────────────────────────────────

router.get('/', (req, res) => {
  try {
    reloadAiConfig();
    reloadDatasourcesConfig();
    const aiCfg = getAiConfig();
    const dsCfg = getDatasourcesConfig();

    // ── AI providers ───────────────────────────────────────────────────────────
    let activeProvider = aiCfg.activeProvider || 'gemini';
    let activeModel    = null;

    const cfgProviders = aiCfg.providers ? { ...aiCfg.providers } : {};

    // Legacy gemini key in datasources/config
    if (!cfgProviders.gemini && aiCfg.gemini && aiCfg.gemini.apiKey) {
      cfgProviders.gemini = { apiKey: aiCfg.gemini.apiKey, model: aiCfg.gemini.model };
    }

    const PLACEHOLDER_RE = /^YOUR_.+_KEY_HERE$|^$/;
    const providerState  = {};

    for (const [id, pCfg] of Object.entries(cfgProviders)) {
      if (id.startsWith('_')) continue;           // skip example/comment entries
      const apiKey = (pCfg.apiKey || '').trim();
      if (PLACEHOLDER_RE.test(apiKey)) continue;  // skip unconfigured

      const type       = pCfg.type || id;
      const knownModels = PROVIDER_MODELS[type];
      const configModel = pCfg.model || (knownModels && knownModels[0].id) || 'unknown';
      const models = knownModels
        ? knownModels
        : [{ id: configModel, label: configModel }];

      const modelInList  = models.some(m => m.id === configModel);
      const finalModels  = modelInList
        ? models
        : [{ id: configModel, label: configModel + ' (configured)' }, ...models];

      providerState[id] = {
        model:   configModel,
        models:  finalModels,
        label:   pCfg.label || PROVIDER_LABELS[type] || id,
        baseUrl: pCfg.baseUrl || null,
      };
    }

    const configuredIds = Object.keys(providerState);
    if (!providerState[activeProvider] && configuredIds.length) {
      activeProvider = configuredIds[0];
    }
    if (providerState[activeProvider]) {
      activeModel = providerState[activeProvider].model;
    }

    // Overlay per-user AI preference (if user has saved one)
    if (req.user) {
      const userAi = db.getUserAiSettings(req.user.id);
      if (userAi) {
        if (userAi.provider && providerState[userAi.provider]) activeProvider = userAi.provider;
        if (userAi.model) activeModel = userAi.model;
      }
    }

    // ── Scheduler ──────────────────────────────────────────────────────────────
    const globalEnabled  = (dsCfg.scheduler && dsCfg.scheduler.enabled) !== false;
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
    reloadAiConfig();
    reloadDatasourcesConfig();
    const aiCfg = getAiConfig();
    const dsCfg = getDatasourcesConfig();
    const { scheduler, ai } = req.body;

    let aiChanged = false;
    let dsChanged = false;

    // ── AI settings ────────────────────────────────────────────────────────────
    if (ai) {
      if (!aiCfg.providers) aiCfg.providers = {};

      if (ai.activeProvider) {
        aiCfg.activeProvider = ai.activeProvider;
        aiChanged = true;
      }

      if (ai.providers) {
        for (const [id, pData] of Object.entries(ai.providers)) {
          if (pData.model && aiCfg.providers[id]) {
            aiCfg.providers[id].model = pData.model;
            aiChanged = true;
          }
        }
      }

      if (aiChanged) {
        saveAiConfig(aiCfg);
        // Also persist per-user preference
        if (req.user) {
          db.upsertUserAiSettings(req.user.id, {
            provider: ai.activeProvider || aiCfg.activeProvider,
            model: ai.providers
              ? (ai.providers[ai.activeProvider || aiCfg.activeProvider] || {}).model || activeModel
              : activeModel,
          });
        }
      }
    }

    // ── Scheduler settings ─────────────────────────────────────────────────────
    if (scheduler) {
      if (!dsCfg.scheduler) dsCfg.scheduler = {};

      if (scheduler.enabled !== undefined) {
        dsCfg.scheduler.enabled = !!scheduler.enabled;
        dsChanged = true;
      }

      if (scheduler.services) {
        if (!dsCfg.scheduler.services) dsCfg.scheduler.services = {};
        for (const [name, svcData] of Object.entries(scheduler.services)) {
          if (!SERVICE_META[name]) continue;
          if (!dsCfg.scheduler.services[name]) dsCfg.scheduler.services[name] = {};
          const target = dsCfg.scheduler.services[name];
          const key    = SERVICE_META[name].intervalKey;
          if (svcData.enabled !== undefined) { target.enabled = !!svcData.enabled; dsChanged = true; }
          if (svcData[key]    !== undefined) { target[key]    = svcData[key];       dsChanged = true; }
        }
      }

      if (dsChanged) saveDatasourcesConfig(dsCfg);
    }

    // ── Hot-reload ─────────────────────────────────────────────────────────────
    if (aiChanged)  resetProvider();
    if (dsChanged) {
      reloadConfig();
      stopScheduler();
      startScheduler();
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[Settings] PUT error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/settings/run/:service ──────────────────────────────────────────

router.post('/run/:service', async (req, res) => {
  const { service } = req.params;

  const runners = {
    usgs:        runFetch,
    weather:     runWeatherFetch,
    topology:    runTopologyDiscovery,
    waterways:   runWaterwayTick,
    cwms:        runCwmsFetch,
    basinSync: runBasinSync,
  };

  const fn = runners[service];
  if (!fn) return res.status(400).json({ error: `Unknown service: ${service}` });

  const state = getRunningState();
  if (state[service]) return res.json({ ok: false, skipped: true, reason: 'already running' });

  fn().catch(e => console.error(`[Settings] run/${service} error:`, e.message));
  res.json({ ok: true, started: true });
});

module.exports = router;
