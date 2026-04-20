'use strict';

/**
 * AI provider factory.
 *
 * Reads config/config.json on every call (busting require cache) so settings
 * changes take effect immediately after resetProvider() is called.
 *
 * Config priority:
 *   1. cfg.ai.activeProvider + cfg.ai.providers[providerId]  (new structure)
 *   2. cfg.gemini                                             (legacy fallback)
 */

let _provider = null;
let _providerKey = null;

function getAIConfig() {
  const { getAiConfig, reloadAiConfig } = require('../config');
  reloadAiConfig();
  const cfg = getAiConfig();

  if (cfg.activeProvider && cfg.providers) {
    const id   = cfg.activeProvider;
    const prov = cfg.providers[id] || {};
    return {
      provider: id,
      type:     prov.type    || id,    // explicit type overrides id-as-type
      apiKey:   prov.apiKey,
      model:    prov.model,
      baseUrl:  prov.baseUrl || null,  // for openai-compatible endpoints
    };
  }

  // Legacy fallback
  return {
    provider: 'gemini',
    type:     'gemini',
    apiKey:   cfg.gemini && cfg.gemini.apiKey,
    model:    (cfg.gemini && cfg.gemini.model) || 'gemini-2.5-flash',
    baseUrl:  null,
  };
}

function getProvider() {
  const aiCfg = getAIConfig();
  const key = `${aiCfg.provider}|${aiCfg.apiKey}|${aiCfg.model}`;

  if (_provider && _providerKey === key) return _provider;

  const type = aiCfg.type || aiCfg.provider;

  if (type === 'gemini') {
    _provider = require('./gemini-provider').createProvider(aiCfg.apiKey, aiCfg.model);
  } else if (type === 'anthropic') {
    _provider = require('./anthropic-provider').createProvider(aiCfg.apiKey, aiCfg.model);
  } else if (type === 'openai' || type === 'openai-compatible') {
    _provider = require('./openai-provider').createProvider(aiCfg.apiKey, aiCfg.model, aiCfg.baseUrl);
  } else {
    throw new Error(
      `Unknown AI provider type "${type}". ` +
      `Supported: gemini, openai, anthropic. ` +
      `For other OpenAI-compatible APIs (Groq, Ollama, etc.) set "type":"openai" and "baseUrl" in the provider config.`
    );
  }

  _providerKey = key;
  return _provider;
}

function resetProvider() {
  _provider = null;
  _providerKey = null;
}

module.exports = { getProvider, resetProvider, getAIConfig };
