'use strict';

/**
 * Anthropic (Claude) provider adapter.
 * Uses axios to call the Anthropic Messages API — no SDK needed.
 *
 * Shared provider interface:
 *   generateText(systemPrompt, userPrompt)               → string
 *   generateJSON(systemPrompt, userPrompt)               → string (JSON)
 *   generateChatMessage(systemPrompt, history, message)  → string
 *   history: [{role:'user'|'model', text:string}]
 */

const axios = require('axios');

const BASE_URL     = 'https://api.anthropic.com/v1';
const API_VERSION  = '2023-06-01';
const MAX_TOKENS   = 4096;

function createProvider(apiKey, model) {
  if (!apiKey) {
    throw new Error('Anthropic API key not configured. Add it to config.json under ai.providers.anthropic.apiKey');
  }

  const modelName = model || 'claude-3-5-haiku-20241022';
  const headers = {
    'x-api-key':         apiKey,
    'anthropic-version': API_VERSION,
    'content-type':      'application/json',
  };

  async function call(system, messages, extra = {}) {
    const body = { model: modelName, max_tokens: MAX_TOKENS, messages, ...extra };
    if (system) body.system = system;
    const resp = await axios.post(BASE_URL + '/messages', body, { headers });
    return resp.data.content[0].text;
  }

  return {
    name:  'anthropic',
    model: modelName,

    async generateText(systemPrompt, userPrompt) {
      return call(systemPrompt, [{ role: 'user', content: userPrompt }]);
    },

    async generateJSON(systemPrompt, userPrompt) {
      const sys = (systemPrompt ? systemPrompt + '\n' : '') +
        'Respond with only valid JSON. No prose, no markdown fences.';
      // Anthropic supports assistant prefill — seed with '{' to force JSON
      const text = await call(sys, [
        { role: 'user',      content: userPrompt },
        { role: 'assistant', content: '{' },
      ]);
      // The response is the continuation after '{', so prepend it back
      const raw = text.trim();
      return raw.startsWith('{') ? raw : '{' + raw;
    },

    async generateChatMessage(systemPrompt, history, message) {
      const messages = [];
      for (const h of history) {
        messages.push({ role: h.role === 'model' ? 'assistant' : h.role, content: h.text });
      }
      messages.push({ role: 'user', content: message });
      return (await call(systemPrompt, messages)).trim();
    },
  };
}

module.exports = { createProvider };
