'use strict';

/**
 * OpenAI provider adapter.
 * Uses axios (already a dependency) to call the OpenAI REST API —
 * no extra npm package needed.
 *
 * Shared provider interface:
 *   generateText(systemPrompt, userPrompt)      → string
 *   generateJSON(systemPrompt, userPrompt)      → string (JSON)
 *   generateChatMessage(systemPrompt, history, message) → string
 *   history: [{role:'user'|'model', text:string}]
 */

const axios = require('axios');

function createProvider(apiKey, model, baseUrl) {
  if (!apiKey) {
    throw new Error('OpenAI-compatible API key not configured. Add it to config.json under the provider\'s apiKey field.');
  }

  const BASE = (baseUrl || 'https://api.openai.com/v1').replace(/\/$/, '');
  const modelName = model || 'gpt-4o-mini';
  const headers = {
    Authorization: 'Bearer ' + apiKey,
    'Content-Type': 'application/json',
  };

  async function chat(messages, extra = {}) {
    const resp = await axios.post(
      BASE + '/chat/completions',
      { model: modelName, messages, ...extra },
      { headers }
    );
    return resp.data.choices[0].message.content;
  }

  return {
    name: 'openai',
    model: modelName,

    async generateText(systemPrompt, userPrompt) {
      const messages = [];
      if (systemPrompt) messages.push({ role: 'system', content: systemPrompt });
      messages.push({ role: 'user', content: userPrompt });
      return chat(messages);
    },

    async generateJSON(systemPrompt, userPrompt) {
      // OpenAI requires the word "json" to appear somewhere when using json_object mode.
      const sysContent = (systemPrompt ? systemPrompt + '\n' : '') +
        'Respond with only valid JSON. No prose, no markdown fences.';
      const messages = [
        { role: 'system', content: sysContent },
        { role: 'user', content: userPrompt },
      ];
      return chat(messages, { response_format: { type: 'json_object' } });
    },

    async generateChatMessage(systemPrompt, history, message) {
      const messages = [];
      if (systemPrompt) messages.push({ role: 'system', content: systemPrompt });
      for (const h of history) {
        messages.push({
          role: h.role === 'model' ? 'assistant' : h.role,
          content: h.text,
        });
      }
      messages.push({ role: 'user', content: message });
      const text = await chat(messages);
      return text.trim();
    },
  };
}

module.exports = { createProvider };
