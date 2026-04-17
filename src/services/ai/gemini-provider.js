'use strict';

const { GoogleGenerativeAI } = require('@google/generative-ai');

/**
 * Gemini provider adapter.
 * Wraps @google/generative-ai to match the shared provider interface:
 *   generateText(systemPrompt, userPrompt)      → string
 *   generateJSON(systemPrompt, userPrompt)      → string (JSON)
 *   generateChatMessage(systemPrompt, history, message) → string
 *   history: [{role:'user'|'model', text:string}]
 */
function createProvider(apiKey, model) {
  if (!apiKey || apiKey === 'YOUR_GEMINI_API_KEY_HERE') {
    throw new Error('Gemini API key not configured. Add it to config.json under ai.providers.gemini.apiKey');
  }

  const client = new GoogleGenerativeAI(apiKey);
  const modelName = model || 'gemini-2.0-flash';

  return {
    name: 'gemini',
    model: modelName,

    async generateText(systemPrompt, userPrompt) {
      const m = client.getGenerativeModel({
        model: modelName,
        ...(systemPrompt ? { systemInstruction: systemPrompt } : {}),
      });
      const result = await m.generateContent(userPrompt);
      return result.response.text();
    },

    async generateJSON(systemPrompt, userPrompt) {
      const m = client.getGenerativeModel({
        model: modelName,
        ...(systemPrompt ? { systemInstruction: systemPrompt } : {}),
        generationConfig: { responseMimeType: 'application/json' },
      });
      const result = await m.generateContent(userPrompt);
      return result.response.text();
    },

    async generateChatMessage(systemPrompt, history, message) {
      const m = client.getGenerativeModel({
        model: modelName,
        ...(systemPrompt ? { systemInstruction: systemPrompt } : {}),
      });
      const geminiHistory = history.map(h => ({
        role: h.role,
        parts: [{ text: h.text }],
      }));
      const chat = m.startChat({ history: geminiHistory });
      const result = await chat.sendMessage(message);
      return result.response.text().trim();
    },
  };
}

module.exports = { createProvider };
