const { GoogleGenerativeAI } = require('@google/generative-ai');
const crypto = require('crypto');
const db = require('../db/database');

let genAI = null;

function getClient() {
  if (!genAI) {
    const cfg = require('../../config/config.json');
    genAI = new GoogleGenerativeAI(cfg.gemini.apiKey);
  }
  return genAI;
}

// Role-specific system prompt prefixes
const ROLE_CONTEXT = {
  general: 'You are a helpful water resources assistant. Provide clear, accessible explanations.',
  data_scientist: `You are a hydrology data analyst. Emphasize statistical patterns, anomalies, confidence intervals,
    trend analysis, and data quality. Include relevant metrics and flag data gaps. Use technical terminology.`,
  vacationer: `You are a recreation safety advisor for water users. Focus on: is it safe, is it enjoyable,
    what should visitors know right now? Translate technical readings (cfs, pH, temp) into plain-English safety
    and experience guidance. Be direct: good/caution/avoid and why.`,
  eco_historian: `You are an ecological historian specializing in Pacific Northwest watersheds. Contextualize current
    readings against historical baselines, seasonal norms, and long-term ecological trends. Note how current conditions
    compare to pre-development baselines where relevant. Highlight ecological significance.`,
  analyst: `You are a water resources analyst producing decision-support summaries. Structure your response with
    key findings, risk factors, and recommended actions. Suitable for briefings to managers and planners.`,
  operator: `You are a dam and water operations engineer. Focus on operational implications: flow rates, storage levels,
    release schedules, flood risk windows, and infrastructure thresholds. Flag anything requiring immediate attention.`,
  regulator: `You are a water quality and flow compliance specialist. Reference relevant regulatory thresholds (Clean Water Act,
    Oregon DEQ standards). Flag any exceedances, near-misses, or trends approaching permit limits. Be precise.`,
};

const SUB_ROLE_ADDENDA = {
  kayaker:        'The user is a recreational kayaker. Focus on flow safety, wave/rapid conditions, and put-in access.',
  swimmer:        'The user is a swimmer. Prioritize E. coli advisories, temperature comfort, and turbidity.',
  fisher:         'The user is an angler. Highlight water temperature for fish activity, dissolved oxygen, and seasonal run timing.',
  hiker:          'The user is a trail/riverside hiker. Focus on bank stability, flood trail closures, and scenic conditions.',
  hydrologist:    'The user is a professional hydrologist. Include full parameter detail, uncertainty ranges, and sensor QA flags.',
  climatologist:  'The user is a climatologist. Emphasize precipitation-runoff relationships and seasonal anomalies.',
  ml_researcher:  'The user is an ML researcher. Discuss data density, gaps, stationarity, and feature engineering potential.',
  policy_analyst: 'The user is a policy analyst. Frame findings in terms of infrastructure investment, public risk, and governance.',
  dam_operator:   'The user operates reservoir infrastructure. Detail inflow/outflow balance, pool elevation, and spill risk.',
  epa:            'The user is EPA/DEQ staff. Note all parameter values relative to applicable standards and permit conditions.',
};

/**
 * Build a SHA-256 hash of a query string for cache keying.
 */
function hashQuery(str) {
  return crypto.createHash('sha256').update(str).digest('hex').slice(0, 16);
}

/**
 * Summarize recent data rows into a compact text block for the AI prompt.
 */
function buildDataContext(recentRows, sites) {
  if (!recentRows.length) return 'No recent monitoring data available.';

  const bySite = {};
  for (const row of recentRows) {
    if (!bySite[row.site_id]) bySite[row.site_id] = { name: row.site_name, params: {} };
    const key = row.parameter_code;
    if (!bySite[row.site_id].params[key]) bySite[row.site_id].params[key] = [];
    bySite[row.site_id].params[key].push(row);
  }

  let ctx = '--- CURRENT MONITORING DATA (Portland OR Region) ---\n';
  for (const [siteId, data] of Object.entries(bySite)) {
    ctx += `\nSite: ${data.name} (USGS ${siteId})\n`;
    for (const [param, rows] of Object.entries(data.params)) {
      rows.sort((a, b) => new Date(b.recorded_at) - new Date(a.recorded_at));
      const latest = rows[0];
      const oldest = rows[rows.length - 1];
      const trend = rows.length > 1
        ? (latest.value > oldest.value ? '↑ rising' : latest.value < oldest.value ? '↓ falling' : '→ stable')
        : '';
      ctx += `  ${latest.parameter_name}: ${latest.value} ${latest.unit}  ${trend}  (as of ${latest.recorded_at.slice(0,16)} UTC)\n`;
    }
  }
  ctx += '\n---\n';
  return ctx;
}

/**
 * Generate an AI insight.
 * @param {object} profile  User profile row from DB
 * @param {string} query    User's question or analysis request
 * @param {object} opts     { siteIds, useCache }
 */
async function generateInsight(profile, query, opts = {}) {
  const { siteIds = null, useCache = true } = opts;

  // Fetch recent data for context
  const allSites = db.getAllSites();
  const targetSiteIds = siteIds || allSites.map(s => s.site_id);
  const recentRows = targetSiteIds.length ? db.getRecentForAI(targetSiteIds, 72) : [];

  const dataContext = buildDataContext(recentRows, allSites);
  const cacheKey = hashQuery(`${profile.role}|${profile.sub_role}|${query}|${dataContext.slice(0, 200)}`);

  if (useCache) {
    const cached = db.getCachedInsight(cacheKey, 20);
    if (cached) return { response: cached.response, cached: true };
  }

  // Build system prompt
  const roleCtx = ROLE_CONTEXT[profile.role] || ROLE_CONTEXT.general;
  const subRoleCtx = SUB_ROLE_ADDENDA[profile.sub_role] || '';
  const userBio = profile.bio ? `\nUser context: ${profile.bio}` : '';
  const interests = profile.interests
    ? (() => { try { const arr = JSON.parse(profile.interests); return arr.length ? `\nUser interests: ${arr.join(', ')}` : ''; } catch { return ''; } })()
    : '';

  const systemPrompt = [
    roleCtx,
    subRoleCtx,
    userBio,
    interests,
    '\nBase your analysis strictly on the data provided. Be specific and actionable.',
    'If data is insufficient for a confident answer, say so and explain what additional data would help.',
    'Format your response with clear sections where appropriate. Keep it concise but thorough.',
  ].filter(Boolean).join('\n');

  const fullPrompt = `${dataContext}\n\nUser question/request: ${query}`;

  const cfg = require('../../config/config.json');
  const model = getClient().getGenerativeModel({
    model: cfg.gemini.model || 'gemini-2.0-flash',
    systemInstruction: systemPrompt,
  });

  const result = await model.generateContent(fullPrompt);
  const response = result.response.text();

  db.saveInsight({
    query_hash:   cacheKey,
    profile_role: profile.role,
    context_json: JSON.stringify({ siteIds: targetSiteIds, dataRows: recentRows.length }),
    prompt:       query,
    response,
  });

  return { response, cached: false };
}

module.exports = { generateInsight, buildDataContext };
