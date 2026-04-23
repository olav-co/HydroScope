const crypto = require('crypto');
const db = require('../db/database');
const { buildWeatherContext, deriveWeatherLocations } = require('./weather');

// Returns true if at least one site coordinate is within 2 degrees of any
// active weather station. Weather stations are now derived from the configured
// sites themselves, so this guard prevents stale weather data (from a previous
// site configuration) from leaking into unrelated site contexts.
function _weatherRelevantForSites(siteLatLons) {
  const weatherLocs = deriveWeatherLocations();
  if (!weatherLocs.length) return false;
  for (const { lat, lon } of siteLatLons) {
    if (lat == null || lon == null) continue;
    for (const wl of weatherLocs) {
      const d = Math.sqrt(Math.pow(lat - wl.lat, 2) + Math.pow(lon - wl.lon, 2));
      if (d < 2) return true;
    }
  }
  return false;
}
const { getProvider } = require('./ai/index');
const { CWMS_PARAMETER_NAMES } = require('./cwms');

/**
 * Retry wrapper for transient AI API errors (rate-limits, 503s, timeouts).
 * Handles both Gemini and OpenAI error shapes.
 */
async function withRetry(fn, maxAttempts = 4, baseDelayMs = 2000) {
  var lastErr;
  for (var attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      var msg = (err.message || '');
      var httpStatus = (err.response && err.response.status) || 0;
      var isTransient = msg.includes('503') || msg.includes('429') ||
                        msg.includes('Service Unavailable') || msg.includes('Too Many Requests') ||
                        msg.includes('overloaded') || msg.includes('high demand') ||
                        msg.includes('rate_limit') || msg.includes('server_error') ||
                        msg.includes('ECONNRESET') || msg.includes('timeout') ||
                        httpStatus === 429 || httpStatus === 503;
      if (!isTransient || attempt === maxAttempts) throw err;
      var delay = baseDelayMs * Math.pow(2, attempt - 1);
      console.log('[AI] Transient error, retrying in ' + delay + 'ms (attempt ' + attempt + '/' + maxAttempts + ')');
      await new Promise(function(r) { setTimeout(r, delay); });
    }
  }
  throw lastErr;
}

// Role-specific system prompt prefixes
const ROLE_CONTEXT = {
  general: 'You are a helpful water resources assistant. Provide clear, accessible explanations.',
  data_scientist: `You are a hydrology data analyst. Emphasize statistical patterns, anomalies, confidence intervals,
    trend analysis, and data quality. Include relevant metrics and flag data gaps. Use technical terminology.`,
  vacationer: `You are a recreation safety advisor for water users. Focus on: is it safe, is it enjoyable,
    what should visitors know right now? Translate technical readings (cfs, pH, temp) into plain-English safety
    and experience guidance. Be direct: good/caution/avoid and why.`,
  eco_historian: `You are an ecological historian specializing in watershed ecology. Contextualize current
    readings against historical baselines, seasonal norms, and long-term ecological trends for the monitored region.
    Note how current conditions compare to pre-development baselines where relevant. Highlight ecological significance.`,
  analyst: `You are a water resources analyst producing decision-support summaries. Structure your response with
    key findings, risk factors, and recommended actions. Suitable for briefings to managers and planners.`,
  operator: `You are a dam and water operations engineer. Focus on operational implications: flow rates, storage levels,
    release schedules, flood risk windows, and infrastructure thresholds. Flag anything requiring immediate attention.`,
  regulator: `You are a water quality and flow compliance specialist. Reference relevant regulatory thresholds (Clean Water Act,
    applicable state and federal standards). Flag any exceedances, near-misses, or trends approaching permit limits. Be precise.`,
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
 * Build the combined sub-role context string for a profile.
 * sub_role may be a JSON array (new) or a bare string (legacy).
 */
function getSubRoleContext(profile) {
  let ids = profile.sub_role;
  if (!ids) return '';
  if (typeof ids === 'string') {
    try { ids = JSON.parse(ids); } catch (_) { ids = [ids]; }
  }
  if (!Array.isArray(ids)) ids = [ids];
  return ids
    .filter(Boolean)
    .map(id => SUB_ROLE_ADDENDA[id] || '')
    .filter(Boolean)
    .join(' ');
}

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

  // Build a lookup of site source (usgs|cwms) from the sites table
  const siteSourceMap = {};
  for (const s of (sites || [])) siteSourceMap[s.site_id] = s.source || 'usgs';

  const bySite = {};
  for (const row of recentRows) {
    if (!bySite[row.site_id]) bySite[row.site_id] = { name: row.site_name, source: row.source || siteSourceMap[row.site_id] || 'usgs', params: {} };
    const key = row.parameter_code;
    if (!bySite[row.site_id].params[key]) bySite[row.site_id].params[key] = [];
    bySite[row.site_id].params[key].push(row);
  }

  // Split into USGS and CWMS sections for clarity
  const usgsSites = Object.entries(bySite).filter(([, d]) => d.source !== 'cwms');
  const cwmsSites = Object.entries(bySite).filter(([, d]) => d.source === 'cwms');

  let ctx = '--- CURRENT MONITORING DATA ---\n';

  if (usgsSites.length) {
    ctx += '\n[USGS Stream Gauges]\n';
    for (const [siteId, data] of usgsSites) {
      ctx += `\nSite: ${data.name} (USGS ${siteId})\n`;
      for (const [param, rows] of Object.entries(data.params)) {
        rows.sort((a, b) => new Date(b.recorded_at) - new Date(a.recorded_at));
        const latest = rows[0];
        const oldest = rows[rows.length - 1];
        const trend = rows.length > 1
          ? (latest.value > oldest.value ? '↑ rising' : latest.value < oldest.value ? '↓ falling' : '→ stable')
          : '';
        ctx += `  ${latest.parameter_name}: ${latest.value} ${latest.unit}  ${trend}  (as of ${latest.recorded_at.slice(0,16)} UTC)\n`;
        if (rows.length > 1) {
          const vals = rows.map(r => r.value);
          const rMin = Math.min(...vals), rMax = Math.max(...vals);
          const rMean = vals.reduce((a, b) => a + b, 0) / vals.length;
          ctx += `    ↳ ${rows.length} readings (${oldest.recorded_at.slice(0,16)}–${latest.recorded_at.slice(0,16)} UTC): min=${rMin.toFixed(2)} max=${rMax.toFixed(2)} mean=${rMean.toFixed(2)} range=${(rMax - rMin).toFixed(2)} ${latest.unit}\n`;
        }
      }
    }
  }

  if (cwmsSites.length) {
    ctx += '\n[CWMS Dam Operations — US Army Corps of Engineers]\n';
    for (const [siteId, data] of cwmsSites) {
      ctx += `\nSite: ${data.name} (CWMS ${siteId})\n`;
      for (const [param, rows] of Object.entries(data.params)) {
        rows.sort((a, b) => new Date(b.recorded_at) - new Date(a.recorded_at));
        const latest = rows[0];
        const oldest = rows[rows.length - 1];
        const trend = rows.length > 1
          ? (latest.value > oldest.value ? '↑ rising' : latest.value < oldest.value ? '↓ falling' : '→ stable')
          : '';
        ctx += `  ${latest.parameter_name}: ${latest.value} ${latest.unit}  ${trend}  (as of ${latest.recorded_at.slice(0,16)} UTC)\n`;
        if (rows.length > 1) {
          const vals = rows.map(r => r.value);
          const rMin = Math.min(...vals), rMax = Math.max(...vals);
          const rMean = vals.reduce((a, b) => a + b, 0) / vals.length;
          ctx += `    ↳ ${rows.length} readings (${oldest.recorded_at.slice(0,16)}–${latest.recorded_at.slice(0,16)} UTC): min=${rMin.toFixed(2)} max=${rMax.toFixed(2)} mean=${rMean.toFixed(2)} range=${(rMax - rMin).toFixed(2)} ${latest.unit}\n`;
        }
      }
    }
  }

  ctx += '\n---\n';

  // Note co-located CWMS/USGS pairs so the AI doesn't treat them as independent signals
  try {
    const pairNotes = [];
    const siteList = sites || [];
    // A CWMS site and a USGS site sharing the same parent_site_id are a dam/tailwater pair
    const cwmsPaired = siteList.filter(s => s.source === 'cwms' && s.parent_site_id);
    for (const c of cwmsPaired) {
      const partner = siteList.find(s => s.source === 'usgs' && s.parent_site_id === c.parent_site_id);
      if (partner) {
        pairNotes.push(`"${c.name}" (CWMS ${c.site_id}) and "${partner.name}" (USGS ${partner.site_id}) are a co-located dam/tailwater pair — CWMS tracks reservoir operations (pool elevation, releases, storage), USGS measures the downstream flow response with a short lag.`);
      }
    }
    if (pairNotes.length) {
      ctx += '\n[CO-LOCATED SITE PAIRS]\n';
      for (const n of pairNotes) ctx += '• ' + n + '\n';
    }
  } catch (_) {}

  // Append weather context only when sites are geographically near the weather stations
  try {
    const weatherRows = db.getRecentWeather(72);
    if (weatherRows && weatherRows.length) {
      const siteLatLons = Object.keys(bySite).map(id => {
        const s = (sites || []).find(s => s.site_id === id);
        return s ? { lat: s.latitude, lon: s.longitude } : {};
      });
      if (_weatherRelevantForSites(siteLatLons)) {
        ctx += buildWeatherContext(weatherRows);
      }
    }
  } catch (_) {}

  return ctx;
}

/**
 * Build a detailed time-series context block for statistical analysis.
 * Used when the user is viewing a specific site+param on the visualize page.
 * Caps at 200 readings (samples evenly if more).
 */
function buildTimeSeriesContext(rows, siteId, siteName, paramCode, paramName) {
  if (!rows || !rows.length) return 'No time-series data available for this site/parameter.';

  var unit = rows[rows.length - 1].unit || '';
  var vals = rows.map(function(r) { return r.value; });

  // Sample if too many
  var displayRows = rows;
  if (rows.length > 200) {
    var step = Math.ceil(rows.length / 200);
    displayRows = rows.filter(function(_, i) { return i % step === 0; });
    // Always include last row
    if (displayRows[displayRows.length - 1] !== rows[rows.length - 1]) {
      displayRows.push(rows[rows.length - 1]);
    }
  }

  var min = Math.min.apply(null, vals);
  var max = Math.max.apply(null, vals);
  var sum = vals.reduce(function(a, b) { return a + b; }, 0);
  var mean = sum / vals.length;
  var variance = vals.reduce(function(a, v) { return a + Math.pow(v - mean, 2); }, 0) / vals.length;
  var stddev = Math.sqrt(variance);

  var ctx = '--- TIME-SERIES DATA: ' + (siteName || siteId) + ' — ' + (paramName || paramCode) + ' ---\n';
  ctx += 'Total readings: ' + vals.length + ' | Period: ' + rows[0].recorded_at.slice(0, 16) + ' → ' + rows[rows.length - 1].recorded_at.slice(0, 16) + ' UTC\n';
  ctx += 'Pre-computed stats (all ' + vals.length + ' readings): min=' + min.toFixed(3) + ' max=' + max.toFixed(3) + ' mean=' + mean.toFixed(3) + ' stddev=' + stddev.toFixed(3) + ' unit=' + unit + '\n';
  ctx += '\nTimestamped readings (UTC, value in ' + unit + '):\n';
  displayRows.forEach(function(r) {
    ctx += r.recorded_at.slice(0, 16) + '  ' + r.value + '\n';
  });
  ctx += '---\n';

  // Append weather context only when this site is near the weather stations
  try {
    var weatherRows = db.getRecentWeather(72);
    if (weatherRows && weatherRows.length) {
      var siteRow = db.getSiteById(siteId);
      var latLons = siteRow ? [{ lat: siteRow.latitude, lon: siteRow.longitude }] : [];
      if (_weatherRelevantForSites(latLons)) ctx += buildWeatherContext(weatherRows);
    }
  } catch (_) {}

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
  const cacheKey = hashQuery(`${profile.role}|${getSubRoleContext(profile)}|${query}|${dataContext.slice(0, 200)}`);

  if (useCache) {
    const cached = db.getCachedInsight(cacheKey, 20);
    if (cached) return { response: cached.response, cached: true };
  }

  // Build system prompt
  const roleCtx = ROLE_CONTEXT[profile.role] || ROLE_CONTEXT.general;
  const subRoleCtx = getSubRoleContext(profile) || '';
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

  const response = await withRetry(function() {
    return getProvider().generateText(systemPrompt, fullPrompt);
  });

  db.saveInsight({
    query_hash:   cacheKey,
    profile_role: profile.role,
    context_json: JSON.stringify({ siteIds: targetSiteIds, dataRows: recentRows.length }),
    prompt:       query,
    response,
  });

  return { response, cached: false };
}

// ── Chat Pipeline ─────────────────────────────────────────────────────────────
//
// Architecture:
//   generateChat()
//     └─ planQuery()      ← single AI call → structured JSON plan
//     └─ fulfillPlan()    ← assembles data from named sources
//     └─ generateResponse()  ← final AI generation with assembled context
//
// The planner replaces all prior heuristic stages:
//   isOnTopic / detectMissingContext / classifyQuery / checkDisambiguation

// ── Chat Modes ────────────────────────────────────────────────────────────────

const HYDROSCOPE_WIKI = `
HydroScope is a real-time hydrology monitoring dashboard. It tracks any combination of USGS stream gauges and US Army Corps of Engineers (CWMS) dam operations sites configured by the user — there is no fixed geographic region.

PAGES AND HOW TO USE THEM:

Dashboard (/):
  What it shows: All monitored sites at a glance. Latest readings for every parameter, color-coded status badges, AI-generated alerts.
  How to use: No setup needed — it loads automatically. Click any site card to jump to its detailed visualization. The AI Alerts section at the top summarizes current network conditions. Badges update every 15 minutes.

Data Visualization (/visualize):
  What it shows: A time-series chart for one site and one parameter over a chosen time window.
  How to use: Select a site from the dropdown, then a parameter (Discharge, Gage Height, etc.), then a time range (24h, 7d, 30d). The chart renders with AI-suggested threshold bands overlaid. Hover for exact values. The AI panel provides written interpretation and contextual thresholds.

Compare (/compare):
  What it shows: Multiple sites plotted together on one chart for the same parameter.
  How to use: Select a parameter first, then check which sites to include. All selected sites appear as colored lines. Useful for spotting lag between upstream and downstream sites, or comparing tributaries side by side.

Forecast (/forecast):
  What it shows: A 7-day weather and flow outlook combining Open-Meteo weather forecast with current stream trends.
  How to use: Select a site. The page shows precipitation forecast, temperature, and an AI-generated flow outlook. Confidence level is indicated. Useful for planning around expected high or low water.

Weather Data (/weather):
  What it shows: Raw hourly weather observations and forecast from Open-Meteo for the monitoring region(s).
  How to use: Browse past actuals and upcoming forecast. Parameters include precipitation, temperature, wind, and more. Weather locations are automatically derived from the configured monitoring sites.

Data Explorer (/data-explorer):
  What it shows: A table of all data series in the database — how many records, earliest and latest timestamps, average measurement interval.
  How to use: Use this to verify data is flowing and check for gaps. If a site shows 0 records or a stale latest timestamp, data collection may have failed.

Flow Network (/flow-network):
  What it shows: An auto-discovered stream network diagram. Each site is a node showing live readings. Arrows show upstream-to-downstream flow direction, derived from USGS drainage area and HUC watershed codes — not drawn manually.
  How to use: Select which sites to display using the chips at the top. Select which parameters to show on each node. The layout auto-arranges by watershed depth. Drag nodes to reposition. Click "Analyze Network" for an AI cascade analysis. Click "Discover Topology" to re-query the USGS watershed database.
  Reading the arrows: Line color = discharge trend (blue rising, orange falling, gray stable). Line thickness = volume. Labels show cfs, distance in km, and % of downstream flow.
  Reading the nodes: Border color is a unique identifier per site only — it does not indicate status.

Water Quality (/water-quality):
  What it shows: Regulatory permit limits per site and parameter, and whether current readings are in compliance.
  How to use: Click "Add Limit" to define a threshold (max, min, or target) for any site/parameter combination. Give it a label and color. Limits appear as reference lines on charts and trigger compliance badges.

Annotations (/annotations):
  What it shows: A log of notable events — dam releases, flood peaks, spills, maintenance windows.
  How to use: Click "Add Annotation" and select a site, category, label, and timestamp. Annotations appear as vertical markers on time-series charts.

Ask AI (/insights):
  What it shows: A full AI analysis interface with longer-form responses, analysis history, and site-specific forecasts.
  How to use: Type a detailed question or select a site for an automated forecast. Supports longer responses and saves your query history.

Profile (/profile):
  What it shows: Your user role and sub-role settings.
  How to use: Set your role (general, analyst, operator, regulator, etc.) and optionally a sub-role (kayaker, hydrologist, dam operator, etc.). This tunes the tone and focus of all AI responses throughout the app.

Settings (/settings):
  What it shows: System configuration — AI provider, model selection, and data fetch schedule.
  How to use: Select which AI provider to use (Gemini, OpenAI) and which model. Configure how often USGS stream data is fetched. Changes take effect immediately on save without restarting the server.

DATA SOURCES:
HydroScope integrates two independent data sources — both are fully active and complement each other:

USGS Stream Gauges (source=usgs):
- Discharge (cfs): Volume of water per second. Primary flood indicator.
- Gage Height (ft): Water surface elevation (stage). Flood stage varies by site.
- Water Temperature (°C): Fish habitat and recreation safety indicator.
- Dissolved Oxygen (mg/L): Below 5 mg/L stresses aquatic life.
- pH: Normal 6.5–8.5. Extremes indicate pollution.
- Updated every 15 minutes from USGS National Water Information System.

CWMS Dam Operations — US Army Corps of Engineers (source=cwms):
- Pool Elevation (ft): Reservoir water surface level above sea level.
- Tailwater Elevation (ft): Water surface immediately downstream of the dam.
- Inflow (cfs): Flow entering the reservoir from the upstream watershed.
- Outflow (cfs): Total water released from the dam (turbine + spillway combined).
- Turbine Flow (cfs): Hydropower generation discharge through turbines.
- Spillway Flow (cfs): Emergency/flood-control spill over or through spillways.
- Storage (ac-ft): Total reservoir volume stored. Conservation and flood pool breakdowns available.
- Flood Pool Used % (%): Fraction of flood control storage currently occupied.
- Power Generation (MWh): Hydroelectric energy produced (where available).
- Tailwater Water Quality: Temperature (°F), Dissolved Oxygen (mg/L), Conductance (µS/cm), pH — measured at dam outlet.
- Precipitation (in or mm): Precipitation at the dam site.
- Data from the public Corps CDA API (cwms-data.usace.army.mil), updated hourly to every 30 min.
- CWMS parameters use descriptive codes (Elev-Pool, Flow-In, Stor, etc.) not USGS numeric codes.

SITES: The monitored sites are whatever the user has configured. Site details (name, location, parameter availability) are provided in the live data context with each query.

STATUS COLORS (dashboard badges, site cards):
- Green (ok): Normal, within expected range.
- Yellow (watch): Elevated or below-normal — monitor.
- Orange (warning): Approaching alert threshold.
- Red (critical): Threshold exceeded, immediate attention.

FLOW NETWORK PAGE COLORS:
- Node border color: Each site is assigned a unique color from a fixed palette purely for visual identification. The color has NO meaning about status.
- Connection line color: Blue = upstream discharge is rising. Orange = falling. Gray = stable.
- Connection line thickness: Proportional to discharge volume (cfs).

DATA REFRESH: Stream data every 15 min (USGS). Weather hourly (Open-Meteo). Topology on server restart.

AI ROLES (Profile page): general, data_scientist, vacationer, eco_historian, analyst, operator, regulator.
`.trim();

const CHAT_MODE_CONFIGS = {
  general: {
    label: 'General',
    system: `You are a concise assistant embedded in a hydrology monitoring dashboard. Answer exactly what was asked — no preamble, no summary, no "great question". 1–3 sentences unless more detail is genuinely required. Only elaborate if the user explicitly asks. If answering requires knowing which sites or data are available, use what's provided in context.`,
    pullData: 'smart',   // pull data only if query is data-adjacent or about sites/basins
    useWiki: false,
  },
  wiki: {
    label: 'Guide',
    system: `You are the help system for HydroScope. Use the provided app knowledge base to answer questions — but reason across it, don't just quote sections. Combine what you know about the pages, data, sites, and features to construct a genuinely helpful answer even when the question doesn't map to one section directly. If a user asks how to do something, walk them through it using what you know. Only say something isn't covered if the question is truly unrelated to the app. Be concise and direct. No preamble.`,
    pullData: false,
    useWiki: true,
  },
  analyst: {
    label: 'Analyst',
    system: `You are a data analyst for a hydrology dashboard. Use the provided live measurements. Reference actual numbers and trends. Be specific — never generic. No preamble. If data for what was asked is missing, say so directly.
When a value is genuinely anomalous for the site and season — outside the range you would expect from your knowledge of these specific Pacific Northwest watersheds — note it concisely. If conditions are unremarkable, do not manufacture context.`,
    pullData: true,
    useWiki: false,
  },
  research: {
    label: 'Research',
    system: `You are a hydrology research assistant with deep knowledge of Pacific Northwest watersheds and the specific USGS monitoring sites in this network: Sandy River (glacier-fed, fast response), Willamette mainstem and tributaries, Clackamas (clean, fast), Tualatin (slow, lowland, summer DO/temp issues), Columbia below Bonneville (hydro-operation dominated), and Bull Run (managed, Portland water supply).
You know their historical flood records, seasonal flow regimes, typical discharge ranges by month, long-term drought periods, notable high-water events, and the regional climate drivers that affect them: atmospheric rivers, El Niño/La Niña cycles, Cascade snowpack and melt timing, rain-on-snow events, summer low-flow patterns.
Lead with what the live data shows. When the current conditions match or diverge from a meaningful historical pattern — a discharge that is high or low relative to seasonal norms, a temperature that signals an early snowmelt or late-season warmth, a precip pattern consistent with a known event type — draw on that knowledge to contextualize it. Be specific: cite approximate magnitudes, typical ranges, event types, or comparable seasons when you do.
Do not reach for historical context when conditions are ordinary. No preamble.`,
    pullData: true,
    useWiki: false,
  },
};

// ── Planner ───────────────────────────────────────────────────────────────────

// Only hard-block requests that have zero hydrology interpretation.
const BLOCK_PATTERNS = [
  /write (me )?(a |an )?(poem|song|story|essay|novel|lyrics|haiku|sonnet|script)/i,
  /generate (a |an )?(poem|story|image|picture|logo|app|website)/i,
  /\b(debug|refactor|fix) (my |this )?(code|function|script|bug|error)\b/i,
  /how (do i|to) (code|program|build|deploy|install) /i,
  /\bin (python|javascript|java|c\+\+|ruby|rust|go|php|swift|kotlin)\b/i,
  /\bsolve (this |the )?(math|equation|algebra)\b/i,
  /\btranslate (this |to )\b/i,
  /recommend (me )?(a |some )?(movie|tv show|show|book|song|restaurant|recipe)/i,
  /\btell me a joke\b/i,
  /\bplay (a game|chess|trivia)\b/i,
];

/**
 * Scan recent conversation history to surface implicit context.
 * Returns sites and params recently discussed — used to resolve
 * pronouns and follow-up references in the planner.
 */
function extractConversationContext(history, allSites) {
  if (!history || !history.length) return { site_ids: [], param_code: null };

  var recentText = history.slice(-6).map(function(h) { return h.text; }).join(' ').toLowerCase();

  var mentionedSiteIds = allSites.filter(function(s) {
    return recentText.includes(s.name.toLowerCase()) ||
           recentText.includes(s.site_id.toLowerCase());
  }).map(function(s) { return s.site_id; });

  // Light param detection from conversation text
  var paramCode = null;
  var paramHints = [
    { code: '00060', terms: ['discharge', 'cfs', 'flow', 'streamflow'] },
    { code: '00065', terms: ['gage height', 'gauge height', 'stage', 'level'] },
    { code: '00010', terms: ['temperature', 'temp', 'water temp'] },
    { code: '00300', terms: ['dissolved oxygen', ' do ', 'do level'] },
    { code: '00400', terms: [' ph ', 'ph level', 'acidity'] },
  ];
  for (var i = 0; i < paramHints.length; i++) {
    if (paramHints[i].terms.some(function(t) { return recentText.includes(t); })) {
      paramCode = paramHints[i].code;
      break;
    }
  }

  return { site_ids: mentionedSiteIds, param_code: paramCode };
}

/**
 * Local pre-classifier — ONLY handles hard blocks.
 * All routing decisions go to the AI planner.
 */
function localPlan(query) {
  if (BLOCK_PATTERNS.some(function(re) { return re.test(query); })) {
    return { mode: 'general', blocked: true, needs_clarification: null, data_sources: [], site_ids: null, hours: 48 };
  }
  return null;
}

/**
 * AI planner — smart routing with full conversation context.
 * Called for every non-blocked query.
 */
async function aiPlan(query, history, pageContext, profile) {
  var allSites = db.getAllSites();
  var siteList = allSites.map(function(s) { return s.site_id + '=' + s.name + ' (source:' + s.source + ')'; }).join(', ');

  // Page context
  var ctxParts = [];
  if (pageContext) {
    if (pageContext.current_page)  ctxParts.push('page=' + (pageContext.current_page_name || pageContext.current_page));
    if (pageContext.site_id)       ctxParts.push('site=' + (pageContext.site_name || pageContext.site_id));
    if (pageContext.param_code)    ctxParts.push('param=' + (pageContext.param_name || pageContext.param_code));
    if (pageContext.visible_hours) ctxParts.push('window=' + pageContext.visible_hours + 'h');
  }

  // Implicit context from conversation history — resolves pronouns and follow-ups
  var convCtx = extractConversationContext(history, allSites);
  var convCtxParts = [];
  if (convCtx.site_ids.length) {
    var convSiteNames = convCtx.site_ids.map(function(id) {
      var s = allSites.find(function(x) { return x.site_id === id; });
      return s ? s.name : id;
    });
    convCtxParts.push('recently discussed sites: ' + convSiteNames.join(', '));
  }
  if (convCtx.param_code) convCtxParts.push('recently discussed param: ' + convCtx.param_code);

  // Last 3 turns of actual conversation text for follow-up resolution
  var historySnippet = '';
  if (history.length) {
    historySnippet = history.slice(-3).map(function(h) {
      return (h.role === 'user' ? 'User' : 'Assistant') + ': ' + h.text.slice(0, 300);
    }).join('\n');
  }

  var PARAM_CODES = [
    'USGS: 00060=discharge(cfs), 00065=gage height(ft), 00010=water temp(°C), 00300=dissolved oxygen(mg/L), 00400=pH',
    'CWMS: ' + Object.entries(CWMS_PARAMETER_NAMES).slice(0, 12).map(function(e) { return e[0] + '=' + e[1]; }).join(', '),
  ].join(' | ');

  var prompt = [
    'You are the query planner for HydroScope, a real-time hydrology monitoring dashboard.',
    'Available sites: ' + siteList,
    'Parameter codes: ' + PARAM_CODES,
    'Page context: ' + (ctxParts.join(', ') || 'none'),
    convCtxParts.length ? 'Conversation context: ' + convCtxParts.join(', ') : '',
    historySnippet ? 'Recent conversation:\n' + historySnippet : '',
    'User role: ' + (profile.role || 'general'),
    '',
    'Current query: "' + query + '"',
    '',
    '## YOUR JOB',
    'Decide how to route and fulfill this query. DEFAULT TO ATTEMPTING AN ANSWER.',
    '',
    '## CRITICAL RULES',
    '1. FOLLOW-UP RESOLUTION: If this looks like a follow-up ("what about now?", "and the Sandy?", "is that normal?", "why?", "how does that compare?"), use the recent conversation to resolve the subject. Pronouns (it, that, there, this, the same) refer to what was just discussed.',
    '2. AMBIGUITY: When the query is short or vague but conversation/page context clarifies intent, USE that context — do not ask for clarification.',
    '3. DEFAULT ACTION: When in doubt, pull current_readings for the most relevant sites and attempt an answer. Never return empty data_sources for a hydrology question.',
    '4. CLARIFICATION: Only set needs_clarification if the query has absolutely no hydrology interpretation and no conversation context to draw from. It should be rare.',
    '',
    'Return ONLY valid JSON, no prose, no code fences:',
    '{',
    '  "mode": "analyst|wiki|research|general",',
    '  "blocked": false,',
    '  "needs_clarification": null,',
    '  "data_sources": [],',
    '  "site_ids": null,',
    '  "param_code": null,',
    '  "hours": 48',
    '}',
    '',
    'mode rules:',
    '  "analyst"  → live data, current conditions, stats, readings, trends, comparisons between sites',
    '  "research" → live data + historical context: is this normal, seasonal patterns, climate drivers, why is this happening, what to expect',
    '  "wiki"     → app UI questions: how to use a page, what does a color/icon mean, feature explanations',
    '  "general"  → greetings, thanks, small talk with no data angle',
    '',
    'data_sources (include all that apply):',
    '  "current_readings" → latest snapshot; use for current conditions, network-wide, or when site is unclear',
    '  "time_series"      → full history with stats for a specific site+param; use when stats, trends, or a time window are needed',
    '  "weather"          → precip/temp forecast; include whenever weather context would help',
    '  "wiki"             → app guide; only for UI/feature questions',
    '  "annotations"      → user event log; include when asking about events, releases, or anomalies',
    '',
    'site_ids: resolve from query + conversation context; null = all sites',
    'param_code: infer from query + conversation; null = default to discharge',
    'hours: explicit window if stated; 48 default, 168 weekly, 720 monthly',
  ].filter(Boolean).join('\n');

  try {
    var text = await withRetry(function() { return getProvider().generateJSON('', prompt); });
    text = text.trim().replace(/^```json\s*/i, '').replace(/\s*```$/i, '');
    var plan = JSON.parse(text);
    plan.mode         = plan.mode         || 'analyst';
    plan.blocked      = plan.blocked      || false;
    plan.data_sources = plan.data_sources || [];
    plan.hours        = plan.hours        || 48;
    plan.param_code   = plan.param_code   || null;
    // If planner returned no data sources for a non-wiki, non-general query, default to current_readings
    if (!plan.data_sources.length && plan.mode !== 'wiki' && plan.mode !== 'general') {
      plan.data_sources = ['current_readings', 'weather'];
    }
    // Enrich site_ids with conversation context when planner left it null
    if (!plan.site_ids && convCtx.site_ids.length && plan.mode !== 'wiki') {
      plan.site_ids = convCtx.site_ids;
    }
    // Same for param_code
    if (!plan.param_code && convCtx.param_code) {
      plan.param_code = convCtx.param_code;
    }
    return plan;
  } catch (_) {
    // Safe fallback — attempt with current readings rather than returning nothing
    return {
      mode: 'analyst', blocked: false, needs_clarification: null,
      data_sources: ['current_readings', 'weather'],
      site_ids: convCtx.site_ids.length ? convCtx.site_ids : null,
      param_code: convCtx.param_code, hours: 48,
    };
  }
}

/**
 * Route a query: block check first, then AI planner.
 */
async function planQuery(query, history, pageContext, profile) {
  var fast = localPlan(query);
  if (fast) return fast;
  return await aiPlan(query, history, pageContext, profile);
}

// ── Data Fulfillment ──────────────────────────────────────────────────────────

async function fulfillPlan(plan, pageContext) {
  var allSites = db.getAllSites();
  var blocks = [];

  for (var i = 0; i < plan.data_sources.length; i++) {
    var source = plan.data_sources[i];

    if (source === 'wiki') {
      var siteList = allSites.map(function(s) { return '- ' + s.name + ' (USGS ' + s.site_id + ')'; }).join('\n');
      blocks.push('--- HYDROSCOPE APP GUIDE ---\n' + HYDROSCOPE_WIKI + '\n\nCURRENTLY CONFIGURED SITES:\n' + siteList + '\n---');

    } else if (source === 'current_readings') {
      var siteIds = plan.site_ids || allSites.map(function(s) { return s.site_id; });
      var rows = siteIds.length ? db.getRecentForAI(siteIds, plan.hours || 48) : [];
      if (rows.length) {
        blocks.push(buildDataContext(rows, allSites));
      } else {
        var names = allSites.map(function(s) { return s.name + ' (USGS ' + s.site_id + ')'; }).join(', ');
        blocks.push('Configured monitoring sites: ' + names + '\n(No recent readings in database yet.)');
      }

    } else if (source === 'time_series') {
      // Resolve site: prefer page context (user is looking at it), fall back to planner-identified site
      var tsSiteId    = (pageContext && pageContext.site_id)    || (plan.site_ids && plan.site_ids[0]) || null;
      // Resolve param: prefer page context, then planner-identified param, then default to discharge
      var tsParamCode = (pageContext && pageContext.param_code) || plan.param_code || '00060';
      var tsHours     = parseInt((pageContext && pageContext.visible_hours)) || plan.hours || 168;
      if (tsSiteId) {
        var tsSite     = allSites.find(function(s) { return s.site_id === tsSiteId; });
        var tsSiteName = (pageContext && pageContext.site_name) || (tsSite && tsSite.name) || tsSiteId;
        var tsParamName = (pageContext && pageContext.param_name) || null;
        var tsRows = db.getTimeSeriesForSite(tsSiteId, tsParamCode, tsHours);
        if (tsRows.length) {
          blocks.push(buildTimeSeriesContext(tsRows, tsSiteId, tsSiteName, tsParamCode, tsParamName));
        }
      }

    } else if (source === 'weather') {
      try {
        var weatherRows = db.getRecentWeather(Math.max(plan.hours || 72, 72));
        if (weatherRows && weatherRows.length) {
          var chatSiteIds = plan.site_ids || (pageContext && pageContext.site_id ? [pageContext.site_id] : []);
          var chatLatLons = chatSiteIds.map(function(id) {
            var s = allSites.find(function(s) { return s.site_id === id; });
            return s ? { lat: s.latitude, lon: s.longitude } : {};
          });
          if (_weatherRelevantForSites(chatLatLons)) blocks.push(buildWeatherContext(weatherRows));
        }
      } catch (_) {}

    } else if (source === 'annotations') {
      try {
        var annoFilter = {};
        if (plan.site_ids && plan.site_ids.length) annoFilter.siteId = plan.site_ids[0];
        var cutoff = new Date(Date.now() - (plan.hours || 168) * 3600 * 1000).toISOString();
        annoFilter.startAt = cutoff;
        var annotations = db.getAnnotations(annoFilter);
        if (annotations && annotations.length) {
          var aBlock = '--- ANNOTATIONS (recent events) ---\n';
          annotations.forEach(function(a) {
            aBlock += '[' + (a.recorded_at || a.created_at || '').slice(0, 16) + '] '
              + (a.site_name || a.site_id || 'All sites') + ' — '
              + (a.category || '') + ': ' + (a.label || '') + '\n';
          });
          blocks.push(aBlock + '---');
        }
      } catch (_) {}
    }
  }

  // For non-wiki modes with a known current page, always inject that page's KB section
  if (plan.mode !== 'wiki' && pageContext && pageContext.current_page_name) {
    var kbPageName = pageContext.current_page_name;
    var wikiLines = HYDROSCOPE_WIKI.split('\n');
    var sectionLines = [];
    var capturing = false;
    for (var ki = 0; ki < wikiLines.length; ki++) {
      var kline = wikiLines[ki];
      if (!capturing && kline.startsWith(kbPageName + ' (')) { capturing = true; }
      else if (capturing && /^[A-Z][A-Za-z ]+ \(\//.test(kline)) { break; }
      if (capturing) sectionLines.push(kline);
    }
    if (sectionLines.length) {
      blocks.unshift('--- CURRENT PAGE: ' + kbPageName + ' ---\n' + sectionLines.join('\n') + '\n---');
    }
  }

  return blocks.join('\n\n');
}

// ── Chat ──────────────────────────────────────────────────────────────────────

/**
 * Main chat entry point.
 *
 * @param {object} profile
 * @param {Array}  history       [{role:'user'|'model', text:'...'}]
 * @param {string} query         latest user message
 * @param {object} pageContext
 * @param {string} requestedMode suggested mode from client
 */
async function generateChat(profile, history, query, pageContext, requestedMode) {
  // ── Step 1: Plan ────────────────────────────────────────────────────────────
  var plan = await planQuery(query, history, pageContext, profile);

  if (plan.blocked) {
    return { blocked: true, mode: plan.mode };
  }

  // If the planner wants clarification but we have history or page context,
  // attempt an answer anyway — the context is enough to make a reasonable interpretation.
  // Only hard-stop clarification when there is genuinely nothing to work with.
  if (plan.needs_clarification) {
    var hasEnoughContext = history.length > 0 || (pageContext && pageContext.current_page);
    if (hasEnoughContext) {
      // Proceed — note the interpretation assumption in the prompt
      plan.needs_clarification = null;
      if (!plan.data_sources.length) plan.data_sources = ['current_readings', 'weather'];
      if (plan.mode === 'general') plan.mode = 'analyst';
    } else {
      // Return needs_context so the client can ask the question, store the original query,
      // and fold the user's answer back into it on the next request.
      return { needs_context: true, question: plan.needs_clarification, mode: plan.mode, modeChanged: plan.mode !== requestedMode };
    }
  }

  // ── Step 2: Fulfill ─────────────────────────────────────────────────────────
  var dataCtxBlock = await fulfillPlan(plan, pageContext);
  var dataIncluded = plan.data_sources.includes('current_readings') || plan.data_sources.includes('time_series');
  var mode = plan.mode;
  var modeChanged = mode !== requestedMode;

  // ── Step 3: Generate ────────────────────────────────────────────────────────
  var modeConfig = CHAT_MODE_CONFIGS[mode] || CHAT_MODE_CONFIGS.general;

  var roleCtx = (profile.role && profile.role !== 'general') ? (ROLE_CONTEXT[profile.role] || '') : '';
  var subCtx  = getSubRoleContext(profile) || '';
  var bioCtx  = profile.bio ? 'User context: ' + profile.bio : '';

  var pageCtxLine = '';
  if (pageContext) {
    var ctxPageName = pageContext.current_page_name || pageContext.current_page || null;
    var parts = [];
    if (ctxPageName)               parts.push('"' + ctxPageName + '" page');
    if (pageContext.site_name)     parts.push('site: ' + pageContext.site_name);
    if (pageContext.param_name)    parts.push('parameter: ' + pageContext.param_name);
    if (pageContext.visible_hours) parts.push('showing last ' + pageContext.visible_hours + 'h');
    if (parts.length) {
      if (mode === 'wiki' && ctxPageName) {
        pageCtxLine = 'The user is on the "' + ctxPageName + '" page of HydroScope. Use the KB section for that page as your primary source when answering. If the query is generic ("what is this", "explain this"), describe that specific page.';
      } else {
        pageCtxLine = 'The user is currently on the ' + parts.join(', ') + ' of HydroScope.';
      }
    }
  }

  // Conversation context note — helps the AI resolve any remaining pronoun/reference ambiguity
  var allSites = db.getAllSites();
  var convCtx = extractConversationContext(history, allSites);
  var convCtxNote = '';
  if (convCtx.site_ids.length) {
    var convNames = convCtx.site_ids.map(function(id) {
      var s = allSites.find(function(x) { return x.site_id === id; });
      return s ? s.name : id;
    });
    convCtxNote = 'Recently discussed in conversation: ' + convNames.join(', ') + '. Treat ambiguous references ("it", "that site", "there", "the same") as referring to these unless otherwise stated.';
  }

  var systemParts = [
    modeConfig.system,
    roleCtx,
    subCtx,
    bioCtx,
    pageCtxLine,
    convCtxNote,
    'Never start with "Great question", "Certainly", "Of course", or any filler. Never repeat the question. Never use headers.',
    dataIncluded ? 'Live monitoring data is provided — reference actual values, not generics.' : '',
  ].filter(Boolean).join('\n');

  var userMessage = dataCtxBlock ? dataCtxBlock + '\n\n' + query : query;

  var response = await withRetry(function() {
    return getProvider().generateChatMessage(systemParts, history, userMessage);
  });

  var cacheKey = hashQuery('chat|' + mode + '|' + profile.role + '|' + ((pageContext && pageContext.current_page) ? pageContext.current_page + '|' : '') + query.slice(0, 120));
  db.saveInsight({
    query_hash:   cacheKey,
    profile_role: profile.role,
    context_json: JSON.stringify({ mode, dataIncluded, pageContext: pageContext || null }),
    prompt:       query,
    response,
  });

  return { response, dataIncluded, mode, modeChanged };
}

// ── Page Enrichments (structured JSON) ───────────────────────────────────────

const ENRICH_SCHEMAS = {
  thresholds: `{
  "thresholds": [
    {
      "id": "unique_snake_case_id",
      "label": "Short display label (max 30 chars)",
      "value": <number expressed in the EXACT SAME UNIT as the parameter — e.g. ft for gage height, ft³/s for discharge, °C for water temp>,
      "color": "#hexcolor",
      "description": "Why this threshold matters for this location",
      "type": "min_advisory|max_advisory|caution|target|regulatory"
    }
  ],
  "insights": [
    {
      "label": "Short label (max 30 chars)",
      "note": "Relevant contextual information that does NOT map to this parameter's numeric scale — e.g. upstream precip, snowmelt conditions, ecological context"
    }
  ],
  "note": "Brief disclaimer (source uncertainty, site-specific factors)"
}

CRITICAL RULES:
- thresholds[] must ONLY contain values in the same unit as the parameter. If the parameter is gage height (ft), every threshold value must be in feet. If discharge (ft³/s), values must be in ft³/s. Never mix units.
- Do NOT put a temperature value on a discharge or gage height chart. Do NOT put a precipitation value on a temperature chart.
- If you have relevant context that uses a different unit or scale (e.g. snowmelt temp thresholds when viewing gage height), put it in insights[] as a text note, not in thresholds[].
- insights[] is for contextual intelligence — upstream conditions, weather drivers, ecological flags — that is genuinely useful but cannot be plotted on this axis.`,
  dashboard: `{
  "status_line": "one sentence overall status",
  "alerts": [{ "level": "info|watch|warning|critical", "message": "..." }],
  "site_notes": {
    "SITE_ID": {
      "level": "ok|watch|warning|critical",
      "badge": "short label or null",
      "note": "1-2 sentence observation for this site",
      "metric_context": "e.g. '35% above 30-day average' or null"
    }
  }
}`,
  visualize: `{
  "trend": "rising|falling|stable|variable",
  "trend_label": "short description of trend direction and duration",
  "key_observation": "most notable data feature",
  "interpretation": "what this pattern likely means (cause/context)",
  "profile_insight": "what this means specifically for the user's role",
  "anomalies": [{ "note": "brief anomaly description" }],
  "recommendation": "actionable next step or watch item"
}`,
  compare: `{
  "summary": "one sentence comparing the sites",
  "key_finding": "most significant difference or similarity and likely cause",
  "outlier": "which site stands out and why, or null if none",
  "profile_insight": "what the comparison reveals for the user's role",
  "recommendation": "suggested action or focus area"
}`,
  forecast: `{
  "outlook": "rising|falling|stable|variable",
  "outlook_label": "short outlook summary",
  "near_term": "conditions expected in next 24-48 hours",
  "extended": "conditions expected over the full forecast window",
  "confidence": "high|moderate|low",
  "confidence_reason": "why confidence is at this level",
  "profile_note": "specific implication for the user's role",
  "watch_for": "key threshold or event to monitor"
}`
};

/**
 * Generate structured page enrichments via AI JSON mode.
 * Returns parsed JS object. Falls back to { _raw: text } on parse failure.
 */
async function generateEnrichment(profile, pageType, contextData) {
  const schema = ENRICH_SCHEMAS[pageType];
  if (!schema) throw new Error('Unknown enrichment page type: ' + pageType);

  // Build data context
  var recentRows = [];
  var allSites = db.getAllSites();

  if (pageType === 'thresholds') {
    if (contextData && contextData.site_id && contextData.param_code) {
      recentRows = db.getTimeSeriesForSite(contextData.site_id, contextData.param_code, 168)
        .slice(-50)
        .map(function(r) {
          return {
            site_id: contextData.site_id, site_name: contextData.site_name || contextData.site_id,
            parameter_code: contextData.param_code, parameter_name: contextData.param_name || contextData.param_code,
            value: r.value, unit: r.unit, recorded_at: r.recorded_at
          };
        });
    }
  } else if (pageType === 'dashboard') {
    var ids = allSites.map(function(s) { return s.site_id; });
    recentRows = ids.length ? db.getRecentForAI(ids, 48) : [];
  } else if (contextData && contextData.site_ids) {
    recentRows = db.getRecentForAI(contextData.site_ids, 72);
  } else if (contextData && contextData.site_id) {
    var hours = pageType === 'visualize' ? (contextData.hours || 168) : 72;
    if (contextData.param_code) {
      recentRows = db.getTimeSeriesForSite(contextData.site_id, contextData.param_code, hours)
        .map(function(r) {
          return {
            site_id: contextData.site_id,
            site_name: contextData.site_name || contextData.site_id,
            parameter_code: contextData.param_code,
            parameter_name: contextData.param_name || contextData.param_code,
            value: r.value,
            unit: r.unit,
            recorded_at: r.recorded_at
          };
        });
    } else {
      recentRows = db.getRecentForAI([contextData.site_id], hours);
    }
  }

  var dataCtx = buildDataContext(recentRows, allSites);

  var cacheKey = hashQuery('enrich|' + pageType + '|' + profile.role + '|' + JSON.stringify(contextData || {}).slice(0, 100) + '|' + dataCtx.slice(0, 150));
  var cached = db.getCachedInsight(cacheKey, 15);
  if (cached) {
    try { return { data: JSON.parse(cached.response), cached: true }; } catch (_) {}
  }

  var roleCtx = ROLE_CONTEXT[profile.role] || ROLE_CONTEXT.general;
  var subCtx = getSubRoleContext(profile) || '';
  var bioCtx = profile.bio ? 'User context: ' + profile.bio : '';

  var systemPrompt = [
    roleCtx, subCtx, bioCtx,
    'You analyze water monitoring data and return ONLY valid JSON. No prose, no markdown, no code fences.',
    'Be specific and data-driven. If data is insufficient, reflect that in confidence/notes.'
  ].filter(Boolean).join('\n');

  var userPrompt = dataCtx + '\n\nReturn JSON matching this exact schema:\n' + schema;

  var text = await withRetry(function() {
    return getProvider().generateJSON(systemPrompt, userPrompt);
  });

  db.saveInsight({
    query_hash: cacheKey,
    profile_role: profile.role,
    context_json: JSON.stringify(contextData || {}),
    prompt: pageType + ' enrichment',
    response: text
  });

  try {
    return { data: JSON.parse(text), cached: false };
  } catch (_) {
    return { data: { _raw: text }, cached: false };
  }
}

// ── Flow Network Analysis ─────────────────────────────────────────────────────

const NETWORK_SCHEMA = `{
  "network_summary": "1-2 sentence overall state of the monitoring network",
  "flow_status": "normal|elevated|flood_risk|drought|variable",
  "implications": [
    {
      "from_site": "upstream site name",
      "to_site": "downstream site name",
      "finding": "What is happening at the upstream site and what impact is likely at the downstream site. Be specific — use actual values and trends.",
      "lag_estimate": "Estimated signal propagation time (e.g. '2-4 hours'), or null if unknown",
      "severity": "info|watch|warning|critical"
    }
  ],
  "site_notes": [
    {
      "site": "site name",
      "note": "Notable condition at this site not covered by an implication"
    }
  ],
  "recommendation": "Most important action or watch item for the operator/analyst right now"
}`;

/**
 * Analyze a flow network topology and produce cascade implication findings.
 */
async function generateNetworkAnalysis(profile, nodes, edges, hours) {
  if (!nodes.length) throw new Error('No nodes provided.');

  var topo = '--- FLOW NETWORK TOPOLOGY ---\n';
  if (!edges.length) {
    topo += 'No connections defined between selected sites.\n';
  } else {
    edges.forEach(function(e) {
      topo += e.from_site_id + ' → ' + e.to_site_id + ' (' + (e.label || 'flows into') + ')\n';
    });
  }
  topo += '\n--- CURRENT READINGS (last ' + hours + ' hours) ---\n';
  nodes.forEach(function(n) {
    var sourceLabel = n.source === 'cwms' ? 'CWMS' : 'USGS';
    topo += '\nSite: ' + n.site_name + ' (' + sourceLabel + ' ' + n.site_id + ')\n';
    if (!n.readings.length) {
      topo += '  No data available for selected parameters.\n';
    } else {
      n.readings.forEach(function(r) {
        var trendStr = r.trend === 'rising' ? '↑ rising' : r.trend === 'falling' ? '↓ falling' : '→ stable';
        topo += '  ' + r.param_name + ': ' + r.value.toFixed(2) + ' ' + r.unit + '  ' + trendStr + '\n';
      });
    }
  });

  var roleCtx = ROLE_CONTEXT[profile.role] || ROLE_CONTEXT.general;
  var subCtx  = getSubRoleContext(profile) || '';

  var systemPrompt = [
    roleCtx, subCtx,
    'You analyze hydrological monitoring networks and trace cascade effects between upstream and downstream sites.',
    'You return ONLY valid JSON. No prose, no markdown, no code fences.',
    'Base implications on actual readings — cite specific values and trends.',
    'If a connection is shown, always produce an implication for it even if conditions are normal.',
  ].filter(Boolean).join('\n');

  var userPrompt = topo + '\n\nReturn JSON matching this exact schema:\n' + NETWORK_SCHEMA;

  var cacheKey = hashQuery('network|' + profile.role + '|' + topo.slice(0, 200));
  var cached = db.getCachedInsight(cacheKey, 10);
  if (cached) {
    try { return { data: JSON.parse(cached.response), cached: true }; } catch(_) {}
  }

  var text = await withRetry(function() {
    return getProvider().generateJSON(systemPrompt, userPrompt);
  });

  db.saveInsight({
    query_hash:   cacheKey,
    profile_role: profile.role,
    context_json: JSON.stringify({ nodes: nodes.map(n => n.site_id), edges }),
    prompt:       'flow network analysis',
    response:     text
  });

  try {
    return { data: JSON.parse(text), cached: false };
  } catch(_) {
    return { data: { _raw: text }, cached: false };
  }
}

module.exports = { generateInsight, generateEnrichment, generateChat, generateNetworkAnalysis, buildDataContext };
