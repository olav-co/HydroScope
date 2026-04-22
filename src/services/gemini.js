const crypto = require('crypto');
const db = require('../db/database');
const { buildWeatherContext } = require('./weather');
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

  // Append weather context from Open-Meteo if available
  try {
    const weatherRows = db.getRecentWeather(72);
    if (weatherRows && weatherRows.length) {
      ctx += buildWeatherContext(weatherRows);
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

  // Also append weather context if available
  try {
    var weatherRows = db.getRecentWeather(72);
    if (weatherRows && weatherRows.length) ctx += buildWeatherContext(weatherRows);
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
HydroScope is a real-time hydrology dashboard for the Portland, Oregon region.

PAGES AND HOW TO USE THEM:

Dashboard (/):
  What it shows: All monitored sites at a glance. Latest readings for every parameter, color-coded status badges, AI-generated alerts.
  How to use: No setup needed — it loads automatically. Click any site card to jump to its detailed visualization. The AI Alerts section at the top summarizes current network conditions. Badges update every 15 minutes.

Data Visualization (/visualize):
  What it shows: A time-series chart for one site and one parameter over a chosen time window.
  How to use: Select a site from the dropdown, then a parameter (Discharge, Gage Height, etc.), then a time range (24h, 7d, 30d). The chart renders with AI-suggested threshold bands overlaid. Scroll the chart to zoom; hover for exact values. Click "AI Analysis" for a written interpretation of the current trend.

Compare (/compare):
  What it shows: Multiple sites plotted together on one chart for the same parameter.
  How to use: Select a parameter first, then check which sites to include. All selected sites appear as colored lines. Useful for spotting lag between upstream and downstream sites, or comparing tributaries side by side.

Forecast (/forecast):
  What it shows: A 7-day weather and flow outlook combining Open-Meteo weather forecast with current stream trends.
  How to use: Select a site. The page shows precipitation forecast, temperature, and an AI-generated flow outlook. Confidence level is indicated. Useful for planning around expected high or low water.

Weather Data (/weather):
  What it shows: Raw hourly weather observations and forecast from Open-Meteo for the monitoring region.
  How to use: Browse past actuals and upcoming forecast. Parameters include precipitation, temperature, wind, and more. This data is also used as context in AI analysis on other pages.

Data Explorer (/data-explorer):
  What it shows: A table of all data series in the database — how many records, earliest and latest timestamps, average measurement interval.
  How to use: Use this to verify data is flowing and check for gaps. If a site shows 0 records or a stale latest timestamp, data collection may have failed. No interaction required beyond reading.

Flow Network (/flow-network):
  What it shows: An auto-discovered stream network diagram. Each site is a node showing live readings. Arrows show upstream-to-downstream flow direction, derived from USGS drainage area and HUC watershed codes — not drawn manually.
  How to use: Select which sites to display using the chips at the top. Select which parameters to show on each node (Discharge, Gage Height, etc.). The layout auto-arranges by watershed depth. Drag nodes to reposition. Click "Analyze Network" to get an AI cascade analysis of how conditions at upstream sites are likely affecting downstream sites. Click "Discover Topology" to re-query the USGS watershed database if connections look wrong.
  Reading the arrows: Line color = discharge trend (blue rising, orange falling, gray stable). Line thickness = volume. Labels show cfs, distance in km, and % of downstream flow.
  Reading the nodes: Border color is a unique identifier per site only — it does not indicate status.

Water Quality (/water-quality):
  What it shows: Regulatory permit limits per site and parameter, and whether current readings are in compliance.
  How to use: Click "Add Limit" to define a threshold (max, min, or target) for any site/parameter combination. Give it a label (e.g. "EPA Max DO") and a color. Once set, limits appear as reference lines on charts and trigger compliance badges.

Annotations (/annotations):
  What it shows: A log of notable events — dam releases, flood peaks, spills, maintenance windows.
  How to use: Click "Add Annotation" and select a site, category, label, and timestamp. Annotations appear as vertical markers on time-series charts so you can correlate events with data changes.

Ask AI (/insights):
  What it shows: A full AI analysis interface with longer-form responses, analysis history, and site-specific forecasts.
  How to use: Type a detailed question or select a site for an automated forecast. Unlike the floating chat, this page supports longer responses and saves your query history. Good for in-depth analysis you want to reference later.

Profile (/profile):
  What it shows: Your user role and sub-role settings.
  How to use: Set your role (general, analyst, operator, regulator, etc.) and optionally a sub-role (kayaker, hydrologist, dam operator, etc.). This tunes the tone and focus of all AI responses throughout the app. You can also add a bio for more personalized context.

Settings (/settings):
  What it shows: System configuration — AI provider, model selection, and data fetch schedule.
  How to use: Select which AI provider to use (Gemini, OpenAI) and which model. Configure how often USGS stream data is fetched. Changes take effect immediately on save without restarting the server.

DATA SOURCES:
HydroScope integrates two independent data sources — both are fully active and complement each other:

USGS Stream Gauges (source=usgs):
- Discharge (cfs): Volume of water per second. Primary flood indicator.
- Gage Height (ft): Water surface elevation (stage). Portland flood stage ≈ 25 ft.
- Water Temperature (°C): Fish habitat and recreation safety indicator.
- Dissolved Oxygen (mg/L): Below 5 mg/L stresses salmon. Tualatin is known for summer DO crashes.
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

SITES (upstream to downstream):
- Bull Run at Bull Run (14138800): Portland's drinking water source. Managed, very clean.
- Sandy River near Marmot (14142500): Glacier-fed from Mt. Hood. Highly responsive to snowmelt and rain.
- Willamette at Springfield (14162500): Upstream Willamette reference near Eugene.
- Willamette at Albany (14174000): Mid-valley Willamette.
- Willamette at Salem (14191000): Oregon's capital. Significant ag runoff input.
- Tualatin at Farmington (14206950): Slow lowland tributary. Summer low flows and temperature/DO issues.
- Clackamas at Oregon City (14211010): Clean, fast tributary joining Willamette at river mile 26.
- Willamette at Portland (14211720): Main city gauge. Tidal influence, ~11,000 mi² drainage.
- Columbia at Bonneville (14246900): Below Bonneville Dam. Hydro-operation dominated.

STATUS COLORS (dashboard badges, site cards):
- Green (ok): Normal, within expected range.
- Yellow (watch): Elevated or below-normal — monitor.
- Orange (warning): Approaching alert threshold.
- Red (critical): Threshold exceeded, immediate attention.

FLOW NETWORK PAGE COLORS:
- Node border color: Each site is assigned a unique color from a fixed palette (blue, purple, green, orange, pink, yellow, etc.) purely for visual identification. The color has NO meaning about status or conditions — it just distinguishes one site from another. If two sites appear to share a color it is because the palette cycles when there are more than 12 sites.
- Connection line color: Blue = upstream discharge is rising. Orange = falling. Gray = stable.
- Connection line thickness: Proportional to discharge volume (cfs).
- Connection line labels: Show cfs value + trend arrow, straight-line distance in km, and % contribution to downstream flow.

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

// Patterns that are unambiguously off-topic — block before any AI call.
const BLOCK_PATTERNS = [
  /write (me )?(a |an )?(poem|song|story|essay|novel|lyrics|haiku|sonnet|script|code|function|class|program)/i,
  /generate (a |an )?(poem|story|image|picture|logo|app|website|code)/i,
  /draw (me )?a /i,
  /\b(debug|refactor|fix) (my |this )?(code|function|script|bug|error)\b/i,
  /how (do i|to) (code|program|build|deploy|install|configure) /i,
  /\bin (python|javascript|java|c\+\+|ruby|rust|go|php|swift|kotlin)\b/i,
  /what (is|are) the capital (of|city)/i,
  /\bsolve (this |the )?(math|equation|problem)\b/i,
  /\btranslate (this |to )\b/i,
  /who (is|was) (the )?(president|prime minister|ceo|founder|inventor|author)/i,
  /recommend (me )?(a |some )?(movie|tv show|show|book|song|restaurant|recipe)/i,
  /\btell me a joke\b/i,
  /\bplay (a game|chess|trivia)\b/i,
];

// Signals that the query is about live monitoring data
const DATA_SIGNALS = [
  // USGS stream gauge parameters
  'discharge','cfs','gage','gauge','stage','level','flow','streamflow',
  'dissolved oxygen','turbidity','ph ',
  // USGS site names
  'willamette','sandy','clackamas','tualatin','columbia','bull run','bonneville','marmot','springfield','albany','salem','farmington',
  // CWMS dam operations parameters
  'pool elevation','forebay','tailwater','inflow','outflow','storage','reservoir storage',
  'turbine flow','spillway','flood pool','conservation pool','acre-feet','acre feet',
  'power generation','mwh','dam release','gate opening','spill',
  'conductance','pool level','reservoir level',
  // CWMS site names (example config)
  'center hill','cheatham','barkley','detroit dam','lookout point','cougar','green peter','foster','fall creek','dorena',
  // General data query terms
  'reading','readings','right now','today','currently','trending','rising','falling','flood','drought',
  'statistics','stddev','standard deviation','outlier','anomal','percentile','correlation',
  // Metric / calculation terms — always analyst-mode territory
  'minimum','maximum','min ','max ',' min',' max',
  'mean','average','median','variance','std ','calculate','computation',
  'domain','range','spread','skew',
];


// Signals that the query is asking for historical comparison or norm context —
// these should go to research mode, not pure analyst, so aiPlan decides
const HISTORICAL_SIGNALS = [
  'normal','unusual','typical','atypical','historic','record','compare','season','seasonal',
  'pattern','before','ever','usually','expect','average year','time of year',
  'el ni','la ni','atmospheric river','snowmelt','snowpack','drought year',
  'high for','low for','above average','below average','above normal','below normal',
];

/**
 * Parse an explicit time window from a query string.
 * Returns hours as a number, or null if none found.
 */
function extractQueryHours(lower) {
  var m;
  if ((m = lower.match(/(?:last|past)\s+(\d+)\s+hour/)))  return parseInt(m[1]);
  if ((m = lower.match(/(?:last|past)\s+(\d+)\s+day/)))   return parseInt(m[1]) * 24;
  if ((m = lower.match(/(?:last|past)\s+(\d+)\s+week/)))  return parseInt(m[1]) * 168;
  if (/(?:last|past)\s+(?:24\s*h|a\s*day|one\s*day)/.test(lower)) return 24;
  if (/(?:last|past)\s+(?:week|7\s*day)/.test(lower))    return 168;
  if (/(?:last|past)\s+(?:month|30\s*day)/.test(lower))  return 720;
  return null;
}

/**
 * Fast local pre-classifier. Returns a plan if the case is clear, null if ambiguous.
 */
function localPlan(query, history, pageContext) {
  var lower = query.toLowerCase();

  // Unambiguous block
  if (BLOCK_PATTERNS.some(function(re) { return re.test(query); })) {
    return { mode: 'general', blocked: true, needs_clarification: null, needs_context: [], data_sources: [], site_ids: null, hours: 48 };
  }

  var hasPageCtx        = pageContext && pageContext.current_page;
  var hasSiteCtx        = pageContext && pageContext.site_id;
  var hasParamCtx       = pageContext && pageContext.param_code;
  var isFollowUp        = history.length > 0;
  var wordCount         = query.trim().split(/\s+/).length;
  var hasDataSignal     = DATA_SIGNALS.some(function(t) { return lower.includes(t); });
  var hasHistoricalSignal = HISTORICAL_SIGNALS.some(function(t) { return lower.includes(t); });
  var queryHours        = extractQueryHours(lower);  // explicit time window in query, e.g. "last 12 hours"

  // Queries that mix live data with historical/norm comparison need research mode —
  // let aiPlan decide so it can choose research + current_readings instead of pure analyst
  if (hasDataSignal && hasHistoricalSignal) return null;

  // Clear wiki: short UI question on a known page with no data signals
  if (hasPageCtx && wordCount <= 10 && !hasDataSignal) {
    if (/\b(what|how|explain|show|this|mean|does|here|looking at|am i|tell me about)\b/i.test(query)) {
      return { mode: 'wiki', blocked: false, needs_clarification: null, needs_context: [], data_sources: ['wiki'], site_ids: null, hours: 48 };
    }
  }

  // Clear analyst: has site+param context and a data signal
  if (hasSiteCtx && hasParamCtx && hasDataSignal) {
    return { mode: 'analyst', blocked: false, needs_clarification: null, needs_context: [],
      data_sources: ['time_series', 'weather'], site_ids: [pageContext.site_id],
      hours: queryHours || parseInt(pageContext.visible_hours) || 168 };
  }

  // Clear analyst: has site context and a data signal (no param)
  if (hasSiteCtx && hasDataSignal) {
    return { mode: 'analyst', blocked: false, needs_clarification: null, needs_context: [],
      data_sources: ['current_readings', 'weather'], site_ids: [pageContext.site_id],
      hours: queryHours || 48 };
  }

  // Clear analyst: data signal, no site context
  // If an explicit time window was requested, let aiPlan handle it — it can identify the site
  // from the query text and request time_series with the right site+param+hours.
  // For current-state questions (no time window), short-circuit to current_readings for all sites.
  if (hasDataSignal) {
    if (queryHours) return null; // fall through to aiPlan for windowed/historical queries
    return { mode: 'analyst', blocked: false, needs_clarification: null, needs_context: [],
      data_sources: ['current_readings', 'weather'], site_ids: null, hours: 48 };
  }

  // Too vague AND no conversation history AND no page context — ask for clarification locally
  if (!isFollowUp && !hasPageCtx && wordCount <= 4 && /\b(what|how|why|show|help|tell)\b/i.test(query)) {
    return { mode: 'general', blocked: false, needs_clarification: 'What specifically would you like to know? I can analyze current flow data, explain how to use a page, or discuss hydrology topics.', needs_context: [], data_sources: [], site_ids: null, hours: 48 };
  }

  // Ambiguous — let AI decide
  return null;
}

/**
 * AI planner — called only when localPlan returns null.
 * Single structured JSON call; decides everything in one shot.
 */
async function aiPlan(query, history, pageContext, profile) {
  var allSites = db.getAllSites();
  var siteList = allSites.map(function(s) { return s.site_id + '=' + s.name; }).join(', ');

  var ctxParts = [];
  if (pageContext) {
    if (pageContext.current_page)  ctxParts.push('page=' + (pageContext.current_page_name || pageContext.current_page));
    if (pageContext.site_id)       ctxParts.push('site=' + (pageContext.site_name || pageContext.site_id));
    if (pageContext.param_code)    ctxParts.push('param=' + (pageContext.param_name || pageContext.param_code));
    if (pageContext.visible_hours) ctxParts.push('window=' + pageContext.visible_hours + 'h');
  }

  var PARAM_CODES = [
    '--- USGS parameter codes ---',
    '00060=discharge(cfs), 00065=gage height(ft), 00010=water temperature(°C), 00300=dissolved oxygen(mg/L), 00400=pH',
    '--- CWMS parameter codes (Corps dams) ---',
    Object.entries(CWMS_PARAMETER_NAMES)
      .slice(0, 15)
      .map(([k, v]) => k + '=' + v)
      .join(', '),
  ].join(' ');

  var prompt = [
    'You are the query planner for HydroScope, a real-time hydrology dashboard for Oregon watersheds.',
    'Available USGS sites: ' + siteList,
    'Available USGS parameter codes: ' + PARAM_CODES,
    'Frontend context already provided: ' + (ctxParts.join(', ') || 'none'),
    'Conversation history turns: ' + history.length,
    'User role: ' + (profile.role || 'general'),
    '',
    'Query: "' + query + '"',
    '',
    'Return ONLY valid JSON with this exact shape — no prose, no code fences:',
    '{',
    '  "mode": "analyst|wiki|research|general",',
    '  "blocked": false,',
    '  "needs_clarification": null,',
    '  "needs_context": [],',
    '  "data_sources": [],',
    '  "site_ids": null,',
    '  "param_code": null,',
    '  "hours": 48',
    '}',
    '',
    'Decision rules:',
    '- mode "wiki"     → questions about the app, UI, pages, features, what colors/icons mean, how to use things',
    '- mode "analyst"  → questions about live data, current conditions, statistics, specific sites or readings — pure numbers, no historical comparison needed',
    '- mode "research" → live data questions that also ask for historical context: is this normal, is this a record, what does this pattern usually mean, how does this compare to a typical year/season, questions about climate drivers (El Niño, atmospheric rivers, snowmelt), why something is happening, what to expect next',
    '- mode "general"  → greetings, small talk, follow-ups too short to classify otherwise',
    '- blocked         → true ONLY for clearly unrelated requests (poems, code help, movie recs)',
    '- needs_clarification → short question string if genuinely too vague; null if page context implies intent; always null for follow-ups',
    '- needs_context   → ONLY list fields not already in "Frontend context provided" above. Options: "selected_site","selected_param","visible_hours","current_page"',
    '- data_sources    → what the system needs to fetch to answer this query:',
    '    "current_readings" = latest snapshot across sites (use for current conditions, network-wide questions)',
    '    "time_series"      = full timestamped history with stats for ONE site+param (use when query asks for range, min, max, trend, stats, or a specific time window at an identifiable site)',
    '    "wiki"             = app guide (pages, features, UI explanations)',
    '    "weather"          = precipitation/temperature forecast context',
    '    "annotations"      = logged events/releases at sites',
    '- site_ids   → resolve from query text using the site list above; null means all sites; be specific when the query names a site',
    '- param_code → USGS code for the parameter the query is about (use list above); null only if truly unspecified (system defaults to discharge)',
    '- hours      → match the time window the user asked for; 48 if not specified, 168 (weekly), 720 (monthly)',
  ].join('\n');

  try {
    var text = await withRetry(function() { return getProvider().generateJSON('', prompt); });
    text = text.trim().replace(/```json\n?|\n?```/g, '');
    var plan = JSON.parse(text);
    plan.mode          = plan.mode          || 'general';
    plan.blocked       = plan.blocked       || false;
    plan.needs_context = plan.needs_context || [];
    plan.data_sources  = plan.data_sources  || [];
    plan.hours         = plan.hours         || 48;
    plan.param_code    = plan.param_code    || null;
    return plan;
  } catch (_) {
    return { mode: 'general', blocked: false, needs_clarification: null, needs_context: [], data_sources: [], site_ids: null, hours: 48 };
  }
}

/**
 * Resolve a plan: local fast path, AI fallback only when ambiguous.
 */
async function planQuery(query, history, pageContext, profile) {
  var fast = localPlan(query, history, pageContext);
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
        if (weatherRows && weatherRows.length) blocks.push(buildWeatherContext(weatherRows));
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

  if (plan.needs_clarification) {
    return { clarification: plan.needs_clarification, mode: plan.mode, modeChanged: plan.mode !== requestedMode };
  }

  if (plan.needs_context && plan.needs_context.length) {
    return { needs_context: true, fields: plan.needs_context, mode: plan.mode };
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

  var systemParts = [
    modeConfig.system,
    roleCtx,
    subCtx,
    bioCtx,
    pageCtxLine,
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
