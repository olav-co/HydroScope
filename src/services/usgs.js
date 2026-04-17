/**
 * USGS Water Services data fetcher.
 * Docs: https://waterservices.usgs.gov/rest/IV-Service.html
 */

const axios = require('axios');

const BASE_URL = 'https://waterservices.usgs.gov/nwis/iv/';

const PARAMETER_LABELS = {
  '00060': { name: 'Discharge',           unit: 'ft³/s' },
  '00065': { name: 'Gage Height',         unit: 'ft'    },
  '00010': { name: 'Water Temperature',   unit: '°C'    },
  '00300': { name: 'Dissolved Oxygen',    unit: 'mg/L'  },
  '00400': { name: 'pH',                  unit: 'pH'    },
};

/**
 * Fetch instantaneous values for a list of sites.
 * @param {string[]} siteIds
 * @param {string[]} parameterCodes
 * @param {string} period  ISO 8601 duration, e.g. 'PT3H', 'P1D'
 * @returns {Array} flat array of measurement rows ready for DB insert
 */
async function fetchInstantaneousValues(siteIds, parameterCodes, period = 'PT3H') {
  const params = {
    format: 'json',
    sites: siteIds.join(','),
    parameterCd: parameterCodes.join(','),
    period,
    siteStatus: 'active',
  };

  const response = await axios.get(BASE_URL, { params, timeout: 30000 });
  const timeSeries = (response.data && response.data.value && response.data.value.timeSeries) || [];

  const rows = [];

  for (const series of timeSeries) {
    const sourceInfo = series.sourceInfo || {};
    const siteCode = sourceInfo.siteCode || [];
    const siteId = siteCode[0] && siteCode[0].value;

    const variable = series.variable || {};
    const varCode = variable.variableCode || [];
    const paramCode = varCode[0] && varCode[0].value;
    const unitCode = (variable.unit && variable.unit.unitCode) || '';
    const paramMeta = PARAMETER_LABELS[paramCode] || { name: variable.variableName, unit: unitCode };

    if (!siteId || !paramCode) continue;

    const seriesValues = series.values || [];
    const firstBlock = seriesValues[0] || {};
    const values = firstBlock.value || [];
    for (const v of values) {
      const numVal = parseFloat(v.value);
      if (isNaN(numVal) || v.value === '-999999') continue;

      // Normalize timestamp to UTC ISO string
      const recorded_at = new Date(v.dateTime).toISOString().replace('T', ' ').replace(/\.\d{3}Z$/, '');

      rows.push({
        site_id:        siteId,
        parameter_code: paramCode,
        parameter_name: paramMeta.name,
        value:          numVal,
        unit:           paramMeta.unit || unitCode,
        recorded_at,
      });
    }
  }

  return rows;
}

/**
 * Fetch the most recent single reading for all configured sites (used for dashboard).
 */
async function fetchCurrentReadings(siteIds, parameterCodes) {
  return fetchInstantaneousValues(siteIds, parameterCodes, 'PT3H');
}

/**
 * Fetch historical data for charting (default 7 days).
 */
async function fetchHistoricalData(siteIds, parameterCodes, days = 7) {
  return fetchInstantaneousValues(siteIds, parameterCodes, `P${days}D`);
}

/**
 * Derive human-readable condition label from discharge value.
 */
function dischargeCondition(cfs, historicalMedian = null) {
  if (cfs === null || cfs === undefined) return 'Unknown';
  if (historicalMedian) {
    const ratio = cfs / historicalMedian;
    if (ratio > 2.0)  return 'Major Flood Stage';
    if (ratio > 1.5)  return 'Moderate Flood Stage';
    if (ratio > 1.2)  return 'Above Normal';
    if (ratio > 0.8)  return 'Near Normal';
    if (ratio > 0.5)  return 'Below Normal';
    return 'Low Flow';
  }
  // Rough absolute thresholds for Willamette-scale rivers
  if (cfs > 150000) return 'Major Flood Stage';
  if (cfs > 80000)  return 'Moderate Flood Stage';
  if (cfs > 40000)  return 'Elevated';
  if (cfs > 10000)  return 'Moderate';
  if (cfs > 2000)   return 'Low-Moderate';
  return 'Low Flow';
}

// ── Watershed Topology Discovery via USGS NLDI ───────────────────────────────
//
// Strategy: for each configured site, ask NLDI "which of my configured sites
// are downstream of me on the main NHDPlus network?" using the DM/nwissite
// navigation endpoint. If site B appears in site A's downstream results,
// A→B is a real upstream→downstream connection.
//
// This is authoritative — it uses the same NHDPlus routing network that USGS
// built the entire linked-data system on. Drainage-area heuristics get
// Bonneville Dam wrong (huge drain area but it's upstream of Portland);
// NLDI gets it right because it follows actual river flow paths.
//
// One HTTP request per site at sync time. Results are cached in the DB.
// ─────────────────────────────────────────────────────────────────────────────

const NLDI_HOST_TOPO = 'api.water.usgs.gov';
const NLDI_BASE_TOPO = '/nldi/linked-data/nwissite';

/**
 * Fetch downstream USGS nwissite features for one site via NLDI DM navigation.
 * Returns an array of site_id strings that are downstream on the main stem.
 */
async function fetchDownstreamSites(siteId) {
  const path = `${NLDI_BASE_TOPO}/USGS-${siteId}/navigation/DM/nwissite?distance=9999`;
  return new Promise((resolve) => {
    const https = require('https');
    const req = https.request(
      { hostname: NLDI_HOST_TOPO, path, method: 'GET',
        headers: { 'User-Agent': 'HydroScope/1.0 (hydrology monitoring dashboard)' },
        timeout: 20000 },
      (res) => {
        if (res.statusCode !== 200) { res.resume(); return resolve([]); }
        let raw = '';
        res.on('data', c => raw += c);
        res.on('end', () => {
          try {
            const json = JSON.parse(raw);
            const ids = (json.features || [])
              .map(f => {
                const ident = f.properties?.identifier || '';
                return ident.startsWith('USGS-') ? ident.slice(5) : null;
              })
              .filter(Boolean);
            resolve(ids);
          } catch(_) { resolve([]); }
        });
      }
    );
    req.on('error', () => resolve([]));
    req.on('timeout', () => { req.destroy(); resolve([]); });
    req.end();
  });
}

/**
 * Remove transitive edges: if A→B and B→C both exist, drop A→C.
 */
function transitiveReduction(connections) {
  const adj = {};
  for (const c of connections) {
    if (!adj[c.from_site_id]) adj[c.from_site_id] = [];
    adj[c.from_site_id].push(c.to_site_id);
  }

  function reachableWithout(start, target) {
    const visited = new Set([start]);
    const queue = (adj[start] || []).filter(n => n !== target);
    for (const n of queue) visited.add(n);
    while (queue.length) {
      const cur = queue.shift();
      if (cur === target) return true;
      for (const next of (adj[cur] || [])) {
        if (!visited.has(next)) { visited.add(next); queue.push(next); }
      }
    }
    return false;
  }

  return connections.filter(c => !reachableWithout(c.from_site_id, c.to_site_id));
}

/**
 * Discover upstream→downstream relationships between configured USGS sites
 * using NLDI DM/nwissite navigation — follows the actual NHDPlus flow network.
 *
 * @param {string[]} siteIds
 * @returns {Array}  [{from_site_id, to_site_id}]
 */
async function discoverNetworkTopology(siteIds) {
  const configured = new Set(siteIds);
  console.log(`[Topology] Querying NLDI DM navigation for ${siteIds.length} sites…`);

  // Fetch downstream sites for all gauges in parallel, staggered 300ms apart
  // to avoid hammering the API.
  const results = await Promise.all(
    siteIds.map((siteId, i) =>
      new Promise(resolve => setTimeout(async () => {
        try {
          const downIds = await fetchDownstreamSites(siteId);
          const inNetwork = downIds.filter(id => configured.has(id) && id !== siteId);
          console.log(`[Topology] ${siteId} → downstream in-network: [${inNetwork.join(', ') || 'none'}]`);
          resolve({ siteId, downIds: inNetwork });
        } catch(e) {
          console.warn(`[Topology] NLDI failed for ${siteId}:`, e.message);
          resolve({ siteId, downIds: [] });
        }
      }, i * 300))
    )
  );

  // Build candidate connections
  const candidates = [];
  for (const { siteId, downIds } of results) {
    for (const dsId of downIds) {
      candidates.push({ from_site_id: siteId, to_site_id: dsId });
    }
  }

  if (!candidates.length) {
    console.warn('[Topology] No connections found via NLDI — check that site IDs are valid USGS nwissite identifiers.');
    return [];
  }

  // Transitive reduction: A→B→C exists, so drop A→C shortcut
  const reduced = transitiveReduction(candidates);
  console.log(`[Topology] ${reduced.length} direct connection(s) from ${candidates.length} candidate(s):`);
  reduced.forEach(c => console.log(`  ${c.from_site_id} → ${c.to_site_id}`));
  return reduced;
}

module.exports = { fetchCurrentReadings, fetchHistoricalData, fetchInstantaneousValues, dischargeCondition, PARAMETER_LABELS, discoverNetworkTopology };
