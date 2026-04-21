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

const https = require('https');

const NLDI_HOST_TOPO = 'api.water.usgs.gov';
const NLDI_BASE_TOPO = '/nldi/linked-data/nwissite';
const NLDI_HOST_LABS = 'api.water.usgs.gov';   // labs.waterdata.usgs.gov was decommissioned

/**
 * Generic NLDI GET helper — returns parsed JSON or null on error.
 */
function nldiGet(hostname, path, timeoutMs = 20000) {
  return new Promise((resolve) => {
    const req = https.request(
      { hostname, path, method: 'GET',
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'HydroScope/1.0 (hydrology monitoring dashboard)',
        },
        timeout: timeoutMs },
      (res) => {
        if (res.statusCode !== 200) {
          console.warn(`[NLDI] ${hostname}${path} → HTTP ${res.statusCode}`);
          res.resume(); return resolve(null);
        }
        let raw = '';
        res.on('data', c => raw += c);
        res.on('end', () => {
          try { resolve(JSON.parse(raw)); } catch(_) { resolve(null); }
        });
      }
    );
    req.on('error', (e) => { console.warn(`[NLDI] ${hostname}${path} error: ${e.message}`); resolve(null); });
    req.on('timeout', () => { req.destroy(); console.warn(`[NLDI] ${hostname}${path} timed out`); resolve(null); });
    req.end();
  });
}

/**
 * Snap a lat/lon to its nearest NHD comid via NLDI position endpoint.
 * Returns comid string or null.
 */
async function fetchComidForLatLon(lat, lon) {
  // encodeURIComponent encodes parens + space so the query value is valid
  const coords = encodeURIComponent(`POINT(${lon} ${lat})`);
  const path = `/nldi/linked-data/comid/position?coords=${coords}`;
  const json = await nldiGet(NLDI_HOST_LABS, path);
  if (!json) return null;
  const feature = (json.features || [])[0];
  if (!feature) return null;
  // Try every known property location NLDI has used across API versions
  const comid = feature.properties?.comid
    || feature.properties?.nhdplus_comid
    || feature.properties?.identifier;
  if (comid) return String(comid);
  // Fallback: extract trailing digits from feature id URL (e.g. "comid.12345678")
  const idStr = String(feature.id || '');
  const m = idStr.match(/(\d+)$/);
  return m ? m[1] : null;
}

/**
 * Navigate a comid upstream (UM) or downstream (DM) to find USGS nwissite gauges.
 * Returns array of USGS site_id strings (numeric, no "USGS-" prefix).
 */
async function fetchComidNeighborUsgsIds(comid, direction) {
  const path = `/nldi/linked-data/comid/${comid}/navigation/${direction}/nwissite?distance=9999`;
  const json = await nldiGet(NLDI_HOST_LABS, path);
  if (!json) return [];
  return (json.features || [])
    .map(f => {
      const ident = f.properties?.identifier || '';
      return ident.startsWith('USGS-') ? ident.slice(5) : null;
    })
    .filter(Boolean);
}

/** Parse comids out of an NLDI flowlines GeoJSON response. */
function _extractComids(json) {
  const out = new Set();
  if (!json) return out;
  for (const f of (json.features || [])) {
    const fromProps = f.properties?.nhdplus_comid || f.properties?.comid;
    if (fromProps) { out.add(String(fromProps)); continue; }
    const idStr = String(f.id || '');
    const m = idStr.match(/(\d+)$/);
    if (m) out.add(m[1]);
  }
  return out;
}

/**
 * Navigate a comid downstream (or upstream) and return the Set of all comids.
 * direction: 'DM' | 'UM'
 * distance: km (500 is the practical cap — covers the longest connected reaches)
 */
async function fetchComidDirectionalComids(comid, direction, distanceKm = 500) {
  const path = `/nldi/linked-data/comid/${comid}/navigation/${direction}/flowlines?distance=${distanceKm}`;
  const json = await nldiGet(NLDI_HOST_LABS, path, 60000);
  if (!json) {
    console.warn(`[Topology] fetchComidDirectionalComids(${comid}, ${direction}): null response`);
    return new Set();
  }
  const out = _extractComids(json);
  console.log(`[Topology]   ${direction} from ${comid}: ${out.size} comids`);
  return out;
}

/**
 * Collect a "channel neighborhood" for a snap comid: all flowline comids within
 * radiusKm upstream AND downstream.  Dam/reservoir lat-lon points often snap to
 * a pool/waterbody comid that is adjacent to but NOT on the main NHD navigation
 * path.  75 km radius ensures we walk far enough up the reservoir to hit the
 * main-stem channel comids the inter-site navigation paths thread through.
 */
async function fetchComidNeighborhood(comid, radiusKm = 75) {
  const [upJson, dnJson] = await Promise.all([
    nldiGet(NLDI_HOST_LABS,
      `/nldi/linked-data/comid/${comid}/navigation/UM/flowlines?distance=${radiusKm}`, 45000),
    nldiGet(NLDI_HOST_LABS,
      `/nldi/linked-data/comid/${comid}/navigation/DM/flowlines?distance=${radiusKm}`, 45000),
  ]);
  const neighborhood = new Set([comid]);
  for (const c of _extractComids(upJson)) neighborhood.add(c);
  for (const c of _extractComids(dnJson)) neighborhood.add(c);
  return neighborhood;
}

/**
 * Fetch downstream USGS nwissite features for one site via NLDI DM navigation.
 * Returns an array of site_id strings that are downstream on the main stem.
 */
async function fetchDownstreamSites(siteId) {
  const path = `${NLDI_BASE_TOPO}/USGS-${encodeURIComponent(siteId)}/navigation/DM/nwissite?distance=9999`;
  const json = await nldiGet(NLDI_HOST_TOPO, path);
  if (!json) return [];
  return (json.features || [])
    .map(f => {
      const ident = f.properties?.identifier || '';
      return ident.startsWith('USGS-') ? ident.slice(5) : null;
    })
    .filter(Boolean);
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
 * Discover upstream→downstream relationships between all configured sites.
 *
 * Accepts either:
 *   - Array of site objects { site_id, source, latitude, longitude } (preferred)
 *   - Array of string IDs (legacy — treated as USGS sites)
 *
 * Strategy:
 *   USGS sites  — existing NLDI DM/nwissite navigation by site ID
 *   CWMS sites  — snap lat/lon to nearest NHD comid, then navigate UM/DM
 *                 to find any configured USGS gauges up- or downstream
 *
 * @param {Array} sites   Site objects or legacy string IDs
 * @returns {Array}  [{from_site_id, to_site_id}]
 */
async function discoverNetworkTopology(sites) {
  // Back-compat: if passed plain strings, wrap them as USGS objects
  if (typeof sites[0] === 'string') {
    sites = sites.map(id => ({ site_id: id, source: 'usgs', latitude: null, longitude: null }));
  }

  const usgsSites = sites.filter(s => s.source !== 'cwms');
  const cwmsSites = sites.filter(s => s.source === 'cwms' && s.latitude && s.longitude);
  const allIds    = new Set(sites.map(s => s.site_id));

  console.log(`[Topology] ${usgsSites.length} USGS, ${cwmsSites.length} CWMS sites`);

  const candidates = [];

  // ── USGS: NLDI DM navigation, parallel, staggered 300 ms ─────────────────
  if (usgsSites.length) {
    const results = await Promise.all(
      usgsSites.map((site, i) =>
        new Promise(resolve => setTimeout(async () => {
          try {
            const downIds  = await fetchDownstreamSites(site.site_id);
            const inNetwork = downIds.filter(id => allIds.has(id) && id !== site.site_id);
            console.log(`[Topology] USGS ${site.site_id} → downstream: [${inNetwork.join(', ') || 'none'}]`);
            resolve({ siteId: site.site_id, downIds: inNetwork });
          } catch(e) {
            console.warn(`[Topology] NLDI failed for ${site.site_id}:`, e.message);
            resolve({ siteId: site.site_id, downIds: [] });
          }
        }, i * 300))
      )
    );

    for (const { siteId, downIds } of results) {
      for (const dsId of downIds) candidates.push({ from_site_id: siteId, to_site_id: dsId });
    }
  }

  // ── CWMS: snap every site to its NHD comid first ─────────────────────────
  const cwmsComids      = new Map(); // site_id → snap comid string
  const cwmsNeighborhoods = new Map(); // site_id → Set of nearby channel comids

  for (let i = 0; i < cwmsSites.length; i++) {
    if (i > 0) await new Promise(r => setTimeout(r, 300));
    const site = cwmsSites[i];
    console.log(`[Topology] CWMS ${site.site_id}: snapping (${site.latitude}, ${site.longitude})…`);
    const comid = await fetchComidForLatLon(site.latitude, site.longitude);
    if (comid) {
      cwmsComids.set(site.site_id, comid);
      console.log(`[Topology] CWMS ${site.site_id}: comid ${comid}`);
    } else {
      console.warn(`[Topology] CWMS ${site.site_id}: no comid — skipping`);
    }
  }

  // Build channel neighborhoods (±75 km) for all snapped CWMS sites in parallel.
  // Dam lat-lons often snap to a reservoir pool comid that isn't on the main NHD
  // navigation path; the wider neighborhood reaches the actual main-stem channel
  // comids that inter-site navigation paths thread through.
  const cwmsDownstream = new Map();  // site_id → Set of DM comids (500 km)
  const cwmsUpstream   = new Map();  // site_id → Set of UM comids (500 km)

  if (cwmsComids.size > 1) {
    console.log('[Topology] Building channel neighborhoods + directional sets for CWMS sites…');
    // Stagger requests to avoid hammering NLDI — 3 calls per site, 600 ms apart
    const entries = [...cwmsComids.entries()];
    for (let i = 0; i < entries.length; i++) {
      const [siteId, comid] = entries[i];
      if (i > 0) await new Promise(r => setTimeout(r, 600));

      const [nbhd, dmSet, umSet] = await Promise.all([
        fetchComidNeighborhood(comid, 75),
        fetchComidDirectionalComids(comid, 'DM', 500),
        fetchComidDirectionalComids(comid, 'UM', 500),
      ]);
      cwmsNeighborhoods.set(siteId, nbhd);
      cwmsDownstream.set(siteId, dmSet);
      cwmsUpstream.set(siteId, umSet);
      console.log(`[Topology] CWMS ${siteId}: neighborhood=${nbhd.size}, DM=${dmSet.size}, UM=${umSet.size} comids`);
    }
  }

  // ── CWMS: for each snapped site, find upstream/downstream peers ───────────
  for (const [siteId, comid] of cwmsComids) {
    await new Promise(r => setTimeout(r, 300));

    // USGS gauges upstream of this dam → gauge → dam
    const upUsgsIds = await fetchComidNeighborUsgsIds(comid, 'UM');
    const upUsgs = upUsgsIds.filter(id => allIds.has(id));
    console.log(`[Topology] CWMS ${siteId} ← upstream USGS: [${upUsgs.join(', ') || 'none'}]`);
    for (const uid of upUsgs) candidates.push({ from_site_id: uid, to_site_id: siteId });

    await new Promise(r => setTimeout(r, 300));

    // USGS gauges downstream of this dam → dam → gauge
    const dnUsgsIds = await fetchComidNeighborUsgsIds(comid, 'DM');
    const dnUsgs = dnUsgsIds.filter(id => allIds.has(id));
    console.log(`[Topology] CWMS ${siteId} → downstream USGS: [${dnUsgs.join(', ') || 'none'}]`);
    for (const uid of dnUsgs) candidates.push({ from_site_id: siteId, to_site_id: uid });

    await new Promise(r => setTimeout(r, 300));

    // CWMS-to-CWMS: two-way check using pre-built directional sets + neighborhoods.
    //   Forward:  siteId's DM set  ∩  otherSite's neighborhood  → siteId → otherSite
    //   Reverse:  otherSite's UM set ∩  siteId's neighborhood   → siteId → otherSite
    // Using both directions handles large reservoirs where the snap comid is far
    // from the main navigation channel in one direction but reachable from the other.
    if (cwmsComids.size > 1) {
      const myDmSet   = cwmsDownstream.get(siteId) || new Set();
      const myNbhd    = cwmsNeighborhoods.get(siteId) || new Set([comid]);

      for (const [otherSiteId, _otherComid] of cwmsComids) {
        if (otherSiteId === siteId) continue;
        const otherNbhd  = cwmsNeighborhoods.get(otherSiteId) || new Set([_otherComid]);
        const otherUmSet = cwmsUpstream.get(otherSiteId) || new Set();

        // Forward: is any of otherSite's neighborhood in my downstream?
        let fwdMatch = null;
        for (const nc of otherNbhd) {
          if (myDmSet.has(nc)) { fwdMatch = nc; break; }
        }

        // Reverse: is any of my neighborhood in otherSite's upstream?
        let revMatch = null;
        if (!fwdMatch) {
          for (const nc of myNbhd) {
            if (otherUmSet.has(nc)) { revMatch = nc; break; }
          }
        }

        const found = fwdMatch !== null || revMatch !== null;
        const via   = fwdMatch ? `fwd comid ${fwdMatch}` : (revMatch ? `rev comid ${revMatch}` : '');
        console.log(`[Topology]   ${siteId}→${otherSiteId}? ${found ? `✓ (${via})` : 'false'}`);
        if (found) {
          candidates.push({ from_site_id: siteId, to_site_id: otherSiteId });
        }
      }
    }
  }

  if (!candidates.length) {
    console.warn('[Topology] No connections found.');
    return [];
  }

  // Transitive reduction: A→B→C exists, so drop A→C shortcut
  const reduced = transitiveReduction(candidates);
  console.log(`[Topology] ${reduced.length} connection(s) from ${candidates.length} candidate(s):`);
  reduced.forEach(c => console.log(`  ${c.from_site_id} → ${c.to_site_id}`));
  return reduced;
}

module.exports = { fetchCurrentReadings, fetchHistoricalData, fetchInstantaneousValues, dischargeCondition, PARAMETER_LABELS, discoverNetworkTopology, fetchComidForLatLon };
