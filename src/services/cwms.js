/**
 * CWMS/CDA Data Fetcher
 * US Army Corps of Engineers — CWMS Data Access public API.
 * Docs: https://cwms-data.usace.army.mil/cwms-data/swagger-ui.html
 *
 * Timeseries ID format:  location.parameter.type.interval.duration.version
 *   e.g. CEHT1-CENTER_HILL.Elev-Pool.Inst.1Hour.0.dcp-rev
 *
 * Response values array: [[epoch_ms, value, quality_code], ...]
 * Quality code 0 = unscreened, 3 = OK, ≥ 5 = missing/questionable — we accept 0–4.
 */

const https = require('https');
const http  = require('http');

// ── Parameter metadata ────────────────────────────────────────────────────────
// Maps the 2nd dot-segment of a timeseries ID to human-readable name.
// Units come from the API response; this table is for display names only.

const CWMS_PARAMETER_NAMES = {
  'Elev-Pool':               'Pool Elevation',
  'Elev-Forebay':            'Forebay Elevation',
  'Elev-Tail':               'Tailwater Elevation',
  'Elev':                    'Elevation',
  'Flow-In':                 'Inflow',
  'Flow-Out':                'Outflow',
  'Flow':                    'Outflow',
  'Flow-Turbine':            'Turbine Flow',
  'Flow-Spillway':           'Spillway Flow',
  'Flow-Canal':              'Canal Flow',
  'Flow-Controlled':         'Controlled Flow',
  'Flow-Uncontrolled':       'Uncontrolled Flow',
  'Flow-Res In':             'Reservoir Inflow',
  'Flow-Res Out':            'Reservoir Outflow',
  'Stor':                    'Storage',
  'Stor-Conservation Pool':  'Conservation Pool Storage',
  'Stor-Flood Pool':         'Flood Pool Storage',
  '%-Flood Control-Used':    'Flood Pool Used %',
  '%-FloodControl':          'Flood Pool Used %',
  '%-Conservation Pool Full':'Conservation Pool Full %',
  '%-Flood Pool Full':       'Flood Pool Full %',
  'Precip-Inc':              'Precipitation',
  'Precip-Cuml':             'Cumulative Precip',
  'Precip-Mean Areal':       'Mean Areal Precip',
  'Energy':                  'Power Generation',
  'Energy-Used':             'Energy Used',
  'Temp-Water-Tail':         'Tailwater Temperature',
  'Conc-Do-Tail':            'DO (Tailwater)',
  'Cond-Tail':               'Conductance (Tailwater)',
  'pH-Tail':                 'pH (Tailwater)',
  'Stage':                   'Stage',
  'Volt-Battery':            'Battery Voltage',
  'Volt-Battery Load':       'Battery Load Voltage',
};

/**
 * Extract the parameter segment from a CWMS timeseries ID.
 * "CEHT1-CENTER_HILL.Elev-Pool.Inst.1Hour.0.dcp-rev" → "Elev-Pool"
 */
function extractParamCode(timeseriesName) {
  const parts = timeseriesName.split('.');
  return parts.length >= 2 ? parts[1] : timeseriesName;
}

/**
 * Fetch a single CWMS timeseries via the public CDA REST API.
 * Returns parsed JSON or throws.
 */
function fetchTimeseries(baseUrl, office, timeseriesName, lookbackHours) {
  return new Promise((resolve, reject) => {
    const end   = new Date();
    const begin = new Date(end.getTime() - lookbackHours * 3600 * 1000);

    // ISO 8601 without milliseconds — CDA expects this format
    const beginStr = begin.toISOString().replace(/\.\d{3}Z$/, 'Z');
    const endStr   = end.toISOString().replace(/\.\d{3}Z$/, 'Z');

    const qs = new URLSearchParams({
      name:   timeseriesName,
      office: office,
      begin:  beginStr,
      end:    endStr,
    }).toString();

    // Resolve protocol, host, port from baseUrl
    const urlObj  = new URL(baseUrl + '/timeseries?' + qs);
    const isHttps = urlObj.protocol === 'https:';
    const client  = isHttps ? https : http;
    const options = {
      hostname: urlObj.hostname,
      port:     urlObj.port ? parseInt(urlObj.port, 10) : (isHttps ? 443 : 80),
      path:     urlObj.pathname + urlObj.search,
      method:   'GET',
      headers: {
        'Accept':     'application/json;q=0.9,*/*;q=0.8',
        'User-Agent': 'HydroScope/1.0 (hydrology monitoring dashboard)',
      },
      timeout: 20000,
    };

    const req = client.request(options, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode} for ${timeseriesName}`));
      }
      let raw = '';
      res.on('data', c => raw += c);
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error('JSON parse error for ' + timeseriesName)); }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout for ' + timeseriesName)); });
    req.end();
  });
}

/**
 * Fetch all configured CWMS timeseries for all configured sites.
 * Requests are staggered 200ms apart to be polite to the public API.
 *
 * @param {Array}  sites   Array of site objects from cfg.cwms.sites
 * @param {object} cwmsCfg cfg.cwms — { baseUrl, office, lookbackHours }
 * @returns {Array} Flat array of measurement rows ready for insertMeasurements()
 */
async function fetchCurrentReadingsCWMS(sites, cwmsCfg) {
  const { baseUrl, office: globalOffice, lookbackHours = 48 } = cwmsCfg;

  // Build a flat list of (siteId, office, timeseriesName) pairs.
  // Use per-site office from DB if set; fall back to global config office.
  const tasks = [];
  for (const site of sites) {
    const siteOffice = site.office || globalOffice;
    for (const tsName of (site.timeseries || [])) {
      tasks.push({ siteId: site.locationId, office: siteOffice, tsName });
    }
  }

  const rows = [];
  let delay  = 0;

  await Promise.all(tasks.map(({ siteId, office, tsName }) =>
    new Promise(resolve => setTimeout(async () => {
      try {
        const data       = await fetchTimeseries(baseUrl, office, tsName, lookbackHours);
        const paramCode  = extractParamCode(tsName);
        const paramName  = CWMS_PARAMETER_NAMES[paramCode] || paramCode;
        const unit       = data.units || data.unit || '';
        // Handle both standard CDA shape (values) and older shape (time-series-data, regular-interval-values)
        const values     = data.values
          || data['time-series-data']
          || data['regular-interval-values']
          || [];

        let added = 0;
        for (const entry of values) {
          const [tsMs, value, qualityCode] = entry;
          // Reject nulls and quality codes ≥ 5 (missing / questionable)
          if (value === null || value === undefined) continue;
          if (qualityCode >= 5) continue;

          const numVal = parseFloat(value);
          if (isNaN(numVal)) continue;

          const recorded_at = new Date(tsMs)
            .toISOString()
            .replace('T', ' ')
            .replace(/\.\d{3}Z$/, '');

          rows.push({
            site_id:        siteId,
            parameter_code: paramCode,
            parameter_name: paramName,
            value:          numVal,
            unit,
            recorded_at,
            source:         'cwms',
          });
          added++;
        }

        if (added) {
          console.log(`[CWMS] ${tsName}: ${added} values`);
        } else {
          const total = data.total != null ? data.total : 'n/a';
          const begin = data.begin || 'n/a';
          const end   = data.end   || 'n/a';
          console.log(`[CWMS] ${tsName}: no data (total=${total}, begin=${begin}, end=${end})`);
        }
      } catch (err) {
        console.warn(`[CWMS] Failed to fetch ${tsName}: ${err.message}`);
      }
      resolve();
    }, delay++ * 200))
  ));

  return rows;
}

module.exports = { fetchCurrentReadingsCWMS, CWMS_PARAMETER_NAMES, extractParamCode };
