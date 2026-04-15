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
  const timeSeries = response.data?.value?.timeSeries ?? [];

  const rows = [];

  for (const series of timeSeries) {
    const siteId   = series.sourceInfo?.siteCode?.[0]?.value;
    const paramCode = series.variable?.variableCode?.[0]?.value;
    const unitCode  = series.variable?.unit?.unitCode ?? '';
    const paramMeta = PARAMETER_LABELS[paramCode] || { name: series.variable?.variableName, unit: unitCode };

    if (!siteId || !paramCode) continue;

    const values = series.values?.[0]?.value ?? [];
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

module.exports = { fetchCurrentReadings, fetchHistoricalData, fetchInstantaneousValues, dischargeCondition, PARAMETER_LABELS };
