/**
 * Open-Meteo weather service.
 * No API key required. https://open-meteo.com/
 *
 * Weather monitoring locations are derived dynamically from the active sites
 * configured in the database. Sites within ~1.5 degrees of each other are
 * clustered into a single fetch location to avoid redundant API calls.
 */

const axios = require('axios');

const BASE_URL = 'https://api.open-meteo.com/v1/forecast';

/**
 * Build weather fetch locations from active sites in the DB.
 * Clusters sites within 1.5 degrees (roughly 100 miles) into one location.
 * Returns [{id, name, lat, lon}].
 */
function deriveWeatherLocations() {
  try {
    var db = require('../db/database');
    var sites = db.getActiveSites().filter(function(s) { return s.latitude && s.longitude; });
    if (!sites.length) return [];

    var clusters = [];
    sites.forEach(function(site) {
      var placed = false;
      for (var ci = 0; ci < clusters.length; ci++) {
        var c = clusters[ci];
        var d = Math.sqrt(Math.pow(site.latitude - c.lat, 2) + Math.pow(site.longitude - c.lon, 2));
        if (d < 1.5) {
          c.members.push(site);
          c.lat = c.members.reduce(function(s, x) { return s + x.latitude; },  0) / c.members.length;
          c.lon = c.members.reduce(function(s, x) { return s + x.longitude; }, 0) / c.members.length;
          placed = true;
          break;
        }
      }
      if (!placed) clusters.push({ lat: site.latitude, lon: site.longitude, members: [site] });
    });

    return clusters.map(function(c) {
      // Prefer basin name → site name → generic fallback
      var named = c.members.find(function(s) { return s.huc8_name; });
      var name  = (named && named.huc8_name) || c.members[0].name || 'Monitoring Region';
      // Stable ID from rounded centroid (sign-aware so east/west don't collide)
      var id = 'wx_' + Math.round(c.lat * 10) + '_' + Math.round(c.lon * 10);
      return { id: id, name: name, lat: c.lat, lon: c.lon };
    });
  } catch (_) {
    return [];
  }
}

// Convert Open-Meteo hourly timestamp "2024-04-15T10:00" → "2024-04-15 10:00:00"
function fmtHour(t) { return t.replace('T', ' ') + ':00'; }
// Convert daily date "2024-04-15" → "2024-04-15 00:00:00"
function fmtDay(d)  { return d + ' 00:00:00'; }

function isNullish(v) { return v === null || v === undefined; }

/**
 * Fetch weather for all three locations.
 * Returns flat array of rows ready for DB insert.
 */
async function fetchWeatherData() {
  var rows = [];
  var locations = deriveWeatherLocations();
  if (!locations.length) {
    console.log('[Weather] No active sites with coordinates — skipping weather fetch.');
    return rows;
  }

  for (var i = 0; i < locations.length; i++) {
    var loc = locations[i];
    try {
      var resp = await axios.get(BASE_URL, {
        params: {
          latitude:           loc.lat,
          longitude:          loc.lon,
          hourly:             'precipitation,temperature_2m,snowfall,rain',
          daily:              'precipitation_sum,temperature_2m_max,temperature_2m_min,snowfall_sum',
          forecast_days:      7,
          past_days:          7,
          timezone:           'auto',
          temperature_unit:   'fahrenheit',
          precipitation_unit: 'inch',
        },
        timeout: 20000,
      });

      var data = resp.data;
      var hourly = data.hourly || {};
      var daily  = data.daily  || {};
      var times  = hourly.time || [];

      // — Hourly rows —
      for (var j = 0; j < times.length; j++) {
        var recAt = fmtHour(times[j]);
        var isForecast = new Date(times[j]) > new Date() ? 1 : 0;

        var h = [
          { param: 'precipitation', val: hourly.precipitation && hourly.precipitation[j], unit: 'in' },
          { param: 'temperature',   val: hourly.temperature_2m && hourly.temperature_2m[j], unit: '°F' },
          { param: 'snowfall',      val: hourly.snowfall && hourly.snowfall[j], unit: 'in' },
          { param: 'rain',          val: hourly.rain && hourly.rain[j], unit: 'in' },
        ];

        for (var k = 0; k < h.length; k++) {
          if (isNullish(h[k].val)) continue;
          // Skip zero snowfall to reduce noise
          if (h[k].param === 'snowfall' && h[k].val === 0) continue;
          rows.push({
            location_id:   loc.id,
            location_name: loc.name,
            latitude:      loc.lat,
            longitude:     loc.lon,
            parameter:     h[k].param,
            value:         h[k].val,
            unit:          h[k].unit,
            interval:      'hourly',
            is_forecast:   isForecast,
            recorded_at:   recAt,
          });
        }
      }

      // — Daily rows —
      var dTimes = daily.time || [];
      for (var j = 0; j < dTimes.length; j++) {
        var recAt = fmtDay(dTimes[j]);
        var isForecast = new Date(dTimes[j]) >= new Date(new Date().toDateString()) ? 1 : 0;

        var d = [
          { param: 'precip_daily',    val: daily.precipitation_sum && daily.precipitation_sum[j], unit: 'in' },
          { param: 'temp_max_daily',  val: daily.temperature_2m_max && daily.temperature_2m_max[j], unit: '°F' },
          { param: 'temp_min_daily',  val: daily.temperature_2m_min && daily.temperature_2m_min[j], unit: '°F' },
          { param: 'snowfall_daily',  val: daily.snowfall_sum && daily.snowfall_sum[j], unit: 'in' },
        ];

        for (var k = 0; k < d.length; k++) {
          if (isNullish(d[k].val)) continue;
          if (d[k].param === 'snowfall_daily' && d[k].val === 0) continue;
          rows.push({
            location_id:   loc.id,
            location_name: loc.name,
            latitude:      loc.lat,
            longitude:     loc.lon,
            parameter:     d[k].param,
            value:         d[k].val,
            unit:          d[k].unit,
            interval:      'daily',
            is_forecast:   isForecast,
            recorded_at:   recAt,
          });
        }
      }

    } catch (err) {
      console.error('[Weather] Failed to fetch ' + loc.name + ':', err.message);
    }
  }

  return rows;
}

/**
 * Build a compact weather summary string for AI context.
 * Takes rows from getRecentWeather() DB query.
 */
function buildWeatherContext(weatherRows) {
  if (!weatherRows || !weatherRows.length) return '';

  var byLoc = {};
  weatherRows.forEach(function(r) {
    if (!byLoc[r.location_id]) byLoc[r.location_id] = { name: r.location_name, rows: [] };
    byLoc[r.location_id].rows.push(r);
  });

  var ctx = '\n--- WEATHER & UPSTREAM PRECIPITATION ---\n';

  // Past 48h precip totals per location
  var cutoff48h = new Date(Date.now() - 48 * 3600 * 1000);
  ctx += 'Past 48-hour precipitation totals:\n';
  Object.keys(byLoc).forEach(function(locId) {
    var loc = byLoc[locId];
    var recent = loc.rows.filter(function(r) {
      return r.parameter === 'precipitation' && r.interval === 'hourly' && !r.is_forecast && new Date(r.recorded_at + 'Z') >= cutoff48h;
    });
    var total = recent.reduce(function(s, r) { return s + r.value; }, 0);
    var hasSnow = loc.rows.some(function(r) { return r.parameter === 'snowfall' && !r.is_forecast && r.value > 0 && new Date(r.recorded_at + 'Z') >= cutoff48h; });
    ctx += '  ' + loc.name + ': ' + total.toFixed(2) + ' in' + (hasSnow ? ' (includes snowfall)' : '') + '\n';
  });

  // 7-day daily forecast
  var today = new Date().toISOString().slice(0, 10);
  ctx += '\n7-day forecast (daily precip in inches):\n';
  Object.keys(byLoc).forEach(function(locId) {
    var loc = byLoc[locId];
    var dailyForecast = loc.rows.filter(function(r) {
      return r.parameter === 'precip_daily' && r.interval === 'daily' && r.is_forecast && r.recorded_at.slice(0, 10) >= today;
    }).sort(function(a, b) { return a.recorded_at.localeCompare(b.recorded_at); }).slice(0, 7);

    if (dailyForecast.length) {
      var vals = dailyForecast.map(function(r) { return r.value.toFixed(2); }).join(', ');
      ctx += '  ' + loc.name + ': [' + vals + ']\n';
    }
  });

  // Current air temp — use whichever location has the most recent reading
  var allTempRows = [];
  Object.keys(byLoc).forEach(function(locId) {
    byLoc[locId].rows.forEach(function(r) {
      if (r.parameter === 'temperature' && !r.is_forecast) allTempRows.push(r);
    });
  });
  if (allTempRows.length) {
    allTempRows.sort(function(a, b) { return b.recorded_at.localeCompare(a.recorded_at); });
    var latestTemp = allTempRows[0];
    ctx += '\nCurrent air temp (' + latestTemp.location_name + '): ' + latestTemp.value.toFixed(1) + '°F\n';
  }

  ctx += '---\n';
  return ctx;
}

module.exports = { fetchWeatherData, buildWeatherContext, deriveWeatherLocations };
