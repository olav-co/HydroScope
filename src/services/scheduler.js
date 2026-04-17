const cron = require('node-cron');
const { fetchCurrentReadings, discoverNetworkTopology } = require('./usgs');
const { fetchWeatherData } = require('./weather');
const { processNextPair } = require('./waterways');
const db = require('../db/database');

// ── Config ────────────────────────────────────────────────────────────────────

let config;
function loadConfig() {
  if (!config) config = require('../../config/config.json');
  return config;
}

function reloadConfig() {
  config = null;
  const p = require.resolve('../../config/config.json');
  if (require.cache[p]) delete require.cache[p];
}

// Defaults and valid intervals per service
const SERVICE_META = {
  usgs: {
    // USGS publishes data every 15 min — polling faster than that is pointless
    unit: 'minutes',
    defaultInterval: 15,
    validIntervals: [15, 30, 60, 120, 240, 360],
    intervalKey: 'intervalMinutes',
  },
  weather: {
    // Forecast models update every few hours; hourly is already overkill
    unit: 'minutes',
    defaultInterval: 60,
    validIntervals: [60, 120, 360, 720, 1440],
    intervalKey: 'intervalMinutes',
  },
  topology: {
    // NLDI network topology changes at most when new gauges come online — daily/weekly is plenty
    unit: 'hours',
    defaultInterval: 24,
    validIntervals: [12, 24, 48, 168],
    intervalKey: 'intervalHours',
  },
  waterways: {
    // River geometry doesn't move — schedule just catches new pairs from topology updates
    unit: 'hours',
    defaultInterval: 24,
    validIntervals: [6, 12, 24, 48, 168],
    intervalKey: 'intervalHours',
  },
};

function getServiceConfig(name) {
  const cfg = loadConfig();
  const sched = (cfg.scheduler && cfg.scheduler.services && cfg.scheduler.services[name]) || {};
  const meta  = SERVICE_META[name];
  const key   = meta.intervalKey;
  const raw   = sched[key] != null ? sched[key] : meta.defaultInterval;
  const interval = meta.validIntervals.includes(raw) ? raw : meta.defaultInterval;
  return {
    enabled: sched.enabled !== false,
    [key]: interval,
    interval,
    unit: meta.unit,
    validIntervals: meta.validIntervals,
  };
}

function getAllServiceConfigs() {
  const result = {};
  for (const name of Object.keys(SERVICE_META)) {
    const svc = getServiceConfig(name);
    result[name] = { ...svc, ...SERVICE_META[name] };
  }
  return result;
}

// ── Running state ─────────────────────────────────────────────────────────────

const _running = { usgs: false, weather: false, topology: false, waterways: false };

function getRunningState() {
  return { ..._running };
}

// ── Cron expression helpers ───────────────────────────────────────────────────

function minutesToCron(m) {
  if (m >= 1440) return '0 0 * * *';
  if (m >= 60)   return `0 */${Math.floor(m / 60)} * * *`;
  return `*/${m} * * * *`;
}

function hoursToCron(h) {
  if (h >= 168) return '0 0 * * 0';     // weekly
  if (h >= 48)  return `0 0 */${Math.floor(h / 24)} * *`; // every N days
  if (h >= 24)  return '0 0 * * *';     // daily
  return `0 */${h} * * *`;              // every N hours
}

// ── Individual run functions ──────────────────────────────────────────────────

async function runFetch() {
  if (_running.usgs) return { skipped: true };
  _running.usgs = true;
  const cfg = loadConfig();
  const sites  = cfg.usgs.sites;
  const params = cfg.usgs.parameters;
  const siteIds = sites.map(s => s.id);
  console.log(`[Scheduler] Fetching USGS data for ${siteIds.length} sites…`);
  try {
    const rows    = await fetchCurrentReadings(siteIds, params);
    const stored  = db.insertMeasurements(rows);
    db.logFetch({ status: 'success', sites_attempted: siteIds.length, records_stored: stored });
    console.log(`[Scheduler] Stored ${stored} new USGS records.`);
    return { ok: true, records_stored: stored, sites: siteIds.length };
  } catch (err) {
    console.error('[Scheduler] USGS fetch error:', err.message);
    db.logFetch({ status: 'error', sites_attempted: siteIds.length, records_stored: 0, error_message: err.message });
    return { ok: false, error: err.message };
  } finally {
    _running.usgs = false;
  }
}

async function runWeatherFetch() {
  if (_running.weather) return { skipped: true };
  _running.weather = true;
  console.log('[Scheduler] Fetching Open-Meteo weather data…');
  try {
    const rows   = await fetchWeatherData();
    const stored = db.insertWeatherReadings(rows);
    console.log(`[Scheduler] Stored ${stored} weather records.`);
    return { ok: true, records_stored: stored };
  } catch (err) {
    console.error('[Scheduler] Weather fetch error:', err.message);
    return { ok: false, error: err.message };
  } finally {
    _running.weather = false;
  }
}

async function runTopologyDiscovery() {
  if (_running.topology) return { skipped: true };
  _running.topology = true;
  const siteIds = db.getAllSites().map(s => s.site_id);
  console.log('[Scheduler] Discovering watershed topology for', siteIds.length, 'sites…');
  try {
    const connections = await discoverNetworkTopology(siteIds);
    db.syncNLDIConnections(connections);
    console.log(`[Scheduler] Topology synced: ${connections.length} connection(s).`);
    connections.forEach(c => console.log(`  ${c.from_site_id} → ${c.to_site_id}`));
    return { ok: true, connections: connections.length };
  } catch (err) {
    console.error('[Scheduler] Topology discovery error:', err.message);
    return { ok: false, error: err.message };
  } finally {
    _running.topology = false;
  }
}

async function runWaterwayTick() {
  if (_running.waterways) return { skipped: true };
  _running.waterways = true;
  try {
    const ran = await processNextPair();
    return { ok: true, processed: ran };
  } catch (err) {
    console.error('[Scheduler] Waterway tick error:', err.message);
    return { ok: false, error: err.message };
  } finally {
    _running.waterways = false;
  }
}

// ── Individual task lifecycle ─────────────────────────────────────────────────

const _tasks = { usgs: null, weather: null, topology: null, waterways: null };

function stopTask(name) {
  if (_tasks[name]) { _tasks[name].stop(); _tasks[name] = null; }
}

function startTask(name) {
  stopTask(name);
  const svc = getServiceConfig(name);
  if (!svc.enabled) {
    console.log(`[Scheduler] ${name} disabled — skipping.`);
    return;
  }

  let expr, fn;
  switch (name) {
    case 'usgs':
      expr = minutesToCron(svc.interval);
      fn   = () => runFetch().catch(e => console.error('[Scheduler] usgs error:', e.message));
      break;
    case 'weather':
      expr = minutesToCron(svc.interval);
      fn   = () => runWeatherFetch().catch(e => console.error('[Scheduler] weather error:', e.message));
      break;
    case 'topology':
      expr = hoursToCron(svc.interval);
      fn   = () => runTopologyDiscovery().catch(e => console.error('[Scheduler] topology error:', e.message));
      break;
    case 'waterways':
      expr = minutesToCron(svc.interval);
      fn   = () => runWaterwayTick().catch(e => console.error('[Scheduler] waterways error:', e.message));
      break;
    default:
      return;
  }

  _tasks[name] = cron.schedule(expr, fn);
  console.log(`[Scheduler] ${name} scheduled: ${expr}`);
}

/**
 * Stop + restart a single service task with fresh config.
 * Called after settings save when only one service changed.
 */
function restartService(name) {
  startTask(name);
}

// ── Full scheduler start / stop ───────────────────────────────────────────────

function startScheduler() {
  const cfg = loadConfig();

  if (!(cfg.scheduler && cfg.scheduler.enabled)) {
    console.log('[Scheduler] Disabled via config.');
    return;
  }

  // Seed monitoring sites from config
  const sites = cfg.usgs.sites;
  for (const site of sites) {
    db.upsertSite({
      site_id: site.id, name: site.name, type: site.type || 'river',
      latitude: site.lat, longitude: site.lon, description: site.description || null,
    });
  }
  console.log(`[Scheduler] Seeded ${sites.length} monitoring sites.`);

  // Immediate runs on startup
  runFetch();
  runWeatherFetch();
  runTopologyDiscovery();

  // Schedule all tasks
  for (const name of Object.keys(SERVICE_META)) startTask(name);
}

function stopScheduler() {
  for (const name of Object.keys(_tasks)) stopTask(name);
}

module.exports = {
  startScheduler, stopScheduler, reloadConfig,
  runFetch, runWeatherFetch, runTopologyDiscovery, runWaterwayTick,
  restartService, getRunningState, getAllServiceConfigs, SERVICE_META,
};
