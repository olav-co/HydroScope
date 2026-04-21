const cron = require('node-cron');
const { fetchCurrentReadings, discoverNetworkTopology } = require('./usgs');
const { fetchWeatherData } = require('./weather');
const { processNextPair } = require('./waterways');
const { fetchCurrentReadingsCWMS } = require('./cwms');
const db = require('../db/database');

// ── Config ────────────────────────────────────────────────────────────────────

const { getDatasourcesConfig, reloadDatasourcesConfig } = require('./config');

function loadConfig() {
  return getDatasourcesConfig();
}

function reloadConfig() {
  reloadDatasourcesConfig();
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
  cwms: {
    // CWMS publishes hourly; some water quality sensors report every 30 min.
    // Default 15 min so new values are caught promptly without hammering the API.
    unit: 'minutes',
    defaultInterval: 15,
    validIntervals: [15, 30, 60, 120, 360],
    intervalKey: 'intervalMinutes',
  },
  basinSync: {
    // Basin geometry is stable — daily sweep catches newly added sites.
    unit: 'hours',
    defaultInterval: 24,
    validIntervals: [12, 24, 48, 168],
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

const _running = { usgs: false, weather: false, topology: false, waterways: false, cwms: false, basinSync: false };

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
  const cfg     = loadConfig();
  const params  = (cfg.usgs && cfg.usgs.parameters) || ['00060', '00065', '00010', '00300', '00400'];
  const siteIds = db.getActiveSites('usgs').map(s => s.site_id);
  if (!siteIds.length) {
    _running.usgs = false;
    return { ok: true, skipped: true, reason: 'No active USGS sites in DB.' };
  }
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
  const allSites = db.getAllSites();
  console.log('[Scheduler] Discovering watershed topology for', allSites.length, 'sites…');
  try {
    const connections = await discoverNetworkTopology(allSites);
    db.syncNLDIConnections(connections);
    console.log(`[Scheduler] Topology synced: ${connections.length} connection(s).`);
    connections.forEach(c => console.log(`  ${c.from_site_id} → ${c.to_site_id}`));

    // After topology sync, immediately drain any pending waterway pairs so new
    // connections get river geometry without waiting for the next cron tick.
    const pending = db.getPendingWaterwayPairs();
    if (pending.length > 0) {
      console.log(`[Scheduler] ${pending.length} waterway pair(s) queued — starting drain…`);
      drainWaterwayPairs();
    }

    return { ok: true, connections: connections.length };
  } catch (err) {
    console.error('[Scheduler] Topology discovery error:', err.message);
    return { ok: false, error: err.message };
  } finally {
    _running.topology = false;
  }
}

/**
 * Self-scheduling loop: process one waterway pair every 4 minutes until
 * no pending pairs remain.  Runs in the background; does not block the caller.
 */
function drainWaterwayPairs() {
  if (_running.waterways) return;  // tick already in flight — it will self-reschedule
  runWaterwayTick()
    .then(result => {
      if (result && result.processed) {
        // More pairs may be waiting — schedule the next one in 4 minutes
        if (db.getPendingWaterwayPairs().length > 0) {
          setTimeout(drainWaterwayPairs, 4 * 60 * 1000);
        } else {
          console.log('[Scheduler] All waterway pairs processed.');
        }
      }
    })
    .catch(e => console.error('[Scheduler] drainWaterwayPairs error:', e.message));
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

async function runCwmsFetch() {
  if (_running.cwms) return { skipped: true };
  _running.cwms = true;
  const cfg = loadConfig();

  // Build site list from DB: active CWMS sites + their timeseries from site_timeseries table.
  // Shape expected by fetchCurrentReadingsCWMS: { locationId, timeseries: [...] }
  const dbSites = db.getAllSitesWithTimeseries().filter(s => s.source === 'cwms' && s.enabled);
  if (!dbSites.length) {
    _running.cwms = false;
    return { ok: true, skipped: true, reason: 'No active CWMS sites in DB.' };
  }
  // Map DB shape → fetchCurrentReadingsCWMS expected shape
  const sites = dbSites.map(s => ({ locationId: s.site_id, timeseries: s.timeseries || [] }))
                        .filter(s => s.timeseries.length > 0);

  if (!sites.length) {
    _running.cwms = false;
    return { ok: true, skipped: true, reason: 'Active CWMS sites have no timeseries configured.' };
  }

  console.log(`[Scheduler] Fetching CWMS data for ${sites.length} site(s)…`);
  try {
    const rows    = await fetchCurrentReadingsCWMS(sites, cfg.cwms || {});
    const stored  = db.insertMeasurements(rows);
    db.logFetch({ source: 'cwms', status: 'success', sites_attempted: sites.length, records_stored: stored });
    console.log(`[Scheduler] Stored ${stored} new CWMS records.`);
    return { ok: true, records_stored: stored, sites: sites.length };
  } catch (err) {
    console.error('[Scheduler] CWMS fetch error:', err.message);
    db.logFetch({ source: 'cwms', status: 'error', sites_attempted: sites.length, records_stored: 0, error_message: err.message });
    return { ok: false, error: err.message };
  } finally {
    _running.cwms = false;
  }
}

async function runBasinSync() {
  if (_running.basinSync) return { skipped: true };
  _running.basinSync = true;
  const sites = db.getSitesMissingHuc();
  if (!sites.length) {
    _running.basinSync = false;
    return { ok: true, skipped: true, reason: 'All sites already have HUC8 data.' };
  }
  console.log(`[Scheduler] Basin sync: resolving HUC8 for ${sites.length} site(s)…`);
  let done = 0, failed = 0;
  try {
    const { lookupHuc8ByPoint } = require('../routes/api/basins');
    for (const site of sites) {
      if (!_running.basinSync) break;
      try {
        const huc = await lookupHuc8ByPoint(site.latitude, site.longitude);
        if (huc) {
          db.updateSiteHuc(site.site_id, huc.huc8_code, huc.huc8_name);
          done++;
        } else {
          console.warn(`[Scheduler] Basin sync: no HUC8 found for ${site.site_id} (${site.latitude}, ${site.longitude})`);
          failed++;
        }
      } catch (e) {
        console.warn(`[Scheduler] Basin sync failed for ${site.site_id}:`, e.message);
        failed++;
      }
      await new Promise(r => setTimeout(r, 150));
    }
    console.log(`[Scheduler] Basin sync complete — ${done} updated, ${failed} failed.`);
    return { ok: true, updated: done, failed };
  } catch (err) {
    console.error('[Scheduler] Basin sync error:', err.message);
    return { ok: false, error: err.message };
  } finally {
    _running.basinSync = false;
  }
}

// ── Individual task lifecycle ─────────────────────────────────────────────────

const _tasks = { usgs: null, weather: null, topology: null, waterways: null, cwms: null, basinSync: null };

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
      expr = hoursToCron(svc.interval);   // unit is hours — was incorrectly using minutesToCron
      fn   = () => drainWaterwayPairs();  // drain all pending pairs, not just one
      break;
    case 'cwms':
      expr = minutesToCron(svc.interval);
      fn   = () => runCwmsFetch().catch(e => console.error('[Scheduler] cwms error:', e.message));
      break;
    case 'basinSync':
      expr = hoursToCron(svc.interval);
      fn   = () => runBasinSync().catch(e => console.error('[Scheduler] Basin sync error:', e.message));
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

  const activeSites = db.getAllSitesWithTimeseries();
  const usgsCount   = activeSites.filter(s => s.source === 'usgs' && s.enabled).length;
  const cwmsCount   = activeSites.filter(s => s.source === 'cwms' && s.enabled).length;
  console.log(`[Scheduler] ${usgsCount} active USGS site(s), ${cwmsCount} active CWMS site(s) from DB.`);

  // Immediate runs on startup
  runFetch();
  runWeatherFetch();
  runTopologyDiscovery();
  if (cwmsCount > 0) runCwmsFetch();
  // Resolve basin geometry for any sites not yet assigned a HUC8
  if (db.getSitesMissingHuc().length > 0) runBasinSync();

  // Schedule all tasks
  for (const name of Object.keys(SERVICE_META)) startTask(name);
}

function stopScheduler() {
  for (const name of Object.keys(_tasks)) stopTask(name);
}

module.exports = {
  startScheduler, stopScheduler, reloadConfig,
  runFetch, runWeatherFetch, runTopologyDiscovery, runWaterwayTick, runCwmsFetch, runBasinSync,
  restartService, getRunningState, getAllServiceConfigs, SERVICE_META,
  drainWaterwayPairs,
};
