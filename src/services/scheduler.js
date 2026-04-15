const cron = require('node-cron');
const { fetchCurrentReadings } = require('./usgs');
const db = require('../db/database');

let config;
function loadConfig() {
  if (!config) config = require('../../config/config.json');
  return config;
}

let schedulerTask = null;

async function runFetch() {
  const cfg = loadConfig();
  const sites = cfg.usgs.sites;
  const params = cfg.usgs.parameters;
  const siteIds = sites.map(s => s.id);

  console.log(`[Scheduler] Fetching USGS data for ${siteIds.length} sites...`);

  try {
    const rows = await fetchCurrentReadings(siteIds, params);
    const stored = db.insertMeasurements(rows);

    db.logFetch({
      status: 'success',
      sites_attempted: siteIds.length,
      records_stored: stored,
    });

    console.log(`[Scheduler] Stored ${stored} new records.`);
  } catch (err) {
    console.error('[Scheduler] Fetch error:', err.message);
    db.logFetch({
      status: 'error',
      sites_attempted: siteIds.length,
      records_stored: 0,
      error_message: err.message,
    });
  }
}

function startScheduler() {
  const cfg = loadConfig();

  if (!cfg.scheduler?.enabled) {
    console.log('[Scheduler] Disabled via config.');
    return;
  }

  const intervalMin = cfg.scheduler?.fetchIntervalMinutes ?? 15;

  // Validate interval
  const validIntervals = [5, 10, 15, 20, 30, 60];
  const interval = validIntervals.includes(intervalMin) ? intervalMin : 15;

  const cronExpr = `*/${interval} * * * *`;
  console.log(`[Scheduler] Scheduled USGS fetch every ${interval} minutes (${cronExpr})`);

  // Seed sites from config into DB
  const sites = cfg.usgs.sites;
  for (const site of sites) {
    db.upsertSite({ site_id: site.id, name: site.name, type: site.type || 'river',
                    latitude: site.lat, longitude: site.lon, description: site.description || null });
  }
  console.log(`[Scheduler] Seeded ${sites.length} monitoring sites.`);

  // Run immediately on startup, then on schedule
  runFetch();

  schedulerTask = cron.schedule(cronExpr, runFetch);
}

function stopScheduler() {
  if (schedulerTask) {
    schedulerTask.stop();
    schedulerTask = null;
  }
}

module.exports = { startScheduler, stopScheduler, runFetch };
