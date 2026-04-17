const express = require('express');
const path = require('path');
const { initDatabase } = require('./db/database');
const { startScheduler } = require('./services/scheduler');
const { runBackgroundCrawl } = require('./services/waterways');

const pagesRouter       = require('./routes/pages');
const dataRouter        = require('./routes/api/data');
const profileRouter     = require('./routes/api/profile');
const insightsRouter    = require('./routes/api/insights');
const exportRouter      = require('./routes/api/export');
const annotationsRouter = require('./routes/api/annotations');
const wqRouter          = require('./routes/api/wq');
const connectionsRouter = require('./routes/api/connections');
const settingsRouter    = require('./routes/api/settings');

const app = express();

let cfg;
try {
  cfg = require('../config/config.json');
  console.log('[HydroScope] Loaded config/config.json');
} catch {
  try {
    cfg = require('../config/config.example.json');
    console.warn('[HydroScope] config.json not found — loaded config.example.json (add your API keys to config.json)');
  } catch {
    console.error('[HydroScope] No config file found. Create config/config.json from config.example.json.');
    process.exit(1);
  }
}

const PORT = process.env.PORT || (cfg.server && cfg.server.port) || 3000;
const HOST = process.env.HOST || (cfg.server && cfg.server.host) || '0.0.0.0';

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// ── View Engine ───────────────────────────────────────────────────────────────
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// ── Routes ────────────────────────────────────────────────────────────────────
app.use('/',                 pagesRouter);
app.use('/api/data',         dataRouter);
app.use('/api/profile',      profileRouter);
app.use('/api/insights',     insightsRouter);
app.use('/api/export',       exportRouter);
app.use('/api/annotations',  annotationsRouter);
app.use('/api/wq',           wqRouter);
app.use('/api/connections',  connectionsRouter);
app.use('/api/settings',     settingsRouter);

// ── 404 / Error handlers ─────────────────────────────────────────────────────
app.use((req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, req, res, _next) => {
  console.error('[Error]', err.message);
  res.status(500).json({ error: err.message });
});

// ── Boot ──────────────────────────────────────────────────────────────────────
async function boot() {
  initDatabase();
  console.log('[DB] Initialized.');
  startScheduler();
  app.listen(PORT, HOST, () => {
    console.log(`[HydroScope] Listening on http://${HOST}:${PORT}`);
    // Crawl waterway geometry in background for any uncached connection pairs.
    // Non-blocking — the server is already accepting requests.
    setTimeout(runBackgroundCrawl, 3000);
  });
}

boot().catch(err => {
  console.error('[HydroScope] Fatal startup error:', err);
  process.exit(1);
});
