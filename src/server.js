const express = require('express');
const path = require('path');
const db = require('./db/database');
const { startScheduler } = require('./services/scheduler');
const { runBackgroundCrawl } = require('./services/waterways');
const { ensureMigrated, applySeedFile, getDatasourcesConfig } = require('./services/config');

const pagesRouter       = require('./routes/pages');
const dataRouter        = require('./routes/api/data');
const profileRouter     = require('./routes/api/profile');
const insightsRouter    = require('./routes/api/insights');
const exportRouter      = require('./routes/api/export');
const annotationsRouter = require('./routes/api/annotations');
const wqRouter          = require('./routes/api/wq');
const connectionsRouter = require('./routes/api/connections');
const settingsRouter    = require('./routes/api/settings');
const sitesRouter       = require('./routes/api/sites');

const app = express();

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(express.json({ limit: '10mb' }));
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
app.use('/api/sites',        sitesRouter);

// ── 404 / Error handlers ─────────────────────────────────────────────────────
app.use((req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, req, res, _next) => {
  console.error('[Error]', err.message);
  res.status(500).json({ error: err.message });
});

// ── Boot ──────────────────────────────────────────────────────────────────────
async function boot() {
  db.initDatabase();
  console.log('[DB] Initialized.');

  // Migrate legacy config.json → split files; seed sites on first run
  ensureMigrated(db);
  applySeedFile(db);

  const dsCfg = getDatasourcesConfig();
  const PORT  = process.env.PORT || (dsCfg.server && dsCfg.server.port) || 3000;
  const HOST  = process.env.HOST || (dsCfg.server && dsCfg.server.host) || '0.0.0.0';

  startScheduler();
  app.listen(PORT, HOST, () => {
    console.log(`[HydroScope] Listening on http://${HOST}:${PORT}`);
    setTimeout(runBackgroundCrawl, 3000);
  });
}

boot().catch(err => {
  console.error('[HydroScope] Fatal startup error:', err);
  process.exit(1);
});
