const express = require('express');
const path = require('path');
const { initDatabase } = require('./db/database');
const { startScheduler } = require('./services/scheduler');

const pagesRouter   = require('./routes/pages');
const dataRouter    = require('./routes/api/data');
const profileRouter = require('./routes/api/profile');
const insightsRouter = require('./routes/api/insights');

const app = express();

let cfg;
try {
  cfg = require('../config/config.json');
} catch {
  console.error('[HydroScope] Missing config/config.json — copy config/config.example.json and fill in your keys.');
  process.exit(1);
}

const PORT = process.env.PORT || cfg.server?.port || 3000;
const HOST = process.env.HOST || cfg.server?.host || '0.0.0.0';

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// ── View Engine ───────────────────────────────────────────────────────────────
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// ── Routes ────────────────────────────────────────────────────────────────────
app.use('/',            pagesRouter);
app.use('/api/data',    dataRouter);
app.use('/api/profile', profileRouter);
app.use('/api/insights',insightsRouter);

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
  });
}

boot().catch(err => {
  console.error('[HydroScope] Fatal startup error:', err);
  process.exit(1);
});
