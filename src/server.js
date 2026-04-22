const express      = require('express');
const path         = require('path');
const cookieParser = require('cookie-parser');
const db           = require('./db/database');
const { startScheduler, drainWaterwayPairs } = require('./services/scheduler');
const { detectAndPairSites } = require('./services/pairing');
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
const usersRouter       = require('./routes/api/users');
const { router: basinsRouter } = require('./routes/api/basins');
const radarRouter       = require('./routes/api/radar');
const groupsRouter      = require('./routes/api/groups');

const app = express();

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

// ── View Engine ───────────────────────────────────────────────────────────────
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// ── Login / Logout ────────────────────────────────────────────────────────────
app.get('/login', (req, res) => {
  if (req.cookies.hydro_uid) {
    const u = db.getUserById(parseInt(req.cookies.hydro_uid, 10));
    if (u) return res.redirect('/');
  }
  const existingUsers = db.getAllUsers();
  res.render('login', { error: null, existingUsers });
});

app.post('/login', (req, res) => {
  const username = (req.body.username || '').trim();
  if (!username) {
    return res.render('login', { error: 'Please enter a username.', existingUsers: db.getAllUsers() });
  }
  try {
    const user = db.findOrCreateUser(username);
    res.cookie('hydro_uid', String(user.id), { httpOnly: true, maxAge: 1000 * 60 * 60 * 24 * 30 });
    res.redirect('/');
  } catch (err) {
    res.render('login', { error: err.message, existingUsers: db.getAllUsers() });
  }
});

app.get('/logout', (req, res) => {
  res.clearCookie('hydro_uid');
  res.redirect('/login');
});

// ── Auth middleware ───────────────────────────────────────────────────────────
app.use((req, res, next) => {
  const uid = req.cookies.hydro_uid;
  if (!uid) {
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Not authenticated' });
    return res.redirect('/login');
  }
  const user = db.getUserById(parseInt(uid, 10));
  if (!user) {
    res.clearCookie('hydro_uid');
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Not authenticated' });
    return res.redirect('/login');
  }
  req.user = user;
  res.locals.user = user;
  next();
});

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
app.use('/api/users',        usersRouter);
app.use('/api/basins',       basinsRouter);
app.use('/api/radar',        radarRouter);
app.use('/api/groups',       groupsRouter);

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

  ensureMigrated(db);
  applySeedFile(db);

  const dsCfg = getDatasourcesConfig();
  const PORT  = process.env.PORT || (dsCfg.server && dsCfg.server.port) || 3000;
  const HOST  = process.env.HOST || (dsCfg.server && dsCfg.server.host) || '0.0.0.0';

  detectAndPairSites();
  startScheduler();
  app.listen(PORT, HOST, () => {
    console.log(`[HydroScope] Listening on http://${HOST}:${PORT}`);
    setTimeout(drainWaterwayPairs, 3000);
  });
}

boot().catch(err => {
  console.error('[HydroScope] Fatal startup error:', err);
  process.exit(1);
});
