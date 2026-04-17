-- ============================================================
-- HydroScope Database Schema
-- ============================================================

-- Monitoring sites (seeded from config on startup)
CREATE TABLE IF NOT EXISTS sites (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id     TEXT    UNIQUE NOT NULL,
  name        TEXT    NOT NULL,
  type        TEXT    NOT NULL DEFAULT 'river',  -- river, reservoir, stream
  latitude    REAL,
  longitude   REAL,
  description TEXT,
  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Time-series measurements from USGS
CREATE TABLE IF NOT EXISTS measurements (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id         TEXT    NOT NULL,
  parameter_code  TEXT    NOT NULL,
  parameter_name  TEXT    NOT NULL,
  value           REAL,
  unit            TEXT,
  recorded_at     DATETIME NOT NULL,
  fetched_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (site_id) REFERENCES sites(site_id)
);

CREATE INDEX IF NOT EXISTS idx_meas_site_time  ON measurements(site_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_meas_param_time ON measurements(parameter_code, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_meas_recorded   ON measurements(recorded_at DESC);

-- Prevent duplicate records for same site/parameter/timestamp
CREATE UNIQUE INDEX IF NOT EXISTS idx_meas_unique
  ON measurements(site_id, parameter_code, recorded_at);

-- User profile (single-row, updated in-place)
CREATE TABLE IF NOT EXISTS user_profile (
  id                INTEGER PRIMARY KEY DEFAULT 1,
  name              TEXT,
  organization      TEXT,
  role              TEXT    NOT NULL DEFAULT 'general',
  sub_role          TEXT,
  interests         TEXT    DEFAULT '[]',         -- JSON array of interest tags
  preferred_sites   TEXT    DEFAULT '[]',         -- JSON array of site_ids
  bio               TEXT,                         -- free-text context for AI
  notify_thresholds TEXT    DEFAULT '{}',         -- JSON threshold config
  created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Insert default profile row if not present
INSERT OR IGNORE INTO user_profile (id, role) VALUES (1, 'general');

-- AI insight cache
CREATE TABLE IF NOT EXISTS insights_cache (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  query_hash   TEXT    NOT NULL,
  profile_role TEXT,
  context_json TEXT,
  prompt       TEXT,
  response     TEXT,
  created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_insights_hash ON insights_cache(query_hash, created_at DESC);

-- Weather readings from Open-Meteo (past actuals + forecast)
CREATE TABLE IF NOT EXISTS weather_readings (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  location_id   TEXT    NOT NULL,
  location_name TEXT    NOT NULL,
  latitude      REAL,
  longitude     REAL,
  parameter     TEXT    NOT NULL,
  value         REAL    NOT NULL,
  unit          TEXT,
  interval      TEXT    NOT NULL DEFAULT 'hourly',  -- hourly | daily
  is_forecast   INTEGER NOT NULL DEFAULT 0,         -- 1 = future projection
  recorded_at   DATETIME NOT NULL,
  fetched_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX  IF NOT EXISTS idx_wx_loc_time ON weather_readings(location_id, recorded_at DESC);
CREATE INDEX  IF NOT EXISTS idx_wx_param    ON weather_readings(parameter, recorded_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_wx_unique
  ON weather_readings(location_id, parameter, interval, recorded_at);

-- Upstream/downstream relationships between monitoring sites
CREATE TABLE IF NOT EXISTS site_connections (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  from_site_id TEXT NOT NULL,   -- upstream site
  to_site_id   TEXT NOT NULL,   -- downstream site
  label        TEXT DEFAULT 'flows into',
  notes        TEXT,
  created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(from_site_id, to_site_id),
  FOREIGN KEY (from_site_id) REFERENCES sites(site_id),
  FOREIGN KEY (to_site_id)   REFERENCES sites(site_id)
);

CREATE INDEX IF NOT EXISTS idx_conn_from ON site_connections(from_site_id);
CREATE INDEX IF NOT EXISTS idx_conn_to   ON site_connections(to_site_id);

-- Event annotations (dam operators log notable events against a time and optional site/parameter)
CREATE TABLE IF NOT EXISTS event_annotations (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id        TEXT,                               -- NULL = global / cross-site
  parameter_code TEXT,                               -- NULL = any parameter
  category       TEXT    NOT NULL DEFAULT 'event',   -- event | alert | maintenance | observation
  label          TEXT    NOT NULL,                   -- short label shown on chart
  note           TEXT,                               -- longer description (optional)
  annotated_at   DATETIME NOT NULL,                  -- the point in time being annotated
  created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (site_id) REFERENCES sites(site_id)
);

CREATE INDEX IF NOT EXISTS idx_ann_site     ON event_annotations(site_id, annotated_at DESC);
CREATE INDEX IF NOT EXISTS idx_ann_time     ON event_annotations(annotated_at DESC);
CREATE INDEX IF NOT EXISTS idx_ann_category ON event_annotations(category);

-- Water quality permit limits (configurable per site/parameter)
CREATE TABLE IF NOT EXISTS wq_permit_limits (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id        TEXT    NOT NULL,
  parameter_code TEXT    NOT NULL,
  limit_type     TEXT    NOT NULL DEFAULT 'max',     -- min | max | target
  value          REAL    NOT NULL,
  label          TEXT,                               -- e.g. "EPA Max", "Target", "Permit Floor"
  color          TEXT    DEFAULT '#ef4444',          -- hex color for chart band
  created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (site_id) REFERENCES sites(site_id)
);

CREATE INDEX IF NOT EXISTS idx_wq_site_param ON wq_permit_limits(site_id, parameter_code);

-- Pre-computed waterway geometry (OSM Overpass paths, cached server-side)
CREATE TABLE IF NOT EXISTS waterway_paths (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  from_site_id TEXT    NOT NULL,
  to_site_id   TEXT    NOT NULL,
  path_json    TEXT,                          -- JSON [[lat,lon],...] or null = no path found
  status       TEXT    NOT NULL DEFAULT 'pending', -- pending | ok | none | error
  fetched_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(from_site_id, to_site_id),
  FOREIGN KEY (from_site_id) REFERENCES sites(site_id),
  FOREIGN KEY (to_site_id)   REFERENCES sites(site_id)
);

CREATE INDEX IF NOT EXISTS idx_wp_from ON waterway_paths(from_site_id);
CREATE INDEX IF NOT EXISTS idx_wp_to   ON waterway_paths(to_site_id);

-- App metadata / migration version tracking
CREATE TABLE IF NOT EXISTS app_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Fetch job log
CREATE TABLE IF NOT EXISTS fetch_log (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  status          TEXT    NOT NULL,  -- success | error | partial
  sites_attempted INTEGER DEFAULT 0,
  records_stored  INTEGER DEFAULT 0,
  error_message   TEXT,
  executed_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);
