-- ============================================================
-- HydroScope Database Schema
-- ============================================================

-- Monitoring sites (managed via /sites UI, seeded from config on first boot)
CREATE TABLE IF NOT EXISTS sites (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id     TEXT    UNIQUE NOT NULL,
  name        TEXT    NOT NULL,
  type        TEXT    NOT NULL DEFAULT 'river',  -- river, reservoir, dam, stream
  source      TEXT    NOT NULL DEFAULT 'usgs',   -- usgs | cwms
  enabled     INTEGER NOT NULL DEFAULT 1,        -- 1=active, 0=paused
  latitude    REAL,
  longitude   REAL,
  description TEXT,
  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_sites_source_enabled ON sites(source, enabled);
CREATE INDEX IF NOT EXISTS idx_sites_huc8 ON sites(huc8_code);

-- HUC8 basin polygons cache (populated on demand from USGS WBD)
CREATE TABLE IF NOT EXISTS basins (
  huc8_code    TEXT     PRIMARY KEY,
  huc8_name    TEXT     NOT NULL,
  polygon_json TEXT,                         -- GeoJSON Feature string
  fetched_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Per-site CWMS timeseries IDs (CDA fetch scheduling)
CREATE TABLE IF NOT EXISTS site_timeseries (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id    TEXT    NOT NULL,
  ts_id      TEXT    NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(site_id, ts_id),
  FOREIGN KEY (site_id) REFERENCES sites(site_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_site_ts ON site_timeseries(site_id);

-- Time-series measurements (USGS and CWMS)
CREATE TABLE IF NOT EXISTS measurements (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id         TEXT    NOT NULL,
  parameter_code  TEXT    NOT NULL,
  parameter_name  TEXT    NOT NULL,
  value           REAL,
  unit            TEXT,
  source          TEXT    NOT NULL DEFAULT 'usgs',  -- usgs | cwms
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

-- Users (username-only, no passwords)
CREATE TABLE IF NOT EXISTS users (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  username    TEXT    NOT NULL UNIQUE COLLATE NOCASE,
  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
  last_login  DATETIME
);

-- Per-user AI provider + model preference
CREATE TABLE IF NOT EXISTS user_ai_settings (
  user_id    INTEGER NOT NULL PRIMARY KEY,
  provider   TEXT,
  model      TEXT,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Per-user favorite basins
CREATE TABLE IF NOT EXISTS user_favorite_basins (
  user_id    INTEGER NOT NULL,
  huc8_code  TEXT    NOT NULL,
  huc8_name  TEXT,
  added_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id, huc8_code),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Combined site sources: links a 'combined' parent to its USGS/CWMS children.
-- Each child can be toggled on/off independently via the `enabled` flag.
CREATE TABLE IF NOT EXISTS site_sources (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  parent_site_id TEXT    NOT NULL,
  child_site_id  TEXT    NOT NULL,
  enabled        INTEGER NOT NULL DEFAULT 1,
  created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(parent_site_id, child_site_id),
  FOREIGN KEY (parent_site_id) REFERENCES sites(site_id) ON DELETE CASCADE,
  FOREIGN KEY (child_site_id)  REFERENCES sites(site_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_site_sources_parent ON site_sources(parent_site_id);
CREATE INDEX IF NOT EXISTS idx_site_sources_child  ON site_sources(child_site_id);

-- User-defined site groups
CREATE TABLE IF NOT EXISTS site_groups (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id          INTEGER NOT NULL,
  name             TEXT    NOT NULL,
  color            TEXT    NOT NULL DEFAULT '#3b82f6',
  is_published     INTEGER NOT NULL DEFAULT 0,        -- 1 = visible to other users
  published_at     DATETIME,
  source_group_id  INTEGER REFERENCES site_groups(id) ON DELETE SET NULL,  -- FK to originator if this is a saved copy
  synced_at        DATETIME,                          -- last time this copy was synced from source
  updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
  created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_site_groups_user ON site_groups(user_id);

-- Members of each site group
CREATE TABLE IF NOT EXISTS site_group_members (
  group_id INTEGER NOT NULL,
  site_id  TEXT    NOT NULL,
  PRIMARY KEY (group_id, site_id),
  FOREIGN KEY (group_id) REFERENCES site_groups(id) ON DELETE CASCADE,
  FOREIGN KEY (site_id)  REFERENCES sites(site_id)  ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_sgm_site ON site_group_members(site_id);

-- App metadata / migration version tracking
CREATE TABLE IF NOT EXISTS app_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- Fetch job log
CREATE TABLE IF NOT EXISTS fetch_log (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  source          TEXT    NOT NULL DEFAULT 'usgs',  -- usgs | cwms | weather
  status          TEXT    NOT NULL,                 -- success | error | partial
  sites_attempted INTEGER DEFAULT 0,
  records_stored  INTEGER DEFAULT 0,
  error_message   TEXT,
  executed_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);
