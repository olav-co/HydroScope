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

-- Fetch job log
CREATE TABLE IF NOT EXISTS fetch_log (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  status          TEXT    NOT NULL,  -- success | error | partial
  sites_attempted INTEGER DEFAULT 0,
  records_stored  INTEGER DEFAULT 0,
  error_message   TEXT,
  executed_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);
