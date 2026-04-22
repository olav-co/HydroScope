const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const DB_PATH = path.join(__dirname, '../../data/hydroscope.db');
const SCHEMA_PATH = path.join(__dirname, 'schema.sql');

let db;

function getDb() {
  if (!db) throw new Error('Database not initialized. Call initDatabase() first.');
  return db;
}

function initDatabase() {
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  // ── Phase 1: column migrations ── run BEFORE schema.sql so that any indexes
  // in schema.sql that reference new columns don't fail on existing databases.
  try { db.exec(`ALTER TABLE site_connections ADD COLUMN source TEXT DEFAULT 'manual'`); } catch (_) {}
  try { db.exec(`ALTER TABLE sites ADD COLUMN source TEXT NOT NULL DEFAULT 'usgs'`); } catch (_) {}
  try { db.exec(`ALTER TABLE sites ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1`); } catch (_) {}
  try { db.exec(`ALTER TABLE measurements ADD COLUMN source TEXT NOT NULL DEFAULT 'usgs'`); } catch (_) {}
  try { db.exec(`ALTER TABLE fetch_log ADD COLUMN source TEXT NOT NULL DEFAULT 'usgs'`); } catch (_) {}
  try { db.exec(`ALTER TABLE sites ADD COLUMN huc8_code TEXT`); } catch (_) {}
  try { db.exec(`ALTER TABLE sites ADD COLUMN huc8_name TEXT`); } catch (_) {}
  try { db.exec(`ALTER TABLE user_profile ADD COLUMN user_id INTEGER`); } catch (_) {}

  // ── Phase 2: apply schema (CREATE TABLE/INDEX IF NOT EXISTS — safe on existing DB)
  const schema = fs.readFileSync(SCHEMA_PATH, 'utf8');
  db.exec(schema);


  // One-time migration: clear all waterway_paths cached with the old untrimmed
  // NLDI algorithm (v1 returned full downstream path to ocean).
  // After clearing, paths will be re-fetched with comid-trimmed logic (v2).
  try {
    const vRow = db.prepare(`SELECT value FROM app_meta WHERE key = 'waterways_algo_v'`).get();
    if (!vRow || vRow.value !== '2') {
      db.exec(`DELETE FROM waterway_paths`);
      db.prepare(`INSERT OR REPLACE INTO app_meta (key, value) VALUES ('waterways_algo_v', '2')`).run();
      console.log('[DB] Cleared waterway_paths cache (upgrading to comid-trimmed NLDI v2)');
    }
  } catch (_) {}

  // Migration v3: clear ALL waterway_paths records for CWMS-site pairs.
  // v3a: 'none'/'error' only (comid-nav upgrade)
  // v3b: also clear 'ok' records — earlier paths used canal/tailrace snaps
  //      instead of geographic trimming, producing garbage loops. Re-fetch all.
  try {
    const v3 = db.prepare(`SELECT value FROM app_meta WHERE key = 'waterways_cwms_v'`).get();
    if (!v3 || v3.value !== '2') {
      const deleted = db.prepare(`
        DELETE FROM waterway_paths
        WHERE CAST(from_site_id AS TEXT) GLOB '*[A-Za-z]*'
           OR CAST(to_site_id   AS TEXT) GLOB '*[A-Za-z]*'
      `).run();
      db.prepare(`INSERT OR REPLACE INTO app_meta (key, value) VALUES ('waterways_cwms_v', '2')`).run();
      if (deleted.changes > 0)
        console.log(`[DB] Cleared ${deleted.changes} CWMS waterway_paths for re-fetch (geo-trim upgrade)`);
    }
  } catch (_) {}

  return db;
}

// ── Sites ────────────────────────────────────────────────────────────────────

function upsertSite({ site_id, name, type, source, latitude, longitude, description, huc8_code, huc8_name }) {
  const db = getDb();
  // NOTE: `enabled` is intentionally excluded from the ON CONFLICT update —
  // we never want an automated re-seed to re-enable a site the user disabled.
  db.prepare(`
    INSERT INTO sites (site_id, name, type, source, latitude, longitude, description, huc8_code, huc8_name, enabled)
    VALUES (@site_id, @name, @type, @source, @latitude, @longitude, @description, @huc8_code, @huc8_name, 1)
    ON CONFLICT(site_id) DO UPDATE SET
      name        = excluded.name,
      type        = excluded.type,
      source      = excluded.source,
      latitude    = excluded.latitude,
      longitude   = excluded.longitude,
      description = excluded.description,
      huc8_code   = COALESCE(excluded.huc8_code, sites.huc8_code),
      huc8_name   = COALESCE(excluded.huc8_name, sites.huc8_name)
  `).run({
    site_id, name, type: type || 'river', source: source || 'usgs',
    latitude: latitude || null, longitude: longitude || null,
    description: description || null,
    huc8_code: huc8_code || null, huc8_name: huc8_name || null,
  });
}

function getAllSites() {
  return getDb().prepare('SELECT * FROM sites ORDER BY name').all();
}

function getSiteById(siteId) {
  return getDb().prepare('SELECT * FROM sites WHERE site_id = ?').get(siteId);
}

// Returns only enabled sites, optionally filtered by source.
// Used by the scheduler — users who disable a site should not have it fetched.
function getActiveSites(source = null) {
  if (source) {
    return getDb().prepare('SELECT * FROM sites WHERE enabled = 1 AND source = ? ORDER BY name').all(source);
  }
  return getDb().prepare('SELECT * FROM sites WHERE enabled = 1 ORDER BY name').all();
}

function setSiteEnabled(siteId, enabled) {
  getDb().prepare('UPDATE sites SET enabled = ? WHERE site_id = ?').run(enabled ? 1 : 0, siteId);
}

function deleteSite(siteId) {
  getDb().prepare('DELETE FROM sites WHERE site_id = ?').run(siteId);
}

// ── Site Timeseries (CWMS CDA fetch scheduling) ───────────────────────────────

function getSiteTimeseries(siteId) {
  return getDb().prepare('SELECT ts_id FROM site_timeseries WHERE site_id = ? ORDER BY id').all(siteId).map(r => r.ts_id);
}

function replaceSiteTimeseries(siteId, tsIds) {
  const db = getDb();
  const del = db.prepare('DELETE FROM site_timeseries WHERE site_id = ?');
  const ins = db.prepare('INSERT OR IGNORE INTO site_timeseries (site_id, ts_id) VALUES (?, ?)');
  db.transaction(() => {
    del.run(siteId);
    for (const ts of (tsIds || [])) ins.run(siteId, ts);
  })();
}

// Returns all sites joined with their CWMS timeseries as a nested array.
function getAllSitesWithTimeseries() {
  const db = getDb();
  const sites  = db.prepare('SELECT * FROM sites ORDER BY source, name').all();
  const tsRows = db.prepare('SELECT site_id, ts_id FROM site_timeseries ORDER BY id').all();
  const tsMap  = {};
  for (const r of tsRows) {
    if (!tsMap[r.site_id]) tsMap[r.site_id] = [];
    tsMap[r.site_id].push(r.ts_id);
  }
  return sites.map(s => ({ ...s, timeseries: tsMap[s.site_id] || [] }));
}

// ── Seed tracking ─────────────────────────────────────────────────────────────

function getSeedApplied() {
  const row = getDb().prepare("SELECT value FROM app_meta WHERE key = 'sites_seed_applied'").get();
  return row ? row.value === '1' : false;
}

function markSeedApplied() {
  getDb().prepare("INSERT OR REPLACE INTO app_meta (key, value) VALUES ('sites_seed_applied', '1')").run();
}

// ── Measurements ─────────────────────────────────────────────────────────────

function insertMeasurements(rows) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO measurements
      (site_id, parameter_code, parameter_name, value, unit, source, recorded_at)
    VALUES
      (@site_id, @parameter_code, @parameter_name, @value, @unit, @source, @recorded_at)
  `);
  const insertMany = db.transaction((rows) => {
    let count = 0;
    for (const row of rows) {
      // Default source to 'usgs' if not specified (backwards compat)
      const info = stmt.run({ source: 'usgs', ...row });
      count += info.changes;
    }
    return count;
  });
  return insertMany(rows);
}

function getLatestReadings(siteIds = null) {
  const db = getDb();
  const siteFilter = siteIds && siteIds.length
    ? `AND m.site_id IN (${siteIds.map(() => '?').join(',')})`
    : '';
  const params = siteIds && siteIds.length ? siteIds : [];

  return db.prepare(`
    SELECT m.site_id, m.parameter_code, m.parameter_name, m.value, m.unit, m.source, m.recorded_at, s.name AS site_name
    FROM measurements m
    JOIN sites s ON s.site_id = m.site_id
    WHERE m.recorded_at = (
      SELECT MAX(m2.recorded_at) FROM measurements m2
      WHERE m2.site_id = m.site_id AND m2.parameter_code = m.parameter_code
    )
    ${siteFilter}
    ORDER BY s.name, m.parameter_code
  `).all(...params);
}

function getTimeSeriesForSite(siteId, parameterCode, hours = 168) {
  return getDb().prepare(`
    SELECT value, unit, recorded_at
    FROM measurements
    WHERE site_id = ? AND parameter_code = ?
      AND recorded_at >= datetime('now', ? || ' hours')
    ORDER BY recorded_at ASC
  `).all(siteId, parameterCode, `-${hours}`);
}

function getRecentForAI(siteIds, hours = 72) {
  const db = getDb();
  const placeholders = siteIds.map(() => '?').join(',');
  return db.prepare(`
    SELECT m.site_id, s.name AS site_name, m.parameter_code, m.parameter_name,
           m.value, m.unit, m.source, m.recorded_at
    FROM measurements m
    JOIN sites s ON s.site_id = m.site_id
    WHERE m.site_id IN (${placeholders})
      AND m.recorded_at >= datetime('now', '-${hours} hours')
    ORDER BY m.site_id, m.parameter_code, m.recorded_at DESC
  `).all(...siteIds);
}

function getCompareData(siteIds, parameterCode, hours = 336) {
  const db = getDb();
  const placeholders = siteIds.map(() => '?').join(',');
  return db.prepare(`
    SELECT m.site_id, s.name AS site_name, m.value, m.unit, m.recorded_at
    FROM measurements m
    JOIN sites s ON s.site_id = m.site_id
    WHERE m.site_id IN (${placeholders})
      AND m.parameter_code = ?
      AND m.recorded_at >= datetime('now', '-${hours} hours')
    ORDER BY m.site_id, m.recorded_at ASC
  `).all(...siteIds, parameterCode);
}

// ── Weather ───────────────────────────────────────────────────────────────────

function insertWeatherReadings(rows) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR REPLACE INTO weather_readings
      (location_id, location_name, latitude, longitude, parameter, value, unit, interval, is_forecast, recorded_at)
    VALUES
      (@location_id, @location_name, @latitude, @longitude, @parameter, @value, @unit, @interval, @is_forecast, @recorded_at)
  `);
  const insertMany = db.transaction((rows) => {
    let count = 0;
    for (const row of rows) { stmt.run(row); count++; }
    return count;
  });
  return insertMany(rows);
}

function getRecentWeather(hours = 72) {
  // Returns past actuals + next 7 days forecast
  return getDb().prepare(`
    SELECT * FROM weather_readings
    WHERE (is_forecast = 0 AND recorded_at >= datetime('now', '-${hours} hours'))
       OR (is_forecast = 1 AND recorded_at <= datetime('now', '+7 days'))
    ORDER BY location_id, parameter, recorded_at ASC
  `).all();
}

function getWeatherForecast(locationId, days = 7) {
  return getDb().prepare(`
    SELECT * FROM weather_readings
    WHERE location_id = ? AND interval = 'daily'
      AND recorded_at >= datetime('now', '-1 day')
      AND recorded_at <= datetime('now', '+? days')
    ORDER BY recorded_at ASC
  `).all(locationId, days);
}

function getWeatherForChart(locationId, parameter, hours = 336) {
  return getDb().prepare(`
    SELECT value, unit, recorded_at, is_forecast
    FROM weather_readings
    WHERE location_id = ? AND parameter = ?
      AND (
        (is_forecast = 0 AND recorded_at >= datetime('now', '-${hours} hours'))
        OR (is_forecast = 1 AND recorded_at <= datetime('now', '+7 days'))
      )
    ORDER BY recorded_at ASC
  `).all(locationId, parameter);
}

function getMeasurementRecords(siteId, parameterCode, limit = 5000, source = null) {
  if (source) {
    return getDb().prepare(`
      SELECT value, unit, recorded_at, fetched_at, source
      FROM measurements
      WHERE site_id = ? AND parameter_code = ? AND source = ?
      ORDER BY recorded_at DESC
      LIMIT ?
    `).all(siteId, parameterCode, source, limit);
  }
  return getDb().prepare(`
    SELECT value, unit, recorded_at, fetched_at, source
    FROM measurements
    WHERE site_id = ? AND parameter_code = ?
    ORDER BY recorded_at DESC
    LIMIT ?
  `).all(siteId, parameterCode, limit);
}

function getWeatherRecords(locationId, parameter, interval, limit = 5000) {
  return getDb().prepare(`
    SELECT value, unit, recorded_at, fetched_at, is_forecast
    FROM weather_readings
    WHERE location_id = ? AND parameter = ? AND interval = ?
    ORDER BY recorded_at DESC
    LIMIT ?
  `).all(locationId, parameter, interval, limit);
}

function getMeasurementSeries() {
  return getDb().prepare(`
    SELECT
      m.site_id,
      s.name                                    AS site_name,
      s.source                                  AS site_source,
      m.parameter_code,
      m.parameter_name,
      m.unit,
      COUNT(*)                                  AS row_count,
      MIN(m.recorded_at)                        AS earliest,
      MAX(m.recorded_at)                        AS latest,
      ROUND(
        (julianday(MAX(m.recorded_at)) - julianday(MIN(m.recorded_at)))
        * 1440.0 / MAX(COUNT(*) - 1, 1)
      , 1)                                      AS avg_interval_min
    FROM measurements m
    JOIN sites s ON s.site_id = m.site_id
    GROUP BY m.site_id, m.parameter_code
    ORDER BY s.source, s.name, m.parameter_code
  `).all();
}

function getWeatherSeries() {
  return getDb().prepare(`
    SELECT
      location_id,
      location_name,
      parameter,
      unit,
      interval                                  AS data_interval,
      COUNT(*)                                  AS row_count,
      SUM(CASE WHEN is_forecast = 0 THEN 1 ELSE 0 END) AS actual_count,
      SUM(CASE WHEN is_forecast = 1 THEN 1 ELSE 0 END) AS forecast_count,
      MIN(recorded_at)                          AS earliest,
      MAX(recorded_at)                          AS latest,
      MAX(fetched_at)                           AS last_fetched,
      ROUND(
        (julianday(MAX(recorded_at)) - julianday(MIN(recorded_at)))
        * 1440.0 / MAX(COUNT(*) - 1, 1)
      , 1)                                      AS avg_interval_min
    FROM weather_readings
    GROUP BY location_id, parameter, interval
    ORDER BY location_id, interval, parameter
  `).all();
}

// ── Site Connections (flow topology) ─────────────────────────────────────────

function getSiteConnections(siteIds) {
  const db = getDb();
  if (!siteIds || !siteIds.length) {
    return db.prepare('SELECT * FROM site_connections ORDER BY created_at').all();
  }
  const ph = siteIds.map(() => '?').join(',');
  return db.prepare(`
    SELECT * FROM site_connections
    WHERE from_site_id IN (${ph}) OR to_site_id IN (${ph})
    ORDER BY created_at
  `).all(...siteIds, ...siteIds);
}

function createSiteConnection({ from_site_id, to_site_id, label, notes }) {
  const info = getDb().prepare(`
    INSERT OR IGNORE INTO site_connections (from_site_id, to_site_id, label, notes)
    VALUES (@from_site_id, @to_site_id, @label, @notes)
  `).run({ from_site_id, to_site_id, label: label || 'flows into', notes: notes || null });
  return info.lastInsertRowid;
}

function deleteSiteConnection(id) {
  return getDb().prepare('DELETE FROM site_connections WHERE id = ?').run(id).changes;
}

/**
 * Replace NLDI-sourced connections with a fresh set.
 * Preserves manually-added and CWMS-sourced connections so CWMS→USGS
 * topology is not wiped on each topology discovery run.
 */
function syncNLDIConnections(connections) {
  const db = getDb();
  const del = db.prepare(`DELETE FROM site_connections WHERE source = 'nldi' OR source IS NULL`);
  const ins = db.prepare(`
    INSERT OR IGNORE INTO site_connections (from_site_id, to_site_id, label, notes, source)
    VALUES (@from_site_id, @to_site_id, 'flows into', 'Auto-discovered via USGS NLDI drainage network', 'nldi')
  `);
  // Reset any waterway_paths that previously failed/were skipped for these
  // specific pairs — they'll be retried by the waterways scheduler.
  // Paths marked 'ok' are preserved (no need to re-fetch good geometry).
  const resetPath = db.prepare(`
    DELETE FROM waterway_paths
    WHERE from_site_id = @from_site_id AND to_site_id = @to_site_id
      AND status != 'ok'
  `);
  db.transaction((conns) => {
    del.run();
    for (const c of conns) {
      ins.run(c);
      resetPath.run(c);
    }
  })(connections);
}

/**
 * Upsert connections for CWMS sites (pool → downstream gauge relationships).
 * Called on startup after CWMS sites are seeded.
 * Uses source='cwms' so syncNLDIConnections does not wipe them.
 */
function syncCWMSConnections(connections) {
  const db = getDb();
  // Remove stale CWMS connections first
  db.prepare(`DELETE FROM site_connections WHERE source = 'cwms'`).run();
  const ins = db.prepare(`
    INSERT OR IGNORE INTO site_connections (from_site_id, to_site_id, label, notes, source)
    VALUES (@from_site_id, @to_site_id, 'releases into', 'Corps dam → downstream gauge', 'cwms')
  `);
  db.transaction((conns) => {
    for (const c of conns) ins.run(c);
  })(connections);
}

function getLastNLDISync() {
  return getDb().prepare(`
    SELECT MAX(created_at) AS synced_at, COUNT(*) AS count
    FROM site_connections WHERE source = 'nldi'
  `).get();
}

// ── Waterway Paths ────────────────────────────────────────────────────────────

function getWaterwayPaths(pairs) {
  // pairs: [{from_site_id, to_site_id}, ...] — if null returns all
  const db = getDb();
  if (!pairs || !pairs.length) {
    return db.prepare('SELECT * FROM waterway_paths').all();
  }
  const stmt = db.prepare('SELECT * FROM waterway_paths WHERE from_site_id = ? AND to_site_id = ?');
  return pairs.map(p => stmt.get(p.from_site_id, p.to_site_id)).filter(Boolean);
}

function upsertWaterwayPath({ from_site_id, to_site_id, path_json, status }) {
  getDb().prepare(`
    INSERT INTO waterway_paths (from_site_id, to_site_id, path_json, status, fetched_at)
    VALUES (@from_site_id, @to_site_id, @path_json, @status, CURRENT_TIMESTAMP)
    ON CONFLICT(from_site_id, to_site_id) DO UPDATE SET
      path_json  = excluded.path_json,
      status     = excluded.status,
      fetched_at = CURRENT_TIMESTAMP
  `).run({ from_site_id, to_site_id, path_json: path_json || null, status: status || 'ok' });
}

function getPendingWaterwayPairs() {
  // Returns connections that need crawling:
  //   - never attempted (no row in waterway_paths)
  //   - status = 'pending' or 'error' (in-progress, reset, or previous failure)
  //   - status = 'none' where either site is a CWMS site (non-numeric ID) —
  //     'none' results from CWMS pairs were produced by old USGS-only nav code
  //     and should be retried now that comid-based nav is in place.
  return getDb().prepare(`
    SELECT sc.from_site_id, sc.to_site_id,
           sf.latitude AS from_lat, sf.longitude AS from_lon,
           st.latitude AS to_lat,   st.longitude AS to_lon
    FROM site_connections sc
    JOIN sites sf ON sf.site_id = sc.from_site_id
    JOIN sites st ON st.site_id = sc.to_site_id
    WHERE NOT EXISTS (
      SELECT 1 FROM waterway_paths wp
      WHERE wp.from_site_id = sc.from_site_id
        AND wp.to_site_id   = sc.to_site_id
        AND wp.status = 'ok'
    )
    AND NOT EXISTS (
      -- Exclude pure USGS pairs where 'none' means genuinely no path
      SELECT 1 FROM waterway_paths wp
      WHERE wp.from_site_id = sc.from_site_id
        AND wp.to_site_id   = sc.to_site_id
        AND wp.status = 'none'
        AND CAST(sc.from_site_id AS TEXT) NOT GLOB '*[A-Za-z]*'
        AND CAST(sc.to_site_id   AS TEXT) NOT GLOB '*[A-Za-z]*'
    )
  `).all();
}

function getAllWaterwayConnectionPairs() {
  return getDb().prepare(`
    SELECT sc.from_site_id, sc.to_site_id,
           sf.latitude AS from_lat, sf.longitude AS from_lon,
           st.latitude AS to_lat,   st.longitude AS to_lon
    FROM site_connections sc
    JOIN sites sf ON sf.site_id = sc.from_site_id
    JOIN sites st ON st.site_id = sc.to_site_id
  `).all();
}

// ── Event Annotations ─────────────────────────────────────────────────────────

function getAnnotations({ siteId, parameterCode, startAt, endAt } = {}) {
  const db = getDb();
  let sql = `SELECT * FROM event_annotations WHERE 1=1`;
  const params = [];
  if (siteId) {
    sql += ` AND (site_id = ? OR site_id IS NULL)`;
    params.push(siteId);
  }
  if (parameterCode) {
    sql += ` AND (parameter_code = ? OR parameter_code IS NULL)`;
    params.push(parameterCode);
  }
  if (startAt) { sql += ` AND annotated_at >= ?`; params.push(startAt); }
  if (endAt)   { sql += ` AND annotated_at <= ?`; params.push(endAt); }
  sql += ` ORDER BY annotated_at ASC`;
  return db.prepare(sql).all(...params);
}

function createAnnotation({ site_id, parameter_code, category, label, note, annotated_at }) {
  const info = getDb().prepare(`
    INSERT INTO event_annotations (site_id, parameter_code, category, label, note, annotated_at)
    VALUES (@site_id, @parameter_code, @category, @label, @note, @annotated_at)
  `).run({ site_id: site_id || null, parameter_code: parameter_code || null,
           category: category || 'event', label, note: note || null, annotated_at });
  return info.lastInsertRowid;
}

function updateAnnotation(id, { category, label, note, annotated_at }) {
  const fields = [];
  const params = {};
  if (category    !== undefined) { fields.push('category = @category');       params.category     = category; }
  if (label       !== undefined) { fields.push('label = @label');             params.label        = label; }
  if (note        !== undefined) { fields.push('note = @note');               params.note         = note; }
  if (annotated_at !== undefined){ fields.push('annotated_at = @annotated_at'); params.annotated_at = annotated_at; }
  if (!fields.length) return 0;
  params.id = id;
  return getDb().prepare(`UPDATE event_annotations SET ${fields.join(', ')} WHERE id = @id`).run(params).changes;
}

function deleteAnnotation(id) {
  return getDb().prepare('DELETE FROM event_annotations WHERE id = ?').run(id).changes;
}

// ── WQ Permit Limits ──────────────────────────────────────────────────────────

function getWQPermitLimits(siteId, parameterCode) {
  let sql = `SELECT * FROM wq_permit_limits WHERE 1=1`;
  const params = [];
  if (siteId)        { sql += ` AND site_id = ?`;        params.push(siteId); }
  if (parameterCode) { sql += ` AND parameter_code = ?`; params.push(parameterCode); }
  sql += ` ORDER BY parameter_code, limit_type`;
  return getDb().prepare(sql).all(...params);
}

function upsertWQPermitLimit({ id, site_id, parameter_code, limit_type, value, label, color }) {
  const db = getDb();
  if (id) {
    db.prepare(`
      UPDATE wq_permit_limits
      SET site_id=@site_id, parameter_code=@parameter_code, limit_type=@limit_type,
          value=@value, label=@label, color=@color
      WHERE id=@id
    `).run({ id, site_id, parameter_code, limit_type, value, label: label||null, color: color||'#ef4444' });
    return id;
  }
  const info = db.prepare(`
    INSERT INTO wq_permit_limits (site_id, parameter_code, limit_type, value, label, color)
    VALUES (@site_id, @parameter_code, @limit_type, @value, @label, @color)
  `).run({ site_id, parameter_code, limit_type, value, label: label||null, color: color||'#ef4444' });
  return info.lastInsertRowid;
}

function deleteWQPermitLimit(id) {
  return getDb().prepare('DELETE FROM wq_permit_limits WHERE id = ?').run(id).changes;
}

// ── User Profile ─────────────────────────────────────────────────────────────

function getProfile(userId) {
  const db = getDb();
  if (userId) {
    let row = db.prepare('SELECT * FROM user_profile WHERE user_id = ?').get(userId);
    if (!row) {
      // Create a fresh profile row for this user
      db.prepare(`INSERT INTO user_profile (user_id, role) VALUES (?, 'general')`).run(userId);
      row = db.prepare('SELECT * FROM user_profile WHERE user_id = ?').get(userId);
    }
    return row;
  }
  // Fallback for legacy callers: return row with id=1
  return db.prepare('SELECT * FROM user_profile WHERE id = 1').get();
}

function updateProfile(userId, fields) {
  const db = getDb();
  const allowed = ['name', 'organization', 'role', 'sub_role', 'interests',
                   'preferred_sites', 'bio', 'notify_thresholds'];
  const updates = Object.keys(fields)
    .filter(k => allowed.includes(k))
    .map(k => `${k} = @${k}`)
    .join(', ');
  if (!updates) return;
  if (userId) {
    // Ensure the row exists first
    getProfile(userId);
    db.prepare(`UPDATE user_profile SET ${updates}, updated_at = CURRENT_TIMESTAMP WHERE user_id = @_uid`)
      .run({ ...fields, _uid: userId });
  } else {
    db.prepare(`UPDATE user_profile SET ${updates}, updated_at = CURRENT_TIMESTAMP WHERE id = 1`)
      .run(fields);
  }
}

// ── Insights Cache ────────────────────────────────────────────────────────────

function getCachedInsight(hash, maxAgeMinutes = 30) {
  return getDb().prepare(`
    SELECT * FROM insights_cache
    WHERE query_hash = ?
      AND created_at >= datetime('now', '-${maxAgeMinutes} minutes')
    ORDER BY created_at DESC LIMIT 1
  `).get(hash);
}

function saveInsight({ query_hash, profile_role, context_json, prompt, response }) {
  getDb().prepare(`
    INSERT INTO insights_cache (query_hash, profile_role, context_json, prompt, response)
    VALUES (@query_hash, @profile_role, @context_json, @prompt, @response)
  `).run({ query_hash, profile_role, context_json, prompt, response });
}

function getRecentInsights(limit = 10) {
  return getDb().prepare(`
    SELECT id, profile_role, prompt, response, created_at
    FROM insights_cache ORDER BY created_at DESC LIMIT ?
  `).all(limit);
}

// ── Fetch Log ────────────────────────────────────────────────────────────────

function logFetch({ source, status, sites_attempted, records_stored, error_message }) {
  getDb().prepare(`
    INSERT INTO fetch_log (source, status, sites_attempted, records_stored, error_message)
    VALUES (@source, @status, @sites_attempted, @records_stored, @error_message)
  `).run({ source: source || 'usgs', status, sites_attempted, records_stored, error_message: error_message || null });
}

function getLastFetch(source = 'usgs') {
  return getDb().prepare('SELECT * FROM fetch_log WHERE source = ? ORDER BY executed_at DESC LIMIT 1').get(source);
}

function getFetchHistory(limit = 10, source = 'usgs') {
  return getDb().prepare(
    'SELECT source, status, sites_attempted, records_stored, error_message, executed_at FROM fetch_log WHERE source = ? ORDER BY executed_at DESC LIMIT ?'
  ).all(source, limit);
}

/**
 * Returns a snapshot of all background service states for the Settings status panel.
 */
function getSystemStatus() {
  const db = getDb();

  // USGS: last 5 fetches
  const usgsFetches = db.prepare(
    `SELECT source, status, sites_attempted, records_stored, error_message, executed_at
     FROM fetch_log WHERE source = 'usgs' ORDER BY executed_at DESC LIMIT 5`
  ).all();

  // CWMS: last 5 fetches
  const cwmsFetches = db.prepare(
    `SELECT source, status, sites_attempted, records_stored, error_message, executed_at
     FROM fetch_log WHERE source = 'cwms' ORDER BY executed_at DESC LIMIT 5`
  ).all();

  // CWMS measurement count
  const cwmsMeta = db.prepare(
    `SELECT COUNT(*) AS total_records, MAX(fetched_at) AS last_fetch
     FROM measurements WHERE source = 'cwms'`
  ).get();

  // Weather: latest fetch time + record counts
  const weatherMeta = db.prepare(`
    SELECT MAX(fetched_at) AS last_fetch,
           COUNT(*) AS total_records,
           SUM(CASE WHEN is_forecast = 0 THEN 1 ELSE 0 END) AS actual_count,
           SUM(CASE WHEN is_forecast = 1 THEN 1 ELSE 0 END) AS forecast_count
    FROM weather_readings
  `).get();

  // Topology: last NLDI sync + connection count
  const topology = db.prepare(`
    SELECT MAX(created_at) AS last_sync, COUNT(*) AS connection_count
    FROM site_connections WHERE source = 'nldi'
  `).get();

  // Manual connections count
  const manualConns = db.prepare(
    `SELECT COUNT(*) AS count FROM site_connections WHERE source != 'nldi' OR source IS NULL`
  ).get();

  // Waterway geometry paths
  const waterwayStats = db.prepare(`
    SELECT status, COUNT(*) AS cnt, MAX(fetched_at) AS last_fetch
    FROM waterway_paths GROUP BY status
  `).all();

  const waterways = { total: 0, ok: 0, pending: 0, none: 0, error: 0, last_fetch: null };
  for (const row of waterwayStats) {
    waterways[row.status] = (waterways[row.status] || 0) + row.cnt;
    waterways.total += row.cnt;
    if (row.status === 'ok' && row.last_fetch) waterways.last_fetch = row.last_fetch;
  }

  // Total connection pairs that need waterway paths
  const totalPairs = db.prepare('SELECT COUNT(*) AS count FROM site_connections').get();
  waterways.total_pairs = totalPairs ? totalPairs.count : 0;

  // Basin geometry sync status
  const hucStats = db.prepare(`
    SELECT
      COUNT(*)                                                    AS total,
      SUM(CASE WHEN huc8_code IS NOT NULL THEN 1 ELSE 0 END)     AS with_huc,
      SUM(CASE WHEN huc8_code IS NULL
               AND latitude IS NOT NULL THEN 1 ELSE 0 END)       AS missing
    FROM sites
  `).get();

  return { usgsFetches, cwmsFetches, cwmsMeta, weatherMeta, topology, manualConns, waterways, hucStats };
}

// ── Basins ────────────────────────────────────────────────────────────────────

function getDistinctBasins() {
  return getDb().prepare(`
    SELECT DISTINCT huc8_code, huc8_name, COUNT(*) AS site_count
    FROM sites WHERE huc8_code IS NOT NULL
    GROUP BY huc8_code ORDER BY huc8_name
  `).all();
}

function getCachedBasinPolygon(huc8Code) {
  return getDb().prepare(`SELECT * FROM basins WHERE huc8_code = ?`).get(huc8Code);
}

function upsertBasinPolygon({ huc8_code, huc8_name, polygon_json }) {
  getDb().prepare(`
    INSERT INTO basins (huc8_code, huc8_name, polygon_json, fetched_at)
    VALUES (@huc8_code, @huc8_name, @polygon_json, CURRENT_TIMESTAMP)
    ON CONFLICT(huc8_code) DO UPDATE SET
      huc8_name    = excluded.huc8_name,
      polygon_json = excluded.polygon_json,
      fetched_at   = CURRENT_TIMESTAMP
  `).run({ huc8_code, huc8_name, polygon_json });
}

function updateSiteHuc(site_id, huc8_code, huc8_name) {
  getDb().prepare(`
    UPDATE sites SET huc8_code = ?, huc8_name = ? WHERE site_id = ?
  `).run(huc8_code || null, huc8_name || null, site_id);
}

function getSitesMissingHuc() {
  return getDb().prepare(`
    SELECT site_id, name, source, latitude, longitude
    FROM sites WHERE huc8_code IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL
  `).all();
}

// ── Users ─────────────────────────────────────────────────────────────────────

function findOrCreateUser(username) {
  const db = getDb();
  const trimmed = (username || '').trim();
  if (!trimmed) throw new Error('Username cannot be empty');
  let user = db.prepare('SELECT * FROM users WHERE username = ? COLLATE NOCASE').get(trimmed);
  if (!user) {
    const info = db.prepare('INSERT INTO users (username) VALUES (?)').run(trimmed);
    user = db.prepare('SELECT * FROM users WHERE id = ?').get(info.lastInsertRowid);
  }
  db.prepare('UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = ?').run(user.id);
  return user;
}

function getUserById(id) {
  return getDb().prepare('SELECT * FROM users WHERE id = ?').get(id);
}

function getAllUsers() {
  return getDb().prepare('SELECT id, username, created_at, last_login FROM users ORDER BY username').all();
}

// ── User AI Settings ──────────────────────────────────────────────────────────

function getUserAiSettings(userId) {
  return getDb().prepare('SELECT * FROM user_ai_settings WHERE user_id = ?').get(userId);
}

function upsertUserAiSettings(userId, { provider, model }) {
  getDb().prepare(`
    INSERT INTO user_ai_settings (user_id, provider, model, updated_at)
    VALUES (@user_id, @provider, @model, CURRENT_TIMESTAMP)
    ON CONFLICT(user_id) DO UPDATE SET
      provider   = excluded.provider,
      model      = excluded.model,
      updated_at = CURRENT_TIMESTAMP
  `).run({ user_id: userId, provider: provider || null, model: model || null });
}

// ── User Favorite Basins ──────────────────────────────────────────────────────

function getUserFavoriteBasins(userId) {
  return getDb().prepare(`
    SELECT huc8_code, huc8_name, added_at FROM user_favorite_basins
    WHERE user_id = ? ORDER BY huc8_name
  `).all(userId);
}

function addUserFavoriteBasin(userId, huc8Code, huc8Name) {
  getDb().prepare(`
    INSERT OR IGNORE INTO user_favorite_basins (user_id, huc8_code, huc8_name)
    VALUES (?, ?, ?)
  `).run(userId, huc8Code, huc8Name || null);
}

function removeUserFavoriteBasin(userId, huc8Code) {
  getDb().prepare('DELETE FROM user_favorite_basins WHERE user_id = ? AND huc8_code = ?').run(userId, huc8Code);
}

function getDistinctBasinsForUser(userId) {
  const db = getDb();
  const basins = db.prepare(`
    SELECT DISTINCT huc8_code, huc8_name, COUNT(*) AS site_count
    FROM sites WHERE huc8_code IS NOT NULL
    GROUP BY huc8_code ORDER BY huc8_name
  `).all();
  if (!userId) return basins.map(b => ({ ...b, is_favorite: false }));
  const favSet = new Set(
    db.prepare('SELECT huc8_code FROM user_favorite_basins WHERE user_id = ?').all(userId).map(r => r.huc8_code)
  );
  return basins.map(b => ({ ...b, is_favorite: favSet.has(b.huc8_code) }));
}

module.exports = {
  initDatabase, getDb,
  upsertSite, getAllSites, getSiteById, getActiveSites, setSiteEnabled, deleteSite,
  getSiteTimeseries, replaceSiteTimeseries, getAllSitesWithTimeseries,
  getSeedApplied, markSeedApplied,
  insertMeasurements, getLatestReadings, getTimeSeriesForSite,
  getRecentForAI, getCompareData,
  insertWeatherReadings, getRecentWeather, getWeatherForecast, getWeatherForChart,
  getMeasurementRecords, getWeatherRecords,
  getMeasurementSeries, getWeatherSeries,
  getSiteConnections, createSiteConnection, deleteSiteConnection, syncNLDIConnections, syncCWMSConnections, getLastNLDISync,
  getWaterwayPaths, upsertWaterwayPath, getPendingWaterwayPairs, getAllWaterwayConnectionPairs,
  getAnnotations, createAnnotation, updateAnnotation, deleteAnnotation,
  getWQPermitLimits, upsertWQPermitLimit, deleteWQPermitLimit,
  getProfile, updateProfile,
  getCachedInsight, saveInsight, getRecentInsights,
  logFetch, getLastFetch, getFetchHistory, getSystemStatus,
  getDistinctBasins, getDistinctBasinsForUser, getCachedBasinPolygon, upsertBasinPolygon, updateSiteHuc, getSitesMissingHuc,
  findOrCreateUser, getUserById, getAllUsers,
  getUserAiSettings, upsertUserAiSettings,
  getUserFavoriteBasins, addUserFavoriteBasin, removeUserFavoriteBasin,
};
