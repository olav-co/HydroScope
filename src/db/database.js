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
  try { db.exec(`ALTER TABLE sites ADD COLUMN parent_site_id TEXT`); } catch (_) {}
  try { db.exec(`ALTER TABLE sites ADD COLUMN comid TEXT`); } catch (_) {}
  try { db.exec(`ALTER TABLE sites ADD COLUMN office TEXT`); } catch (_) {}
  // cwms_usgs_aliases and cwms_alias_offices created via schema.sql (IF NOT EXISTS)
  try { db.exec(`ALTER TABLE basins ADD COLUMN centroid_lat REAL`); } catch (_) {}
  try { db.exec(`ALTER TABLE basins ADD COLUMN centroid_lon REAL`); } catch (_) {}
  try { db.exec(`ALTER TABLE basins ADD COLUMN bbox_minlon REAL`); } catch (_) {}
  try { db.exec(`ALTER TABLE basins ADD COLUMN bbox_minlat REAL`); } catch (_) {}
  try { db.exec(`ALTER TABLE basins ADD COLUMN bbox_maxlon REAL`); } catch (_) {}
  try { db.exec(`ALTER TABLE basins ADD COLUMN bbox_maxlat REAL`); } catch (_) {}
  try { db.exec(`ALTER TABLE site_groups ADD COLUMN is_published INTEGER DEFAULT 0`); }
  catch (e) { if (!e.message.includes('duplicate column')) console.warn('[DB migration] site_groups.is_published:', e.message); }
  try { db.exec(`ALTER TABLE site_groups ADD COLUMN published_at DATETIME`); }
  catch (e) { if (!e.message.includes('duplicate column')) console.warn('[DB migration] site_groups.published_at:', e.message); }
  try { db.exec(`ALTER TABLE site_groups ADD COLUMN source_group_id INTEGER`); }
  catch (e) { if (!e.message.includes('duplicate column')) console.warn('[DB migration] site_groups.source_group_id:', e.message); }
  try { db.exec(`ALTER TABLE site_groups ADD COLUMN synced_at DATETIME`); }
  catch (e) { if (!e.message.includes('duplicate column')) console.warn('[DB migration] site_groups.synced_at:', e.message); }
  try { db.exec(`ALTER TABLE site_groups ADD COLUMN updated_at DATETIME`); }
  catch (e) { if (!e.message.includes('duplicate column')) console.warn('[DB migration] site_groups.updated_at:', e.message); }

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

function upsertSite({ site_id, name, type, source, latitude, longitude, description, huc8_code, huc8_name, office }) {
  const db = getDb();
  // NOTE: `enabled` is intentionally excluded from the ON CONFLICT update —
  // we never want an automated re-seed to re-enable a site the user disabled.
  db.prepare(`
    INSERT INTO sites (site_id, name, type, source, latitude, longitude, description, huc8_code, huc8_name, office, enabled)
    VALUES (@site_id, @name, @type, @source, @latitude, @longitude, @description, @huc8_code, @huc8_name, @office, 1)
    ON CONFLICT(site_id) DO UPDATE SET
      name        = excluded.name,
      type        = excluded.type,
      source      = excluded.source,
      latitude    = excluded.latitude,
      longitude   = excluded.longitude,
      description = excluded.description,
      huc8_code   = COALESCE(excluded.huc8_code, sites.huc8_code),
      huc8_name   = COALESCE(excluded.huc8_name, sites.huc8_name),
      office      = COALESCE(excluded.office, sites.office)
  `).run({
    site_id, name, type: type || 'river', source: source || 'usgs',
    latitude: latitude || null, longitude: longitude || null,
    description: description || null,
    huc8_code: huc8_code || null, huc8_name: huc8_name || null,
    office: office || null,
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

function setSiteComid(siteId, comid) {
  getDb().prepare('UPDATE sites SET comid = ? WHERE site_id = ?').run(comid || null, siteId);
}

function updateSiteCoords(siteId, lat, lon) {
  getDb().prepare('UPDATE sites SET latitude = ?, longitude = ? WHERE site_id = ?').run(lat, lon, siteId);
}

// ── CWMS ↔ USGS alias table ───────────────────────────────────────────────────

function upsertCwmsUsgsAliases(rows) {
  // rows: [{ cwmsId, usgsId, office }]
  const stmt = getDb().prepare(`
    INSERT OR REPLACE INTO cwms_usgs_aliases (cwms_id, usgs_id, office, fetched_at)
    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
  `);
  const tx = getDb().transaction(rows => { for (const r of rows) stmt.run(r.cwmsId, r.usgsId, r.office || null); });
  tx(rows);
}

function getCwmsUsgsAlias(cwmsId) {
  return getDb().prepare('SELECT usgs_id FROM cwms_usgs_aliases WHERE cwms_id = ?').get(cwmsId)?.usgs_id || null;
}

function getAllCwmsUsgsAliases() {
  return getDb().prepare('SELECT cwms_id, usgs_id, office FROM cwms_usgs_aliases').all();
}

function getAliasFetchedAt(office) {
  return getDb().prepare('SELECT fetched_at FROM cwms_alias_offices WHERE office = ?').get(office)?.fetched_at || null;
}

function markAliasFetched(office, count) {
  getDb().prepare(`
    INSERT OR REPLACE INTO cwms_alias_offices (office, fetched_at, alias_count)
    VALUES (?, CURRENT_TIMESTAMP, ?)
  `).run(office, count);
}

function getDistinctCwmsOffices() {
  return getDb().prepare(`SELECT DISTINCT office FROM cwms_usgs_aliases`).all().map(r => r.office).filter(Boolean);
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

function getDistinctWeatherLocations() {
  return getDb().prepare(
    `SELECT DISTINCT location_id, location_name, latitude, longitude
     FROM weather_readings ORDER BY location_name`
  ).all();
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

// ── Official / Seasonal Thresholds ───────────────────────────────────────────

function getOfficialThresholds(siteId, paramCode) {
  return getDb().prepare(`
    SELECT * FROM site_thresholds
    WHERE site_id = ? AND param_code = ?
    ORDER BY category DESC, value DESC
  `).all(siteId, paramCode);
}

function upsertOfficialThreshold(siteId, paramCode, t) {
  getDb().prepare(`
    INSERT INTO site_thresholds
      (site_id, param_code, threshold_id, label, value, unit, source, source_label, type, color, category, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(site_id, param_code, threshold_id) DO UPDATE SET
      label=excluded.label, value=excluded.value, unit=excluded.unit,
      source=excluded.source, source_label=excluded.source_label,
      type=excluded.type, color=excluded.color, category=excluded.category,
      fetched_at=CURRENT_TIMESTAMP
  `).run(
    siteId, paramCode, t.threshold_id, t.label, t.value,
    t.unit || null, t.source, t.source_label || null,
    t.type, t.color, t.category || 'official'
  );
}

function clearOfficialThresholds(siteId, paramCode) {
  getDb().prepare('DELETE FROM site_thresholds WHERE site_id = ? AND param_code = ?').run(siteId, paramCode);
}

// ── Site param codes (what parameters actually have recorded data) ─────────────

function getSiteParamCodes(siteId) {
  return getDb().prepare(
    `SELECT DISTINCT parameter_code, parameter_name FROM measurements WHERE site_id = ? ORDER BY parameter_code`
  ).all(siteId);
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

function getAllCachedBasins() {
  return getDb().prepare(`SELECT huc8_code, huc8_name FROM basins ORDER BY huc8_name`).all();
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

// Returns basins whose bbox contains the point — fast pre-filter before ray-casting.
// Expects polygon_json to be present for point-in-polygon verification.
function getBasinCandidatesForPoint(lon, lat) {
  return getDb().prepare(`
    SELECT huc8_code, huc8_name, polygon_json
    FROM basins
    WHERE bbox_minlon IS NOT NULL
      AND bbox_minlon <= @lon AND bbox_maxlon >= @lon
      AND bbox_minlat <= @lat AND bbox_maxlat >= @lat
      AND polygon_json IS NOT NULL
    ORDER BY huc8_code
  `).all({ lon, lat });
}

function getSitesMissingHuc() {
  return getDb().prepare(`
    SELECT site_id, name, source, latitude, longitude
    FROM sites WHERE huc8_code IS NULL AND latitude IS NOT NULL AND longitude IS NOT NULL
  `).all();
}

function getBasinsInBbox(minLon, minLat, maxLon, maxLat) {
  // Standard AABB intersection: basin rect overlaps viewport rect when neither is
  // entirely left/right/above/below the other.  Falls back to centroid point-in-box
  // for rows that were synced before bbox columns were populated.
  return getDb().prepare(`
    SELECT huc8_code, huc8_name
    FROM basins
    WHERE (
      -- bbox intersection (preferred — catches edge-overlapping basins)
      (bbox_minlon IS NOT NULL
        AND bbox_maxlon >= @minLon AND bbox_minlon <= @maxLon
        AND bbox_maxlat >= @minLat AND bbox_minlat <= @maxLat)
      OR
      -- centroid fallback for rows without bbox data yet
      (bbox_minlon IS NULL
        AND centroid_lon BETWEEN @minLon AND @maxLon
        AND centroid_lat BETWEEN @minLat AND @maxLat)
    )
    ORDER BY huc8_name
  `).all({ minLon, minLat, maxLon, maxLat });
}

// Suggest basins related to active ones via HUC hierarchy.
// Same HUC4 (first 4 digits) = same watershed subregion — most relevant.
// Falls back to same HUC2 (first 2 digits) if not enough results.
function getBasinRecommendations(activeCodes, excludeCodes, limit = 8) {
  if (!activeCodes.length) return [];
  const db      = getDb();
  const exclude = new Set(excludeCodes);
  const seen    = new Set();
  const results = [];

  const huc4s = [...new Set(activeCodes.map(c => c.slice(0, 4)))];
  const huc2s = [...new Set(activeCodes.map(c => c.slice(0, 2)))];

  for (const prefix of huc4s) {
    const rows = db.prepare(
      `SELECT huc8_code, huc8_name FROM basins WHERE huc8_code LIKE ? ORDER BY huc8_name LIMIT 20`
    ).all(prefix + '%');
    for (const r of rows) {
      if (!seen.has(r.huc8_code) && !exclude.has(r.huc8_code)) {
        seen.add(r.huc8_code); results.push(r);
      }
    }
  }

  if (results.length < limit) {
    for (const prefix of huc2s) {
      const rows = db.prepare(
        `SELECT huc8_code, huc8_name FROM basins WHERE huc8_code LIKE ? ORDER BY huc8_name LIMIT 20`
      ).all(prefix + '%');
      for (const r of rows) {
        if (!seen.has(r.huc8_code) && !exclude.has(r.huc8_code)) {
          seen.add(r.huc8_code); results.push(r);
          if (results.length >= limit) break;
        }
      }
      if (results.length >= limit) break;
    }
  }

  return results.slice(0, limit);
}

// Resolve a list of huc8 codes to full basin records (for AI suggestion matching).
function getBasinsByCodes(codes) {
  if (!codes.length) return [];
  const placeholders = codes.map(() => '?').join(',');
  return getDb().prepare(
    `SELECT huc8_code, huc8_name FROM basins WHERE huc8_code IN (${placeholders}) ORDER BY huc8_name`
  ).all(...codes);
}

function searchBasinsLocal(q) {
  if (!q) return [];
  const db = getDb();
  if (/^\d+$/.test(q)) {
    return db.prepare(
      `SELECT huc8_code, huc8_name FROM basins WHERE huc8_code LIKE ? ORDER BY huc8_name LIMIT 150`
    ).all(q + '%');
  }
  return db.prepare(
    `SELECT huc8_code, huc8_name FROM basins WHERE UPPER(huc8_name) LIKE UPPER(?) ORDER BY huc8_name LIMIT 150`
  ).all('%' + q + '%');
}

function getNotableBasinsLocal(names) {
  const db   = getDb();
  const seen = new Set();
  const out  = [];
  for (const name of names) {
    const rows = db.prepare(
      `SELECT huc8_code, huc8_name FROM basins WHERE UPPER(huc8_name) LIKE UPPER(?) ORDER BY huc8_name LIMIT 2`
    ).all(name + '%');
    for (const row of rows) {
      if (!seen.has(row.huc8_code)) { seen.add(row.huc8_code); out.push(row); }
    }
  }
  return out;
}

function getBasinCount() {
  const row = getDb().prepare(
    `SELECT COUNT(*) as total,
            SUM(CASE WHEN bbox_minlon IS NOT NULL THEN 1 ELSE 0 END) as with_bbox
     FROM basins`
  ).get();
  return { total: row.total, with_bbox: row.with_bbox };
}

function updateBasinBbox(huc8_code, { bbox_minlon, bbox_minlat, bbox_maxlon, bbox_maxlat }) {
  getDb().prepare(`
    UPDATE basins SET bbox_minlon=@bbox_minlon, bbox_minlat=@bbox_minlat,
                      bbox_maxlon=@bbox_maxlon, bbox_maxlat=@bbox_maxlat
    WHERE huc8_code=@huc8_code
  `).run({ huc8_code, bbox_minlon, bbox_minlat, bbox_maxlon, bbox_maxlat });
}

function bulkUpsertBasins(rows) {
  const stmt = getDb().prepare(`
    INSERT INTO basins (huc8_code, huc8_name, polygon_json, fetched_at,
                        centroid_lat, centroid_lon,
                        bbox_minlon, bbox_minlat, bbox_maxlon, bbox_maxlat)
    VALUES (@huc8_code, @huc8_name, @polygon_json, CURRENT_TIMESTAMP,
            @centroid_lat, @centroid_lon,
            @bbox_minlon, @bbox_minlat, @bbox_maxlon, @bbox_maxlat)
    ON CONFLICT(huc8_code) DO UPDATE SET
      huc8_name    = excluded.huc8_name,
      polygon_json = excluded.polygon_json,
      fetched_at   = CURRENT_TIMESTAMP,
      centroid_lat = excluded.centroid_lat,
      centroid_lon = excluded.centroid_lon,
      bbox_minlon  = excluded.bbox_minlon,
      bbox_minlat  = excluded.bbox_minlat,
      bbox_maxlon  = excluded.bbox_maxlon,
      bbox_maxlat  = excluded.bbox_maxlat
  `);
  getDb().transaction(arr => { for (const r of arr) stmt.run(r); })(rows);
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

// ── Combined / paired sites ───────────────────────────────────────────────────

function createCombinedSite({ site_id, name, latitude, longitude }) {
  getDb().prepare(`
    INSERT OR REPLACE INTO sites (site_id, name, type, source, latitude, longitude, enabled)
    VALUES (?, ?, 'combined', 'combined', ?, ?, 1)
  `).run(site_id, name, latitude, longitude);
}

function clearCombinedSites() {
  const d = getDb();
  d.prepare(`UPDATE sites SET parent_site_id = NULL WHERE parent_site_id IS NOT NULL`).run();
  d.prepare(`DELETE FROM sites WHERE source = 'combined'`).run();
}

function addSiteSource(parentSiteId, childSiteId) {
  getDb().prepare(`
    INSERT OR IGNORE INTO site_sources (parent_site_id, child_site_id) VALUES (?, ?)
  `).run(parentSiteId, childSiteId);
}

function setParentSite(childSiteId, parentSiteId) {
  getDb().prepare(`UPDATE sites SET parent_site_id = ? WHERE site_id = ?`).run(parentSiteId, childSiteId);
}

function getSiteChildren(parentSiteId) {
  return getDb().prepare(`
    SELECT ss.child_site_id, ss.enabled, s.source, s.name, s.latitude, s.longitude
    FROM site_sources ss
    JOIN sites s ON s.site_id = ss.child_site_id
    WHERE ss.parent_site_id = ?
    ORDER BY s.source
  `).all(parentSiteId);
}

function setSiteSourceEnabled(parentSiteId, childSiteId, enabled) {
  getDb().prepare(`
    UPDATE site_sources SET enabled = ? WHERE parent_site_id = ? AND child_site_id = ?
  `).run(enabled ? 1 : 0, parentSiteId, childSiteId);
}

// ── Site Groups ───────────────────────────────────────────────────────────────

function getGroupsForUser(userId) {
  try {
    // Full query — requires sharing columns (added via ALTER TABLE migration or new schema).
    return getDb().prepare(`
      SELECT g.id, g.name, g.color, g.created_at, g.updated_at,
             g.is_published, g.published_at,
             g.source_group_id, g.synced_at,
             COUNT(m.site_id) AS member_count,
             src.name         AS source_name,
             src.is_published AS source_is_published,
             src.updated_at   AS source_updated_at,
             su.username      AS source_author
      FROM site_groups g
      LEFT JOIN site_group_members m   ON m.group_id = g.id
      LEFT JOIN site_groups src        ON src.id = g.source_group_id
      LEFT JOIN users su               ON su.id   = src.user_id
      WHERE g.user_id = ?
      GROUP BY g.id
      ORDER BY g.name
    `).all(userId);
  } catch (_) {
    // Fallback: sharing columns not yet migrated on this DB.
    // Return nulls for sharing fields so the rest of the page still works.
    return getDb().prepare(`
      SELECT g.id, g.name, g.color, g.created_at,
             COUNT(m.site_id) AS member_count,
             NULL AS updated_at, 0 AS is_published, NULL AS published_at,
             NULL AS source_group_id, NULL AS synced_at,
             NULL AS source_name, 0 AS source_is_published,
             NULL AS source_updated_at, NULL AS source_author
      FROM site_groups g
      LEFT JOIN site_group_members m ON m.group_id = g.id
      WHERE g.user_id = ?
      GROUP BY g.id
      ORDER BY g.name
    `).all(userId);
  }
}

function getGroupById(groupId, userId) {
  return getDb().prepare(`SELECT * FROM site_groups WHERE id = ? AND user_id = ?`).get(groupId, userId);
}

function getGroupMembers(groupId) {
  return getDb().prepare(`SELECT site_id FROM site_group_members WHERE group_id = ?`).all(groupId).map(r => r.site_id);
}

function createGroup(userId, name, color) {
  const info = getDb().prepare(`
    INSERT INTO site_groups (user_id, name, color) VALUES (?, ?, ?)
  `).run(userId, name, color || '#3b82f6');
  return info.lastInsertRowid;
}

function updateGroup(groupId, userId, { name, color }) {
  const fields = ['updated_at = CURRENT_TIMESTAMP'];
  const params = {};
  if (name  !== undefined) { fields.push('name = @name');   params.name  = name; }
  if (color !== undefined) { fields.push('color = @color'); params.color = color; }
  params.id = groupId; params.userId = userId;
  return getDb().prepare(
    `UPDATE site_groups SET ${fields.join(', ')} WHERE id = @id AND user_id = @userId`
  ).run(params).changes;
}

function deleteGroup(groupId, userId) {
  return getDb().prepare('DELETE FROM site_groups WHERE id = ? AND user_id = ?').run(groupId, userId).changes;
}

function setGroupMembers(groupId, userId, siteIds) {
  const d = getDb();
  if (!d.prepare('SELECT id FROM site_groups WHERE id = ? AND user_id = ?').get(groupId, userId)) return false;
  d.transaction(() => {
    d.prepare('DELETE FROM site_group_members WHERE group_id = ?').run(groupId);
    const ins = d.prepare('INSERT OR IGNORE INTO site_group_members (group_id, site_id) VALUES (?, ?)');
    for (const siteId of (siteIds || [])) ins.run(groupId, siteId);
    d.prepare('UPDATE site_groups SET updated_at = CURRENT_TIMESTAMP WHERE id = ?').run(groupId);
  })();
  return true;
}

function publishGroup(groupId, userId, isPublished) {
  const sql = isPublished
    ? `UPDATE site_groups SET is_published=1, published_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=? AND user_id=?`
    : `UPDATE site_groups SET is_published=0, updated_at=CURRENT_TIMESTAMP WHERE id=? AND user_id=?`;
  return getDb().prepare(sql).run(groupId, userId).changes;
}

// Returns all published groups from other users, with sample site names and
// whether this user already has a saved copy.
function getPublishedGroups(currentUserId) {
  return getDb().prepare(`
    SELECT g.id, g.name, g.color, g.published_at, g.updated_at,
           u.username AS author,
           COUNT(DISTINCT m.site_id) AS member_count,
           GROUP_CONCAT(s.name, '||') AS site_names_raw,
           (SELECT id FROM site_groups saved
            WHERE saved.user_id = @uid AND saved.source_group_id = g.id
            LIMIT 1) AS saved_local_id
    FROM site_groups g
    JOIN  users u ON u.id = g.user_id
    LEFT JOIN site_group_members m ON m.group_id = g.id
    LEFT JOIN sites s ON s.site_id = m.site_id
    WHERE g.is_published = 1 AND g.user_id != @uid
    GROUP BY g.id
    ORDER BY g.published_at DESC
  `).all({ uid: currentUserId });
}

// Save a copy of a published group into the current user's groups.
// Returns the local group id (existing copy if already saved, new if first save).
function saveGroupCopy(userId, sourceGroupId) {
  const d = getDb();
  const source = d.prepare(`SELECT * FROM site_groups WHERE id = ? AND is_published = 1`).get(sourceGroupId);
  if (!source) return null;
  // Idempotent — return existing copy if already saved
  const existing = d.prepare(`SELECT id FROM site_groups WHERE user_id = ? AND source_group_id = ?`).get(userId, sourceGroupId);
  if (existing) return existing.id;
  // Create local copy
  const r = d.prepare(`
    INSERT INTO site_groups (user_id, name, color, source_group_id, synced_at, updated_at)
    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
  `).run(userId, source.name, source.color, sourceGroupId);
  const newId = r.lastInsertRowid;
  // Copy members
  const members = d.prepare(`SELECT site_id FROM site_group_members WHERE group_id = ?`).all(sourceGroupId);
  if (members.length) {
    const ins = d.prepare(`INSERT OR IGNORE INTO site_group_members (group_id, site_id) VALUES (?, ?)`);
    d.transaction(() => { for (const m of members) ins.run(newId, m.site_id); })();
  }
  return newId;
}

// Sync a saved copy to match the current published source (name, color, members).
// Returns false if the source is no longer available/published.
function syncGroupFromSource(groupId, userId) {
  const d = getDb();
  const local = d.prepare(`SELECT * FROM site_groups WHERE id = ? AND user_id = ?`).get(groupId, userId);
  if (!local || !local.source_group_id) return false;
  const source = d.prepare(`SELECT * FROM site_groups WHERE id = ? AND is_published = 1`).get(local.source_group_id);
  if (!source) return false;
  d.transaction(() => {
    d.prepare(`UPDATE site_groups SET name=?, color=?, synced_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?`)
     .run(source.name, source.color, groupId);
    d.prepare(`DELETE FROM site_group_members WHERE group_id = ?`).run(groupId);
    const ins = d.prepare(`INSERT OR IGNORE INTO site_group_members (group_id, site_id) VALUES (?, ?)`);
    const members = d.prepare(`SELECT site_id FROM site_group_members WHERE group_id = ?`).all(source.id);
    for (const m of members) ins.run(groupId, m.site_id);
  })();
  return true;
}

function getTimeSeriesForSites(siteIds, parameterCode, hours = 168) {
  if (!siteIds.length) return [];
  const placeholders = siteIds.map(() => '?').join(',');
  return getDb().prepare(`
    SELECT value, unit, recorded_at, site_id
    FROM measurements
    WHERE site_id IN (${placeholders})
      AND parameter_code = ?
      AND recorded_at >= datetime('now', ? || ' hours')
    ORDER BY recorded_at ASC
  `).all(...siteIds, parameterCode, `-${hours}`);
}

module.exports = {
  initDatabase, getDb,
  upsertSite, getAllSites, getSiteById, getActiveSites, setSiteEnabled, deleteSite, setSiteComid, updateSiteCoords,
  getSiteTimeseries, replaceSiteTimeseries, getAllSitesWithTimeseries,
  getSeedApplied, markSeedApplied,
  insertMeasurements, getLatestReadings, getTimeSeriesForSite, getSiteParamCodes,
  getRecentForAI, getCompareData,
  insertWeatherReadings, getDistinctWeatherLocations, getRecentWeather, getWeatherForecast, getWeatherForChart,
  getMeasurementRecords, getWeatherRecords,
  getMeasurementSeries, getWeatherSeries,
  getSiteConnections, createSiteConnection, deleteSiteConnection, syncNLDIConnections, syncCWMSConnections, getLastNLDISync,
  getWaterwayPaths, upsertWaterwayPath, getPendingWaterwayPairs, getAllWaterwayConnectionPairs,
  getAnnotations, createAnnotation, updateAnnotation, deleteAnnotation,
  getOfficialThresholds, upsertOfficialThreshold, clearOfficialThresholds,
  getWQPermitLimits, upsertWQPermitLimit, deleteWQPermitLimit,
  getProfile, updateProfile,
  getCachedInsight, saveInsight, getRecentInsights,
  logFetch, getLastFetch, getFetchHistory, getSystemStatus,
  getDistinctBasins, getDistinctBasinsForUser, getAllCachedBasins, getCachedBasinPolygon, upsertBasinPolygon, updateSiteHuc, getSitesMissingHuc,
  getBasinsInBbox, getBasinCandidatesForPoint, searchBasinsLocal, getNotableBasinsLocal, getBasinCount, bulkUpsertBasins, updateBasinBbox,
  getBasinRecommendations, getBasinsByCodes,
  findOrCreateUser, getUserById, getAllUsers,
  getUserAiSettings, upsertUserAiSettings,
  getUserFavoriteBasins, addUserFavoriteBasin, removeUserFavoriteBasin,
  createCombinedSite, clearCombinedSites, addSiteSource, setParentSite,
  getSiteChildren, setSiteSourceEnabled, getTimeSeriesForSites,
  upsertCwmsUsgsAliases, getCwmsUsgsAlias, getAllCwmsUsgsAliases,
  getAliasFetchedAt, markAliasFetched, getDistinctCwmsOffices,
  getGroupsForUser, getGroupById, getGroupMembers, createGroup, updateGroup, deleteGroup, setGroupMembers,
  publishGroup, getPublishedGroups, saveGroupCopy, syncGroupFromSource,
};
