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

  const schema = fs.readFileSync(SCHEMA_PATH, 'utf8');
  db.exec(schema);

  return db;
}

// ── Sites ────────────────────────────────────────────────────────────────────

function upsertSite({ site_id, name, type, latitude, longitude, description }) {
  const db = getDb();
  db.prepare(`
    INSERT INTO sites (site_id, name, type, latitude, longitude, description)
    VALUES (@site_id, @name, @type, @latitude, @longitude, @description)
    ON CONFLICT(site_id) DO UPDATE SET
      name = excluded.name,
      type = excluded.type,
      latitude = excluded.latitude,
      longitude = excluded.longitude,
      description = excluded.description
  `).run({ site_id, name, type, latitude, longitude, description: description || null });
}

function getAllSites() {
  return getDb().prepare('SELECT * FROM sites ORDER BY name').all();
}

function getSiteById(siteId) {
  return getDb().prepare('SELECT * FROM sites WHERE site_id = ?').get(siteId);
}

// ── Measurements ─────────────────────────────────────────────────────────────

function insertMeasurements(rows) {
  const db = getDb();
  const stmt = db.prepare(`
    INSERT OR IGNORE INTO measurements
      (site_id, parameter_code, parameter_name, value, unit, recorded_at)
    VALUES
      (@site_id, @parameter_code, @parameter_name, @value, @unit, @recorded_at)
  `);
  const insertMany = db.transaction((rows) => {
    let count = 0;
    for (const row of rows) {
      const info = stmt.run(row);
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
    SELECT m.site_id, m.parameter_code, m.parameter_name, m.value, m.unit, m.recorded_at, s.name AS site_name
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
           m.value, m.unit, m.recorded_at
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

// ── User Profile ─────────────────────────────────────────────────────────────

function getProfile() {
  return getDb().prepare('SELECT * FROM user_profile WHERE id = 1').get();
}

function updateProfile(fields) {
  const db = getDb();
  const allowed = ['name', 'organization', 'role', 'sub_role', 'interests',
                   'preferred_sites', 'bio', 'notify_thresholds'];
  const updates = Object.keys(fields)
    .filter(k => allowed.includes(k))
    .map(k => `${k} = @${k}`)
    .join(', ');
  if (!updates) return;
  db.prepare(`UPDATE user_profile SET ${updates}, updated_at = CURRENT_TIMESTAMP WHERE id = 1`)
    .run(fields);
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

function logFetch({ status, sites_attempted, records_stored, error_message }) {
  getDb().prepare(`
    INSERT INTO fetch_log (status, sites_attempted, records_stored, error_message)
    VALUES (@status, @sites_attempted, @records_stored, @error_message)
  `).run({ status, sites_attempted, records_stored, error_message: error_message || null });
}

function getLastFetch() {
  return getDb().prepare('SELECT * FROM fetch_log ORDER BY executed_at DESC LIMIT 1').get();
}

module.exports = {
  initDatabase, getDb,
  upsertSite, getAllSites, getSiteById,
  insertMeasurements, getLatestReadings, getTimeSeriesForSite,
  getRecentForAI, getCompareData,
  getProfile, updateProfile,
  getCachedInsight, saveInsight, getRecentInsights,
  logFetch, getLastFetch
};
