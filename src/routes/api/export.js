const express = require('express');
const router  = express.Router();
const XLSX    = require('xlsx');
const db      = require('../../db/database');

// ── helpers ───────────────────────────────────────────────────────────────────

function sendCsv(res, rows, filename) {
  if (!rows.length) {
    return res.status(404).json({ error: 'No data found for the requested parameters.' });
  }
  const headers = Object.keys(rows[0]);
  const lines   = [
    headers.join(','),
    ...rows.map(r => headers.map(h => {
      const v = r[h] == null ? '' : String(r[h]);
      return v.includes(',') || v.includes('"') || v.includes('\n')
        ? `"${v.replace(/"/g, '""')}"` : v;
    }).join(','))
  ];
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(lines.join('\r\n'));
}

function sendXlsx(res, rows, sheetName, filename) {
  if (!rows.length) {
    return res.status(404).json({ error: 'No data found for the requested parameters.' });
  }
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, sheetName);
  const buf = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  res.send(buf);
}

function safeFilename(parts) {
  return parts.map(p => String(p).replace(/[^a-z0-9_\-]/gi, '_')).join('_');
}

// ── GET /api/export/measurements ─────────────────────────────────────────────
// ?site_id=&param_code=&hours=&fmt=csv|xlsx
router.get('/measurements', (req, res) => {
  try {
    const { site_id, param_code, hours = 168, fmt = 'csv' } = req.query;
    if (!site_id || !param_code) {
      return res.status(400).json({ error: 'site_id and param_code are required.' });
    }
    const rows = db.getMeasurementRecords(site_id, param_code, 50000);
    const filename = safeFilename(['hydroscope', site_id, param_code, `${hours}h`]);
    if (fmt === 'xlsx') return sendXlsx(res, rows, 'Measurements', `${filename}.xlsx`);
    sendCsv(res, rows, `${filename}.csv`);
  } catch (err) {
    console.error('[export/measurements]', err.message);
    res.status(500).json({ error: 'Export failed.' });
  }
});

// ── GET /api/export/weather ───────────────────────────────────────────────────
// ?location_id=&parameter=&interval=hourly|daily&fmt=csv|xlsx
router.get('/weather', (req, res) => {
  try {
    const { location_id, parameter, interval = 'hourly', fmt = 'csv' } = req.query;
    if (!location_id || !parameter) {
      return res.status(400).json({ error: 'location_id and parameter are required.' });
    }
    const rows = db.getWeatherRecords(location_id, parameter, interval, 50000);
    const filename = safeFilename(['hydroscope_wx', location_id, parameter, interval]);
    if (fmt === 'xlsx') return sendXlsx(res, rows, 'Weather', `${filename}.xlsx`);
    sendCsv(res, rows, `${filename}.csv`);
  } catch (err) {
    console.error('[export/weather]', err.message);
    res.status(500).json({ error: 'Export failed.' });
  }
});

// ── GET /api/export/compare ───────────────────────────────────────────────────
// ?sites=id1,id2&param_code=&hours=&fmt=csv|xlsx
router.get('/compare', (req, res) => {
  try {
    const { sites, param_code, hours = 168, fmt = 'csv' } = req.query;
    if (!sites || !param_code) {
      return res.status(400).json({ error: 'sites and param_code are required.' });
    }
    const siteIds = sites.split(',').map(s => s.trim()).filter(Boolean);
    if (siteIds.length < 2) return res.status(400).json({ error: 'At least 2 sites required.' });
    const rows = db.getCompareData(siteIds, param_code, Number(hours));
    const filename = safeFilename(['hydroscope_compare', param_code, `${hours}h`]);
    if (fmt === 'xlsx') return sendXlsx(res, rows, 'Compare', `${filename}.xlsx`);
    sendCsv(res, rows, `${filename}.csv`);
  } catch (err) {
    console.error('[export/compare]', err.message);
    res.status(500).json({ error: 'Export failed.' });
  }
});

// ── GET /api/export/annotations ───────────────────────────────────────────────
// ?site_id=&param_code=&fmt=csv|xlsx
router.get('/annotations', (req, res) => {
  try {
    const { site_id, param_code, fmt = 'csv' } = req.query;
    const rows = db.getAnnotations({ siteId: site_id, parameterCode: param_code });
    const filename = safeFilename(['hydroscope_annotations', site_id || 'all']);
    if (fmt === 'xlsx') return sendXlsx(res, rows, 'Annotations', `${filename}.xlsx`);
    sendCsv(res, rows, `${filename}.csv`);
  } catch (err) {
    console.error('[export/annotations]', err.message);
    res.status(500).json({ error: 'Export failed.' });
  }
});

module.exports = router;
