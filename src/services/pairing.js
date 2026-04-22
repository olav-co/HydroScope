'use strict';

const db = require('../db/database');

// Words that don't distinguish dam/gauge names — ignored during name matching
const NOISE = new Set([
  'dam', 'reservoir', 'lake', 'river', 'station', 'gauge', 'gage',
  'the', 'and', 'at', 'on', 'or', 'near', 'below', 'above', 'bl', 'ab', 'nr',
]);

function keyWords(name) {
  return name.toLowerCase()
    .replace(/[^a-z0-9 ]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length > 2 && !NOISE.has(w));
}

function nameSimilarity(cwmsName, usgsName) {
  const ck = keyWords(cwmsName);
  const uk = new Set(keyWords(usgsName));
  if (!ck.length) return 0;
  return ck.filter(w => uk.has(w)).length / ck.length;
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180)
    * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function makeCombinedName(cwmsName) {
  return cwmsName
    .replace(/\s+(dam|reservoir|lake)\s*$/i, '')
    .trim() + ' [CWMS+USGS]';
}

function detectAndPairSites() {
  const allSites  = db.getAllSites();
  const cwmsSites = allSites.filter(s => s.source === 'cwms' && s.latitude && s.longitude);
  const usgsSites = allSites.filter(s => s.source === 'usgs' && s.latitude && s.longitude);

  if (!cwmsSites.length || !usgsSites.length) return;

  // Save existing source-enabled states before clearing so user toggles survive restarts.
  // Combined site IDs are deterministic (CMBO_ + cwmsId) so they'll match after re-creation.
  const savedStates = {};
  for (const p of allSites.filter(s => s.source === 'combined')) {
    for (const c of db.getSiteChildren(p.site_id)) {
      savedStates[`${p.site_id}|${c.child_site_id}`] = c.enabled;
    }
  }

  db.clearCombinedSites();

  let pairCount = 0;

  for (const cwms of cwmsSites) {
    let best = null, bestScore = 0;

    for (const usgs of usgsSites) {
      const sim  = nameSimilarity(cwms.name, usgs.name);
      const dist = haversineKm(cwms.latitude, cwms.longitude, usgs.latitude, usgs.longitude);
      // Proximity bonus (max 0.3) for sites within 25 km — covers dam + tailwater gauge distance
      const prox  = dist <= 25 ? (1 - dist / 25) * 0.3 : 0;
      const score = sim + prox;

      // Require ≥ 40 % name overlap to avoid false positives across different drainages
      if (sim >= 0.40 && score > bestScore) {
        bestScore = score;
        best = usgs;
      }
    }

    if (best) {
      const parentId = 'CMBO_' + cwms.site_id;
      db.createCombinedSite({
        site_id:   parentId,
        name:      makeCombinedName(cwms.name),
        latitude:  cwms.latitude,
        longitude: cwms.longitude,
      });
      db.addSiteSource(parentId, cwms.site_id);
      db.addSiteSource(parentId, best.site_id);
      db.setParentSite(cwms.site_id, parentId);
      db.setParentSite(best.site_id, parentId);

      // Restore previously saved enabled states
      const ck = `${parentId}|${cwms.site_id}`;
      const uk = `${parentId}|${best.site_id}`;
      if (ck in savedStates) db.setSiteSourceEnabled(parentId, cwms.site_id, savedStates[ck]);
      if (uk in savedStates) db.setSiteSourceEnabled(parentId, best.site_id, savedStates[uk]);

      pairCount++;
      const distKm = haversineKm(cwms.latitude, cwms.longitude, best.latitude, best.longitude).toFixed(1);
      console.log(`[Pairing] ${cwms.name} ↔ ${best.name} (score: ${bestScore.toFixed(2)}, ${distKm} km) → ${parentId}`);
    }
  }

  if (pairCount) console.log(`[Pairing] ${pairCount} combined site(s) created.`);
  else           console.log('[Pairing] No USGS/CWMS pairs detected.');
}

module.exports = { detectAndPairSites };
