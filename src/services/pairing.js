'use strict';

const db = require('../db/database');
const { getSiteComid }  = require('./waterways');
const { buildAliasMap } = require('./cwmsAliases');

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

// ── Concurrency guard ─────────────────────────────────────────────────────────
let _running = false;
let _queued  = false;

async function detectAndPairSites() {
  if (_running) { _queued = true; return; }
  _running = true;
  try {
    await _doPairing();
  } finally {
    _running = false;
    if (_queued) {
      _queued = false;
      detectAndPairSites().catch(() => {});
    }
  }
}

async function _doPairing() {
  const allSites  = db.getAllSites();
  const cwmsSites = allSites.filter(s => s.source === 'cwms' && s.latitude && s.longitude);
  const usgsSites = allSites.filter(s => s.source === 'usgs' && s.latitude && s.longitude);

  if (!cwmsSites.length || !usgsSites.length) return;

  // Save enabled states before wiping combined sites.
  const savedStates = {};
  for (const p of allSites.filter(s => s.source === 'combined')) {
    for (const c of db.getSiteChildren(p.site_id)) {
      savedStates[`${p.site_id}|${c.child_site_id}`] = c.enabled;
    }
  }

  db.clearCombinedSites();

  // Priority 1: CDA Agency Aliases — human-configured, authoritative.
  const aliasMap  = buildAliasMap(); // Map<cwmsId, usgsId>
  const usgsById  = new Map(usgsSites.map(s => [s.site_id, s]));
  const usedUsgs  = new Set();

  // Priority 2: NHD COMID — fetch for all sites (DB-cached; NLDI on miss).
  const comidMap = {};
  await Promise.all([...cwmsSites, ...usgsSites].map(async s => {
    try {
      const comid = await getSiteComid(s.site_id);
      if (comid) comidMap[s.site_id] = comid;
    } catch (_) {}
  }));

  let pairCount = 0;

  for (const cwms of cwmsSites) {
    let best = null, matchReason = '';

    // ── Priority 1: CDA alias (exact human-configured mapping) ───────────────
    const aliasUsgsId = aliasMap.get(cwms.site_id)
      // Sub-location fallback: "Cheatham-TW" → try parent "Cheatham"
      ?? aliasMap.get(cwms.site_id.replace(/-[^-]+$/, ''));

    if (aliasUsgsId && usgsById.has(aliasUsgsId) && !usedUsgs.has(aliasUsgsId)) {
      best = usgsById.get(aliasUsgsId);
      matchReason = `CDA alias → ${aliasUsgsId}`;
    }

    // ── Priority 2: NHD COMID match ──────────────────────────────────────────
    if (!best) {
      const cwmsComid = comidMap[cwms.site_id];
      if (cwmsComid) {
        for (const usgs of usgsSites) {
          if (usedUsgs.has(usgs.site_id)) continue;
          if (comidMap[usgs.site_id] === cwmsComid) {
            best = usgs;
            matchReason = `COMID ${cwmsComid}`;
            break;
          }
        }
      }
    }

    // ── Priority 3: proximity ≤2 km ──────────────────────────────────────────
    // Corps dam and tailwater gauge are on adjacent NHD reaches (different COMIDs)
    // but always within a few hundred metres of each other.
    if (!best) {
      const cwmsComid = comidMap[cwms.site_id];
      let bestDist = 2.0;
      for (const usgs of usgsSites) {
        if (usedUsgs.has(usgs.site_id)) continue;
        const dist = haversineKm(cwms.latitude, cwms.longitude, usgs.latitude, usgs.longitude);
        if (dist < bestDist) {
          bestDist = dist;
          best = usgs;
          matchReason = `proximity ${dist.toFixed(2)} km (cwms_comid=${cwmsComid || 'none'} usgs_comid=${comidMap[usgs.site_id] || 'none'})`;
        }
      }
    }

    if (best) {
      usedUsgs.add(best.site_id);

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

      const ck = `${parentId}|${cwms.site_id}`;
      const uk = `${parentId}|${best.site_id}`;
      if (ck in savedStates) db.setSiteSourceEnabled(parentId, cwms.site_id, savedStates[ck]);
      if (uk in savedStates) db.setSiteSourceEnabled(parentId, best.site_id, savedStates[uk]);

      pairCount++;
      console.log(`[Pairing] ${cwms.name} ↔ ${best.name} (${matchReason}) → ${parentId}`);
    } else {
      const nearest = usgsSites.map(u => ({
        name: u.name,
        dist: haversineKm(cwms.latitude, cwms.longitude, u.latitude, u.longitude),
      })).sort((a, b) => a.dist - b.dist)[0];
      const cwmsComid = comidMap[cwms.site_id];
      console.log(`[Pairing] no match for CWMS "${cwms.name}" (comid=${cwmsComid || 'none'}, alias=${aliasMap.get(cwms.site_id) || 'none'})${nearest ? ` nearest="${nearest.name}" ${nearest.dist.toFixed(1)}km` : ''}`);
    }
  }

  if (pairCount) console.log(`[Pairing] ${pairCount} combined site(s) created.`);
  else           console.log('[Pairing] No USGS/CWMS pairs detected.');
}

module.exports = { detectAndPairSites };
