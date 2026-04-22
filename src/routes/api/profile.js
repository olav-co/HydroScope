const express = require('express');
const router = express.Router();
const db = require('../../db/database');

const ROLES = {
  general:      { label: 'General',         sub_roles: [] },
  data_scientist: {
    label: 'Data Scientist',
    sub_roles: [
      { id: 'hydrologist',   label: 'Hydrologist' },
      { id: 'climatologist', label: 'Climatologist' },
      { id: 'ml_researcher', label: 'ML / Predictive Modeling' },
    ],
  },
  vacationer: {
    label: 'Vacationer / Recreationist',
    sub_roles: [
      { id: 'kayaker',  label: 'Kayaker / Paddler' },
      { id: 'swimmer',  label: 'Swimmer' },
      { id: 'fisher',   label: 'Angler / Fisher' },
      { id: 'hiker',    label: 'Hiker / Trail User' },
    ],
  },
  eco_historian: {
    label: 'Eco Historian',
    sub_roles: [
      { id: 'researcher',  label: 'Academic Researcher' },
      { id: 'naturalist',  label: 'Naturalist' },
      { id: 'journalist',  label: 'Environmental Journalist' },
    ],
  },
  analyst: {
    label: 'Analyst',
    sub_roles: [
      { id: 'policy_analyst',  label: 'Policy Analyst' },
      { id: 'business_analyst',label: 'Business / Risk Analyst' },
      { id: 'water_planner',   label: 'Water Resources Planner' },
    ],
  },
  operator: {
    label: 'Operator',
    sub_roles: [
      { id: 'dam_operator',    label: 'Dam / Reservoir Operator' },
      { id: 'utility_operator',label: 'Water Utility Operator' },
      { id: 'floodcontrol',    label: 'Flood Control Engineer' },
    ],
  },
  regulator: {
    label: 'Regulator',
    sub_roles: [
      { id: 'epa',          label: 'EPA / DEQ Staff' },
      { id: 'federal_agency',label: 'Federal Agency (USACE, BOR)' },
      { id: 'state_agency', label: 'State / Local Agency' },
    ],
  },
};

const INTEREST_OPTIONS = [
  { id: 'water_quality',    label: 'Water Quality' },
  { id: 'flood_risk',       label: 'Flood Risk & Management' },
  { id: 'ecological_health',label: 'Ecological Health' },
  { id: 'recreational_use', label: 'Recreational Use' },
  { id: 'historical_trends',label: 'Historical Trends' },
  { id: 'climate_impacts',  label: 'Climate & Weather Impacts' },
  { id: 'compliance',       label: 'Regulatory Compliance' },
  { id: 'infrastructure',   label: 'Infrastructure & Operations' },
  { id: 'drinking_water',   label: 'Drinking Water Supply' },
  { id: 'fish_habitat',     label: 'Fish & Habitat' },
];

// GET /api/profile
router.get('/', (req, res) => {
  const profile = db.getProfile(req.user ? req.user.id : null);
  try { profile.interests = JSON.parse(profile.interests || '[]'); } catch { profile.interests = []; }
  try { profile.preferred_sites = JSON.parse(profile.preferred_sites || '[]'); } catch { profile.preferred_sites = []; }
  // sub_role: stored as JSON array; fall back gracefully for old single-string values
  try {
    const parsed = JSON.parse(profile.sub_role || '[]');
    profile.sub_role = Array.isArray(parsed) ? parsed : (parsed ? [parsed] : []);
  } catch { profile.sub_role = profile.sub_role ? [profile.sub_role] : []; }
  res.json({ ok: true, profile, roles: ROLES, interests: INTEREST_OPTIONS });
});

// GET /api/profile/roles
router.get('/roles', (req, res) => {
  res.json({ ok: true, roles: ROLES, interests: INTEREST_OPTIONS });
});

// PUT /api/profile
router.put('/', (req, res) => {
  const { name, organization, role, sub_role, interests, preferred_sites, bio, notify_thresholds } = req.body;

  const fields = {};
  if (name         !== undefined) fields.name         = name;
  if (organization !== undefined) fields.organization = organization;
  if (role         !== undefined) fields.role         = role;
  if (sub_role     !== undefined) fields.sub_role     = JSON.stringify(Array.isArray(sub_role) ? sub_role.slice(0, 3) : (sub_role ? [sub_role] : []));
  if (interests    !== undefined) fields.interests    = JSON.stringify(Array.isArray(interests) ? interests : []);
  if (preferred_sites !== undefined) fields.preferred_sites = JSON.stringify(Array.isArray(preferred_sites) ? preferred_sites : []);
  if (bio          !== undefined) fields.bio          = bio;
  if (notify_thresholds !== undefined) fields.notify_thresholds = JSON.stringify(notify_thresholds);

  db.updateProfile(req.user ? req.user.id : null, fields);
  res.json({ ok: true, message: 'Profile updated.' });
});

module.exports = router;
