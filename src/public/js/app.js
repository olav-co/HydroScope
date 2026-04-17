// HydroScope — Global client utilities

// ── Theme ────────────────────────────────────────────────────────────────────

(function () {
  var saved = localStorage.getItem('hs-theme') || 'dark';
  if (saved === 'light') document.body.classList.add('light');
})();

function toggleTheme() {
  var isLight = document.body.classList.toggle('light');
  localStorage.setItem('hs-theme', isLight ? 'light' : 'dark');
  var btn = document.getElementById('themeToggle');
  if (btn) btn.title = isLight ? 'Switch to dark mode' : 'Switch to light mode';
  // Redraw any open charts so grid lines update
  if (typeof Chart !== 'undefined') {
    Chart.instances.forEach(function(c) { c.update(); });
  }
}

// ── AI Panel System ──────────────────────────────────────────────────────────

var _aiConfigs = {};

/**
 * Register and optionally auto-load an AI panel.
 * @param {string} panelId   - matches the id on the panel container
 * @param {string} query     - the question/prompt to send
 * @param {Array}  siteIds   - optional array of site ids to scope
 * @param {boolean} autoLoad - run immediately
 */
function initAiPanel(panelId, query, siteIds, autoLoad) {
  _aiConfigs[panelId] = { query: query, siteIds: siteIds || null };
  if (autoLoad) loadAiPanel(panelId, false);
}

/** Update the query for a panel (useful when user changes site selection). */
function updateAiPanel(panelId, query, siteIds) {
  _aiConfigs[panelId] = { query: query, siteIds: siteIds || null };
}

async function loadAiPanel(panelId, bypassCache) {
  var cfg = _aiConfigs[panelId];
  if (!cfg) return;

  var panel = document.getElementById(panelId);
  if (!panel) return;

  panel.querySelector('.ai-loading').classList.remove('hidden');
  panel.querySelector('.ai-body').classList.add('hidden');
  panel.querySelector('.ai-empty').classList.add('hidden');
  panel.querySelector('.ai-cached').classList.add('hidden');

  try {
    var resp = await fetch('/api/insights/ask', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: cfg.query,
        site_ids: cfg.siteIds,
        bypass_cache: bypassCache || false
      })
    });
    var json = await resp.json();

    panel.querySelector('.ai-loading').classList.add('hidden');
    var body = panel.querySelector('.ai-body');
    body.textContent = json.response || json.error || 'No response.';
    body.classList.remove('hidden');
    if (json.cached) panel.querySelector('.ai-cached').classList.remove('hidden');
  } catch (err) {
    panel.querySelector('.ai-loading').classList.add('hidden');
    var body = panel.querySelector('.ai-body');
    body.textContent = 'AI unavailable: ' + err.message;
    body.classList.remove('hidden');
  }
}

function refreshAiPanel(panelId) {
  loadAiPanel(panelId, true);
}
