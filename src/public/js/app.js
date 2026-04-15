// HydroScope — Global client-side utilities

// Register Chart.js date adapter (chartjs-adapter-date-fns via CDN is needed for time scale)
// Loaded via script tag in head.ejs for compare page

document.addEventListener('DOMContentLoaded', () => {
  // Apply saved theme preference (dark by default)
  document.documentElement.classList.add('dark');
});
