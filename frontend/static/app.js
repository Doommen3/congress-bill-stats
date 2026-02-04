// ==========================================
// State Management
// ==========================================
let currentLegislature = 'congress'; // 'congress' | 'illinois'
let currentData = [];
let currentSummary = null;
let sortKey = 'sponsored_total';
let sortDir = 'desc';

// ==========================================
// DOM Elements - Congress
// ==========================================
const statusEl = document.getElementById('status');
const summaryEl = document.getElementById('summary');
const dataNoteEl = document.getElementById('dataNote');

const congressControls = document.getElementById('congressControls');
const tableBody = document.getElementById('tbody');
const dataTable = document.getElementById('dataTable');
const chamberFilter = document.getElementById('chamberFilter');
const searchBox = document.getElementById('searchBox');
const congressInput = document.getElementById('congressInput');
const loadBtn = document.getElementById('loadBtn');
const exportBtn = document.getElementById('exportBtn');
const headers = document.querySelectorAll('#dataTable .th-sort');

// ==========================================
// DOM Elements - Illinois
// ==========================================
const illinoisControls = document.getElementById('illinoisControls');
const ilTableBody = document.getElementById('ilTbody');
const ilDataTable = document.getElementById('ilDataTable');
const ilChamberFilter = document.getElementById('ilChamberFilter');
const ilSearchBox = document.getElementById('ilSearchBox');
const ilSessionSelect = document.getElementById('ilSessionSelect');
const ilLoadBtn = document.getElementById('ilLoadBtn');
const ilExportBtn = document.getElementById('ilExportBtn');
const ilHeaders = document.querySelectorAll('#ilDataTable .th-sort');

// ==========================================
// DOM Elements - Toggle
// ==========================================
const congressBtn = document.getElementById('congressBtn');
const illinoisBtn = document.getElementById('illinoisBtn');

// ==========================================
// Utility Functions
// ==========================================
function fmt(n) { return new Intl.NumberFormat().format(n); }
function setStatus(msg) { statusEl.textContent = msg || ''; }

// ==========================================
// Legislature Toggle
// ==========================================
function switchLegislature(legislature) {
  currentLegislature = legislature;
  currentData = [];
  currentSummary = null;

  // Toggle button states
  congressBtn.classList.toggle('active', legislature === 'congress');
  illinoisBtn.classList.toggle('active', legislature === 'illinois');

  // Toggle controls visibility
  congressControls.style.display = legislature === 'congress' ? 'grid' : 'none';
  illinoisControls.style.display = legislature === 'illinois' ? 'grid' : 'none';

  // Toggle table visibility
  dataTable.style.display = legislature === 'congress' ? 'table' : 'none';
  ilDataTable.style.display = legislature === 'illinois' ? 'table' : 'none';

  // Update data note
  if (legislature === 'congress') {
    dataNoteEl.textContent = 'Law counts use the Congress.gov /law endpoint for accurate enacted legislation data. First load may take a few minutes; then it\'s cached.';
  } else {
    dataNoteEl.textContent = 'Data from Illinois General Assembly FTP XML files. First load may take a few minutes as bill data is fetched; then it\'s cached.';
  }

  // Reset sort
  sortKey = legislature === 'illinois' ? 'primary_sponsor_total' : 'sponsored_total';
  sortDir = 'desc';

  // Clear current display
  if (legislature === 'congress') {
    tableBody.innerHTML = '';
  } else {
    ilTableBody.innerHTML = '';
  }
  summaryEl.innerHTML = '';
  setStatus('');

  // Load data for new legislature
  loadData();
}

// ==========================================
// Render Functions
// ==========================================
function renderCongress() {
  let rows = currentData.slice();

  const chamber = chamberFilter.value;
  if (chamber !== 'both') {
    rows = rows.filter(r => (r.chamber || '').toLowerCase() === chamber);
  }

  const q = searchBox.value.trim().toLowerCase();
  if (q) {
    rows = rows.filter(r => {
      const name = (r.sponsorName || '').toLowerCase();
      const state = (r.state || '').toLowerCase();
      return name.includes(q) || state.includes(q);
    });
  }

  rows.sort((a, b) => {
    const va = sortKey === 'primary_sponsor_total'
      ? (a.primary_sponsor_total ?? a.sponsored_total ?? 0)
      : (a[sortKey] ?? '');
    const vb = sortKey === 'primary_sponsor_total'
      ? (b.primary_sponsor_total ?? b.sponsored_total ?? 0)
      : (b[sortKey] ?? '');
    if (typeof va === 'number' && typeof vb === 'number') {
      return sortDir === 'asc' ? va - vb : vb - va;
    }
    const sa = String(va).toLowerCase();
    const sb = String(vb).toLowerCase();
    if (sa < sb) return sortDir === 'asc' ? -1 : 1;
    if (sa > sb) return sortDir === 'asc' ? 1 : -1;
    return 0;
  });

  tableBody.innerHTML = rows.map(r => `
    <tr>
      <td>${r.sponsorName || '—'}</td>
      <td>${(r.chamber || '—').replace(/^\w/, c => c.toUpperCase())}</td>
      <td>${r.party || '—'}</td>
      <td>${r.state || '—'}</td>
      <td class="right">${fmt(r.sponsored_total || 0)}</td>
      <td class="right">${fmt(r.public_law_count || 0)}</td>
      <td class="right">${fmt(r.private_law_count || 0)}</td>
      <td class="right">${fmt(r.enacted_total || 0)}</td>
    </tr>
  `).join('');

  // Update summary
  if (currentSummary) {
    summaryEl.innerHTML = `
      <strong>Summary:</strong> ${fmt(currentSummary.total_bills || 0)} bills |
      ${fmt(currentSummary.total_laws || 0)} enacted laws
      (${fmt(currentSummary.public_laws || 0)} public, ${fmt(currentSummary.private_laws || 0)} private) |
      ${fmt(currentSummary.total_legislators || 0)} legislators
    `;
  } else {
    summaryEl.innerHTML = '';
  }
}

function renderIllinois() {
  let rows = currentData.slice();

  const chamber = ilChamberFilter.value;
  if (chamber !== 'both') {
    rows = rows.filter(r => (r.chamber || '').toLowerCase() === chamber);
  }

  const q = ilSearchBox.value.trim().toLowerCase();
  if (q) {
    rows = rows.filter(r => {
      const name = (r.sponsorName || '').toLowerCase();
      const district = String(r.district || '');
      return name.includes(q) || district.includes(q);
    });
  }

  rows.sort((a, b) => {
    const va = a[sortKey] ?? '';
    const vb = b[sortKey] ?? '';
    if (typeof va === 'number' && typeof vb === 'number') {
      return sortDir === 'asc' ? va - vb : vb - va;
    }
    const sa = String(va).toLowerCase();
    const sb = String(vb).toLowerCase();
    if (sa < sb) return sortDir === 'asc' ? -1 : 1;
    if (sa > sb) return sortDir === 'asc' ? 1 : -1;
    return 0;
  });

  ilTableBody.innerHTML = rows.map(r => `
    <tr>
      <td>${r.sponsorName || '—'}</td>
      <td>${(r.chamber || '—').replace(/^\w/, c => c.toUpperCase())}</td>
      <td>${r.party || '—'}</td>
      <td class="right">${r.district || '—'}</td>
      <td class="right">${fmt(r.primary_sponsor_total ?? r.sponsored_total ?? 0)}</td>
      <td class="right">${fmt(r.chief_co_sponsor_total || 0)}</td>
      <td class="right">${fmt(r.co_sponsor_total || 0)}</td>
      <td class="right">${fmt(r.enacted_total || 0)}</td>
    </tr>
  `).join('');

  // Update summary
  if (currentSummary) {
    summaryEl.innerHTML = `
      <strong>Summary:</strong> ${fmt(currentSummary.total_bills || 0)} bills |
      ${fmt(currentSummary.total_laws || 0)} enacted (became Public Acts) |
      ${fmt(currentSummary.total_legislators || 0)} legislators with sponsor activity
    `;
  } else {
    summaryEl.innerHTML = '';
  }
}

function render() {
  if (currentLegislature === 'congress') {
    renderCongress();
  } else {
    renderIllinois();
  }
}

// ==========================================
// Data Loading
// ==========================================
async function loadCongressData(forceRefresh = false) {
  const congress = parseInt(congressInput.value || '119', 10);
  setStatus('Loading (first run may take a few minutes)...');
  loadBtn.disabled = true;
  loadBtn.textContent = 'Loading...';

  try {
    const refreshParam = forceRefresh ? '&refresh=true' : '';
    const r = await fetch(`/api/stats?congress=${encodeURIComponent(congress)}${refreshParam}`);
    if (!r.ok) {
      const t = await r.text();
      setStatus('Error: ' + t.slice(0, 300));
      return;
    }
    const json = await r.json();
    currentData = json.rows || [];
    currentSummary = json.summary || null;

    const genTime = json.generated_at ? new Date(json.generated_at * 1000).toLocaleString() : 'unknown';
    setStatus(`Loaded ${currentData.length} legislators for the ${json.congress}th Congress. Data as of: ${genTime}`);
    render();
  } catch (e) {
    setStatus('Network error: ' + (e && e.message ? e.message : e));
  } finally {
    loadBtn.disabled = false;
    loadBtn.textContent = 'Load / Refresh';
  }
}

async function loadIllinoisData(forceRefresh = false) {
  const session = parseInt(ilSessionSelect.value || '104', 10);
  setStatus('Loading Illinois data (first run may take several minutes)...');
  ilLoadBtn.disabled = true;
  ilLoadBtn.textContent = 'Loading...';

  try {
    const refreshParam = forceRefresh ? '&refresh=true' : '';
    const r = await fetch(`/api/il-stats?session=${encodeURIComponent(session)}${refreshParam}`);
    if (!r.ok) {
      const t = await r.text();
      setStatus('Error: ' + t.slice(0, 300));
      return;
    }
    const json = await r.json();
    currentData = json.rows || [];
    currentSummary = json.summary || null;

    const genTime = json.generated_at ? new Date(json.generated_at * 1000).toLocaleString() : 'unknown';
    const years = json.years || session;
    setStatus(`Loaded ${currentData.length} legislators for the ${json.ga_session}th Illinois GA (${years}). Data as of: ${genTime}`);
    render();
  } catch (e) {
    setStatus('Network error: ' + (e && e.message ? e.message : e));
  } finally {
    ilLoadBtn.disabled = false;
    ilLoadBtn.textContent = 'Load / Refresh';
  }
}

async function loadData(forceRefresh = false) {
  if (currentLegislature === 'congress') {
    await loadCongressData(forceRefresh);
  } else {
    await loadIllinoisData(forceRefresh);
  }
}

// ==========================================
// CSV Export
// ==========================================
function exportCongressCSV() {
  if (!currentData.length) {
    alert('No data to export. Please load data first.');
    return;
  }

  const csvHeaders = ['Legislator', 'Chamber', 'Party', 'State', 'Sponsored', 'Public Laws', 'Private Laws', 'Total Laws'];
  const rows = currentData.map(r => [
    r.sponsorName || '',
    r.chamber || '',
    r.party || '',
    r.state || '',
    r.sponsored_total || 0,
    r.public_law_count || 0,
    r.private_law_count || 0,
    r.enacted_total || 0,
  ]);

  downloadCSV(csvHeaders, rows, `congress_${congressInput.value}_stats.csv`);
}

function exportIllinoisCSV() {
  if (!currentData.length) {
    alert('No data to export. Please load data first.');
    return;
  }

  const csvHeaders = ['Legislator', 'Chamber', 'Party', 'District', 'Primary', 'Chief Co', 'Co', 'Enacted'];
  const rows = currentData.map(r => [
    r.sponsorName || '',
    r.chamber || '',
    r.party || '',
    r.district || '',
    r.primary_sponsor_total ?? r.sponsored_total ?? 0,
    r.chief_co_sponsor_total || 0,
    r.co_sponsor_total || 0,
    r.enacted_total || 0,
  ]);

  downloadCSV(csvHeaders, rows, `illinois_ga_${ilSessionSelect.value}_stats.csv`);
}

function downloadCSV(headers, rows, filename) {
  const csvContent = [
    headers.join(','),
    ...rows.map(row => row.map(cell => {
      const str = String(cell);
      if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return '"' + str.replace(/"/g, '""') + '"';
      }
      return str;
    }).join(','))
  ].join('\n');

  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function exportCSV() {
  if (currentLegislature === 'congress') {
    exportCongressCSV();
  } else {
    exportIllinoisCSV();
  }
}

// ==========================================
// Event Handlers - Sorting
// ==========================================
function setupSortHandlers(headerElements, tableType) {
  headerElements.forEach(th => {
    th.addEventListener('click', () => {
      const key = th.getAttribute('data-key');
      if (sortKey === key) {
        sortDir = sortDir === 'asc' ? 'desc' : 'asc';
      } else {
        sortKey = key;
        // Default to descending for numeric columns
        const numericCols = [
          'sponsored_total',
          'primary_sponsor_total',
          'chief_co_sponsor_total',
          'co_sponsor_total',
          'enacted_total',
          'public_law_count',
          'private_law_count',
          'district'
        ];
        sortDir = numericCols.includes(key) ? 'desc' : 'asc';
      }
      render();
    });
  });
}

setupSortHandlers(headers, 'congress');
setupSortHandlers(ilHeaders, 'illinois');

// ==========================================
// Event Handlers - Controls
// ==========================================
// Toggle buttons
congressBtn.addEventListener('click', () => switchLegislature('congress'));
illinoisBtn.addEventListener('click', () => switchLegislature('illinois'));

// Congress controls
loadBtn.addEventListener('click', () => loadData());
exportBtn.addEventListener('click', exportCSV);
searchBox.addEventListener('input', () => render());
chamberFilter.addEventListener('change', () => render());

// Illinois controls
ilLoadBtn.addEventListener('click', () => loadData());
ilExportBtn.addEventListener('click', exportCSV);
ilSearchBox.addEventListener('input', () => render());
ilChamberFilter.addEventListener('change', () => render());

// ==========================================
// Initialize
// ==========================================
document.addEventListener('DOMContentLoaded', () => loadData());
