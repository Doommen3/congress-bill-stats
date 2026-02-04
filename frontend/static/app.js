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
// DOM Elements - Leaderboards & Stats
// ==========================================
const leaderboardList = document.getElementById('leaderboardList');
const leaderboardTabs = document.querySelectorAll('.lb-tab');
let currentLeaderboard = 'sponsored';

// ==========================================
// DOM Elements - Illinois Graph
// ==========================================
const networkSectionEl = document.getElementById('networkSection');
const networkContainerEl = document.getElementById('networkContainer');
const networkHintEl = document.getElementById('networkHint');
const networkRefreshBtn = document.getElementById('networkRefresh');
const networkShowLabelsEl = document.getElementById('networkShowLabels');
const networkMinConnectionsEl = document.getElementById('networkMinConnections');
const networkGraphModeEl = document.getElementById('networkGraphMode');
const networkZoomInBtn = document.getElementById('networkZoomIn');
const networkZoomOutBtn = document.getElementById('networkZoomOut');
const networkFitViewBtn = document.getElementById('networkFitView');

const NETWORK_VIEW = 'network';
const EDGE_BUNDLING_VIEW = 'edge_bundling';
const NETWORK_VIEW_STORAGE_KEY = 'il_graph_mode';
let currentNetworkView = NETWORK_VIEW;

try {
  const savedView = localStorage.getItem(NETWORK_VIEW_STORAGE_KEY);
  if (savedView === EDGE_BUNDLING_VIEW) {
    currentNetworkView = EDGE_BUNDLING_VIEW;
  }
} catch (err) {
  // Ignore localStorage access failures (private mode / strict browser settings).
}

if (networkGraphModeEl) {
  networkGraphModeEl.value = currentNetworkView;
}

// ==========================================
// Utility Functions
// ==========================================
function fmt(n) { return new Intl.NumberFormat().format(n); }
function setStatus(msg) { statusEl.textContent = msg || ''; }
function calcSuccessRate(enacted, sponsored) {
  if (!sponsored || sponsored === 0) return 0;
  return (enacted / sponsored) * 100;
}
function fmtPct(n) { return n.toFixed(1) + '%'; }

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
  sortKey = legislature === 'congress' ? 'primary_sponsor_total' : 'primary_sponsor_total';
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
    let va, vb;
    if (sortKey === 'primary_sponsor_total') {
      va = a.primary_sponsor_total ?? a.sponsored_total ?? 0;
      vb = b.primary_sponsor_total ?? b.sponsored_total ?? 0;
    } else if (sortKey === 'success_rate') {
      const aSpon = a.primary_sponsor_total ?? a.sponsored_total ?? 0;
      const bSpon = b.primary_sponsor_total ?? b.sponsored_total ?? 0;
      va = calcSuccessRate(a.enacted_total || 0, aSpon);
      vb = calcSuccessRate(b.enacted_total || 0, bSpon);
    } else {
      va = a[sortKey] ?? '';
      vb = b[sortKey] ?? '';
    }
    if (typeof va === 'number' && typeof vb === 'number') {
      return sortDir === 'asc' ? va - vb : vb - va;
    }
    const sa = String(va).toLowerCase();
    const sb = String(vb).toLowerCase();
    if (sa < sb) return sortDir === 'asc' ? -1 : 1;
    if (sa > sb) return sortDir === 'asc' ? 1 : -1;
    return 0;
  });

  tableBody.innerHTML = rows.map(r => {
    const sponsored = r.primary_sponsor_total ?? r.sponsored_total ?? 0;
    const enacted = r.enacted_total || 0;
    const successRate = calcSuccessRate(enacted, sponsored);
    return `
    <tr>
      <td>${r.sponsorName || '—'}</td>
      <td>${(r.chamber || '—').replace(/^\w/, c => c.toUpperCase())}</td>
      <td>${r.party || '—'}</td>
      <td>${r.state || '—'}</td>
      <td class="right">${fmt(sponsored)}</td>
      <td class="right">${fmt(r.cosponsor_total || 0)}</td>
      <td class="right">${fmt(r.original_cosponsor_total || 0)}</td>
      <td class="right">${fmt(r.public_law_count || 0)}</td>
      <td class="right">${fmt(r.private_law_count || 0)}</td>
      <td class="right">${fmt(enacted)}</td>
      <td class="right">${fmtPct(successRate)}</td>
    </tr>
  `;}).join('');

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
    let va, vb;
    if (sortKey === 'success_rate') {
      const aSpon = a.primary_sponsor_total ?? a.sponsored_total ?? 0;
      const bSpon = b.primary_sponsor_total ?? b.sponsored_total ?? 0;
      va = calcSuccessRate(a.enacted_total || 0, aSpon);
      vb = calcSuccessRate(b.enacted_total || 0, bSpon);
    } else if (sortKey === 'avg_days_to_enactment') {
      // Null values go to end
      va = a.avg_days_to_enactment ?? 9999;
      vb = b.avg_days_to_enactment ?? 9999;
    } else if (sortKey === 'bipartisan_score') {
      va = a.bipartisan_score ?? -1;
      vb = b.bipartisan_score ?? -1;
    } else {
      va = a[sortKey] ?? '';
      vb = b[sortKey] ?? '';
    }
    if (typeof va === 'number' && typeof vb === 'number') {
      return sortDir === 'asc' ? va - vb : vb - va;
    }
    const sa = String(va).toLowerCase();
    const sb = String(vb).toLowerCase();
    if (sa < sb) return sortDir === 'asc' ? -1 : 1;
    if (sa > sb) return sortDir === 'asc' ? 1 : -1;
    return 0;
  });

  ilTableBody.innerHTML = rows.map(r => {
    const sponsored = r.primary_sponsor_total ?? r.sponsored_total ?? 0;
    const enacted = r.enacted_total || 0;
    const successRate = calcSuccessRate(enacted, sponsored);
    const avgDays = r.avg_days_to_enactment;
    const bipartisan = r.bipartisan_score;
    return `
    <tr>
      <td>${r.sponsorName || '—'}</td>
      <td>${(r.chamber || '—').replace(/^\w/, c => c.toUpperCase())}</td>
      <td>${r.party || '—'}</td>
      <td class="right">${r.district || '—'}</td>
      <td class="right">${fmt(sponsored)}</td>
      <td class="right">${fmt(r.chief_co_sponsor_total || 0)}</td>
      <td class="right">${fmt(r.co_sponsor_total || 0)}</td>
      <td class="right">${fmt(enacted)}</td>
      <td class="right">${fmtPct(successRate)}</td>
      <td class="right">${avgDays != null ? avgDays : '—'}</td>
      <td class="right">${bipartisan != null ? fmtPct(bipartisan) : '—'}</td>
    </tr>
  `;}).join('');

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
    // Hide Illinois-specific sections
    document.getElementById('timelineSection').style.display = 'none';
    networkSectionEl.style.display = 'none';
  } else {
    renderIllinois();
    // Show and render Illinois-specific visualizations
    renderTimeline();
    renderNetwork();
  }
  renderPartyStats();
  renderLeaderboard();
}

// ==========================================
// Party Stats
// ==========================================
function renderPartyStats() {
  if (!currentData.length) {
    document.getElementById('demSponsored').textContent = '—';
    document.getElementById('demEnacted').textContent = '—';
    document.getElementById('demSuccessRate').textContent = '—';
    document.getElementById('repSponsored').textContent = '—';
    document.getElementById('repEnacted').textContent = '—';
    document.getElementById('repSuccessRate').textContent = '—';
    document.getElementById('allSponsored').textContent = '—';
    document.getElementById('allEnacted').textContent = '—';
    document.getElementById('allSuccessRate').textContent = '—';
    return;
  }

  const dems = currentData.filter(r => (r.party || '').toUpperCase() === 'D');
  const reps = currentData.filter(r => (r.party || '').toUpperCase() === 'R');

  const calcAvg = (arr, key) => {
    if (!arr.length) return 0;
    const sum = arr.reduce((acc, r) => acc + (r[key] ?? r.sponsored_total ?? 0), 0);
    return sum / arr.length;
  };

  const calcAvgSuccess = (arr) => {
    if (!arr.length) return 0;
    const rates = arr.map(r => {
      const sponsored = r.primary_sponsor_total ?? r.sponsored_total ?? 0;
      const enacted = r.enacted_total || 0;
      return calcSuccessRate(enacted, sponsored);
    });
    return rates.reduce((a, b) => a + b, 0) / rates.length;
  };

  // Democrats
  document.getElementById('demSponsored').textContent = calcAvg(dems, 'primary_sponsor_total').toFixed(1);
  document.getElementById('demEnacted').textContent = calcAvg(dems, 'enacted_total').toFixed(2);
  document.getElementById('demSuccessRate').textContent = fmtPct(calcAvgSuccess(dems));

  // Republicans
  document.getElementById('repSponsored').textContent = calcAvg(reps, 'primary_sponsor_total').toFixed(1);
  document.getElementById('repEnacted').textContent = calcAvg(reps, 'enacted_total').toFixed(2);
  document.getElementById('repSuccessRate').textContent = fmtPct(calcAvgSuccess(reps));

  // Overall
  document.getElementById('allSponsored').textContent = calcAvg(currentData, 'primary_sponsor_total').toFixed(1);
  document.getElementById('allEnacted').textContent = calcAvg(currentData, 'enacted_total').toFixed(2);
  document.getElementById('allSuccessRate').textContent = fmtPct(calcAvgSuccess(currentData));
}

// ==========================================
// Leaderboards
// ==========================================
function renderLeaderboard() {
  if (!currentData.length) {
    leaderboardList.innerHTML = '<li class="muted">No data loaded</li>';
    return;
  }

  let sorted = [];
  let valueFormatter = fmt;

  if (currentLeaderboard === 'sponsored') {
    sorted = [...currentData].sort((a, b) => {
      const va = a.primary_sponsor_total ?? a.sponsored_total ?? 0;
      const vb = b.primary_sponsor_total ?? b.sponsored_total ?? 0;
      return vb - va;
    });
    valueFormatter = (r) => fmt(r.primary_sponsor_total ?? r.sponsored_total ?? 0) + ' bills';
  } else if (currentLeaderboard === 'enacted') {
    sorted = [...currentData].sort((a, b) => (b.enacted_total || 0) - (a.enacted_total || 0));
    valueFormatter = (r) => fmt(r.enacted_total || 0) + ' laws';
  } else if (currentLeaderboard === 'success') {
    // Only include legislators with at least 5 sponsored bills for meaningful success rate
    const minBills = 5;
    sorted = currentData
      .filter(r => (r.primary_sponsor_total ?? r.sponsored_total ?? 0) >= minBills)
      .map(r => ({
        ...r,
        _successRate: calcSuccessRate(r.enacted_total || 0, r.primary_sponsor_total ?? r.sponsored_total ?? 0)
      }))
      .sort((a, b) => b._successRate - a._successRate);
    valueFormatter = (r) => fmtPct(r._successRate);
  } else if (currentLeaderboard === 'bipartisan') {
    // Only include legislators with bipartisan score data
    sorted = currentData
      .filter(r => r.bipartisan_score != null && r.bipartisan_score > 0)
      .sort((a, b) => (b.bipartisan_score || 0) - (a.bipartisan_score || 0));
    valueFormatter = (r) => fmtPct(r.bipartisan_score || 0);
  } else if (currentLeaderboard === 'velocity') {
    // Only include legislators with enacted bills (and thus velocity data)
    sorted = currentData
      .filter(r => r.avg_days_to_enactment != null && (r.enacted_total || 0) > 0)
      .sort((a, b) => (a.avg_days_to_enactment || 999) - (b.avg_days_to_enactment || 999)); // Lower is better
    valueFormatter = (r) => (r.avg_days_to_enactment || 0) + ' days';
  }

  const top10 = sorted.slice(0, 10);

  leaderboardList.innerHTML = top10.map((r, i) => {
    const partyClass = (r.party || '').toUpperCase() === 'D' ? 'dem' : ((r.party || '').toUpperCase() === 'R' ? 'rep' : '');
    const location = currentLegislature === 'congress' ? (r.state || '') : ('District ' + (r.district || ''));
    return `
      <li>
        <span class="lb-name">${r.sponsorName || 'Unknown'}</span>
        <span class="lb-party ${partyClass}">(${r.party || '?'}-${location})</span>
        <span class="lb-value">${valueFormatter(r)}</span>
      </li>
    `;
  }).join('');
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
  loadBtn.textContent = 'Load';
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
  ilLoadBtn.textContent = 'Load';
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

  const csvHeaders = ['Legislator', 'Chamber', 'Party', 'State', 'Primary', 'Cosponsor', 'Original Cosponsor', 'Public Laws', 'Private Laws', 'Total Laws', 'Success Rate'];
  const rows = currentData.map(r => {
    const sponsored = r.primary_sponsor_total ?? r.sponsored_total ?? 0;
    const enacted = r.enacted_total || 0;
    const successRate = calcSuccessRate(enacted, sponsored);
    return [
      r.sponsorName || '',
      r.chamber || '',
      r.party || '',
      r.state || '',
      sponsored,
      r.cosponsor_total || 0,
      r.original_cosponsor_total || 0,
      r.public_law_count || 0,
      r.private_law_count || 0,
      enacted,
      fmtPct(successRate),
    ];
  });

  downloadCSV(csvHeaders, rows, `congress_${congressInput.value}_stats.csv`);
}

function exportIllinoisCSV() {
  if (!currentData.length) {
    alert('No data to export. Please load data first.');
    return;
  }

  const csvHeaders = ['Legislator', 'Chamber', 'Party', 'District', 'Primary', 'Chief Co', 'Co', 'Enacted', 'Success Rate'];
  const rows = currentData.map(r => {
    const sponsored = r.primary_sponsor_total ?? r.sponsored_total ?? 0;
    const enacted = r.enacted_total || 0;
    const successRate = calcSuccessRate(enacted, sponsored);
    return [
      r.sponsorName || '',
      r.chamber || '',
      r.party || '',
      r.district || '',
      sponsored,
      r.chief_co_sponsor_total || 0,
      r.co_sponsor_total || 0,
      enacted,
      fmtPct(successRate),
    ];
  });

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
          'cosponsor_total',
          'original_cosponsor_total',
          'chief_co_sponsor_total',
          'co_sponsor_total',
          'enacted_total',
          'public_law_count',
          'private_law_count',
          'district',
          'success_rate',
          'avg_days_to_enactment',
          'bipartisan_score'
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

// Leaderboard tabs
leaderboardTabs.forEach(tab => {
  tab.addEventListener('click', () => {
    leaderboardTabs.forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    currentLeaderboard = tab.getAttribute('data-lb');
    renderLeaderboard();
  });
});

// ==========================================
// Timeline Chart
// ==========================================
let timelineChart = null;

async function renderTimeline() {
  const section = document.getElementById('timelineSection');
  if (currentLegislature !== 'illinois') {
    section.style.display = 'none';
    return;
  }
  section.style.display = 'block';

  const session = parseInt(ilSessionSelect.value || '104', 10);
  try {
    const resp = await fetch(`/api/il-timeline?session=${encodeURIComponent(session)}`);
    if (!resp.ok) return;
    const data = await resp.json();

    const ctx = document.getElementById('timelineChart').getContext('2d');

    // Destroy previous chart if exists
    if (timelineChart) {
      timelineChart.destroy();
    }

    // Format month labels nicely (YYYY-MM -> Month YYYY)
    const labels = data.months.map(m => {
      const [year, month] = m.split('-');
      const date = new Date(parseInt(year), parseInt(month) - 1);
      return date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
    });

    timelineChart = new Chart(ctx, {
      type: 'line',
      data: {
        labels: labels,
        datasets: [
          {
            label: 'Bills Filed',
            data: data.filed,
            borderColor: '#4a90d9',
            backgroundColor: 'rgba(74, 144, 217, 0.1)',
            fill: true,
            tension: 0.3,
          },
          {
            label: 'Bills Enacted',
            data: data.enacted,
            borderColor: '#2ecc71',
            backgroundColor: 'rgba(46, 204, 113, 0.1)',
            fill: true,
            tension: 0.3,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: 'top',
          },
          title: {
            display: false,
          },
        },
        scales: {
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: 'Number of Bills',
            },
          },
          x: {
            title: {
              display: true,
              text: 'Month',
            },
          },
        },
      },
    });
  } catch (e) {
    console.error('Error loading timeline:', e);
  }
}

// ==========================================
// Co-sponsor Network Graph
// ==========================================
let networkSimulation = null;
let networkZoom = null;
let networkSvg = null;
let networkG = null;
let networkNodes = null;

function networkPartyColor(party) {
  const p = (party || '').toUpperCase();
  if (p === 'D') return '#3b82f6';
  if (p === 'R') return '#ef4444';
  return '#6b7280';
}

function parseNetworkLinkEndpointId(endpoint) {
  if (endpoint && typeof endpoint === 'object') {
    return endpoint.id ? String(endpoint.id) : '';
  }
  return endpoint == null ? '' : String(endpoint);
}

function buildEdgeBundlingHierarchy(data) {
  const grouped = {};
  const connectionMap = new Map();

  (data.nodes || []).forEach(node => {
    const id = parseNetworkLinkEndpointId(node.id);
    if (!id) return;
    const chamber = (node.chamber || 'other').toLowerCase();
    const party = (node.party || 'other').toUpperCase();
    if (!grouped[chamber]) grouped[chamber] = {};
    if (!grouped[chamber][party]) grouped[chamber][party] = [];
    grouped[chamber][party].push(node);
    if (!connectionMap.has(id)) {
      connectionMap.set(id, new Set());
    }
  });

  (data.links || []).forEach(link => {
    const source = parseNetworkLinkEndpointId(link.source);
    const target = parseNetworkLinkEndpointId(link.target);
    if (!source || !target || source === target) return;
    if (!connectionMap.has(source) || !connectionMap.has(target)) return;
    connectionMap.get(source).add(target);
    connectionMap.get(target).add(source);
  });

  const chamberOrder = { house: 0, senate: 1 };
  const partyOrder = { D: 0, R: 1 };
  const root = { name: 'Illinois GA', children: [] };

  Object.keys(grouped)
    .sort((a, b) => (chamberOrder[a] ?? 99) - (chamberOrder[b] ?? 99) || a.localeCompare(b))
    .forEach(chamber => {
      const chamberNode = { name: chamber[0].toUpperCase() + chamber.slice(1), key: chamber, children: [] };
      Object.keys(grouped[chamber])
        .sort((a, b) => (partyOrder[a] ?? 99) - (partyOrder[b] ?? 99) || a.localeCompare(b))
        .forEach(party => {
          const partyNode = { name: party, key: party, children: [] };
          grouped[chamber][party]
            .slice()
            .sort((a, b) => (a.name || '').localeCompare(b.name || ''))
            .forEach(member => {
              const id = parseNetworkLinkEndpointId(member.id);
              if (!id) return;
              partyNode.children.push({
                name: member.name || id,
                id,
                party: member.party,
                chamber: member.chamber,
                district: member.district,
                connection_ids: Array.from(connectionMap.get(id) || []).sort(),
              });
            });
          if (partyNode.children.length) {
            chamberNode.children.push(partyNode);
          }
        });
      if (chamberNode.children.length) {
        root.children.push(chamberNode);
      }
    });

  return root;
}

function updateNetworkHint() {
  if (!networkHintEl) return;
  if (currentNetworkView === EDGE_BUNDLING_VIEW) {
    networkHintEl.textContent = 'Hierarchical edge bundling view. Drag to pan, scroll to zoom, and use Fit to reset.';
    return;
  }
  networkHintEl.textContent = 'Drag to pan, scroll to zoom. Click "Fit" to see all nodes.';
}

// Scale force parameters based on node count
function getNetworkForceParams(nodeCount) {
  if (nodeCount < 20) return { charge: -300, distance: 100, collision: 25 };
  if (nodeCount < 50) return { charge: -200, distance: 80, collision: 20 };
  if (nodeCount < 100) return { charge: -100, distance: 60, collision: 15 };
  return { charge: -50, distance: 40, collision: 10 };
}

function computeNetworkForcePositions(nodes, links, width, height) {
  if (!Array.isArray(nodes) || !nodes.length) return [];

  const forceParams = getNetworkForceParams(nodes.length);
  const simulationNodes = nodes.map(node => ({ ...node }));
  const simulationLinks = (links || []).map(link => ({
    source: parseNetworkLinkEndpointId(link.source),
    target: parseNetworkLinkEndpointId(link.target),
    value: Number(link.value) || 1,
  }));

  const simulation = d3.forceSimulation(simulationNodes)
    .force('link', d3.forceLink(simulationLinks).id(d => d.id).distance(forceParams.distance))
    .force('charge', d3.forceManyBody().strength(forceParams.charge))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide().radius(forceParams.collision))
    .force('x', d3.forceX(width / 2).strength(0.05))
    .force('y', d3.forceY(height / 2).strength(0.05))
    .stop();

  const iterations = Math.max(140, Math.min(360, simulationNodes.length * 4));
  for (let i = 0; i < iterations; i += 1) {
    simulation.tick();
  }
  simulation.stop();

  const padding = 50;
  return simulationNodes.map(node => ({
    id: String(node.id),
    x: Math.max(padding, Math.min(width - padding, node.x ?? width / 2)),
    y: Math.max(padding, Math.min(height - padding, node.y ?? height / 2)),
  }));
}

// Fit the graph to view
function fitNetworkToView() {
  if (!networkSvg || !networkZoom) return;
  if (currentNetworkView === EDGE_BUNDLING_VIEW) {
    networkSvg.transition()
      .duration(400)
      .call(networkZoom.transform, d3.zoomIdentity);
    return;
  }
  if (!networkG || !networkNodes || !networkNodes.length) return;

  const width = networkContainerEl.clientWidth || 800;
  const height = 500;
  const padding = 50;

  // Calculate bounding box of all nodes
  const xExtent = d3.extent(networkNodes, d => d.x);
  const yExtent = d3.extent(networkNodes, d => d.y);

  if (xExtent[0] == null || yExtent[0] == null) return;

  const graphWidth = xExtent[1] - xExtent[0] || 1;
  const graphHeight = yExtent[1] - yExtent[0] || 1;

  // Calculate scale to fit
  const scale = Math.min(
    (width - 2 * padding) / graphWidth,
    (height - 2 * padding) / graphHeight,
    2 // Max zoom level
  );

  // Calculate center offset
  const centerX = (xExtent[0] + xExtent[1]) / 2;
  const centerY = (yExtent[0] + yExtent[1]) / 2;

  const translateX = width / 2 - centerX * scale;
  const translateY = height / 2 - centerY * scale;

  // Apply transform with animation
  networkSvg.transition()
    .duration(500)
    .call(networkZoom.transform, d3.zoomIdentity.translate(translateX, translateY).scale(scale));
}

function renderForceNetworkGraph(data, showLabels) {
  // Store nodes reference for fit-to-view
  networkNodes = data.nodes;

  // Set up SVG dimensions
  const width = networkContainerEl.clientWidth || 800;
  const height = 500;

  // Compute force positions up front so layout is predictable and easier to compare.
  const computedPositions = computeNetworkForcePositions(data.nodes, data.links, width, height);
  const positionById = new Map(computedPositions.map(pos => [pos.id, pos]));
  data.nodes.forEach(node => {
    const pos = positionById.get(String(node.id));
    if (!pos) return;
    node.x = pos.x;
    node.y = pos.y;
  });

  // Create SVG with zoom support
  networkSvg = d3.select(networkContainerEl)
    .append('svg')
    .attr('width', width)
    .attr('height', height)
    .attr('viewBox', [0, 0, width, height])
    .style('cursor', 'grab');

  // Create a group for all zoomable content
  networkG = networkSvg.append('g').attr('class', 'zoom-group');

  // Set up zoom behavior
  networkZoom = d3.zoom()
    .scaleExtent([0.1, 4])
    .on('zoom', (event) => {
      networkG.attr('transform', event.transform);
    });

  networkSvg.call(networkZoom);

  // Get force parameters based on node count
  const forceParams = getNetworkForceParams(data.nodes.length);

  // Create simulation
  if (networkSimulation) {
    networkSimulation.stop();
  }

  networkSimulation = d3.forceSimulation(data.nodes)
    .force('link', d3.forceLink(data.links).id(d => d.id).distance(forceParams.distance))
    .force('charge', d3.forceManyBody().strength(forceParams.charge))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide().radius(forceParams.collision))
    .force('x', d3.forceX(width / 2).strength(0.05))
    .force('y', d3.forceY(height / 2).strength(0.05));

  // Draw links
  const link = networkG.append('g')
    .attr('class', 'links')
    .selectAll('line')
    .data(data.links)
    .join('line')
    .attr('stroke', '#999')
    .attr('stroke-opacity', 0.6)
    .attr('stroke-width', d => Math.sqrt(d.value));

  // Draw nodes
  const node = networkG.append('g')
    .attr('class', 'nodes')
    .selectAll('circle')
    .data(data.nodes)
    .join('circle')
    .attr('r', Math.max(5, 10 - data.nodes.length / 30))
    .attr('fill', d => networkPartyColor(d.party))
    .attr('stroke', '#fff')
    .attr('stroke-width', 1.5)
    .style('cursor', 'pointer')
    .call(d3.drag()
      .on('start', dragstarted)
      .on('drag', dragged)
      .on('end', dragended));

  // Add tooltips
  node.append('title')
    .text(d => `${d.name} (${d.party || '?'}-${d.district || '?'})`);

  // Add labels if enabled - scale font size based on node count
  let labels = null;
  if (showLabels) {
    // Calculate font size based on node count
    let fontSize = 10;
    if (data.nodes.length >= 100) fontSize = 7;
    else if (data.nodes.length >= 60) fontSize = 8;
    else if (data.nodes.length >= 30) fontSize = 9;

    labels = networkG.append('g')
      .attr('class', 'labels')
      .selectAll('text')
      .data(data.nodes)
      .join('text')
      .text(d => (d.name || '').split(' ').pop())  // Last name only
      .attr('font-size', fontSize + 'px')
      .attr('font-weight', '500')
      .attr('dx', 10)
      .attr('dy', 4)
      .attr('fill', '#1f2937')
      // Add white halo for readability
      .attr('stroke', 'white')
      .attr('stroke-width', 3)
      .attr('paint-order', 'stroke')
      .style('pointer-events', 'none');
  }

  // Update positions on tick with boundary awareness
  const padding = 50;
  networkSimulation.on('tick', () => {
    // Soft boundary constraint (pull towards center if too far)
    data.nodes.forEach(d => {
      d.x = Math.max(padding, Math.min(width - padding, d.x));
      d.y = Math.max(padding, Math.min(height - padding, d.y));
    });

    link
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);

    node
      .attr('cx', d => d.x)
      .attr('cy', d => d.y);

    if (labels) {
      labels
        .attr('x', d => d.x)
        .attr('y', d => d.y);
    }
  });

  // Auto fit-to-view after simulation stabilizes
  networkSimulation.on('end', () => {
    setTimeout(fitNetworkToView, 100);
  });

  function dragstarted(event, d) {
    if (!event.active) networkSimulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
  }

  function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
  }

  function dragended(event, d) {
    if (!event.active) networkSimulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
  }
}

function renderEdgeBundlingGraph(data, showLabels) {
  if (networkSimulation) {
    networkSimulation.stop();
    networkSimulation = null;
  }
  networkNodes = null;

  const width = networkContainerEl.clientWidth || 800;
  const height = 500;
  const radius = Math.max(120, Math.min(width, height) / 2 - 80);
  const hierarchyData = data.hierarchy || buildEdgeBundlingHierarchy(data);

  networkSvg = d3.select(networkContainerEl)
    .append('svg')
    .attr('width', width)
    .attr('height', height)
    .attr('viewBox', [0, 0, width, height])
    .style('cursor', 'grab');

  networkG = networkSvg.append('g').attr('class', 'zoom-group');

  networkZoom = d3.zoom()
    .scaleExtent([0.4, 5])
    .on('zoom', (event) => {
      networkG.attr('transform', event.transform);
    });

  networkSvg.call(networkZoom);

  const plot = networkG.append('g')
    .attr('transform', `translate(${width / 2},${height / 2})`);

  const root = d3.hierarchy(hierarchyData)
    .sort((a, b) => String(a.data.name || '').localeCompare(String(b.data.name || '')));

  d3.cluster()
    .size([Math.PI * 2, radius])(root);

  const leaves = root.leaves();
  if (!leaves.length) {
    networkContainerEl.innerHTML = '<p class="muted">No hierarchical groups available for edge bundling.</p>';
    return;
  }
  const leafById = new Map(leaves.map(leaf => [leaf.data.id, leaf]));
  const bundledEdges = [];

  (data.links || []).forEach(link => {
    const sourceId = parseNetworkLinkEndpointId(link.source);
    const targetId = parseNetworkLinkEndpointId(link.target);
    if (!sourceId || !targetId || sourceId === targetId) return;

    const sourceLeaf = leafById.get(sourceId);
    const targetLeaf = leafById.get(targetId);
    if (!sourceLeaf || !targetLeaf) return;

    bundledEdges.push({
      path: sourceLeaf.path(targetLeaf),
      value: Number(link.value) || 1,
    });
  });

  const radialLine = d3.lineRadial()
    .curve(d3.curveBundle.beta(0.85))
    .radius(d => d.y)
    .angle(d => d.x - Math.PI / 2);

  plot.append('g')
    .attr('class', 'bundle-links')
    .selectAll('path')
    .data(bundledEdges)
    .join('path')
    .attr('fill', 'none')
    .attr('stroke', '#9ca3af')
    .attr('stroke-width', d => 0.7 + Math.log2(d.value + 1) * 0.5)
    .attr('stroke-opacity', d => Math.min(0.8, 0.15 + Math.log2(d.value + 1) * 0.12))
    .attr('d', d => radialLine(d.path));

  const leafNodes = plot.append('g')
    .attr('class', 'bundle-nodes')
    .selectAll('circle')
    .data(leaves)
    .join('circle')
    .attr('r', 3.5)
    .attr('transform', d => `rotate(${(d.x * 180 / Math.PI) - 90}) translate(${d.y},0)`)
    .attr('fill', d => networkPartyColor(d.data.party))
    .attr('stroke', '#fff')
    .attr('stroke-width', 1);

  leafNodes.append('title')
    .text(d => `${d.data.name} (${d.data.party || '?'}-${d.data.district || '?'})`);

  if (showLabels) {
    plot.append('g')
      .attr('class', 'bundle-labels')
      .selectAll('text')
      .data(leaves)
      .join('text')
      .attr('transform', d => {
        const rotate = (d.x * 180 / Math.PI) - 90;
        const flip = d.x >= Math.PI ? 180 : 0;
        return `rotate(${rotate}) translate(${d.y + 8},0) rotate(${flip})`;
      })
      .attr('text-anchor', d => d.x < Math.PI ? 'start' : 'end')
      .attr('font-size', '8px')
      .attr('font-weight', '500')
      .attr('fill', '#1f2937')
      .attr('stroke', 'white')
      .attr('stroke-width', 3)
      .attr('paint-order', 'stroke')
      .style('pointer-events', 'none')
      .text(d => (d.data.name || '').split(' ').pop());
  }

  fitNetworkToView();
}

async function renderNetwork() {
  if (currentLegislature !== 'illinois') {
    networkSectionEl.style.display = 'none';
    return;
  }
  networkSectionEl.style.display = 'block';

  const session = parseInt(ilSessionSelect.value || '104', 10);
  const minConnections = parseInt(networkMinConnectionsEl.value || '3', 10);
  const showLabels = networkShowLabelsEl.checked;
  currentNetworkView = (networkGraphModeEl && networkGraphModeEl.value === EDGE_BUNDLING_VIEW)
    ? EDGE_BUNDLING_VIEW
    : NETWORK_VIEW;
  updateNetworkHint();

  try {
    const resp = await fetch(
      `/api/il-network?session=${encodeURIComponent(session)}&min_connections=${minConnections}&view=${encodeURIComponent(currentNetworkView)}`
    );
    if (!resp.ok) return;
    const data = await resp.json();

    networkContainerEl.innerHTML = '';

    if (!data.nodes.length) {
      networkContainerEl.innerHTML = '<p class="muted">No co-sponsor connections found with minimum ' + minConnections + ' shared bills.</p>';
      networkNodes = null;
      return;
    }

    if (currentNetworkView === EDGE_BUNDLING_VIEW) {
      renderEdgeBundlingGraph(data, showLabels);
    } else {
      renderForceNetworkGraph(data, showLabels);
    }
  } catch (e) {
    console.error('Error loading network:', e);
  }
}

// Zoom control functions
function networkZoomIn() {
  if (networkSvg && networkZoom) {
    networkSvg.transition().duration(300).call(networkZoom.scaleBy, 1.5);
  }
}

function networkZoomOut() {
  if (networkSvg && networkZoom) {
    networkSvg.transition().duration(300).call(networkZoom.scaleBy, 0.67);
  }
}

// ==========================================
// Event Handlers - Timeline & Network
// ==========================================
networkRefreshBtn.addEventListener('click', renderNetwork);
networkShowLabelsEl.addEventListener('change', renderNetwork);
networkMinConnectionsEl.addEventListener('change', renderNetwork);
networkZoomInBtn.addEventListener('click', networkZoomIn);
networkZoomOutBtn.addEventListener('click', networkZoomOut);
networkFitViewBtn.addEventListener('click', fitNetworkToView);
networkGraphModeEl.addEventListener('change', () => {
  currentNetworkView = networkGraphModeEl.value === EDGE_BUNDLING_VIEW ? EDGE_BUNDLING_VIEW : NETWORK_VIEW;
  try {
    localStorage.setItem(NETWORK_VIEW_STORAGE_KEY, currentNetworkView);
  } catch (err) {
    // Ignore localStorage write failures.
  }
  updateNetworkHint();
  renderNetwork();
});

// ==========================================
// Initialize
// ==========================================
document.addEventListener('DOMContentLoaded', () => {
  updateNetworkHint();
  loadData();
});
