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
    document.getElementById('networkSection').style.display = 'none';
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

// Scale force parameters based on node count
function getNetworkForceParams(nodeCount) {
  if (nodeCount < 20) return { charge: -300, distance: 100, collision: 25 };
  if (nodeCount < 50) return { charge: -200, distance: 80, collision: 20 };
  if (nodeCount < 100) return { charge: -100, distance: 60, collision: 15 };
  return { charge: -50, distance: 40, collision: 10 };
}

// Fit the graph to view
function fitNetworkToView() {
  if (!networkSvg || !networkG || !networkNodes || !networkNodes.length) return;

  const container = document.getElementById('networkContainer');
  const width = container.clientWidth || 800;
  const height = 500;
  const padding = 50;

  // Calculate bounding box of all nodes
  const xExtent = d3.extent(networkNodes, d => d.x);
  const yExtent = d3.extent(networkNodes, d => d.y);

  if (!xExtent[0] || !yExtent[0]) return;

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

async function renderNetwork() {
  const section = document.getElementById('networkSection');
  if (currentLegislature !== 'illinois') {
    section.style.display = 'none';
    return;
  }
  section.style.display = 'block';

  const session = parseInt(ilSessionSelect.value || '104', 10);
  const minConnections = parseInt(document.getElementById('networkMinConnections').value || '3', 10);
  const showLabels = document.getElementById('networkShowLabels').checked;

  try {
    const resp = await fetch(`/api/il-network?session=${encodeURIComponent(session)}&min_connections=${minConnections}`);
    if (!resp.ok) return;
    const data = await resp.json();

    const container = document.getElementById('networkContainer');
    container.innerHTML = '';

    if (!data.nodes.length) {
      container.innerHTML = '<p class="muted">No co-sponsor connections found with minimum ' + minConnections + ' shared bills.</p>';
      networkNodes = null;
      return;
    }

    // Store nodes reference for fit-to-view
    networkNodes = data.nodes;

    // Set up SVG dimensions
    const width = container.clientWidth || 800;
    const height = 500;

    // Create SVG with zoom support
    networkSvg = d3.select(container)
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

    // Color scale by party
    const partyColor = (party) => {
      const p = (party || '').toUpperCase();
      if (p === 'D') return '#3b82f6';
      if (p === 'R') return '#ef4444';
      return '#6b7280';
    };

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
      .attr('fill', d => partyColor(d.party))
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

    // Add labels if enabled (only for smaller graphs or when explicitly requested)
    let labels = null;
    if (showLabels && data.nodes.length < 80) {
      labels = networkG.append('g')
        .attr('class', 'labels')
        .selectAll('text')
        .data(data.nodes)
        .join('text')
        .text(d => d.name.split(' ').pop())  // Last name only
        .attr('font-size', data.nodes.length < 30 ? '10px' : '8px')
        .attr('dx', 12)
        .attr('dy', 4)
        .attr('fill', '#374151')
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
document.getElementById('networkRefresh').addEventListener('click', renderNetwork);
document.getElementById('networkShowLabels').addEventListener('change', renderNetwork);
document.getElementById('networkMinConnections').addEventListener('change', renderNetwork);
document.getElementById('networkZoomIn').addEventListener('click', networkZoomIn);
document.getElementById('networkZoomOut').addEventListener('click', networkZoomOut);
document.getElementById('networkFitView').addEventListener('click', fitNetworkToView);

// ==========================================
// Initialize
// ==========================================
document.addEventListener('DOMContentLoaded', () => loadData());
