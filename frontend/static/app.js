const statusEl = document.getElementById('status');
const summaryEl = document.getElementById('summary');
const tableBody = document.getElementById('tbody');
const chamberFilter = document.getElementById('chamberFilter');
const searchBox = document.getElementById('searchBox');
const congressInput = document.getElementById('congressInput');
const loadBtn = document.getElementById('loadBtn');
const exportBtn = document.getElementById('exportBtn');
const headers = document.querySelectorAll('.th-sort');

let currentData = [];
let currentSummary = null;
let sortKey = 'sponsored_total';
let sortDir = 'desc';

function fmt(n) { return new Intl.NumberFormat().format(n); }
function setStatus(msg) { statusEl.textContent = msg || ''; }

function render() {
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

  // Update summary if available
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

async function loadData(forceRefresh = false) {
  const congress = parseInt(congressInput.value || '119', 10);
  setStatus('Loading (first run may take a few minutes)…');
  loadBtn.disabled = true; loadBtn.textContent = 'Loading…';
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

    // Format the generated_at timestamp
    const genTime = json.generated_at ? new Date(json.generated_at * 1000).toLocaleString() : 'unknown';
    setStatus(`Loaded ${currentData.length} legislators for the ${json.congress}th Congress. Data as of: ${genTime}`);
    render();
  } catch (e) {
    setStatus('Network error: ' + (e && e.message ? e.message : e));
  } finally {
    loadBtn.disabled = false; loadBtn.textContent = 'Load / Refresh';
  }
}

headers.forEach(th => {
  th.addEventListener('click', () => {
    const key = th.getAttribute('data-key');
    if (sortKey === key) {
      sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      sortKey = key;
      // Default to descending for numeric columns
      const numericCols = ['sponsored_total', 'enacted_total', 'public_law_count', 'private_law_count'];
      sortDir = numericCols.includes(key) ? 'desc' : 'asc';
    }
    render();
  });
});

function exportCSV() {
  if (!currentData.length) {
    alert('No data to export. Please load data first.');
    return;
  }

  const headers = ['Legislator', 'Chamber', 'Party', 'State', 'Sponsored', 'Public Laws', 'Private Laws', 'Total Laws'];
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

  const csvContent = [
    headers.join(','),
    ...rows.map(row => row.map(cell => {
      // Escape quotes and wrap in quotes if contains comma
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
  a.download = `congress_${congressInput.value}_stats.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

loadBtn.addEventListener('click', () => loadData());
exportBtn.addEventListener('click', exportCSV);
searchBox.addEventListener('input', () => render());
chamberFilter.addEventListener('change', () => render());

// Auto-load on first paint
document.addEventListener('DOMContentLoaded', () => loadData());
