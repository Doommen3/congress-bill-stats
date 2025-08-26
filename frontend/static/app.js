const statusEl = document.getElementById('status');
const tableBody = document.getElementById('tbody');
const chamberFilter = document.getElementById('chamberFilter');
const searchBox = document.getElementById('searchBox');
const congressInput = document.getElementById('congressInput');
const loadBtn = document.getElementById('loadBtn');
const exportBtn = document.getElementById('exportBtn');
const headers = document.querySelectorAll('.th-sort');

let currentData = [];
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
      <td class="right">${fmt(r.public_law_total || 0)}</td>
      <td class="right">${fmt(r.private_law_total || 0)}</td>
      <td class="right">${fmt(r.enacted_total || 0)}</td>
    </tr>
  `).join('');
}

async function loadData() {
  const congress = parseInt(congressInput.value || '119', 10);
  setStatus('Loading (first run may take a few minutes)…');
  loadBtn.disabled = true; loadBtn.textContent = 'Loading…';
  try {
    const r = await fetch(`/api/stats?congress=${encodeURIComponent(congress)}`);
    if (!r.ok) {
      const t = await r.text();
      setStatus('Error: ' + t.slice(0, 300));
      return;
    }
    const json = await r.json();
    currentData = json.rows || [];
    setStatus(`Loaded ${currentData.length} legislators for the ${json.congress}th Congress.`);
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
      sortDir = (key === 'sponsored_total' || key === 'enacted_total' ||
                 key === 'public_law_total' || key === 'private_law_total') ? 'desc' : 'asc';
    }
    render();
  });
});

loadBtn.addEventListener('click', () => loadData());
searchBox.addEventListener('input', () => render());
chamberFilter.addEventListener('change', () => render());

// Auto-load on first paint
document.addEventListener('DOMContentLoaded', () => loadData());
