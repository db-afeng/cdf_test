const $ = id => document.getElementById(id);
const terminal = new Set(['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']);
const fmt = n => Number(n).toLocaleString();
let loading = false;
let acting = false;
let busy = false;
let activeRun = Number(sessionStorage.getItem('cdf-run')) || null;
let lastState = null;

function notice(text, error = false) {
  $('notice').textContent = text;
  $('notice').hidden = !text;
  $('notice').classList.toggle('error', error);
}

function controls() {
  document.querySelectorAll('.run, #delete').forEach(button => {
    button.disabled = loading || acting || busy || Boolean(activeRun);
  });
  $('refresh').disabled = loading || acting;
  $('pipeline-status').setAttribute('aria-busy', String(loading));
  $('record-section').setAttribute('aria-busy', String(loading));
}

async function api(url, body) {
  const options = body ? {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Demo-Action': '1' },
    body: JSON.stringify(body)
  } : {};
  const response = await fetch(url, options);
  const data = await response.json();
  if (!response.ok) throw Error(data.error || 'Request failed');
  return data;
}

function cell(row, value, status, numeric = false) {
  const td = document.createElement('td');
  if (numeric) td.className = 'numeric';
  if (status) {
    const span = document.createElement('span');
    span.className = `status ${status}`;
    span.textContent = value;
    td.appendChild(span);
  } else {
    td.textContent = value ?? '-';
  }
  row.appendChild(td);
}

function emptyRow(target, columns, text) {
  const row = document.createElement('tr');
  const td = document.createElement('td');
  td.colSpan = columns;
  td.className = 'empty-state';
  td.textContent = text;
  row.appendChild(td);
  $(target).appendChild(row);
}

function ledger(id, rows) {
  $(id).replaceChildren();
  if (!rows.length) emptyRow(id, 4, 'No events recorded.');
  for (const entry of rows) {
    const row = document.createElement('tr');
    cell(row, entry.version);
    cell(row, entry.change, entry.change === 'delete' ? 'delete' : null);
    cell(row, fmt(entry.events), null, true);
    cell(row, entry.sample_ids.join(', '));
    $(id).appendChild(row);
  }
}

function renderRecords() {
  if (!lastState) return;
  const selected = new Set(lastState.focused);
  const showSamples = $('show-samples').checked;
  const rows = lastState.records
    .filter(row => showSamples || selected.has(row.order_id))
    .sort((a, b) => Number(selected.has(b.order_id)) - Number(selected.has(a.order_id)) || a.order_id - b.order_id);
  $('records').replaceChildren();
  if (!rows.length) emptyRow('records', 7, 'Enter an order ID above to check its outputs.');
  for (const record of rows) {
    const row = document.createElement('tr');
    if (selected.has(record.order_id) && showSamples) row.className = 'focused';
    cell(row, record.order_id);
    cell(row, record.customer);
    cell(row, record.region);
    cell(row, record.amount == null ? '-' : '$' + Number(record.amount).toFixed(2), null, true);
    cell(row, record.source_present ? 'Present' : 'Removed', record.source_present ? 'present' : 'absent');
    cell(row, record.a_present ? (record.source_present ? 'Present' : 'Pending delete') : 'Absent',
      record.a_present ? (record.source_present ? 'present' : 'stale') : 'absent');
    cell(row, record.b_lines, record.b_lines && !record.source_present ? 'stale' : record.b_lines ? 'present' : 'absent', true);
    $('records').appendChild(row);
  }
}

function renderState(data) {
  lastState = data;
  const metrics = data.metrics;
  busy = data.busy;
  $('namespace').textContent = data.namespace;
  $('freshness').textContent = `Updated ${new Date(data.observed_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} | Reset baseline: version ${data.baseline.baseline_version}`;
  for (const [id, key] of [
    ['source-count', 'source_rows'], ['a-count', 'a_rows'], ['b-count', 'b_rows'],
    ['source-events', 'source_round_events'], ['a-events', 'a_round_events'], ['b-events', 'b_round_events']
  ]) $(id).textContent = fmt(metrics[key]);
  const pendingA = Math.max(0, metrics.source_round_events - metrics.a_round_events);
  const pendingB = Math.max(0, metrics.source_round_events - metrics.b_round_events);
  for (const [id, count] of [['a-pending', pendingA], ['b-pending', pendingB]]) {
    $(id).textContent = fmt(count);
    $(id).classList.toggle('pending-count', count > 0);
  }
  $('a-total').textContent = fmt(metrics.a_total_events) + ' total events';
  $('b-total').textContent = fmt(metrics.b_total_events) + ' total events';
  $('evidence-copy').textContent = metrics.source_round_events === 0
    ? 'No source changes since the last reset.'
    : pendingA === 0 && pendingB === 0
      ? `A and B each processed ${fmt(metrics.source_round_events)} new source event${metrics.source_round_events === 1 ? '' : 's'}.`
      : `Pending source events: A ${fmt(pendingA)}, B ${fmt(pendingB)}. Run the remaining pipeline${pendingA > 0 && pendingB > 0 ? 's' : ''} to apply the changes.`;
  renderRecords();
  ledger('ledger-a', data.ledger_a);
  ledger('ledger-b', data.ledger_b);
  if (!activeRun && data.runs.length) {
    activeRun = data.runs[0].run_id;
    sessionStorage.setItem('cdf-run', activeRun);
  }
}

async function refresh() {
  if (loading) return;
  loading = true;
  controls();
  try {
    const data = await api('/api/state?ids=' + encodeURIComponent($('ids').value || '42'));
    renderState(data);
    if ($('notice').textContent === 'Loading data...') notice('');
  } catch (error) {
    $('freshness').textContent = lastState ? 'Refresh failed. These counts may be out of date.' : 'Data unavailable.';
    notice(error.message + ' You can retry with Refresh data.', true);
  } finally {
    loading = false;
    controls();
  }
}

const stateLabels = {
  PENDING: 'Waiting to start', QUEUED: 'Queued', BLOCKED: 'Waiting for an earlier task',
  RUNNING: 'Running', TERMINATING: 'Finishing', TERMINATED: 'Finished',
  SKIPPED: 'Skipped', INTERNAL_ERROR: 'Failed', WAITING_FOR_RETRY: 'Waiting to retry',
  SUCCESS: 'Complete', FAILED: 'Failed', CANCELED: 'Cancelled', TIMEDOUT: 'Timed out'
};
const taskLabels = { restore_source: 'Restore orders', engineer_a: 'Engineer A', engineer_b: 'Engineer B' };

async function pollRun() {
  if (!activeRun) return;
  try {
    const data = await api('/api/run/' + activeRun);
    $('run-panel').hidden = false;
    $('run-link').href = data.url;
    const tasks = data.tasks.map(task => `${taskLabels[task.name] || task.name}: ${stateLabels[task.result || task.state] || task.result || task.state}`);
    $('run-status').textContent = [stateLabels[data.result || data.state] || data.state, ...tasks].join(' | ');
    if (terminal.has(data.state)) {
      activeRun = null;
      sessionStorage.removeItem('cdf-run');
      busy = false;
      notice(data.result === 'SUCCESS' ? 'Run complete. Counts refreshed.' : `Run ${stateLabels[data.result] || 'failed'}. ${data.message || 'Open the run for details.'}`, data.result !== 'SUCCESS');
      await refresh();
    }
  } catch (error) {
    notice(error.message, true);
  } finally {
    controls();
  }
}

async function action(fn) {
  acting = true;
  controls();
  try { await fn(); }
  catch (error) { notice(error.message, true); }
  finally { acting = false; controls(); }
}

$('delete').addEventListener('click', () => action(async () => {
  const data = await api('/api/delete', { ids: $('ids').value });
  notice(data.message);
  await refresh();
}));
const actionLabels = { a: 'Engineer A', b: 'Engineer B', both: 'Both pipelines', reset: 'Reset' };
document.querySelectorAll('.run').forEach(button => button.addEventListener('click', () => action(async () => {
  const selectedAction = button.dataset.action;
  const data = await api('/api/run', { action: selectedAction });
  activeRun = data.run_id;
  sessionStorage.setItem('cdf-run', activeRun);
  notice(`${actionLabels[selectedAction]} started.`);
  await pollRun();
})));
$('refresh').addEventListener('click', refresh);
$('ids').addEventListener('change', refresh);
$('show-samples').addEventListener('change', renderRecords);

async function tick() {
  if (!document.hidden && !acting) {
    if (activeRun) await pollRun();
    else if (busy) await refresh();
  }
  setTimeout(tick, 8000);
}
refresh().then(pollRun);
setTimeout(tick, 8000);
