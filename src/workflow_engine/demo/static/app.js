import {
  intervals,
  peakOverlap
} from './evidence.js';
const $ = id => document.getElementById(id);
const colors = ['#168a80', '#7862bc', '#c97935', '#427ba9'];
const stateColors = {
  SUCCEEDED: '#d8f2e6',
  RUNNING: '#d9eafa',
  READY: '#e8e4fb',
  RETRY_WAIT: '#fff0cf',
  FAILED: '#fbe1df',
  SKIPPED: '#e9edf1',
  PENDING: '#e9edf1'
};
const seconds = ns => Number(ns) / 1e9;
const utc = value => value ? new Date(value).toISOString().slice(11, 23) + 'Z' : '—';
const short = value => value ? value.slice(0, 8) : '—';
const el = (tag, text, cls) => {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (cls) node.className = cls;
  return node;
};

function badge(status) {
  return el('span', status, 'badge state-' + status);
}

function svg(tag, attrs, text) {
  const n = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  if (text !== undefined) n.textContent = text;
  return n;
}
let selected = new URLSearchParams(location.search).get('run');
$('follow').checked = !selected;
$('runs').addEventListener('change', () => {
  selected = $('runs').value;
  $('follow').checked = false;
  history.replaceState(null, '', '?run=' + selected);
});

function drawDag(data, workerLabel) {
  const target = $('dag');
  target.replaceChildren();
  const defs = data.run.definition.tasks;
  const levels = new Map();

  function level(key) {
    if (levels.has(key)) return levels.get(key);
    const task = defs.find(t => t.task_id === key);
    const n = task.depends_on.length ? 1 + Math.max(...task.depends_on.map(level)) : 0;
    levels.set(key, n);
    return n;
  }
  defs.forEach(t => level(t.task_id));
  const rows = new Map();
  for (const task of defs) {
    const n = levels.get(task.task_id);
    if (!rows.has(n)) rows.set(n, []);
    rows.get(n).push(task.task_id);
  }
  const width = 640,
    height = 248;
  target.setAttribute('viewBox', `0 0 ${width} ${height}`);
  const points = new Map();
  for (const [n, keys] of rows) keys.forEach((key, i) => points.set(key, {
    x: 20 + n * 460 / Math.max(1, rows.size - 1),
    y: keys.length === 1 ? 106 : 12 + i * 192 / (keys.length - 1)
  }));
  for (const task of defs)
    for (const parent of task.depends_on) {
      const a = points.get(parent),
        b = points.get(task.task_id);
      target.append(svg('path', {
        d: `M ${a.x+140} ${a.y+15} L ${b.x-8} ${b.y+15}`,
        fill: 'none',
        stroke: '#99aabd',
        'stroke-width': 1.7
      }));
      target.append(svg('path', {
        d: `M ${b.x-14} ${b.y+11} L ${b.x-8} ${b.y+15} L ${b.x-14} ${b.y+19}`,
        fill: 'none',
        stroke: '#99aabd'
      }));
    }
  for (const task of data.tasks) {
    const p = points.get(task.task_key);
    const attempts = data.attempts.filter(a => a.task_id === task.id);
    const latest = attempts.at(-1);
    target.append(svg('rect', {
      x: p.x,
      y: p.y,
      width: 140,
      height: 36,
      rx: 6,
      fill: stateColors[task.status] || '#eee'
    }));
    target.append(svg('text', {
      x: p.x + 7,
      y: p.y + 13,
      'font-size': 12,
      'font-weight': 600
    }, task.task_key + ' · ' + task.status));
    if (latest) target.append(svg('text', {
      x: p.x + 7,
      y: p.y + 25,
      'font-size': 9
    }, `#${latest.attempt_number} · ${workerLabel(latest.worker_session_id)}`));
  }
}

function render(data) {
  $('empty').hidden = true;
  $('content').hidden = false;
  $('scenario').textContent = data.run.scenario + ' scenario';
  $('run-id').textContent = 'Run ID  ' + data.run.id;
  $('run-status').replaceWith(Object.assign(badge(data.run.status), {
    id: 'run-status'
  }));
  $('freshness').textContent = 'Database snapshot ' + utc(data.snapshot_at) + ' · polling 500 ms';
  const values = intervals(data.samples),
    owners = new Set(data.attempts.map(a => a.worker_session_id).filter(Boolean));
  $('overlap').textContent = peakOverlap(values);
  $('owners').textContent = owners.size;
  $('sample-count').textContent = data.samples.length;
  const index = id => Math.max(0, data.workers.findIndex(w => w.id === id));
  const workerLabel = id => id ? `Worker ${index(id)+1}` : 'Unassigned';
  const color = id => colors[index(id) % colors.length];
  drawDag(data, workerLabel);
  $('workers').replaceChildren();
  for (const worker of data.workers) {
    const box = el('div', undefined, 'worker');
    box.style.setProperty('--worker', color(worker.id));
    const fresh = worker.status === 'ACTIVE' && new Date(worker.heartbeat_expires_at) > new Date(data.snapshot_at);
    box.append(el('b', `${workerLabel(worker.id)} · ${fresh?'heartbeat fresh':'offline / expired'} · ${worker.max_concurrency} slot(s)`));
    box.append(el('code', worker.id));
    box.append(el('small', worker.worker_name));
    box.append(el('p', `Registry ${worker.status} · heartbeat ${utc(worker.last_heartbeat_at)} · expires ${utc(worker.heartbeat_expires_at)}`, 'muted'));
    $('workers').append(box);
  }
  if (!data.workers.length) $('workers').append(el('p', 'Waiting for a demo Worker to register.', 'muted'));
  $('timeline').replaceChildren();
  for (const domain of new Set(values.map(v => v.domain))) {
    const group = values.filter(v => v.domain === domain);
    const first = group.reduce((a, v) => v.start < a ? v.start : a, group[0].start);
    const end = group.reduce((a, v) => v.end > a ? v.end : a, first);
    const span = Math.max(1, seconds(end - first));
    $('timeline').append(el('div', domain, 'domain'));
    for (const value of group.sort((a, b) => a.start < b.start ? -1 : 1)) {
      const attempt = data.attempts.find(a => a.id === value.attempt);
      const task = data.tasks.find(t => t.id === attempt.task_id);
      const lane = el('div', undefined, 'lane');
      const label = el('div', `${task.task_key} / Attempt ${attempt.attempt_number}`);
      label.append(el('small', `${workerLabel(attempt.worker_session_id)} · ${seconds(value.end-value.start).toFixed(2)}s ${value.finished?'observed':'observed so far; end unknown'}`));
      const track = el('div', undefined, 'track');
      const bar = el('div', undefined, 'bar' + (value.finished ? '' : ' unfinished'));
      bar.style.setProperty('--worker', color(attempt.worker_session_id));
      bar.style.left = (seconds(value.start - first) / span * 100) + '%';
      bar.style.width = (seconds(value.end - value.start) / span * 100) + '%';
      bar.title = `${short(value.id)}: ${value.count} real samples`;
      track.append(bar);
      lane.append(label, track);
      $('timeline').append(lane);
    }
    const axis = el('div', undefined, 'axis');
    for (let i = 0; i <= 4; i++) axis.append(el('span', (span * i / 4).toFixed(1) + 's'));
    $('timeline').append(axis);
  }
  if (!values.length) $('timeline').append(el('p', 'No Handler START received yet. Claimed/RUNNING does not mean observed execution.', 'muted'));
  $('attempts').replaceChildren();
  for (const attempt of data.attempts) {
    const task = data.tasks.find(t => t.id === attempt.task_id);
    const evidence = values.filter(v => v.attempt === attempt.id);
    const tr = el('tr');
    let td = el('td', `${task.task_key} / #${attempt.attempt_number}`);
    td.title = attempt.id;
    td.append(el('small', short(attempt.id)));
    tr.append(td);
    td = el('td', workerLabel(attempt.worker_session_id));
    td.append(el('small', short(attempt.worker_session_id)));
    tr.append(td);
    td = el('td');
    td.append(badge(attempt.status));
    tr.append(td, el('td', utc(attempt.acquired_at)));
    tr.append(el('td', evidence.map(v => `${seconds(v.end-v.start).toFixed(2)}s · ${v.finished?'FINISH observed':'no FINISH; end unknown'}`).join('; ') || 'No START observed'));
    tr.append(el('td', utc(attempt.accepted_at)));
    td = el('td', attempt.scheduled_at ? `Recorded ${utc(attempt.scheduled_at)}` : '—');
    if (attempt.available_at) td.append(el('small', `Eligible ${utc(attempt.available_at)}`));
    tr.append(td);
    $('attempts').append(tr);
  }
  $('raw').href = '/demo/runs/' + data.run.id;
  $('identities').textContent = JSON.stringify({
    run_id: data.run.id,
    tasks: data.tasks.map(t => ({
      key: t.task_key,
      id: t.id
    })),
    attempts: data.attempts.map(a => ({
      id: a.id,
      task_id: a.task_id,
      number: a.attempt_number,
      worker_id: a.worker_session_id
    }))
  }, null, 2);
}

async function get(path) {
  const response = await fetch(path, {
    cache: 'no-store',
    signal: AbortSignal.timeout(5000)
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}
async function poll() {
  try {
    const runs = await get('/demo/runs');
    if ($('follow').checked && runs.length) selected = runs[0].run_id;
    const signature = JSON.stringify(runs.map(r => [r.run_id, r.status]));
    if ($('runs').dataset.signature !== signature) {
      $('runs').replaceChildren(...runs.map(r => {
        const option = el('option', `${r.scenario} · ${short(r.run_id)} · ${r.status}`);
        option.value = r.run_id;
        return option;
      }));
      $('runs').dataset.signature = signature;
    }
    if (selected) {
      $('runs').value = selected;
      const id = selected;
      const data = await get('/demo/runs/' + encodeURIComponent(id));
      if (id === selected) render(data);
    }
    $('error').hidden = true;
  } catch (error) {
    $('error').hidden = false;
    $('error').textContent = 'Snapshot unavailable — showing previous data; Worker state may be stale. ' + error.message;
    $('freshness').textContent = 'Disconnected · data frozen';
  } finally {
    setTimeout(poll, 500);
  }
}
poll();
