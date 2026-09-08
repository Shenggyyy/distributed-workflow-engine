import {
  dependencyView,
  workerView,
  leaseView,
  recoveryView,
  dagRows,
  latestAttempt
} from './flow.js';
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
  const rows = dagRows(defs);
  const width = Math.max(660, Math.max(...rows.map(row => row.length)) * 165);
  const height = rows.length * 100 + 20;
  target.setAttribute('viewBox', `0 0 ${width} ${height}`);
  target.style.height = height + 'px';
  const points = new Map();
  rows.forEach((keys, row) => keys.forEach((key, i) => points.set(key, {
    x: (i + 0.5) * width / keys.length - 70,
    y: 12 + row * 100
  })));
  for (const task of defs)
    for (const parent of task.depends_on) {
      const a = points.get(parent),
        b = points.get(task.task_id);
      target.append(svg('path', {
        d: `M ${a.x+70} ${a.y+36} L ${b.x+70} ${b.y-6}`,
        fill: 'none',
        stroke: '#99aabd',
        'stroke-width': 1.7
      }));
      target.append(svg('path', {
        d: `M ${b.x+66} ${b.y-12} L ${b.x+70} ${b.y-6} L ${b.x+74} ${b.y-12}`,
        fill: 'none',
        stroke: '#99aabd'
      }));
    }
  for (const task of data.tasks) {
    const p = points.get(task.task_key);
    const latest = latestAttempt(data, task.id);
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

function renderFlow(data, values, workerLabel, color) {
  const taskName = attempt => data.tasks.find(t => t.id === attempt.task_id)?.task_key || 'Unknown';
  $('scenario-purpose').textContent = {
    parallel: '观察 A / B / C / D 如何使用同一个 Worker 的两个槽位；Join 必须等待四个任务全部成功。',
    distribution: '两个独立容器从同一个 Run 领取任务。观察下面实际出现的归属，以及两种颜色的采样区间是否重叠。',
    recovery: '专用 Worker 被停止后，观察心跳、Lease 与旧 Attempt；重试由新的领取生成新 Attempt。'
  } [data.run.scenario] || '查看本次真实定义与执行结果。';
  const definitionText = JSON.stringify(data.run.definition, null, 2);
  if ($('definition').textContent !== definitionText) $('definition').textContent = definitionText;
  $('ready-tasks').replaceChildren();
  $('waiting-tasks').replaceChildren();
  const describe = view => ({
    claimable: view.dependencies.length ? '所有依赖已成功；数据库状态 READY，可申请领取。' : '无前置依赖；数据库状态 READY，可申请领取。',
    inconsistent: 'READY 与依赖快照不一致，请检查原始数据；不宣称可执行。',
    dependencies: '等待：' + view.blockers.map(d => `${d.key} (${d.status})`).join('、'),
    scheduler: '依赖已满足，仍是 PENDING：等待下一次调度事务更新。',
    backoff: '重试退避中；可重试时间 ' + utc(view.last?.available_at),
    retry_due: '已到可重试时间；仍是 RETRY_WAIT，等待调度事务更新为 READY。',
    retry_unknown: 'RETRY_WAIT，但快照没有可用重试时间。'
  } [view.reason] || view.task.status);
  for (const task of data.tasks) {
    if (!['READY', 'PENDING', 'RETRY_WAIT'].includes(task.status)) continue;
    const view = dependencyView(data, task),
      card = el('div', undefined, 'task-card');
    card.append(el('b', task.task_key), badge(task.status), el('p', describe(view)));
    const target = task.status === 'READY' ? 'ready-tasks' : 'waiting-tasks';
    $(target).append(card);
  }
  if (!$('ready-tasks').children.length) $('ready-tasks').append(el('p', '当前没有 READY 任务。已领取任务见第 04 步。', 'muted'));
  if (!$('waiting-tasks').children.length) $('waiting-tasks').append(el('p', '当前没有等待依赖或重试的任务。', 'muted'));
  const leaseLabels = {
    unknown: '未记录 Lease',
    historical: '历史 Lease；Attempt 已为终态',
    expired: 'Lease 已到期；等待引擎确认',
    not_due: 'Lease 在本快照尚未到期'
  };
  $('workers').replaceChildren();
  $('owner-summary').replaceChildren();
  for (const worker of data.workers) {
    const view = workerView(data, worker),
      box = el('div', undefined, 'worker');
    box.style.setProperty('--worker', color(worker.id));
    const heartbeatLabel = {
      fresh: '心跳新鲜',
      expired: '心跳已到期，等待会话状态更新',
      lost: '会话 LOST',
      stopped: '会话 STOPPED'
    } [view.heartbeat];
    box.append(el('b', `${workerLabel(worker.id)} · ${heartbeatLabel}`), el('code', worker.id), el('small', worker.worker_name));
    box.append(el('p', `${worker.max_concurrency} 个配置槽位 · ${view.allocations.length} 个 RUNNING 归属`, 'capacity'));
    box.append(el('p', `最后心跳 ${utc(worker.last_heartbeat_at)} · 截止 ${utc(worker.heartbeat_expires_at)} · registry ${worker.status}`, 'muted'));
    for (const attempt of view.allocations) {
      const card = el('div', undefined, 'allocation');
      card.append(el('b', `${taskName(attempt)} / Attempt ${attempt.attempt_number}`), badge(attempt.status));
      card.append(el('p', `已确认领取 ${utc(attempt.acquired_at)}`));
      const observations = data.samples.filter(s => s.attempt_id === attempt.id);
      const evidence = values.filter(v => v.attempt === attempt.id);
      card.append(el('p', evidence.length ? evidence.map(v => `${v.finished ? '已收到 FINISH，查看完成回报' : '已有 START / PULSE，尚无 FINISH；当前是否仍在执行未知'} · ${seconds(v.end-v.start).toFixed(2)}s 已观测`).join('；') : '尚无 Handler START：领取不等于已开始执行。'));
      const lastReceipt = observations.map(s => s.recorded_at).sort().at(-1);
      if (lastReceipt) card.append(el('small', `最后采样接收 (DB) ${utc(lastReceipt)}；不是执行时钟`));
      card.append(el('p', `${leaseLabels[leaseView(data, attempt)]} · 最近续约 ${utc(attempt.last_renewed_at)} · 截止 ${utc(attempt.lease_expires_at)}`, 'lease'));
      box.append(card);
    }
    if (!view.allocations.length) box.append(el('p', view.runTerminal ? 'Run 已结束，无当前领取。正常退出后的心跳过期不能证明任务失败。' : '无当前领取；不推测是否正在发送领取请求。', 'muted'));
    const last = view.attempts.at(-1);
    if (last && !view.allocations.length) box.append(el('p', `保留的 Attempt：${taskName(last)} #${last.attempt_number} ${last.status} · ${leaseLabels[leaseView(data, last)]} ${utc(last.lease_expires_at)}`, 'muted'));
    $('workers').append(box);
    const summary = el('p', `${workerLabel(worker.id)}：${view.attempts.map(a => `${taskName(a)} #${a.attempt_number} ${a.status}`).join(' · ') || '尚无领取'}`, 'owner-line');
    summary.style.setProperty('--worker', color(worker.id));
    $('owner-summary').append(summary);
  }
  if (!data.workers.length) $('workers').append(el('p', '等待专用演示 Worker 注册。', 'muted'));
  $('results').replaceChildren();
  for (const task of data.tasks.filter(t => t.status === 'SUCCEEDED')) {
    const next = data.run.definition.tasks.filter(t => t.depends_on.includes(task.task_key));
    $('results').append(el('p', `${task.task_key} 已成功${next.length ? ' → 为 ' + next.map(t => t.task_id).join('、') + ' 满足一项依赖；各后续任务仍需全部依赖成功。' : '；没有后续依赖任务。'}`, 'success-line'));
  }
  if (!$('results').children.length) $('results').append(el('p', '尚无成功任务。等待真实完成回报与调度结果。', 'muted'));
  $('recovery').replaceChildren();
  for (const attempt of data.attempts.filter(a => ['LOST', 'FAILED', 'TIMED_OUT'].includes(a.status))) {
    const view = recoveryView(data, attempt),
      box = el('div', undefined, 'recovery-card');
    box.append(el('h3', `${taskName(attempt)} · 旧 Attempt #${attempt.attempt_number} ${attempt.status}`));
    const stages = el('div', undefined, 'recovery-stages');
    stages.append(el('p', `① 旧归属 ${workerLabel(attempt.worker_session_id)}\n历史 Lease 截止 ${utc(attempt.lease_expires_at)}\n旧结果不会授权新 Attempt。`));
    stages.append(el('p', attempt.scheduled_at ? `② 重试计划已保存\n记录 ${utc(attempt.scheduled_at)}\n可重试 ${utc(attempt.available_at)}` : '② 未记录重试计划；不推测会重试。'));
    stages.append(el('p', view.next ? `③ 新领取已确认\nAttempt #${view.next.attempt_number} · ${workerLabel(view.next.worker_session_id)}\n${utc(view.next.acquired_at)} · ${view.next.status}` : '③ 尚未记录下一次领取\n重试不会复用旧 Attempt。'));
    box.append(stages);
    box.append(el('p', '当前 Task 状态：' + data.tasks.find(t => t.id === attempt.task_id).status + '。以上是持久化记录，不是推测的消息流。', 'muted'));
    $('recovery').append(box);
  }
  if (!$('recovery').children.length) $('recovery').append(el('p', '尚无失效或失败 Attempt 记录。心跳停更、Lease 到期与引擎确认可能出现在不同快照。', 'muted'));
  $('replacement-note').hidden = data.run.scenario !== 'recovery';
  const successes = data.tasks.filter(t => t.status === 'SUCCEEDED').length;
  $('outcome').textContent = `Run ${data.run.status} · ${successes}/${data.tasks.length} 个任务成功。` +
    (data.run.status === 'SUCCEEDED' ? '引擎已确认整个工作流成功。' : data.run.status === 'FAILED' ? '引擎已确认工作流失败，请查看失败与跳过的任务。' : '工作流尚未完成；下方保留已经观测到的执行证据。');
}

function render(data) {
  $('empty').hidden = true;
  $('content').hidden = false;
  $('scenario').textContent = ({
    parallel: '并行 · 一个 Worker，两个槽位',
    distribution: '分配 · 两个独立 Worker',
    recovery: '恢复 · 新 Attempt 重新执行'
  } [data.run.scenario] || data.run.scenario);
  $('run-id').textContent = 'Run ID  ' + data.run.id;
  $('run-status').replaceWith(Object.assign(badge(data.run.status), {
    id: 'run-status'
  }));
  $('freshness').textContent = '数据库快照 ' + utc(data.snapshot_at) + ' · 轮询 500 ms';
  const values = intervals(data.samples),
    owners = new Set(data.attempts.map(a => a.worker_session_id).filter(Boolean));
  $('overlap').textContent = peakOverlap(values);
  $('owners').textContent = owners.size;
  $('sample-count').textContent = data.samples.length;
  const index = id => data.workers.findIndex(w => w.id === id);
  const workerLabel = id => !id ? 'Unassigned' : index(id) < 0 ? `未登记 Worker ${short(id)}` : `Worker ${index(id)+1}`;
  const color = id => index(id) < 0 ? '#748398' : colors[index(id) % colors.length];
  drawDag(data, workerLabel);
  renderFlow(data, values, workerLabel, color);
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
