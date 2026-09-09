import {
  createI18n
} from './i18n.js';
import {
  dependencyView,
  workerView,
  leaseView,
  recoveryView,
  dagRows,
  latestAttempt,
  isDiamond,
  timedDiamondScenario,
  diamondRecoveryView
} from './flow.js';
import {
  intervals,
  peakOverlap
} from './evidence.js';
const $ = id => document.getElementById(id);
const language = createI18n({
  languages: navigator.languages?.length ? navigator.languages : [navigator.language],
  storage: () => window.localStorage
});
const t = (key, params) => language.t(key, params);
let latestSnapshot = null;
let latestRuns = [];
let hasListed = false;
let lastError = null;
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
  const taskName = attempt => data.tasks.find(t => t.id === attempt.task_id)?.task_key || t('common.unknown');
  const diamond = timedDiamondScenario(data.run.definition, data.run.scenario);
  $('scenario-purpose').textContent = diamond ? t('scenario.diamond.' + diamond) :
    t('scenario.purpose.' + (['parallel', 'distribution', 'recovery'].includes(data.run.scenario) ? data.run.scenario : 'unknown'));
  const definitionText = JSON.stringify(data.run.definition, null, 2);
  if ($('definition').textContent !== definitionText) $('definition').textContent = definitionText;
  $('ready-tasks').replaceChildren();
  $('waiting-tasks').replaceChildren();
  const describe = view => {
    if (view.reason === 'claimable') return t(view.dependencies.length ? 'dependency.claimable' : 'dependency.root');
    if (view.reason === 'dependencies') return t('dependency.dependencies', {
      blockers: view.blockers.map(d => t('dependency.blocker', {
        task: d.key,
        status: d.status
      })).join(t('common.listSeparator'))
    });
    if (view.reason === 'backoff') return t('dependency.backoff', {
      time: utc(view.last?.available_at)
    });
    return ['inconsistent', 'scheduler', 'retry_due', 'retry_unknown'].includes(view.reason) ? t('dependency.' + view.reason) : view.task.status;
  };
  for (const task of data.tasks) {
    if (!['READY', 'PENDING', 'RETRY_WAIT'].includes(task.status)) continue;
    const view = dependencyView(data, task),
      card = el('div', undefined, 'task-card');
    card.append(el('b', task.task_key), badge(task.status), el('p', describe(view)));
    const target = task.status === 'READY' ? 'ready-tasks' : 'waiting-tasks';
    $(target).append(card);
  }
  if (!$('ready-tasks').children.length) $('ready-tasks').append(el('p', t('dependency.noReady'), 'muted'));
  if (!$('waiting-tasks').children.length) $('waiting-tasks').append(el('p', t('dependency.noWaiting'), 'muted'));
  $('workers').replaceChildren();
  $('owner-summary').replaceChildren();
  for (const worker of data.workers) {
    const view = workerView(data, worker),
      box = el('div', undefined, 'worker');
    box.style.setProperty('--worker', color(worker.id));
    box.append(el('b', t('worker.heading', {
      worker: workerLabel(worker.id),
      heartbeat: t('heartbeat.' + view.heartbeat)
    })), el('code', worker.id), el('small', worker.worker_name));
    box.append(el('p', t('worker.capacity', {
      capacity: worker.max_concurrency,
      count: view.allocations.length
    }), 'capacity'));
    box.append(el('p', t('heartbeat.details', {
      time: utc(worker.last_heartbeat_at),
      deadline: utc(worker.heartbeat_expires_at),
      status: worker.status
    }), 'muted'));
    for (const attempt of view.allocations) {
      const card = el('div', undefined, 'allocation');
      card.append(el('b', t('attempt.label', {
        task: taskName(attempt),
        number: attempt.attempt_number
      })), badge(attempt.status));
      card.append(el('p', t('worker.claim', {
        time: utc(attempt.acquired_at)
      })));
      const observations = data.samples.filter(s => s.attempt_id === attempt.id);
      const evidence = values.filter(v => v.attempt === attempt.id);
      card.append(el('p', evidence.length ? evidence.map(v => t('observation.duration', {
        label: t(v.finished ? 'observation.finished' : 'observation.unfinished'),
        seconds: seconds(v.end - v.start).toFixed(2)
      })).join(t('common.evidenceSeparator')) : t('observation.noStart')));
      const lastReceipt = observations.map(s => s.recorded_at).sort().at(-1);
      if (lastReceipt) card.append(el('small', t('observation.lastReceipt', {
        time: utc(lastReceipt)
      })));
      card.append(el('p', t('lease.details', {
        label: t('lease.' + leaseView(data, attempt)),
        renewal: utc(attempt.last_renewed_at),
        deadline: utc(attempt.lease_expires_at)
      }), 'lease'));
      box.append(card);
    }
    if (!view.allocations.length) box.append(el('p', t(view.runTerminal ? 'worker.runTerminal' : 'worker.noAllocation'), 'muted'));
    const last = view.attempts.at(-1);
    if (last && !view.allocations.length) box.append(el('p', t('worker.retainedAttempt', {
      task: taskName(last),
      number: last.attempt_number,
      status: last.status,
      lease: t('lease.' + leaseView(data, last)),
      deadline: utc(last.lease_expires_at)
    }), 'muted'));
    $('workers').append(box);
    const summary = el('p', t('worker.summary', {
      worker: workerLabel(worker.id),
      attempts: view.attempts.map(a => t('attempt.summary', {
        task: taskName(a),
        number: a.attempt_number,
        status: a.status
      })).join(' · ') || t('worker.noClaims')
    }), 'owner-line');
    summary.style.setProperty('--worker', color(worker.id));
    $('owner-summary').append(summary);
  }
  if (!data.workers.length) $('workers').append(el('p', t('worker.noWorkers'), 'muted'));
  $('results').replaceChildren();
  for (const task of data.tasks.filter(t => t.status === 'SUCCEEDED')) {
    const next = data.run.definition.tasks.filter(t => t.depends_on.includes(task.task_key));
    $('results').append(el('p', t(next.length ? 'result.successWithChildren' : 'result.successLeaf', {
      task: task.task_key,
      children: next.map(task => task.task_id).join(t('common.listSeparator'))
    }), 'success-line'));
  }
  if (!$('results').children.length) $('results').append(el('p', t('result.none'), 'muted'));
  const branchRecovery = diamondRecoveryView(data);
  if (branchRecovery?.sibling) {
    $('results').append(el('p', t('result.diamondSiblingKept'), 'success-line'));
    if (branchRecovery.waitingStatus) $('results').append(el('p', t('result.diamondJoinWaiting', {
      status: branchRecovery.waitingStatus
    }), 'explanation'));
    if (branchRecovery.retry) $('results').append(el('p', t(branchRecovery.sameWorker ?
      'result.diamondSurvivorClaim' : 'result.diamondOtherClaim', {
      number: branchRecovery.retry.attempt_number,
      worker: workerLabel(branchRecovery.retry.worker_session_id),
      status: branchRecovery.retry.status
    }), 'explanation'));
  }
  $('recovery').replaceChildren();
  for (const attempt of data.attempts.filter(a => ['LOST', 'FAILED', 'TIMED_OUT'].includes(a.status))) {
    const view = recoveryView(data, attempt),
      box = el('div', undefined, 'recovery-card');
    box.append(el('h3', t('recovery.heading', {
      task: taskName(attempt),
      number: attempt.attempt_number,
      status: attempt.status
    })));
    const stages = el('div', undefined, 'recovery-stages');
    stages.append(el('p', t('recovery.oldOwner', {
      worker: workerLabel(attempt.worker_session_id),
      deadline: utc(attempt.lease_expires_at)
    })));
    stages.append(el('p', attempt.scheduled_at ? t('recovery.schedule', {
      recorded: utc(attempt.scheduled_at),
      eligible: utc(attempt.available_at)
    }) : t('recovery.noSchedule')));
    stages.append(el('p', view.next ? t('recovery.newClaim', {
      number: view.next.attempt_number,
      worker: workerLabel(view.next.worker_session_id),
      time: utc(view.next.acquired_at),
      status: view.next.status
    }) : t('recovery.noClaim')));
    box.append(stages);
    box.append(el('p', t('recovery.currentTask', {
      status: data.tasks.find(task => task.id === attempt.task_id).status
    }), 'muted'));
    $('recovery').append(box);
  }
  if (!$('recovery').children.length) $('recovery').append(el('p', t('recovery.none'), 'muted'));
  $('replacement-note').hidden = data.run.scenario !== 'recovery' ||
    (isDiamond(data.run.definition) && !diamond);
  $('replacement-note').textContent = t(diamond ? 'step5.diamondRecoveryNote' : 'step5.replacementNote');
  const successes = data.tasks.filter(t => t.status === 'SUCCEEDED').length;
  $('outcome').textContent = t('outcome.count', {
      status: data.run.status,
      succeeded: successes,
      total: data.tasks.length
    }) +
    t(data.run.status === 'SUCCEEDED' ? 'outcome.succeeded' : data.run.status === 'FAILED' ? 'outcome.failed' : 'outcome.running');
}

function render(data) {
  $('empty').hidden = true;
  $('content').hidden = false;
  $('scenario').textContent = scenarioText('title', data.run.scenario);
  $('run-id').textContent = t('run.id', {
    id: data.run.id
  });
  $('run-status').replaceWith(Object.assign(badge(data.run.status), {
    id: 'run-status'
  }));
  $('freshness').textContent = t('freshness.snapshot', {
    time: utc(data.snapshot_at)
  });
  const values = intervals(data.samples),
    owners = new Set(data.attempts.map(a => a.worker_session_id).filter(Boolean));
  $('overlap').textContent = peakOverlap(values);
  $('owners').textContent = owners.size;
  $('sample-count').textContent = data.samples.length;
  const index = id => data.workers.findIndex(w => w.id === id);
  const workerLabel = id => !id ? t('worker.unassigned') : index(id) < 0 ? t('worker.unregistered', {
    id: short(id)
  }) : t('worker.label', {
    number: index(id) + 1
  });
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
      const label = el('div', t('attempt.label', {
        task: task.task_key,
        number: attempt.attempt_number
      }));
      label.append(el('small', t('timeline.label', {
        worker: workerLabel(attempt.worker_session_id),
        seconds: seconds(value.end - value.start).toFixed(2),
        observation: t(value.finished ? 'timeline.finished' : 'timeline.unfinished')
      })));
      const track = el('div', undefined, 'track');
      const bar = el('div', undefined, 'bar' + (value.finished ? '' : ' unfinished'));
      bar.style.setProperty('--worker', color(attempt.worker_session_id));
      bar.style.left = (seconds(value.start - first) / span * 100) + '%';
      bar.style.width = (seconds(value.end - value.start) / span * 100) + '%';
      bar.title = t('timeline.samples', {
        id: short(value.id),
        count: value.count
      });
      track.append(bar);
      lane.append(label, track);
      $('timeline').append(lane);
    }
    const axis = el('div', undefined, 'axis');
    for (let i = 0; i <= 4; i++) axis.append(el('span', (span * i / 4).toFixed(1) + 's'));
    $('timeline').append(axis);
  }
  if (!values.length) $('timeline').append(el('p', t('timeline.empty'), 'muted'));
  $('attempts').replaceChildren();
  for (const attempt of data.attempts) {
    const task = data.tasks.find(t => t.id === attempt.task_id);
    const evidence = values.filter(v => v.attempt === attempt.id);
    const tr = el('tr');
    let td = el('td', t('attempt.compact', {
      task: task.task_key,
      number: attempt.attempt_number
    }));
    td.title = attempt.id;
    td.append(el('small', short(attempt.id)));
    tr.append(td);
    td = el('td', workerLabel(attempt.worker_session_id));
    td.append(el('small', short(attempt.worker_session_id)));
    tr.append(td);
    td = el('td');
    td.append(badge(attempt.status));
    tr.append(td, el('td', utc(attempt.acquired_at)));
    tr.append(el('td', evidence.map(v => t('attempt.sample', {
      seconds: seconds(v.end - v.start).toFixed(2),
      observation: t(v.finished ? 'attempt.finish' : 'attempt.noFinish')
    })).join(t('common.evidenceSeparator')) || t('attempt.noStart')));
    tr.append(el('td', utc(attempt.accepted_at)));
    td = el('td', attempt.scheduled_at ? t('attempt.recorded', {
      time: utc(attempt.scheduled_at)
    }) : '—');
    if (attempt.available_at) td.append(el('small', t('attempt.eligible', {
      time: utc(attempt.available_at)
    })));
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

function scenarioText(kind, scenario) {
  return ['parallel', 'distribution', 'recovery'].includes(scenario) ? t(`scenario.${kind}.${scenario}`) : scenario;
}

function translateStatic() {
  document.documentElement.lang = language.language;
  document.title = t('page.title');
  for (const node of document.querySelectorAll('[data-i18n]')) node.textContent = t(node.dataset.i18n);
  for (const node of document.querySelectorAll('[data-i18n-aria-label]')) node.setAttribute('aria-label', t(node.dataset.i18nAriaLabel));
  for (const button of document.querySelectorAll('[data-language]')) button.setAttribute('aria-pressed', String(button.dataset.language === language.language));
}

function renderRunOptions() {
  const signature = JSON.stringify([language.language, selected, latestRuns.map(r => [r.run_id, r.scenario, r.status])]);
  if ($('runs').dataset.signature === signature) return;
  const options = latestRuns.map(r => {
    const option = el('option', t('run.option', {
      scenario: scenarioText('name', r.scenario),
      id: short(r.run_id),
      status: r.status
    }));
    option.value = r.run_id;
    return option;
  });
  // The list is bounded; a pinned older Run can still be selected and queried.
  if (selected && !latestRuns.some(r => r.run_id === selected)) {
    const option = el('option', selected);
    option.value = selected;
    options.push(option);
  }
  if (!options.length) options.push(el('option', t('controls.noRuns')));
  $('runs').replaceChildren(...options);
  if (selected) $('runs').value = selected;
  $('runs').dataset.signature = signature;
}

function renderConnection() {
  $('error').hidden = !lastError;
  if (lastError) {
    let detail;
    if (lastError.status) detail = t('error.http', {
      status: lastError.status
    });
    else if (['TimeoutError', 'AbortError'].includes(lastError.name)) detail = t('error.timeout');
    else detail = t(lastError.name === 'TypeError' ? 'error.network' : 'error.unexpected');
    $('error').textContent = t('error.snapshot', {
      detail
    });
    $('freshness').textContent = t('freshness.disconnected');
  } else {
    $('freshness').textContent = latestSnapshot ? t('freshness.snapshot', {
      time: utc(latestSnapshot.snapshot_at)
    }) : t(hasListed ? 'freshness.empty' : 'freshness.connecting');
  }
}

function selectLanguage(next) {
  const y = window.scrollY;
  const anchor = y > 0 ? [...document.querySelectorAll('.step')].find(node => node.getBoundingClientRect().bottom > 130) : null;
  const offset = anchor?.getBoundingClientRect().top;
  if (!language.select(next)) return;
  translateStatic();
  renderRunOptions();
  if (latestSnapshot) render(latestSnapshot);
  renderConnection();
  // Keep existing details elements and the reader's place despite text reflow.
  const top = anchor ? window.scrollY + anchor.getBoundingClientRect().top - offset : y;
  window.scrollTo({
    top,
    behavior: 'instant'
  });
}

for (const button of document.querySelectorAll('[data-language]')) {
  button.addEventListener('click', () => selectLanguage(button.dataset.language));
}
translateStatic();

async function get(path) {
  const response = await fetch(path, {
    cache: 'no-store',
    signal: AbortSignal.timeout(5000)
  });
  if (!response.ok) throw Object.assign(new Error(`HTTP ${response.status}`), {
    status: response.status
  });
  return response.json();
}
async function poll() {
  try {
    latestRuns = await get('/demo/runs');
    hasListed = true;
    if ($('follow').checked && latestRuns.length) selected = latestRuns[0].run_id;
    renderRunOptions();
    if (selected) {
      const id = selected;
      const data = await get('/demo/runs/' + encodeURIComponent(id));
      if (id === selected) {
        latestSnapshot = data;
        render(data);
      }
    }
    lastError = null;
  } catch (error) {
    lastError = error;
  } finally {
    renderConnection();
    setTimeout(poll, 500);
  }
}
poll();
