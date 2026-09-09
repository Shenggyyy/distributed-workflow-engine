// Read-only projections of ONE server snapshot. These are not recorded events.
export function timestampNs(value) {
  if (!value) return null;
  const match = value.match(/^(.*T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/);
  if (!match) return null;
  const millis = Date.parse(match[1] + match[3]);
  return Number.isFinite(millis) ? BigInt(millis) * 1000000n + BigInt((match[2] || '').padEnd(9, '0')) : null;
}

export function isDue(deadline, snapshotAt) {
  const end = timestampNs(deadline), now = timestampNs(snapshotAt);
  return end === null || now === null ? null : end <= now;
}

export function latestAttempt(data, taskId) {
  return data.attempts.filter(a => a.task_id === taskId)
    .sort((a, b) => a.attempt_number - b.attempt_number).at(-1);
}

export function dependencyView(data, task) {
  const definition = data.run.definition.tasks.find(t => t.task_id === task.task_key);
  const dependencies = (definition?.depends_on || []).map(key => ({
    key, status: data.tasks.find(t => t.task_key === key)?.status || 'UNKNOWN'
  }));
  const blockers = dependencies.filter(d => d.status !== 'SUCCEEDED');
  const last = latestAttempt(data, task.id);
  let reason;
  if (task.status === 'READY') reason = blockers.length ? 'inconsistent' : 'claimable';
  else if (task.status === 'PENDING') reason = blockers.length ? 'dependencies' : 'scheduler';
  else if (task.status === 'RETRY_WAIT') reason = !last?.available_at ? 'retry_unknown' :
    isDue(last.available_at, data.snapshot_at) === true ? 'retry_due' : 'backoff';
  else reason = task.status.toLowerCase();
  return {task, dependencies, blockers, last, reason};
}

export function workerView(data, worker) {
  const attempts = data.attempts.filter(a => a.worker_session_id === worker.id);
  return {
    attempts,
    allocations: attempts.filter(a => a.status === 'RUNNING'),
    heartbeat: worker.status !== 'ACTIVE' ? worker.status.toLowerCase() :
      isDue(worker.heartbeat_expires_at, data.snapshot_at) === false ? 'fresh' : 'expired',
    runTerminal: ['SUCCEEDED', 'FAILED'].includes(data.run.status)
  };
}

export function leaseView(data, attempt) {
  if (!attempt.lease_expires_at) return 'unknown';
  if (attempt.status !== 'RUNNING') return 'historical';
  return isDue(attempt.lease_expires_at, data.snapshot_at) === true ? 'expired' : 'not_due';
}

export function recoveryView(data, attempt) {
  const next = data.attempts.filter(a => a.task_id === attempt.task_id &&
    a.attempt_number > attempt.attempt_number)
    .sort((a, b) => a.attempt_number - b.attempt_number)[0];
  return {attempt, next, due: isDue(attempt.available_at, data.snapshot_at)};
}

// Match the saved graph, never a current factory or a scenario label alone.
export function isDiamond(definition) {
  const tasks = definition?.tasks;
  if (!Array.isArray(tasks) || tasks.length !== 4) return false;
  const expected = {A: [], B: ['A'], C: ['A'], D: ['B', 'C']};
  if (new Set(tasks.map(task => task.task_id)).size !== 4) return false;
  return tasks.every(task => Object.hasOwn(expected, task.task_id) &&
    Array.isArray(task.depends_on) &&
    task.depends_on.length === expected[task.task_id].length &&
    new Set(task.depends_on).size === task.depends_on.length &&
    task.depends_on.every(parent => expected[task.task_id].includes(parent)));
}

export function timedDiamondScenario(definition, scenario) {
  if (!isDiamond(definition) || !['parallel', 'distribution', 'recovery'].includes(scenario)) return null;
  const types = {A: 'demo.diamond.a', B: 'demo.diamond.b',
    C: scenario === 'recovery' ? 'demo.diamond.recover' : 'demo.diamond.c', D: 'demo.diamond.d'};
  return definition.tasks.every(task => task.task_type === types[task.task_id]) ? scenario : null;
}

export function diamondRecoveryView(data) {
  if (data.run.scenario !== 'recovery' || !isDiamond(data.run.definition)) return null;
  const b = data.tasks.find(task => task.task_key === 'B');
  const c = data.tasks.find(task => task.task_key === 'C');
  const d = data.tasks.find(task => task.task_key === 'D');
  if (!b || !c || !d) return null;
  const attempts = data.attempts.filter(attempt => attempt.task_id === b.id);
  const sibling = b.status === 'SUCCEEDED' && attempts.length === 1 &&
    attempts[0].attempt_number === 1 && attempts[0].status === 'SUCCEEDED' ? attempts[0] : null;
  const latest = latestAttempt(data, c.id);
  const retry = latest?.attempt_number > 1 ? latest : null;
  return {sibling, retry,
    waitingStatus: sibling && d.status === 'PENDING' && c.status !== 'SUCCEEDED' ? c.status : null,
    sameWorker: Boolean(sibling && retry && sibling.worker_session_id === retry.worker_session_id)};
}

export function dagRows(definitions) {
  const levels = new Map();
  function level(key) {
    if (levels.has(key)) return levels.get(key);
    const definition = definitions.find(t => t.task_id === key);
    const value = definition.depends_on.length ? 1 + Math.max(...definition.depends_on.map(level)) : 0;
    levels.set(key, value);
    return value;
  }
  definitions.forEach(t => level(t.task_id));
  const rows = [];
  for (const definition of definitions) {
    const index = levels.get(definition.task_id);
    (rows[index] ||= []).push(definition.task_id);
  }
  return rows;
}
