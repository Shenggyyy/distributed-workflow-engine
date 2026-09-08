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
