import assert from 'node:assert/strict';
import test from 'node:test';
import {intervals, peakOverlap} from '../src/workflow_engine/demo/static/evidence.js';
import {dependencyView, workerView, leaseView, recoveryView, dagRows, isDue} from '../src/workflow_engine/demo/static/flow.js';

const interval = (start, end, domain = 'kernel') => ({start: BigInt(start), end: BigInt(end), domain});

const snapshot = () => ({
  snapshot_at: '2026-09-09T00:00:10.000001Z',
  run: {status:'RUNNING', definition:{tasks:[
    {task_id:'A', depends_on:[]}, {task_id:'Join', depends_on:['A']}
  ]}},
  tasks:[{id:'a', task_key:'A', status:'RUNNING'}, {id:'join', task_key:'Join', status:'PENDING'}],
  attempts:[], samples:[]
});

test('dependency satisfaction is not an invented READY transition', () => {
  const data = snapshot();
  assert.equal(dependencyView(data, data.tasks[1]).reason, 'dependencies');
  data.tasks[0].status = 'SUCCEEDED';
  assert.equal(dependencyView(data, data.tasks[1]).reason, 'scheduler');
  assert.equal(data.tasks[1].status, 'PENDING');
  data.tasks[1].status = 'READY';
  assert.equal(dependencyView(data, data.tasks[1]).reason, 'claimable');
  data.tasks[0].status = 'FAILED';
  assert.equal(dependencyView(data, data.tasks[1]).reason, 'inconsistent');
});

test('database deadlines preserve sub-millisecond boundaries and do not mutate attempts', () => {
  const data = snapshot();
  assert.equal(isDue('2026-09-09T00:00:10.000002+00:00', data.snapshot_at), false);
  assert.equal(isDue(data.snapshot_at, data.snapshot_at), true);
  assert.equal(isDue(null, data.snapshot_at), null);
  const attempt = {status:'RUNNING', lease_expires_at:'2026-09-09T00:00:09Z'};
  assert.equal(leaseView(data, attempt), 'expired');
  assert.equal(attempt.status, 'RUNNING');
  attempt.status = 'SUCCEEDED';
  assert.equal(leaseView(data, attempt), 'historical');
});

test('retry retains old evidence and uses the next actual attempt without assigning a worker', () => {
  const data = snapshot();
  const old = {id:'old', task_id:'a', status:'LOST', attempt_number:1, available_at:'2026-09-09T00:00:11Z'};
  data.attempts.push(old);
  data.tasks[0].status = 'RETRY_WAIT';
  assert.equal(dependencyView(data, data.tasks[0]).reason, 'backoff');
  assert.equal(recoveryView(data, old).next, undefined);
  data.snapshot_at = '2026-09-09T00:00:12Z';
  assert.equal(dependencyView(data, data.tasks[0]).reason, 'retry_due');
  const next = {id:'new', task_id:'a', status:'RUNNING', attempt_number:2, worker_session_id:'arbitrary-owner'};
  data.attempts.unshift(next);
  assert.equal(recoveryView(data, old).next, next);
});

test('expired heartbeat after success is not an execution failure or running handler', () => {
  const data = snapshot();
  data.run.status = 'SUCCEEDED';
  const worker = {id:'w', status:'LOST', heartbeat_expires_at:'2026-09-09T00:00:05Z'};
  data.attempts.push({worker_session_id:'w', status:'SUCCEEDED'});
  const view = workerView(data, worker);
  assert.equal(view.heartbeat, 'lost');
  assert.equal(view.runTerminal, true);
  assert.equal(view.allocations.length, 0);
  assert.equal(view.attempts[0].status, 'SUCCEEDED');
});

test('vertical DAG layers follow real dependencies even with unsorted definitions', () => {
  const rows = dagRows([
    {task_id:'Join', depends_on:['A','B']}, {task_id:'B', depends_on:['Read']},
    {task_id:'Read', depends_on:[]}, {task_id:'A', depends_on:['Read']}
  ]);
  assert.deepEqual(rows, [['Read'], ['B','A'], ['Join']]);
});

test('overlap needs positive intersection in the same clock domain', () => {
  assert.equal(peakOverlap([interval(1, 8), interval(3, 6)]), 2);
  assert.equal(peakOverlap([interval(1, 3), interval(3, 6)]), 1);
  assert.equal(peakOverlap([interval(1, 8), interval(3, 6, 'other')]), 1);
  assert.equal(peakOverlap([interval(1, 1)]), 0);
});

test('missing FINISH ends only at the last real sample with nanosecond precision', () => {
  const samples = [
    {invocation_id:'one', attempt_id:'attempt', clock_domain:'kernel', sequence:1,
      phase:'PULSE', monotonic_ns:'123456789012345679'},
    {invocation_id:'one', attempt_id:'attempt', clock_domain:'kernel', sequence:0,
      phase:'START', monotonic_ns:'123456789012345678'},
  ];
  const [value] = intervals(samples);
  assert.equal(value.end - value.start, 1n);
  assert.equal(value.finished, false);
  assert.deepEqual(intervals(samples), intervals(samples));
  samples.push({...samples[0], sequence:2, phase:'FINISH', monotonic_ns:'123456789012345680'});
  assert.equal(intervals(samples)[0].finished, true);
});

test('duplicate invocations of an Attempt remain separate evidence', () => {
  const samples = ['one','two'].flatMap(id => [
    {invocation_id:id, attempt_id:'same', clock_domain:'kernel', sequence:0,
      phase:'START', monotonic_ns:'10'},
    {invocation_id:id, attempt_id:'same', clock_domain:'kernel', sequence:1,
      phase:'FINISH', monotonic_ns:'20'},
  ]);
  assert.equal(intervals(samples).length, 2);
  assert.equal(peakOverlap(intervals(samples)), 2);
});
