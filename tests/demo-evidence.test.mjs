import assert from 'node:assert/strict';
import test from 'node:test';
import {intervals, peakOverlap} from '../src/workflow_engine/demo/static/evidence.js';
import {dependencyView, workerView, leaseView, recoveryView, dagRows, isDue,
  isDiamond, timedDiamondScenario, diamondRecoveryView} from '../src/workflow_engine/demo/static/flow.js';

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

const diamond = () => ({tasks: [
  {task_id:'D', depends_on:['C','B'], task_type:'demo.diamond.d'},
  {task_id:'C', depends_on:['A'], task_type:'demo.diamond.recover'},
  {task_id:'A', depends_on:[], task_type:'demo.diamond.a'},
  {task_id:'B', depends_on:['A'], task_type:'demo.diamond.b'}
]});

test('diamond recognition uses exact saved edges, independent of row order or scenario label', () => {
  const definition = diamond(), original = JSON.stringify(definition);
  assert.equal(isDiamond(definition), true);
  assert.equal(timedDiamondScenario(definition, 'recovery'), 'recovery');
  assert.equal(timedDiamondScenario(definition, 'distribution'), null);
  assert.deepEqual(dagRows(definition.tasks), [['A'], ['C','B'], ['D']]);
  assert.equal(JSON.stringify(definition), original);
  definition.tasks[0].depends_on = ['A'];
  assert.equal(isDiamond(definition), false);
  definition.tasks[0].depends_on = ['B','B'];
  assert.equal(isDiamond(definition), false);
  assert.equal(isDiamond(snapshot().run.definition), false, 'Legacy A→Join stays unchanged');
  assert.equal(isDiamond({tasks: [...diamond().tasks, {task_id:'Join', depends_on:['D']}]}), false);
  const duplicate = diamond();
  duplicate.tasks[0].task_id = 'C';
  assert.equal(isDiamond(duplicate), false);
  const custom = diamond();
  custom.tasks[0].task_type = 'custom.handler';
  assert.equal(isDiamond(custom), true);
  assert.equal(timedDiamondScenario(custom, 'recovery'), null, 'Shape alone cannot establish timed Handler durations');
});

test('diamond branch wait and retained sibling descriptions use actual task and attempt states', () => {
  const data = {run:{scenario:'recovery', definition:diamond()},
    tasks:[{id:'a',task_key:'A',status:'SUCCEEDED'}, {id:'b',task_key:'B',status:'RUNNING'},
      {id:'c',task_key:'C',status:'RETRY_WAIT'}, {id:'d',task_key:'D',status:'PENDING'}],
    attempts:[{id:'b1',task_id:'b',attempt_number:1,status:'RUNNING',worker_session_id:'survivor'},
      {id:'c1',task_id:'c',attempt_number:1,status:'LOST',worker_session_id:'lost'}]};
  assert.equal(diamondRecoveryView(data).sibling, null, 'Do not announce B success early');
  data.tasks[1].status = data.attempts[0].status = 'SUCCEEDED';
  let view = diamondRecoveryView(data);
  assert.equal(view.waitingStatus, 'RETRY_WAIT');
  assert.equal(view.retry, null, 'Do not invent the next claim or owner');
  data.attempts.push({id:'c2',task_id:'c',attempt_number:2,status:'RUNNING',worker_session_id:'survivor'});
  data.tasks[2].status = 'RUNNING';
  view = diamondRecoveryView(data);
  assert.equal(view.sameWorker, true);
  assert.equal(view.waitingStatus, 'RUNNING');
  data.attempts[2].worker_session_id = 'different-observed-owner';
  assert.equal(diamondRecoveryView(data).sameWorker, false, 'Do not relabel unexpected ownership');
  data.tasks[2].status = 'SUCCEEDED';
  assert.equal(diamondRecoveryView(data).waitingStatus, null, 'Successful C is no longer a blocker');
  data.attempts.push({...data.attempts[0], id:'b2', attempt_number:2});
  assert.equal(diamondRecoveryView(data).sibling, null, 'Never hide a second B Attempt');
  data.run.definition = snapshot().run.definition;
  assert.equal(diamondRecoveryView(data), null);
});
