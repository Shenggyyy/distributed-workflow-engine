import assert from 'node:assert/strict';
import test from 'node:test';
import {intervals, peakOverlap} from '../src/workflow_engine/demo/static/evidence.js';

const interval = (start, end, domain = 'kernel') => ({start: BigInt(start), end: BigInt(end), domain});

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
