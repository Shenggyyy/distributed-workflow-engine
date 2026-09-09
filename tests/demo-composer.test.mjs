import assert from 'node:assert/strict';
import test from 'node:test';
import {COMPOSER_STORAGE_KEY, createComposer} from '../src/workflow_engine/demo/static/composer.js';
import {createI18n, LANGUAGE_KEY} from '../src/workflow_engine/demo/static/i18n.js';

// Transport fixtures exercise only the editor state machine, not engine execution.
const policy = {max_attempts: 2, timeout_seconds: 90, initial_backoff_ms: 10000, max_backoff_ms: 10000};
const key = 'custom-test-operation-1';
const receipt = {
  run_id: '86fbc515-ee94-4c3b-a10c-5a5801527319',
  workflow_version_id: 'de33cd9d-479e-431d-93f7-80bf69b03f21',
  scenario: 'custom'
};
const rawDefinition = {
  name: 'Custom_preview',
  tasks: [
    {task_id: 'Publish_report', task_type: 'demo.join', depends_on: ['Read_rows']},
    {task_id: 'Read_rows', task_type: 'demo.observe'}
  ]
};
const draft = JSON.stringify(rawDefinition, null, 2);
const canonical = {
  name: rawDefinition.name,
  schema_version: 2,
  tasks: rawDefinition.tasks.map(task => ({...task, depends_on: task.depends_on || [], execution: {...policy}}))
};
const body = JSON.stringify(canonical);
const preview = () => ({definition: structuredClone(canonical), layers: [['Read_rows'], ['Publish_report']]});
const record = (extra = {}) => JSON.stringify({version: 1, key, body, ...extra});

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}

function memory(initial = null) {
  const values = new Map(initial === null ? [] : [[COMPOSER_STORAGE_KEY, initial]]);
  const writes = [];
  return {
    values, writes,
    getItem: key => values.get(key) ?? null,
    setItem(key, value) { values.set(key, value); writes.push({key, value}); },
    removeItem(key) { values.delete(key); writes.push({key, removed: true}); }
  };
}

function harness(options = {}) {
  const stored = options.stored || memory();
  const validations = [], submissions = [], changes = [];
  let keys = 0;
  const composer = createComposer({
    validate: text => {
      validations.push(text);
      return options.validate ? options.validate(text) : Promise.resolve(preview());
    },
    submit: (body, key) => {
      submissions.push({body, key});
      return options.submit ? options.submit(body, key) : Promise.resolve(structuredClone(receipt));
    },
    newKey: () => { keys++; return options.newKey ? options.newKey() : key; },
    storage: options.storage || (() => stored),
    onChange: state => { changes.push(state); options.onChange?.(state); }
  });
  return {composer, stored, validations, submissions, changes, get keys() { return keys; }};
}

async function validated(options = {}) {
  const h = harness(options);
  assert.equal(h.composer.edit(draft), true);
  assert.equal(await h.composer.validate(), true);
  assert.equal(h.composer.getState().phase, 'validated');
  return h;
}

test('template drafts and server validation never create a Run or allocate a key', async () => {
  const response = preview();
  const h = harness({validate: async () => response});
  assert.equal(h.composer.getState().phase, 'draft');
  assert.equal(h.composer.newDraft(draft), true);
  assert.equal(await h.composer.create(), false);
  assert.equal(await h.composer.validate(), true);
  assert.deepEqual(h.validations, [draft]);
  const state = h.composer.getState();
  assert.equal(state.text, draft);
  assert.deepEqual(state.preview.definition, canonical);
  assert.deepEqual(state.preview.layers, [['Read_rows'], ['Publish_report']]);
  response.definition.name = 'Changed_response';
  response.layers[0][0] = 'Changed_layer';
  assert.deepEqual(h.composer.getState().preview.definition, canonical);
  assert.deepEqual(h.composer.getState().preview.layers, [['Read_rows'], ['Publish_report']]);
  const language = createI18n({languages: ['en'], storage: () => h.stored});
  language.select('zh-CN');
  language.select('en');
  assert.deepEqual(h.composer.getState(), state);
  assert.deepEqual(h.submissions, []);
  assert.equal(h.keys, 0);
  assert.ok(h.stored.writes.every(write => write.key === LANGUAGE_KEY));
});

for (const operation of ['edit', 'newDraft']) {
  test(`${operation} invalidates a pending preview and ignores its late response`, async () => {
    const first = deferred(), second = deferred();
    let calls = 0;
    const h = harness({validate: () => (++calls === 1 ? first : second).promise});
    h.composer.edit(draft);
    const oldValidation = h.composer.validate();
    assert.equal(h.composer.getState().phase, 'validating');
    const oldRevision = h.composer.getState().revision;
    const edited = draft.replace('Custom_preview', 'Edited_workflow');
    assert.equal(h.composer[operation](edited), true);
    assert.ok(h.composer.getState().revision > oldRevision);
    assert.equal(h.composer.getState().preview, null);
    assert.equal(await h.composer.create(), false);
    const newValidation = h.composer.validate();
    const latest = preview();
    latest.definition.name = 'Edited_workflow';
    second.resolve(latest);
    assert.equal(await newValidation, true);
    first.resolve(preview());
    assert.equal(await oldValidation, false);
    assert.equal(h.composer.getState().preview.definition.name, 'Edited_workflow');
    assert.equal(h.composer.getState().text, edited);
    assert.deepEqual(h.submissions, []);
  });
}

test('a stale validation failure cannot erase a later successful preview', async () => {
  const stale = deferred();
  let calls = 0;
  const h = harness({validate: () => ++calls === 1 ? stale.promise : Promise.resolve(preview())});
  h.composer.edit(draft);
  const first = h.composer.validate();
  h.composer.edit(`${draft}\n`);
  await h.composer.validate();
  stale.reject(new Error('private transport message'));
  assert.equal(await first, false);
  assert.equal(h.composer.getState().phase, 'validated');
  assert.equal(h.composer.getState().error, null);
  assert.deepEqual(h.submissions, []);
});

test('explicit creation freezes server body and key before sending; duplicate clicks cannot send', async () => {
  const sending = deferred();
  const stored = memory();
  const h = await validated({stored, submit: (sentBody, sentKey) => {
    assert.deepEqual(JSON.parse(stored.getItem(COMPOSER_STORAGE_KEY)), {version: 1, key: sentKey, body: sentBody});
    return sending.promise;
  }});
  const creation = h.composer.create();
  assert.equal(h.composer.getState().phase, 'submitting');
  assert.deepEqual(h.composer.getState().pending, {body, key});
  assert.deepEqual(h.submissions, [{body, key}]);
  assert.equal(await h.composer.create(), false);
  assert.equal(await h.composer.retry(), false);
  assert.equal(h.composer.edit('{}'), false);
  assert.equal(h.composer.newDraft('{}'), false);
  assert.equal(await h.composer.validate(), false);
  assert.equal(h.keys, 1);
  const external = h.composer.getState();
  assert.throws(() => { external.pending.key = 'wrong-key'; }, TypeError);
  assert.throws(() => { external.preview.definition.tasks[0].task_id = 'Wrong'; }, TypeError);
  const result = structuredClone(receipt);
  sending.resolve(result);
  assert.equal(await creation, true);
  result.run_id = 'modified-response';
  assert.equal(h.composer.getState().phase, 'created');
  assert.deepEqual(h.composer.getState().receipt, receipt);
  assert.deepEqual(JSON.parse(stored.getItem(COMPOSER_STORAGE_KEY)), {version: 1, key, body, receipt});
  assert.equal(await h.composer.create(), false);
  assert.equal(await h.composer.retry(), false);
  assert.equal(h.submissions.length, 1);
});

test('unknown completion permits only explicit same-key byte-identical retry', async () => {
  const failed = deferred(), retried = deferred();
  let attempts = 0;
  const h = await validated({submit: () => (++attempts === 1 ? failed : retried).promise});
  const creation = h.composer.create();
  failed.reject(Object.assign(new Error('secret body and server stack'), {name: 'TypeError'}));
  assert.equal(await creation, false);
  const state = h.composer.getState();
  assert.equal(state.phase, 'unknown');
  assert.equal(state.error.reason, 'submission_unknown');
  assert.equal(h.composer.edit('{}'), false);
  assert.equal(h.composer.newDraft('{}'), false);
  assert.equal(await h.composer.create(), false);
  assert.equal(await h.composer.validate(), false);
  assert.equal(h.submissions.length, 1);
  const language = createI18n({languages: ['en'], storage: () => h.stored});
  language.select('zh-CN');
  assert.deepEqual(h.composer.getState(), state);
  const retry = h.composer.retry();
  assert.equal(await h.composer.retry(), false);
  assert.deepEqual(h.submissions, [{body, key}, {body, key}]);
  assert.equal(h.keys, 1);
  retried.resolve(structuredClone(receipt));
  assert.equal(await retry, true);
  assert.deepEqual(h.composer.getState().receipt, receipt);
});

test('reload restores pending identity or accepted receipt without automatically submitting', async () => {
  for (const accepted of [false, true]) {
    const stored = memory(record(accepted ? {receipt} : {}));
    const h = harness({stored});
    assert.equal(h.composer.getState().phase, accepted ? 'created' : 'unknown');
    assert.deepEqual(h.composer.getState().pending, {key, body});
    assert.deepEqual(h.composer.getState().receipt, accepted ? receipt : null);
    assert.deepEqual(h.submissions, []);
    assert.deepEqual(h.validations, []);
    assert.equal(h.keys, 0);
    assert.deepEqual(stored.writes, []);
    if (accepted) {
      assert.equal(await h.composer.retry(), false);
      assert.equal(h.composer.newDraft(draft), true);
      assert.equal(h.composer.getState().phase, 'draft');
      assert.equal(h.composer.getState().pending, null);
      assert.equal(h.composer.getState().receipt, null);
    } else {
      assert.equal(await h.composer.retry(), true);
      assert.deepEqual(h.submissions, [{key, body}]);
    }
  }
});

test('storage denial is explicit same-page fallback and retains retry identity', async () => {
  const denied = () => { throw new Error('denied'); };
  for (const storage of [denied, () => null, () => ({getItem: denied, setItem: denied}),
    () => ({getItem: () => null, setItem: denied})]) {
    let attempts = 0;
    const h = await validated({storage, submit: async () => {
      if (++attempts === 1) throw new TypeError('network unavailable');
      return receipt;
    }});
    assert.equal(await h.composer.create(), false);
    assert.equal(h.composer.getState().phase, 'unknown');
    assert.equal(h.composer.getState().storage, 'unavailable');
    assert.equal(await h.composer.retry(), true);
    assert.deepEqual(h.submissions, [{key, body}, {key, body}]);
    assert.equal(h.keys, 1);
  }
});

test('a receipt-storage failure preserves known success without allowing another send', async () => {
  const stored = memory();
  const write = stored.setItem;
  stored.setItem = (name, value) => {
    if (JSON.parse(value).receipt) throw new Error('storage quota reached');
    write(name, value);
  };
  const h = await validated({stored});
  assert.equal(await h.composer.create(), true);
  assert.equal(h.composer.getState().phase, 'created');
  assert.equal(h.composer.getState().storage, 'unavailable');
  assert.deepEqual(h.composer.getState().receipt, receipt);
  assert.equal(await h.composer.retry(), false);
  assert.deepEqual(h.submissions, [{key, body}]);
  // Reload knows only the persisted pending operation and offers explicit replay.
  const reloaded = harness({stored});
  assert.equal(reloaded.composer.getState().phase, 'unknown');
  assert.deepEqual(reloaded.submissions, []);
  assert.deepEqual(reloaded.composer.getState().pending, {key, body});
});

test('restoration preserves exact saved body bytes rather than reserializing identity', async () => {
  const savedBody = `${JSON.stringify(canonical, null, 2)}\n`;
  const h = harness({stored: memory(record({body: savedBody}))});
  assert.deepEqual(h.composer.getState().pending, {key, body: savedBody});
  assert.equal(await h.composer.retry(), true);
  assert.deepEqual(h.submissions, [{key, body: savedBody}]);
});

test('malformed restored data is preserved for recovery and blocks all submissions', async () => {
  const invalid = [
    'not-json', 'null', JSON.stringify({version: 2, key, body}), record({key: 'bad key'}),
    record({body: '{}'}), record({body: JSON.stringify(rawDefinition)}),
    record({receipt: {...receipt, scenario: 'parallel'}}),
    record({receipt: {...receipt, run_id: 'not-a-uuid'}})
  ];
  for (const original of invalid) {
    const stored = memory(original), h = harness({stored});
    assert.equal(h.composer.getState().phase, 'blocked');
    assert.equal(h.composer.getState().error.reason, 'storage_invalid');
    assert.equal(h.composer.getState().recovery, original);
    assert.equal(h.composer.edit(draft), false);
    assert.equal(h.composer.newDraft(draft), false);
    assert.equal(await h.composer.validate(), false);
    assert.equal(await h.composer.create(), false);
    assert.equal(await h.composer.retry(), false);
    assert.equal(stored.getItem(COMPOSER_STORAGE_KEY), original);
    assert.deepEqual(stored.writes, []);
    assert.deepEqual(h.submissions, []);
    assert.equal(h.keys, 0);
  }
});

test('a different stored operation is never overwritten or sent over by create or retry', async () => {
  const other = record({key: 'other-tab-operation'});
  const first = await validated();
  first.stored.values.set(COMPOSER_STORAGE_KEY, other);
  assert.equal(await first.composer.create(), false);
  assert.equal(first.composer.getState().error.reason, 'storage_conflict');
  assert.equal(first.stored.getItem(COMPOSER_STORAGE_KEY), other);
  assert.deepEqual(first.submissions, []);
  assert.deepEqual(first.stored.writes, []);

  const second = harness({stored: memory(record())});
  second.stored.values.set(COMPOSER_STORAGE_KEY, other);
  assert.equal(await second.composer.retry(), false);
  assert.equal(second.composer.getState().error.reason, 'storage_conflict');
  assert.equal(second.stored.getItem(COMPOSER_STORAGE_KEY), other);
  assert.deepEqual(second.submissions, []);
  assert.deepEqual(second.stored.writes, []);
});

test('invalid keys leave a usable preview without persistence or submission', async () => {
  for (const newKey of [() => '', () => 'contains spaces', () => 'x'.repeat(129), () => null,
    () => { throw new Error('random source unavailable'); }]) {
    const h = await validated({newKey});
    assert.equal(await h.composer.create(), false);
    assert.equal(h.composer.getState().phase, 'validated');
    assert.equal(h.composer.getState().error.reason, 'key_invalid');
    assert.deepEqual(h.composer.getState().preview.definition, canonical);
    assert.deepEqual(h.submissions, []);
    assert.deepEqual(h.stored.writes, []);
  }
});

test('malformed validation responses cannot become a publishable preview', async () => {
  const cases = [
    null, {definition: rawDefinition, layers: [['Read_rows'], ['Publish_report']]},
    {definition: canonical, layers: [['Read_rows']]},
    {definition: canonical, layers: [['Publish_report'], ['Read_rows']]},
    {definition: canonical, layers: [['Read_rows'], ['Publish_report', 'Read_rows']]},
    {definition: {...canonical, name: 'x'.repeat(17000)}, layers: [['Read_rows'], ['Publish_report']]}
  ];
  for (const response of cases) {
    const h = harness({validate: async () => response});
    h.composer.edit(draft);
    assert.equal(await h.composer.validate(), false);
    assert.equal(h.composer.getState().phase, 'draft');
    assert.equal(h.composer.getState().preview, null);
    assert.equal(h.composer.getState().error.reason, 'invalid_preview');
    assert.equal(await h.composer.create(), false);
    assert.deepEqual(h.submissions, []);
    assert.equal(h.keys, 0);
  }
});

test('malformed creation receipts remain unknown and retry the saved operation', async () => {
  for (const bad of [null, {}, {...receipt, scenario: 'parallel'}, {...receipt, run_id: 'invalid'},
    {...receipt, workflow_version_id: null}]) {
    let attempts = 0;
    const h = await validated({submit: async () => ++attempts === 1 ? bad : receipt});
    assert.equal(await h.composer.create(), false);
    assert.equal(h.composer.getState().phase, 'unknown');
    assert.equal(h.composer.getState().receipt, null);
    assert.equal(h.composer.getState().error.reason, 'invalid_receipt');
    assert.deepEqual(h.composer.getState().pending, {key, body});
    assert.equal(await h.composer.retry(), true);
    assert.deepEqual(h.submissions, [{key, body}, {key, body}]);
  }
});

test('safe errors retain useful transport fields without private response values', async () => {
  const error = Object.assign(new Error('private-message-sentinel'), {
    name: 'HTTPError', code: 'invalid_request', status: 422,
    details: [{location: ['body', 'tasks', 1, 'task_type'], type: 'demo_handler_not_allowed',
      input: 'private-input-sentinel', message: 'private-detail-sentinel'}],
    response: {secret: 'private-response-sentinel'}
  });
  const h = harness({validate: async () => { throw error; }});
  h.composer.edit(draft);
  assert.equal(await h.composer.validate(), false);
  const state = h.composer.getState();
  assert.equal(state.error.reason, 'validation_failed');
  assert.equal(state.error.code, 'invalid_request');
  assert.equal(state.error.status, 422);
  assert.equal(state.error.name, 'HTTPError');
  assert.deepEqual(state.error.details, [{location: ['body', 'tasks', 1, 'task_type'], type: 'demo_handler_not_allowed'}]);
  assert.doesNotMatch(JSON.stringify(state), /private-.*-sentinel/);
  assert.equal(h.composer.edit(`${draft}\n`), true);
  assert.equal(h.composer.getState().error, null);
  assert.deepEqual(h.submissions, []);
});

test('a failing state listener cannot turn an accepted operation into a second submission', async () => {
  const h = await validated({onChange: () => { throw new Error('render failure'); }});
  assert.equal(await h.composer.create(), true);
  assert.equal(h.composer.getState().phase, 'created');
  assert.deepEqual(h.composer.getState().receipt, receipt);
  assert.equal(await h.composer.create(), false);
  assert.deepEqual(h.submissions, [{key, body}]);
});
