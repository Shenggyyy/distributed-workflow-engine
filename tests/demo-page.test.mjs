import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import * as flow from '../src/workflow_engine/demo/static/flow.js';
import * as dag from '../src/workflow_engine/demo/static/dag.js';
import * as evidence from '../src/workflow_engine/demo/static/evidence.js';
import * as i18n from '../src/workflow_engine/demo/static/i18n.js';
import * as composerView from '../src/workflow_engine/demo/static/composer-view.js';
import {COMPOSER_STORAGE_KEY} from '../src/workflow_engine/demo/static/composer.js';
import {messages} from '../src/workflow_engine/demo/static/messages.js';

const html = readFileSync(new URL('../src/workflow_engine/demo/static/index.html', import.meta.url), 'utf8');
const source = readFileSync(new URL('../src/workflow_engine/demo/static/app.js', import.meta.url), 'utf8')
  .replace(/^import\s+\{[^}]+\}\s+from\s+['"][^'"]+['"];\s*/gm, '');

// This small DOM adapter exercises controller effects, not browser layout. Real
// layout, scrolling and visual acceptance are checked separately in a browser.
class Node {
  constructor(tagName) {
    this.tagName = tagName.toUpperCase();
    this.children = [];
    this.attributes = {};
    this.dataset = {};
    this.listeners = {};
    this.style = {setProperty(name, value) { this[name] = value; }};
    this.ownText = '';
    this.hidden = false;
    this.open = false;
    this.checked = false;
    this.disabled = false;
    this.readOnly = false;
    this.scrollTop = 0;
    this.selectionDirection = 'none';
    this.value = '';
  }
  set value(value) {
    this.currentValue = String(value);
    this.selectionStart = this.selectionEnd = this.currentValue.length;
  }
  get value() { return this.currentValue; }
  set textContent(value) { this.ownText = String(value); this.children = []; }
  get textContent() { return this.ownText + this.children.map(n => n.textContent).join(''); }
  setAttribute(name, value) {
    this.attributes[name] = String(value);
    if (name === 'id') this.id = value;
    if (name === 'class') this.className = value;
    if (['hidden', 'checked', 'open'].includes(name)) this[name] = true;
    if (name.startsWith('data-')) {
      const key = name.slice(5).replace(/-([a-z])/g, (_, letter) => letter.toUpperCase());
      this.dataset[key] = String(value);
    }
  }
  getAttribute(name) { return this.attributes[name] ?? null; }
  append(...nodes) {
    for (const node of nodes) { node.parentNode = this; this.children.push(node); }
  }
  replaceChildren(...nodes) { this.ownText = ''; this.children = []; this.append(...nodes); }
  replaceWith(node) {
    const index = this.parentNode.children.indexOf(this);
    this.parentNode.children[index] = node;
    node.parentNode = this.parentNode;
  }
  addEventListener(event, callback) { (this.listeners[event] ||= []).push(callback); }
  focus() { this.ownerDocument.activeElement = this; }
  setSelectionRange(start, end, direction = 'none') {
    this.selectionStart = start; this.selectionEnd = end; this.selectionDirection = direction;
  }
  dispatch(event) {
    for (const callback of this.listeners[event] || []) callback({target: this, currentTarget: this});
  }
  getBoundingClientRect() { return {top: 100, bottom: 500, height: 400}; }
}

function descendants(node) { return [node, ...node.children.flatMap(descendants)]; }

function matches(node, selector) {
  if (selector.startsWith('[')) return node.getAttribute(selector.slice(1, -1)) !== null;
  if (selector.startsWith('.')) return (node.className || '').split(' ').includes(selector.slice(1));
  return node.tagName === selector.toUpperCase();
}

function documentFixture() {
  const root = new Node('document');
  const stack = [root];
  const voidTags = new Set(['META', 'LINK', 'INPUT', 'BR', 'HR', 'IMG']);
  for (const token of html.match(/<[^>]+>|[^<]+/g)) {
    if (token.startsWith('<!')) continue;
    if (token.startsWith('</')) { stack.pop(); continue; }
    if (token.startsWith('<')) {
      const node = new Node(token.match(/^<([\w-]+)/)[1]);
      const attrs = token.slice(token.indexOf(' '), -1);
      if (token.includes(' ')) {
        for (const [, key, quoted, plain] of attrs.matchAll(/([\w-]+)(?:="([^"]*)"|=([^\s>]+))?/g)) {
          node.setAttribute(key, quoted ?? plain ?? '');
        }
      }
      stack.at(-1).append(node);
      if (!voidTags.has(node.tagName)) stack.push(node);
    } else {
      stack.at(-1).ownText += token.replaceAll('&amp;', '&');
    }
  }
  const document = {
    root,
    getElementById(id) { return descendants(root).find(node => node.id === id); },
    createElement(tag) { return Object.assign(new Node(tag), {ownerDocument: this}); },
    createElementNS(_namespace, tag) { return this.createElement(tag); },
    querySelectorAll(selector) { return descendants(root).filter(node => matches(node, selector)); },
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; },
    get documentElement() { return this.querySelector('html'); },
    get body() { return this.querySelector('body'); },
  };
  for (const node of descendants(root)) node.ownerDocument = document;
  return document;
}

const at = seconds => `2026-09-09T10:00:${String(seconds).padStart(2, '0')}.000001Z`;
function runningSnapshot() {
  const tasks = ['原始任务-A', 'B', 'Join'].map((key, index) => ({
    id: `task-${index}`, task_key: key, status: index < 2 ? 'RUNNING' : 'PENDING',
  }));
  const attempts = tasks.slice(0, 2).map((task, index) => ({
    id: `attempt-${index}`, task_id: task.id, attempt_number: 1, status: 'RUNNING',
    worker_session_id: 'worker-unchanged-id', acquired_at: at(1), last_renewed_at: at(5),
    lease_expires_at: at(11), accepted_at: null, scheduled_at: null, available_at: null,
  }));
  return {
    snapshot_at: at(6),
    run: {id: 'run-unchanged-id', status: 'RUNNING', scenario: 'parallel', definition: {
      tasks: tasks.map((task, index) => ({task_id: task.task_key,
        depends_on: index === 2 ? tasks.slice(0, 2).map(t => t.task_key) : []})),
    }},
    tasks, attempts,
    workers: [{id: 'worker-unchanged-id', worker_name: 'demo-original-name',
      max_concurrency: 2, status: 'ACTIVE', last_heartbeat_at: at(5), heartbeat_expires_at: at(11)}],
    samples: attempts.flatMap((attempt, index) => ['START', 'PULSE'].map((phase, sequence) => ({
      invocation_id: `invocation-${index}`, attempt_id: attempt.id, sequence, phase,
      clock_domain: 'raw-boot-id:offset-0', recorded_at: at(2 + sequence),
      monotonic_ns: String(1000000000n + BigInt(index) * 100000000n + BigInt(sequence) * 2000000000n),
    }))),
  };
}

function diamondSnapshot({scenario = 'recovery', retried = false} = {}) {
  const recovery = scenario === 'recovery';
  const tasks = ['A','B','C','D'].map((key, index) => ({id:`diamond-task-${key}`,
    task_key:key, status:index === 0 || (index === 1 && recovery) ? 'SUCCEEDED' :
      index === 3 ? 'PENDING' : index === 2 && recovery && !retried ? 'RETRY_WAIT' : 'RUNNING'}));
  const attempts = tasks.slice(0, 3).map((task, index) => ({
    id:`diamond-attempt-${task.task_key}`, task_id:task.id, attempt_number:1,
    status:index === 2 && recovery ? 'LOST' : task.status,
    worker_session_id:index === 1 ? 'survivor-worker' : 'first-worker',
    acquired_at:at(index ? 8 : 1), last_renewed_at:at(index ? 10 : 5), lease_expires_at:at(index ? 16 : 11),
    accepted_at:task.status === 'SUCCEEDED' ? at(index ? 14 : 7) : null,
    scheduled_at:index === 2 && recovery ? at(16) : null,
    available_at:index === 2 && recovery ? at(20) : null,
  }));
  if (retried) attempts.push({...attempts[2], id:'diamond-attempt-C2', attempt_number:2,
    status:'RUNNING', worker_session_id:'survivor-worker', acquired_at:at(20),
    last_renewed_at:at(21), lease_expires_at:at(27), scheduled_at:null, available_at:null});
  return {
    snapshot_at:at(retried ? 22 : 18),
    run:{id:'diamond-run', scenario, status:'RUNNING', definition:{schema_version:2,
      tasks:tasks.map((task, index) => ({task_id:task.task_key,
        task_type:index === 2 && recovery ? 'demo.diamond.recover' : 'demo.diamond.' + task.task_key.toLowerCase(),
        depends_on:index === 0 ? [] : index === 3 ? ['B','C'] : ['A']}))}},
    tasks, attempts,
    workers:['first-worker','survivor-worker'].map((id, index) => ({id, worker_name:'raw-worker-' + index,
      max_concurrency:1, status:recovery && index === 0 ? 'LOST' : 'ACTIVE',
      last_heartbeat_at:at(17), heartbeat_expires_at:at(23)})),
    samples:attempts.flatMap((attempt, index) => ['START', attempt.status === 'SUCCEEDED' ? 'FINISH' : 'PULSE'].map((phase, sequence) => ({
      invocation_id:'diamond-invocation-' + index, attempt_id:attempt.id, clock_domain:'original-kernel',
      phase, sequence, recorded_at:at(index > 2 ? 21 : index ? 12 : 6),
      monotonic_ns:String(BigInt(index > 2 ? 20 : index ? 8 : 1) * 1000000000n + BigInt(sequence) * 1000000000n),
    }))),
  };
}

function customSnapshot({serial = false} = {}) {
  const longName = 'Fetch_' + 'x'.repeat(58);
  const definitions = serial ? Array.from({length: 12}, (_, index) => ({
    task_id: index ? `Stage_${index}` : longName,
    task_type: 'demo.observe',
    depends_on: index ? [index === 1 ? longName : `Stage_${index - 1}`] : [],
  })) : [
    {task_id: 'Publish', task_type: 'demo.join', depends_on: ['Validate', 'Summarize']},
    {task_id: 'Summarize', task_type: 'demo.observe', depends_on: [longName]},
    {task_id: longName, task_type: 'demo.observe', depends_on: []},
    {task_id: 'Validate', task_type: 'demo.observe', depends_on: [longName, 'Fetch_aux']},
    {task_id: 'Fetch_aux', task_type: 'demo.observe', depends_on: []},
  ];
  return {
    snapshot_at: at(6),
    run: {id: 'b15e128f-9c67-426c-9034-11ea915d3654', scenario: 'custom', status: 'RUNNING',
      definition: {schema_version: 2, name: 'Custom_raw_name', tasks: definitions}},
    tasks: definitions.map((definition, index) => ({id: `custom-task-${index}`,
      task_key: definition.task_id, status: definition.depends_on.length ? 'PENDING' : 'READY'})),
    attempts: [], workers: [], samples: [],
  };
}

const customPolicy = {max_attempts: 2, timeout_seconds: 90, initial_backoff_ms: 10000, max_backoff_ms: 10000};
const customReceipt = {run_id: 'dc5741ce-6432-4fea-ac89-6439fe0b03d9',
  workflow_version_id: 'bf40bb47-ae6a-4f1a-8e3b-90b8b138d98b', scenario: 'custom'};
const customKey = 'submitted-from-page-test';
function customDefinition() {
  return {name: 'Page_custom_workflow', schema_version: 2, tasks: [
    {task_id: 'Publish_report', task_type: 'demo.join', depends_on: ['Read_rows'], execution: {...customPolicy}},
    {task_id: 'Read_rows', task_type: 'demo.observe', depends_on: [], execution: {...customPolicy}}
  ]};
}
function customCatalog() {
  return {
    handlers: [['demo.observe',8], ['demo.recover',20], ['demo.join',2], ['demo.diamond.a',6],
      ['demo.diamond.b',8], ['demo.diamond.c',14], ['demo.diamond.recover',20], ['demo.diamond.d',3]]
      .map(([task_type, seconds]) => ({task_type, seconds})),
    limits: {max_tasks: 12, max_body_bytes: 16384, max_attempts: 2, timeout_seconds: 90,
      worker_count: 2, slots_per_worker: 1},
    templates: ['parallel', 'distribution', 'recovery'].map(scenario => ({scenario,
      definition: {name: `demo_custom_template_${scenario}`, schema_version: 2,
        tasks: diamondSnapshot({scenario}).run.definition.tasks.map(task => ({...task, execution: {...customPolicy}}))}}))
  };
}
const response = (payload, status = 200) => ({ok: status >= 200 && status < 300, status,
  json: async () => structuredClone(payload)});
const settle = () => new Promise(resolve => setImmediate(resolve));
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}

async function controller({snapshot = runningSnapshot(), selected = true, historySnapshots = [], port = '18080',
  requestHandler, storedEntries = [], storageDenied = false} = {}) {
  const document = documentFixture();
  const requests = [], timers = [], historyCalls = [], stored = new Map(storedEntries);
  let failure = null;
  const window = {scrollY: 1234, scrollX: 0, innerHeight: 800,
    localStorage: {getItem(key) { if (storageDenied) throw new Error('Storage denied'); return stored.get(key) ?? null; },
      setItem(key, value) { if (storageDenied) throw new Error('Storage denied'); stored.set(key, value); },
      removeItem(key) { stored.delete(key); }},
    scrollTo(x, y) { this.scrollY = typeof x === 'object' ? x.top : y; }};
  const context = vm.createContext({
    ...flow, ...dag, ...evidence, ...i18n, ...composerView, document, window,
    navigator: {languages: ['en-GB'], language: 'en-GB'},
    location: {search: selected && snapshot ? '?run=' + snapshot.run.id : '', port, protocol: 'http:'},
    history: {replaceState(...args) { historyCalls.push(args); }},
    URLSearchParams, AbortSignal, TypeError, Error, crypto: {randomUUID: () => customKey},
    setTimeout(callback) { timers.push(callback); },
    fetch: async (path, options = {}) => {
      requests.push({path, options});
      if (failure) throw failure;
      if (requestHandler) {
        const handled = requestHandler(path, options);
        if (handled !== undefined) return handled;
      }
      if (path === '/demo/custom/catalog') return response(customCatalog());
      const snapshots = [...(snapshot ? [snapshot] : []), ...historySnapshots];
      const payload = path === '/demo/runs' ? snapshots.map(data => ({run_id:data.run.id,
        scenario:data.run.scenario, status:data.run.status})) :
        snapshots.find(data => path === '/demo/runs/' + data.run.id);
      return response(payload);
    },
  });
  vm.runInContext(source, context, {filename: 'app.js'});
  await settle();
  return {
    document, requests, timers, window, stored, historyCalls, settle,
    node(id) { return document.getElementById(id); },
    switchTo(language) {
      const button = document.querySelectorAll('[data-language]').find(node => node.dataset.language === language);
      assert.ok(button, `Language button ${language} exists`);
      button.dispatch('click');
    },
    async nextPoll(error = null) {
      failure = error;
      assert.ok(timers.length, 'Polling continues on its existing schedule');
      timers.shift()();
      await settle();
    },
  };
}

const bars = page => descendants(page.node('timeline')).filter(node => (node.className || '').split(' ').includes('bar'))
  .map(node => ({left: node.style.left, width: node.style.width, color: node.style['--worker']}));

test('static text and accessibility keys resolve in both catalogs without scattered prose', () => {
  const document = documentFixture();
  for (const attr of ['data-i18n', 'data-i18n-aria-label']) {
    const nodes = document.querySelectorAll(`[${attr}]`);
    assert.ok(nodes.length > 0);
    for (const node of nodes) {
      const key = node.getAttribute(attr);
      for (const language of i18n.LANGUAGES) assert.ok(Object.hasOwn(messages[language], key), `${language}: ${key}`);
    }
  }
  assert.equal(document.querySelectorAll('.step').length, 6);
  for (const node of descendants(document.root)) {
    const text = node.ownText.trim();
    if (!text || node.tagName === 'CODE' || /^\d+$/.test(text) ||
      (text === '/' && node.getAttribute('aria-hidden') === 'true')) continue;
    assert.ok(node.getAttribute('data-i18n'), `Static text must have a catalog key: ${text}`);
  }
});

for (const selected of [true, false]) {
  test(`switch during RUNNING preserves evidence and ${selected ? 'chosen Run' : 'follow mode'} without requests`, async () => {
    const snapshot = runningSnapshot(), original = JSON.stringify(snapshot);
    const page = await controller({snapshot, selected});
    assert.equal(page.node('error').hidden, true);
    assert.equal(page.node('run-status').textContent, 'RUNNING');
    assert.equal(page.node('overlap').textContent, '2');
    page.node('attempt-details').open = false;
    const definitionDetails = page.node('definition').parentNode;
    definitionDetails.open = true;
    const before = {
      requests: page.requests.length, history: page.historyCalls.length,
      run: page.node('runs').value, follow: page.node('follow').checked,
      raw: page.node('identities').textContent, definition: page.node('definition').textContent,
      geometry: bars(page), scroll: page.window.scrollY,
    };
    page.switchTo('zh-CN');
    assert.equal(page.document.documentElement.lang, 'zh-CN');
    assert.match(page.document.title, /看懂一次分布式执行/);
    assert.equal(page.node('dag').getAttribute('aria-label'), '从上到下的真实工作流 DAG');
    assert.equal(page.document.querySelectorAll('[data-language]').find(node => node.dataset.language === 'zh-CN').getAttribute('aria-pressed'), 'true');
    assert.match(page.node('waiting-tasks').textContent, /等待：原始任务-A \(RUNNING\)\、B \(RUNNING\)/);
    assert.match(page.node('workers').textContent, /2 个配置槽位/);
    assert.match(page.node('attempts').textContent, /没有 FINISH；结束时间未知/);
    assert.equal(page.node('attempt-details').open, false);
    assert.equal(definitionDetails.open, true);
    assert.equal(page.node('runs').value, before.run);
    assert.equal(page.node('follow').checked, before.follow);
    assert.equal(page.node('identities').textContent, before.raw);
    assert.equal(page.node('definition').textContent, before.definition);
    assert.deepEqual(bars(page), before.geometry);
    assert.equal(page.node('run-status').textContent, 'RUNNING');
    assert.equal(page.node('raw').href, '/demo/runs/run-unchanged-id');
    assert.match(page.node('attempts').textContent, /10:00:01\.000Z/);
    page.switchTo('en');
    assert.equal(page.document.documentElement.lang, 'en');
    assert.match(page.document.title, /Understand distributed execution/);
    assert.equal(page.node('dag').getAttribute('aria-label'), 'Actual workflow DAG, arranged from top to bottom');
    assert.match(page.node('waiting-tasks').textContent, /Waiting for: 原始任务-A \(RUNNING\), B \(RUNNING\)/);
    assert.match(page.node('workers').textContent, /2 configured slots/);
    assert.match(page.node('attempts').textContent, /no FINISH; end unknown/);
    assert.equal(page.requests.length, before.requests);
    assert.equal(page.historyCalls.length, before.history);
    assert.equal(page.window.scrollY, before.scroll);
    assert.equal(page.timers.length, 1, 'Switching does not add a polling loop');
    assert.ok(page.requests.every(request => !request.options.method || request.options.method === 'GET'));
    assert.equal(JSON.stringify(snapshot), original, 'Rendering never mutates engine data');
  });
}

test('a cached connection error translates without clearing evidence or restarting polling', async () => {
  const page = await controller();
  const raw = page.node('identities').textContent;
  await page.nextPoll(new TypeError('fetch failed'));
  assert.equal(page.node('error').hidden, false);
  assert.match(page.node('error').textContent, /network request failed/);
  assert.match(page.node('freshness').textContent, /Disconnected/);
  const requests = page.requests.length;
  page.switchTo('zh-CN');
  assert.match(page.node('error').textContent, /网络请求失败/);
  assert.match(page.node('freshness').textContent, /数据冻结/);
  assert.equal(page.node('identities').textContent, raw);
  assert.equal(page.requests.length, requests);
  await page.nextPoll();
  assert.equal(page.node('error').hidden, true);
  assert.match(page.node('freshness').textContent, /数据库快照/);
});

test('empty Runs and pending Handler evidence translate without inventing state', async () => {
  const page = await controller({snapshot: null, selected: false});
  assert.equal(page.node('empty').hidden, false);
  assert.equal(page.node('content').hidden, true);
  assert.match(page.node('empty').textContent, /Waiting for a demo Run/);
  const requests = page.requests.length;
  page.switchTo('zh-CN');
  assert.match(page.node('empty').textContent, /等待演示 Run/);
  assert.match(page.node('runs').textContent, /尚无 Run/);
  assert.match(page.node('freshness').textContent, /尚无演示 Run/);
  assert.equal(page.requests.length, requests);
  const snapshot = runningSnapshot();
  snapshot.samples = [];
  const claimed = await controller({snapshot});
  assert.match(claimed.node('timeline').textContent, /No Handler START/);
  claimed.switchTo('zh-CN');
  assert.match(claimed.node('timeline').textContent, /尚无 Handler START/);
  assert.match(claimed.node('workers').textContent, /领取不等于已开始执行/);
  assert.equal(claimed.node('overlap').textContent, '0');
});

test('retry explanations keep LOST, future eligibility and absence of a new claim explicit', async () => {
  const snapshot = runningSnapshot();
  snapshot.run.scenario = 'recovery';
  snapshot.tasks[0].status = 'RETRY_WAIT';
  Object.assign(snapshot.attempts[0], {status: 'LOST', scheduled_at: at(5), available_at: at(12), lease_expires_at: at(4)});
  const page = await controller({snapshot});
  assert.match(page.node('waiting-tasks').textContent, /Retry backoff; eligible at 10:00:12\.000Z/);
  assert.match(page.node('recovery').textContent, /Old Attempt #1 LOST/);
  assert.match(page.node('recovery').textContent, /No next claim recorded/);
  assert.equal(page.node('replacement-note').hidden, false);
  const requests = page.requests.length;
  page.switchTo('zh-CN');
  assert.match(page.node('waiting-tasks').textContent, /重试退避中；可重试时间 10:00:12\.000Z/);
  assert.match(page.node('recovery').textContent, /旧 Attempt #1 LOST/);
  assert.match(page.node('recovery').textContent, /尚未记录下一次领取/);
  assert.match(page.node('recovery').textContent, /当前 Task 状态：RETRY_WAIT/);
  assert.equal(page.node('attempts').children.length, 2);
  assert.equal(page.requests.length, requests);
});

test('diamond recovery explains persisted sibling success and join waiting in both languages', async () => {
  const snapshot = diamondSnapshot();
  const page = await controller({snapshot});
  assert.equal(page.node('error').hidden, true);
  assert.match(page.node('scenario-purpose').textContent, /A → B\/C → D/);
  assert.match(page.node('scenario-purpose').textContent, /C 20s/);
  assert.match(page.node('results').textContent, /B remains SUCCEEDED on Attempt #1/);
  assert.match(page.node('results').textContent, /D is PENDING: B succeeded, but C is RETRY_WAIT/);
  assert.doesNotMatch(page.node('results').textContent, /Confirmed C Attempt #2/);
  assert.match(page.node('replacement-note').textContent, /starts no replacement Worker/);
  assert.doesNotMatch(page.node('replacement-note').textContent, /Historical/);
  const groups = descendants(page.node('dag')).filter(node => node.getAttribute('data-task-key'));
  const row = key => Number(descendants(groups.find(node => node.getAttribute('data-task-key') === key))
    .find(node => node.tagName === 'RECT').getAttribute('y'));
  assert.ok(row('A') < row('B'));
  assert.equal(row('B'), row('C'));
  assert.ok(row('C') < row('D'));
  const raw = page.node('identities').textContent;
  page.switchTo('zh-CN');
  assert.match(page.node('results').textContent, /B 在 Attempt #1 保持 SUCCEEDED/);
  assert.match(page.node('results').textContent, /D 仍为 PENDING：B 已成功，但 C 为 RETRY_WAIT/);
  assert.match(page.node('replacement-note').textContent, /不启动替代 Worker/);
  assert.equal(page.node('identities').textContent, raw);
  const retryPage = await controller({snapshot:diamondSnapshot({retried:true})});
  assert.match(retryPage.node('results').textContent, /Confirmed C Attempt #2 on Worker 2, the same Worker that completed B · RUNNING/);
  retryPage.switchTo('zh-CN');
  assert.match(retryPage.node('results').textContent, /已确认 C Attempt #2 由 Worker 2 领取，与完成 B 的是同一个 Worker · RUNNING/);
});

test('selecting a historical Run keeps its saved DAG and legacy explanation without POST or language side effects', async () => {
  const legacy = runningSnapshot();
  legacy.run.scenario = 'recovery';
  legacy.run.definition.tasks = [{task_id:'A',task_type:'demo.recover',depends_on:[]},
    {task_id:'Join',task_type:'demo.join',depends_on:['A']}];
  legacy.tasks = [{id:'legacy-a',task_key:'A',status:'RUNNING'}, {id:'legacy-join',task_key:'Join',status:'PENDING'}];
  legacy.attempts = [];
  legacy.samples = [];
  const diamond = diamondSnapshot({scenario:'distribution'});
  const legacyJson = JSON.stringify(legacy.run.definition, null, 2);
  const page = await controller({snapshot:diamond, historySnapshots:[legacy]});
  assert.match(page.node('scenario-purpose').textContent, /C 14s/);
  page.node('runs').value = legacy.run.id;
  page.node('runs').dispatch('change');
  await page.nextPoll();
  assert.equal(page.node('definition').textContent, legacyJson);
  const legacyNodes = descendants(page.node('dag')).filter(node => node.getAttribute('data-task-key'));
  assert.deepEqual(legacyNodes.map(node => node.getAttribute('data-task-key')).sort(), ['A', 'Join']);
  assert.match(legacyNodes.find(node => node.getAttribute('data-task-key') === 'Join').textContent, /PENDING/);
  assert.match(page.node('replacement-note').textContent, /Historical A → Join/);
  assert.doesNotMatch(page.node('scenario-purpose').textContent, /C 20s/);
  const requests = page.requests.length, historyCount = page.historyCalls.length;
  page.node('attempt-details').open = false;
  page.switchTo('zh-CN');
  assert.match(page.node('replacement-note').textContent, /历史 A → Join/);
  assert.equal(page.node('runs').value, legacy.run.id);
  assert.equal(page.node('follow').checked, false);
  assert.equal(page.node('attempt-details').open, false);
  assert.equal(page.node('definition').textContent, legacyJson);
  assert.equal(page.requests.length, requests);
  assert.equal(page.historyCalls.length, historyCount);
  page.node('runs').value = diamond.run.id;
  page.node('runs').dispatch('change');
  await page.nextPoll();
  assert.match(page.node('scenario-purpose').textContent, /C 14s/);
  assert.equal(page.node('replacement-note').hidden, true);
  assert.ok(page.requests.every(request => !request.options.method || request.options.method === 'GET'));
});

test('custom saved topology and full 64-character names survive bilingual rendering and waiting guidance', async () => {
  const snapshot = customSnapshot(), original = JSON.stringify(snapshot);
  const page = await controller({snapshot, port: '18081'});
  assert.equal(page.node('error').hidden, true);
  assert.match(page.node('scenario').textContent, /Custom/);
  assert.equal(page.node('replacement-note').hidden, true);
  assert.equal(page.node('resource-guidance').hidden, false);
  assert.equal(page.node('worker-command').hidden, false);
  const command = `uv run python scripts/demo.py --port 18081 workers --run-id ${snapshot.run.id}`;
  assert.equal(page.node('worker-command').textContent, command);
  assert.doesNotMatch(page.node('scenario-purpose').textContent, /A → B\/C → D|C 20s|C's actual owner/);
  assert.equal(page.node('overlap').textContent, '0');
  assert.equal(page.node('owners').textContent, '0');
  const groups = () => descendants(page.node('dag')).filter(node => node.getAttribute('data-task-key'));
  const keys = snapshot.run.definition.tasks.map(task => task.task_id).sort();
  assert.deepEqual(groups().map(node => node.getAttribute('data-task-key')).sort(), keys);
  const longName = keys.find(key => key.length === 64);
  assert.ok(longName);
  const visibleNames = () => groups().map(group => descendants(group)
    .filter(node => node.getAttribute('data-node-line') === 'name').map(node => node.textContent).join('')).sort();
  assert.deepEqual(visibleNames(), keys, 'Full raw names remain visible, not just in tooltips');
  const edges = () => descendants(page.node('dag')).filter(node => node.getAttribute('data-from'))
    .map(node => `${node.getAttribute('data-from')}->${node.getAttribute('data-to')}`).sort();
  const expectedEdges = snapshot.run.definition.tasks.flatMap(task => task.depends_on.map(parent => `${parent}->${task.task_id}`)).sort();
  assert.deepEqual(edges(), expectedEdges);
  assert.ok(page.node('waiting-tasks').textContent.includes(`${longName} (READY)`));
  assert.match(page.node('waiting-tasks').textContent, /Fetch_aux \(READY\)/);
  const before = {requests: page.requests.length, definition: page.node('definition').textContent,
    guidance: page.node('resource-guidance').textContent, edges: edges(), run: page.node('runs').value};
  page.node('attempt-details').open = false;
  page.switchTo('zh-CN');
  assert.match(page.node('scenario').textContent, /自定义/);
  assert.notEqual(page.node('resource-guidance').textContent, before.guidance);
  assert.match(page.node('resource-guidance').textContent, /[\u4e00-\u9fff]/);
  assert.equal(page.node('worker-command').textContent, command);
  assert.equal(page.node('runs').value, before.run);
  assert.equal(page.node('follow').checked, false);
  assert.equal(page.node('attempt-details').open, false);
  assert.equal(page.node('definition').textContent, before.definition);
  assert.deepEqual(groups().map(node => node.getAttribute('data-task-key')).sort(), keys);
  assert.deepEqual(visibleNames(), keys);
  assert.deepEqual(edges(), before.edges);
  assert.ok(page.node('waiting-tasks').textContent.includes(`${longName} (READY)`));
  page.switchTo('en');
  assert.equal(page.requests.length, before.requests);
  assert.equal(JSON.stringify(snapshot), original);
  assert.ok(page.requests.every(request => !request.options.method || request.options.method === 'GET'));
});

test('serial custom DAG retains twelve actual layers without claiming observed parallelism', async () => {
  const snapshot = customSnapshot({serial: true});
  const page = await controller({snapshot});
  assert.equal(page.node('error').hidden, true);
  const groups = descendants(page.node('dag')).filter(node => node.getAttribute('data-task-key'));
  assert.equal(groups.length, 12);
  const positions = groups.map(group => Number(descendants(group).find(node => node.tagName === 'RECT').getAttribute('y')));
  assert.equal(new Set(positions).size, 12);
  assert.ok(Math.max(...positions) > 420);
  assert.equal(page.node('overlap').textContent, '0');
  assert.match(page.node('waiting-tasks').textContent, /Stage_10 \(PENDING\)/);
  assert.equal(page.node('replacement-note').hidden, true);
  page.switchTo('zh-CN');
  assert.equal(page.node('overlap').textContent, '0');
  assert.match(page.node('waiting-tasks').textContent, /Stage_10 \(PENDING\)/);
});

test('custom Worker startup command disappears after membership or a terminal result', async () => {
  const snapshot = customSnapshot();
  const page = await controller({snapshot});
  const command = page.node('worker-command').textContent;
  assert.equal(page.node('worker-command').hidden, false);
  snapshot.workers.push({id: 'registered-custom-worker-a', worker_name: 'actual-custom-worker-a',
    max_concurrency: 1, status: 'ACTIVE', last_heartbeat_at: at(5), heartbeat_expires_at: at(11)});
  await page.nextPoll();
  assert.equal(page.node('worker-command').hidden, true);
  assert.equal(page.node('resource-guidance').hidden, false);
  const partial = page.node('resource-guidance').textContent;
  page.switchTo('zh-CN');
  assert.notEqual(page.node('resource-guidance').textContent, partial);
  assert.equal(page.node('worker-command').hidden, true);
  snapshot.workers.push({...snapshot.workers[0], id: 'registered-custom-worker-b', worker_name: 'actual-custom-worker-b'});
  await page.nextPoll();
  assert.equal(page.node('worker-command').hidden, true);
  assert.equal(page.node('resource-guidance').hidden, false);
  assert.match(page.node('resource-guidance').textContent, /2 个心跳有效的 Worker/);
  snapshot.workers = [];
  snapshot.run.status = 'FAILED';
  await page.nextPoll();
  assert.equal(page.node('worker-command').hidden, true);
  assert.equal(page.node('resource-guidance').hidden, true);
  assert.ok(command.includes(snapshot.run.id));
});

const customPosts = page => page.requests.filter(request => request.options.method === 'POST');
const creations = page => customPosts(page).filter(request => request.path === '/demo/custom/runs');
const previewPayload = () => ({definition: customDefinition(), layers: [['Read_rows'], ['Publish_report']],
  limits: customCatalog().limits});
function typeDraft(page, text) {
  page.node('custom-json').value = text;
  page.node('custom-json').dispatch('input');
}
async function previewDraft(page, text = JSON.stringify(customDefinition(), null, 2)) {
  typeDraft(page, text);
  page.node('custom-validate').dispatch('click');
  await page.settle();
  assert.equal(page.node('custom-preview').hidden, false);
  assert.equal(page.node('custom-create').disabled, false);
  return text;
}
function createdSnapshot() {
  const data = customSnapshot();
  data.run.id = customReceipt.run_id;
  data.run.definition = customDefinition();
  data.tasks = data.run.definition.tasks.map((task, index) => ({id: `created-task-${index}`,
    task_key: task.task_id, status: task.depends_on.length ? 'PENDING' : 'READY'}));
  return data;
}

test('editor templates only copy drafts, and backend preview retains canonical names without execution states', async () => {
  const page = await controller({requestHandler: path => path === '/demo/custom/validate' ? response(previewPayload()) : undefined});
  const selected = page.node('runs').value;
  const initialRequests = page.requests.length;
  assert.equal(page.requests.filter(request => request.path === '/demo/custom/catalog').length, 1);
  assert.equal(page.node('custom-template').children.length, 3);
  assert.match(page.node('custom-handlers').textContent, /demo\.observe/);
  assert.match(page.node('custom-limits').textContent, /12/);
  page.node('custom-template').value = 'recovery';
  page.node('custom-template').dispatch('change');
  page.node('custom-load').dispatch('click');
  await page.settle();
  assert.deepEqual(JSON.parse(page.node('custom-json').value), customCatalog().templates[2].definition);
  assert.equal(page.node('custom-preview').hidden, true);
  assert.equal(page.requests.length, initialRequests);
  assert.equal(page.node('runs').value, selected);
  const submitted = customDefinition();
  delete submitted.schema_version;
  for (const task of submitted.tasks) delete task.execution;
  const text = await previewDraft(page, JSON.stringify(submitted, null, 2));
  const request = customPosts(page)[0];
  assert.equal(request.path, '/demo/custom/validate');
  assert.equal(request.options.body, text);
  assert.equal(new Headers(request.options.headers).get('Content-Type'), 'application/json');
  assert.deepEqual(JSON.parse(page.node('custom-canonical').textContent), customDefinition());
  const previewNodes = descendants(page.node('custom-preview-dag')).filter(node => node.getAttribute('data-task-key'));
  assert.deepEqual(previewNodes.map(node => node.getAttribute('data-task-key')), ['Publish_report', 'Read_rows']);
  assert.doesNotMatch(page.node('custom-preview-dag').textContent, /READY|RUNNING|SUCCEEDED|Worker|Attempt/);
  assert.equal(page.node('runs').value, selected);
  assert.equal(creations(page).length, 0);
  assert.equal(page.document.querySelectorAll('.step').length, 6);
});

test('editing during validation invalidates preview and language switching preserves draft caret and selected evidence', async () => {
  const late = deferred();
  const page = await controller({requestHandler: path => path === '/demo/custom/validate' ? late.promise : undefined});
  const draft = JSON.stringify(customDefinition(), null, 2);
  typeDraft(page, draft);
  page.node('custom-validate').dispatch('click');
  assert.equal(page.node('custom-create').disabled, true);
  const edited = draft.replace('Page_custom_workflow', 'Edited_workflow');
  typeDraft(page, edited);
  const textarea = page.node('custom-json');
  textarea.focus();
  textarea.setSelectionRange(5, 17, 'backward');
  textarea.scrollTop = 80;
  page.node('composer-panel').open = true;
  page.node('attempt-details').open = false;
  const before = {requests: page.requests.length, run: page.node('runs').value,
    follow: page.node('follow').checked, scroll: page.window.scrollY, geometry: bars(page)};
  page.switchTo('zh-CN');
  assert.equal(textarea.value, edited);
  assert.equal(textarea.selectionStart, 5);
  assert.equal(textarea.selectionEnd, 17);
  assert.equal(textarea.selectionDirection, 'backward');
  assert.equal(textarea.scrollTop, 80);
  assert.equal(page.document.activeElement, textarea);
  assert.equal(page.node('composer-panel').open, true);
  assert.equal(page.node('attempt-details').open, false);
  assert.equal(page.node('runs').value, before.run);
  assert.equal(page.node('follow').checked, before.follow);
  assert.equal(page.window.scrollY, before.scroll);
  assert.deepEqual(bars(page), before.geometry);
  assert.equal(page.requests.length, before.requests);
  late.resolve(response(previewPayload()));
  await page.settle();
  assert.equal(page.node('custom-preview').hidden, true);
  assert.equal(page.node('custom-create').disabled, true);
  assert.equal(textarea.value, edited);
  page.switchTo('en');
  assert.equal(page.requests.length, before.requests);
  assert.equal(creations(page).length, 0);
});

test('explicit create sends one frozen operation and selects the confirmed no-Worker Run', async () => {
  const created = deferred();
  const data = createdSnapshot();
  const page = await controller({port: '18081', requestHandler: path => {
    if (path === '/demo/custom/validate') return response(previewPayload());
    if (path === '/demo/custom/runs') return created.promise;
    if (path === '/demo/runs/' + customReceipt.run_id) return response(data);
  }});
  await previewDraft(page);
  const selected = page.node('runs').value;
  page.node('custom-create').dispatch('click');
  page.node('custom-create').dispatch('click');
  assert.equal(creations(page).length, 1);
  assert.equal(page.node('custom-json').readOnly, true);
  assert.equal(page.node('custom-create').disabled, true);
  const sent = creations(page)[0];
  assert.equal(sent.options.body, JSON.stringify(customDefinition()));
  assert.equal(new Headers(sent.options.headers).get('Idempotency-Key'), customKey);
  assert.equal(page.node('custom-key').textContent, customKey);
  assert.equal(page.node('custom-body').textContent, sent.options.body);
  page.node('custom-operation').open = true;
  const requests = page.requests.length;
  page.switchTo('zh-CN');
  assert.equal(page.node('custom-operation').open, true);
  assert.equal(page.node('runs').value, selected);
  assert.equal(page.requests.length, requests);
  assert.equal(page.node('custom-key').textContent, customKey);
  created.resolve(response(customReceipt, 201));
  await page.settle();
  assert.deepEqual(JSON.parse(page.node('custom-receipt').textContent), customReceipt);
  assert.equal(page.node('runs').value, customReceipt.run_id);
  assert.equal(page.node('follow').checked, false);
  assert.equal(page.node('content').hidden, true, 'Receipt selects a Run but cannot invent its snapshot');
  await page.nextPoll();
  assert.equal(page.node('content').hidden, false);
  assert.ok(page.node('run-id').textContent.includes(customReceipt.run_id));
  assert.equal(page.node('run-status').textContent, 'RUNNING');
  assert.equal(page.node('worker-command').hidden, false);
  assert.equal(page.node('worker-command').textContent,
    `uv run python scripts/demo.py --port 18081 workers --run-id ${customReceipt.run_id}`);
  assert.equal(page.node('overlap').textContent, '0');
  assert.equal(page.node('owners').textContent, '0');
  assert.equal(page.node('sample-count').textContent, '0');
  assert.equal(page.node('replacement-note').hidden, true);
  assert.match(page.node('ready-tasks').textContent, /Read_rows/);
  assert.match(page.node('waiting-tasks').textContent, /Read_rows \(READY\)/);
  assert.equal(creations(page).length, 1);
  assert.equal(page.timers.length, 1, 'Creation must not introduce another poll loop');
});

test('unknown submit is visibly retried with the same key and body, and reload never submits automatically', async () => {
  let attempts = 0;
  const page = await controller({requestHandler: path => {
    if (path === '/demo/custom/validate') return response(previewPayload());
    if (path === '/demo/custom/runs') {
      if (++attempts === 1) return Promise.reject(new TypeError('private-response-loss-sentinel'));
      return response(customReceipt, 201);
    }
    if (path === '/demo/runs/' + customReceipt.run_id) return response(createdSnapshot());
  }});
  await previewDraft(page);
  page.node('custom-create').dispatch('click');
  await page.settle();
  assert.equal(page.node('custom-retry').hidden, false);
  assert.equal(page.node('custom-json').readOnly, true);
  assert.equal(page.node('custom-create').disabled, true);
  assert.doesNotMatch(page.node('custom-errors').textContent, /private-response-loss-sentinel/);
  const pending = page.stored.get(COMPOSER_STORAGE_KEY);
  assert.equal(JSON.parse(pending).key, customKey);
  assert.equal(JSON.parse(pending).body, creations(page)[0].options.body);
  const oldSelection = page.node('runs').value;
  const restored = await controller({storedEntries: [[COMPOSER_STORAGE_KEY, pending]], requestHandler: path => {
    if (path === '/demo/custom/runs') return response(customReceipt, 201);
    if (path === '/demo/runs/' + customReceipt.run_id) return response(createdSnapshot());
  }});
  assert.equal(restored.node('runs').value, oldSelection);
  assert.equal(restored.node('custom-retry').hidden, false);
  assert.equal(creations(restored).length, 0);
  const before = restored.requests.length;
  const english = restored.node('custom-status').textContent;
  restored.switchTo('zh-CN');
  assert.notEqual(restored.node('custom-status').textContent, english);
  assert.match(restored.node('custom-status').textContent, /[\u4e00-\u9fff]/);
  assert.equal(restored.requests.length, before);
  restored.node('custom-retry').dispatch('click');
  restored.node('custom-retry').dispatch('click');
  await restored.settle();
  assert.equal(creations(restored).length, 1);
  assert.equal(creations(restored)[0].options.body, creations(page)[0].options.body);
  assert.equal(new Headers(creations(restored)[0].options.headers).get('Idempotency-Key'), customKey);
  assert.equal(restored.node('runs').value, customReceipt.run_id);
  assert.deepEqual(JSON.parse(restored.node('custom-receipt').textContent), customReceipt);
});

test('validation errors translate safely while retaining API code and precise field path', async () => {
  const page = await controller({requestHandler: path => path === '/demo/custom/validate' ? response({error: {
    code: 'invalid_request', message: 'private-server-message-sentinel',
    details: [{type: 'demo_handler_not_allowed', location: ['body', 'tasks', 1, 'task_type'], input: 'private-input-sentinel'}]
  }}, 422) : undefined});
  typeDraft(page, JSON.stringify(customDefinition()));
  page.node('custom-validate').dispatch('click');
  await page.settle();
  assert.equal(page.node('custom-errors').hidden, false);
  const english = page.node('custom-errors').textContent;
  assert.match(english, /invalid_request/);
  assert.match(english, /demo_handler_not_allowed/);
  assert.match(english, /body.*tasks.*1.*task_type/);
  assert.doesNotMatch(english, /private-.*-sentinel/);
  assert.equal(page.node('custom-preview').hidden, true);
  assert.equal(page.node('custom-create').disabled, true);
  const count = page.requests.length;
  page.switchTo('zh-CN');
  const chinese = page.node('custom-errors').textContent;
  assert.notEqual(chinese, english);
  assert.match(chinese, /[\u4e00-\u9fff]/);
  assert.match(chinese, /demo_handler_not_allowed/);
  assert.doesNotMatch(chinese, /private-.*-sentinel/);
  assert.equal(page.requests.length, count);
  assert.equal(creations(page).length, 0);
});

test('storage fallback and corrupt saved operation are visible without losing current observation', async () => {
  const fallback = await controller({storageDenied: true});
  assert.equal(fallback.node('custom-storage').hidden, false);
  const english = fallback.node('custom-storage').textContent;
  fallback.switchTo('zh-CN');
  assert.notEqual(fallback.node('custom-storage').textContent, english);
  assert.match(fallback.node('custom-storage').textContent, /[\u4e00-\u9fff]/);
  assert.equal(fallback.node('run-status').textContent, 'RUNNING');
  const raw = '{"unrecoverable":true}';
  const blocked = await controller({storedEntries: [[COMPOSER_STORAGE_KEY, raw]]});
  assert.equal(blocked.node('custom-recovery').hidden, false);
  assert.equal(blocked.node('custom-recovery').textContent, raw);
  assert.equal(blocked.node('custom-json').readOnly, true);
  assert.equal(blocked.node('custom-create').disabled, true);
  assert.equal(blocked.node('custom-validate').disabled, true);
  assert.equal(blocked.stored.get(COMPOSER_STORAGE_KEY), raw);
  assert.equal(blocked.node('run-status').textContent, 'RUNNING');
  assert.equal(creations(blocked).length, 0);
});

test('restored success requires explicit Observe and starting a new draft does not create another Run', async () => {
  const stored = JSON.stringify({version: 1, key: customKey, body: JSON.stringify(customDefinition()), receipt: customReceipt});
  const page = await controller({storedEntries: [[COMPOSER_STORAGE_KEY, stored]], requestHandler: path =>
    path === '/demo/runs/' + customReceipt.run_id ? response(createdSnapshot()) : undefined});
  assert.equal(page.node('runs').value, 'run-unchanged-id');
  assert.deepEqual(JSON.parse(page.node('custom-receipt').textContent), customReceipt);
  assert.equal(page.node('custom-observe').hidden, false);
  assert.equal(page.node('custom-retry').hidden, true);
  const requests = page.requests.length;
  page.switchTo('zh-CN');
  assert.equal(page.requests.length, requests);
  assert.equal(page.node('runs').value, 'run-unchanged-id');
  page.node('custom-observe').dispatch('click');
  assert.equal(page.node('runs').value, customReceipt.run_id);
  assert.equal(page.requests.length, requests);
  await page.nextPoll();
  assert.ok(page.node('run-id').textContent.includes(customReceipt.run_id));
  const observedRequests = page.requests.length;
  page.node('custom-new').dispatch('click');
  assert.equal(page.node('custom-json').value, '');
  assert.equal(page.node('custom-json').readOnly, false);
  assert.equal(page.node('custom-create').disabled, true);
  assert.equal(page.node('custom-receipt').hidden, true);
  assert.equal(page.stored.has(COMPOSER_STORAGE_KEY), false);
  page.node('custom-template').value = 'parallel';
  page.node('custom-load').dispatch('click');
  assert.deepEqual(JSON.parse(page.node('custom-json').value), customCatalog().templates[0].definition);
  assert.equal(page.node('runs').value, customReceipt.run_id);
  assert.equal(page.requests.length, observedRequests);
  assert.equal(customPosts(page).length, 0);
});

test('catalog failure and explicit reload preserve drafts and existing observation', async () => {
  let requests = 0;
  const reload = deferred();
  const page = await controller({requestHandler: path => {
    if (path === '/demo/custom/catalog') return ++requests === 1 ?
      response({error: {code: 'database_unavailable', message: 'private-catalog-sentinel'}}, 503) : reload.promise;
  }});
  assert.equal(page.node('custom-load').disabled, true);
  assert.equal(page.node('custom-catalog-error').hidden, false);
  assert.doesNotMatch(page.node('custom-catalog-error').textContent, /private-catalog-sentinel/);
  const draft = JSON.stringify(customDefinition(), null, 2);
  typeDraft(page, draft);
  const english = page.node('custom-catalog-error').textContent;
  page.switchTo('zh-CN');
  assert.notEqual(page.node('custom-catalog-error').textContent, english);
  assert.equal(requests, 1);
  assert.equal(page.node('run-status').textContent, 'RUNNING');
  page.node('custom-catalog-reload').dispatch('click');
  page.node('custom-catalog-reload').dispatch('click');
  assert.equal(requests, 2);
  reload.resolve(response(customCatalog()));
  await page.settle();
  assert.equal(page.node('custom-catalog-error').hidden, true);
  assert.equal(page.node('custom-load').disabled, false);
  assert.equal(page.node('custom-json').value, draft);
  assert.equal(page.node('custom-preview').hidden, true);
  assert.equal(page.node('runs').value, 'run-unchanged-id');
  assert.equal(customPosts(page).length, 0);
});
