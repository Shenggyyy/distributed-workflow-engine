import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import * as flow from '../src/workflow_engine/demo/static/flow.js';
import * as evidence from '../src/workflow_engine/demo/static/evidence.js';
import * as i18n from '../src/workflow_engine/demo/static/i18n.js';
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
    this.value = '';
  }
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
    createElement(tag) { return new Node(tag); },
    createElementNS(_namespace, tag) { return new Node(tag); },
    querySelectorAll(selector) { return descendants(root).filter(node => matches(node, selector)); },
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; },
    get documentElement() { return this.querySelector('html'); },
    get body() { return this.querySelector('body'); },
  };
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

async function controller({snapshot = runningSnapshot(), selected = true} = {}) {
  const document = documentFixture();
  const requests = [], timers = [], historyCalls = [], stored = new Map();
  let failure = null;
  const window = {scrollY: 1234, scrollX: 0, innerHeight: 800,
    localStorage: {getItem(key) { return stored.get(key) ?? null; }, setItem(key, value) { stored.set(key, value); }},
    scrollTo(x, y) { this.scrollY = typeof x === 'object' ? x.top : y; }};
  const context = vm.createContext({
    ...flow, ...evidence, ...i18n, document, window,
    navigator: {languages: ['en-GB'], language: 'en-GB'},
    location: {search: selected && snapshot ? '?run=' + snapshot.run.id : ''},
    history: {replaceState(...args) { historyCalls.push(args); }},
    URLSearchParams, AbortSignal, TypeError, Error,
    setTimeout(callback) { timers.push(callback); },
    fetch: async (path, options) => {
      requests.push({path, options});
      if (failure) throw failure;
      const payload = path === '/demo/runs' ? snapshot ? [{run_id: snapshot.run.id,
        scenario: snapshot.run.scenario, status: snapshot.run.status}] : [] : snapshot;
      return {ok: true, json: async () => payload};
    },
  });
  vm.runInContext(source, context, {filename: 'app.js'});
  const settle = () => new Promise(resolve => setImmediate(resolve));
  await settle();
  return {
    document, requests, timers, window, stored, historyCalls,
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
