import assert from 'node:assert/strict';
import test from 'node:test';
import {messages} from '../src/workflow_engine/demo/static/messages.js';
import {LANGUAGE_KEY, initialLanguage, createI18n, translate} from '../src/workflow_engine/demo/static/i18n.js';

const memory = value => ({
  getItem: key => key === LANGUAGE_KEY ? value : null,
  setItem(key, next) { assert.equal(key, LANGUAGE_KEY); value = next; }
});
const denied = () => { throw new Error('Storage unavailable'); };

test('first visit follows primary browser language, with English fallback', () => {
  for (const tag of ['zh', 'zh-CN', 'zh-TW', 'ZH-Hant-HK']) {
    assert.equal(initialLanguage([tag]), 'zh-CN');
  }
  for (const languages of [[], ['en-US'], ['fr-FR', 'zh-CN'], ['zhish'], ['']]) {
    assert.equal(initialLanguage(languages), 'en');
  }
});

test('valid manual choice survives reload and overrides browser language', () => {
  const stored = memory(null);
  const options = {languages: ['zh-CN'], storage: () => stored};
  const ui = createI18n(options);
  assert.equal(ui.language, 'zh-CN');
  assert.equal(ui.select('en'), true);
  assert.equal(createI18n(options).language, 'en');
  assert.equal(ui.select('en'), false);
  assert.equal(ui.select('fr'), false);
  assert.equal(ui.language, 'en');
  assert.equal(initialLanguage(['zh'], () => memory('invalid')), 'zh-CN');
});

test('choosing the initial language still saves an explicit preference', () => {
  const stored = memory(null);
  const ui = createI18n({languages: ['zh'], storage: () => stored});
  ui.select('zh-CN');
  assert.equal(initialLanguage(['en'], () => stored), 'zh-CN');
});

test('denied getters, reads and writes never prevent language selection', () => {
  for (const storage of [denied, () => ({getItem: denied, setItem: denied}), () => null]) {
    const ui = createI18n({languages: ['zh-TW'], storage});
    assert.equal(ui.language, 'zh-CN');
    assert.equal(ui.select('en'), true);
    assert.equal(ui.language, 'en');
    assert.equal(ui.select('zh-CN'), true);
  }
});

test('both complete catalogs have matching keys and named parameters', () => {
  assert.deepEqual(Object.keys(messages.en).sort(), Object.keys(messages['zh-CN']).sort());
  const placeholders = value => [...value.matchAll(/\{(\w+)\}/g)].map(m => m[1]).sort();
  for (const [key, english] of Object.entries(messages.en)) {
    const chinese = messages['zh-CN'][key];
    assert.ok(english.trim() && chinese.trim(), key);
    assert.deepEqual(placeholders(english), placeholders(chinese), key);
    // Language buttons use native names so a reader can always switch back.
    if (key !== 'language.zh-CN') assert.doesNotMatch(english, /[\u3400-\u9fff]/, key);
    const params = Object.fromEntries(placeholders(english).map(p => [p, `{raw:${p}}`]));
    for (const lang of ['en', 'zh-CN']) {
      const result = translate(lang, key, params);
      for (const p of Object.values(params)) assert.ok(result.includes(p), key);
    }
  }
});

test('dynamic explanations preserve raw identifiers, states and clock evidence', () => {
  for (const language of ['en', 'zh-CN']) {
    const raw = 'Task_原始-name{untouched}';
    assert.ok(translate(language, 'dependency.dependencies', {blockers: raw}).includes(raw));
    assert.match(translate(language, 'dependency.scheduler'), /PENDING/);
    assert.match(translate(language, 'dependency.retry_due'), /RETRY_WAIT.*READY/);
    assert.match(translate(language, 'attempt.noFinish'), /FINISH/);
    assert.match(translate(language, 'heartbeat.lost'), /LOST/);
    const claim = translate(language, 'recovery.newClaim', {
      number: 2, worker: 'worker-original-uuid', time: '12:00:01.001Z', status: 'RUNNING'
    });
    for (const value of ['2', 'worker-original-uuid', '12:00:01.001Z', 'RUNNING']) {
      assert.ok(claim.includes(value));
    }
  }
});

test('unknown keys and missing interpolation values fail visibly during development', () => {
  assert.throws(() => translate('en', 'not.a.real.key'), /Unknown translation key/);
  const key = Object.keys(messages.en).find(k => /\{\w+\}/.test(messages.en[k]));
  assert.ok(key);
  assert.throws(() => translate('en', key), /Missing translation parameter/);
  const plainKey = Object.keys(messages.en).find(k => !/\{\w+\}/.test(messages.en[k]));
  assert.equal(translate('de', plainKey), messages.en[plainKey]);
});
