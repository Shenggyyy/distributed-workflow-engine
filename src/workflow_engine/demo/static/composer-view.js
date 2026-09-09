import {createComposer} from './composer.js';
import {drawDefinitionDag} from './dag.js';

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const token = value => typeof value === 'string' && /^[A-Za-z][A-Za-z0-9_.:-]{0,79}$/.test(value);
const scenarios = ['parallel', 'distribution', 'recovery'];
const issues = new Set(['json_invalid', 'duplicate_task_id', 'duplicate_dependency',
  'unknown_dependency', 'self_dependency', 'cycle_detected', 'demo_task_limit',
  'demo_handler_not_allowed', 'demo_execution_policy', 'string_pattern_mismatch',
  'string_too_long', 'string_too_short', 'missing', 'extra_forbidden', 'too_short',
  'too_long', 'execution_requires_schema_v2', 'demo_body_too_large', 'demo_json_required',
  'demo_submission_conflict', 'database_unavailable', 'storage_error']);

function safeFailure(error) {
  const result = {};
  if (token(error?.name)) result.name = error.name;
  if (token(error?.code)) result.code = error.code;
  if (Number.isInteger(error?.status) && error.status >= 100 && error.status <= 599) result.status = error.status;
  if (Array.isArray(error?.details)) result.details = error.details.slice(0, 20)
    .filter(detail => object(detail) && token(detail.type) && Array.isArray(detail.location) &&
      detail.location.length <= 16 && detail.location.every(part =>
        (typeof part === 'string' && part.length <= 64 && /^[A-Za-z0-9_.-]+$/.test(part)) ||
        (Number.isInteger(part) && part >= 0 && part <= 1000)))
    .map(detail => ({type: detail.type, location: [...detail.location]}));
  return result;
}

function validCatalog(value) {
  const bounds = ['max_tasks', 'max_body_bytes', 'max_attempts', 'timeout_seconds', 'worker_count', 'slots_per_worker'];
  return object(value) && object(value.limits) && bounds.every(key => Number.isInteger(value.limits[key]) && value.limits[key] > 0) &&
    Array.isArray(value.handlers) && value.handlers.length > 0 && value.handlers.length <= 16 &&
    value.handlers.every(handler => object(handler) && token(handler.task_type) && Number.isFinite(handler.seconds) && handler.seconds > 0) &&
    Array.isArray(value.templates) && value.templates.length === scenarios.length &&
    new Set(value.templates.map(template => template?.scenario)).size === scenarios.length &&
    value.templates.every(template => object(template) && scenarios.includes(template.scenario) &&
      object(template.definition) && Array.isArray(template.definition.tasks));
}

export function initComposerView({document, t, fetch, storage, newKey, onCreated}) {
  const $ = id => document.getElementById(id);
  const text = (id, value) => {
    if ($(id).textContent !== value) $(id).textContent = value;
  };
  const element = (tag, value) => {
    const node = document.createElement(tag);
    node.textContent = value;
    return node;
  };
  const request = async (path, expected, options = {}) => {
    try {
      const response = await fetch(path, {...options, cache: 'no-store', signal: AbortSignal.timeout(5000)});
      let value;
      try { value = await response.json(); }
      catch { throw {name: 'ResponseError', code: 'invalid_response', status: response.status}; }
      if (response.status !== expected) throw {name: 'HTTPError', status: response.status,
        code: value?.error?.code, details: value?.error?.details};
      return value;
    } catch (error) { throw safeFailure(error); }
  };
  let catalog = null, catalogLoading = false, catalogError = null, previewSignature = null;
  const issue = type => t('composer.issue.' + (issues.has(type) ? type : 'other'));
  const describeFailure = error => {
    if (error.status) return t('composer.error.http', {
      status: error.status, code: error.code || 'UNKNOWN', description: issue(error.code)
    });
    return t(['TimeoutError', 'AbortError'].includes(error.name) ? 'composer.error.timeout' : 'composer.error.connection');
  };
  const renderCatalog = canDraft => {
    $('custom-catalog-reload').disabled = catalogLoading;
    $('custom-template').disabled = !catalog || !canDraft;
    $('custom-load').disabled = !catalog || !canDraft;
    const problem = catalogLoading ? t('composer.catalog.loading') : catalogError ?
      t(catalogError.code === 'invalid_catalog' || catalogError.code === 'invalid_response' ?
        'composer.catalog.invalid' : 'composer.catalog.failed') + ' ' + describeFailure(catalogError) : '';
    text('custom-catalog-error', problem);
    $('custom-catalog-error').hidden = !problem;
    if (!catalog) return;
    const limits = catalog.limits;
    text('custom-limits', t('composer.limits', {tasks: limits.max_tasks, bytes: limits.max_body_bytes,
      attempts: limits.max_attempts, timeout: limits.timeout_seconds, workers: limits.worker_count, slots: limits.slots_per_worker}));
    for (const option of $('custom-template').children) option.textContent = t('composer.template.' + option.value);
    $('custom-handlers').replaceChildren(...catalog.handlers.map(handler => element('li',
      t('composer.handler', {handler: handler.task_type, seconds: handler.seconds}))));
  };
  const render = () => {
    const state = model.getState();
    const editable = ['draft', 'validating', 'validated'].includes(state.phase);
    const canDraft = editable || state.phase === 'created';
    // Assigning even the same textarea value can reset selection in a browser.
    if ($('custom-json').value !== state.text) $('custom-json').value = state.text;
    $('custom-json').readOnly = !editable;
    $('custom-new').disabled = !canDraft;
    $('custom-validate').disabled = !editable || state.phase === 'validating' || !state.text.trim();
    $('custom-create').disabled = state.phase !== 'validated';
    const storageProblem = ['storage_invalid', 'storage_conflict'].includes(state.error?.reason);
    $('custom-retry').hidden = state.phase !== 'unknown';
    $('custom-retry').disabled = state.phase !== 'unknown' || storageProblem;
    $('custom-observe').hidden = !state.receipt;
    $('custom-observe').disabled = !state.receipt;
    text('custom-status', t(storageProblem && state.phase !== 'created' ?
      'composer.error.' + state.error.reason : 'composer.phase.' + state.phase));
    text('custom-storage', t(state.storage === 'unavailable' ? 'composer.storage.unavailable' : 'composer.storage.available'));
    $('custom-storage').hidden = state.storage !== 'unavailable';
    const errors = [];
    if (state.error) {
      errors.push(element('li', t('composer.error.' + state.error.reason)));
      if (state.error.status || state.error.name) errors.push(element('li', describeFailure(state.error)));
      for (const detail of state.error.details || []) errors.push(element('li', t('composer.error.detail', {
        location: detail.location.join('.'), type: detail.type, description: issue(detail.type)
      })));
    }
    $('custom-errors').replaceChildren(...errors);
    $('custom-errors').hidden = !errors.length;
    $('custom-preview').hidden = !state.preview;
    const signature = state.preview ? JSON.stringify(state.preview.definition) : null;
    if (signature !== previewSignature) {
      if (state.preview) drawDefinitionDag($('custom-preview-dag'), state.preview.definition);
      else $('custom-preview-dag').replaceChildren();
      previewSignature = signature;
    }
    text('custom-canonical', state.preview ? JSON.stringify(state.preview.definition, null, 2) : '');
    $('custom-operation').hidden = !state.pending;
    text('custom-key', state.pending?.key || '');
    text('custom-body', state.pending?.body || '');
    text('custom-receipt', state.receipt ? JSON.stringify(state.receipt, null, 2) : '');
    $('custom-receipt').hidden = !state.receipt;
    text('custom-recovery', state.recovery || '');
    $('custom-recovery').hidden = !state.recovery;
    renderCatalog(canDraft);
  };
  const model = createComposer({
    validate: body => request('/demo/custom/validate', 200, {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body
    }),
    submit: (body, key) => request('/demo/custom/runs', 201, {
      method: 'POST', headers: {'Content-Type': 'application/json', 'Idempotency-Key': key}, body
    }),
    storage, newKey, onChange: render
  });
  const observe = () => {
    const receipt = model.getState().receipt;
    if (receipt) {
      // A view callback cannot change the accepted receipt or retry a POST.
      try { onCreated?.(receipt); } catch { /* keep explicit Observe available */ }
    }
  };
  const send = async method => { if (await model[method]()) observe(); };
  const loadCatalog = async () => {
    if (catalogLoading) return false;
    catalogLoading = true;
    catalogError = null;
    render();
    try {
      const result = await request('/demo/custom/catalog', 200);
      if (!validCatalog(result)) throw {code: 'invalid_catalog', status: 200};
      catalog = JSON.parse(JSON.stringify(result));
      const selected = $('custom-template').value;
      const options = catalog.templates.map(template => {
        const option = element('option', t('composer.template.' + template.scenario));
        option.value = template.scenario;
        return option;
      });
      $('custom-template').replaceChildren(...options);
      $('custom-template').value = scenarios.includes(selected) ? selected : catalog.templates[0].scenario;
    } catch (error) { catalogError = safeFailure(error); }
    finally { catalogLoading = false; render(); }
    return !catalogError;
  };
  $('custom-json').addEventListener('input', () => model.edit($('custom-json').value));
  $('custom-new').addEventListener('click', () => model.newDraft(''));
  $('custom-load').addEventListener('click', () => {
    const template = catalog?.templates.find(item => item.scenario === $('custom-template').value);
    if (template) model.newDraft(JSON.stringify(template.definition, null, 2));
  });
  $('custom-validate').addEventListener('click', () => { void model.validate(); });
  $('custom-create').addEventListener('click', () => { void send('create'); });
  $('custom-retry').addEventListener('click', () => {
    if (!['storage_invalid', 'storage_conflict'].includes(model.getState().error?.reason)) void send('retry');
  });
  $('custom-observe').addEventListener('click', observe);
  $('custom-catalog-reload').addEventListener('click', () => { void loadCatalog(); });
  const ready = loadCatalog();
  return Object.freeze({model, ready, renderLanguage: render});
}
