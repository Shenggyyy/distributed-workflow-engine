// The server owns definition validation. This module owns only a draft and the
// identity of one explicit submission; it never starts Workers or polls Runs.
export const COMPOSER_STORAGE_KEY = 'dwe-demo-custom-submission-v1';
const BODY_LIMIT = 16384;
const KEY = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TOKEN = /^[A-Za-z][A-Za-z0-9_.:-]{0,79}$/;
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const clone = value => JSON.parse(JSON.stringify(value));

function freeze(value) {
  const stack = [value];
  while (stack.length) {
    const current = stack.pop();
    if (!object(current) && !Array.isArray(current)) continue;
    Object.freeze(current);
    stack.push(...Object.values(current));
  }
  return value;
}

function bodyDefinition(body) {
  if (typeof body !== 'string' || new TextEncoder().encode(body).length > BODY_LIMIT) return null;
  try {
    const value = JSON.parse(body);
    // Restoration checks the bounded canonical envelope, not handlers, policy,
    // dependency semantics or a second client-side version of the validator.
    if (!object(value) || value.schema_version !== 2 || typeof value.name !== 'string' ||
      !value.name.length || !Array.isArray(value.tasks) || !value.tasks.length || value.tasks.length > 12) return null;
    if (!value.tasks.every(task => object(task) && typeof task.task_id === 'string' && task.task_id.length &&
      typeof task.task_type === 'string' && task.task_type.length && Array.isArray(task.depends_on) &&
      task.depends_on.every(parent => typeof parent === 'string') && object(task.execution))) return null;
    return value;
  } catch { return null; }
}

function receipt(value) {
  return object(value) && Object.keys(value).length === 3 && value.scenario === 'custom' &&
    typeof value.run_id === 'string' && UUID.test(value.run_id) &&
    typeof value.workflow_version_id === 'string' && UUID.test(value.workflow_version_id) ? clone(value) : null;
}

function record(raw) {
  // A canonical 16 KiB body can nearly double when escaped inside this record.
  if (typeof raw !== 'string' || raw.length > 40960 || new TextEncoder().encode(raw).length > 40960) return null;
  try {
    const value = JSON.parse(raw);
    if (!object(value) || value.version !== 1 || typeof value.key !== 'string' || !KEY.test(value.key) ||
      !bodyDefinition(value.body)) return null;
    if (Object.hasOwn(value, 'receipt') && !receipt(value.receipt)) return null;
    return {version: 1, key: value.key, body: value.body, ...(value.receipt ? {receipt: receipt(value.receipt)} : {})};
  } catch { return null; }
}

function safeError(reason, error = {}) {
  const result = {reason};
  for (const key of ['code', 'name']) {
    if (typeof error?.[key] === 'string' && TOKEN.test(error[key])) result[key] = error[key];
  }
  if (Number.isInteger(error?.status) && error.status >= 100 && error.status <= 599) result.status = error.status;
  if (Array.isArray(error?.details)) {
    result.details = error.details.slice(0, 20).filter(detail => object(detail) &&
      typeof detail.type === 'string' && TOKEN.test(detail.type) && Array.isArray(detail.location) &&
      detail.location.length <= 16 && detail.location.every(part =>
        (typeof part === 'string' && part.length <= 64 && /^[A-Za-z0-9_.-]+$/.test(part)) ||
        (Number.isInteger(part) && part >= 0 && part <= 1000)))
      .map(detail => ({type: detail.type, location: [...detail.location]}));
  }
  return result;
}

export function createComposer({validate: validateRequest, submit, newKey, storage, onChange} = {}) {
  let state = {phase: 'draft', text: '', revision: 0, preview: null, pending: null,
    receipt: null, error: null, storage: 'available', recovery: null};
  let validationSequence = 0;
  const getState = () => freeze(clone(state));
  const publish = () => {
    // Observers cannot change a transport result or mutate internal state.
    try { onChange?.(getState()); } catch { /* observer isolation */ }
  };
  const readStorage = () => {
    try {
      const store = typeof storage === 'function' ? storage() : storage;
      if (!store) throw new TypeError();
      const raw = store.getItem(COMPOSER_STORAGE_KEY);
      state.storage = 'available';
      return {store, raw};
    } catch {
      state.storage = 'unavailable';
      return null;
    }
  };
  const same = (a, b) => a?.key === b?.key && a?.body === b?.body;
  const persist = confirmed => {
    const saved = readStorage();
    if (!saved) return true; // Visible memory-only fallback, not reload recovery.
    if (saved.raw !== null) {
      const previous = record(saved.raw);
      if (!previous || (!previous.receipt && !same(previous, state.pending))) {
        state.error = safeError(previous ? 'storage_conflict' : 'storage_invalid');
        state.recovery = String(saved.raw);
        return false;
      }
    }
    try {
      saved.store.setItem(COMPOSER_STORAGE_KEY, JSON.stringify({version: 1, ...state.pending,
        ...(confirmed ? {receipt: confirmed} : {})}));
    } catch { state.storage = 'unavailable'; }
    return true;
  };
  const saved = readStorage();
  if (saved && saved.raw !== null) {
    const previous = record(saved.raw);
    if (!previous) {
      state.phase = 'blocked';
      state.error = safeError('storage_invalid');
      state.recovery = String(saved.raw);
    } else {
      state.pending = {key: previous.key, body: previous.body};
      state.text = previous.body;
      state.receipt = previous.receipt || null;
      state.phase = previous.receipt ? 'created' : 'unknown';
    }
  }

  const editable = () => ['draft', 'validating', 'validated'].includes(state.phase);
  const edit = text => {
    if (!editable() || typeof text !== 'string') return false;
    validationSequence++;
    state = {...state, phase: 'draft', text, revision: state.revision + 1,
      preview: null, error: null, recovery: null};
    publish();
    return true;
  };
  const newDraft = text => {
    if ((!editable() && state.phase !== 'created') || typeof text !== 'string') return false;
    if (state.phase === 'created') {
      const stored = readStorage();
      if (stored && stored.raw !== null) {
        const previous = record(stored.raw);
        if (!previous || !previous.receipt) {
          state = {...state, phase: 'blocked', recovery: String(stored.raw),
            error: safeError(previous ? 'storage_conflict' : 'storage_invalid')};
          publish();
          return false;
        }
        if (same(previous, state.pending)) {
          try { stored.store.removeItem(COMPOSER_STORAGE_KEY); }
          catch { state.storage = 'unavailable'; }
        }
      }
    }
    state = {...state, phase: 'draft', pending: null, receipt: null};
    return edit(text);
  };
  const validate = async () => {
    if (!editable() || state.phase === 'validating') return false;
    const revision = state.revision, sequence = ++validationSequence, text = state.text;
    state = {...state, phase: 'validating', preview: null, error: null};
    publish();
    const current = () => sequence === validationSequence && revision === state.revision && state.phase === 'validating';
    let result;
    try { result = await validateRequest(text); }
    catch (error) {
      if (!current()) return false;
      state = {...state, phase: 'draft', error: safeError('validation_failed', error)};
      publish();
      return false;
    }
    if (!current()) return false;
    try {
      const definition = bodyDefinition(JSON.stringify(result?.definition));
      const layers = clone(result?.layers);
      const keys = definition?.tasks.map(task => task.task_id);
      const flat = Array.isArray(layers) && layers.every(row => Array.isArray(row) && row.length) ? layers.flat() : [];
      if (!definition || !flat.length || flat.length !== keys.length || new Set(flat).size !== keys.length ||
        !flat.every(key => typeof key === 'string' && keys.includes(key))) throw new TypeError();
      const depth = new Map(layers.flatMap((row, index) => row.map(key => [key, index])));
      // Check coherence of the server's returned preview, not a locally generated
      // validation result: every reported dependency must be in an earlier layer.
      if (!definition.tasks.every(task => task.depends_on.every(parent =>
        depth.has(parent) && depth.get(parent) < depth.get(task.task_id)))) throw new TypeError();
      state = {...state, phase: 'validated', preview: freeze({definition, layers})};
    } catch {
      state = {...state, phase: 'draft', error: safeError('invalid_preview')};
    }
    const accepted = state.phase === 'validated';
    publish();
    return accepted;
  };
  const send = async () => {
    if (!persist(null)) {
      state.phase = state.receipt ? 'created' : 'unknown';
      publish();
      return false;
    }
    publish();
    const {body, key} = state.pending;
    let value;
    try { value = await submit(body, key); }
    catch (error) {
      state = {...state, phase: 'unknown', error: safeError('submission_unknown', error)};
      publish();
      return false;
    }
    const confirmed = receipt(value);
    if (!confirmed) {
      state = {...state, phase: 'unknown', error: safeError('invalid_receipt')};
      publish();
      return false;
    }
    state = {...state, phase: 'created', receipt: freeze(confirmed), error: null};
    persist(confirmed);
    publish();
    return true;
  };
  const create = async () => {
    if (state.phase !== 'validated') return false;
    state.phase = 'submitting'; // Lock before invoking even the injected key factory.
    let key;
    try {
      key = newKey();
      if (typeof key !== 'string' || !KEY.test(key)) throw new TypeError();
    } catch {
      state = {...state, phase: 'validated', error: safeError('key_invalid')};
      publish();
      return false;
    }
    state = {...state, pending: freeze({key, body: JSON.stringify(state.preview.definition)}), error: null};
    return send();
  };
  const retry = async () => {
    if (state.phase !== 'unknown' || !state.pending) return false;
    state = {...state, phase: 'submitting', error: null};
    return send();
  };
  return Object.freeze({getState, edit, validate, create, retry, newDraft});
}
