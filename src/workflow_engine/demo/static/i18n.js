import {messages} from './messages.js';

export const LANGUAGE_KEY = 'dwe.demo.language';
export const LANGUAGES = Object.freeze(['en', 'zh-CN']);

// The storage getter itself can throw (for example, in a restricted iframe).
export function initialLanguage(languages = [], storage = () => null) {
  try {
    const saved = storage()?.getItem(LANGUAGE_KEY);
    if (LANGUAGES.includes(saved)) return saved;
  } catch {
    // Preferences are optional; viewing engine evidence must still work.
  }
  return /^zh(?:-|$)/i.test(languages[0] || '') ? 'zh-CN' : 'en';
}

export function translate(language, key, params = {}) {
  const catalog = messages[LANGUAGES.includes(language) ? language : 'en'];
  if (!Object.hasOwn(catalog, key)) throw new Error(`Unknown translation key: ${key}`);
  return catalog[key].replace(/\{(\w+)\}/g, (_, name) => {
    if (!Object.hasOwn(params, name)) throw new Error(`Missing translation parameter: ${name}`);
    return String(params[name]);
  });
}

// This object owns language only. It has no Run, transport, clock or engine state.
export function createI18n({languages = [], storage = () => null} = {}) {
  let language = initialLanguage(languages, storage);
  return {
    get language() { return language; },
    t(key, params) { return translate(language, key, params); },
    select(next) {
      if (!LANGUAGES.includes(next)) return false;
      const changed = language !== next;
      language = next;
      try {
        storage()?.setItem(LANGUAGE_KEY, next);
      } catch {
        // Keep the in-memory choice even if persistence is denied or full.
      }
      return changed;
    }
  };
}
