const assert = require('node:assert/strict');
const vm = require('node:vm');
let stored = {normal: '{"filename":340}', corrupt: '{oops', oversized: ' '.repeat(513),
  hostile: '{"filename":"900px;evil","type":-900,"status":1e300,"unknown":12}'}[storageMode];
const localStorage = {
  getItem(key) { assert.equal(key, 'shoko-linker:table-widths:v1');
    if (storageMode === 'denied') throw Error('denied'); return stored; },
  setItem(key, value) { assert.equal(key, 'shoko-linker:table-widths:v1');
    if (storageMode === 'denied') throw Error('denied'); stored = value; }
};
function session(route) {
  const listeners = new Map(), windowListeners = new Map(), timers = [], requests = [];
  let incoming, replacements = 0;
  function register(map, name, fn) {
    assert(!map.has(name), 'Listeners must be registered only once: ' + name);
    map.set(name, fn);
  }
  const classes = new Set();
  const document = {activeElement: null, hidden: false,
    documentElement: {classList: {add: name => classes.add(name), remove: name => classes.delete(name)}},
    addEventListener: (name, fn) => register(listeners, name, fn),
    getElementById: () => notice,
    querySelector: () => null,
    querySelectorAll(selector) {
      if (['time[data-compact-time]', '[data-timezone]'].includes(selector)) return [];
      if (selector === 'table[data-table]') return [current.table];
      if (selector === '[data-live]') return [current, setup];
      throw Error(selector);
    }
  };
  const notice = {hidden: true, textContent: ''};
  const settingsForm = {value: 'unsaved settings'};
  const setup = {dataset: {live: 'setup'}, selected: 'my selected root',
    querySelector: selector => selector === '[data-setup-digest]' ?
      {dataset: {loading: 'false', setupDigest: 'same'}} : null,
    querySelectorAll: () => [], replaceWith() { throw Error('Setup choice lost'); }};
  function section(content) {
    const container = {scrollLeft: 0};
    const columns = ['type', 'filename', 'path', 'library', 'status',
      ...(route === '/history' ? ['requested', 'updated'] : []), 'links'];
    const cols = columns.map(key => ({dataset: {column: key}, style: {}}));
    const table = {dataset: {table: 'files'}, style: {},
      closest: selector => { assert.equal(selector, '.table-scroll'); return container; },
      querySelectorAll: selector => selector === 'col[data-column]' ? cols : handles};
    const handles = columns.map(key => ({dataset: {resize: key}, attrs: {}, captured: null,
      closest: selector => selector === '[data-resize]' ? handles.find(h => h.dataset.resize === key) : table,
      setAttribute(name, value) { this.attrs[name] = String(value); },
      focus(options) { assert.equal(options.preventScroll, true); document.activeElement = this; },
      setPointerCapture(id) { this.captured = id; },
      releasePointerCapture(id) { assert.equal(this.captured, id); this.captured = null; }
    }));
    const disclosures = [{dataset: {disclosure: 'outer'}, open: false},
      {dataset: {disclosure: 'inner'}, open: true}];
    return {dataset: {live: 'table'}, tagName: 'SECTION', table, handles, container, cols, disclosures,
      contains: value => handles.includes(value),
      querySelectorAll: selector => {
        if (selector === 'table[data-table]') return [table];
        if (selector === 'details[data-disclosure]') return disclosures;
        throw Error(selector);
      },
      get outerHTML() { return JSON.stringify([content, table.style, cols, handles.map(h => h.attrs),
        disclosures.map(d => d.open)]); },
      replaceWith(fresh) { replacements++; current = fresh; }
    };
  }
  let current = section('original');
  const window = {localStorage, location: {pathname: route, search: '?view=exclusions&page=2'},
    addEventListener: (name, fn) => register(windowListeners, name, fn),
    setTimeout: fn => timers.push(fn)};
  class DOMParser { parseFromString() { const fresh = incoming; return {querySelectorAll() { return []; },
    querySelector(selector) {
      if (selector === '[data-live="metrics"]') return {};
      if (selector === '[data-live="table"]') return fresh;
      if (selector === '[data-live="setup"]') return setup;
      return null;
    }
  }; }}
  const fetch = async url => { requests.push(url); return {ok: true, text: async () => ''}; };
  vm.runInNewContext(production, {document, window, DOMParser, fetch, AbortSignal});
  function event(name, values = {}) {
    let prevented = false;
    listeners.get(name)({target: current.handles[1], pointerId: 7, button: 0, clientX: 100,
      preventDefault() { prevented = true; }, ...values});
    return prevented;
  }
  return {get current() { return current; }, get replacements() { return replacements; },
    document, event, classes, requests, windowListeners, settingsForm, setup,
    async poll(content) { incoming = section(content); await timers.shift()();
      assert.equal(notice.textContent, ''); assert.equal(timers.length, 1); }
  };
}
(async () => {
  const s = session('/issues');
  const initial = storageMode === 'normal' ? 340 : 260;
  assert.equal(s.current.cols[1].style.width, initial + 'px');
  if (storageMode === 'hostile') {
    assert.equal(s.current.cols[0].style.width, '45px');
    assert.equal(s.current.cols[4].style.width, '1200px');
  }
  s.current.container.scrollLeft = 190;
  s.current.disclosures[0].open = true;
  s.current.disclosures[1].open = false;
  s.current.handles[1].focus({preventScroll: true});
  await s.poll('original');
  assert.equal(s.replacements, 0, 'Normalization before comparison prevents false replacements');
  assert(s.event('keydown', {key: 'ArrowRight'}));
  assert.equal(s.current.cols[1].style.width, (initial + 10) + 'px');
  assert.equal(s.current.handles[1].attrs['aria-valuenow'], String(initial + 10));
  assert(s.event('pointerdown'));
  assert.equal(s.current.handles[1].captured, 7);
  assert(s.classes.has('resizing-columns'));
  s.event('pointermove', {pointerId: 8, clientX: 400});
  assert.equal(s.current.cols[1].style.width, (initial + 10) + 'px');
  assert(s.event('pointermove', {clientX: 160}));
  assert.equal(s.current.cols[1].style.width, (initial + 70) + 'px');
  await s.poll('obsolete while dragging');
  await s.poll('changed');
  assert.equal(s.replacements, 0, 'Active pointer prevents replacement');
  s.event('pointerup', {pointerId: 8});
  assert.equal(s.replacements, 0);
  s.event('pointerup');
  assert.equal(s.replacements, 1, 'Latest pending response applied immediately on release');
  assert.equal(s.current.cols[1].style.width, (initial + 70) + 'px');
  assert.equal(s.current.container.scrollLeft, 190);
  assert.equal(s.document.activeElement, s.current.handles[1]);
  assert.deepEqual(s.current.disclosures.map(d => d.open), [true, false]);
  await s.poll('changed');
  assert.equal(s.replacements, 1);
  for (const clientX of [-100000, 100000]) {
    s.event('pointerdown'); s.event('pointermove', {clientX}); s.event('pointercancel');
    assert(!s.classes.has('resizing-columns'));
  }
  assert.equal(s.current.cols[1].style.width, '900px');
  s.event('keydown', {key: 'ArrowRight'});
  assert.equal(s.current.cols[1].style.width, '900px');
  s.event('pointerdown'); s.event('pointermove', {clientX: -100000});
  s.event('lostpointercapture');
  s.event('keydown', {key: 'ArrowLeft'});
  assert.equal(s.current.cols[1].style.width, '100px');
  for (const kind of ['blur', 'pagehide', 'visibilitychange']) {
    s.event('pointerdown'); await s.poll(kind);
    if (kind === 'visibilitychange') { s.document.hidden = true; s.event(kind); }
    else s.windowListeners.get(kind)();
    assert(!s.classes.has('resizing-columns'));
    assert.equal(s.current.handles[1].captured, null);
  }
  assert.equal(s.setup.selected, 'my selected root');
  assert.equal(s.settingsForm.value, 'unsaved settings');
  assert(s.requests.every(url => url === '/issues?view=exclusions&page=2'));
  if (storageMode !== 'denied') {
    assert.deepEqual(Object.keys(JSON.parse(stored)), ['type', 'filename', 'path', 'library', 'status',
      'requested', 'updated', 'links']);
    const navigated = session('/history');
    assert.equal(navigated.current.cols[5].style.width, '115px');
    assert.equal(navigated.current.cols[6].style.width, '115px');
    assert.equal(navigated.current.cols[7].style.width, '70px');
    assert(navigated.event('keydown', {target: navigated.current.handles[5], key: 'ArrowRight'}));
    assert.equal(navigated.current.cols[5].style.width, '125px');
    await navigated.poll('history updated');
    assert.equal(navigated.current.cols[5].style.width, '125px');
    assert.equal(navigated.document.activeElement, null);
    assert.equal(navigated.current.cols[1].style.width, '100px');
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
