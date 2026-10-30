"""Execute the production poller against DOM boundary doubles using Node's built-ins."""

import json
import shutil
import subprocess

import pytest

from shoko_linker.dashboard import _DASHBOARD_JS


def test_poller_preserves_keyed_nested_disclosures_before_comparing_html() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is unavailable for the dashboard JavaScript regression")
    script = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
const timers = [];
const requested = [];
let replacements = 0;
let current, incoming;
function detail(key, open = false) { return {dataset: {disclosure: key}, open}; }
function section(key, content, children = []) {
  return {
    dataset: {live: key}, tagName: 'SECTION', content, children,
    querySelectorAll(selector) {
      assert.equal(selector, 'details[data-disclosure]'); return this.children;
    },
    get outerHTML() {
      return JSON.stringify([this.content, this.children.map(d => [d.dataset.disclosure, d.open])]);
    },
    replaceWith(fresh) { replacements++; current.set(key, fresh); }
  };
}
const setupPanel = {dataset: {loading: 'false', setupDigest: 'unchanged'}};
const setup = {
  dataset: {live: 'setup'}, tagName: 'DIV', userChoice: 'keep my selected library',
  querySelector(selector) { return selector === '[data-setup-digest]' ? setupPanel : null; },
  querySelectorAll(selector) { assert.equal(selector, 'button'); return []; },
  replaceWith() { throw Error('Setup choices must not be replaced'); }
};
const form = {selected: 'user editing settings'};
const notice = {hidden: true, textContent: ''};
const document = {
      addEventListener() {},
  getElementById() { return notice; },
  querySelector(selector) { assert.equal(selector, '[data-refresh-url]'); return null; },
  querySelectorAll(selector) {
    if (selector === 'table[data-table]') return [];
    assert.equal(selector, '[data-live]'); return [...current.values()];
  }
};
const window = {addEventListener() {},
      location: {pathname: '/issues', search: '?view=exclusions&page=2'},
  setTimeout(fn) { timers.push(fn); }};
class DOMParser {
  parseFromString() {
    return {querySelector(selector) {
      if (selector === '[data-refresh-url]') return null;
      if (selector === 'input[name="csrf_token"]') return null;
      const key = selector.match(/^\[data-live="(.+)"\]$/)[1];
      return incoming.get(key) || null;
    }};
  }
}
const fetch = async (url) => { requested.push(url); return {ok: true, text: async () => ''}; };
current = new Map([
  ['metrics', section('metrics', '36')], ['setup', setup],
  ['tracked-files', section('tracked-files', 'old reasons',
    [detail('waiting-files', true), detail('review-files', false)])],
  ['action-history', section('action-history', 'old actions',
    [detail('action-file:1', true), detail('action-file:removed', true)])]
]);
vm.runInNewContext(PRODUCTION_SCRIPT, {document, window, DOMParser, fetch, AbortSignal});
(async () => {
  incoming = new Map([
    ['metrics', section('metrics', '36')], ['setup', setup],
    ['tracked-files', section('tracked-files', 'new reasons',
      [detail('review-files', true), detail('waiting-files', false)])],
    ['action-history', section('action-history', 'new actions',
      [detail('action-file:1', false), detail('action-file:new', false)])]
  ]);
  await timers.shift()();
  assert.equal(notice.textContent, '');
  assert.equal(replacements, 2);
  const files = current.get('tracked-files').children;
  assert.deepEqual(files.map(d => [d.dataset.disclosure, d.open]),
    [['review-files', false], ['waiting-files', true]]);
  assert.deepEqual(current.get('action-history').children.map(d => d.open), [true, false]);
  // Fresh HTML has the original default disclosure state, but unchanged data.
  incoming = new Map([
    ['metrics', section('metrics', '36')], ['setup', setup],
    ['tracked-files', section('tracked-files', 'new reasons',
      [detail('review-files', true), detail('waiting-files', false)])],
    ['action-history', section('action-history', 'new actions',
      [detail('action-file:1', false), detail('action-file:new', false)])]
  ]);
  await timers.shift()();
  assert.equal(replacements, 2, 'Open state must be restored BEFORE comparing outerHTML');
  assert.equal(notice.textContent, '');
  assert.equal(current.get('setup'), setup);
  assert.equal(setup.userChoice, 'keep my selected library');
  assert.equal(form.selected, 'user editing settings');
  assert.deepEqual(requested, ['/issues?view=exclusions&page=2', '/issues?view=exclusions&page=2']);
})().catch(error => { console.error(error); process.exitCode = 1; });
"""
    script = script.replace("PRODUCTION_SCRIPT", json.dumps(_DASHBOARD_JS))
    result = subprocess.run(  # noqa: S603 - resolved Node, fixed script, no shell
        [node, "-"],
        input=script,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
