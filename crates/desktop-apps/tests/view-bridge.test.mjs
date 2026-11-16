// Prepared for: node --test crates/desktop-apps/tests/view-bridge.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../src/view_bridge.js', import.meta.url), 'utf8');
function bridge(origin, topLevel = true) {
  const callbacks = [];
  const sent = [];
  const child = {};
  const window = {
    addEventListener(type, callback) { if (type === 'message') callbacks.push(callback); },
    ipc: { postMessage(body) { sent.push(JSON.parse(body)); } },
  };
  window.top = topLevel ? window : {};
  vm.runInNewContext(source.replace('__PIONEER_ORIGIN__', JSON.stringify(origin))
    .replace('__PIONEER_TOKEN__', JSON.stringify('private-view-token')), {
    window, location: { origin: 'http://localhost:3000' },
    document: { querySelector() { return { contentWindow: child }; } },
  });
  return { window, child, sent, send(source, origin, data) {
    for (const callback of callbacks) callback({ source, origin, data });
  } };
}
const request = { type: 'pioneer.view.setPlacement', id: 'request-1', placement: 'bottom' };

test('installed bridge admits only its App frame and never trusts a supplied native token', () => {
  const b = bridge(null);
  b.send({}, 'null', request);
  b.send(b.window, 'null', request);
  assert.equal(b.sent.length, 0);
  b.send(b.child, 'null', { ...request, token: 'forged-token' });
  assert.deepEqual(b.sent, [{ ...request, token: 'private-view-token' }]);
  b.send(b.child, 'null', { ...request, placement: 'close' });
  b.send(b.child, 'null', { ...request, type: 'host.pty.create' });
  assert.equal(b.sent.length, 1);
  const close = { type: 'pioneer.view.close', id: 'close-1' };
  b.send({}, 'null', close);
  assert.equal(b.sent.length, 1);
  b.send(b.child, 'null', close);
  assert.deepEqual(b.sent[1], { ...close, token: 'private-view-token' });
});

test('direct bridge requires self and its exact origin, and subframes get no forwarding bridge', () => {
  const b = bridge('http://localhost:3000');
  b.send(b.child, 'http://localhost:3000', request);
  b.send(b.window, 'http://evil.test', request);
  assert.equal(b.sent.length, 0);
  b.send(b.window, 'http://localhost:3000', request);
  assert.equal(b.sent.length, 1);
  const close = { type: 'pioneer.view.close', id: 'close-2' };
  b.send(b.child, 'http://localhost:3000', close);
  b.send(b.window, 'http://evil.test', close);
  assert.equal(b.sent.length, 1);
  b.send(b.window, 'http://localhost:3000', close);
  assert.deepEqual(b.sent[1], { ...close, token: 'private-view-token' });
  const frame = bridge('http://localhost:3000', false);
  frame.send(frame.window, 'http://localhost:3000', request);
  assert.equal(frame.sent.length, 0);
  const navigated = bridge('http://other-origin.test');
  navigated.send(navigated.window, 'http://localhost:3000', request);
  assert.equal(navigated.sent.length, 0);
});
