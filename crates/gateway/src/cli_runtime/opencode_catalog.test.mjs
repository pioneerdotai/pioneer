// Prepared regression; run only after code acceptance permits tests.
// node --experimental-vm-modules --test opencode_catalog.test.mjs
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import { createContext, SourceTextModule, SyntheticModule } from 'node:vm';

// The plugin uses the host's Effect runtime. This tiny lazy-effect double lets
// the policy boundary be exercised without OpenCode, a model or subprocesses.
class HostEffect {
  constructor(run) { this.run = run; }
  *[Symbol.iterator]() { return yield this; }
}
const Effect = {
  gen: body => new HostEffect(async () => {
    const generator = body();
    let step = generator.next();
    while (!step.done) step = generator.next(await step.value.run());
    return step.value;
  }),
  fail: error => new HostEffect(async () => { throw error; }),
  die: error => new HostEffect(async () => { throw error; }),
};
class ToolError extends Error {
  constructor({ message, metadata }) {
    super(message);
    this._tag = 'Tool.Error';
    this.metadata = metadata;
  }
}
const effect = value => new HostEffect(async () => value());

async function installedPolicy() {
  const calls = [];
  const native = input => effect(() => { calls.push(input); return { content: 'native output' }; });
  const tools = new Map([
    ['shell', { id: 'shell', execute: native }],
    ['pioneer_task', { id: 'pioneer_task', execute: native, options: { namespace: 'pioneer', codemode: false }, input: { type: 'object' } }],
  ]);
  let transform;
  let rpc;
  const editor = { update: (id, update) => { if (tools.has(id)) update(tools.get(id)); } };
  const host = {
    tool: {
      transform: callback => effect(() => { transform = callback; callback(editor); }),
      list: () => effect(() => [...tools.values()]),
    },
    rpc: { register: (_definition, handlers) => effect(() => { rpc = handlers; }) },
  };
  const context = createContext({});
  const module = new SourceTextModule(await readFile(new URL('./opencode_catalog.js', import.meta.url), 'utf8'), { context });
  await module.link(specifier => {
    const modules = { effect: { Effect }, '@opencode/plugin/promise/tool': { Error: ToolError } };
    assert.ok(Object.hasOwn(modules, specifier));
    const exports = modules[specifier];
    return new SyntheticModule(Object.keys(exports), function () {
      for (const [key, value] of Object.entries(exports)) this.setExport(key, value);
    }, { context });
  });
  await module.evaluate();
  await module.namespace.default.effect(host).run();
  return { calls, tools, native, catalog: () => rpc.catalog().run(), reload: () => transform(editor) };
}

test('background input never calls the native executor; foreground and Pioneer tasks remain callable', async () => {
  const host = await installedPolicy();
  await assert.rejects(host.tools.get('shell').execute({ command: 'side effect', background: true }, {}).run(), error => {
    assert.equal(error._tag, 'Tool.Error');
    assert.equal(error.metadata.pioneerBackgroundShellRejected, true);
    return true;
  });
  assert.deepEqual(host.calls, []);
  assert.deepEqual(await host.tools.get('shell').execute({ command: 'foreground' }, {}).run(), { content: 'native output' });
  await host.tools.get('shell').execute({ command: 'foreground explicit', background: false }, {}).run();
  await host.tools.get('pioneer_task').execute({ background: true }, {}).run();
  assert.equal(host.calls.length, 3);
  assert.deepEqual(Array.from(await host.catalog(), tool => tool.id), ['pioneer_task']);
});

test('catalog refuses a replaced executor and the policy is reapplied on native registry reload', async () => {
  const host = await installedPolicy();
  host.tools.get('shell').execute = host.native;
  await assert.rejects(host.catalog(), /foreground-shell policy is not installed/);
  host.reload();
  await assert.rejects(host.tools.get('shell').execute({ background: true }, {}).run(), ToolError);
  assert.deepEqual(host.calls, []);
  assert.equal((await host.catalog()).length, 1);
});
