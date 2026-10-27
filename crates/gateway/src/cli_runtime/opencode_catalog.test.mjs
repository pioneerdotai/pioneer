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
  pipe(...steps) { return steps.reduce((value, step) => step(value), this); }
  *[Symbol.iterator]() { return yield this; }
}
const Effect = {
  gen: body => new HostEffect(async () => {
    const generator = body();
    let step = generator.next();
    while (!step.done) step = generator.next(await step.value.run());
    return step.value;
  }),
  // Scope/cancellation/locking are host-owned; this unit double does not prove
  // their runtime behavior or OpenCode's decoding/permission implementation.
  provideService: () => operation => operation,
  uninterruptible: operation => operation,
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

const Scope = { Scope: effect(() => ({})) };
const Semaphore = { makeUnsafe: () => ({ withPermit: operation => operation }) };
const nativeContext = { sessionID: 'ses_native', messageID: 'msg_native', id: 'call_native' };

async function installedPolicy({ lateDiscovery = false } = {}) {
  const calls = [];
  const native = (input, context) => effect(() => {
    calls.push({ input, context });
    return { content: 'native output' };
  });
  const schema = { type: 'object', properties: { background: { type: 'boolean' } } };
  const pioneer = id => ({ id, execute: native, options: { namespace: 'pioneer', codemode: false }, input: schema });
  let discovered = [pioneer('pioneer_task')];
  let tools = new Map();
  const transforms = [];
  let rpc;
  const rebuild = () => {
    // Mirrors the verified State.create contract: rebuild from a fresh base,
    // replay registered transforms in order; update copies the tool record.
    tools = new Map([['shell', { id: 'shell', execute: native }]]);
    const editor = {
      list: () => [...tools.values()],
      add: tool => tools.set(tool.id, { ...tool, options: tool.options && { ...tool.options } }),
      update: (id, update) => {
        if (!tools.has(id)) return;
        const tool = { ...tools.get(id), options: tools.get(id).options && { ...tools.get(id).options } };
        update(tool);
        tools.set(id, tool);
      },
    };
    for (const transform of transforms) transform(editor);
  };
  const register = callback => {
    transforms.push(callback);
    rebuild();
    return { dispose: effect(() => { const index = transforms.indexOf(callback); if (index >= 0) transforms.splice(index, 1); rebuild(); }) };
  };
  const host = {
    tool: {
      transform: callback => effect(() => register(callback)),
      list: () => effect(() => [...tools.values()]),
    },
    rpc: { register: (_definition, handlers) => effect(() => { rpc = handlers; }) },
  };
  const discover = () => register(editor => { for (const tool of discovered) editor.add(tool); });
  const context = createContext({});
  const module = new SourceTextModule(await readFile(new URL('./opencode_catalog.js', import.meta.url), 'utf8'), { context });
  await module.link(specifier => {
    const modules = { effect: { Effect, Scope, Semaphore }, '@opencode/plugin/promise/tool': { Error: ToolError } };
    assert.ok(Object.hasOwn(modules, specifier));
    const exports = modules[specifier];
    return new SyntheticModule(Object.keys(exports), function () {
      for (const [key, value] of Object.entries(exports)) this.setExport(key, value);
    }, { context });
  });
  await module.evaluate();
  await module.namespace.default.effect(host).run();
  if (!lateDiscovery) discover(); // Native MCP transform registers after the plugin.
  return {
    calls, get tools() { return tools; }, native, schema, pioneer,
    catalog: () => rpc.catalog().run(),
    discover,
    reload: (next = discovered) => { discovered = next; rebuild(); },
    replaceShell: () => register(editor => editor.update('shell', tool => { tool.execute = native; })),
  };
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
  await host.catalog();
  await host.tools.get('pioneer_task').execute({ background: true }, nativeContext).run();
  assert.equal(host.calls.length, 3);
  assert.deepEqual(Array.from(await host.catalog(), tool => tool.id), ['pioneer_task']);
});

test('catalog refuses a replaced executor and the policy is reapplied on native registry reload', async () => {
  const host = await installedPolicy();
  const replacement = host.replaceShell();
  await assert.rejects(host.catalog(), /foreground-shell policy is not installed/);
  await replacement.dispose.run();
  host.reload();
  await assert.rejects(host.tools.get('shell').execute({ background: true }, {}).run(), ToolError);
  assert.deepEqual(host.calls, []);
  assert.equal((await host.catalog()).length, 1);
});

test('owned MCP wrapper uses Context and leaves public input and schema intact after late discovery and reload', async () => {
  const host = await installedPolicy({ lateDiscovery: true });
  assert.deepEqual(Array.from(await host.catalog()), []);
  host.discover();
  const catalog = await host.catalog();
  assert.deepEqual(JSON.parse(JSON.stringify(catalog[0].input)), host.schema);
  const input = {
    background: true,
    __pioneer_mcp: { sessionID: 'forged', messageID: 'forged', id: 'forged' },
    arguments: { legitimate: true },
  };
  const before = JSON.stringify(input);
  await host.tools.get('pioneer_task').execute(input, nativeContext).run();
  assert.equal(JSON.stringify(input), before);
  assert.deepEqual(JSON.parse(JSON.stringify(host.calls[0].input)), {
    __pioneer_mcp: nativeContext, arguments: input,
  });
  assert.equal(host.calls[0].context, nativeContext);
  host.reload([host.pioneer('pioneer_task'), host.pioneer('pioneer_late')]);
  await host.tools.get('pioneer_late').execute({ background: false }, { ...nativeContext, id: 'late_call' }).run();
  assert.equal(host.calls[1].input.__pioneer_mcp.id, 'late_call');
  assert.deepEqual(Array.from(await host.catalog(), tool => tool.id), ['pioneer_task', 'pioneer_late']);
});

test('model fields cannot supply missing executor identity', async () => {
  const host = await installedPolicy();
  await host.catalog();
  await assert.rejects(host.tools.get('pioneer_task').execute({
    __pioneer_mcp: nativeContext, sessionID: 'model', messageID: 'model', id: 'model',
  }, {}).run(), /executor context is missing native identity/);
  assert.deepEqual(host.calls, []);
});
