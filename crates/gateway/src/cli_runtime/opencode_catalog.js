// Native V2 executor policy and registry attestation. Imports are provided by
// the OpenCode host; this package has no installed dependencies or model tools.
import { Effect, Scope, Semaphore } from 'effect';
import { Error as ToolError } from '@opencode/plugin/promise/tool';
export default {
  id: '__PIONEER_CATALOG_ID__',
  effect: context => Effect.gen(function* () {
    const guarded = new WeakSet();
    const correlated = new WeakSet();
    const pluginScope = yield* Scope.Scope;
    const catalogLock = Semaphore.makeUnsafe(1);
    let correlationTransform;
    const managed = tool => tool.id.startsWith('pioneer_') && tool.options?.namespace === 'pioneer';
    const wrapManaged = editor => {
      for (const current of editor.list().filter(managed)) {
        editor.update(current.id, tool => {
          if (correlated.has(tool.execute)) return;
          const execute = tool.execute;
          // Tool.runtime decodes the public input before calling this executor.
          // Keep the native executor (including permission.assert) and Context.
          // Never change input: native lifecycle events use that original value.
          tool.execute = (input, context) => {
            if (![context.sessionID, context.messageID, context.id].every(value => typeof value === 'string' && value.length > 0)) {
              return Effect.fail(new ToolError({ message: 'Pioneer MCP executor context is missing native identity' }));
            }
            return execute({
              __pioneer_mcp: { sessionID: context.sessionID, messageID: context.messageID, id: context.id },
              arguments: input,
            }, context);
          };
          correlated.add(tool.execute);
        });
      }
    };
    // Wrap the executor, rather than acting on Tool.Called notifications.
    // Native Tool.execute decodes input and then calls this Effect. Tool.Error
    // is its typed failure channel and becomes a settled failed tool result.
    yield* context.tool.transform(editor => {
      editor.update('shell', tool => {
        const execute = tool.execute;
        tool.execute = (input, context) => input.background === true
          ? Effect.fail(new ToolError({
              message: 'Pioneer does not support native background shell. Run the command in the foreground or use Pioneer task tools.',
              metadata: { pioneerBackgroundShellRejected: true },
            }))
          : execute(input, context);
        guarded.add(tool.execute);
      });
    });
    yield* context.rpc.register(
      {
        id: '__PIONEER_CATALOG_ID__',
        methods: {
          catalog: {
            input: { type: 'object', additionalProperties: false },
            output: {
              type: 'array',
              items: {
                type: 'object',
                required: ['id', 'namespace', 'codemode', 'input'],
                properties: {
                  id: { type: 'string' },
                  namespace: { type: 'string' },
                  codemode: { type: 'boolean' },
                  input: { type: 'object' },
                },
                additionalProperties: false,
              },
            },
          },
        },
        events: {},
      },
      {
        catalog: () => Effect.gen(function* () {
          // State transforms replay in registration order. MCP discovery can
          // register after this plugin. Put our transform last at attestation;
          // native reloads then replay it after the MCP registration transform.
          let tools = yield* context.tool.list();
          if (tools.filter(managed).some(tool => !correlated.has(tool.execute))) {
            yield* Effect.gen(function* () {
              const previous = correlationTransform;
              correlationTransform = yield* context.tool.transform(wrapManaged).pipe(
                Effect.provideService(Scope.Scope, pluginScope),
              );
              // Install before disposing: keep existing executors wrapped even
              // if another fiber reads the registry during replacement.
              if (previous) yield* previous.dispose;
            }).pipe(Effect.uninterruptible);
            tools = yield* context.tool.list();
          }
          // Admission must fail if a missing/overwritten transform lost the
          // shell barrier. Registry transforms replay after native reloads.
          const shell = tools.find(tool => tool.id === 'shell');
          if (!shell || !guarded.has(shell.execute)) {
            return yield* Effect.die(new Error('Pioneer foreground-shell policy is not installed'));
          }
          if (tools.filter(managed).some(tool => !correlated.has(tool.execute))) {
            return yield* Effect.die(new Error('Pioneer MCP correlation wrapper is not installed'));
          }
          return tools.filter(tool => tool.id.startsWith('pioneer_')).map(tool => ({
            id: tool.id,
            namespace: tool.options?.namespace ?? '',
            codemode: tool.options?.codemode !== false,
            input: tool.input,
          }));
        }).pipe(catalogLock.withPermit),
      },
    );
  }),
};
