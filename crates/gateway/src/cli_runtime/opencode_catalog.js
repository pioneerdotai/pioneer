// Native V2 policy and read-only registry attestation. Imports are provided by
// the OpenCode host; this package has no installed dependencies or model tools.
import { Effect } from 'effect';
import { Error as ToolError } from '@opencode/plugin/promise/tool';
export default {
  id: '__PIONEER_CATALOG_ID__',
  effect: context => Effect.gen(function* () {
    const guarded = new WeakSet();
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
          const tools = yield* context.tool.list();
          // Admission must fail if a missing/overwritten transform lost the
          // shell barrier. Registry transforms replay after native reloads.
          const shell = tools.find(tool => tool.id === 'shell');
          if (!shell || !guarded.has(shell.execute)) {
            return yield* Effect.die(new Error('Pioneer foreground-shell policy is not installed'));
          }
          return tools.filter(tool => tool.id.startsWith('pioneer_')).map(tool => ({
            id: tool.id,
            namespace: tool.options?.namespace ?? '',
            codemode: tool.options?.codemode !== false,
            input: tool.input,
          }));
        }),
      },
    );
  }),
};
