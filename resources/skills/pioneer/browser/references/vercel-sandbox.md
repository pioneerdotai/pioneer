# Browser automation in Vercel Sandbox

<!-- Modified for Pioneer. -->

Use this reference when implementing browser work inside Vercel Sandbox
microVMs, rather than running an ordinary local browser. The app's server code
creates an isolated VM, runs agent-browser/Chromium there, retrieves results
and stops the VM. This is an SDK integration pattern, not a new Pioneer tool.

## Dependencies and credentials

The upstream pattern uses `@agent-browser/sandbox` and `@vercel/sandbox` in the
target app. Add dependencies only as part of the requested implementation,
using that project's package manager and compatible versions. The fresh-VM
bootstrap installs agent-browser, Chromium and required system libraries.
Use `installSystemDependencies: false` only for an image that already has those
libraries; a browser binary alone is insufficient.

On Vercel, the SDK uses deployment OIDC. For local development, the existing
private environment can supply `VERCEL_TOKEN`, `VERCEL_TEAM_ID` and
`VERCEL_PROJECT_ID`; otherwise the helpers use `VERCEL_OIDC_TOKEN` when present.
Do not embed tokens in code, return them from routes or echo environment files.
Creating cloud VMs remains subject to existing user scope and runtime access.

## Bind one unique browser session

The caller supplies `browserSession`, the task/operation's unique named session.
Every browser command, including cleanup, must carry it. Reusing a sandbox
does not justify using an unnamed browser shared by independent operations.
The following TypeScript helper binds the actual upstream `session` option:

```ts
import {
  createAgentBrowserSnapshot,
  runAgentBrowserCommand,
  withAgentBrowserSandbox,
  type VercelSandboxSession,
} from "@agent-browser/sandbox/vercel";

function bindBrowser(sandbox: VercelSandboxSession, browserSession: string) {
  if (!browserSession.trim()) throw new Error("A unique browser session is required.");
  return async function browser<TJson = unknown>(args: readonly string[], json = true) {
    const result = await runAgentBrowserCommand<TJson>(sandbox, args, {
      session: browserSession,
      json,
    });
    const status = result.json as { data?: { confirmation_required?: boolean } } | null;
    if (status?.data?.confirmation_required === true ||
        (!json && result.stdout.startsWith("Confirmation required:"))) {
      throw new Error("Browser action is pending the configured CLI confirmation.");
    }
    return result;
  };
}

async function withBrowser<T>(
  browserSession: string,
  fn: (
    sandbox: VercelSandboxSession,
    browser: ReturnType<typeof bindBrowser>,
  ) => Promise<T>,
): Promise<T> {
  return withAgentBrowserSandbox(async (sandbox) => {
    return fn(sandbox, bindBrowser(sandbox, browserSession));
  });
}
```

The helper's default lifecycle stops the task-owned VM even when the callback
throws. If the requested app deliberately retains a sandbox, it must own its
lifetime and close only its own browser sessions; do not leak a VM on error or
stop a sandbox belonging to another operation. Apply app auth, target scope and
rate limits before admitting browser work.

An existing CLI policy may require confirmation even with exit code 0. The
wrapper stops on that pending result instead of treating it as a completed
action. Handle the configured gate through the existing runtime; do not clear
the policy or auto-confirm from this SDK helper. The plain-text marker is from
0.39.4; re-check it when upgrading the CLI.

## Screenshot and accessibility snapshot

`runAgentBrowserCommand` enables JSON output by default. A screenshot result
contains a remote file path; that file must be retrieved before the VM stops.

```ts
export async function captureUrl(
  browserSession: string,
  url: string,
  readyWaitArgs: readonly string[],
) {
  return withBrowser(browserSession, async (sandbox, browser) => {
    await browser(["open", url]);
    // Examples: ["--text", "Dashboard"] or ["--url", "**/dashboard"].
    if (readyWaitArgs.length) await browser(["wait", ...readyWaitArgs]);
    const titleResult = await browser<{ data?: { title?: string } }>(["get", "title"]);
    const tree = await browser(["snapshot", "-i", "-c"], false);
    const capture = await browser<{ data?: { path?: string } }>(["screenshot"]);
    const path = capture.json?.data?.path;
    if (!path) throw new Error("Screenshot returned no file path.");
    const encoded = await sandbox.runCommand("base64", ["-w", "0", path]);
    if (encoded.exitCode !== undefined && encoded.exitCode !== 0) {
      throw new Error("Could not retrieve screenshot.");
    }
    const screenshotBase64 = (await encoded.stdout()).trim();
    await browser(["close"], false);
    return {
      title: titleResult.json?.data?.title,
      snapshot: tree.stdout,
      screenshotBase64,
    };
  });
}
```

For text-only work, omit capture/retrieval and return the requested snapshot or
targeted text. Full snapshots or `read` include content omitted by `-i`.
Readiness is app-specific; a generic `networkidle` wait can stall on polling or
streaming apps. Do not add a fixed sleep to hide that ambiguity.

Base64 here transports image bytes between the app and VM; it is not a useful
model-visible answer. Decode/save the actual image when needed, inspect it with
an available image tool before visual claims, and use the actual delivery
mechanism in [tools and runtime](tools-and-runtime.md). A remote `/tmp` path
does not name a file on the Pioneer host.

## Multi-step forms and other workflows

Keep all steps in the same sandbox and bound browser session. First inspect a
snapshot, then select current refs or observed semantic locators. The following
function consumes refs already discovered in that same active page; it does
not guess a universal Submit ref:

```ts
async function submitObservedForm(
  browser: ReturnType<typeof bindBrowser>,
  fields: ReadonlyArray<{ ref: string; value: string }>,
  submitRef: string,
  postSubmitWaitArgs: readonly string[],
) {
  if (!postSubmitWaitArgs.length) throw new Error("An app-specific result wait is required.");
  for (const field of fields) {
    await browser(["fill", field.ref, field.value]);
  }
  await browser(["click", submitRef]);
  await browser(["wait", ...postSubmitWaitArgs]);
  return browser(["snapshot", "-i"], false);
}
```

Use this only for non-secret form values within authorized effects; credentials
need the private mechanisms in [authentication](authentication.md). Do not log
the helper's raw command/result objects when they may include private data.
After a navigation or DOM replacement, snapshot again instead of trusting old
refs. Following a timeout on a submission, inspect the resulting app state
before any retry so the function cannot silently double-submit.

## VM snapshots for startup

A Vercel **sandbox snapshot** is a reusable VM image containing dependencies,
agent-browser and Chromium. It is different from an accessibility snapshot,
which describes the current page. A prebuilt image can avoid installing the
browser on each fresh invocation; startup time still depends on the actual
environment and is not guaranteed to be sub-second.

```ts
const snapshotId = await createAgentBrowserSnapshot();
// Store the non-secret image identifier in the app's deployment configuration.
```

Set `AGENT_BROWSER_SNAPSHOT_ID` to that returned id for future helper-created
sandboxes. Build the reusable image before site login and do not capture private
browser state, temporary credentials or unrelated job data in it. Refresh the
image deliberately when dependency/browser versions change.

The upstream [snapshot creation script](https://github.com/vercel-labs/agent-browser/blob/main/examples/environments/scripts/create-snapshot.ts)
is an external example, not a file bundled in this skill. Adapt it to the
target app rather than trying to run a nonexistent local `examples/` path.

## Scheduling and server integration

For a user asking Pioneer to run a future or recurring check, use
`system:pioneer/tasks` only when its skill and permitted task tools are available.
Cloud SDK execution does not create a Pioneer schedule or select delivery.
Do not add Vercel Cron merely because an ordinary browser task could recur.

When the requested deliverable is instead a Vercel app with its own cron,
call the bound-session helper from an authenticated scheduled route. Generate
the session uniquely per invocation, for example with the app runtime's UUID:

```ts
// Example route handler within the target app; reuse withBrowser above.
export async function GET(request: Request) {
  // Apply the app's actual scheduled-route authentication before browser work.
  // This function must not be deployed as an unauthenticated public job runner.
  const browserSession = `pricing-${crypto.randomUUID()}`;
  const snapshot = await withBrowser(browserSession, async (_sandbox, browser) => {
    await browser(["open", "https://example.com/pricing"]);
    const result = await browser(["snapshot", "-i", "-c"], false);
    await browser(["close"], false);
    return result.stdout;
  });
  return Response.json({ ok: true, snapshot });
}
```

The code above shows the browser portion; implement the target app's existing
route authentication, target validation and result handling before deployment.
Store only needed/redacted results; sending alerts is a separate requested
effect. A cron configuration for that app can then select the route:

```json
{ "crons": [{ "path": "/api/cron", "schedule": "0 9 * * *" }] }
```

Use the hosting platform's actual timezone/scheduling semantics and the user's
requested frequency. Do not treat this example as permission to deploy a job.

## Environment and framework placement

| Variable | Purpose |
| --- | --- |
| `AGENT_BROWSER_SNAPSHOT_ID` | Optional prebuilt VM image identifier |
| `VERCEL_TOKEN` | Private local/explicit SDK credential when used |
| `VERCEL_TEAM_ID` / `VERCEL_PROJECT_ID` | Intended existing cloud scope |
| `VERCEL_OIDC_TOKEN` | Deployment OIDC path when available |

| Framework | Typical server-side integration point |
| --- | --- |
| Next.js | Server actions, API routes or route handlers |
| SvelteKit | `+page.server.ts` / `+server.ts` |
| Nuxt | `server/api/` / `server/routes/` |
| Remix | Server loaders/actions |
| Astro | Server-rendered frontmatter or API routes |

Keep credentials and SDK execution on the server. Use the upstream
[environment examples](https://github.com/vercel-labs/agent-browser/tree/main/examples/environments)
and [sandbox package](https://github.com/vercel-labs/agent-browser/tree/main/packages/%40agent-browser/sandbox)
as explicit external references. The browser CLI, SDK and VM image versions
must be checked together when their interfaces differ from this snapshot.
