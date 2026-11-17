# Protected Vercel deployments

<!-- Modified for Pioneer. -->

Use this guide when a target is behind Vercel Authentication, SSO or Deployment
Protection. Prefer the caller's existing Vercel identity and short-lived OIDC
access where supported. Keep the deployment's protection intact; making a
domain public is not ordinary browser setup.

Use the task's unique `$browser_session` throughout. Read
[authentication](authentication.md) and [tools and runtime](tools-and-runtime.md)
for private credentials, execution processes and version differences.

## Same-project Preview access

A local development token for the target project normally reaches that
project's protected Preview deployment through its default Trusted Sources
self-access rule. Verify the intended target, project and scope first.

```bash
vc whoami
vc --version
```

The upstream workflow requires Vercel CLI **53.3.0 or newer** for
`vc project token`. Versions 50.25.0 through 53.2.x write the token to stderr,
which can leak into logs while leaving command substitution empty. Do not mint
or recover tokens from stderr on those versions. Use a supported installed
version or an authorized upgrade path; do not silently replace a shared CLI.

Set `$VERCEL_PROJECT`, `$VERCEL_SCOPE` and `$VERCEL_PREVIEW_URL` from verified
project/deployment context. In a directory with a verified `.vercel/project.json`
link, omitting the project name is also supported. Do not run `vc link` merely
to obtain a token: the upstream CLI can also pull development variables into
`.env.local`. Ask for project/scope only when the available context cannot
identify them safely.

The following wrapper keeps the minted token and command output out of the
transcript. Its inputs identify the authorized deployment, not secret values:

```bash
python3 - "$browser_session" "$VERCEL_PREVIEW_URL" "$VERCEL_PROJECT" "$VERCEL_SCOPE" <<'PY'
import json
import subprocess
import sys

session, target, project, scope = sys.argv[1:]
mint = subprocess.run(
    ["vc", "project", "token", project, "--scope", scope],
    capture_output=True, text=True,
)
if mint.returncode != 0 or not mint.stdout.strip():
    raise SystemExit("Token minting failed; inspect only redacted diagnostics.")
token = mint.stdout.strip()
headers = json.dumps({"x-vercel-trusted-oidc-idp-token": token})
opened = subprocess.run(
    ["agent-browser", "--session", session, "--json", "open", target, "--headers", headers],
    capture_output=True, text=True,
)
if opened.returncode != 0:
    raise SystemExit("Authenticated open failed; inspect only redacted diagnostics.")
try:
    result = json.loads(opened.stdout)
except (ValueError, TypeError):
    raise SystemExit("Authenticated open returned no usable status; inspect redacted diagnostics.")
if not isinstance(result, dict) or result.get("success") is not True:
    raise SystemExit("Authenticated open did not succeed.")
data = result.get("data")
if isinstance(data, dict) and data.get("confirmation_required") is True:
    raise SystemExit("Open is pending the configured CLI confirmation; handle that gate before continuing.")
print("Authenticated open completed; verify the actual page in this session.")
PY
agent-browser --session "$browser_session" snapshot -i
```

Run credential-bearing subprocesses only through the existing trusted runtime;
do not log expanded argument arrays or enable shell/process tracing around
them. Capturing an error is not permission to paste its raw contents later.
Use redacted status/error information when diagnosing failures.

The correct protection header is `x-vercel-trusted-oidc-idp-token`.
`x-vercel-oidc-token` carries workload identity into Vercel Functions and does
not replace the inbound Deployment Protection header.

`open --headers` scopes the header to the target origin and applies it to
document and in-page requests. Continue in the same named session. If it is
closed/restarted, repeat the authenticated open with a fresh appropriate token.
Verify the actual application state; a successful open command alone does not
prove application login succeeded. Do not print/persist the token in source,
environment files, report fields or screenshots.

## Other callers and environments

Trusted Sources configuration can be needed when a development token must
reach Production, the caller is a different project/team, self-access rules
were customized or Vercel reports `TRUSTED_SOURCES_ENVIRONMENT_MISMATCH`.

The upstream workflow manages these mappings in the target project's
**Settings → Deployment Protection → Trusted Sources**; it documents no public
CLI/REST management path. A development caller reaching protected Production
needs the specific `development` to `production` mapping.

For a browse/test request, diagnose and report the exact missing caller and
environment mapping to the project's administrator instead of widening access
as a side effect. Access-control changes are separate work governed by existing
Pioneer permissions and the user's scope. If that exact change was already
authorized, do not demand another approval merely because the browser guide
mentions it; use the supported available surface. Retry target access after
the actual rule is in place, not merely after a claimed successful click.

Other concrete blockers include an unauthenticated Vercel CLI without existing
credentials, expired interactive login, unknown project/scope, or disabled
Secure Backend Access with OIDC Federation on the caller. For the last case,
the relevant setting is **Settings → Security**. State what is missing and
continue independent diagnosis where possible; do not ask for secret values
in chat or invent a supported management command.

## Diagnose the boundary that failed

| Result | Interpretation |
| --- | --- |
| Redirect to `vercel.com/login` | Deployment Protection did not accept the request |
| `TRUSTED_SOURCES_ENVIRONMENT_MISMATCH` | Valid token, but caller environment is not permitted for the target |
| Application 401/403 after protection passes | Application-level auth still needs its own diagnosis |
| Recognizable application 404 on a deliberately missing route | Evidence that the request reached the app, not proof of app login |

Keep application credentials separate from protection credentials. A bypass of
Deployment Protection does not change the app's own access model.

## Static-secret fallback

Use Protection Bypass for Automation when OIDC is unavailable or the caller
cannot supply its header, within the existing authorized access scope.
Enabling/rotating a bypass secret changes access control; do so only when that
effect is part of the user's task. Upstream provides `vc project protection`
for that operation. There is no need to enable or rotate an already suitable
credential just to use it.

Keep the dedicated bypass secret in the runtime's private credential/environment
mechanism, and inject it through a non-logging wrapper as above with headers:

```text
x-vercel-protection-bypass: value read privately at runtime
x-vercel-set-bypass-cookie: true
```

The cookie directive is optional for requested reusable access and creates a
`_vercel_jwt` cookie. Saved browser state containing it is a credential; do not
include it in report bundles. Never put the secret itself in a URL or command
literal. Keep its rotation/retention policy scoped to the user's request.

Avoid dead ends: `vercel share` is not the upstream CLI's automation command;
Shareable Links are intended for people, and `vercel curl` cannot render a UI.
Deployment Protection Exceptions make a domain public and should not be used
merely to unblock automation. Installed CLI help takes precedence for versioned
syntax, without extending the task's authority.
