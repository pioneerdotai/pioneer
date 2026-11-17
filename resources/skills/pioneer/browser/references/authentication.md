# Authentication

<!-- Modified for Pioneer. -->
Use the task session from [SKILL.md](../SKILL.md) in every command. Bare examples
assume `AGENT_BROWSER_SESSION` is set for this execution.

## Contents

- [Choose an existing authentication path](#choose-an-existing-authentication-path)
- [Vault login and prepared pages](#vault-login-and-prepared-pages)
- [Credential plugins](#credential-plugins)
- [Cookies and state files](#cookies-and-state-files)
- [Profiles and automatic persistence](#profiles-and-automatic-persistence)
- [OAuth, SSO and 2FA](#oauth-sso-and-2fa)
- [Basic auth and expiry](#basic-auth-and-expiry)

## Choose an existing authentication path

Use an already authorized browser session, supplied state/cookie file or
configured vault profile before requesting more credentials. Ask for missing
input only when the task actually depends on it; existing authorization remains
in force. Do not read credential file contents into the transcript.

For an authorized session in existing Chrome, use an approved CDP endpoint with
tab pinning. Do not restart or kill the user's normal browser just to expose a
debugging port. If a separate debug browser must be provisioned, use a dedicated
profile and the platform's current Chrome instructions.

Secrets should travel through file input, a configured vault or provider
credentials outside model-authored arguments. Avoid literal passwords, session
tokens, bearer headers and `echo "<password>"` examples in tool calls.

## Vault login and prepared pages

Use an existing vault profile:

```bash
agent-browser auth list
agent-browser auth show work
agent-browser auth login work
agent-browser wait --url "**/dashboard"
agent-browser snapshot -i
```

Choose the profile and success condition for the actual application.
`auth show` reports profile metadata, not passwords. If provisioning a vault
profile is within scope and the user provided a private password file, stdin
avoids putting its value in the command:

```bash
agent-browser auth save work --url https://app.example.com/login --username user@example.com --password-stdin < "$password_file"
```

The CLI can filter custom credential controls by visibility, layout size,
opacity and disabled/readonly state. Use installed `auth login --help` for
`--username-selector`, `--password-selector` and `--submit-selector`
overrides. Replaced fields or redirected focus can cause a safe failure before
submission; inspect the form instead of forcing a second submit blindly.

For login reached through consent, an in-page modal or another stateful step,
preserve the prepared document:

```bash
agent-browser open https://app.example.com
agent-browser snapshot -i
# Open the observed sign-in control, then:
agent-browser auth login work --no-navigate
```

`--no-navigate` skips the initial navigation only. It requires an active
top-level HTTP(S) page, validates scheme/host/effective port against the effective
credential URL before filling, then runs the normal controls/submit flow.
A command-level `--url` overrides stored/provider URL metadata; in this mode
it is an expected-origin constraint, not a navigation destination. For an
authorized hosted identity page:

```bash
agent-browser auth login work --no-navigate --url https://identity.example.com/login
```

## Credential plugins

Inspect configured plugins first:

```bash
agent-browser plugin list
agent-browser plugin show vault
agent-browser auth login work --credential-provider vault --item "Work account"
```

Combine with `--no-navigate` and `--url` for stateful identity-provider flows.
Provider secrets remain outside generated process arguments and normal output.
Use the vault vendor's own login/session mechanism; do not add vault tokens to
plugin command arguments.

Plugins run external executables over `agent-browser.plugin.v1`.
`plugin add` changes configuration (normally
`~/.agent-browser/config.json`; `--project` writes a project file).
Do this when plugin setup is requested, not as a routine login fallback.
Auto-discovered `./agent-browser.json` cannot set plugins; an explicitly
selected config, environment or user config can. Review the executable and
requested capability before using an unconfigured plugin.

Capabilities include `credential.read`, `browser.provider`, `launch.mutate`
and `command.run`. Core capabilities use dedicated paths such as `auth login`
or `--provider`; generic custom operations use
`plugin run <name> <type> --payload <json>`. Existing agent-browser
`--confirm-actions` policies still apply, together with Pioneer permissions;
do not add a new blanket confirmation requirement or bypass a configured gate.

## Cookies and state files

For an approved cookie export or request saved by the user to a private file:

```bash
agent-browser open
agent-browser cookies set --curl "$cookie_file"
agent-browser open https://app.example.com/dashboard
agent-browser wait --text "Dashboard"
```

`cookies set --curl` detects supported JSON, cURL or Cookie-header formats.
Use `--domain` when the input format has no domain metadata and the intended
domain is known. Do not paste the cURL or cookie values into the model context.

For explicit portable state:

```bash
agent-browser --state "$state_file" open https://app.example.com/dashboard
agent-browser wait --url "**/dashboard"
# After a verified authorized login:
agent-browser state save "$state_file"
```

To restore into an already open task session, launch a blank page, use
`state load "$state_file"`, then navigate to the target. An approved existing
Chrome connection can export state with `state save`; prefer the explicitly
chosen CDP session over auto-discovering an unrelated browser.

State files carry cookies and localStorage and may contain plaintext tokens.
Keep them outside public report directories/version control with private
permissions. Do not overwrite or remove a user-provided file as a speculative
expiry fix. Explicit exports should be treated as secret regardless of whether
automatic restore encryption is configured.

## Profiles and automatic persistence

`--profile <name|path>` persists broader browser data, including IndexedDB,
service workers and cache. Use a dedicated automation profile and avoid opening
the same profile concurrently from several sessions. A normal user's profile
is not a generic automation scratch directory.

`--session <recorded-name> --restore` automatically saves/restores cookies and
storage; the named session alone does not enable persistence. See
[session management](session-management.md#persistence-and-restore) for
validation, autosave, idle shutdown and cleanup.

When using `AGENT_BROWSER_ENCRYPTION_KEY` for automatic state encryption,
provide it through the host's existing secret mechanism and keep the same key
available for reuse. Generating a new key every run loses access to prior state;
do not print keys or place them in generated source.

The [authenticated-session template](../templates/authenticated-session.sh)
supports reusable authentication flows; read its usage and concrete success
condition before running it.

## OAuth, SSO and 2FA

Start from the application's observed sign-in flow. Follow its actual redirects
and popup/tab changes, validate the expected identity-provider origin, then
wait for the authenticated application result. Avoid guessed provider URLs,
blindly filling a ref from the previous page or generic `wait 2000` delays.

Use an existing provider vault profile where available. If the user must
complete a challenge, passkey, consent or 2FA step, let them do it in the
authorized browser and wait for the application-specific completion signal.
Ask for user intervention only at the actual missing step. Save state after
verified success if reuse was requested.

For a protected Vercel deployment, use
[protected deployments](protected-vercel-deployments.md) rather than treating a
platform protection page as the application's own login.

## Basic auth and expiry

HTTP Basic auth can be configured before navigation with `set credentials`;
it applies to future tabs too. Do not construct command literals containing its
password. Use an existing trusted config/provider or another host-supported
secret input path, and consult installed help if that mechanism is unavailable.

On expiry, inspect the application's current response and reauthenticate through
the intended vault, file or interactive route. Verify an actual authenticated
element/URL or server result. A URL lacking the word "login" and the presence
of an arbitrary storage token are not sufficient proof.

Refresh and replace saved state only after successful login. Do not repeat
unrelated mutations, dump the entire cookie/storage contents, delete old state
unconditionally or ask again for permissions already granted. See
[trust boundaries](trust-boundaries.md) for handling auth-bearing diagnostics.
