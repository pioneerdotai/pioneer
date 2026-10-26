# Setup and troubleshooting

<!-- Modified for Pioneer. -->

Load this after a missing command, launch failure or actual
browser problem, not before every browser action.

## Diagnose the failure

Use the task's named session for session-specific commands. Start with the
error already returned; choose the relevant diagnostic:

```bash
agent-browser --version
agent-browser doctor --offline --quick
agent-browser doctor
agent-browser --session "$browser_session" session info --json
```

`doctor` checks installation, config, Chrome, daemons, providers and network; the
full form includes a scratch launch test. It cleans stale socket/pid/version
sidecars. `--json` gives structured results; exit code 1 means a failed check,
while warnings alone allow exit 0. `doctor --fix` also performs repairs such as
reinstalling Chrome or purging old state: use it only when those repairs fit the
authorized scope, not as the default response to an application error.

For an unknown command/flag after an upgrade, consult installed help and the
matching installed guide; see [version differences](tools-and-runtime.md#version-differences).
A failed application wait is not evidence that Node or Chrome must be reinstalled.

## Provision only what is missing

An existing working CLI/browser needs no setup. Do not replace the user's Node,
package manager, browser or global installation merely to match these docs.

For an npm-based installation:

```bash
npm install -g agent-browser
agent-browser install
```

Use the existing Node manager if npm needs provisioning. The upstream source
build declares Node.js 24+, pnpm 11+ and Rust; its native daemon itself does not
require Node. Upstream also supports Homebrew/Cargo installations. Keep the
installation method consistent; `npx` is unnecessary when the binary is installed.
Use upstream version-specific build instructions for source builds rather than
assuming an old `pnpm build` recipe still applies.

On Linux, install required system libraries when absent:

```bash
agent-browser install --with-deps
```

This is conditional, not a second mandatory install after every download. If it
fails, use the package-manager diagnostics printed on that host. It can require
system privileges; apply the runtime's normal permissions rather than silently
changing unrelated machine configuration.

An existing browser can be selected with `AGENT_BROWSER_EXECUTABLE_PATH` or
`--executable-path`. On Linux ARM64, check installed support and the actual binary
path; Chrome for Testing may be unavailable while system Chromium is usable.
For recording only, `ffmpeg` and the matching codec are needed; see
[video recording](video-recording.md#requirements).

The optional shell templates require Bash 3.2+ and jq 1.6+ to inspect structured
CLI replies. Ordinary direct CLI use does not require jq. Read the template's
usage before choosing its dependencies or running it.

## Linux missing shared libraries

Chrome exit code `127` plus `error while loading shared libraries: ...` means
the browser exists but an OS library is missing. Run `install --with-deps` and
follow its missing-library/package-manager output. Then retry a minimal launch
in the task session:

```bash
agent-browser --session "$browser_session" open https://example.com
agent-browser --session "$browser_session" get title
agent-browser --session "$browser_session" close
```

Use the user's target URL if network access is restricted to it. If the next
failure is `No usable sandbox` or `Permission denied (13)`, inspect AppArmor.

## AppArmor Chrome sandbox failure

On AppArmor-restricted Linux hosts, Chrome for Testing in a nonstandard path can
be denied access to user namespaces. Ubuntu 24.04 commonly exposes this with
`kernel.apparmor_restrict_unprivileged_userns=1`.

Retain the browser sandbox. For an authorized host repair, add a profile matching
the **actual installed Chrome path** rather than changing the global userns
restriction. This existing Pioneer recipe covers versioned agent-browser
downloads; adjust the path if diagnostics show a different location:

```bash
sudo tee /etc/apparmor.d/agent-browser-chrome >/dev/null <<'APPARMOR'
abi <abi/4.0>,
include <tunables/global>

profile agent-browser-chrome @{HOME}/.agent-browser/browsers/chrome-*/chrome flags=(unconfined) {
  userns,
  include if exists <local/agent-browser-chrome>
}
APPARMOR

sudo systemctl start apparmor
sudo apparmor_parser -r /etc/apparmor.d/agent-browser-chrome
```

Close/relaunch only this task's owned session, then use `doctor` and the minimal
launch above to validate. Remove any temporary `--no-sandbox` workaround after
the normal sandbox works. Do not close all sessions, kill the user's browser or
disable system-wide protections as routine repair.

## Page and interaction failures

| Observation | Next step |
| --- | --- |
| Ref missing/stale or replaced element is ambiguous | Snapshot again and choose the currently observed control. |
| Element exists in DOM but not snapshot | Check full/scoped snapshot, scroll into view or wait for the real render condition. |
| `covered by <...>` | Interact with the observed overlay/banner, snapshot again, then retry. |
| Custom input ignores fill/type | Focus it, then try `keyboard inserttext` or `keyboard type`. |
| `tab_gone` | List tabs or create a new owned tab; do not act on another tab accidentally. |
| `dialogBlocked:true` | Inspect/resolve the pending dialog before continuing. |
| `revived:true` / `activeTabRevived:true` | The tab reloaded; re-check unsaved form state and snapshot again. |
| SPA wait times out | Inspect current URL/content and choose a result-specific condition; avoid generic `networkidle`. |
| Auth expires | Use the intended vault/state/restore flow and verify the authenticated result. |
| WebGPU canvas is black | Read [WebGPU](webgpu.md), wait for rendering and diagnose the capture path. |

`page_wedged` means the page stopped responding. With an owned browser, repeated
tab timeouts may replace it with a blank tab; navigate again and inspect. With
`--cdp`/`--auto-connect`, or a policy preventing tab creation/closure, it is left
alone. Choose a permitted tab recovery action without restarting unrelated work.

Cross-origin or nested iframes may need explicit `frame` selection. Do not add
CORS-bypass headers or weaken TLS just because a snapshot omitted content.
See [snapshot refs](snapshot-refs.md) and [proxy support](proxy-support.md) for
the relevant mechanisms.

For browser-visible debugging use `--headed` when launch conditions permit,
`console`, `errors`, `highlight`, or a scoped trace/recording. Avoid dumping
credential-bearing console/network content into the transcript. Preserve the
original error and report what was actually verified after repair.
