# Electron app automation

<!-- Modified for Pioneer. -->

Use this reference for an Electron app that actually exposes a Chrome DevTools
Protocol (CDP) endpoint. Native desktop apps do not generally expose CDP, and
an app's Chromium ancestry alone does not prove that its current build permits
remote debugging. Prefer an available service connector for supported tasks.

Read the [browser entrypoint](../SKILL.md) for the core loop and
[tools and runtime](tools-and-runtime.md) for execution and file delivery.
Every command below uses `$browser_session`, the unique session selected for
this task. Supply it in each command process; a previous shell's variables may
not survive another tool call.

## Launch and attach

1. Identify the intended app, installed executable and available CDP endpoint.
2. When launching is part of the task, pass the remote debugging flag at launch.
3. Connect, list targets and select the intended window or webview.
4. Snapshot, act on observed refs, wait for the result and inspect again.

Examples for installed apps whose builds support remote debugging:

```bash
# macOS: choose the app and an unused local port.
open -a "Slack" --args --remote-debugging-port=9222
open -a "Visual Studio Code" --args --remote-debugging-port=9223
```

```bash
# Linux: use the executable actually installed on this host.
slack --remote-debugging-port=9222
code --remote-debugging-port=9223
```

```powershell
# Windows PowerShell: verify the installed path first.
& "$env:LOCALAPPDATA\slack\slack.exe" --remote-debugging-port=9222
& "$env:LOCALAPPDATA\Programs\Microsoft VS Code\Code.exe" --remote-debugging-port=9223
```

These are alternatives, not a list to launch in full. Existing app instances
may ignore new launch flags. Do not kill or quit the user's running app to
enable automation as routine setup. Use an existing endpoint or a separate
test instance/profile when the app supports one; if the task needs a restart
that is outside its existing authorization, explain the concrete requirement.

Check readiness through the selected local endpoint, rather than sleeping for
an assumed startup time:

```bash
curl --fail --silent --output /dev/null http://127.0.0.1:9222/json/version
agent-browser --session "$browser_session" connect 9222
agent-browser --session "$browser_session" tab list --json
agent-browser --session "$browser_session" snapshot -i
```

If readiness fails, inspect launch/endpoint diagnostics and retry only within
the task's launch budget. Do not poll indefinitely or expose the debugging port
on a public interface. CDP grants broad access to that app's browser context.

After `connect`, later commands in the same named session remain attached.
Alternatively use an explicit endpoint on each command:

```bash
agent-browser --session "$browser_session" --cdp 9222 snapshot -i
```

`--auto-connect` can discover a running Chromium endpoint, but use it only when
the discovered app/profile can be verified as the intended target. A hardcoded
port is not proof of ownership either.

## Windows, tabs and webviews

Electron may expose several page and webview targets. List them before acting;
native menus and operating-system dialogs may be outside the renderer's tree.

```bash
agent-browser --session "$browser_session" tab list --json
# Use a stable tabId or CDP targetId returned above, not a positional integer.
agent-browser --session "$browser_session" tab t2
agent-browser --session "$browser_session" snapshot -i
```

When a `<webview>` is exposed as a separate CDP target, it appears alongside
page targets. Select its returned id, then use the normal snapshot workflow.
Do not confuse a separate webview target with an iframe in a page; iframe
context is handled by `frame` and described in [core workflows](core.md).

For shared CDP browsers, consider `--pin-tab` so a closed bound target produces
`tab_gone` rather than acting on another target. Pinning does not serialize two
agents attached to the same target. Coordinate target ownership and see
[session management](session-management.md).

## Inspect, interact and capture

Refs below illustrate actions; choose them from the current target's snapshot.

```bash
agent-browser --session "$browser_session" snapshot -i
agent-browser --session "$browser_session" click @e10
# Replace this with the actual section/result expected after that click.
agent-browser --session "$browser_session" wait --text "Settings"
agent-browser --session "$browser_session" snapshot -i
agent-browser --session "$browser_session" get text @e5
agent-browser --session "$browser_session" screenshot --annotate "$output_dir/app-state.png"
```

Use full snapshots or `read` when text content is omitted by `snapshot -i`.
For a search form, fill its observed input, submit, then wait for a result
heading, known route or app condition tied to that query. Do not reuse the old
result's presence as the success condition.

```bash
agent-browser --session "$browser_session" fill @e3 "search query"
agent-browser --session "$browser_session" press Enter
# Use the actual query-specific result text observed in this app.
agent-browser --session "$browser_session" wait --text "Results for search query"
agent-browser --session "$browser_session" snapshot -i
```

For custom editors that intercept normal input, focus the observed control and
try `keyboard inserttext` or `keyboard type` with non-secret content:

```bash
agent-browser --session "$browser_session" focus @e3
agent-browser --session "$browser_session" keyboard inserttext "search query"
```

Captures can use `screenshot`, `screenshot --full` or `--annotate` as needed.
View actual images before making visual claims and keep private app content
out of unrelated reports. A screenshot does not prove a native dialog or menu
was accessible through CDP.

## Multiple apps and appearance

Use separate task-unique session names and separate endpoints for independent
apps. The caller supplies `$slack_session` and `$code_session`; names like
`slack` or `vscode` alone collide with other tasks.

```bash
agent-browser --session "$slack_session" connect 9222
agent-browser --session "$code_session" connect 9223
agent-browser --session "$slack_session" snapshot -i
agent-browser --session "$code_session" snapshot -i
```

CDP emulation can affect the app's color scheme. Inspect the current appearance
first. When the task calls for dark-mode emulation, use:

```bash
agent-browser --session "$browser_session" --color-scheme dark snapshot -i
```

The alternative environment setting is `AGENT_BROWSER_COLOR_SCHEME=dark` in
that command's process. Avoid changing the user's appearance merely to attach.

## Troubleshooting and finish

| Symptom | Next check |
| --- | --- |
| Connection refused | Verify the chosen app enabled CDP at launch and the endpoint is ready. Check the port owner locally, e.g. `lsof -i :9222`. |
| Endpoint responds, app missing | List CDP targets; verify the endpoint belongs to the intended app/build. |
| Expected element absent | Select the correct page/webview, expand or scroll the relevant region, then snapshot again. Native UI may be outside CDP. |
| Input ignored | Focus the actual editable control; try keyboard insertion for a custom editor. |
| State changes in another window | Confirm active target, session and any shared-CDP coordination before further actions. |

Disconnect only this task's agent-browser attachment as described in
[session management](session-management.md). Preserve user-owned windows and
tabs; do not quit the app or use global browser cleanup. For Slack-specific
navigation and reading, continue with [Slack](slack.md).
