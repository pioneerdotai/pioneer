# Session management

<!-- Modified for Pioneer. -->

## Contents

- [Choose and carry a session](#choose-and-carry-a-session)
- [Isolation and shared Chrome](#isolation-and-shared-chrome)
- [Persistence and restore](#persistence-and-restore)
- [Multiple users and variants](#multiple-users-and-variants)
- [Cleanup and recovery](#cleanup-and-recovery)

## Choose and carry a session

The unnamed default is one shared browser across conversations and agents.
Give each independent task a distinct name; a shared worktree or target domain
does not identify a Pioneer task.

When a task/turn id is available, it can be part of the prefix for the CLI's
stable id helper. Replace the example key with the current task's actual id:

```bash
browser_task_key="<unique-task-or-turn-id>"
browser_session="$(agent-browser session id --scope worktree --prefix "pioneer-browser-$browser_task_key")"
agent-browser --session "$browser_session" open https://app.example.com
agent-browser --session "$browser_session" snapshot -i
```

The helper scopes to worktree, falling back to Git root and canonical cwd.
`--scope cwd` and `--scope git-root` are alternatives. A stable scope plus a
generic prefix alone still collides between agents on that scope. For work
outside a known task, choose a fresh nonce and keep the resulting session name.

Record the actual name for subsequent tool calls and compaction. Shell exports
are process-local; either pass `--session` every time or explicitly set
`AGENT_BROWSER_SESSION` for each execution. Bare commands in this package assume
that environment has already been set. Do not change it midway accidentally.

`--namespace` / `AGENT_BROWSER_NAMESPACE` can isolate daemon sockets and restore
directories, but do not replace distinct task session names.

## Isolation and shared Chrome

Separate locally launched sessions have separate cookies, storage, IndexedDB,
cache, browsing history and tabs. Full browser isolation does not apply when
sessions attach to the same Chrome or Electron app: they share that browser's
cookies/storage. Use a separate launched browser/profile for different users.

When attaching to a user-provided CDP endpoint, pin the session to its own tab:

```bash
agent-browser --session "$browser_session" --cdp 9222 --pin-tab open https://app.example.com
agent-browser --session "$browser_session" tab list --json
```

Use only an endpoint the user authorized. `--pin-tab` creates a fresh tab on
initial attachment rather than adopting an existing one. The binding survives
daemon restarts by CDP target id. It also works with `--auto-connect`.

- If the bound tab closes, commands fail with `tab_gone` rather than switching
  to someone else's tab. Recovery uses `tab new <url>` or `tab list` followed by
  an explicitly chosen tab.
- Another running pinned session owns its tab; a conflicting switch returns
  `tab_locked`.
- Pinning is sticky. `--no-pin-tab` turns it off; an explicit session close
  releases the pin.
- JSON `tab_gone` includes `data.targetId` and possibly a sanitized
  `data.lastUrl`; batch returns that recovery object under `result`.
  HTTP credentials, queries and fragments are removed from that URL; opaque
  URLs may be omitted.

The session has one active tab. Select it before acting and snapshot after
switching. Use stable returned `tN` ids, labels or CDP target ids; numeric
positions such as `tab 2` are not supported. Daemon-local `tN` ids can change
on restart; CDP target ids persist as long as the actual target exists.

## Persistence and restore

A named session isolates commands; it does **not** by itself persist login or
open tabs across shutdown. Use persistence only when the task calls for reuse.

```bash
agent-browser --session "$browser_session" --restore open https://app.example.com/dashboard
# After authorized login and verification:
agent-browser --session "$browser_session" --restore close
# Continue this same task/reuse later with the recorded name:
agent-browser --session "$browser_session" --restore open https://app.example.com/dashboard
```

Bare `--restore` uses the session name as persistence key; `--restore <name>`
selects a separate key deliberately. Do not fabricate paths under
`~/.agent-browser/sessions/`. Restore saves auth/storage state, not a promise to
reconstruct every tab or unsaved form.

State saves on close, daemon shutdown, idle timeout and compatible relaunch,
and periodically after settling (default autosave interval 30000 ms).
`AGENT_BROWSER_AUTOSAVE_INTERVAL_MS=0` leaves shutdown saves only.
`--restore-save auto` skips auto-save when restore/validation failed;
`never` disables periodic saves too. Use `always` only when replacing the
previous state is intended.

Use a real application-specific validation condition:

```bash
agent-browser --session "$browser_session" --restore --restore-check-url "**/dashboard" open https://app.example.com/dashboard
agent-browser --session "$browser_session" session info --json
```

`--restore-check-text` and `--restore-check-fn` are alternatives. A random
localStorage key's existence does not prove the server still accepts it.

Manual `state save`, `state load` and launch-time `--state <file>` are useful
for explicitly supplied portable files. Treat exports as credentials, keep
permissions private and do not publish or commit them. Vault login, profiles,
cookie imports, encryption and expiry are covered in
[authentication](authentication.md).

Inactive headless daemons normally save configured restore state and exit after
one hour. `--idle-timeout` / `AGENT_BROWSER_IDLE_TIMEOUT_MS` tune this;
`0` disables it. Dashboard input counts as activity. Headed, Safari/iOS
WebDriver and externally attached browsers are exempt from the default timeout;
provider-owned cloud sessions are not.

## Multiple users and variants

Derive separate task-scoped names, such as the task name with `-admin` and
`-viewer` suffixes. Keep command streams sequential within each active tab.
Independent browsers can run concurrently, but shared profile directories and
shared-CDP cookies defeat separate-user isolation.

For A/B checks, use separate sessions and capture the same application state
under each variant. Record account, viewport and experiment parameters so a
comparison does not mistake different setup for a product difference.

## Cleanup and recovery

Close a locally launched session owned by this task when finished:

```bash
agent-browser --session "$browser_session" close
```

For agent-browser 0.39.4, closing an externally attached CDP session disconnects
agent-browser without sending `Browser.close` to the user's Chrome. Remove
only temporary tabs this task created, leave user tabs untouched, then release
the task's connection. Never kill the desktop app or use `close --all` as
routine cleanup. Do not close a caller-owned session from a reusable script.

Do not delete reusable state, clear all cookies or edit project ignore files
just as a generic cleanup step. Remove only temporary data created by this task
when it is no longer needed. A saved auth state and a public report belong in
different locations.

After a timeout or lost command response, inspect URL/state before repeating a
write. Session diagnostics can help distinguish restore failures from a closed
browser; see [setup and troubleshooting](setup-and-troubleshooting.md).
Cloud stop failures have provider-specific semantics; check the matching guide
and do not assume a local daemon exit stopped a billed remote session.
