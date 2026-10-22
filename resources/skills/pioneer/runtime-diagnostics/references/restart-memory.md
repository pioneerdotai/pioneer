# User-run memory preparation and restoring normal startup

Read before suggesting a restart. This is the macOS allocation-history/live-heap
path. CPU attach, allocation churn from attach and VM inspection can avoid it.
Linux/Windows have no equivalent prepared-allocator adapter in this implementation;
do not present these commands as cross-platform support.

## Ownership rule

**The agent prepares instructions; the user runs the stop/launch/restore commands
outside Pioneer.** The Gateway can host the investigating turn, and the desktop
may launch or supervise it. Restarting either can disconnect the agent or destroy
its execution. The agent must not run these steps through tools, an attached
subagent, a scheduled task, a detached shell, a service-manager action or a delayed
signal. An automatic cleanup trap that kills the hosting process is equally unsafe.

Before a restart, finish autonomous snapshot/bundle/symbol preparation and save a
checkpoint in a private durable case directory, outside ephemeral stack logs:

- Symptom and the exact question remaining; target service/run/PID/start identity.
- Actual CLI/app paths, configuration selection, runtime home and normal supervisor.
- Symbol artifact location, module IDs and validation results already obtained.
- Evidence paths, expected files, disk/RAM headroom and chosen launch/log budgets.
- Full user instructions for both capture and restoration, plus a resumption note.

Do not assume this agent remains alive after the user stops the process. The
instructions must be enough for the user to obtain a capture without a live agent,
and later provide the case path in the resumed or a new conversation. Tell the
user plainly that this disconnection is expected.

## What preparation does

`pioneer diagnostics launch-memory` starts one explicitly owned executable. It
sets `MallocStackLoggingNoCompact=1` and a private `MallocStackLoggingDirectory`
for that child only, removing `MallocStackLogging`. Do not set any of them through
`export`, shell profiles, `launchctl setenv`, service definitions, or the global
login environment. Native profiler/version helper tools remove the three variables from their
own environment; the application can still pass its environment to other children.
Account for that cost and scope when examining process descendants.

The wrapper requires persisted telemetry consent, checks it and its private log
budget throughout preparation, and expects target diagnostics within 15 seconds.
It is single-flight per runtime home. The allowed lifetime is 1..300 seconds;
`--log-budget-mib` defaults to 1024, with range 1..4096. Quota is logical bytes,
not sparse-file optimism, and is checked periodically. A budget limits the run;
it does not guarantee that a workload fits the entire window.

At expiry, Ctrl+C, consent/budget failure or error, the wrapper kills/reaps its
**owned launch** before removing its temporary native logs. This is why launching
it under the investigating agent is unsafe. Its logs are not the retained profile
and are not meant to be stored indefinitely. Captures have to finish before the
launch lifetime expires. Stop reproducing once the needed sample is collected.
Preparation may end early on errors; inspect its stdout/stderr instead of assuming
it completed the intended time window.

## Produce a concrete handoff

Replace paths, optional `PIONEER_CONFIG`, installation details and case directory
with verified values before giving this to the user. Only the newly launched
PID/run ID can be discovered afterward. Do not invent `--memory`, `--dsym`, or
`--diagnostics` application startup flags: preparation is the wrapper command below.
The examples show a 300-second lifetime to leave room for discovery, one capture
and native processing, with a 1024-MiB log quota. Choose less when sufficient and
compatible with the available disk/RAM. Do not automatically raise it after overflow.

The user needs **two terminals outside Pioneer**. Both must use the same OS user
and effective configuration as the target. Set these variables separately in each
terminal using absolute values supplied by the agent:

```bash
diag_cli='/absolute/path/to/pioneer'
diag_case='/absolute/path/to/private-diagnostic-case'
# Only when this installation uses an explicit config selection:
# export PIONEER_CONFIG='/absolute/path/to/the-existing-config.toml'
```

Setting that config path selects the existing configuration; never dump its
contents or select another live user's data. The case directory must already be
private and have room for receipts and artifacts. Check free space before start
and during the run; preserve at least 5 GiB. On a constrained machine, do not run
memory preparation and another expensive profiler/download at the same time.

### Gateway: terminal A, user only

First identify the normal service manager and whether the desktop auto-starts
Gateway. Instruct the user to close the desktop if it would restart the service,
or pause the relevant supervisor through its normal controls. A foreground
Gateway using the same home/listen address must not run alongside the normal
service. Do not supply blanket `pkill`, force-kill or guessed launchd/systemd commands.

For the standard installation managed by `pioneer start/stop`:

```bash
# USER ONLY: this disconnects a Gateway-hosted agent.
"$diag_cli" stop
# Verify the normal service stopped, using its status output and the old PID.
"$diag_cli" status --json

# USER ONLY: keep this terminal open; this owns the temporary Gateway.
"$diag_cli" diagnostics launch-memory --seconds 300 --log-budget-mib 1024 \
  -- "$diag_cli" gateway-service
```

If the installed binary is owned by another supervisor/container, replace the
stop/restore instructions with its verified normal controls rather than applying
`pioneer stop` to the wrong installation. Never tell the user to create a second
empty runtime home as a substitute for reproducing the real issue.

The launcher prints `launched_pid` and lifetime/budget information. Target logs
may follow. Copy the printed PID for terminal B. The launcher's success does not
mean that a separate capture was collected.

### Desktop: terminal A, user only

Have the user quit the **application normally**, including any background/tray
instance, and verify the old desktop process ended. This may also disconnect the
investigation. Record whether Gateway was already running; leave its ordinary
startup settings unchanged. For desktop-only diagnosis, arrange for its normal Gateway to be running
**before** the prepared desktop starts, using the user's normal controls. This
avoids having the prepared desktop launch a new Gateway with an inherited malloc
environment. Do not prepare that Gateway unless it is also an explicit target.
Avoid accidentally starting a second desktop instance.

For a verified standard macOS installation:

```bash
diag_app='/Applications/Pioneer.app/Contents/MacOS/pioneer-app'
# USER ONLY, after quitting the old desktop through its normal Quit action:
"$diag_cli" diagnostics launch-memory --seconds 300 --log-budget-mib 1024 \
  -- "$diag_app"
```

Use the actual installed bundle/executable, not this example if the location is
different. Launch the executable directly through the wrapper. `open -a Pioneer`
can reuse a running instance and cannot stand in for a prepared owned child.
If Gatekeeper/signing/attach blocks the real package, keep those protections and
report the limitation. Do not modify the installed executable or entitlements.

### Either target: terminal B, user only

After the new process is available, resolve its exact run by the PID printed in
terminal A. Here Python 3 is used only to select a safe `list` entry; confirm it
is installed while preparing the handoff, or give manual run-ID selection instead.

```bash
diag_pid='PID_PRINTED_BY_TERMINAL_A'
diag_run="$("$diag_cli" diagnostics list | python3 -c '
import json, sys
matches = [t["run"]["run_id"] for t in json.load(sys.stdin)
           if t["run"]["pid"] == int(sys.argv[1])]
if len(matches) != 1:
    raise SystemExit("Expected one new diagnostic target; recheck PID and startup")
print(matches[0])
' "$diag_pid")" || exit 1
"$diag_cli" diagnostics snapshot --target "$diag_run" \
  > "$diag_case/prepared-snapshot.json"
```

Check that snapshot `stack_logging` is `full_history_since_launch` and the PID/run
are the new target. If not, stop and report the mismatch rather than running an
unprepared history capture. A replacement process has a new run ID. Do not reuse
the pre-restart ID.

Reproduce the smallest relevant user action, then run **one chosen mode**:

```bash
# Live allocations, grouped by allocation stack:
"$diag_cli" diagnostics live-allocations --target "$diag_run" --seconds 60 \
  > "$diag_case/live-allocations-receipt.json"

# ALTERNATIVE when allocation/free history is the actual question:
# "$diag_cli" diagnostics allocation-history --target "$diag_run" --seconds 60 \
#   > "$diag_case/allocation-history-receipt.json"
```

`--seconds 60` gives this native reader a larger supported completion budget than
the default 10; it does not record a 60-second allocation time series. The native
reader gets about 70 seconds before tool timeout (60 plus a 10-second allowance),
with setup and cleanup outside that interval. Native processing can take over a
minute and several GiB of RAM. Keep terminal A alive
until the reader finishes. Read stdout even on nonzero exit, preserve the receipt
and error, and locate retained files using its `local_reference`.

Optionally save a bundle while the target still exists, if time and disk allow:

```bash
"$diag_cli" diagnostics bundle --target "$diag_run" \
  > "$diag_case/prepared-bundle-receipt.json"
```

The capture lives under `<runtime_home>/diagnostics/captures`, not inside the
preparation logs. Note its exact directory before ending the session. Do not start
new captures near the lifetime deadline or repeat a failed attempt automatically.
If a capture must be cancelled, terminal B can run `diagnostics cancel --target
"$diag_run"`; wait for the capture receipt before ending preparation when possible.

## Stop preparation and restore ordinary launch: user only

1. Once the chosen capture finishes, press **Ctrl+C in terminal A**, or let its
   lifetime finish. Wait for the wrapper to exit and reap its temporary child.
   For desktop, close the prepared app normally if it is still open; do not start
   a second normal instance while the prepared one is alive.
2. Verify the recorded prepared PID ended and `diagnostics list` no longer lists
   that run. PID-only checks can race with reuse; retain the recorded start/run
   identity. If the wrapper crashed and its child may still live, use the user's
   process/supervisor controls for that identified instance. Do not delete its logs
   or lock blindly; report the incomplete cleanup. The wrapper owns its direct
   child, not a complete process-tree cleanup contract. Check any descendants
   known to have inherited malloc logging; have the user stop those identified
   temporary processes through their own controls before treating log cleanup
   and resource restoration as complete.
3. Restore **the same normal launch method** and original configuration. With a
   standard Gateway service, use this from the user's external terminal:

   ```bash
   env -u MallocStackLogging -u MallocStackLoggingNoCompact \
       -u MallocStackLoggingDirectory "$diag_cli" start
   "$diag_cli" status --json
   ```

   For desktop, reopen the original bundle through Finder, or, for the verified
   standard location:

   ```bash
   env -u MallocStackLogging -u MallocStackLoggingNoCompact \
       -u MallocStackLoggingDirectory /usr/bin/open '/Applications/Pioneer.app'
   ```

   If a different supervisor was paused, restore it through the original controls.
   The wrapper only set child-local variables, so there should be no persistent
   diagnostic edit to roll back. If a prior procedure changed a launch definition
   or global environment, restore only those documented changes as well.
4. Rediscover the normal process. Check its new run and snapshot:
   `stack_logging` must be `not_prepared`, and normal reachability should return.
   A fresh run/consent history is expected. Do not disable ordinary metrics/traces
   to turn off malloc recording; those are separate mechanisms.
5. Continue the investigation using the saved checkpoint and retained capture.
   Give the agent the private case path and restart outcome. The agent can then
   validate symbols and analyze files without another restart. Do not claim the
   environment was restored until these checks are observed or supplied by the user.

Explain this in the user's language: "This launch records detailed memory data
and ends automatically. You run it outside Pioneer because stopping Gateway may
close this agent. After the capture, close that temporary launch and start the app
normally; ordinary startup does not keep the detailed memory recording enabled."
