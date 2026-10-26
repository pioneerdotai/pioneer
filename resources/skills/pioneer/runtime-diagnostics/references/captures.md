# Bounded native captures

Read after initial snapshots show that function-level evidence is needed. No
capture command restarts the target. The diagnostic client asks the live target
to start an owned profiler child; cancellation controls that profiler only.
Native attach can perturb a process, so choose a short capture suited to the
question, keep disk/RAM headroom and respect the user's operational constraints.
Do not attach a debugger that suspends the hosting process as a substitute.

## Platform support

| Action | macOS | Linux | Windows |
| --- | --- | --- | --- |
| `cpu` | xctrace Time Profiler `.trace` | perf `cpu-clock`, DWARF stack `perf.data` | Native adapter unavailable |
| `allocations` | xctrace Allocations, from attach onward | Unavailable | Unavailable |
| `allocation-history` | malloc_history `-allEvents`, prepared launch required | Unavailable | Unavailable |
| `live-allocations` | malloc_history `-allBySize`, prepared launch required | Unavailable | Unavailable |
| `vm` | vmmap `-summary` text | pmap `-x` text | Native adapter unavailable |
| `list`, `snapshot`, `bundle` | Supported | Supported | Supported |

macOS needs developer tools, the requested template and permission to attach to
the actual signed executable. Linux needs compatible `perf`/`pmap` and kernel
permissions. An unavailable adapter is not a zero result. Do not weaken signing,
entitlements, SIP, perf security settings or run the whole application as root
to force attach. Report a permission/tool blocker precisely.

## Commands

Use the private case directory, verified CLI path and exact run ID from discovery:

```bash
"$diag_cli" diagnostics cpu --target "$diag_run" --seconds 20 --frequency 99 \
  > "$diag_case/cpu-receipt.json"
"$diag_cli" diagnostics vm --target "$diag_run" --seconds 10 \
  > "$diag_case/vm-receipt.json"
```

Run only the relevant action, not every example as a mandatory sequence. macOS
allocation churn without a restart:

```bash
"$diag_cli" diagnostics allocations --target "$diag_run" --seconds 20 \
  > "$diag_case/allocations-receipt.json"
```

`--seconds` accepts 1..60 for all capture actions; default is 10. For CPU/perf or
xctrace it is the recording interval. For malloc_history/vmmap/pmap it participates
in the tool completion budget, not a promise of time-series sampling. macOS
CPU/Allocations has a further 120 seconds for native analysis and saving; the CLI
allows that finalization. Do not wrap it in an outer timeout of `seconds + 10`.
A command still finalizing has not necessarily hung. Follow tool/process activity
and its bounded manifest result.

`--frequency` accepts 19..199, default 99. Linux uses strict requested sampling
frequency; the macOS template controls frequency, so the number is not proof of
the actual sample rate. Native sample loss can be unknown. The recording does not
reconstruct work before attach.

To profile an active registered child rather than the parent:

```bash
"$diag_cli" diagnostics cpu --target "$diag_run" --child "$diag_child_generation" \
  --seconds 20 --frequency 99 > "$diag_case/child-cpu-receipt.json"
```

Obtain the generation from `resources.children`, verify PID/start identity and
active ownership, and recheck if it exits. Profiler children cannot be profiled.
The child executable can differ from the parent; do not apply parent symbols
without validating the child's actual module ID and build scope.

Cancel an active capture from a separate available tool call or terminal:

```bash
"$diag_cli" diagnostics cancel --target "$diag_run"
```

Cancellation can produce partial native files. It does not turn off launch-time
malloc stack logging, stop the application, or restore its launch configuration.
See `restart-memory.md` for that separate lifecycle. Losing the client connection
is not proof the target-side profiler stopped; inspect the target and send the
supported cancel request if needed. Do not use broad `pkill`, kill an app by name,
or remove files while the target-side worker is still using them.

## Artifacts and completion

Read stdout JSON even when the command exits nonzero; `status=failed` deliberately
makes the CLI fail after emitting a receipt. Resolve `local_reference` under
`<runtime_home>/diagnostics`, then inspect the actual manifest/files:

- Capture/run/consent IDs, target PID/start identity/child generation/build scope.
- Tool/version/configuration, actual format and measured wall/monotonic window.
- `module`, symbol status/coverage, native coverage, nullable `lost_samples`.
- `status`, `incomplete_reasons`, file names/bytes/SHA256.

`completed` describes the native collection result, not proof of useful symbols,
a minimum sample count, complete allocation ownership or a solved incident.
`partial` may have useful data, but could also contain only logs and no readable
profile. Open the native artifact and confirm records/stacks for the selected
process/window before claiming usable data. A `failed` receipt or nonempty
stdout alone does not establish a profile. Retain failure reasons and stderr.

macOS CPU/Allocations produces `profile.trace/`; Linux CPU produces `perf.data`;
VM and malloc_history produce `tool-output.txt`. `tool-stderr.txt` and tool-version
metadata explain adapter failures. A native `.trace` is a directory; preserve the
whole directory. The bundle is a separate snapshot with references to captures.
A manifest's `files` inventory is the authority for what was retained.

Gateway parent CPU captures also retain `sql-cpu.json` when the local SQL
collector is available. SQL schema v2 contains the full retained `groups` table,
ranked after aggregation by measured CPU (measured zero precedes null), rather
than the first 64 groups. Read `sql_cpu.status` **and**
`sql_cpu.aggregation.complete` in the manifest: `recorded` alone only confirms
storage. A complete aggregation means all observed groups fit independent
first-admission reservations: up to 20,000 measured keys (including measured
zero), plus 8320 initially unmeasured keys (8192 sequential completions and
128 active/incomplete scopes of headroom). The extra allowance is finite;
segment/simultaneous-scope limits do not bound all unmeasured completions.
The full fingerprint/workload/site/control key is preserved. Text allowances
are cumulative per class: 2,560,000 measured / 1,064,960 unmeasured UTF-8 bytes,
not a truncation of individual keys. Current fixed labels fit <=95 bytes/key.
An admitted key keeps its original charge if later measured, and all its updates
continue accumulating without admission. Early unmeasured keys cannot spend the
measured reservation.

Completeness does not assert full SQL instrumentation or all process CPU
coverage. An incomplete table names its first exhausted class/count/text budget
in `incomplete_reason`; `coverage.group_overflow_scopes` counts rejected scopes,
not an estimated number of distinct lost keys.

`aggregation.accounted` equals the sum of retained groups plus disjoint
`group_overflow`, for each counter and measured CPU. `measured_unknown` overlaps
null-fingerprint groups and possibly overflow; never add it again. CPU null means
unmeasured, while zero is a successfully measured zero. Error/cancellation
counters can overlap completed/incomplete and are not extra calls. Scope/segment
losses, unknown fingerprint/workload, clock failures and unfinished segments
remain separate coverage facts; unobserved CPU is unknown, never residual zero.
Native status/window remain independent: a stored SQL table does not repair a
failed native profile. A storage failure reports `status=unavailable` and retains
summary counters, without claiming that the table was saved.

Eligible Gateway parent CPU captures reserve a maximum 44,703,744-byte compact
SQL sidecar, derived from all 28,320 admitted keys, cumulative text with JSON
escaping, maximum-width counters and metadata. The full table is only in this
local sidecar; read it with its SQL byte bound, not the generic 8 MiB JSON limit.
Manifest/API/CLI carry a short summary and file reference. Their existing 8 MiB
JSON/read limits remain; the full manifest allowance is reserved before native
spawn, including build provenance which may use the library fallback.

Inside the unchanged 128 MiB capture budget, native artifacts plus owner/active
markers are limited to 81,125,376 bytes, 1022 files and 2046 directory entries.
SQL JSON and manifest each have a reserved file/entry while the active lease is
still held. Inventory before manifest has a 120 MiB allowance; the manifest can
use up to its existing 8 MiB limit. Checks run before spawn, periodically and
after native exit/stop, including final flush. Overshoot can trim/discard owned
native output with a partial/failed status; the sealed SQL table survives native
failure recovery and is then saved independently. Disk/permissions/checksum
errors can still make SQL storage unavailable; consent revocation retains its
existing priority over retention. Other capture modes keep their limits.

Artifacts are private local data. Retained captures are bounded to 128 MiB each,
16 entries / 512 MiB total and 7 days. Native output is checked periodically and
can overshoot temporarily. Overflows may leave partial data or omit a profile.
Preparation logs use a separate budget/namespace. These limits neither cap total
profiler RAM nor reserve 5 GiB free disk. Check volume capacity yourself before
and during collection; cancel diagnostic work early if resources approach limits.
Do not increase quotas or patch timeouts as a routine reaction to a failed capture.

## Read the results

For macOS `.trace`, use an installed Instruments reader or xctrace export. Start
with the table of contents to discover actual schemas before requesting rows:

```bash
xcrun xctrace export --input "$diag_trace" --toc \
  > "$diag_case/trace-toc.xml"
```

Use the installed tool's help and that TOC for the exact export query. Do not
hard-code a table path from another Xcode version. Confirm the correct target,
threads, sample window and module identities. Compare inclusive/exclusive stack
weights and distinguish active CPU samples from waiting stacks. A frequent frame
can be an ancestor of the expensive leaf, so include the call path in the finding.

For Linux perf, `perf buildid-list -i "$diag_perf_data"` identifies modules;
`perf report -i "$diag_perf_data" --stdio` opens the capture. If symbols are
missing, use the exact executable/debug object from `symbols.md` and the tool's
supported build-ID/symbol search configuration. Do not treat unresolved addresses
or a locally rebuilt executable as named source evidence.

`allocation-history` describes recorded allocation/free activity since prepared
launch. `live-allocations` groups allocations still live at inspection. Confirm
that the reported sizes/call counts mean what the native output says; one
aggregate call count is not necessarily the count of currently live objects.
Repeated comparable live snapshots can show growth, but retaining allocation
stacks does not prove who owns the memory or why it remains reachable. Memory
mapping categories and RSS/footprint are different scopes. Do not claim a complete
Rust retain graph or GPU accounting from these tools.

## Failure decisions

- `preparation_required`: use attach allocation churn or VM if sufficient;
  otherwise prepare the external-user restart handoff, never restart yourself.
- `tool_timeout`: retain partial data and measured runtime. For macOS
  malloc_history use explicit `--seconds 60` for a single justified attempt, not
  the short default. A previously slow run is not a universal timing guarantee.
- `capture_busy` / storage/preparation busy: identify the current owner; do not
  spawn another profiler or delete its lock. Revisit only when it is finished.
- Identity changed, reused PID, exited target or changed consent epoch: do not
  rebind old evidence to a new run. Rediscover and report the boundary.
- Permission/tool/version or overflow failure: report the actual reason and
  available lower-cost evidence. Preserve data before proposing a narrow repair.

One capture should answer one concrete question. Do not automatically repeat
controls, long workloads, memory captures or whole-project tests to qualify the
entire product during an incident investigation.
