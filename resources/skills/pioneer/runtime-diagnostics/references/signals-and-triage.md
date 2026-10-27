# Process signals, work ownership, and live triage

Read for any investigation. Contents: discovery, snapshot commands, the four
signal families, backend correlation, interpretation, and reporting.

## Discover the real installation

Resolve the installed `pioneer` CLI and target executable, rather than a stale
worktree binary. Record `pioneer version --json` and `pioneer status --json` if
supported. Status includes the configured runtime home and installation state;
its service/reachability fields do not prove that local diagnostics is available.
The default home is `$HOME/.pioneer`, but configuration can change it. The CLI
loads `AppConfig`, including `PIONEER_CONFIG` when set. Use the target's same OS
user and effective configuration. There is no diagnostics `--runtime-home` flag.
Do not print an entire environment or configuration containing credentials.

Check whether the selected executable implements `diagnostics` from its version
and command output. `pioneer diagnostics --help` is not a supported discovery
command in this implementation: it is parsed as an action. Use `list`. An old
binary reporting an unknown command needs a newer installation managed by the
user, not a covert agent-driven upgrade/restart.

Use process metadata to identify the target PID, start identity, executable and
supervisor. Check parent/ancestor relationships to your shell, CLI runtime,
Gateway, and desktop. A service can host the agent without being its Unix parent;
unknown hosting relationships retain the no-restart rule from SKILL.md. Inspect
only the processes needed, rather than dumping all users' arguments or secrets.

## Collect without restarting

The following are POSIX shell examples. Set the variables to verified paths;
produce concrete commands with those values in a user handoff. On Windows use
PowerShell equivalents and absolute paths to `pioneer.exe`; actions and flags are
identical. Do not change persistent telemetry settings to make discovery work.

```bash
diag_cli='/absolute/path/to/pioneer'
umask 077
diag_case="$(mktemp -d "${TMPDIR:-/tmp}/pioneer-diagnostics.XXXXXX")"
"$diag_cli" diagnostics list > "$diag_case/targets.json"
```

Select exactly one `run.run_id` from the returned safe list (it omits endpoint
tokens). Save it in the case notes, then use it explicitly:

```bash
diag_run='EXACT_RUN_ID_FROM_TARGETS_JSON'
"$diag_cli" diagnostics snapshot --target "$diag_run" > "$diag_case/snapshot-before.json"
# Let a few configured sample periods elapse while observing the reported symptom.
"$diag_cli" diagnostics snapshot --target "$diag_run" > "$diag_case/snapshot-after.json"
"$diag_cli" diagnostics bundle --target "$diag_run" > "$diag_case/bundle-receipt.json"
```

Do not literally paste `EXACT_RUN_ID_FROM_TARGETS_JSON` as a target. Prefer an
existing user-owned case path with enough free space if `/tmp` is on a constrained
volume. A new snapshot does not force a fresh OS sample; compare its producer
sample timestamps. Default resource sampling is 5 seconds. Startup-configured
`PIONEER_RESOURCE_SAMPLE_SECONDS` clamps to 1..60 (invalid text falls back to 5);
do not restart the host or increase
sample frequency as the first diagnostic action.

A bundle receipt's `local_reference` is relative to `<runtime_home>/diagnostics`.
For example, `captures/pioneer-capture-ID/manifest.json` identifies a directory
containing `snapshot.json` and a bundle manifest. It is not a ZIP, an Axiom URL,
or a copy of all previous profiles. Its `existing_profiles` are references;
preserve their corresponding directories separately if the analysis needs them.
Bundles can be `partial` because no earlier profile or module ID exists. Read
`missing_reasons`; that is different from failing to save a valid snapshot.

If discovery returns nothing, check installed version, user/config/home, process
liveness and startup diagnostics availability. Unreadable/malformed descriptors
are reported as skipped, not as healthy targets. Do not edit tokens/descriptors,
copy another user's endpoint, or repeatedly restart services to repair discovery.
A `consent_disabled` response also blocks local snapshots/bundles/captures. Respect
that preference; explain the existing telemetry setting if the user wants to
change it. Do not silently enable it or patch the persisted TOML to bypass consent.

## 1. Identity and resource accounting

Every snapshot includes `run.run_id`, `run.pid`, `run.initialized_unix_ms` and a
`run.build` manifest. The build records revision/dirty state, target, profile,
features and provenance completeness when available. Use those fields to select
source. A version or source commit alone does not identify an exact executable;
validate native module identity for profiles as described in `symbols.md`.

`resources` contains:

- `self_process`, `host`, and `children`, with distinct sampling timestamps.
- `period_ms`, `last_success_monotonic_ms`, `last_success_unix_ms`, `stale`,
  failed/skipped sample counts and child overflow/history coverage.
- `self_process.raw`: OS lifetime `user_ns`/`system_ns`, start identity, and
  resource `values`. Each value carries `field`, nullable `value`, `status`,
  `source`, `unit`, `scope` and `temporality`.
- `cpu_status`, actual `elapsed_ns`, CPU deltas, `cpu_cores`, and separate
  `machine_fraction` and `quota_fraction` when their denominators are known.

One `cpu_cores` unit means one fully occupied logical core. Thus 1.0 is 100% of
one core, not 100% of the machine. For a valid interval:

`cpu_cores = (delta_user_ns + delta_system_ns) / elapsed_ns`

For two raw lifetime counters in the same process/start identity:
`core_seconds = delta(user_ns + system_ns) / 1e9`.
Never subtract counters across restarts or consent resets, sum repeated lifetime
gauges as increments, or turn `first`, `reset`, `gap`, suspend/clock discontinuity,
exit/reuse, unavailable or stale samples into zero. An observed zero delta is
meaningful; missing evidence is not zero.

RSS is resident memory; macOS physical footprint is separately reported. Neither
is allocator live heap. Adding RSS across children double-counts shared pages.
IO fields distinguish physical storage from logical IO where supported. Host
memory/pressure is machine/container context, not memory owned by the target.
macOS pressure is a source-defined level/bitmask, Linux PSI is a percentage,
and Windows memory load has its own source. Read `unit`/`source`, not just numbers.
Unsupported fields remain explicitly unsupported on each OS.

For children, match `generation` + PID + `start_identity` and check `role`,
`owner_generation`, `parent_run`, `work`, spawn/ownership-end/observed-exit times,
`outcome` and `final_usage`. `--child` uses this registration generation, not PID
or an owner/provider generation. Only registered active children can be selected.
Roles include CLI agents, MCP, shell, browser, platform/desktop helpers and
profilers. Short-lived children may exit between samples; dropped ownership does
not prove physical exit or complete final CPU accounting. Service-manager
processes and arbitrary grandchildren are not a complete observed process tree.
Do not charge profiler children to normal application work.

## 2. Function-level evidence

Resource snapshots establish which process is busy. They do not establish which
function spent the CPU. Use a bounded CPU capture during the symptom; see
`captures.md`. For memory, separate allocation churn, currently live allocations,
VM growth and an ownership leak. Increasing RSS alone does not choose among them.
Sampling stacks and work context are correlated by run/build/time. There is no
promise that every CPU sample belongs to a specific async work token.

## 3. Initiating work and execution boundaries

Read `live.active` and `live.journal` work records. `context` carries:

- local `token`, `parent` and up to four `links`;
- `kind`, `trigger`, `executor`, `attempt`, consent generation and `origin`;
- optional trace/span IDs, serialized locally as byte arrays.

Follow parent/links only within the same run and consent epoch. Tokens are local,
not globally unique request IDs. Render byte-array trace IDs as zero-padded hex
before searching the backend, rather than treating the array's JSON as an ID.
Use bundle `selected_trace_ids` when available. A linked operation can have
multiple causes; do not invent a single parent when the instrumentation records
links or an intentionally independent root.

Instrumented boundaries include RPC, message/task launches, post-startup work,
event/ordered ingress, client commands/RPC/publication, OS threads, blocking work,
children, timers and database operations. Context is captured before the boundary
and restored per poll/callback. Coverage is explicit: accepted context, missing,
invalid, locally rooted, or intentionally independent. Not every library-created
thread/task or external process is covered.

A worker lifecycle record means the worker exists; it is not the specific work
currently spending CPU. Use its active operation, progress and profile stacks.
Missing lineage and unknown database ownership are coverage gaps to report, not
proof that a particular domain caused the load.

## 4. Live work, queues, waits and responsiveness

Active records include `state`, `stage`, `started_ms`, `last_progress_ms`,
`state_since_ms`, optional deadline/backoff, `worker_lifetime`, lease count and
wait-overflow. States include queued, running, waiting, sleeping-until and terminal.
Wait categories include database admission/pool/blocking, channel capacity, queue
item, network/reply, child, timer, user/provider and lock. Stages include admission,
database, send/receive, decode, publication, callback, retry and shutdown.

Compare repeated producer snapshots: a long recorded wait plus low process CPU
suggests blocking; high CPU needs stacks. `running` is an instrumentation state,
not proof of continuous CPU execution. Parallel spans overlap; do not add all
span durations as an end-to-end critical path.

`live.queues` records existing queue owners, not a replacement channel system:

- Kind/capacity, depth, payload-byte quality and definition, and source timestamps.
- Offered/enqueued/dequeued/processed, rejected/cancelled/dropped/coalesced, stale,
  resnapshot/replay, committed/commit-rejected, send waiters and accounting errors.
- Optional oldest-item age and explicit age coverage, initial-depth/balance scope
  and metadata overflow.

Enqueue acceptance, dequeue, processing, delivery and durable commit are distinct.
A coalesced wake token is not a lost business operation. Bytes can represent just
wake tokens or estimates. Unknown oldest age is not zero; only owners with a
verified native head can supply it. An unbounded queue's absent capacity is not
zero capacity. Check `last_observed_ms` instead of assuming a new read refreshed
its owner data. Database admission/pool snapshots distinguish capacity and waits;
they do not identify SQL functions burning CPU without other evidence.

`runtimes.runtimes` exposes registered Tokio roles, workers, alive tasks, global
queue depth and responsiveness probes. `runtimes.foreground` covers a supported
desktop foreground loop. `expected_probe_overdue` is meaningful only when a probe
was expected on a driven runtime; `idle_no_expected_probe` and `not_recorded` do
not mean stuck. Watchdog ticks/late ticks describe the independent observer.
Foreign/library workers remain outside this registry. This is not per-task Tokio
CPU accounting or a complete UI frame-time/presentation measurement.

Live history is bounded to 120 seconds, 4,096 records and 2 MiB. Check first/last
sequence, `overwritten`, `lost`, active/queue overflow, abandoned work,
`metric_counter_overflow`, child coverage and runtime registration overflow.
The active-work limit is 512 and queue limit is 128. A gap means the record may
have been evicted or never observed; absence is not proof no work ran. Monotonic
clocks are process-local. Correlate different processes using their wall time,
run/build IDs and known clock uncertainty, not raw monotonic equality.

## Existing metrics/traces and Axiom

Use the available telemetry connector or existing authorized backend credentials.
Discover actual datasets, sample records and field layout first. Axiom ingestion
may rename or flatten OTLP fields; do not invent dataset names, field paths,
tokens, or a fixed APL query. Start with a narrow time window around the symptom.

OTLP resources carry `service.name` (`pioneer.gateway` or `pioneer.desktop`),
`service.instance.id` (the local run ID), `service.version`,
`pioneer.build.manifest.id`, build target/profile, environment and OS. Filter by
run/build and time before aggregating. Native module IDs are a separate symbol
contract. Metric labels are deliberately low-cardinality; do not expect a series
for every work token, PID, thread, SQL text or operation payload.

Useful instrument names (map to the backend's actual schema):

| Question | Instruments |
| --- | --- |
| CPU and validity | `pioneer.process.cpu.cores`, `.cpu.available`, `.cpu.fraction`, `.cpu.time.snapshot` |
| Memory/IO and source capability | `pioneer.process.resource.snapshot`, `.resource.status`, `.resource.available` |
| Sampler freshness | `pioneer.process.sample.timestamp`, `.sample.age`, `pioneer.process.samples` |
| Work/ownership coverage | `pioneer.work.active`, `.oldest.age`, `.boundaries`, `.context.loss` |
| Queue pressure/outcomes | `pioneer.queue.current`, `.oldest.age`, `.events` |
| Runtime and loss | `pioneer.runtime.current`, `pioneer.observation.loss.snapshot` |

In the table, a leading dot continues the same prefix. Resource/queue/runtime
aggregate fields and labels carry the meaning; inspect them before comparing.
OS lifetime resource/CPU and epoch loss snapshots are gauges, not increments.
New event counters use the existing Delta export pipeline. Work coverage ratios
use a denominator within one boundary **and phase**, not capture plus receive.
Child detail can appear in bounded `process.child.sample` spans. Aggregates and
sampling limit coverage; a trace duration remains elapsed time, not CPU time.

Check local `export_health`: `observed`, `unavailable_signals`, `failed_exports`,
metrics/traces last-success age and `backend_completeness`. The bitmask uses
metrics=1 and traces=2. SDK success reports the final transport result, not full
downstream retention or OTLP partial-success coverage. During an exporter outage,
local snapshots/bundles can still answer questions without inventing backend data.

## Report the finding

Lead with the answer in plain language. Include the selected process/run/build,
window and source, CPU/core-seconds or observed memory/IO, implicated stack/work
chain and artifact paths. Separate evidence from a hypothesis. State relevant
coverage gaps and profiler overhead, then give the smallest next action. If the
answer requires restarting the hosting process, provide the user handoff and
checkpoint instead of claiming that autonomous work can continue through it.
