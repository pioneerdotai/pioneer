# CLI Pioneer task tools — review notes

- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/cli-pioneer-task-tools`
- Branch: `feature/cli-pioneer-task-tools`
- Base: `df89ef2442622f2c6c4275caa9ae1b8a10eae7c2` (local main at start; no fetch/pull).
- Tests must not run before explicit external acceptance. Compilation and formatting only.

## Scope and requirement map (before implementation)

| Requirement | Existing implementation | Necessary adaptation |
| --- | --- | --- |
| Native definitions and CLI projection | HTML-specific projection/identity/quota/recovery branches | Small typed family registry; exact reserved identity; bounded external allowance |
| Task semantics and authorization | GatewayTaskToolProvider, task schemas/handlers, execution-bound action writer | Materialize those same tools for authorized CLI executions, including start options |
| Wait/cancel | DurableWait and task activity observer; cancellable MCP bridge | Preserve runtime execution class and unbounded activity-backed wait |
| Mutation replay | Action receipts and task mutation keys; native item correlation | Keep canonical native call identity and durable output lookup across MCP history |
| Parent finalization | CLI uses revision only; native uses task finalization snapshots | Gate success on authoritative pending/review/observations and continue same logical execution |
| Skills | Canonical tasks/subagents, existing export receipts; required skills rejected | Narrow runtime support allowlist, automatic selection, explicit CLI bindings and mandatory policy |
| Native behavior | Separate visibility/request_tools and task runtime | Leave native visibility path intact |

## Decisions and self-audit

Initial inspection confirmed the OpenCode facade lacks HTML quota adjustment; Codex/Claude and recovery use HTML-specific exceptions. No database transactions will be extended across CLI/task waits. Memory and unrelated worktrees are out of scope.

## Validation

No tests run. Validation is limited to compilation and formatting; behavior has not been confirmed by execution. This document records implementation self-review, not external acceptance.

## Implemented design

`turn_mcp/native_tools.rs` is the internal family registry for HTML and Tasks. It owns reserved identity, canonical definitions, authorization action, execution timeout metadata derived from the original specifications (no invented Task transport deadline), exact catalog allowance, shared Task materialization, and logical CLI mutation call identity. The native agent still uses its original providers, visibility and `request_tools`; registering a definition does not expose it there. `threads_start_options` has one protocol definition used by both paths.

CLI admission first projects authorized actions, then narrows the catalog against the persisted Agent identity/capabilities before freezing it. Message mode retains its previous HTML-only behavior. Native tools are exempted from external-server capability admission only by reserved source identity; each task invocation additionally revalidates its own action and uses the existing capsule/reviewer/service checks. External resolution is bounded before native definitions are appended. All three adapters count only exact internal definitions; Claude's existing file entries remain separately provider-owned.

Tasks execute through the normal shared tool runtime and `GatewayTaskToolProvider::for_cli`, with the original schemas, handlers, permissions, Task Service, launch inheritance, review policy and result projection. No memory tools are registered. Gateway supplies workspace/thread/turn and root context. CLI tool history preserves native item IDs; `cli_task_call_id` derives a turn/tool-scoped action identity, and explicit mutation keys survive changed provider call IDs.

The existing action receipt carries a CLI call fingerprint/key. Task creation commits its task/trigger/run references in that same receipt transaction. A lost response returns the stable creation references from the receipt without requiring or disclosing current Task state; other committed mutations return an explicit already-committed result with their target references. Live concurrent deliveries share the existing mutation cache/lock, with argument fingerprint validation. The cache is not the durability guarantee. Recovery also supplies IDs of Tasks already created by the logical turn, including scheduled and detached Tasks, so an absent response is not presented as failed creation.

The parent completion gate reads `GatewayTaskToolProvider` finalization snapshots, blocks on pending/review-required/unobserved terminal results, and reuses `task.terminal.observed`. A withheld completion prepares another native attempt within the same logical Turn and execution window. Its durable input and the source terminal ACK are committed atomically with that attempt. The existing provider start/recovery path resumes the same provider conversation with the same frozen authorization, tools and skills; it receives task facts and an explicit instruction not to repeat the original request. Task-enabled recovery, as well as ordinary orchestration continuation, prepares and activates a fresh native-attempt MCP lease for all three providers; Codex/Claude reuse a compatible process and the manager retains its existing checkpoint barrier for necessary replacements. OpenCode retains its existing server replacement behavior. Cancellation and terminal parent cleanup continue through existing owners. Snapshot and completed-attempt reconciliation use this gate too. The final commit uses the accepted task revision. For a task-capable CLI producing no final message, an empty, deterministic final item supplies the existing atomic finalization fence; legacy no-task/no-message completion keeps its previous path.

Required-system-skill policy remains unchanged globally. Only canonical system owner `pioneer`, slugs `tasks` and `subagents`, are supported for CLI export. They are selected automatically when actual `task_*` operations are projected (options-only access does not trigger orchestration policy), installed with the existing folder/receipt mechanism, and restored using existing fingerprints (canonical package patch versions bumped for the CLI binding). The files and references are copied unchanged from their canonical directories. Runtime bindings explain exported skill names, provider-qualified tool names and relative reference paths. Mandatory orchestration policy is included in each corresponding CLI instruction plan; native and CLI policies share rule paragraphs and differ only in their explicit loading binding.

## Invariants and self-audit findings

- **Authorization:** frozen projection is an admission ceiling, not a current grant. Invoker checks current task action; materialization and handlers retain capability, capsule, exact reviewer and current resource checks. CLI access alone grants no Task action. No model-owned root/turn/capsule parameters were added.
- **Domain failures:** CLI Task results reuse the native handler sanitizer, so `task_state_conflict`, access denial, invalid arguments and unconfirmed operations retain their domain meaning. Call cancellation remains transport cancellation. State conflicts are not mislabeled as permission denial.
- **External limits:** exact internal source/name/schema/description/version identity receives allowance. Matching names alone do not. OpenCode's missing HTML allowance is corrected by the shared count.
- **Codex client timeout:** final verification caught that a null normalized config field did not disable the actual 60-second client default. A generic native-spec deadline policy now selects and attests the explicit long-call ceiling, including recovery fingerprints; external lookalike names cannot select it. This required a narrow extension to the existing managed Codex config serializer, not a protocol change.
- **DurableWait:** native execution class is retained in CLI history. Confirmed Task activity reaches the existing timeout supervisor. Ending a wait window or cancelling an MCP call does not cancel a child. The bridge/facade retain their bounded queues, cancellation, isolation and existing MCP protocol.
- **Replay:** self-audit found the create-commit/response/anchor gap and closed it with references committed in the existing action receipt. Turn-scoped call identity avoids collisions from reused native IDs; explicit keys cover provider-ID changes. Concurrent identical keys are serialized and different arguments rejected. Receipt keys use the existing trimmed-key identity; the provider matrix exercises an explicit key with surrounding whitespace across response loss. Native invocation semantics are unchanged.
- **Replay authorization:** final self-review caught an unauthorized live-state read while reconstructing creation output under a create-only grant. Replay now returns only committed references and directs current-state observation through the ordinary authorized tools. It does not republish an anchor or expose new status, title, configuration or waitability.
- **Parent completion:** self-audit found revision-only admission insufficient, then found recovery snapshot bypasses and the no-message fence gap. All have a semantic gate or the existing revision-fenced finalization. The finalization candidate is retained if a transient snapshot read fails; success remains fenced, avoiding lost provider text.
- **Completed-attempt recovery:** final tracing caught that ordinary terminal-event admission intentionally excludes completed native attempts. Task continuation has a narrow authoritative-completed-attempt source, still requiring an InProgress logical Turn, exact identity, live CLI owner and unchanged latest attempt in its writer. Ordinary terminal-event and observational Blocked guards remain active-only. Saved-outcome replay reports Deferred while the logical Turn remains InProgress, so its timeout/command supervision is not retired merely because a native completion was consumed. Existing terminal ACKs are preserved and no ACK is invented when there is no saved outcome.
- **Final candidate race:** deferred final messages retain an exact native-attempt/item marker in the existing journal. If concurrent authorized detach removes the last blocker before terminal handling, only that candidate can be promoted behind the semantic/revision gate; text from earlier attempts is never selected.
- **Continuation lifecycle:** self-audit caught that ordinary orchestration must not open a new budget window. New native attempts retain the current execution window and do not consume failure budget merely for waiting. Preparation failures remain observable and use existing bounded recovery. A post-admission projection failure terminalizes through Blocked instead of leaving an orphan running turn.
- **Stalled orchestration:** final review found that repeated native completion without a Task action could loop. The existing native journal now persists the Task revision, terminal-tool count and consecutive empty-completion count. Three repeated empty completions block through the existing guarded Blocked/cleanup path. That writer now also accepts an explicitly identified pending terminal delivery, checks its exact current source, and commits its ACK with Blocked atomically; ordinary observational Blocked still cannot consume pending outcomes. A finished tool call (including provider timeout/cancellation) or a changed Task revision resets the count; ordinary long waits do not count as failures.
- **Database:** no new connections, SQL bypasses, manual permits or long transactions. New continuation transaction validates source execution, active CLI owner/lease, canonical Turn status and the pending saved terminal outcome (or authoritative Completed native attempt) before ACK/attempt transfer. Idempotent preparation also checks current owner and Turn authority before returning an existing attempt. Task snapshot data can become stale only toward an extra continuation; it cannot authorize successful finalization. Hashing, serialization, provider I/O, child waits and notification fanout occur outside database capacity. Task creation references are prepared before its existing atomic writer. Existing scheduling handles are retained.
- **Provider continuity:** final review replaced forced Codex/Claude process closure with terminalizing only the prior MCP attempt lease and reusing the exact process contract. Any necessary replacement passes through the existing manager/provider checkpoint barrier; Claude transcript continuity is not inferred from Pioneer history.
- **Fixtures:** required orchestration packages are materialized through the bundled mechanism in temporary test roots. The recording provider derives projection fingerprints/session generation from its actual launch spec; production checks were not relaxed to accommodate recovery fixtures.

## Review entry points and requirement coverage

| Contract | Code to review |
| --- | --- |
| Shared definitions, identity, exact allowance | `turn_mcp/native_tools.rs`, `turn_mcp/projection.rs`, three provider `*_mcp.rs`, frozen `cli_runtime/mcp/recovery.rs` |
| Admission ceiling versus current authorization | `message/turn_handlers.rs::{cli_native_tool_definitions, finalize_cli_native_tool_projection}`, `turn_mcp/invoker.rs`, existing Task authorization and handlers |
| Same Task business behavior and errors | `mcp_service.rs::{materialize_cli_task_tools, execute, canonical_task_error_result}`, `task_tools/mod.rs`; the original Task Service owns state transitions |
| Durable idempotency and raw/native/logical IDs | `native_tools::cli_task_call_id`, `TaskToolHandler::{bind_cli_mutation_receipt, prior_successful_mutation_output}`, `TaskService::create_task` receipt commit; existing provider correlation ledgers remain unchanged |
| Completion, restart and repeated terminal events | `cli_runtime/handlers.rs::{cli_task_finalization_state, continue_cli_attached_task_work, continue_cli_tasks_from_terminal_snapshot, prepare_cli_runtime_success_finalization}`, shared attempt start/recovery |
| Atomic continuation and bounded failure | `CrudStore::{prepare_cli_runtime_task_continuation, cli_runtime_completed_task_continuation_source, materialize_cli_runtime_blocked_turn_guarded}`; same owner/window, guarded ACK transfer, existing Blocked cleanup |
| Skills and mandatory instructions | `message/skills/catalog.rs`, `cli_runtime/{skills,context}.rs`, `promt::compile_cli_runtime_delivery_plan`, canonical package directories and existing export receipts |
| Native visibility and execution | Agent visibility/request_tools implementation is untouched; shared schema extraction preserves names/inputs, `cli_replay=false` retains original Task provider path |

## Provider waiting limits (source review, no runtime probes)

Final source review found that Codex's existing `tool_timeout_sec: null` was only effective-config normalization, leaving its actual default 60-second MCP deadline. Managed Codex projections with a trusted native specification lacking a hard deadline now explicitly select a 12-hour client ceiling, aligned with OpenCode's existing allowance. Gateway limits on external calls remain unchanged. The deadline participates in staged/effective config attestation and semantic restart identity; HTML-only/legacy fingerprints retain their previous contract. Equivalent integral/floating duration encodings normalize numerically, while different fractional values still fail attestation. Claude's managed task-enabled process disables automatic MCP backgrounding and its separate idle timer; the Task Service still owns observation/activity behavior. The existing overall provider execution limit remains in effect. OpenCode's managed server explicitly retains its documented 12-hour execution allowance, independently of startup/catalog deadlines. No short task_wait transport timeout was introduced. A provider deadline ends its call, not the child Task.

Sources inspected: [Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference), [Claude environment variables](https://code.claude.com/docs/en/env-vars), [OpenCode V2 MCP timeouts](https://opencode.ai/v2/docs/mcp-servers#timeouts). Global CLI configuration and user skills were not changed.

## Regression changes (not executed)

- CRUD transition regressions: injected write failures roll back continuation input/ACK/attempt and Blocked/ACK together; concurrent interactive/maintenance writes and reopened-store replay preserve one attempt and the original execution window; stale-owner replay is rejected. Existing physical reader/writer and queued-cancellation conformance coverage is retained.
- Codex deadline regression: an explicit long-call ceiling survives staged/effective attestation, affects restart identity, rejects changed/malformed durations, and preserves legacy golden configurations.
- Registry/provider matrix: full external catalogs plus HTML and all task definitions; schema preservation and exact allowance for Codex, Claude and OpenCode; lookalike external names and undersized limits.
- Frozen recovery fixture now includes task tools, shared schemas and DurableWait metadata alongside external annotations.
- Logical call identity: response replay, changed provider ID with mutation key, and cross-turn/tool isolation.
- CLI integration matrix: automatic skill delivery and reference-file preservation; scheduled creation without options lookup; lost-response replay; mutation-key mismatch; pause/reschedule/resume/wait/get/cancel.
- Attached CLI parent: early completion continues using the existing child ID and duplicate terminal events do not create another attempt.
- Existing foreign-capsule matrix: a real scheduled task in another authorized CLI root remains invisible/unmanageable to the first root.
- Concurrent detach matrix: a deferred final candidate is promoted only after the last blocker disappears, preserving the original text.
- Lost wait response: a provider-side failed observation remains DurableWait in history, resets empty-orchestration accounting and does not cancel the child.
- Stalled parent matrix: repeated completion without any task/tool progress is bounded and produces Blocked.
- CLI review matrix: wait window does not cancel, exact candidate read, revise, accept, detach and cancel through the existing service contract.
- Prompt/runtime support checks: mandatory orchestration policy and narrow required-system-skill allowlist. Existing native task visibility/request_tools and capsule/review tests remain in place.

Future commands, **only after full external review, fixes, and explicit acceptance**:

```sh
cargo test -p pioneer-cli-agent-runtime codex_mcp_config::tests
cargo test -p pioneer-crud tests::cli_runtime_active_bindings
cargo test -p pioneer-gateway turn_mcp::native_tools::tests
cargo test -p pioneer-gateway cli_runtime::mcp::recovery::tests
cargo test -p pioneer-gateway message::tests::cli_task_tools
cargo test -p pioneer-gateway cli_runtime::skills::tests
cargo test -p pioneer-gateway task_tools::tests
cargo test -p pioneer-gateway message::tests::task_capsule
cargo test -p pioneer-gateway message::tests::agent_mode_materializes_task_tools_and_chat_mode_does_not
cargo test -p pioneer-gateway message::tests::capability_persistence_order_materializes_before_skill_install_and_native_start
cargo test -p pioneer-promt cli_runtime_context::tests
cargo test -p pioneer-agent
cargo test -p pioneer-tasks
```

## Executed checks and remaining limits

- Read applicable root AGENTS.md; inspected clean main and worktree list before changes; isolated the worktree from the recorded local-main base.
- `git diff --check`: final pass, exit 0.
- `rustfmt --edition 2024 --config skip_children=true --check` on all changed Rust files: final pass, exit 0.
- Compilation only: initial `cargo check -p pioneer-gateway --lib` and subsequent `cargo check --tests` exposed type/privacy/future-boundary issues, which were fixed. An early compilation was interrupted after its error when its own incremental cache consumed disk; only this worktree's generated incremental cache was removed. Superseded compilation snapshots were stopped and restarted after self-review fixes; only compiler processes verified to belong to this worktree were interrupted. The final command below passed on the unchanged source snapshot.
- A temporary editing helper was initially written in `/tmp`; it addressed only this worktree and has been removed. Project changes are confined to the created worktree.
- No tests, applications, CLI scenarios, e2e scripts or doctests were executed. No fetch/pull, merge, push, branch/worktree removal or user data modification.
- Behavior is not claimed to be runtime-validated. Provider execution ceilings and the existing recovery/receipt retention contracts still apply; this work does not promise semantic deduplication of genuinely new tool operations without a shared call/mutation identity. External review and the authorized post-acceptance test run remain outstanding.
- Existing Task Service review-time detach currently uses an untyped rejection, which the native sanitizer reports as `task_operation_failed`. This behavior is preserved; forbidden detach does not change the candidate or attachment. Broader Task error taxonomy changes are out of scope.
- Unrelated pre-existing warnings in CRUD artifact helpers, Gateway voice code, an unused episodic-workspace test helper, and CRUD compaction-preparation test variables were observed and left unchanged.
- The primary checkout now points to `4b4fef29973c7c1ea99e20700b48a6bafa2309a3`. This branch intentionally remains based on the recorded local main at task start; no merge/rebase or changes to that checkout were performed.

### Final verification

- `CARGO_INCREMENTAL=0 cargo check --tests -p pioneer-cli-agent-runtime -p pioneer-gateway -p pioneer-crud -p pioneer-promt -p pioneer-protocol -p pioneer-tasks` — **passed**, exit 0 (16m 30s). This compiled test targets and did not execute tests.
- Intermediate Gateway library and Gateway/prompt/protocol/tasks test-target compilation also passed before the last self-review fixes; superseded checks are not presented as final verification.
- The 39 changed source files remained unchanged throughout the final successful compilation (verified by a content hash); the report was completed afterward.
- Final self-review traced admission → frozen provider projection → invocation/current authorization → Task mutation/wait/review → parent continuation/finalization → saved-outcome and completed-attempt recovery. No known required implementation item remains deferred. Execution validation awaits explicit external acceptance as instructed.
- The local review commit includes code, unexecuted regression tests and these notes. Git hooks/signing are disabled only for that commit command to prevent indirect test execution or interactive signing; repository/global configuration is not changed. No merge/push was performed.
