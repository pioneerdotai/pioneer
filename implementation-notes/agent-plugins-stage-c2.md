# C2 provider inventory and native transport prerequisite

Status: **READY_FOR_STAGE_C2_REVIEW — BLOCKER; C2 NOT_COMPLETE**.
This delivery invokes the explicit missing reliable close/ACK exception in
`stage-c2-implementation-prompt.md` §4. It does not claim working CLI plugin
selection, coordinator acceptance, behavioral validation or permission for D/E.
Dependent plugin CLI preparation/admission/continuations remain closed until the
native ownership/stop contract below can be reviewed and completed.

## Snapshot

- Branch: `feature/agent-plugins-simple`.
- Worktree and cwd for commands below:
  `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`.
- Baseline: `80af78634261ccc48d0cd75b3261bb6359f0a905`, initially clean.
- Code HEAD: `f435854b9db4f026d637558b42b95238c7b1b157`. Delivery adds only this handoff; final HEAD is
  `git log -1 --format=%H -- implementation-notes/agent-plugins-stage-c2.md`.
- Main, archive branches/worktrees and mobile were not modified; no merge,
  reset/clean, push, deployment, app/provider/fixture/migration execution.

Read applicable repository AGENTS, current proposal v2, B rereview, native stop
final review, full C1 review plus accepted C1 rereview, and A/B/C1/C1-stop handoffs.
No desktop UI changed; no new tables, installers, OAuth, native client/FFI
dependencies, plugin coordinator, jobs/leases or generation management added.

## Delivered native helper

`cli-agent-runtime/src/codex.rs` extends the **existing** `CodexJsonlRpcOwner`:
`abort_and_join_result(&mut self)` joins its actual reader, RPC dispatcher and
ordered-ingress workers. Handles are awaited by reference and removed only after
join; cancellation retains the unfinished handle. A consumed panic is sticky,
other workers still drain, and a retry cannot turn an empty vector into success
after a failed join. Expected abort cancellation is accepted only after actual
join. Existing best-effort `shutdown`/`abort_and_join` and detached constructors
retain their APIs. New owned capacity/budget constructor calls the same existing
worker factory and passes the same configured limits and native event budget.

`gateway/src/cli_runtime/codex_session.rs` uses that owned constructor in the real
persistent factory. Explicit initialize failure joins the transport after the
existing best-effort process cleanup (not a new startup stop proof). A published
session retains the owner behind its native mutex;
normal close waits for process termination/stderr, then joins the transport,
then removes the overlay. Transport failure prevents overlay cleanup. Isolation,
attestation, launch spec, MCP config/projection, permissions and OAuth unchanged.
This is **transport** completion, not complete provider/tools/MCP stop proof.
There is no DB capacity or registry lock held across the new joins.

## Concrete blocker and smallest remaining native contracts

1. `cli_runtime/manager.rs::{start_and_publish_locked,close_session,
   close_session_instance,close_all}`: factory spawn/handshake precedes publication;
   cancellation during factory startup drops local process/tasks without retained
   wait evidence. Close removes the cached owner **before** the native await and
   calls `after_session_close` even on error. Timeout/cancellation loses inventory;
   a subsequent `false` close is only absence. `remove_if_generation` releases on
   event EOF without process/tool join. Existing exact `CliSessionInstanceId` and
   per-key start lock already exist: extend these seams to publish native cleanup
   ownership at spawn before handshake awaits; retain a closing owner in the same
   manager until result-returning drain completes, with failed/cancelled close
   blocking reuse/replacement. Do not derive proof from EOF/terminal DB status.
2. `claude_session.rs::ClaudeStreamClient::{spawn_reader,new}` detaches the reader
   and OrderedEventIngress worker; `ClaudeCLIAgentRuntimeSession::close` waits only
   for process/stderr before deleting its managed root. Retain these exact workers
   and join through the existing session close, using `spawn_owned` already offered
   by runtime-events. Pending request senders and ingress publication must finish
   or fail, with cancellation retaining handles, before deleting projections.
3. Both `*RequiredMcpBridge::fail_closed` replace their state with Failed, abort and
   **drop** the server JoinHandle. Supervisor revoke returns a bool and suppresses
   cleanup errors. `mcp/server.rs::run` already drains facade/call JoinSet on normal
   termination; aborting it skips that awaited path. Extend its existing handle to
   request normal shutdown, retain and join that actual server, propagate drain
   errors, then revoke. Native installation stop alone does not join the facade's
   nested Gateway calls. Reuse those existing handles, not a second executor.
4. `cli_runtime/handlers.rs::interrupt_and_close_cli_runtime_binding` returns `()`
   and logs interrupt/close errors; it captures a handle but finally closes by
   logical key, so a replacement can be targeted. Use the captured instance for
   the result-returning stop seam. Detached event/request pumps also need their
   existing instance's publication/request completion evidence. Accepted startup,
   recovery and closing instances must all be in lifecycle inventory before a
   plugin mutation can report stop success.

These are observed source contracts, not hypothetical platform limitations.
The helper above repairs the Codex transport portion only. Enabling plugin CLI
selection now would permit file swap after lost startup/close ownership. The
existing rejection and graph CLI-binding failure are deliberately retained under
the task's §4 exception; they are **unresolved C2 work**, not supported behavior.

## Existing entrypoints → gap → next minimal change → source coverage

Paths below are relative to `crates/gateway/src` unless a crate is named.
“Pending” means NOT_IMPLEMENTED / regression NOT_WRITTEN in this delivery.

| Existing entrypoint | Current behavior/gap and next change | Source regression |
| --- | --- | --- |
| `message/turn_handlers.rs::normalize_turn_skill_capabilities`, native prepare/start | API trusted parent expansion, prepared snapshot, presentation parent, tool-capable provider check and final parent admission exist. Reuse them; do not trust public child IDs/labels. | Existing B/C1 sources NOT_RUN; unchanged |
| `message/agent_runtime.rs` durable TurnSkillsResolved; `crud/src/plugins.rs::ready_plugin_selection` | API ready uses committed native bindings and resolver IDs; parent/ownership revalidated in writer, empty allowed. CLI needs this same transition after its actual native binding writes. | Existing B missing binding/exclusion sources NOT_RUN; CLI pending |
| `message/artifact_tools.rs`, `mcp_service.rs` late dispatch | Existing `plugin_turn_child_available` gates API read_skill/MCP on ready snapshot and current parent. Preserve compact catalog/full text read_skill. | Existing B/C1 NOT_RUN; unchanged |
| `message/turn_handlers.rs::turn_start_cli_runtime` | Explicit parent rejection before normalization. After stop prerequisites, expand server-side, retain parent presentation, prepare snapshot and ready actual bindings, hold parent admission through native publication. | Pending |
| `turn_handlers.rs::prepare_cli_runtime_combined_preflight`; `cli_runtime/skills.rs` | Native resolver/dependencies/trust and real materializer/receipts exist. Authored-name destination collides; only member folder copied. Extend that builder/copier for opaque owned aliases/full contained context. | Existing standalone NOT_RUN; plugin aliases/assets pending |
| `cli_runtime/{continuation,manager}.rs` | Typed launch/reuse identity, existing exact instance and start locks exist. No retained startup/close proof. Extend those native contracts and share accepted parent admission. | New transport sources NOT_RUN; manager pending |
| `cli_runtime/{claude_session,codex_session}.rs` | Controlled launch/attestation exist; Claude workers and both nested bridges lack awaited completion. Codex transport now owned/joined; other stop seams above pending. | Three new native unit sources NOT_RUN; full close pending |
| `cli_runtime/mcp/{server,supervisor,coordinator,recovery}.rs`; `claude_mcp.rs`, `codex_mcp.rs` | Existing frozen MCP projections, activation/generation checks, invoker and facade. Reuse actual native bindings; no unmanaged package/OAuth. Normal server drain exists but fail_closed discards its handle. | Existing facade sources NOT_RUN; strict lifecycle drain pending |
| `turn_handlers.rs::{restore_cli_runtime_launch_spec,start_cli_runtime_recovery_attempt}`; `resilience/recovery.rs` CLI branch | Restores durable authority/cwd/projection and provider binding. API recovery takes parent guard; CLI routing precedes that API guard. Add exact parent/revision gate and native inventory before CLI acquire/resume/publication. | Pending |
| `cli_runtime/{turn_binding,turn_recovery}.rs` | Pre/post-start attempt bindings and bounded readiness recovery scanner exist; they are not process ACKs. Keep persistence, add snapshot/admission at actual provider boundary. | Existing binding/recovery NOT_RUN; plugin pending |
| `cli_runtime/thread_binding.rs`; `turn_handlers.rs` resume/retry/fork/history bridges | Exact provider-session/fork/context receipts exist; skill restore checks native receipt/hash. Fresh parent gates/revision must precede continuation; old pending native requests cannot grant new revision. | Pending |
| `cli_runtime/handlers.rs` steer/fork/request respond | Real steer/fork reuse existing sessions and authority. Add same parent guard/current snapshot and captured-instance stop; management-only native sessions stay isolated. | Pending |
| `cli_runtime/handlers.rs::cli_runtime_review_start` | Already rejects managed runtime review; existing concrete endpoint limitation, preserve error. | Existing behavior unchanged |
| `message/task_handlers.rs`; `turn_handlers.rs::prepare_task_cli_runtime_turn` and detached task branch | Task admission normalizes capabilities; prepared CLI task delegates to the same CLI start. Detached parent plugins are explicitly rejected. Preserve parent in persisted launch/presentation and normalize again at each actual start, after stop prerequisites. | Pending |
| `message/agent_runtime.rs::select_native_graph_owners`; `message/plugins.rs::stop_plugin_execution` | Actual native API drain/MCP stop precede mutation, CLI binding explicitly fails. Extend existing inventory/drain to captured CLI instances; no bool/DB terminal status as stop proof. | Existing API stop NOT_RUN; CLI pending |
| `cli_runtime/{instruction_projection,input_mapping}.rs` | Frozen elevated instructions and text/file mapping exist. Keep native instructions; owned Skills use genuine native invocation items, not text-tool emulation or selectable child chips. | Unchanged; plugin continuation pending |

## Asset/alias and mixed-package source trace (not a runtime result)

API baseline: one parent → authoritative eligible native IDs → native bindings /
ready → compact catalog + read_skill and existing MCP invoker. Ordinary bundled
skill context is `parent/package/skills/<member>` with siblings available in that
package; `skill_source` uses the entire actual native uploaded installation without
fallback. Standalone keeps native definition/folder. All use native policy/trust.
Parent gate/revision, graph drain and actual MCP stop precede update/disable/remove;
stale continuation requires reselection. These accepted source paths were not run.

Actual **standalone** Codex path: `build_cli_runtime_skill_install_plans` currently
builds `sanitize_name(authored_name)` → configured native home `skills/<name>` →
real `replace_external_runtime_skill` + receipt/folder hash →
`prepend_codex_installed_skill_items` sends native `Skill {name,path: .../SKILL.md}`.
Actual **standalone** Claude path: same native materialization →
`options.selected_skills` → `materialize_claude_selected_skill_plugins` creates
`selected-skill-plugins/pioneer-selected-skill-<index>/skills/<name>` and exact
generated manifest → strict `--plugin-dir` on that managed projection only.

The current alias builders are `build_cli_runtime_skill_install_plans` and the
Claude managed plugin-name loop above; neither yet derives parent/SkillId aliases.
No opaque alias is claimed delivered. For both CLIs, ordinary mixed-plugin paths
stop at the explicit parent rejection; stdio/OAuth HTTP therefore do not get a CLI
plugin snapshot/facade. A native override would copy its whole native tree under
the existing copier, while ordinary bundled folder-only copy loses sibling paths.
The next materializer change must export verified **full package** context under
the controlled destination and separately register only selected members, with
stable parent/SkillId aliases in generated projections only. Retain containment,
denied diagnostics, modes/hash, exact receipts and existing native invocations;
never rewrite installed SKILL or export unselected plugins as implicit capabilities.
Mixed/override/standalone CLI lifecycle source trace remains incomplete until stop,
alias/context, ready and continuation gates are connected. No runtime claims.

## Actual checks and regression status

| Command (cwd above) | Actual exit / evidence |
| --- | --- |
| `CARGO_INCREMENTAL=0 cargo check -p pioneer-gateway --lib`, initial dirty baseline | **101**, `target/plugin-c2-native-transport-check.log`; unqualified Result/error macro in the new helper, corrected |
| Same command, final source bytes | **0**, 4m39s, `target/plugin-c2-native-transport-check-2.log`; includes native CLI runtime library |
| `rustfmt --edition 2024 --config skip_children=true --check crates/cli-agent-runtime/src/codex.rs crates/gateway/src/cli_runtime/codex_session.rs` | **0**; both files also formatted |
| `git diff --check`; `git diff --check 80af78634261ccc48d0cd75b3261bb6359f0a905 HEAD` at code HEAD | **0 / 0** |

Compilation/formatting ran on dirty baseline `80af7863`; final production/test
source bytes are exactly those subsequently committed at code HEAD `f435854b`.
Only this handoff was untracked after that commit. Final delivery working tree
is checked after its documentation commit. Compiler notice: existing unused
`set_mcp_policy`. No protocol/generated contracts changed; schema/native artifact
generation was not needed or run. These are compilation/format results, not
provider behavior or conformance evidence. Commit hooks disabled.

New source tests in `cli-agent-runtime/src/codex.rs`:
`persistent_owned_transport_retains_capacities_and_closes_pending_rpc` (in-memory
transport closes pending request/channels and rejects late request),
`transport_join_cancellation_retains_pending_handle_and_consumed_panic` (actual
delayed task plus panic, cancelled waiter, other-task drain and sticky retry),
`old_transport_close_does_not_stop_replacement_transport` (independent native
transport remains usable). Native transport unit level, no provider/process fixture.
All own tests **NOT_RUN**; all own test targets **NOT_COMPILED**. Their types and
behavior have not been validated. Scoped rustfmt parses test source only.
Historical external test compilation remains **UNKNOWN_EXTERNAL_ACTIVITY**, not
this task's command/evidence. No application, provider, fixture, migration,
functional/smoke/device/browser run; no mobile or test stage begun.
