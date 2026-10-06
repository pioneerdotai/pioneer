# C2 — trusted plugin selection across existing providers

Status: **READY_FOR_STAGE_C2_REVIEW**. Implementation is submitted for source
review; this is not coordinator acceptance, a behavioral pass, or permission for
mobile/D or testing/E. The earlier transport-only/blocker delivery is superseded
by this handoff; accepted stop prerequisite remains unchanged except the concrete
Turn-to-instance integration described below.

## Snapshot

- Branch: `feature/agent-plugins-simple`.
- Worktree / command cwd:
  `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`.
- Baseline: `707c3d5f7ed5228a7370ccc0a7b5e7f76b35d528`, initially clean.
- Projection/startup commit: `a3cd4f246e5e23c4d1cd1ee25f7773d92ff0cf21`.
- Final code HEAD: `9bc094a13edbaa5cce4ef4c16f000b6097b8e08f`.
- Delivery HEAD is the documentation commit containing this handoff:
  `git log -1 --format=%H -- implementation-notes/agent-plugins-stage-c2.md`.
  Its exact hash is also returned in the delivery response. Code working tree was
  clean before this documentation edit; final status is checked after commit.

Read repository AGENTS, proposal v2, complete C2 prompt/resume, partial review,
stop review/rereview and accepted B/C1 material. No UI/mobile, new table, installer,
OAuth engine, native client dependency or operation framework. Main, stopped
implementation branches and archive untouched; no reset/clean/merge/rebase/push,
deployment or app/provider/fixture/migration execution. Hooks disabled for commits.

## Entrypoint → remaining baseline gap → local adaptation → regression source

Paths are under `crates/gateway/src`; source coverage is **NOT_RUN / NOT_COMPILED**.

| Entrypoint | Gap closed / existing operation reused | Source coverage |
| --- | --- | --- |
| `message/turn_handlers.rs`, API start | Kept one Gateway normalizer, native admission/resolver/catalog/read_skill. Moved prepared snapshot after durable Turn creation: its repository requires that row. Failure blocks the admitted Turn. | Existing B/C1; new ready/binding regression |
| Same file, ordinary Claude/Codex start | Removed blanket plugin rejection; parent envelope goes through internal ThreadManager expansion, native preflight, real copier/receipts, native bindings and ready transition. Root Agent launch expands before native-grant validation; public child grants remain rejected. | `ordinary_owned_cli_start_uses_native_skill_and_ready_parent_for_both_providers`, both plain and exact Agent launches |
| `cli_runtime/skills.rs`; `claude_session.rs` | Authored-name/member-only copy replaced only for owned components by stable aliases and full contained context. Same native installer/copier, receipt locks, hashes/modes and attestation. | Alias, full context/assets/override/receipt and Claude manifest sources below |
| `cli_runtime/manager.rs`, startup/reuse | Ready selection and context hashes participate in existing launch-spec equality. Selection is in existing Starting entry before factory await. Changed revision/context uses existing close/restart. | Startup/revision/replacement source below |
| `turn_handlers.rs` shared prepare/restore; native thread binding/history bridges | Retry/edit/history/resume use canonical parent presentation and pinned revision. Restored frozen bindings must be ready/current, receipts must match exact parent/alias/hash. | Ready/binding regression; owned receipt restore cases; existing history/standalone sources |
| `cli_runtime/handlers.rs`, steer/fork | Parent guard and exact current instance checked before supported native calls. Management-only session remains isolated. Existing provider restrictions retained. | Shared guard/manager sources; existing continuation sources |
| Same file, pending request response | Before durable acceptance/native response, require current ready Turn selection and hold parent guard. Old pending tools cannot authorize a changed revision. | Ready/disable source; existing pending-response sources |
| `turn_handlers.rs::start_cli_runtime_recovery_attempt`; observation-gap reconciliation | Guard before restore/factory/native work; restore existing MCP facade/projection and verified skill receipts; register actual Turn owner before provider work. | Ready/current-authority and manager startup sources; existing recovery sources |
| Detached Composer Tasks and `message/task_handlers.rs::task_create` | Preserve parent in actor contract/presentation; freeze native grants through Gateway expansion before ordinary admission. Client-owned grants cannot be adopted as expansion. | Task pinning/forgery/ceiling source |
| `task_agent_executor.rs`, initial/revision/reviewer/restore | Re-normalize pinned parents against durable native ceiling. API starts take existing parent guard; CLI starts delegate to shared preparation. Internal admission retains parent envelope, executes native leaves. Standalone SkillPack uses original native admission branch. | Task ceiling/ready source; existing Composer SkillPack/native Task sources |
| `agent_action_tools.rs` StartAgent/outbox; `task_tools/mod.rs` immediate inheritance | Inherit trusted source-Turn parent selection, cap leaves to requested/frozen grants, never implicitly widen access. Existing atomic graph/action outbox JSON carries server snapshot for crash repair; no second store/dispatcher. | Task ceiling/current-authority source; existing action/outbox sources |
| `message/plugins.rs::stop_plugin_execution`; `agent_runtime.rs` graph/fallback stop | Capture exact existing CLI owners, await accepted stop helper with common deadline, require inventory empty before existing native MCP shutdown/file mutation. Graph uses actual Turn ownership rather than cancelling replacement by logical key. | New owner/replacement source plus accepted strict stop/cancel/failure sources |

No client-generated expansion is trusted. Bundled children stay existing native
records; public parent presentation/chip, standalone lists and API compact skill
contract remain as accepted in B/C1. Unselected plugin implicit filtering, native
policy/trust/dependencies, approved tools/commands, nono and isolation are reused.

## Exact source → projection → native invocation

For each eligible owned Skill, stable alias is
`pioneer-plugin-<hex(SHA256(parent length + parent ID + native SkillId))[0..48]>`.
Authored names and installed SKILL bytes are unchanged; same authored names in
several parents/standalone do not share aliases. A standalone deliberately using
the opaque alias alongside that owned skill fails collision rather than overwrite.

Ordinary bundled source is the **whole** `parent.package_path`; ownership member
path must equal `skills/<key>` and native definition path must resolve to that
contained member. `skill_source` override selects **whole native installation
context**, without package fallback. Both use the existing native copier and full
folder receipt/hash/modes. Optional receipt fields identify parent and selected
relative member; standalone legacy v1 conversion/v2 semantics remain supported.

- Codex: context copied under configured native home
  `.pioneer-selected-contexts/<alias>`, outside auto-discovered `skills/`.
  Existing `prepend_codex_installed_skill_items` sends genuine native
  `Skill { name: alias, path: context/skills/<key>/SKILL.md }`; native override sends
  `context/SKILL.md`. Existing controlled overlay/attestation keeps plugin/app
  features disabled; no unmanaged native plugin is registered.
- Claude: same materialization becomes `options.selected_skills`; existing
  controlled managed plugin wrapper copies the full verified tree below
  `<managed selected plugin>/context`. Generated manifest selects exactly
  `./context/skills/<key>` (override `./context`). Only that wrapper is passed to
  existing strict `--plugin-dir`; sibling hooks/commands/skills and original
  package manifest remain nested context, not auto-registered root components.
  Standalone retains its old selected-skill wrapper/layout.
- Mixed MCP: Gateway expansion supplies existing native server IDs to the same
  combined preflight, `ResolvedMcpTurnProjection`, committed MCP bindings,
  CLIAgentRuntime MCP launch projection/activation and existing facade/invoker.
  Stdio/HTTP configuration and OAuth remain native operations accepted in A/C1.
  Facade availability, permission decisions and restored frozen projection stay
  authoritative; no tools are emulated from Skill text.

These are source traces, not native-provider runtime evidence.

## Ready, admission, lifecycle and recovery

Prepared metadata is written only after durable Turn creation. CLI materializes
all required files before the existing TurnSkillsResolved handler commits native
skill bindings and authorization, then calls existing `ready_plugin_selection`.
That short writer transition rechecks candidate ownership, parent revision/state
and committed native bindings; projection/ready failure prevents provider start.
No filesystem/hash/process/network/join/notification occurs under DB capacity.
Request/background scoped stores and existing repository transactions are reused.

Sorted existing parent guards cover CLI preparation through factory/native-start
ACK and provider/recovery/continuation enqueue. Snapshot current workspace,
revision, enabled, installed and no-pending state is checked; frozen owned native
bindings absent from ready selection and mismatched receipt ownership fail closed.
Historical chip or receipt alone is not authorization. Stale revision is not
silently refreshed. Tasks retain native grant ceiling even when parent expands
more broadly; an empty allowed expansion still presents one parent.

Concrete integration gap: a Skills-only durable CLI binding has logical session
key, not process identity. Existing actual session owner now retains a bounded
set of admitted Turn IDs (65,536, overload requires close/retry). Graph stop
matches key **and** that set, capturing exact instance, not a replacement.
Terminal DB rows do not release old callbacks' ownership. Registration happens
before provider Turn/thread work; unresolved pending ownership fails closed.
This is metadata in the existing owner, not another registry or durable machinery.

Disable/native edit/update/remove first close parent gate using existing C1
mutation path, drain graph/exact CLI owners (including Starting/Closing inventory)
and native MCP facade/session work, then mutate package/projection. Assets-only
refresh also stops existing parent CLI owners before overwriting shared projection.
Unknown/publication race/timeout/cancel/drain error leaves repairable gate closed;
accepted retained-owner strict stop implementation supplies retry/join evidence.
Old captured owner cannot cancel replacement. Queued Tasks/outbox/recovery recheck
old selection before launch and cannot reopen a changed gate.

Existing managed review/compaction restrictions and provider-specific unsupported
fork/steer remain. Codex controlled overlay and Claude sandbox/permission modes
are unchanged. Pure bounded spool decoding limitation documented by accepted
stop reviews remains; no claim of arbitrary descendant-process completion.
No unresolved C2 implementation blocker identified by source tracing; real
provider behavior, test types and platform effects remain unverified.

## Actual checks

All commands used cwd above. Production checks only:
`CARGO_INCREMENTAL=0 cargo check -p pioneer-gateway --lib > target/<log> 2>&1`.
Intermediate checks below ran against evolving dirty baseline `707c3d5f` and do
not validate later edits; only delivery check covers final production bytes.

| Log under `target/` | Actual exit |
| --- | --- |
| `plugin-c2-resume-context-check.log` | 0, 14m58s |
| `plugin-c2-resume-integration-check.log` | 101, E0658 (`&str.as_str()`); corrected |
| `plugin-c2-resume-provider-check.log` | 0, 12m17s |
| `plugin-c2-resume-final-check.log` | 0, 4m20s |
| `plugin-c2-resume-task-admission-check.log` | 0, 2m10s |
| `plugin-c2-resume-final-source-check.log` | 0, 4m05s; receipt/coverage edits followed, intermediate |
| `plugin-c2-resume-delivery-check.log` | **0**, 2m10s; production bytes exactly final code HEAD `9bc094a1` |

Final `rustfmt --edition 2024 --config skip_children=true --check` on the 15
changed source files other than the existing large `message/tests.rs`: **exit 0**.
That existing harness only adds a four-line module declaration; its full source
and new module were parsed without file writes by
`rustfmt --edition 2024 --config skip_children=false --emit stdout crates/gateway/src/message/tests.rs > /dev/null`: **exit 0**.
`git diff --check` and `git diff --check 707c3d5f HEAD`: **exit 0**.
Production compiler notice: existing unused `set_mcp_policy`. No public/generated
contract changed; schema/bindings/native generation not needed or performed.

New regression sources: `skills.rs::{owned_aliases_distinguish_authored_names_and_standalone_destinations,
owned_context_receipts_cover_package_siblings_native_override_and_assets_only_updates}`
(includes exact-parent receipt recovery rejection),
`claude_session.rs::owned_claude_manifests_select_exact_member_with_full_context_and_no_native_autoload`,
`manager.rs::owned_selection_is_visible_before_factory_await_and_revision_change_restarts`,
and `message/plugin_c2_tests.rs` Task server pinning/forgery/ceiling, prepared/ready
binding/disable/partial-selection rejection, and ordinary/exact-Agent CLI starts
for both providers. Existing standalone/SkillPack, outbox, recovery and accepted
stop sources retained. All own tests **NOT_RUN**, test targets **NOT_COMPILED**;
rustfmt is source parsing, not type checking. Historical external activity stays
**UNKNOWN_EXTERNAL_ACTIVITY** separately. No app, fixture, provider/process,
functional/smoke/device/browser or migration execution. Mobile and E not started.
