# Implementation review handoff

## Baselines and restrictions
- Pioneer base/main and initial HEAD: c0f9f151ef43a3b57a6bec279a347bca9a8526e2; initial checkout clean.
- Mobile base/main and initial HEAD: 456c7c113a110846377d986864bbc91a330d1db8; initial checkout clean.
- Own Pioneer branch: feat/pioneer-html-skill-six-previews; own worktree: /Users/alexander/Code/pioneer/pioneer/.worktrees/pioneer-html-skill-six-previews.
- No tests, builds, check/clippy, application/agent smoke runs, previews, generators or dependency installation permitted. Only cargo fmt permitted. No push/merge/deploy.

## Requirement map (before source edits)
| Requirement | Behavior owner/files | Compatibility risk | Static review / written test plan |
|---|---|---|---|
| Six actual previews, atomic transport | tools/html.rs; gateway/message/html_tools; protocol/artifact.rs; mcp_service.rs; cli_runtime/projector.rs; output_policy.rs | Width-only identity, partial/replayed results, media budgets and exact versions | Trace generation/storage/native/MCP; add order, incomplete-set, exact-binding and legacy tests |
| Legacy retention/recovery | crud html_preview_retention{,_queries}; artifacts/service; gateway/compaction | Existing groups of source+3, bounded queries and ownership | Read all group consumers; write new 7-member and legacy regression coverage |
| Standard preview theme / dynamic hosts | desktop theme JSON/theme.rs; desktop-thread HTML; mobile html-document/html-row/theme | Palette drift, same-mode updates, state loss | Trace token mapping and notifications; write focused host contract tests if changed |
| Canonical HTML skill | resources/skills/pioneer/html; manifest/build/system_skills | Hidden required catalog identity, assets and CLI renaming | Inspect package links and licensing; add bundle/catalog/export coverage |
| Native / CLI automatic loading | agent chat skills; skills prompt/runtime; gateway catalogs/turn handlers/CLI skills | Empty attachments, no task tools, recovery exports and collisions | Trace initial/continued/recovered execution; write independent HTML availability/export cases |
| Short HTML policy | promt content/bundle/compiler/section/instruction_delivery/cli_runtime_context | Unused policy or wrong trust/scope | Trace actual assembly and continuation delivery; update policy tests |

## Review checkpoints
Preview/transport, themes/clients and skill/prompt static checkpoints completed below. Final integration diff review and formatting recorded at the end.

### Retention/recovery integration pass
- New capture provenance uses htmlTool.version=2 (agreed with preview owner); legacy version=1 remains width-only. New groups allow source+6, query limit 8 detects extras; legacy groups still allow <=4. All siblings require the same recognized provenance version and unique valid frame indexes. Existing exact version/binding/adoption checks and serialized short transaction remain.
- Found and fixed consumers outside capture: temporary preview SQL predicate, temporary blob provenance, and provider resolver had v1-only guards. Render/publication stays v1. No DB schema/migration or scheduling changes.
- Existing release lifecycle fixture now covers both persisted formats, including source+6, active/frozen/delivery ownership, rollback, archived idempotent replay and blob-backed provider reads. Added mixed/duplicate/oversized group preservation cases.
- Existing gateway frozen-replay fixture now exercises both legacy and v2 history. Existing OpenAI/Copilot/Ollama/Responses provider contract tests now exercise six images, exact theme-qualified versions and captions. These tests are written only, not executed.

### Preview/transport checkpoint
- Reviewed actual capture, replay, native projection, MCP facade and output-policy diff. Browser captures every (appearance,width) in a fresh context, budget/cancellation ownership remains bounded; input source+frames have exact persisted versions and frame identities include both dimensions.
- Root v2 result is formatVersion=2; artifact provenance v2. Only recognized historical unversioned width-only triples use legacy projection. Incomplete historical capture is not regenerated.
- PNG aggregate stays 3 MiB. Only HTML structured/retained JSON ceiling increases 96 -> 192 KiB for six bounded diagnostic sets. Global transport/provider ceilings unchanged.
- Static review caught legacy caption drift; original legacy caption bytes must be preserved for frozen replay. Added distinct MCP projection regression request to supplement native/provider coverage. No tests executed.

### Themes/clients checkpoint
- Shared canonical resource resources/themes/pioneer.json is moved byte-for-byte from desktop. Gateway pure JSON projection and desktop embed/watch paths consume it, without Gateway->GPUI dependency or client theme engine.
- Roles: background/background, foreground/foreground, muted/muted.foreground, border/border, accent/primary.background, accent-foreground/primary.foreground. Desktop projects current Theme fields; mobile uses current theme.colors.accent/accentForeground and explicit appearance metadata independent of theme name.
- Existing desktop screen Theme observation -> reconciliation -> complete palette inequality -> pioneer-theme evaluate_script remains. Existing mobile source memo retains initial source while palette effect injects pioneer-theme on current palette updates. No ordinary palette-change reload added. Mobile settings/theme.ts needs no change.
- Narrowed a mock VM test claim: generated handler token writes are testable; fake unchanged input is not evidence of real DOM-state preservation. Lifecycle state preservation is static evidence pending permitted runtime validation.

### Skill/prompt checkpoint
- Canonical system:pioneer/html package has required implicit invocation, hidden catalog, allowed model invocation and stable 21-char SkillId HtmL6PrEvIeWsPiOnEeR1. Existing generic build/materialization, skill prompt internal references/read_skill and skill_asset_root paths are reused.
- CLI exporter extends existing owned/hashed/receipted reserved exports with pioneer-system-html. Required HTML selection is independent of task-tools and empty explicit attachments; selected skill restoration uses existing persisted manifests/receipts. Six references and MIT license are exported with body.
- Separate include_html_policy enters real prompt assembly and all native continuation/recovery compilation sites; CLI capability context derives HTML operations from frozen/current MCP projection. Pioneer trust and existing native-turn/CLI-thread scope preserved; html_policy is a reserved builtin manifest id.
- Review corrected CLI canonical-operation vs qualified-callable wording; current runtime catalog remains authority. No opaque IDs synthesized. No tasks/subagents text or behavior changed.

### Integration corrections before final formatting
- Added actual MCP assembly regression with real ArtifactService and six distinct PNGs. The production assembly block is a private helper called by the real facade; activity lifetime remains with execute_html_tool. Test verifies exact bytes/versions, order, labels, diagnostics and atomic error after five successful reads when sixth exact-version lookup fails.
- Preserved exact legacy caption bytes in native projection, MCP replay and native media preflight. Strengthened existing legacy caption regression to exact equality; cosmetic provider prefix edits were dropped. This avoids an accidental historical wire-hash change.
- Extended existing Claude plugin namespace and selected-skill lifecycle fixtures to include ordinary html and reserved pioneer-system-html, restore/reuse, composition changes and path/hash restart.
- Corrected new prompt test channel expectation: SelectedCapabilities belongs in provider instructions, not user turn context. Production trust/scope rules already matched the established policy.


## Repository state for reviewer
- Pioneer worktree: /Users/alexander/Code/pioneer/pioneer/.worktrees/pioneer-html-skill-six-previews; branch feat/pioneer-html-skill-six-previews; review the final branch HEAD against base c0f9f151ef43a3b57a6bec279a347bca9a8526e2.
- Mobile worktree: /Users/alexander/Code/pioneer/pioneer/.worktrees/pioneer-html-skill-six-previews-app; branch feat/pioneer-html-skill-six-previews; commit c9f0dd79552df3449ee76dcb1bd0a6bca84118d1 against base 456c7c113a110846377d986864bbc91a330d1db8. Mobile status is clean.
- Local commits use `git -c core.hooksPath=/dev/null -c commit.gpgsign=false commit`; no hooks, merge, push or deployment.

## Limits and review gate
Only static source/diff inspection and cargo fmt have been performed. Compilation, Rust/JS tests, check/clippy, TypeScript/lint, app/Gateway/CLI/browser smoke runs, HTML preview/render, schema/skill generators and dependency installation have NOT been run. No claim of successful compilation, passing tests, real browser output or verified live webview behavior is made.

Schema sources were reconciled statically. Tool input schema is owned directly in tools/html.rs; new result format is explicit JSON, with no new wire DTO. Prompt capability/compile inputs are internal. No generated schema files needed modification; generators were not run.

After full external review and explicit acceptance, a separately authorized validation pass should run focused Rust nextest suites for tools, gateway (MCP/browser/system_skills/CLI skills/recovery/compaction), artifacts retention, provider adapters and promt; the existing mobile html-document Jest suite and type checks; then actual browser six-frame capture and desktop/mobile live-theme smoke tests (both modes and a same-mode palette change). Follow repository/platform feature flags and AGENTS.md at that time. Keep the execution prohibition until external acceptance. Standard preview palettes do not validate arbitrary future custom palettes.

The published document uses each host's actual loaded palette. Existing mobile colors differ from desktop's standard JSON (e.g. its primary accent); this implementation preserves those host values and aligns semantic roles, without recoloring the application or transmitting client palettes/mode to Gateway.

## Formatting performed
Working directory for all commands: own Pioneer worktree.
- `cargo fmt -p pioneer-agent -p pioneer-artifacts -p pioneer-crud -p pioneer-desktop -p pioneer-desktop-thread -p pioneer-promt -p pioneer-provider` — exit 0, no output.
- `cargo fmt -p pioneer-artifacts -- --config skip_children=true crates/artifacts/src/html_preview_retention_tests.rs` — exit 0, no output; explicit include! file (cargo fmt module traversal does not discover it).
- `cargo fmt -p pioneer-gateway -p pioneer-protocol -p pioneer-tools` — exit 0, no output.
- `cargo fmt -p pioneer-agent -p pioneer-artifacts -p pioneer-crud -p pioneer-desktop -p pioneer-desktop-thread -p pioneer-gateway -p pioneer-promt -p pioneer-protocol -p pioneer-provider -p pioneer-tools --check -- crates/artifacts/src/html_preview_retention_tests.rs` — exit 0, no output. This is formatting/parse inspection only, not compilation.

## Final static audit
Reviewed the actual changes against both base SHAs, including all new skill/reference/license/theme/helper files and the mobile commit. Searches for width-only identity, three-image assumptions and four-artifact group bounds leave only explicit legacy handling/tests; the per-artifact releasedCopies <=3 is an unrelated existing copy-path bound and stays intact. No unrelated formatting changes, dependencies, crates, services, migrations, client->Gateway theme payloads, user-theme infrastructure, design-artifact or public publishing integrations were introduced. No new TODO or unfinished branch remains.

Existing protocol/artifact.rs and CLI projector recursively compact HTML metadata/media and preserve arbitrary preview arrays; no fixed count required changes. Existing compaction archive transformation retains all diagnostic JSON while removing media and was kept. Existing build.rs includes all package files and manifest hashes without a special HTML build path. Existing skills prompt/runtime, agent skill resolver and mobile settings/theme selection already provide the needed generic contracts and were not refactored.

Written coverage (not executed): six actual capture PNGs and per-frame console/theme/geometry; complete native media projection and rejection matrix; real-artifact MCP projection with exact byte/version failures and stable legacy captions; legacy/v2 retention with rollback, frozen ownership and replay; six-image OpenAI/Copilot/Ollama/Responses transports; required HTML skill resolution and stable bundle identity; three-runtime export/reference bytes/ordinary-name collision/receipt restoration; Claude namespace/session reuse and restart; HTML-only prompt delivery without tasks/attachments; standard theme projection; mobile embedded JavaScript token writes on mode and same-mode palette changes.

## Changed files
Paths below are relative to the respective repository. The theme JSON is a move with unchanged contents.

Pioneer:
crates/agent/src/chat/mod.rs
crates/artifacts/src/html_preview_retention_tests.rs
crates/artifacts/src/provider_resolver.rs
crates/artifacts/src/service.rs
crates/crud/src/repositories/html_preview_retention.rs
crates/crud/src/repositories/html_preview_retention_queries.rs
crates/desktop-thread/src/timeline/html.rs
crates/desktop/src/theme.rs
crates/desktop/themes/pioneer.json
crates/gateway/src/cli_runtime/claude_session.rs
crates/gateway/src/cli_runtime/context.rs
crates/gateway/src/cli_runtime/manager.rs
crates/gateway/src/cli_runtime/skills.rs
crates/gateway/src/compaction/html_preview_retention_tests.rs
crates/gateway/src/mcp_service.rs
crates/gateway/src/message/html_tools/browser.rs
crates/gateway/src/message/html_tools/mod.rs
crates/gateway/src/message/skills/catalog.rs
crates/gateway/src/message/turn_handlers.rs
crates/gateway/src/system_skills.rs
crates/promt/src/bundle.rs
crates/promt/src/cli_runtime_context.rs
crates/promt/src/compile/compiler.rs
crates/promt/src/content.rs
crates/promt/src/instruction_delivery.rs
crates/promt/src/section.rs
crates/promt/tests/compile_snapshots.rs
crates/protocol/src/turn.rs
crates/provider/src/attachments/regression.rs
crates/provider/src/providers/copilot.rs
crates/provider/src/providers/ollama.rs
crates/provider/src/providers/openai.rs
crates/provider/src/providers/responses_api_compatible/tests.rs
crates/tools/src/html.rs
crates/tools/src/output_policy.rs
resources/skills/bundled-system-skills.toml
crates/gateway/src/message/html_tools/theme.rs
resources/themes/pioneer.json
resources/skills/pioneer/html/SKILL.md
resources/skills/pioneer/html/LICENSE
resources/skills/pioneer/html/references/theme-and-layout.md
resources/skills/pioneer/html/references/tools-and-runtime.md
resources/skills/pioneer/html/references/documents-and-plans.md
resources/skills/pioneer/html/references/diagrams.md
resources/skills/pioneer/html/references/charts-and-data.md
resources/skills/pioneer/html/references/interactive-content.md
IMPLEMENTATION_REVIEW.md

Mobile:
src/components/thread/timeline/rows/html-document.ts
src/components/thread/timeline/rows/html-document.test.ts
src/components/thread/timeline/rows/html-row.tsx
unistyles.ts


## Attached work disposition
Used the invoked pioneer-system-subagents skill to split implementation work. Skill/prompt and theme/client candidates were read, reviewed against actual diffs, revised and explicitly accepted. Parent directly integrated/reviewed all preview/MCP and legacy-caption changes from source; after final formatting and static audit its remaining report-only continuation was no longer needed and was explicitly cancelled (not diagnosed as stalled). No active or unreviewed child work is required for this handoff. This is implementation self-review, not the requested independent full external acceptance; the execution gate remains closed.

## Final Git observations
- Pioneer implementation commit: ab0d725789db95ff88896ed2e97d79beb87ff125 (`Add inline HTML system skill and six themed previews`); own worktree clean immediately after commit. A following handoff-only commit records these observations.
- Mobile commit: c9f0dd79552df3449ee76dcb1bd0a6bca84118d1 (`Align inline HTML with the active mobile theme`); own worktree clean.
- Final read found the main Pioneer checkout clean at 12fb677bc996957b64471a4b88acb08e1efc3eb4, different from the captured initial c0f9f151ef43a3b57a6bec279a347bca9a8526e2. That checkout/ref advanced outside this implementation's Git operations. Do not substitute the newer main as the review base; no rebase/merge was performed here. Mobile main remains clean at its initial SHA.


## External static review corrections (incremental review)

### Scope and starting state
- Pioneer checked implementation HEAD before edits: `5af0110bc39e910861b940358b94227cd5fb005c`; branch `feat/pioneer-html-skill-six-previews`, clean worktree. Original base remains `c0f9f151ef43a3b57a6bec279a347bca9a8526e2`.
- Mobile checked HEAD: `c9f0dd79552df3449ee76dcb1bd0a6bca84118d1`; same branch name, clean worktree. Original base remains `456c7c113a110846377d986864bbc91a330d1db8`. No incremental mobile edits.
- Only the three externally reported findings and their necessary regression coverage are changed. The execution gate remains closed pending repeated external acceptance of the entire implementation.

| Finding | Cause | Correction | Regression authored/updated, NOT run |
|---|---|---|---|
| P1 frozen HTML catalog recovery | Old `appearance` schema fingerprint fails native identity lookup before HTML replay; returning today's schema/description would also change canonical naming and the manifest. | Preserve the exact entire previous preview definition from the original base in a recovery-only function. Match only its canonical fingerprint for an existing first-party binding; return its original schema and description. Recognize that exact schema+description in `exact_definition`, preserving unprefixed `html_preview`. | `frozen_legacy_html_session_recovers_and_invokes_retained_three_frame_preview`: real durable projection/bindings, real Codex recovery, exact projection/preflight/facade/restart identity equality and unchanged persisted records; subsequent CLI-origin invoker validation followed by three real persisted PNG/version reads through the existing execution seam and production MCP media assembler. Asserts rejection of altered schema on the real recovery path and loss of first-party recognition for an altered description. |
| P2 integration skill harness | Explicit fixture system roots bypass standard materialization but contain only tasks/subagents. HTML admission consequently fails before the intended successful start or negative guard. | Copy canonical HTML alongside the other two packages. Expect two explicit+three system bindings, exclude all three reserved exports from explicit input ordering, include HTML in zero-attachment/composer/bootstrap assertions. Fix additional lifecycle receipt count (one explicit+three system). | Existing Codex/Claude/OpenCode lifecycle/preflight, ordering, composer-pack and empty-context cases updated. Added `cli_html_skill_resolves_without_task_tools_or_explicit_attachments` using the same harness and actual resolver, with an empty attachment list and task orchestration disabled. |
| P2 unavailable image input | Policy/skill required preview unconditionally despite existing vision guards leaving render available. | Shared native/CLI policy, canonical Workflow and runtime reference now require six-image/diagnostic inspection when supported, forbid false visual-verification claims or unchanged unsupported retries, and allow permitted render with an explicit limitation or text when reliability needs visual inspection. Both-theme HTML support remains mandatory. | Existing policy delivery coverage still exercises the shared policy through actual compiler/delivery assembly. This is an instruction-only change; vision guards and tool descriptions remain unchanged. |

### Incremental self-review checkpoints
- P1: reread the production diff and the actual recovery -> runtime view -> `current_identity` -> `validate_frozen_identity` path, projection `base_callable_name`/manifest reconstruction, and invoker's second identity check. Neither frozen validation, manifest/callable/provider-fingerprint equality nor saved records are bypassed or rewritten. New advertised catalogs still use only current `definitions()`; they do not advertise `appearance`. `html_render` retains its unchanged contract. The legacy fixture is the complete original-base definition, not today's schema with a cosmetic edit.
- The P1 regression uses the existing validated execution seam for retained output and the production artifact/MCP assembler. It targets the recovery/invocation identity boundary and exact legacy media projection; it does not exercise a browser or the full `execute_html` capture handler. Existing legacy history/replay and v2 atomicity coverage remains in place, unexecuted. Self-review aligned the fixture facade limits with the actual transport-bounded recovery limits and preserved the nested ToolDrift diagnostic when asserting an unknown schema rejection. A final pass caught a negative-control weakness: the unknown schema also changes projection naming. The regression additionally checks the unknown fingerprint with the original raw callable, so a name mismatch cannot mask a permissive fingerprint fallback.
- P2 harness: traced every harness user, including unsupported required system skill, Claude model-invocation restriction, destination collision/copy failure, and missing MCP/skill references. They retain their original public-error, side-effect and lifecycle guards; the complete canonical fixture removes the unrelated missing-HTML failure. Found the additional stale receipt-count assertion and fixed it. Production admission/export/receipts/collision logic is unchanged.
- P2 capability fallback: reread both expanded policy loading branches, skill Workflow and runtime reference against `html_preview_vision` and the existing render description. Native and CLI share the same conditional rule and keep their distinct skill-loading instructions. No vision guard, partial-success path, capability infrastructure or publication permission was changed.

### Final incremental static audit
- Reviewed the entire incremental source diff against `5af0110bc39e910861b940358b94227cd5fb005c` and reconciled the cumulative file inventory/diff against the original Pioneer base. Revisited the existing six-frame capture/output policies, standard JSON projection and both hosts' cumulative theme diffs. New-call order remains light 360/768/1024 then dark 360/768/1024; budgets, cancellation, exact versions and legacy-only triples remain unchanged by this revision.
- Mobile has no incremental changes against the checked HEAD; its cumulative diff remains the previously reviewed four files. The shared standard theme remains `resources/themes/pioneer.json`; runtime hosts still apply their current loaded palettes and `pioneer-theme`, including same-mode changes. No client-to-Gateway theme payload is added.
- No additional dependencies, migrations, services, broad MCP/skills refactors, TODOs or unrelated formatting edits. No installed runtime skills or main checkouts were edited. No tests, builds, typechecks, lints, application/Gateway/CLI/browser checks, HTML preview/render or generators were executed. Static inspection and formatting cannot establish compilation or runtime correctness.
- Incremental changed files: `crates/tools/src/html.rs`, `crates/gateway/src/turn_mcp/native_tools.rs`, `crates/gateway/src/mcp_service.rs`, `crates/gateway/src/message/tests.rs`, `crates/promt/src/content.rs`, `resources/skills/pioneer/html/SKILL.md`, `resources/skills/pioneer/html/references/tools-and-runtime.md`, and this handoff. The earlier cumulative inventory additionally includes the newly changed `native_tools.rs` and `message/tests.rs`.

### Formatting and next review gate
- `cargo fmt -p pioneer-gateway -p pioneer-promt -p pioneer-tools` — exit 0, no output (first formatting pass in this revision).
- `cargo fmt -p pioneer-gateway -p pioneer-promt -p pioneer-tools` — final three-package formatting pass, no output.
- `cargo fmt -p pioneer-gateway -p pioneer-promt -p pioneer-tools --check` — exit 0, no output.
- `cargo fmt -p pioneer-gateway` — exit 0, no output, after test-only negative-control strengthening.
- `cargo fmt -p pioneer-gateway -p pioneer-promt -p pioneer-tools --check` — repeated after that change; exit 0, no output. All these results concern formatting only.
- After repeated full external review and explicit acceptance only: run the new focused recovery/invocation regression, HTML-only resolver case and updated integration harness matrix, followed by the previously listed validation plan. No automatic test execution follows this handoff.

The incremental changes are committed locally with hooks disabled; resolve the final revision with `git rev-parse HEAD` in the Pioneer worktree. Mobile remains at its checked HEAD. No new execution permissions are implied by this handoff.


## Embedded JavaScript wording follow-up

- User requested clearer emphasis on JavaScript after accepting the implementation and publishing PR #177. The canonical skill description/entrypoint and shared native/CLI policy now explicitly describe CSS and embedded JavaScript for working in-document controls. Interactive/runtime references name concrete behaviors such as recalculation, table filtering, tabs, and chart updates.
- This is guidance for an existing capability, not a runtime change. The documents still require both themes, and controls do not gain agent tools, chat-send permissions, durable state, or automatic interaction verification through screenshots.
- Tool descriptions already request finished HTML/CSS/JavaScript. Their frozen catalog bytes are deliberately unchanged: this wording clarification does not require another tool-contract compatibility path. Skill name, ID, exports, and loading paths remain unchanged.
- Static diff review only; no tests, builds, scripts, generators, applications or preview/render calls. The existing CI monitor is paused during editing and resumes for the new PR head with a verified next check at +15 minutes.

- Formatting: `cargo fmt -p pioneer-promt` and `cargo fmt -p pioneer-promt --check` completed without output (exit 0). No executable skill validator or project checks were run.
