# Capsule-scoped delegation: external static review handoff

READY_FOR_EXTERNAL_STATIC_REVIEW

## Candidate and isolation

- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/capsule-scoped-delegation`.
- Branch: `fix/capsule-scoped-delegation`.
- BASE_SHA: `f0cd14fab56ad507ebe0e5a51b72d4efece68598` (fetched origin/main).
- Externally reviewed previous candidate: `6bfcddc89ef05ab189cc49e8957b9a892d0f24b4`.
- Corrected implementation candidate HEAD: `50d0962ef9eb704e4985e7ae4ca9dbb8278b9a32`.
- Delivery candidate HEAD: the handoff-only commit immediately following that implementation commit on this branch; resolve with `git rev-parse fix/capsule-scoped-delegation`. Its literal SHA is supplied in the final delivery message (a commit cannot embed its own SHA).
- Initial main HEAD matched BASE_SHA and main status was clean. Requested branch/path were free. Main and all neighboring worktrees were left untouched. Fetch, worktree creation and commits used disabled hooks. No push, PR, merge, deployment or production access.

## Baseline, cause, minimal solution

Baseline source inspection confirmed that delegated-root creation persisted an execution_bound edge. Discovery matched the creator execution or identity-bound policy; adapter, service and commit also required the creator identity. Thus later turns/occurrences could not recover the explicit destination.

Add one existing-route variant, `capsule_bound`. Only the delegated-root creation default changes. Explicit route management can serialize the new variant under its existing authorization. Root admission grants remain execution_bound; return-route requirements remain identity_bound. No existing route is promoted. Creator execution and identity remain persisted provenance; actual executor remains the action/receipt/outbox actor. New delegated-root routes have no arbitrary TTL; explicitly supplied expiry still applies.

Pre-edit boundary map: protocol/DDL -> atomic thread and route creation -> persisted source discovery -> runtime binding/catalog -> opaque target -> canonical prepare -> transactional commit -> task recovery. Planned evidence: lifecycle discovery fixture, independent binding recovery fixture, composite authorization controls, transactional race controls, schema install/upgrade/down fixtures. All evidence below is authored or static, not executed.

## Authorization map and retained invariants

| Boundary | Implementation and retained guard |
| --- | --- |
| Protocol | AgentRouteKind plus its four checked-in JSON schema occurrences; generation of schemas deferred |
| Creation | agent_action_tools::commit_create_thread emits capsule_bound only for RootDelegation; existing commit_agent_thread_creation persists thread and route in the action transaction and preserves replay/idempotency |
| CRUD insertion | Both string-to-enum conversions recognize the kind; unexpired server-owned indefinite route exception includes CapsuleBound, still requiring exact AgentExecution authority actor; creator workspace/identity/capsule/existence are validated |
| Discovery | list_agent_delegation_routes_for_source loads the executor by key, verifies identity, and selects capsule routes by persisted home_root_thread_id AND workspace; model supplies neither scope nor lineage |
| Catalog | refresh_start_options_catalog checks active, expiry, current policy generations, workspace, Gateway and source capsule before binding; source identity equality is skipped only for CapsuleBound |
| Binding | bind_persisted_route requires source capsule equality for CapsuleBound; old execution/identity fences stay intact |
| Prepare | CanonicalAgentActionService still checks executor envelope, current policy, source actions and child-start permission before authorize_route; source capsule, action subset, destination, disclosure, destination identity/profile and hop rules are unchanged |
| Opaque target | Existing options remain execution/fingerprint bound; resolver only maps currently bound exact routes. Presence of an option grants no action. Receipt generations are checked in the write transaction |
| Commit | revalidate_agent_execution_for_commit retains actor, execution/attempt/current source identity and grant checks. revalidate_agent_route_for_commit skips only creator identity/execution equality for CapsuleBound, retaining persisted source home/workspace, current policy and route generations, status/expiry, action, disclosure and exact outbox destination |
| Recovery | resolve_parent_context validates CapsuleBound against immutable durable admission root; restore_task_agent_action_binding reconstructs envelope from persisted identity/profile/action ceiling, not creator liveness |
| Revocation | Existing route CAS updates status/generation; catalog reload and transactional route reread affect every descendant without copied routes. Management retains source/destination permission checks |
| Review | No task reviewer designation, acceptance or review protocol changes |

## Shared root scope and depth

Internal thread creation uses facts.home_root_thread_id and validates the parent's persisted lineage. Depth changes the internal lineage/resource admission, not inter-capsule hop count. Agent children retain their admitted home root; persisted task recovery verifies execution.home_root_thread_id against its admitted effective home root. `lineage_from_task_run_turn` uses parent.home_root_thread_id, so nested occurrences do not become new security roots merely because their execution thread changes.

Task model execution and result-delivery recovery enter `restore_task_agent_action_binding`; the existing persisted identity/profile/allowed_actions/grant fingerprints reconstruct the restricted envelope. `resolve_parent_context` is shared by task execution paths, and the durable admission belongs to the task rather than an occurrence. Delayed, one-shot, interval, cron, retry and subsequent occurrences therefore use the same root scope. This is a static call-path argument; the recovery fixture exercises their common binding boundary, not a running scheduler or each trigger parser.

An explicitly routed run in B uses B as home_root_thread_id while its durable source admission root remains A. A's route is consequently not selected inside B. No reverse route, intermediate hop authorization or access to internal B threads is synthesized. The exact destination remains pinned throughout. Existing limits on supported depth/resources still apply.

## Schema, costs, downgrade

New registered migration: m20261104_000001_capsule_bound_routes. Historical migrations and generated Entity are unchanged. The model columns/types do not change: only CHECK admits a new string and an index is added.

SQLite cannot alter CHECK via the installed builder. The migration reads installed table DDL, replaces the one known predicate, stages rows, drops/recreates the original table under its original name, copies rows back, and reinstalls saved explicit indexes/triggers in one pinned SQLx transaction obtained from SeaORM’s existing SQLite pool. It does not rename the original table or rewrite incoming references. PK/UNIQUE/FK/CHECK definitions stay in the installed DDL. Foreign-key enforcement is read, disabled when needed, checked, and restored on the same physical pool lease. A migration-local Drop guard is armed immediately upon physical acquisition for both FK=ON and FK=OFF, before even the active-transaction entry check. After success or a settled error, restoration and absence of an active transaction are verified before that lease may return to the pool. Cancellation, failed restoration, or any path bypassing verification causes SQLx close_on_drop on that exact connection. Runtime FK=OFF entry never changes the setting, but remains guarded until the same final FK and transaction-depth checks pass; queued rollback on Drop alone is insufficient. Ambient transactions and non-SQLx/non-SQLite executors are rejected before setting changes or DDL. Ordinary standalone SQLite Migrator calls support both FK=ON and FK=OFF, including multi-connection pools; no pool-wide setting change is made.

Schema commit/marker crash retry recognizes the installed target CHECK. Down refuses ANY capsule_bound row, including revoked/expired rows; no deletion or silent downgrade is performed. After authorized removal outside this change, down restores the old CHECK and removes only the new index. Down/reapply is covered in the fixture.

Index idx_agent_route_capsule_active(source_capsule_id, route_kind, status, expires_at) serves the new bounded discovery arm; existing execution/identity indexes remain. Upgrade scans/copies the route table twice, rebuilds its existing indexes and adds this index (O(N) copying plus index sorting); requires temporary table/index/journal disk space and a schema writer lock. No background backfill, per-occurrence route copies or production repair. External review should assess expected route-table size and upgrade maintenance window.

## Authored regressions (NOT_RUN)

- thread_launch_choices::delegated_root_route_survives_creator_completion_and_persisted_descendants: real thread_create persists capsule_bound without TTL, then discovery after creator completion, later execution and persisted descendant chain through eight levels; same-identity foreign capsule denied. Descendant rows exercise storage discovery, not full execution admission.
- agent_runtime_adapter::persisted_occurrence_bindings_recover_capsule_route_without_live_creator: discard creator adapter, reconstruct successive attempt/depth bindings through the common persisted function, bind and resolve exact destination, reject foreign scope and obsolete option after route removal.
- agent_route_service::capsule_route_rebinds_across_execution_and_identity_without_widening_authority: different identity/execution admitted in A, old kinds still denied, B cannot use A's edge; exact destination, restricted source export, disclosure, policy generation, revocation and expiry controls.
- agent_domain_launch_continuity_tests::capsule_route_commit_revalidates_saved_receipt_against_current_route: actual transactional route revalidation accepts a different creator identity/execution in A, then refuses unchanged saved receipt after status/expiry/generation/policy/action/disclosure/scope/destination changes; old kinds retain identity/execution refusal. Existing graph fixture provides executor facts; route state is seeded at the storage boundary. This tests the route-specific commit guard, not the whole ledger commit.
- capsule_bound_route_schema integration suite: fresh install, unknown kind rejection, legacy execution-bound row and route-event retention, custom index and functional trigger preservation, new lookup index, rejected lossy down with retained data, successful down and reapply.
- task_start_failure::capsule_delegation_survives_nested_task_binding_recovery_and_revocation: existing recording CLI fixture admits a creator Task; actual thread_create saves the route, actual task_create admits its nested Task. Source execution is finalized through CrudStore and its completed fence is read back. Nested admission/lineage is loaded, with depth greater than one. Old registered binding/tool state is removed; two subsequent launches reconstruct the same persisted task execution through materialize_task_agent_action_binding_for_execution, then prepare_agent_action_binding loads the production catalog. No manual route attachment is used. threads_start_options issues the opaque target; thread_message_send commits through the existing handler, with receipt source/destination/actual actor and outbox owner checked. Revoke via existing CAS API makes the handler reload the catalog, reject the old target with the stable authorization error, and persist no receipt/outbox. Only the model/process boundary is replaced by the existing RecordingCliRuntimeSession.
- Additional schema fixtures: fk_on_error_after_setting_change_restores_physical_connection; commit_failure_with_unsettled_transaction_discards_connection (ON/OFF matrix); preexisting_unsettled_transaction_is_rejected_and_discarded; fk_on_cancellation_during_rebuild_discards_physical_connection; fk_settings_survive_success_and_ambient_transaction_is_rejected_without_changes. TEMP markers identify physical reuse; FK PRAGMA is observed on a pinned lease, and legacy rows/absence of staging table prove rollback. An index-name conflict fails after FK changes and row rebuilding, exercising successful rollback/restoration on the same physical lease. A shared SQLx commit-hook fixture rejects commit in both original FK modes and exposes unsettled transaction tracking, which must discard that lease. Shared assertions verify TEMP-marker loss, unchanged route kind/fingerprint, absent staging table and FK=ON on the replacement (the pool default, even after an OFF source lease). Both matrix cases then successfully reapply the migration and recheck the original row. A separate early-entry fixture creates the unsettled transaction before migration acquisition and verifies rejection, physical replacement, row preservation and subsequent migration success. An update hook parks the staging-row copy before cancellation, so cancellation ordering is not inferred from elapsed time.

Existing fixtures for launch ceilings, independent-root dispatch/replay, cold task recovery and designated reviewer checks remain unchanged. The separate adapter unit test remains useful for exact binding/option contracts and does not serve as evidence of full Task recovery. No assertions were removed or tests disabled. The new integration regression covers two cold reconstructions of one already-admitted task execution, not two scheduler-generated occurrences or a process-wide Gateway restart. The creator AgentExecution is completed with the normal finalizer; its Task and recording session need not finish to prove independence from that execution. No live delayed/cron scheduler, external process/model, or production reproduction is claimed. All newly authored behavioral evidence is NOT_RUN; compilation/type correctness is not established by formatting.

## External-review corrections

| Finding | Correction | Authored regression / static evidence |
| --- | --- | --- |
| FK PRAGMA and transaction could use different physical pool connections; early error/cancellation could skip restore | Local MigrationLease owns one SQLx PoolConnection. It is armed before FK mutation and disarmed only after original setting and transaction completion are verified. Unknown completion or cancellation closes the lease, while a settled failure can restore and safely reuse it. No global pool reconfiguration or generic connection infrastructure | Schema tests observe actual FK plus a TEMP physical-connection marker on success/error/cancellation; normal OFF down/reapply and fresh ON install retained; ambient transaction rejected before changes |
| P2: FK=OFF and preexisting active transactions bypassed the discard guard | Initialize discard=true at lease construction and remove the FK-dependent assignment. Only the existing final FK/depth verification can disarm it. Correct misleading FK=OFF rollback comment | Extend the commit-failure test as an ON/OFF matrix using the shared fixture/assertions; add an early-entry unsettled-transaction regression; both verify physical replacement, original row/fingerprint and successful next migration. Tests NOT_RUN |
| Documented unit variant changes schemars enum shape | Change CapsuleBound documentation to an ordinary // comment. No JSON schema edits were needed in this correction | Inspected all four AgentRouteKind definitions: top-level agent_route_kind.json and nested definitions in create_params/list_response/projection remain string enums with exactly execution_bound, identity_bound, capsule_bound. Authoritative regeneration/comparison: NOT_RUN |
| Adapter test skipped real Task recovery/catalog/commit | Add one nested Task integration regression using existing recording CLI fixture; retain useful narrower tests | Actual admission and Task lineage, completed creator, repeated persisted task-binding restoration, production catalog loading, opaque target, canonical message commit, then revoked-target rejection. No production test-only API added; only an existing test invocation helper becomes pub(super) within test modules |

API/source audit used installed sea-orm/sea-orm-migration 2.0.4 and re-exported SQLx 0.9.0: DatabaseExecutor variants, get_sqlite_connection_pool/acquire, Connection::begin/is_in_transaction, Transaction Drop rollback, and PoolConnection::close_on_drop were read directly. Schema reasoning also accounts for the reviewer's schemars_derive 1.2.3 variant-doc behavior. No dependency or lockfile changes were required.

Self-review re-read the complete accumulated diff from BASE_SHA, the migration and all added tests, task admission and lineage callers, runtime binding publication, handler catalog refresh/error sanitization, route CAS revoke, and canonical route commit checks. The correction does not change capsule discovery/authorization semantics, old route kinds, grants, reviewer designation, destination/disclosure/generation fences, or root ownership. No route promotion, reverse/transitive authority, production repair, or child_turn_missing change was introduced. No generated Entity was edited.

## P2 correction audit (static only)

The reviewed HEAD was `6bfcddc89ef05ab189cc49e8957b9a892d0f24b4`. The implementation change is limited to MigrationLease initialization and its comments; schema DDL, authorization, checked-in JSON and the nested Task recovery regression are unchanged.

- Before physical acquisition: unsupported executor/ambient transaction is rejected without changing settings; acquisition failure is handled by SQLx.
- Immediately after acquisition: discard is true. Preexisting tracked transaction, initial FK-read error, or cancellation cannot return this lease as verified clean.
- ON and OFF: FK checks, begin, rebuild, commit, rollback, restoration, and their await/cancellation boundaries retain the armed guard. There is no mode-dependent guard assignment. The owned SQLx transaction queues rollback on Drop; the armed lease closes the physical connection whenever verification was bypassed or failed, including paths that never acquired a transaction.
- Settled failure: successful rollback plus original-FK equality and zero tracked depth permit safe reuse and preserve the original operation error. This remains covered by the settled-error reuse fixture.
- Success: disarming occurs only after both checks pass. There is no await between disarming and returning.

The ON/OFF matrix explicitly observes the original mode before injection and expects FK=ON after physical replacement because that is the fixture pool's connection default. The early-entry fixture flushes queued rollback and asserts nonzero tracked depth before handing the connection back for migration acquisition. These are authored behavioral assertions, not executed observations. Cancellation fixture coverage remains ON-only; OFF cancellation safety was audited statically through the unconditional guard, not claimed as a newly executed scenario.

Re-read the full accumulated BASE_SHA diff, including earlier authorization, migration and Task recovery tests, and reviewed the installed SQLx transaction Drop/worker commit/rollback behavior. No additional authorization, schema or Task lifecycle changes were made. Formatting and whitespace checks pass; all prohibited checks remain NOT_RUN.

## Static self-review and deferred validation

Read all changed production functions with their callers, all route-kind matches/string conversions, root admission and return-route exceptions, catalog options, canonical authorize/prepare, persisted recovery and transactional scope checks. Reviewed the full diff from BASE_SHA, including tests and migration. Retained retry after schema commit and the corrected opaque-option test (generation enforcement belongs to receipt commit). External review found that the initial FK restoration was not physically pinned; this correction replaces that handling. No broad inheritance system or unrelated child_turn_missing fix was added.

Read root, CRUD, migration and Entity instructions. Entity generation from temporary normal-Migrator DB is explicitly deferred by the user's execution prohibition. No Entity files were hand-edited. Schema JSON was updated minimally by inspection; authoritative schema generation/comparison is also deferred.

Known limitations: no upgrade of old production execution_bound/identity_bound delegations; diagnostic production IDs are not embedded in implementation. Old production delegation repair requires separate authorization. All behavioral/compile/migration claims require later validation. The migration must be invoked through a standalone SQLx SQLite pool (normal runtime bootstrap uses its serialized writer); enclosing transactions are intentionally refused. Cancellation after lease acquisition in either FK mode discards the unverified connection. Cancellation fixtures use file databases, so replacement does not destroy the sole lifetime owner of an in-memory database. No SQL, EXPLAIN, migration or runtime experiment was performed.

LOCAL_TESTS: NOT_RUN
TEST_COMPILATION: NOT_RUN
CARGO_CHECK_AND_BUILDS: NOT_RUN
MIGRATION_EXECUTION: NOT_RUN
CODEGEN: NOT_RUN
RUNTIME_REPRODUCTION: NOT_RUN
FORMATTING: PASS — cargo fmt --all --check (exit 0)
DIFF_CHECK: PASS — git diff --check, git diff BASE_SHA..HEAD --check, and accumulated working diff inspection (exit 0)

## Future verification, only after external acceptance and separate permission

1. Inspect `git diff BASE_SHA..HEAD` and this handoff; external static acceptance is not supplied by this document.
2. `cargo fmt --all --check` and `git diff BASE_SHA..HEAD --check`.
3. `cargo nextest run --locked -p pioneer-crud --test-threads 2 --no-fail-fast --retries 0 capsule_`.
4. `cargo nextest run --locked -p pioneer-gateway --test-threads 2 --no-fail-fast --retries 0 capsule_`, especially capsule_delegation_survives_nested_task_binding_recovery_and_revocation, and focused persisted_occurrence_bindings/thread_launch_choices/task_launch_continuity suites. `cargo nextest run --locked -p pioneer-crud --test capsule_bound_route_schema --test-threads 2 --no-fail-fast --retries 0` covers the entire schema suite, including fk_ fixtures.
5. After narrow suites, workspace validation per AGENTS.md: `cargo nextest run --locked --workspace --test-threads 2 --no-fail-fast --retries 0`; doctests separately if needed.
6. Run normal Migrator against an isolated temporary DB and follow crates/entity/AGENTS.md exactly to regenerate and compare Entity; CHECK/index-only change should not change model shape. Run the repository's authoritative protocol schema generation/comparison then compare the four edited schemas.
7. Validate later-turn dispatch, restricted identity, scheduled/interval/cron new occurrence, revoke-before-commit and Gateway restart in an isolated runtime; never reuse production diagnostic IDs as fixture authority.

Candidate is frozen after the handoff commit until an external reviewer correction prompt. No CI or runtime acceptance is implied.
