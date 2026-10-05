# Agent Plugins — C1 handoff

Status: **READY_FOR_STAGE_C1_REVIEW**. Source implementation submitted for review;
no test execution or behavior acceptance is claimed. This replaces the historical
partial C1 handoff; its native-stop blocker was accepted separately at the baseline.

## Revision and scope

- Branch: `feature/agent-plugins-simple`.
- Worktree/cwd for every command below:
  `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`.
- Resume base HEAD: `9e468ea67f1ad67d67783f496c6649384df43383`, initially clean.
- Final implementation HEAD: `3e007cdd199fcdd1575f0380c63fcf5b5ebb3a23`.
  A documentation-only commit delivers this handoff; its hash is in the final response.
- Resume commits: `386c011f` native stop/gate foundation; `d274303a` native atomic
  contracts; `1b6d206f` foreground lifecycle/shared contracts; `3e007cdd` desktop.
- Implementation was clean before writing this handoff. Main, archived branches,
  mobile/FFI and mobile worktree were not changed. No merge/rebase/push/deployment.

## Implemented paths and genuine operation reuse

Parent enable/disable, upload update/confirmation, explicit component retry/restore,
Continue repair and remove/purge run sequentially under one parent mutex. Gate/revision
commit precedes actual native graph/fallback and MCP stop acknowledgements. Unknown
owners, cancellation, transition, timeout or cleanup failure leave execution closed.
The accepted stop machinery is reused; no independent plugin execution engine exists.

Ordinary turn launch and native recovery share this same mutex through publication
acknowledgement. Inventory adds the existing manager's admitted/retiring identities and
trusted queued graph candidates. Final MCP reload takes the owning typed parent guard;
startup, restart and OAuth background launch cannot use the installed-but-pending gate.
Maintenance reconciliation only marks unfinished parents interrupted.

- Skills package install/update use `install_skill_source` / `update_skill_source`,
  the original materializer, security validator, prepare/commit/rollback, audit and
  native installation/policy stores. Assets-only changes force the full native update.
  Package update preserves the native ID, policy, stored trust and override mask.
- Removal calls extracted `uninstall_skill_with_plugin_change` and
  `uninstall_mcp_with_plugin_change`, which are also used by standalone handlers.
- Portable MCP uses the accepted typed adapter and the same `install_mcp_plan` body.
  Same-key native IDs, enabled/implicit flags and configured field overrides survive.
  OAuth compatibility is decided by the existing OAuth synchronize/disconnect engine.
- Native owned policy/config/source/uninstall edits acquire parent before native
  lifecycle admission. Their native record, policy/audit, override mask and pending
  outcome marker commit in the existing writer transaction. Standalone branches keep
  the legacy parsing, explicit Authorization, response and cleanup behavior.
- New secret values use existing keystore writers with fresh opaque refs. Pending
  retention precedes writing them; atomic publication changes the cleanup set. Strict
  owned cleanup retains the parent on error. Existing GC retains native and pending
  references from a bounded database snapshot, parsed after releasing DB capacity.

No tables, child installers, settings copies, OAuth engine, jobs, leases, operation
polling, version compatibility machinery or app domain data were added in C1.

## Lifecycle interruption and repetition

The current bounded parent plan (64 KiB / 256 members) holds keys, reserved IDs,
fingerprints, fixed host staging names and opaque cleanup refs, never native settings
or credential values. Upload owner/workspace/purpose/TTL validation, consumption and
the update gate commit atomically through existing upload repositories.

- Before stop ACK: Continue repeats genuine stop; used package/child files stay intact.
- Around package rename: fixed package/next/repair/backup paths and fingerprints detect
  the first rename, completed swap and wrong files. Continue repeats remaining work.
- Around native publication: actual rows/ownership and committed tree fingerprints
  distinguish committed siblings. Retry touches only requested failed components;
  omitted keys select failed components only. Successful siblings are retained.
- Package removal interrupted before forgetting a link uses an existing failed status
  plus `component_removed_for_update`; user removal stays `removed_by_user`. A fresh
  replacement that re-adds a deleted native identity explicitly warns/asks consent for
  new ID/default permissions. Normal updates never restore user-removed children.
- Lost package payload accepts an explicit fresh upload or Remove. Changed files fail
  integrity checks. Uncommitted native config/source/policy requires explicit reapply
  through the existing editor; Continue reports `reapply_native_change`. A committed
  native marker permits cleanup/reload/settle without replaying an unknown config.
- Remove repeats verified native uninstalls and strict OAuth/secret cleanup before
  deleting package and parent. Default preserves host data; explicit purge deletes
  only this parent's host data after stop ACK. No data receipt/restore service.
- Cancellation after a final writer commit rereads its authoritative state instead
  of restoring a stale pending plan. Partial component outcomes remain partial.

Concrete local addition: `package.integrity`, a bounded private 64-byte published-tree
hash outside package assets. Secure publication omits denied assets, so the authored
snapshot hash alone cannot verify published files on repair. This file stores no data,
settings or credentials; invalid/oversized/symlink metadata fails closed.

## Protocol and desktop

Specific RPCs: `plugins/setEnabled`, `update`, `remove`, `retry`, `continue`.
`plugins/preview` accepts optional target `{plugin_id, expected_revision}`; update
preview returns authored inventory and concrete additions/updates/removals,
authorization changes and identity-reset warnings. Mutations return final PluginItem
or confirmed removal, with existing busy/stale/forbidden/interrupted error patterns.
`PluginManagementIntent`/`PluginsMutateParams` are typed shared client adapters, not a
new generic wire endpoint. Protocol schemas were generated from the production exporter.

Desktop Plugins uses the shared typed client and uncertain-outcome refetch state.
Native Skills/MCP retained presenters receive a local details destination and verify
actual ownership; original config/policy/restart/OAuth controllers and browser callback
lifecycle are reused. The MCP editor constrains an owned edit to its authoritative
single server identity. Parent enable, archive/folder update preview/confirmation,
individual Retry/Restore, Continue and remove/data checkbox are localized EN/RU.
Busy, stale, network, auth, partial and interrupted states clear local pending/refetch.
Standalone screens/pickers still hide owned children; B composer remains one parent
chip. MEMBER disclosures redact hidden keys, identifiers and pointers in new preview,
repair/results and existing publications.

## Actual permitted checks

Final production source snapshot equals final implementation HEAD (the two final
code commits only partitioned these already-checked files).

| Command/check | Actual result / evidence |
| --- | --- |
| `CARGO_INCREMENTAL=0 cargo check -p pioneer-gateway -p pioneer-desktop-plugins --lib` | exit **0**, 1m25s, `target/plugin-c1-production-final-check.log`; includes changed production Agent/CRUD/Client/native desktop crates |
| `CARGO_INCREMENTAL=0 cargo run -p pioneer-protocol --bin schema -- schemas` | exit **0**, 33.16s, `target/plugin-c1-schema-generation.log`; exporter inspected, schemas only |
| `rustfmt --edition 2024 --config skip_children=true --check` over changed Rust files | exit **0**; also formatted changed files with rustfmt |
| `git diff --check` | exit **0**, final source snapshot |
| Parse changed locale TOML and plugin schema JSON | exit **0**, Python standard library only |

Earlier intermediate combined production checks also exited 0:
`target/plugin-c1-production-check-3.log` (4m08s), `...-4.log` (2m17s).
Intermediate scoped Gateway/desktop checks had exit 101 for SeaORM/raw-query and
lifecycle argument errors, the new upload enum's exhaustive match, and GPUI Task/
Checkbox imports; corrected before final checks. Earlier successful scoped checks
are intermediate compilation evidence only. Early `/tmp` logs became unavailable;
no behavior conclusion or final success is inferred from their command invocation.
Remaining compiler notices: existing unused `set_mcp_policy`; dependency `block 0.1.6`
future compatibility warning. No new unresolved production compilation error.

## Regression sources and remaining work

All regression sources **NOT_RUN**; own test targets **NOT_COMPILED**.

- CRUD ownership tests: scheduling/rollback, atomic native policy/audit/mask/marker,
  same-key ID/policy/override and sibling retention, package-vs-user removal,
  atomic upload owner/TTL/consumption, failure fingerprints and Maintenance interruption.
- Gateway: shared start/recovery admission, closed-parent direct MCP start, actual
  pending/retiring owners, fresh-secret typed remapping and pending GC protection.
- Package lifecycle: assets-only swap, both rename boundaries, lost-payload fresh
  repair, repeated swap, wrong fingerprint, bounded integrity/symlink rejection,
  MEMBER preview redaction. The prior B owned-policy fixture now uses admitted CRUD.
- Client: uncertain action requires refetch, parent-only history/draft selection,
  fixed-identity single-server native editor preserving explicit Authorization.

No app/fixture/provider/migration/smoke/device/functional scenario was run. These are
source regressions and compilation evidence, not behavior checks. Historical B
UNKNOWN_EXTERNAL_ACTIVITY (`cargo check --tests`, unknown owner/outcome) remains
external evidence and is not attributed to this C1 implementation.

No known C1 blocker is intentionally deferred. Coordinator review is still required;
UI/browser/stop/interruption behavior has not been exercised. C2 full provider and
continuation coverage, D mobile/generated native contracts/platform effects, and E
final review plus separately authorized tests remain outstanding. Unsupported native
provider continuation/CLI stop bindings stay explicit failures. **Stop after C1 review;
this submission does not accept implementation, authorize tests or start C2/mobile.**
