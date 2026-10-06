# Agent Plugins — stage D handoff

Status: **READY_FOR_STAGE_D_REVIEW**. Source review only; E is not started. Android native build is blocked by an undiscovered/unconfigured NDK, recorded below.

| Repository / worktree | Branch | Base | Code HEAD | Delivery |
| --- | --- | --- | --- | --- |
| Rust: `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple` | `feature/agent-plugins-simple` | `983dc787ba5fab229a14632c3043cfe1426b618c` | `e9e275c9677da137f855586e349a3cbef258b1a6` | Commit containing this handoff; resolve with `git log -1 --format=%H -- implementation-notes/agent-plugins-stage-d.md` |
| Mobile: `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple-app` | `feature/agent-plugins-simple` | `2867e763c0603b0c170bab61c57e7ad1d133d0c5` | `fc6a46ad0bd26e536549bc7fe4e847aa40cabb8b` (initial D commit `828c9916`) | Same as code HEAD |

Both bases were clean. Delivery worktrees are clean after the handoff commit; native outputs stay locally ignored under the existing policy. Main and archived implementation branches were not changed. Commit hooks were disabled explicitly; no push/deploy/app launch.

| Existing entrypoint | Mobile gap | Minimal adaptation | Regression sources |
| --- | --- | --- | --- |
| `client/plugins.rs`, composer domain/catalog | No scoped Plugins management/picker | Shared typed intent/publication and existing bounded client worker pattern; target-only parent toggle | `plugins.rs`, `plugins/runtime.rs`, `composer/catalog.rs` |
| Skills operation controller / upload flow | Phone archive and explicit preview consent | Same bounded uploader; native cache URI read on its worker; preview retained until explicit Install/Apply | `skills/operations.rs`, `skills/upload_flow.rs` |
| Native Skills/MCP operations and OAuth shell | Child settings / phone callback | Reuse native intents and Health/details reads; native OS mailbox relays existing flow/action | `client-ffi/plugin_shell.rs`, `mcp-oauth/service.rs`, mobile `oauth.test.ts` |
| Mobile binding/settings/composer/history | No Plugins surfaces / MCP icon fallback | Existing scoped binding, full management card, separate modal, explicit Plugin presentation | Existing shared domain and above regressions |
| Schema / TS / Nitro / native scripts | Baseline mobile contracts predate accepted C2 | Genuine export/type/Nitrogen generators and production library builds | Non-test reproducibility/integrity checks below |

## Connected source flows

- Settings → Plugins list/card: parent enable, install preview/confirmation, update preview/apply, interrupted-operation Continue, failed component Retry, explicit removed-component Restore consent, remove with explicit purge-data choice (default retained). Busy/error/partial/read-only/offline/stale states remain visible. A timeout is uncertain; refetch is required before another mutation.
- Child cards invoke accepted native Skills Policy/Remove/Update and MCP Policy/Configure/Restart/Remove/SignIn/Cancel/RetryBrowser/Disconnect. Native IDs, owned configuration name protection, child restrictions, installer audits/security/OAuth/rollback remain authoritative. Skill trust/dependency/security/validation gates and MCP runtime/health/transport/cleanup are shown. Skill source override uses the native Update operation with archive bytes.
- Mobile file effect copies a document into app cache. FFI validates a local file URI and queues the existing uploader; phone URI never becomes a Gateway filesystem path. Start/chunks/finalize/preview/apply and cancellation retain origin connection/epoch. Preview is claimed once. Cache cleanup follows preparation completion or explicit cancellation. Standalone desktop upload branch is retained; native Skills/MCP mutation requests now use the existing bound sender too.
- `/composer-plugins` is a separate immediate modal. Shared catalog/selector/reducer stores one parent capability `{plugin_id, expected_revision}` per chosen plugin, and removes that parent only. New selection requires current proven combined Skills/MCP target support; selected unknown/stale parents remain removable and retain their original revision. No child options/expansion/chips are sent by mobile.
- Text send and detached Task use the existing shared composer submission/turn preparation. Voice capture/context/finalize also use that shared draft/operation snapshot. Existing draft persistence/reconciliation and edit/retry/message revision/history paths carry the same parent metadata; history and composer now explicitly render Puzzle for Plugin. Current Gateway/provider admission and C2 projections perform expansion and isolation. These are source traces, not executed scenarios.
- Existing shared Skills management/composer projections exclude `plugin_owner` records; MCP standalone selectors do likewise. Common catalogs still retain owned records for parent details. There are no mobile standalone Skills/MCP settings screens in this baseline to redesign.
- OAuth uses `pioneer[-dev]:///oauth/mcp/callback` via Expo WebBrowser/Linking/AppState. Native mailbox validates exact route, live flow, nonce, one response and duplicate delivery; existing shared bound callback/Cancel relays reach the original Gateway/workspace/installation. Exchange/refresh/revoke/credentials stay on Gateway. Failed/locked browser launch and rejected browser response retain the existing flow for Retry; only actual cancel/dismiss requests native Cancel; Cancel/expiry/retirement close local effect ownership; closing a card does not revoke consent. Router payload is cleared; authorization URL/code/state are not logged/persisted.

Shared state/intent/selector and effect planning are UI-neutral `pioneer-client` items. Module comments explicitly describe desktop migration from its shell-local catalog/action state to these typed publications; desktop retains direct Rust native operations. JSON/Nitro/C ABI and native file/browser mailbox belong to `client-ffi`; OS picking/browser/navigation/localization belong to mobile. No Gateway operation framework, component installer, OAuth engine, DB table, native plugin bypass, Apps runtime or compatibility layer was added.

Two concrete backend/build gaps: Gateway originally accepted only loopback redirects; its existing validator now also accepts the two fixed native callback routes. Desktop loopback/native explicit Authorization semantics remain unchanged. `libc 0.2.190` removed iOS declarations still used by `backtrace 0.3.76`; workspace pins `0.2.189` and changes only that lock entry. Initial iOS failure is recorded below. Two desktop adaptations handle the new upload target and named preview DTO without UI redesign.

## Actual non-test checks

Rust cwd is the Rust worktree above; mobile cwd is the mobile worktree above. Every mobile generator/build/check below had `PIONEER_RUST_ROOT` pointing to that Rust worktree. Source checks include current code bytes; commits do not change their digest. Logs are local `/tmp` evidence, not checked-in command journals.

| Command / scope | Actual exit | Log / result |
| --- | --- | --- |
| `cargo check --locked --offline -p pioneer-client-ffi -p pioneer-mcp-oauth -p pioneer-desktop-plugins -p pioneer-desktop-skills --lib` after pin | 0 | `/tmp/pioneer-d-scoped-check-pinned.log`; production libs only, existing `block` future-incompat warning |
| `client:schema`, `client:types` genuine generators + `bun run client:check` reproducibility | 0 (reproducibility) | `/tmp/pioneer-d-schema5.log`, `pioneer-d-types4.log`, `pioneer-d-client-contract-check-final.log`; 549 schemas / 550 TS files; generated baseline C2 drift included, tuple DTOs replaced with named shared structures |
| `bun run nitro:generate` | 0 | `/tmp/pioneer-d-nitro.log`; genuine Hybrid method/code generation, `pluginShellJson` |
| `bun run client:contract` then explicit `node .../boundary-integrity.mjs check-source` after pin | 0 | `/tmp/pioneer-d-source-contract-pinned.log`, `pioneer-d-check-source-pinned.log`; final committed-source recheck also 0 (`pioneer-d-check-source-final-delivery.log`) |
| `node node_modules/typescript/bin/tsc --noEmit --project /tmp/pioneer-d-tsconfig.json` | 0 | `/tmp/pioneer-d-ts-check-delivery.log`; production `src`, Nitro spec, root theme augmentation; excludes all `.test.*`, `.spec.*`, `__tests__` |
| Scoped rustfmt / Prettier checks; both `git diff --check` | 0 | `/tmp/pioneer-d-rust-format-final.log`, `pioneer-d-prettier-check-final.log`; formatting only |
| `bun run locale` | 0 | `/tmp/pioneer-d-locales-final.log`; EN/RU plus genuine translation resource generation; other locales use existing fallback |
| Initial `rust:build:ios` | 101 | `/tmp/pioneer-d-ios-build.log`; upstream `backtrace/libc` `_dyld_*` declarations failure, fixed by exact compatible pin |
| Final `rust:build:ios` / `rust:check:ios` | 0 / 0 | `/tmp/pioneer-d-ios-build-pinned.log`, `pioneer-d-ios-integrity-final.log`; real aarch64 device + simulator release libraries, XCFramework + sealed/matched integrity. This is a Rust library build, not an application/device run |
| `rust:build:android` | 1 | `/tmp/pioneer-d-android-build.log`; cargo-ndk reports **Could not find any NDK**; four Rust targets are installed, NDK is not discoverable/configured |
| `rust:check:android` | 1 | `/tmp/pioneer-d-android-integrity.log`; all four `.so` artifacts missing. NOT_BUILT, not sealed or passed |

Earlier local Rust/TS/schema iterations did fail and were corrected; final checks above supersede them. Dependency installation used `--ignore-scripts`, so install hooks/test runners did not run. No source digest was edited to disguise a native mismatch; source contract is genuinely regenerated, and successful native outputs must be built/sealed against it.

Source identity: Rust `abeed596fdd5a53a8b0d1f3ae278b804f5220a7b76929274ccbb97e051cfe682`, mobile boundary `f946a17499ca8a69854e305313514e1eb08f13b7e1a0255bf36a25acd3fa7d3d`. Source-contract file SHA256: `20979af9b27b53aed506ffb4328a7c926dab651828a3efb1ee3ed7a30a079530`. Genuine ignored `modules/pioneer-client-nitro/rust/ios/integrity.json` records device library `fe6c97f9d72dc8a42b759ab90de43612d4e765f098420c371e7ccdcfb5cf668a`, simulator library `ba6a2fdd813f9cd2e0bb1a3564686ffa5ff08cc2ad78b199e3e88cef3b96cc0e`, and both headers `27b4c1a27fab6801d7b3a20c6c48eb17b91bc760a27a94ef24020f0bbf5c4f4d`. Device `xcrun nm -gU` confirmed `_pioneer_client_ffi_plugin_shell` (exit 0, `/tmp/pioneer-d-ios-device-symbols.log`). Android artifacts are unavailable because NDK is not discoverable/configured. Existing scripts require a real rebuild before integrity passes; no prebuilt hashes were substituted.

## Regression sources and remaining E work

All new/updated test sources are **NOT_RUN / NOT_COMPILED**: target-only toggle preserving another parent's A7 revision under A8 catalog refresh; disabled/unknown/stale parent projection and target support; closed/uncertain/foreign-connection and close→reopen admission; origin-bound native upload/preview without auto-install; one-use pure upload-flow preview completion; exact native callback route/nonce/duplicate/retirement; native redirect versus retained loopback boundaries; mocked TS callback routing and original-flow handling of locked/cancel/dismiss browser outcomes. Legacy upload test patterns were adapted to the internal bound-flag tuple without compilation.

Own app/device/simulator/provider/browser/OAuth/fixture/functional/smoke/conformance scenarios: **NOT_RUN**. Historical coordinator activity remains **UNKNOWN_EXTERNAL_ACTIVITY**. A concurrent external `cargo check -p pioneer-crud -p pioneer-gateway -p pioneer-migration --tests` was observed during reconciliation; it was not launched/controlled here and is not evidence for D.

Unresolved: install/configure an Android NDK, then genuine four-ABI production rebuild and integrity checks. E still needs separately authorized behavior/tests: mixed/empty/partial archives, modes/containment/limits/cancel/expiry and wrong Gateway, lifecycle partial/repair/stale consent, OAuth provider native registration + cancel/background/resume/cold/stale callbacks + cleanup, parent-only drafts/history/voice/Task/edit/retry for native/Claude/Codex, permissions/trust and source overrides. No functional OAuth success or device UI acceptance is claimed. E requires coordinator acceptance and separate permission; no E work has begun.
