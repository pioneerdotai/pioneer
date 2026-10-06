# Agent Plugins — stage D repeat-review handoff

Status: **READY_FOR_STAGE_D_REVIEW**. Only D-01/D-02 and Android blocker investigation; E is not started. Source review and production compilation do not establish UI/provider/OAuth behavior.

| Worktree / branch | Fix base | Code HEAD | Delivery |
| --- | --- | --- | --- |
| Rust `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple`, `feature/agent-plugins-simple` | `528de95ff5c926089fd603e60c5b046bdef9de1b` | `d1fd496ba526254a3f3d22c0caa8b596e66033ad` | Commit containing this handoff; resolve with `git log -1 --format=%H -- implementation-notes/agent-plugins-stage-d.md` |
| Mobile `/Users/alexander/Code/pioneer/pioneer/.worktrees/agent-plugins-simple-app`, same branch | `fc6a46ad0bd26e536549bc7fe4e847aa40cabb8b` | `6c0b7794572ce1ae1dbb252c64cc0752a5ea1175` | Same as code HEAD |

Both fix bases were clean. Final tracked worktrees are clean after delivery; genuine native artifacts remain locally ignored under the existing policy. Main/archived branches were not changed; no push/deploy. Commit hooks were explicitly disabled. Initial D implementation is retained (Rust code `e9e275c9`, mobile `828c9916` plus `fc6a46ad`); its previous handoff remains available at `528de95f:implementation-notes/agent-plugins-stage-d.md`.

## Corrections and reused operations

- **D-01:** Plugins Work captures its immutable parent at admission. The actual worker pre-RPC boundary uses a checked current predicate and that captured value; it never indexes a mutable publications map. Close/controller stop/Close→Observe discard stale work. The controller mutex is released before any RPC/catalog wait; no catch-unwind workaround.
- **D-02:** Plugin child admission checks the originating epoch and the same live/busy parent generation under a short guard that spans only native validation/enqueue. `skills_intent_bound` / `mcp_intent_bound` call the same extracted native operation body as ordinary `skills_intent` / `mcp_intent`. Both entry and enqueue revalidate an explicitly expected epoch; queued native Action/Work retains it. Existing native workers send through `requests_for_connection(work.epoch.2)`, so replacement after admission cannot redirect a mutation to B. Skills policy coercion/lifecycle gates, MCP validation/configuration and native optimistic/rollback/completion behavior remain in that one body. Existing public standalone signatures pass `None` and retain their legacy admission branch.
- ConfigureMcp, Skills Policy/Remove, MCP Policy/Restart/Remove/SignIn/Disconnect/Cancel/RetryBrowser all use bound admission. Existing Retry browser now receives its queued epoch instead of recapturing it; scheduling and the browser worker validate it. Browser admission carries immutable flow connection. Native shell Cancel uses `mcp_oauth_cancel_relay_bound(event, admission.origin_connection())`, invoking the same native Cancel RPC; its old unbound public entrypoint remains available. Callback exchange still uses the existing original-flow bound relay. No second OAuth engine/installer, new tables, operations framework or wire DTOs were introduced. New shared shell primitives document desktop OS-adapter migration next to their definitions.

D management/archive/composer/history flows remain as delivered: selected Gateway management, native Skills/MCP child operations, bounded phone-cache archive bytes with explicit preview consent, existing native browser/callback effect, separate Plugins modal and one parent capability/chip. Common catalogs retain owned children; standalone projections hide them. Gateway expansion and accepted provider paths remain authoritative. UI/OS effects stay in mobile/FFI; shared typed behavior remains UI-neutral Rust. No new mobile UI or lifecycle scope was added in this fix.

## Actual non-test checks

Rust cwd = Rust worktree above. Mobile cwd = mobile worktree above; every mobile command used `PIONEER_RUST_ROOT` equal to that Rust worktree. Scripts were read before execution: schema production binaries, generators, Rust release libraries and static checks; no test/app hooks. Checks below use the committed `d1fd496b` / `6c0b779` source bytes (committing does not alter the digest).

| Actual command / scope | Exit | Evidence |
| --- | --- | --- |
| `cargo check --locked --offline -p pioneer-client -p pioneer-client-ffi --lib` | 0 | `/tmp/pioneer-d-fix-cargo-check-final.log`; preserved crate-private unbound Retry helper now has only test consumers and produces a dead_code warning |
| First `bun run client:schema`, started before the final related FFI correction finished | 1 (nested Cargo 101) | `/tmp/pioneer-d-fix-schema.log`; intermediate core/FFI API mismatch, superseded by the stable-source regeneration below |
| `bun run client` (genuine schema → types → source contract) | 0 | `/tmp/pioneer-d-fix-client-generation-final.log`; schemas/types unchanged, source contract genuinely updated |
| `bun run client:check` | 0 | `/tmp/pioneer-d-fix-client-check.log`; reproducible 549 schemas / 550 generated files |
| `bun run nitro:generate` | 0 | `/tmp/pioneer-d-fix-nitro.log`; genuine generator, outputs unchanged |
| `node modules/pioneer-client-nitro/scripts/boundary-integrity.mjs check-source` | 0 | `/tmp/pioneer-d-fix-check-source.log`; current source matched |
| `node node_modules/typescript/bin/tsc --noEmit --project /tmp/pioneer-d-tsconfig.json` | 0 | `/tmp/pioneer-d-fix-ts.log`; production only, `.test.*`, `.spec.ts`, `__tests__` excluded; no `.spec.tsx` sources present |
| Scoped `rustfmt --edition 2024 --check` / both `git diff --check` | 0 | `/tmp/pioneer-d-fix-format-final.log`, `pioneer-d-fix-diff-rust.log`, `pioneer-d-fix-diff-mobile.log` |
| Prettier source-contract check with default repository indent / generator's canonical `--tab-width 2` | 1 / 0 | `/tmp/pioneer-d-fix-prettier.log`, `pioneer-d-fix-prettier-generated.log`; generator bytes preserved, no manual formatting/hash edit |
| `bun run rust:build:android` / `bun run rust:check:android` | 1 / 1 | `/tmp/pioneer-d-fix-android-build.log`, `pioneer-d-fix-android-integrity.log`; NDK unavailable, all four libraries missing, **NOT_BUILT** |
| `bun run rust:build:ios` / `bun run rust:check:ios` | 0 / 0 | `/tmp/pioneer-d-fix-ios-build.log`, `pioneer-d-fix-ios-integrity.log`; genuine device (2m39s) + simulator (2m55s) release builds, XCFramework, seal and separate static integrity check |

Source identity: Rust `4dcade08ea2118a4a20d628a2c0c39e87532d2ea04b3ae3b91e13e94e44d5db9`; mobile boundary `f946a17499ca8a69854e305313514e1eb08f13b7e1a0255bf36a25acd3fa7d3d`; source-contract SHA256 `9a9f142863e49f05f7ab95dbcd63a33ff9d212977995bec867c7a8ba5ed48a99`. Genuine ignored manifest: mobile `modules/pioneer-client-nitro/rust/ios/integrity.json`. Device library SHA256 `20d94fd9a0d9362af38ee50fcf8411aba3bc6fe914b159d056803734992e5d3d`; simulator `795b9ab95ad9a79775f7dab7883b0aaae8123a8a82f020de647bae12e95fffd2`; both headers `27b4c1a27fab6801d7b3a20c6c48eb17b91bc760a27a94ef24020f0bbf5c4f4d`. The manifest also identifies Info.plist. These artifacts replace baseline outputs and match the fix snapshot; no library/source hash was manually substituted. Final committed-source recheck also exit 0 (`/tmp/pioneer-d-fix-check-source-delivery.log`).

## Regression sources and limits

New regression sources are **NOT_RUN / NOT_COMPILED**:

- `client/plugins/runtime.rs`: controlled pause after the first current check; Close/stop with absent map and Close→Observe; immutable old parent, rejected stale child admission, accessible mutex and the next Observe handled by the same pre-RPC worker cycle.
- `client/skills/operations.rs`, `client/mcp/operations.rs`: controlled pre-admission pause, A→B with copied IDs/catalogs/management permission; Policy/Remove and every MCP intent including Configure/SignIn/Disconnect/Cancel/Retry enqueue nothing on B. Already admitted work retains its original epoch and becomes non-current on replacement before send.
- `client/mcp/oauth.rs`: copied flow retained across replacement; bound Retry rejects before any browser worker/callback scheduling, immutable flow connection remains A.
- `client/transport/ws/runtime/command_sender.rs`: raw command queue, replacement between bound transport creation and send; mutation/OAuth Request carries expected A rather than current B. Existing worker rejects that expected-connection mismatch. `catalog_test_support.rs` adds only a synthetic identity-switch helper.

These are source assertions, not passed behavioral checks. Existing D regression sources retain their NOT_RUN/NOT_COMPILED status. Own tests/test targets, apps, fixtures, functional/device/simulator/browser/provider/OAuth/migration scenarios: **NOT_RUN / NOT_COMPILED** as applicable. Historical **UNKNOWN_EXTERNAL_ACTIVITY** remains separate: an external `cargo check -p pioneer-crud -p pioneer-gateway -p pioneer-migration --tests` was observed during original D, not launched/controlled here and not evidence for this implementation.

## Android blocker and remaining work

Android is **NOT_BUILT**, not completed. `/tmp/pioneer-d-fix-android-discovery.log` records read-only discovery: `ANDROID_HOME=/Users/alexander/Library/Android/sdk`, but that path and `/Users/alexander/Library/Android` do not exist; NDK overrides are unset. No real SDK/NDK was found in checked user/Library/Applications, Homebrew Cask/share, `/usr/local`, `/opt`, local SDK roots, Downloads or repository `local.properties` locations. `cargo-ndk` exists at `/Users/alexander/.cargo/bin/cargo-ndk`; all four Android Rust targets are installed. Actual build reports **Could not find any NDK**, exit 1; actual static check reports all four `.so` missing, exit 1. No SDK was installed/rewritten and no Android output was sealed.

To unblock: provide/install a real Android SDK with **NDK (Side by side)**, set `ANDROID_HOME` to its existing SDK root and `ANDROID_NDK_HOME` to that root's `ndk/<installed-version>` directory (containing the actual LLVM toolchain), then run the existing `rust:build:android` and `rust:check:android` in this mobile worktree with the same feature `PIONEER_RUST_ROOT`. Those scripts build/seal/check armeabi-v7a, arm64-v8a, x86 and x86_64. No build-system redesign is needed.

Unresolved: Android toolchain/artifacts and separately authorized behavioral testing. No other v2 deviation or new management infrastructure. E still requires coordinator acceptance and explicit test authorization: actual archive/security/lifecycle failures, wrong Gateway, OAuth native registration/callback/background/cleanup, parent-only composer/history/draft/voice/Task/edit/retry and provider behavior. Nothing in these production checks establishes those outcomes; E has not begun.
