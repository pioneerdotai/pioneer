# CUA Gateway migration — handoff

Current status (2026-10-24): the prior migration acceptance is superseded for the server product by the server-contract audit. The restoration below is an uncommitted candidate for full external static review; no execution phase is authorized. Historical review/publication records are preserved unchanged. See the final “Server/computer-use contract restoration” section.

Publication update: the user accepted this implementation and authorized commit/push, PR creation and CI repairs. PR: https://github.com/pioneerdotai/pioneer/pull/172. The review-round statuses and repository snapshots below are preserved historical records from before publication. The accepted implementation was committed as `7e208b9912d3615392d83aeb25e7f92137711f2b`. Current CI follow-up is recorded at the end.

**Готово к повторному внешнему статическому ревью; компиляция и тесты не запускались.** Round 2 residual G02 and G12 fixes are unstaged over the unchanged 114-path reviewed index and preserved round-1 fixes. G01 and G03–G11 were closed by the external round-2 static audit; they were not rewritten. The existing untracked build-support file is preserved and listed below. Daemon, inventory, UI, signing, packaging and installation were not executed. Round-1 task history and round-2 task resolution are recorded below; internal integration is not external acceptance or permission to execute.

## Immutable product contract and product review

The only execution topology is: any authorized client or autonomous task → selected Gateway → Gateway-owned local cua-driver daemon → that Gateway's computer. Files, applications, windows, display, input and browser paths belong to the Gateway machine. Desktop, mobile and CLI are clients. No Desktop registration, window, connection, computer-use transport, client-origin routing or fallback participates in this implementation.

Gateway owns trusted launch configuration, readiness, daemon process/IPC identity, admission, sessions, cursor, cancellation, generation retirement and shutdown. OS grants never replace Pioneer authorization of each operation and its complete arguments. No in-process automation runtime is constructed. Closing a client is independent of cancelling its task or stopping Gateway.

Product-contract self-review preceded technical review. The injected backend is constructed only in Gateway startup; all tool handlers receive that same backend. Desktop source has no new execution or driver owner. Its package contains the same standalone Gateway payload. Existing macOS GUI LaunchAgent, Linux user service and Windows interactive user task remain the service boundaries. Source review found no ownership/lifetime dependency on an initiating client. These are source conclusions, not demonstrated platform behavior.

## Repository state

- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/cua-gateway-migration`.
- Branch: `feature/cua-gateway-migration`.
- Actual committed base and current HEAD: `6c1e6b2c49359c76e49624c0545bbafc4a3d310a`.
- Prompt-preparation reference `e1e6c5dbf544afe04b2bc92869180219804a409e` was already superseded by committed main at start. No old migration implementation, deleted branch, reflog or Git object was sought or restored.
- Main checkout was clean at start and the later read-only observation; HEAD was still the actual base. It was not edited. No other worktree was changed; no reset, clean, stash, merge, rebase, push or install occurred.
- Git author identity observed: `alexanderoskin <alexander.oskin@gmail.com>`. No commit was made. The complete change is an uncommitted worktree diff; final staging/status is recorded at the end.
- Cargo.lock is unchanged. It still describes the previous graph, including now-unused legacy packages. Its resolution/removal of unused entries and the pinned SDK dependency graph are expressly deferred. No checksums or package versions were invented and `--locked` was not weakened.

## Pre-implementation decision and pinned source evidence

This decision was recorded before coding in this same document: Gateway directly owns the supported daemon-backed `EmbeddedCuaDriverHost`, connects through `CuaDriver::connect`, and creates private trusted sessions. Never use `CuaDriver::create` or a second in-process engine. Keep startup/stop futures owned through cancellation and reap a generation before replacement. macOS needs a real signed Gateway app responsibility chain; advisory labels and AssociatedBundleIdentifiers are insufficient. Windows/Linux require the Gateway's real interactive/GUI session. Original capture bytes and metadata use existing ArtifactService and multimodal delivery.

All SDK, daemon and copied catalog source is pinned to **688c5fd62a9f13dddc309970c948df494e7de33f**, workspace **0.38.0**, contract **0.8.0**, MCP **2025-06-18**. No pin change or main-API mixing was used.

Primary evidence, all at that immutable revision:

- [Embedding contract](https://github.com/trycua/cua/blob/688c5fd62a9f13dddc309970c948df494e7de33f/libs/cua-driver/rust/Skills/cua-driver/EMBEDDING.md): direct child and TCC responsibility, Standard mode, read-only permission probes, parent liveness, GUI-session limitations.
- [SDK embedded lifecycle](https://github.com/trycua/cua/blob/688c5fd62a9f13dddc309970c948df494e7de33f/libs/cua-driver/rust/crates/cua-driver-sdk/src/embedded.rs): owned child, safe environment, private endpoint, PID/contract/version handshake, startup cancellation and stop/reaping.
- [SDK session client](https://github.com/trycua/cua/blob/688c5fd62a9f13dddc309970c948df494e7de33f/libs/cua-driver/rust/crates/cua-driver-sdk/src/service_session.rs): persistent authenticated transport, resume credentials, unknown action interruption; infallible close discards its response, so Pioneer first checks real end_session cleanup.
- [Canonical native manifest](https://github.com/trycua/cua/blob/688c5fd62a9f13dddc309970c948df494e7de33f/libs/cua-driver/contract/manifest.json) and [browser tool schemas](https://github.com/trycua/cua/blob/688c5fd62a9f13dddc309970c948df494e7de33f/libs/cua-driver/rust/crates/cua-driver-core/src/browser/tools.rs): passive schema source, no runtime inventory calls.
- [Session cleanup](https://github.com/trycua/cua/blob/688c5fd62a9f13dddc309970c948df494e7de33f/libs/cua-driver/rust/crates/cua-driver-core/src/session_tools.rs), [actual identity report](https://github.com/trycua/cua/blob/688c5fd62a9f13dddc309970c948df494e7de33f/libs/cua-driver/rust/crates/cua-driver-core/src/health_report.rs), [workspace](https://github.com/trycua/cua/blob/688c5fd62a9f13dddc309970c948df494e7de33f/libs/cua-driver/rust/Cargo.toml), [upstream lockfile](https://github.com/trycua/cua/blob/688c5fd62a9f13dddc309970c948df494e7de33f/libs/cua-driver/rust/Cargo.lock).

The SDK has relative workspace dependencies on contract/core/platform/cursor crates. The daemon additionally depends on `libs/cua/crates/cua-telemetry`, outside cua-driver. The package producer therefore fetches a clean **full** pinned repository and builds in its original workspace with its original lockfile and target configuration. The source archive was downloaded for reading only. The 17 vendored native entries retain the source name/description/input_schema fields exactly; output schemas/platform/capability metadata are omitted from this listing projection. Browser schema expressions are copied from pinned source. Public materialization removes the optional host-owned session property and adds Gateway/session/no-replay instructions. No platform registry or inventory overlay is constructed.

MIT license and upstream third-party notices accompany the payload and vendored schemas. Optional AGPL perception is not built/shipped or exposed. Full binary dependency/license audit remains a packaging qualification item.

## Requirement → owner/file map

| Requirement | Implementation owner/files |
| --- | --- |
| Standalone/mobile/remote Gateway ownership, no client lifetime | gateway/computer_use.rs, gateway/lib.rs; existing service and client transport boundaries |
| One process, bounded queue, private identity, stop/restart/cancel | gateway/computer_use.rs; Gateway shutdown and message/agent_runtime.rs terminal boundary |
| Per-call authorization, exact allowlist, frozen CLI binding | tools/permissions/mod.rs, protocol/turn.rs, tools/handlers/computer_use/catalog.rs, gateway/authorization/catalog.rs, turn_mcp/native_tools.rs, mcp_service.rs |
| Native/provider and permitted skill proxies | tools/lib.rs, handlers/computer_use/mod.rs, handlers/skill.rs, runtime.rs, classifier.rs, orchestrator.rs |
| Typed original images, artifact authorization/quota/history | computer_use/images.rs, message/artifact_tools.rs, artifacts/provider_resolver.rs, tools/context.rs, agent/chat/mod.rs |
| Provider result-group order and CLI image content/frame bound | provider/attachments/mod.rs, gateway/mcp_service.rs, cli_runtime/mcp/facade.rs |
| Upstream visible cursor and real cleanup | gateway/computer_use.rs; pinned get_agent_cursor_state/get_session/end_session contracts |
| Gateway OS identity and GUI-session services | cli/service/macos.rs, windows.rs; existing linux.rs; config/default.toml |
| Six-target standalone and Desktop-carried package | packaging/cua-driver.py, build-gateway.py, desktop package scripts, release.yml, installers |
| Legacy removal and remaining shared contracts | tools/handlers/computer_use deletions; tools/config/protocol/prompt/registration cleanup; Cargo manifests |

Paths in this table abbreviate `crates/` and `scripts/` only for readability; exact per-file coverage follows.

## Lifecycle, cancellation and authorization

Gateway startup creates one supervisor worker independently of all client connections. Driver startup is lazy; missing packaging, GUI capabilities or grants reject computer use without preventing the rest of Gateway from starting. Queue capacity is 16, active owner admission 32. Admission reserves bounded queue space before registering an owner. The worker serializes all computer actions across native, CLI and proxy paths; no daemon is created for each call.

The trusted executable comes from the installed sidecar, never an agent endpoint/override. Manifest admission checks repository, immutable revision, version, native target, permission owner, relative executable and SHA256. Private random endpoints are Unix sockets inside a Pioneer-prefixed private temporary directory or Windows named pipes. The pinned daemon enforces Unix peer identity / Windows pipe ACL and host PID restrictions. A protected runtime-home lock excludes a second owner. A bounded numeric PID receipt refuses replacement while an old crashed-owner daemon is still alive; PID reuse fails closed and requires explicit recovery, never signalling or attaching to a foreign process. There is no new persistence framework.

Startup remains polled alongside SDK stop after cancellation, rather than dropping a partially spawned child. The supervisor records the real ready PID before any session/action, verifies driver 0.38.0 and relies on the pinned SDK's metadata compatibility/parent checks. Each generation gets a new endpoint and generation ID. Standard runtime and session modes are explicit, bypass is false, stderr inheritance/driver telemetry/update check are disabled. SDK-managed administrative environment is host-side only; conflicting policy may fail startup, never grant agent authority.

Each turn has its own private trusted session. Every call retains Pioneer ComputerUse permission evaluation and full-argument approval hashing, including proxy delegation; CLI additionally retains existing McpUse authority, frozen binding/schema identity and invocation correlation. Host session/cursor/trust/admin tools are absent from the public 25-operation allowlist. Top-level session/reserved fields are rejected. Raw CUA-looking JSON or an external name provides no media or authorization provenance.

The worker observes cancellation, reply disappearance and a 120-second operation deadline. Lost/unparseable IPC, timeout, disconnect or cancellation retires and reaps the whole generation before new work. A failed stop poisons the worker: no replacement is admitted. SDK blocking IPC may finish late only against that retired private endpoint. No mutation is replayed automatically. Queue rejection occurs before IPC. Action results preserve refusals/partial/unverifiable effects; delivered input is not goal completion.

A fresh window/desktop/browser observation is required before input. Every mutation invalidates all turns' prior observations of the shared computer. Session cleanup can close owned browser windows, so it also invalidates other observations. Descriptive session/generation metadata accompanies original tokens; it is not trusted input authority. New sessions/generations require fresh observation.

Owner lease drop releases a turn; common committed native turn finalization and existing CLI terminal/cancellation cleanup explicitly revoke it even if proxy handles remain. The host checks actual end_session cleanup before closing its transport; incomplete/lost cleanup retires the generation. A typed local delivery receipt holds the per-owner gate across artifact admission, runtime projection and durable outer completion; error/drop revokes pending authority. Native nested proxies defer their receipt; the native agent additionally retains it with ExecutedToolResult until exact typed ChatMessage history append ACK and bounded provider-message preparation, committing within each result future before the parallel batch gather. Early ItemCompleted only retains technical projection and grants no observation authority. CLI holds the same receipt until the actual final encoded response fits the facade's actual ceiling. The host additionally checks a private generation/nonce acknowledgement, invalidated by intervening input/cleanup; that nonce is stripped before model/history/telemetry. Real Gateway shutdown joins the worker and stops its daemon; abrupt Gateway death is also fenced by the upstream parent-liveness pipe and subsequent PID receipt. Closing Desktop/client has no such hook.

Only exact registered CUA leaves and their configured function-proxy chains of at most two edges receive the new admission inheritance, Never/one-attempt recovery and cancellation grace. Inheritance also checks the same lock Arc and actual immediate configured origin/target against the nested permission chain. Unrelated dispatch clears ambient CUA scope and retains its prior recovery/classification. Exclusive→Shared inheritance works with a queued writer; Shared→Exclusive rejects through the existing common terminal event/finish_trace path. Router-aware runtime/model Err classification and the private orchestrator dispatch scope preserve no-replay through both wrappers. Typed delegated outcomes/media/provenance survive the chain; content-free CUA technical projection remains enforced. No general scheduler was introduced.

## Images, provider history and CLI

The trusted producer decodes genuine upstream MCP image parts after backend execution, with count/per-image/total allocation bounds. It admits the **original bytes** through ArtifactService after revalidating ArtifactCreateThread, ArtifactBindThread and ArtifactRead at a consistent policy generation. Call/part identity, MIME and original-byte digest are hashed into a valid artifact ID; a new CLI process generation reusing a JSON-RPC ID cannot alias different old capture bytes. Existing quota, binding and retention/GC contracts own storage; no CUA screenshot store or coordinate transform exists.

Only the binary data field is replaced in retained result JSON by its exact admitted artifact identity. Geometry, origin, scale, window/session/generation and element tokens remain unchanged. Typed Image parts carry the original artifact-backed path/version; typed Text retains capture metadata. Generic attachment-path inference is confined to existing native artifact/HTML producers. Real nested proxies carry their actual nested runtime's media, not inferred outer JSON.

Canonical full result/history is persisted before bounded model projection. Artifact-backed original blob paths avoid an unrelated readable-copy TTL. Capture metadata/captions stay paired with admitted media; if framing cannot fit, the existing bounded-context error is returned rather than silently corrupting geometry/tokens. Current complete tool-result groups and pinned user media are preserved; only older tool media is eligible for normal request-budget eviction. Budget failure remains explicit if protected content cannot fit.

Chat-format preparation derives image User messages **after the entire contiguous tool-result group**, preserving all tool IDs/order and canonical history. Anthropic/Gemini/Bedrock retain their own nested formats. Existing attachment preparation owns indexes, media validation and budgets; Pioneer introduces no resize/re-encoding loop.

CLI materializes the same backend in a bounded per-turn cache, preserving session/cursor between calls. It reads exact authorized artifact bytes and emits actual MCP Image content, plus metadata text. A private typed receipt remains pending through invocation audit and final encoding, including escaped request ID, metadata, JSON-RPC envelope and newline. Only a fitting result commits it; error, oversize, cancellation or dropped future revokes the session. The old global-limit-minus-1024 CUA estimate is removed. Cache cleanup cancels active calls and removes the turn under the same mutex used for cancellation-aware admission; a CUA liveness re-read after registration covers waiters registered after committed terminal cleanup. Oversize results return a bounded correlated noReplay/unknown error; the existing invocation ledger prevents reexecution of the same request.

Technical event arguments are hashes/lengths for CUA and function-proxy envelopes; unrelated HTTP/shell skill argument behavior is retained. CUA technical output contains success/incomplete facts and controlled diagnostics, not screen/tree/text/base64/stderr. Canonical conversation content remains the authorized model/history surface.

## OS ownership and source state of all six targets

All rows describe implemented source/package paths, **not qualified binaries**.

| Target | Gateway/service/daemon identity | Source requirements and remaining qualification |
| --- | --- | --- |
| x86_64-apple-darwin | Per-user LaunchAgent in gui/uid runs bin/cua/Pioneer Gateway.app/Contents/MacOS/pioneer; direct child Contents/Helpers/cua-driver | macOS 13 build configuration; stable Pioneer Developer ID app/helper signatures, notarization/stapling; x86 ONNX dylibs in bootstrap and app; actual TCC chain/grants/capture/cursor unqualified |
| aarch64-apple-darwin | Same Gateway app and LaunchAgent, native ARM daemon | Same stable identity/signing and graphical login requirements; no pre-login/root LaunchDaemon promise; real platform checks deferred |
| x86_64-unknown-linux-gnu | Existing systemd user service, direct bin/cua/cua-driver | GUI X11/Wayland and bus/display environment; X11/Xi/Xtst/Xext, Wayland/xkbcommon, PipeWire/libspa/portal build/runtime requirements; actual minimum libc/dynamic dependency audit deferred |
| aarch64-unknown-linux-gnu | Same user-service/owned daemon topology, native ARM build | Same GUI/portal requirements; no upstream glibc-2.31 compatibility claim for Pioneer Ubuntu-built outputs; native ARM qualification deferred |
| x86_64-pc-windows-msvc | Current-user ScheduledTask explicitly Interactive/Limited; bin/cua/cua-driver.exe and adjacent cua-driver-uia.exe | Logged-in interactive desktop; integrity/UIPI and secure-desktop limits; upstream static CRT target settings; UIA companion packaged, no default elevation |
| aarch64-pc-windows-msvc | Same interactive Limited task, native ARM daemon/UIA companion | Same session/integrity boundary; native ARM package, capture/input/cursor and helper behavior unqualified |

macOS verifies **observed** bundle identity (`parent_application`, real Gateway PID, ai.pioneer.gateway) via upstream health_report and read-only host-attributed Accessibility/Screen Recording checks. Bundle labels, filenames and AssociatedBundleIdentifiers are not treated as proof of OS grants. Grant instructions name Pioneer Gateway.app on the Gateway machine; a raw development CLI can serve other Gateway functions but fails CUA readiness without the responsible application. Launchd/TCC attribution and future signature continuity still need real qualification.

Linux lingering does not create a graphical session. If the user service starts before GUI environment import, configure/restart it from that logged-in session. Wayland portal consent can recur per session; packaged optional wayland-helper sources do not imply privileged compositor-plugin installation. Windows cannot control a pre-login/session-0 desktop or higher-integrity/secure desktop by default. Missing capabilities return refusal, not mock success. Standard mode may also refuse residual boundaries such as existing logged-in browser profiles without supported host authorization; no unrestricted fallback is installed.

Monitorless Mac mini is a separate future platform qualification: there is no physical-monitor-count gate and no Desktop requirement. An available monitorless GUI/capture target can be used through upstream capabilities. No usable GUI/display/capture, absent grants and failed cursor readiness are distinct failures. No custom virtual-display driver was added and fully GUI-less operation is not promised.

## Packaging, installation, recovery and scope

Exactly six Gateway ZIP names replace the old default/computer-use variants and gzip selection. Standalone and Desktop-carried Gateway packages include a bootstrap executable, pinned local daemon, provenance manifest, MIT/third-party notices, platform companions and native libraries described above. No manually installed CUA, Python or Node automation runtime is required. The existing Unix installer bootstrap still uses Python for ZIP extraction; that is not a new CUA runtime dependency.

The release producer checks clean exact upstream HEAD, preserves its lockfile/relative dependencies/CRT configuration and hashes the final signed daemon. On macOS the manifest stays outside sealed app resources; nested helper/libraries/Gateway/app are signed and the app notarized/stapled. Signing credentials are required rather than inventing an ad-hoc production identity. Existing Pioneer symbol/module provenance is preserved; only variant producers/consumers and the signed packaged-Gateway receipt are adjusted. Runtime dependency/link/license inventories require actual generated-binary audit later.

Outer archive checksum verification remains. Extraction rejects unsafe/duplicate/symlink paths and bounds members/total payload; validates pin/target/hash/notices/platform layout before activation. Executable and sidecar stage/rollback together. macOS root ONNX dylibs stage with the sidecar, not over the running bootstrap. Rollback retains its backup for retries, and existing schema-may-have-advanced refusal precedes any restore. A retained cua.rollback blocks a new update until explicit recovery.

The existing `pioneer stop` uninstalls the persistent service definition, not installation files; the stopped installed Gateway remains usable. There is no pre-existing separate file-uninstall command to extend. Removing the installation directory also removes its colocated CUA sidecar; no global CUA installation/service is introduced. No uninstall action was executed.

The legacy xa11y/xcap engine, own coordinate/tree/screenshot/recovery/goal loop, monolithic tool/schema/examples, feature/configuration and legacy-only tests are deleted. Cargo.lock removal is deferred solely because the resolver is prohibited. Shared attachment/artifact/filesystem/security contracts remain. No GPUI screen changed; no new plugin/remote-host framework, renderer, storage/permission engine, virtual-display platform or unrelated release/symbol redesign was added. The shared proxy barrier/provenance and installer recovery edits are necessary crossed-owner fixes for this migration, with real consumers, rather than opportunistic refactors.

## Regression owners and static review limits

Tests were authored/updated, **never executed**. Source trace is not a passing test. The test-audit authoring rule was applied to observable owners; this user task explicitly authorizes necessary production migration edits outside test-only batches.

| Required scenario | Owner evidence / authored regression boundary |
| --- | --- |
| 1. Standalone without Desktop | Gateway constructor/injected backend and complete standalone package/service path; actual installation/GUI qualification deferred |
| 2. Mobile call after Desktop exit; reconnect independent | No client/connection owner in supervisor; common turn terminal release differs from transport disconnect; real multi-client qualification deferred |
| 3. Selected/remote Gateway computer | Local installed executable, local IPC and local ArtifactService at Gateway; no client machine endpoint or path substitution |
| 4. Startup/stop/cancel/no replay | Gateway queue overload/cancelled waiter and crashed-owner PID receipt regressions; SDK lifecycle/retirement source trace; real process/action race qualification deferred |
| 5. Original typed images end to end | Trusted producer byte/metadata admission test; existing agent retention regression extended for current parallel group/pinned media; provider group-order regression; CLI final-frame/ledger regression |
| 6. Direct/proxy grants/media/error/admission | Existing exact-payload permission regression migrated; two-proxy owner regression covers ComputerUse approval, typed media/capture metadata, unknown outcome, content-free technical output and lease cleanup; runtime queued-writer matrix includes Exclusive→Shared and rejected Shared→Exclusive |
| 7. Honest missing OS capability; monitorless | Real identity/grants/cursor feedback/refusal paths; no fake GUI success; real 3-OS and monitorless qualification expressly deferred |

Initial implementation self-review traversed the product contract first, then producer/consumer call sites, security snapshots, runtime/session lifecycle, retained media, CLI frame boundaries, installers and all changed/new/deleted file groups below. It found and corrected undelivered-observation reuse, old native snapshot registration, untrusted JSON attachment inference, two-proxy technical-tree projection, CLI cache/drop and finalization ownership gaps, cleanup response uncertainty, prior-process restart overlap, install payload/ONNX rollback mismatch and remaining variant documentation. Compilation/API/borrow/link/OS correctness is still unverified by execution.

## Attached work: final resolution and infrastructure observations

All delegated instructions carried the original Gateway-only contract, worktree/scope limits and absolute execution ban. No scheduled or detached work was created. Local contributions and crossed-owner changes were inspected directly; the following control states must not be confused with external acceptance:

| Task / run | Final confirmed state |
| --- | --- |
| mMbRRrA499XQyKymQoT00 / vjnokz1y4YKPLOQTrUPxO | Exact candidate trc_vjnokz1y4YKPLOQTrUPxO_vwJEiAOD5xfnhMiX6HPGc read and accepted as the initial packaging contribution; follow-up issues handled separately |
| lYuiFcKCzrY1b8sJjtwVe / kJsBstD8nW3vCLf5SBhaS | Earlier task_get/task_wait reported terminal completed/succeeded; initial four-file contribution inspected; its incomplete image producer prompted the follow-up below |
| RMZRshIG2q4QWkCoM4RS6 / 9afz1l4IHiBYdmI6rxg5r | Final successful task_get and task_list report completed/succeeded. Its earlier missing image-producer integration was supplied by the follow-up and parent corrections; complete local contribution reviewed |
| jTriH2CPc7gzp13kdW1OF / HeqZUmVPfQf30JVO0Zgmd | Final successful task_get and task_list report completed/succeeded. Full local producer contribution and report inspected, then corrected by parent |
| UELYiodxsGsEC3tZChTsP / SL8KbUvFpYmQmHVu14bfH | Final successful task_list reports completed. Full local packaging-consumer contribution/report inspected, with crossed-owner integration reviewed by parent |

Many task_get/task_wait calls returned MCP -32002: MCP turn is not active, kind session_binding_unavailable. This initially prevented confirmation, and the blocker was reported rather than inventing candidate IDs or acceptance. Later successful task_get observations and **task_list(parentTaskId=NDXB5WV03YNhqPNjfzWTs, limit=20), truncated=false** returned exactly the five tasks above, all completed. Thus none of the attached tasks created by this execution remains pending/running/waiting_review. No scheduled/detached work was created. The request to restore the MCP binding became unnecessary once this conclusive read succeeded; task_wait itself is not represented as repaired.

The first packaging candidate has an explicit successful task_accept record. For the other terminal contributions, no additional task_accept success or unseen candidate content is fabricated: their returned terminal summaries, full local reports, actual source diffs and parent integration were reviewed directly. The initial image/tools omissions were not accepted as full migration completion; they were implemented and corrected in the final diff. This internal review does not replace the original external static review or alter its execution ban. No direct database/host workaround was used.

## Initial implementation commands and deferred checks

Initial implementation only (before round 1; no staging repeated this round): read-only pwd/filesystem existence/AGENTS/skills/source inspection; rg/rg --files/cat/sed/wc; git status/branch/rev-parse/worktree list/var GIT_AUTHOR_IDENT/var GIT_COMMITTER_IDENT/diff (including names/stat/numstat)/ls-files; git add -A only in this worktree and git diff --cached --binary export; creation of this branch/worktree from committed main; immutable GitHub API/raw/codeload source reads and archive extraction; apply_patch and custom Python text editing/JSON source comparison; permitted cargo fmt on affected packages and cargo fmt -- --check; git diff --check. Subagents reported the same allowed classes of commands and no execution outside this ban. Early speculative source paths returned 404/not-found and were corrected to the real rust workspace and LICENSE.md. Large exploratory text output was bounded/re-read by narrower file queries.

Observed static JSON comparison: 17 native entries; no name absent from pinned source; no difference in retained name/description/input_schema fields. Omitted manifest output/platform/capability fields are a deliberate passive listing projection, not silently changed API. Formatting and whitespace checks are not typechecking, testing or SDK validation.

Deferred until **full external static review and explicit original-reviewer authorization**: Cargo dependency resolution and lockfile review/update; cargo check/build/clippy, compilers/typecheck/linters; all tests/nextest/doctests/integration/E2E/smoke/harness; real SDK inventory, daemon and desktop actions; all six target build/link/native-dependency audits; real package contents/signing/notary/install/update/recovery/service-uninstall qualification; OS grants/responsible-process chain, IPC isolation, crash/cancel/action race qualification; native/provider/CLI/proxy multimodal history/replay and budget qualification; monitorless Mac mini. No such result is represented as passing.

## Per-file static coverage

The following inventory covers every changed, new and deleted path. Its conclusions are static source review, never executable validation.

| Path | State | Static review coverage |
| --- | --- | --- |
| `.github/workflows/build-debug-symbols.yml` | M | Variant-free symbol-input names and expected producer matrix; unrelated symbol logic retained. |
| `.github/workflows/release.yml` | M | Six ZIP producers/consumers and publication count; native build prerequisites; stable macOS Gateway signing/notary configuration. Never executed. |
| `.github/workflows/smoke-install.yml` | M | Existing installer fixture sites now construct complete ZIP payloads; fixture does not establish CUA readiness. |
| `.gitignore` | M | Ignore local read-only source/coordination scratch under .cua-source; worktree isolation retained. |
| `Cargo.toml` | M | Remove only workspace xa11y/xcap declarations. Lockfile resolution deferred. |
| `README.md` | M | Complete-archive installation and real Gateway permission/session ownership; no runtime-success claim. |
| `config/default.toml` | M | Remove old engine tuning; retain capture-byte bound; Gateway app identity defaults. |
| `crates/agent/src/chat/mod.rs` | M | Original typed media through bounded/history path; current result group and user media protected; delete old screenshot/session guessing; adapt existing retention test. |
| `crates/agent/src/chat/tooling.rs` | M | Legacy-only failure fixture uses canonical CUA operation; shared timeline behavior retained. |
| `crates/agent/src/manager_tests.rs` | M | Canonical visibility fixtures and remove deleted-feature probing/duplicate fake extensions; tests unrun. |
| `crates/artifacts/src/provider_resolver.rs` | M | Authorized stored CUA actor resolves immutable original blob, preserving existing fallback and ArtifactService lifecycle. |
| `crates/artifacts/src/registration.rs` | M | Remove obsolete ComputerUseSnapshot registration source; shared artifact API retained. |
| `crates/cli/Cargo.toml` | M | Remove deleted feature forwarding, retaining unrelated dependencies/features. |
| `crates/cli/src/installer.rs` | M | Complete ZIP identity/target/hash validation, bounded safe extraction, paired binary/sidecar activation and retained rollback; ONNX bootstrap libraries; schema refusal preserved; owner regression unrun. |
| `crates/cli/src/service/macos.rs` | M | Existing gui/uid LaunchAgent launches signed Gateway app executable; remove former cosmetic symlink; development fallback cannot establish CUA readiness. |
| `crates/cli/src/service/windows.rs` | M | Current-user task principal explicitly Interactive/Limited; no elevation or session-0 service. |
| `crates/config/src/lib.rs` | M | Delete only obsolete backend fields/defaults and legacy assertions; retain max_total_bytes. |
| `crates/gateway/Cargo.toml` | M | Exact Git SDK pin, Windows query-only PID API dependency, remove legacy feature. Dependency graph unresolved. |
| `crates/gateway/src/authorization/catalog.rs` | M | Central allowlist represented by real internal-tool catalog consumer; existing authorization owner inventory adapted. |
| `crates/gateway/src/cli_runtime/mcp/facade.rs` | M | Actual encoded frame bound/correlated noReplay error; existing ledger retained; owner regression unrun. |
| `crates/gateway/src/computer_use.rs` | A | Gateway-only actor/supervisor, identity/permissions/private IPC, bounded ownership, cancellation/no replay, real cursor cleanup, PID receipt; admission/liveness regressions unrun. |
| `crates/gateway/src/lib.rs` | M | Single startup-owned supervisor injection and joined shutdown, independent of clients; obsolete config removed. |
| `crates/gateway/src/mcp_service.rs` | M | Frozen native CUA dispatch, bounded per-turn runtime/session cache, real typed image conversion, delivery revocation and framed error diagnostics; terminal cleanup shared. |
| `crates/gateway/src/message/agent_runtime.rs` | M | Common committed terminal boundary revokes CUA even if retained proxy handles survive; no task/domain scheduler change. |
| `crates/gateway/src/message/artifact_registration.rs` | M | Delete old metadata/path-based snapshot inference and its legacy-only tests; shared registration remains. |
| `crates/gateway/src/message/artifact_tools.rs` | M | Weak Gateway image sink; trusted turn location, consistent authorization generation, original bytes, safe hashed identity, exact artifact version. |
| `crates/gateway/src/message/mod.rs` | M | Shared admission slot binding and backend config getter; prune obsolete test initializer field. |
| `crates/gateway/src/message/task_agent_executor.rs` | M | Only obsolete computer-use initializer field removed in test fixture. |
| `crates/gateway/src/message/tests.rs` | M | Existing native CLI catalog count includes actual CUA operations; no new source-grep proof. |
| `crates/gateway/src/message/turn_handlers.rs` | M | Canonical passive catalog filtered by existing authority, ComputerUse policy and positive/negative tool restrictions. |
| `crates/gateway/src/turn_mcp/native_tools.rs` | M | Pinned CUA family identity/specs/schema drift verification and existing owner-matrix extension. |
| `crates/promt/src/render/turn_preflight.rs` | M | Canonical CUA selection instructions and updated existing text expectation; no old monolithic tool guidance. |
| `crates/promt/tests/compile_snapshots.rs` | M | Two explicit existing prompt snapshots updated for canonical domain list; no generator/test execution. |
| `crates/protocol/src/lib.rs` | M | Export central allowlist; formatter import wrapping only. |
| `crates/protocol/src/turn.rs` | M | One exact 25-name allowlist and existing computer-use output-policy mapping; shared policy structures retained. |
| `crates/provider/src/attachments/mod.rs` | M | Typed tool media projected after complete result group before preparation/indexing; canonical history unchanged; native nested formats retained; owner test unrun. |
| `crates/provider/src/providers/azure_openai.rs` | M | Only old tool-name fixtures/expected names migrated; existing adapter media conversion retained; tests unrun. |
| `crates/provider/src/providers/chat_completions_api_compatible.rs` | M | Only old tool-name fixtures/expected names migrated; existing adapter media conversion retained; tests unrun. |
| `crates/provider/src/providers/openai.rs` | M | Only old tool-name fixtures/expected names migrated; existing adapter media conversion retained; tests unrun. |
| `crates/provider/src/providers/openrouter.rs` | M | Only old tool-name fixtures/expected names migrated; existing adapter media conversion retained; tests unrun. |
| `crates/tools/Cargo.toml` | M | Remove xa11y/xcap/image engine-only dependencies and obsolete feature; shared provider attachment dependency retained. |
| `crates/tools/src/apply_patch/file_mutation/mod.rs` | M | Remove unused legacy-only re-export; shared secure filesystem implementation unchanged. |
| `crates/tools/src/classifier.rs` | M | Canonical effects/refusals/unknown and trusted proxy metadata; no automatic replay or legacy recovery engine. |
| `crates/tools/src/context.rs` | M | Typed media, delegated outcome/provenance through actual result/history; external CUA-like JSON trust regression unrun. |
| `crates/tools/src/domain.rs` | M | ComputerUse domain aliases exact central CUA list; remove deleted-feature/monolithic inventory tests. |
| `crates/tools/src/handlers/computer_use/LICENSE.cua` | A | Vendored MIT attribution, no optional perception implementation. |
| `crates/tools/src/handlers/computer_use/actions.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/backend/mock.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/backend/mod.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/backend/screenshots.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/backend/xa11y.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/browser_catalog.rs` | A | Selected browser schema expressions from exact pinned source, no administration inventory. |
| `crates/tools/src/handlers/computer_use/catalog.rs` | A | Passive exact allowlist projection, host-owned session removed, recovery Never; no daemon/inventory execution. |
| `crates/tools/src/handlers/computer_use/handler.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/images.rs` | A | Bounded original-byte decoding/admission, unchanged capture metadata, typed provenance/media; producer regression unrun. |
| `crates/tools/src/handlers/computer_use/mod.rs` | M | No process/IPC ownership; injected backend, turn lease, undelivered-result guard and trusted producer. |
| `crates/tools/src/handlers/computer_use/model.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/native-contract.json` | A | 17 retained name/description/input_schema entries match immutable manifest; omitted metadata documented above. |
| `crates/tools/src/handlers/computer_use/permissions.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/platform.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/runtime.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/runtime/action.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/runtime/lifecycle.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/runtime/snapshot.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/runtime/verify.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/state.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/targets.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/actions.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/active_app.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/app_identity.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/artifacts.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/fallback_suggestions.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/flow.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/input.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/integration_smoke.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/mod.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/preflight.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/real_desktop.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/recovery.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/regressions.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/session_target_policy.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/snapshot_bound_targets.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/structured_action_trace.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/target_resolution.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/tree.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tests/verify.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/tree.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/computer_use/util.rs` | D | Deleted legacy-only backend/loop/target/tree/screenshot or its test module. Module wiring and active callers were removed; shared artifact/filesystem helpers retained. |
| `crates/tools/src/handlers/mod.rs` | M | Unconditional Gateway CUA bridge wiring; remove legacy feature gating/export. |
| `crates/tools/src/handlers/request_tools.rs` | M | Existing domain availability regression uses new canonical tool set. |
| `crates/tools/src/handlers/skill.rs` | M | Real nested media/outcome/provenance/receipt forwarding; blanket proxy recovery override removed in G06; no external JSON trust. |
| `crates/tools/src/lib.rs` | M | Injectable shared backend/admission slot and per-turn materialization; remove engine config; existing proxy owner regression extended for typed media/unknown/grants/technical output/cleanup. |
| `crates/tools/src/orchestrator.rs` | M | Prefer typed delegated outcome; exact complete-argument ComputerUse approval test migrated, never run. |
| `crates/tools/src/output_policy.rs` | M | Canonical CUA policy lookup; existing attachment inference confined to native artifact/HTML producers; typed proxy media preserves real consumers. |
| `crates/tools/src/output_projection.rs` | M | Delete old coordinate/tree compactors; enforce content-free CUA technical display/storage/recovery through typed proxy chain. |
| `crates/tools/src/permissions/mod.rs` | M | Every allowlisted CUA operation uses existing ComputerUse action with complete payload hash; old semantic/launch classifiers removed; permission regression adapted. |
| `crates/tools/src/process_policy.rs` | M | Remove unused old native-launch environment plan and its tests; shared process sandbox/environment contracts unchanged. |
| `crates/tools/src/router.rs` | M | Delete numeric session-key routing; bounded configured exact CUA resolution drives no-replay recovery and Err classification, preserving unrelated proxies. |
| `crates/tools/src/runtime.rs` | M | Runtime-bound inherited admission prevents nested queued-writer deadlock, rejects shared-to-exclusive upgrade; CUA/function-proxy technical argument redaction and typed CUA projection; owner matrix unrun. |
| `crates/tools/src/spec.rs` | M | Delete old monolithic schema/actions/coordinate/examples/recovery tests; shared specs retained. |
| `crates/tools/src/visibility.rs` | M | Existing visibility fixtures use canonical observation name. |
| `docs/reviews/cua-gateway-migration.md` | A | Single current contract/evidence/status/coverage/deferred-check handoff; no acceptance claim. |
| `install.ps1` | M | Single ZIP asset/bootstrap name; obsolete variant switch/env removed; exact root executable extraction. |
| `install.sh` | M | Single complete ZIP, obsolete variant flag/env/help/gzip path removed; only top-level bootstrap/dylibs extracted. |
| `resources/skills/pioneer/runtime-diagnostics/references/symbols.md` | M | Only obsolete Gateway variant/artifact-name documentation aligned with producers. |
| `scripts/packaging/build-gateway.py` | M | Single Gateway build and full pinned sidecar packaging; existing symbol provenance retained. |
| `scripts/packaging/cua-driver.py` | A | Full pinned workspace producer, all six targets, native companions/notices, stable macOS signing/notary and final daemon digest; never executed. |
| `scripts/packaging/desktop/check-package-contents.sh` | M | Check actual nested Gateway daemon/manifest and Windows companion in Desktop-carried ZIPs. |
| `scripts/packaging/desktop/package-linux-appimage.sh` | M | Bundle same standalone CUA Gateway ZIP; obsolete feature/variant removed. |
| `scripts/packaging/desktop/package-macos.sh` | M | Bundle same separately responsible signed Gateway app payload; preserve bootstrap and existing Desktop package/signing flow. |
| `scripts/packaging/desktop/package-windows-msi.ps1` | M | Bundle same Gateway daemon/UIA ZIP; no client execution transport. |
| `scripts/packaging/release-inputs.py` | M | Variant-free reuse receipt and exact revision/target validation; unrelated module identity checks retained. |
| `scripts/packaging/smoke-gateway-asset.py` | A | Test-only complete installer layout fixture with explicitly non-ready daemon; no fake GUI/grants success. |
| `scripts/packaging/symbol-inputs.py` | M | Remove only obsolete feature-variant receipt requirement; preserve provenance validation. |

## Round 1 — G01–G11 closure and source evidence

The external audit is `../cua-gateway-review-round1/AUDIT.md` with its scoped reports/COVERAGE. Those materials were read, never edited. Product-contract review came first: Gateway still owns the sole daemon/supervisor; no Desktop execution, registry, host transport, fallback or second engine was added. Scope remains the original migration. The following are implemented source corrections, **not passing tests or external acceptance**.

| ID | Correction and actual owner/callers | Static evidence / authored regression (NOT RUN) |
| --- | --- | --- |
| G01 | `tools/router.rs`, `runtime.rs`, `classifier.rs`, `handlers/skill.rs`; `agent/chat/mod.rs` Err consumers | `computer_use_leaf` resolves only actual configured registered exact leaves through ≤2 FunctionProxy edges, with cycle/depth bounds. Router classification is used for runtime and model/history errors; private dispatch scope preserves orchestrator Err classification without overwriting permission origin. Recovery and outer cancellation grace follow the same path. Existing two-proxy owner now exercises backend ExecutionFailed and Cancelled through one/two wrappers, checks every runtime failure outcome, router/model classifier, actual input count, and unrelated read_file proxy recoverability. Initial child revision put the private scope in classify_result; parent caught this and accepted only the corrected classify_error candidate. |
| G02 | `gateway/computer_use.rs`; `tools/context.rs`, `handlers/computer_use/{mod,images}.rs`, `runtime.rs`, `handlers/skill.rs`, `lib.rs` | Capture sets host pending generation/nonce, never observed. One per-turn gate stays owned by a nonserialized typed receipt through sink admission and outer runtime projection/durable publication. Mutation cannot enter backend before acknowledgement. Drop/error/cancellation revokes; concurrent other-owner input/cleanup clears pending nonce so late acknowledgement fails. Forwarded output retains the real receipt; nested runtimes do not commit it. Post-publication cancellation handling is restricted to outputs carrying this receipt, preserving unrelated tools. Existing transport fixtures extended with delayed rejected sink plus concurrent Shared mutation (zero input calls), and real durable completion rejection/ack-drop/cancel/caller-drop (zero acknowledgements and subsequent input requires fresh observation). No OS readiness/grant success is mocked. |
| G03 | `.github/workflows/smoke-install.yml` | Only libxi-dev and libxtst-dev added to the three Linux pre-CLI-build prerequisite blocks, satisfying pinned x11 xi≥1.7/xtst≥1.2 development dependencies. Ordinary CI not changed. Smoke/build not run. |
| G04 | New `crates/gateway/build_support.rs`, actual consumers `crates/{gateway,cli}/build.rs` | Mirrors pinned daemon final-binary SDK/framework/Swift discovery via SDKROOT/xcrun/xcode-select and existing toolchain directories, including /usr/lib/swift rpath. CLI and standalone Gateway main are actual SDK-linked final binaries. Desktop/client manifests have no Gateway/SDK dependency (client explicitly forbids Gateway); Desktop packaging separately builds CLI, so no unnecessary Desktop build.rs change. Commands embedded in build scripts have not been executed. |
| G05 | `tools/lib.rs` existing two-proxy fixture | Removed invalid delivery_mode only from strict type_text proxy fixture. Valid text/window target, typed media/geometry, no-replay, full permission and cleanup assertions retained. Production schema/validator unchanged. The independent orchestrator-only payload test deliberately bypasses router schema and still tests complete-argument authorization. |
| G06 | `tools/router.rs`, `runtime.rs`, `classifier.rs`, `handlers/skill.rs` | Same CUA-only resolution scopes recovery, fatal classification, grace and barrier inheritance. Inheritance checks lock Arc plus immediate configured edge/origin and target; unrelated dispatch clears ambient scope. Existing queued-writer matrix extended with unrelated negative edge (leaf never executes), preserving CUA Shared/Exclusive compositions and depth/cycle guards. No general scheduling/recovery redesign. |
| G07 | `tools/runtime.rs` | Shared→Exclusive refusal is an Err value in the existing result arm, reaching emit_failed_with_outcome and finish_trace. Existing rejection matrix now checks started/terminal event balance; queued writer completion also witnesses barrier resource release. No new lifecycle engine/hooks. |
| G08 | `gateway/mcp_service.rs`, `cli_runtime/mcp/facade.rs` | CLI uses deferred runtime delivery and passes typed CuaCliDeliveryGuard to one task-local Option owned by the request future. Only actual encode_response fit against actual max_frame_bytes commits. Errors/drop revoke without name/JSON trust or larger limit. Existing facade owner fixture uses a 256-control-character ID and near-bound result: correlated bounded response_frame_too_large/noReplay, one execution per ID, uncommitted receipt revoked, new action denied until fresh fitting observation. Transport fixture tests publication authority, not GUI success. |
| G09 | `gateway/mcp_service.rs` actual cache/terminal consumers | Existing cache mutex serializes cancellation+remove with cancellation/stopping-aware admission. Both insert-before-cleanup and registered-before-cleanup/late-materialization orders close; cancelled materializer is unreachable. Post-registration authoritative CUA turn re-read closes registration-after-committed-terminal window; DB await is outside cache/active locks. Existing service fixture exercises both orders over 32 finished turns and admits a new live turn, with no tombstones/unbounded registry. |
| G10 | `.cargo/config.toml`, `desktop/package-macos.sh`, `README.md`; existing inner app/upstream config | Pioneer compile/link environment forces MACOSX_DEPLOYMENT_TARGET=13.0; inner Gateway.app, outer Pioneer.app and short installation documentation agree. No feature-name-based raise to 15 and no change to other OS policy. |
| G11 | `.github/workflows/build-debug-symbols.yml` | Consumer again parses old optional -default/-computer-use suffix separately from target. Chosen trusted run must contain one complete historical or variant-free inventory, rejecting mixed/duplicate generations. Source/workflow/run/commit/catalog/hash trust checks remain. Producers stay variant-free; no backend/feature/package fallback returns. |

The narrow crossed-owner changes are required by actual consumers: ToolDeliveryReceipt connects the existing trusted producer, nested runtime publication and CLI final framing; it is local and nonpersistent, not a new transport protocol. The shared macOS helper has two real SDK-linked final consumers. The small cache-admission helper is used by production and its existing service owner regression, not a test-only production seam. No scope expansion beyond these concrete blockers was necessary.

## Round 1 — additional per-file self-review coverage

The original 114-path inventory above remains applicable. All original changed/new paths and deletion-only wiring were re-reviewed together with round fixes; shared artifact/filesystem/security owners and committed base behavior were preserved. No active xa11y/xcap/old schema/coordinate/registration references were found by bounded source searches (Cargo.lock intentionally excluded). Pin/catalog/session/cursor authority and all six package producer/consumer paths were traced. This source inspection cannot establish borrow/API/link/runtime/platform correctness.

| Additional path absent from initial 114-path index | State | Review coverage |
| --- | --- | --- |
| `.cargo/config.toml` | unstaged M | G10 minimum applies to normal workspace Cargo routes; other environment/flags unchanged. |
| `crates/cli/build.rs` | unstaged M | G04 final CLI/link inputs shared helper plus existing provenance manifest. |
| `crates/gateway/build.rs` | unstaged M | G04 standalone Gateway final executable receives same inputs; existing skill generation preserved. |
| `crates/gateway/build_support.rs` | **untracked new** | Entire file read against exact pinned daemon build.rs; platform-gated supported SDK/toolchain discovery, required Swift/framework searches/rpaths, MIT source attribution. Must be included in any review/next staging. |

Round-touched original-index paths: `.github/workflows/{build-debug-symbols,smoke-install}.yml`, `README.md`, `crates/agent/src/chat/mod.rs`, `crates/gateway/src/{computer_use,mcp_service}.rs`, `crates/gateway/src/cli_runtime/mcp/facade.rs`, `crates/tools/src/{classifier,context,lib,router,runtime}.rs`, `crates/tools/src/handlers/computer_use/{mod,images}.rs`, `crates/tools/src/handlers/skill.rs`, `scripts/packaging/desktop/package-macos.sh`, and this handoff. Their complete staged+unstaged source and connected callers were reviewed, not only individual hunks. Deleted legacy paths stay deleted. No production hook without a consumer, duplicate harness or weakened schema/assertion was introduced. All written regression outcomes remain unknown until authorized execution.

## Round 1 — attached work and permitted observations

All child instructions carried the original Gateway ownership, strict minimal scope, unchanged pin/index/Cargo.lock and execution ban. No scheduled/detached work or descendant subtasks were created.

| Task / run | Exact reviewed candidate / resolution |
| --- | --- |
| `uVAWHxcahfzznKNMuaXph` / `VcvJ31V1lUODZmNtlSJMB` | `trc_VcvJ31V1lUODZmNtlSJMB_wAzTZHUwdv9h6eoyHyZcc`, read fully with scoped source/scratch, task_accept succeeded; completed. |
| `nW5rh4aS2JLhCqKbdJCNA` / `zXqtve6tVtBew77l8P5qe` | `trc_zXqtve6tVtBew77l8P5qe_3Up1xMy05gjavWQHwKG4i`, read fully with actual facade/cache callers, task_accept succeeded; completed. |
| `rpkmMJf8RpJMQOFUlJmhh` / `hwcuFXDjqKvuEcQm6riU9` | Initial candidate revised for misplaced Err scope; exact revision `trc_hwcuFXDjqKvuEcQm6riU9_revise_trc_hwcuFXDjqKvuEcQm6riU9_dGxLFCJVvvYhVKHsoVGtE_1` read via task_result and source, task_accept succeeded; completed. Parent integrated G02 regressions and narrowed receipt cancellation after acceptance. |

A transient task_result session_binding_unavailable occurred on recovery; successful subsequent task_get/task_wait restored observation. No duplicate tasks were created; exact candidates were then read/handled. The final acceptance returned finalAnswerAllowed=true and taskTerminal=true; each of the other two accept receipts also confirms completed/succeeded. Later task_list/task_wait observations again returned session_binding_unavailable; no new active work was created and those read failures do not undo the three explicit terminal acceptance receipts. Internal child acceptance does not accept the migration externally or lift the execution ban.

Commands actually used this round: read-only pwd/ls/cat/sed/head/tail/rg/rg --files; Git status/branch/rev-parse/var/diff/stat/numstat/ls-files; one git write-tree recording the unchanged staged tree `30f9c7b472ad6b8936bb11acc2768732d29e0e33`; binary index patch export and shasum -a 256; Python stdlib pathlib text editing/reporting only; cargo fmt on pioneer-tools/agent/gateway/cli and corresponding --check; git diff --check and git diff --cached --check. No git add was run this round. No tests/compiler/resolver/project scripts/SDK/daemon/platform probes were run. Early broad source output was narrowed to exclude giant non-code fixtures; formatting is not executable verification.

Initial reviewed index: **114 staged paths**, binary SHA256 **dafc1641f3d9bf61b814984297e4ee7a96b6655e6896a4b10d963a18014f5a67**. It is preserved byte-for-byte as `.cua-source/round1-original-index.patch` and in the unchanged Git index. Round fixes remain unstaged; only new untracked source is `crates/gateway/build_support.rs`. Ignored coordination/source/patch files are local working artifacts, not additional handoff reports. Final observed status: **114 staged paths / 20 unstaged tracked paths / 1 untracked new file** (118 distinct changed/new paths including the original deletions). The original index and saved patch SHA256 both match the value above. `cargo fmt -p pioneer-tools -p pioneer-agent -p pioneer-gateway -p pioneer-cli --check` returned exit 0; both whitespace checks returned exit 0; Cargo.lock diff against HEAD is empty. A complete binary-capable current-tree patch (HEAD diff plus the untracked helper) is saved at `.cua-source/round1-current-tree.patch`; the original index patch remains separate and unchanged.

Base = HEAD remains `6c1e6b2c49359c76e49624c0545bbafc4a3d310a`; branch `feature/cua-gateway-migration`. Author/committer identity observed: `alexanderoskin <alexander.oskin@gmail.com>`. No commit/push or other worktree mutation. Cargo.lock has no staged/unstaged change; its graph remains unresolved under the explicit resolver ban.

Deferred until complete external static acceptance **and separate explicit original-reviewer permission**: dependency/lockfile resolution, all compilation/API/borrow/link and native library audits, all authored/retained tests, race/fault/cancellation/process qualification, smoke/install/update/recovery/signing/notary/package checks, actual three-OS/six-target capture/input/cursor/grants and responsible-process qualification, provider/CLI multimodal delivery/replay/budgets, and monitorless Mac mini. This round does not self-authorize any of that phase.

**Готово к повторному внешнему статическому ревью; компиляция и тесты не запускались.**

## Round 2 — residual G02 and G12

Entry matched the requested branch/base/HEAD and all hashes: index `dafc1641f3d9bf61b814984297e4ee7a96b6655e6896a4b10d963a18014f5a67`; tracked HEAD→current `d2ec74363e6e03bc65fb9148c0fcbdc607f1ff4bb5c2be3bac00593ce38f00f8`; full snapshot with helper `d116dd6ba018db02e9f9ba75b91c23dce87575f5a3a67aa08ea38780f344f25e`. Entry was 114 staged / 20 unstaged tracked / 1 untracked, 118 distinct paths including 37 deletions. Entry index, unstaged patch, full current-tree patch and helper were copied to ignored `.cua-source/round2-entry-*` snapshots before edits. No index staging, reset or other worktree mutation occurred. Reviewer `../cua-gateway-review-round2/{AUDIT.md,scoped reports,COVERAGE.md,snapshot.json}` was read only. The earlier round-1 G02 closure claim was incomplete: this round fixes the precise later native boundary found by external review.

| ID | Files and narrow source correction | Static evidence and regression owner (NOT RUN) |
| --- | --- | --- |
| G02 | `crates/agent/src/chat/mod.rs`, test-only `crates/agent/src/manager_tests.rs` | Exact registered CUA leaf/configured bounded proxy paths use the existing deferred runtime method. A private nonserialized NativeToolDelivery carries the real receipt and attempt cancellation with ExecutedToolResult across task return/join, exact TurnLlmContextAppended ACK and bounded provider-message preparation. Early runtime CallCompleted→ItemCompleted ACK does not commit it. Preparation failure/cancel/drop releases the real receipt and revokes authority; attempt cancellation stays registered until delivery resolves. Commit occurs inside each concurrent result future before buffer_unordered batch collection, so a capture can release an input waiting on its gate without gather-before-commit deadlock. Unrelated native dispatch keeps its previous runtime method and preparation; CLI keeps its separate actual final-frame boundary. Existing agent parallel durable-result owner is extended through the real CUA handler/image admission/receipt, rather than manually constructing receipts or ACKing only ItemCompleted. Early tools fault cases and configured one/two-proxy/no-replay owner tests stay intact. |
| G12 | `crates/cli/src/installer.rs` | macOS prepare_candidate_probe_layout creates a private TempDir under install bin, copies candidate cua.next/bootstrap-libs beside the exact staged executable and moves that executable into the isolated layout preserving permissions. Existing version/schema command probes this same staged_binary; activation renames this same file after the existing migration guard. TempDir lives through the install/recovery scope and drops on refusal/error. Live root libraries, binary and payload are unchanged before admission. Existing combined activation/rollback remains; Linux/Windows paths are unchanged. Existing installer rollback owner is extended with clean/no-prior-libs and differing prior/candidate/shared-name libraries; the authored command fixture observes actual version --json adjacent files and schema rejection, then coherent activation and library/payload recovery. It is not proof of native dyld loading. |

Product-contract self-review preceded technical review: the sole local daemon, readiness/private IPC, session/cursor/cancel/shutdown remain Gateway-owned; Desktop/mobile/CLI remain clients. No new transport, registry, persistence, scheduler, renderer, engine, goal/retry/coordinate loop or global DYLD environment was added. G02 changes the local result owner only; G12 uses the existing installer staging and activation boundaries. These fixes need no scope expansion. Original media bytes, geometry/tokens, authorized artifact paths/quotas/retention, per-call grants and no-replay classification remain in their existing owners. No technical event was expanded to contain screen payload.

Round-2 full-tree self-review uses the external 118-path inventory above plus complete current delta/callers. A byte/hash comparison of all snapshot paths (including null entries for deletions) confirmed that only agent chat, its existing manager test owner, installer and this handoff differ from the externally reviewed entry; the other paths and all 37 legacy deletions remain unchanged, including the untracked build-support helper. The connected native runtime/receipt/proxy, exact event publisher/forwarder, per-result join/cancellation, CLI final-frame and candidate preflight/activation/rollback paths were read. Prior G01/G03–G11 corrections are preserved, not reimplemented. Source inspection is not compilation, test execution or platform qualification.

Round-2 permitted commands so far: pwd/ls/cat/sed/rg/rg --files source/AGENTS/skill/audit reads; git status/branch/rev-parse/diff/numstat/hash observations; shasum -a 256; cp local entry snapshots; Python stdlib pathlib/hashlib/json/difflib text editing/source snapshot comparison only; cargo fmt and --check for pioneer-agent and pioneer-cli; git diff --check and git diff --cached --check. An initial agent fmt --check reported only a newly inserted test-field indentation difference; final formatting is repeated after the child finishes. No tests, project scripts, resolver/compiler, SDK/daemon/UI actions or platform probes were executed. The shell/version fixture is authored test code and was never run.

No new source file was introduced this round. Existing untracked `crates/gateway/build_support.rs` remains part of the full reviewable diff and is unchanged. Cargo.lock and CUA pin/workspace version remain unchanged. Native Intel dyld smoke, exact-history races/provider delivery/regressions and all previously deferred compilation/API/borrow/link/platform/package/install checks require full external acceptance and a separate explicit original-reviewer execution authorization.

Round-2 written regressions (never executed): the existing `completed_parallel_tool_result_is_journaled_before_the_batch_finishes` retains its unrelated fast/slow proof and adds the real CUA matrix: success, exact-history rejection, history receiver drop, native caller drop, turn cancellation with late successful ACK, and bounded provider-message rejection after successful exact ACK. Test-only broker revalidation waits for actual sink admission while the real handler holds its gate; sink waits for the parallel input permission request, both cancellably, so capture/input order is deterministic without fabricated receipt/observed state. Exact retained typed image/bytes, geometry/generation/token, private nonce stripping and next provider request media are asserted. Existing tools fault and configured one/two-proxy tests are unchanged. The existing installer rollback owner now observes candidate-local library contents through the authored version-command fixture, clean/upgrade refusal preserves prior bytes, success activates the same probed candidate, and existing library/payload rollback assertions remain. None of these assertions is a passing result.

| Round-2 attached task / run | Exact candidate and final resolution |
| --- | --- |
| `ryh4NJIdMXTWHPOZUdXx1` / `OqBa3oGpGzKfqn5CnQTjZ` | `trc_OqBa3oGpGzKfqn5CnQTjZ_bxoA4yZfhpVX8GGCx4Bc3` read via task_result plus full installer source/diff/report; task_accept succeeded, completed/succeeded. |
| `1U8Q49OIVeFgZGOU0YBqY` / `HAjUqothnRwdky49LsF5u` | Initial candidate revised for nondeterministic capture/input ordering. Exact revision `trc_HAjUqothnRwdky49LsF5u_revise_trc_HAjUqothnRwdky49LsF5u_P2lY0aTNjkq55iIkaRrjk_1` read via task_result with complete test source/broker callers; task_accept succeeded, completed/succeeded. |

All delegated instructions carried the original Gateway ownership, scope and absolute execution ban. No duplicate, scheduled, detached or descendant work was created. Both exact candidates were reviewed and accepted; final acceptance reports taskTerminal=true and finalAnswerAllowed=true. This is internal child integration, not external migration acceptance or execution permission.

Final round-2 source state: branch `feature/cua-gateway-migration`, base=HEAD `6c1e6b2c49359c76e49624c0545bbafc4a3d310a`; **114 staged / 22 unstaged tracked / 1 untracked source / 118 distinct paths**. Only four paths differ from the external round-2 snapshot: agent chat, agent manager tests, CLI installer and this handoff. All other 114 paths, all 37 deletions and the existing untracked helper are preserved. No new source files this round. Index binary patch and saved entry patch retain SHA256 `dafc1641f3d9bf61b814984297e4ee7a96b6655e6896a4b10d963a18014f5a67`. Cargo.lock equals HEAD (Git blob `6ca3a85049a18befee7f6e288cbbd912d4cae66c`); helper SHA256 remains `b5fc83e8f338b53433cbe0fbb997368ba55d856050efada614418a712dc6dbb0`. Author/committer observed `alexanderoskin <alexander.oskin@gmail.com>`; no commit/push/staging.

Final allowed observations: `cargo fmt -p pioneer-agent -p pioneer-cli --check`, `git diff --check`, `git diff --cached --check`; all returned exit 0 in the final separate observations. Final task_wait also returned both tasks completed/succeeded, 0 pending/reviewRequired/failed/blocked/cancelled. Full current-tree review patch, including the unchanged untracked helper, is `.cua-source/round2-current-tree.patch`; original index and entry round-1 snapshots remain separate. Source/JSON/hash comparison is scope accounting, not a regression test. Compilation, all tests, SDK/platform/runtime checks and native Intel dyld load smoke remain deferred.

**Готово к повторному внешнему статическому ревью; компиляция и тесты не запускались.**

## Publication and initial CI follow-up

The user accepted the implementation and authorized publication and concrete CI repairs, with local builds/tests still prohibited. PR: https://github.com/pioneerdotai/pioneer/pull/172. The English PR description went through humanizer and contains no testing-status section. Accepted implementation commit: `7e208b9912d3615392d83aeb25e7f92137711f2b`; original base: `6c1e6b2c49359c76e49624c0545bbafc4a3d310a`.

CI run `40059382645` failed dependency selection on macOS and Windows: the old lockfile held `zune-core 0.5.3`, conflicting with the pinned CUA core's exact `=0.5.1` compatibility constraint. The narrow non-build command `cargo update -p zune-core@0.5.3 --precise 0.5.1` resolved the graph, added the SDK/platform crates at the unchanged immutable revision and removed unused xa11y/xcap dependencies. No manifest pin, checksum, version entry or `--locked` consumer was manually weakened. This resolves the lockfile deferral in the historical static-review snapshots above; build/link/runtime qualification remains CI's job.

The repair changes only Cargo.lock and this handoff. Static inspection covered the exact CUA Git source/version entries, legacy-package removal and Pioneer workspace versions; `git diff --check` was run. Current origin/main was fetched and remained an ancestor of this branch; GitHub reported MERGEABLE. No local build, test, compiler, SDK/daemon or platform probe was run.

One recurring monitor, `1EuSrEXDwGwWI6UvduAfp`, owns PR CI follow-up every 15 minutes and sends results to the original thread. Its stored instructions include this worktree, immutable Gateway ownership, bounded scope, no local builds/tests, English conventional commit/title format, no CI-skip markers, pause during repair, main conflict checks and permanent stop after verified current-head GREEN or PR closure. Its schedule was paused before this repair and will be resumed only after push, with the next firing explicitly verified at +15 minutes.


## CI follow-up: macOS SDK selection

Run `40059658574` at `0f7ba629904950dee6d361bf4dc44005bd3d4ff9` passed Windows but failed macOS job `120770191669` in `apple-metal 0.8.8`'s Swift bridge: the selected SDK lacked `MTLResidencySet`, `MTLSamplerReductionMode` and other newer Metal APIs. The exact pinned CUA platform-macos manifest requires screencapturekit 8.0.1, whose nonoptional apple-metal dependency builds this bridge. Published apple-metal source documents its audited SDK as macOS 26.2; it has no feature to exclude the failing API families.

Narrow migration-required infrastructure exception: change only the existing macOS CI matrix runner from macos-14 to macos-15 and explicitly select its installed Xcode 26.2. Official runner inventory lists this Xcode and macOS 26.2 SDK on macos-15; macos-14 has no Xcode 26. CI's macOS check becomes `ci-macos-15-aarch64-apple-darwin`; Windows, Linux build, four shards and Linux aggregate remain expected. Branch protection API reported main unprotected and its branch rules endpoint returned an empty list. References: https://github.com/actions/runner-images/blob/main/images/macos/macos-15-arm64-Readme.md and https://github.com/doom-fish/apple-metal-rs.

This preserves native compilation, exact CUA revision, Cargo.lock and --locked. No runtime compatibility claim follows from choosing a newer build SDK. Release and smoke workflows are outside this observed failure and remain unchanged. Future monitor firings were verified paused before editing. Validation is static diff inspection and git diff --check only; local builds/tests, compilers and platform probes remain prohibited.


## Monitor follow-up: merge current main

Current main `fa0ec29bfd19f9ada8f66be9665d77f5e51e0095` introduced confirmed conflicts in agent native-result preparation and Gateway Windows dependencies. Future monitor firings were paused and verified before merging. The normal merge preserves CUA cancellation and durable delivery receipts while retaining main's complete thread-page projection inside that same preparation boundary. Gateway windows-sys features include both process management and main's network interface requirements. Other incoming changes were merged by Git and statically inspected. The immutable CUA revision and --locked consumers remain unchanged. No local builds, tests, compilers or platform probes were run; fresh full CI is required on the merge head.

## CI follow-up: hidden-domain catalog budget

Run `40074335763` at `6526d6e4` failed Linux shard 4 in `request_tools_hidden_domain_catalog_token_guard_stays_compact`: the complete catalog grew to 1,297 characters with the CUA allowlist, exceeding the existing 1,200-character budget. Shortened only the two introductory sentences, preserving the request_tools instruction, overlapping-domain visibility semantics and every domain/tool name. Source-text accounting gives 1,172 characters; this is static text analysis, not test execution. The existing budget and all assertions remain unchanged. Future monitor firings were verified paused before editing; no local builds/tests or platform probes were run. Full CI on the repair head remains required.


## Server/computer-use contract restoration — 2026-10-24

This section supersedes the historical decision to replace default/computer-use variants with six CU ZIPs. Removing the old engine did not authorize removing the server product. Source-review inputs were `../cua-server-contract-audit-20261024/AUDIT.md` and its Windows, macOS and matrix/Linux reports. Those historical audit files were read only and are not rewritten.

### State and review boundary

- Worktree: `/Users/alexander/Code/pioneer/pioneer/.worktrees/cua-gateway-migration`; branch `feature/cua-gateway-migration`.
- Original behavior baseline: `6c1e6b2c49359c76e49624c0545bbafc4a3d310a`; first migration: `7e208b9912d3615392d83aeb25e7f92137711f2b`.
- Entry and final committed HEAD: `747f60be0aa1718f8f294a924a6a39b9d9a80f39`. Entry status was clean. Restoration remains unstaged; no commit/push/staging/reset/rebase/merge was performed in this round. Existing main merge and CI/media/permission fixes are preserved.
- Last integrated main / merge-base with the observed local origin/main: `fa0ec29bfd19f9ada8f66be9665d77f5e51e0095`. Observed local tracking origin/main is `4bc0ba4fb220e9b4aad8f23b9215b96d4e4dd962`; it was not fetched or merged this round. No claim of current remote mergeability follows.
- The original CI monitor `1EuSrEXDwGwWI6UvduAfp` was verified paused with no active runs; its stale nextFireAt is not an active schedule. It was not resumed, cancelled or replaced.
- This is a reviewable source candidate, not external acceptance. Compilation, test results, actual native dependency closure, minimum OS versions, installation/update/recovery and platform/runtime compatibility remain unverified.

### Requirement → owner map (planned before edits)

| Requirement | Owner / changed area |
| --- | --- |
| CU-free default dependency graph; optional CU backend | CLI/Gateway/tools manifests; Gateway lifecycle/native MCP; shared build helper |
| Catalogs agree with available execution | tools registry/materializer and prompt renderer; existing tools/agent/prompt test owners |
| Twelve producers and matching consumers | Gateway packager, release and symbols workflows, release/symbol input helpers |
| Desktop explicitly consumes CU | Desktop packagers, package-content guard and Rust bundled/development launcher |
| Server/CU archive and recovery boundaries | CLI installer and macOS executable selection; existing installer regressions |
| Public selection and supported legacy CU migration | install.sh/install.ps1 and README |

All owners received the execution ban and the same six-target/two-variant matrix. Scope stayed within restoring this contract; no new protocol, SDK fork, engine, service, display virtualization, storage or scheduler was added.

### Final producer / consumer matrix

All assets have prefix `pioneer-gateway-`. Producer is `.github/workflows/release.yml:93` → `scripts/packaging/build-gateway.py:29`; selectors are `installer.rs:1023`, the root bootstrap scripts, and Desktop `launcher.rs:607`.

| Target | Default/server asset | Computer-use asset |
| --- | --- | --- |
| x86_64-unknown-linux-gnu | linux-x86_64.gz | linux-x86_64-computer-use.zip |
| aarch64-unknown-linux-gnu | linux-aarch64.gz | linux-aarch64-computer-use.zip |
| aarch64-apple-darwin | macos-aarch64.gz | macos-aarch64-computer-use.zip |
| x86_64-apple-darwin | macos-x86_64.zip | macos-x86_64-computer-use.zip |
| x86_64-pc-windows-msvc | windows-x86_64.zip | windows-x86_64-computer-use.zip |
| aarch64-pc-windows-msvc | windows-aarch64.zip | windows-aarch64-computer-use.zip |

The unsuffixed Windows and macOS Intel ZIPs contain a real server executable, not a CU executable under a compatible name. Server gzip remains a gzip-compressed executable, never a renamed ZIP. Existing Intel ONNX dylibs remain in both ZIP variants.

### Static evidence and coherent-block self-audit

1. **Dependency / lifecycle boundary.** `cli/Cargo.toml:11` forwards computer-use to Gateway; `gateway/Cargo.toml:12` enables the optional pinned SDK and tools feature. Tools consumers in agent/artifacts/memory/promt do not enable CU themselves. No other manifest owns the SDK. Gateway's module, supervisor construction, injected backend and shutdown are gated (`gateway/src/lib.rs:20,528,662,1090`). Default backend is None. Both existing final executable build-helper consumers (`cli/build.rs:6`, `gateway/build.rs:45`) reach a shared early CU-feature guard (`gateway/build_support.rs:9`) before any Swift/framework discovery. The workspace-wide forced deployment target is removed. Explicit CLI/Gateway CU builds, CI and Desktop development/packaging consumers opt into CU and its macOS deployment input. Cargo.lock and CUA pin `688c5fd62a9f13dddc309970c948df494e7de33f` are unchanged. No resolver was run; any later --locked resolution issue must be handled in a separately authorized phase, without invented entries.
2. **Catalog / authorization boundary.** Tools specs are empty without CU; the materializer also requires an injected backend. This closes the provider/direct registration leak under tools-only feature unification while Gateway's own CU feature is disabled. Native MCP identity/catalog and its per-turn CUA admission block (`gateway/src/message/turn_handlers.rs:3921`) are independently gated by Gateway's feature. The default prompt omits the CU domain; the known domain/permission vocabulary remains for existing classification and unavailable-domain diagnostics. Existing request_tools visibility and per-call authorization semantics remain. The supervisor body, media/image admission, original image bytes, durable delivery receipts, cursor, cancellation and no-replay implementation were not rewritten.
3. **Release / platform inputs.** build-gateway.py validates each target/variant/name before building. Only CU adds `--features computer-use` and invokes cua-driver.py; server takes the old gzip/standalone-ZIP branches. CUA signing/notarization setup is CU-only. Linux retains baseline prerequisites; newly added Xi/Xtst/X11 inputs are CU-only. CUA daemon portal features are not treated as the host SDK graph. There is no new Windows Service, macOS LaunchDaemon or Linux service mechanism. Windows AtLogOn/Limited and Linux systemd user service source is unchanged. macOS retains the existing GUI LaunchAgent; the server selects the standalone executable even if an old CUA app remains.
4. **Packaging / provenance.** Publication requires all 12 exact assets and checksums include both gzip and ZIP. Retained Gateway inventories carry target and variant. Desktop selects CU explicitly in packaging and its Rust runtime asset lookup. Reuse and symbol processing retain run/commit/target/catalog/hash/module checks and additionally verify variant against embedded build features. The symbols workflow still rejects mixed/duplicate/incomplete inventories and supports historical inventory generations. It never rebuilds release binaries to obtain symbols. Existing CU package signing, daemon/companions, manifest and pin validation remain intact.
5. **Install / recovery.** `installer.rs:1525` accepts bounded server gzip on its original targets; `:1557` preserves safe ZIP paths, duplicate/symlink and size guards. Server ZIPs need no CUA manifest/daemon/app and reject CUA payload entries; Intel ONNX remains required. CU keeps the complete-package checks (`:1715`) and repeat validation before activation. Server dispatch never calls CUA validation/activation. Clean server installs without companion libraries bypass companion replacement; Intel ONNX updates use the existing common recovery unit without requiring a driver/app/manifest. A CU→server transition retires the old managed app/daemon into the existing backup unit while activating server libraries; a server→CU transition installs the complete validated package. Candidate-local macOS libraries (`:1290`), schema preflight (`:352`) before service stop (`:408`), retained backup guards and the no-rollback-after-possible-schema-advancement rule (`:1184`) remain. No foreign-process actions were added. Source inspection does not demonstrate successful recovery.
6. **Three update paths.** Old server self-updaters keep all six original asset names/formats and receive a server binary. Root scripts run the downloaded executable as bootstrap after checksum verification, so they use the new payload-aware installer. New CLI local-archive installation uses the new extractor and its own compiled variant. Switching variants uses the bootstrap of the desired variant. Old CU Linux/macOS ARM updaters look for -computer-use.gz; old CU Windows/macOS Intel updaters can match the ZIP name but discard sidecars. Neither can install the new full package; README gives the supported explicit bootstrap migration using the script from the desired release tag's repository source and SHA256SUMS from that release (scripts are not assumed to be published release assets). No transparent binary-only CU migration is claimed.
7. **Scope preservation.** Production changes are limited to feature/build/catalog gates, packaging/selector/provenance, archive/companion transitions and the macOS selector. Agent changes are test-only. New supervisor/SDK transport, authorization and image/cancel/cursor bodies are unchanged. Historical reports and neighboring worktrees are unchanged. Prior CI Xcode/media/permission fixes and the shortened prompt introduction/budget remain; CI now explicitly includes CU for its existing CU regressions while retaining default compile checks.

### Authored regressions, never run

Applied `test-audit` to existing owners, with production/test editing separated and protected regions compared. Registry coverage now distinguishes build-feature and backend availability. Agent generic parallel journaling remains unconditional; its complete six-case CUA delivery matrix remains when CU is available. Existing visibility/prompt fixtures reflect the build and use a nonexecuting backend only where registration is required. Prompt coverage now owns the server/CU availability difference; duplicate exact-map assertions were consolidated within that affected owner, with the 1,200-character budget unchanged.

The installer owner covers target asset names, server gzip/ZIP admission and wrong-format/CU rejection, complete CU ZIP admission with missing manifest/wrong pin/hash/missing companion, actual activation/recovery and Intel library/app restoration. Existing schema uncertainty and retained-backup cases remain. The symbol-input owner adds wrong-variant rejection before extraction/native tooling. Desktop's existing command fixture uses the CU archive name. These are authored regressions and source traces, not passing results or native qualification.

### Commands and deferred execution

Performed only source/Git/skill/audit reads (`pwd`, `ls`, `cat`, `sed`, `rg`, `git show/status/diff/rev-parse/merge-base`), standard-library text editing/hash/snapshot scripts, `cargo fmt` / `cargo fmt --check`, `git diff --check` and read-only task inspection/review operations. No project Python code was imported or executed. Child formatter observations included transient differences while another owner was editing; the final check is recorded below. No Cargo metadata/fetch/resolver, compiler, tests, linter/typecheck, packaging/install/CI/dev scripts, SDK/daemon or OS probes were run or triggered. No commit or push was made.

Deferred until full external source review and separate explicit execution permission: locked server and CU compilation on all six native targets; default and CU installer/catalog/agent regressions (nextest with --test-threads 2 --no-fail-fast --retries 0); doctests separately; symbol-input regressions; package/signature/provenance checks; old-server update, explicit old-CU bootstrap, both variant transitions and recovery before/after potential schema advancement; real PE/Mach-O/ELF closure and minimum OS qualification; CU graphical-session/permission/cursor/media/cancellation readiness. Compilation or runtime compatibility is not inferred from Cargo manifests, newer SDKs, test source or child acceptance.

Review exports will be saved under ignored `.cua-source/`: `server-contract-restoration.patch` (HEAD → current tree), `server-contract-full-integrated-main.patch` (fa0ec29b → current tree, complete corrected migration), and `server-contract-full-original-base.patch` (6c1e6b2 → current tree, including the already committed main merge). They contain all tracked source changes; this round introduces no new source paths. They are source artifacts, not builds.


### Attached work resolution and final source state

| Task / run | Exact review resolution |
| --- | --- |
| Packaging `XOdwsVuE0D4BsnC4rgzEa` / `6rZeYuW7GFQx7xJZIdWSW` | Exact revision `trc_6rZeYuW7GFQx7xJZIdWSW_revise_trc_6rZeYuW7GFQx7xJZIdWSW_Xs8XzHVhnE6JFwepisYXn_1` read through task_result (SHA256 `ebca69e526e303a55450ec4bd212c5a8ceff955ab5537b9cee230bca974a359d`), independently compared with source, accepted; completed/succeeded. |
| Installer `wkyUYuZA2yTTOC9bNAi3s` / `Y4pWNwfMEF6lxpgZHWrkJ` | Exact revision `trc_Y4pWNwfMEF6lxpgZHWrkJ_revise_trc_Y4pWNwfMEF6lxpgZHWrkJ_zBqig0fRfDxTSvoVEW4Tj_1` read through task_result (SHA256 `c79e774eb58a259d04f3836d04dc3372f35d8e72f03058605726643eb15b40ad`), independently compared with source, accepted; completed/succeeded. |
| Compile/catalog `DfsaplxB7MmkqSt64F8EX` / `B7gkAWZdZc9j7uwUdGhFL` | Initial and first revised exact candidates were read, then revised for missing default/CU test-fixture boundaries. During the second revision, runtime terminated the task as cancelled with execution `conflict` (“The operation conflicts with current state.”); no final candidate was returned or accepted. Parent directly reviewed the final manifests/build gates/registry and test-only agent changes and completed prompt/Desktop/per-turn gates. The cancelled child's unaccepted report is not treated as final evidence. |

Final task_wait confirmed **3 terminal tasks, 0 pending and 0 review-required**; no child was detached or replaced. Runtime cancellation of the compile/catalog child is recorded rather than disguised as acceptance. Internal acceptance of the other two is not external source acceptance and does not authorize execution.

Final branch/HEAD remain `feature/cua-gateway-migration` / `747f60be0aa1718f8f294a924a6a39b9d9a80f39`: **33 unstaged tracked files, 0 staged files, 0 untracked source files**. The final `cargo fmt --all --check`, `git diff --check`, `git diff --cached --check` and `git diff --exit-code -- Cargo.lock` returned exit 0. The agent chat production prefix was compared with HEAD and is identical; its changes are confined to the existing test module. Protected parent test-phase prefixes remained identical to their snapshots. Complete final source diffs were read, including the final agent fixtures and the one-line per-turn native CU gate. No tests or compiled/runtime/platform checks were run. Full external review and separate explicit execution permission remain outstanding.


## Server contract: external review round 1 corrections (R01–R07, 2026-10-24)

This section records the source candidate correcting the seven findings in
`../cua-server-restoration-review-round1/AUDIT.md`. Earlier review records above
remain historical. **This candidate has not received external acceptance. Tests,
compilation, packaging and runtime checks remain prohibited until full external
source acceptance and separate explicit permission for execution.**

### Entry identity and preservation

Worktree remains `/Users/alexander/Code/pioneer/pioneer/.worktrees/cua-gateway-migration`,
branch `feature/cua-gateway-migration`, HEAD
`747f60be0aa1718f8f294a924a6a39b9d9a80f39`. Original base is
`6c1e6b2c49359c76e49624c0545bbafc4a3d310a`; integrated main is
`fa0ec29bfd19f9ada8f66be9665d77f5e51e0095`. Entry had 33 unstaged tracked files,
zero staged files and zero untracked source files, matching the reviewer snapshot.
No branch, worktree, merge, commit, push, reset, stash or monitor operation occurred.
The paused CI monitor was not changed. No subagents or task-backed work were created
or reviewed in this correction round; the earlier task resolutions above remain historical.

Entry copies and patches are retained under ignored
`.cua-source/server-review-r01-r07-entry/`. SHA256 at entry:

| Input | SHA256 |
| --- | --- |
| HEAD → entry restoration patch | `3c3d68104785da2f5feb9b3eff1a34ccb951e3d1413fe39a9ec4edce05cdfd0f` |
| Integrated main → entry patch | `21a4b7b5b04243cc46067b4d9c6c4238629ce75c862949291aed62948d75305f` |
| Original base → entry patch | `f8128fc325f591339dbcd8410bd1578620545ca2a87d1c1bd06339354d15bfa3` |
| Empty staged patch | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| Raw Git index bytes | `09a4be787e9a7cf1005dc4325e273816d7b7c1bfe9efca19620aab9be86a04ce` |
| Cargo.lock (equals HEAD) | `f76eb868626c41ddf0da43d9db28ed2bb918c0e55c6ed666ed0b30733617abe5` |

The raw index and Cargo.lock hashes remain identical. The CUA revision remains
`688c5fd62a9f13dddc309970c948df494e7de33f`; no manifest, dependency version,
checksum, lock entry or `--locked` policy was changed in this round.

### Requirement → owner → source resolution

All seven findings are addressed in source, pending external review and execution.

| Finding | Existing owners changed | Decision and static evidence |
| --- | --- | --- |
| R01 / P1 | `.github/workflows/release.yml`, `.github/workflows/smoke-install.yml` | CU standalone Gateway uses `macos-15` for aarch64 and native `macos-15-intel` for x86_64; server retains its existing host selection. Both Desktop build and the separate packaging job use these architecture-specific hosts through their shared matrix. Each of these jobs and macOS smoke explicitly selects Xcode 26.2 and exports DEVELOPER_DIR for subprocesses. Target values, archive names, ONNX companions, signing, catalog and symbol provenance paths are unchanged. |
| R02 / P2 | `crates/promt/src/render/request_tools.rs`, `crates/agent/src/chat/mod.rs` | Existing prompt functions receive a narrow `computer_use_available` argument. All six real native-plan callers supply the current router's registered `cua_get_window_state` presence for tool-enabled plans, or false for no-tool plans. The renderer additionally retains its tools-feature guard. Initial prompt, refreshed continuation and retry paths agree. No capability framework or registration change was added. |
| R03 / P2 | `crates/agent/src/manager_tests.rs` | Only the three reported visibility scenarios opt into a nonexecuting backend fixture. Their existing provider requests require CU when the tools feature registers it and require its absence in server builds. Selected/unselected memory/task/artifact checks and text-only versus image-capable provider behavior remain. The generic tool-loop fixture stays backend-less. |
| R04 / P2 | `crates/gateway/src/message/tests.rs` | Only the expected CUA binding contribution depends on Gateway's own `computer-use` feature. The persistence, ordering, complete binding count and native-start scenario remain unconditional. |
| R05 / P2 | `crates/promt/tests/compile_snapshots.rs` | Both existing continuation owners use the final short intro and exercise availability false/true. The manually maintained complete expectation retains all domain/tool names. Exact full-string equality replaces the inline insta assertion to express the two expected variants without duplicating both large literals. Only the expected CU line is omitted for unavailable builds/runtimes; actual output is never filtered or normalized. A literal-format trailing newline is removed from the expectation only. No snapshot generator/auto-accept was run. |
| R06 / P2 | `.github/workflows/ci.yml` | Adds one Linux default/server nextest invocation before the existing CU archive. Explicit packages are CLI/Gateway/tools/promt/agent, `--no-default-features`, no CU feature arguments. Filters select existing installer tests, registry availability, request_tools/continuation prompts, the three agent visibility scenarios/native prompt owner, and Gateway capability persistence. `--locked --test-threads 2 --no-fail-fast --retries 0` retained. CU archive, four shards and doctests remain. This edits future CI configuration only. |
| R07 / P3 | `resources/skills/pioneer/runtime-diagnostics/references/symbols.md` | Documents `debug-symbol-inputs-gateway-TARGET-default` and `...-computer-use`, Desktop's CU Gateway, and the historical unsuffixed generation. No symbol producer, consumer or trust check changed for this finding. |

R01 uses the already selected Xcode/SDK policy, supported by the primary
[runner label table](https://github.com/actions/runner-images#available-images),
[Intel inventory](https://github.com/actions/runner-images/blob/main/images/macos/macos-15-Readme.md),
[ARM inventory](https://github.com/actions/runner-images/blob/main/images/macos/macos-15-arm64-Readme.md)
and [Apple Xcode 26.2 requirements](https://developer.apple.com/documentation/xcode-release-notes/xcode-26_2-release-notes).
Both runner inventories list Xcode 26.2 at the explicit path; Apple requires
macOS 15.6 or newer for its host. The new host selection is a build policy, not
an increased runtime minimum or a platform compatibility claim. CU's existing
scoped deployment input is unchanged; no global MACOSX_DEPLOYMENT_TARGET was restored.

### Complete target / variant matrix retained

Names below follow `pioneer-gateway-`. All six server assets retain the old
updater contract; all six CU assets contain the complete driver package.

| Target | Server asset | CU asset | macOS CU host, where applicable |
| --- | --- | --- | --- |
| x86_64-unknown-linux-gnu | linux-x86_64.gz | linux-x86_64-computer-use.zip | — |
| aarch64-unknown-linux-gnu | linux-aarch64.gz | linux-aarch64-computer-use.zip | — |
| aarch64-apple-darwin | macos-aarch64.gz | macos-aarch64-computer-use.zip | macos-15 / ARM64 |
| x86_64-apple-darwin | macos-x86_64.zip | macos-x86_64-computer-use.zip | macos-15-intel / x86_64 |
| x86_64-pc-windows-msvc | windows-x86_64.zip | windows-x86_64-computer-use.zip | — |
| aarch64-pc-windows-msvc | windows-aarch64.zip | windows-aarch64-computer-use.zip | — |

### Final static self-review and test authorship

The complete HEAD → candidate restoration diff was reviewed, including the
pre-existing restoration. Source traces reconfirm the following boundaries:

1. CLI default → Gateway default → tools default has no CUA activation. The only
   CU manifest edges are CLI forwarding to Gateway and Gateway enabling optional
   SDK/tools. Neither the five selected server CI packages nor their manifest
   consumers request CU. Both final-binary build helper consumers reach the early
   CU guard before Swift/framework discovery. This is manifest/source analysis;
   actual Cargo resolution and linking remain deferred.
2. Gateway still owns the supervisor, backend injection and shutdown. Server
   injects None; mixed tools-on/Gateway-off still has empty materialization and
   no native admission. Prompt now uses that real registry. Known permission/domain
   vocabulary, per-call approval, request_tools selection and the 1200-character
   guard remain. The hand-written CU catalog expectation contains 25 operations
   and 1172 characters; this count is text analysis, not renderer execution.
3. Producer, CLI selector, root bootstraps, Desktop selector, checksums/publication,
   retained inventories and symbol consumers still agree on all 12 assets. Server
   gzip is a standalone compressed executable; server ZIP contains a server
   executable plus existing Intel ONNX libraries where required. Only CU invokes
   the daemon/app packager. Desktop explicitly restores and packages CU.
4. Installer extraction, candidate-local libraries, complete CU identity/hash/pin/
   target/companion checks, schema preflight before stopping the old service,
   activation, retained backups and schema-advancement rollback prohibition are
   unchanged this round. CU→server and server→CU boundaries remain as described
   in the restoration section. Old server names/formats are retained on every OS;
   old CU binary-only extractors still require the documented payload-aware
   bootstrap, not a promise of transparent update compatibility.
5. Supervisor/private IPC/identity, authorization, original image bytes/geometry/
   tokens, cursor, native/CLI/proxy delivery, cancellation/generation fencing and
   no-replay bodies were not changed. Snapshot comparison retains these reviewed
   bytes. Windows AtLogOn/Limited, macOS GUI LaunchAgent and Linux systemd user
   service boundaries remain; no new service, SDK fork, legacy engine or display
   infrastructure was introduced.
6. Compared with all 267 audit snapshot path states, only these nine code/config/
   instruction files and this appended handoff differ; the other 257 path states
   are byte-for-byte unchanged, including all 143 inherited-main states. Historical
   reviewer materials were read only. This self-review is not reviewer acceptance.

Applied test-audit to exclusively test regions/files after completing production
and CI edits. Protected mixed-file production prefixes were saved before the
separate test phase and remained identical through its edits/checks. The existing
prompt compiler owner covers backend None/injected with the real tool registry;
feature-off is covered by its server build. The three manager fixtures own
provider/preflight visibility; the Gateway owner retains persistence/order; the
two continuation owners retain exact prompt bytes. These address distinct existing
boundaries without new harnesses, source-grep tests, skipped scenarios or test-only
production seams. None of these regressions was executed; no pre/post passing
comparison is claimed.

### Files, commands and deferred execution

This round changes ten existing tracked files: the nine owners listed in the table
and this handoff. Final state is **36 unstaged tracked files, 0 staged files,
0 untracked source files**. In addition to this round's files, the following 26
entry-restoration files remain unchanged from the incoming candidate:

- `.cargo/config.toml`; `.github/workflows/build-debug-symbols.yml`; `README.md`.
- `crates/cli/Cargo.toml`; `crates/cli/src/installer.rs`; `crates/cli/src/service/macos.rs`.
- `crates/desktop/src/gateway/control/launcher.rs`.
- `crates/gateway/Cargo.toml`; `crates/gateway/build_support.rs`; `crates/gateway/src/lib.rs`;
  `crates/gateway/src/message/turn_handlers.rs`; `crates/gateway/src/turn_mcp/native_tools.rs`.
- `crates/tools/Cargo.toml`; `crates/tools/src/lib.rs`;
  `crates/tools/src/handlers/computer_use/mod.rs`.
- `install.sh`; `install.ps1`.
- `scripts/packaging/build-gateway.py`; `scripts/packaging/cua-driver.py`;
  `scripts/packaging/desktop/check-package-contents.sh`;
  `scripts/packaging/desktop/package-linux-appimage.sh`;
  `scripts/packaging/desktop/package-macos.sh`;
  `scripts/packaging/desktop/package-windows-msi.ps1`;
  `scripts/packaging/release-inputs.py`; `scripts/packaging/symbol-inputs.py`;
  `scripts/packaging/test_symbol_inputs.py`.

Performed source/skill/audit reads with `rg`, `cat`, `sed`, read-only Git
status/diff/show/rev-parse, browser reads of official Apple/GitHub/nextest documentation,
and Python standard-library text/hash/JSON/TOML analysis and edits. No project code
was imported. `cargo fmt --all` ran before the test-only phase; later formatting
checks were check-only, with reported test-region formatting corrected as text.
The final `cargo fmt --all --check`, `git diff --check`, `git diff --cached --check`
and `git diff --exit-code -- Cargo.lock` returned exit 0. Earlier fmt checks reported
only formatting differences subsequently corrected; they were not test runs.

Complete final source exports under `.cua-source/server-review-r01-r07-entry/`:
`findings.patch` (entry → corrected candidate), `final-restoration.patch`
(HEAD → candidate), `final-integrated.patch` (integrated main → candidate),
`final-original.patch` (original base → candidate). `final-state.json` records
exact export hashes, source hashes, Git state and preservation checks. These are
source artifacts only; ignored entry/evidence files are not untracked source.

Still deferred: all locked server/CU compilation across six targets; executing
selected nextest owners in both configurations (including tools-on/backend-None),
CU archive/shards/doctests; snapshot execution; packaging/signature/provenance and
native dependency closure; old-server update, old-CU bootstrap, both variant
transitions/recovery; OS minimum/session/permission/cursor/media/cancellation
qualification. No Cargo resolver, tests, builds, compiler, linter, typecheck,
CI/workflow dispatch, packaging/install/dev scripts, SDK/daemon, GUI actions or
OS/native probes ran. Actual compilation/runtime/platform compatibility remains
unverified. Execution stays prohibited pending full external source acceptance
and separate explicit authorization.


## R08 implementation: legacy CU update compatibility (2026-10-24)

Round 2 rejected manual bootstrap as a replacement for ordinary old-CU update.
The minimal integration is the existing executable boundary: old selectors and
extractors deliver a compatibility executable; it carries the complete signed
CU package, prepares it without changing live installation state during version
probe, and delegates commands to that package. Immutable package-identity directories
keep the old updater's binary-only rollback paired with its own payload generation.
There is no recursive installer, Desktop dependency or daemon execution in the bridge.
Modern consumers select a distinct complete-package ZIP; legacy names retain real
gzip-executable/ZIP formats. The producer/publication and existing installer/symbol
owners own these changes; R01–R07 remain preserved. Implementation evidence, matrix,
regressions and final state follow below after the final source self-review.


### R08 final compatibility design and exact old consumer

R08 is addressed in this source candidate, pending the external reviewer. The
round 1 manual-bootstrap exception and the older handoff's twelve-physical-files
assumption are superseded here. There are still twelve product combinations,
with six additional compatibility assets for old binary-only CU consumers.

The integration is the **existing executable version/command boundary**, not a
new updater framework. `crates/cli/src/cu_update.rs` is a release-only entry point;
`legacy-cu-bootstrap` forwards CU and is not a default feature. `cli/build.rs`
binds the already completed CU ZIP with include_bytes before bridge compilation.
No executable overlay is appended after signing. Ordinary/default and ordinary
CU builds do not require this build input or build the bridge entry point.

Exact baseline `6c1e6b2c49359c76e49624c0545bbafc4a3d310a` installer source trace:

| Old boundary | Exact baseline installer.rs evidence | R08 integration |
| --- | --- | --- |
| Release/asset readiness | 725–737, 753–781, 796–802 | All six old CU names and SHA256SUMS are published again. |
| Compiled selector | 1013–1037 | Real gzip executable for Linux/ARM macOS; ZIP for Windows/Intel macOS. |
| Archive checksum | 283–304 | Old updater authenticates the bridge including all embedded package bytes. |
| Extractor | 1500–1604 | Outer ZIP has one executable basename; the complete signed app/UIA/daemon/manifest stays inside its embedded ZIP. |
| Status / runtime home / schema | 329–382 | Bridge prepares a private generation and returns the real new Gateway's version/migration metadata; old updater retains its own schema preflight before stop. |
| Backup/stop/replace/probe/link/start/persist | 385–594 | No nested install runs. The old parent continues its own lifecycle while bridge commands delegate to the verified packaged Gateway. |
| Unsafe rollback prohibition | 1172–1183, callers 517–553 | The old baseline's schema-advancement rule is unchanged; the bridge does not intercept or relax it. |

Local tag `0.64.0` has installer bytes identical to this baseline (SHA256
`bd876a9dc03c5d260e699d170e512fbebc7e00843c93f97efd3513931bedccf3`).
The staged schema-preflight flow was also inspected in local tags `0.62.0` and
`0.63.0`. Earlier local tags have different flows: `0.61.0` probes after replacement;
`v0.1.0` has no pre-start candidate version probe, and pre-v0.22 Intel macOS
selectors use gzip. These are source observations, not an invented support cutoff
or evidence of installed/released binaries. No repository support policy defining
all supported historical installations was found. The six compatibility inputs
below correspond to the exact consumer specified by R08 and locally tagged 0.64.0;
this handoff does not claim qualification of every historical tag or of those older
rollback policies. Release/installed-version inventory and platform execution
remain deferred, not silently replaced with a manual migration instruction.

### Six legacy inputs and the modern/server matrix

All names below have prefix `pioneer-gateway-`. Publication requires all eighteen
exact archives; SHA256SUMS and upload include them all.

| Target | Unchanged server | Old CU consumer / compatibility archive | New CU installer / Desktop package |
| --- | --- | --- | --- |
| x86_64-unknown-linux-gnu | linux-x86_64.gz | linux-x86_64-computer-use.gz | linux-x86_64-computer-use-package.zip |
| aarch64-unknown-linux-gnu | linux-aarch64.gz | linux-aarch64-computer-use.gz | linux-aarch64-computer-use-package.zip |
| aarch64-apple-darwin | macos-aarch64.gz | macos-aarch64-computer-use.gz | macos-aarch64-computer-use-package.zip |
| x86_64-apple-darwin | macos-x86_64.zip | macos-x86_64-computer-use.zip | macos-x86_64-computer-use-package.zip |
| x86_64-pc-windows-msvc | windows-x86_64.zip | windows-x86_64-computer-use.zip | windows-x86_64-computer-use-package.zip |
| aarch64-pc-windows-msvc | windows-aarch64.zip | windows-aarch64-computer-use.zip | windows-aarch64-computer-use-package.zip |

The Intel compatibility ZIP supplies a genuine ONNX dylib copied from the complete
package, under `libonnxruntime.pioneer-cu-SHA256.dylib`. The legacy extractor counts
it as required but does not overwrite existing loaded library names before schema
admission. This is not a fake library or substitution of the driver. Actual Gateway
execution/probes use the full standard-named libraries in the private generation.
The additional hash-named compatibility library is retained; no automatic cleanup
of recovery-related files was introduced.

### Preparation, activation, subsequent update and recovery

`legacy_cu_update_main` → `prepare_legacy_cu_package` → `probe_legacy_cu_package`
→ same original CLI arguments to the actual Gateway. Environment and stdio inherit.
Unix uses exec; Windows waits for the verified child and propagates its exit code.
The bridge has no driver/session/cursor implementation and starts no daemon itself.

Preparation uses the existing bounded ZIP extractor and full pin/target/manifest/
daemon hash/companion checks in an owner-private temporary directory next to the
old candidate. It activates companions only inside that new directory, then
publishes the whole package via one rename to `.pioneer-cu-PACKAGE_SHA256`.
It never changes live executable/cua/libraries/service, runtime home, settings,
connections, credentials, histories, tasks or databases. Before any Gateway command,
reuse validates every expected regular file against embedded archive bytes and
rejects symlinks, missing/changed files and extra files (including loadable DLLs).
The real executable probe requires exact release version, OS, architecture, CU
flag and migration metadata. Normal version JSON gains only the backward-compatible
`computer_use` boolean; legacy readers ignore unknown fields.

For the old updater's staged probe, this is preparation only. The parent still
checks schema and replaces its installed executable normally. For start, the
verified real CLI installs/starts the existing platform service from that generation;
on macOS its existing selector chooses the signed Gateway app and matching helper.
No Desktop process is required. Runtime-home/config resolution remains the existing
AppConfig path; the bridge does not supply a new home or copy user data.

A pre-start outer rollback restores the old executable and leaves the unused new
private generation separate; no old live payload was replaced. A bridge restored
by a later new-installer rollback resolves its own embedded package hash, not a
shared mutable payload of another version. Old and new generation files are retained.
After possible schema advancement, the existing automatic rollback prohibition
remains. Real filesystem/service failure and recovery behavior is still unrun.

The first and later normal `update/self-update` commands delegate to the actual
CU CLI, whose selector uses the complete-package ZIP and whose installer still
uses the configured managed install root. It can replace the outer bridge with
the real new executable and coherent `bin/cua` payload. Existing server→server,
server→CU and CU→server extraction/activation/schema/backup logic remains; old private
packages may remain as retained recovery inputs and are not executed by server.

### Producer, provenance and preserved contracts (C01–C12 self-review)

- **C01/C07/C08:** Gateway supervisor, SDK pin, private IPC, driver sessions/cursor,
  cancellation and shutdown bodies remain byte-identical to the accepted R01–R07
  candidate. The new entry point forwards to that actual Gateway; no legacy engine,
  SDK fork, service, remote transport or display infrastructure was added.
- **C02:** default feature graphs and both final-binary early CU guards remain.
  The new bridge requires explicit legacy-cu-bootstrap → computer-use; ordinary
  server/default builds do not embed packages, discover CUA Swift/frameworks or
  build/sign/package the daemon. All six unchanged server names/formats remain.
- **C03/C04:** build-gateway.py produces the complete CU ZIP first, compiles the
  bridge with those exact bytes, then creates genuine legacy gzip/ZIP archives.
  New CLI, shell/PowerShell scripts, all Desktop packagers/selectors/content guards
  and synthetic smoke asset names consistently use the complete-package suffix.
  Twelve product combinations remain; publication now checks eighteen files.
- **C05:** no recursive run_install or schema bypass. Existing new installer
  preflight-before-stop, candidate-local libraries, full extraction validation,
  retained backups and schema-uncertain rollback guard are unchanged. Private
  generations avoid mixing old executable recovery with a new mutable payload.
- **C06:** R01 native macOS hosts and Xcode 26.2/DEVELOPER_DIR remain. Existing signed,
  notarized/stapled app packaging is preserved; the bridge is separately signed and
  notarized after embedding. The existing notarization owner is shared, not duplicated.
  Apple documents the two notarization passes for custom installers and that
  standalone binaries cannot have tickets stapled:
  [Customizing the notarization workflow](https://developer.apple.com/documentation/security/customizing-the-notarization-workflow).
  System permission prompts/grant transfer, actual signing/TCC/OS minimum and native
  dependency closure are explicitly unqualified. No automatic OS-grant migration
  is promised. Existing Windows AtLogOn/Limited, macOS LaunchAgent and Linux systemd
  user service mechanisms are retained.
- **C09/C10/C11:** authorization, frozen CLI/MCP bindings, permitted proxies, typed
  image bytes/geometry/tokens, delivery safeguards, no-replay and generation fencing
  source is unchanged. The package generation only changes the installed path of
  the same signed/verified binaries; native/CLI/proxy media paths are not rewritten.
- **C12 / R01–R07:** no unrelated main cleanup. Accepted SDK policy, runtime prompt
  availability, visibility/binding fixtures, exact continuation expectations, server
  CI owner run and variant symbol names are preserved. New scope beyond the prior
  inventory is the necessary CLI dispatch owner and release-only entry point.

The bridge gets its own exact-input inventory under the existing CU Gateway
`symbol-inputs...-computer-use/compatibility/` artifact. It retains source run/commit/
target/catalog identity, native module/PDB/object inputs and final signed-byte
ledger. The symbols consumer validates its association with the parent CU inventory,
then processes it without rebuilding. Desktop's reuse still restores the real CU
Gateway, with its unchanged hash/module/run/catalog checks; it does not substitute
the bridge or claim it shipped the standalone compatibility executable.

### Authored regressions and internal review resolutions

Applied test-audit to existing owners, with protected production prefixes saved
for each test-only batch. The Windows preparation gap found by the second reviewer
was corrected by separating file preparation from the mandatory real executable
probe, then extending the same tests on every platform. This is not a test-only
production seam: the actual bridge calls both phases consecutively.

- Existing installer asset-name owner now checks all six server and six modern CU
  selectors, independent of the host used for a future test run.
- Existing complete archive/rollback owner retains its original activation/recovery
  assertions and adds corrupt manifest/pin/hash/missing-companion preparation errors,
  success/reuse, executable tamper/extra DLL rejection, and preservation of prior
  executable/settings/credentials/data fixtures. File assertions apply on Windows
  as well. Only shell-based subprocess/version/schema cases are Unix-specific;
  they include wrong version/OS/architecture/variant and missing migration metadata.
- Existing ReleaseInputTests calls the real compatibility archive producer across
  all six targets, then models the exact old gzip or flattened ZIP consumer. It
  proves one executable basename, opaque payload survival, real Intel companion
  placement and unchanged prior libraries/settings. It does not pretend that this
  consumer can install new sidecar layouts. The same owner adds wrong bridge
  run/commit/target/catalog/variant provenance rejection before child native tools.
- Existing Desktop command fixture expects the modern complete-package name. The
  already accepted server/CU CI owners and archive/sharding configuration remain.

No test was run, no snapshot generated and no pre/post passing comparison claimed.
The archive fixtures use opaque executable/library bytes; they do not establish
native executable validity, successful restart, DLL closure or OS permission readiness.

| Attached task / run | Exact resolution |
| --- | --- |
| Legacy trace `qUVVSAEMFtpTFK1sfUHP4` / `H3VoVam3ESxV05GQHucwo` | Candidate `trc_H3VoVam3ESxV05GQHucwo_NX0yWxshiMS29ps1JeU2B`, content SHA256 `1eb4293321c14894f8e4baa98ed2550bc3d40275b4215995e8ac4ee94fa30e74`, read in full and independently checked; accepted, completed/succeeded. |
| Candidate source review `VRvD38Cl1bfbewzpYOjuH` / `OEHqtD8pnnzNIvgrETUui` | Initial candidate `trc_OEHqtD8pnnzNIvgrETUui_5Hm30jrLJ8t1r0YD20htF` (SHA256 `138509b2a62c7a29742162755f6774af646fefc0daa34b80cbee234e2410428d`) read and revised for Windows file/preparation coverage. Exact revision `trc_OEHqtD8pnnzNIvgrETUui_revise_trc_OEHqtD8pnnzNIvgrETUui_5Hm30jrLJ8t1r0YD20htF_1` (SHA256 `07f2a8d9ed72c852ba111b88538ee7254f44479e840b89f6ad3e2b5d3d99ee18`) read, compared with source and accepted; completed/succeeded. Parent additionally reviewed the final metadata/provenance test expansions. |

Both were read-only and received the complete execution ban. No attached work
was detached or left unresolved. Internal acceptance is not external source acceptance.

### Final files, hashes, permitted commands and deferred execution

Branch/HEAD remain `feature/cua-gateway-migration` /
`747f60be0aa1718f8f294a924a6a39b9d9a80f39`; original base and integrated main are
unchanged. Final tree has **38 unstaged tracked files, zero staged files and one
untracked source file** (`crates/cli/src/cu_update.rs`). Nothing was committed/pushed,
no branch/worktree/reset/clean/stash/merge/rebase operation ran, and the paused monitor
was not inspected or changed. No dependency/pin/version/checksum/lock entry or
`--locked` policy was changed. Cargo.lock still equals HEAD (SHA256
`f76eb868626c41ddf0da43d9db28ed2bb918c0e55c6ed666ed0b30733617abe5`); raw index remains
`09a4be787e9a7cf1005dc4325e273816d7b7c1bfe9efca19620aab9be86a04ce` and staged diff
remains empty. All 143 inherited-main path states remain unchanged from entry.

This R08 delta changes nineteen existing files and adds one entry point:
`.github/workflows/{release,smoke-install}.yml`, `README.md`,
`crates/cli/{Cargo.toml,build.rs,src/installer.rs,src/lib.rs,src/cu_update.rs}`,
`crates/desktop/src/gateway/control/launcher.rs`, this handoff, `install.sh`,
`install.ps1`, `scripts/packaging/{build-gateway.py,cua-driver.py,symbol-inputs.py,test_symbol_inputs.py}`
and `scripts/packaging/desktop/{check-package-contents.sh,package-linux-appimage.sh,package-macos.sh,package-windows-msi.ps1}`.
All other incoming source bytes are retained. Reviewer artifacts were read only.

Entry hashes and tracked-source snapshots are in ignored `.cua-source/r08-entry/`.
Entry restoration/integrated/original patch SHA256 values are respectively
`abb4e5ef8359fd37bacf26909eb0d49c4fd4397e007e34e94b7cebca0f204485`,
`d11015ba9f696831b04db3cc1df00e07f453453add8a72c30417b401e958fbc3`,
`5ab51f09acb09a39ea08182a9e62b8f07844bdb502f6b41ac3b159d90a9ffeda`.
`candidate.patch` is entry→R08; `final-restoration.patch`, `final-integrated.patch`
and `final-original.patch` include the new untracked entry point as well as all
tracked diffs. `final-state.json` records exact final source/export hashes, statuses,
preservation and allowed-check results for external review.

Actual commands were source/skill/audit reads (`cat`, `sed`, `rg`), read-only Git
status/diff/show/rev-parse/tag/ls-files, Python standard-library text/hash/JSON/TOML
analysis and text edits, official Apple documentation browsing, and the attached
task create/get/wait/revise/accept calls recorded above. No project Python module
was imported or executed. `cargo fmt --all` ran during production editing;
`cargo fmt -p pioneer-cli -- --emit stdout` wrote temporary formatter output,
from which only test regions were copied after checking production-prefix equality.
Final `cargo fmt --all --check`, `git diff --check`, `git diff --cached --check`
and `git diff --exit-code -- Cargo.lock` passed. Earlier formatting-only differences
were corrected; no test/build outcome is implied by fmt.

Deferred until complete external source acceptance and separate explicit execution
permission: locked default/CU/bridge compilation and exact native closure on all
six targets, the authored nextest/Python owners and platform fixtures, actual old
0.64.0 update/self-update and supported-installed-version inventory, new CU updates,
variant switches, preflight/activation/restart/recovery fault boundaries, complete
release checksums/assets/signing/notarization/provenance, OS permissions/cursor/media/
cancellation and minimum-OS qualification. No tests, compiler/build/check/typecheck/
linter/resolver, CI/workflow, packaging/install/dev scripts, SDK/daemon, GUI or OS
probes ran. **This is a reviewable source candidate, not external acceptance or
passing compilation/runtime/platform evidence.**


## 2026-10-24 — requested merge of current main after source acceptance

The user accepted R01–R08, authorized publication (accepted implementation commit
`abbdf80a654e48c9c414d82fc1a5de0dc19bdea2`), then requested resolution of the PR's
main conflicts and another push. Entry branch was `feature/cua-gateway-migration`,
HEAD `abbdf80a654e48c9c414d82fc1a5de0dc19bdea2`, with clean worktree/index.
After `git fetch origin`, main was
`c0f9f151ef43a3b57a6bec279a347bca9a8526e2` (previous common ancestor
`fa0ec29bfd19f9ada8f66be9665d77f5e51e0095`). GitHub reported CONFLICTING/DIRTY.
A normal `git merge --no-commit --no-ff origin/main` exposed one content conflict
in `crates/gateway/src/cli_runtime/mcp/facade.rs`.

Resolution preserves both the CUA task-local typed delivery guard and main's
adapter-selected `CliMcpCallContract`. Main's native identity parsing, ledger
identity, admission deadline and real callers remain; CUA final-frame receipt
commit/revocation and no-replay behavior remain. The existing oversized-response
regression now supplies a valid independent Claude native tool-use ID while
retaining the escaped RPC ID, duplicate/revocation/action/fresh-observation
assertions. Otherwise main's auto-merged changes are retained.

Applied test-audit only to this test-fixture batch after the production conflict
resolution: the exclusively test-module edit preserved the full production prefix
and all other tracked file hashes. No assertion was removed, no new harness or
production seam was added. Static tree comparison also confirmed all 114
feature-only paths match entry HEAD and all 89 main-only paths match fetched main;
the eleven overlapping paths were inspected for the existing migration changes
and main's provider/native correlation additions. Server/CU features, six-target
release/compatibility packages, update/recovery and provenance owners were not
changed by conflict resolution. Cargo.lock equals entry HEAD; CUA pin remains
`688c5fd62a9f13dddc309970c948df494e7de33f`.

Actual checks: `cargo fmt --all --check`, `git diff --check`,
`git diff --cached --check`, Git lockfile equality, and Python standard-library
source/hash comparison. An initial bytes-to-Path mistake in the comparison script
failed before any write and was corrected. No local tests/builds/compiler/linter,
resolver, project scripts or runtime/platform probes ran. Existing monitor
`1EuSrEXDwGwWI6UvduAfp` was paused before merging (no run active), and will resume
on the same fifteen-minute schedule after the authorized merge commit/push.
Fresh GitHub CI and platform/runtime qualification remain pending.

## 2026-10-24 — CI cancellation fixture database lifetime

Monitor run 25 inspected current head `57a960d90480ae1eba9b77f0ac639e82ea79204d`
and CI run `40093388808`. Windows, macOS, Linux build and shard 1 passed;
shard 3 failed in `turn_mcp_cancel_reaches_upstream_and_late_release_cannot_succeed`
when its post-cancellation audit read found no `mcp_audit_event` table.
Shards 2 and 4 were still running. Future monitor firings were paused and verified
before editing.

The invoker fixture used `sqlite::memory:`, whose schema can disappear when
cancellation retires its pooled connection. The test-only fixture now retains a
TempDir and uses a migrated SQLite file for its lifetime; the existing service
setup accepts the database URL, retaining memory setup for unrelated tests.
Cancellation, late release, upstream call-count and exact audit reason assertions
are unchanged. No production code changed: its full prefix was compared with the
entry bytes. This is a narrow test-fixture exception outside CUA code, needed for
the existing merged-main cancellation regression; no product contract changed.

Applied test-audit to the test-only edit. `cargo fmt --all --check`,
`git diff --check` and unchanged Cargo.lock comparison passed. No local tests,
builds, compilers, resolvers or platform probes ran; fresh CI must validate this
repair. Fetched main `12fb677bc996957b64471a4b88acb08e1efc3eb4` merges without
conflicts (`git merge-tree --write-tree`); GitHub reports MERGEABLE. CUA pin,
server/CU variants, update/recovery and release assets remain unchanged.
