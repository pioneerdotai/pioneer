# AGENTS.md

## Crate Role

`pioneer-client` is the shell-neutral Rust client core. Code in this crate must be
useful to more than one shell, or be a lower-level domain primitive that a shell
can consume without UI assumptions.

## What Belongs Here

Put code in this crate when it is shell-neutral application behavior:

- gateway registry/profile planning, validation, connection timing, and
  websocket runtime primitives;
- typed gateway RPC params, response reductions, notification reductions, and
  event semantics;
- workspace planning, bootstrap, selectors, preference reductions, and mutation
  reductions;
- thread, folder, timeline, composer, provider, skill, MCP, settings, artifact,
  and agents-doc planning/projection when it does not depend on a UI framework;
- shared runtime state, snapshots, selectors, reducers, and effect planning;
- public schemas only for contracts that are truly shared domain or shared shell
  contracts.

The good shared layer is normally a planner, reducer, selector, transport
primitive, projection, or immutable DTO used by multiple shells.

## What Does Not Belong Here

Do not add shell boundary code to this crate just because mobile needs it:

- React Native, Nitro, JSI, Swift, Kotlin, Objective-C, JavaScript, TypeScript,
  MMKV, SecureStore, or mobile lifecycle assumptions;
- GPUI widgets, desktop windows, menus, themes, dialogs, localized UI copy, or OS
  file/open/reveal behavior;
- `extern "C"` functions, raw pointer handling, C ABI response envelopes, or
  bridge memory ownership helpers;
- JSON-string invocation wrappers whose main job is to deserialize a request,
  call a lower-level shared planner, and serialize a result;
- Nitro/TS-friendly event envelopes such as `ClientEvent` when desktop consumes
  a different lower-level runtime event model;
- `*Request`/`*Result` DTOs that are only used by `client-ffi`;
- mobile-shaped `Client*` projections unless desktop is migrated to the same
  shell-neutral projection in the same change.

If a symbol is only consumed by `client-ffi`, it usually belongs in
`pioneer-client-ffi`, not here.

## Public API Rule

Every new public item needs a consumer story:

- If desktop and mobile can both use it directly, keep it shell-neutral and place
  it here.
- If desktop should use it but currently does not, either migrate desktop in the
  same change or document the explicit migration plan next to the change.
- If it is only a Nitro/TS/C ABI adapter, put it in `client-ffi`.
- If it improves desktop code, use neutral names and types. Do not use
  `Client*` names for mobile-shaped projections in core.

Do not force desktop through FFI-style wrappers just to make code appear shared.
Desktop is already Rust and should usually call typed domain planners/reducers
directly.

## Module Boundaries

- `gateway`: shared endpoint types, registry/profile planning, connectivity,
  timings, secret reference helpers, and websocket runtime concepts.
- `transport` and `rpc`: shell-neutral websocket and JSON-RPC primitives.
- `runtime`: shared runtime queues, event filtering, websocket reductions, and
  notification reductions. Boundary event envelopes belong in `client-ffi`.
- `workspaces::actions` and `workspaces::bootstrap`: shared workspace plans and
  reductions. Boundary management wrappers belong in `client-ffi` unless desktop
  intentionally adopts them because they improve desktop code.
- `threads`: shared thread tree params, reductions, sidebar-neutral planning,
  and domain projections. Mobile-only query wrappers belong in `client-ffi`.
- `contracts` and `schema`: keep only shared contracts here. Boundary schemas
  should be generated from the boundary crate or boundary-scoped modules.

## Dependency Rules

This crate must stay UI-free and shell-free. Do not add dependencies on:

- `gpui`, `gpui-kit`, `gpui-component`, `terminal`;
- `pioneer-desktop`;
- React Native/Nitro/mobile bridge crates;
- platform UI/localization crates such as `rust-i18n`;
- gateway server implementation crates such as `pioneer-gateway`.

Prefer `pioneer-protocol` types for gateway protocol data and keep shell storage
behind explicit plans instead of performing shell persistence directly.

## Testing

Use focused checks for the area changed:

```bash
cargo test -p pioneer-client
cargo test -p pioneer-client --features schema
```

If schema contracts change, regenerate/check schemas with:

```bash
cargo run -p pioneer-client --features schema --bin schema -- schemas/client
```

Mobile boundary request/result/event schemas are exported by
`pioneer-client-ffi`; do not add those DTOs back to this crate just to make the
mobile type generator work.

When moving logic out of desktop, also run the relevant desktop tests/checks.
