# AGENTS.md

## Crate Role

`pioneer-client-ffi` is the shell boundary crate. It exposes the Rust client core
to mobile/Nitro/C ABI consumers and owns bridge-specific request/result/event
shapes.

This crate may be mobile-facing. It must not become a second client core.

## What Belongs Here

Put code in this crate when it adapts the shared Rust core to the bridge:

- `extern "C"` exported functions and raw pointer/memory ownership handling;
- Nitro/C ABI request and response envelopes;
- JSON-string deserialization/serialization used by the bridge;
- TS/Nitro-friendly `*Request`, `*Result`, and event envelope DTOs;
- wrappers that call `pioneer-client` planners/reducers/runtime methods;
- bridge-specific schema exports for mobile-generated TypeScript types;
- bridge threading/queue code that prevents long work from blocking the React
  Native JS thread.

Boundary DTOs are acceptable here even when desktop does not use them.

## What Does Not Belong Here

Do not implement application behavior here:

- no gateway registry rules duplicated from `pioneer-client`;
- no workspace switching/create/rename rules duplicated from `pioneer-client`;
- no thread tree sorting/projection rules duplicated from `pioneer-client`;
- no provider, skill, MCP, settings, timeline, or artifact business logic
  reimplemented for mobile;
- no desktop/GPUI code;
- no direct persistence policy that should be shell-owned.

If a bridge method needs new behavior, first add or reuse a shell-neutral
planner/reducer/projection in `pioneer-client`, then call it from this crate.

## Boundary Pattern

FFI functions should stay thin:

1. read and validate bridge input;
2. deserialize into a boundary DTO;
3. call `pioneer-client` typed API;
4. serialize a boundary result;
5. return through the common FFI response path.

When adding many exported methods, prefer shared helpers/macros for repeated
pointer/input/response handling instead of hand-writing the same boilerplate.

## Threading

Bridge calls must not block the React Native JS thread with long work.

- Keep websocket loops, reconnects, waits, and IO inside Rust runtime workers or
  nonblocking queues.
- Expose event draining/next-event style APIs that return promptly to JS.
- Do not run long polling, sleeps, or gateway connection loops directly in a
  synchronous bridge call.

## Storage And Secrets

Mobile-native storage belongs to the mobile shell. This crate may return plans
such as token writes/deletes, but should not hard-code MMKV, SecureStore, keychain
paths, or mobile app data directories.

Avoid adding direct `pioneer-protocol` dependencies unless there is a deliberate
reason. Prefer consuming `pioneer-client` public API and re-exported/shared DTOs.

## Testing

Use focused checks for FFI changes:

```bash
cargo test -p pioneer-client-ffi
cargo test -p pioneer-client
```

If bridge schemas are changed, verify schema generation/type generation in the
consumer workflow before handing the change off.

```bash
cargo run -p pioneer-client-ffi --features schema --bin schema -- schemas/client
```
