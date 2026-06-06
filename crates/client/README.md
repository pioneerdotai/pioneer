# pioneer-client

`pioneer-client` is the shared, shell-neutral Rust client core for Pioneer clients.
It owns the client logic that should not be duplicated in desktop, mobile, or any
future shell.

## Boundary

Move code into this crate when it:

- talks to the gateway or prepares typed gateway RPC payloads;
- interprets `pioneer-protocol` notifications or response data;
- maintains client read models, reducers, snapshots, selectors, or timeline rows;
- coordinates composer, turn, thread, workspace, provider, skill, MCP, settings,
  artifact, or agents document workflows without UI dependencies;
- exposes public DTO contracts or JSON Schema for shell boundaries.

Keep code in a shell crate when it needs GPUI, React Native, windows, menus,
theme/layout state, dialogs, native file pickers, OS open/reveal calls, local
gateway service install/start/recovery, or localized UI copy.

## Main Modules

- `gateway`: endpoint types, registry normalization, connectivity, runtime
  reducers, secret reference helpers, and timings.
- `transport::ws`: websocket request/event primitives, upload/download frame
  helpers, reconnect planning, and typed gateway command wrappers.
- `rpc`: JSON-RPC request lifecycle primitives used by shell-owned transports.
- `state`: aggregate read models, reducers, selectors, snapshots, and effects.
- `conversation` and `timeline`: gateway history/notification projection and
  UI-neutral timeline rows/labels.
- `composer`, `turns`, `threads`, and `workspaces`: shell-neutral workflow
  planning and selectors.
- `providers`, `skills`, `mcp`, and `settings`: catalogs, policy/actions,
  settings, and presentation DTOs.
- `artifacts` and `agents_doc`: artifact transfer/cache helpers and agents
  document state machines behind platform traits where needed.
- `contracts` and `schema`: public DTO contract registry and optional schema
  export.

## Examples

Examples are normal Cargo examples and should stay shell-neutral:

```bash
cargo run -p pioneer-client --example gateway_registry
cargo run -p pioneer-client --example composer_turn_input
cargo run -p pioneer-client --example json_rpc_payload
```

Use these examples as the first reference when wiring a shell to shared Rust
client logic. They intentionally do not include React Native bridge code,
desktop UI code, or mobile-specific tests.

## Schema Export

The optional `schema` feature exports the public DTO/schema boundary:

```bash
cargo run -p pioneer-client --features schema --bin schema -- schemas/client
```

The generated schemas are intended for shell-facing type generation. Internal
reducers and implementation details should not be added to the schema registry
unless they are part of the public client contract.
