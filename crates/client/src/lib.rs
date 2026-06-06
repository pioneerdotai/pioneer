//! Shared, shell-neutral client core for Pioneer clients.
//!
//! `pioneer-client` owns client-side transport primitives, protocol
//! interpretation, read models, selectors, presentation DTOs, schema contracts,
//! and workflow coordination shared by Pioneer shells. Desktop, mobile, and any
//! future client shell should keep UI rendering, native dialogs, process
//! management, and platform-specific file/open behavior outside this crate.
//!
//! ## Boundary
//!
//! Code belongs here when it is useful to more than one client shell and can run
//! without GPUI or desktop services:
//!
//! - gateway endpoint normalization, registry state, websocket request helpers,
//!   typed RPC payloads, and notification reduction;
//! - conversation projection, timeline rows, selectors, status/read models, and
//!   shell-neutral presentation DTOs;
//! - composer, turn, workspace, provider, skill, MCP, settings, artifact, and
//!   agents document workflows that do not call native UI APIs;
//! - public DTO contracts and optional JSON Schema export.
//!
//! Code should stay in a shell crate when it needs windows, views, menus,
//! theme/layout state, dialogs, native file pickers, OS open/reveal calls, local
//! gateway service installation/startup, or localization resources.
//!
//! ## API Map
//!
//! - [`gateway`] owns endpoint, registry, connectivity, lifecycle, and timing
//!   helpers for client-side gateway attachment.
//! - [`transport::ws`] owns websocket request/event primitives and typed command
//!   wrappers over [`rpc`].
//! - [`state`] owns aggregate read models, reducers, snapshots, selectors, and
//!   client effects.
//! - [`conversation`] and [`timeline`] own protocol-to-read-model projection and
//!   UI-neutral timeline rows.
//! - [`composer`], [`turns`], [`threads`], and [`workspaces`] own user workflow
//!   planning and selector logic.
//! - [`providers`], [`skills`], [`mcp`], and [`settings`] own catalog, policy,
//!   settings, and shell-neutral presentation helpers.
//! - [`artifacts`] and [`agents_doc`] own artifact transfer/cache helpers and
//!   agents document state machines behind platform traits where needed.
//! - [`contracts`] and [`schema`] define the public DTO/schema boundary for
//!   shell integration.
//!
//! ## Examples
//!
//! Compilable API examples live in `crates/client/examples`:
//!
//! - `gateway_registry.rs` normalizes local/remote gateway registry data.
//! - `composer_turn_input.rs` converts shell-selected attachments into protocol
//!   `UserInput` values.
//! - `json_rpc_payload.rs` builds a JSON-RPC request payload for a shell-owned
//!   transport.
//!
//! Run them from the workspace root with:
//!
//! ```text
//! cargo run -p pioneer-client --example gateway_registry
//! cargo run -p pioneer-client --example composer_turn_input
//! cargo run -p pioneer-client --example json_rpc_payload
//! ```

#![forbid(unsafe_code)]

pub mod agents_doc;
pub mod artifacts;
pub mod composer;
pub mod contracts;
pub mod conversation;
mod error;
pub mod gateway;
pub mod ids;
pub mod mcp;
pub mod notifications;
pub mod platform;
pub mod providers;
pub mod rpc;
pub mod schema;
pub mod settings;
pub mod skills;
pub mod state;
pub mod tasks;
pub mod threads;
pub mod timeline;
pub mod transport;
pub mod turns;
pub mod workspaces;

pub use error::{ClientError, ClientResult};
