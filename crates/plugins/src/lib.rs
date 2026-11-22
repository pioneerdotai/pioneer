//! Bounded offline Agent Plugins 1.0.0 loading and Codex/Claude/Cursor package import. No component execution,
//! installation, database, network or OAuth operations belong here.
mod discovery;
mod manifest;
mod metadata;
mod source;
pub use discovery::{LoadedPluginPlan, load};
pub use manifest::{
    MCP_SCHEMA, MCP_SCHEMA_BYTES, Manifest, PLUGIN_SCHEMA, PLUGIN_SCHEMA_BYTES, SPEC_VERSION,
    parse_manifest,
};
use pioneer_packages::{Boundary, Diagnostic, Entry, Snapshot, load_mcp};
use pioneer_packages::{diagnostic, package_mcp};
