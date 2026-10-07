//! Bounded offline Agent Plugins 1.0.0 loading. No component execution,
//! installation, database, network or OAuth operations belong here.
pub mod containment;
mod diagnostic;
mod discovery;
mod manifest;
pub mod portable_mcp;
mod snapshot;
pub use diagnostic::{Boundary, Diagnostic};
pub use discovery::{ComponentPlan, LoadedPluginPlan, load};
pub use manifest::{
    MCP_SCHEMA, MCP_SCHEMA_BYTES, Manifest, PLUGIN_SCHEMA, PLUGIN_SCHEMA_BYTES, SPEC_VERSION,
    parse_manifest,
};
pub use portable_mcp::{PortableServer, ResolvedServer, parse_mcp};
pub use snapshot::{Entry, Limits, Snapshot};
