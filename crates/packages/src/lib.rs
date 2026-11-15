//! Offline package containment, snapshots and native component preparation.
//! Domain manifests, runtime execution and installation orchestration belong to consumers.
mod component;
pub mod containment;
pub mod diagnostic;
pub mod images;
pub mod package_mcp;
mod snapshot;
pub use component::{ComponentPlan, valid_skill_at};
pub use diagnostic::{Boundary, Diagnostic};
pub use package_mcp::{PackageMcpServer, load_mcp};
pub use snapshot::{Entry, Limits, SelectionKind, Snapshot};
