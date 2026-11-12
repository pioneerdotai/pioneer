//! Strict, offline App format. This module never invokes the Plugin format adapter.
mod json;
mod manifest;
mod selection;
pub use manifest::{AppManifest, AppPresentation, ManifestError};
pub use selection::{SelectionKind, SelectionPlan};
