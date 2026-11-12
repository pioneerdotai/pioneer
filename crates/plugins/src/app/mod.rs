//! Strict, offline App format. This module never invokes the Plugin format adapter.
mod json;
mod loader;
mod manifest;
mod selection;
pub use manifest::path as canonical_package_path;
pub use manifest::{AppManifest, AppPresentation, ManifestError};
pub use selection::{SelectionKind, SelectionPlan};

pub use loader::{LoadedAppPackage, load};
