//! Strict, offline App format. This module never invokes the Plugin format adapter.
mod json;
mod loader;
mod manifest;
mod selection;
mod source;
pub use manifest::path as canonical_package_path;
pub use manifest::{AppManifest, AppPresentation, ManifestError};
pub use selection::{SelectionKind, SelectionPlan};
pub use source::PreparedAppSource;

pub use loader::load_source;
pub use loader::{
    LoadedAppPackage, capture_release, load, load_for_architecture,
    load_for_architecture_cancellable, select_release,
};

pub mod runtime;

/// App delivery includes prebuilt backend libraries, independently of Skill limits.
pub const MAX_UPLOAD_ARCHIVE_BYTES: usize = 256 * 1024 * 1024;
pub const PACKAGE_LIMITS: pioneer_packages::Limits = pioneer_packages::Limits {
    expanded_bytes: 1024 * 1024 * 1024,
    entry_bytes: 256 * 1024 * 1024,
    entries: 16384,
    depth: 64,
    components: 256,
};

// Authoring snapshots still own their bytes and retain the previous limits.
pub const SOURCE_LIMITS: pioneer_packages::Limits = pioneer_packages::Limits {
    expanded_bytes: 256 * 1024 * 1024,
    entry_bytes: 64 * 1024 * 1024,
    ..PACKAGE_LIMITS
};
