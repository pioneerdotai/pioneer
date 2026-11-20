//! Preparation of local authoring inputs. This does not install or execute an App.
use crate::{LoadedAppPackage, SelectionPlan};
use pioneer_packages::{Limits, Snapshot};
use std::path::{Path, PathBuf};

/// A validated source snapshot, deliberately distinct from a release package.
/// The snapshot owns the bytes used by component publication; later edits to
/// the author's directory cannot mutate this revision.
#[derive(Debug, Clone)]
pub struct PreparedAppSource {
    pub project: PathBuf,
    pub package: LoadedAppPackage,
    pub snapshot: Snapshot,
    pub selection: SelectionPlan,
}

impl PreparedAppSource {
    /// Read only app.json and its declared presentation/component inputs.
    /// Runtime syntax is still checked by AppManifest, but release executable
    /// modes, guest architecture and artifact checksums do not apply here.
    pub fn capture(
        project: &Path,
        limits: Limits,
        cancelled: impl Fn() -> bool,
    ) -> anyhow::Result<Self> {
        let project = project.canonicalize()?;
        anyhow::ensure!(project.is_dir(), "apps.source_not_directory");
        let (_, snapshot) = SelectionPlan::capture_source(&project, limits, &cancelled)?;
        let package = crate::loader::load_source(&snapshot)?;
        let selection = SelectionPlan::for_source(&package.manifest)?;
        anyhow::ensure!(!cancelled(), "apps.source_cancelled");
        Ok(Self {
            project,
            package,
            snapshot,
            selection,
        })
    }
}
