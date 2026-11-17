//! One opaque run ID shared by OTLP resources and local diagnostics.
use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BuildManifest {
    pub schema_version: u32,
    pub rust_toolchain: Option<String>,
    pub rustflags_sha256: Option<String>,
    pub schema_registry_revision: Option<String>,
    pub component: Option<String>,
    pub manifest_id: String,
    pub revision: Option<String>,
    pub dirty: Option<bool>,
    pub tracked_diff_sha256: Option<String>,
    pub target: Option<String>,
    pub profile: Option<String>,
    pub features: Vec<String>,
    pub target_features: Option<String>,
    pub dependency_revision: Option<String>,
    pub provenance_completeness: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunIdentity {
    pub schema_version: u32,
    pub run_id: String,
    pub initialized_unix_ms: u64,
    pub pid: u32,
    pub build: BuildManifest,
}
static BUILD: OnceLock<BuildManifest> = OnceLock::new();
static RUN: OnceLock<RunIdentity> = OnceLock::new();
static CLOCK: OnceLock<Instant> = OnceLock::new();

/// Executable calls this before telemetry initialization; an embedded library
/// manifest cannot describe the executable's enabled features.
pub fn configure_build(json: &str) -> anyhow::Result<()> {
    let build: BuildManifest = serde_json::from_str(json)?;
    anyhow::ensure!(
        json.len() <= 16 * 1024 && build.features.len() <= 128,
        "build manifest too large"
    );
    anyhow::ensure!(RUN.get().is_none(), "run identity already initialized");
    BUILD
        .set(build)
        .map_err(|_| anyhow::anyhow!("build already configured"))
}
pub fn run_identity() -> &'static RunIdentity {
    RUN.get_or_init(new_run)
}
fn new_run() -> RunIdentity {
    RunIdentity {
        schema_version: 1,
        run_id: nanoid::nanoid!(24),
        initialized_unix_ms: unix_ms(),
        pid: std::process::id(),
        build: BUILD.get().cloned().unwrap_or_else(|| {
            serde_json::from_str(include_str!(concat!(
                env!("OUT_DIR"),
                "/pioneer-build.json"
            )))
            .expect("compiled build manifest")
        }),
    }
}
pub(crate) fn monotonic_ms() -> u64 {
    CLOCK
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub(crate) fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    #[test]
    fn equal_version_builds_have_distinct_runs() {
        let first = super::new_run();
        let second = super::new_run();
        assert_ne!(first.run_id, second.run_id);
        assert_eq!(first.build, second.build);
        assert_eq!(first.pid, second.pid);
    }
}
