use crate::ProcessSpawnPlan;
use crate::error::ToolError;
use pioneer_protocol::{
    SandboxBackendKind, SandboxBackendRequirement, TurnExecutionSecuritySnapshot,
    TurnFilesystemAccess, TurnFilesystemSandboxEntry, TurnFilesystemSandboxKind,
    TurnFilesystemSandboxPath, TurnNetworkMode, TurnTmpMode,
};
use std::collections::BTreeSet;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct NativeSandboxRequest<'a> {
    pub snapshot: &'a TurnExecutionSecuritySnapshot,
    pub process_plan: &'a ProcessSpawnPlan,
    pub workspace_roots: &'a [PathBuf],
    pub execution_label: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeSandboxPreparedSpawn {
    pub backend: SandboxBackendKind,
    pub process_plan: ProcessSpawnPlan,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeSandboxPrepareOutcome {
    Ready(NativeSandboxPreparedSpawn),
    Degraded {
        backend: SandboxBackendKind,
        reason: String,
    },
    Unavailable {
        backend: SandboxBackendKind,
        reason: String,
    },
}

pub trait NativeSandboxBackend {
    fn kind(&self) -> SandboxBackendKind;

    fn prepare(&self, request: &NativeSandboxRequest<'_>) -> NativeSandboxPrepareOutcome;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonoBackendSupport {
    pub supported: bool,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonoCapabilityPlan {
    pub read_roots: Vec<PathBuf>,
    pub write_roots: Vec<PathBuf>,
    /// Backend-only paths required to load executables, dynamic libraries,
    /// language toolchains, and other non-secret runtime dependencies. These
    /// do not become model-visible filesystem grants in the turn snapshot.
    pub runtime_read_paths: Vec<PathBuf>,
    /// Backend-only scratch/artifact paths required by normal CLI programs.
    pub runtime_write_paths: Vec<PathBuf>,
    pub cwd: PathBuf,
    pub network_blocked: bool,
}

#[derive(Debug, Clone, Default)]
pub struct NonoSandboxBackend {
    support_override: Option<NonoBackendSupport>,
}

impl NonoSandboxBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_support_override_for_tests(support: NonoBackendSupport) -> Self {
        Self {
            support_override: Some(support),
        }
    }

    pub fn support_info(&self) -> NonoBackendSupport {
        self.support_override
            .clone()
            .unwrap_or_else(detect_nono_support)
    }
}

impl NativeSandboxBackend for NonoSandboxBackend {
    fn kind(&self) -> SandboxBackendKind {
        SandboxBackendKind::Nono
    }

    fn prepare(&self, request: &NativeSandboxRequest<'_>) -> NativeSandboxPrepareOutcome {
        let support = self.support_info();
        if !support.supported {
            return NativeSandboxPrepareOutcome::Unavailable {
                backend: SandboxBackendKind::Nono,
                reason: support.details,
            };
        }

        let capability_plan = match build_nono_capability_plan_for_process(
            request.snapshot,
            Some(request.process_plan),
        ) {
            Ok(plan) => plan,
            Err(error) => {
                return NativeSandboxPrepareOutcome::Unavailable {
                    backend: SandboxBackendKind::Nono,
                    reason: error.to_string(),
                };
            }
        };

        NativeSandboxPrepareOutcome::Ready(NativeSandboxPreparedSpawn {
            backend: SandboxBackendKind::Nono,
            process_plan: request.process_plan.clone(),
            notes: vec![format!(
                "nono capability plan: read_roots={}, write_roots={}, runtime_read_paths={}, runtime_write_paths={}, network_blocked={}",
                capability_plan.read_roots.len(),
                capability_plan.write_roots.len(),
                capability_plan.runtime_read_paths.len(),
                capability_plan.runtime_write_paths.len(),
                capability_plan.network_blocked
            )],
        })
    }
}

pub fn build_nono_capability_plan(
    snapshot: &TurnExecutionSecuritySnapshot,
) -> Result<NonoCapabilityPlan, ToolError> {
    build_nono_capability_plan_for_process(snapshot, None)
}

fn build_nono_capability_plan_for_process(
    snapshot: &TurnExecutionSecuritySnapshot,
    process_plan: Option<&ProcessSpawnPlan>,
) -> Result<NonoCapabilityPlan, ToolError> {
    let cwd = PathBuf::from(snapshot.sandbox.cwd.as_str());
    if snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted {
        let network_blocked = snapshot.network.mode != TurnNetworkMode::Enabled;
        return Ok(NonoCapabilityPlan {
            read_roots: Vec::new(),
            // Applying a network-only nono profile still starts from a
            // deny-by-default filesystem profile. Grant the filesystem root
            // explicitly so an independent network restriction cannot
            // accidentally narrow an unrestricted filesystem policy.
            write_roots: if network_blocked {
                vec![PathBuf::from("/")]
            } else {
                Vec::new()
            },
            runtime_read_paths: Vec::new(),
            runtime_write_paths: Vec::new(),
            cwd,
            network_blocked,
        });
    }

    let mut read_roots = Vec::new();
    let mut write_roots = Vec::new();
    for entry in &snapshot.sandbox.filesystem.entries {
        let path = nono_entry_path(snapshot, entry).ok_or_else(|| {
            ToolError::Rejected(format!(
                "nono backend cannot map filesystem entry `{:?}`",
                entry.path
            ))
        })?;
        match entry.access {
            TurnFilesystemAccess::Read => read_roots.push(path),
            TurnFilesystemAccess::Write => write_roots.push(path),
            TurnFilesystemAccess::None => {
                return Err(ToolError::Rejected(format!(
                    "nono backend cannot map filesystem entry `{}` without an access grant",
                    path.display()
                )));
            }
        }
    }

    Ok(NonoCapabilityPlan {
        read_roots,
        write_roots,
        runtime_read_paths: nono_runtime_read_paths(process_plan),
        runtime_write_paths: nono_runtime_write_paths(snapshot, process_plan)?,
        cwd,
        network_blocked: snapshot.network.mode != TurnNetworkMode::Enabled,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn configure_nono_command(
    command: &mut tokio::process::Command,
    snapshot: &TurnExecutionSecuritySnapshot,
    process_plan: &ProcessSpawnPlan,
) -> Result<(), ToolError> {
    if nono_policy_is_noop(snapshot) {
        return Ok(());
    }

    let capability_set = build_nono_capability_set(snapshot, Some(process_plan))?;
    unsafe {
        command.pre_exec(move || {
            nono::Sandbox::apply(&capability_set)
                .map(|_| ())
                .map_err(|error| {
                    std::io::Error::other(format!("failed to apply nono sandbox: {error}"))
                })
        });
    }
    Ok(())
}

fn nono_policy_is_noop(snapshot: &TurnExecutionSecuritySnapshot) -> bool {
    snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted
        && snapshot.network.mode == TurnNetworkMode::Enabled
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn configure_nono_command(
    _command: &mut tokio::process::Command,
    _snapshot: &TurnExecutionSecuritySnapshot,
    _process_plan: &ProcessSpawnPlan,
) -> Result<(), ToolError> {
    Err(ToolError::Rejected(
        "nono backend is not supported on this platform".to_owned(),
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn build_nono_capability_set(
    snapshot: &TurnExecutionSecuritySnapshot,
    process_plan: Option<&ProcessSpawnPlan>,
) -> Result<nono::CapabilitySet, ToolError> {
    let plan = build_nono_capability_plan_for_process(snapshot, process_plan)?;
    let mut capabilities = nono::CapabilitySet::new();
    for path in &plan.read_roots {
        capabilities = allow_nono_path(capabilities, path, nono::AccessMode::Read, "read root")?;
    }
    for path in &plan.write_roots {
        capabilities = allow_nono_path(
            capabilities,
            path,
            nono::AccessMode::ReadWrite,
            "write root",
        )?;
    }
    for path in &plan.runtime_read_paths {
        capabilities = allow_nono_path(
            capabilities,
            path,
            nono::AccessMode::Read,
            "runtime read path",
        )?;
    }
    for path in &plan.runtime_write_paths {
        capabilities = allow_nono_path(
            capabilities,
            path,
            nono::AccessMode::ReadWrite,
            "runtime write path",
        )?;
    }
    if plan.network_blocked {
        capabilities = capabilities.block_network();
    }
    Ok(capabilities)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn allow_nono_path(
    capabilities: nono::CapabilitySet,
    path: &std::path::Path,
    access: nono::AccessMode,
    label: &str,
) -> Result<nono::CapabilitySet, ToolError> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        ToolError::Rejected(format!(
            "nono {label} `{}` is unavailable: {error}",
            path.display()
        ))
    })?;
    let result = if metadata.is_dir() {
        capabilities.allow_path(path, access)
    } else {
        capabilities.allow_file(path, access)
    };
    result.map_err(|error| {
        ToolError::Rejected(format!(
            "nono {label} `{}` was rejected: {error}",
            path.display()
        ))
    })
}

fn nono_runtime_read_paths(process_plan: Option<&ProcessSpawnPlan>) -> Vec<PathBuf> {
    let mut paths = BTreeSet::new();
    for path in platform_runtime_read_paths() {
        insert_existing_path(&mut paths, PathBuf::from(path));
    }

    let Some(process_plan) = process_plan else {
        return paths.into_iter().collect();
    };
    if let Some(path) = process_plan.environment.get("PATH") {
        for entry in std::env::split_paths(path) {
            insert_existing_path(&mut paths, entry);
        }
    }
    for key in ["NODE_PATH"] {
        if let Some(value) = process_plan.environment.get(key) {
            for entry in std::env::split_paths(value) {
                insert_existing_path(&mut paths, entry);
            }
        }
    }
    for key in [
        "RUSTUP_HOME",
        "GOROOT",
        "JAVA_HOME",
        "MAVEN_HOME",
        "GRADLE_HOME",
        "NVM_DIR",
        "NVM_BIN",
        "PNPM_HOME",
        "BUN_INSTALL",
        "VOLTA_HOME",
        "PYENV_ROOT",
        "VIRTUAL_ENV",
        "CONDA_PREFIX",
        "SDKROOT",
        "DEVELOPER_DIR",
    ] {
        if let Some(value) = process_plan.environment.get(key) {
            insert_existing_path(&mut paths, PathBuf::from(value));
        }
    }
    if let Some(cargo_home) = process_plan.environment.get("CARGO_HOME") {
        insert_cargo_runtime_paths(&mut paths, PathBuf::from(cargo_home));
    }
    if let Some(go_path) = process_plan.environment.get("GOPATH") {
        let go_path = PathBuf::from(go_path);
        for relative in ["bin", "pkg"] {
            insert_existing_path(&mut paths, go_path.join(relative));
        }
    }
    if let Some(home) = process_plan.environment.get("HOME") {
        let home = PathBuf::from(home);
        insert_cargo_runtime_paths(&mut paths, home.join(".cargo"));
        for relative in [
            ".rustup",
            ".nvm/versions",
            ".fnm",
            ".local/bin",
            ".local/share/fnm",
            ".local/share/pnpm",
            ".bun",
            ".volta",
            ".pyenv",
            ".conda",
            "Library/pnpm",
        ] {
            insert_existing_path(&mut paths, home.join(relative));
        }
        for relative in ["go/bin", "go/pkg"] {
            insert_existing_path(&mut paths, home.join(relative));
        }
    }

    paths.into_iter().collect()
}

fn insert_cargo_runtime_paths(paths: &mut BTreeSet<PathBuf>, cargo_home: PathBuf) {
    for relative in ["bin", "registry", "git", ".package-cache"] {
        insert_existing_path(paths, cargo_home.join(relative));
    }
}

fn nono_runtime_write_paths(
    snapshot: &TurnExecutionSecuritySnapshot,
    process_plan: Option<&ProcessSpawnPlan>,
) -> Result<Vec<PathBuf>, ToolError> {
    let mut paths = BTreeSet::new();
    for path in platform_runtime_read_write_paths() {
        insert_existing_path(&mut paths, PathBuf::from(path));
    }
    match snapshot.sandbox.tmp.mode {
        TurnTmpMode::Host => {
            insert_existing_path(&mut paths, std::env::temp_dir());
            if let Some(process_plan) = process_plan {
                for key in ["TMPDIR", "TEMP", "TMP"] {
                    if let Some(value) = process_plan.environment.get(key) {
                        insert_existing_path(&mut paths, PathBuf::from(value));
                    }
                }
            }
        }
        TurnTmpMode::Isolated => {
            for root in &snapshot.sandbox.tmp.writable_roots {
                insert_existing_path(&mut paths, PathBuf::from(root));
            }
            if let Some(process_plan) = process_plan {
                let runtime_temp = process_plan.runtime_temp_path().ok_or_else(|| {
                    ToolError::Rejected(
                        "nono isolated tmp policy is missing its private process temp directory"
                            .to_owned(),
                    )
                })?;
                insert_existing_path(&mut paths, runtime_temp.to_path_buf());
            }
        }
    }
    if let Some(process_plan) = process_plan
        && let Some(value) = process_plan.environment.get("PIONEER_ARTIFACT_OUTPUT_DIR")
    {
        insert_existing_path(&mut paths, PathBuf::from(value));
    }
    Ok(paths.into_iter().collect())
}

fn insert_existing_path(paths: &mut BTreeSet<PathBuf>, path: PathBuf) {
    if path.as_os_str().is_empty() || std::fs::metadata(path.as_path()).is_err() {
        return;
    }
    paths.insert(path);
}

#[cfg(target_os = "macos")]
fn platform_runtime_read_paths() -> &'static [&'static str] {
    &[
        "/bin",
        "/sbin",
        "/usr/bin",
        "/usr/sbin",
        "/usr/lib",
        "/usr/libexec",
        "/usr/share",
        "/System/Library",
        "/System/Cryptexes",
        "/Library/Frameworks",
        "/private/etc",
        "/private/var/db/dyld",
        "/private/var/db/timezone",
        "/private/var/db/xcode_select_link",
        "/private/var/select",
        "/Library/Developer/CommandLineTools",
        "/Applications/Xcode.app",
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/opt/homebrew/lib",
        "/opt/homebrew/share",
        "/opt/homebrew/Cellar",
        "/opt/homebrew/opt",
        "/opt/homebrew/Frameworks",
        "/opt/homebrew/Library/Homebrew",
        "/usr/local/bin",
        "/usr/local/sbin",
        "/usr/local/lib",
        "/usr/local/share",
        "/usr/local/Cellar",
        "/usr/local/opt",
        "/usr/local/Frameworks",
        "/usr/local/Homebrew",
        "/nix/store",
        "/dev/zero",
        "/dev/random",
        "/dev/urandom",
    ]
}

#[cfg(target_os = "linux")]
fn platform_runtime_read_paths() -> &'static [&'static str] {
    &[
        "/bin",
        "/sbin",
        "/usr/bin",
        "/usr/sbin",
        "/usr/local/bin",
        "/lib",
        "/lib64",
        "/usr/lib",
        "/usr/lib64",
        "/usr/local/lib",
        "/usr/share",
        "/etc/alternatives",
        "/etc/hosts",
        "/etc/resolv.conf",
        "/etc/nsswitch.conf",
        "/etc/gai.conf",
        "/etc/ld.so.cache",
        "/etc/localtime",
        "/etc/timezone",
        "/etc/os-release",
        "/etc/ssl",
        "/etc/pki",
        "/dev/zero",
        "/dev/random",
        "/dev/urandom",
        "/proc/self",
        "/proc/cpuinfo",
        "/proc/meminfo",
        "/proc/stat",
        "/proc/loadavg",
        "/proc/version",
        "/nix/store",
        "/run/current-system/sw",
    ]
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn platform_runtime_read_write_paths() -> &'static [&'static str] {
    &["/dev/null", "/dev/tty"]
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn platform_runtime_read_write_paths() -> &'static [&'static str] {
    &[]
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn platform_runtime_read_paths() -> &'static [&'static str] {
    &[]
}

fn nono_entry_path(
    snapshot: &TurnExecutionSecuritySnapshot,
    entry: &TurnFilesystemSandboxEntry,
) -> Option<PathBuf> {
    if let Some(path) = entry.resolved_path.as_deref() {
        return Some(PathBuf::from(path));
    }
    match &entry.path {
        TurnFilesystemSandboxPath::CurrentWorkingDirectory
        | TurnFilesystemSandboxPath::WorkspaceRoot => {
            Some(PathBuf::from(snapshot.sandbox.cwd.as_str()))
        }
        TurnFilesystemSandboxPath::ExplicitPath { path } => Some(PathBuf::from(path)),
        TurnFilesystemSandboxPath::SlashTmp | TurnFilesystemSandboxPath::Tmpdir => {
            Some(std::env::temp_dir())
        }
        TurnFilesystemSandboxPath::Root
        | TurnFilesystemSandboxPath::ProjectRoot { .. }
        | TurnFilesystemSandboxPath::RuntimeHome => None,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn detect_nono_support() -> NonoBackendSupport {
    let support = nono::Sandbox::support_info();
    NonoBackendSupport {
        supported: support.is_supported,
        details: support.details,
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn detect_nono_support() -> NonoBackendSupport {
    NonoBackendSupport {
        supported: false,
        details: format!("nono is not supported on {}", std::env::consts::OS),
    }
}

pub fn prepare_native_sandbox_backend<B: NativeSandboxBackend>(
    backend: &B,
    request: &NativeSandboxRequest<'_>,
) -> Result<NativeSandboxPrepareOutcome, ToolError> {
    let outcome = backend.prepare(request);
    validate_native_sandbox_outcome(backend.kind(), request.snapshot, outcome)
}

fn validate_native_sandbox_outcome(
    backend_kind: SandboxBackendKind,
    snapshot: &TurnExecutionSecuritySnapshot,
    outcome: NativeSandboxPrepareOutcome,
) -> Result<NativeSandboxPrepareOutcome, ToolError> {
    if !snapshot_allows_backend(snapshot, backend_kind) {
        return Err(ToolError::Rejected(format!(
            "sandbox backend `{}` cannot broaden resolved snapshot backend selection",
            sandbox_backend_kind_as_str(backend_kind)
        )));
    }

    match &outcome {
        NativeSandboxPrepareOutcome::Ready(prepared) => {
            if prepared.backend != backend_kind
                || !snapshot_allows_backend(snapshot, prepared.backend)
            {
                return Err(ToolError::Rejected(format!(
                    "sandbox backend `{}` returned disallowed prepared backend `{}`",
                    sandbox_backend_kind_as_str(backend_kind),
                    sandbox_backend_kind_as_str(prepared.backend)
                )));
            }
        }
        NativeSandboxPrepareOutcome::Unavailable { backend, reason }
        | NativeSandboxPrepareOutcome::Degraded { backend, reason } => {
            if *backend != backend_kind || !snapshot_allows_backend(snapshot, *backend) {
                return Err(ToolError::Rejected(format!(
                    "sandbox backend `{}` reported disallowed backend `{}`",
                    sandbox_backend_kind_as_str(backend_kind),
                    sandbox_backend_kind_as_str(*backend)
                )));
            }
            if snapshot.sandbox.backend_requirement == SandboxBackendRequirement::Required {
                return Err(ToolError::Rejected(format!(
                    "required sandbox backend `{}` is not ready: {reason}",
                    sandbox_backend_kind_as_str(*backend)
                )));
            }
        }
    }

    Ok(outcome)
}

fn snapshot_allows_backend(
    snapshot: &TurnExecutionSecuritySnapshot,
    backend: SandboxBackendKind,
) -> bool {
    snapshot.backend.sandbox_backend == Some(backend)
        || snapshot
            .sandbox
            .backend_preference
            .iter()
            .any(|candidate| *candidate == backend)
}

fn sandbox_backend_kind_as_str(kind: SandboxBackendKind) -> &'static str {
    match kind {
        SandboxBackendKind::Nono => "nono",
        SandboxBackendKind::WindowsRestrictedToken => "windows_restricted_token",
        SandboxBackendKind::ProviderNative => "provider_native",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        SandboxBackendKind, SandboxBackendRequirement, TurnExecutionSecuritySnapshot,
        TurnFilesystemAccess, TurnFilesystemSandboxEntry, TurnFilesystemSandboxPath,
        TurnPermissionMode, TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
    };
    use std::collections::BTreeMap;

    #[derive(Debug, Clone)]
    struct FakeBackend {
        kind: SandboxBackendKind,
        outcome: NativeSandboxPrepareOutcome,
    }

    impl NativeSandboxBackend for FakeBackend {
        fn kind(&self) -> SandboxBackendKind {
            self.kind
        }

        fn prepare(&self, _request: &NativeSandboxRequest<'_>) -> NativeSandboxPrepareOutcome {
            self.outcome.clone()
        }
    }

    fn process_plan() -> ProcessSpawnPlan {
        ProcessSpawnPlan {
            cwd: PathBuf::from("/tmp/workspace"),
            timeout_ms: 60_000,
            inherit_environment: true,
            environment: BTreeMap::new(),
            removed_environment: Vec::new(),
            runtime_temp_dir: None,
        }
    }

    fn request<'a>(
        snapshot: &'a TurnExecutionSecuritySnapshot,
        plan: &'a ProcessSpawnPlan,
    ) -> NativeSandboxRequest<'a> {
        NativeSandboxRequest {
            snapshot,
            process_plan: plan,
            workspace_roots: &[],
            execution_label: "test",
        }
    }

    #[test]
    fn sandbox_backend_required_unavailable_prevents_spawn() {
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
            "/tmp/workspace",
            Vec::new(),
            1,
        );
        let plan = process_plan();
        let backend = FakeBackend {
            kind: SandboxBackendKind::Nono,
            outcome: NativeSandboxPrepareOutcome::Unavailable {
                backend: SandboxBackendKind::Nono,
                reason: "missing platform primitive".to_owned(),
            },
        };

        let error = prepare_native_sandbox_backend(&backend, &request(&snapshot, &plan))
            .expect_err("required unavailable backend should fail");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("required sandbox backend"))
        );
    }

    #[test]
    fn sandbox_backend_optional_degraded_is_structured_but_allowed() {
        let mut snapshot =
            TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp/workspace", 1);
        snapshot.sandbox.backend_requirement = SandboxBackendRequirement::Optional;
        snapshot.sandbox.backend_preference = vec![SandboxBackendKind::Nono];
        snapshot.backend.sandbox_backend = Some(SandboxBackendKind::Nono);
        let plan = process_plan();
        let backend = FakeBackend {
            kind: SandboxBackendKind::Nono,
            outcome: NativeSandboxPrepareOutcome::Degraded {
                backend: SandboxBackendKind::Nono,
                reason: "running without filesystem restrictions for full access".to_owned(),
            },
        };

        let outcome = prepare_native_sandbox_backend(&backend, &request(&snapshot, &plan))
            .expect("optional degraded backend should be returned as structured outcome");

        assert!(matches!(
            outcome,
            NativeSandboxPrepareOutcome::Degraded { .. }
        ));
    }

    #[test]
    fn nono_maps_snapshot_roots_and_network_to_capability_plan() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace = temp.path().join("workspace");
        let read_only = temp.path().join("read-only");
        std::fs::create_dir_all(workspace.as_path()).expect("create workspace");
        std::fs::create_dir_all(read_only.as_path()).expect("create read-only");
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            workspace.to_string_lossy(),
            vec![
                TurnFilesystemSandboxEntry::workspace_root(
                    TurnFilesystemAccess::Write,
                    workspace.to_string_lossy(),
                ),
                TurnFilesystemSandboxEntry {
                    path: TurnFilesystemSandboxPath::ExplicitPath {
                        path: read_only.to_string_lossy().into_owned(),
                    },
                    access: TurnFilesystemAccess::Read,
                    provenance: pioneer_protocol::TurnSecurityRuleProvenance::Project,
                    resolved_path: Some(read_only.to_string_lossy().into_owned()),
                },
            ],
            1,
        );

        let plan = build_nono_capability_plan(&snapshot).expect("nono plan should build");

        assert_eq!(plan.cwd.as_path(), workspace.as_path());
        assert_eq!(plan.write_roots, vec![workspace.clone()]);
        assert_eq!(plan.read_roots, vec![read_only]);
        assert!(plan.network_blocked);
    }

    #[test]
    fn nono_required_unsupported_fails_before_spawn() {
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            "/tmp/workspace",
            Vec::new(),
            1,
        );
        let plan = process_plan();
        let backend = NonoSandboxBackend::with_support_override_for_tests(NonoBackendSupport {
            supported: false,
            details: "nono support unavailable in test".to_owned(),
        });

        let error = prepare_native_sandbox_backend(&backend, &request(&snapshot, &plan))
            .expect_err("required unavailable nono should fail before spawn");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("required sandbox backend") && message.contains("nono support unavailable"))
        );
    }

    #[test]
    fn nono_full_access_capability_plan_is_unrestricted_noop() {
        let snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp/workspace", 1);

        let plan = build_nono_capability_plan(&snapshot).expect("full access plan should build");

        assert!(plan.read_roots.is_empty());
        assert!(plan.write_roots.is_empty());
        assert!(!plan.network_blocked);
        assert!(nono_policy_is_noop(&snapshot));
    }

    #[test]
    fn nono_unrestricted_filesystem_still_enforces_disabled_network() {
        let mut snapshot =
            TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp/workspace", 1);
        snapshot.network = pioneer_protocol::TurnNetworkPolicySnapshot::disabled();
        snapshot.sandbox.network = snapshot.network.clone();

        let plan = build_nono_capability_plan(&snapshot).expect("nono plan should build");

        assert!(plan.read_roots.is_empty());
        assert_eq!(plan.write_roots, vec![PathBuf::from("/")]);
        assert!(plan.network_blocked);
        assert!(!nono_policy_is_noop(&snapshot));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn nono_restricted_command_loads_runtime_without_exposing_sibling_data() {
        if !nono::Sandbox::is_supported() {
            return;
        }

        let current = std::env::current_dir().expect("test cwd");
        let temp = tempfile::tempdir_in(current).expect("sandbox fixture");
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        let artifact_output = temp.path().join("artifact-output");
        let skill_root = temp.path().join("skills/workspace-a/selected");
        let other_workspace_skill_root = temp.path().join("skills/workspace-b/private");
        std::fs::create_dir_all(workspace.as_path()).expect("workspace");
        std::fs::create_dir_all(outside.as_path()).expect("outside");
        std::fs::create_dir_all(artifact_output.as_path()).expect("artifact output");
        std::fs::create_dir_all(skill_root.as_path()).expect("selected skill root");
        std::fs::create_dir_all(other_workspace_skill_root.as_path())
            .expect("other workspace skill root");
        let outside_file = outside.join("secret.txt");
        std::fs::write(outside_file.as_path(), "secret\n").expect("outside fixture");
        let skill_script = skill_root.join("run.sh");
        std::fs::write(skill_script.as_path(), "selected skill\n").expect("selected skill script");
        let other_workspace_skill = other_workspace_skill_root.join("secret.txt");
        std::fs::write(other_workspace_skill.as_path(), "private skill\n")
            .expect("other workspace skill");

        let mut snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            workspace.to_string_lossy(),
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                workspace.to_string_lossy(),
            )],
            1,
        );
        snapshot
            .sandbox
            .filesystem
            .entries
            .push(TurnFilesystemSandboxEntry {
                path: pioneer_protocol::TurnFilesystemSandboxPath::ExplicitPath {
                    path: artifact_output.to_string_lossy().into_owned(),
                },
                access: TurnFilesystemAccess::Write,
                provenance: pioneer_protocol::TurnSecurityRuleProvenance::Runtime,
                resolved_path: Some(artifact_output.to_string_lossy().into_owned()),
            });
        snapshot
            .sandbox
            .filesystem
            .entries
            .push(TurnFilesystemSandboxEntry {
                path: pioneer_protocol::TurnFilesystemSandboxPath::ExplicitPath {
                    path: skill_root.to_string_lossy().into_owned(),
                },
                access: TurnFilesystemAccess::Read,
                provenance: pioneer_protocol::TurnSecurityRuleProvenance::Runtime,
                resolved_path: Some(skill_root.to_string_lossy().into_owned()),
            });
        let neighboring_temp = tempfile::Builder::new()
            .prefix("pioneer-neighbor-")
            .tempdir()
            .expect("neighboring host temp");
        let neighboring_temp_file = neighboring_temp.path().join("secret.txt");
        std::fs::write(neighboring_temp_file.as_path(), "temp secret\n")
            .expect("neighboring temp fixture");
        let script = "if cat \"$1\" >/dev/null 2>&1; then exit 42; fi; \
             if cat \"$2\" >/dev/null 2>&1; then exit 43; fi; \
             cat \"$3\" >/dev/null 2>&1 || exit 44; \
             if cat \"$4\" >/dev/null 2>&1; then exit 45; fi; \
             test -d \"$TMPDIR\" || exit 46; \
             command -v cargo >/dev/null || exit 47; \
             cargo --version >/dev/null || exit 48; \
             printf 'fn main() {}' > \"$TMPDIR/probe.rs\"; \
             rustc --crate-name pioneer_sandbox_probe \"$TMPDIR/probe.rs\" \
               -o \"$TMPDIR/probe\" || exit 49; \
             \"$TMPDIR/probe\" || exit 50; \
             printf 'temp' > \"$TMPDIR/owned.txt\"; \
             printf 'artifact' > \"$5/output.txt\"; printf 'ok' > created.txt";
        let command_argv = vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            script.to_owned(),
            "sh".to_owned(),
            outside_file.to_string_lossy().into_owned(),
            neighboring_temp_file.to_string_lossy().into_owned(),
            skill_script.to_string_lossy().into_owned(),
            other_workspace_skill.to_string_lossy().into_owned(),
            artifact_output.to_string_lossy().into_owned(),
        ];
        let args = crate::context::ExecCommandArgs {
            command: Some(command_argv.clone()),
            workdir: None,
            timeout_ms: None,
            max_output_tokens: None,
            yield_time_ms: None,
            tty: Some(false),
        };
        let process_plan = crate::build_process_spawn_plan(
            Some(&snapshot),
            workspace.as_path(),
            &args,
            &BTreeMap::new(),
            60_000,
        )
        .expect("isolated process plan");
        let runtime_temp = process_plan
            .runtime_temp_path()
            .expect("private runtime temp")
            .to_path_buf();
        let mut command = tokio::process::Command::new("/bin/sh");
        command.args(&command_argv[1..]);
        command.current_dir(workspace.as_path());
        command.env_clear();
        command.envs(&process_plan.environment);
        configure_nono_command(&mut command, &snapshot, &process_plan).expect("nono configuration");

        let output = command
            .output()
            .await
            .expect("sandboxed command should spawn");
        assert!(
            output.status.success(),
            "sandboxed shell failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join("created.txt")).expect("workspace output"),
            "ok"
        );
        assert_eq!(
            std::fs::read_to_string(runtime_temp.join("owned.txt")).expect("private temp output"),
            "temp"
        );
        assert_eq!(
            std::fs::read_to_string(artifact_output.join("output.txt")).expect("artifact output"),
            "artifact"
        );
        drop(process_plan);
        assert!(
            !runtime_temp.exists(),
            "private temp must be removed after process completion"
        );
    }

    #[test]
    fn sandbox_backend_cannot_broaden_snapshot_backend_selection() {
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
                pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
                pioneer_protocol::TurnPermissionProfileSource::Composer,
            ),
            "/tmp/workspace",
            Vec::new(),
            1,
        );
        let plan = process_plan();
        let backend = FakeBackend {
            kind: SandboxBackendKind::WindowsRestrictedToken,
            outcome: NativeSandboxPrepareOutcome::Ready(NativeSandboxPreparedSpawn {
                backend: SandboxBackendKind::WindowsRestrictedToken,
                process_plan: plan.clone(),
                notes: Vec::new(),
            }),
        };

        let error = prepare_native_sandbox_backend(&backend, &request(&snapshot, &plan))
            .expect_err("backend outside snapshot preference should fail");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("cannot broaden"))
        );
    }
}
