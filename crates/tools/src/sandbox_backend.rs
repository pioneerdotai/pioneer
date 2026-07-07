use crate::ProcessSpawnPlan;
use crate::error::ToolError;
use pioneer_protocol::{
    SandboxBackendKind, SandboxBackendRequirement, TurnExecutionSecuritySnapshot,
    TurnFilesystemAccess, TurnFilesystemSandboxEntry, TurnFilesystemSandboxKind,
    TurnFilesystemSandboxPath, TurnNetworkMode,
};
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

        let capability_plan = match build_nono_capability_plan(request.snapshot) {
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
                "nono capability plan: read_roots={}, write_roots={}, network_blocked={}",
                capability_plan.read_roots.len(),
                capability_plan.write_roots.len(),
                capability_plan.network_blocked
            )],
        })
    }
}

pub fn build_nono_capability_plan(
    snapshot: &TurnExecutionSecuritySnapshot,
) -> Result<NonoCapabilityPlan, ToolError> {
    let cwd = PathBuf::from(snapshot.sandbox.cwd.as_str());
    if snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted {
        return Ok(NonoCapabilityPlan {
            read_roots: Vec::new(),
            write_roots: Vec::new(),
            cwd,
            network_blocked: false,
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
        cwd,
        network_blocked: snapshot.network.mode != TurnNetworkMode::Enabled,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn configure_nono_command(
    command: &mut tokio::process::Command,
    snapshot: &TurnExecutionSecuritySnapshot,
) -> Result<(), ToolError> {
    if snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted {
        return Ok(());
    }

    let capability_set = build_nono_capability_set(snapshot)?;
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

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn configure_nono_command(
    _command: &mut tokio::process::Command,
    _snapshot: &TurnExecutionSecuritySnapshot,
) -> Result<(), ToolError> {
    Err(ToolError::Rejected(
        "nono backend is not supported on this platform".to_owned(),
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn build_nono_capability_set(
    snapshot: &TurnExecutionSecuritySnapshot,
) -> Result<nono::CapabilitySet, ToolError> {
    let plan = build_nono_capability_plan(snapshot)?;
    let mut capabilities = nono::CapabilitySet::new();
    for path in &plan.read_roots {
        capabilities = capabilities
            .allow_path(path, nono::AccessMode::Read)
            .map_err(|error| ToolError::Rejected(format!("nono read root rejected: {error}")))?;
    }
    for path in &plan.write_roots {
        capabilities = capabilities
            .allow_path(path, nono::AccessMode::ReadWrite)
            .map_err(|error| ToolError::Rejected(format!("nono write root rejected: {error}")))?;
    }
    if plan.network_blocked {
        capabilities = capabilities.block_network();
    }
    Ok(capabilities)
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
