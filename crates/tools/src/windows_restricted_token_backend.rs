use crate::error::ToolError;
use crate::sandbox_backend::{
    NativeSandboxBackend, NativeSandboxPrepareOutcome, NativeSandboxPreparedSpawn,
    NativeSandboxRequest,
};
use pioneer_protocol::{
    SandboxBackendKind, TurnExecutionSecuritySnapshot, TurnFilesystemAccess,
    TurnFilesystemSandboxEntry, TurnFilesystemSandboxKind, TurnFilesystemSandboxPath,
    TurnNetworkMode, TurnTmpMode,
};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsRestrictedTokenSupport {
    pub supported: bool,
    pub details: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowsWorkspaceGrantAccess {
    Read,
    ReadWrite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsWorkspaceGrant {
    pub path: PathBuf,
    pub access: WindowsWorkspaceGrantAccess,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsRestrictedTokenPlan {
    pub restricted_token: bool,
    pub job_object: bool,
    pub private_desktop: bool,
    pub workspace_grants: Vec<WindowsWorkspaceGrant>,
    pub network_block_requested: bool,
    pub tmp_isolation_requested: bool,
    pub environment_inherits_host: bool,
    pub environment_vars: Vec<String>,
    pub removed_environment_vars: Vec<String>,
    pub unsupported_required_features: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct WindowsRestrictedTokenBackend {
    support_override: Option<WindowsRestrictedTokenSupport>,
}

impl WindowsRestrictedTokenBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_support_override_for_tests(support: WindowsRestrictedTokenSupport) -> Self {
        Self {
            support_override: Some(support),
        }
    }

    pub fn support_info(&self) -> WindowsRestrictedTokenSupport {
        self.support_override
            .clone()
            .unwrap_or_else(platform::support_info)
    }
}

impl NativeSandboxBackend for WindowsRestrictedTokenBackend {
    fn kind(&self) -> SandboxBackendKind {
        SandboxBackendKind::WindowsRestrictedToken
    }

    fn prepare(&self, request: &NativeSandboxRequest<'_>) -> NativeSandboxPrepareOutcome {
        let support = self.support_info();
        if !support.supported {
            return NativeSandboxPrepareOutcome::Unavailable {
                backend: SandboxBackendKind::WindowsRestrictedToken,
                reason: support.details,
            };
        }

        let plan = match build_windows_restricted_token_plan(request.snapshot, request.process_plan)
        {
            Ok(plan) => plan,
            Err(error) => {
                return NativeSandboxPrepareOutcome::Unavailable {
                    backend: SandboxBackendKind::WindowsRestrictedToken,
                    reason: error.to_string(),
                };
            }
        };
        if !plan.unsupported_required_features.is_empty() {
            return NativeSandboxPrepareOutcome::Unavailable {
                backend: SandboxBackendKind::WindowsRestrictedToken,
                reason: format!(
                    "windows restricted-token backend cannot enforce required policy features: {}",
                    plan.unsupported_required_features.join(", ")
                ),
            };
        }

        NativeSandboxPrepareOutcome::Ready(NativeSandboxPreparedSpawn {
            backend: SandboxBackendKind::WindowsRestrictedToken,
            process_plan: request.process_plan.clone(),
            notes: vec![format!(
                "windows restricted-token plan: grants={}, network_block_requested={}, tmp_isolation_requested={}",
                plan.workspace_grants.len(),
                plan.network_block_requested,
                plan.tmp_isolation_requested
            )],
        })
    }
}

pub fn build_windows_restricted_token_plan(
    snapshot: &TurnExecutionSecuritySnapshot,
    process_plan: &crate::ProcessSpawnPlan,
) -> Result<WindowsRestrictedTokenPlan, ToolError> {
    let unrestricted = snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted;
    let network_block_requested = snapshot.network.mode != TurnNetworkMode::Enabled;
    let tmp_isolation_requested = snapshot.sandbox.tmp.mode == TurnTmpMode::Isolated;
    let mut unsupported_required_features = Vec::new();
    let mut workspace_grants = Vec::new();

    if !unrestricted {
        for entry in &snapshot.sandbox.filesystem.entries {
            let path = windows_entry_path(snapshot, entry).ok_or_else(|| {
                ToolError::Rejected(format!(
                    "windows restricted-token backend cannot map filesystem entry `{:?}`",
                    entry.path
                ))
            })?;
            let access = match entry.access {
                TurnFilesystemAccess::Read => WindowsWorkspaceGrantAccess::Read,
                TurnFilesystemAccess::Write => WindowsWorkspaceGrantAccess::ReadWrite,
                TurnFilesystemAccess::None => {
                    unsupported_required_features.push(format!(
                        "filesystem entry `{}` has no access grant",
                        path.display()
                    ));
                    continue;
                }
            };
            workspace_grants.push(WindowsWorkspaceGrant { path, access });
        }
    }

    if network_block_requested {
        unsupported_required_features.push(match snapshot.network.mode {
            TurnNetworkMode::Disabled => {
                "network disabled policy has no native restricted-token mechanism yet".to_owned()
            }
            TurnNetworkMode::Restricted => {
                "restricted domain network policy has no native restricted-token mechanism yet"
                    .to_owned()
            }
            TurnNetworkMode::Enabled => unreachable!("network block is only requested otherwise"),
        });
    }
    if tmp_isolation_requested && snapshot.sandbox.tmp.writable_roots.is_empty() {
        unsupported_required_features.push(
            "isolated tmp policy needs an explicit Windows temp root before spawn".to_owned(),
        );
    }

    Ok(WindowsRestrictedTokenPlan {
        restricted_token: !unrestricted,
        job_object: !unrestricted,
        private_desktop: !unrestricted,
        workspace_grants,
        network_block_requested,
        tmp_isolation_requested,
        environment_inherits_host: process_plan.inherit_environment,
        environment_vars: process_plan.environment.keys().cloned().collect(),
        removed_environment_vars: process_plan.removed_environment.clone(),
        unsupported_required_features,
    })
}

pub fn configure_windows_restricted_token_command(
    _command: &mut tokio::process::Command,
    _snapshot: &TurnExecutionSecuritySnapshot,
    _process_plan: &crate::ProcessSpawnPlan,
) -> Result<(), ToolError> {
    Err(ToolError::Rejected(
        "windows restricted-token backend requires a dedicated Windows process runner before spawn"
            .to_owned(),
    ))
}

fn windows_entry_path(
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

#[cfg(target_os = "windows")]
mod platform {
    use super::WindowsRestrictedTokenSupport;

    pub fn support_info() -> WindowsRestrictedTokenSupport {
        WindowsRestrictedTokenSupport {
            supported: true,
            details: "native Windows restricted-token APIs are available".to_owned(),
        }
    }
}

#[cfg(not(target_os = "windows"))]
mod platform {
    use super::WindowsRestrictedTokenSupport;

    pub fn support_info() -> WindowsRestrictedTokenSupport {
        WindowsRestrictedTokenSupport {
            supported: false,
            details: format!(
                "windows restricted-token backend is not supported on {}",
                std::env::consts::OS
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProcessSpawnPlan;
    use pioneer_protocol::{
        TurnFilesystemSandboxEntry, TurnNetworkPolicySnapshot, TurnPermissionMode,
        TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnTmpMode, TurnTmpPolicy,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn process_plan() -> ProcessSpawnPlan {
        let mut environment = BTreeMap::new();
        environment.insert("PATH".to_owned(), "C:\\Windows\\System32".to_owned());
        ProcessSpawnPlan {
            cwd: PathBuf::from("C:\\work"),
            timeout_ms: 60_000,
            inherit_environment: false,
            environment,
            removed_environment: vec!["SECRET_TOKEN".to_owned()],
        }
    }

    fn windows_ready_snapshot() -> TurnExecutionSecuritySnapshot {
        let mut snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            "C:\\work",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                "C:\\work",
            )],
            1,
        );
        snapshot.network = TurnNetworkPolicySnapshot::enabled();
        snapshot.sandbox.network = TurnNetworkPolicySnapshot::enabled();
        snapshot.sandbox.tmp = TurnTmpPolicy {
            mode: TurnTmpMode::Host,
            writable_roots: Vec::new(),
        };
        snapshot.backend.sandbox_backend = Some(SandboxBackendKind::WindowsRestrictedToken);
        snapshot.sandbox.backend_preference = vec![SandboxBackendKind::WindowsRestrictedToken];
        snapshot
    }

    #[test]
    fn windows_sandbox_plan_models_restricted_token_grants_and_environment() {
        let snapshot = windows_ready_snapshot();
        let plan = build_windows_restricted_token_plan(&snapshot, &process_plan())
            .expect("windows plan should build");

        assert!(plan.restricted_token);
        assert!(plan.job_object);
        assert!(plan.private_desktop);
        assert_eq!(plan.workspace_grants.len(), 1);
        assert_eq!(
            plan.workspace_grants[0].access,
            WindowsWorkspaceGrantAccess::ReadWrite
        );
        assert!(!plan.environment_inherits_host);
        assert_eq!(plan.environment_vars, vec!["PATH".to_owned()]);
        assert_eq!(
            plan.removed_environment_vars,
            vec!["SECRET_TOKEN".to_owned()]
        );
        assert!(plan.unsupported_required_features.is_empty());
    }

    #[test]
    fn windows_sandbox_plan_flags_unsupported_network_and_tmp_policy() {
        let snapshot = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            "C:\\work",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                "C:\\work",
            )],
            1,
        );

        let plan = build_windows_restricted_token_plan(&snapshot, &process_plan())
            .expect("windows plan should build with visible unsupported features");

        assert!(plan.network_block_requested);
        assert!(plan.tmp_isolation_requested);
        assert!(
            plan.unsupported_required_features
                .iter()
                .any(|reason| reason.contains("network disabled policy"))
        );
        assert!(
            plan.unsupported_required_features
                .iter()
                .any(|reason| reason.contains("isolated tmp policy"))
        );
    }

    #[test]
    fn windows_sandbox_backend_reports_non_windows_stub_as_unavailable() {
        let snapshot = windows_ready_snapshot();
        let plan = process_plan();
        let backend = WindowsRestrictedTokenBackend::with_support_override_for_tests(
            WindowsRestrictedTokenSupport {
                supported: false,
                details: "not on windows".to_owned(),
            },
        );
        let request = NativeSandboxRequest {
            snapshot: &snapshot,
            process_plan: &plan,
            workspace_roots: &[],
            execution_label: "test",
        };

        let outcome = backend.prepare(&request);

        assert!(matches!(
            outcome,
            NativeSandboxPrepareOutcome::Unavailable { reason, .. } if reason == "not on windows"
        ));
    }

    #[test]
    fn windows_sandbox_backend_returns_ready_when_supported_and_plan_complete() {
        let snapshot = windows_ready_snapshot();
        let plan = process_plan();
        let backend = WindowsRestrictedTokenBackend::with_support_override_for_tests(
            WindowsRestrictedTokenSupport {
                supported: true,
                details: "windows".to_owned(),
            },
        );
        let request = NativeSandboxRequest {
            snapshot: &snapshot,
            process_plan: &plan,
            workspace_roots: &[],
            execution_label: "test",
        };

        let outcome = backend.prepare(&request);

        assert!(matches!(outcome, NativeSandboxPrepareOutcome::Ready(_)));
    }
}
