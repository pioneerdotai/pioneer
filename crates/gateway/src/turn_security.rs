use anyhow::{Context, Result, bail};
use pioneer_cli_agent_runtime::capabilities::{
    CLIAgentRuntimeProviderKind, CLIRuntimeProviderCapabilities, cli_runtime_provider_capabilities,
};
use pioneer_protocol::{
    AgentExecutionBackend, BackendSecurityCapabilities, CLIAgentRuntimeKind, SandboxBackendKind,
    SandboxBackendRequirement, TaskAgentSecurityCap, TurnCommandRiskPolicy, TurnEnvironmentPolicy,
    TurnExecutionSecuritySnapshot, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
    TurnFilesystemSandboxKind, TurnFilesystemSandboxPath, TurnNetworkMode,
    TurnNetworkPolicySnapshot, TurnPermissionMode, TurnPermissionProfileSelection,
    TurnPermissionProfileSnapshot, TurnProcessPolicySnapshot, TurnProcessTimeoutPolicy,
    TurnSandboxMode, TurnSecurityBackendSnapshot, TurnSecurityCapabilityKind,
    TurnSecurityDegradation, TurnSecurityEnforcementStatus, TurnSecurityExecutionBackendKind,
    TurnSecurityParentCapSnapshot, TurnSecurityRuleProvenance, TurnShellPolicy, TurnStartParams,
};
use std::collections::BTreeSet;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TurnSecurityWorkspaceTrust {
    Trusted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnSecurityManagedPolicyInput {
    pub workspace_trust: TurnSecurityWorkspaceTrust,
    pub force_supervised: bool,
    pub force_network_disabled: bool,
}

impl Default for TurnSecurityManagedPolicyInput {
    fn default() -> Self {
        Self {
            workspace_trust: TurnSecurityWorkspaceTrust::Trusted,
            force_supervised: false,
            force_network_disabled: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TurnSecurityResolverExecutionBackend {
    NativeApiProvider { provider: String },
    CodexCli { runtime_id: String },
    ClaudeCli { runtime_id: String },
}

impl TurnSecurityResolverExecutionBackend {
    #[cfg(test)]
    pub(crate) fn kind(&self) -> TurnSecurityExecutionBackendKind {
        match self {
            Self::NativeApiProvider { .. } => TurnSecurityExecutionBackendKind::Native,
            Self::CodexCli { .. } => TurnSecurityExecutionBackendKind::CodexCli,
            Self::ClaudeCli { .. } => TurnSecurityExecutionBackendKind::ClaudeCli,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnSecurityResolverInputContext {
    pub workspace_id: String,
    pub workspace_root: Option<PathBuf>,
    pub project_roots: Vec<PathBuf>,
    pub app_read_roots: Vec<PathBuf>,
    pub effective_model_provider: String,
    pub resolved_permission_profile: TurnPermissionProfileSnapshot,
    pub parent_cap: Option<TurnSecurityParentCapSnapshot>,
    pub managed_policy: TurnSecurityManagedPolicyInput,
    pub created_at_unix_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnSecurityResolverInput {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub workspace_root: PathBuf,
    pub project_roots: Vec<PathBuf>,
    pub app_read_roots: Vec<PathBuf>,
    pub composer_permission_selection: Option<TurnPermissionProfileSelection>,
    pub resolved_permission_profile: TurnPermissionProfileSnapshot,
    pub execution_backend: TurnSecurityResolverExecutionBackend,
    pub parent_cap: Option<TurnSecurityParentCapSnapshot>,
    pub managed_policy: TurnSecurityManagedPolicyInput,
    pub created_at_unix_ms: i64,
}

impl TurnSecurityResolverInput {
    pub(crate) fn from_turn_start_params(
        params: &TurnStartParams,
        context: TurnSecurityResolverInputContext,
    ) -> Result<Self> {
        let thread_id = required_trimmed("thread_id", params.thread_id.as_str())?;
        let turn_id = required_trimmed("turn_id", params.turn_id.as_str())?;
        let workspace_id = required_trimmed("workspace_id", context.workspace_id.as_str())?;
        let workspace_root = required_path("workspace_root", context.workspace_root)?;
        let project_roots = context
            .project_roots
            .into_iter()
            .map(|path| validate_path("project_root", path))
            .collect::<Result<Vec<_>>>()?;
        let app_read_roots = context
            .app_read_roots
            .into_iter()
            .map(|path| validate_path("app_read_root", path))
            .collect::<Result<Vec<_>>>()?;
        let execution_backend = resolve_execution_backend(
            params.execution_backend.as_ref(),
            context.effective_model_provider.as_str(),
        )?;

        Ok(Self {
            workspace_id,
            thread_id,
            turn_id,
            workspace_root,
            project_roots,
            app_read_roots,
            composer_permission_selection: params.permission_profile.clone(),
            resolved_permission_profile: context.resolved_permission_profile,
            execution_backend,
            parent_cap: context.parent_cap,
            managed_policy: context.managed_policy,
            created_at_unix_ms: context.created_at_unix_ms,
        })
    }
}

pub(crate) fn resolve_turn_execution_security(
    input: &TurnSecurityResolverInput,
) -> Result<TurnExecutionSecuritySnapshot> {
    let mode = effective_permission_mode(input);
    let permission_profile = input.resolved_permission_profile.clone();
    let cwd = input.workspace_root.to_string_lossy().into_owned();
    let created_at_unix_ms = input.created_at_unix_ms;

    let mut snapshot = match mode {
        TurnPermissionMode::FullAccess => {
            TurnExecutionSecuritySnapshot::unrestricted_full_access(cwd, created_at_unix_ms)
        }
        TurnPermissionMode::AutoAcceptEdits => TurnExecutionSecuritySnapshot::workspace_write(
            permission_profile,
            cwd,
            filesystem_entries(input, TurnFilesystemAccess::Write),
            created_at_unix_ms,
        ),
        TurnPermissionMode::Supervised => TurnExecutionSecuritySnapshot::read_only(
            permission_profile,
            cwd,
            filesystem_entries(input, TurnFilesystemAccess::Read),
            created_at_unix_ms,
        ),
    };
    snapshot.permission_profile = input.resolved_permission_profile.clone();

    Ok(apply_turn_security_backend_capabilities(snapshot, input))
}

pub(crate) fn task_security_cap_from_snapshot(
    snapshot: &TurnExecutionSecuritySnapshot,
) -> TaskAgentSecurityCap {
    TaskAgentSecurityCap {
        max_permission_profile: pioneer_protocol::task_permission_cap_from_snapshot(
            &snapshot.permission_profile,
        ),
        max_filesystem_entries: task_cap_filesystem_entries(snapshot),
        max_network_policy: snapshot.network.clone(),
        max_sandbox_mode: snapshot.sandbox.mode,
        max_process_policy: snapshot.process.clone(),
    }
}

pub(crate) fn resolve_task_child_execution_security(
    parent_turn_id: &str,
    parent_snapshot: &TurnExecutionSecuritySnapshot,
    task_cap: &TaskAgentSecurityCap,
    child_permission_profile: TurnPermissionProfileSnapshot,
    effective_model_provider: impl Into<String>,
    child_thread_id: impl Into<String>,
    child_turn_id: impl Into<String>,
    created_at_unix_ms: i64,
) -> Result<TurnExecutionSecuritySnapshot> {
    validate_task_security_cap_within_parent(parent_snapshot, task_cap)?;

    let cap_profile =
        pioneer_protocol::task_permission_cap_snapshot(&task_cap.max_permission_profile);
    let parent_profile = pioneer_protocol::inherited_turn_permission_profile_from_snapshot(
        &parent_snapshot.permission_profile,
    );
    let child_permission_profile = pioneer_protocol::intersect_turn_permission_profiles(
        &child_permission_profile,
        &cap_profile,
        pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
    );
    let child_permission_profile = pioneer_protocol::intersect_turn_permission_profiles(
        &child_permission_profile,
        &parent_profile,
        pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
    );

    let requested_sandbox_mode = sandbox_mode_for_permission_mode(child_permission_profile.mode);
    let sandbox_mode = most_restrictive_sandbox_mode(
        most_restrictive_sandbox_mode(parent_snapshot.sandbox.mode, task_cap.max_sandbox_mode),
        requested_sandbox_mode,
    );
    let requested_network_policy =
        network_policy_for_permission_mode(child_permission_profile.mode);
    let network = intersect_network_policies(
        &intersect_network_policies(&parent_snapshot.network, &task_cap.max_network_policy),
        &requested_network_policy,
    );
    let requested_process_policy =
        process_policy_for_permission_mode(child_permission_profile.mode);
    let process = intersect_process_policies(
        &intersect_process_policies(&parent_snapshot.process, &task_cap.max_process_policy),
        &requested_process_policy,
    );
    let cwd = parent_snapshot.sandbox.cwd.clone();
    let child_thread_id = child_thread_id.into();
    let child_turn_id = child_turn_id.into();

    let mut snapshot = match sandbox_mode {
        TurnSandboxMode::Unrestricted => {
            TurnExecutionSecuritySnapshot::unrestricted_full_access(cwd.clone(), created_at_unix_ms)
        }
        TurnSandboxMode::WorkspaceWrite => TurnExecutionSecuritySnapshot::workspace_write(
            child_permission_profile.clone(),
            cwd.clone(),
            intersect_filesystem_entries(parent_snapshot, task_cap, TurnFilesystemAccess::Write)?,
            created_at_unix_ms,
        ),
        TurnSandboxMode::ReadOnly => TurnExecutionSecuritySnapshot::read_only(
            child_permission_profile.clone(),
            cwd.clone(),
            intersect_filesystem_entries(parent_snapshot, task_cap, TurnFilesystemAccess::Read)?,
            created_at_unix_ms,
        ),
    };
    snapshot.source = pioneer_protocol::TurnSecuritySnapshotSource::TaskInherited;
    snapshot.permission_profile = child_permission_profile;
    snapshot.network = network.clone();
    snapshot.sandbox.network = network;
    snapshot.process = process;
    snapshot.parent_cap = Some(TurnSecurityParentCapSnapshot {
        parent_turn_id: parent_turn_id.to_owned(),
        max_permission_profile: task_cap.max_permission_profile.clone(),
        max_filesystem_entries: task_cap.max_filesystem_entries.clone(),
        max_network_policy: task_cap.max_network_policy.clone(),
        max_sandbox_mode: task_cap.max_sandbox_mode,
    });

    let resolver_input = TurnSecurityResolverInput {
        workspace_id: parent_snapshot.sandbox.cwd.clone(),
        thread_id: child_thread_id,
        turn_id: child_turn_id,
        workspace_root: PathBuf::from(cwd),
        project_roots: Vec::new(),
        app_read_roots: Vec::new(),
        composer_permission_selection: None,
        resolved_permission_profile: snapshot.permission_profile.clone(),
        execution_backend: TurnSecurityResolverExecutionBackend::NativeApiProvider {
            provider: effective_model_provider.into(),
        },
        parent_cap: snapshot.parent_cap.clone(),
        managed_policy: TurnSecurityManagedPolicyInput::default(),
        created_at_unix_ms,
    };
    let snapshot = apply_turn_security_backend_capabilities(snapshot, &resolver_input);
    if let pioneer_protocol::TurnSecurityEnforcementStatus::Unavailable { reason } =
        &snapshot.enforcement
    {
        bail!("task child execution security unavailable: {reason}");
    }
    Ok(snapshot)
}

pub(crate) fn apply_turn_security_backend_capabilities(
    mut snapshot: TurnExecutionSecuritySnapshot,
    input: &TurnSecurityResolverInput,
) -> TurnExecutionSecuritySnapshot {
    let backend = backend_snapshot_for(input);
    snapshot.backend = backend.clone();
    snapshot.enforcement = enforcement_status_for(&snapshot, &backend.capabilities);
    snapshot
}

fn backend_snapshot_for(input: &TurnSecurityResolverInput) -> TurnSecurityBackendSnapshot {
    match &input.execution_backend {
        TurnSecurityResolverExecutionBackend::NativeApiProvider { provider } => {
            if input.resolved_permission_profile.mode == TurnPermissionMode::FullAccess {
                TurnSecurityBackendSnapshot {
                    execution_backend: TurnSecurityExecutionBackendKind::Native,
                    sandbox_backend: None,
                    provider: Some(provider.clone()),
                    capabilities: BackendSecurityCapabilities::unrestricted(),
                }
            } else {
                TurnSecurityBackendSnapshot {
                    execution_backend: TurnSecurityExecutionBackendKind::Native,
                    sandbox_backend: Some(native_sandbox_backend()),
                    provider: Some(provider.clone()),
                    capabilities: BackendSecurityCapabilities::native_sandboxed(),
                }
            }
        }
        TurnSecurityResolverExecutionBackend::CodexCli { runtime_id } => {
            let capabilities =
                cli_runtime_provider_capabilities(CLIAgentRuntimeProviderKind::Codex);
            TurnSecurityBackendSnapshot {
                execution_backend: TurnSecurityExecutionBackendKind::CodexCli,
                sandbox_backend: cli_provider_sandbox_backend(capabilities),
                provider: Some(runtime_id.clone()),
                capabilities: backend_capabilities_from_cli_provider(capabilities),
            }
        }
        TurnSecurityResolverExecutionBackend::ClaudeCli { runtime_id } => {
            let capabilities =
                cli_runtime_provider_capabilities(CLIAgentRuntimeProviderKind::Claude);
            TurnSecurityBackendSnapshot {
                execution_backend: TurnSecurityExecutionBackendKind::ClaudeCli,
                sandbox_backend: cli_provider_sandbox_backend(capabilities),
                provider: Some(runtime_id.clone()),
                capabilities: backend_capabilities_from_cli_provider(capabilities),
            }
        }
    }
}

fn cli_provider_sandbox_backend(
    capabilities: &CLIRuntimeProviderCapabilities,
) -> Option<SandboxBackendKind> {
    capabilities
        .sandbox
        .provider_sandbox_policy
        .is_supported()
        .then_some(SandboxBackendKind::ProviderNative)
}

fn backend_capabilities_from_cli_provider(
    capabilities: &CLIRuntimeProviderCapabilities,
) -> BackendSecurityCapabilities {
    BackendSecurityCapabilities {
        can_enforce_filesystem: capabilities
            .sandbox
            .detailed_filesystem_sandbox
            .is_supported(),
        can_enforce_network: capabilities.sandbox.detailed_network_sandbox.is_supported(),
        can_enforce_process: capabilities.sandbox.detailed_process_sandbox.is_supported(),
        supports_turn_scope_approval: capabilities.approval.turn_scope_approval.is_supported(),
        supports_session_scope_approval: capabilities
            .approval
            .session_scope_approval
            .is_supported(),
        supports_request_permissions: capabilities.approval.request_permissions.is_supported(),
    }
}

fn native_sandbox_backend() -> SandboxBackendKind {
    if cfg!(target_os = "windows") {
        SandboxBackendKind::WindowsRestrictedToken
    } else {
        SandboxBackendKind::Nono
    }
}

fn enforcement_status_for(
    snapshot: &TurnExecutionSecuritySnapshot,
    capabilities: &BackendSecurityCapabilities,
) -> TurnSecurityEnforcementStatus {
    if snapshot.sandbox.mode == TurnSandboxMode::Unrestricted {
        return TurnSecurityEnforcementStatus::Active;
    }

    let mut degraded = Vec::new();
    if !capabilities.can_enforce_filesystem {
        degraded.push(TurnSecurityDegradation {
            capability: TurnSecurityCapabilityKind::Filesystem,
            reason: "backend cannot enforce the resolved filesystem sandbox".to_owned(),
        });
    }
    if snapshot.network.mode == pioneer_protocol::TurnNetworkMode::Disabled
        && !capabilities.can_enforce_network
    {
        degraded.push(TurnSecurityDegradation {
            capability: TurnSecurityCapabilityKind::Network,
            reason: "backend cannot enforce the resolved network policy".to_owned(),
        });
    }
    if !capabilities.can_enforce_process {
        degraded.push(TurnSecurityDegradation {
            capability: TurnSecurityCapabilityKind::Process,
            reason: "backend cannot enforce the resolved process policy".to_owned(),
        });
    }
    if snapshot.approval.request_permissions && !capabilities.supports_request_permissions {
        degraded.push(TurnSecurityDegradation {
            capability: TurnSecurityCapabilityKind::Approval,
            reason: "backend cannot request permissions for this turn".to_owned(),
        });
    }

    if degraded.is_empty() {
        TurnSecurityEnforcementStatus::Active
    } else if snapshot.sandbox.backend_requirement == SandboxBackendRequirement::Required {
        let capabilities = degraded
            .iter()
            .map(|item| format!("{:?}", item.capability).to_lowercase())
            .collect::<Vec<_>>()
            .join(", ");
        TurnSecurityEnforcementStatus::Unavailable {
            reason: format!("required security capabilities unavailable: {capabilities}"),
        }
    } else {
        TurnSecurityEnforcementStatus::PartiallyActive { degraded }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TurnSecurityDiagnosticSummary {
    pub enforcement_status: &'static str,
    pub diagnostic_code: &'static str,
    pub degraded_capabilities: Vec<&'static str>,
}

pub(crate) fn turn_security_diagnostic_summary(
    snapshot: &TurnExecutionSecuritySnapshot,
) -> TurnSecurityDiagnosticSummary {
    let degraded_capabilities = match &snapshot.enforcement {
        TurnSecurityEnforcementStatus::PartiallyActive { degraded } => degraded
            .iter()
            .map(|degradation| security_capability_label(degradation.capability))
            .collect(),
        TurnSecurityEnforcementStatus::Unavailable { reason } => {
            unavailable_capabilities_from_reason(reason)
        }
        TurnSecurityEnforcementStatus::Active => Vec::new(),
    };

    TurnSecurityDiagnosticSummary {
        enforcement_status: enforcement_status_label(&snapshot.enforcement),
        diagnostic_code: security_diagnostic_code(snapshot),
        degraded_capabilities,
    }
}

fn enforcement_status_label(status: &TurnSecurityEnforcementStatus) -> &'static str {
    match status {
        TurnSecurityEnforcementStatus::Active => "active",
        TurnSecurityEnforcementStatus::PartiallyActive { .. } => "partially_active",
        TurnSecurityEnforcementStatus::Unavailable { .. } => "unavailable",
    }
}

fn security_diagnostic_code(snapshot: &TurnExecutionSecuritySnapshot) -> &'static str {
    match &snapshot.enforcement {
        TurnSecurityEnforcementStatus::Active => "sandbox_active",
        TurnSecurityEnforcementStatus::PartiallyActive { .. } => {
            if snapshot.backend.execution_backend == TurnSecurityExecutionBackendKind::Native {
                "native_sandbox_backend_degraded"
            } else {
                "provider_sandbox_capability_degraded"
            }
        }
        TurnSecurityEnforcementStatus::Unavailable { .. } => {
            if snapshot.backend.execution_backend == TurnSecurityExecutionBackendKind::Native {
                "native_sandbox_backend_unavailable"
            } else {
                "provider_sandbox_capability_unavailable"
            }
        }
    }
}

fn unavailable_capabilities_from_reason(reason: &str) -> Vec<&'static str> {
    [
        (TurnSecurityCapabilityKind::Filesystem, "filesystem"),
        (TurnSecurityCapabilityKind::Network, "network"),
        (TurnSecurityCapabilityKind::Process, "process"),
        (TurnSecurityCapabilityKind::Approval, "approval"),
        (
            TurnSecurityCapabilityKind::SandboxBackend,
            "sandbox_backend",
        ),
    ]
    .into_iter()
    .filter_map(|(capability, label)| reason.contains(label).then_some(capability))
    .map(security_capability_label)
    .collect()
}

fn security_capability_label(capability: TurnSecurityCapabilityKind) -> &'static str {
    match capability {
        TurnSecurityCapabilityKind::Filesystem => "filesystem",
        TurnSecurityCapabilityKind::Network => "network",
        TurnSecurityCapabilityKind::Process => "process",
        TurnSecurityCapabilityKind::Approval => "approval",
        TurnSecurityCapabilityKind::SandboxBackend => "sandbox_backend",
    }
}

fn effective_permission_mode(input: &TurnSecurityResolverInput) -> TurnPermissionMode {
    input.resolved_permission_profile.mode
}

fn filesystem_entries(
    input: &TurnSecurityResolverInput,
    access: TurnFilesystemAccess,
) -> Vec<TurnFilesystemSandboxEntry> {
    let mut entries =
        Vec::with_capacity(1 + input.project_roots.len() + input.app_read_roots.len());
    entries.push(TurnFilesystemSandboxEntry::workspace_root(
        access,
        input.workspace_root.to_string_lossy().into_owned(),
    ));
    for project_root in &input.project_roots {
        entries.push(TurnFilesystemSandboxEntry {
            path: TurnFilesystemSandboxPath::ExplicitPath {
                path: project_root.to_string_lossy().into_owned(),
            },
            access,
            provenance: TurnSecurityRuleProvenance::Project,
            resolved_path: Some(project_root.to_string_lossy().into_owned()),
        });
    }
    for app_read_root in &input.app_read_roots {
        entries.push(TurnFilesystemSandboxEntry {
            path: TurnFilesystemSandboxPath::ExplicitPath {
                path: app_read_root.to_string_lossy().into_owned(),
            },
            access: TurnFilesystemAccess::Read,
            provenance: TurnSecurityRuleProvenance::Runtime,
            resolved_path: Some(app_read_root.to_string_lossy().into_owned()),
        });
    }
    entries
}

fn task_cap_filesystem_entries(
    snapshot: &TurnExecutionSecuritySnapshot,
) -> Vec<TurnFilesystemSandboxEntry> {
    if snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted {
        vec![TurnFilesystemSandboxEntry::workspace_root(
            TurnFilesystemAccess::Write,
            snapshot.sandbox.cwd.clone(),
        )]
    } else {
        snapshot.sandbox.filesystem.entries.clone()
    }
}

fn validate_task_security_cap_within_parent(
    parent_snapshot: &TurnExecutionSecuritySnapshot,
    task_cap: &TaskAgentSecurityCap,
) -> Result<()> {
    let narrowed_permission_mode = pioneer_protocol::most_restrictive_turn_permission_mode(
        parent_snapshot.permission_profile.mode,
        task_cap.max_permission_profile.mode,
    );
    if narrowed_permission_mode != task_cap.max_permission_profile.mode {
        bail!("task security cap permission profile exceeds parent turn");
    }
    if most_restrictive_sandbox_mode(parent_snapshot.sandbox.mode, task_cap.max_sandbox_mode)
        != task_cap.max_sandbox_mode
    {
        bail!("task security cap sandbox mode exceeds parent turn");
    }
    if most_restrictive_network_mode(
        parent_snapshot.network.mode,
        task_cap.max_network_policy.mode,
    ) != task_cap.max_network_policy.mode
    {
        bail!("task security cap network policy exceeds parent turn");
    }
    if !process_policy_within(&task_cap.max_process_policy, &parent_snapshot.process) {
        bail!("task security cap process policy exceeds parent turn");
    }
    if parent_snapshot.sandbox.filesystem.kind != TurnFilesystemSandboxKind::Unrestricted {
        for entry in &task_cap.max_filesystem_entries {
            if !filesystem_entry_allowed_by_parent(
                entry,
                parent_snapshot.sandbox.filesystem.entries.as_slice(),
            ) {
                bail!("task security cap filesystem roots exceed parent turn");
            }
        }
    }
    Ok(())
}

fn intersect_filesystem_entries(
    parent_snapshot: &TurnExecutionSecuritySnapshot,
    task_cap: &TaskAgentSecurityCap,
    requested_access: TurnFilesystemAccess,
) -> Result<Vec<TurnFilesystemSandboxEntry>> {
    let parent_entries =
        if parent_snapshot.sandbox.filesystem.kind == TurnFilesystemSandboxKind::Unrestricted {
            task_cap.max_filesystem_entries.clone()
        } else {
            parent_snapshot.sandbox.filesystem.entries.clone()
        };
    let mut entries = Vec::new();
    for cap_entry in &task_cap.max_filesystem_entries {
        let mut narrowed = cap_entry.clone();
        narrowed.provenance = TurnSecurityRuleProvenance::TaskCap;
        narrowed.access = most_restrictive_filesystem_access(cap_entry.access, requested_access);
        if parent_snapshot.sandbox.filesystem.kind != TurnFilesystemSandboxKind::Unrestricted {
            let Some(parent_entry) = matching_parent_filesystem_entry(cap_entry, &parent_entries)
            else {
                bail!("task security cap filesystem roots exceed parent turn");
            };
            narrowed.access =
                most_restrictive_filesystem_access(narrowed.access, parent_entry.access);
            narrowed.resolved_path = cap_entry
                .resolved_path
                .clone()
                .or_else(|| parent_entry.resolved_path.clone());
        }
        if narrowed.access != TurnFilesystemAccess::None {
            entries.push(narrowed);
        }
    }
    if entries.is_empty() {
        bail!("task security cap does not allow any filesystem roots");
    }
    Ok(entries)
}

fn filesystem_entry_allowed_by_parent(
    cap_entry: &TurnFilesystemSandboxEntry,
    parent_entries: &[TurnFilesystemSandboxEntry],
) -> bool {
    matching_parent_filesystem_entry(cap_entry, parent_entries)
        .map(|parent| {
            most_restrictive_filesystem_access(parent.access, cap_entry.access) == cap_entry.access
        })
        .unwrap_or(false)
}

fn matching_parent_filesystem_entry<'a>(
    cap_entry: &TurnFilesystemSandboxEntry,
    parent_entries: &'a [TurnFilesystemSandboxEntry],
) -> Option<&'a TurnFilesystemSandboxEntry> {
    parent_entries.iter().find(|parent| {
        let Some(parent_path) = filesystem_entry_resolved_path(parent) else {
            return false;
        };
        let Some(cap_path) = filesystem_entry_resolved_path(cap_entry) else {
            return false;
        };
        cap_path.starts_with(parent_path)
    })
}

fn filesystem_entry_resolved_path(entry: &TurnFilesystemSandboxEntry) -> Option<PathBuf> {
    entry
        .resolved_path
        .as_ref()
        .map(PathBuf::from)
        .or_else(|| match &entry.path {
            TurnFilesystemSandboxPath::ExplicitPath { path } => Some(PathBuf::from(path)),
            _ => None,
        })
        .map(normalize_path_lexically)
}

fn normalize_path_lexically(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn sandbox_mode_for_permission_mode(mode: TurnPermissionMode) -> TurnSandboxMode {
    match mode {
        TurnPermissionMode::FullAccess => TurnSandboxMode::Unrestricted,
        TurnPermissionMode::AutoAcceptEdits => TurnSandboxMode::WorkspaceWrite,
        TurnPermissionMode::Supervised => TurnSandboxMode::ReadOnly,
    }
}

fn network_policy_for_permission_mode(mode: TurnPermissionMode) -> TurnNetworkPolicySnapshot {
    match mode {
        TurnPermissionMode::FullAccess => TurnNetworkPolicySnapshot::enabled(),
        TurnPermissionMode::AutoAcceptEdits | TurnPermissionMode::Supervised => {
            TurnNetworkPolicySnapshot::disabled()
        }
    }
}

fn process_policy_for_permission_mode(mode: TurnPermissionMode) -> TurnProcessPolicySnapshot {
    match mode {
        TurnPermissionMode::FullAccess => TurnProcessPolicySnapshot::unrestricted(),
        TurnPermissionMode::AutoAcceptEdits | TurnPermissionMode::Supervised => {
            TurnProcessPolicySnapshot::restricted()
        }
    }
}

fn most_restrictive_sandbox_mode(left: TurnSandboxMode, right: TurnSandboxMode) -> TurnSandboxMode {
    match (left, right) {
        (TurnSandboxMode::ReadOnly, _) | (_, TurnSandboxMode::ReadOnly) => {
            TurnSandboxMode::ReadOnly
        }
        (TurnSandboxMode::WorkspaceWrite, _) | (_, TurnSandboxMode::WorkspaceWrite) => {
            TurnSandboxMode::WorkspaceWrite
        }
        (TurnSandboxMode::Unrestricted, TurnSandboxMode::Unrestricted) => {
            TurnSandboxMode::Unrestricted
        }
    }
}

fn most_restrictive_network_mode(left: TurnNetworkMode, right: TurnNetworkMode) -> TurnNetworkMode {
    match (left, right) {
        (TurnNetworkMode::Disabled, _) | (_, TurnNetworkMode::Disabled) => {
            TurnNetworkMode::Disabled
        }
        (TurnNetworkMode::Restricted, _) | (_, TurnNetworkMode::Restricted) => {
            TurnNetworkMode::Restricted
        }
        (TurnNetworkMode::Enabled, TurnNetworkMode::Enabled) => TurnNetworkMode::Enabled,
    }
}

fn most_restrictive_filesystem_access(
    left: TurnFilesystemAccess,
    right: TurnFilesystemAccess,
) -> TurnFilesystemAccess {
    match (left, right) {
        (TurnFilesystemAccess::None, _) | (_, TurnFilesystemAccess::None) => {
            TurnFilesystemAccess::None
        }
        (TurnFilesystemAccess::Read, _) | (_, TurnFilesystemAccess::Read) => {
            TurnFilesystemAccess::Read
        }
        (TurnFilesystemAccess::Write, TurnFilesystemAccess::Write) => TurnFilesystemAccess::Write,
    }
}

fn intersect_network_policies(
    left: &TurnNetworkPolicySnapshot,
    right: &TurnNetworkPolicySnapshot,
) -> TurnNetworkPolicySnapshot {
    let mode = most_restrictive_network_mode(left.mode, right.mode);
    if mode == TurnNetworkMode::Disabled {
        return TurnNetworkPolicySnapshot::disabled();
    }
    if mode == TurnNetworkMode::Enabled {
        return TurnNetworkPolicySnapshot::enabled();
    }
    TurnNetworkPolicySnapshot {
        mode,
        allowed_domains: intersect_optional_allow_lists(
            &left.allowed_domains,
            &right.allowed_domains,
        ),
        denied_domains: union_sorted(&left.denied_domains, &right.denied_domains),
        allow_localhost: left.allow_localhost && right.allow_localhost,
        allow_unix_sockets: left.allow_unix_sockets && right.allow_unix_sockets,
    }
}

fn intersect_process_policies(
    left: &TurnProcessPolicySnapshot,
    right: &TurnProcessPolicySnapshot,
) -> TurnProcessPolicySnapshot {
    TurnProcessPolicySnapshot {
        shell: TurnShellPolicy {
            enabled: left.shell.enabled && right.shell.enabled,
            allow_stdin: left.shell.allow_stdin && right.shell.allow_stdin,
            allow_session_inheritance: left.shell.allow_session_inheritance
                && right.shell.allow_session_inheritance,
        },
        environment: TurnEnvironmentPolicy {
            inherit: left.environment.inherit && right.environment.inherit,
            allowed_vars: intersect_optional_allow_lists(
                &left.environment.allowed_vars,
                &right.environment.allowed_vars,
            ),
            denied_patterns: union_sorted(
                &left.environment.denied_patterns,
                &right.environment.denied_patterns,
            ),
        },
        timeout: TurnProcessTimeoutPolicy {
            max_duration_ms: left
                .timeout
                .max_duration_ms
                .min(right.timeout.max_duration_ms),
        },
        command_risk: TurnCommandRiskPolicy {
            denied_commands: union_sorted(
                &left.command_risk.denied_commands,
                &right.command_risk.denied_commands,
            ),
            allowed_command_families: intersect_optional_allow_lists(
                &left.command_risk.allowed_command_families,
                &right.command_risk.allowed_command_families,
            ),
        },
    }
}

fn process_policy_within(
    child: &TurnProcessPolicySnapshot,
    parent: &TurnProcessPolicySnapshot,
) -> bool {
    (!child.shell.enabled || parent.shell.enabled)
        && (!child.shell.allow_stdin || parent.shell.allow_stdin)
        && (!child.shell.allow_session_inheritance || parent.shell.allow_session_inheritance)
        && (!child.environment.inherit || parent.environment.inherit)
        && child.timeout.max_duration_ms <= parent.timeout.max_duration_ms
}

fn intersect_optional_allow_lists(left: &[String], right: &[String]) -> Vec<String> {
    if left.is_empty() {
        return sorted_unique(right);
    }
    if right.is_empty() {
        return sorted_unique(left);
    }
    let right_set = right.iter().collect::<BTreeSet<_>>();
    left.iter()
        .filter(|value| right_set.contains(value))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn union_sorted(left: &[String], right: &[String]) -> Vec<String> {
    left.iter()
        .chain(right.iter())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn sorted_unique(values: &[String]) -> Vec<String> {
    values
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn resolve_execution_backend(
    requested: Option<&AgentExecutionBackend>,
    effective_model_provider: &str,
) -> Result<TurnSecurityResolverExecutionBackend> {
    match requested {
        Some(AgentExecutionBackend::ApiProvider { provider }) => {
            Ok(TurnSecurityResolverExecutionBackend::NativeApiProvider {
                provider: required_trimmed("api provider", provider.as_str())?,
            })
        }
        Some(AgentExecutionBackend::CLIAgentRuntime {
            runtime_id,
            runtime_kind,
        }) => {
            let runtime_id = required_trimmed("cli runtime id", runtime_id.as_str())?;
            match runtime_kind {
                CLIAgentRuntimeKind::Codex => {
                    Ok(TurnSecurityResolverExecutionBackend::CodexCli { runtime_id })
                }
                CLIAgentRuntimeKind::Claude => {
                    Ok(TurnSecurityResolverExecutionBackend::ClaudeCli { runtime_id })
                }
            }
        }
        Some(AgentExecutionBackend::ACPAgentRuntime { runtime_id }) => {
            bail!(
                "ACP agent runtime `{}` cannot build a turn security resolver input",
                runtime_id
            )
        }
        None => Ok(TurnSecurityResolverExecutionBackend::NativeApiProvider {
            provider: required_trimmed("effective model provider", effective_model_provider)?,
        }),
    }
}

fn required_trimmed(label: &str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        bail!("{label} is required for turn security resolver input");
    }
    Ok(value.to_owned())
}

fn required_path(label: &str, value: Option<PathBuf>) -> Result<PathBuf> {
    let value =
        value.with_context(|| format!("{label} is required for turn security resolver input"))?;
    validate_path(label, value)
}

fn validate_path(label: &str, value: PathBuf) -> Result<PathBuf> {
    if value.as_os_str().is_empty() {
        bail!("{label} cannot be empty for turn security resolver input");
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        CLIAgentRuntimeSandboxPolicy, PermissionBehavior, ToolPermissionPolicySnapshot,
        TurnCLIRuntimeOptions, TurnFilesystemSandboxKind, TurnNetworkMode, TurnPermissionMode,
        TurnPermissionProfileSource,
    };
    use pioneer_tools::{
        FilePolicyChecker, FilePolicyDenyReason, PermissionActionKind, PermissionDecision,
        PermissionEvaluationContext, ProfileToolPermissionEvaluator, ToolPayload,
        ToolPermissionEvaluator, enforce_mcp_network_policy, extract_permission_intent,
    };
    use serde_json::json;

    fn context() -> TurnSecurityResolverInputContext {
        TurnSecurityResolverInputContext {
            workspace_id: "workspace_1".to_owned(),
            workspace_root: Some(PathBuf::from("/tmp/workspace_1")),
            project_roots: vec![PathBuf::from("/tmp/workspace_1/project")],
            app_read_roots: Vec::new(),
            effective_model_provider: "openai".to_owned(),
            resolved_permission_profile: TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
            parent_cap: None,
            managed_policy: TurnSecurityManagedPolicyInput::default(),
            created_at_unix_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn security_resolver_input_builds_from_api_provider_turn_start_params() {
        let params = TurnStartParams {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: None,
            permission_profile: Some(TurnPermissionProfileSelection {
                mode: TurnPermissionMode::Supervised,
            }),
            cli_runtime_options: None,
        };

        let input = TurnSecurityResolverInput::from_turn_start_params(&params, context())
            .expect("resolver input should build");

        assert_eq!(input.workspace_id, "workspace_1");
        assert_eq!(input.thread_id, "thread_1");
        assert_eq!(input.turn_id, "turn_1");
        assert_eq!(input.workspace_root, PathBuf::from("/tmp/workspace_1"));
        assert_eq!(
            input.project_roots,
            vec![PathBuf::from("/tmp/workspace_1/project")]
        );
        assert_eq!(
            input.composer_permission_selection,
            Some(TurnPermissionProfileSelection {
                mode: TurnPermissionMode::Supervised
            })
        );
        assert_eq!(
            input.execution_backend,
            TurnSecurityResolverExecutionBackend::NativeApiProvider {
                provider: "openai".to_owned()
            }
        );
        assert_eq!(
            input.execution_backend.kind(),
            TurnSecurityExecutionBackendKind::Native
        );
    }

    #[test]
    fn security_resolver_input_rejects_missing_workspace_root() {
        let params = TurnStartParams {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        };
        let mut context = context();
        context.workspace_root = None;

        let error = TurnSecurityResolverInput::from_turn_start_params(&params, context)
            .expect_err("missing workspace root should fail");
        assert!(
            format!("{error:#}").contains("workspace_root is required"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn security_resolver_input_maps_cli_backend_without_provider_knobs() {
        let params = TurnStartParams {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: Some(AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "codex_personal".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Codex,
            }),
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: Some(TurnCLIRuntimeOptions {
                sandbox: Some(CLIAgentRuntimeSandboxPolicy(json!({
                    "type": "dangerFullAccess"
                }))),
                effort: Some("high".to_owned()),
                personality: Some("concise".to_owned()),
                summary: None,
                steer_if_active: None,
            }),
        };

        let input = TurnSecurityResolverInput::from_turn_start_params(&params, context())
            .expect("resolver input should build");

        assert_eq!(
            input.execution_backend,
            TurnSecurityResolverExecutionBackend::CodexCli {
                runtime_id: "codex_personal".to_owned()
            }
        );
        assert_eq!(
            input.execution_backend.kind(),
            TurnSecurityExecutionBackendKind::CodexCli
        );
    }

    #[test]
    fn security_resolver_modes_compile_product_modes_to_explicit_sandbox_snapshots() {
        for (mode, expected_sandbox, expected_access, expected_network) in [
            (
                TurnPermissionMode::FullAccess,
                TurnSandboxMode::Unrestricted,
                None,
                TurnNetworkMode::Enabled,
            ),
            (
                TurnPermissionMode::AutoAcceptEdits,
                TurnSandboxMode::WorkspaceWrite,
                Some(TurnFilesystemAccess::Write),
                TurnNetworkMode::Disabled,
            ),
            (
                TurnPermissionMode::Supervised,
                TurnSandboxMode::ReadOnly,
                Some(TurnFilesystemAccess::Read),
                TurnNetworkMode::Disabled,
            ),
        ] {
            let params = TurnStartParams {
                thread_id: "thread_1".to_owned(),
                turn_id: "turn_1".to_owned(),
                input: Vec::new(),
                capabilities: Vec::new(),
                model: None,
                model_provider: None,
                sandbox_policy: None,
                mode: None,
                execution_backend: None,
                reasoning: None,
                permission_profile: Some(TurnPermissionProfileSelection { mode }),
                cli_runtime_options: None,
            };
            let mut context = context();
            context.resolved_permission_profile = TurnPermissionProfileSnapshot::from_mode(
                mode,
                TurnPermissionProfileSource::Composer,
            );
            let input = TurnSecurityResolverInput::from_turn_start_params(&params, context)
                .expect("input should build");

            let snapshot =
                resolve_turn_execution_security(&input).expect("snapshot should resolve");

            assert_eq!(
                snapshot.permission_profile,
                input.resolved_permission_profile
            );
            assert_eq!(snapshot.sandbox.mode, expected_sandbox);
            assert_eq!(snapshot.network.mode, expected_network);
            assert_eq!(snapshot.sandbox.network.mode, expected_network);
            if let Some(expected_access) = expected_access {
                assert_eq!(
                    snapshot.sandbox.filesystem.kind,
                    TurnFilesystemSandboxKind::Restricted
                );
                assert_eq!(snapshot.sandbox.filesystem.entries.len(), 2);
                assert!(
                    snapshot
                        .sandbox
                        .filesystem
                        .entries
                        .iter()
                        .all(|entry| entry.access == expected_access)
                );
            } else {
                assert_eq!(
                    snapshot.sandbox.filesystem.kind,
                    TurnFilesystemSandboxKind::Unrestricted
                );
                assert!(snapshot.sandbox.filesystem.entries.is_empty());
            }
        }
    }

    #[test]
    fn security_resolver_includes_app_read_roots_as_runtime_read_roots() {
        let app_read_root = PathBuf::from("/tmp/pioneer-runtime/skills");
        let mut input = resolver_input_for_mode_and_backend(TurnPermissionMode::Supervised, None);
        input.app_read_roots = vec![app_read_root.clone()];

        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        let app_entry = snapshot
            .sandbox
            .filesystem
            .entries
            .iter()
            .find(|entry| entry.resolved_path.as_deref() == app_read_root.to_str())
            .expect("app read root should be included");
        assert_eq!(app_entry.access, TurnFilesystemAccess::Read);
        assert_eq!(app_entry.provenance, TurnSecurityRuleProvenance::Runtime);
    }

    #[test]
    fn security_resolver_keeps_app_roots_read_only_in_workspace_write_mode() {
        let app_read_root = PathBuf::from("/tmp/pioneer-runtime/skills");
        let mut input =
            resolver_input_for_mode_and_backend(TurnPermissionMode::AutoAcceptEdits, None);
        input.app_read_roots = vec![app_read_root.clone()];

        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        let workspace_entry = snapshot
            .sandbox
            .filesystem
            .entries
            .iter()
            .find(|entry| entry.provenance == TurnSecurityRuleProvenance::Workspace)
            .expect("workspace root should be included");
        assert_eq!(workspace_entry.access, TurnFilesystemAccess::Write);

        let app_entry = snapshot
            .sandbox
            .filesystem
            .entries
            .iter()
            .find(|entry| entry.resolved_path.as_deref() == app_read_root.to_str())
            .expect("app read root should be included");
        assert_eq!(app_entry.access, TurnFilesystemAccess::Read);
        assert_eq!(app_entry.provenance, TurnSecurityRuleProvenance::Runtime);
    }

    #[test]
    fn process_policy_resolver_sets_shell_env_and_timeout_defaults() {
        let full_access = resolve_turn_execution_security(&resolver_input_for_mode_and_backend(
            TurnPermissionMode::FullAccess,
            None,
        ))
        .expect("full access snapshot should resolve");
        assert!(full_access.process.shell.enabled);
        assert!(full_access.process.environment.inherit);
        assert!(full_access.process.environment.denied_patterns.is_empty());
        assert_eq!(full_access.process.timeout.max_duration_ms, 30 * 60 * 1000);
        assert_eq!(
            full_access
                .permission_profile
                .effective_policy
                .shell_command,
            pioneer_protocol::PermissionBehavior::Allow
        );

        let workspace_write = resolve_turn_execution_security(
            &resolver_input_for_mode_and_backend(TurnPermissionMode::AutoAcceptEdits, None),
        )
        .expect("workspace-write snapshot should resolve");
        assert!(workspace_write.process.shell.enabled);
        assert!(workspace_write.process.environment.inherit);
        assert!(
            workspace_write
                .process
                .environment
                .denied_patterns
                .iter()
                .any(|pattern| pattern.contains("TOKEN"))
        );
        assert_eq!(
            workspace_write.process.timeout.max_duration_ms,
            30 * 60 * 1000
        );
        assert_eq!(
            workspace_write
                .permission_profile
                .effective_policy
                .shell_command,
            pioneer_protocol::PermissionBehavior::Ask
        );
    }

    #[test]
    fn network_policy_full_access_resolves_enabled_network() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::FullAccess, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(snapshot.network.mode, TurnNetworkMode::Enabled);
        assert_eq!(snapshot.sandbox.network.mode, TurnNetworkMode::Enabled);
    }

    #[test]
    fn network_policy_restricted_product_modes_resolve_disabled_network() {
        for mode in [
            TurnPermissionMode::Supervised,
            TurnPermissionMode::AutoAcceptEdits,
        ] {
            let input = resolver_input_for_mode_and_backend(mode, None);
            let snapshot =
                resolve_turn_execution_security(&input).expect("snapshot should resolve");

            assert_eq!(snapshot.network.mode, TurnNetworkMode::Disabled);
            assert_eq!(snapshot.sandbox.network.mode, TurnNetworkMode::Disabled);
        }
    }

    #[test]
    fn mcp_policy_read_only_hint_routes_to_mcp_read_permission() {
        let invocation = mcp_invocation(
            Some(true),
            Some(false),
            Some(false),
            serde_json::json!({ "q": "permissions" }),
        );
        let intent = extract_permission_intent(&invocation);
        let context = PermissionEvaluationContext::for_turn(
            "workspace_1",
            "thread_1",
            "turn_1",
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
        );

        assert_eq!(intent.action, PermissionActionKind::McpRead);
        assert_eq!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Allow {
                reason: pioneer_tools::PermissionDecisionReason::PolicyAllowsAction
            }
        );
    }

    #[test]
    fn mcp_policy_unknown_tool_routes_to_write_or_unknown_approval() {
        let invocation =
            mcp_invocation(None, None, None, serde_json::json!({ "path": "README.md" }));
        let intent = extract_permission_intent(&invocation);
        let context = PermissionEvaluationContext::for_turn(
            "workspace_1",
            "thread_1",
            "turn_1",
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
        );

        assert_eq!(intent.action, PermissionActionKind::McpWriteOrUnknown);
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: pioneer_tools::PermissionDecisionReason::UnknownActionDefault,
                ..
            }
        ));
    }

    #[test]
    fn mcp_policy_network_disabled_blocks_open_world_side_effect() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::Supervised, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");
        let classification =
            pioneer_mcp::classify_mcp_tool_policy(pioneer_mcp::McpToolSafetyHints {
                read_only_hint: Some(false),
                destructive_hint: Some(false),
                open_world_hint: Some(true),
            });

        let error =
            enforce_mcp_network_policy(Some(&snapshot), &classification, "resend", "send_email")
                .expect_err("open-world MCP should be blocked when network is disabled");

        assert!(
            matches!(error, pioneer_tools::ToolError::Rejected(ref message) if message.contains("network is disabled")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn unknown_capability_native_tool_uses_unknown_action_default() {
        let invocation = unknown_tool_invocation("mystery_tool");
        let intent = extract_permission_intent(&invocation);
        let context = PermissionEvaluationContext::for_turn(
            "workspace_1",
            "thread_1",
            "turn_1",
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
        );

        assert_eq!(intent.action, PermissionActionKind::Unknown);
        assert!(intent.is_unknown_capability());
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: pioneer_tools::PermissionDecisionReason::UnknownActionDefault,
                ..
            }
        ));
    }

    #[test]
    fn unknown_capability_deny_policy_stops_native_tool() {
        let invocation = unknown_tool_invocation("mystery_tool");
        let intent = extract_permission_intent(&invocation);
        let context = PermissionEvaluationContext::for_turn(
            "workspace_1",
            "thread_1",
            "turn_1",
            TurnPermissionProfileSnapshot {
                mode: TurnPermissionMode::Supervised,
                source: TurnPermissionProfileSource::Composer,
                effective_policy: ToolPermissionPolicySnapshot::all(PermissionBehavior::Deny),
            },
        );

        assert_eq!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Deny {
                reason: pioneer_tools::PermissionDecisionReason::UnknownActionDefault,
                message: "unknown tool capability denied by turn permission profile".to_owned()
            }
        );
    }

    #[test]
    fn unknown_capability_skill_action_uses_unknown_action_default() {
        let invocation = unknown_skill_invocation();
        let intent = extract_permission_intent(&invocation);
        let context = PermissionEvaluationContext::for_turn(
            "workspace_1",
            "thread_1",
            "turn_1",
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::Supervised,
                TurnPermissionProfileSource::Composer,
            ),
        );

        assert_eq!(intent.action, PermissionActionKind::DynamicSkillTool);
        assert!(intent.is_unknown_capability());
        assert!(matches!(
            ProfileToolPermissionEvaluator.evaluate(&context, &invocation, &intent),
            PermissionDecision::Ask {
                reason: pioneer_tools::PermissionDecisionReason::UnknownActionDefault,
                ..
            }
        ));
    }

    #[test]
    fn shell_security_full_access_resolves_unrestricted_process_execution() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::FullAccess, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(snapshot.sandbox.mode, TurnSandboxMode::Unrestricted);
        assert_eq!(snapshot.backend.sandbox_backend, None);
        assert!(snapshot.process.shell.enabled);
        assert!(snapshot.process.environment.inherit);
    }

    #[test]
    fn shell_security_restricted_native_turn_requires_native_sandbox_backend() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::AutoAcceptEdits, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(snapshot.sandbox.mode, TurnSandboxMode::WorkspaceWrite);
        assert_eq!(
            snapshot.sandbox.backend_requirement,
            SandboxBackendRequirement::Required
        );
        assert!(matches!(
            snapshot.backend.sandbox_backend,
            Some(SandboxBackendKind::Nono) | Some(SandboxBackendKind::WindowsRestrictedToken)
        ));
        assert!(snapshot.process.shell.enabled);
        assert!(snapshot.backend.capabilities.can_enforce_process);
    }

    fn resolver_input_for_mode_and_backend(
        mode: TurnPermissionMode,
        execution_backend: Option<AgentExecutionBackend>,
    ) -> TurnSecurityResolverInput {
        let params = TurnStartParams {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend,
            reasoning: None,
            permission_profile: Some(TurnPermissionProfileSelection { mode }),
            cli_runtime_options: None,
        };
        let mut context = context();
        context.resolved_permission_profile =
            TurnPermissionProfileSnapshot::from_mode(mode, TurnPermissionProfileSource::Composer);
        TurnSecurityResolverInput::from_turn_start_params(&params, context)
            .expect("input should build")
    }

    fn mcp_invocation(
        read_only_hint: Option<bool>,
        destructive_hint: Option<bool>,
        open_world_hint: Option<bool>,
        arguments: serde_json::Value,
    ) -> pioneer_tools::ToolInvocation {
        pioneer_tools::ToolInvocation {
            call_id: "call_mcp_policy".to_owned(),
            tool_name: "mcp_resend_send_email".to_owned(),
            source: pioneer_tools::ToolCallSource::Model,
            payload: ToolPayload::Mcp {
                server: "srv_resend".to_owned(),
                tool: "send_email".to_owned(),
                arguments,
                read_only_hint,
                destructive_hint,
                open_world_hint,
            },
            workdir: PathBuf::from("/tmp/workspace_1"),
            environment: std::collections::BTreeMap::new(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: pioneer_tools::ToolRecoveryMetadata::default(),
            permission_metadata: pioneer_tools::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn unknown_tool_invocation(tool_name: &str) -> pioneer_tools::ToolInvocation {
        pioneer_tools::ToolInvocation {
            call_id: "call_unknown_policy".to_owned(),
            tool_name: tool_name.to_owned(),
            source: pioneer_tools::ToolCallSource::Model,
            payload: ToolPayload::Function {
                arguments: serde_json::json!({ "value": true }),
            },
            workdir: PathBuf::from("/tmp/workspace_1"),
            environment: std::collections::BTreeMap::new(),
            attempt_id: 1,
            idempotency_key: None,
            recovery: pioneer_tools::ToolRecoveryMetadata::default(),
            permission_metadata: pioneer_tools::ToolPermissionMetadata::default(),
            execution_security_snapshot: None,
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn unknown_skill_invocation() -> pioneer_tools::ToolInvocation {
        let mut invocation = unknown_tool_invocation("skill_proxy_unknown");
        invocation.permission_metadata = pioneer_tools::ToolPermissionMetadata {
            dynamic_skill: Some(pioneer_tools::DynamicSkillPermissionMetadata {
                kind: pioneer_tools::DynamicSkillPermissionKind::FunctionProxy,
                skill_slug: "user:workspace/proxy".to_owned(),
                source_kind: "User".to_owned(),
                trust_level: "Community".to_owned(),
                target_tool: None,
                configured_method: None,
                configured_url: None,
            }),
        };
        invocation
    }

    #[test]
    fn security_backend_capabilities_native_restricted_snapshot_is_active() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::AutoAcceptEdits, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(
            snapshot.backend.execution_backend,
            TurnSecurityExecutionBackendKind::Native
        );
        assert!(matches!(
            snapshot.backend.sandbox_backend,
            Some(SandboxBackendKind::Nono) | Some(SandboxBackendKind::WindowsRestrictedToken)
        ));
        assert_eq!(snapshot.enforcement, TurnSecurityEnforcementStatus::Active);
    }

    #[test]
    fn sandbox_backend_resolver_selects_required_native_backend_only() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::Supervised, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(
            snapshot.sandbox.backend_requirement,
            SandboxBackendRequirement::Required
        );
        assert!(matches!(
            snapshot.backend.sandbox_backend,
            Some(SandboxBackendKind::Nono) | Some(SandboxBackendKind::WindowsRestrictedToken)
        ));
        assert_eq!(
            snapshot.backend.sandbox_backend,
            snapshot.sandbox.backend_preference.first().copied()
        );
        assert!(snapshot.backend.capabilities.can_enforce_filesystem);
        assert!(snapshot.backend.capabilities.can_enforce_process);
    }

    #[test]
    fn sandbox_backend_resolver_keeps_full_access_optional_and_unrestricted() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::FullAccess, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(
            snapshot.sandbox.backend_requirement,
            SandboxBackendRequirement::Optional
        );
        assert_eq!(snapshot.backend.sandbox_backend, None);
        assert_eq!(snapshot.enforcement, TurnSecurityEnforcementStatus::Active);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn nono_resolver_selects_nono_for_unix_native_restricted_snapshots() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::AutoAcceptEdits, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(
            snapshot.backend.sandbox_backend,
            Some(SandboxBackendKind::Nono)
        );
        assert_eq!(
            snapshot.sandbox.backend_preference,
            vec![SandboxBackendKind::Nono]
        );
        assert_eq!(
            snapshot.sandbox.backend_requirement,
            SandboxBackendRequirement::Required
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_sandbox_resolver_selects_restricted_token_for_native_restricted_snapshots() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::AutoAcceptEdits, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(
            snapshot.backend.sandbox_backend,
            Some(SandboxBackendKind::WindowsRestrictedToken)
        );
        assert_eq!(
            snapshot.sandbox.backend_preference,
            vec![SandboxBackendKind::WindowsRestrictedToken]
        );
        assert_eq!(
            snapshot.sandbox.backend_requirement,
            SandboxBackendRequirement::Required
        );
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn windows_sandbox_resolver_does_not_select_restricted_token_on_non_windows() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::AutoAcceptEdits, None);
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_ne!(
            snapshot.backend.sandbox_backend,
            Some(SandboxBackendKind::WindowsRestrictedToken)
        );
    }

    #[test]
    fn security_backend_capabilities_codex_keeps_resolved_sandbox() {
        let input = resolver_input_for_mode_and_backend(
            TurnPermissionMode::AutoAcceptEdits,
            Some(AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "codex_personal".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Codex,
            }),
        );
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(snapshot.sandbox.mode, TurnSandboxMode::WorkspaceWrite);
        assert_eq!(
            snapshot.backend.execution_backend,
            TurnSecurityExecutionBackendKind::CodexCli
        );
        assert_eq!(
            snapshot.backend.sandbox_backend,
            Some(SandboxBackendKind::ProviderNative)
        );
        assert_eq!(snapshot.enforcement, TurnSecurityEnforcementStatus::Active);
    }

    #[test]
    fn cli_capabilities_codex_provider_descriptor_feeds_backend_snapshot() {
        let input = resolver_input_for_mode_and_backend(
            TurnPermissionMode::Supervised,
            Some(AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "codex_personal".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Codex,
            }),
        );
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(
            snapshot.backend.execution_backend,
            TurnSecurityExecutionBackendKind::CodexCli
        );
        assert_eq!(
            snapshot.backend.sandbox_backend,
            Some(SandboxBackendKind::ProviderNative)
        );
        assert!(snapshot.backend.capabilities.can_enforce_filesystem);
        assert!(snapshot.backend.capabilities.can_enforce_network);
        assert!(snapshot.backend.capabilities.can_enforce_process);
        assert!(snapshot.backend.capabilities.supports_request_permissions);
        assert!(snapshot.backend.capabilities.supports_turn_scope_approval);
        assert!(
            !snapshot
                .backend
                .capabilities
                .supports_session_scope_approval
        );
        assert_eq!(snapshot.enforcement, TurnSecurityEnforcementStatus::Active);
    }

    #[test]
    fn security_backend_capabilities_claude_restricted_snapshot_is_unavailable() {
        let input = resolver_input_for_mode_and_backend(
            TurnPermissionMode::Supervised,
            Some(AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "claude_personal".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Claude,
            }),
        );
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(snapshot.sandbox.mode, TurnSandboxMode::ReadOnly);
        assert_eq!(
            snapshot.backend.execution_backend,
            TurnSecurityExecutionBackendKind::ClaudeCli
        );
        assert_eq!(snapshot.backend.sandbox_backend, None);
        assert!(
            matches!(
                snapshot.enforcement,
                TurnSecurityEnforcementStatus::Unavailable { .. }
            ),
            "Claude must not claim detailed sandbox enforcement"
        );
    }

    #[test]
    fn cli_capabilities_claude_provider_descriptor_marks_detailed_sandbox_unavailable() {
        let input = resolver_input_for_mode_and_backend(
            TurnPermissionMode::AutoAcceptEdits,
            Some(AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "claude_personal".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Claude,
            }),
        );
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        assert_eq!(
            snapshot.backend.execution_backend,
            TurnSecurityExecutionBackendKind::ClaudeCli
        );
        assert_eq!(snapshot.backend.sandbox_backend, None);
        assert!(!snapshot.backend.capabilities.can_enforce_filesystem);
        assert!(!snapshot.backend.capabilities.can_enforce_network);
        assert!(!snapshot.backend.capabilities.can_enforce_process);
        assert!(snapshot.backend.capabilities.supports_request_permissions);
        assert!(snapshot.backend.capabilities.supports_turn_scope_approval);
        assert!(matches!(
            snapshot.enforcement,
            TurnSecurityEnforcementStatus::Unavailable { .. }
        ));
    }

    #[test]
    fn security_diagnostics_claude_unavailable_marks_provider_limitation_without_paths() {
        let input = resolver_input_for_mode_and_backend(
            TurnPermissionMode::Supervised,
            Some(AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "claude_personal".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Claude,
            }),
        );
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");
        let diagnostic = turn_security_diagnostic_summary(&snapshot);

        assert_eq!(diagnostic.enforcement_status, "unavailable");
        assert_eq!(
            diagnostic.diagnostic_code,
            "provider_sandbox_capability_unavailable"
        );
        assert!(
            diagnostic.degraded_capabilities.contains(&"filesystem"),
            "provider limitation should identify missing filesystem enforcement"
        );
        let rendered = format!("{diagnostic:?}");
        assert!(!rendered.contains("/tmp/workspace_1"));
        assert!(!rendered.contains("PATH"));
    }

    #[test]
    fn security_diagnostics_native_unavailable_marks_backend_failure_without_paths() {
        let input = resolver_input_for_mode_and_backend(TurnPermissionMode::AutoAcceptEdits, None);
        let mut snapshot =
            resolve_turn_execution_security(&input).expect("snapshot should resolve");
        snapshot.backend.capabilities = BackendSecurityCapabilities::unrestricted();
        snapshot.enforcement = enforcement_status_for(&snapshot, &snapshot.backend.capabilities);

        let diagnostic = turn_security_diagnostic_summary(&snapshot);

        assert_eq!(diagnostic.enforcement_status, "unavailable");
        assert_eq!(
            diagnostic.diagnostic_code,
            "native_sandbox_backend_unavailable"
        );
        assert!(
            diagnostic.degraded_capabilities.contains(&"filesystem"),
            "native backend failure should identify missing filesystem enforcement"
        );
        let rendered = format!("{diagnostic:?}");
        assert!(!rendered.contains("/tmp/workspace_1"));
        assert!(!rendered.contains("PATH"));
    }

    #[test]
    fn task_security_parent_full_access_can_delegate_full_access_when_cap_allows_it() {
        let parent =
            TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp/task-security-full", 1);
        let cap = task_security_cap_from_snapshot(&parent);
        let child_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::FullAccess,
            TurnPermissionProfileSource::TaskPermissionCap,
        );

        let snapshot = resolve_task_child_execution_security(
            "parent_turn",
            &parent,
            &cap,
            child_profile,
            "openai",
            "child_thread",
            "child_turn",
            2,
        )
        .expect("full access parent should delegate when cap allows it");

        assert_eq!(snapshot.sandbox.mode, TurnSandboxMode::Unrestricted);
        assert_eq!(snapshot.network.mode, TurnNetworkMode::Enabled);
        assert!(snapshot.process.shell.allow_session_inheritance);
        assert_eq!(
            snapshot
                .parent_cap
                .as_ref()
                .expect("parent cap should persist")
                .parent_turn_id,
            "parent_turn"
        );
    }

    #[test]
    fn task_security_parent_full_access_is_narrowed_by_task_cap() {
        let parent =
            TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp/task-security-narrow", 1);
        let mut cap = task_security_cap_from_snapshot(&parent);
        cap.max_permission_profile =
            pioneer_protocol::task_permission_cap_for_mode(TurnPermissionMode::AutoAcceptEdits);
        cap.max_sandbox_mode = TurnSandboxMode::WorkspaceWrite;
        cap.max_network_policy = pioneer_protocol::TurnNetworkPolicySnapshot::disabled();
        cap.max_process_policy = pioneer_protocol::TurnProcessPolicySnapshot::restricted();
        let child_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::FullAccess,
            TurnPermissionProfileSource::TaskPermissionCap,
        );

        let snapshot = resolve_task_child_execution_security(
            "parent_turn",
            &parent,
            &cap,
            child_profile,
            "openai",
            "child_thread",
            "child_turn",
            2,
        )
        .expect("task cap should narrow full access request");

        assert_eq!(
            snapshot.permission_profile.mode,
            TurnPermissionMode::AutoAcceptEdits
        );
        assert_eq!(snapshot.sandbox.mode, TurnSandboxMode::WorkspaceWrite);
        assert_eq!(snapshot.network.mode, TurnNetworkMode::Disabled);
    }

    #[test]
    fn task_security_rejects_cap_that_widens_write_roots() {
        let parent = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            "/tmp/task-security-workspace",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                "/tmp/task-security-workspace",
            )],
            1,
        );
        let mut cap = task_security_cap_from_snapshot(&parent);
        cap.max_filesystem_entries = vec![TurnFilesystemSandboxEntry::workspace_root(
            TurnFilesystemAccess::Write,
            "/tmp/task-security-outside",
        )];

        let error = resolve_task_child_execution_security(
            "parent_turn",
            &parent,
            &cap,
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::TaskPermissionCap,
            ),
            "openai",
            "child_thread",
            "child_turn",
            2,
        )
        .expect_err("outside root must be rejected");

        assert!(format!("{error:#}").contains("filesystem roots exceed parent"));
    }

    #[test]
    fn task_security_rejects_cap_that_widens_network() {
        let parent = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            "/tmp/task-security-network",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                "/tmp/task-security-network",
            )],
            1,
        );
        let mut cap = task_security_cap_from_snapshot(&parent);
        cap.max_network_policy = pioneer_protocol::TurnNetworkPolicySnapshot::enabled();

        let error = resolve_task_child_execution_security(
            "parent_turn",
            &parent,
            &cap,
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::FullAccess,
                TurnPermissionProfileSource::TaskPermissionCap,
            ),
            "openai",
            "child_thread",
            "child_turn",
            2,
        )
        .expect_err("network widening must be rejected");

        assert!(format!("{error:#}").contains("network policy exceeds parent"));
    }

    #[test]
    fn task_security_rejects_cap_that_widens_shell_permissions() {
        let mut parent = TurnExecutionSecuritySnapshot::workspace_write(
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::Composer,
            ),
            "/tmp/task-security-process",
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                "/tmp/task-security-process",
            )],
            1,
        );
        parent.process.shell.enabled = false;
        let mut cap = task_security_cap_from_snapshot(&parent);
        cap.max_process_policy.shell.enabled = true;

        let error = resolve_task_child_execution_security(
            "parent_turn",
            &parent,
            &cap,
            TurnPermissionProfileSnapshot::from_mode(
                TurnPermissionMode::AutoAcceptEdits,
                TurnPermissionProfileSource::TaskPermissionCap,
            ),
            "openai",
            "child_thread",
            "child_turn",
            2,
        )
        .expect_err("process widening must be rejected");

        assert!(format!("{error:#}").contains("process policy exceeds parent"));
    }

    #[test]
    fn file_policy_accepts_gateway_resolved_workspace_write_snapshot() {
        let workspace = tempfile::tempdir().expect("workspace tempdir");
        let outside = tempfile::tempdir().expect("outside tempdir");
        let mut context = context();
        context.workspace_root = Some(workspace.path().to_path_buf());
        context.project_roots = Vec::new();
        context.resolved_permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::AutoAcceptEdits,
            TurnPermissionProfileSource::Composer,
        );
        let params = TurnStartParams {
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: None,
            permission_profile: Some(TurnPermissionProfileSelection {
                mode: TurnPermissionMode::AutoAcceptEdits,
            }),
            cli_runtime_options: None,
        };
        let input = TurnSecurityResolverInput::from_turn_start_params(&params, context)
            .expect("input should build");
        let snapshot = resolve_turn_execution_security(&input).expect("snapshot should resolve");

        let allowed =
            FilePolicyChecker::check_write(&snapshot, workspace.path().join("created.txt"));
        assert!(allowed.is_allowed());

        let denied = FilePolicyChecker::check_write(&snapshot, outside.path().join("blocked.txt"));
        assert_eq!(
            denied
                .deny()
                .expect("outside write should be denied")
                .reason,
            FilePolicyDenyReason::OutsideAllowedRoots
        );
    }
}
