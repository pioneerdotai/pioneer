use anyhow::{Context, Result, bail};
use pioneer_crud::{
    CrudStore, find_turn_initiator, load_device, load_principal_by_id, load_session,
};
use pioneer_protocol::{
    AgentExecutionBackend, AuthSessionId, CLIAgentRuntimeKind, DeviceId, PersistedActorRef,
    PrincipalId, PrincipalKind, PrincipalStatus, TurnCapability, TurnPermissionMode,
    TurnPermissionProfileCap, TurnPermissionProfileSnapshot, TurnSkillBinding,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::auth::AuthenticatedSessionPrincipal;
use crate::request_context::RequestContext;

use super::{
    AuthorizationResolver, AuthorizationService, AuthorizedThread, ProofResolution, ResourceAction,
    record_stale_policy_revision,
};

const EXECUTION_AUTHORIZATION_CONTEXT_VERSION: u32 = 1;
const SKILL_PROJECTION_VERSION: u32 = 1;
const CLI_RUNTIME_PROJECTION_VERSION: u32 = 1;

/// Keeps the intentional legacy System/Superuser compatibility boundary while
/// preventing a persisted Member turn from continuing without its durable
/// execution authorization envelope.
pub(crate) async fn ensure_contextless_execution_is_trusted(
    store: &CrudStore,
    turn_id: &str,
) -> Result<()> {
    let Some(PersistedActorRef::Principal(principal_id)) =
        find_turn_initiator(&store.database_connection(), turn_id)
            .await
            .context("failed to resolve contextless execution initiator")?
    else {
        return Ok(());
    };
    let principal = load_principal_by_id(&store.database_connection(), &principal_id)
        .await
        .context("failed to load contextless execution initiator")?
        .context("contextless execution initiator no longer exists")?;
    if principal.kind == PrincipalKind::User {
        bail!("Member execution has no durable authorization context");
    }
    Ok(())
}

/// Immutable, non-secret admission envelope for user-triggered execution.
///
/// The value is persisted for restart/recovery, but it is not a credential:
/// every privileged continuation must still re-resolve current authorization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionAuthorizationContext {
    version: u32,
    initiating_principal_id: PrincipalId,
    initiating_session_id: AuthSessionId,
    workspace_id: String,
    root_thread_id: String,
    policy_revision: u64,
    capability_projection_fingerprint: String,
    permission_profile_cap: TurnPermissionProfileCap,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mcp_projection: Option<ExecutionMcpProjectionIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    skill_projection: Option<ExecutionSkillProjectionIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cli_runtime_projection: Option<ExecutionCliRuntimeProjectionIdentity>,
}

impl ExecutionAuthorizationContext {
    #[cfg(test)]
    pub(crate) fn for_test(
        principal: &AuthenticatedSessionPrincipal,
        workspace_id: &str,
        root_thread_id: &str,
        permission_profile: &TurnPermissionProfileSnapshot,
        execution_backend: Option<&AgentExecutionBackend>,
    ) -> Self {
        let permission_profile_cap =
            pioneer_protocol::task_permission_cap_from_snapshot(permission_profile);
        let capability_projection_fingerprint = capability_projection_fingerprint(
            workspace_id,
            root_thread_id,
            "test-provider",
            "test-model",
            execution_backend,
            &[],
            &permission_profile_cap,
        )
        .expect("test execution projection must serialize");
        Self {
            version: EXECUTION_AUTHORIZATION_CONTEXT_VERSION,
            initiating_principal_id: principal.principal_id.clone(),
            initiating_session_id: principal.session_id.clone(),
            workspace_id: workspace_id.to_owned(),
            root_thread_id: root_thread_id.to_owned(),
            policy_revision: 0,
            capability_projection_fingerprint,
            permission_profile_cap,
            mcp_projection: None,
            skill_projection: None,
            cli_runtime_projection: cli_runtime_projection(execution_backend),
        }
    }

    pub(crate) fn initiating_principal_id(&self) -> &PrincipalId {
        &self.initiating_principal_id
    }

    pub(crate) fn initiating_session_id(&self) -> &AuthSessionId {
        &self.initiating_session_id
    }

    pub(crate) fn workspace_id(&self) -> &str {
        self.workspace_id.as_str()
    }

    pub(crate) fn root_thread_id(&self) -> &str {
        self.root_thread_id.as_str()
    }

    #[cfg(test)]
    pub(crate) const fn policy_revision(&self) -> u64 {
        self.policy_revision
    }

    #[cfg(test)]
    pub(crate) fn mcp_projection(&self) -> Option<(u32, &str)> {
        self.mcp_projection
            .as_ref()
            .map(|projection| (projection.version, projection.manifest_hash.as_str()))
    }

    pub(crate) fn skill_projection(&self) -> Option<(u32, &str)> {
        self.skill_projection
            .as_ref()
            .map(|projection| (projection.version, projection.manifest_hash.as_str()))
    }

    #[cfg(test)]
    pub(crate) fn cli_runtime_projection(&self) -> Option<(u32, &str, CLIAgentRuntimeKind)> {
        self.cli_runtime_projection.as_ref().map(|projection| {
            (
                projection.version,
                projection.runtime_id.as_str(),
                projection.runtime_kind,
            )
        })
    }

    pub(crate) fn authorization_fingerprint(&self) -> Result<String> {
        let encoded = serde_json::to_vec(self)
            .context("failed to encode execution authorization identity")?;
        Ok(hex::encode(Sha256::digest(encoded)))
    }

    pub(crate) fn to_persisted_json(&self) -> Result<String> {
        serde_json::to_string(self).context("failed to serialize execution authorization context")
    }

    pub(crate) fn from_persisted_json(json: &str) -> Result<Self> {
        let context: Self = serde_json::from_str(json)
            .context("failed to deserialize execution authorization context")?;
        if context.version != EXECUTION_AUTHORIZATION_CONTEXT_VERSION {
            bail!(
                "unsupported execution authorization context version {}",
                context.version
            );
        }
        if context.workspace_id.trim().is_empty()
            || context.root_thread_id.trim().is_empty()
            || context.capability_projection_fingerprint.len() != 64
            || !context
                .capability_projection_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("invalid persisted execution authorization context");
        }
        if let Some(projection) = context.mcp_projection.as_ref() {
            validate_projection_identity(
                "MCP",
                projection.version,
                projection.manifest_hash.as_str(),
            )?;
        }
        if let Some(projection) = context.skill_projection.as_ref() {
            validate_projection_identity(
                "skill",
                projection.version,
                projection.manifest_hash.as_str(),
            )?;
        }
        if let Some(projection) = context.cli_runtime_projection.as_ref() {
            validate_cli_runtime_projection(projection)?;
        }
        Ok(context)
    }

    pub(crate) fn bind_mcp_projection(
        &mut self,
        workspace_id: &str,
        version: u32,
        manifest_hash: &str,
    ) -> Result<()> {
        if workspace_id != self.workspace_id {
            bail!("MCP projection workspace differs from authorized execution workspace");
        }
        validate_projection_identity("MCP", version, manifest_hash)?;
        let projection = ExecutionMcpProjectionIdentity {
            version,
            manifest_hash: manifest_hash.to_owned(),
        };
        if self
            .mcp_projection
            .as_ref()
            .is_some_and(|bound| bound != &projection)
        {
            bail!("execution context is already bound to a different MCP projection");
        }
        self.mcp_projection = Some(projection);
        Ok(())
    }

    pub(crate) fn verify_mcp_projection(
        &self,
        workspace_id: &str,
        version: u32,
        manifest_hash: &str,
    ) -> Result<()> {
        if workspace_id != self.workspace_id {
            bail!("MCP projection workspace differs from authorized execution workspace");
        }
        validate_projection_identity("MCP", version, manifest_hash)?;
        let Some(bound) = self.mcp_projection.as_ref() else {
            bail!("execution context is not bound to an MCP projection");
        };
        if bound.version != version || bound.manifest_hash != manifest_hash {
            bail!("MCP projection does not match the execution authorization context");
        }
        Ok(())
    }

    pub(crate) fn bind_skill_projection(
        &mut self,
        workspace_id: &str,
        bindings: &[TurnSkillBinding],
    ) -> Result<()> {
        if workspace_id != self.workspace_id {
            bail!("skill projection workspace differs from authorized execution workspace");
        }
        let manifest_hash = skill_projection_manifest_hash(workspace_id, bindings)?;
        let projection = ExecutionSkillProjectionIdentity {
            version: SKILL_PROJECTION_VERSION,
            manifest_hash,
        };
        if self
            .skill_projection
            .as_ref()
            .is_some_and(|bound| bound != &projection)
        {
            bail!("execution context is already bound to a different skill projection");
        }
        self.skill_projection = Some(projection);
        Ok(())
    }

    pub(crate) fn verify_skill_projection(
        &self,
        workspace_id: &str,
        bindings: &[TurnSkillBinding],
    ) -> Result<()> {
        if workspace_id != self.workspace_id {
            bail!("skill projection workspace differs from authorized execution workspace");
        }
        let expected_hash = skill_projection_manifest_hash(workspace_id, bindings)?;
        let Some(bound) = self.skill_projection.as_ref() else {
            bail!("execution context is not bound to a skill projection");
        };
        if bound.version != SKILL_PROJECTION_VERSION || bound.manifest_hash != expected_hash {
            bail!("skill projection does not match the execution authorization context");
        }
        Ok(())
    }

    pub(crate) fn verify_cli_runtime_projection(
        &self,
        workspace_id: &str,
        runtime_id: &str,
        runtime_kind: CLIAgentRuntimeKind,
    ) -> Result<()> {
        if workspace_id != self.workspace_id {
            bail!("CLI runtime workspace differs from authorized execution workspace");
        }
        let Some(bound) = self.cli_runtime_projection.as_ref() else {
            bail!("execution context is not bound to a CLI runtime");
        };
        if bound.version != CLI_RUNTIME_PROJECTION_VERSION
            || bound.runtime_id != runtime_id
            || bound.runtime_kind != runtime_kind
        {
            bail!("CLI runtime does not match the execution authorization context");
        }
        Ok(())
    }

    pub(crate) fn derive_continuation(
        &self,
        provider: &str,
        model: &str,
        execution_backend: Option<&AgentExecutionBackend>,
        capabilities: &[TurnCapability],
        effective_permission_profile: &TurnPermissionProfileSnapshot,
    ) -> Result<Self> {
        let parent_cap =
            pioneer_protocol::task_permission_cap_snapshot(&self.permission_profile_cap);
        let capped = pioneer_protocol::intersect_turn_permission_profiles(
            effective_permission_profile,
            &parent_cap,
            pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
        );
        if &capped != effective_permission_profile {
            bail!("task continuation permission profile exceeds initiating execution cap");
        }
        let permission_profile_cap =
            pioneer_protocol::task_permission_cap_from_snapshot(effective_permission_profile);
        let capability_projection_fingerprint = capability_projection_fingerprint(
            self.workspace_id.as_str(),
            self.root_thread_id.as_str(),
            provider,
            model,
            execution_backend,
            capabilities,
            &permission_profile_cap,
        )?;
        Ok(Self {
            version: EXECUTION_AUTHORIZATION_CONTEXT_VERSION,
            initiating_principal_id: self.initiating_principal_id.clone(),
            initiating_session_id: self.initiating_session_id.clone(),
            workspace_id: self.workspace_id.clone(),
            root_thread_id: self.root_thread_id.clone(),
            policy_revision: self.policy_revision,
            capability_projection_fingerprint,
            permission_profile_cap,
            mcp_projection: None,
            skill_projection: None,
            cli_runtime_projection: cli_runtime_projection(execution_backend),
        })
    }

    /// Rebuilds the initiating identity from durable auth state and resolves
    /// the exact root against the current policy and ACL.
    ///
    /// The persisted envelope is deliberately not a bearer credential. The
    /// synthetic principal returned here has an expired access lease and is
    /// suitable only for the authorization resolver in the current process.
    pub(crate) async fn revalidate(
        &self,
        store: &CrudStore,
        action: ResourceAction,
        current_policy_revision: u64,
    ) -> Result<RevalidatedExecutionAuthorization> {
        let database = store.database_connection();
        let session = load_session(&database, &self.initiating_session_id)
            .await
            .context("failed to load initiating execution session")?
            .context("initiating execution session no longer exists")?;
        if session.status != "active"
            || session.principal_id != self.initiating_principal_id.as_str()
            || session
                .refresh_expires_at
                .is_none_or(|expires_at| expires_at <= chrono::Utc::now())
        {
            bail!("initiating execution session is no longer active");
        }

        let principal = load_principal_by_id(&database, &self.initiating_principal_id)
            .await
            .context("failed to load initiating execution principal")?
            .context("initiating execution principal no longer exists")?;
        if principal.status != PrincipalStatus::Active
            || principal.gateway_id.as_str() != session.gateway_id
        {
            bail!("initiating execution principal is no longer active");
        }
        let role_key = match principal.kind {
            PrincipalKind::Superuser if principal.role_key.is_none() => None,
            PrincipalKind::User => Some(
                principal
                    .role_key
                    .as_deref()
                    .map(pioneer_protocol::RoleKey::new)
                    .transpose()
                    .context("initiating execution principal has an invalid role")?
                    .filter(pioneer_protocol::RoleKey::is_supported)
                    .context("initiating execution principal has an unsupported role")?,
            ),
            PrincipalKind::Superuser => {
                bail!("initiating Superuser execution principal has a role")
            }
        };

        let device_id =
            DeviceId::new(session.device_id).context("initiating session has an invalid device")?;
        let device = load_device(&database, &device_id)
            .await
            .context("failed to load initiating execution device")?
            .context("initiating execution device no longer exists")?;
        if device.status != "active"
            || device.gateway_id != session.gateway_id
            || device.principal_id != session.principal_id
        {
            bail!("initiating execution device is no longer active");
        }

        let principal = AuthenticatedSessionPrincipal {
            gateway_id: principal.gateway_id,
            principal_id: principal.id,
            kind: principal.kind,
            role_key,
            device_id,
            session_id: self.initiating_session_id.clone(),
            access_jti: "non-bearer-execution-revalidation".to_owned(),
            access_expires_at_unix: 0,
        };
        let action_gate = AuthorizationService::new().authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            action,
        );
        let authorization = AuthorizationResolver::new(store.clone())
            .authorize_thread(
                &principal,
                &action_gate,
                action,
                self.root_thread_id.as_str(),
                Some(self.workspace_id.as_str()),
            )
            .await
            .context("failed to resolve current execution authorization")?;
        let ProofResolution::Authorized(authorization) = authorization else {
            bail!("initiating principal no longer has access to the execution root");
        };

        if self.policy_revision != current_policy_revision {
            record_stale_policy_revision();
        }
        Ok(RevalidatedExecutionAuthorization {
            principal,
            authorization,
            #[cfg(test)]
            persisted_policy_revision: self.policy_revision,
            #[cfg(test)]
            current_policy_revision,
        })
    }

    pub(crate) async fn revalidate_for_turn_scope(
        &self,
        store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        action: ResourceAction,
        current_policy_revision: u64,
    ) -> Result<RevalidatedExecutionAuthorization> {
        if self.workspace_id != workspace_id {
            bail!("turn workspace differs from its execution authorization context");
        }
        let Some((stored_workspace_id, _)) = store.get_turn(thread_id, turn_id).await? else {
            bail!("turn is absent from its declared thread");
        };
        if stored_workspace_id != workspace_id {
            bail!("turn parent scope differs from its execution authorization context");
        }
        if thread_id != self.root_thread_id {
            let lineage = store
                .get_task_thread_lineage(thread_id)
                .await
                .context("failed to resolve task continuation lineage")?
                .context("non-root execution turn has no task lineage")?;
            if lineage.child_thread_id != thread_id || lineage.root_thread_id != self.root_thread_id
            {
                bail!("task continuation is outside its authorized execution root");
            }
        }
        self.revalidate(store, action, current_policy_revision)
            .await
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionMcpProjectionIdentity {
    version: u32,
    manifest_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSkillProjectionIdentity {
    version: u32,
    manifest_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionCliRuntimeProjectionIdentity {
    version: u32,
    runtime_id: String,
    runtime_kind: CLIAgentRuntimeKind,
}

#[derive(Serialize)]
struct SkillProjectionManifest<'a> {
    workspace_id: &'a str,
    bindings: Vec<SkillProjectionBinding<'a>>,
}

#[derive(Serialize)]
struct SkillProjectionBinding<'a> {
    skill_id: &'a str,
    version: Option<&'a str>,
    fingerprint: &'a str,
    source_kind: &'a str,
}

fn skill_projection_manifest_hash(
    workspace_id: &str,
    bindings: &[TurnSkillBinding],
) -> Result<String> {
    if workspace_id.trim().is_empty() {
        bail!("skill projection requires an exact workspace");
    }
    let mut canonical = bindings
        .iter()
        .map(|binding| {
            if binding.fingerprint.is_empty()
                || binding.fingerprint != binding.fingerprint.trim()
                || binding.fingerprint.chars().count() > 128
            {
                bail!("skill projection contains an invalid fingerprint");
            }
            if binding
                .skill_version
                .as_deref()
                .is_some_and(|version| version.is_empty() || version != version.trim())
            {
                bail!("skill projection contains an invalid version");
            }
            if !matches!(
                binding.source_kind.as_str(),
                "system" | "user" | "registry" | "agent"
            ) {
                bail!("skill projection contains an unsupported source kind");
            }
            if binding.source_kind == "agent"
                && binding
                    .skill_version
                    .as_deref()
                    .and_then(|version| version.parse::<i64>().ok())
                    .is_none_or(|version| version <= 0)
            {
                bail!("learned skill projection requires an exact positive version");
            }
            Ok(SkillProjectionBinding {
                skill_id: binding.skill_id.as_str(),
                version: binding.skill_version.as_deref(),
                fingerprint: binding.fingerprint.as_str(),
                source_kind: binding.source_kind.as_str(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    canonical.sort_by(|left, right| left.skill_id.cmp(right.skill_id));
    if canonical
        .windows(2)
        .any(|pair| pair[0].skill_id == pair[1].skill_id)
    {
        bail!("skill projection contains duplicate skill identities");
    }
    let encoded = serde_json::to_vec(&SkillProjectionManifest {
        workspace_id,
        bindings: canonical,
    })
    .context("failed to encode skill projection manifest")?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn validate_projection_identity(kind: &str, version: u32, manifest_hash: &str) -> Result<()> {
    if version == 0
        || manifest_hash.len() != 64
        || !manifest_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("invalid {kind} projection identity");
    }
    Ok(())
}

fn validate_cli_runtime_projection(
    projection: &ExecutionCliRuntimeProjectionIdentity,
) -> Result<()> {
    if projection.version != CLI_RUNTIME_PROJECTION_VERSION
        || projection.runtime_id.trim().is_empty()
        || projection.runtime_id != projection.runtime_id.trim()
    {
        bail!("invalid CLI runtime projection identity");
    }
    Ok(())
}

fn cli_runtime_projection(
    execution_backend: Option<&AgentExecutionBackend>,
) -> Option<ExecutionCliRuntimeProjectionIdentity> {
    match execution_backend {
        Some(AgentExecutionBackend::CLIAgentRuntime {
            runtime_id,
            runtime_kind,
        }) => Some(ExecutionCliRuntimeProjectionIdentity {
            version: CLI_RUNTIME_PROJECTION_VERSION,
            runtime_id: runtime_id.clone(),
            runtime_kind: *runtime_kind,
        }),
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) struct RevalidatedExecutionAuthorization {
    principal: AuthenticatedSessionPrincipal,
    authorization: AuthorizedThread,
    #[cfg(test)]
    persisted_policy_revision: u64,
    #[cfg(test)]
    current_policy_revision: u64,
}

impl RevalidatedExecutionAuthorization {
    pub(crate) fn principal(&self) -> &AuthenticatedSessionPrincipal {
        &self.principal
    }

    pub(crate) fn authorization(&self) -> &AuthorizedThread {
        &self.authorization
    }

    #[cfg(test)]
    pub(crate) const fn policy_revision_changed(&self) -> bool {
        self.persisted_policy_revision != self.current_policy_revision
    }

    #[cfg(test)]
    pub(crate) const fn current_policy_revision(&self) -> u64 {
        self.current_policy_revision
    }
}

/// Short-lived admission seed. It can only be created from the authenticated
/// request and an exact typed thread proof, then finalized from materialized
/// server-owned execution state.
#[derive(Clone, Debug)]
pub(crate) struct ExecutionAuthorizationAdmission {
    initiating_principal_id: PrincipalId,
    initiating_session_id: AuthSessionId,
    workspace_id: String,
    root_thread_id: String,
    policy_revision: u64,
    member: bool,
}

impl ExecutionAuthorizationAdmission {
    pub(crate) fn from_authorized_thread(
        request: &RequestContext,
        authorization: &AuthorizedThread,
        policy_revision: u64,
    ) -> Result<Self> {
        if authorization.principal_id() != &request.principal().principal_id {
            bail!("execution authorization principal does not match request");
        }
        Self::from_revalidated_thread(
            request,
            authorization.workspace_id(),
            authorization.thread_id(),
            policy_revision,
        )
    }

    pub(crate) fn from_revalidated_thread(
        request: &RequestContext,
        workspace_id: &str,
        root_thread_id: &str,
        policy_revision: u64,
    ) -> Result<Self> {
        let workspace_id = workspace_id.trim();
        let root_thread_id = root_thread_id.trim();
        if workspace_id.is_empty() || root_thread_id.is_empty() {
            bail!("execution authorization requires exact workspace and root thread");
        }
        Ok(Self {
            initiating_principal_id: request.principal().principal_id.clone(),
            initiating_session_id: request.principal().session_id.clone(),
            workspace_id: workspace_id.to_owned(),
            root_thread_id: root_thread_id.to_owned(),
            policy_revision,
            member: request.principal().kind == PrincipalKind::User,
        })
    }

    pub(crate) const fn is_member(&self) -> bool {
        self.member
    }

    pub(crate) fn workspace_id(&self) -> &str {
        self.workspace_id.as_str()
    }

    pub(crate) fn root_thread_id(&self) -> &str {
        self.root_thread_id.as_str()
    }

    /// Member requests may keep or narrow the persisted provider/model, but
    /// cannot rewrite the server-owned thread configuration.
    pub(crate) fn validate_provider_request(
        &self,
        persisted_provider: &str,
        persisted_model: &str,
        requested_provider: Option<&str>,
        requested_model: Option<&str>,
        execution_backend: Option<&AgentExecutionBackend>,
    ) -> Result<()> {
        if !self.member {
            return Ok(());
        }
        if requested_provider
            .map(str::trim)
            .is_some_and(|provider| provider != persisted_provider)
        {
            bail!("Member cannot change the server-selected provider");
        }
        if requested_model
            .map(str::trim)
            .is_some_and(|model| model != persisted_model)
        {
            bail!("Member cannot change the server-selected model");
        }
        if let Some(AgentExecutionBackend::ApiProvider { provider }) = execution_backend
            && provider.trim() != persisted_provider
        {
            bail!("Member execution backend does not match the server-selected provider");
        }
        Ok(())
    }

    pub(crate) fn permission_profile_cap(&self) -> TurnPermissionProfileCap {
        if self.member {
            pioneer_protocol::task_permission_cap_for_mode(TurnPermissionMode::Supervised)
        } else {
            pioneer_protocol::task_permission_cap_for_mode(TurnPermissionMode::FullAccess)
        }
    }

    pub(crate) fn cap_permission_profile(
        &self,
        requested: &TurnPermissionProfileSnapshot,
    ) -> TurnPermissionProfileSnapshot {
        if !self.member {
            return requested.clone();
        }
        let cap = pioneer_protocol::task_permission_cap_snapshot(&self.permission_profile_cap());
        pioneer_protocol::intersect_turn_permission_profiles(
            requested,
            &cap,
            pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
        )
    }

    pub(crate) fn finalize(
        &self,
        workspace_id: &str,
        root_thread_id: &str,
        provider: &str,
        model: &str,
        execution_backend: Option<&AgentExecutionBackend>,
        capabilities: &[TurnCapability],
        effective_permission_profile: &TurnPermissionProfileSnapshot,
    ) -> Result<ExecutionAuthorizationContext> {
        if workspace_id != self.workspace_id || root_thread_id != self.root_thread_id {
            bail!("materialized execution scope differs from authorized root");
        }
        let permission_profile_cap = if self.member {
            self.permission_profile_cap()
        } else {
            pioneer_protocol::task_permission_cap_from_snapshot(effective_permission_profile)
        };
        let capability_projection_fingerprint = capability_projection_fingerprint(
            workspace_id,
            root_thread_id,
            provider,
            model,
            execution_backend,
            capabilities,
            &permission_profile_cap,
        )?;
        Ok(ExecutionAuthorizationContext {
            version: EXECUTION_AUTHORIZATION_CONTEXT_VERSION,
            initiating_principal_id: self.initiating_principal_id.clone(),
            initiating_session_id: self.initiating_session_id.clone(),
            workspace_id: self.workspace_id.clone(),
            root_thread_id: self.root_thread_id.clone(),
            policy_revision: self.policy_revision,
            capability_projection_fingerprint,
            permission_profile_cap,
            mcp_projection: None,
            skill_projection: None,
            cli_runtime_projection: cli_runtime_projection(execution_backend),
        })
    }
}

fn capability_projection_fingerprint(
    workspace_id: &str,
    root_thread_id: &str,
    provider: &str,
    model: &str,
    execution_backend: Option<&AgentExecutionBackend>,
    capabilities: &[TurnCapability],
    permission_profile_cap: &TurnPermissionProfileCap,
) -> Result<String> {
    let projection = CapabilityProjectionFingerprintInput {
        workspace_id,
        root_thread_id,
        provider,
        model,
        execution_backend,
        capabilities,
        permission_profile_cap,
    };
    let encoded =
        serde_json::to_vec(&projection).context("failed to encode server capability projection")?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
struct CapabilityProjectionFingerprintInput<'a> {
    workspace_id: &'a str,
    root_thread_id: &'a str,
    provider: &'a str,
    model: &'a str,
    execution_backend: Option<&'a AgentExecutionBackend>,
    capabilities: &'a [TurnCapability],
    permission_profile_cap: &'a TurnPermissionProfileCap,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthenticatedSessionPrincipal;
    use crate::authorization::{
        AuthorizationDecision, AuthorizationResource, ResourceAction, ThreadResourceId,
        WorkspaceResourceId,
    };
    use crate::request_context::{CanonicalMethod, ConnectionContext};
    use pioneer_protocol::{
        DeviceId, GatewayId, PersistedActorRef, RoleKey, TurnPermissionProfileSelection,
    };
    use sea_orm::{ConnectionTrait, Database};
    use std::sync::Arc;

    fn member_request() -> RequestContext {
        let principal = Arc::new(AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new("G".repeat(21)).expect("gateway id"),
            principal_id: PrincipalId::new("P".repeat(21)).expect("principal id"),
            kind: PrincipalKind::User,
            role_key: Some(RoleKey::member()),
            device_id: DeviceId::new("D".repeat(21)).expect("device id"),
            session_id: AuthSessionId::new("S".repeat(21)).expect("session id"),
            access_jti: "J".repeat(21),
            access_expires_at_unix: u64::MAX,
        });
        RequestContext::new(
            &ConnectionContext::new(7, principal),
            None,
            CanonicalMethod::rpc("turn/start"),
        )
    }

    fn authorized_thread(request: &RequestContext) -> AuthorizedThread {
        super::super::resolver::authorized_thread_for_test(
            request.principal().principal_id.clone(),
            ResourceAction::ThreadWrite,
            AuthorizationResource::Thread {
                workspace_id: WorkspaceResourceId::new("workspace-a").expect("workspace id"),
                thread_id: ThreadResourceId::new("thread-a").expect("thread id"),
            },
            AuthorizationDecision::AllowPolicy {
                role: RoleKey::member(),
                reason: super::super::AllowReason::PrivateThreadParticipant,
            },
        )
    }

    #[tokio::test]
    async fn contextless_member_execution_is_rejected_but_system_execution_is_retained() {
        use migration::{Migrator, MigratorTrait};

        let database = Database::connect("sqlite::memory:")
            .await
            .expect("connect isolated execution database");
        Migrator::up(&database, None)
            .await
            .expect("migrate isolated execution database");
        database
            .execute_unprepared(
                "INSERT INTO gateway_identity (
                    id, singleton_key, identity_bootstrap_version, auth_schema_version
                 ) VALUES (
                    'G00000000000000000001', 1, 1, 0
                 );
                 INSERT INTO gateway_principal (
                    id, gateway_id, kind, role_key, status, display_name, nickname, nickname_key
                 ) VALUES (
                    'P00000000000000000002', 'G00000000000000000001', 'user', 'member',
                    'active', 'Member', 'member', 'member'
                 );
                 INSERT INTO workspace (id, name, is_active, is_current) VALUES
                    ('W00000000000000000001', 'Execution', 1, 1);
                 INSERT INTO thread (
                    id, workspace_id, preview, mode, model, model_provider, status, origin_kind,
                    access_class
                 ) VALUES (
                    'thread_contextless', 'W00000000000000000001', '', 'agent', 'model',
                    'provider', 'active', 'user', 'private'
                 );
                 INSERT INTO turn (
                    id, thread_id, status, turn_kind, origin,
                    initiated_by_actor_kind, initiated_by_actor_id
                 ) VALUES (
                    'turn_contextless', 'thread_contextless', 'in_progress', 'conversation',
                    'user', 'principal', 'P00000000000000000002'
                 );",
            )
            .await
            .expect("seed contextless Member execution");
        let store = CrudStore::new(database.clone());

        ensure_contextless_execution_is_trusted(&store, "turn_contextless")
            .await
            .expect_err("contextless Member execution must fail closed");

        database
            .execute_unprepared(
                "UPDATE turn
                 SET initiated_by_actor_kind = 'system', initiated_by_actor_id = NULL
                 WHERE id = 'turn_contextless'",
            )
            .await
            .expect("convert fixture to trusted System execution");
        ensure_contextless_execution_is_trusted(&store, "turn_contextless")
            .await
            .expect("contextless System execution remains compatible");
    }

    #[test]
    fn member_context_is_exact_non_secret_and_round_trips() {
        let request = member_request();
        let proof = authorized_thread(&request);
        let admission =
            ExecutionAuthorizationAdmission::from_authorized_thread(&request, &proof, 41)
                .expect("authorized admission");
        let requested = pioneer_protocol::resolve_turn_permission_profile(Some(
            &TurnPermissionProfileSelection::full_access(),
        ));
        let effective = admission.cap_permission_profile(&requested);
        assert_eq!(effective.mode, TurnPermissionMode::Supervised);

        let context = admission
            .finalize(
                "workspace-a",
                "thread-a",
                "openai",
                "model-a",
                None,
                &[],
                &effective,
            )
            .expect("final context");
        let json = context.to_persisted_json().expect("serialize context");
        assert!(!json.contains("api_key"));
        assert!(!json.contains("secret"));
        assert_eq!(
            ExecutionAuthorizationContext::from_persisted_json(&json).expect("restore context"),
            context
        );
        assert_eq!(context.policy_revision(), 41);
        assert_eq!(context.root_thread_id(), "thread-a");
        assert!(context.mcp_projection().is_none());
        assert!(context.skill_projection().is_none());
    }

    #[test]
    fn mcp_projection_binding_is_exact_immutable_and_secret_free() {
        let request = member_request();
        let proof = authorized_thread(&request);
        let admission =
            ExecutionAuthorizationAdmission::from_authorized_thread(&request, &proof, 3)
                .expect("authorized admission");
        let effective = admission
            .cap_permission_profile(&pioneer_protocol::default_turn_permission_profile_snapshot());
        let mut context = admission
            .finalize(
                "workspace-a",
                "thread-a",
                "openai",
                "model-a",
                None,
                &[],
                &effective,
            )
            .expect("execution context");
        let manifest_hash = "c".repeat(64);
        context
            .bind_mcp_projection("workspace-a", 1, manifest_hash.as_str())
            .expect("bind exact MCP projection");
        context
            .verify_mcp_projection("workspace-a", 1, manifest_hash.as_str())
            .expect("verify exact MCP projection");
        assert!(
            context
                .verify_mcp_projection("workspace-a", 1, "d".repeat(64).as_str())
                .is_err()
        );
        assert!(
            context
                .bind_mcp_projection("workspace-a", 2, manifest_hash.as_str())
                .is_err()
        );

        let json = context.to_persisted_json().expect("serialize context");
        let persisted: serde_json::Value =
            serde_json::from_str(json.as_str()).expect("execution context JSON");
        let mcp_projection = persisted
            .get("mcp_projection")
            .and_then(serde_json::Value::as_object)
            .expect("MCP projection object");
        assert_eq!(mcp_projection.len(), 2);
        assert!(mcp_projection.contains_key("version"));
        assert!(mcp_projection.contains_key("manifest_hash"));
        assert!(!json.contains("headers"));
        assert!(!json.contains("secret"));
        assert_eq!(
            ExecutionAuthorizationContext::from_persisted_json(json.as_str())
                .expect("restore context")
                .mcp_projection(),
            Some((1, manifest_hash.as_str()))
        );
    }

    #[test]
    fn skill_projection_binding_is_exact_immutable_and_path_free() {
        let request = member_request();
        let proof = authorized_thread(&request);
        let admission =
            ExecutionAuthorizationAdmission::from_authorized_thread(&request, &proof, 4)
                .expect("authorized admission");
        let effective = admission
            .cap_permission_profile(&pioneer_protocol::default_turn_permission_profile_snapshot());
        let mut context = admission
            .finalize(
                "workspace-a",
                "thread-a",
                "openai",
                "model-a",
                None,
                &[],
                &effective,
            )
            .expect("execution context");
        let binding = TurnSkillBinding {
            skill_id: pioneer_protocol::SkillId::new("K".repeat(21)).expect("skill id"),
            skill_owner: Some("publisher".to_owned()),
            skill_slug: "approved-skill".to_owned(),
            skill_version: Some("1.2.3".to_owned()),
            fingerprint: "a".repeat(64),
            source_kind: "registry".to_owned(),
            resolved_reason: "explicit".to_owned(),
        };
        context
            .bind_skill_projection("workspace-a", std::slice::from_ref(&binding))
            .expect("bind exact skill projection");
        context
            .verify_skill_projection("workspace-a", std::slice::from_ref(&binding))
            .expect("verify exact skill projection");

        let mut changed = binding.clone();
        changed.skill_version = Some("1.2.4".to_owned());
        assert!(
            context
                .verify_skill_projection("workspace-a", std::slice::from_ref(&changed))
                .is_err()
        );
        assert!(
            context
                .bind_skill_projection("workspace-a", std::slice::from_ref(&changed))
                .is_err()
        );

        let json = context.to_persisted_json().expect("serialize context");
        assert!(!json.contains("install_path"));
        assert!(!json.contains("source_ref"));
        assert!(!json.contains("archive"));
        assert!(!json.contains("publisher"));
        assert!(!json.contains("approved-skill"));
        let restored =
            ExecutionAuthorizationContext::from_persisted_json(json.as_str()).expect("restore");
        assert_eq!(restored.skill_projection(), context.skill_projection());
    }

    #[test]
    fn cli_runtime_projection_is_exact_and_continuation_cannot_widen_permissions() {
        let request = member_request();
        let proof = authorized_thread(&request);
        let admission =
            ExecutionAuthorizationAdmission::from_authorized_thread(&request, &proof, 4)
                .expect("authorized admission");
        let supervised = admission
            .cap_permission_profile(&pioneer_protocol::default_turn_permission_profile_snapshot());
        let backend = AgentExecutionBackend::CLIAgentRuntime {
            runtime_id: "codex-work".to_owned(),
            runtime_kind: CLIAgentRuntimeKind::Codex,
        };
        let context = admission
            .finalize(
                "workspace-a",
                "thread-a",
                "cli_runtime:codex-work",
                "model-a",
                Some(&backend),
                &[],
                &supervised,
            )
            .expect("execution context");
        context
            .verify_cli_runtime_projection("workspace-a", "codex-work", CLIAgentRuntimeKind::Codex)
            .expect("exact runtime projection");
        assert!(
            context
                .verify_cli_runtime_projection(
                    "workspace-a",
                    "claude-work",
                    CLIAgentRuntimeKind::Claude,
                )
                .is_err()
        );
        assert!(
            context
                .derive_continuation(
                    "cli_runtime:codex-work",
                    "model-a",
                    Some(&backend),
                    &[],
                    &pioneer_protocol::default_turn_permission_profile_snapshot(),
                )
                .is_err(),
            "a task continuation cannot restore a profile above the initiating cap"
        );
        let derived = context
            .derive_continuation(
                "cli_runtime:codex-work",
                "model-a",
                Some(&backend),
                &[],
                &supervised,
            )
            .expect("narrow continuation");
        assert_eq!(
            derived.cli_runtime_projection(),
            Some((1, "codex-work", CLIAgentRuntimeKind::Codex))
        );
        assert_eq!(
            derived.initiating_session_id(),
            context.initiating_session_id()
        );
        assert_eq!(derived.root_thread_id(), context.root_thread_id());
    }

    #[test]
    fn member_cannot_widen_provider_or_root() {
        let request = member_request();
        let proof = authorized_thread(&request);
        let admission =
            ExecutionAuthorizationAdmission::from_authorized_thread(&request, &proof, 0)
                .expect("authorized admission");
        assert!(
            admission
                .validate_provider_request("openai", "model-a", Some("other"), None, None)
                .is_err()
        );
        let effective = admission
            .cap_permission_profile(&pioneer_protocol::default_turn_permission_profile_snapshot());
        assert!(
            admission
                .finalize(
                    "workspace-a",
                    "thread-b",
                    "openai",
                    "model-a",
                    None,
                    &[],
                    &effective,
                )
                .is_err()
        );
    }

    #[test]
    fn context_is_not_an_actor_or_bearer_credential() {
        let _: PersistedActorRef =
            PersistedActorRef::Principal(member_request().principal().principal_id.clone());
        assert!(!std::any::type_name::<ExecutionAuthorizationContext>().contains("Credential"));
    }

    #[tokio::test]
    async fn persisted_context_revalidates_current_session_and_acl() {
        use crate::authorization_test_support::{IsolatedEpic4Harness, MEMBER_A_ID};
        use sea_orm::ConnectionTrait;

        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated Epic 4 harness");
        let workspace_id = "W00000000000000000001";
        let thread_id = "T0000000000000000000A";
        let context = ExecutionAuthorizationContext {
            version: EXECUTION_AUTHORIZATION_CONTEXT_VERSION,
            initiating_principal_id: PrincipalId::new(MEMBER_A_ID).expect("principal id"),
            initiating_session_id: AuthSessionId::new("S0000000000000000000A").expect("session id"),
            workspace_id: workspace_id.to_owned(),
            root_thread_id: thread_id.to_owned(),
            policy_revision: 7,
            capability_projection_fingerprint: "a".repeat(64),
            permission_profile_cap: pioneer_protocol::task_permission_cap_for_mode(
                TurnPermissionMode::Supervised,
            ),
            mcp_projection: None,
            skill_projection: None,
            cli_runtime_projection: None,
        };
        let store = CrudStore::new(harness.database.clone());
        harness
            .database
            .execute_unprepared(&format!(
                "INSERT INTO thread(\
                    id,workspace_id,name,preview,mode,model,model_provider,status,\
                    origin_kind,sidebar_visibility,access_class,created_at,updated_at,\
                    created_by_actor_kind,created_by_actor_id\
                 ) VALUES(\
                    '{thread_id}','{workspace_id}','Private A','',\
                    'chat','test','test','active','user','visible','private',\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'principal','{MEMBER_A_ID}'\
                 );\
                 INSERT INTO thread_membership(\
                    thread_id,principal_id,added_by_actor_kind,added_by_actor_id,\
                    created_at,updated_at\
                 ) VALUES(\
                    '{thread_id}','{MEMBER_A_ID}','system',NULL,\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
                 );"
            ))
            .await
            .expect("materialize execution authorization root");

        let revalidated = context
            .revalidate(&store, ResourceAction::ThreadWrite, 8)
            .await
            .expect("current session and ACL authorize continuation");
        assert_eq!(revalidated.principal().principal_id.as_str(), MEMBER_A_ID);
        assert_eq!(revalidated.authorization().thread_id(), thread_id);
        assert!(revalidated.policy_revision_changed());
        assert_eq!(revalidated.current_policy_revision(), 8);
        assert_eq!(revalidated.principal().access_expires_at_unix, 0);

        harness
            .database
            .execute_unprepared(&format!(
                "DELETE FROM thread_membership \
                 WHERE thread_id = '{thread_id}' \
                 AND principal_id = '{MEMBER_A_ID}'"
            ))
            .await
            .expect("remove current thread access");
        assert!(
            context
                .revalidate(&store, ResourceAction::ThreadWrite, 9)
                .await
                .is_err(),
            "persisted context must not retain revoked thread access"
        );
    }

    #[tokio::test]
    async fn persisted_context_rejects_revoked_initiating_session() {
        use crate::authorization_test_support::{
            IsolatedEpic4Harness, MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID,
        };
        use sea_orm::ConnectionTrait;

        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated Epic 4 harness");
        let context = ExecutionAuthorizationContext {
            version: EXECUTION_AUTHORIZATION_CONTEXT_VERSION,
            initiating_principal_id: PrincipalId::new(MEMBER_A_ID).expect("principal id"),
            initiating_session_id: AuthSessionId::new("S0000000000000000000A").expect("session id"),
            workspace_id: "W00000000000000000001".to_owned(),
            root_thread_id: THREAD_RED_PRIVATE_A_ID.to_owned(),
            policy_revision: 0,
            capability_projection_fingerprint: "b".repeat(64),
            permission_profile_cap: pioneer_protocol::task_permission_cap_for_mode(
                TurnPermissionMode::Supervised,
            ),
            mcp_projection: None,
            skill_projection: None,
            cli_runtime_projection: None,
        };
        let store = CrudStore::new(harness.database.clone());
        harness
            .database
            .execute_unprepared(
                "UPDATE auth_session SET status = 'revoked', revoked_at = CURRENT_TIMESTAMP, \
                 revoke_reason = 'security_reset' \
                 WHERE id = 'S0000000000000000000A'",
            )
            .await
            .expect("revoke initiating session");

        assert!(
            context
                .revalidate(&store, ResourceAction::ThreadWrite, 0)
                .await
                .is_err(),
            "persisted context must not authorize after session revocation"
        );
    }
}
