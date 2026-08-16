use anyhow::{Context, Result, anyhow, bail};
use futures_util::future::BoxFuture;
use pioneer_crud::{CrudStore, PersistedThreadAccessClass};
#[cfg(test)]
use pioneer_protocol::TurnPermissionMode;
use pioneer_protocol::{
    AgentExecutionBackend, AuthSessionId, CLIAgentRuntimeKind, GatewayId, PolicyGeneration,
    PrincipalId, PrincipalKind, RoleKey, TaskCreateParams, TaskGetResponse, TaskTriggerKind,
    ThreadVisibility, TurnCapability, TurnPermissionProfileCap, TurnPermissionProfileSnapshot,
    TurnSkillBinding, TurnStartParams, UserInput,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

use crate::auth::AuthenticatedSessionPrincipal;
use crate::human_interaction::HumanInteractionBudget;
use crate::request_context::RequestContext;
use crate::thread::RuntimeDraftAccess;

use super::{
    AuthorizationResolver, AuthorizationService, AuthorizedThread, ProofResolution, ResourceAction,
    RuntimePrincipalPolicy, record_stale_policy_revision,
};

const EXECUTION_AUTHORIZATION_CONTEXT_VERSION: u32 = 8;
const SKILL_PROJECTION_VERSION: u32 = 1;
const CLI_RUNTIME_PROJECTION_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionContinuityPolicy {
    /// Continue under the current root collaboration set and fence if no
    /// participant retains the immutable backend action.
    ContinueShared,
    /// Fence future side effects as soon as collaboration authority disappears.
    FenceOnAuthorityLoss,
    /// Fence and request backend cancellation when collaboration authority disappears.
    StopOnAuthorityLoss,
}

/// Immutable resource ceiling carried by every execution grant. A role may
/// narrow actions inside this boundary, but cannot widen a Turn beyond its
/// admitted collaboration capsule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionResourceBoundary {
    RootThreadCapsule,
}

const EXECUTION_RUNTIME_ACTIONS: &[ResourceAction] = &[
    ResourceAction::ThreadRead,
    ResourceAction::AgentTurnStart,
    ResourceAction::AgentExecutionObserve,
    ResourceAction::AgentExecutionCancel,
    ResourceAction::AgentExecutionResume,
    ResourceAction::AgentExecutionSteer,
    ResourceAction::AgentRequestObserve,
    ResourceAction::AgentRequestRespond,
    ResourceAction::MessageCreate,
    ResourceAction::ArtifactRead,
    ResourceAction::ArtifactCreateThread,
    ResourceAction::ArtifactBindThread,
    ResourceAction::MemoryRead,
    ResourceAction::MemoryCreateThread,
    ResourceAction::MemoryUpdateThread,
    ResourceAction::MemoryForgetThread,
    ResourceAction::TaskRead,
    ResourceAction::TaskCreate,
    ResourceAction::TaskReview,
    ResourceAction::TaskCancel,
    ResourceAction::TaskScheduleManage,
    ResourceAction::TaskDetach,
    ResourceAction::ProviderUse,
    ResourceAction::McpUse,
    ResourceAction::SkillUse,
    ResourceAction::CliRuntimeUse,
    ResourceAction::CliRuntimeControl,
    ResourceAction::CliThreadFork,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionAdmissionEntryPoint {
    InteractiveTurn,
    VoiceTurn,
    DetachedTask,
    Task,
    Scheduler,
    Subagent,
    CliRuntime,
    Recovery,
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutionAdmissionRequest {
    pub(crate) entry_point: ExecutionAdmissionEntryPoint,
    pub(crate) required_root_action: ResourceAction,
    pub(crate) additional_required_actions: Vec<ResourceAction>,
    pub(crate) workspace_id: String,
    pub(crate) root_thread_id: String,
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) execution_backend: Option<AgentExecutionBackend>,
    pub(crate) capabilities: Vec<TurnCapability>,
    pub(crate) artifacts: Vec<ExecutionArtifactGrant>,
    pub(crate) has_local_attachment_sources: bool,
    pub(crate) has_url_attachment_sources: bool,
    pub(crate) provider_authority_fingerprint: Option<String>,
}

impl ExecutionAdmissionRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_turn(
        entry_point: ExecutionAdmissionEntryPoint,
        required_root_action: ResourceAction,
        additional_required_actions: Vec<ResourceAction>,
        workspace_id: &str,
        root_thread_id: &str,
        provider: &str,
        model: &str,
        params: &TurnStartParams,
        capabilities: &[TurnCapability],
        provider_authority_fingerprint: Option<String>,
    ) -> Result<Self> {
        let input_sources = execution_input_sources(params.input.as_slice())?;
        Ok(Self {
            entry_point,
            required_root_action,
            additional_required_actions,
            workspace_id: workspace_id.to_owned(),
            root_thread_id: root_thread_id.to_owned(),
            provider: provider.to_owned(),
            model: model.to_owned(),
            execution_backend: params.execution_backend.clone(),
            capabilities: capabilities.to_vec(),
            artifacts: input_sources.artifacts,
            has_local_attachment_sources: input_sources.has_local_paths,
            has_url_attachment_sources: input_sources.has_urls,
            provider_authority_fingerprint,
        })
    }

    pub(crate) fn for_task(
        params: &TaskCreateParams,
        root_thread_id: &str,
        fallback_provider: &str,
        fallback_model: &str,
        provider_authority_fingerprint: Option<String>,
    ) -> Result<Self> {
        let launch = params
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.composer_work.as_ref())
            .map(|work| &work.launch);
        let provider = launch
            .and_then(|launch| launch.model_provider.as_deref())
            .or_else(|| {
                params
                    .agent_spec
                    .as_ref()
                    .and_then(|spec| spec.model_provider.as_deref())
            })
            .unwrap_or(fallback_provider)
            .trim();
        let model = launch
            .and_then(|launch| launch.model.as_deref())
            .or_else(|| {
                params
                    .agent_spec
                    .as_ref()
                    .and_then(|spec| spec.model.as_deref())
            })
            .unwrap_or(fallback_model)
            .trim();
        let entry_point = if params.trigger.spec.kind() == TaskTriggerKind::Immediate {
            ExecutionAdmissionEntryPoint::Task
        } else {
            ExecutionAdmissionEntryPoint::Scheduler
        };
        let mut input_sources = match launch {
            Some(launch) => execution_input_sources(launch.input.as_slice())?,
            None => ExecutionInputSources::default(),
        };
        if let Some(agent_spec) = params.agent_spec.as_ref() {
            if let Some(input) = agent_spec.prompt.input.as_ref() {
                input_sources.merge(task_agent_input_sources(input)?);
            }
            if let Some(context) = agent_spec
                .context_policy
                .as_ref()
                .and_then(|policy| policy.custom_context.as_ref())
            {
                input_sources.merge(task_agent_input_sources(
                    &pioneer_protocol::TaskAgentInput {
                        text: None,
                        variables: Vec::new(),
                        attachments: context.attachments.clone(),
                        references: context.references.clone(),
                    },
                )?);
            }
        }
        let additional_required_actions = params
            .agent_spec
            .as_ref()
            .and_then(|spec| spec.context_policy.as_ref())
            .is_some_and(|policy| policy.include_artifacts)
            .then_some(ResourceAction::ArtifactRead)
            .into_iter()
            .collect();
        Ok(Self {
            entry_point,
            required_root_action: ResourceAction::TaskCreate,
            additional_required_actions,
            workspace_id: params.workspace_id.clone(),
            root_thread_id: root_thread_id.to_owned(),
            provider: provider.to_owned(),
            model: model.to_owned(),
            execution_backend: launch.and_then(|launch| launch.execution_backend.clone()),
            capabilities: launch
                .map(|launch| launch.capabilities.clone())
                .unwrap_or_default(),
            artifacts: input_sources.artifacts,
            has_local_attachment_sources: input_sources.has_local_paths,
            has_url_attachment_sources: input_sources.has_urls,
            provider_authority_fingerprint,
        })
    }

    pub(crate) fn for_existing_task(
        response: &TaskGetResponse,
        root_thread_id: &str,
        fallback_provider: &str,
        fallback_model: &str,
        provider_authority_fingerprint: Option<String>,
    ) -> Result<Self> {
        let launch = response
            .task
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.composer_work.as_ref())
            .map(|work| &work.launch);
        let agent_spec = response
            .agent_specs
            .iter()
            .rev()
            .find(|spec| spec.run_id.is_none());
        let provider = launch
            .and_then(|launch| launch.model_provider.as_deref())
            .or_else(|| agent_spec.and_then(|spec| spec.model_provider.as_deref()))
            .unwrap_or(fallback_provider)
            .trim();
        let model = launch
            .and_then(|launch| launch.model.as_deref())
            .or_else(|| agent_spec.and_then(|spec| spec.model.as_deref()))
            .unwrap_or(fallback_model)
            .trim();
        let entry_point = if response
            .triggers
            .first()
            .is_some_and(|trigger| trigger.kind() == TaskTriggerKind::Immediate)
        {
            ExecutionAdmissionEntryPoint::Task
        } else {
            ExecutionAdmissionEntryPoint::Scheduler
        };
        let mut input_sources = match launch {
            Some(launch) => execution_input_sources(launch.input.as_slice())?,
            None => ExecutionInputSources::default(),
        };
        if let Some(agent_spec) = agent_spec {
            if let Some(input) = agent_spec.prompt.input.as_ref() {
                input_sources.merge(task_agent_input_sources(input)?);
            }
            if let Some(context) = agent_spec.context_policy.as_ref()
                && let Some(custom_context) = context.custom_context.as_ref()
            {
                input_sources.merge(task_agent_input_sources(
                    &pioneer_protocol::TaskAgentInput {
                        text: None,
                        variables: Vec::new(),
                        attachments: custom_context.attachments.clone(),
                        references: custom_context.references.clone(),
                    },
                )?);
            }
        }
        let additional_required_actions = agent_spec
            .and_then(|spec| spec.context_policy.as_ref())
            .is_some_and(|policy| policy.include_artifacts)
            .then_some(ResourceAction::ArtifactRead)
            .into_iter()
            .collect();
        Ok(Self {
            entry_point,
            required_root_action: ResourceAction::TaskCreate,
            additional_required_actions,
            workspace_id: response.task.workspace_id.clone(),
            root_thread_id: root_thread_id.to_owned(),
            provider: provider.to_owned(),
            model: model.to_owned(),
            execution_backend: launch.and_then(|launch| launch.execution_backend.clone()),
            capabilities: launch
                .map(|launch| launch.capabilities.clone())
                .unwrap_or_default(),
            artifacts: input_sources.artifacts,
            has_local_attachment_sources: input_sources.has_local_paths,
            has_url_attachment_sources: input_sources.has_urls,
            provider_authority_fingerprint,
        })
    }
}

#[derive(Default)]
struct ExecutionInputSources {
    artifacts: Vec<ExecutionArtifactGrant>,
    has_local_paths: bool,
    has_urls: bool,
}

impl ExecutionInputSources {
    fn merge(&mut self, mut other: Self) {
        self.artifacts.append(&mut other.artifacts);
        self.has_local_paths |= other.has_local_paths;
        self.has_urls |= other.has_urls;
    }
}

fn task_agent_input_sources(
    input: &pioneer_protocol::TaskAgentInput,
) -> Result<ExecutionInputSources> {
    let mut sources = ExecutionInputSources::default();
    for attachment in &input.attachments {
        sources.has_local_paths |= attachment
            .path
            .as_deref()
            .is_some_and(|path| !path.trim().is_empty());
        sources.has_urls |= attachment
            .url
            .as_deref()
            .is_some_and(|url| !url.trim().is_empty());
        if attachment.kind == pioneer_protocol::TaskAgentInputAttachmentKind::Artifact {
            let artifact_id = attachment
                .artifact_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .context("task artifact attachment has no artifact id")?;
            let version_id = attachment
                .version_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .context("task artifact attachment requires an exact version")?;
            sources.artifacts.push(ExecutionArtifactGrant {
                artifact_id: artifact_id.to_owned(),
                version_id: version_id.to_owned(),
            });
        }
    }
    for reference in &input.references {
        if reference.kind == pioneer_protocol::TaskAgentInputReferenceKind::Artifact {
            let artifact_id = reference.id.trim();
            if artifact_id.is_empty() {
                bail!("task artifact reference has no artifact id");
            }
            let version_id = reference
                .version_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .context("task artifact reference requires an exact version")?;
            sources.artifacts.push(ExecutionArtifactGrant {
                artifact_id: artifact_id.to_owned(),
                version_id: version_id.to_owned(),
            });
        }
    }
    Ok(sources)
}

fn execution_input_sources(input: &[UserInput]) -> Result<ExecutionInputSources> {
    let artifacts = input
        .iter()
        .filter_map(|input| match input {
            UserInput::Artifact {
                artifact_id,
                version_id: Some(version_id),
            } => Some(ExecutionArtifactGrant {
                artifact_id: artifact_id.clone(),
                version_id: version_id.clone(),
            }),
            UserInput::Artifact {
                version_id: None, ..
            }
            | UserInput::Text { .. }
            | UserInput::Image { .. }
            | UserInput::LocalImage { .. }
            | UserInput::File { .. }
            | UserInput::LocalFile { .. }
            | UserInput::Audio { .. }
            | UserInput::LocalAudio { .. }
            | UserInput::Video { .. }
            | UserInput::LocalVideo { .. }
            | UserInput::Mention { .. } => None,
        })
        .collect::<Vec<_>>();
    if input.iter().any(|input| {
        matches!(
            input,
            UserInput::Artifact {
                version_id: None,
                ..
            }
        )
    }) {
        bail!("execution artifact requires an exact version");
    }
    Ok(ExecutionInputSources {
        artifacts,
        has_local_paths: input.iter().any(is_local_attachment_source),
        has_urls: input.iter().any(is_url_attachment_source),
    })
}

fn is_local_attachment_source(input: &UserInput) -> bool {
    matches!(
        input,
        UserInput::LocalImage { .. }
            | UserInput::LocalFile { .. }
            | UserInput::LocalAudio { .. }
            | UserInput::LocalVideo { .. }
    )
}

fn is_url_attachment_source(input: &UserInput) -> bool {
    matches!(
        input,
        UserInput::Image { .. }
            | UserInput::File { .. }
            | UserInput::Audio { .. }
            | UserInput::Video { .. }
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionArtifactGrant {
    pub(crate) artifact_id: String,
    pub(crate) version_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionProviderGrant {
    provider: String,
    model: String,
    authority_fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionCliGrant {
    runtime_id: String,
    model: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionGrantManifest {
    version: u32,
    entry_point: ExecutionAdmissionEntryPoint,
    operational_projection_fingerprint: String,
    actions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    provider: Option<ExecutionProviderGrant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cli: Option<ExecutionCliGrant>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    skills: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    mcp_servers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    artifacts: Vec<ExecutionArtifactGrant>,
}

impl ExecutionGrantManifest {
    fn allows_action(&self, action: ResourceAction) -> bool {
        self.actions
            .binary_search_by(|candidate| candidate.as_str().cmp(action.safe_name()))
            .is_ok()
    }

    fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.operational_projection_fingerprint.len() != 64
            || !self
                .operational_projection_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.actions.windows(2).any(|pair| pair[0] >= pair[1])
            || self
                .actions
                .iter()
                .any(|action| ResourceAction::from_safe_name(action).is_none())
            || self.skills.windows(2).any(|pair| pair[0] >= pair[1])
            || self.mcp_servers.windows(2).any(|pair| pair[0] >= pair[1])
            || self
                .artifacts
                .windows(2)
                .any(|pair| artifact_grant_key(&pair[0]) >= artifact_grant_key(&pair[1]))
        {
            bail!("invalid execution grant manifest");
        }
        match (&self.provider, &self.cli) {
            (Some(_), None) if self.allows_action(ResourceAction::ProviderUse) => {}
            (None, Some(_)) if self.allows_action(ResourceAction::CliRuntimeUse) => {}
            _ => bail!("execution grant manifest has an invalid backend grant"),
        }
        if let Some(provider) = self.provider.as_ref() {
            validate_non_empty_identity("provider", provider.provider.as_str())?;
            validate_non_empty_identity("model", provider.model.as_str())?;
            validate_sha256_identity(
                "provider authority",
                provider.authority_fingerprint.as_str(),
            )?;
        }
        if let Some(cli) = self.cli.as_ref() {
            validate_non_empty_identity("CLI runtime", cli.runtime_id.as_str())?;
            validate_non_empty_identity("CLI model", cli.model.as_str())?;
        }
        for skill_id in &self.skills {
            validate_non_empty_identity("skill", skill_id)?;
        }
        for server_id in &self.mcp_servers {
            validate_non_empty_identity("MCP server", server_id)?;
        }
        for artifact in &self.artifacts {
            validate_non_empty_identity("artifact", artifact.artifact_id.as_str())?;
            validate_non_empty_identity("artifact version", artifact.version_id.as_str())?;
        }
        Ok(())
    }
}

fn artifact_grant_key(grant: &ExecutionArtifactGrant) -> (&str, &str) {
    (grant.artifact_id.as_str(), grant.version_id.as_str())
}

fn validate_non_empty_identity(kind: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value != value.trim() {
        bail!("invalid {kind} identity");
    }
    Ok(())
}

fn validate_sha256_identity(kind: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("invalid {kind} fingerprint");
    }
    Ok(())
}

#[cfg(test)]
fn test_execution_grant_manifest(
    execution_backend: Option<&AgentExecutionBackend>,
    provider: &str,
    model: &str,
) -> ExecutionGrantManifest {
    let mut actions = EXECUTION_RUNTIME_ACTIONS
        .iter()
        .map(|action| action.safe_name().to_owned())
        .collect::<Vec<_>>();
    actions.sort();
    actions.dedup();
    let (provider, cli) = match execution_backend {
        Some(AgentExecutionBackend::CLIAgentRuntime { runtime_id, .. })
        | Some(AgentExecutionBackend::ACPAgentRuntime { runtime_id }) => (
            None,
            Some(ExecutionCliGrant {
                runtime_id: runtime_id.clone(),
                model: model.to_owned(),
            }),
        ),
        _ => (
            Some(ExecutionProviderGrant {
                provider: provider.to_owned(),
                model: model.to_owned(),
                authority_fingerprint: "a".repeat(64),
            }),
            None,
        ),
    };
    ExecutionGrantManifest {
        version: 1,
        entry_point: ExecutionAdmissionEntryPoint::InteractiveTurn,
        operational_projection_fingerprint: "b".repeat(64),
        actions,
        provider,
        cli,
        skills: Vec::new(),
        mcp_servers: Vec::new(),
        artifacts: Vec::new(),
    }
}

#[derive(Clone)]
pub(crate) struct ExecutionAdmissionService {
    resolver: AuthorizationResolver,
    policy: AuthorizationService,
}

impl ExecutionAdmissionService {
    pub(crate) fn new(store: CrudStore) -> Self {
        Self {
            resolver: AuthorizationResolver::new(store.clone()),
            policy: AuthorizationService::new(),
        }
    }

    pub(crate) async fn admit(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        policy_revision: u64,
        request: &ExecutionAdmissionRequest,
    ) -> Result<ExecutionGrantManifest> {
        self.admit_with_runtime_draft(principal, policy_revision, request, None)
            .await
    }

    async fn admit_with_runtime_draft(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        policy_revision: u64,
        request: &ExecutionAdmissionRequest,
        runtime_draft: Option<&RuntimeDraftAccess>,
    ) -> Result<ExecutionGrantManifest> {
        validate_non_empty_identity("workspace", request.workspace_id.as_str())?;
        validate_non_empty_identity("root thread", request.root_thread_id.as_str())?;
        validate_non_empty_identity("provider", request.provider.as_str())?;
        validate_non_empty_identity("model", request.model.as_str())?;
        if self
            .policy
            .runtime_principal_policy(principal.kind, principal.role_key.as_ref())
            == Some(RuntimePrincipalPolicy::ScopedCollaboration)
            && (request.has_local_attachment_sources || request.has_url_attachment_sources)
        {
            bail!(
                "scoped execution attachments require an authorized exact-version artifact; local paths and external URLs are not accepted"
            );
        }

        let mut actions = BTreeSet::new();
        self.require_root_action(
            principal,
            request,
            request.required_root_action,
            runtime_draft,
        )
        .await?;
        actions.insert(request.required_root_action.safe_name().to_owned());
        for action in request.additional_required_actions.iter().copied() {
            self.require_root_action(principal, request, action, runtime_draft)
                .await?;
            actions.insert(action.safe_name().to_owned());
        }

        let (provider, cli) = match request.execution_backend.as_ref() {
            Some(AgentExecutionBackend::CLIAgentRuntime { runtime_id, .. })
            | Some(AgentExecutionBackend::ACPAgentRuntime { runtime_id }) => {
                self.require_root_action(
                    principal,
                    request,
                    ResourceAction::CliRuntimeUse,
                    runtime_draft,
                )
                .await?;
                if !self.policy.cli_model_allowed(
                    principal.kind,
                    principal.role_key.as_ref(),
                    runtime_id,
                    request.model.as_str(),
                ) {
                    bail!("selected CLI runtime/model is outside the role projection");
                }
                actions.insert(ResourceAction::CliRuntimeUse.safe_name().to_owned());
                (
                    None,
                    Some(ExecutionCliGrant {
                        runtime_id: runtime_id.clone(),
                        model: request.model.clone(),
                    }),
                )
            }
            _ => {
                self.require_root_action(
                    principal,
                    request,
                    ResourceAction::ProviderUse,
                    runtime_draft,
                )
                .await?;
                if !self.policy.provider_model_allowed(
                    principal.kind,
                    principal.role_key.as_ref(),
                    request.provider.as_str(),
                    request.model.as_str(),
                ) {
                    bail!("selected provider/model is outside the role projection");
                }
                let authority_fingerprint = request
                    .provider_authority_fingerprint
                    .as_deref()
                    .context("provider execution has no resolved authority fingerprint")?;
                validate_sha256_identity("provider authority", authority_fingerprint)?;
                actions.insert(ResourceAction::ProviderUse.safe_name().to_owned());
                (
                    Some(ExecutionProviderGrant {
                        provider: request.provider.clone(),
                        model: request.model.clone(),
                        authority_fingerprint: authority_fingerprint.to_owned(),
                    }),
                    None,
                )
            }
        };

        let mut skills = BTreeSet::new();
        let mut mcp_servers = BTreeSet::new();
        for capability in &request.capabilities {
            match &capability.kind {
                pioneer_protocol::TurnCapabilityKind::Skill { skill_id, .. } => {
                    self.require_root_action(
                        principal,
                        request,
                        ResourceAction::SkillUse,
                        runtime_draft,
                    )
                    .await?;
                    if !self.policy.skill_allowed(
                        principal.kind,
                        principal.role_key.as_ref(),
                        skill_id.as_str(),
                    ) {
                        bail!("selected skill is outside the role projection");
                    }
                    actions.insert(ResourceAction::SkillUse.safe_name().to_owned());
                    skills.insert(skill_id.to_string());
                }
                pioneer_protocol::TurnCapabilityKind::McpServer { name, .. }
                | pioneer_protocol::TurnCapabilityKind::McpTool {
                    server_name: name, ..
                } => {
                    self.require_root_action(
                        principal,
                        request,
                        ResourceAction::McpUse,
                        runtime_draft,
                    )
                    .await?;
                    if !self.policy.mcp_server_allowed(
                        principal.kind,
                        principal.role_key.as_ref(),
                        name.as_str(),
                    ) {
                        bail!("selected MCP server is outside the role projection");
                    }
                    actions.insert(ResourceAction::McpUse.safe_name().to_owned());
                    mcp_servers.insert(name.clone());
                }
                pioneer_protocol::TurnCapabilityKind::SkillPack { .. } => {
                    bail!("unexpanded skill pack reached execution admission");
                }
            }
        }

        let mut artifacts = request.artifacts.clone();
        artifacts.sort_by(|left, right| artifact_grant_key(left).cmp(&artifact_grant_key(right)));
        artifacts.dedup_by(|left, right| artifact_grant_key(left) == artifact_grant_key(right));
        for artifact in &artifacts {
            validate_non_empty_identity("artifact", artifact.artifact_id.as_str())?;
            validate_non_empty_identity("artifact version", artifact.version_id.as_str())?;
            let gate = self.policy.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::ArtifactRead,
            );
            let resolved = match runtime_draft {
                Some(access)
                    if access.workspace_id() == request.workspace_id
                        && access.thread_id() == request.root_thread_id =>
                {
                    self.resolver
                        .authorize_runtime_draft_artifact(
                            principal,
                            &gate,
                            ResourceAction::ArtifactRead,
                            artifact.artifact_id.as_str(),
                            access,
                        )
                        .await
                }
                Some(_) => bail!("runtime draft differs from the execution artifact root"),
                None => {
                    self.resolver
                        .authorize_artifact(
                            principal,
                            &gate,
                            ResourceAction::ArtifactRead,
                            artifact.artifact_id.as_str(),
                            Some(request.workspace_id.as_str()),
                            Some(request.root_thread_id.as_str()),
                        )
                        .await
                }
            }
            .context("failed to resolve execution artifact grant")?;
            if !matches!(resolved, ProofResolution::Authorized(_)) {
                bail!("selected artifact is outside the execution root");
            }
            actions.insert(ResourceAction::ArtifactRead.safe_name().to_owned());
        }

        // This is the immutable capability ceiling of the collaborative
        // execution capsule, not a snapshot of the initiating role. Admission
        // above still proves every action/resource needed to start the work.
        // Each later operation is independently intersected with the current
        // caller/collaborator role by ExecutionLeaseRegistry. Keeping the
        // initiator's action subset here would make a future `runner` the
        // accidental owner and prevent an `approver` or `reviewer` in the same
        // thread from exercising its own role actions.
        for action in EXECUTION_RUNTIME_ACTIONS {
            actions.insert(action.safe_name().to_owned());
        }

        let operational_projection_fingerprint = self
            .policy
            .operational_projection(
                principal.kind,
                principal.role_key.as_ref(),
                request.workspace_id.as_str(),
                policy_revision,
            )
            .context("execution principal has no operational projection")?
            .fingerprint;
        let manifest = ExecutionGrantManifest {
            version: 1,
            entry_point: request.entry_point,
            operational_projection_fingerprint,
            actions: actions.into_iter().collect(),
            provider,
            cli,
            skills: skills.into_iter().collect(),
            mcp_servers: mcp_servers.into_iter().collect(),
            artifacts,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub(crate) async fn admit_context(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        policy_revision: u64,
        request: &ExecutionAdmissionRequest,
        requested_permission_cap: Option<&TurnPermissionProfileCap>,
    ) -> Result<ExecutionAuthorizationContext> {
        let grant_manifest = self.admit(principal, policy_revision, request).await?;
        let role_cap = self
            .policy
            .turn_permission_profile_cap(principal.kind, principal.role_key.as_ref())
            .context("execution principal has no permission cap")?;
        let human_interaction_budget = self
            .policy
            .human_interaction_budget(principal.kind, principal.role_key.as_ref())
            .context("execution principal has no human interaction budget")?;
        let mcp_invocation_limits = self
            .policy
            .mcp_invocation_resource_limits(principal.kind, principal.role_key.as_ref())
            .context("execution principal has no MCP invocation resource policy")?;
        let native_event_budget = self
            .policy
            .native_event_resource_budget(principal.kind, principal.role_key.as_ref())
            .context("execution principal has no native event resource policy")?;
        let role_cap_snapshot = pioneer_protocol::task_permission_cap_snapshot(&role_cap);
        let effective_snapshot = match requested_permission_cap {
            Some(requested) => pioneer_protocol::intersect_turn_permission_profiles(
                &pioneer_protocol::task_permission_cap_snapshot(requested),
                &role_cap_snapshot,
                pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
            ),
            None => role_cap_snapshot,
        };
        let permission_profile_cap =
            pioneer_protocol::task_permission_cap_from_snapshot(&effective_snapshot);
        let role_key = self
            .policy
            .resolved_role_key(principal.kind, principal.role_key.as_ref())
            .context("execution principal has an unsupported role")?
            .to_owned();
        let capability_projection_fingerprint = capability_projection_fingerprint(
            request.workspace_id.as_str(),
            request.root_thread_id.as_str(),
            request.provider.as_str(),
            request.model.as_str(),
            request.execution_backend.as_ref(),
            request.capabilities.as_slice(),
            &permission_profile_cap,
        )?;
        Ok(ExecutionAuthorizationContext {
            version: EXECUTION_AUTHORIZATION_CONTEXT_VERSION,
            authority: ExecutionAuthorityEnvelope::PrincipalGrant {
                principal_id: principal.principal_id.clone(),
                session_id: principal.session_id.clone(),
                principal_kind: principal.kind,
                role_key: role_key.clone(),
            },
            initiating_principal_id: principal.principal_id.clone(),
            initiating_session_id: principal.session_id.clone(),
            workspace_id: request.workspace_id.clone(),
            root_thread_id: request.root_thread_id.clone(),
            policy_revision,
            role_key,
            policy_fingerprint: super::RoleDefinitionRegistry::new().policy_fingerprint(),
            capability_projection_fingerprint,
            permission_profile_cap,
            human_interaction_budget,
            mcp_invocation_limits,
            native_event_budget,
            continuity_policy: ExecutionContinuityPolicy::StopOnAuthorityLoss,
            resource_boundary: ExecutionResourceBoundary::RootThreadCapsule,
            grant_manifest,
            mcp_projection: None,
            skill_projection: None,
            cli_runtime_projection: cli_runtime_projection(request.execution_backend.as_ref()),
        })
    }

    async fn require_root_action(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        request: &ExecutionAdmissionRequest,
        action: ResourceAction,
        runtime_draft: Option<&RuntimeDraftAccess>,
    ) -> Result<()> {
        if !self
            .root_action_allowed(principal, request, action, runtime_draft)
            .await?
        {
            bail!("execution admission denied action `{}`", action.safe_name());
        }
        Ok(())
    }

    async fn root_action_allowed(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        request: &ExecutionAdmissionRequest,
        action: ResourceAction,
        runtime_draft: Option<&RuntimeDraftAccess>,
    ) -> Result<bool> {
        let gate =
            self.policy
                .authorize_action(principal.kind, principal.role_key.as_ref(), action);
        let resolved = match runtime_draft {
            Some(access)
                if access.workspace_id() == request.workspace_id
                    && access.thread_id() == request.root_thread_id =>
            {
                self.resolver
                    .authorize_runtime_draft(principal, &gate, action, access)
                    .await
            }
            Some(_) => bail!("runtime draft differs from the execution root"),
            None => {
                self.resolver
                    .authorize_thread(
                        principal,
                        &gate,
                        action,
                        request.root_thread_id.as_str(),
                        Some(request.workspace_id.as_str()),
                    )
                    .await
            }
        }
        .with_context(|| {
            format!(
                "failed to resolve execution action `{}`",
                action.safe_name()
            )
        })?;
        Ok(matches!(resolved, ProofResolution::Authorized(_)))
    }
}

/// Typed authority provenance. Missing provenance is never interpreted as an
/// internal or unrestricted grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ExecutionAuthorityEnvelope {
    PrincipalGrant {
        principal_id: PrincipalId,
        session_id: AuthSessionId,
        principal_kind: PrincipalKind,
        role_key: String,
    },
    SystemGrant {
        issuer: String,
        policy_generation: u64,
    },
    ServiceGrant {
        issuer: String,
        service_id: String,
        policy_generation: u64,
    },
}

/// Immutable, non-secret admission context for execution.
///
/// The value is persisted for restart/recovery, but it is not a credential:
/// every privileged continuation must still re-resolve current authorization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionAuthorizationContext {
    version: u32,
    authority: ExecutionAuthorityEnvelope,
    initiating_principal_id: PrincipalId,
    initiating_session_id: AuthSessionId,
    workspace_id: String,
    root_thread_id: String,
    policy_revision: u64,
    role_key: String,
    policy_fingerprint: String,
    capability_projection_fingerprint: String,
    permission_profile_cap: TurnPermissionProfileCap,
    human_interaction_budget: HumanInteractionBudget,
    mcp_invocation_limits: pioneer_protocol::McpInvocationResourceLimits,
    native_event_budget: pioneer_cli_agent_runtime::NativeEventBudget,
    continuity_policy: ExecutionContinuityPolicy,
    resource_boundary: ExecutionResourceBoundary,
    grant_manifest: ExecutionGrantManifest,
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
            authority: ExecutionAuthorityEnvelope::PrincipalGrant {
                principal_id: principal.principal_id.clone(),
                session_id: principal.session_id.clone(),
                principal_kind: principal.kind,
                role_key: AuthorizationService::new()
                    .resolved_role_key(principal.kind, principal.role_key.as_ref())
                    .expect("test principal role must be registered")
                    .to_owned(),
            },
            initiating_principal_id: principal.principal_id.clone(),
            initiating_session_id: principal.session_id.clone(),
            workspace_id: workspace_id.to_owned(),
            root_thread_id: root_thread_id.to_owned(),
            policy_revision: PolicyGeneration::INITIAL.get(),
            role_key: AuthorizationService::new()
                .resolved_role_key(principal.kind, principal.role_key.as_ref())
                .expect("test principal role must be registered")
                .to_owned(),
            policy_fingerprint: crate::authorization::RoleDefinitionRegistry::new()
                .policy_fingerprint(),
            capability_projection_fingerprint,
            permission_profile_cap,
            human_interaction_budget: HumanInteractionBudget::DEFAULT,
            mcp_invocation_limits: AuthorizationService::new()
                .mcp_invocation_resource_limits(principal.kind, principal.role_key.as_ref())
                .expect("test principal role must have MCP invocation limits"),
            native_event_budget: AuthorizationService::new()
                .native_event_resource_budget(principal.kind, principal.role_key.as_ref())
                .expect("test principal role must have native event limits"),
            continuity_policy: ExecutionContinuityPolicy::StopOnAuthorityLoss,
            resource_boundary: ExecutionResourceBoundary::RootThreadCapsule,
            grant_manifest: test_execution_grant_manifest(
                execution_backend,
                "test-provider",
                "test-model",
            ),
            mcp_projection: None,
            skill_projection: None,
            cli_runtime_projection: cli_runtime_projection(execution_backend),
        }
    }

    #[cfg(test)]
    pub(crate) fn bind_test_provider_authority(
        &mut self,
        provider: &str,
        model: &str,
        authority_fingerprint: &str,
    ) {
        assert!(self.grant_manifest.cli.is_none());
        self.grant_manifest.provider = Some(ExecutionProviderGrant {
            provider: provider.to_owned(),
            model: model.to_owned(),
            authority_fingerprint: authority_fingerprint.to_owned(),
        });
    }

    pub(crate) fn initiating_principal_id(&self) -> &PrincipalId {
        &self.initiating_principal_id
    }

    pub(crate) fn initiating_session_id(&self) -> &AuthSessionId {
        &self.initiating_session_id
    }

    /// Verifies that immutable authority provenance still has an exact
    /// durable actor row. Current role/status are intentionally not compared:
    /// those mutable facts are reprojected by the execution lease, while a
    /// missing or differently typed actor is an integrity failure.
    pub(crate) fn verify_persisted_actor_binding(
        &self,
        initiator: Option<&pioneer_protocol::PersistedActorRef>,
        persisted_principal: Option<(&PrincipalId, PrincipalKind)>,
    ) -> Result<()> {
        match &self.authority {
            ExecutionAuthorityEnvelope::PrincipalGrant {
                principal_id,
                principal_kind,
                ..
            } => {
                let Some(pioneer_protocol::PersistedActorRef::Principal(actor_id)) = initiator
                else {
                    bail!("execution principal grant has no durable principal initiator");
                };
                if actor_id != principal_id {
                    bail!("execution principal grant differs from its durable initiator");
                }
                let Some((persisted_id, persisted_kind)) = persisted_principal else {
                    bail!("execution principal grant references a missing principal");
                };
                if persisted_id != principal_id || persisted_kind != *principal_kind {
                    bail!("execution principal grant differs from its durable principal row");
                }
            }
            ExecutionAuthorityEnvelope::SystemGrant { .. } => {
                if initiator != Some(&pioneer_protocol::PersistedActorRef::System) {
                    bail!("execution System grant differs from its durable initiator");
                }
            }
            ExecutionAuthorityEnvelope::ServiceGrant { .. } => {
                bail!("execution Service grant has no installed durable actor binding");
            }
        }
        Ok(())
    }

    pub(crate) fn grants_action(&self, action: ResourceAction) -> bool {
        self.grant_manifest.allows_action(action)
    }

    pub(crate) const fn human_interaction_budget(&self) -> HumanInteractionBudget {
        self.human_interaction_budget
    }

    pub(crate) fn permission_profile_cap(&self) -> &TurnPermissionProfileCap {
        &self.permission_profile_cap
    }

    #[cfg(test)]
    pub(crate) const fn mcp_invocation_resource_limits(
        &self,
    ) -> pioneer_protocol::McpInvocationResourceLimits {
        self.mcp_invocation_limits
    }

    pub(crate) fn effective_mcp_invocation_resource_limits(
        &self,
    ) -> Result<pioneer_protocol::McpInvocationResourceLimits> {
        let (principal_kind, role_key) = self.registered_role_identity()?;
        let current = AuthorizationService::new()
            .mcp_invocation_resource_limits(principal_kind, role_key.as_ref())
            .context("execution role has no current MCP invocation resource policy")?;
        self.mcp_invocation_limits
            .intersect(current)
            .context("execution MCP invocation resource policy is incompatible")
    }

    #[cfg(test)]
    pub(crate) const fn native_event_resource_budget(
        &self,
    ) -> pioneer_cli_agent_runtime::NativeEventBudget {
        self.native_event_budget
    }

    pub(crate) fn effective_native_event_resource_budget(
        &self,
    ) -> Result<pioneer_cli_agent_runtime::NativeEventBudget> {
        let (principal_kind, role_key) = self.registered_role_identity()?;
        let current = AuthorizationService::new()
            .native_event_resource_budget(principal_kind, role_key.as_ref())
            .context("execution role has no current native event resource policy")?;
        self.native_event_budget
            .intersect(current)
            .context("execution native event resource policy is incompatible")
    }

    pub(crate) fn approval_scope_cap(&self) -> pioneer_protocol::TurnApprovalScopePolicySnapshot {
        approval_scope_policy_for_mode(self.permission_profile_cap.mode)
    }

    pub(crate) fn continuation_action(&self) -> ResourceAction {
        if self.grant_manifest.cli.is_some() {
            ResourceAction::CliRuntimeUse
        } else {
            ResourceAction::ProviderUse
        }
    }

    pub(crate) fn continuation_provider(&self) -> Option<(&str, &str)> {
        self.grant_manifest
            .provider
            .as_ref()
            .map(|grant| (grant.provider.as_str(), grant.model.as_str()))
    }

    pub(crate) fn continuation_cli_runtime(&self) -> Option<(&str, &str)> {
        self.grant_manifest
            .cli
            .as_ref()
            .map(|grant| (grant.runtime_id.as_str(), grant.model.as_str()))
    }

    pub(crate) fn granted_action_names(&self) -> &[String] {
        self.grant_manifest.actions.as_slice()
    }

    pub(crate) fn granted_skill_ids(&self) -> &[String] {
        self.grant_manifest.skills.as_slice()
    }

    pub(crate) fn granted_mcp_server_ids(&self) -> &[String] {
        self.grant_manifest.mcp_servers.as_slice()
    }

    pub(crate) fn workspace_id(&self) -> &str {
        self.workspace_id.as_str()
    }

    pub(crate) fn root_thread_id(&self) -> &str {
        self.root_thread_id.as_str()
    }

    pub(crate) fn role_key(&self) -> &str {
        self.role_key.as_str()
    }

    pub(crate) fn runtime_principal_policy(&self) -> Result<RuntimePrincipalPolicy> {
        let (principal_kind, role_key) = self.registered_role_identity()?;
        AuthorizationService::new()
            .runtime_principal_policy(principal_kind, role_key.as_ref())
            .context("execution role has no runtime principal policy")
    }

    pub(crate) fn policy_fingerprint(&self) -> &str {
        self.policy_fingerprint.as_str()
    }

    pub(crate) const fn policy_revision(&self) -> u64 {
        self.policy_revision
    }

    pub(crate) const fn continuity_policy(&self) -> ExecutionContinuityPolicy {
        self.continuity_policy
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

    pub(crate) fn verify_current_provider_authority(
        &self,
        registry: &pioneer_provider::ProviderRegistry,
    ) -> Result<()> {
        let Some(provider) = self.grant_manifest.provider.as_ref() else {
            return Ok(());
        };
        let current = registry.authority_fingerprint_for_workspace(
            self.workspace_id.as_str(),
            provider.provider.as_str(),
        );
        if current.as_str() != provider.authority_fingerprint {
            bail!("provider authority changed; execution requires a fresh admission");
        }
        Ok(())
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
        match &context.authority {
            ExecutionAuthorityEnvelope::PrincipalGrant {
                principal_id,
                session_id,
                principal_kind,
                role_key,
            } if principal_id == &context.initiating_principal_id
                && session_id == &context.initiating_session_id
                && role_key == &context.role_key
                && RoleKey::new(role_key.clone()).ok().is_some_and(|role_key| {
                    super::RoleDefinitionRegistry::new()
                        .resolve_key(&role_key)
                        .is_some_and(|definition| definition.principal_kind == *principal_kind)
                }) => {}
            ExecutionAuthorityEnvelope::PrincipalGrant { .. } => {
                bail!("execution principal grant differs from its admission identity")
            }
            ExecutionAuthorityEnvelope::SystemGrant { .. }
            | ExecutionAuthorityEnvelope::ServiceGrant { .. } => {
                bail!("execution authority variant has no installed revalidator")
            }
        }
        if context.workspace_id.trim().is_empty()
            || context.root_thread_id.trim().is_empty()
            || PolicyGeneration::new(context.policy_revision).is_none()
            || RoleKey::new(context.role_key.clone()).is_err()
            || context.policy_fingerprint.len() != 64
            || !context
                .policy_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || context.capability_projection_fingerprint.len() != 64
            || !context.mcp_invocation_limits.is_valid()
            || !context.native_event_budget.is_valid()
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
        context.grant_manifest.validate()?;
        Ok(context)
    }

    /// Loads and binds an execution context to the exact durable Turn. This is
    /// the only runtime loading path: parse-valid JSON is insufficient without
    /// matching actor provenance, workspace, and root-capsule lineage.
    pub(crate) fn load_for_turn<'a>(
        store: &'a CrudStore,
        turn_id: &'a str,
    ) -> BoxFuture<'a, Result<Self>> {
        // Walk the lineage iteratively. Merely boxing a recursive future still
        // nests every ancestor's `poll` call on the same Tokio worker stack;
        // a durable lineage can be arbitrarily deep and therefore must not be
        // represented by Rust call-stack depth.
        Box::pin(async move {
            let mut current_turn_id = turn_id.to_owned();
            let mut requested_context = None;
            let mut visited = BTreeSet::new();

            loop {
                if !visited.insert(current_turn_id.clone()) {
                    bail!("execution Turn lineage contains a cycle");
                }
                let encoded = store
                    .get_turn_execution_authorization_context(current_turn_id.as_str())
                    .await?
                    .with_context(|| {
                        format!("turn `{current_turn_id}` has no durable authority envelope")
                    })?;
                let context = Self::from_persisted_json(encoded.as_str()).with_context(|| {
                    format!("turn `{current_turn_id}` has an invalid authority envelope")
                })?;
                if requested_context.is_none() {
                    requested_context = Some(context.clone());
                }

                match context
                    .verify_durable_turn_binding(store, current_turn_id.as_str())
                    .await?
                {
                    Some(parent_turn_id) => current_turn_id = parent_turn_id,
                    None => {
                        return requested_context
                            .context("execution Turn validation produced no requested context");
                    }
                }
            }
        })
    }

    async fn verify_durable_turn_binding(
        &self,
        store: &CrudStore,
        turn_id: &str,
    ) -> Result<Option<String>> {
        let scope = pioneer_crud::resolve_turn_authorization_scope(
            &store.database_connection(),
            turn_id,
            None,
            None,
        )
        .await?
        .with_context(|| format!("execution turn `{turn_id}` no longer exists"))?;
        let (_, turn) = store
            .get_turn(scope.thread_id.as_str(), turn_id)
            .await?
            .with_context(|| format!("execution turn `{turn_id}` disappeared during validation"))?;
        let initiator = pioneer_crud::find_turn_initiator(&store.database_connection(), turn_id)
            .await?
            .with_context(|| format!("execution turn `{turn_id}` has no durable initiator"))?;
        let persisted_principal = pioneer_crud::load_principal_by_id(
            &store.database_connection(),
            &self.initiating_principal_id,
        )
        .await?
        .map(|principal| (principal.id, principal.kind));
        let grant_actor =
            pioneer_protocol::PersistedActorRef::Principal(self.initiating_principal_id.clone());
        self.verify_persisted_actor_binding(
            Some(&grant_actor),
            persisted_principal
                .as_ref()
                .map(|(principal_id, principal_kind)| (principal_id, *principal_kind)),
        )?;

        if scope.workspace_id != self.workspace_id {
            bail!("execution Turn workspace differs from its authority envelope");
        }
        let ancestor_turn_id = match &initiator {
            pioneer_protocol::PersistedActorRef::Principal(principal_id)
                if principal_id == &self.initiating_principal_id =>
            {
                // A current collaborator may start an interactive Turn from
                // any internal child in the same root capsule. Its authority
                // comes from the fresh principal admission plus the durable
                // child -> root lineage checked below; it is not a system-
                // derived TaskRunTurn and must not be forced to impersonate
                // one. System-created child turns continue through the strict
                // typed Task derivation path below.
                None
            }
            pioneer_protocol::PersistedActorRef::Principal(_) => {
                bail!("execution Turn initiator differs from its principal grant")
            }
            pioneer_protocol::PersistedActorRef::System => {
                self.verify_derived_turn(store, &scope, &turn).await?
            }
        };
        if scope.thread_id == self.root_thread_id {
            if scope.thread.access_class == PersistedThreadAccessClass::Internal {
                bail!("execution authority root points at an internal child thread");
            }
            if store
                .get_task_thread_lineage(scope.thread_id.as_str())
                .await?
                .is_some()
            {
                bail!("execution authority root unexpectedly has child-thread lineage");
            }
            return Ok(ancestor_turn_id);
        }

        if scope.thread.access_class != PersistedThreadAccessClass::Internal {
            bail!("execution child Turn is not isolated in an internal thread");
        }
        let lineage = store
            .get_task_thread_lineage(scope.thread_id.as_str())
            .await?
            .context("execution child Turn has no durable lineage")?;
        if lineage.child_thread_id != scope.thread_id
            || lineage.root_thread_id != self.root_thread_id
            || lineage.root_thread_id == lineage.child_thread_id
            || lineage.depth <= 0
        {
            bail!("execution child Turn lineage differs from its authority envelope");
        }
        let root = pioneer_crud::resolve_thread_authorization_scope(
            &store.database_connection(),
            self.root_thread_id.as_str(),
            Some(self.workspace_id.as_str()),
        )
        .await?
        .context("execution authority root thread no longer exists")?;
        if root.access_class == PersistedThreadAccessClass::Internal {
            bail!("execution authority root resolves to another internal child");
        }
        Ok(ancestor_turn_id)
    }

    fn verify_derived_turn<'a>(
        &'a self,
        store: &'a CrudStore,
        scope: &'a pioneer_crud::TurnAuthorizationScope,
        turn: &'a pioneer_protocol::Turn,
    ) -> BoxFuture<'a, Result<Option<String>>> {
        Box::pin(async move {
            // A Task occurrence uses the durable run id as its Turn id and may be
            // projected either at the capsule root or inside an existing child
            // thread. It is fully typed by TaskRun + TaskExecutionAdmission before
            // the new run's own hidden child Turn exists, so requiring a
            // TaskRunTurn binding here creates an impossible ordering cycle.
            if turn.turn_kind == pioneer_protocol::TurnKind::TaskRun
                && let Some(run) = store.get_task_run(turn.id.as_str()).await?
            {
                if run.executor_kind != pioneer_protocol::TaskExecutorKind::Agent {
                    bail!("System Task occurrence is not backed by an Agent run");
                }
                let admission = store
                    .get_task_execution_admission(run.task_id.as_str())
                    .await?
                    .context("System Task occurrence has no durable execution admission")?;
                let admission_context = Self::load_for_task_admission(store, &admission).await?;
                self.verify_authority_derivation(&admission_context)?;
                return Ok(None);
            }
            if scope.thread_id == self.root_thread_id {
                bail!("System execution at a capsule root has no typed Task derivation");
            }

            let lineage = store
                .get_task_thread_lineage(scope.thread_id.as_str())
                .await?
                .context("derived child execution has no durable lineage")?;
            let parent_turn_id = lineage
                .created_by_turn_id
                .as_deref()
                .context("derived child execution lineage has no creating Turn")?;
            if parent_turn_id == turn.id {
                bail!("derived child execution lineage is self-referential");
            }
            let parent_scope = pioneer_crud::resolve_turn_authorization_scope(
                &store.database_connection(),
                parent_turn_id,
                Some(self.workspace_id.as_str()),
                Some(lineage.parent_thread_id.as_str()),
            )
            .await?
            .context("derived child execution creating Turn is missing or outside its parent")?;
            if lineage.created_by_thread_id.as_deref() != Some(parent_scope.thread_id.as_str()) {
                bail!("derived child execution lineage has inconsistent creating thread");
            }
            if parent_scope.thread_id != self.root_thread_id {
                let parent_lineage = store
                    .get_task_thread_lineage(parent_scope.thread_id.as_str())
                    .await?
                    .context("nested child parent has no durable lineage")?;
                if parent_lineage.root_thread_id != self.root_thread_id
                    || parent_lineage.depth >= lineage.depth
                {
                    bail!("nested child lineage does not monotonically approach its root");
                }
            }
            let task_run_turn = store
                .get_task_run_turn_by_turn(scope.thread_id.as_str(), turn.id.as_str())
                .await?
                .context("derived child execution has no typed Task run Turn")?;
            if task_run_turn.thread_id != scope.thread_id || task_run_turn.turn_id != turn.id {
                bail!("derived child execution differs from its Task run Turn binding");
            }
            let run = store
                .get_task_run(task_run_turn.run_id.as_str())
                .await?
                .context("derived child execution has no durable Task run")?;
            if run.id != task_run_turn.run_id
                || run.task_id != task_run_turn.task_id
                || run.executor_kind != pioneer_protocol::TaskExecutorKind::Agent
            {
                bail!("derived child execution has an invalid Task run binding");
            }
            let admission = store
                .get_task_execution_admission(run.task_id.as_str())
                .await?
                .context("derived child execution has no durable Task execution admission")?;
            let admission_context = Self::load_for_task_admission(store, &admission).await?;
            self.verify_authority_derivation(&admission_context)?;
            // The creating Turn proves structural lineage only. In the shared
            // capsule model its initiator is not an ownership boundary; the
            // outer iterative verifier validates that ancestor independently.
            Ok(Some(parent_turn_id.to_owned()))
        })
    }

    fn verify_authority_derivation(&self, admitted: &Self) -> Result<()> {
        self.verify_same_authority_provenance(admitted)?;
        if !self.granted_action_names().iter().all(|action| {
            admitted
                .granted_action_names()
                .iter()
                .any(|admitted| admitted == action)
        }) {
            bail!("derived execution widens its admitted action ceiling");
        }
        Ok(())
    }

    fn verify_same_authority_provenance(&self, parent: &Self) -> Result<()> {
        if self.initiating_principal_id != parent.initiating_principal_id
            || self.initiating_session_id != parent.initiating_session_id
            || self.workspace_id != parent.workspace_id
            || self.root_thread_id != parent.root_thread_id
            || self.role_key != parent.role_key
            || self.policy_revision != parent.policy_revision
            || self.policy_fingerprint != parent.policy_fingerprint
            || self.resource_boundary != parent.resource_boundary
        {
            bail!("derived execution differs from its parent authority provenance");
        }
        Ok(())
    }

    pub(crate) async fn load_for_task_admission(
        store: &CrudStore,
        admission: &pioneer_crud::TaskExecutionAdmissionRecord,
    ) -> Result<Self> {
        let context = Self::from_persisted_json(admission.authorization_context_json.as_str())
            .with_context(|| {
                format!(
                    "Task `{}` has an invalid durable authority envelope",
                    admission.task_id
                )
            })?;
        let task = store
            .get_task(admission.task_id.as_str())
            .await?
            .with_context(|| format!("execution Task `{}` no longer exists", admission.task_id))?;
        if task.task.executor_kind != pioneer_protocol::TaskExecutorKind::Agent
            || task.task.workspace_id != admission.workspace_id
        {
            bail!("Task execution admission differs from its durable Task");
        }
        context
            .verify_task_admission_boundary(
                store,
                admission.workspace_id.as_str(),
                admission.root_thread_id.as_str(),
                admission.initiating_principal_id.as_str(),
            )
            .await?;
        Ok(context)
    }

    pub(crate) async fn verify_task_admission_boundary(
        &self,
        store: &CrudStore,
        workspace_id: &str,
        root_thread_id: &str,
        initiating_principal_id: &str,
    ) -> Result<()> {
        let initiating_principal_id = PrincipalId::new(initiating_principal_id)
            .context("Task execution admission has an invalid initiating principal")?;
        if self.workspace_id != workspace_id
            || self.root_thread_id != root_thread_id
            || self.initiating_principal_id != initiating_principal_id
        {
            bail!("Task execution admission differs from its authority envelope");
        }
        let persisted_principal = pioneer_crud::load_principal_by_id(
            &store.database_connection(),
            &initiating_principal_id,
        )
        .await?
        .map(|principal| (principal.id, principal.kind));
        let initiator = pioneer_protocol::PersistedActorRef::Principal(initiating_principal_id);
        self.verify_persisted_actor_binding(
            Some(&initiator),
            persisted_principal
                .as_ref()
                .map(|(principal_id, principal_kind)| (principal_id, *principal_kind)),
        )?;
        let root = pioneer_crud::resolve_thread_authorization_scope(
            &store.database_connection(),
            root_thread_id,
            Some(workspace_id),
        )
        .await?
        .context("Task execution admission root thread no longer exists")?;
        if root.access_class == PersistedThreadAccessClass::Internal {
            bail!("Task execution admission cannot use an internal child as its root");
        }
        Ok(())
    }

    fn registered_role_identity(&self) -> Result<(PrincipalKind, Option<RoleKey>)> {
        let role_key = RoleKey::new(self.role_key.clone()).context("execution role is invalid")?;
        let definition = super::RoleDefinitionRegistry::new()
            .resolve_key(&role_key)
            .context("execution role is not registered")?;
        let scoped_role_key = match definition.principal_kind {
            PrincipalKind::Superuser => None,
            PrincipalKind::User => Some(role_key),
        };
        Ok((definition.principal_kind, scoped_role_key))
    }

    pub(crate) fn admitted_resource_budgets(
        &self,
    ) -> Result<(
        pioneer_crud::ExecutionAdmissionQuotaPolicy,
        pioneer_protocol::TaskResourceBudget,
    )> {
        let (principal_kind, role_key) = self.registered_role_identity()?;
        let policy = AuthorizationService::new();
        let execution = policy
            .execution_resource_policy(principal_kind, role_key.as_ref())
            .context("execution role has no registered resource policy")?;
        let tasks = policy
            .task_resource_budget(principal_kind, role_key.as_ref())
            .context("execution role has no registered Task resource budget")?;
        Ok((execution, tasks))
    }

    /// Builds a concrete Turn admission from an immutable execution context
    /// after its current collaboration authority has been revalidated. The
    /// context remains frozen at its original admission generation; only this
    /// new Turn reservation is stamped with the generation that was actually
    /// checked. The repository still compares that generation atomically at
    /// insert time, so a concurrent authority change remains fail closed.
    pub(crate) fn durable_turn_admission_after_revalidation(
        &self,
        actual_thread_id: &str,
        turn_id: &str,
        execution_backend: Option<&AgentExecutionBackend>,
        revalidated: &RevalidatedExecutionAuthorization,
    ) -> Result<pioneer_crud::NewTurnAdmission> {
        revalidated.verify_context(self)?;
        let current_policy_fingerprint =
            crate::authorization::RoleDefinitionRegistry::new().policy_fingerprint();
        if revalidated.validated_policy_fingerprint != current_policy_fingerprint {
            bail!("revalidated Turn admission policy fingerprint is stale");
        }
        self.build_durable_turn_admission(
            actual_thread_id,
            turn_id,
            execution_backend,
            revalidated.validated_policy_generation,
            revalidated.validated_policy_fingerprint.as_str(),
        )
    }

    fn build_durable_turn_admission(
        &self,
        actual_thread_id: &str,
        turn_id: &str,
        execution_backend: Option<&AgentExecutionBackend>,
        validated_policy_generation: u64,
        validated_policy_fingerprint: &str,
    ) -> Result<pioneer_crud::NewTurnAdmission> {
        if actual_thread_id.trim().is_empty() || turn_id.trim().is_empty() {
            bail!("durable Turn admission requires exact thread and Turn ids");
        }
        let (principal_kind, scoped_role_key) = self.registered_role_identity()?;
        let policy = AuthorizationService::new()
            .execution_resource_policy(principal_kind, scoped_role_key.as_ref())
            .context("execution role has no registered resource policy")?;
        let operation_class = if matches!(
            execution_backend,
            Some(AgentExecutionBackend::CLIAgentRuntime { .. })
                | Some(AgentExecutionBackend::ACPAgentRuntime { .. })
        ) {
            pioneer_crud::ExecutionAdmissionClass::CliProcess
        } else if actual_thread_id == self.root_thread_id {
            pioneer_crud::ExecutionAdmissionClass::InteractiveTurn
        } else {
            pioneer_crud::ExecutionAdmissionClass::AttachedChild
        };
        let authorization_fingerprint = self.authorization_fingerprint()?;
        let mut digest = Sha256::new();
        digest.update(b"pioneer:durable-turn-admission:v1\0");
        digest.update(authorization_fingerprint.as_bytes());
        digest.update(b"\0");
        digest.update(actual_thread_id.as_bytes());
        digest.update(b"\0");
        digest.update(turn_id.as_bytes());

        Ok(pioneer_crud::NewTurnAdmission {
            turn_id: turn_id.to_owned(),
            thread_id: actual_thread_id.to_owned(),
            workspace_id: self.workspace_id.clone(),
            request_digest: hex::encode(digest.finalize()),
            policy_generation: Some(validated_policy_generation),
            role_key: Some(self.role_key.clone()),
            policy_fingerprint: Some(validated_policy_fingerprint.to_owned()),
            execution_lease: Some(super::ExecutionAdmissionGovernor::lease(
                self.initiating_principal_id.as_str(),
                self.role_key.as_str(),
                self.workspace_id.as_str(),
                validated_policy_fingerprint,
                policy,
                operation_class,
                "turn",
                turn_id,
            )),
        })
    }

    pub(crate) fn bind_mcp_projection(
        &mut self,
        workspace_id: &str,
        version: u32,
        manifest_hash: &str,
        server_names: &[String],
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
        if !server_names.is_empty() && !self.grant_manifest.allows_action(ResourceAction::McpUse) {
            bail!("execution admission does not grant MCP use");
        }
        let (principal_kind, role_key) = self.registered_role_identity()?;
        let policy = AuthorizationService::new();
        let mut exact_servers = server_names
            .iter()
            .map(|name| name.trim().to_owned())
            .collect::<Vec<_>>();
        if exact_servers.iter().any(|name| {
            name.is_empty() || !policy.mcp_server_allowed(principal_kind, role_key.as_ref(), name)
        }) {
            bail!("MCP projection contains a server outside the role projection");
        }
        exact_servers.sort();
        exact_servers.dedup();
        self.grant_manifest.mcp_servers = exact_servers;
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
        if !bindings.is_empty() && !self.grant_manifest.allows_action(ResourceAction::SkillUse) {
            bail!("execution admission does not grant skill use");
        }
        let (principal_kind, role_key) = self.registered_role_identity()?;
        let policy = AuthorizationService::new();
        let mut exact_skills = bindings
            .iter()
            .map(|binding| binding.skill_id.to_string())
            .collect::<Vec<_>>();
        if exact_skills.iter().any(|skill_id| {
            !policy.skill_allowed(principal_kind, role_key.as_ref(), skill_id.as_str())
        }) {
            bail!("skill projection contains a skill outside the role projection");
        }
        exact_skills.sort();
        exact_skills.dedup();
        self.grant_manifest.skills = exact_skills;
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
        provider_authority_fingerprint: Option<&str>,
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
        let grant_manifest = self.derive_continuation_manifest(
            provider,
            model,
            execution_backend,
            capabilities,
            provider_authority_fingerprint,
        )?;
        Ok(Self {
            version: EXECUTION_AUTHORIZATION_CONTEXT_VERSION,
            authority: self.authority.clone(),
            initiating_principal_id: self.initiating_principal_id.clone(),
            initiating_session_id: self.initiating_session_id.clone(),
            workspace_id: self.workspace_id.clone(),
            root_thread_id: self.root_thread_id.clone(),
            policy_revision: self.policy_revision,
            role_key: self.role_key.clone(),
            policy_fingerprint: self.policy_fingerprint.clone(),
            capability_projection_fingerprint,
            permission_profile_cap,
            human_interaction_budget: self.human_interaction_budget,
            mcp_invocation_limits: self.mcp_invocation_limits,
            native_event_budget: self.native_event_budget,
            continuity_policy: self.continuity_policy,
            resource_boundary: self.resource_boundary,
            grant_manifest,
            mcp_projection: None,
            skill_projection: None,
            cli_runtime_projection: cli_runtime_projection(execution_backend),
        })
    }

    fn derive_continuation_manifest(
        &self,
        provider: &str,
        model: &str,
        execution_backend: Option<&AgentExecutionBackend>,
        capabilities: &[TurnCapability],
        provider_authority_fingerprint: Option<&str>,
    ) -> Result<ExecutionGrantManifest> {
        let policy = AuthorizationService::new();
        let (principal_kind, role_key) = self.registered_role_identity()?;

        let (provider_grant, cli_grant) = match execution_backend {
            Some(AgentExecutionBackend::CLIAgentRuntime { runtime_id, .. })
            | Some(AgentExecutionBackend::ACPAgentRuntime { runtime_id }) => {
                if !self
                    .grant_manifest
                    .allows_action(ResourceAction::CliRuntimeUse)
                    || !policy.cli_model_allowed(
                        principal_kind,
                        role_key.as_ref(),
                        runtime_id,
                        model,
                    )
                {
                    bail!("task continuation CLI runtime/model is not granted");
                }
                (
                    None,
                    Some(ExecutionCliGrant {
                        runtime_id: runtime_id.clone(),
                        model: model.to_owned(),
                    }),
                )
            }
            _ => {
                if !self
                    .grant_manifest
                    .allows_action(ResourceAction::ProviderUse)
                    || !policy.provider_model_allowed(
                        principal_kind,
                        role_key.as_ref(),
                        provider,
                        model,
                    )
                {
                    bail!("task continuation provider/model is not granted");
                }
                let authority_fingerprint = provider_authority_fingerprint
                    .context("task continuation provider authority is unresolved")?;
                validate_sha256_identity("provider authority", authority_fingerprint)?;
                (
                    Some(ExecutionProviderGrant {
                        provider: provider.to_owned(),
                        model: model.to_owned(),
                        authority_fingerprint: authority_fingerprint.to_owned(),
                    }),
                    None,
                )
            }
        };

        let mut skills = BTreeSet::new();
        let mut mcp_servers = BTreeSet::new();
        for capability in capabilities {
            match &capability.kind {
                pioneer_protocol::TurnCapabilityKind::Skill { skill_id, .. } => {
                    if !self.grant_manifest.allows_action(ResourceAction::SkillUse)
                        || !policy.skill_allowed(
                            principal_kind,
                            role_key.as_ref(),
                            skill_id.as_str(),
                        )
                    {
                        bail!("task continuation skill is not granted");
                    }
                    skills.insert(skill_id.to_string());
                }
                pioneer_protocol::TurnCapabilityKind::McpServer { name, .. }
                | pioneer_protocol::TurnCapabilityKind::McpTool {
                    server_name: name, ..
                } => {
                    if !self.grant_manifest.allows_action(ResourceAction::McpUse)
                        || !policy.mcp_server_allowed(principal_kind, role_key.as_ref(), name)
                    {
                        bail!("task continuation MCP server is not granted");
                    }
                    mcp_servers.insert(name.clone());
                }
                pioneer_protocol::TurnCapabilityKind::SkillPack { .. } => {
                    bail!("unexpanded skill pack reached task continuation admission");
                }
            }
        }

        let manifest = ExecutionGrantManifest {
            version: 1,
            entry_point: ExecutionAdmissionEntryPoint::Task,
            operational_projection_fingerprint: self
                .grant_manifest
                .operational_projection_fingerprint
                .clone(),
            actions: self.grant_manifest.actions.clone(),
            provider: provider_grant,
            cli: cli_grant,
            skills: skills.into_iter().collect(),
            mcp_servers: mcp_servers.into_iter().collect(),
            artifacts: self.grant_manifest.artifacts.clone(),
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Rebuilds a current root-collaboration proof without treating the
    /// initiating actor as the exclusive execution owner. The caller must
    /// first select `principal` from an active execution lease projection.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn revalidate_for_collaborator(
        &self,
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        action: ResourceAction,
        current_policy_revision: u64,
    ) -> Result<RevalidatedExecutionAuthorization> {
        self.verify_turn_scope(store, workspace_id, thread_id, turn_id)
            .await?;
        self.revalidate_root_for_collaborator(store, principal, action, current_policy_revision)
            .await
    }

    pub(crate) async fn revalidate_root_for_collaborator(
        &self,
        store: &CrudStore,
        principal: &AuthenticatedSessionPrincipal,
        action: ResourceAction,
        current_policy_revision: u64,
    ) -> Result<RevalidatedExecutionAuthorization> {
        if !self.grant_manifest.allows_action(action) {
            bail!(
                "execution admission manifest does not grant action `{}`",
                action.safe_name()
            );
        }

        let policy = AuthorizationService::new();
        let current_role_key = policy
            .resolved_role_key(principal.kind, principal.role_key.as_ref())
            .context("execution collaboration principal has an unsupported role")?;
        let action_gate =
            policy.authorize_action(principal.kind, principal.role_key.as_ref(), action);
        let current_role_cap = policy
            .turn_permission_profile_cap(principal.kind, principal.role_key.as_ref())
            .context("execution collaboration role has no permission profile cap")?;
        let admitted_cap =
            pioneer_protocol::task_permission_cap_snapshot(&self.permission_profile_cap);
        let current_cap = pioneer_protocol::task_permission_cap_snapshot(&current_role_cap);
        let effective_permission_profile = pioneer_protocol::intersect_turn_permission_profiles(
            &admitted_cap,
            &current_cap,
            pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
        );
        let authorization = AuthorizationResolver::new(store.clone())
            .authorize_thread(
                principal,
                &action_gate,
                action,
                self.root_thread_id.as_str(),
                Some(self.workspace_id.as_str()),
            )
            .await
            .context("failed to resolve current execution collaboration authority")?;
        let ProofResolution::Authorized(authorization) = authorization else {
            bail!("current collaborator no longer has access to the execution root");
        };
        let current_policy_fingerprint = super::RoleDefinitionRegistry::new().policy_fingerprint();
        if self.policy_revision != current_policy_revision
            || self.policy_fingerprint != current_policy_fingerprint
        {
            record_stale_policy_revision(
                self.policy_revision,
                current_policy_revision,
                self.role_key.as_str(),
                current_role_key,
                self.policy_fingerprint.as_str(),
                current_policy_fingerprint.as_str(),
            );
        }
        Ok(RevalidatedExecutionAuthorization {
            principal: principal.clone(),
            authorization,
            resource_boundary: self.resource_boundary,
            effective_permission_profile,
            admitted_workspace_id: self.workspace_id.clone(),
            admitted_root_thread_id: self.root_thread_id.clone(),
            admitted_principal_id: self.initiating_principal_id.clone(),
            admitted_session_id: self.initiating_session_id.clone(),
            admitted_role_key: self.role_key.clone(),
            admitted_policy_generation: self.policy_revision,
            admitted_policy_fingerprint: self.policy_fingerprint.clone(),
            validated_policy_generation: current_policy_revision,
            validated_policy_fingerprint: current_policy_fingerprint,
        })
    }

    pub(crate) async fn verify_turn_scope(
        &self,
        store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<()> {
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
        Ok(())
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
    resource_boundary: ExecutionResourceBoundary,
    effective_permission_profile: TurnPermissionProfileSnapshot,
    admitted_workspace_id: String,
    admitted_root_thread_id: String,
    admitted_principal_id: PrincipalId,
    admitted_session_id: AuthSessionId,
    admitted_role_key: String,
    admitted_policy_generation: u64,
    admitted_policy_fingerprint: String,
    validated_policy_generation: u64,
    validated_policy_fingerprint: String,
}

impl RevalidatedExecutionAuthorization {
    pub(crate) fn principal(&self) -> &AuthenticatedSessionPrincipal {
        &self.principal
    }

    pub(crate) fn authorization(&self) -> &AuthorizedThread {
        &self.authorization
    }

    pub(crate) const fn resource_boundary(&self) -> ExecutionResourceBoundary {
        self.resource_boundary
    }

    pub(crate) fn effective_permission_profile(&self) -> &TurnPermissionProfileSnapshot {
        &self.effective_permission_profile
    }

    pub(crate) const fn validated_policy_generation(&self) -> u64 {
        self.validated_policy_generation
    }

    fn verify_context(&self, context: &ExecutionAuthorizationContext) -> Result<()> {
        if self.admitted_workspace_id != context.workspace_id
            || self.admitted_root_thread_id != context.root_thread_id
            || self.admitted_principal_id != context.initiating_principal_id
            || self.admitted_session_id != context.initiating_session_id
            || self.admitted_role_key != context.role_key
            || self.admitted_policy_generation != context.policy_revision
            || self.admitted_policy_fingerprint != context.policy_fingerprint
            || self.resource_boundary != context.resource_boundary
        {
            bail!("current authorization proof belongs to a different execution authority");
        }
        let context_permission_profile =
            pioneer_protocol::task_permission_cap_snapshot(&context.permission_profile_cap);
        let effective = pioneer_protocol::intersect_turn_permission_profiles(
            &context_permission_profile,
            &self.effective_permission_profile,
            pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
        );
        if effective != context_permission_profile {
            bail!("execution continuation exceeds its revalidated permission profile");
        }
        Ok(())
    }
}

/// Short-lived admission seed. It can only be created from the authenticated
/// request and either an exact persisted-thread proof or the exact owner
/// capability of a runtime draft, then finalized from server-owned execution
/// state.
#[derive(Clone, Debug)]
pub(crate) struct ExecutionAuthorizationAdmission {
    principal: AuthenticatedSessionPrincipal,
    initiating_principal_id: PrincipalId,
    initiating_session_id: AuthSessionId,
    workspace_id: String,
    target_thread_id: String,
    root_thread_id: String,
    policy_revision: u64,
    role_key: String,
    policy_fingerprint: String,
    runtime_principal_policy: RuntimePrincipalPolicy,
    permission_profile_cap: TurnPermissionProfileCap,
    human_interaction_budget: HumanInteractionBudget,
    mcp_invocation_limits: pioneer_protocol::McpInvocationResourceLimits,
    native_event_budget: pioneer_cli_agent_runtime::NativeEventBudget,
    execution_class: pioneer_crud::ExecutionAdmissionClass,
    grant_manifest: Option<ExecutionGrantManifest>,
    runtime_draft: Option<RuntimeDraftMaterialization>,
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeDraftMaterialization {
    access: RuntimeDraftAccess,
    creator: RuntimeDraftCreator,
}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeDraftCreator {
    ScopedPrincipal {
        gateway_id: GatewayId,
        principal_id: PrincipalId,
        access_class: PersistedThreadAccessClass,
    },
    Absolute {
        access_class: PersistedThreadAccessClass,
    },
}

impl RuntimeDraftMaterialization {
    pub(crate) fn from_authorized_runtime_draft(
        request: &RequestContext,
        access: RuntimeDraftAccess,
    ) -> Result<Self> {
        let owner = access.owner();
        if owner.connection_id != request.connection_id()
            || owner.identity.principal_id != request.principal().principal_id
            || owner.identity.session_id != request.principal().session_id
        {
            bail!("runtime draft authorization owner does not match request");
        }
        let runtime_policy = AuthorizationService::new()
            .runtime_principal_policy(
                request.principal().kind,
                request.principal().role_key.as_ref(),
            )
            .context("runtime draft principal has an unsupported role")?;
        let creator = match runtime_policy {
            RuntimePrincipalPolicy::ScopedCollaboration => RuntimeDraftCreator::ScopedPrincipal {
                gateway_id: request.principal().gateway_id.clone(),
                principal_id: request.principal().principal_id.clone(),
                access_class: if access.visibility() == Some(ThreadVisibility::Workspace) {
                    PersistedThreadAccessClass::Workspace
                } else {
                    PersistedThreadAccessClass::Private
                },
            },
            RuntimePrincipalPolicy::Absolute => RuntimeDraftCreator::Absolute {
                access_class: if access.visibility() == Some(ThreadVisibility::Workspace) {
                    PersistedThreadAccessClass::Workspace
                } else {
                    PersistedThreadAccessClass::Private
                },
            },
        };
        Ok(Self { access, creator })
    }

    pub(crate) fn access(&self) -> &RuntimeDraftAccess {
        &self.access
    }

    pub(crate) fn creator(&self) -> &RuntimeDraftCreator {
        &self.creator
    }
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
        if authorization.action() != ResourceAction::AgentTurnStart {
            bail!("execution authorization action does not match request");
        }
        Self::from_revalidated_thread(
            request,
            authorization.workspace_id(),
            authorization.thread_id(),
            authorization.collaboration_root_thread_id(),
            policy_revision,
        )
    }

    pub(crate) fn from_authorized_runtime_draft(
        request: &RequestContext,
        access: RuntimeDraftAccess,
        policy_revision: u64,
    ) -> Result<Self> {
        let materialization =
            RuntimeDraftMaterialization::from_authorized_runtime_draft(request, access)?;
        let mut admission = Self::from_revalidated_thread(
            request,
            materialization.access.workspace_id(),
            materialization.access.thread_id(),
            materialization.access.thread_id(),
            policy_revision,
        )?;
        admission.runtime_draft = Some(materialization);
        Ok(admission)
    }

    fn from_revalidated_thread(
        request: &RequestContext,
        workspace_id: &str,
        target_thread_id: &str,
        root_thread_id: &str,
        policy_revision: u64,
    ) -> Result<Self> {
        let workspace_id = workspace_id.trim();
        let target_thread_id = target_thread_id.trim();
        let root_thread_id = root_thread_id.trim();
        if workspace_id.is_empty() || target_thread_id.is_empty() || root_thread_id.is_empty() {
            bail!("execution authorization requires exact workspace, target, and root thread");
        }
        let policy = super::AuthorizationService::new();
        let permission_profile_cap = policy
            .turn_permission_profile_cap(
                request.principal().kind,
                request.principal().role_key.as_ref(),
            )
            .ok_or_else(|| anyhow!("execution authorization requires a supported role"))?;
        let human_interaction_budget = policy
            .human_interaction_budget(
                request.principal().kind,
                request.principal().role_key.as_ref(),
            )
            .ok_or_else(|| anyhow!("execution authorization requires an interaction budget"))?;
        let mcp_invocation_limits = policy
            .mcp_invocation_resource_limits(
                request.principal().kind,
                request.principal().role_key.as_ref(),
            )
            .ok_or_else(|| anyhow!("execution authorization requires MCP invocation limits"))?;
        let native_event_budget = policy
            .native_event_resource_budget(
                request.principal().kind,
                request.principal().role_key.as_ref(),
            )
            .ok_or_else(|| anyhow!("execution authorization requires native event limits"))?;
        let role_key = policy
            .resolved_role_key(
                request.principal().kind,
                request.principal().role_key.as_ref(),
            )
            .ok_or_else(|| anyhow!("execution authorization requires a supported role"))?
            .to_owned();
        let runtime_principal_policy = policy
            .runtime_principal_policy(
                request.principal().kind,
                request.principal().role_key.as_ref(),
            )
            .ok_or_else(|| anyhow!("execution authorization requires a runtime policy"))?;
        Ok(Self {
            principal: request.principal().clone(),
            initiating_principal_id: request.principal().principal_id.clone(),
            initiating_session_id: request.principal().session_id.clone(),
            workspace_id: workspace_id.to_owned(),
            target_thread_id: target_thread_id.to_owned(),
            root_thread_id: root_thread_id.to_owned(),
            policy_revision,
            role_key,
            policy_fingerprint: crate::authorization::RoleDefinitionRegistry::new()
                .policy_fingerprint(),
            runtime_principal_policy,
            permission_profile_cap,
            human_interaction_budget,
            mcp_invocation_limits,
            native_event_budget,
            execution_class: pioneer_crud::ExecutionAdmissionClass::InteractiveTurn,
            grant_manifest: None,
            runtime_draft: None,
        })
    }

    pub(crate) const fn uses_scoped_collaboration_policy(&self) -> bool {
        matches!(
            self.runtime_principal_policy,
            RuntimePrincipalPolicy::ScopedCollaboration
        )
    }

    pub(crate) fn workspace_id(&self) -> &str {
        self.workspace_id.as_str()
    }

    pub(crate) fn root_thread_id(&self) -> &str {
        self.root_thread_id.as_str()
    }

    pub(crate) fn target_thread_id(&self) -> &str {
        self.target_thread_id.as_str()
    }

    pub(crate) fn runtime_draft(&self) -> Option<&RuntimeDraftMaterialization> {
        self.runtime_draft.as_ref()
    }

    pub(crate) const fn native_event_resource_budget(
        &self,
    ) -> pioneer_cli_agent_runtime::NativeEventBudget {
        self.native_event_budget
    }

    pub(crate) fn policy_provenance(&self) -> (&str, u64, &str) {
        (
            self.role_key.as_str(),
            self.policy_revision,
            self.policy_fingerprint.as_str(),
        )
    }

    pub(crate) fn execution_quota_lease(
        &self,
        subject_kind: &str,
        subject_id: &str,
    ) -> Result<pioneer_crud::NewExecutionAdmissionLease> {
        let policy = AuthorizationService::new()
            .execution_resource_policy(self.principal.kind, self.principal.role_key.as_ref())
            .context("execution authorization role has no resource policy")?;
        Ok(super::ExecutionAdmissionGovernor::lease(
            self.initiating_principal_id.as_str(),
            self.role_key.as_str(),
            self.workspace_id.as_str(),
            self.policy_fingerprint.as_str(),
            policy,
            self.execution_class,
            subject_kind,
            subject_id,
        ))
    }

    pub(crate) async fn authorize_composite(
        &mut self,
        store: &CrudStore,
        request: &ExecutionAdmissionRequest,
    ) -> Result<()> {
        if request.workspace_id != self.workspace_id
            || request.root_thread_id != self.root_thread_id
        {
            bail!("composite execution intent differs from the authorized root");
        }
        self.execution_class = if matches!(
            request.execution_backend,
            Some(AgentExecutionBackend::CLIAgentRuntime { .. })
                | Some(AgentExecutionBackend::ACPAgentRuntime { .. })
        ) {
            pioneer_crud::ExecutionAdmissionClass::CliProcess
        } else {
            pioneer_crud::ExecutionAdmissionClass::InteractiveTurn
        };
        let runtime_draft = self.runtime_draft.as_ref().map(|draft| draft.access());
        let manifest = ExecutionAdmissionService::new(store.clone())
            .admit_with_runtime_draft(
                &self.principal,
                self.policy_revision,
                request,
                runtime_draft,
            )
            .await?;
        self.grant_manifest = Some(manifest);
        Ok(())
    }

    pub(crate) async fn validate_durable_start(
        &self,
        store: &CrudStore,
        provider_registry: &pioneer_provider::ProviderRegistry,
    ) -> Result<()> {
        if self.grant_manifest.is_none() {
            bail!("execution has no composite admission manifest");
        }
        let current = pioneer_crud::current_policy_generation(&store.database_connection())
            .await
            .context("failed to load current policy generation before durable start")?;
        if current.get() != self.policy_revision {
            bail!(
                "execution admission generation {} is stale; current generation is {}",
                self.policy_revision,
                current.get()
            );
        }
        if let Some(provider) = self
            .grant_manifest
            .as_ref()
            .and_then(|manifest| manifest.provider.as_ref())
        {
            let current = provider_registry.authority_fingerprint_for_workspace(
                self.workspace_id.as_str(),
                provider.provider.as_str(),
            );
            if current.as_str() != provider.authority_fingerprint {
                bail!("provider authority changed before durable execution start");
            }
        }
        Ok(())
    }

    /// Validate consistency between an explicit API backend and the provider
    /// selected for this launch. Provider/model selection is an ordinary
    /// thread operation; the configured provider/runtime is authorized and
    /// resolved separately at the execution boundary.
    pub(crate) fn validate_provider_request(
        &self,
        persisted_provider: &str,
        persisted_model: &str,
        requested_provider: Option<&str>,
        requested_model: Option<&str>,
        execution_backend: Option<&AgentExecutionBackend>,
    ) -> Result<()> {
        let effective_provider = requested_provider
            .map(str::trim)
            .filter(|provider| !provider.is_empty())
            .unwrap_or_else(|| persisted_provider.trim());
        if let Some(AgentExecutionBackend::ApiProvider { provider }) = execution_backend
            && provider.trim() != effective_provider
        {
            bail!("execution backend does not match the selected provider");
        }
        let effective_model = requested_model
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .unwrap_or_else(|| persisted_model.trim());
        let policy = AuthorizationService::new();
        match execution_backend {
            Some(AgentExecutionBackend::CLIAgentRuntime { runtime_id, .. })
            | Some(AgentExecutionBackend::ACPAgentRuntime { runtime_id }) => {
                if !policy.cli_model_allowed(
                    self.principal.kind,
                    self.principal.role_key.as_ref(),
                    runtime_id.trim(),
                    effective_model,
                ) {
                    bail!("selected CLI runtime/model is outside the role projection");
                }
            }
            _ => {
                if !policy.provider_model_allowed(
                    self.principal.kind,
                    self.principal.role_key.as_ref(),
                    effective_provider,
                    effective_model,
                ) {
                    bail!("selected provider/model is outside the role projection");
                }
            }
        }
        Ok(())
    }

    pub(crate) fn cap_permission_profile(
        &self,
        requested: &TurnPermissionProfileSnapshot,
    ) -> TurnPermissionProfileSnapshot {
        let cap = pioneer_protocol::task_permission_cap_snapshot(&self.permission_profile_cap);
        pioneer_protocol::intersect_turn_permission_profiles(
            requested,
            &cap,
            pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
        )
    }

    pub(crate) fn finalize(
        &self,
        workspace_id: &str,
        target_thread_id: &str,
        provider: &str,
        model: &str,
        execution_backend: Option<&AgentExecutionBackend>,
        capabilities: &[TurnCapability],
        effective_permission_profile: &TurnPermissionProfileSnapshot,
    ) -> Result<ExecutionAuthorizationContext> {
        if workspace_id != self.workspace_id || target_thread_id != self.target_thread_id {
            bail!("materialized execution scope differs from authorized target");
        }
        let permission_profile_cap = pioneer_protocol::task_permission_cap_from_snapshot(
            &self.cap_permission_profile(effective_permission_profile),
        );
        let capability_projection_fingerprint = capability_projection_fingerprint(
            workspace_id,
            self.root_thread_id.as_str(),
            provider,
            model,
            execution_backend,
            capabilities,
            &permission_profile_cap,
        )?;
        let grant_manifest = match self.grant_manifest.clone() {
            Some(manifest) => manifest,
            #[cfg(test)]
            None => test_execution_grant_manifest(execution_backend, provider, model),
            #[cfg(not(test))]
            None => bail!("execution has no composite admission manifest"),
        };
        Ok(ExecutionAuthorizationContext {
            version: EXECUTION_AUTHORIZATION_CONTEXT_VERSION,
            authority: ExecutionAuthorityEnvelope::PrincipalGrant {
                principal_id: self.initiating_principal_id.clone(),
                session_id: self.initiating_session_id.clone(),
                principal_kind: self.principal.kind,
                role_key: self.role_key.clone(),
            },
            initiating_principal_id: self.initiating_principal_id.clone(),
            initiating_session_id: self.initiating_session_id.clone(),
            workspace_id: self.workspace_id.clone(),
            root_thread_id: self.root_thread_id.clone(),
            policy_revision: self.policy_revision,
            role_key: self.role_key.clone(),
            policy_fingerprint: self.policy_fingerprint.clone(),
            capability_projection_fingerprint,
            permission_profile_cap,
            human_interaction_budget: self.human_interaction_budget,
            mcp_invocation_limits: self.mcp_invocation_limits,
            native_event_budget: self.native_event_budget,
            continuity_policy: ExecutionContinuityPolicy::StopOnAuthorityLoss,
            resource_boundary: ExecutionResourceBoundary::RootThreadCapsule,
            grant_manifest,
            mcp_projection: None,
            skill_projection: None,
            cli_runtime_projection: cli_runtime_projection(execution_backend),
        })
    }
}

fn approval_scope_policy_for_mode(
    mode: pioneer_protocol::TurnPermissionMode,
) -> pioneer_protocol::TurnApprovalScopePolicySnapshot {
    match mode {
        pioneer_protocol::TurnPermissionMode::FullAccess => {
            pioneer_protocol::TurnApprovalScopePolicySnapshot::full_access()
        }
        pioneer_protocol::TurnPermissionMode::AutoAcceptEdits => {
            pioneer_protocol::TurnApprovalScopePolicySnapshot::auto_accept_edits()
        }
        pioneer_protocol::TurnPermissionMode::Supervised => {
            pioneer_protocol::TurnApprovalScopePolicySnapshot::supervised()
        }
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
    use std::sync::Arc;

    fn member_request() -> RequestContext {
        scoped_request(RoleKey::member())
    }

    fn scoped_request(role_key: RoleKey) -> RequestContext {
        let principal = Arc::new(AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new("G".repeat(21)).expect("gateway id"),
            principal_id: PrincipalId::new("P".repeat(21)).expect("principal id"),
            kind: PrincipalKind::User,
            role_key: Some(role_key),
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
        let role = request
            .principal()
            .role_key
            .clone()
            .expect("scoped test principal role");
        super::super::resolver::authorized_thread_for_test(
            request.principal().principal_id.clone(),
            ResourceAction::AgentTurnStart,
            AuthorizationResource::Thread {
                workspace_id: WorkspaceResourceId::new("workspace-a").expect("workspace id"),
                thread_id: ThreadResourceId::new("thread-a").expect("thread id"),
            },
            AuthorizationDecision::AllowPolicy {
                role,
                reason: super::super::AllowReason::PrivateThreadParticipant,
            },
        )
    }

    #[test]
    fn synthetic_role_flows_through_execution_without_runtime_role_variants() {
        let request = scoped_request(RoleKey::new("synthetic_executor").unwrap());
        let proof = authorized_thread(&request);
        let admission =
            ExecutionAuthorizationAdmission::from_authorized_thread(&request, &proof, 17)
                .expect("synthetic registered role admission");
        assert!(admission.uses_scoped_collaboration_policy());
        assert_eq!(admission.policy_provenance().0, "synthetic_executor");
    }

    #[test]
    fn mcp_resource_policy_is_role_owned_persisted_and_cannot_widen_on_continuation() {
        let request = scoped_request(RoleKey::new("synthetic_executor").unwrap());
        let proof = authorized_thread(&request);
        let admission =
            ExecutionAuthorizationAdmission::from_authorized_thread(&request, &proof, 19)
                .expect("synthetic registered role admission");
        let effective = admission
            .cap_permission_profile(&pioneer_protocol::default_turn_permission_profile_snapshot());
        let context = admission
            .finalize(
                "workspace-a",
                "thread-a",
                "allowed-provider",
                "allowed-model",
                None,
                &[],
                &effective,
            )
            .expect("synthetic execution context");
        let limits = context.mcp_invocation_resource_limits();
        assert_eq!(limits.max_arguments_bytes, 4 * 1024);
        assert_eq!(limits.max_concurrent_calls, 1);
        assert_eq!(
            context
                .effective_mcp_invocation_resource_limits()
                .expect("current role ceiling"),
            limits
        );

        let mut widened_persisted_ceiling = context.clone();
        widened_persisted_ceiling.mcp_invocation_limits = Default::default();
        assert_eq!(
            widened_persisted_ceiling
                .effective_mcp_invocation_resource_limits()
                .expect("current role must clamp a wider persisted ceiling"),
            limits
        );

        let mut narrower_persisted_ceiling = context.clone();
        narrower_persisted_ceiling
            .mcp_invocation_limits
            .max_arguments_bytes = 1024;
        assert_eq!(
            narrower_persisted_ceiling
                .effective_mcp_invocation_resource_limits()
                .expect("admission ceiling must remain immutable")
                .max_arguments_bytes,
            1024
        );

        let restored = ExecutionAuthorizationContext::from_persisted_json(
            context
                .to_persisted_json()
                .expect("serialize execution context")
                .as_str(),
        )
        .expect("restore execution context");
        assert_eq!(restored.mcp_invocation_resource_limits(), limits);

        let continuation = restored
            .derive_continuation(
                "allowed-provider",
                "allowed-model",
                None,
                &[],
                &effective,
                Some("a".repeat(64).as_str()),
            )
            .expect("derive exact continuation");
        assert_eq!(continuation.mcp_invocation_resource_limits(), limits);
        let native_limits = context.native_event_resource_budget();
        assert_eq!(native_limits.max_frame_bytes, 16 * 1024);
        assert_eq!(
            context
                .effective_native_event_resource_budget()
                .expect("current native event role ceiling"),
            native_limits
        );
        let mut widened_native_ceiling = context.clone();
        widened_native_ceiling.native_event_budget =
            pioneer_cli_agent_runtime::NativeEventBudget::default();
        assert_eq!(
            widened_native_ceiling
                .effective_native_event_resource_budget()
                .expect("current role clamps native event ceiling"),
            native_limits
        );
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
            .bind_mcp_projection("workspace-a", 1, manifest_hash.as_str(), &[])
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
                .bind_mcp_projection("workspace-a", 2, manifest_hash.as_str(), &[])
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
                    None,
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
                None,
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
    fn every_durable_child_turn_gets_an_exact_active_quota_admission() {
        let request = member_request();
        let proof = authorized_thread(&request);
        let admission =
            ExecutionAuthorizationAdmission::from_authorized_thread(&request, &proof, 7)
                .expect("authorized admission");
        let effective = admission
            .cap_permission_profile(&pioneer_protocol::default_turn_permission_profile_snapshot());
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
            .expect("execution context");
        let current_policy_fingerprint =
            crate::authorization::RoleDefinitionRegistry::new().policy_fingerprint();
        let revalidated = RevalidatedExecutionAuthorization {
            principal: request.principal().clone(),
            authorization: authorized_thread(&request),
            resource_boundary: context.resource_boundary,
            effective_permission_profile: effective.clone(),
            admitted_workspace_id: context.workspace_id.clone(),
            admitted_root_thread_id: context.root_thread_id.clone(),
            admitted_principal_id: context.initiating_principal_id.clone(),
            admitted_session_id: context.initiating_session_id.clone(),
            admitted_role_key: context.role_key.clone(),
            admitted_policy_generation: context.policy_revision,
            admitted_policy_fingerprint: context.policy_fingerprint.clone(),
            validated_policy_generation: 60,
            validated_policy_fingerprint: current_policy_fingerprint,
        };

        let child = context
            .durable_turn_admission_after_revalidation(
                "thread-child",
                "turn-child",
                None,
                &revalidated,
            )
            .expect("child admission");
        assert_eq!(child.policy_generation, Some(60));
        assert_eq!(child.role_key.as_deref(), Some("member"));
        assert_eq!(child.request_digest.len(), 64);
        let child_lease = child.execution_lease.expect("child quota lease");
        assert_eq!(
            child_lease.operation_class,
            pioneer_crud::ExecutionAdmissionClass::AttachedChild
        );
        assert_eq!(child_lease.subject_kind, "turn");
        assert_eq!(child_lease.subject_id, "turn-child");

        let root = context
            .durable_turn_admission_after_revalidation("thread-a", "turn-root", None, &revalidated)
            .expect("root continuation admission");
        assert_eq!(
            root.execution_lease
                .expect("root quota lease")
                .operation_class,
            pioneer_crud::ExecutionAdmissionClass::InteractiveTurn
        );

        let cli_backend = AgentExecutionBackend::CLIAgentRuntime {
            runtime_id: "codex-work".to_owned(),
            runtime_kind: CLIAgentRuntimeKind::Codex,
        };
        let cli_child = context
            .durable_turn_admission_after_revalidation(
                "thread-cli-child",
                "turn-cli-child",
                Some(&cli_backend),
                &revalidated,
            )
            .expect("CLI child admission");
        assert_eq!(
            cli_child
                .execution_lease
                .expect("CLI quota lease")
                .operation_class,
            pioneer_crud::ExecutionAdmissionClass::CliProcess
        );

        let mut unrelated = context.clone();
        unrelated.root_thread_id = "thread-unrelated".to_owned();
        assert!(
            unrelated
                .durable_turn_admission_after_revalidation(
                    "thread-child",
                    "turn-unrelated",
                    None,
                    &revalidated,
                )
                .is_err(),
            "a current proof must not authorize a different durable execution context"
        );
    }

    #[test]
    fn member_can_select_provider_but_cannot_widen_permission_profile_or_root() {
        let request = member_request();
        let proof = authorized_thread(&request);
        let admission =
            ExecutionAuthorizationAdmission::from_authorized_thread(&request, &proof, 0)
                .expect("authorized admission");
        assert!(
            admission
                .validate_provider_request(
                    "openai",
                    "model-a",
                    Some("other"),
                    Some("model-b"),
                    None
                )
                .is_ok()
        );
        assert!(
            admission
                .validate_provider_request(
                    "openai",
                    "model-a",
                    Some("other"),
                    Some("model-b"),
                    Some(&AgentExecutionBackend::ApiProvider {
                        provider: "openai".to_owned(),
                    }),
                )
                .is_err()
        );
        let supervised = pioneer_protocol::compile_turn_permission_profile(
            TurnPermissionMode::Supervised,
            pioneer_protocol::TurnPermissionProfileSource::Composer,
        );
        let effective = admission.cap_permission_profile(&supervised);
        assert_eq!(effective.mode, TurnPermissionMode::Supervised);
        assert_eq!(effective.effective_policy, supervised.effective_policy);
        assert_eq!(
            effective.source,
            pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap
        );
        let full_access = pioneer_protocol::compile_turn_permission_profile(
            TurnPermissionMode::FullAccess,
            pioneer_protocol::TurnPermissionProfileSource::Composer,
        );
        assert_eq!(
            admission.cap_permission_profile(&full_access).mode,
            TurnPermissionMode::Supervised
        );
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
}
