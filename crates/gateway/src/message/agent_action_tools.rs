//! Runtime bridge for agent domain's typed, execution-bound agent tools.
//!
//! Every successful mutation passes through its aggregate's canonical writer
//! and commits the matching action/receipt/outbox in that same transaction.

use super::MessageProcessor;
use crate::authorization::{AgentActionKindName, AgentToolAdapterError, BoundAgentActionAdapter};
use anyhow::Context as _;
use async_trait::async_trait;
use pioneer_agent::TurnToolContext;
use pioneer_protocol::{
    AgentActionIntent, AgentExecutionId, AgentModelToolName, AgentPublicOutcome,
    AgentStartOptionsToolInput, AgentStartTarget, AgentToolCapability, AgentToolOptionsProjection,
    AgentToolResultStatus, AgentToolSafeResult, PersistedActorRef, ThreadMode, TurnStartParams,
    project_agent_model_tool_catalog,
};
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind, ToolError, ToolEventTrace,
    ToolExtensionBundle, ToolHandler, ToolIdempotencyMode, ToolInvocation, ToolOutput, ToolPayload,
    ToolRecoveryMetadata, ToolRetryClass, ToolSpec, dynamic_unknown_output_policy,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub(crate) struct CurrentAgentIdentitySourceFence {
    revision: i64,
    fingerprint: String,
}

fn cli_identity_catalog(
    processor: &MessageProcessor,
    configured: &[pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig],
) -> anyhow::Result<Vec<pioneer_crud::CliRuntimeIdentitySeed>> {
    let identity_settings =
        crate::cli_runtime::config::load_effective_cli_runtime_identity_settings(
            processor.artifact_runtime_home.as_path(),
        )?;
    crate::identity::catalog::from_effective_settings(configured.to_vec(), &identity_settings)
}

pub(crate) async fn current_agent_identity_source_fence(
    processor: &MessageProcessor,
    execution_id: &str,
) -> anyhow::Result<CurrentAgentIdentitySourceFence> {
    let database = processor.crud_store.database_connection();
    let execution = pioneer_crud::load_agent_execution(&database, execution_id)
        .await?
        .context("AgentExecution is unavailable for source revalidation")?;
    let identity =
        pioneer_crud::load_agent_identity(&database, execution.agent_identity_id.as_str())
            .await?
            .context("Agent identity is unavailable for source revalidation")?;
    if identity.workspace_id != execution.workspace_id
        || identity.status != "active"
        || identity.retired_at.is_some()
    {
        anyhow::bail!("Agent identity source is retired or unavailable");
    }
    match identity.source_kind.as_str() {
        pioneer_crud::SOURCE_NATIVE_AGENT => {
            let config =
                pioneer_crud::load_native_agent_config(&database, identity.source_id.as_str())
                    .await?
                    .context("native Agent source config is unavailable")?;
            if config.workspace_id != execution.workspace_id
                || !config.enabled
                || config.config_revision != identity.source_revision
            {
                anyhow::bail!("native Agent source is disabled or unavailable");
            }
        }
        pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE => {
            let configured = processor.load_cli_runtime_instances()?;
            let identity_settings =
                crate::cli_runtime::config::load_effective_cli_runtime_identity_settings(
                    processor.artifact_runtime_home.as_path(),
                )?;
            let catalog =
                crate::identity::catalog::from_effective_settings(configured, &identity_settings)?;
            let runtime = catalog
                .iter()
                .find(|runtime| runtime.id == identity.source_id && runtime.enabled)
                .context("CLI Agent source is disabled or unavailable")?;
            if pioneer_crud::cli_runtime_identity_fingerprint(runtime)
                != identity.source_fingerprint
            {
                anyhow::bail!("CLI Agent source projection is stale");
            }
        }
        pioneer_crud::SOURCE_EPHEMERAL => {}
        _ => anyhow::bail!("Agent identity source kind is unsupported"),
    }
    Ok(CurrentAgentIdentitySourceFence {
        revision: identity.source_revision,
        fingerprint: identity.source_fingerprint,
    })
}

pub(crate) fn apply_current_identity_source_fence(
    plan: &mut crate::authorization::AgentActionCommitPlan,
    fence: &CurrentAgentIdentitySourceFence,
) {
    plan.input.expected_current_identity_source_revision = fence.revision;
    plan.input.expected_current_identity_source_fingerprint = fence.fingerprint.clone();
}

/// The binding is installed by the server after durable child-turn
/// admission.  It is intentionally keyed by turn id rather than by a model
/// or provider session id: reconnecting a runtime must not change authorship.
#[derive(Clone)]
pub(crate) struct AgentActionRuntimeBinding {
    pub(crate) adapter: Arc<Mutex<BoundAgentActionAdapter>>,
    pub(crate) options: AgentToolOptionsProjection,
    pub(crate) capabilities: BTreeSet<AgentToolCapability>,
}

impl AgentActionRuntimeBinding {
    pub(crate) fn new(
        adapter: BoundAgentActionAdapter,
        options: AgentToolOptionsProjection,
        capabilities: BTreeSet<AgentToolCapability>,
    ) -> Self {
        Self {
            adapter: Arc::new(Mutex::new(adapter)),
            options,
            capabilities,
        }
    }

    pub(crate) async fn refresh_start_options_catalog(
        &mut self,
        processor: &MessageProcessor,
        turn_id: &str,
    ) -> anyhow::Result<()> {
        let database = processor.crud_store.database_connection();
        let facts = self.adapter.lock().await.persistence_facts();
        let execution = pioneer_crud::load_agent_execution(&database, facts.execution_id.as_str())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("AgentExecution is missing while projecting launch options")
            })?;
        let fallback_thread = processor
            .crud_store
            .get_thread_model(execution.home_root_thread_id.as_str())
            .await?;
        let default_provider = match &facts.profile.backend {
            pioneer_protocol::AgentExecutionProfileBackend::ApiProvider => {
                facts.profile.provider_id.clone()
            }
            _ => fallback_thread
                .as_ref()
                .map(|thread| thread.model_provider.clone())
                .ok_or_else(|| anyhow::anyhow!("launch catalog has no API provider fallback"))?,
        };
        let default_model = fallback_thread
            .as_ref()
            .map(|thread| thread.model.clone())
            .unwrap_or_else(|| facts.profile.model_id.clone());
        let (current_identities, current_profiles) = current_workspace_launch_catalog(
            processor,
            execution.workspace_id.as_str(),
            &facts.identity,
            &facts.profile,
            default_provider.as_str(),
            default_model.as_str(),
        )
        .await?;
        let ceiling =
            load_execution_child_launch_ceiling(&database, facts.execution_id.as_str()).await?;
        let (projected_identities, projected_profiles) =
            intersect_child_launch_ceiling(&ceiling, &current_identities, &current_profiles);
        let parent_identity_available = ceiling.allow_inherit_parent_identity
            && projected_identities
                .iter()
                .any(|identity| identity.id == facts.identity.id);
        let parent_profile_available = ceiling.allow_inherit_parent_profile
            && projected_profiles
                .iter()
                .any(|profile| profile.id == facts.profile.id);
        let authority = processor
            .load_turn_execution_authorization_context(turn_id)
            .await?;
        let graph_root = pioneer_crud::load_agent_execution(
            &database,
            execution.work_graph_root_execution_id.as_str(),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("launch catalog graph root is missing"))?;
        if authority.workspace_id() != execution.workspace_id
            || authority.root_thread_id() != graph_root.home_root_thread_id
        {
            anyhow::bail!("launch catalog authority differs from its AgentExecution");
        }
        let mut same_capsule_targets = Vec::new();
        for lineage in processor
            .crud_store
            .list_task_thread_lineage_by_root_thread_bounded(
                execution.home_root_thread_id.as_str(),
                pioneer_protocol::ChildAgentLaunchGrantSet::MAX_IDENTITIES as u64,
            )
            .await?
        {
            if lineage.root_thread_id != execution.home_root_thread_id {
                continue;
            }
            let Some(thread) = processor
                .crud_store
                .get_thread_model(lineage.child_thread_id.as_str())
                .await?
            else {
                continue;
            };
            if thread.workspace_id != execution.workspace_id {
                continue;
            }
            let label = thread
                .name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .unwrap_or("Internal collaboration thread")
                .to_owned();
            same_capsule_targets.push((thread.id, label));
        }
        let allowed_skill_ids = authority
            .granted_skill_ids()
            .iter()
            .map(|id| {
                pioneer_protocol::SkillId::new(id.clone()).map_err(|error| {
                    anyhow::anyhow!("authorization grant contains invalid Skill ID: {error:?}")
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .filter(|id| ceiling.skill_ids.contains(id))
            .collect::<Vec<_>>();
        let allowed_mcp_server_ids = authority
            .granted_mcp_server_capability_ids()
            .iter()
            .filter(|id| ceiling.mcp_server_ids.contains(id))
            .cloned()
            .collect::<Vec<_>>();
        let immutable_permission_cap =
            pioneer_protocol::task_permission_cap_snapshot(&ceiling.max_permission_profile);
        let current_permission_cap =
            pioneer_protocol::task_permission_cap_snapshot(authority.permission_profile_cap());
        let effective_permission_cap = pioneer_protocol::task_permission_cap_from_snapshot(
            &pioneer_protocol::intersect_turn_permission_profiles(
                &immutable_permission_cap,
                &current_permission_cap,
                pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
            ),
        );
        let route_policy_generation = processor.current_authorization_revision().await?.max(1);
        let gateway = pioneer_crud::load_gateway_singleton(&database)
            .await?
            .ok_or_else(|| anyhow::anyhow!("launch catalog Gateway identity is missing"))?;
        let now_millis = pioneer_crud::utc_now().timestamp_millis();
        let mut routes = Vec::new();
        for row in pioneer_crud::list_agent_delegation_routes_for_source(
            &database,
            execution.id.as_str(),
            facts.identity.id.as_str(),
        )
        .await?
        {
            let projection = pioneer_crud::agent_delegation_route_projection(&row)?;
            if !projection.status.is_live()
                || projection.validate(Some(now_millis)).is_err()
                || projection.source_workspace_id != execution.workspace_id
                || projection.destination_workspace_id != execution.workspace_id
                || projection.source_gateway_id != gateway.id.as_str()
                || projection.destination_gateway_id != gateway.id.as_str()
                || projection.source_capsule_id != execution.home_root_thread_id
                || projection.source_policy_generation != route_policy_generation
                || projection.destination_policy_generation != route_policy_generation
                || projection.source_agent_identity_id != facts.identity.id
            {
                continue;
            }
            let Some(destination) = processor
                .crud_store
                .get_thread_model(projection.destination_thread_id.as_str())
                .await?
            else {
                continue;
            };
            if destination.workspace_id != execution.workspace_id {
                continue;
            }
            routes.push(
                crate::authorization::AgentRouteFacts::from_projection(&projection)
                    .map_err(|message| anyhow::anyhow!(message))?,
            );
        }
        let mut adapter = self.adapter.lock().await;
        adapter
            .replace_same_capsule_targets(same_capsule_targets)
            .map_err(|error| anyhow::anyhow!("failed to bind same-capsule targets: {error:?}"))?;
        adapter
            .replace_persisted_routes(routes)
            .map_err(|error| anyhow::anyhow!("failed to bind launch routes: {error:?}"))?;
        self.options = adapter.install_start_options_catalog(
            projected_identities,
            projected_profiles,
            parent_identity_available,
            ceiling.allow_server_derived_ephemeral,
            parent_profile_available,
            allowed_skill_ids,
            allowed_mcp_server_ids,
            effective_permission_cap,
        );
        Ok(())
    }
}

async fn load_execution_child_launch_ceiling<C: sea_orm::ConnectionTrait>(
    db: &C,
    execution_id: &str,
) -> anyhow::Result<pioneer_protocol::ChildAgentLaunchGrantSet> {
    let grant = pioneer_crud::load_agent_execution_grant(db, execution_id)
        .await?
        .context("AgentExecution has no immutable launch grant")?;
    let grant: serde_json::Value = serde_json::from_str(grant.grant_json.as_str())
        .context("AgentExecution launch grant is invalid")?;
    let ceiling: pioneer_protocol::ChildAgentLaunchGrantSet = serde_json::from_value(
        grant
            .get("child_launch_grant")
            .cloned()
            .context("AgentExecution has no immutable child launch ceiling")?,
    )
    .context("AgentExecution child launch ceiling is invalid")?;
    ceiling.validate().map_err(|error| {
        anyhow::anyhow!("AgentExecution child launch ceiling failed validation: {error:?}")
    })?;
    Ok(ceiling)
}

fn intersect_child_launch_ceiling(
    ceiling: &pioneer_protocol::ChildAgentLaunchGrantSet,
    current_identities: &[pioneer_protocol::AgentIdentityProjection],
    current_profiles: &[pioneer_protocol::AgentExecutionProfileProjection],
) -> (
    Vec<pioneer_protocol::AgentIdentityProjection>,
    Vec<pioneer_protocol::AgentExecutionProfileProjection>,
) {
    let identities = ceiling
        .identities
        .iter()
        .filter(|granted| current_identities.iter().any(|current| current == *granted))
        .cloned()
        .collect::<Vec<_>>();
    let identity_ids = identities
        .iter()
        .map(|identity| identity.id.clone())
        .collect::<BTreeSet<_>>();
    let profiles = ceiling
        .profiles
        .iter()
        .filter(|granted| current_profiles.iter().any(|current| current == *granted))
        .filter(|profile| {
            profile
                .compatible_agent_identity_ids
                .iter()
                .any(|identity_id| identity_ids.contains(identity_id))
        })
        .cloned()
        .collect();
    (identities, profiles)
}

pub(crate) async fn current_workspace_launch_catalog(
    processor: &MessageProcessor,
    workspace_id: &str,
    parent_identity: &pioneer_protocol::AgentIdentityProjection,
    parent_profile: &pioneer_protocol::AgentExecutionProfileProjection,
    default_provider: &str,
    default_model: &str,
) -> anyhow::Result<(
    Vec<pioneer_protocol::AgentIdentityProjection>,
    Vec<pioneer_protocol::AgentExecutionProfileProjection>,
)> {
    let database = processor.crud_store.database_connection();
    let cli_runtimes = processor.load_cli_runtime_instances()?;
    let cli_identity_catalog = cli_identity_catalog(processor, cli_runtimes.as_slice())?;
    let identities = pioneer_crud::list_active_agent_identities(&database, workspace_id).await?;
    let mut projected_identities = Vec::new();
    let mut projected_profiles = Vec::new();

    for identity in identities {
        let source_kind = match identity.source_kind.as_str() {
            pioneer_crud::SOURCE_NATIVE_AGENT => {
                let Some(config) =
                    pioneer_crud::load_native_agent_config(&database, identity.source_id.as_str())
                        .await?
                else {
                    continue;
                };
                if !config.enabled
                    || config.workspace_id != workspace_id
                    || config.config_revision != identity.source_revision
                {
                    continue;
                }
                pioneer_protocol::AgentIdentitySourceKind::NativeAgent
            }
            pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE => {
                let Some(runtime) = cli_identity_catalog
                    .iter()
                    .find(|runtime| runtime.id == identity.source_id && runtime.enabled)
                else {
                    continue;
                };
                if pioneer_crud::cli_runtime_identity_fingerprint(runtime)
                    != identity.source_fingerprint
                {
                    continue;
                }
                pioneer_protocol::AgentIdentitySourceKind::CliRuntimeInstance
            }
            // Persisted ephemeral identities are execution-local history and
            // are never reusable catalog entries.
            pioneer_crud::SOURCE_EPHEMERAL => continue,
            _ => continue,
        };
        let Some(snapshot) = pioneer_crud::load_current_agent_presentation_snapshot(
            &database,
            identity.id.as_str(),
            identity.source_revision,
            identity.source_fingerprint.as_str(),
        )
        .await?
        else {
            continue;
        };
        let projection = pioneer_protocol::AgentIdentityProjection::new(
            pioneer_protocol::AgentIdentityId::new(identity.id.clone()).map_err(|error| {
                anyhow::anyhow!("workspace agent identity has invalid id: {error:?}")
            })?,
            source_kind,
            snapshot.display_name,
            snapshot.nickname,
            snapshot.avatar_revision,
            snapshot.role_label,
            u64::try_from(identity.source_revision)
                .map_err(|_| anyhow::anyhow!("workspace identity revision is invalid"))?,
            identity.source_fingerprint,
        )
        .map_err(|error| anyhow::anyhow!("workspace identity projection is invalid: {error:?}"))?;
        let profile = if projection.id == parent_identity.id {
            parent_profile.clone()
        } else {
            workspace_launch_profile(
                &projection,
                identity.source_id.as_str(),
                cli_runtimes.as_slice(),
                default_provider,
                default_model,
                parent_profile.allowed_reasoning.as_slice(),
                parent_profile.allowed_permission_profiles.as_slice(),
                parent_profile.catalog_generation.max(1),
                parent_profile.policy_generation.max(1),
            )?
        };
        projected_identities.push(projection);
        projected_profiles.push(profile);
    }

    if parent_identity.source_kind == pioneer_protocol::AgentIdentitySourceKind::Ephemeral {
        projected_identities.push(parent_identity.clone());
        projected_profiles.push(parent_profile.clone());
    }
    projected_identities.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
    projected_profiles.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
    if projected_identities.len() > pioneer_protocol::ChildAgentLaunchGrantSet::MAX_IDENTITIES {
        let mut bounded = projected_identities
            .iter()
            .filter(|identity| identity.id == parent_identity.id)
            .cloned()
            .collect::<Vec<_>>();
        let remaining = pioneer_protocol::ChildAgentLaunchGrantSet::MAX_IDENTITIES
            .saturating_sub(bounded.len());
        bounded.extend(
            projected_identities
                .into_iter()
                .filter(|identity| identity.id != parent_identity.id)
                .take(remaining),
        );
        bounded.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        projected_identities = bounded;
    }
    let visible_ids = projected_identities
        .iter()
        .map(|identity| identity.id.clone())
        .collect::<BTreeSet<_>>();
    projected_profiles.retain(|profile| {
        !profile.compatible_agent_identity_ids.is_empty()
            && profile
                .compatible_agent_identity_ids
                .iter()
                .all(|identity_id| visible_ids.contains(identity_id))
    });
    if projected_profiles.len() > pioneer_protocol::ChildAgentLaunchGrantSet::MAX_PROFILES {
        let mut bounded = projected_profiles
            .iter()
            .filter(|profile| profile.id == parent_profile.id)
            .cloned()
            .collect::<Vec<_>>();
        let remaining =
            pioneer_protocol::ChildAgentLaunchGrantSet::MAX_PROFILES.saturating_sub(bounded.len());
        bounded.extend(
            projected_profiles
                .into_iter()
                .filter(|profile| profile.id != parent_profile.id)
                .take(remaining),
        );
        bounded.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        projected_profiles = bounded;
    }
    Ok((projected_identities, projected_profiles))
}

pub(crate) async fn current_workspace_child_launch_ceiling(
    processor: &MessageProcessor,
    workspace_id: &str,
    parent_identity: &pioneer_protocol::AgentIdentityProjection,
    parent_profile: &pioneer_protocol::AgentExecutionProfileProjection,
    default_provider: &str,
    default_model: &str,
    allow_server_derived_ephemeral: bool,
    skill_ids: Vec<pioneer_protocol::SkillId>,
    mcp_server_ids: Vec<String>,
    max_permission_profile: pioneer_protocol::TurnPermissionProfileCap,
) -> anyhow::Result<pioneer_protocol::ChildAgentLaunchGrantSet> {
    let (identities, profiles) = current_workspace_launch_catalog(
        processor,
        workspace_id,
        parent_identity,
        parent_profile,
        default_provider,
        default_model,
    )
    .await?;
    pioneer_protocol::ChildAgentLaunchGrantSet::new(identities, profiles)
        .and_then(|grant| {
            grant.with_policy(
                true,
                allow_server_derived_ephemeral,
                true,
                skill_ids,
                mcp_server_ids,
                max_permission_profile,
            )
        })
        .map_err(|error| anyhow::anyhow!("workspace child launch ceiling is invalid: {error:?}"))
}

pub(crate) fn workspace_launch_profile(
    identity: &pioneer_protocol::AgentIdentityProjection,
    source_id: &str,
    cli_runtimes: &[pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig],
    default_provider: &str,
    default_model: &str,
    allowed_reasoning: &[pioneer_protocol::TurnReasoningSelection],
    allowed_permission_profiles: &[pioneer_protocol::TurnPermissionMode],
    catalog_generation: u64,
    policy_generation: u64,
) -> anyhow::Result<pioneer_protocol::AgentExecutionProfileProjection> {
    let (backend, provider_id, model_id) = match identity.source_kind {
        pioneer_protocol::AgentIdentitySourceKind::NativeAgent => (
            pioneer_protocol::AgentExecutionProfileBackend::ApiProvider,
            default_provider.to_owned(),
            default_model.to_owned(),
        ),
        pioneer_protocol::AgentIdentitySourceKind::CliRuntimeInstance => {
            let _runtime = cli_runtimes
                .iter()
                .find(|runtime| runtime.id == source_id && runtime.enabled)
                .ok_or_else(|| anyhow::anyhow!("CLI identity runtime is unavailable"))?;
            // The admitted Turn/Task model is the exact execution choice.
            // Runtime catalog order is mutable configuration and must never
            // replace that pinned model during projection or recovery.
            let model = default_model.to_owned();
            (
                pioneer_protocol::AgentExecutionProfileBackend::CliRuntime {
                    runtime_instance_id: source_id.to_owned(),
                },
                format!("cli_runtime:{source_id}"),
                model,
            )
        }
        pioneer_protocol::AgentIdentitySourceKind::Ephemeral => {
            anyhow::bail!("historical ephemeral identity cannot become a launch option")
        }
    };
    let fingerprint = hex::encode(Sha256::digest(
        format!(
            "pioneer:agent-runtime:workspace-launch-profile:v1\0{}\0{}\0{}\0{}\0{}",
            identity.id, source_id, provider_id, model_id, policy_generation
        )
        .as_bytes(),
    ));
    let profile_id =
        pioneer_protocol::AgentExecutionProfileId::new(format!("P{}", &fingerprint[..20]))
            .map_err(|error| {
                anyhow::anyhow!("derived workspace profile id is invalid: {error:?}")
            })?;
    Ok(pioneer_protocol::AgentExecutionProfileProjection {
        id: profile_id,
        compatible_agent_identity_ids: vec![identity.id.clone()],
        backend,
        provider_id: provider_id.clone(),
        model_id: model_id.clone(),
        provider_display_name: provider_id,
        model_display_name: model_id,
        allowed_reasoning: allowed_reasoning.to_vec(),
        allowed_permission_profiles: allowed_permission_profiles.to_vec(),
        catalog_generation,
        policy_generation,
        fingerprint,
    })
}

pub(crate) async fn resolve_workspace_task_launch(
    processor: &MessageProcessor,
    workspace_id: &str,
    default_provider: &str,
    default_model: &str,
    selection: Option<&pioneer_protocol::AgentLaunchSelection>,
    requested_backend: Option<&pioneer_protocol::AgentExecutionBackend>,
    ephemeral_seed: &str,
) -> anyhow::Result<(
    pioneer_protocol::AgentLaunchSelection,
    Option<(
        pioneer_protocol::AgentIdentityProjection,
        pioneer_protocol::AgentExecutionProfileProjection,
    )>,
)> {
    use pioneer_protocol::{AgentExecutionProfileSelection, AgentIdentitySelection};

    let requested_cli_runtime_id = match requested_backend {
        Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime { runtime_id, .. }) => {
            Some(runtime_id.as_str())
        }
        Some(pioneer_protocol::AgentExecutionBackend::ACPAgentRuntime { runtime_id }) => {
            anyhow::bail!("Task launch selected unsupported ACP runtime `{runtime_id}`")
        }
        Some(pioneer_protocol::AgentExecutionBackend::ApiProvider { provider }) => {
            if provider != default_provider {
                anyhow::bail!("Task API backend differs from its admitted provider")
            }
            None
        }
        None => None,
    };
    let requested_id = match selection.map(|selection| &selection.agent) {
        None => None,
        Some(AgentIdentitySelection::InheritParent) => {
            anyhow::bail!("public Task launch cannot inherit an AgentExecution identity")
        }
        Some(AgentIdentitySelection::DefaultPioneer) => None,
        Some(AgentIdentitySelection::Exact { agent_identity_id }) => {
            Some(agent_identity_id.as_str())
        }
        Some(AgentIdentitySelection::ServerDerivedEphemeral { .. }) => None,
    };
    let database = processor.crud_store.database_connection();
    let cli_runtimes = processor.load_cli_runtime_instances()?;
    let cli_identity_catalog = cli_identity_catalog(processor, cli_runtimes.as_slice())?;
    let identity_row = if let Some(runtime_id) = requested_cli_runtime_id {
        if matches!(
            selection.map(|selection| &selection.agent),
            Some(AgentIdentitySelection::DefaultPioneer)
        ) {
            anyhow::bail!("Pioneer identity cannot use a CLI runtime backend")
        }
        let identity = pioneer_crud::load_active_agent_identity_by_source(
            &database,
            workspace_id,
            pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE,
            runtime_id,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("workspace Task CLI identity is unavailable"))?;
        if requested_id.is_some_and(|requested_id| requested_id != identity.id) {
            anyhow::bail!("workspace Task identity differs from its requested CLI runtime")
        }
        identity
    } else if let Some(requested_id) = requested_id {
        pioneer_crud::load_active_agent_identity(&database, workspace_id, requested_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("workspace Task identity is stale or unavailable"))?
    } else {
        let config = pioneer_crud::load_native_agent_config_by_system_key(
            &database,
            workspace_id,
            "pioneer",
        )
        .await?
        .filter(|config| config.enabled)
        .ok_or_else(|| anyhow::anyhow!("workspace Pioneer identity is unavailable"))?;
        pioneer_crud::load_active_agent_identity_by_source(
            &database,
            workspace_id,
            pioneer_crud::SOURCE_NATIVE_AGENT,
            config.id.as_str(),
        )
        .await?
        .filter(|identity| identity.source_revision == config.config_revision)
        .ok_or_else(|| anyhow::anyhow!("workspace Pioneer identity is stale or unavailable"))?
    };
    let source_kind = match identity_row.source_kind.as_str() {
        pioneer_crud::SOURCE_NATIVE_AGENT if requested_cli_runtime_id.is_none() => {
            let config =
                pioneer_crud::load_native_agent_config(&database, identity_row.source_id.as_str())
                    .await?
                    .filter(|config| {
                        config.enabled
                            && config.workspace_id == workspace_id
                            && config.config_revision == identity_row.source_revision
                    })
                    .ok_or_else(|| anyhow::anyhow!("workspace Task identity source is stale"))?;
            if requested_id.is_none() && config.system_key.as_deref() != Some("pioneer") {
                anyhow::bail!("default workspace Task identity is not Pioneer");
            }
            pioneer_protocol::AgentIdentitySourceKind::NativeAgent
        }
        pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE
            if match requested_backend {
                None => true,
                Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime {
                    runtime_id,
                    ..
                }) => runtime_id == &identity_row.source_id,
                Some(
                    pioneer_protocol::AgentExecutionBackend::ApiProvider { .. }
                    | pioneer_protocol::AgentExecutionBackend::ACPAgentRuntime { .. },
                ) => false,
            } =>
        {
            let runtime = cli_identity_catalog
                .iter()
                .find(|runtime| runtime.id == identity_row.source_id && runtime.enabled)
                .filter(|runtime| {
                    pioneer_crud::cli_runtime_identity_fingerprint(runtime)
                        == identity_row.source_fingerprint
                })
                .ok_or_else(|| anyhow::anyhow!("workspace Task CLI identity source is stale"))?;
            let _ = runtime;
            pioneer_protocol::AgentIdentitySourceKind::CliRuntimeInstance
        }
        _ => anyhow::bail!("workspace Task identity source is unsupported"),
    };
    let snapshot = pioneer_crud::load_current_agent_presentation_snapshot(
        &database,
        identity_row.id.as_str(),
        identity_row.source_revision,
        identity_row.source_fingerprint.as_str(),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("workspace Task identity presentation is unavailable"))?;
    let identity = pioneer_protocol::AgentIdentityProjection::new(
        pioneer_protocol::AgentIdentityId::new(identity_row.id.clone())
            .map_err(|error| anyhow::anyhow!("workspace Task identity id is invalid: {error:?}"))?,
        source_kind,
        snapshot.display_name,
        snapshot.nickname,
        snapshot.avatar_revision,
        snapshot.role_label,
        u64::try_from(identity_row.source_revision)
            .map_err(|_| anyhow::anyhow!("workspace Task identity revision is invalid"))?,
        identity_row.source_fingerprint,
    )
    .map_err(|error| anyhow::anyhow!("workspace Task identity is invalid: {error:?}"))?;
    let source_id = identity_row.source_id;
    let policy_generation = processor.current_authorization_revision().await?.max(1);
    let allowed_reasoning = selection
        .and_then(|selection| selection.execution.reasoning.clone())
        .into_iter()
        .collect::<Vec<_>>();
    let profile = workspace_launch_profile(
        &identity,
        source_id.as_str(),
        cli_runtimes.as_slice(),
        default_provider,
        default_model,
        allowed_reasoning.as_slice(),
        &[
            pioneer_protocol::TurnPermissionMode::FullAccess,
            pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
            pioneer_protocol::TurnPermissionMode::Supervised,
        ],
        policy_generation,
        policy_generation,
    )?;
    if let Some(
        selection @ pioneer_protocol::AgentLaunchSelection {
            agent: AgentIdentitySelection::ServerDerivedEphemeral { .. },
            ..
        },
    ) = selection
    {
        let ephemeral_options = pioneer_protocol::AgentStartOptionsProjection {
            agents: vec![identity],
            inherit_parent_agent_available: false,
            derived_ephemeral_available: true,
            profiles: vec![profile],
            inherit_parent_profile_available: false,
            allowed_skill_ids: Vec::new(),
            allowed_mcp_server_ids: Vec::new(),
            max_permission_profile: pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
            ),
            generation_fingerprint: hex::encode(Sha256::digest(
                format!("agent-task-ephemeral-options\0{ephemeral_seed}").as_bytes(),
            )),
        };
        let resolved = crate::authorization::resolve_ephemeral_agent_launch_selection(
            selection,
            ephemeral_seed,
            &ephemeral_options,
            None,
        )
        .map_err(|error| anyhow::anyhow!("ephemeral Task launch is invalid: {error:?}"))?;
        return Ok((selection.clone(), Some(resolved)));
    }
    match selection.map(|selection| &selection.execution.profile) {
        None => {}
        Some(AgentExecutionProfileSelection::Exact { profile_id }) if profile_id == &profile.id => {
        }
        Some(AgentExecutionProfileSelection::Exact { .. }) => {
            anyhow::bail!("workspace Task execution profile is stale or incompatible")
        }
        Some(AgentExecutionProfileSelection::InheritParent) => {
            anyhow::bail!("public Task launch requires an exact execution profile")
        }
    }
    let canonical = selection
        .cloned()
        .unwrap_or_else(|| pioneer_protocol::AgentLaunchSelection {
            agent: if identity.source_kind == pioneer_protocol::AgentIdentitySourceKind::NativeAgent
            {
                AgentIdentitySelection::DefaultPioneer
            } else {
                AgentIdentitySelection::Exact {
                    agent_identity_id: identity.id.clone(),
                }
            },
            execution: pioneer_protocol::AgentExecutionSelection {
                profile: AgentExecutionProfileSelection::Exact {
                    profile_id: profile.id.clone(),
                },
                reasoning: None,
                permission_profile: None,
                skill_ids: Vec::new(),
                mcp_server_ids: Vec::new(),
            },
        });
    Ok((canonical, Some((identity, profile))))
}

/// Compile the capability portion of the common agent domain launch contract
/// into the canonical Turn representation. The selection carries stable
/// Skill IDs and canonical MCP server capability IDs; labels and runtime
/// metadata remain server-owned.
pub(crate) fn launch_selection_capabilities(
    selection: &pioneer_protocol::AgentExecutionSelection,
) -> anyhow::Result<Vec<pioneer_protocol::TurnCapability>> {
    use pioneer_protocol::{McpScopeKind, TurnCapability, TurnCapabilityKind};

    let mut capabilities = Vec::with_capacity(
        selection
            .skill_ids
            .len()
            .saturating_add(selection.mcp_server_ids.len()),
    );
    for skill_id in &selection.skill_ids {
        capabilities.push(TurnCapability {
            id: pioneer_protocol::skill_capability_key(skill_id),
            kind: TurnCapabilityKind::Skill {
                skill_id: skill_id.clone(),
                pack_id: None,
            },
            label: None,
        });
    }
    for capability_id in &selection.mcp_server_ids {
        let rest = capability_id
            .strip_prefix("mcp-server:")
            .ok_or_else(|| anyhow::anyhow!("invalid MCP server capability id"))?;
        let (scope, name) = rest
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid MCP server capability id"))?;
        let scope_kind = match scope {
            "workspace" => McpScopeKind::Workspace,
            "user" => McpScopeKind::User,
            _ => anyhow::bail!("invalid MCP server capability scope"),
        };
        if name.trim().is_empty()
            || pioneer_protocol::mcp_server_capability_key(scope_kind, name) != *capability_id
        {
            anyhow::bail!("invalid MCP server capability id");
        }
        capabilities.push(TurnCapability {
            id: capability_id.clone(),
            kind: TurnCapabilityKind::McpServer {
                name: name.to_owned(),
                scope_kind,
            },
            label: None,
        });
    }
    Ok(capabilities)
}

pub(crate) fn root_agent_execution_id_for_turn(turn_id: &str) -> String {
    pioneer_crud::canonical_agent_id('E', &format!("root-agent-turn\0{turn_id}"))
}

async fn restore_persisted_agent_action_binding(
    processor: &Arc<MessageProcessor>,
    context: &TurnToolContext,
    execution_id: &AgentExecutionId,
) -> anyhow::Result<()> {
    let database = processor.crud_store.database_connection();
    let execution = pioneer_crud::load_agent_execution(&database, execution_id.as_str())
        .await?
        .ok_or_else(|| anyhow::anyhow!("persisted AgentExecution is missing"))?;
    if execution.workspace_id != context.workspace_id
        || execution.parent_thread_id.as_deref() != Some(context.thread_id.as_str())
        || execution.finished_at.is_some()
        || matches!(
            execution.status.as_str(),
            "completed" | "failed" | "cancelled"
        )
    {
        anyhow::bail!("persisted AgentExecution no longer owns this active Turn");
    }
    let identity_row =
        pioneer_crud::load_agent_identity(&database, execution.agent_identity_id.as_str())
            .await?
            .ok_or_else(|| anyhow::anyhow!("persisted Agent identity is missing"))?;
    if identity_row.workspace_id != execution.workspace_id || identity_row.status != "active" {
        anyhow::bail!("persisted Agent identity is unavailable");
    }
    current_agent_identity_source_fence(processor.as_ref(), execution.id.as_str()).await?;
    let source_kind = match identity_row.source_kind.as_str() {
        pioneer_crud::SOURCE_NATIVE_AGENT => pioneer_protocol::AgentIdentitySourceKind::NativeAgent,
        pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE => {
            pioneer_protocol::AgentIdentitySourceKind::CliRuntimeInstance
        }
        pioneer_crud::SOURCE_EPHEMERAL => pioneer_protocol::AgentIdentitySourceKind::Ephemeral,
        _ => anyhow::bail!("persisted Agent identity source kind is unsupported"),
    };
    let snapshot_id = execution
        .presentation_snapshot_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("persisted AgentExecution has no presentation snapshot"))?;
    let snapshot = pioneer_crud::load_agent_presentation_snapshot(&database, snapshot_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("persisted Agent presentation snapshot is missing"))?;
    if snapshot.agent_identity_id != identity_row.id
        || snapshot.source_revision != execution.identity_source_revision
        || snapshot.source_fingerprint != execution.identity_source_fingerprint
    {
        anyhow::bail!("persisted Agent presentation snapshot is inconsistent");
    }
    let identity = pioneer_protocol::AgentIdentityProjection::new(
        pioneer_protocol::AgentIdentityId::new(identity_row.id).map_err(|error| {
            anyhow::anyhow!("persisted Agent identity id is invalid: {error:?}")
        })?,
        source_kind,
        snapshot.display_name,
        snapshot.nickname,
        snapshot.avatar_revision,
        snapshot.role_label,
        u64::try_from(execution.identity_source_revision)
            .map_err(|_| anyhow::anyhow!("persisted Agent identity revision is invalid"))?,
        execution.identity_source_fingerprint.clone(),
    )
    .map_err(|error| anyhow::anyhow!("persisted Agent identity is invalid: {error:?}"))?;
    let grant = pioneer_crud::load_agent_execution_grant(&database, execution_id.as_str())
        .await?
        .ok_or_else(|| anyhow::anyhow!("persisted AgentExecution grant is missing"))?;
    let grant: serde_json::Value = serde_json::from_str(grant.grant_json.as_str())?;
    let profile: pioneer_protocol::AgentExecutionProfileProjection = serde_json::from_value(
        grant
            .get("profile")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("persisted Agent grant has no exact profile"))?,
    )?;
    if execution.resolved_profile_id.as_deref() != Some(profile.id.as_str())
        || execution.resolved_profile_fingerprint.as_deref() != Some(profile.fingerprint.as_str())
    {
        anyhow::bail!("persisted Agent profile differs from its execution snapshot");
    }
    let role_key = grant
        .get("role_key")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("persisted Agent grant has no exact subject role"))?;
    let agent_authorization_fingerprint = grant
        .get("agent_authorization_fingerprint")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("persisted Agent grant has no authorization fingerprint"))?
        .to_owned();
    let allowed_action_names: Vec<String> = serde_json::from_value(
        grant
            .get("allowed_actions")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("persisted Agent grant has no action ceiling"))?,
    )?;
    let persisted_policy_generation = grant
        .get("agent_policy_generation")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("persisted Agent grant has no policy generation"))?;
    let depth = grant
        .get("depth")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let policy_generation = processor.current_authorization_revision().await?.max(1);
    let resource_state =
        pioneer_crud::load_agent_execution_resource_state(&database, execution_id.as_str())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("persisted AgentExecution resource attempt is missing")
            })?;
    let typed_root = AgentExecutionId::new(execution.work_graph_root_execution_id)
        .map_err(|error| anyhow::anyhow!("persisted Agent graph root is invalid: {error:?}"))?;
    let (adapter, options, capabilities) =
        crate::authorization::materialize_persisted_task_agent_action_binding(
            execution_id.clone(),
            execution.home_root_thread_id.as_str(),
            typed_root,
            identity,
            profile,
            u64::try_from(execution.execution_generation)
                .map_err(|_| anyhow::anyhow!("persisted Agent generation is invalid"))?,
            u64::try_from(resource_state.attempt_generation)
                .map_err(|_| anyhow::anyhow!("persisted Agent attempt is invalid"))?,
            u16::try_from(depth)
                .map_err(|_| anyhow::anyhow!("persisted Agent depth is invalid"))?,
            &format!("recovery-turn:{}", context.turn_id),
            role_key,
            persisted_policy_generation,
            policy_generation,
            agent_authorization_fingerprint.as_str(),
            allowed_action_names.as_slice(),
        )
        .map_err(|error| anyhow::anyhow!("failed to restore Agent action binding: {error:?}"))?;
    let binding = processor
        .prepare_agent_action_binding(
            context.turn_id.clone(),
            AgentActionRuntimeBinding::new(adapter, options, capabilities),
        )
        .await?;
    processor
        .register_agent_action_binding(context.turn_id.clone(), binding)
        .await;
    Ok(())
}

/// Materialize the first AgentExecution for a user-started executable Turn.
/// Descendant and Task Turns install their binding at their own atomic writer;
/// this path owns only a root Chat/Agent Composer or CLI Turn that has no
/// parent execution. Message Turns are authored messages and have no
/// responding execution.
async fn ensure_root_agent_action_binding(
    processor: &Arc<MessageProcessor>,
    context: &TurnToolContext,
    requested_launch: Option<&pioneer_protocol::AgentLaunchSelection>,
    requested_backend: Option<&pioneer_protocol::AgentExecutionBackend>,
) -> anyhow::Result<()> {
    if processor
        .agent_action_binding(context.turn_id.as_str())
        .await
        .is_some()
    {
        return Ok(());
    }
    let database = processor.crud_store.database_connection();
    let Some((_, turn)) = processor
        .crud_store
        .get_turn(context.thread_id.as_str(), context.turn_id.as_str())
        .await?
    else {
        anyhow::bail!("Agent Turn disappeared before root execution admission");
    };
    if turn.mode == ThreadMode::Message {
        return Ok(());
    }
    if let Some(response) =
        pioneer_crud::load_agent_turn_response(&database, context.turn_id.as_str()).await?
    {
        let execution_id = AgentExecutionId::new(response.execution_id).map_err(|error| {
            anyhow::anyhow!("persisted responding AgentExecution id is invalid: {error:?}")
        })?;
        return restore_persisted_agent_action_binding(processor, context, &execution_id).await;
    }
    // Agent-authored child Turns are admitted by StartAgent/Task writers. A
    // missing in-memory binding for those paths is a recovery concern, never
    // permission to synthesize a second root from current configuration.
    if let Some(PersistedActorRef::AgentExecution(execution_id)) =
        turn.author.as_ref().map(|author| &author.actor)
    {
        return restore_persisted_agent_action_binding(processor, context, execution_id).await;
    }

    let thread = processor
        .crud_store
        .get_thread_model(context.thread_id.as_str())
        .await?
        .ok_or_else(|| anyhow::anyhow!("root Agent thread is unavailable"))?;
    if thread.workspace_id != context.workspace_id {
        anyhow::bail!("root Agent Turn workspace differs from its thread");
    }
    let authority = processor
        .load_turn_execution_authorization_context(context.turn_id.as_str())
        .await?;
    if authority.workspace_id() != context.workspace_id {
        anyhow::bail!("root Agent Turn authority differs from its collaboration root");
    }
    let cli_binding = processor
        .crud_store
        .get_cli_runtime_turn_binding(context.turn_id.as_str())
        .await?;
    let prepared = prepare_root_agent_execution_admission(
        processor,
        context,
        &thread,
        &authority,
        requested_launch,
        requested_backend,
        cli_binding
            .as_ref()
            .map(|binding| binding.runtime_id.as_str()),
    )
    .await?;
    let committed = processor
        .crud_store
        .commit_agent_execution_graph(prepared.graph.clone())
        .await?;
    if committed.queued {
        anyhow::bail!("admitted root Agent Turn was unexpectedly queued");
    }
    processor
        .notify_agent_work_graph_state_changed(committed.root_execution_id.as_str())
        .await;
    register_prepared_root_agent_action_binding(processor, context, prepared).await
}

pub(crate) struct PreparedRootAgentExecutionAdmission {
    pub(crate) graph: pioneer_crud::AgentExecutionGraphCommitInput,
    execution_id: AgentExecutionId,
    home_root_thread_id: String,
    identity: pioneer_protocol::AgentIdentityProjection,
    profile: pioneer_protocol::AgentExecutionProfileProjection,
    policy_generation: u64,
    authorization_fingerprint: String,
    allowed_actions: Vec<String>,
}

pub(crate) async fn prepare_root_agent_execution_admission(
    processor: &MessageProcessor,
    context: &TurnToolContext,
    thread: &pioneer_protocol::Thread,
    authority: &crate::authorization::ExecutionAuthorizationContext,
    requested_launch: Option<&pioneer_protocol::AgentLaunchSelection>,
    requested_backend: Option<&pioneer_protocol::AgentExecutionBackend>,
    bound_cli_runtime_id: Option<&str>,
) -> anyhow::Result<PreparedRootAgentExecutionAdmission> {
    if thread.id != context.thread_id
        || thread.workspace_id != context.workspace_id
        || authority.workspace_id() != context.workspace_id
    {
        anyhow::bail!("root Agent admission differs from its collaboration root");
    }
    let home_root_thread_id = authority.root_thread_id().to_owned();
    let database = processor.crud_store.database_connection();
    let current_scope = pioneer_crud::resolve_thread_authorization_scope(
        &database,
        context.thread_id.as_str(),
        Some(context.workspace_id.as_str()),
    )
    .await?
    .context("root Agent Turn thread is unavailable for collaboration admission")?;
    if context.thread_id == home_root_thread_id {
        if current_scope.access_class == pioneer_crud::PersistedThreadAccessClass::Internal
            || processor
                .crud_store
                .get_task_thread_lineage(context.thread_id.as_str())
                .await?
                .is_some()
        {
            anyhow::bail!("root Agent admission points at an internal collaboration child");
        }
    } else {
        if current_scope.access_class != pioneer_crud::PersistedThreadAccessClass::Internal {
            anyhow::bail!("child Agent Turn is not in an internal collaboration thread");
        }
        let lineage = processor
            .crud_store
            .get_task_thread_lineage(context.thread_id.as_str())
            .await?
            .context("child Agent Turn has no durable collaboration lineage")?;
        if lineage.child_thread_id != context.thread_id
            || lineage.root_thread_id != home_root_thread_id
            || lineage.root_thread_id == lineage.child_thread_id
            || lineage.depth <= 0
        {
            anyhow::bail!("child Agent Turn lineage differs from its collaboration root");
        }
        let root_scope = pioneer_crud::resolve_thread_authorization_scope(
            &database,
            home_root_thread_id.as_str(),
            Some(context.workspace_id.as_str()),
        )
        .await?
        .context("root Agent collaboration thread is unavailable")?;
        if root_scope.access_class == pioneer_crud::PersistedThreadAccessClass::Internal {
            anyhow::bail!("root Agent collaboration root resolves to an internal child");
        }
    }
    let requested_cli_runtime_id = match requested_backend {
        Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime { runtime_id, .. }) => {
            Some(runtime_id.as_str())
        }
        Some(pioneer_protocol::AgentExecutionBackend::ACPAgentRuntime { .. }) => {
            anyhow::bail!("ACP-backed root Agent execution is unsupported")
        }
        Some(pioneer_protocol::AgentExecutionBackend::ApiProvider { .. }) => None,
        None => bound_cli_runtime_id,
    };
    let requested_identity = requested_launch.map(|launch| &launch.agent);
    if matches!(
        requested_identity,
        Some(pioneer_protocol::AgentIdentitySelection::InheritParent)
    ) {
        anyhow::bail!("a root Agent execution cannot inherit an identity");
    }
    let deriving_ephemeral = matches!(
        requested_identity,
        Some(pioneer_protocol::AgentIdentitySelection::ServerDerivedEphemeral { .. })
    );
    let cli_runtimes = processor.load_cli_runtime_instances()?;
    let cli_identity_catalog = cli_identity_catalog(processor, cli_runtimes.as_slice())?;
    let identity_row = if let Some(runtime_id) = requested_cli_runtime_id {
        if matches!(
            requested_identity,
            Some(pioneer_protocol::AgentIdentitySelection::DefaultPioneer)
        ) {
            anyhow::bail!("Pioneer identity cannot use a CLI runtime backend");
        }
        let identity = pioneer_crud::load_active_agent_identity_by_source(
            &database,
            context.workspace_id.as_str(),
            pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE,
            runtime_id,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("root CLI Agent identity is unavailable"))?;
        if let Some(pioneer_protocol::AgentIdentitySelection::Exact { agent_identity_id }) =
            requested_identity
            && agent_identity_id.as_str() != identity.id
        {
            anyhow::bail!("root Agent identity differs from its requested CLI runtime");
        }
        identity
    } else if let Some(pioneer_protocol::AgentIdentitySelection::Exact { agent_identity_id }) =
        requested_identity
    {
        pioneer_crud::load_active_agent_identity(
            &database,
            context.workspace_id.as_str(),
            agent_identity_id.as_str(),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("root Agent identity is unavailable"))?
    } else {
        let config = pioneer_crud::load_native_agent_config_by_system_key(
            &database,
            context.workspace_id.as_str(),
            "pioneer",
        )
        .await?
        .filter(|config| config.enabled)
        .ok_or_else(|| anyhow::anyhow!("root Pioneer config is unavailable"))?;
        pioneer_crud::load_active_agent_identity_by_source(
            &database,
            context.workspace_id.as_str(),
            pioneer_crud::SOURCE_NATIVE_AGENT,
            config.id.as_str(),
        )
        .await?
        .filter(|identity| identity.source_revision == config.config_revision)
        .ok_or_else(|| anyhow::anyhow!("root Pioneer identity is stale or unavailable"))?
    };
    let source_kind = match identity_row.source_kind.as_str() {
        pioneer_crud::SOURCE_NATIVE_AGENT if requested_cli_runtime_id.is_none() => {
            let config =
                pioneer_crud::load_native_agent_config(&database, identity_row.source_id.as_str())
                    .await?
                    .filter(|config| {
                        config.enabled
                            && config.workspace_id == context.workspace_id
                            && config.config_revision == identity_row.source_revision
                    })
                    .ok_or_else(|| anyhow::anyhow!("root native Agent identity source is stale"))?;
            if !matches!(
                requested_identity,
                Some(pioneer_protocol::AgentIdentitySelection::Exact { .. })
            ) && config.system_key.as_deref() != Some("pioneer")
            {
                anyhow::bail!("default root Agent identity is not Pioneer");
            }
            pioneer_protocol::AgentIdentitySourceKind::NativeAgent
        }
        pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE
            if requested_cli_runtime_id
                .is_some_and(|runtime_id| runtime_id == identity_row.source_id) =>
        {
            cli_identity_catalog
                .iter()
                .find(|runtime| runtime.id == identity_row.source_id && runtime.enabled)
                .filter(|runtime| {
                    pioneer_crud::cli_runtime_identity_fingerprint(runtime)
                        == identity_row.source_fingerprint
                })
                .ok_or_else(|| anyhow::anyhow!("root CLI Agent identity source is stale"))?;
            pioneer_protocol::AgentIdentitySourceKind::CliRuntimeInstance
        }
        _ => anyhow::bail!("root Agent identity source is incompatible with its backend"),
    };
    let snapshot = pioneer_crud::load_current_agent_presentation_snapshot(
        &database,
        identity_row.id.as_str(),
        identity_row.source_revision,
        identity_row.source_fingerprint.as_str(),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("root Agent identity presentation is unavailable"))?;
    let authoritative_snapshot_id = snapshot.id.clone();
    let base_identity = pioneer_protocol::AgentIdentityProjection::new(
        pioneer_protocol::AgentIdentityId::new(identity_row.id.clone())
            .map_err(|error| anyhow::anyhow!("root Agent identity id is invalid: {error:?}"))?,
        source_kind,
        snapshot.display_name,
        snapshot.nickname,
        snapshot.avatar_revision,
        snapshot.role_label,
        u64::try_from(identity_row.source_revision)
            .map_err(|_| anyhow::anyhow!("root Agent identity revision is invalid"))?,
        identity_row.source_fingerprint.clone(),
    )
    .map_err(|error| anyhow::anyhow!("root Agent identity is invalid: {error:?}"))?;
    let policy_generation = processor.current_authorization_revision().await?.max(1);
    let allowed_reasoning = requested_launch
        .and_then(|launch| launch.execution.reasoning.clone())
        .into_iter()
        .collect::<Vec<_>>();
    let base_profile = workspace_launch_profile(
        &base_identity,
        identity_row.source_id.as_str(),
        cli_runtimes.as_slice(),
        thread.model_provider.as_str(),
        thread.model.as_str(),
        allowed_reasoning.as_slice(),
        &[
            pioneer_protocol::TurnPermissionMode::FullAccess,
            pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
            pioneer_protocol::TurnPermissionMode::Supervised,
        ],
        policy_generation,
        policy_generation,
    )?;
    let (mut child_identities, mut child_profiles) = current_workspace_launch_catalog(
        processor,
        context.workspace_id.as_str(),
        &base_identity,
        &base_profile,
        thread.model_provider.as_str(),
        thread.model.as_str(),
    )
    .await?;
    let child_skill_ids = authority
        .granted_skill_ids()
        .iter()
        .map(|id| {
            pioneer_protocol::SkillId::new(id.clone()).map_err(|error| {
                anyhow::anyhow!("root child launch Skill ID is invalid: {error:?}")
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let selection =
        requested_launch
            .cloned()
            .unwrap_or_else(|| pioneer_protocol::AgentLaunchSelection {
                agent: if base_identity.source_kind
                    == pioneer_protocol::AgentIdentitySourceKind::NativeAgent
                {
                    pioneer_protocol::AgentIdentitySelection::DefaultPioneer
                } else {
                    pioneer_protocol::AgentIdentitySelection::Exact {
                        agent_identity_id: base_identity.id.clone(),
                    }
                },
                execution: pioneer_protocol::AgentExecutionSelection {
                    profile: pioneer_protocol::AgentExecutionProfileSelection::Exact {
                        profile_id: base_profile.id.clone(),
                    },
                    reasoning: None,
                    permission_profile: None,
                    skill_ids: Vec::new(),
                    mcp_server_ids: Vec::new(),
                },
            });
    let start_options = pioneer_protocol::AgentStartOptionsProjection {
        agents: child_identities.clone(),
        inherit_parent_agent_available: false,
        derived_ephemeral_available: true,
        profiles: child_profiles.clone(),
        inherit_parent_profile_available: false,
        allowed_skill_ids: child_skill_ids.clone(),
        allowed_mcp_server_ids: authority.granted_mcp_server_capability_ids().to_vec(),
        max_permission_profile: authority.permission_profile_cap().clone(),
        generation_fingerprint: format!("root:{}:{policy_generation}", context.turn_id),
    };
    if selection
        .execution
        .skill_ids
        .iter()
        .any(|id| !start_options.allowed_skill_ids.contains(id))
        || selection
            .execution
            .mcp_server_ids
            .iter()
            .any(|id| !start_options.allowed_mcp_server_ids.contains(id))
    {
        anyhow::bail!("root Agent launch widens its admitted Skill or MCP authority");
    }
    if let Some(permission) = selection.execution.permission_profile.as_ref() {
        let selected = pioneer_protocol::task_permission_cap_snapshot(
            &pioneer_protocol::task_permission_cap_for_mode(permission.mode),
        );
        let ceiling =
            pioneer_protocol::task_permission_cap_snapshot(&start_options.max_permission_profile);
        if pioneer_protocol::intersect_turn_permission_profiles(
            &selected,
            &ceiling,
            pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
        ) != selected
        {
            anyhow::bail!("root Agent launch widens its admitted permission profile");
        }
    }
    let (identity, profile) = if deriving_ephemeral {
        crate::authorization::resolve_ephemeral_agent_launch_selection(
            &selection,
            context.turn_id.as_str(),
            &start_options,
            None,
        )
    } else {
        crate::authorization::resolve_agent_launch_selection(&selection, &start_options, None, None)
    }
    .map_err(|error| anyhow::anyhow!("root Agent launch is invalid: {error:?}"))?;
    if let Some(reasoning) = selection.execution.reasoning.as_ref()
        && !profile.allowed_reasoning.contains(reasoning)
    {
        anyhow::bail!("root Agent reasoning exceeds its exact execution profile");
    }
    if deriving_ephemeral {
        child_identities.push(identity.clone());
        child_profiles.push(profile.clone());
        child_identities.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        child_identities.dedup_by(|left, right| left.id == right.id);
        child_profiles.sort_by(|left, right| left.id.as_str().cmp(right.id.as_str()));
        child_profiles.dedup_by(|left, right| left.id == right.id);
    }
    let identity_source_kind = if deriving_ephemeral {
        pioneer_crud::SOURCE_EPHEMERAL.to_owned()
    } else {
        identity_row.source_kind.clone()
    };
    let identity_source_id = if deriving_ephemeral {
        format!("root-turn:{}", context.turn_id)
    } else {
        identity_row.source_id.clone()
    };
    let identity_source_revision = if deriving_ephemeral {
        i64::try_from(identity.source_revision)
            .map_err(|_| anyhow::anyhow!("ephemeral root identity revision is invalid"))?
    } else {
        identity_row.source_revision
    };
    let identity_source_fingerprint = identity.source_fingerprint.clone();
    let agent_authorization_envelope = crate::authorization::AgentAuthorizationFacts {
        identity_id: identity.id.clone(),
        identity_status: pioneer_protocol::AgentIdentityStatus::Active,
        role_key: "thread_agent".to_owned(),
        root_capsule_id: home_root_thread_id.clone(),
        parent_envelope: None,
        policy_generation,
    }
    .derive_envelope(crate::authorization::RoleDefinitionRegistry::new())
    .ok_or_else(|| anyhow::anyhow!("root Agent authorization envelope is unavailable"))?;
    let agent_authorization_fingerprint = agent_authorization_envelope.fingerprint.clone();
    let agent_authorization_allowed_actions = agent_authorization_envelope.allowed_action_names();
    let child_launch_grant =
        pioneer_protocol::ChildAgentLaunchGrantSet::new(child_identities, child_profiles)
            .and_then(|grant| {
                grant.with_policy(
                    true,
                    agent_authorization_envelope
                        .allows(crate::authorization::ResourceAction::ChildStart),
                    true,
                    child_skill_ids,
                    authority.granted_mcp_server_capability_ids().to_vec(),
                    authority.permission_profile_cap().clone(),
                )
            })
            .map_err(|error| anyhow::anyhow!("root child launch ceiling is invalid: {error:?}"))?;
    let execution_id_value = root_agent_execution_id_for_turn(context.turn_id.as_str());
    let execution_id = AgentExecutionId::new(execution_id_value.clone())
        .map_err(|error| anyhow::anyhow!("root Agent execution id is invalid: {error:?}"))?;
    let snapshot_id = if deriving_ephemeral {
        pioneer_crud::canonical_agent_id(
            'S',
            &format!("root-agent-snapshot\0{}\0{}", identity.id, context.turn_id),
        )
    } else {
        authoritative_snapshot_id
    };
    let now = pioneer_crud::utc_now();
    let policy = crate::authorization::AgentWorkResourcePolicy::default();
    let authorization_fingerprint = authority.authorization_fingerprint()?;
    let root_routes = authority
        .root_route_grants()
        .iter()
        .map(|grant| {
            let expires_at =
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(grant.expires_at)
                    .map(|value| value.fixed_offset())
                    .ok_or_else(|| anyhow::anyhow!("root Agent route expiry is invalid"))?;
            Ok(pioneer_crud::AgentDelegationRouteInput {
                id: grant.route_id.to_string(),
                source_execution_id: execution_id_value.clone(),
                destination_thread_id: grant.destination_thread_id.clone(),
                source_capsule_id: Some(home_root_thread_id.clone()),
                destination_capsule_id: Some(grant.destination_capsule_id.clone()),
                source_workspace_id: Some(context.workspace_id.clone()),
                destination_workspace_id: Some(context.workspace_id.clone()),
                source_gateway_id: Some(grant.gateway_id.clone()),
                destination_gateway_id: Some(grant.gateway_id.clone()),
                source_identity_id: Some(identity.id.to_string()),
                destination_agent_identity_id: grant
                    .destination_agent_identity_id
                    .as_ref()
                    .map(ToString::to_string),
                destination_profile_id: grant
                    .destination_profile_id
                    .as_ref()
                    .map(ToString::to_string),
                home_capsule_id: Some(home_root_thread_id.clone()),
                route_kind: "execution_bound".to_owned(),
                authority_actor_json: grant.authority_actor_json.clone(),
                authority_fingerprint: grant.authority_fingerprint.clone(),
                allowed_actions_json: serde_json::to_string(&grant.allowed_actions)?,
                disclosure_json: serde_json::to_string(&grant.disclosure)?,
                route_generation: 1,
                source_policy_generation: i64::try_from(grant.policy_generation)
                    .map_err(|_| anyhow::anyhow!("root Agent route generation is invalid"))?,
                destination_policy_generation: i64::try_from(grant.policy_generation)
                    .map_err(|_| anyhow::anyhow!("root Agent route generation is invalid"))?,
                hop_count: 1,
                max_hops: 8,
                return_route_id: grant.return_route_id.as_ref().map(ToString::to_string),
                grant_fingerprint: grant.grant_fingerprint.clone(),
                status: "active".to_owned(),
                updated_at: now.clone(),
                expires_at: Some(expires_at),
                now: now.clone(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let root_grant_json = serde_json::json!({
        "kind": "root_turn",
        "turn_id": context.turn_id.clone(),
        "identity": identity.clone(),
        "profile": profile.clone(),
        "selection": selection.clone(),
        "role_key": agent_authorization_envelope.role_key.clone(),
        "agent_policy_generation": agent_authorization_envelope.policy_generation,
        "allowed_actions": agent_authorization_allowed_actions.clone(),
        "agent_authorization_fingerprint": agent_authorization_fingerprint.clone(),
        "child_launch_grant": child_launch_grant,
    })
    .to_string();
    let root_grant_fingerprint =
        pioneer_crud::agent_execution_grant_fingerprint(root_grant_json.as_str())?;
    let graph = pioneer_crud::AgentExecutionGraphCommitInput {
        identity: pioneer_crud::AgentIdentityInput {
            id: identity.id.as_str().to_owned(),
            workspace_id: context.workspace_id.clone(),
            source_kind: identity_source_kind,
            source_id: identity_source_id,
            source_revision: identity_source_revision,
            source_fingerprint: identity_source_fingerprint.clone(),
            now: now.clone(),
        },
        presentation: pioneer_crud::PresentationSnapshotInput {
            id: snapshot_id.clone(),
            agent_identity_id: identity.id.as_str().to_owned(),
            source_revision: identity_source_revision,
            source_fingerprint: identity_source_fingerprint.clone(),
            display_name: identity.display_name.clone(),
            nickname: identity.nickname.clone(),
            avatar_revision: identity.avatar_revision.clone(),
            role_label: identity.role_label.clone(),
            now: now.clone(),
        },
        root_execution_id: execution_id_value.clone(),
        root_execution: Some(pioneer_crud::AgentExecutionInput {
            id: execution_id_value.clone(),
            workspace_id: context.workspace_id.clone(),
            agent_identity_id: identity.id.as_str().to_owned(),
            identity_source_revision,
            identity_source_fingerprint: identity_source_fingerprint.clone(),
            parent_execution_id: None,
            parent_task_id: None,
            parent_thread_id: Some(context.thread_id.clone()),
            home_root_thread_id: home_root_thread_id.clone(),
            work_graph_root_execution_id: execution_id_value.clone(),
            requested_identity_selection_json: serde_json::to_string(&selection.agent)?,
            requested_profile_selection_json: serde_json::to_string(&selection.execution)?,
            resolved_profile_id: Some(profile.id.as_str().to_owned()),
            resolved_profile_fingerprint: Some(profile.fingerprint.clone()),
            presentation_snapshot_id: Some(snapshot_id.clone()),
            authorization_context_fingerprint: authorization_fingerprint.clone(),
            execution_generation: 1,
            status: "created".to_owned(),
            now: now.clone(),
        }),
        child_execution: pioneer_crud::AgentExecutionInput {
            id: execution_id_value.clone(),
            workspace_id: context.workspace_id.clone(),
            agent_identity_id: identity.id.as_str().to_owned(),
            identity_source_revision,
            identity_source_fingerprint,
            parent_execution_id: None,
            parent_task_id: None,
            parent_thread_id: Some(context.thread_id.clone()),
            home_root_thread_id: home_root_thread_id.clone(),
            work_graph_root_execution_id: execution_id_value.clone(),
            requested_identity_selection_json: serde_json::to_string(&selection.agent)?,
            requested_profile_selection_json: serde_json::to_string(&selection.execution)?,
            resolved_profile_id: Some(profile.id.as_str().to_owned()),
            resolved_profile_fingerprint: Some(profile.fingerprint.clone()),
            presentation_snapshot_id: Some(snapshot_id.clone()),
            authorization_context_fingerprint: authorization_fingerprint,
            execution_generation: 1,
            status: "created".to_owned(),
            now: now.clone(),
        },
        root_resource_state: None,
        child_resource_state: pioneer_crud::AgentResourceStateInput {
            id: pioneer_crud::canonical_agent_id(
                'R',
                &format!("root-agent-resource\0{execution_id_value}"),
            ),
            execution_id: execution_id_value.clone(),
            attempt_generation: 1,
            branch_key: format!("root-turn:{}", context.turn_id),
            fair_order: 1,
            now: now.clone(),
        },
        grant: pioneer_crud::AgentExecutionGrantInput {
            id: pioneer_crud::canonical_agent_id(
                'G',
                &format!("root-agent-grant\0{execution_id_value}"),
            ),
            execution_id: execution_id_value.clone(),
            parent_execution_id: None,
            child_identity_id: identity.id.as_str().to_owned(),
            grant_fingerprint: root_grant_fingerprint,
            grant_json: root_grant_json,
            now: now.clone(),
        },
        response: Some(pioneer_crud::AgentTurnResponseInput {
            turn_id: context.turn_id.clone(),
            execution_id: execution_id_value.clone(),
            presentation_snapshot_id: snapshot_id,
            now: now.clone(),
        }),
        root_routes,
        max_concurrency: i32::try_from(policy.max_concurrency).unwrap_or(i32::MAX),
        max_queue_depth: i32::try_from(policy.max_queue_depth).unwrap_or(i32::MAX),
        max_depth: i32::from(policy.max_depth),
        max_fan_out: i32::from(policy.max_fan_out),
        max_total_nodes: i32::try_from(policy.max_total_nodes).unwrap_or(i32::MAX),
        idle_timeout_secs: i64::try_from(policy.idle_timeout_secs).unwrap_or(i64::MAX),
        hard_timeout_secs: i64::try_from(policy.hard_timeout_secs).unwrap_or(i64::MAX),
        child_permit_id: pioneer_crud::canonical_agent_id(
            'P',
            &format!("root-agent-permit\0{execution_id_value}"),
        ),
        child_queue_id: pioneer_crud::canonical_agent_id(
            'Q',
            &format!("root-agent-queue\0{execution_id_value}"),
        ),
        task_actor_contract: None,
        task_occurrence_contract: None,
        contract_now: now.timestamp(),
    };
    Ok(PreparedRootAgentExecutionAdmission {
        graph,
        execution_id,
        home_root_thread_id,
        identity,
        profile,
        policy_generation,
        authorization_fingerprint: agent_authorization_fingerprint,
        allowed_actions: agent_authorization_allowed_actions,
    })
}

pub(crate) async fn register_prepared_root_agent_action_binding(
    processor: &MessageProcessor,
    context: &TurnToolContext,
    prepared: PreparedRootAgentExecutionAdmission,
) -> anyhow::Result<()> {
    let (adapter, options, capabilities) =
        crate::authorization::materialize_persisted_task_agent_action_binding(
            prepared.execution_id.clone(),
            prepared.home_root_thread_id.as_str(),
            prepared.execution_id,
            prepared.identity,
            prepared.profile,
            1,
            1,
            0,
            &format!("root-turn:{}", context.turn_id),
            "thread_agent",
            prepared.policy_generation,
            prepared.policy_generation,
            prepared.authorization_fingerprint.as_str(),
            prepared.allowed_actions.as_slice(),
        )
        .map_err(|error| anyhow::anyhow!("failed to bind root Agent tools: {error:?}"))?;
    let mut binding = AgentActionRuntimeBinding::new(adapter, options, capabilities);
    binding
        .refresh_start_options_catalog(processor, context.turn_id.as_str())
        .await?;
    processor
        .register_agent_action_binding(context.turn_id.clone(), binding)
        .await;
    Ok(())
}

pub(crate) async fn materialize(
    processor: Arc<MessageProcessor>,
    context: TurnToolContext,
) -> Result<pioneer_agent::TurnToolMaterialization, String> {
    ensure_root_agent_action_binding(&processor, &context, None, None)
        .await
        .map_err(|error| format!("failed to bind root Agent execution: {error:#}"))?;
    let Some(binding) = processor
        .agent_action_binding(context.turn_id.as_str())
        .await
    else {
        return Ok(pioneer_agent::TurnToolMaterialization::default());
    };

    // Target options are required by message as well as launch actions. The
    // catalog still capability-filters agent_start_options/agent_start, while
    // every projected mutation receives the same immutable opaque targets.
    let options = Some(binding.options.clone());
    let catalog = project_agent_model_tool_catalog(&binding.capabilities, options.as_ref());
    if catalog.is_empty() {
        return Ok(pioneer_agent::TurnToolMaterialization::default());
    }

    let adapter = binding.adapter.clone();
    let mut bundle = ToolExtensionBundle::default();
    for entry in catalog {
        let name = entry.name.as_str().to_owned();
        bundle
            .specs
            .push(ConfiguredToolSpec::with_output_projection(
                ToolSpec::new(
                    name.clone(),
                    entry.description,
                    entry.parameters,
                    PayloadKind::Function,
                )
                .with_recovery(agent_tool_recovery(entry.name)),
                ExecutionClass::Shared,
                dynamic_unknown_output_policy(),
                pioneer_tools::ToolOutputProjectionKind::DynamicGeneric,
            ));
        bundle.handlers.push((
            name,
            Arc::new(AgentActionToolHandler {
                adapter: adapter.clone(),
                options: options.clone(),
                tool: entry.name,
                processor: Arc::downgrade(&processor),
                context: context.clone(),
            }) as Arc<dyn ToolHandler>,
        ));
    }

    Ok(pioneer_agent::TurnToolMaterialization {
        bundles: vec![bundle],
        diagnostics: Vec::new(),
    })
}

fn agent_tool_recovery(tool: AgentModelToolName) -> ToolRecoveryMetadata {
    if matches!(
        tool,
        AgentModelToolName::AgentStartOptions
            | AgentModelToolName::Wait
            | AgentModelToolName::Result
    ) {
        ToolRecoveryMetadata {
            retry_class: ToolRetryClass::Transient,
            idempotency_mode: ToolIdempotencyMode::Safe,
            max_attempts: 2,
            can_resume: true,
            max_wall_clock_secs: None,
        }
    } else {
        ToolRecoveryMetadata {
            retry_class: ToolRetryClass::Arguments,
            idempotency_mode: ToolIdempotencyMode::RequiresKey,
            max_attempts: 1,
            can_resume: false,
            max_wall_clock_secs: None,
        }
    }
}

struct AgentActionToolHandler {
    adapter: Arc<Mutex<BoundAgentActionAdapter>>,
    options: Option<AgentToolOptionsProjection>,
    tool: AgentModelToolName,
    processor: Weak<MessageProcessor>,
    context: TurnToolContext,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
struct DurableAgentStartDispatch {
    thread_id: String,
    turn_id: String,
    params: TurnStartParams,
    identity: pioneer_protocol::AgentIdentityProjection,
    profile: pioneer_protocol::AgentExecutionProfileProjection,
    identity_source_revision: u64,
    identity_source_fingerprint: String,
    execution_generation: i64,
    depth: u16,
    branch_key: String,
    home_root_thread_id: String,
    work_graph_root_execution_id: AgentExecutionId,
    role_key: String,
    policy_generation: u64,
    policy_fingerprint: String,
    allowed_actions: Vec<String>,
}

/// Deliver the transactional agent domain action outbox. Non-runtime domain
/// mutations are already fully materialized by their canonical transaction;
/// their outbox row is an acknowledgement boundary. StartAgent additionally
/// rehydrates and dispatches the committed Turn only after a durable permit
/// exists.
pub(crate) async fn process_due_agent_action_outbox(
    processor: &Arc<MessageProcessor>,
    limit: u64,
) -> anyhow::Result<usize> {
    let database = processor.crud_store.database_connection();
    let now = pioneer_crud::utc_now();
    let rows = pioneer_crud::claim_agent_action_outbox(&database, now.clone(), limit).await?;
    let mut delivered = 0usize;
    for row in rows {
        let result = dispatch_agent_action_outbox_row(processor, &row).await;
        match result {
            Ok(AgentActionOutboxDispatch::Delivered) => {
                if pioneer_crud::mark_agent_action_outbox_delivered(
                    &database,
                    row.id.as_str(),
                    row.attempts,
                    pioneer_crud::utc_now(),
                )
                .await?
                {
                    delivered = delivered.saturating_add(1);
                }
            }
            Ok(AgentActionOutboxDispatch::AwaitingPermit) => {
                pioneer_crud::defer_agent_action_outbox_for_permit(
                    &database,
                    row.id.as_str(),
                    row.attempts,
                    pioneer_crud::utc_now(),
                )
                .await?;
            }
            Err(_error) => {
                let marked = pioneer_crud::mark_agent_action_outbox_failed(
                    &database,
                    row.id.as_str(),
                    row.attempts,
                    pioneer_crud::utc_now(),
                )
                .await?;
                if marked && row.attempts >= pioneer_crud::AGENT_ACTION_OUTBOX_MAX_ATTEMPTS {
                    tracing::error!(
                        outbox_id = row.id,
                        action_id = row.action_id,
                        attempts = row.attempts,
                        failure_class = "outbox_delivery_failed",
                        "agent domain action outbox reached its delivery retry limit"
                    );
                }
            }
        }
    }
    Ok(delivered)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentActionOutboxDispatch {
    Delivered,
    AwaitingPermit,
}

async fn dispatch_agent_action_outbox_row(
    processor: &Arc<MessageProcessor>,
    row: &pioneer_entity::agent_action_outbox::Model,
) -> anyhow::Result<AgentActionOutboxDispatch> {
    let payload: serde_json::Value = serde_json::from_str(row.payload_json.as_str())?;
    if payload.get("kind").and_then(serde_json::Value::as_str) != Some("start_agent") {
        return Ok(AgentActionOutboxDispatch::Delivered);
    }
    let execution_id = payload
        .get("spawned_execution_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("StartAgent outbox has no spawned execution"))?;
    let dispatch: DurableAgentStartDispatch = serde_json::from_value(
        payload
            .get("dispatch")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("StartAgent outbox has no dispatch envelope"))?,
    )?;
    if dispatch.thread_id != dispatch.params.thread_id
        || dispatch.turn_id != dispatch.params.turn_id
        || dispatch.identity_source_revision != dispatch.identity.source_revision
        || dispatch.identity_source_fingerprint != dispatch.identity.source_fingerprint
    {
        anyhow::bail!("StartAgent outbox dispatch envelope is inconsistent");
    }
    let execution = pioneer_crud::load_agent_execution(
        &processor.crud_store.database_connection(),
        execution_id,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("StartAgent execution is missing"))?;
    if execution.id != execution_id
        || execution.agent_identity_id != dispatch.identity.id.as_str()
        || execution.identity_source_revision
            != i64::try_from(dispatch.identity_source_revision).unwrap_or(-1)
        || execution.identity_source_fingerprint != dispatch.identity_source_fingerprint
        || execution.resolved_profile_id.as_deref() != Some(dispatch.profile.id.as_str())
        || execution.resolved_profile_fingerprint.as_deref()
            != Some(dispatch.profile.fingerprint.as_str())
        || execution.home_root_thread_id != dispatch.home_root_thread_id
        || execution.work_graph_root_execution_id != dispatch.work_graph_root_execution_id.as_str()
        || execution.execution_generation != dispatch.execution_generation
    {
        anyhow::bail!("StartAgent outbox differs from its durable execution");
    }
    let identity = pioneer_crud::load_agent_identity(
        &processor.crud_store.database_connection(),
        dispatch.identity.id.as_str(),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("StartAgent identity is missing"))?;
    if identity.workspace_id != execution.workspace_id || identity.status != "active" {
        anyhow::bail!("StartAgent identity continuity check failed");
    }
    let target_thread = processor
        .crud_store
        .get_thread_model(dispatch.thread_id.as_str())
        .await?
        .ok_or_else(|| anyhow::anyhow!("StartAgent target thread is missing"))?;
    if target_thread.workspace_id != execution.workspace_id {
        anyhow::bail!("StartAgent target thread left its admitted workspace");
    }
    let (_, committed_turn) = processor
        .crud_store
        .get_turn(dispatch.thread_id.as_str(), dispatch.turn_id.as_str())
        .await?
        .ok_or_else(|| anyhow::anyhow!("committed StartAgent Turn is missing"))?;
    let input_author = committed_turn
        .author
        .clone()
        .ok_or_else(|| anyhow::anyhow!("committed StartAgent Turn has no exact input author"))?;
    let pioneer_protocol::PersistedActorRef::AgentExecution(input_execution_id) =
        &input_author.actor
    else {
        anyhow::bail!("committed StartAgent input author is not an AgentExecution");
    };
    if execution.parent_execution_id.as_deref() != Some(input_execution_id.as_str()) {
        anyhow::bail!("committed StartAgent input author is not the child execution parent");
    }
    let response = pioneer_crud::load_agent_turn_response(
        &processor.crud_store.database_connection(),
        dispatch.turn_id.as_str(),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("committed StartAgent Turn has no response execution"))?;
    if response.execution_id != execution.id
        || Some(response.presentation_snapshot_id.as_str())
            != execution.presentation_snapshot_id.as_deref()
    {
        anyhow::bail!("committed StartAgent response differs from its child execution");
    }
    processor
        .ensure_thread_loaded(dispatch.thread_id.as_str(), execution.workspace_id.as_str())
        .await?;
    if committed_turn.status != pioneer_protocol::TurnStatus::InProgress
        || matches!(
            execution.status.as_str(),
            "completed" | "succeeded" | "failed" | "blocked" | "cancelled" | "timed_out"
        )
        || processor
            .agent_manager
            .observe_turn(dispatch.thread_id.as_str(), dispatch.turn_id.as_str())
            .await
            .is_some()
    {
        // The runtime was already activated (or has since reached a terminal
        // state). This is the crash-after-dispatch replay window: acknowledge
        // the outbox without starting the same provider/CLI turn again.
        return Ok(AgentActionOutboxDispatch::Delivered);
    }
    let resource = pioneer_crud::load_agent_execution_resource_state(
        &processor.crud_store.database_connection(),
        execution_id,
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("StartAgent execution resource state is missing"))?;
    if resource.status == "queued" && resource.permit_id.is_none() {
        return Ok(AgentActionOutboxDispatch::AwaitingPermit);
    }
    if resource.status != "running" || resource.permit_id.is_none() {
        anyhow::bail!("StartAgent execution has an inconsistent durable permit state");
    }
    let input = processor
        .crud_store
        .get_turn_inputs(dispatch.turn_id.as_str())
        .await?;
    let outcome = processor
        .thread_manager
        .rehydrate_committed_agent_turn(&dispatch.params, input)
        .await?;
    let authority = processor
        .load_turn_execution_authorization_context(dispatch.turn_id.as_str())
        .await?;
    if authority.workspace_id() != execution.workspace_id
        || authority.root_thread_id() != execution.home_root_thread_id
        || authority.authorization_fingerprint()? != execution.authorization_context_fingerprint
    {
        anyhow::bail!("StartAgent durable authority no longer matches its execution");
    }
    let policy_generation = processor.current_authorization_revision().await?;
    let execution_id = AgentExecutionId::new(execution.id.clone())
        .map_err(|error| anyhow::anyhow!("invalid durable StartAgent execution: {error:?}"))?;
    let execution_generation = u64::try_from(dispatch.execution_generation)
        .map_err(|_| anyhow::anyhow!("invalid durable StartAgent generation"))?;
    let (adapter, options, capabilities) =
        crate::authorization::materialize_persisted_task_agent_action_binding(
            execution_id,
            dispatch.home_root_thread_id.as_str(),
            dispatch.work_graph_root_execution_id.clone(),
            dispatch.identity.clone(),
            dispatch.profile.clone(),
            execution_generation,
            u64::try_from(resource.attempt_generation)
                .map_err(|_| anyhow::anyhow!("invalid durable StartAgent attempt"))?,
            dispatch.depth,
            dispatch.branch_key.as_str(),
            dispatch.role_key.as_str(),
            dispatch.policy_generation,
            policy_generation,
            dispatch.policy_fingerprint.as_str(),
            dispatch.allowed_actions.as_slice(),
        )
        .map_err(|error| anyhow::anyhow!("failed to restore StartAgent tools: {error:?}"))?;
    let binding = processor
        .prepare_agent_action_binding(
            dispatch.turn_id.clone(),
            AgentActionRuntimeBinding::new(adapter, options, capabilities),
        )
        .await?;
    processor
        .register_agent_action_binding(dispatch.turn_id.clone(), binding)
        .await;
    match dispatch.params.execution_backend.clone() {
        Some(pioneer_protocol::AgentExecutionBackend::ApiProvider { .. }) => {
            let prepared = processor
                .prepare_committed_agent_api_turn(&dispatch.params, outcome, &authority)
                .await
                .map_err(anyhow::Error::msg)?;
            processor
                .dispatch_prepared_api_provider_turn_start(prepared)
                .await;
        }
        Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime {
            runtime_id,
            runtime_kind,
        }) => {
            let permission_profile =
                processor.materialized_turn_permission_profile(&outcome.materialization.turn)?;
            let security_snapshot = processor
                .crud_store
                .get_turn_execution_security_snapshot(dispatch.turn_id.as_str())
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("committed StartAgent CLI Turn has no security snapshot")
                })?
                .snapshot;
            let history = processor
                .load_conversation_history_for_workspace(
                    execution.workspace_id.as_str(),
                    dispatch.thread_id.as_str(),
                    dispatch.turn_id.as_str(),
                )
                .await;
            // The outbox owns activation. Clear only the in-memory rehydrated
            // draft before the shared CLI admission path recreates the same
            // deterministic, already-durable Turn idempotently.
            processor
                .thread_manager
                .rollback_turn_start(outcome.rollback_context)
                .await;
            let prepared = processor
                .prepare_committed_agent_cli_runtime_turn(
                    dispatch.params,
                    runtime_id,
                    runtime_kind,
                    permission_profile,
                    security_snapshot,
                    authority,
                    dispatch.thread_id.clone(),
                    dispatch.thread_id,
                    history,
                    input_author,
                )
                .await?;
            processor
                .activate_prepared_committed_agent_cli_runtime_turn(prepared)
                .await?;
        }
        Some(pioneer_protocol::AgentExecutionBackend::ACPAgentRuntime { runtime_id }) => {
            anyhow::bail!(
                "committed StartAgent ACP runtime `{runtime_id}` has no installed dispatcher"
            );
        }
        None => anyhow::bail!("committed StartAgent has no exact execution backend"),
    }
    Ok(AgentActionOutboxDispatch::Delivered)
}

#[async_trait]
impl ToolHandler for AgentActionToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let result = async {
            if self.tool == AgentModelToolName::AgentStartOptions {
                let _: AgentStartOptionsToolInput = match invocation.payload {
                    ToolPayload::Function { arguments } => serde_json::from_value(arguments),
                    ToolPayload::Custom { input } => serde_json::from_str(input.as_str()),
                    _ => {
                        return Err(ToolError::invalid_arguments(
                            AgentPublicOutcome::AgentActionNotAllowed.as_str(),
                        ));
                    }
                }
                .map_err(|_| {
                    ToolError::invalid_arguments(AgentPublicOutcome::AgentActionNotAllowed.as_str())
                })?;
                let options = self.options.as_ref().ok_or_else(|| {
                    ToolError::execution_failed("agent start options are unavailable")
                })?;
                let payload = serde_json::to_value(options).map_err(|error| {
                    ToolError::internal(format!("failed to encode options: {error}"))
                })?;
                return Ok(Box::new(FunctionToolOutput::with_payload(
                    "server-projected agent options",
                    true,
                    payload,
                )) as Box<dyn ToolOutput>);
            }

            let arguments = match invocation.payload {
                ToolPayload::Function { arguments } => arguments,
                ToolPayload::Custom { input } => serde_json::from_str(&input).map_err(|error| {
                    ToolError::invalid_arguments(format!("invalid agent tool arguments: {error}"))
                })?,
                other => {
                    return Err(ToolError::invalid_arguments(format!(
                        "agent tool requires function arguments, got {}",
                        other.log_payload()
                    )));
                }
            };

            let adapter = self.adapter.lock().await;
            let intent = adapter
                .intent_from_model_call(
                    invocation.call_id.as_str(),
                    self.tool,
                    arguments,
                    self.options.as_ref(),
                )
                .map_err(adapter_tool_error)?;
            let Some(intent) = intent else {
                return Ok(denied_output(
                    AgentPublicOutcome::AgentActionNotAllowed,
                    "observation is not available for this execution",
                ));
            };
            let (replay_normalized, replay_request_fingerprint) = adapter
                .preview_for_replay(&intent)
                .map_err(adapter_tool_error)?;
            let replay_action_id = replay_normalized.action_id.clone();
            let replay_execution_id = replay_normalized.execution_id.clone();
            let replay_kind = replay_normalized.kind;
            let replay_idempotency_key = replay_normalized.idempotency_key.clone();
            drop(adapter);
            let processor = self.processor.upgrade().ok_or_else(|| {
                ToolError::execution_failed("message processor is no longer available")
            })?;
            let database = processor.crud_store.database_connection();
            let (existing_action, existing_receipt, existing_outbox) = tokio::try_join!(
                pioneer_crud::load_agent_action(&database, replay_action_id.as_str()),
                pioneer_crud::load_agent_action_receipt(&database, replay_action_id.as_str()),
                pioneer_crud::load_agent_action_outbox(&database, replay_action_id.as_str()),
            )
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to inspect agent action replay: {error:#}"
                ))
            })?;
            if let Some(action) = existing_action {
                if action.execution_id != replay_execution_id.as_str()
                    || action.idempotency_key != replay_idempotency_key
                    || action.request_fingerprint != replay_request_fingerprint
                {
                    return Err(ToolError::invalid_arguments(
                        "agent action idempotency key was reused with different input",
                    ));
                }
                let receipt = existing_receipt.ok_or_else(|| {
                    ToolError::execution_failed("committed agent action has no receipt")
                })?;
                let outbox = existing_outbox.ok_or_else(|| {
                    ToolError::execution_failed("committed agent action has no outbox record")
                })?;
                let status = if replay_kind == pioneer_protocol::AgentActionKind::StartAgent {
                    let payload: serde_json::Value =
                        serde_json::from_str(outbox.payload_json.as_str()).map_err(|error| {
                            ToolError::execution_failed(format!(
                                "committed agent action has an invalid outbox: {error}"
                            ))
                        })?;
                    let spawned_execution_id = payload
                        .get("spawned_execution_id")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            ToolError::execution_failed(
                                "committed StartAgent action has no execution reference",
                            )
                        })?;
                    let resource = pioneer_crud::load_agent_execution_resource_state(
                        &database,
                        spawned_execution_id,
                    )
                    .await
                    .map_err(|error| {
                        ToolError::execution_failed(format!(
                            "failed to inspect committed StartAgent state: {error:#}"
                        ))
                    })?;
                    if resource
                        .as_ref()
                        .is_some_and(|state| state.status == "queued")
                    {
                        AgentToolResultStatus::Queued
                    } else {
                        AgentToolResultStatus::Accepted
                    }
                } else {
                    AgentToolResultStatus::Accepted
                };
                let safe = AgentToolSafeResult {
                    status,
                    outcome: Some(AgentPublicOutcome::AgentActionAlreadyCommitted),
                    receipt_id: Some(receipt.id),
                    outbox_id: Some(outbox.id),
                    public_message: None,
                };
                let payload = serde_json::to_value(&safe).map_err(|error| {
                    ToolError::internal(format!("failed to encode replay result: {error}"))
                })?;
                return Ok(Box::new(FunctionToolOutput::with_payload(
                    format!("{} already committed", replay_kind.safe_name()),
                    true,
                    payload,
                )) as Box<dyn ToolOutput>);
            }
            if existing_receipt.is_some() || existing_outbox.is_some() {
                return Err(ToolError::execution_failed(
                    "agent action replay encountered an incomplete durable commit",
                ));
            }
            let processor = self.processor.upgrade().ok_or_else(|| {
                ToolError::execution_failed("message processor is no longer available")
            })?;
            let execution_id = {
                let adapter = self.adapter.lock().await;
                adapter.execution_id().as_str().to_owned()
            };
            let source_fence =
                current_agent_identity_source_fence(processor.as_ref(), execution_id.as_str())
                    .await
                    .map_err(|error| {
                        ToolError::execution_failed(format!(
                            "Agent identity source revalidation failed: {error:#}"
                        ))
                    })?;
            let mut adapter = self.adapter.lock().await;
            if let AgentActionIntent::SendMessage {
                target,
                input,
                execution_id,
                ..
            } = &intent
            {
                let prepared = adapter.prepare(&intent).map_err(adapter_tool_error)?;
                let policy_fingerprint = adapter.policy_fingerprint().to_owned();
                let policy_generation = adapter.current_policy_generation();
                let author = adapter.presentation_snapshot().to_turn_author_snapshot();
                let mut plan = adapter
                    .prepare_commit(
                        &prepared,
                        None,
                        policy_fingerprint.as_str(),
                        policy_generation,
                    )
                    .map_err(adapter_tool_error)?;
                apply_current_identity_source_fence(&mut plan, &source_fence);
                let target = target.clone();
                let input = input.clone();
                let execution_id = execution_id.clone();
                drop(adapter);
                return self
                    .commit_send_message(target, input, execution_id, author, plan)
                    .await;
            }
            if let AgentActionIntent::CreateThread {
                option,
                execution_id,
                ..
            } = &intent
            {
                let prepared = adapter.prepare(&intent).map_err(adapter_tool_error)?;
                let policy_fingerprint = adapter.policy_fingerprint().to_owned();
                let policy_generation = adapter.current_policy_generation();
                let facts = adapter.persistence_facts();
                let mut plan = adapter
                    .prepare_commit(
                        &prepared,
                        None,
                        policy_fingerprint.as_str(),
                        policy_generation,
                    )
                    .map_err(adapter_tool_error)?;
                apply_current_identity_source_fence(&mut plan, &source_fence);
                let audience = option.audience.clone();
                let execution_id = execution_id.clone();
                drop(adapter);
                return self
                    .commit_create_thread(audience, execution_id, policy_generation, facts, plan)
                    .await;
            }
            if let AgentActionIntent::StartAgent { start, .. } = &intent {
                let start = start.clone();
                drop(adapter);
                let processor = self.processor.upgrade().ok_or_else(|| {
                    ToolError::execution_failed("message processor is no longer available")
                })?;
                let parent_authority = processor
                    .load_turn_execution_authorization_context(self.context.turn_id.as_str())
                    .await
                    .map_err(|error| {
                        ToolError::execution_failed(format!(
                            "failed to load parent execution authority: {error:#}"
                        ))
                    })?;
                let mut adapter = self.adapter.lock().await;
                let prepared = adapter.prepare(&intent).map_err(adapter_tool_error)?;
                let options = self.options.as_ref().ok_or_else(|| {
                    ToolError::execution_failed("agent start options are unavailable")
                })?;
                let child = adapter
                    .materialize_start_agent(
                        &prepared,
                        &start,
                        options,
                        Some(parent_authority.initiating_principal_id().clone()),
                    )
                    .map_err(adapter_tool_error)?;
                let parent_facts = adapter.persistence_facts();
                let policy_fingerprint = adapter.policy_fingerprint().to_owned();
                let policy_generation = adapter.current_policy_generation();
                let mut plan = adapter
                    .prepare_commit(
                        &prepared,
                        None,
                        policy_fingerprint.as_str(),
                        policy_generation,
                    )
                    .map_err(adapter_tool_error)?;
                apply_current_identity_source_fence(&mut plan, &source_fence);
                drop(adapter);
                return self
                    .commit_start_agent(start, parent_authority, parent_facts, child, plan)
                    .await;
            }

            Err(ToolError::internal(
                "projected agent mutation has no registered canonical writer",
            ))
        }
        .await;
        result.map_err(sanitize_agent_tool_error)
    }
}

impl AgentActionToolHandler {
    async fn commit_start_agent(
        &self,
        start: pioneer_protocol::StartAgentIntent,
        parent_authority: crate::authorization::ExecutionAuthorizationContext,
        parent_facts: crate::authorization::AgentExecutionPersistenceFacts,
        child: crate::authorization::MaterializedChildAgentStart,
        mut plan: crate::authorization::AgentActionCommitPlan,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let processor = self.processor.upgrade().ok_or_else(|| {
            ToolError::execution_failed("message processor is no longer available")
        })?;
        let thread_id = match &start.target {
            AgentStartTarget::CurrentThread => self.context.thread_id.clone(),
            AgentStartTarget::SameCapsuleThread { thread_id }
            | AgentStartTarget::RoutedThread { thread_id, .. } => thread_id.clone(),
        };
        let target_thread = processor
            .crud_store
            .get_thread_model(thread_id.as_str())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to resolve Agent target: {error:#}"))
            })?
            .ok_or_else(|| ToolError::execution_failed("Agent target is unavailable"))?;
        if target_thread.workspace_id != self.context.workspace_id {
            return Err(ToolError::execution_failed(
                "Agent target is unavailable for the current execution",
            ));
        }
        processor
            .ensure_thread_loaded(thread_id.as_str(), self.context.workspace_id.as_str())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to load Agent target: {error:#}"))
            })?;

        let execution_backend = match &child.grant.profile.backend {
            pioneer_protocol::AgentExecutionProfileBackend::ApiProvider => {
                Some(pioneer_protocol::AgentExecutionBackend::ApiProvider {
                    provider: child.grant.profile.provider_id.clone(),
                })
            }
            pioneer_protocol::AgentExecutionProfileBackend::AcpAgentRuntime { runtime_id } => {
                Some(pioneer_protocol::AgentExecutionBackend::ACPAgentRuntime {
                    runtime_id: runtime_id.clone(),
                })
            }
            pioneer_protocol::AgentExecutionProfileBackend::CliRuntime {
                runtime_instance_id,
            } => {
                let runtime = processor
                    .load_cli_runtime_instances()
                    .map_err(|error| {
                        ToolError::execution_failed(format!(
                            "failed to resolve CLI runtime selection: {error:#}"
                        ))
                    })?
                    .into_iter()
                    .find(|runtime| runtime.id == *runtime_instance_id && runtime.enabled)
                    .ok_or_else(|| {
                        ToolError::execution_failed("selected CLI runtime is no longer available")
                    })?;
                let runtime_kind = match runtime.kind {
                    pioneer_config::GatewayCliAgentRuntimeKindConfig::Codex => {
                        pioneer_protocol::CLIAgentRuntimeKind::Codex
                    }
                    pioneer_config::GatewayCliAgentRuntimeKindConfig::Claude => {
                        pioneer_protocol::CLIAgentRuntimeKind::Claude
                    }
                };
                Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime {
                    runtime_id: runtime_instance_id.clone(),
                    runtime_kind,
                })
            }
        };
        let permission_profile = pioneer_protocol::resolve_turn_permission_profile(
            start.launch.execution.permission_profile.as_ref(),
        );
        let requested_capabilities = launch_selection_capabilities(&start.launch.execution)
            .map_err(|error| {
                ToolError::invalid_arguments(format!(
                    "invalid child Agent launch capabilities: {error:#}"
                ))
            })?;
        let normalized_capabilities = processor
            .normalize_turn_skill_capabilities(
                self.context.workspace_id.as_str(),
                requested_capabilities.as_slice(),
            )
            .await
            .map_err(|error| {
                ToolError::invalid_arguments(format!(
                    "child Agent launch capabilities are unavailable: {error}"
                ))
            })?;
        let turn_id = pioneer_crud::canonical_agent_id(
            'T',
            &format!("agent-start-turn\0{}", plan.projection.action_id),
        );
        let parent_security_snapshot = processor
            .crud_store
            .get_turn_execution_security_snapshot(self.context.turn_id.as_str())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to load parent Agent security snapshot: {error:#}"
                ))
            })?
            .ok_or_else(|| {
                ToolError::execution_failed("parent Agent security snapshot is unavailable")
            })?
            .snapshot;
        let resolver_backend = match execution_backend.as_ref() {
            Some(pioneer_protocol::AgentExecutionBackend::ApiProvider { provider }) => {
                crate::turn_security::TurnSecurityResolverExecutionBackend::NativeApiProvider {
                    provider: provider.clone(),
                }
            }
            Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime {
                runtime_id,
                runtime_kind: pioneer_protocol::CLIAgentRuntimeKind::Codex,
            }) => crate::turn_security::TurnSecurityResolverExecutionBackend::CodexCli {
                runtime_id: runtime_id.clone(),
            },
            Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime {
                runtime_id,
                runtime_kind: pioneer_protocol::CLIAgentRuntimeKind::Claude,
            }) => crate::turn_security::TurnSecurityResolverExecutionBackend::ClaudeCli {
                runtime_id: runtime_id.clone(),
            },
            Some(pioneer_protocol::AgentExecutionBackend::ACPAgentRuntime { .. }) | None => {
                return Err(ToolError::execution_failed(
                    "selected Agent backend has no installed security/runtime adapter",
                ));
            }
        };
        let child_security_cap =
            crate::turn_security::task_security_cap_from_snapshot(&parent_security_snapshot);
        let child_security_snapshot =
            crate::turn_security::resolve_task_child_execution_security_for_backend(
                self.context.workspace_id.as_str(),
                self.context.turn_id.as_str(),
                &parent_security_snapshot,
                &child_security_cap,
                permission_profile,
                resolver_backend,
                thread_id.as_str(),
                turn_id.as_str(),
                pioneer_crud::utc_now().timestamp_millis(),
            )
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to derive child Agent security snapshot: {error:#}"
                ))
            })?;
        let permission_profile = child_security_snapshot.permission_profile.clone();
        let provider_authority = matches!(
            &child.grant.profile.backend,
            pioneer_protocol::AgentExecutionProfileBackend::ApiProvider
        )
        .then(|| {
            processor
                .provider_registry
                .authority_fingerprint_for_workspace(
                    self.context.workspace_id.as_str(),
                    child.grant.profile.provider_id.as_str(),
                )
        });
        let child_authority = parent_authority
            .derive_agent_continuation(
                child.grant.home_root_thread_id.as_str(),
                child.grant.profile.provider_id.as_str(),
                child.grant.profile.model_id.as_str(),
                execution_backend.as_ref(),
                normalized_capabilities.execution.as_slice(),
                &permission_profile,
                provider_authority.as_ref().map(|value| value.as_str()),
            )
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "child execution authority could not be derived: {error:#}"
                ))
            })?;
        let authority_json = child_authority.to_persisted_json().map_err(|error| {
            ToolError::internal(format!("failed to encode child authority: {error:#}"))
        })?;
        let params = TurnStartParams {
            agent_delegation_routes: Vec::new(),
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            input: child.authored_turn.input.0.clone(),
            capabilities: normalized_capabilities.execution,
            model: Some(child.grant.profile.model_id.clone()),
            model_provider: Some(child.grant.profile.provider_id.clone()),
            sandbox_policy: None,
            mode: Some(ThreadMode::Agent),
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: execution_backend.clone(),
            reasoning: start.launch.execution.reasoning.clone(),
            permission_profile: start.launch.execution.permission_profile.clone(),
            cli_runtime_options: None,
        };
        let outcome = processor
            .thread_manager
            .agent_turn_start_with_permission_profile(
                params.clone(),
                permission_profile.clone(),
                child.authored_turn.author.clone(),
            )
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to prepare child Agent: {error:#}"))
            })?;
        let audit_event = processor.turn_profile_selected_audit_event_for_turn(
            self.context.workspace_id.as_str(),
            thread_id.as_str(),
            turn_id.as_str(),
            permission_profile,
        );
        let authorization_revision = processor
            .authorization_invalidation_hub
            .current_revision()
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "child Agent policy generation is unavailable: {error:#}"
                ))
            })?;
        let child_authority_revalidation = processor
            .execution_leases
            .revalidate_context(
                processor.crud_store.as_ref(),
                &child_authority,
                crate::authorization::ResourceAction::AgentTurnStart,
                authorization_revision,
            )
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "child Agent authority is no longer current: {error:#}"
                ))
            })?;
        let admission = child_authority
            .durable_turn_admission_after_revalidation(
                thread_id.as_str(),
                turn_id.as_str(),
                execution_backend.as_ref(),
                &child_authority_revalidation,
            )
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to derive child Turn admission: {error:#}"
                ))
            })?;
        let now = pioneer_crud::utc_now();
        let identity_row = pioneer_crud::load_agent_identity(
            &processor.crud_store.database_connection(),
            child.grant.identity.id.as_str(),
        )
        .await
        .map_err(|error| {
            ToolError::execution_failed(format!("failed to resolve child identity: {error:#}"))
        })?;
        let source_kind = match child.grant.identity.source_kind {
            pioneer_protocol::AgentIdentitySourceKind::NativeAgent => {
                pioneer_crud::SOURCE_NATIVE_AGENT
            }
            pioneer_protocol::AgentIdentitySourceKind::CliRuntimeInstance => {
                pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE
            }
            pioneer_protocol::AgentIdentitySourceKind::Ephemeral => pioneer_crud::SOURCE_EPHEMERAL,
        };
        let source_id = identity_row
            .as_ref()
            .map(|row| row.source_id.clone())
            .unwrap_or_else(|| format!("agent-execution:{}", child.grant.execution_id));
        if identity_row.as_ref().is_some_and(|row| {
            row.workspace_id != self.context.workspace_id
                || row.source_kind != source_kind
                || row.source_revision
                    != i64::try_from(child.grant.identity.source_revision).unwrap_or(-1)
                || row.source_fingerprint != child.grant.identity.source_fingerprint
        }) {
            processor
                .thread_manager
                .rollback_turn_start(outcome.rollback_context.clone())
                .await;
            return Err(ToolError::execution_failed(
                "child identity changed before commit",
            ));
        }
        let source_revision =
            i64::try_from(child.grant.identity.source_revision).map_err(|_| {
                ToolError::execution_failed("child identity revision exceeds persistence bounds")
            })?;
        let execution_generation = i64::try_from(parent_facts.execution_generation)
            .ok()
            .and_then(|generation| generation.checked_add(1))
            .ok_or_else(|| {
                ToolError::execution_failed("child execution generation exceeds persistence bounds")
            })?;
        let (child_adapter, child_options, child_capabilities) =
            crate::authorization::materialize_child_agent_action_binding(
                &child.grant,
                child.grant.identity.source_revision,
                child.grant.identity.source_fingerprint.as_str(),
                u64::try_from(execution_generation).map_err(|_| {
                    ToolError::execution_failed("child execution generation is invalid")
                })?,
                parent_authority.policy_revision(),
            )
            .map_err(adapter_tool_error)?;
        let authorization_fingerprint =
            child_authority
                .authorization_fingerprint()
                .map_err(|error| {
                    ToolError::internal(format!("failed to fingerprint child authority: {error:#}"))
                })?;
        let snapshot_id = pioneer_crud::canonical_agent_id(
            'S',
            &format!(
                "agent-start-snapshot\0{}\0{}",
                child.grant.identity.id, child.grant.execution_id
            ),
        );
        let resource =
            plan.input.resource.clone().ok_or_else(|| {
                ToolError::internal("StartAgent action has no resource admission")
            })?;
        if resource.execution_id != child.grant.execution_id.as_str()
            || resource.root_execution_id != child.grant.root_execution_id.as_str()
        {
            processor
                .thread_manager
                .rollback_turn_start(outcome.rollback_context.clone())
                .await;
            return Err(ToolError::internal(
                "StartAgent resource admission differs from its child graph",
            ));
        }
        let policy = crate::authorization::AgentWorkResourcePolicy::default();
        let agent_authorization_fingerprint = child.grant.envelope.fingerprint.clone();
        let execution_id = child.grant.execution_id.clone();
        let child_grant_json = serde_json::json!({
            "kind": "agent_child",
            "parent_execution_id": parent_facts.execution_id.clone(),
            "execution_id": execution_id.clone(),
            "root_execution_id": child.grant.root_execution_id.clone(),
            "home_root_thread_id": child.grant.home_root_thread_id.clone(),
            "identity": child.grant.identity.clone(),
            "profile": child.grant.profile.clone(),
            "depth": child.grant.depth,
            "role_key": child.grant.envelope.role_key.clone(),
            "agent_policy_generation": child.grant.envelope.policy_generation,
            "allowed_actions": child.grant.envelope.allowed_action_names(),
            "agent_authorization_fingerprint": agent_authorization_fingerprint,
            "child_launch_grant": child.grant.child_launch_ceiling.clone(),
        })
        .to_string();
        let child_grant_fingerprint = pioneer_crud::agent_execution_grant_fingerprint(
            child_grant_json.as_str(),
        )
        .map_err(|error| {
            ToolError::internal(format!(
                "failed to fingerprint child launch grant: {error:#}"
            ))
        })?;
        let graph = pioneer_crud::AgentExecutionGraphCommitInput {
            identity: pioneer_crud::AgentIdentityInput {
                id: child.grant.identity.id.as_str().to_owned(),
                workspace_id: self.context.workspace_id.clone(),
                source_kind: source_kind.to_owned(),
                source_id,
                source_revision,
                source_fingerprint: child.grant.identity.source_fingerprint.clone(),
                now: now.clone(),
            },
            presentation: pioneer_crud::PresentationSnapshotInput {
                id: snapshot_id.clone(),
                agent_identity_id: child.grant.identity.id.as_str().to_owned(),
                source_revision,
                source_fingerprint: child.grant.identity.source_fingerprint.clone(),
                display_name: child.grant.identity.display_name.clone(),
                nickname: child.grant.identity.nickname.clone(),
                avatar_revision: child.grant.identity.avatar_revision.clone(),
                role_label: child.grant.identity.role_label.clone(),
                now: now.clone(),
            },
            root_execution_id: child.grant.root_execution_id.as_str().to_owned(),
            root_execution: None,
            child_execution: pioneer_crud::AgentExecutionInput {
                id: execution_id.as_str().to_owned(),
                workspace_id: self.context.workspace_id.clone(),
                agent_identity_id: child.grant.identity.id.as_str().to_owned(),
                identity_source_revision: source_revision,
                identity_source_fingerprint: child.grant.identity.source_fingerprint.clone(),
                parent_execution_id: Some(parent_facts.execution_id.as_str().to_owned()),
                parent_task_id: None,
                parent_thread_id: Some(thread_id.clone()),
                home_root_thread_id: child.grant.home_root_thread_id.clone(),
                work_graph_root_execution_id: child.grant.root_execution_id.as_str().to_owned(),
                requested_identity_selection_json: serde_json::to_string(&start.launch.agent)
                    .map_err(|error| {
                        ToolError::internal(format!(
                            "failed to encode child identity selection: {error}"
                        ))
                    })?,
                requested_profile_selection_json: serde_json::to_string(&start.launch.execution)
                    .map_err(|error| {
                        ToolError::internal(format!(
                            "failed to encode child profile selection: {error}"
                        ))
                    })?,
                resolved_profile_id: Some(child.grant.profile.id.as_str().to_owned()),
                resolved_profile_fingerprint: Some(child.grant.profile.fingerprint.clone()),
                presentation_snapshot_id: Some(snapshot_id.clone()),
                authorization_context_fingerprint: authorization_fingerprint,
                execution_generation,
                status: "created".to_owned(),
                now: now.clone(),
            },
            root_resource_state: None,
            child_resource_state: pioneer_crud::AgentResourceStateInput {
                id: resource.resource_state_id.clone(),
                execution_id: execution_id.as_str().to_owned(),
                attempt_generation: resource.attempt_generation,
                branch_key: resource.branch_key.clone(),
                fair_order: resource.fair_order,
                now: now.clone(),
            },
            grant: pioneer_crud::AgentExecutionGrantInput {
                id: pioneer_crud::canonical_agent_id(
                    'G',
                    &format!("agent-start-grant\0{execution_id}"),
                ),
                execution_id: execution_id.as_str().to_owned(),
                parent_execution_id: Some(parent_facts.execution_id.as_str().to_owned()),
                child_identity_id: child.grant.identity.id.as_str().to_owned(),
                grant_fingerprint: child_grant_fingerprint,
                grant_json: child_grant_json,
                now: now.clone(),
            },
            response: Some(pioneer_crud::AgentTurnResponseInput {
                turn_id: turn_id.clone(),
                execution_id: execution_id.as_str().to_owned(),
                presentation_snapshot_id: snapshot_id,
                now: now.clone(),
            }),
            root_routes: Vec::new(),
            max_concurrency: i32::try_from(policy.max_concurrency).unwrap_or(i32::MAX),
            max_queue_depth: i32::try_from(policy.max_queue_depth).unwrap_or(i32::MAX),
            max_depth: i32::from(policy.max_depth),
            max_fan_out: i32::from(policy.max_fan_out),
            max_total_nodes: i32::try_from(policy.max_total_nodes).unwrap_or(i32::MAX),
            idle_timeout_secs: i64::try_from(policy.idle_timeout_secs).unwrap_or(i64::MAX),
            hard_timeout_secs: i64::try_from(policy.hard_timeout_secs).unwrap_or(i64::MAX),
            child_permit_id: resource.permit_id.clone().unwrap_or_else(|| {
                pioneer_crud::canonical_agent_id(
                    'P',
                    &format!("agent-start-permit\0{execution_id}"),
                )
            }),
            child_queue_id: resource.queue_id.clone().unwrap_or_else(|| {
                pioneer_crud::canonical_agent_id('Q', &format!("agent-start-queue\0{execution_id}"))
            }),
            task_actor_contract: None,
            task_occurrence_contract: None,
            contract_now: now.timestamp(),
        };
        let mut outbox_payload: serde_json::Value =
            serde_json::from_str(plan.input.outbox_payload_json.as_str()).map_err(|error| {
                ToolError::internal(format!("invalid planned action outbox payload: {error}"))
            })?;
        let outbox_object = outbox_payload
            .as_object_mut()
            .ok_or_else(|| ToolError::internal("planned action outbox payload is not an object"))?;
        outbox_object.insert(
            "dispatch".to_owned(),
            serde_json::json!({
                "thread_id": thread_id.clone(),
                "turn_id": turn_id.clone(),
                "params": params.clone(),
                "identity": child.grant.identity.clone(),
                "profile": child.grant.profile.clone(),
                "identity_source_revision": child.grant.identity.source_revision,
                "identity_source_fingerprint": child.grant.identity.source_fingerprint.clone(),
                "execution_generation": execution_generation,
                "depth": child.grant.depth,
                "branch_key": child.grant.branch_key.clone(),
                "home_root_thread_id": child.grant.home_root_thread_id.clone(),
                "work_graph_root_execution_id": child.grant.root_execution_id.clone(),
                "role_key": child.grant.envelope.role_key.clone(),
                "policy_generation": child.grant.envelope.policy_generation,
                "policy_fingerprint": child.grant.envelope.fingerprint.clone(),
                "allowed_actions": child.grant.envelope.allowed_action_names(),
            }),
        );
        plan.input.outbox_payload_json = outbox_payload.to_string();
        // Durable graph admission is authoritative. The per-adapter
        // coordinator is only an early backpressure hint and must not write a
        // competing permit/queue transition.
        plan.input.resource = None;
        let graph_result = match processor
            .crud_store
            .materialize_agent_turn_start_with_graph_and_action(
                &outcome.materialization.thread,
                outcome.materialization.sandbox_mode,
                &outcome.materialization.turn,
                &outcome.materialization.input,
                None,
                child.authored_turn.author.actor.clone(),
                audit_event,
                authority_json.as_str(),
                admission,
                super::turn_handlers::new_turn_execution(
                    processor.turn_execution_owner_id.as_ref(),
                    execution_backend.as_ref(),
                    &outcome.materialization,
                )
                .map_err(|error| {
                    ToolError::execution_failed(format!(
                        "failed to create child Turn execution ownership: {error:#}"
                    ))
                })?,
                &child_security_snapshot,
                processor.turn_security_audit_events_for_turn(
                    outcome.started_notification.workspace_id.as_str(),
                    outcome.started_notification.thread_id.as_str(),
                    outcome.started_notification.turn.id.as_str(),
                    &child_security_snapshot,
                ),
                graph,
                plan.input,
            )
            .await
        {
            Ok(result) => result,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(outcome.rollback_context)
                    .await;
                return Err(ToolError::execution_failed(format!(
                    "failed to commit child Agent: {error:#}"
                )));
            }
        };
        plan.projection.queued = graph_result.queued;
        processor
            .register_execution_lease(turn_id.as_str())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to register child Turn execution ownership: {error:#}"
                ))
            })?;
        processor
            .notify_agent_work_graph_state_changed(graph_result.root_execution_id.as_str())
            .await;
        let binding = processor
            .prepare_agent_action_binding(
                turn_id.clone(),
                AgentActionRuntimeBinding::new(
                    child_adapter,
                    child_options,
                    child_capabilities,
                ),
            )
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to project child Agent launch catalog: {error:#}"
                ))
            })?;
        processor
            .register_agent_action_binding(turn_id.clone(), binding)
            .await;
        processor
            .send_notification_to_authorized_thread_connections(
                thread_id.as_str(),
                pioneer_protocol::constants::events::TURN_STARTED,
                &outcome.started_notification,
                outcome.started_notification_connection_ids.clone(),
            )
            .await;
        processor
            .notify_thread_tree_changed(self.context.workspace_id.clone())
            .await;
        let safe = BoundAgentActionAdapter::safe_result(&plan.projection);
        let payload = serde_json::to_value(&safe).map_err(|error| {
            ToolError::internal(format!("failed to encode agent result: {error}"))
        })?;
        Ok(Box::new(FunctionToolOutput::with_payload(
            if graph_result.queued {
                "child Agent queued"
            } else {
                "child Agent committed"
            },
            true,
            payload,
        )))
    }

    async fn commit_create_thread(
        &self,
        audience: pioneer_protocol::AgentThreadAudienceTemplate,
        execution_id: AgentExecutionId,
        policy_generation: u64,
        facts: crate::authorization::AgentExecutionPersistenceFacts,
        mut plan: crate::authorization::AgentActionCommitPlan,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let processor = self.processor.upgrade().ok_or_else(|| {
            ToolError::execution_failed("message processor is no longer available")
        })?;
        let source_thread = processor
            .crud_store
            .get_thread_model(self.context.thread_id.as_str())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "failed to resolve Thread creation context: {error:#}"
                ))
            })?
            .ok_or_else(|| ToolError::execution_failed("Thread creation context is unavailable"))?;
        if source_thread.workspace_id != self.context.workspace_id {
            return Err(ToolError::execution_failed(
                "Thread creation context is unavailable",
            ));
        }
        let gateway =
            pioneer_crud::load_gateway_singleton(&processor.crud_store.database_connection())
                .await
                .map_err(|error| {
                    ToolError::execution_failed(format!(
                        "failed to resolve Gateway identity: {error:#}"
                    ))
                })?
                .ok_or_else(|| ToolError::execution_failed("Gateway identity is unavailable"))?;
        let thread_id = pioneer_crud::canonical_agent_id(
            'T',
            &format!("agent-created-thread\0{}", plan.projection.action_id),
        );
        let now = pioneer_crud::utc_now();
        let (origin_kind, sidebar_visibility, visibility, destination_capsule_id) = match audience {
            pioneer_protocol::AgentThreadAudienceTemplate::HomeCapsule => (
                pioneer_protocol::ThreadOriginKind::System,
                pioneer_protocol::ThreadSidebarVisibility::Hidden,
                None,
                facts.home_root_thread_id.clone(),
            ),
            pioneer_protocol::AgentThreadAudienceTemplate::RootDelegation => (
                pioneer_protocol::ThreadOriginKind::Collaborative,
                pioneer_protocol::ThreadSidebarVisibility::Visible,
                Some(pioneer_protocol::ThreadVisibility::Workspace),
                thread_id.clone(),
            ),
        };
        plan.input.destination_scope_id = Some(destination_capsule_id.clone());
        plan.input.disclosure_class = match audience {
            pioneer_protocol::AgentThreadAudienceTemplate::HomeCapsule => "same_capsule",
            pioneer_protocol::AgentThreadAudienceTemplate::RootDelegation => "delegated_root",
        }
        .to_owned();
        let lineage = if matches!(
            audience,
            pioneer_protocol::AgentThreadAudienceTemplate::HomeCapsule
        ) {
            let parent_depth = if self.context.thread_id == facts.home_root_thread_id {
                0
            } else {
                let parent_lineage = processor
                    .crud_store
                    .get_task_thread_lineage(self.context.thread_id.as_str())
                    .await
                    .map_err(|error| {
                        ToolError::execution_failed(format!(
                            "failed to resolve internal Thread parent lineage: {error:#}"
                        ))
                    })?
                    .ok_or_else(|| {
                        ToolError::execution_failed(
                            "internal Thread parent has no durable capsule lineage",
                        )
                    })?;
                if parent_lineage.root_thread_id != facts.home_root_thread_id {
                    return Err(ToolError::execution_failed(
                        "internal Thread parent is outside the execution capsule",
                    ));
                }
                parent_lineage.depth
            };
            Some(pioneer_protocol::TaskThreadLineage {
                child_thread_id: thread_id.clone(),
                parent_thread_id: self.context.thread_id.clone(),
                root_thread_id: facts.home_root_thread_id.clone(),
                depth: parent_depth.checked_add(1).ok_or_else(|| {
                    ToolError::execution_failed("internal Thread lineage depth overflow")
                })?,
                origin_kind: Some("agent_action".to_owned()),
                created_by_thread_id: Some(self.context.thread_id.clone()),
                created_by_turn_id: Some(self.context.turn_id.clone()),
                created_at: now.timestamp(),
            })
        } else {
            None
        };
        let thread = pioneer_protocol::Thread {
            workspace_id: self.context.workspace_id.clone(),
            id: thread_id.clone(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Message,
            model: source_thread.model,
            model_provider: source_thread.model_provider,
            reasoning_effort: source_thread.reasoning_effort,
            created_at: now.timestamp(),
            updated_at: now.timestamp(),
            status: pioneer_protocol::ThreadStatus::Idle,
            origin_kind,
            sidebar_visibility,
            agent_nickname: None,
            agent_role: None,
            visibility,
            turns: Vec::new(),
        };
        let route_id = pioneer_crud::canonical_agent_id(
            'R',
            &format!("agent-created-thread-route\0{}", plan.projection.action_id),
        );
        let allowed_actions = vec![
            pioneer_protocol::AgentRouteAction::SendMessage,
            pioneer_protocol::AgentRouteAction::StartAgent,
            pioneer_protocol::AgentRouteAction::CreateTask,
            pioneer_protocol::AgentRouteAction::ScheduleTask,
            pioneer_protocol::AgentRouteAction::DeliverResult,
        ];
        let disclosure = pioneer_protocol::AgentRouteDisclosurePolicy {
            text: true,
            artifacts: true,
            context: true,
            user_input: true,
            result_return: pioneer_protocol::AgentResultReturnPolicy::FullResult,
        };
        let mut digest = Sha256::new();
        digest.update(b"pioneer:agent-runtime:created-thread-route:v1\0");
        digest.update(route_id.as_bytes());
        digest.update([0]);
        digest.update(execution_id.as_str().as_bytes());
        digest.update([0]);
        digest.update(thread_id.as_bytes());
        digest.update([0]);
        digest.update(facts.agent_authorization_fingerprint.as_bytes());
        let grant_fingerprint = hex::encode(digest.finalize());
        let policy_generation = i64::try_from(policy_generation).map_err(|_| {
            ToolError::execution_failed("policy generation exceeds persistence bounds")
        })?;
        let route = if matches!(
            audience,
            pioneer_protocol::AgentThreadAudienceTemplate::RootDelegation
        ) {
            Some(pioneer_crud::AgentDelegationRouteInput {
                id: route_id,
                source_execution_id: execution_id.as_str().to_owned(),
                destination_thread_id: thread_id.clone(),
                source_capsule_id: Some(facts.home_root_thread_id.clone()),
                destination_capsule_id: Some(destination_capsule_id),
                source_workspace_id: Some(self.context.workspace_id.clone()),
                destination_workspace_id: Some(self.context.workspace_id.clone()),
                source_gateway_id: Some(gateway.id.to_string()),
                destination_gateway_id: Some(gateway.id.to_string()),
                source_identity_id: Some(facts.identity.id.as_str().to_owned()),
                destination_agent_identity_id: None,
                destination_profile_id: None,
                home_capsule_id: Some(facts.home_root_thread_id.clone()),
                route_kind: "execution_bound".to_owned(),
                authority_actor_json: serde_json::to_string(
                    &pioneer_protocol::PersistedActorRef::AgentExecution(execution_id.clone()),
                )
                .map_err(|error| {
                    ToolError::internal(format!("failed to encode route authority actor: {error}"))
                })?,
                authority_fingerprint: facts.agent_authorization_fingerprint.clone(),
                allowed_actions_json: serde_json::to_string(&allowed_actions).map_err(|error| {
                    ToolError::internal(format!("failed to encode route actions: {error}"))
                })?,
                disclosure_json: serde_json::to_string(&disclosure).map_err(|error| {
                    ToolError::internal(format!("failed to encode route disclosure: {error}"))
                })?,
                route_generation: 1,
                source_policy_generation: policy_generation.max(1),
                destination_policy_generation: policy_generation.max(1),
                hop_count: 1,
                max_hops: 8,
                return_route_id: None,
                grant_fingerprint,
                status: "active".to_owned(),
                updated_at: now.clone(),
                expires_at: None,
                now: now.clone(),
            })
        } else {
            None
        };
        let mut outbox_payload: serde_json::Value =
            serde_json::from_str(plan.input.outbox_payload_json.as_str()).map_err(|error| {
                ToolError::internal(format!(
                    "failed to decode canonical Thread creation outbox: {error}"
                ))
            })?;
        let outbox_object = outbox_payload.as_object_mut().ok_or_else(|| {
            ToolError::internal("canonical Thread creation outbox is not an object")
        })?;
        outbox_object.insert(
            "created_thread_id".to_owned(),
            serde_json::Value::String(thread_id.clone()),
        );
        outbox_object.insert(
            "thread_audience".to_owned(),
            serde_json::to_value(audience).map_err(|error| {
                ToolError::internal(format!("failed to encode Thread audience: {error}"))
            })?,
        );
        if let Some(route) = route.as_ref() {
            outbox_object.insert(
                "created_route_id".to_owned(),
                serde_json::Value::String(route.id.clone()),
            );
        }
        plan.input.outbox_payload_json = outbox_payload.to_string();
        let safe = BoundAgentActionAdapter::safe_result(&plan.projection);
        processor
            .crud_store
            .commit_agent_thread_creation_with_action(
                thread.clone(),
                execution_id,
                route,
                lineage,
                now.clone(),
                now,
                plan.input,
            )
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to commit agent Thread: {error:#}"))
            })?;
        processor
            .thread_manager
            .system_thread_start_seeded(
                self.context.workspace_id.clone(),
                pioneer_protocol::ThreadStartParams {
                    thread_id: thread.id.clone(),
                    workspace_id: thread.workspace_id.clone(),
                    name: thread.name.clone(),
                    model: Some(thread.model.clone()),
                    model_provider: Some(thread.model_provider.clone()),
                    sandbox: None,
                    mode: Some(thread.mode),
                    origin_kind: Some(thread.origin_kind),
                    sidebar_visibility: Some(thread.sidebar_visibility),
                    visibility: thread.visibility,
                    agent_nickname: None,
                    agent_role: None,
                },
                Some(thread),
                None,
            )
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "committed agent Thread could not be loaded: {error:#}"
                ))
            })?;
        processor
            .notify_thread_tree_changed(self.context.workspace_id.clone())
            .await;
        Self::mark_outbox_delivered(processor.as_ref(), plan.projection.outbox_id.as_str()).await;

        let payload = serde_json::to_value(&safe).map_err(|error| {
            ToolError::internal(format!("failed to encode agent result: {error}"))
        })?;
        Ok(Box::new(FunctionToolOutput::with_payload(
            "agent Thread committed",
            true,
            payload,
        )))
    }

    async fn commit_send_message(
        &self,
        target: AgentStartTarget,
        input: pioneer_protocol::AgentAuthoredInput,
        execution_id: AgentExecutionId,
        author: pioneer_protocol::TurnAuthorSnapshot,
        plan: crate::authorization::AgentActionCommitPlan,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let processor = self.processor.upgrade().ok_or_else(|| {
            ToolError::execution_failed("message processor is no longer available")
        })?;
        let thread_id = match target {
            AgentStartTarget::CurrentThread => self.context.thread_id.clone(),
            AgentStartTarget::SameCapsuleThread { thread_id }
            | AgentStartTarget::RoutedThread { thread_id, .. } => thread_id,
        };
        let thread = processor
            .crud_store
            .get_thread_model(thread_id.as_str())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to resolve message target: {error:#}"))
            })?
            .ok_or_else(|| ToolError::execution_failed("message target is unavailable"))?;
        if thread.workspace_id != self.context.workspace_id {
            return Err(ToolError::execution_failed(
                "message target is unavailable for the current execution",
            ));
        }
        processor
            .ensure_thread_loaded(thread_id.as_str(), self.context.workspace_id.as_str())
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to load message target: {error:#}"))
            })?;
        let turn_id = pioneer_crud::canonical_agent_id(
            'T',
            &format!("agent-message-turn\0{}", plan.projection.action_id),
        );
        let params = TurnStartParams {
            agent_delegation_routes: Vec::new(),
            thread_id: thread_id.clone(),
            turn_id,
            input: input.0,
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: Some(ThreadMode::Message),
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
        };
        let outcome = processor
            .thread_manager
            .prepare_completed_agent_message_turn(&params, author)
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to prepare agent message: {error:#}"))
            })?;
        let audit_event = processor.turn_profile_selected_audit_event_for_turn(
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
            outcome.materialization.turn.permission_profile.clone(),
        );
        let safe = BoundAgentActionAdapter::safe_result(&plan.projection);
        processor
            .crud_store
            .materialize_completed_agent_message_turn_with_action(
                pioneer_crud::CompletedMessageTurnWrite {
                    thread: &outcome.materialization.thread,
                    sandbox_mode: outcome.materialization.sandbox_mode,
                    started_turn: &outcome.materialization.turn,
                    input: &outcome.materialization.input,
                    actor: PersistedActorRef::AgentExecution(execution_id),
                    completed: outcome.completed_notification.clone(),
                    audit_event,
                },
                plan.input,
            )
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to commit agent message: {error:#}"))
            })?;
        if let Err(_error) = processor
            .thread_manager
            .commit_completed_message_turn(&outcome)
            .await
        {
            tracing::warn!(
                thread_id = thread_id.as_str(),
                failure_class = "agent_message_loaded_state_apply_failed",
                "committed agent Message could not be applied to loaded state"
            );
        }
        processor
            .send_notification_to_authorized_thread_connections(
                thread_id.as_str(),
                pioneer_protocol::constants::events::TURN_STARTED,
                &outcome.started_notification,
                outcome.notification_connection_ids.clone(),
            )
            .await;
        processor
            .send_notification_to_authorized_thread_connections(
                thread_id.as_str(),
                pioneer_protocol::constants::events::TURN_COMPLETED,
                &outcome.completed_notification,
                outcome.notification_connection_ids,
            )
            .await;
        processor
            .notify_semantic_user_message_changed(
                outcome.completed_notification.workspace_id.as_str(),
                thread_id.as_str(),
                outcome.completed_notification.turn.id.as_str(),
            )
            .await;
        processor
            .notify_thread_tree_changed(outcome.completed_notification.workspace_id)
            .await;
        Self::mark_outbox_delivered(processor.as_ref(), plan.projection.outbox_id.as_str()).await;

        let payload = serde_json::to_value(&safe).map_err(|error| {
            ToolError::internal(format!("failed to encode agent result: {error}"))
        })?;
        Ok(Box::new(FunctionToolOutput::with_payload(
            "agent message committed",
            true,
            payload,
        )))
    }

    async fn mark_outbox_delivered(processor: &MessageProcessor, outbox_id: &str) {
        match pioneer_crud::mark_agent_action_outbox_delivered(
            &processor.crud_store.database_connection(),
            outbox_id,
            0,
            pioneer_crud::utc_now(),
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(outbox_id, "agent action outbox changed before finalization")
            }
            Err(_error) => tracing::warn!(
                outbox_id,
                failure_class = "agent_action_outbox_finalize_failed",
                "agent action outbox could not be finalized after delivery"
            ),
        }
    }
}

pub(crate) async fn authorize_task_observations(
    processor: &MessageProcessor,
    execution_id: &AgentExecutionId,
    root_execution_id: &AgentExecutionId,
    task_ids: &[String],
) -> Result<(), ToolError> {
    if task_ids.is_empty()
        || task_ids.len() > 64
        || task_ids
            .iter()
            .any(|task_id| task_id.len() != 21 || task_id.trim() != task_id)
        || task_ids.iter().collect::<BTreeSet<_>>().len() != task_ids.len()
    {
        return Err(ToolError::invalid_arguments(
            AgentPublicOutcome::AgentActionNotAllowed.as_str(),
        ));
    }
    for task_id in task_ids {
        let contract = processor
            .crud_store
            .get_task_actor_contract(task_id.as_str())
            .await
            .map_err(|_| {
                ToolError::execution_failed("task is unavailable for the current execution")
            })?
            .ok_or_else(|| {
                ToolError::execution_failed("task is unavailable for the current execution")
            })?;
        let exact_creator = matches!(
            &contract.creator,
            PersistedActorRef::AgentExecution(creator) if creator == execution_id
        );
        let same_graph =
            contract.work_graph_root_execution_id.as_deref() == Some(root_execution_id.as_str());
        let owns_occurrence = if exact_creator || same_graph {
            false
        } else {
            processor
                .crud_store
                .task_occurrence_matches_execution_or_graph(
                    task_id.as_str(),
                    execution_id.as_str(),
                    root_execution_id.as_str(),
                )
                .await
                .map_err(|_| {
                    ToolError::execution_failed("task is unavailable for the current execution")
                })?
        };
        if !exact_creator && !same_graph && !owns_occurrence {
            return Err(ToolError::execution_failed(
                "task is unavailable for the current execution",
            ));
        }
    }
    Ok(())
}

pub(crate) fn adapter_tool_error(error: AgentToolAdapterError) -> ToolError {
    match error {
        AgentToolAdapterError::InvalidInput(_) => {
            ToolError::invalid_arguments(AgentPublicOutcome::AgentActionNotAllowed.as_str())
        }
        AgentToolAdapterError::OptionsUnavailable => {
            ToolError::execution_failed(AgentPublicOutcome::AgentActorUnavailable.as_str())
        }
        AgentToolAdapterError::Action(error) => ToolError::execution_failed(
            match error {
                crate::authorization::AgentActionServiceError::MalformedIntent(_)
                | crate::authorization::AgentActionServiceError::NotAuthorized(_) => {
                    AgentPublicOutcome::AgentActionNotAllowed
                }
                crate::authorization::AgentActionServiceError::PayloadBoundary => {
                    AgentPublicOutcome::AgentActionPayloadLimitExceeded
                }
                crate::authorization::AgentActionServiceError::TargetNotAuthorized(_) => {
                    AgentPublicOutcome::AgentDestinationUnavailable
                }
                crate::authorization::AgentActionServiceError::ResourceBoundary => {
                    AgentPublicOutcome::AgentWorkGraphLimitExceeded
                }
                crate::authorization::AgentActionServiceError::Commit(_) => {
                    AgentPublicOutcome::AgentActionConflict
                }
            }
            .as_str(),
        ),
    }
}

/// Model-facing tool failures must never echo database/provider errors, raw
/// target identifiers, host metadata, or authored payloads. Preserve only the
/// fixed agent domain outcome vocabulary produced by the canonical adapter;
/// collapse every other detail at this final disclosure boundary.
pub(crate) fn sanitize_agent_tool_error(error: ToolError) -> ToolError {
    fn stable_outcome(message: &str) -> Option<&'static str> {
        [
            AgentPublicOutcome::AgentActorUnavailable,
            AgentPublicOutcome::AgentIdentityUnavailable,
            AgentPublicOutcome::AgentIdentityRevisionStale,
            AgentPublicOutcome::AgentExecutionStale,
            AgentPublicOutcome::AgentExecutionProfileUnavailable,
            AgentPublicOutcome::AgentExecutionSelectionNotAllowed,
            AgentPublicOutcome::AgentNicknameUnavailable,
            AgentPublicOutcome::AgentActionNotAllowed,
            AgentPublicOutcome::AgentRouteRequired,
            AgentPublicOutcome::AgentRouteRevoked,
            AgentPublicOutcome::AgentRouteExpired,
            AgentPublicOutcome::AgentDestinationUnavailable,
            AgentPublicOutcome::AgentContextExportDenied,
            AgentPublicOutcome::AgentActionConflict,
            AgentPublicOutcome::AgentActionAlreadyCommitted,
            AgentPublicOutcome::AgentRecoveryQuarantined,
            AgentPublicOutcome::AgentWorkQueued,
            AgentPublicOutcome::AgentWorkQueueFull,
            AgentPublicOutcome::AgentWorkGraphLimitExceeded,
            AgentPublicOutcome::AgentActionPayloadLimitExceeded,
            AgentPublicOutcome::AgentRuntimeIntegrityLost,
        ]
        .into_iter()
        .map(AgentPublicOutcome::as_str)
        .find(|candidate| *candidate == message)
    }

    match error {
        ToolError::InvalidArguments(message) => ToolError::invalid_arguments(
            stable_outcome(message.as_str())
                .unwrap_or(AgentPublicOutcome::AgentActionNotAllowed.as_str()),
        ),
        ToolError::NotFound(_) | ToolError::NotVisible(_) | ToolError::Rejected(_) => {
            ToolError::Rejected(
                AgentPublicOutcome::AgentActionNotAllowed
                    .as_str()
                    .to_owned(),
            )
        }
        ToolError::Cancelled(_) => {
            ToolError::cancelled(AgentPublicOutcome::AgentExecutionStale.as_str())
        }
        ToolError::ExecutionFailed(message) => ToolError::execution_failed(
            stable_outcome(message.as_str())
                .unwrap_or(AgentPublicOutcome::AgentRuntimeIntegrityLost.as_str()),
        ),
        ToolError::Internal(_) => {
            ToolError::internal(AgentPublicOutcome::AgentRuntimeIntegrityLost.as_str())
        }
    }
}

fn denied_output(outcome: AgentPublicOutcome, message: &str) -> Box<dyn ToolOutput> {
    let safe = AgentToolSafeResult {
        status: AgentToolResultStatus::Denied,
        outcome: Some(outcome),
        receipt_id: None,
        outbox_id: None,
        public_message: Some(message.to_owned()),
    };
    let payload = serde_json::to_value(safe).expect("safe agent result serializes");
    Box::new(FunctionToolOutput::with_payload(message, false, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(id: &str, revision: u64) -> pioneer_protocol::AgentIdentityProjection {
        pioneer_protocol::AgentIdentityProjection::new(
            pioneer_protocol::AgentIdentityId::new(id).unwrap(),
            pioneer_protocol::AgentIdentitySourceKind::NativeAgent,
            "Agent",
            "agent",
            None,
            None,
            revision,
            format!("source-{revision}"),
        )
        .unwrap()
    }

    fn profile(
        id: &str,
        identity: &pioneer_protocol::AgentIdentityProjection,
    ) -> pioneer_protocol::AgentExecutionProfileProjection {
        pioneer_protocol::AgentExecutionProfileProjection {
            id: pioneer_protocol::AgentExecutionProfileId::new(id).unwrap(),
            compatible_agent_identity_ids: vec![identity.id.clone()],
            backend: pioneer_protocol::AgentExecutionProfileBackend::ApiProvider,
            provider_id: "provider".to_owned(),
            model_id: "model".to_owned(),
            provider_display_name: "Provider".to_owned(),
            model_display_name: "Model".to_owned(),
            allowed_reasoning: Vec::new(),
            allowed_permission_profiles: Vec::new(),
            catalog_generation: 1,
            policy_generation: 1,
            fingerprint: format!("profile-{id}"),
        }
    }

    #[test]
    fn mutation_recovery_requires_idempotency() {
        let recovery = agent_tool_recovery(AgentModelToolName::SendMessage);
        assert_eq!(recovery.idempotency_mode, ToolIdempotencyMode::RequiresKey);
        assert_eq!(recovery.max_attempts, 1);
    }

    #[test]
    fn observation_recovery_is_resumable() {
        let recovery = agent_tool_recovery(AgentModelToolName::Result);
        assert_eq!(recovery.idempotency_mode, ToolIdempotencyMode::Safe);
        assert!(recovery.can_resume);
    }

    #[test]
    fn adapter_failures_use_stable_non_disclosing_outcomes() {
        let destination = adapter_tool_error(AgentToolAdapterError::Action(
            crate::authorization::AgentActionServiceError::TargetNotAuthorized("hidden target"),
        ));
        assert_eq!(
            destination.to_string(),
            "tool execution failed: agent_destination_unavailable"
        );
        let resource = adapter_tool_error(AgentToolAdapterError::Action(
            crate::authorization::AgentActionServiceError::ResourceBoundary,
        ));
        assert_eq!(
            resource.to_string(),
            "tool execution failed: agent_work_graph_limit_exceeded"
        );
        assert!(!destination.to_string().contains("hidden target"));

        let payload = adapter_tool_error(AgentToolAdapterError::Action(
            crate::authorization::AgentActionServiceError::PayloadBoundary,
        ));
        assert_eq!(
            payload.to_string(),
            "tool execution failed: agent_action_payload_limit_exceeded"
        );
    }

    #[test]
    fn model_tool_failure_boundary_never_echoes_hidden_payloads() {
        let secret = "secret://host/path/private-target";
        let errors = [
            ToolError::invalid_arguments(secret),
            ToolError::NotFound(secret.to_owned()),
            ToolError::NotVisible(secret.to_owned()),
            ToolError::Rejected(secret.to_owned()),
            ToolError::cancelled(secret),
            ToolError::execution_failed(secret),
            ToolError::internal(secret),
        ];
        for error in errors {
            let public = sanitize_agent_tool_error(error).to_string();
            assert!(!public.contains(secret));
            assert!(public.contains("agent_"));
        }

        let stable = sanitize_agent_tool_error(ToolError::execution_failed(
            AgentPublicOutcome::AgentDestinationUnavailable.as_str(),
        ));
        assert_eq!(
            stable.to_string(),
            "tool execution failed: agent_destination_unavailable"
        );
    }

    #[test]
    fn current_catalog_can_only_narrow_an_immutable_child_launch_ceiling() {
        let granted_identity = identity("A12345678901234567890", 1);
        let granted_profile = profile("P12345678901234567890", &granted_identity);
        let ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(
            vec![granted_identity.clone()],
            vec![granted_profile.clone()],
        )
        .unwrap();
        let added_identity = identity("A12345678901234567891", 1);
        let added_profile = profile("P12345678901234567891", &added_identity);
        let (identities, profiles) = intersect_child_launch_ceiling(
            &ceiling,
            &[granted_identity.clone(), added_identity],
            &[granted_profile.clone(), added_profile],
        );
        assert_eq!(identities, vec![granted_identity.clone()]);
        assert_eq!(profiles, vec![granted_profile]);

        let (identities, profiles) =
            intersect_child_launch_ceiling(&ceiling, &[identity("A12345678901234567890", 2)], &[]);
        assert!(identities.is_empty());
        assert!(profiles.is_empty());
    }
}
