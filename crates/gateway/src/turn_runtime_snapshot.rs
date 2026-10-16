use anyhow::{Context, Result, bail};
use pioneer_agent::{
    AgentTurnHookRuntimeContext, ExecutionWindowUsageSnapshot, ResolvedArtifactInput,
    RestoredRecoveryTurnRequest, WorkspaceSkillPolicy,
};
use pioneer_crud::{CrudStore, NewTurnRuntimeSnapshot, TurnRuntimeSnapshotRecord};
use pioneer_protocol::ReasoningEffort;
use pioneer_protocol::{
    SkillId, ThreadMode, TurnCapability, TurnCapabilityKind, TurnExecutionSecuritySnapshot,
    TurnPermissionProfileSnapshot, UserInput,
};
use pioneer_provider::{ChatMessage, ReasoningConfig};
use pioneer_skills::{
    AgentSkillRuntimeEntry, MAX_ACTIVE_AGENT_SKILLS, SkillPolicyKey,
    ensure_agent_skill_overlay_capacity,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};

const MAX_AGENT_SKILL_PINS_JSON_BYTES: usize = 16 * 1024;
const MAX_AGENT_SKILL_FINGERPRINT_CHARS: usize = 128;

#[derive(Serialize)]
struct CanonicalArtifactSnapshot<'a> {
    artifact_id: &'a str,
    version_id: Option<&'a str>,
    content_type: pioneer_provider::InputContentType,
    attachment: CanonicalAttachmentSnapshot<'a>,
}

#[derive(Serialize)]
struct CanonicalAttachmentSnapshot<'a> {
    mime_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha256: Option<&'a str>,
    source: CanonicalArtifactSource<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact: Option<&'a pioneer_provider::AttachmentArtifactContext>,
}

#[derive(Serialize)]
struct CanonicalArtifactSource<'a> {
    source: &'static str,
    #[serde(serialize_with = "serialize_canonical_artifact_locator")]
    reference: &'a str,
}

fn serialize_canonical_artifact_locator<S: serde::Serializer>(
    id: &str,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    struct Locator<'a>(&'a str);
    impl std::fmt::Display for Locator<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "pioneer-artifact:{}", self.0)
        }
    }
    serializer.collect_str(&Locator(id))
}

#[derive(Serialize)]
struct StoredOrdinarySkillCatalog<'a> {
    schema_version: u32,
    catalog: &'a pioneer_skills::SkillCatalogSnapshot,
}
#[derive(Deserialize)]
struct RestoredOrdinarySkillCatalog {
    schema_version: u32,
    catalog: pioneer_skills::SkillCatalogSnapshot,
}
fn validate_ordinary_skill_catalog(catalog: &pioneer_skills::SkillCatalogSnapshot) -> Result<()> {
    anyhow::ensure!(
        catalog.skills.len()
            <= pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES
                / (pioneer_protocol::SKILL_ID_LEN + 2),
        "ordinary skill catalog cardinality exceeds snapshot byte policy"
    );
    let mut ids = HashSet::new();
    for skill in &catalog.skills {
        anyhow::ensure!(
            ids.insert(skill.identity.skill_id.as_str()),
            "duplicate accepted ordinary skill identity"
        );
        anyhow::ensure!(
            !skill.identity.fingerprint.is_empty()
                && skill.identity.fingerprint.len() <= MAX_AGENT_SKILL_FINGERPRINT_CHARS,
            "invalid accepted ordinary skill fingerprint"
        );
    }
    Ok(())
}
fn restored_ordinary_skill_catalog(
    snapshot: &TurnRuntimeSnapshotRecord,
) -> Result<pioneer_skills::SkillCatalogSnapshot> {
    let value = snapshot
        .ordinary_skill_catalog_json
        .as_deref()
        .context("accepted ordinary skill catalog is unavailable in legacy snapshot")?;
    let stored: RestoredOrdinarySkillCatalog = from_snapshot_json(value, "ordinary skill catalog")?;
    anyhow::ensure!(
        stored.schema_version == 1,
        "unsupported ordinary skill catalog snapshot version"
    );
    validate_ordinary_skill_catalog(&stored.catalog)?;
    Ok(stored.catalog)
}

pub(crate) fn restored_provider_model_contract(
    snapshot: &TurnRuntimeSnapshotRecord,
) -> Result<pioneer_provider::AcceptedProviderModelContract> {
    let contract = pioneer_provider::AcceptedProviderModelContract::from_json(
        snapshot
            .provider_model_contract_json
            .as_deref()
            .context("accepted model contract is unavailable in legacy snapshot")?,
    )?;
    let provider = pioneer_provider::provider_definition(&snapshot.provider_name)
        .map_or(snapshot.provider_name.as_str(), |d| d.name);
    contract.require_selection(provider, &snapshot.model)?;
    Ok(contract)
}

/// Structural compatibility is separate from existence and fresh authorization.
/// No DB access occurs here, so a decode rejection cannot hide a failed read.
pub(crate) fn validate_runtime_snapshot_for_resume(
    snapshot: &TurnRuntimeSnapshotRecord,
) -> Result<()> {
    snapshot.validate_storage_size()?;
    restored_ordinary_skill_catalog(snapshot)?;
    restored_provider_model_contract(snapshot)?;
    let initial = snapshot
        .initial_phase_json
        .as_deref()
        .context("native initial preparation unavailable")?;
    let _ = pioneer_agent::NativeInitialPreparation::restore(
        initial,
        &snapshot.workspace_id,
        &snapshot.thread_id,
        &snapshot.turn_id,
        snapshot
            .remaining_storage_bytes()?
            .checked_add(initial.len())
            .context("native initial phase allowance invalid")?,
    )?;
    pioneer_agent::NativeInitialPreparation::validate_for_resume(
        initial,
        snapshot
            .remaining_storage_bytes()?
            .checked_add(initial.len())
            .context("native initial phase allowance invalid")?,
    )?;
    let _: ThreadMode = from_snapshot_json(&snapshot.mode_json, "thread mode")?;
    let _: AgentTurnHookRuntimeContext =
        from_snapshot_json(&snapshot.hook_runtime_context_json, "hook runtime context")?;
    restore_workspace_skill_policies(&snapshot.workspace_skill_policies_json)?;
    let input: Vec<UserInput> = from_snapshot_json(&snapshot.input_json, "turn input")?;
    anyhow::ensure!(
        input.iter().all(|value| match value {
            UserInput::Text { .. } | UserInput::Mention { .. } => true,
            UserInput::Artifact {
                version_id: Some(version),
                ..
            } => !version.is_empty(),
            _ => false,
        }),
        "legacy mutable media has no immutable accepted identity"
    );
    restored_execution_capabilities(&snapshot.capabilities_json)?;
    let _: Vec<ResolvedArtifactInput> =
        from_snapshot_json(&snapshot.resolved_artifacts_json, "resolved artifacts")?;
    let _: HashMap<String, String> =
        from_snapshot_json(&snapshot.runtime_environment_json, "runtime environment")?;
    agent_skill_version_pins_from_snapshot(snapshot)?;
    if let Some(effort) = snapshot.reasoning_effort.as_deref() {
        reasoning_config_from_snapshot_effort(effort)?;
    }
    if snapshot.history_json.trim_start().starts_with('[') {
        let _: Vec<ChatMessage> =
            from_snapshot_json(&snapshot.history_json, "conversation history")?;
    } else {
        let _: pioneer_compaction::frozen::FrozenHistoryRef =
            from_snapshot_json(&snapshot.history_json, "history reference")?;
    }
    Ok(())
}

pub(crate) async fn execution_window_usage_snapshot(
    store: &CrudStore,
    turn_id: &str,
) -> Result<ExecutionWindowUsageSnapshot> {
    let usage = store.aggregate_turn_execution_window_usage(turn_id).await?;
    Ok(ExecutionWindowUsageSnapshot {
        total_windows: usage.latest_window_index.max(usage.total_windows),
        total_tool_calls: usage.total_tool_calls,
        total_wall_clock_ms: usage.total_wall_clock_ms,
        total_provider_tokens: usage.total_provider_tokens,
        provider_token_usage_unknown: usage.provider_token_usage_unknown,
        consecutive_no_progress_windows: usage.consecutive_no_progress_windows,
        consecutive_unconfirmed_progress_windows: usage.consecutive_unconfirmed_progress_windows,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredWorkspaceSkillPolicy {
    skill_id: SkillId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentSkillVersionPin {
    pub skill_id: SkillId,
    pub version_id: String,
    pub fingerprint: String,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn new_turn_runtime_snapshot(
    thread_id: &str,
    workspace_id: &str,
    turn_id: &str,
    mode: ThreadMode,
    hook_runtime_context: &AgentTurnHookRuntimeContext,
    model: &str,
    provider_name: &str,
    reasoning_effort: Option<&str>,
    workspace_skill_policies: &HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
    input: &[UserInput],
    capabilities: &[TurnCapability],
    resolved_artifacts: &[ResolvedArtifactInput],
    runtime_environment: &HashMap<String, String>,
    history: &[ChatMessage],
    agent_skill_overlay: &[AgentSkillRuntimeEntry],
    skill_catalog: &pioneer_skills::SkillCatalogSnapshot,
) -> Result<NewTurnRuntimeSnapshot> {
    let limit = pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES;
    let mut remaining = limit;
    for scalar in [
        thread_id,
        workspace_id,
        turn_id,
        model,
        provider_name,
        reasoning_effort.unwrap_or(""),
    ] {
        remaining = remaining
            .checked_sub(scalar.len())
            .context("runtime snapshot scalar byte limit exceeded")?;
    }
    anyhow::ensure!(
        resolved_artifacts.len() <= pioneer_protocol::TURN_EXECUTION_ATTACHMENT_REFERENCE_MAX_COUNT,
        "runtime snapshot artifact cardinality limit exceeded"
    );
    anyhow::ensure!(
        workspace_skill_policies.len() <= limit / (pioneer_protocol::SKILL_ID_LEN + 2),
        "runtime snapshot policy cardinality exceeds encoded byte limit"
    );
    let mut canonical_artifacts = Vec::with_capacity(resolved_artifacts.len());
    for resolved in resolved_artifacts {
        anyhow::ensure!(
            resolved
                .version_id
                .as_deref()
                .is_some_and(|v| !v.is_empty()),
            "artifact snapshot requires exact version"
        );
        anyhow::ensure!(
            resolved.artifact_id.len() <= remaining,
            "runtime snapshot artifact identity byte limit exceeded"
        );
        let attachment = &resolved.attachment;
        canonical_artifacts.push(CanonicalArtifactSnapshot {
            artifact_id: &resolved.artifact_id,
            version_id: resolved.version_id.as_deref(),
            content_type: resolved.content_type,
            attachment: CanonicalAttachmentSnapshot {
                mime_type: &attachment.mime_type,
                name: attachment.name.as_deref(),
                size_bytes: attachment.size_bytes,
                sha256: attachment.sha256.as_deref(),
                source: CanonicalArtifactSource {
                    source: "reference",
                    reference: &resolved.artifact_id,
                },
                artifact: attachment.artifact.as_ref(),
            },
        });
    }
    let agent_skill_versions_json = agent_skill_versions_json(agent_skill_overlay)?;
    remaining = remaining
        .checked_sub(agent_skill_versions_json.as_deref().unwrap_or("").len())
        .context("runtime snapshot aggregate byte limit exceeded")?;
    // Validate capability shape with the existing owner before bounded encoding.
    validate_execution_capabilities(capabilities)?;
    let capabilities_json =
        to_snapshot_json_bounded(capabilities, "turn capabilities", &mut remaining)?;
    validate_ordinary_skill_catalog(skill_catalog)?;
    let ordinary_skill_catalog_json = Some(to_snapshot_json_bounded(
        &StoredOrdinarySkillCatalog {
            schema_version: 1,
            catalog: skill_catalog,
        },
        "ordinary skill catalog",
        &mut remaining,
    )?);
    let now = chrono::Utc::now().fixed_offset();
    Ok(NewTurnRuntimeSnapshot {
        turn_id: turn_id.to_owned(),
        thread_id: thread_id.to_owned(),
        workspace_id: workspace_id.to_owned(),
        mode_json: to_snapshot_json_bounded(&mode, "thread mode", &mut remaining)?,
        model: model.to_owned(),
        provider_name: provider_name.to_owned(),
        reasoning_effort: reasoning_effort.map(str::to_owned),
        agent_skill_versions_json,
        ordinary_skill_catalog_json,
        provider_model_contract_json: None,
        initial_phase_json: None,
        hook_runtime_context_json: to_snapshot_json_bounded(
            hook_runtime_context,
            "hook runtime context",
            &mut remaining,
        )?,
        workspace_skill_policies_json: to_snapshot_json_bounded(
            &stored_workspace_skill_policies(workspace_skill_policies),
            "workspace skill policies",
            &mut remaining,
        )?,
        input_json: to_snapshot_json_bounded(input, "turn input", &mut remaining)?,
        capabilities_json,
        resolved_artifacts_json: to_snapshot_json_bounded(
            &canonical_artifacts,
            "resolved artifacts",
            &mut remaining,
        )?,
        runtime_environment_json: to_snapshot_json_bounded(
            runtime_environment,
            "runtime environment",
            &mut remaining,
        )?,
        history_json: to_snapshot_json_bounded(history, "conversation history", &mut remaining)?,
        created_at: now,
        updated_at: now,
    })
}

fn accepted_media_name(
    locator: &str,
    local: bool,
    kind: pioneer_provider::InputContentType,
) -> String {
    // The filename is semantic input, never a public diagnostic or source URL.
    let name = if local {
        std::path::Path::new(locator)
            .file_name()
            .and_then(|v| v.to_str())
            .map(str::to_owned)
    } else {
        url::Url::parse(locator).ok().and_then(|url| {
            url.path_segments()
                .and_then(|mut parts| parts.next_back())
                .filter(|v| !v.is_empty())
                .map(str::to_owned)
        })
    };
    name.filter(|name| name.len() <= 255 && !name.chars().any(char::is_control))
        .unwrap_or_else(|| {
            match kind {
                pioneer_provider::InputContentType::Image => "accepted-image",
                pioneer_provider::InputContentType::Audio => "accepted-audio",
                pioneer_provider::InputContentType::Video => "accepted-video",
                _ => "accepted-file",
            }
            .to_owned()
        })
}

pub(crate) async fn capture_native_media_inputs(
    artifacts: &pioneer_artifacts::ArtifactService,
    providers: &pioneer_provider::ProviderRegistry,
    workspace: &str,
    thread: &str,
    provider: &str,
    azure_connection: Option<&pioneer_provider::AzureResolvedConnection>,
    input: &[UserInput],
    security: &TurnExecutionSecuritySnapshot,
    actor: &pioneer_protocol::PersistedActorRef,
) -> Result<Vec<UserInput>> {
    use pioneer_protocol::{ArtifactCreatedByKind, ArtifactKind};
    use pioneer_provider::{InputContentType as Kind, MessageAttachment};
    let config = pioneer_provider::default_attachment_pipeline_config();
    let mut remaining = config.max_total_bytes_per_request;
    let context = pioneer_provider::ProviderAttemptContext::new(
        pioneer_provider::ProviderTimeoutPolicy::default(),
        false,
    )?;
    let mut result = Vec::with_capacity(input.len());
    let mut count = 0;
    for value in input {
        anyhow::ensure!(
            !context.cancellation.is_cancelled() && tokio::time::Instant::now() < context.deadline,
            "immutable media ingress deadline exceeded"
        );
        let (locator, kind, local) = match value {
            UserInput::LocalImage { path } => (path, Kind::Image, true),
            UserInput::LocalFile { path } => (path, Kind::File, true),
            UserInput::LocalAudio { path } => (path, Kind::Audio, true),
            UserInput::LocalVideo { path } => (path, Kind::Video, true),
            UserInput::Image { url } => (url, Kind::Image, false),
            UserInput::File { url } => (url, Kind::File, false),
            UserInput::Audio { url } => (url, Kind::Audio, false),
            UserInput::Video { url } => (url, Kind::Video, false),
            UserInput::Artifact {
                artifact_id,
                version_id,
            } => {
                count += 1;
                anyhow::ensure!(
                    count <= config.max_attachments_per_request,
                    "media input count limit exceeded"
                );
                let version = version_id
                    .as_deref()
                    .filter(|version| !version.is_empty())
                    .context("exact artifact input version required")?;
                let summary = artifacts
                    .get_exact_artifact_ref(workspace, artifact_id, version)
                    .await?;
                let size =
                    usize::try_from(summary.size_bytes.context("artifact size is unknown")?)?;
                anyhow::ensure!(
                    size <= config.max_bytes_per_attachment,
                    "media input byte limit exceeded"
                );
                remaining = remaining
                    .checked_sub(size)
                    .context("media aggregate limit exceeded")?;
                result.push(value.clone());
                continue;
            }
            _ => {
                result.push(value.clone());
                continue;
            }
        };
        count += 1;
        anyhow::ensure!(
            count <= config.max_attachments_per_request,
            "media input count limit exceeded"
        );
        let limit = remaining.min(config.max_bytes_per_attachment);
        let mime = pioneer_provider::infer_mime_from_reference(locator, kind);
        let bytes = if local {
            pioneer_tools::FilePolicyChecker::read_accepted_bytes(
                security.clone(),
                locator.into(),
                limit,
                context.clone(),
            )
            .await
            .map_err(|_| anyhow::anyhow!("authorized media file capture failed"))?
        } else {
            pioneer_tools::enforce_network_url(Some(security), locator, "native media ingress")
                .map_err(|_| anyhow::anyhow!("authorized media URL capture denied"))?;
            let authority = match azure_connection {
                Some(connection) => providers
                    .authority_fingerprint_for_workspace_with_frozen_azure(
                        workspace, provider, connection,
                    )?,
                None => providers.authority_fingerprint_for_workspace(workspace, provider)?,
            }
            .as_str()
            .to_owned();
            let attachment = MessageAttachment::from_url(locator, &mime);
            pioneer_provider::with_provider_attempt_context(
                pioneer_provider::ProviderAttemptContext {
                    cancellation: context.cancellation.child_token(),
                    ..context.clone()
                },
                pioneer_provider::attachments::read_accepted_source_bytes(
                    provider,
                    authority,
                    &attachment,
                    kind,
                    &config,
                    limit,
                ),
            )
            .await
            .map_err(|_| anyhow::anyhow!("authorized media URL capture failed"))?
        };
        remaining = remaining
            .checked_sub(bytes.len())
            .context("media aggregate limit exceeded")?;
        let summary = artifacts
            .ingest_bytes(pioneer_artifacts::IngestArtifactBytesRequest {
                workspace_id: workspace.to_owned(),
                primary_thread_id: Some(thread.to_owned()),
                bytes,
                display_name: accepted_media_name(locator, local, kind),
                kind: match kind {
                    Kind::Image => ArtifactKind::Image,
                    Kind::Audio => ArtifactKind::Audio,
                    Kind::Video => ArtifactKind::Video,
                    _ => ArtifactKind::File,
                },
                mime_type: Some(mime),
                created_by_kind: match actor {
                    pioneer_protocol::PersistedActorRef::Principal(_) => {
                        ArtifactCreatedByKind::User
                    }
                    pioneer_protocol::PersistedActorRef::AgentExecution(_) => {
                        ArtifactCreatedByKind::Agent
                    }
                    pioneer_protocol::PersistedActorRef::System => ArtifactCreatedByKind::System,
                },
                created_by_actor_id: match actor {
                    pioneer_protocol::PersistedActorRef::Principal(id) => Some(id.to_string()),
                    pioneer_protocol::PersistedActorRef::AgentExecution(id) => Some(id.to_string()),
                    pioneer_protocol::PersistedActorRef::System => None,
                },
                binding: None,
                metadata: Default::default(),
            })
            .await
            .map_err(|_| anyhow::anyhow!("immutable media ingress commit failed"))?;
        result.push(UserInput::Artifact {
            artifact_id: summary.artifact.artifact_id,
            version_id: summary.artifact.version_id,
        });
    }
    anyhow::ensure!(
        count <= config.max_attachments_per_request,
        "media input count limit exceeded"
    );
    anyhow::ensure!(
        !context.cancellation.is_cancelled() && tokio::time::Instant::now() < context.deadline,
        "immutable media ingress deadline exceeded"
    );
    Ok(result)
}

pub(crate) async fn materialize_owned_history(
    artifacts: &pioneer_artifacts::ArtifactService,
    workspace_id: &str,
    mut history: Vec<ChatMessage>,
) -> Result<Vec<ChatMessage>> {
    pioneer_observability::turn_startup::scope_current_stage(pioneer_observability::turn_startup::Stage::HistoryArtifacts, async {
            let config = pioneer_provider::default_attachment_pipeline_config();
            let mut remaining = config.max_total_bytes_per_request;
            let mut count = 0;
            for (message_index, message) in history.iter_mut().enumerate() {
                for (part_index, part) in message.content_parts.iter_mut().enumerate() {
                    let expected_kind = part.kind();
                    let attachment = match part {
                        pioneer_provider::MessageContentPart::Image { image } => image,
                        pioneer_provider::MessageContentPart::File { file } => file,
                        pioneer_provider::MessageContentPart::Audio { audio } => audio,
                        pioneer_provider::MessageContentPart::Video { video } => video,
                        pioneer_provider::MessageContentPart::Text { .. } => continue,
                    };
                    let Some(artifact) = attachment.artifact.as_ref() else {
                        continue;
                    };
                    anyhow::ensure!(
                        artifact.workspace_id.is_empty() || artifact.workspace_id == workspace_id,
                        "historical attachment {message_index}:{part_index} belongs to another workspace"
                    );
                    let version_id = artifact.artifact_version_id.as_deref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "historical attachment {message_index}:{part_index} has no accepted artifact version"
                        )
                    })?;
                    count += 1;
                    anyhow::ensure!(count <= config.max_attachments_per_request, "historical media count limit exceeded");
                    let resolved = artifacts
                        .resolve_owned_provider_attachment(
                            workspace_id,
                            artifact.artifact_id.as_str(),
                            version_id,
                            remaining.min(config.max_bytes_per_attachment),
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "failed to materialize accepted historical artifact `{}` version `{version_id}`",
                                artifact.artifact_id
                            )
                        })?;
                    anyhow::ensure!(
                        resolved.version_id.as_deref() == Some(version_id),
                        "historical artifact resolver changed the accepted version"
                    );
                    anyhow::ensure!(resolved.content_type == expected_kind,
                        "historical attachment kind changed after accepted capture");
                    remaining = remaining.checked_sub(usize::try_from(resolved.attachment.size_bytes.unwrap_or(u64::MAX))?)
                        .context("historical media aggregate byte limit exceeded")?;
                    anyhow::ensure!(attachment.sha256.as_ref().is_none_or(|hash| resolved.attachment.sha256.as_ref() == Some(hash)),
                        "historical accepted media hash changed");
                    anyhow::ensure!(attachment.size_bytes.is_none_or(|size| resolved.attachment.size_bytes == Some(size)),
                        "historical accepted media size changed");
                    anyhow::ensure!(attachment.mime_type == resolved.attachment.mime_type,
                        "historical accepted media MIME changed");
                    *attachment = resolved.attachment;
                }
            }
            Ok(history)
        })
        .await
}

pub(crate) async fn resolve_owned_artifact_inputs(
    artifacts: &pioneer_artifacts::ArtifactService,
    workspace_id: &str,
    input: &[UserInput],
) -> Result<Vec<ResolvedArtifactInput>> {
    let config = pioneer_provider::default_attachment_pipeline_config();
    let mut remaining = config.max_total_bytes_per_request;
    let mut resolved = Vec::new();
    for value in input {
        match value {
            UserInput::Artifact {
                artifact_id,
                version_id,
            } => {
                anyhow::ensure!(
                    resolved.len() < config.max_attachments_per_request,
                    "artifact input count exceeds limit"
                );
                let version = version_id
                    .as_deref()
                    .filter(|v| !v.is_empty())
                    .context("native artifact input requires exact accepted version")?;
                let owned = artifacts
                    .resolve_owned_provider_attachment(
                        workspace_id,
                        artifact_id,
                        version,
                        remaining.min(config.max_bytes_per_attachment),
                    )
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!(
                            "exact artifact input is unavailable or exceeds read limits"
                        )
                    })?;
                let size = usize::try_from(owned.attachment.size_bytes.unwrap_or(u64::MAX))?;
                remaining = remaining
                    .checked_sub(size)
                    .context("artifact input aggregate limit exceeded")?;
                resolved.push(ResolvedArtifactInput {
                    artifact_id: owned.artifact_id,
                    version_id: owned.version_id,
                    content_type: owned.content_type,
                    attachment: owned.attachment,
                });
            }
            UserInput::Image { .. }
            | UserInput::LocalImage { .. }
            | UserInput::File { .. }
            | UserInput::LocalFile { .. }
            | UserInput::Audio { .. }
            | UserInput::LocalAudio { .. }
            | UserInput::Video { .. }
            | UserInput::LocalVideo { .. } => {
                bail!(
                    "legacy mutable media has no accepted immutable artifact; explicit resubmission required"
                );
            }
            _ => {}
        }
    }
    Ok(resolved)
}

pub(crate) async fn restored_recovery_turn_request_from_snapshot(
    store: &CrudStore,
    artifacts: Option<&pioneer_artifacts::ArtifactService>,
    snapshot: &TurnRuntimeSnapshotRecord,
    permission_profile: TurnPermissionProfileSnapshot,
    execution_security_snapshot: TurnExecutionSecuritySnapshot,
    agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
) -> Result<RestoredRecoveryTurnRequest> {
    snapshot.validate_storage_size()?;
    let execution_owner_generation = store
        .get_turn_execution(&snapshot.turn_id)
        .await?
        .context("native dispatch execution owner unavailable")?
        .owner_generation;
    let azure_connection = if pioneer_provider::provider_definition(&snapshot.provider_name)
        .is_some_and(|definition| definition.name == "azure-openai")
    {
        let context = crate::authorization::ExecutionAuthorizationContext::load_for_turn(
            store,
            &snapshot.turn_id,
        )
        .await?;
        anyhow::ensure!(
            context.workspace_id() == snapshot.workspace_id,
            "accepted Azure authority workspace differs from native snapshot"
        );
        context.require_provider_binding(&snapshot.provider_name, &snapshot.model)?;
        Some(
            context
                .provider_azure_connection()
                .context("native Azure restore is missing its accepted connection")?
                .clone(),
        )
    } else {
        None
    };
    let (hook_runtime_context, history) =
        restored_conversation_scope_from_snapshot(store, snapshot).await?;
    let skill_catalog = restored_ordinary_skill_catalog(snapshot)?;
    let capabilities = restored_execution_capabilities(&snapshot.capabilities_json)?;
    for capability in &capabilities {
        let TurnCapabilityKind::Skill { skill_id, .. } = &capability.kind else {
            continue;
        };
        if agent_skill_overlay
            .iter()
            .any(|skill| &skill.skill_id == skill_id)
        {
            continue;
        }
        let skill = skill_catalog
            .skills
            .iter()
            .find(|skill| &skill.identity.skill_id == skill_id)
            .context("selected ordinary skill is absent from accepted snapshot")?;
        anyhow::ensure!(
            skill.is_available(),
            "accepted selected ordinary skill is unavailable"
        );
        if !matches!(
            skill.identity.source_kind,
            pioneer_skills::SkillSourceKind::System
        ) {
            let current = store
                .find_skill_installation(skill_id)
                .await?
                .context("selected ordinary skill was removed since admission")?;
            anyhow::ensure!(
                current.scope_key == snapshot.workspace_id,
                "selected ordinary skill scope was revoked"
            );
        }
        if let Some(policy) = store
            .find_workspace_skill_policy(&snapshot.workspace_id, skill_id)
            .await?
        {
            anyhow::ensure!(
                policy.enabled != Some(false),
                "selected ordinary skill use was disabled since admission"
            );
        }
    }
    let input: Vec<UserInput> = from_snapshot_json(&snapshot.input_json, "turn input")?;
    let (resolved_artifacts, history) = if let Some(artifacts) = artifacts {
        let fresh =
            resolve_owned_artifact_inputs(artifacts, &snapshot.workspace_id, &input).await?;
        let recorded: Vec<ResolvedArtifactInput> =
            from_snapshot_json(&snapshot.resolved_artifacts_json, "resolved artifacts")?;
        for expected in &recorded {
            let current = fresh
                .iter()
                .find(|current| {
                    current.artifact_id == expected.artifact_id
                        && current.version_id == expected.version_id
                })
                .context("snapshot artifact identity changed")?;
            anyhow::ensure!(
                current.content_type == expected.content_type
                    && current.attachment.mime_type == expected.attachment.mime_type
                    && expected
                        .attachment
                        .sha256
                        .as_ref()
                        .is_none_or(|hash| current.attachment.sha256.as_ref() == Some(hash))
                    && expected
                        .attachment
                        .size_bytes
                        .is_none_or(|size| current.attachment.size_bytes == Some(size)),
                "snapshot accepted media contract changed"
            );
        }
        (
            fresh,
            materialize_owned_history(artifacts, &snapshot.workspace_id, history).await?,
        )
    } else {
        anyhow::ensure!(
            !input.iter().any(|value| matches!(
                value,
                UserInput::Artifact { .. }
                    | UserInput::LocalImage { .. }
                    | UserInput::LocalFile { .. }
                    | UserInput::LocalAudio { .. }
                    | UserInput::LocalVideo { .. }
                    | UserInput::Image { .. }
                    | UserInput::File { .. }
                    | UserInput::Audio { .. }
                    | UserInput::Video { .. }
            )) && !history.iter().any(ChatMessage::has_attachments),
            "native artifact restore owner is unavailable"
        );
        (Vec::new(), history)
    };
    Ok(RestoredRecoveryTurnRequest {
        turn_id: snapshot.turn_id.clone(),
        execution_window_index: 1,
        mode: from_snapshot_json(&snapshot.mode_json, "thread mode")?,
        hook_runtime_context,
        model: snapshot.model.clone(),
        provider_name: snapshot.provider_name.clone(),
        azure_connection,
        provider_model_contract: Some(restored_provider_model_contract(snapshot)?),
        initial_preparation: Some(
            pioneer_agent::NativeInitialPreparation::restore(
                snapshot
                    .initial_phase_json
                    .as_deref()
                    .context("native initial preparation unavailable")?,
                &snapshot.workspace_id,
                &snapshot.thread_id,
                &snapshot.turn_id,
                snapshot
                    .remaining_storage_bytes()?
                    .checked_add(snapshot.initial_phase_json.as_ref().map_or(0, String::len))
                    .context("native initial phase byte budget invalid")?,
            )?
            .with_execution_owner_generation(execution_owner_generation),
        ),
        reasoning: snapshot
            .reasoning_effort
            .as_deref()
            .map(reasoning_config_from_snapshot_effort)
            .transpose()?,
        workspace_skill_policies: restore_workspace_skill_policies(
            &snapshot.workspace_skill_policies_json,
        )?,
        skill_catalog,
        agent_skill_overlay,
        input,
        capabilities,
        resolved_artifacts,
        runtime_environment: from_snapshot_json(
            &snapshot.runtime_environment_json,
            "runtime environment",
        )?,
        history,
        permission_profile,
        execution_security_snapshot: Some(execution_security_snapshot),
    })
}

pub(crate) async fn restored_conversation_scope_from_snapshot(
    store: &CrudStore,
    snapshot: &TurnRuntimeSnapshotRecord,
) -> Result<(AgentTurnHookRuntimeContext, Vec<ChatMessage>)> {
    let (context, projection) =
        restored_conversation_scope_projection_from_snapshot(store, snapshot).await?;
    Ok((context, projection.messages))
}

/// The caller has loaded this exact execution's durable runtime record. Its
/// published history is the accepted source grant, independent of the Task
/// input that was used to prepare an earlier execution.
pub(crate) async fn accepted_history_scopes_from_snapshot(
    store: &CrudStore,
    snapshot: &TurnRuntimeSnapshotRecord,
) -> Result<std::collections::BTreeSet<String>> {
    let context: AgentTurnHookRuntimeContext =
        from_snapshot_json(&snapshot.hook_runtime_context_json, "hook runtime context")?;
    let mut allowed = std::collections::BTreeSet::from([snapshot.thread_id.clone()]);
    allowed.extend(context.conversation_thread_id.iter().cloned());
    if snapshot.history_json.trim_start().starts_with('[') {
        return Ok(allowed);
    }
    let descriptor: pioneer_compaction::frozen::FrozenHistoryRef =
        from_snapshot_json(&snapshot.history_json, "frozen history reference")?;
    let _hold = store
        .compaction_acquire_frozen_history(&snapshot.workspace_id, &descriptor)
        .await?;
    let owner = store
        .compaction_frozen_history_owner(&snapshot.workspace_id, &descriptor)
        .await?
        .ok_or_else(|| anyhow::anyhow!("runtime history manifest is unavailable"))?;
    // Older accepted runtime records may still reference their parent-owned
    // manifest. Authenticate that destination from the hook or the exact
    // TaskRun relationship, without opening the Task's frozen input.
    let owner_matches = if owner == snapshot.thread_id
        || context.conversation_thread_id.as_deref() == Some(owner.as_str())
    {
        true
    } else {
        store
            .compaction_task_basis_thread(
                &snapshot.workspace_id,
                &snapshot.thread_id,
                &snapshot.turn_id,
            )
            .await?
            .as_deref()
            == Some(owner.as_str())
    };
    anyhow::ensure!(
        owner_matches,
        "runtime history manifest belongs to another destination"
    );
    allowed.extend(
        crate::compaction::frozen::accepted_history_scopes(
            store,
            &snapshot.workspace_id,
            &owner,
            &snapshot.history_json,
        )
        .await?,
    );
    Ok(allowed)
}

pub(crate) async fn restored_conversation_scope_projection_from_snapshot(
    store: &CrudStore,
    snapshot: &TurnRuntimeSnapshotRecord,
) -> Result<(
    AgentTurnHookRuntimeContext,
    crate::compaction::frozen::RestoredAcceptedHistory,
)> {
    snapshot.validate_storage_size()?;
    let context: AgentTurnHookRuntimeContext =
        from_snapshot_json(&snapshot.hook_runtime_context_json, "hook runtime context")?;
    let _frozen_hold = crate::compaction::frozen::acquire_history_json(
        store,
        &snapshot.workspace_id,
        &snapshot.history_json,
    )
    .await?;
    let allowed = accepted_history_scopes_from_snapshot(store, snapshot).await?;
    let history = crate::compaction::frozen::restore_accepted_history_for_execution_with_order(
        store,
        &snapshot.workspace_id,
        context.conversation_thread_id.as_deref(),
        &snapshot.thread_id,
        &allowed,
        &snapshot.history_json,
        true,
    )
    .await?;
    Ok((context, history))
}

/// Read compatibility is explicit: already accepted array snapshots keep their
/// exact old projection. Newly written manifests resolve canonical references.
/// Callers supply scopes from accepted execution metadata, never model input.
pub(crate) async fn restore_history_json(
    store: &CrudStore,
    workspace: &str,
    allowed: &std::collections::BTreeSet<String>,
    value: &str,
) -> Result<Vec<ChatMessage>> {
    if value.trim_start().starts_with('[') {
        return from_snapshot_json(value, "legacy conversation history");
    }
    let descriptor: pioneer_compaction::frozen::FrozenHistoryRef =
        from_snapshot_json(value, "frozen history reference")?;
    crate::compaction::frozen::restore(store, workspace, allowed, &descriptor).await
}

fn agent_skill_versions_json(entries: &[AgentSkillRuntimeEntry]) -> Result<Option<String>> {
    if entries.is_empty() {
        return Ok(None);
    }
    ensure_agent_skill_overlay_capacity(entries)
        .context("Agent skill overlay is invalid for the authoritative turn snapshot")?;
    let mut pins = entries
        .iter()
        .map(|entry| AgentSkillVersionPin {
            skill_id: entry.skill_id.clone(),
            version_id: entry.version_id.clone(),
            fingerprint: entry.fingerprint.clone(),
        })
        .collect::<Vec<_>>();
    pins.sort_by(|left, right| left.skill_id.cmp(&right.skill_id));
    validate_agent_skill_version_pins(pins.as_slice())?;
    let json = to_snapshot_json(&pins, "Agent skill versions")?;
    if json.len() > MAX_AGENT_SKILL_PINS_JSON_BYTES {
        bail!("Agent skill version snapshot exceeds its bounded JSON size");
    }
    Ok(Some(json))
}

pub(crate) fn agent_skill_version_pins_from_snapshot(
    snapshot: &TurnRuntimeSnapshotRecord,
) -> Result<Vec<AgentSkillVersionPin>> {
    let Some(json) = snapshot.agent_skill_versions_json.as_deref() else {
        return Ok(Vec::new());
    };
    if json.len() > MAX_AGENT_SKILL_PINS_JSON_BYTES {
        bail!("Agent skill version snapshot exceeds its bounded JSON size");
    }
    let pins: Vec<AgentSkillVersionPin> = from_snapshot_json(json, "Agent skill versions")?;
    validate_agent_skill_version_pins(pins.as_slice())?;
    Ok(pins)
}

pub(crate) async fn restore_agent_skill_overlay_from_snapshot(
    store: &pioneer_crud::CrudStore,
    workspace_id: &str,
    snapshot: &TurnRuntimeSnapshotRecord,
) -> Result<Vec<AgentSkillRuntimeEntry>> {
    let pins = agent_skill_version_pins_from_snapshot(snapshot)?;
    let mut entries = Vec::with_capacity(pins.len());
    for pin in pins {
        let version = store
            .get_agent_skill_version(workspace_id, pin.version_id.as_str())
            .await
            .with_context(|| {
                format!(
                    "failed to load pinned Agent skill version `{}`",
                    pin.version_id
                )
            })?
            .with_context(|| {
                format!(
                    "pinned Agent skill version `{}` is missing from workspace `{workspace_id}`",
                    pin.version_id
                )
            })?;
        if version.skill_id != pin.skill_id
            || version.version.id != pin.version_id
            || version.version.fingerprint != pin.fingerprint
        {
            bail!(
                "pinned Agent skill version `{}` does not match its authoritative snapshot",
                pin.version_id
            );
        }
        entries.push(crate::self_improvement::overlay::agent_skill_runtime_entry(
            version,
        ));
    }
    ensure_agent_skill_overlay_capacity(entries.as_slice())
        .context("restored Agent skill overlay violates production runtime capacity")?;
    Ok(entries)
}

fn validate_agent_skill_version_pins(pins: &[AgentSkillVersionPin]) -> Result<()> {
    if pins.len() > MAX_ACTIVE_AGENT_SKILLS {
        bail!("Agent skill version snapshot exceeds the active Agent skill limit");
    }
    let mut skill_ids = HashSet::with_capacity(pins.len());
    let mut version_ids = HashSet::with_capacity(pins.len());
    for pin in pins {
        if pin.version_id.len() != pioneer_protocol::SKILL_ID_LEN
            || !pin
                .version_id
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        {
            bail!("Agent skill snapshot contains an invalid version ID");
        }
        if pin.fingerprint.is_empty()
            || pin.fingerprint != pin.fingerprint.trim()
            || pin.fingerprint.chars().count() > MAX_AGENT_SKILL_FINGERPRINT_CHARS
        {
            bail!("Agent skill snapshot contains an invalid fingerprint");
        }
        if !skill_ids.insert(pin.skill_id.clone()) || !version_ids.insert(pin.version_id.clone()) {
            bail!("Agent skill snapshot contains duplicate identities");
        }
    }
    Ok(())
}

#[cfg(test)]
fn execution_capabilities_json(capabilities: &[TurnCapability]) -> Result<String> {
    validate_execution_capabilities(capabilities)?;
    to_snapshot_json(capabilities, "turn capabilities")
}

pub(crate) fn restored_execution_capabilities(value: &str) -> Result<Vec<TurnCapability>> {
    let capabilities: Vec<TurnCapability> = from_snapshot_json(value, "turn capabilities")?;
    validate_execution_capabilities(&capabilities)?;
    Ok(capabilities)
}

fn validate_execution_capabilities(capabilities: &[TurnCapability]) -> Result<()> {
    anyhow::ensure!(
        capabilities.len() <= pioneer_protocol::TURN_EXECUTION_CAPABILITY_MAX_COUNT,
        "runtime snapshot capability cardinality limit exceeded"
    );
    let mut seen_skill_ids = HashSet::new();
    for capability in capabilities {
        match &capability.kind {
            pioneer_protocol::TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            } => {
                if capability.id != pioneer_protocol::skill_capability_key(skill_id) {
                    bail!(
                        "runtime skill capability `{}` does not match `{skill_id}`",
                        capability.id
                    );
                }
                if !seen_skill_ids.insert(skill_id.clone()) {
                    bail!("runtime skill capability `{skill_id}` is duplicated");
                }
            }
            pioneer_protocol::TurnCapabilityKind::Skill {
                pack_id: Some(_), ..
            }
            | pioneer_protocol::TurnCapabilityKind::SkillPack { .. } => {
                bail!("skill pack metadata cannot enter the runtime snapshot");
            }
            pioneer_protocol::TurnCapabilityKind::McpServer { .. }
            | pioneer_protocol::TurnCapabilityKind::McpTool { .. } => {}
        }
    }
    Ok(())
}

fn reasoning_config_from_snapshot_effort(effort: &str) -> Result<ReasoningConfig> {
    ReasoningEffort::from_str(effort)
        .map(ReasoningConfig::effort)
        .with_context(|| format!("unsupported reasoning effort `{effort}` in runtime snapshot"))
}

fn stored_workspace_skill_policies(
    policies: &HashMap<SkillPolicyKey, WorkspaceSkillPolicy>,
) -> Vec<StoredWorkspaceSkillPolicy> {
    let mut stored = policies
        .iter()
        .map(|(key, policy)| StoredWorkspaceSkillPolicy {
            skill_id: key.skill_id.clone(),
            enabled: policy.enabled,
            allow_implicit_invocation: policy.allow_implicit_invocation,
        })
        .collect::<Vec<_>>();
    stored.sort_by(|left, right| left.skill_id.cmp(&right.skill_id));
    stored
}

fn restore_workspace_skill_policies(
    value: &str,
) -> Result<HashMap<SkillPolicyKey, WorkspaceSkillPolicy>> {
    let stored: Vec<StoredWorkspaceSkillPolicy> =
        from_snapshot_json(value, "workspace skill policies")?;
    Ok(stored
        .into_iter()
        .map(|policy| {
            (
                SkillPolicyKey::new(policy.skill_id),
                WorkspaceSkillPolicy {
                    enabled: policy.enabled,
                    allow_implicit_invocation: policy.allow_implicit_invocation,
                },
            )
        })
        .collect())
}

pub(crate) fn to_snapshot_json<T: Serialize + ?Sized>(value: &T, label: &str) -> Result<String> {
    let mut remaining = pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES;
    to_snapshot_json_bounded(value, label, &mut remaining)
}

fn to_snapshot_json_bounded<T: Serialize + ?Sized>(
    value: &T,
    label: &str,
    remaining: &mut usize,
) -> Result<String> {
    struct Writer {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other(
                    "runtime snapshot encoded byte limit exceeded",
                ));
            }
            let next_len = self.bytes.len() + bytes.len();
            if next_len > self.bytes.capacity() {
                let capacity = next_len
                    .max(self.bytes.capacity().saturating_mul(2))
                    .min(self.limit);
                self.bytes.reserve_exact(capacity - self.bytes.len());
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        limit: *remaining,
    };
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| anyhow::anyhow!("failed to encode bounded {label} snapshot"))?;
    *remaining -= writer.bytes.len();
    String::from_utf8(writer.bytes).map_err(|_| anyhow::anyhow!("invalid encoded {label} snapshot"))
}

pub(crate) fn from_snapshot_json<T: DeserializeOwned>(value: &str, label: &str) -> Result<T> {
    anyhow::ensure!(
        value.len() <= pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES,
        "{label} snapshot encoded byte limit exceeded"
    );
    serde_json::from_str(value)
        .map_err(|_| anyhow::anyhow!("failed to decode required {label} snapshot"))
}

#[cfg(test)]
mod tests {
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::TurnRuntimeSnapshotRecord;
    use pioneer_protocol::{
        SkillId, SkillPackId, TurnCapability, TurnCapabilityKind, skill_capability_key,
        skill_pack_capability_key,
    };
    use pioneer_skills::AgentSkillRuntimeEntry;
    use sea_orm::{ConnectionTrait, Database};

    use super::{
        agent_skill_version_pins_from_snapshot, agent_skill_versions_json,
        execution_capabilities_json, restore_agent_skill_overlay_from_snapshot,
        restored_execution_capabilities,
    };

    const WORKSPACE: &str = "ws_pinned_agent_skill";
    const SKILL_ID: &str = "AAAAAAAAAAAAAAAAAAAAA";
    const VERSION_ONE: &str = "111111111111111111111";
    const VERSION_TWO: &str = "222222222222222222222";

    pub(crate) fn model_contract_json(provider: &str, model: &str) -> String {
        struct Selection<'a>(&'a str);
        #[async_trait::async_trait]
        impl pioneer_provider::Provider for Selection<'_> {
            fn name(&self) -> &str {
                self.0
            }
            async fn chat(
                &self,
                _: pioneer_provider::ChatRequest,
            ) -> anyhow::Result<pioneer_provider::ChatResponse> {
                anyhow::bail!("restore-only fixture does not dispatch")
            }
            async fn stream_chat(
                &self,
                _: pioneer_provider::ChatRequest,
            ) -> anyhow::Result<
                futures_util::stream::BoxStream<
                    'static,
                    anyhow::Result<pioneer_provider::StreamChunk>,
                >,
            > {
                anyhow::bail!("restore-only fixture does not dispatch")
            }
        }
        pioneer_provider::Provider::capture_model_contract(&Selection(provider), model)
            .unwrap()
            .to_json()
            .unwrap()
    }

    pub(super) fn runtime_snapshot(
        agent_skill_versions_json: Option<String>,
    ) -> TurnRuntimeSnapshotRecord {
        let now = chrono::Utc::now().fixed_offset();
        TurnRuntimeSnapshotRecord {
            turn_id: "turn_pinned_snapshot".to_owned(),
            thread_id: "thread_pinned_snapshot".to_owned(),
            workspace_id: WORKSPACE.to_owned(),
            mode_json: r#""Agent""#.to_owned(),
            model: "model".to_owned(),
            provider_name: "provider".to_owned(),
            reasoning_effort: None,
            agent_skill_versions_json,
            provider_model_contract_json: Some(model_contract_json("provider", "model")),
            initial_phase_json: Some(
                pioneer_agent::NativeInitialPreparation::new_admission_json(
                    WORKSPACE,
                    "thread_pinned_snapshot",
                    "turn_pinned_snapshot",
                    pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES,
                )
                .unwrap(),
            ),
            ordinary_skill_catalog_json: Some(
                r#"{"schema_version":1,"catalog":{"version":0,"generated_at_unix":0,"skills":[]}}"#
                    .into(),
            ),
            hook_runtime_context_json: serde_json::to_string(
                &pioneer_agent::AgentTurnHookRuntimeContext::default(),
            )
            .unwrap(),
            workspace_skill_policies_json: "[]".to_owned(),
            input_json: "[]".to_owned(),
            capabilities_json: "[]".to_owned(),
            resolved_artifacts_json: "[]".to_owned(),
            runtime_environment_json: "{}".to_owned(),
            history_json: "[]".to_owned(),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn resume_compatibility_rejects_malformed_and_unknown_required_state() {
        let mut snapshot = runtime_snapshot(None);
        snapshot.mode_json = serde_json::to_string(&pioneer_protocol::ThreadMode::Agent).unwrap();
        assert!(super::validate_runtime_snapshot_for_resume(&snapshot).is_ok());
        let original = snapshot.clone();
        snapshot.hook_runtime_context_json = "{}".into();
        assert!(super::validate_runtime_snapshot_for_resume(&snapshot).is_err());
        snapshot = original.clone();
        snapshot.provider_model_contract_json = None;
        assert!(super::validate_runtime_snapshot_for_resume(&snapshot).is_err());
        snapshot = original.clone();
        let mut profile: serde_json::Value =
            serde_json::from_str(snapshot.provider_model_contract_json.as_deref().unwrap())
                .unwrap();
        profile["schema_version"] = 2.into();
        snapshot.provider_model_contract_json = Some(profile.to_string());
        assert!(super::validate_runtime_snapshot_for_resume(&snapshot).is_err());
        snapshot = original.clone();
        snapshot.input_json = "{\"incomplete\":true}".into();
        assert!(super::validate_runtime_snapshot_for_resume(&snapshot).is_err());
        snapshot = original.clone();
        snapshot.history_json = "{\"schema_version\":999}".into();
        assert!(super::validate_runtime_snapshot_for_resume(&snapshot).is_err());
        snapshot = original;
        snapshot.agent_skill_versions_json = Some("[{\"version_id\":\"missing\"}]".into());
        assert!(super::validate_runtime_snapshot_for_resume(&snapshot).is_err());
    }

    fn pin_json(version_id: &str, fingerprint: &str) -> String {
        serde_json::json!([{
            "skill_id": SKILL_ID,
            "version_id": version_id,
            "fingerprint": fingerprint,
        }])
        .to_string()
    }

    fn skill(seed: char) -> TurnCapability {
        let skill_id = SkillId::new(seed.to_string().repeat(21)).expect("skill id");
        TurnCapability {
            id: skill_capability_key(&skill_id),
            kind: TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            },
            label: Some(seed.to_string()),
        }
    }

    #[test]
    fn execution_snapshot_round_trips_only_flat_unique_skills() {
        let capabilities = vec![skill('A'), skill('B')];
        let json = execution_capabilities_json(&capabilities).expect("flattened snapshot");
        assert!(!json.contains("packId"));
        assert!(!json.contains("skillPack"));
        assert_eq!(
            restored_execution_capabilities(json.as_str()).expect("restored snapshot"),
            capabilities
        );
    }

    #[test]
    fn execution_snapshot_rejects_pack_metadata_and_duplicate_skills() {
        let pack_id = SkillPackId::new("P".repeat(21)).expect("pack id");
        let skill_id = SkillId::new("S".repeat(21)).expect("skill id");
        let packed_child = TurnCapability {
            id: skill_capability_key(&skill_id),
            kind: TurnCapabilityKind::Skill {
                skill_id,
                pack_id: Some(pack_id.clone()),
            },
            label: None,
        };
        let full_pack = TurnCapability {
            id: skill_pack_capability_key(&pack_id),
            kind: TurnCapabilityKind::SkillPack { pack_id },
            label: None,
        };
        let packed_json = serde_json::to_string(&[packed_child.clone()]).expect("packed JSON");
        let full_pack_json = serde_json::to_string(&[full_pack.clone()]).expect("pack JSON");
        assert!(execution_capabilities_json(&[packed_child]).is_err());
        assert!(execution_capabilities_json(&[full_pack]).is_err());
        assert!(restored_execution_capabilities(packed_json.as_str()).is_err());
        assert!(restored_execution_capabilities(full_pack_json.as_str()).is_err());

        let duplicate = skill('D');
        assert!(execution_capabilities_json(&[duplicate.clone(), duplicate]).is_err());
    }

    #[test]
    fn agent_skill_pins_are_bounded_canonical_and_never_copy_skill_content() {
        let entry = AgentSkillRuntimeEntry {
            skill_id: SkillId::new(SKILL_ID).expect("valid skill ID"),
            slug: "stable-procedure".to_owned(),
            version_id: VERSION_ONE.to_owned(),
            version_number: 1,
            display_name: "Stable procedure".to_owned(),
            runtime_description: "Use for stable procedures.".to_owned(),
            body: "SECRET BODY MUST NOT ENTER THE SNAPSHOT".to_owned(),
            fingerprint: "fingerprint-one".to_owned(),
        };
        let json = agent_skill_versions_json(std::slice::from_ref(&entry))
            .expect("valid pins")
            .expect("nonempty overlay must create pins");
        assert!(!json.contains("SECRET BODY"));
        assert!(!json.contains("stable-procedure"));
        let pins = agent_skill_version_pins_from_snapshot(&runtime_snapshot(Some(json)))
            .expect("pins must round-trip");
        assert_eq!(pins.len(), 1);
        assert_eq!(pins[0].skill_id, entry.skill_id);
        assert_eq!(pins[0].version_id, VERSION_ONE);
        assert_eq!(pins[0].fingerprint, "fingerprint-one");

        let mut corrupt = entry;
        corrupt.version_id = "not-a-version-id".to_owned();
        assert!(agent_skill_versions_json(&[corrupt]).is_err());
        assert!(
            agent_skill_version_pins_from_snapshot(&runtime_snapshot(Some(
                r#"[{"skill_id":"AAAAAAAAAAAAAAAAAAAAA","version_id":"111111111111111111111","fingerprint":"fingerprint-one","body":"forbidden"}]"#
                    .to_owned(),
            )))
            .is_err()
        );
    }

    #[tokio::test]
    async fn recovery_loads_only_exact_pinned_versions_and_fails_closed_on_drift() {
        let database = Database::connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite must open");
        Migrator::up(&database, None)
            .await
            .expect("migrations must apply");
        database
            .execute_unprepared(&format!(
                "INSERT INTO workspace (id, name, is_active, is_current) VALUES \
                    ('{WORKSPACE}', 'Pinned Agent skill', 1, 1); \
                 INSERT INTO agent_skill (id, workspace_id, slug) VALUES \
                    ('{SKILL_ID}', '{WORKSPACE}', 'stable-procedure'); \
                 INSERT INTO agent_skill_version (
                    id, skill_id, version_number, source_run_id, parent_version_id,
                    candidate_key, display_name, skill_markdown, instruction_body,
                    when_to_use, when_not_to_use, fingerprint, source_turn_ids_json
                 ) VALUES
                    ('{VERSION_ONE}', '{SKILL_ID}', 1, NULL, NULL, 'candidate-one',
                     'Stable procedure v1', '# v1', 'Exact immutable body v1.',
                     'Use v1', 'Do not use v1', 'fingerprint-one', '[]'),
                    ('{VERSION_TWO}', '{SKILL_ID}', 2, NULL, '{VERSION_ONE}', 'candidate-two',
                     'Stable procedure v2', '# v2', 'Exact immutable body v2.',
                     'Use v2', 'Do not use v2', 'fingerprint-two', '[]'); \
                 UPDATE agent_skill SET active_version_id = '{VERSION_TWO}'
                 WHERE id = '{SKILL_ID}';"
            ))
            .await
            .expect("pinned version fixtures must insert");
        let store = pioneer_crud::CrudStore::new(database.clone());

        assert!(
            restore_agent_skill_overlay_from_snapshot(&store, WORKSPACE, &runtime_snapshot(None),)
                .await
                .expect("an empty pin list must stay empty")
                .is_empty(),
            "a skill activated after the turn snapshot must not appear in recovery"
        );

        let pinned_v1 = runtime_snapshot(Some(pin_json(VERSION_ONE, "fingerprint-one")));
        let restored_v1 = restore_agent_skill_overlay_from_snapshot(&store, WORKSPACE, &pinned_v1)
            .await
            .expect("an update after turn start must not replace the pinned version");
        assert_eq!(restored_v1[0].version_id, VERSION_ONE);
        assert_eq!(restored_v1[0].body, "Exact immutable body v1.");

        database
            .execute_unprepared(&format!(
                "UPDATE agent_skill SET active_version_id = '{VERSION_ONE}' \
                 WHERE id = '{SKILL_ID}'"
            ))
            .await
            .expect("rollback pointer fixture must apply");
        let pinned_v2 = runtime_snapshot(Some(pin_json(VERSION_TWO, "fingerprint-two")));
        let restored_v2 = restore_agent_skill_overlay_from_snapshot(&store, WORKSPACE, &pinned_v2)
            .await
            .expect("rollback after turn start must not replace the pinned version");
        assert_eq!(restored_v2[0].version_id, VERSION_TWO);
        assert_eq!(restored_v2[0].body, "Exact immutable body v2.");

        assert!(
            restore_agent_skill_overlay_from_snapshot(
                &store,
                WORKSPACE,
                &runtime_snapshot(Some(pin_json(VERSION_TWO, "wrong-fingerprint"))),
            )
            .await
            .is_err(),
            "fingerprint mismatch must fail recovery closed"
        );
        assert!(
            restore_agent_skill_overlay_from_snapshot(
                &store,
                WORKSPACE,
                &runtime_snapshot(Some(pin_json(
                    "999999999999999999999",
                    "fingerprint-missing",
                ))),
            )
            .await
            .is_err(),
            "missing pinned version must fail recovery closed"
        );
    }
}

#[cfg(test)]
#[path = "turn_runtime_snapshot_r04_tests.rs"]
mod r04_media_tests;

#[cfg(test)]
mod r05_snapshot_bounds_tests {
    use super::*;
    #[test]
    fn snapshot_writer_counts_actual_unicode_escaping_and_shared_remaining_bytes() {
        let mut remaining = 4;
        assert_eq!(
            to_snapshot_json_bounded("é", "fixture", &mut remaining).unwrap(),
            "\"é\""
        );
        assert_eq!(remaining, 0);
        let mut remaining = 7;
        assert!(to_snapshot_json_bounded("\0", "fixture", &mut remaining).is_err());
        assert_eq!(remaining, 7);
        assert!(to_snapshot_json_bounded(&"x".repeat(100_000), "fixture", &mut remaining).is_err());
        let mut remaining = 7;
        to_snapshot_json_bounded("é", "first", &mut remaining).unwrap();
        assert!(to_snapshot_json_bounded("é", "second", &mut remaining).is_err());
        assert_eq!(remaining, 3);
    }
    #[test]
    fn snapshot_projection_never_copies_or_encodes_large_owned_media_body() {
        let limit = pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES;
        let resolved = ResolvedArtifactInput {
            artifact_id: "artifact".into(),
            version_id: Some("version".into()),
            content_type: pioneer_provider::InputContentType::Image,
            attachment: pioneer_provider::MessageAttachment {
                mime_type: "image/png".into(),
                name: Some("image.png".into()),
                size_bytes: Some((limit * 4) as u64),
                sha256: Some("a".repeat(64)),
                source: pioneer_provider::AttachmentDataSource::Bytes {
                    base64_data: "A".repeat(limit * 4),
                },
                artifact: Some(pioneer_provider::AttachmentArtifactContext {
                    workspace_id: "ws".into(),
                    artifact_id: "artifact".into(),
                    artifact_version_id: Some("version".into()),
                }),
            },
        };
        let make = |environment: &HashMap<String, String>| {
            new_turn_runtime_snapshot(
                "thread",
                "ws",
                "turn",
                ThreadMode::Agent,
                &AgentTurnHookRuntimeContext::default(),
                "model",
                "fixture",
                None,
                &HashMap::new(),
                &[],
                &[],
                std::slice::from_ref(&resolved),
                environment,
                &[],
                &[],
                &pioneer_skills::SkillCatalogSnapshot {
                    version: 0,
                    generated_at_unix: 0,
                    skills: Vec::new(),
                },
            )
        };
        let snapshot = make(&HashMap::new()).unwrap();
        assert!(snapshot.resolved_artifacts_json.len() < 1024);
        assert!(!snapshot.resolved_artifacts_json.contains("base64_data"));
        let restored: Vec<ResolvedArtifactInput> =
            serde_json::from_str(&snapshot.resolved_artifacts_json).unwrap();
        assert_eq!(
            restored[0].attachment.source,
            pioneer_provider::AttachmentDataSource::Reference {
                reference: "pioneer-artifact:artifact".into()
            }
        );
        assert_eq!(restored[0].attachment.sha256, resolved.attachment.sha256);
        let environment = HashMap::from([("key".into(), "x".repeat(limit))]);
        assert!(make(&environment).is_err());
    }
    #[test]
    fn oversized_legacy_section_fails_without_echoing_stored_content() {
        let json = format!(
            "\"SECRET{}\"",
            "x".repeat(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES)
        );
        let error = from_snapshot_json::<String>(&json, "legacy fixture").unwrap_err();
        assert!(!format!("{error:#?}").contains("SECRET"));
        let error = from_snapshot_json::<Vec<String>>("\"SECRET\"", "legacy fixture").unwrap_err();
        assert!(!format!("{error:#?}").contains("SECRET"));
    }
}

#[cfg(test)]
mod r05_ordinary_catalog_tests {
    use super::*;
    fn catalog() -> pioneer_skills::SkillCatalogSnapshot {
        use pioneer_skills::*;
        let skill = compile_skill_definition(compile::CompileSkillInput {
            skill_id: pioneer_protocol::SkillId::new("SSSSSSSSSSSSSSSSSSSSS").unwrap(),
            owner: Some("fixture".into()),
            slug: "writer".into(),
            name: "Writer".into(),
            display_name: "Writer".into(),
            description: "accepted description".into(),
            body: "accepted body".into(),
            source_kind: SkillSourceKind::User,
            source_root: "/old/root".into(),
            skill_dir: "/old/root/writer".into(),
            skill_file: "/old/root/writer/SKILL.md".into(),
            version_hint: Some("1".into()),
            fingerprint: "accepted-fingerprint".into(),
            user_invocable: true,
            disable_model_invocation: false,
            paths: Vec::new(),
            allowed_tools: Vec::new(),
            runtime_tools: Vec::new(),
            trust_level: SkillTrustLevel::Verified,
            dependencies: SkillDependencies::default(),
            license: None,
            compatibility: None,
            metadata_raw: serde_json::json!({}),
            conformance: validation::SkillConformanceReport::default(),
        });
        SkillCatalogSnapshot {
            version: 41,
            generated_at_unix: 1700000000,
            skills: vec![skill],
        }
    }
    #[test]
    fn ordinary_catalog_restores_exact_accepted_body_identity_and_revision() {
        let mut admitted = catalog();
        let mut snapshot = tests::runtime_snapshot(None);
        snapshot.ordinary_skill_catalog_json = Some(
            to_snapshot_json(
                &StoredOrdinarySkillCatalog {
                    schema_version: 1,
                    catalog: &admitted,
                },
                "ordinary skill catalog",
            )
            .unwrap(),
        );
        let expected = serde_json::to_value(&admitted).unwrap();
        admitted.version = 42;
        admitted.skills[0].instructions.body = "changed body".into();
        admitted.skills[0].identity.fingerprint = "changed-fingerprint".into();
        admitted.skills[0].identity.skill_dir = "/changed/root/writer".into();
        let restored = restored_ordinary_skill_catalog(&snapshot).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), expected);
        admitted.skills.clear();
        assert_eq!(
            restored_ordinary_skill_catalog(&snapshot).unwrap().skills[0]
                .instructions
                .body,
            "accepted body"
        );
    }
    #[test]
    fn ordinary_catalog_missing_unknown_duplicate_and_oversized_are_explicit_errors() {
        let mut snapshot = tests::runtime_snapshot(None);
        snapshot.ordinary_skill_catalog_json = None;
        assert!(restored_ordinary_skill_catalog(&snapshot).is_err());
        let mut admitted = catalog();
        snapshot.ordinary_skill_catalog_json = Some(
            to_snapshot_json(
                &StoredOrdinarySkillCatalog {
                    schema_version: 99,
                    catalog: &admitted,
                },
                "ordinary skill catalog",
            )
            .unwrap(),
        );
        assert!(restored_ordinary_skill_catalog(&snapshot).is_err());
        admitted.skills.push(admitted.skills[0].clone());
        assert!(validate_ordinary_skill_catalog(&admitted).is_err());
        admitted.skills.pop();
        admitted.skills[0].instructions.body =
            "x".repeat(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES);
        let mut remaining = pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES;
        assert!(
            to_snapshot_json_bounded(
                &StoredOrdinarySkillCatalog {
                    schema_version: 1,
                    catalog: &admitted
                },
                "ordinary skill catalog",
                &mut remaining
            )
            .is_err()
        );
    }
}
