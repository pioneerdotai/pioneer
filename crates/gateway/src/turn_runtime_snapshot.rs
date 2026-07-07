use anyhow::{Context, Result};
use pioneer_agent::{
    AgentTurnHookRuntimeContext, ResolvedArtifactInput, RestoredRecoveryTurnRequest,
    WorkspaceSkillPolicy,
};
use pioneer_crud::{NewTurnRuntimeSnapshot, TurnRuntimeSnapshotRecord};
use pioneer_protocol::ReasoningEffort;
use pioneer_protocol::{
    ThreadMode, TurnCapability, TurnExecutionSecuritySnapshot, TurnPermissionProfileSnapshot,
    UserInput,
};
use pioneer_provider::{ChatMessage, ReasoningConfig};
use pioneer_skills::SkillPolicyKey;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredWorkspaceSkillPolicy {
    slug: String,
    source_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    allow_implicit_invocation: Option<bool>,
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
) -> Result<NewTurnRuntimeSnapshot> {
    let now = chrono::Utc::now().fixed_offset();
    Ok(NewTurnRuntimeSnapshot {
        turn_id: turn_id.to_owned(),
        thread_id: thread_id.to_owned(),
        workspace_id: workspace_id.to_owned(),
        mode_json: to_snapshot_json(&mode, "thread mode")?,
        model: model.to_owned(),
        provider_name: provider_name.to_owned(),
        reasoning_effort: reasoning_effort.map(str::to_owned),
        hook_runtime_context_json: to_snapshot_json(hook_runtime_context, "hook runtime context")?,
        workspace_skill_policies_json: to_snapshot_json(
            &stored_workspace_skill_policies(workspace_skill_policies),
            "workspace skill policies",
        )?,
        input_json: to_snapshot_json(input, "turn input")?,
        capabilities_json: to_snapshot_json(capabilities, "turn capabilities")?,
        resolved_artifacts_json: to_snapshot_json(resolved_artifacts, "resolved artifacts")?,
        runtime_environment_json: to_snapshot_json(runtime_environment, "runtime environment")?,
        history_json: to_snapshot_json(history, "conversation history")?,
        created_at: now,
        updated_at: now,
    })
}

pub(crate) fn restored_recovery_turn_request_from_snapshot(
    snapshot: &TurnRuntimeSnapshotRecord,
    permission_profile: TurnPermissionProfileSnapshot,
    execution_security_snapshot: TurnExecutionSecuritySnapshot,
) -> Result<RestoredRecoveryTurnRequest> {
    Ok(RestoredRecoveryTurnRequest {
        turn_id: snapshot.turn_id.clone(),
        execution_window_index: 1,
        mode: from_snapshot_json(&snapshot.mode_json, "thread mode")?,
        hook_runtime_context: from_snapshot_json(
            &snapshot.hook_runtime_context_json,
            "hook runtime context",
        )?,
        model: snapshot.model.clone(),
        provider_name: snapshot.provider_name.clone(),
        reasoning: snapshot
            .reasoning_effort
            .as_deref()
            .map(reasoning_config_from_snapshot_effort)
            .transpose()?,
        workspace_skill_policies: restore_workspace_skill_policies(
            &snapshot.workspace_skill_policies_json,
        )?,
        input: from_snapshot_json(&snapshot.input_json, "turn input")?,
        capabilities: from_snapshot_json(&snapshot.capabilities_json, "turn capabilities")?,
        resolved_artifacts: from_snapshot_json(
            &snapshot.resolved_artifacts_json,
            "resolved artifacts",
        )?,
        runtime_environment: from_snapshot_json(
            &snapshot.runtime_environment_json,
            "runtime environment",
        )?,
        history: from_snapshot_json(&snapshot.history_json, "conversation history")?,
        permission_profile,
        execution_security_snapshot: Some(execution_security_snapshot),
    })
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
            slug: key.slug.clone(),
            source_kind: key.source_kind.clone(),
            enabled: policy.enabled,
            allow_implicit_invocation: policy.allow_implicit_invocation,
        })
        .collect::<Vec<_>>();
    stored.sort_by(|left, right| {
        left.source_kind
            .cmp(&right.source_kind)
            .then_with(|| left.slug.cmp(&right.slug))
    });
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
                SkillPolicyKey::new(policy.slug, policy.source_kind),
                WorkspaceSkillPolicy {
                    enabled: policy.enabled,
                    allow_implicit_invocation: policy.allow_implicit_invocation,
                },
            )
        })
        .collect())
}

fn to_snapshot_json<T: Serialize + ?Sized>(value: &T, label: &str) -> Result<String> {
    serde_json::to_string(value).with_context(|| format!("failed to serialize {label} snapshot"))
}

fn from_snapshot_json<T: DeserializeOwned>(value: &str, label: &str) -> Result<T> {
    serde_json::from_str(value).with_context(|| format!("failed to deserialize {label} snapshot"))
}
