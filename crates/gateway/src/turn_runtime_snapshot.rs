use anyhow::{Context, Result, bail};
use pioneer_agent::{
    AgentTurnHookRuntimeContext, ResolvedArtifactInput, RestoredRecoveryTurnRequest,
    WorkspaceSkillPolicy,
};
use pioneer_crud::{NewTurnRuntimeSnapshot, TurnRuntimeSnapshotRecord};
use pioneer_protocol::ReasoningEffort;
use pioneer_protocol::{
    SkillId, ThreadMode, TurnCapability, TurnExecutionSecuritySnapshot,
    TurnPermissionProfileSnapshot, UserInput,
};
use pioneer_provider::{ChatMessage, ReasoningConfig};
use pioneer_skills::SkillPolicyKey;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredWorkspaceSkillPolicy {
    skill_id: SkillId,
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
        capabilities_json: execution_capabilities_json(capabilities)?,
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
        skill_catalog: pioneer_skills::SkillCatalogSnapshot {
            version: 0,
            generated_at_unix: 0,
            skills: Vec::new(),
        },
        input: from_snapshot_json(&snapshot.input_json, "turn input")?,
        capabilities: restored_execution_capabilities(&snapshot.capabilities_json)?,
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

fn execution_capabilities_json(capabilities: &[TurnCapability]) -> Result<String> {
    validate_execution_capabilities(capabilities)?;
    to_snapshot_json(capabilities, "turn capabilities")
}

fn restored_execution_capabilities(value: &str) -> Result<Vec<TurnCapability>> {
    let capabilities: Vec<TurnCapability> = from_snapshot_json(value, "turn capabilities")?;
    validate_execution_capabilities(&capabilities)?;
    Ok(capabilities)
}

fn validate_execution_capabilities(capabilities: &[TurnCapability]) -> Result<()> {
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

fn to_snapshot_json<T: Serialize + ?Sized>(value: &T, label: &str) -> Result<String> {
    serde_json::to_string(value).with_context(|| format!("failed to serialize {label} snapshot"))
}

fn from_snapshot_json<T: DeserializeOwned>(value: &str, label: &str) -> Result<T> {
    serde_json::from_str(value).with_context(|| format!("failed to deserialize {label} snapshot"))
}

#[cfg(test)]
mod tests {
    use super::{execution_capabilities_json, restored_execution_capabilities};
    use pioneer_protocol::{
        SkillId, SkillPackId, TurnCapability, TurnCapabilityKind, skill_capability_key,
        skill_pack_capability_key,
    };

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
}
