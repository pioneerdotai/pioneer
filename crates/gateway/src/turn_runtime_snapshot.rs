use anyhow::{Context, Result, bail};
use pioneer_agent::{
    AgentTurnHookRuntimeContext, ExecutionWindowUsageSnapshot, ResolvedArtifactInput,
    RestoredRecoveryTurnRequest, WorkspaceSkillPolicy,
};
use pioneer_crud::{CrudStore, NewTurnRuntimeSnapshot, TurnRuntimeSnapshotRecord};
use pioneer_protocol::ReasoningEffort;
use pioneer_protocol::{
    SkillId, ThreadMode, TurnCapability, TurnExecutionSecuritySnapshot,
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
        agent_skill_versions_json: agent_skill_versions_json(agent_skill_overlay)?,
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
    agent_skill_overlay: Vec<AgentSkillRuntimeEntry>,
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
        agent_skill_overlay,
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

pub(crate) fn restored_conversation_scope_from_snapshot(
    snapshot: &TurnRuntimeSnapshotRecord,
) -> Result<(AgentTurnHookRuntimeContext, Vec<ChatMessage>)> {
    Ok((
        from_snapshot_json(&snapshot.hook_runtime_context_json, "hook runtime context")?,
        from_snapshot_json(&snapshot.history_json, "conversation history")?,
    ))
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

    fn runtime_snapshot(agent_skill_versions_json: Option<String>) -> TurnRuntimeSnapshotRecord {
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
            hook_runtime_context_json: "{}".to_owned(),
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
