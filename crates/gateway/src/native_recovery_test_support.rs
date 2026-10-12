//! Source-only fixtures shared by the two actual native recovery consumers.
use pioneer_agent::ToolLoopConfig;
use pioneer_crud::{CrudStore, NewTurnExecutionWindowRecord, TurnExecutionWindowRecord};
use pioneer_protocol::{
    ExecutionCheckpointPayload, ExecutionProgressSummary, ExecutionWindowStatus,
};
use sea_orm::{ActiveModelTrait, Set};
use serde_json::json;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn charged_native_window(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    owner: &str,
    index: u32,
    config: &ToolLoopConfig,
    receipts: [(Option<u64>, Option<u64>); 2],
    progress: ExecutionProgressSummary,
) -> (TurnExecutionWindowRecord, ExecutionCheckpointPayload) {
    let now = chrono::Utc::now().fixed_offset();
    if store.get_turn_execution(turn).await.unwrap().is_none() {
        pioneer_entity::turn_execution::ActiveModel {
            turn_id: Set(turn.into()),
            thread_id: Set(thread.into()),
            workspace_id: Set(workspace.into()),
            executor_kind: Set("native_agent".into()),
            executor_key: Set(Some("echo".into())),
            status: Set("running".into()),
            owner_id: Set(owner.into()),
            owner_generation: Set(1),
            lease_until: Set(now + chrono::Duration::minutes(2)),
            heartbeat_at: Set(now),
            started_at: Set(Some(now)),
            completed_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&store.database_connection())
        .await
        .unwrap();
    }
    store
        .persist_native_cancellation_context(
            pioneer_protocol::NativeTerminalEffectPreparation {
                batch_id: format!("{turn}:cancel"),
                workspace_id: workspace.into(),
                thread_id: thread.into(),
                turn_id: turn.into(),
                runtime_generation: 1,
                effects: vec![],
            },
            owner,
            now.timestamp(),
            true,
        )
        .await
        .unwrap();
    let window = store.create_turn_execution_window(NewTurnExecutionWindowRecord {
        workspace_id: workspace.into(), thread_id: thread.into(), turn_id: turn.into(), window_index: index,
        status: ExecutionWindowStatus::Running, exhaustion_reason: None,
        agent_round_count: 0, tool_call_count: 0, provider_token_count: 0,
        metadata_json: json!({"runtimeWindowId":format!("{turn}:window:{index}"),"acceptedOwnerMarker":"keep"}),
        started_at: now,
    },now,now).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let prompt = pioneer_promt::compile_prompt(pioneer_promt::PromptCompileInput {
        workspace_root: root.path().into(),
        profile: pioneer_promt::PromptProfile::AssistantFull,
        skills_prompt: None,
        retry_instruction: None,
        include_tool_recovery_policy: true,
        include_task_orchestration_policy: false,
        continue_generation_hint: false,
        runtime_sections: vec![],
        dynamic_sections: vec![],
        dynamic_context: None,
        extra_system: Some("original admission clock".into()),
        limits: Default::default(),
    })
    .unwrap();
    let instructions = pioneer_promt::compile_instruction_delivery_plan(prompt).unwrap();
    let manifest = json!({"sources":[],"diagnostics":[]});
    let mut phase: serde_json::Value = serde_json::from_str(
        &pioneer_agent::NativeInitialPreparation::new_admission_json(
            workspace,
            thread,
            turn,
            pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES,
        )
        .unwrap(),
    )
    .unwrap();
    phase["state"] = json!({"phase":"completed","outputs":{
        "native_agent_action_contract":null,"mcp_descriptors":[],"mcp_manifest_hash":null,
        "policy":{"policies":pioneer_hooks::HookPolicySet::default()},
        "prompt_context":{"contexts":pioneer_hooks::HookPromptContextSet::default(),"manifest":manifest},
        "prompt_sections":{"sections":pioneer_hooks::HookPromptSectionSet::default(),"manifest":manifest},
        "instructions":instructions,"capability_diagnostics":[],
        "skill_plan":{"tools":[],"read_skill_index":{},"excluded_tools":[]},
        "tool_metadata":pioneer_hooks::HookToolBundleSet::default(),"extension_specs":[],
        "provider_tool_calling":false,"initiating_thread_id":null,"visible_tools":[],"core_tools":[],
        "selected_optional_tools":[],"requested_tools":[],"config":config,
        "preflight_usage":{"attempts":1,"known_tokens":receipts[0].0.unwrap_or(0)+receipts[0].1.unwrap_or(0),
            "unknown":receipts[0].0.is_none() || receipts[0].1.is_none()}
    }});
    let phase = phase.to_string();
    assert!(
        pioneer_agent::NativeInitialPreparation::completed_for_resume(
            &phase,
            pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES
        )
        .unwrap()
    );
    let mut snapshot = crate::turn_runtime_snapshot::new_turn_runtime_snapshot(
        thread,
        workspace,
        turn,
        pioneer_protocol::ThreadMode::Agent,
        &pioneer_agent::AgentTurnHookRuntimeContext::task("native-recovery-fixture"),
        "test-model",
        "echo",
        None,
        &Default::default(),
        &[],
        &[],
        &[],
        &Default::default(),
        &[],
        &[],
        &pioneer_skills::SkillCatalogSnapshot {
            version: 1,
            generated_at_unix: now.timestamp(),
            skills: vec![],
        },
    )
    .unwrap();
    snapshot.initial_phase_json = Some(phase);
    store.upsert_turn_runtime_snapshot(snapshot).await.unwrap();
    for (ordinal, (input, output)) in receipts.into_iter().enumerate() {
        let item = pioneer_protocol::TurnItem::SystemEvent {
            id: format!("usage_{index}_{ordinal}"),
            level: pioneer_protocol::SystemEventLevel::Info,
            message: "durable provider usage".into(),
            code: Some("provider_usage".into()),
            details: Some(
                json!({"schema_version":1,"window_index":index,"status":"completed",
                "phase":if ordinal==0 {"preflight"} else {"main"},
                "usage":{"input_tokens":input,"output_tokens":output}}),
            ),
        };
        let mut started = item.clone();
        if let pioneer_protocol::TurnItem::SystemEvent {
            details: Some(details),
            ..
        } = &mut started
        {
            details["status"] = json!("started");
            details["usage"] = json!({});
        }
        store
            .materialize_item_started(
                pioneer_protocol::ItemStartedNotification {
                    workspace_id: workspace.into(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    item: started,
                },
                now.timestamp(),
            )
            .await
            .unwrap();
        store
            .materialize_item_updated(
                pioneer_protocol::ItemUpdatedNotification {
                    workspace_id: workspace.into(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    item: item.clone(),
                },
                now.timestamp(),
            )
            .await
            .unwrap();
        store
            .materialize_item_updated(
                pioneer_protocol::ItemUpdatedNotification {
                    workspace_id: workspace.into(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    item,
                },
                now.timestamp(),
            )
            .await
            .unwrap();
    }
    let tokens = receipts
        .iter()
        .map(|(a, b)| a.unwrap_or(0) + b.unwrap_or(0))
        .sum();
    let complete = receipts.iter().all(|(a, b)| a.is_some() && b.is_some());
    let mut payload = pioneer_protocol::build_execution_checkpoint_payload(
        workspace,
        thread,
        turn,
        pioneer_protocol::build_execution_checkpoint_original_request_summary(&[]),
        pioneer_protocol::ExecutionCheckpointWindowSummary {
            progress: Some(progress),
            window_id: Some(format!("{turn}:window:{index}")),
            window_index: index,
            started_at_unix_ms: Some(now.timestamp_millis()),
            completed_at_unix_ms: None,
            agent_round_count: 2,
            tool_call_count: 0,
            provider_token_count: Some(tokens),
            exhaustion_reason: None,
        },
        pioneer_protocol::build_execution_checkpoint_provider_budget_summary(
            pioneer_protocol::ExecutionCheckpointProviderBudgetInput {
                model: Some("test-model".into()),
                model_provider: Some("echo".into()),
                agent_round_count: 2,
                tool_call_count: 0,
                provider_token_count: Some(tokens),
                exhaustion_reason: None,
                exhausted_limit: None,
                exhausted_observed: None,
            },
        ),
        pioneer_protocol::build_execution_checkpoint_tool_summary(&[], 32),
        vec![],
    );
    payload.provider_budget.provider_usage_available = complete;
    // Storage accepts the already Agent-validated state as opaque. Its exact
    // cold codec/guard proof is covered by the Agent semantic fixtures.
    payload.native_semantic_state = Some(
        json!({"schema_version":pioneer_protocol::NATIVE_SEMANTIC_STATE_SCHEMA_VERSION,
        "checkpoint_revision":1,"attached_task_wait":null,"progress_frontier":{"schema_version":1,
            "advanced_observations":progress.frontier[0],"unchanged_observations":progress.frontier[1],"unknown_observations":progress.frontier[2]}}),
    );
    store
        .commit_native_semantic_checkpoint(payload, None, 1, owner, 1)
        .await
        .unwrap();
    let semantic = store
        .native_semantic_checkpoint_for_turn(turn)
        .await
        .unwrap()
        .unwrap();
    (
        window,
        serde_json::from_value(semantic.payload_json).unwrap(),
    )
}
