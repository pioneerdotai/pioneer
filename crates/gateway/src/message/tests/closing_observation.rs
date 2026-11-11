use super::*;

#[tokio::test]
async fn artifact_retry_wait_is_distinct_from_gate_checks_and_resolves_on_recheck() {
    use pioneer_observability::{
        live::{WaitKind, WorkState},
        scenario::accept_params,
    };
    pioneer_observability::set_telemetry_enabled(true);
    let (workspaces, store, _) = setup_workspace_manager().await;
    let processor = MessageProcessor::with_agent_manager(
        Arc::new(ThreadManager::new("test-model", "openai")),
        Arc::new(AgentManager::new(test_provider(), test_tool_loop_config())),
        Arc::new(SessionManager::new()),
        workspaces,
        store,
    );
    let mut params = serde_json::json!({"_pioneer_correlation":{
        "v":1,"trace":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        "span":"2222222222222222","flags":1}});
    let (source, _) = accept_params("thread/open", &mut params);
    let waits = || {
        pioneer_observability::live::snapshot()
            .unwrap()
            .active
            .into_iter()
            .filter(|work| {
                work.context.trace_id == Some([0xee; 16])
                    && matches!(work.state, WorkState::Waiting(WaitKind::UserOrProvider))
            })
            .map(|work| work.context.token)
            .collect::<Vec<_>>()
    };
    let decision = source
        .scope(processor.artifact_finalization_decision(
            "closing-thread",
            "closing-turn",
            "Saved under $PIONEER_ARTIFACT_OUTPUT_DIR/report.txt",
        ))
        .await;
    assert!(matches!(
        decision,
        pioneer_agent::TurnFinalizationDecision::Retry { .. }
    ));
    assert_eq!(waits().len(), 1);
    // No artificial polling: the next actual finalization request resolves
    // the corrective wait, independently of any later turn or socket work.
    let decision = source
        .scope(processor.artifact_finalization_decision(
            "closing-thread",
            "closing-turn",
            "The artifact is registered.",
        ))
        .await;
    assert!(matches!(
        decision,
        pioneer_agent::TurnFinalizationDecision::Allow
    ));
    assert!(waits().is_empty());
    let decision = source
        .scope(processor.artifact_finalization_decision(
            "closing-thread",
            "closing-turn",
            "Saved under $PIONEER_ARTIFACT_OUTPUT_DIR/report.txt",
        ))
        .await;
    assert!(matches!(
        decision,
        pioneer_agent::TurnFinalizationDecision::Fail { .. }
    ));
    assert!(waits().is_empty()); // observer did not reset the business retry budget
    processor
        .clear_artifact_finalization_state("closing-turn")
        .await;
    assert!(waits().is_empty());
}
