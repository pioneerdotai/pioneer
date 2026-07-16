use pioneer_gateway::codex_mcp_conformance::run_codex_mcp_deterministic_conformance;

#[tokio::test]
async fn codex_projection_and_native_lifecycle_are_deterministic() {
    let evidence = run_codex_mcp_deterministic_conformance()
        .await
        .expect("deterministic Codex MCP checks should pass");

    assert_eq!(evidence.callable_a, "mcp_fixture_tool_a");
    assert_eq!(evidence.callable_b, "mcp_fixture_tool_b");
    assert_eq!(evidence.manifest_a.len(), 64);
    assert_eq!(evidence.manifest_b.len(), 64);
    assert!(evidence.same_projection_reused);
    assert!(evidence.changed_projection_requires_restart);
    assert!(evidence.native_thread_identity_preserved);
    assert!(evidence.concurrent_projections_isolated);
    assert!(evidence.empty_projection_is_empty);
    assert!(evidence.exact_managed_config_isolated);
    assert!(evidence.exact_approval_fallback_allowed);
    assert!(evidence.stale_approval_fallback_denied);
    assert!(evidence.exact_native_item_bound);
    assert!(evidence.unselected_native_item_rejected);
    assert!(evidence.native_timeline_deduplicated);
    assert!(evidence.initial_turn_blocked_until_exact_list);
    assert!(evidence.helper_attached);
    assert!(evidence.bridge_call_succeeded);
    assert!(evidence.bridge_cancellation_propagated);
    assert!(evidence.bridge_cleanup_complete);
    assert!(evidence.secret_canary_absent);
    assert_eq!(evidence.recorded_started_items, 1);
    assert_eq!(evidence.recorded_progress_items, 1);
    assert_eq!(evidence.recorded_completed_items, 1);
    assert_eq!(evidence.recorded_permission_requests, 1);
}
