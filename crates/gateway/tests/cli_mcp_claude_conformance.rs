use pioneer_gateway::claude_mcp_conformance::run_claude_mcp_deterministic_conformance;

#[tokio::test]
async fn claude_projection_permissions_and_lifecycle_are_deterministic() {
    let evidence = run_claude_mcp_deterministic_conformance()
        .await
        .expect("deterministic Claude MCP checks should pass");

    assert_eq!(evidence.callable_a, "mcp_server_tool_a");
    assert_eq!(evidence.callable_b, "mcp_server_tool_b");
    assert_eq!(evidence.qualified_a, "mcp__pioneer__mcp_server_tool_a");
    assert_eq!(evidence.qualified_b, "mcp__pioneer__mcp_server_tool_b");
    assert_eq!(evidence.manifest_a.len(), 64);
    assert_eq!(evidence.manifest_b.len(), 64);
    assert!(evidence.same_projection_reused);
    assert!(evidence.changed_projection_requires_restart);
    assert!(evidence.provider_session_identity_preserved);
    assert!(evidence.concurrent_projections_isolated);
    assert!(evidence.empty_projection_is_empty);
    assert!(evidence.strict_managed_config_isolated);
    assert!(evidence.mixed_skill_server_preflight_preserved);
    assert!(evidence.mixed_skill_tool_preflight_preserved);
    assert!(evidence.exact_native_item_bound);
    assert!(evidence.unselected_native_item_rejected);
    assert!(evidence.exact_permission_request_parsed);
    assert!(evidence.wildcard_permission_request_rejected);
    assert!(evidence.exact_permission_fallback_allowed);
    assert!(evidence.permission_callback_deduplicated);
    assert!(evidence.native_timeline_deduplicated);
    assert!(evidence.initial_turn_blocked_until_exact_list);
    assert!(evidence.helper_attached);
    assert!(evidence.bridge_call_succeeded);
    assert!(evidence.bridge_cancellation_propagated);
    assert!(evidence.bridge_cleanup_complete);
    assert!(evidence.secret_canary_absent);
    assert_eq!(evidence.recorded_scenarios, 6);
    assert_eq!(evidence.recorded_tool_uses, 3);
    assert_eq!(evidence.recorded_unique_call_ids, 2);
}
