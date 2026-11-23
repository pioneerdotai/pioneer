use super::*;

#[tokio::test]
async fn skill_bindings_and_execution_authorization_context_commit_atomically() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "workspace_skill_context",
            "thread_skill_context",
            "turn_skill_context",
        )
        .await;
        let committed = vec![TurnSkillBindingRecord {
            skill_id: SkillId::new("S".repeat(21)).expect("valid skill id"),
            skill_owner: Some("pioneer".to_owned()),
            skill_slug: "committed-skill".to_owned(),
            skill_version: Some("1.0.0".to_owned()),
            fingerprint: "committed-fingerprint".to_owned(),
            source_kind: "system".to_owned(),
            resolved_reason: "explicit_composer_capability".to_owned(),
        }];
        let committed_context = r#"{"version":1,"state":"committed"}"#;
        store
            .replace_turn_skill_bindings_with_authorization_context(
                "turn_skill_context",
                committed.as_slice(),
                1_700_000_000,
                committed_context,
            )
            .await
            .expect("skill projection and authorization context should persist");

        let attempted = vec![TurnSkillBindingRecord {
            skill_id: SkillId::new("T".repeat(21)).expect("valid skill id"),
            skill_owner: None,
            skill_slug: "attempted-skill".to_owned(),
            skill_version: Some("2.0.0".to_owned()),
            fingerprint: "attempted-fingerprint".to_owned(),
            source_kind: "user".to_owned(),
            resolved_reason: "path_match".to_owned(),
        }];
        store
            .replace_turn_skill_bindings_with_authorization_context(
                "turn_skill_context",
                attempted.as_slice(),
                1_700_000_100,
                "   ",
            )
            .await
            .expect_err("invalid authorization context must abort the whole transaction");
        assert_eq!(
            store
                .find_turn_skill_bindings("turn_skill_context")
                .await
                .expect("committed skill binding query"),
            committed
        );
        assert_eq!(
            store
                .get_turn_execution_authorization_context("turn_skill_context")
                .await
                .expect("committed skill authorization context query"),
            Some(committed_context.to_owned())
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cli_mcp_activation_atomically_binds_turn_and_every_frozen_tool() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "workspace_projection_activation",
            "thread_projection_activation",
            "turn_projection_activation",
        )
        .await;
        let replacement = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_activation",
                "workspace_projection_activation",
                "manifest-activation",
                2,
            ),
            bindings: vec![test_turn_mcp_binding(1), test_turn_mcp_binding(2)],
        };
        store
            .replace_turn_mcp_projection(&replacement)
            .await
            .expect("projection should persist");
        let timestamp = unix_to_datetime(1_700_000_101);
        store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: "turn_projection_activation".to_owned(),
                thread_id: "thread_projection_activation".to_owned(),
                continuation_thread_id: "thread_projection_activation".to_owned(),
                workspace_id: "workspace_projection_activation".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "native-thread".to_owned(),
                native_turn_id: None,
                request_id: None,
                status: "running".to_owned(),
                model: None,
                cwd: None,
                sandbox_json: None,
                approval_policy: None,
                input_mapping_json: "{}".to_owned(),
                created_at: timestamp,
                updated_at: timestamp,
            })
            .await
            .expect("CLI turn binding should persist");
        let metadata = CliRuntimeTurnMcpMetadata {
            adapter_kind: "codex_synthetic_mcp".to_owned(),
            manifest_hash: "manifest-activation".to_owned(),
            projection_fingerprint: "projection-fingerprint".to_owned(),
            provider_contract_fingerprint: "provider-contract".to_owned(),
            isolation_contract_fingerprint: "isolation-contract".to_owned(),
            session_generation: 4,
            projection_activation_generation: 9,
        };
        let turn_binding = store
            .bind_cli_runtime_turn_mcp_activation("turn_projection_activation", metadata.clone())
            .await
            .expect("activation should bind atomically");

        assert_eq!(turn_binding.mcp, Some(metadata));
        assert!(
            store
                .list_turn_mcp_bindings("turn_projection_activation")
                .await
                .expect("bindings should load")
                .iter()
                .all(|binding| binding.projection_activation_generation == 9)
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn mcp_projection_and_execution_authorization_context_commit_atomically() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "workspace_projection_context",
            "thread_projection_context",
            "turn_projection_context",
        )
        .await;
        let committed = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_context",
                "workspace_projection_context",
                "manifest-context-committed",
                1,
            ),
            bindings: vec![test_turn_mcp_binding(1)],
        };
        let committed_context = r#"{"version":1,"state":"committed"}"#;
        store
            .replace_turn_mcp_projection_with_authorization_context(&committed, committed_context)
            .await
            .expect("projection and authorization context should persist");
        assert_eq!(
            store
                .get_turn_execution_authorization_context("turn_projection_context")
                .await
                .expect("authorization context query"),
            Some(committed_context.to_owned())
        );

        let attempted = TurnMcpProjectionReplacement {
            projection: test_turn_mcp_projection(
                "turn_projection_context",
                "workspace_projection_context",
                "manifest-context-attempted",
                2,
            ),
            bindings: vec![test_turn_mcp_binding(2), test_turn_mcp_binding(3)],
        };
        store
            .replace_turn_mcp_projection_with_authorization_context(&attempted, "   ")
            .await
            .expect_err("invalid authorization context must abort the whole transaction");
        assert_eq!(
            store
                .get_turn_mcp_projection("turn_projection_context")
                .await
                .expect("committed projection query"),
            Some(committed.projection)
        );
        assert_eq!(
            store
                .list_turn_mcp_bindings("turn_projection_context")
                .await
                .expect("committed binding query"),
            committed.bindings
        );
        assert_eq!(
            store
                .get_turn_execution_authorization_context("turn_projection_context")
                .await
                .expect("committed authorization context query"),
            Some(committed_context.to_owned())
        );
        Ok(())
    })
    .await
    .unwrap();
}

fn test_turn_mcp_projection(
    turn_id: &str,
    workspace_id: &str,
    manifest_hash: &str,
    tool_count: i32,
) -> TurnMcpProjectionRecord {
    TurnMcpProjectionRecord {
        turn_id: turn_id.to_owned(),
        workspace_id: workspace_id.to_owned(),
        projection_version: 1,
        manifest_hash: manifest_hash.to_owned(),
        resolution_status: "resolved".to_owned(),
        tool_count,
        created_at_unix: 1_700_000_100,
    }
}

fn test_turn_mcp_binding(index: usize) -> TurnMcpBindingRecord {
    let canonical_callable_name = format!("mcp_test_tool_{index}");
    TurnMcpBindingRecord {
        server_installation_id: format!("server_mcp_{index:02}"),
        server_name: "test".to_owned(),
        raw_tool_name: format!("tool_{index}"),
        callable_name: canonical_callable_name.clone(),
        canonical_callable_name: canonical_callable_name.clone(),
        provider_callable_name: format!("mcp__pioneer__{canonical_callable_name}"),
        catalog_version: "catalog-v1".to_owned(),
        fingerprint: format!("installation-fingerprint-{index}"),
        canonical_schema_fingerprint: format!("canonical-schema-{index}"),
        provider_schema_fingerprint: format!("provider-schema-{index}"),
        annotations_json: r#"{"readOnlyHint":true}"#.to_owned(),
        annotations_digest: format!("annotations-{index}"),
        effective_timeout_ms: 20_000,
        runtime_generation: 7,
        projection_activation_generation: 3,
        selection_reason: "explicit_composer_capability".to_owned(),
        capability_id: Some(format!("mcp:test:tool_{index}")),
    }
}
