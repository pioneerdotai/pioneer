use super::*;
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_protocol::ThreadMode;

#[tokio::test]
async fn turn_runtime_snapshot_upsert_round_trips_and_updates() {
    pioneer_postgres_test::with_database(async |fixture| {
        let writer = fixture.connection();
        writer.execute_unprepared(
            "UPDATE workspace SET is_current = false WHERE is_current;
             INSERT INTO workspace (id,name,is_active,is_current)
                VALUES ('ws_turn_runtime_snapshot','Runtime fixture',true,true);
             INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status,origin_kind)
                VALUES ('thread_runtime_snapshot','ws_turn_runtime_snapshot','','agent','model','provider','active','user');
             INSERT INTO turn (id,thread_id,status,turn_kind,origin)
                VALUES ('turn_runtime_snapshot','thread_runtime_snapshot','in_progress','conversation','user'),
                       ('turn_runtime_base_fallback','thread_runtime_snapshot','in_progress','conversation','user');",
        ).await?;
        let store = CompactionStore::for_test(pioneer_sqlite::SqliteDatabase::new(
            fixture.connect_read_only().await?, writer.clone(),
        ));
        let created_at = unix_to_datetime(1_700_000_000);
        let updated_at = unix_to_datetime(1_700_000_010);
        let snapshot = NewTurnRuntimeSnapshot {
            turn_id: "turn_runtime_snapshot".to_owned(),
            thread_id: "thread_runtime_snapshot".to_owned(),
            workspace_id: "ws_turn_runtime_snapshot".to_owned(),
            mode: pioneer_protocol::ThreadMode::Agent,
            model: "model-a".to_owned(),
            provider_name: "provider-a".to_owned(),
            reasoning_effort: None,
            agent_skill_versions_json: Some(
                r#"[{"skill_id":"AAAAAAAAAAAAAAAAAAAAA","version_id":"111111111111111111111","fingerprint":"fingerprint-a"}]"#
                    .to_owned(),
            ),
            hook_runtime_context_json: r#"{"mode":"agent","actor_kind":"agent"}"#.to_owned(),
            workspace_skill_policies_json: "[]".to_owned(),
            input_json: r#"[{"type":"text","text":"hello","textElements":[]}]"#.to_owned(),
            capabilities_json: "[]".to_owned(),
            resolved_artifacts_json: "[]".to_owned(),
            runtime_environment_json: r#"{"PIONEER_ARTIFACT_OUTPUT_DIR":"/tmp/a"}"#.to_owned(),
            history_json: r#"[{"role":"user","content":"hello"}]"#.to_owned(),
            created_at,
            updated_at: created_at,
        };

        let inserted = store
            .upsert_turn_runtime_snapshot(snapshot.clone())
            .await
            .expect("snapshot insert should succeed");
        assert_eq!(inserted.model, "model-a");
        assert_eq!(inserted.mode, ThreadMode::Agent);
        assert_eq!(inserted.created_at, created_at);
        assert!(inserted.agent_skill_versions_json.is_some());

        let mut replacement = snapshot.clone();
        replacement.mode = ThreadMode::Chat;
        replacement.model = "model-b".to_owned();
        replacement.provider_name = "provider-b".to_owned();
        replacement.reasoning_effort = Some("max".to_owned());
        replacement.agent_skill_versions_json = None;
        replacement.runtime_environment_json =
            r#"{"PIONEER_ARTIFACT_OUTPUT_DIR":"/tmp/b"}"#.to_owned();
        replacement.created_at = updated_at;
        replacement.updated_at = updated_at;

        let updated = store
            .upsert_turn_runtime_snapshot(replacement)
            .await
            .expect("snapshot update should succeed");
        assert_eq!(updated.model, "model-b");
        assert_eq!(updated.mode, ThreadMode::Chat);
        assert_eq!(updated.provider_name, "provider-b");
        assert_eq!(updated.reasoning_effort.as_deref(), Some("max"));
        assert!(updated.agent_skill_versions_json.is_none());
        assert_eq!(updated.created_at, created_at);
        assert_eq!(updated.updated_at, updated_at);

        let fetched = store
            .get_turn_runtime_snapshot("turn_runtime_snapshot")
            .await
            .expect("snapshot read should succeed")
            .expect("snapshot should exist");
        assert_eq!(
            fetched.runtime_environment_json,
            updated.runtime_environment_json
        );
        assert_eq!(fetched.reasoning_effort.as_deref(), Some("max"));
        assert_eq!(fetched.mode, ThreadMode::Chat);
        let stored_mode = pioneer_entity::turn_runtime_snapshot::Entity::find_by_id(
            "turn_runtime_snapshot",
        ).select_only()
        .column(pioneer_entity::turn_runtime_snapshot::Column::Mode)
        .into_tuple::<String>()
        .one(fixture.connection())
        .await?;
        assert_eq!(stored_mode.as_deref(), Some("chat"));

        store
            .connection
            .execute_unprepared(
                "CREATE FUNCTION reject_agent_skill_pin_snapshot() RETURNS trigger LANGUAGE plpgsql AS $fault$
                 BEGIN RAISE EXCEPTION 'forced Agent pin failure'; END $fault$;
                 CREATE TRIGGER reject_agent_skill_pin_snapshot \
                 BEFORE INSERT ON turn_runtime_snapshot \
                 FOR EACH ROW WHEN (NEW.agent_skill_versions_json IS NOT NULL) \
                 EXECUTE FUNCTION reject_agent_skill_pin_snapshot()",
            )
            .await
            .expect("conditional Agent pin failure trigger must install");
        let mut base_fallback = snapshot;
        base_fallback.turn_id = "turn_runtime_base_fallback".to_owned();
        assert!(
            store
                .upsert_turn_runtime_snapshot(base_fallback.clone())
                .await
                .is_err(),
            "conditional storage failure must reject the Agent-pinned snapshot"
        );
        assert!(store.get_turn_runtime_snapshot("turn_runtime_base_fallback").await?.is_none());
        base_fallback.agent_skill_versions_json = None;
        let base_only = store
            .upsert_turn_runtime_snapshot(base_fallback)
            .await
            .expect("the same authoritative snapshot must persist without optional Agent pins");
        assert!(base_only.agent_skill_versions_json.is_none());

        assert_eq!(
            store
                .delete_turn_runtime_snapshot("turn_runtime_snapshot")
                .await
                .expect("snapshot delete should succeed"),
            1
        );
        assert!(
            store
                .get_turn_runtime_snapshot("turn_runtime_snapshot")
                .await
                .expect("snapshot read after delete should succeed")
                .is_none()
        );
            Ok(())
        }).await.unwrap();
}

use pioneer_crud_execution::{
    cli_runtime_binding::{
        CliRuntimeProviderSessionLifecycle, CliRuntimeThreadMcpMetadata,
        CliRuntimeTurnBindingListFilter, CliRuntimeTurnMcpMetadata, NewCliRuntimeThreadBinding,
        NewCliRuntimeTurnBinding, PrepareClaudeProviderSessionBinding,
        PreparedClaudeProviderSessionMode,
    },
    cli_runtime_store::CliRuntimeStore,
};

async fn binding_store(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace: &str,
) -> CompactionStore {
    let writer = fixture.connection();
    writer
        .execute_unprepared("UPDATE workspace SET is_current = FALSE WHERE is_current")
        .await
        .unwrap();
    writer.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ($1,'CLI binding fixture',TRUE,TRUE)",
        [workspace.into()],
    )).await.unwrap();
    CompactionStore::for_test(pioneer_sqlite::SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        writer.clone(),
    ))
}

#[tokio::test]
async fn cli_runtime_thread_and_turn_bindings_upsert_idempotently() {
    pioneer_postgres_test::with_database(async |fixture| {
        use futures_util::FutureExt;
        let store = binding_store(fixture, "ws_cli_bind").await;
        let database = store.database_connection();
        let execution = CliRuntimeStore::new(database.clone());
        let mut frozen = database.work_signal("maintenance.frozen").subscribe();
        frozen.begin_pass(&database).await.unwrap();
        let mut html = database.work_signal("maintenance.html").subscribe();
        html.begin_pass(&database).await.unwrap();
        let mut consumers = ["gateway.control", "gateway.cli"]
            .map(|domain| database.work_signal(domain).subscribe());
        for consumer in &mut consumers {
            consumer.begin_pass(&database).await.unwrap();
        }
        let created_at = unix_to_datetime(1_700_010_000);
        let updated_at = unix_to_datetime(1_700_010_030);

        let thread_binding = NewCliRuntimeThreadBinding {
            thread_id: "thread_cli_bind".to_owned(),
            workspace_id: "ws_cli_bind".to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            native_thread_id: "codex-thread-a".to_owned(),
            native_session_id: Some("session-a".to_owned()),
            native_root_thread_id: None,
            native_cwd: Some("/tmp/project-a".to_owned()),
            native_model: Some("gpt-5".to_owned()),
            resume_cursor_json: "{}".to_owned(),
            status: "active".to_owned(),
            created_at,
            updated_at: created_at,
        };
        let inserted_thread = store
            .upsert_cli_runtime_thread_binding(thread_binding.clone())
            .await
            .expect("thread binding insert should succeed");
        assert_eq!(
            inserted_thread.native_session_id.as_deref(),
            Some("session-a")
        );
        assert!(inserted_thread.mcp.is_none());

        for consumer in &mut consumers {
            assert!(
                consumer.changed().now_or_never().is_some(),
                "committed binding wakes its consumers"
            );
            consumer.begin_pass(&database).await.unwrap();
        }
        assert!(
            frozen.wait(None).now_or_never().is_none(),
            "a binding with no frozen descriptor cannot release frozen data"
        );
        assert!(
            html.wait(None).now_or_never().is_none(),
            "a CLI receipt without history is not an HTML retention root"
        );
        let repeated_thread = store
            .upsert_cli_runtime_thread_binding(thread_binding.clone())
            .await
            .unwrap();
        assert_eq!(repeated_thread, inserted_thread);
        for consumer in &mut consumers {
            assert!(
                consumer.changed().now_or_never().is_none(),
                "exact replay cannot wake another empty pass"
            );
        }

        let thread_mcp = CliRuntimeThreadMcpMetadata {
            adapter_kind: "codex_stdio_bridge".to_owned(),
            manifest_hash: "manifest-thread-v1".to_owned(),
            projection_fingerprint: "projection-thread-v1".to_owned(),
            provider_contract_fingerprint: "provider-contract-v1".to_owned(),
            isolation_contract_fingerprint: "isolation-contract-v1".to_owned(),
            session_generation: 11,
            provider_session_id: Some("01900000-0000-7000-8000-000000000021".to_owned()),
            provider_session_lifecycle_state: Some("verified".to_owned()),
            provider_session_last_verified_process_generation: Some(11),
        };
        let thread_with_mcp = execution
            .set_cli_runtime_thread_mcp_metadata("thread_cli_bind", Some(thread_mcp.clone()))
            .await
            .expect("thread MCP metadata update should succeed");
        assert_eq!(thread_with_mcp.mcp, Some(thread_mcp.clone()));

        let mut replacement_thread = thread_binding;
        replacement_thread.native_session_id = Some("session-b".to_owned());
        replacement_thread.native_model = Some("gpt-5.1".to_owned());
        replacement_thread.resume_cursor_json = r#"{"cursor":"after-turn-1"}"#.to_owned();
        replacement_thread.created_at = updated_at;
        replacement_thread.updated_at = updated_at;
        let updated_thread = store
            .upsert_cli_runtime_thread_binding(replacement_thread)
            .await
            .expect("thread binding update should succeed");
        assert_eq!(updated_thread.created_at, created_at);
        assert_eq!(updated_thread.updated_at, updated_at);
        assert_eq!(
            updated_thread.native_session_id.as_deref(),
            Some("session-b")
        );
        assert_eq!(updated_thread.mcp, Some(thread_mcp));

        let by_native = execution
            .get_cli_runtime_thread_binding_by_native_thread("codex", "codex-thread-a")
            .await
            .expect("native thread lookup should succeed")
            .expect("native thread binding should exist");
        assert_eq!(by_native.thread_id, "thread_cli_bind");
        assert_eq!(
            execution
                .list_cli_runtime_thread_bindings_for_runtime("ws_cli_bind", "codex")
                .await
                .expect("runtime thread list should succeed")
                .len(),
            1
        );

        let turn_binding = NewCliRuntimeTurnBinding {
            turn_id: "turn_cli_bind".to_owned(),
            thread_id: "thread_cli_bind".to_owned(),
            continuation_thread_id: "thread_cli_bind".to_owned(),
            workspace_id: "ws_cli_bind".to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            native_thread_id: "codex-thread-a".to_owned(),
            native_turn_id: Some("codex-turn-a".to_owned()),
            request_id: Some("rpc-request-a".to_owned()),
            status: "running".to_owned(),
            model: Some("gpt-5".to_owned()),
            cwd: Some("/tmp/project-a".to_owned()),
            sandbox_json: Some(r#"{"mode":"workspace-write"}"#.to_owned()),
            approval_policy: Some("on-request".to_owned()),
            input_mapping_json: r#"{"items":1}"#.to_owned(),
            created_at,
            updated_at: created_at,
        };
        store
            .upsert_cli_runtime_turn_binding(turn_binding.clone())
            .await
            .expect("turn binding insert should succeed");

        let turn_mcp = CliRuntimeTurnMcpMetadata {
            adapter_kind: "codex_stdio_bridge".to_owned(),
            manifest_hash: "manifest-turn-v1".to_owned(),
            projection_fingerprint: "projection-turn-v1".to_owned(),
            provider_contract_fingerprint: "provider-contract-v1".to_owned(),
            isolation_contract_fingerprint: "isolation-contract-v1".to_owned(),
            session_generation: 11,
            projection_activation_generation: 3,
        };
        let turn_with_mcp = execution
            .set_cli_runtime_turn_mcp_metadata("turn_cli_bind", Some(turn_mcp.clone()))
            .await
            .expect("turn MCP metadata update should succeed");
        assert_eq!(turn_with_mcp.mcp, Some(turn_mcp.clone()));

        let mut replacement_turn = turn_binding;
        replacement_turn.status = "completed".to_owned();
        replacement_turn.model = Some("gpt-5.1".to_owned());
        replacement_turn.created_at = updated_at;
        replacement_turn.updated_at = updated_at;
        let updated_turn = store
            .upsert_cli_runtime_turn_binding(replacement_turn)
            .await
            .expect("turn binding update should succeed");
        assert_eq!(updated_turn.created_at, created_at);
        assert_eq!(updated_turn.status, "completed");
        assert_eq!(updated_turn.mcp, Some(turn_mcp));

        let by_request = execution
            .get_cli_runtime_turn_binding_by_request("rpc-request-a")
            .await
            .expect("request lookup should succeed")
            .expect("request turn binding should exist");
        assert_eq!(by_request.turn_id, "turn_cli_bind");
        let by_native_turn = execution
            .get_cli_runtime_turn_binding_by_native_turn("codex", "codex-turn-a")
            .await
            .expect("native turn lookup should succeed")
            .expect("native turn binding should exist");
        assert_eq!(by_native_turn.thread_id, "thread_cli_bind");
        assert_eq!(
            execution
                .list_cli_runtime_turn_bindings_for_thread("thread_cli_bind")
                .await
                .expect("thread turn list should succeed")
                .len(),
            1
        );

        store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: "turn_cli_running".to_owned(),
                thread_id: "thread_cli_bind".to_owned(),
                continuation_thread_id: "thread_cli_bind".to_owned(),
                workspace_id: "ws_cli_bind".to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "codex-thread-a".to_owned(),
                native_turn_id: None,
                request_id: Some("rpc-request-running".to_owned()),
                status: "running".to_owned(),
                model: Some("gpt-5".to_owned()),
                cwd: Some("/tmp/project-a".to_owned()),
                sandbox_json: None,
                approval_policy: Some("on-request".to_owned()),
                input_mapping_json: r#"{"items":2}"#.to_owned(),
                created_at: updated_at,
                updated_at,
            })
            .await
            .expect("running turn binding insert should succeed");
        let active_turn_bindings = execution
            .list_cli_runtime_turn_bindings(CliRuntimeTurnBindingListFilter {
                workspace_id: Some("ws_cli_bind".to_owned()),
                runtime_id: Some("codex".to_owned()),
                statuses: vec!["starting".to_owned(), "running".to_owned()],
                ..Default::default()
            })
            .await
            .expect("active CLI runtime turn bindings should list");
        assert_eq!(active_turn_bindings.len(), 1);
        assert_eq!(active_turn_bindings[0].turn_id, "turn_cli_running");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cli_runtime_binding_claude_session_identity_is_durable_and_generation_fenced() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = binding_store(fixture, "ws_claude_session_identity").await;
        let now = unix_to_datetime(1_700_020_000);
        let new_binding = |thread_id: &str| NewCliRuntimeThreadBinding {
            thread_id: thread_id.to_owned(),
            workspace_id: "ws_claude_session_identity".to_owned(),
            runtime_id: "claude".to_owned(),
            runtime_kind: "claude".to_owned(),
            native_thread_id: "pending".to_owned(),
            native_session_id: None,
            native_root_thread_id: None,
            native_cwd: Some("/tmp/project".to_owned()),
            native_model: Some("claude-sonnet".to_owned()),
            resume_cursor_json: "{}".to_owned(),
            status: "active".to_owned(),
            created_at: now,
            updated_at: now,
        };
        let first_id = "01900000-0000-7000-8000-000000000001";
        let prepared = store
            .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
                thread_binding: new_binding("claude-thread"),
                proposed_provider_session_id: first_id.to_owned(),
                force_new: false,
            })
            .await
            .expect("prepare Claude identity");
        assert_eq!(prepared.mode, PreparedClaudeProviderSessionMode::New);
        assert_eq!(
            prepared
                .binding
                .provider_session
                .as_ref()
                .expect("provider binding")
                .lifecycle,
            CliRuntimeProviderSessionLifecycle::Prepared
        );

        let duplicate = store
            .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
                thread_binding: new_binding("claude-thread"),
                proposed_provider_session_id: "01900000-0000-7000-8000-000000000099".to_owned(),
                force_new: false,
            })
            .await
            .expect("idempotent duplicate prepare");
        assert_eq!(
            duplicate
                .binding
                .provider_session
                .as_ref()
                .expect("duplicate provider binding")
                .provider_session_id,
            first_id
        );

        let reloaded = CompactionStore::for_test(store.database_connection());
        let execution = CliRuntimeStore::new(store.database_connection());
        assert_eq!(
            reloaded
                .get_cli_runtime_thread_binding("claude-thread")
                .await
                .expect("reload binding")
                .expect("reloaded binding")
                .provider_session
                .expect("reloaded provider binding")
                .provider_session_id,
            first_id
        );
        let verified = execution
            .verify_claude_provider_session_binding("claude-thread", first_id, Some(first_id), 2)
            .await
            .expect("verify emitted identity");
        let provider = verified
            .provider_session
            .expect("verified provider binding");
        assert_eq!(
            provider.lifecycle,
            CliRuntimeProviderSessionLifecycle::Verified
        );
        assert_eq!(provider.last_verified_process_generation, Some(2));
        assert!(
            execution
                .verify_claude_provider_session_binding(
                    "claude-thread",
                    first_id,
                    Some(first_id),
                    1,
                )
                .await
                .is_err(),
            "stale process generation must not verify"
        );
        assert_eq!(
            reloaded
                .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
                    thread_binding: new_binding("claude-thread"),
                    proposed_provider_session_id: "01900000-0000-7000-8000-000000000098".to_owned(),
                    force_new: false,
                })
                .await
                .expect("prepare verified identity")
                .mode,
            PreparedClaudeProviderSessionMode::Resume
        );

        let replacement_id = "01900000-0000-7000-8000-000000000097";
        let replacement = reloaded
            .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
                thread_binding: new_binding("claude-thread"),
                proposed_provider_session_id: replacement_id.to_owned(),
                force_new: true,
            })
            .await
            .expect("stale Claude context should rotate to a fresh durable provider session");
        assert_eq!(replacement.mode, PreparedClaudeProviderSessionMode::New);
        assert_eq!(replacement.binding.native_thread_id, replacement_id);
        let replacement_provider = replacement
            .binding
            .provider_session
            .expect("replacement provider metadata");
        assert_eq!(replacement_provider.provider_session_id, replacement_id);
        assert_eq!(
            replacement_provider.lifecycle,
            CliRuntimeProviderSessionLifecycle::Prepared
        );
        assert_eq!(
            replacement_provider.last_verified_process_generation, None,
            "a new provider conversation must not inherit verification from the stale process"
        );

        for (thread_id, emitted) in [
            ("claude-missing", None),
            (
                "claude-mismatch",
                Some("01900000-0000-7000-8000-000000000077"),
            ),
        ] {
            let expected = if thread_id == "claude-missing" {
                "01900000-0000-7000-8000-000000000010"
            } else {
                "01900000-0000-7000-8000-000000000011"
            };
            reloaded
                .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
                    thread_binding: new_binding(thread_id),
                    proposed_provider_session_id: expected.to_owned(),
                    force_new: false,
                })
                .await
                .expect("prepare rejection fixture");
            assert!(
                execution
                    .verify_claude_provider_session_binding(thread_id, expected, emitted, 3,)
                    .await
                    .is_err()
            );
            let invalid = reloaded
                .get_cli_runtime_thread_binding(thread_id)
                .await
                .expect("read invalid fixture")
                .expect("invalid fixture")
                .provider_session
                .expect("invalid provider binding");
            assert_eq!(
                invalid.lifecycle,
                CliRuntimeProviderSessionLifecycle::Invalid
            );
            assert!(
                reloaded
                    .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
                        thread_binding: new_binding(thread_id),
                        proposed_provider_session_id: expected.to_owned(),
                        force_new: false,
                    })
                    .await
                    .is_err(),
                "invalid identity must not be silently replaced"
            );
        }

        assert!(
            reloaded
                .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
                    thread_binding: new_binding("claude-invalid-uuid"),
                    proposed_provider_session_id: String::new(),
                    force_new: false,
                })
                .await
                .is_err()
        );

        reloaded
            .upsert_cli_runtime_thread_binding(new_binding("claude-legacy-without-real-id"))
            .await
            .expect("legacy Claude binding fixture");
        assert!(
            reloaded
                .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
                    thread_binding: new_binding("claude-legacy-without-real-id"),
                    proposed_provider_session_id: "01900000-0000-7000-8000-000000000012".to_owned(),
                    force_new: false,
                })
                .await
                .is_err(),
            "legacy synthetic binding must not be silently treated as durable continuity"
        );
        let recovered_legacy = reloaded
            .prepare_claude_provider_session_binding(PrepareClaudeProviderSessionBinding {
                thread_binding: new_binding("claude-legacy-without-real-id"),
                proposed_provider_session_id: "01900000-0000-7000-8000-000000000013".to_owned(),
                force_new: true,
            })
            .await
            .expect("explicit history bootstrap should replace a legacy Claude binding");
        assert_eq!(
            recovered_legacy.mode,
            PreparedClaudeProviderSessionMode::New
        );
        assert_eq!(
            recovered_legacy.binding.native_thread_id,
            "01900000-0000-7000-8000-000000000013"
        );
        Ok(())
    })
    .await
    .unwrap();
}
