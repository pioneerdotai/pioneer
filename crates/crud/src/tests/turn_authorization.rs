use super::*;

#[tokio::test]
async fn authorized_admission_rolls_back_on_owner_failure_and_replay_preserves_receipt() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_owner_admission").await;
        let timestamp = 1_700_000_000;
        let thread = sample_thread("ws_owner_admission", "thr_owner_admission", timestamp);
        let security_snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            "/workspace/owner-admission",
            timestamp * 1000,
        );
        for kind in [
            crate::TurnExecutorKind::NativeAgent,
            crate::TurnExecutorKind::ApiProvider,
            crate::TurnExecutorKind::CliRuntime,
            crate::TurnExecutorKind::AcpRuntime,
        ] {
            let mut turn = sample_turn(&format!("turn_owner_{}", kind.as_str()));
            turn.mode = ThreadMode::Agent;
            let audit = pioneer_protocol::TurnPermissionAuditEvent {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: turn.id.clone(),
                event_kind: TurnPermissionAuditEventKind::ProfileSelected,
                profile_mode: turn.permission_profile.mode,
                profile_source: turn.permission_profile.source,
                security_snapshot_id: None,
                security_snapshot_version: None,
                security_reason_code: None,
                security_capability: None,
                item_id: None,
                tool_call_id: None,
                tool_name: None,
                action_kind: None,
                request_key: None,
                decision: None,
                reason: None,
                cached: false,
            };
            let admission = crate::NewTurnAdmission {
                turn_id: turn.id.clone(),
                thread_id: thread.id.clone(),
                workspace_id: thread.workspace_id.clone(),
                request_digest: "a".repeat(64),
                policy_generation: None,
                role_key: None,
                policy_fingerprint: None,
                execution_lease: None,
            };
            let execution = crate::NewTurnExecution {
                turn_id: turn.id.clone(),
                thread_id: thread.id.clone(),
                workspace_id: thread.workspace_id.clone(),
                executor_kind: kind,
                executor_key: Some("test-executor".to_owned()),
                status: crate::TurnExecutionStatus::Starting,
                owner_id: "admitting-owner".to_owned(),
                lease_until: unix_to_datetime(timestamp + 45),
                created_at: unix_to_datetime(timestamp),
            };
            // Fail at the ownership write, after Turn events, projections and
            // admission have already been written inside the transaction.
            store
                .connection
                .execute_unprepared(
                    "CREATE OR REPLACE FUNCTION reject_admission_owner() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected ownership failure'; END $$; CREATE TRIGGER reject_admission_owner BEFORE INSERT ON turn_execution FOR EACH ROW EXECUTE FUNCTION reject_admission_owner()",
                )
                .await
                .unwrap();
            for reject_owner in [true, false] {
                let result = store
                .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
                    &thread,
                    SandboxMode::FullAccess,
                    &turn,
                    &[UserInput::Text {
                        text: "execute admitted work".to_owned(),
                        text_elements: Vec::new(),
                    }],
                    None,
                    TurnWorkOwner::Turn,
                    PersistedActorRef::System,
                    audit.clone(),
                    r#"{"kind":"test_explicit_authority"}"#,
                    None,
                    Some(admission.clone()),
                    Some(execution.clone()),
                    &security_snapshot,
                    Vec::new(),
                    None,
                    None,
                )
                .await;
                if reject_owner {
                    let error = result.expect_err("owner failure must roll back admission");
                    assert!(format!("{error:#}").contains("injected ownership failure"));
                    assert!(
                        store
                            .get_turn(&thread.id, &turn.id)
                            .await
                            .unwrap()
                            .is_none()
                    );
                    assert!(store.get_turn_admission(&turn.id).await.unwrap().is_none());
                    assert!(store.get_turn_execution(&turn.id).await.unwrap().is_none());
                    assert!(
                        pioneer_entity::turn_event::Entity::find()
                            .filter(pioneer_entity::turn_event::Column::TurnId.eq(&turn.id))
                            .all(&store.connection)
                            .await
                            .unwrap()
                            .is_empty()
                    );
                    assert!(
                        pioneer_entity::turn_event_projection_state::Entity::find()
                            .filter(
                                pioneer_entity::turn_event_projection_state::Column::TurnId
                                    .eq(&turn.id)
                            )
                            .all(&store.connection)
                            .await
                            .unwrap()
                            .is_empty()
                    );
                    store
                        .connection
                        .execute_unprepared("DROP TRIGGER reject_admission_owner ON turn_execution")
                        .await
                        .unwrap();
                } else {
                    result.expect("Turn and ownership should commit together");
                }
            }
            assert!(
                store
                    .get_turn(&thread.id, &turn.id)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(store.get_turn_admission(&turn.id).await.unwrap().is_some());
            let admitted = store.get_turn_execution(&turn.id).await.unwrap().unwrap();
            assert_eq!(admitted.executor_kind, kind);
            assert_eq!(admitted.owner_id, "admitting-owner");
            assert_eq!(admitted.owner_generation, 1);
            let claimed = store
                .claim_expired_turn_execution(
                    &admitted,
                    "replacement-owner",
                    timestamp + 46,
                    timestamp + 91,
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(claimed.owner_generation, 2);
            // Projection-state loss permits replay of sequence 1. It must
            // neither create an owner nor reset the existing ownership CAS.
            mark_first_turn_projection_failed(&store, &turn.id, timestamp + 47).await;
            let replay = store
                .replay_due_turn_event_projections(timestamp + 47, 1)
                .await
                .unwrap();
            assert_eq!(replay.projected, 1);
            assert_eq!(
                store.get_turn_execution(&turn.id).await.unwrap().unwrap(),
                claimed
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stale_policy_generation_rolls_back_started_event_and_projection() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();
        let changed = crate::append_authorization_change(
            &connection,
            pioneer_protocol::AuthorizationChangeKind::RolePolicy,
            pioneer_protocol::AuthorizationChangeScope::Role {
                role_key: pioneer_protocol::RoleKey::member(),
            },
        )
        .await
        .expect("policy revoke must advance generation");
        assert_eq!(changed.policy_generation.get(), 2);
        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_atomic_admission_fault".to_owned(),
            id: "thr_atomic_admission_fault".to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            work_owner: Default::default(),
            id: "turn_atomic_admission_fault".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        let audit = pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            event_kind: TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: turn.permission_profile.mode,
            profile_source: turn.permission_profile.source,
            security_snapshot_id: None,
            security_snapshot_version: None,
            security_reason_code: None,
            security_capability: None,
            item_id: None,
            tool_call_id: None,
            tool_name: None,
            action_kind: None,
            request_key: None,
            decision: None,
            reason: None,
            cached: false,
        };
        let security_snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            "/workspace/atomic-admission",
            timestamp.saturating_mul(1000),
        );

        store
            .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                None,
                TurnWorkOwner::Turn,
                pioneer_protocol::PersistedActorRef::System,
                audit,
                r#"{"kind":"test_explicit_authority"}"#,
                None,
                Some(crate::NewTurnAdmission {
                    turn_id: turn.id.clone(),
                    thread_id: thread.id.clone(),
                    workspace_id: thread.workspace_id.clone(),
                    request_digest: "a".repeat(64),
                    policy_generation: Some(1),
                    role_key: Some("member".to_owned()),
                    policy_fingerprint: Some("a".repeat(64)),
                    execution_lease: None,
                }),
                Some(crate::NewTurnExecution {
                    turn_id: turn.id.clone(),
                    thread_id: thread.id.clone(),
                    workspace_id: thread.workspace_id.clone(),
                    executor_kind: crate::TurnExecutorKind::ApiProvider,
                    executor_key: Some("openai".to_owned()),
                    status: crate::TurnExecutionStatus::Starting,
                    owner_id: "test-owner".to_owned(),
                    lease_until: unix_to_datetime(timestamp + 45),
                    created_at: unix_to_datetime(timestamp),
                }),
                &security_snapshot,
                Vec::new(),
                None,
                None,
            )
            .await
            .expect_err("stale policy admission must abort the whole batch");

        assert!(
            pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
                .one(&connection)
                .await
                .expect("turn lookup should succeed")
                .is_none()
        );
        assert!(
            pioneer_entity::turn_event::Entity::find()
                .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id))
                .all(&connection)
                .await
                .expect("event lookup should succeed")
                .is_empty()
        );
        assert!(
            pioneer_entity::turn_execution::Entity::find()
                .all(&connection)
                .await
                .expect("Turn execution lookup should succeed")
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn turn_execution_security_snapshot_roundtrips_unrestricted_snapshot() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) = postgres_store_with_started_turn(
            fixture,
            "ws_security_unrestricted",
            "thr_security_unrestricted",
            "turn_security_unrestricted",
        )
        .await;
        let snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            "/workspace/security-unrestricted",
            1_700_000_000_000,
        );

        let updated = store
            .set_turn_execution_security_snapshot(turn.id.as_str(), &snapshot)
            .await
            .expect("security snapshot update should succeed");
        assert!(updated, "existing turn must be updated");

        let record = store
            .get_turn_execution_security_snapshot(turn.id.as_str())
            .await
            .expect("security snapshot read should succeed")
            .expect("security snapshot should exist");
        assert_eq!(record.turn_id, turn.id);
        assert_eq!(record.version, 1);
        assert_eq!(record.snapshot, snapshot);

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
            .one(&store.connection)
            .await
            .expect("turn query should succeed")
            .expect("turn should exist");
        assert_eq!(persisted_turn.execution_security_snapshot_version, Some(1));
        let persisted_snapshot = serde_json::from_str::<TurnExecutionSecuritySnapshot>(
            persisted_turn
                .execution_security_snapshot_json
                .as_deref()
                .expect("security snapshot json should persist"),
        )
        .expect("security snapshot json should decode");
        assert_eq!(
            persisted_snapshot.sandbox.mode,
            TurnSandboxMode::Unrestricted
        );
        assert_eq!(persisted_snapshot, snapshot);
        let budget = pioneer_protocol::NativeContextBudget {
            service_mode: None,
            instance: thread.model_provider.clone(),
            authority: Some("account-a".into()),
            model: thread.model.clone(),
            manual_window: None,
            context_window: 272000,
            max_input: None,
            max_output: Some(16000),
        };
        assert!(
            !store
                .pin_turn_native_context("other-workspace", &turn.id, &budget, None)
                .await
                .unwrap()
        );
        assert!(
            store
                .pin_turn_native_context("ws_security_unrestricted", &turn.id, &budget, None)
                .await
                .unwrap()
        );
        let mut wider = budget.clone();
        wider.context_window = 1050000;
        assert!(
            !store
                .pin_turn_native_context("ws_security_unrestricted", &turn.id, &wider, None)
                .await
                .unwrap()
        );
        let mut expected = snapshot;
        expected.native_context = Some(budget);
        let pinned = store
            .get_turn_execution_security_snapshot(&turn.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pinned.snapshot, expected);
        let mut legacy_plan = wider.clone();
        legacy_plan.service_mode = Some(pioneer_protocol::NativeServiceModePlan {
            id: "standard".into(),
            parameters: pioneer_protocol::ServiceModeParameters::OpenAiStandard,
        });
        assert!(
            store
                .pin_turn_native_context("ws_security_unrestricted", &turn.id, &legacy_plan, None)
                .await
                .unwrap()
        );
        let pinned_mode = store
            .get_turn_execution_security_snapshot(&turn.id)
            .await
            .unwrap()
            .unwrap()
            .snapshot;
        assert_eq!(
            pinned_mode.native_context.as_ref().unwrap().context_window,
            272000
        );
        expected.native_context.as_mut().unwrap().service_mode = legacy_plan.service_mode.clone();
        assert_eq!(pinned_mode, expected);
        legacy_plan.service_mode.as_mut().unwrap().id = "pro".into();
        legacy_plan.service_mode.as_mut().unwrap().parameters =
            pioneer_protocol::ServiceModeParameters::OpenAiPro;
        assert!(
            !store
                .pin_turn_native_context("ws_security_unrestricted", &turn.id, &legacy_plan, None)
                .await
                .unwrap()
        );
        // A saved user choice projects through the normal thread read on restart.
        let manual = expected.native_context.as_mut().unwrap();
        manual.service_mode = legacy_plan.service_mode.clone();
        manual.manual_window = Some(1050000);
        manual.context_window = 1050000;
        store
            .set_turn_execution_security_snapshot(&turn.id, &expected)
            .await
            .unwrap();
        let restored = store.get_thread_model(&thread.id).await.unwrap().unwrap();
        assert_eq!(restored.context_window, Some(1050000));
        assert_eq!(
            restored.service_mode.as_ref().map(|mode| mode.id.as_str()),
            Some("pro")
        );
        assert_eq!(
            restored.context_authority,
            Some(pioneer_protocol::native_context_choice_authority(
                "account-a",
                &thread.model
            ))
        );
        expected.native_context.as_mut().unwrap().manual_window = None;
        store
            .set_turn_execution_security_snapshot(&turn.id, &expected)
            .await
            .unwrap();
        let reset = store.get_thread_model(&thread.id).await.unwrap().unwrap();
        assert_eq!(
            reset.service_mode.as_ref().map(|mode| mode.id.as_str()),
            Some("pro")
        );
        assert_eq!(
            (reset.context_window, reset.context_authority),
            (None, None)
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn security_snapshot_audit_decision_persists_snapshot_linkage() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, thread, turn) = postgres_store_with_started_turn(
            fixture,
            "ws_security_audit_link",
            "thr_security_audit_link",
            "turn_security_audit_link",
        )
        .await;
        let snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access(
            "/workspace/security-audit-link",
            1_700_000_000_000,
        );
        store
            .set_turn_execution_security_snapshot(turn.id.as_str(), &snapshot)
            .await
            .expect("security snapshot update should succeed");

        let audit = pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            event_kind: TurnPermissionAuditEventKind::DecisionDenied,
            profile_mode: TurnPermissionMode::Supervised,
            profile_source: TurnPermissionProfileSource::Composer,
            security_snapshot_id: Some(snapshot.audit_id(turn.id.as_str())),
            security_snapshot_version: Some(snapshot.version),
            security_reason_code: None,
            security_capability: None,
            item_id: Some("item_security_audit".to_owned()),
            tool_call_id: Some("call_security_audit".to_owned()),
            tool_name: Some("read_file".to_owned()),
            action_kind: Some(pioneer_protocol::TurnPermissionActionKind::FileRead),
            request_key: None,
            decision: Some(pioneer_protocol::TurnPermissionAuditDecision::Deny),
            reason: Some(pioneer_protocol::TurnPermissionDecisionReason::SandboxDenied),
            cached: false,
        };
        store
            .materialize_turn_permission_audit(audit.clone(), 1_700_000_002)
            .await
            .expect("security audit event should persist");

        let turn_items = store
            .get_turn_item_events(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("turn events should load")
            .expect("turn events should exist");
        let persisted = turn_items
            .events
            .iter()
            .find_map(|event| match &event.payload {
                TurnItemEventPayload::TurnPermissionAudit(event)
                    if event.event_kind == TurnPermissionAuditEventKind::DecisionDenied =>
                {
                    Some(event)
                }
                _ => None,
            })
            .expect("decision audit event should be projected");

        assert_eq!(
            persisted.security_snapshot_id.as_deref(),
            audit.security_snapshot_id.as_deref()
        );
        assert_eq!(persisted.security_snapshot_version, Some(1));
        assert_eq!(
            persisted.reason,
            Some(pioneer_protocol::TurnPermissionDecisionReason::SandboxDenied)
        );
        Ok(())
    })
    .await
    .unwrap();
}
