use super::*;
use crate::store::events::turn_projector::TurnProjector;

#[tokio::test]
async fn materialize_turn_start_without_prompt_manifest_persists_turn() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_000000000000000001".to_owned(),
            id: "thr_000000000000000001".to_owned(),
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
            id: "turn_000000000000000001".to_owned(),
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
        let input = vec![UserInput::Text {
            text: "hello".to_owned(),
            text_elements: Vec::new(),
        }];
        let principal_actor = PersistedActorRef::Principal(
            PrincipalId::new("P00000000000000000001").expect("valid principal id"),
        );

        let missing_actor_error = store
            .materialize_turn_event(
                crate::events::TurnEventPayload::TurnStarted(
                    crate::events::TurnStartedEventPayload {
                        thread: thread.clone(),
                        sandbox_mode: SandboxMode::FullAccess,
                        turn: turn.clone(),
                        input: input.clone(),
                        actor: None,
                        reasoning_effort: None,
                        work_owner: Default::default(),
                    },
                ),
                timestamp,
            )
            .await
            .expect_err("normal materialization must reject a missing actor");
        assert!(
            missing_actor_error
                .to_string()
                .contains("missing its required actor")
        );

        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                input.as_slice(),
                principal_actor.clone(),
            )
            .await
            .expect("turn start without manifest should persist");

        let (workspace_id, fetched_turn) = store
            .get_turn(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("must read turn")
            .expect("turn should exist");

        assert_eq!(workspace_id, thread.workspace_id);
        assert_eq!(fetched_turn.id, turn.id);
        assert_eq!(fetched_turn.status, TurnStatus::InProgress);
        assert_eq!(fetched_turn.prompt_manifest, None);
        let permission_profile = &fetched_turn.permission_profile;
        assert_eq!(permission_profile.mode, TurnPermissionMode::FullAccess);
        assert_eq!(
            permission_profile.source,
            TurnPermissionProfileSource::Defaulted
        );
        assert_eq!(
            permission_profile.effective_policy,
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        );

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
            .one(&connection)
            .await
            .expect("must query persisted turn")
            .expect("persisted turn should exist");
        assert_eq!(persisted_turn.prompt_manifest_json, "{}");
        assert_eq!(persisted_turn.reasoning_effort, None);
        assert_eq!(
            persisted_turn.permission_profile_mode.as_deref(),
            Some("full_access")
        );
        assert_eq!(
            persisted_turn.permission_profile_source.as_deref(),
            Some("defaulted")
        );
        assert!(persisted_turn.permission_profile_snapshot_json.is_some());
        assert_eq!(
            persisted_turn.initiated_by_actor_kind.as_deref(),
            Some("principal")
        );
        assert_eq!(
            persisted_turn.initiated_by_actor_id.as_deref(),
            Some("P00000000000000000001")
        );

        let persisted_thread = pioneer_entity::thread::Entity::find_by_id(thread.id.clone())
            .one(&connection)
            .await
            .expect("must query persisted thread")
            .expect("persisted thread should exist");
        assert_eq!(
            persisted_thread.created_by_actor_kind.as_deref(),
            Some("principal")
        );
        assert_eq!(
            persisted_thread.created_by_actor_id.as_deref(),
            Some("P00000000000000000001")
        );

        let persisted_event = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
            .one(&connection)
            .await
            .expect("must query persisted turn/start event")
            .expect("turn/start event should exist");
        let decoded_event: crate::events::TurnEventPayload = serde_json::from_value(
            read_fixture_event_payload(&store.connection, &persisted_event)
                .await
                .unwrap(),
        )
        .expect("persisted turn/start event should decode");
        let crate::events::TurnEventPayload::TurnStarted(decoded_started) = decoded_event else {
            panic!("expected persisted turn/start event");
        };
        assert_eq!(decoded_started.actor, Some(principal_actor));

        let system_turn = Turn {
            id: "turn_000000000000000002".to_owned(),
            ..turn.clone()
        };
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &system_turn,
                &[],
                PersistedActorRef::System,
            )
            .await
            .expect("System turn should persist");

        let persisted_thread = pioneer_entity::thread::Entity::find_by_id(thread.id.clone())
            .one(&connection)
            .await
            .expect("must query persisted thread after second turn")
            .expect("persisted thread should exist");
        assert_eq!(
            persisted_thread.created_by_actor_kind.as_deref(),
            Some("principal"),
            "a later turn must not overwrite the existing thread creator"
        );
        assert_eq!(
            persisted_thread.created_by_actor_id.as_deref(),
            Some("P00000000000000000001")
        );
        let persisted_system_turn =
            pioneer_entity::turn::Entity::find_by_id(system_turn.id.clone())
                .one(&connection)
                .await
                .expect("must query persisted System turn")
                .expect("persisted System turn should exist");
        assert_eq!(
            persisted_system_turn.initiated_by_actor_kind.as_deref(),
            Some("system")
        );
        assert_eq!(persisted_system_turn.initiated_by_actor_id, None);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn fresh_projection_refuses_actorless_turn_start_before_and_after_identity_bootstrap() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_legacy_actor".to_owned(),
            id: "thr_legacy_actor".to_owned(),
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
            id: "turn_legacy_actor".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
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
        let payload =
            crate::events::TurnEventPayload::TurnStarted(crate::events::TurnStartedEventPayload {
                thread: thread.clone(),
                sandbox_mode: SandboxMode::FullAccess,
                turn: turn.clone(),
                input: Vec::new(),
                actor: None,
                reasoning_effort: None,
                work_owner: Default::default(),
            });
        let legacy_event = crate::events::AppendedTurnEvent {
            id: "event_legacy_actor".to_owned(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            sequence: 1,
            payload,
            idempotency_key: None,
            was_inserted: true,
            created_at: unix_to_datetime(timestamp),
        };

        for initialized in [false, true] {
            if initialized {
                let gateway_id =
                    GatewayId::new("G00000000000000000001").expect("gateway id should be valid");
                let principal_id = PrincipalId::new("P00000000000000000001")
                    .expect("principal id should be valid");
                crate::create_gateway_singleton(
                    &connection,
                    &gateway_id,
                    1,
                    unix_to_datetime(timestamp),
                )
                .await
                .expect("Gateway identity should persist");
                crate::create_superuser(
                    &connection,
                    &principal_id,
                    &gateway_id,
                    "Superuser",
                    "superuser",
                    "superuser",
                    unix_to_datetime(timestamp),
                )
                .await
                .expect("Superuser should persist");
            }
            let error = TurnProjector::new()
                .project(&connection, &legacy_event)
                .await
                .expect_err("projection must not infer a missing actor");
            assert!(error.to_string().contains("turn/start requires an actor"));
            assert!(
                pioneer_entity::thread::Entity::find_by_id(&thread.id)
                    .one(&connection)
                    .await?
                    .is_none()
            );
            assert!(
                pioneer_entity::turn::Entity::find_by_id(&turn.id)
                    .one(&connection)
                    .await?
                    .is_none()
            );
            assert!(
                pioneer_entity::turn_event::Entity::find_by_id(&legacy_event.id)
                    .one(&connection)
                    .await?
                    .is_none()
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn terminal_turn_projection_rejects_missing_turn_start_without_partial_writes() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();
        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let turn_id = "turn_terminal_without_start";
        let mut terminal_turn = sample_turn(turn_id);
        terminal_turn.status = TurnStatus::Completed;

        let error = store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: "ws_terminal_without_start".to_owned(),
                    thread_id: "thr_terminal_without_start".to_owned(),
                    turn: terminal_turn,
                },
                timestamp,
            )
            .await
            .expect_err("terminal projection without turn/start must fail closed");
        let error = format!("{error:#}");
        assert!(
            error.contains("refusing to append orphan `turn/completed` event")
                && error.contains("missing turn `turn_terminal_without_start`")
                && error.contains("thread `thr_terminal_without_start`"),
            "unexpected terminal projection error: {error}"
        );

        assert!(
            pioneer_entity::turn::Entity::find_by_id(turn_id)
                .one(&connection)
                .await
                .expect("turn lookup should succeed")
                .is_none(),
            "failed terminal projection must not insert an actorless turn"
        );
        assert!(
            pioneer_entity::turn_event::Entity::find()
                .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn_id))
                .one(&connection)
                .await
                .expect("turn event lookup should succeed")
                .is_none(),
            "orphan terminal event must be rejected before append"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn normal_writes_refuse_to_repair_actorless_legacy_rows_implicitly() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_actorless_normal_write").await;
        let timestamp = 1_700_000_000;
        let created_at = unix_to_datetime(timestamp);

        let actorless_thread = sample_thread(
            "ws_actorless_normal_write",
            "thr_actorless_normal",
            timestamp,
        );
        let seed = store.connection.begin().await?;
        crate::store::conversations::thread_writes::upsert_thread(
            &seed,
            &actorless_thread,
            created_at,
            created_at,
        )
        .await
        .expect("explicit legacy seam should create actorless thread fixture");
        seed.commit().await?;

        let thread_error = store
            .upsert_thread_model(&actorless_thread, PersistedActorRef::System)
            .await
            .expect_err("normal thread write must not repair missing creator implicitly");
        assert!(
            thread_error
                .to_string()
                .contains("is missing its persisted creator")
        );

        let attributed_thread =
            sample_thread("ws_actorless_normal_write", "thr_actorless_turn", timestamp);
        store
            .upsert_thread_model(&attributed_thread, PersistedActorRef::System)
            .await
            .expect("normal thread fixture should carry its creator");
        let actorless_turn = sample_turn("turn_actorless_normal");
        let seed = store.connection.begin().await?;
        crate::store::conversations::turn_writes::upsert_turn(
            &seed,
            actorless_turn.id.as_str(),
            attributed_thread.id.as_str(),
            &actorless_turn,
            None,
            None,
            created_at,
            created_at,
        )
        .await
        .expect("explicit legacy seam should create actorless turn fixture");
        seed.commit().await?;

        let turn_error = store
            .materialize_turn_start(
                &attributed_thread,
                SandboxMode::FullAccess,
                &actorless_turn,
                &[],
                PersistedActorRef::System,
            )
            .await
            .expect_err("normal turn write must not repair missing initiator implicitly");
        assert!(
            turn_error
                .to_string()
                .contains("is missing its persisted initiator")
        );
        let persisted_actorless_turn =
            pioneer_entity::turn::Entity::find_by_id(actorless_turn.id.as_str())
                .one(&store.connection)
                .await
                .expect("turn lookup should succeed")
                .expect("legacy actorless turn fixture should remain present");
        assert_eq!(persisted_actorless_turn.initiated_by_actor_kind, None);
        assert_eq!(persisted_actorless_turn.initiated_by_actor_id, None);

        let event = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(actorless_turn.id.as_str()))
            .one(&store.connection)
            .await
            .expect("turn event lookup should succeed")
            .expect("rejected projection must preserve its append-only event");
        let projection_state =
            pioneer_entity::turn_event_projection_state::Entity::find_by_id(event.id)
                .one(&store.connection)
                .await
                .expect("projection state lookup should succeed")
                .expect("rejected projection must preserve its failure state");
        assert_eq!(
            projection_state.status,
            pioneer_crud_conversations::event_projection::PROJECTION_STATUS_FAILED
        );
        assert!(
            projection_state
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("is missing its persisted initiator")),
            "projection failure must retain the actorless legacy-row diagnostic"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn canonical_turn_start_decode_and_replay_preserve_actor_without_rewriting_history() {
    pioneer_postgres_test::with_database(async |fixture| {
        let principal = PrincipalId::new("P00000000000000000001")
            .expect("canonical replay principal id should be valid");
        for (suffix, actor, expected_kind, expected_id) in [
            (
                "principal",
                PersistedActorRef::Principal(principal),
                "principal",
                Some("P00000000000000000001"),
            ),
            ("system", PersistedActorRef::System, "system", None),
        ] {
            let connection = fixture.connection().clone();
            let store = CrudStore::new(connection);
            let connection = store.database_connection();
            let timestamp = 1_700_000_000;
            let thread_id = format!("thr_replay_actor_{suffix}");
            let turn_id = format!("turn_replay_actor_{suffix}");
            let thread = sample_thread("ws_replay_actor", thread_id.as_str(), timestamp);
            let turn = sample_turn(turn_id.as_str());
            let payload = crate::events::TurnEventPayload::TurnStarted(
                crate::events::TurnStartedEventPayload {
                    thread: thread.clone(),
                    sandbox_mode: SandboxMode::FullAccess,
                    turn: turn.clone(),
                    input: Vec::new(),
                    actor: Some(actor.clone()),
                    reasoning_effort: None,
                    work_owner: Default::default(),
                },
            );

            let appended = pioneer_crud_execution::event_append::append_event(
                &connection,
                &payload,
                unix_to_datetime(timestamp),
            )
            .await
            .expect("raw canonical event should append");
            let raw_before = pioneer_entity::turn_event::Entity::find_by_id(appended.id.as_str())
                .one(&connection)
                .await
                .expect("raw canonical event query should succeed")
                .expect("raw canonical event should exist");
            let replayed = turn_event::find_event_by_id(&connection, &appended.id)
                .await?
                .expect("canonical portable event should decode for replay");
            assert!(raw_before.payload_payload_id.is_some());
            let crate::events::TurnEventPayload::TurnStarted(decoded) = &replayed.payload else {
                panic!("replayed canonical event should be turn/started");
            };
            assert_eq!(decoded.actor.as_ref(), Some(&actor));

            TurnProjector::new()
                .project(&connection, &replayed)
                .await
                .expect("decoded canonical event should replay");

            let persisted_thread = pioneer_entity::thread::Entity::find_by_id(thread.id.as_str())
                .one(&connection)
                .await
                .expect("replayed thread query should succeed")
                .expect("replayed thread should exist");
            assert_eq!(
                persisted_thread.created_by_actor_kind.as_deref(),
                Some(expected_kind)
            );
            assert_eq!(persisted_thread.created_by_actor_id.as_deref(), expected_id);
            let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn.id.as_str())
                .one(&connection)
                .await
                .expect("replayed turn query should succeed")
                .expect("replayed turn should exist");
            assert_eq!(
                persisted_turn.initiated_by_actor_kind.as_deref(),
                Some(expected_kind)
            );
            assert_eq!(persisted_turn.initiated_by_actor_id.as_deref(), expected_id);

            let raw_after = pioneer_entity::turn_event::Entity::find_by_id(appended.id.as_str())
                .one(&connection)
                .await
                .expect("raw canonical event re-query should succeed")
                .expect("raw canonical event should remain");
            assert_eq!(
                raw_after, raw_before,
                "projection replay must not rewrite append-only event payloads"
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn materialize_turn_start_persists_explicit_permission_profile() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_permission_profile".to_owned(),
            id: "thr_permission_profile".to_owned(),
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
        let permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::Supervised,
            TurnPermissionProfileSource::Composer,
        );
        let turn = Turn {
            work_owner: Default::default(),
            id: "turn_permission_profile".to_owned(),
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
            permission_profile: permission_profile.clone(),
        };

        seed_observation_workspace(&store, &thread.workspace_id).await;
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("turn start with profile should persist");

        let (_, fetched_turn) = store
            .get_turn(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("must read turn")
            .expect("turn should exist");
        assert_eq!(fetched_turn.permission_profile, permission_profile);

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
            .one(&connection)
            .await
            .expect("must query persisted turn")
            .expect("persisted turn should exist");
        assert_eq!(
            persisted_turn.permission_profile_mode.as_deref(),
            Some("supervised")
        );
        assert_eq!(
            persisted_turn.permission_profile_source.as_deref(),
            Some("composer")
        );
        assert_eq!(
            serde_json::from_str::<TurnPermissionProfileSnapshot>(
                persisted_turn
                    .permission_profile_snapshot_json
                    .as_deref()
                    .expect("permission profile snapshot should persist")
            )
            .expect("snapshot should decode"),
            permission_profile
        );

        let audit_events_before_explicit_audit = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
            .filter(
                pioneer_entity::turn_event::Column::EventType
                    .eq(pioneer_protocol::constants::events::TURN_PERMISSION_AUDIT),
            )
            .all(&connection)
            .await
            .expect("must query turn permission audit events");
        assert!(
            audit_events_before_explicit_audit.is_empty(),
            "CRUD must not synthesize profile-selected audit events during turn/start"
        );

        store
            .materialize_turn_permission_audit(
                pioneer_protocol::TurnPermissionAuditEvent {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    event_kind: TurnPermissionAuditEventKind::ProfileSelected,
                    profile_mode: permission_profile.mode,
                    profile_source: permission_profile.source,
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
                },
                timestamp,
            )
            .await
            .expect("explicit profile-selected audit should persist");

        let audit_events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
            .filter(
                pioneer_entity::turn_event::Column::EventType
                    .eq(pioneer_protocol::constants::events::TURN_PERMISSION_AUDIT),
            )
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&connection)
            .await
            .expect("must query turn permission audit events");
        assert_eq!(audit_events.len(), 1);
        let audit_payload: crate::events::TurnEventPayload = serde_json::from_value(
            read_fixture_event_payload(&store.connection, &audit_events[0])
                .await
                .unwrap(),
        )
        .expect("audit event payload should decode");
        let crate::events::TurnEventPayload::TurnPermissionAudit(audit) = audit_payload else {
            panic!("expected turn permission audit event");
        };
        assert_eq!(
            audit.event_kind,
            TurnPermissionAuditEventKind::ProfileSelected
        );
        assert_eq!(audit.workspace_id, thread.workspace_id);
        assert_eq!(audit.thread_id, thread.id);
        assert_eq!(audit.turn_id, turn.id);
        assert_eq!(audit.profile_mode, TurnPermissionMode::Supervised);
        assert_eq!(audit.profile_source, TurnPermissionProfileSource::Composer);
        assert!(audit.tool_name.is_none());
        assert!(audit.request_key.is_none());

        let turn_item_events = store
            .get_turn_item_events(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("turn item events should load")
            .expect("turn item events should exist");
        assert!(
            turn_item_events.events.iter().any(|event| matches!(
                &event.payload,
                TurnItemEventPayload::TurnPermissionAudit(audit)
                    if audit.event_kind == TurnPermissionAuditEventKind::ProfileSelected
            )),
            "permission audit should be exposed through turn item event replay"
        );

        let history = store
            .get_thread_history(thread.id.as_str(), Some(16))
            .await
            .expect("thread history should load")
            .expect("thread history should exist");
        assert!(
            history.events.iter().any(|event| matches!(
                &event.payload,
                ThreadHistoryEventPayload::TurnPermissionAudit(audit)
                    if audit.event_kind == TurnPermissionAuditEventKind::ProfileSelected
            )),
            "permission audit should be exposed through thread history"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn materialize_non_executable_system_turn_projects_both_events_atomically() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_atomic_profile".to_owned(),
            id: "thr_atomic_profile".to_owned(),
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
        let permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::Supervised,
            TurnPermissionProfileSource::Composer,
        );
        let turn = Turn {
            work_owner: Default::default(),
            id: "turn_atomic_profile".to_owned(),
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
            permission_profile: permission_profile.clone(),
        };
        let audit = pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: turn.id.clone(),
            event_kind: TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: permission_profile.mode,
            profile_source: permission_profile.source,
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

        let mut executable_turn = turn.clone();
        executable_turn.id = "turn_contextless_execution_rejected".to_owned();
        executable_turn.mode = ThreadMode::Agent;
        let mut executable_audit = audit.clone();
        executable_audit.turn_id = executable_turn.id.clone();
        seed_observation_workspace(&store, &thread.workspace_id).await;
        store
            .materialize_non_executable_system_turn_start_with_permission_audit(
                &thread,
                SandboxMode::FullAccess,
                &executable_turn,
                &[],
                executable_audit,
            )
            .await
            .expect_err("an execution-capable Turn must require an authority envelope");
        assert!(
            pioneer_entity::turn::Entity::find_by_id(executable_turn.id)
                .one(&connection)
                .await
                .expect("rejected Turn lookup should succeed")
                .is_none()
        );

        store
            .materialize_non_executable_system_turn_start_with_permission_audit(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                audit,
            )
            .await
            .expect("turn start and profile audit should persist atomically");

        let events = pioneer_entity::turn_event::Entity::find()
            .filter(pioneer_entity::turn_event::Column::TurnId.eq(turn.id.clone()))
            .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
            .all(&connection)
            .await
            .expect("must query turn events");
        assert!(store.get_turn_execution(&turn.id).await.unwrap().is_none());
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].event_type,
            pioneer_protocol::constants::events::TURN_STARTED
        );
        assert_eq!(
            events[1].event_type,
            pioneer_protocol::constants::events::TURN_PERMISSION_AUDIT
        );

        let projection_states = pioneer_entity::turn_event_projection_state::Entity::find()
            .filter(pioneer_entity::turn_event_projection_state::Column::TurnId.eq(turn.id.clone()))
            .order_by_asc(pioneer_entity::turn_event_projection_state::Column::Sequence)
            .all(&connection)
            .await
            .expect("must query turn event projection states");
        assert_eq!(projection_states.len(), 2);
        assert!(projection_states.iter().all(|state| {
            state.status
                == pioneer_crud_conversations::event_projection::PROJECTION_STATUS_PROJECTED
        }));

        let turn_item_events = store
            .get_turn_item_events(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("turn item events should load")
            .expect("turn item events should exist");
        assert!(turn_item_events.events.iter().any(|event| matches!(
            &event.payload,
            TurnItemEventPayload::TurnPermissionAudit(audit)
                if audit.event_kind == TurnPermissionAuditEventKind::ProfileSelected
        )));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn materialize_turn_start_persists_explicit_reasoning_effort() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = CrudStore::new(connection.clone());
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_reasoning_effort".to_owned(),
            id: "thr_reasoning_effort".to_owned(),
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
            id: "turn_reasoning_effort".to_owned(),
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

        store
            .materialize_turn_start_with_reasoning_effort(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                Some("high"),
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("turn start with reasoning effort should persist");

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn.id.clone())
            .one(&connection)
            .await
            .expect("must query persisted turn")
            .expect("persisted turn should exist");
        assert_eq!(persisted_turn.reasoning_effort.as_deref(), Some("high"));
        Ok(())
    })
    .await
    .unwrap();
}

async fn read_fixture_event_payload(
    db: &pioneer_postgres_pools::PostgresDatabase,
    row: &pioneer_entity::turn_event::Model,
) -> anyhow::Result<serde_json::Value> {
    use anyhow::Context;

    let mut events =
        pioneer_crud_conversations::turn_payload::hydrate_events(db, vec![row.clone()]).await?;
    events
        .pop()
        .expect("one event")
        .payload
        .context("fixture event payload")
}
