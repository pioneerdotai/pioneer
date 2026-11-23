use super::*;

#[tokio::test]
async fn concurrent_first_messages_share_one_thread_and_read_it_in_their_transactions() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_atomic_first_message").await;
        let connection = store.database_connection();
        let timestamp = 1_700_000_000;
        let principal_id =
            PrincipalId::new("P00000000000000000001").expect("principal id should be valid");
        let gateway_id =
            GatewayId::new("G00000000000000000001").expect("gateway id should be valid");
        let now = unix_to_datetime(timestamp);
        create_gateway_singleton(&connection, &gateway_id, 0, now)
            .await
            .expect("gateway identity should persist");
        create_superuser(
            &connection,
            &principal_id,
            &gateway_id,
            "Author",
            "author",
            "author",
            now,
        )
        .await
        .expect("author principal should persist");

        let actor = PersistedActorRef::Principal(principal_id.clone());
        let author = pioneer_protocol::TurnAuthorSnapshot {
            cli_runtime_kind: None,
            actor: actor.clone(),
            display_name: "Author".to_owned(),
            nickname: "author".to_owned(),
            avatar_revision: None,
            agent: None,
        };
        let mut thread = sample_thread(
            "ws_atomic_first_message",
            "thr_atomic_first_message",
            timestamp,
        );
        thread.mode = ThreadMode::Message;
        thread.status = ThreadStatus::Idle;
        thread.visibility = Some(pioneer_protocol::ThreadVisibility::Workspace);
        thread.preview = "first message".to_owned();
        let mut started = sample_turn("turn_atomic_first_message");
        started.mode = ThreadMode::Message;
        started.author = Some(author.clone());
        thread.turns.push(started.clone());
        let mut completed = started.clone();
        completed.status = TurnStatus::Completed;
        let input = vec![UserInput::Text {
            text: "first message".to_owned(),
            text_elements: Vec::new(),
        }];
        let audit_event = pioneer_protocol::TurnPermissionAuditEvent {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: started.id.clone(),
            event_kind: TurnPermissionAuditEventKind::ProfileSelected,
            profile_mode: started.permission_profile.mode,
            profile_source: started.permission_profile.source,
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

        let second_store = CrudStore::new(pioneer_postgres_pools::PostgresDatabase::new(
            fixture.connect_read_only().await?,
            fixture.connect().await?,
        ));
        let blocker = fixture.connection().begin().await?;
        pioneer_crud_conversations::thread_state::lock_thread_creation(&blocker, &thread.id)
            .await?;
        let check = fixture.connect().await?;

        let mut second_started = started.clone();
        second_started.id = "turn_concurrent_first_message".to_owned();
        let mut second_completed = second_started.clone();
        second_completed.status = TurnStatus::Completed;
        let mut second_thread = thread.clone();
        second_thread.turns = vec![second_started.clone()];
        let mut second_audit = audit_event.clone();
        second_audit.turn_id = second_started.id.clone();

        let first_write = store
            .materialize_new_superuser_completed_message_turn_with_permission_audit(
                CompletedMessageTurnWrite {
                    thread: &thread,
                    sandbox_mode: SandboxMode::FullAccess,
                    started_turn: &started,
                    input: input.as_slice(),
                    actor: actor.clone(),
                    completed: TurnCompletedNotification {
                        workspace_id: thread.workspace_id.clone(),
                        thread_id: thread.id.clone(),
                        turn: completed,
                    },
                    audit_event,
                },
                crate::PersistedThreadAccessClass::Workspace,
            );
        let second_write = second_store
            .materialize_new_superuser_completed_message_turn_with_permission_audit(
                CompletedMessageTurnWrite {
                    thread: &second_thread,
                    sandbox_mode: SandboxMode::FullAccess,
                    started_turn: &second_started,
                    input: input.as_slice(),
                    actor: actor.clone(),
                    completed: TurnCompletedNotification {
                        workspace_id: second_thread.workspace_id.clone(),
                        thread_id: second_thread.id.clone(),
                        turn: second_completed,
                    },
                    audit_event: second_audit,
                },
                crate::PersistedThreadAccessClass::Workspace,
            );
        let release_after_both_wait = async {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let row = check
                        .query_one_raw(Statement::from_string(
                            DatabaseBackend::Postgres,
                            "SELECT count(*) AS waiters FROM pg_locks
                         WHERE locktype = 'advisory' AND NOT granted AND database =
                             (SELECT oid FROM pg_database WHERE datname = current_database())",
                        ))
                        .await?
                        .expect("one lock count");
                    if row.try_get::<i64>("", "waiters")? == 2 {
                        return Ok::<_, sea_orm::DbErr>(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await??;
            blocker.commit().await?;
            Ok::<_, anyhow::Error>(())
        };
        // The two pools reach the same PostgreSQL lock before either can
        // observe a thread row. Both writes must commit after it is released.
        let (first, second, released) =
            tokio::join!(first_write, second_write, release_after_both_wait);
        released?;
        first.expect("first Message and thread should commit atomically");
        second.expect("other first Message should attach to the same thread");

        let persisted_thread = pioneer_entity::thread::Entity::find_by_id(thread.id.as_str())
            .one(&connection)
            .await
            .expect("new thread should load")
            .expect("new thread should exist");
        assert_eq!(
            persisted_thread.created_by_actor_kind.as_deref(),
            Some("principal")
        );
        assert_eq!(
            persisted_thread.created_by_actor_id.as_deref(),
            Some(principal_id.as_str())
        );
        for accepted in [&started, &second_started] {
            let (_, persisted_turn) = store
                .get_turn(thread.id.as_str(), accepted.id.as_str())
                .await
                .expect("committed Message should load")
                .expect("committed Message should exist");
            assert_eq!(persisted_turn.status, TurnStatus::Completed);
            assert_eq!(persisted_turn.author.as_ref(), Some(&author));
            assert_eq!(
                crate::find_turn_initiator(&connection, accepted.id.as_str())
                    .await
                    .expect("Message initiator should load"),
                Some(actor.clone())
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn first_completed_message_observes_its_thread_created_in_the_same_transaction() {
    let store = test_store_with_workspace("ws_atomic_first_message").await;
    let connection = store.database_connection();
    let timestamp = 1_700_000_000;
    let principal_id =
        PrincipalId::new("P00000000000000000001").expect("principal id should be valid");
    let gateway_id = GatewayId::new("G00000000000000000001").expect("gateway id should be valid");
    let now = unix_to_datetime(timestamp);
    create_gateway_singleton(&connection, &gateway_id, 0, now)
        .await
        .expect("gateway identity should persist");
    create_superuser(
        &connection,
        &principal_id,
        &gateway_id,
        "Author",
        "author",
        "author",
        now,
    )
    .await
    .expect("author principal should persist");

    let actor = PersistedActorRef::Principal(principal_id.clone());
    let author = pioneer_protocol::TurnAuthorSnapshot {
        cli_runtime_kind: None,
        actor: actor.clone(),
        display_name: "Author".to_owned(),
        nickname: "author".to_owned(),
        avatar_revision: None,
        agent: None,
    };
    let mut thread = sample_thread(
        "ws_atomic_first_message",
        "thr_atomic_first_message",
        timestamp,
    );
    thread.mode = ThreadMode::Message;
    thread.status = ThreadStatus::Idle;
    thread.visibility = Some(pioneer_protocol::ThreadVisibility::Workspace);
    thread.preview = "first message".to_owned();
    let mut started = sample_turn("turn_atomic_first_message");
    started.mode = ThreadMode::Message;
    started.author = Some(author.clone());
    thread.turns.push(started.clone());
    let mut completed = started.clone();
    completed.status = TurnStatus::Completed;
    let input = vec![UserInput::Text {
        text: "first message".to_owned(),
        text_elements: Vec::new(),
    }];
    let audit_event = pioneer_protocol::TurnPermissionAuditEvent {
        workspace_id: thread.workspace_id.clone(),
        thread_id: thread.id.clone(),
        turn_id: started.id.clone(),
        event_kind: TurnPermissionAuditEventKind::ProfileSelected,
        profile_mode: started.permission_profile.mode,
        profile_source: started.permission_profile.source,
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

    store
        .materialize_new_superuser_completed_message_turn_with_permission_audit(
            CompletedMessageTurnWrite {
                thread: &thread,
                sandbox_mode: SandboxMode::FullAccess,
                started_turn: &started,
                input: input.as_slice(),
                actor: actor.clone(),
                completed: TurnCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn: completed,
                },
                audit_event,
            },
            crate::PersistedThreadAccessClass::Workspace,
        )
        .await
        .expect("first Message and its thread should commit atomically");

    let persisted_thread = pioneer_entity::thread::Entity::find_by_id(thread.id.as_str())
        .one(&connection)
        .await
        .expect("new thread should load")
        .expect("new thread should exist");
    assert_eq!(
        persisted_thread.created_by_actor_kind.as_deref(),
        Some("principal")
    );
    assert_eq!(
        persisted_thread.created_by_actor_id.as_deref(),
        Some(principal_id.as_str())
    );
    let (_, persisted_turn) = store
        .get_turn(thread.id.as_str(), started.id.as_str())
        .await
        .expect("first Message should load")
        .expect("first Message should exist");
    assert_eq!(persisted_turn.status, TurnStatus::Completed);
    assert_eq!(persisted_turn.author.as_ref(), Some(&author));
    assert_eq!(
        crate::find_turn_initiator(&connection, started.id.as_str())
            .await
            .expect("first Message initiator should load"),
        Some(actor)
    );
}

#[tokio::test]
async fn atomic_batch_without_graph_invalidates_consumers_only_on_change() {
    pioneer_postgres_test::with_database(async |fixture| {
        let (store, _, _) = postgres_store_with_started_turn(
            fixture,
            "ws_atomic_wake",
            "thr_atomic_wake",
            "turn_atomic_wake",
        )
        .await;
        let database = store.database_connection();
        // Independent store wrappers and consumers share the runtime signals.
        let writer = CrudStore::new(database.clone());
        let mut frozen = database.work_signal("maintenance.frozen").subscribe();
        frozen.begin_pass(&database).await.unwrap();
        let mut html = database.work_signal("maintenance.html").subscribe();
        html.begin_pass(&database).await.unwrap();
        let mut consumers = ["gateway.control", "projection", "compaction"]
            .map(|domain| database.work_signal(domain).subscribe());
        for consumer in &mut consumers {
            consumer.begin_pass(&database).await.unwrap();
        }
        let mut cleanup = [
            "maintenance.native",
            "maintenance.receipts",
            "maintenance.zstd",
        ]
        .map(|domain| database.work_signal(domain).subscribe());
        for consumer in &mut cleanup {
            consumer.begin_pass(&database).await.unwrap();
        }
        let event = TurnEventPayload::ItemCompleted(ItemCompletedNotification {
            workspace_id: "ws_atomic_wake".into(),
            thread_id: "thr_atomic_wake".into(),
            turn_id: "turn_atomic_wake".into(),
            item: TurnItem::AgentMessage {
                id: "atomic_wake_message".into(),
                text: "durable message".into(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            },
        });
        writer
            .materialize_turn_events_atomically(vec![event.clone()], 1_700_000_100)
            .await
            .unwrap();
        for consumer in &mut consumers {
            assert!(
                futures_util::FutureExt::now_or_never(consumer.wait(None)).is_some(),
                "ordinary committed batches must invalidate each independent consumer"
            );
            consumer.begin_pass(&database).await.unwrap();
        }
        writer
            .materialize_turn_events_atomically(vec![event.clone()], 1_700_000_100)
            .await
            .unwrap();
        for consumer in &mut consumers {
            assert!(
                futures_util::FutureExt::now_or_never(consumer.wait(None)).is_none(),
                "an already projected duplicate must let consumers remain parked"
            );
        }
        for consumer in &mut cleanup {
            assert!(
                futures_util::FutureExt::now_or_never(consumer.wait(None)).is_none(),
                "running item writes and duplicate ACKs must leave cleanup parked"
            );
        }
        assert!(
            futures_util::FutureExt::now_or_never(frozen.wait(None)).is_none(),
            "ordinary item projection does not mutate a frozen root or operation"
        );
        assert!(
            futures_util::FutureExt::now_or_never(html.wait(None)).is_none(),
            "item writes do not change HTML ownership"
        );
        let (_, mut terminal) = writer
            .get_turn("thr_atomic_wake", "turn_atomic_wake")
            .await
            .unwrap()
            .unwrap();
        terminal.status = TurnStatus::Completed;
        writer
            .materialize_turn_completed(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: "ws_atomic_wake".into(),
                    thread_id: "thr_atomic_wake".into(),
                    turn: terminal,
                },
                1_700_000_101,
            )
            .await
            .unwrap();
        for consumer in &mut cleanup {
            assert!(
                futures_util::FutureExt::now_or_never(consumer.wait(None)).is_some(),
                "terminal commit makes the accumulated receipts eligible"
            );
            consumer.begin_pass(&database).await.unwrap();
        }
        assert!(
            futures_util::FutureExt::now_or_never(html.wait(None)).is_some(),
            "terminal lifecycle transition wakes HTML retirement"
        );
        html.begin_pass(&database).await.unwrap();
        let mut late = event;
        if let TurnEventPayload::ItemCompleted(notification) = &mut late {
            if let TurnItem::AgentMessage { id, .. } = &mut notification.item {
                *id = "late_independent_message".into();
            }
        }
        writer
            .materialize_turn_events_atomically(vec![late], 1_700_000_102)
            .await
            .unwrap();
        for consumer in &mut cleanup {
            assert!(
                futures_util::FutureExt::now_or_never(consumer.wait(None)).is_some(),
                "late terminal-turn projection must not be stranded behind a completed pass"
            );
        }
        assert!(
            futures_util::FutureExt::now_or_never(html.wait(None)).is_none(),
            "a late item does not reopen HTML cleanup for an already terminal Turn"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn atomic_started_completed_batch_preserves_every_supported_initiator_kind() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_atomic_actor_matrix").await;
        let connection = store.database_connection();
        let timestamp = 1_700_000_000;
        let thread = sample_thread(
            "ws_atomic_actor_matrix",
            "thr_atomic_actor_matrix",
            timestamp,
        );
        store
            .upsert_thread_model(&thread, PersistedActorRef::System)
            .await
            .expect("actor-matrix thread should persist");

        let principal_id =
            PrincipalId::new("P00000000000000000001").expect("principal id should be valid");
        let principal_actor = PersistedActorRef::Principal(principal_id.clone());
        let principal_author = pioneer_protocol::TurnAuthorSnapshot {
            cli_runtime_kind: None,
            actor: principal_actor.clone(),
            display_name: "Principal".to_owned(),
            nickname: "principal".to_owned(),
            avatar_revision: None,
            agent: None,
        };
        let execution_id = pioneer_protocol::AgentExecutionId::new("E00000000000000000001")
            .expect("execution id should be valid");
        let agent_actor = PersistedActorRef::AgentExecution(execution_id.clone());
        let agent_author = pioneer_protocol::AgentPresentationSnapshot {
            agent_identity_id: pioneer_protocol::AgentIdentityId::new("I00000000000000000001")
                .expect("agent identity id should be valid"),
            agent_execution_id: execution_id,
            identity_source_kind: pioneer_protocol::AgentIdentitySourceKind::NativeAgent,
            identity_source_revision: 7,
            display_name: "Builder".to_owned(),
            nickname: "builder".to_owned(),
            avatar_revision: None,
            role_label: Some("worker".to_owned()),
        }
        .to_turn_author_snapshot();

        for (index, suffix, actor, author) in [
            (1_i64, "principal", principal_actor, Some(principal_author)),
            (2, "agent", agent_actor, Some(agent_author)),
            (3, "system", PersistedActorRef::System, None),
        ] {
            let mut event_thread = thread.clone();
            event_thread.updated_at = timestamp + index;
            event_thread.preview = format!("message from {suffix}");
            let expected_read_author = match (&actor, &author) {
                (PersistedActorRef::System, None) => Some(pioneer_protocol::TurnAuthorSnapshot {
                    cli_runtime_kind: None,
                    actor: PersistedActorRef::System,
                    display_name: "System".to_owned(),
                    nickname: "system".to_owned(),
                    avatar_revision: None,
                    agent: None,
                }),
                _ => author.clone(),
            };
            let mut started = sample_turn(format!("turn_atomic_{suffix}").as_str());
            started.mode = ThreadMode::Message;
            started.author = author.clone();
            let mut completed = started.clone();
            completed.status = TurnStatus::Completed;
            let audit_event = pioneer_protocol::TurnPermissionAuditEvent {
                workspace_id: event_thread.workspace_id.clone(),
                thread_id: event_thread.id.clone(),
                turn_id: started.id.clone(),
                event_kind: TurnPermissionAuditEventKind::ProfileSelected,
                profile_mode: started.permission_profile.mode,
                profile_source: started.permission_profile.source,
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
            let input = vec![UserInput::Text {
                text: event_thread.preview.clone(),
                text_elements: Vec::new(),
            }];

            store
                .materialize_completed_message_turn_with_permission_audit(
                    CompletedMessageTurnWrite {
                        thread: &event_thread,
                        sandbox_mode: SandboxMode::FullAccess,
                        started_turn: &started,
                        input: input.as_slice(),
                        actor: actor.clone(),
                        completed: TurnCompletedNotification {
                            workspace_id: event_thread.workspace_id.clone(),
                            thread_id: event_thread.id.clone(),
                            turn: completed,
                        },
                        audit_event,
                    },
                )
                .await
                .unwrap_or_else(|error| panic!("{suffix} Message batch should commit: {error:#}"));

            assert_eq!(
                crate::find_turn_initiator(&connection, started.id.as_str())
                    .await
                    .expect("persisted initiator should load"),
                Some(actor.clone()),
                "{suffix} initiator must come from TurnStarted.actor"
            );
            let (_, persisted) = store
                .get_turn(event_thread.id.as_str(), started.id.as_str())
                .await
                .expect("persisted Turn should load")
                .expect("persisted Turn should exist");
            assert_eq!(persisted.status, TurnStatus::Completed);
            assert_eq!(persisted.author, expected_read_author);
            if matches!(actor, PersistedActorRef::System) {
                let row = pioneer_entity::turn::Entity::find_by_id(started.id.as_str())
                    .one(&connection)
                    .await
                    .expect("System Turn row should load")
                    .expect("System Turn row should exist");
                assert_eq!(row.initiated_by_actor_kind.as_deref(), Some("system"));
                assert_eq!(row.initiated_by_actor_id, None);
                assert_eq!(row.author_display_name_snapshot, None);
                assert_eq!(row.author_nickname_snapshot, None);
            }
        }
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn completed_message_materializes_started_and_completed_in_one_batch() {
    pioneer_postgres_test::with_database(async |fixture| {
    let store = postgres_store_with_workspace(fixture, "ws_atomic_message").await;
    let connection = store.database_connection();
    let timestamp = 1_700_000_000;
    let author = PrincipalId::new("P00000000000000000001").expect("principal id");
    let gateway_id = GatewayId::new("G00000000000000000001").expect("gateway id");
    let now = unix_to_datetime(timestamp);
    create_gateway_singleton(&connection, &gateway_id, 0, now)
        .await
        .expect("gateway identity should persist");
    create_superuser(
        &connection,
        &author,
        &gateway_id,
        "Author",
        "author",
        "author",
        now,
    )
    .await
    .expect("author principal should persist");
    let authoritative_thread = Thread {
        context_window: None,
        service_mode: None,
        context_authority: None,
        workspace_id: "ws_atomic_message".to_owned(),
        id: "thr_atomic_message".to_owned(),
        name: None,
        preview: String::new(),
        preview_author: None,
        mode: ThreadMode::Agent,
        model: "gpt-5.4".to_owned(),
        model_provider: "openai".to_owned(),
        reasoning_effort: None,
        created_at: timestamp,
        updated_at: timestamp,
        status: ThreadStatus::Idle,
        origin_kind: ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: Vec::new(),
    };
    store
        .upsert_thread_model(
            &authoritative_thread,
            PersistedActorRef::Principal(author.clone()),
        )
        .await
        .expect("authoritative thread should persist before Message projection");
    let mut thread = authoritative_thread.clone();
    // Simulate a Message prepared before a concurrent thread-management
    // update. The event envelope is intentionally stale and must not
    // replace the current thread metadata when its Turn is projected.
    thread.name = Some("stale name".to_owned());
    thread.preview = "hello".to_owned();
    thread.mode = ThreadMode::Message;
    thread.model = "stale-model".to_owned();
    thread.model_provider = "stale-provider".to_owned();
    thread.updated_at = timestamp + 1;
    let permission_profile = pioneer_protocol::default_turn_permission_profile_snapshot();
    let author_snapshot = pioneer_protocol::TurnAuthorSnapshot {
        cli_runtime_kind: None,
        actor: PersistedActorRef::Principal(author.clone()),
        display_name: "Author".to_owned(),
        nickname: "author".to_owned(),
        avatar_revision: None,
        agent: None,
    };
    let started = Turn {
        work_owner: Default::default(),
        id: "turn_atomic_message".to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        mode: ThreadMode::Message,
        author: Some(author_snapshot.clone()),
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: permission_profile.clone(),
    };
    thread.turns.push(started.clone());
    let mut completed = started.clone();
    completed.status = TurnStatus::Completed;
    let completed_notification = pioneer_protocol::TurnCompletedNotification {
        workspace_id: thread.workspace_id.clone(),
        thread_id: thread.id.clone(),
        turn: completed,
    };
    let audit = pioneer_protocol::TurnPermissionAuditEvent {
        workspace_id: thread.workspace_id.clone(),
        thread_id: thread.id.clone(),
        turn_id: started.id.clone(),
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
    let input = vec![UserInput::Text {
        text: "hello".to_owned(),
        text_elements: Vec::new(),
    }];

    store
        .materialize_completed_message_turn_with_permission_audit(CompletedMessageTurnWrite {
            thread: &thread,
            sandbox_mode: SandboxMode::FullAccess,
            started_turn: &started,
            input: &input,
            actor: PersistedActorRef::Principal(author.clone()),
            completed: completed_notification,
            audit_event: audit,
        })
        .await
        .expect("Message batch should commit");

    let events = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(started.id.clone()))
        .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
        .all(&connection)
        .await
        .expect("must query turn events");
    assert_eq!(events.len(), 3);
    assert_eq!(
        events[0].event_type,
        pioneer_protocol::constants::events::TURN_STARTED
    );
    assert_eq!(
        events[1].event_type,
        pioneer_protocol::constants::events::TURN_COMPLETED
    );
    assert_eq!(
        events[2].event_type,
        pioneer_protocol::constants::events::TURN_PERMISSION_AUDIT
    );
    let (_, persisted) = store
        .get_turn(thread.id.as_str(), started.id.as_str())
        .await
        .expect("turn read should succeed")
        .expect("turn should exist");
    assert_eq!(persisted.status, TurnStatus::Completed);
    assert_eq!(persisted.mode, ThreadMode::Message);
    assert_eq!(persisted.author.as_ref(), Some(&author_snapshot));
    assert_eq!(
        crate::find_turn_initiator(&connection, started.id.as_str())
            .await
            .expect("turn initiator should load"),
        Some(PersistedActorRef::Principal(author.clone()))
    );
    assert_eq!(
        store.get_turn_inputs(started.id.as_str()).await.unwrap(),
        input
    );
    assert!(
        store
            .list_self_improvement_source_turns_after(thread.workspace_id.as_str(), 0, 0, 10,)
            .await
            .expect("Message source projection should query")
            .is_empty(),
        "ordinary Message completion must not schedule self-improvement work"
    );
    let persisted_thread = store
        .get_thread_model(thread.id.as_str())
        .await
        .expect("thread read should succeed")
        .expect("thread should remain persisted");
    assert_eq!(persisted_thread.name, authoritative_thread.name);
    assert_eq!(persisted_thread.mode, authoritative_thread.mode);
    assert_eq!(persisted_thread.model, authoritative_thread.model);
    assert_eq!(
        persisted_thread.model_provider,
        authoritative_thread.model_provider
    );
    assert_eq!(persisted_thread.preview, "hello");
    assert_eq!(persisted_thread.updated_at, timestamp + 1);
    assert_eq!(
        store
            .get_thread_sandbox_mode(thread.id.as_str())
            .await
            .expect("Message thread policy should load"),
        Some(SandboxMode::FullAccess)
    );

    let invalid_input = store
        .edit_turn_message(EditTurnMessageRequest {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: started.id.clone(),
            expected_revision: 0,
            input: vec![UserInput::Text {
                text: "   ".to_owned(),
                text_elements: Vec::new(),
            }],
            mentions: Vec::new(),
            changed_by: PersistedActorRef::Principal(author.clone()),
            changed_at_unix: timestamp + 1,
        })
        .await
        .expect_err("invalid replacement input must be rejected transactionally");
    assert_eq!(
        invalid_input.downcast_ref::<TurnMessageMutationFailure>(),
        Some(&TurnMessageMutationFailure::InvalidInput)
    );

    let missing_mention = store
        .edit_turn_message(EditTurnMessageRequest {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: started.id.clone(),
            expected_revision: 0,
            input: input.clone(),
            mentions: vec![TurnMention {
                principal_id: PrincipalId::new("P00000000000000000009")
                    .expect("missing principal id"),
                nickname: "Missing".to_owned(),
            }],
            changed_by: PersistedActorRef::Principal(author.clone()),
            changed_at_unix: timestamp + 1,
        })
        .await
        .expect_err("missing mention target must be rejected transactionally");
    assert_eq!(
        missing_mention.downcast_ref::<TurnMessageMutationFailure>(),
        Some(&TurnMessageMutationFailure::InvalidTarget)
    );

    let missing_artifact = store
        .edit_turn_message(EditTurnMessageRequest {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: started.id.clone(),
            expected_revision: 0,
            input: vec![UserInput::Artifact {
                artifact_id: "missing_artifact".to_owned(),
                version_id: Some("missing_version".to_owned()),
            }],
            mentions: Vec::new(),
            changed_by: PersistedActorRef::Principal(author.clone()),
            changed_at_unix: timestamp + 1,
        })
        .await
        .expect_err("missing artifact target must be rejected transactionally");
    assert_eq!(
        missing_artifact.downcast_ref::<TurnMessageMutationFailure>(),
        Some(&TurnMessageMutationFailure::InvalidTarget)
    );
    assert!(
        pioneer_crud_conversations::turn_message_revision::list_turn_message_revisions(&connection, started.id.as_str(), None, 10)
            .await
            .expect("failed replacements must leave revision history readable")
            .is_empty(),
        "failed replacement validation must not append a revision"
    );
    assert_eq!(
        store.get_turn_inputs(started.id.as_str()).await.unwrap(),
        input,
        "failed replacement validation must preserve current content"
    );

    let edited_input = vec![UserInput::Text {
        text: "edited".to_owned(),
        text_elements: Vec::new(),
    }];
    let edited = store
        .edit_turn_message(EditTurnMessageRequest {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: started.id.clone(),
            expected_revision: 0,
            input: edited_input.clone(),
            mentions: Vec::new(),
            changed_by: PersistedActorRef::Principal(author.clone()),
            changed_at_unix: timestamp + 1,
        })
        .await
        .expect("Message edit should commit atomically");
    assert_eq!(edited.turn.status, TurnStatus::Completed);
    assert_eq!(edited.turn.mode, ThreadMode::Message);
    assert_eq!(edited.turn.message_revision, 1);
    assert_eq!(edited.input, edited_input);
    assert_eq!(
        store.get_turn_inputs(started.id.as_str()).await.unwrap(),
        edited_input
    );
    assert_eq!(
        pioneer_crud_conversations::thread_metadata::find_thread_by_id(&connection, thread.id.as_str())
            .await
            .unwrap()
            .expect("edited Message thread should remain")
            .preview,
        "edited"
    );
    let edited_context = store
        .get_thread_conversation_history(thread.id.as_str(), 10)
        .await
        .expect("edited Message context should load");
    let edited_entry = edited_context
        .iter()
        .find(|entry| entry.turn_id == started.id)
        .expect("edited Message should remain in conversation context");
    assert_eq!(edited_entry.user_text.as_deref(), Some("edited"));
    assert_ne!(edited_entry.user_text.as_deref(), Some("hello"));
    let edited_history = store
        .get_thread_history(thread.id.as_str(), None)
        .await
        .expect("edited Message history should load")
        .expect("edited Message thread should exist");
    assert!(edited_history.events.iter().any(|event| matches!(
        &event.payload,
        ThreadHistoryEventPayload::TurnStarted { turn, input, .. }
            if turn.id == started.id
                && turn.message_revision == 1
                && input == &edited_input
    )));
    assert!(!edited_history.events.iter().any(|event| matches!(
        &event.payload,
        ThreadHistoryEventPayload::TurnStarted {
            input: projected_input,
            ..
        } if projected_input == &input
    )));
    let revisions =
        pioneer_crud_conversations::turn_message_revision::list_turn_message_revisions(&connection, started.id.as_str(), None, 10)
            .await
            .expect("revision history should load");
    assert_eq!(revisions.len(), 1);
    let prior = pioneer_crud_conversations::turn_message_revision::turn_message_revision_from_model(revisions[0].clone())
        .expect("prior revision should decode");
    assert_eq!(prior.revision, 0);
    assert_eq!(prior.input, Some(input.clone()));

    let stale = store
        .edit_turn_message(EditTurnMessageRequest {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: started.id.clone(),
            expected_revision: 0,
            input: vec![UserInput::Text {
                text: "stale".to_owned(),
                text_elements: Vec::new(),
            }],
            mentions: Vec::new(),
            changed_by: PersistedActorRef::Principal(author.clone()),
            changed_at_unix: timestamp + 2,
        })
        .await
        .expect_err("stale edit must conflict");
    assert_eq!(
        stale.downcast_ref::<TurnMessageMutationFailure>(),
        Some(&TurnMessageMutationFailure::RevisionConflict)
    );
    assert_eq!(
        pioneer_crud_conversations::turn_message_revision::list_turn_message_revisions(&connection, started.id.as_str(), None, 10)
            .await
            .unwrap()
            .len(),
        1,
        "failed edit must not append a second prior revision"
    );
    assert_eq!(
        store.get_turn_inputs(started.id.as_str()).await.unwrap(),
        edited_input,
        "failed edit must not replace current content"
    );
    let events = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(started.id.clone()))
        .order_by_asc(pioneer_entity::turn_event::Column::Sequence)
        .all(&connection)
        .await
        .expect("must query post-edit events");
    assert_eq!(events.len(), 4);
    assert_eq!(
        events.last().map(|event| event.event_type.as_str()),
        Some(pioneer_protocol::constants::events::TURN_MESSAGE_EDITED)
    );

    let deleted = store
        .delete_turn_message(DeleteTurnMessageRequest {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: started.id.clone(),
            expected_revision: 1,
            changed_by: PersistedActorRef::Principal(author.clone()),
            changed_at_unix: timestamp + 3,
        })
        .await
        .expect("Message delete should commit atomically");
    assert!(deleted.event.is_some());
    assert!(deleted.turn.message_deleted);
    assert_eq!(deleted.turn.message_revision, 2);
    assert_eq!(deleted.turn.status, TurnStatus::Completed);
    assert!(
        store
            .get_turn_inputs(started.id.as_str())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        pioneer_crud_conversations::thread_metadata::find_thread_by_id(&connection, thread.id.as_str())
            .await
            .unwrap()
            .expect("deleted Message thread should remain")
            .preview,
        ""
    );
    let deleted_context = store
        .get_thread_conversation_history(thread.id.as_str(), 10)
        .await
        .expect("deleted Message context should load");
    let deleted_entry = deleted_context
        .iter()
        .find(|entry| entry.turn_id == started.id)
        .expect("deleted Message keeps its stable Turn position");
    assert_eq!(deleted_entry.user_text, None);
    assert!(deleted_entry.user_artifacts.is_empty());
    assert_ne!(deleted_entry.user_text.as_deref(), Some("edited"));
    assert_ne!(deleted_entry.user_text.as_deref(), Some("hello"));
    let deleted_history = store
        .get_thread_history(thread.id.as_str(), None)
        .await
        .expect("deleted Message history should load")
        .expect("deleted Message thread should exist");
    assert!(deleted_history.events.iter().any(|event| matches!(
        &event.payload,
        ThreadHistoryEventPayload::TurnStarted { turn, input, .. }
            if turn.id == started.id
                && turn.message_deleted
                && turn.message_revision == 2
                && input.is_empty()
    )));
    assert!(deleted_history.events.iter().any(|event| matches!(
        &event.payload,
        ThreadHistoryEventPayload::TurnCompleted { turn, .. }
            if turn.id == started.id
                && turn.message_deleted
                && turn.message_revision == 2
    )));
    assert!(!deleted_history.events.iter().any(|event| matches!(
        &event.payload,
        ThreadHistoryEventPayload::TurnStarted {
            input: projected_input,
            ..
        } if projected_input == &edited_input || projected_input == &input
    )));
    let revisions = store
        .get_turn_message_revisions(started.id.as_str(), None, 10)
        .await
        .expect("edit and delete revisions should load");
    assert_eq!(revisions.len(), 2);
    assert_eq!(revisions[0].revision, 1);
    assert_eq!(revisions[0].input, Some(edited_input.clone()));
    assert_eq!(revisions[1].revision, 0);
    assert_eq!(revisions[1].input, Some(input.clone()));

    let duplicate = store
        .delete_turn_message(DeleteTurnMessageRequest {
            workspace_id: thread.workspace_id.clone(),
            thread_id: thread.id.clone(),
            turn_id: started.id.clone(),
            expected_revision: 1,
            changed_by: PersistedActorRef::Principal(author.clone()),
            changed_at_unix: timestamp + 4,
        })
        .await
        .expect("same delete retry should return the existing tombstone");
    assert!(duplicate.event.is_none());
    assert_eq!(duplicate.turn, deleted.turn);
    assert_eq!(
        store
            .get_turn_message_revisions(started.id.as_str(), None, 10)
            .await
            .unwrap()
            .len(),
        2,
        "duplicate delete must not append another revision"
    );
    let events = pioneer_entity::turn_event::Entity::find()
        .filter(pioneer_entity::turn_event::Column::TurnId.eq(started.id.clone()))
        .all(&connection)
        .await
        .expect("must query post-delete events");
    assert_eq!(events.len(), 5, "duplicate delete must not append an event");
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type == pioneer_protocol::constants::events::TURN_MESSAGE_DELETED
            })
            .count(),
        1
    );

    let delete_event = pioneer_crud_conversations::turn_payload::hydrate_events(&connection, events)
        .await
        .expect("load retained event payloads")
        .into_iter()
        .find(|event| {
            event.event_type == pioneer_protocol::constants::events::TURN_MESSAGE_DELETED
        })
        .map(pioneer_crud_execution::event_append::appended_event_from_model)
        .transpose()
        .expect("delete event should decode")
        .expect("delete event should exist");
    connection
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE \"turn\" SET message_deleted_at=NULL, message_deleted_by_actor_kind=NULL, message_deleted_by_actor_id=NULL WHERE id=$1",
            [started.id.clone().into()],
        ))
        .await
        .expect("test should corrupt tombstone projection");
    connection
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "UPDATE thread SET preview=$1 WHERE id=$2",
            ["edited".into(), thread.id.clone().into()],
        ))
        .await
        .expect("test should restore the deleted Message preview");
    crate::store::conversations::turn_writes::replace_turn_input(
        &connection,
        started.id.as_str(),
        edited_input.as_slice(),
        unix_to_datetime(timestamp + 5),
    )
    .await
    .expect("test should corrupt current input projection");

    store
        .turn_projector
        .project(&connection, &delete_event)
        .await
        .expect("delete replay should repair the current projection");
    store
        .turn_projector
        .project(&connection, &delete_event)
        .await
        .expect("repeated delete replay should be idempotent");
    let (_, repaired) = store
        .get_turn(thread.id.as_str(), started.id.as_str())
        .await
        .unwrap()
        .expect("repaired Turn should exist");
    assert_eq!(repaired.status, TurnStatus::Completed);
    assert_eq!(repaired.message_revision, 2);
    assert!(repaired.message_deleted);
    assert_eq!(
        pioneer_crud_conversations::thread_metadata::find_thread_by_id(&connection, thread.id.as_str())
            .await
            .unwrap()
            .expect("replayed Message thread should remain")
            .preview,
        "",
        "delete replay must remove the deleted Message preview"
    );
    assert!(
        store
            .get_turn_inputs(started.id.as_str())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .get_turn_message_revisions(started.id.as_str(), None, 10)
            .await
            .unwrap()
            .len(),
        2,
        "replay must not duplicate immutable revisions"
    );

    for (fixture_update, label) in [
        (
            "message_deleted_at=NULL, turn_kind='task_run', origin='user', status='completed'",
            "task Turn",
        ),
        (
            "message_deleted_at=NULL, turn_kind='conversation', origin='system', status='completed'",
            "system Turn",
        ),
        (
            "message_deleted_at=NULL, turn_kind='conversation', origin='user', status='in_progress'",
            "non-terminal Turn",
        ),
        (
            "message_deleted_at=NULL, turn_kind='conversation', origin='user', status='completed', initiated_by_actor_kind='system', initiated_by_actor_id=NULL",
            "system-authored Turn",
        ),
    ] {
        let update_sql = format!("UPDATE turn SET {fixture_update} WHERE id='{}'", started.id);
        connection
            .execute_unprepared(update_sql.as_str())
            .await
            .unwrap_or_else(|error| panic!("prepare {label} fixture: {error}"));
        let error = store
            .edit_turn_message(EditTurnMessageRequest {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: started.id.clone(),
                expected_revision: 2,
                input: vec![UserInput::Text {
                    text: "must remain immutable".to_owned(),
                    text_elements: Vec::new(),
                }],
                mentions: Vec::new(),
                changed_by: PersistedActorRef::Principal(author.clone()),
                changed_at_unix: timestamp + 6,
            })
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<TurnMessageMutationFailure>(),
            Some(&TurnMessageMutationFailure::ImmutableMessage),
            "{label} must remain immutable even if its mode column says Message"
        );
        let error = store
            .delete_turn_message(DeleteTurnMessageRequest {
                workspace_id: thread.workspace_id.clone(),
                thread_id: thread.id.clone(),
                turn_id: started.id.clone(),
                expected_revision: 2,
                changed_by: PersistedActorRef::Principal(author.clone()),
                changed_at_unix: timestamp + 7,
            })
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<TurnMessageMutationFailure>(),
            Some(&TurnMessageMutationFailure::ImmutableMessage),
            "{label} must not accept a Message tombstone"
        );
    }
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn message_mutation_rechecks_current_member_thread_access() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "W00000000000000000006";
        let thread_id = "T00000000000000000006";
        let store = postgres_store_with_workspace(fixture, workspace_id).await;
        let connection = store.database_connection();
        let gateway_id = GatewayId::new("G00000000000000000001").expect("gateway id");
        let superuser_id = PrincipalId::new("P00000000000000000001").expect("principal id");
        let member_id = PrincipalId::new("P00000000000000000002").expect("principal id");
        let now = unix_to_datetime(1_700_000_000);
        create_gateway_singleton(&connection, &gateway_id, 0, now)
            .await
            .expect("gateway identity should persist");
        create_superuser(
            &connection,
            &superuser_id,
            &gateway_id,
            "Superuser",
            "superuser",
            "superuser",
            now,
        )
        .await
        .expect("superuser should persist");
        create_member_principal(
            &connection,
            NewMemberPrincipalRow {
                id: member_id.clone(),
                gateway_id: gateway_id.clone(),
                role_key: RoleKey::member(),
                display_name: "Member".to_owned(),
                nickname: "member".to_owned(),
                nickname_key: "member".to_owned(),
                now,
            },
        )
        .await
        .expect("member should persist");
        let transaction = connection.begin().await.expect("membership transaction");
        insert_workspace_membership(
            &transaction,
            &NewWorkspaceMembership {
                gateway_id: gateway_id.clone(),
                principal_id: member_id.clone(),
                workspace_id: workspace_id.to_owned(),
                granted_by: PersistedActorRef::Principal(superuser_id.clone()),
                now,
            },
        )
        .await
        .expect("workspace membership should persist");
        transaction.commit().await.expect("membership commit");

        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Message,
            model: String::new(),
            model_provider: String::new(),
            reasoning_effort: None,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::Collaborative,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: Some(pioneer_protocol::ThreadVisibility::Workspace),
            turns: Vec::new(),
        };
        crate::store::conversations::thread_writes::insert_user_thread_with_creator(
            &connection,
            &thread,
            &PersistedActorRef::Principal(superuser_id.clone()),
            crate::PersistedThreadAccessClass::Workspace,
            now,
            now,
        )
        .await
        .expect("workspace thread should persist");
        let read_anchor_turn_id = "R00000000000000000001";
        insert_unread_test_turn(
            &store,
            workspace_id,
            thread_id,
            read_anchor_turn_id,
            "001",
            "message",
            Some(superuser_id.as_ref()),
            false,
        )
        .await;

        let transaction = connection.begin().await.expect("authorization transaction");
        assert_eq!(
            principal_current_thread_access_kind(
                &transaction,
                workspace_id,
                thread_id,
                &superuser_id,
            )
            .await
            .expect("current Superuser ACL should resolve"),
            Some(pioneer_protocol::PrincipalKind::Superuser)
        );
        assert!(
            message_mutation_actor_current_thread_write_kind(
                &transaction,
                workspace_id,
                thread_id,
                &PersistedActorRef::Principal(member_id.clone()),
            )
            .await
            .expect("current ACL should resolve")
                == Some(pioneer_protocol::PrincipalKind::User)
        );
        transaction
            .rollback()
            .await
            .expect("authorization rollback");

        let transaction = connection.begin().await.expect("revocation transaction");
        delete_workspace_membership(&transaction, &gateway_id, &member_id, workspace_id)
            .await
            .expect("workspace membership should revoke");
        transaction.commit().await.expect("revocation commit");

        let transaction = connection.begin().await.expect("authorization transaction");
        assert!(
            message_mutation_actor_current_thread_write_kind(
                &transaction,
                workspace_id,
                thread_id,
                &PersistedActorRef::Principal(member_id.clone()),
            )
            .await
            .expect("revoked ACL should resolve")
            .is_none()
        );
        transaction
            .rollback()
            .await
            .expect("authorization rollback");

        let error = store
            .mark_thread_read(crate::MarkThreadReadRequest {
                through_block: None,
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                principal_id: member_id,
                through_turn_id: read_anchor_turn_id.to_owned(),
                read_at_unix: 1_700_000_001,
            })
            .await
            .expect_err("revoked Member must not mutate or disclose a read cursor");
        assert_eq!(
            error.downcast_ref::<crate::ThreadReadFailure>(),
            Some(&crate::ThreadReadFailure::InvalidTarget)
        );
        Ok(())
    })
    .await
    .unwrap();
}
