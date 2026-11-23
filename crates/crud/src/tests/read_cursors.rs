use super::*;

#[tokio::test]
async fn completed_agent_replies_are_exact_read_positions_even_after_a_later_turn() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_unread_agent";
        let thread_id = "T00000000000000000001";
        let gateway_id = GatewayId::new("G00000000000000000001").expect("gateway id");
        let reader_id = PrincipalId::new("P00000000000000000001").expect("principal id");
        let store = postgres_store_with_workspace(fixture, workspace_id).await;
        let now = unix_to_datetime(1_700_000_000);
        crate::create_gateway_singleton(&store.connection, &gateway_id, 0, now)
            .await
            .expect("gateway identity should persist");
        crate::create_superuser(
            &store.connection,
            &reader_id,
            &gateway_id,
            "Reader",
            "reader",
            "reader",
            now,
        )
        .await
        .expect("reader principal should persist");
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: String::new(),
            model_provider: String::new(),
            reasoning_effort: None,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_000,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::Collaborative,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: Some(pioneer_protocol::ThreadVisibility::Workspace),
            turns: Vec::new(),
        };
        store
            .upsert_thread_model(&thread, PersistedActorRef::Principal(reader_id.clone()))
            .await
            .expect("thread should persist");

        let old_turn = "A00000000000000000001";
        let later_turn = "B00000000000000000001";
        for (turn_id, key) in [
            (
                old_turn,
                "00000001700000100000:A00000000000000000001:000:user",
            ),
            (
                later_turn,
                "00000001700000101000:B00000000000000000001:000:user",
            ),
        ] {
            insert_unread_test_turn(
                &store,
                workspace_id,
                thread_id,
                turn_id,
                key,
                "agent",
                Some(reader_id.as_ref()),
                false,
            )
            .await;
        }
        // Model an active execution whose beginning is already behind the cursor.
        pioneer_entity::turn::Entity::update_many()
            .col_expr(
                pioneer_entity::turn::Column::Status,
                Expr::value("in_progress"),
            )
            .filter(pioneer_entity::turn::Column::Id.eq(old_turn))
            .exec(&store.connection)
            .await
            .unwrap();
        async fn position(
            store: &CrudStore,
            thread: &str,
            block: &str,
        ) -> pioneer_protocol::ThreadReadPosition {
            let row =
                pioneer_crud_conversations::thread_timeline_projection::find_thread_timeline_block_by_id(
                    &store.connection,
                    thread,
                    block,
                )
                .await
                .unwrap()
                .unwrap();
            pioneer_protocol::ThreadReadPosition {
                block_id: row.block_id,
                sort_key: row.sort_key,
            }
        }
        let request = |turn: &str, block: Option<pioneer_protocol::ThreadReadPosition>| {
            crate::MarkThreadReadRequest {
                workspace_id: workspace_id.into(),
                thread_id: thread_id.into(),
                principal_id: reader_id.clone(),
                through_turn_id: turn.into(),
                through_block: block,
                read_at_unix: 1_700_000_200,
            }
        };
        let initial = store
            .mark_thread_read(request(later_turn, None))
            .await
            .unwrap();
        assert_eq!(initial.unread_count, 0, "own prompts remain excluded");
        let historical = store
            .mark_thread_read(request(
                later_turn,
                Some(position(&store, thread_id, &format!("user:{later_turn}")).await),
            ))
            .await
            .unwrap();
        assert_eq!(
            historical.cursor, initial.cursor,
            "exact reads preserve historical user-message anchors too"
        );
        let item = |id: &str, text: &str, phase| pioneer_protocol::TurnItem::AgentMessage {
            id: id.into(),
            text: text.into(),
            phase,
            markdown: None,
            markdown_version: None,
        };
        let answer = pioneer_protocol::TurnItem::AgentMessage {
            id: "first-final".into(),
            text: "Markdown fallback".into(),
            markdown: Some(pioneer_protocol::MarkdownDocument::from_plain_text(
                "Markdown answer",
            )),
            markdown_version: None,
            phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
        };
        store
            .materialize_item_started(
                pioneer_protocol::ItemStartedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: answer.clone(),
                },
                1_700_000_201,
            )
            .await
            .unwrap();
        store
            .materialize_item_updated(
                pioneer_protocol::ItemUpdatedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: answer.clone(),
                },
                1_700_000_201,
            )
            .await
            .unwrap();
        let first_id = crate::assistant_block_id(old_turn, answer.item_id());
        assert_eq!(
            store
                .unread_counts_for_threads(&reader_id, &[thread_id.into()])
                .await
                .unwrap()
                .get(thread_id),
            None
        );
        let streaming_position = position(&store, thread_id, &first_id).await;
        assert!(
            store
                .mark_thread_read(request(old_turn, Some(streaming_position.clone())))
                .await
                .is_err(),
            "a streaming block is not a readable answer"
        );
        let completed = pioneer_protocol::ItemCompletedNotification {
            workspace_id: workspace_id.into(),
            thread_id: thread_id.into(),
            turn_id: old_turn.into(),
            item: answer,
        };
        store
            .materialize_item_completed(completed.clone(), 1_700_000_202)
            .await
            .unwrap();
        let counts = store
            .unread_counts_for_threads(&reader_id, &[thread_id.into()])
            .await
            .unwrap();
        assert_eq!(counts.get(thread_id), Some(&1));
        assert_eq!(
            crate::count_unread_message_blocks(
                &store.connection,
                thread_id,
                reader_id.as_ref(),
                &initial.cursor.sort_key
            )
            .await
            .unwrap(),
            1
        );
        assert!(
            store
                .mark_thread_read(request(old_turn, Some(streaming_position)))
                .await
                .is_err(),
            "a speculative pre-completion sort key cannot acknowledge the moved answer"
        );
        let first = store
            .mark_thread_read(request(
                old_turn,
                Some(position(&store, thread_id, &first_id).await),
            ))
            .await
            .unwrap();
        assert!(
            first.cursor.sort_key > initial.cursor.sort_key,
            "late completion must escape the saved cursor"
        );
        assert_eq!(first.unread_count, 0);
        store
            .materialize_item_completed(completed.clone(), 1_700_000_202)
            .await
            .unwrap();
        // A pre-upgrade completed block has no completion marker. Its content
        // refresh must preserve an already-read historical position.
        pioneer_entity::thread_timeline_block::Entity::update_many()
            .col_expr(
                pioneer_entity::thread_timeline_block::Column::MetadataJson,
                Expr::value("{}"),
            )
            .filter(pioneer_entity::thread_timeline_block::Column::BlockId.eq(first_id.clone()))
            .exec(&store.connection)
            .await
            .unwrap();
        store
            .materialize_item_updated(
                pioneer_protocol::ItemUpdatedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: completed.item.clone(),
                },
                1_700_000_203,
            )
            .await
            .unwrap();
        store
            .materialize_item_snapshot_updated(
                pioneer_protocol::ItemUpdatedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: completed.item,
                },
                1_700_000_203,
            )
            .await
            .unwrap();
        let replay = store
            .mark_thread_read(request(
                old_turn,
                Some(position(&store, thread_id, &first_id).await),
            ))
            .await
            .unwrap();
        assert_eq!(
            first, replay,
            "completion redelivery and reads are idempotent"
        );
        // Task deliveries are standalone final answers, even on a non-user Turn,
        // and an empty final text is valid (HTML is attached by the reader).
        pioneer_entity::turn::Entity::update_many()
            .col_expr(
                pioneer_entity::turn::Column::Origin,
                Expr::value("scheduled_task"),
            )
            .col_expr(
                pioneer_entity::turn::Column::InitiatedByActorKind,
                Expr::value("system"),
            )
            .col_expr(
                pioneer_entity::turn::Column::InitiatedByActorId,
                Expr::value(Option::<String>::None),
            )
            .filter(pioneer_entity::turn::Column::Id.eq(old_turn))
            .exec(&store.connection)
            .await
            .unwrap();
        let task_item = item(
            &pioneer_protocol::task_delivery_result_item_id("unread-delivery"),
            "",
            pioneer_protocol::AgentMessagePhase::FinalAnswer,
        );
        let task_id = crate::assistant_block_id(old_turn, task_item.item_id());
        store
            .materialize_task_delivery_item(
                pioneer_protocol::ItemStartedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: task_item.clone(),
                },
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: task_item.clone(),
                },
                1_700_000_202,
                None,
            )
            .await
            .unwrap();
        let commentary = item(
            "commentary",
            "still working",
            pioneer_protocol::AgentMessagePhase::Commentary,
        );
        store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: commentary,
                },
                1_700_000_202,
            )
            .await
            .unwrap();
        let stale = store
            .mark_thread_read(request(
                old_turn,
                Some(position(&store, thread_id, &first_id).await),
            ))
            .await
            .unwrap();
        assert_eq!(
            stale.unread_count, 1,
            "same-Turn final answers are independent; commentary is excluded"
        );
        let mut foreign_thread = thread.clone();
        foreign_thread.id = "foreign-thread".into();
        store
            .upsert_thread_model(
                &foreign_thread,
                PersistedActorRef::Principal(reader_id.clone()),
            )
            .await
            .unwrap();
        insert_unread_test_turn(
            &store,
            workspace_id,
            &foreign_thread.id,
            "foreign-turn",
            "001",
            "agent",
            Some(reader_id.as_ref()),
            false,
        )
        .await;
        let task_position = position(&store, thread_id, &task_id).await;
        let work_position = position(&store, thread_id, &crate::work_block_id(old_turn)).await;
        for (turn, block) in [
            (later_turn, task_position.clone()),
            (
                old_turn,
                pioneer_protocol::ThreadReadPosition {
                    block_id: "missing".into(),
                    sort_key: "missing".into(),
                },
            ),
            (
                old_turn,
                position(&store, &foreign_thread.id, "user:foreign-turn").await,
            ),
            (old_turn, work_position),
            (
                old_turn,
                pioneer_protocol::ThreadReadPosition {
                    block_id: task_id.clone(),
                    sort_key: first.cursor.sort_key.clone(),
                },
            ),
        ] {
            assert!(
                store
                    .mark_thread_read(request(turn, Some(block)))
                    .await
                    .is_err(),
                "mismatched, foreign, service and forged positions fail closed"
            );
        }
        let second = store
            .mark_thread_read(request(old_turn, Some(task_position)))
            .await
            .unwrap();
        assert!(
            second.cursor.sort_key > first.cursor.sort_key,
            "same-second completions still advance"
        );
        assert_eq!(second.unread_count, 0);
        store
            .materialize_task_delivery_item(
                pioneer_protocol::ItemStartedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: task_item.clone(),
                },
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: task_item,
                },
                1_700_000_204,
                None,
            )
            .await
            .unwrap();
        let redelivered = store
            .mark_thread_read(request(
                old_turn,
                Some(position(&store, thread_id, &task_id).await),
            ))
            .await
            .unwrap();
        assert_eq!(second.cursor, redelivered.cursor);
        assert_eq!(redelivered.unread_count, 0);
        let stale_user = store
            .mark_thread_read(request(later_turn, None))
            .await
            .unwrap();
        assert_eq!(second.cursor, stale_user.cursor);

        // A subsequent prompt in the same clock tick must follow the reply,
        // with its work still following the prompt in the usual rank order.
        let (_, mut next_turn) = store
            .get_turn(thread_id, later_turn)
            .await
            .unwrap()
            .unwrap();
        next_turn.id = "C00000000000000000001".into();
        next_turn.status = TurnStatus::InProgress;
        let mut next_thread = thread.clone();
        next_thread.updated_at = 1_700_000_101;
        store
            .materialize_turn_start(
                &next_thread,
                SandboxMode::FullAccess,
                &next_turn,
                &[UserInput::Text {
                    text: "Next request".into(),
                    text_elements: Vec::new(),
                }],
                PersistedActorRef::Principal(reader_id.clone()),
            )
            .await
            .unwrap();
        let prompt = position(&store, thread_id, &crate::user_block_id(&next_turn.id)).await;
        let work = position(&store, thread_id, &crate::work_block_id(&next_turn.id)).await;
        assert!(prompt.sort_key > second.cursor.sort_key);
        assert!(work.sort_key > prompt.sort_key);
        assert_eq!(
            work.sort_key.split(':').next(),
            prompt.sort_key.split(':').next(),
            "work remains anchored to its prompt's published domain"
        );
        let millis = second
            .cursor
            .sort_key
            .split(':')
            .next()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let request_time = chrono::DateTime::from_timestamp_millis(millis)
            .unwrap()
            .fixed_offset();
        assert_eq!(
            prompt
                .sort_key
                .split(':')
                .next()
                .unwrap()
                .parse::<i64>()
                .unwrap(),
            millis + 1
        );
        let mut approval = crate::NewCliRuntimePendingRequest {
            request_id: "shifted-prompt-approval".into(),
            runtime_id: "codex".into(),
            runtime_kind: "codex".into(),
            workspace_id: workspace_id.into(),
            thread_id: thread_id.into(),
            turn_id: Some(next_turn.id.clone()),
            native_thread_id: None,
            native_turn_id: None,
            native_item_id: None,
            request_kind: "command_approval".into(),
            payload_json: r#"{"command":"echo ready","cwd":"/tmp"}"#.into(),
            created_at: request_time,
            updated_at: request_time,
        };
        store
            .create_cli_runtime_pending_request(approval.clone())
            .await
            .unwrap();
        let approval_id = crate::approval_block_id(&next_turn.id, &approval.request_id);
        let pending = position(&store, thread_id, &approval_id).await;
        assert!(prompt.sort_key < work.sort_key && work.sort_key < pending.sort_key);
        assert!(
            store
                .mark_thread_read(request(&next_turn.id, Some(pending.clone())))
                .await
                .is_err(),
            "an exact approval position is still not a message read anchor"
        );
        assert_eq!(
            prompt.sort_key.split(':').next(),
            pending.sort_key.split(':').next()
        );
        approval.updated_at += chrono::Duration::seconds(1);
        approval.payload_json = r#"{"command":"echo updated","cwd":"/tmp"}"#.into();
        let updated = store
            .create_cli_runtime_pending_request(approval.clone())
            .await
            .unwrap();
        assert_eq!(updated.created_at, request_time);
        assert_eq!(
            position(&store, thread_id, &approval_id).await,
            pending,
            "metadata updates preserve approval identity and position"
        );
        store
            .resolve_cli_runtime_pending_request(crate::ResolveCliRuntimePendingRequest {
                request_id: approval.request_id.clone(),
                status: crate::CliRuntimePendingRequestStatus::Answered,
                response_json: Some(r#"{"approved":true,"scope":"once"}"#.into()),
                updated_at: approval.updated_at,
                resolved_at: approval.updated_at,
            })
            .await
            .unwrap()
            .unwrap();
        assert!(
            pioneer_crud_conversations::thread_timeline_projection::find_thread_timeline_block_by_id(
                &store.connection,
                thread_id,
                &approval_id,
            )
            .await
            .unwrap()
            .is_none(),
            "completed approval leaves no stale row"
        );
        assert_eq!(
            position(&store, thread_id, &crate::work_block_id(&next_turn.id)).await,
            work
        );
        // A later request already has the correct time domain: retain it.
        approval.request_id = "later-approval".into();
        approval.created_at += chrono::Duration::seconds(10);
        approval.updated_at = approval.created_at;
        store
            .create_cli_runtime_pending_request(approval.clone())
            .await
            .unwrap();
        let later_approval = position(
            &store,
            thread_id,
            &crate::approval_block_id(&next_turn.id, &approval.request_id),
        )
        .await;
        assert_eq!(
            later_approval.sort_key,
            format!(
                "{:020}:{}:150:approval:{}",
                approval.created_at.timestamp_millis(),
                next_turn.id,
                approval.request_id
            )
        );

        // Agent-authored Message Turns are visible UserBlocks, but are not
        // unread units. Their exact viewed position can acknowledge an answer
        // immediately before them without broadening the old Turn-only RPC.
        store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: workspace_id.into(),
                    thread_id: thread_id.into(),
                    turn_id: old_turn.into(),
                    item: item(
                        "last-final",
                        "Unread answer",
                        pioneer_protocol::AgentMessagePhase::FinalAnswer,
                    ),
                },
                1_700_000_205,
            )
            .await
            .unwrap();
        let last = position(
            &store,
            thread_id,
            &crate::assistant_block_id(old_turn, "last-final"),
        )
        .await;
        let agent_turn = "D00000000000000000001";
        let seed = store.connection.begin().await?;
        let agent_key = pioneer_crud_conversations::thread_timeline_projection::new_message_sort_key(
            &seed,
            thread_id,
            &format!("user:{agent_turn}"),
            agent_turn,
            format!("00000001700000101000:{agent_turn}:000:user"),
            1_700_000_205,
        )
        .await
        .unwrap();
        seed.commit().await?;
        insert_unread_test_turn(
            &store,
            workspace_id,
            thread_id,
            agent_turn,
            &agent_key,
            "message",
            Some("E00000000000000000001"),
            false,
        )
        .await;
        pioneer_entity::turn::Entity::update_many()
            .col_expr(
                pioneer_entity::turn::Column::InitiatedByActorKind,
                Expr::value("agent_execution"),
            )
            .filter(pioneer_entity::turn::Column::Id.eq(agent_turn))
            .exec(&store.connection)
            .await
            .unwrap();
        assert!(agent_key > last.sort_key);
        assert_eq!(
            store
                .unread_counts_for_threads(&reader_id, &[thread_id.into()])
                .await
                .unwrap()
                .get(thread_id),
            Some(&1),
            "agent-authored UserBlock does not add unread"
        );
        assert!(
            store
                .mark_thread_read(request(agent_turn, None))
                .await
                .is_err(),
            "legacy Turn-only rules stay unchanged"
        );
        let read = store
            .mark_thread_read(request(
                agent_turn,
                Some(position(&store, thread_id, &format!("user:{agent_turn}")).await),
            ))
            .await
            .unwrap();
        assert_eq!(
            read.unread_count, 0,
            "the visible exact agent message clears the earlier completed answer"
        );
        assert_eq!(read.cursor.sort_key, agent_key);
        Ok(())
    })
    .await
    .unwrap();
}
