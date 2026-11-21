//! PostgreSQL regressions for server unread activation and access boundaries.
use super::*;
use crate::repositories::unread_start;
use pioneer_entity::{gateway_identity, unread_message_position};
use pioneer_protocol::{AgentMessagePhase, ThreadVisibility};

const WS: &str = "W00000000000000000001";
const THREAD: &str = "T00000000000000000001";
const PRIVATE: &str = "T00000000000000000002";
const NOW: i64 = 1_700_000_000;

struct Fixture {
    store: CrudStore,
    gateway: GatewayId,
    owner: PrincipalId,
    member: PrincipalId,
    thread: Thread,
    clock: std::cell::Cell<i64>,
}
impl Fixture {
    async fn new(fixture: &pioneer_postgres_test::TestDatabase) -> Self {
        let store = postgres_store_with_workspace(fixture, WS).await;
        let gateway = GatewayId::new("G00000000000000000001").unwrap();
        let owner = PrincipalId::new("P00000000000000000001").unwrap();
        let member = PrincipalId::new("P00000000000000000002").unwrap();
        let tx = store.connection.begin().await.unwrap();
        create_gateway_singleton(&tx, &gateway, 0, unix_to_datetime(NOW))
            .await
            .unwrap();
        create_superuser(
            &tx,
            &owner,
            &gateway,
            "Owner",
            "owner",
            "owner",
            unix_to_datetime(NOW),
        )
        .await
        .unwrap();
        create_member_principal(
            &tx,
            NewMemberPrincipalRow {
                id: member.clone(),
                gateway_id: gateway.clone(),
                role_key: RoleKey::member(),
                display_name: "Member".into(),
                nickname: "member".into(),
                nickname_key: "member".into(),
                now: unix_to_datetime(NOW),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        gateway_identity::Entity::update_many()
            .col_expr(
                gateway_identity::Column::UnreadStartedAt,
                Expr::val(NOW - 2),
            )
            .filter(gateway_identity::Column::Id.eq(gateway.to_string()))
            .exec(&store.connection)
            .await
            .unwrap();

        let mut thread = sample_thread(WS, THREAD, NOW);
        thread.visibility = Some(ThreadVisibility::Workspace);
        let tx = store.connection.begin().await.unwrap();
        crate::repositories::thread::insert_user_thread_with_creator(
            &tx,
            &thread,
            &PersistedActorRef::Principal(owner.clone()),
            crate::PersistedThreadAccessClass::Workspace,
            unix_to_datetime(NOW),
            unix_to_datetime(NOW),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        Self {
            store,
            gateway,
            owner,
            member,
            thread,
            clock: std::cell::Cell::new(NOW),
        }
    }
    fn next(&self) -> i64 {
        let next = self.clock.get() + 2;
        self.clock.set(next);
        next
    }
    async fn grant_workspace(&self) {
        let tx = self.store.connection.begin().await.unwrap();
        insert_workspace_membership(
            &tx,
            &NewWorkspaceMembership {
                gateway_id: self.gateway.clone(),
                principal_id: self.member.clone(),
                workspace_id: WS.into(),
                granted_by: PersistedActorRef::Principal(self.owner.clone()),
                now: unix_to_datetime(self.next()),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    async fn prompt(&self, thread: &Thread, id: &str) {
        let mut thread = thread.clone();
        thread.updated_at = self.next();
        self.store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &sample_turn(id),
                &[UserInput::Text {
                    text: "message".into(),
                    text_elements: Vec::new(),
                }],
                PersistedActorRef::Principal(self.owner.clone()),
            )
            .await
            .unwrap();
    }
    async fn accept_answer(
        &self,
        thread: &str,
        turn: &str,
        id: &str,
        text: &str,
    ) -> anyhow::Result<()> {
        self.store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: WS.into(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    item: TurnItem::AgentMessage {
                        id: id.into(),
                        text: text.into(),
                        phase: AgentMessagePhase::FinalAnswer,
                        markdown: (!text.is_empty())
                            .then(|| pioneer_protocol::MarkdownDocument::from_plain_text(text)),
                        markdown_version: None,
                    },
                },
                self.next(),
            )
            .await
    }

    async fn answer(
        &self,
        thread: &str,
        turn: &str,
        id: &str,
        text: &str,
    ) -> pioneer_protocol::ThreadReadPosition {
        self.accept_answer(thread, turn, id, text).await.unwrap();
        self.answer_position(thread, turn, id).await
    }

    async fn answer_position(
        &self,
        thread: &str,
        turn: &str,
        id: &str,
    ) -> pioneer_protocol::ThreadReadPosition {
        let block =
            crate::repositories::thread_timeline_projection::find_thread_timeline_block_by_id(
                &self.store.connection,
                thread,
                &crate::assistant_block_id(turn, id),
            )
            .await
            .unwrap()
            .unwrap();
        pioneer_protocol::ThreadReadPosition {
            block_id: block.block_id,
            sort_key: block.sort_key,
        }
    }
    async fn count(&self, principal: &PrincipalId, thread: &str) -> u64 {
        let batch = self
            .store
            .unread_counts_for_threads(principal, &[thread.into(), thread.into()])
            .await
            .unwrap();
        let count = batch.get(thread).copied().unwrap_or(0);
        assert_eq!(
            count,
            crate::count_unread_message_blocks(
                &self.store.connection,
                thread,
                principal.as_ref(),
                ""
            )
            .await
            .unwrap()
        );
        count
    }
}

#[tokio::test]
async fn activation_excludes_history_preserves_real_read_and_survives_reopen() {
    pioneer_postgres_test::with_database(async |fixture| {
        let f = Fixture::new(fixture).await;
        f.grant_workspace().await;
        let old = "A00000000000000000001";
        f.prompt(&f.thread, old).await;
        f.prompt(&f.thread, "B00000000000000000001").await;
        f.answer(THREAD, old, "old-final", "history").await;
        let position = f.answer(THREAD, old, "old-final-2", "more history").await;
        let confirmed = f
            .store
            .mark_thread_read(crate::MarkThreadReadRequest {
                workspace_id: WS.into(),
                thread_id: THREAD.into(),
                principal_id: f.owner.clone(),
                through_turn_id: old.into(),
                through_block: Some(position),
                read_at_unix: NOW,
            })
            .await
            .unwrap()
            .cursor;
        f.store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: WS.into(),
                    thread_id: THREAD.into(),
                    turn_id: old.into(),
                    item: TurnItem::AgentMessage {
                        id: "old-commentary".into(),
                        text: "working".into(),
                        phase: AgentMessagePhase::Commentary,
                        markdown: None,
                        markdown_version: None,
                    },
                },
                NOW,
            )
            .await
            .unwrap();
        // A confirmed logical position can survive the timeline projection losing a block.
        for item in ["old-final", "old-final-2"] {
            crate::delete_thread_timeline_block(
                &f.store.connection,
                &crate::assistant_block_id(old, item),
            )
            .await
            .unwrap();
        }
        // Set the persisted activation boundary while real history and an
        // independently confirmed read cursor already exist.
        let new_activation = f.next();
        let activate = async |tx: &sea_orm::DatabaseTransaction| {
            let gateway = f.gateway.to_string();
            gateway_identity::Entity::update_many()
                .col_expr(
                    gateway_identity::Column::UnreadStartedAt,
                    Expr::val(new_activation),
                )
                .filter(gateway_identity::Column::Id.eq(gateway))
                .exec(tx)
                .await
                .unwrap()
        };
        let before = gateway_identity::Entity::find_by_id(f.gateway.to_string())
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap()
            .unread_started_at;
        let tx = f.store.connection.begin().await.unwrap();
        assert_eq!(activate(&tx).await.rows_affected, 1);
        tx.rollback().await.unwrap();
        assert_eq!(
            gateway_identity::Entity::find_by_id(f.gateway.to_string())
                .one(&f.store.connection)
                .await
                .unwrap()
                .unwrap()
                .unread_started_at,
            before
        );
        // Publish the same boundary after rolling back the first attempt.
        let tx = f.store.connection.begin().await.unwrap();
        assert_eq!(activate(&tx).await.rows_affected, 1);
        tx.commit().await.unwrap();
        let start = gateway_identity::Entity::find_by_id(f.gateway.to_string())
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(f.count(&f.member, THREAD).await, 0);
        assert_eq!(f.count(&f.owner, THREAD).await, 0);
        assert!(
            crate::find_thread_read_cursor(&f.store.connection, &f.member, THREAD)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            crate::thread_read_cursor_from_model(
                &crate::find_thread_read_cursor(&f.store.connection, &f.owner, THREAD)
                    .await
                    .unwrap()
                    .unwrap()
            ),
            confirmed
        );
        // A late final answer on the old Turn is a new canonical publication.
        f.clock.set(start.unread_started_at + 2);
        let new = f.answer(THREAD, old, "late-final", "new").await;
        assert!(new.sort_key > format!("{:020}:", start.unread_started_at * 1000));
        assert_eq!(f.count(&f.member, THREAD).await, 1);
        assert_eq!(
            f.count(&f.owner, THREAD).await,
            1,
            "answers to own requests count"
        );
        // A completed commentary item is not yet a completed canonical answer.
        f.answer(THREAD, old, "old-commentary", "first final answer")
            .await;
        assert_eq!(f.count(&f.member, THREAD).await, 2);
        let reopened = CrudStore::new(fixture.connect().await?);
        assert_eq!(
            reopened
                .unread_counts_for_threads(&f.member, &[THREAD.into()])
                .await
                .unwrap()
                .get(THREAD),
            Some(&2)
        );
        assert_eq!(
            gateway_identity::Entity::find_by_id(f.gateway.to_string())
                .one(&f.store.connection)
                .await
                .unwrap()
                .unwrap()
                .unread_started_at,
            start.unread_started_at
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn grants_regrants_and_visibility_use_actual_access_not_first_open() {
    pioneer_postgres_test::with_database(async |fixture| {
        let f = Fixture::new(fixture).await;
        let old = "A00000000000000000001";
        f.prompt(&f.thread, old).await;
        f.answer(THREAD, old, "before-access", "history").await;
        assert_eq!(f.count(&f.member, THREAD).await, 0);
        f.grant_workspace().await;
        f.answer(THREAD, old, "after-grant-before-open", "new")
            .await;
        f.grant_workspace().await; // idempotent grant must keep its saved timestamp
        assert_eq!(f.count(&f.member, THREAD).await, 1);
        let tx = f.store.connection.begin().await.unwrap();
        delete_workspace_membership(&tx, &f.gateway, &f.member, WS)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        f.answer(THREAD, old, "during-revoke", "inaccessible").await;
        assert_eq!(f.count(&f.member, THREAD).await, 0);
        f.grant_workspace().await;
        assert_eq!(
            f.count(&f.member, THREAD).await,
            0,
            "regrant does not resurrect inaccessible history"
        );
        f.answer(THREAD, old, "after-regrant", "new").await;
        assert_eq!(f.count(&f.member, THREAD).await, 1);

        // A separate workspace grant cannot advance the first workspace's start.
        let other_ws = "W00000000000000000002";
        pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
            id: Set(other_ws.into()),
            name: Set("Other".into()),
            is_active: Set(true),
            is_current: Set(false),
            created_at: Set(unix_to_datetime(f.next())),
            updated_at: Set(unix_to_datetime(f.next())),
        })
        .exec(&f.store.connection)
        .await
        .unwrap();
        let tx = f.store.connection.begin().await.unwrap();
        insert_workspace_membership(
            &tx,
            &NewWorkspaceMembership {
                gateway_id: f.gateway.clone(),
                principal_id: f.member.clone(),
                workspace_id: other_ws.into(),
                granted_by: PersistedActorRef::Principal(f.owner.clone()),
                now: unix_to_datetime(f.next()),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(f.count(&f.member, THREAD).await, 1);

        let public_reader = PrincipalId::new("P00000000000000000003").unwrap();
        let tx = f.store.connection.begin().await.unwrap();
        create_member_principal(
            &tx,
            NewMemberPrincipalRow {
                id: public_reader.clone(),
                gateway_id: f.gateway.clone(),
                role_key: RoleKey::member(),
                display_name: "Public reader".into(),
                nickname: "public-reader".into(),
                nickname_key: "public-reader".into(),
                now: unix_to_datetime(f.next()),
            },
        )
        .await
        .unwrap();
        insert_workspace_membership(
            &tx,
            &NewWorkspaceMembership {
                gateway_id: f.gateway.clone(),
                principal_id: public_reader.clone(),
                workspace_id: WS.into(),
                granted_by: PersistedActorRef::Principal(f.owner.clone()),
                now: unix_to_datetime(f.next()),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let mut private = f.thread.clone();
        private.id = PRIVATE.into();
        private.visibility = Some(ThreadVisibility::Private);
        f.store
            .upsert_thread_model(&private, PersistedActorRef::Principal(f.owner.clone()))
            .await
            .unwrap();
        let private_turn = "A00000000000000000002";
        f.prompt(&private, private_turn).await;
        f.answer(PRIVATE, private_turn, "private-history", "old")
            .await;
        assert_eq!(f.count(&f.member, PRIVATE).await, 0);
        let tx = f.store.connection.begin().await.unwrap();
        crate::insert_thread_membership(
            &tx,
            &crate::NewThreadMembership {
                gateway_id: f.gateway.clone(),
                principal_id: f.member.clone(),
                workspace_id: WS.into(),
                thread_id: PRIVATE.into(),
                added_by: PersistedActorRef::Principal(f.owner.clone()),
                now: unix_to_datetime(f.next()),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        f.answer(PRIVATE, private_turn, "after-private-grant", "new")
            .await;
        assert_eq!(f.count(&f.member, PRIVATE).await, 1);
        let tx = f.store.connection.begin().await.unwrap();
        crate::repositories::thread::update_user_thread_management(
            &tx,
            WS,
            PRIVATE,
            None,
            None,
            Some(crate::PersistedThreadAccessClass::Workspace),
            None,
            unix_to_datetime(f.next()),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            f.count(&f.member, PRIVATE).await,
            1,
            "private participant keeps earlier continuous access"
        );
        assert_eq!(
            f.count(&public_reader, PRIVATE).await,
            0,
            "new public access excludes the former private history"
        );
        f.answer(PRIVATE, private_turn, "after-public-exposure", "new")
            .await;
        assert_eq!(f.count(&public_reader, PRIVATE).await, 1);
        assert_eq!(f.count(&f.member, PRIVATE).await, 2);

        // Removing/regranting private access captures a new boundary, while an
        // idempotent add preserves the current one. Return to private first.
        let tx = f.store.connection.begin().await.unwrap();
        crate::repositories::thread::update_user_thread_management(
            &tx,
            WS,
            PRIVATE,
            None,
            None,
            Some(crate::PersistedThreadAccessClass::Private),
            None,
            unix_to_datetime(f.next()),
        )
        .await
        .unwrap();
        crate::delete_thread_membership(&tx, &f.gateway, WS, PRIVATE, &f.member)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        f.answer(PRIVATE, private_turn, "private-gap", "old at regrant")
            .await;
        for round in 0..2 {
            let tx = f.store.connection.begin().await.unwrap();
            crate::insert_thread_membership(
                &tx,
                &crate::NewThreadMembership {
                    gateway_id: f.gateway.clone(),
                    principal_id: f.member.clone(),
                    workspace_id: WS.into(),
                    thread_id: PRIVATE.into(),
                    added_by: PersistedActorRef::Principal(f.owner.clone()),
                    now: unix_to_datetime(f.next()),
                },
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            if round == 0 {
                assert_eq!(f.count(&f.member, PRIVATE).await, 0);
                f.answer(PRIVATE, private_turn, "after-private-regrant", "new")
                    .await;
            }
        }
        assert_eq!(
            f.count(&f.member, PRIVATE).await,
            1,
            "idempotent private add keeps the earlier eligibility boundary"
        );
        f.answer(PRIVATE, private_turn, "after-private-regrant", "new")
            .await;
        assert_eq!(f.count(&f.member, PRIVATE).await, 1);
        let missing = PrincipalId::new("P00000000000000000009").unwrap();
        assert_eq!(f.count(&missing, PRIVATE).await, 0);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn replay_edits_and_projection_rebuild_reuse_first_completion_position() {
    pioneer_postgres_test::with_database(async |fixture| {
        let f = Fixture::new(fixture).await;
        f.grant_workspace().await;
        assert_eq!(
            f.count(&f.member, THREAD).await,
            0,
            "empty accessible thread"
        );
        let turn = "A00000000000000000001";
        f.prompt(&f.thread, turn).await;
        let first = f.answer(THREAD, turn, "html-only", "").await;
        f.answer(THREAD, turn, "mixed", "text with Markdown").await;
        let original_count = f.count(&f.member, THREAD).await;
        assert_eq!(original_count, 3); // other user's prompt and two canonical responses
        let repeated = f.answer(THREAD, turn, "html-only", "edited text").await;
        assert_eq!(first, repeated);
        // A rebuild deletes projection rows, not canonical publication identities.
        crate::delete_thread_timeline_blocks_for_thread(&f.store.connection, THREAD)
            .await
            .unwrap();
        f.store
            .materialize_item_snapshot_updated(
                pioneer_protocol::ItemUpdatedNotification {
                    workspace_id: WS.into(),
                    thread_id: THREAD.into(),
                    turn_id: turn.into(),
                    item: TurnItem::AgentMessage {
                        id: "html-only".into(),
                        text: "edited text".into(),
                        phase: AgentMessagePhase::FinalAnswer,
                        markdown: None,
                        markdown_version: None,
                    },
                },
                NOW,
            )
            .await
            .unwrap();
        let block =
            crate::repositories::thread_timeline_projection::find_thread_timeline_block_by_id(
                &f.store.connection,
                THREAD,
                &crate::assistant_block_id(turn, "html-only"),
            )
            .await
            .unwrap()
            .unwrap();
        let replayed = pioneer_protocol::ThreadReadPosition {
            block_id: block.block_id,
            sort_key: block.sort_key,
        };
        assert_eq!(first, replayed);
        assert_eq!(f.count(&f.member, THREAD).await, 1);
        let read = f
            .store
            .mark_thread_read(crate::MarkThreadReadRequest {
                workspace_id: WS.into(),
                thread_id: THREAD.into(),
                principal_id: f.member.clone(),
                through_turn_id: turn.into(),
                through_block: Some(replayed.clone()),
                read_at_unix: NOW,
            })
            .await
            .unwrap();
        assert_eq!(read.unread_count, 0);
        assert_eq!(
            f.answer(THREAD, turn, "html-only", "repeated").await,
            replayed
        );
        assert_eq!(f.count(&f.member, THREAD).await, 0);
        f.answer(THREAD, turn, "later-final", "late").await;
        assert_eq!(f.count(&f.member, THREAD).await, 1);
        assert_eq!(
            crate::find_thread_read_cursor(&f.store.connection, &f.member, THREAD)
                .await
                .unwrap()
                .unwrap()
                .last_read_sort_key,
            read.cursor.sort_key
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn rollback_retry_and_second_resolution_boundaries() {
    pioneer_postgres_test::with_database(async |fixture| {
        let f = Fixture::new(fixture).await;
        let tx = f.store.connection.begin().await.unwrap();
        insert_workspace_membership(
            &tx,
            &NewWorkspaceMembership {
                gateway_id: f.gateway.clone(),
                principal_id: f.member.clone(),
                workspace_id: WS.into(),
                granted_by: PersistedActorRef::Principal(f.owner.clone()),
                now: unix_to_datetime(f.next()),
            },
        )
        .await
        .unwrap();
        unread_start::publish_message_position(
            &tx,
            THREAD,
            "rollback-message",
            "rollback-turn",
            format!("{:020}:rollback:200:answer", NOW * 1000),
            f.next(),
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        assert!(
            unread_message_position::Entity::find()
                .filter(unread_message_position::Column::ThreadId.eq(THREAD))
                .filter(unread_message_position::Column::BlockId.eq("rollback-message"))
                .one(&f.store.connection)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            crate::find_workspace_membership(&f.store.connection, &f.member, WS)
                .await
                .unwrap()
                .is_none()
        );
        let ((), ()) = tokio::join!(f.grant_workspace(), f.grant_workspace());
        let turn = "A00000000000000000001";
        f.prompt(&f.thread, turn).await;
        let (first, second) = tokio::join!(
            f.accept_answer(THREAD, turn, "parallel-a", "a"),
            f.accept_answer(THREAD, turn, "parallel-b", "b"),
        );
        for result in [first, second] {
            if let Err(error) = result {
                assert!(
                    crate::turn_event_was_appended_before_error(&error)
                        && error
                            .to_string()
                            .contains("waiting for an earlier turn event"),
                    "parallel publication must either finish or durably defer: {error:#}",
                );
            }
        }
        let replay = f
            .store
            .replay_due_turn_event_projections(f.next(), 4)
            .await?;
        assert_eq!(replay.failed, 0);
        assert_eq!(replay.exhausted, 0);
        f.answer_position(THREAD, turn, "parallel-a").await;
        f.answer_position(THREAD, turn, "parallel-b").await;
        assert_eq!(f.count(&f.member, THREAD).await, 3);
        let tx = f.store.connection.begin().await.unwrap();
        delete_workspace_membership(&tx, &f.gateway, &f.member, WS)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let (_, raced) = tokio::join!(
            f.grant_workspace(),
            f.answer(THREAD, turn, "raced-final", "race")
        );
        let grant = crate::find_workspace_membership(&f.store.connection, &f.member, WS)
            .await
            .unwrap()
            .unwrap();
        let publication = unread_message_position::Entity::find()
            .filter(unread_message_position::Column::ThreadId.eq(THREAD.to_owned()))
            .filter(unread_message_position::Column::BlockId.eq(raced.block_id))
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            f.count(&f.member, THREAD).await,
            u64::from(publication.published_at > grant.created_at.timestamp())
        );
        // Same-second access/publication may be excluded. This is an agreed
        // product tolerance, not a reason to fabricate a confirmed read cursor.
        let tx = f.store.connection.begin().await.unwrap();
        delete_workspace_membership(&tx, &f.gateway, &f.member, WS)
            .await
            .unwrap();
        insert_workspace_membership(
            &tx,
            &NewWorkspaceMembership {
                gateway_id: f.gateway.clone(),
                principal_id: f.member.clone(),
                workspace_id: WS.into(),
                granted_by: PersistedActorRef::Principal(f.owner.clone()),
                now: unix_to_datetime(publication.published_at),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(f.count(&f.member, THREAD).await, 0);
        assert!(
            crate::find_thread_read_cursor(&f.store.connection, &f.member, THREAD)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            f.store
                .unread_counts_for_threads(&f.member, &[])
                .await
                .unwrap()
                .is_empty()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn owner_without_invitation_excludes_history_before_principal_creation() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, WS).await;
        let mut thread = sample_thread(WS, THREAD, NOW);
        thread.visibility = Some(ThreadVisibility::Workspace);
        let turn = sample_turn("A00000000000000000001");
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[UserInput::Text {
                    text: "imported history".into(),
                    text_elements: vec![],
                }],
                PersistedActorRef::System,
            )
            .await
            .unwrap();
        let reply = |id: &str| pioneer_protocol::ItemCompletedNotification {
            workspace_id: WS.into(),
            thread_id: THREAD.into(),
            turn_id: turn.id.clone(),
            item: TurnItem::AgentMessage {
                id: id.into(),
                text: "answer".into(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            },
        };
        store
            .materialize_item_completed(reply("old"), NOW + 1)
            .await
            .unwrap();
        let gateway = GatewayId::new("G00000000000000000001").unwrap();
        let owner = PrincipalId::new("P00000000000000000001").unwrap();
        let tx = store.connection.begin().await.unwrap();
        create_gateway_singleton(&tx, &gateway, 0, unix_to_datetime(NOW + 10))
            .await
            .unwrap();
        create_superuser(
            &tx,
            &owner,
            &gateway,
            "Owner",
            "owner",
            "owner",
            unix_to_datetime(NOW + 10),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert!(
            store
                .unread_counts_for_threads(&owner, &[THREAD.into()])
                .await
                .unwrap()
                .is_empty()
        );
        store
            .materialize_item_completed(reply("after-owner-access"), NOW + 11)
            .await
            .unwrap();
        assert_eq!(
            store
                .unread_counts_for_threads(&owner, &[THREAD.into()])
                .await
                .unwrap()
                .get(THREAD),
            Some(&1)
        );
        assert!(
            crate::find_thread_read_cursor(&store.connection, &owner, THREAD)
                .await
                .unwrap()
                .is_none()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn late_answer_passes_confirmed_logical_position_after_projection_loss() {
    pioneer_postgres_test::with_database(async |fixture| {
        let f = Fixture::new(fixture).await;
        f.grant_workspace().await;
        let old = "A00000000000000000001";
        f.prompt(&f.thread, old).await;
        f.clock.set(NOW + 98);
        f.prompt(&f.thread, "Z00000000000000000001").await;
        f.clock.set(NOW + 98);
        let first = f.answer(THREAD, old, "first", "late answer").await;
        let read = f
            .store
            .mark_thread_read(crate::MarkThreadReadRequest {
                workspace_id: WS.into(),
                thread_id: THREAD.into(),
                principal_id: f.member.clone(),
                through_turn_id: old.into(),
                through_block: Some(first),
                read_at_unix: NOW + 100,
            })
            .await
            .unwrap();
        assert_eq!(read.unread_count, 0);
        crate::delete_thread_timeline_blocks_for_thread(&f.store.connection, THREAD)
            .await
            .unwrap();
        // Only the initial access boundary is coarse. A real confirmed cursor is
        // still exact: this completion has the same second but must pass its tick.
        f.clock.set(NOW + 98);
        let second = f.answer(THREAD, old, "second", "new answer").await;
        assert!(second.sort_key > read.cursor.sort_key);
        assert_eq!(f.count(&f.member, THREAD).await, 1);
        assert_eq!(
            crate::find_thread_read_cursor(&f.store.connection, &f.member, THREAD)
                .await
                .unwrap()
                .unwrap()
                .last_read_sort_key,
            read.cursor.sort_key
        );
        Ok(())
    })
    .await
    .unwrap();
}
