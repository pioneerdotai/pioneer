//! Compile-only regressions for server unread activation and access boundaries.
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
    async fn new() -> Self {
        let store = test_store_with_workspace(WS).await;
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
    async fn answer(
        &self,
        thread: &str,
        turn: &str,
        id: &str,
        text: &str,
    ) -> pioneer_protocol::ThreadReadPosition {
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
            .unwrap();
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
async fn legacy_activation_excludes_history_preserves_real_read_and_survives_reopen() {
    let f = Fixture::new().await;
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
    // A confirmed logical position can survive projection loss before rollout.
    for item in ["old-final", "old-final-2"] {
        crate::delete_thread_timeline_block(
            &f.store.connection,
            &crate::assistant_block_id(old, item),
        )
        .await
        .unwrap();
    }
    // Cancellation/rollback must leave neither a moved activation nor a
    // partially replaced publication ledger. A later retry can commit safely.
    let migration = Migrator::migrations().pop().unwrap();
    let before = gateway_identity::Entity::find_by_id(f.gateway.to_string())
        .one(&f.store.connection)
        .await
        .unwrap()
        .unwrap()
        .unread_started_at;
    let tx = f.store.connection.begin().await.unwrap();
    let manager = migration::SchemaManager::new(&*tx);
    migration.down(&manager).await.unwrap();
    migration.up(&manager).await.unwrap();
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
    // Exercise the actual new migration with populated pre-rollout sources.
    let tx = f.store.connection.begin().await.unwrap();
    let manager = migration::SchemaManager::new(&*tx);
    migration.down(&manager).await.unwrap();
    migration.up(&manager).await.unwrap();
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
    let reopened = CrudStore::new(f.store.connection.clone());
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
}

#[tokio::test]
async fn grants_regrants_and_visibility_use_actual_access_not_first_open() {
    let f = Fixture::new().await;
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
}

#[tokio::test]
async fn replay_edits_and_projection_rebuild_reuse_first_completion_position() {
    let f = Fixture::new().await;
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
    let block = crate::repositories::thread_timeline_projection::find_thread_timeline_block_by_id(
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
}

#[tokio::test]
async fn rollback_retry_and_second_resolution_boundaries() {
    let f = Fixture::new().await;
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
        unread_message_position::Entity::find_by_id((
            THREAD.to_owned(),
            "rollback-message".to_owned()
        ))
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
    let _ = tokio::join!(
        f.answer(THREAD, turn, "parallel-a", "a"),
        f.answer(THREAD, turn, "parallel-b", "b")
    );
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
    let publication =
        unread_message_position::Entity::find_by_id((THREAD.to_owned(), raced.block_id))
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
}

#[tokio::test]
async fn owner_without_invitation_excludes_history_before_principal_creation() {
    let store = test_store_with_workspace(WS).await;
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
}

#[tokio::test]
async fn late_answer_passes_confirmed_logical_position_after_projection_loss() {
    let f = Fixture::new().await;
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
}
