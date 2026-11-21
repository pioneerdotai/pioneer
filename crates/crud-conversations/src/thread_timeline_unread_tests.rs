use super::count_unread_message_blocks_for_threads;
use crate::test_support::insert_unread_test_turn;
use crate::thread_mutations::upsert_thread_with_creator;
use crate::values::unix_to_datetime;
use pioneer_crud_access::identity::{create_gateway_singleton, create_superuser};
use pioneer_protocol::{
    GatewayId, PersistedActorRef, PrincipalId, Thread, ThreadMode, ThreadOriginKind,
    ThreadSidebarVisibility, ThreadStatus,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set, TransactionTrait, sea_query::Expr};

async fn insert_fixture_thread(db: &SqliteDatabase, id: &str, reader: &PrincipalId) {
    let when = 1_700_000_000;
    let timestamp = unix_to_datetime(when);
    let tx = db.begin().await.unwrap();
    upsert_thread_with_creator(
        &tx,
        &sample_thread("batch-unread", id, when),
        &PersistedActorRef::Principal(reader.clone()),
        timestamp,
        timestamp,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

fn sample_thread(workspace_id: &str, thread_id: &str, timestamp: i64) -> Thread {
    Thread {
        context_window: None,
        service_mode: None,
        context_authority: None,
        workspace_id: workspace_id.to_owned(),
        id: thread_id.to_owned(),
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
    }
}

#[pioneer_postgres_test::postgres_test]
async fn unread_batch_seeks_positions_and_preserves_sparse_distinct_membership() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connection().clone(),
    );
    let reader = PrincipalId::new("P00000000000000000001").unwrap();
    let gateway = GatewayId::new("G00000000000000000001").unwrap();
    let now = unix_to_datetime(1_700_000_000);
    create_gateway_singleton(&db, &gateway, 0, now)
        .await
        .unwrap();
    create_superuser(
        &db, &reader, &gateway, "Reader", "reader", "reader", now,
        0, // The fixture contains no completed task runs.
    )
    .await
    .unwrap();
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("batch-unread".into()),
        name: Set("Batch".into()),
        is_active: Set(true),
        is_current: Set(false),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(&db)
    .await
    .unwrap();
    let mut ids = Vec::new();
    // Span more than one bounded SQL batch and exercise the access filters.
    for index in 0..130 {
        let id = format!("batch-{index:03}");
        insert_fixture_thread(&db, &id, &reader).await;
        for key in ["001", "002"] {
            insert_unread_test_turn(
                &db,
                "batch-unread",
                &id,
                &format!("{id}-{key}"),
                key,
                "message",
                Some("P00000000000000000002"),
                false,
            )
            .await;
        }
        if index % 3 != 0 {
            pioneer_entity::thread_read_cursor::Entity::insert(
                pioneer_entity::thread_read_cursor::ActiveModel {
                    principal_id: Set(reader.to_string()),
                    thread_id: Set(id.clone()),
                    last_read_sort_key: Set(if index % 3 == 1 { "001" } else { "" }.into()),
                    last_read_turn_id: Set(format!("{id}-001")),
                    updated_at: Set(unix_to_datetime(1_700_000_200)),
                },
            )
            .exec(&db)
            .await
            .unwrap();
        }
        ids.push(id);
    }
    let counts = count_unread_message_blocks_for_threads(&db, &ids, reader.as_str())
        .await
        .unwrap();
    for (index, id) in ids.iter().enumerate() {
        assert_eq!(counts.get(id), Some(&(if index % 3 == 1 { 1 } else { 2 })));
    }
    ids.extend([
        ids[0].clone(),
        ids[129].clone(),
        ids[0].clone(),
        "no-blocks".into(),
    ]);
    assert_eq!(
        count_unread_message_blocks_for_threads(&db, &ids, reader.as_str())
            .await
            .unwrap(),
        counts
    );
    assert!(
        count_unread_message_blocks_for_threads(&db, &[], reader.as_str())
            .await
            .unwrap()
            .is_empty()
    );
    // A block with a mismatched owning thread must not become unread.
    insert_fixture_thread(&db, "mismatch", &reader).await;
    pioneer_entity::thread_timeline_block::Entity::update_many()
        .col_expr(
            pioneer_entity::thread_timeline_block::Column::ThreadId,
            Expr::value("mismatch"),
        )
        .filter(pioneer_entity::thread_timeline_block::Column::BlockId.eq("user:batch-000-001"))
        .exec(&db)
        .await
        .unwrap();
    assert!(
        count_unread_message_blocks_for_threads(&db, &["mismatch".into()], reader.as_str())
            .await
            .unwrap()
            .is_empty()
    );
    pioneer_entity::thread_read_cursor::Entity::update_many()
        .col_expr(
            pioneer_entity::thread_read_cursor::Column::LastReadSortKey,
            Expr::value("999"),
        )
        .filter(pioneer_entity::thread_read_cursor::Column::ThreadId.eq("batch-001"))
        .exec(&db)
        .await
        .unwrap();
    assert!(
        count_unread_message_blocks_for_threads(&db, &["batch-001".into()], reader.as_str())
            .await
            .unwrap()
            .is_empty()
    );
    // Deleted and hidden parent threads do not expose unread messages.
    pioneer_entity::thread_read_cursor::Entity::delete_many()
        .filter(pioneer_entity::thread_read_cursor::Column::ThreadId.eq("batch-002"))
        .exec(&db)
        .await
        .unwrap();
    pioneer_entity::thread::Entity::delete_by_id("batch-002")
        .exec(&db)
        .await
        .unwrap();
    assert!(
        count_unread_message_blocks_for_threads(&db, &["batch-002".into()], reader.as_str())
            .await
            .unwrap()
            .is_empty()
    );
    pioneer_entity::thread::Entity::update_many()
        .col_expr(
            pioneer_entity::thread::Column::SidebarVisibility,
            Expr::val("hidden"),
        )
        .filter(pioneer_entity::thread::Column::Id.eq("batch-004"))
        .exec(&db)
        .await
        .unwrap();
    assert!(
        count_unread_message_blocks_for_threads(&db, &["batch-004".into()], reader.as_str())
            .await
            .unwrap()
            .is_empty()
    );
    pioneer_entity::thread_read_cursor::Entity::delete_many()
        .filter(pioneer_entity::thread_read_cursor::Column::ThreadId.is_in(ids))
        .exec(&db)
        .await
        .unwrap();
    pioneer_entity::workspace::Entity::delete_by_id("batch-unread")
        .exec(&db)
        .await
        .unwrap();
    assert!(
        count_unread_message_blocks_for_threads(&db, &["batch-003".into()], reader.as_str())
            .await
            .unwrap()
            .is_empty()
    );
}
