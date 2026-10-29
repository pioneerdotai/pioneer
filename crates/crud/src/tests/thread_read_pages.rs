use super::{
    migrations_before, migrations_through, test_store_with_workspace,
    test_store_with_workspace_migrator,
};
use migration::{MigratorTrait, SchemaManager};
use pioneer_protocol::{
    PersistedActorRef, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
};
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DatabaseBackend, EntityTrait, Set, Statement,
    TransactionTrait,
};

const CREATION_ORDER_MIGRATION: &str = "m20261029_000001_thread_read_creation_order";
struct BeforeCreationOrder;
impl MigratorTrait for BeforeCreationOrder {
    fn migrations() -> Vec<Box<dyn migration::MigrationTrait>> {
        migrations_before(CREATION_ORDER_MIGRATION)
    }
}
struct ThroughCreationOrder;
impl MigratorTrait for ThroughCreationOrder {
    fn migrations() -> Vec<Box<dyn migration::MigrationTrait>> {
        migrations_through(CREATION_ORDER_MIGRATION)
    }
}

#[tokio::test]
async fn creation_order_index_installs_upgrades_and_rolls_back_without_changing_threads() {
    for upgrade in [false, true] {
        let store = if upgrade {
            test_store_with_workspace_migrator::<BeforeCreationOrder>("creation-index").await
        } else {
            test_store_with_workspace_migrator::<ThroughCreationOrder>("creation-index").await
        };
        store
            .upsert_thread_model(
                &thread("retained", "creation-index"),
                PersistedActorRef::System,
            )
            .await
            .unwrap();
        let before = store
            .list_thread_read_candidates_page("creation-index", None, 10)
            .await
            .unwrap();
        let transaction = store.connection.begin().await.unwrap();
        let schema = SchemaManager::new(&*transaction);
        assert_eq!(
            schema
                .has_index("thread", "idx_thread_workspace_created_id")
                .await
                .unwrap(),
            !upgrade
        );
        ThroughCreationOrder::up(&*transaction, None).await.unwrap();
        let columns = transaction
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "PRAGMA index_info('idx_thread_workspace_created_id')",
            ))
            .await
            .unwrap();
        assert_eq!(
            columns
                .into_iter()
                .map(|row| row.try_get::<String>("", "name").unwrap())
                .collect::<Vec<_>>(),
            ["workspace_id", "created_at", "id"]
        );
        ThroughCreationOrder::down(&*transaction, Some(1))
            .await
            .unwrap();
        assert!(
            !schema
                .has_index("thread", "idx_thread_workspace_created_id")
                .await
                .unwrap()
        );
        ThroughCreationOrder::up(&*transaction, None).await.unwrap();
        assert!(
            schema
                .has_index("thread", "idx_thread_workspace_created_id")
                .await
                .unwrap()
        );
        transaction.commit().await.unwrap();
        assert_eq!(
            store
                .list_thread_read_candidates_page("creation-index", None, 10)
                .await
                .unwrap(),
            before
        );
    }
}

fn thread(id: &str, workspace: &str) -> Thread {
    Thread {
        workspace_id: workspace.into(),
        id: id.into(),
        name: Some(id.into()),
        preview: "saved preview".into(),
        preview_author: None,
        mode: ThreadMode::Agent,
        model: "test".into(),
        model_provider: "test".into(),
        reasoning_effort: None,
        created_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        status: ThreadStatus::Closed,
        origin_kind: ThreadOriginKind::System,
        sidebar_visibility: ThreadSidebarVisibility::Hidden,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: vec![],
    }
}

#[tokio::test]
async fn observation_pages_include_hidden_archived_and_keep_position_after_updates() {
    let store = test_store_with_workspace("read-pages").await;
    // IDs deliberately disagree with chronology. Adjacent dates differ by one
    // nanosecond; b/c tie and must cross the first page boundary by descending ID.
    for (id, date) in [
        ("z-old", "2026-01-01T00:00:00.123456000+00:00"),
        ("a-new", "2026-01-01T00:00:01+00:00"),
        ("b-tie", "2026-01-01T00:00:00.123456789+00:00"),
        ("c-tie", "2026-01-01T00:00:00.123456789+00:00"),
        ("d-fraction", "2026-01-01T00:00:00.123456788+00:00"),
    ] {
        store
            .upsert_thread_model(&thread(id, "read-pages"), PersistedActorRef::System)
            .await
            .unwrap();
        let mut row: pioneer_entity::thread::ActiveModel =
            pioneer_entity::thread::Entity::find_by_id(id)
                .one(&store.connection)
                .await
                .unwrap()
                .unwrap()
                .into();
        row.created_at = Set(chrono::DateTime::parse_from_rfc3339(date).unwrap());
        row.update(&store.connection).await.unwrap();
    }
    let first = store
        .list_thread_read_candidates_page("read-pages", None, 2)
        .await
        .unwrap();
    assert_eq!(
        first.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        ["a-new", "c-tie"]
    );
    // Changes to mutable activity fields must not move rows across the cursor.
    let mut updated = thread("a-new", "read-pages");
    updated.updated_at += 100;
    store
        .upsert_thread_model(&updated, PersistedActorRef::System)
        .await
        .unwrap();
    // The position is self-contained even after its source row disappears.
    pioneer_entity::thread::Entity::delete_by_id("c-tie")
        .exec(&store.connection)
        .await
        .unwrap();
    let mut newest = thread("0-latest", "read-pages");
    newest.created_at = 1_900_000_000;
    store
        .upsert_thread_model(&newest, PersistedActorRef::System)
        .await
        .unwrap();
    let next = store
        .list_thread_read_candidates_page("read-pages", first.last(), 2)
        .await
        .unwrap();
    assert_eq!(
        next.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        ["b-tie", "d-fraction"]
    );
    let tail = store
        .list_thread_read_candidates_page("read-pages", next.last(), 2)
        .await
        .unwrap();
    assert_eq!(
        tail.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        ["z-old"]
    );
    let restarted = store
        .list_thread_read_candidates_page("read-pages", None, 1)
        .await
        .unwrap();
    assert_eq!(restarted[0].id, "0-latest");
    assert!(
        store
            .list_thread_read_candidates_page("other-workspace", None, 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_thread_read_candidates_page("read-pages", tail.last(), 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_thread_read_candidates_page("read-pages", None, 201)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn child_pages_continue_beyond_128_and_are_scoped_to_actual_parent() {
    let store = test_store_with_workspace("child-pages").await;
    let mut rows = Vec::new();
    for i in 0..151 {
        rows.push(pioneer_entity::thread_lineage::ActiveModel {
            child_thread_id: Set(format!("child-{i:03}")),
            parent_thread_id: Set("intermediate".into()),
            root_thread_id: Set("root".into()),
            depth: Set(2),
            created_at: Set(crate::util::unix_to_datetime(1_700_000_000)),
            origin_kind: Set(None),
            created_by_thread_id: Set(None),
            created_by_turn_id: Set(None),
        });
    }
    for (id, parent, depth) in [("sibling", "root", 1), ("grandchild", "child-000", 3)] {
        rows.push(pioneer_entity::thread_lineage::ActiveModel {
            child_thread_id: Set(id.into()),
            parent_thread_id: Set(parent.into()),
            root_thread_id: Set("root".into()),
            depth: Set(depth),
            created_at: Set(crate::util::unix_to_datetime(1_700_000_000)),
            origin_kind: Set(None),
            created_by_thread_id: Set(None),
            created_by_turn_id: Set(None),
        });
    }
    pioneer_entity::thread_lineage::Entity::insert_many(rows)
        .exec(&store.connection)
        .await
        .unwrap();
    let mut actual = Vec::new();
    let mut after = None;
    loop {
        let page = store
            .list_thread_child_lineage_page("intermediate", after.as_deref(), 17)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        after = page.last().map(|r| r.child_thread_id.clone());
        actual.extend(page.into_iter().map(|r| r.child_thread_id));
    }
    let expected = (0..151)
        .map(|i| format!("child-{i:03}"))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    let grandchildren = store
        .list_thread_child_lineage_page("child-000", None, 17)
        .await
        .unwrap();
    assert_eq!(grandchildren.len(), 1);
    assert_eq!(grandchildren[0].child_thread_id, "grandchild");
    assert!(
        store
            .list_thread_child_lineage_page("intermediate", None, 0)
            .await
            .is_err()
    );
}
