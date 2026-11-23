use pioneer_crud::CrudStore;
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_postgres_test::{TestDatabase, with_database};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

async fn open(fixture: &TestDatabase) -> CrudStore {
    CrudStore::new(PostgresDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connect().await.unwrap(),
    ))
    .with_maintenance_access()
}

async fn scalar(store: &CrudStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}

async fn fixture(db: &TestDatabase) -> CrudStore {
    pioneer_crud_compaction::plan_test_support::seed_payload_sources(db.connection()).await;
    open(db).await
}

fn stage_one_completion() -> pioneer_protocol::ItemCompletedNotification {
    pioneer_protocol::ItemCompletedNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item: pioneer_protocol::TurnItem::SystemEvent {
            id: "stage-one-item".into(),
            level: pioneer_protocol::SystemEventLevel::Info,
            message: "точные portable bytes 🦀\n".repeat(12000),
            code: Some("stage.one.fixture".into()),
            details: None,
        },
    }
}

#[tokio::test]
async fn portable_turn_payload_real_producer_restart_retry_and_corruption() {
    with_database(async |db| {
    let store = fixture(db).await;
    // Remove the unrelated noncanonical fixture bodies. The real producer below
    // owns sequence allocation, item projection, receipts and watermark.
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event; DELETE FROM turn_item")
        .await
        .unwrap();
    let completion = stage_one_completion();
    store
        .materialize_item_snapshot_updated(
            pioneer_protocol::ItemUpdatedNotification {
                workspace_id: completion.workspace_id.clone(),
                thread_id: completion.thread_id.clone(),
                turn_id: completion.turn_id.clone(),
                item: completion.item.clone(),
            },
            1_700_000_099,
        )
        .await
        .unwrap();
    // A standalone snapshot persists an encoded item without a canonical event.
    assert_eq!(scalar(&store, "SELECT count(*) n FROM turn_event").await, 0);
    assert_eq!(
        scalar(
            &store,
            "SELECT count(*) n FROM turn_item WHERE payload_payload_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        store.get_turn_item("turn", "stage-one-item").await.unwrap(),
        Some(completion.item.clone())
    );
    store
        .materialize_item_completed(completion.clone(), 1_700_000_100)
        .await
        .unwrap();
    store
        .materialize_item_completed(completion.clone(), 1_700_000_100)
        .await
        .unwrap();
    assert_eq!(
        store.get_turn_item("turn", "stage-one-item").await.unwrap(),
        Some(completion.item.clone())
    );
    drop(store);
    let reopened = open(db).await;
    assert_eq!(
        scalar(
            &reopened,
            "SELECT count(*) n FROM turn_event WHERE payload_payload_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        scalar(
            &reopened,
            "SELECT count(*) n FROM turn_item WHERE payload_payload_id IS NOT NULL"
        )
        .await,
        1
    );
    assert_eq!(
        reopened
            .get_turn_item("turn", "stage-one-item")
            .await
            .unwrap(),
        Some(completion.item.clone())
    );
    // Replay uses the exact bytes of the persisted event.
    reopened
        .materialize_item_completed(completion.clone(), 1_700_000_100)
        .await
        .unwrap();
    assert_eq!(
        scalar(&reopened, "SELECT count(*) n FROM turn_event").await,
        1
    );
    assert_eq!(scalar(&reopened, "SELECT count(DISTINCT payload_id) n FROM portable_payload_part WHERE codec=1").await,2);
    reopened.database_connection().execute_unprepared(
        "DELETE FROM portable_payload_part WHERE payload_id=(SELECT payload_payload_id FROM turn_item WHERE item_id='stage-one-item') AND byte_offset=0"
    ).await.unwrap();
    let error = reopened
        .get_turn_item("turn", "stage-one-item")
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("portable payload"),
        "{error:#}"
    );
    // Remove that independent fault before testing the event header. Otherwise
    // a later item preflight could make the next negative control pass falsely.
    reopened
        .database_connection()
        .execute_unprepared("DELETE FROM turn_item WHERE item_id='stage-one-item'")
        .await
        .unwrap();
    reopened
        .materialize_item_completed(completion.clone(), 1_700_000_100)
        .await
        .unwrap();
    reopened.database_connection().execute_unprepared("DELETE FROM portable_payload_part WHERE payload_id=(SELECT payload_payload_id FROM turn_event LIMIT 1) AND byte_offset=0").await.unwrap();
    let error = reopened
        .materialize_item_completed(completion, 1_700_000_100)
        .await
        .unwrap_err();
    assert!(
        format!("{error:#}").contains("portable"),
        "missing block lost its portable error: {error:#}"
    );
        Ok(())
    }).await.unwrap();
}
