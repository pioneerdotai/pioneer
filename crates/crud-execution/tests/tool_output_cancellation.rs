use pioneer_crud_execution::tool_output::{page, record_output};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, TransactionTrait};

#[pioneer_postgres_test::postgres_test]
async fn cancelled_tool_output_write_reopens_with_a_read_only_pool() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    for statement in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        writer.execute_unprepared(statement).await.unwrap();
    }
    let reader = fixture.connect_read_only().await.unwrap();
    let database = SqliteDatabase::new(reader, writer);
    assert!(database.reader_query_only_enabled().await.unwrap());
    let scoped = database.maintenance();
    let output = pioneer_protocol::ItemDeltaNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item_id: "persisted-shell".into(),
        delta: "before cancellation".into(),
        stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
        payload: None,
        markdown: None,
        markdown_version: None,
    };
    record_output(&scoped, "physical-output", &output)
        .await
        .unwrap();
    let held = database.begin().await.unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            record_output(&database, "cancelled-output", &output)
        )
        .await
        .is_err()
    );

    held.rollback().await.unwrap();
    let rows = page(&scoped, "ws", "thread", "turn", "persisted-shell", 0)
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "cancelled queued write must not persist later"
    );
    record_output(&scoped, "after-cancel", &output)
        .await
        .unwrap();

    drop(scoped);
    drop(database);
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let reopened = SqliteDatabase::new(reader, writer).maintenance();
    let rows = page(&reopened, "ws", "thread", "turn", "persisted-shell", 0)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|(_, row)| row.text == "before cancellation")
    );
}
