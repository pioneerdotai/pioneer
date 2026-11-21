use pioneer_crud_conversations::tool_result_access::tool_result_access_source;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use serde_json::json;

#[pioneer_postgres_test::postgres_test]
async fn disclosure_reads_observer_arguments_without_decoding_deep_storage() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection();
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_current) VALUES ('tool-access','Tool access',false);
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
          VALUES ('access-thread','tool-access','','agent','fixture','fixture','active');
         INSERT INTO turn(id,thread_id,status) VALUES ('access-turn','access-thread','completed');",
    )
    .await
    .unwrap();
    let arguments = json!({"thread_id":"protected-source"});
    let mut opaque = json!("saved");
    for _ in 0..180 {
        opaque = json!({"layer":opaque});
    }
    let payload = json!({
        "toolName":"threads_history_read",
        "arguments":arguments,
        "storage":{"kind":"metadata","metadata":opaque}
    });
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload)
         VALUES ('access-row','access-turn','result','dynamic_tool_call','completed',$1::text::json)",
        [payload.to_string().into()],
    ))
    .await
    .unwrap();

    let database = SqliteDatabase::new(fixture.connect_read_only().await.unwrap(), db.clone());
    let found = tool_result_access_source(
        &database,
        "tool-access",
        "access-thread",
        "access-turn",
        "result",
    )
    .await
    .unwrap();
    assert_eq!(
        found,
        Some(json!({
            "itemType":"dynamic_tool_call",
            "toolName":"threads_history_read",
            "arguments":arguments
        }))
    );
    assert!(
        tool_result_access_source(
            &database,
            "foreign",
            "access-thread",
            "access-turn",
            "result"
        )
        .await
        .unwrap()
        .is_none()
    );
}
