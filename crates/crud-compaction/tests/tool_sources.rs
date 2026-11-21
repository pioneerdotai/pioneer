use pioneer_crud_compaction::tool_sources::compaction_tool_item_reference;
use pioneer_crud_conversations::turn_item::{ItemWrite, upsert};
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_postgres_test::{postgres_test, scoped_database};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

#[postgres_test]
async fn shell_reference_reads_metadata_for_block_payload_and_handle_scope() {
    let fixture = scoped_database().await.unwrap();
    let db = fixture.connection();
    db.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE);
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
         VALUES ('thread','ws','','agent','m','p','active','user','workspace');
         INSERT INTO turn(id,thread_id,status,turn_kind,origin)
         VALUES ('turn','thread','completed','conversation','user')",
    )
    .await
    .unwrap();
    let wire = format!(
        r#"{{"storage":{{"kind":"shell","kind":"unrelated"}},"unrelated":"\u0000","output":"{}"}}"#,
        "🔧".repeat(65_536)
    );
    let body = PreparedPayload::new_turn_item(&wire).unwrap();
    let tx = db.begin().await.unwrap();
    let now: sea_orm::entity::prelude::DateTimeWithTimeZone =
        "2026-11-19T00:00:00Z".parse().unwrap();
    upsert(
        &tx,
        ItemWrite {
            id: "stored-shell".into(),
            turn_id: "turn".into(),
            item_id: "shell".into(),
            item_type: "command_execution".into(),
            status: Some("completed".into()),
            created_at: now,
            updated_at: now,
        },
        &body,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let corrupted = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE portable_payload_part SET sha256=decode(repeat('00', 32), 'hex')
             WHERE payload_id=$1 AND codec=1",
            [body.id().into()],
        ))
        .await
        .unwrap()
        .rows_affected();
    assert!(
        corrupted > 1,
        "the retained body has multiple compressed blocks"
    );

    let reader = fixture.connect_read_only().await.unwrap();
    let actual = compaction_tool_item_reference(&reader, "ws", "thread", "turn", "shell")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual.id, "stored-shell");
    assert_eq!(actual.scope, "item:turn");
    assert_eq!(actual.version, "item-revision:1");
    for (workspace, thread, turn, item) in [
        ("other", "thread", "turn", "shell"),
        ("ws", "other", "turn", "shell"),
        ("ws", "thread", "other", "shell"),
        ("ws", "thread", "turn", "other"),
    ] {
        assert!(
            compaction_tool_item_reference(&reader, workspace, thread, turn, item)
                .await
                .unwrap()
                .is_none()
        );
    }
}
