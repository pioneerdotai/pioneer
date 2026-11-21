use migration::{Migrator, MigratorTrait};
use pioneer_crud_compaction::store::CompactionStore;
use pioneer_crud_conversations::turn_item::{ItemWrite, upsert};
use pioneer_crud_payloads::storage::PreparedPayload;
use pioneer_entity::compaction_item_revision as revision;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, EntityTrait, TransactionTrait};

async fn store(fixture: &pioneer_postgres_test::TestDatabase) -> CompactionStore {
    let read = fixture.connect_read_only().await.unwrap();
    let write = fixture.connect_with_limit(1).await.unwrap();
    let db = SqliteDatabase::new(read, write);
    assert!(db.reader_query_only_enabled().await.unwrap());
    CompactionStore::new(db, Default::default()).with_maintenance_access()
}

async fn put_item(store: &CompactionStore, data: &str) {
    let body = PreparedPayload::new_turn_item(data).unwrap();
    let tx = store.database_connection().begin().await.unwrap();
    let now = "2026-11-19T00:00:00Z".parse().unwrap();
    upsert(
        &tx,
        ItemWrite {
            id: "item".into(),
            turn_id: "turn".into(),
            item_id: "item".into(),
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
}

async fn assert_revision(store: &CompactionStore, id: &str, version: i64, present: i64) {
    let db = store.database_connection();
    let row = revision::Entity::find_by_id(id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.turn_id, "turn");
    assert_eq!(row.revision, version);
    assert_eq!(row.present, present);
}

#[tokio::test]
async fn item_revisions_and_fragments_survive_database_reopen_with_whole_and_block_payloads() {
    for blocked in [false, true] {
        pioneer_postgres_test::with_database(async |fixture| {
            let scoped = store(fixture).await;
            let db = scoped.database_connection();
            db.execute_unprepared(
                "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE);
                 INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class)
                 VALUES ('thread','ws','','agent','m','p','active','user','workspace');
                 INSERT INTO turn(id,thread_id,status,turn_kind,origin)
                 VALUES ('turn','thread','completed','conversation','user');
                 INSERT INTO compaction_item_revision(source_id,turn_id,revision,present)
                 VALUES ('deleted','turn',9,0);",
            )
            .await?;
            let output = if blocked {
                format!("retained result {}", "🧰".repeat(20_000))
            } else {
                "retained result".to_owned()
            };
            let original_data = serde_json::json!({
                "storage": { "kind": "shell" },
                "output": output,
            })
            .to_string();
            put_item(&scoped, &original_data).await;
            db.execute_unprepared("UPDATE compaction_item_revision SET revision=7 WHERE source_id='item'").await?;
            assert_revision(&scoped, "item", 7, 1).await;
            assert_revision(&scoped, "deleted", 9, 0).await;
            drop(db);
            drop(scoped);

            let reopened = store(fixture).await;
            let db = reopened.database_connection();
            Migrator::up(fixture.connection(), None).await?;
            assert!(Migrator::get_pending_migrations(fixture.connection()).await?.is_empty());
            assert_revision(&reopened, "item", 7, 1).await;
            assert_revision(&reopened, "deleted", 9, 0).await;
            let original = reopened
                .compaction_tool_result_fragment("ws", "thread", "turn", "item", None, 0)
                .await?.unwrap();
            assert_eq!(original.reference.version, "item-revision:7");
            assert!(original.text.contains("retained result"));

            let changed_data = serde_json::json!({
                "storage": { "kind": "shell" },
                "output": format!("changed {output}"),
            })
            .to_string();
            put_item(&reopened, &changed_data).await;
            assert!(
                reopened
                    .compaction_reference_fragment("ws", "thread", &original.reference, 0)
                    .await
                    .is_err()
            );
            assert_revision(&reopened, "item", 8, 1).await;

            db.execute_unprepared("DELETE FROM turn_item WHERE id='item'").await?;
            assert_revision(&reopened, "item", 9, 0).await;
            put_item(&reopened, "{}").await;
            assert_revision(&reopened, "item", 10, 1).await;
            assert_eq!(
                reopened
                    .compaction_history_turn_page(
                        "ws",
                        "thread",
                        "",
                        &reopened.compaction_history_read_fence().await?,
                    )
                    .await?.len(),
                1
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}
