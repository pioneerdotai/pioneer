// Schema lifecycle proof only; planner/scan claims require separate measurements.
use migration::{Migrator, MigratorTrait};
use pioneer_entity::agent_action_outbox;
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, Database, DbBackend, EntityTrait, Set, Statement,
};

const MIGRATION: &str = "m20261106_000001_agent_ledger_deadline_index";
const INDEX: &str = "idx_agent_action_outbox_status_created_id";

#[tokio::test]
async fn ledger_deadline_index_fresh_upgrade_down_reapply_preserves_rows() {
    for fresh in [true, false] {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        let migrations = Migrator::migrations();
        let position = migrations
            .iter()
            .position(|m| m.name() == MIGRATION)
            .unwrap();
        if fresh {
            Migrator::up(&db, None).await.unwrap();
        } else {
            Migrator::up(&db, Some(position as u32)).await.unwrap();
        }
        let created = chrono::DateTime::from_timestamp(1_700_000_000, 123_000_000)
            .unwrap()
            .fixed_offset();
        let original = agent_action_outbox::ActiveModel {
            id: Set("preserved-ledger-row".into()),
            action_id: Set("legacy-action".into()),
            owner_execution_id: Set("legacy-owner".into()),
            payload_json: Set("legacy-payload".into()),
            status: Set("delivered".into()),
            attempts: Set(1),
            next_attempt_at: Set(None),
            delivered_at: Set(Some(created)),
            last_error: Set(None),
            created_at: Set(created),
        }
        .insert(&db)
        .await
        .unwrap();
        if !fresh {
            Migrator::up(&db, Some(1)).await.unwrap();
        }
        for installed in [true, false, true] {
            if !installed {
                Migrator::down(&db, Some(1)).await.unwrap();
            } else {
                // Idempotent Migrator call on fresh/upgrade; reinstalls after down.
                Migrator::up(&db, None).await.unwrap();
            }
            let columns = db
                .query_all_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    format!("PRAGMA index_info('{INDEX}')"),
                ))
                .await
                .unwrap();
            let names: Vec<String> = columns
                .iter()
                .map(|r| r.try_get("", "name").unwrap())
                .collect();
            if installed {
                assert_eq!(names, ["status", "created_at", "id"]);
            } else {
                assert!(names.is_empty());
            }
            assert_eq!(
                agent_action_outbox::Entity::find_by_id("preserved-ledger-row")
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap(),
                original
            );
        }
    }
}
