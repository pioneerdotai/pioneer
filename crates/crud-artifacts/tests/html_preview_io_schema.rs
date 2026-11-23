use anyhow::{Context, Result};
use migration::{Migrator, MigratorTrait};
use pioneer_entity::html_preview_io_work as work;
use pioneer_postgres_test::with_empty_database;
use sea_orm::{ConnectionTrait, DbBackend, EntityTrait, Set, Statement};

const HISTORY_STORAGE: &str = "m20261119_000007_history_storage";

struct ThroughHistoryStorage;

impl MigratorTrait for ThroughHistoryStorage {
    fn migrations() -> Vec<Box<dyn migration::MigrationTrait>> {
        let mut migrations = Migrator::migrations();
        let history = migrations
            .iter()
            .position(|migration| migration.name() == HISTORY_STORAGE)
            .expect("missing history migration");
        migrations.truncate(history + 1);
        migrations
    }
}

async fn scalar<T: sea_orm::TryGetable>(db: &impl ConnectionTrait, sql: &str) -> Result<T> {
    Ok(db
        .query_one_raw(Statement::from_string(DbBackend::Postgres, sql))
        .await?
        .context("missing schema assertion result")?
        .try_get("", "value")?)
}

#[tokio::test]
async fn install_rollback_and_reapply_keep_an_empty_queue_and_unique_point_locators() -> Result<()>
{
    for partial in [false, true] {
        with_empty_database(async |fixture| {
            let db = fixture.connection();
            if partial {
                let before = u32::try_from(ThroughHistoryStorage::migrations().len() - 1)?;
                ThroughHistoryStorage::up(db, Some(before)).await?;
            }
            ThroughHistoryStorage::up(db, None).await?;
            let reader = fixture.connect_read_only().await?;
            assert_eq!(
                scalar::<String>(&reader, "SELECT current_setting('transaction_read_only') AS value").await?,
                "on",
            );
            for reapply in [false, true] {
                if reapply {
                    ThroughHistoryStorage::down(db, Some(1)).await?;
                    assert_eq!(
                        scalar::<i64>(&reader, "SELECT count(*) AS value FROM pg_class WHERE relname IN ('html_preview_io_work', 'idx_html_preview_io_due')").await?,
                        0,
                    );
                    ThroughHistoryStorage::up(db, None).await?;
                }
                assert!(work::Entity::find().all(&reader).await?.is_empty());
                assert!(
                    scalar::<bool>(&reader, "SELECT to_regclass('public.html_preview_index_backfill') IS NULL AS value").await?
                );
                assert_eq!(
                    scalar::<i64>(&reader, "SELECT count(*) AS value FROM information_schema.columns WHERE table_schema='public' AND table_name='html_preview_io_work'").await?,
                    16,
                );
                assert_eq!(
                    scalar::<i64>(&reader, "SELECT count(*) AS value FROM pg_index WHERE indrelid='html_preview_io_work'::regclass AND NOT indisprimary").await?,
                    1,
                );
                let point = work::ActiveModel {
                    family: Set("witness".into()),
                    object_id: Set("manifest-locator".into()),
                    ordinal: Set(7),
                    incarnation: Set("incarnation".into()),
                    requested_revision: Set("revision".into()),
                    pass_revision: Set(String::new()),
                    workspace_id: Set("workspace".into()),
                    state: Set("ready".into()),
                    due_at: Set(Some(0)),
                    ..Default::default()
                };
                work::Entity::insert(point.clone()).exec_without_returning(db).await?;
                assert!(work::Entity::insert(point).exec_without_returning(db).await.is_err());
                assert_eq!(work::Entity::find().all(&reader).await?.len(), 1);
            }
            Ok(())
        })
        .await?;
    }
    Ok(())
}
