//! Closed gate: schema regressions are written but intentionally not executed.
use migration::{Migrator, MigratorTrait};
use pioneer_crud::CrudStore;
use pioneer_entity::html_preview_io_work as work;
use pioneer_sqlite::{SqliteDatabase, SqliteWriteClass, SqliteWriteExecutor};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DbBackend, EntityTrait, Statement, TransactionTrait,
};

const NAME: &str = "m20261101_000103_idle_html_preview_io_work";
struct ThroughHtmlIo;
impl MigratorTrait for ThroughHtmlIo {
    fn migrations() -> Vec<Box<dyn migration::MigrationTrait>> {
        let mut all = Migrator::migrations();
        let index = all.iter().position(|m| m.name() == NAME).unwrap();
        all.truncate(index + 1);
        all
    }
}
async fn scalar(store: &CrudStore, sql: &str) -> i64 {
    store
        .database_connection()
        .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap()
}
#[tokio::test]
async fn fresh_upgrade_down_reapply_preserve_schema_contract_and_only_seed_locators() {
    for upgrade in [false, true] {
        pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("html-io-schema.sqlite");
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
        options
            .max_connections(1)
            .min_connections(1)
            .sqlx_logging(false);
        let connection = Database::connect(options).await.unwrap();
        connection
            .execute_unprepared("PRAGMA foreign_keys=OFF")
            .await
            .unwrap();
        let writer = SqliteWriteExecutor::new(connection);
        if upgrade {
            let before = Migrator::migrations()
                .iter()
                .position(|m| m.name() == NAME)
                .unwrap();
            writer
                .run_migrations::<ThroughHtmlIo>(SqliteWriteClass::Maintenance, Some(before as u32))
                .await
                .unwrap();
        }
        writer
            .run_migrations::<ThroughHtmlIo>(SqliteWriteClass::Maintenance, None)
            .await
            .unwrap();
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
        options
            .max_connections(1)
            .min_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
        let reader = Database::connect(options).await.unwrap();
        let store = CrudStore::new(SqliteDatabase::from_executor(reader, writer.clone()))
            .with_maintenance_access();
        assert!(
            store
                .database_connection()
                .reader_query_only_enabled()
                .await
                .unwrap()
        );
        for reapply in [false, true] {
            if reapply {
                let db = store.database_connection();
                let tx = db.begin().await.unwrap();
                ThroughHtmlIo::down(&*tx, Some(1)).await.unwrap();
                tx.commit().await.unwrap();
                assert_eq!(scalar(&store, "SELECT count(*) n FROM sqlite_master WHERE name IN ('html_preview_io_work','idx_html_preview_io_due')").await, 0);
                writer
                    .run_migrations::<ThroughHtmlIo>(SqliteWriteClass::Maintenance, None)
                    .await
                    .unwrap();
            }
            let rows = work::Entity::find()
                .all(&store.database_connection())
                .await
                .unwrap();
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|r| matches!(
                r.family.as_str(),
                "seed_witness" | "seed_copies"
            ) && r.object_id.is_empty()
                && r.ordinal == 0
                && r.state == "ready"
                && r.seed_version == 0));
            assert_eq!(
                scalar(
                    &store,
                    "SELECT count(*) n FROM pragma_table_info('html_preview_io_work')"
                )
                .await,
                16
            );
            assert_eq!(scalar(&store, "SELECT count(*) n FROM pragma_index_list('html_preview_io_work') WHERE origin='c'").await, 1);
            let db = store.database_connection();
            let duplicate = db.execute_unprepared("INSERT INTO html_preview_io_work SELECT * FROM html_preview_io_work WHERE family='seed_witness'").await;
            assert!(
                duplicate.is_err(),
                "composite primary key must reject duplicate locators"
            );
        }
    }
}
