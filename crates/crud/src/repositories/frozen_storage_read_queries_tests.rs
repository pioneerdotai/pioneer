use super::*;

// Dialect and parameter transport are independent contracts: SQLite's
// optimization must not become SQL syntax sent to PostgreSQL or MySQL.
#[test]
fn cached_reads_keep_dialect_and_bound_inputs_separate() {
    for backend in [DbBackend::Sqlite, DbBackend::Postgres, DbBackend::MySql] {
        let reads = FrozenReads::get(backend).unwrap();
        let after = "a'雪; --";
        let upper = "z\"\\雪";
        let id = "id' OR 1=1 --";
        let id_copies = if backend == DbBackend::Sqlite { 1 } else { 3 };
        let root_copies = if backend == DbBackend::Sqlite { 1 } else { 5 };
        let mut preflight_values: Vec<Value> = vec![id.into(); id_copies];
        if backend != DbBackend::Sqlite {
            preflight_values.push(1_u64.into());
        }
        let mut root_values: Vec<Value> = vec![id.into(); root_copies];
        if backend != DbBackend::Sqlite {
            root_values.push(1_u64.into());
        }
        let page_values = |after: &str, upper: &str, limit: u64| {
            let mut values: Vec<Value> = vec![after.into(), upper.into()];
            if backend != DbBackend::Sqlite {
                values.push(limit.into());
            }
            values
        };
        for (statement, expected, limit) in [
            (
                reads.discard_page.bind(&[after, upper]),
                page_values(after, upper, META_ROWS),
                META_ROWS,
            ),
            (reads.discard_preflight.bind(&[id]), preflight_values, 1),
            (
                reads.expiry_page.bind(&[after, upper]),
                page_values(after, upper, 16),
                16,
            ),
            (reads.directly_held.bind(&[id]), root_values, 1),
        ] {
            assert_eq!(statement.db_backend, backend);
            if backend == DbBackend::Sqlite {
                assert!(
                    statement.sql.ends_with(&format!(" LIMIT {limit}")),
                    "all full SQLite read shapes need a constant limit"
                );
            }
            assert_eq!(statement.values.unwrap().0, expected);
            assert!(!statement.sql.contains(after));
            assert!(!statement.sql.contains(upper));
            assert!(!statement.sql.contains(id));
            assert_eq!(
                statement.sql.contains("AS MATERIALIZED"),
                backend == DbBackend::Sqlite,
                "materialization must be SQLite-only: {backend:?}"
            );
            if backend != DbBackend::Sqlite {
                assert!(!statement.sql.contains(INPUT_TABLE));
                assert!(!statement.sql.contains("AS BLOB"));
            }
        }
        // Rebinding a later call cannot mutate a statement already dispatched
        // or leave a prior cursor in the shared SQL template.
        let first = reads.discard_page.bind(&[after, upper]);
        let second = reads.discard_page.bind(&["next", "last"]);
        assert_eq!(
            first.values.unwrap().0,
            page_values(after, upper, META_ROWS)
        );
        assert_eq!(
            second.values.unwrap().0,
            page_values("next", "last", META_ROWS)
        );
    }
}

// Existing lifetime regressions cover candidate/hold decisions and page
// continuation with data. This protects a different adapter risk: the complete
// cached SQL must execute through the query-only pool and retain index ordering.
#[tokio::test]
async fn complete_sqlite_reads_execute_without_page_sort_on_the_read_only_pool() {
    use migration::{Migrator, MigratorTrait};
    use sea_orm::{ConnectOptions, Database};

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cached-frozen-reads.sqlite");
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    writer
        .execute_unprepared("PRAGMA foreign_keys=OFF")
        .await
        .unwrap();
    Migrator::up(&writer, None).await.unwrap();

    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(1)
        .min_connections(1)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let store = CrudStore::new(pioneer_sqlite::SqliteDatabase::new(reader, writer))
        .with_maintenance_access();
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
    let reads = FrozenReads::get(DbBackend::Sqlite).unwrap();
    for (after, upper) in [("", "z"), ("a'雪; --", "z\"\\雪")] {
        for statement in [
            reads.discard_page.bind(&[after, upper]),
            reads.expiry_page.bind(&[after, upper]),
            reads.discard_preflight.bind(&[after]),
            reads.directly_held.bind(&[after]),
        ] {
            assert!(
                store
                    .connection
                    .query_all_raw(statement)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        for mut page in [
            reads.discard_page.bind(&[after, upper]),
            reads.expiry_page.bind(&[after, upper]),
        ] {
            page.sql.insert_str(0, "EXPLAIN QUERY PLAN ");
            let plan = store.connection.query_all_raw(page).await.unwrap();
            assert!(!plan.is_empty());
            for row in plan {
                let detail: String = row.try_get("", "detail").unwrap();
                assert!(
                    !detail.to_ascii_uppercase().contains("TEMP B-TREE"),
                    "the physical page must keep its existing index order: {detail}"
                );
            }
        }
    }
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
}
