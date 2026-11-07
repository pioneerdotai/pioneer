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
        preflight_values.push(1_u64.into());
        let mut root_values: Vec<Value> = vec![id.into(); root_copies];
        root_values.push(1_u64.into());
        for (statement, expected) in [
            (
                reads.discard_page.bind(&[after, upper]),
                vec![after.into(), upper.into(), META_ROWS.into()],
            ),
            (reads.discard_preflight.bind(&[id]), preflight_values),
            (
                reads.expiry_page.bind(&[after, upper]),
                vec![after.into(), upper.into(), 16_u64.into()],
            ),
            (reads.directly_held.bind(&[id]), root_values),
        ] {
            assert_eq!(statement.db_backend, backend);
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
            vec![after.into(), upper.into(), META_ROWS.into()]
        );
        assert_eq!(
            second.values.unwrap().0,
            vec!["next".into(), "last".into(), META_ROWS.into()]
        );
    }
}
