use super::*;

#[test]
fn cached_reads_bind_each_input_without_caching_eligibility() {
    let reads = FrozenReads::get();
    let after = "a'雪; --";
    let upper = "z\"\\雪";
    let id = "id' OR 1=1 --";
    let page_values =
        |after: &str, upper: &str, limit: u64| vec![after.into(), upper.into(), limit.into()];
    for (statement, expected) in [
        (
            reads.discard_page.bind(&[after, upper]),
            page_values(after, upper, META_ROWS),
        ),
        (
            reads.expiry_page.bind(&[after, upper]),
            page_values(after, upper, 16),
        ),
        (
            reads.discard_preflight.bind(&[id]),
            vec![id.into(), id.into(), id.into(), 1_u64.into()],
        ),
        (
            reads.directly_held.bind(&[id]),
            vec![
                id.into(),
                id.into(),
                id.into(),
                id.into(),
                id.into(),
                1_u64.into(),
            ],
        ),
    ] {
        assert_eq!(statement.db_backend, DbBackend::Postgres);
        assert_eq!(statement.values.unwrap().0, expected);
        assert!(!statement.sql.contains(after));
        assert!(!statement.sql.contains(upper));
        assert!(!statement.sql.contains(id));
    }
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

#[pioneer_postgres_test::postgres_test]
async fn complete_postgres_reads_keep_physical_page_order_in_the_read_only_pool() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let db = pioneer_postgres_pools::PostgresDatabase::new(reader, fixture.connection().clone())
        .maintenance();
    assert!(db.reader_is_read_only().await.unwrap());

    for (after, upper) in [("", "z"), ("a'雪; --", "z\"\\雪")] {
        let reads = FrozenReads::get();
        for statement in [
            reads.discard_page.bind(&[after, upper]),
            reads.expiry_page.bind(&[after, upper]),
            reads.discard_preflight.bind(&[after]),
            reads.directly_held.bind(&[after]),
        ] {
            assert!(db.query_all_raw(statement).await.unwrap().is_empty());
        }
        for mut page in [
            reads.discard_page.bind(&[after, upper]),
            reads.expiry_page.bind(&[after, upper]),
        ] {
            page.sql.insert_str(0, "EXPLAIN (COSTS OFF) ");
            let plan: Vec<String> = db
                .query_all_raw(page)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.try_get("", "QUERY PLAN").unwrap())
                .collect();
            assert!(plan.iter().any(|row| row.contains("Index Scan")));
            assert!(!plan.iter().any(|row| row.contains("Sort")), "{plan:#?}");
        }
    }
    assert!(db.reader_is_read_only().await.unwrap());
}
