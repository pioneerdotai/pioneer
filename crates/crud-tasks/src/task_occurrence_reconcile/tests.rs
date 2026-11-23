use super::*;
use sea_orm::{DatabaseBackend, Statement};

const NOW: i64 = 4_000_000_000;

#[pioneer_postgres_test::postgres_test]
async fn due_and_scope_plans_use_covering_keyset_indexes() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection();
    let scope = scope::Model {
        task_id: "task".into(),
        generation: 1,
        after_run_id: Some("a".into()),
        upper_run_id: Some("z".into()),
    };
    for (query, index) in [
        (due_query(NOW, 32), "idx_task_occurrence_reconcile_due"),
        (scope_page_query(&scope, 15), "idx_task_run_task_id"),
    ] {
        let mut statement = db.get_database_backend().build(&query);
        statement.sql = format!("EXPLAIN (COSTS OFF) {}", statement.sql);
        let plan: Vec<String> = db
            .query_all_raw(statement)
            .await
            .unwrap()
            .iter()
            .map(|row| row.try_get("", "QUERY PLAN").unwrap())
            .collect();
        assert!(plan.iter().any(|row| row.contains(index)), "{plan:#?}");
        assert!(
            plan.iter()
                .all(|row| !row.contains("Sort") && !row.contains("Seq Scan on task_run")),
            "{plan:#?}"
        );
    }
    // Existing unique bindings remain the lookup indexes; don't duplicate them.
    let rows = db
        .query_all_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT indisunique AS \"unique\" FROM pg_index WHERE indrelid='task_occurrence_contract'::regclass".to_owned(),
        ))
        .await
        .unwrap();
    assert!(
        rows.iter()
            .any(|row| row.try_get::<bool>("", "unique").unwrap())
    );
}
