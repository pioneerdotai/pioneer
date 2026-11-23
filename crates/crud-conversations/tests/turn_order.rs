use pioneer_crud_conversations::turn_order::{
    latest_turn_id_by_creation_order, turn_before_launch_and_intervening_by_creation_order,
    turn_ids_after_by_creation_order,
};
use pioneer_postgres_pools::PostgresDatabase;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

#[pioneer_postgres_test::postgres_test]
async fn cli_predecessor_uses_history_timestamp_sequence_and_insertion_ordinal() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection();
    db.execute_unprepared(
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE); \
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) \
                 VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
             INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES \
                 ('z','thread','completed','conversation','user','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
                 ('a','thread','completed','conversation','user','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
                 ('launch','thread','completed','conversation','user','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
                 ('backdated','thread','completed','conversation','user','2026-01-01T09:00:00Z',CURRENT_TIMESTAMP), \
                 ('occurrence','thread','completed','task_run','system','2026-01-01T10:00:00Z',CURRENT_TIMESTAMP), \
                 ('future','thread','completed','conversation','user','2026-01-01T11:00:00Z',CURRENT_TIMESTAMP)",
        ).await.unwrap();
    db.execute_unprepared(
        "INSERT INTO turn(id,thread_id,status,created_at)
             SELECT 'earlier-'||n,'thread','completed',TIMESTAMPTZ '2025-12-31T00:00:00Z'
               FROM generate_series(1,2500) n;
             ANALYZE turn;
             ANALYZE compaction_turn_creation;",
    )
    .await
    .unwrap();
    let scoped = PostgresDatabase::new(fixture.connect_read_only().await.unwrap(), db.clone());
    let timestamp = chrono::DateTime::parse_from_rfc3339("2026-01-01T10:00:00Z").unwrap();
    assert_eq!(
        turn_before_launch_and_intervening_by_creation_order(
            &scoped,
            "thread",
            "launch",
            "occurrence"
        )
        .await
        .unwrap(),
        Some((Some("a".to_owned()), true)),
    );
    // The range query that locates the next timestamp must seek through
    // an existing thread/timestamp index even when unrelated tail turns
    // grow. Only the selected second is tie-broken by sequence/insertion ordinal.
    let plan = db
        .query_all_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "EXPLAIN (COSTS OFF) SELECT created_at FROM turn WHERE thread_id=$1 \
                 AND created_at>$2 ORDER BY created_at ASC LIMIT 1",
            vec!["thread".into(), timestamp.into()],
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "QUERY PLAN").unwrap())
        .collect::<Vec<_>>();
    assert!(
        plan.iter().any(|detail| {
            detail.contains("Index Cond:")
                && detail.contains("thread_id")
                && detail.contains("created_at")
                && detail.contains('>')
        }),
        "CLI boundary timestamp query must seek rather than rank the full thread: {plan:#?}"
    );
    let bucket_plan = db
        .query_all_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "EXPLAIN (COSTS OFF) SELECT t.id FROM turn t \
                 LEFT JOIN compaction_turn_creation c ON c.turn_id=(t.id COLLATE \"default\") \
                 WHERE t.thread_id=$1 AND t.created_at=$2 \
                 ORDER BY COALESCE(c.sequence,0),t.insertion_ordinal,t.id LIMIT 1",
            vec!["thread".into(), timestamp.into()],
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "QUERY PLAN").unwrap())
        .collect::<Vec<_>>();
    assert!(
        bucket_plan.iter().any(|detail| {
            detail.contains("Index Cond:")
                && detail.contains("thread_id")
                && detail.contains("created_at")
        }),
        "tie-breaking must be confined to the indexed timestamp bucket: {bucket_plan:#?}"
    );
    assert_eq!(
        latest_turn_id_by_creation_order(&scoped, "thread")
            .await
            .unwrap()
            .as_deref(),
        Some("future")
    );
    assert_eq!(
        turn_ids_after_by_creation_order(&scoped, "thread", "a")
            .await
            .unwrap(),
        vec!["launch", "occurrence", "future"]
    );
    db.execute_unprepared("DELETE FROM compaction_turn_creation WHERE turn_id IN ('z','a')")
        .await
        .unwrap();
    // Insertion ordinals preserve the tie order when no creation sequence exists.
    assert_eq!(
        turn_before_launch_and_intervening_by_creation_order(
            &scoped,
            "thread",
            "launch",
            "occurrence"
        )
        .await
        .unwrap(),
        Some((Some("a".to_owned()), true)),
    );
}
