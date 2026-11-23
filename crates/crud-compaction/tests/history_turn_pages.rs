use pioneer_crud_compaction::SOURCE_PAGE_BYTES;
use pioneer_crud_compaction::history_fence::compaction_history_read_fence;
use pioneer_crud_compaction::history_turn_pages::{
    compaction_history_selected_turn_page, compaction_history_turn_page,
};
use sea_orm::{ConnectionTrait, DbBackend, Statement};

#[pioneer_postgres_test::postgres_test]
async fn accepted_turn_metadata_is_selected_before_boundary_subqueries() -> anyhow::Result<()> {
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let mut connection = fixture.connection().clone();
    let recorded = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Statement>::new()));
    let captured = recorded.clone();
    connection
        .set_metric_callback(move |info| captured.lock().unwrap().push(info.statement.clone()));
    let db = &connection;
    seed(db).await;
    db.execute_unprepared(
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) \
         VALUES ('other-thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .await
    .unwrap();
    for index in 0..32 {
        for thread in ["thread", "other-thread"] {
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) \
                 VALUES ($1,$2,'completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                [format!("{thread}-tail-{index:02}").into(), thread.into()],
            ))
            .await
            .unwrap();
        }
    }
    let fence = compaction_history_read_fence(db).await?;
    recorded.lock().unwrap().clear();
    let page = compaction_history_selected_turn_page(db, "ws", "thread", &["turn".into()], &fence)
        .await
        .unwrap();
    assert_eq!(
        page.iter().map(|turn| turn.id.as_str()).collect::<Vec<_>>(),
        ["turn"]
    );
    let statement = recorded
        .lock()
        .unwrap()
        .iter()
        .find(|statement| statement.sql.contains("input_high_water"))
        .cloned()
        .expect("selected metadata query must be recorded");
    assert!(statement.sql.contains(" IN "), "{statement:?}");
    let mut plan_statement = statement;
    plan_statement.sql = format!("EXPLAIN (COSTS OFF) {}", plan_statement.sql);
    let plan = db
        .query_all_raw(plan_statement)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get::<String>("", "QUERY PLAN").unwrap())
        .collect::<Vec<_>>();
    assert!(
        plan.iter()
            .any(|detail| detail.contains("Index Scan using turn_pkey on turn"))
            && plan
                .iter()
                .any(|detail| detail.contains("Index Cond:") && detail.contains("id = 'turn'")),
        "selected turn metadata must use its exact ID lookup: {plan:#?}"
    );
    assert!(
        compaction_history_selected_turn_page(db, "other", "thread", &["turn".into()], &fence)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        compaction_history_selected_turn_page(
            db,
            "ws",
            "thread",
            &vec!["turn".to_owned(); 65],
            &fence,
        )
        .await
        .is_err(),
        "the selected metadata query must respect its row quantum"
    );
    assert!(
        compaction_history_selected_turn_page(
            db,
            "ws",
            "thread",
            &["x".repeat(SOURCE_PAGE_BYTES + 1)],
            &fence,
        )
        .await
        .is_err(),
        "metadata IDs must respect the same byte quantum as source pages"
    );
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn history_fence_excludes_late_completion_and_replacement_after_delete() -> anyhow::Result<()>
{
    let fixture = pioneer_postgres_test::scoped_database().await?;
    let db = fixture.connection();
    seed(db).await;
    source(db, "before", 1, "before snapshot").await?;
    let fence = compaction_history_read_fence(db).await?;
    source(db, "late", 2, "completed after snapshot").await?;
    let page = compaction_history_turn_page(db, "ws", "thread", "", &fence)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].event_high_water, 1);
    assert!(
        compaction_history_turn_page(db, "other", "thread", "", &fence)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        compaction_history_turn_page(db, "ws", "thread", &page[0].id, &fence)
            .await
            .unwrap()
            .is_empty()
    );
    let next = compaction_history_read_fence(db).await?;
    assert_eq!(
        compaction_history_turn_page(db, "ws", "thread", "", &next)
            .await
            .unwrap()[0]
            .event_high_water,
        2
    );
    // The new canonical event must have a distinct capture revision even
    // after the previous event was removed.
    db.execute_unprepared("DELETE FROM turn_event WHERE id='late'")
        .await
        .unwrap();
    source(db, "replacement", 3, "new after deletion").await?;
    assert_eq!(
        compaction_history_turn_page(db, "ws", "thread", "", &next)
            .await
            .unwrap()[0]
            .event_high_water,
        1
    );
    Ok(())
}

async fn seed(db: &impl ConnectionTrait) {
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
}

async fn source(
    db: &impl ConnectionTrait,
    id: &str,
    sequence: i64,
    payload: &str,
) -> anyhow::Result<()> {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO turn_event (id,thread_id,turn_id,sequence,event_type,payload,created_at)
        VALUES ($1,'thread','turn',$2,'fixture',$3::text::json,CURRENT_TIMESTAMP)",
        [
            id.into(),
            sequence.into(),
            serde_json::to_string(payload)?.into(),
        ],
    ))
    .await?;
    Ok(())
}
