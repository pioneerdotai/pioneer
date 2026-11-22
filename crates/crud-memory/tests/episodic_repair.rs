use pioneer_crud_memory::thread_episodic::{
    NewThreadEpisodicItemRecord, ThreadEpisodicItemStatus, ThreadEpisodicItemVisibility,
    ThreadEpisodicSourceActorRole, ThreadEpisodicSourceRuntimeKind,
};
use pioneer_protocol::ThreadEpisodicSourceContext;

async fn seed_workspace(db: &sea_orm::DatabaseConnection) -> anyhow::Result<()> {
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES ('ws_thread_episodic','Episodic fixture')",
    )
    .await?;
    Ok(())
}
use pioneer_crud_memory::episodic_repository as thread_episodic_repository;
use sea_orm::{ConnectionTrait, DatabaseBackend, QuerySelect, QueryTrait, Statement};

#[tokio::test]
async fn thread_episodic_lifecycle_repair_plan_scopes_jobs_to_source_occurrence() {
    pioneer_postgres_test::with_database(async |fixture| {
        for query in [
            thread_episodic_repository::excluded_source_versions_needing_repair(
                "ws_thread_episodic_repair_plan",
                "thread_target",
                "turn_target",
                "item_target",
            ),
            thread_episodic_repository::deleted_source_versions_needing_repair(
                "ws_thread_episodic_repair_plan",
                "thread_target",
                "turn_target",
                "item_target",
            ),
        ] {
            let statement = query.limit(32).build(DatabaseBackend::Postgres);
            let plan = fixture
                .connection()
                .query_all_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    format!("EXPLAIN (COSTS OFF) {}", statement.sql),
                    statement
                        .values
                        .expect("repair query should bind occurrence values"),
                ))
                .await
                .expect("repair query plan should resolve")
                .into_iter()
                .map(|row| {
                    row.try_get::<String>("", "QUERY PLAN")
                        .expect("query plan should expose detail")
                })
                .collect::<Vec<_>>();
            assert!(
                plan.iter().any(|detail| {
                    detail.contains("idx_thread_episodic_index_jobs_item")
                        && detail.contains("Index")
                }),
                "repair must probe jobs by the occurrence item ids: {plan:#?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|detail| { detail.contains("Seq Scan on thread_episodic_index_jobs") }),
                "repair must not materialize a global job scan: {plan:#?}"
            );
        }
        Ok(())
    })
    .await
    .unwrap();
}

fn thread_episodic_item_fixture(
    id: &str,
    item_id: &str,
    text_hash: &str,
    status: ThreadEpisodicItemStatus,
    visibility: ThreadEpisodicItemVisibility,
) -> NewThreadEpisodicItemRecord {
    NewThreadEpisodicItemRecord {
        id: Some(id.to_owned()),
        workspace_id: "ws_thread_episodic".to_owned(),
        thread_id: "thread_episodic_1".to_owned(),
        turn_id: "turn_episodic_1".to_owned(),
        item_id: item_id.to_owned(),
        source_actor_role: ThreadEpisodicSourceActorRole::Assistant,
        source_runtime_kind: ThreadEpisodicSourceRuntimeKind::AssistantTurn,
        source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
        visibility,
        status,
        text_hash: text_hash.to_owned(),
        source_text_hash: "source_hash_1".to_owned(),
        projection_group_id: "projection_group_1".to_owned(),
        language_hint: Some("ru".to_owned()),
        token_estimate: 12,
        indexed_at: None,
        deleted_at: None,
    }
}

#[tokio::test]
async fn thread_episodic_item_upsert_is_idempotent_by_source_identity_and_hash() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        seed_workspace(db).await?;
        let now = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("valid fixture timestamp")
            .fixed_offset();
        let first = thread_episodic_item_fixture(
            "item_episodic_1",
            "assistant_message_1",
            "same_text_hash",
            ThreadEpisodicItemStatus::Active,
            ThreadEpisodicItemVisibility::UserVisible,
        );
        let inserted =
            thread_episodic_repository::upsert_item_by_source_identity(db, first.clone(), now)
                .await
                .expect("first upsert should insert");

        let mut duplicate = first;
        duplicate.id = Some("item_episodic_2".to_owned());
        let second = thread_episodic_repository::upsert_item_by_source_identity(
            db,
            duplicate,
            now + chrono::TimeDelta::seconds(100),
        )
        .await
        .expect("duplicate upsert should return existing row");

        assert_eq!(second.id, inserted.id);
        let rows = thread_episodic_repository::list_items_for_thread(
            db,
            "ws_thread_episodic",
            "thread_episodic_1",
            10,
        )
        .await
        .expect("admin list should succeed");
        assert_eq!(rows.len(), 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn thread_episodic_recallable_items_suppress_deleted_excluded_and_internal_rows() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = fixture.connection();
        seed_workspace(db).await?;
        let now = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("valid fixture timestamp")
            .fixed_offset();
        for item in [
            thread_episodic_item_fixture(
                "item_active",
                "assistant_message_active",
                "active_hash",
                ThreadEpisodicItemStatus::Active,
                ThreadEpisodicItemVisibility::UserVisible,
            ),
            thread_episodic_item_fixture(
                "item_excluded",
                "assistant_message_excluded",
                "excluded_hash",
                ThreadEpisodicItemStatus::Excluded,
                ThreadEpisodicItemVisibility::UserVisible,
            ),
            thread_episodic_item_fixture(
                "item_deleted",
                "assistant_message_deleted",
                "deleted_hash",
                ThreadEpisodicItemStatus::Deleted,
                ThreadEpisodicItemVisibility::UserVisible,
            ),
            thread_episodic_item_fixture(
                "item_internal",
                "assistant_message_internal",
                "internal_hash",
                ThreadEpisodicItemStatus::Active,
                ThreadEpisodicItemVisibility::InternalHidden,
            ),
        ] {
            thread_episodic_repository::upsert_item_by_source_identity(db, item, now)
                .await
                .expect("item upsert should succeed");
        }

        let admin_rows = thread_episodic_repository::list_items_for_thread(
            db,
            "ws_thread_episodic",
            "thread_episodic_1",
            10,
        )
        .await
        .expect("admin list should include all rows");
        assert_eq!(admin_rows.len(), 4);

        let recallable = thread_episodic_repository::list_recallable_items_for_thread(
            db,
            "ws_thread_episodic",
            "thread_episodic_1",
            10,
        )
        .await
        .expect("recallable list should succeed");
        assert_eq!(recallable.len(), 1);
        assert_eq!(recallable[0].id, "item_active");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn snapshot_claim_serializes_with_updates_to_the_episodic_job() -> anyhow::Result<()> {
    use anyhow::Context;
    use pioneer_crud_memory::{
        MemoryStore,
        thread_episodic::{NewThreadEpisodicIndexJobRecord, ThreadEpisodicIndexJobStatus},
    };
    use pioneer_postgres_pools::PostgresDatabase;
    use sea_orm::{DbBackend, TransactionTrait};
    use std::time::Duration;

    for commit_edit in [false, true] {
        pioneer_postgres_test::with_database(async |fixture| {
            let db = fixture.connection();
            seed_workspace(db).await?;
            let now_unix = 1_700_000_000;
            let now = chrono::DateTime::from_timestamp(now_unix, 0)
                .unwrap()
                .fixed_offset();
            let item = thread_episodic_repository::upsert_item_by_source_identity(
                db,
                thread_episodic_item_fixture(
                    "episodic-pending-item",
                    "canonical-item",
                    "source-hash",
                    ThreadEpisodicItemStatus::PendingIndex,
                    ThreadEpisodicItemVisibility::UserVisible,
                ),
                now,
            )
            .await?;
            let candidate_writer = fixture.connect_with_limit(1).await?;
            let pid: i32 = candidate_writer
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid()",
                ))
                .await?
                .context("candidate connection PID")?
                .try_get_by_index(0)?;
            let store = MemoryStore::new(PostgresDatabase::new(
                fixture.connect_read_only().await?,
                candidate_writer,
            ));
            let job = store
                .insert_thread_episodic_index_job_if_absent(
                    NewThreadEpisodicIndexJobRecord {
                        id: None,
                        workspace_id: item.workspace_id,
                        thread_id: item.thread_id,
                        index_item_id: item.id,
                        status: ThreadEpisodicIndexJobStatus::Queued,
                        next_run_at: now,
                        last_error: None,
                    },
                    now_unix,
                )
                .await?;

            let edit = db.begin().await?;
            edit.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE thread_episodic_index_jobs SET last_error='refill deferred' WHERE id=$1",
                [job.id.clone().into()],
            ))
            .await?;
            let job_id = job.id.clone();
            let claim = tokio::spawn(async move {
                store
                    .claim_thread_episodic_index_job_from_snapshot(&job, now_unix, 3, None, None)
                    .await
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked: bool = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0",
                            [pid.into()],
                        ))
                        .await?
                        .context("claim lock observation")?
                        .try_get_by_index(0)?;
                    if blocked {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("claim did not wait for an in-progress job edit")??;

            if commit_edit {
                edit.commit().await?;
                assert!(
                    claim.await??.is_none(),
                    "a stale snapshot must not acquire an attempt"
                );
            } else {
                edit.rollback().await?;
                let claimed = claim.await??.context("unchanged job should be claimed")?;
                assert_eq!(claimed.status, ThreadEpisodicIndexJobStatus::Running);
                assert_eq!(claimed.attempt_count, 1);
            }
            let row = thread_episodic_repository::find_index_job_by_id(db, &job_id)
                .await?
                .context("durable job row")?;
            assert_eq!(row.attempt_count, if commit_edit { 0 } else { 1 });
            assert_eq!(
                row.last_error.as_deref(),
                commit_edit.then_some("refill deferred")
            );
            Ok(())
        })
        .await?;
    }
    Ok(())
}
