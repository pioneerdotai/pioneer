use anyhow::{Context, Result};
use chrono::DateTime;
use pioneer_crud_execution::agent_queue::{
    candidates, extend_idle_deadline, load_fair_queued_candidate,
};
use pioneer_entity::agent_execution_resource_state as resource;
use pioneer_postgres_test::with_database;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter};

#[tokio::test]
async fn fair_queue_selects_eligible_branch_heads_before_limiting_and_obeys_capacity() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            r#"
            INSERT INTO workspace (id, name) VALUES ('w', 'Workspace');
            INSERT INTO agent_identity (id, workspace_id, source_kind, source_id, source_fingerprint)
                VALUES ('Identity0000000000000', 'w', 'ephemeral', 'identity', 'fingerprint');
            INSERT INTO agent_execution
                (id, workspace_id, agent_identity_id, identity_source_revision, identity_source_fingerprint,
                 home_root_thread_id, work_graph_root_execution_id, requested_identity_selection_json,
                 requested_profile_selection_json, authorization_context_fingerprint)
                SELECT 'Execution' || lpad(n::text, 12, '0'), 'w', 'Identity0000000000000',
                       1, 'fingerprint', 'thread',
                       CASE WHEN n <= 5 THEN 'Execution000000000001' ELSE 'Execution000000000006' END,
                       '{}', '{}', 'authorization'
                FROM generate_series(1, 7) AS n;
            INSERT INTO agent_work_resource_scope (root_execution_id, aggregate_usage_json, last_scheduled_sequence)
                VALUES ('Execution000000000001', '{}', 2), ('Execution000000000006', '{}', 1);
            INSERT INTO agent_work_branch_schedule (root_execution_id, branch_key, last_scheduled_sequence)
                VALUES ('Execution000000000001', 'a', 3), ('Execution000000000001', 'b', 0),
                       ('Execution000000000006', 'a', 0);
            INSERT INTO agent_work_queue
                (id, root_execution_id, execution_id, branch_key, enqueue_sequence, eligible_at, created_at)
                VALUES ('a', 'Execution000000000001', 'Execution000000000002', 'a', 1, NULL, '2026-11-19T10:00:00Z'),
                       ('a-next', 'Execution000000000001', 'Execution000000000003', 'a', 2, NULL, '2026-11-19T10:00:00Z'),
                       ('b-future', 'Execution000000000001', 'Execution000000000004', 'b', 1, '2026-11-19T10:01:00Z', '2026-11-19T10:00:00Z'),
                       ('b', 'Execution000000000001', 'Execution000000000005', 'b', 2, NULL, '2026-11-19T10:00:00Z'),
                       ('other', 'Execution000000000006', 'Execution000000000007', 'a', 1, NULL, '2026-11-19T10:00:00Z');
            INSERT INTO agent_execution_resource_state
                (id, execution_id, progress_frontier_json, local_usage_json, branch_key, fair_order, idle_deadline, hard_deadline)
                VALUES ('lease', 'Execution000000000001', '{}', '{}', 'a', 0,
                        '2026-11-19T10:00:05Z', '2026-11-19T10:00:10Z');
            "#,
        )
        .await?;
        let now = DateTime::parse_from_rfc3339("2026-11-19T10:00:00Z")?;
        let root = "Execution000000000001";
        assert_eq!(candidates(db, now, 1).await?, ["other"]);
        assert_eq!(candidates(db, now, 8).await?, ["other", "b"]);
        assert_eq!(
            load_fair_queued_candidate(db, root, now).await?.context("no eligible queue item")?.id,
            "b"
        );
        db.execute_unprepared(
            "UPDATE agent_work_branch_schedule SET last_scheduled_sequence = 4 WHERE branch_key = 'b'",
        )
        .await?;
        assert_eq!(
            load_fair_queued_candidate(db, root, now).await?.context("no eligible queue item")?.id,
            "a"
        );
        db.execute_unprepared(
            "INSERT INTO agent_running_permit (id, root_execution_id, execution_id)
             VALUES ('held', 'Execution000000000006', 'Execution000000000007')",
        )
        .await?;
        assert_eq!(candidates(db, now, 8).await?, ["a"]);

        let now_plus = |seconds| now + chrono::Duration::seconds(seconds);
        for (proposed, expected) in [(3, 5), (8, 8), (15, 10)] {
            resource::Entity::update_many()
                .filter(resource::Column::Id.eq("lease"))
                .col_expr(resource::Column::IdleDeadline, extend_idle_deadline(now_plus(proposed)))
                .exec(db)
                .await?;
            let current = resource::Entity::find_by_id("lease").one(db).await?.context("no lease")?;
            assert_eq!(current.idle_deadline, Some(now_plus(expected)));
        }
        Ok(())
    })
    .await
}
