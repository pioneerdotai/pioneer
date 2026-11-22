//! Public Task completion inbox persistence under the migrated PostgreSQL schema.

use pioneer_crud_execution::canonical_id::canonical_agent_id;
use pioneer_crud_tasks::completion_inbox::*;
use pioneer_postgres_test::scoped_database;
use sea_orm::{ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, Statement};

#[path = "../src/test_support/completion_inbox.rs"]
mod test_fixture;
use test_fixture::{run, task};

const A: &str = "P0000000000000000000A";
const B: &str = "P0000000000000000000B";

async fn fixture() -> std::sync::Arc<pioneer_postgres_test::TestDatabase> {
    let fixture = scoped_database().await.unwrap();
    test_fixture::seed(fixture.connection()).await;
    fixture
}

fn access(principal: &str) -> TaskCompletionInboxAccess<'_> {
    TaskCompletionInboxAccess {
        workspace_id: "W0000000000000000000I",
        principal_id: principal,
        absolute: false,
    }
}
async fn unread(db: &impl ConnectionTrait, principal: &str) -> u64 {
    count_unread_task_completion_inbox(db, &access(principal))
        .await
        .unwrap()
}
async fn saved_result_item(db: &impl ConnectionTrait, turn: &str, item: &str) {
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,active_attempt_number,created_at,updated_at) VALUES($1,$2,$3,'agent_message','completed','{}',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [format!("saved-{turn}-{item}").into(), turn.into(), item.into()])).await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn visible_results_resolve_exact_occurrence_and_delivery_with_personal_cross_device_receipts()
{
    use pioneer_protocol::TaskCompletionInboxViewedParams;
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "visible",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "thread",
        "detached",
    )
    .await;
    for (id, number) in [
        ("first-visible", 1),
        ("second-visible", 2),
        ("third-visible", 3),
    ] {
        run(db, id, "visible", "succeeded", number).await;
    }
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES('child-answer','T0000000000000000000I','completed','{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('delivered-answer','T0000000000000000000P','completed','{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES('binding','visible','first-visible','T0000000000000000000I','child-answer','initial',1,1,'candidate_created',CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('visible-delivery','W0000000000000000000I','visible','second-visible','visible-key','thread','T0000000000000000000P','delivered',1,1,'delivered-answer',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    saved_result_item(
        db,
        "delivered-answer",
        &pioneer_protocol::task_delivery_result_item_id("visible-delivery"),
    )
    .await;
    // Two child deliveries share the same occurrence Turn. Only the viewed
    // canonical result item may acknowledge its own run.
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('neighbour-delivery','W0000000000000000000I','visible','third-visible','neighbour-key','thread','T0000000000000000000P','delivered',1,1,'delivered-answer',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let mut params = TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.into(),
        thread_id: "T0000000000000000000I".into(),
        run_ids: vec![],
        result_turn_ids: vec!["child-answer".into()],
        result_items: vec![],
    };
    let at = "2026-10-16 10:00:00 +00:00";
    assert_eq!(
        mark_viewed_task_completion_inbox_read(db, &access(A), &params, at)
            .await
            .unwrap(),
        ["turn:child-answer"]
    );
    assert_eq!(unread(db, A).await, 2);
    assert_eq!(unread(db, B).await, 3);
    // Same receipt is visible through a second independent reader and idempotent.
    mark_viewed_task_completion_inbox_read(db, &access(A), &params, at)
        .await
        .unwrap();
    params.thread_id = "T0000000000000000000P".into();
    params.result_turn_ids = vec!["child-answer".into()];
    assert!(
        mark_viewed_task_completion_inbox_read(db, &access(A), &params, at)
            .await
            .unwrap()
            .is_empty()
    );
    params.result_turn_ids = vec!["delivered-answer".into()];
    assert!(
        mark_viewed_task_completion_inbox_read(db, &access(A), &params, at)
            .await
            .unwrap()
            .is_empty(),
        "a whole parent Turn cannot read its child deliveries"
    );
    params.result_turn_ids.clear();
    params.result_items = vec![pioneer_protocol::TaskCompletionInboxViewedResult {
        turn_id: "delivered-answer".into(),
        item_id: "unrelated-parent-answer".into(),
    }];
    assert!(
        mark_viewed_task_completion_inbox_read(db, &access(A), &params, at)
            .await
            .unwrap()
            .is_empty()
    );
    params.result_items = vec![pioneer_protocol::TaskCompletionInboxViewedResult {
        turn_id: "delivered-answer".into(),
        item_id: pioneer_protocol::task_delivery_result_item_id("visible-delivery"),
    }];
    mark_viewed_task_completion_inbox_read(db, &access(A), &params, at)
        .await
        .unwrap();
    assert_eq!(
        unread(db, A).await,
        1,
        "another repeat of the same task stays unread"
    );
    assert_eq!(unread(db, B).await, 3);
    let receipts = pioneer_entity::task_run_read::Entity::find()
        .all(db)
        .await
        .unwrap();
    assert_eq!(receipts.len(), 2);
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.run_id != "third-visible")
    );
}

#[pioneer_postgres_test::postgres_test]
async fn failed_dedicated_delivery_status_uses_its_exact_canonical_turn_link() {
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "failure-result",
        Some("T0000000000000000000P"),
        None,
        "system",
        "thread",
        "detached",
    )
    .await;
    run(db, "failure-result-run", "failure-result", "failed", 1).await;
    let turn_id = canonical_agent_id('T', "task-delivery-turn\0failure-result-delivery");
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES($1,'T0000000000000000000P','failed','{}','conversation','task_delivery',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [turn_id.clone().into()])).await.unwrap();
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('failure-result-delivery','W0000000000000000000I','failure-result','failure-result-run','failure-result-key','thread','T0000000000000000000P','delivered',1,1,$1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [turn_id.clone().into()])).await.unwrap();
    let params = pioneer_protocol::TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.into(),
        thread_id: "T0000000000000000000P".into(),
        run_ids: vec![],
        result_turn_ids: vec![turn_id],
        result_items: vec![],
    };
    assert_eq!(
        mark_viewed_task_completion_inbox_read(
            db,
            &access(A),
            &params,
            "2026-10-16 10:00:00 +00:00"
        )
        .await
        .unwrap()
        .len(),
        1
    );
    assert_eq!(unread(db, A).await, 0);
    assert_eq!(unread(db, B).await, 1);
}

#[pioneer_postgres_test::postgres_test]
async fn child_delivery_item_does_not_read_the_parent_execution_run_sharing_its_turn() {
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "parent-visible-task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    task(
        db,
        "child-visible-task",
        Some("T0000000000000000000I"),
        Some("parent-visible-task"),
        "tool",
        "thread",
        "attached",
    )
    .await;
    run(
        db,
        "parent-visible-run",
        "parent-visible-task",
        "succeeded",
        1,
    )
    .await;
    run(
        db,
        "child-visible-run",
        "child-visible-task",
        "succeeded",
        1,
    )
    .await;
    db.execute_unprepared("INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES('shared-execution-turn','T0000000000000000000I','completed','{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES('parent-visible-binding','parent-visible-task','parent-visible-run','T0000000000000000000I','shared-execution-turn','initial',1,1,'candidate_created',CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('exact-child-delivery','W0000000000000000000I','child-visible-task','child-visible-run','exact-child-key','thread','T0000000000000000000I','delivered',1,1,'shared-execution-turn',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    // Both children have finished; their parent is still working. Looking at
    // one result cannot acknowledge its neighbour or the parent's own run.
    db.execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='shared-execution-turn'")
        .await
        .unwrap();
    run(db, "second-child-run", "child-visible-task", "succeeded", 2).await;
    db.execute_unprepared("INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id,created_at,updated_at) VALUES('second-child-delivery','W0000000000000000000I','child-visible-task','second-child-run','second-child-key','thread','T0000000000000000000I','delivered',1,1,'shared-execution-turn',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    saved_result_item(
        db,
        "shared-execution-turn",
        &pioneer_protocol::task_delivery_result_item_id("exact-child-delivery"),
    )
    .await;
    saved_result_item(
        db,
        "shared-execution-turn",
        &pioneer_protocol::task_delivery_result_item_id("second-child-delivery"),
    )
    .await;
    let params = pioneer_protocol::TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.into(),
        thread_id: "T0000000000000000000I".into(),
        run_ids: vec![],
        result_turn_ids: vec![],
        result_items: vec![pioneer_protocol::TaskCompletionInboxViewedResult {
            turn_id: "shared-execution-turn".into(),
            item_id: pioneer_protocol::task_delivery_result_item_id("exact-child-delivery"),
        }],
    };
    mark_viewed_task_completion_inbox_read(db, &access(A), &params, "2026-10-16 10:00:00 +00:00")
        .await
        .unwrap();
    assert_eq!(unread(db, A).await, 2);
    assert_eq!(unread(db, B).await, 3);
    assert!(
        find_task_completion_inbox_entry(db, &access(A), "parent-visible-run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_none()
    );
    assert!(
        find_task_completion_inbox_entry(db, &access(A), "child-visible-run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_some()
    );
    // Leaving the viewport performs no undo. Finishing the containing parent
    // later cannot revoke the already persisted child's personal receipt.
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='shared-execution-turn'")
        .await
        .unwrap();
    assert_eq!(unread(db, A).await, 2);
    assert!(
        find_task_completion_inbox_entry(db, &access(A), "second-child-run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_none()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn reviewed_run_requires_accepted_candidate_not_old_answer_or_reviewer() {
    use pioneer_crud_tasks::completion_inbox::mark_viewed_task_completion_inbox_read as viewed;
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "reviewed",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "detached",
    )
    .await;
    run(db, "reviewed-run", "reviewed", "waiting_review", 1).await;
    for (turn, binding, kind, status, sequence) in [
        (
            "old-answer",
            "old-binding",
            "initial",
            "candidate_created",
            1,
        ),
        (
            "accepted-answer",
            "accepted-binding",
            "revision",
            "candidate_created",
            2,
        ),
        (
            "reviewer-answer",
            "reviewer-binding",
            "review",
            "review_recorded",
            3,
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES($1,'T0000000000000000000I','completed','{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",[turn.into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES($1,'reviewed','reviewed-run','T0000000000000000000I',$2,$3,$4, $5,$6,CURRENT_TIMESTAMP)",
            [binding.into(),turn.into(),kind.into(),sequence.into(),sequence.into(),status.into()])).await.unwrap();
        saved_result_item(db, turn, "final-answer").await;
    }
    for (candidate, binding, turn, round, status) in [
        (
            "old-candidate",
            "old-binding",
            "old-answer",
            1i64,
            "superseded",
        ),
        (
            "accepted-candidate",
            "accepted-binding",
            "accepted-answer",
            2,
            "accepted",
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES($1,'reviewed','reviewed-run',$2,'T0000000000000000000I',$3,$4,$5,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            [candidate.into(),binding.into(),turn.into(),round.into(),status.into()])).await.unwrap();
    }
    let proof = |turn: &str, status_only: bool| pioneer_protocol::TaskCompletionInboxViewedParams {
        workspace_id: access(A).workspace_id.into(),
        thread_id: "T0000000000000000000I".into(),
        run_ids: vec![],
        result_turn_ids: if status_only {
            vec![turn.into()]
        } else {
            vec![]
        },
        result_items: if status_only {
            vec![]
        } else {
            vec![pioneer_protocol::TaskCompletionInboxViewedResult {
                turn_id: turn.into(),
                item_id: "final-answer".into(),
            }]
        },
    };
    let at = "2026-10-16 10:00:00 +00:00";
    assert!(
        viewed(db, &access(A), &proof("old-answer", false), at)
            .await
            .unwrap()
            .is_empty()
    );
    db.execute_unprepared("UPDATE task_run SET status='succeeded' WHERE id='reviewed-run'")
        .await
        .unwrap();
    for candidate_state in ["rejected", "superseded"] {
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE task_result_candidate SET status=$1 WHERE id='old-candidate'",
            [candidate_state.into()],
        ))
        .await
        .unwrap();
        // Re-evaluation after completion invalidation, with the old row still
        // visible, must not acknowledge the subsequently accepted result.
        for old in ["old-answer", "reviewer-answer"] {
            for status_only in [false, true] {
                assert!(
                    viewed(db, &access(A), &proof(old, status_only), at)
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
        }
        assert_eq!(unread(db, A).await, 1);
    }
    assert!(
        !viewed(db, &access(A), &proof("accepted-answer", false), at)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(unread(db, A).await, 0);
    assert_eq!(unread(db, B).await, 1);
}

#[pioneer_postgres_test::postgres_test]
async fn terminal_errors_without_candidates_require_the_matching_work_frontier() {
    use pioneer_crud_tasks::completion_inbox::mark_viewed_task_completion_inbox_read as viewed;
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "errors",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "detached",
    )
    .await;
    for (ix, (run_status, turn_status, work_status)) in [
        ("failed", "failed", "failed"),
        ("blocked", "blocked", "blocked"),
        ("cancelled", "interrupted", "cancelled"),
        ("timed_out", "failed", "failed"),
        ("failed", "interrupted", "interrupted"),
    ]
    .into_iter()
    .enumerate()
    {
        let run_id = format!("error-run-{ix}");
        let turn_id = format!("error-turn-{ix}");
        run(db, &run_id, "errors", run_status, ix as i64 + 1).await;
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO turn(id,thread_id,status,prompt_manifest_json,turn_kind,origin,created_at,updated_at) VALUES($1,'T0000000000000000000I',$2,'{}','task_run','attached_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [turn_id.clone().into(), turn_status.into()])).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES($1,'errors',$2,'T0000000000000000000I',$3,'recovery',1,1,$4,CURRENT_TIMESTAMP)",
            [format!("error-binding-{ix}").into(),run_id.into(),turn_id.clone().into(),work_status.into()])).await.unwrap();
        let params = pioneer_protocol::TaskCompletionInboxViewedParams {
            workspace_id: access(A).workspace_id.into(),
            thread_id: "T0000000000000000000I".into(),
            run_ids: vec![],
            result_turn_ids: vec![turn_id],
            result_items: vec![],
        };
        assert!(
            !viewed(db, &access(A), &params, "2026-10-16 10:00:00 +00:00")
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(unread(db, A).await, 0);
    assert_eq!(unread(db, B).await, 5);
}

#[pioneer_postgres_test::postgres_test]
async fn legacy_grant_uses_epoch_and_retains_the_ambiguous_second_without_receipts() {
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "legacy-access",
        Some("T0000000000000000000P"),
        None,
        "webhook",
        "none",
        "detached",
    )
    .await;
    // Preserve the membership timestamp invariant while exercising the legacy
    // timezone/subsecond representation of the grant itself.
    db.execute_unprepared("UPDATE workspace_membership SET created_at='2026-10-16 13:00:00.999999 +03:00',updated_at='2026-10-16 13:00:00.999999 +03:00' WHERE principal_id='P0000000000000000000A'").await.unwrap();
    for (i, id, at) in [
        (1, "legacy-pre", "2026-10-16T09:59:59Z"),
        (2, "legacy-tie", "2026-10-16 10:00:00 +00:00"),
        (3, "legacy-post", "2026-10-16T12:00:01+02:00"),
    ] {
        run(db, id, "legacy-access", "succeeded", i).await;
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE task_run SET completed_at=$1::text::timestamptz WHERE id=$2",
            [at.into(), id.into()],
        ))
        .await
        .unwrap();
    }
    assert_eq!(
        unread(db, A).await,
        2,
        "legacy completion seconds cannot prove subsecond order; do not discard legitimate tied completions"
    );
    assert!(
        pioneer_entity::task_run_read::Entity::find()
            .all(db)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(unread(db, B).await, 3);
}

#[pioneer_postgres_test::postgres_test]
async fn addressed_patch_uses_unread_membership_and_exact_completion_order() {
    let fixture = fixture().await;
    let connection = fixture.connection();
    let db = pioneer_postgres_pools::PostgresDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        connection.clone(),
    )
    .maintenance();
    assert!(db.reader_is_read_only().await.unwrap());
    task(
        &db,
        "patch-task",
        Some("T0000000000000000000P"),
        None,
        "system",
        "none",
        "detached",
    )
    .await;
    for (number, id) in ["earlier", "later", "read", "historical"]
        .into_iter()
        .enumerate()
    {
        run(&db, id, "patch-task", "succeeded", number as i64 + 1).await;
    }
    db.execute_unprepared(
        "UPDATE task_run SET completed_at='2026-10-16 10:00:00.100 +00:00' WHERE id='earlier'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE task_run SET completed_at='2026-10-16 10:00:00.200 +00:00' WHERE id='later'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE task_run SET completed_at='2020-01-01 00:00:00 +00:00' WHERE id='historical'",
    )
    .await
    .unwrap();
    mark_task_completion_inbox_read(&db, &access(A), "read", "2026-10-16 11:00:00 +00:00")
        .await
        .unwrap();
    let ids = ["earlier", "later", "read", "historical", "missing"].map(str::to_owned);
    let (entries, total) = addressed_patch(&db, &access(A), &ids).await.unwrap();
    assert_eq!(total, 2);
    assert_eq!(
        entries
            .iter()
            .map(|r| r.run_id.as_str())
            .collect::<Vec<_>>(),
        vec!["later", "earlier"]
    );
    assert_ne!(entries[0].completed_key, entries[1].completed_key);
    assert_eq!(entries[0].completed_at, entries[1].completed_at);
    // Broad history is deliberately retained, but must never become an upsert.
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "historical")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        find_task_completion_inbox_entry(&db, &access(A), "read")
            .await
            .unwrap()
            .is_some()
    );
    db.execute_unprepared(
        "DELETE FROM workspace_membership WHERE principal_id='P0000000000000000000A'",
    )
    .await
    .unwrap();
    let (entries, total) = addressed_patch(&db, &access(A), &ids).await.unwrap();
    assert!(entries.is_empty());
    assert_eq!(total, 0);
}

#[pioneer_postgres_test::postgres_test]
async fn read_all_covers_unloaded_pages_and_leaves_later_commits_at_the_same_time_unread() {
    use sea_orm::TransactionTrait;
    let fixture = fixture().await;
    let db = fixture.connection();
    let writer = fixture.connect().await.unwrap();
    task(
        db,
        "bulk",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    task(
        db,
        "hidden-bulk",
        Some("T0000000000000000000V"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    for ix in 0..65 {
        run(db, &format!("bulk-{ix:03}"), "bulk", "succeeded", ix + 1).await;
    }
    run(db, "hidden-bulk-run", "hidden-bulk", "failed", 1).await;
    db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000A' AND thread_id='T0000000000000000000V'").await.unwrap();
    assert_eq!(
        list_task_completion_inbox(db, &access(A), None, 30)
            .await
            .unwrap()
            .len(),
        30
    );
    mark_all_task_completion_inbox_read(db, &access(A), "2026-10-16 10:00:00 +00:00")
        .await
        .unwrap();
    assert_eq!(unread(db, A).await, 0);
    assert_eq!(unread(db, B).await, 66);
    assert!(
        list_task_completion_inbox(db, &access(A), None, 30)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        pioneer_entity::task_run_read::Entity::find()
            .all(db)
            .await
            .unwrap()
            .len(),
        65
    );
    // Writer order, not a lossy time watermark or lexicographic run ID, defines inclusion.
    run(db, "aaa-later", "bulk", "succeeded", 66).await;
    assert_eq!(unread(db, A).await, 1);
    assert_eq!(
        list_task_completion_inbox(db, &access(A), None, 30)
            .await
            .unwrap()[0]
            .run_id,
        "aaa-later"
    );
    let foreign = TaskCompletionInboxAccess {
        workspace_id: "W0000000000000000000O",
        ..access(A)
    };
    mark_all_task_completion_inbox_read(db, &foreign, "2026-10-16 10:00:00 +00:00")
        .await
        .unwrap();
    assert_eq!(unread(db, A).await, 1);
    let principal_access = access(A);
    for completion_first in [true, false] {
        let racing = if completion_first {
            "committed-first"
        } else {
            "visible-after"
        };
        let tx = writer.begin().await.unwrap();
        run(
            &tx,
            racing,
            "bulk",
            "succeeded",
            if completion_first { 67 } else { 68 },
        )
        .await;
        if completion_first {
            tx.commit().await.unwrap();
            mark_all_task_completion_inbox_read(
                db,
                &principal_access,
                "2026-10-16 10:00:00 +00:00",
            )
            .await
            .unwrap();
        } else {
            // The independent transaction has staged its insert. Read-all
            // must finish against committed data while that insert is pending.
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                mark_all_task_completion_inbox_read(
                    db,
                    &principal_access,
                    "2026-10-16 10:00:00 +00:00",
                ),
            )
            .await
            .expect("read-all must not wait for the uncommitted completion")
            .unwrap();
            tx.commit().await.unwrap();
        }
        let included =
            pioneer_entity::task_run_read::Entity::find_by_id((A.to_owned(), racing.to_owned()))
                .one(db)
                .await
                .unwrap()
                .is_some();
        assert_eq!(included, completion_first);
        assert_eq!(unread(db, A).await, u64::from(!completion_first));
    }
    run(db, "strictly-after-race", "bulk", "succeeded", 69).await;
    assert_eq!(unread(db, A).await, 2);
    writer.close().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn read_entity_preserves_personal_keys_and_cascades_deleted_run() {
    use pioneer_entity::{task_run, task_run_read};
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    run(db, "first", "task", "succeeded", 1).await;
    run(db, "second", "task", "succeeded", 2).await;
    mark_personal_read(db, A, "first").await;
    mark_personal_read(db, B, "first").await;
    mark_personal_read(db, A, "second").await;
    let receipt = task_run_read::Entity::find_by_id((A.to_owned(), "first".to_owned()))
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.read_at, "2026-10-16 11:00:00 +00:00");
    assert_eq!(receipt.principal_id, A);
    assert_eq!(receipt.run_id, "first");
    mark_task_completion_inbox_read(db, &access(A), "first", "2026-10-17 11:00:00 +00:00")
        .await
        .unwrap();
    assert_eq!(
        task_run_read::Entity::find_by_id((A.to_owned(), "first".to_owned()))
            .one(db)
            .await
            .unwrap()
            .unwrap()
            .read_at,
        receipt.read_at,
        "a repeated read keeps the original personal receipt"
    );
    assert_eq!(
        task_run_read::Entity::find()
            .filter(task_run_read::Column::RunId.eq("first"))
            .all(db)
            .await
            .unwrap()
            .len(),
        2,
        "the composite key keeps one receipt per principal and run"
    );
    // PostgreSQL cascades both personal receipts when the exact run is deleted.
    task_run::Entity::delete_by_id("first")
        .exec(db)
        .await
        .unwrap();
    assert_eq!(
        task_run_read::Entity::find()
            .filter(task_run_read::Column::RunId.eq("first"))
            .all(db)
            .await
            .unwrap()
            .len(),
        0,
        "declarative ON DELETE CASCADE removes receipts for the deleted run"
    );
    assert!(
        task_run_read::Entity::find_by_id((A.to_owned(), "second".to_owned()))
            .one(db)
            .await
            .unwrap()
            .is_some()
    );
    for (principal, expected_unread) in [(A, 0), (B, 1)] {
        let page = list_task_completion_inbox(db, &access(principal), None, 30)
            .await
            .unwrap();
        assert_eq!(page.len() as u64, expected_unread);
        if expected_unread > 0 {
            assert_eq!(page[0].run_id, "second");
        }
        assert_eq!(unread(db, principal).await, expected_unread);
        assert!(
            find_task_completion_inbox_entry(db, &access(principal), "first")
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn read_requires_an_existing_authorized_terminal_run() {
    use pioneer_entity::task_run_read;
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "private",
        Some("T0000000000000000000V"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    run(db, "private-run", "private", "succeeded", 1).await;
    run(db, "running-run", "private", "running", 2).await;
    run(db, "orphan-run", "private", "succeeded", 3).await;
    db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000B' AND thread_id='T0000000000000000000V'").await.unwrap();
    db.execute_unprepared("UPDATE task_run SET task_id='missing-task' WHERE id='orphan-run'")
        .await
        .unwrap();
    for run_id in ["missing-run", "private-run", "running-run", "orphan-run"] {
        mark_personal_read(db, B, run_id).await;
    }
    // Absolute TaskRead still cannot substitute a different workspace or skip
    // run/task existence and terminal checks in the atomic INSERT SELECT.
    let wrong_workspace = TaskCompletionInboxAccess {
        workspace_id: "W0000000000000000000O",
        principal_id: A,
        absolute: true,
    };
    mark_task_completion_inbox_read(
        db,
        &wrong_workspace,
        "private-run",
        "2026-10-17 11:00:00 +00:00",
    )
    .await
    .unwrap();
    let absolute = TaskCompletionInboxAccess {
        absolute: true,
        ..access(A)
    };
    for run_id in ["missing-run", "running-run", "orphan-run"] {
        mark_task_completion_inbox_read(db, &absolute, run_id, "2026-10-17 11:00:00 +00:00")
            .await
            .unwrap();
    }
    assert!(
        task_run_read::Entity::find()
            .all(db)
            .await
            .unwrap()
            .is_empty()
    );
    mark_personal_read(db, A, "private-run").await;
    let receipts = task_run_read::Entity::find().all(db).await.unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].principal_id, A);
    assert_eq!(receipts[0].run_id, "private-run");
}

async fn mark_personal_read(db: &impl ConnectionTrait, principal: &str, run_id: &str) {
    mark_task_completion_inbox_read(db, &access(principal), run_id, "2026-10-16 11:00:00 +00:00")
        .await
        .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn terminal_statuses_policies_executors_attachment_and_recurring_runs_share_one_source() {
    let fixture = fixture().await;
    let db = fixture.connection();
    let mut expected = 0;
    for (ix, status) in [
        "succeeded",
        "failed",
        "blocked",
        "cancelled",
        "timed_out",
        "queued",
        "starting",
        "running",
        "waiting",
        "waiting_review",
    ]
    .iter()
    .enumerate()
    {
        for (jx, mode) in ["none", "thread", "user_notification", "webhook"]
            .iter()
            .enumerate()
        {
            let id = format!("task-{ix}-{jx}");
            task(
                db,
                &id,
                Some("T0000000000000000000P"),
                None,
                ["agent", "tool", "workflow", "webhook", "system"][ix % 5],
                mode,
                if jx % 2 == 0 { "attached" } else { "detached" },
            )
            .await;
            run(db, &format!("run-{ix}-{jx}"), &id, status, 1).await;
            if ix < 5 {
                expected += 1;
            }
        }
    }
    // A service System task has no accessible collaboration root. A System
    // task attached to a public root was included by the matrix above.
    task(db, "service", None, None, "system", "none", "detached").await;
    run(db, "service-run", "service", "succeeded", 1).await;
    run(db, "second-occurrence", "task-0-0", "succeeded", 2).await;
    expected += 1;
    assert_eq!(unread(db, A).await, expected);
    let page = list_task_completion_inbox(db, &access(A), None, 100)
        .await
        .unwrap();
    assert_eq!(page.len() as u64, expected);
    assert!(page.iter().all(|e| e.thread_id == "T0000000000000000000P"
        && e.summary.as_deref() == Some("summary")
        && e.completed_at == 1792144800));
    assert_eq!(page.iter().filter(|e| e.task_id == "task-0-0").count(), 2);
    let absolute = TaskCompletionInboxAccess {
        absolute: true,
        ..access(A)
    };
    assert_eq!(
        count_unread_task_completion_inbox(db, &absolute)
            .await
            .unwrap(),
        expected,
        "absolute access still excludes service tasks without a user thread"
    );
    // Replaying deliveries and re-reading events cannot insert inbox rows.
    let again = list_task_completion_inbox(db, &access(A), None, 100)
        .await
        .unwrap();
    assert_eq!(
        page.iter().map(|e| &e.run_id).collect::<Vec<_>>(),
        again.iter().map(|e| &e.run_id).collect::<Vec<_>>()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn personal_reads_survive_reopen_and_acl_revocation_removes_list_count_get_and_write() {
    use pioneer_crud_access::authorization_scope::resolve_task_authorization_scope;
    let fixture = fixture().await;
    let writer = fixture.connect().await.unwrap();
    let db = &writer;
    task(
        db,
        "root",
        Some("T0000000000000000000V"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    task(
        db,
        "child",
        Some("T0000000000000000000P"),
        Some("root"),
        "system",
        "webhook",
        "detached",
    )
    .await;
    db.execute_unprepared(
        "UPDATE task SET owner_kind='user',owner_id='P0000000000000000000A' WHERE id='root'",
    )
    .await
    .unwrap();
    run(db, "run", "child", "blocked", 1).await;
    assert_eq!(unread(db, A).await, 1);
    assert_eq!(unread(db, B).await, 1);
    let scope = resolve_task_authorization_scope(db, "child", Some("W0000000000000000000I"), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        scope.root_thread_id.as_deref(),
        Some("T0000000000000000000V")
    );
    mark_personal_read(db, A, "run").await;
    mark_personal_read(db, A, "run").await;
    assert_eq!(unread(db, A).await, 0);
    assert_eq!(unread(db, B).await, 1);
    assert!(
        find_task_completion_inbox_entry(db, &access(A), "run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_some()
    );
    writer.close().await.unwrap();
    let writer = fixture.connect().await.unwrap();
    let db = &writer;
    assert_eq!(unread(db, A).await, 0);
    // Another device reloads the same durable receipt, not a local copy.
    assert!(
        find_task_completion_inbox_entry(db, &access(A), "run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_some()
    );
    db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000B' AND thread_id='T0000000000000000000V'").await.unwrap();
    assert_eq!(unread(db, B).await, 0);
    assert!(
        list_task_completion_inbox(db, &access(B), None, 30)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        find_task_completion_inbox_entry(db, &access(B), "run")
            .await
            .unwrap()
            .is_none()
    );
    mark_personal_read(db, B, "run").await;
    let receipts = db
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT COUNT(*) n FROM task_run_read WHERE principal_id='P0000000000000000000B'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipts.try_get::<i64>("", "n").unwrap(), 0);
    db.execute_unprepared(
        "DELETE FROM workspace_membership WHERE principal_id='P0000000000000000000A'",
    )
    .await
    .unwrap();
    assert!(
        find_task_completion_inbox_entry(db, &access(A), "run")
            .await
            .unwrap()
            .is_none()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn time_and_id_seek_pages_are_stable_and_total_unread_is_not_page_size() {
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "recurring",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    for ix in 0..65 {
        run(
            db,
            &format!("run-{ix:03}"),
            "recurring",
            "succeeded",
            ix + 1,
        )
        .await;
    }
    assert_eq!(unread(db, A).await, 65);
    let first = list_task_completion_inbox(db, &access(A), None, 30)
        .await
        .unwrap();
    assert_eq!(first.len(), 30);
    assert_eq!(first[0].run_id, "run-064");
    let anchor = first.last().unwrap();
    mark_personal_read(db, A, &anchor.run_id).await;
    // A new occurrence with the same completion time cannot shift the seek.
    run(db, "run-999", "recurring", "succeeded", 66).await;
    let second = list_task_completion_inbox(
        db,
        &access(A),
        Some((&anchor.completed_key, &anchor.run_id)),
        30,
    )
    .await
    .unwrap();
    assert_eq!(second[0].run_id, "run-034");
    assert!(
        second
            .iter()
            .all(|e| !first.iter().any(|f| f.run_id == e.run_id))
    );
    let anchor = second.last().unwrap();
    let third = list_task_completion_inbox(
        db,
        &access(A),
        Some((&anchor.completed_key, &anchor.run_id)),
        30,
    )
    .await
    .unwrap();
    assert_eq!(third.len(), 5);
    assert_eq!(unread(db, A).await, 65);
    let foreign = TaskCompletionInboxAccess {
        workspace_id: "W0000000000000000000O",
        principal_id: A,
        absolute: true,
    };
    assert_eq!(
        count_unread_task_completion_inbox(db, &foreign)
            .await
            .unwrap(),
        0
    );
}

#[pioneer_postgres_test::postgres_test]
async fn scheduled_and_recurring_trigger_history_does_not_depend_on_current_task_state() {
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "recurring",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "detached",
    )
    .await;
    for (ix, kind) in [
        "immediate",
        "scheduled_at",
        "interval",
        "cron",
        "manual",
        "external",
        "dependency",
    ]
    .iter()
    .enumerate()
    {
        let trigger = format!("trigger-{ix}");
        let run_id = format!("run-{ix}");
        // The list deliberately does not parse trigger configuration or join
        // an execution/timeline subscription to discover saved completions.
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"INSERT INTO task_trigger(id,task_id,kind,status,spec_json,created_at,updated_at) VALUES($1,'recurring',$2,'paused','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",[trigger.clone().into(),(*kind).into()])).await.unwrap();
        run(db, &run_id, "recurring", "succeeded", ix as i64 + 1).await;
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE task_run SET trigger_id=$1 WHERE id=$2",
            [trigger.into(), run_id.into()],
        ))
        .await
        .unwrap();
    }
    db.execute_unprepared(
        "UPDATE task SET status='scheduled',completed_at=NULL WHERE id='recurring'",
    )
    .await
    .unwrap();
    assert_eq!(unread(db, A).await, 7);
    assert_eq!(
        list_task_completion_inbox(db, &access(A), None, 30)
            .await
            .unwrap()
            .len(),
        7
    );
}

#[pioneer_postgres_test::postgres_test]
async fn same_run_updates_preserve_personal_read_and_list_projection_is_bounded() {
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "attached",
    )
    .await;
    run(db, "run", "task", "blocked", 1).await;
    mark_personal_read(db, A, "run").await;
    let result=serde_json::json!({"summary":"界".repeat(2048),"data":{"html":"x".repeat(65536)},"artifacts":[]}).to_string();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE task_run SET status='succeeded',result_json=$1,error_json=NULL WHERE id='run'",
        [result.into()],
    ))
    .await
    .unwrap();
    let rows = list_task_completion_inbox(db, &access(A), None, 30)
        .await
        .unwrap();
    assert!(rows.is_empty());
    let row = find_task_completion_inbox_entry(db, &access(A), "run")
        .await
        .unwrap()
        .unwrap();
    assert!(row.read_at.is_some());
    assert_eq!(row.summary.as_ref().unwrap().chars().count(), 1024);
    assert_eq!(unread(db, A).await, 0);
    let failure=serde_json::json!({"code":"provider-error","message":"m".repeat(4096),"class":"provider","details":{"secret":"not for the list"}}).to_string();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE task_run SET status='failed',result_json=NULL,error_json=$1 WHERE id='run'",
        [failure.into()],
    ))
    .await
    .unwrap();
    let row = find_task_completion_inbox_entry(db, &access(A), "run")
        .await
        .unwrap()
        .unwrap();
    let error = row.error_json.unwrap();
    assert!(error.len() < 1536);
    assert!(!error.contains("not for the list"));
    assert!(row.read_at.is_some());
    // A reopened blocked run is no longer a completed entry despite its stale
    // saved completion timestamp. The later terminal state reuses its run ID.
    db.execute_unprepared("UPDATE task_run SET status='running' WHERE id='run'")
        .await
        .unwrap();
    assert!(
        find_task_completion_inbox_entry(db, &access(A), "run")
            .await
            .unwrap()
            .is_none()
    );
    db.execute_unprepared("UPDATE task_run SET status='succeeded',error_json=NULL WHERE id='run'")
        .await
        .unwrap();
    // Reopening the same run does not revoke its personal read receipt.
    assert!(
        list_task_completion_inbox(db, &access(A), None, 30)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(unread(db, A).await, 0);
    assert!(
        find_task_completion_inbox_entry(db, &access(A), "run")
            .await
            .unwrap()
            .unwrap()
            .read_at
            .is_some()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn an_unloaded_old_payload_is_not_parsed_to_return_a_metadata_page_or_total() {
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "task",
        Some("T0000000000000000000P"),
        None,
        "agent",
        "none",
        "detached",
    )
    .await;
    for ix in 0..31 {
        run(db, &format!("run-{ix:03}"), "task", "succeeded", ix + 1).await;
    }
    // PostgreSQL validates JSON on write. Give the historical result a large
    // body and a distinguishable summary for the exact later page.
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE task_run SET result_json=$1 WHERE id='run-000'",
        [serde_json::json!({
            "summary":"history outside the requested page",
            "data":{"details":"界".repeat(65_536)}
        })
        .to_string()
        .into()],
    ))
    .await
    .unwrap();
    let page = list_task_completion_inbox(db, &access(A), None, 30)
        .await
        .unwrap();
    assert_eq!(page.len(), 30);
    assert_eq!(page.last().unwrap().run_id, "run-001");
    assert_eq!(unread(db, A).await, 31);
    let last = page.last().unwrap();
    let tail = list_task_completion_inbox(
        db,
        &access(A),
        Some((&last.completed_key, &last.run_id)),
        30,
    )
    .await
    .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].run_id, "run-000");
    assert_eq!(
        tail[0].summary.as_deref(),
        Some("history outside the requested page")
    );
}

#[pioneer_postgres_test::postgres_test]
async fn empty_and_small_workspaces_page_independently_of_large_foreign_history() {
    let fixture = fixture().await;
    let db = fixture.connection();
    db.execute_unprepared("INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal,priority,revision,created_at,updated_at) VALUES('foreign-task','W0000000000000000000O','workspace','W0000000000000000000O','system','completed','Foreign','Goal',0,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<4096) INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,result_json,created_at,updated_at) SELECT 'foreign-' || lpad(n::text, 5, '0'),'foreign-task','foreign-' || lpad(n::text, 5, '0'),1,n,'succeeded','system','2099-01-01 00:00:00 +00:00',json_build_object('summary','foreign run must not be projected','data',n)::text,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM numbers").await.unwrap();
    // No tasks in the requested workspace: the outer workspace index seek
    // yields no run lookups, regardless of newer completions elsewhere.
    assert!(
        list_task_completion_inbox(db, &access(A), None, 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        count_unread_task_completion_inbox(db, &access(A))
            .await
            .unwrap(),
        0
    );
    task(
        db,
        "local",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "attached",
    )
    .await;
    run(db, "local-pending", "local", "running", 6).await;
    assert!(
        list_task_completion_inbox(db, &access(A), None, 2)
            .await
            .unwrap()
            .is_empty()
    );
    for ix in 0..5 {
        run(db, &format!("local-{ix}"), "local", "succeeded", ix + 1).await;
    }
    let mut cursor: Option<(String, String)> = None;
    let mut ids = Vec::new();
    loop {
        let page = list_task_completion_inbox(
            db,
            &access(A),
            cursor.as_ref().map(|(at, id)| (at.as_str(), id.as_str())),
            2,
        )
        .await
        .unwrap();
        assert_eq!(
            count_unread_task_completion_inbox(db, &access(A))
                .await
                .unwrap(),
            5
        );
        assert!(
            page.iter()
                .all(|row| row.task_id == "local" && row.summary.as_deref() == Some("summary"))
        );
        if page.is_empty() {
            break;
        }
        let last = page.last().unwrap();
        cursor = Some((last.completed_key.clone(), last.run_id.clone()));
        ids.extend(page.into_iter().map(|row| row.run_id));
    }
    assert_eq!(ids, ["local-4", "local-3", "local-2", "local-1", "local-0"]);
    // Replaying the loaded prefix after an ACL loss and a resumed run must
    // remove both rows. Continuation starts after the new, authorized page.
    task(
        db,
        "hidden",
        Some("T0000000000000000000V"),
        None,
        "agent",
        "thread",
        "detached",
    )
    .await;
    run(db, "private-run", "hidden", "succeeded", 1).await;
    mark_personal_read(db, A, "local-2").await;
    db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000A' AND thread_id='T0000000000000000000V'").await.unwrap();
    db.execute_unprepared("UPDATE task_run SET status='running' WHERE id='local-3'")
        .await
        .unwrap();
    let page = list_task_completion_inbox(db, &access(A), None, 3)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|row| row.run_id.as_str())
            .collect::<Vec<_>>(),
        ["local-4", "local-1", "local-0"]
    );
    assert!(page.iter().all(|row| row.read_at.is_none()));
    assert_eq!(
        count_unread_task_completion_inbox(db, &access(A))
            .await
            .unwrap(),
        3
    );
    let last = page.last().unwrap();
    let next =
        list_task_completion_inbox(db, &access(A), Some((&last.completed_key, &last.run_id)), 3)
            .await
            .unwrap();
    assert_eq!(
        next.iter()
            .map(|row| row.run_id.as_str())
            .collect::<Vec<_>>(),
        Vec::<&str>::new()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn unread_filter_precedes_limit_and_keyset_cursor() {
    let fixture = fixture().await;
    let db = fixture.connection();
    task(
        db,
        "pages",
        Some("T0000000000000000000P"),
        None,
        "tool",
        "none",
        "detached",
    )
    .await;
    for ix in 0..80 {
        let id = format!("page-{ix:03}");
        run(db, &id, "pages", "succeeded", ix + 1).await;
        if ix >= 35 {
            mark_personal_read(db, A, &id).await;
        }
    }
    let first = list_task_completion_inbox(db, &access(A), None, 30)
        .await
        .unwrap();
    assert_eq!(first.len(), 30);
    assert_eq!(first[0].run_id, "page-034");
    let anchor = first.last().unwrap();
    let next = list_task_completion_inbox(
        db,
        &access(A),
        Some((&anchor.completed_key, &anchor.run_id)),
        30,
    )
    .await
    .unwrap();
    assert_eq!(next.len(), 5);
    assert_eq!(unread(db, A).await, 35);
}
