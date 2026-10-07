//! Regression code for Proposal 72/03. Compile only until implementation review.
use super::*;
use crate::repositories::{task_actor_contract as contracts, task_occurrence_reconcile as queue};
use crate::{
    TaskOccurrenceClaimDeferral, TaskOccurrenceClaimFailure, TaskOccurrenceReconcileClaim,
    TaskOccurrenceTerminalRepairOutcome as Outcome,
};
use pioneer_entity::{
    agent_execution as agents, task_occurrence_contract as occurrences,
    task_occurrence_reconcile_pending as pending, task_occurrence_reconcile_scope as scopes,
    task_occurrence_reconcile_seed as seed, task_occurrence_reconcile_sequence as sequence,
    task_run as runs, task_run_execution as executions,
};
use sea_orm::{ActiveModelTrait, FromQueryResult, IntoActiveModel};

const NOW: i64 = 4_000_000_000;
const AT: i64 = 1_700_000_000;
const MIGRATION: &str = "m20261004_000001_task_occurrence_reconcile";

// Pin migration fixtures to the schema under test so later, irreversible
// migrations do not participate in its rollback or installation checks.
struct TrackerMigrator;

impl MigratorTrait for TrackerMigrator {
    fn migrations() -> Vec<Box<dyn migration::MigrationTrait>> {
        migrations_through(MIGRATION)
    }
}

async fn task(store: &CrudStore, kind: TaskExecutorKind) -> Task {
    let mut task = sample_task(AT);
    task.executor_kind = kind;
    let prepared = crate::repositories::task::prepare_task_projection(&task).unwrap();
    crate::repositories::task::upsert_prepared_task(&store.connection, prepared)
        .await
        .unwrap();
    task
}

async fn source_run(store: &CrudStore, task: &Task, n: u64, status: TaskRunStatus) -> TaskRun {
    let mut run = sample_task_run(AT);
    run.id = format!("r{n:020}");
    run.run_group_id = run.id.clone();
    run.task_id = task.id.clone();
    run.run_number = (n + 1).try_into().unwrap();
    run.executor_kind = task.executor_kind;
    run.trigger_id = None;
    run.status = status;
    run.completed_at = status.is_terminal().then_some(AT + 1);
    crate::repositories::task_run::upsert_run(&store.connection, &run)
        .await
        .unwrap();
    run
}

async fn authorities(store: &CrudStore, task: &Task, run: &TaskRun, n: u64) -> String {
    let id = format!("e{n:020}");
    crate::repositories::task_run_execution::insert_execution_if_absent(
        &store.connection,
        crate::repositories::task_run_execution::NewTaskRunExecution {
            id: id.clone(),
            task_id: task.id.clone(),
            task_run_id: run.id.clone(),
            executor_kind: task.executor_kind,
            status: TaskRunExecutionStatus::Succeeded,
            worker_id: None,
            lease_until: None,
            heartbeat_at: None,
            started_at: Some(AT),
            completed_at: Some(AT + 1),
            result: None,
            error: None,
            created_at: AT,
            updated_at: AT + 1,
        },
    )
    .await
    .unwrap();
    if task.executor_kind == TaskExecutorKind::Agent {
        ensure_test_agent_identity(store, "ws_task", AT).await;
        attach_test_agent_execution_contract(
            store, "ws_task", &task.id, &run.id, &id, "thr_task", AT,
        )
        .await;
        agents::Entity::update_many()
            .col_expr(agents::Column::Status, Expr::val("succeeded"))
            .col_expr(
                agents::Column::FinishedAt,
                Expr::val(unix_to_datetime(AT + 1)),
            )
            .filter(agents::Column::Id.eq(id.clone()))
            .exec(&store.connection)
            .await
            .unwrap();
    } else {
        store
            .upsert_task_occurrence_contract(
                &TaskOccurrenceContract {
                    occurrence_id: run.id.clone(),
                    task_id: task.id.clone(),
                    run_id: run.id.clone(),
                    trigger_id: None,
                    occurrence_key: format!("immediate:{}", run.id),
                    execution_generation: n + 1,
                    agent_execution_id: None,
                    work_graph_root_execution_id: None,
                    root_resource_scope_id: None,
                    status: TaskOccurrenceStatus::Running,
                    queue_position: None,
                    retry_attempt: 0,
                    action_idempotency_key: format!("action:{}", run.id),
                    route_id: None,
                    result_return_route_id: None,
                    delivery_plan: None,
                    terminal_reason: None,
                },
                AT,
            )
            .await
            .unwrap();
    }
    id
}

async fn fixture(kind: TaskExecutorKind) -> (CrudStore, TaskRun, String) {
    let store = test_store_with_workspace("ws_task").await;
    let task = task(&store, kind).await;
    let run = source_run(&store, &task, 0, TaskRunStatus::Succeeded).await;
    let id = authorities(&store, &task, &run, 0).await;
    (store, run, id)
}

async fn pending_row(store: &CrudStore, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id.to_owned())
        .one(&store.connection)
        .await
        .unwrap()
}
async fn claim(store: &CrudStore, id: &str, now: i64) -> TaskOccurrenceReconcileClaim {
    let row = pending_row(store, id).await.unwrap();
    store
        .claim_task_occurrence_reconcile(
            &crate::TaskOccurrenceReconcileCandidate {
                run_id: id.to_owned(),
                generation: row.generation,
            },
            &|| now,
        )
        .await
        .unwrap()
        .unwrap()
}
async fn clear_bookkeeping(store: &CrudStore) {
    pending::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
    scopes::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
    seed::Entity::update_many()
        .col_expr(seed::Column::StatusIndex, Expr::val(5))
        .exec(&store.connection)
        .await
        .unwrap();
}
async fn occurrence_status(store: &CrudStore, id: &str, status: &str) {
    occurrences::Entity::update_many()
        .col_expr(occurrences::Column::Status, Expr::val(status))
        .filter(occurrences::Column::RunId.eq(id))
        .exec(&store.connection)
        .await
        .unwrap();
}
async fn sql(store: &CrudStore, text: &str, values: Vec<sea_orm::Value>) {
    store
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            text,
            values,
        ))
        .await
        .unwrap();
}
async fn matches_diagnostic(store: &CrudStore, run_id: &str, expected: bool) {
    let facts = contracts::load_terminal_occurrence_metadata(&store.connection, run_id)
        .await
        .unwrap();
    assert_eq!(facts.is_mismatch(), expected);
    let scan = store
        .list_terminal_task_occurrence_mismatches(100)
        .await
        .unwrap();
    assert_eq!(
        scan.iter().any(|row| row.run_id == run_id),
        expected,
        "metadata must preserve the literal old five-authority SQL predicate"
    );
}

#[tokio::test]
async fn literal_predicate_status_executor_and_system_null_binding_matrix() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    for (status, target) in [
        ("succeeded", Some("delivered")),
        ("cancelled", Some("cancelled")),
        ("failed", Some("failed")),
        ("blocked", Some("failed")),
        ("timed_out", Some("failed")),
        ("running", None),
        ("unknown", None),
    ] {
        sql(
            &store,
            "UPDATE task_run SET status=? WHERE id=?",
            vec![status.into(), run.id.clone().into()],
        )
        .await;
        for execution_status in [status, "different"] {
            sql(
                &store,
                "UPDATE task_run_execution SET status=? WHERE task_run_id=?",
                vec![execution_status.into(), run.id.clone().into()],
            )
            .await;
            for occurrence_status_value in
                ["running", "delivered", "failed", "cancelled", "unknown"]
            {
                occurrence_status(&store, &run.id, occurrence_status_value).await;
                matches_diagnostic(
                    &store,
                    &run.id,
                    target.is_some_and(|target| target != occurrence_status_value)
                        && status == execution_status,
                )
                .await;
            }
        }
    }
    sql(
        &store,
        "UPDATE task_run SET status='succeeded' WHERE id=?",
        vec![run.id.clone().into()],
    )
    .await;
    sql(
        &store,
        "UPDATE task_run_execution SET status='succeeded' WHERE task_run_id=?",
        vec![run.id.clone().into()],
    )
    .await;
    occurrence_status(&store, &run.id, "running").await;
    for (a, b, c) in [
        (None, None, None),
        (Some("x"), None, None),
        (None, Some("x"), None),
        (None, None, Some("x")),
        (Some("x"), Some("x"), Some("x")),
    ] {
        sql(&store,"UPDATE task_occurrence_contract SET agent_execution_id=?,work_graph_root_execution_id=?,root_resource_scope_id=? WHERE run_id=?",
            vec![a.map(String::from).into(),b.map(String::from).into(),c.map(String::from).into(),run.id.clone().into()]).await;
        matches_diagnostic(&store, &run.id, a.is_none() && b.is_none() && c.is_none()).await;
    }
    sql(&store,"UPDATE task_occurrence_contract SET agent_execution_id=NULL,work_graph_root_execution_id=NULL,root_resource_scope_id=NULL WHERE run_id=?",vec![run.id.clone().into()]).await;
    for (table, key) in [("task_run", "id"), ("task_run_execution", "task_run_id")] {
        sql(
            &store,
            &format!("UPDATE {table} SET completed_at=NULL WHERE {key}=?"),
            vec![run.id.clone().into()],
        )
        .await;
        matches_diagnostic(&store, &run.id, false).await;
        sql(
            &store,
            &format!("UPDATE {table} SET completed_at=? WHERE {key}=?"),
            vec![unix_to_datetime(AT + 1).into(), run.id.clone().into()],
        )
        .await;
    }
    for kind in ["agent", "system", "unknown"] {
        sql(
            &store,
            "UPDATE task SET executor_kind=? WHERE id=?",
            vec![kind.into(), run.task_id.clone().into()],
        )
        .await;
        matches_diagnostic(&store, &run.id, kind == "system").await;
    }
}

#[tokio::test]
async fn literal_agent_identity_and_null_matrix() {
    let (store, run, id) = fixture(TaskExecutorKind::Agent).await;
    matches_diagnostic(&store, &run.id, true).await;
    let original = agents::Entity::find_by_id(id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    for mutation in [
        "parent_task_id=NULL",
        "parent_task_id='other'",
        "execution_generation=2",
        "status='failed'",
        "finished_at=NULL",
        "work_graph_root_execution_id='z00000000000000000000'",
    ] {
        sql(
            &store,
            &format!("UPDATE agent_execution SET {mutation} WHERE id=?"),
            vec![id.clone().into()],
        )
        .await;
        matches_diagnostic(&store, &run.id, false).await;
        original
            .clone()
            .into_active_model()
            .reset_all()
            .update(&store.connection)
            .await
            .unwrap();
    }
    for mutation in [
        "agent_execution_id=NULL",
        "agent_execution_id='other'",
        "work_graph_root_execution_id=NULL",
        "work_graph_root_execution_id='other'",
        "root_resource_scope_id=NULL",
        "root_resource_scope_id='other'",
        "execution_generation=2",
        "task_id='other'",
    ] {
        let original = occurrences::Entity::find_by_id(run.id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
        sql(
            &store,
            &format!("UPDATE task_occurrence_contract SET {mutation} WHERE run_id=?"),
            vec![run.id.clone().into()],
        )
        .await;
        matches_diagnostic(&store, &run.id, false).await;
        original
            .into_active_model()
            .reset_all()
            .update(&store.connection)
            .await
            .unwrap();
    }
    sql(
        &store,
        "UPDATE task SET workspace_id='rebound' WHERE id=?",
        vec![run.task_id.clone().into()],
    )
    .await;
    matches_diagnostic(&store, &run.id, false).await;
    sql(
        &store,
        "UPDATE task SET workspace_id='ws_task',executor_kind='system' WHERE id=?",
        vec![run.task_id.clone().into()],
    )
    .await;
    matches_diagnostic(&store, &run.id, false).await;
}

#[tokio::test]
async fn status_repair_and_post_trigger_ack_are_one_commit_and_rollback_together() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    let claimed = claim(&store, &run.id, NOW).await;
    let before = pending_row(&store, &run.id).await.unwrap();
    store.connection.execute_unprepared("CREATE TRIGGER reject_contract_ack BEFORE DELETE ON task_occurrence_reconcile_pending BEGIN SELECT RAISE(ABORT,'ack refusal'); END").await.unwrap();
    assert!(
        store
            .reconcile_claimed_task_occurrence(&claimed, NOW)
            .await
            .is_err()
    );
    assert_eq!(pending_row(&store, &run.id).await.unwrap(), before);
    matches_diagnostic(&store, &run.id, true).await;
    store
        .connection
        .execute_unprepared("DROP TRIGGER reject_contract_ack")
        .await
        .unwrap();
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&claimed, NOW)
            .await
            .unwrap(),
        Outcome::Changed
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    matches_diagnostic(&store, &run.id, false).await;
    // A subsequent mutation is a new obligation, even at the same timestamp.
    occurrence_status(&store, &run.id, "recovering").await;
    let new = pending_row(&store, &run.id).await.unwrap();
    assert!(new.generation > claimed.generation);
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&claimed, NOW)
            .await
            .unwrap(),
        Outcome::StaleClaim
    );
    assert_eq!(pending_row(&store, &run.id).await.unwrap(), new);
}

#[tokio::test]
async fn preparation_mutation_and_task_scope_rebinding_fence_stale_repair() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    let claimed = claim(&store, &run.id, NOW).await;
    let facts = contracts::load_terminal_occurrence_metadata(&store.connection, &run.id)
        .await
        .unwrap();
    sql(
        &store,
        "UPDATE task SET executor_kind='agent' WHERE id=?",
        vec![run.task_id.clone().into()],
    )
    .await;
    // Task scope trigger does not enumerate runs or change this pending row.
    assert_eq!(
        pending_row(&store, &run.id).await.unwrap().generation,
        claimed.generation
    );
    assert_eq!(
        store
            .with_maintenance_access()
            .commit_terminal_task_occurrence(&run.id, NOW, Some((&claimed, &facts)))
            .await
            .unwrap(),
        Outcome::StaleClaim
    );
    matches_diagnostic(&store, &run.id, false).await;
    sql(
        &store,
        "UPDATE task SET executor_kind='system' WHERE id=?",
        vec![run.task_id.clone().into()],
    )
    .await;
    occurrence_status(&store, &run.id, "recovering").await;
    let newer = pending_row(&store, &run.id).await.unwrap();
    assert_eq!(
        store
            .with_maintenance_access()
            .commit_terminal_task_occurrence(&run.id, NOW, Some((&claimed, &facts)))
            .await
            .unwrap(),
        Outcome::StaleClaim
    );
    assert_eq!(pending_row(&store, &run.id).await.unwrap(), newer);
}

#[tokio::test]
async fn missing_late_authorities_and_same_id_aba_are_covered() {
    let store = test_store_with_workspace("ws_task").await;
    let task = task(&store, TaskExecutorKind::System).await;
    let run = source_run(&store, &task, 0, TaskRunStatus::Succeeded).await;
    let claimed = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&claimed, NOW)
            .await
            .unwrap(),
        Outcome::NotRepairable
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    let id = authorities(&store, &task, &run, 0).await;
    let claimed = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&claimed, NOW)
            .await
            .unwrap(),
        Outcome::Changed
    );
    let execution = executions::Entity::find_by_id(id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    executions::Entity::delete_by_id(id)
        .exec(&store.connection)
        .await
        .unwrap();
    let deleted = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&deleted, NOW)
            .await
            .unwrap(),
        Outcome::NotRepairable
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    executions::Entity::insert(execution.into_active_model())
        .exec(&store.connection)
        .await
        .unwrap();
    let reinserted = pending_row(&store, &run.id).await.unwrap();
    assert!(reinserted.generation > deleted.generation);
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&deleted, NOW)
            .await
            .unwrap(),
        Outcome::StaleClaim
    );
    assert_eq!(pending_row(&store, &run.id).await.unwrap(), reinserted);
}

#[tokio::test]
async fn late_task_occurrence_and_run_requeue_after_missing_authority_ack() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    let task = pioneer_entity::task::Entity::find_by_id(run.task_id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    pioneer_entity::task::Entity::delete_by_id(run.task_id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    let owned = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&owned, NOW)
            .await
            .unwrap(),
        Outcome::NotRepairable
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    pioneer_entity::task::Entity::insert(task.into_active_model())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(
        pending_row(&store, &run.id).await.is_none(),
        "Task INSERT coalesces a scope"
    );
    assert_eq!(
        store.expand_task_occurrence_scope(&|| NOW).await.unwrap(),
        2
    );
    let owned = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&owned, NOW)
            .await
            .unwrap(),
        Outcome::Changed
    );

    let occurrence = occurrences::Entity::find_by_id(run.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    occurrences::Entity::delete_by_id(run.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    let owned = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&owned, NOW)
            .await
            .unwrap(),
        Outcome::NotFound
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    let mut occurrence = occurrence.into_active_model();
    occurrence.status = Set("recovering".into());
    occurrences::Entity::insert(occurrence)
        .exec(&store.connection)
        .await
        .unwrap();
    let owned = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&owned, NOW)
            .await
            .unwrap(),
        Outcome::Changed
    );

    let source = runs::Entity::find_by_id(run.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    runs::Entity::delete_by_id(run.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    let owned = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&owned, NOW)
            .await
            .unwrap(),
        Outcome::NotFound
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    runs::Entity::insert(source.into_active_model())
        .exec(&store.connection)
        .await
        .unwrap();
    let owned = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&owned, NOW)
            .await
            .unwrap(),
        Outcome::AlreadyConsistent
    );
    assert!(pending_row(&store, &run.id).await.is_none());
}

#[tokio::test]
async fn agent_late_insert_uses_execution_pk_and_does_not_invent_bindings() {
    let (store, run, id) = fixture(TaskExecutorKind::Agent).await;
    let model = agents::Entity::find_by_id(id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    // Resource/grant FKs intentionally restrict deletion; use a second shared
    // execution id to represent a missing, later inserted authority.
    let late = "l00000000000000000000";
    sql(
        &store,
        "UPDATE task_run_execution SET id=? WHERE task_run_id=?",
        vec![late.into(), run.id.clone().into()],
    )
    .await;
    sql(&store,"UPDATE task_occurrence_contract SET agent_execution_id=?,work_graph_root_execution_id=?,root_resource_scope_id=? WHERE run_id=?",
        vec![late.into(),late.into(),late.into(),run.id.clone().into()]).await;
    let claimed = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&claimed, NOW)
            .await
            .unwrap(),
        Outcome::NotRepairable
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    let mut new = model.into_active_model();
    new.id = Set(late.into());
    new.work_graph_root_execution_id = Set(late.into());
    agents::Entity::insert(new)
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(pending_row(&store, &run.id).await.is_some());
    let claimed = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&claimed, NOW)
            .await
            .unwrap(),
        Outcome::Changed
    );
}

#[tokio::test]
async fn heartbeat_lease_payload_progress_and_identical_upserts_do_not_refresh_dirty() {
    let (store, run, id) = fixture(TaskExecutorKind::System).await;
    let before = pending_row(&store, &run.id).await.unwrap();
    let scope_before = scopes::Entity::find_by_id(run.task_id.clone())
        .one(&store.connection)
        .await
        .unwrap();
    sql(&store,"UPDATE task_run SET heartbeat_at=CURRENT_TIMESTAMP,lock_expires_at=CURRENT_TIMESTAMP,locked_by='worker',result_json='{}',error_json='{}',updated_at=CURRENT_TIMESTAMP WHERE id=?",vec![run.id.clone().into()]).await;
    sql(&store,"UPDATE task_run_execution SET heartbeat_at=CURRENT_TIMESTAMP,lease_until=CURRENT_TIMESTAMP,worker_id='worker',result_json='{}',error_json='{}',updated_at=CURRENT_TIMESTAMP WHERE id=?",vec![id.into()]).await;
    sql(&store,"UPDATE task_occurrence_contract SET queue_position=3,terminal_reason='progress',delivery_plan_json='{}',status=status WHERE run_id=?",vec![run.id.clone().into()]).await;
    sql(&store,"UPDATE task SET title='progress',revision=revision+1,result_json='{}',executor_kind=executor_kind,workspace_id=workspace_id WHERE id=?",vec![run.task_id.clone().into()]).await;
    crate::repositories::task_run::upsert_run(&store.connection, &run)
        .await
        .unwrap();
    task(&store, TaskExecutorKind::System).await;
    assert_eq!(pending_row(&store, &run.id).await.unwrap(), before);
    assert_eq!(
        scopes::Entity::find_by_id(run.task_id.clone())
            .one(&store.connection)
            .await
            .unwrap(),
        scope_before
    );
    let (agent_store, agent_run, agent_id) = fixture(TaskExecutorKind::Agent).await;
    sql(
        &agent_store,
        "UPDATE agent_execution SET status='running',finished_at=NULL WHERE id=?",
        vec![agent_id.clone().into()],
    )
    .await;
    sql(
        &agent_store,
        "UPDATE task_run_execution SET status='running',completed_at=NULL WHERE id=?",
        vec![agent_id.clone().into()],
    )
    .await;
    sql(
        &agent_store,
        "UPDATE task_run SET status='running',completed_at=NULL WHERE id=?",
        vec![agent_run.id.clone().into()],
    )
    .await;
    pioneer_entity::agent_execution_resource_state::Entity::update_many()
        .col_expr(
            pioneer_entity::agent_execution_resource_state::Column::Status,
            Expr::val("running"),
        )
        .filter(
            pioneer_entity::agent_execution_resource_state::Column::ExecutionId
                .eq(agent_id.clone()),
        )
        .exec(&agent_store.connection)
        .await
        .unwrap();
    let before = pending_row(&agent_store, &agent_run.id).await.unwrap();
    assert!(
        agent_store
            .heartbeat_execution_for_agent_attempt(&agent_id, 1, AT + 20, None)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        agent_store
            .record_agent_execution_progress(&agent_id, 1, "{}", AT + 21, None)
            .await
            .unwrap()
    );
    sql(
        &agent_store,
        "UPDATE agent_execution SET updated_at=CURRENT_TIMESTAMP,status=status WHERE id=?",
        vec![agent_id.into()],
    )
    .await;
    assert_eq!(
        pending_row(&agent_store, &agent_run.id).await.unwrap(),
        before
    );
}

#[tokio::test]
async fn scope_fanout_is_bounded_fenced_and_covers_new_ids_behind_cursor() {
    let store = test_store_with_workspace("ws_task").await;
    let task = task(&store, TaskExecutorKind::System).await;
    for n in 0..100 {
        source_run(&store, &task, n, TaskRunStatus::Succeeded).await;
    }
    clear_bookkeeping(&store).await;
    sql(
        &store,
        "UPDATE task SET executor_kind='agent' WHERE id=?",
        vec![task.id.clone().into()],
    )
    .await;
    assert!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .is_empty()
    );
    let before = scopes::Entity::find_by_id(task.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        store.expand_task_occurrence_scope(&|| NOW).await.unwrap(),
        16
    );
    assert_eq!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .len(),
        15
    );
    let progressed = scopes::Entity::find_by_id(task.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert!(progressed.after_run_id.is_some());
    assert!(progressed.generation > before.generation);
    let mut behind = sample_task_run(AT);
    behind.id = "a00000000000000000000".into();
    behind.run_group_id = behind.id.clone();
    behind.trigger_id = None;
    behind.executor_kind = TaskExecutorKind::System;
    behind.run_number = 1000;
    crate::repositories::task_run::upsert_run(&store.connection, &behind)
        .await
        .unwrap();
    assert!(pending_row(&store, &behind.id).await.is_some());
    let fixed = progressed.upper_run_id.clone();
    source_run(&store, &task, 1000, TaskRunStatus::Succeeded).await;
    assert_eq!(
        scopes::Entity::find_by_id(task.id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
            .upper_run_id,
        fixed
    );
    sql(
        &store,
        "UPDATE task SET workspace_id='rebound' WHERE id=?",
        vec![task.id.clone().into()],
    )
    .await;
    let reset = scopes::Entity::find_by_id(task.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert!(reset.generation > progressed.generation);
    assert!(reset.after_run_id.is_none());
    // Several restart-safe quanta finish this one changed Task, never a cycle.
    for _ in 0..8 {
        assert!(
            store
                .clone()
                .expand_task_occurrence_scope(&|| NOW)
                .await
                .unwrap()
                <= 16
        );
    }
    assert!(
        scopes::Entity::find_by_id(task.id)
            .one(&store.connection)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.expand_task_occurrence_scope(&|| NOW).await.unwrap(),
        0
    );
    assert_eq!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .len(),
        102
    );
}

#[tokio::test]
async fn migration_objects_and_marker_rollback_on_trigger_installation_failure() {
    let store = test_store_with_workspace_migrator::<TrackerMigrator>("ws_task")
        .await
        .with_maintenance_access();
    let tx = store.connection.begin().await.unwrap();
    TrackerMigrator::down(&*tx, Some(1)).await.unwrap();
    tx.commit().await.unwrap();
    store.connection.execute_unprepared("CREATE TRIGGER task_occurrence_reconcile_task_run_update AFTER UPDATE ON task_run WHEN 0 BEGIN SELECT 1; END").await.unwrap();
    let tx = store.connection.begin().await.unwrap();
    assert!(TrackerMigrator::up(&*tx, None).await.is_err());
    tx.rollback().await.unwrap();
    for query in [
        "SELECT name FROM sqlite_master WHERE name IN ('task_occurrence_reconcile_pending','task_occurrence_reconcile_scope','task_occurrence_reconcile_seed','task_occurrence_reconcile_sequence','idx_task_occurrence_reconcile_due','idx_task_run_task_id','idx_task_run_status_id','task_occurrence_reconcile_task_run_insert')",
        "SELECT version FROM seaql_migrations WHERE version='m20261004_000001_task_occurrence_reconcile'",
    ] {
        assert!(
            store
                .connection
                .query_all_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    query.to_owned()
                ))
                .await
                .unwrap()
                .is_empty()
        );
    }
    store
        .connection
        .execute_unprepared("DROP TRIGGER task_occurrence_reconcile_task_run_update")
        .await
        .unwrap();
    let tx = store.connection.begin().await.unwrap();
    TrackerMigrator::up(&*tx, None).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        seed::Entity::find_by_id(1)
            .one(&store.connection)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn seed_interruption_keeps_fixed_bound_and_accepts_terminal_history() {
    let store = test_store_with_workspace_migrator::<TrackerMigrator>("ws_task").await;
    let maintenance = store.with_maintenance_access();
    let tx = maintenance.connection.begin().await.unwrap();
    TrackerMigrator::down(&*tx, Some(1)).await.unwrap();
    tx.commit().await.unwrap();
    let task = task(&store, TaskExecutorKind::System).await;
    for n in 0..40 {
        source_run(&store, &task, n, TaskRunStatus::Queued).await;
    }
    source_run(&store, &task, 50, TaskRunStatus::Succeeded).await;
    let tx = maintenance.connection.begin().await.unwrap();
    TrackerMigrator::up(&*tx, None).await.unwrap();
    tx.commit().await.unwrap();
    let initial = seed::Entity::find_by_id(1)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(initial.upper_run_id, Some(format!("r{:020}", 39)));
    assert!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .is_empty()
    );
    store.connection.execute_unprepared("CREATE TRIGGER reject_seed_progress BEFORE UPDATE ON task_occurrence_reconcile_seed BEGIN SELECT RAISE(ABORT,'interrupted'); END").await.unwrap();
    assert!(
        store
            .seed_unfinished_task_occurrences(&|| NOW)
            .await
            .is_err()
    );
    assert_eq!(
        seed::Entity::find_by_id(1)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap(),
        initial
    );
    assert!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER reject_seed_progress")
        .await
        .unwrap();
    assert_eq!(
        store
            .seed_unfinished_task_occurrences(&|| NOW)
            .await
            .unwrap(),
        16
    );
    source_run(&store, &task, 100, TaskRunStatus::Queued).await; // own INSERT, outside fixed seed bound
    assert!(
        pending_row(&store, &format!("r{:020}", 100))
            .await
            .is_some()
    );
    for _ in 0..7 {
        assert!(
            store
                .clone()
                .seed_unfinished_task_occurrences(&|| NOW)
                .await
                .unwrap()
                <= 16
        );
    }
    let complete = seed::Entity::find_by_id(1)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(complete.status_index, 5);
    assert_eq!(
        store
            .seed_unfinished_task_occurrences(&|| NOW)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .len(),
        41
    );
    assert!(pending_row(&store, &format!("r{:020}", 50)).await.is_none());
}

#[tokio::test]
async fn failed_claim_deferral_fences_null_token_due_count_generation_and_stale_error() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    store.connection.execute_unprepared("CREATE TRIGGER refuse_contract_claim BEFORE UPDATE OF claim_token ON task_occurrence_reconcile_pending WHEN NEW.claim_token IS NOT NULL BEGIN SELECT RAISE(ABORT,'claim refusal'); END").await.unwrap();
    let before = pending_row(&store, &run.id).await.unwrap();
    let candidate = crate::TaskOccurrenceReconcileCandidate {
        run_id: run.id.clone(),
        generation: before.generation,
    };
    let error = store
        .claim_task_occurrence_reconcile(&candidate, &|| NOW)
        .await
        .unwrap_err();
    let error = error.downcast::<TaskOccurrenceClaimFailure>().unwrap();
    assert_eq!(error.deferral, TaskOccurrenceClaimDeferral::Deferred);
    let deferred = pending_row(&store, &run.id).await.unwrap();
    assert_eq!(deferred.claim_token, None);
    assert_eq!(deferred.attempt_count, 1);
    assert_eq!(deferred.next_attempt_at, NOW + 5);
    assert_eq!(
        queue::defer_failed_claim(
            &store.with_maintenance_access().connection,
            &before,
            &|| NOW + 100
        )
        .await
        .unwrap(),
        TaskOccurrenceClaimDeferral::StateChanged
    );
    occurrence_status(&store, &run.id, "recovering").await;
    let refreshed = pending_row(&store, &run.id).await.unwrap();
    assert!(refreshed.generation > deferred.generation);
    assert_eq!(refreshed.next_attempt_at, deferred.next_attempt_at);
    assert_eq!(
        queue::defer_failed_claim(
            &store.with_maintenance_access().connection,
            &deferred,
            &|| NOW + 100
        )
        .await
        .unwrap(),
        TaskOccurrenceClaimDeferral::StateChanged
    );
    store
        .connection
        .execute_unprepared("DROP TRIGGER refuse_contract_claim")
        .await
        .unwrap();
    let owned = claim(&store, &run.id, NOW + 5).await;
    let snapshot = pending_row(&store, &run.id).await.unwrap();
    assert_eq!(
        queue::defer_failed_claim(
            &store.with_maintenance_access().connection,
            &refreshed,
            &|| NOW + 100
        )
        .await
        .unwrap(),
        TaskOccurrenceClaimDeferral::StateChanged
    );
    assert_eq!(pending_row(&store, &run.id).await.unwrap(), snapshot);
    assert_eq!(owned.attempt_count, 2);
    assert_eq!(owned.next_attempt_at, NOW + 15);
}

#[tokio::test]
async fn resume_retry_rebind_delete_and_monotonic_sequence_overflow() {
    let (store, run, id) = fixture(TaskExecutorKind::System).await;
    let owned = claim(&store, &run.id, NOW).await;
    sql(
        &store,
        "UPDATE task_run SET status='queued',completed_at=NULL WHERE id=?",
        vec![run.id.clone().into()],
    )
    .await;
    let resumed = pending_row(&store, &run.id).await.unwrap();
    assert!(resumed.generation > owned.generation);
    assert_eq!(resumed.next_attempt_at, owned.next_attempt_at);
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&owned, NOW)
            .await
            .unwrap(),
        Outcome::StaleClaim
    );
    let claimed = claim(&store, &run.id, NOW + 5).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&claimed, NOW)
            .await
            .unwrap(),
        Outcome::NotRepairable
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    let new = "new_run_for_rebinding";
    sql(
        &store,
        "UPDATE task_run_execution SET task_run_id=? WHERE id=?",
        vec![new.into(), id.into()],
    )
    .await;
    assert!(pending_row(&store, &run.id).await.is_some());
    assert!(pending_row(&store, new).await.is_some());
    pending::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
    sql(
        &store,
        "UPDATE task_occurrence_contract SET run_id=? WHERE run_id=?",
        vec![new.into(), run.id.clone().into()],
    )
    .await;
    assert!(pending_row(&store, &run.id).await.is_some());
    assert!(pending_row(&store, new).await.is_some());
    pending::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
    runs::Entity::delete_by_id(run.id.clone())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(pending_row(&store, &run.id).await.is_some());
    sequence::Entity::update_many()
        .col_expr(sequence::Column::Generation, Expr::val(i64::MAX))
        .exec(&store.connection)
        .await
        .unwrap();
    let before = occurrences::Entity::find()
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert!(
        occurrences::Entity::update_many()
            .col_expr(occurrences::Column::Status, Expr::val("cancelled"))
            .exec(&store.connection)
            .await
            .is_err()
    );
    assert_eq!(
        occurrences::Entity::find()
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn over_budget_poison_claims_and_healthy_rows_share_fair_quanta() {
    let store = test_store_with_workspace("ws_task").await;
    let task = task(&store, TaskExecutorKind::System).await;
    for n in 0..100 {
        let run = source_run(&store, &task, n, TaskRunStatus::Succeeded).await;
        authorities(&store, &task, &run, n).await;
    }
    store.connection.execute_unprepared("CREATE TRIGGER poison_contract_claim BEFORE UPDATE OF claim_token ON task_occurrence_reconcile_pending WHEN NEW.claim_token IS NOT NULL AND OLD.run_id < 'r00000000000000000070' BEGIN SELECT RAISE(ABORT,'poison claim'); END").await.unwrap();
    let mut selected = std::collections::HashSet::new();
    let mut changed = 0;
    for _ in 0..4 {
        let scope_inputs = store.expand_task_occurrence_scope(&|| NOW).await.unwrap();
        let seed_inputs = store
            .seed_unfinished_task_occurrences(&|| NOW)
            .await
            .unwrap();
        let candidates = store.discover_task_occurrence_reconcile(NOW).await.unwrap();
        assert!(scope_inputs + seed_inputs + candidates.len() as u64 <= 64);
        assert!(candidates.len() <= 32);
        for candidate in candidates {
            assert!(
                selected.insert(candidate.run_id.clone()),
                "no locator selected twice before its retry deadline"
            );
            match store
                .claim_task_occurrence_reconcile(&candidate, &|| NOW)
                .await
            {
                Ok(Some(claim)) => {
                    assert_eq!(
                        store
                            .reconcile_claimed_task_occurrence(&claim, NOW)
                            .await
                            .unwrap(),
                        Outcome::Changed
                    );
                    changed += 1;
                }
                Err(error) => assert_eq!(
                    error
                        .downcast::<TaskOccurrenceClaimFailure>()
                        .unwrap()
                        .deferral,
                    TaskOccurrenceClaimDeferral::Deferred
                ),
                Ok(None) => panic!("fixture has no competing holder"),
            }
        }
    }
    assert_eq!(selected.len(), 100);
    assert_eq!(changed, 30);
    assert!(
        store
            .discover_task_occurrence_reconcile(NOW)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store.has_pending_task_occurrence_reconcile().await.unwrap(),
        "delayed backlog is not recovery"
    );
    assert_eq!(
        store
            .discover_task_occurrence_reconcile(NOW + 5)
            .await
            .unwrap()
            .len(),
        32
    );
}

#[derive(FromQueryResult)]
struct Plan {
    detail: String,
}
#[tokio::test]
async fn due_scope_and_seed_plans_use_covering_keyset_indexes() {
    let store = test_store_with_workspace("ws_task").await;
    let scope = scopes::Model {
        task_id: "task".into(),
        generation: 1,
        after_run_id: Some("a".into()),
        upper_run_id: Some("z".into()),
    };
    let seed = seed::Model {
        singleton: 1,
        status_index: 0,
        after_run_id: Some("a".into()),
        upper_run_id: Some("z".into()),
    };
    for (query, index) in [
        (
            queue::due_query(NOW, 32),
            "idx_task_occurrence_reconcile_due",
        ),
        (queue::scope_page_query(&scope, 15), "idx_task_run_task_id"),
        (queue::seed_page_query(&seed, 15), "idx_task_run_status_id"),
    ] {
        let mut statement = store.connection.get_database_backend().build(&query);
        statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
        let plan = Plan::find_by_statement(statement)
            .all(&store.with_maintenance_access().connection)
            .await
            .unwrap();
        assert!(
            plan.iter()
                .any(|row| row.detail.contains(index) && row.detail.contains("SEARCH"))
        );
        assert!(
            plan.iter()
                .all(|row| !row.detail.contains("USE TEMP B-TREE")
                    && !row.detail.starts_with("SCAN task_run"))
        );
    }
    // Existing unique bindings remain the lookup indexes; don't duplicate them.
    let rows = store
        .connection
        .query_all_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "PRAGMA index_list('task_occurrence_contract')".to_owned(),
        ))
        .await
        .unwrap();
    assert!(
        rows.iter()
            .any(|row| row.try_get::<i64>("", "unique").unwrap() == 1)
    );
}

#[derive(Default)]
struct Routes {
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadClass>>,
    writes: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    queued: tokio::sync::Notify,
    watch: std::sync::atomic::AtomicBool,
}
impl pioneer_sqlite::SqliteReadObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        if let pioneer_sqlite::SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}
impl pioneer_sqlite::SqliteWriteObserver for Routes {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
        if self.watch.load(Ordering::SeqCst)
            && matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Enqueued {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                }
            )
        {
            self.queued.notify_one();
        }
    }
}
async fn disk_store(path: &std::path::Path, routes: Arc<Routes>) -> CrudStore {
    let mut options = sea_orm::ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| o.pragma("journal_mode", "WAL"));
    let writer = Database::connect(options).await.unwrap();
    let executor = pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, routes.clone());
    executor
        .run_migrations::<Migrator>(pioneer_sqlite::SqliteWriteClass::Maintenance, None)
        .await
        .unwrap();
    let mut options =
        sea_orm::ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(path));
    options
        .max_connections(1)
        .min_connections(1)
        .map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
    let reader = Database::connect(options).await.unwrap();
    CrudStore::new(
        pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(reader, executor, routes),
    )
}
async fn disk_fixture(store: &CrudStore) -> TaskRun {
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_task".into()),
        name: Set("Contracts".into()),
        is_active: Set(true),
        is_current: Set(true),
        created_at: Set(unix_to_datetime(AT)),
        updated_at: Set(unix_to_datetime(AT)),
    })
    .exec(&store.connection)
    .await
    .unwrap();
    let task = task(store, TaskExecutorKind::System).await;
    let run = source_run(store, &task, 0, TaskRunStatus::Succeeded).await;
    authorities(store, &task, &run, 0).await;
    run
}

#[tokio::test]
async fn physical_reader_routes_and_cancelled_claim_or_repair_release_capacity() {
    use pioneer_sqlite::{SqliteReadClass, SqliteWriteClass, SqliteWriteEvent};
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let routes = Arc::new(Routes::default());
    let store = disk_store(&directory.path().join("contracts.sqlite"), routes.clone()).await;
    let run = disk_fixture(&store).await;
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
    routes.reads.lock().unwrap().clear();
    routes.writes.lock().unwrap().clear();
    let candidate = store
        .discover_task_occurrence_reconcile(NOW)
        .await
        .unwrap()
        .remove(0);
    assert!(
        routes
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|c| *c == SqliteReadClass::Maintenance)
    );
    assert!(routes.writes.lock().unwrap().is_empty());
    let before = pending_row(&store, &run.id).await.unwrap();
    let hold = store.connection.begin().await.unwrap();
    routes.watch.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let c = candidate.clone();
    let waiting =
        tokio::spawn(async move { worker.claim_task_occurrence_reconcile(&c, &|| NOW).await });
    tokio::time::timeout(Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    assert!(
        store.get_task_run(&run.id).await.unwrap().is_some(),
        "interactive reader runs during queued background claim"
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    hold.rollback().await.unwrap();
    routes.watch.store(false, Ordering::SeqCst);
    assert_eq!(pending_row(&store, &run.id).await.unwrap(), before);
    assert!(routes.writes.lock().unwrap().iter().any(|event|matches!(event,
        SqliteWriteEvent::Cancelled{class:SqliteWriteClass::Maintenance,queue,..} if queue.maintenance==0)));
    let claimed = store
        .claim_task_occurrence_reconcile(&candidate, &|| NOW)
        .await
        .unwrap()
        .unwrap();
    let delayed = pending_row(&store, &run.id).await.unwrap();
    let hold = store.connection.begin().await.unwrap();
    routes.watch.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let c = claimed.clone();
    let waiting =
        tokio::spawn(async move { worker.reconcile_claimed_task_occurrence(&c, NOW).await });
    tokio::time::timeout(Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    hold.rollback().await.unwrap();
    routes.watch.store(false, Ordering::SeqCst);
    assert_eq!(pending_row(&store, &run.id).await.unwrap(), delayed);
    matches_diagnostic(&store, &run.id, true).await;
    routes.reads.lock().unwrap().clear();
    routes.writes.lock().unwrap().clear();
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&claimed, NOW)
            .await
            .unwrap(),
        Outcome::Changed
    );
    assert!(
        routes
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|c| *c == SqliteReadClass::Maintenance)
    );
    assert!(
        routes
            .writes
            .lock()
            .unwrap()
            .iter()
            .all(|event| !matches!(event,
        SqliteWriteEvent::Enqueued{class,..} if *class!=SqliteWriteClass::Maintenance))
    );
    assert!(store.connection.reader_query_only_enabled().await.unwrap());
}

#[tokio::test]
async fn claim_clock_is_read_after_admission_and_backoff_survives_reopen() {
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("contracts.sqlite");
    let routes = Arc::new(Routes::default());
    let store = disk_store(&path, routes.clone()).await;
    let run = disk_fixture(&store).await;
    let candidate = store
        .discover_task_occurrence_reconcile(NOW)
        .await
        .unwrap()
        .remove(0);
    let hold = store.connection.begin().await.unwrap();
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(NOW));
    routes.watch.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let now = clock.clone();
    let waiting = tokio::spawn(async move {
        worker
            .claim_task_occurrence_reconcile(&candidate, &|| now.load(Ordering::SeqCst))
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    clock.store(NOW + 100, Ordering::SeqCst);
    hold.rollback().await.unwrap();
    let claimed = waiting.await.unwrap().unwrap().unwrap();
    assert_eq!(claimed.next_attempt_at, NOW + 105);
    let row = pending_row(&store, &run.id).await.unwrap();
    drop(store);
    let reopened = disk_store(&path, Arc::new(Routes::default())).await;
    assert_eq!(pending_row(&reopened, &run.id).await.unwrap(), row);
    assert!(
        reopened
            .discover_task_occurrence_reconcile(NOW + 104)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        reopened
            .has_pending_task_occurrence_reconcile()
            .await
            .unwrap()
    );
    let candidate = reopened
        .discover_task_occurrence_reconcile(NOW + 105)
        .await
        .unwrap()
        .remove(0);
    let next = reopened
        .claim_task_occurrence_reconcile(&candidate, &|| NOW + 105)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(next.claim_token, claimed.claim_token);
    assert_eq!(next.attempt_count, 2);
    assert_eq!(next.next_attempt_at, NOW + 115);
    assert_eq!(
        reopened
            .reconcile_claimed_task_occurrence(&claimed, NOW + 105)
            .await
            .unwrap(),
        Outcome::StaleClaim
    );
}

#[tokio::test]
async fn m_true_repair_failure_retains_obligation_and_backoff_saturates() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    store.connection.execute_unprepared("CREATE TRIGGER refuse_contract_repair BEFORE UPDATE OF status ON task_occurrence_contract BEGIN SELECT RAISE(ABORT,'repair refused'); END").await.unwrap();
    let mut now = NOW;
    for attempt in 1..=18 {
        let claimed = claim(&store, &run.id, now).await;
        let before = pending_row(&store, &run.id).await.unwrap();
        assert!(
            store
                .reconcile_claimed_task_occurrence(&claimed, now)
                .await
                .is_err()
        );
        assert_eq!(pending_row(&store, &run.id).await.unwrap(), before);
        assert_eq!(before.attempt_count, attempt.min(16));
        assert_eq!(
            before.next_attempt_at - now,
            [5, 10, 20, 40, 80, 160, 300][(attempt as usize - 1).min(6)]
        );
        now = before.next_attempt_at;
    }
    assert!(store.has_pending_task_occurrence_reconcile().await.unwrap());
}

#[tokio::test]
async fn source_pk_rebinds_and_deletes_enqueue_old_and_new_locators() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    clear_bookkeeping(&store).await;
    let rebound = "rebound_run_key";
    sql(
        &store,
        "UPDATE task_run SET id=? WHERE id=?",
        vec![rebound.into(), run.id.clone().into()],
    )
    .await;
    assert!(pending_row(&store, &run.id).await.is_some());
    assert!(pending_row(&store, rebound).await.is_some());
    clear_bookkeeping(&store).await;
    sql(
        &store,
        "UPDATE task_occurrence_contract SET occurrence_id='new_occurrence_key' WHERE run_id=?",
        vec![run.id.clone().into()],
    )
    .await;
    assert!(pending_row(&store, &run.id).await.is_some());
    clear_bookkeeping(&store).await;
    occurrences::Entity::delete_by_id("new_occurrence_key".to_owned())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(pending_row(&store, &run.id).await.is_some());
    clear_bookkeeping(&store).await;
    sql(
        &store,
        "UPDATE task SET id='new_task_key' WHERE id=?",
        vec![run.task_id.clone().into()],
    )
    .await;
    assert!(
        scopes::Entity::find_by_id(run.task_id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        scopes::Entity::find_by_id("new_task_key".to_owned())
            .one(&store.connection)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .is_empty(),
        "Task rebind doesn't fan out in foreground"
    );
    scopes::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
    pioneer_entity::task::Entity::delete_by_id("new_task_key".to_owned())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(
        scopes::Entity::find_by_id("new_task_key".to_owned())
            .one(&store.connection)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn agent_old_new_keys_use_execution_pk_even_without_occurrence_rows() {
    let (store, _, original_id) = fixture(TaskExecutorKind::Agent).await;
    let task = task(&store, TaskExecutorKind::Agent).await;
    let a = source_run(&store, &task, 1, TaskRunStatus::Succeeded).await;
    let b = source_run(&store, &task, 2, TaskRunStatus::Succeeded).await;
    let a_id = "a00000000000000000000";
    let b_id = "b00000000000000000000";
    for (run, id) in [(&a, a_id), (&b, b_id)] {
        crate::repositories::task_run_execution::insert_execution_if_absent(
            &store.connection,
            crate::repositories::task_run_execution::NewTaskRunExecution {
                id: id.into(),
                task_id: task.id.clone(),
                task_run_id: run.id.clone(),
                executor_kind: TaskExecutorKind::Agent,
                status: TaskRunExecutionStatus::Succeeded,
                worker_id: None,
                lease_until: None,
                heartbeat_at: None,
                started_at: Some(AT),
                completed_at: Some(AT + 1),
                result: None,
                error: None,
                created_at: AT,
                updated_at: AT + 1,
            },
        )
        .await
        .unwrap();
    }
    let mut model = agents::Entity::find_by_id(original_id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap()
        .into_active_model();
    model.id = Set(a_id.into());
    agents::Entity::insert(model)
        .exec(&store.connection)
        .await
        .unwrap();
    clear_bookkeeping(&store).await;
    sql(
        &store,
        "UPDATE agent_execution SET id=? WHERE id=?",
        vec![b_id.into(), a_id.into()],
    )
    .await;
    assert!(pending_row(&store, &a.id).await.is_some());
    assert!(pending_row(&store, &b.id).await.is_some());
    clear_bookkeeping(&store).await;
    agents::Entity::delete_by_id(b_id.to_owned())
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(pending_row(&store, &b.id).await.is_some());
    assert!(pending_row(&store, &a.id).await.is_none());
}

#[tokio::test]
async fn scope_generation_refresh_while_waiting_for_writer_rejects_old_prefix() {
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let routes = Arc::new(Routes::default());
    let store = disk_store(&directory.path().join("scope.sqlite"), routes.clone()).await;
    let run = disk_fixture(&store).await;
    clear_bookkeeping(&store).await;
    sql(
        &store,
        "UPDATE task SET workspace_id='before' WHERE id=?",
        vec![run.task_id.clone().into()],
    )
    .await;
    let hold = store.connection.begin().await.unwrap();
    routes.watch.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let waiting = tokio::spawn(async move { worker.expand_task_occurrence_scope(&|| NOW).await });
    tokio::time::timeout(Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    hold.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE task SET workspace_id='newer' WHERE id=?",
        [run.task_id.clone().into()],
    ))
    .await
    .unwrap();
    let fresh = scopes::Entity::find_by_id(run.task_id.clone())
        .one(&hold)
        .await
        .unwrap()
        .unwrap();
    hold.commit().await.unwrap();
    assert_eq!(waiting.await.unwrap().unwrap(), 1);
    assert_eq!(
        scopes::Entity::find_by_id(run.task_id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap(),
        fresh
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    routes.watch.store(false, Ordering::SeqCst);
    assert_eq!(
        store.expand_task_occurrence_scope(&|| NOW).await.unwrap(),
        2
    );
    assert!(
        scopes::Entity::find_by_id(run.task_id)
            .one(&store.connection)
            .await
            .unwrap()
            .is_none()
    );
    assert!(pending_row(&store, &run.id).await.is_some());
}

#[tokio::test]
async fn metadata_repair_does_not_decode_unrelated_payloads() {
    let (store, run, id) = fixture(TaskExecutorKind::System).await;
    sql(&store,"UPDATE task SET metadata_json='invalid JSON',result_json='invalid JSON',error_json='invalid JSON' WHERE id=?",vec![run.task_id.clone().into()]).await;
    sql(
        &store,
        "UPDATE task_run SET result_json='invalid JSON',error_json='invalid JSON' WHERE id=?",
        vec![run.id.clone().into()],
    )
    .await;
    sql(&store,"UPDATE task_run_execution SET result_json='invalid JSON',error_json='invalid JSON' WHERE id=?",vec![id.into()]).await;
    sql(
        &store,
        "UPDATE task_occurrence_contract SET delivery_plan_json='invalid JSON' WHERE run_id=?",
        vec![run.id.clone().into()],
    )
    .await;
    let owned = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&owned, NOW)
            .await
            .unwrap(),
        Outcome::Changed
    );
    assert!(pending_row(&store, &run.id).await.is_none());
}

#[tokio::test]
async fn poison_scope_rotates_without_advancing_prefix_and_healthy_scope_progresses() {
    let store = test_store_with_workspace("ws_task").await;
    let a = task(&store, TaskExecutorKind::System).await;
    let mut b = a.clone();
    b.id = "task_other_scope".into();
    crate::repositories::task::upsert_prepared_task(
        &store.connection,
        crate::repositories::task::prepare_task_projection(&b).unwrap(),
    )
    .await
    .unwrap();
    for n in 0..40 {
        source_run(&store, &a, n, TaskRunStatus::Succeeded).await;
    }
    for n in 100..105 {
        source_run(&store, &b, n, TaskRunStatus::Succeeded).await;
    }
    clear_bookkeeping(&store).await;
    for task in [&a, &b] {
        sql(
            &store,
            "UPDATE task SET executor_kind='agent' WHERE id=?",
            vec![task.id.clone().into()],
        )
        .await;
    }
    let before = scopes::Entity::find_by_id(a.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    store.connection.execute_unprepared("CREATE TRIGGER refuse_scope_enqueue BEFORE INSERT ON task_occurrence_reconcile_pending WHEN NEW.run_id='r00000000000000000000' BEGIN SELECT RAISE(ABORT,'poison scope'); END").await.unwrap();
    assert!(store.expand_task_occurrence_scope(&|| NOW).await.is_err());
    let failed = scopes::Entity::find_by_id(a.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.after_run_id, before.after_run_id);
    assert_eq!(failed.upper_run_id, before.upper_run_id);
    assert!(failed.generation > before.generation);
    assert!(
        pending::Entity::find()
            .all(&store.connection)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.expand_task_occurrence_scope(&|| NOW).await.unwrap(),
        6
    );
    assert!(
        scopes::Entity::find_by_id(b.id)
            .one(&store.connection)
            .await
            .unwrap()
            .is_none()
    );
    for n in 100..105 {
        assert!(pending_row(&store, &format!("r{n:020}")).await.is_some());
    }
    store
        .connection
        .execute_unprepared("DROP TRIGGER refuse_scope_enqueue")
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(store.expand_task_occurrence_scope(&|| NOW).await.unwrap() <= 16);
    }
    assert!(
        scopes::Entity::find_by_id(a.id)
            .one(&store.connection)
            .await
            .unwrap()
            .is_none()
    );
}
