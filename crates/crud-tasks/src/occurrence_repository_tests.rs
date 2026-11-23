//! Task occurrence reconciliation and durable execution facts on PostgreSQL.
use super::TaskOccurrenceTerminalRepairOutcome as Outcome;
use super::*;
use crate::task_event_context::TaskEventContextRepository;
use crate::task_occurrence_producers as producer;
use crate::test_support::task_projection::{sample_task, sample_task_run};
use pioneer_crud_execution::agent_execution::{AgentExecutionInput, AgentResourceStateInput};
use pioneer_entity::{
    agent_execution as agents, task as tasks, task_occurrence_contract as occurrences,
    task_occurrence_reconcile_pending as pending, task_occurrence_reconcile_scope as scopes,
    task_occurrence_reconcile_sequence as sequence, task_run as runs,
    task_run_execution as executions,
};
use pioneer_protocol::*;
use sea_orm::sea_query::Expr;
use sea_orm::{ActiveModelTrait, IntoActiveModel};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DbBackend, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, Set, Statement,
};
use sha2::{Digest, Sha256};
use std::sync::{Arc, atomic::Ordering};

const NOW: i64 = 4_000_000_000;
const AT: i64 = 1_700_000_000;
async fn contract_store(workspace: &str) -> TaskOccurrenceRepository {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let writer = fixture.connection().clone();
    writer.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Postgres,
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ($1,'Task occurrence',TRUE,FALSE)", [workspace.into()])).await.unwrap();
    TaskOccurrenceRepository::new(PostgresDatabase::new(reader, writer))
}

async fn task(store: &TaskOccurrenceRepository, kind: TaskExecutorKind) -> Task {
    let mut task = sample_task(AT);
    task.executor_kind = kind;
    let prepared = crate::task::prepare_task_projection(&task).unwrap();
    let tx = store.connection.begin().await.unwrap();
    crate::task::upsert_prepared_task(&tx, prepared)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    task
}

async fn source_run(
    store: &TaskOccurrenceRepository,
    task: &Task,
    n: u64,
    status: TaskRunStatus,
) -> TaskRun {
    let mut run = sample_task_run(AT);
    run.id = format!("r{n:020}");
    run.run_group_id = run.id.clone();
    run.task_id = task.id.clone();
    run.run_number = (n + 1).try_into().unwrap();
    run.executor_kind = task.executor_kind;
    run.trigger_id = None;
    run.status = status;
    run.completed_at = status.is_terminal().then_some(AT + 1);
    let tx = store.connection.begin().await.unwrap();
    crate::task_run::upsert_run(&tx, &run).await.unwrap();
    tx.commit().await.unwrap();
    run
}

async fn authorities(
    store: &TaskOccurrenceRepository,
    task: &Task,
    run: &TaskRun,
    n: u64,
) -> String {
    let id = format!("e{n:020}");
    let tx = store.connection.begin().await.unwrap();
    crate::task_run_execution::insert_execution_if_absent(
        &tx,
        crate::task_run_execution::NewTaskRunExecution {
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
    tx.commit().await.unwrap();
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

async fn fixture(kind: TaskExecutorKind) -> (TaskOccurrenceRepository, TaskRun, String) {
    let store = contract_store("ws_task").await;
    let task = task(&store, kind).await;
    let run = source_run(&store, &task, 0, TaskRunStatus::Succeeded).await;
    let id = authorities(&store, &task, &run, 0).await;
    (store, run, id)
}

async fn pending_row(store: &TaskOccurrenceRepository, id: &str) -> Option<pending::Model> {
    pending::Entity::find_by_id(id.to_owned())
        .one(&store.connection)
        .await
        .unwrap()
}
async fn claim(
    store: &TaskOccurrenceRepository,
    id: &str,
    now: i64,
) -> TaskOccurrenceReconcileClaim {
    let row = pending_row(store, id).await.unwrap();
    store
        .claim_task_occurrence_reconcile(
            &TaskOccurrenceReconcileCandidate {
                run_id: id.to_owned(),
                generation: row.generation,
            },
            &|| now,
        )
        .await
        .unwrap()
        .unwrap()
}
async fn clear_bookkeeping(store: &TaskOccurrenceRepository) {
    pending::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
    scopes::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
}
async fn update_occurrence_status(
    store: &TaskOccurrenceRepository,
    id: &str,
    status: &str,
) -> anyhow::Result<()> {
    let tx = store.connection.begin().await?;
    let before = occurrences::Entity::find()
        .filter(occurrences::Column::RunId.eq(id))
        .lock_exclusive()
        .one(&tx)
        .await?
        .unwrap();
    occurrences::Entity::update_many()
        .col_expr(occurrences::Column::Status, Expr::val(status))
        .filter(occurrences::Column::RunId.eq(id))
        .exec(&tx)
        .await?;
    let after = occurrences::Entity::find_by_id(&before.occurrence_id)
        .one(&tx)
        .await?;
    let publication = producer::contract_change(&tx, Some(&before), after.as_ref()).await;
    if publication.is_err() {
        tx.rollback().await?;
        return publication;
    }
    tx.commit().await?;
    Ok(())
}

async fn occurrence_status(store: &TaskOccurrenceRepository, id: &str, status: &str) {
    update_occurrence_status(store, id, status).await.unwrap();
}

async fn mutate_task(
    tx: &sea_orm::DatabaseTransaction,
    id: &str,
    new_id: Option<&str>,
    statement: &str,
    values: Vec<sea_orm::Value>,
) {
    let before = tasks::Entity::find_by_id(id)
        .lock_exclusive()
        .one(tx)
        .await
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        statement,
        values,
    ))
    .await
    .unwrap();
    let after = match new_id {
        Some(id) => tasks::Entity::find_by_id(id).one(tx).await.unwrap(),
        None => None,
    };
    producer::task_change(tx, before.as_ref(), after.as_ref())
        .await
        .unwrap();
}

async fn update_task_scope(store: &TaskOccurrenceRepository, id: &str, statement: &str) {
    let tx = store.connection.begin().await.unwrap();
    mutate_task(&tx, id, Some(id), statement, vec![id.into()]).await;
    tx.commit().await.unwrap();
}
async fn sql(store: &TaskOccurrenceRepository, text: &str, values: Vec<sea_orm::Value>) {
    store
        .connection
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            text,
            values,
        ))
        .await
        .unwrap();
}
async fn matches_diagnostic(store: &TaskOccurrenceRepository, run_id: &str, expected: bool) {
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
        "metadata must preserve the five-authority mismatch predicate"
    );
}

#[pioneer_postgres_test::postgres_test]
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
            "UPDATE task_run SET status=$1 WHERE id=$2",
            vec![status.into(), run.id.clone().into()],
        )
        .await;
        for execution_status in [status, "different"] {
            sql(
                &store,
                "UPDATE task_run_execution SET status=$1 WHERE task_run_id=$2",
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
        "UPDATE task_run SET status='succeeded' WHERE id=$1",
        vec![run.id.clone().into()],
    )
    .await;
    sql(
        &store,
        "UPDATE task_run_execution SET status='succeeded' WHERE task_run_id=$1",
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
        sql(&store,"UPDATE task_occurrence_contract SET agent_execution_id=$1,work_graph_root_execution_id=$2,root_resource_scope_id=$3 WHERE run_id=$4",
            vec![a.map(String::from).into(),b.map(String::from).into(),c.map(String::from).into(),run.id.clone().into()]).await;
        matches_diagnostic(&store, &run.id, a.is_none() && b.is_none() && c.is_none()).await;
    }
    sql(&store,"UPDATE task_occurrence_contract SET agent_execution_id=NULL,work_graph_root_execution_id=NULL,root_resource_scope_id=NULL WHERE run_id=$1",vec![run.id.clone().into()]).await;
    for (table, key) in [("task_run", "id"), ("task_run_execution", "task_run_id")] {
        sql(
            &store,
            &format!("UPDATE {table} SET completed_at=NULL WHERE {key}=$1"),
            vec![run.id.clone().into()],
        )
        .await;
        matches_diagnostic(&store, &run.id, false).await;
        sql(
            &store,
            &format!("UPDATE {table} SET completed_at=$1 WHERE {key}=$2"),
            vec![unix_to_datetime(AT + 1).into(), run.id.clone().into()],
        )
        .await;
    }
    for kind in ["agent", "system", "unknown"] {
        sql(
            &store,
            "UPDATE task SET executor_kind=$1 WHERE id=$2",
            vec![kind.into(), run.task_id.clone().into()],
        )
        .await;
        matches_diagnostic(&store, &run.id, kind == "system").await;
    }
}

#[pioneer_postgres_test::postgres_test]
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
            &format!("UPDATE agent_execution SET {mutation} WHERE id=$1"),
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
            &format!("UPDATE task_occurrence_contract SET {mutation} WHERE run_id=$1"),
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
    update_task_scope(
        &store,
        &run.task_id,
        "UPDATE task SET workspace_id='rebound' WHERE id=$1",
    )
    .await;
    matches_diagnostic(&store, &run.id, false).await;
    sql(
        &store,
        "UPDATE task SET workspace_id='ws_task',executor_kind='system' WHERE id=$1",
        vec![run.task_id.clone().into()],
    )
    .await;
    matches_diagnostic(&store, &run.id, false).await;
}

#[pioneer_postgres_test::postgres_test]
async fn status_repair_and_post_trigger_ack_are_one_commit_and_rollback_together() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    let claimed = claim(&store, &run.id, NOW).await;
    let before = pending_row(&store, &run.id).await.unwrap();
    store.connection.execute_unprepared("CREATE FUNCTION reject_contract_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'ack refusal'; END; $$; CREATE TRIGGER reject_contract_ack BEFORE DELETE ON task_occurrence_reconcile_pending FOR EACH ROW EXECUTE FUNCTION reject_contract_ack()").await.unwrap();
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
        .execute_unprepared("DROP TRIGGER reject_contract_ack ON task_occurrence_reconcile_pending")
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

#[pioneer_postgres_test::postgres_test]
async fn preparation_mutation_and_task_scope_rebinding_fence_stale_repair() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    let claimed = claim(&store, &run.id, NOW).await;
    let facts = contracts::load_terminal_occurrence_metadata(&store.connection, &run.id)
        .await
        .unwrap();
    update_task_scope(
        &store,
        &run.task_id,
        "UPDATE task SET executor_kind='agent' WHERE id=$1",
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
    update_task_scope(
        &store,
        &run.task_id,
        "UPDATE task SET executor_kind='system' WHERE id=$1",
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

#[pioneer_postgres_test::postgres_test]
async fn missing_late_authorities_and_same_id_aba_are_covered() {
    let store = contract_store("ws_task").await;
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
    let tx = store.connection.begin().await.unwrap();
    executions::Entity::delete_by_id(id)
        .exec(&tx)
        .await
        .unwrap();
    producer::execution_change(&tx, Some(&execution), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let deleted = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&deleted, NOW)
            .await
            .unwrap(),
        Outcome::NotRepairable
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    let tx = store.connection.begin().await.unwrap();
    let execution = executions::Entity::insert(execution.into_active_model())
        .exec_with_returning(&tx)
        .await
        .unwrap();
    producer::execution_change(&tx, None, Some(&execution))
        .await
        .unwrap();
    tx.commit().await.unwrap();
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

#[pioneer_postgres_test::postgres_test]
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
    let tx = store.connection.begin().await.unwrap();
    let task = tasks::Entity::insert(task.into_active_model())
        .exec_with_returning(&tx)
        .await
        .unwrap();
    producer::task_change(&tx, None, Some(&task)).await.unwrap();
    tx.commit().await.unwrap();
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
    let tx = store.connection.begin().await.unwrap();
    occurrences::Entity::delete_by_id(run.id.clone())
        .exec(&tx)
        .await
        .unwrap();
    producer::contract_change(&tx, Some(&occurrence), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
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
    let tx = store.connection.begin().await.unwrap();
    let occurrence = occurrences::Entity::insert(occurrence)
        .exec_with_returning(&tx)
        .await
        .unwrap();
    producer::contract_change(&tx, None, Some(&occurrence))
        .await
        .unwrap();
    tx.commit().await.unwrap();
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
    let tx = store.connection.begin().await.unwrap();
    runs::Entity::delete_by_id(run.id.clone())
        .exec(&tx)
        .await
        .unwrap();
    producer::run_change(&tx, Some(&source), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let owned = claim(&store, &run.id, NOW).await;
    assert_eq!(
        store
            .reconcile_claimed_task_occurrence(&owned, NOW)
            .await
            .unwrap(),
        Outcome::NotFound
    );
    assert!(pending_row(&store, &run.id).await.is_none());
    let tx = store.connection.begin().await.unwrap();
    let source = runs::Entity::insert(source.into_active_model())
        .exec_with_returning(&tx)
        .await
        .unwrap();
    producer::run_change(&tx, None, Some(&source))
        .await
        .unwrap();
    tx.commit().await.unwrap();
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

#[pioneer_postgres_test::postgres_test]
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
        "UPDATE task_run_execution SET id=$1 WHERE task_run_id=$2",
        vec![late.into(), run.id.clone().into()],
    )
    .await;
    sql(&store,"UPDATE task_occurrence_contract SET agent_execution_id=$1,work_graph_root_execution_id=$2,root_resource_scope_id=$3 WHERE run_id=$4",
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
    let tx = store.connection.begin().await.unwrap();
    let new = agents::Entity::insert(new)
        .exec_with_returning(&tx)
        .await
        .unwrap();
    producer::agent_change(&tx, None, Some(&new)).await.unwrap();
    tx.commit().await.unwrap();
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

#[pioneer_postgres_test::postgres_test]
async fn heartbeat_lease_payload_progress_and_identical_upserts_do_not_refresh_dirty() {
    let (store, run, id) = fixture(TaskExecutorKind::System).await;
    let before = pending_row(&store, &run.id).await.unwrap();
    let scope_before = scopes::Entity::find_by_id(run.task_id.clone())
        .one(&store.connection)
        .await
        .unwrap();
    sql(&store,"UPDATE task_run SET heartbeat_at=CURRENT_TIMESTAMP,lock_expires_at=CURRENT_TIMESTAMP,locked_by='worker',result_json='{}',error_json='{}',updated_at=CURRENT_TIMESTAMP WHERE id=$1",vec![run.id.clone().into()]).await;
    sql(&store,"UPDATE task_run_execution SET heartbeat_at=CURRENT_TIMESTAMP,lease_until=CURRENT_TIMESTAMP,worker_id='worker',result_json='{}',error_json='{}',updated_at=CURRENT_TIMESTAMP WHERE id=$1",vec![id.into()]).await;
    sql(&store,"UPDATE task_occurrence_contract SET queue_position=3,terminal_reason='progress',delivery_plan_json='{}',status=status WHERE run_id=$1",vec![run.id.clone().into()]).await;
    sql(&store,"UPDATE task SET title='progress',revision=revision+1,result_json='{}',executor_kind=executor_kind,workspace_id=workspace_id WHERE id=$1",vec![run.task_id.clone().into()]).await;
    let tx = store.connection.begin().await.unwrap();
    crate::task_run::upsert_run(&tx, &run).await.unwrap();
    tx.commit().await.unwrap();
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
        "UPDATE agent_execution SET status='running',finished_at=NULL WHERE id=$1",
        vec![agent_id.clone().into()],
    )
    .await;
    sql(
        &agent_store,
        "UPDATE task_run_execution SET status='running',completed_at=NULL WHERE id=$1",
        vec![agent_id.clone().into()],
    )
    .await;
    sql(
        &agent_store,
        "UPDATE task_run SET status='running',completed_at=NULL WHERE id=$1",
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
        crate::execution_store::TaskExecutionStore::new(&agent_store.connection)
            .heartbeat_execution_for_agent_attempt(&agent_id, 1, AT + 20, None)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        pioneer_crud_execution::turn_ownership::TurnOwnership::new(
            &agent_store.connection,
            &Default::default(),
        )
        .record_agent_execution_progress(&agent_id, 1, "{}", AT + 21, None)
        .await
        .unwrap()
    );
    sql(
        &agent_store,
        "UPDATE agent_execution SET updated_at=CURRENT_TIMESTAMP,status=status WHERE id=$1",
        vec![agent_id.into()],
    )
    .await;
    assert_eq!(
        pending_row(&agent_store, &agent_run.id).await.unwrap(),
        before
    );
}

#[pioneer_postgres_test::postgres_test]
async fn scope_fanout_is_bounded_fenced_and_covers_new_ids_behind_cursor() {
    let store = contract_store("ws_task").await;
    let task = task(&store, TaskExecutorKind::System).await;
    for n in 0..100 {
        source_run(&store, &task, n, TaskRunStatus::Succeeded).await;
    }
    clear_bookkeeping(&store).await;
    update_task_scope(
        &store,
        &task.id,
        "UPDATE task SET executor_kind='agent' WHERE id=$1",
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
    let tx = store.connection.begin().await.unwrap();
    crate::task_run::upsert_run(&tx, &behind).await.unwrap();
    tx.commit().await.unwrap();
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
    update_task_scope(
        &store,
        &task.id,
        "UPDATE task SET workspace_id='rebound' WHERE id=$1",
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

// This migration fixture predates webhook payloads. Insert only historical
// columns and avoid RETURNING the current entity's newer columns.
#[pioneer_postgres_test::postgres_test]
async fn failed_claim_deferral_fences_null_token_due_count_generation_and_stale_error() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    store.connection.execute_unprepared("CREATE FUNCTION refuse_contract_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'claim refusal'; END; $$; CREATE TRIGGER refuse_contract_claim BEFORE UPDATE OF claim_token ON task_occurrence_reconcile_pending FOR EACH ROW WHEN (NEW.claim_token IS NOT NULL) EXECUTE FUNCTION refuse_contract_claim()").await.unwrap();
    let before = pending_row(&store, &run.id).await.unwrap();
    let candidate = TaskOccurrenceReconcileCandidate {
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
        .execute_unprepared(
            "DROP TRIGGER refuse_contract_claim ON task_occurrence_reconcile_pending",
        )
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

#[pioneer_postgres_test::postgres_test]
async fn resume_retry_rebind_delete_and_monotonic_sequence_overflow() {
    let (store, run, id) = fixture(TaskExecutorKind::System).await;
    let owned = claim(&store, &run.id, NOW).await;
    let tx = store.connection.begin().await.unwrap();
    let before = runs::Entity::find_by_id(&run.id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE task_run SET status='queued',completed_at=NULL WHERE id=$1",
        [run.id.clone().into()],
    ))
    .await
    .unwrap();
    let after = runs::Entity::find_by_id(&run.id)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    producer::run_change(&tx, Some(&before), Some(&after))
        .await
        .unwrap();
    tx.commit().await.unwrap();
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
    let tx = store.connection.begin().await.unwrap();
    let before = executions::Entity::find_by_id(&id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE task_run_execution SET task_run_id=$1 WHERE id=$2",
        [new.into(), id.clone().into()],
    ))
    .await
    .unwrap();
    let after = executions::Entity::find_by_id(&id)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    producer::execution_change(&tx, Some(&before), Some(&after))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(pending_row(&store, &run.id).await.is_some());
    assert!(pending_row(&store, new).await.is_some());
    pending::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
    let tx = store.connection.begin().await.unwrap();
    let before = occurrences::Entity::find_by_id(&run.id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE task_occurrence_contract SET run_id=$1 WHERE run_id=$2",
        [new.into(), run.id.clone().into()],
    ))
    .await
    .unwrap();
    let after = occurrences::Entity::find_by_id(&run.id)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    producer::contract_change(&tx, Some(&before), Some(&after))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(pending_row(&store, &run.id).await.is_some());
    assert!(pending_row(&store, new).await.is_some());
    pending::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
    let tx = store.connection.begin().await.unwrap();
    let before_run = runs::Entity::find_by_id(&run.id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    runs::Entity::delete_by_id(run.id.clone())
        .exec(&tx)
        .await
        .unwrap();
    producer::run_change(&tx, Some(&before_run), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
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
        update_occurrence_status(&store, new, "cancelled")
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

#[pioneer_postgres_test::postgres_test]
async fn over_budget_poison_claims_and_healthy_rows_share_fair_quanta() {
    let store = contract_store("ws_task").await;
    let task = task(&store, TaskExecutorKind::System).await;
    for n in 0..100 {
        let run = source_run(&store, &task, n, TaskRunStatus::Succeeded).await;
        authorities(&store, &task, &run, n).await;
    }
    store.connection.execute_unprepared("CREATE FUNCTION poison_contract_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'poison claim'; END; $$; CREATE TRIGGER poison_contract_claim BEFORE UPDATE OF claim_token ON task_occurrence_reconcile_pending FOR EACH ROW WHEN (NEW.claim_token IS NOT NULL AND OLD.run_id < 'r00000000000000000070') EXECUTE FUNCTION poison_contract_claim()").await.unwrap();
    let mut selected = std::collections::HashSet::new();
    let mut changed = 0;
    for _ in 0..4 {
        let scope_inputs = store.expand_task_occurrence_scope(&|| NOW).await.unwrap();
        let candidates = store.discover_task_occurrence_reconcile(NOW).await.unwrap();
        assert!(scope_inputs + candidates.len() as u64 <= 64);
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

#[derive(Default)]
struct Routes {
    reads: std::sync::Mutex<Vec<pioneer_postgres_pools::PostgresReadClass>>,
    writes: std::sync::Mutex<Vec<pioneer_postgres_pools::PostgresWriteEvent>>,
    queued: tokio::sync::Notify,
    watch: std::sync::atomic::AtomicBool,
}
impl pioneer_postgres_pools::PostgresReadObserver for Routes {
    fn observe(&self, event: pioneer_postgres_pools::PostgresReadEvent) {
        if let pioneer_postgres_pools::PostgresReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}
impl pioneer_postgres_pools::PostgresWriteObserver for Routes {
    fn observe(&self, event: pioneer_postgres_pools::PostgresWriteEvent) {
        self.writes.lock().unwrap().push(event);
        if self.watch.load(Ordering::SeqCst)
            && matches!(
                event,
                pioneer_postgres_pools::PostgresWriteEvent::Enqueued {
                    class: pioneer_postgres_pools::PostgresWriteClass::Maintenance,
                    ..
                }
            )
        {
            self.queued.notify_one();
        }
    }
}
async fn observed_store(
    fixture: &pioneer_postgres_test::TestDatabase,
    routes: Arc<Routes>,
) -> TaskOccurrenceRepository {
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture
        .connect_with_options(|options| {
            options
                .max_connections(1)
                .map_sqlx_postgres_opts(|options| {
                    options.options([("default_transaction_read_only", "on")])
                });
        })
        .await
        .unwrap();
    let executor =
        pioneer_postgres_pools::PostgresWriteExecutor::with_observer(writer, routes.clone());
    TaskOccurrenceRepository::new(
        pioneer_postgres_pools::PostgresDatabase::from_executor_with_read_observer(
            reader, executor, routes,
        ),
    )
}
async fn disk_fixture(store: &TaskOccurrenceRepository) -> TaskRun {
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set("ws_task".into()),
        name: Set("Contracts".into()),
        is_active: Set(true),
        is_current: Set(false),
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

#[pioneer_postgres_test::postgres_test]
async fn physical_reader_routes_and_cancelled_claim_or_repair_release_capacity() {
    use pioneer_postgres_pools::{PostgresReadClass, PostgresWriteClass, PostgresWriteEvent};
    use std::time::Duration;
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let routes = Arc::new(Routes::default());
    let store = observed_store(&fixture, routes.clone()).await;
    let run = disk_fixture(&store).await;
    assert!(store.connection.reader_is_read_only().await.unwrap());
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
            .all(|c| *c == PostgresReadClass::Maintenance)
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
        store
            .event_context()
            .get_task_run(&run.id)
            .await
            .unwrap()
            .is_some(),
        "interactive reader runs during queued background claim"
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    hold.rollback().await.unwrap();
    routes.watch.store(false, Ordering::SeqCst);
    assert_eq!(pending_row(&store, &run.id).await.unwrap(), before);
    assert!(routes.writes.lock().unwrap().iter().any(|event|matches!(event,
        PostgresWriteEvent::Cancelled{class:PostgresWriteClass::Maintenance,queue,..} if queue.maintenance==0)));
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
            .all(|c| *c == PostgresReadClass::Maintenance)
    );
    assert!(
        routes
            .writes
            .lock()
            .unwrap()
            .iter()
            .all(|event| !matches!(event,
        PostgresWriteEvent::Enqueued{class,..} if *class!=PostgresWriteClass::Maintenance))
    );
    assert!(store.connection.reader_is_read_only().await.unwrap());
}

#[pioneer_postgres_test::postgres_test]
async fn claim_clock_is_read_after_admission_and_backoff_survives_reopen() {
    use std::time::Duration;
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let routes = Arc::new(Routes::default());
    let store = observed_store(&fixture, routes.clone()).await;
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
    let reopened = observed_store(&fixture, Arc::new(Routes::default())).await;
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

#[pioneer_postgres_test::postgres_test]
async fn m_true_repair_failure_retains_obligation_and_backoff_saturates() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    store.connection.execute_unprepared("CREATE FUNCTION refuse_contract_repair() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'repair refused'; END; $$; CREATE TRIGGER refuse_contract_repair BEFORE UPDATE OF status ON task_occurrence_contract FOR EACH ROW EXECUTE FUNCTION refuse_contract_repair()").await.unwrap();
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

#[pioneer_postgres_test::postgres_test]
async fn source_pk_rebinds_and_deletes_enqueue_old_and_new_locators() {
    let (store, run, _) = fixture(TaskExecutorKind::System).await;
    clear_bookkeeping(&store).await;
    let rebound = "rebound_run_key";
    let tx = store.connection.begin().await.unwrap();
    let before = runs::Entity::find_by_id(&run.id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE task_run SET id=$1 WHERE id=$2",
        [rebound.into(), run.id.clone().into()],
    ))
    .await
    .unwrap();
    let after = runs::Entity::find_by_id(rebound)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    producer::run_change(&tx, Some(&before), Some(&after))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(pending_row(&store, &run.id).await.is_some());
    assert!(pending_row(&store, rebound).await.is_some());
    clear_bookkeeping(&store).await;
    let tx = store.connection.begin().await.unwrap();
    let before = occurrences::Entity::find_by_id(&run.id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE task_occurrence_contract SET occurrence_id='new_occurrence_key' WHERE run_id=$1",
        [run.id.clone().into()],
    ))
    .await
    .unwrap();
    let after = occurrences::Entity::find_by_id("new_occurrence_key")
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    producer::contract_change(&tx, Some(&before), Some(&after))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(pending_row(&store, &run.id).await.is_some());
    clear_bookkeeping(&store).await;
    let tx = store.connection.begin().await.unwrap();
    occurrences::Entity::delete_by_id("new_occurrence_key")
        .exec(&tx)
        .await
        .unwrap();
    producer::contract_change(&tx, Some(&after), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(pending_row(&store, &run.id).await.is_some());
    clear_bookkeeping(&store).await;
    let tx = store.connection.begin().await.unwrap();
    mutate_task(
        &tx,
        &run.task_id,
        Some("new_task_key"),
        "UPDATE task SET id='new_task_key' WHERE id=$1",
        vec![run.task_id.clone().into()],
    )
    .await;
    tx.commit().await.unwrap();
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
    let tx = store.connection.begin().await.unwrap();
    mutate_task(
        &tx,
        "new_task_key",
        None,
        "DELETE FROM task WHERE id=$1",
        vec!["new_task_key".into()],
    )
    .await;
    tx.commit().await.unwrap();
    assert!(
        scopes::Entity::find_by_id("new_task_key".to_owned())
            .one(&store.connection)
            .await
            .unwrap()
            .is_some()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn agent_old_new_keys_use_execution_pk_even_without_occurrence_rows() {
    let (store, _, original_id) = fixture(TaskExecutorKind::Agent).await;
    let task = task(&store, TaskExecutorKind::Agent).await;
    let a = source_run(&store, &task, 1, TaskRunStatus::Succeeded).await;
    let b = source_run(&store, &task, 2, TaskRunStatus::Succeeded).await;
    let a_id = "a00000000000000000000";
    let b_id = "b00000000000000000000";
    for (run, id) in [(&a, a_id), (&b, b_id)] {
        let tx = store.connection.begin().await.unwrap();
        crate::task_run_execution::insert_execution_if_absent(
            &tx,
            crate::task_run_execution::NewTaskRunExecution {
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
        tx.commit().await.unwrap();
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
    let tx = store.connection.begin().await.unwrap();
    let before = agents::Entity::find_by_id(a_id)
        .lock_exclusive()
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    tx.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Postgres,
        "UPDATE agent_execution SET id=$1 WHERE id=$2",
        [b_id.into(), a_id.into()],
    ))
    .await
    .unwrap();
    let after = agents::Entity::find_by_id(b_id)
        .one(&tx)
        .await
        .unwrap()
        .unwrap();
    producer::agent_change(&tx, Some(&before), Some(&after))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(pending_row(&store, &a.id).await.is_some());
    assert!(pending_row(&store, &b.id).await.is_some());
    clear_bookkeeping(&store).await;
    let tx = store.connection.begin().await.unwrap();
    agents::Entity::delete_by_id(b_id.to_owned())
        .exec(&tx)
        .await
        .unwrap();
    producer::agent_change(&tx, Some(&after), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(pending_row(&store, &b.id).await.is_some());
    assert!(pending_row(&store, &a.id).await.is_none());
}

#[pioneer_postgres_test::postgres_test]
async fn scope_generation_refresh_while_waiting_for_writer_rejects_old_prefix() {
    use std::time::Duration;
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let routes = Arc::new(Routes::default());
    let store = observed_store(&fixture, routes.clone()).await;
    let run = disk_fixture(&store).await;
    clear_bookkeeping(&store).await;
    update_task_scope(
        &store,
        &run.task_id,
        "UPDATE task SET workspace_id='before' WHERE id=$1",
    )
    .await;
    let hold = store.connection.begin().await.unwrap();
    routes.watch.store(true, Ordering::SeqCst);
    let worker = store.clone();
    let waiting = tokio::spawn(async move { worker.expand_task_occurrence_scope(&|| NOW).await });
    tokio::time::timeout(Duration::from_secs(5), routes.queued.notified())
        .await
        .unwrap();
    mutate_task(
        &hold,
        &run.task_id,
        Some(&run.task_id),
        "UPDATE task SET workspace_id='newer' WHERE id=$1",
        vec![run.task_id.clone().into()],
    )
    .await;
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

#[pioneer_postgres_test::postgres_test]
async fn metadata_repair_does_not_decode_unrelated_payloads() {
    let (store, run, id) = fixture(TaskExecutorKind::System).await;
    sql(&store,"UPDATE task SET metadata_json='\"invalid JSON\"'::json,result_json='\"invalid JSON\"',error_json='\"invalid JSON\"' WHERE id=$1",vec![run.task_id.clone().into()]).await;
    sql(
        &store,
        "UPDATE task_run SET result_json='\"invalid JSON\"',error_json='\"invalid JSON\"' WHERE id=$1",
        vec![run.id.clone().into()],
    )
    .await;
    sql(&store,"UPDATE task_run_execution SET result_json='\"invalid JSON\"',error_json='\"invalid JSON\"' WHERE id=$1",vec![id.into()]).await;
    sql(
        &store,
        "UPDATE task_occurrence_contract SET delivery_plan_json='\"invalid JSON\"' WHERE run_id=$1",
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

#[pioneer_postgres_test::postgres_test]
async fn poison_scope_rotates_without_advancing_prefix_and_healthy_scope_progresses() {
    let store = contract_store("ws_task").await;
    let a = task(&store, TaskExecutorKind::System).await;
    let mut b = a.clone();
    b.id = "task_other_scope".into();
    let tx = store.connection.begin().await.unwrap();
    crate::task::upsert_prepared_task(&tx, crate::task::prepare_task_projection(&b).unwrap())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for n in 0..40 {
        source_run(&store, &a, n, TaskRunStatus::Succeeded).await;
    }
    for n in 100..105 {
        source_run(&store, &b, n, TaskRunStatus::Succeeded).await;
    }
    clear_bookkeeping(&store).await;
    for task in [&a, &b] {
        update_task_scope(
            &store,
            &task.id,
            "UPDATE task SET executor_kind='agent' WHERE id=$1",
        )
        .await;
    }
    let before = scopes::Entity::find_by_id(a.id.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    store.connection.execute_unprepared("CREATE FUNCTION refuse_scope_enqueue() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'poison scope'; END; $$; CREATE TRIGGER refuse_scope_enqueue BEFORE INSERT ON task_occurrence_reconcile_pending FOR EACH ROW WHEN (NEW.run_id='r00000000000000000000') EXECUTE FUNCTION refuse_scope_enqueue()").await.unwrap();
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
        .execute_unprepared(
            "DROP TRIGGER refuse_scope_enqueue ON task_occurrence_reconcile_pending",
        )
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

async fn ensure_test_agent_identity(
    store: &TaskOccurrenceRepository,
    workspace_id: &str,
    timestamp: i64,
) {
    let now = unix_to_datetime(timestamp);
    pioneer_crud_execution::agent_identity_catalog::ensure_pioneer_for_workspace(
        &store.connection,
        workspace_id,
        now.clone(),
    )
    .await
    .expect("agent domain workspace identity should seed");
}
async fn attach_test_agent_execution_contract(
    store: &TaskOccurrenceRepository,
    workspace_id: &str,
    task_id: &str,
    run_id: &str,
    execution_id: &str,
    home_root_thread_id: &str,
    timestamp: i64,
) {
    let now = unix_to_datetime(timestamp);
    let identity = pioneer_entity::agent_identity::Entity::find()
        .filter(pioneer_entity::agent_identity::Column::WorkspaceId.eq(workspace_id))
        .one(&store.connection)
        .await
        .expect("test Agent identity lookup should succeed")
        .expect("test Agent identity should exist");
    let snapshot =
        pioneer_crud_execution::agent_identity::load_current_agent_presentation_snapshot(
            &store.connection,
            identity.id.as_str(),
            identity.source_revision,
            identity.source_fingerprint.as_str(),
        )
        .await
        .expect("test Agent presentation lookup should succeed")
        .expect("test Agent presentation should exist");

    let tx = store.connection.begin().await.unwrap();
    insert_agent_execution_fixture(
        &tx,
        &AgentExecutionInput {
            id: execution_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            agent_identity_id: identity.id,
            identity_source_revision: identity.source_revision,
            identity_source_fingerprint: identity.source_fingerprint,
            parent_execution_id: None,
            parent_task_id: Some(task_id.to_owned()),
            parent_thread_id: Some(home_root_thread_id.to_owned()),
            home_root_thread_id: home_root_thread_id.to_owned(),
            work_graph_root_execution_id: execution_id.to_owned(),
            requested_identity_selection_json: "{}".to_owned(),
            requested_profile_selection_json: "{}".to_owned(),
            resolved_profile_id: None,
            resolved_profile_fingerprint: None,
            presentation_snapshot_id: Some(snapshot.id),
            authorization_context_fingerprint: "test-authorization".to_owned(),
            execution_generation: 1,
            status: "running".to_owned(),
            now,
        },
    )
    .await
    .expect("test AgentExecution should persist");
    pioneer_crud_execution::agent_graph_resources::ensure_root_resource_scope(
        &tx,
        execution_id,
        1,
        8,
        8,
        8,
        64,
        now,
    )
    .await
    .expect("test Agent root scope should persist");
    pioneer_crud_execution::agent_execution::insert_agent_resource_state(
        &tx,
        &AgentResourceStateInput {
            id: format!("resource-{execution_id}-1"),
            execution_id: execution_id.to_owned(),
            attempt_generation: 1,
            branch_key: format!("task:{task_id}:{run_id}"),
            fair_order: 1,
            now,
        },
    )
    .await
    .expect("test Agent resource attempt should persist");
    tx.commit().await.unwrap();
    store
        .upsert_task_occurrence_contract(
            &TaskOccurrenceContract {
                occurrence_id: run_id.to_owned(),
                task_id: task_id.to_owned(),
                run_id: run_id.to_owned(),
                trigger_id: None,
                occurrence_key: format!("immediate:{run_id}"),
                execution_generation: 1,
                agent_execution_id: Some(execution_id.to_owned()),
                work_graph_root_execution_id: Some(execution_id.to_owned()),
                root_resource_scope_id: Some(execution_id.to_owned()),
                status: TaskOccurrenceStatus::Running,
                queue_position: None,
                retry_attempt: 0,
                action_idempotency_key: format!("task:{task_id}:{run_id}"),
                route_id: None,
                result_return_route_id: None,
                delivery_plan: None,
                terminal_reason: None,
            },
            timestamp,
        )
        .await
        .expect("test Task occurrence contract should persist");
}
async fn insert_agent_execution_fixture(
    tx: &sea_orm::DatabaseTransaction,
    input: &AgentExecutionInput,
) -> Result<pioneer_entity::agent_execution::Model> {
    let (row, inserted) =
        pioneer_crud_execution::agent_execution::insert_agent_execution(tx, input).await?;
    if inserted {
        producer::agent_change(tx, None, Some(&row)).await?;
    }
    Ok(row)
}

impl TaskOccurrenceRepository {
    fn event_context(&self) -> TaskEventContextRepository<'_> {
        TaskEventContextRepository::new(&self.connection)
    }
}
