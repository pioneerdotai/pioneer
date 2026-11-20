//! Transactional producer regressions; external code acceptance gate CLOSED.
use super::*;
use crate::repositories::{task, task_run};
use pioneer_entity::task_child_reconcile_scope as scope;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};

const AT: i64 = 1_700_000_000;

async fn producer_store() -> CrudStore {
    let store = test_store_with_workspace("ws_task").await;
    pioneer_sqlite::apply_sqlite_pragmas(&store.connection)
        .await
        .unwrap();
    store.with_maintenance_access()
}

#[tokio::test]
async fn child_task_obligation_rolls_back_with_business_projection() {
    let store = producer_store().await;
    let task = sample_task(AT);
    let tx = store.connection.begin().await.unwrap();
    task::upsert_prepared_task(&tx, task::prepare_task_projection(&task).unwrap())
        .await
        .unwrap();
    assert!(
        scope::Entity::find_by_id(("task".to_owned(), task.id.clone()))
            .one(&tx)
            .await
            .unwrap()
            .is_some()
    );
    tx.rollback().await.unwrap();
    assert!(
        task::find_task_by_id(&store.connection, &task.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        scope::Entity::find_by_id(("task".to_owned(), task.id))
            .one(&store.connection)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn task_payload_refresh_does_not_invalidate_child_scope() {
    let store = producer_store().await;
    let mut task = sample_task(AT);
    let tx = store.connection.begin().await.unwrap();
    task::upsert_prepared_task(&tx, task::prepare_task_projection(&task).unwrap())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let key = ("task".to_owned(), task.id.clone());
    let before = scope::Entity::find_by_id(key.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    task.title = "changed presentation only".to_owned();
    task.updated_at += 1;
    let tx = store.connection.begin().await.unwrap();
    task::upsert_prepared_task(&tx, task::prepare_task_projection(&task).unwrap())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        before,
        scope::Entity::find_by_id(key)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
    );
}

#[tokio::test]
async fn run_status_projection_durably_invalidates_origin_group() {
    let store = producer_store().await;
    let task = sample_task(AT);
    let mut run = sample_task_run(AT);
    run.task_id = task.id.clone();
    let tx = store.connection.begin().await.unwrap();
    task::upsert_prepared_task(&tx, task::prepare_task_projection(&task).unwrap())
        .await
        .unwrap();
    task_run::upsert_run(&tx, &run).await.unwrap();
    tx.commit().await.unwrap();
    let key = ("run".to_owned(), run.id.clone());
    let before = scope::Entity::find_by_id(key.clone())
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    let tx = store.connection.begin().await.unwrap();
    task_run::update_run_status(
        &tx,
        &run.id,
        TaskRunStatus::Running,
        unix_to_datetime(AT + 1),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_ne!(
        before.requested_revision,
        scope::Entity::find_by_id(key)
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
            .requested_revision
    );
    assert!(
        scope::Entity::find()
            .filter(scope::Column::ScopeKind.eq("task+run-group"))
            .one(&store.connection)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn moved_link_keeps_new_point_locator_when_it_sorts_before_old() {
    use pioneer_entity::task_child_reconcile_pending as point;
    let store = producer_store().await;
    let mut link = TaskRunTurn {
        id: "child-move-link".into(),
        task_id: "child-move-task".into(),
        run_id: "child-move-run".into(),
        execution_id: None,
        thread_id: "z-old-thread".into(),
        turn_id: "child-move-turn".into(),
        kind: TaskRunTurnKind::Initial,
        round: 0,
        sequence: 0,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at: AT,
        started_at: None,
        completed_at: None,
    };
    // FK-disabled repair may carry orphan locators; queue membership itself
    // never authorizes capsule destruction.
    store.upsert_task_run_turn(link.clone()).await.unwrap();
    link.thread_id = "a-new-thread".into();
    store.upsert_task_run_turn(link.clone()).await.unwrap();
    let pending = point::Entity::find_by_id(link.turn_id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.thread_id, link.thread_id);
}

#[tokio::test]
async fn unknown_commit_response_keeps_business_and_obligation_without_replaying_write() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let store = producer_store().await;
    let task = sample_task(AT);
    let attempts = AtomicUsize::new(0);
    // Lose only the response after a real commit. This is neither rollback nor
    // an empty result, and must not enter the known-rollback CAS retry policy.
    let result: anyhow::Result<()> = store
        .run_serialized_write(|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            let tx = store.connection.begin().await?;
            task::upsert_prepared_task(&tx, task::prepare_task_projection(&task)?).await?;
            tx.commit().await?;
            Err(anyhow::anyhow!("simulated lost commit response"))
        })
        .await;
    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(
        task::find_task_by_id(&store.connection, &task.id)
            .await
            .unwrap()
            .is_some()
    );
    let pending = scope::Entity::find_by_id(("task".to_owned(), task.id))
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert!(pending.next_attempt_at.is_some());
    assert!(pending.claim_token.is_none());
}
