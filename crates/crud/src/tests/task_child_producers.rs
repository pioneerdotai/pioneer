//! Transactional producer regressions; external code acceptance gate CLOSED.
use super::*;
use pioneer_crud_tasks::task;
use pioneer_entity::task_child_reconcile_scope as scope;
use sea_orm::{EntityTrait, TransactionTrait};

const AT: i64 = 1_700_000_000;

async fn producer_store() -> CrudStore {
    let store = test_store_with_workspace("ws_task").await;
    store.with_maintenance_access()
}

#[pioneer_postgres_test::postgres_test]
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
