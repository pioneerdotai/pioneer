//! Owned, cancellable legacy seed. Only a committed EOF marker completes it.
use anyhow::Result;
use pioneer_crud::{ChildSeedOutcome, CrudStore};
use tokio_util::sync::CancellationToken;
pub(super) async fn run(store: &CrudStore) -> Result<()> {
    run_with_clock(
        store,
        &|| chrono::Utc::now().timestamp(),
        super::maintenance_cancellation(),
    )
    .await
}
async fn run_with_clock<F: Fn() -> i64 + Sync>(
    store: &CrudStore,
    clock: &F,
    cancellation: CancellationToken,
) -> Result<()> {
    let store = store.with_maintenance_access();
    loop {
        let outcome = tokio::select! { biased;
            _=cancellation.cancelled()=>return Err(super::StartupMaintenanceCancelled.into()),
            result=store.seed_child_work_with_clock(clock)=>result?,
        };
        let delay = match outcome {
            ChildSeedOutcome::CommittedDone => return Ok(()),
            ChildSeedOutcome::Missing => anyhow::bail!("child seed marker is missing"),
            ChildSeedOutcome::Progress => super::BACKGROUND_BATCH_PAUSE,
            ChildSeedOutcome::Deferred { retry_at }
            | ChildSeedOutcome::Claimed {
                expires_at: retry_at,
            } => {
                std::time::Duration::from_secs(retry_at.saturating_sub(clock()).clamp(1, 2) as u64)
            }
        };
        // No DB resource/permit is retained while awaiting a retry or shutdown.
        tokio::select! { biased;
            _=cancellation.cancelled()=>return Err(super::StartupMaintenanceCancelled.into()),
            _=tokio::time::sleep(delay)=>{},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::ChildSeedOutcome;
    use sea_orm::sea_query::Expr;
    use sea_orm::{ColumnTrait, Database, EntityTrait, QueryFilter};
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    #[tokio::test]
    async fn deferred_seed_owner_cancels_without_done_and_restart_resumes_after_deadline() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        let store = Arc::new(CrudStore::new(db));
        use pioneer_entity::task_child_reconcile_scope as scope;
        scope::Entity::update_many()
            .filter(scope::Column::ScopeKind.eq("historical-seed"))
            .col_expr(scope::Column::NextAttemptAt, Expr::value(200i64))
            .exec(&store.database_connection())
            .await
            .unwrap();
        let wall = Arc::new(AtomicI64::new(100));
        let observed = Arc::new(tokio::sync::Notify::new());
        let cancellation = CancellationToken::new();
        let owner = {
            let store = store.clone();
            let wall = wall.clone();
            let observed = observed.clone();
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                run_with_clock(
                    &store,
                    &|| {
                        observed.notify_one();
                        wall.load(Ordering::SeqCst)
                    },
                    cancellation,
                )
                .await
            })
        };
        observed.notified().await;
        assert!(
            !owner.is_finished(),
            "a deferred marker must retain its owner, not report startup completion"
        );
        cancellation.cancel();
        let error = owner.await.unwrap().unwrap_err();
        assert!(
            error
                .downcast_ref::<super::super::StartupMaintenanceCancelled>()
                .is_some()
        );
        assert_eq!(
            store.child_seed_outcome(100).await.unwrap(),
            ChildSeedOutcome::Deferred { retry_at: 200 }
        );
        wall.store(200, Ordering::SeqCst);
        run_with_clock(
            &store,
            &|| wall.load(Ordering::SeqCst),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            store.child_seed_outcome(200).await.unwrap(),
            ChildSeedOutcome::CommittedDone
        );
    }
}
