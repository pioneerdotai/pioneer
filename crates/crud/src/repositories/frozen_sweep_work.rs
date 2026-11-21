//! Durable eligibility only. These tokens never authorize physical deletion.
//! All helpers inherit the maintenance handle and release capacity per quantum.
use crate::CrudStore;
use anyhow::Result;

pub(super) use pioneer_crud_compaction::sweep_work::{Admission, Completion, ServiceRound, Work};

pub(super) async fn fanout_step(store: &CrudStore, targets: Option<&[String]>) -> Result<bool> {
    pioneer_crud_compaction::sweep_work::fanout_step(&store.connection, targets).await
}

pub(super) async fn next_retry_deadline(store: &CrudStore) -> Result<Option<i64>> {
    pioneer_crud_compaction::sweep_work::next_retry_deadline(&store.connection).await
}

pub(super) async fn discover(
    store: &CrudStore,
    active: &[(&str, i64)],
    kind: i64,
    round: &mut ServiceRound,
    targets: Option<&[String]>,
    retries_only: bool,
) -> Result<Admission> {
    pioneer_crud_compaction::sweep_work::discover(
        &store.connection,
        active,
        kind,
        round,
        targets,
        retries_only,
    )
    .await
}
