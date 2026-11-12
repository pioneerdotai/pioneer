use pioneer_crud::CrudStore;
pub(super) async fn run(store: &CrudStore) -> anyhow::Result<()> {
    let store = store.with_maintenance_access();
    loop {
        let progress = store
            .run_background_database_quantum(|| store.backfill_execution_observations_quantum())
            .await?;
        if progress.complete {
            break;
        }
        super::maintenance_checkpoint().await?;
    }
    loop {
        let progress = store
            .run_background_database_quantum(|| store.backfill_aux_execution_observations_quantum())
            .await?;
        if progress.complete {
            return Ok(());
        }
        super::maintenance_checkpoint().await?;
    }
}
