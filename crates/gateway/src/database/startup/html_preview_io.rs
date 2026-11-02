use pioneer_crud::CrudStore;

/// Historical IO discovery belongs to the existing cancellable data-migration
/// lifecycle. Its durable seed cursors resume independently of runtime passes.
pub(super) async fn run(store: &CrudStore) -> anyhow::Result<()> {
    loop {
        super::maintenance_checkpoint().await?;
        let cancellation = super::maintenance_cancellation();
        let more = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return super::maintenance_checkpoint().await,
            result = store.html_preview_io_seed_startup_quantum() => result?,
        };
        if !more {
            return Ok(());
        }
    }
}
