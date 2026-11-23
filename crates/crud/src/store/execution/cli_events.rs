use crate::{
    CliRuntimeNativeEventListFilter, CliRuntimeNativeEventRecord, CrudStore,
    NativeEventCleanupMetrics, NewCliRuntimeNativeEvent,
};
use anyhow::Result;

impl CrudStore {
    pub async fn append_cli_runtime_native_event(
        &self,
        event: NewCliRuntimeNativeEvent,
    ) -> Result<CliRuntimeNativeEventRecord> {
        pioneer_crud_execution::native_events::NativeEventRepository::new(&self.connection)
            .append_cli_runtime_native_event(event)
            .await
    }

    pub async fn append_cli_runtime_native_event_if_absent(
        &self,
        event: NewCliRuntimeNativeEvent,
    ) -> Result<CliRuntimeNativeEventRecord> {
        pioneer_crud_execution::native_events::NativeEventRepository::new(&self.connection)
            .append_cli_runtime_native_event_if_absent(event)
            .await
    }

    pub async fn list_cli_runtime_native_events(
        &self,
        filter: CliRuntimeNativeEventListFilter,
    ) -> Result<Vec<CliRuntimeNativeEventRecord>> {
        pioneer_crud_execution::native_events::NativeEventRepository::new(&self.connection)
            .list_cli_runtime_native_events(filter)
            .await
    }

    /// One bounded, turn-addressed maintenance quantum. No source-table scan.
    pub async fn cleanup_native_events_quantum(&self) -> Result<NativeEventCleanupMetrics> {
        pioneer_crud_execution::native_event_cleanup::run_quantum(&self.connection).await
    }

    /// Earliest queued native event cleanup retry.
    pub async fn native_event_cleanup_deadline(&self) -> Result<Option<i64>> {
        pioneer_crud_execution::native_event_cleanup::deadline(&self.connection).await
    }

    pub async fn latest_cli_runtime_native_event(
        &self,
        filter: CliRuntimeNativeEventListFilter,
    ) -> Result<Option<CliRuntimeNativeEventRecord>> {
        pioneer_crud_execution::native_events::NativeEventRepository::new(&self.connection)
            .latest_cli_runtime_native_event(filter)
            .await
    }
}
