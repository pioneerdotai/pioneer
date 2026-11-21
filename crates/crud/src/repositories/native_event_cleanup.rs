use anyhow::Result;

#[cfg(test)]
#[path = "native_event_cleanup_tests.rs"]
mod tests;

impl crate::CrudStore {
    /// Earliest queued native event cleanup retry.
    pub async fn native_event_cleanup_deadline(&self) -> Result<Option<i64>> {
        pioneer_crud_execution::native_event_cleanup::deadline(&self.connection).await
    }
}
