use crate::{CrudStore, ThreadCallScope};
use anyhow::Result;
use pioneer_calls::Call;
use pioneer_crud_calls::CallRepository;

impl CrudStore {
    pub async fn is_thread_call_member(
        &self,
        scope: &ThreadCallScope<'_>,
        principal: &str,
    ) -> Result<bool> {
        CallRepository(&self.connection)
            .contains(scope, principal)
            .await
    }

    pub async fn can_access_thread_call_media(
        &self,
        call: &Call,
        principal: &str,
        now: i64,
    ) -> Result<bool> {
        CallRepository(&self.connection)
            .media_access(call, principal, now)
            .await
    }

    pub async fn classify_thread_call(&self, scope: &ThreadCallScope<'_>) -> Result<Vec<String>> {
        CallRepository(&self.connection).classify(scope).await
    }

    pub async fn thread_call(&self, call_id: &str) -> Result<Option<Call>> {
        CallRepository(&self.connection).get(call_id).await
    }

    pub async fn latest_thread_call(
        &self,
        scope: &ThreadCallScope<'_>,
        actor: &str,
    ) -> Result<Option<Call>> {
        CallRepository(&self.connection).latest(scope, actor).await
    }

    pub async fn thread_call_history(
        &self,
        scope: &ThreadCallScope<'_>,
        before: Option<&Call>,
        actor: &str,
    ) -> Result<(Vec<Call>, Option<String>)> {
        CallRepository(&self.connection)
            .history(scope, before, actor)
            .await
    }

    pub async fn active_thread_call(&self, thread_id: &str) -> Result<Option<Call>> {
        CallRepository(&self.connection).active(thread_id).await
    }

    pub async fn active_thread_calls_for_principal(&self, principal: &str) -> Result<Vec<Call>> {
        CallRepository(&self.connection)
            .active_for_principal(principal)
            .await
    }

    pub async fn active_thread_call_page(&self, after: Option<&str>) -> Result<Vec<Call>> {
        CallRepository(&self.connection).active_page(after).await
    }

    pub async fn save_thread_call(
        &self,
        call: &Call,
        previous_revision: Option<i64>,
        actor: Option<&str>,
    ) -> Result<bool> {
        CallRepository(&self.connection)
            .save(call, previous_revision, actor)
            .await
    }
}
