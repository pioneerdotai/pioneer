use super::*;
use pioneer_call_service::CallError;
use pioneer_calls::Call;
use pioneer_crud::ThreadCallScope;
use pioneer_protocol::{ThreadCallAction, ThreadCallParams};

impl MessageProcessor {
    pub(crate) async fn run_thread_calls(&self, calls: &Calls) {
        let store = Arc::new(self.crud_store.with_maintenance_access());
        let processor = Arc::new(self.scoped_with_crud_store(store.clone()));
        calls
            .run(&store, move |call| {
                let processor = processor.clone();
                async move { processor.publish_call_changed(&call).await }
            })
            .await;
    }

    pub(crate) fn calls(&self) -> Option<&Calls> {
        self.thread_calls.as_deref()
    }

    pub(crate) async fn call_media_target(&self) -> Option<pioneer_tunnel::CallMediaTarget> {
        self.remote_access_supervisor
            .as_ref()?
            .call_media_target()
            .await
    }

    pub(crate) fn call_store(&self) -> &CrudStore {
        &self.crud_store
    }

    pub(super) async fn thread_call(
        &self,
        context: &RequestContext,
        request_id: RequestId,
        params: ThreadCallParams,
    ) {
        let connection_id = context.connection_id();
        let Some(calls) = self.calls() else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    "calling is unavailable",
                ),
            )
            .await;
            return;
        };
        let gateway = context.principal().gateway_id.to_string();
        let scope = ThreadCallScope {
            gateway: &gateway,
            workspace: &params.workspace_id,
            thread: &params.thread_id,
        };
        let principal = context.principal().principal_id.to_string();
        let session_id = context.principal().session_id.as_str();
        let relay_url = if calls.media.relay_udp_addr().is_some()
            && matches!(
                &params.action,
                ThreadCallAction::Connect { .. } | ThreadCallAction::Restore { .. }
            ) {
            self.call_media_target()
                .await
                .map(|target| target.signaling_url())
        } else {
            None
        };
        match calls
            .process(
                &self.crud_store,
                &scope,
                &principal,
                session_id,
                params.action,
                relay_url.as_ref().map(url::Url::as_str),
            )
            .await
        {
            Ok(result) => {
                if let Ok(response) = JsonRpcResponse::from_result(request_id, &result) {
                    let _ = self.send_json(connection_id, &response).await;
                }
            }
            Err(error) => {
                let (code, message) = match error {
                    CallError::Missing => (INVALID_REQUEST_CODE, "call not found"),
                    CallError::Conflict => (INVALID_REQUEST_CODE, "call state conflict"),
                    CallError::Invalid => (INVALID_PARAMS_CODE, "invalid call parameters"),
                    CallError::Backend(error) => {
                        // Never expose Twirp responses, connection URLs, or JWTs.
                        drop(error);
                        tracing::warn!("thread call operation failed");
                        (INVALID_REQUEST_CODE, "calling is unavailable")
                    }
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), code, message),
                )
                .await;
            }
        }
    }

    pub(crate) async fn publish_call_changed(&self, call: &Call) -> anyhow::Result<()> {
        let mut recipients = Vec::new();
        for connection in self.session_manager.connection_ids().await {
            let Ok(principal) = self.session_manager.connection_principal(connection).await else {
                continue;
            };
            let actor = principal.principal_id.as_str();
            if principal.gateway_id.as_str() == call.gateway_id
                && call.visible_to(actor)
                && self
                    .crud_store
                    .is_thread_call_member(&ThreadCallScope::from(call), actor)
                    .await?
            {
                recipients.push(connection);
            }
        }
        self.send_retryable_thread_notification(
            &call.snapshot.thread_id,
            events::THREAD_CALL_CHANGED,
            &call.snapshot,
            recipients,
        )
        .await
    }
}
