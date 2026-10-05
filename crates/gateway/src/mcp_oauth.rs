//! Thin integration with Gateway sessions and MCP projections. Lifecycle lives
//! entirely in pioneer-mcp-oauth.
use super::mcp_service::{McpService, McpServiceInner, installation_from_record};
use pioneer_mcp_oauth::{OAuthEvent, OAuthEventSink, OAuthState};
use pioneer_protocol::{
    AuthSecretString, JsonRpcNotification, McpOAuthNotification, McpOAuthState, McpScopeKind,
    constants::events,
};
use std::sync::Weak;
use tokio::sync::mpsc;

pub(crate) struct GatewayOAuthSink {
    pub(crate) owner: Weak<McpServiceInner>,
    pub(crate) events: mpsc::UnboundedSender<OAuthEvent>,
}
#[async_trait::async_trait]
impl OAuthEventSink for GatewayOAuthSink {
    async fn client_available(&self, id: u64, _workspace: &str) -> bool {
        let Some(inner) = self.owner.upgrade() else {
            return false;
        };
        let service = maintenance_service(inner);
        !service
            .authorized_management_notification_recipients(vec![id])
            .await
            .is_empty()
    }
    async fn emit(&self, event: OAuthEvent) {
        // No algorithm, network exchange or OAuth supervision in this adapter.
        let _ = self.events.send(event);
    }
}
/// Select the database class at this background event boundary. Clones passed
/// through restart into runtime actors retain the same scoped handle.
pub(crate) fn maintenance_service(inner: std::sync::Arc<McpServiceInner>) -> McpService {
    let store = std::sync::Arc::new(inner.crud_store.with_maintenance_access());
    McpService {
        inner,
        runtime_store: Some(store),
    }
}
pub(crate) async fn consume_oauth_events(
    owner: Weak<McpServiceInner>,
    mut events_rx: mpsc::UnboundedReceiver<OAuthEvent>,
) {
    while let Some(event) = events_rx.recv().await {
        let Some(inner) = owner.upgrade() else {
            return;
        };
        let service = maintenance_service(inner);
        let store = service
            .runtime_store
            .as_ref()
            .expect("OAuth event Maintenance scope");
        let rows = match store
            .list_mcp_server_installations(&event.scope_kind, &event.scope_key)
            .await
        {
            Ok(rows) => rows,
            Err(_) => continue,
        };
        let Some(row) = rows
            .into_iter()
            .find(|r| r.id.as_deref() == Some(&event.installation_id))
        else {
            continue;
        };
        let Ok(installation) = installation_from_record(&row) else {
            continue;
        };
        if !service
            .oauth()
            .event_is_current(&event, &installation)
            .await
        {
            continue;
        }
        // Delivery is an effect too: a late URL must not cross uninstall or
        // identity replacement while recipient authorization is awaiting. This
        // gate holds no SQLite capacity; re-read after acquiring admission.
        let delivery = service
            .installation_lifecycle_guard(&event.scope_kind, &event.scope_key, &row.name)
            .await;
        let Ok(Some(current)) = store
            .find_mcp_server_installation(&event.scope_kind, &event.scope_key, &row.name)
            .await
        else {
            continue;
        };
        if current.id.as_deref() != Some(&event.installation_id) {
            continue;
        }
        let Ok(current_installation) = installation_from_record(&current) else {
            continue;
        };
        if !service
            .oauth()
            .event_is_current(&event, &current_installation)
            .await
        {
            continue;
        }
        let notification = McpOAuthNotification {
            workspace_id: event.workspace_id.clone(),
            server_id: event.installation_id.clone(),
            name: row.name.clone(),
            scope_kind: if row.scope_kind == "user" {
                McpScopeKind::User
            } else {
                McpScopeKind::Workspace
            },
            flow_id: event.flow_id.clone(),
            state: map_state(event.state),
            authorization_url: event.authorization_url.clone().map(AuthSecretString::new),
            diagnostic: event.diagnostic.clone(),
        };
        if let Some(client) = event.client_id {
            if !service
                .authorized_management_notification_recipients(vec![client])
                .await
                .is_empty()
            {
                if let Ok(notification) =
                    JsonRpcNotification::from_params(events::MCP_OAUTH_CHANGED, &notification)
                {
                    if let Ok(payload) = serde_json::to_string(&notification) {
                        let _ = service
                            .inner
                            .session_manager
                            .send_text(client, payload)
                            .await;
                    }
                }
            }
        } else if event.flow_id.is_none()
            && matches!(
                event.state,
                OAuthState::Recovered
                    | OAuthState::CleanupRequired
                    | OAuthState::Failed
                    | OAuthState::AuthRequired
                    | OAuthState::InsufficientScope
            )
        {
            // Safe lifecycle projection only. Consent URLs/effects are never
            // broadcast; ordinary successful rotation emits no event at all.
            service
                .send_management_notification(events::MCP_OAUTH_CHANGED, &notification)
                .await;
        }
        drop(delivery);
        #[cfg(test)]
        let barrier = if matches!(
            event.state,
            OAuthState::Authorized
                | OAuthState::AuthRequired
                | OAuthState::InsufficientScope
                | OAuthState::Recovered
        ) {
            service.inner.oauth_effect_barrier.lock().unwrap().clone()
        } else {
            None
        };
        #[cfg(test)]
        if let Some((entered, release, _)) = &barrier {
            entered.notify_one();
            release.notified().await;
        }
        // Notifications may await recipient authorization. Admit the effect
        // separately, against the current durable UUID/identity and generation.
        let _ = service.apply_oauth_runtime_event(&event, &row.name).await;
        #[cfg(test)]
        if let Some((_, _, completed)) = barrier {
            completed.notify_one();
        }
    }
}
pub(crate) fn map_state(state: OAuthState) -> McpOAuthState {
    match state {
        OAuthState::Idle => McpOAuthState::Idle,
        OAuthState::Preparing => McpOAuthState::Preparing,
        OAuthState::AwaitingCallback => McpOAuthState::AwaitingCallback,
        OAuthState::Exchanging => McpOAuthState::Exchanging,
        OAuthState::Resolving => McpOAuthState::Resolving,
        OAuthState::Retired => McpOAuthState::Retired,
        OAuthState::CleanupRequired => McpOAuthState::CleanupRequired,
        OAuthState::Authorized | OAuthState::Recovered => McpOAuthState::Authorized,
        OAuthState::AuthRequired => McpOAuthState::AuthRequired,
        OAuthState::InsufficientScope => McpOAuthState::InsufficientScope,
        OAuthState::Denied => McpOAuthState::Denied,
        OAuthState::Cancelled => McpOAuthState::Cancelled,
        OAuthState::TimedOut => McpOAuthState::TimedOut,
        OAuthState::Failed => McpOAuthState::Failed,
    }
}
