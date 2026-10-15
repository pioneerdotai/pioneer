use crate::{
    catalog::GatewayConnectionState,
    sidebar::{CatalogSidebar, sidebar_label, sidebar_menu_icon},
};
use gpui_kit::component::IconName as OAuthIconName;
use gpui_kit::component::{button::*, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::mcp::oauth::OAuthPresentation;
use pioneer_client::mcp::types::{McpOAuthState, McpServerStatus};
use pioneer_client::mcp::{operations::McpIntent, types::McpListItem};
impl CatalogSidebar {
    pub(crate) fn render_mcp_oauth_actions(&self, server: &McpListItem) -> Option<AnyElement> {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return None;
        }
        let presentation = self.input.oauth.get(&server.id);
        let management = self
            .input
            .mcp_server_details
            .as_ref()
            .filter(|details| details.server.id == server.id)
            .and_then(|details| details.management.as_ref());
        let persisted_state = management.and_then(|management| management.oauth_state);
        if persisted_state.is_none() && presentation.is_none() {
            return None;
        }
        let state = pioneer_client::mcp::oauth::effective_oauth_management_state(
            presentation.map(|(event, _)| event.state),
            persisted_state,
            presentation.is_some_and(|(event, _)| {
                event.flow_id.is_some()
                    && event.diagnostic.as_deref() == Some("oauth_callback_unavailable")
            }),
        );
        let presentation = presentation.filter(|(event, _)| Some(event.state) == state);
        let callback_unavailable = presentation.is_some_and(|(event, _)| {
            event.flow_id.is_some()
                && event.diagnostic.as_deref() == Some("oauth_callback_unavailable")
        });
        let actions = oauth_management_actions(
            state,
            server.status,
            callback_unavailable,
            management.and_then(|management| management.oauth_cleanup_available),
        );
        let error_hint = presentation
            .filter(|(event, _)| event.state == McpOAuthState::Failed)
            .map(|(event, failed)| oauth_label(event, *failed));
        let flow = presentation.and_then(|(event, _)| event.flow_id.clone());
        let fallback = presentation
            .is_some_and(|(event, failed)| *failed && event.authorization_url.is_some());
        if !actions.sign_in && !(actions.cancel && flow.is_some()) && !actions.clear && !fallback {
            return None;
        }
        Some(
            v_flex()
                .gap_1()
                .when(actions.sign_in, |this| {
                    this.child(self.oauth_sidebar_button(
                        "mcp-details-sidebar-oauth-signin",
                        t!("mcp.oauth.sign_in").to_string(),
                        OAuthIconName::User,
                        McpIntent::SignIn {
                            server_id: server.id.clone(),
                        },
                        error_hint.clone(),
                    ))
                })
                .when(fallback, |this| {
                    this.child(self.oauth_sidebar_button(
                        "mcp-details-sidebar-oauth-link",
                        t!("mcp.oauth.open_link").to_string(),
                        OAuthIconName::ExternalLink,
                        McpIntent::RetryAuthorizationBrowser {
                            server_id: server.id.clone(),
                        },
                        error_hint.clone(),
                    ))
                })
                .when(actions.cancel, |this| {
                    this.when_some(flow, |this, flow_id| {
                        this.child(self.oauth_sidebar_button(
                            "mcp-details-sidebar-oauth-cancel",
                            t!("buttons.cancel").to_string(),
                            OAuthIconName::Close,
                            McpIntent::CancelAuthorization {
                                server_id: server.id.clone(),
                                flow_id,
                            },
                            None,
                        ))
                    })
                })
                .when(actions.clear, |this| {
                    this.child(self.oauth_sidebar_button(
                        "mcp-details-sidebar-oauth-clear",
                        if state == Some(McpOAuthState::Authorized) {
                            t!("mcp.oauth.disconnect").to_string()
                        } else {
                            t!("mcp.oauth.clear_sign_in").to_string()
                        },
                        OAuthIconName::CircleX,
                        McpIntent::Disconnect {
                            server_id: server.id.clone(),
                        },
                        error_hint,
                    ))
                })
                .into_any_element(),
        )
    }

    fn oauth_sidebar_button(
        &self,
        id: &'static str,
        label: String,
        icon: OAuthIconName,
        intent: McpIntent,
        error_hint: Option<String>,
    ) -> Button {
        let owner = self.owner.clone();
        let server_id = match &intent {
            McpIntent::SignIn { server_id }
            | McpIntent::RetryAuthorizationBrowser { server_id }
            | McpIntent::CancelAuthorization { server_id, .. }
            | McpIntent::Disconnect { server_id } => server_id,
            _ => unreachable!("OAuth sidebar action"),
        };
        Button::new(id)
            .ghost()
            .justify_start()
            .px_2()
            .disabled(
                self.gateway.connection_state != GatewayConnectionState::Connected
                    || self.is_mcp_pending(server_id),
            )
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_start()
                    .gap_2()
                    .child(sidebar_menu_icon(icon))
                    .child(sidebar_label(label)),
            )
            .when_some(error_hint, |button, hint| button.tooltip(hint))
            .on_click(move |_, _, cx| {
                let _ = owner.update(cx, |view, cx| {
                    let intent = match &intent {
                        McpIntent::SignIn { server_id } => McpIntent::SignIn {
                            server_id: server_id.clone(),
                        },
                        McpIntent::RetryAuthorizationBrowser { server_id } => {
                            McpIntent::RetryAuthorizationBrowser {
                                server_id: server_id.clone(),
                            }
                        }
                        McpIntent::CancelAuthorization { server_id, flow_id } => {
                            McpIntent::CancelAuthorization {
                                server_id: server_id.clone(),
                                flow_id: flow_id.clone(),
                            }
                        }
                        McpIntent::Disconnect { server_id } => McpIntent::Disconnect {
                            server_id: server_id.clone(),
                        },
                        _ => unreachable!("OAuth sidebar action"),
                    };
                    view.send(intent, cx);
                    cx.notify();
                });
            })
    }
}
pub(crate) fn preparation_error_label(code: &str) -> Option<String> {
    match code {
        "oauth_callback_port_invalid" => Some(t!("mcp.oauth.callback_port_invalid").to_string()),
        "oauth_configuration_load_failed" => {
            Some(t!("mcp.oauth.configuration_load_failed").to_string())
        }
        "oauth_callback_port_unavailable" => Some(t!("mcp.oauth.callback_unavailable").to_string()),
        _ => None,
    }
}
fn oauth_label(event: &OAuthPresentation, failed: bool) -> String {
    if let Some(label) = event
        .diagnostic
        .as_deref()
        .and_then(preparation_error_label)
    {
        return label;
    }

    if event.diagnostic.as_deref() == Some("oauth_callback_unavailable") {
        return t!("mcp.oauth.callback_unavailable").to_string();
    }
    if failed {
        return t!("mcp.oauth.browser_failed").to_string();
    }
    match event.diagnostic.as_deref() {
        Some("oauth_callback_preparation_failed") => {
            return t!("mcp.oauth.prepare_failed").to_string();
        }
        Some("OAuth provider temporarily unavailable") => {
            return t!("mcp.oauth.recovering").to_string();
        }
        Some("OAuth client authentication method unsupported") => {
            return t!("mcp.oauth.client_auth_unsupported").to_string();
        }
        Some("OAuth client registration unavailable; configure a registered client_id") => {
            return t!("mcp.oauth.registration_required").to_string();
        }
        Some("OAuth callback address does not match the saved registration") => {
            return t!("mcp.oauth.redirect_mismatch").to_string();
        }
        _ => {}
    }
    t!("mcp.oauth.failed").to_string()
}

struct OAuthManagementActions {
    sign_in: bool,
    cancel: bool,
    clear: bool,
}
fn oauth_management_actions(
    state: Option<McpOAuthState>,
    status: McpServerStatus,
    callback_unavailable: bool,
    cleanup_available: Option<bool>,
) -> OAuthManagementActions {
    let active = callback_unavailable
        || matches!(
            state,
            Some(
                McpOAuthState::Preparing
                    | McpOAuthState::AwaitingCallback
                    | McpOAuthState::Exchanging
            )
        );
    let requires_consent = status == McpServerStatus::AuthRequired
        || matches!(
            state,
            Some(
                McpOAuthState::AuthRequired
                    | McpOAuthState::Denied
                    | McpOAuthState::TimedOut
                    | McpOAuthState::Cancelled
                    | McpOAuthState::InsufficientScope
            )
        );
    OAuthManagementActions {
        sign_in: requires_consent
            && !active
            && !matches!(
                state,
                Some(McpOAuthState::Resolving | McpOAuthState::CleanupRequired)
            ),
        cancel: active,
        clear: !active
            && state.is_some_and(|state| state != McpOAuthState::Idle)
            && (cleanup_available.unwrap_or(true)
                || matches!(
                    state,
                    Some(McpOAuthState::Resolving | McpOAuthState::CleanupRequired)
                )),
    }
}
#[cfg(test)]
mod tests {
    use super::{McpOAuthState, McpServerStatus, oauth_management_actions};
    #[test]
    fn configuration_failure_copy_is_distinct_in_action_and_consent_presentations() {
        use pioneer_client::mcp::oauth::{OAuthPreparationError, OAuthPresentation};
        for (reason, key) in [
            (
                OAuthPreparationError::InvalidCallbackPort,
                "mcp.oauth.callback_port_invalid",
            ),
            (
                OAuthPreparationError::ConfigurationLoad,
                "mcp.oauth.configuration_load_failed",
            ),
            (
                OAuthPreparationError::PortUnavailable,
                "mcp.oauth.callback_unavailable",
            ),
        ] {
            let label = super::preparation_error_label(reason.code()).unwrap();
            assert_eq!(label, t!(key).to_string());
            assert_ne!(label, t!("mcp.oauth.failed").to_string());
            if reason != OAuthPreparationError::PortUnavailable {
                assert_ne!(label, t!("mcp.oauth.prepare_failed").to_string());
                assert_ne!(label, t!("mcp.oauth.callback_unavailable").to_string());
            }
            let event = OAuthPresentation {
                workspace_id: "workspace".into(),
                server_id: "server".into(),
                name: "server".into(),
                scope_kind: pioneer_client::mcp::actions::mcp_server_restart_params(
                    "workspace",
                    "server",
                )
                .scope_kind,
                flow_id: Some("flow".into()),
                state: McpOAuthState::Failed,
                authorization_url: None,
                diagnostic: Some(reason.code().into()),
            };
            assert_eq!(super::oauth_label(&event, false), label);
        }
    }
    #[test]
    fn signed_out_without_saved_registration_offers_signin_without_clear() {
        for status in [McpServerStatus::AuthRequired, McpServerStatus::Starting] {
            let actions = oauth_management_actions(
                Some(McpOAuthState::AuthRequired),
                status,
                false,
                Some(false),
            );
            assert!(actions.sign_in);
            assert!(!actions.clear && !actions.cancel);
        }
        let saved = oauth_management_actions(
            Some(McpOAuthState::AuthRequired),
            McpServerStatus::AuthRequired,
            false,
            Some(true),
        );
        assert!(saved.sign_in && saved.clear);
        let cleanup = oauth_management_actions(
            Some(McpOAuthState::CleanupRequired),
            McpServerStatus::AuthRequired,
            false,
            Some(true),
        );
        assert!(cleanup.clear);
        assert!(!cleanup.sign_in && !cleanup.cancel);
    }
    #[test]
    fn current_details_cleanup_overrides_stale_denied_copy_and_actions() {
        let state = pioneer_client::mcp::oauth::effective_oauth_management_state(
            Some(McpOAuthState::Denied),
            Some(McpOAuthState::CleanupRequired),
            false,
        );
        assert_eq!(state, Some(McpOAuthState::CleanupRequired));
        let actions = oauth_management_actions(state, McpServerStatus::AuthRequired, false, None);
        assert!(actions.clear);
        assert!(!actions.sign_in && !actions.cancel);
        for pending in [
            McpOAuthState::Preparing,
            McpOAuthState::AwaitingCallback,
            McpOAuthState::Exchanging,
            McpOAuthState::Resolving,
        ] {
            assert_eq!(
                pioneer_client::mcp::oauth::effective_oauth_management_state(
                    Some(pending),
                    Some(McpOAuthState::CleanupRequired),
                    false
                ),
                Some(pending)
            );
        }
    }
    #[test]
    fn failed_clear_remains_actionable_without_restarting_or_signing_in() {
        for status in [
            McpServerStatus::Ready,
            McpServerStatus::AuthRequired,
            McpServerStatus::Degraded,
        ] {
            let actions =
                oauth_management_actions(Some(McpOAuthState::CleanupRequired), status, false, None);
            assert!(actions.clear);
            assert!(!actions.sign_in);
            assert!(!actions.cancel);
        }
        let actions = oauth_management_actions(
            Some(McpOAuthState::AwaitingCallback),
            McpServerStatus::AuthRequired,
            false,
            None,
        );
        assert!(actions.cancel);
        assert!(!actions.clear);
        let actions = oauth_management_actions(
            Some(McpOAuthState::AuthRequired),
            McpServerStatus::AuthRequired,
            false,
            None,
        );
        assert!(actions.sign_in && actions.clear);
    }
}
