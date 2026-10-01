use crate::catalog::McpCatalogView;
use gpui_kit::component::{Sizable, button::*, theme::ActiveTheme, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::mcp::oauth::OAuthPresentation;
use pioneer_client::mcp::types::{McpOAuthState, McpServerStatus};
use pioneer_client::mcp::{operations::McpIntent, types::McpListItem};
impl McpCatalogView {
    pub(crate) fn render_mcp_oauth(
        &self,
        server: &McpListItem,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if !self
            .principal_presentation_capabilities()
            .can_manage_capabilities
        {
            return div().into_any_element();
        }
        let presentation = self.input.oauth.get(&server.id);
        let persisted_state = self
            .input
            .mcp_server_details
            .as_ref()
            .and_then(|d| d.management.as_ref())
            .and_then(|m| m.oauth_state);
        if persisted_state.is_none() && presentation.is_none() {
            return div().into_any_element();
        }
        let state = pioneer_client::mcp::oauth::effective_oauth_management_state(
            presentation.map(|(p, _)| p.state),
            persisted_state,
            presentation.is_some_and(|(event, _)| {
                event.flow_id.is_some()
                    && event.diagnostic.as_deref() == Some("oauth_callback_unavailable")
            }),
        );
        // Do not render a stale terminal label, URL or action over current cleanup.
        let presentation = presentation.filter(|(event, _)| Some(event.state) == state);
        // A listener failure leaves Gateway's consent flow alive. Cancel that
        // flow before offering another sign-in; an exchange failure has ended it.
        let callback_unavailable = presentation.is_some_and(|(event, _)| {
            event.flow_id.is_some()
                && event.diagnostic.as_deref() == Some("oauth_callback_unavailable")
        });
        let actions = oauth_management_actions(state, server.status, callback_unavailable);
        let view = cx.entity();
        let id = server.id.clone();
        v_flex()
            .px_6()
            .py_3()
            .gap_2()
            .border_b_1()
            .border_color(cx.theme().border)
            .when_some(presentation, |this, (event, failed)| {
                this.child(div().text_sm().child(oauth_label(event, *failed)))
                    .when(*failed, |this| {
                        this.when_some(event.authorization_url.as_ref(), |this, _url| {
                            let view = view.clone();
                            let id = id.clone();
                            this.child(
                                Button::new(self.ui_id(&server.id, "oauth-link"))
                                    .outline()
                                    .small()
                                    .label(t!("mcp.oauth.open_link").to_string())
                                    .on_click(move |_, _, cx| {
                                        view.update(cx, |view, cx| {
                                            view.send(
                                                McpIntent::RetryAuthorizationBrowser {
                                                    server_id: id.clone(),
                                                },
                                                cx,
                                            )
                                        });
                                    }),
                            )
                        })
                    })
            })
            .when(presentation.is_none(), |this| {
                this.when_some(state, |this, state| {
                    let label = if state == McpOAuthState::Failed
                        && server.status != McpServerStatus::AuthRequired
                    {
                        t!("mcp.oauth.recovering").to_string()
                    } else {
                        oauth_state_label(state)
                    };
                    this.child(div().text_sm().child(label))
                })
            })
            .child(
                h_flex()
                    .gap_2()
                    .when(actions.sign_in, |this| {
                        let view = view.clone();
                        let id = id.clone();
                        this.child(
                            Button::new(self.ui_id(&server.id, "oauth-signin"))
                                .outline()
                                .small()
                                .label(t!("mcp.oauth.sign_in").to_string())
                                .on_click(move |_, _, cx| {
                                    view.update(cx, |view, cx| {
                                        view.send(
                                            McpIntent::SignIn {
                                                server_id: id.clone(),
                                            },
                                            cx,
                                        )
                                    })
                                }),
                        )
                    })
                    .when(actions.cancel, |this| {
                        this.when_some(
                            presentation.and_then(|(p, _)| p.flow_id.clone()),
                            |this, flow_id| {
                                let view = view.clone();
                                let id = id.clone();
                                this.child(
                                    Button::new(self.ui_id(&server.id, "oauth-cancel"))
                                        .outline()
                                        .small()
                                        .label(t!("buttons.cancel").to_string())
                                        .on_click(move |_, _, cx| {
                                            view.update(cx, |view, cx| {
                                                view.send(
                                                    McpIntent::CancelAuthorization {
                                                        server_id: id.clone(),
                                                        flow_id: flow_id.clone(),
                                                    },
                                                    cx,
                                                )
                                            })
                                        }),
                                )
                            },
                        )
                    })
                    .when(actions.clear, |this| {
                        this.child(
                            Button::new(self.ui_id(&server.id, "oauth-disconnect"))
                                .ghost()
                                .small()
                                .label(if state == Some(McpOAuthState::Authorized) {
                                    t!("mcp.oauth.disconnect").to_string()
                                } else {
                                    t!("mcp.oauth.clear_sign_in").to_string()
                                })
                                .on_click(move |_, _, cx| {
                                    view.update(cx, |view, cx| {
                                        view.send(
                                            McpIntent::Disconnect {
                                                server_id: id.clone(),
                                            },
                                            cx,
                                        )
                                    })
                                }),
                        )
                    }),
            )
            .into_any_element()
    }
}
fn oauth_label(event: &OAuthPresentation, failed: bool) -> String {
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
    oauth_state_label(event.state)
}
fn oauth_state_label(state: McpOAuthState) -> String {
    match state {
        McpOAuthState::Preparing => t!("mcp.oauth.preparing"),
        McpOAuthState::AwaitingCallback => t!("mcp.oauth.awaiting"),
        McpOAuthState::Exchanging => t!("mcp.oauth.exchanging"),
        McpOAuthState::Resolving => t!("mcp.oauth.resolving"),
        McpOAuthState::CleanupRequired => t!("mcp.oauth.cleanup_required"),
        McpOAuthState::Authorized => t!("mcp.oauth.authorized"),
        McpOAuthState::Denied => t!("mcp.oauth.denied"),
        McpOAuthState::TimedOut => t!("mcp.oauth.timed_out"),
        McpOAuthState::Cancelled => t!("mcp.oauth.cancelled"),
        McpOAuthState::InsufficientScope => t!("mcp.oauth.scope_required"),
        McpOAuthState::AuthRequired => t!("mcp.oauth.auth_required"),
        McpOAuthState::Failed => t!("mcp.oauth.failed"),
        McpOAuthState::Retired | McpOAuthState::Idle => t!("mcp.oauth.auth_required"),
    }
    .to_string()
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
                McpOAuthState::Denied
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
        clear: !active && state.is_some_and(|state| state != McpOAuthState::Idle),
    }
}
#[cfg(test)]
mod tests {
    use super::{McpOAuthState, McpServerStatus, oauth_management_actions};
    #[test]
    fn current_details_cleanup_overrides_stale_denied_copy_and_actions() {
        let state = pioneer_client::mcp::oauth::effective_oauth_management_state(
            Some(McpOAuthState::Denied),
            Some(McpOAuthState::CleanupRequired),
            false,
        );
        assert_eq!(state, Some(McpOAuthState::CleanupRequired));
        let actions = oauth_management_actions(state, McpServerStatus::AuthRequired, false);
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
                oauth_management_actions(Some(McpOAuthState::CleanupRequired), status, false);
            assert!(actions.clear);
            assert!(!actions.sign_in);
            assert!(!actions.cancel);
        }
        let actions = oauth_management_actions(
            Some(McpOAuthState::AwaitingCallback),
            McpServerStatus::AuthRequired,
            false,
        );
        assert!(actions.cancel);
        assert!(!actions.clear);
        let actions = oauth_management_actions(
            Some(McpOAuthState::AuthRequired),
            McpServerStatus::AuthRequired,
            false,
        );
        assert!(actions.sign_in && actions.clear);
    }
}
