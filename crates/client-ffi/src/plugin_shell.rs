//! Native file/browser boundary. Gateway lifecycle, consent and credentials are
//! owned by existing client/Gateway operations, not by this mailbox.
use pioneer_client::{core::ClientCore, mcp::oauth::*};
use pioneer_protocol::AuthSecretString;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak, mpsc},
    time::{Duration, Instant},
};

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PluginShellRequest {
    #[serde(rename = "configure_oauth")]
    ConfigureOAuth {
        redirect_uri: String,
    },
    #[serde(rename = "poll_oauth")]
    PollOAuth,
    ComposerPicker {
        thread_id: String,
        draft_id: pioneer_client::composer::store::DraftId,
        query: String,
    },
    #[serde(rename = "oauth_callback")]
    OAuthCallback {
        url: String,
    },
    BrowserResult {
        flow_id: String,
        cancelled: bool,
    },
    StartArchive {
        workspace_id: String,
        expected_connection: u64,
        target: pioneer_client::skills::operations::SkillUploadTarget,
        file_uri: String,
    },
    CancelUpload {
        workspace_id: String,
        operation_id: u64,
    },
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct NativeOAuthLaunch {
    pub flow_id: String,
    pub workspace_id: String,
    pub server_id: String,
    pub authorization_url: AuthSecretString,
    pub redirect_uri: String,
}
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Serialize)]
pub struct PluginShellResponse {
    pub accepted: bool,
    pub operation_id: Option<u64>,
    pub launches: Vec<NativeOAuthLaunch>,
    pub picker: Vec<pioneer_client::plugins::PluginPickerRow>,
    pub active_flows: Vec<String>,
}
impl PluginShellResponse {
    fn accepted(accepted: bool) -> Self {
        Self {
            accepted,
            operation_id: None,
            launches: vec![],
            picker: vec![],
            active_flows: vec![],
        }
    }
}
enum BrowserCompletion {
    Callback(OAuthCallbackFields),
    Cancelled,
    Failed,
}
struct Flow {
    relay: OAuthCallbackRelay,
    callback_url: Option<AuthSecretString>,
    event: OAuthPresentation,
    admission: OAuthBrowserAdmission,
    state: AuthSecretString,
    delivered: bool,
    sender: mpsc::SyncSender<BrowserCompletion>,
    expires: Instant,
}
#[derive(Default)]
pub(crate) struct NativeOAuthShell {
    redirect: Mutex<Option<String>>,
    flows: Mutex<BTreeMap<String, Flow>>,
    core: Weak<ClientCore>,
}
impl NativeOAuthShell {
    pub(crate) fn new(core: &Arc<ClientCore>) -> Self {
        Self {
            core: Arc::downgrade(core),
            ..Default::default()
        }
    }
    fn configure(&self, redirect: String) -> Result<(), String> {
        let u = url::Url::parse(&redirect).map_err(|_| "invalid_native_redirect")?;
        if !matches!(u.scheme(), "pioneer" | "pioneer-dev")
            || u.host_str().is_some()
            || u.port().is_some()
            || u.path() != "/oauth/mcp/callback"
            || u.query().is_some()
            || u.fragment().is_some()
            || !u.username().is_empty()
            || u.password().is_some()
        {
            return Err("invalid_native_redirect".into());
        }
        let mut current = self.redirect.lock().unwrap();
        if current.as_ref().is_some_and(|r| r != &redirect) {
            return Err("native_redirect_already_configured".into());
        }
        *current = Some(redirect);
        Ok(())
    }
    fn poll(&self) -> PluginShellResponse {
        let redirect = self.redirect.lock().unwrap().clone().unwrap_or_default();
        let mut flows = self.flows.lock().unwrap();
        flows.retain(|_, f| f.admission.is_current() && f.expires > Instant::now());
        let active_flows = flows.keys().cloned().collect();
        let launches = flows
            .iter_mut()
            .filter_map(|(id, f)| {
                if f.delivered {
                    return None;
                }
                f.delivered = true;
                Some(NativeOAuthLaunch {
                    flow_id: id.clone(),
                    workspace_id: f.event.workspace_id.clone(),
                    server_id: f.event.server_id.clone(),
                    authorization_url: f.event.authorization_url.clone()?,
                    redirect_uri: redirect.clone(),
                })
            })
            .collect();
        PluginShellResponse {
            accepted: true,
            operation_id: None,
            launches,
            picker: vec![],
            active_flows,
        }
    }
    fn callback(&self, raw: &str) -> bool {
        let Ok(u) = url::Url::parse(raw) else {
            return false;
        };
        let Some(redirect) = self.redirect.lock().unwrap().clone() else {
            return false;
        };
        let mut route = u.clone();
        route.set_query(None);
        route.set_fragment(None);
        if route.as_str() != redirect || u.fragment().is_some() {
            return false;
        }
        let pairs: Vec<_> = u.query_pairs().collect();
        // Reject duplicated identity/response fields instead of choosing a value.
        if ["state", "code", "error", "iss"]
            .iter()
            .any(|name| pairs.iter().filter(|(k, _)| k == name).count() > 1)
        {
            return false;
        }
        let get = |key: &str| {
            pairs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.to_string())
        };
        let Some(state) = get("state").filter(|s| !s.is_empty()) else {
            return false;
        };
        let code = get("code").filter(|s| !s.is_empty());
        let error = get("error").filter(|s| !s.is_empty());
        if code.is_some() == error.is_some() {
            return false;
        }
        let mut flows = self.flows.lock().unwrap();
        let Some((_, flow)) = flows.iter_mut().find(|(_, f)| {
            f.admission.is_current()
                && f.expires > Instant::now()
                && f.state.expose_secret() == state
        }) else {
            return false;
        };
        if let Some(previous) = &flow.callback_url {
            return previous.expose_secret() == raw;
        }
        let sent = flow
            .sender
            .try_send(BrowserCompletion::Callback(OAuthCallbackFields {
                state: AuthSecretString::new(state),
                code: code.map(AuthSecretString::new),
                issuer: get("iss"),
                error,
            }))
            .is_ok();
        if sent {
            flow.callback_url = Some(AuthSecretString::new(raw));
        }
        sent
    }
    fn browser_result(&self, id: &str, cancelled: bool) -> bool {
        self.flows
            .lock()
            .unwrap()
            .get(id)
            .filter(|f| f.admission.is_current())
            .is_some_and(|f| {
                f.sender
                    .try_send(if cancelled {
                        BrowserCompletion::Cancelled
                    } else {
                        BrowserCompletion::Failed
                    })
                    .is_ok()
            })
    }
}
impl McpOAuthShell for NativeOAuthShell {
    fn prepare(&self) -> anyhow::Result<String> {
        self.redirect
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("native_callback_unavailable"))
    }
    fn authorize(
        &self,
        event: &OAuthPresentation,
        relay: OAuthCallbackRelay,
        admission: OAuthBrowserAdmission,
    ) -> OAuthBrowserEffectResult {
        let (Some(flow), Some(raw)) = (&event.flow_id, &event.authorization_url) else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        let Ok(url) = url::Url::parse(raw.expose_secret()) else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        let states: Vec<_> = url
            .query_pairs()
            .filter(|(k, _)| k == "state")
            .map(|(_, v)| v.into_owned())
            .collect();
        if states.len() != 1 || states[0].is_empty() || !admission.claim() {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        }
        let Some(core) = self.core.upgrade() else {
            return OAuthBrowserEffectResult::CallbackUnavailable;
        };
        let cancel = core.mcp_oauth_cancel_relay(event);
        drop(core);
        let (tx, rx) = mpsc::sync_channel(1);
        let expires = Instant::now() + Duration::from_secs(600);
        {
            let mut flows = self.flows.lock().unwrap();
            if !flows.contains_key(flow) && flows.len() >= 8 {
                admission.complete();
                return OAuthBrowserEffectResult::BrowserUnavailable;
            }
            flows.insert(
                flow.clone(),
                Flow {
                    relay: relay.clone(),
                    callback_url: None,
                    event: event.clone(),
                    admission: admission.clone(),
                    state: AuthSecretString::new(states[0].clone()),
                    delivered: false,
                    sender: tx,
                    expires,
                },
            );
        }
        // Existing shared OAuth browser worker owns this wait, never the JS
        // thread or Gateway dispatcher. Native callback only sends to its inbox.
        while admission.is_current() && Instant::now() < expires {
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(BrowserCompletion::Callback(fields)) => {
                    let accepted = relay(fields);
                    admission.complete();
                    return if accepted {
                        OAuthBrowserEffectResult::Opened
                    } else {
                        OAuthBrowserEffectResult::CallbackUnavailable
                    };
                }
                Ok(BrowserCompletion::Cancelled) => {
                    let _ = cancel();
                    admission.complete();
                    self.release(flow);
                    return OAuthBrowserEffectResult::Opened;
                }
                Ok(BrowserCompletion::Failed) => {
                    admission.complete();
                    return OAuthBrowserEffectResult::BrowserUnavailable;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => break,
            }
        }
        admission.complete();
        self.release(flow);
        OAuthBrowserEffectResult::CallbackUnavailable
    }
    fn retry_authorize(&self, event: &OAuthPresentation) -> OAuthBrowserEffectResult {
        let retry = {
            let flows = self.flows.lock().unwrap();
            event
                .flow_id
                .as_ref()
                .and_then(|id| flows.get(id))
                .map(|f| (f.relay.clone(), f.admission.clone()))
        };
        match retry {
            Some((relay, admission)) => self.authorize(event, relay, admission),
            None => OAuthBrowserEffectResult::CallbackUnavailable,
        }
    }
    fn release(&self, flow: &str) {
        self.flows.lock().unwrap().remove(flow);
    }
    fn shutdown(&self) {
        self.flows.lock().unwrap().clear();
    }
}
pub(crate) fn execute(
    runtime: &crate::ClientFfiRuntime,
    request: PluginShellRequest,
) -> Result<PluginShellResponse, String> {
    match request {
        PluginShellRequest::ConfigureOAuth { redirect_uri } => {
            runtime.native_oauth.configure(redirect_uri)?;
            runtime
                .core
                .set_mcp_oauth_shell(runtime.native_oauth.clone());
            Ok(PluginShellResponse::accepted(true))
        }
        PluginShellRequest::ComposerPicker {
            thread_id,
            draft_id,
            query,
        } => Ok(PluginShellResponse {
            picker: runtime
                .core
                .composer_plugin_picker(&thread_id, draft_id, &query),
            ..PluginShellResponse::accepted(true)
        }),
        PluginShellRequest::PollOAuth => Ok(runtime.native_oauth.poll()),
        PluginShellRequest::OAuthCallback { url } => Ok(PluginShellResponse::accepted(
            runtime.native_oauth.callback(&url),
        )),
        PluginShellRequest::BrowserResult { flow_id, cancelled } => Ok(
            PluginShellResponse::accepted(runtime.native_oauth.browser_result(&flow_id, cancelled)),
        ),
        PluginShellRequest::StartArchive {
            workspace_id,
            expected_connection,
            target,
            file_uri,
        } => {
            use pioneer_client::skills::operations::SkillUploadTarget;
            if !matches!(
                target,
                SkillUploadTarget::PluginPreview
                    | SkillUploadTarget::PluginUpdatePreview { .. }
                    | SkillUploadTarget::Update { .. }
            ) {
                return Err("plugin_preview_target_required".into());
            }
            let path = url::Url::parse(&file_uri)
                .ok()
                .filter(|u| u.scheme() == "file" && u.query().is_none() && u.fragment().is_none())
                .and_then(|u| u.to_file_path().ok())
                .ok_or_else(|| "archive_must_be_copied_to_native_cache".to_owned())?;
            let operation = runtime
                .core
                .start_skill_upload_bound(&workspace_id, target, path, Some(expected_connection))
                .map_err(|_| "plugin_archive_upload_unavailable".to_owned())?;
            let id = operation.id();
            let mut handles = runtime.plugin_uploads.lock().unwrap();
            handles.retain(|(w, _), _| w != &workspace_id);
            handles.insert((workspace_id, id), operation);
            Ok(PluginShellResponse {
                operation_id: Some(id),
                ..PluginShellResponse::accepted(true)
            })
        }
        PluginShellRequest::CancelUpload {
            workspace_id,
            operation_id,
        } => {
            runtime
                .plugin_uploads
                .lock()
                .unwrap()
                .remove(&(workspace_id.clone(), operation_id));
            runtime
                .core
                .cancel_skill_upload(&workspace_id, operation_id);
            Ok(PluginShellResponse::accepted(true))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // NOT_RUN / NOT_COMPILED: callback parsing/relay, no OAuth provider or app.
    #[test]
    fn callbacks_require_exact_live_route_state_and_single_response() {
        let shell = NativeOAuthShell::default();
        shell
            .configure("pioneer:///oauth/mcp/callback".into())
            .unwrap();
        let admission = OAuthBrowserAdmission::default();
        let (sender, receiver) = mpsc::sync_channel(1);
        shell.flows.lock().unwrap().insert(
            "flow".into(),
            Flow {
                event: OAuthPresentation {
                    workspace_id: "original".into(),
                    server_id: "installation".into(),
                    name: "name".into(),
                    scope_kind: pioneer_protocol::McpScopeKind::Workspace,
                    flow_id: Some("flow".into()),
                    state: pioneer_protocol::McpOAuthState::AwaitingCallback,
                    authorization_url: None,
                    diagnostic: None,
                },
                relay: Arc::new(|_| true),
                callback_url: None,
                admission: admission.clone(),
                state: AuthSecretString::new("nonce"),
                delivered: true,
                sender,
                expires: Instant::now() + Duration::from_secs(30),
            },
        );
        for bad in [
            "pioneer:///oauth/mcp/callback?state=foreign&code=a",
            "pioneer://host/oauth/mcp/callback?state=nonce&code=a",
            "pioneer:///oauth/mcp/callback?state=nonce&state=nonce&code=a",
            "pioneer:///oauth/mcp/callback?state=nonce&code=a&error=denied",
        ] {
            assert!(!shell.callback(bad));
        }
        let valid = "pioneer:///oauth/mcp/callback?state=nonce&code=opaque&iss=https%3A%2F%2Fissuer.example";
        assert!(shell.callback(valid));
        assert!(shell.callback(valid)); // duplicate OS delivery, one exchange
        let BrowserCompletion::Callback(fields) = receiver.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(fields.issuer.as_deref(), Some("https://issuer.example"));
        assert!(receiver.try_recv().is_err());
        admission.retire();
        assert!(!shell.callback(valid));
        shell.shutdown();
        assert!(shell.poll().active_flows.is_empty());
    }
}
