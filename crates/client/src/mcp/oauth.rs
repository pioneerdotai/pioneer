//! Shared flow reduction and native-effect boundary. OS listeners and browser
//! launchers are supplied by the shell; callback exchange belongs to Gateway.
use crate::core::*;
use pioneer_protocol::{
    GatewayNotification, McpOAuthAction, McpOAuthParams, McpOAuthResponse, McpOAuthState,
    constants::methods,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

pub use pioneer_protocol::McpOAuthNotification as OAuthPresentation;
/// Shared precedence for Desktop and other management shells. A current cleanup
/// snapshot supersedes completed consent, while a newer live operation keeps its
/// presentation until its own current notification/retirement arrives.
pub fn effective_oauth_management_state(
    presentation: Option<McpOAuthState>,
    management: Option<McpOAuthState>,
    callback_unavailable: bool,
) -> Option<McpOAuthState> {
    if management == Some(McpOAuthState::CleanupRequired)
        && !callback_unavailable
        && presentation.is_none_or(completed_consent_presentation)
    {
        management
    } else {
        presentation.or(management)
    }
}
fn completed_consent_presentation(state: McpOAuthState) -> bool {
    matches!(
        state,
        McpOAuthState::Denied
            | McpOAuthState::Cancelled
            | McpOAuthState::TimedOut
            | McpOAuthState::Failed
            | McpOAuthState::Authorized
            | McpOAuthState::AuthRequired
            | McpOAuthState::InsufficientScope
            | McpOAuthState::Idle
    )
}
pub struct OAuthCallbackFields {
    pub state: pioneer_protocol::AuthSecretString,
    pub code: Option<pioneer_protocol::AuthSecretString>,
    pub issuer: Option<String>,
    pub error: Option<String>,
}
pub type OAuthCallbackRelay = Arc<dyn Fn(OAuthCallbackFields) -> bool + Send + Sync>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OAuthBrowserEffectResult {
    Opened,
    BrowserUnavailable,
    CallbackUnavailable,
}
/// Short launch admission; retirement never waits for an OS browser launcher.
/// A launch admitted before retirement cannot be recalled by the OS.
#[derive(Clone, Default)]
pub struct OAuthBrowserAdmission(Arc<AtomicU8>, Option<u64>);
impl OAuthBrowserAdmission {
    fn for_origin(connection: Option<u64>) -> Self {
        Self(Arc::default(), connection)
    }
    /// Immutable flow origin for OS dismissal. Mobile uses it with the native
    /// Cancel relay; desktop shell adapters can adopt the same bound relay.
    pub fn origin_connection(&self) -> Option<u64> {
        self.1
    }
    pub fn claim(&self) -> bool {
        self.0
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    pub fn complete(&self) {
        let _ = self
            .0
            .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire);
    }
    pub fn retire(&self) {
        self.0.store(2, Ordering::Release);
    }
    pub fn is_current(&self) -> bool {
        self.0.load(Ordering::Acquire) != 2
    }
}
/// Safe local preparation causes; never carries raw configuration or OS errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OAuthPreparationError {
    InvalidCallbackPort,
    ConfigurationLoad,
    PortUnavailable,
}
impl OAuthPreparationError {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidCallbackPort => "oauth_callback_port_invalid",
            Self::ConfigurationLoad => "oauth_configuration_load_failed",
            Self::PortUnavailable => "oauth_callback_port_unavailable",
        }
    }
}
impl std::fmt::Display for OAuthPreparationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}
impl std::error::Error for OAuthPreparationError {}

pub trait McpOAuthShell: Send + Sync {
    /// Immutable local configuration failure, used to explain an install's
    /// protected challenge without sending configuration details to Gateway.
    fn configuration_error(&self) -> Option<OAuthPreparationError> {
        None
    }

    /// Returns only after the shell callback route is ready. Must use the same URI
    /// across restarts so persisted client registrations remain valid.
    fn prepare(&self) -> anyhow::Result<String>;
    /// Open at most one browser tab and relay a parsed callback asynchronously.
    /// Browser failure retains the fallback URL; listener failure forbids opening it.
    fn authorize(
        &self,
        event: &OAuthPresentation,
        relay: OAuthCallbackRelay,
        admission: OAuthBrowserAdmission,
    ) -> OAuthBrowserEffectResult;
    fn retry_authorize(&self, _event: &OAuthPresentation) -> OAuthBrowserEffectResult {
        OAuthBrowserEffectResult::CallbackUnavailable
    }
    fn release(&self, flow_id: &str);
    fn shutdown(&self);
}
#[derive(Default)]
pub(crate) struct OAuthController {
    owner: std::sync::Weak<ClientCore>,
    workers: Vec<std::thread::JoinHandle<()>>,
    browser_busy: HashSet<String>,
    retired_presentations: std::collections::VecDeque<String>,
    shell: Option<Arc<dyn McpOAuthShell>>,
    flows: HashMap<String, (std::time::Instant, OAuthBrowserAdmission)>,
    presentations: HashMap<(String, String), (OAuthPresentation, bool)>,
}
impl Drop for OAuthController {
    fn drop(&mut self) {
        for (_, admission) in self.flows.values() {
            admission.retire();
        }
        if let Some(shell) = &self.shell {
            shell.shutdown();
        }
        for worker in self.workers.drain(..) {
            if worker.thread().id() != std::thread::current().id() {
                let _ = worker.join();
            }
        }
    }
}
impl ClientCore {
    pub fn set_mcp_oauth_shell(self: &Arc<Self>, shell: Arc<dyn McpOAuthShell>) {
        let mut owner = self.mcp_oauth.lock().expect("MCP OAuth owner poisoned");
        owner.owner = Arc::downgrade(self);
        owner.shell = Some(shell);
    }
    pub(crate) fn prepare_mcp_oauth(&self) -> anyhow::Result<Option<String>> {
        let shell = self
            .mcp_oauth
            .lock()
            .expect("MCP OAuth owner poisoned")
            .shell
            .clone();
        shell.map(|s| s.prepare()).transpose()
    }
    pub fn mcp_oauth_presentation(
        &self,
        workspace: &str,
        server: &str,
    ) -> Option<(OAuthPresentation, bool)> {
        self.mcp_oauth
            .lock()
            .expect("MCP OAuth owner poisoned")
            .presentations
            .get(&(workspace.into(), server.into()))
            .cloned()
    }
    /// Shell browser dismissal uses the same native Cancel action, bound to
    /// the originating transport. Mobile consumes this relay; desktop browser
    /// adapters can use it when their OS reports session dismissal.
    pub fn mcp_oauth_cancel_relay(
        &self,
        event: &OAuthPresentation,
    ) -> Arc<dyn Fn() -> bool + Send + Sync> {
        self.mcp_oauth_cancel_relay_bound(event, self.provider_runtime_epoch().2)
    }
    /// Same native Cancel operation with the admitted browser flow's origin.
    /// Mobile consumes this; desktop OS dismissal adapters can migrate here.
    pub fn mcp_oauth_cancel_relay_bound(
        &self,
        event: &OAuthPresentation,
        connection: Option<u64>,
    ) -> Arc<dyn Fn() -> bool + Send + Sync> {
        let sender = self.transport_runtime().ws_command_sender();
        let event = event.clone();
        Arc::new(move || {
            let (Some(connection), Some(flow_id)) = (connection, event.flow_id.clone()) else {
                return false;
            };
            let params = McpOAuthParams {
                workspace_id: event.workspace_id.clone(),
                server_id: event.server_id.clone(),
                name: event.name.clone(),
                scope_kind: event.scope_kind,
                action: McpOAuthAction::Cancel { flow_id },
            };
            crate::rpc::send_json_rpc_request_typed::<McpOAuthResponse, _, _>(
                &sender.requests_for_connection(connection),
                methods::MCP_OAUTH,
                &params,
                std::time::Duration::from_secs(60),
            )
            .is_ok()
        })
    }
    pub(crate) fn retry_mcp_oauth_browser(
        &self,
        workspace: &str,
        server: &str,
    ) -> anyhow::Result<()> {
        self.retry_mcp_oauth_browser_bound(workspace, server, self.provider_runtime_epoch())
    }
    pub(crate) fn retry_mcp_oauth_browser_bound(
        &self,
        workspace: &str,
        server: &str,
        epoch: (u64, u64, Option<u64>),
    ) -> anyhow::Result<()> {
        let key = (workspace.to_owned(), server.to_owned());
        let (shell, event, admission) = {
            let owner = self.mcp_oauth.lock().expect("MCP OAuth owner poisoned");
            anyhow::ensure!(self.provider_runtime_epoch() == epoch, "connection_changed");
            let shell = owner
                .shell
                .clone()
                .ok_or_else(|| anyhow::anyhow!("oauth_shell_unavailable"))?;
            let (event, failed) = owner
                .presentations
                .get(&key)
                .ok_or_else(|| anyhow::anyhow!("oauth_flow_unavailable"))?;
            anyhow::ensure!(
                event.state == McpOAuthState::AwaitingCallback && *failed,
                "oauth_flow_unavailable"
            );
            let (_, admission) = event
                .flow_id
                .as_ref()
                .and_then(|flow| owner.flows.get(flow))
                .ok_or_else(|| anyhow::anyhow!("oauth_flow_unavailable"))?;
            (shell, event.clone(), admission.clone())
        };
        self.schedule_mcp_browser_effect(shell, event, admission, epoch, None)
    }

    /// Only this dedicated, bounded owner waits for shell/OS effects. Neither
    /// the Gateway dispatcher nor the MCP action worker joins a browser call.
    fn schedule_mcp_browser_effect(
        &self,
        shell: Arc<dyn McpOAuthShell>,
        event: OAuthPresentation,
        admission: OAuthBrowserAdmission,
        epoch: (u64, u64, Option<u64>),
        relay: Option<OAuthCallbackRelay>,
    ) -> anyhow::Result<()> {
        let flow = event
            .flow_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("oauth_flow_unavailable"))?;
        let mut owner = self.mcp_oauth.lock().expect("MCP OAuth owner poisoned");
        anyhow::ensure!(
            admission.is_current() && epoch == self.provider_runtime_epoch(),
            "oauth_flow_unavailable"
        );
        anyhow::ensure!(!owner.browser_busy.contains(&flow), "oauth_browser_pending");
        let workers = std::mem::take(&mut owner.workers);
        for worker in workers {
            if worker.is_finished() {
                let _ = worker.join();
            } else {
                owner.workers.push(worker);
            }
        }
        anyhow::ensure!(owner.workers.len() < 8, "oauth_browser_capacity");
        let weak = owner.owner.clone();
        anyhow::ensure!(weak.strong_count() > 0, "oauth_shell_unavailable");
        owner.browser_busy.insert(flow.clone());
        let worker_flow = flow.clone();
        let result = std::thread::Builder::new()
            .name("client-mcp-oauth-browser".into())
            .spawn(move || {
                let effect = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if !admission.is_current()
                        || !weak
                            .upgrade()
                            .is_some_and(|core| core.provider_runtime_epoch() == epoch)
                    {
                        return OAuthBrowserEffectResult::CallbackUnavailable;
                    }
                    match relay {
                        Some(relay) => shell.authorize(&event, relay, admission.clone()),
                        None => shell.retry_authorize(&event),
                    }
                }))
                .unwrap_or(OAuthBrowserEffectResult::CallbackUnavailable);
                if let Some(core) = weak.upgrade() {
                    core.reduce_mcp_browser_effect(
                        shell,
                        event,
                        admission,
                        epoch,
                        effect,
                        &worker_flow,
                    );
                } else {
                    shell.release(&worker_flow);
                }
            });
        match result {
            Ok(worker) => {
                owner.workers.push(worker);
                Ok(())
            }
            Err(_) => {
                owner.browser_busy.remove(&flow);
                anyhow::bail!("oauth_browser_unavailable")
            }
        }
    }

    fn reduce_mcp_browser_effect(
        &self,
        shell: Arc<dyn McpOAuthShell>,
        event: OAuthPresentation,
        admission: OAuthBrowserAdmission,
        epoch: (u64, u64, Option<u64>),
        effect: OAuthBrowserEffectResult,
        flow: &str,
    ) {
        let key = (event.workspace_id.clone(), event.server_id.clone());
        let mut owner = self.mcp_oauth.lock().expect("MCP OAuth owner poisoned");
        owner.browser_busy.remove(flow);
        if !admission.is_current()
            || epoch != self.provider_runtime_epoch()
            || !owner.presentations.get(&key).is_some_and(|(current, _)| {
                current.flow_id == event.flow_id && current.state == McpOAuthState::AwaitingCallback
            })
        {
            drop(owner);
            shell.release(flow);
            return;
        }
        let mut presentation = event.clone();
        if effect == OAuthBrowserEffectResult::CallbackUnavailable {
            admission.retire();
            presentation.state = McpOAuthState::Failed;
            presentation.authorization_url = None;
            presentation.diagnostic = Some("oauth_callback_unavailable".into());
        }
        owner.presentations.insert(
            key,
            (
                presentation,
                effect == OAuthBrowserEffectResult::BrowserUnavailable,
            ),
        );
        drop(owner);
        if effect == OAuthBrowserEffectResult::CallbackUnavailable {
            shell.release(flow);
        }
        self.publish_mcp_oauth_revision(&event);
        self.refresh_mcp(&event.workspace_id);
        self.refresh_plugin_publication(&event.workspace_id);
    }
    pub(crate) fn forget_mcp_oauth(&self, workspace: &str, server: &str) {
        let mut owner = self.mcp_oauth.lock().expect("MCP OAuth owner poisoned");
        let flow = owner
            .presentations
            .remove(&(workspace.into(), server.into()))
            .and_then(|(event, _)| event.flow_id);
        if let Some((_, admission)) = flow.as_ref().and_then(|flow| owner.flows.get(flow)) {
            admission.retire();
        }
        let shell = owner.shell.clone();
        drop(owner);
        if let (Some(shell), Some(flow)) = (shell, flow) {
            shell.release(&flow);
        }
    }
    pub(crate) fn fence_mcp_oauth(&self) {
        let mut owner = self.mcp_oauth.lock().expect("MCP OAuth owner poisoned");
        for (_, admission) in owner.flows.values() {
            admission.retire();
        }
        owner.flows.clear();
        owner.browser_busy.clear();
        owner.presentations.clear();
        owner.retired_presentations.clear();
        // Shutdown only drops listener state (no launcher, network or join).
        // Keep the short controller admission closed until the old shell is
        // fenced, so a new epoch cannot register its relay before this shutdown.
        if let Some(shell) = &owner.shell {
            shell.shutdown();
        }
    }
    pub(crate) fn stop_mcp_oauth(&self) {
        let mut owner = self.mcp_oauth.lock().expect("MCP OAuth owner poisoned");
        for (_, admission) in owner.flows.values() {
            admission.retire();
        }
        let shell = owner.shell.take();
        owner.flows.clear();
        owner.browser_busy.clear();
        owner.presentations.clear();
        owner.retired_presentations.clear();
        drop(owner);
        if let Some(shell) = shell {
            shell.shutdown();
        }
    }
    fn publish_mcp_oauth_revision(&self, event: &OAuthPresentation) {
        let scope = ClientScope::McpAction {
            workspace_id: event.workspace_id.clone(),
            target: event.server_id.clone(),
        };
        let revision = self
            .snapshot(&scope)
            .map_or(0, |p| p.revisions().scoped().get())
            + 1;
        let mut publication = self
            .mcp_action_snapshot(&event.workspace_id, &event.server_id)
            .map(|current| (*current).clone())
            .unwrap_or(super::operations::McpActionPublication {
                operation_id: 0,
                revision,
                state: super::operations::McpActionState::Succeeded,
                kind: super::operations::McpActionKind::SignIn,
                field_error: None,
            });
        publication.revision = revision;
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            Arc::new(publication),
            vec![],
        );
    }
    pub(crate) fn observe_mcp_oauth_notification(
        &self,
        notification: &GatewayNotification,
    ) -> bool {
        if let GatewayNotification::McpChanged(changed) = notification {
            let mut owner = self.mcp_oauth.lock().expect("MCP OAuth owner poisoned");
            let removed = owner
                .presentations
                .iter()
                .filter(|((workspace, _), (event, _))| {
                    workspace == &changed.workspace_id
                        && changed.changed.iter().any(|item| {
                            item.name == event.name
                                && matches!(
                                    item.action,
                                    pioneer_protocol::McpChangedAction::Uninstall
                                )
                        })
                })
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let mut releases = Vec::new();
            for key in removed {
                if let Some((event, _)) = owner.presentations.remove(&key) {
                    if let Some(flow) = event.flow_id {
                        if let Some((_, admission)) = owner.flows.get(&flow) {
                            admission.retire();
                        }
                        releases.push(flow);
                    }
                }
            }
            let shell = owner.shell.clone();
            drop(owner);
            if let Some(shell) = shell {
                for flow in releases {
                    shell.release(&flow);
                }
            }
            return false;
        }
        let GatewayNotification::McpOAuthChanged(event) = notification else {
            return false;
        };
        if !self.capability_management_allowed(&event.workspace_id) {
            return true;
        }
        let key = (event.workspace_id.clone(), event.server_id.clone());
        let mut owner = self.mcp_oauth.lock().expect("MCP OAuth owner poisoned");
        // The Gateway knows preparation failed, but the immutable device shell
        // owns its configuration cause. Preserve all notification fencing below.
        let mut local_event = event.clone();
        if event.diagnostic.as_deref() == Some("oauth_callback_preparation_failed") {
            if let Some(reason) = owner
                .shell
                .as_ref()
                .and_then(|shell| shell.configuration_error())
            {
                local_event.diagnostic = Some(reason.code().into());
            }
        }
        let event = &local_event;
        owner.flows.retain(|_, (deadline, admission)| {
            if *deadline > std::time::Instant::now() {
                true
            } else {
                admission.retire();
                false
            }
        });
        if event.state == McpOAuthState::Retired {
            let Some(flow) = event.flow_id.as_ref() else {
                return true;
            };
            if !owner.retired_presentations.contains(flow) {
                if owner.retired_presentations.len() == 1024 {
                    owner.retired_presentations.pop_front();
                }
                owner.retired_presentations.push_back(flow.clone());
            }
            if let Some((_, admission)) = owner.flows.get(flow) {
                admission.retire();
            }
            let removed = owner
                .presentations
                .get(&key)
                .is_some_and(|(current, _)| current.flow_id.as_ref() == Some(flow));
            if removed {
                owner.presentations.remove(&key);
            }
            let shell = owner.shell.clone();
            drop(owner);
            if let Some(shell) = shell {
                shell.release(flow);
            }
            if removed {
                self.publish_mcp_oauth_revision(event);
                self.refresh_mcp(&event.workspace_id);
            }
            return true;
        }
        if event
            .flow_id
            .as_ref()
            .is_some_and(|flow| owner.retired_presentations.contains(flow))
        {
            return true;
        }
        if event.state == McpOAuthState::AwaitingCallback
            && event
                .flow_id
                .as_ref()
                .is_some_and(|id| owner.flows.contains_key(id))
        {
            return true;
        }
        if matches!(
            event.state,
            McpOAuthState::Cancelled
                | McpOAuthState::TimedOut
                | McpOAuthState::Failed
                | McpOAuthState::Denied
                | McpOAuthState::Authorized
                | McpOAuthState::Resolving
                | McpOAuthState::CleanupRequired
        ) && event.flow_id.is_some()
            && owner.presentations.get(&key).is_some_and(|(current, _)| {
                current.flow_id.is_some()
                    && current.flow_id != event.flow_id
                    && !(event.state == McpOAuthState::CleanupRequired
                        && completed_consent_presentation(current.state)
                        && current.diagnostic.as_deref() != Some("oauth_callback_unavailable"))
            })
        {
            if event.state == McpOAuthState::CleanupRequired {
                if let Some(flow) = &event.flow_id {
                    if owner.retired_presentations.len() == 1024 {
                        owner.retired_presentations.pop_front();
                    }
                    if !owner.retired_presentations.contains(flow) {
                        owner.retired_presentations.push_back(flow.clone());
                    }
                }
            }
            return true;
        }
        // A new live consent supersedes a previously observed cleanup operation
        // for the whole session, even after the new consent later becomes Denied.
        if matches!(
            event.state,
            McpOAuthState::Preparing | McpOAuthState::AwaitingCallback
        ) && let Some((previous, _)) = owner.presentations.get(&key)
            && previous.state == McpOAuthState::CleanupRequired
            && previous.flow_id != event.flow_id
            && let Some(flow) = previous.flow_id.clone()
        {
            if owner.retired_presentations.len() == 1024 {
                owner.retired_presentations.pop_front();
            }
            owner.retired_presentations.push_back(flow);
        }
        if let Some((previous, _)) = owner.presentations.get(&key) {
            if previous.flow_id != event.flow_id {
                if let Some((_, admission)) = previous
                    .flow_id
                    .as_ref()
                    .and_then(|flow| owner.flows.get(flow))
                {
                    admission.retire();
                }
            }
        }
        owner
            .presentations
            .insert(key.clone(), (event.clone(), false));
        let Some(flow) = event.flow_id.clone() else {
            drop(owner);
            self.publish_mcp_oauth_revision(event);
            self.refresh_mcp(&event.workspace_id);
            return true;
        };
        if event.state == McpOAuthState::CleanupRequired {
            // Management cleanup has no browser/listener ownership of its own.
            // Reduce and refresh even in a shell without an OAuth launcher.
            drop(owner);
            self.publish_mcp_oauth_revision(event);
            self.refresh_mcp(&event.workspace_id);
            return true;
        }
        let Some(shell) = owner.shell.clone() else {
            return true;
        };
        if event.state != McpOAuthState::AwaitingCallback {
            if matches!(
                event.state,
                McpOAuthState::Authorized
                    | McpOAuthState::Resolving
                    | McpOAuthState::Denied
                    | McpOAuthState::Cancelled
                    | McpOAuthState::TimedOut
                    | McpOAuthState::Failed
            ) {
                if let Some((_, admission)) = owner.flows.get(&flow) {
                    admission.retire();
                }
                drop(owner);
                shell.release(&flow);
            } else {
                drop(owner);
            }
            self.publish_mcp_oauth_revision(event);
            self.refresh_mcp(&event.workspace_id);
            return true;
        }
        if owner.flows.contains_key(&flow) {
            return true;
        }
        let epoch = self.provider_runtime_epoch();
        let admission = OAuthBrowserAdmission::for_origin(epoch.2);
        owner.flows.insert(
            flow.clone(),
            (
                std::time::Instant::now() + std::time::Duration::from_secs(600),
                admission.clone(),
            ),
        );
        let sender = self.transport_runtime().ws_command_sender();
        let event_for_callback = event.clone();
        drop(owner);
        let callback_admission = admission.clone();
        let relay: OAuthCallbackRelay = Arc::new(move |callback| {
            if !callback_admission.is_current() {
                return false;
            }
            // The transport request is fenced to the originating connection.
            // Callback completion never logs or projects the code.
            let params = McpOAuthParams {
                workspace_id: event_for_callback.workspace_id.clone(),
                server_id: event_for_callback.server_id.clone(),
                name: event_for_callback.name.clone(),
                scope_kind: event_for_callback.scope_kind,
                action: McpOAuthAction::Callback {
                    flow_id: flow.clone(),
                    state: callback.state,
                    code: callback.code,
                    issuer: callback.issuer,
                    error: callback.error,
                },
            };
            let Some(connection) = epoch.2 else {
                return false;
            };
            crate::rpc::send_json_rpc_request_typed::<McpOAuthResponse, _, _>(
                &sender.requests_for_connection(connection),
                methods::MCP_OAUTH,
                &params,
                std::time::Duration::from_secs(60),
            )
            .is_ok()
        });
        let scheduled = self.schedule_mcp_browser_effect(
            shell.clone(),
            event.clone(),
            admission.clone(),
            epoch,
            Some(relay),
        );
        if scheduled.is_err() {
            self.reduce_mcp_browser_effect(
                shell,
                event.clone(),
                admission,
                epoch,
                OAuthBrowserEffectResult::CallbackUnavailable,
                event.flow_id.as_deref().unwrap_or_default(),
            );
        }
        self.publish_mcp_oauth_revision(event);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    #[derive(Default)]
    struct Shell {
        open: AtomicUsize,
        release: AtomicUsize,
        shutdown: AtomicUsize,
        callback_unavailable: AtomicBool,
    }
    impl McpOAuthShell for Shell {
        fn prepare(&self) -> anyhow::Result<String> {
            Ok("http://127.0.0.1:37643/oauth/mcp/callback".into())
        }
        fn authorize(
            &self,
            _: &OAuthPresentation,
            _: OAuthCallbackRelay,
            admission: OAuthBrowserAdmission,
        ) -> OAuthBrowserEffectResult {
            if !admission.claim() {
                return OAuthBrowserEffectResult::CallbackUnavailable;
            }
            admission.complete();
            if self.callback_unavailable.load(Ordering::SeqCst) {
                return OAuthBrowserEffectResult::CallbackUnavailable;
            }
            self.open.fetch_add(1, Ordering::SeqCst);
            OAuthBrowserEffectResult::BrowserUnavailable
        }
        fn retry_authorize(&self, _: &OAuthPresentation) -> OAuthBrowserEffectResult {
            if self.callback_unavailable.load(Ordering::SeqCst) {
                return OAuthBrowserEffectResult::CallbackUnavailable;
            }
            self.open.fetch_add(1, Ordering::SeqCst);
            OAuthBrowserEffectResult::Opened
        }
        fn release(&self, _: &str) {
            self.release.fetch_add(1, Ordering::SeqCst);
        }
        fn shutdown(&self) {
            self.shutdown.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn wait_browser(core: &ClientCore) {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(3);
        loop {
            let owner = core.mcp_oauth.lock().unwrap();
            if owner.browser_busy.is_empty()
                && owner.workers.iter().all(|worker| worker.is_finished())
            {
                return;
            }
            drop(owner);
            assert!(
                std::time::Instant::now() < until,
                "owned browser reduction did not finish"
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
    fn event(state: McpOAuthState) -> OAuthPresentation {
        OAuthPresentation {
            workspace_id: "workspace".into(),
            server_id: "server".into(),
            name: "server".into(),
            scope_kind: pioneer_protocol::McpScopeKind::Workspace,
            flow_id: Some("flow".into()),
            state,
            authorization_url: Some(pioneer_protocol::AuthSecretString::new(
                "https://provider.test/authorize?state=state",
            )),
            diagnostic: None,
        }
    }
    #[test]
    fn blocked_browser_does_not_block_projection_or_epoch_retirement() {
        struct BlockingShell {
            entered: std::sync::mpsc::SyncSender<()>,
            release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
            launched: AtomicUsize,
        }
        impl McpOAuthShell for BlockingShell {
            fn prepare(&self) -> anyhow::Result<String> {
                Ok("http://127.0.0.1/callback".into())
            }
            fn authorize(
                &self,
                _: &OAuthPresentation,
                _: OAuthCallbackRelay,
                admission: OAuthBrowserAdmission,
            ) -> OAuthBrowserEffectResult {
                self.entered.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(3))
                    .unwrap();
                if !admission.claim() {
                    return OAuthBrowserEffectResult::CallbackUnavailable;
                }
                self.launched.fetch_add(1, Ordering::SeqCst);
                admission.complete();
                OAuthBrowserEffectResult::Opened
            }
            fn release(&self, _: &str) {}
            fn shutdown(&self) {}
        }
        let core = crate::catalog_test_support::client();
        let (entered, observed) = std::sync::mpsc::sync_channel(1);
        let (release, resumed) = std::sync::mpsc::sync_channel(1);
        let shell = Arc::new(BlockingShell {
            entered,
            release: std::sync::Mutex::new(resumed),
            launched: AtomicUsize::new(0),
        });
        core.set_mcp_oauth_shell(shell.clone());
        let effect_core = core.clone();
        let effect = std::thread::spawn(move || {
            effect_core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
                event(McpOAuthState::AwaitingCallback),
            ))
        });
        observed
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let projection_core = core.clone();
        let (finished, completion) = std::sync::mpsc::sync_channel(1);
        let retirement = std::thread::spawn(move || {
            assert_eq!(
                projection_core
                    .mcp_oauth_presentation("workspace", "server")
                    .unwrap()
                    .0
                    .state,
                McpOAuthState::AwaitingCallback
            );
            projection_core.fence_mcp_oauth();
            finished.send(()).unwrap();
        });
        let retired_without_launcher = completion
            .recv_timeout(std::time::Duration::from_secs(1))
            .is_ok();
        release.send(()).unwrap();
        retirement.join().unwrap();
        effect.join().unwrap();
        assert!(
            retired_without_launcher,
            "projection and epoch retirement must not wait for launcher"
        );
        assert_eq!(shell.launched.load(Ordering::SeqCst), 0);
        assert!(core.mcp_oauth_presentation("workspace", "server").is_none());
        core.stop_mcp_oauth();
    }
    #[test]
    fn production_dispatcher_and_action_queue_retire_while_browser_is_blocked() {
        struct BlockingBrowser {
            calls: AtomicUsize,
            block: AtomicBool,
            entered: std::sync::mpsc::SyncSender<()>,
            release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
            released: AtomicUsize,
        }
        impl McpOAuthShell for BlockingBrowser {
            fn prepare(&self) -> anyhow::Result<String> {
                Ok("http://127.0.0.1/callback".into())
            }
            fn authorize(
                &self,
                _: &OAuthPresentation,
                _: OAuthCallbackRelay,
                admission: OAuthBrowserAdmission,
            ) -> OAuthBrowserEffectResult {
                if !admission.claim() {
                    return OAuthBrowserEffectResult::CallbackUnavailable;
                }
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.block.load(Ordering::SeqCst) {
                    self.entered.send(()).unwrap();
                    self.release
                        .lock()
                        .unwrap()
                        .recv_timeout(std::time::Duration::from_secs(3))
                        .unwrap();
                }
                admission.complete();
                OAuthBrowserEffectResult::BrowserUnavailable
            }
            fn retry_authorize(&self, _: &OAuthPresentation) -> OAuthBrowserEffectResult {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.entered.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(3))
                    .unwrap();
                OAuthBrowserEffectResult::Opened
            }
            fn release(&self, _: &str) {
                self.released.fetch_add(1, Ordering::SeqCst);
            }
            fn shutdown(&self) {}
        }
        struct CancelTransport(AtomicUsize);
        impl crate::rpc::JsonRpcRequestTransport for CancelTransport {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                reply: crate::rpc::JsonRpcResponseSender,
            ) -> Result<(), String> {
                let value: serde_json::Value = serde_json::from_str(&payload).unwrap();
                assert_eq!(value["params"]["action"]["kind"], "cancel");
                self.0.fetch_add(1, Ordering::SeqCst);
                reply
                    .send(Ok(serde_json::json!({"accepted":true})))
                    .unwrap();
                Ok(())
            }
        }
        let core = crate::catalog_test_support::client();
        core.accept_mcp_catalog_for_test(
            "workspace",
            crate::catalog_test_support::mcp(&["server"]),
        );
        let target = core.mcp_catalog_snapshot("workspace").unwrap().servers()[0]
            .id
            .clone();
        let (entered, observed) = std::sync::mpsc::sync_channel(4);
        let (release, resumed) = std::sync::mpsc::sync_channel(4);
        let browser = Arc::new(BlockingBrowser {
            calls: AtomicUsize::new(0),
            block: AtomicBool::new(true),
            entered,
            release: std::sync::Mutex::new(resumed),
            released: AtomicUsize::new(0),
        });
        core.set_mcp_oauth_shell(browser.clone());
        core.start_gateway_event_dispatcher();
        core.start_mcp_operation_controller();
        let transport = Arc::new(CancelTransport(AtomicUsize::new(0)));
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(transport.clone());
        let mut awaiting = event(McpOAuthState::AwaitingCallback);
        awaiting.server_id = target.clone();
        let ingress = |presentation: OAuthPresentation| {
            core.transport_runtime().inject_test_event(
                crate::transport::ws::GatewayWsEvent::Notification {
                    connection_id: 7,
                    notification: GatewayNotification::McpOAuthChanged(presentation),
                },
            )
        };
        ingress(awaiting.clone());
        observed
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        ingress(awaiting.clone());
        let mut terminal = awaiting.clone();
        terminal.state = McpOAuthState::Cancelled;
        terminal.authorization_url = None;
        ingress(terminal.clone());
        let until = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while core
            .mcp_oauth_presentation("workspace", &target)
            .unwrap()
            .0
            .state
            != McpOAuthState::Cancelled
        {
            assert!(
                std::time::Instant::now() < until,
                "real event dispatcher waited for browser"
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(browser.released.load(Ordering::SeqCst) > 0);
        assert_eq!(browser.calls.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        wait_browser(&core);
        assert_eq!(
            core.mcp_oauth_presentation("workspace", &target)
                .unwrap()
                .0
                .state,
            McpOAuthState::Cancelled
        );
        browser.block.store(false, Ordering::SeqCst);
        awaiting.flow_id = Some("retry-flow".into());
        ingress(awaiting.clone());
        wait_for_presentation(&core, &target, |(_, failed)| *failed);
        core.mcp_intent(
            "workspace",
            super::super::operations::McpIntent::RetryAuthorizationBrowser {
                server_id: target.clone(),
            },
        )
        .unwrap();
        observed
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while core
            .mcp_action_snapshot("workspace", &target)
            .unwrap()
            .state
            == super::super::operations::McpActionState::Pending
        {
            assert!(std::time::Instant::now() < until);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        core.mcp_intent(
            "workspace",
            super::super::operations::McpIntent::CancelAuthorization {
                server_id: target.clone(),
                flow_id: "retry-flow".into(),
            },
        )
        .unwrap();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while transport.0.load(Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < until,
                "Cancel queued behind manual launcher"
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        terminal.flow_id = awaiting.flow_id;
        ingress(terminal);
        wait_for_presentation(&core, &target, |(event, _)| {
            event.state == McpOAuthState::Cancelled
        });
        release.send(()).unwrap();
        wait_browser(&core);
        assert_eq!(
            core.mcp_oauth_presentation("workspace", &target)
                .unwrap()
                .0
                .state,
            McpOAuthState::Cancelled
        );
        assert_eq!(browser.calls.load(Ordering::SeqCst), 3);
        browser.block.store(true, Ordering::SeqCst);
        let mut next = event(McpOAuthState::AwaitingCallback);
        next.server_id = target.clone();
        next.flow_id = Some("disconnect-flow".into());
        ingress(next);
        observed
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        core.transport_runtime().inject_test_event(
            crate::transport::ws::GatewayWsEvent::Disconnected {
                connection_id: 7,
                endpoint_id: "test".into(),
                endpoint_name: "test".into(),
                endpoint_kind: crate::gateway::types::GatewayEndpointKind::Local,
                gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                    "http://127.0.0.1:9999",
                )
                .unwrap(),
                reason: "injected".into(),
            },
        );
        let until = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while core.mcp_oauth_presentation("workspace", &target).is_some() {
            assert!(
                std::time::Instant::now() < until,
                "connection retirement waited for launcher"
            );
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        release.send(()).unwrap();
        wait_browser(&core);
        assert!(core.mcp_oauth_presentation("workspace", &target).is_none());
        assert_eq!(browser.calls.load(Ordering::SeqCst), 4);
        core.shutdown();
    }
    fn wait_for_presentation(
        core: &ClientCore,
        target: &str,
        predicate: impl Fn(&(OAuthPresentation, bool)) -> bool,
    ) {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if core
                .mcp_oauth_presentation("workspace", target)
                .as_ref()
                .is_some_and(&predicate)
            {
                return;
            }
            assert!(std::time::Instant::now() < until);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
    #[test]
    fn unavailable_callback_never_offers_a_dead_signin_link() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        shell.callback_unavailable.store(true, Ordering::SeqCst);
        core.set_mcp_oauth_shell(shell.clone());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(event(
            McpOAuthState::AwaitingCallback,
        )));
        wait_browser(&core);
        let (presentation, failed_browser) =
            core.mcp_oauth_presentation("workspace", "server").unwrap();
        assert_eq!(presentation.state, McpOAuthState::Failed);
        assert!(presentation.authorization_url.is_none());
        assert!(!failed_browser);
        wait_browser(&core);
        assert_eq!(shell.open.load(Ordering::SeqCst), 0);
        core.stop_mcp_oauth();
    }
    #[test]
    fn duplicate_effects_preserve_browser_failure_fallback() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        let event = event(McpOAuthState::AwaitingCallback);
        for _ in 0..3 {
            assert!(
                core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
                    event.clone()
                ))
            );
        }
        wait_browser(&core);
        assert_eq!(shell.open.load(Ordering::SeqCst), 1);
        wait_browser(&core);
        assert!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .1
        );
        wait_browser(&core);
        assert!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .0
                .authorization_url
                .is_some()
        );
        core.stop_mcp_oauth();
    }
    // NOT_RUN / NOT_COMPILED. The retained presentation/flow deliberately
    // survives synthetic identity replacement. A stale retry opens no shell and
    // schedules no callback, even when the new Gateway copied the same flow ID.
    #[test]
    fn queued_retry_keeps_its_origin_at_browser_admission() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        let presentation = event(McpOAuthState::AwaitingCallback);
        let origin = core.provider_runtime_epoch();
        {
            let mut owner = core.mcp_oauth.lock().unwrap();
            owner
                .presentations
                .insert(("workspace".into(), "server".into()), (presentation, true));
            owner.flows.insert(
                "flow".into(),
                (
                    std::time::Instant::now() + std::time::Duration::from_secs(60),
                    OAuthBrowserAdmission::for_origin(origin.2),
                ),
            );
        }
        crate::catalog_test_support::switch_connection(&core, 8);
        assert_eq!(
            core.mcp_oauth.lock().unwrap().flows["flow"]
                .1
                .origin_connection(),
            origin.2
        );
        assert_eq!(
            core.retry_mcp_oauth_browser_bound("workspace", "server", origin)
                .unwrap_err()
                .to_string(),
            "connection_changed"
        );
        assert!(core.mcp_oauth.lock().unwrap().workers.is_empty());
        assert_eq!(shell.open.load(Ordering::SeqCst), 0);
        core.stop_mcp_oauth();
    }
    #[test]
    fn manual_browser_retry_requires_live_operation_and_updates_failure_projection() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(event(
            McpOAuthState::AwaitingCallback,
        )));
        wait_browser(&core);
        assert!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .1
        );
        wait_browser(&core);
        core.retry_mcp_oauth_browser("workspace", "server").unwrap();
        wait_browser(&core);
        assert!(
            !core
                .mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .1
        );
        wait_browser(&core);
        assert_eq!(shell.open.load(Ordering::SeqCst), 2);
        assert!(core.retry_mcp_oauth_browser("workspace", "server").is_err());
        core.fence_mcp_oauth();
        assert!(core.retry_mcp_oauth_browser("workspace", "server").is_err());
        wait_browser(&core);
        assert_eq!(shell.open.load(Ordering::SeqCst), 2);
        core.stop_mcp_oauth();
    }

    #[test]
    fn completed_flow_releases_listener_and_connection_fence_clears_urls() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(event(
            McpOAuthState::AwaitingCallback,
        )));
        let mut completed = event(McpOAuthState::Authorized);
        completed.authorization_url = None;
        wait_browser(&core);
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(completed));
        assert_eq!(shell.release.load(Ordering::SeqCst), 1);
        wait_browser(&core);
        assert!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .0
                .authorization_url
                .is_none()
        );
        core.fence_mcp_oauth();
        assert!(core.mcp_oauth_presentation("workspace", "server").is_none());
        assert_eq!(shell.shutdown.load(Ordering::SeqCst), 1);
        assert!(core.prepare_mcp_oauth().unwrap().is_some());
        core.stop_mcp_oauth();
    }
    #[test]
    fn resolving_releases_relay_and_preserves_actionable_uncertain_projection() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(event(
            McpOAuthState::AwaitingCallback,
        )));
        wait_browser(&core);
        let mut resolving = event(McpOAuthState::Resolving);
        resolving.authorization_url = None;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(resolving));
        assert_eq!(shell.release.load(Ordering::SeqCst), 1);
        let projection = core
            .mcp_oauth_presentation("workspace", "server")
            .unwrap()
            .0;
        assert_eq!(projection.state, McpOAuthState::Resolving);
        assert!(projection.authorization_url.is_none());
        assert!(core.retry_mcp_oauth_browser("workspace", "server").is_err());
        core.fence_mcp_oauth();
        assert!(core.mcp_oauth_presentation("workspace", "server").is_none());
        core.stop_mcp_oauth();
    }
    #[test]
    fn addressed_retirement_clears_old_presentation_without_touching_new_flow() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(event(
            McpOAuthState::AwaitingCallback,
        )));
        wait_browser(&core);
        let mut resolving = event(McpOAuthState::Resolving);
        resolving.authorization_url = None;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
            resolving.clone(),
        ));
        let mut retirement = resolving.clone();
        retirement.state = McpOAuthState::Retired;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
            retirement.clone(),
        ));
        assert!(core.mcp_oauth_presentation("workspace", "server").is_none());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
            resolving.clone(),
        ));
        assert!(
            core.mcp_oauth_presentation("workspace", "server").is_none(),
            "late old Resolving cannot restore precedence"
        );
        let mut replacement = event(McpOAuthState::Preparing);
        replacement.flow_id = Some("new-flow".into());
        replacement.authorization_url = None;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
            replacement.clone(),
        ));
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(retirement));
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(resolving));
        assert_eq!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .0
                .flow_id,
            replacement.flow_id
        );
        core.stop_mcp_oauth();
    }
    #[test]
    fn failed_clear_retains_new_cleanup_presentation_and_retires_only_matching_flow() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        let mut old = event(McpOAuthState::Resolving);
        old.authorization_url = None;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(old.clone()));
        old.state = McpOAuthState::Retired;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(old.clone()));
        let mut cleanup = event(McpOAuthState::CleanupRequired);
        cleanup.authorization_url = None;
        cleanup.flow_id = Some("cleanup-attempt".into());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(cleanup.clone()));
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(old));
        assert_eq!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .0
                .state,
            McpOAuthState::CleanupRequired
        );
        assert!(core.retry_mcp_oauth_browser("workspace", "server").is_err());
        let mut replacement = event(McpOAuthState::Preparing);
        replacement.flow_id = Some("new-consent".into());
        replacement.authorization_url = None;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
            replacement.clone(),
        ));
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(cleanup.clone()));
        cleanup.state = McpOAuthState::Retired;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(cleanup));
        assert_eq!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .0
                .flow_id,
            replacement.flow_id
        );
        assert_eq!(shell.open.load(Ordering::SeqCst), 0);
        core.stop_mcp_oauth();
    }
    #[test]
    fn current_cleanup_replaces_old_denied_but_not_new_consent() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        let mut denied = event(McpOAuthState::Denied);
        denied.authorization_url = None;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(denied.clone()));
        let mut cleanup = event(McpOAuthState::CleanupRequired);
        cleanup.flow_id = Some("B-managed-clear".into());
        cleanup.authorization_url = None;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(cleanup.clone()));
        assert_eq!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .0
                .state,
            McpOAuthState::CleanupRequired
        );
        assert_eq!(
            effective_oauth_management_state(Some(denied.state), Some(cleanup.state), false),
            Some(McpOAuthState::CleanupRequired)
        );
        let mut new = event(McpOAuthState::Preparing);
        new.flow_id = Some("new-consent".into());
        new.authorization_url = None;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(new.clone()));
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(cleanup.clone()));
        assert_eq!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .0
                .flow_id,
            new.flow_id
        );
        let mut unseen = cleanup.clone();
        unseen.flow_id = Some("previous-unseen-cleanup".into());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(unseen.clone()));
        new.state = McpOAuthState::Denied;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(new.clone()));
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(cleanup.clone()));
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(unseen));
        cleanup.state = McpOAuthState::Retired;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(cleanup));
        assert_eq!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .0
                .flow_id,
            new.flow_id
        );
        assert_eq!(shell.open.load(Ordering::SeqCst), 0);
        core.stop_mcp_oauth();
    }
    #[test]
    fn unauthorized_client_never_receives_browser_effect() {
        let core = Arc::new(ClientCore::new());
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(event(
            McpOAuthState::AwaitingCallback,
        )));
        wait_browser(&core);
        assert_eq!(shell.open.load(Ordering::SeqCst), 0);
        assert!(core.mcp_oauth_presentation("workspace", "server").is_none());
        core.stop_mcp_oauth();
    }
    #[test]
    fn config_update_preserves_listener_and_does_not_repeat_browser_effect() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        let pending = event(McpOAuthState::AwaitingCallback);
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(pending.clone()));
        for _ in 0..2 {
            core.observe_mcp_oauth_notification(&GatewayNotification::McpChanged(
                pioneer_protocol::McpChangedNotification {
                    workspace_id: "workspace".into(),
                    snapshot_version: 1,
                    changed: vec![pioneer_protocol::McpChangedItem {
                        name: "server".into(),
                        source_kind: pioneer_protocol::McpSourceKind::Config,
                        action: pioneer_protocol::McpChangedAction::Update,
                    }],
                },
            ));
            core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(
                pending.clone(),
            ));
        }
        assert_eq!(shell.release.load(Ordering::SeqCst), 0);
        wait_browser(&core);
        assert_eq!(shell.open.load(Ordering::SeqCst), 1);
        assert!(core.mcp_oauth_presentation("workspace", "server").is_some());
        core.stop_mcp_oauth();
    }
    #[test]
    fn retired_identity_event_cannot_cancel_replacement_operation() {
        let core = crate::catalog_test_support::client();
        let shell = Arc::new(Shell::default());
        core.set_mcp_oauth_shell(shell.clone());
        let mut replacement = event(McpOAuthState::AwaitingCallback);
        replacement.flow_id = Some("replacement".into());
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(replacement));
        let mut retired = event(McpOAuthState::Cancelled);
        retired.authorization_url = None;
        core.observe_mcp_oauth_notification(&GatewayNotification::McpOAuthChanged(retired));
        assert_eq!(shell.release.load(Ordering::SeqCst), 0);
        assert_eq!(
            core.mcp_oauth_presentation("workspace", "server")
                .unwrap()
                .0
                .flow_id
                .as_deref(),
            Some("replacement")
        );
        core.stop_mcp_oauth();
    }
}
