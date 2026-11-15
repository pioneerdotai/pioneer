//! App catalog reads and revision-bound presentation shared by native shells.
use crate::composer::capabilities::{ComposerCapability, ComposerCapabilityKind};
use pioneer_protocol::PluginsListResponse;

impl crate::core::ClientCore {
    pub fn read_apps(&self, workspace: &str) -> anyhow::Result<PluginsListResponse> {
        let epoch = self.provider_runtime_epoch();
        let bound = self
            .transport_runtime()
            .ws_command_sender()
            .requests_for_connection(
                epoch
                    .2
                    .ok_or_else(|| anyhow::anyhow!("gateway_not_connected"))?,
            );
        let result = crate::transport::ws::command_sender::packages_list(
            &bound,
            pioneer_protocol::PackageKind::App,
            pioneer_protocol::PluginsListParams {
                workspace_id: workspace.into(),
            },
        )?;
        anyhow::ensure!(self.provider_runtime_epoch() == epoch, "app_catalog_stale");
        Ok(result)
    }
}

impl crate::core::ClientCore {
    /// Catalog text is usable only at the selected revision. Refresh never
    /// silently upgrades a pinned chip's presentation or execution identity.
    pub fn composer_capability_label(
        &self,
        thread: &str,
        capability: &ComposerCapability,
        locale: &str,
    ) -> String {
        if let ComposerCapabilityKind::App {
            app_id,
            expected_revision,
            ..
        } = &capability.kind
        {
            if let Some(parent) = self.composer_catalog_snapshot(thread).and_then(|p| {
                p.apps
                    .iter()
                    .find(|p| &p.id == app_id && p.revision == *expected_revision)
                    .cloned()
            }) {
                return parent.metadata.name_for_locale(&parent.name, locale);
            }
        }
        capability.label.clone()
    }
}

use std::{collections::BTreeMap, sync::Arc};
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, serde::Serialize)]
pub struct AppViewPublication {
    pub state: String,
    pub url: Option<pioneer_protocol::AuthSecretString>,
    pub base_url: Option<pioneer_protocol::AuthSecretString>,
    pub title: String,
    pub expires_at: Option<u64>,
}
struct ViewEntry {
    stop: Option<std::sync::mpsc::SyncSender<()>>,
    package_revision: Option<i64>,
    execution_id: Option<String>,
    closed: bool,
    workspace: String,
    app: String,
    epoch: (u64, u64, Option<u64>),
    publication: AppViewPublication,
}
#[derive(Default)]
pub(crate) struct AppViews {
    next: u64,
    entries: BTreeMap<u64, ViewEntry>,
}
impl crate::core::ClientCore {
    pub(crate) fn retire_app_views(&self) {
        for entry in self.app_views.lock().unwrap().entries.values_mut() {
            entry.closed = true;
            entry.stop.take();
            entry.publication.url = None;
            entry.publication.base_url = None;
        }
    }
    /// Both shells use this bounded asynchronous native operation. No network
    /// request runs on a GPUI render callback or the React Native JS thread.
    pub fn begin_app_view(self: &Arc<Self>, workspace: String, app: String) -> anyhow::Result<u64> {
        let epoch = self.provider_runtime_epoch();
        anyhow::ensure!(epoch.2.is_some(), "gateway_not_connected");
        let mut views = self.app_views.lock().unwrap();
        anyhow::ensure!(views.entries.len() < 16, "app_view_capacity");
        views.next = views
            .next
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("app_view_capacity"))?;
        let ticket = views.next;
        let (stop, stopped) = std::sync::mpsc::sync_channel(1);
        views.entries.insert(
            ticket,
            ViewEntry {
                stop: Some(stop),
                package_revision: None,
                execution_id: None,
                closed: false,
                workspace: workspace.clone(),
                app: app.clone(),
                epoch,
                publication: AppViewPublication {
                    state: "loading".into(),
                    url: None,
                    base_url: None,
                    title: String::new(),
                    expires_at: None,
                },
            },
        );
        drop(views);
        let owner = Arc::downgrade(self);
        let worker = std::thread::Builder::new()
            .name("client-app-view".into())
            .spawn(move || {
                let Some(core) = owner.upgrade() else {
                    return;
                };
                let result = core.open_app_view(ticket, &workspace, &app);
                let current_epoch = core.provider_runtime_epoch();
                let lease = {
                    let mut views = core.app_views.lock().unwrap();
                    match result {
                        Ok((url, base_url, bootstrap)) => {
                            if let Some(entry) = views
                                .entries
                                .get_mut(&ticket)
                                .filter(|e| !e.closed && e.epoch == current_epoch)
                            {
                                entry.package_revision = Some(bootstrap.package_revision);
                                entry.execution_id = Some(bootstrap.execution_id.clone());
                                entry.publication = AppViewPublication {
                                    state: "ready".into(),
                                    url: Some(pioneer_protocol::AuthSecretString::new(
                                        url.expose_url(),
                                    )),
                                    base_url: Some(pioneer_protocol::AuthSecretString::new(
                                        base_url.expose_url(),
                                    )),
                                    title: bootstrap.title.clone(),
                                    expires_at: Some(bootstrap.expires_at),
                                };
                            } else {
                                if let Some(entry) = views.entries.get_mut(&ticket) {
                                    entry.stop.take();
                                    entry.publication.state = "offline".into();
                                }
                            }
                            // Even a view closed during minting must release its new lease.
                            Some((bootstrap.relative_url, bootstrap.expires_at))
                        }
                        Err(error) => {
                            tracing::warn!(app_id = %app, error = %error, "App view opening failed");
                            if let Some(entry) = views.entries.get_mut(&ticket) {
                                entry.publication.state = if entry.epoch != current_epoch {
                                    "offline"
                                } else if crate::rpc::json_rpc_authorization_failure(&error)
                                    .is_some()
                                {
                                    "denied"
                                } else {
                                    "failed"
                                }
                                .into();
                            }
                            None
                        }
                    }
                };
                if let Some((relative, expiry)) = lease {
                    core.hold_app_view(ticket, relative, expiry, epoch, stopped);
                }
                let mut views = core.app_views.lock().unwrap();
                if views.entries.get(&ticket).is_some_and(|entry| entry.closed) {
                    views.entries.remove(&ticket);
                }
            });
        if worker.is_err() {
            self.app_views.lock().unwrap().entries.remove(&ticket);
            anyhow::bail!("app_view_capacity");
        }
        Ok(ticket)
    }
    pub fn app_view_publication(&self, ticket: u64) -> Option<AppViewPublication> {
        let epoch = self.provider_runtime_epoch();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|t| t.as_secs())
            .unwrap_or(u64::MAX);
        let mut views = self.app_views.lock().unwrap();
        let entry = views.entries.get_mut(&ticket)?;
        if entry.closed {
            return None;
        }
        if entry.epoch != epoch
            || entry
                .publication
                .expires_at
                .is_some_and(|expiry| expiry <= now)
        {
            entry.stop.take();
            entry.publication.url = None;
            entry.publication.base_url = None;
            entry.publication.state = "offline".into();
        }
        Some(entry.publication.clone())
    }
    pub fn close_app_view(&self, ticket: u64) {
        let mut views = self.app_views.lock().unwrap();
        if let Some(entry) = views.entries.get_mut(&ticket) {
            entry.closed = true;
            entry.publication.url = None;
            entry.publication.base_url = None;
            entry.stop.take();
            if matches!(entry.publication.state.as_str(), "loading" | "ready") {
                return;
            }
        }
        views.entries.remove(&ticket);
    }
    pub(crate) fn invalidate_app_views(&self, event: &pioneer_protocol::AppsChangedNotification) {
        for entry in self
            .app_views
            .lock()
            .unwrap()
            .entries
            .values_mut()
            .filter(|e| e.workspace == event.workspace_id && e.app == event.app_id)
        {
            let changed = if event.runtime_only {
                entry
                    .execution_id
                    .as_ref()
                    .is_some_and(|id| Some(id) != event.execution_id.as_ref())
            } else {
                entry
                    .package_revision
                    .is_some_and(|revision| revision != event.revision)
            };
            if changed {
                entry.stop.take();
                entry.publication.url = None;
                entry.publication.base_url = None;
                entry.publication.state = "unavailable".into();
            }
        }
    }
    fn hold_app_view(
        &self,
        ticket: u64,
        relative: pioneer_protocol::AuthSecretString,
        mut expiry: u64,
        epoch: (u64, u64, Option<u64>),
        stopped: std::sync::mpsc::Receiver<()>,
    ) {
        let Some(connection) = epoch.2 else {
            return;
        };
        let sender = self.transport_runtime().ws_command_sender();
        let bound = sender.requests_for_connection(connection);
        loop {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|t| t.as_secs())
                .unwrap_or(u64::MAX);
            let delay = std::time::Duration::from_secs(
                expiry.saturating_sub(now.saturating_add(45)).max(1),
            );
            if !matches!(
                stopped.recv_timeout(delay),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ) {
                break;
            }
            if self.provider_runtime_epoch() != epoch {
                break;
            }
            let result: anyhow::Result<pioneer_protocol::AppViewLeaseResponse> =
                crate::rpc::send_json_rpc_request_typed(
                    &bound,
                    pioneer_protocol::constants::methods::APPS_RENEW_VIEW,
                    &pioneer_protocol::AppViewLeaseParams {
                        relative_url: relative.clone(),
                    },
                    crate::rpc::RPC_REQUEST_TIMEOUT,
                );
            match result {
                Ok(value) if self.provider_runtime_epoch() == epoch => {
                    expiry = value.expires_at;
                    if let Some(entry) = self.app_views.lock().unwrap().entries.get_mut(&ticket) {
                        entry.publication.expires_at = Some(expiry);
                    }
                }
                _ => {
                    if let Some(entry) = self.app_views.lock().unwrap().entries.get_mut(&ticket) {
                        entry.publication.url = None;
                        entry.publication.base_url = None;
                        entry.publication.state = "offline".into();
                    }
                    break;
                }
            }
        }
        if self.provider_runtime_epoch() == epoch {
            let _: anyhow::Result<pioneer_protocol::AppViewLeaseResponse> =
                crate::rpc::send_json_rpc_request_typed(
                    &bound,
                    pioneer_protocol::constants::methods::APPS_CLOSE_VIEW,
                    &pioneer_protocol::AppViewLeaseParams {
                        relative_url: relative,
                    },
                    std::time::Duration::from_secs(5),
                );
        }
    }
    fn open_app_view(
        &self,
        ticket: u64,
        workspace: &str,
        app: &str,
    ) -> anyhow::Result<(
        crate::transport::http::BrowserViewUrl,
        crate::transport::http::BrowserViewUrl,
        pioneer_protocol::AppViewBootstrap,
    )> {
        let epoch = self.provider_runtime_epoch();
        let sender = self.transport_runtime().ws_command_sender();
        let bound = sender.requests_for_connection(
            epoch
                .2
                .ok_or_else(|| anyhow::anyhow!("gateway_not_connected"))?,
        );
        let params = pioneer_protocol::AppViewParams {
            workspace_id: workspace.into(),
            app_id: app.into(),
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
        loop {
            anyhow::ensure!(
                self.provider_runtime_epoch() == epoch
                    && self
                        .app_views
                        .lock()
                        .unwrap()
                        .entries
                        .get(&ticket)
                        .is_some_and(|e| !e.closed),
                "app_view_retired"
            );
            let info: pioneer_protocol::AppRuntimeInfo = crate::rpc::send_json_rpc_request_typed(
                &bound,
                pioneer_protocol::constants::methods::APPS_RUNTIME_INFO,
                &params,
                crate::rpc::RPC_REQUEST_TIMEOUT,
            )?;
            if info.state == "ready" {
                break;
            }
            if !matches!(
                info.state.as_str(),
                "stopped" | "preparing" | "starting" | "backoff"
            ) || std::time::Instant::now() >= deadline
            {
                // Gateway supplies a bounded, credential-redacted output tail
                // only to superusers. Preserve it when opening the view fails.
                tracing::warn!(
                    app_id = app,
                    runtime_state = %info.state,
                    diagnostic = ?info.diagnostic,
                    runtime_logs = ?info.logs,
                    "App runtime did not become ready"
                );
                anyhow::bail!("app_not_ready");
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        let bootstrap: pioneer_protocol::AppViewBootstrap =
            crate::rpc::send_json_rpc_request_typed(
                &bound,
                pioneer_protocol::constants::methods::APPS_OPEN_VIEW,
                &pioneer_protocol::AppViewParams {
                    workspace_id: workspace.into(),
                    app_id: app.into(),
                },
                crate::rpc::RPC_REQUEST_TIMEOUT,
            )?;
        let access = sender
            .current_gateway_http_access()
            .map_err(|_| anyhow::anyhow!("gateway_not_connected"))?;
        anyhow::ensure!(self.provider_runtime_epoch() == epoch, "app_view_stale");
        let url = crate::transport::http::BrowserViewUrl::resolve_app(
            &access.gateway_base_url,
            bootstrap.relative_url.expose_secret(),
        )
        .map_err(|_| anyhow::anyhow!("app_view_invalid_url"))?;
        let token = bootstrap
            .relative_url
            .expose_secret()
            .strip_prefix("/storage/app-views/")
            .and_then(|tail| tail.split_once('/'))
            .map(|(token, _)| token)
            .ok_or_else(|| anyhow::anyhow!("app_view_invalid_url"))?;
        let base_url = crate::transport::http::BrowserViewUrl::resolve_app(
            &access.gateway_base_url,
            &format!("/storage/app-views/{token}/"),
        )
        .map_err(|_| anyhow::anyhow!("app_view_invalid_url"))?;
        Ok((url, base_url, bootstrap))
    }
}

pub(crate) fn app_label(parent: &pioneer_protocol::PluginItem) -> String {
    parent
        .metadata
        .display_name
        .clone()
        .unwrap_or_else(|| parent.name.clone())
}
pub(crate) fn app_capability(p: &pioneer_protocol::PluginItem) -> Option<ComposerCapability> {
    (p.enabled && p.state == "installed").then(|| ComposerCapability {
        id: pioneer_protocol::app_capability_key(&p.id),
        label: p
            .metadata
            .display_name
            .clone()
            .unwrap_or_else(|| p.name.clone()),
        kind: ComposerCapabilityKind::App {
            app_id: p.id.clone(),
            expected_revision: p.revision,
            dark_icon: p
                .metadata
                .app
                .as_ref()
                .and_then(|a| a.composer_icon.as_ref())
                .map(|i| i.dark.clone()),
            icon: p
                .metadata
                .composer_icon
                .clone()
                .or_else(|| p.metadata.logo.clone()),
        },
    })
}
pub(crate) fn app_target_supported(
    parent: &pioneer_protocol::PluginItem,
    target: crate::composer::capabilities::ComposerCapabilityTarget,
) -> bool {
    let policy = target.policy();
    parent.components.iter().all(|c| match c.kind.as_str() {
        "skill" => policy.supports_skills,
        "mcp" => policy.supports_mcp_tools,
        _ => false,
    })
}
