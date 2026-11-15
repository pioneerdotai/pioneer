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
    pub development: bool,
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
    observed_revision: Option<i64>,
    observed_execution: Option<Option<String>>,
    closed: bool,
    workspace: String,
    app: String,
    epoch: (u64, u64, Option<u64>),
    publication: AppViewPublication,
}
impl ViewEntry {
    fn can_publish(
        &self,
        epoch: (u64, u64, Option<u64>),
        bootstrap: &pioneer_protocol::AppViewBootstrap,
    ) -> bool {
        !self.closed
            && self.epoch == epoch
            && (!bootstrap.development
                || (self
                    .observed_revision
                    .is_none_or(|revision| revision == bootstrap.package_revision)
                    && self.observed_execution.as_ref().is_none_or(|execution| {
                        execution.as_deref() == Some(bootstrap.execution_id.as_str())
                    })))
    }
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
                observed_revision: None,
                observed_execution: None,
                closed: false,
                workspace: workspace.clone(),
                app: app.clone(),
                epoch,
                publication: AppViewPublication {
                    development: false,
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
                        Ok((publication, bootstrap)) => {
                            if let Some(entry) = views
                                .entries
                                .get_mut(&ticket)
                                .filter(|e| e.can_publish(current_epoch, &bootstrap))
                            {
                                entry.package_revision = Some(bootstrap.package_revision);
                                entry.execution_id = Some(bootstrap.execution_id.clone());
                                entry.publication = publication;
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
            // Opening can overlap a mutation or runtime revocation before its
            // bootstrap arrives. Direct dev pages cannot rely on the release
            // proxy to fence that stale response, so retain both event identities.
            if event.runtime_only {
                entry.observed_execution = Some(event.execution_id.clone());
            } else {
                entry.observed_revision = Some(event.revision);
            }
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
    ) -> anyhow::Result<(AppViewPublication, pioneer_protocol::AppViewBootstrap)> {
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
        let publication = (|| {
            let access = sender
                .current_gateway_http_access()
                .map_err(|_| anyhow::anyhow!("gateway_not_connected"))?;
            anyhow::ensure!(self.provider_runtime_epoch() == epoch, "app_view_stale");
            let local = self.active_gateway_endpoint().is_some_and(|endpoint| {
                endpoint.kind == crate::gateway::types::GatewayEndpointKind::Local
                    && endpoint.gateway_base_url == access.gateway_base_url
            });
            view_publication(&access.gateway_base_url, local, &bootstrap)
        })();
        match publication {
            Ok(publication) => Ok((publication, bootstrap)),
            Err(error) => {
                // Validation/topology rejection happens after minting. Release
                // the lease even though it will never be published to a shell.
                let _: anyhow::Result<pioneer_protocol::AppViewLeaseResponse> =
                    crate::rpc::send_json_rpc_request_typed(
                        &bound,
                        pioneer_protocol::constants::methods::APPS_CLOSE_VIEW,
                        &pioneer_protocol::AppViewLeaseParams {
                            relative_url: bootstrap.relative_url,
                        },
                        std::time::Duration::from_secs(5),
                    );
                Err(error)
            }
        }
    }
}

fn view_publication(
    gateway: &crate::gateway::endpoint::GatewayBaseUrl,
    local_gateway: bool,
    bootstrap: &pioneer_protocol::AppViewBootstrap,
) -> anyhow::Result<AppViewPublication> {
    use crate::transport::http::BrowserViewUrl;
    // Validate the lease separately. A dev browser never receives this grant.
    let proxied = BrowserViewUrl::resolve_app(gateway, bootstrap.relative_url.expose_secret())
        .map_err(|_| anyhow::anyhow!("app_view_invalid_url"))?;
    let (url, base_url) = if bootstrap.development {
        let gateway_url = url::Url::parse(gateway.as_str())?;
        let dev = url::Url::parse(
            bootstrap
                .dev_url
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("app_view_invalid_url"))?,
        )?;
        let loopback = |url: &url::Url| match url.host() {
            Some(url::Host::Domain("localhost")) => true,
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        anyhow::ensure!(
            local_gateway && loopback(&gateway_url),
            "apps.dev_requires_local_desktop_gateway"
        );
        anyhow::ensure!(
            dev.scheme() == "http" && loopback(&dev)
                && dev.username().is_empty() && dev.password().is_none()
                && dev.path() == "/" && dev.query().is_none() && dev.fragment().is_none()
                && dev.port_or_known_default().is_some_and(|port| port != 0)
                // Loopback aliases must not admit the Gateway's own listener.
                && dev.port_or_known_default() != gateway_url.port_or_known_default(),
            "app_view_invalid_url"
        );
        (bootstrap.dev_url.clone().unwrap(), None)
    } else {
        anyhow::ensure!(bootstrap.dev_url.is_none(), "app_view_invalid_url");
        let token = bootstrap
            .relative_url
            .expose_secret()
            .strip_prefix("/storage/app-views/")
            .and_then(|tail| tail.split_once('/'))
            .map(|(token, _)| token)
            .ok_or_else(|| anyhow::anyhow!("app_view_invalid_url"))?;
        let base_url =
            BrowserViewUrl::resolve_app(gateway, &format!("/storage/app-views/{token}/"))
                .map_err(|_| anyhow::anyhow!("app_view_invalid_url"))?;
        (
            proxied.expose_url().to_owned(),
            Some(pioneer_protocol::AuthSecretString::new(
                base_url.expose_url(),
            )),
        )
    };
    Ok(AppViewPublication {
        development: bootstrap.development,
        state: "ready".into(),
        url: Some(pioneer_protocol::AuthSecretString::new(url)),
        base_url,
        title: bootstrap.title.clone(),
        expires_at: Some(bootstrap.expires_at),
    })
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

#[cfg(test)]
mod view_tests {
    use super::*;

    fn bootstrap() -> pioneer_protocol::AppViewBootstrap {
        pioneer_protocol::AppViewBootstrap {
            development: true,
            dev_url: Some("http://localhost:3000".into()),
            package_revision: 7,
            relative_url: pioneer_protocol::AuthSecretString::new(format!(
                "/storage/app-views/{}/ignored-release-entry",
                "A".repeat(43)
            )),
            expires_at: u64::MAX,
            execution_id: "dev-execution".into(),
            title: "Example (dev)".into(),
        }
    }

    #[test]
    fn dev_browser_location_is_the_registered_url_and_never_contains_its_lease() {
        let gateway =
            crate::gateway::endpoint::GatewayBaseUrl::parse_presentation("http://127.0.0.1:17878")
                .unwrap();
        let mut bootstrap = bootstrap();
        let first = view_publication(&gateway, true, &bootstrap).unwrap();
        assert_eq!(
            first.url.as_ref().unwrap().expose_secret(),
            "http://localhost:3000"
        );
        assert!(first.base_url.is_none());
        assert!(
            !serde_json::to_string(&first)
                .unwrap()
                .contains("storage/app-views")
        );
        bootstrap.relative_url = pioneer_protocol::AuthSecretString::new(format!(
            "/storage/app-views/{}/",
            "B".repeat(43)
        ));
        let second = view_publication(&gateway, true, &bootstrap).unwrap();
        assert_eq!(first.url, second.url);
        // Installed Apps continue using the Gateway capability, including its
        // base prefix, even when the Gateway registry entry is remote.
        bootstrap.development = false;
        bootstrap.dev_url = None;
        let release = view_publication(&gateway, false, &bootstrap).unwrap();
        assert!(
            release
                .url
                .unwrap()
                .expose_secret()
                .starts_with("http://127.0.0.1:17878/storage/app-views/")
        );
        assert!(release.base_url.is_some());
    }

    #[test]
    fn dev_opening_rejects_remote_topology_and_internal_or_unregistered_locations() {
        let local =
            crate::gateway::endpoint::GatewayBaseUrl::parse_presentation("http://127.0.0.1:17878")
                .unwrap();
        let remote =
            crate::gateway::endpoint::GatewayBaseUrl::parse_presentation("https://gateway.example")
                .unwrap();
        assert!(view_publication(&local, false, &bootstrap()).is_err());
        assert!(view_publication(&remote, true, &bootstrap()).is_err());
        for url in [
            None,
            Some("http://localhost:17878"),
            Some("http://localhost:17878/storage/app-views/x"),
            Some("http://remote.example:3000"),
            Some("http://user:secret@localhost:3000"),
            Some("file:///tmp/index.html"),
        ] {
            let mut bootstrap = bootstrap();
            bootstrap.dev_url = url.map(str::to_owned);
            assert!(view_publication(&local, true, &bootstrap).is_err());
        }
    }

    #[test]
    fn direct_views_retire_through_the_existing_revision_and_session_lifecycle() {
        let core = crate::core::ClientCore::new();
        let gateway =
            crate::gateway::endpoint::GatewayBaseUrl::parse_presentation("http://127.0.0.1:17878")
                .unwrap();
        for runtime_only in [false, true] {
            let (stop, stopped) = std::sync::mpsc::sync_channel(1);
            core.app_views.lock().unwrap().entries.insert(
                1,
                ViewEntry {
                    stop: Some(stop),
                    package_revision: Some(7),
                    execution_id: Some("dev-execution".into()),
                    observed_revision: None,
                    observed_execution: None,
                    closed: false,
                    workspace: "workspace".into(),
                    app: "dev-app".into(),
                    epoch: core.provider_runtime_epoch(),
                    publication: view_publication(&gateway, true, &bootstrap()).unwrap(),
                },
            );
            let mut event = pioneer_protocol::AppsChangedNotification {
                workspace_id: "other-workspace".into(),
                app_id: "dev-app".into(),
                revision: 8,
                runtime_only,
                execution_id: None,
            };
            core.invalidate_app_views(&event);
            assert!(core.app_view_publication(1).unwrap().url.is_some());
            event.workspace_id = "workspace".into();
            core.invalidate_app_views(&event);
            let retired = core.app_view_publication(1).unwrap();
            assert_eq!(retired.state, "unavailable");
            assert!(retired.url.is_none());
            assert!(retired.base_url.is_none());
            assert!(matches!(
                stopped.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Disconnected)
            ));
            // Repeat the event before the RPC response identifies the opening
            // view. A direct dev page must not publish that stale bootstrap.
            let epoch = core.provider_runtime_epoch();
            {
                let mut views = core.app_views.lock().unwrap();
                let opening = views.entries.get_mut(&1).unwrap();
                opening.package_revision = None;
                opening.execution_id = None;
                opening.observed_revision = None;
                opening.observed_execution = None;
                opening.publication.state = "loading".into();
                assert!(opening.can_publish(epoch, &bootstrap()));
            }
            core.invalidate_app_views(&event);
            let views = core.app_views.lock().unwrap();
            let opening = views.entries.get(&1).unwrap();
            assert!(!opening.can_publish(epoch, &bootstrap()));
            let mut release = bootstrap();
            release.development = false;
            release.dev_url = None;
            assert!(opening.can_publish(epoch, &release));
        }
        core.retire_app_views();
        assert!(core.app_view_publication(1).is_none());
    }
}
