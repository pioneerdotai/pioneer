//! Gateway-owned authoring connections. Source files are never platform resources.
use super::super::*;
use anyhow::{Context, Result, ensure};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use pioneer_protocol::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

pub(in crate::message) fn is_dev(parent: &pioneer_crud::PackageRecord) -> bool {
    parent.kind == PackageKind::App && parent.source_upload_id.starts_with("dev:")
}
#[derive(Clone, Serialize, Deserialize)]
pub(in crate::message) struct DevSource {
    project: PathBuf,
    url: String,
    gateway: GatewayId,
    principal: PrincipalId,
    device: DeviceId,
    session: AuthSessionId,
}
impl DevSource {
    pub(in crate::message) fn identity(&self, workspace: &str) -> String {
        let mut hash = Sha256::new();
        hash.update(workspace.as_bytes());
        hash.update([0]);
        hash.update(self.project.to_string_lossy().as_bytes());
        format!("dev:{}", hex::encode(hash.finalize()))
    }
    fn address(&self) -> Result<SocketAddr> {
        address(&self.url)
    }
    fn context(&self) -> RequestContext {
        // Durable local management authority, never an access credential. Only
        // a Superuser can create this record; no authored metadata sets it.
        let principal = Arc::new(crate::auth::AuthenticatedSessionPrincipal {
            gateway_id: self.gateway.clone(),
            principal_id: self.principal.clone(),
            device_id: self.device.clone(),
            session_id: self.session.clone(),
            kind: PrincipalKind::Superuser,
            role_key: None,
            access_jti: "non-bearer-dev-app-owner".into(),
            access_expires_at_unix: 0,
        });
        let connection = crate::request_context::ConnectionContext::new(0, principal);
        RequestContext::new(
            &connection,
            None,
            crate::request_context::CanonicalMethod::rpc(constants::methods::APPS_DEV_ADD),
        )
    }
}
fn address(value: &str) -> Result<SocketAddr> {
    let url = url::Url::parse(value)?;
    ensure!(
        value.trim() == value
            && url.scheme() == "http"
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none(),
        "apps.dev_requires_local_http_origin"
    );
    let host = match url.host().context("apps.dev_invalid_url")? {
        url::Host::Domain("localhost") => std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        url::Host::Ipv4(ip) if ip.is_loopback() => ip.into(),
        url::Host::Ipv6(ip) if ip.is_loopback() => ip.into(),
        _ => anyhow::bail!("apps.dev_remote_server_unsupported"),
    };
    let port = url
        .port_or_known_default()
        .context("apps.dev_invalid_port")?;
    ensure!(port != 0, "apps.dev_invalid_port");
    Ok(SocketAddr::new(host, port))
}
fn read_source(parent: &pioneer_crud::PackageRecord) -> Result<DevSource> {
    ensure!(is_dev(parent), "apps.not_dev");
    let path = Path::new(&parent.package_path)
        .parent()
        .context("apps.dev_invalid_storage")?
        .join("dev.json");
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(32769)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 32768, "apps.dev_invalid_source");
    let source: DevSource = serde_json::from_slice(&bytes)?;
    ensure!(
        source.identity(&parent.workspace_id) == parent.source_upload_id,
        "apps.dev_invalid_identity"
    );
    source.address()?;
    Ok(source)
}
pub(super) fn view_url(parent: &pioneer_crud::PackageRecord) -> Result<String> {
    // Read the durable, identity-checked registration, never App metadata or
    // client-supplied view params. read_source revalidates the local URL.
    Ok(read_source(parent)?.url)
}
struct ViewState {
    workspace: String,
    execution: String,
    diagnostic: Option<String>,
}
#[derive(Default)]
pub(in crate::message) struct DevApps {
    views: Mutex<BTreeMap<String, ViewState>>,
    worker: Mutex<Option<(CancellationToken, JoinHandle<()>)>>,
}
impl MessageProcessor {
    pub(super) async fn dev_apps_request(&self, context: &RequestContext, request: JsonRpcRequest) {
        let id = request.id.clone();
        let result = self
            .execute_dev_apps(
                context,
                &request.method,
                &id,
                request.params.unwrap_or(serde_json::Value::Null),
            )
            .await;
        match result {
            Ok(value) => {
                if let Ok(response) = JsonRpcResponse::from_result(id.clone(), &value) {
                    let _ = self.send_json(context.connection_id(), &response).await;
                }
            }
            Err(error) => {
                self.send_error(
                    context.connection_id(),
                    super::super::packages::plugin_error(&id, &error.to_string()),
                )
                .await;
            }
        }
    }
    async fn execute_dev_apps(
        &self,
        context: &RequestContext,
        method: &str,
        id: &RequestId,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        async {
            ensure!(
                context.principal().kind == PrincipalKind::Superuser,
                "apps.dev_requires_superuser"
            );

            let workspace = params["workspace_id"]
                .as_str()
                .context("apps.invalid_workspace")?
                .to_owned();
            self.workspace_manager
                .validate_workspace_id(&workspace)
                .await?;
            match method {
                constants::methods::APPS_DEV_ADD => {
                    let params: AppsDevAddParams = serde_json::from_value(params)?;
                    address(&params.url)?;
                    let path = PathBuf::from(&params.project);
                    ensure!(
                        path.is_absolute(),
                        "apps.dev_requires_absolute_project_on_gateway"
                    );
                    let prepared =
                        super::super::package_delivery::prepare_cancellable(move |cancel| {
                            pioneer_apps::PreparedAppSource::capture(
                                &path,
                                pioneer_apps::PACKAGE_LIMITS,
                                || cancel.is_cancelled(),
                            )
                        })
                        .await?;
                    let principal = context.principal();
                    let source = DevSource {
                        project: prepared.project.clone(),
                        url: params.url,
                        gateway: principal.gateway_id.clone(),
                        principal: principal.principal_id.clone(),
                        device: principal.device_id.clone(),
                        session: principal.session_id.clone(),
                    };
                    let identity = source.identity(&workspace);
                    let lock = self.plugin_mutation_lock(&identity).await;
                    let _guard = lock.lock().await;
                    if let Some(parent) = self.crud_store.find_plugin_by_upload(&identity).await? {
                        let old = read_source(&parent)?;
                        ensure!(
                            old.url == source.url,
                            "apps.dev_already_added_with_other_url_remove_first"
                        );
                        return Ok(serde_json::json!({"id":parent.id}));
                    }
                    let plan =
                        super::super::packages::package_plan::source_app_plan(prepared.package)?;
                    let item = self
                        .install_plugin(
                            context,
                            &id,
                            &workspace,
                            None,
                            Some(source),
                            &self.package_storage_root(&workspace, PackageKind::App),
                            prepared.snapshot,
                            plan,
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!("{}", e.error.message))?;
                    Ok(serde_json::json!({"id":item.id}))
                }
                constants::methods::APPS_DEV_LIST => {
                    let _: AppsListParams = serde_json::from_value(params)?;
                    let mut items = Vec::new();
                    for parent in self
                        .crud_store
                        .list_package_installations(&workspace, PackageKind::App)
                        .await?
                    {
                        if !is_dev(&parent) {
                            continue;
                        }
                        let source = read_source(&parent)?;
                        let views = self.dev_apps.views.lock().await;
                        let mut diagnostic = views.get(&parent.id).and_then(|v| v.diagnostic.clone());
                        if diagnostic.is_none() && parent.state == "interrupted" { diagnostic = Some("apps.dev_operation_interrupted; source or component change not applied".into()); }
                        let state = if diagnostic.as_deref().is_some_and(|error| error.starts_with("apps.dev_component_failed")) {
                            "component_error"
                        } else if diagnostic.is_some() {
                            "change_not_applied"
                        } else if parent.state != "installed" {
                            &parent.state
                        } else {
                            "connected"
                        }
                        .to_owned();
                        drop(views);
                        let manifest = pioneer_apps::AppManifest::parse(
                            parent
                                .app_manifest_json
                                .as_deref()
                                .context("apps.invalid_manifest")?
                                .as_bytes(),
                        )?;
                        let reachable = tokio::time::timeout(
                            Duration::from_secs(1),
                            tokio::net::TcpStream::connect(source.address()?),
                        )
                        .await
                        .is_ok_and(|r| r.is_ok());
                        items.push(AppDevConnection {
                            id: parent.id,
                            name: manifest.presentation("", &Default::default()).name,
                            project: source.project.to_string_lossy().into(),
                            url: source.url,
                            state: if !reachable && diagnostic.is_none() {
                                "server_unavailable".into()
                            } else {
                                state
                            },
                            diagnostic,
                        });
                    }
                    Ok(serde_json::to_value(items)?)
                }
                constants::methods::APPS_DEV_REMOVE => {
                    let params: AppsDetailsParams = serde_json::from_value(params)?;
                    let Some(parent) = self
                        .crud_store
                        .find_package_installation(&params.app_id)
                        .await?
                    else {
                        return Ok(serde_json::json!({"removed":true}));
                    };
                    ensure!(
                        parent.workspace_id == workspace && is_dev(&parent),
                        "apps.not_dev"
                    );
                    let result = self
                        .mutate_plugin(
                            context,
                            &id,
                            PluginsMutateParams {
                                workspace_id: workspace,
                                plugin_id: parent.id,
                                expected_revision: parent.revision,
                                intent: PluginManagementIntent::Remove { purge_data: true },
                            },
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!("{}", e.error.message))?;
                    Ok(serde_json::to_value(result)?)
                }
                _ => anyhow::bail!("apps.invalid_request"),
            }
        }
        .await
    }
    pub(crate) async fn local_dev_app(
        &self,
        identity: &crate::identity::IdentityBootstrapSnapshot,
        operation: DevAppOperation,
    ) -> Result<serde_json::Value> {
        let (method, params) = match operation {
            DevAppOperation::Add(params) => (
                constants::methods::APPS_DEV_ADD,
                serde_json::to_value(params)?,
            ),
            DevAppOperation::List(params) => (
                constants::methods::APPS_DEV_LIST,
                serde_json::to_value(params)?,
            ),
            DevAppOperation::Remove(params) => (
                constants::methods::APPS_DEV_REMOVE,
                serde_json::to_value(params)?,
            ),
        };
        let source = DevSource {
            project: PathBuf::new(),
            url: String::new(),
            gateway: identity.gateway.id.clone(),
            principal: identity.superuser.id.clone(),
            device: DeviceId::new("D00000000000000000000")?,
            session: AuthSessionId::new("S00000000000000000000")?,
        };
        self.execute_dev_apps(
            &source.context(),
            method,
            &RequestId::new(generate_id(21))?,
            params,
        )
        .await
    }
    pub(in crate::message) async fn stop_dev_view(&self, app: &str) {
        if let Some(view) = self.dev_apps.views.lock().await.remove(app) {
            if let Some(grants) = self.view_grant_service() {
                grants.invalidate_app_views(&view.workspace, Some(app), None);
            }
        }
    }
    pub(in crate::message) async fn app_view_status(
        &self,
        app: &str,
    ) -> super::runtime::RuntimeStatus {
        if let Some(view) = self.dev_apps.views.lock().await.get(app) {
            return super::runtime::RuntimeStatus {
                state: "ready".into(),
                failure: view.diagnostic.clone(),
                execution_id: Some(view.execution.clone()),
            };
        }
        match self.crud_store.find_package_installation(app).await {
            Ok(Some(parent)) if is_dev(&parent) => {
                return super::runtime::RuntimeStatus {
                    state: "preparing".into(),
                    failure: None,
                    execution_id: None,
                };
            }
            Err(_) => {
                return super::runtime::RuntimeStatus {
                    state: "failed".into(),
                    failure: Some("apps.state_unavailable".into()),
                    execution_id: None,
                };
            }
            _ => {}
        }
        self.app_runtime.status(app).await
    }
    pub(crate) async fn start_dev_apps(&self) {
        let mut slot = self.dev_apps.worker.lock().await;
        if slot.is_some() {
            return;
        }
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let owner = self.scoped_with_database_class(pioneer_sqlite::SqliteWriteClass::Maintenance);
        let task = tokio::spawn(async move {
            let database = owner.crud_store.database_connection();
            let mut changes = database.work_signal("apps.lifecycle").subscribe();
            let mut workers = BTreeMap::<
                String,
                (
                    CancellationToken,
                    JoinHandle<()>,
                    Arc<tokio::sync::Notify>,
                    i64,
                    String,
                ),
            >::new();
            loop {
                if stop.is_cancelled() {
                    break;
                }
                if tokio::select! { _ = stop.cancelled() => break, result = changes.begin_pass(&database) => result }.is_err() {
                    tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                    continue;
                }
                let discovery: Result<Vec<pioneer_crud::PackageRecord>> = async {
                    let through = owner
                        .crud_store
                        .app_runtime_frontier()
                        .await?
                        .unwrap_or_default();
                    let mut after = String::new();
                    let mut found = Vec::new();
                    loop {
                        let page = owner.crud_store.app_runtime_page(&after, &through).await?;
                        if page.is_empty() {
                            break;
                        }
                        for (id, parent) in page {
                            after = id;
                            if let Some(parent) = parent {
                                if is_dev(&parent)
                                    && parent.enabled
                                    && parent.state != "removing"
                                    && pioneer_crud::resolve_workspace_authorization_scope(
                                        &database,
                                        &parent.workspace_id,
                                    )
                                    .await?
                                    .is_some_and(|scope| scope.is_active)
                                {
                                    found.push(parent);
                                }
                            }
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok(found)
                }
                .await;
                if let Ok(parents) = discovery {
                    let seen: BTreeSet<_> = parents.iter().map(|p| p.id.clone()).collect();
                    let removed: Vec<_> = workers
                        .keys()
                        .filter(|id| !seen.contains(*id))
                        .cloned()
                        .collect();
                    for id in removed {
                        if let Some((cancel, task, _, _, _)) = workers.remove(&id) {
                            cancel.cancel();
                            let _ = task.await;
                        }
                        owner.stop_dev_view(&id).await;
                    }
                    for parent in parents {
                        if let Some((_, task, wake, revision, state)) = workers.get_mut(&parent.id)
                        {
                            if !task.is_finished() {
                                if *revision != parent.revision || *state != parent.state {
                                    *revision = parent.revision;
                                    *state = parent.state.clone();
                                    wake.notify_one();
                                }
                                continue;
                            }
                        }
                        let token = stop.child_token();
                        let child = token.clone();
                        let this = owner.clone();
                        let id = parent.id.clone();
                        let wake = Arc::new(tokio::sync::Notify::new());
                        let signal = wake.clone();
                        let revision = parent.revision;
                        let state = parent.state.clone();
                        workers.insert(
                            id,
                            (
                                token,
                                tokio::spawn(async move {
                                    this.watch_dev_app(parent, child, signal).await;
                                }),
                                wake,
                                revision,
                                state,
                            ),
                        );
                    }
                } else {
                    tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                    continue;
                }
                tokio::select! { _ = stop.cancelled() => break, _ = changes.changed() => {} }
            }
            for (id, (cancel, task, _, _, _)) in workers {
                cancel.cancel();
                let _ = task.await;
                owner.stop_dev_view(&id).await;
            }
        });
        *slot = Some((cancel, task));
    }
    pub(crate) async fn shutdown_dev_apps(&self) {
        if let Some((cancel, task)) = self.dev_apps.worker.lock().await.take() {
            cancel.cancel();
            let _ = task.await;
        }
    }
    async fn watch_dev_app(
        &self,
        mut parent: pioneer_crud::PackageRecord,
        stop: CancellationToken,
        wake: Arc<tokio::sync::Notify>,
    ) {
        self.stop_dev_view(&parent.id).await;
        let source = match read_source(&parent) {
            Ok(source) => source,
            Err(error) => {
                self.dev_apps.views.lock().await.insert(
                    parent.id.clone(),
                    ViewState {
                        workspace: parent.workspace_id.clone(),
                        execution: generate_id(21),
                        diagnostic: Some(error.to_string()),
                    },
                );
                return;
            }
        };
        let signal = wake.clone();
        let interests = Arc::new(std::sync::RwLock::new(BTreeSet::from([source
            .project
            .join("app.json")])));
        let event_error = Arc::new(std::sync::Mutex::new(None::<String>));
        let mut watcher = loop {
            let filter = interests.clone();
            let failures = event_error.clone();
            let signal = signal.clone();
            match RecommendedWatcher::new(
                move |event: notify::Result<notify::Event>| match event {
                    Err(error) => {
                        *failures.lock().unwrap_or_else(|p| p.into_inner()) =
                            Some(error.to_string());
                        signal.notify_one();
                    }
                    Ok(event) if !matches!(event.kind, EventKind::Access(_)) => {
                        let selected = filter.read().unwrap_or_else(|p| p.into_inner());
                        if event.need_rescan()
                            || event.paths.iter().any(|path| {
                                selected.iter().any(|input| {
                                    path == input
                                        || input.starts_with(path)
                                        || path.starts_with(input)
                                })
                            })
                        {
                            signal.notify_one();
                        }
                    }
                    _ => {}
                },
                notify::Config::default().with_follow_symlinks(false),
            ) {
                Ok(watcher) => break watcher,
                Err(error) => {
                    let mut views = self.dev_apps.views.lock().await;
                    let view = views.entry(parent.id.clone()).or_insert_with(|| ViewState {
                        workspace: parent.workspace_id.clone(),
                        execution: generate_id(21),
                        diagnostic: None,
                    });
                    view.diagnostic = Some(format!("apps.dev_watch_failed: {error}"));
                    drop(views);
                    tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                }
            }
        };
        let mut watched = BTreeSet::new();
        // No recursive project watch: only selected directories and their
        // ancestors, including the parent anchor for source deletion/recreation.
        let mut selected = BTreeSet::from([source.project.join("app.json")]);
        loop {
            if stop.is_cancelled() {
                break;
            }
            let mut targets = BTreeSet::new();
            for path in &selected {
                let mut path = if path.is_dir() {
                    path.as_path()
                } else {
                    path.parent().unwrap_or(&source.project)
                };
                loop {
                    if path.is_dir() {
                        targets.insert(path.to_path_buf());
                    }
                    if path != source.project && source.project.starts_with(path) && path.is_dir() {
                        break;
                    }
                    let Some(next) = path.parent() else {
                        break;
                    };
                    path = next;
                }
            }
            let mut watch_error = event_error.lock().unwrap_or_else(|p| p.into_inner()).take();
            let additions: Vec<_> = targets.difference(&watched).cloned().collect();
            for path in additions {
                match watcher.watch(&path, RecursiveMode::NonRecursive) {
                    Ok(()) => {
                        watched.insert(path);
                    }
                    Err(error) => {
                        watch_error = Some(error.to_string());
                    }
                }
            }
            for path in watched.difference(&targets) {
                let _ = watcher.unwatch(path);
            }
            watched.retain(|path| targets.contains(path));
            let path = source.project.clone();
            let cancellation = stop.clone();
            let filter = interests.clone();
            let planned = super::super::package_delivery::prepare_cancellable({
                let path = path.clone();
                let discovery_stop = stop.clone();
                move |cancel| {
                    let snapshot = pioneer_packages::Snapshot::capture_selected(
                        &path,
                        pioneer_packages::Limits {
                            entry_bytes: 1024 * 1024,
                            expanded_bytes: 1024 * 1024,
                            ..pioneer_apps::PACKAGE_LIMITS
                        },
                        &BTreeMap::from([(
                            "app.json".into(),
                            pioneer_packages::SelectionKind::File,
                        )]),
                        &|| cancel.is_cancelled(),
                    )?;
                    let manifest = pioneer_apps::AppManifest::parse(
                        snapshot.file("app.json").context("apps.invalid_manifest")?,
                    )?;
                    let plan = pioneer_apps::SelectionPlan::for_source(&manifest)?;
                    let inputs: BTreeSet<_> =
                        plan.entries().keys().map(|key| path.join(key)).collect();
                    let mut paths = inputs.clone();
                    // Structural capture uses the package walker (limits and
                    // symlink fences), before image/locale/Skill/MCP semantics.
                    // Invalid component content must not hide its directories.
                    if let Ok(snapshot) = pioneer_packages::Snapshot::capture_selected(
                        &path,
                        pioneer_apps::PACKAGE_LIMITS,
                        plan.entries(),
                        &|| cancel.is_cancelled() || discovery_stop.is_cancelled(),
                    ) {
                        paths.extend(snapshot.entries().keys().map(|key| path.join(key)));
                    }
                    Ok((inputs, paths))
                }
            })
            .await;
            if let Ok((inputs, paths)) = planned {
                selected.extend(paths);
                filter
                    .write()
                    .unwrap_or_else(|p| p.into_inner())
                    .extend(inputs);
            }
            let captured = super::super::package_delivery::prepare_cancellable(move |cancel| {
                pioneer_apps::PreparedAppSource::capture(
                    &path,
                    pioneer_apps::PACKAGE_LIMITS,
                    || cancel.is_cancelled() || cancellation.is_cancelled(),
                )
            })
            .await;
            if stop.is_cancelled() {
                break;
            }
            let retry_watch = watch_error.is_some();
            let outcome: Result<()> = async {
                if let Some(error) = watch_error {
                    anyhow::bail!("apps.dev_watch_failed: {error}");
                }
                if let Some(current) = self
                    .crud_store
                    .find_package_installation(&parent.id)
                    .await?
                {
                    if current.pending_json.is_some() && current.state != "removing" {
                        let request = RequestId::new(generate_id(21))?;
                        self.mutate_plugin(
                            &source.context(),
                            &request,
                            PluginsMutateParams {
                                workspace_id: current.workspace_id,
                                plugin_id: current.id,
                                expected_revision: current.revision,
                                intent: PluginManagementIntent::Continue,
                            },
                        )
                        .await
                        .map_err(|e| anyhow::anyhow!("{}", e.error.message))?;
                    }
                }
                let captured = captured?;
                ensure!(
                    captured.project == source.project,
                    "apps.dev_project_identity_changed"
                );
                selected = captured
                    .snapshot
                    .entries()
                    .keys()
                    .map(|key| source.project.join(key))
                    .collect();
                *interests.write().unwrap_or_else(|p| p.into_inner()) = captured
                    .selection
                    .entries()
                    .keys()
                    .map(|key| source.project.join(key))
                    .collect();
                let current = self
                    .crud_store
                    .find_package_installation(&parent.id)
                    .await?
                    .context("apps.dev_removed")?;
                ensure!(
                    current.enabled && current.state != "removing",
                    "apps.dev_unavailable"
                );
                if current.package_fingerprint != captured.package.tree_digest {
                    self.refresh_dev_app(&source.context(), &current, captured)
                        .await?;
                }
                parent = self
                    .crud_store
                    .find_package_installation(&parent.id)
                    .await?
                    .context("apps.dev_removed")?;
                let failed = self
                    .crud_store
                    .list_plugin_components(&parent.id)
                    .await?
                    .into_iter()
                    .find(|component| component.status == "failed");
                if let Some(component) = failed {
                    anyhow::bail!(
                        "apps.dev_component_failed: {}: {}",
                        component.member_key,
                        component.diagnostic.unwrap_or_default()
                    );
                }
                Ok(())
            }
            .await;
            if stop.is_cancelled() {
                break;
            }
            let mut views = self.dev_apps.views.lock().await;
            let view = views.entry(parent.id.clone()).or_insert_with(|| ViewState {
                workspace: parent.workspace_id.clone(),
                execution: generate_id(21),
                diagnostic: None,
            });
            view.diagnostic = outcome.as_ref().err().map(ToString::to_string);
            drop(views);
            // Reconcile watches after discovering selected subdirectories before
            // sleeping, and recapture once to close the registration race.
            let mut new_dirs = BTreeSet::new();
            for selected in &selected {
                let mut path = selected.as_path();
                while path.starts_with(&source.project) {
                    if path.is_dir() {
                        new_dirs.insert(path.to_path_buf());
                    }
                    let Some(next) = path.parent() else {
                        break;
                    };
                    path = next;
                }
            }
            let additions: Vec<_> = new_dirs.difference(&watched).cloned().collect();
            let mut failed_watch = false;
            for path in additions {
                match watcher.watch(&path, RecursiveMode::NonRecursive) {
                    Ok(()) => {
                        watched.insert(path);
                        wake.notify_one();
                    }
                    Err(error) => {
                        failed_watch = true;
                        if let Some(view) = self.dev_apps.views.lock().await.get_mut(&parent.id) {
                            view.diagnostic = Some(format!("apps.dev_watch_failed: {error}"));
                        }
                    }
                }
            }
            if failed_watch || retry_watch {
                tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                continue;
            }
            tokio::select! { _ = stop.cancelled() => break, _ = wake.notified() => { tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_millis(150)) => {} } } }
            // Directory replacement invalidates native registrations even if
            // the lexical paths are unchanged (notably on Linux).
            for path in &watched {
                let _ = watcher.unwatch(path);
            }
            watched.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_origin_cannot_select_remote_authority_or_embed_credentials() {
        for origin in [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://[::1]:3000",
        ] {
            let endpoint = address(origin).unwrap();
            assert!(endpoint.ip().is_loopback());
            assert_eq!(endpoint.port(), 3000);
        }
        for origin in [
            "https://localhost:3000",
            "http://example.test:3000",
            "http://192.0.2.1:3000",
            "http://user:secret@localhost:3000",
            "http://localhost:3000/app",
            "http://localhost:3000?token=secret",
            "http://localhost:3000#fragment",
            "http://localhost:0",
            " http://localhost:3000",
        ] {
            assert!(address(origin).is_err(), "{origin}");
        }
    }
}
