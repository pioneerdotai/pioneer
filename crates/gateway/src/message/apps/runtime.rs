//! Autonomous App executions. Desired state is the existing package enabled/state/revision.
use super::super::*;
use super::provision;
use anyhow::{Context, Result, bail};
use sha2::Digest;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default, serde::Serialize)]
pub(crate) struct RuntimeStatus {
    pub state: String,
    pub failure: Option<String>,
    pub execution_id: Option<String>,
}
type SdkCreation = JoinHandle<microsandbox::MicrosandboxResult<microsandbox::Sandbox>>;
struct Execution {
    active: Arc<AtomicBool>,
    vm_pending: Arc<AtomicBool>,
    sandbox: Arc<RwLock<Option<Arc<microsandbox::Sandbox>>>>,
    creation: Arc<Mutex<Option<SdkCreation>>>,
    logs: Arc<std::sync::Mutex<std::collections::VecDeque<String>>>,
    revision: i64,
    cancel: CancellationToken,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
    status: Arc<RwLock<RuntimeStatus>>,
    upstream: Arc<RwLock<Option<(std::net::SocketAddr, pioneer_protocol::AuthSecretString)>>>,
}
#[derive(Default)]
pub(crate) struct AppRuntime {
    executions: Mutex<BTreeMap<String, Execution>>,
    failures: Mutex<BTreeMap<String, RuntimeStatus>>,
    worker: Mutex<Option<(CancellationToken, JoinHandle<()>)>>,
    preparation: Mutex<()>,
    backend: tokio::sync::OnceCell<Arc<microsandbox::LocalBackend>>,
    bootstrap_failure: RwLock<Option<String>>,
}
impl AppRuntime {
    pub(crate) async fn status(&self, app: &str) -> RuntimeStatus {
        let status = self
            .executions
            .lock()
            .await
            .get(app)
            .map(|e| e.status.clone());
        if let Some(status) = status {
            return status.read().await.clone();
        }
        if let Some(status) = self.failures.lock().await.get(app).cloned() {
            return status;
        }
        let failure = self.bootstrap_failure.read().await.clone();
        RuntimeStatus {
            state: if failure.is_some() {
                "failed"
            } else {
                "stopped"
            }
            .into(),
            failure,
            execution_id: None,
        }
    }
    pub(crate) async fn logs(&self, app: &str) -> Vec<String> {
        let logs = self
            .executions
            .lock()
            .await
            .get(app)
            .map(|e| e.logs.clone());
        logs.map(|logs| {
            logs.lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .cloned()
                .collect()
        })
        .unwrap_or_default()
    }
    pub(crate) async fn upstream(
        &self,
        app: &str,
    ) -> Option<(
        std::net::SocketAddr,
        pioneer_protocol::AuthSecretString,
        CancellationToken,
    )> {
        let value = self
            .executions
            .lock()
            .await
            .get(app)
            .map(|e| (e.upstream.clone(), e.cancel.clone()));
        match value {
            Some((upstream, cancel)) if !cancel.is_cancelled() => upstream
                .read()
                .await
                .clone()
                .map(|(address, credential)| (address, credential, cancel)),
            _ => None,
        }
    }
}
impl MessageProcessor {
    pub(in crate::message) async fn stop_app_runtime(&self, app: &str) -> Result<()> {
        self.stop_dev_view(app).await;
        self.host_api.fence_app(app);
        let execution = self.app_runtime.executions.lock().await.get(app).map(|e| {
            (
                e.cancel.clone(),
                e.vm_pending.clone(),
                e.task.clone(),
                e.status.clone(),
            )
        });
        let Some((cancel, pending, task, status)) = execution else {
            self.app_runtime.failures.lock().await.remove(app);
            return Ok(());
        };
        cancel.cancel();
        // Independent callers share the same stop barrier; none can publish
        // removal or replacement while another caller still awaits this VM.
        let mut joining = task.lock().await;
        if let Some(handle) = joining.as_mut() {
            let _ = handle.await;
            joining.take();
        }
        if pending.load(Ordering::Acquire) {
            if self.cleanup_app_vm(app).await.is_err() {
                *status.write().await = RuntimeStatus {
                    state: "failed".into(),
                    failure: Some("apps.runtime_cleanup_not_confirmed".into()),
                    execution_id: None,
                };
                bail!("apps.runtime_cleanup_not_confirmed");
            }
            pending.store(false, Ordering::Release);
        }
        let mut executions = self.app_runtime.executions.lock().await;
        if executions
            .get(app)
            .is_some_and(|e| Arc::ptr_eq(&e.task, &task))
        {
            executions.remove(app);
        }
        drop(executions);
        drop(joining);
        self.app_runtime.failures.lock().await.remove(app);
        Ok(())
    }
    pub(crate) async fn start_app_runtimes(&self) {
        let mut worker = self.app_runtime.worker.lock().await;
        if worker.is_some() {
            return;
        }
        let cancel = CancellationToken::new();
        let child = cancel.clone();
        let owner = self.scoped_with_database_class(SqliteWriteClass::Maintenance);
        let task = tokio::spawn(async move {
            let recovery = async {
                let backend = owner
                    .app_runtime
                    .backend
                    .get_or_try_init(|| async {
                        provision::backend(&owner.artifact_runtime_home.join("apps-runtime"))
                    })
                    .await?
                    .clone();
                provision::recover(backend).await
            }
            .await;
            if recovery.is_err() {
                tracing::error!("App SDK recovery cleanup was not confirmed");
                *owner.app_runtime.bootstrap_failure.write().await =
                    Some("apps.runtime_cleanup_not_confirmed".into());
                return;
            }
            // No execution survives the private-home VM fence above. Stale
            // bootstrap directories contain only revoked execution credentials.
            let stale_root = owner.artifact_runtime_home.join("app-executions");
            let cleanup = super::super::package_delivery::prepare_cancellable(move |cancel| {
                if !stale_root.exists() {
                    return Ok(());
                }
                let metadata = std::fs::symlink_metadata(&stale_root)?;
                anyhow::ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "apps.invalid_execution_root"
                );
                for entry in std::fs::read_dir(&stale_root)? {
                    super::super::package_delivery::check_preparation(&cancel)?;
                    let entry = entry?;
                    let name = entry.file_name();
                    let Some(name) = name.to_str().filter(|name| {
                        name.len() == 21
                            && name
                                .bytes()
                                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                    }) else {
                        continue;
                    };
                    let path = stale_root.join(name);
                    if entry.file_type()?.is_dir() {
                        std::fs::remove_dir_all(path)?;
                    }
                }
                Ok(())
            });
            if tokio::select! { _ = child.cancelled() => return, result = cleanup => result }
                .is_err()
            {
                *owner.app_runtime.bootstrap_failure.write().await =
                    Some("apps.execution_bootstrap_recovery_failed".into());
                tracing::error!("App execution bootstrap recovery failed");
                return;
            }
            let database = owner.crud_store.database_connection();
            let mut changes = database.work_signal("apps.lifecycle").subscribe();
            loop {
                if child.is_cancelled() {
                    break;
                }
                let admitted = tokio::select! { _ = child.cancelled() => break, result = changes.begin_pass(&database) => result };
                if admitted.is_err() {
                    tokio::select! { _ = child.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                    continue;
                }
                let through = match owner.crud_store.app_runtime_frontier().await {
                    Ok(frontier) => frontier.unwrap_or_default(),
                    Err(_) => {
                        tokio::select! { _ = child.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                        continue;
                    }
                };
                let mut after = String::new();
                let mut seen = HashSet::new();
                let mut complete = true;
                loop {
                    let page = match owner.crud_store.app_runtime_page(&after, &through).await {
                        Ok(p) => p,
                        Err(_) => {
                            complete = false;
                            break;
                        }
                    };
                    if page.is_empty() {
                        break;
                    }
                    for (id, parent) in page {
                        after = id.clone();
                        seen.insert(id.clone());
                        if child.is_cancelled() {
                            complete = false;
                            break;
                        }
                        if let Some(parent) = parent {
                            if owner.reconcile_app(parent, &child).await.is_err() {
                                complete = false;
                                break;
                            }
                        } else {
                            let _ = owner.stop_app_runtime(&id).await;
                        }
                    }
                    tokio::task::yield_now().await;
                }
                if !complete {
                    tokio::select! { _ = child.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                    continue;
                }
                let absent: Vec<_> = owner
                    .app_runtime
                    .executions
                    .lock()
                    .await
                    .keys()
                    .filter(|id| !seen.contains(*id))
                    .cloned()
                    .collect();
                for id in absent {
                    let _ = owner.stop_app_runtime(&id).await;
                }
                tokio::select! { _ = child.cancelled() => break, _ = changes.changed() => {} }
            }
        });
        *worker = Some((cancel, task));
    }
    pub(crate) async fn shutdown_app_runtimes(&self) {
        if let Some((cancel, task)) = self.app_runtime.worker.lock().await.take() {
            cancel.cancel();
            let _ = task.await;
        }
        let ids: Vec<_> = self
            .app_runtime
            .executions
            .lock()
            .await
            .keys()
            .cloned()
            .collect();
        for id in ids {
            if self.stop_app_runtime(&id).await.is_err() {
                tracing::error!("App VM cleanup was not confirmed during shutdown");
            }
        }
    }
    async fn reconcile_app(
        &self,
        parent: pioneer_crud::PackageRecord,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        if super::dev::is_dev(&parent) {
            return Ok(());
        }
        let mutation = self.plugin_mutation_lock(&parent.id).await;
        let _guard = tokio::select! { _ = shutdown.cancelled() => return Ok(()), guard = mutation.lock_owned() => guard };
        let parent = match self
            .crud_store
            .find_package_installation(&parent.id)
            .await?
        {
            Some(current) if current.kind == pioneer_protocol::PackageKind::App => current,
            _ => {
                let _ = self.stop_app_runtime(&parent.id).await;
                return Ok(());
            }
        };
        let workspace = pioneer_crud::resolve_workspace_authorization_scope(
            &self.crud_store.database_connection(),
            &parent.workspace_id,
        )
        .await?;
        let active = parent.enabled
            && parent.state == "installed"
            && workspace.is_some_and(|scope| scope.is_active);
        let manifest = parent
            .app_manifest_json
            .as_deref()
            .and_then(|v| pioneer_apps::AppManifest::parse(v.as_bytes()).ok());
        if active && manifest.is_none() {
            if self.stop_app_runtime(&parent.id).await.is_ok() {
                self.app_runtime.failures.lock().await.insert(
                    parent.id,
                    RuntimeStatus {
                        state: "failed".into(),
                        failure: Some("apps.invalid_manifest".into()),
                        execution_id: None,
                    },
                );
            }
            return Ok(());
        }
        let active = active && manifest.as_ref().is_some_and(|m| m.runtime().is_some());
        let same = self
            .app_runtime
            .executions
            .lock()
            .await
            .get(&parent.id)
            .is_some_and(|e| e.revision == parent.revision && !e.cancel.is_cancelled());
        if active && same {
            return Ok(());
        }
        if self.stop_app_runtime(&parent.id).await.is_err() {
            return Ok(());
        }
        if !active {
            return Ok(());
        }
        if self
            .app_runtime
            .executions
            .lock()
            .await
            .values()
            .filter(|e| e.active.load(Ordering::Acquire) || e.vm_pending.load(Ordering::Acquire))
            .count()
            >= 8
        {
            self.app_runtime.failures.lock().await.insert(
                parent.id,
                RuntimeStatus {
                    state: "failed".into(),
                    failure: Some("apps.runtime_capacity".into()),
                    ..Default::default()
                },
            );
            return Ok(());
        }
        let active = Arc::new(AtomicBool::new(true));
        let vm_pending = Arc::new(AtomicBool::new(false));
        let sandbox = Arc::new(RwLock::new(None));
        let creation = Arc::new(Mutex::new(None));
        let logs = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
        let cancel = shutdown.child_token();
        let status = Arc::new(RwLock::new(RuntimeStatus {
            state: "preparing".into(),
            ..Default::default()
        }));
        let upstream = Arc::new(RwLock::new(None));
        let owner = self.clone();
        let record = parent.clone();
        let token = cancel.clone();
        let progress = status.clone();
        let endpoint = upstream.clone();
        let running = active.clone();
        let pending = vm_pending.clone();
        let sandbox_handle = sandbox.clone();
        let creation_handle = creation.clone();
        let output = logs.clone();
        let task = tokio::spawn(async move {
            let app_id = record.id.clone();
            let workspace_id = record.workspace_id.clone();
            let result = std::panic::AssertUnwindSafe(owner.run_app(
                record,
                &token,
                &progress,
                &endpoint,
                &pending,
                &sandbox_handle,
                &creation_handle,
                &output,
            ))
            .catch_unwind()
            .await;
            let cleanup = if pending.load(Ordering::Acquire) {
                owner.cleanup_app_vm(&app_id).await
            } else {
                Ok(())
            };
            if cleanup.is_ok() {
                pending.store(false, Ordering::Release);
            }
            *endpoint.write().await = None;
            if let Some(views) = owner.view_grant_service() {
                views.invalidate_app_views(&workspace_id, Some(&app_id), None);
            }
            let mut state = progress.write().await;
            state.state = if token.is_cancelled() && cleanup.is_ok() {
                "stopped"
            } else {
                "failed"
            }
            .into();
            if cleanup.is_err() {
                state.failure = Some("apps.runtime_cleanup_not_confirmed".into());
            } else if !token.is_cancelled() {
                state.failure = Some(
                    match &result {
                        Ok(Err(error)) => match error.to_string().as_str() {
                            "apps.unsupported_host_platform" => "apps.unsupported_host_platform",
                            "apps.virtualization_unavailable" => "apps.virtualization_unavailable",
                            "apps.crash_budget_exhausted" => "apps.crash_budget_exhausted",
                            _ => "apps.execution_failed",
                        },
                        _ => "apps.execution_failed",
                    }
                    .into(),
                );
            }
            running.store(false, Ordering::Release);
            drop(state);
            owner
                .crud_store
                .database_connection()
                .work_signal("apps.lifecycle")
                .notify();
        });
        self.app_runtime.executions.lock().await.insert(
            parent.id,
            Execution {
                active,
                vm_pending,
                sandbox,
                creation,
                logs,
                revision: parent.revision,
                cancel,
                task: Arc::new(Mutex::new(Some(task))),
                status,
                upstream,
            },
        );
        Ok(())
    }
    async fn cleanup_app_vm(&self, app: &str) -> Result<()> {
        let slot = self
            .app_runtime
            .executions
            .lock()
            .await
            .get(app)
            .map(|e| e.sandbox.clone());
        let creation = self
            .app_runtime
            .executions
            .lock()
            .await
            .get(app)
            .map(|e| e.creation.clone());
        if let Some(creation) = creation {
            let mut creating = creation.lock().await;
            if let Some(task) = creating.as_mut() {
                task.abort();
                if let Ok(Ok(sandbox)) = task.await {
                    if let Some(slot) = &slot {
                        *slot.write().await = Some(Arc::new(sandbox));
                    }
                }
                creating.take();
            }
        }
        let deadline = tokio::time::Instant::now() + provision::CLEANUP_BUDGET;
        let retained = match &slot {
            Some(slot) => slot.read().await.clone(),
            None => None,
        };
        if let Some(sandbox) = retained {
            provision::cleanup_before(deadline, async {
                sandbox
                    .destroy_with(microsandbox::sandbox::DestroyOptions {
                        force: true,
                        timeout: provision::CLEANUP_BUDGET,
                    })
                    .await?;
                Ok(())
            })
            .await?;
        } else {
            // Interrupted creation may have published its SDK catalog record
            // before returning a handle. Recover that same named local resource.
            let backend = self
                .app_runtime
                .backend
                .get()
                .context("apps.runtime_unavailable")?
                .clone();
            let name = format!(
                "pioneer-app-{}",
                hex::encode(sha2::Sha256::digest(app.as_bytes()))
            );
            provision::remove(backend, &name, deadline).await?;
        }
        if let Some(slot) = slot {
            slot.write().await.take();
        }
        Ok(())
    }
    async fn run_app(
        &self,
        parent: pioneer_crud::PackageRecord,
        cancel: &CancellationToken,
        status: &Arc<RwLock<RuntimeStatus>>,
        upstream: &Arc<RwLock<Option<(std::net::SocketAddr, pioneer_protocol::AuthSecretString)>>>,
        pending: &Arc<AtomicBool>,
        sandbox_slot: &Arc<RwLock<Option<Arc<microsandbox::Sandbox>>>>,
        creation_slot: &Arc<Mutex<Option<SdkCreation>>>,
        logs: &Arc<std::sync::Mutex<std::collections::VecDeque<String>>>,
    ) -> Result<()> {
        let root = PathBuf::from(&parent.package_path);
        let expected = parent.package_fingerprint.clone();
        let limits = self
            .package_delivery_context()
            .limits_for_package(parent.kind);
        let preparing = super::super::package_delivery::prepare_cancellable(move |cancellation| {
            let snapshot =
                pioneer_packages::Snapshot::capture(&root, limits, || cancellation.is_cancelled())?;
            anyhow::ensure!(snapshot.tree_digest() == expected, "apps.package_changed");
            Ok(pioneer_apps::load_for_architecture(
                &snapshot,
                super::architecture()?,
            )?)
        });
        let package = tokio::select! { _ = cancel.cancelled() => bail!("apps.cancelled"), result = preparing => result? };
        let runtime = package.manifest.runtime().context("apps.runtime_missing")?;
        let architecture = super::architecture()?;
        let artifact = runtime.artifact(architecture)?;
        let image = provision::image(runtime.environment)?;
        let backend = self
            .app_runtime
            .backend
            .get()
            .context("apps.runtime_unavailable")?
            .clone();
        let (_, preparation_failures) = prepare_with_recovery(cancel, status, || async {
            let _preparation = tokio::select! { _ = cancel.cancelled() => bail!("apps.cancelled"), lock = self.app_runtime.preparation.lock() => lock };
            provision::prepare(&backend, cancel).await
        }).await?;
        let name = format!(
            "pioneer-app-{}",
            hex::encode(sha2::Sha256::digest(parent.id.as_bytes()))
        );
        let package_root = tokio::fs::canonicalize(&parent.package_path).await?;
        let data_root = tokio::fs::canonicalize(&parent.data_path).await?;
        let cwd = pioneer_apps::canonical_package_path(&runtime.cwd, "runtime/cwd")?;
        for attempt in preparation_failures..5u32 {
            if cancel.is_cancelled() {
                break;
            }
            self.publish_app_runtime_changed(&parent, None).await;
            let execution_id = pioneer_protocol::generate_id(21);
            *status.write().await = RuntimeStatus {
                state: "starting".into(),
                failure: None,
                execution_id: Some(execution_id.clone()),
            };
            let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
            let address = listener.local_addr()?;
            drop(listener);
            let bootstrap_root = self
                .artifact_runtime_home
                .join("app-executions")
                .join(&execution_id);
            tokio::fs::create_dir_all(bootstrap_root.parent().context("apps.invalid_path")?)
                .await?;
            tokio::fs::create_dir(&bootstrap_root).await?;
            let mut bootstrap_cleanup = super::super::package_delivery::TemporaryDirectory::new()?;
            bootstrap_cleanup.remove_on_drop(bootstrap_root.clone());
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tokio::fs::set_permissions(&bootstrap_root, std::fs::Permissions::from_mode(0o700))
                    .await?;
            }
            let port = self
                .gateway_listen_addr
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
                .context("apps.gateway_endpoint_unavailable")?
                .port();
            let (credential, _) = self.host_api.issue(
                parent.id.clone(),
                parent.workspace_id.clone(),
                execution_id.clone(),
                cancel.child_token(),
            );
            let lease = HostLease {
                api: self.host_api.clone(),
                execution: execution_id.clone(),
            };
            let secret = credential.clone();
            let host = serde_json::json!({"endpoint":format!("ws://host.microsandbox.internal:{port}/apps/host"),"credential":credential,"capabilities":["host.pty"]});
            let proxy_credential =
                pioneer_protocol::AuthSecretString::new(hex::encode(rand::random::<[u8; 32]>()));
            let bootstrap = serde_json::json!({"execution_id":execution_id,"host_api":host,"ui_base_path":"/","ui_proxy":{"credential":proxy_credential}});
            let bootstrap_path = bootstrap_root.join("bootstrap.json");
            let mut options = tokio::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                options.mode(0o600);
            }
            use tokio::io::AsyncWriteExt;
            let mut file = options.open(&bootstrap_path).await?;
            file.write_all(&serde_json::to_vec(&bootstrap)?).await?;
            file.sync_all().await?;
            drop(file);
            let mut policy =
                microsandbox::NetworkPolicy::from_profiles([microsandbox::NetworkProfile::Public]);
            let mut host_policy = microsandbox::NetworkPolicy::builder()
                .egress(|rule| rule.tcp().port(port).allow_host())
                .build()?;
            host_policy.rules.extend(policy.rules);
            policy.rules = host_policy.rules;
            let mut builder = microsandbox::Sandbox::builder(&name)
                .image(image.as_str())
                .pull_policy(microsandbox::sandbox::PullPolicy::IfMissing)
                .memory(512u32)
                .cpus(1)
                .root_disk(1024u32)
                .user("65532:65532")
                .label("pioneer.apps", "true")
                .workdir(format!("/app/{cwd}"))
                .entrypoint(runtime.command.clone())
                .cmd(Vec::<String>::new())
                .volume("/app", |m| {
                    m.bind(&package_root).readonly().owner(65532, 65532)
                })
                .volume("/data", |m| {
                    m.bind(&data_root).quota(1024u32).owner(65532, 65532)
                })
                .volume("/pioneer", |m| {
                    m.bind(&bootstrap_root).readonly().owner(65532, 65532)
                })
                .env("PIONEER_BOOTSTRAP", "/pioneer/bootstrap.json")
                .env("APP_ROOT", "/app")
                .env("APP_DATA", "/data")
                .network(|network| network.enabled(true).policy(policy))
                .port_bind(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    address.port(),
                    runtime.port,
                );
            if let Some(artifact) = artifact {
                let path = pioneer_packages::containment::resolve_contained(
                    &package_root,
                    &package_root.join(&artifact.path),
                )?;
                builder =
                    builder.volume("/payload", |m| m.bind(path).readonly().owner(65532, 65532));
            }
            // The SDK's public creation job supports cancellation by abort.
            // Retain that SDK handle in the existing execution owner and await
            // settlement before attempting catalog cleanup or replacement.
            pending.store(true, Ordering::Release);
            let mut creating = creation_slot.lock().await;
            let scoped: Arc<dyn microsandbox::Backend> = backend.clone();
            let (progress, task) =
                microsandbox::with_backend(scoped, async { builder.create_with_progress() })
                    .await?;
            drop(progress);
            *creating = Some(task);
            let task = creating.as_mut().expect("owned SDK creation job");
            let (settled, interrupted) = tokio::select! {
                biased;
                result = &mut *task => (result, None),
                _ = cancel.cancelled() => { task.abort(); ((&mut *task).await, Some("apps.cancelled")) },
                _ = tokio::time::sleep(Duration::from_secs(180)) => { task.abort(); ((&mut *task).await, Some("apps.startup_timeout")) },
            };
            creating.take();
            drop(creating);
            let created: Result<microsandbox::Sandbox> = match settled {
                Ok(result) => result.map_err(Into::into),
                Err(error) if error.is_cancelled() => Err(anyhow::anyhow!(
                    interrupted.unwrap_or("apps.creation_cancelled")
                )),
                Err(error) => Err(error.into()),
            };
            let sandbox = match created {
                Ok(sandbox) => Arc::new(sandbox),
                Err(error) => {
                    self.cleanup_app_vm(&parent.id).await?;
                    pending.store(false, Ordering::Release);
                    if cancel.is_cancelled() {
                        return Ok(());
                    }
                    let error: anyhow::Error = error.into();
                    if provision::retryable(&error) && attempt < 4 {
                        tokio::select! { _ = cancel.cancelled() => return Ok(()), _ = tokio::time::sleep(Duration::from_secs(1u64 << attempt)) => {} }
                        continue;
                    }
                    return Err(error);
                }
            };
            *sandbox_slot.write().await = Some(sandbox.clone());
            if cancel.is_cancelled() || interrupted.is_some() {
                self.cleanup_app_vm(&parent.id).await?;
                pending.store(false, Ordering::Release);
                if cancel.is_cancelled() {
                    return Ok(());
                }
                bail!("apps.startup_timeout");
            }
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(2))
                .build()?;
            let mut process = sandbox
                .exec_default_stream_with(|options| options.stdin_null().tty(false))
                .await?;
            let secrets = vec![secret, proxy_credential.expose_secret().to_owned()];
            let output_logs = logs.clone();
            let (exited_tx, mut exited) = tokio::sync::oneshot::channel();
            let output = tokio::spawn(async move {
                let mut stdout = (Vec::new(), false);
                let mut stderr = (Vec::new(), false);
                while let Some(event) = process.recv().await {
                    match event {
                        microsandbox::ExecEvent::Stdout(bytes) => {
                            log_chunk(&bytes, &mut stdout, &output_logs, &secrets)
                        }
                        microsandbox::ExecEvent::Stderr(bytes) => {
                            log_chunk(&bytes, &mut stderr, &output_logs, &secrets)
                        }
                        microsandbox::ExecEvent::Exited { .. }
                        | microsandbox::ExecEvent::Failed(_) => break,
                        _ => {}
                    }
                }
                let _ = exited_tx.send(());
            });
            let ready = async {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
                loop {
                    if client
                        .get(format!("http://{address}{}", runtime.readiness.path))
                        .header(
                            "pioneer-app-execution-credential",
                            proxy_credential.expose_secret(),
                        )
                        .send()
                        .await
                        .is_ok_and(|r| r.status().is_success())
                    {
                        let unauthenticated = client
                            .get(format!("http://{address}{}", runtime.readiness.path))
                            .send()
                            .await?;
                        anyhow::ensure!(
                            matches!(
                                unauthenticated.status(),
                                reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
                            ),
                            "apps.ui_proxy_authentication_required"
                        );
                        return Ok::<_, anyhow::Error>(());
                    }
                    if tokio::time::Instant::now() >= deadline {
                        bail!("apps.readiness_timeout");
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            };
            let ready = tokio::select! { _ = cancel.cancelled() => false, _ = &mut exited => false, result = ready => result.is_ok() };
            if ready {
                *upstream.write().await = Some((address, proxy_credential.clone()));
                status.write().await.state = "ready".into();
                self.publish_app_runtime_changed(&parent, Some(execution_id.clone()))
                    .await;
                tokio::select! { _ = cancel.cancelled() => {}, _ = &mut exited => {}, _ = sandbox.wait() => {} }
            }
            *upstream.write().await = None;
            drop(lease);
            if let Some(views) = self.view_grant_service() {
                views.invalidate_app_views(&parent.workspace_id, Some(&parent.id), None);
            }
            let cleanup = self.cleanup_app_vm(&parent.id).await;
            output.abort();
            let _ = output.await;
            cleanup?;
            pending.store(false, Ordering::Release);
            self.publish_app_runtime_changed(&parent, None).await;
            if cancel.is_cancelled() {
                return Ok(());
            }
            status.write().await.state = "backoff".into();
            tokio::select! { _ = cancel.cancelled() => return Ok(()), _ = tokio::time::sleep(Duration::from_secs(1u64 << attempt)) => {} }
        }
        bail!("apps.crash_budget_exhausted")
    }
}
// Preparation failures consume the same finite execution-attempt budget as VM
// failures. Retained failed entries are deliberately not restarted by self-wakes.
async fn prepare_with_recovery<T, F, Fut>(
    cancel: &CancellationToken,
    status: &Arc<RwLock<RuntimeStatus>>,
    mut prepare: F,
) -> Result<(T, u32)>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    for attempt in 0..5u32 {
        status.write().await.state = "preparing".into();
        if cancel.is_cancelled() {
            bail!("apps.cancelled");
        }
        // Preparation owns cancellation and must finish its cleanup before the
        // retry owner advances; dropping it here could release admission before SDK setup settles.
        let result = prepare().await;
        if cancel.is_cancelled() {
            bail!("apps.cancelled");
        }
        match result {
            Ok(value) => return Ok((value, attempt)),
            Err(error) if provision::retryable(&error) && attempt < 4 => {
                status.write().await.state = "backoff".into();
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => bail!("apps.cancelled"),
                    _ = tokio::time::sleep(Duration::from_secs(1u64 << attempt)) => {},
                }
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("finite preparation budget")
}

fn log_chunk(
    bytes: &[u8],
    line: &mut (Vec<u8>, bool),
    logs: &Arc<std::sync::Mutex<std::collections::VecDeque<String>>>,
    secrets: &[String],
) {
    for byte in bytes {
        if *byte == b'\n' {
            if !line.1 {
                let mut text = String::from_utf8_lossy(&line.0).into_owned();
                for secret in secrets {
                    if !secret.is_empty() {
                        text = text.replace(secret, "[REDACTED]");
                    }
                }
                let text: String = text
                    .chars()
                    .filter(|c| !c.is_control() || *c == '\t')
                    .take(1024)
                    .collect();
                let mut tail = logs.lock().unwrap_or_else(|e| e.into_inner());
                if tail.len() == 64 {
                    tail.pop_front();
                }
                tail.push_back(text);
            }
            line.0.clear();
            line.1 = false;
        } else if !line.1 {
            if line.0.len() == 8192 {
                line.1 = true;
                line.0.clear();
            } else {
                line.0.push(*byte);
            }
        }
    }
}

struct HostLease {
    api: Arc<crate::host_api::HostApi>,
    execution: String,
}
impl Drop for HostLease {
    fn drop(&mut self) {
        self.api.revoke_execution(&self.execution);
    }
}

#[cfg(test)]
mod preparation_recovery_regressions {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn preparation_recovery_is_finite_and_invalid_setup_failure_is_terminal() {
        for (failures, invalid_setup, expected_calls, succeeds) in [
            (1, false, 2, true),
            (9, false, 5, false),
            (1, true, 1, false),
        ] {
            let cancel = CancellationToken::new();
            let status = Arc::new(RwLock::new(RuntimeStatus::default()));
            let mut calls = 0;
            let started = tokio::time::Instant::now();
            let result = prepare_with_recovery(&cancel, &status, || {
                calls += 1;
                std::future::ready(if calls <= failures {
                    if invalid_setup {
                        Err(microsandbox::MicrosandboxError::RuntimeIncomplete(
                            "partial installation".into(),
                        )
                        .into())
                    } else {
                        Err(microsandbox::MicrosandboxError::Io(std::io::Error::from(
                            std::io::ErrorKind::TimedOut,
                        ))
                        .into())
                    }
                } else {
                    Ok("prepared")
                })
            })
            .await;
            assert_eq!(calls, expected_calls);
            assert_eq!(
                started.elapsed(),
                Duration::from_secs(if invalid_setup {
                    0
                } else if succeeds {
                    1
                } else {
                    15
                })
            );
            assert_eq!(result.is_ok(), succeeds);
            if succeeds {
                assert_eq!(result.unwrap(), ("prepared", 1));
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn cancellation_during_preparation_backoff_does_not_attempt_again() {
        let cancel = CancellationToken::new();
        let status = Arc::new(RwLock::new(RuntimeStatus::default()));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let task = {
            let cancel = cancel.clone();
            let status = status.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                prepare_with_recovery(&cancel, &status, || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Err::<(), _>(
                        microsandbox::MicrosandboxError::Io(std::io::Error::from(
                            std::io::ErrorKind::TimedOut,
                        ))
                        .into(),
                    ))
                })
                .await
            })
        };
        while status.read().await.state != "backoff" {
            tokio::task::yield_now().await;
        }
        cancel.cancel();
        assert_eq!(
            task.await.unwrap().unwrap_err().to_string(),
            "apps.cancelled"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod owned_sdk_operation_tests {
    use super::*;
    #[tokio::test]
    async fn cancellation_waits_for_owned_setup_to_settle() {
        let cancel = CancellationToken::new();
        let status = Arc::new(RwLock::new(RuntimeStatus::default()));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let settled = Arc::new(AtomicBool::new(false));
        let task = {
            let cancel = cancel.clone();
            let started = started.clone();
            let release = release.clone();
            let settled = settled.clone();
            tokio::spawn(async move {
                prepare_with_recovery(&cancel, &status, || async {
                    started.notify_one();
                    release.notified().await;
                    settled.store(true, Ordering::Release);
                    Ok(())
                })
                .await
            })
        };
        started.notified().await;
        cancel.cancel();
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "cancellation must not drop an owned setup future"
        );
        release.notify_one();
        assert_eq!(
            task.await.unwrap().unwrap_err().to_string(),
            "apps.cancelled"
        );
        assert!(settled.load(Ordering::Acquire));
    }
}

#[cfg(test)]
mod backend_execution_access_tests {
    use super::*;
    fn app(id: &str, workspace: &str) -> pioneer_crud::PackageRecord {
        pioneer_crud::PackageRecord {
        kind: pioneer_protocol::PackageKind::App,
        app_manifest_json: Some(serde_json::json!({"slug":"example-app", "name":"Example", "version":"1.0.0", "shortDescription":"Example", "developer":{"name":"Author"}, "icon":{"light":"./icon.png", "dark":"./icon.png"}}).to_string()),
        app_localizations_json: Some("{}".into()),
        operation_error_json: None,
        id: id.into(),
        workspace_id: workspace.into(),
        name: "example-app".into(),
        version: None,
        source_upload_id: format!("upload-{id}"),
        package_path: format!("/managed/apps/{id}/package"),
        data_path: format!("/managed/apps/{id}/data"),
        package_fingerprint: "tree".into(),
        enabled: true,
        allow_implicit_invocation: false,
        state: "installing".into(),
        revision: 1,
        display_name: None,
        short_description: None,
        description: None,
        long_description: None,
        developer_name: None,
        author_name: None,
        author_email: None,
        author_url: None,
        homepage: None,
        repository: None,
        documentation_url: None,
        support_url: None,
        privacy_policy_url: None,
        terms_of_service_url: None,
        license: None,
        category: None,
        brand_color: None,
        logo_path: None,
        logo_revision: None,
        composer_icon_path: None,
        composer_icon_revision: None,
        pending_json: None,
        last_error: None,
        created_at: chrono::DateTime::from_timestamp(1, 0).unwrap().fixed_offset(),
        updated_at: chrono::DateTime::from_timestamp(1, 0).unwrap().fixed_offset(),
    }
    }
    #[tokio::test]
    async fn current_app_execution_authenticates_without_consent_and_stop_retires_it() {
        let (processor, _, _, _, _, workspace) =
            crate::message::tests::setup_workspace_message_processor().await;
        let home = tempfile::tempdir().unwrap();
        let processor = processor.with_runtime_home_for_tests(home.path().to_owned());
        let id = pioneer_protocol::generate_id(21);
        processor
            .crud_store
            .insert_package_installation(&app(&id, &workspace))
            .await
            .unwrap();
        processor
            .crud_store
            .settle_plugin_installation(&id, 1, "installed", None)
            .await
            .unwrap();
        let installed = processor
            .crud_store
            .find_package_installation(&id)
            .await
            .unwrap()
            .unwrap();
        let cancel = CancellationToken::new();
        let execution_id = pioneer_protocol::generate_id(21);
        processor.app_runtime.executions.lock().await.insert(
            id.clone(),
            Execution {
                active: Arc::new(AtomicBool::new(true)),
                vm_pending: Arc::new(AtomicBool::new(false)),
                sandbox: Arc::new(RwLock::new(None)),
                creation: Arc::new(Mutex::new(None)),
                logs: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
                revision: installed.revision,
                cancel: cancel.clone(),
                task: Arc::new(Mutex::new(None)),
                status: Arc::new(RwLock::new(RuntimeStatus {
                    state: "ready".into(),
                    execution_id: Some(execution_id.clone()),
                    failure: None,
                })),
                upstream: Arc::new(RwLock::new(None)),
            },
        );
        let (_, current) = processor.host_api.issue(
            id.clone(),
            workspace.clone(),
            execution_id,
            cancel.child_token(),
        );
        processor.validate_host_execution(&current).await.unwrap();
        let status = processor.app_runtime.executions.lock().await[&id]
            .status
            .clone();
        status.write().await.state = "failed".into();
        assert!(
            processor.validate_host_execution(&current).await.is_err(),
            "a retained execution ID alone is not live authority"
        );
        status.write().await.state = "ready".into();

        let (_, foreign) = processor.host_api.issue(
            id.clone(),
            workspace,
            pioneer_protocol::generate_id(21),
            CancellationToken::new(),
        );
        assert!(processor.validate_host_execution(&foreign).await.is_err());
        processor.stop_app_runtime(&id).await.unwrap();
        assert!(processor.validate_host_execution(&current).await.is_err());
        assert!(
            !processor
                .app_runtime
                .executions
                .lock()
                .await
                .contains_key(&id)
        );
    }
}
