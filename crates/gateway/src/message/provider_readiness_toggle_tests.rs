//! Deterministic supervisor coverage. All enabled probes use the in-memory
//! fake below; these fixtures never execute provider binaries.
use super::tests::{mcp_readiness, model_snapshot, runtime_summary};
use super::*;
use pioneer_protocol::{GatewayCliRuntimeSettings, GatewaySettingsUpdate};

fn ids(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn selected(values: &[&str]) -> ProviderWarmupScope {
    ProviderWarmupScope::cli_instances(ids(values))
}

fn ready_result(mut summary: RuntimeSummary, marker: &str) -> CliRuntimeProbeResult {
    summary.status = RuntimeStatus::Ready;
    summary.capabilities.supports_mcp_tools = true;
    summary.account = Some(pioneer_protocol::RuntimeAccountSnapshot {
        authenticated: true,
        account_id: Some(format!("account-{marker}")),
        email: Some(format!("{marker}@example.test")),
        display_name: Some(marker.into()),
        plan: Some("fake".into()),
        auth_method: Some("fixture".into()),
    });
    summary.version = Some(marker.into());
    summary.models_refreshed_at_unix_ms = Some(1);
    summary.diagnostics = vec![pioneer_protocol::RuntimeDiagnostic {
        level: pioneer_protocol::RuntimeDiagnosticLevel::Info,
        code: "fixture.ready".into(),
        message: marker.into(),
    }];
    summary.recent_stderr = vec![format!("stderr-{marker}")];
    let mut mcp = mcp_readiness();
    match summary.kind {
        CLIAgentRuntimeKind::Codex => {}
        CLIAgentRuntimeKind::Claude => {
            mcp.injection = pioneer_protocol::CliMcpInjectionKind::ClaudeStrictStdioMcp;
            mcp.projection_update =
                pioneer_protocol::CliMcpProjectionUpdateKind::ClaudeRestartProcessResumeSession;
        }
        CLIAgentRuntimeKind::OpenCode => {
            mcp.injection = pioneer_protocol::CliMcpInjectionKind::OpenCodeDynamicStdioMcp;
            mcp.projection_update =
                pioneer_protocol::CliMcpProjectionUpdateKind::OpenCodeRestartServerResumeSession;
        }
    }
    CliRuntimeProbeResult {
        summary,
        models: Some(model_snapshot(marker)),
        mcp_readiness: Some(mcp),
    }
}

async fn publish_result(
    supervisor: &ProviderReadinessSupervisor,
    workspace: &str,
    generation: u64,
    scope: &ProviderWarmupScope,
    results: Vec<CliRuntimeProbeResult>,
) -> Option<(
    CliRuntimeReadinessSnapshot,
    Vec<CliRuntimeReadinessChange>,
    BTreeSet<String>,
)> {
    supervisor
        .merge_probe_results_if_generation(
            workspace,
            generation,
            scope,
            results
                .iter()
                .map(|result| result.summary.clone())
                .collect(),
            results
                .iter()
                .filter_map(|result| {
                    result
                        .models
                        .clone()
                        .map(|models| (result.summary.runtime_id.clone(), models))
                })
                .collect(),
            results
                .into_iter()
                .filter_map(|result| {
                    result
                        .mcp_readiness
                        .map(|mcp| (result.summary.runtime_id, mcp))
                })
                .collect(),
        )
        .await
}

async fn seeded_supervisor() -> ProviderReadinessSupervisor {
    let supervisor = ProviderReadinessSupervisor::default();
    let generation = supervisor
        .begin_cli_probe("ws", &ProviderWarmupScope::CliOnly)
        .await;
    publish_result(
        &supervisor,
        "ws",
        generation,
        &ProviderWarmupScope::CliOnly,
        ["a", "b", "c"]
            .into_iter()
            .map(|id| ready_result(runtime_summary(id, RuntimeStatus::Ready), id))
            .collect(),
    )
    .await
    .unwrap();
    supervisor
}

fn assert_preserved(
    before: &CliRuntimeReadinessSnapshot,
    after: &CliRuntimeReadinessSnapshot,
    id: &str,
) {
    assert_eq!(
        Some(
            before
                .runtimes
                .iter()
                .find(|runtime| runtime.runtime_id == id)
                .expect("preserved runtime must exist in the baseline"),
        ),
        after
            .runtimes
            .iter()
            .find(|runtime| runtime.runtime_id == id)
    );
    assert_eq!(before.models.get(id), after.models.get(id));
    assert_eq!(before.mcp_readiness.get(id), after.mcp_readiness.get(id));
}

#[tokio::test]
async fn toggle_preserves_every_unaffected_artifact_and_rejects_old_results() {
    let supervisor = seeded_supervisor().await;
    let before = supervisor.snapshot("ws").await.unwrap();
    let old = supervisor.begin_cli_probe("ws", &selected(&["a"])).await;
    let mut disabled = runtime_summary("a", RuntimeStatus::Disabled);
    disabled.enabled = false;
    let changes = supervisor
        .invalidate_instances("ws", &ids(&["a"]), vec![disabled.clone()])
        .await;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].runtime, disabled);
    assert_eq!(changes[0].revision, before.revision + 1);
    let after = supervisor.snapshot("ws").await.unwrap();
    for id in ["b", "c"] {
        assert_preserved(&before, &after, id);
    }
    assert!(!after.models.contains_key("a"));
    assert!(!after.mcp_readiness.contains_key("a"));
    let (late, changes, accepted) = publish_result(
        &supervisor,
        "ws",
        old,
        &selected(&["a"]),
        vec![ready_result(
            runtime_summary("a", RuntimeStatus::Ready),
            "old",
        )],
    )
    .await
    .unwrap();
    assert!(changes.is_empty());
    assert!(accepted.is_empty());
    assert_eq!(late, after);

    // An equal final configuration is still a different epoch.
    supervisor
        .invalidate_instances(
            "ws",
            &ids(&["a"]),
            vec![runtime_summary("a", RuntimeStatus::Initializing)],
        )
        .await;
    let (late, changes, _) = publish_result(
        &supervisor,
        "ws",
        old,
        &selected(&["a"]),
        vec![ready_result(
            runtime_summary("a", RuntimeStatus::Ready),
            "old",
        )],
    )
    .await
    .unwrap();
    assert!(changes.is_empty());
    assert!(matches!(
        late.runtimes
            .iter()
            .find(|r| r.runtime_id == "a")
            .unwrap()
            .status,
        RuntimeStatus::Initializing
    ));
    let current = supervisor.begin_cli_probe("ws", &selected(&["a"])).await;
    let (ready, _, _) = publish_result(
        &supervisor,
        "ws",
        current,
        &selected(&["a"]),
        vec![ready_result(
            runtime_summary("a", RuntimeStatus::Ready),
            "new",
        )],
    )
    .await
    .unwrap();
    for id in ["b", "c"] {
        assert_preserved(&before, &ready, id);
    }
    assert_eq!(ready.models["a"].result.models[0].id, "new");
}

#[tokio::test]
async fn deleted_instance_cannot_return_from_a_late_full_probe() {
    let supervisor = seeded_supervisor().await;
    let old = supervisor
        .begin_cli_probe("ws", &ProviderWarmupScope::CliOnly)
        .await;
    let changes = supervisor
        .invalidate_instances("ws", &ids(&["a"]), Vec::new())
        .await;
    assert_eq!(changes.len(), 1);
    assert!(changes[0].removed);
    let before = supervisor.snapshot("ws").await.unwrap();
    let (after, _, accepted) = publish_result(
        &supervisor,
        "ws",
        old,
        &ProviderWarmupScope::CliOnly,
        ["a", "b", "c"]
            .into_iter()
            .map(|id| ready_result(runtime_summary(id, RuntimeStatus::Ready), id))
            .collect(),
    )
    .await
    .unwrap();
    assert!(!accepted.contains("a"));
    assert_eq!(before, after);
    assert!(!after.models.contains_key("a"));
    assert!(!after.mcp_readiness.contains_key("a"));
}

#[tokio::test]
async fn overlapping_full_and_partial_probes_publish_only_fresh_instances_in_either_order() {
    for full_finishes_first in [false, true] {
        let supervisor = seeded_supervisor().await;
        let full = supervisor
            .begin_cli_probe("ws", &ProviderWarmupScope::CliOnly)
            .await;
        supervisor
            .invalidate_instances(
                "ws",
                &ids(&["a"]),
                vec![runtime_summary("a", RuntimeStatus::Initializing)],
            )
            .await;
        let partial = supervisor.begin_cli_probe("ws", &selected(&["a"])).await;
        let full_scope = ProviderWarmupScope::CliOnly;
        let finish_full = || {
            publish_result(
                &supervisor,
                "ws",
                full,
                &full_scope,
                ["a", "b", "c"]
                    .into_iter()
                    .map(|id| ready_result(runtime_summary(id, RuntimeStatus::Ready), "full"))
                    .collect(),
            )
        };
        let scope = selected(&["a"]);
        let finish_partial = || {
            publish_result(
                &supervisor,
                "ws",
                partial,
                &scope,
                vec![ready_result(
                    runtime_summary("a", RuntimeStatus::Ready),
                    "partial",
                )],
            )
        };
        if full_finishes_first {
            finish_full().await.unwrap();
            finish_partial().await.unwrap();
        } else {
            finish_partial().await.unwrap();
            finish_full().await.unwrap();
        }
        let snapshot = supervisor.snapshot("ws").await.unwrap();
        assert_eq!(snapshot.models["a"].result.models[0].id, "partial");
        for id in ["b", "c"] {
            assert_eq!(snapshot.models[id].result.models[0].id, "full");
            assert_eq!(
                snapshot
                    .runtimes
                    .iter()
                    .find(|r| r.runtime_id == id)
                    .unwrap()
                    .version
                    .as_deref(),
                Some("full")
            );
            assert!(snapshot.mcp_readiness.contains_key(id));
        }
        // Conversely a later full execution owns overlapping publication.
        let newer = supervisor
            .begin_cli_probe("ws", &ProviderWarmupScope::CliOnly)
            .await;
        publish_result(
            &supervisor,
            "ws",
            newer,
            &ProviderWarmupScope::CliOnly,
            ["a", "b", "c"]
                .into_iter()
                .map(|id| ready_result(runtime_summary(id, RuntimeStatus::Ready), "new-full"))
                .collect(),
        )
        .await
        .unwrap();
        assert!(finish_partial().await.is_none());
    }
}

#[tokio::test]
async fn successive_changes_and_first_seed_are_fenced_per_instance() {
    let supervisor = ProviderReadinessSupervisor::default();
    let old = supervisor.cli_generation("ws").await;
    supervisor
        .invalidate_instances(
            "ws",
            &ids(&["a"]),
            vec![runtime_summary("a", RuntimeStatus::Initializing)],
        )
        .await;
    assert!(supervisor.snapshot("ws").await.is_none());
    assert!(
        supervisor
            .insert_snapshot_if_absent_if_generation(
                "ws",
                old,
                vec![runtime_summary("a", RuntimeStatus::Ready)]
            )
            .await
            .is_none()
    );
    let generation = supervisor.cli_generation("ws").await;
    supervisor
        .insert_snapshot_if_absent_if_generation(
            "ws",
            generation,
            ["a", "b", "c"]
                .into_iter()
                .map(|id| runtime_summary(id, RuntimeStatus::Initializing))
                .collect(),
        )
        .await
        .unwrap();
    let a = supervisor.begin_cli_probe("ws", &selected(&["a"])).await;
    supervisor
        .invalidate_instances(
            "ws",
            &ids(&["b"]),
            vec![runtime_summary("b", RuntimeStatus::Initializing)],
        )
        .await;
    publish_result(
        &supervisor,
        "ws",
        a,
        &selected(&["a"]),
        vec![ready_result(
            runtime_summary("a", RuntimeStatus::Ready),
            "a",
        )],
    )
    .await
    .unwrap();
    let b = supervisor.begin_cli_probe("ws", &selected(&["b"])).await;
    publish_result(
        &supervisor,
        "ws",
        b,
        &selected(&["b"]),
        vec![ready_result(
            runtime_summary("b", RuntimeStatus::Ready),
            "b",
        )],
    )
    .await
    .unwrap();
    let snapshot = supervisor.snapshot("ws").await.unwrap();
    assert_eq!(snapshot.runtimes.len(), 3);
    assert_eq!(snapshot.models["a"].result.models[0].id, "a");
    assert_eq!(snapshot.models["b"].result.models[0].id, "b");
    assert!(matches!(
        snapshot
            .runtimes
            .iter()
            .find(|r| r.runtime_id == "c")
            .unwrap()
            .status,
        RuntimeStatus::Initializing
    ));
}

#[test]
fn selected_scopes_survive_inbox_rerun_retry_and_independent_retry_completion() {
    let mut inbox = ProviderWarmupInbox::default();
    for id in ["a", "b", "a"] {
        inbox.push(ProviderWarmupCommand::Workspace {
            workspace_id: "ws".into(),
            scope: selected(&[id]),
            force_scope: Some(selected(&[id])),
        });
    }
    let commands = inbox.drain();
    let [
        ProviderWarmupCommand::Workspace {
            scope, force_scope, ..
        },
    ] = commands.as_slice()
    else {
        panic!("one coalesced command");
    };
    assert_eq!(*scope, selected(&["a", "b"]));
    assert_eq!(*force_scope, Some(scope.clone()));
    assert_eq!(
        scope.merge(ProviderWarmupScope::ApiOnly).cli,
        CliProbeScope::Instances(ids(&["a", "b"]))
    );
    let mut in_flight = HashMap::from([("ws".into(), ProviderWarmupScope::All)]);
    let mut rerun = HashMap::new();
    for id in ["a", "b"] {
        assert!(!register_workspace_probe(
            &mut in_flight,
            &mut rerun,
            "ws",
            selected(&[id]),
            true
        ));
    }
    assert_eq!(rerun["ws"], selected(&["a", "b"]));
    let mut retry = HashMap::from([("ws".into(), selected(&["c"]))]);
    complete_retry_scope(
        &mut retry,
        "ws",
        scope.clone(),
        scope.retry_scope(false, true),
    );
    assert_eq!(retry["ws"], selected(&["a", "b", "c"]));
    complete_retry_scope(&mut retry, "ws", selected(&["a"]), None);
    assert_eq!(retry["ws"], selected(&["b", "c"]));
    let mut all_retry = Some(selected(&["a"]));
    merge_optional_scope(&mut all_retry, selected(&["b"]));
    assert_eq!(all_retry, Some(selected(&["a", "b"])));
    assert_eq!(
        ProviderWarmupScope::CliOnly.subtract(selected(&["a"])),
        Some(ProviderWarmupScope::CliOnly)
    );
}

struct Harness {
    processor: Arc<MessageProcessor>,
    workspace: String,
    peer: String,
    context: crate::request_context::RequestContext,
    counts: Arc<StdMutex<HashMap<String, usize>>>,
    failures: Arc<StdMutex<BTreeSet<String>>>,
    _home: tempfile::TempDir,
    _wake: mpsc::Receiver<()>,
    _notifications: mpsc::Receiver<axum::extract::ws::Message>,
}

impl Harness {
    async fn new() -> Self {
        let (processor, sessions, connection, _, workspaces, workspace) =
            crate::message::tests::setup_workspace_message_processor().await;
        sessions.unregister_connection(connection).await;
        // Keep the complete notification sequence across repeated saves and
        // probes without evicting this fixture's settings connection.
        let (tx, notifications) = mpsc::channel(512);
        let connection = crate::session::test_support::register_authenticated_test_connection(
            sessions.as_ref(),
            tx,
        )
        .await;
        let peer = workspaces
            .create_workspace("toggle-peer", Some("Toggle peer"))
            .await
            .unwrap()
            .id;
        let home = tempfile::tempdir().unwrap();
        let processor = processor.with_runtime_home_for_tests(home.path().into());
        let config = crate::isolated_test_app_config().unwrap();
        let name = &config.gateway.settings_file_name;
        let path = home.path().join(name);
        let mut settings = crate::settings::load_or_create_gateway_settings(
            &path,
            config.gateway.settings_version,
            name,
        )
        .unwrap();
        let mut runtimes = GatewayCliRuntimeSettings::default();
        let mut second = runtimes.instances[0].clone();
        second.id = "codex_two".into();
        second.display_name = "Codex two".into();
        second.nickname = "codex_two".into();
        runtimes.instances.push(second);
        for instance in &mut runtimes.instances {
            instance.binary_path = home
                .path()
                .join(format!("fake-{}", instance.id))
                .to_string_lossy()
                .into_owned();
            instance.home_path = home
                .path()
                .join(&instance.id)
                .to_string_lossy()
                .into_owned();
        }
        settings
            .set_cli_runtime_settings_for_tests(runtimes)
            .unwrap();
        crate::settings::save_gateway_settings(&path, &settings).unwrap();
        sessions
            .set_connection_workspace(connection, Some(workspace.clone()))
            .await;
        let context = crate::message::tests::registered_request_context(
            &processor,
            connection,
            "settings/update",
        )
        .await;
        let counts = Arc::new(StdMutex::new(HashMap::<String, usize>::new()));
        let failures = Arc::new(StdMutex::new(BTreeSet::<String>::new()));
        *processor.provider_readiness.cli_probe_fake.lock().unwrap() = Some(Arc::new({
            let counts = counts.clone();
            let failures = failures.clone();
            move |instance: pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig| {
                *counts
                    .lock()
                    .unwrap()
                    .entry(instance.id.clone())
                    .or_default() += 1;
                let fail = failures.lock().unwrap().contains(&instance.id);
                let marker = instance.id.clone();
                let mut result = ready_result(
                    super::super::cli_runtime::cli_runtime_summary_from_instance(instance, None),
                    &marker,
                );
                if fail {
                    result.summary.status = RuntimeStatus::NeedsAuth;
                    result.summary.capabilities.supports_mcp_tools = false;
                    result.summary.models_refreshed_at_unix_ms = None;
                    result.models = None;
                    result.mcp_readiness = None;
                }
                result
            }
        }));
        let (tx, wake) = mpsc::channel(1);
        *processor.provider_readiness.command_tx.lock().unwrap() = Some(tx);
        Self {
            processor: Arc::new(processor),
            workspace,
            peer,
            context,
            counts,
            failures,
            _home: home,
            _wake: wake,
            _notifications: notifications,
        }
    }

    fn settings_path(&self) -> std::path::PathBuf {
        self._home.path().join(
            crate::isolated_test_app_config()
                .unwrap()
                .gateway
                .settings_file_name,
        )
    }

    fn runtimes(&self) -> GatewayCliRuntimeSettings {
        let config = crate::isolated_test_app_config().unwrap();
        crate::settings::load_or_create_gateway_settings(
            &self.settings_path(),
            config.gateway.settings_version,
            &config.gateway.settings_file_name,
        )
        .unwrap()
        .effective_cli_runtime_settings_for_workspace(&config.gateway, Some(&self.workspace))
    }

    async fn save(&self, runtimes: GatewayCliRuntimeSettings) {
        self.processor
            .update_gateway_settings(
                &self.context,
                GatewaySettingsUpdate {
                    cli_runtimes: Some(runtimes),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    async fn baseline(&self) {
        let mut scheduler = ProviderProbeScheduler::new(Arc::downgrade(&self.processor));
        scheduler
            .schedule_all(ProviderWarmupScope::CliOnly, false, None)
            .await
            .unwrap();
        assert!(
            drain(&mut scheduler)
                .await
                .iter()
                .all(|(_, _, retry)| retry.is_none())
        );
        self.counts.lock().unwrap().clear();
    }

    fn commands(&self) -> Vec<ProviderWarmupCommand> {
        self.processor
            .provider_readiness
            .pending_commands
            .lock()
            .unwrap()
            .drain()
    }

    async fn apply_commands(
        &self,
    ) -> Vec<(String, ProviderWarmupScope, Option<ProviderWarmupScope>)> {
        let mut scheduler = ProviderProbeScheduler::new(Arc::downgrade(&self.processor));
        for command in self.commands() {
            match command {
                ProviderWarmupCommand::AllWorkspaces { scope, force_scope } => {
                    scheduler
                        .schedule_all_request(scope, force_scope, None)
                        .await
                        .unwrap();
                }
                ProviderWarmupCommand::Workspace {
                    workspace_id,
                    scope,
                    force_scope,
                } => {
                    scheduler.schedule_workspace_request(workspace_id, scope, force_scope);
                }
            }
        }
        drain(&mut scheduler).await
    }
}

async fn drain(
    scheduler: &mut ProviderProbeScheduler,
) -> Vec<(String, ProviderWarmupScope, Option<ProviderWarmupScope>)> {
    let mut completed = Vec::new();
    while let Some(result) = scheduler.probes.join_next_with_id().await {
        let (task_id, (workspace, scope, outcome)) = result.unwrap();
        scheduler.task_workspaces.remove(&task_id);
        scheduler.in_flight.remove(&workspace);
        let Ok(Ok(retry)) = outcome else {
            panic!("fake warmup failed");
        };
        if let Some(rerun) = scheduler.rerun.remove(&workspace) {
            scheduler.schedule_workspace(workspace.clone(), rerun, false);
        }
        completed.push((workspace, scope, retry));
    }
    completed
}

#[tokio::test]
async fn saved_toggles_probe_only_the_changed_instance_across_workspaces_and_kinds() {
    let h = Harness::new().await;
    h.baseline().await;
    for target in ["codex", "claude", "opencode", "codex_two"] {
        let before = h
            .processor
            .provider_readiness
            .snapshot(&h.workspace)
            .await
            .unwrap();
        let peer_before = h
            .processor
            .provider_readiness
            .snapshot(&h.peer)
            .await
            .unwrap();
        let mut settings = h.runtimes();
        settings
            .instances
            .iter_mut()
            .find(|i| i.id == target)
            .unwrap()
            .enabled = false;
        h.save(settings).await;
        for (workspace, old) in [(&h.workspace, &before), (&h.peer, &peer_before)] {
            let now = h
                .processor
                .provider_readiness
                .snapshot(workspace)
                .await
                .unwrap();
            let disabled = now
                .runtimes
                .iter()
                .find(|r| r.runtime_id == target)
                .unwrap();
            assert!(!disabled.enabled);
            assert!(matches!(disabled.status, RuntimeStatus::Disabled));
            for other in old.runtimes.iter().filter(|r| r.runtime_id != target) {
                assert_preserved(old, &now, &other.runtime_id);
            }
        }
        assert!(
            h.apply_commands()
                .await
                .iter()
                .all(|(_, scope, retry)| *scope == selected(&[target]) && retry.is_none())
        );
        assert!(
            h.counts.lock().unwrap().is_empty(),
            "disabled runtime must not reach fake live probe"
        );
        let mut settings = h.runtimes();
        settings
            .instances
            .iter_mut()
            .find(|i| i.id == target)
            .unwrap()
            .enabled = true;
        h.save(settings).await;
        h.apply_commands().await;
        assert_eq!(
            *h.counts.lock().unwrap(),
            HashMap::from([(target.to_owned(), 2)])
        );
        let after = h
            .processor
            .provider_readiness
            .snapshot(&h.workspace)
            .await
            .unwrap();
        for other in before.runtimes.iter().filter(|r| r.runtime_id != target) {
            assert_preserved(&before, &after, &other.runtime_id);
        }
        h.counts.lock().unwrap().clear();
    }
}

#[tokio::test]
async fn identical_reordered_and_compaction_only_saves_do_not_invalidate_or_schedule() {
    let h = Harness::new().await;
    h.baseline().await;
    let before = h
        .processor
        .provider_readiness
        .snapshot(&h.workspace)
        .await
        .unwrap();
    let peer = h
        .processor
        .provider_readiness
        .snapshot(&h.peer)
        .await
        .unwrap();
    let generation = h
        .processor
        .provider_readiness
        .cli_generation(&h.workspace)
        .await;
    for mode in 0..3 {
        let mut settings = h.runtimes();
        if mode == 1 {
            settings.instances.reverse();
        }
        if mode == 2 {
            settings
                .instances
                .iter_mut()
                .find(|i| i.id == "codex")
                .unwrap()
                .compaction_model = Some(pioneer_protocol::GatewayModelSelection::Explicit {
                transport: pioneer_protocol::ModelSelectionTransport::Codex,
                instance: "codex".into(),
                model: "compaction-only".into(),
                reasoning_effort: None,
            });
        }
        h.save(settings).await;
        assert!(h.commands().is_empty());
        assert_eq!(
            h.processor
                .provider_readiness
                .snapshot(&h.workspace)
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            h.processor
                .provider_readiness
                .snapshot(&h.peer)
                .await
                .unwrap(),
            peer
        );
        assert_eq!(
            h.processor
                .provider_readiness
                .cli_generation(&h.workspace)
                .await,
            generation
        );
        assert!(h.counts.lock().unwrap().is_empty());
    }
    let config = crate::isolated_test_app_config().unwrap();
    let settings = crate::settings::load_or_create_gateway_settings(
        &h.settings_path(),
        config.gateway.settings_version,
        &config.gateway.settings_file_name,
    )
    .unwrap();
    let a =
        settings.effective_cli_runtime_settings_for_workspace(&config.gateway, Some(&h.workspace));
    let b = settings.effective_cli_runtime_settings_for_workspace(&config.gateway, Some(&h.peer));
    assert_ne!(
        a.instances
            .iter()
            .find(|i| i.id == "codex")
            .unwrap()
            .compaction_model,
        b.instances
            .iter()
            .find(|i| i.id == "codex")
            .unwrap()
            .compaction_model
    );
}

#[tokio::test]
async fn added_changed_and_removed_instances_do_not_reset_existing_siblings() {
    let h = Harness::new().await;
    h.baseline().await;
    let before = h
        .processor
        .provider_readiness
        .snapshot(&h.workspace)
        .await
        .unwrap();
    let mut settings = h.runtimes();
    let mut added = settings.instances[0].clone();
    added.id = "added".into();
    added.nickname = "added".into();
    added.display_name = "Added".into();
    added.compaction_model = None;
    settings.instances.push(added);
    h.save(settings).await;
    h.apply_commands().await;
    assert_eq!(
        *h.counts.lock().unwrap(),
        HashMap::from([("added".into(), 2)])
    );
    for old in &before.runtimes {
        assert_preserved(
            &before,
            &h.processor
                .provider_readiness
                .snapshot(&h.workspace)
                .await
                .unwrap(),
            &old.runtime_id,
        );
    }
    h.counts.lock().unwrap().clear();
    let mut settings = h.runtimes();
    settings
        .instances
        .iter_mut()
        .find(|i| i.id == "added")
        .unwrap()
        .binary_path
        .push_str("-changed");
    h.save(settings).await;
    h.apply_commands().await;
    assert_eq!(
        *h.counts.lock().unwrap(),
        HashMap::from([("added".into(), 2)])
    );
    h.counts.lock().unwrap().clear();
    let mut settings = h.runtimes();
    settings.instances.retain(|i| i.id != "added");
    h.save(settings).await;
    h.apply_commands().await;
    assert!(h.counts.lock().unwrap().is_empty());
    let after = h
        .processor
        .provider_readiness
        .snapshot(&h.workspace)
        .await
        .unwrap();
    assert_eq!(after.runtimes.len(), before.runtimes.len());
    assert!(!after.models.contains_key("added"));
    assert!(!after.mcp_readiness.contains_key("added"));
    for old in &before.runtimes {
        assert_preserved(&before, &after, &old.runtime_id);
    }
}

#[tokio::test]
async fn fake_scheduler_rerun_and_retry_keep_selected_ids_and_independent_retry() {
    let h = Harness::new().await;
    h.baseline().await;
    h.failures.lock().unwrap().insert("codex".into());
    let mut scheduler = ProviderProbeScheduler::new(Arc::downgrade(&h.processor));
    assert!(scheduler.schedule_workspace(h.workspace.clone(), selected(&["codex"]), false));
    assert!(!scheduler.schedule_workspace(h.workspace.clone(), selected(&["codex_two"]), true));
    let completed = drain(&mut scheduler).await;
    assert_eq!(completed.len(), 2);
    let mut retries = HashMap::from([(h.workspace.clone(), selected(&["claude"]))]);
    for (workspace, requested, retry) in completed {
        complete_retry_scope(&mut retries, &workspace, requested, retry);
    }
    assert_eq!(retries[&h.workspace], selected(&["codex", "claude"]));
    h.counts.lock().unwrap().clear();
    for (workspace, scope) in &retries {
        scheduler.schedule_workspace(workspace.clone(), scope.clone(), false);
    }
    let completed = drain(&mut scheduler).await;
    assert_eq!(
        *h.counts.lock().unwrap(),
        HashMap::from([("codex".into(), 1), ("claude".into(), 1)])
    );
    assert_eq!(completed[0].2, Some(selected(&["codex"])));
    // The independent full reconciliation still runs every enabled instance.
    h.counts.lock().unwrap().clear();
    scheduler.schedule_workspace(h.workspace.clone(), ProviderWarmupScope::CliOnly, false);
    drain(&mut scheduler).await;
    assert_eq!(h.counts.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn first_targeted_scheduler_execution_keeps_a_complete_catalog() {
    let h = Harness::new().await;
    let mut settings = h.runtimes();
    settings
        .instances
        .iter_mut()
        .find(|i| i.id == "codex")
        .unwrap()
        .enabled = false;
    h.save(settings).await;
    assert!(
        h.processor
            .provider_readiness
            .snapshot(&h.workspace)
            .await
            .is_none()
    );
    h.apply_commands().await;
    let snapshot = h
        .processor
        .provider_readiness
        .snapshot(&h.workspace)
        .await
        .unwrap();
    assert_eq!(snapshot.runtimes.len(), 4);
    assert!(h.counts.lock().unwrap().is_empty());
    for runtime in &snapshot.runtimes {
        assert!(if runtime.runtime_id == "codex" {
            matches!(runtime.status, RuntimeStatus::Disabled)
        } else {
            matches!(runtime.status, RuntimeStatus::Initializing)
        });
    }
}

#[tokio::test]
async fn save_failure_leaves_snapshots_epochs_and_commands_untouched() {
    let h = Harness::new().await;
    h.baseline().await;
    let before = h
        .processor
        .provider_readiness
        .snapshot(&h.workspace)
        .await
        .unwrap();
    let peer = h
        .processor
        .provider_readiness
        .snapshot(&h.peer)
        .await
        .unwrap();
    let generation = h
        .processor
        .provider_readiness
        .cli_generation(&h.workspace)
        .await;
    let contents = std::fs::read(h.settings_path()).unwrap();
    let mut settings = h.runtimes();
    settings.instances[0].enabled = false;
    let result = h
        .processor
        .update_gateway_settings_with_saver(
            &h.context,
            GatewaySettingsUpdate {
                cli_runtimes: Some(settings),
                ..Default::default()
            },
            |_, _| anyhow::bail!("injected settings save failure"),
        )
        .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("injected settings save failure")
    );
    assert_eq!(std::fs::read(h.settings_path()).unwrap(), contents);
    assert_eq!(
        h.processor
            .provider_readiness
            .snapshot(&h.workspace)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        h.processor
            .provider_readiness
            .snapshot(&h.peer)
            .await
            .unwrap(),
        peer
    );
    assert_eq!(
        h.processor
            .provider_readiness
            .cli_generation(&h.workspace)
            .await,
        generation
    );
    assert!(h.commands().is_empty());
    assert!(h.counts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn projection_failure_rolls_back_file_before_any_readiness_publication() {
    use sea_orm::ConnectionTrait;
    let h = Harness::new().await;
    h.baseline().await;
    let before = h
        .processor
        .provider_readiness
        .snapshot(&h.workspace)
        .await
        .unwrap();
    let contents = std::fs::read(h.settings_path()).unwrap();
    h.processor.crud_store.database_connection().execute_unprepared(
        "CREATE TRIGGER reject_toggle_projection BEFORE INSERT ON agent_identity BEGIN SELECT RAISE(ABORT, 'injected CLI projection failure'); END;"
    ).await.unwrap();
    let mut settings = h.runtimes();
    settings.instances[0].enabled = false;
    assert!(
        h.processor
            .update_gateway_settings(
                &h.context,
                GatewaySettingsUpdate {
                    cli_runtimes: Some(settings),
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(h.settings_path()).unwrap(), contents);
    assert_eq!(
        h.processor
            .provider_readiness
            .snapshot(&h.workspace)
            .await
            .unwrap(),
        before
    );
    assert!(h.commands().is_empty());
    assert!(h.counts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn retired_workspace_epoch_is_not_reused_by_a_new_seed() {
    let supervisor = seeded_supervisor().await;
    let old = supervisor.begin_cli_probe("ws", &selected(&["a"])).await;
    supervisor.retain_workspaces(&HashSet::new()).await;
    let new = supervisor.cli_generation("ws").await;
    assert!(new > old);
    supervisor
        .insert_snapshot_if_absent_if_generation(
            "ws",
            new,
            vec![runtime_summary("a", RuntimeStatus::Initializing)],
        )
        .await
        .unwrap();
    assert!(
        publish_result(
            &supervisor,
            "ws",
            old,
            &selected(&["a"]),
            vec![ready_result(
                runtime_summary("a", RuntimeStatus::Ready),
                "retired"
            )]
        )
        .await
        .is_none()
    );
    assert!(supervisor.snapshot("ws").await.unwrap().models.is_empty());
}

#[tokio::test]
async fn successive_saves_coalesce_ids_and_preserve_known_catalogs_until_regular_cleanup() {
    let h = Harness::new().await;
    h.baseline().await;
    let before = h
        .processor
        .provider_readiness
        .snapshot(&h.workspace)
        .await
        .unwrap();
    h.processor.provider_readiness.cli_generation("known").await;
    h.processor
        .provider_readiness
        .replace_snapshot_with_probe_artifacts(
            "known",
            before.runtimes.clone(),
            before.models.clone(),
            before.mcp_readiness.clone(),
        )
        .await;
    let known = h
        .processor
        .provider_readiness
        .snapshot("known")
        .await
        .unwrap();
    for target in ["codex", "claude"] {
        let mut settings = h.runtimes();
        settings
            .instances
            .iter_mut()
            .find(|i| i.id == target)
            .unwrap()
            .binary_path
            .push_str("-changed");
        h.save(settings).await;
    }
    let commands = h.commands();
    let [ProviderWarmupCommand::AllWorkspaces { scope, force_scope }] = commands.as_slice() else {
        panic!("one global command");
    };
    assert_eq!(*scope, selected(&["codex", "claude"]));
    assert_eq!(*force_scope, Some(scope.clone()));
    let mut scheduler = ProviderProbeScheduler::new(Arc::downgrade(&h.processor));
    scheduler
        .schedule_all_request(scope.clone(), force_scope.clone(), None)
        .await
        .unwrap();
    drain(&mut scheduler).await;
    assert_eq!(
        *h.counts.lock().unwrap(),
        HashMap::from([("codex".into(), 2), ("claude".into(), 2)])
    );
    for workspace in [h.workspace.as_str(), h.peer.as_str(), "known"] {
        let snapshot = h
            .processor
            .provider_readiness
            .snapshot(workspace)
            .await
            .unwrap();
        for sibling in ["opencode", "codex_two"] {
            assert_preserved(&before, &snapshot, sibling);
        }
    }
    let known_after = h
        .processor
        .provider_readiness
        .snapshot("known")
        .await
        .unwrap();
    for sibling in ["opencode", "codex_two"] {
        assert_preserved(&known, &known_after, sibling);
    }
    scheduler
        .schedule_all(ProviderWarmupScope::CliOnly, false, None)
        .await
        .unwrap();
    drain(&mut scheduler).await;
    assert!(
        h.processor
            .provider_readiness
            .snapshot("known")
            .await
            .is_none()
    );
    scheduler.shutdown().await;
    assert!(scheduler.in_flight.is_empty());
    assert!(scheduler.rerun.is_empty());
    assert!(scheduler.task_workspaces.is_empty());
}

#[tokio::test]
async fn settings_application_lock_keeps_seed_and_probe_from_reading_staged_configuration() {
    let h = Harness::new().await;
    let path = h.settings_path();
    let original = std::fs::read(&path).unwrap();
    let guard = h.processor.gateway_settings_update_lock.lock().await;
    let config = crate::isolated_test_app_config().unwrap();
    let mut staged = crate::settings::load_or_create_gateway_settings(
        &path,
        config.gateway.settings_version,
        &config.gateway.settings_file_name,
    )
    .unwrap();
    let mut runtimes = h.runtimes();
    runtimes
        .instances
        .iter_mut()
        .find(|i| i.id == "codex")
        .unwrap()
        .enabled = false;
    staged
        .apply_protocol_update_for_workspace(
            GatewaySettingsUpdate {
                cli_runtimes: Some(runtimes),
                ..Default::default()
            },
            Some(&h.workspace),
        )
        .unwrap();
    crate::settings::save_gateway_settings(&path, &staged).unwrap();
    let (seed_started, seed_wait) = tokio::sync::oneshot::channel();
    let seed = tokio::spawn({
        let processor = h.processor.clone();
        let workspace = h.workspace.clone();
        async move {
            seed_started.send(()).unwrap();
            processor
                .cli_runtime_readiness_snapshot_or_seed(&workspace)
                .await
                .unwrap()
        }
    });
    let (probe_started, probe_wait) = tokio::sync::oneshot::channel();
    let probe = tokio::spawn({
        let processor = h.processor.clone();
        let workspace = h.workspace.clone();
        async move {
            probe_started.send(()).unwrap();
            let trace = GatewayProviderWarmupTrace::start(GatewayProviderWarmupScope::Cli);
            processor
                .perform_cli_runtime_warmup(
                    &workspace,
                    &trace,
                    &Arc::new(Semaphore::new(1)),
                    &selected(&["codex"]),
                )
                .await
                .unwrap()
        }
    });
    seed_wait.await.unwrap();
    probe_wait.await.unwrap();
    assert!(!seed.is_finished());
    assert!(!probe.is_finished());
    assert!(
        h.processor
            .provider_readiness
            .snapshot(&h.workspace)
            .await
            .is_none()
    );
    assert!(h.counts.lock().unwrap().is_empty());
    // Simulate rollback before releasing the same application lock.
    std::fs::write(path, original).unwrap();
    drop(guard);
    let seeded = seed.await.unwrap();
    assert!(
        seeded
            .runtimes
            .iter()
            .find(|r| r.runtime_id == "codex")
            .unwrap()
            .enabled
    );
    assert_eq!(probe.await.unwrap(), CliRuntimeWarmupOutcome::Ready);
    assert_eq!(
        *h.counts.lock().unwrap(),
        HashMap::from([("codex".into(), 1)])
    );
}

#[tokio::test]
async fn queued_probe_does_not_start_after_its_instance_is_disabled() {
    let h = Harness::new().await;
    h.baseline().await;
    let concurrency = Arc::new(Semaphore::new(0));
    let old_generation = h
        .processor
        .provider_readiness
        .cli_generation(&h.workspace)
        .await;
    let probe = tokio::spawn({
        let processor = h.processor.clone();
        let workspace = h.workspace.clone();
        let concurrency = concurrency.clone();
        async move {
            let trace = GatewayProviderWarmupTrace::start(GatewayProviderWarmupScope::Cli);
            processor
                .perform_cli_runtime_warmup(&workspace, &trace, &concurrency, &selected(&["codex"]))
                .await
                .unwrap()
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while h
            .processor
            .provider_readiness
            .cli_generation(&h.workspace)
            .await
            == old_generation
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut settings = h.runtimes();
    settings
        .instances
        .iter_mut()
        .find(|i| i.id == "codex")
        .unwrap()
        .enabled = false;
    h.save(settings).await;
    concurrency.add_permits(1);
    probe.await.unwrap();
    h.apply_commands().await;
    assert!(h.counts.lock().unwrap().is_empty());
    assert!(matches!(
        h.processor
            .provider_readiness
            .snapshot(&h.workspace)
            .await
            .unwrap()
            .runtimes
            .iter()
            .find(|r| r.runtime_id == "codex")
            .unwrap()
            .status,
        RuntimeStatus::Disabled
    ));
}

#[test]
fn merged_full_request_does_not_promote_a_selected_force_to_a_full_rerun() {
    for global in [false, true] {
        let mut inbox = ProviderWarmupInbox::default();
        for (scope, force_scope) in [
            (ProviderWarmupScope::All, None),
            (selected(&["a"]), Some(selected(&["a"]))),
        ] {
            inbox.push(if global {
                ProviderWarmupCommand::AllWorkspaces { scope, force_scope }
            } else {
                ProviderWarmupCommand::Workspace {
                    workspace_id: "ws".into(),
                    scope,
                    force_scope,
                }
            });
        }
        let mut in_flight = HashMap::from([("ws".into(), ProviderWarmupScope::All)]);
        let mut rerun = HashMap::new();
        for command in inbox.drain() {
            let (scope, force_scope) = match command {
                ProviderWarmupCommand::Workspace {
                    scope, force_scope, ..
                }
                | ProviderWarmupCommand::AllWorkspaces { scope, force_scope } => {
                    (scope, force_scope)
                }
            };
            assert!(!register_workspace_probe_request(
                &mut in_flight,
                &mut rerun,
                "ws",
                scope,
                force_scope
            ));
        }
        assert_eq!(rerun["ws"], selected(&["a"]));
    }
}

#[tokio::test]
async fn merged_command_behind_full_probe_repeats_only_the_forced_id_on_fake() {
    let h = Harness::new().await;
    h.baseline().await;
    let mut scheduler = ProviderProbeScheduler::new(Arc::downgrade(&h.processor));
    scheduler.schedule_workspace(h.workspace.clone(), ProviderWarmupScope::CliOnly, false);
    let mut inbox = ProviderWarmupInbox::default();
    inbox.push(ProviderWarmupCommand::Workspace {
        workspace_id: h.workspace.clone(),
        scope: ProviderWarmupScope::CliOnly,
        force_scope: None,
    });
    inbox.push(ProviderWarmupCommand::Workspace {
        workspace_id: h.workspace.clone(),
        scope: selected(&["codex"]),
        force_scope: Some(selected(&["codex"])),
    });
    for command in inbox.drain() {
        let ProviderWarmupCommand::Workspace {
            workspace_id,
            scope,
            force_scope,
        } = command
        else {
            unreachable!();
        };
        scheduler.schedule_workspace_request(workspace_id, scope, force_scope);
    }
    let completed = drain(&mut scheduler).await;
    assert_eq!(completed.len(), 2);
    assert_eq!(completed[1].1, selected(&["codex"]));
    assert_eq!(
        *h.counts.lock().unwrap(),
        HashMap::from([
            ("codex".into(), 2),
            ("codex_two".into(), 1),
            ("claude".into(), 1),
            ("opencode".into(), 1)
        ])
    );
}

async fn deferred_forced_global_request_survives_listing_failure(ordinary_full_before_retry: bool) {
    let h = Harness::new().await;
    h.baseline().await;
    let supervisor = &h.processor.provider_readiness;
    let before = supervisor.snapshot(&h.workspace).await.unwrap();
    let prior = [
        supervisor.cli_generation(&h.workspace).await,
        supervisor.cli_generation(&h.peer).await,
    ];
    let mut scheduler = ProviderProbeScheduler::new(Arc::downgrade(&h.processor));
    scheduler.provider_concurrency = Arc::new(Semaphore::new(0));
    scheduler
        .schedule_all(ProviderWarmupScope::CliOnly, false, None)
        .await
        .unwrap();
    // Both old full probes capture configuration/epochs, then wait for a permit.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if supervisor.cli_generation(&h.workspace).await > prior[0]
                && supervisor.cli_generation(&h.peer).await > prior[1]
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let old = [
        supervisor.cli_generation(&h.workspace).await,
        supervisor.cli_generation(&h.peer).await,
    ];
    let mut settings = h.runtimes();
    settings
        .instances
        .iter_mut()
        .find(|instance| instance.id == "codex")
        .unwrap()
        .binary_path
        .push_str("-new");
    h.save(settings).await;
    let commands = h.commands();
    let [ProviderWarmupCommand::AllWorkspaces { scope, force_scope }] = commands.as_slice() else {
        panic!("one global mutation request");
    };
    let mut pending = None;
    let mut backoff = ProviderRetrySchedule::default();
    let now = tokio::time::Instant::now();
    scheduler.workspace_list_failures = 1;
    assert!(
        scheduler
            .schedule_all_or_defer(
                &mut pending,
                &mut backoff,
                PendingProviderWarmup {
                    scope: scope.clone(),
                    force_scope: force_scope.clone()
                },
                now,
                false
            )
            .await
            .is_none()
    );
    let deadline = backoff.next_attempt.unwrap();
    assert_eq!(deadline, now + PROVIDER_READINESS_RETRY_INTERVAL);
    assert_eq!(
        pending.as_ref().unwrap().force_scope,
        Some(selected(&["codex"]))
    );
    assert!(scheduler.rerun.is_empty());
    if ordinary_full_before_retry {
        assert!(
            scheduler
                .schedule_all_or_defer(
                    &mut pending,
                    &mut backoff,
                    PendingProviderWarmup {
                        scope: ProviderWarmupScope::CliOnly,
                        force_scope: None
                    },
                    now + Duration::from_secs(1),
                    false
                )
                .await
                .is_some()
        );
        assert_eq!(pending.as_ref().unwrap().scope, selected(&["codex"]));
        assert_eq!(
            pending.as_ref().unwrap().force_scope,
            Some(selected(&["codex"]))
        );
        assert_eq!(backoff.next_attempt, Some(deadline));
        assert_eq!(backoff.attempts, 1);
        assert!(
            scheduler.rerun.is_empty(),
            "ordinary full request is absorbed by old full probes"
        );
    }
    let deadline = if !ordinary_full_before_retry {
        // A failed retry tick must preserve force as well as command failure.
        scheduler.workspace_list_failures = 1;
        let request = pending.clone().unwrap();
        assert!(
            scheduler
                .schedule_all_or_defer(&mut pending, &mut backoff, request, deadline, true)
                .await
                .is_none()
        );
        assert_eq!(
            pending.as_ref().unwrap().force_scope,
            Some(selected(&["codex"]))
        );
        assert_eq!(
            backoff.next_attempt,
            Some(deadline + PROVIDER_READINESS_RETRY_INTERVAL * 2)
        );
        backoff.next_attempt.unwrap()
    } else {
        deadline
    };
    let request = pending.clone().unwrap();
    assert!(
        scheduler
            .schedule_all_or_defer(&mut pending, &mut backoff, request, deadline, true)
            .await
            .is_some()
    );
    assert!(pending.is_none());
    for (index, workspace) in [h.workspace.as_str(), h.peer.as_str()]
        .into_iter()
        .enumerate()
    {
        assert_eq!(scheduler.rerun[workspace], selected(&["codex"]));
        // The old epoch cannot publish A even though its full probe still owns
        // the singleflight slot. The helper above, not the test, queues rerun A.
        let a = before
            .runtimes
            .iter()
            .find(|runtime| runtime.runtime_id == "codex")
            .unwrap()
            .clone();
        let (snapshot, changes, accepted) = publish_result(
            supervisor,
            workspace,
            old[index],
            &selected(&["codex"]),
            vec![ready_result(a, "stale")],
        )
        .await
        .unwrap();
        assert!(changes.is_empty());
        assert!(accepted.is_empty());
        assert!(matches!(
            snapshot
                .runtimes
                .iter()
                .find(|runtime| runtime.runtime_id == "codex")
                .unwrap()
                .status,
            RuntimeStatus::Initializing
        ));
        for sibling in ["claude", "opencode", "codex_two"] {
            assert_preserved(&before, &snapshot, sibling);
        }
    }
    scheduler
        .provider_concurrency
        .add_permits(MAX_CONCURRENT_PROVIDER_PROBES);
    let completed = drain(&mut scheduler).await;
    assert_eq!(
        completed
            .iter()
            .filter(|(_, scope, _)| *scope == selected(&["codex"]))
            .count(),
        2
    );
    assert_eq!(
        completed
            .iter()
            .filter(|(_, scope, _)| *scope == ProviderWarmupScope::CliOnly)
            .count(),
        2
    );
    assert_eq!(
        *h.counts.lock().unwrap(),
        HashMap::from([
            ("codex".into(), 2),
            ("claude".into(), 2),
            ("opencode".into(), 2),
            ("codex_two".into(), 2)
        ])
    );
    for workspace in [&h.workspace, &h.peer] {
        let snapshot = supervisor.snapshot(workspace).await.unwrap();
        assert!(matches!(
            snapshot
                .runtimes
                .iter()
                .find(|runtime| runtime.runtime_id == "codex")
                .unwrap()
                .status,
            RuntimeStatus::Ready
        ));
        for sibling in ["claude", "opencode", "codex_two"] {
            assert_preserved(&before, &snapshot, sibling);
        }
    }
    assert!(scheduler.rerun.is_empty());
}

#[tokio::test]
async fn deferred_global_force_reaches_rerun_after_workspace_listing_recovers() {
    deferred_forced_global_request_survives_listing_failure(false).await;
}

#[tokio::test]
async fn ordinary_full_request_cannot_discharge_deferred_global_force() {
    deferred_forced_global_request_survives_listing_failure(true).await;
}

#[test]
fn deferred_global_requests_merge_force_and_keep_bounded_retry_deadline() {
    let mut pending = PendingProviderWarmup {
        scope: selected(&["a"]),
        force_scope: Some(selected(&["a"])),
    };
    pending.merge(selected(&["b"]), Some(selected(&["b"])));
    pending.merge(ProviderWarmupScope::CliOnly, None);
    let remaining = pending
        .subtract_scheduled(&PendingProviderWarmup {
            scope: ProviderWarmupScope::CliOnly,
            force_scope: Some(selected(&["a"])),
        })
        .unwrap();
    assert_eq!(remaining.scope, selected(&["b"]));
    assert_eq!(remaining.force_scope, Some(selected(&["b"])));
    let now = tokio::time::Instant::now();
    let mut backoff = ProviderRetrySchedule::default();
    backoff.ensure_scheduled(now);
    let deadline = backoff.next_attempt;
    backoff.ensure_scheduled(now + Duration::from_secs(1));
    assert_eq!(backoff.next_attempt, deadline);
    for _ in 0..10 {
        backoff.record_attempt(now);
    }
    assert_eq!(
        backoff.next_attempt,
        Some(now + PROVIDER_READINESS_RETRY_MAX_DELAY)
    );
}

#[derive(Clone, Copy)]
enum FailedCliSave {
    AfterWrite,
    RollbackBeforeWrite,
    UnknownFile,
    RollbackAfterRestoringFile,
}

async fn assert_cli_save_failure_outcome(mode: FailedCliSave) {
    use sea_orm::ConnectionTrait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let h = Harness::new().await;
    h.baseline().await;
    let supervisor = &h.processor.provider_readiness;
    let before = supervisor.snapshot(&h.workspace).await.unwrap();
    let peer_before = supervisor.snapshot(&h.peer).await.unwrap();
    let original_file = std::fs::read(h.settings_path()).unwrap();
    let prior_generation = supervisor.cli_generation(&h.workspace).await;
    let peer_generation = supervisor.cli_generation(&h.peer).await;
    let concurrency = Arc::new(Semaphore::new(0));
    let queued = tokio::spawn({
        let processor = h.processor.clone();
        let workspace = h.workspace.clone();
        let concurrency = concurrency.clone();
        async move {
            let trace = GatewayProviderWarmupTrace::start(GatewayProviderWarmupScope::Cli);
            processor
                .perform_cli_runtime_warmup(&workspace, &trace, &concurrency, &selected(&["codex"]))
                .await
                .unwrap()
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while supervisor.cli_generation(&h.workspace).await == prior_generation {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let queued_generation = supervisor.cli_generation(&h.workspace).await;
    let rollback = matches!(
        mode,
        FailedCliSave::RollbackBeforeWrite | FailedCliSave::RollbackAfterRestoringFile
    );
    if rollback {
        h.processor.crud_store.database_connection().execute_unprepared(
            "CREATE TRIGGER reject_failed_toggle_projection BEFORE INSERT ON agent_identity BEGIN SELECT RAISE(ABORT, 'injected CLI projection failure'); END;"
        ).await.unwrap();
    }
    let mut settings = h.runtimes();
    settings
        .instances
        .iter_mut()
        .find(|instance| instance.id == "codex")
        .unwrap()
        .enabled = false;
    let calls = AtomicUsize::new(0);
    let result = h
        .processor
        .update_gateway_settings_with_saver(
            &h.context,
            GatewaySettingsUpdate {
                cli_runtimes: Some(settings.clone()),
                ..Default::default()
            },
            |path, desired| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                assert!(
                    call < if rollback { 2 } else { 1 },
                    "a failed mutation must not be retried automatically"
                );
                if call == 1 && matches!(mode, FailedCliSave::RollbackBeforeWrite) {
                    anyhow::bail!("injected rollback before write failure");
                }
                if matches!(mode, FailedCliSave::UnknownFile) {
                    std::fs::write(path, "[cli_runtimes\n")?;
                    anyhow::bail!("injected uncertain file failure");
                }
                crate::settings::save_gateway_settings(path, desired)?;
                if call == 1 {
                    anyhow::bail!("injected rollback after rename failure");
                }
                if matches!(mode, FailedCliSave::AfterWrite) {
                    anyhow::bail!("injected save after rename failure");
                }
                Ok(())
            },
        )
        .await;
    let error = format!("{:#}", result.unwrap_err());
    assert_eq!(calls.load(Ordering::SeqCst), if rollback { 2 } else { 1 });
    match mode {
        FailedCliSave::AfterWrite => assert!(error.contains("injected save after rename failure")),
        FailedCliSave::RollbackBeforeWrite => {
            assert!(error.contains("injected CLI projection failure"));
            assert!(error.contains("injected rollback before write failure"));
        }
        FailedCliSave::RollbackAfterRestoringFile => {
            assert!(error.contains("injected CLI projection failure"));
            assert!(error.contains("injected rollback after rename failure"));
        }
        FailedCliSave::UnknownFile => {
            assert!(error.contains("injected uncertain file failure"));
            assert!(error.contains("failed to confirm persisted CLI configuration"));
        }
    }
    let restored = matches!(mode, FailedCliSave::RollbackAfterRestoringFile);
    if restored {
        assert_eq!(std::fs::read(h.settings_path()).unwrap(), original_file);
    } else if matches!(mode, FailedCliSave::UnknownFile) {
        assert_eq!(
            std::fs::read_to_string(h.settings_path()).unwrap(),
            "[cli_runtimes\n",
            "readback must not rewrite the file"
        );
    } else {
        assert_eq!(h.runtimes(), settings);
    }
    let after = supervisor.snapshot(&h.workspace).await.unwrap();
    let peer_after = supervisor.snapshot(&h.peer).await.unwrap();
    if restored {
        assert_eq!(
            supervisor.cli_generation(&h.workspace).await,
            queued_generation
        );
        assert_eq!(supervisor.cli_generation(&h.peer).await, peer_generation);
        assert_eq!(after, before);
        assert_eq!(peer_after, peer_before);
        assert!(
            supervisor
                .accepts_cli_probe(&h.workspace, queued_generation, "codex")
                .await
        );
    } else {
        assert!(supervisor.cli_generation(&h.workspace).await > queued_generation);
        assert!(supervisor.cli_generation(&h.peer).await > peer_generation);
        assert!(
            !supervisor
                .accepts_cli_probe(&h.workspace, queued_generation, "codex")
                .await
        );
        assert!(
            supervisor
                .accepts_cli_probe(&h.workspace, queued_generation, "claude")
                .await
        );
        for (previous, current) in [(&before, &after), (&peer_before, &peer_after)] {
            assert_eq!(current.revision, previous.revision + 1);
            for sibling in ["claude", "opencode", "codex_two"] {
                assert_preserved(previous, current, sibling);
            }
            let a = current
                .runtimes
                .iter()
                .find(|runtime| runtime.runtime_id == "codex")
                .unwrap();
            if matches!(mode, FailedCliSave::UnknownFile) {
                assert!(matches!(a.status, RuntimeStatus::Error { .. }));
            } else {
                assert!(!a.enabled);
                assert!(matches!(a.status, RuntimeStatus::Disabled));
            }
            assert!(a.account.is_none());
            assert!(a.version.is_none());
            assert!(a.models_refreshed_at_unix_ms.is_none());
            assert!(!a.capabilities.supports_mcp_tools);
            assert!(!current.models.contains_key("codex"));
            assert!(!current.mcp_readiness.contains_key("codex"));
        }
        for (workspace, epoch, previous, current) in [
            (h.workspace.as_str(), queued_generation, &before, &after),
            (h.peer.as_str(), peer_generation, &peer_before, &peer_after),
        ] {
            let (late, changes, accepted) = publish_result(
                supervisor,
                workspace,
                epoch,
                &selected(&["codex"]),
                vec![CliRuntimeProbeResult {
                    summary: previous
                        .runtimes
                        .iter()
                        .find(|runtime| runtime.runtime_id == "codex")
                        .unwrap()
                        .clone(),
                    models: previous.models.get("codex").cloned(),
                    mcp_readiness: previous.mcp_readiness.get("codex").cloned(),
                }],
            )
            .await
            .unwrap();
            assert!(changes.is_empty());
            assert!(accepted.is_empty());
            assert_eq!(&late, current);
        }
    }
    assert!(h.commands().is_empty());
    concurrency.add_permits(1);
    queued.await.unwrap();
    assert_eq!(supervisor.snapshot(&h.workspace).await.unwrap(), after);
    if restored {
        assert_eq!(
            *h.counts.lock().unwrap(),
            HashMap::from([("codex".into(), 1)])
        );
    } else {
        assert!(
            h.counts.lock().unwrap().is_empty(),
            "old queued A must not reach its live probe"
        );
    }
    assert!(h.commands().is_empty());
}

#[tokio::test]
async fn save_error_after_rename_fences_actual_changed_cli_configuration() {
    assert_cli_save_failure_outcome(FailedCliSave::AfterWrite).await;
}

#[tokio::test]
async fn failed_projection_and_failed_rollback_fence_actual_changed_cli_configuration() {
    assert_cli_save_failure_outcome(FailedCliSave::RollbackBeforeWrite).await;
}

#[tokio::test]
async fn unreadable_save_outcome_fences_only_attempted_ids_without_rewriting_file() {
    assert_cli_save_failure_outcome(FailedCliSave::UnknownFile).await;
}

#[tokio::test]
async fn rollback_error_after_restoring_file_preserves_confirmed_readiness() {
    assert_cli_save_failure_outcome(FailedCliSave::RollbackAfterRestoringFile).await;
}
