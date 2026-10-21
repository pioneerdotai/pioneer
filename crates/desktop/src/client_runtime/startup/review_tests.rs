//! Review regressions use the existing shell/runtime fixture and native GPUI draw.
use super::*;
use gpui_kit::{AppContext, TestAppContext};
use pioneer_client::{
    composer::{
        state_machine::{ComposerDomainAction, ComposerDomainState},
        store::ComposerIntent,
    },
    core::{ClientMutationAuthority, ClientRevisions, ClientScope, ScopedRevision},
    gateway::{
        onboarding_effects::OnboardingEnvironment,
        session_controller::{GatewaySessionPublication, StartupStageState},
        timings::{GatewayTimings, GatewayWsTimings},
        types::{GatewayEndpoint, GatewayEndpointKind, GatewayRegistry},
    },
    navigation::NavigationIntent,
    state::{
        client_state::{GatewayConnectionState, GatewayStatusLevel},
        reducers::{GatewayStatusProjection, GatewayStatusTextUpdate},
    },
};
use pioneer_protocol::*;
use serde_json::json;

fn prepare(core: &ClientCore, with_thread: bool) {
    core.install_onboarding_environment_for_test(OnboardingEnvironment {
        registry: GatewayRegistry {
            version: pioneer_client::gateway::registry::CURRENT_GATEWAY_REGISTRY_VERSION,
            installation_id: Some("synthetic".into()),
            active_gateway_id: Some("gateway".into()),
            local: None,
            remotes: vec![GatewayEndpoint {
                id: "gateway".into(),
                name: "Synthetic".into(),
                gateway_base_url:
                    pioneer_client::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                        "https://gateway.invalid",
                    )
                    .unwrap(),
                kind: GatewayEndpointKind::Remote,
                session_ref: Some("synthetic-session".into()),
                server_gateway_id: Some("G00000000000000000001".try_into().unwrap()),
                workspace_id: Some("workspace".into()),
                service_name: None,
            }],
        },
        binding_journals: vec![],
        discard_unbound_remote_candidates: false,
        default_remote_name: "Remote".into(),
        remote_connect_timeout_min: std::time::Duration::ZERO,
        installation: ClientInstallationDescriptor {
            installation_id: "synthetic".into(),
            display_name: "Synthetic".into(),
            client_kind: ClientKind::Desktop,
            platform: None,
            client_version: None,
        },
        timings: GatewayTimings::from_millis(10, 10, 10).unwrap(),
        ws_timings: GatewayWsTimings::from_millis(10, 10, 10, 10, 20, 0).unwrap(),
        local_provisioned: false,
        local_install_required: false,
        local_update_required: false,
    });
    let authority = ClientMutationAuthority::for_test();
    authority.accept_identity_for_test(core, serde_json::from_value(json!({
        "gateway":{"id":"G00000000000000000001"},
        "principal":{"id":"P00000000000000000001","kind":"user","display_name":"Alice","nickname":"alice"},
        "device":{"id":"D00000000000000000001","installation_id":"synthetic","display_name":"Test","client_kind":"desktop","status":"active"},
        "session":{"id":"S00000000000000000001","device_id":"D00000000000000000001","token_family_id":"F00000000000000000001","status":"active","refresh_generation":1,"refresh_expires_at_unix":1000}
    })).unwrap()).unwrap();
    let resources = AuthorizationOperationalResourceProjection {
        fingerprint: "resources".into(),
        providers: AuthorizationResourceSelector {
            all: true,
            ids: vec![],
        },
        provider_models_all: true,
        cli_runtimes: AuthorizationResourceSelector {
            all: true,
            ids: vec![],
        },
        cli_models_all: true,
        ..Default::default()
    };
    let capabilities = AuthorizationCapabilitySnapshot {
        schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
        authorization_revision: 1,
        principal_id: "P00000000000000000001".try_into().unwrap(),
        role_key: "member".into(),
        role: AuthorizationRolePresentation {
            key: "member".into(),
            display_name: "Member".into(),
            description: String::new(),
            built_in: true,
        },
        global: Default::default(),
        workspace: Some(AuthorizationWorkspaceCapabilitySnapshot {
            workspace_id: "workspace".into(),
            capabilities: AuthorizationWorkspaceCapabilities {
                can_read: true,
                can_create_thread: true,
                can_use_providers: true,
                can_use_cli_runtimes: true,
                ..Default::default()
            },
            operational_resources: resources.clone(),
            execution_draft_policy: AuthorizationExecutionDraftPolicyProjection {
                fingerprint: "policy".into(),
                resources,
                permission_options: vec![],
                can_attach_artifacts: false,
                mcp_invocation_limits: Default::default(),
            },
        }),
        thread: Some(AuthorizationThreadCapabilitySnapshot {
            workspace_id: "workspace".into(),
            thread_id: "a".into(),
            capabilities: AuthorizationThreadCapabilities {
                can_read: true,
                can_write: true,
                can_start_turn: true,
                ..Default::default()
            },
        }),
    };
    let ticket = core.current_auth_ticket();
    assert_eq!(
        core.accept_authorization_projection(ticket.0, ticket.1, capabilities.clone()),
        pioneer_client::authorization::AuthorizationProjectionAcceptance::Accepted
    );
    if with_thread {
        core.upsert_thread(serde_json::from_value(json!({"workspace_id":"workspace","id":"a","preview":"","mode":"Message","model":"model","model_provider":"provider","created_at":1,"updated_at":1,"status":"Idle","origin_kind":"user","sidebar_visibility":"visible","turns":[]})).unwrap());
        authority.accept_thread_capabilities_for_test(core, capabilities);
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: ComposerDomainState {
                selected_mode: ThreadMode::Message,
                mode_manually_selected: true,
                ..Default::default()
            },
        });
    }
    core.navigate(
        NavigationIntent::SelectThread {
            workspace_id: Some("workspace".into()),
            thread_id: with_thread.then(|| "a".into()),
        },
        None,
    );
    authority.publish_workspace_tree_for_test(core, "workspace", false, None);
    let mut session = GatewaySessionPublication::default();
    session.startup.transport_ready = true;
    session.status = Some(GatewayStatusProjection {
        status: GatewayStatusTextUpdate::KeepExisting,
        status_level: GatewayStatusLevel::Neutral,
        connection_state: GatewayConnectionState::Connected,
        clear_gateway_error: false,
    });
    publish_session(core, session);
    assert!(!core.onboarding_loading());
    assert!(!core.onboarding_setup_required());
}

fn publish_session(core: &ClientCore, session: GatewaySessionPublication) {
    let authority = ClientMutationAuthority::for_test();
    let previous = core
        .snapshot(&ClientScope::Session)
        .map(|p| p.revisions())
        .unwrap_or_default();
    core.publish(
        &authority,
        ClientScope::Session,
        ClientRevisions::new(
            previous.domain(),
            previous.presentation(),
            previous.content(),
            ScopedRevision::new(previous.scoped().get() + 1),
        ),
        Arc::new(session),
        vec![],
    );
}

#[test]
fn failed_initial_tree_with_create_permission_is_degraded_and_errors_its_stage() {
    let core = ClientCore::new();
    prepare(&core, false);
    let authority = ClientMutationAuthority::for_test();
    assert!(
        core.authorization_snapshot(Some("workspace"), None)
            .unwrap()
            .workspace
            .unwrap()
            .capabilities
            .can_create_thread
    );
    assert!(core.navigation_snapshot().active_thread_id().is_none());
    authority.publish_workspace_tree_for_test(&core, "workspace", true, None);
    let mut telemetry = StartupTelemetry::new(DesktopStartupTrace::start());
    telemetry.observe(&core, None);
    assert_eq!(readiness(&core, None), None);
    assert!(
        telemetry
            .active
            .contains_key(&DesktopStartupStage::ThreadTreeLoad)
    );
    authority.publish_workspace_tree_for_test(
        &core,
        "workspace",
        false,
        Some("synthetic initial read failure".into()),
    );
    assert_eq!(
        readiness(&core, None),
        Some(DesktopStartupOutcome::Degraded)
    );
    telemetry.observe(&core, None);
    assert_eq!(
        telemetry
            .completed
            .get(&DesktopStartupStage::ThreadTreeLoad),
        Some(&StartupStageState::Failed)
    );
    assert!(
        !telemetry
            .active
            .contains_key(&DesktopStartupStage::ThreadTreeLoad)
    );
}

#[test]
fn tree_errors_are_scoped_and_background_refresh_is_not_success_or_required_failure() {
    let core = ClientCore::new();
    prepare(&core, true);
    let authority = ClientMutationAuthority::for_test();
    authority.publish_workspace_tree_for_test(&core, "other", false, Some("other scope".into()));
    assert_eq!(
        readiness(&core, Some(DesktopStartupOutcome::Ready)),
        Some(DesktopStartupOutcome::Ready)
    );
    // Keep the fixture's connected session when injecting an unscoped milestone.
    // The real session owner has not been initialized by this publication fixture.
    let mut session = (*core.gateway_session()).clone();
    session.startup.stages.insert(
        pioneer_client::gateway::session_controller::StartupStage::ThreadTree,
        StartupStageState::Failed,
    );
    publish_session(&core, session);
    assert_eq!(
        readiness(&core, Some(DesktopStartupOutcome::Ready)),
        Some(DesktopStartupOutcome::Ready)
    );
    authority.publish_workspace_tree_for_test(
        &core,
        "workspace",
        false,
        Some("background refresh".into()),
    );
    assert_eq!(
        readiness(&core, Some(DesktopStartupOutcome::Ready)),
        Some(DesktopStartupOutcome::Ready)
    );
    let mut telemetry = StartupTelemetry::new(DesktopStartupTrace::start());
    telemetry.observe(&core, Some(DesktopStartupOutcome::Ready));
    assert_eq!(
        telemetry
            .completed
            .get(&DesktopStartupStage::ThreadTreeLoad),
        Some(&StartupStageState::Cancelled)
    );
    core.navigate(
        NavigationIntent::SelectThread {
            workspace_id: Some("workspace".into()),
            thread_id: None,
        },
        None,
    );
    assert_eq!(
        readiness(&core, None),
        Some(DesktopStartupOutcome::Degraded)
    );
}

#[gpui_kit::test]
fn ready_native_composer_requires_application_draw_and_once_only_acknowledgement(
    cx: &mut TestAppContext,
) {
    use gpui_kit::component::Root;
    use tokio::sync::oneshot::error::TryRecvError;
    cx.update(gpui_kit::init);
    cx.update(crate::client_runtime::DesktopRuntimeCoordinator::install_for_test);
    let core = cx.update(|cx| {
        cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .core()
    });
    prepare(&core, true);
    let (root, cx) = cx.add_window_view(|window, cx| {
        let registrar = cx
            .global::<crate::client_runtime::DesktopRuntimeCoordinator>()
            .registrar();
        let navigation = crate::desktop_navigation::DesktopNavigationStore::new(registrar.as_ref());
        let layout = cx.new(|cx| crate::shell_state::ShellStateStore::new(window, cx));
        let shell = cx
            .new(|cx| crate::desktop_shell::DesktopShellView::new(navigation, layout, window, cx));
        Root::new(shell, window, cx)
    });
    cx.run_until_parked();
    let shell = root.read_with(cx, |root, _| {
        root.view()
            .clone()
            .downcast::<crate::desktop_shell::DesktopShellView>()
            .unwrap()
    });
    assert_eq!(
        cx.update(|_, cx| composer_outcome(&shell, cx)),
        Some(DesktopStartupOutcome::Ready)
    );
    let stale = FrameCandidate::new(&core, DesktopStartupOutcome::Ready);
    let draft = core.composer_snapshot("a").unwrap();
    // Message clears execution selection. This empty thread has no previous
    // turn to restore a model from, so configure Agent through the usual action.
    for action in [
        ComposerDomainAction::SetModeFromUser {
            mode: ThreadMode::Agent,
        },
        ComposerDomainAction::SetModelSelectionFromUser {
            provider: Some("provider".into()),
            model: Some("model".into()),
            capability_target: None,
        },
    ] {
        core.composer_intent(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id: draft.draft_id(),
            action,
        });
    }
    assert!(
        core.composer_snapshot("a")
            .unwrap()
            .has_complete_model_selection()
    );
    assert_eq!(
        cx.update(|_, cx| readiness(&core, composer_outcome(&shell, cx))),
        None,
        "published inputs must reach native composer"
    );
    let owner = cx.update(|_, cx| {
        DesktopStartupCoordinator::new(DesktopStartupTrace::start(), core.clone(), cx)
    });
    cx.run_until_parked();
    assert_eq!(
        cx.update(|_, cx| readiness(&core, composer_outcome(&shell, cx))),
        Some(DesktopStartupOutcome::Ready)
    );
    assert!(
        owner.applications_for_test.receiver_count() > 0,
        "native application alone must not finish startup"
    );
    assert!(!cx.update(|_, cx| stale.is_current(&core, composer_outcome(&shell, cx))));
    let rejected = PendingFrame::new(
        DesktopStartupTrace::start().stage(DesktopStartupStage::OperationalFrame),
    );
    let (sent, mut received) = tokio::sync::oneshot::channel();
    cx.update(|window, _| rejected.schedule(window, &core, stale, sent));
    cx.update(|window, cx| {
        window.simulate_next_frame(cx);
    });
    assert_eq!(received.try_recv(), Ok(false));
    assert!(rejected.0.borrow().is_none());
    assert!(
        owner.applications_for_test.receiver_count() > 0,
        "callbacks are not a draw"
    );
    // This is the actual production root/native composer frame, not a callback
    // simulation or a stand-in element. GPUI's API also clears its element arena.
    cx.update(|window, cx| window.draw(cx).clear(cx));
    assert!(cx.debug_bounds("thread-composer").is_some());
    assert!(
        owner.applications_for_test.receiver_count() > 0,
        "draw still needs acknowledgement"
    );
    cx.update(|window, cx| {
        window.simulate_next_frame(cx);
    });
    cx.run_until_parked();
    assert_eq!(
        owner.applications_for_test.receiver_count(),
        0,
        "completed task releases its watch receiver"
    );
    assert_eq!(
        owner.applications_for_test.sender_count(),
        1,
        "shell/composer observers release their captured senders"
    );
    assert_eq!(received.try_recv(), Err(TryRecvError::Closed));
    core.composer_intent(ComposerIntent::EditText {
        thread_id: "a".into(),
        draft_id: draft.draft_id(),
        text: "already working".into(),
    });
    cx.run_until_parked();
    assert_eq!(owner.applications_for_test.receiver_count(), 0);
    assert_eq!(
        owner.applications_for_test.sender_count(),
        1,
        "later publications cannot restart completed startup"
    );
}

#[test]
fn runtime_wait_uses_application_acknowledgement_after_cli_discovery() {
    let core = ClientCore::new();
    prepare(&core, true);
    let draft = core.composer_snapshot("a").unwrap();
    for action in [
        ComposerDomainAction::SetModeFromUser {
            mode: ThreadMode::Agent,
        },
        ComposerDomainAction::SetModelSelectionFromUser {
            provider: Some("cli_runtime:codex".into()),
            model: Some("model".into()),
            capability_target: None,
        },
    ] {
        core.composer_intent(ComposerIntent::Domain {
            thread_id: "a".into(),
            draft_id: draft.draft_id(),
            action,
        });
    }
    core.composer_intent(ComposerIntent::RetryRuntimeSelection {
        thread_id: "a".into(),
        draft_id: draft.draft_id(),
    });
    let authority = ClientMutationAuthority::for_test();
    let mut runtime: RuntimeSummary = serde_json::from_value(json!({"runtime_id":"codex","kind":"codex","display_name":"Codex","enabled":true,"status":{"state":"initializing"},"capabilities":RuntimeCapabilities { supports_threads:true, supports_model_list:true, ..Default::default() }})).unwrap();
    authority.complete_provider_runtime_for_test(
        &core,
        "workspace",
        Ok(CLIRuntimeListResponse {
            revision: 1,
            runtimes: vec![runtime.clone()],
        }),
    );
    assert_eq!(
        core.provider_runtime_snapshot("workspace")
            .unwrap()
            .request(),
        &pioneer_client::providers::runtime::ProviderRuntimeRequestState::Ready
    );
    assert!(
        core.composer_snapshot("a")
            .unwrap()
            .selected_provider_ready()
    );
    let mut telemetry = StartupTelemetry::new(DesktopStartupTrace::start());
    telemetry.observe(&core, None);
    assert!(
        telemetry
            .active
            .contains_key(&DesktopStartupStage::ComposerRuntimeWait)
    );
    assert!(
        !telemetry
            .completed
            .contains_key(&DesktopStartupStage::ComposerRuntimeWait)
    );
    // A different runtime becoming ready is not evidence about this selection.
    let mut other = runtime.clone();
    other.runtime_id = "other".into();
    other.status = RuntimeStatus::Ready;
    authority.provider_runtime_status_changed_for_test(
        &core,
        CLIRuntimeStatusChangedNotification {
            workspace_id: "workspace".into(),
            revision: 2,
            runtime: other,
            removed: false,
        },
    );
    telemetry.observe(&core, None);
    assert!(
        telemetry
            .active
            .contains_key(&DesktopStartupStage::ComposerRuntimeWait)
    );
    runtime.status = RuntimeStatus::Ready;
    authority.provider_runtime_status_changed_for_test(
        &core,
        CLIRuntimeStatusChangedNotification {
            workspace_id: "workspace".into(),
            revision: 3,
            runtime,
            removed: false,
        },
    );
    telemetry.observe(&core, None);
    assert!(
        telemetry
            .active
            .contains_key(&DesktopStartupStage::ComposerRuntimeWait),
        "wait for native application acknowledgement"
    );
    telemetry.observe(&core, Some(DesktopStartupOutcome::Ready));
    assert_eq!(
        telemetry
            .completed
            .get(&DesktopStartupStage::ComposerRuntimeWait),
        Some(&StartupStageState::Succeeded)
    );
    assert!(
        !telemetry
            .active
            .contains_key(&DesktopStartupStage::ComposerRuntimeWait)
    );
}
