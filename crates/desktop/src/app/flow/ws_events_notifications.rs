use super::*;
use crate::app::root::{AdministrationContentView, MainContentView};
use pioneer_client::administration::{AdministrationEvent, AdministrationRefetch};
use pioneer_client::authorization::AccessChangedPlan;
#[cfg(test)]
use pioneer_client::authorization::{ThreadAuthorizationScope, plan_access_changed};
use pioneer_client::notifications::router::{
    ArtifactDeletedRefreshReduction, ArtifactThreadRefreshReduction, CLIRuntimeSnapshotReduction,
    SkillsRefreshReduction, ThreadArtifactsRefreshReduction, WorkspacePreferenceReduction,
    WorkspaceRefreshReduction, apply_workspace_changed_to_catalog,
};
use pioneer_client::providers::list::CliRuntimeSnapshotUpdate;
use pioneer_client::runtime::{ClientRuntimeNotification, ClientRuntimeNotificationContext};
use pioneer_client::workspaces::selectors as workspace_selectors;

impl PioneerDesktop {
    pub(in crate::app::flow) fn apply_gateway_notification(
        &mut self,
        notification: GatewayNotification,
        cx: &mut Context<Self>,
    ) {
        if self
            .gateway
            .client_runtime
            .client_core()
            .apply_thread_notification(notification.clone())
        {
            return;
        }
        let active_workspace = self.active_workspace_scope_for_notifications();
        let mcp_workspace = self.mcp_workspace_scope();
        let notification_thread_workspace_matches =
            self.notification_thread_workspace_matches(&notification);
        let start = self.thread_start_coordinator();
        let artifacts = self.current_active_thread_id().and_then(|thread| {
            self.gateway
                .client_runtime
                .client_core()
                .artifact_snapshot(thread)
        });
        let context = ClientRuntimeNotificationContext {
            pending_thread_id: start.pending_thread_id.as_deref(),
            active_thread_id: self.current_active_thread_id(),
            active_workspace_id: active_workspace.as_deref(),
            notification_thread_workspace_matches,
            active_thread_artifacts: artifacts
                .as_ref()
                .map_or(&[], |input| input.items.as_slice()),
            preferred_workspace_id: self.preferred_workspace_id(),
            workspaces: self.workspaces(),
            mcp_workspace_id: mcp_workspace.as_deref(),
            mcp_selected_server_id: self.navigation_input.mcp_server_id(),
            mcp_details_loaded: self.mcp_server_details.is_some(),
        };
        let reduction = self
            .gateway
            .client_runtime
            .reduce_gateway_notification(notification, context);
        if let Some(reduction) = reduction {
            self.apply_gateway_notification_reduction(reduction, cx);
        }
    }

    fn notification_thread_workspace_matches(&self, notification: &GatewayNotification) -> bool {
        match notification {
            GatewayNotification::ThreadClosed(notification) => self.thread_workspace_matches(
                notification.thread_id.as_str(),
                notification.workspace_id.as_str(),
            ),
            GatewayNotification::ThreadArtifactsChanged(notification) => self
                .thread_workspace_matches(
                    notification.thread_id.as_str(),
                    notification.workspace_id.as_str(),
                ),
            _ => false,
        }
    }

    fn apply_gateway_notification_reduction(
        &mut self,
        reduction: ClientRuntimeNotification,
        cx: &mut Context<Self>,
    ) {
        match reduction {
            ClientRuntimeNotification::AccessChanged(notification) => {
                self.apply_access_changed_notification(notification, cx);
            }
            ClientRuntimeNotification::AuthorizationProjectionChanged(notification) => {
                self.apply_authorization_projection_changed_notification(notification, cx);
            }
            ClientRuntimeNotification::AdministrationChanged(event) => {
                self.apply_administration_event(event, cx);
            }
            ClientRuntimeNotification::ThreadStarted(_) => {}
            ClientRuntimeNotification::TurnLifecycle(_) => {}
            ClientRuntimeNotification::ConversationEvent(_) => {}
            ClientRuntimeNotification::ThreadClosed(_) => {}
            ClientRuntimeNotification::WorkspaceRefresh(_) => {}
            ClientRuntimeNotification::ThreadUpdated(_) => {}
            ClientRuntimeNotification::ThreadParticipantsChanged(_) => {}
            ClientRuntimeNotification::SkillsRefresh(reduction) => {
                self.apply_skills_refresh_reduction(reduction);
            }
            ClientRuntimeNotification::McpRefresh(reduction) => {
                self.apply_mcp_refresh_reduction(reduction);
            }
            ClientRuntimeNotification::McpServerStatusChanged(reduction) => {
                self.apply_mcp_server_status_changed_reduction(reduction, cx);
            }
            ClientRuntimeNotification::McpServerCatalogChanged(reduction) => {
                self.apply_mcp_server_catalog_changed_reduction(reduction, cx);
            }
            ClientRuntimeNotification::ThreadArtifactsRefresh(_)
            | ClientRuntimeNotification::ArtifactThreadRefresh(_)
            | ClientRuntimeNotification::ArtifactDeletedRefresh(_) => {}
            ClientRuntimeNotification::SemanticTimeline(_) => {}
            ClientRuntimeNotification::VoiceSessionResult(_) => {}
            ClientRuntimeNotification::CLIRuntimeSnapshot(reduction) => {
                self.apply_cli_runtime_snapshot_reduction(reduction, cx);
            }
            ClientRuntimeNotification::CLIRuntimePendingRequests(_) => {}
            ClientRuntimeNotification::PendingRequests { .. } => {}
            ClientRuntimeNotification::TaskUserNotificationDelivered(_) => {}
            ClientRuntimeNotification::GatewayRemoteAccessStatusChanged(_)
            | ClientRuntimeNotification::GatewayThreadEpisodicVectorRefillStatusChanged(_)
            | ClientRuntimeNotification::GatewayVoiceInputStatusChanged(_) => {}
            ClientRuntimeNotification::WorkspaceChanged { .. } => {}
        }
    }

    fn apply_administration_event(&mut self, event: AdministrationEvent, cx: &mut Context<Self>) {
        let current_profile_changed = matches!(
            &event,
            AdministrationEvent::MemberChanged(notification)
                if self.gateway.current_auth.as_ref().is_some_and(|auth| {
                    auth.principal.id == notification.principal_id
                })
        );
        let invalidation = self.administration.apply_event(&event);
        if invalidation.apply {
            self.apply_administration_refetches(invalidation.effects, cx);
            if current_profile_changed {
                self.refresh_current_principal(cx);
            }
        }
    }

    pub(in crate::app) fn apply_administration_refetches(
        &mut self,
        effects: Vec<AdministrationRefetch>,
        cx: &mut Context<Self>,
    ) {
        for effect in effects {
            match effect {
                AdministrationRefetch::InvitationList
                    if self.main_content_view() == MainContentView::Administration
                        && self.administration_content_view()
                            == AdministrationContentView::Invitations =>
                {
                    self.refresh_invitations(false, cx);
                }
                AdministrationRefetch::MemberDirectory => {
                    self.members_error = None;
                    if self.main_content_view() == MainContentView::Administration
                        && self.administration_content_view() == AdministrationContentView::Members
                    {
                        self.refresh_members(false, cx);
                    }
                }
                AdministrationRefetch::WorkspaceMembers { workspace_id }
                    if self.main_content_view() == MainContentView::Administration
                        && self.administration_content_view()
                            == AdministrationContentView::Members =>
                {
                    self.refresh_workspace_members(workspace_id, cx);
                }
                _ => {}
            }
        }
    }

    fn apply_access_changed_notification(
        &mut self,
        notification: pioneer_protocol::AccessChangedNotification,
        cx: &mut Context<Self>,
    ) {
        let active_workspace_id = self.active_workspace_id().map(str::to_owned);
        // The Client plan records selection before the authorization fence. A navigation
        // publication may already have cleared selection when this binding runs.
        let plan = self
            .gateway
            .client_runtime
            .client_core()
            .apply_thread_access_change(&notification);
        if !plan.apply {
            return;
        }

        let administration_invalidation = self.administration.apply_access_changed(&notification);

        // A newer revision is one atomic fence across global, workspace and
        // thread projections. No capability from the previous generation may
        // remain readable while its replacement is fetched.

        self.gateway.capability_snapshot = None;

        self.read_workspace_catalog_output();

        for thread_id in &plan.invalidate_thread_ids {
            self.remove_thread_conversation(thread_id.as_str());
        }
        let workspace_wide = plan.change == pioneer_protocol::AccessChangeKind::WorkspaceMembership;
        let workspace_access_lost = workspace_wide
            && notification.outcome == pioneer_protocol::AccessChangeOutcome::Revoked;
        if workspace_wide
            && (plan.clear_active_workspace
                || active_workspace_id.as_deref() == Some(plan.workspace_id.as_str()))
        {
            // Membership/role changes fence every provider projection from
            // the previous authorization generation. The shared client
            // effect below reloads both catalogs through current-ACL APIs.
            self.providers.clear_for_workspace_switch();
            self.sync_open_model_selector_cli_runtime_snapshot();
        }

        let active_editor_lost = workspace_access_lost
            && self
                .active_agents_doc_editor_scope
                .as_ref()
                .is_some_and(|scope| scope.workspace_id() == plan.workspace_id);
        if active_editor_lost {
            self.active_agents_doc_editor_scope = None;
            self.agents_doc_editor = None;
            if self.main_content_view() == MainContentView::AgentsDoc {
                self.set_main_content_view(MainContentView::Threads, cx);
            }
        }

        if workspace_access_lost {
            self.gateway
                .client_runtime
                .client_core()
                .apply_pending_requests(
                pioneer_client::cli_runtime::approvals::PendingRequestsReduction::ClearWorkspace {
                    workspace_id: plan.workspace_id.clone(),
                },
            );
        }

        if plan.clear_active_thread {
            self.set_active_thread_id(None);
        }

        if plan.clear_workspace_capability_projections {
            self.clear_workspace_capability_projections();
            self.clear_persisted_active_gateway_workspace_id();
        }

        let affected_workspace_is_active = plan.clear_active_workspace
            || active_workspace_id.as_deref() == Some(plan.workspace_id.as_str());
        if affected_workspace_is_active
            && (workspace_access_lost || !plan.invalidate_thread_ids.is_empty())
        {}
        execute_desktop_client_effects(self, plan.effects, cx);
        if administration_invalidation.apply {
            self.apply_administration_refetches(administration_invalidation.effects, cx);
        }
        self.refresh_current_principal(cx);
        cx.notify();
    }

    fn apply_authorization_projection_changed_notification(
        &mut self,
        notification: pioneer_protocol::AuthorizationProjectionChangedNotification,
        cx: &mut Context<Self>,
    ) {
        let (invalidate_workspace, invalidate_thread) = desktop_authorization_projection_effects(
            &notification.affected,
            self.active_workspace_id(),
            self.current_active_thread_id(),
        );
        if !invalidate_workspace && !invalidate_thread {
            return;
        }
        self.gateway.capability_snapshot = None;

        // The durable generation is a fail-closed fence, not merely a cache
        // hint.  Once old projections are removed, immediately rebuild both
        // active scopes from the Gateway so a connected client cannot remain
        // indefinitely disabled (or retain a stale draft) until an unrelated
        // lifecycle event happens to refresh it.
        self.refresh_current_principal(cx);

        cx.notify();
    }

    fn apply_skills_refresh_reduction(&mut self, reduction: SkillsRefreshReduction) {
        if reduction.queue_skills_refresh {
            self.queue_skills_refresh();
        }
    }

    fn apply_cli_runtime_snapshot_reduction(
        &mut self,
        reduction: CLIRuntimeSnapshotReduction,
        cx: &mut Context<Self>,
    ) {
        match reduction {
            CLIRuntimeSnapshotReduction::Upsert {
                revision,
                runtime,
                removed,
                workspace_matches: true,
                ..
            } => {
                match self
                    .providers
                    .apply_cli_runtime_snapshot_update(revision, *runtime, removed)
                {
                    CliRuntimeSnapshotUpdate::Applied => {
                        self.sync_open_model_selector_cli_runtime_snapshot();
                        cx.notify();
                    }
                    CliRuntimeSnapshotUpdate::ReloadRequired => {
                        self.load_cli_provider_snapshot(cx);
                    }
                    CliRuntimeSnapshotUpdate::Stale => {}
                }
            }
            CLIRuntimeSnapshotReduction::Reload {
                workspace_matches: true,
                ..
            } => self.load_cli_provider_snapshot(cx),
            CLIRuntimeSnapshotReduction::Upsert { .. }
            | CLIRuntimeSnapshotReduction::Reload { .. } => {}
        }
    }

    fn active_workspace_scope_for_notifications(&self) -> Option<String> {
        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(GatewayRuntime::active_workspace_id);
        workspace_selectors::resolve_workspace_scope(
            self.active_workspace_id(),
            self.preferred_workspace_id(),
            runtime_workspace_id,
        )
    }
}

fn desktop_authorization_projection_effects(
    affected: &pioneer_protocol::AuthorizationChangeScope,
    active_workspace_id: Option<&str>,
    active_thread_id: Option<&str>,
) -> (bool, bool) {
    match affected {
        pioneer_protocol::AuthorizationChangeScope::Global
        | pioneer_protocol::AuthorizationChangeScope::Role { .. }
        | pioneer_protocol::AuthorizationChangeScope::Principal { .. } => (true, true),
        pioneer_protocol::AuthorizationChangeScope::PrincipalWorkspace { workspace_id, .. } => {
            let active = active_workspace_id == Some(workspace_id.as_str());
            (active, active)
        }
        pioneer_protocol::AuthorizationChangeScope::PrincipalThread {
            workspace_id,
            thread_id,
            ..
        } => (
            false,
            active_workspace_id == Some(workspace_id.as_str())
                && active_thread_id == Some(thread_id.as_str()),
        ),
        pioneer_protocol::AuthorizationChangeScope::Invitation { .. } => (false, false),
        pioneer_protocol::AuthorizationChangeScope::Workspace { workspace_id }
        | pioneer_protocol::AuthorizationChangeScope::ResourceSelector { workspace_id, .. } => {
            let active = active_workspace_id == Some(workspace_id.as_str());
            (active, active)
        }
        pioneer_protocol::AuthorizationChangeScope::Thread {
            workspace_id,
            thread_id,
        } => (
            false,
            active_workspace_id == Some(workspace_id.as_str())
                && active_thread_id == Some(thread_id.as_str()),
        ),
    }
}

#[cfg(test)]
fn desktop_thread_authorization_scopes(
    coordinators: &std::collections::HashMap<String, crate::app::thread::ThreadCoordinator>,
) -> Vec<ThreadAuthorizationScope> {
    coordinators
        .iter()
        .map(|(thread_id, coordinator)| ThreadAuthorizationScope {
            thread_id: thread_id.clone(),
            workspace_id: coordinator.workspace_id.clone(),
        })
        .collect()
}

#[cfg(test)]
fn desktop_access_change_invalidates_workspace_capability_snapshot(
    notification: &pioneer_protocol::AccessChangedNotification,
) -> bool {
    notification.change == pioneer_protocol::AccessChangeKind::WorkspaceMembership
}

fn apply_desktop_workspace_catalog_invalidation(
    workspaces: &mut Vec<pioneer_protocol::Workspace>,
    preferred_workspace_id: &mut Option<String>,
    plan: &AccessChangedPlan,
) {
    if plan.change != pioneer_protocol::AccessChangeKind::WorkspaceMembership {
        return;
    }
    workspaces.retain(|workspace| workspace.id != plan.workspace_id);
    if preferred_workspace_id.as_deref() == Some(plan.workspace_id.as_str()) {
        *preferred_workspace_id = None;
    }
}

#[cfg(test)]
mod access_change_tests {
    use super::*;
    use crate::app::thread::ThreadCoordinator;
    use pioneer_protocol::{AccessChangeKind, AccessChangedNotification};
    use std::collections::HashMap;

    #[::core::prelude::v1::test]
    fn policy_generation_change_invalidates_only_the_exact_desktop_scope() {
        let principal_id = pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap();
        assert_eq!(
            desktop_authorization_projection_effects(
                &pioneer_protocol::AuthorizationChangeScope::PrincipalThread {
                    principal_id: principal_id.clone(),
                    workspace_id: "workspace-active".to_owned(),
                    thread_id: "thread-active".to_owned(),
                },
                Some("workspace-active"),
                Some("thread-active"),
            ),
            (false, true)
        );
        assert_eq!(
            desktop_authorization_projection_effects(
                &pioneer_protocol::AuthorizationChangeScope::PrincipalThread {
                    principal_id,
                    workspace_id: "workspace-active".to_owned(),
                    thread_id: "thread-other".to_owned(),
                },
                Some("workspace-active"),
                Some("thread-active"),
            ),
            (false, false)
        );
        assert_eq!(
            desktop_authorization_projection_effects(
                &pioneer_protocol::AuthorizationChangeScope::Workspace {
                    workspace_id: "workspace-active".to_owned(),
                },
                Some("workspace-active"),
                Some("thread-active"),
            ),
            (true, true)
        );
        assert_eq!(
            desktop_authorization_projection_effects(
                &pioneer_protocol::AuthorizationChangeScope::Invitation {
                    invitation_id: pioneer_protocol::InvitationId::new("I00000000000000000001",)
                        .unwrap(),
                },
                Some("workspace-active"),
                Some("thread-active"),
            ),
            (false, false)
        );
    }

    #[::core::prelude::v1::test]
    fn thread_scoped_access_changes_retain_verified_workspace_capabilities() {
        for change in [
            AccessChangeKind::ThreadCreated,
            AccessChangeKind::ThreadVisibility,
            AccessChangeKind::ThreadParticipantAdded,
            AccessChangeKind::ThreadParticipantRemoved,
        ] {
            assert!(
                !desktop_access_change_invalidates_workspace_capability_snapshot(
                    &AccessChangedNotification {
                        authorization_revision: 2,
                        workspace_id: "workspace-member".to_owned(),
                        thread_id: Some("thread-member".to_owned()),
                        outcome: pioneer_protocol::AccessChangeOutcome::Retained,
                        change,
                    },
                ),
                "{change:?} must refresh without collapsing workspace agent capabilities"
            );
        }

        assert!(
            desktop_access_change_invalidates_workspace_capability_snapshot(
                &AccessChangedNotification {
                    authorization_revision: 3,
                    workspace_id: "workspace-member".to_owned(),
                    thread_id: None,
                    outcome: pioneer_protocol::AccessChangeOutcome::Retained,
                    change: AccessChangeKind::WorkspaceMembership,
                },
            )
        );
    }

    fn workspace(id: &str) -> pioneer_protocol::Workspace {
        pioneer_protocol::Workspace {
            id: id.to_owned(),
            name: format!("{id} workspace"),
            is_active: true,
            is_current: false,
            created_at: 1,
            updated_at: 2,
        }
    }

    #[::core::prelude::v1::test]
    fn desktop_adapter_uses_shared_plan_and_keeps_unrelated_workspace_threads() {
        let coordinators = HashMap::from([
            (
                "thread-protected".to_owned(),
                ThreadCoordinator::pending("thread-protected", "workspace-protected"),
            ),
            (
                "thread-kept".to_owned(),
                ThreadCoordinator::pending("thread-kept", "workspace-kept"),
            ),
        ]);
        let scopes = desktop_thread_authorization_scopes(&coordinators);

        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 9,
                workspace_id: "workspace-protected".to_owned(),
                thread_id: None,
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::WorkspaceMembership,
            },
            Some(8),
            Some("workspace-protected"),
            Some("thread-protected"),
            scopes.as_slice(),
        );

        assert!(plan.apply);
        assert!(plan.clear_active_workspace);
        assert!(plan.clear_active_thread);
        assert_eq!(plan.invalidate_thread_ids, vec!["thread-protected"]);
        assert!(scopes.iter().any(|scope| scope.thread_id == "thread-kept"));
    }

    #[::core::prelude::v1::test]
    fn desktop_adapter_ignores_stale_access_change_without_superuser_state_effects() {
        let coordinators = HashMap::from([(
            "thread-superuser".to_owned(),
            ThreadCoordinator::pending("thread-superuser", "workspace-superuser"),
        )]);
        let scopes = desktop_thread_authorization_scopes(&coordinators);

        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 4,
                workspace_id: "workspace-superuser".to_owned(),
                thread_id: Some("thread-superuser".to_owned()),
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::ThreadVisibility,
            },
            Some(4),
            Some("workspace-superuser"),
            Some("thread-superuser"),
            scopes.as_slice(),
        );

        assert!(!plan.apply);
        assert!(plan.invalidate_thread_ids.is_empty());
        assert!(plan.effects.is_empty());
    }

    #[::core::prelude::v1::test]
    fn desktop_thread_access_loss_preserves_workspace_catalog_and_preference() {
        let mut workspaces = vec![workspace("workspace-kept"), workspace("workspace-affected")];
        let mut preferred_workspace_id = Some("workspace-affected".to_owned());
        let plan = plan_access_changed(
            &AccessChangedNotification {
                authorization_revision: 5,
                workspace_id: "workspace-affected".to_owned(),
                thread_id: Some("thread-affected".to_owned()),
                outcome: pioneer_protocol::AccessChangeOutcome::Revoked,
                change: AccessChangeKind::ThreadParticipantRemoved,
            },
            Some(4),
            Some("workspace-affected"),
            Some("thread-affected"),
            &[
                ThreadAuthorizationScope {
                    thread_id: "thread-affected".to_owned(),
                    workspace_id: "workspace-affected".to_owned(),
                },
                ThreadAuthorizationScope {
                    thread_id: "thread-kept".to_owned(),
                    workspace_id: "workspace-affected".to_owned(),
                },
            ],
        );

        assert_eq!(plan.invalidate_thread_ids, vec!["thread-affected"]);
        apply_desktop_workspace_catalog_invalidation(
            &mut workspaces,
            &mut preferred_workspace_id,
            &plan,
        );

        assert_eq!(
            workspaces,
            vec![workspace("workspace-kept"), workspace("workspace-affected")]
        );
        assert_eq!(
            preferred_workspace_id.as_deref(),
            Some("workspace-affected")
        );
    }

    #[::core::prelude::v1::test]
    fn desktop_access_loss_wiring_evicts_every_protected_projection() {
        let source = include_str!("ws_events_notifications.rs");
        let production_source = source
            .split_once("#[cfg(test)]\nmod access_change_tests")
            .map(|(production_source, _)| production_source)
            .expect("access-change tests must remain separated from production wiring");
        for required in [
            "remove_threads(plan.invalidate_thread_ids.as_slice())",
            "self.remove_thread_conversation(thread_id.as_str())",
            "self.thread_folders",
            "self.thread_placements",
            "self.thread_agents_doc_summaries",
            "self.active_agents_doc_editor_scope = None",
            "PendingRequestsReduction::ClearWorkspace",
            "*self.thread_timeline_view_state.borrow_mut() = Default::default()",
            "self.thread_timeline_item_expanded.borrow_mut().clear()",
            "self.thread_timeline_terminal_item.borrow_mut().clear()",
            "*self.code_highlight_cache.borrow_mut() = Default::default()",
            "self.clear_workspace_capability_projections()",
            "self.clear_persisted_active_gateway_workspace_id()",
            "execute_desktop_client_effects(self, plan.effects, cx)",
        ] {
            assert!(
                production_source.contains(required),
                "Desktop access-loss path is missing `{required}`"
            );
        }
        for forbidden in [
            "remove_gateway(",
            "delete_gateway(",
            "clear_refresh_credential(",
            "revoke_auth_session(",
        ] {
            assert!(
                !production_source.contains(forbidden),
                "Desktop access-loss path must preserve endpoint/session state: `{forbidden}`"
            );
        }

        let mutations_source = include_str!("../root/mutations.rs");
        for required in [
            "self.providers.clear_for_workspace_switch()",
            "self.mcp_servers.clear()",
            "self.installed_skills.clear()",
            "self.composer_domain().capabilities.clear()",
            "self.composer_domain().skill_selections.clear()",
        ] {
            assert!(
                mutations_source.contains(required),
                "Desktop capability cleanup is missing `{required}`"
            );
        }
    }

    #[::core::prelude::v1::test]
    fn desktop_routes_administration_events_through_shared_revisioned_invalidation() {
        let source = include_str!("ws_events_notifications.rs");
        let production_source = source
            .split_once("#[cfg(test)]\nmod access_change_tests")
            .map(|(production_source, _)| production_source)
            .expect("Desktop notification tests must remain outside production wiring");

        assert!(production_source.contains("self.administration.apply_event(&event)"));
        assert!(
            production_source.contains("self.administration.apply_access_changed(&notification)")
        );
        assert!(
            production_source.contains(
                "ClientRuntimeNotification::AuthorizationProjectionChanged(notification)"
            )
        );
        assert!(production_source.contains("self.refresh_current_principal(cx)"));
        assert!(production_source.contains("self.request_active_thread_capabilities(true, cx)"));
        assert!(
            !production_source
                .contains("ClientRuntimeNotification::AdministrationChanged(_) => {}")
        );
    }
}

#[cfg(test)]
use pioneer_client::gateway::settings_store::apply_vector_refill_notification;

#[cfg(test)]
mod vector_refill_tests {
    use super::*;
    use pioneer_protocol::{
        GatewayThreadEpisodicVectorLocalModelStatus, GatewayThreadEpisodicVectorProvider,
        GatewayThreadEpisodicVectorRefillStatus,
        GatewayThreadEpisodicVectorRefillStatusChangedNotification,
        GatewayThreadEpisodicVectorSearchSettings,
    };

    fn notification(
        status: GatewayThreadEpisodicVectorRefillStatus,
        local_model_status: Option<GatewayThreadEpisodicVectorLocalModelStatus>,
        downloaded_bytes: Option<u64>,
        total_bytes: Option<u64>,
    ) -> GatewayThreadEpisodicVectorRefillStatusChangedNotification {
        GatewayThreadEpisodicVectorRefillStatusChangedNotification {
            workspace_id: "workspace-a".to_owned(),
            status,
            local_model_status,
            downloaded_bytes,
            total_bytes,
        }
    }

    #[::core::prelude::v1::test]
    fn vector_refill_progress_reducer_covers_download_and_terminal_states() {
        let mut settings = GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProvider::Local),
            model: Some("bge-small-en-v1.5".to_owned()),
            local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::Missing,
            ..GatewayThreadEpisodicVectorSearchSettings::default()
        };

        let terminal = apply_vector_refill_notification(
            &mut settings,
            &notification(
                GatewayThreadEpisodicVectorRefillStatus::Running,
                Some(GatewayThreadEpisodicVectorLocalModelStatus::Downloading),
                Some(16 * 1024 * 1024),
                Some(64 * 1024 * 1024),
            ),
        );
        assert!(!terminal);
        assert_eq!(
            settings.local_model_status,
            GatewayThreadEpisodicVectorLocalModelStatus::Downloading
        );
        assert_eq!(settings.downloaded_bytes, Some(16 * 1024 * 1024));
        assert_eq!(settings.total_bytes, Some(64 * 1024 * 1024));

        let terminal = apply_vector_refill_notification(
            &mut settings,
            &notification(
                GatewayThreadEpisodicVectorRefillStatus::Running,
                Some(GatewayThreadEpisodicVectorLocalModelStatus::Installed),
                None,
                None,
            ),
        );
        assert!(!terminal);
        assert_eq!(
            settings.local_model_status,
            GatewayThreadEpisodicVectorLocalModelStatus::Installed
        );
        assert_eq!(settings.downloaded_bytes, None);
        assert_eq!(settings.total_bytes, None);

        settings.downloaded_bytes = Some(64 * 1024 * 1024);
        settings.total_bytes = Some(64 * 1024 * 1024);
        let terminal = apply_vector_refill_notification(
            &mut settings,
            &notification(
                GatewayThreadEpisodicVectorRefillStatus::Failed,
                Some(GatewayThreadEpisodicVectorLocalModelStatus::Failed),
                None,
                None,
            ),
        );
        assert!(terminal);
        assert_eq!(
            settings.local_model_status,
            GatewayThreadEpisodicVectorLocalModelStatus::Failed
        );
        assert_eq!(settings.downloaded_bytes, None);
        assert_eq!(settings.total_bytes, None);
    }

    #[::core::prelude::v1::test]
    fn vector_refill_progress_reducer_accepts_legacy_running_notification() {
        let mut settings = GatewayThreadEpisodicVectorSearchSettings {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProvider::Local),
            local_model_status: GatewayThreadEpisodicVectorLocalModelStatus::Missing,
            ..GatewayThreadEpisodicVectorSearchSettings::default()
        };

        let terminal = apply_vector_refill_notification(
            &mut settings,
            &notification(
                GatewayThreadEpisodicVectorRefillStatus::Running,
                None,
                None,
                None,
            ),
        );

        assert!(!terminal);
        assert_eq!(
            settings.local_model_status,
            GatewayThreadEpisodicVectorLocalModelStatus::Downloading
        );
    }
}
