use super::*;
use pioneer_client::composer::{state_machine::ComposerDomainState, store::ComposerOperationKind};
impl ComposerView {
    pub(super) fn desktop_voice_context_locked(&self) -> bool {
        self.composer_input
            .as_ref()
            .and_then(|input| input.operation())
            .is_some_and(|operation| {
                operation.kind == ComposerOperationKind::Voice && operation.pending()
            })
    }

    pub(super) fn current_active_thread_id(&self) -> Option<&str> {
        Some(&self.thread_id)
    }
    pub(super) fn composer_domain(&self) -> &ComposerDomainState {
        static EMPTY: std::sync::LazyLock<ComposerDomainState> =
            std::sync::LazyLock::new(ComposerDomainState::default);
        self.composer_input
            .as_ref()
            .map_or(&EMPTY, |input| input.domain())
    }
    pub(super) fn composer_authorization_fingerprint(&self) -> Option<&str> {
        self.composer_input
            .as_ref()
            .and_then(|input| input.authorization_fingerprint())
    }
    pub(super) fn active_thread_conversation(
        &self,
    ) -> Option<pioneer_client::threads::registry::ConversationSnapshot> {
        self.client
            .thread_snapshot(&self.thread_id)
            .map(|snapshot| snapshot.conversation())
    }
    pub(super) fn active_workspace_id(&self) -> Option<String> {
        self.client
            .thread_coordinator_snapshot(&self.thread_id)
            .map(|thread| thread.workspace_id.clone())
    }
    pub(super) fn active_thread_snapshot(
        &self,
    ) -> pioneer_client::state::snapshot::ActiveThreadSnapshot {
        let thread = self.client.thread_coordinator_snapshot(&self.thread_id);
        pioneer_client::state::snapshot::ActiveThreadSnapshot::from_coordinator(
            Some(&self.thread_id),
            thread.as_deref(),
            self.is_draft(),
            self.connection_state == GatewayConnectionState::Connected,
            self.client.thread_start_requested(),
        )
    }
    fn is_draft(&self) -> bool {
        self.active_workspace_id().is_some_and(|workspace| {
            self.client.thread_workspace_draft(&workspace).as_deref() == Some(&self.thread_id)
        })
    }
    fn authorization(
        &self,
    ) -> Option<pioneer_client::authorization::AuthorizationCapabilitySnapshot> {
        self.identity_input
            .as_ref()?
            .capabilities
            .snapshot(self.active_workspace_id().as_deref(), None)
    }
    pub(super) fn principal_presentation_capabilities(
        &self,
    ) -> pioneer_client::authorization::PrincipalPresentationCapabilities {
        self.identity_input
            .as_ref()
            .and_then(|identity| {
                identity
                    .capabilities
                    .snapshot(self.active_workspace_id().as_deref(), None)
            })
            .as_ref()
            .map(pioneer_client::authorization::principal_presentation_capabilities)
            .unwrap_or_default()
    }
    pub(super) fn thread_presentation_capabilities(
        &self,
        thread: &str,
    ) -> Option<pioneer_client::authorization::ThreadPresentationCapabilities> {
        let input = self
            .thread_capability_input
            .as_ref()
            .filter(|p| p.thread_id == thread)?;
        Some(
            pioneer_client::authorization::thread_presentation_capabilities(
                input
                    .snapshot
                    .as_ref()?
                    .thread
                    .as_ref()
                    .map(|scope| &scope.capabilities),
            ),
        )
    }
    pub(super) fn can_write_active_thread_presentation(&self) -> bool {
        if self.is_draft() {
            return self
                .authorization()
                .and_then(|p| p.workspace)
                .is_some_and(|w| w.capabilities.can_create_thread);
        }
        self.thread_presentation_capabilities(&self.thread_id)
            .is_some_and(|c| c.can_write)
    }
    pub(super) fn can_start_active_thread_agent_presentation(&self) -> bool {
        if self.is_draft() {
            return self
                .authorization()
                .and_then(|p| p.workspace)
                .is_some_and(|w| w.capabilities.can_create_thread);
        }
        self.thread_presentation_capabilities(&self.thread_id)
            .is_some_and(|c| c.can_start_turn)
    }
    pub(super) fn can_cancel_active_thread_agent_presentation(&self) -> bool {
        self.thread_presentation_capabilities(&self.thread_id)
            .is_some_and(|c| c.can_cancel_agent_execution)
    }
    pub(super) fn active_artifact_presentation_policy(
        &self,
    ) -> pioneer_client::artifacts::presentation::ArtifactPresentationPolicy {
        if self.is_draft() {
            let workspace = self.authorization().and_then(|snapshot| snapshot.workspace);
            return pioneer_client::artifacts::presentation::artifact_presentation_policy(
                workspace
                    .as_ref()
                    .is_some_and(|scope| scope.capabilities.can_read_artifacts),
                workspace
                    .as_ref()
                    .is_some_and(|scope| scope.execution_draft_policy.can_attach_artifacts),
                self.connection_state == GatewayConnectionState::Connected,
            );
        }
        let capabilities = self.thread_presentation_capabilities(&self.thread_id);
        pioneer_client::artifacts::presentation::artifact_presentation_policy(
            capabilities.is_some_and(|c| c.can_read_artifacts),
            capabilities.is_some_and(|c| c.can_write_artifacts && c.can_bind_artifacts),
            self.connection_state == GatewayConnectionState::Connected,
        )
    }
    pub(super) fn composer_edit_target(
        &self,
    ) -> Option<&pioneer_client::composer::message_edit::ComposerMessageEditTarget> {
        self.composer_input
            .as_ref()
            .and_then(|input| input.message_edit())
    }
    pub(super) fn authorized_composer_permission_options(
        &self,
    ) -> Vec<pioneer_client::composer::permissions::ComposerPermissionModeOption> {
        self.composer_input
            .as_ref()
            .map(|input| input.permission_options().to_vec())
            .unwrap_or_default()
    }
    pub(super) fn effective_composer_capabilities(&self) -> Vec<ComposerCapability> {
        pioneer_client::composer::capabilities::plan_composer_submission(
            self.composer_domain().selected_provider.as_deref(),
            "",
            false,
            &self.composer_domain().capabilities,
        )
        .capabilities
    }
    pub(super) fn has_complete_composer_model_selection(&self) -> bool {
        pioneer_client::composer::model_selection::has_complete_composer_model_selection(
            self.composer_domain().selected_provider.as_deref(),
            self.composer_domain().selected_model.as_deref(),
        ) && self
            .composer_input
            .as_ref()
            .is_some_and(|input| input.selected_provider_ready())
    }
    pub(super) fn composer_upload_in_progress(&self) -> bool {
        use pioneer_client::composer::store::ComposerOperationStatus;
        self.composer_input
            .as_ref()
            .and_then(|input| input.operation())
            .is_some_and(|operation| match operation.kind {
                ComposerOperationKind::Send => operation.pending(),
                ComposerOperationKind::Voice => matches!(
                    operation.status,
                    ComposerOperationStatus::Preparing | ComposerOperationStatus::Uploading
                ),
                _ => false,
            })
    }
    pub(super) fn message_mutation_pending(&self) -> bool {
        self.client
            .message_deletion_snapshot(&self.thread_id)
            .is_some_and(|p| {
                p.state == pioneer_client::threads::message_deletion::MessageDeletionState::Pending
            })
            || self
                .composer_input
                .as_ref()
                .and_then(|input| input.operation())
                .is_some_and(|operation| {
                    operation.kind == ComposerOperationKind::EditMessage && operation.pending()
                })
    }
    pub(super) fn thread_member_directory_loading(&self) -> bool {
        self.thread_member_input.as_ref().is_some_and(|p| {
            p.workspace_request == pioneer_client::threads::members::ThreadMemberReadState::Loading
        })
    }
}

use pioneer_client::providers::presentation::ProviderModelDisplayState;
impl ComposerView {
    pub(super) fn composer_model_display_state(&self) -> ProviderModelDisplayState {
        use pioneer_client::composer::model_display::ComposerModelDisplayRequestState;
        if !self
            .composer_input
            .as_ref()
            .is_some_and(|input| input.selected_provider_ready())
        {
            return ProviderModelDisplayState::Missing;
        }
        let Some(display) = self
            .composer_input
            .as_ref()
            .and_then(|input| input.model_display())
        else {
            return if self.composer_model_selection_pending() {
                ProviderModelDisplayState::Loading
            } else {
                ProviderModelDisplayState::Missing
            };
        };
        if display.key.provider
            != self
                .composer_domain()
                .selected_provider
                .as_deref()
                .unwrap_or_default()
            || display.key.model
                != self
                    .composer_domain()
                    .selected_model
                    .as_deref()
                    .unwrap_or_default()
        {
            return ProviderModelDisplayState::Loading;
        }
        if display.request == ComposerModelDisplayRequestState::Loading {
            return ProviderModelDisplayState::Loading;
        }
        display
            .label
            .clone()
            .map(ProviderModelDisplayState::Label)
            .unwrap_or(ProviderModelDisplayState::Missing)
    }

    fn composer_model_selection_pending(&self) -> bool {
        !self.composer_domain().model_manually_selected
            && (self.connection_state.is_transitioning()
                || thread_creation_pending(
                    &self.thread_id,
                    &self.client.thread_start_snapshot(),
                    self.client.thread_start_requested(),
                )
                || self.active_thread_snapshot().history_loading)
    }
}

fn thread_creation_pending(
    thread: &str,
    start: &pioneer_client::threads::start::ThreadStartCoordinator,
    queued: bool,
) -> bool {
    start.pending_thread_id.as_deref() == Some(thread)
        && (queued || start.in_progress || start.next_attempt_at.is_some())
}

#[cfg(test)]
mod loading_tests {
    use super::thread_creation_pending;
    #[test]
    fn reserved_or_failed_draft_does_not_mean_model_loading() {
        let mut start = pioneer_client::threads::start::ThreadStartCoordinator {
            pending_thread_id: Some("draft".into()),
            ..Default::default()
        };
        assert!(!thread_creation_pending("draft", &start, false));
        start.in_progress = true;
        assert!(thread_creation_pending("draft", &start, false));
        assert!(!thread_creation_pending("other", &start, false));
        start.in_progress = false;
        start.next_attempt_at = Some(std::time::Instant::now());
        assert!(thread_creation_pending("draft", &start, false));
        start.next_attempt_at = None;
        assert!(!thread_creation_pending("draft", &start, false));
    }
}
