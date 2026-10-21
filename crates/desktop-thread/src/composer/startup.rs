//! Passive startup inspection of the same inputs used by the displayed composer.
use super::*;
use pioneer_client::{
    composer::{
        catalog::ComposerCatalogRequestState,
        turn_prepare::{ComposerSubmissionPrerequisites, composer_submission_prerequisites_met},
    },
    core::ClientScope,
    timeline::types::ThreadMode,
};
use pioneer_observability::DesktopStartupOutcome;
use std::sync::Arc;

impl ComposerView {
    pub(crate) fn startup_outcome(&self) -> Option<DesktopStartupOutcome> {
        // Core publication precedes asynchronous application in the native view.
        // Inspect only committed inputs, and never synchronize or request work here.
        let navigation = self.client.navigation_snapshot();
        if navigation.active_thread_id() != Some(self.thread_id.as_str())
            || navigation.workspace_id() != self.active_workspace_id().as_deref()
        {
            return None;
        }
        let current = self.client.composer_snapshot(&self.thread_id)?;
        if self
            .composer_input
            .as_ref()
            .is_none_or(|p| !p.has_same_submission_prerequisites(&current))
        {
            return None;
        }
        let identity = self.client.snapshot(&ClientScope::Administration { workspace_id: None })?
            .typed::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>()?.payload();
        if self
            .identity_input
            .as_ref()
            .is_none_or(|p| !Arc::ptr_eq(p, &identity))
        {
            return None;
        }
        let capabilities = self.client.thread_capability_snapshot(&self.thread_id);
        if match (&self.thread_capability_input, &capabilities) {
            (Some(applied), Some(current)) => !Arc::ptr_eq(applied, current),
            (None, None) => false,
            _ => true,
        } {
            return None;
        }
        if !desktop_composer_transport_ready(self.connection_state) {
            return None;
        }
        if !self.composer_scoped_write_allowed() {
            return Some(DesktopStartupOutcome::Degraded);
        }
        let policy = capabilities
            .as_ref()
            .and_then(|p| p.snapshot.clone())
            .or_else(|| {
                identity
                    .capabilities
                    .snapshot(navigation.workspace_id(), None)
            })?
            .workspace?;
        if Some(policy.workspace_id.as_str()) != navigation.workspace_id() {
            return None;
        }
        if current.authorization_fingerprint()
            != Some(policy.execution_draft_policy.fingerprint.as_str())
        {
            // Send reconciles against this policy before accepting an operation.
            // Wait for the existing owner to publish that reconciliation.
            return None;
        }
        if self.composer_domain().selected_mode != ThreadMode::Message {
            if !current.selected_provider_ready()
                && !self
                    .principal_presentation_capabilities()
                    .can_use_cli_runtimes
            {
                return Some(DesktopStartupOutcome::Degraded);
            }
            let workspace = navigation.workspace_id()?;
            let discovery = self
                .client
                .provider_runtime_snapshot(workspace)
                .filter(|p| p.workspace_id() == workspace);
            // A ready list is not proof that the selected runtime has finished
            // preparing. Read the current owner's matching row without demand.
            if !current.selected_provider_ready()
                && current.runtime_selection().is_some_and(|p| p.request.state == ComposerCatalogRequestState::Ready)
                && discovery.as_ref().is_none_or(|p| p.workspace_id() != workspace
                    || *p.request() != pioneer_client::providers::runtime::ProviderRuntimeRequestState::Ready)
            {
                return None;
            }
            let selected = current.domain().selected_provider.as_deref().and_then(
                pioneer_client::providers::list::runtime_id_from_cli_runtime_provider_key,
            );
            let row = discovery.as_ref().and_then(|p| {
                p.runtimes()
                    .iter()
                    .find(|row| Some(row.runtime().runtime_id.as_str()) == selected)
            });
            // Initializing can precede capability discovery. Capabilities are
            // required only by the existing ready/Send gate, not by this wait.
            let status = row
                .filter(|row| row.runtime().enabled)
                .map(|row| &row.runtime().status);
            let row_ready = row.is_some_and(|row| {
                pioneer_client::providers::list::provider_ready_for_model_selector(
                    current.domain().selected_provider.as_deref(),
                    std::slice::from_ref(row.runtime()),
                )
            });
            if selected.is_some()
                && (discovery.is_none() || current.selected_provider_ready() != row_ready)
            {
                // The owner row and composer projection must describe the same
                // selection. Await their existing publications while they differ.
                return None;
            }
            match model_selection_outcome(&current, workspace, status) {
                Some(DesktopStartupOutcome::Ready) => {}
                Some(DesktopStartupOutcome::SetupRequired)
                    if self.composer_model_selection_pending() =>
                {
                    return None;
                }
                other => return other,
            }
        }
        composer_submission_prerequisites_met(ComposerSubmissionPrerequisites {
            gateway_connected: desktop_composer_transport_ready(self.connection_state)
                && self.composer_scoped_write_allowed(),
            has_active_thread: self.current_active_thread_id().is_some(),
            selected_mode: self.composer_domain().selected_mode,
            has_complete_model_selection: self.has_complete_composer_model_selection(),
        })
        .then_some(DesktopStartupOutcome::Ready)
    }
}

fn model_selection_outcome(
    current: &pioneer_client::composer::store::ComposerPublication,
    workspace: &str,
    selected_runtime_status: Option<&pioneer_client::providers::types::RuntimeStatus>,
) -> Option<DesktopStartupOutcome> {
    if current.domain().selected_mode == ThreadMode::Message {
        return Some(DesktopStartupOutcome::Ready);
    }
    if !pioneer_client::composer::model_selection::has_complete_composer_model_selection(
        current.domain().selected_provider.as_deref(),
        current.domain().selected_model.as_deref(),
    ) {
        return Some(DesktopStartupOutcome::SetupRequired);
    }
    let selected_cli = current
        .domain()
        .selected_provider
        .as_deref()
        .and_then(pioneer_client::providers::list::runtime_id_from_cli_runtime_provider_key)
        .is_some();
    if selected_cli
        && current.runtime_selection().is_none_or(|p| {
            p.workspace_id != workspace
                || p.identity.draft_id != current.draft_id()
                || p.selected_provider != current.domain().selected_provider
        })
    {
        return None;
    }
    if current.has_complete_model_selection() {
        return Some(DesktopStartupOutcome::Ready);
    }
    match current
        .runtime_selection()
        .filter(|p| {
            p.identity.draft_id == current.draft_id()
                && p.selected_provider == current.domain().selected_provider
                && p.workspace_id == workspace
        })
        .map(|p| &p.request.state)
    {
        None | Some(ComposerCatalogRequestState::Idle | ComposerCatalogRequestState::Loading) => {
            None
        }
        Some(ComposerCatalogRequestState::Ready) => {
            if matches!(
                selected_runtime_status,
                Some(pioneer_client::providers::types::RuntimeStatus::Initializing)
            ) {
                None
            } else {
                Some(DesktopStartupOutcome::SetupRequired)
            }
        }
        Some(
            ComposerCatalogRequestState::Failed { .. } | ComposerCatalogRequestState::Cancelled,
        ) => Some(DesktopStartupOutcome::Degraded),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::{
        composer::{
            state_machine::ComposerDomainState,
            store::{ComposerIntent, ComposerPublication},
        },
        core::ClientCore,
    };
    use serde_json::json;

    fn publication(
        mode: ThreadMode,
        runtime: Option<ComposerCatalogRequestState>,
        ready: bool,
    ) -> ComposerPublication {
        let core = ClientCore::new();
        core.composer_intent(ComposerIntent::Open {
            thread_id: "thread".into(),
            defaults: ComposerDomainState {
                selected_mode: mode,
                mode_manually_selected: true,
                ..Default::default()
            },
        });
        let current = core.composer_snapshot("thread").unwrap();
        let mut value = serde_json::to_value(&*current).unwrap();
        value["draft"]["domain"]["selected_provider"] = json!("cli_runtime:codex");
        value["draft"]["domain"]["selected_model"] = json!("model");
        value["selected_provider_ready"] = json!(ready);
        value["execution_selection_ready"] = json!(ready);
        if let Some(state) = runtime {
            value["runtime_selection"] = json!({
                "identity": {"thread_id":"thread", "draft_id":current.draft_id(), "generation":1},
                "workspace_id":"workspace", "request":{"generation":1,"state":state},
                "selected_provider":"cli_runtime:codex", "selected_provider_ready":ready,
                "active_runtime_supports_steer":null
            });
        }
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn selected_runtime_wait_failure_and_unavailable_are_distinct() {
        for mode in [ThreadMode::Chat, ThreadMode::Agent] {
            for state in [
                None,
                Some(ComposerCatalogRequestState::Idle),
                Some(ComposerCatalogRequestState::Loading),
            ] {
                assert_eq!(
                    model_selection_outcome(&publication(mode, state, false), "workspace", None),
                    None
                );
            }
            assert_eq!(
                model_selection_outcome(
                    &publication(
                        mode,
                        Some(ComposerCatalogRequestState::Failed {
                            message: "synthetic".into()
                        }),
                        false
                    ),
                    "workspace",
                    None
                ),
                Some(DesktopStartupOutcome::Degraded)
            );
            assert_eq!(
                model_selection_outcome(
                    &publication(mode, Some(ComposerCatalogRequestState::Ready), false),
                    "workspace",
                    None
                ),
                Some(DesktopStartupOutcome::SetupRequired)
            );
            assert_eq!(
                model_selection_outcome(
                    &publication(mode, Some(ComposerCatalogRequestState::Ready), true),
                    "workspace",
                    None
                ),
                Some(DesktopStartupOutcome::Ready)
            );
        }
    }

    #[test]
    fn message_needs_neither_selection_nor_runtime_and_empty_text_is_ready() {
        let mut value =
            serde_json::to_value(publication(ThreadMode::Message, None, false)).unwrap();
        value["draft"]["domain"]["selected_provider"] = json!(null);
        value["draft"]["domain"]["selected_model"] = json!(null);
        let message: ComposerPublication = serde_json::from_value(value).unwrap();
        assert!(message.draft().text.is_empty());
        assert_eq!(
            model_selection_outcome(&message, "workspace", None),
            Some(DesktopStartupOutcome::Ready)
        );
    }

    #[test]
    fn metadata_and_user_content_do_not_fence_preparation_but_selection_changes_do() {
        let ready = publication(
            ThreadMode::Agent,
            Some(ComposerCatalogRequestState::Ready),
            true,
        );
        let mut value = serde_json::to_value(&ready).unwrap();
        value["revision"] = json!(ready.revision() + 1);
        value["model_display"] = json!({
            "identity": {"thread_id":"thread", "draft_id":ready.draft_id(), "generation":2},
            "key":{"workspace_id":"workspace", "provider":"cli_runtime:codex", "model":"model"},
            "reasoning_effort":null, "request":{"kind":"loading"}, "label":null, "reasoning_effort_label":null
        });
        value["draft"]["text"] = json!("user has already started working");
        let content: ComposerPublication = serde_json::from_value(value.clone()).unwrap();
        assert!(ready.has_same_submission_prerequisites(&content));
        value["draft"]["domain"]["selected_model"] = json!("other-model");
        let other: ComposerPublication = serde_json::from_value(value).unwrap();
        assert!(!ready.has_same_submission_prerequisites(&other));
    }

    #[test]
    fn missing_model_is_setup_required_without_waiting_for_runtime() {
        let mut value = serde_json::to_value(publication(ThreadMode::Agent, None, false)).unwrap();
        value["draft"]["domain"]["selected_model"] = json!(null);
        let unconfigured: ComposerPublication = serde_json::from_value(value).unwrap();
        assert_eq!(
            model_selection_outcome(&unconfigured, "workspace", None),
            Some(DesktopStartupOutcome::SetupRequired)
        );
    }

    #[test]
    fn runtime_from_an_old_workspace_cannot_complete_preparation() {
        for ready in [false, true] {
            let selected = publication(
                ThreadMode::Agent,
                Some(ComposerCatalogRequestState::Ready),
                ready,
            );
            assert_eq!(model_selection_outcome(&selected, "other", None), None);
        }
    }
}
