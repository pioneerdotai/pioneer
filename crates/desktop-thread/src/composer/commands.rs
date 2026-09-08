use super::*;
use gpui_kit::*;
use pioneer_client::{
    composer::{state_machine::ComposerDomainAction, store::*},
    core::ClientTransitionOutcome,
};
impl ComposerView {
    pub(crate) fn composer_domain_intent(&mut self, action: ComposerDomainAction) -> bool {
        let Some(input) = &self.composer_input else {
            return false;
        };
        let transition = self.client.composer_intent(ComposerIntent::Domain {
            thread_id: self.thread_id.clone(),
            draft_id: input.draft_id(),
            action,
        });
        self.composer_input = self.client.composer_snapshot(&self.thread_id);
        transition.outcome() == ClientTransitionOutcome::Changed
    }
    pub(super) fn composer_text_intent(&mut self, text: String) -> bool {
        let Some(input) = &self.composer_input else {
            return false;
        };
        if self.composer_editor_draft != Some(input.draft_id()) || input.draft().text == text {
            return false;
        }
        let transition = self.client.composer_intent(ComposerIntent::EditText {
            thread_id: self.thread_id.clone(),
            draft_id: input.draft_id(),
            text,
        });
        self.composer_input = self.client.composer_snapshot(&self.thread_id);
        transition.outcome() == ClientTransitionOutcome::Changed
    }
    pub(super) fn present_composer_authorization_notice(&mut self) {
        let Some(input) = &self.composer_input else {
            return;
        };
        let Some(fingerprint) = input.authorization_fingerprint() else {
            return;
        };
        let key = (input.draft_id(), fingerprint.to_owned());
        if self.composer_policy_notice.as_ref() == Some(&key) {
            return;
        }
        if input.reconciliation().is_some_and(|result| result.reasons.iter().any(|reason| reason.kind != pioneer_client::composer::reconciliation::ExecutionDraftReconciliationKind::PolicyGeneration)) {
            self.composer_policy_notice = Some(key);
            self.composer_upload_error = Some("Composer selections were updated to match the current policy".into());
        }
    }
    pub(super) fn remove_composer_attachment(&mut self, path: String) -> bool {
        if self.desktop_voice_context_locked() {
            return false;
        }
        let changed = self.composer_domain_intent(ComposerDomainAction::RemoveAttachment { path });
        if changed {
            self.composer_upload_error = None;
        }
        changed
    }
    pub(super) fn remove_composer_capability(&mut self, id: String) -> bool {
        !self.desktop_voice_context_locked()
            && self.composer_domain_intent(ComposerDomainAction::RemoveCapability { id })
    }
    pub(super) fn remove_composer_skill_selection(
        &mut self,
        selection: pioneer_client::composer::skill_selection::ComposerSkillSelection,
    ) -> bool {
        !self.desktop_voice_context_locked()
            && self.composer_domain_intent(ComposerDomainAction::RemoveSkillSelection { selection })
    }
    pub(super) fn open_composer_file_picker(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        if !self.active_artifact_presentation_policy().can_attach {
            return;
        }
        let Some(plan) = self.begin_operation(ComposerOperationKind::PickFiles) else {
            return;
        };
        self.native_generation = self
            .native_generation
            .checked_add(1)
            .expect("native operation generation exhausted");
        let presentation = crate::ports::ThreadPresentationOperation::new(
            self.thread_id.clone(),
            self.mount,
            self.native_generation,
        );
        let selection = self
            .files
            .select_attachments(presentation, plan.clone(), cx);
        let client = self.client.clone();
        self.file_task = Some(cx.spawn(async move |view, cx| {
            let completion = selection.await;
            if client.complete_composer_operation(plan.identity, completion) {
                let _ = view.update(cx, |view, cx| {
                    view.composer_input = client.composer_snapshot(&view.thread_id);
                    cx.notify();
                });
            }
        }));
    }
    pub(super) fn begin_operation(
        &mut self,
        operation: ComposerOperationKind,
    ) -> Option<ComposerOperationPlan> {
        let input = self.composer_input.as_ref()?;
        if self
            .client
            .composer_intent(ComposerIntent::BeginOperation {
                thread_id: self.thread_id.clone(),
                draft_id: input.draft_id(),
                operation,
            })
            .outcome()
            != ClientTransitionOutcome::Changed
        {
            return None;
        }
        self.composer_input = self.client.composer_snapshot(&self.thread_id);
        let plan = self.composer_input.as_ref()?.operation()?.plan.clone()?;
        match plan.kind {
            ComposerOperationKind::PickFiles => self.file_operation = Some(plan.identity.clone()),
            ComposerOperationKind::Send => self.send_operation = Some(plan.identity.clone()),
            _ => {}
        }
        Some(plan)
    }
    pub(super) fn submit_composer_message(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        self.present_composer_authorization_notice();
        if !self.can_submit_message(cx) || self.composer_authorization_fingerprint().is_none() {
            return;
        }
        let Some(plan) = self.begin_operation(ComposerOperationKind::Send) else {
            return;
        };
        let client = self.client.clone();
        let files = self.files.clone();
        let context = pioneer_client::composer::workflow::ComposerSendContext {
            workspace_id: self.active_workspace_id(),
            endpoint_kind: client.connected_gateway_endpoint_kind(),
            failure_message: t!("chat.composer.send_failed").to_string(),
        };
        self.composer_upload_error = None;
        let send = cx.background_spawn(async move {
            client.submit_composer_send(plan.identity, files.as_ref(), context)
        });
        self.send_task = Some(cx.spawn(async move |view, cx| {
            let _ = send.await;
            let _ = view.update(cx, |view, cx| {
                view.composer_input = view.client.composer_snapshot(&view.thread_id);
                cx.notify();
            });
        }));
        cx.notify();
    }
    pub(super) fn steer_active_cli_runtime_turn(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        if let Some(input) = &self.composer_input {
            self.client.composer_intent(ComposerIntent::SubmitSteer {
                thread_id: self.thread_id.clone(),
                draft_id: input.draft_id(),
            });
        }
        cx.notify();
    }
    pub(super) fn stop_active_turn(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        if !self.can_cancel_active_thread_agent_presentation() {
            return;
        }
        self.client.request_turn_cancellation(
            pioneer_client::turns::cancellation::TurnCancellationIntent {
                thread_id: self.thread_id.clone(),
                reason: Some(t!("chat.composer.stop_reason").to_string()),
            },
        );
        cx.notify();
    }
    pub(crate) fn start_composer_message_edit(
        &mut self,
        presentation: pioneer_client::timeline::rows::UserMessagePresentation,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.message_mutation_pending() {
            return;
        }
        let Some(input) = &self.composer_input else {
            return;
        };
        if self
            .client
            .composer_intent(ComposerIntent::StartMessageEdit {
                thread_id: self.thread_id.clone(),
                draft_id: input.draft_id(),
                turn_id: presentation.turn_id,
            })
            .outcome()
            == ClientTransitionOutcome::Changed
        {
            self.composer_input = self.client.composer_snapshot(&self.thread_id);
            self.controlled_text(window, cx);
            self.focus(window, cx);
            cx.notify();
        }
    }
    pub(crate) fn cancel_composer_message_edit(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.message_mutation_pending() {
            return;
        }
        if let Some(input) = &self.composer_input {
            self.client.composer_intent(ComposerIntent::Clear {
                thread_id: self.thread_id.clone(),
                draft_id: input.draft_id(),
            });
            self.composer_input = self.client.composer_snapshot(&self.thread_id);
            self.controlled_text(window, cx);
            cx.notify();
        }
    }
    pub(super) fn submit_composer_message_edit(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        if self.message_mutation_pending() {
            return;
        }
        if let Some(input) = &self.composer_input {
            self.client
                .composer_intent(ComposerIntent::SubmitMessageEdit {
                    thread_id: self.thread_id.clone(),
                    draft_id: input.draft_id(),
                });
        }
        cx.notify();
    }
}
