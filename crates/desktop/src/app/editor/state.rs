use super::content::{
    agents_doc_conflict_refresh_projection, agents_doc_content_hash, agents_doc_get_params,
    agents_doc_is_version_conflict_error, agents_doc_load_projection,
    agents_doc_save_error_message, agents_doc_save_params, agents_doc_saved_at_now,
};
use crate::gateway::GatewayWsCommandSender;
use anyhow::Error;
use gpui::{prelude::*, *};
use gpui_component::input::{EditorState, InputEvent};
use pioneer_client::agents_doc::autosave::{
    AGENTS_DOC_AUTOSAVE_DELAY, AgentsDocAutosaveDecision, AgentsDocAutosaveState,
    AgentsDocEditorLoadState, AgentsDocEditorSaveState,
};
use pioneer_protocol::{
    ThreadAgentsDocGetResponse, ThreadAgentsDocPayload, ThreadAgentsDocResolvedPayload,
    ThreadAgentsDocSaveReason, ThreadAgentsDocSaveResponse,
};

pub(in crate::app) struct AgentsDocEditor {
    pub(super) workspace_id: String,
    pub(super) folder_id: Option<String>,
    pub(super) explicit_doc: Option<ThreadAgentsDocPayload>,
    pub(super) effective_doc: Option<ThreadAgentsDocResolvedPayload>,
    pub(super) input: Entity<EditorState>,
    pub(super) load_state: AgentsDocEditorLoadState,
    pub(super) autosave: AgentsDocAutosaveState,
    suppress_input_change_count: usize,
    ws_sender: GatewayWsCommandSender,
    window_handle: AnyWindowHandle,
    _input_subscription: Option<Subscription>,
}

impl AgentsDocEditor {
    pub(super) fn new(
        workspace_id: String,
        folder_id: Option<String>,
        input: Entity<EditorState>,
        ws_sender: GatewayWsCommandSender,
        window_handle: AnyWindowHandle,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut editor = Self {
            workspace_id,
            folder_id,
            explicit_doc: None,
            effective_doc: None,
            input,
            load_state: AgentsDocEditorLoadState::Loading,
            autosave: AgentsDocAutosaveState::new(),
            suppress_input_change_count: 0,
            ws_sender,
            window_handle,
            _input_subscription: None,
        };
        let subscription = cx.subscribe_in(
            &editor.input,
            window,
            |editor, _state, event, window, cx| match event {
                InputEvent::Change => editor.handle_input_change(window, cx),
                InputEvent::Blur => editor.flush_pending_save(window, cx),
                InputEvent::Focus | InputEvent::PressEnter { .. } => {}
            },
        );
        editor._input_subscription = Some(subscription);
        editor
    }

    pub(super) fn start_load(&mut self, cx: &mut Context<Self>) {
        self.load_state = AgentsDocEditorLoadState::Loading;
        self.explicit_doc = None;
        self.effective_doc = None;
        self.autosave.reset_from_explicit(None);

        let ws_sender = self.ws_sender.clone();
        let params = agents_doc_get_params(self.workspace_id.as_str(), self.folder_id.as_deref());
        let input = self.input.clone();
        let window_handle = self.window_handle;
        cx.spawn(move |editor: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.thread_agents_doc_get(params) })
                    .await;

                let _ = cx.update_window(window_handle, |_, window, cx| {
                    let _ = editor.update(cx, |editor, cx| {
                        match result {
                            Ok(response) => editor.apply_load_response(response, input, window, cx),
                            Err(error) => {
                                editor.explicit_doc = None;
                                editor.effective_doc = None;
                                editor.autosave.reset_from_explicit(None);
                                editor.load_state =
                                    AgentsDocEditorLoadState::Failed(format!("{error:#}"));
                                editor.set_input_value_suppressed(input, String::new(), window, cx);
                            }
                        }
                        cx.notify();
                    });
                });
            }
        })
        .detach();
    }

    fn apply_load_response(
        &mut self,
        response: ThreadAgentsDocGetResponse,
        input: Entity<EditorState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let projection = agents_doc_load_projection(response);
        self.explicit_doc = projection.explicit_doc;
        self.effective_doc = projection.effective_doc;
        self.load_state = AgentsDocEditorLoadState::Loaded;
        self.autosave
            .reset_from_loaded(self.explicit_doc.as_ref(), self.effective_doc.as_ref());
        self.set_input_value_suppressed(input, projection.buffer, window, cx);
    }

    fn set_input_value_suppressed(
        &mut self,
        input: Entity<EditorState>,
        value: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.suppress_input_change_count = self.suppress_input_change_count.saturating_add(1);
        input.update(cx, |state, cx| {
            state.set_value(value, window, cx);
        });
    }

    fn handle_input_change(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.suppress_input_change_count > 0 {
            self.suppress_input_change_count -= 1;
            return;
        }
        self.mark_dirty_and_schedule_autosave(window, cx);
    }

    fn mark_dirty_and_schedule_autosave(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.load_state, AgentsDocEditorLoadState::Loaded) {
            return;
        }

        let content = self.input.read(cx).value().to_string();
        if let AgentsDocAutosaveDecision::Schedule { generation } =
            self.autosave.mark_changed(content.as_str())
        {
            self.schedule_autosave(generation, cx);
        }
        cx.notify();
    }

    fn schedule_autosave(&self, generation: u64, cx: &mut Context<Self>) {
        cx.spawn(move |editor: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                cx.background_executor()
                    .timer(AGENTS_DOC_AUTOSAVE_DELAY)
                    .await;
                let _ = editor.update(&mut cx, |editor, cx| {
                    if !editor.autosave.debounce_due(generation) {
                        return;
                    }
                    editor.start_save_for_current_buffer(generation, cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn flush_pending_save(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        if !matches!(self.load_state, AgentsDocEditorLoadState::Loaded) {
            return;
        }

        let content = self.input.read(cx).value().to_string();
        if let AgentsDocAutosaveDecision::SaveNow { generation } =
            self.autosave.flush(content.as_str())
        {
            self.start_save_for_current_buffer(generation, cx);
        }
        cx.notify();
    }

    pub(super) fn retry_save_now(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.flush_pending_save(window, cx);
    }

    fn start_save_for_current_buffer(&mut self, generation: u64, cx: &mut Context<Self>) {
        if self.autosave.save_in_flight {
            return;
        }

        let content = self.input.read(cx).value().to_string();
        let expected_version = self.autosave.last_saved_version;
        let ws_sender = self.ws_sender.clone();
        let params = agents_doc_save_params(
            self.workspace_id.as_str(),
            self.folder_id.as_deref(),
            content.as_str(),
            expected_version,
            ThreadAgentsDocSaveReason::Autosave,
        );
        let window_handle = self.window_handle;

        self.autosave.mark_saving();
        cx.spawn(move |editor: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.thread_agents_doc_save(params) })
                    .await;

                let _ = cx.update_window(window_handle, |_, window, cx| {
                    let _ = editor.update(cx, |editor, cx| {
                        editor.apply_save_result(generation, result, window, cx);
                        cx.notify();
                    });
                });
            }
        })
        .detach();
    }

    fn apply_save_result(
        &mut self,
        _generation: u64,
        result: Result<ThreadAgentsDocSaveResponse, Error>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(response) => {
                self.explicit_doc = Some(response.doc.clone());
                let current_content = self.input.read(cx).value().to_string();
                let current_hash = agents_doc_content_hash(current_content.as_str());
                let decision = self.autosave.finish_success(
                    &response.doc,
                    current_hash.as_str(),
                    agents_doc_saved_at_now(),
                );
                if let AgentsDocAutosaveDecision::Schedule { generation } = decision {
                    self.schedule_autosave(generation, cx);
                }
            }
            Err(error) => {
                if agents_doc_is_version_conflict_error(&error) {
                    let local_content = self.input.read(cx).value().to_string();
                    self.start_conflict_refresh(local_content, cx);
                } else {
                    let message = agents_doc_save_error_message(&error);
                    self.autosave.finish_error(message);
                }
            }
        }
    }

    fn start_conflict_refresh(&mut self, local_content: String, cx: &mut Context<Self>) {
        self.autosave
            .finish_error(t!("editor.agents_doc.save_conflict").to_string());

        let ws_sender = self.ws_sender.clone();
        let params = agents_doc_get_params(self.workspace_id.as_str(), self.folder_id.as_deref());
        let window_handle = self.window_handle;

        cx.spawn(move |editor: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.thread_agents_doc_get(params) })
                    .await;

                let _ = cx.update_window(window_handle, |_, _window, cx| {
                    let _ = editor.update(cx, |editor, cx| {
                        editor.apply_conflict_refresh_result(local_content, result);
                        cx.notify();
                    });
                });
            }
        })
        .detach();
    }

    fn apply_conflict_refresh_result(
        &mut self,
        local_content: String,
        result: Result<ThreadAgentsDocGetResponse, Error>,
    ) {
        match result {
            Ok(response) => {
                let projection = agents_doc_conflict_refresh_projection(response);
                let Some(remote_doc) = projection.remote_doc else {
                    self.autosave.finish_error(
                        t!("editor.agents_doc.save_conflict_missing_remote").to_string(),
                    );
                    return;
                };
                self.explicit_doc = projection.explicit_doc;
                self.effective_doc = projection.effective_doc;
                self.autosave.enter_conflict(local_content, remote_doc);
            }
            Err(error) => {
                self.autosave.finish_error(format!(
                    "{}: {error:#}",
                    t!("editor.agents_doc.save_conflict")
                ));
            }
        }
    }

    pub(super) fn reload_remote_conflict(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let remote_doc = match &self.autosave.save_state {
            AgentsDocEditorSaveState::Conflict { remote_doc, .. } => remote_doc.clone(),
            _ => return,
        };

        self.explicit_doc = Some(remote_doc.clone());
        self.autosave.reload_remote(&remote_doc);
        self.set_input_value_suppressed(self.input.clone(), remote_doc.content, window, cx);
        cx.notify();
    }

    pub(super) fn overwrite_remote_conflict(&mut self, cx: &mut Context<Self>) {
        if !matches!(
            self.autosave.save_state,
            AgentsDocEditorSaveState::Conflict { .. }
        ) {
            return;
        }

        let content = self.input.read(cx).value().to_string();
        if let AgentsDocAutosaveDecision::SaveNow { generation } =
            self.autosave.prepare_conflict_overwrite(content.as_str())
        {
            self.start_save_for_current_buffer(generation, cx);
        }
        cx.notify();
    }
}
