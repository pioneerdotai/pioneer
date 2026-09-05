use super::state::AgentsDocEditor;
use crate::app::{
    root::{GatewayConnectionState, MainContentView, PioneerDesktop, ThreadAgentsDocEditorScope},
    sidebar::agents_doc_tree_node_key,
};
use gpui_kit::component::{input::EditorState, theme::ActiveTheme};
use gpui_kit::{prelude::*, *};

impl PioneerDesktop {
    pub(crate) fn render_agents_doc_editor(&self, cx: &mut Context<Self>) -> AnyElement {
        pioneer_observability::record_qualification_diagnostic!(record_render(
            pioneer_observability::RenderRegion::AgentsDoc
        ));
        self.agents_doc_editor
            .as_ref()
            .map(|editor| editor.clone().into_any_element())
            .unwrap_or_else(|| {
                div()
                    .size_full()
                    .bg(cx.theme().background)
                    .flex()
                    .items_center()
                    .justify_center()
                    .text_sm()
                    .opacity(0.6)
                    .child(t!("editor.agents_doc.loading").to_string())
                    .into_any_element()
            })
    }

    pub(in crate::app) fn open_agents_doc_editor(
        &mut self,
        scope: ThreadAgentsDocEditorScope,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        if self.gateway.ws_connection_id.is_none() {
            return;
        }

        if self.active_agents_doc_editor_scope.as_ref() == Some(&scope)
            && self.agents_doc_editor.is_some()
        {
            self.set_main_content_view(MainContentView::AgentsDoc, cx);
            self.set_thread_tree_selected_node_id(Some(agents_doc_tree_node_key(&scope)));
            self.rebuild_sidebar_tree_state(cx);
            return;
        }

        if let Some(active_editor) = self.agents_doc_editor.clone() {
            let _ = active_editor.update(cx, |editor, cx| {
                editor.flush_pending_save(window, cx);
            });
        }

        let selected_node_id = agents_doc_tree_node_key(&scope);
        let (workspace_id, folder_id) = scope.clone().into_parts();
        if let Some(folder_id) = folder_id.as_deref() {
            self.set_thread_folder_expanded(folder_id, true, cx);
        }

        let input = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("markdown")
                .line_number(true)
                .soft_wrap(true)
                .default_value("")
        });
        let editor_workspace_id = workspace_id.clone();
        let editor_folder_id = folder_id.clone();
        let editor = cx.new(|cx| {
            let mut editor = AgentsDocEditor::new(
                editor_workspace_id,
                editor_folder_id,
                input,
                self.gateway.ws_command_sender.clone(),
                window.window_handle(),
                window,
                cx,
            );
            editor.start_load(cx);
            editor
        });
        self.active_agents_doc_editor_scope = Some(scope);
        self.agents_doc_editor = Some(editor);
        self.set_thread_tree_selected_node_id(Some(selected_node_id));
        self.set_main_content_view(MainContentView::AgentsDoc, cx);
        self.rebuild_sidebar_tree_state(cx);
    }
}
