use crate::app::{
    root::{GatewayConnectionState, MainContentView, PioneerDesktop, ThreadAgentsDocEditorScope},
    sidebar::agents_doc_tree_node_key,
};
use gpui_kit::component::theme::ActiveTheme;
use gpui_kit::{prelude::*, *};
use pioneer_desktop_agents_doc::{AgentsDocumentConfig, AgentsDocumentEditor};

impl PioneerDesktop {
    pub(crate) fn agents_document_scope(&self, cx: &App) -> Option<ThreadAgentsDocEditorScope> {
        self.agents_doc_editor
            .as_ref()
            .map(|editor| editor.read(cx).scope().clone())
    }

    pub(crate) fn present_document_close_error(
        &mut self,
        error: &pioneer_client::agents_doc::controller::AgentsDocumentCloseError,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        use pioneer_client::agents_doc::controller::AgentsDocumentCloseError;
        if let AgentsDocumentCloseError::SaveFailed(scope)
        | AgentsDocumentCloseError::Conflict(scope) = error
        {
            self.open_agents_doc_editor(scope.clone(), window, cx);
        }
        pioneer_desktop_agents_doc::present_close_error(error, window, cx);
    }

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

        self.navigation_intent(
            pioneer_client::navigation::NavigationIntent::OpenAgentsDocument {
                scope: scope.clone(),
            },
        );

        if self
            .agents_doc_editor
            .as_ref()
            .is_some_and(|editor| editor.read(cx).scope() == &scope)
        {
            self.set_main_content_view(MainContentView::AgentsDoc, cx);
            return;
        }

        if let Some(active_editor) = self.agents_doc_editor.clone() {
            let _ = active_editor.update(cx, |editor, cx| {
                editor.flush_pending_save(window, cx);
            });
        }

        let editor = AgentsDocumentEditor::new(
            AgentsDocumentConfig::new(
                self.gateway.client_runtime.client_core().clone(),
                cx.global::<crate::client_runtime::DesktopRuntimeCoordinator>()
                    .registrar(),
                scope.clone(),
            ),
            window,
            cx,
        );
        self.agents_doc_editor = Some(editor);
        self.set_main_content_view(MainContentView::AgentsDoc, cx);
    }
}
