use super::*;
use pioneer_client::threads::tree as thread_tree;

impl PioneerDesktop {
    pub(crate) fn upsert_thread_for_workspace(&mut self, thread_id: &str, workspace_id: &str) {
        self.upsert_thread_coordinator(thread_id, workspace_id);
    }

    pub(crate) fn thread_workspace_matches(&self, thread_id: &str, workspace_id: &str) -> bool {
        self.thread_workspace_id(thread_id).as_deref() == Some(workspace_id)
    }

    pub(crate) fn open_task_child_thread(
        &mut self,
        child_thread_id: String,
        title: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(parent_thread_id) = self.current_active_thread_id().map(str::to_owned) else {
            return;
        };
        if parent_thread_id == child_thread_id {
            return;
        }
        let Some(workspace_id) = self
            .thread_workspace_id(parent_thread_id.as_str())
            .or_else(|| self.active_workspace_id().map(str::to_owned))
        else {
            return;
        };

        self.remember_active_thread_draft(cx);
        self.navigation_intent(
            pioneer_client::navigation::NavigationIntent::PushTaskThread {
                entry: TaskThreadNavigationEntry::new(
                    parent_thread_id,
                    child_thread_id.clone(),
                    workspace_id.clone(),
                    title,
                ),
            },
        );
        self.set_main_content_view(MainContentView::Threads, cx);
        self.set_active_thread_id(Some(child_thread_id.clone()));
        self.clear_composer(window, cx);
        self.set_preferred_workspace_id(Some(workspace_id.clone()));

        if let Some(connection_id) = self.gateway.ws_connection_id {
            self.ensure_thread_subscription(
                child_thread_id.clone(),
                workspace_id.clone(),
                connection_id,
                cx,
            );
            self.refresh_cli_runtime_thread_binding(
                child_thread_id.clone(),
                workspace_id,
                connection_id,
                cx,
            );
        }

        self.ensure_thread_semantic_timeline_loaded(child_thread_id.as_str(), cx);
        cx.notify();
    }

    pub(crate) fn close_task_child_thread(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(entry) = self.navigation_input.lineage().last().cloned() else {
            return;
        };
        self.navigation_intent(pioneer_client::navigation::NavigationIntent::PopTaskThread);
        self.set_active_thread_id(Some(entry.parent_thread_id().to_owned()));
        self.restore_thread_draft(entry.parent_thread_id(), window, cx);
        self.set_preferred_workspace_id(Some(entry.workspace_id().to_owned()));

        if let Some(connection_id) = self.gateway.ws_connection_id {
            self.ensure_thread_subscription(
                entry.parent_thread_id().to_owned(),
                entry.workspace_id().to_owned(),
                connection_id,
                cx,
            );
            self.refresh_cli_runtime_thread_binding(
                entry.parent_thread_id().to_owned(),
                entry.workspace_id().to_owned(),
                connection_id,
                cx,
            );
        }

        self.ensure_thread_semantic_timeline_loaded(entry.parent_thread_id(), cx);
        cx.notify();
    }

    pub(in crate::app) fn ensure_thread_subscription(
        &mut self,
        thread_id: String,
        workspace_id: String,
        connection_id: u64,
        _cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || self.gateway.ws_connection_id != Some(connection_id)
        {
            return;
        }

        let tracks_startup_active_thread =
            self.current_active_thread_id() == Some(thread_id.as_str());
        if tracks_startup_active_thread {
            self.startup
                .begin(pioneer_observability::DesktopStartupStage::ActiveThreadSubscribe);
            self.active_thread_resubscribe_pending = true;
        }

        self.gateway
            .client_runtime
            .client_core()
            .schedule_thread_subscription(&thread_id, &workspace_id);
    }

    pub(in crate::app) fn ensure_thread_semantic_timeline_loaded(
        &mut self,
        thread_id: &str,
        cx: &mut Context<Self>,
    ) {
        self.request_semantic_thread_newest_page(thread_id.to_owned(), cx);
    }

    pub(in crate::app) fn refresh_cli_runtime_thread_binding(
        &mut self,
        thread_id: String,
        workspace_id: String,
        connection_id: u64,
        _cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected
            || self.gateway.ws_connection_id != Some(connection_id)
        {
            return;
        }

        self.gateway
            .client_runtime
            .client_core()
            .schedule_thread_cli_binding(&thread_id, &workspace_id);
    }
}

pub(crate) fn resolve_thread_tree_workspace_id(
    active_workspace_id: Option<&str>,
    preferred_workspace_id: Option<&str>,
    runtime_workspace_id: Option<&str>,
) -> Option<String> {
    pioneer_client::workspaces::selectors::resolve_workspace_scope(
        active_workspace_id,
        preferred_workspace_id,
        runtime_workspace_id,
    )
}
