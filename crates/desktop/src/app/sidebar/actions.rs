use crate::app::{
    root::{
        GatewayConnectionState, MainContentView, PioneerDesktop, ThreadAgentsDocEditorScope,
        ThreadAgentsDocSummaryKey,
    },
    sidebar::{SidebarTreeDragItem, SidebarTreeDragPayload},
};
use gpui::{prelude::*, *};
use pioneer_protocol::{
    ThreadAgentsDocArchiveParams, ThreadAgentsDocSummary, ThreadFolderCreateParams,
    ThreadFolderDeleteParams, ThreadFolderMoveParams, ThreadMoveParams,
};
use tracing::warn;

impl PioneerDesktop {
    pub(super) fn open_or_create_new_thread_from_sidebar(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.set_main_content_view(MainContentView::Threads, cx);
        self.remember_active_thread_draft(cx);

        if let Some(draft_thread_id) = self.resolve_existing_draft_thread_id() {
            self.activate_thread_with_draft_restore(draft_thread_id, window, cx);
            self.rebuild_sidebar_tree_state(cx);
            return;
        }

        self.set_thread_tree_selected_node_id(None);
        self.set_active_thread_id(None);
        self.clear_composer(window, cx);
        self.rebuild_sidebar_tree_state(cx);
        self.request_thread_start_if_needed();
        let _ = self.drive_thread_start_queue(cx);
    }

    pub(super) fn create_folder_from_sidebar(&mut self, cx: &mut Context<Self>) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(workspace_id) = self.sidebar_workspace_id() else {
            return;
        };

        let name = self.next_new_folder_name(workspace_id.as_str());
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let workspace_for_request = workspace_id.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.thread_folder_create(ThreadFolderCreateParams {
                            workspace_id: workspace_for_request,
                            parent_folder_id: None,
                            name,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    if let Err(error) = result {
                        warn!(
                            error = %format!("{error:#}"),
                            "failed to create thread folder"
                        );
                    }

                    view.refresh_thread_list(cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn rename_folder_from_sidebar(
        &mut self,
        folder_id: String,
        new_name: String,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(folder) = self.thread_folder(folder_id.as_str()).cloned() else {
            return;
        };
        let Some(active_workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        if folder.workspace_id != active_workspace_id {
            return;
        }
        let trimmed_name = new_name.trim();
        if trimmed_name.is_empty() || folder.name == trimmed_name {
            return;
        }

        let child_folder_ids: Vec<String> = self
            .thread_folders
            .values()
            .filter(|child| {
                child.workspace_id == folder.workspace_id
                    && child.parent_folder_id.as_deref() == Some(folder.id.as_str())
            })
            .map(|child| child.id.clone())
            .collect();
        let child_thread_ids: Vec<String> = self
            .thread_placements
            .values()
            .filter(|placement| {
                placement.workspace_id == folder.workspace_id
                    && placement.folder_id.as_deref() == Some(folder.id.as_str())
            })
            .map(|placement| placement.thread_id.clone())
            .collect();

        let ws_sender = self.gateway.ws_command_sender.clone();
        let workspace_id = folder.workspace_id.clone();
        let old_folder_id = folder.id.clone();
        let parent_folder_id = folder.parent_folder_id.clone();
        let new_name = trimmed_name.to_owned();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let workspace_id_for_request = workspace_id.clone();
            let old_folder_id_for_request = old_folder_id.clone();
            let parent_folder_id_for_request = parent_folder_id.clone();
            let child_folder_ids_for_request = child_folder_ids.clone();
            let child_thread_ids_for_request = child_thread_ids.clone();
            let new_name_for_request = new_name.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        let created = ws_sender.thread_folder_create(ThreadFolderCreateParams {
                            workspace_id: workspace_id_for_request.clone(),
                            parent_folder_id: parent_folder_id_for_request,
                            name: new_name_for_request,
                        })?;
                        let new_folder_id = created.folder.id;

                        for child_folder_id in child_folder_ids_for_request {
                            ws_sender.thread_folder_move(ThreadFolderMoveParams {
                                workspace_id: workspace_id_for_request.clone(),
                                folder_id: child_folder_id,
                                parent_folder_id: Some(new_folder_id.clone()),
                            })?;
                        }

                        for thread_id in child_thread_ids_for_request {
                            ws_sender.thread_move(ThreadMoveParams {
                                workspace_id: workspace_id_for_request.clone(),
                                thread_id,
                                folder_id: Some(new_folder_id.clone()),
                            })?;
                        }

                        ws_sender.thread_folder_delete(ThreadFolderDeleteParams {
                            workspace_id: workspace_id_for_request,
                            folder_id: old_folder_id_for_request,
                        })?;

                        Ok::<(), anyhow::Error>(())
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    if let Err(error) = result {
                        warn!(
                            error = %format!("{error:#}"),
                            "failed to rename thread folder"
                        );
                    }

                    view.refresh_thread_list(cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn delete_folder_from_sidebar(&mut self, folder_id: String, cx: &mut Context<Self>) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(folder) = self.thread_folder(folder_id.as_str()).cloned() else {
            return;
        };
        let Some(active_workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        if folder.workspace_id != active_workspace_id {
            return;
        }

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let folder_id_for_request = folder_id.clone();
            let workspace_for_request = folder.workspace_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.thread_folder_delete(ThreadFolderDeleteParams {
                            workspace_id: workspace_for_request,
                            folder_id: folder_id_for_request,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    if let Err(error) = result {
                        warn!(
                            error = %format!("{error:#}"),
                            "failed to delete thread folder"
                        );
                    }

                    view.refresh_thread_list(cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn open_root_agents_doc_editor_from_sidebar(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(workspace_id) = self.sidebar_workspace_id() else {
            return;
        };

        self.open_agents_doc_editor(
            ThreadAgentsDocEditorScope::Root { workspace_id },
            window,
            cx,
        );
    }

    pub(super) fn open_folder_agents_doc_editor_from_sidebar(
        &mut self,
        folder_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(folder) = self.thread_folder(folder_id.as_str()).cloned() else {
            return;
        };
        let Some(active_workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        if folder.workspace_id != active_workspace_id {
            return;
        }

        self.open_agents_doc_editor(
            ThreadAgentsDocEditorScope::Folder {
                workspace_id: folder.workspace_id,
                folder_id,
            },
            window,
            cx,
        );
    }

    pub(super) fn remove_folder_agents_doc_override_from_sidebar(
        &mut self,
        folder_id: String,
        cx: &mut Context<Self>,
    ) {
        self.remove_agents_doc_override_from_sidebar(Some(folder_id), cx);
    }

    pub(super) fn remove_root_agents_doc_override_from_sidebar(&mut self, cx: &mut Context<Self>) {
        self.remove_agents_doc_override_from_sidebar(None, cx);
    }

    fn remove_agents_doc_override_from_sidebar(
        &mut self,
        folder_id: Option<String>,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        let Some(summary) = self
            .thread_agents_doc_summary_for_workspace(folder_id.as_deref(), workspace_id.as_str())
            .cloned()
        else {
            return;
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let params = agents_doc_archive_params_for_summary(&summary, folder_id.as_deref());

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.thread_agents_doc_archive(params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            if response.archived {
                                view.remove_agents_doc_from_sidebar_state(
                                    summary.workspace_id.as_str(),
                                    folder_id.as_deref(),
                                    cx,
                                );
                            }
                        }
                        Err(error) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to remove AGENTS.md override"
                            );
                        }
                    }

                    view.refresh_thread_list(cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn remove_agents_doc_from_sidebar_state(
        &mut self,
        workspace_id: &str,
        folder_id: Option<&str>,
        cx: &mut Context<Self>,
    ) {
        self.thread_agents_doc_summaries
            .remove(&ThreadAgentsDocSummaryKey::from_folder_id(folder_id));

        let active_editor_matches =
            self.active_agents_doc_editor_scope
                .as_ref()
                .is_some_and(|scope| {
                    scope.workspace_id() == workspace_id && scope.folder_id() == folder_id
                });

        if active_editor_matches {
            self.active_agents_doc_editor_scope = None;
            self.agents_doc_editor = None;
            self.set_thread_tree_selected_node_id(None);
            if self.main_content_view == MainContentView::AgentsDoc {
                self.set_main_content_view(MainContentView::Threads, cx);
                return;
            }
        }

        self.rebuild_sidebar_tree_state(cx);
    }

    pub(super) fn handle_sidebar_drop_to_folder(
        &mut self,
        payload: SidebarTreeDragPayload,
        target_folder_id: String,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }

        match payload.item {
            SidebarTreeDragItem::Thread { thread_id } => {
                self.request_thread_move(thread_id, Some(target_folder_id), cx);
            }
            SidebarTreeDragItem::Folder { folder_id } => {
                if folder_id == target_folder_id {
                    return;
                }
                self.request_folder_move(folder_id, Some(target_folder_id), cx);
            }
        }
    }

    pub(super) fn handle_sidebar_drop_to_root(
        &mut self,
        payload: SidebarTreeDragPayload,
        cx: &mut Context<Self>,
    ) {
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }

        match payload.item {
            SidebarTreeDragItem::Thread { thread_id } => {
                self.request_thread_move(thread_id, None, cx);
            }
            SidebarTreeDragItem::Folder { folder_id } => {
                self.request_folder_move(folder_id, None, cx);
            }
        }
    }

    fn request_thread_move(
        &mut self,
        thread_id: String,
        folder_id: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        if self.thread_workspace_id(thread_id.as_str()) != Some(workspace_id.as_str()) {
            return;
        }

        if let Some(folder_id) = folder_id.as_deref() {
            let Some(folder) = self.thread_folder(folder_id) else {
                return;
            };
            if folder.workspace_id != workspace_id {
                return;
            }
        }

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let thread_id_for_request = thread_id.clone();
            let folder_id_for_request = folder_id.clone();
            let workspace_id_for_request = workspace_id.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.thread_move(ThreadMoveParams {
                            workspace_id: workspace_id_for_request,
                            thread_id: thread_id_for_request,
                            folder_id: folder_id_for_request,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    if let Err(error) = result {
                        warn!(
                            error = %format!("{error:#}"),
                            "failed to move thread in tree"
                        );
                    }

                    view.refresh_thread_list(cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn request_folder_move(
        &mut self,
        folder_id: String,
        parent_folder_id: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(folder) = self.thread_folder(folder_id.as_str()).cloned() else {
            return;
        };
        let Some(active_workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        if folder.workspace_id != active_workspace_id {
            return;
        }

        if let Some(parent_folder_id) = parent_folder_id.as_deref() {
            let Some(parent_folder) = self.thread_folder(parent_folder_id) else {
                return;
            };
            if parent_folder.workspace_id != folder.workspace_id {
                return;
            }
        }

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let folder_id_for_request = folder_id.clone();
            let parent_for_request = parent_folder_id.clone();
            let workspace_for_request = folder.workspace_id.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.thread_folder_move(ThreadFolderMoveParams {
                            workspace_id: workspace_for_request,
                            folder_id: folder_id_for_request,
                            parent_folder_id: parent_for_request,
                        })
                    })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    if let Err(error) = result {
                        warn!(
                            error = %format!("{error:#}"),
                            "failed to move thread folder"
                        );
                    }

                    view.refresh_thread_list(cx);
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn sidebar_workspace_id(&self) -> Option<String> {
        self.active_workspace_id().map(str::to_owned).or_else(|| {
            self.gateway
                .runtime
                .as_ref()
                .and_then(crate::gateway::GatewayRuntime::active_workspace_id)
                .map(str::to_owned)
        })
    }

    fn next_new_folder_name(&self, workspace_id: &str) -> String {
        let base_name = t!("sidebar.folder.new").to_string();
        if !self
            .thread_folders
            .values()
            .any(|folder| folder.workspace_id == workspace_id && folder.name == base_name)
        {
            return base_name;
        }

        for index in 2..10_000 {
            let name = format!("{base_name} {index}");
            if !self
                .thread_folders
                .values()
                .any(|folder| folder.workspace_id == workspace_id && folder.name == name)
            {
                return name;
            }
        }

        base_name
    }
}

fn agents_doc_archive_params_for_summary(
    summary: &ThreadAgentsDocSummary,
    folder_id: Option<&str>,
) -> ThreadAgentsDocArchiveParams {
    ThreadAgentsDocArchiveParams {
        workspace_id: summary.workspace_id.clone(),
        folder_id: folder_id.map(str::to_owned),
        expected_version: Some(summary.version),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::ThreadAgentsDocStatus;

    fn summary(folder_id: Option<&str>) -> ThreadAgentsDocSummary {
        ThreadAgentsDocSummary {
            id: "agd_1".to_owned(),
            workspace_id: "ws_1".to_owned(),
            folder_id: folder_id.map(str::to_owned),
            status: ThreadAgentsDocStatus::Active,
            content_sha256: "sha".to_owned(),
            version: 7,
            char_count: 4,
            updated_at: 1_700_000_000,
        }
    }

    #[::core::prelude::v1::test]
    fn agents_doc_conflict_remove_override_targets_selected_folder_id() {
        let params = agents_doc_archive_params_for_summary(&summary(Some("fld_1")), Some("fld_1"));

        assert_eq!(params.workspace_id, "ws_1");
        assert_eq!(params.folder_id.as_deref(), Some("fld_1"));
        assert_eq!(params.expected_version, Some(7));
    }

    #[::core::prelude::v1::test]
    fn agents_doc_conflict_remove_root_override_has_no_folder_id() {
        let params = agents_doc_archive_params_for_summary(&summary(None), None);

        assert_eq!(params.workspace_id, "ws_1");
        assert_eq!(params.folder_id, None);
        assert_eq!(params.expected_version, Some(7));
    }
}
