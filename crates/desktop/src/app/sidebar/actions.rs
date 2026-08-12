use crate::app::{
    root::{GatewayConnectionState, MainContentView, PioneerDesktop, ThreadAgentsDocEditorScope},
    sidebar::{SidebarTreeDragItem, SidebarTreeDragPayload},
};
use gpui::{prelude::*, *};
use pioneer_client::agents_doc::scope as agents_doc_scope;
use pioneer_client::threads::{scope as thread_scope, tree as thread_tree};
use pioneer_client::workspaces::selectors as workspace_selectors;
use pioneer_protocol::{ThreadOriginKind, ThreadVisibility};
use tracing::warn;

impl PioneerDesktop {
    pub(super) fn open_or_create_new_thread_from_sidebar(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(visibility) = thread_scope::thread_create_visibility_plan(
            self.gateway
                .capability_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.workspace.as_ref())
                .map(|snapshot| &snapshot.capabilities),
            ThreadOriginKind::Collaborative,
        )
        .default_visibility
        else {
            return;
        };

        self.open_or_create_new_thread_with_visibility(visibility, window, cx);
    }

    fn open_or_create_new_thread_with_visibility(
        &mut self,
        visibility: ThreadVisibility,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.pending_thread_create_visibility = visibility;
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
        if !self
            .principal_presentation_capabilities()
            .can_manage_workspace
        {
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(workspace_id) = self.sidebar_workspace_id() else {
            return;
        };

        let create_params = match thread_tree::plan_thread_folder_create(
            &self.thread_folders,
            Some(workspace_id.as_str()),
            t!("sidebar.folder.new").as_ref(),
        ) {
            Ok(params) => params,
            Err(_) => return,
        };
        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.thread_folder_create(create_params) })
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
        if !self
            .principal_presentation_capabilities()
            .can_manage_workspace
        {
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(active_workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        let rename_request = match thread_tree::plan_thread_folder_rename(
            &self.thread_folders,
            &self.thread_placements,
            Some(active_workspace_id.as_str()),
            folder_id.as_str(),
            new_name.as_str(),
        ) {
            thread_tree::ThreadFolderRenamePlan::Request(request) => request,
            thread_tree::ThreadFolderRenamePlan::Skip(_) => return,
        };

        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let rename_request = rename_request.clone();

            async move {
                let result = cx
                    .background_spawn(async move {
                        let created =
                            ws_sender.thread_folder_create(rename_request.create.clone())?;
                        let new_folder_id = created.folder.id;
                        let follow_up = thread_tree::thread_folder_rename_follow_up_params(
                            &rename_request,
                            new_folder_id,
                        );

                        for params in follow_up.folder_moves {
                            ws_sender.thread_folder_move(params)?;
                        }

                        for params in follow_up.thread_moves {
                            ws_sender.thread_move(params)?;
                        }

                        ws_sender.thread_folder_delete(follow_up.delete)?;

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

    pub(super) fn rename_thread_from_sidebar(
        &mut self,
        thread_id: String,
        new_name: String,
        cx: &mut Context<Self>,
    ) {
        if !self.can_manage_thread_presentation(thread_id.as_str()) {
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        let update_params = match thread_tree::plan_thread_rename(
            &self.thread_coordinators,
            Some(workspace_id.as_str()),
            thread_id.as_str(),
            new_name.as_str(),
        ) {
            thread_tree::ThreadRenamePlan::Request(params) => params,
            thread_tree::ThreadRenamePlan::Skip(_) => return,
        };

        let ws_sender = self.gateway.ws_command_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let update_params = update_params.clone();

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.thread_update(update_params) })
                    .await;

                let _ = this.update(&mut cx, |view, cx| {
                    if view.gateway.ws_connection_id != Some(connection_id) {
                        return;
                    }

                    match result {
                        Ok(response) => {
                            let reduction = thread_tree::reduce_thread_rename_success(response);
                            view.upsert_thread_snapshot(reduction.thread);
                            view.upsert_thread_for_workspace(
                                reduction.thread_id.as_str(),
                                reduction.workspace_id.as_str(),
                            );
                            view.rebuild_sidebar_tree_state(cx);
                        }
                        Err(error) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "failed to rename thread"
                            );
                        }
                    }

                    cx.notify();
                });
            }
        })
        .detach();
    }

    pub(super) fn delete_folder_from_sidebar(&mut self, folder_id: String, cx: &mut Context<Self>) {
        if !self
            .principal_presentation_capabilities()
            .can_manage_workspace
        {
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }
        let Some(connection_id) = self.gateway.ws_connection_id else {
            return;
        };
        let Some(active_workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        let delete_params = match thread_tree::plan_thread_folder_delete(
            &self.thread_folders,
            Some(active_workspace_id.as_str()),
            folder_id.as_str(),
        ) {
            thread_tree::ThreadFolderDeletePlan::Request(params) => params,
            thread_tree::ThreadFolderDeletePlan::Skip(_) => return,
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let delete_params = delete_params.clone();

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.thread_folder_delete(delete_params) })
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
        if !self
            .principal_presentation_capabilities()
            .can_manage_workspace
        {
            return;
        }
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
        if !self
            .principal_presentation_capabilities()
            .can_manage_workspace
        {
            return;
        }
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
        if !self
            .principal_presentation_capabilities()
            .can_manage_workspace
        {
            return;
        }
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
            let params = agents_doc_scope::agents_doc_archive_params_for_summary(
                &summary,
                folder_id.as_deref(),
            );

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
        agents_doc_scope::remove_thread_agents_doc_summary(
            &mut self.thread_agents_doc_summaries,
            folder_id,
        );

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
        if !self
            .principal_presentation_capabilities()
            .can_manage_workspace
        {
            return;
        }
        if self.gateway.connection_state != GatewayConnectionState::Connected {
            return;
        }

        match payload.item {
            SidebarTreeDragItem::Thread { thread_id } => {
                self.request_thread_move(thread_id, Some(target_folder_id), cx);
            }
            SidebarTreeDragItem::Folder { folder_id } => {
                let workspace_id = self.sidebar_workspace_id();
                let can_drop = thread_tree::can_drop_sidebar_tree_item_on_folder(
                    &self.thread_folders,
                    workspace_id.as_deref(),
                    thread_tree::SidebarTreeDragItemRef::Folder {
                        folder_id: folder_id.as_str(),
                    },
                    target_folder_id.as_str(),
                );
                if !can_drop {
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
        if !self
            .principal_presentation_capabilities()
            .can_manage_workspace
        {
            return;
        }
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
        let move_params = match thread_tree::plan_thread_move(
            &self.thread_coordinators,
            &self.thread_folders,
            Some(workspace_id.as_str()),
            thread_id.as_str(),
            folder_id.as_deref(),
        ) {
            thread_tree::ThreadMovePlan::Request(params) => params,
            thread_tree::ThreadMovePlan::Skip(_) => return,
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let move_params = move_params.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.thread_move(move_params) })
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
        let Some(active_workspace_id) = self.sidebar_workspace_id() else {
            return;
        };
        let move_params = match thread_tree::plan_thread_folder_move(
            &self.thread_folders,
            Some(active_workspace_id.as_str()),
            folder_id.as_str(),
            parent_folder_id.as_deref(),
        ) {
            thread_tree::ThreadFolderMovePlan::Request(params) => params,
            thread_tree::ThreadFolderMovePlan::Skip(_) => return,
        };

        let ws_sender = self.gateway.ws_command_sender.clone();
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            let move_params = move_params.clone();

            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.thread_folder_move(move_params) })
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
        let runtime_workspace_id = self
            .gateway
            .runtime
            .as_ref()
            .and_then(crate::gateway::GatewayRuntime::active_workspace_id);
        workspace_selectors::resolve_workspace_scope(
            self.active_workspace_id(),
            None,
            runtime_workspace_id,
        )
    }
}
