use crate::{
    file_opener::{FileOpenerId, available_or_file_manager, is_file_opener_available},
    settings::{self, FileOpenerThreadScope, FileOpenerWorkspaceScope},
};
use gpui_kit::{Context, prelude::*};
use tracing::warn;

use super::PioneerDesktop;

impl PioneerDesktop {
    pub(in crate::app) fn active_workspace_file_opener(&self, cx: &Context<Self>) -> FileOpenerId {
        self.active_file_opener_workspace_scope()
            .map(|scope| available_or_file_manager(settings::workspace_file_opener(cx, &scope)))
            .unwrap_or_default()
    }

    pub(in crate::app) fn effective_file_opener_for_thread(
        &self,
        thread_id: &str,
        cx: &Context<Self>,
    ) -> FileOpenerId {
        let Some(scope) = self.file_opener_thread_scope(thread_id) else {
            return self.active_workspace_file_opener(cx);
        };
        let workspace_default =
            available_or_file_manager(settings::workspace_file_opener(cx, &scope.workspace));
        settings::thread_file_opener_override(cx, &scope)
            .filter(|opener| is_file_opener_available(*opener))
            .unwrap_or(workspace_default)
    }

    pub(in crate::app) fn workspace_file_opener_for_thread(
        &self,
        thread_id: &str,
        cx: &Context<Self>,
    ) -> FileOpenerId {
        self.file_opener_thread_scope(thread_id)
            .map(|scope| {
                available_or_file_manager(settings::workspace_file_opener(cx, &scope.workspace))
            })
            .unwrap_or_else(|| self.active_workspace_file_opener(cx))
    }

    pub(in crate::app) fn active_thread_file_opener(&self, cx: &Context<Self>) -> FileOpenerId {
        self.current_active_thread_id()
            .map(|thread_id| self.effective_file_opener_for_thread(thread_id, cx))
            .unwrap_or_else(|| self.active_workspace_file_opener(cx))
    }

    pub(in crate::app) fn thread_file_opener_override(
        &self,
        thread_id: &str,
        cx: &Context<Self>,
    ) -> Option<FileOpenerId> {
        let scope = self.file_opener_thread_scope(thread_id)?;
        settings::thread_file_opener_override(cx, &scope)
            .filter(|opener| is_file_opener_available(*opener))
    }

    pub(in crate::app) fn apply_workspace_file_opener(
        &mut self,
        opener: FileOpenerId,
        cx: &mut Context<Self>,
    ) {
        let Some(scope) = self.active_file_opener_workspace_scope() else {
            return;
        };
        if let Err(error) = settings::set_workspace_file_opener(cx, &scope, opener) {
            warn!(
                error = %format!("{error:#}"),
                workspace_id = scope.workspace_id,
                "failed to save workspace file opener"
            );
        }
    }

    pub(in crate::app) fn apply_thread_file_opener_override(
        &mut self,
        thread_id: &str,
        opener: Option<FileOpenerId>,
        cx: &mut Context<Self>,
    ) {
        let Some(scope) = self.file_opener_thread_scope(thread_id) else {
            return;
        };
        if let Err(error) = settings::set_thread_file_opener_override(cx, &scope, opener) {
            warn!(
                error = %format!("{error:#}"),
                thread_id,
                workspace_id = scope.workspace.workspace_id,
                "failed to save thread file opener override"
            );
        }
    }

    fn active_file_opener_workspace_scope(&self) -> Option<FileOpenerWorkspaceScope> {
        let workspace_id = self.active_workspace_id()?;
        self.file_opener_workspace_scope(workspace_id)
    }

    fn file_opener_thread_scope(&self, thread_id: &str) -> Option<FileOpenerThreadScope> {
        let workspace_id = self.thread_workspace_id(thread_id)?;
        Some(FileOpenerThreadScope {
            workspace: self.file_opener_workspace_scope(&workspace_id)?,
            thread_id: thread_id.to_owned(),
        })
    }

    fn file_opener_workspace_scope(&self, workspace_id: &str) -> Option<FileOpenerWorkspaceScope> {
        let principal_id = self
            .gateway
            .current_auth
            .as_ref()?
            .principal
            .id
            .as_str()
            .to_owned();
        let gateway_id = self
            .gateway
            .client_runtime
            .client_core()
            .gateway_registry()
            .as_ref()?
            .active_gateway_id()?
            .to_owned();
        Some(FileOpenerWorkspaceScope {
            principal_id,
            gateway_id,
            workspace_id: workspace_id.to_owned(),
        })
    }
}
