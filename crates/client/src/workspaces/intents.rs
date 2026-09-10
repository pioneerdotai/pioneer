//! Workspace commands share one domain owner across pointer, keyboard and FFI callers.
use crate::{core::ClientCore, threads::tree};
use serde::{Deserialize, Serialize};
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceIntent {
    CreateWorkspace {
        name: String,
    },
    RenameWorkspace {
        workspace_id: String,
        name: String,
    },
    NewThread {
        workspace_id: String,
    },
    SelectThread {
        workspace_id: String,
        thread_id: String,
    },
    SelectWorkspace {
        workspace_id: String,
    },
    RenameThread {
        workspace_id: String,
        thread_id: String,
        name: String,
    },
    DeleteThread {
        workspace_id: String,
        thread_id: String,
    },
    MoveThread {
        workspace_id: String,
        thread_id: String,
        folder_id: Option<String>,
    },
    CreateFolder {
        workspace_id: String,
        name: String,
    },
    RenameFolder {
        workspace_id: String,
        folder_id: String,
        name: String,
    },
    DeleteFolder {
        workspace_id: String,
        folder_id: String,
    },
    MoveFolder {
        workspace_id: String,
        folder_id: String,
        parent_folder_id: Option<String>,
    },
    RemoveAgentsDocument {
        workspace_id: String,
        folder_id: Option<String>,
    },
}
impl WorkspaceIntent {
    pub fn workspace_id(&self) -> Option<&str> {
        match self {
            Self::CreateWorkspace { .. } => None,
            Self::RenameWorkspace { workspace_id, .. }
            | Self::NewThread { workspace_id }
            | Self::SelectThread { workspace_id, .. }
            | Self::SelectWorkspace { workspace_id }
            | Self::RenameThread { workspace_id, .. }
            | Self::DeleteThread { workspace_id, .. }
            | Self::MoveThread { workspace_id, .. }
            | Self::CreateFolder { workspace_id, .. }
            | Self::RenameFolder { workspace_id, .. }
            | Self::DeleteFolder { workspace_id, .. }
            | Self::MoveFolder { workspace_id, .. }
            | Self::RemoveAgentsDocument { workspace_id, .. } => Some(workspace_id),
        }
    }
}
impl ClientCore {
    pub fn update_directory_thread(
        &self,
        transport: &impl crate::rpc::JsonRpcRequestTransport,
        params: pioneer_protocol::ThreadUpdateParams,
    ) -> anyhow::Result<pioneer_protocol::ThreadUpdateResponse> {
        let connection = self.gateway_http_generation();
        let authorization = self.authorization_connection_generation();
        let authorization_revision = self.authorization_revision();
        let generation = self.workspace_operation_generation(&params.workspace_id);
        let response =
            crate::transport::ws::command_sender::thread_update(transport, params.clone())?;
        anyhow::ensure!(
            !self.is_stopped()
                && self.gateway_http_generation() == connection
                && self.authorization_connection_generation() == authorization
                && self.authorization_revision() == authorization_revision
                && self.workspace_operation_generation(&params.workspace_id) == generation
                && response.thread.id == params.thread_id
                && response.thread.workspace_id == params.workspace_id,
            "Thread update completion is stale or has a different scope"
        );
        self.upsert_thread(response.thread.clone());
        Ok(response)
    }
    pub fn execute_workspace_intent(&self, intent: WorkspaceIntent) -> anyhow::Result<()> {
        if let WorkspaceIntent::CreateWorkspace { name } = intent {
            if let super::commands::WorkspaceCreateResult::Created { reduction } =
                self.create_and_select_workspace(name)?
            {
                let workspace = reduction.switch_workspace_id;
                self.load_selected_workspace_directory(&workspace);
            }
            return Ok(());
        }
        if let WorkspaceIntent::RenameWorkspace { workspace_id, name } = intent {
            self.rename_workspace(workspace_id, name)?;
            return Ok(());
        }
        let workspace = intent
            .workspace_id()
            .expect("scoped workspace command")
            .to_owned();
        anyhow::ensure!(!self.is_stopped(), "Workspace capability is closed");
        if let WorkspaceIntent::SelectWorkspace { workspace_id } = intent {
            self.switch_workspace(workspace_id.clone())?;
            self.load_selected_workspace_directory(&workspace_id);
            return Ok(());
        }
        let connection = self.gateway_http_generation();
        let authorization_generation = self.authorization_connection_generation();
        let authorization_revision = self.authorization_revision();
        let operation_generation = self.workspace_operation_generation(&workspace);
        let still_current = || {
            !self.is_stopped()
                && self.gateway_http_generation() == connection
                && self.authorization_connection_generation() == authorization_generation
                && self.authorization_revision() == authorization_revision
                && self.workspace_operation_generation(&workspace) == operation_generation
        };
        let authorization = self
            .authorization_snapshot(Some(&workspace), None)
            .ok_or_else(|| anyhow::anyhow!("Workspace authorization unavailable"))?;
        let capabilities =
            crate::authorization::principal_presentation_capabilities(&authorization);
        let snapshot = self
            .workspace_tree(&workspace)
            .ok_or_else(|| anyhow::anyhow!("Workspace directory unavailable"))?;
        let tree = snapshot.snapshot();
        let sender = self.transport_runtime().ws_command_sender();
        let ensure_thread = |id: &str, manage: bool| -> anyhow::Result<()> {
            anyhow::ensure!(
                tree.threads_by_id
                    .get(id)
                    .is_some_and(|thread| thread.workspace_id == workspace),
                "Thread scope mismatch"
            );
            if manage {
                let scoped = self.authorization_snapshot(Some(&workspace), Some(id));
                anyhow::ensure!(
                    capabilities.can_manage_all_threads
                        || scoped
                            .and_then(|s| s.thread)
                            .is_some_and(|t| t.capabilities.can_manage),
                    "Thread action unavailable"
                );
            }
            Ok(())
        };
        let folders = tree.folders_by_id.clone();
        let placements = tree.placements_by_thread_id.clone();
        match intent {
            WorkspaceIntent::NewThread { .. } => {
                let visibility = crate::threads::scope::thread_create_visibility_plan(
                    authorization.workspace.as_ref().map(|w| &w.capabilities),
                    pioneer_protocol::ThreadOriginKind::Collaborative,
                )
                .default_visibility
                .ok_or_else(|| anyhow::anyhow!("Thread creation unavailable"))?;
                let draft = self
                    .navigation_snapshot()
                    .draft(&workspace)
                    .map(str::to_owned);
                if draft.is_none() {
                    self.create_workspace_thread_draft(&sender, &workspace, visibility)?;
                }
            }
            WorkspaceIntent::SelectThread { thread_id, .. } => {
                ensure_thread(&thread_id, false)?;
                self.open_workspace_thread(workspace, Some(thread_id), None);
                return Ok(());
            }
            WorkspaceIntent::RenameThread {
                thread_id, name, ..
            } => {
                ensure_thread(&thread_id, true)?;
                let name = name.trim();
                if name.is_empty() || tree.threads_by_id[&thread_id].name.as_deref() == Some(name) {
                    return Ok(());
                }
                let result = self.update_directory_thread(
                    &sender,
                    pioneer_protocol::ThreadUpdateParams {
                        workspace_id: workspace.clone(),
                        thread_id: thread_id.clone(),
                        name: Some(name.to_owned()),
                        visibility: None,
                        archived: None,
                    },
                )?;
                anyhow::ensure!(
                    still_current()
                        && result.thread.workspace_id == workspace
                        && result.thread.id == thread_id,
                    "Thread completion is stale"
                );
                return Ok(());
            }
            WorkspaceIntent::DeleteThread { thread_id, .. } => {
                ensure_thread(&thread_id, true)?;
                let result = self.update_directory_thread(
                    &sender,
                    pioneer_protocol::ThreadUpdateParams {
                        workspace_id: workspace.clone(),
                        thread_id: thread_id.clone(),
                        name: None,
                        visibility: None,
                        archived: Some(true),
                    },
                )?;
                anyhow::ensure!(
                    still_current()
                        && result.thread.workspace_id == workspace
                        && result.thread.id == thread_id,
                    "Thread completion is stale"
                );
                return Ok(());
            }
            WorkspaceIntent::MoveThread {
                thread_id,
                folder_id,
                ..
            } => {
                ensure_thread(&thread_id, true)?;
                anyhow::ensure!(
                    capabilities.can_manage_workspace,
                    "Workspace action unavailable"
                );
                if let Some(folder) = &folder_id {
                    anyhow::ensure!(
                        folders
                            .get(folder)
                            .is_some_and(|f| f.workspace_id == workspace),
                        "Folder scope mismatch"
                    );
                }
                if placements
                    .get(&thread_id)
                    .is_some_and(|p| p.folder_id == folder_id)
                {
                    return Ok(());
                }
                sender.thread_move(pioneer_protocol::ThreadMoveParams {
                    workspace_id: workspace.clone(),
                    thread_id,
                    folder_id,
                })?;
            }
            WorkspaceIntent::CreateFolder { name, .. } => {
                anyhow::ensure!(
                    capabilities.can_manage_workspace,
                    "Workspace action unavailable"
                );
                let plan = tree::plan_thread_folder_create(&folders, Some(&workspace), &name)
                    .map_err(|_| anyhow::anyhow!("Folder creation unavailable"))?;
                sender.thread_folder_create(plan)?;
            }
            WorkspaceIntent::RenameFolder {
                folder_id, name, ..
            } => {
                anyhow::ensure!(
                    capabilities.can_manage_workspace,
                    "Workspace action unavailable"
                );
                let tree::ThreadFolderRenamePlan::Request(plan) = tree::plan_thread_folder_rename(
                    &folders,
                    &placements,
                    Some(&workspace),
                    &folder_id,
                    &name,
                ) else {
                    return Ok(());
                };
                let created = sender.thread_folder_create(plan.create.clone())?;
                anyhow::ensure!(
                    still_current() && created.folder.workspace_id == workspace,
                    "Folder completion is stale"
                );
                let follow = tree::thread_folder_rename_follow_up_params(&plan, created.folder.id);
                for params in follow.folder_moves {
                    anyhow::ensure!(still_current(), "Folder operation cancelled");
                    sender.thread_folder_move(params)?;
                }
                for params in follow.thread_moves {
                    anyhow::ensure!(still_current(), "Folder operation cancelled");
                    sender.thread_move(params)?;
                }
                anyhow::ensure!(still_current(), "Folder operation cancelled");
                sender.thread_folder_delete(follow.delete)?;
            }
            WorkspaceIntent::DeleteFolder { folder_id, .. } => {
                anyhow::ensure!(
                    capabilities.can_manage_workspace,
                    "Workspace action unavailable"
                );
                let tree::ThreadFolderDeletePlan::Request(plan) =
                    tree::plan_thread_folder_delete(&folders, Some(&workspace), &folder_id)
                else {
                    return Ok(());
                };
                sender.thread_folder_delete(plan)?;
            }
            WorkspaceIntent::MoveFolder {
                folder_id,
                parent_folder_id,
                ..
            } => {
                anyhow::ensure!(
                    capabilities.can_manage_workspace,
                    "Workspace action unavailable"
                );
                let tree::ThreadFolderMovePlan::Request(plan) = tree::plan_thread_folder_move(
                    &folders,
                    Some(&workspace),
                    &folder_id,
                    parent_folder_id.as_deref(),
                ) else {
                    return Ok(());
                };
                sender.thread_folder_move(plan)?;
            }
            WorkspaceIntent::RemoveAgentsDocument { folder_id, .. } => {
                anyhow::ensure!(
                    capabilities.can_manage_workspace,
                    "Workspace action unavailable"
                );
                let key = folder_id.as_deref().unwrap_or("__root__");
                let Some(summary) = tree.agents_doc_summaries_by_folder_key.get(key) else {
                    return Ok(());
                };
                sender.thread_agents_doc_archive(
                    crate::agents_doc::scope::agents_doc_archive_params_for_summary(
                        summary,
                        folder_id.as_deref(),
                    ),
                )?;
            }
            WorkspaceIntent::SelectWorkspace { .. }
            | WorkspaceIntent::CreateWorkspace { .. }
            | WorkspaceIntent::RenameWorkspace { .. } => unreachable!(),
        }
        anyhow::ensure!(still_current(), "Workspace command is stale");
        self.refresh_workspace_tree(&workspace)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct UpdateTransport {
        response: pioneer_protocol::Thread,
    }
    impl crate::rpc::JsonRpcRequestTransport for UpdateTransport {
        fn send_json_rpc_request(
            &self,
            _: String,
            request: String,
            reply: crate::rpc::JsonRpcResponseSender,
        ) -> Result<(), String> {
            let request: serde_json::Value = serde_json::from_str(&request).unwrap();
            assert_eq!(
                request["method"],
                pioneer_protocol::constants::methods::THREAD_UPDATE
            );
            reply
                .send(Ok(serde_json::json!({"thread":self.response})))
                .unwrap();
            Ok(())
        }
    }
    #[test]
    fn mutation_response_updates_directory_without_waiting_for_a_ws_hint() {
        let core = ClientCore::new();
        let thread: pioneer_protocol::Thread = serde_json::from_value(serde_json::json!({"id":"t","workspace_id":"w","name":"before","preview":"","mode":"Chat","model":"m","model_provider":"p","created_at":1,"updated_at":1,"status":"Idle","turns":[]})).unwrap();
        core.upsert_thread(thread.clone());
        let params = pioneer_protocol::ThreadUpdateParams {
            workspace_id: "w".into(),
            thread_id: "t".into(),
            name: Some("after".into()),
            visibility: None,
            archived: None,
        };
        let mut changed = thread;
        changed.name = Some("after".into());
        changed.updated_at = 2;
        core.update_directory_thread(
            &UpdateTransport {
                response: changed.clone(),
            },
            params.clone(),
        )
        .unwrap();
        assert_eq!(
            core.workspace_tree("w").unwrap().snapshot().threads_by_id["t"]
                .name
                .as_deref(),
            Some("after")
        );
        let before = core.workspace_tree("w").unwrap();
        changed.workspace_id = "wrong".into();
        assert!(
            core.update_directory_thread(&UpdateTransport { response: changed }, params)
                .is_err()
        );
        assert!(std::sync::Arc::ptr_eq(
            &before,
            &core.workspace_tree("w").unwrap()
        ));
    }
}
