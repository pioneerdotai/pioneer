use crate::store::conversations::thread_writes;
use crate::{
    CliRuntimeThreadBindingRecord, CrudStore, NewThreadMembership, PersistedThreadAccessClass,
    PrivateThreadParticipantMutation, ResolvedThreadAgentsDocRecord, ThreadAgentsDocRecord,
    ThreadAgentsDocRevisionRecord, ThreadAgentsDocSaveReason, ThreadAgentsDocScopeContext,
    ThreadAgentsDocSummaryRecord, ThreadManagementChange, ThreadReadPosition,
    find_workspace_membership, insert_thread_membership,
};
use anyhow::{Context, Result};
use pioneer_crud_conversations::thread_metadata;
use pioneer_crud_conversations::{thread_agents_doc, thread_tree};
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_postgres_pools::run_database_attempt;
use pioneer_protocol::{
    GatewayId, PersistedActorRef, PrincipalId, Thread, ThreadFolder, ThreadPlacement,
};
use sea_orm::TransactionTrait;

impl CrudStore {
    /// Persists a thread through the normal write path.
    ///
    /// Historical actorless rows are handled only by the projector's explicit
    /// legacy replay seam; every normal insert must carry a durable creator.
    pub async fn upsert_thread_model(
        &self,
        thread_model: &Thread,
        creator: PersistedActorRef,
    ) -> Result<()> {
        let task_change = self.connection.work_signal("tasks.lifecycle").changing();
        let task_before =
            thread_metadata::find_thread_workspace(&self.connection, &thread_model.id).await?;
        let task_requested = thread_model.workspace_id.clone();
        let task_result = async {
            let thread_model = thread_model.clone();
            let creator = creator.clone();
            let created_at = unix_to_datetime(thread_model.created_at);
            let updated_at = unix_to_datetime(thread_model.updated_at);
            self.retry_write_on_conflict(|| {
                let thread_model = thread_model.clone();
                let creator = creator.clone();
                async move {
                    let tx = self.connection.begin().await?;
                    thread_writes::upsert_thread_with_creator(
                        &tx,
                        &thread_model,
                        &creator,
                        created_at,
                        updated_at,
                    )
                    .await?;
                    tx.commit().await?;
                    Ok(())
                }
            })
            .await
        }
        .await;
        if task_result.is_ok() {
            task_change.completed(task_before.as_ref() != Some(&task_requested));
        }
        task_result
    }

    /// Creates a new ordinary user thread and, for a Member, its creator
    /// membership in one transaction. No caller-visible state exists if either
    /// insert fails.
    pub async fn create_member_private_thread(
        &self,
        thread_model: &Thread,
        gateway_id: &GatewayId,
        creator_principal_id: &PrincipalId,
        creator: PersistedActorRef,
    ) -> Result<()> {
        let thread_model = thread_model.clone();
        let gateway_id = gateway_id.clone();
        let creator_principal_id = creator_principal_id.clone();
        let created_at = unix_to_datetime(thread_model.created_at);
        let updated_at = unix_to_datetime(thread_model.updated_at);
        self.retry_write_on_conflict(|| {
            let thread_model = thread_model.clone();
            let gateway_id = gateway_id.clone();
            let creator_principal_id = creator_principal_id.clone();
            let creator = creator.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin member thread creation transaction")?;
                thread_writes::insert_user_thread_with_creator(
                    &transaction,
                    &thread_model,
                    &creator,
                    PersistedThreadAccessClass::Private,
                    created_at,
                    updated_at,
                )
                .await?;
                insert_thread_membership(
                    &transaction,
                    &NewThreadMembership {
                        gateway_id,
                        principal_id: creator_principal_id,
                        workspace_id: thread_model.workspace_id.clone(),
                        thread_id: thread_model.id.clone(),
                        added_by: creator,
                        now: updated_at,
                    },
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit member thread creation transaction")
            }
        })
        .await
    }

    /// Creates a new ordinary user thread for the absolute Superuser. The
    /// access class is explicit but cannot be `internal`.
    pub async fn create_superuser_thread(
        &self,
        thread_model: &Thread,
        creator: PersistedActorRef,
        access_class: PersistedThreadAccessClass,
    ) -> Result<()> {
        let thread_model = thread_model.clone();
        let created_at = unix_to_datetime(thread_model.created_at);
        let updated_at = unix_to_datetime(thread_model.updated_at);
        self.retry_write_on_conflict(|| {
            let thread_model = thread_model.clone();
            let creator = creator.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin superuser thread creation transaction")?;
                thread_writes::insert_user_thread_with_creator(
                    &transaction,
                    &thread_model,
                    &creator,
                    access_class,
                    created_at,
                    updated_at,
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit superuser thread creation transaction")
            }
        })
        .await
    }

    /// Applies a creator/Superuser thread-management patch under one exact
    /// transaction. Passing a Member principal re-checks both its current
    /// workspace membership and persisted creator identity inside the write.
    pub async fn update_user_thread_management(
        &self,
        workspace_id: &str,
        thread_id: &str,
        member_principal_id: Option<&PrincipalId>,
        name: Option<&str>,
        access_class: Option<PersistedThreadAccessClass>,
        archived: Option<bool>,
    ) -> Result<Option<ThreadManagementChange>> {
        let workspace_id = workspace_id.to_owned();
        let thread_id = thread_id.to_owned();
        let member_principal_id = member_principal_id.cloned();
        let name = name.map(str::to_owned);
        self.retry_write_on_conflict(|| {
            let workspace_id = workspace_id.clone();
            let thread_id = thread_id.clone();
            let member_principal_id = member_principal_id.clone();
            let name = name.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin thread management transaction")?;
                if let Some(principal_id) = member_principal_id.as_ref()
                    && find_workspace_membership(&transaction, principal_id, workspace_id.as_str())
                        .await?
                        .is_none()
                {
                    transaction
                        .rollback()
                        .await
                        .context("failed to roll back unauthorized thread management")?;
                    return Ok(None);
                }
                let result = thread_writes::update_user_thread_management(
                    &transaction,
                    workspace_id.as_str(),
                    thread_id.as_str(),
                    member_principal_id.as_ref(),
                    name.as_deref(),
                    access_class,
                    archived,
                    unix_to_datetime(chrono::Utc::now().timestamp()),
                )
                .await?;
                transaction
                    .commit()
                    .await
                    .context("failed to commit thread management transaction")?;
                Ok(result)
            }
        })
        .await
    }

    pub async fn list_private_thread_participant_ids(
        &self,
        gateway_id: &GatewayId,
        workspace_id: &str,
        thread_id: &str,
        acting_member_principal_id: Option<&PrincipalId>,
    ) -> Result<Option<Vec<PrincipalId>>> {
        pioneer_crud_access::thread_participants::ParticipantRepository::new(&self.connection)
            .list_private_thread_participant_ids(
                gateway_id,
                workspace_id,
                thread_id,
                acting_member_principal_id,
            )
            .await
    }

    pub async fn add_private_thread_participant(
        &self,
        gateway_id: &GatewayId,
        workspace_id: &str,
        thread_id: &str,
        acting_member_principal_id: Option<&PrincipalId>,
        target_principal_id: &PrincipalId,
        actor: PersistedActorRef,
    ) -> Result<Option<PrivateThreadParticipantMutation>> {
        pioneer_crud_access::thread_participants::ParticipantRepository::new(&self.connection)
            .add_private_thread_participant(
                gateway_id,
                workspace_id,
                thread_id,
                acting_member_principal_id,
                target_principal_id,
                actor,
                &pioneer_crud_tasks::completion_inbox::InboxCompletionOrder,
            )
            .await
    }

    pub async fn remove_private_thread_participant(
        &self,
        gateway_id: &GatewayId,
        workspace_id: &str,
        thread_id: &str,
        acting_member_principal_id: Option<&PrincipalId>,
        target_principal_id: &PrincipalId,
    ) -> Result<Option<PrivateThreadParticipantMutation>> {
        pioneer_crud_access::thread_participants::ParticipantRepository::new(&self.connection)
            .remove_private_thread_participant(
                gateway_id,
                workspace_id,
                thread_id,
                acting_member_principal_id,
                target_principal_id,
            )
            .await
    }

    pub async fn list_accessible_threads_for_principal(
        &self,
        principal_id: &PrincipalId,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<Thread>> {
        pioneer_crud_timeline::thread_display::ThreadDisplayStore::new(&self.connection)
            .list_accessible_threads_for_principal(principal_id, workspace_id, limit)
            .await
    }

    /// Ordinary visible conversation candidates for threads_list; ACL is separate.
    /// Does not hydrate turns or change read cursors/subscriptions.
    pub async fn list_thread_read_candidates_page(
        &self,
        workspace_id: &str,
        after: Option<&ThreadReadPosition>,
        limit: u64,
    ) -> Result<Vec<ThreadReadPosition>> {
        pioneer_crud_conversations::thread_state::list_read_candidates_page(
            &self.connection,
            workspace_id,
            after,
            limit,
        )
        .await
    }

    pub async fn is_thread_list_conversation(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<bool> {
        pioneer_crud_conversations::thread_state::is_list_conversation(
            &self.connection,
            workspace_id,
            thread_id,
        )
        .await
    }

    pub async fn list_threads_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<Thread>> {
        pioneer_crud_timeline::thread_display::ThreadDisplayStore::new(&self.connection)
            .list_threads_for_workspace(workspace_id, limit)
            .await
    }

    #[cfg(test)]
    async fn attach_latest_thread_turn_snapshots(
        &self,
        threads: &mut [Thread],
        creator_executions: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
        pioneer_crud_timeline::thread_display::ThreadDisplayStore::new(&self.connection)
            .attach_latest_thread_turn_snapshots(threads, creator_executions)
            .await
    }

    pub async fn list_thread_folders(&self, workspace_id: &str) -> Result<Vec<ThreadFolder>> {
        thread_tree::list_thread_folders(&self.connection, workspace_id).await
    }

    pub async fn list_thread_placements(&self, workspace_id: &str) -> Result<Vec<ThreadPlacement>> {
        thread_tree::list_thread_placements(&self.connection, workspace_id).await
    }

    pub async fn get_thread_placement(&self, thread_id: &str) -> Result<Option<ThreadPlacement>> {
        thread_tree::get_thread_placement(&self.connection, thread_id).await
    }

    pub async fn get_thread_agents_doc_explicit(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Option<ThreadAgentsDocRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .find_explicit(&self.connection, workspace_id, folder_id)
            .await
    }

    pub async fn create_thread_agents_doc_draft(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
        actor_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<ThreadAgentsDocRecord> {
        run_database_attempt(async {
            thread_agents_doc::ThreadAgentsDocRepository::new()
                .create_draft(
                    &self.connection,
                    workspace_id,
                    folder_id,
                    thread_agents_doc::now(),
                    actor_id,
                )
                .await
        })
        .await
    }

    pub async fn save_thread_agents_doc(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
        content: &str,
        expected_version: Option<i64>,
        actor_id: Option<&str>,
        save_reason: ThreadAgentsDocSaveReason,
    ) -> thread_agents_doc::ThreadAgentsDocResult<ThreadAgentsDocRecord> {
        run_database_attempt(async {
            thread_agents_doc::ThreadAgentsDocRepository::new()
                .save_content(
                    &self.connection,
                    workspace_id,
                    folder_id,
                    content,
                    expected_version,
                    thread_agents_doc::now(),
                    actor_id,
                    save_reason,
                )
                .await
        })
        .await
    }

    pub async fn archive_thread_agents_doc(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
        expected_version: Option<i64>,
        actor_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Option<ThreadAgentsDocRecord>> {
        run_database_attempt(async {
            thread_agents_doc::ThreadAgentsDocRepository::new()
                .archive(
                    &self.connection,
                    workspace_id,
                    folder_id,
                    expected_version,
                    thread_agents_doc::now(),
                    actor_id,
                )
                .await
        })
        .await
    }

    pub async fn list_thread_agents_doc_revisions(
        &self,
        doc_id: &str,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Vec<ThreadAgentsDocRevisionRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .list_revisions(&self.connection, doc_id)
            .await
    }

    pub async fn list_thread_agents_doc_summaries(
        &self,
        workspace_id: &str,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Vec<ThreadAgentsDocSummaryRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .list_summaries(&self.connection, workspace_id)
            .await
    }

    pub async fn resolve_thread_agents_doc_for_folder(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Option<ResolvedThreadAgentsDocRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .resolve_for_folder(&self.connection, workspace_id, folder_id)
            .await
    }

    pub async fn resolve_thread_agents_doc_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Option<ResolvedThreadAgentsDocRecord>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .resolve_for_thread(&self.connection, workspace_id, thread_id)
            .await
    }

    pub async fn resolve_thread_agents_doc_folder_for_thread(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> thread_agents_doc::ThreadAgentsDocResult<Option<String>> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .folder_for_thread(&self.connection, workspace_id, thread_id)
            .await
    }

    pub async fn get_thread_agents_doc_scope_context(
        &self,
        workspace_id: &str,
        folder_id: Option<&str>,
    ) -> thread_agents_doc::ThreadAgentsDocResult<ThreadAgentsDocScopeContext> {
        thread_agents_doc::ThreadAgentsDocRepository::new()
            .scope_context(&self.connection, workspace_id, folder_id)
            .await
    }

    pub async fn create_thread_folder(
        &self,
        workspace_id: &str,
        parent_folder_id: Option<&str>,
        name: &str,
    ) -> Result<ThreadFolder> {
        self.retry_write_on_conflict(|| {
            thread_tree::create_thread_folder(
                &self.connection,
                workspace_id,
                parent_folder_id,
                name,
            )
        })
        .await
    }

    pub async fn delete_thread_folder_promote(
        &self,
        workspace_id: &str,
        folder_id: &str,
    ) -> Result<bool> {
        self.retry_write_on_conflict(|| {
            thread_tree::delete_thread_folder_promote(&self.connection, workspace_id, folder_id)
        })
        .await
    }

    pub async fn move_thread_to_folder(
        &self,
        workspace_id: &str,
        thread_id: &str,
        folder_id: Option<&str>,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| {
            thread_tree::move_thread_to_folder(&self.connection, workspace_id, thread_id, folder_id)
        })
        .await
    }

    pub async fn move_folder(
        &self,
        workspace_id: &str,
        folder_id: &str,
        parent_folder_id: Option<&str>,
    ) -> Result<()> {
        self.retry_write_on_conflict(|| {
            thread_tree::move_folder(&self.connection, workspace_id, folder_id, parent_folder_id)
        })
        .await
    }

    pub async fn update_thread_name(&self, thread_id: &str, name: &str) -> Result<()> {
        self.retry_write_on_conflict(|| async {
            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread_metadata::update_thread_name(&self.connection, thread_id, name, now).await
        })
        .await
    }

    pub async fn update_thread_name_if_changed(&self, thread_id: &str, name: &str) -> Result<bool> {
        self.retry_write_on_conflict(|| async {
            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread_metadata::update_thread_name_if_changed(&self.connection, thread_id, name, now)
                .await
        })
        .await
    }

    pub async fn update_automatic_thread_name(
        &self,
        thread_id: &str,
        name: &str,
        fallback: Option<&str>,
        binding: Option<&CliRuntimeThreadBindingRecord>,
    ) -> Result<bool> {
        self.retry_write_on_conflict(|| async {
            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread_writes::update_automatic_thread_name(
                &self.connection,
                thread_id,
                name,
                fallback,
                binding,
                now,
            )
            .await
        })
        .await
    }
}
