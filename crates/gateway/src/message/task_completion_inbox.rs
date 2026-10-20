use super::*;
use crate::authorization::{
    ActionGateDecision, AuthorizationExternalError, AuthorizationService, ResourceAction,
};
use pioneer_protocol::{
    TaskCompletionInboxEntry, TaskCompletionInboxEntryParams, TaskCompletionInboxListParams,
    TaskCompletionInboxListResponse,
};

impl MessageProcessor {
    fn completion_inbox_access<'a>(
        &self,
        context: &'a RequestContext,
        workspace: &'a str,
    ) -> Option<pioneer_crud::TaskCompletionInboxAccess<'a>> {
        let gate = AuthorizationService::new().authorize_action(
            context.principal().kind,
            context.principal().role_key.as_ref(),
            ResourceAction::TaskRead,
        );
        if matches!(gate, ActionGateDecision::Deny { .. }) {
            return None;
        }
        Some(pioneer_crud::TaskCompletionInboxAccess {
            workspace_id: workspace,
            principal_id: context.principal().principal_id.as_str(),
            absolute: matches!(gate, ActionGateDecision::AllowAbsolute),
        })
    }

    pub(super) async fn task_completion_inbox_list(
        &self,
        context: &RequestContext,
        workspace: &crate::authorization::AuthorizedWorkspace,
        id: RequestId,
        params: TaskCompletionInboxListParams,
        read_all: bool,
    ) {
        let before = match params
            .cursor
            .as_deref()
            .map(decode_inbox_cursor)
            .transpose()
        {
            Ok(cursor)
                if params.workspace_id == workspace.workspace_id()
                    && (!read_all || params.cursor.is_none()) =>
            {
                cursor
            }
            _ => {
                self.send_error(
                    context.connection_id(),
                    JsonRpcErrorResponse::new(
                        Some(id),
                        INVALID_PARAMS_CODE,
                        "invalid task inbox cursor or workspace",
                    ),
                )
                .await;
                return;
            }
        };
        let Some(access) = self.completion_inbox_access(context, workspace.workspace_id()) else {
            self.send_error(
                context.connection_id(),
                AuthorizationExternalError::NotFound.response(id),
            )
            .await;
            return;
        };
        let result = async {
            let limit = params.limit.unwrap_or(30).clamp(1, 100) as usize;
            // A short read snapshot keeps the page and total on the same ACL
            // and receipt revision. Release it before projection/serialization.
            let db = self.crud_store.database_connection();
            if read_all {
                pioneer_crud::mark_all_task_completion_inbox_read(
                    &db,
                    &access,
                    &chrono::Utc::now().to_rfc3339(),
                )
                .await?;
                // The mutation is durable and writer capacity has been released.
                // Invalidate even if the subsequent response snapshot fails.
                self.notify_task_completion_inbox_read(context, workspace.workspace_id())
                    .await;
            }
            let snapshot = db.begin_read().await?;
            let rows = pioneer_crud::list_task_completion_inbox(
                &snapshot,
                &access,
                before.as_ref().map(|(at, id)| (at.as_str(), id.as_str())),
                limit + 1,
            )
            .await?;
            let unread_count =
                pioneer_crud::count_unread_task_completion_inbox(&snapshot, &access).await?;
            snapshot.commit().await?;
            let next_cursor = if rows.len() > limit {
                let last = &rows[limit - 1];
                Some(serde_json::to_string(&(&last.completed_key, &last.run_id))?)
            } else {
                None
            };
            let entries = rows
                .into_iter()
                .take(limit)
                .map(|row| completion_inbox_entry(workspace.workspace_id(), row))
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok::<_, anyhow::Error>(TaskCompletionInboxListResponse {
                entries,
                unread_count,
                next_cursor,
            })
        }
        .await;
        match result {
            Ok(response) => {
                self.send_task_response(context.connection_id(), id, &response)
                    .await;
            }
            Err(_) => {
                self.send_error(
                    context.connection_id(),
                    AuthorizationExternalError::Unavailable.response(id),
                )
                .await
            }
        }
    }

    pub(super) async fn task_completion_inbox_entry(
        &self,
        context: &RequestContext,
        workspace: &crate::authorization::AuthorizedWorkspace,
        id: RequestId,
        params: TaskCompletionInboxEntryParams,
        mark_read: bool,
    ) {
        if params.workspace_id != workspace.workspace_id() || params.run_id.trim().is_empty() {
            self.send_error(
                context.connection_id(),
                JsonRpcErrorResponse::new(
                    Some(id),
                    INVALID_PARAMS_CODE,
                    "invalid task inbox scope",
                ),
            )
            .await;
            return;
        }
        let Some(access) = self.completion_inbox_access(context, workspace.workspace_id()) else {
            self.send_error(
                context.connection_id(),
                AuthorizationExternalError::NotFound.response(id),
            )
            .await;
            return;
        };
        let result = async {
            let db = self.crud_store.database_connection();
            if mark_read {
                let at = chrono::Utc::now().to_rfc3339();
                pioneer_crud::mark_task_completion_inbox_read(&db, &access, &params.run_id, &at)
                    .await?;
            }
            let snapshot = db.begin_read().await?;
            let row =
                pioneer_crud::find_task_completion_inbox_entry(&snapshot, &access, &params.run_id)
                    .await?;
            let unread_count =
                pioneer_crud::count_unread_task_completion_inbox(&snapshot, &access).await?;
            snapshot.commit().await?;
            row.map(|row| {
                completion_inbox_entry(workspace.workspace_id(), row).map(|entry| {
                    pioneer_protocol::TaskCompletionInboxEntryResponse {
                        entry,
                        unread_count,
                    }
                })
            })
            .transpose()
        }
        .await;
        match result {
            Ok(Some(entry)) => {
                self.send_task_response(context.connection_id(), id, &entry)
                    .await;
                if mark_read {
                    self.notify_task_completion_inbox_read(context, workspace.workspace_id())
                        .await;
                }
            }
            Ok(None) => {
                self.send_error(
                    context.connection_id(),
                    AuthorizationExternalError::NotFound.response(id),
                )
                .await
            }
            Err(_) => {
                self.send_error(
                    context.connection_id(),
                    AuthorizationExternalError::Unavailable.response(id),
                )
                .await
            }
        }
    }

    pub(super) async fn task_completion_inbox_viewed(
        &self,
        context: &RequestContext,
        workspace: &crate::authorization::AuthorizedWorkspace,
        id: RequestId,
        params: pioneer_protocol::TaskCompletionInboxViewedParams,
    ) {
        let Some(access) = self.completion_inbox_access(context, workspace.workspace_id()) else {
            self.send_error(
                context.connection_id(),
                AuthorizationExternalError::NotFound.response(id),
            )
            .await;
            return;
        };
        if params.workspace_id != workspace.workspace_id()
            || params.run_ids.len() + params.result_turn_ids.len() + params.result_items.len() > 100
        {
            self.send_error(
                context.connection_id(),
                JsonRpcErrorResponse::new(
                    Some(id),
                    INVALID_PARAMS_CODE,
                    "invalid task inbox view scope",
                ),
            )
            .await;
            return;
        }
        let result = pioneer_crud::mark_viewed_task_completion_inbox_read(
            &self.crud_store.database_connection(),
            &access,
            &params,
            &chrono::Utc::now().to_rfc3339(),
        )
        .await;
        match result {
            Ok(confirmed) => {
                self.send_task_response(context.connection_id(), id, &confirmed)
                    .await;
                if !confirmed.is_empty() {
                    self.notify_task_completion_inbox_read_excluding_source(
                        context,
                        workspace.workspace_id(),
                        true,
                    )
                    .await;
                }
            }
            Err(_) => {
                self.send_error(
                    context.connection_id(),
                    AuthorizationExternalError::Unavailable.response(id),
                )
                .await
            }
        }
    }

    pub(super) async fn notify_task_completion_inbox_read(
        &self,
        context: &RequestContext,
        workspace_id: &str,
    ) {
        self.notify_task_completion_inbox_read_excluding_source(context, workspace_id, false)
            .await;
    }

    async fn notify_task_completion_inbox_read_excluding_source(
        &self,
        context: &RequestContext,
        workspace_id: &str,
        exclude_source: bool,
    ) {
        // A payload-free invalidation goes only to this principal's devices.
        // Every receiver must reload through current TaskRead and root ACL.
        let connections = self
            .session_manager
            .connection_ids_for_principal(&context.principal().principal_id)
            .await
            .into_iter()
            // Only viewed has a mandatory post-confirmation range reload in
            // shared Client (desktop and mobile); independent hints and other
            // read paths keep their original fanout. A failed reload surfaces
            // an error/Retry instead of depending on this redundant own hint.
            .filter(|connection| !exclude_source || *connection != context.connection_id())
            .collect();
        let hint = pioneer_protocol::TaskCompletionInboxChangedNotification {
            workspace_id: workspace_id.to_owned(),
        };
        self.send_notification_to_connections(
            events::TASK_COMPLETION_INBOX_CHANGED,
            &hint,
            connections,
        )
        .await;
    }
}

fn decode_inbox_cursor(cursor: &str) -> anyhow::Result<(String, String)> {
    anyhow::ensure!(cursor.len() <= 256, "invalid inbox cursor");
    let (at, id): (String, String) = serde_json::from_str(cursor)?;
    anyhow::ensure!(
        !at.is_empty() && at.len() <= 80 && !id.is_empty() && id.len() <= 128,
        "invalid inbox cursor"
    );
    Ok((at, id))
}

fn completion_inbox_entry(
    workspace: &str,
    row: pioneer_crud::TaskCompletionInboxRow,
) -> anyhow::Result<TaskCompletionInboxEntry> {
    let error = row
        .error_json
        .as_deref()
        .map(serde_json::from_str::<pioneer_protocol::TaskError>)
        .transpose()?;
    Ok(TaskCompletionInboxEntry {
        workspace_id: workspace.to_owned(),
        task_id: row.task_id,
        run_id: row.run_id,
        title: row.title,
        thread_id: row.thread_id,
        thread_title: row.thread_title,
        status: row.status,
        completed_at: row.completed_at,
        summary: row.summary,
        error: error
            .as_ref()
            .map(|error| crate::task_projection::project_error(error).error.message),
        read_at: row.read_at,
    })
}
