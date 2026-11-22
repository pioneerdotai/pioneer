use crate::{
    AcceptCliRuntimePendingRequestResponse, CliRuntimePendingRequestListFilter,
    CliRuntimePendingRequestRecord, CliRuntimeRequestAuthorizationBinding, CrudStore,
    GatewayControlWake, NewCliRuntimePendingRequest, ResolveCliRuntimePendingRequest,
    TransitionCliRuntimePendingRequestDelivery,
};
use anyhow::{Context, Result};
use pioneer_crud_execution::cli_runtime_binding;
use pioneer_crud_timeline::timeline_live_projection::project_cli_runtime_pending_request;
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn create_cli_runtime_pending_request(
        &self,
        request: NewCliRuntimePendingRequest,
    ) -> Result<CliRuntimePendingRequestRecord> {
        let control_wakes = vec![GatewayControlWake::Request(
            request.request_id.as_str().to_owned(),
        )];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result =
            async {
                self.retry_write_on_conflict(|| async {
                    let transaction = self.connection.begin().await.context(
                        "failed to begin CLI runtime pending request create transaction",
                    )?;
                    let record =
                        cli_runtime_binding::create_pending_request(&transaction, request.clone())
                            .await?;
                    if let Err(error) =
                        project_cli_runtime_pending_request(&*transaction, &record).await
                    {
                        return Err(crate::store::connection::rollback_preserving_error(
                            transaction,
                            error,
                        )
                        .await);
                    }
                    transaction.commit().await.context(
                        "failed to commit CLI runtime pending request create transaction",
                    )?;
                    Ok(record)
                })
                .await
            }
            .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.is_ok());
            }
        }
        cli_result
    }

    pub async fn open_cli_runtime_pending_request(
        &self,
        request: NewCliRuntimePendingRequest,
    ) -> Result<CliRuntimePendingRequestRecord> {
        let control_wakes = vec![GatewayControlWake::Request(
            request.request_id.as_str().to_owned(),
        )];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            self.retry_write_on_conflict(|| async {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin CLI runtime pending request open transaction")?;
                let record =
                    cli_runtime_binding::open_pending_request(&transaction, request.clone())
                        .await?;
                if let Err(error) =
                    project_cli_runtime_pending_request(&*transaction, &record).await
                {
                    return Err(crate::store::connection::rollback_preserving_error(
                        transaction,
                        error,
                    )
                    .await);
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit CLI runtime pending request open transaction")?;
                Ok(record)
            })
            .await
        }
        .await;
        if cli_result.is_ok() {
            for change in cli_changes {
                change.completed(true);
            }
        }
        cli_result
    }

    pub async fn open_cli_runtime_pending_request_with_authorization(
        &self,
        request: NewCliRuntimePendingRequest,
        authorization: CliRuntimeRequestAuthorizationBinding,
        max_pending_requests: usize,
    ) -> Result<CliRuntimePendingRequestRecord> {
        let control_wakes = vec![GatewayControlWake::Request(
            request.request_id.as_str().to_owned(),
        )];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            self.retry_write_on_conflict(|| async {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin authorized CLI runtime request transaction")?;
                if let Some(turn_id) = request.turn_id.as_deref() {
                    cli_runtime_binding::ensure_pending_request_capacity(
                        &transaction,
                        request.request_id.as_str(),
                        turn_id,
                        max_pending_requests,
                    )
                    .await?;
                }
                let opened =
                    cli_runtime_binding::open_pending_request(&transaction, request.clone())
                        .await?;
                let record = cli_runtime_binding::bind_pending_request_authorization(
                    &transaction,
                    opened.request_id.as_str(),
                    &authorization,
                )
                .await?;
                if let Err(error) =
                    project_cli_runtime_pending_request(&*transaction, &record).await
                {
                    return Err(crate::store::connection::rollback_preserving_error(
                        transaction,
                        error,
                    )
                    .await);
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit authorized CLI runtime request transaction")?;
                Ok(record)
            })
            .await
        }
        .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.is_ok());
            }
        }
        cli_result
    }

    pub async fn open_native_human_interaction_request_with_authorization(
        &self,
        request: NewCliRuntimePendingRequest,
        authorization: CliRuntimeRequestAuthorizationBinding,
        max_pending_requests: usize,
    ) -> Result<CliRuntimePendingRequestRecord> {
        self.cli_runtime_store()
            .open_native_human_interaction_request_with_authorization(
                request,
                authorization,
                max_pending_requests,
            )
            .await
    }

    pub async fn resolve_native_human_interaction_request(
        &self,
        resolution: ResolveCliRuntimePendingRequest,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        self.cli_runtime_store()
            .resolve_native_human_interaction_request(resolution)
            .await
    }

    pub async fn accept_native_human_interaction_response(
        &self,
        response: AcceptCliRuntimePendingRequestResponse,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        self.cli_runtime_store()
            .accept_native_human_interaction_response(response)
            .await
    }

    pub async fn transition_native_human_interaction_delivery(
        &self,
        transition: TransitionCliRuntimePendingRequestDelivery,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        self.cli_runtime_store()
            .transition_native_human_interaction_delivery(transition)
            .await
    }

    pub async fn get_cli_runtime_pending_request(
        &self,
        request_id: &str,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        let request_id = request_id.to_owned();
        cli_runtime_binding::find_pending_request(&self.connection, request_id.as_str()).await
    }

    pub async fn cli_runtime_pending_request_frontier_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Option<(sea_orm::entity::prelude::DateTimeWithTimeZone, String)>> {
        cli_runtime_binding::pending_request_frontier_for_turn(&self.connection, turn_id).await
    }

    pub async fn list_cli_runtime_pending_requests(
        &self,
        filter: CliRuntimePendingRequestListFilter,
    ) -> Result<Vec<CliRuntimePendingRequestRecord>> {
        cli_runtime_binding::list_pending_requests(&self.connection, filter).await
    }

    pub async fn list_open_cli_runtime_pending_requests_for_descendant_threads(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<Vec<CliRuntimePendingRequestRecord>> {
        cli_runtime_binding::list_open_pending_requests_for_descendant_threads(
            &self.connection,
            workspace_id,
            thread_id,
        )
        .await
    }

    pub async fn resolve_cli_runtime_pending_request(
        &self,
        resolution: ResolveCliRuntimePendingRequest,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        let control_wakes = vec![GatewayControlWake::Request(
            resolution.request_id.as_str().to_owned(),
        )];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            self.retry_write_on_conflict(|| async {
                let transaction =
                    self.connection.begin().await.context(
                        "failed to begin CLI runtime pending request resolve transaction",
                    )?;
                let record =
                    cli_runtime_binding::resolve_pending_request(&transaction, resolution.clone())
                        .await?;
                if let Some(record) = &record
                    && let Err(error) =
                        project_cli_runtime_pending_request(&*transaction, record).await
                {
                    return Err(crate::store::connection::rollback_preserving_error(
                        transaction,
                        error,
                    )
                    .await);
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit CLI runtime pending request resolve transaction")?;
                Ok(record)
            })
            .await
        }
        .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.as_ref().is_ok_and(|changed| changed.is_some()));
            }
        }
        cli_result
    }

    pub async fn accept_cli_runtime_pending_request_response(
        &self,
        response: AcceptCliRuntimePendingRequestResponse,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        let control_wakes = vec![GatewayControlWake::Request(
            response.request_id.as_str().to_owned(),
        )];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            self.retry_write_on_conflict(|| async {
                let transaction =
                    self.connection.begin().await.context(
                        "failed to begin human interaction response acceptance transaction",
                    )?;
                let record = cli_runtime_binding::accept_pending_request_response(
                    &transaction,
                    response.clone(),
                )
                .await?;
                if let Some(record) = &record
                    && let Err(error) =
                        project_cli_runtime_pending_request(&*transaction, record).await
                {
                    return Err(crate::store::connection::rollback_preserving_error(
                        transaction,
                        error,
                    )
                    .await);
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit human interaction response acceptance")?;
                Ok(record)
            })
            .await
        }
        .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.as_ref().is_ok_and(|changed| changed.is_some()));
            }
        }
        cli_result
    }

    pub async fn transition_cli_runtime_pending_request_delivery(
        &self,
        transition: TransitionCliRuntimePendingRequestDelivery,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        let control_wakes = vec![GatewayControlWake::Request(
            transition.request_id.as_str().to_owned(),
        )];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            self.retry_write_on_conflict(|| async {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin human interaction delivery transaction")?;
                let record = cli_runtime_binding::transition_pending_request_delivery(
                    &transaction,
                    transition.clone(),
                )
                .await?;
                if let Some(record) = &record
                    && let Err(error) =
                        project_cli_runtime_pending_request(&*transaction, record).await
                {
                    return Err(crate::store::connection::rollback_preserving_error(
                        transaction,
                        error,
                    )
                    .await);
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit human interaction delivery transition")?;
                Ok(record)
            })
            .await
        }
        .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.as_ref().is_ok_and(|changed| changed.is_some()));
            }
        }
        cli_result
    }

    pub async fn cancel_cli_runtime_pending_request(
        &self,
        request_id: &str,
        response_json: Option<String>,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        let control_wakes = vec![GatewayControlWake::Request(request_id.to_owned())];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result =
            async {
                let request_id = request_id.to_owned();
                self.retry_write_on_conflict(|| async {
                    let transaction = self.connection.begin().await.context(
                        "failed to begin CLI runtime pending request cancel transaction",
                    )?;
                    let record = cli_runtime_binding::cancel_pending_request(
                        &transaction,
                        request_id.clone(),
                        response_json.clone(),
                        updated_at,
                    )
                    .await?;
                    if let Some(record) = &record
                        && let Err(error) =
                            project_cli_runtime_pending_request(&*transaction, record).await
                    {
                        return Err(crate::store::connection::rollback_preserving_error(
                            transaction,
                            error,
                        )
                        .await);
                    }
                    transaction.commit().await.context(
                        "failed to commit CLI runtime pending request cancel transaction",
                    )?;
                    Ok(record)
                })
                .await
            }
            .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.as_ref().is_ok_and(|changed| changed.is_some()));
            }
        }
        cli_result
    }

    pub async fn expire_cli_runtime_pending_request(
        &self,
        request_id: &str,
        response_json: Option<String>,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<Option<CliRuntimePendingRequestRecord>> {
        let control_wakes = vec![GatewayControlWake::Request(request_id.to_owned())];
        let cli_changes = [
            self.control_work_change("gateway.control", &control_wakes),
            self.control_work_change("gateway.cli", &control_wakes),
        ];
        let cli_result =
            async {
                let request_id = request_id.to_owned();
                self.retry_write_on_conflict(|| async {
                    let transaction = self.connection.begin().await.context(
                        "failed to begin CLI runtime pending request expire transaction",
                    )?;
                    let record = cli_runtime_binding::expire_pending_request(
                        &transaction,
                        request_id.clone(),
                        response_json.clone(),
                        updated_at,
                    )
                    .await?;
                    if let Some(record) = &record
                        && let Err(error) =
                            project_cli_runtime_pending_request(&*transaction, record).await
                    {
                        return Err(crate::store::connection::rollback_preserving_error(
                            transaction,
                            error,
                        )
                        .await);
                    }
                    transaction.commit().await.context(
                        "failed to commit CLI runtime pending request expire transaction",
                    )?;
                    Ok(record)
                })
                .await
            }
            .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.as_ref().is_ok_and(|changed| changed.is_some()));
            }
        }
        cli_result
    }
}
