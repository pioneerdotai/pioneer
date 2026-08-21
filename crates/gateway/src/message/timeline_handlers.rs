use super::timeline_cursor::{
    ResolvedTimelineAnchor, TimelineLimitPolicy, encode_thread_timeline_cursor,
    encode_turn_work_cursor, resolve_thread_timeline_anchor, resolve_turn_work_anchor,
    validate_timeline_limit,
};
use super::*;
use crate::authorization::{AuthorizationExternalError, AuthorizedThread, AuthorizedTurn};
use anyhow::{Context, Result, anyhow, bail};
use pioneer_crud::{
    BLOCK_KIND_APPROVAL, BLOCK_KIND_ASSISTANT_MESSAGE, BLOCK_KIND_DETACHED_TASK_RUN,
    BLOCK_KIND_RUNNING, BLOCK_KIND_SYSTEM, BLOCK_KIND_TURN_WORK, BLOCK_KIND_USER_MESSAGE,
    CliRuntimePendingRequestListFilter, ProjectionPageAnchor, SEMANTIC_TIMELINE_PROJECTION_VERSION,
    ThreadTimelineApprovalScope, WORK_VISIBILITY_VISIBLE, approval_block_id,
};
use pioneer_entity::{thread_timeline_block, turn_work_item_projection, turn_work_projection};
use pioneer_protocol::{
    CLIRuntimePendingRequest, CLIRuntimePendingRequestStatus, CLIRuntimeRequestKind,
    TaskThreadLineage, ThreadReadParams, ThreadTimelinePageParams, ThreadTimelinePageResponse,
    TimelineBlock, TimelineBlockKind, TimelinePageInfo, TimelineReplySummary, Turn, TurnItem,
    TurnWorkBlock, TurnWorkItem, TurnWorkItemStatus, TurnWorkItemsGetParams,
    TurnWorkItemsGetResponse, TurnWorkPageParams, TurnWorkPageResponse, TurnWorkPresentation,
    TurnWorkState, UserInput, UserMessageAttachment,
};

const TURN_WORK_ITEMS_GET_MAX_IDS: usize = 200;
const TURN_WORK_SNAPSHOT_MAX_ATTEMPTS: usize = 4;
const TIMELINE_REPLY_TEXT_MAX_CHARS: usize = 280;

fn timeline_public_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    diagnostic: impl std::fmt::Display,
) -> JsonRpcErrorResponse {
    let public_code = if jsonrpc_code == INVALID_PARAMS_CODE {
        pioneer_protocol::PublicErrorCode::InvalidInput
    } else {
        pioneer_protocol::PublicErrorCode::Internal
    };
    crate::public_error::agent_rpc_error(
        request_id,
        jsonrpc_code,
        public_code,
        pioneer_protocol::PublicErrorStage::Observation,
        diagnostic,
    )
}

fn timeline_delivery_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    diagnostic: impl std::fmt::Display,
) -> JsonRpcErrorResponse {
    crate::public_error::agent_rpc_error(
        request_id,
        jsonrpc_code,
        pioneer_protocol::PublicErrorCode::Internal,
        pioneer_protocol::PublicErrorStage::Delivery,
        diagnostic,
    )
}

struct ThreadTimelineRowsPage {
    rows: Vec<thread_timeline_block::Model>,
    has_more_before: bool,
    has_more_after: bool,
}

struct TurnWorkRowsPage {
    rows: Vec<turn_work_item_projection::Model>,
    has_more_before: bool,
    has_more_after: bool,
}

struct TurnWorkPageSnapshot {
    projection: turn_work_projection::Model,
    rows_page: TurnWorkRowsPage,
    items: Vec<TurnWorkItem>,
}

struct TurnWorkItemsSnapshot {
    projection: turn_work_projection::Model,
    items: Vec<TurnWorkItem>,
    removed_work_item_ids: Vec<String>,
}

#[derive(Default)]
struct UserMessageTimelineBatch {
    turns: HashMap<String, Turn>,
    inputs: HashMap<String, Vec<UserInput>>,
    attachments: HashMap<String, Vec<UserMessageAttachment>>,
}

#[derive(Default)]
struct AssistantMessageTimelineBatch {
    items: HashMap<(String, String), TurnItem>,
    authors: HashMap<String, pioneer_protocol::TurnAuthorSnapshot>,
}

fn exact_agent_turn_input_author(turn: &Turn) -> Option<pioneer_protocol::TurnAuthorSnapshot> {
    let author = turn.author.as_ref()?;
    let pioneer_protocol::PersistedActorRef::AgentExecution(execution_id) = &author.actor else {
        return None;
    };
    let presentation = author.agent.as_ref()?;
    if &presentation.agent_execution_id != execution_id
        || presentation.to_turn_author_snapshot() != *author
    {
        return None;
    }
    Some(author.clone())
}

fn log_thread_read_outcome(
    thread_id: &str,
    through_turn_id: &str,
    outcome: &'static str,
    unread_count: u64,
    started: std::time::Instant,
) {
    debug!(
        thread_id,
        through_turn_id,
        operation = "thread_read",
        outcome,
        unread_count,
        latency_ms = started.elapsed().as_millis(),
        "Thread read cursor outcome"
    );
}

impl MessageProcessor {
    pub(super) async fn thread_read(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedThread,
        request_id: RequestId,
        params: ThreadReadParams,
    ) {
        let started = std::time::Instant::now();
        let connection_id = request_context.connection_id();
        let thread_id = params.thread_id.trim();
        let through_turn_id = params.through_turn_id.trim();
        if authorization.thread_id() != thread_id
            || thread_id.is_empty()
            || through_turn_id.is_empty()
        {
            log_thread_read_outcome(thread_id, through_turn_id, "rejected", 0, started);
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let response_payload = match self
            .crud_store
            .mark_thread_read(pioneer_crud::MarkThreadReadRequest {
                workspace_id: authorization.workspace_id().to_owned(),
                thread_id: thread_id.to_owned(),
                principal_id: request_context.principal().principal_id.clone(),
                through_turn_id: through_turn_id.to_owned(),
                read_at_unix: now_timestamp_secs(),
            })
            .await
        {
            Ok(response) => response,
            Err(error)
                if error
                    .downcast_ref::<pioneer_crud::ThreadReadFailure>()
                    .is_some() =>
            {
                log_thread_read_outcome(thread_id, through_turn_id, "rejected", 0, started);
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
                return;
            }
            Err(error) => {
                log_thread_read_outcome(thread_id, through_turn_id, "rejected", 0, started);
                warn!(
                    connection_id,
                    thread_id,
                    error = %format!("{error:#}"),
                    "failed to advance thread read cursor"
                );
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::Unavailable.response(request_id),
                )
                .await;
                return;
            }
        };
        log_thread_read_outcome(
            thread_id,
            through_turn_id,
            "accepted",
            response_payload.unread_count,
            started,
        );
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_delivery_error(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };
        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send thread/read response"
            );
        }
        let notification = pioneer_protocol::ThreadReadCursorChangedNotification {
            workspace_id: response_payload.workspace_id,
            thread_id: response_payload.thread_id.clone(),
            cursor: response_payload.cursor,
            unread_count: response_payload.unread_count,
        };
        let candidate_connection_ids = self
            .session_manager
            .connection_ids_for_principal(&request_context.principal().principal_id)
            .await;
        self.send_thread_scoped_notification_to_connections(
            response_payload.thread_id.as_str(),
            events::THREAD_READ_CURSOR_CHANGED,
            &notification,
            candidate_connection_ids,
        )
        .await;
    }

    pub(super) async fn thread_timeline_page(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedThread,
        request_id: RequestId,
        params: ThreadTimelinePageParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if params.thread_id.trim().is_empty() {
            self.send_error(
                connection_id,
                timeline_public_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` is required",
                        methods::THREAD_TIMELINE_PAGE
                    ),
                ),
            )
            .await;
            return;
        }

        let limit =
            match validate_timeline_limit(params.limit, TimelineLimitPolicy::thread_timeline()) {
                Ok(limit) => limit,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        error.into_error_response(request_id, methods::THREAD_TIMELINE_PAGE),
                    )
                    .await;
                    return;
                }
            };

        let anchor = match resolve_thread_timeline_anchor(
            &params.anchor,
            params.thread_id.as_str(),
            SEMANTIC_TIMELINE_PROJECTION_VERSION,
        ) {
            Ok(anchor) => anchor,
            Err(error) => {
                self.send_error(
                    connection_id,
                    error.into_error_response(request_id, methods::THREAD_TIMELINE_PAGE),
                )
                .await;
                return;
            }
        };

        let thread_model = match self
            .crud_store
            .get_thread_by_id(params.thread_id.as_str())
            .await
        {
            Ok(Some(thread_model)) => thread_model,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("thread `{}` was not found", params.thread_id),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        if thread_model.workspace_id != authorization.workspace_id() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let approval_action = crate::authorization::ResourceAction::AgentRequestObserve;
        let approval_gate = crate::authorization::AuthorizationService::new().authorize_action(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
            approval_action,
        );
        let approval_resolver =
            crate::authorization::AuthorizationResolver::new(self.crud_store.as_ref().clone());
        let mut approval_resolution = approval_resolver
            .authorize_thread(
                request_context.principal(),
                &approval_gate,
                approval_action,
                params.thread_id.as_str(),
                Some(authorization.workspace_id()),
            )
            .await;
        if matches!(
            approval_resolution
                .as_ref()
                .ok()
                .and_then(|resolution| resolution.denial()),
            Some(crate::authorization::AuthorizationDecision::Deny {
                reason: crate::authorization::DenyReason::MissingAuthoritativeResource,
                ..
            })
        ) {
            approval_resolution = approval_resolver
                .authorize_internal_thread_via_root(
                    request_context.principal(),
                    &approval_gate,
                    approval_action,
                    params.thread_id.as_str(),
                    Some(authorization.workspace_id()),
                )
                .await;
        }
        let can_observe_agent_requests = matches!(
            approval_resolution,
            Ok(crate::authorization::ProofResolution::Authorized(_))
        );
        let approval_scope = ThreadTimelineApprovalScope {
            can_observe_agent_requests,
        };
        let rows_page = match self
            .load_thread_timeline_rows(
                params.thread_id.as_str(),
                Some(&approval_scope),
                anchor,
                limit,
            )
            .await
        {
            Ok(page) => page,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread timeline page: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let page_info = match self.thread_timeline_page_info(
            rows_page.rows.as_slice(),
            rows_page.has_more_before,
            rows_page.has_more_after,
        ) {
            Ok(page_info) => page_info,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to encode timeline cursors: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let requested_thread_id = params.thread_id.clone();
        let workspace_id = thread_model.workspace_id.clone();
        let mut blocks = match self
            .thread_timeline_blocks_from_rows(rows_page.rows, Some(&approval_scope))
            .await
        {
            Ok(blocks) => blocks,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to materialize thread timeline blocks: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        let descendant_pending_blocks = match self
            .descendant_pending_request_blocks(
                workspace_id.as_str(),
                requested_thread_id.as_str(),
                Some(&approval_scope),
            )
            .await
        {
            Ok(blocks) => blocks,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load descendant pending approvals: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };
        append_timeline_blocks_dedup(&mut blocks, descendant_pending_blocks);

        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let response_payload = ThreadTimelinePageResponse {
            workspace_id,
            thread_id: requested_thread_id,
            projection_version: SEMANTIC_TIMELINE_PROJECTION_VERSION,
            blocks,
            page: page_info,
        };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_delivery_error(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send thread/timeline/page response"
            );
        }
    }

    pub(super) async fn turn_work_page(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnWorkPageParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim()
            || authorization.turn_id() != params.turn_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                timeline_public_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_WORK_PAGE
                    ),
                ),
            )
            .await;
            return;
        }

        let limit = match validate_timeline_limit(params.limit, TimelineLimitPolicy::turn_work()) {
            Ok(limit) => limit,
            Err(error) => {
                self.send_error(
                    connection_id,
                    error.into_error_response(request_id, methods::TURN_WORK_PAGE),
                )
                .await;
                return;
            }
        };

        let anchor = match resolve_turn_work_anchor(
            &params.anchor,
            params.thread_id.as_str(),
            params.turn_id.as_str(),
            SEMANTIC_TIMELINE_PROJECTION_VERSION,
        ) {
            Ok(anchor) => anchor,
            Err(error) => {
                self.send_error(
                    connection_id,
                    error.into_error_response(request_id, methods::TURN_WORK_PAGE),
                )
                .await;
                return;
            }
        };

        let work_projection = match self
            .crud_store
            .get_turn_work_projection(params.turn_id.as_str())
            .await
        {
            Ok(Some(projection))
                if projection.thread_id == params.thread_id
                    && projection.workspace_id == authorization.workspace_id() =>
            {
                projection
            }
            Ok(Some(_)) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: turn `{}` does not belong to thread `{}`",
                            methods::TURN_WORK_PAGE,
                            params.turn_id,
                            params.thread_id
                        ),
                    ),
                )
                .await;
                return;
            }
            Ok(None) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "turn work projection for turn `{}` was not found",
                            params.turn_id
                        ),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load turn work projection: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let snapshot = match self
            .load_consistent_turn_work_page_snapshot(work_projection, anchor, limit)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load turn work page: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let page_info = match turn_work_page_info(
            snapshot.rows_page.rows.as_slice(),
            snapshot.rows_page.has_more_before,
            snapshot.rows_page.has_more_after,
        ) {
            Ok(page_info) => page_info,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to encode turn work cursors: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let workspace_id = snapshot.projection.workspace_id.clone();
        let source_high_watermark = snapshot.projection.source_high_watermark;
        let projection_updated_at_unix_micros = snapshot.projection.updated_at.timestamp_micros();
        let work = match self
            .turn_work_block_from_projection(snapshot.projection)
            .await
        {
            Ok(work) => work,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to materialize turn work block: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let response_payload = TurnWorkPageResponse {
            workspace_id,
            thread_id: params.thread_id,
            turn_id: params.turn_id,
            projection_version: SEMANTIC_TIMELINE_PROJECTION_VERSION,
            source_high_watermark,
            projection_updated_at_unix_micros,
            work,
            items: snapshot.items,
            page: page_info,
        };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_delivery_error(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/work/page response"
            );
        }
    }

    pub(super) async fn turn_work_items_get(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnWorkItemsGetParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim()
            || authorization.turn_id() != params.turn_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        if params.thread_id.trim().is_empty()
            || params.turn_id.trim().is_empty()
            || params.work_item_ids.is_empty()
            || params
                .work_item_ids
                .iter()
                .any(|work_item_id| work_item_id.trim().is_empty())
            || params.work_item_ids.len() > TURN_WORK_ITEMS_GET_MAX_IDS
        {
            self.send_error(
                connection_id,
                timeline_public_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id`, `turn_id`, and 1..={TURN_WORK_ITEMS_GET_MAX_IDS} non-empty `work_item_ids` are required",
                        methods::TURN_WORK_ITEMS_GET
                    ),
                ),
            )
            .await;
            return;
        }

        let mut work_item_ids = params.work_item_ids;
        work_item_ids.sort();
        work_item_ids.dedup();

        let work_projection = match self
            .crud_store
            .get_turn_work_projection(params.turn_id.as_str())
            .await
        {
            Ok(Some(projection))
                if projection.thread_id == params.thread_id
                    && projection.workspace_id == authorization.workspace_id() =>
            {
                projection
            }
            Ok(Some(_)) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: turn `{}` does not belong to thread `{}`",
                            methods::TURN_WORK_ITEMS_GET,
                            params.turn_id,
                            params.thread_id
                        ),
                    ),
                )
                .await;
                return;
            }
            Ok(None) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "turn work projection for turn `{}` was not found",
                            params.turn_id
                        ),
                    ),
                )
                .await;
                return;
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load turn work projection: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let snapshot = match self
            .load_consistent_turn_work_items_snapshot(work_projection, work_item_ids)
            .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_public_error(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load turn work items: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let workspace_id = snapshot.projection.workspace_id.clone();
        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;

        let response_payload = TurnWorkItemsGetResponse {
            workspace_id,
            thread_id: params.thread_id,
            turn_id: params.turn_id,
            projection_version: SEMANTIC_TIMELINE_PROJECTION_VERSION,
            source_high_watermark: snapshot.projection.source_high_watermark,
            projection_updated_at_unix_micros: snapshot.projection.updated_at.timestamp_micros(),
            items: snapshot.items,
            removed_work_item_ids: snapshot.removed_work_item_ids,
        };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    timeline_delivery_error(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send turn/work/items/get response"
            );
        }
    }

    async fn load_thread_timeline_rows(
        &self,
        thread_id: &str,
        approval_scope: Option<&ThreadTimelineApprovalScope>,
        anchor: ResolvedTimelineAnchor,
        limit: u64,
    ) -> Result<ThreadTimelineRowsPage> {
        let fetch_limit = limit.saturating_add(1);
        let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);

        match anchor {
            ResolvedTimelineAnchor::Newest => {
                let mut rows = self
                    .crud_store
                    .list_thread_timeline_projection_page(
                        thread_id,
                        approval_scope,
                        ProjectionPageAnchor::End,
                        fetch_limit,
                    )
                    .await?;
                let has_more_before = rows.len() > limit_usize;
                if has_more_before {
                    rows.remove(0);
                }
                Ok(ThreadTimelineRowsPage {
                    rows,
                    has_more_before,
                    has_more_after: false,
                })
            }
            ResolvedTimelineAnchor::Oldest => {
                let mut rows = self
                    .crud_store
                    .list_thread_timeline_projection_page(
                        thread_id,
                        approval_scope,
                        ProjectionPageAnchor::Start,
                        fetch_limit,
                    )
                    .await?;
                let has_more_after = rows.len() > limit_usize;
                if has_more_after {
                    rows.truncate(limit_usize);
                }
                Ok(ThreadTimelineRowsPage {
                    rows,
                    has_more_before: false,
                    has_more_after,
                })
            }
            ResolvedTimelineAnchor::Before(sort_key) => {
                let mut rows = self
                    .crud_store
                    .list_thread_timeline_projection_page(
                        thread_id,
                        approval_scope,
                        ProjectionPageAnchor::Before(sort_key.as_str()),
                        fetch_limit,
                    )
                    .await?;
                let has_more_before = rows.len() > limit_usize;
                if has_more_before {
                    rows.remove(0);
                }
                Ok(ThreadTimelineRowsPage {
                    rows,
                    has_more_before,
                    has_more_after: true,
                })
            }
            ResolvedTimelineAnchor::After(sort_key) => {
                let mut rows = self
                    .crud_store
                    .list_thread_timeline_projection_page(
                        thread_id,
                        approval_scope,
                        ProjectionPageAnchor::After(sort_key.as_str()),
                        fetch_limit,
                    )
                    .await?;
                let has_more_after = rows.len() > limit_usize;
                if has_more_after {
                    rows.truncate(limit_usize);
                }
                Ok(ThreadTimelineRowsPage {
                    rows,
                    has_more_before: true,
                    has_more_after,
                })
            }
            ResolvedTimelineAnchor::Around(sort_key) => {
                self.load_thread_timeline_rows_around(
                    thread_id,
                    approval_scope,
                    sort_key.as_str(),
                    limit,
                )
                .await
            }
        }
    }

    async fn load_thread_timeline_rows_around(
        &self,
        thread_id: &str,
        approval_scope: Option<&ThreadTimelineApprovalScope>,
        sort_key: &str,
        limit: u64,
    ) -> Result<ThreadTimelineRowsPage> {
        let before_limit = limit / 2;
        let mut before_rows = self
            .crud_store
            .list_thread_timeline_projection_page(
                thread_id,
                approval_scope,
                ProjectionPageAnchor::Before(sort_key),
                before_limit.saturating_add(1),
            )
            .await?;
        let has_more_before =
            before_rows.len() > usize::try_from(before_limit).unwrap_or(usize::MAX);
        if has_more_before {
            before_rows.remove(0);
        }

        let anchor_row = self
            .crud_store
            .find_thread_timeline_projection_block_by_sort_key(thread_id, approval_scope, sort_key)
            .await?;
        let anchor_len = u64::from(anchor_row.is_some());
        let after_limit = limit
            .saturating_sub(u64::try_from(before_rows.len()).unwrap_or(u64::MAX))
            .saturating_sub(anchor_len);
        let mut after_rows = self
            .crud_store
            .list_thread_timeline_projection_page(
                thread_id,
                approval_scope,
                ProjectionPageAnchor::After(sort_key),
                after_limit.saturating_add(1),
            )
            .await?;
        let has_more_after = after_rows.len() > usize::try_from(after_limit).unwrap_or(usize::MAX);
        if has_more_after {
            after_rows.truncate(usize::try_from(after_limit).unwrap_or(usize::MAX));
        }

        let mut rows = before_rows;
        if let Some(anchor_row) = anchor_row {
            rows.push(anchor_row);
        }
        rows.extend(after_rows);

        Ok(ThreadTimelineRowsPage {
            rows,
            has_more_before,
            has_more_after,
        })
    }

    async fn load_consistent_turn_work_page_snapshot(
        &self,
        mut projection: turn_work_projection::Model,
        anchor: ResolvedTimelineAnchor,
        limit: u64,
    ) -> Result<TurnWorkPageSnapshot> {
        for _ in 0..TURN_WORK_SNAPSHOT_MAX_ATTEMPTS {
            let rows_page = self
                .load_turn_work_rows(projection.turn_id.as_str(), anchor.clone(), limit)
                .await?;
            let items = self
                .turn_work_items_from_rows(rows_page.rows.as_slice())
                .await?;
            let Some(current_projection) = self
                .crud_store
                .get_turn_work_projection(projection.turn_id.as_str())
                .await?
            else {
                return Err(anyhow!(
                    "turn work projection for turn `{}` disappeared while reading its page",
                    projection.turn_id
                ));
            };

            if turn_work_projection_revision(&projection)
                == turn_work_projection_revision(&current_projection)
            {
                return Ok(TurnWorkPageSnapshot {
                    projection: current_projection,
                    rows_page,
                    items,
                });
            }
            projection = current_projection;
        }

        Err(anyhow!(
            "turn work projection for turn `{}` kept changing while reading its page",
            projection.turn_id
        ))
    }

    async fn load_consistent_turn_work_items_snapshot(
        &self,
        mut projection: turn_work_projection::Model,
        work_item_ids: Vec<String>,
    ) -> Result<TurnWorkItemsSnapshot> {
        for _ in 0..TURN_WORK_SNAPSHOT_MAX_ATTEMPTS {
            let rows = self
                .crud_store
                .list_turn_work_item_projections_by_ids(
                    projection.turn_id.as_str(),
                    work_item_ids.as_slice(),
                    Some(WORK_VISIBILITY_VISIBLE),
                )
                .await?;
            let items = self.turn_work_items_from_rows(rows.as_slice()).await?;
            let Some(current_projection) = self
                .crud_store
                .get_turn_work_projection(projection.turn_id.as_str())
                .await?
            else {
                return Err(anyhow!(
                    "turn work projection for turn `{}` disappeared while reading its items",
                    projection.turn_id
                ));
            };

            if turn_work_projection_revision(&projection)
                == turn_work_projection_revision(&current_projection)
            {
                let returned_ids = rows
                    .iter()
                    .map(|row| row.work_item_id.as_str())
                    .collect::<HashSet<_>>();
                let removed_work_item_ids = work_item_ids
                    .iter()
                    .filter(|work_item_id| !returned_ids.contains(work_item_id.as_str()))
                    .cloned()
                    .collect();
                return Ok(TurnWorkItemsSnapshot {
                    projection: current_projection,
                    items,
                    removed_work_item_ids,
                });
            }
            projection = current_projection;
        }

        Err(anyhow!(
            "turn work projection for turn `{}` kept changing while reading its items",
            projection.turn_id
        ))
    }

    async fn load_turn_work_rows(
        &self,
        turn_id: &str,
        anchor: ResolvedTimelineAnchor,
        limit: u64,
    ) -> Result<TurnWorkRowsPage> {
        let fetch_limit = limit.saturating_add(1);
        let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
        let visibility = Some(WORK_VISIBILITY_VISIBLE);

        match anchor {
            ResolvedTimelineAnchor::Newest => {
                let mut rows = self
                    .crud_store
                    .list_turn_work_item_projection_page(
                        turn_id,
                        visibility,
                        ProjectionPageAnchor::End,
                        fetch_limit,
                    )
                    .await?;
                let has_more_before = rows.len() > limit_usize;
                if has_more_before {
                    rows.remove(0);
                }
                Ok(TurnWorkRowsPage {
                    rows,
                    has_more_before,
                    has_more_after: false,
                })
            }
            ResolvedTimelineAnchor::Oldest => {
                let mut rows = self
                    .crud_store
                    .list_turn_work_item_projection_page(
                        turn_id,
                        visibility,
                        ProjectionPageAnchor::Start,
                        fetch_limit,
                    )
                    .await?;
                let has_more_after = rows.len() > limit_usize;
                if has_more_after {
                    rows.truncate(limit_usize);
                }
                Ok(TurnWorkRowsPage {
                    rows,
                    has_more_before: false,
                    has_more_after,
                })
            }
            ResolvedTimelineAnchor::Before(order_key) => {
                let mut rows = self
                    .crud_store
                    .list_turn_work_item_projection_page(
                        turn_id,
                        visibility,
                        ProjectionPageAnchor::Before(order_key.as_str()),
                        fetch_limit,
                    )
                    .await?;
                let has_more_before = rows.len() > limit_usize;
                if has_more_before {
                    rows.remove(0);
                }
                Ok(TurnWorkRowsPage {
                    rows,
                    has_more_before,
                    has_more_after: true,
                })
            }
            ResolvedTimelineAnchor::After(order_key) => {
                let mut rows = self
                    .crud_store
                    .list_turn_work_item_projection_page(
                        turn_id,
                        visibility,
                        ProjectionPageAnchor::After(order_key.as_str()),
                        fetch_limit,
                    )
                    .await?;
                let has_more_after = rows.len() > limit_usize;
                if has_more_after {
                    rows.truncate(limit_usize);
                }
                Ok(TurnWorkRowsPage {
                    rows,
                    has_more_before: true,
                    has_more_after,
                })
            }
            ResolvedTimelineAnchor::Around(order_key) => {
                self.load_turn_work_rows_around(turn_id, order_key.as_str(), limit)
                    .await
            }
        }
    }

    async fn load_turn_work_rows_around(
        &self,
        turn_id: &str,
        order_key: &str,
        limit: u64,
    ) -> Result<TurnWorkRowsPage> {
        let visibility = Some(WORK_VISIBILITY_VISIBLE);
        let before_limit = limit / 2;
        let mut before_rows = self
            .crud_store
            .list_turn_work_item_projection_page(
                turn_id,
                visibility,
                ProjectionPageAnchor::Before(order_key),
                before_limit.saturating_add(1),
            )
            .await?;
        let has_more_before =
            before_rows.len() > usize::try_from(before_limit).unwrap_or(usize::MAX);
        if has_more_before {
            before_rows.remove(0);
        }

        let anchor_row = self
            .crud_store
            .find_turn_work_item_projection_by_order_key(turn_id, order_key, visibility)
            .await?;
        let anchor_len = u64::from(anchor_row.is_some());
        let after_limit = limit
            .saturating_sub(u64::try_from(before_rows.len()).unwrap_or(u64::MAX))
            .saturating_sub(anchor_len);
        let mut after_rows = self
            .crud_store
            .list_turn_work_item_projection_page(
                turn_id,
                visibility,
                ProjectionPageAnchor::After(order_key),
                after_limit.saturating_add(1),
            )
            .await?;
        let has_more_after = after_rows.len() > usize::try_from(after_limit).unwrap_or(usize::MAX);
        if has_more_after {
            after_rows.truncate(usize::try_from(after_limit).unwrap_or(usize::MAX));
        }

        let mut rows = before_rows;
        if let Some(anchor_row) = anchor_row {
            rows.push(anchor_row);
        }
        rows.extend(after_rows);

        Ok(TurnWorkRowsPage {
            rows,
            has_more_before,
            has_more_after,
        })
    }

    fn thread_timeline_page_info(
        &self,
        rows: &[thread_timeline_block::Model],
        has_more_before: bool,
        has_more_after: bool,
    ) -> Result<TimelinePageInfo> {
        let before_cursor = rows
            .first()
            .map(|row| {
                encode_thread_timeline_cursor(
                    SEMANTIC_TIMELINE_PROJECTION_VERSION,
                    row.thread_id.as_str(),
                    row.block_id.as_str(),
                    row.sort_key.as_str(),
                )
            })
            .transpose()
            .map_err(|error| anyhow!(error))?;
        let after_cursor = rows
            .last()
            .map(|row| {
                encode_thread_timeline_cursor(
                    SEMANTIC_TIMELINE_PROJECTION_VERSION,
                    row.thread_id.as_str(),
                    row.block_id.as_str(),
                    row.sort_key.as_str(),
                )
            })
            .transpose()
            .map_err(|error| anyhow!(error))?;

        Ok(TimelinePageInfo {
            before_cursor,
            after_cursor,
            has_more_before,
            has_more_after,
        })
    }

    async fn thread_timeline_blocks_from_rows(
        &self,
        rows: Vec<thread_timeline_block::Model>,
        approval_scope: Option<&ThreadTimelineApprovalScope>,
    ) -> Result<Vec<TimelineBlock>> {
        let user_messages = self.user_message_timeline_batch(rows.as_slice()).await?;
        let action_targets = rows
            .iter()
            .filter_map(|row| match row.block_kind.as_str() {
                BLOCK_KIND_USER_MESSAGE | BLOCK_KIND_RUNNING => {
                    row.turn_id.clone().map(|turn_id| (turn_id, None))
                }
                BLOCK_KIND_ASSISTANT_MESSAGE => row
                    .turn_id
                    .clone()
                    .zip(row.source_key.clone())
                    .map(|(turn_id, item_id)| (turn_id, Some(item_id))),
                _ => None,
            })
            .collect::<Vec<_>>();
        let action_timeline = pioneer_crud::load_agent_action_timeline_projections_for_targets(
            &self.crud_store.database_connection(),
            action_targets.as_slice(),
        )
        .await?;
        let assistant_messages = self
            .assistant_message_timeline_batch(rows.as_slice())
            .await?;
        let mut blocks = Vec::with_capacity(rows.len());
        for row in rows {
            let target_key = match row.block_kind.as_str() {
                BLOCK_KIND_USER_MESSAGE => {
                    row.turn_id.as_ref().map(|turn_id| (turn_id.clone(), None))
                }
                BLOCK_KIND_ASSISTANT_MESSAGE => row.turn_id.as_ref().and_then(|turn_id| {
                    row.source_key
                        .as_ref()
                        .map(|item_id| (turn_id.clone(), Some(item_id.clone())))
                }),
                BLOCK_KIND_RUNNING => row.turn_id.as_ref().map(|turn_id| (turn_id.clone(), None)),
                _ => None,
            };
            let action_projection = target_key
                .as_ref()
                .and_then(|target_key| action_timeline.get(target_key));
            if let Some(block) = self
                .thread_timeline_block_from_row(
                    row,
                    approval_scope,
                    &user_messages,
                    &assistant_messages,
                    action_projection,
                )
                .await?
            {
                blocks.push(block);
            }
        }
        Ok(blocks)
    }

    async fn user_message_timeline_batch(
        &self,
        rows: &[thread_timeline_block::Model],
    ) -> Result<UserMessageTimelineBatch> {
        let mut page_turn_ids = Vec::new();
        let mut seen_turn_ids = HashSet::new();
        for row in rows
            .iter()
            .filter(|row| row.block_kind == BLOCK_KIND_USER_MESSAGE)
        {
            let turn_id = row
                .turn_id
                .as_deref()
                .or(row.source_key.as_deref())
                .context("user message timeline block is missing turn_id")?;
            if seen_turn_ids.insert(turn_id.to_owned()) {
                page_turn_ids.push(turn_id.to_owned());
            }
        }
        if page_turn_ids.is_empty() {
            return Ok(UserMessageTimelineBatch::default());
        }

        let first_row = rows
            .first()
            .context("timeline user-message batch has no source row")?;
        let thread_id = first_row.thread_id.as_str();
        let workspace_id = first_row.workspace_id.as_str();
        let mut turns = self
            .crud_store
            .get_turns_by_thread_and_ids(thread_id, page_turn_ids.as_slice())
            .await?;
        let reply_turn_ids = page_turn_ids
            .iter()
            .filter_map(|turn_id| turns.get(turn_id))
            .filter_map(|turn| turn.reply_to_turn_id.as_ref())
            .filter(|turn_id| !turns.contains_key(turn_id.as_str()))
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        turns.extend(
            self.crud_store
                .get_turns_by_thread_and_ids(thread_id, reply_turn_ids.as_slice())
                .await?,
        );

        for turn_id in &page_turn_ids {
            if !turns.contains_key(turn_id.as_str()) {
                anyhow::bail!("user message timeline block references a missing Turn `{turn_id}`");
            }
        }

        let input_turn_ids = turns
            .values()
            .filter(|turn| !turn.message_deleted)
            .map(|turn| turn.id.clone())
            .collect::<Vec<_>>();
        let inputs = self
            .crud_store
            .get_turn_inputs_for_turns(input_turn_ids.as_slice())
            .await?;

        let requested_artifacts = inputs
            .values()
            .flatten()
            .filter_map(|input| match input {
                UserInput::Artifact {
                    artifact_id,
                    version_id: Some(version_id),
                } => Some((artifact_id.clone(), version_id.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        let exact_artifact_refs = self
            .crud_store
            .list_exact_artifact_refs(workspace_id, requested_artifacts.as_slice())
            .await?;

        let item_attachments = self
            .crud_store
            .list_turn_items_by_type_for_turns(page_turn_ids.as_slice(), "user_message")
            .await?;
        let mut attachments = HashMap::new();
        for turn_id in &page_turn_ids {
            let Some(turn) = turns.get(turn_id.as_str()) else {
                continue;
            };
            if turn.message_deleted {
                attachments.insert(turn_id.clone(), Vec::new());
                continue;
            }
            let mut resolved = Vec::new();
            if let Some(items) = item_attachments.get(turn_id.as_str()) {
                for item in items {
                    if let TurnItem::UserMessage {
                        attachments: item_attachments,
                        ..
                    } = item
                    {
                        resolved.extend(item_attachments.iter().cloned());
                    }
                }
            }
            if let Some(inputs) = inputs.get(turn_id.as_str()) {
                let exact = inputs
                    .iter()
                    .filter_map(|input| match input {
                        UserInput::Artifact {
                            artifact_id,
                            version_id: Some(version_id),
                        } => exact_artifact_refs
                            .get(&(artifact_id.clone(), version_id.clone()))
                            .cloned()
                            .map(|artifact| UserMessageAttachment::Artifact { artifact }),
                        _ => None,
                    })
                    .collect();
                resolved = merge_user_message_attachments(resolved, exact);
            }
            attachments.insert(turn_id.clone(), resolved);
        }

        Ok(UserMessageTimelineBatch {
            turns,
            inputs,
            attachments,
        })
    }

    async fn assistant_message_timeline_batch(
        &self,
        rows: &[thread_timeline_block::Model],
    ) -> Result<AssistantMessageTimelineBatch> {
        let item_targets = rows
            .iter()
            .filter(|row| row.block_kind == BLOCK_KIND_ASSISTANT_MESSAGE)
            .map(|row| {
                Ok::<_, anyhow::Error>((
                    row.turn_id
                        .clone()
                        .context("assistant message timeline block is missing turn_id")?,
                    row.source_key
                        .clone()
                        .context("assistant message timeline block is missing source_key")?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let turn_ids = rows
            .iter()
            .filter(|row| {
                matches!(
                    row.block_kind.as_str(),
                    BLOCK_KIND_ASSISTANT_MESSAGE
                        | BLOCK_KIND_RUNNING
                        | BLOCK_KIND_TURN_WORK
                        | BLOCK_KIND_DETACHED_TASK_RUN
                        | BLOCK_KIND_APPROVAL
                        | BLOCK_KIND_SYSTEM
                )
            })
            .filter_map(|row| row.turn_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if turn_ids.is_empty() {
            return Ok(AssistantMessageTimelineBatch::default());
        }
        let thread_id = rows
            .first()
            .context("assistant message timeline batch has no source row")?
            .thread_id
            .as_str();
        let turns = self
            .crud_store
            .get_turns_by_thread_and_ids(thread_id, turn_ids.as_slice())
            .await?;
        for turn_id in &turn_ids {
            if !turns.contains_key(turn_id.as_str()) {
                bail!("assistant message timeline block references missing Turn `{turn_id}`");
            }
        }

        let item_turn_ids = item_targets
            .iter()
            .map(|(turn_id, _)| turn_id.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let turn_items = if item_turn_ids.is_empty() {
            HashMap::new()
        } else {
            self.crud_store
                .list_turn_items_by_type_for_turns(item_turn_ids.as_slice(), "agent_message")
                .await?
        };
        let mut items = HashMap::new();
        for (turn_id, item_id) in &item_targets {
            let item = turn_items
                .get(turn_id.as_str())
                .and_then(|items| items.iter().find(|item| item.item_id() == item_id))
                .with_context(|| {
                    format!("assistant message item `{item_id}` for turn `{turn_id}` was not found")
                })?;
            if !matches!(item, TurnItem::AgentMessage { .. }) {
                bail!("assistant message timeline target is not an Agent message");
            }
            items.insert((turn_id.clone(), item_id.clone()), item.clone());
        }

        let authors = self
            .timeline_row_authors(&turns, turn_ids.as_slice())
            .await?;
        Ok(AssistantMessageTimelineBatch { items, authors })
    }

    async fn timeline_row_authors(
        &self,
        turns: &HashMap<String, Turn>,
        turn_ids: &[String],
    ) -> Result<HashMap<String, pioneer_protocol::TurnAuthorSnapshot>> {
        let database = self.crud_store.database_connection();
        let responses = pioneer_crud::load_agent_turn_responses_for_turns(&database, turn_ids)
            .await?;
        let responses_by_turn = responses
            .iter()
            .map(|response| (response.turn_id.as_str(), response))
            .collect::<HashMap<_, _>>();
        let direct_authors_by_turn = turn_ids
            .iter()
            .filter_map(|turn_id| {
                turns
                    .get(turn_id.as_str())
                    .filter(|turn| turn.turn_kind == pioneer_protocol::TurnKind::TaskRun)
                    .and_then(exact_agent_turn_input_author)
                    .map(|author| (turn_id.clone(), author))
            })
            .collect::<HashMap<_, _>>();
        let execution_ids = responses
            .iter()
            .map(|response| response.execution_id.clone())
            .collect::<Vec<_>>();
        let projected_by_execution =
            pioneer_crud::load_agent_authors_for_executions(&database, execution_ids.as_slice())
                .await?;
        let mut authors = HashMap::new();
        for turn_id in turn_ids {
            let author = if let Some(response) = responses_by_turn.get(turn_id.as_str()) {
                projected_by_execution
                    .get(response.execution_id.as_str())
                    .filter(|projected| {
                        projected.presentation_snapshot_id == response.presentation_snapshot_id
                    })
                    .map(|projected| projected.author.clone())
            } else {
                direct_authors_by_turn.get(turn_id.as_str()).cloned()
            };
            if let Some(author) = author {
                authors.insert(turn_id.clone(), author);
            }
        }
        Ok(authors)
    }

    async fn descendant_pending_request_blocks(
        &self,
        workspace_id: &str,
        thread_id: &str,
        approval_scope: Option<&ThreadTimelineApprovalScope>,
    ) -> Result<Vec<TimelineBlock>> {
        if approval_scope.is_some_and(|scope| !scope.can_observe_agent_requests) {
            return Ok(Vec::new());
        }
        let descendant_thread_ids = self.descendant_task_thread_ids(thread_id).await?;
        if descendant_thread_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut blocks = Vec::new();
        for descendant_thread_id in descendant_thread_ids {
            let requests = self
                .crud_store
                .list_cli_runtime_pending_requests(CliRuntimePendingRequestListFilter {
                    workspace_id: Some(workspace_id.to_owned()),
                    thread_id: Some(descendant_thread_id.clone()),
                    open_only: true,
                    ..Default::default()
                })
                .await?;
            for request in requests {
                if cli_runtime_pending_request_visible_to_scope(&request, approval_scope) {
                    let author = match request.turn_id.as_deref() {
                        Some(turn_id) => {
                            self.timeline_agent_author_for_turn(
                                descendant_thread_id.as_str(),
                                turn_id,
                            )
                            .await?
                        }
                        None => None,
                    };
                    if let Some(block) = pending_request_proxy_block(request, author)? {
                        blocks.push(block);
                    }
                }
            }
        }
        blocks.sort_by(|left, right| {
            left.sort_key
                .cmp(&right.sort_key)
                .then_with(|| left.block_id.cmp(&right.block_id))
        });
        Ok(blocks)
    }

    async fn descendant_task_thread_ids(&self, thread_id: &str) -> Result<Vec<String>> {
        let root_thread_id = self
            .crud_store
            .get_task_thread_lineage(thread_id)
            .await?
            .map(|lineage| lineage.root_thread_id)
            .unwrap_or_else(|| thread_id.to_owned());
        let lineage_rows = self
            .crud_store
            .list_task_thread_lineage_by_root_thread(root_thread_id.as_str())
            .await?;
        if lineage_rows.is_empty() {
            return Ok(Vec::new());
        }

        let parent_by_child = parent_by_child_thread_id(lineage_rows.as_slice());
        let mut descendants = Vec::new();
        let mut seen = HashSet::<String>::new();
        for lineage in lineage_rows {
            if lineage.child_thread_id != thread_id
                && timeline_lineage_descends_from(
                    lineage.child_thread_id.as_str(),
                    thread_id,
                    &parent_by_child,
                )
                && seen.insert(lineage.child_thread_id.clone())
            {
                descendants.push(lineage.child_thread_id);
            }
        }
        Ok(descendants)
    }

    async fn thread_timeline_block_from_row(
        &self,
        row: thread_timeline_block::Model,
        approval_scope: Option<&ThreadTimelineApprovalScope>,
        user_messages: &UserMessageTimelineBatch,
        assistant_messages: &AssistantMessageTimelineBatch,
        action_projection: Option<&pioneer_crud::AgentActionTimelineProjection>,
    ) -> Result<Option<TimelineBlock>> {
        let kind = match row.block_kind.as_str() {
            BLOCK_KIND_USER_MESSAGE => user_message_timeline_block_kind(
                &row,
                user_messages,
                action_projection.and_then(|projection| projection.route.clone()),
            )?,
            BLOCK_KIND_TURN_WORK => {
                let author = row
                    .turn_id
                    .as_deref()
                    .and_then(|turn_id| assistant_messages.authors.get(turn_id))
                    .cloned();
                self.turn_work_timeline_block_kind(&row, author).await?
            }
            BLOCK_KIND_DETACHED_TASK_RUN => {
                let author = row
                    .turn_id
                    .as_deref()
                    .and_then(|turn_id| assistant_messages.authors.get(turn_id))
                    .cloned();
                self.detached_task_run_timeline_block_kind(&row, author)
                    .await?
            }
            BLOCK_KIND_ASSISTANT_MESSAGE => {
                self.assistant_message_timeline_block_kind(
                    &row,
                    assistant_messages,
                    action_projection,
                )?
            }
            BLOCK_KIND_RUNNING => TimelineBlockKind::TurnState {
                state: TurnWorkState::Running,
                message: None,
                author: row
                    .turn_id
                    .as_deref()
                    .and_then(|turn_id| assistant_messages.authors.get(turn_id))
                    .cloned(),
                route: action_projection.and_then(|projection| projection.route.clone()),
            },
            BLOCK_KIND_APPROVAL => {
                let author = row
                    .turn_id
                    .as_deref()
                    .and_then(|turn_id| assistant_messages.authors.get(turn_id))
                    .cloned();
                let Some(kind) = self
                    .approval_timeline_block_kind(&row, approval_scope, author)
                    .await?
                else {
                    return Ok(None);
                };
                kind
            }
            BLOCK_KIND_SYSTEM => {
                let author = row
                    .turn_id
                    .as_deref()
                    .and_then(|turn_id| assistant_messages.authors.get(turn_id))
                    .cloned();
                terminal_turn_state_timeline_block_kind(&row, author)?
            }
            other => {
                return Err(anyhow!(
                    "unsupported thread timeline block kind `{other}` for block `{}`",
                    row.block_id
                ));
            }
        };

        Ok(Some(TimelineBlock {
            workspace_id: row.workspace_id,
            thread_id: row.thread_id,
            block_id: row.block_id,
            turn_id: row.turn_id,
            sort_key: row.sort_key,
            started_at_unix_ms: row.started_at.map(|value| value.timestamp_millis()),
            updated_at_unix_ms: Some(row.updated_at.timestamp_millis()),
            kind,
        }))
    }

    async fn approval_timeline_block_kind(
        &self,
        row: &thread_timeline_block::Model,
        approval_scope: Option<&ThreadTimelineApprovalScope>,
        author: Option<pioneer_protocol::TurnAuthorSnapshot>,
    ) -> Result<Option<TimelineBlockKind>> {
        let request_id = row
            .source_key
            .as_deref()
            .context("approval timeline block is missing source request id")?;
        let record = self
            .crud_store
            .get_cli_runtime_pending_request(request_id)
            .await
            .with_context(|| {
                format!("failed to load CLI runtime pending request `{request_id}`")
            })?;
        let Some(record) = record else {
            if approval_scope.is_some_and(|scope| !scope.can_observe_agent_requests) {
                return Ok(None);
            }
            return Ok(Some(TimelineBlockKind::PendingRequest {
                runtime_id: String::new(),
                request_id: request_id.to_owned(),
                status: CLIRuntimePendingRequestStatus::Expired,
                item_id: None,
                author,
                request: CLIRuntimePendingRequest {
                    kind: CLIRuntimeRequestKind::Other,
                    title: None,
                    message: None,
                    native_request_id: None,
                    payload: None,
                },
            }));
        };
        if !cli_runtime_pending_request_visible_to_scope(&record, approval_scope) {
            return Ok(None);
        }
        let request =
            serde_json::from_str::<CLIRuntimePendingRequest>(record.payload_json.as_str())
                .with_context(|| {
                    format!(
                        "failed to decode CLI runtime pending request `{}` payload",
                        record.request_id
                    )
                })?;

        Ok(Some(TimelineBlockKind::PendingRequest {
            runtime_id: record.runtime_id,
            request_id: record.request_id,
            status: parse_cli_runtime_pending_request_status(record.status.as_str()),
            item_id: record.native_item_id,
            author,
            request,
        }))
    }

    async fn turn_work_timeline_block_kind(
        &self,
        row: &thread_timeline_block::Model,
        author: Option<pioneer_protocol::TurnAuthorSnapshot>,
    ) -> Result<TimelineBlockKind> {
        let turn_id = row
            .turn_id
            .as_deref()
            .context("turn work timeline block is missing turn_id")?;
        let Some(work_projection) = self.crud_store.get_turn_work_projection(turn_id).await? else {
            return Err(anyhow!(
                "turn work projection for turn `{turn_id}` was not found"
            ));
        };

        Ok(TimelineBlockKind::TurnWork {
            work: self
                .turn_work_block_from_projection_with_author(work_projection, author)
                .await?,
        })
    }

    fn assistant_message_timeline_block_kind(
        &self,
        row: &thread_timeline_block::Model,
        batch: &AssistantMessageTimelineBatch,
        action_projection: Option<&pioneer_crud::AgentActionTimelineProjection>,
    ) -> Result<TimelineBlockKind> {
        let turn_id = row
            .turn_id
            .as_deref()
            .context("assistant message timeline block is missing turn_id")?;
        let item_id = row
            .source_key
            .as_deref()
            .context("assistant message timeline block is missing source_key")?;
        let item = batch
            .items
            .get(&(turn_id.to_owned(), item_id.to_owned()))
            .with_context(|| {
                format!("assistant message item `{item_id}` for turn `{turn_id}` was not batched")
            })?
            .clone();
        let author = batch.authors.get(turn_id).cloned();

        let TurnItem::AgentMessage {
            id, text, markdown, ..
        } = item
        else {
            return Err(anyhow!(
                "timeline block `{}` points to non-agent item `{item_id}`",
                row.block_id
            ));
        };
        let markdown = markdown.or_else(|| Some(markdown::parse_markdown_document(text.as_str())));

        Ok(TimelineBlockKind::AssistantMessage {
            item_id: id,
            text,
            status: TurnWorkItemStatus::Completed,
            markdown,
            author,
            route: action_projection.and_then(|projection| projection.route.clone()),
        })
    }

    async fn detached_task_run_timeline_block_kind(
        &self,
        row: &thread_timeline_block::Model,
        author: Option<pioneer_protocol::TurnAuthorSnapshot>,
    ) -> Result<TimelineBlockKind> {
        let turn_id = row
            .turn_id
            .as_deref()
            .context("detached task run timeline block is missing turn_id")?;
        let item_id = row
            .source_key
            .as_deref()
            .context("detached task run timeline block is missing source_key")?;
        let Some(item) = self.crud_store.get_turn_item(turn_id, item_id).await? else {
            return Err(anyhow!(
                "detached task run item `{item_id}` for turn `{turn_id}` was not found"
            ));
        };
        let TurnItem::Task { item: task } = item else {
            return Err(anyhow!(
                "timeline block `{}` points to non-task item `{item_id}`",
                row.block_id
            ));
        };
        Ok(TimelineBlockKind::DetachedTaskRun { task, author })
    }

    async fn turn_work_items_from_rows(
        &self,
        rows: &[turn_work_item_projection::Model],
    ) -> Result<Vec<TurnWorkItem>> {
        let item_ids = rows
            .iter()
            .map(|row| row.item_id.clone())
            .collect::<Vec<_>>();
        let mut items_by_id = self
            .crud_store
            .get_turn_items_by_ids(
                rows.first()
                    .map(|row| row.turn_id.as_str())
                    .unwrap_or_default(),
                item_ids.as_slice(),
            )
            .await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(mut item) = items_by_id.remove(row.item_id.as_str()) else {
                return Err(anyhow!(
                    "turn item `{}` for work row `{}` was not found",
                    row.item_id,
                    row.work_item_id
                ));
            };
            Self::normalize_item_markdown(&mut item);
            items.push(Self::turn_work_item_from_row(row, item)?);
        }
        Ok(items)
    }

    fn turn_work_item_from_row(
        row: &turn_work_item_projection::Model,
        item: TurnItem,
    ) -> Result<TurnWorkItem> {
        Ok(TurnWorkItem {
            work_item_id: row.work_item_id.clone(),
            item_id: row.item_id.clone(),
            turn_id: row.turn_id.clone(),
            order_key: row.order_key.clone(),
            source_sequence: row.source_sequence,
            source_updated_at_unix_micros: row.updated_at.timestamp_micros(),
            item_type: item.item_type(),
            status: parse_turn_work_item_status(row.status.as_str()),
            started_at_unix_ms: row.started_at.map(|value| value.timestamp_millis()),
            completed_at_unix_ms: row.completed_at.map(|value| value.timestamp_millis()),
            item,
            metadata: parse_optional_metadata(row.metadata_json.as_str())?,
        })
    }

    pub(super) async fn turn_work_block_from_projection(
        &self,
        projection: turn_work_projection::Model,
    ) -> Result<TurnWorkBlock> {
        let author = self
            .timeline_agent_author_for_turn(
                projection.thread_id.as_str(),
                projection.turn_id.as_str(),
            )
            .await?;
        self.turn_work_block_from_projection_with_author(projection, author)
            .await
    }

    async fn turn_work_block_from_projection_with_author(
        &self,
        projection: turn_work_projection::Model,
        author: Option<pioneer_protocol::TurnAuthorSnapshot>,
    ) -> Result<TurnWorkBlock> {
        let first_cursor = match projection.first_work_item_id.as_deref() {
            Some(work_item_id) => self.turn_work_item_cursor(work_item_id).await?,
            None => None,
        };
        let last_cursor = match projection.last_work_item_id.as_deref() {
            Some(work_item_id) => self.turn_work_item_cursor(work_item_id).await?,
            None => None,
        };

        let visible_work_count = projection.visible_work_count.max(0) as u64;
        let agent_work_graph = self
            .crud_store
            .get_agent_work_graph_projection_for_turn(projection.turn_id.as_str())
            .await?;
        Ok(TurnWorkBlock {
            turn_id: projection.turn_id,
            presentation: parse_turn_work_presentation(projection.presentation.as_str()),
            state: parse_turn_work_state(projection.state.as_str()),
            agent_work_graph,
            author,
            started_at_unix_ms: projection.started_at.map(|value| value.timestamp_millis()),
            completed_at_unix_ms: projection
                .completed_at
                .map(|value| value.timestamp_millis()),
            elapsed_ms: projection
                .elapsed_ms
                .and_then(|value| u64::try_from(value).ok()),
            work_count: projection.work_count.max(0) as u64,
            visible_work_count,
            hidden_work_count: projection.hidden_work_count.max(0) as u64,
            has_more_before: visible_work_count > 0,
            has_more_after: visible_work_count > 0,
            before_cursor: first_cursor,
            after_cursor: last_cursor,
            first_work_item_id: projection.first_work_item_id,
            last_work_item_id: projection.last_work_item_id,
        })
    }

    pub(super) async fn timeline_agent_author_for_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<pioneer_protocol::TurnAuthorSnapshot>> {
        let Some((_, turn)) = self.crud_store.get_turn(thread_id, turn_id).await? else {
            return Ok(None);
        };
        let turn_id = turn_id.to_owned();
        let turns = HashMap::from([(turn_id.clone(), turn)]);
        Ok(self
            .timeline_row_authors(&turns, std::slice::from_ref(&turn_id))
            .await?
            .remove(turn_id.as_str()))
    }

    async fn turn_work_item_cursor(
        &self,
        work_item_id: &str,
    ) -> Result<Option<pioneer_protocol::TimelineCursor>> {
        let Some(item_projection) = self
            .crud_store
            .get_turn_work_item_projection(work_item_id)
            .await?
        else {
            return Ok(None);
        };
        encode_turn_work_cursor(
            SEMANTIC_TIMELINE_PROJECTION_VERSION,
            item_projection.thread_id.as_str(),
            item_projection.turn_id.as_str(),
            item_projection.work_item_id.as_str(),
            item_projection.order_key.as_str(),
        )
        .map(Some)
        .map_err(|error| anyhow!(error))
    }
}

fn turn_work_page_info(
    rows: &[turn_work_item_projection::Model],
    has_more_before: bool,
    has_more_after: bool,
) -> Result<TimelinePageInfo> {
    let before_cursor = rows
        .first()
        .map(|row| {
            encode_turn_work_cursor(
                SEMANTIC_TIMELINE_PROJECTION_VERSION,
                row.thread_id.as_str(),
                row.turn_id.as_str(),
                row.work_item_id.as_str(),
                row.order_key.as_str(),
            )
        })
        .transpose()
        .map_err(|error| anyhow!(error))?;
    let after_cursor = rows
        .last()
        .map(|row| {
            encode_turn_work_cursor(
                SEMANTIC_TIMELINE_PROJECTION_VERSION,
                row.thread_id.as_str(),
                row.turn_id.as_str(),
                row.work_item_id.as_str(),
                row.order_key.as_str(),
            )
        })
        .transpose()
        .map_err(|error| anyhow!(error))?;

    Ok(TimelinePageInfo {
        before_cursor,
        after_cursor,
        has_more_before,
        has_more_after,
    })
}

fn turn_work_projection_revision(projection: &turn_work_projection::Model) -> (i64, i64) {
    (
        projection.source_high_watermark,
        projection.updated_at.timestamp_micros(),
    )
}

fn user_message_timeline_block_kind(
    row: &thread_timeline_block::Model,
    batch: &UserMessageTimelineBatch,
    route: Option<pioneer_protocol::SafeRouteProvenance>,
) -> Result<TimelineBlockKind> {
    let turn_id = row
        .turn_id
        .as_deref()
        .or(row.source_key.as_deref())
        .context("user message timeline block is missing turn_id")?;
    let turn = batch.turns.get(turn_id).with_context(|| {
        format!("user message timeline block references missing Turn `{turn_id}`")
    })?;
    let inputs = if turn.message_deleted {
        Vec::new()
    } else {
        batch.inputs.get(turn_id).cloned().unwrap_or_default()
    };
    let (text, input_attachments) = user_message_text_and_attachments(inputs.as_slice());
    let attachments = if turn.message_deleted {
        Vec::new()
    } else {
        merge_user_message_attachments(
            input_attachments,
            batch.attachments.get(turn_id).cloned().unwrap_or_default(),
        )
    };
    let reply = turn
        .reply_to_turn_id
        .as_deref()
        .map(|reply_turn_id| {
            let target = batch.turns.get(reply_turn_id).with_context(|| {
                format!(
                    "reply target `{reply_turn_id}` for timeline Turn `{turn_id}` is unavailable"
                )
            })?;
            let text = if target.message_deleted {
                None
            } else {
                let target_inputs = batch
                    .inputs
                    .get(reply_turn_id)
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                let (text, _) = user_message_text_and_attachments(target_inputs);
                bounded_timeline_reply_text(text.as_str())
            };
            Ok::<_, anyhow::Error>(TimelineReplySummary {
                turn_id: reply_turn_id.to_owned(),
                author: target.author.clone(),
                text,
                deleted: target.message_deleted,
            })
        })
        .transpose()?;

    Ok(TimelineBlockKind::UserMessage {
        item_id: None,
        inputs,
        text,
        attachments,
        mode: turn.mode,
        author: turn.author.clone(),
        route,
        reply,
        mentions: turn.mentions.clone(),
        revision: turn.message_revision,
        edited: turn.message_revision > 0,
        deleted: turn.message_deleted,
    })
}

fn bounded_timeline_reply_text(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let mut bounded = text
        .chars()
        .take(TIMELINE_REPLY_TEXT_MAX_CHARS)
        .collect::<String>();
    if text.chars().count() > TIMELINE_REPLY_TEXT_MAX_CHARS {
        bounded.push('…');
    }
    Some(bounded)
}

fn user_message_text_and_attachments(input: &[UserInput]) -> (String, Vec<UserMessageAttachment>) {
    let mut text_parts = Vec::new();
    let mut attachments = Vec::new();

    for value in input {
        match value {
            UserInput::Text { text, .. } => {
                if !text.trim().is_empty() {
                    text_parts.push(text.clone());
                }
            }
            UserInput::Image { url } => {
                attachments.push(UserMessageAttachment::Image { url: url.clone() });
            }
            UserInput::LocalImage { path } => {
                attachments.push(UserMessageAttachment::LocalImage { path: path.clone() });
            }
            UserInput::File { url } => {
                attachments.push(UserMessageAttachment::File { url: url.clone() });
            }
            UserInput::LocalFile { path } => {
                attachments.push(UserMessageAttachment::LocalFile { path: path.clone() });
            }
            UserInput::Audio { url } => {
                attachments.push(UserMessageAttachment::Audio { url: url.clone() });
            }
            UserInput::LocalAudio { path } => {
                attachments.push(UserMessageAttachment::LocalAudio { path: path.clone() });
            }
            UserInput::Video { url } => {
                attachments.push(UserMessageAttachment::Video { url: url.clone() });
            }
            UserInput::LocalVideo { path } => {
                attachments.push(UserMessageAttachment::LocalVideo { path: path.clone() });
            }
            UserInput::Artifact { .. } => {}
            UserInput::Mention { name, .. } => {
                text_parts.push(format!("mention: {name}"));
            }
        }
    }

    (text_parts.join("\n"), attachments)
}

fn merge_user_message_attachments(
    mut primary: Vec<UserMessageAttachment>,
    secondary: Vec<UserMessageAttachment>,
) -> Vec<UserMessageAttachment> {
    for attachment in secondary {
        if let UserMessageAttachment::Artifact { artifact: incoming } = attachment {
            if let Some(UserMessageAttachment::Artifact { artifact: existing }) =
                primary.iter_mut().find(|candidate| {
                    matches!(
                        candidate,
                        UserMessageAttachment::Artifact { artifact }
                            if artifact.artifact_id == incoming.artifact_id
                                && artifact.version_id == incoming.version_id
                    )
                })
            {
                // The persisted user-message snapshot can carry a generated
                // preview while the fresh exact-version lookup deliberately
                // omits it. Artifact identity is the immutable
                // (artifact_id, version_id) pair, not the entire mutable
                // presentation snapshot. Refresh canonical metadata without
                // turning the two snapshots into duplicate chips.
                let preview = incoming
                    .preview
                    .clone()
                    .or_else(|| existing.preview.clone());
                *existing = incoming;
                existing.preview = preview;
            } else {
                primary.push(UserMessageAttachment::Artifact { artifact: incoming });
            }
        } else if !primary.contains(&attachment) {
            primary.push(attachment);
        }
    }
    primary
}

fn parse_turn_work_presentation(value: &str) -> TurnWorkPresentation {
    match value {
        "collapsed_after_final" => TurnWorkPresentation::CollapsedAfterFinal,
        "expanded_terminal_no_final" => TurnWorkPresentation::ExpandedTerminalNoFinal,
        "expanded_live" => TurnWorkPresentation::ExpandedLive,
        _ => TurnWorkPresentation::ExpandedLive,
    }
}

fn terminal_turn_state_timeline_block_kind(
    row: &thread_timeline_block::Model,
    author: Option<pioneer_protocol::TurnAuthorSnapshot>,
) -> Result<TimelineBlockKind> {
    terminal_turn_state_timeline_block_kind_from_metadata(row.metadata_json.as_str(), author)
}

fn terminal_turn_state_timeline_block_kind_from_metadata(
    metadata_json: &str,
    author: Option<pioneer_protocol::TurnAuthorSnapshot>,
) -> Result<TimelineBlockKind> {
    let metadata = parse_optional_metadata(metadata_json)?;
    let state = metadata
        .as_ref()
        .and_then(|value| value.get("state"))
        .and_then(JsonValue::as_str)
        .map(parse_turn_work_state)
        .unwrap_or(TurnWorkState::Running);
    let message = metadata
        .as_ref()
        .and_then(|value| value.get("message"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    Ok(TimelineBlockKind::TurnState {
        state,
        message,
        author,
        route: None,
    })
}

fn parse_turn_work_state(value: &str) -> TurnWorkState {
    match value {
        "starting" => TurnWorkState::Starting,
        "running" => TurnWorkState::Running,
        "waiting_for_approval" => TurnWorkState::WaitingForApproval,
        "stalled" => TurnWorkState::Stalled,
        "completed" => TurnWorkState::Completed,
        "blocked" => TurnWorkState::Blocked,
        "failed" => TurnWorkState::Failed,
        "interrupted" => TurnWorkState::Interrupted,
        _ => TurnWorkState::Running,
    }
}

fn parse_turn_work_item_status(value: &str) -> TurnWorkItemStatus {
    match value {
        "running" => TurnWorkItemStatus::Running,
        "blocked" => TurnWorkItemStatus::Blocked,
        "failed" => TurnWorkItemStatus::Failed,
        "cancelled" | "canceled" => TurnWorkItemStatus::Cancelled,
        "completed" => TurnWorkItemStatus::Completed,
        _ => TurnWorkItemStatus::Completed,
    }
}

fn parse_cli_runtime_pending_request_status(value: &str) -> CLIRuntimePendingRequestStatus {
    match value {
        "pending" => CLIRuntimePendingRequestStatus::Pending,
        "answered" => CLIRuntimePendingRequestStatus::Answered,
        "resolved" => CLIRuntimePendingRequestStatus::Resolved,
        "cancelled" => CLIRuntimePendingRequestStatus::Cancelled,
        "expired" => CLIRuntimePendingRequestStatus::Expired,
        _ => CLIRuntimePendingRequestStatus::Expired,
    }
}

fn cli_runtime_pending_request_visible_to_scope(
    _record: &CliRuntimePendingRequestRecord,
    approval_scope: Option<&ThreadTimelineApprovalScope>,
) -> bool {
    approval_scope.is_none_or(|scope| scope.can_observe_agent_requests)
}

fn pending_request_proxy_block(
    record: CliRuntimePendingRequestRecord,
    author: Option<pioneer_protocol::TurnAuthorSnapshot>,
) -> Result<Option<TimelineBlock>> {
    if record.status.is_terminal() {
        return Ok(None);
    }
    let Some(turn_id) = record.turn_id.clone() else {
        return Ok(None);
    };

    let request = serde_json::from_str::<CLIRuntimePendingRequest>(record.payload_json.as_str())
        .with_context(|| {
            format!(
                "failed to decode CLI runtime pending request `{}` payload",
                record.request_id
            )
        })?;
    let sort_key = pending_request_proxy_sort_key(&record, turn_id.as_str());

    Ok(Some(TimelineBlock {
        workspace_id: record.workspace_id,
        thread_id: record.thread_id,
        block_id: approval_block_id(turn_id.as_str(), record.request_id.as_str()),
        turn_id: Some(turn_id.clone()),
        sort_key,
        started_at_unix_ms: Some(record.created_at.timestamp_millis()),
        updated_at_unix_ms: Some(record.updated_at.timestamp_millis()),
        kind: TimelineBlockKind::PendingRequest {
            runtime_id: record.runtime_id,
            request_id: record.request_id,
            status: CLIRuntimePendingRequestStatus::Pending,
            item_id: record.native_item_id,
            author,
            request,
        },
    }))
}

fn pending_request_proxy_sort_key(
    record: &CliRuntimePendingRequestRecord,
    turn_id: &str,
) -> String {
    format!(
        "{:020}:{}:150:approval:{}",
        record.created_at.timestamp_millis().max(0),
        turn_id,
        record.request_id
    )
}

fn parent_by_child_thread_id(lineage_rows: &[TaskThreadLineage]) -> HashMap<String, String> {
    let mut parent_by_child = HashMap::new();
    for lineage in lineage_rows {
        parent_by_child.insert(
            lineage.child_thread_id.clone(),
            lineage.parent_thread_id.clone(),
        );
    }
    parent_by_child
}

fn timeline_lineage_descends_from(
    child_thread_id: &str,
    ancestor_thread_id: &str,
    parent_by_child: &HashMap<String, String>,
) -> bool {
    let mut current = child_thread_id;
    for _ in 0..=parent_by_child.len() {
        let Some(parent) = parent_by_child.get(current) else {
            return false;
        };
        if parent == ancestor_thread_id {
            return true;
        }
        if parent == current {
            return false;
        }
        current = parent.as_str();
    }
    false
}

fn append_timeline_blocks_dedup(blocks: &mut Vec<TimelineBlock>, extra: Vec<TimelineBlock>) {
    if extra.is_empty() {
        return;
    }
    let mut existing = blocks
        .iter()
        .map(|block| block.block_id.clone())
        .collect::<HashSet<_>>();
    blocks.extend(
        extra
            .into_iter()
            .filter(|block| existing.insert(block.block_id.clone())),
    );
}

fn parse_optional_metadata(value: &str) -> Result<Option<JsonValue>> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    let parsed: JsonValue = serde_json::from_str(value)
        .with_context(|| "failed to decode work item projection metadata")?;
    match &parsed {
        JsonValue::Null => Ok(None),
        JsonValue::Object(map) if map.is_empty() => Ok(None),
        _ => Ok(Some(parsed)),
    }
}

#[cfg(test)]
mod timeline_handler_unit_tests {
    use super::*;

    fn agent_authored_turn() -> Turn {
        let presentation = pioneer_protocol::AgentPresentationSnapshot {
            agent_identity_id: pioneer_protocol::AgentIdentityId::new("AAAAAAAAAAAAAAAAAAAAA")
                .unwrap(),
            agent_execution_id: pioneer_protocol::AgentExecutionId::new("EAAAAAAAAAAAAAAAAAAAA")
                .unwrap(),
            identity_source_kind: pioneer_protocol::AgentIdentitySourceKind::NativeAgent,
            identity_source_revision: 1,
            display_name: "Codex CLI".to_owned(),
            nickname: "codex".to_owned(),
            avatar_revision: Some("agent-avatar".to_owned()),
            role_label: None,
        };
        Turn {
            id: "turn".to_owned(),
            status: pioneer_protocol::TurnStatus::InProgress,
            turn_kind: pioneer_protocol::TurnKind::TaskRun,
            origin: pioneer_protocol::TurnOrigin::DetachedTask,
            mode: pioneer_protocol::ThreadMode::Agent,
            author: Some(presentation.to_turn_author_snapshot()),
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::compile_turn_permission_profile(
                pioneer_protocol::TurnPermissionMode::FullAccess,
                pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
            ),
        }
    }

    fn artifact_attachment(
        status: pioneer_protocol::ArtifactStatus,
        preview: Option<pioneer_protocol::ArtifactPreviewRef>,
    ) -> UserMessageAttachment {
        UserMessageAttachment::Artifact {
            artifact: pioneer_protocol::ArtifactRef {
                artifact_id: "artifact".to_owned(),
                version_id: Some("version".to_owned()),
                display_name: "attachment.png".to_owned(),
                kind: pioneer_protocol::ArtifactKind::Image,
                mime_type: Some("image/png".to_owned()),
                size_bytes: Some(42),
                sha256: Some("sha256".to_owned()),
                status,
                preview,
            },
        }
    }

    fn pending_request_for(principal_id: &str, session_id: &str) -> CliRuntimePendingRequestRecord {
        let now = chrono::Utc::now().fixed_offset();
        CliRuntimePendingRequestRecord {
            request_id: "approval-request".to_owned(),
            runtime_id: "codex".to_owned(),
            runtime_kind: "codex".to_owned(),
            workspace_id: "workspace".to_owned(),
            thread_id: "thread".to_owned(),
            turn_id: Some("turn".to_owned()),
            native_thread_id: None,
            native_turn_id: None,
            native_item_id: None,
            request_kind: "command_approval".to_owned(),
            payload_json: "{}".to_owned(),
            status: StoredCliRuntimePendingRequestStatus::Pending,
            response_json: None,
            authorization_binding: Some(pioneer_crud::CliRuntimeRequestAuthorizationBinding {
                initiating_principal_id: principal_id.to_owned(),
                initiating_session_id: session_id.to_owned(),
                initiating_session_generation: 0,
                authorization_context_fingerprint: "a".repeat(64),
            }),
            responding_principal_id: None,
            responding_session_id: None,
            response_authorization_revision: None,
            delivery_attempts: 0,
            delivery_error: None,
            response_contains_secret: false,
            created_at: now,
            updated_at: now,
            resolved_at: None,
        }
    }

    #[test]
    fn approval_payload_visibility_follows_atomic_observe_action_not_initiator_identity() {
        let record = pending_request_for("principal-a", "session-a");
        let observer = ThreadTimelineApprovalScope {
            can_observe_agent_requests: true,
        };
        let reader_without_agent_observe = ThreadTimelineApprovalScope {
            can_observe_agent_requests: false,
        };

        assert!(cli_runtime_pending_request_visible_to_scope(
            &record,
            Some(&observer)
        ));
        assert!(!cli_runtime_pending_request_visible_to_scope(
            &record,
            Some(&reader_without_agent_observe)
        ));
        assert!(
            cli_runtime_pending_request_visible_to_scope(&record, None),
            "trusted internal projections may include already-authorized approvals"
        );
    }

    #[test]
    fn exact_agent_input_author_is_available_before_execution_graph_projection() {
        let turn = agent_authored_turn();

        assert_eq!(exact_agent_turn_input_author(&turn), turn.author);
    }

    #[test]
    fn agent_input_author_without_immutable_presentation_is_omitted() {
        let mut turn = agent_authored_turn();
        turn.author.as_mut().unwrap().agent = None;

        assert!(exact_agent_turn_input_author(&turn).is_none());
    }

    #[test]
    fn terminal_system_row_metadata_preserves_state_and_message() {
        assert_eq!(
            terminal_turn_state_timeline_block_kind_from_metadata(
                r#"{"state":"interrupted","message":"stopped by user"}"#,
                None,
            )
            .unwrap(),
            TimelineBlockKind::TurnState {
                state: TurnWorkState::Interrupted,
                message: Some("stopped by user".to_owned()),
                author: None,
                route: None,
            }
        );
    }

    #[test]
    fn attachment_merge_deduplicates_artifact_snapshots_by_exact_version() {
        let preview = pioneer_protocol::ArtifactPreviewRef {
            projection_kind: pioneer_protocol::ArtifactProjectionKind::Thumbnail,
            status: pioneer_protocol::ArtifactProjectionStatus::Ready,
            artifact_id: "artifact".to_owned(),
            version_id: "version".to_owned(),
            blob_id: Some("preview-blob".to_owned()),
            mime_type: Some("image/png".to_owned()),
            size_bytes: Some(12),
            sha256: Some("preview-sha256".to_owned()),
        };
        let merged = merge_user_message_attachments(
            vec![artifact_attachment(
                pioneer_protocol::ArtifactStatus::Pending,
                Some(preview.clone()),
            )],
            vec![artifact_attachment(
                pioneer_protocol::ArtifactStatus::Ready,
                None,
            )],
        );

        assert_eq!(merged.len(), 1);
        assert!(matches!(
            merged.as_slice(),
            [UserMessageAttachment::Artifact { artifact }]
                if artifact.status == pioneer_protocol::ArtifactStatus::Ready
                    && artifact.preview.as_ref() == Some(&preview)
        ));
    }
}
