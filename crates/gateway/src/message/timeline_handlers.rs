use super::timeline_cursor::{
    ResolvedTimelineAnchor, TimelineLimitPolicy, encode_thread_timeline_cursor,
    encode_turn_work_cursor, resolve_thread_timeline_anchor, resolve_turn_work_anchor,
    validate_timeline_limit,
};
use super::*;
use anyhow::{Context, Result, anyhow};
use pioneer_crud::{
    BLOCK_KIND_APPROVAL, BLOCK_KIND_ASSISTANT_MESSAGE, BLOCK_KIND_RUNNING, BLOCK_KIND_SYSTEM,
    BLOCK_KIND_TURN_WORK, BLOCK_KIND_USER_MESSAGE, CliRuntimePendingRequestListFilter,
    ProjectionPageAnchor, SEMANTIC_TIMELINE_PROJECTION_VERSION, WORK_VISIBILITY_VISIBLE,
    approval_block_id,
};
use pioneer_entity::{thread_timeline_block, turn_work_item_projection, turn_work_projection};
use pioneer_protocol::{
    CLIRuntimePendingRequest, CLIRuntimePendingRequestStatus, CLIRuntimeRequestKind,
    TaskThreadLineage, ThreadTimelinePageParams, ThreadTimelinePageResponse, TimelineBlock,
    TimelineBlockKind, TimelinePageInfo, TurnItem, TurnWorkBlock, TurnWorkItem, TurnWorkItemStatus,
    TurnWorkPageParams, TurnWorkPageResponse, TurnWorkPresentation, TurnWorkState, UserInput,
    UserMessageAttachment,
};

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

impl MessageProcessor {
    pub(super) async fn thread_timeline_page(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ThreadTimelinePageParams,
    ) {
        if params.thread_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
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
                    JsonRpcErrorResponse::new(
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
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load thread: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let rows_page = match self
            .load_thread_timeline_rows(params.thread_id.as_str(), anchor, limit)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
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
                    JsonRpcErrorResponse::new(
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
        let mut blocks = match self.thread_timeline_blocks_from_rows(rows_page.rows).await {
            Ok(blocks) => blocks,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
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
            .descendant_pending_request_blocks(workspace_id.as_str(), requested_thread_id.as_str())
            .await
        {
            Ok(blocks) => blocks,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
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
                    JsonRpcErrorResponse::new(
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
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnWorkPageParams,
    ) {
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
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
            Ok(Some(projection)) if projection.thread_id == params.thread_id => projection,
            Ok(Some(_)) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
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
                    JsonRpcErrorResponse::new(
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
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to load turn work projection: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let rows_page = match self
            .load_turn_work_rows(params.turn_id.as_str(), anchor, limit)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
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
            rows_page.rows.as_slice(),
            rows_page.has_more_before,
            rows_page.has_more_after,
        ) {
            Ok(page_info) => page_info,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to encode turn work cursors: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let items = match self.turn_work_items_from_rows(rows_page.rows).await {
            Ok(items) => items,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to materialize turn work items: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let workspace_id = work_projection.workspace_id.clone();
        let work = match self.turn_work_block_from_projection(work_projection).await {
            Ok(work) => work,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
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
            work,
            items,
            page: page_info,
        };
        let response = match JsonRpcResponse::from_result(request_id, &response_payload) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
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

    async fn load_thread_timeline_rows(
        &self,
        thread_id: &str,
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
                self.load_thread_timeline_rows_around(thread_id, sort_key.as_str(), limit)
                    .await
            }
        }
    }

    async fn load_thread_timeline_rows_around(
        &self,
        thread_id: &str,
        sort_key: &str,
        limit: u64,
    ) -> Result<ThreadTimelineRowsPage> {
        let before_limit = limit / 2;
        let mut before_rows = self
            .crud_store
            .list_thread_timeline_projection_page(
                thread_id,
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
            .find_thread_timeline_projection_block_by_sort_key(thread_id, sort_key)
            .await?;
        let anchor_len = u64::from(anchor_row.is_some());
        let after_limit = limit
            .saturating_sub(u64::try_from(before_rows.len()).unwrap_or(u64::MAX))
            .saturating_sub(anchor_len);
        let mut after_rows = self
            .crud_store
            .list_thread_timeline_projection_page(
                thread_id,
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
    ) -> Result<Vec<TimelineBlock>> {
        let mut blocks = Vec::with_capacity(rows.len());
        for row in rows {
            blocks.push(self.thread_timeline_block_from_row(row).await?);
        }
        Ok(blocks)
    }

    async fn descendant_pending_request_blocks(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<Vec<TimelineBlock>> {
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
                    thread_id: Some(descendant_thread_id),
                    status: Some(StoredCliRuntimePendingRequestStatus::Pending),
                    ..Default::default()
                })
                .await?;
            for request in requests {
                if let Some(block) = pending_request_proxy_block(request)? {
                    blocks.push(block);
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
    ) -> Result<TimelineBlock> {
        let kind = match row.block_kind.as_str() {
            BLOCK_KIND_USER_MESSAGE => self.user_message_timeline_block_kind(&row).await?,
            BLOCK_KIND_TURN_WORK => self.turn_work_timeline_block_kind(&row).await?,
            BLOCK_KIND_ASSISTANT_MESSAGE => {
                self.assistant_message_timeline_block_kind(&row).await?
            }
            BLOCK_KIND_RUNNING => TimelineBlockKind::TurnState {
                state: TurnWorkState::Running,
                message: None,
            },
            BLOCK_KIND_APPROVAL => self.approval_timeline_block_kind(&row).await?,
            BLOCK_KIND_SYSTEM => terminal_turn_state_timeline_block_kind(&row)?,
            other => {
                return Err(anyhow!(
                    "unsupported thread timeline block kind `{other}` for block `{}`",
                    row.block_id
                ));
            }
        };

        Ok(TimelineBlock {
            workspace_id: row.workspace_id,
            thread_id: row.thread_id,
            block_id: row.block_id,
            turn_id: row.turn_id,
            sort_key: row.sort_key,
            started_at_unix_ms: row.started_at.map(|value| value.timestamp_millis()),
            updated_at_unix_ms: Some(row.updated_at.timestamp_millis()),
            kind,
        })
    }

    async fn approval_timeline_block_kind(
        &self,
        row: &thread_timeline_block::Model,
    ) -> Result<TimelineBlockKind> {
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
            return Ok(TimelineBlockKind::PendingRequest {
                runtime_id: String::new(),
                request_id: request_id.to_owned(),
                status: CLIRuntimePendingRequestStatus::Expired,
                item_id: None,
                request: CLIRuntimePendingRequest {
                    kind: CLIRuntimeRequestKind::Other,
                    title: None,
                    message: None,
                    native_request_id: None,
                    payload: None,
                },
            });
        };
        let request =
            serde_json::from_str::<CLIRuntimePendingRequest>(record.payload_json.as_str())
                .with_context(|| {
                    format!(
                        "failed to decode CLI runtime pending request `{}` payload",
                        record.request_id
                    )
                })?;

        Ok(TimelineBlockKind::PendingRequest {
            runtime_id: record.runtime_id,
            request_id: record.request_id,
            status: parse_cli_runtime_pending_request_status(record.status.as_str()),
            item_id: record.native_item_id,
            request,
        })
    }

    async fn user_message_timeline_block_kind(
        &self,
        row: &thread_timeline_block::Model,
    ) -> Result<TimelineBlockKind> {
        let turn_id = row
            .turn_id
            .as_deref()
            .or(row.source_key.as_deref())
            .context("user message timeline block is missing turn_id")?;
        let inputs = self.crud_store.get_turn_inputs(turn_id).await?;
        let (text, input_attachments) = user_message_text_and_attachments(inputs.as_slice());
        let item_attachments = self.user_message_item_attachments(turn_id).await?;
        Ok(TimelineBlockKind::UserMessage {
            item_id: None,
            inputs,
            text,
            attachments: merge_user_message_attachments(input_attachments, item_attachments),
        })
    }

    async fn user_message_item_attachments(
        &self,
        turn_id: &str,
    ) -> Result<Vec<UserMessageAttachment>> {
        let items = self
            .crud_store
            .list_turn_items_by_type(turn_id, "user_message")
            .await?;
        let mut attachments = Vec::new();
        for item in items {
            if let TurnItem::UserMessage {
                attachments: item_attachments,
                ..
            } = item
            {
                attachments.extend(item_attachments);
            }
        }
        Ok(attachments)
    }

    async fn turn_work_timeline_block_kind(
        &self,
        row: &thread_timeline_block::Model,
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
                .turn_work_block_from_projection(work_projection)
                .await?,
        })
    }

    async fn assistant_message_timeline_block_kind(
        &self,
        row: &thread_timeline_block::Model,
    ) -> Result<TimelineBlockKind> {
        let turn_id = row
            .turn_id
            .as_deref()
            .context("assistant message timeline block is missing turn_id")?;
        let item_id = row
            .source_key
            .as_deref()
            .context("assistant message timeline block is missing source_key")?;
        let Some(item) = self.crud_store.get_turn_item(turn_id, item_id).await? else {
            return Err(anyhow!(
                "assistant message item `{item_id}` for turn `{turn_id}` was not found"
            ));
        };

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
        })
    }

    async fn turn_work_items_from_rows(
        &self,
        rows: Vec<turn_work_item_projection::Model>,
    ) -> Result<Vec<TurnWorkItem>> {
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(self.turn_work_item_from_row(row).await?);
        }
        Ok(items)
    }

    async fn turn_work_item_from_row(
        &self,
        row: turn_work_item_projection::Model,
    ) -> Result<TurnWorkItem> {
        let Some(mut item) = self
            .crud_store
            .get_turn_item(row.turn_id.as_str(), row.item_id.as_str())
            .await?
        else {
            return Err(anyhow!(
                "turn item `{}` for work row `{}` was not found",
                row.item_id,
                row.work_item_id
            ));
        };
        Self::normalize_item_markdown(&mut item);

        Ok(TurnWorkItem {
            work_item_id: row.work_item_id,
            item_id: row.item_id,
            turn_id: row.turn_id,
            order_key: row.order_key,
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
        let first_cursor = match projection.first_work_item_id.as_deref() {
            Some(work_item_id) => self.turn_work_item_cursor(work_item_id).await?,
            None => None,
        };
        let last_cursor = match projection.last_work_item_id.as_deref() {
            Some(work_item_id) => self.turn_work_item_cursor(work_item_id).await?,
            None => None,
        };

        let visible_work_count = projection.visible_work_count.max(0) as u64;
        Ok(TurnWorkBlock {
            turn_id: projection.turn_id,
            presentation: parse_turn_work_presentation(projection.presentation.as_str()),
            state: parse_turn_work_state(projection.state.as_str()),
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
            UserInput::Artifact { artifact_id, .. } => {
                text_parts.push(format!("artifact: {artifact_id}"));
            }
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
        if !primary.contains(&attachment) {
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
) -> Result<TimelineBlockKind> {
    terminal_turn_state_timeline_block_kind_from_metadata(row.metadata_json.as_str())
}

fn terminal_turn_state_timeline_block_kind_from_metadata(
    metadata_json: &str,
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
    Ok(TimelineBlockKind::TurnState { state, message })
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

fn pending_request_proxy_block(
    record: CliRuntimePendingRequestRecord,
) -> Result<Option<TimelineBlock>> {
    if record.status != StoredCliRuntimePendingRequestStatus::Pending {
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

    #[test]
    fn terminal_system_row_metadata_preserves_state_and_message() {
        assert_eq!(
            terminal_turn_state_timeline_block_kind_from_metadata(
                r#"{"state":"interrupted","message":"stopped by user"}"#,
            )
            .unwrap(),
            TimelineBlockKind::TurnState {
                state: TurnWorkState::Interrupted,
                message: Some("stopped by user".to_owned()),
            }
        );
    }
}
