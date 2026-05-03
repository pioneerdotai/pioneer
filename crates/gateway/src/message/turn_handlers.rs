use super::*;

impl MessageProcessor {
    pub(super) async fn turn_start(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnStartParams,
    ) {
        if params.thread_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` is required",
                        methods::TURN_START
                    ),
                ),
            )
            .await;
            return;
        }

        if params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `turn_id` is required",
                        methods::TURN_START
                    ),
                ),
            )
            .await;
            return;
        }

        let outcome = match self.thread_manager.turn_start(connection_id, params).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to start turn: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self
            .crud_store
            .materialize_turn_start(
                &outcome.materialization.thread,
                outcome.materialization.sandbox_mode,
                &outcome.materialization.turn,
                &outcome.materialization.input,
            )
            .await
        {
            self.thread_manager
                .rollback_turn_start(outcome.rollback_context.clone())
                .await;

            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("failed to persist turn/start state: {error:#}"),
                ),
            )
            .await;
            return;
        }

        if let Err(error) = self
            .agent_manager
            .ensure_thread(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.workspace_id.as_str(),
            )
            .await
        {
            self.mark_turn_failed(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                format!("failed to prepare agent thread runtime: {error}"),
            )
            .await;
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("failed to prepare agent thread runtime: {error}"),
                ),
            )
            .await;
            return;
        }

        self.ensure_agent_listener_task(outcome.started_notification.thread_id.as_str())
            .await;

        let history = self
            .load_conversation_history(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
            )
            .await;

        let workspace_skill_policies = match self
            .crud_store
            .list_workspace_skill_policies(outcome.started_notification.workspace_id.as_str())
            .await
        {
            Ok(records) => records
                .into_iter()
                .map(|record| {
                    (
                        pioneer_skills::SkillPolicyKey::new(record.skill_slug, record.source_kind),
                        pioneer_agent::WorkspaceSkillPolicy {
                            enabled: record.enabled,
                            allow_implicit_invocation: record.allow_implicit_invocation,
                        },
                    )
                })
                .collect::<std::collections::HashMap<_, _>>(),
            Err(error) => {
                warn!(
                    workspace_id = outcome.started_notification.workspace_id,
                    error = %format!("{error:#}"),
                    "failed to load workspace skill policies; continuing with defaults"
                );
                std::collections::HashMap::new()
            }
        };

        if let Err(error) = self
            .agent_manager
            .start_turn(
                outcome.started_notification.thread_id.as_str(),
                outcome.started_notification.turn.id.as_str(),
                outcome.materialization.thread.mode,
                &outcome.materialization.thread.model,
                &outcome.materialization.thread.model_provider,
                workspace_skill_policies,
                outcome.materialization.input.clone(),
                history,
            )
            .await
        {
            self.mark_turn_failed(
                outcome.started_notification.thread_id.clone(),
                outcome.started_notification.turn.id.clone(),
                format!("failed to dispatch turn to agent runtime: {error}"),
            )
            .await;
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("failed to dispatch turn: {error}"),
                ),
            )
            .await;
            return;
        }

        self.session_manager
            .set_connection_workspace(
                connection_id,
                Some(outcome.started_notification.workspace_id.clone()),
            )
            .await;

        let response = match JsonRpcResponse::from_result(request_id, &outcome.response) {
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
                "failed to send turn/start response"
            );
            return;
        }

        let notification = match JsonRpcNotification::from_params(
            events::TURN_STARTED,
            &outcome.started_notification,
        ) {
            Ok(notification) => notification,
            Err(error) => {
                warn!(error = %error, "failed to encode turn/started notification");
                return;
            }
        };

        match serde_json::to_string(&notification) {
            Ok(payload) => {
                for notification_connection_id in outcome.started_notification_connection_ids {
                    if let Err(error) = self
                        .session_manager
                        .send_text(notification_connection_id, payload.clone())
                        .await
                    {
                        warn!(
                            connection_id = notification_connection_id,
                            error = %format!("{error:#}"),
                            "failed to send turn/started notification"
                        );
                    }
                }
            }
            Err(error) => {
                warn!(error = %error, "failed to serialize turn/started notification");
            }
        }

        self.emit_user_message_item_lifecycle(
            outcome.started_notification.workspace_id.as_str(),
            outcome.started_notification.thread_id.as_str(),
            outcome.started_notification.turn.id.as_str(),
            outcome.materialization.input.as_slice(),
        )
        .await;

        // Spawn background title generation on first turn (fire-and-forget) only for user-origin threads.
        if outcome.materialization.thread.name.is_none()
            && outcome.materialization.thread.origin_kind
                == pioneer_protocol::ThreadOriginKind::User
        {
            self.spawn_initial_thread_title_task(
                outcome.started_notification.thread_id.clone(),
                first_user_text(outcome.materialization.input.as_slice()),
            );
        }
    }

    pub(super) async fn turn_cancel(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnCancelParams,
    ) {
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_CANCEL
                    ),
                ),
            )
            .await;
            return;
        }

        let thread_id = params.thread_id.trim().to_owned();
        let turn_id = params.turn_id.trim().to_owned();
        let reason = params
            .reason
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("turn cancelled by user")
            .to_owned();

        let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("turn `{turn_id}` not found in thread `{thread_id}`"),
                ),
            )
            .await;
            return;
        };

        let subscribed = self
            .thread_manager
            .subscribed_connection_ids(thread_id.as_str())
            .await
            .contains(&connection_id);
        if !subscribed {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!(
                        "connection `{connection_id}` is not subscribed to thread `{thread_id}`"
                    ),
                ),
            )
            .await;
            return;
        }

        if turn.status != TurnStatus::InProgress {
            self.send_turn_cancel_response(
                connection_id,
                request_id,
                TurnCancelResponse {
                    thread_id,
                    workspace_id,
                    turn,
                },
            )
            .await;
            return;
        }

        match self
            .agent_manager
            .cancel_turn(thread_id.as_str(), turn_id.as_str(), reason.as_str())
            .await
        {
            Ok(()) => {}
            Err(pioneer_agent::AgentControlError::ThreadNotFound)
            | Err(pioneer_agent::AgentControlError::NoActiveTurn) => {
                warn!(
                    thread_id,
                    turn_id,
                    "agent runtime had no active turn during turn/cancel; terminalizing in gateway"
                );
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to cancel turn: {error}"),
                    ),
                )
                .await;
                return;
            }
        }

        if !self
            .mark_turn_interrupted(thread_id.clone(), turn_id.clone(), reason)
            .await
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("failed to interrupt turn `{turn_id}` in thread `{thread_id}`"),
                ),
            )
            .await;
            return;
        }

        let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(thread_id.as_str(), turn_id.as_str())
            .await
        else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!("turn `{turn_id}` disappeared after cancellation"),
                ),
            )
            .await;
            return;
        };

        self.send_turn_cancel_response(
            connection_id,
            request_id,
            TurnCancelResponse {
                thread_id,
                workspace_id,
                turn,
            },
        )
        .await;
    }

    async fn send_turn_cancel_response(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        response_payload: TurnCancelResponse,
    ) {
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
                "failed to send turn/cancel response"
            );
        }
    }

    pub(super) async fn turn_get(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnGetParams,
    ) {
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_GET
                    ),
                ),
            )
            .await;
            return;
        }

        let result = if let Some((workspace_id, turn)) = self
            .thread_manager
            .turn_get(params.thread_id.as_str(), params.turn_id.as_str())
            .await
        {
            Some(TurnGetResponse {
                thread_id: params.thread_id.clone(),
                workspace_id,
                turn,
            })
        } else {
            match self
                .crud_store
                .get_turn(params.thread_id.as_str(), params.turn_id.as_str())
                .await
            {
                Ok(Some((workspace_id, turn))) => Some(TurnGetResponse {
                    thread_id: params.thread_id.clone(),
                    workspace_id,
                    turn,
                }),
                Ok(None) => None,
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to fetch turn: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            }
        };

        let Some(result) = result else {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_REQUEST_CODE,
                    format!(
                        "turn `{}` in thread `{}` was not found",
                        params.turn_id, params.thread_id
                    ),
                ),
            )
            .await;
            return;
        };

        self.session_manager
            .set_connection_workspace(connection_id, Some(result.workspace_id.clone()))
            .await;

        let response = match JsonRpcResponse::from_result(request_id, &result) {
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
                "failed to send turn/get response"
            );
        }
    }

    pub(super) async fn turn_items(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnItemsParams,
    ) {
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_ITEMS
                    ),
                ),
            )
            .await;
            return;
        }

        let mut result = match self
            .crud_store
            .get_turn_item_events(params.thread_id.as_str(), params.turn_id.as_str())
            .await
        {
            Ok(Some(value)) => value,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "turn `{}` in thread `{}` was not found",
                            params.turn_id, params.thread_id
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
                        format!("failed to fetch turn items: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        Self::enrich_turn_item_events_markdown(result.events.as_mut_slice());

        self.session_manager
            .set_connection_workspace(connection_id, Some(result.workspace_id.clone()))
            .await;

        let response = match JsonRpcResponse::from_result(request_id, &result) {
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
                "failed to send turn/items response"
            );
        }
    }

    pub(super) async fn turn_timeline(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: TurnTimelineParams,
    ) {
        if params.thread_id.trim().is_empty() || params.turn_id.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `thread_id` and `turn_id` are required",
                        methods::TURN_TIMELINE
                    ),
                ),
            )
            .await;
            return;
        }

        let result = match self.compose_turn_timeline(params).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        "turn was not found",
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
                        format!("failed to compose turn timeline: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        self.session_manager
            .set_connection_workspace(connection_id, Some(result.workspace_id.clone()))
            .await;

        let response = match JsonRpcResponse::from_result(request_id, &result) {
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
                "failed to send turn/timeline response"
            );
        }
    }

    async fn compose_turn_timeline(
        &self,
        params: TurnTimelineParams,
    ) -> anyhow::Result<Option<TurnTimelineResponse>> {
        let Some(mut parent) = self
            .crud_store
            .get_turn_item_events(params.thread_id.as_str(), params.turn_id.as_str())
            .await?
        else {
            return Ok(None);
        };
        Self::enrich_turn_item_events_markdown(parent.events.as_mut_slice());

        let mut items = Vec::new();
        let mut task_anchor_ids = std::collections::BTreeSet::<String>::new();

        for event in parent.events.clone() {
            collect_task_id_from_turn_event(&event, &mut task_anchor_ids);
            items.push(timeline_item_for_turn_event(
                "parent",
                TimelineOriginKind::ParentTurn,
                TimelineLane::Parent,
                None,
                None,
                None,
                None,
                event,
            ));
        }

        if params.compose_tasks {
            let owned_tasks = self
                .task_runtime
                .service()
                .list_tasks(TaskListParams {
                    workspace_id: parent.workspace_id.clone(),
                    owner_kind: Some(pioneer_protocol::TaskOwnerKind::Thread),
                    owner_id: Some(params.thread_id.clone()),
                    parent_task_id: None,
                    root_task_id: None,
                    status: None,
                    limit: Some(500),
                })
                .await?
                .tasks;
            for task in owned_tasks {
                if task.created_by_turn_id.as_deref() == Some(params.turn_id.as_str()) {
                    task_anchor_ids.insert(task.id);
                }
            }

            let mut task_group_by_task_id = std::collections::BTreeMap::<String, String>::new();
            for anchor_task_id in &task_anchor_ids {
                task_group_by_task_id
                    .entry(anchor_task_id.clone())
                    .or_insert_with(|| anchor_task_id.clone());
                let descendant_tasks = self
                    .task_runtime
                    .service()
                    .list_tasks(TaskListParams {
                        workspace_id: parent.workspace_id.clone(),
                        owner_kind: None,
                        owner_id: None,
                        parent_task_id: None,
                        root_task_id: Some(anchor_task_id.clone()),
                        status: None,
                        limit: Some(500),
                    })
                    .await?
                    .tasks;
                for task in descendant_tasks {
                    task_group_by_task_id
                        .entry(task.id)
                        .or_insert_with(|| anchor_task_id.clone());
                }
            }

            let mut task_ids_to_compose = std::collections::BTreeSet::<String>::new();
            task_ids_to_compose.extend(task_anchor_ids.iter().cloned());
            task_ids_to_compose.extend(task_group_by_task_id.keys().cloned());

            for task_id in task_ids_to_compose {
                let grouped_task_id = task_group_by_task_id
                    .get(task_id.as_str())
                    .cloned()
                    .unwrap_or_else(|| task_id.clone());
                let task_events = self
                    .crud_store
                    .get_task_events(task_id.as_str(), None)
                    .await?;
                for event in task_events.events {
                    if !params.include_collapsed_task_events
                        && is_collapsible_task_event(event.event_type.as_str())
                    {
                        continue;
                    }
                    let run_id = event.run_id.clone();
                    items.push(TimelineItem {
                        id: format!("task:{}:{}", event.task_id, event.sequence),
                        origin: TimelineOrigin {
                            kind: TimelineOriginKind::TaskEvent,
                            task_id: Some(grouped_task_id.clone()),
                            run_id,
                            child_thread_id: event.thread_id.clone(),
                            child_turn_id: event.turn_id.clone(),
                            origin_event_id: Some(event.id.clone()),
                            origin_turn_item_id: None,
                            origin_sequence: event.sequence,
                            occurred_at: timeline_timestamp_ms(event.created_at),
                            lane: TimelineLane::Task,
                        },
                        payload: TimelinePayload::TaskEvent { event },
                    });
                }

                let max_child_items = params.max_child_items_per_task.unwrap_or(100) as usize;
                for lineage in self
                    .crud_store
                    .list_thread_lineage_for_task(task_id.as_str())
                    .await?
                {
                    let Some(mut child_items) = self
                        .crud_store
                        .get_turn_item_events(
                            lineage.child_thread_id.as_str(),
                            lineage.child_turn_id.as_str(),
                        )
                        .await?
                    else {
                        continue;
                    };
                    Self::enrich_turn_item_events_markdown(child_items.events.as_mut_slice());
                    for event in select_child_turn_events_for_timeline(
                        child_items.events,
                        params.include_collapsed_task_events,
                        max_child_items,
                    ) {
                        let lane = lane_for_turn_event(&event, true);
                        items.push(timeline_item_for_turn_event(
                            "child",
                            TimelineOriginKind::ChildTurn,
                            lane,
                            Some(grouped_task_id.clone()),
                            Some(lineage.task_run_id.clone()),
                            Some(lineage.child_thread_id.clone()),
                            Some(lineage.child_turn_id.clone()),
                            event,
                        ));
                    }
                }
            }
        }

        items.sort_by(|left, right| {
            left.origin
                .occurred_at
                .cmp(&right.origin.occurred_at)
                .then_with(|| {
                    source_priority(left.origin.kind).cmp(&source_priority(right.origin.kind))
                })
                .then_with(|| {
                    left.origin
                        .origin_sequence
                        .cmp(&right.origin.origin_sequence)
                })
                .then_with(|| left.id.cmp(&right.id))
        });
        items.dedup_by(|left, right| left.id == right.id);
        let last_sequence = items
            .iter()
            .map(|item| item.origin.origin_sequence)
            .max()
            .unwrap_or(parent.last_sequence);
        Ok(Some(TurnTimelineResponse {
            thread_id: params.thread_id,
            workspace_id: parent.workspace_id,
            turn_id: params.turn_id,
            items,
            last_sequence,
        }))
    }
}

fn collect_task_id_from_turn_event(
    event: &TurnItemEvent,
    task_ids: &mut std::collections::BTreeSet<String>,
) {
    match &event.payload {
        TurnItemEventPayload::ItemStarted { item, .. }
        | TurnItemEventPayload::ItemCompleted { item, .. }
        | TurnItemEventPayload::ItemUpdated { item, .. } => {
            if let TurnItem::Task { item } = item {
                task_ids.insert(item.task_id.clone());
            }
        }
        _ => {}
    }
}

fn timeline_item_for_turn_event(
    prefix: &str,
    kind: TimelineOriginKind,
    lane: TimelineLane,
    task_id: Option<String>,
    run_id: Option<String>,
    child_thread_id: Option<String>,
    child_turn_id: Option<String>,
    event: TurnItemEvent,
) -> TimelineItem {
    let origin_turn_item_id = turn_event_item_id(&event);
    TimelineItem {
        id: format!(
            "{}:{}:{}",
            prefix,
            origin_turn_item_id.as_deref().unwrap_or("turn"),
            event.sequence
        ),
        origin: TimelineOrigin {
            kind,
            task_id,
            run_id,
            child_thread_id,
            child_turn_id,
            origin_event_id: None,
            origin_turn_item_id,
            origin_sequence: event.sequence,
            occurred_at: event.created_at,
            lane,
        },
        payload: TimelinePayload::TurnItemEvent { event },
    }
}

fn timeline_timestamp_ms(timestamp: i64) -> i64 {
    if timestamp > 1_000_000_000_000 {
        timestamp
    } else {
        timestamp.saturating_mul(1000)
    }
}

fn turn_event_item_id(event: &TurnItemEvent) -> Option<String> {
    match &event.payload {
        TurnItemEventPayload::ItemStarted { item, .. }
        | TurnItemEventPayload::ItemCompleted { item, .. }
        | TurnItemEventPayload::ItemUpdated { item, .. } => turn_item_id(item).map(str::to_owned),
        TurnItemEventPayload::ItemDelta { item_id, .. }
        | TurnItemEventPayload::ItemTimeoutDetected { item_id, .. }
        | TurnItemEventPayload::ItemRecoveryOpened { item_id, .. }
        | TurnItemEventPayload::ItemRecoveryAttached { item_id, .. }
        | TurnItemEventPayload::ItemRetryScheduled { item_id, .. }
        | TurnItemEventPayload::ItemRetryAttemptStarted { item_id, .. }
        | TurnItemEventPayload::ItemRecoverySucceeded { item_id, .. }
        | TurnItemEventPayload::ItemRecoveryExhausted { item_id, .. }
        | TurnItemEventPayload::ItemToolRetryScheduled { item_id, .. }
        | TurnItemEventPayload::ItemToolRetryResolved { item_id, .. }
        | TurnItemEventPayload::ItemToolRetryExhausted { item_id, .. } => Some(item_id.clone()),
        TurnItemEventPayload::TurnToolLoopBudgetExceeded { .. } => None,
    }
}

fn turn_item_id(item: &TurnItem) -> Option<&str> {
    match item {
        TurnItem::UserMessage { id, .. }
        | TurnItem::AgentMessage { id, .. }
        | TurnItem::Reasoning { id, .. }
        | TurnItem::SystemEvent { id, .. }
        | TurnItem::CommandExecution { id, .. }
        | TurnItem::FileChange { id, .. }
        | TurnItem::WebSearch { id, .. }
        | TurnItem::WebFetch { id, .. }
        | TurnItem::Download { id, .. }
        | TurnItem::DynamicToolCall { id, .. } => Some(id.as_str()),
        TurnItem::Task { item } => Some(item.id.as_str()),
    }
}

fn turn_item_type(item: &TurnItem) -> TurnItemType {
    match item {
        TurnItem::UserMessage { .. } => TurnItemType::UserMessage,
        TurnItem::AgentMessage { .. } => TurnItemType::AgentMessage,
        TurnItem::Reasoning { .. } => TurnItemType::Reasoning,
        TurnItem::SystemEvent { .. } => TurnItemType::SystemEvent,
        TurnItem::Task { .. } => TurnItemType::Task,
        TurnItem::CommandExecution { .. } => TurnItemType::CommandExecution,
        TurnItem::FileChange { .. } => TurnItemType::FileChange,
        TurnItem::WebSearch { .. } => TurnItemType::WebSearch,
        TurnItem::WebFetch { .. } => TurnItemType::WebFetch,
        TurnItem::Download { .. } => TurnItemType::Download,
        TurnItem::DynamicToolCall { .. } => TurnItemType::DynamicToolCall,
    }
}

fn lane_for_turn_event(event: &TurnItemEvent, child: bool) -> TimelineLane {
    let child_lane = |item: &TurnItem| match item {
        TurnItem::Reasoning { .. } => TimelineLane::ChildReasoning,
        TurnItem::AgentMessage { .. } => TimelineLane::ChildResult,
        TurnItem::CommandExecution { .. }
        | TurnItem::FileChange { .. }
        | TurnItem::WebSearch { .. }
        | TurnItem::WebFetch { .. }
        | TurnItem::Download { .. }
        | TurnItem::DynamicToolCall { .. } => TimelineLane::ChildTool,
        _ => TimelineLane::ChildAgent,
    };
    match &event.payload {
        TurnItemEventPayload::ItemStarted { item, .. }
        | TurnItemEventPayload::ItemCompleted { item, .. }
        | TurnItemEventPayload::ItemUpdated { item, .. }
            if child =>
        {
            child_lane(item)
        }
        TurnItemEventPayload::ItemDelta { stream, .. } if child => match stream {
            Some(ItemDeltaStream::AgentMessage) => TimelineLane::ChildResult,
            _ => TimelineLane::ChildAgent,
        },
        _ if child => TimelineLane::ChildAgent,
        _ => TimelineLane::Parent,
    }
}

fn is_collapsible_task_event(event_type: &str) -> bool {
    matches!(
        event_type,
        events::TASK_CREATED
            | events::TASK_SCHEDULED
            | events::TASK_QUEUED
            | events::TASK_RUN_CREATED
            | events::TASK_RUN_STARTED
            | events::TASK_PROGRESS
            | events::TASK_RUN_COMPLETED
            | events::TASK_RUN_RETRY_SCHEDULED
            | events::TASK_COMPLETED
            | events::TASK_RESCHEDULED
            | events::TASK_TREE_CHANGED
            | events::TASK_DELIVERY_QUEUED
            | events::TASK_DELIVERY_STARTED
            | events::TASK_DELIVERY_DELIVERED
            | events::TASK_WRITE_LOCK_ACQUIRED
            | events::TASK_WRITE_LOCK_RELEASED
            | events::TASK_WRITE_LOCK_BLOCKED
            | events::TASK_WRITE_LOCK_EXPIRED
    )
}

fn is_collapsible_child_turn_event(payload: &TurnItemEventPayload) -> bool {
    matches!(
        payload,
        TurnItemEventPayload::ItemDelta { .. }
            | TurnItemEventPayload::ItemRecoveryOpened { .. }
            | TurnItemEventPayload::ItemRecoveryAttached { .. }
            | TurnItemEventPayload::ItemRetryScheduled { .. }
            | TurnItemEventPayload::ItemRetryAttemptStarted { .. }
            | TurnItemEventPayload::ItemRecoverySucceeded { .. }
            | TurnItemEventPayload::ItemToolRetryScheduled { .. }
            | TurnItemEventPayload::ItemToolRetryResolved { .. }
    )
}

fn child_turn_item_types(
    events: &[TurnItemEvent],
) -> std::collections::HashMap<String, TurnItemType> {
    let mut item_types = std::collections::HashMap::new();
    for event in events {
        let (item_id, item_type) = match &event.payload {
            TurnItemEventPayload::ItemStarted { item, .. }
            | TurnItemEventPayload::ItemCompleted { item, .. }
            | TurnItemEventPayload::ItemUpdated { item, .. } => {
                let Some(item_id) = turn_item_id(item) else {
                    continue;
                };
                (item_id.to_owned(), turn_item_type(item))
            }
            _ => continue,
        };
        item_types.insert(item_id, item_type);
    }
    item_types
}

fn should_keep_child_delta_event(
    payload: &TurnItemEventPayload,
    item_types: &std::collections::HashMap<String, TurnItemType>,
) -> bool {
    let TurnItemEventPayload::ItemDelta {
        item_id, stream, ..
    } = payload
    else {
        return false;
    };

    match item_types.get(item_id.as_str()).copied() {
        Some(TurnItemType::Reasoning) | Some(TurnItemType::AgentMessage) => true,
        // If a delta arrives before the lifecycle event, keep only explicit agent-message streams.
        None => matches!(stream, Some(ItemDeltaStream::AgentMessage)),
        _ => false,
    }
}

fn select_child_turn_events_for_timeline(
    mut events: Vec<TurnItemEvent>,
    include_collapsed_task_events: bool,
    max_child_items: usize,
) -> Vec<TurnItemEvent> {
    if !include_collapsed_task_events {
        let item_types = child_turn_item_types(events.as_slice());
        events.retain(|event| {
            if should_keep_child_delta_event(&event.payload, &item_types) {
                return true;
            }
            !is_collapsible_child_turn_event(&event.payload)
        });
    }
    let skip = events.len().saturating_sub(max_child_items);
    events.into_iter().skip(skip).collect()
}

fn source_priority(kind: TimelineOriginKind) -> u8 {
    match kind {
        TimelineOriginKind::ParentTurn => 0,
        TimelineOriginKind::TaskEvent => 1,
        TimelineOriginKind::ChildTurn => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_item_timeline_origin_uses_created_at_not_sequence() {
        let item = timeline_item_for_turn_event(
            "child",
            TimelineOriginKind::ChildTurn,
            TimelineLane::ChildAgent,
            Some("task_1".to_owned()),
            Some("run_1".to_owned()),
            Some("child_thread_1".to_owned()),
            Some("child_turn_1".to_owned()),
            TurnItemEvent {
                sequence: 99,
                created_at: 1_700_000_000_123,
                payload: TurnItemEventPayload::ItemDelta {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item_id: "child_item_1".to_owned(),
                    delta: "chunk".to_owned(),
                    stream: None,
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                },
            },
        );

        assert_eq!(item.origin.origin_sequence, 99);
        assert_eq!(item.origin.occurred_at, 1_700_000_000_123);
    }

    #[test]
    fn default_collapsed_task_events_include_normal_lifecycle_noise() {
        for event_type in [
            events::TASK_CREATED,
            events::TASK_RUN_CREATED,
            events::TASK_RESCHEDULED,
            events::TASK_RUN_STARTED,
            events::TASK_PROGRESS,
            events::TASK_RUN_COMPLETED,
            events::TASK_RUN_RETRY_SCHEDULED,
            events::TASK_COMPLETED,
            events::TASK_WRITE_LOCK_BLOCKED,
            events::TASK_WRITE_LOCK_EXPIRED,
        ] {
            assert!(
                is_collapsible_task_event(event_type),
                "{event_type} should be hidden from default composed timeline"
            );
        }

        assert!(!is_collapsible_task_event(events::TASK_RUN_FAILED));
        assert!(!is_collapsible_task_event(events::TASK_FAILED));
    }

    #[test]
    fn collapsible_child_turn_events_hide_progress_and_retry_bookkeeping() {
        assert!(is_collapsible_child_turn_event(
            &TurnItemEventPayload::ItemDelta {
                workspace_id: "ws_1".to_owned(),
                thread_id: "child_thread_1".to_owned(),
                turn_id: "child_turn_1".to_owned(),
                item_id: "item_1".to_owned(),
                delta: "chunk".to_owned(),
                stream: Some(ItemDeltaStream::Generic),
                payload: None,
                markdown: None,
                markdown_version: None,
            }
        ));
        assert!(is_collapsible_child_turn_event(
            &TurnItemEventPayload::ItemToolRetryScheduled {
                workspace_id: "ws_1".to_owned(),
                thread_id: "child_thread_1".to_owned(),
                turn_id: "child_turn_1".to_owned(),
                item_id: "item_1".to_owned(),
                item_type: pioneer_protocol::TurnItemType::DynamicToolCall,
                tool_retry_episode_id: "retry_1".to_owned(),
                tool_name: "grep_files".to_owned(),
                attempt_number: 1,
                error_class: pioneer_protocol::ToolRetryErrorClass::ExecutionFailed,
                retry_hint: "retry".to_owned(),
                budgets: Vec::new(),
                failure_signature_fingerprint: "sig".to_owned(),
                reason: "recoverable_tool_output".to_owned(),
            }
        ));
        assert!(is_collapsible_child_turn_event(
            &TurnItemEventPayload::ItemToolRetryResolved {
                workspace_id: "ws_1".to_owned(),
                thread_id: "child_thread_1".to_owned(),
                turn_id: "child_turn_1".to_owned(),
                item_id: "item_1".to_owned(),
                item_type: pioneer_protocol::TurnItemType::DynamicToolCall,
                tool_retry_episode_id: "retry_1".to_owned(),
                tool_name: "grep_files".to_owned(),
                attempt_number: 1,
                resolution: pioneer_protocol::ToolRetryResolution::Succeeded,
                budgets: Vec::new(),
                reason: "retry_episode_resolved".to_owned(),
            }
        ));
        assert!(!is_collapsible_child_turn_event(
            &TurnItemEventPayload::ItemToolRetryExhausted {
                workspace_id: "ws_1".to_owned(),
                thread_id: "child_thread_1".to_owned(),
                turn_id: "child_turn_1".to_owned(),
                item_id: "item_1".to_owned(),
                item_type: pioneer_protocol::TurnItemType::DynamicToolCall,
                tool_retry_episode_id: "retry_1".to_owned(),
                tool_name: "grep_files".to_owned(),
                attempt_number: 2,
                error_class: pioneer_protocol::ToolRetryErrorClass::ExecutionFailed,
                exhaustion_kind: pioneer_protocol::ToolRetryExhaustionKind::TotalRetryRounds,
                budgets: Vec::new(),
                failure_signature_fingerprint: "sig".to_owned(),
                reason: "retry_episode_exhausted".to_owned(),
            }
        ));
    }

    #[test]
    fn child_timeline_selection_keeps_latest_non_collapsible_events() {
        let events = vec![
            TurnItemEvent {
                sequence: 1,
                created_at: 1,
                payload: TurnItemEventPayload::ItemDelta {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item_id: "item_1".to_owned(),
                    delta: "delta".to_owned(),
                    stream: Some(ItemDeltaStream::Generic),
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                },
            },
            TurnItemEvent {
                sequence: 2,
                created_at: 2,
                payload: TurnItemEventPayload::ItemStarted {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_1".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            TurnItemEvent {
                sequence: 3,
                created_at: 3,
                payload: TurnItemEventPayload::ItemCompleted {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_1".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            TurnItemEvent {
                sequence: 4,
                created_at: 4,
                payload: TurnItemEventPayload::ItemToolRetryScheduled {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item_id: "tool_1".to_owned(),
                    item_type: pioneer_protocol::TurnItemType::DynamicToolCall,
                    tool_retry_episode_id: "retry_1".to_owned(),
                    tool_name: "grep_files".to_owned(),
                    attempt_number: 1,
                    error_class: pioneer_protocol::ToolRetryErrorClass::ExecutionFailed,
                    retry_hint: "retry".to_owned(),
                    budgets: Vec::new(),
                    failure_signature_fingerprint: "sig".to_owned(),
                    reason: "recoverable_tool_output".to_owned(),
                },
            },
            TurnItemEvent {
                sequence: 5,
                created_at: 5,
                payload: TurnItemEventPayload::ItemStarted {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item: TurnItem::AgentMessage {
                        id: "agent_1".to_owned(),
                        text: "final".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
            },
            TurnItemEvent {
                sequence: 6,
                created_at: 6,
                payload: TurnItemEventPayload::ItemCompleted {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item: TurnItem::AgentMessage {
                        id: "agent_1".to_owned(),
                        text: "final".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
            },
        ];

        let selected = select_child_turn_events_for_timeline(events, false, 3);
        let selected_sequences = selected
            .into_iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>();
        assert_eq!(
            selected_sequences,
            vec![3, 5, 6],
            "collapsible child progress/retry bookkeeping should be filtered while keeping latest lifecycle/final events"
        );
    }

    #[test]
    fn child_timeline_selection_keeps_reasoning_deltas() {
        let events = vec![
            TurnItemEvent {
                sequence: 1,
                created_at: 1,
                payload: TurnItemEventPayload::ItemStarted {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_1".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            TurnItemEvent {
                sequence: 2,
                created_at: 2,
                payload: TurnItemEventPayload::ItemDelta {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    delta: "thinking chunk".to_owned(),
                    stream: Some(ItemDeltaStream::Generic),
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                },
            },
            TurnItemEvent {
                sequence: 3,
                created_at: 3,
                payload: TurnItemEventPayload::ItemToolRetryScheduled {
                    workspace_id: "ws_1".to_owned(),
                    thread_id: "child_thread_1".to_owned(),
                    turn_id: "child_turn_1".to_owned(),
                    item_id: "tool_1".to_owned(),
                    item_type: pioneer_protocol::TurnItemType::DynamicToolCall,
                    tool_retry_episode_id: "retry_1".to_owned(),
                    tool_name: "grep_files".to_owned(),
                    attempt_number: 1,
                    error_class: pioneer_protocol::ToolRetryErrorClass::ExecutionFailed,
                    retry_hint: "retry".to_owned(),
                    budgets: Vec::new(),
                    failure_signature_fingerprint: "sig".to_owned(),
                    reason: "recoverable_tool_output".to_owned(),
                },
            },
        ];

        let selected = select_child_turn_events_for_timeline(events, false, 10);
        let selected_sequences = selected
            .into_iter()
            .map(|event| event.sequence)
            .collect::<Vec<_>>();
        assert_eq!(
            selected_sequences,
            vec![1, 2],
            "reasoning deltas should stay visible in composed child timeline"
        );
    }
}
