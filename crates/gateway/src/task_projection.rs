use pioneer_protocol::{
    PublicErrorStage, PublicTask, PublicTaskAgendaItem, PublicTaskAgendaResponse,
    PublicTaskAgentConfiguration, PublicTaskArtifact, PublicTaskConfiguration,
    PublicTaskDeliveriesResponse, PublicTaskDelivery, PublicTaskDeliveryAttempt,
    PublicTaskDeliveryPolicy, PublicTaskDependency, PublicTaskEvent, PublicTaskEventsResponse,
    PublicTaskFailure, PublicTaskGetResponse, PublicTaskListResponse, PublicTaskResult,
    PublicTaskResultCandidate, PublicTaskResultContractConfiguration, PublicTaskResultReadResponse,
    PublicTaskReviewContent, PublicTaskReviewContentFormat, PublicTaskRun, PublicTaskTree,
    PublicTaskTreeResponse, PublicTaskTrigger, PublicTaskTriggerConfiguration,
    PublicTaskTriggerSpec, PublicTaskWaitItem, PublicTaskWaitNonWaitableItem,
    PublicTaskWaitResponse, PublicTaskWaitReviewItem, Task, TaskAgendaResponse,
    TaskDeliveriesResponse, TaskDelivery, TaskDeliveryAttempt, TaskError, TaskErrorClass,
    TaskEventsResponse, TaskGetResponse, TaskListResponse, TaskOperatorDeliveries,
    TaskOperatorDetails, TaskResult, TaskResultCandidate, TaskRun, TaskTree, TaskTreeResponse,
    TaskTrigger, TaskTriggerSpec, TaskValue, TaskWaitItem, TaskWaitResponse,
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub(crate) const DEFAULT_TASK_REVIEW_CONTENT_BYTES: usize = 64 * 1024;
pub(crate) const MAX_TASK_REVIEW_CONTENT_BYTES: usize = 512 * 1024;

pub(crate) fn project_task_get(
    response: &TaskGetResponse,
    operator_allowed: bool,
) -> PublicTaskGetResponse {
    project_task_get_with_configuration(response, operator_allowed, false)
}

pub(crate) fn project_task_get_with_configuration(
    response: &TaskGetResponse,
    operator_allowed: bool,
    configuration_allowed: bool,
) -> PublicTaskGetResponse {
    PublicTaskGetResponse {
        task: project_task(&response.task),
        triggers: response.triggers.iter().map(project_trigger).collect(),
        runs: response.runs.iter().map(project_run).collect(),
        dependencies: response
            .dependencies
            .iter()
            .map(project_dependency)
            .collect(),
        configuration: configuration_allowed.then(|| project_task_configuration(response)),
        operator: operator_allowed.then(|| TaskOperatorDetails {
            task: response.task.clone(),
            agent_specs: response.agent_specs.clone(),
            write_locks: response.write_locks.clone(),
            thread_lineage: response.thread_lineage.clone(),
            task_run_thread_bindings: response.task_run_thread_bindings.clone(),
            task_run_turns: response.task_run_turns.clone(),
            result_candidates: response.result_candidates.clone(),
            result_review_events: response.result_review_events.clone(),
        }),
    }
}

fn project_task_configuration(response: &TaskGetResponse) -> PublicTaskConfiguration {
    // A scheduled RunCreated event reuses the durable spec id, so its SQL
    // projection can replace the base row with the frozen run snapshot. Prefer
    // a separately materialized base row, but fall back to the latest effective
    // snapshot so configuration remains readable after the first launch.
    let agent_spec = response
        .agent_specs
        .iter()
        .rev()
        .find(|spec| spec.run_id.is_none())
        .or_else(|| response.agent_specs.last());
    PublicTaskConfiguration {
        triggers: response
            .triggers
            .iter()
            .map(project_trigger_configuration)
            .collect(),
        agent: agent_spec.map(|spec| PublicTaskAgentConfiguration {
            agent_role: spec.agent_role.clone(),
            agent_nickname: spec.agent_nickname.clone(),
            model: spec.model.clone(),
            model_provider: spec.model_provider.clone(),
            instructions: spec.prompt.instructions.clone(),
            output_instructions: spec.prompt.output_instructions.clone(),
            input_configured: spec.prompt.input.is_some(),
            context_policy_configured: spec.context_policy.is_some(),
            tool_policy_configured: spec.tool_policy.is_some(),
            result_contract: spec.result_contract.as_ref().map(|contract| {
                PublicTaskResultContractConfiguration {
                    format: contract.format,
                    required: contract.required,
                    schema_configured: contract.schema.is_some(),
                }
            }),
            review_policy: spec.review_policy.clone(),
            depth: spec.depth,
            max_depth: spec.max_depth,
        }),
        lifecycle_policy: response.task.lifecycle_policy.clone(),
        delivery_policy: response.task.delivery_policy.as_ref().map(|policy| {
            PublicTaskDeliveryPolicy {
                mode: policy.mode,
                thread_target: policy.thread_target,
                thread_id: policy.thread_id.clone(),
                webhook_configured: policy.webhook_url.is_some(),
                include_result: policy.include_result,
                format: policy.format,
            }
        }),
        retry_policy: response.task.retry_policy.clone(),
        timeout_policy: response.task.timeout_policy.clone(),
        concurrency_policy: response.task.concurrency_policy.clone(),
    }
}

fn project_trigger_configuration(trigger: &TaskTrigger) -> PublicTaskTriggerConfiguration {
    let spec = match &trigger.spec {
        TaskTriggerSpec::Immediate => PublicTaskTriggerSpec::Immediate,
        TaskTriggerSpec::ScheduledAt {
            scheduled_at,
            timezone,
            catch_up_policy,
        } => PublicTaskTriggerSpec::ScheduledAt {
            scheduled_at: *scheduled_at,
            timezone: timezone.clone(),
            catch_up_policy: catch_up_policy.clone(),
        },
        TaskTriggerSpec::Interval {
            interval_seconds,
            interval_anchor_at,
            catch_up_policy,
        } => PublicTaskTriggerSpec::Interval {
            interval_seconds: *interval_seconds,
            interval_anchor_at: *interval_anchor_at,
            catch_up_policy: catch_up_policy.clone(),
        },
        TaskTriggerSpec::Cron {
            cron_expr,
            timezone,
            catch_up_policy,
        } => PublicTaskTriggerSpec::Cron {
            cron_expr: cron_expr.clone(),
            timezone: timezone.clone(),
            catch_up_policy: catch_up_policy.clone(),
        },
        TaskTriggerSpec::Manual { allowed_actor } => PublicTaskTriggerSpec::Manual {
            allowed_actor: *allowed_actor,
        },
        TaskTriggerSpec::External {
            source,
            event_type,
            filter,
        } => PublicTaskTriggerSpec::External {
            source: source.clone(),
            event_type: event_type.clone(),
            filter_configured: filter.is_some(),
        },
        TaskTriggerSpec::Dependency { policy } => PublicTaskTriggerSpec::Dependency {
            policy: policy.clone(),
        },
    };
    PublicTaskTriggerConfiguration {
        id: trigger.id.clone(),
        status: trigger.status,
        kind: trigger.kind(),
        spec,
        next_fire_at: trigger.next_fire_at,
        last_fire_at: trigger.last_fire_at,
    }
}

pub(crate) fn project_task_list(response: &TaskListResponse) -> PublicTaskListResponse {
    PublicTaskListResponse {
        tasks: response.tasks.iter().map(project_task).collect(),
        next_cursor: response.next_cursor,
    }
}

pub(crate) fn project_task_tree(
    response: &TaskTreeResponse,
    operator_allowed: bool,
) -> PublicTaskTreeResponse {
    PublicTaskTreeResponse {
        tree: project_tree(&response.tree),
        operator: operator_allowed.then(|| response.tree.clone()),
    }
}

pub(crate) fn project_task_events(
    response: &TaskEventsResponse,
    operator_allowed: bool,
) -> PublicTaskEventsResponse {
    PublicTaskEventsResponse {
        task_id: response.task_id.clone(),
        events: response
            .events
            .iter()
            .map(|event| PublicTaskEvent {
                id: event.id.clone(),
                task_id: event.task_id.clone(),
                run_id: event.run_id.clone(),
                sequence: event.sequence,
                event_type: event.event_type.clone(),
                created_at: event.created_at,
            })
            .collect(),
        last_sequence: response.last_sequence,
        has_more: response.has_more,
        operator_events: operator_allowed.then(|| response.events.clone()),
    }
}

pub(crate) fn project_task_deliveries(
    response: &TaskDeliveriesResponse,
    operator_allowed: bool,
) -> PublicTaskDeliveriesResponse {
    PublicTaskDeliveriesResponse {
        deliveries: response.deliveries.iter().map(project_delivery).collect(),
        attempts: response
            .attempts
            .iter()
            .map(project_delivery_attempt)
            .collect(),
        operator: operator_allowed.then(|| TaskOperatorDeliveries {
            deliveries: response.deliveries.clone(),
            attempts: response.attempts.clone(),
        }),
    }
}

pub(crate) fn project_task_wait(response: &TaskWaitResponse) -> PublicTaskWaitResponse {
    project_task_wait_with_review_content(response, &BTreeMap::new())
}

pub(crate) fn project_task_wait_with_review_content(
    response: &TaskWaitResponse,
    review_content: &BTreeMap<String, PublicTaskReviewContent>,
) -> PublicTaskWaitResponse {
    PublicTaskWaitResponse {
        completed: response.completed.iter().map(project_wait_item).collect(),
        failed: response.failed.iter().map(project_wait_item).collect(),
        blocked: response.blocked.iter().map(project_wait_item).collect(),
        cancelled: response.cancelled.iter().map(project_wait_item).collect(),
        review_required: response
            .review_required
            .iter()
            .map(|review| PublicTaskWaitReviewItem {
                item: project_wait_item(&review.item),
                candidate: project_candidate(&review.candidate),
                review_content: review_content.get(review.candidate.id.as_str()).cloned(),
                remaining_revision_rounds: review.remaining_revision_rounds,
                allowed_actions: review.allowed_actions.clone(),
                revision_blocked_reason: review.revision_blocked_reason,
            })
            .collect(),
        pending: response.pending.iter().map(project_wait_item).collect(),
        non_waitable: response
            .non_waitable
            .iter()
            .map(|item| PublicTaskWaitNonWaitableItem {
                item: project_wait_item(&item.item),
                reason: item.reason,
                next_fire_at: item.next_fire_at,
            })
            .collect(),
        timed_out: response.timed_out,
        total_count: response.total_count,
        terminal_count: response.terminal_count,
        pending_count: response.pending_count,
        review_required_count: response.review_required_count,
        blocked_count: response.blocked_count,
        non_waitable_count: response.non_waitable_count,
        mode: response.mode,
    }
}

pub(crate) fn project_task_result_read_response(
    candidate: &TaskResultCandidate,
    cursor: Option<&str>,
    max_bytes: usize,
) -> Result<PublicTaskResultReadResponse, String> {
    Ok(PublicTaskResultReadResponse {
        candidate: project_candidate(candidate),
        review_content: project_task_review_content(candidate, cursor, max_bytes)?,
    })
}

pub(crate) fn project_task_review_content(
    candidate: &TaskResultCandidate,
    cursor: Option<&str>,
    max_bytes: usize,
) -> Result<PublicTaskReviewContent, String> {
    let (format, full_content) = review_content_source(candidate)?;
    let total_bytes = full_content.len();
    let start = match cursor {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| "task result cursor is invalid".to_owned())?,
        None => 0,
    };
    if start > total_bytes || !full_content.is_char_boundary(start) {
        return Err("task result cursor is invalid".to_owned());
    }

    let max_bytes = max_bytes.clamp(1, MAX_TASK_REVIEW_CONTENT_BYTES);
    let mut end = start.saturating_add(max_bytes).min(total_bytes);
    while end > start && !full_content.is_char_boundary(end) {
        end -= 1;
    }
    if end == start && start < total_bytes {
        end = start
            + full_content[start..]
                .chars()
                .next()
                .expect("non-empty suffix has a first character")
                .len_utf8();
    }

    let truncated = end < total_bytes;
    Ok(PublicTaskReviewContent {
        format,
        content: full_content[start..end].to_owned(),
        total_bytes: total_bytes as u64,
        content_sha256: format!(
            "sha256:{}",
            hex::encode(Sha256::digest(full_content.as_bytes()))
        ),
        truncated,
        next_cursor: truncated.then(|| end.to_string()),
    })
}

fn review_content_source(
    candidate: &TaskResultCandidate,
) -> Result<(PublicTaskReviewContentFormat, String), String> {
    let result = candidate.result.as_ref();
    if let Some(TaskResult {
        data: Some(TaskValue::Object(values)),
        ..
    }) = result
        && values.get("fallbackUsed") == Some(&TaskValue::Bool(true))
        && let Some(TaskValue::String(raw_text)) = values.get("rawText")
    {
        return Ok((PublicTaskReviewContentFormat::Text, raw_text.clone()));
    }

    if let Some(TaskResult {
        data: Some(TaskValue::String(text)),
        ..
    }) = result
    {
        return Ok((PublicTaskReviewContentFormat::Text, text.clone()));
    }

    if let Some(data) = result.and_then(|result| result.data.as_ref()) {
        return serde_json::to_string(&task_value_to_json(data))
            .map(|content| (PublicTaskReviewContentFormat::Json, content))
            .map_err(|error| format!("failed to encode task result content: {error}"));
    }

    result
        .and_then(|result| result.summary.clone())
        .or_else(|| candidate.summary.clone())
        .map(|content| (PublicTaskReviewContentFormat::Text, content))
        .ok_or_else(|| "task result candidate has no reviewable content".to_owned())
}

fn task_value_to_json(value: &TaskValue) -> serde_json::Value {
    match value {
        TaskValue::Null => serde_json::Value::Null,
        TaskValue::Bool(value) => serde_json::Value::Bool(*value),
        TaskValue::Integer(value) => serde_json::Value::Number((*value).into()),
        TaskValue::Number(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        TaskValue::String(value) => serde_json::Value::String(value.clone()),
        TaskValue::List(values) => {
            serde_json::Value::Array(values.iter().map(task_value_to_json).collect())
        }
        TaskValue::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), task_value_to_json(value)))
                .collect(),
        ),
    }
}

pub(crate) fn project_task_agenda(response: &TaskAgendaResponse) -> PublicTaskAgendaResponse {
    PublicTaskAgendaResponse {
        items: response
            .items
            .iter()
            .map(|item| PublicTaskAgendaItem {
                task: project_task(&item.task),
                trigger: item.trigger.as_ref().map(project_trigger),
                latest_run: item.latest_run.as_ref().map(project_run),
                latest_delivery: item.latest_delivery.as_ref().map(project_delivery),
                goal_preview: item.goal_preview.clone(),
                result_preview: item.result_preview.clone(),
            })
            .collect(),
    }
}

pub(crate) fn project_task(task: &Task) -> PublicTask {
    PublicTask {
        id: task.id.clone(),
        workspace_id: task.workspace_id.clone(),
        owner_kind: task.owner_kind,
        owner_id: task.owner_id.clone(),
        created_by_thread_id: task.created_by_thread_id.clone(),
        created_by_turn_id: task.created_by_turn_id.clone(),
        root_task_id: task.root_task_id.clone(),
        parent_task_id: task.parent_task_id.clone(),
        executor_kind: task.executor_kind,
        status: task.status,
        title: task.title.clone(),
        goal: task.goal.clone(),
        priority: task.priority,
        result: task.result.as_ref().map(project_result),
        error: task.error.as_ref().map(project_error),
        revision: task.revision,
        created_at: task.created_at,
        updated_at: task.updated_at,
        completed_at: task.completed_at,
    }
}

pub(crate) fn project_result(result: &TaskResult) -> PublicTaskResult {
    PublicTaskResult {
        summary: result.summary.clone(),
        artifacts: result
            .artifacts
            .iter()
            .map(|artifact| PublicTaskArtifact {
                artifact_id: artifact.artifact_id.clone(),
                version_id: artifact.version_id.clone(),
                mime_type: artifact.mime_type.clone(),
            })
            .collect(),
    }
}

pub(crate) fn project_error(error: &TaskError) -> PublicTaskFailure {
    PublicTaskFailure {
        class: error.class,
        error: crate::public_error::map_agent_failure(
            task_public_error_code(error.class),
            PublicErrorStage::Execution,
            error.message.as_str(),
        ),
    }
}

fn task_public_error_code(class: TaskErrorClass) -> pioneer_protocol::PublicErrorCode {
    match class {
        TaskErrorClass::Cancelled => pioneer_protocol::PublicErrorCode::Conflict,
        TaskErrorClass::Timeout => pioneer_protocol::PublicErrorCode::Timeout,
        TaskErrorClass::Validation => pioneer_protocol::PublicErrorCode::InvalidInput,
        TaskErrorClass::Policy => pioneer_protocol::PublicErrorCode::PolicyDenied,
        TaskErrorClass::Provider | TaskErrorClass::Tool | TaskErrorClass::Dependency => {
            pioneer_protocol::PublicErrorCode::Unavailable
        }
        TaskErrorClass::Internal | TaskErrorClass::Unknown => {
            pioneer_protocol::PublicErrorCode::Internal
        }
    }
}

pub(crate) fn project_run(run: &TaskRun) -> PublicTaskRun {
    PublicTaskRun {
        id: run.id.clone(),
        task_id: run.task_id.clone(),
        attempt_number: run.attempt_number,
        run_number: run.run_number,
        status: run.status,
        executor_kind: run.executor_kind,
        started_at: run.started_at,
        completed_at: run.completed_at,
        result: run.result.as_ref().map(project_result),
        error: run.error.as_ref().map(project_error),
        created_at: run.created_at,
        updated_at: run.updated_at,
    }
}

pub(crate) fn project_trigger(trigger: &TaskTrigger) -> PublicTaskTrigger {
    PublicTaskTrigger {
        id: trigger.id.clone(),
        task_id: trigger.task_id.clone(),
        status: trigger.status,
        kind: trigger.kind(),
        next_fire_at: trigger.next_fire_at,
        last_fire_at: trigger.last_fire_at,
        created_at: trigger.created_at,
        updated_at: trigger.updated_at,
    }
}

fn project_tree(tree: &TaskTree) -> PublicTaskTree {
    PublicTaskTree {
        task: project_task(&tree.task),
        triggers: tree.triggers.iter().map(project_trigger).collect(),
        runs: tree.runs.iter().map(project_run).collect(),
        dependencies: tree.dependencies.iter().map(project_dependency).collect(),
        children: tree.children.iter().map(project_tree).collect(),
    }
}

pub(crate) fn project_delivery(delivery: &TaskDelivery) -> PublicTaskDelivery {
    let error = delivery
        .error_snapshot
        .as_ref()
        .map(project_error)
        .or_else(|| {
            delivery
                .last_error
                .as_deref()
                .map(|message| PublicTaskFailure {
                    class: TaskErrorClass::Internal,
                    error: crate::public_error::map_agent_failure(
                        pioneer_protocol::PublicErrorCode::Internal,
                        PublicErrorStage::Delivery,
                        message,
                    ),
                })
        });
    PublicTaskDelivery {
        id: delivery.id.clone(),
        task_id: delivery.task_id.clone(),
        run_id: delivery.run_id.clone(),
        mode: delivery.mode,
        status: delivery.status,
        attempt_count: delivery.attempt_count,
        max_attempts: delivery.max_attempts,
        result: delivery.result_snapshot.as_ref().map(project_result),
        error,
        delivered_at: delivery.delivered_at,
        created_at: delivery.created_at,
        updated_at: delivery.updated_at,
    }
}

pub(crate) fn project_delivery_attempt(attempt: &TaskDeliveryAttempt) -> PublicTaskDeliveryAttempt {
    PublicTaskDeliveryAttempt {
        id: attempt.id.clone(),
        delivery_id: attempt.delivery_id.clone(),
        attempt_number: attempt.attempt_number,
        status: attempt.status,
        started_at: attempt.started_at,
        completed_at: attempt.completed_at,
    }
}

fn project_wait_item(item: &TaskWaitItem) -> PublicTaskWaitItem {
    PublicTaskWaitItem {
        task: project_task(&item.task),
        run: item.run.as_ref().map(project_run),
    }
}

fn project_candidate(candidate: &TaskResultCandidate) -> PublicTaskResultCandidate {
    PublicTaskResultCandidate {
        id: candidate.id.clone(),
        task_id: candidate.task_id.clone(),
        run_id: candidate.run_id.clone(),
        round: candidate.round,
        status: candidate.status,
        result: candidate.result.as_ref().map(project_result),
        summary: candidate.summary.clone(),
        created_at: candidate.created_at,
        updated_at: candidate.updated_at,
    }
}

fn project_dependency(dependency: &pioneer_protocol::TaskDependency) -> PublicTaskDependency {
    PublicTaskDependency {
        id: dependency.id.clone(),
        task_id: dependency.task_id.clone(),
        depends_on_task_id: dependency.depends_on_task_id.clone(),
        kind: dependency.kind.clone(),
        created_at: dependency.created_at,
    }
}

#[cfg(test)]
mod tests {
    use pioneer_protocol::{
        Task, TaskAgentInput, TaskAgentPrompt, TaskAgentSpec, TaskError, TaskErrorClass,
        TaskExecutorKind, TaskExternalTriggerFilter, TaskGetResponse, TaskOwnerKind, TaskResult,
        TaskResultCandidate, TaskResultCandidateStatus, TaskStatus, TaskTrigger, TaskTriggerSpec,
        TaskTriggerStatus, TaskValue,
    };
    use std::collections::BTreeMap;

    use super::{
        DEFAULT_TASK_REVIEW_CONTENT_BYTES, project_result, project_task_get,
        project_task_get_with_configuration, project_task_result_read_response,
        project_task_review_content,
    };

    fn review_candidate(data: TaskValue) -> TaskResultCandidate {
        TaskResultCandidate {
            id: "candidate_review_content_1".to_owned(),
            task_id: "task_review_content_1".to_owned(),
            run_id: "run_review_content_01".to_owned(),
            task_run_turn_id: "task_run_turn_review_1".to_owned(),
            thread_id: "thread_review_child_1".to_owned(),
            turn_id: "turn_review_child_01".to_owned(),
            round: 1,
            status: TaskResultCandidateStatus::PendingReview,
            result: Some(TaskResult {
                summary: Some("short preview".to_owned()),
                data: Some(data),
                artifacts: Vec::new(),
                completed_by_run_id: Some("run_review_content_01".to_owned()),
            }),
            extraction_error: None,
            summary: Some("short preview".to_owned()),
            diagnostics: vec!["private extractor diagnostic".to_owned()],
            final_review_event_id: None,
            created_at: 1,
            updated_at: 1,
            resolved_at: None,
        }
    }

    #[test]
    fn fallback_review_content_returns_raw_text_without_internal_metadata() {
        let candidate = review_candidate(TaskValue::Object(BTreeMap::from([
            (
                "rawText".to_owned(),
                TaskValue::String("full child-authored result".to_owned()),
            ),
            ("schemaValid".to_owned(), TaskValue::Bool(false)),
            ("fallbackUsed".to_owned(), TaskValue::Bool(true)),
            (
                "diagnostics".to_owned(),
                TaskValue::List(vec![TaskValue::String("private parser path".to_owned())]),
            ),
            (
                "sourceThreadId".to_owned(),
                TaskValue::String("hidden_thread".to_owned()),
            ),
            (
                "sourceTurnId".to_owned(),
                TaskValue::String("hidden_turn".to_owned()),
            ),
        ])));

        let response =
            project_task_result_read_response(&candidate, None, DEFAULT_TASK_REVIEW_CONTENT_BYTES)
                .expect("fallback result should project");
        let encoded = serde_json::to_string(&response).expect("result response should serialize");

        assert_eq!(
            response.review_content.content,
            "full child-authored result"
        );
        assert_eq!(
            response.review_content.format,
            pioneer_protocol::PublicTaskReviewContentFormat::Text
        );
        assert!(!encoded.contains("private parser path"));
        assert!(!encoded.contains("hidden_thread"));
        assert!(!encoded.contains("hidden_turn"));
        assert!(!encoded.contains("private extractor diagnostic"));
    }

    #[test]
    fn structured_review_content_uses_natural_json() {
        let candidate = review_candidate(TaskValue::Object(BTreeMap::from([
            ("answer".to_owned(), TaskValue::Bool(true)),
            ("count".to_owned(), TaskValue::Integer(3)),
        ])));

        let content = project_task_review_content(&candidate, None, 1024)
            .expect("structured result should project");

        assert_eq!(
            content.format,
            pioneer_protocol::PublicTaskReviewContentFormat::Json
        );
        assert_eq!(content.content, r#"{"answer":true,"count":3}"#);
    }

    #[test]
    fn large_review_content_pages_on_utf8_boundaries_and_reassembles() {
        let full_text = format!("начало:{}:конец", "я".repeat(40_000));
        let candidate = review_candidate(TaskValue::Object(BTreeMap::from([
            ("rawText".to_owned(), TaskValue::String(full_text.clone())),
            ("fallbackUsed".to_owned(), TaskValue::Bool(true)),
        ])));
        let mut cursor = None;
        let mut rebuilt = String::new();
        let mut expected_hash = None;

        loop {
            let page = project_task_review_content(&candidate, cursor.as_deref(), 10_001)
                .expect("every continuation page should project");
            if let Some(hash) = expected_hash.as_ref() {
                assert_eq!(&page.content_sha256, hash);
            } else {
                expected_hash = Some(page.content_sha256.clone());
            }
            rebuilt.push_str(page.content.as_str());
            if !page.truncated {
                assert!(page.next_cursor.is_none());
                break;
            }
            cursor = page.next_cursor;
        }

        assert_eq!(rebuilt, full_text);
    }

    #[test]
    fn ordinary_result_projection_remains_summary_only() {
        let result = TaskResult {
            summary: Some("visible summary".to_owned()),
            data: Some(TaskValue::String("private full result".to_owned())),
            artifacts: Vec::new(),
            completed_by_run_id: None,
        };

        let encoded = serde_json::to_string(&project_result(&result))
            .expect("ordinary public result should serialize");

        assert!(encoded.contains("visible summary"));
        assert!(!encoded.contains("private full result"));
    }

    #[test]
    fn collaborator_projection_drops_host_paths_webhooks_and_raw_diagnostics() {
        let canary_path = "/Users/operator/private/task-result.txt";
        let canary_webhook = "https://hooks.example.test/deliver?token=secret";
        let canary_error = "database failed at /var/lib/pioneer/private.sqlite";
        let canary_input = "private task input that must not be disclosed";
        let canary_external_filter = "payload.secret == 'private'";
        let latest_projected_instruction =
            "effective instruction retained after the first scheduled run";
        let mut response = TaskGetResponse {
            task: Task {
                id: "task-1".to_owned(),
                workspace_id: "workspace-1".to_owned(),
                owner_kind: TaskOwnerKind::Thread,
                owner_id: Some("thread-1".to_owned()),
                created_by_thread_id: Some("thread-1".to_owned()),
                created_by_turn_id: None,
                root_task_id: None,
                parent_task_id: None,
                executor_kind: TaskExecutorKind::Agent,
                status: TaskStatus::Failed,
                title: "Task".to_owned(),
                goal: "Goal".to_owned(),
                priority: 0,
                lifecycle_policy: None,
                delivery_policy: Some(pioneer_protocol::TaskDeliveryPolicy {
                    mode: pioneer_protocol::TaskDeliveryMode::Webhook,
                    thread_target: None,
                    thread_id: None,
                    webhook_url: Some(canary_webhook.to_owned()),
                    include_result: true,
                    format: pioneer_protocol::TaskDeliveryFormat::Summary,
                }),
                retry_policy: None,
                timeout_policy: None,
                concurrency_policy: None,
                metadata: None,
                result: Some(pioneer_protocol::TaskResult {
                    summary: Some("done".to_owned()),
                    data: None,
                    artifacts: vec![pioneer_protocol::TaskArtifact {
                        artifact_id: Some("artifact-1".to_owned()),
                        version_id: None,
                        path: Some(canary_path.to_owned()),
                        url: None,
                        mime_type: Some("text/plain".to_owned()),
                        metadata: None,
                    }],
                    completed_by_run_id: None,
                }),
                error: Some(TaskError {
                    code: "internal".to_owned(),
                    message: canary_error.to_owned(),
                    class: TaskErrorClass::Internal,
                    details: None,
                    failed_run_id: None,
                }),
                revision: 1,
                created_at: 1,
                updated_at: 1,
                completed_at: Some(1),
            },
            triggers: vec![
                TaskTrigger {
                    id: "trigger-cron".to_owned(),
                    task_id: "task-1".to_owned(),
                    status: TaskTriggerStatus::Active,
                    spec: TaskTriggerSpec::Cron {
                        cron_expr: "0 5 * * *".to_owned(),
                        timezone: "Europe/Moscow".to_owned(),
                        catch_up_policy: None,
                    },
                    next_fire_at: Some(1_700_000_000),
                    last_fire_at: None,
                    created_at: 1,
                    updated_at: 1,
                },
                TaskTrigger {
                    id: "trigger-external".to_owned(),
                    task_id: "task-1".to_owned(),
                    status: TaskTriggerStatus::Active,
                    spec: TaskTriggerSpec::External {
                        source: "third-party".to_owned(),
                        event_type: Some("release".to_owned()),
                        filter: Some(TaskExternalTriggerFilter {
                            expression: Some(canary_external_filter.to_owned()),
                            fields: BTreeMap::from([(
                                "secret".to_owned(),
                                TaskValue::String("private".to_owned()),
                            )]),
                        }),
                    },
                    next_fire_at: None,
                    last_fire_at: None,
                    created_at: 1,
                    updated_at: 1,
                },
            ],
            runs: Vec::new(),
            agent_specs: vec![TaskAgentSpec {
                id: "agent-spec-1".to_owned(),
                task_id: "task-1".to_owned(),
                run_id: None,
                agent_role: Some("release-auditor".to_owned()),
                agent_nickname: Some("Auditor".to_owned()),
                model: Some("test-model".to_owned()),
                model_provider: Some("test-provider".to_owned()),
                prompt: TaskAgentPrompt {
                    goal: "Audit releases".to_owned(),
                    instructions: vec!["Inspect every new release.".to_owned()],
                    input: Some(TaskAgentInput {
                        text: Some(canary_input.to_owned()),
                        variables: Vec::new(),
                        attachments: Vec::new(),
                        references: Vec::new(),
                    }),
                    output_instructions: Some("Return concise markdown.".to_owned()),
                },
                context_policy: None,
                tool_policy: None,
                permission_cap: None,
                security_cap: None,
                result_contract: None,
                review_policy: None,
                depth: 0,
                max_depth: 3,
                created_at: 1,
                updated_at: 1,
            }],
            dependencies: Vec::new(),
            write_locks: Vec::new(),
            thread_lineage: Vec::new(),
            task_run_thread_bindings: Vec::new(),
            task_run_turns: Vec::new(),
            result_candidates: Vec::new(),
            result_review_events: Vec::new(),
        };
        let mut run_spec = response.agent_specs[0].clone();
        run_spec.id = "agent-spec-run-1".to_owned();
        run_spec.run_id = Some("run-1".to_owned());
        run_spec.prompt.instructions = vec![latest_projected_instruction.to_owned()];
        response.agent_specs.push(run_spec);

        let encoded = serde_json::to_string(&project_task_get(&response, false))
            .expect("public Task projection serializes");
        assert!(!encoded.contains(canary_path));
        assert!(!encoded.contains(canary_webhook));
        assert!(!encoded.contains(canary_error));
        assert!(!encoded.contains("deliveryPolicy"));
        assert!(!encoded.contains("operator"));
        assert!(encoded.contains("\"error\":"));
        assert!(encoded.contains("correlation_id"));

        let managed =
            serde_json::to_string(&project_task_get_with_configuration(&response, false, true))
                .expect("managed Task projection serializes");
        assert!(managed.contains("\"configuration\":"));
        assert!(managed.contains("Inspect every new release."));
        assert!(managed.contains("Return concise markdown."));
        assert!(managed.contains("0 5 * * *"));
        assert!(managed.contains("Europe/Moscow"));
        assert!(managed.contains("\"webhookConfigured\":true"));
        assert!(!managed.contains(canary_webhook));
        assert!(!managed.contains(canary_path));
        assert!(!managed.contains(canary_error));
        assert!(!managed.contains(canary_input));
        assert!(!managed.contains(canary_external_filter));
        assert!(!managed.contains(latest_projected_instruction));
        assert!(!managed.contains("\"operator\":"));
        assert!(managed.contains("\"filterConfigured\":true"));
        assert!(managed.contains("third-party"));

        // RunCreated snapshots intentionally reuse the durable spec id. The
        // database projection therefore replaces the base row with the frozen
        // run row after the first scheduled launch. Management reads must
        // still expose that effective configuration when no separate base row
        // remains, while continuing to redact the input and security details.
        response.agent_specs.remove(0);
        let post_run =
            serde_json::to_string(&project_task_get_with_configuration(&response, false, true))
                .expect("post-run managed Task projection serializes");
        assert!(post_run.contains(latest_projected_instruction));
        assert!(!post_run.contains(canary_input));
        assert!(!post_run.contains("\"operator\":"));
    }
}
