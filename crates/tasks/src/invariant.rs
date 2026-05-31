use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, TaskRuntimeInvariantDeliveryRecord, TaskRuntimeInvariantEventRecord,
    TaskRuntimeInvariantSnapshot, TaskRuntimeInvariantStaleAttemptRecord,
    TaskRuntimeInvariantTurnRecord,
};
use pioneer_protocol::{TaskEventPayload, TaskResult, TaskValue};
use sea_orm::{ConnectOptions, Database};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

mod task_review_invariants;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRuntimeInvariantSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRuntimeChildLinkRef {
    pub event_id: String,
    pub sequence: i64,
    pub child_thread_id: String,
    pub child_turn_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskRuntimeInvariantViolationKind {
    DuplicateLifecycleEvents {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        event_type: String,
        semantic_key: String,
        event_ids: Vec<String>,
        sequences: Vec<i64>,
    },
    MultipleChildThreadLinksForRun {
        task_id: String,
        run_id: String,
        links: Vec<TaskRuntimeChildLinkRef>,
    },
    InvalidDeliveredTaskResult {
        task_id: String,
        run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery_id: Option<String>,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fallback_used: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_valid: Option<bool>,
    },
    DeliveryPointsToInvalidResult {
        task_id: String,
        run_id: String,
        delivery_id: String,
        reason: String,
    },
    StaleInProgressTurn {
        turn_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread_id: Option<String>,
        stale_for_seconds: i64,
        observed_at_unix: i64,
        updated_at_unix: i64,
    },
    StaleTurnItemAttempt {
        turn_id: String,
        item_id: String,
        attempt_id: String,
        item_status: String,
        attempt_status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attempt_number: Option<i64>,
        reason: String,
    },
    PrimaryExecutorBindingMissingLineage {
        task_id: String,
        run_id: String,
        binding_id: String,
        thread_id: String,
    },
    PrimaryExecutorBindingMissingExecution {
        task_id: String,
        run_id: String,
        binding_id: String,
        thread_id: String,
    },
    MultiplePrimaryExecutorBindingsForRun {
        task_id: String,
        run_id: String,
        binding_ids: Vec<String>,
    },
    TaskRunTurnMissingLineage {
        task_id: String,
        run_id: String,
        task_run_turn_id: String,
        thread_id: String,
        turn_id: String,
    },
    TaskRunTurnMissingTurn {
        task_id: String,
        run_id: String,
        task_run_turn_id: String,
        turn_id: String,
    },
    AcceptedCandidateMissingTurn {
        task_id: String,
        run_id: String,
        candidate_id: String,
        task_run_turn_id: String,
    },
    SucceededRunMissingAcceptedCandidate {
        task_id: String,
        run_id: String,
    },
    AcceptedCandidateMissingResult {
        task_id: String,
        run_id: String,
        candidate_id: String,
    },
    AcceptedCandidateMissingFinalReviewEvent {
        task_id: String,
        run_id: String,
        candidate_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        final_review_event_id: Option<String>,
    },
    MultipleAcceptedCandidatesForRun {
        task_id: String,
        run_id: String,
        candidate_ids: Vec<String>,
    },
    TaskResultCandidateTurnMismatch {
        task_id: String,
        run_id: String,
        candidate_id: String,
        task_run_turn_id: String,
        candidate_thread_id: String,
        turn_thread_id: String,
        candidate_turn_id: String,
        turn_turn_id: String,
    },
    TaskResultCandidatePrimaryBindingMismatch {
        task_id: String,
        run_id: String,
        candidate_id: String,
        binding_id: String,
        candidate_thread_id: String,
        binding_thread_id: String,
    },
    TaskResultCandidateRoundMismatch {
        task_id: String,
        run_id: String,
        candidate_id: String,
        task_run_turn_id: String,
        candidate_round: i64,
        turn_round: i64,
    },
    ReviewEventMissingCandidate {
        task_id: String,
        run_id: String,
        review_event_id: String,
        candidate_id: String,
    },
    ReviewEventMissingTaskRunTurn {
        task_id: String,
        run_id: String,
        review_event_id: String,
        task_run_turn_id: String,
    },
    FinalReviewEventDecisionMismatch {
        task_id: String,
        run_id: String,
        candidate_id: String,
        review_event_id: String,
        candidate_status: String,
        review_decision: String,
    },
    DuplicateTaskRunTurnSequence {
        task_id: String,
        run_id: String,
        sequence: i64,
        task_run_turn_ids: Vec<String>,
    },
    NonContiguousTaskRunTurnSequence {
        task_id: String,
        run_id: String,
        task_run_turn_id: String,
        expected_sequence: i64,
        actual_sequence: i64,
    },
    DuplicateCandidateProducingRound {
        task_id: String,
        run_id: String,
        round: i64,
        task_run_turn_ids: Vec<String>,
    },
    NonContiguousCandidateProducingRound {
        task_id: String,
        run_id: String,
        task_run_turn_id: String,
        expected_round: i64,
        actual_round: i64,
    },
}

impl TaskRuntimeInvariantViolationKind {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::DuplicateLifecycleEvents { .. } => "duplicate_lifecycle_events",
            Self::MultipleChildThreadLinksForRun { .. } => "multiple_child_thread_links_for_run",
            Self::InvalidDeliveredTaskResult { .. } => "invalid_delivered_task_result",
            Self::DeliveryPointsToInvalidResult { .. } => "delivery_points_to_invalid_result",
            Self::StaleInProgressTurn { .. } => "stale_in_progress_turn",
            Self::StaleTurnItemAttempt { .. } => "stale_turn_item_attempt",
            Self::PrimaryExecutorBindingMissingLineage { .. } => {
                "primary_executor_binding_missing_lineage"
            }
            Self::PrimaryExecutorBindingMissingExecution { .. } => {
                "primary_executor_binding_missing_execution"
            }
            Self::MultiplePrimaryExecutorBindingsForRun { .. } => {
                "multiple_primary_executor_bindings_for_run"
            }
            Self::TaskRunTurnMissingLineage { .. } => "task_run_turn_missing_lineage",
            Self::TaskRunTurnMissingTurn { .. } => "task_run_turn_missing_turn",
            Self::AcceptedCandidateMissingTurn { .. } => "accepted_candidate_missing_turn",
            Self::SucceededRunMissingAcceptedCandidate { .. } => {
                "succeeded_run_missing_accepted_candidate"
            }
            Self::AcceptedCandidateMissingResult { .. } => "accepted_candidate_missing_result",
            Self::AcceptedCandidateMissingFinalReviewEvent { .. } => {
                "accepted_candidate_missing_final_review_event"
            }
            Self::MultipleAcceptedCandidatesForRun { .. } => "multiple_accepted_candidates_for_run",
            Self::TaskResultCandidateTurnMismatch { .. } => "task_result_candidate_turn_mismatch",
            Self::TaskResultCandidatePrimaryBindingMismatch { .. } => {
                "task_result_candidate_primary_binding_mismatch"
            }
            Self::TaskResultCandidateRoundMismatch { .. } => "task_result_candidate_round_mismatch",
            Self::ReviewEventMissingCandidate { .. } => "review_event_missing_candidate",
            Self::ReviewEventMissingTaskRunTurn { .. } => "review_event_missing_task_run_turn",
            Self::FinalReviewEventDecisionMismatch { .. } => "final_review_event_decision_mismatch",
            Self::DuplicateTaskRunTurnSequence { .. } => "duplicate_task_run_turn_sequence",
            Self::NonContiguousTaskRunTurnSequence { .. } => {
                "non_contiguous_task_run_turn_sequence"
            }
            Self::DuplicateCandidateProducingRound { .. } => "duplicate_candidate_producing_round",
            Self::NonContiguousCandidateProducingRound { .. } => {
                "non_contiguous_candidate_producing_round"
            }
        }
    }

    pub const fn default_severity(&self) -> TaskRuntimeInvariantSeverity {
        match self {
            Self::StaleInProgressTurn { .. } => TaskRuntimeInvariantSeverity::Warning,
            Self::DuplicateLifecycleEvents { .. }
            | Self::MultipleChildThreadLinksForRun { .. }
            | Self::InvalidDeliveredTaskResult { .. }
            | Self::DeliveryPointsToInvalidResult { .. }
            | Self::StaleTurnItemAttempt { .. }
            | Self::PrimaryExecutorBindingMissingLineage { .. }
            | Self::PrimaryExecutorBindingMissingExecution { .. }
            | Self::MultiplePrimaryExecutorBindingsForRun { .. }
            | Self::TaskRunTurnMissingLineage { .. }
            | Self::TaskRunTurnMissingTurn { .. }
            | Self::AcceptedCandidateMissingTurn { .. }
            | Self::SucceededRunMissingAcceptedCandidate { .. }
            | Self::AcceptedCandidateMissingResult { .. }
            | Self::AcceptedCandidateMissingFinalReviewEvent { .. }
            | Self::MultipleAcceptedCandidatesForRun { .. }
            | Self::TaskResultCandidateTurnMismatch { .. }
            | Self::TaskResultCandidatePrimaryBindingMismatch { .. }
            | Self::TaskResultCandidateRoundMismatch { .. }
            | Self::ReviewEventMissingCandidate { .. }
            | Self::ReviewEventMissingTaskRunTurn { .. }
            | Self::FinalReviewEventDecisionMismatch { .. }
            | Self::DuplicateTaskRunTurnSequence { .. }
            | Self::NonContiguousTaskRunTurnSequence { .. }
            | Self::DuplicateCandidateProducingRound { .. }
            | Self::NonContiguousCandidateProducingRound { .. } => {
                TaskRuntimeInvariantSeverity::Error
            }
        }
    }

    pub fn primary_entity_id(&self) -> &str {
        match self {
            Self::DuplicateLifecycleEvents { task_id, .. }
            | Self::MultipleChildThreadLinksForRun { task_id, .. }
            | Self::InvalidDeliveredTaskResult { task_id, .. }
            | Self::DeliveryPointsToInvalidResult { task_id, .. }
            | Self::PrimaryExecutorBindingMissingLineage { task_id, .. }
            | Self::PrimaryExecutorBindingMissingExecution { task_id, .. }
            | Self::MultiplePrimaryExecutorBindingsForRun { task_id, .. }
            | Self::TaskRunTurnMissingLineage { task_id, .. }
            | Self::TaskRunTurnMissingTurn { task_id, .. }
            | Self::AcceptedCandidateMissingTurn { task_id, .. }
            | Self::SucceededRunMissingAcceptedCandidate { task_id, .. }
            | Self::AcceptedCandidateMissingResult { task_id, .. }
            | Self::AcceptedCandidateMissingFinalReviewEvent { task_id, .. }
            | Self::MultipleAcceptedCandidatesForRun { task_id, .. }
            | Self::TaskResultCandidateTurnMismatch { task_id, .. }
            | Self::TaskResultCandidatePrimaryBindingMismatch { task_id, .. }
            | Self::TaskResultCandidateRoundMismatch { task_id, .. }
            | Self::ReviewEventMissingCandidate { task_id, .. }
            | Self::ReviewEventMissingTaskRunTurn { task_id, .. }
            | Self::FinalReviewEventDecisionMismatch { task_id, .. }
            | Self::DuplicateTaskRunTurnSequence { task_id, .. }
            | Self::NonContiguousTaskRunTurnSequence { task_id, .. }
            | Self::DuplicateCandidateProducingRound { task_id, .. }
            | Self::NonContiguousCandidateProducingRound { task_id, .. } => task_id.as_str(),
            Self::StaleInProgressTurn { turn_id, .. } => turn_id.as_str(),
            Self::StaleTurnItemAttempt { item_id, .. } => item_id.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRuntimeInvariantViolation {
    pub severity: TaskRuntimeInvariantSeverity,
    pub code: String,
    pub entity_id: String,
    pub message: String,
    pub kind: TaskRuntimeInvariantViolationKind,
}

impl TaskRuntimeInvariantViolation {
    pub fn new(kind: TaskRuntimeInvariantViolationKind, message: impl Into<String>) -> Self {
        let severity = kind.default_severity();
        let code = kind.code().to_owned();
        let entity_id = kind.primary_entity_id().to_owned();
        Self {
            severity,
            code,
            entity_id,
            message: message.into(),
            kind,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRuntimeInvariantReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_at_unix: Option<i64>,
    #[serde(default)]
    pub violations: Vec<TaskRuntimeInvariantViolation>,
}

impl TaskRuntimeInvariantReport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_db_path(mut self, db_path: impl Into<String>) -> Self {
        self.db_path = Some(db_path.into());
        self
    }

    pub fn with_generated_at(mut self, generated_at_unix: i64) -> Self {
        self.generated_at_unix = Some(generated_at_unix);
        self
    }

    pub fn push(&mut self, violation: TaskRuntimeInvariantViolation) {
        self.violations.push(violation);
    }

    pub fn is_empty(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn violation_count(&self) -> usize {
        self.violations.len()
    }

    pub fn error_count(&self) -> usize {
        self.violations
            .iter()
            .filter(|violation| violation.severity == TaskRuntimeInvariantSeverity::Error)
            .count()
    }

    pub fn warning_count(&self) -> usize {
        self.violations
            .iter()
            .filter(|violation| violation.severity == TaskRuntimeInvariantSeverity::Warning)
            .count()
    }
}

impl fmt::Display for TaskRuntimeInvariantReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "task runtime invariant report: {} violation(s), {} error(s), {} warning(s)",
            self.violation_count(),
            self.error_count(),
            self.warning_count()
        )?;
        if let Some(db_path) = &self.db_path {
            writeln!(f, "db: {db_path}")?;
        }
        for violation in &self.violations {
            writeln!(
                f,
                "- [{:?}] {} {}: {}",
                violation.severity, violation.code, violation.entity_id, violation.message
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TaskRuntimeInvariantScanner {
    stale_turn_after_seconds: i64,
}

impl Default for TaskRuntimeInvariantScanner {
    fn default() -> Self {
        Self {
            stale_turn_after_seconds: 30 * 60,
        }
    }
}

impl TaskRuntimeInvariantScanner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stale_turn_after_seconds(mut self, value: i64) -> Self {
        self.stale_turn_after_seconds = value.max(0);
        self
    }

    pub async fn scan_store(
        &self,
        store: &CrudStore,
        observed_at_unix: i64,
    ) -> Result<TaskRuntimeInvariantReport> {
        let snapshot = store.load_task_runtime_invariant_snapshot().await?;
        let mut report = self.scan_snapshot(&snapshot, observed_at_unix)?;
        task_review_invariants::detect_task_review_violations(store, &mut report).await?;
        Ok(report)
    }

    pub async fn scan_sqlite_path(
        &self,
        db_path: impl AsRef<Path>,
        observed_at_unix: i64,
    ) -> Result<TaskRuntimeInvariantReport> {
        let db_path = db_path.as_ref();
        let mut options = ConnectOptions::new(sqlite_read_only_connection_url(db_path));
        options.sqlx_logging(false);
        let connection = Database::connect(options).await.with_context(|| {
            format!(
                "failed to open sqlite database `{}` for invariant scan",
                db_path.display()
            )
        })?;
        let store = CrudStore::new(connection);
        let mut report = self.scan_store(&store, observed_at_unix).await?;
        report.db_path = Some(db_path.display().to_string());
        Ok(report)
    }

    fn scan_snapshot(
        &self,
        snapshot: &TaskRuntimeInvariantSnapshot,
        observed_at_unix: i64,
    ) -> Result<TaskRuntimeInvariantReport> {
        let mut report = TaskRuntimeInvariantReport::new().with_generated_at(observed_at_unix);
        let events = load_task_events(snapshot.task_events.as_slice())?;
        self.detect_duplicate_lifecycle_events(&events, &mut report);
        self.detect_child_link_violations(&events, &mut report);
        self.detect_invalid_deliveries(snapshot.delivered_task_results.as_slice(), &mut report)?;
        self.detect_stale_turns(
            snapshot.in_progress_turns.as_slice(),
            observed_at_unix,
            &mut report,
        );
        self.detect_stale_turn_item_attempts(
            snapshot.stale_turn_item_attempts.as_slice(),
            &mut report,
        );
        Ok(report)
    }

    fn detect_duplicate_lifecycle_events(
        &self,
        events: &[TaskEventScanRow],
        report: &mut TaskRuntimeInvariantReport,
    ) {
        let mut groups: BTreeMap<String, Vec<&TaskEventScanRow>> = BTreeMap::new();
        for event in events {
            if let Some(key) = lifecycle_semantic_key(&event.payload) {
                groups.entry(key).or_default().push(event);
            }
        }

        for (semantic_key, group) in groups {
            if group.len() < 2 {
                continue;
            }
            let first = group[0];
            let event_ids = group
                .iter()
                .map(|event| event.id.clone())
                .collect::<Vec<_>>();
            let sequences = group.iter().map(|event| event.sequence).collect::<Vec<_>>();
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::DuplicateLifecycleEvents {
                    task_id: first.task_id.clone(),
                    run_id: first.run_id.clone(),
                    event_type: first.event_type.clone(),
                    semantic_key,
                    event_ids,
                    sequences,
                },
                "duplicate task lifecycle events share the same semantic key",
            ));
        }
    }

    fn detect_child_link_violations(
        &self,
        events: &[TaskEventScanRow],
        report: &mut TaskRuntimeInvariantReport,
    ) {
        let mut links_by_run: BTreeMap<String, Vec<(&TaskEventScanRow, TaskRuntimeChildLinkRef)>> =
            BTreeMap::new();

        for event in events {
            let TaskEventPayload::ChildThreadLinked { lineage } = &event.payload else {
                continue;
            };
            let link = TaskRuntimeChildLinkRef {
                event_id: event.id.clone(),
                sequence: event.sequence,
                child_thread_id: lineage.child_thread_id.clone(),
                child_turn_id: lineage.child_turn_id.clone(),
            };
            links_by_run
                .entry(lineage.task_run_id.clone())
                .or_default()
                .push((event, link));
        }

        for (run_id, links) in links_by_run {
            if links.len() < 2 {
                continue;
            }
            let task_id = links[0].0.task_id.clone();
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::MultipleChildThreadLinksForRun {
                    task_id,
                    run_id,
                    links: links.into_iter().map(|(_, link)| link).collect(),
                },
                "one task run has multiple child thread link events",
            ));
        }
    }

    fn detect_invalid_deliveries(
        &self,
        delivered_task_results: &[TaskRuntimeInvariantDeliveryRecord],
        report: &mut TaskRuntimeInvariantReport,
    ) -> Result<()> {
        for row in delivered_task_results {
            if row.run_status.as_deref() != Some("succeeded") {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::DeliveryPointsToInvalidResult {
                        task_id: row.task_id.clone(),
                        run_id: row.run_id.clone(),
                        delivery_id: row.delivery_id.clone(),
                        reason: format!(
                            "delivered task_delivery points to run status `{}`",
                            row.run_status.as_deref().unwrap_or("missing")
                        ),
                    },
                    "delivered task_delivery does not point to a succeeded task_run",
                ));
                continue;
            }

            let Some(result_json) = row.result_json.as_deref() else {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::DeliveryPointsToInvalidResult {
                        task_id: row.task_id.clone(),
                        run_id: row.run_id.clone(),
                        delivery_id: row.delivery_id.clone(),
                        reason: "succeeded run has no result_json".to_owned(),
                    },
                    "delivered task_delivery points to a run without result_json",
                ));
                continue;
            };

            let result = serde_json::from_str::<TaskResult>(result_json).with_context(|| {
                format!(
                    "failed to decode task_run.result_json for run `{}`",
                    row.run_id
                )
            })?;
            let fallback_used = task_result_bool_flag(&result, "fallbackUsed");
            let schema_valid = task_result_bool_flag(&result, "schemaValid");

            if fallback_used == Some(true) || schema_valid == Some(false) {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::InvalidDeliveredTaskResult {
                        task_id: row.task_id.clone(),
                        run_id: row.run_id.clone(),
                        delivery_id: Some(row.delivery_id.clone()),
                        reason: "delivered task result is fallback or schema-invalid".to_owned(),
                        fallback_used,
                        schema_valid,
                    },
                    "delivered task result failed acceptance invariants",
                ));
            }
        }

        Ok(())
    }

    fn detect_stale_turns(
        &self,
        in_progress_turns: &[TaskRuntimeInvariantTurnRecord],
        observed_at_unix: i64,
        report: &mut TaskRuntimeInvariantReport,
    ) {
        for row in in_progress_turns {
            let stale_for_seconds = observed_at_unix.saturating_sub(row.updated_at_unix);
            if stale_for_seconds < self.stale_turn_after_seconds {
                continue;
            }
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::StaleInProgressTurn {
                    turn_id: row.turn_id.clone(),
                    thread_id: row.thread_id.clone(),
                    stale_for_seconds,
                    observed_at_unix,
                    updated_at_unix: row.updated_at_unix,
                },
                "turn remained in_progress past the stale threshold",
            ));
        }
    }

    fn detect_stale_turn_item_attempts(
        &self,
        stale_turn_item_attempts: &[TaskRuntimeInvariantStaleAttemptRecord],
        report: &mut TaskRuntimeInvariantReport,
    ) {
        for row in stale_turn_item_attempts {
            let reason = format!(
                "terminal turn_item.status `{}` has `{}` attempt",
                row.item_status, row.attempt_status
            );
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::StaleTurnItemAttempt {
                    turn_id: row.turn_id.clone(),
                    item_id: row.item_id.clone(),
                    attempt_id: row.attempt_id.clone(),
                    item_status: row.item_status.clone(),
                    attempt_status: row.attempt_status.clone(),
                    attempt_number: Some(row.attempt_number),
                    reason,
                },
                "terminal turn_item has a nonterminal or stale attempt",
            ));
        }
    }
}

#[derive(Debug, Clone)]
struct TaskEventScanRow {
    id: String,
    task_id: String,
    run_id: Option<String>,
    sequence: i64,
    event_type: String,
    payload: TaskEventPayload,
}

fn load_task_events(rows: &[TaskRuntimeInvariantEventRecord]) -> Result<Vec<TaskEventScanRow>> {
    rows.iter()
        .map(|row| {
            let payload = serde_json::from_str::<TaskEventPayload>(row.payload_json.as_str())
                .with_context(|| format!("failed to decode task_event payload `{}`", row.id))?;
            Ok(TaskEventScanRow {
                id: row.id.clone(),
                task_id: row.task_id.clone(),
                run_id: row.run_id.clone(),
                sequence: row.sequence,
                event_type: row.event_type.clone(),
                payload,
            })
        })
        .collect()
}

fn lifecycle_semantic_key(payload: &TaskEventPayload) -> Option<String> {
    payload.idempotency_key()
}

fn task_result_bool_flag(result: &TaskResult, flag: &str) -> Option<bool> {
    let Some(TaskValue::Object(data)) = &result.data else {
        return None;
    };
    match data.get(flag) {
        Some(TaskValue::Bool(value)) => Some(*value),
        _ => None,
    }
}

fn sqlite_read_only_connection_url(path: &Path) -> String {
    let normalized_path = path.to_string_lossy().replace('\\', "/");
    let has_windows_drive_prefix = normalized_path
        .as_bytes()
        .get(1)
        .is_some_and(|value| *value == b':');
    let needs_leading_slash =
        (path.is_absolute() || has_windows_drive_prefix) && !normalized_path.starts_with('/');
    let path_part = if needs_leading_slash {
        format!("/{normalized_path}")
    } else {
        normalized_path
    };
    format!("sqlite://{path_part}?mode=ro")
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_protocol::{
        TaskError, TaskErrorClass, TaskRunThreadBinding, TaskRunThreadBindingKind, ThreadLineage,
    };
    use sea_orm::Database;

    fn violation(kind: TaskRuntimeInvariantViolationKind) -> TaskRuntimeInvariantViolation {
        TaskRuntimeInvariantViolation::new(kind, "detected test invariant violation")
    }

    #[test]
    fn constructs_core_violation_kinds_with_debuggable_ids() {
        let links = vec![
            TaskRuntimeChildLinkRef {
                event_id: "event_child_1".to_owned(),
                sequence: 3,
                child_thread_id: "child_thread_1".to_owned(),
                child_turn_id: "child_turn_1".to_owned(),
            },
            TaskRuntimeChildLinkRef {
                event_id: "event_child_2".to_owned(),
                sequence: 4,
                child_thread_id: "child_thread_2".to_owned(),
                child_turn_id: "child_turn_2".to_owned(),
            },
        ];
        let mut report = TaskRuntimeInvariantReport::new()
            .with_db_path("/tmp/gateway.db")
            .with_generated_at(1_770_000_000);

        report.push(violation(
            TaskRuntimeInvariantViolationKind::DuplicateLifecycleEvents {
                task_id: "task_1".to_owned(),
                run_id: Some("run_1".to_owned()),
                event_type: "task/run/started".to_owned(),
                semantic_key: "run:run_1:started".to_owned(),
                event_ids: vec!["event_1".to_owned(), "event_2".to_owned()],
                sequences: vec![1, 2],
            },
        ));
        report.push(violation(
            TaskRuntimeInvariantViolationKind::MultipleChildThreadLinksForRun {
                task_id: "task_1".to_owned(),
                run_id: "run_1".to_owned(),
                links,
            },
        ));
        report.push(violation(
            TaskRuntimeInvariantViolationKind::InvalidDeliveredTaskResult {
                task_id: "task_2".to_owned(),
                run_id: "run_2".to_owned(),
                delivery_id: Some("delivery_1".to_owned()),
                reason: "fallback result was delivered".to_owned(),
                fallback_used: Some(true),
                schema_valid: Some(false),
            },
        ));
        report.push(violation(
            TaskRuntimeInvariantViolationKind::DeliveryPointsToInvalidResult {
                task_id: "task_3".to_owned(),
                run_id: "run_3".to_owned(),
                delivery_id: "delivery_2".to_owned(),
                reason: "delivery references rejected run".to_owned(),
            },
        ));
        report.push(violation(
            TaskRuntimeInvariantViolationKind::StaleInProgressTurn {
                turn_id: "turn_1".to_owned(),
                thread_id: Some("thread_1".to_owned()),
                stale_for_seconds: 3600,
                observed_at_unix: 1_770_003_600,
                updated_at_unix: 1_770_000_000,
            },
        ));
        report.push(violation(
            TaskRuntimeInvariantViolationKind::StaleTurnItemAttempt {
                turn_id: "turn_2".to_owned(),
                item_id: "item_1".to_owned(),
                attempt_id: "attempt_1".to_owned(),
                item_status: "completed".to_owned(),
                attempt_status: "running".to_owned(),
                attempt_number: Some(1),
                reason: "terminal item has running attempt".to_owned(),
            },
        ));
        assert_eq!(report.violation_count(), 6);
        assert_eq!(report.error_count(), 5);
        assert_eq!(report.warning_count(), 1);
        assert!(format!("{report}").contains("duplicate_lifecycle_events task_1"));
        assert!(format!("{report}").contains("stale_in_progress_turn turn_1"));
    }

    #[test]
    fn violation_metadata_is_derived_from_kind() {
        let violation = TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::StaleTurnItemAttempt {
                turn_id: "turn_1".to_owned(),
                item_id: "item_1".to_owned(),
                attempt_id: "attempt_1".to_owned(),
                item_status: "completed".to_owned(),
                attempt_status: "running".to_owned(),
                attempt_number: Some(2),
                reason: "terminal item has running attempt".to_owned(),
            },
            "terminal item has running attempt",
        );

        assert_eq!(violation.severity, TaskRuntimeInvariantSeverity::Error);
        assert_eq!(violation.code, "stale_turn_item_attempt");
        assert_eq!(violation.entity_id, "item_1");
    }

    #[test]
    fn empty_report_has_no_errors() {
        let report = TaskRuntimeInvariantReport::new();

        assert!(report.is_empty());
        assert_eq!(report.error_count(), 0);
        assert_eq!(report.warning_count(), 0);
    }

    #[tokio::test]
    async fn scanner_reports_target_binding_violations_through_crud_store() {
        let db = Database::connect("sqlite::memory:").await.expect("sqlite");
        Migrator::up(&db, None)
            .await
            .expect("migrations should run");
        let store = CrudStore::new(db);
        store
            .upsert_task_run_thread_binding(TaskRunThreadBinding {
                id: "binding_missing_target_refs".to_owned(),
                task_id: "task_missing_target_refs".to_owned(),
                run_id: "run_missing_target_refs".to_owned(),
                execution_id: None,
                thread_id: "child_thread_missing_target_refs".to_owned(),
                binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
                created_at: 1,
            })
            .await
            .expect("binding should insert through CrudStore");

        let report = TaskRuntimeInvariantScanner::new()
            .scan_store(&store, 2)
            .await
            .expect("scan should run");

        assert!(report.violations.iter().any(|violation| matches!(
            &violation.kind,
            TaskRuntimeInvariantViolationKind::PrimaryExecutorBindingMissingLineage { .. }
        )));
        assert!(report.violations.iter().any(|violation| matches!(
            &violation.kind,
            TaskRuntimeInvariantViolationKind::PrimaryExecutorBindingMissingExecution { .. }
        )));
    }

    #[tokio::test]
    async fn scanner_detects_fixture_violations_without_repairing_rows() {
        let invalid_result = TaskResult {
            summary: Some("not a real result".to_owned()),
            data: Some(TaskValue::Object(BTreeMap::from([
                ("fallbackUsed".to_owned(), TaskValue::Bool(true)),
                ("schemaValid".to_owned(), TaskValue::Bool(false)),
            ]))),
            artifacts: Vec::new(),
            completed_by_run_id: Some("run_2".to_owned()),
        };
        let snapshot = TaskRuntimeInvariantSnapshot {
            task_events: vec![
                task_event_record(
                    "event_started_1",
                    1,
                    TaskEventPayload::RunStarted {
                        task_id: "task_1".to_owned(),
                        run_id: "run_1".to_owned(),
                        started_at: 1,
                    },
                ),
                task_event_record(
                    "event_started_2",
                    2,
                    TaskEventPayload::RunStarted {
                        task_id: "task_1".to_owned(),
                        run_id: "run_1".to_owned(),
                        started_at: 2,
                    },
                ),
                task_event_record(
                    "event_child_1",
                    3,
                    TaskEventPayload::ChildThreadLinked {
                        lineage: lineage("child_thread_1", "child_turn_1"),
                    },
                ),
                task_event_record(
                    "event_child_2",
                    4,
                    TaskEventPayload::ChildThreadLinked {
                        lineage: lineage("child_thread_2", "child_turn_2"),
                    },
                ),
            ],
            delivered_task_results: vec![TaskRuntimeInvariantDeliveryRecord {
                delivery_id: "delivery_1".to_owned(),
                task_id: "task_2".to_owned(),
                run_id: "run_2".to_owned(),
                run_status: Some("succeeded".to_owned()),
                result_json: Some(serde_json::to_string(&invalid_result).unwrap()),
            }],
            in_progress_turns: vec![TaskRuntimeInvariantTurnRecord {
                turn_id: "turn_stale".to_owned(),
                thread_id: Some("thread_1".to_owned()),
                updated_at_unix: 1_700_000_000,
            }],
            stale_turn_item_attempts: vec![TaskRuntimeInvariantStaleAttemptRecord {
                turn_id: "turn_2".to_owned(),
                item_id: "item_1".to_owned(),
                item_status: "completed".to_owned(),
                attempt_id: "attempt_1".to_owned(),
                attempt_status: "running".to_owned(),
                attempt_number: 1,
            }],
        };

        let report = TaskRuntimeInvariantScanner::new()
            .with_stale_turn_after_seconds(60)
            .scan_snapshot(&snapshot, 2_000_000_000)
            .expect("scan should succeed");
        let codes = report
            .violations
            .iter()
            .map(|violation| violation.code.as_str())
            .collect::<Vec<_>>();

        assert!(codes.contains(&"duplicate_lifecycle_events"));
        assert!(codes.contains(&"multiple_child_thread_links_for_run"));
        assert!(codes.contains(&"invalid_delivered_task_result"));
        assert!(codes.contains(&"stale_in_progress_turn"));
        assert!(codes.contains(&"stale_turn_item_attempt"));
    }

    #[tokio::test]
    async fn scanner_clean_fixture_reports_success() {
        let valid_result = TaskResult {
            summary: Some("valid result".to_owned()),
            data: Some(TaskValue::Object(BTreeMap::from([
                ("fallbackUsed".to_owned(), TaskValue::Bool(false)),
                ("schemaValid".to_owned(), TaskValue::Bool(true)),
            ]))),
            artifacts: Vec::new(),
            completed_by_run_id: Some("run_clean".to_owned()),
        };
        let snapshot = TaskRuntimeInvariantSnapshot {
            task_events: vec![task_event_record(
                "event_started_clean",
                1,
                TaskEventPayload::RunStarted {
                    task_id: "task_clean".to_owned(),
                    run_id: "run_clean".to_owned(),
                    started_at: 1,
                },
            )],
            delivered_task_results: vec![TaskRuntimeInvariantDeliveryRecord {
                delivery_id: "delivery_clean".to_owned(),
                task_id: "task_clean".to_owned(),
                run_id: "run_clean".to_owned(),
                run_status: Some("succeeded".to_owned()),
                result_json: Some(serde_json::to_string(&valid_result).unwrap()),
            }],
            in_progress_turns: Vec::new(),
            stale_turn_item_attempts: Vec::new(),
        };

        let report = TaskRuntimeInvariantScanner::new()
            .with_stale_turn_after_seconds(60)
            .scan_snapshot(&snapshot, 2_000_000_000)
            .expect("scan should succeed");

        assert!(report.is_empty(), "{report}");
    }

    #[tokio::test]
    async fn scanner_reports_contradictory_run_terminal_events() {
        let snapshot = TaskRuntimeInvariantSnapshot {
            task_events: vec![
                task_event_record(
                    "event_run_completed",
                    1,
                    TaskEventPayload::RunCompleted {
                        task_id: "task_1".to_owned(),
                        run_id: "run_1".to_owned(),
                        result: Some(TaskResult {
                            summary: Some("done".to_owned()),
                            data: None,
                            artifacts: Vec::new(),
                            completed_by_run_id: Some("run_1".to_owned()),
                        }),
                        completed_at: 10,
                    },
                ),
                task_event_record(
                    "event_run_failed",
                    2,
                    TaskEventPayload::RunFailed {
                        task_id: "task_1".to_owned(),
                        run_id: "run_1".to_owned(),
                        error: Some(TaskError {
                            code: "late_failure".to_owned(),
                            message: "late failure".to_owned(),
                            class: TaskErrorClass::Internal,
                            details: None,
                            failed_run_id: Some("run_1".to_owned()),
                        }),
                        completed_at: 11,
                    },
                ),
            ],
            delivered_task_results: Vec::new(),
            in_progress_turns: Vec::new(),
            stale_turn_item_attempts: Vec::new(),
        };

        let report = TaskRuntimeInvariantScanner::new()
            .scan_snapshot(&snapshot, 2_000_000_000)
            .expect("scan should succeed");

        assert!(
            report.violations.iter().any(|violation| matches!(
                &violation.kind,
                TaskRuntimeInvariantViolationKind::DuplicateLifecycleEvents {
                    run_id: Some(run_id),
                    semantic_key,
                    ..
                } if run_id == "run_1" && semantic_key == "run:run_1:terminal"
            )),
            "{report}"
        );
    }

    fn lineage(child_thread_id: &str, child_turn_id: &str) -> ThreadLineage {
        ThreadLineage {
            child_thread_id: child_thread_id.to_owned(),
            child_turn_id: child_turn_id.to_owned(),
            parent_thread_id: "parent_thread".to_owned(),
            parent_turn_id: Some("parent_turn".to_owned()),
            task_id: "task_1".to_owned(),
            task_run_id: "run_1".to_owned(),
            root_thread_id: "root_thread".to_owned(),
            depth: 1,
            created_at: 1,
        }
    }

    fn task_event_record(
        event_id: &str,
        sequence: i64,
        payload: TaskEventPayload,
    ) -> TaskRuntimeInvariantEventRecord {
        TaskRuntimeInvariantEventRecord {
            id: event_id.to_owned(),
            task_id: payload.task_id().to_owned(),
            run_id: payload.run_id().map(str::to_owned),
            sequence,
            event_type: payload.event_type().to_owned(),
            payload_json: serde_json::to_string(&payload).unwrap(),
        }
    }
}
