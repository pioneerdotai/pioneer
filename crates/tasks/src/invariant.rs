use anyhow::{Context, Result};
use pioneer_crud::CrudStore;
use pioneer_protocol::{TaskEventPayload, TaskResult, TaskValue};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, QueryResult, Statement};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

mod task_review_migration;

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
    LineageMissingForChildLinkEvent {
        task_id: String,
        run_id: String,
        event_id: String,
        sequence: i64,
        child_thread_id: String,
        child_turn_id: String,
    },
    ChildLinkEventCanonicalLineageMismatch {
        task_id: String,
        run_id: String,
        event_id: String,
        sequence: i64,
        event_child_thread_id: String,
        event_child_turn_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        canonical_child_thread_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        canonical_child_turn_id: Option<String>,
    },
    MissingPrimaryExecutorBinding {
        task_id: String,
        run_id: String,
        child_thread_id: String,
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
    LineageChildTurnMissingTurn {
        task_id: String,
        run_id: String,
        child_thread_id: String,
        child_turn_id: String,
    },
    ExecutionChildTurnMissingTurn {
        task_id: String,
        run_id: String,
        execution_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        child_thread_id: Option<String>,
        child_turn_id: String,
    },
    LineageMissingTaskRunTurn {
        task_id: String,
        run_id: String,
        child_thread_id: String,
        child_turn_id: String,
    },
    ExecutionMissingTaskRunTurn {
        task_id: String,
        run_id: String,
        execution_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        child_thread_id: Option<String>,
        child_turn_id: String,
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
            Self::LineageMissingForChildLinkEvent { .. } => "lineage_missing_for_child_link_event",
            Self::ChildLinkEventCanonicalLineageMismatch { .. } => {
                "child_link_event_canonical_lineage_mismatch"
            }
            Self::MissingPrimaryExecutorBinding { .. } => "missing_primary_executor_binding",
            Self::PrimaryExecutorBindingMissingLineage { .. } => {
                "primary_executor_binding_missing_lineage"
            }
            Self::PrimaryExecutorBindingMissingExecution { .. } => {
                "primary_executor_binding_missing_execution"
            }
            Self::MultiplePrimaryExecutorBindingsForRun { .. } => {
                "multiple_primary_executor_bindings_for_run"
            }
            Self::LineageChildTurnMissingTurn { .. } => "lineage_child_turn_missing_turn",
            Self::ExecutionChildTurnMissingTurn { .. } => "execution_child_turn_missing_turn",
            Self::LineageMissingTaskRunTurn { .. } => "lineage_missing_task_run_turn",
            Self::ExecutionMissingTaskRunTurn { .. } => "execution_missing_task_run_turn",
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
            | Self::LineageMissingForChildLinkEvent { .. }
            | Self::ChildLinkEventCanonicalLineageMismatch { .. }
            | Self::MissingPrimaryExecutorBinding { .. }
            | Self::PrimaryExecutorBindingMissingLineage { .. }
            | Self::PrimaryExecutorBindingMissingExecution { .. }
            | Self::MultiplePrimaryExecutorBindingsForRun { .. }
            | Self::LineageChildTurnMissingTurn { .. }
            | Self::ExecutionChildTurnMissingTurn { .. }
            | Self::LineageMissingTaskRunTurn { .. }
            | Self::ExecutionMissingTaskRunTurn { .. }
            | Self::TaskRunTurnMissingTurn { .. }
            | Self::AcceptedCandidateMissingTurn { .. }
            | Self::SucceededRunMissingAcceptedCandidate { .. }
            | Self::AcceptedCandidateMissingResult { .. }
            | Self::AcceptedCandidateMissingFinalReviewEvent { .. }
            | Self::MultipleAcceptedCandidatesForRun { .. } => TaskRuntimeInvariantSeverity::Error,
        }
    }

    pub fn primary_entity_id(&self) -> &str {
        match self {
            Self::DuplicateLifecycleEvents { task_id, .. }
            | Self::MultipleChildThreadLinksForRun { task_id, .. }
            | Self::InvalidDeliveredTaskResult { task_id, .. }
            | Self::DeliveryPointsToInvalidResult { task_id, .. }
            | Self::LineageMissingForChildLinkEvent { task_id, .. }
            | Self::ChildLinkEventCanonicalLineageMismatch { task_id, .. }
            | Self::MissingPrimaryExecutorBinding { task_id, .. }
            | Self::PrimaryExecutorBindingMissingLineage { task_id, .. }
            | Self::PrimaryExecutorBindingMissingExecution { task_id, .. }
            | Self::MultiplePrimaryExecutorBindingsForRun { task_id, .. }
            | Self::LineageChildTurnMissingTurn { task_id, .. }
            | Self::ExecutionChildTurnMissingTurn { task_id, .. }
            | Self::LineageMissingTaskRunTurn { task_id, .. }
            | Self::ExecutionMissingTaskRunTurn { task_id, .. }
            | Self::TaskRunTurnMissingTurn { task_id, .. }
            | Self::AcceptedCandidateMissingTurn { task_id, .. }
            | Self::SucceededRunMissingAcceptedCandidate { task_id, .. }
            | Self::AcceptedCandidateMissingResult { task_id, .. }
            | Self::AcceptedCandidateMissingFinalReviewEvent { task_id, .. }
            | Self::MultipleAcceptedCandidatesForRun { task_id, .. } => task_id.as_str(),
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
        self.scan_connection(&store.database_connection(), observed_at_unix)
            .await
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
        let mut report = self.scan_connection(&connection, observed_at_unix).await?;
        report.db_path = Some(db_path.display().to_string());
        Ok(report)
    }

    pub async fn scan_connection<C: ConnectionTrait>(
        &self,
        db: &C,
        observed_at_unix: i64,
    ) -> Result<TaskRuntimeInvariantReport> {
        let mut report = TaskRuntimeInvariantReport::new().with_generated_at(observed_at_unix);
        let events = load_task_events(db).await?;
        self.detect_duplicate_lifecycle_events(&events, &mut report);
        self.detect_child_link_violations(db, &events, &mut report)
            .await?;
        self.detect_invalid_deliveries(db, &mut report).await?;
        self.detect_stale_turns(db, observed_at_unix, &mut report)
            .await?;
        self.detect_stale_turn_item_attempts(db, &mut report)
            .await?;
        task_review_migration::detect_migration_violations(db, &mut report).await?;
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

    async fn detect_child_link_violations<C: ConnectionTrait>(
        &self,
        db: &C,
        events: &[TaskEventScanRow],
        report: &mut TaskRuntimeInvariantReport,
    ) -> Result<()> {
        let lineage_by_run = load_thread_lineage_by_run(db).await?;
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

            match lineage_by_run.get(lineage.task_run_id.as_str()) {
                None => report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::LineageMissingForChildLinkEvent {
                        task_id: lineage.task_id.clone(),
                        run_id: lineage.task_run_id.clone(),
                        event_id: event.id.clone(),
                        sequence: event.sequence,
                        child_thread_id: lineage.child_thread_id.clone(),
                        child_turn_id: lineage.child_turn_id.clone(),
                    },
                    "task event child link has no canonical thread_lineage row for its run",
                )),
                Some(canonical)
                    if canonical.child_thread_id != lineage.child_thread_id
                        || canonical.child_turn_id != lineage.child_turn_id =>
                {
                    report.push(TaskRuntimeInvariantViolation::new(
                        TaskRuntimeInvariantViolationKind::ChildLinkEventCanonicalLineageMismatch {
                            task_id: lineage.task_id.clone(),
                            run_id: lineage.task_run_id.clone(),
                            event_id: event.id.clone(),
                            sequence: event.sequence,
                            event_child_thread_id: lineage.child_thread_id.clone(),
                            event_child_turn_id: lineage.child_turn_id.clone(),
                            canonical_child_thread_id: Some(canonical.child_thread_id.clone()),
                            canonical_child_turn_id: Some(canonical.child_turn_id.clone()),
                        },
                        "task event child link does not match canonical thread_lineage row",
                    ));
                }
                Some(_) => {}
            }
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

        Ok(())
    }

    async fn detect_invalid_deliveries<C: ConnectionTrait>(
        &self,
        db: &C,
        report: &mut TaskRuntimeInvariantReport,
    ) -> Result<()> {
        let rows = query_all(
            db,
            r#"
            select
                d.id as delivery_id,
                d.task_id as task_id,
                d.run_id as run_id,
                d.status as delivery_status,
                r.status as run_status,
                r.result_json as result_json
            from task_delivery d
            left join task_run r on r.id = d.run_id
            where d.status = 'delivered'
            order by d.created_at asc
            "#,
        )
        .await?;

        for row in rows {
            let delivery_id = get_string(&row, "delivery_id")?;
            let task_id = get_string(&row, "task_id")?;
            let run_id = get_string(&row, "run_id")?;
            let run_status = get_optional_string(&row, "run_status")?;
            let result_json = get_optional_string(&row, "result_json")?;

            if run_status.as_deref() != Some("succeeded") {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::DeliveryPointsToInvalidResult {
                        task_id,
                        run_id,
                        delivery_id,
                        reason: format!(
                            "delivered task_delivery points to run status `{}`",
                            run_status.as_deref().unwrap_or("missing")
                        ),
                    },
                    "delivered task_delivery does not point to a succeeded task_run",
                ));
                continue;
            }

            let Some(result_json) = result_json else {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::DeliveryPointsToInvalidResult {
                        task_id,
                        run_id,
                        delivery_id,
                        reason: "succeeded run has no result_json".to_owned(),
                    },
                    "delivered task_delivery points to a run without result_json",
                ));
                continue;
            };

            let result =
                serde_json::from_str::<TaskResult>(result_json.as_str()).with_context(|| {
                    format!("failed to decode task_run.result_json for run `{run_id}`")
                })?;
            let fallback_used = task_result_bool_flag(&result, "fallbackUsed");
            let schema_valid = task_result_bool_flag(&result, "schemaValid");

            if fallback_used == Some(true) || schema_valid == Some(false) {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::InvalidDeliveredTaskResult {
                        task_id,
                        run_id,
                        delivery_id: Some(delivery_id),
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

    async fn detect_stale_turns<C: ConnectionTrait>(
        &self,
        db: &C,
        observed_at_unix: i64,
        report: &mut TaskRuntimeInvariantReport,
    ) -> Result<()> {
        let rows = query_all(
            db,
            r#"
            select
                id as turn_id,
                thread_id as thread_id,
                cast(strftime('%s', updated_at) as integer) as updated_at_unix
            from turn
            where status = 'in_progress'
            order by updated_at asc
            "#,
        )
        .await?;

        for row in rows {
            let updated_at_unix = get_i64(&row, "updated_at_unix")?;
            let stale_for_seconds = observed_at_unix.saturating_sub(updated_at_unix);
            if stale_for_seconds < self.stale_turn_after_seconds {
                continue;
            }
            let turn_id = get_string(&row, "turn_id")?;
            let thread_id = get_optional_string(&row, "thread_id")?;
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::StaleInProgressTurn {
                    turn_id,
                    thread_id,
                    stale_for_seconds,
                    observed_at_unix,
                    updated_at_unix,
                },
                "turn remained in_progress past the stale threshold",
            ));
        }

        Ok(())
    }

    async fn detect_stale_turn_item_attempts<C: ConnectionTrait>(
        &self,
        db: &C,
        report: &mut TaskRuntimeInvariantReport,
    ) -> Result<()> {
        let rows = query_all(
            db,
            r#"
            select
                ti.turn_id as turn_id,
                ti.item_id as item_id,
                ti.status as item_status,
                tia.id as attempt_id,
                tia.status as attempt_status,
                tia.attempt_number as attempt_number
            from turn_item ti
            join turn_item_attempt tia
                on tia.turn_id = ti.turn_id
               and tia.item_id = ti.item_id
            where ti.status in ('completed', 'failed', 'timed_out', 'cancelled')
              and tia.status in ('running', 'timed_out')
            order by ti.turn_id asc, ti.item_id asc, tia.attempt_number asc
            "#,
        )
        .await?;

        for row in rows {
            let item_status = get_string(&row, "item_status")?;
            let attempt_status = get_string(&row, "attempt_status")?;
            let reason =
                format!("terminal turn_item.status `{item_status}` has `{attempt_status}` attempt");
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::StaleTurnItemAttempt {
                    turn_id: get_string(&row, "turn_id")?,
                    item_id: get_string(&row, "item_id")?,
                    attempt_id: get_string(&row, "attempt_id")?,
                    item_status,
                    attempt_status,
                    attempt_number: Some(get_i64(&row, "attempt_number")?),
                    reason,
                },
                "terminal turn_item has a nonterminal or stale attempt",
            ));
        }

        Ok(())
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

#[derive(Debug, Clone)]
struct ThreadLineageScanRow {
    child_thread_id: String,
    child_turn_id: String,
}

async fn load_task_events<C: ConnectionTrait>(db: &C) -> Result<Vec<TaskEventScanRow>> {
    let rows = query_all(
        db,
        r#"
        select id, task_id, run_id, sequence, event_type, payload_json
        from task_event
        order by task_id asc, sequence asc
        "#,
    )
    .await?;
    rows.into_iter()
        .map(|row| {
            let id = get_string(&row, "id")?;
            let task_id = get_string(&row, "task_id")?;
            let run_id = get_optional_string(&row, "run_id")?;
            let sequence = get_i64(&row, "sequence")?;
            let event_type = get_string(&row, "event_type")?;
            let payload_json = get_string(&row, "payload_json")?;
            let payload = serde_json::from_str::<TaskEventPayload>(payload_json.as_str())
                .with_context(|| format!("failed to decode task_event payload `{id}`"))?;
            Ok(TaskEventScanRow {
                id,
                task_id,
                run_id,
                sequence,
                event_type,
                payload,
            })
        })
        .collect()
}

async fn load_thread_lineage_by_run<C: ConnectionTrait>(
    db: &C,
) -> Result<BTreeMap<String, ThreadLineageScanRow>> {
    let rows = query_all(
        db,
        r#"
        select task_run_id, child_thread_id, child_turn_id
        from thread_lineage
        order by created_at asc
        "#,
    )
    .await?;
    let mut by_run = BTreeMap::new();
    for row in rows {
        let task_run_id = get_string(&row, "task_run_id")?;
        by_run.entry(task_run_id).or_insert(ThreadLineageScanRow {
            child_thread_id: get_string(&row, "child_thread_id")?,
            child_turn_id: get_string(&row, "child_turn_id")?,
        });
    }
    Ok(by_run)
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

async fn query_all<C: ConnectionTrait>(db: &C, sql: &str) -> Result<Vec<QueryResult>> {
    db.query_all_raw(Statement::from_string(DbBackend::Sqlite, sql.to_owned()))
        .await
        .with_context(|| format!("failed to run invariant scanner query: {sql}"))
}

async fn table_exists<C: ConnectionTrait>(db: &C, table: &str) -> Result<bool> {
    let sql = format!(
        "select name from sqlite_master where type = 'table' and name = {}",
        sqlite_string_literal(table)
    );
    let row = db
        .query_one_raw(Statement::from_string(DbBackend::Sqlite, sql.clone()))
        .await
        .with_context(|| format!("failed to check whether table exists: {table}"))?;
    Ok(row.is_some())
}

fn sqlite_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn get_string(row: &QueryResult, column: &str) -> Result<String> {
    row.try_get::<String>("", column)
        .with_context(|| format!("failed to read `{column}` as string"))
}

fn get_optional_string(row: &QueryResult, column: &str) -> Result<Option<String>> {
    row.try_get::<Option<String>>("", column)
        .with_context(|| format!("failed to read `{column}` as optional string"))
}

fn get_i64(row: &QueryResult, column: &str) -> Result<i64> {
    row.try_get::<i64>("", column)
        .with_context(|| format!("failed to read `{column}` as i64"))
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
    use pioneer_protocol::{TaskError, TaskErrorClass, ThreadLineage};
    use sea_orm::{ConnectionTrait, Database};

    fn violation(kind: TaskRuntimeInvariantViolationKind) -> TaskRuntimeInvariantViolation {
        TaskRuntimeInvariantViolation::new(kind, "detected test invariant violation")
    }

    #[test]
    fn constructs_every_violation_kind_with_debuggable_ids() {
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
        report.push(violation(
            TaskRuntimeInvariantViolationKind::LineageMissingForChildLinkEvent {
                task_id: "task_4".to_owned(),
                run_id: "run_4".to_owned(),
                event_id: "event_3".to_owned(),
                sequence: 7,
                child_thread_id: "child_thread_3".to_owned(),
                child_turn_id: "child_turn_3".to_owned(),
            },
        ));
        report.push(violation(
            TaskRuntimeInvariantViolationKind::ChildLinkEventCanonicalLineageMismatch {
                task_id: "task_5".to_owned(),
                run_id: "run_5".to_owned(),
                event_id: "event_4".to_owned(),
                sequence: 8,
                event_child_thread_id: "child_thread_4".to_owned(),
                event_child_turn_id: "child_turn_4".to_owned(),
                canonical_child_thread_id: Some("child_thread_5".to_owned()),
                canonical_child_turn_id: Some("child_turn_5".to_owned()),
            },
        ));

        assert_eq!(report.violation_count(), 8);
        assert_eq!(report.error_count(), 7);
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
    async fn scanner_detects_fixture_violations_without_repairing_rows() {
        let db = Database::connect("sqlite::memory:").await.expect("sqlite");
        create_minimal_scanner_schema(&db).await;

        insert_task_event(
            &db,
            "event_started_1",
            1,
            &TaskEventPayload::RunStarted {
                task_id: "task_1".to_owned(),
                run_id: "run_1".to_owned(),
                started_at: 1,
            },
        )
        .await;
        insert_task_event(
            &db,
            "event_started_2",
            2,
            &TaskEventPayload::RunStarted {
                task_id: "task_1".to_owned(),
                run_id: "run_1".to_owned(),
                started_at: 2,
            },
        )
        .await;
        insert_task_event(
            &db,
            "event_child_1",
            3,
            &TaskEventPayload::ChildThreadLinked {
                lineage: lineage("child_thread_1", "child_turn_1"),
            },
        )
        .await;
        insert_task_event(
            &db,
            "event_child_2",
            4,
            &TaskEventPayload::ChildThreadLinked {
                lineage: lineage("child_thread_2", "child_turn_2"),
            },
        )
        .await;
        execute(
            &db,
            "insert into thread_lineage(child_thread_id, child_turn_id, parent_thread_id, parent_turn_id, task_id, task_run_id, root_thread_id, depth, created_at)
             values ('child_thread_1', 'child_turn_1', 'parent_thread', 'parent_turn', 'task_1', 'run_1', 'root_thread', 1, '2026-05-15 00:00:00')",
        )
        .await;

        let invalid_result = TaskResult {
            summary: Some("not a real result".to_owned()),
            data: Some(TaskValue::Object(BTreeMap::from([
                ("fallbackUsed".to_owned(), TaskValue::Bool(true)),
                ("schemaValid".to_owned(), TaskValue::Bool(false)),
            ]))),
            artifacts: Vec::new(),
            completed_by_run_id: Some("run_2".to_owned()),
        };
        execute(
            &db,
            format!(
                "insert into task_run(id, task_id, status, result_json) values ('run_2', 'task_2', 'succeeded', {})",
                sql_literal(&serde_json::to_string(&invalid_result).unwrap())
            )
            .as_str(),
        )
        .await;
        execute(
            &db,
            "insert into task_delivery(id, task_id, run_id, status, created_at) values ('delivery_1', 'task_2', 'run_2', 'delivered', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into turn(id, thread_id, status, updated_at) values ('turn_stale', 'thread_1', 'in_progress', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into turn_item(id, turn_id, item_id, item_type, status) values ('turn_item_row', 'turn_2', 'item_1', 'dynamic_tool_call', 'completed')",
        )
        .await;
        execute(
            &db,
            "insert into turn_item_attempt(id, turn_id, item_id, item_type, attempt_number, status) values ('attempt_1', 'turn_2', 'item_1', 'dynamic_tool_call', 1, 'running')",
        )
        .await;

        let report = TaskRuntimeInvariantScanner::new()
            .with_stale_turn_after_seconds(60)
            .scan_connection(&db, 2_000_000_000)
            .await
            .expect("scan should succeed");
        let codes = report
            .violations
            .iter()
            .map(|violation| violation.code.as_str())
            .collect::<Vec<_>>();

        assert!(codes.contains(&"duplicate_lifecycle_events"));
        assert!(codes.contains(&"multiple_child_thread_links_for_run"));
        assert!(codes.contains(&"child_link_event_canonical_lineage_mismatch"));
        assert!(codes.contains(&"invalid_delivered_task_result"));
        assert!(codes.contains(&"stale_in_progress_turn"));
        assert!(codes.contains(&"stale_turn_item_attempt"));
    }

    #[tokio::test]
    async fn scanner_clean_fixture_reports_success() {
        let db = Database::connect("sqlite::memory:").await.expect("sqlite");
        create_minimal_scanner_schema(&db).await;

        insert_task_event(
            &db,
            "event_started_clean",
            1,
            &TaskEventPayload::RunStarted {
                task_id: "task_clean".to_owned(),
                run_id: "run_clean".to_owned(),
                started_at: 1,
            },
        )
        .await;
        let valid_result = TaskResult {
            summary: Some("valid result".to_owned()),
            data: Some(TaskValue::Object(BTreeMap::from([
                ("fallbackUsed".to_owned(), TaskValue::Bool(false)),
                ("schemaValid".to_owned(), TaskValue::Bool(true)),
            ]))),
            artifacts: Vec::new(),
            completed_by_run_id: Some("run_clean".to_owned()),
        };
        execute(
            &db,
            format!(
                "insert into task_run(id, task_id, status, result_json) values ('run_clean', 'task_clean', 'succeeded', {})",
                sql_literal(&serde_json::to_string(&valid_result).unwrap())
            )
            .as_str(),
        )
        .await;
        execute(
            &db,
            "insert into task_delivery(id, task_id, run_id, status, created_at) values ('delivery_clean', 'task_clean', 'run_clean', 'delivered', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into turn(id, thread_id, status, updated_at) values ('turn_clean', 'thread_clean', 'completed', '2026-05-15 00:00:00')",
        )
        .await;

        let report = TaskRuntimeInvariantScanner::new()
            .with_stale_turn_after_seconds(60)
            .scan_connection(&db, 2_000_000_000)
            .await
            .expect("scan should succeed");

        assert!(report.is_empty(), "{report}");
    }

    #[tokio::test]
    async fn scanner_clean_task_review_migration_fixture_reports_success() {
        let db = Database::connect("sqlite::memory:").await.expect("sqlite");
        create_minimal_scanner_schema(&db).await;
        create_task_review_migration_scanner_schema(&db).await;

        execute(
            &db,
            "insert into thread_lineage(child_thread_id, child_turn_id, parent_thread_id, parent_turn_id, task_id, task_run_id, root_thread_id, depth, created_at)
             values ('child_thread_review_clean', 'child_turn_review_clean', 'parent_thread', 'parent_turn', 'task_review_clean', 'run_review_clean', 'root_thread', 1, '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into turn(id, thread_id, status, updated_at) values ('child_turn_review_clean', 'child_thread_review_clean', 'completed', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_thread_binding(id, task_id, run_id, execution_id, thread_id, binding_kind, created_at)
             values ('binding_review_clean', 'task_review_clean', 'run_review_clean', 'execution_review_clean', 'child_thread_review_clean', 'primary_executor', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_turn(id, task_id, run_id, turn_id, created_at)
             values ('task_run_turn_review_clean', 'task_review_clean', 'run_review_clean', 'child_turn_review_clean', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_result_candidate(id, task_id, run_id, task_run_turn_id, status, result_json, final_review_event_id, created_at)
             values ('candidate_review_clean', 'task_review_clean', 'run_review_clean', 'task_run_turn_review_clean', 'accepted', '{\"summary\":\"ok\"}', 'review_event_clean', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_result_review_event(id, candidate_id)
             values ('review_event_clean', 'candidate_review_clean')",
        )
        .await;

        let report = TaskRuntimeInvariantScanner::new()
            .scan_connection(&db, 2_000_000_000)
            .await
            .expect("scan should succeed");

        assert!(report.is_empty(), "{report}");
    }

    #[tokio::test]
    async fn scanner_reports_task_review_migration_violations() {
        let db = Database::connect("sqlite::memory:").await.expect("sqlite");
        create_minimal_scanner_schema(&db).await;
        create_task_review_migration_scanner_schema(&db).await;

        execute(
            &db,
            "insert into thread_lineage(child_thread_id, child_turn_id, parent_thread_id, parent_turn_id, task_id, task_run_id, root_thread_id, depth, created_at)
             values ('child_thread_missing_binding', 'child_turn_missing_binding', 'parent_thread', 'parent_turn', 'task_review_bad', 'run_review_bad', 'root_thread', 1, '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into thread_lineage(child_thread_id, child_turn_id, parent_thread_id, parent_turn_id, task_id, task_run_id, root_thread_id, depth, created_at)
             values ('child_thread_missing_execution', 'child_turn_missing_execution', 'parent_thread', 'parent_turn', 'task_review_missing_execution', 'run_review_missing_execution', 'root_thread', 1, '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_thread_binding(id, task_id, run_id, execution_id, thread_id, binding_kind, created_at)
             values ('binding_missing_lineage', 'task_review_orphan', 'run_review_orphan', 'execution_review_orphan', 'child_thread_orphan', 'primary_executor', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_thread_binding(id, task_id, run_id, execution_id, thread_id, binding_kind, created_at)
             values ('binding_missing_execution', 'task_review_missing_execution', 'run_review_missing_execution', null, 'child_thread_missing_execution', 'primary_executor', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_execution(id, task_id, task_run_id, executor_kind, status, child_thread_id, child_turn_id, created_at)
             values ('execution_missing_binding', 'task_review_execution_missing_binding', 'run_review_execution_missing_binding', 'agent', 'succeeded', 'child_thread_execution_missing_binding', 'child_turn_execution_missing_binding', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_thread_binding(id, task_id, run_id, execution_id, thread_id, binding_kind, created_at)
             values ('binding_duplicate_primary_one', 'task_review_duplicate_primary', 'run_review_duplicate_primary', 'execution_duplicate_primary_one', 'child_thread_duplicate_primary_one', 'primary_executor', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_thread_binding(id, task_id, run_id, execution_id, thread_id, binding_kind, created_at)
             values ('binding_duplicate_primary_two', 'task_review_duplicate_primary', 'run_review_duplicate_primary', 'execution_duplicate_primary_two', 'child_thread_duplicate_primary_two', 'primary_executor', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into thread_lineage(child_thread_id, child_turn_id, parent_thread_id, parent_turn_id, task_id, task_run_id, root_thread_id, depth, created_at)
             values ('child_thread_lineage_no_trt', 'child_turn_lineage_no_trt', 'parent_thread', 'parent_turn', 'task_review_lineage_no_trt', 'run_review_lineage_no_trt', 'root_thread', 1, '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into turn(id, thread_id, status, updated_at)
             values ('child_turn_lineage_no_trt', 'child_thread_lineage_no_trt', 'completed', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_thread_binding(id, task_id, run_id, execution_id, thread_id, binding_kind, created_at)
             values ('binding_lineage_no_trt', 'task_review_lineage_no_trt', 'run_review_lineage_no_trt', 'execution_lineage_no_trt', 'child_thread_lineage_no_trt', 'primary_executor', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_execution(id, task_id, task_run_id, executor_kind, status, child_thread_id, child_turn_id, created_at)
             values ('execution_no_trt', 'task_review_execution_no_trt', 'run_review_execution_no_trt', 'agent', 'succeeded', 'child_thread_execution_no_trt', 'child_turn_execution_no_trt', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into turn(id, thread_id, status, updated_at)
             values ('child_turn_execution_no_trt', 'child_thread_execution_no_trt', 'completed', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_thread_binding(id, task_id, run_id, execution_id, thread_id, binding_kind, created_at)
             values ('binding_execution_no_trt', 'task_review_execution_no_trt', 'run_review_execution_no_trt', 'execution_no_trt', 'child_thread_execution_no_trt', 'primary_executor', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_turn(id, task_id, run_id, turn_id, created_at)
             values ('task_run_turn_missing_turn', 'task_review_bad', 'run_review_bad', 'missing_turn', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_result_candidate(id, task_id, run_id, task_run_turn_id, status, final_review_event_id, created_at)
             values ('candidate_missing_turn', 'task_review_bad', 'run_review_bad', 'missing_task_run_turn', 'accepted', 'missing_review_event', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_result_candidate(id, task_id, run_id, task_run_turn_id, status, final_review_event_id, created_at)
             values ('candidate_duplicate_one', 'task_review_dup', 'run_review_dup', 'missing_task_run_turn_1', 'accepted', null, '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_result_candidate(id, task_id, run_id, task_run_turn_id, status, final_review_event_id, created_at)
             values ('candidate_duplicate_two', 'task_review_dup', 'run_review_dup', 'missing_task_run_turn_2', 'accepted', null, '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run(id, task_id, status, result_json)
             values ('run_review_missing_candidate', 'task_review_missing_candidate', 'succeeded', '{\"summary\":\"missing candidate\"}')",
        )
        .await;
        execute(
            &db,
            "insert into turn(id, thread_id, status, updated_at)
             values ('turn_review_missing_candidate', 'thread_review_missing_candidate', 'completed', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_turn(id, task_id, run_id, turn_id, created_at)
             values ('task_run_turn_missing_candidate', 'task_review_missing_candidate', 'run_review_missing_candidate', 'turn_review_missing_candidate', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into turn(id, thread_id, status, updated_at)
             values ('turn_review_missing_result', 'thread_review_missing_result', 'completed', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_run_turn(id, task_id, run_id, turn_id, created_at)
             values ('task_run_turn_missing_result', 'task_review_missing_result', 'run_review_missing_result', 'turn_review_missing_result', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_result_candidate(id, task_id, run_id, task_run_turn_id, status, result_json, final_review_event_id, created_at)
             values ('candidate_missing_result', 'task_review_missing_result', 'run_review_missing_result', 'task_run_turn_missing_result', 'accepted', null, 'review_missing_result', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into task_result_review_event(id, candidate_id)
             values ('review_missing_result', 'candidate_missing_result')",
        )
        .await;

        let report = TaskRuntimeInvariantScanner::new()
            .scan_connection(&db, 2_000_000_000)
            .await
            .expect("scan should succeed");
        let codes = report
            .violations
            .iter()
            .map(|violation| violation.code.as_str())
            .collect::<Vec<_>>();

        assert!(
            codes.contains(&"missing_primary_executor_binding"),
            "{report}"
        );
        assert!(
            codes
                .iter()
                .filter(|code| **code == "missing_primary_executor_binding")
                .count()
                >= 2,
            "{report}"
        );
        assert!(
            codes.contains(&"primary_executor_binding_missing_lineage"),
            "{report}"
        );
        assert!(
            codes.contains(&"primary_executor_binding_missing_execution"),
            "{report}"
        );
        assert!(
            codes.contains(&"multiple_primary_executor_bindings_for_run"),
            "{report}"
        );
        assert!(
            codes.contains(&"lineage_child_turn_missing_turn"),
            "{report}"
        );
        assert!(
            codes.contains(&"execution_child_turn_missing_turn"),
            "{report}"
        );
        assert!(codes.contains(&"lineage_missing_task_run_turn"), "{report}");
        assert!(
            codes.contains(&"execution_missing_task_run_turn"),
            "{report}"
        );
        assert!(codes.contains(&"task_run_turn_missing_turn"), "{report}");
        assert!(
            codes.contains(&"accepted_candidate_missing_turn"),
            "{report}"
        );
        assert!(
            codes.contains(&"succeeded_run_missing_accepted_candidate"),
            "{report}"
        );
        assert!(
            codes.contains(&"accepted_candidate_missing_result"),
            "{report}"
        );
        assert!(
            codes.contains(&"accepted_candidate_missing_final_review_event"),
            "{report}"
        );
        assert!(
            codes.contains(&"multiple_accepted_candidates_for_run"),
            "{report}"
        );
    }

    #[tokio::test]
    async fn baseline_failure_fixture_reports_current_db_diagnosis() {
        let db = Database::connect("sqlite::memory:").await.expect("sqlite");
        create_minimal_scanner_schema(&db).await;

        insert_task_event(
            &db,
            "event_child_current_1",
            1,
            &TaskEventPayload::ChildThreadLinked {
                lineage: lineage("child_thread_current_1", "child_turn_current_1"),
            },
        )
        .await;
        insert_task_event(
            &db,
            "event_child_current_2",
            2,
            &TaskEventPayload::ChildThreadLinked {
                lineage: lineage("child_thread_current_2", "child_turn_current_2"),
            },
        )
        .await;
        execute(
            &db,
            "insert into thread_lineage(child_thread_id, child_turn_id, parent_thread_id, parent_turn_id, task_id, task_run_id, root_thread_id, depth, created_at)
             values ('child_thread_current_1', 'child_turn_current_1', 'parent_thread', 'parent_turn', 'task_1', 'run_1', 'root_thread', 1, '2026-05-15 00:00:00')",
        )
        .await;

        let fallback_result = TaskResult {
            summary: Some("fallback result".to_owned()),
            data: Some(TaskValue::Object(BTreeMap::from([
                ("fallbackUsed".to_owned(), TaskValue::Bool(true)),
                ("schemaValid".to_owned(), TaskValue::Bool(false)),
            ]))),
            artifacts: Vec::new(),
            completed_by_run_id: Some("run_fallback".to_owned()),
        };
        execute(
            &db,
            format!(
                "insert into task_run(id, task_id, status, result_json) values ('run_fallback', 'task_fallback', 'succeeded', {})",
                sql_literal(&serde_json::to_string(&fallback_result).unwrap())
            )
            .as_str(),
        )
        .await;
        execute(
            &db,
            "insert into task_delivery(id, task_id, run_id, status, created_at) values ('delivery_fallback', 'task_fallback', 'run_fallback', 'delivered', '2026-05-15 00:00:00')",
        )
        .await;
        execute(
            &db,
            "insert into turn(id, thread_id, status, updated_at) values ('turn_current_stale', 'thread_current', 'in_progress', '2026-05-15 00:00:00')",
        )
        .await;

        let report = TaskRuntimeInvariantScanner::new()
            .with_stale_turn_after_seconds(60)
            .scan_connection(&db, 2_000_000_000)
            .await
            .expect("scan should succeed");

        assert!(
            report.violations.iter().any(|violation| matches!(
                violation.kind,
                TaskRuntimeInvariantViolationKind::DuplicateLifecycleEvents { .. }
            )),
            "{report}"
        );
        assert!(
            report.violations.iter().any(|violation| matches!(
                violation.kind,
                TaskRuntimeInvariantViolationKind::MultipleChildThreadLinksForRun { .. }
            )),
            "{report}"
        );
        assert!(
            report.violations.iter().any(|violation| matches!(
                violation.kind,
                TaskRuntimeInvariantViolationKind::InvalidDeliveredTaskResult { .. }
            )),
            "{report}"
        );
        assert!(
            report.violations.iter().any(|violation| matches!(
                violation.kind,
                TaskRuntimeInvariantViolationKind::StaleInProgressTurn { .. }
            )),
            "{report}"
        );
    }

    #[tokio::test]
    async fn scanner_reports_contradictory_run_terminal_events() {
        let db = Database::connect("sqlite::memory:").await.expect("sqlite");
        create_minimal_scanner_schema(&db).await;

        insert_task_event(
            &db,
            "event_run_completed",
            1,
            &TaskEventPayload::RunCompleted {
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
        )
        .await;
        insert_task_event(
            &db,
            "event_run_failed",
            2,
            &TaskEventPayload::RunFailed {
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
        )
        .await;

        let report = TaskRuntimeInvariantScanner::new()
            .scan_connection(&db, 2_000_000_000)
            .await
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

    async fn create_minimal_scanner_schema<C: ConnectionTrait>(db: &C) {
        for statement in [
            "create table task_event(id text primary key, task_id text not null, run_id text, thread_id text, turn_id text, sequence integer not null, event_type text not null, payload_json text not null, created_at text not null)",
            "create table thread_lineage(child_thread_id text primary key, child_turn_id text not null, parent_thread_id text not null, parent_turn_id text, task_id text not null, task_run_id text not null, root_thread_id text not null, depth integer not null, created_at text not null)",
            "create table task_run(id text primary key, task_id text not null, status text not null, result_json text)",
            "create table task_run_execution(id text primary key, task_id text not null, task_run_id text not null, executor_kind text not null, status text not null, child_thread_id text, child_turn_id text, created_at text not null)",
            "create table task_delivery(id text primary key, task_id text not null, run_id text not null, status text not null, created_at text not null)",
            "create table turn(id text primary key, thread_id text not null, status text not null, updated_at text not null)",
            "create table turn_item(id text primary key, turn_id text not null, item_id text not null, item_type text not null, status text)",
            "create table turn_item_attempt(id text primary key, turn_id text not null, item_id text not null, item_type text not null, attempt_number integer not null, status text not null)",
        ] {
            execute(db, statement).await;
        }
    }

    async fn create_task_review_migration_scanner_schema<C: ConnectionTrait>(db: &C) {
        for statement in [
            "create table task_run_thread_binding(id text primary key, task_id text not null, run_id text not null, execution_id text, thread_id text not null, binding_kind text not null, created_at text not null)",
            "create table task_run_turn(id text primary key, task_id text not null, run_id text not null, turn_id text not null, created_at text not null)",
            "create table task_result_candidate(id text primary key, task_id text not null, run_id text not null, task_run_turn_id text not null, status text not null, result_json text, final_review_event_id text, created_at text not null)",
            "create table task_result_review_event(id text primary key, candidate_id text not null)",
        ] {
            execute(db, statement).await;
        }
    }

    async fn insert_task_event<C: ConnectionTrait>(
        db: &C,
        event_id: &str,
        sequence: i64,
        payload: &TaskEventPayload,
    ) {
        execute(
            db,
            format!(
                "insert into task_event(id, task_id, run_id, thread_id, turn_id, sequence, event_type, payload_json, created_at)
                 values ({}, {}, {}, null, null, {sequence}, {}, {}, '2026-05-15 00:00:00')",
                sql_literal(event_id),
                sql_literal(payload.task_id()),
                optional_sql_literal(payload.run_id()),
                sql_literal(payload.event_type()),
                sql_literal(&serde_json::to_string(payload).unwrap())
            )
            .as_str(),
        )
        .await;
    }

    async fn execute<C: ConnectionTrait>(db: &C, sql: &str) {
        db.execute_raw(Statement::from_string(DbBackend::Sqlite, sql.to_owned()))
            .await
            .unwrap_or_else(|error| panic!("failed SQL `{sql}`: {error}"));
    }

    fn optional_sql_literal(value: Option<&str>) -> String {
        value.map(sql_literal).unwrap_or_else(|| "null".to_owned())
    }

    fn sql_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }
}
