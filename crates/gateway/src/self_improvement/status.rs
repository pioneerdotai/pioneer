use super::{
    analysis_checkpoint::checkpoint_progress, settings::AuthoritativeSelfImprovementSettings,
};
use anyhow::Result;
use pioneer_crud::{CrudStore, SelfImprovementRunRecord};
use pioneer_protocol::{
    GatewaySelfImprovementStatus, SelfImprovementPhase as Phase,
    SelfImprovementStatusReason as Reason,
};

/// A read-only projection. Never starts a run, loads history, or calls a model.
pub(crate) async fn snapshot(
    store: &CrudStore,
    workspace_id: &str,
    settings: &AuthoritativeSelfImprovementSettings,
    worker_available: bool,
    now: i64,
) -> Result<GatewaySelfImprovementStatus> {
    let mut status = GatewaySelfImprovementStatus {
        workspace_id: workspace_id.to_owned(),
        phase: Phase::Waiting,
        reason: Reason::Preparing,
        observed_at_unix: now,
        last_run_at_unix: None,
        last_result: None,
        next_scheduled_at_unix: None,
        next_retry_at_unix: None,
        progress: None,
    };
    if !settings.desired_enabled {
        status.phase = Phase::Disabled;
        status.reason = Reason::Disabled;
        return Ok(status);
    }
    if !settings.effective_enabled || !worker_available {
        status.phase = Phase::Unavailable;
        status.reason = if !settings.effective_enabled {
            Reason::ModelUnavailable
        } else {
            Reason::WorkerUnavailable
        };
        return Ok(status);
    }
    status.next_scheduled_at_unix = Some(next_daily_at(now));
    let Some(state) = store
        .get_self_improvement_workspace_state(workspace_id)
        .await?
    else {
        return Ok(status);
    };
    let Some(enabled_at) = state.effective_enabled_at_unix else {
        return Ok(status);
    };
    let run = match store
        .get_oldest_unresolved_self_improvement_run(workspace_id, state.activation_epoch)
        .await?
    {
        Some(run) => Some(run),
        None => {
            store
                .get_latest_self_improvement_run(workspace_id, state.activation_epoch)
                .await?
        }
    };
    if let Some(run) = run {
        project_run(&mut status, &run, now);
        if matches!(run.status.as_str(), "completed" | "cancelled")
            && run.updated_at_unix.div_euclid(86400) < now.div_euclid(86400)
        {
            status.phase = Phase::Waiting;
            status.reason = if store
                .list_self_improvement_source_turns_after(
                    workspace_id,
                    state.cursor_source_id,
                    enabled_at,
                    1,
                )
                .await?
                .is_empty()
            {
                Reason::NoNewSources
            } else {
                Reason::AwaitingSchedule
            };
            status.progress = None;
        }
    } else {
        status.reason = if store
            .list_self_improvement_source_turns_after(
                workspace_id,
                state.cursor_source_id,
                enabled_at,
                1,
            )
            .await?
            .is_empty()
        {
            Reason::NoNewSources
        } else {
            Reason::AwaitingSchedule
        };
    }
    Ok(status)
}

fn next_daily_at(now: i64) -> i64 {
    let Some(time) = chrono::DateTime::from_timestamp(now, 0) else {
        return now;
    };
    now.saturating_add(super::supervisor::next_daily_utc_delay(time).as_secs() as i64)
}

fn project_run(
    status: &mut GatewaySelfImprovementStatus,
    run: &SelfImprovementRunRecord,
    now: i64,
) {
    status.last_run_at_unix = Some(run.created_at_unix);
    status.progress = checkpoint_progress(run);
    let error_reason = run.last_error.as_deref().map(failure_reason);
    let (phase, reason) = match run.status.as_str() {
        "running"
            if run
                .lease_expires_at_unix
                .is_some_and(|expiry| expiry <= now) =>
        {
            (Phase::Waiting, Reason::Recovering)
        }
        "running" => (
            Phase::Running,
            if status
                .progress
                .is_some_and(|p| p.processed_chunks == p.total_chunks)
            {
                Reason::Finalizing
            } else {
                Reason::Analyzing
            },
        ),
        "pending" => {
            status.next_retry_at_unix = run.next_attempt_at_unix;
            if let Some(reason) = error_reason {
                (Phase::Retrying, reason)
            } else {
                (Phase::Waiting, Reason::Pending)
            }
        }
        "failed" => {
            let reason = error_reason.unwrap_or(Reason::Unknown);
            if reason == Reason::OutputLimit {
                // The supervisor deliberately does not blindly retry an unchanged token limit.
                status.next_scheduled_at_unix = None;
            } else {
                status.next_retry_at_unix = Some(next_daily_at(run.updated_at_unix));
            }
            (Phase::Failed, reason)
        }
        "cancelled" => (Phase::Cancelled, Reason::Cancelled),
        "completed" if run.outcome.as_deref() == Some("no_change") => {
            let summary = run
                .result_summary
                .as_deref()
                .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok());
            let reason = match summary
                .as_ref()
                .and_then(|v| v.get("reason"))
                .and_then(|v| v.as_str())
            {
                Some("no_candidate") => Reason::NoCandidate,
                Some("reviewer_rejected") => Reason::ReviewerRejected,
                Some("host_validation_rejected") => Reason::ValidationRejected,
                Some("model_contract_rejected") => Reason::InvalidResponse,
                _ => Reason::Unknown,
            };
            (Phase::NoChange, reason)
        }
        "completed" => (
            Phase::Completed,
            match run.applied_action.as_deref() {
                Some("create") => Reason::Created,
                Some("update") => Reason::Updated,
                Some("rollback") => Reason::RolledBack,
                _ => Reason::Unknown,
            },
        ),
        _ => (Phase::Unavailable, Reason::Unknown),
    };
    status.phase = phase;
    status.reason = reason;
    if matches!(run.status.as_str(), "completed" | "failed" | "cancelled") {
        status.last_result = Some(reason);
    }
}

/// Only allowlisted meanings cross the UI boundary, never raw provider payloads.
fn failure_reason(error: &str) -> Reason {
    let (class, code) = error.split_once(':').unwrap_or((error, ""));
    match (class, code) {
        ("provider_timeout", _) => Reason::Timeout,
        ("max_output_tokens", _) | ("provider_termination", "model_output_token_limit") => {
            Reason::OutputLimit
        }
        ("model_contract", _) => Reason::InvalidResponse,
        ("provider_termination", "model_response_filtered") => Reason::ResponseFiltered,
        ("provider_unavailable", _) => Reason::ModelUnavailable,
        (
            "provider_termination"
            | "network_transient"
            | "rate_limit"
            | "provider_5xx"
            | "auth_expired"
            | "auth_invalid"
            | "stream_stall"
            | "stream_truncated"
            | "empty_response",
            _,
        ) => Reason::ProviderError,
        _ => Reason::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_config::GatewaySelfImprovementModelSelectionConfig;
    use pioneer_crud::NewSelfImprovementRun;
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};

    fn run() -> SelfImprovementRunRecord {
        SelfImprovementRunRecord {
            id: "run".to_owned(),
            workspace_id: "workspace".to_owned(),
            activation_epoch: 1,
            scheduled_date_utc: "2026-07-23".to_owned(),
            source_lower_exclusive: 0,
            source_upper_inclusive: 1,
            status: "running".to_owned(),
            claim_token: Some("token".to_owned()),
            claimed_by: Some("worker".to_owned()),
            lease_expires_at_unix: Some(100),
            attempt_count: 1,
            next_attempt_at_unix: None,
            learner_provider: "provider".to_owned(),
            learner_model: "model".to_owned(),
            learner_reasoning_effort: None,
            reviewer_provider: "provider".to_owned(),
            reviewer_model: "model".to_owned(),
            reviewer_reasoning_effort: None,
            pipeline_contract_version: "contract".to_owned(),
            analysis_cursor_json: None,
            analysis_digest_json: None,
            outcome: None,
            applied_action: None,
            skill_id: None,
            previous_version_id: None,
            resulting_version_id: None,
            result_summary: None,
            last_error: None,
            created_at_unix: 1,
            updated_at_unix: 1,
        }
    }

    fn blank() -> GatewaySelfImprovementStatus {
        GatewaySelfImprovementStatus {
            workspace_id: "workspace".into(),
            phase: Phase::Waiting,
            reason: Reason::Preparing,
            observed_at_unix: 50,
            last_run_at_unix: None,
            last_result: None,
            next_scheduled_at_unix: Some(86400),
            next_retry_at_unix: None,
            progress: None,
        }
    }

    #[test]
    fn phases_preserve_no_change_failures_progress_and_recovery_distinctions() {
        let mut run = run();
        run.analysis_cursor_json = Some(
            serde_json::json!({
                "schemaVersion": 2, "sourceLowerExclusive": 0, "sourceUpperInclusive": 1,
                "planFingerprint": "plan", "chunkCount": 101, "nextChunkIndex": 24,
                "validatedChunkCount": 24,
            })
            .to_string(),
        );
        let mut status = blank();
        project_run(&mut status, &run, 50);
        assert_eq!(
            (status.phase, status.reason),
            (Phase::Running, Reason::Analyzing)
        );
        assert_eq!(status.progress.unwrap().processed_chunks, 24);
        assert_eq!(status.progress.unwrap().total_chunks, 101);
        project_run(&mut status, &run, 101);
        assert_eq!(status.reason, Reason::Recovering);

        for (error, reason) in [
            ("provider_timeout:review_timeout", Reason::Timeout),
            (
                "provider_termination:model_output_token_limit",
                Reason::OutputLimit,
            ),
            (
                "max_output_tokens:provider_transport_failed",
                Reason::OutputLimit,
            ),
            (
                "model_contract:malformed_model_json",
                Reason::InvalidResponse,
            ),
            (
                "provider_termination:model_provider_error",
                Reason::ProviderError,
            ),
        ] {
            run.status = "failed".into();
            run.last_error = Some(error.into());
            let mut status = blank();
            project_run(&mut status, &run, 50);
            assert_eq!(status.phase, Phase::Failed);
            assert_eq!(status.reason, reason);
            assert_eq!(
                status.next_scheduled_at_unix.is_none(),
                reason == Reason::OutputLimit
            );
            assert_eq!(status.progress.unwrap().processed_chunks, 24);
            run.status = "pending".into();
            run.next_attempt_at_unix = Some(200);
            let mut status = blank();
            project_run(&mut status, &run, 50);
            assert_eq!(status.phase, Phase::Retrying);
            assert_eq!(status.next_retry_at_unix, Some(200));
        }
        run.status = "completed".into();
        run.last_error = None;
        run.outcome = Some("no_change".into());
        for (reason, expected) in [
            ("no_candidate", Reason::NoCandidate),
            ("reviewer_rejected", Reason::ReviewerRejected),
            ("host_validation_rejected", Reason::ValidationRejected),
        ] {
            run.result_summary = Some(serde_json::json!({"reason":reason}).to_string());
            let mut status = blank();
            project_run(&mut status, &run, 50);
            assert_eq!((status.phase, status.reason), (Phase::NoChange, expected));
        }
        run.outcome = Some("accepted".into());
        for (action, reason) in [
            ("create", Reason::Created),
            ("update", Reason::Updated),
            ("rollback", Reason::RolledBack),
        ] {
            run.applied_action = Some(action.into());
            let mut status = blank();
            project_run(&mut status, &run, 50);
            assert_eq!((status.phase, status.reason), (Phase::Completed, reason));
        }
    }

    #[test]
    fn untrusted_diagnostics_and_invalid_progress_never_leak_into_status() {
        let mut run = run();
        run.status = "failed".into();
        run.last_error = Some("private token and response text".into());
        run.analysis_cursor_json = Some("bad json".into());
        let mut status = blank();
        project_run(&mut status, &run, 50);
        assert_eq!(status.reason, Reason::Unknown);
        assert_eq!(status.progress, None);
        let json = serde_json::to_string(&status).unwrap();
        assert!(!json.contains("private"));
        assert!(!json.contains("bad json"));
        assert_eq!(next_daily_at(86400), 172800);
        assert_eq!(next_daily_at(86399), 86400);
    }

    #[tokio::test]
    async fn read_only_snapshot_distinguishes_empty_disabled_and_workspace_scoped_runs() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        db.execute_unprepared("INSERT INTO workspace (id,name,is_active,is_current) VALUES ('a','A',1,1),('b','B',1,0)").await.unwrap();
        let store = CrudStore::new(db.clone());
        let model = GatewaySelfImprovementModelSelectionConfig {
            provider: "api".into(),
            model: "model".into(),
            reasoning_effort: None,
        };
        let mut settings = AuthoritativeSelfImprovementSettings {
            desired_enabled: true,
            effective_enabled: true,
            default_model: Some(model.clone()),
            reviewer_model_override: None,
            reviewer_model: Some(model),
        };
        let a = store
            .activate_self_improvement_workspace("a", 10)
            .await
            .unwrap();
        let b = store
            .activate_self_improvement_workspace("b", 10)
            .await
            .unwrap();
        let empty = snapshot(&store, "a", &settings, true, 50).await.unwrap();
        assert_eq!(empty.reason, Reason::NoNewSources);
        assert_eq!(empty.next_scheduled_at_unix, Some(86400));
        assert!(
            store
                .get_latest_self_improvement_run("a", a.activation_epoch)
                .await
                .unwrap()
                .is_none()
        );
        let at = chrono::DateTime::from_timestamp(15, 0)
            .unwrap()
            .fixed_offset();
        for (sql, values) in [
            ("INSERT INTO thread (id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at)
              VALUES ('thread_a','a','','agent','model','api','active','user','private',?,?)", vec![at.into(), at.into()]),
            ("INSERT INTO turn (id,thread_id,status,turn_kind,origin,created_at,updated_at)
              VALUES ('turn_a','thread_a','completed','conversation','user',?,?)", vec![at.into(), at.into()]),
            ("INSERT INTO turn_event (id,thread_id,turn_id,sequence,event_type,payload,created_at)
              VALUES ('event_a','thread_a','turn_a',1,'turn/completed','{}',?)", vec![at.into()]),
            ("INSERT INTO self_improvement_source_turn (workspace_id,thread_id,turn_id,terminal_event_id,terminal_at,created_at)
              VALUES ('a','thread_a','turn_a','event_a',?,?)", vec![at.into(), at.into()]),
        ] {
            db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite, sql, values)).await.unwrap();
        }
        assert_eq!(
            snapshot(&store, "a", &settings, true, 50)
                .await
                .unwrap()
                .reason,
            Reason::AwaitingSchedule
        );
        store
            .create_or_get_self_improvement_run(
                NewSelfImprovementRun {
                    workspace_id: "a".into(),
                    activation_epoch: a.activation_epoch,
                    scheduled_date_utc: "1970-01-01".into(),
                    source_lower_exclusive: 0,
                    source_upper_inclusive: 1,
                    learner_provider: "api".into(),
                    learner_model: "model".into(),
                    learner_reasoning_effort: None,
                    reviewer_provider: "api".into(),
                    reviewer_model: "model".into(),
                    reviewer_reasoning_effort: None,
                    pipeline_contract_version: "v3".into(),
                },
                20,
            )
            .await
            .unwrap();
        assert_eq!(
            snapshot(&store, "a", &settings, true, 50)
                .await
                .unwrap()
                .reason,
            Reason::Pending
        );
        assert_eq!(
            snapshot(&store, "b", &settings, true, 50)
                .await
                .unwrap()
                .reason,
            Reason::NoNewSources
        );
        assert!(
            store
                .get_latest_self_improvement_run("b", b.activation_epoch)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_latest_self_improvement_run("a", a.activation_epoch + 1)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .get_self_improvement_workspace_state("a")
                .await
                .unwrap()
                .unwrap()
                .cursor_source_id,
            0
        );
        // Yesterday's outcome remains visible without pretending learning ran today.
        db.execute_unprepared(
            "UPDATE self_improvement_run SET status = 'completed', outcome = 'no_change',
             result_summary = '{\"reason\":\"no_candidate\"}' WHERE workspace_id = 'a';
             UPDATE self_improvement_workspace_state SET cursor_source_id = 1 WHERE workspace_id = 'a'",
        )
        .await
        .unwrap();
        let completed = snapshot(&store, "a", &settings, true, 50).await.unwrap();
        assert_eq!(completed.phase, Phase::NoChange);
        assert_eq!(completed.last_run_at_unix, Some(20));
        let tomorrow = snapshot(&store, "a", &settings, true, 86450).await.unwrap();
        assert_eq!(tomorrow.phase, Phase::Waiting);
        assert_eq!(tomorrow.reason, Reason::NoNewSources);
        assert_eq!(tomorrow.last_result, Some(Reason::NoCandidate));
        assert_eq!(tomorrow.next_scheduled_at_unix, Some(172800));
        assert_eq!(tomorrow.progress, None);

        settings.desired_enabled = false;
        assert_eq!(
            snapshot(&store, "a", &settings, true, 50)
                .await
                .unwrap()
                .phase,
            Phase::Disabled
        );
        settings.desired_enabled = true;
        assert_eq!(
            snapshot(&store, "a", &settings, false, 50)
                .await
                .unwrap()
                .reason,
            Reason::WorkerUnavailable
        );
        settings.effective_enabled = false;
        assert_eq!(
            snapshot(&store, "a", &settings, true, 50)
                .await
                .unwrap()
                .reason,
            Reason::ModelUnavailable
        );
    }
}
