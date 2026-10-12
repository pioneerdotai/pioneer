//! Deferred deterministic contract fixtures. No harness execution before acceptance.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn review(index: usize) -> ReviewRequiredTaskObservation {
    ReviewRequiredTaskObservation {
        task_id: format!("task_{index:03}"),
        run_id: format!("run_{index:03}"),
        candidate_id: format!("candidate_{index:03}"),
        candidate_revision: "a".repeat(64),
        title: "Review output".into(),
        status: "waiting_review".into(),
        candidate_status: "pending_review".into(),
        round: 1,
        summary: Some(format!("Result {index}")),
        result_preview: None,
        extraction_error_preview: None,
        diagnostics: vec![],
        child_thread_id: None,
        child_turn_id: None,
        max_revision_rounds: 2,
        remaining_revision_rounds: 2,
        allowed_actions: vec![
            "task_accept".into(),
            "task_revise".into(),
            "task_cancel".into(),
        ],
        revision_blocked_reason: None,
    }
}

#[test]
fn all_review_pages_survive_restart_and_exact_revision_changes() {
    for count in [21, 40, 41] {
        let mut records = (0..count).rev().map(review).collect::<Vec<_>>();
        let mut delivered = BTreeSet::new();
        let mut seen = BTreeSet::new();
        while let Some(page) = render_review_required_snapshot(&records, &delivered) {
            assert!(page.observations.len() <= 20);
            for item in &page.observations {
                assert!(seen.insert(item.candidate_id.clone()));
            }
            delivered.extend(page.signatures);
            // The actual durable cursor field uses the same serde representation.
            delivered = serde_json::from_slice(&serde_json::to_vec(&delivered).unwrap()).unwrap();
            records.reverse();
            retain_current_review_delivery(&records, &mut delivered).unwrap();
        }
        assert_eq!(seen.len(), count);
        assert_eq!(records.len(), count, "delivery never accepts a candidate");
        records[0].candidate_id = "new_revision_candidate".into();
        records[0].round += 1;
        retain_current_review_delivery(&records, &mut delivered).unwrap();
        let changed = render_review_required_snapshot(&records, &delivered).unwrap();
        assert_eq!(changed.observations.len(), 1);
        assert_eq!(
            changed.observations[0].candidate_id,
            "new_revision_candidate"
        );
        assert!(delivered.len() <= count);
    }
}

#[test]
fn same_candidate_identity_with_new_authoritative_result_revision_is_delivered_again() {
    let mut record = review(0);
    let mut delivered = BTreeSet::from([review_required_observation_signature(&record)]);
    record.candidate_revision = "b".repeat(64);
    retain_current_review_delivery(&[record.clone()], &mut delivered).unwrap();
    assert!(delivered.is_empty());
    assert_eq!(
        render_review_required_snapshot(&[record.clone()], &delivered)
            .unwrap()
            .observations
            .len(),
        1
    );
    record.candidate_revision.clear();
    assert!(retain_current_review_delivery(&[record], &mut delivered).is_err());
}

fn pending() -> TaskFinalizationSnapshot {
    TaskFinalizationSnapshot {
        revision: "liveness-revision-1".into(),
        pending: vec![PendingAttachedTask {
            task_id: "child".into(),
            run_id: Some("run".into()),
            title: "Child".into(),
            status: "running".into(),
        }],
        ..Default::default()
    }
}

struct WaitingTasks {
    polls: AtomicUsize,
    change_after: usize,
    revoked: bool,
}
#[async_trait::async_trait]
impl TaskToolProvider for WaitingTasks {
    fn terminal_cleanup_runtime_contract(&self) -> &'static str {
        "test.wait.v1"
    }
    async fn materialize_task_tools(
        &self,
        _: TaskTurnContext,
    ) -> Result<TaskToolMaterialization, String> {
        Ok(Default::default())
    }
    async fn pending_attached_tasks(
        &self,
        _: TaskTurnContext,
    ) -> Result<Vec<PendingAttachedTask>, String> {
        Ok(pending().pending)
    }
    async fn review_required_attached_task_observations(
        &self,
        _: TaskTurnContext,
    ) -> Result<Vec<ReviewRequiredTaskObservation>, String> {
        Ok(vec![])
    }
    async fn terminal_attached_task_observations(
        &self,
        _: TaskTurnContext,
    ) -> Result<Vec<TerminalTaskObservation>, String> {
        Ok(vec![])
    }
    async fn cleanup_attached_tasks(&self, _: TaskTurnContext, _: String) -> Result<(), String> {
        Ok(())
    }
    async fn cleanup_attached_tasks_idempotent(
        &self,
        _: &str,
        _: TaskTurnContext,
        _: String,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn wait_for_attached_task_finalization_snapshot(
        &self,
        _: TaskTurnContext,
        timeout_ms: u64,
    ) -> Result<TaskFinalizationSnapshot, String> {
        tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
        let poll = self.polls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.revoked {
            return Err("revoked".into());
        }
        let mut snapshot = pending();
        snapshot.revision = format!("heartbeat-revision-{poll}");
        if poll >= self.change_after {
            snapshot.pending.clear();
        }
        Ok(snapshot)
    }
}

fn wait_proof() -> pioneer_protocol::NativeAttachedTaskWait {
    pioneer_protocol::NativeAttachedTaskWait {
        schema_version: 1,
        baseline: attached_task_semantic_revision(&pending()).unwrap(),
        item_id: "reasoning".into(),
        checkpoint_id: pioneer_protocol::native_semantic_checkpoint_id("turn"),
        owner_generation: 1,
        runtime_generation: 1,
    }
}

#[tokio::test(start_paused = true)]
async fn unchanged_timeouts_and_liveness_revisions_remain_one_owned_host_wait() {
    let tasks = Arc::new(WaitingTasks {
        polls: AtomicUsize::new(0),
        change_after: 4,
        revoked: false,
    });
    let owner: Arc<dyn TaskToolProvider> = tasks.clone();
    let (command, _commands) = tokio::sync::mpsc::channel(8);
    let control = TurnExecutionControl::new(command, 1);
    let hub = AgentEventHub::new();
    let wait: pioneer_protocol::NativeAttachedTaskWait =
        serde_json::from_value(serde_json::to_value(wait_proof()).unwrap()).unwrap();
    let started = tokio::time::Instant::now();
    let snapshot =
        await_attached_task_change(Some(&owner), &wait, "ws", "thread", "turn", &control, &hub)
            .await
            .unwrap();
    assert!(snapshot.pending.is_empty());
    assert_eq!(tasks.polls.load(Ordering::SeqCst), 4);
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(4 * ATTACHED_TASK_FINALIZATION_WAIT_MS)
    );
    let mut durable = hub.take_durable_receiver().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::ZERO, durable.recv())
            .await
            .is_err(),
        "unchanged wait produces no repeated durable control/provider accounting event"
    );
}

#[tokio::test(start_paused = true)]
async fn stop_drops_wait_and_revocation_stops_before_any_provider_dispatch() {
    for revoked in [false, true] {
        let tasks = Arc::new(WaitingTasks {
            polls: AtomicUsize::new(0),
            change_after: usize::MAX,
            revoked,
        });
        let owner: Arc<dyn TaskToolProvider> = tasks.clone();
        let (command, _commands) = tokio::sync::mpsc::channel(8);
        let control = TurnExecutionControl::new(command, 1);
        let hub = AgentEventHub::new();
        let wait = wait_proof();
        let result = if revoked {
            await_attached_task_change(Some(&owner), &wait, "ws", "thread", "turn", &control, &hub)
                .await
        } else {
            let cancel = control.cancellation_token();
            tokio::join!(
                await_attached_task_change(
                    Some(&owner),
                    &wait,
                    "ws",
                    "thread",
                    "turn",
                    &control,
                    &hub
                ),
                async {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    cancel.cancel();
                }
            )
            .0
        };
        assert!(matches!(result, Err(ChatTurnError::Blocked(_))));
        assert_eq!(
            tasks.polls.load(Ordering::SeqCst),
            if revoked { 3 } else { 0 }
        );
        let polls = tasks.polls.load(Ordering::SeqCst);
        tokio::time::advance(Duration::from_secs(120)).await;
        assert_eq!(
            tasks.polls.load(Ordering::SeqCst),
            polls,
            "no detached observation survives Stop"
        );
    }
}

#[test]
fn waiting_revision_excludes_lease_and_detects_actual_task_transition() {
    let snapshot = pending();
    let mut other = snapshot.clone();
    other.revision = "lease-renewed".into();
    other.pending[0].title = "cosmetic title".into();
    assert_eq!(
        attached_task_semantic_revision(&snapshot).unwrap(),
        attached_task_semantic_revision(&other).unwrap()
    );
    other.pending[0].status = "waiting_review".into();
    assert_ne!(
        attached_task_semantic_revision(&snapshot).unwrap(),
        attached_task_semantic_revision(&other).unwrap()
    );
}

#[test]
fn full_native_checkpoint_restores_wait_and_review_suffix_without_fresh_state() {
    let mut context = super::tests::execution_checkpoint_context_fixture();
    let budget = pioneer_tools::ToolRetryBudgetConfig::default();
    let controller = pioneer_tools::ToolRetryController::new(budget.clone());
    let lifecycle = super::tool_retry_lifecycle::ToolRetryLifecycleTracker::default();
    let records = (0..41).map(review).collect::<Vec<_>>();
    let delivered = BTreeSet::from_iter(
        records[..20]
            .iter()
            .map(review_required_observation_signature),
    );
    let wait = Some(pioneer_protocol::NativeAttachedTaskWait {
        checkpoint_id: pioneer_protocol::native_semantic_checkpoint_id(&context.payload.turn_id),
        ..wait_proof()
    });
    context.payload.native_semantic_state = Some(
        super::semantic_state::capture(
            5,
            &wait,
            &context.payload.turn_id,
            &budget,
            &super::native_control::NativeControlState::default(),
            &[],
            pioneer_tools::ToolNoProgressMode::Enforce,
            &pioneer_tools::ToolProgressFrontier::default(),
            &controller,
            &lifecycle,
            &Some("original pending retry".into()),
            &None,
            &delivered,
            &BTreeSet::new(),
            &TaskMutationFinalizationGuard::default(),
            "original summary",
            &[],
            &[],
        )
        .unwrap(),
    );
    context.payload.window.progress = Some(pioneer_protocol::ExecutionProgressSummary {
        schema_version: 1,
        baseline: [0; 3],
        frontier: [0; 3],
    });
    context.window_index = context.payload.window.window_index;
    let restore = |context: &ExecutionCheckpointContext| {
        super::semantic_state::NativeSemanticState::restore(
            Some(context),
            &context.payload.workspace_id,
            &context.payload.thread_id,
            &context.payload.turn_id,
            context.window_index + 1,
            &budget,
            false,
            pioneer_tools::ToolNoProgressMode::Enforce,
        )
    };
    let cold = restore(&context).unwrap();
    assert_eq!(cold.attached_task_wait, wait);
    assert_eq!(
        cold.pending_retry_instruction.as_deref(),
        Some("original pending retry")
    );
    assert_eq!(cold.checkpoint_revision, 5);
    assert_eq!(cold.observed_review_required_signatures, delivered);
    let suffix =
        render_review_required_snapshot(&records, &cold.observed_review_required_signatures)
            .unwrap();
    assert_eq!(suffix.observations.len(), 20);
    assert_eq!(suffix.observations[0].task_id, "task_020");
    let mut missing = context.clone();
    missing
        .payload
        .native_semantic_state
        .as_mut()
        .unwrap()
        .as_object_mut()
        .unwrap()
        .remove("attached_task_wait");
    assert!(
        restore(&missing).is_err(),
        "missing durable wait state never defaults to a fresh goal"
    );
    // Codec8 has no producer for legacy display signatures. Matching IDs cannot
    // prove delivery of a mutable candidate's new authoritative version.
    for signature in [
        "task_000:run_000:candidate_000:pending_review:1:2:task_accept,task_revise,task_cancel:"
            .to_owned(),
        "g".repeat(64),
    ] {
        let mut incompatible = context.clone();
        incompatible.payload.native_semantic_state.as_mut().unwrap()["observed_review_required_signatures"] =
            serde_json::json!([signature]);
        assert!(restore(&incompatible).is_err());
    }
    let mut old = context.clone();
    old.payload.native_semantic_state.as_mut().unwrap()["schema_version"] = 7.into();
    assert!(restore(&old).is_err());
    let mut future = context.clone();
    future.payload.native_semantic_state.as_mut().unwrap()["attached_task_wait"]["schema_version"] =
        2.into();
    assert!(restore(&future).is_err());
}
