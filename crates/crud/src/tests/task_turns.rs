use super::*;

#[tokio::test]
async fn detached_task_owned_parent_projects_message_without_turn_work() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_detached_parent_owner";
        let thread_id = "thread_detached_parent_owner";
        let turn_id = "turn_detached_parent_owner";
        let store = postgres_store_with_workspace(fixture, workspace_id).await;
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.6".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::Collaborative,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            work_owner: Default::default(),
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: ThreadMode::Agent,
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        store
            .upsert_thread_model(&thread, PersistedActorRef::System)
            .await
            .expect("collaborative thread should persist");
        store
            .materialize_turn_event(
                crate::events::TurnEventPayload::TurnStarted(
                    crate::events::TurnStartedEventPayload {
                        thread: thread.clone(),
                        sandbox_mode: SandboxMode::FullAccess,
                        turn,
                        input: vec![UserInput::Text {
                            text: "run this in a detached task".to_owned(),
                            text_elements: Vec::new(),
                        }],
                        actor: Some(PersistedActorRef::System),
                        reasoning_effort: None,
                        work_owner: TurnWorkOwner::DetachedTask,
                    },
                ),
                timestamp,
            )
            .await
            .expect("detached parent start should persist atomically");

        let persisted_turn = pioneer_entity::turn::Entity::find_by_id(turn_id)
            .one(&store.connection)
            .await
            .expect("detached parent should load")
            .expect("detached parent should exist");
        assert_eq!(persisted_turn.work_owner, "detached_task");
        let (_, snapshot_turn) = store.get_turn(thread_id, turn_id).await.unwrap().unwrap();
        assert_eq!(snapshot_turn.work_owner, TurnWorkOwner::DetachedTask);
        let snapshot = store.get_thread_model(thread_id).await.unwrap().unwrap();
        assert_eq!(
            snapshot
                .turns
                .iter()
                .find(|turn| turn.id == turn_id)
                .unwrap()
                .work_owner,
            TurnWorkOwner::DetachedTask
        );
        // Old terminal payloads did not carry work_owner. Read projections
        // must hydrate the saved fact without rewriting the canonical event.
        let mut historical_terminal = snapshot_turn.clone();
        historical_terminal.status = TurnStatus::Completed;
        historical_terminal.work_owner = TurnWorkOwner::Turn;
        store
            .materialize_turn_completed(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: historical_terminal,
                },
                timestamp + 1,
            )
            .await
            .unwrap();
        let history = store
            .get_thread_history(thread_id, None)
            .await
            .unwrap()
            .unwrap();
        for event in history.events {
            match event.payload {
                pioneer_protocol::ThreadHistoryEventPayload::TurnStarted { turn, .. }
                | pioneer_protocol::ThreadHistoryEventPayload::TurnCompleted { turn, .. } => {
                    assert_eq!(turn.work_owner, TurnWorkOwner::DetachedTask)
                }
                _ => {}
            }
        }
        assert!(
            store
                .get_turn_work_projection(turn_id)
                .await
                .expect("detached parent work projection should query")
                .is_none(),
            "the parent Turn must never own an observable work projection"
        );
        let blocks = store
            .list_thread_timeline_projection_page(thread_id, None, ProjectionPageAnchor::Start, 10)
            .await
            .expect("detached parent timeline should load");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].block_kind, BLOCK_KIND_USER_MESSAGE);
        assert_eq!(blocks[0].turn_id.as_deref(), Some(turn_id));
        Ok(())
    })
    .await
    .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn task_run_occurrence_terminalization_repairs_legacy_canonical_binding_once() {
    let (store, _thread, run) = terminal_task_run_occurrence_fixture(Some(TurnKind::TaskRun)).await;

    let ticket =
        pioneer_entity::task_run_occurrence_reconcile_pending::Entity::find_by_id(run.id.clone())
            .one(&store.connection)
            .await
            .unwrap()
            .expect("terminal mismatch must publish a ticket");
    assert!(ticket.generation > 0);
    assert_eq!(
        store
            .discover_task_run_occurrence_reconcile(i64::MAX, 10)
            .await
            .expect("due tracked occurrences should list"),
        vec![TaskRunOccurrenceReconcileCandidate {
            run_id: run.id.clone(),
            generation: ticket.generation
        }],
    );
    assert_eq!(
        store
            .compare_and_materialize_task_run_occurrence_terminal(run.id.as_str(), 1_700_000_003,)
            .await
            .expect("legacy canonical occurrence should reconcile"),
        TaskRunOccurrenceTerminalizationOutcome::Changed,
    );
    assert_eq!(
        store
            .compare_and_materialize_task_run_occurrence_terminal(run.id.as_str(), 1_700_000_004,)
            .await
            .expect("repeated reconciliation should be idempotent"),
        TaskRunOccurrenceTerminalizationOutcome::AlreadyConsistent,
    );
    assert!(
        store
            .discover_task_run_occurrence_reconcile(i64::MAX, 10)
            .await
            .expect("due tracked occurrences should list")
            .is_empty(),
        "a repaired occurrence must leave the durable pending set",
    );
    let occurrence = store
        .get_turn("thr_task", run.id.as_str())
        .await
        .expect("occurrence should load")
        .expect("occurrence should exist")
        .1;
    assert_eq!(occurrence.status, TurnStatus::Completed);
    assert_eq!(occurrence.turn_kind, TurnKind::TaskRun);
}

#[tokio::test]
async fn blocked_task_missing_admission_is_readmitted_with_resume_atomically() {
    pioneer_postgres_test::with_database(async |fixture| {
        let connection = fixture.connection().clone();

        let store = CrudStore::new(connection);
        let timestamp = 1_700_000_000;
        let task = sample_task(timestamp);
        let trigger = sample_task_trigger(timestamp);
        store
            .append_task_events(
                vec![
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    TaskEventPayload::TriggerCreated {
                        trigger: trigger.clone(),
                    },
                    TaskEventPayload::TaskBlocked {
                        task_id: task.id.clone(),
                        error: None,
                        blocked_at: timestamp + 1,
                    },
                ],
                timestamp,
            )
            .await
            .expect("legacy task should block without an admission row");

        let mut resumed_task = task.clone();
        resumed_task.status = TaskStatus::Scheduled;
        resumed_task.updated_at = timestamp + 2;
        resumed_task.revision += 1;
        let mut resumed_trigger = trigger;
        resumed_trigger.status = TaskTriggerStatus::Active;
        resumed_trigger.next_fire_at = Some(timestamp + 3600);
        resumed_trigger.updated_at = timestamp + 2;
        let ceilings = crate::ExecutionQuotaCeilings {
            per_principal: 10,
            per_role: 10,
            per_workspace: 10,
            gateway: 10,
        };
        store
            .append_task_events_with_execution_readmission(
                vec![TaskEventPayload::TaskResumed {
                    task: resumed_task,
                    triggers: vec![resumed_trigger],
                    reason: Some("permissions confirmed".to_owned()),
                    resumed_at: timestamp + 2,
                }],
                timestamp + 2,
                NewTaskExecutionAdmission {
                    task_id: task.id.clone(),
                    workspace_id: task.workspace_id.clone(),
                    root_thread_id: "thr_task".to_owned(),
                    initiating_principal_id: "principal_task".to_owned(),
                    authorization_context_json: "{\"test\":true}".to_owned(),
                    created_at: unix_to_datetime(timestamp + 2),
                    execution_lease: Some(NewExecutionAdmissionLease {
                        id: format!("quota:task:{}", task.id),
                        subject_kind: "task".to_owned(),
                        subject_id: task.id.clone(),
                        operation_class: crate::ExecutionAdmissionClass::ScheduledTask,
                        principal_id: "principal_task".to_owned(),
                        role_key: "test_role".to_owned(),
                        workspace_id: task.workspace_id.clone(),
                        policy_fingerprint: "test_policy".to_owned(),
                        policy: crate::ExecutionAdmissionQuotaPolicy {
                            active: ceilings,
                            queued: ceilings,
                            scheduled: ceilings,
                        },
                    }),
                },
            )
            .await
            .expect("resume and missing admission insert should commit together");

        let resumed = store
            .get_task(task.id.as_str())
            .await
            .expect("resumed task should load")
            .expect("resumed task should exist");
        assert_eq!(resumed.task.status, TaskStatus::Scheduled);
        assert_eq!(resumed.triggers[0].status, TaskTriggerStatus::Active);
        assert!(
            store
                .get_task_execution_admission(task.id.as_str())
                .await
                .expect("admission should query")
                .is_some()
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_run_occurrence_turn_kind_and_origin_roundtrip() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = postgres_store_with_workspace(fixture, "ws_000000000000000001").await;
        let timestamp = 1_700_000_000;
        let thread = Thread {
            context_window: None,
            service_mode: None,
            context_authority: None,
            workspace_id: "ws_000000000000000001".to_owned(),
            id: "thr_000000000000000001".to_owned(),
            name: None,
            preview: "human conversation".to_owned(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.4".to_owned(),
            model_provider: "openai".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Idle,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        store
            .upsert_thread_model(&thread, pioneer_protocol::PersistedActorRef::System)
            .await
            .expect("parent thread should exist before task occurrence");
        let mut stale_task_thread = thread.clone();
        stale_task_thread.preview = "stale task payload".to_owned();
        stale_task_thread.status = ThreadStatus::Active;
        let turn = Turn {
            work_owner: Default::default(),
            id: "run_0000000000000000001".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::TaskRun,
            origin: TurnOrigin::ScheduledTask,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };

        store
            .materialize_turn_start(
                &stale_task_thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("task run occurrence turn start should persist");

        let (_, fetched_turn) = store
            .get_turn(thread.id.as_str(), turn.id.as_str())
            .await
            .expect("must read turn")
            .expect("turn should exist");
        assert_eq!(fetched_turn.turn_kind, TurnKind::TaskRun);
        assert_eq!(fetched_turn.origin, TurnOrigin::ScheduledTask);

        let threads = store
            .list_threads_for_workspace(thread.workspace_id.as_str(), 10)
            .await
            .expect("must list threads");
        let listed_thread = threads
            .iter()
            .find(|candidate| candidate.id == thread.id)
            .expect("thread should exist");
        assert_eq!(
            listed_thread.status,
            ThreadStatus::Idle,
            "task occurrence must not make the parent foreground active"
        );
        assert_eq!(
            listed_thread.preview, thread.preview,
            "stale task payload must not overwrite concurrent parent metadata"
        );
        let snapshot_turn = listed_thread
            .turns
            .iter()
            .find(|candidate| candidate.id == turn.id)
            .expect("task run occurrence turn should appear in owner thread");
        assert_eq!(snapshot_turn.turn_kind, TurnKind::TaskRun);
        assert_eq!(snapshot_turn.origin, TurnOrigin::ScheduledTask);

        let foreground_turn = Turn {
            work_owner: Default::default(),
            id: "turn_0000000000000000002".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        let mut active_thread = thread.clone();
        active_thread.updated_at = timestamp + 1;
        active_thread.status = ThreadStatus::Active;
        store
            .materialize_turn_start(
                &active_thread,
                SandboxMode::FullAccess,
                &foreground_turn,
                &[],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("foreground conversation should start");

        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn: Turn {
                        status: TurnStatus::Completed,
                        ..turn
                    },
                },
                timestamp + 2,
            )
            .await
            .expect("task occurrence should complete");
        assert_eq!(
            store
                .get_thread_model(thread.id.as_str())
                .await
                .expect("parent should load")
                .expect("parent should exist")
                .status,
            ThreadStatus::Active,
            "task completion must not idle a concurrent conversation"
        );

        let mut stale_idle_parent = store
            .get_thread_model(thread.id.as_str())
            .await
            .expect("parent should load")
            .expect("parent should exist");
        stale_idle_parent.status = ThreadStatus::Idle;
        store
            .upsert_thread_model(
                &stale_idle_parent,
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("stale idle parent should persist");
        assert_eq!(
            store
                .reconcile_thread_foreground_statuses(timestamp + 2)
                .await
                .expect("foreground status repair should succeed"),
            1
        );
        assert_eq!(
            store
                .get_thread_model(thread.id.as_str())
                .await
                .expect("parent should load")
                .expect("parent should exist")
                .status,
            ThreadStatus::Active
        );

        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn: Turn {
                        status: TurnStatus::Completed,
                        ..foreground_turn
                    },
                },
                timestamp + 3,
            )
            .await
            .expect("foreground conversation should complete");
        assert_eq!(
            store
                .get_thread_model(thread.id.as_str())
                .await
                .expect("parent should load")
                .expect("parent should exist")
                .status,
            ThreadStatus::Idle
        );

        let mut stale_active_parent = store
            .get_thread_model(thread.id.as_str())
            .await
            .expect("parent should load")
            .expect("parent should exist");
        stale_active_parent.status = ThreadStatus::Active;
        store
            .upsert_thread_model(
                &stale_active_parent,
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("legacy task-owned active status should persist");
        assert_eq!(
            store
                .reconcile_thread_foreground_statuses(timestamp + 4)
                .await
                .expect("legacy foreground status repair should succeed"),
            1
        );
        assert_eq!(
            store
                .get_thread_model(thread.id.as_str())
                .await
                .expect("parent should load")
                .expect("parent should exist")
                .status,
            ThreadStatus::Idle
        );
        Ok(())
    })
    .await
    .unwrap();
}
