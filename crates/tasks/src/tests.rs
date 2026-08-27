use crate::{
    CreateTaskResultReviewerContextParams, RecordTaskResultReviewEventParams,
    RecordUserTaskResultReviewEventParams, TaskCreateContext, TaskExecutionContext,
    TaskExecutionHandle, TaskExecutor, TaskExecutorRecoveryOutcome, TaskExecutorStartOutcome,
    TaskMutationContext, TaskResultReviewActor, TaskReviewRuntimeConfig, TaskRuntime,
    TaskRuntimeConfig, TaskRuntimeResult, TaskWaitContext, WriteLockDecision,
};
use async_trait::async_trait;
use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    ArtifactBindingTargetRecord, CrudStore, IngestArtifactMetadataRecord, NewArtifactBlobRecord,
    TaskEventAppendStatus,
};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
    ArtifactRole, TaskAcceptParams, TaskAgentInput, TaskAgentInputVariable, TaskAgentPrompt,
    TaskAgentReviewMode, TaskAgentReviewPolicy, TaskAgentSecurityCap, TaskAgentSpecInput,
    TaskAgentToolPolicy, TaskAgentWriteMode, TaskArtifact, TaskAttachmentMode, TaskCancelParams,
    TaskConcurrencyConflictPolicy, TaskConcurrencyPolicy, TaskCreateParams, TaskDeliveriesParams,
    TaskDeliveryMode, TaskDeliveryPolicy, TaskDeliveryStatus, TaskDetachParams, TaskError,
    TaskErrorClass, TaskEventPayload, TaskEventsParams, TaskExecutorKind, TaskLifecyclePolicy,
    TaskOccurrenceContract, TaskOccurrenceStatus, TaskOwnerKind, TaskParentTerminalAction,
    TaskPauseParams, TaskRescheduleParams, TaskRescheduleReason, TaskResourceBudget, TaskResult,
    TaskResultCandidate, TaskResultCandidateStatus, TaskResultReviewDecision,
    TaskResultReviewEventKind, TaskResultReviewResolutionStrategy, TaskResultReviewerKind,
    TaskResultReviewerSpec, TaskResumeParams, TaskRetryBackoffKind, TaskRetryPolicy,
    TaskReviseParams, TaskRun, TaskRunExecutionStatus, TaskRunStatus, TaskRunThreadBinding,
    TaskRunThreadBindingKind, TaskRunTurn, TaskRunTurnKind, TaskRunTurnStatus, TaskStatus,
    TaskThreadLineage, TaskTriggerCatchUpPolicy, TaskTriggerInput, TaskTriggerSpec,
    TaskTriggerStatus, TaskUpdateParams, TaskValue, TaskWaitMode, TaskWaitParams,
    TaskWaitReviewAction, TaskWaitRevisionBlockedReason, ThreadLineage, TurnFilesystemAccess,
    TurnFilesystemSandboxEntry, TurnNetworkPolicySnapshot, TurnPermissionMode,
    TurnProcessPolicySnapshot, TurnSandboxMode,
};
use sea_orm::{ActiveValue::Set, Database, EntityTrait};
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

const TEST_WORKSPACE_ID: &str = "ws_tasks";
const TEST_PRINCIPAL_ID: &str = "P00000000000000000001";
const TEST_PARENT_EXECUTION_ID: &str = "E12345678901234567890";
const TEST_PARENT_THREAD_ID: &str = "H12345678901234567890";
const TEST_PARENT_TURN_ID: &str = "T12345678901234567890";
const TEST_REVIEWER_EXECUTION_ID: &str = "E22345678901234567890";
const TEST_REVIEWER_THREAD_ID: &str = "H22345678901234567890";
const TEST_PRESENTATION_SNAPSHOT_ID: &str = "S12345678901234567890";

#[derive(Default)]
struct CompletingSystemExecutor;

#[async_trait]
impl TaskExecutor for CompletingSystemExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::System
    }

    async fn start_run(
        &self,
        _context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
        handle.mark_started(run.created_at).await?;
        handle
            .complete_run(
                Some(TaskResult {
                    summary: Some(format!("completed run {}", run.run_number)),
                    data: Some(TaskValue::Integer(run.run_number)),
                    artifacts: Vec::new(),
                    completed_by_run_id: Some(run.id.clone()),
                }),
                run.created_at,
            )
            .await?;
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        _run_id: &str,
        _reason: &str,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()> {
        Ok(())
    }

    async fn recover_run(
        &self,
        _context: TaskExecutionContext,
        _run: TaskRun,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
        Ok(TaskExecutorRecoveryOutcome::LeftUnchanged)
    }
}

#[derive(Default)]
struct FailingSystemExecutor;

#[async_trait]
impl TaskExecutor for FailingSystemExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::System
    }

    async fn start_run(
        &self,
        _context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
        handle.mark_started(run.created_at).await?;
        handle
            .fail_run(
                Some(TaskError {
                    code: "test_failure".to_owned(),
                    message: "test failure".to_owned(),
                    class: TaskErrorClass::Internal,
                    details: None,
                    failed_run_id: Some(run.id.clone()),
                }),
                run.created_at,
            )
            .await?;
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        _run_id: &str,
        _reason: &str,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()> {
        Ok(())
    }

    async fn recover_run(
        &self,
        _context: TaskExecutionContext,
        _run: TaskRun,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
        Ok(TaskExecutorRecoveryOutcome::LeftUnchanged)
    }
}

#[derive(Clone)]
struct SlowAgentExecutor {
    starts: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

#[async_trait]
impl TaskExecutor for SlowAgentExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::Agent
    }

    async fn start_run(
        &self,
        _context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        handle.mark_started(run.created_at).await?;
        handle
            .complete_run(
                Some(TaskResult {
                    summary: Some("agent complete".to_owned()),
                    data: None,
                    artifacts: Vec::new(),
                    completed_by_run_id: Some(run.id.clone()),
                }),
                run.created_at,
            )
            .await?;
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        _run_id: &str,
        _reason: &str,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct LineageRecordingAgentExecutor {
    starts: Arc<AtomicUsize>,
    recoveries: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

#[async_trait]
impl TaskExecutor for LineageRecordingAgentExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::Agent
    }

    async fn start_run(
        &self,
        _context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let execution = handle
            .load_execution()
            .await?
            .expect("agent execution should be reserved before start");
        handle.mark_started(run.created_at).await?;
        let child_thread_id = pioneer_protocol::generate_id(21);
        let child_turn_id = pioneer_protocol::generate_id(21);
        let lineage = TaskThreadLineage {
            child_thread_id: child_thread_id.clone(),
            parent_thread_id: "parent_thread".to_owned(),
            root_thread_id: "parent_thread".to_owned(),
            depth: 1,
            origin_kind: Some("task_run".to_owned()),
            created_by_thread_id: Some("parent_thread".to_owned()),
            created_by_turn_id: Some("parent_turn".to_owned()),
            created_at: run.created_at,
        };
        let binding = TaskRunThreadBinding {
            id: format!("test_binding_{}", run.id),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: Some(execution.id.clone()),
            thread_id: child_thread_id.clone(),
            binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
            created_at: run.created_at,
        };
        let task_run_turn = TaskRunTurn {
            id: format!("test_turn_{child_turn_id}"),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: Some(execution.id),
            thread_id: child_thread_id,
            turn_id: child_turn_id,
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::InProgress,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: run.created_at,
            started_at: Some(run.created_at),
            completed_at: None,
        };
        handle
            .link_child_thread_with_runtime(lineage, binding, task_run_turn, run.created_at)
            .await?;
        self.release.notified().await;
        handle
            .complete_run(
                Some(TaskResult {
                    summary: Some("lineage executor complete".to_owned()),
                    data: None,
                    artifacts: Vec::new(),
                    completed_by_run_id: Some(run.id.clone()),
                }),
                run.created_at.saturating_add(1),
            )
            .await?;
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        _run_id: &str,
        _reason: &str,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()> {
        Ok(())
    }

    async fn recover_run(
        &self,
        _context: TaskExecutionContext,
        _run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
        self.recoveries.fetch_add(1, Ordering::SeqCst);
        assert!(handle.load_execution().await?.is_some());
        Ok(TaskExecutorRecoveryOutcome::AlreadyRunning)
    }
}

#[derive(Default)]
struct CancellationFailingSystemExecutor;

#[async_trait]
impl TaskExecutor for CancellationFailingSystemExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::System
    }

    async fn start_run(
        &self,
        _context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
        handle.mark_started(run.created_at).await?;
        handle
            .fail_run(
                Some(TaskError {
                    code: "child_turn_cancelled".to_owned(),
                    message: "task cancelled".to_owned(),
                    class: TaskErrorClass::Cancelled,
                    details: None,
                    failed_run_id: Some(run.id.clone()),
                }),
                run.created_at,
            )
            .await?;
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        _run_id: &str,
        _reason: &str,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()> {
        Ok(())
    }
}

async fn runtime() -> TaskRuntime {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite memory database should connect");
    Migrator::up(&connection, None)
        .await
        .expect("migration should apply");
    seed_task_test_workspace(&connection).await;
    let store = Arc::new(CrudStore::new(connection));
    TaskRuntime::new(store)
}

async fn runtime_with_review_config() -> TaskRuntime {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite memory database should connect");
    Migrator::up(&connection, None)
        .await
        .expect("migration should apply");
    seed_task_test_workspace(&connection).await;
    let store = Arc::new(CrudStore::new(connection));
    TaskRuntime::new_with_config(
        store,
        TaskRuntimeConfig {
            review: TaskReviewRuntimeConfig {
                enabled: true,
                allow_task_create_review_policy: true,
                default_parent_review_for_immediate_attached_agent_tasks: false,
                default_max_revision_rounds: 2,
                auto_accept_after_seconds: 300,
            },
        },
    )
}

async fn seed_task_test_workspace(connection: &sea_orm::DatabaseConnection) {
    let now = pioneer_crud::utc_now();
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set(TEST_WORKSPACE_ID.to_owned()),
        name: Set("Tasks Test Workspace".to_owned()),
        is_active: Set(true),
        is_current: Set(true),
        created_at: Set(now.clone()),
        updated_at: Set(now),
    })
    .exec(connection)
    .await
    .expect("Tasks test workspace should seed");
    pioneer_crud::ensure_pioneer_for_workspace(connection, TEST_WORKSPACE_ID, now)
        .await
        .expect("Tasks test workspace should seed its reserved Pioneer identity");
}

#[tokio::test]
async fn task_event_idempotency_rejects_conflicting_duplicate_key_for_task() {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite memory database should connect");
    Migrator::up(&connection, None)
        .await
        .expect("migration should apply");
    let store = CrudStore::new(connection);
    let run = idempotency_test_run(TaskRunStatus::Queued, 1);

    let inserted = store
        .append_task_event(
            TaskEventPayload::RunCreated {
                run: run.clone(),
                agent_spec: None,
            },
            1_700_000_000,
        )
        .await
        .expect("first keyed event should insert");
    assert_eq!(inserted.append_status, TaskEventAppendStatus::Inserted);

    let replay = store
        .append_task_event(
            TaskEventPayload::RunCreated {
                run: run.clone(),
                agent_spec: None,
            },
            1_700_000_001,
        )
        .await
        .expect("identical keyed event should be idempotent");
    assert_eq!(replay.append_status, TaskEventAppendStatus::AlreadyExists);

    let duplicate = store
        .append_task_event(
            TaskEventPayload::RunCreated {
                run: idempotency_test_run(TaskRunStatus::Queued, 2),
                agent_spec: None,
            },
            1_700_000_002,
        )
        .await;

    assert!(
        duplicate.is_err(),
        "task event idempotency must reject a different payload for the same key"
    );
}

fn idempotency_test_run(status: TaskRunStatus, run_number: i64) -> TaskRun {
    TaskRun {
        id: "run_00000000000001".to_owned(),
        task_id: "task_0000000000001".to_owned(),
        trigger_id: None,
        parent_run_id: None,
        run_group_id: "run_group_00000001".to_owned(),
        attempt_number: 1,
        retry_of_run_id: None,
        ready_at: Some(1_700_000_000),
        run_number,
        status,
        executor_kind: TaskExecutorKind::System,
        started_at: None,
        completed_at: None,
        heartbeat_at: None,
        locked_by: None,
        lock_expires_at: None,
        result: None,
        error: None,
        created_at: 1_700_000_000,
        updated_at: 1_700_000_000,
    }
}

fn create_params(spec: TaskTriggerSpec) -> TaskCreateParams {
    TaskCreateParams {
        workspace_id: TEST_WORKSPACE_ID.to_owned(),
        owner_kind: TaskOwnerKind::Workspace,
        owner_id: Some(TEST_WORKSPACE_ID.to_owned()),
        created_by_thread_id: None,
        created_by_turn_id: None,
        parent_task_id: None,
        executor_kind: TaskExecutorKind::System,
        title: "Task".to_owned(),
        goal: "Do the task".to_owned(),
        priority: 0,
        trigger: TaskTriggerInput { spec },
        launch: None,
        agent_spec: None,
        lifecycle_policy: None,
        delivery_policy: None,
        retry_policy: None,
        timeout_policy: None,
        concurrency_policy: None,
        metadata: None,
    }
}

fn test_agent_launch_facts() -> (
    pioneer_protocol::AgentLaunchSelection,
    pioneer_protocol::AgentIdentityProjection,
    pioneer_protocol::AgentExecutionProfileProjection,
    crate::TaskAgentAuthorizationGrantSeed,
) {
    let identity = pioneer_protocol::AgentIdentityProjection::new(
        pioneer_protocol::AgentIdentityId::new("A12345678901234567890".to_owned())
            .expect("valid test Agent identity id"),
        pioneer_protocol::AgentIdentitySourceKind::NativeAgent,
        "Tasks Test Agent",
        "tasks-test-agent",
        None,
        None,
        1,
        "tasks-test-agent-v1",
    )
    .expect("valid test Agent identity");
    let profile = pioneer_protocol::AgentExecutionProfileProjection {
        id: pioneer_protocol::AgentExecutionProfileId::new("P12345678901234567890".to_owned())
            .expect("valid test Agent execution profile id"),
        compatible_agent_identity_ids: vec![identity.id.clone()],
        backend: pioneer_protocol::AgentExecutionProfileBackend::ApiProvider,
        provider_id: "openai".to_owned(),
        model_id: "test-model".to_owned(),
        provider_display_name: "OpenAI".to_owned(),
        model_display_name: "Test Model".to_owned(),
        allowed_reasoning: Vec::new(),
        allowed_permission_profiles: vec![TurnPermissionMode::AutoAcceptEdits],
        catalog_generation: 1,
        policy_generation: 1,
        fingerprint: "tasks-test-profile-v1".to_owned(),
    };
    let launch = pioneer_protocol::AgentLaunchSelection {
        agent: pioneer_protocol::AgentIdentitySelection::Exact {
            agent_identity_id: identity.id.clone(),
        },
        execution: pioneer_protocol::AgentExecutionSelection {
            profile: pioneer_protocol::AgentExecutionProfileSelection::Exact {
                profile_id: profile.id.clone(),
            },
            reasoning: None,
            permission_profile: None,
            skill_ids: Vec::new(),
            mcp_server_ids: Vec::new(),
        },
    };
    let child_launch_grant = pioneer_protocol::ChildAgentLaunchGrantSet::new(
        vec![identity.clone()],
        vec![profile.clone()],
    )
    .expect("valid test child launch grant");
    let authorization = crate::TaskAgentAuthorizationGrantSeed {
        role_key: "tasks_test_role".to_owned(),
        policy_generation: 1,
        allowed_actions: vec!["task.execute".to_owned()],
        fingerprint: "a".repeat(64),
        child_launch_grant,
    };
    (launch, identity, profile, authorization)
}

fn configure_agent_task(params: &mut TaskCreateParams) {
    params.executor_kind = TaskExecutorKind::Agent;
    params.launch = Some(test_agent_launch_facts().0);
}

fn test_agent_presentation_snapshot(
    execution_id: &str,
) -> pioneer_protocol::AgentPresentationSnapshot {
    let (_, identity, _, _) = test_agent_launch_facts();
    pioneer_protocol::AgentPresentationSnapshot {
        agent_identity_id: identity.id,
        agent_execution_id: pioneer_protocol::AgentExecutionId::new(execution_id.to_owned())
            .expect("valid test Agent execution id"),
        identity_source_kind: identity.source_kind,
        identity_source_revision: identity.source_revision,
        display_name: identity.display_name,
        nickname: identity.nickname,
        avatar_revision: identity.avatar_revision,
        role_label: identity.role_label,
    }
}

async fn persist_test_agent_turn(
    runtime: &TaskRuntime,
    execution_id: &str,
    thread_id: &str,
    turn_id: &str,
    parent_execution_id: Option<&str>,
) {
    let store = runtime.service().store();
    let database = store.database_connection();
    let now = pioneer_crud::utc_now();
    let (launch, identity, profile, _) = test_agent_launch_facts();
    let snapshot = test_agent_presentation_snapshot(execution_id);

    pioneer_crud::ensure_agent_identity(
        &database,
        &pioneer_crud::AgentIdentityInput {
            id: identity.id.as_str().to_owned(),
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            source_kind: pioneer_crud::SOURCE_NATIVE_AGENT.to_owned(),
            source_id: "tasks-test-agent".to_owned(),
            source_revision: i64::try_from(identity.source_revision)
                .expect("test identity revision should fit i64"),
            source_fingerprint: identity.source_fingerprint.clone(),
            now: now.clone(),
        },
    )
    .await
    .expect("test Agent identity should persist");
    pioneer_crud::insert_presentation_snapshot(
        &database,
        &pioneer_crud::PresentationSnapshotInput {
            id: TEST_PRESENTATION_SNAPSHOT_ID.to_owned(),
            agent_identity_id: identity.id.as_str().to_owned(),
            source_revision: i64::try_from(identity.source_revision)
                .expect("test identity revision should fit i64"),
            source_fingerprint: identity.source_fingerprint.clone(),
            display_name: snapshot.display_name.clone(),
            nickname: snapshot.nickname.clone(),
            avatar_revision: snapshot.avatar_revision.clone(),
            role_label: snapshot.role_label.clone(),
            now: now.clone(),
        },
    )
    .await
    .expect("test Agent presentation should persist");
    pioneer_crud::insert_agent_execution(
        &database,
        &pioneer_crud::AgentExecutionInput {
            id: execution_id.to_owned(),
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            agent_identity_id: identity.id.as_str().to_owned(),
            identity_source_revision: i64::try_from(identity.source_revision)
                .expect("test identity revision should fit i64"),
            identity_source_fingerprint: identity.source_fingerprint,
            parent_execution_id: parent_execution_id.map(str::to_owned),
            parent_task_id: None,
            parent_thread_id: parent_execution_id.map(|_| TEST_PARENT_THREAD_ID.to_owned()),
            home_root_thread_id: TEST_PARENT_THREAD_ID.to_owned(),
            work_graph_root_execution_id: parent_execution_id.unwrap_or(execution_id).to_owned(),
            requested_identity_selection_json: serde_json::to_string(&launch.agent)
                .expect("test identity selection should serialize"),
            requested_profile_selection_json: serde_json::to_string(&launch.execution.profile)
                .expect("test profile selection should serialize"),
            resolved_profile_id: Some(profile.id.as_str().to_owned()),
            resolved_profile_fingerprint: Some(profile.fingerprint),
            presentation_snapshot_id: Some(TEST_PRESENTATION_SNAPSHOT_ID.to_owned()),
            authorization_context_fingerprint: "b".repeat(64),
            execution_generation: 1,
            status: "running".to_owned(),
            now: now.clone(),
        },
    )
    .await
    .expect("test Agent execution should persist");

    let actor =
        pioneer_protocol::PersistedActorRef::AgentExecution(snapshot.agent_execution_id.clone());
    let thread = pioneer_protocol::Thread {
        workspace_id: TEST_WORKSPACE_ID.to_owned(),
        id: thread_id.to_owned(),
        name: None,
        preview: String::new(),
        preview_author: None,
        mode: pioneer_protocol::ThreadMode::Agent,
        model: profile.model_id,
        model_provider: profile.provider_id,
        reasoning_effort: None,
        created_at: 1_700_000_000,
        updated_at: 1_700_000_000,
        status: pioneer_protocol::ThreadStatus::Active,
        origin_kind: pioneer_protocol::ThreadOriginKind::User,
        sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: Vec::new(),
    };
    let turn = pioneer_protocol::Turn {
        id: turn_id.to_owned(),
        status: pioneer_protocol::TurnStatus::InProgress,
        turn_kind: pioneer_protocol::TurnKind::Conversation,
        origin: pioneer_protocol::TurnOrigin::User,
        mode: pioneer_protocol::ThreadMode::Agent,
        author: Some(snapshot.to_turn_author_snapshot()),
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
    };
    store
        .upsert_thread_model(&thread, actor.clone())
        .await
        .expect("test Agent thread should persist");
    store
        .materialize_turn_start(
            &thread,
            pioneer_protocol::SandboxMode::FullAccess,
            &turn,
            &[],
            actor,
        )
        .await
        .expect("test Agent turn should persist");
    pioneer_crud::insert_agent_turn_response(
        &database,
        &pioneer_crud::AgentTurnResponseInput {
            turn_id: turn_id.to_owned(),
            execution_id: execution_id.to_owned(),
            presentation_snapshot_id: TEST_PRESENTATION_SNAPSHOT_ID.to_owned(),
            now,
        },
    )
    .await
    .expect("test Agent turn response should persist");
}

async fn persist_test_child_agent_execution(runtime: &TaskRuntime, execution_id: &str) {
    let store = runtime.service().store();
    let database = store.database_connection();
    let now = pioneer_crud::utc_now();
    let (launch, identity, profile, _) = test_agent_launch_facts();
    let snapshot = test_agent_presentation_snapshot(execution_id);

    pioneer_crud::ensure_agent_identity(
        &database,
        &pioneer_crud::AgentIdentityInput {
            id: identity.id.as_str().to_owned(),
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            source_kind: pioneer_crud::SOURCE_NATIVE_AGENT.to_owned(),
            source_id: "tasks-test-agent".to_owned(),
            source_revision: i64::try_from(identity.source_revision)
                .expect("test identity revision should fit i64"),
            source_fingerprint: identity.source_fingerprint.clone(),
            now: now.clone(),
        },
    )
    .await
    .expect("test child Agent identity should persist");
    pioneer_crud::insert_presentation_snapshot(
        &database,
        &pioneer_crud::PresentationSnapshotInput {
            id: TEST_PRESENTATION_SNAPSHOT_ID.to_owned(),
            agent_identity_id: identity.id.as_str().to_owned(),
            source_revision: i64::try_from(identity.source_revision)
                .expect("test identity revision should fit i64"),
            source_fingerprint: identity.source_fingerprint.clone(),
            display_name: snapshot.display_name,
            nickname: snapshot.nickname,
            avatar_revision: snapshot.avatar_revision,
            role_label: snapshot.role_label,
            now: now.clone(),
        },
    )
    .await
    .expect("test child Agent presentation should persist");
    pioneer_crud::insert_agent_execution(
        &database,
        &pioneer_crud::AgentExecutionInput {
            id: execution_id.to_owned(),
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            agent_identity_id: identity.id.as_str().to_owned(),
            identity_source_revision: i64::try_from(identity.source_revision)
                .expect("test identity revision should fit i64"),
            identity_source_fingerprint: identity.source_fingerprint,
            parent_execution_id: Some(TEST_PARENT_EXECUTION_ID.to_owned()),
            parent_task_id: None,
            parent_thread_id: Some(TEST_PARENT_THREAD_ID.to_owned()),
            home_root_thread_id: TEST_PARENT_THREAD_ID.to_owned(),
            work_graph_root_execution_id: TEST_PARENT_EXECUTION_ID.to_owned(),
            requested_identity_selection_json: serde_json::to_string(&launch.agent)
                .expect("test identity selection should serialize"),
            requested_profile_selection_json: serde_json::to_string(&launch.execution.profile)
                .expect("test profile selection should serialize"),
            resolved_profile_id: Some(profile.id.as_str().to_owned()),
            resolved_profile_fingerprint: Some(profile.fingerprint),
            presentation_snapshot_id: Some(TEST_PRESENTATION_SNAPSHOT_ID.to_owned()),
            authorization_context_fingerprint: "b".repeat(64),
            execution_generation: 1,
            status: "running".to_owned(),
            now,
        },
    )
    .await
    .expect("test child Agent execution should persist");
}

/// Produces the explicit positive authority seed required by Agent Tasks in
/// service-level tests. The Tasks crate deliberately treats the serialized
/// authorization context as opaque; Gateway integration tests exercise the
/// real context parser and revalidator. Keeping this helper keyed by the
/// requested executor prevents tests from normalizing a missing Agent
/// authority while leaving System Tasks free of an execution envelope.
fn task_create_context_for(params: &TaskCreateParams) -> TaskCreateContext {
    if params.executor_kind != TaskExecutorKind::Agent {
        return TaskCreateContext::default();
    }

    let generous = pioneer_crud::ExecutionQuotaCeilings {
        per_principal: 1_024,
        per_role: 1_024,
        per_workspace: 1_024,
        gateway: 1_024,
    };
    let (launch, identity, profile, authorization) = test_agent_launch_facts();
    assert_eq!(params.launch.as_ref(), Some(&launch));
    TaskCreateContext {
        actor_id: Some(TEST_PRINCIPAL_ID.to_owned()),
        creator_presentation_snapshot: None,
        execution_destination_thread_id: None,
        execution_route_id: None,
        execution_route_receipt_json: None,
        execution_route_expires_at_millis: None,
        delivery_route_id: None,
        delivery_route_receipt_json: None,
        delivery_route_expires_at_millis: None,
        creator_work_graph_root_execution_id: None,
        work_graph_root_execution_id: None,
        launch_selection: Some(launch),
        resolved_launch_identity: Some(identity),
        resolved_launch_profile: Some(profile),
        agent_authorization_grant: Some(authorization),
        conversation_snapshot: None,
        execution_admission: Some(crate::TaskExecutionAdmissionSeed {
            workspace_id: params.workspace_id.clone(),
            root_thread_id: params
                .created_by_thread_id
                .clone()
                .or_else(|| params.owner_id.clone())
                .unwrap_or_else(|| "thr_tasks_test_root".to_owned()),
            initiating_principal_id: TEST_PRINCIPAL_ID.to_owned(),
            authorization_context_json: r#"{"test_authority":"tasks_unit"}"#.to_owned(),
            role_key: "tasks_test_role".to_owned(),
            policy_fingerprint: "0".repeat(64),
            execution_resources: pioneer_crud::ExecutionAdmissionQuotaPolicy {
                active: generous,
                queued: generous,
                scheduled: generous,
            },
            task_resources: TaskResourceBudget::default(),
        }),
        agent_action_commit: None,
    }
}

fn task_create_context_for_parent_agent(params: &TaskCreateParams) -> TaskCreateContext {
    let mut context = task_create_context_for(params);
    let snapshot = test_agent_presentation_snapshot(TEST_PARENT_EXECUTION_ID);
    context.actor_id = Some(TEST_PARENT_EXECUTION_ID.to_owned());
    context.creator_presentation_snapshot = Some(snapshot);
    context.creator_work_graph_root_execution_id = Some(TEST_PARENT_EXECUTION_ID.to_owned());
    context.work_graph_root_execution_id = Some(TEST_PARENT_EXECUTION_ID.to_owned());
    context
}

async fn record_active_task_execution_turn(
    runtime: &TaskRuntime,
    task_id: &str,
    run_id: &str,
) -> (String, String) {
    let thread_id = pioneer_protocol::generate_id(21);
    let turn_id = pioneer_protocol::generate_id(21);
    let appended = runtime
        .service()
        .append_event(
            TaskEventPayload::TaskRunTurnStarted {
                task_run_turn: TaskRunTurn {
                    id: pioneer_protocol::generate_id(21),
                    task_id: task_id.to_owned(),
                    run_id: run_id.to_owned(),
                    execution_id: None,
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    kind: TaskRunTurnKind::Initial,
                    round: 0,
                    sequence: 0,
                    status: TaskRunTurnStatus::InProgress,
                    reviews_candidate_id: None,
                    requested_by_candidate_id: None,
                    requested_by_review_event_id: None,
                    created_at: 1_700_000_000,
                    started_at: Some(1_700_000_000),
                    completed_at: None,
                },
            },
            1_700_000_000,
        )
        .await
        .expect("active task execution turn should append");
    runtime.service().publish_and_wake(vec![appended]).await;
    (thread_id, turn_id)
}

fn test_permission_cap() -> pioneer_protocol::TurnPermissionProfileCap {
    pioneer_protocol::task_permission_cap_from_snapshot(
        &pioneer_protocol::default_turn_permission_profile_snapshot(),
    )
}

fn test_security_cap() -> TaskAgentSecurityCap {
    TaskAgentSecurityCap {
        max_permission_profile: pioneer_protocol::task_permission_cap_for_mode(
            TurnPermissionMode::AutoAcceptEdits,
        ),
        max_filesystem_kind: Some(pioneer_protocol::TurnFilesystemSandboxKind::Restricted),
        max_filesystem_entries: vec![TurnFilesystemSandboxEntry::workspace_root(
            TurnFilesystemAccess::Write,
            "/workspace",
        )],
        max_network_policy: TurnNetworkPolicySnapshot::disabled(),
        max_sandbox_mode: TurnSandboxMode::WorkspaceWrite,
        max_process_policy: TurnProcessPolicySnapshot::restricted(),
    }
}

fn agent_spec(max_depth: i64) -> TaskAgentSpecInput {
    TaskAgentSpecInput {
        agent_role: None,
        agent_nickname: None,
        model: Some("test-model".to_owned()),
        model_provider: Some("openai".to_owned()),
        prompt: TaskAgentPrompt {
            goal: "Do agent work".to_owned(),
            instructions: Vec::new(),
            input: None,
            output_instructions: None,
        },
        context_policy: None,
        tool_policy: None,
        permission_cap: Some(test_permission_cap()),
        security_cap: Some(test_security_cap()),
        result_contract: None,
        review_policy: None,
        depth: 0,
        max_depth,
    }
}

async fn create_waiting_review_agent_task(
    runtime: &TaskRuntime,
    max_revision_rounds: u32,
    candidate_round: u32,
    candidate_status: TaskResultCandidateStatus,
) -> (String, String, String) {
    create_waiting_review_agent_task_with_policy(
        runtime,
        TaskAgentReviewPolicy::parent_agent_default(max_revision_rounds),
        candidate_round,
        candidate_status,
    )
    .await
}

async fn create_waiting_review_agent_task_with_policy(
    runtime: &TaskRuntime,
    review_policy: TaskAgentReviewPolicy,
    candidate_round: u32,
    candidate_status: TaskResultCandidateStatus,
) -> (String, String, String) {
    persist_test_agent_turn(
        runtime,
        TEST_PARENT_EXECUTION_ID,
        TEST_PARENT_THREAD_ID,
        TEST_PARENT_TURN_ID,
        None,
    )
    .await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    params.created_by_thread_id = Some(TEST_PARENT_THREAD_ID.to_owned());
    params.created_by_turn_id = Some(TEST_PARENT_TURN_ID.to_owned());
    let mut spec = agent_spec(3);
    spec.review_policy = Some(review_policy);
    params.agent_spec = Some(spec);

    let response = runtime
        .service()
        .create_task(task_create_context_for_parent_agent(&params), params)
        .await
        .expect("review task should create");
    let run = response.run.expect("review task should have run");
    let execution = runtime
        .service()
        .store()
        .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, run.created_at)
        .await
        .expect("execution should reserve");
    persist_test_child_agent_execution(runtime, execution.id.as_str()).await;
    let handle = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    let parent_thread_id = TEST_PARENT_THREAD_ID.to_owned();
    let parent_turn_id = TEST_PARENT_TURN_ID.to_owned();
    let child_thread_id = pioneer_protocol::generate_id(21);
    let child_turn_id = pioneer_protocol::generate_id(21);
    let turn_id = format!("review_turn_{}", run.id);
    let lineage = TaskThreadLineage {
        child_thread_id: child_thread_id.clone(),
        parent_thread_id: parent_thread_id.clone(),
        root_thread_id: parent_thread_id.clone(),
        depth: 1,
        origin_kind: Some("task_run".to_owned()),
        created_by_thread_id: Some(parent_thread_id),
        created_by_turn_id: Some(parent_turn_id),
        created_at: run.created_at,
    };
    let binding = TaskRunThreadBinding {
        id: format!("review_binding_{}", run.id),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id.clone()),
        thread_id: child_thread_id.clone(),
        binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
        created_at: run.created_at,
    };
    let mut task_run_turn = TaskRunTurn {
        id: turn_id,
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id),
        thread_id: child_thread_id.clone(),
        turn_id: child_turn_id.clone(),
        kind: TaskRunTurnKind::Initial,
        round: candidate_round,
        sequence: candidate_round,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at: run.created_at,
        started_at: Some(run.created_at),
        completed_at: None,
    };
    handle
        .link_child_thread_with_runtime(lineage, binding, task_run_turn.clone(), run.created_at)
        .await
        .expect("child runtime should link");

    task_run_turn.status = TaskRunTurnStatus::CandidateCreated;
    task_run_turn.completed_at = Some(run.created_at.saturating_add(1));
    let candidate = TaskResultCandidate {
        id: format!("candidate_{}", run.id),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        task_run_turn_id: task_run_turn.id.clone(),
        thread_id: child_thread_id,
        turn_id: child_turn_id,
        round: candidate_round,
        status: candidate_status,
        result: (candidate_status == TaskResultCandidateStatus::PendingReview).then(|| {
            TaskResult {
                summary: Some("candidate summary".to_owned()),
                data: Some(TaskValue::String("candidate result".to_owned())),
                artifacts: Vec::new(),
                completed_by_run_id: Some(run.id.clone()),
            }
        }),
        extraction_error: (candidate_status == TaskResultCandidateStatus::ExtractionFailed).then(
            || TaskError {
                code: "extraction_failed".to_owned(),
                message: "candidate extraction failed".to_owned(),
                class: TaskErrorClass::Validation,
                details: None,
                failed_run_id: Some(run.id.clone()),
            },
        ),
        summary: Some("candidate summary".to_owned()),
        diagnostics: Vec::new(),
        final_review_event_id: None,
        created_at: run.created_at.saturating_add(1),
        updated_at: run.created_at.saturating_add(1),
        resolved_at: None,
    };
    let candidate_id = candidate.id.clone();
    handle
        .record_pending_review_result_candidate(
            task_run_turn,
            candidate,
            run.created_at.saturating_add(1),
        )
        .await
        .expect("pending review candidate should record");
    (response.task.id, run.id, candidate_id)
}

fn accept_params(task_id: String, run_id: String, candidate_id: String) -> TaskAcceptParams {
    TaskAcceptParams {
        task_id,
        run_id,
        candidate_id,
        reason: Some("accepted by test".to_owned()),
    }
}

fn revise_params(task_id: String, run_id: String, candidate_id: String) -> TaskReviseParams {
    TaskReviseParams {
        task_id,
        run_id,
        candidate_id,
        feedback: "Please fix the missing acceptance criteria and return an updated result."
            .to_owned(),
        additional_instructions: Vec::new(),
    }
}

async fn parent_accept_context_for_candidate(
    runtime: &TaskRuntime,
    task_id: &str,
    candidate_id: &str,
) -> TaskMutationContext {
    let response = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: task_id.to_owned(),
        })
        .await
        .expect("task should read");
    let candidate = response
        .result_candidates
        .iter()
        .find(|candidate| candidate.id == candidate_id)
        .expect("candidate should exist");
    let lineage = response
        .thread_lineage
        .iter()
        .find(|lineage| lineage.child_thread_id == candidate.thread_id)
        .expect("candidate source lineage should exist");
    let mut context = TaskMutationContext::parent_agent(
        lineage
            .created_by_thread_id
            .clone()
            .unwrap_or_else(|| lineage.parent_thread_id.clone()),
        lineage
            .created_by_turn_id
            .clone()
            .expect("parent turn id should exist"),
    );
    context.actor_id = Some(TEST_PARENT_EXECUTION_ID.to_owned());
    context
}

async fn parent_review_actor_for_candidate(
    runtime: &TaskRuntime,
    task_id: &str,
    candidate_id: &str,
) -> TaskResultReviewActor {
    let context = parent_accept_context_for_candidate(runtime, task_id, candidate_id).await;
    TaskResultReviewActor {
        reviewer_kind: TaskResultReviewerKind::ParentAgent,
        reviewer: pioneer_protocol::TaskResultReviewerRef::AgentExecution(
            pioneer_protocol::AgentExecutionId::new(TEST_PARENT_EXECUTION_ID.to_owned())
                .expect("valid test parent Agent execution id"),
        ),
        reviewer_thread_id: context.thread_id,
        reviewer_turn_id: context.turn_id,
        reviewer_user_id: None,
        reviewer_agent_spec_id: None,
    }
}

fn composer_work_create_params(
    trigger: TaskTriggerSpec,
    parent_thread_id: &str,
    payload_version: u32,
) -> TaskCreateParams {
    let mut params = create_params(trigger);
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some(parent_thread_id.to_owned());
    params.created_by_thread_id = Some(parent_thread_id.to_owned());
    params.created_by_turn_id = Some(format!("turn_{parent_thread_id}"));
    configure_agent_task(&mut params);
    params.agent_spec = Some(agent_spec(3));
    params.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Detached,
        on_parent_cancel: TaskParentTerminalAction::KeepRunning,
        on_parent_failure: TaskParentTerminalAction::KeepRunning,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    params.metadata = Some(pioneer_protocol::TaskMetadata {
        labels: vec!["composer-work".to_owned()],
        data: None,
        composer_work: Some(pioneer_protocol::TaskComposerWork {
            version: payload_version,
            launch: pioneer_protocol::TurnStartParams {
                agent_delegation_routes: Vec::new(),
                thread_id: parent_thread_id.to_owned(),
                turn_id: format!("planned_{parent_thread_id}"),
                input: vec![pioneer_protocol::UserInput::Text {
                    text: "run the exact composer request".to_owned(),
                    text_elements: Vec::new(),
                }],
                capabilities: Vec::new(),
                model: Some("test-model".to_owned()),
                model_provider: Some("openai".to_owned()),
                sandbox_policy: None,
                mode: Some(pioneer_protocol::ThreadMode::Agent),
                agent_launch: None,
                reply_to_turn_id: None,
                mentioned_principal_ids: Vec::new(),
                execution_backend: Some(pioneer_protocol::AgentExecutionBackend::ApiProvider {
                    provider: "openai".to_owned(),
                }),
                reasoning: None,
                permission_profile: None,
                cli_runtime_options: None,
            },
        }),
    });
    params
}

#[tokio::test]
async fn composer_work_rejects_unsupported_payload_version_before_commit() {
    let runtime = runtime().await;
    let parent_thread_id = "thr_composer_version";
    let params = composer_work_create_params(
        TaskTriggerSpec::Immediate,
        parent_thread_id,
        pioneer_protocol::TASK_COMPOSER_WORK_VERSION + 1,
    );

    let error = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect_err("unsupported composer payload must fail safely");
    assert!(
        format!("{error:#}").contains("unsupported composer work payload version"),
        "unexpected error: {error:#}"
    );
    assert!(
        runtime
            .service()
            .store()
            .list_tasks(pioneer_protocol::TaskListParams {
                workspace_id: "ws_tasks".to_owned(),
                limit: Some(100),
                ..Default::default()
            })
            .await
            .expect("tasks should list")
            .is_empty(),
        "invalid composer work must not create an ordinary Task fallback"
    );
}

#[tokio::test]
async fn composer_work_accepts_scheduled_interval_and_cron_detached_tasks() {
    let runtime = runtime().await;
    let cases = [
        (
            "scheduled",
            TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            },
        ),
        (
            "interval",
            TaskTriggerSpec::Interval {
                interval_seconds: 600,
                interval_anchor_at: Some(4_000_000_000),
                catch_up_policy: None,
            },
        ),
        (
            "cron",
            TaskTriggerSpec::Cron {
                cron_expr: "0 7 * * *".to_owned(),
                timezone: "Europe/Moscow".to_owned(),
                catch_up_policy: None,
            },
        ),
    ];

    for (name, trigger) in cases {
        let parent_thread_id = format!("thr_composer_{name}");
        let params = composer_work_create_params(
            trigger,
            parent_thread_id.as_str(),
            pioneer_protocol::TASK_COMPOSER_WORK_VERSION,
        );
        let response = runtime
            .service()
            .create_task(task_create_context_for(&params), params)
            .await
            .unwrap_or_else(|error| panic!("{name} composer work should create: {error:#}"));

        assert_eq!(response.task.status, TaskStatus::Scheduled);
        assert_eq!(
            response
                .task
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.composer_work.as_ref())
                .map(|work| work.launch.thread_id.as_str()),
            Some(parent_thread_id.as_str())
        );
        assert!(
            response.run.is_none(),
            "{name} Task should wait for its trigger"
        );
    }
}

#[tokio::test]
async fn composer_work_rejects_attached_subagent_lifecycle() {
    let runtime = runtime().await;
    let mut params = composer_work_create_params(
        TaskTriggerSpec::Immediate,
        "thr_composer_attached",
        pioneer_protocol::TASK_COMPOSER_WORK_VERSION,
    );
    params.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Attached,
        on_parent_cancel: TaskParentTerminalAction::Cancel,
        on_parent_failure: TaskParentTerminalAction::Cancel,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });

    let error = runtime
        .service()
        .create_task(TaskCreateContext::default(), params)
        .await
        .expect_err("attached subagent work must keep the existing Task prompt path");
    assert!(
        format!("{error:#}").contains("requires detached task lifecycle"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn review_event_advisory_keeps_candidate_pending() {
    let runtime = runtime_with_review_config().await;
    let reviewer_spec = TaskResultReviewerSpec {
        reviewer_kind: TaskResultReviewerKind::ReviewAgent,
        agent_nickname: Some("reviewer".to_owned()),
        agent_role: Some("review".to_owned()),
        required: true,
        weight: None,
    };
    let policy = TaskAgentReviewPolicy {
        mode: TaskAgentReviewMode::ParentAgentWithReviewers,
        max_revision_rounds: 2,
        require_explicit_acceptance: true,
        reviewers: vec![reviewer_spec.clone()],
        resolution_strategy:
            TaskResultReviewResolutionStrategy::RequireAllRequiredReviewersThenParent,
    };
    let (_task_id, run_id, candidate_id) = create_waiting_review_agent_task_with_policy(
        &runtime,
        policy,
        0,
        TaskResultCandidateStatus::PendingReview,
    )
    .await;
    let reviewer_context = runtime
        .service()
        .create_task_result_reviewer_context(CreateTaskResultReviewerContextParams {
            candidate_id: candidate_id.clone(),
            reviewer_index: 0,
            reviewer_spec: reviewer_spec.clone(),
            reviewer_thread_id: TEST_REVIEWER_THREAD_ID.to_owned(),
            reviewer_turn_id: TEST_REVIEWER_EXECUTION_ID.to_owned(),
            created_at: Some(10_000),
        })
        .await
        .expect("reviewer context should create");
    persist_test_agent_turn(
        &runtime,
        TEST_REVIEWER_EXECUTION_ID,
        TEST_REVIEWER_THREAD_ID,
        TEST_REVIEWER_EXECUTION_ID,
        Some(TEST_PARENT_EXECUTION_ID),
    )
    .await;

    let recorded = runtime
        .service()
        .record_task_result_review_event(RecordTaskResultReviewEventParams {
            candidate_id: candidate_id.clone(),
            review_event_id: Some("review_advisory_pending".to_owned()),
            actor: TaskResultReviewActor {
                reviewer_kind: TaskResultReviewerKind::ReviewAgent,
                reviewer: pioneer_protocol::TaskResultReviewerRef::AgentExecution(
                    pioneer_protocol::AgentExecutionId::new(TEST_REVIEWER_EXECUTION_ID.to_owned())
                        .unwrap(),
                ),
                reviewer_thread_id: Some(reviewer_context.task_run_turn.thread_id),
                reviewer_turn_id: Some(reviewer_context.task_run_turn.turn_id),
                reviewer_user_id: None,
                reviewer_agent_spec_id: Some(crate::task_result_reviewer_spec_key(
                    0,
                    &reviewer_spec,
                )),
            },
            event_kind: TaskResultReviewEventKind::Advisory,
            decision: TaskResultReviewDecision::RequestChanges,
            feedback_text: Some("needs edits".to_owned()),
            feedback: None,
            confidence: Some(0.75),
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: Some(10_000),
        })
        .await
        .expect("advisory review event should record");

    assert_eq!(
        recorded.candidate.status,
        TaskResultCandidateStatus::PendingReview
    );
    assert!(recorded.candidate.final_review_event_id.is_none());
    assert!(recorded.resolution.is_none());
    let events = runtime
        .service()
        .store()
        .list_task_result_review_events(candidate_id.as_str())
        .await
        .expect("review events should list");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].decision, TaskResultReviewDecision::RequestChanges);
    assert!(
        runtime
            .service()
            .store()
            .get_pending_task_result_candidate(run_id.as_str())
            .await
            .expect("pending candidate should query")
            .is_some()
    );
}

#[tokio::test]
async fn review_event_decision_accept_resolves_candidate() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let actor =
        parent_review_actor_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str()).await;

    let recorded = runtime
        .service()
        .record_task_result_review_event(RecordTaskResultReviewEventParams {
            candidate_id: candidate_id.clone(),
            review_event_id: Some("review_parent_accept".to_owned()),
            actor,
            event_kind: TaskResultReviewEventKind::Decision,
            decision: TaskResultReviewDecision::Accept,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: Some(10_001),
        })
        .await
        .expect("parent accept should record");

    assert_eq!(
        recorded.candidate.status,
        TaskResultCandidateStatus::Accepted
    );
    assert_eq!(
        recorded.candidate.final_review_event_id.as_deref(),
        Some("review_parent_accept")
    );
    assert!(recorded.candidate.resolved_at.is_some());
}

#[tokio::test]
async fn review_event_decision_request_changes_rejects_candidate() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let actor =
        parent_review_actor_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str()).await;

    let recorded = runtime
        .service()
        .record_task_result_review_event(RecordTaskResultReviewEventParams {
            candidate_id,
            review_event_id: Some("review_parent_request_changes".to_owned()),
            actor,
            event_kind: TaskResultReviewEventKind::Decision,
            decision: TaskResultReviewDecision::RequestChanges,
            feedback_text: Some("redo this".to_owned()),
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: Some(10_002),
        })
        .await
        .expect("parent request changes should record");

    assert_eq!(
        recorded.candidate.status,
        TaskResultCandidateStatus::Rejected
    );
    assert_eq!(
        recorded.candidate.final_review_event_id.as_deref(),
        Some("review_parent_request_changes")
    );
}

#[tokio::test]
async fn review_event_cancel_resolves_candidate_cancelled() {
    let runtime = runtime_with_review_config().await;
    let (_task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;

    let recorded = runtime
        .service()
        .record_task_result_review_event(RecordTaskResultReviewEventParams {
            candidate_id,
            review_event_id: Some("review_system_cancel".to_owned()),
            actor: TaskResultReviewActor::system(),
            event_kind: TaskResultReviewEventKind::Decision,
            decision: TaskResultReviewDecision::Cancel,
            feedback_text: Some("cancelled by test".to_owned()),
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: Some(10_003),
        })
        .await
        .expect("system cancel should record");

    assert_eq!(
        recorded.candidate.status,
        TaskResultCandidateStatus::Cancelled
    );
    assert_eq!(
        recorded.candidate.final_review_event_id.as_deref(),
        Some("review_system_cancel")
    );
}

#[tokio::test]
async fn review_event_override_updates_final_pointer_without_losing_history() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let actor =
        parent_review_actor_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str()).await;

    runtime
        .service()
        .record_task_result_review_event(RecordTaskResultReviewEventParams {
            candidate_id: candidate_id.clone(),
            review_event_id: Some("review_parent_reject_before_override".to_owned()),
            actor: actor.clone(),
            event_kind: TaskResultReviewEventKind::Decision,
            decision: TaskResultReviewDecision::Reject,
            feedback_text: Some("reject first".to_owned()),
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: Some(10_004),
        })
        .await
        .expect("initial reject should record");

    let overridden = runtime
        .service()
        .record_task_result_review_event(RecordTaskResultReviewEventParams {
            candidate_id: candidate_id.clone(),
            review_event_id: Some("review_parent_override_accept".to_owned()),
            actor,
            event_kind: TaskResultReviewEventKind::Override,
            decision: TaskResultReviewDecision::Accept,
            feedback_text: Some("accept after dispute".to_owned()),
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: Some(10_005),
        })
        .await
        .expect("override accept should record");

    assert_eq!(
        overridden.candidate.status,
        TaskResultCandidateStatus::Accepted
    );
    assert_eq!(
        overridden.candidate.final_review_event_id.as_deref(),
        Some("review_parent_override_accept")
    );
    let events = runtime
        .service()
        .store()
        .list_task_result_review_events(candidate_id.as_str())
        .await
        .expect("review events should list");
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[1].supersedes_review_event_id.as_deref(),
        Some("review_parent_reject_before_override")
    );
}

#[tokio::test]
async fn reviewer_contexts_allow_multiple_review_turns_for_one_candidate_round() {
    let runtime = runtime_with_review_config().await;
    let (_task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let reviewer_spec = TaskResultReviewerSpec {
        reviewer_kind: TaskResultReviewerKind::ReviewAgent,
        agent_nickname: Some("reviewer".to_owned()),
        agent_role: Some("review".to_owned()),
        required: true,
        weight: None,
    };

    let first = runtime
        .service()
        .create_task_result_reviewer_context(CreateTaskResultReviewerContextParams {
            candidate_id: candidate_id.clone(),
            reviewer_index: 0,
            reviewer_spec: reviewer_spec.clone(),
            reviewer_thread_id: "reviewer_thread_one".to_owned(),
            reviewer_turn_id: "reviewer_turn_one".to_owned(),
            created_at: Some(20_000),
        })
        .await
        .expect("first reviewer context should create");
    let second = runtime
        .service()
        .create_task_result_reviewer_context(CreateTaskResultReviewerContextParams {
            candidate_id: candidate_id.clone(),
            reviewer_index: 1,
            reviewer_spec,
            reviewer_thread_id: "reviewer_thread_two".to_owned(),
            reviewer_turn_id: "reviewer_turn_two".to_owned(),
            created_at: Some(20_001),
        })
        .await
        .expect("second reviewer context should create");

    assert!(first.created);
    assert!(second.created);
    assert_eq!(
        first.binding.binding_kind,
        TaskRunThreadBindingKind::Reviewer
    );
    assert_eq!(
        second.binding.binding_kind,
        TaskRunThreadBindingKind::Reviewer
    );
    assert_eq!(first.task_run_turn.kind, TaskRunTurnKind::Review);
    assert_eq!(second.task_run_turn.kind, TaskRunTurnKind::Review);
    assert_eq!(first.task_run_turn.round, 0);
    assert_eq!(second.task_run_turn.round, 0);
    assert_eq!(
        first.task_run_turn.reviews_candidate_id.as_deref(),
        Some(candidate_id.as_str())
    );
    assert_eq!(
        second.task_run_turn.reviews_candidate_id.as_deref(),
        Some(candidate_id.as_str())
    );
    assert!(second.task_run_turn.sequence > first.task_run_turn.sequence);

    let turns = runtime
        .service()
        .store()
        .list_task_run_turns(run_id.as_str())
        .await
        .expect("turns should list");
    assert_eq!(
        turns
            .iter()
            .filter(|turn| turn.kind == TaskRunTurnKind::Review)
            .count(),
        2
    );
    assert!(
        runtime
            .service()
            .store()
            .get_task_result_candidate_by_turn(first.task_run_turn.id.as_str())
            .await
            .expect("candidate lookup should succeed")
            .is_none(),
        "review turns must not produce task result candidates"
    );

    let first_again = runtime
        .service()
        .create_task_result_reviewer_context(CreateTaskResultReviewerContextParams {
            candidate_id,
            reviewer_index: 0,
            reviewer_spec: TaskResultReviewerSpec {
                reviewer_kind: TaskResultReviewerKind::ReviewAgent,
                agent_nickname: Some("reviewer".to_owned()),
                agent_role: Some("review".to_owned()),
                required: true,
                weight: None,
            },
            reviewer_thread_id: "reviewer_thread_one".to_owned(),
            reviewer_turn_id: "reviewer_turn_one".to_owned(),
            created_at: Some(20_010),
        })
        .await
        .expect("existing reviewer context should reload");
    assert!(!first_again.created);
    assert_eq!(first_again.task_run_turn.id, first.task_run_turn.id);
}

#[tokio::test]
async fn user_review_event_resolves_when_policy_is_user_final() {
    let runtime = runtime_with_review_config().await;
    let policy = TaskAgentReviewPolicy {
        mode: TaskAgentReviewMode::UserApproval,
        max_revision_rounds: 1,
        require_explicit_acceptance: true,
        reviewers: Vec::new(),
        resolution_strategy: TaskResultReviewResolutionStrategy::UserFinal,
    };
    let (_task_id, _run_id, candidate_id) = create_waiting_review_agent_task_with_policy(
        &runtime,
        policy,
        0,
        TaskResultCandidateStatus::PendingReview,
    )
    .await;

    let recorded = runtime
        .service()
        .record_user_task_result_review_event(
            TaskMutationContext::user(TEST_PRINCIPAL_ID),
            RecordUserTaskResultReviewEventParams {
                candidate_id,
                review_event_id: Some("review_user_accept".to_owned()),
                decision: TaskResultReviewDecision::Accept,
                feedback_text: Some("approved".to_owned()),
                feedback: None,
                confidence: None,
                next_task_run_turn_id: None,
                created_at: Some(30_000),
            },
        )
        .await
        .expect("user final review should record");

    assert_eq!(
        recorded.review_event.reviewer_kind,
        TaskResultReviewerKind::User
    );
    assert_eq!(
        recorded.review_event.reviewer_user_id.as_deref(),
        Some(TEST_PRINCIPAL_ID)
    );
    assert_eq!(
        recorded.candidate.status,
        TaskResultCandidateStatus::Accepted
    );
    assert_eq!(
        recorded.candidate.final_review_event_id.as_deref(),
        Some("review_user_accept")
    );
}

#[tokio::test]
async fn user_review_event_is_blocked_when_policy_is_parent_final() {
    let runtime = runtime_with_review_config().await;
    let (_task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;

    let error = runtime
        .service()
        .record_user_task_result_review_event(
            TaskMutationContext::user(TEST_PRINCIPAL_ID),
            RecordUserTaskResultReviewEventParams {
                candidate_id,
                review_event_id: Some("review_user_blocked".to_owned()),
                decision: TaskResultReviewDecision::Accept,
                feedback_text: None,
                feedback: None,
                confidence: None,
                next_task_run_turn_id: None,
                created_at: Some(30_001),
            },
        )
        .await
        .expect_err("user final review should be blocked by parent-final policy");
    assert!(
        format!("{error:#}").contains("immutable reviewer intent"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn accept_validation_allows_own_parent_review_context() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;

    runtime
        .service()
        .validate_task_result_candidate_accept_for_test(
            context,
            accept_params(task_id, run_id, candidate_id),
        )
        .await
        .expect("parent context should be allowed to accept");
}

#[tokio::test]
async fn result_read_returns_exact_candidate_for_its_parent_reviewer() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;

    let candidate = runtime
        .service()
        .get_task_result_candidate_for_reviewer(context, candidate_id.as_str())
        .await
        .expect("exact parent reviewer should read its immutable candidate");

    assert_eq!(candidate.id, candidate_id);
    assert_eq!(candidate.round, 0);
    assert!(candidate.result.is_some());
}

#[tokio::test]
async fn result_read_rejects_sibling_execution_for_parent_candidate() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let mut context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    context.actor_id = Some("Z".repeat(21));

    let error = runtime
        .service()
        .get_task_result_candidate_for_reviewer(context, candidate_id.as_str())
        .await
        .expect_err("a sibling execution must not read another parent's candidate");

    assert!(
        format!("{error:#}").contains("immutable reviewer intent"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn accept_validation_rejects_missing_actor_without_state_change() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;

    let error = runtime
        .service()
        .validate_task_result_candidate_accept_for_test(
            TaskMutationContext::default(),
            accept_params(task_id.clone(), run_id, candidate_id.clone()),
        )
        .await
        .expect_err("accept without parent/user actor should fail");
    assert!(
        format!("{error:#}").contains("requires parent-agent thread/turn"),
        "unexpected error: {error:#}"
    );
    let candidate = runtime
        .service()
        .store()
        .get_task_result_candidate(candidate_id.as_str())
        .await
        .expect("candidate lookup should succeed")
        .expect("candidate should still exist");
    assert_eq!(candidate.status, TaskResultCandidateStatus::PendingReview);
    assert!(candidate.final_review_event_id.is_none());
}

#[tokio::test]
async fn accept_validation_rejects_wrong_run_id() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;

    let error = runtime
        .service()
        .validate_task_result_candidate_accept_for_test(
            context,
            accept_params(task_id, "wrong_run_for_accept".to_owned(), candidate_id),
        )
        .await
        .expect_err("wrong run id should fail");
    assert!(
        format!("{error:#}").contains("not found for task"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn accept_validation_rejects_terminal_run() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    let handle = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        task_id.clone(),
        run_id.clone(),
    );
    handle
        .complete_run(
            Some(TaskResult {
                summary: Some("already completed".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: Some(run_id.clone()),
            }),
            40_000,
        )
        .await
        .expect("test run should complete");

    let error = runtime
        .service()
        .validate_task_result_candidate_accept_for_test(
            context,
            accept_params(task_id, run_id, candidate_id),
        )
        .await
        .expect_err("terminal run should fail accept validation");
    assert!(
        format!("{error:#}").contains("not waiting for review"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn accept_validation_rejects_extraction_failed_candidate() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) = create_waiting_review_agent_task(
        &runtime,
        2,
        0,
        TaskResultCandidateStatus::ExtractionFailed,
    )
    .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;

    let error = runtime
        .service()
        .validate_task_result_candidate_accept_for_test(
            context,
            accept_params(task_id, run_id, candidate_id),
        )
        .await
        .expect_err("extraction_failed candidate cannot be accepted");
    assert!(
        format!("{error:#}").contains("extraction_failed"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn accept_validation_rejects_active_child_turn() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    let response = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: task_id.clone(),
        })
        .await
        .expect("task should read");
    let candidate = response
        .result_candidates
        .iter()
        .find(|candidate| candidate.id == candidate_id)
        .expect("candidate should exist");
    let active_turn = TaskRunTurn {
        id: "active_accept_validation_turn".to_owned(),
        task_id: task_id.clone(),
        run_id: run_id.clone(),
        execution_id: None,
        thread_id: candidate.thread_id.clone(),
        turn_id: "active_accept_validation_child_turn".to_owned(),
        kind: TaskRunTurnKind::Revision,
        round: candidate.round.saturating_add(1),
        sequence: 99,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: Some(candidate.id.clone()),
        requested_by_review_event_id: None,
        created_at: 50_000,
        started_at: Some(50_000),
        completed_at: None,
    };
    let appended = runtime
        .service()
        .append_event(
            TaskEventPayload::TaskRunTurnStarted {
                task_run_turn: active_turn,
            },
            50_000,
        )
        .await
        .expect("active turn should append");
    runtime.service().publish_and_wake(vec![appended]).await;

    let error = runtime
        .service()
        .validate_task_result_candidate_accept_for_test(
            context,
            accept_params(task_id, run_id, candidate_id),
        )
        .await
        .expect_err("active child turn should block accept");
    assert!(
        format!("{error:#}").contains("in-progress child turn"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn revise_validation_rejects_missing_actor() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;

    let error = runtime
        .service()
        .validate_task_result_candidate_revise_for_test(
            TaskMutationContext::default(),
            revise_params(task_id, run_id, candidate_id),
        )
        .await
        .expect_err("revise without actor should fail");
    assert!(
        format!("{error:#}").contains("requires parent-agent thread/turn"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn revise_validation_rejects_max_revision_rounds() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 2, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;

    let error = runtime
        .service()
        .validate_task_result_candidate_revise_for_test(
            context,
            revise_params(task_id, run_id, candidate_id),
        )
        .await
        .expect_err("max revision rounds should fail");
    assert!(
        format!("{error:#}").contains("max_revision_rounds"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn revise_validation_rejects_empty_feedback() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    let mut params = revise_params(task_id, run_id, candidate_id);
    params.feedback = "   ".to_owned();

    let error = runtime
        .service()
        .validate_task_result_candidate_revise_for_test(context, params)
        .await
        .expect_err("empty feedback should fail");
    assert!(
        format!("{error:#}").contains("feedback must be non-empty"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn revise_validation_rejects_active_child_turn() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    let response = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: task_id.clone(),
        })
        .await
        .expect("task should read");
    let candidate = response
        .result_candidates
        .iter()
        .find(|candidate| candidate.id == candidate_id)
        .expect("candidate should exist");
    let active_turn = TaskRunTurn {
        id: "active_revise_validation_turn".to_owned(),
        task_id: task_id.clone(),
        run_id: run_id.clone(),
        execution_id: None,
        thread_id: candidate.thread_id.clone(),
        turn_id: "active_revise_validation_child_turn".to_owned(),
        kind: TaskRunTurnKind::Revision,
        round: candidate.round.saturating_add(1),
        sequence: 99,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: Some(candidate.id.clone()),
        requested_by_review_event_id: None,
        created_at: 50_000,
        started_at: Some(50_000),
        completed_at: None,
    };
    let appended = runtime
        .service()
        .append_event(
            TaskEventPayload::TaskRunTurnStarted {
                task_run_turn: active_turn,
            },
            50_000,
        )
        .await
        .expect("active turn should append");
    runtime.service().publish_and_wake(vec![appended]).await;

    let error = runtime
        .service()
        .validate_task_result_candidate_revise_for_test(
            context,
            revise_params(task_id, run_id, candidate_id),
        )
        .await
        .expect_err("active child turn should block revise");
    assert!(
        format!("{error:#}").contains("in-progress child turn"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn revise_validation_rejects_terminal_run() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    let handle = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        task_id.clone(),
        run_id.clone(),
    );
    handle
        .complete_run(
            Some(TaskResult {
                summary: Some("already completed".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: Some(run_id.clone()),
            }),
            40_000,
        )
        .await
        .expect("test run should complete");

    let error = runtime
        .service()
        .validate_task_result_candidate_revise_for_test(
            context,
            revise_params(task_id, run_id, candidate_id),
        )
        .await
        .expect_err("terminal run should fail revise validation");
    assert!(
        format!("{error:#}").contains("not waiting for review"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn revise_task_result_candidate_records_rejection_and_revision_turn_idempotently() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    let params = revise_params(task_id.clone(), run_id.clone(), candidate_id.clone());

    let revised = runtime
        .service()
        .revise_task_result_candidate(context.clone(), params.clone())
        .await
        .expect("parent revise should reject candidate and reserve revision turn");

    assert!(revised.requested);
    assert!(!revised.already_requested);
    assert_eq!(revised.task.status, TaskStatus::Running);
    assert_eq!(revised.run.status, TaskRunStatus::Running);
    assert_eq!(
        revised.candidate.status,
        TaskResultCandidateStatus::Rejected
    );
    assert_eq!(
        revised.review_event.decision,
        TaskResultReviewDecision::RequestChanges
    );
    assert_eq!(
        revised.review_event.next_task_run_turn_id.as_deref(),
        Some(revised.task_run_turn.id.as_str())
    );
    assert_eq!(revised.task_run_turn.kind, TaskRunTurnKind::Revision);
    assert_eq!(revised.task_run_turn.round, 1);
    assert_eq!(
        revised.task_run_turn.requested_by_candidate_id.as_deref(),
        Some(candidate_id.as_str())
    );
    assert_eq!(
        revised
            .task_run_turn
            .requested_by_review_event_id
            .as_deref(),
        Some(revised.review_event.id.as_str())
    );
    assert_eq!(revised.child_thread_id, revised.candidate.thread_id);

    let review_events = runtime
        .service()
        .store()
        .list_task_result_review_events(candidate_id.as_str())
        .await
        .expect("review events should list");
    assert_eq!(review_events.len(), 1);
    let revision_turns = runtime
        .service()
        .store()
        .list_task_run_turns(run_id.as_str())
        .await
        .expect("turns should list")
        .into_iter()
        .filter(|turn| turn.kind == TaskRunTurnKind::Revision)
        .collect::<Vec<_>>();
    assert_eq!(revision_turns, vec![revised.task_run_turn.clone()]);
    let task_events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: task_id.clone(),
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("events should list");
    assert!(task_events.events.iter().any(|event| matches!(
        event.payload,
        TaskEventPayload::TaskRevisionRequested { .. }
    )));

    let duplicate = runtime
        .service()
        .revise_task_result_candidate(context, params)
        .await
        .expect("duplicate parent revise should be idempotent");
    assert!(duplicate.already_requested);
    assert_eq!(duplicate.review_event.id, revised.review_event.id);
    assert_eq!(duplicate.task_run_turn.id, revised.task_run_turn.id);
    let review_events_after_duplicate = runtime
        .service()
        .store()
        .list_task_result_review_events(candidate_id.as_str())
        .await
        .expect("review events should list after duplicate");
    assert_eq!(review_events_after_duplicate.len(), 1);
    let revision_turns_after_duplicate = runtime
        .service()
        .store()
        .list_task_run_turns(run_id.as_str())
        .await
        .expect("turns should list after duplicate")
        .into_iter()
        .filter(|turn| turn.kind == TaskRunTurnKind::Revision)
        .count();
    assert_eq!(revision_turns_after_duplicate, 1);
}

#[tokio::test]
async fn revision_completion_creates_next_round_pending_review_candidate() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;

    let revised = runtime
        .service()
        .revise_task_result_candidate(
            context,
            revise_params(task_id.clone(), run_id.clone(), candidate_id.clone()),
        )
        .await
        .expect("parent revise should reserve revision turn");
    let completed_at = revised.task_run_turn.created_at.saturating_add(10);
    let mut completed_turn = revised.task_run_turn.clone();
    completed_turn.status = TaskRunTurnStatus::CandidateCreated;
    completed_turn.completed_at = Some(completed_at);
    let next_candidate = TaskResultCandidate {
        id: format!("candidate_revision_{}", run_id),
        task_id: task_id.clone(),
        run_id: run_id.clone(),
        task_run_turn_id: completed_turn.id.clone(),
        thread_id: completed_turn.thread_id.clone(),
        turn_id: completed_turn.turn_id.clone(),
        round: completed_turn.round,
        status: TaskResultCandidateStatus::PendingReview,
        result: Some(TaskResult {
            summary: Some("revised candidate summary".to_owned()),
            data: Some(TaskValue::String("revised candidate result".to_owned())),
            artifacts: Vec::new(),
            completed_by_run_id: Some(run_id.clone()),
        }),
        extraction_error: None,
        summary: Some("revised candidate summary".to_owned()),
        diagnostics: Vec::new(),
        final_review_event_id: None,
        created_at: completed_at,
        updated_at: completed_at,
        resolved_at: None,
    };
    let handle = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        task_id.clone(),
        run_id.clone(),
    );
    handle
        .record_pending_review_result_candidate(
            completed_turn,
            next_candidate.clone(),
            completed_at,
        )
        .await
        .expect("revision candidate should enter review");

    let response = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: task_id.clone(),
        })
        .await
        .expect("task should read");
    assert_eq!(response.task.status, TaskStatus::WaitingReview);
    assert_eq!(
        response
            .runs
            .iter()
            .find(|run| run.id == run_id)
            .expect("run should exist")
            .status,
        TaskRunStatus::WaitingReview
    );
    let previous = runtime
        .service()
        .store()
        .get_task_result_candidate(candidate_id.as_str())
        .await
        .expect("previous candidate lookup should succeed")
        .expect("previous candidate should exist");
    assert_eq!(previous.status, TaskResultCandidateStatus::Rejected);
    let pending = runtime
        .service()
        .store()
        .get_pending_task_result_candidate(run_id.as_str())
        .await
        .expect("pending candidate lookup should succeed")
        .expect("revision candidate should be pending review");
    assert_eq!(pending.id, next_candidate.id);
    assert_eq!(pending.round, 1);
    assert_eq!(
        pending.task_run_turn_id, revised.task_run_turn.id,
        "new candidate must stay linked to the revision turn"
    );
    assert!(
        runtime
            .service()
            .store()
            .get_accepted_task_result_candidate(run_id.as_str())
            .await
            .expect("accepted candidate lookup should succeed")
            .is_none(),
        "review-enabled revision result must not auto-accept"
    );
}

#[tokio::test]
async fn accept_task_result_candidate_parent_records_accept_and_finalizes_run() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    let params = accept_params(task_id.clone(), run_id.clone(), candidate_id.clone());

    let accepted = runtime
        .service()
        .accept_task_result_candidate(context.clone(), params.clone())
        .await
        .expect("parent accept should finalize candidate");

    assert!(accepted.accepted);
    assert!(!accepted.already_accepted);
    assert_eq!(accepted.status, TaskStatus::Completed);
    assert_eq!(accepted.task.status, TaskStatus::Completed);
    assert_eq!(accepted.run.status, TaskRunStatus::Succeeded);
    assert_eq!(
        accepted.candidate.status,
        TaskResultCandidateStatus::Accepted
    );
    assert_eq!(
        accepted.review_event.reviewer_kind,
        TaskResultReviewerKind::ParentAgent
    );
    assert_eq!(
        accepted.review_event.decision,
        TaskResultReviewDecision::Accept
    );
    assert_eq!(
        accepted.result.summary.as_deref(),
        Some("candidate summary")
    );
    assert_eq!(
        accepted
            .run
            .result
            .as_ref()
            .and_then(|result| result.summary.as_deref()),
        Some("candidate summary")
    );
    assert_eq!(
        accepted
            .task
            .result
            .as_ref()
            .and_then(|result| result.summary.as_deref()),
        Some("candidate summary")
    );

    let events = runtime
        .service()
        .store()
        .list_task_result_review_events(candidate_id.as_str())
        .await
        .expect("review events should list");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].id, accepted.review_event.id);
    assert!(
        runtime
            .service()
            .store()
            .get_pending_task_result_candidate(run_id.as_str())
            .await
            .expect("pending candidate lookup should succeed")
            .is_none()
    );

    let duplicate = runtime
        .service()
        .accept_task_result_candidate(context, params)
        .await
        .expect("duplicate parent accept should be idempotent");
    assert!(duplicate.already_accepted);
    assert_eq!(duplicate.review_event.id, accepted.review_event.id);
    let events_after_duplicate = runtime
        .service()
        .store()
        .list_task_result_review_events(candidate_id.as_str())
        .await
        .expect("review events should list after duplicate");
    assert_eq!(events_after_duplicate.len(), 1);
}

#[tokio::test]
async fn auto_accept_expired_review_candidates_accepts_pending_candidate_after_timeout() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let candidate = runtime
        .service()
        .store()
        .get_task_result_candidate(candidate_id.as_str())
        .await
        .expect("candidate lookup should succeed")
        .expect("candidate should exist");

    let before_timeout = runtime
        .service()
        .auto_accept_expired_review_candidates(candidate.created_at.saturating_add(299), 1024)
        .await
        .expect("before-timeout scan should succeed");
    assert_eq!(before_timeout, 0);

    let accepted = runtime
        .service()
        .auto_accept_expired_review_candidates(candidate.created_at.saturating_add(300), 1024)
        .await
        .expect("timeout scan should accept candidate");
    assert_eq!(accepted, 1);

    let response = runtime
        .service()
        .store()
        .get_task(task_id.as_str())
        .await
        .expect("task lookup should succeed")
        .expect("task should exist");
    assert_eq!(response.task.status, TaskStatus::Completed);
    let run = response
        .runs
        .iter()
        .find(|run| run.id == run_id)
        .expect("run should exist");
    assert_eq!(run.status, TaskRunStatus::Succeeded);
    assert_eq!(
        run.result
            .as_ref()
            .and_then(|result| result.summary.as_deref()),
        Some("candidate summary")
    );
    let accepted_candidate = runtime
        .service()
        .store()
        .get_task_result_candidate(candidate_id.as_str())
        .await
        .expect("candidate lookup should succeed after accept")
        .expect("candidate should exist after accept");
    assert_eq!(
        accepted_candidate.status,
        TaskResultCandidateStatus::Accepted
    );
    assert!(accepted_candidate.final_review_event_id.is_some());

    let events = runtime
        .service()
        .store()
        .list_task_result_review_events(candidate_id.as_str())
        .await
        .expect("review events should list");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_kind, TaskResultReviewEventKind::SystemAuto);
    assert_eq!(events[0].reviewer_kind, TaskResultReviewerKind::RuntimeAuto);
    assert_eq!(events[0].decision, TaskResultReviewDecision::Accept);
    assert_eq!(
        accepted_candidate.final_review_event_id.as_deref(),
        Some(events[0].id.as_str())
    );
    assert_eq!(
        events[0].created_at,
        candidate.created_at.saturating_add(300)
    );
}

#[tokio::test]
async fn auto_accept_expired_review_candidates_skips_extraction_failed_candidate() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) = create_waiting_review_agent_task(
        &runtime,
        2,
        0,
        TaskResultCandidateStatus::ExtractionFailed,
    )
    .await;
    let candidate = runtime
        .service()
        .store()
        .get_task_result_candidate(candidate_id.as_str())
        .await
        .expect("candidate lookup should succeed")
        .expect("candidate should exist");

    let accepted = runtime
        .service()
        .auto_accept_expired_review_candidates(candidate.created_at.saturating_add(300), 1024)
        .await
        .expect("timeout scan should succeed");
    assert_eq!(accepted, 0);

    let response = runtime
        .service()
        .store()
        .get_task(task_id.as_str())
        .await
        .expect("task lookup should succeed")
        .expect("task should exist");
    assert_eq!(response.task.status, TaskStatus::WaitingReview);
    let candidate_after = runtime
        .service()
        .store()
        .get_task_result_candidate(candidate_id.as_str())
        .await
        .expect("candidate lookup should succeed after scan")
        .expect("candidate should exist after scan");
    assert_eq!(
        candidate_after.status,
        TaskResultCandidateStatus::ExtractionFailed
    );
    let events = runtime
        .service()
        .store()
        .list_task_result_review_events(candidate_id.as_str())
        .await
        .expect("review events should list");
    assert!(events.is_empty());
}

#[tokio::test]
async fn accept_task_result_candidate_duplicate_still_requires_authorized_actor() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    let params = accept_params(task_id, run_id, candidate_id.clone());

    runtime
        .service()
        .accept_task_result_candidate(context, params.clone())
        .await
        .expect("initial parent accept should finalize candidate");

    let error = runtime
        .service()
        .accept_task_result_candidate(TaskMutationContext::default(), params)
        .await
        .expect_err("duplicate accept without actor must stay unauthorized");
    assert!(
        format!("{error:#}").contains("requires parent-agent thread/turn"),
        "unexpected error: {error:#}"
    );
    let events = runtime
        .service()
        .store()
        .list_task_result_review_events(candidate_id.as_str())
        .await
        .expect("review events should list");
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn accept_task_result_candidate_promotes_candidate_artifact_binding_to_final_result() {
    let runtime = runtime_with_review_config().await;
    let (task_id, run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;
    let context =
        parent_accept_context_for_candidate(&runtime, task_id.as_str(), candidate_id.as_str())
            .await;
    let mut candidate = runtime
        .service()
        .store()
        .get_task_result_candidate(candidate_id.as_str())
        .await
        .expect("candidate should load")
        .expect("candidate should exist");
    let artifact = runtime
        .service()
        .store()
        .ingest_artifact_metadata(
            NewArtifactBlobRecord {
                workspace_id: "ws_tasks".to_owned(),
                sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
                size_bytes: 17,
                mime_type: Some("text/plain".to_owned()),
                storage_backend: "test".to_owned(),
                storage_key: "accept-candidate-artifact.txt".to_owned(),
                metadata: BTreeMap::new(),
            },
            IngestArtifactMetadataRecord {
                workspace_id: "ws_tasks".to_owned(),
                primary_thread_id: Some(candidate.thread_id.clone()),
                display_name: "accept-candidate-artifact.txt".to_owned(),
                kind: ArtifactKind::Text,
                mime_type: Some("text/plain".to_owned()),
                created_by_kind: ArtifactCreatedByKind::Task,
                created_by_actor_id: Some(task_id.clone()),
                metadata: BTreeMap::new(),
            },
            None,
            BTreeMap::new(),
        )
        .await
        .expect("artifact should ingest");
    let artifact_id = artifact.artifact.id.clone();
    let version_id = artifact.version.id.clone();
    candidate
        .result
        .as_mut()
        .expect("candidate should have result")
        .artifacts
        .push(TaskArtifact {
            artifact_id: Some(artifact_id.clone()),
            version_id: Some(version_id.clone()),
            path: None,
            url: None,
            mime_type: Some("text/plain".to_owned()),
            metadata: None,
        });
    runtime
        .service()
        .store()
        .upsert_task_result_candidate(candidate.clone())
        .await
        .expect("candidate artifact result should update");
    runtime
        .service()
        .store()
        .bind_artifact(
            "ws_tasks",
            artifact_id.as_str(),
            Some(version_id.as_str()),
            ArtifactBindingTargetRecord {
                thread_id: Some(candidate.thread_id.clone()),
                turn_id: Some(candidate.turn_id.clone()),
                message_id: None,
                turn_item_id: None,
                tool_call_id: None,
                task_id: Some(task_id.clone()),
                task_run_id: Some(run_id.clone()),
                binding_kind: ArtifactBindingKind::TaskResultCandidate,
                direction: ArtifactBindingDirection::Output,
                role: Some(ArtifactRole::Task),
                item_index: Some(0),
            },
            BTreeMap::new(),
        )
        .await
        .expect("candidate artifact binding should insert");

    let accepted = runtime
        .service()
        .accept_task_result_candidate(
            context.clone(),
            accept_params(task_id.clone(), run_id.clone(), candidate_id.clone()),
        )
        .await
        .expect("artifact candidate accept should finalize");
    assert_eq!(accepted.run.status, TaskRunStatus::Succeeded);

    let summary = runtime
        .service()
        .store()
        .get_artifact_summary("ws_tasks", artifact_id.as_str(), Some(version_id.as_str()))
        .await
        .expect("artifact summary should load");
    assert!(summary.bindings.iter().any(|binding| {
        binding.binding_kind == ArtifactBindingKind::TaskResultCandidate
            && binding.task_id.as_deref() == Some(task_id.as_str())
            && binding.task_run_id.as_deref() == Some(run_id.as_str())
            && binding.item_index == Some(0)
    }));
    let final_bindings = summary
        .bindings
        .iter()
        .filter(|binding| {
            binding.binding_kind == ArtifactBindingKind::TaskResult
                && binding.task_id.as_deref() == Some(task_id.as_str())
                && binding.task_run_id.as_deref() == Some(run_id.as_str())
                && binding.item_index == Some(0)
        })
        .collect::<Vec<_>>();
    assert_eq!(final_bindings.len(), 1);

    runtime
        .service()
        .accept_task_result_candidate(
            context,
            accept_params(task_id.clone(), run_id.clone(), candidate_id),
        )
        .await
        .expect("duplicate artifact accept should stay idempotent");
    let summary_after_duplicate = runtime
        .service()
        .store()
        .get_artifact_summary("ws_tasks", artifact_id.as_str(), Some(version_id.as_str()))
        .await
        .expect("artifact summary should load after duplicate accept");
    let final_binding_count = summary_after_duplicate
        .bindings
        .iter()
        .filter(|binding| {
            binding.binding_kind == ArtifactBindingKind::TaskResult
                && binding.task_id.as_deref() == Some(task_id.as_str())
                && binding.task_run_id.as_deref() == Some(run_id.as_str())
                && binding.item_index == Some(0)
        })
        .count();
    assert_eq!(final_binding_count, 1);
}

#[tokio::test]
async fn accept_task_result_candidate_user_records_user_review_when_policy_allows() {
    let runtime = runtime_with_review_config().await;
    let policy = TaskAgentReviewPolicy {
        mode: TaskAgentReviewMode::UserApproval,
        max_revision_rounds: 1,
        require_explicit_acceptance: true,
        reviewers: Vec::new(),
        resolution_strategy: TaskResultReviewResolutionStrategy::UserFinal,
    };
    let (task_id, run_id, candidate_id) = create_waiting_review_agent_task_with_policy(
        &runtime,
        policy,
        0,
        TaskResultCandidateStatus::PendingReview,
    )
    .await;

    let accepted = runtime
        .service()
        .accept_task_result_candidate(
            TaskMutationContext::user(TEST_PRINCIPAL_ID),
            accept_params(task_id, run_id, candidate_id),
        )
        .await
        .expect("user accept should finalize candidate for user-final policy");

    assert_eq!(
        accepted.review_event.reviewer_kind,
        TaskResultReviewerKind::User
    );
    assert_eq!(
        accepted.review_event.reviewer_user_id.as_deref(),
        Some(TEST_PRINCIPAL_ID)
    );
    assert_eq!(
        accepted.candidate.status,
        TaskResultCandidateStatus::Accepted
    );
    assert_eq!(accepted.run.status, TaskRunStatus::Succeeded);
    assert_eq!(accepted.task.status, TaskStatus::Completed);
}

#[tokio::test]
async fn immediate_create_uses_scheduler_and_creates_one_queued_run() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");

    assert_eq!(response.task.status, TaskStatus::Queued);
    let run = response.run.expect("immediate task should have a run");
    assert_eq!(run.status, TaskRunStatus::Queued);

    let created_again = runtime
        .process_due_once(i64::MAX / 4)
        .await
        .expect("scheduler should be idempotent");
    assert_eq!(created_again, 0);
}

#[tokio::test]
async fn duplicate_scheduler_wakeups_do_not_create_duplicate_one_shot_runs() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 10,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");

    let (left, right) = tokio::join!(runtime.process_due_once(10), runtime.process_due_once(10));
    let created = left.expect("left scheduler pass should succeed")
        + right.expect("right scheduler pass should succeed");
    assert_eq!(created, 1);

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
}

#[tokio::test]
async fn agent_run_is_atomically_claimed_before_spawn() {
    let runtime = runtime().await;
    let starts = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    runtime
        .register_executor(Arc::new(SlowAgentExecutor {
            starts: starts.clone(),
            release: release.clone(),
        }))
        .await;

    let mut params = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 10,
        timezone: Some("UTC".to_owned()),
        catch_up_policy: None,
    });
    configure_agent_task(&mut params);
    let mut spec = agent_spec(2);
    spec.prompt.instructions = vec!["Execute the scheduled test run once.".to_owned()];
    spec.prompt.output_instructions = Some("Return a concise test result.".to_owned());
    params.agent_spec = Some(spec);
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("scheduled agent task should create");

    assert_eq!(
        runtime
            .process_due_once(10)
            .await
            .expect("first scheduler pass should dispatch"),
        1
    );
    tokio::task::yield_now().await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    assert_eq!(
        runtime
            .process_due_once(10)
            .await
            .expect("second scheduler pass should not redispatch claimed run"),
        0
    );
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
    assert_eq!(task.runs[0].status, TaskRunStatus::Starting);
    let execution = runtime
        .service()
        .store()
        .load_execution_for_run(task.runs[0].id.as_str())
        .await
        .expect("execution should load")
        .expect("execution should exist");
    assert_eq!(execution.status, TaskRunExecutionStatus::Starting);

    release.notify_waiters();
    timeout(Duration::from_secs(2), async {
        loop {
            let task = runtime
                .service()
                .get_task(pioneer_protocol::TaskGetParams {
                    task_id: response.task.id.clone(),
                })
                .await
                .expect("task should read");
            if task.runs[0].status == TaskRunStatus::Succeeded {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("agent task should complete");
}

#[tokio::test]
async fn running_agent_recovery_reuses_one_execution_and_one_child_lineage() {
    let runtime = runtime().await;
    let starts = Arc::new(AtomicUsize::new(0));
    let recoveries = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    runtime
        .register_executor(Arc::new(LineageRecordingAgentExecutor {
            starts: starts.clone(),
            recoveries: recoveries.clone(),
            release: release.clone(),
        }))
        .await;

    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    params.agent_spec = Some(agent_spec(2));
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("agent task should create");
    let run = response.run.expect("immediate run");

    timeout(Duration::from_secs(2), async {
        loop {
            if starts.load(Ordering::SeqCst) == 1
                && !runtime
                    .service()
                    .store()
                    .list_task_run_turns(run.id.as_str())
                    .await
                    .expect("task run turns should load")
                    .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("agent run should link one child");
    let initial_execution = runtime
        .service()
        .store()
        .load_execution_for_run(run.id.as_str())
        .await
        .expect("execution should load")
        .expect("execution should exist");
    runtime
        .service()
        .store()
        .heartbeat_execution(
            initial_execution.id.as_str(),
            run.created_at,
            Some(run.created_at.saturating_sub(1)),
        )
        .await
        .expect("execution lease should be made recoverable");

    runtime
        .start()
        .await
        .expect("runtime recovery should start");
    timeout(Duration::from_secs(2), async {
        loop {
            if recoveries.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("running run should be recovered once");

    let store = runtime.service().store();
    let execution = store
        .load_execution_for_run(run.id.as_str())
        .await
        .expect("execution should load")
        .expect("execution should exist");
    let binding = store
        .get_task_run_primary_thread_binding(run.id.as_str())
        .await
        .expect("binding should load")
        .expect("primary binding should exist");
    let turns = store
        .list_task_run_turns(run.id.as_str())
        .await
        .expect("task run turns should load");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(binding.execution_id.as_deref(), Some(execution.id.as_str()));
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].thread_id, binding.thread_id);
    let execution_count = store
        .count_task_run_executions_for_run(run.id.as_str())
        .await
        .expect("execution count query should work");
    assert_eq!(execution_count, 1);

    release.notify_waiters();
    timeout(Duration::from_secs(2), async {
        loop {
            let task = runtime
                .service()
                .get_task(pioneer_protocol::TaskGetParams {
                    task_id: response.task.id.clone(),
                })
                .await
                .expect("task should read");
            if task.runs[0].status == TaskRunStatus::Succeeded {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("agent run should complete after release");
}

#[tokio::test]
async fn starting_agent_recovery_reuses_reserved_execution_identity() {
    let runtime = runtime().await;
    let recoveries = Arc::new(AtomicUsize::new(0));

    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    params.agent_spec = Some(agent_spec(2));
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("agent task should create");
    let run = response.run.expect("immediate run");
    let store = runtime.service().store();
    let claimed = store
        .claim_task_run_for_dispatch(run.id.as_str(), run.created_at)
        .await
        .expect("run claim should work")
        .expect("run should claim");
    assert_eq!(claimed.status, TaskRunStatus::Starting);
    let reserved = store
        .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, run.created_at)
        .await
        .expect("execution should reserve");
    runtime
        .register_executor(Arc::new(LineageRecordingAgentExecutor {
            starts: Arc::new(AtomicUsize::new(0)),
            recoveries: recoveries.clone(),
            release: Arc::new(Notify::new()),
        }))
        .await;

    runtime
        .start()
        .await
        .expect("runtime recovery should start");
    timeout(Duration::from_secs(2), async {
        loop {
            if recoveries.load(Ordering::SeqCst) >= 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("starting run should be recovered once");

    let recovered = store
        .load_execution_for_run(run.id.as_str())
        .await
        .expect("execution should load")
        .expect("execution should exist");
    assert_eq!(recovered.id, reserved.id);
}

#[tokio::test]
async fn mark_started_is_idempotent_and_emits_one_started_event() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let run = response.run.expect("immediate run");
    let claimed = runtime
        .service()
        .store()
        .claim_task_run_for_dispatch(run.id.as_str(), run.created_at)
        .await
        .expect("run should claim")
        .expect("claim should apply");
    assert_eq!(claimed.status, TaskRunStatus::Starting);

    let handle = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    handle
        .mark_started(run.created_at)
        .await
        .expect("first mark_started should work");
    handle
        .mark_started(run.created_at)
        .await
        .expect("second mark_started should no-op");

    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: run.task_id.clone(),
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("events should read");
    let started_count = events
        .events
        .iter()
        .filter(|event| matches!(event.payload, TaskEventPayload::RunStarted { .. }))
        .count();
    assert_eq!(started_count, 1);
}

#[tokio::test]
async fn concurrent_execution_reservations_reuse_one_child_identity() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    params.agent_spec = Some(agent_spec(2));
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("agent task should create");
    let run = response.run.expect("immediate run");
    let store = runtime.service().store();

    let (left, right) = tokio::join!(
        store.reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, run.created_at),
        store.reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, run.created_at),
    );
    let left = left.expect("left reservation should succeed");
    let right = right.expect("right reservation should succeed");

    assert_eq!(left.id, right.id);
    assert_eq!(left.task_run_id, run.id);
    assert_eq!(right.task_run_id, run.id);
    assert_eq!(left.status, TaskRunExecutionStatus::Reserved);

    let loaded = store
        .load_execution_for_run(run.id.as_str())
        .await
        .expect("execution should load")
        .expect("execution should exist");
    assert_eq!(loaded.id, left.id);

    let count = store
        .count_task_run_executions_for_run(run.id.as_str())
        .await
        .expect("count query should work");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn execution_repository_tracks_claim_running_heartbeat_and_terminal_state() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let run = response.run.expect("immediate run");
    let store = runtime.service().store();
    let execution = store
        .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::System, run.created_at)
        .await
        .expect("execution should reserve");

    assert_eq!(execution.status, TaskRunExecutionStatus::Reserved);

    let claimed = store
        .claim_execution(execution.id.as_str(), "worker_1", run.created_at + 60)
        .await
        .expect("execution should claim")
        .expect("execution should still exist");
    assert_eq!(claimed.status, TaskRunExecutionStatus::Starting);
    assert_eq!(claimed.worker_id.as_deref(), Some("worker_1"));
    assert_eq!(claimed.lease_until, Some(run.created_at + 60));

    let running = store
        .mark_execution_running(
            execution.id.as_str(),
            run.created_at + 1,
            Some(run.created_at + 90),
        )
        .await
        .expect("execution should mark running")
        .expect("execution should still exist");
    assert_eq!(running.status, TaskRunExecutionStatus::Running);
    assert_eq!(running.started_at, Some(run.created_at + 1));
    assert_eq!(running.heartbeat_at, Some(run.created_at + 1));
    assert_eq!(running.lease_until, Some(run.created_at + 90));

    let heartbeat = store
        .heartbeat_execution(
            execution.id.as_str(),
            run.created_at + 2,
            Some(run.created_at + 120),
        )
        .await
        .expect("execution should heartbeat")
        .expect("execution should still exist");
    assert_eq!(heartbeat.heartbeat_at, Some(run.created_at + 2));
    assert_eq!(heartbeat.lease_until, Some(run.created_at + 120));

    let result = TaskResult {
        summary: Some("done".to_owned()),
        data: Some(TaskValue::String("ok".to_owned())),
        artifacts: Vec::new(),
        completed_by_run_id: Some(run.id.clone()),
    };
    let terminal = store
        .mark_execution_terminal(
            execution.id.as_str(),
            TaskRunExecutionStatus::Succeeded,
            run.created_at + 3,
            Some(&result),
            None,
        )
        .await
        .expect("execution should mark terminal")
        .expect("execution should still exist");
    assert_eq!(terminal.status, TaskRunExecutionStatus::Succeeded);
    assert_eq!(terminal.completed_at, Some(run.created_at + 3));
    assert_eq!(terminal.lease_until, None);
    assert_eq!(terminal.result, Some(result));
}

#[tokio::test]
async fn one_task_run_can_link_only_one_child_thread() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    params.agent_spec = Some(agent_spec(2));
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("task should create");
    let run = response.run.expect("immediate run");
    let execution = runtime
        .service()
        .store()
        .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, run.created_at)
        .await
        .expect("execution should reserve");
    let handle = TaskExecutionHandle::new(
        runtime.service().store(),
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    let parent_thread_id = pioneer_protocol::generate_id(21);
    let parent_turn_id = pioneer_protocol::generate_id(21);
    let root_thread_id = parent_thread_id.clone();
    let child_thread_id = pioneer_protocol::generate_id(21);
    let child_turn_id = pioneer_protocol::generate_id(21);
    let first_lineage = TaskThreadLineage {
        child_thread_id: child_thread_id.clone(),
        parent_thread_id: parent_thread_id.clone(),
        root_thread_id: root_thread_id.clone(),
        depth: 1,
        origin_kind: Some("task_run".to_owned()),
        created_by_thread_id: Some(parent_thread_id.clone()),
        created_by_turn_id: Some(parent_turn_id.clone()),
        created_at: run.created_at,
    };
    let first_binding = TaskRunThreadBinding {
        id: "binding_first".to_owned(),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id.clone()),
        thread_id: child_thread_id.clone(),
        binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
        created_at: run.created_at,
    };
    let first_turn = TaskRunTurn {
        id: "turn_first".to_owned(),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id.clone()),
        thread_id: child_thread_id,
        turn_id: child_turn_id,
        kind: TaskRunTurnKind::Initial,
        round: 0,
        sequence: 0,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at: run.created_at,
        started_at: Some(run.created_at),
        completed_at: None,
    };
    handle
        .link_child_thread_with_runtime(first_lineage, first_binding, first_turn, run.created_at)
        .await
        .expect("first lineage should link");

    let duplicate_child_thread_id = pioneer_protocol::generate_id(21);
    let duplicate_child_turn_id = pioneer_protocol::generate_id(21);
    let duplicate_lineage = TaskThreadLineage {
        child_thread_id: duplicate_child_thread_id.clone(),
        parent_thread_id: parent_thread_id.clone(),
        root_thread_id,
        depth: 1,
        origin_kind: Some("task_run".to_owned()),
        created_by_thread_id: Some(parent_thread_id),
        created_by_turn_id: Some(parent_turn_id),
        created_at: run.created_at,
    };
    let duplicate_binding = TaskRunThreadBinding {
        id: "binding_duplicate".to_owned(),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id.clone()),
        thread_id: duplicate_child_thread_id.clone(),
        binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
        created_at: run.created_at,
    };
    let duplicate_turn = TaskRunTurn {
        id: "turn_duplicate".to_owned(),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id),
        thread_id: duplicate_child_thread_id,
        turn_id: duplicate_child_turn_id,
        kind: TaskRunTurnKind::Initial,
        round: 0,
        sequence: 0,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at: run.created_at,
        started_at: Some(run.created_at),
        completed_at: None,
    };
    let error = handle
        .link_child_thread_with_runtime(
            duplicate_lineage,
            duplicate_binding,
            duplicate_turn,
            run.created_at,
        )
        .await
        .expect_err("second lineage for same run must fail");
    assert!(
        format!("{error:#}").contains("thread lineage")
            || format!("{error:#}").contains("UNIQUE")
            || format!("{error:#}").contains("constraint failed")
            || format!("{error:#}").contains("already exists")
            || format!("{error:#}").contains("reserved task run execution")
    );
}

#[tokio::test]
async fn scheduled_create_does_not_fire_before_due_time() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");

    assert_eq!(response.task.status, TaskStatus::Scheduled);
    assert!(response.run.is_none());
    let created = runtime
        .process_due_once(3_999_999_999)
        .await
        .expect("scheduler should process");
    assert_eq!(created, 0);
}

#[tokio::test]
async fn scheduled_trigger_fires_when_due() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 10,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");

    let created = runtime
        .process_due_once(10)
        .await
        .expect("scheduler should fire due trigger");
    assert_eq!(created, 1);
    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
}

#[tokio::test]
async fn due_trigger_materialization_rolls_back_run_when_occurrence_contract_is_invalid() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 10,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");
    let task_id = response.task.id.clone();
    let trigger = response.trigger;
    let run = TaskRun {
        id: pioneer_protocol::generate_id(21),
        task_id: task_id.clone(),
        trigger_id: Some(trigger.id.clone()),
        parent_run_id: None,
        run_group_id: pioneer_protocol::generate_id(21),
        attempt_number: 1,
        retry_of_run_id: None,
        ready_at: Some(10),
        run_number: 1,
        status: TaskRunStatus::Queued,
        executor_kind: response.task.executor_kind,
        started_at: None,
        completed_at: None,
        heartbeat_at: None,
        locked_by: None,
        lock_expires_at: None,
        result: None,
        error: None,
        created_at: 10,
        updated_at: 10,
    };
    let invalid_occurrence = TaskOccurrenceContract {
        occurrence_id: String::new(),
        task_id: task_id.clone(),
        run_id: run.id.clone(),
        trigger_id: Some(trigger.id.clone()),
        occurrence_key: format!("{}:1", trigger.id),
        execution_generation: 1,
        agent_execution_id: None,
        work_graph_root_execution_id: None,
        root_resource_scope_id: None,
        status: TaskOccurrenceStatus::Queued,
        queue_position: None,
        retry_attempt: 0,
        action_idempotency_key: format!("task:{task_id}:{}", run.id),
        route_id: None,
        result_return_route_id: None,
        terminal_reason: None,
    };

    let error = runtime
        .service()
        .store()
        .append_due_trigger_task_events(
            trigger.id.as_str(),
            10,
            10,
            vec![
                TaskEventPayload::TaskQueued {
                    task_id: task_id.clone(),
                    run_id: Some(run.id.clone()),
                },
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: None,
                },
            ],
            vec![invalid_occurrence],
            vec![(run.id, run.executor_kind)],
        )
        .await
        .expect_err("invalid occurrence must abort the due-trigger transaction");
    assert!(format!("{error:#}").contains("invalid task occurrence contract"));

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams { task_id })
        .await
        .expect("task should remain readable after rollback");
    assert!(task.runs.is_empty());
    assert_eq!(task.triggers[0].next_fire_at, Some(10));
    assert_eq!(task.triggers[0].status, TaskTriggerStatus::Active);
}

#[tokio::test]
async fn interval_trigger_recomputes_next_fire_at() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Interval {
                interval_seconds: 10,
                interval_anchor_at: Some(4_000_000_000),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("interval task should create");

    let created = runtime
        .process_due_once(4_000_000_000)
        .await
        .expect("scheduler should fire interval trigger");
    assert_eq!(created, 1);
    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
    assert!(
        task.triggers[0]
            .next_fire_at
            .is_some_and(|value| value > 4_000_000_000)
    );
}

#[tokio::test]
async fn interval_task_fires_repeatedly_after_terminal_runs_and_stays_active() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Interval {
                interval_seconds: 10,
                interval_anchor_at: Some(4_000_000_000),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("interval task should create");

    assert_eq!(
        runtime
            .process_due_once(4_000_000_000)
            .await
            .expect("first interval fire should succeed"),
        1
    );
    assert_eq!(
        runtime
            .process_due_once(4_000_000_010)
            .await
            .expect("second interval fire should succeed"),
        1
    );

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 2);
    assert_eq!(task.task.status, TaskStatus::Scheduled);
    assert_eq!(task.triggers[0].status, TaskTriggerStatus::Active);
    assert!(
        task.triggers[0]
            .next_fire_at
            .is_some_and(|next| next > 4_000_000_010)
    );
}

#[tokio::test]
async fn recurring_due_trigger_with_active_serial_run_skips_fire_and_moves_next_fire_forward() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Interval {
        interval_seconds: 10,
        interval_anchor_at: Some(4_000_000_000),
        catch_up_policy: None,
    });
    params.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 1,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("interval task should create");

    assert_eq!(
        runtime
            .process_due_once(4_000_000_000)
            .await
            .expect("first interval fire should create one run"),
        1
    );
    let after_first = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read after first fire");
    assert_eq!(after_first.runs.len(), 1);
    assert_eq!(after_first.task.status, TaskStatus::Queued);
    assert_eq!(after_first.triggers[0].next_fire_at, Some(4_000_000_010));

    assert_eq!(
        runtime
            .process_due_once(4_000_000_010)
            .await
            .expect("overlapped interval fire should be skipped"),
        0
    );
    let after_overlap = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read after overlap skip");
    assert_eq!(after_overlap.runs.len(), 1);
    assert_eq!(after_overlap.task.status, TaskStatus::Queued);
    assert_eq!(after_overlap.triggers[0].status, TaskTriggerStatus::Active);
    assert_eq!(after_overlap.triggers[0].next_fire_at, Some(4_000_000_020));

    assert_eq!(
        runtime
            .process_due_once(4_000_000_010)
            .await
            .expect("repeated scheduler pass before next fire should stay idle"),
        0
    );
    let after_repeat = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read after repeated pass");
    assert_eq!(after_repeat.runs.len(), 1);
    assert_eq!(after_repeat.triggers[0].next_fire_at, Some(4_000_000_020));
}

#[tokio::test]
async fn daily_cron_catches_up_latest_missed_once_and_advances_to_next_day() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Cron {
                cron_expr: "0 7 * * *".to_owned(),
                timezone: "Europe/Moscow".to_owned(),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("cron task should create");
    let first_fire = response
        .trigger
        .next_fire_at
        .expect("cron should have initial fire");
    let missed_by_two_hours = first_fire + 2 * 60 * 60;

    assert_eq!(
        runtime
            .process_due_once(missed_by_two_hours)
            .await
            .expect("missed daily cron should catch up once"),
        1
    );

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
    assert_eq!(task.triggers[0].last_fire_at, Some(first_fire));
    assert_eq!(
        task.triggers[0].next_fire_at,
        Some(first_fire + 24 * 60 * 60)
    );

    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("events should read");
    assert!(events.events.iter().any(|event| {
        matches!(
            &event.payload,
            TaskEventPayload::TaskRescheduled {
                reason: TaskRescheduleReason::TriggerFired,
                trigger,
                ..
            } if trigger.last_fire_at == Some(first_fire)
        )
    }));
}

#[tokio::test]
async fn skip_missed_cron_advances_trigger_without_creating_run() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Cron {
                cron_expr: "0 7 * * *".to_owned(),
                timezone: "Europe/Moscow".to_owned(),
                catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
            }),
        )
        .await
        .expect("cron task should create");
    let first_fire = response
        .trigger
        .next_fire_at
        .expect("cron should have initial fire");
    let missed_by_two_hours = first_fire + 2 * 60 * 60;

    assert_eq!(
        runtime
            .process_due_once(missed_by_two_hours)
            .await
            .expect("missed cron should skip without creating a run"),
        0
    );

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read");
    assert!(task.runs.is_empty());
    assert_eq!(task.triggers[0].last_fire_at, Some(first_fire));
    assert_eq!(
        task.triggers[0].next_fire_at,
        Some(first_fire + 24 * 60 * 60)
    );

    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("events should read");
    assert!(events.events.iter().any(|event| {
        matches!(
            &event.payload,
            TaskEventPayload::TaskRescheduled {
                reason: TaskRescheduleReason::MissedFireSkipped,
                trigger,
                ..
            } if trigger.last_fire_at == Some(first_fire)
        )
    }));
}

#[tokio::test]
async fn default_interval_catch_up_computes_latest_missed_without_scan_limit_failure() {
    let runtime = runtime().await;
    let anchor = 4_000_000_000;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Interval {
                interval_seconds: 15,
                interval_anchor_at: Some(anchor),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("interval task should create");
    let latest_missed = anchor + 15 * 20_000;

    assert_eq!(
        runtime
            .process_due_once(latest_missed + 5)
            .await
            .expect("long-missed interval should catch up arithmetically"),
        1
    );

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
    assert_eq!(task.triggers[0].last_fire_at, Some(latest_missed));
    assert_eq!(task.triggers[0].next_fire_at, Some(latest_missed + 15));
}

#[tokio::test]
async fn run_all_missed_interval_respects_batch_limit_and_active_run_slots() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Interval {
        interval_seconds: 10,
        interval_anchor_at: Some(4_000_000_000),
        catch_up_policy: Some(TaskTriggerCatchUpPolicy::run_all_missed(4)),
    });
    params.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 2,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("interval task should create");

    assert_eq!(
        runtime
            .process_due_once(4_000_000_035)
            .await
            .expect("scheduler should create only available run slots"),
        2
    );
    let after_first_batch = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read");
    assert_eq!(after_first_batch.runs.len(), 2);
    assert_eq!(
        after_first_batch.triggers[0].last_fire_at,
        Some(4_000_000_010)
    );
    assert_eq!(
        after_first_batch.triggers[0].next_fire_at,
        Some(4_000_000_020)
    );

    assert_eq!(
        runtime
            .process_due_once(4_000_000_035)
            .await
            .expect("active runs should prevent another parallel batch"),
        0
    );
    let after_overlap = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read after overlap");
    assert_eq!(after_overlap.runs.len(), 2);
    assert_eq!(after_overlap.triggers[0].next_fire_at, Some(4_000_000_040));
}

#[tokio::test]
async fn failed_run_schedules_retry_and_defers_terminal_delivery_until_exhausted() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(FailingSystemExecutor))
        .await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_retry_owner".to_owned());
    params.created_by_thread_id = Some("thr_retry_owner".to_owned());
    params.retry_policy = Some(TaskRetryPolicy {
        max_attempts: 2,
        backoff: TaskRetryBackoffKind::Fixed,
        initial_delay_seconds: Some(5),
        max_delay_seconds: None,
        retry_on: vec![TaskErrorClass::Internal],
    });
    params.delivery_policy = Some(TaskDeliveryPolicy {
        mode: TaskDeliveryMode::Thread,
        thread_target: Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread),
        thread_id: Some("thr_retry_owner".to_owned()),
        webhook_url: None,
        include_result: true,
        format: pioneer_protocol::TaskDeliveryFormat::Summary,
    });

    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("retry task should create");

    let after_first = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read after first failure");
    assert_eq!(after_first.task.status, TaskStatus::Queued);
    assert_eq!(after_first.runs.len(), 2);
    let first_run = after_first
        .runs
        .iter()
        .find(|run| run.attempt_number == 1)
        .expect("first run should exist");
    let retry_run = after_first
        .runs
        .iter()
        .find(|run| run.attempt_number == 2)
        .expect("retry run should exist");
    assert_eq!(retry_run.status, TaskRunStatus::Queued);
    assert_eq!(
        retry_run.retry_of_run_id.as_deref(),
        Some(first_run.id.as_str())
    );
    let retry_ready_at = retry_run.ready_at.expect("retry should have ready time");
    assert_eq!(retry_ready_at, first_run.created_at.saturating_add(5));

    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id.clone()),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read after first failure");
    assert!(deliveries.deliveries.is_empty());

    assert_eq!(
        runtime
            .process_due_once(retry_ready_at)
            .await
            .expect("retry attempt should dispatch"),
        1
    );
    let after_second = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read after retry exhaustion");
    assert_eq!(after_second.task.status, TaskStatus::Failed);
    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id.clone(),
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("task events should read");
    assert!(
        events
            .events
            .iter()
            .any(|event| matches!(event.payload, TaskEventPayload::RunRetryExhausted { .. }))
    );

    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read after exhaustion");
    assert_eq!(deliveries.deliveries.len(), 1);
}

#[tokio::test]
async fn write_locks_block_conflicting_agent_runs_and_recover_release_terminal_locks() {
    let runtime = runtime().await;

    let mut first = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut first);
    let mut first_spec = agent_spec(3);
    first_spec.tool_policy = Some(TaskAgentToolPolicy {
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
        write_mode: TaskAgentWriteMode::ScopedWrite,
        allowed_paths: vec!["src".to_owned()],
        network_access: false,
    });
    first.agent_spec = Some(first_spec);
    first.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 1,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let first = runtime
        .service()
        .create_task(task_create_context_for(&first), first)
        .await
        .expect("first agent task should create");
    let first_run_id = first.run.expect("first run should exist").id;

    let mut second = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut second);
    let mut second_spec = agent_spec(3);
    second_spec.tool_policy = Some(TaskAgentToolPolicy {
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
        write_mode: TaskAgentWriteMode::ScopedWrite,
        allowed_paths: vec!["src/lib.rs".to_owned()],
        network_access: false,
    });
    second.agent_spec = Some(second_spec);
    second.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 1,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let second = runtime
        .service()
        .create_task(task_create_context_for(&second), second)
        .await
        .expect("second agent task should create");
    let second_run_id = second.run.expect("second run should exist").id;

    let first_decision = runtime
        .service()
        .acquire_write_locks_for_run(first_run_id.as_str(), 100)
        .await
        .expect("first run should acquire lock");
    assert!(matches!(first_decision, WriteLockDecision::Acquired(_)));

    let second_decision = runtime
        .service()
        .acquire_write_locks_for_run(second_run_id.as_str(), 101)
        .await
        .expect("second run should queue on conflict");
    assert!(matches!(second_decision, WriteLockDecision::Queued));

    let second_events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: second.task.id.clone(),
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("second task events should read");
    assert!(second_events.events.iter().any(|event| {
        matches!(
            event.payload,
            TaskEventPayload::WriteLockBlocked { ref conflicts, .. } if conflicts.len() == 1
        )
    }));

    let completed = runtime
        .service()
        .append_event(
            TaskEventPayload::RunCompleted {
                task_id: first.task.id.clone(),
                run_id: first_run_id.clone(),
                result: None,
                completed_at: 200,
            },
            200,
        )
        .await
        .expect("first run completion should append");
    runtime.service().publish_and_wake(vec![completed]).await;
    runtime
        .service()
        .recover_retry_and_lock_state(201)
        .await
        .expect("lock recovery should release terminal run lock");

    let second_decision = runtime
        .service()
        .acquire_write_locks_for_run(second_run_id.as_str(), 202)
        .await
        .expect("second run should acquire after release");
    assert!(matches!(second_decision, WriteLockDecision::Acquired(_)));
}

#[tokio::test]
async fn waiting_review_recovery_preserves_stale_write_lock() {
    let runtime = runtime().await;

    let mut first = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut first);
    let mut first_spec = agent_spec(3);
    first_spec.tool_policy = Some(TaskAgentToolPolicy {
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
        write_mode: TaskAgentWriteMode::ScopedWrite,
        allowed_paths: vec!["src".to_owned()],
        network_access: false,
    });
    first.agent_spec = Some(first_spec);
    first.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 1,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let first = runtime
        .service()
        .create_task(task_create_context_for(&first), first)
        .await
        .expect("first agent task should create");
    let first_run_id = first.run.expect("first run should exist").id;

    let mut second = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut second);
    let mut second_spec = agent_spec(3);
    second_spec.tool_policy = Some(TaskAgentToolPolicy {
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
        write_mode: TaskAgentWriteMode::ScopedWrite,
        allowed_paths: vec!["src/lib.rs".to_owned()],
        network_access: false,
    });
    second.agent_spec = Some(second_spec);
    second.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 1,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let second = runtime
        .service()
        .create_task(task_create_context_for(&second), second)
        .await
        .expect("second agent task should create");
    let second_run_id = second.run.expect("second run should exist").id;

    let first_decision = runtime
        .service()
        .acquire_write_locks_for_run(first_run_id.as_str(), 100)
        .await
        .expect("first run should acquire lock");
    assert!(matches!(first_decision, WriteLockDecision::Acquired(_)));

    let entered_review = runtime
        .service()
        .append_event(
            TaskEventPayload::TaskRunEnteredReview {
                task_id: first.task.id.clone(),
                run_id: first_run_id.clone(),
                candidate_id: "candidate_waiting_review_lock".to_owned(),
                entered_at: 200,
            },
            200,
        )
        .await
        .expect("waiting review event should append");
    runtime
        .service()
        .publish_and_wake(vec![entered_review])
        .await;

    let recovered = runtime
        .service()
        .recover_retry_and_lock_state(3_701)
        .await
        .expect("lock recovery should preserve waiting review lock");
    assert_eq!(recovered, 1);

    let locks = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: first.task.id.clone(),
        })
        .await
        .expect("task should read")
        .write_locks;
    assert_eq!(locks.len(), 1);
    assert_eq!(
        locks[0].status,
        pioneer_protocol::TaskWriteLockStatus::Acquired
    );
    assert_eq!(locks[0].expires_at, None);

    let second_decision = runtime
        .service()
        .acquire_write_locks_for_run(second_run_id.as_str(), 3_702)
        .await
        .expect("second run should still queue while first waits for review");
    assert!(matches!(second_decision, WriteLockDecision::Queued));

    let first_events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: first.task.id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("first task events should read");
    assert!(first_events.events.iter().any(|event| {
        matches!(
            event.payload,
            TaskEventPayload::WriteLockExtended { ref lock, .. }
                if lock.run_id == first_run_id
        )
    }));
    assert!(!first_events.events.iter().any(|event| {
        matches!(
            event.payload,
            TaskEventPayload::WriteLockExpired { ref lock, .. }
                if lock.run_id == first_run_id
        )
    }));
}

#[tokio::test]
async fn pause_excludes_due_trigger_and_resume_restores_future_fire() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");

    let paused = runtime
        .service()
        .pause_task(
            TaskMutationContext::default(),
            TaskPauseParams {
                task_id: response.task.id.clone(),
                reason: Some("hold".to_owned()),
            },
        )
        .await
        .expect("pause should succeed");
    assert_eq!(paused.triggers[0].status, TaskTriggerStatus::Paused);
    assert_eq!(
        runtime
            .process_due_once(4_000_000_000)
            .await
            .expect("paused due scan should succeed"),
        0
    );

    let resumed = runtime
        .service()
        .resume_task(
            TaskMutationContext::default(),
            TaskResumeParams {
                task_id: response.task.id.clone(),
                reason: Some("continue".to_owned()),
            },
        )
        .await
        .expect("resume should succeed");
    assert_eq!(resumed.triggers[0].status, TaskTriggerStatus::Active);
    assert_eq!(resumed.triggers[0].next_fire_at, Some(4_000_000_000));
}

#[tokio::test]
async fn scheduler_reconciles_legacy_due_trigger_for_blocked_task_once() {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite memory database should connect");
    Migrator::up(&connection, None)
        .await
        .expect("migration should apply");
    let store = Arc::new(CrudStore::new(connection));
    let runtime = TaskRuntime::new(store.clone());
    let fire_at = 4_000_000_000;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: fire_at,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskBlocked {
                task_id: response.task.id.clone(),
                error: None,
                blocked_at: fire_at - 2,
            },
            fire_at - 2,
        )
        .await
        .expect("task should block");

    // Recreate the pre-fix production state: terminal Task with an overdue,
    // still-active trigger. The terminal guard keeps the Task blocked while
    // the trigger projection becomes schedulable again.
    let mut stale_trigger = response.trigger;
    stale_trigger.status = TaskTriggerStatus::Active;
    stale_trigger.next_fire_at = Some(fire_at);
    stale_trigger.updated_at = fire_at - 1;
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskRescheduled {
                task_id: response.task.id.clone(),
                trigger: stale_trigger,
                rescheduled_at: fire_at - 1,
                reason: TaskRescheduleReason::UserRequested,
            },
            fire_at - 1,
        )
        .await
        .expect("legacy inconsistent trigger should materialize");
    assert_eq!(
        store
            .list_due_active_task_triggers(fire_at)
            .await
            .expect("due triggers should list")
            .len(),
        1
    );

    assert_eq!(
        runtime
            .process_due_once(fire_at)
            .await
            .expect("scheduler reconciliation should succeed"),
        0
    );
    let reconciled = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("reconciled task should read");
    assert_eq!(reconciled.task.status, TaskStatus::Blocked);
    assert!(reconciled.runs.is_empty());
    assert_eq!(reconciled.triggers[0].status, TaskTriggerStatus::Paused);
    assert_eq!(reconciled.triggers[0].next_fire_at, None);
    assert!(
        store
            .list_due_active_task_triggers(fire_at)
            .await
            .expect("due triggers should list after reconciliation")
            .is_empty()
    );
    assert_eq!(
        runtime
            .process_due_once(fire_at)
            .await
            .expect("second scheduler pass should remain idle"),
        0
    );
    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("task events should read");
    assert_eq!(
        events
            .events
            .iter()
            .filter(|event| matches!(
                event.payload,
                TaskEventPayload::TaskRescheduled {
                    reason: TaskRescheduleReason::RunTerminalStatusRefresh,
                    ..
                }
            ))
            .count(),
        1
    );
}

#[tokio::test]
async fn blocked_task_can_be_cancelled_and_closes_paused_trigger() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskBlocked {
                task_id: response.task.id.clone(),
                error: None,
                blocked_at: 1_700_000_001,
            },
            1_700_000_001,
        )
        .await
        .expect("task should block");

    let cancelled = runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: response.task.id.clone(),
                reason: Some("close blocked work".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::TaskOnly,
            },
        )
        .await
        .expect("blocked task should cancel");
    assert_eq!(cancelled.task.status, TaskStatus::Cancelled);
    let persisted = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("cancelled task should read");
    assert_eq!(persisted.task.status, TaskStatus::Cancelled);
    assert_eq!(persisted.triggers[0].status, TaskTriggerStatus::Cancelled);
    assert_eq!(persisted.triggers[0].next_fire_at, None);
}

#[tokio::test]
async fn blocked_agent_task_requires_readmission_and_resumes_atomically() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Cron {
        cron_expr: "0 5 * * *".to_owned(),
        timezone: "Europe/Moscow".to_owned(),
        catch_up_policy: None,
    });
    configure_agent_task(&mut params);
    let mut spec = agent_spec(3);
    spec.prompt.instructions = vec![
        "Use currently available runtime capabilities by capability.".to_owned(),
        "Fail clearly when required data is unavailable.".to_owned(),
    ];
    spec.prompt.output_instructions =
        Some("Return concise markdown or a clear failure reason.".to_owned());
    params.agent_spec = Some(spec);
    let create_context = task_create_context_for(&params);
    let execution_admission = create_context
        .execution_admission
        .clone()
        .expect("agent task should have admission");
    let response = runtime
        .service()
        .create_task(create_context, params)
        .await
        .expect("scheduled Agent task should create");
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskBlocked {
                task_id: response.task.id.clone(),
                error: Some(TaskError {
                    code: "authorization_missing".to_owned(),
                    message: "execution admission is missing".to_owned(),
                    class: TaskErrorClass::Policy,
                    details: None,
                    failed_run_id: None,
                }),
                blocked_at: 1_700_000_001,
            },
            1_700_000_001,
        )
        .await
        .expect("task should block");

    let error = runtime
        .service()
        .resume_task(
            TaskMutationContext::default(),
            TaskResumeParams {
                task_id: response.task.id.clone(),
                reason: Some("retry without authority".to_owned()),
            },
        )
        .await
        .expect_err("blocked Agent task must fail closed without readmission");
    assert!(error.to_string().contains("authorization readmission"));

    let mut context = TaskMutationContext::default();
    context.execution_admission = Some(execution_admission);
    let resumed = runtime
        .service()
        .resume_task(
            context,
            TaskResumeParams {
                task_id: response.task.id,
                reason: Some("permissions confirmed".to_owned()),
            },
        )
        .await
        .expect("blocked Agent task should resume with readmission");
    assert_eq!(resumed.task.status, TaskStatus::Scheduled);
    assert_eq!(resumed.task.error, None);
    assert_eq!(resumed.triggers[0].status, TaskTriggerStatus::Active);
    assert!(resumed.triggers[0].next_fire_at.is_some());
}

#[tokio::test]
async fn cron_trigger_can_fire_repeatedly_with_timezone() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Cron {
                cron_expr: "0 9 * * *".to_owned(),
                timezone: "UTC".to_owned(),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("cron task should create");
    let first_fire = response
        .trigger
        .next_fire_at
        .expect("cron should have initial fire");
    assert_eq!(
        runtime
            .process_due_once(first_fire)
            .await
            .expect("first cron fire should succeed"),
        1
    );
    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read");
    let second_fire = task.triggers[0]
        .next_fire_at
        .expect("cron should advance after terminal run");
    assert!(second_fire > first_fire);
    assert_eq!(
        runtime
            .process_due_once(second_fire)
            .await
            .expect("second cron fire should succeed"),
        1
    );
}

#[tokio::test]
async fn terminal_run_enqueues_origin_thread_delivery_from_normalized_result() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let mut params = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
        catch_up_policy: None,
    });
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_owner".to_owned());
    params.created_by_thread_id = Some("thr_owner".to_owned());

    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("scheduled task should create");
    let policy = response
        .task
        .delivery_policy
        .as_ref()
        .expect("scheduled task should persist delivery policy");
    assert_eq!(policy.mode, TaskDeliveryMode::Thread);
    assert_eq!(
        policy.thread_target,
        Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread)
    );
    assert_eq!(policy.thread_id.as_deref(), Some("thr_owner"));
    runtime
        .process_due_once(4_000_000_000)
        .await
        .expect("scheduled task should fire");

    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read");
    assert_eq!(deliveries.deliveries.len(), 1);
    let delivery = &deliveries.deliveries[0];
    assert_eq!(delivery.mode, TaskDeliveryMode::Thread);
    assert_eq!(
        delivery.thread_target,
        Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread)
    );
    assert_eq!(delivery.status, TaskDeliveryStatus::Pending);
    assert_eq!(delivery.target_thread_id.as_deref(), Some("thr_owner"));
    assert_eq!(
        delivery
            .result_snapshot
            .as_ref()
            .and_then(|result| result.summary.as_deref()),
        Some("completed run 1")
    );
}

#[tokio::test]
async fn immediate_detached_thread_task_defaults_to_origin_thread_delivery() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_background_owner".to_owned());
    params.created_by_thread_id = Some("thr_background_owner".to_owned());
    params.created_by_turn_id = Some("turn_background_creator".to_owned());
    params.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Detached,
        on_parent_cancel: TaskParentTerminalAction::KeepRunning,
        on_parent_failure: TaskParentTerminalAction::KeepRunning,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });

    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("immediate detached task should create and run");

    assert_eq!(
        response
            .task
            .delivery_policy
            .as_ref()
            .map(|policy| policy.mode),
        Some(TaskDeliveryMode::Thread)
    );
    assert_eq!(
        response
            .task
            .delivery_policy
            .as_ref()
            .and_then(|policy| policy.thread_target),
        Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread)
    );
    assert_eq!(
        response
            .task
            .delivery_policy
            .as_ref()
            .and_then(|policy| policy.thread_id.as_deref()),
        Some("thr_background_owner")
    );
    assert_eq!(
        response
            .task
            .lifecycle_policy
            .as_ref()
            .map(|policy| policy.attachment),
        Some(TaskAttachmentMode::Detached)
    );
    assert!(
        response.run.is_some(),
        "immediate background task should create a run during admission"
    );

    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read");
    assert_eq!(deliveries.deliveries.len(), 1);
    let delivery = &deliveries.deliveries[0];
    assert_eq!(delivery.mode, TaskDeliveryMode::Thread);
    assert_eq!(
        delivery.thread_target,
        Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread)
    );
    assert_eq!(delivery.status, TaskDeliveryStatus::Pending);
    assert_eq!(
        delivery.target_thread_id.as_deref(),
        Some("thr_background_owner")
    );
    assert_eq!(
        delivery
            .result_snapshot
            .as_ref()
            .and_then(|result| result.summary.as_deref()),
        Some("completed run 1")
    );
}

#[tokio::test]
async fn immediate_attached_thread_task_keeps_no_delivery_default() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_attached_owner".to_owned());
    params.created_by_thread_id = Some("thr_attached_owner".to_owned());
    params.created_by_turn_id = Some("turn_attached_creator".to_owned());

    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("immediate attached task should create and run");

    assert_eq!(
        response
            .task
            .lifecycle_policy
            .as_ref()
            .map(|policy| policy.attachment),
        Some(TaskAttachmentMode::Attached)
    );
    assert_eq!(
        response
            .task
            .delivery_policy
            .as_ref()
            .map(|policy| policy.mode),
        Some(TaskDeliveryMode::None),
        "attached subagent result remains owned by the joining parent turn"
    );
    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read");
    assert!(deliveries.deliveries.is_empty());
}

#[tokio::test]
async fn cancel_task_cancels_pending_deliveries() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let mut params = create_params(TaskTriggerSpec::Interval {
        interval_seconds: 10,
        interval_anchor_at: Some(4_000_000_000),
        catch_up_policy: None,
    });
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_owner".to_owned());
    params.created_by_thread_id = Some("thr_owner".to_owned());
    params.delivery_policy = Some(TaskDeliveryPolicy {
        mode: TaskDeliveryMode::Thread,
        thread_target: Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread),
        thread_id: Some("thr_owner".to_owned()),
        webhook_url: None,
        include_result: true,
        format: pioneer_protocol::TaskDeliveryFormat::Summary,
    });
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("interval task should create");
    runtime
        .process_due_once(4_000_000_000)
        .await
        .expect("interval task should fire");

    runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: response.task.id.clone(),
                reason: Some("stop scheduled work".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("cancel should succeed");

    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read");
    assert_eq!(
        deliveries.deliveries[0].status,
        TaskDeliveryStatus::Cancelled
    );
}

#[tokio::test]
async fn invalid_cron_timezone_is_rejected() {
    let runtime = runtime().await;
    let error = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Cron {
                cron_expr: "0 8 * * *".to_owned(),
                timezone: "Not/AZone".to_owned(),
                catch_up_policy: None,
            }),
        )
        .await
        .expect_err("invalid timezone should fail");
    assert!(format!("{error:#}").contains("invalid timezone"));
}

#[tokio::test]
async fn invalid_interval_is_rejected() {
    let runtime = runtime().await;
    let error = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Interval {
                interval_seconds: 0,
                interval_anchor_at: None,
                catch_up_policy: None,
            }),
        )
        .await
        .expect_err("invalid interval should fail");
    assert!(format!("{error:#}").contains("must be at least 10 seconds"));
}

#[tokio::test]
async fn cron_trigger_computes_next_fire_in_timezone() {
    let next = crate::TaskTriggerCalculator::initial_next_fire_at(
        &TaskTriggerSpec::Cron {
            cron_expr: "0 9 * * *".to_owned(),
            timezone: "UTC".to_owned(),
            catch_up_policy: None,
        },
        1_700_000_000,
    )
    .expect("cron should compute")
    .expect("cron should have next fire");
    assert!(next > 1_700_000_000);
}

#[tokio::test]
async fn cron_trigger_computes_moscow_morning_fire_in_utc() {
    let next = crate::TaskTriggerCalculator::initial_next_fire_at(
        &TaskTriggerSpec::Cron {
            cron_expr: "0 7 * * *".to_owned(),
            timezone: "Europe/Moscow".to_owned(),
            catch_up_policy: None,
        },
        1_778_752_618,
    )
    .expect("cron should compute")
    .expect("cron should have next fire");
    assert_eq!(next, 1_778_817_600);
}

#[tokio::test]
async fn agent_task_create_requires_permission_cap() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    let mut spec = agent_spec(3);
    spec.permission_cap = None;
    params.agent_spec = Some(spec);

    let error = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect_err("agent task should reject missing permission cap");

    assert!(format!("{error:#}").contains("agent_spec.permission_cap"));
}

#[tokio::test]
async fn agent_task_create_requires_explicit_execution_admission() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    params.agent_spec = Some(agent_spec(3));

    let error = runtime
        .service()
        .create_task(TaskCreateContext::default(), params)
        .await
        .expect_err("agent task without execution authority must fail closed");

    assert!(
        format!("{error:#}").contains("explicit execution authorization admission"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn task_security_cap_persists_on_agent_spec() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    let mut spec = agent_spec(3);
    let security_cap = test_security_cap();
    spec.security_cap = Some(security_cap.clone());
    params.agent_spec = Some(spec);

    let created = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("agent task should create");
    assert_eq!(
        created
            .agent_spec
            .as_ref()
            .and_then(|spec| spec.security_cap.as_ref()),
        Some(&security_cap)
    );

    let fetched = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: created.task.id,
        })
        .await
        .expect("created task should read");
    assert_eq!(
        fetched
            .agent_specs
            .last()
            .and_then(|spec| spec.security_cap.as_ref()),
        Some(&security_cap)
    );
}

#[tokio::test]
async fn security_intersection_missing_security_cap_is_rejected_for_agent_task() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    let mut spec = agent_spec(3);
    spec.security_cap = None;
    params.agent_spec = Some(spec);

    let error = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect_err("agent task should reject missing security cap");
    assert!(
        format!("{error:#}").contains("agent_spec.security_cap"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn recovery_security_missing_security_cap_is_not_defaulted() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    let mut spec = agent_spec(3);
    spec.security_cap = None;
    params.agent_spec = Some(spec);

    let error = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect_err("agent task should reject missing security cap");
    let message = format!("{error:#}");
    assert!(message.contains("agent_spec.security_cap"));
    assert!(!message.contains("FullAccess"));
    assert!(!message.contains("full_access"));
}

#[tokio::test]
async fn scheduled_agent_task_requires_self_contained_prompt_contract() {
    let runtime = runtime().await;

    let mut missing_prompt = create_params(TaskTriggerSpec::Cron {
        cron_expr: "0 7 * * *".to_owned(),
        timezone: "Europe/Moscow".to_owned(),
        catch_up_policy: None,
    });
    configure_agent_task(&mut missing_prompt);
    missing_prompt.agent_spec = Some(agent_spec(3));
    let error = runtime
        .service()
        .create_task(task_create_context_for(&missing_prompt), missing_prompt)
        .await
        .expect_err("scheduled agent task should reject empty prompt");
    assert!(format!("{error:#}").contains("self-contained executor instructions"));

    let mut missing_output = create_params(TaskTriggerSpec::Cron {
        cron_expr: "0 7 * * *".to_owned(),
        timezone: "Europe/Moscow".to_owned(),
        catch_up_policy: None,
    });
    configure_agent_task(&mut missing_output);
    let mut spec = agent_spec(3);
    spec.prompt.instructions = vec![
        "Use currently available runtime capabilities by capability, not stale tool names."
            .to_owned(),
        "If required data is unavailable, report a clear failure.".to_owned(),
    ];
    missing_output.agent_spec = Some(spec);
    let error = runtime
        .service()
        .create_task(task_create_context_for(&missing_output), missing_output)
        .await
        .expect_err("scheduled agent task should reject missing output contract");
    assert!(format!("{error:#}").contains("output instructions"));

    let mut valid = create_params(TaskTriggerSpec::Cron {
        cron_expr: "0 7 * * *".to_owned(),
        timezone: "Europe/Moscow".to_owned(),
        catch_up_policy: None,
    });
    configure_agent_task(&mut valid);
    let mut spec = agent_spec(3);
    spec.prompt.instructions = vec![
        "Use currently available runtime capabilities by capability, not stale tool names."
            .to_owned(),
        "If required data is unavailable, report a clear failure.".to_owned(),
    ];
    spec.prompt.output_instructions =
        Some("Return concise markdown with result fields or explicit failure reason.".to_owned());
    valid.agent_spec = Some(spec);
    runtime
        .service()
        .create_task(task_create_context_for(&valid), valid)
        .await
        .expect("scheduled agent task should accept a durable prompt contract");
}

#[tokio::test]
async fn update_task_patches_task_trigger_and_base_agent_spec_atomically() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Cron {
        cron_expr: "0 7 * * *".to_owned(),
        timezone: "Europe/Moscow".to_owned(),
        catch_up_policy: None,
    });
    configure_agent_task(&mut params);
    let mut spec = agent_spec(3);
    spec.prompt.instructions = vec![
        "Use currently available runtime capabilities by capability.".to_owned(),
        "Fail clearly when required data is unavailable.".to_owned(),
    ];
    spec.prompt.output_instructions =
        Some("Return markdown with result fields or a clear failure reason.".to_owned());
    params.agent_spec = Some(spec);
    let created = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("scheduled agent task should create");

    let updated = runtime
        .service()
        .update_task(
            TaskMutationContext::default(),
            TaskUpdateParams {
                task_id: created.task.id.clone(),
                expected_revision: Some(created.task.revision),
                title: Some("Updated task".to_owned()),
                goal: Some("Run the updated task".to_owned()),
                trigger: Some(TaskTriggerInput {
                    spec: TaskTriggerSpec::Cron {
                        cron_expr: "15 8 * * *".to_owned(),
                        timezone: "Europe/Moscow".to_owned(),
                        catch_up_policy: None,
                    },
                }),
                instructions: Some(vec![
                    "Use currently available runtime capabilities by capability.".to_owned(),
                    "Report unavailable required data as an explicit failure.".to_owned(),
                ]),
                input_text: Some("city=Moscow".to_owned()),
                input: Some(TaskAgentInput {
                    text: None,
                    variables: vec![TaskAgentInputVariable {
                        name: "units".to_owned(),
                        value: TaskValue::String("metric".to_owned()),
                    }],
                    attachments: Vec::new(),
                    references: Vec::new(),
                }),
                output_instructions: Some("Return concise markdown.".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect("task update should succeed");

    assert_eq!(updated.task.title, "Updated task");
    assert_eq!(updated.task.goal, "Run the updated task");
    assert_eq!(updated.task.revision, created.task.revision + 1);
    assert!(updated.changed_fields.contains(&"trigger".to_owned()));
    assert!(updated.changed_fields.contains(&"instructions".to_owned()));
    let updated_trigger = updated.trigger.expect("trigger should be updated");
    assert_eq!(
        updated_trigger.spec,
        TaskTriggerSpec::Cron {
            cron_expr: "15 8 * * *".to_owned(),
            timezone: "Europe/Moscow".to_owned(),
            catch_up_policy: None,
        }
    );
    let updated_agent_spec = updated.agent_spec.expect("agent spec should be updated");
    assert_eq!(updated_agent_spec.prompt.goal, "Run the updated task");
    assert_eq!(
        updated_agent_spec
            .prompt
            .input
            .as_ref()
            .and_then(|input| input.text.as_deref()),
        Some("city=Moscow")
    );
    assert_eq!(
        updated_agent_spec
            .prompt
            .input
            .as_ref()
            .map(|input| input.variables.len()),
        Some(1)
    );

    let fetched = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: created.task.id,
        })
        .await
        .expect("updated task should read");
    assert_eq!(fetched.task.title, "Updated task");
    assert_eq!(
        fetched
            .agent_specs
            .iter()
            .rev()
            .find(|spec| spec.run_id.is_none())
            .expect("base spec should exist")
            .prompt
            .output_instructions
            .as_deref(),
        Some("Return concise markdown.")
    );
    assert!(
        fetched
            .triggers
            .last()
            .expect("trigger should exist")
            .next_fire_at
            .is_some()
    );
}

#[tokio::test]
async fn update_task_rejects_scheduled_agent_without_prompt_contract() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut params);
    params.agent_spec = Some(agent_spec(3));
    let created = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("immediate agent task should create");

    let error = runtime
        .service()
        .update_task(
            TaskMutationContext::default(),
            TaskUpdateParams {
                task_id: created.task.id,
                trigger: Some(TaskTriggerInput {
                    spec: TaskTriggerSpec::Cron {
                        cron_expr: "0 7 * * *".to_owned(),
                        timezone: "Europe/Moscow".to_owned(),
                        catch_up_policy: None,
                    },
                }),
                ..Default::default()
            },
        )
        .await
        .expect_err("scheduled update should validate final prompt");
    assert!(
        format!("{error:#}").contains("self-contained executor instructions"),
        "unexpected error: {error:#}"
    );
}

#[tokio::test]
async fn lifecycle_defaults_attach_only_immediate_parent_turn_tasks() {
    let runtime = runtime().await;
    let mut immediate = create_params(TaskTriggerSpec::Immediate);
    immediate.created_by_thread_id = Some("thread_a".to_owned());
    immediate.created_by_turn_id = Some("turn_a".to_owned());
    let immediate = runtime
        .service()
        .create_task(task_create_context_for(&immediate), immediate)
        .await
        .expect("immediate task should create");
    assert_eq!(
        immediate.task.lifecycle_policy.unwrap().attachment,
        TaskAttachmentMode::Attached
    );

    let scheduled = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");
    assert_eq!(
        scheduled.task.lifecycle_policy.unwrap().attachment,
        TaskAttachmentMode::Detached
    );
}

#[tokio::test]
async fn max_depth_is_enforced_before_child_events_are_appended() {
    let runtime = runtime().await;
    let mut root = create_params(TaskTriggerSpec::Manual {
        allowed_actor: None,
    });
    root.created_by_thread_id = Some("thread_depth_root".to_owned());
    configure_agent_task(&mut root);
    root.agent_spec = Some(agent_spec(1));
    let root = runtime
        .service()
        .create_task(task_create_context_for(&root), root)
        .await
        .expect("root task should create");

    let mut child = create_params(TaskTriggerSpec::Manual {
        allowed_actor: None,
    });
    configure_agent_task(&mut child);
    child.created_by_thread_id = Some("thread_depth_root".to_owned());
    child.parent_task_id = Some(root.task.id.clone());
    child.agent_spec = Some(agent_spec(1));
    let error = runtime
        .service()
        .create_task(task_create_context_for(&child), child)
        .await
        .expect_err("child beyond max depth should fail");
    assert!(format!("{error:#}").contains("exceeds max depth"));

    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: root.task.id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("events should read");
    assert!(
        events
            .events
            .iter()
            .all(|event| !matches!(event.payload, TaskEventPayload::DepthLimitExceeded { .. }))
    );
}

#[tokio::test]
async fn scheduled_parent_task_can_create_child_when_depth_allows() {
    let runtime = runtime().await;
    let mut root = create_params(TaskTriggerSpec::Cron {
        cron_expr: "0 7 * * *".to_owned(),
        timezone: "UTC".to_owned(),
        catch_up_policy: None,
    });
    root.created_by_thread_id = Some("thread_scheduled_parent".to_owned());
    configure_agent_task(&mut root);
    let mut root_spec = agent_spec(3);
    root_spec.prompt.instructions = vec!["Run the scheduled parent task.".to_owned()];
    root_spec.prompt.output_instructions = Some("Return the scheduled result.".to_owned());
    root.agent_spec = Some(root_spec);
    let root = runtime
        .service()
        .create_task(task_create_context_for(&root), root)
        .await
        .expect("scheduled root task should create");

    let mut child = create_params(TaskTriggerSpec::Immediate);
    configure_agent_task(&mut child);
    child.created_by_thread_id = Some("thread_scheduled_parent".to_owned());
    child.parent_task_id = Some(root.task.id.clone());
    child.agent_spec = Some(agent_spec(3));
    let child = runtime
        .service()
        .create_task(task_create_context_for(&child), child)
        .await
        .expect("child task should be allowed while depth remains within max_depth");
    let child_spec = child.agent_spec.expect("child agent spec");
    assert!(child_spec.depth > 0);
    assert!(child_spec.depth <= child_spec.max_depth);
    assert_eq!(child_spec.max_depth, 3);
}

#[tokio::test]
async fn reschedule_updates_trigger_and_wakes_scheduler() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");

    let rescheduled = runtime
        .service()
        .reschedule_task(
            TaskMutationContext::default(),
            TaskRescheduleParams {
                task_id: response.task.id.clone(),
                trigger: TaskTriggerInput {
                    spec: TaskTriggerSpec::ScheduledAt {
                        scheduled_at: 10,
                        timezone: Some("UTC".to_owned()),
                        catch_up_policy: None,
                    },
                },
            },
        )
        .await
        .expect("task should reschedule");
    assert_eq!(rescheduled.trigger.next_fire_at, Some(10));

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
}

#[tokio::test]
async fn reschedule_cannot_bypass_minimum_interval() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("scheduled task should create");

    let error = runtime
        .service()
        .reschedule_task(
            TaskMutationContext::default(),
            TaskRescheduleParams {
                task_id: response.task.id,
                trigger: TaskTriggerInput {
                    spec: TaskTriggerSpec::Interval {
                        interval_seconds: 1,
                        interval_anchor_at: None,
                        catch_up_policy: None,
                    },
                },
            },
        )
        .await
        .expect_err("reschedule must enforce the shared minimum interval");

    assert!(error.to_string().contains("at least 10 seconds"));
}

#[tokio::test]
async fn cancel_task_is_idempotent_and_cancels_trigger_and_runs() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let run_id = response.run.expect("run should exist").id;

    let first = runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: response.task.id.clone(),
                reason: Some("stop".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("cancel should succeed");
    let second = runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: response.task.id.clone(),
                reason: Some("stop again".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("second cancel should be idempotent");
    assert_eq!(first.task.status, TaskStatus::Cancelled);
    assert_eq!(second.task.status, TaskStatus::Cancelled);

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert!(task.triggers.iter().all(|trigger| {
        matches!(
            trigger.status,
            TaskTriggerStatus::Cancelled | TaskTriggerStatus::Exhausted
        )
    }));
    assert_eq!(
        task.runs
            .iter()
            .find(|run| run.id == run_id)
            .expect("run should exist")
            .status,
        TaskRunStatus::Cancelled
    );
}

#[tokio::test]
async fn executing_task_cannot_cancel_itself() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let run = response.run.expect("immediate task should have a run");
    let (thread_id, turn_id) =
        record_active_task_execution_turn(&runtime, response.task.id.as_str(), run.id.as_str())
            .await;

    let error = runtime
        .service()
        .cancel_task(
            TaskMutationContext::parent_agent(thread_id, turn_id),
            TaskCancelParams {
                task_id: response.task.id.clone(),
                reason: Some("mistaken self cleanup".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect_err("an executing task must not cancel itself");
    assert!(
        format!("{error:#}").contains("cannot_cancel_current_execution_task"),
        "unexpected error: {error:#}"
    );

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should remain readable");
    assert_ne!(task.task.status, TaskStatus::Cancelled);
}

#[tokio::test]
async fn executing_task_cannot_cancel_an_ancestor() {
    let runtime = runtime().await;
    let mut root_params = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
        catch_up_policy: None,
    });
    root_params.created_by_thread_id = Some("thread_ancestor_root".to_owned());
    let root = runtime
        .service()
        .create_task(task_create_context_for(&root_params), root_params)
        .await
        .expect("root task should create");
    let mut child_params = create_params(TaskTriggerSpec::Immediate);
    child_params.created_by_thread_id = Some("thread_ancestor_root".to_owned());
    child_params.parent_task_id = Some(root.task.id.clone());
    let child = runtime
        .service()
        .create_task(task_create_context_for(&child_params), child_params)
        .await
        .expect("child task should create");
    let child_run = child.run.expect("immediate child should have a run");
    let (thread_id, turn_id) =
        record_active_task_execution_turn(&runtime, child.task.id.as_str(), child_run.id.as_str())
            .await;

    let error = runtime
        .service()
        .cancel_task(
            TaskMutationContext::parent_agent(thread_id, turn_id),
            TaskCancelParams {
                task_id: root.task.id.clone(),
                reason: Some("mistaken ancestor cleanup".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect_err("an executing task must not cancel an ancestor");
    assert!(
        format!("{error:#}").contains("cannot_cancel_current_execution_ancestor"),
        "unexpected error: {error:#}"
    );

    let root_after = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: root.task.id,
        })
        .await
        .expect("root task should remain readable");
    assert_ne!(root_after.task.status, TaskStatus::Cancelled);
}

#[tokio::test]
async fn authenticated_user_can_cancel_an_executing_task() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let run = response.run.expect("immediate task should have a run");
    let _ = record_active_task_execution_turn(&runtime, response.task.id.as_str(), run.id.as_str())
        .await;

    let cancelled = runtime
        .service()
        .cancel_task(
            TaskMutationContext::user("user_1"),
            TaskCancelParams {
                task_id: response.task.id,
                reason: Some("user requested stop".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("authenticated user cancellation should remain allowed");
    assert_eq!(cancelled.task.status, TaskStatus::Cancelled);
}

#[tokio::test]
async fn executing_task_can_cancel_an_unrelated_task() {
    let runtime = runtime().await;
    let execution_task = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("execution task should create");
    let execution_run = execution_task
        .run
        .expect("immediate execution task should have a run");
    let (thread_id, turn_id) = record_active_task_execution_turn(
        &runtime,
        execution_task.task.id.as_str(),
        execution_run.id.as_str(),
    )
    .await;
    let unrelated = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("unrelated task should create");

    let cancelled = runtime
        .service()
        .cancel_task(
            TaskMutationContext::parent_agent(thread_id, turn_id),
            TaskCancelParams {
                task_id: unrelated.task.id,
                reason: Some("remove unrelated task".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("an execution should retain permission to cancel unrelated tasks");
    assert_eq!(cancelled.task.status, TaskStatus::Cancelled);
}

#[tokio::test]
async fn cancel_attached_subtree_cancels_detaches_and_keeps_by_policy() {
    let runtime = runtime().await;
    let mut root_params = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
        catch_up_policy: None,
    });
    root_params.created_by_thread_id = Some("thread_cancel_root".to_owned());
    let root = runtime
        .service()
        .create_task(task_create_context_for(&root_params), root_params)
        .await
        .expect("root task should create");

    let mut attached_cancel = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
        catch_up_policy: None,
    });
    attached_cancel.created_by_thread_id = Some("thread_cancel_root".to_owned());
    attached_cancel.parent_task_id = Some(root.task.id.clone());
    attached_cancel.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Attached,
        on_parent_cancel: TaskParentTerminalAction::Cancel,
        on_parent_failure: TaskParentTerminalAction::Cancel,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    let attached_cancel = runtime
        .service()
        .create_task(task_create_context_for(&attached_cancel), attached_cancel)
        .await
        .expect("attached cancel child should create");

    let mut attached_detach = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
        catch_up_policy: None,
    });
    attached_detach.created_by_thread_id = Some("thread_cancel_root".to_owned());
    attached_detach.parent_task_id = Some(root.task.id.clone());
    attached_detach.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Attached,
        on_parent_cancel: TaskParentTerminalAction::Detach,
        on_parent_failure: TaskParentTerminalAction::Detach,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    let attached_detach = runtime
        .service()
        .create_task(task_create_context_for(&attached_detach), attached_detach)
        .await
        .expect("attached detach child should create");

    let mut detached_keep = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
        catch_up_policy: None,
    });
    detached_keep.created_by_thread_id = Some("thread_cancel_root".to_owned());
    detached_keep.parent_task_id = Some(root.task.id.clone());
    detached_keep.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Detached,
        on_parent_cancel: TaskParentTerminalAction::Cancel,
        on_parent_failure: TaskParentTerminalAction::Cancel,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    let detached_keep = runtime
        .service()
        .create_task(task_create_context_for(&detached_keep), detached_keep)
        .await
        .expect("detached child should create");

    let cancelled = runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: root.task.id.clone(),
                reason: Some("parent stopped".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("subtree cancel should succeed");

    assert!(
        cancelled
            .cancelled_tasks
            .iter()
            .any(|task| task.id.as_str() == root.task.id.as_str())
    );
    assert!(
        cancelled
            .cancelled_tasks
            .iter()
            .any(|task| task.id.as_str() == attached_cancel.task.id.as_str())
    );
    assert!(
        cancelled
            .detached_tasks
            .iter()
            .any(|task| task.id.as_str() == attached_detach.task.id.as_str())
    );
    assert!(
        cancelled
            .kept_tasks
            .iter()
            .any(|task| task.id.as_str() == detached_keep.task.id.as_str())
    );

    let attached_cancel_state = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: attached_cancel.task.id,
        })
        .await
        .expect("cancel child should read")
        .task;
    let attached_detach_state = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: attached_detach.task.id,
        })
        .await
        .expect("detach child should read")
        .task;
    let detached_keep_state = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: detached_keep.task.id,
        })
        .await
        .expect("kept child should read")
        .task;

    assert_eq!(attached_cancel_state.status, TaskStatus::Cancelled);
    assert_eq!(attached_detach_state.status, TaskStatus::Scheduled);
    assert_eq!(
        attached_detach_state.lifecycle_policy.unwrap().attachment,
        TaskAttachmentMode::Detached
    );
    assert_eq!(detached_keep_state.status, TaskStatus::Scheduled);
}

#[tokio::test]
async fn detach_task_updates_attachment_without_cancelling() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
        catch_up_policy: None,
    });
    params.lifecycle_policy = Some(pioneer_protocol::TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Attached,
        on_parent_cancel: pioneer_protocol::TaskParentTerminalAction::Cancel,
        on_parent_failure: pioneer_protocol::TaskParentTerminalAction::Cancel,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    let response = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .expect("task should create");
    let detached = runtime
        .service()
        .detach_task(
            TaskMutationContext::default(),
            TaskDetachParams {
                task_id: response.task.id,
            },
        )
        .await
        .expect("detach should succeed");

    assert_eq!(detached.task.status, TaskStatus::Scheduled);
    assert_eq!(
        detached.task.lifecycle_policy.unwrap().attachment,
        TaskAttachmentMode::Detached
    );
}

#[tokio::test]
async fn wait_wakes_from_event_bus_on_terminal_event() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("scheduled task should create");

    let service = runtime.service();
    let task_id = response.task.id.clone();
    let waiter = tokio::spawn({
        let service = service.clone();
        let task_id = task_id.clone();
        async move {
            service
                .wait_tasks(
                    TaskWaitContext::default(),
                    TaskWaitParams {
                        task_ids: vec![task_id],
                        run_ids: Vec::new(),
                        timeout_ms: Some(5_000),
                        return_completed: true,
                        return_pending: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("wait should complete")
        }
    });

    service
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id,
                reason: Some("test cancellation".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("task should cancel");

    let waited = waiter.await.expect("waiter should join");
    assert_eq!(waited.cancelled.len(), 1);
    assert!(!waited.timed_out);
}

#[tokio::test]
async fn wait_classifies_terminal_run_even_before_task_terminal_event() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let run_id = response.run.expect("run should exist").id;

    let waiter = tokio::spawn({
        let service = runtime.service();
        let run_id = run_id.clone();
        async move {
            service
                .wait_tasks(
                    TaskWaitContext::default(),
                    TaskWaitParams {
                        task_ids: Vec::new(),
                        run_ids: vec![run_id],
                        timeout_ms: Some(5_000),
                        return_completed: true,
                        return_pending: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("wait should complete")
        }
    });

    let appended = runtime
        .service()
        .append_event(
            TaskEventPayload::RunCompleted {
                task_id: response.task.id,
                run_id,
                result: None,
                completed_at: 42,
            },
            42,
        )
        .await
        .expect("run completion event should append");
    runtime.service().publish_and_wake(vec![appended]).await;

    let waited = waiter.await.expect("waiter should join");
    assert_eq!(waited.completed.len(), 1);
    assert!(waited.pending.is_empty());
}

#[tokio::test]
async fn wait_detects_terminal_state_without_in_memory_wake_delivery() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let run_id = response.run.expect("run should exist").id;
    let task_id = response.task.id;

    let waiter = tokio::spawn({
        let service = runtime.service();
        let run_id = run_id.clone();
        async move {
            service
                .wait_tasks(
                    TaskWaitContext::default(),
                    TaskWaitParams {
                        task_ids: Vec::new(),
                        run_ids: vec![run_id],
                        timeout_ms: Some(5_000),
                        return_completed: true,
                        return_pending: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("wait should complete from durable state")
        }
    });

    runtime
        .service()
        .append_event(
            TaskEventPayload::RunCompleted {
                task_id,
                run_id,
                result: None,
                completed_at: 42,
            },
            42,
        )
        .await
        .expect("run completion event should append");

    let waited = waiter.await.expect("waiter should join");
    assert_eq!(waited.completed.len(), 1);
    assert!(!waited.timed_out);
}

#[tokio::test]
async fn wait_timeout_returns_partial_pending_state() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("task should create");

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext::default(),
            TaskWaitParams {
                task_ids: vec![response.task.id],
                run_ids: Vec::new(),
                timeout_ms: Some(5),
                return_completed: true,
                return_pending: true,
                ..Default::default()
            },
        )
        .await
        .expect("wait should return timeout");
    assert!(waited.timed_out);
    assert_eq!(waited.pending.len(), 1);
}

#[tokio::test]
async fn wait_with_requested_timeout_uses_the_observation_deadline() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("task should create");
    let budget = TaskResourceBudget {
        max_wait_duration_ms: 5,
        ..TaskResourceBudget::default()
    };

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext {
                task_resource_budget: Some(budget),
                ..TaskWaitContext::default()
            },
            TaskWaitParams {
                task_ids: vec![response.task.id],
                run_ids: Vec::new(),
                timeout_ms: Some(50),
                return_completed: true,
                return_pending: true,
                ..Default::default()
            },
        )
        .await
        .expect("wait should return control at the observation deadline");

    assert!(waited.timed_out);
    assert_eq!(waited.pending.len(), 1);
}

#[tokio::test]
async fn wait_return_pending_false_still_waits_on_internal_pending_state() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("task should create");

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext::default(),
            TaskWaitParams {
                task_ids: vec![response.task.id],
                run_ids: Vec::new(),
                timeout_ms: Some(5),
                return_completed: true,
                return_pending: false,
                ..Default::default()
            },
        )
        .await
        .expect("wait should return timeout");
    assert!(waited.timed_out);
    assert_eq!(waited.total_count, 1);
    assert_eq!(waited.pending_count, 1);
    assert!(waited.pending.is_empty());
}

#[tokio::test]
async fn wait_all_terminal_keeps_waiting_for_review_required_candidate() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext::default(),
            TaskWaitParams {
                task_ids: vec![task_id],
                run_ids: Vec::new(),
                timeout_ms: Some(5),
                mode: TaskWaitMode::AllTerminal,
                return_completed: true,
                return_pending: true,
            },
        )
        .await
        .expect("wait should return timeout");

    assert!(waited.timed_out);
    assert_eq!(waited.terminal_count, 0);
    assert_eq!(waited.pending_count, 0);
    assert_eq!(waited.review_required_count, 1);
    assert_eq!(waited.review_required.len(), 1);
    assert_eq!(waited.review_required[0].candidate.id, candidate_id);
}

#[tokio::test]
async fn wait_all_terminal_or_review_required_returns_candidate() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext::default(),
            TaskWaitParams {
                task_ids: vec![task_id],
                run_ids: Vec::new(),
                timeout_ms: Some(5_000),
                mode: TaskWaitMode::AllTerminalOrReviewRequired,
                return_completed: true,
                return_pending: true,
            },
        )
        .await
        .expect("wait should return review-required candidate");

    assert!(!waited.timed_out);
    assert_eq!(waited.terminal_count, 0);
    assert_eq!(waited.pending_count, 0);
    assert_eq!(waited.review_required_count, 1);
    assert!(waited.completed.is_empty());
    assert!(waited.pending.is_empty());
    let review = &waited.review_required[0];
    assert_eq!(review.candidate.id, candidate_id);
    assert_eq!(review.item.task.status, TaskStatus::WaitingReview);
    assert_eq!(
        review.item.run.as_ref().map(|run| run.status),
        Some(TaskRunStatus::WaitingReview)
    );
    assert!(review.item.child_thread_id.is_some());
    assert!(review.item.child_turn_id.is_some());
    assert_eq!(review.max_revision_rounds, 2);
    assert_eq!(review.remaining_revision_rounds, 2);
    assert_eq!(
        review.allowed_actions,
        vec![
            TaskWaitReviewAction::TaskAccept,
            TaskWaitReviewAction::TaskRevise,
            TaskWaitReviewAction::TaskCancel,
        ]
    );
    assert_eq!(review.revision_blocked_reason, None);
}

#[tokio::test]
async fn wait_any_terminal_or_review_required_returns_on_review_candidate() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 0, TaskResultCandidateStatus::PendingReview)
            .await;

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext::default(),
            TaskWaitParams {
                task_ids: vec![task_id],
                run_ids: Vec::new(),
                timeout_ms: Some(5_000),
                mode: TaskWaitMode::AnyTerminalOrReviewRequired,
                return_completed: true,
                return_pending: true,
            },
        )
        .await
        .expect("wait should return review-required candidate");

    assert!(!waited.timed_out);
    assert_eq!(waited.review_required_count, 1);
    assert_eq!(waited.review_required[0].candidate.id, candidate_id);
    assert!(waited.completed.is_empty());
}

#[tokio::test]
async fn wait_review_required_removes_revise_when_revision_limit_reached() {
    let runtime = runtime_with_review_config().await;
    let (task_id, _run_id, _candidate_id) =
        create_waiting_review_agent_task(&runtime, 2, 2, TaskResultCandidateStatus::PendingReview)
            .await;

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext::default(),
            TaskWaitParams {
                task_ids: vec![task_id],
                run_ids: Vec::new(),
                timeout_ms: Some(5_000),
                mode: TaskWaitMode::AllTerminalOrReviewRequired,
                return_completed: true,
                return_pending: true,
            },
        )
        .await
        .expect("wait should return review-required candidate");

    let review = &waited.review_required[0];
    assert_eq!(review.remaining_revision_rounds, 0);
    assert_eq!(
        review.allowed_actions,
        vec![
            TaskWaitReviewAction::TaskAccept,
            TaskWaitReviewAction::TaskCancel,
        ]
    );
    assert_eq!(
        review.revision_blocked_reason,
        Some(TaskWaitRevisionBlockedReason::MaxRevisionRoundsReached)
    );
}

#[tokio::test]
async fn wait_returns_non_waitable_snapshot_for_future_scheduled_task_without_active_run() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("task should create");

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext::default(),
            TaskWaitParams {
                task_ids: vec![response.task.id],
                run_ids: Vec::new(),
                timeout_ms: None,
                return_completed: true,
                return_pending: true,
                ..Default::default()
            },
        )
        .await
        .expect("wait should return non-waitable snapshot immediately");

    assert!(!waited.timed_out);
    assert_eq!(waited.pending_count, 0);
    assert_eq!(waited.non_waitable_count, 1);
    assert_eq!(waited.non_waitable.len(), 1);
    assert_eq!(
        waited.non_waitable[0].reason,
        pioneer_protocol::TaskWaitNonWaitableReason::FutureScheduledTaskWithoutActiveRun
    );
}

#[tokio::test]
async fn wait_any_terminal_returns_after_first_target_finishes() {
    let runtime = runtime().await;
    let first = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("first task should create");
    let second = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("second task should create");

    let waiter = tokio::spawn({
        let service = runtime.service();
        let task_ids = vec![first.task.id.clone(), second.task.id.clone()];
        async move {
            service
                .wait_tasks(
                    TaskWaitContext::default(),
                    TaskWaitParams {
                        task_ids,
                        run_ids: Vec::new(),
                        timeout_ms: Some(5_000),
                        mode: TaskWaitMode::AnyTerminal,
                        return_completed: true,
                        return_pending: true,
                    },
                )
                .await
                .expect("wait should complete")
        }
    });

    runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: first.task.id,
                reason: Some("test cancellation".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("first task should cancel");

    let waited = waiter.await.expect("waiter should join");
    assert!(!waited.timed_out);
    assert_eq!(waited.total_count, 2);
    assert_eq!(waited.terminal_count, 1);
    assert_eq!(waited.pending_count, 1);
    assert_eq!(waited.cancelled.len(), 1);
}

#[tokio::test]
async fn cancellation_class_failure_projects_as_cancelled_without_task_failed() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CancellationFailingSystemExecutor))
        .await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");

    runtime
        .process_due_once(i64::MAX / 4)
        .await
        .expect("scheduler should run");

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read");
    assert_eq!(task.task.status, TaskStatus::Cancelled);
    assert_eq!(task.runs.last().unwrap().status, TaskRunStatus::Cancelled);

    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("events should read");
    assert!(
        events
            .events
            .iter()
            .all(|event| event.event_type != pioneer_protocol::constants::events::TASK_FAILED)
    );
}

#[tokio::test]
async fn projector_does_not_regress_cancelled_task_after_late_failure_events() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();
    let run_id = response.run.expect("run should exist").id;

    for event in [
        TaskEventPayload::RunCancelled {
            task_id: task_id.clone(),
            run_id: run_id.clone(),
            reason: Some("parent cancelled".to_owned()),
            cancelled_at: 10,
        },
        TaskEventPayload::TaskCancelled {
            task_id: task_id.clone(),
            reason: Some("parent cancelled".to_owned()),
            completed_at: 10,
        },
        TaskEventPayload::RunFailed {
            task_id: task_id.clone(),
            run_id: run_id.clone(),
            error: Some(TaskError {
                code: "child_turn_cancelled".to_owned(),
                message: "task cancelled".to_owned(),
                class: TaskErrorClass::Cancelled,
                details: None,
                failed_run_id: Some(run_id.clone()),
            }),
            completed_at: 11,
        },
        TaskEventPayload::TaskFailed {
            task_id: task_id.clone(),
            error: Some(TaskError {
                code: "child_turn_cancelled".to_owned(),
                message: "task cancelled".to_owned(),
                class: TaskErrorClass::Cancelled,
                details: None,
                failed_run_id: Some(run_id),
            }),
            completed_at: 11,
        },
    ] {
        runtime
            .service()
            .append_event(event, 11)
            .await
            .expect("event should append");
    }

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams { task_id })
        .await
        .expect("task should read");
    assert_eq!(task.task.status, TaskStatus::Cancelled);
    assert_eq!(task.runs.last().unwrap().status, TaskRunStatus::Cancelled);
}

#[tokio::test]
async fn projector_replay_run_started_after_terminal_run_is_noop() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();
    let run_id = response.run.expect("run should exist").id;

    runtime
        .service()
        .append_event(
            TaskEventPayload::RunCompleted {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                result: Some(TaskResult {
                    summary: Some("done".to_owned()),
                    data: None,
                    artifacts: Vec::new(),
                    completed_by_run_id: Some(run_id.clone()),
                }),
                completed_at: 10,
            },
            10,
        )
        .await
        .expect("run completed should append");
    runtime
        .service()
        .append_event(
            TaskEventPayload::RunStarted {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                started_at: 11,
            },
            11,
        )
        .await
        .expect("late run started should append");
    let late_failure = runtime
        .service()
        .append_event(
            TaskEventPayload::RunFailed {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                error: Some(TaskError {
                    code: "late_failure".to_owned(),
                    message: "late failure".to_owned(),
                    class: TaskErrorClass::Internal,
                    details: None,
                    failed_run_id: Some(run_id.clone()),
                }),
                completed_at: 12,
            },
            12,
        )
        .await;
    assert!(
        late_failure.is_err(),
        "contradictory terminal run event should be rejected"
    );

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams { task_id })
        .await
        .expect("task should read");
    assert_eq!(task.runs.last().unwrap().status, TaskRunStatus::Succeeded);
    assert_eq!(task.task.status, TaskStatus::Queued);
}

#[tokio::test]
async fn duplicate_keyed_run_started_append_is_noop_without_new_sequence() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();
    let run_id = response.run.expect("run should exist").id;
    let event = TaskEventPayload::RunStarted {
        task_id: task_id.clone(),
        run_id: run_id.clone(),
        started_at: 11,
    };

    let first = runtime
        .service()
        .append_event(event.clone(), 11)
        .await
        .expect("first run started should append");
    let duplicate = runtime
        .service()
        .append_event(event, 11)
        .await
        .expect("duplicate run started should be a no-op");

    assert_eq!(first.append_status, TaskEventAppendStatus::Inserted);
    assert_eq!(
        duplicate.append_status,
        TaskEventAppendStatus::AlreadyExists
    );
    assert_eq!(duplicate.id, first.id);
    assert_eq!(duplicate.sequence, first.sequence);
    let expected_key = format!("run:{run_id}:started");
    assert_eq!(
        duplicate.idempotency_key.as_deref(),
        Some(expected_key.as_str())
    );

    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("task events should read");
    assert_eq!(
        events
            .events
            .iter()
            .filter(|event| event.idempotency_key.as_deref() == Some(expected_key.as_str()))
            .count(),
        1
    );
}

#[tokio::test]
async fn duplicate_child_thread_link_append_is_noop_without_new_sequence() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();
    let run_id = response.run.expect("run should exist").id;
    let lineage = ThreadLineage {
        child_thread_id: "child_thread_0000001".to_owned(),
        child_turn_id: "child_turn_00000001".to_owned(),
        parent_thread_id: "parent_thread_000000".to_owned(),
        parent_turn_id: Some("parent_turn_0000001".to_owned()),
        task_id: task_id.clone(),
        task_run_id: run_id.clone(),
        root_thread_id: "parent_thread_000000".to_owned(),
        depth: 1,
        created_at: 12,
    };
    let event = TaskEventPayload::ChildThreadLinked { lineage };

    let first = runtime
        .service()
        .append_event(event.clone(), 12)
        .await
        .expect("first child link should append");
    let duplicate = runtime
        .service()
        .append_event(event, 12)
        .await
        .expect("duplicate child link should be a no-op");

    assert_eq!(first.append_status, TaskEventAppendStatus::Inserted);
    assert_eq!(
        duplicate.append_status,
        TaskEventAppendStatus::AlreadyExists
    );
    assert_eq!(duplicate.id, first.id);
    assert_eq!(duplicate.sequence, first.sequence);

    let expected_key = format!("run:{run_id}:child_thread_linked");
    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("task events should read");
    assert_eq!(
        events
            .events
            .iter()
            .filter(|event| event.idempotency_key.as_deref() == Some(expected_key.as_str()))
            .count(),
        1
    );
}

#[tokio::test]
async fn projector_replay_task_queued_after_task_cancelled_is_noop() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();
    let run_id = response.run.expect("run should exist").id;

    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskCancelled {
                task_id: task_id.clone(),
                reason: Some("stop".to_owned()),
                completed_at: 20,
            },
            20,
        )
        .await
        .expect("task cancelled should append");
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskQueued {
                task_id: task_id.clone(),
                run_id: Some(run_id),
            },
            21,
        )
        .await
        .expect("late queued should append");

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams { task_id })
        .await
        .expect("task should read");
    assert_eq!(task.task.status, TaskStatus::Cancelled);
}

#[tokio::test]
async fn event_bus_filters_committed_events_by_workspace_and_root() {
    let runtime = runtime().await;
    let mut subscription = runtime.event_bus().subscribe(crate::TaskEventFilter {
        workspace_id: Some("ws_tasks".to_owned()),
        task_ids: Vec::new(),
        run_ids: Vec::new(),
        root_task_id: None,
        parent_task_id: None,
    });
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("task should create");

    let delivery = timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("subscription should receive");
    let crate::TaskEventWakeDelivery::Wake(wake) = delivery else {
        panic!("event bus should deliver a wake");
    };
    assert_eq!(wake.workspace_id.as_deref(), Some("ws_tasks"));
    assert_eq!(wake.task_id, response.task.id);
}

#[tokio::test]
async fn event_bus_ignores_non_inserted_duplicate_events() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();
    let event = TaskEventPayload::TaskCompleted {
        task_id: task_id.clone(),
        result: None,
        completed_at: 15,
    };

    runtime
        .service()
        .append_event(event.clone(), 15)
        .await
        .expect("first terminal event should insert");
    let duplicate = runtime
        .service()
        .append_event(event, 15)
        .await
        .expect("duplicate terminal event should be idempotent");
    assert_eq!(
        duplicate.append_status,
        TaskEventAppendStatus::AlreadyExists
    );

    let mut subscription = runtime.event_bus().subscribe(crate::TaskEventFilter {
        task_ids: vec![task_id],
        ..Default::default()
    });
    runtime.event_bus().publish(duplicate).await;

    timeout(Duration::from_millis(50), subscription.recv())
        .await
        .expect_err("duplicate event should not wake subscribers");
}

#[tokio::test]
async fn task_event_cursor_reads_committed_history_after_runtime_restart() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("task should create");
    let store = runtime.service().store();
    let task_id = response.task.id.clone();

    let initial_events = runtime
        .service()
        .list_task_events_after(task_id.as_str(), 0)
        .await
        .expect("cursor should read committed events");
    assert!(!initial_events.is_empty());
    let last_sequence = initial_events
        .last()
        .expect("events checked as non-empty")
        .sequence;

    let restarted_runtime = TaskRuntime::new(store);
    let replayed_events = restarted_runtime
        .service()
        .list_task_events_after(task_id.as_str(), 0)
        .await
        .expect("cursor should replay committed events after runtime restart");
    assert_eq!(replayed_events.len(), initial_events.len());

    let no_new_events = restarted_runtime
        .service()
        .list_task_events_after(task_id.as_str(), last_sequence)
        .await
        .expect("cursor should honor after_sequence");
    assert!(no_new_events.is_empty());

    let task_ids = restarted_runtime
        .service()
        .list_task_event_task_ids()
        .await
        .expect("task event task ids should be discoverable after restart");
    assert!(task_ids.iter().any(|id| id == &task_id));
}

#[tokio::test]
async fn startup_reconciliation_emits_recovered_event_for_dormant_active_trigger() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("manual task should create");

    runtime.start().await.expect("runtime should start");
    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("events should read");

    assert!(events.events.iter().any(|event| {
        matches!(
            event.payload,
            pioneer_protocol::TaskEventPayload::TaskRecovered { .. }
        )
    }));
}

#[tokio::test]
async fn runtime_start_processes_overdue_scheduled_triggers_before_returning() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 1,
                timezone: Some("UTC".to_owned()),
                catch_up_policy: None,
            }),
        )
        .await
        .expect("overdue scheduled task should create");

    runtime.start().await.expect("runtime should start");

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
    assert_eq!(task.runs[0].status, TaskRunStatus::Succeeded);
    assert_eq!(task.triggers[0].status, TaskTriggerStatus::Exhausted);
    assert_eq!(task.triggers[0].next_fire_at, None);
}

#[tokio::test]
async fn startup_reconciliation_is_idempotent() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("manual task should create");

    runtime.start().await.expect("runtime should start");
    runtime.start().await.expect("runtime should start twice");
    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id,
            after_sequence: None,
            limit: None,
        })
        .await
        .expect("events should read");

    let recovered_count = events
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                pioneer_protocol::TaskEventPayload::TaskRecovered { .. }
            )
        })
        .count();
    assert_eq!(recovered_count, 1);
}

#[tokio::test]
async fn role_owned_task_observation_budget_clamps_list_and_event_pages() {
    let runtime = runtime().await;
    let first = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("first task should create");
    runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("second task should create");
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskPaused {
                task: first.task.clone(),
                triggers: vec![first.trigger.clone()],
                reason: Some("budget fixture".to_owned()),
                paused_at: 20,
            },
            20,
        )
        .await
        .expect("second task event should append");

    let budget = TaskResourceBudget {
        max_page_items: 1,
        max_event_page_items: 1,
        ..TaskResourceBudget::default()
    };
    let page = runtime
        .service()
        .list_tasks_with_budget(
            pioneer_protocol::TaskListParams {
                workspace_id: "ws_tasks".to_owned(),
                limit: None,
                ..Default::default()
            },
            budget,
        )
        .await
        .expect("bounded task list should succeed");
    assert_eq!(page.tasks.len(), 1);
    assert!(page.next_cursor.is_some());

    let events = runtime
        .service()
        .get_task_events_with_budget(
            TaskEventsParams {
                task_id: first.task.id,
                after_sequence: None,
                limit: None,
            },
            budget,
        )
        .await
        .expect("bounded task events should succeed");
    assert_eq!(events.events.len(), 1);
    assert!(events.has_more);
}
