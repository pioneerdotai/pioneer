mod agent_diff_event_compaction;
mod agent_domain_upgrade;
mod authorization_legacy_backfill;
mod cli_runtime_native_event_compaction;
mod execution_authority_integrity;
mod stable_skill_id_backfill;
mod task_anchor_backfill;
mod task_event_fanout_cursor_backfill;
pub(crate) mod thread_episodic_workspace_capsule_refill;
mod timeline_pagination_backfill;
mod turn_event_projection_stream_state_backfill;
mod turn_item_attempt_payload_compaction;
mod turn_item_execution_class_backfill;
mod turn_permission_profile_backfill;
mod zstd_payload_compression;

use pioneer_config::{
    GatewayContextCompactionTimeoutConfig, GatewayThreadEpisodicVectorSearchConfig,
};
use pioneer_crud::CrudStore;
use pioneer_provider::ProviderRegistry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use thread_episodic_workspace_capsule_refill::ThreadEpisodicWorkspaceCapsuleRefillStatusSender;
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;

tokio::task_local! {
    static STARTUP_MAINTENANCE_CANCELLATION: CancellationToken;
}

const BACKGROUND_BATCH_PAUSE: std::time::Duration = std::time::Duration::from_millis(25);

#[derive(Debug)]
struct StartupMaintenanceCancelled;

impl std::fmt::Display for StartupMaintenanceCancelled {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Gateway startup maintenance was cancelled")
    }
}

impl std::error::Error for StartupMaintenanceCancelled {}

pub(super) async fn maintenance_checkpoint() -> anyhow::Result<()> {
    let cancellation = maintenance_cancellation();
    tokio::select! {
        _ = cancellation.cancelled() => Err(StartupMaintenanceCancelled.into()),
        _ = tokio::time::sleep(BACKGROUND_BATCH_PAUSE) => Ok(()),
    }
}

fn maintenance_cancellation() -> CancellationToken {
    STARTUP_MAINTENANCE_CANCELLATION
        .try_with(Clone::clone)
        .unwrap_or_default()
}

fn is_maintenance_cancelled(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<StartupMaintenanceCancelled>()
        .is_some()
}

#[derive(Default)]
pub(crate) struct ThreadEpisodicWorkspaceRefillSupervisor {
    operation: Mutex<()>,
    global_operation: Mutex<()>,
    active: Mutex<HashMap<String, ActiveWorkspaceRefill>>,
    active_global_settings_refill: Mutex<Option<ActiveWorkspaceRefill>>,
    settings_owned_workspaces: Mutex<HashSet<String>>,
    shutting_down: AtomicBool,
}

struct ActiveWorkspaceRefill {
    cancellation: CancellationToken,
    completed: watch::Receiver<bool>,
}

#[derive(Clone, Copy)]
enum RefillOwner {
    Startup,
    Settings,
}

pub(crate) struct ThreadEpisodicWorkspaceRefillLease {
    cancellation: CancellationToken,
    completed: Option<watch::Sender<bool>>,
}

/// Completes the mandatory execution-authority integrity gate before any
/// listener, recovery worker, or task runtime can observe active executions.
/// Invalid rows are quarantined; storage/scan failures abort Gateway startup.
pub(crate) async fn enforce_execution_authority_integrity(
    crud_store: &CrudStore,
) -> anyhow::Result<()> {
    execution_authority_integrity::run(crud_store).await
}

/// Converts all pre-Agent-domain Task data before the Gateway listener or any
/// task worker can observe the database. The runtime only supports the final
/// representation produced by this upgrade.
pub(crate) async fn upgrade_agent_domain_data(
    message_processor: &crate::message::MessageProcessor,
) -> anyhow::Result<()> {
    agent_domain_upgrade::apply(message_processor).await
}

impl ThreadEpisodicWorkspaceRefillSupervisor {
    pub(crate) async fn begin_settings(
        &self,
        workspace_id: &str,
    ) -> ThreadEpisodicWorkspaceRefillLease {
        let _operation = self.operation.lock().await;
        if self.shutting_down.load(Ordering::Acquire) {
            return cancelled_refill_lease();
        }
        self.settings_owned_workspaces
            .lock()
            .await
            .insert(workspace_id.to_owned());
        self.cancel_and_wait_locked(workspace_id).await;

        self.insert_active(workspace_id).await
    }

    async fn begin_startup(
        &self,
        workspace_id: &str,
    ) -> Option<ThreadEpisodicWorkspaceRefillLease> {
        let _operation = self.operation.lock().await;
        if self.shutting_down.load(Ordering::Acquire) {
            return None;
        }
        if self
            .settings_owned_workspaces
            .lock()
            .await
            .contains(workspace_id)
        {
            return None;
        }
        self.cancel_and_wait_locked(workspace_id).await;
        Some(self.insert_active(workspace_id).await)
    }

    async fn reserve_settings_workspaces(&self, workspace_ids: &[String]) {
        let _operation = self.operation.lock().await;
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        self.settings_owned_workspaces
            .lock()
            .await
            .extend(workspace_ids.iter().cloned());
        for workspace_id in workspace_ids {
            self.cancel_and_wait_locked(workspace_id.as_str()).await;
        }
    }

    async fn insert_active(&self, workspace_id: &str) -> ThreadEpisodicWorkspaceRefillLease {
        let cancellation = CancellationToken::new();
        let (completed_tx, completed_rx) = watch::channel(false);
        self.active.lock().await.insert(
            workspace_id.to_owned(),
            ActiveWorkspaceRefill {
                cancellation: cancellation.clone(),
                completed: completed_rx,
            },
        );
        ThreadEpisodicWorkspaceRefillLease {
            cancellation,
            completed: Some(completed_tx),
        }
    }

    pub(crate) async fn cancel_and_wait(&self, workspace_id: &str) {
        let _operation = self.operation.lock().await;
        self.settings_owned_workspaces
            .lock()
            .await
            .insert(workspace_id.to_owned());
        self.cancel_and_wait_locked(workspace_id).await;
    }

    pub(crate) async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);

        // A global settings refill can still be between listing workspaces and
        // acquiring its first workspace lease. Cancel and join that outer task
        // before draining per-workspace leases so no task can escape shutdown.
        let _global_operation = self.global_operation.lock().await;
        if let Some(active) = self.active_global_settings_refill.lock().await.take() {
            cancel_and_wait_active_refill(active).await;
        }

        let _operation = self.operation.lock().await;
        let active = self
            .active
            .lock()
            .await
            .drain()
            .map(|(_, active)| active)
            .collect::<Vec<_>>();
        for mut active in active {
            active.cancellation.cancel();
            while !*active.completed.borrow() {
                if active.completed.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    async fn spawn_global_settings_refill<F, Fut>(self: &Arc<Self>, run: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let _global_operation = self.global_operation.lock().await;
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }

        // Replacement is the generation fence: the next settings snapshot is
        // not allowed to start until the task carrying the older snapshot has
        // observed cancellation and fully stopped.
        if let Some(active) = self.active_global_settings_refill.lock().await.take() {
            cancel_and_wait_active_refill(active).await;
        }
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }

        let cancellation = CancellationToken::new();
        let (completed_tx, completed_rx) = watch::channel(false);
        *self.active_global_settings_refill.lock().await = Some(ActiveWorkspaceRefill {
            cancellation: cancellation.clone(),
            completed: completed_rx,
        });
        tokio::spawn(async move {
            run(cancellation).await;
            let _ = completed_tx.send(true);
        });
    }

    async fn cancel_and_wait_locked(&self, workspace_id: &str) {
        let active = self.active.lock().await.remove(workspace_id);
        let Some(mut active) = active else {
            return;
        };

        active.cancellation.cancel();
        while !*active.completed.borrow() {
            if active.completed.changed().await.is_err() {
                break;
            }
        }
    }
}

async fn cancel_and_wait_active_refill(mut active: ActiveWorkspaceRefill) {
    active.cancellation.cancel();
    while !*active.completed.borrow() {
        if active.completed.changed().await.is_err() {
            break;
        }
    }
}

fn cancelled_refill_lease() -> ThreadEpisodicWorkspaceRefillLease {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    ThreadEpisodicWorkspaceRefillLease {
        cancellation,
        completed: None,
    }
}

impl ThreadEpisodicWorkspaceRefillLease {
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

impl Drop for ThreadEpisodicWorkspaceRefillLease {
    fn drop(&mut self) {
        if let Some(completed) = self.completed.take() {
            let _ = completed.send(true);
        }
    }
}

pub(crate) async fn run(
    message_processor: Arc<crate::message::MessageProcessor>,
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    thread_episodic_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    thread_episodic_workspace_vector_search_configs: BTreeMap<
        String,
        GatewayThreadEpisodicVectorSearchConfig,
    >,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    refill_supervisor: Arc<ThreadEpisodicWorkspaceRefillSupervisor>,
    context_compaction_timeout_config: GatewayContextCompactionTimeoutConfig,
    cancellation: CancellationToken,
) -> anyhow::Result<()> {
    STARTUP_MAINTENANCE_CANCELLATION
        .scope(
            cancellation,
            run_inner(
                message_processor,
                crud_store,
                thread_episodic_storage_root,
                thread_episodic_vector_search_config,
                thread_episodic_workspace_vector_search_configs,
                provider_registry,
                runtime_home,
                refill_status_sender,
                refill_supervisor,
                context_compaction_timeout_config,
            ),
        )
        .await
}

async fn run_inner(
    message_processor: Arc<crate::message::MessageProcessor>,
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    thread_episodic_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    thread_episodic_workspace_vector_search_configs: BTreeMap<
        String,
        GatewayThreadEpisodicVectorSearchConfig,
    >,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    refill_supervisor: Arc<ThreadEpisodicWorkspaceRefillSupervisor>,
    context_compaction_timeout_config: GatewayContextCompactionTimeoutConfig,
) -> anyhow::Result<()> {
    let crud_store = Arc::new(crud_store.with_background_write_admission());
    let interrupted_before_unix = chrono::Utc::now().timestamp();
    let trace = pioneer_observability::GatewayOperationTrace::start(
        pioneer_observability::GatewayOperation::DatabaseStartupMaintenance,
    );
    let mut failed = false;
    macro_rules! run_stage {
        ($stage:expr, $future:expr) => {{
            if let Err(error) = maintenance_checkpoint().await {
                trace.finish_cancelled();
                return Err(error);
            }
            let stage = trace.stage($stage);
            if let Err(error) = $future.await {
                if is_maintenance_cancelled(&error) {
                    stage.cancel();
                    trace.finish_cancelled();
                    return Err(error);
                } else {
                    drop(stage);
                    failed = true;
                    tracing::warn!(error = %format!("{error:#}"), "Gateway startup maintenance stage failed");
                }
            } else {
                stage.succeed();
            }
        }};
    }
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseTurnItemExecutionClassBackfill,
        turn_item_execution_class_backfill::run(
            crud_store.as_ref(),
            context_compaction_timeout_config,
        )
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseTurnEventProjectionBackfill,
        turn_event_projection_stream_state_backfill::run(crud_store.as_ref())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseTaskEventFanoutCursorBackfill,
        task_event_fanout_cursor_backfill::run(crud_store.as_ref())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseAuthorizationLegacyBackfill,
        authorization_legacy_backfill::run(crud_store.as_ref())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseStableSkillIdBackfill,
        stable_skill_id_backfill::run(crud_store.as_ref(), message_processor.as_ref())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseTaskAnchorBackfill,
        task_anchor_backfill::run(crud_store.as_ref())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseTurnPermissionProfileBackfill,
        turn_permission_profile_backfill::run(crud_store.as_ref(), runtime_home.as_path())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseTimelinePaginationBackfill,
        timeline_pagination_backfill::run(crud_store.as_ref())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseAgentDiffCompaction,
        agent_diff_event_compaction::run(crud_store.as_ref())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseCliRuntimeEventCompaction,
        cli_runtime_native_event_compaction::run(crud_store.as_ref())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabaseTurnItemAttemptPayloadCompaction,
        turn_item_attempt_payload_compaction::run(crud_store.as_ref())
    );
    run_stage!(
        pioneer_observability::GatewayOperationStage::DatabasePayloadCompression,
        zstd_payload_compression::run(crud_store.as_ref())
    );
    let refill_stage =
        trace.stage(pioneer_observability::GatewayOperationStage::DatabaseThreadEpisodicRefill);
    let refill_cancellation = maintenance_cancellation();
    run_thread_episodic_workspace_capsule_refill(
        crud_store,
        thread_episodic_storage_root,
        thread_episodic_vector_search_config,
        thread_episodic_workspace_vector_search_configs,
        provider_registry,
        runtime_home,
        refill_status_sender,
        refill_supervisor,
        Some(interrupted_before_unix),
        RefillOwner::Startup,
        refill_cancellation.clone(),
    )
    .await;
    if refill_cancellation.is_cancelled() {
        refill_stage.cancel();
        trace.finish_cancelled();
        return Err(StartupMaintenanceCancelled.into());
    }
    refill_stage.succeed();
    if failed {
        trace.finish_failure();
        anyhow::bail!("one or more Gateway startup maintenance stages failed");
    } else {
        trace.finish_success();
    }
    Ok(())
}

pub(crate) async fn spawn_thread_episodic_workspace_capsule_refill(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    thread_episodic_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    thread_episodic_workspace_vector_search_configs: BTreeMap<
        String,
        GatewayThreadEpisodicVectorSearchConfig,
    >,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    refill_supervisor: Arc<ThreadEpisodicWorkspaceRefillSupervisor>,
) {
    let task_refill_supervisor = refill_supervisor.clone();
    refill_supervisor
        .spawn_global_settings_refill(move |cancellation| async move {
            run_thread_episodic_workspace_capsule_refill(
                crud_store,
                thread_episodic_storage_root,
                thread_episodic_vector_search_config,
                thread_episodic_workspace_vector_search_configs,
                provider_registry,
                runtime_home,
                refill_status_sender,
                task_refill_supervisor,
                None,
                RefillOwner::Settings,
                cancellation,
            )
            .await;
        })
        .await;
}

pub(crate) async fn spawn_thread_episodic_workspace_capsule_refill_for_workspace(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    workspace_id: String,
    workspace_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    default_thread_episodic_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    thread_episodic_workspace_vector_search_configs: BTreeMap<
        String,
        GatewayThreadEpisodicVectorSearchConfig,
    >,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    refill_supervisor: Arc<ThreadEpisodicWorkspaceRefillSupervisor>,
) {
    let refill_lease = refill_supervisor
        .begin_settings(workspace_id.as_str())
        .await;
    let cancellation = refill_lease.cancellation();
    if cancellation.is_cancelled() {
        return;
    }
    let _handle = tokio::spawn(async move {
        let _refill_lease = refill_lease;
        run_thread_episodic_workspace_capsule_refill_for_workspace(
            crud_store,
            thread_episodic_storage_root,
            workspace_id,
            workspace_vector_search_config,
            default_thread_episodic_vector_search_config,
            thread_episodic_workspace_vector_search_configs,
            provider_registry,
            runtime_home,
            refill_status_sender,
            cancellation,
        )
        .await;
    });
}

async fn run_thread_episodic_workspace_capsule_refill(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    thread_episodic_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    thread_episodic_workspace_vector_search_configs: BTreeMap<
        String,
        GatewayThreadEpisodicVectorSearchConfig,
    >,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    refill_supervisor: Arc<ThreadEpisodicWorkspaceRefillSupervisor>,
    interrupted_before_unix: Option<i64>,
    owner: RefillOwner,
    cancellation: CancellationToken,
) {
    thread_episodic_workspace_capsule_refill::run(
        crud_store,
        thread_episodic_storage_root,
        thread_episodic_vector_search_config,
        thread_episodic_workspace_vector_search_configs,
        provider_registry,
        runtime_home,
        refill_status_sender,
        refill_supervisor,
        interrupted_before_unix,
        owner,
        cancellation,
    )
    .await;
}

async fn run_thread_episodic_workspace_capsule_refill_for_workspace(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    workspace_id: String,
    workspace_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    default_thread_episodic_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    thread_episodic_workspace_vector_search_configs: BTreeMap<
        String,
        GatewayThreadEpisodicVectorSearchConfig,
    >,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    cancellation: CancellationToken,
) {
    thread_episodic_workspace_capsule_refill::run_workspace(
        crud_store,
        thread_episodic_storage_root,
        workspace_id,
        workspace_vector_search_config,
        default_thread_episodic_vector_search_config,
        thread_episodic_workspace_vector_search_configs,
        provider_registry,
        runtime_home,
        refill_status_sender,
        None,
        cancellation,
    )
    .await;
}

#[cfg(test)]
pub(crate) use task_event_fanout_cursor_backfill::backfill_once as backfill_task_event_fanout_cursors_once;
#[cfg(test)]
pub(crate) use timeline_pagination_backfill::backfill_once as backfill_timeline_pagination_once;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn workspace_refill_supervisor_cancels_and_waits_for_active_lease() {
        let supervisor = Arc::new(ThreadEpisodicWorkspaceRefillSupervisor::default());
        let lease = supervisor.begin_settings("workspace-a").await;
        let cancellation = lease.cancellation();
        let task = tokio::spawn(async move {
            cancellation.cancelled().await;
            drop(lease);
        });

        supervisor.cancel_and_wait("workspace-a").await;

        task.await.expect("refill task");
        assert!(supervisor.active.lock().await.is_empty());
    }

    #[tokio::test]
    async fn beginning_replacement_waits_for_previous_refill_to_stop() {
        let supervisor = Arc::new(ThreadEpisodicWorkspaceRefillSupervisor::default());
        let previous = supervisor.begin_settings("workspace-a").await;
        let previous_cancellation = previous.cancellation();
        let previous_task = tokio::spawn(async move {
            previous_cancellation.cancelled().await;
            drop(previous);
        });

        let replacement = supervisor.begin_settings("workspace-a").await;

        previous_task.await.expect("previous refill task");
        assert!(!replacement.cancellation().is_cancelled());
        drop(replacement);
        supervisor.cancel_and_wait("workspace-a").await;
    }

    #[tokio::test]
    async fn startup_refill_cannot_replace_newer_settings_owned_workspace() {
        let supervisor = ThreadEpisodicWorkspaceRefillSupervisor::default();

        supervisor.cancel_and_wait("workspace-a").await;

        assert!(supervisor.begin_startup("workspace-a").await.is_none());
        assert!(supervisor.begin_startup("workspace-b").await.is_some());
    }

    #[tokio::test]
    async fn global_settings_reservation_fences_all_startup_workspaces_at_once() {
        let supervisor = ThreadEpisodicWorkspaceRefillSupervisor::default();
        supervisor
            .reserve_settings_workspaces(&["workspace-a".to_owned(), "workspace-b".to_owned()])
            .await;

        assert!(supervisor.begin_startup("workspace-a").await.is_none());
        assert!(supervisor.begin_startup("workspace-b").await.is_none());
    }

    #[tokio::test]
    async fn newer_global_settings_refill_cancels_and_joins_older_generation() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::sync::Notify;

        let supervisor = Arc::new(ThreadEpisodicWorkspaceRefillSupervisor::default());
        let first_started = Arc::new(Notify::new());
        let first_stopped = Arc::new(AtomicBool::new(false));
        let started = first_started.clone();
        let stopped = first_stopped.clone();
        supervisor
            .spawn_global_settings_refill(move |cancellation| async move {
                started.notify_one();
                cancellation.cancelled().await;
                stopped.store(true, Ordering::Release);
            })
            .await;
        first_started.notified().await;

        supervisor.spawn_global_settings_refill(|_| async {}).await;

        assert!(first_stopped.load(Ordering::Acquire));
        supervisor.shutdown().await;
    }
}
