mod agent_diff_event_compaction;
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
mod turn_permission_profile_backfill;
mod zstd_payload_compression;

use pioneer_config::GatewayThreadEpisodicVectorSearchConfig;
use pioneer_crud::CrudStore;
use pioneer_provider::ProviderRegistry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use thread_episodic_workspace_capsule_refill::ThreadEpisodicWorkspaceCapsuleRefillStatusSender;
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(crate) struct ThreadEpisodicWorkspaceRefillSupervisor {
    operation: Mutex<()>,
    active: Mutex<HashMap<String, ActiveWorkspaceRefill>>,
    settings_owned_workspaces: Mutex<HashSet<String>>,
}

struct ActiveWorkspaceRefill {
    cancellation: CancellationToken,
    completed: watch::Receiver<bool>,
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

impl ThreadEpisodicWorkspaceRefillSupervisor {
    pub(crate) async fn begin(&self, workspace_id: &str) -> ThreadEpisodicWorkspaceRefillLease {
        let _operation = self.operation.lock().await;
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

pub(crate) fn spawn(
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
) {
    let interrupted_before_unix = chrono::Utc::now().timestamp();
    let _handle = tokio::spawn(async move {
        turn_event_projection_stream_state_backfill::run(crud_store.as_ref()).await;
        task_event_fanout_cursor_backfill::run(crud_store.as_ref()).await;
        authorization_legacy_backfill::run(crud_store.as_ref()).await;
        stable_skill_id_backfill::run(crud_store.as_ref(), message_processor.as_ref()).await;
        task_anchor_backfill::run(crud_store.as_ref()).await;
        turn_permission_profile_backfill::run(crud_store.as_ref(), runtime_home.as_path()).await;
        timeline_pagination_backfill::run(crud_store.as_ref()).await;
        agent_diff_event_compaction::run(crud_store.as_ref()).await;
        cli_runtime_native_event_compaction::run(crud_store.as_ref()).await;
        turn_item_attempt_payload_compaction::run(crud_store.as_ref()).await;
        zstd_payload_compression::run(crud_store.as_ref()).await;
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
        )
        .await;
    });
}

pub(crate) fn spawn_thread_episodic_workspace_capsule_refill(
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
    let _handle = tokio::spawn(async move {
        run_thread_episodic_workspace_capsule_refill(
            crud_store,
            thread_episodic_storage_root,
            thread_episodic_vector_search_config,
            thread_episodic_workspace_vector_search_configs,
            provider_registry,
            runtime_home,
            refill_status_sender,
            refill_supervisor,
            None,
        )
        .await;
    });
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
    let refill_lease = refill_supervisor.begin(workspace_id.as_str()).await;
    let cancellation = refill_lease.cancellation();
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
        let lease = supervisor.begin("workspace-a").await;
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
        let previous = supervisor.begin("workspace-a").await;
        let previous_cancellation = previous.cancellation();
        let previous_task = tokio::spawn(async move {
            previous_cancellation.cancelled().await;
            drop(previous);
        });

        let replacement = supervisor.begin("workspace-a").await;

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
}
