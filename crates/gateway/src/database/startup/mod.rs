mod agent_diff_event_compaction;
mod cli_runtime_native_event_compaction;
mod task_anchor_backfill;
pub(crate) mod thread_episodic_workspace_capsule_refill;
mod timeline_pagination_backfill;
mod turn_item_attempt_payload_compaction;
mod turn_permission_profile_backfill;
mod zstd_payload_compression;

use pioneer_config::GatewayThreadEpisodicVectorSearchConfig;
use pioneer_crud::CrudStore;
use pioneer_provider::ProviderRegistry;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use thread_episodic_workspace_capsule_refill::ThreadEpisodicWorkspaceCapsuleRefillStatusSender;

pub(crate) fn spawn(
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
) {
    let _handle = tokio::spawn(async move {
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
        )
        .await;
    });
}

pub(crate) fn spawn_thread_episodic_workspace_capsule_refill_for_workspace(
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
) {
    let _handle = tokio::spawn(async move {
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
) {
    thread_episodic_workspace_capsule_refill::run(
        crud_store,
        thread_episodic_storage_root,
        thread_episodic_vector_search_config,
        thread_episodic_workspace_vector_search_configs,
        provider_registry,
        runtime_home,
        refill_status_sender,
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
    )
    .await;
}

#[cfg(test)]
pub(crate) use task_anchor_backfill::backfill_once as backfill_task_anchors_once;
#[cfg(test)]
pub(crate) use timeline_pagination_backfill::backfill_once as backfill_timeline_pagination_once;
