mod agent_diff_event_compaction;
mod cli_runtime_native_event_compaction;
mod task_anchor_backfill;
pub(crate) mod thread_episodic_workspace_capsule_refill;
mod timeline_pagination_backfill;
mod turn_item_attempt_payload_compaction;
mod turn_permission_profile_backfill;
mod zstd_payload_compression;

use pioneer_config::{
    GatewayThreadEpisodicVectorProviderConfig, GatewayThreadEpisodicVectorSearchConfig,
};
use pioneer_crud::CrudStore;
use pioneer_provider::ProviderRegistry;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};

pub(crate) fn spawn(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    thread_episodic_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
) {
    let _handle = tokio::spawn(async move {
        task_anchor_backfill::run(crud_store.as_ref()).await;
        turn_permission_profile_backfill::run(crud_store.as_ref()).await;
        timeline_pagination_backfill::run(crud_store.as_ref()).await;
        agent_diff_event_compaction::run(crud_store.as_ref()).await;
        cli_runtime_native_event_compaction::run(crud_store.as_ref()).await;
        turn_item_attempt_payload_compaction::run(crud_store.as_ref()).await;
        zstd_payload_compression::run(crud_store.as_ref()).await;
        run_thread_episodic_workspace_capsule_refill(
            crud_store,
            thread_episodic_storage_root,
            thread_episodic_vector_search_config,
            provider_registry,
            runtime_home,
        )
        .await;
    });
}

pub(crate) fn spawn_thread_episodic_workspace_capsule_refill(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    thread_episodic_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
) {
    let _handle = tokio::spawn(async move {
        run_thread_episodic_workspace_capsule_refill(
            crud_store,
            thread_episodic_storage_root,
            thread_episodic_vector_search_config,
            provider_registry,
            runtime_home,
        )
        .await;
    });
}

async fn run_thread_episodic_workspace_capsule_refill(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    thread_episodic_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
) {
    match crate::thread_episodic_embedding::ensure_local_embedding_model_downloaded_if_needed(
        runtime_home.as_path(),
        &thread_episodic_vector_search_config,
    )
    .await
    {
        Ok(true) => info!(
            model = %thread_episodic_vector_search_config.local_model.as_deref().unwrap_or(""),
            "local embedding model downloaded before thread episodic refill"
        ),
        Ok(false) => {}
        Err(error) => {
            warn!(
                error = %error,
                "failed to download local embedding model before thread episodic refill"
            );
            return;
        }
    }

    if !local_embedding_model_ready_for_refill(
        runtime_home.as_path(),
        &thread_episodic_vector_search_config,
    ) {
        info!(
            model = %thread_episodic_vector_search_config.local_model.as_deref().unwrap_or(""),
            "thread episodic vector refill is waiting for local embedding model files"
        );
        return;
    }

    thread_episodic_workspace_capsule_refill::run(
        crud_store,
        thread_episodic_storage_root,
        thread_episodic_vector_search_config,
        provider_registry,
        runtime_home,
    )
    .await;
}

fn local_embedding_model_ready_for_refill(
    runtime_home: &std::path::Path,
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> bool {
    if !config.enabled || config.provider != Some(GatewayThreadEpisodicVectorProviderConfig::Local)
    {
        return true;
    }

    let Some(model) = config
        .local_model
        .as_deref()
        .map(str::trim)
        .filter(|model| !model.is_empty())
    else {
        return false;
    };

    crate::thread_episodic_embedding::local_embedding_model_files(runtime_home, model)
        .map(|files| files.model_path.exists() && files.tokenizer_path.exists())
        .unwrap_or(false)
}

#[cfg(test)]
pub(crate) use task_anchor_backfill::backfill_once as backfill_task_anchors_once;
#[cfg(test)]
pub(crate) use timeline_pagination_backfill::backfill_once as backfill_timeline_pagination_once;
