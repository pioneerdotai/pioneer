use crate::thread_episodic::{
    ConfigBackedThreadEpisodicIndexEmbeddingProviderResolver,
    RuntimeVectorThreadEpisodicIndexPayloadProvider, StoreThreadEpisodicIndexPayloadProvider,
    StoreThreadEpisodicIngestor, ThreadEpisodicIndexEmbeddingProviderResolver,
    ThreadEpisodicIndexExecutorConfig, ThreadEpisodicIndexPayloadProvider,
    ThreadEpisodicIndexResolutionError, ThreadEpisodicIndexResolutionFailureKind,
    ThreadEpisodicResolvedIndexRequest, ThreadEpisodicThreadReindexRequest,
    memvid_stats_reach_capacity_threshold,
};
use anyhow::{Context, Result, anyhow, bail};
use fs4::{FileExt as Fs4FileExt, TryLockError as Fs4TryLockError};
use pioneer_config::{
    GatewayThreadEpisodicVectorProviderConfig, GatewayThreadEpisodicVectorSearchConfig,
};
use pioneer_crud::{
    CrudStore, PROJECTION_META_STATUS_BACKFILLING, PROJECTION_META_STATUS_COMPLETE,
    PROJECTION_META_STATUS_FAILED, PROJECTION_META_STATUS_PENDING, ProjectionMetaConfigRecord,
    ProjectionMetaRecord, THREAD_EPISODIC_USER_DELETED_ERROR, THREAD_EPISODIC_USER_EXCLUDED_ERROR,
    ThreadEpisodicCapsuleCapacityUpdate, ThreadEpisodicCapsuleWriteState,
    ThreadEpisodicIndexAttemptOutcome, ThreadEpisodicIndexJobCompletionUpdate,
    ThreadEpisodicIndexJobFailureUpdate, ThreadEpisodicIndexJobRecord,
    ThreadEpisodicItemIndexedUpdate, ThreadEpisodicRefillThread, find_projection_meta,
    list_projection_meta_by_key_prefix, update_projection_meta_status,
    upsert_projection_meta_with_config,
};
use pioneer_memory::{
    MemvidThreadEpisodicBackend, ThreadEpisodicEmbeddingProvider, ThreadEpisodicMemvidBackend,
    ThreadEpisodicMemvidFailureKind, ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidStats,
    ThreadEpisodicWorkspaceOwnership, lock_thread_episodic_workspace,
    remove_thread_episodic_capsule_file, thread_episodic_storage_uri_from_path,
};
use pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus;
use pioneer_provider::ProviderRegistry;
use sea_orm::ConnectionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(test)]
pub(crate) use tests::make_refill_history_unavailable;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub(crate) const THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY: &str =
    "thread_episodic_workspace_capsule_refill";
pub(crate) const THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION: i64 = 1;
const THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_CONFIG_VERSION: u32 = 1;

const REFILL_ENQUEUE_BATCH_SIZE: u64 = 1024;
const REFILL_EXECUTOR_MAX_BATCHES: u64 = 100_000;
const REFILL_JOB_CLAIM_LIMIT: u64 = 1;
const REFILL_LOCK_FILE_NAME: &str = ".thread_episodic_workspace_capsule_refill.lock";
const REFILL_INDEX_ERROR_MAX_CHARS: usize = 512;
const LEGACY_REFILL_WORKSPACE_ID: &str = "__default__";
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ThreadEpisodicWorkspaceCapsuleRefillStatusEvent {
    pub(crate) workspace_id: String,
    pub(crate) status: GatewayThreadEpisodicVectorRefillStatus,
    pub(crate) local_model_status:
        Option<pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus>,
    pub(crate) downloaded_bytes: Option<u64>,
    pub(crate) total_bytes: Option<u64>,
}

pub(crate) type ThreadEpisodicWorkspaceCapsuleRefillStatusSender =
    tokio::sync::broadcast::Sender<ThreadEpisodicWorkspaceCapsuleRefillStatusEvent>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget {
    config_hash: String,
    payload: ThreadEpisodicWorkspaceCapsuleRefillProjectionPayload,
    payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct ThreadEpisodicWorkspaceCapsuleRefillProjectionPayload {
    schema_version: u32,
    vector_search_enabled: bool,
    provider: Option<String>,
    model: Option<String>,
    dimension: Option<u32>,
    normalized: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    preparation_version: Option<String>,
    config_hash: String,
}

impl ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget {
    pub(crate) fn lexical_only() -> Self {
        Self::from_vector_search_config(&GatewayThreadEpisodicVectorSearchConfig::default())
    }

    pub(crate) fn from_vector_search_config(
        config: &GatewayThreadEpisodicVectorSearchConfig,
    ) -> Self {
        let vector_search_enabled = config.has_selected_embedding_model();
        let provider = vector_search_enabled.then(|| {
            config
                .provider
                .map(crate::settings::vector_provider_identity_name)
                .unwrap_or("missing")
                .to_owned()
        });
        let model = vector_search_enabled.then(|| projection_embedding_model(config));
        let dimension = vector_search_enabled
            .then(|| crate::settings::resolved_vector_embedding_dimension(config))
            .flatten();
        Self::from_projection_parts(
            vector_search_enabled,
            provider,
            model,
            dimension,
            vector_search_enabled.then_some(config.embedding_normalized),
        )
    }

    pub(crate) fn from_embedding_provider(
        provider: &dyn ThreadEpisodicEmbeddingProvider,
    ) -> Result<Self> {
        let dimension = u32::try_from(provider.dimension()).with_context(|| {
            format!(
                "embedding dimension {} for `{}`/`{}` does not fit projection metadata",
                provider.dimension(),
                provider.provider_id(),
                provider.model()
            )
        })?;
        Ok(Self::from_projection_parts(
            true,
            Some(provider.provider_id().to_owned()),
            Some(provider.model().to_owned()),
            Some(dimension),
            Some(provider.normalized()),
        ))
    }

    pub(crate) fn from_embedding_identity(
        identity: &pioneer_memory::ThreadEpisodicEmbeddingIdentity,
    ) -> Result<Self> {
        Ok(Self::from_projection_parts(
            true,
            Some(identity.provider_id.clone()),
            Some(identity.model.clone()),
            Some(u32::try_from(identity.dimension)?),
            Some(identity.normalized),
        ))
    }

    fn from_projection_parts(
        vector_search_enabled: bool,
        provider: Option<String>,
        model: Option<String>,
        dimension: Option<u32>,
        normalized: Option<bool>,
    ) -> Self {
        let config_hash =
            crate::settings::thread_episodic_vector_projection_identity_hash_for_parts(
                vector_search_enabled,
                provider.as_deref().unwrap_or(if vector_search_enabled {
                    "missing"
                } else {
                    "disabled"
                }),
                model.as_deref().unwrap_or(""),
                dimension,
                normalized.unwrap_or(false),
            );
        let preparation_version = (vector_search_enabled && provider.as_deref() == Some("local"))
            .then(|| {
                pioneer_provider::providers::local_embedding_model_info(
                    model.as_deref().unwrap_or(""),
                )
                .and_then(|info| info.preparation_version())
                .map(str::to_owned)
            })
            .flatten();
        let payload = ThreadEpisodicWorkspaceCapsuleRefillProjectionPayload {
            schema_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_CONFIG_VERSION,
            vector_search_enabled,
            provider,
            model,
            dimension,
            normalized,
            preparation_version,
            config_hash: config_hash.clone(),
        };
        let payload_json = serde_json::to_string(&payload)
            .expect("thread episodic refill projection payload should serialize");
        Self {
            config_hash,
            payload,
            payload_json,
        }
    }

    pub(crate) fn meta_config_record(&self) -> ProjectionMetaConfigRecord {
        ProjectionMetaConfigRecord {
            projection_config_hash: Some(self.config_hash.clone()),
            projection_config_json: Some(self.payload_json.clone()),
        }
    }

    fn matches_projection_meta(&self, meta: &ProjectionMetaRecordLike<'_>) -> bool {
        if meta.projection_config_hash != Some(self.config_hash.as_str()) {
            return false;
        }
        let Some(payload_json) = meta.projection_config_json else {
            return false;
        };
        serde_json::from_str::<ThreadEpisodicWorkspaceCapsuleRefillProjectionPayload>(payload_json)
            .is_ok_and(|payload| payload == self.payload)
    }

    fn matches_projection_meta_selection(&self, meta: &ProjectionMetaRecordLike<'_>) -> bool {
        let Some(payload_json) = meta.projection_config_json else {
            return false;
        };
        serde_json::from_str::<ThreadEpisodicWorkspaceCapsuleRefillProjectionPayload>(payload_json)
            .is_ok_and(|payload| {
                payload.schema_version == self.payload.schema_version
                    && payload.vector_search_enabled == self.payload.vector_search_enabled
                    && payload.provider == self.payload.provider
                    && payload.model == self.payload.model
                    && payload.normalized == self.payload.normalized
                    && payload.preparation_version == self.payload.preparation_version
                    && match self.payload.dimension {
                        Some(dimension) => payload.dimension == Some(dimension),
                        None => true,
                    }
            })
    }

    fn requires_embedding_provider(&self) -> bool {
        self.payload.vector_search_enabled
    }

    fn matches_embedding_provider_selection(
        &self,
        provider: &dyn ThreadEpisodicEmbeddingProvider,
    ) -> bool {
        if !self.payload.vector_search_enabled {
            return true;
        }
        self.payload.provider.as_deref() == Some(provider.provider_id())
            && self.payload.model.as_deref() == Some(provider.model())
            && self.payload.normalized == Some(provider.normalized())
    }

    fn matches_embedding_provider(&self, provider: &dyn ThreadEpisodicEmbeddingProvider) -> bool {
        if !self.payload.vector_search_enabled {
            return true;
        }
        self.matches_embedding_provider_selection(provider)
            && self
                .payload
                .dimension
                .is_some_and(|dimension| usize::try_from(dimension) == Ok(provider.dimension()))
    }
}

struct ProjectionMetaRecordLike<'a> {
    projection_config_hash: Option<&'a str>,
    projection_config_json: Option<&'a str>,
}

fn projection_embedding_model(config: &GatewayThreadEpisodicVectorSearchConfig) -> String {
    config
        .selected_embedding_model()
        .unwrap_or_default()
        .to_owned()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ThreadEpisodicWorkspaceCapsuleRefillSummary {
    pub(crate) skipped: bool,
    pub(crate) lock_contended: bool,
    pub(crate) resumed: bool,
    pub(crate) capsule_files_deleted: u64,
    pub(crate) capsule_files_missing: u64,
    pub(crate) non_file_storage_uris: u64,
    pub(crate) capsule_rows_deleted: u64,
    pub(crate) item_rows_deleted: u64,
    pub(crate) exclusion_rows_deleted: u64,
    pub(crate) index_jobs_replaced: u64,
    pub(crate) thread_directory_rows_deleted: u64,
    pub(crate) workspace_count: usize,
    pub(crate) source_threads_reindexed: usize,
    pub(crate) source_threads_failed: usize,
    pub(crate) source_thread_count: i64,
    pub(crate) source_turn_count: i64,
    pub(crate) source_turn_item_count: i64,
    pub(crate) refill_jobs_enqueued: usize,
    pub(crate) executor_batches: u64,
    pub(crate) completed_jobs: usize,
    pub(crate) failed_retryable_jobs: usize,
    pub(crate) failed_terminal_jobs: usize,
    pub(crate) has_incomplete_jobs: bool,
}

pub(super) async fn run(
    crud_store: Arc<CrudStore>,
    indexing_enabled: bool,
    executor_config: ThreadEpisodicIndexExecutorConfig,
    thread_episodic_storage_root: PathBuf,
    vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    workspace_vector_search_configs: BTreeMap<String, GatewayThreadEpisodicVectorSearchConfig>,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    refill_supervisor: Arc<super::ThreadEpisodicWorkspaceRefillSupervisor>,
    owner: super::RefillOwner,
    supervisor_cancellation: CancellationToken,
) {
    if !indexing_enabled || supervisor_cancellation.is_cancelled() {
        return;
    }
    let crud_store = Arc::new(crud_store.with_maintenance_access());
    let workspace_ids = match tokio::select! {
        _ = supervisor_cancellation.cancelled() => return,
        workspace_ids = crud_store.list_thread_episodic_refill_workspace_ids() => workspace_ids,
    } {
        Ok(workspace_ids) => workspace_ids,
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "thread episodic workspace capsule refill failed to list source workspaces"
            );
            return;
        }
    };
    if matches!(owner, super::RefillOwner::Settings) {
        // Claim the complete settings generation before processing its first
        // workspace. Otherwise an older startup refill could begin workspace B
        // while the settings task is still rebuilding workspace A.
        refill_supervisor
            .reserve_settings_workspaces(workspace_ids.as_slice())
            .await;
    }
    for workspace_id in workspace_ids {
        if supervisor_cancellation.is_cancelled() {
            return;
        }
        let refill_lease = match owner {
            super::RefillOwner::Startup => {
                let Some(refill_lease) =
                    refill_supervisor.begin_startup(workspace_id.as_str()).await
                else {
                    continue;
                };
                refill_lease
            }
            super::RefillOwner::Settings => {
                refill_supervisor
                    .begin_settings(workspace_id.as_str())
                    .await
            }
        };
        let lease_cancellation = refill_lease.cancellation();
        let cancellation = supervisor_cancellation.child_token();
        let relay_cancellation = cancellation.clone();
        let cancellation_relay = tokio::spawn(async move {
            lease_cancellation.cancelled().await;
            relay_cancellation.cancel();
        });
        let workspace_vector_search_config = effective_workspace_vector_search_config(
            &vector_search_config,
            &workspace_vector_search_configs,
            workspace_id.as_str(),
        );
        run_workspace(
            crud_store.clone(),
            indexing_enabled,
            executor_config,
            thread_episodic_storage_root.clone(),
            workspace_id,
            workspace_vector_search_config,
            vector_search_config.clone(),
            workspace_vector_search_configs.clone(),
            provider_registry.clone(),
            runtime_home.clone(),
            refill_status_sender.clone(),
            cancellation,
        )
        .await;
        cancellation_relay.abort();
        let _ = cancellation_relay.await;
        drop(refill_lease);
    }
}

pub(super) async fn run_workspace(
    crud_store: Arc<CrudStore>,
    indexing_enabled: bool,
    executor_config: ThreadEpisodicIndexExecutorConfig,
    thread_episodic_storage_root: PathBuf,
    workspace_id: String,
    workspace_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    default_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    workspace_vector_search_configs: BTreeMap<String, GatewayThreadEpisodicVectorSearchConfig>,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    cancellation: CancellationToken,
) {
    let crud_store = Arc::new(crud_store.with_maintenance_access());
    if cancellation.is_cancelled() || !indexing_enabled {
        return;
    }
    if workspace_vector_search_config.enabled
        && !workspace_vector_search_config.has_selected_embedding_model()
    {
        return;
    }

    let projection_target =
        ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
            &workspace_vector_search_config,
        );
    match tokio::select! {
        _ = cancellation.cancelled() => return,
        result = inspect_current_refill_work(
            crud_store.as_ref(),
            &workspace_id,
            &projection_target
        ) => result,
    } {
        Ok(Some(summary)) if summary.skipped => return,
        Ok(_) => {}
        Err(error) => {
            warn!(error = %format!("{error:#}"), "failed to inspect episodic refill work");
            return;
        }
    }

    let local_model_status = local_embedding_model_status_for_refill(
        runtime_home.as_path(),
        &workspace_vector_search_config,
    );
    let download_progress_observer = local_model_status
        .filter(|status| {
            *status != pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Installed
        })
        .map(|_| {
            let progress_sender = refill_status_sender.clone();
            let progress_workspace_id = workspace_id.clone();
            Arc::new(
                move |progress: crate::thread_episodic_embedding::LocalEmbeddingModelDownloadProgress| {
                    notify_local_model_download_progress(
                        progress_sender.as_ref(),
                        progress_workspace_id.as_str(),
                        progress,
                    );
                },
            ) as crate::thread_episodic_embedding::LocalEmbeddingModelDownloadProgressObserver
        });
    if download_progress_observer.is_some() {
        notify_local_model_download_progress(
            refill_status_sender.as_ref(),
            workspace_id.as_str(),
            crate::thread_episodic_embedding::LocalEmbeddingModelDownloadProgress {
                downloaded_bytes: 0,
                total_bytes: None,
            },
        );
    }

    let local_model_ready = tokio::select! {
        _ = cancellation.cancelled() => return,
        ready = ensure_local_embedding_model_ready_for_workspace_refill(
            runtime_home.as_path(),
            workspace_id.as_str(),
            &workspace_vector_search_config,
            download_progress_observer,
        ) => ready,
    };
    if !local_model_ready {
        if local_embedding_model_status_for_refill(
            runtime_home.as_path(),
            &workspace_vector_search_config,
        ) == Some(pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Failed)
        {
            notify_local_model_status(
                refill_status_sender.as_ref(),
                workspace_id.as_str(),
                GatewayThreadEpisodicVectorRefillStatus::Failed,
                pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Failed,
            );
        }
        return;
    }
    if local_model_status.is_some_and(|status| {
        status != pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Installed
    }) && local_embedding_model_status_for_refill(
        runtime_home.as_path(),
        &workspace_vector_search_config,
    ) == Some(pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Installed)
    {
        notify_local_model_status(
            refill_status_sender.as_ref(),
            workspace_id.as_str(),
            GatewayThreadEpisodicVectorRefillStatus::Running,
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Installed,
        );
    }

    let projection_target =
        ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
            &workspace_vector_search_config,
        );
    let embedding_provider_resolver = projection_target.requires_embedding_provider().then(|| {
        let resolver = Arc::new(
            ConfigBackedThreadEpisodicIndexEmbeddingProviderResolver::new(
                provider_registry,
                runtime_home,
                default_vector_search_config,
            ),
        );
        resolver.apply_workspace_configs(workspace_vector_search_configs);
        resolver as Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>
    });
    let ownership_acquired = std::sync::atomic::AtomicBool::new(false);
    let refill = tokio::select! { biased;
        _ = cancellation.cancelled() => None,
        refill = refill_once_with_projection_resolver_and_config(
            crud_store,
            thread_episodic_storage_root.as_path(),
            workspace_id.as_str(),
            projection_target,
            embedding_provider_resolver,
            refill_status_sender.as_ref(),
            executor_config,
            Some(&ownership_acquired),
            #[cfg(test)] None,
        ) => Some(refill),
    };
    // Dropping the refill future cancels DB/provider admission. Its blocking
    // writes retain ownership; the completion lease must outlive those writes.
    drop(join_owned_refill_execution(&workspace_id, &ownership_acquired).await);
    let Some(refill) = refill else {
        return;
    };
    match refill {
        Ok(summary) if summary.skipped && summary.lock_contended => {
            info!(
                "thread episodic workspace capsule refill skipped because another process holds the refill lock"
            );
        }
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
                resumed = summary.resumed,
                capsule_files_deleted = summary.capsule_files_deleted,
                capsule_files_missing = summary.capsule_files_missing,
                non_file_storage_uris = summary.non_file_storage_uris,
                capsule_rows_deleted = summary.capsule_rows_deleted,
                item_rows_deleted = summary.item_rows_deleted,
                exclusion_rows_deleted = summary.exclusion_rows_deleted,
                index_jobs_replaced = summary.index_jobs_replaced,
                thread_directory_rows_deleted = summary.thread_directory_rows_deleted,
                workspace_id = %workspace_id,
                workspace_count = summary.workspace_count,
                source_threads_reindexed = summary.source_threads_reindexed,
                source_threads_failed = summary.source_threads_failed,
                refill_jobs_enqueued = summary.refill_jobs_enqueued,
                executor_batches = summary.executor_batches,
                completed_jobs = summary.completed_jobs,
                "thread episodic workspace capsule refill completed"
            );
        }
        Err(error) => {
            warn!(
                workspace_id = %workspace_id,
                error = %format!("{error:#}"),
                "thread episodic workspace capsule refill failed at startup"
            );
        }
    }
}

fn effective_workspace_vector_search_config(
    default_config: &GatewayThreadEpisodicVectorSearchConfig,
    workspace_configs: &BTreeMap<String, GatewayThreadEpisodicVectorSearchConfig>,
    workspace_id: &str,
) -> GatewayThreadEpisodicVectorSearchConfig {
    workspace_configs
        .get(workspace_id)
        .cloned()
        .unwrap_or_else(|| default_config.clone())
}

async fn ensure_local_embedding_model_ready_for_workspace_refill(
    runtime_home: &Path,
    workspace_id: &str,
    config: &GatewayThreadEpisodicVectorSearchConfig,
    progress_observer: Option<
        crate::thread_episodic_embedding::LocalEmbeddingModelDownloadProgressObserver,
    >,
) -> bool {
    match crate::thread_episodic_embedding::ensure_local_embedding_model_downloaded_if_needed(
        runtime_home,
        config,
        progress_observer,
    )
    .await
    {
        Ok(true) => info!(
            model = %selected_local_embedding_model(config).unwrap_or(""),
            workspace_id = %workspace_id,
            "local embedding model downloaded before thread episodic workspace refill"
        ),
        Ok(false) => {}
        Err(error) => {
            warn!(
                workspace_id = %workspace_id,
                error = %error,
                "failed to download local embedding model before thread episodic workspace refill"
            );
            return false;
        }
    }

    if !local_embedding_model_ready_for_refill(runtime_home, config) {
        info!(
            model = %selected_local_embedding_model(config).unwrap_or(""),
            workspace_id = %workspace_id,
            "thread episodic vector refill is waiting for local embedding model files"
        );
        return false;
    }

    true
}

fn local_embedding_model_ready_for_refill(
    runtime_home: &Path,
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> bool {
    if !config.enabled || config.provider != Some(GatewayThreadEpisodicVectorProviderConfig::Local)
    {
        return true;
    }

    let Some(model) = selected_local_embedding_model(config) else {
        return false;
    };

    crate::thread_episodic_embedding::local_embedding_model_files(runtime_home, model)
        .map(|files| files.model_path.exists() && files.tokenizer_path.exists())
        .unwrap_or(false)
}

fn selected_local_embedding_model(
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> Option<&str> {
    config
        .model
        .as_deref()
        .or(config.local_model.as_deref())
        .map(str::trim)
        .filter(|model| !model.is_empty())
}

fn local_embedding_model_status_for_refill(
    runtime_home: &Path,
    config: &GatewayThreadEpisodicVectorSearchConfig,
) -> Option<pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus> {
    if !config.enabled || config.provider != Some(GatewayThreadEpisodicVectorProviderConfig::Local)
    {
        return None;
    }

    selected_local_embedding_model(config).map(|model| {
        crate::thread_episodic_embedding::local_embedding_model_status(
            runtime_home,
            true,
            Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local),
            model,
        )
    })
}

#[allow(dead_code)]
pub(crate) async fn refill_once(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    refill_once_with_projection(
        crud_store,
        thread_episodic_storage_root,
        ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
        None,
    )
    .await
}

pub(crate) async fn refill_once_with_projection(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    projection_target: ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    embedding_provider: Option<Arc<dyn ThreadEpisodicEmbeddingProvider>>,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let workspace_id = first_refill_workspace_id_or_legacy(crud_store.as_ref()).await?;
    refill_once_with_workspace_projection(
        crud_store,
        thread_episodic_storage_root,
        workspace_id.as_str(),
        projection_target,
        embedding_provider,
    )
    .await
}

pub(crate) async fn refill_once_with_workspace_projection(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    workspace_id: &str,
    projection_target: ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    embedding_provider: Option<Arc<dyn ThreadEpisodicEmbeddingProvider>>,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let embedding_provider_resolver = projection_target
        .requires_embedding_provider()
        .then(|| {
            embedding_provider.map(|provider| {
                Arc::new(FixedThreadEpisodicIndexEmbeddingProviderResolver::new(
                    Some(provider),
                )) as Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>
            })
        })
        .flatten();
    refill_once_with_projection_resolver(
        crud_store,
        thread_episodic_storage_root,
        workspace_id,
        projection_target,
        embedding_provider_resolver,
        None,
    )
    .await
}

pub(crate) async fn refill_once_with_projection_resolver(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    workspace_id: &str,
    projection_target: ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    embedding_provider_resolver: Option<Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>>,
    refill_status_sender: Option<&ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    refill_once_with_projection_resolver_and_config(
        crud_store,
        thread_episodic_storage_root,
        workspace_id,
        projection_target,
        embedding_provider_resolver,
        refill_status_sender,
        ThreadEpisodicIndexExecutorConfig::default(),
        None,
        #[cfg(test)]
        None,
    )
    .await
}

// Call after the execution future has completed or been dropped. A receipt
// prevents cancellation before admission from waiting for another owner's work.
// This joins only real FS completion, outside capacity, and performs no DB cleanup.
pub(crate) async fn join_owned_refill_execution(
    workspace_id: &str,
    ownership_acquired: &std::sync::atomic::AtomicBool,
) -> Option<ThreadEpisodicWorkspaceOwnership> {
    if ownership_acquired.load(std::sync::atomic::Ordering::Acquire) {
        Some(lock_thread_episodic_workspace(workspace_id).await)
    } else {
        None
    }
}

pub(crate) async fn refill_once_with_projection_resolver_and_config(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    workspace_id: &str,
    mut projection_target: ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    mut embedding_provider_resolver: Option<Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>>,
    refill_status_sender: Option<&ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    executor_config: ThreadEpisodicIndexExecutorConfig,
    ownership_acquired: Option<&std::sync::atomic::AtomicBool>,
    #[cfg(test)] after_claim: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let db = crud_store.database_connection();
    if let Some(summary) =
        inspect_current_refill_work(crud_store.as_ref(), workspace_id, &projection_target).await?
    {
        if summary.skipped {
            return Ok(summary);
        }
    }
    let Some(_lock_guard) =
        try_acquire_refill_lock_for_workspace(thread_episodic_storage_root, workspace_id)?
    else {
        return Ok(ThreadEpisodicWorkspaceCapsuleRefillSummary {
            skipped: true,
            lock_contended: true,
            ..Default::default()
        });
    };
    // Wait outside DB capacity for ordinary claims/capsule writes to finish.
    // Refusing a live writer must not silently abandon a requested replacement.
    #[cfg(test)]
    if let Some(notice) = REFILL_OWNERSHIP_TEST_NOTICE
        .lock()
        .unwrap()
        .remove(&(thread_episodic_storage_root.to_owned(), false))
    {
        let _ = notice.send(());
    }
    let ownership = lock_thread_episodic_workspace(workspace_id).await;
    if let Some(acquired) = ownership_acquired {
        acquired.store(true, std::sync::atomic::Ordering::Release);
    }
    #[cfg(test)]
    if let Some(notice) = REFILL_OWNERSHIP_TEST_NOTICE
        .lock()
        .unwrap()
        .remove(&(thread_episodic_storage_root.to_owned(), true))
    {
        let _ = notice.send(());
    }
    // Exclusive workspace ownership proves that no ordinary/refill write is
    // still live. This also continues a canceled generation's durable Running
    // attempts when its writer admission was canceled; no timestamp inference.
    crud_store
        .requeue_running_thread_episodic_index_jobs_for_workspace(
            workspace_id,
            chrono::Utc::now().timestamp(),
            executor_config.max_attempts,
        )
        .await?;
    let reset_pending = projection_reset_is_pending(crud_store.as_ref(), workspace_id).await?;
    if !reset_pending
        && refill_is_current_for_workspace_target(
            crud_store.as_ref(),
            workspace_id,
            &projection_target,
        )
        .await?
        && crud_store
            .next_scheduled_thread_episodic_index_job_at_for_workspace(workspace_id)
            .await?
            .is_none()
    {
        return Ok(ThreadEpisodicWorkspaceCapsuleRefillSummary {
            skipped: true,
            ..Default::default()
        });
    }
    notify_refill_status(
        refill_status_sender,
        workspace_id,
        GatewayThreadEpisodicVectorRefillStatus::Running,
    );
    // Provider failure cannot turn incomplete preparation into a prepared marker.
    let preflight = async {
        if projection_target.requires_embedding_provider() {
            let provider = resolve_refill_embedding_provider_for_target(
                workspace_id,
                &projection_target,
                embedding_provider_resolver.as_ref(),
            )
            .await?;
            projection_target =
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                    provider.as_ref(),
                )?;
            embedding_provider_resolver = Some(Arc::new(
                FixedThreadEpisodicIndexEmbeddingProviderResolver::new(Some(provider)),
            )
                as Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>);
        }
        preflight_refill_embedding_resolver(
            workspace_id,
            &projection_target,
            embedding_provider_resolver.as_ref(),
        )
        .await
    }
    .await;
    // Unknown configured dimension is only a provisional selection. Decide
    // resume/reset after resolution, using the final identity and current marker.
    let reset_pending = projection_reset_is_pending(crud_store.as_ref(), workspace_id).await?;
    let existing_meta =
        find_refill_projection_meta_for_workspace(crud_store.as_ref(), workspace_id).await?;
    let resume_existing = !reset_pending
        && refill_can_resume_existing_projection(
            workspace_id,
            existing_meta.as_ref(),
            &projection_target,
        )?;
    let replace_projection = reset_pending
        || existing_meta.as_ref().is_some_and(|meta| {
            let marker = ProjectionMetaRecordLike {
                projection_config_hash: meta.projection_config_hash.as_deref(),
                projection_config_json: meta.projection_config_json.as_deref(),
            };
            meta.projection_version != THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
                || (!projection_target.matches_projection_meta(&marker)
                    && !projection_target.matches_projection_meta_selection(&marker))
        });
    let preserve_complete = !reset_pending
        && existing_meta
            .as_ref()
            .is_some_and(|meta| projection_meta_is_current_for_target(meta, &projection_target));
    if let Err(error) = preflight {
        let message = format!("{error:#}");
        if reset_pending {
            // Resolution failure proves no identity change. An unknown configured
            // dimension must not erase the resolved identity, files_cleaned or
            // cursor and renew a processed prefix's execution budget.
            update_projection_meta_status(
                &db,
                &projection_reset_checkpoint_key(workspace_id)?,
                PROJECTION_META_STATUS_PENDING,
                Some(message.as_str()),
                now_datetime(),
            )
            .await?;
        } else if preserve_complete {
            record_complete_refill_error(&db, workspace_id, &error).await?;
        } else if let Some(meta) = existing_meta.as_ref() {
            let status = if meta.status == PROJECTION_META_STATUS_COMPLETE {
                // Failed selection B has not changed the ready projection A.
                PROJECTION_META_STATUS_COMPLETE
            } else if matches!(
                meta.status.as_str(),
                PROJECTION_META_STATUS_BACKFILLING | PROJECTION_META_STATUS_FAILED
            ) {
                PROJECTION_META_STATUS_FAILED
            } else {
                PROJECTION_META_STATUS_PENDING
            };
            update_projection_meta_status(
                &db,
                &refill_projection_key_for_workspace(workspace_id)?,
                status,
                Some(message.as_str()),
                now_datetime(),
            )
            .await?;
        } else {
            mark_refill_preparation_failed(&db, workspace_id, &error, &projection_target).await?;
        }
        notify_refill_status(
            refill_status_sender,
            workspace_id,
            GatewayThreadEpisodicVectorRefillStatus::Failed,
        );
        return Err(error);
    }
    let prepared = if resume_existing {
        Ok(prepare_resumed_refill(existing_meta.as_ref()))
    } else {
        // Pending is incomplete preparation. Continue it without deleting jobs
        // already persisted by earlier preparation or canonical source saves.
        // The checkpoint remains pending until cleanup and the new pending
        // target are durable, even if config reverts to the old complete target.
        let cleanup = if replace_projection {
            mark_projection_reset(
                &db,
                workspace_id,
                PROJECTION_META_STATUS_PENDING,
                &projection_target,
            )
            .await?;
            cleanup_derived_artifacts(
                crud_store.as_ref(),
                chrono::Utc::now().timestamp(),
                thread_episodic_storage_root,
                workspace_id,
                ownership.clone(),
            )
            .await?
        } else {
            ThreadEpisodicWorkspaceCapsuleRefillSummary::default()
        };
        mark_refill_preparing(&db, workspace_id, &projection_target).await?;
        if replace_projection {
            mark_projection_reset(
                &db,
                workspace_id,
                PROJECTION_META_STATUS_COMPLETE,
                &projection_target,
            )
            .await?;
        }
        prepare_fresh_refill(
            crud_store.clone(),
            thread_episodic_storage_root,
            workspace_id,
            cleanup,
        )
        .await
    };
    let mut summary = match prepared {
        Ok(summary) => summary,
        Err(error) => {
            if resume_existing {
                mark_refill_failed(&db, workspace_id, &error, &projection_target).await?;
            } else {
                mark_refill_preparation_failed(&db, workspace_id, &error, &projection_target)
                    .await?;
            }
            notify_refill_status(
                refill_status_sender,
                workspace_id,
                GatewayThreadEpisodicVectorRefillStatus::Failed,
            );
            return Err(error);
        }
    };
    if !preserve_complete {
        mark_refill_backfilling(&db, workspace_id, &summary, &projection_target).await?;
    }
    match execute_refill_jobs(
        crud_store,
        thread_episodic_storage_root,
        workspace_id,
        embedding_provider_resolver,
        executor_config,
        &mut summary,
        ownership.clone(),
        #[cfg(test)]
        after_claim,
    )
    .await
    {
        Ok(()) => {
            if preserve_complete {
                update_projection_meta_status(
                    &db,
                    &refill_projection_key_for_workspace(workspace_id)?,
                    PROJECTION_META_STATUS_COMPLETE,
                    None,
                    now_datetime(),
                )
                .await?;
            } else {
                mark_refill_complete(&db, workspace_id, &summary, &projection_target).await?;
            }
            notify_refill_status(
                refill_status_sender,
                workspace_id,
                GatewayThreadEpisodicVectorRefillStatus::Complete,
            );
            Ok(summary)
        }
        Err(error) => {
            if preserve_complete {
                record_complete_refill_error(&db, workspace_id, &error).await?;
            } else {
                mark_refill_failed(&db, workspace_id, &error, &projection_target).await?;
            }
            notify_refill_status(
                refill_status_sender,
                workspace_id,
                GatewayThreadEpisodicVectorRefillStatus::Failed,
            );
            Err(error)
        }
    }
}

async fn resolve_refill_embedding_provider_for_target(
    workspace_id: &str,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    embedding_provider_resolver: Option<&Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>>,
) -> Result<Arc<dyn ThreadEpisodicEmbeddingProvider>> {
    let Some(embedding_provider_resolver) = embedding_provider_resolver else {
        bail!("thread episodic vector refill requires an active embedding provider resolver");
    };

    let provider = embedding_provider_resolver
        .resolve_active_embedding_provider(workspace_id)
        .await
        .map_err(|error| {
            anyhow!(
                "thread episodic vector refill provider preflight failed for workspace `{}`: {}",
                workspace_id,
                error.message
            )
        })?;
    let Some(provider) = provider else {
        bail!(
            "thread episodic vector refill provider preflight returned no provider for workspace `{}`",
            workspace_id
        );
    };
    if !projection_target.matches_embedding_provider_selection(provider.as_ref()) {
        bail!(
            "thread episodic vector refill provider identity does not match projection target for workspace `{}`",
            workspace_id
        );
    }
    Ok(provider)
}

#[allow(dead_code)]
pub(crate) async fn refill_once_for_vector_search_config(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    vector_search_config: &GatewayThreadEpisodicVectorSearchConfig,
    embedding_provider: Option<Arc<dyn ThreadEpisodicEmbeddingProvider>>,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let workspace_id = first_refill_workspace_id_or_legacy(crud_store.as_ref()).await?;
    refill_once_with_workspace_projection(
        crud_store,
        thread_episodic_storage_root,
        workspace_id.as_str(),
        ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
            vector_search_config,
        ),
        embedding_provider,
    )
    .await
}

async fn first_refill_workspace_id_or_legacy(crud_store: &CrudStore) -> Result<String> {
    Ok(crud_store
        .list_thread_episodic_refill_workspace_ids()
        .await?
        .into_iter()
        .next()
        .unwrap_or_else(|| LEGACY_REFILL_WORKSPACE_ID.to_owned()))
}

struct RefillLockGuard {
    file: File,
}

struct FixedThreadEpisodicIndexEmbeddingProviderResolver {
    provider: Option<Arc<dyn ThreadEpisodicEmbeddingProvider>>,
}

impl FixedThreadEpisodicIndexEmbeddingProviderResolver {
    fn new(provider: Option<Arc<dyn ThreadEpisodicEmbeddingProvider>>) -> Self {
        Self { provider }
    }
}

#[async_trait::async_trait]
impl ThreadEpisodicIndexEmbeddingProviderResolver
    for FixedThreadEpisodicIndexEmbeddingProviderResolver
{
    async fn resolve_active_embedding_provider(
        &self,
        _workspace_id: &str,
    ) -> std::result::Result<
        Option<Arc<dyn ThreadEpisodicEmbeddingProvider>>,
        ThreadEpisodicIndexResolutionError,
    > {
        Ok(self.provider.clone())
    }
}

impl Drop for RefillLockGuard {
    fn drop(&mut self) {
        let _ = Fs4FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
fn try_acquire_refill_lock(thread_episodic_storage_root: &Path) -> Result<Option<RefillLockGuard>> {
    try_acquire_refill_lock_for_workspace(thread_episodic_storage_root, LEGACY_REFILL_WORKSPACE_ID)
}

fn try_acquire_refill_lock_for_workspace(
    thread_episodic_storage_root: &Path,
    workspace_id: &str,
) -> Result<Option<RefillLockGuard>> {
    std::fs::create_dir_all(thread_episodic_storage_root).with_context(|| {
        format!(
            "failed to create thread episodic storage root `{}` for refill lock",
            thread_episodic_storage_root.display()
        )
    })?;
    let lock_path =
        thread_episodic_storage_root.join(refill_lock_file_name_for_workspace(workspace_id)?);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(lock_path.as_path())
        .with_context(|| {
            format!(
                "failed to open thread episodic workspace refill lock `{}`",
                lock_path.display()
            )
        })?;
    match Fs4FileExt::try_lock(&file) {
        Ok(()) => Ok(Some(RefillLockGuard { file })),
        Err(Fs4TryLockError::WouldBlock) => Ok(None),
        Err(error) => Err(error).with_context(|| {
            format!(
                "failed to acquire thread episodic workspace refill lock `{}`",
                lock_path.display()
            )
        }),
    }
}

pub(crate) fn refill_projection_key_for_workspace(workspace_id: &str) -> Result<String> {
    if workspace_id == LEGACY_REFILL_WORKSPACE_ID {
        return Ok(THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY.to_owned());
    }
    let workspace_key_hash = pioneer_crud::thread_episodic_key_hash("workspace", workspace_id)
        .with_context(|| {
            format!("failed to hash workspace id `{workspace_id}` for refill projection key")
        })?;
    Ok(format!(
        "{THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY}:{workspace_key_hash}"
    ))
}

fn refill_lock_file_name_for_workspace(workspace_id: &str) -> Result<String> {
    if workspace_id == LEGACY_REFILL_WORKSPACE_ID {
        return Ok(REFILL_LOCK_FILE_NAME.to_owned());
    }
    let workspace_key_hash = pioneer_crud::thread_episodic_key_hash("workspace", workspace_id)
        .with_context(|| format!("failed to hash workspace id `{workspace_id}` for refill lock"))?;
    Ok(format!(
        ".thread_episodic_workspace_capsule_refill.{workspace_key_hash}.lock"
    ))
}

#[allow(dead_code)]
pub(crate) async fn refill_is_current(crud_store: &CrudStore) -> Result<bool> {
    refill_is_current_for_target(
        crud_store,
        &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
    )
    .await
}

pub(crate) async fn refill_is_current_for_target(
    crud_store: &CrudStore,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<bool> {
    let projection_key_prefix = THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY;
    let workspace_projection_key_prefix = format!("{projection_key_prefix}:");
    for meta in
        list_projection_meta_by_key_prefix(&crud_store.database_connection(), projection_key_prefix)
            .await?
    {
        if meta.projection_key != projection_key_prefix
            && !meta
                .projection_key
                .starts_with(workspace_projection_key_prefix.as_str())
        {
            continue;
        }
        if projection_meta_is_current_for_target(&meta, projection_target) {
            let checkpoint_key = if meta.projection_key == projection_key_prefix {
                projection_reset_checkpoint_key(LEGACY_REFILL_WORKSPACE_ID)?
            } else {
                format!(
                    "thread_episodic_projection_reset:{}",
                    meta.projection_key
                        .strip_prefix(workspace_projection_key_prefix.as_str())
                        .context("invalid scoped refill key")?
                )
            };
            if find_projection_meta(&crud_store.database_connection(), &checkpoint_key)
                .await?
                .is_none_or(|checkpoint| checkpoint.status != PROJECTION_META_STATUS_PENDING)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub(crate) async fn refill_is_current_for_workspace_target(
    crud_store: &CrudStore,
    workspace_id: &str,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<bool> {
    let Some(meta) = find_refill_projection_meta_for_workspace(crud_store, workspace_id).await?
    else {
        return Ok(false);
    };

    Ok(
        projection_meta_is_current_for_target(&meta, projection_target)
            && !projection_reset_is_pending(crud_store, workspace_id).await?,
    )
}

fn projection_meta_is_current_for_target(
    meta: &pioneer_entity::thread_timeline_projection_meta::Model,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> bool {
    let marker = ProjectionMetaRecordLike {
        projection_config_hash: meta.projection_config_hash.as_deref(),
        projection_config_json: meta.projection_config_json.as_deref(),
    };
    meta.projection_version == THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
        && meta.status == PROJECTION_META_STATUS_COMPLETE
        && (projection_target.matches_projection_meta(&marker)
            || projection_target.matches_projection_meta_selection(&marker))
}

#[allow(dead_code)]
pub(crate) async fn refill_status_for_target(
    crud_store: &CrudStore,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus> {
    refill_status_for_workspace_target(crud_store, LEGACY_REFILL_WORKSPACE_ID, projection_target)
        .await
}

pub(crate) async fn refill_status_for_workspace_target(
    crud_store: &CrudStore,
    workspace_id: &str,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus> {
    let Some(meta) = find_refill_projection_meta_for_workspace(crud_store, workspace_id).await?
    else {
        return Ok(pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required);
    };

    if meta.projection_version != THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
        || projection_reset_is_pending(crud_store, workspace_id).await?
    {
        return Ok(pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required);
    }

    let marker = ProjectionMetaRecordLike {
        projection_config_hash: meta.projection_config_hash.as_deref(),
        projection_config_json: meta.projection_config_json.as_deref(),
    };
    if !projection_target.matches_projection_meta(&marker)
        && !projection_target.matches_projection_meta_selection(&marker)
    {
        return Ok(pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required);
    }

    let status = match meta.status.as_str() {
        PROJECTION_META_STATUS_COMPLETE => {
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Complete
        }
        PROJECTION_META_STATUS_PENDING if meta.last_error.is_some() => {
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Failed
        }
        PROJECTION_META_STATUS_PENDING => {
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Running
        }
        PROJECTION_META_STATUS_BACKFILLING => {
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Running
        }
        PROJECTION_META_STATUS_FAILED => {
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Failed
        }
        _ => pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required,
    };
    Ok(status)
}

async fn find_refill_projection_meta_for_workspace(
    crud_store: &CrudStore,
    workspace_id: &str,
) -> Result<Option<pioneer_entity::thread_timeline_projection_meta::Model>> {
    let db = crud_store.database_connection();
    let projection_key = refill_projection_key_for_workspace(workspace_id)?;
    if let Some(meta) = find_projection_meta(&db, projection_key.as_str()).await? {
        return Ok(Some(meta));
    }
    Ok(None)
}

// One existing progress row records interrupted artifact cleanup. Keeping it
// separate from the target marker also covers reverting config after a crash.
fn projection_reset_checkpoint_key(workspace_id: &str) -> Result<String> {
    pioneer_crud::thread_episodic_projection_reset_key(workspace_id)
}

pub(crate) async fn projection_reset_is_pending(
    crud_store: &CrudStore,
    workspace_id: &str,
) -> Result<bool> {
    Ok(find_projection_meta(
        &crud_store.database_connection(),
        &projection_reset_checkpoint_key(workspace_id)?,
    )
    .await?
    .is_some_and(|checkpoint| checkpoint.status == PROJECTION_META_STATUS_PENDING))
}

pub(crate) async fn mark_projection_reset<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    status: &str,
    target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<()> {
    anyhow::ensure!(
        !target.requires_embedding_provider() || target.payload.dimension.is_some(),
        "projection reset requires a resolved embedding identity"
    );
    let now = now_datetime();
    let key = projection_reset_checkpoint_key(workspace_id)?;
    let identity = format!(
        "{}:{}",
        THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION, target.config_hash
    );
    let previous = find_projection_meta(db, &key).await?;
    let progress = if let Some(previous) = previous.filter(|meta| {
        meta.status == PROJECTION_META_STATUS_PENDING
            && meta.projection_config_hash.as_deref() == Some(identity.as_str())
    }) {
        serde_json::from_str::<pioneer_crud::ThreadEpisodicProjectionResetProgress>(
            previous
                .projection_config_json
                .as_deref()
                .context("reset progress missing")?,
        )?
    } else {
        pioneer_crud::ThreadEpisodicProjectionResetProgress::default()
    };
    let progress_json = serde_json::to_string(&progress)?;
    upsert_projection_meta_with_config(
        db,
        ProjectionMetaRecord {
            projection_key: projection_reset_checkpoint_key(workspace_id)?,
            projection_version: 1,
            status: status.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: (status == PROJECTION_META_STATUS_COMPLETE).then_some(now),
            created_at: now,
            updated_at: now,
        },
        ProjectionMetaConfigRecord {
            projection_config_hash: Some(identity),
            projection_config_json: Some(progress_json),
        },
    )
    .await
}

// Called with workspace ownership, before ordinary claims and again before FS.
// Prepared backfilling/failed projections can accept jobs; pending reset cannot.
pub(crate) async fn projection_accepts_index_target(
    store: &CrudStore,
    workspace: &str,
    target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<bool> {
    if projection_reset_is_pending(store, workspace).await? {
        return Ok(false);
    }
    let meta = find_refill_projection_meta_for_workspace(store, workspace).await?;
    Ok(meta.as_ref().is_some_and(|meta| {
        meta.projection_version == THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
            && matches!(
                meta.status.as_str(),
                PROJECTION_META_STATUS_COMPLETE
                    | PROJECTION_META_STATUS_BACKFILLING
                    | PROJECTION_META_STATUS_FAILED
            )
            && target.matches_projection_meta(&ProjectionMetaRecordLike {
                projection_config_hash: meta.projection_config_hash.as_deref(),
                projection_config_json: meta.projection_config_json.as_deref(),
            })
            && (meta.status == PROJECTION_META_STATUS_COMPLETE
                || meta.backfill_started_at.is_some())
    }))
}

// A complete marker describes prepared history. New durable source saves may
// have left later jobs, so only an idle queue takes the provider-free fast path.
async fn inspect_current_refill_work(
    crud_store: &CrudStore,
    workspace_id: &str,
    target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<Option<ThreadEpisodicWorkspaceCapsuleRefillSummary>> {
    if !refill_is_current_for_workspace_target(crud_store, workspace_id, target).await? {
        return Ok(None);
    }
    let mut summary = ThreadEpisodicWorkspaceCapsuleRefillSummary {
        skipped: true,
        ..Default::default()
    };
    summary.skipped = !crud_store
        .unfinished_thread_episodic_index_job_exists_for_workspace(workspace_id)
        .await?;
    Ok(Some(summary))
}

fn refill_can_resume_existing_projection(
    workspace_id: &str,
    meta: Option<&pioneer_entity::thread_timeline_projection_meta::Model>,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<bool> {
    let Some(meta) = meta else {
        return Ok(false);
    };
    if meta.projection_version != THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
        || !matches!(
            meta.status.as_str(),
            PROJECTION_META_STATUS_BACKFILLING
                | PROJECTION_META_STATUS_FAILED
                | PROJECTION_META_STATUS_COMPLETE
        )
    {
        return Ok(false);
    }

    let marker = ProjectionMetaRecordLike {
        projection_config_hash: meta.projection_config_hash.as_deref(),
        projection_config_json: meta.projection_config_json.as_deref(),
    };
    if !projection_target.matches_projection_meta(&marker)
        && !projection_target.matches_projection_meta_selection(&marker)
    {
        return Ok(false);
    }

    // All modern writers enter backfilling only after successful preparation;
    // execution failures preserve that marker's workspace, target and counts.
    // Global legacy markers and the mere presence of jobs are not evidence.
    Ok(
        meta.projection_key == refill_projection_key_for_workspace(workspace_id)?
            && (meta.status == PROJECTION_META_STATUS_COMPLETE
                || meta.backfill_started_at.is_some()),
    )
}

async fn prepare_fresh_refill(
    crud_store: Arc<CrudStore>,
    _thread_episodic_storage_root: &Path,
    workspace_id: &str,
    mut summary: ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let now_unix = chrono::Utc::now().timestamp();
    rebuild_refill_items_from_history(crud_store.clone(), workspace_id, now_unix, &mut summary)
        .await?;
    populate_refill_source_counts(crud_store.as_ref(), workspace_id, &mut summary).await?;
    enqueue_refill_jobs(crud_store.as_ref(), workspace_id, now_unix, &mut summary).await?;
    Ok(summary)
}

fn prepare_resumed_refill(
    existing_meta: Option<&pioneer_entity::thread_timeline_projection_meta::Model>,
) -> ThreadEpisodicWorkspaceCapsuleRefillSummary {
    let mut summary = ThreadEpisodicWorkspaceCapsuleRefillSummary {
        resumed: true,
        ..Default::default()
    };
    summary.workspace_count = 1;
    if let Some(meta) = existing_meta {
        summary.source_thread_count = meta.source_thread_count;
        summary.source_turn_count = meta.source_turn_count;
        summary.source_turn_item_count = meta.source_turn_item_count;
    }
    summary
}

async fn populate_refill_source_counts(
    crud_store: &CrudStore,
    workspace_id: &str,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    summary.workspace_count = 1;
    let source_counts = crud_store
        .count_thread_episodic_refill_sources_for_workspace(workspace_id)
        .await
        .context("failed to count thread episodic refill sources")?;
    summary.source_thread_count = source_counts.source_thread_count;
    summary.source_turn_count = source_counts.source_turn_count;
    summary.source_turn_item_count = source_counts.source_turn_item_count;
    Ok(())
}

async fn preflight_refill_embedding_resolver(
    workspace_id: &str,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    embedding_provider_resolver: Option<&Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>>,
) -> Result<()> {
    if !projection_target.requires_embedding_provider() {
        return Ok(());
    }
    let Some(embedding_provider_resolver) = embedding_provider_resolver else {
        bail!("thread episodic vector refill requires an active embedding provider resolver");
    };

    let provider = embedding_provider_resolver
        .resolve_active_embedding_provider(workspace_id)
        .await
        .map_err(|error| {
            anyhow!(
                "thread episodic vector refill provider preflight failed for workspace `{}`: {}",
                workspace_id,
                error.message
            )
        })?;
    let Some(provider) = provider else {
        bail!(
            "thread episodic vector refill provider preflight returned no provider for workspace `{}`",
            workspace_id
        );
    };
    if !projection_target.matches_embedding_provider(provider.as_ref()) {
        bail!(
            "thread episodic vector refill provider identity does not match projection target for workspace `{}`",
            workspace_id
        );
    }

    Ok(())
}

async fn cleanup_derived_artifacts(
    crud_store: &CrudStore,
    now_unix: i64,
    _thread_episodic_storage_root: &Path,
    workspace_id: &str,
    ownership: ThreadEpisodicWorkspaceOwnership,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let checkpoint = find_projection_meta(
        &crud_store.database_connection(),
        &projection_reset_checkpoint_key(workspace_id)?,
    )
    .await?
    .context("reset checkpoint missing before cleanup")?;
    let progress: pioneer_crud::ThreadEpisodicProjectionResetProgress = serde_json::from_str(
        checkpoint
            .projection_config_json
            .as_deref()
            .context("reset progress missing before cleanup")?,
    )?;
    let mut summary = ThreadEpisodicWorkspaceCapsuleRefillSummary::default();
    if !progress.files_cleaned {
        let capsules = crud_store
            .list_all_thread_episodic_capsules_for_workspace(workspace_id)
            .await
            .context("failed to list thread episodic capsules before workspace refill")?;
        for capsule in capsules {
            match delete_capsule_file(capsule.storage_uri.as_str(), ownership.clone()).await? {
                CapsuleFileDeleteOutcome::Deleted => {
                    summary.capsule_files_deleted = summary.capsule_files_deleted.saturating_add(1);
                }
                CapsuleFileDeleteOutcome::Missing => {
                    summary.capsule_files_missing = summary.capsule_files_missing.saturating_add(1);
                }
                CapsuleFileDeleteOutcome::NonFileUri => {
                    summary.non_file_storage_uris = summary.non_file_storage_uris.saturating_add(1);
                }
            }
        }

        summary.capsule_rows_deleted = crud_store
            .delete_thread_episodic_capsules_for_workspace(workspace_id)
            .await
            .context("failed to delete thread episodic capsule rows")?;
        summary.thread_directory_rows_deleted = crud_store
            .delete_thread_episodic_thread_directory_entries_for_workspace(workspace_id)
            .await
            .context("failed to delete stale thread episodic thread directory rows")?;
    }
    summary.index_jobs_replaced = crud_store
        .reset_thread_episodic_projection(workspace_id, now_unix)
        .await
        .context("failed to reset episodic projection frames and claims")?;
    Ok(summary)
}

async fn rebuild_refill_items_from_history(
    crud_store: Arc<CrudStore>,
    workspace_id: &str,
    now_unix: i64,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    let threads = crud_store
        .list_thread_episodic_refill_threads_for_workspace(workspace_id)
        .await
        .context("failed to list thread episodic source threads for workspace refill")?;
    rebuild_refill_threads_from_history(crud_store, threads, now_unix, summary).await
}

async fn rebuild_refill_threads_from_history(
    crud_store: Arc<CrudStore>,
    threads: Vec<ThreadEpisodicRefillThread>,
    now_unix: i64,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    let ingestor = StoreThreadEpisodicIngestor::with_config(crud_store, true);
    let mut first_error = None;
    for thread in threads {
        match ingestor
            .reindex_thread_from_history(ThreadEpisodicThreadReindexRequest {
                workspace_id: thread.workspace_id,
                thread_id: thread.thread_id,
                history_event_limit: None,
                item_scan_limit: 1_000_000,
                now_unix,
            })
            .await
        {
            Ok(reindex_summary) => {
                summary.source_threads_reindexed =
                    summary.source_threads_reindexed.saturating_add(1);
                summary.refill_jobs_enqueued = summary.refill_jobs_enqueued.saturating_add(
                    reindex_summary
                        .missing_jobs_created
                        .saturating_add(reindex_summary.existing_jobs),
                );
            }
            Err(error) => {
                summary.source_threads_failed = summary.source_threads_failed.saturating_add(1);
                let error_display = format!("{error:#}");
                if first_error.is_none() {
                    first_error = Some(error.context("failed to rebuild one source thread"));
                }
                warn!(
                    error = %error_display,
                    "thread episodic workspace refill failed to rebuild one source thread"
                );
            }
        }
    }
    if let Some(error) = first_error {
        bail!(
            "thread episodic workspace refill failed to rebuild {} source threads: {error:#}",
            summary.source_threads_failed
        );
    }
    Ok(())
}

async fn enqueue_refill_jobs(
    crud_store: &CrudStore,
    workspace_id: &str,
    now_unix: i64,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    loop {
        let enqueued = crud_store
            .enqueue_thread_episodic_refill_index_jobs_for_workspace(
                workspace_id,
                now_unix,
                REFILL_ENQUEUE_BATCH_SIZE,
            )
            .await
            .context("failed to enqueue thread episodic refill index jobs")?;
        summary.refill_jobs_enqueued = summary.refill_jobs_enqueued.saturating_add(enqueued);
        if enqueued == 0 {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
static REFILL_RETRY_NOTICE: std::sync::Mutex<Option<(String, tokio::sync::oneshot::Sender<()>)>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) fn notify_next_retry_wait_for_test(
    workspace: &str,
    notice: tokio::sync::oneshot::Sender<()>,
) {
    *REFILL_RETRY_NOTICE.lock().unwrap() = Some((workspace.to_owned(), notice));
}

#[cfg(test)]
static REFILL_OWNERSHIP_TEST_NOTICE: std::sync::Mutex<
    BTreeMap<(PathBuf, bool), tokio::sync::oneshot::Sender<()>>,
> = std::sync::Mutex::new(BTreeMap::new());

#[cfg(test)]
pub(crate) fn notify_ownership_wait_for_test(
    root: &Path,
    notice: tokio::sync::oneshot::Sender<()>,
) {
    REFILL_OWNERSHIP_TEST_NOTICE
        .lock()
        .unwrap()
        .insert((root.to_owned(), false), notice);
}

#[cfg(test)]
type RefillBlockingTestGate = (
    tokio::sync::oneshot::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);
#[cfg(test)]
static REFILL_BLOCKING_TEST_GATES: std::sync::Mutex<BTreeMap<PathBuf, RefillBlockingTestGate>> =
    std::sync::Mutex::new(BTreeMap::new());

// Stops an actual Native backend request inside a blocking owner. No DB
// capacity is held; sender drop on assertion/panic releases the blocking task.
#[cfg(test)]
struct BlockingRefillTestBackend {
    native: Arc<dyn ThreadEpisodicMemvidBackend>,
    ownership: ThreadEpisodicWorkspaceOwnership,
    gate: std::sync::Mutex<Option<RefillBlockingTestGate>>,
}
#[cfg(test)]
#[async_trait::async_trait]
impl ThreadEpisodicMemvidBackend for BlockingRefillTestBackend {
    fn capabilities(&self) -> pioneer_memory::ThreadEpisodicMemvidBackendCapabilities {
        self.native.capabilities()
    }
    async fn index_item(
        &self,
        request: pioneer_memory::ThreadEpisodicMemvidIndexRequest,
    ) -> std::result::Result<
        ThreadEpisodicMemvidIndexOutput,
        pioneer_memory::ThreadEpisodicMemvidError,
    > {
        let gate = self.gate.lock().unwrap().take();
        if let Some((started, release)) = gate {
            let native = self.native.clone();
            let ownership = self.ownership.clone();
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let _ownership = ownership;
                let _ = started.send(());
                release.recv().map_err(|_| {
                    pioneer_memory::ThreadEpisodicMemvidError::retryable(
                        "blocking refill release dropped",
                    )
                })?;
                runtime.block_on(native.index_item(request))
            })
            .await
            .map_err(|_| {
                pioneer_memory::ThreadEpisodicMemvidError::retryable("blocking refill join failed")
            })?
        } else {
            self.native.index_item(request).await
        }
    }
    async fn search(
        &self,
        request: pioneer_memory::ThreadEpisodicMemvidSearchRequest,
    ) -> std::result::Result<
        pioneer_memory::ThreadEpisodicMemvidSearchOutput,
        pioneer_memory::ThreadEpisodicMemvidError,
    > {
        self.native.search(request).await
    }
}

async fn execute_refill_jobs(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    workspace_id: &str,
    embedding_provider_resolver: Option<Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>>,
    config: ThreadEpisodicIndexExecutorConfig,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
    ownership: ThreadEpisodicWorkspaceOwnership,
    #[cfg(test)] mut after_claim: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<()> {
    let storage_uri_root = thread_episodic_storage_uri_from_path(thread_episodic_storage_root);
    let backend: Arc<dyn ThreadEpisodicMemvidBackend> =
        Arc::new(MemvidThreadEpisodicBackend::new().with_workspace_ownership(ownership.clone()));
    #[cfg(test)]
    let backend: Arc<dyn ThreadEpisodicMemvidBackend> = {
        let gate = REFILL_BLOCKING_TEST_GATES
            .lock()
            .unwrap()
            .remove(thread_episodic_storage_root);
        if let Some(gate) = gate {
            Arc::new(BlockingRefillTestBackend {
                native: backend,
                ownership,
                gate: std::sync::Mutex::new(Some(gate)),
            })
        } else {
            backend
        }
    };
    let base_payload_provider: Arc<dyn ThreadEpisodicIndexPayloadProvider> = Arc::new(
        StoreThreadEpisodicIndexPayloadProvider::new(crud_store.clone(), storage_uri_root),
    );
    let payload_provider: Arc<dyn ThreadEpisodicIndexPayloadProvider> =
        if let Some(embedding_provider_resolver) = embedding_provider_resolver {
            Arc::new(RuntimeVectorThreadEpisodicIndexPayloadProvider::new(
                base_payload_provider,
                embedding_provider_resolver,
                crud_store.clone(),
            ))
        } else {
            base_payload_provider
        };
    loop {
        if summary.executor_batches >= REFILL_EXECUTOR_MAX_BATCHES {
            break;
        }
        let now_unix = chrono::Utc::now().timestamp();
        let batch = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id,
                now_unix,
                REFILL_JOB_CLAIM_LIMIT,
                config.max_attempts,
            )
            .await
            .context("failed to claim thread episodic workspace refill index jobs")?;
        let jobs = batch.claimed;
        let claim_failures = batch.failures;
        if jobs.is_empty() && claim_failures.is_empty() {
            if summary.failed_terminal_jobs > 0
                || crud_store
                    .terminal_thread_episodic_index_job_exists_for_workspace(workspace_id)
                    .await?
            {
                bail!("thread episodic workspace refill has terminally failed index jobs");
            }
            summary.has_incomplete_jobs = crud_store
                .unfinished_thread_episodic_index_job_exists_for_workspace(workspace_id)
                .await?;
            if !summary.has_incomplete_jobs {
                return Ok(());
            }

            let next_run_at = crud_store
                .next_scheduled_thread_episodic_index_job_at_for_workspace(workspace_id)
                .await
                .context("failed to find the next scheduled thread episodic refill job")?;
            let Some(next_run_at) = next_run_at else {
                bail!("thread episodic workspace refill stalled with an unfinished running job");
            };
            let delay_secs = next_run_at.saturating_sub(chrono::Utc::now().timestamp());
            if delay_secs > 0 {
                #[cfg(test)]
                {
                    let mut notice = REFILL_RETRY_NOTICE.lock().unwrap();
                    if notice
                        .as_ref()
                        .is_some_and(|(workspace, _)| workspace == workspace_id)
                    {
                        if let Some((_, notice)) = notice.take() {
                            let _ = notice.send(());
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(delay_secs as u64)).await;
            } else {
                tokio::task::yield_now().await;
            }
            continue;
        }

        #[cfg(test)]
        if let Some(started) = after_claim.take() {
            // The real claim has committed, no DB capacity is held and provider
            // work has not begun. Cancellation drops an ordinary async future.
            let _ = started.send(());
            std::future::pending::<()>().await;
        }
        summary.executor_batches = summary.executor_batches.saturating_add(1);
        let execution = execute_claimed_refill_batch(
            crud_store.clone(),
            backend.as_ref(),
            payload_provider.as_ref(),
            jobs,
            now_unix,
            config,
            summary,
            #[cfg(test)]
            None,
        )
        .await;
        // Finish confirmed healthy claims before observing any uncertain one.
        // These snapshots are not dispatch receipts. Serialized writer state
        // observation, under this workspace's ownership, fences settlement.
        let claim_error_count = claim_failures.len();
        for (mut expected, failure_class) in claim_failures {
            tracing::warn!(failure_class, "episodic refill claim deferred");
            // Prospective intent only; the writer still enforces the actual
            // execution budget. Even an exhausted input needs guarded backoff
            // when its terminal bookkeeping failed.
            let attempt = expected.attempt_count.saturating_add(1);
            let input = expected.clone();
            expected.attempt_count = attempt;
            expected.updated_at = fixed_datetime_from_unix(now_unix);
            let settlement_now = chrono::Utc::now().timestamp();
            use futures_util::FutureExt;
            let settlement = std::panic::AssertUnwindSafe(async {
                crud_store
                    .settle_thread_episodic_owned_index_attempt(
                        &expected,
                        Some(&input),
                        ThreadEpisodicIndexJobFailureUpdate {
                            retryable: attempt < config.max_attempts,
                            next_run_at_unix: Some(next_refill_retry_at(
                                &expected,
                                settlement_now,
                                config,
                            )),
                            last_error: Some(failure_class.to_owned()),
                            capacity_error: false,
                            last_attempt_latency_ms: None,
                        },
                        settlement_now,
                    )
                    .await
            })
            .catch_unwind()
            .await;
            if !matches!(settlement, Ok(Ok(()))) {
                tracing::warn!(
                    failure_class = "refill_claim_settlement",
                    "episodic uncertain claim remains durable for guarded recovery"
                );
            }
        }
        if claim_error_count > 0 {
            summary.has_incomplete_jobs = true;
            bail!(
                "thread episodic refill encountered {claim_error_count} claim failure(s); healthy remainder processed; execution_failed={}",
                execution.is_err()
            );
        }
        execution?;
    }

    summary.has_incomplete_jobs = crud_store
        .unfinished_thread_episodic_index_job_exists_for_workspace(workspace_id)
        .await?;
    if summary.failed_terminal_jobs > 0
        || crud_store
            .terminal_thread_episodic_index_job_exists_for_workspace(workspace_id)
            .await?
    {
        bail!("thread episodic workspace refill has terminally failed index jobs");
    }
    if summary.has_incomplete_jobs {
        bail!(
            "thread episodic workspace refill reached its executor limit with unfinished index jobs"
        );
    }

    Ok(())
}

async fn execute_claimed_refill_batch(
    crud_store: Arc<CrudStore>,
    backend: &dyn ThreadEpisodicMemvidBackend,
    payload_provider: &dyn ThreadEpisodicIndexPayloadProvider,
    jobs: Vec<ThreadEpisodicIndexJobRecord>,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
    #[cfg(test)] fail_before_processing_job_id: Option<&str>,
) -> Result<()> {
    let mut batch_errors = Vec::new();
    for job in jobs {
        let job_id = job.id.clone();
        use futures_util::FutureExt;
        let result = std::panic::AssertUnwindSafe(async {
            #[cfg(test)]
            let result = if fail_before_processing_job_id == Some(job_id.as_str()) {
                persist_refill_reconciliation_error(
                    crud_store.clone(),
                    &job,
                    now_unix,
                    config,
                    anyhow!("injected refill source reconciliation failure"),
                    Some("injected primary reconciliation persistence failure"),
                    Some("injected fallback reconciliation persistence failure"),
                )
                .await
                .map(|_| ())
            } else {
                execute_claimed_refill_job(
                    crud_store.clone(),
                    backend,
                    payload_provider,
                    job,
                    now_unix,
                    config,
                    summary,
                )
                .await
            };
            #[cfg(not(test))]
            let result = execute_claimed_refill_job(
                crud_store.clone(),
                backend,
                payload_provider,
                job,
                now_unix,
                config,
                summary,
            )
            .await;
            result
        })
        .catch_unwind()
        .await;
        let result = match result {
            Ok(result) => result,
            Err(_) => {
                tracing::warn!(
                    failure_class = "refill_candidate_unwind",
                    "episodic refill candidate interrupted"
                );
                // Actual Running is left durable; caller's ownership protects
                // it until backend blocking work finishes and recovery observes it.
                Err(anyhow!("episodic refill candidate unwind"))
            }
        };
        if let Err(error) = result {
            batch_errors.push(format!("job `{job_id}`: {error:#}"));
        }
    }
    if !batch_errors.is_empty() {
        bail!(
            "thread episodic refill could not durably finish {} claimed job(s): {}",
            batch_errors.len(),
            batch_errors.join("; ")
        );
    }
    Ok(())
}

async fn execute_claimed_refill_job(
    crud_store: Arc<CrudStore>,
    backend: &dyn ThreadEpisodicMemvidBackend,
    payload_provider: &dyn ThreadEpisodicIndexPayloadProvider,
    job: ThreadEpisodicIndexJobRecord,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    let attempt_started_at = Instant::now();
    match payload_provider.resolve_index_request(&job).await {
        Ok(resolved) => {
            execute_resolved_refill_job(
                crud_store,
                backend,
                job,
                resolved,
                now_unix,
                config,
                attempt_started_at,
                summary,
            )
            .await
        }
        Err(error) if error.kind == ThreadEpisodicIndexResolutionFailureKind::SourceChanged => {
            reconcile_and_release_refill_claim(crud_store, &job, now_unix, config)
                .await
                .map(|_| ())
        }
        Err(error) => {
            let ThreadEpisodicIndexResolutionError {
                kind,
                message,
                source_payload,
            } = error;
            let retryable = matches!(kind, ThreadEpisodicIndexResolutionFailureKind::Retryable)
                && job.attempt_count < config.max_attempts;
            let original_error_message = message.clone();
            let persisted = match persist_refill_job_failure(
                crud_store.as_ref(),
                &job,
                retryable,
                false,
                Some(message),
                source_payload.as_deref(),
                now_unix,
                attempt_started_at,
                config,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    let (outcome, recovered_retryable) =
                        release_refill_claim_after_persistence_error(
                            crud_store,
                            &job,
                            now_unix,
                            config,
                            retryable,
                            &error,
                            Some(original_error_message.as_str()),
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "failed to recover refill claim after resolution persistence error: {error:#}"
                            )
                        })?;
                    if outcome == ThreadEpisodicIndexAttemptOutcome::Applied {
                        if recovered_retryable {
                            summary.failed_retryable_jobs =
                                summary.failed_retryable_jobs.saturating_add(1);
                        } else {
                            summary.failed_terminal_jobs =
                                summary.failed_terminal_jobs.saturating_add(1);
                        }
                    }
                    return Ok(());
                }
            };
            match persisted {
                ThreadEpisodicIndexAttemptOutcome::Applied => {}
                ThreadEpisodicIndexAttemptOutcome::SourceChanged
                | ThreadEpisodicIndexAttemptOutcome::Excluded => {
                    reconcile_and_release_refill_claim(crud_store, &job, now_unix, config).await?;
                    return Ok(());
                }
                ThreadEpisodicIndexAttemptOutcome::StaleAttempt => return Ok(()),
            }
            if retryable {
                summary.failed_retryable_jobs = summary.failed_retryable_jobs.saturating_add(1);
            } else {
                summary.failed_terminal_jobs = summary.failed_terminal_jobs.saturating_add(1);
            }
            Ok(())
        }
    }
}

async fn execute_resolved_refill_job(
    crud_store: Arc<CrudStore>,
    backend: &dyn ThreadEpisodicMemvidBackend,
    job: ThreadEpisodicIndexJobRecord,
    resolved: ThreadEpisodicResolvedIndexRequest,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
    attempt_started_at: Instant,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    match backend.index_item(resolved.request.clone()).await {
        Ok(output) => {
            let capsule_id = resolved.request.capsule_id.clone();
            let output_stats = output.stats.clone();
            let persisted = match persist_successful_refill_item(
                crud_store.as_ref(),
                &job,
                resolved,
                output,
                now_unix,
                attempt_started_at,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    let (outcome, retryable) = release_refill_claim_after_persistence_error(
                        crud_store.clone(),
                        &job,
                        now_unix,
                        config,
                        job.attempt_count < config.max_attempts,
                        &error,
                        Some("backend index succeeded"),
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to recover refill claim after success persistence error: {error:#}"
                        )
                    })?;
                    if outcome == ThreadEpisodicIndexAttemptOutcome::Applied {
                        if retryable {
                            summary.failed_retryable_jobs =
                                summary.failed_retryable_jobs.saturating_add(1);
                        } else {
                            summary.failed_terminal_jobs =
                                summary.failed_terminal_jobs.saturating_add(1);
                        }
                    }
                    return Ok(());
                }
            };
            match persisted {
                ThreadEpisodicIndexAttemptOutcome::Applied => {}
                ThreadEpisodicIndexAttemptOutcome::SourceChanged
                | ThreadEpisodicIndexAttemptOutcome::Excluded => {
                    reconcile_and_release_refill_claim(crud_store.clone(), &job, now_unix, config)
                        .await?;
                    return Ok(());
                }
                ThreadEpisodicIndexAttemptOutcome::StaleAttempt => return Ok(()),
            }
            update_refill_capsule_capacity(
                crud_store.as_ref(),
                capsule_id.as_str(),
                &output_stats,
                None,
                false,
                now_unix,
                config,
            )
            .await;
            if memvid_stats_reach_capacity_threshold(&output_stats, config.near_capacity_percent) {
                rotate_refill_capsule_after_capacity_event(
                    crud_store.as_ref(),
                    capsule_id.as_str(),
                    now_unix,
                    "near_capacity",
                )
                .await;
            }
            summary.completed_jobs = summary.completed_jobs.saturating_add(1);
        }
        Err(error) => {
            let retryable = matches!(
                error.kind,
                ThreadEpisodicMemvidFailureKind::Retryable
                    | ThreadEpisodicMemvidFailureKind::CapacityExceeded
            ) && job.attempt_count < config.max_attempts;
            let capacity_error = matches!(
                error.kind,
                ThreadEpisodicMemvidFailureKind::CapacityExceeded
            );
            let capsule_id = resolved.request.capsule_id.clone();
            let error_message = error.message;
            let persisted = match persist_refill_job_failure(
                crud_store.as_ref(),
                &job,
                retryable,
                capacity_error,
                Some(error_message.clone()),
                Some(resolved.source_payload.as_str()),
                now_unix,
                attempt_started_at,
                config,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    let (outcome, retryable) = release_refill_claim_after_persistence_error(
                        crud_store.clone(),
                        &job,
                        now_unix,
                        config,
                        retryable,
                        &error,
                        Some(error_message.as_str()),
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to recover refill claim after backend persistence error: {error:#}"
                        )
                    })?;
                    if outcome == ThreadEpisodicIndexAttemptOutcome::Applied {
                        if retryable {
                            summary.failed_retryable_jobs =
                                summary.failed_retryable_jobs.saturating_add(1);
                        } else {
                            summary.failed_terminal_jobs =
                                summary.failed_terminal_jobs.saturating_add(1);
                        }
                    }
                    return Ok(());
                }
            };
            match persisted {
                ThreadEpisodicIndexAttemptOutcome::Applied => {
                    if capacity_error {
                        update_refill_capsule_capacity(
                            crud_store.as_ref(),
                            capsule_id.as_str(),
                            &ThreadEpisodicMemvidStats::default(),
                            Some(error_message),
                            true,
                            now_unix,
                            config,
                        )
                        .await;
                        rotate_refill_capsule_after_capacity_event(
                            crud_store.as_ref(),
                            capsule_id.as_str(),
                            now_unix,
                            "capacity_exceeded",
                        )
                        .await;
                    }
                }
                ThreadEpisodicIndexAttemptOutcome::SourceChanged
                | ThreadEpisodicIndexAttemptOutcome::Excluded => {
                    reconcile_and_release_refill_claim(crud_store.clone(), &job, now_unix, config)
                        .await?;
                    return Ok(());
                }
                ThreadEpisodicIndexAttemptOutcome::StaleAttempt => return Ok(()),
            }
            if retryable {
                summary.failed_retryable_jobs = summary.failed_retryable_jobs.saturating_add(1);
            } else {
                summary.failed_terminal_jobs = summary.failed_terminal_jobs.saturating_add(1);
            }
        }
    }

    Ok(())
}

async fn reconcile_refill_job_source(
    crud_store: Arc<CrudStore>,
    job: &ThreadEpisodicIndexJobRecord,
    now_unix: i64,
) -> Result<Option<pioneer_crud::ThreadEpisodicSourceReconcileOutcome>> {
    let Some(item) = crud_store
        .find_thread_episodic_item(job.index_item_id.as_str())
        .await?
    else {
        return Ok(None);
    };
    let outcome = StoreThreadEpisodicIngestor::with_config(crud_store, true)
        .reconcile_canonical_source_occurrence(
            item.workspace_id.as_str(),
            item.thread_id.as_str(),
            item.turn_id.as_str(),
            item.item_id.as_str(),
            now_unix,
        )
        .await?;
    Ok(Some(outcome))
}

async fn release_refill_claim_after_persistence_error(
    crud_store: Arc<CrudStore>,
    job: &ThreadEpisodicIndexJobRecord,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
    retryable: bool,
    persistence_error: &anyhow::Error,
    original_result: Option<&str>,
) -> Result<(ThreadEpisodicIndexAttemptOutcome, bool)> {
    let retryable = retryable && job.attempt_count < config.max_attempts;
    let update = ThreadEpisodicIndexJobFailureUpdate {
        retryable,
        next_run_at_unix: retryable.then(|| next_refill_retry_at(job, now_unix, config)),
        last_error: Some(sanitize_refill_index_error(
            format!(
                "failed to persist thread episodic attempt result: {persistence_error:#}; original result: {}",
                original_result.unwrap_or("unknown index attempt result")
            )
            .as_str(),
        )),
        capacity_error: false,
        last_attempt_latency_ms: None,
    };
    let outcome = crud_store
        .recover_thread_episodic_index_attempt_after_persistence_error(
            job.id.as_str(),
            job.attempt_count,
            update,
            now_unix,
        )
        .await
        .context("failed to conditionally recover refill claim after persistence error")?;
    if outcome == ThreadEpisodicIndexAttemptOutcome::Applied {
        reconcile_refill_job_source(crud_store, job, now_unix)
            .await
            .context("failed to reconcile source after recording refill persistence failure")?;
    }
    Ok((outcome, retryable))
}

async fn reconcile_and_release_refill_claim(
    crud_store: Arc<CrudStore>,
    job: &ThreadEpisodicIndexJobRecord,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
) -> Result<ThreadEpisodicIndexAttemptOutcome> {
    match reconcile_refill_job_source(crud_store.clone(), job, now_unix).await {
        Ok(Some(pioneer_crud::ThreadEpisodicSourceReconcileOutcome::PreservedExclusion)) => {
            crud_store
                .cancel_thread_episodic_index_attempt(
                    job.id.as_str(),
                    job.attempt_count,
                    THREAD_EPISODIC_USER_EXCLUDED_ERROR,
                    now_unix,
                )
                .await
        }
        Ok(Some(pioneer_crud::ThreadEpisodicSourceReconcileOutcome::PreservedDeletion)) => {
            crud_store
                .cancel_thread_episodic_index_attempt(
                    job.id.as_str(),
                    job.attempt_count,
                    THREAD_EPISODIC_USER_DELETED_ERROR,
                    now_unix,
                )
                .await
        }
        Ok(Some(pioneer_crud::ThreadEpisodicSourceReconcileOutcome::Current))
            if job.attempt_count >= config.max_attempts =>
        {
            crud_store
                .fail_thread_episodic_index_attempt_without_source_validation(
                    job.id.as_str(),
                    job.attempt_count,
                    ThreadEpisodicIndexJobFailureUpdate {
                        retryable: false,
                        next_run_at_unix: None,
                        last_error: Some(
                            "thread episodic source changed repeatedly while resolving the same claim"
                                .to_owned(),
                        ),
                        capacity_error: false,
                        last_attempt_latency_ms: None,
                    },
                    now_unix,
                )
                .await
        }
        Ok(None) => {
            crud_store
                .fail_thread_episodic_index_attempt_without_source_validation(
                    job.id.as_str(),
                    job.attempt_count,
                    ThreadEpisodicIndexJobFailureUpdate {
                        retryable: false,
                        next_run_at_unix: None,
                        last_error: Some(
                            "thread episodic index item disappeared during source reconciliation"
                                .to_owned(),
                        ),
                        capacity_error: false,
                        last_attempt_latency_ms: None,
                    },
                    now_unix,
                )
                .await
        }
        Ok(_) => {
            crud_store
                .requeue_thread_episodic_index_attempt(
                    job.id.as_str(),
                    job.attempt_count,
                    now_unix,
                    None,
                )
                .await
        }
        Err(error) => {
            persist_refill_reconciliation_error(
                crud_store,
                job,
                now_unix,
                config,
                error,
                #[cfg(test)]
                None,
                #[cfg(test)]
                None,
            )
            .await
        }
    }
}

async fn persist_refill_reconciliation_error(
    crud_store: Arc<CrudStore>,
    job: &ThreadEpisodicIndexJobRecord,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
    reconciliation_error: anyhow::Error,
    #[cfg(test)] injected_primary_error: Option<&str>,
    #[cfg(test)] injected_fallback_error: Option<&str>,
) -> Result<ThreadEpisodicIndexAttemptOutcome> {
    let retryable = job.attempt_count < config.max_attempts;
    let next_run_at_unix = retryable.then(|| next_refill_retry_at(job, now_unix, config));
    let reconciliation_error = format!("{reconciliation_error:#}");
    let update = ThreadEpisodicIndexJobFailureUpdate {
        retryable,
        next_run_at_unix,
        last_error: Some(sanitize_refill_index_error(
            format!("failed to reconcile changed thread episodic source: {reconciliation_error}")
                .as_str(),
        )),
        capacity_error: false,
        last_attempt_latency_ms: None,
    };
    let primary = {
        #[cfg(test)]
        if let Some(error) = injected_primary_error {
            Err(anyhow::anyhow!(error.to_owned()))
        } else {
            crud_store
                .fail_thread_episodic_index_attempt_without_source_validation(
                    job.id.as_str(),
                    job.attempt_count,
                    update,
                    now_unix,
                )
                .await
        }
        #[cfg(not(test))]
        crud_store
            .fail_thread_episodic_index_attempt_without_source_validation(
                job.id.as_str(),
                job.attempt_count,
                update,
                now_unix,
            )
            .await
    };
    match primary {
        Ok(outcome) => Ok(outcome),
        Err(persist_error) => {
            let persist_error = format!("{persist_error:#}");
            let recovery_update = ThreadEpisodicIndexJobFailureUpdate {
                retryable,
                next_run_at_unix,
                last_error: Some(sanitize_refill_index_error(
                    format!(
                        "failed to reconcile changed thread episodic source: {reconciliation_error}; failed to persist reconciliation outcome: {persist_error}"
                    )
                    .as_str(),
                )),
                capacity_error: false,
                last_attempt_latency_ms: None,
            };
            let recovered = {
                #[cfg(test)]
                if let Some(error) = injected_fallback_error {
                    Err(anyhow::anyhow!(error.to_owned()))
                } else {
                    crud_store
                        .recover_thread_episodic_index_attempt_after_persistence_error(
                            job.id.as_str(),
                            job.attempt_count,
                            recovery_update,
                            now_unix,
                        )
                        .await
                }
                #[cfg(not(test))]
                crud_store
                    .recover_thread_episodic_index_attempt_after_persistence_error(
                        job.id.as_str(),
                        job.attempt_count,
                        recovery_update,
                        now_unix,
                    )
                    .await
            };
            recovered.with_context(|| {
                format!(
                    "failed to persist thread episodic reconciliation error after primary persistence failure `{persist_error}`; original reconciliation error: {reconciliation_error}"
                )
            })
        }
    }
}

async fn persist_successful_refill_item(
    crud_store: &CrudStore,
    job: &ThreadEpisodicIndexJobRecord,
    resolved: ThreadEpisodicResolvedIndexRequest,
    output: ThreadEpisodicMemvidIndexOutput,
    now_unix: i64,
    attempt_started_at: Instant,
) -> Result<ThreadEpisodicIndexAttemptOutcome> {
    let outcome = crud_store
        .complete_thread_episodic_index_attempt(
            job.id.as_str(),
            job.attempt_count,
            resolved.source_payload.as_str(),
            ThreadEpisodicItemIndexedUpdate {
                capsule_id: resolved.request.capsule_id.clone(),
                capsule_ref: resolved.request.capsule_ref.clone(),
                segment_index: resolved.segment_index,
                frame_id: output.frame_id,
                frame_uri: output.frame_uri.clone(),
                embedding_artifact_id: resolved.embedding_artifact_id.clone(),
            },
            ThreadEpisodicIndexJobCompletionUpdate {
                capsule_id: resolved.request.capsule_id,
                capsule_ref: resolved.request.capsule_ref,
                segment_index: resolved.segment_index,
                frame_uri: output.frame_uri,
                last_attempt_latency_ms: Some(elapsed_ms(attempt_started_at)),
            },
            now_unix,
        )
        .await
        .with_context(|| {
            format!(
                "failed to commit thread episodic refill attempt `{}`",
                job.id
            )
        })?;
    Ok(outcome)
}

async fn persist_refill_job_failure(
    crud_store: &CrudStore,
    job: &ThreadEpisodicIndexJobRecord,
    retryable: bool,
    capacity_error: bool,
    error_message: Option<String>,
    expected_source_payload: Option<&str>,
    now_unix: i64,
    attempt_started_at: Instant,
    config: ThreadEpisodicIndexExecutorConfig,
) -> Result<ThreadEpisodicIndexAttemptOutcome> {
    let next_run_at_unix = if retryable && capacity_error {
        Some(now_unix)
    } else {
        retryable.then(|| next_refill_retry_at(job, now_unix, config))
    };
    let sanitized_error =
        error_message.map(|message| sanitize_refill_index_error(message.as_str()));
    let update = ThreadEpisodicIndexJobFailureUpdate {
        retryable,
        next_run_at_unix,
        last_error: sanitized_error,
        capacity_error,
        last_attempt_latency_ms: Some(elapsed_ms(attempt_started_at)),
    };
    let persisted = if let Some(expected_source_payload) = expected_source_payload {
        crud_store
            .fail_thread_episodic_index_attempt(
                job.id.as_str(),
                job.attempt_count,
                expected_source_payload,
                update,
                now_unix,
            )
            .await
    } else {
        crud_store
            .fail_thread_episodic_index_attempt_without_source_validation(
                job.id.as_str(),
                job.attempt_count,
                update,
                now_unix,
            )
            .await
    };
    let outcome = persisted.with_context(|| {
        format!(
            "failed to persist thread episodic refill job `{}` failure",
            job.id
        )
    })?;
    if outcome == ThreadEpisodicIndexAttemptOutcome::StaleAttempt {
        return Ok(outcome);
    }

    Ok(outcome)
}

async fn update_refill_capsule_capacity(
    crud_store: &CrudStore,
    capsule_id: &str,
    stats: &ThreadEpisodicMemvidStats,
    last_error: Option<String>,
    capacity_exceeded: bool,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
) {
    let now = fixed_datetime_from_unix(now_unix);
    let near_capacity_at = stats
        .utilization_percent
        .and_then(|value| (value >= config.near_capacity_percent).then_some(now));
    let update = ThreadEpisodicCapsuleCapacityUpdate {
        capacity_bytes: stats.capacity_bytes,
        size_bytes: stats.size_bytes,
        utilization_percent: stats.utilization_percent,
        active_frame_count: stats.active_frame_count,
        near_capacity_at,
        capacity_exceeded_at: capacity_exceeded.then_some(now),
        last_error,
    };
    if let Err(error) = crud_store
        .update_thread_episodic_capsule_capacity(capsule_id, update, now_unix)
        .await
    {
        warn!(
            capsule_id,
            error = %error,
            "failed to update thread episodic refill capsule capacity metadata"
        );
    }
}

async fn rotate_refill_capsule_after_capacity_event(
    crud_store: &CrudStore,
    capsule_id: &str,
    now_unix: i64,
    reason: &str,
) {
    match crud_store
        .transition_thread_episodic_active_write_segment(
            capsule_id,
            ThreadEpisodicCapsuleWriteState::Full,
            now_unix,
        )
        .await
    {
        Ok(Some(rotated)) => {
            info!(
                capsule_id = %rotated.id,
                segment_index = rotated.segment_index,
                reason,
                "thread episodic workspace refill rotated active segment to full"
            );
        }
        Ok(None) => {
            warn!(
                capsule_id,
                reason, "thread episodic workspace refill segment rotation skipped"
            );
        }
        Err(error) => {
            warn!(
                capsule_id,
                reason,
                error = %error,
                "thread episodic workspace refill failed to rotate active segment"
            );
        }
    }
}

fn next_refill_retry_at(
    job: &ThreadEpisodicIndexJobRecord,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
) -> i64 {
    let exponent = job.attempt_count.saturating_sub(1).clamp(0, 8) as u32;
    let delay = config
        .retry_base_delay_secs
        .saturating_mul(2_i64.saturating_pow(exponent))
        .min(config.retry_max_delay_secs);
    now_unix.saturating_add(delay)
}

fn sanitize_refill_index_error(message: &str) -> String {
    let mut sanitized = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if sanitized.chars().count() > REFILL_INDEX_ERROR_MAX_CHARS {
        sanitized = sanitized
            .chars()
            .take(REFILL_INDEX_ERROR_MAX_CHARS)
            .collect();
    }
    sanitized
}

fn fixed_datetime_from_unix(value: i64) -> DateTimeWithTimeZone {
    chrono::DateTime::from_timestamp(value, 0)
        .unwrap_or_else(chrono::Utc::now)
        .fixed_offset()
}

fn elapsed_ms(started_at: Instant) -> i64 {
    started_at.elapsed().as_millis().min(i64::MAX as u128) as i64
}

enum CapsuleFileDeleteOutcome {
    Deleted,
    Missing,
    NonFileUri,
}

async fn delete_capsule_file(
    storage_uri: &str,
    ownership: ThreadEpisodicWorkspaceOwnership,
) -> Result<CapsuleFileDeleteOutcome> {
    let Some(path) = storage_uri.strip_prefix("file://") else {
        return Ok(CapsuleFileDeleteOutcome::NonFileUri);
    };
    match remove_thread_episodic_capsule_file(Path::new(path), ownership).await {
        Ok(()) => Ok(CapsuleFileDeleteOutcome::Deleted),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(CapsuleFileDeleteOutcome::Missing)
        }
        Err(error) => Err(error)
            .with_context(|| format!("failed to delete thread episodic capsule file `{path}`")),
    }
}

async fn mark_refill_preparing<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<()> {
    upsert_refill_progress_marker(
        db,
        workspace_id,
        PROJECTION_META_STATUS_PENDING,
        None,
        None,
        projection_target,
    )
    .await
}

async fn mark_refill_backfilling<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    summary: &ThreadEpisodicWorkspaceCapsuleRefillSummary,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<()> {
    upsert_refill_progress_marker(
        db,
        workspace_id,
        PROJECTION_META_STATUS_BACKFILLING,
        Some(summary),
        None,
        projection_target,
    )
    .await
}

async fn mark_refill_preparation_failed<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    error: &anyhow::Error,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<()> {
    upsert_refill_progress_marker(
        db,
        workspace_id,
        PROJECTION_META_STATUS_PENDING,
        None,
        Some(format!("{error:#}")),
        projection_target,
    )
    .await
}

async fn upsert_refill_progress_marker<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    status: &str,
    summary: Option<&ThreadEpisodicWorkspaceCapsuleRefillSummary>,
    last_error: Option<String>,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<()> {
    let now = now_datetime();
    let projection_key = refill_projection_key_for_workspace(workspace_id)?;
    upsert_projection_meta_with_config(
        db,
        ProjectionMetaRecord {
            projection_key,
            projection_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            status: status.to_owned(),
            source_thread_count: summary.map_or(0, |summary| summary.source_thread_count),
            source_turn_count: summary.map_or(0, |summary| summary.source_turn_count),
            source_turn_item_count: summary.map_or(0, |summary| summary.source_turn_item_count),
            source_turn_event_count: 0,
            last_error,
            backfill_started_at: Some(now),
            backfilled_at: None,
            created_at: now,
            updated_at: now,
        },
        projection_target.meta_config_record(),
    )
    .await
}

async fn mark_refill_complete<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    summary: &ThreadEpisodicWorkspaceCapsuleRefillSummary,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<()> {
    let now = now_datetime();
    let projection_key = refill_projection_key_for_workspace(workspace_id)?;
    upsert_projection_meta_with_config(
        db,
        ProjectionMetaRecord {
            projection_key,
            projection_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            status: PROJECTION_META_STATUS_COMPLETE.to_owned(),
            source_thread_count: summary.source_thread_count,
            source_turn_count: summary.source_turn_count,
            source_turn_item_count: summary.source_turn_item_count,
            source_turn_event_count: saturating_i64_from_u64(
                (summary.completed_jobs.min(i64::MAX as usize) as u64)
                    .saturating_add(summary.capsule_files_deleted),
            ),
            last_error: None,
            backfill_started_at: Some(now),
            backfilled_at: Some(now),
            created_at: now,
            updated_at: now,
        },
        projection_target.meta_config_record(),
    )
    .await
}

fn saturating_i64_from_u64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

// Extra jobs report errors without withdrawing already usable frames.
async fn record_complete_refill_error<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    error: &anyhow::Error,
) -> Result<()> {
    let key = refill_projection_key_for_workspace(workspace_id)?;
    let message = format!("{error:#}");
    update_projection_meta_status(
        db,
        &key,
        PROJECTION_META_STATUS_COMPLETE,
        Some(&message),
        now_datetime(),
    )
    .await?;
    Ok(())
}

async fn mark_refill_failed<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    error: &anyhow::Error,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<()> {
    let now = now_datetime();
    let projection_key = refill_projection_key_for_workspace(workspace_id)?;
    let last_error = format!("{error:#}");
    if update_projection_meta_status(
        db,
        projection_key.as_str(),
        PROJECTION_META_STATUS_FAILED,
        Some(last_error.as_str()),
        now,
    )
    .await?
    {
        return Ok(());
    }

    upsert_projection_meta_with_config(
        db,
        ProjectionMetaRecord {
            projection_key,
            projection_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            status: PROJECTION_META_STATUS_FAILED.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: Some(last_error),
            backfill_started_at: None,
            backfilled_at: None,
            created_at: now,
            updated_at: now,
        },
        projection_target.meta_config_record(),
    )
    .await
}

fn now_datetime() -> DateTimeWithTimeZone {
    chrono::Utc::now().fixed_offset()
}

fn notify_refill_status(
    sender: Option<&ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    workspace_id: &str,
    status: GatewayThreadEpisodicVectorRefillStatus,
) {
    let Some(sender) = sender else {
        return;
    };
    let _ = sender.send(ThreadEpisodicWorkspaceCapsuleRefillStatusEvent {
        workspace_id: workspace_id.to_owned(),
        status,
        local_model_status: None,
        downloaded_bytes: None,
        total_bytes: None,
    });
}

fn notify_local_model_download_progress(
    sender: Option<&ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    workspace_id: &str,
    progress: crate::thread_episodic_embedding::LocalEmbeddingModelDownloadProgress,
) {
    let Some(sender) = sender else {
        return;
    };
    let _ = sender.send(ThreadEpisodicWorkspaceCapsuleRefillStatusEvent {
        workspace_id: workspace_id.to_owned(),
        status: GatewayThreadEpisodicVectorRefillStatus::Running,
        local_model_status: Some(
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Downloading,
        ),
        downloaded_bytes: Some(progress.downloaded_bytes),
        total_bytes: progress.total_bytes,
    });
}

fn notify_local_model_status(
    sender: Option<&ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    workspace_id: &str,
    status: GatewayThreadEpisodicVectorRefillStatus,
    local_model_status: pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus,
) {
    let Some(sender) = sender else {
        return;
    };
    let _ = sender.send(ThreadEpisodicWorkspaceCapsuleRefillStatusEvent {
        workspace_id: workspace_id.to_owned(),
        status,
        local_model_status: Some(local_model_status),
        downloaded_bytes: None,
        total_bytes: None,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::bootstrap;
    use crate::thread_episodic::{
        StoreThreadEpisodicIngestor, ThreadEpisodicCommittedItem, ThreadEpisodicIngestor,
    };
    use crate::thread_episodic_embedding::CHUNKED_EMBEDDING_INPUT_ERROR_MARKER;
    use crate::workspace::WorkspaceManager;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        NewThreadEpisodicExclusionRecord, NewThreadEpisodicIndexJobRecord,
        NewThreadEpisodicItemRecord, NewThreadEpisodicThreadDirectoryRecord,
        ThreadEpisodicActiveWriteSegmentRequest, ThreadEpisodicCapsuleStatus,
        ThreadEpisodicExclusionReason, ThreadEpisodicGraphEnrichmentState,
        ThreadEpisodicIndexJobStatus, ThreadEpisodicItemStatus, ThreadEpisodicItemVisibility,
        ThreadEpisodicSourceActorRole, ThreadEpisodicSourceRuntimeKind,
        ThreadEpisodicThreadDirectoryStatus, ThreadEpisodicThreadDirectoryVisibility,
    };
    use pioneer_memory::{
        ThreadEpisodicEmbeddingError, ThreadEpisodicExactSourceTarget,
        ThreadEpisodicMemvidSearchRequest, ThreadEpisodicMemvidSearchSegment,
        ThreadEpisodicSearchProfile, ThreadEpisodicSearchProfileKind,
    };
    use pioneer_protocol::{
        ItemCompletedNotification, ItemUpdatedNotification, SandboxMode, TaskExecutorKind,
        TaskStatus, TaskTriggerKind, TaskTurnItem, Thread,
        ThreadEpisodicSourceActorRole as ProtocolThreadEpisodicSourceActorRole,
        ThreadEpisodicSourceContext, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, Turn, TurnItem, TurnItemType, TurnKind, TurnOrigin, TurnStatus, UserInput,
    };
    use pioneer_sqlite::SqliteDatabase;
    use sea_orm::{
        ConnectOptions, ConnectionTrait, Database, DatabaseBackend, EntityTrait, Statement,
        TransactionTrait,
    };
    use sha2::{Digest, Sha256};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    #[test]
    fn nomic_preparation_version_invalidates_legacy_projection_and_resume_gate() {
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_projection_parts(
            true,
            Some("local".to_owned()),
            Some("nomic-embed-text-v1.5".to_owned()),
            Some(768),
            Some(true),
        );
        let mut legacy = serde_json::to_value(&target.payload).unwrap();
        legacy
            .as_object_mut()
            .unwrap()
            .remove("preparation_version");
        let legacy_json = legacy.to_string();
        let legacy_meta = ProjectionMetaRecordLike {
            projection_config_hash: Some("legacy"),
            projection_config_json: Some(&legacy_json),
        };
        assert!(!target.matches_projection_meta(&legacy_meta));
        assert!(!target.matches_projection_meta_selection(&legacy_meta));
        let current_meta = ProjectionMetaRecordLike {
            projection_config_hash: Some(&target.config_hash),
            projection_config_json: Some(&target.payload_json),
        };
        assert!(target.matches_projection_meta(&current_meta));
        assert!(target.matches_projection_meta_selection(&current_meta));
        for model in ["bge-small-en-v1.5", "bge-base-en-v1.5", "gte-large"] {
            let target =
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_projection_parts(
                    true,
                    Some("local".to_owned()),
                    Some(model.to_owned()),
                    Some(
                        pioneer_provider::providers::local_embedding_model_info(model)
                            .unwrap()
                            .dimension as u32,
                    ),
                    Some(true),
                );
            assert!(target.payload.preparation_version.is_none());
            assert!(!target.payload_json.contains("preparation_version"));
        }
    }

    #[test]
    fn local_model_progress_events_carry_bytes_and_terminal_status_clears_them() {
        let (sender, mut receiver) = tokio::sync::broadcast::channel(4);

        notify_local_model_download_progress(
            Some(&sender),
            "workspace-a",
            crate::thread_episodic_embedding::LocalEmbeddingModelDownloadProgress {
                downloaded_bytes: 16 * 1024 * 1024,
                total_bytes: Some(64 * 1024 * 1024),
            },
        );
        let progress = receiver.try_recv().expect("progress event");
        assert_eq!(progress.workspace_id, "workspace-a");
        assert_eq!(
            progress.status,
            GatewayThreadEpisodicVectorRefillStatus::Running
        );
        assert_eq!(
            progress.local_model_status,
            Some(pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Downloading)
        );
        assert_eq!(progress.downloaded_bytes, Some(16 * 1024 * 1024));
        assert_eq!(progress.total_bytes, Some(64 * 1024 * 1024));

        notify_local_model_status(
            Some(&sender),
            "workspace-a",
            GatewayThreadEpisodicVectorRefillStatus::Running,
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Installed,
        );
        let installed = receiver.try_recv().expect("installed event");
        assert_eq!(
            installed.local_model_status,
            Some(pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Installed)
        );
        assert_eq!(installed.downloaded_bytes, None);
        assert_eq!(installed.total_bytes, None);
    }

    struct StaticThreadEpisodicEmbeddingProvider {
        provider_id: &'static str,
        model: &'static str,
        dimension: usize,
        normalized: bool,
        embedding: Vec<f32>,
        error: Option<ThreadEpisodicEmbeddingError>,
        calls: AtomicUsize,
    }

    impl StaticThreadEpisodicEmbeddingProvider {
        fn new(embedding: Vec<f32>) -> Self {
            Self::with_identity("openai", "text-embedding-3-small", embedding)
        }

        fn with_identity(
            provider_id: &'static str,
            model: &'static str,
            embedding: Vec<f32>,
        ) -> Self {
            Self::with_declared_dimension(provider_id, model, embedding.len(), embedding)
        }

        fn with_declared_dimension(
            provider_id: &'static str,
            model: &'static str,
            dimension: usize,
            embedding: Vec<f32>,
        ) -> Self {
            Self {
                provider_id,
                model,
                dimension,
                normalized: true,
                embedding,
                error: None,
                calls: AtomicUsize::new(0),
            }
        }

        fn retryable_failure() -> Self {
            Self {
                provider_id: "openai",
                model: "text-embedding-3-small",
                dimension: 3,
                normalized: true,
                embedding: vec![0.1, 0.2, 0.3],
                error: Some(ThreadEpisodicEmbeddingError::retryable_provider_failure(
                    "openai",
                    "text-embedding-3-small",
                    "rate limited",
                )),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl ThreadEpisodicEmbeddingProvider for StaticThreadEpisodicEmbeddingProvider {
        fn provider_id(&self) -> &str {
            self.provider_id
        }

        fn model(&self) -> &str {
            self.model
        }

        fn dimension(&self) -> usize {
            self.dimension
        }

        fn normalized(&self) -> bool {
            self.normalized
        }

        fn embed_text(
            &self,
            _text: &str,
        ) -> std::result::Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.error.clone() {
                return Err(error);
            }
            Ok(self.embedding.clone())
        }
    }

    struct ScriptedThreadEpisodicEmbeddingProvider {
        responses: Mutex<VecDeque<std::result::Result<Vec<f32>, ThreadEpisodicEmbeddingError>>>,
        fallback_embedding: Vec<f32>,
        calls: AtomicUsize,
    }

    impl ScriptedThreadEpisodicEmbeddingProvider {
        fn new(
            responses: Vec<std::result::Result<Vec<f32>, ThreadEpisodicEmbeddingError>>,
        ) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                fallback_embedding: vec![0.1, 0.2, 0.3],
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl ThreadEpisodicEmbeddingProvider for ScriptedThreadEpisodicEmbeddingProvider {
        fn provider_id(&self) -> &str {
            "openai"
        }

        fn model(&self) -> &str {
            "text-embedding-3-small"
        }

        fn dimension(&self) -> usize {
            self.fallback_embedding.len()
        }

        fn normalized(&self) -> bool {
            true
        }

        fn embed_text(
            &self,
            _text: &str,
        ) -> std::result::Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.responses
                .lock()
                .expect("scripted embedding responses lock")
                .pop_front()
                .unwrap_or_else(|| Ok(self.fallback_embedding.clone()))
        }
    }

    struct BlockingFirstFailureEmbeddingProvider {
        started: std::sync::mpsc::SyncSender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        calls: AtomicUsize,
    }

    impl BlockingFirstFailureEmbeddingProvider {
        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl ThreadEpisodicEmbeddingProvider for BlockingFirstFailureEmbeddingProvider {
        fn provider_id(&self) -> &str {
            "openai"
        }

        fn model(&self) -> &str {
            "text-embedding-3-small"
        }

        fn dimension(&self) -> usize {
            3
        }

        fn normalized(&self) -> bool {
            true
        }

        fn embed_text(
            &self,
            _text: &str,
        ) -> std::result::Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                self.started
                    .send(())
                    .expect("embedding test should announce its first call");
                self.release
                    .lock()
                    .expect("embedding release lock")
                    .recv()
                    .expect("embedding test should release its first call");
                return Err(ThreadEpisodicEmbeddingError::retryable_provider_failure(
                    "openai",
                    "text-embedding-3-small",
                    "controlled provider failure for stale source",
                ));
            }
            Ok(vec![0.1, 0.2, 0.3])
        }
    }

    fn immediate_retry_config(max_attempts: i64) -> ThreadEpisodicIndexExecutorConfig {
        ThreadEpisodicIndexExecutorConfig {
            retry_base_delay_secs: 0,
            retry_max_delay_secs: 0,
            max_attempts,
            ..ThreadEpisodicIndexExecutorConfig::default()
        }
    }

    async fn refill_once_with_test_executor_config(
        crud_store: Arc<CrudStore>,
        thread_episodic_storage_root: &Path,
        workspace_id: &str,
        projection_target: ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
        embedding_provider: Arc<dyn ThreadEpisodicEmbeddingProvider>,
        executor_config: ThreadEpisodicIndexExecutorConfig,
    ) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
        let resolver = Arc::new(FixedThreadEpisodicIndexEmbeddingProviderResolver::new(
            Some(embedding_provider),
        )) as Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>;
        refill_once_with_projection_resolver_and_config(
            crud_store,
            thread_episodic_storage_root,
            workspace_id,
            projection_target,
            Some(resolver),
            None,
            executor_config,
            None,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_complete_marker_skips_migration() {
        let (crud_store, temp_dir, _workspace_id) = setup_store().await;
        mark_refill_marker(
            crud_store.as_ref(),
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
        )
        .await;

        let summary = refill_once(crud_store, temp_dir.path())
            .await
            .expect("complete marker should skip");

        assert!(summary.skipped);
    }

    #[tokio::test]
    async fn thread_episodic_refill_marker_records_lexical_projection_identity() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only();

        let summary = refill_once_with_workspace_projection(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
            None,
        )
        .await
        .expect("empty refill should complete");

        assert!(!summary.skipped);
        let meta = find_projection_meta(
            &crud_store.database_connection(),
            refill_projection_key_for_workspace(workspace_id.as_str())
                .expect("workspace refill key should build")
                .as_str(),
        )
        .await
        .expect("meta query should succeed")
        .expect("meta exists");
        assert_eq!(
            meta.projection_config_hash.as_deref(),
            Some(target.config_hash.as_str())
        );
        assert_eq!(
            meta.projection_config_json.as_deref(),
            Some(target.payload_json.as_str())
        );
        assert!(
            refill_is_current(crud_store.as_ref())
                .await
                .expect("current check")
        );
    }

    #[tokio::test]
    async fn thread_episodic_refill_marker_treats_vector_marker_as_stale_for_lexical_target() {
        let (crud_store, _temp_dir, workspace_id) = setup_store().await;
        let vector_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &GatewayThreadEpisodicVectorSearchConfig {
                    enabled: true,
                    provider: Some(GatewayThreadEpisodicVectorProviderConfig::OpenAi),
                    model: Some("text-embedding-3-small".to_owned()),
                    local_model: Some("bge-small-en-v1.5".to_owned()),
                    embedding_normalized: true,
                    use_search_instructions: false,
                },
            );
        mark_refill_marker_with_workspace_target(
            crud_store.as_ref(),
            workspace_id.as_str(),
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &vector_target,
        )
        .await;

        assert!(
            !refill_is_current(crud_store.as_ref())
                .await
                .expect("lexical current check")
        );
        assert!(
            refill_is_current_for_target(crud_store.as_ref(), &vector_target)
                .await
                .expect("vector current check")
        );
    }

    #[tokio::test]
    async fn thread_episodic_refill_marker_invalidates_vector_identity_changes() {
        let (crud_store, _temp_dir, _workspace_id) = setup_store().await;
        let base_config = GatewayThreadEpisodicVectorSearchConfig {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProviderConfig::OpenAi),
            model: Some("text-embedding-3-small".to_owned()),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            embedding_normalized: true,
            use_search_instructions: false,
        };
        let base_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &base_config,
            );
        mark_refill_marker_with_target(
            crud_store.as_ref(),
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &base_target,
        )
        .await;

        for changed_config in [
            GatewayThreadEpisodicVectorSearchConfig {
                model: Some("text-embedding-3-large".to_owned()),
                ..base_config.clone()
            },
            GatewayThreadEpisodicVectorSearchConfig {
                embedding_normalized: false,
                ..base_config.clone()
            },
            GatewayThreadEpisodicVectorSearchConfig {
                provider: Some(GatewayThreadEpisodicVectorProviderConfig::OpenRouter),
                model: Some("openai/text-embedding-3-small".to_owned()),
                ..base_config.clone()
            },
        ] {
            let changed_target =
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                    &changed_config,
                );
            assert!(
                !refill_is_current_for_target(crud_store.as_ref(), &changed_target)
                    .await
                    .expect("changed vector current check"),
                "changed config should be stale: {changed_config:?}"
            );
        }
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_lock_prevents_duplicate_migration() {
        let (crud_store, temp_dir, _workspace_id) = setup_store().await;
        let _guard = try_acquire_refill_lock(temp_dir.path())
            .expect("lock acquisition should not error")
            .expect("first lock should be acquired");
        let (status_sender, mut status_receiver) = tokio::sync::broadcast::channel(4);

        let summary = refill_once_with_projection_resolver(
            crud_store.clone(),
            temp_dir.path(),
            LEGACY_REFILL_WORKSPACE_ID,
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
            None,
            Some(&status_sender),
        )
        .await
        .expect("contended refill should skip without error");

        assert!(summary.skipped);
        assert!(summary.lock_contended);
        assert!(matches!(
            status_receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        assert!(
            find_projection_meta(
                &crud_store.database_connection(),
                THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY,
            )
            .await
            .expect("meta query should succeed")
            .is_none()
        );
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_missing_marker_marks_empty_database_complete() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;

        let summary = refill_once_with_workspace_projection(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
            None,
        )
        .await
        .expect("empty refill should complete");

        assert!(!summary.skipped);
        assert_eq!(summary.workspace_count, 1);
        let meta = find_projection_meta(
            &crud_store.database_connection(),
            refill_projection_key_for_workspace(workspace_id.as_str())
                .expect("workspace refill key should build")
                .as_str(),
        )
        .await
        .expect("meta query should succeed")
        .expect("meta exists");
        assert_eq!(meta.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(
            meta.projection_version,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
        );

        let skipped = refill_once_with_workspace_projection(
            crud_store,
            temp_dir.path(),
            workspace_id.as_str(),
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
            None,
        )
        .await
        .expect("second refill should skip");
        assert!(skipped.skipped);
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_retries_failed_backfilling_and_old_versions() {
        for (status, version) in [
            (PROJECTION_META_STATUS_FAILED, 1),
            (PROJECTION_META_STATUS_BACKFILLING, 1),
            (PROJECTION_META_STATUS_COMPLETE, 0),
        ] {
            let (crud_store, temp_dir, workspace_id) = setup_store().await;
            mark_refill_marker_with_workspace_target(
                crud_store.as_ref(),
                &workspace_id,
                status,
                version,
                &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
            )
            .await;

            let summary = refill_once(crud_store.clone(), temp_dir.path())
                .await
                .expect("non-current marker should retry");

            assert!(!summary.skipped, "status={status} version={version}");
            let meta = find_projection_meta(
                &crud_store.database_connection(),
                &refill_projection_key_for_workspace(&workspace_id).unwrap(),
            )
            .await
            .expect("meta query should succeed")
            .expect("meta exists");
            assert_eq!(meta.status, PROJECTION_META_STATUS_COMPLETE);
            assert_eq!(
                meta.projection_version,
                THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
            );
        }
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_cleanup_resets_derived_artifacts_rerunnably() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let old_capsule = crud_store
            .resolve_thread_episodic_active_write_segment(
                ThreadEpisodicActiveWriteSegmentRequest {
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_capsule".to_owned(),
                    storage_uri_root: thread_episodic_storage_uri_from_path(temp_dir.path()),
                },
                1_700_000_000,
            )
            .await
            .expect("old per-thread capsule should resolve");
        let old_capsule_path = PathBuf::from(
            old_capsule
                .storage_uri
                .strip_prefix("file://")
                .expect("test storage uri should be file uri"),
        );
        tokio::fs::create_dir_all(
            old_capsule_path
                .parent()
                .expect("old capsule should have parent directory"),
        )
        .await
        .expect("old capsule parent dir should be created");
        tokio::fs::write(&old_capsule_path, b"stale old mv2")
            .await
            .expect("old capsule file should be created");
        let orphan_capsule_path = temp_dir
            .path()
            .join("thread_episodic")
            .join("orphan_workspace")
            .join("orphan_thread")
            .join("orphan.mv2");
        tokio::fs::create_dir_all(
            orphan_capsule_path
                .parent()
                .expect("orphan capsule should have parent directory"),
        )
        .await
        .expect("orphan capsule parent dir should be created");
        tokio::fs::write(&orphan_capsule_path, b"orphan old mv2")
            .await
            .expect("orphan capsule file should be created");
        let missing_old_capsule = crud_store
            .resolve_thread_episodic_active_write_segment(
                ThreadEpisodicActiveWriteSegmentRequest {
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_missing_file".to_owned(),
                    storage_uri_root: thread_episodic_storage_uri_from_path(temp_dir.path()),
                },
                1_700_000_000,
            )
            .await
            .expect("missing old per-thread capsule should resolve");
        assert_ne!(old_capsule.id, missing_old_capsule.id);
        let item = crud_store
            .upsert_thread_episodic_item(
                NewThreadEpisodicItemRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_capsule".to_owned(),
                    turn_id: "turn_cleanup".to_owned(),
                    item_id: "item_cleanup".to_owned(),
                    source_actor_role: ThreadEpisodicSourceActorRole::User,
                    source_runtime_kind: ThreadEpisodicSourceRuntimeKind::UserTurn,
                    source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
                    visibility: ThreadEpisodicItemVisibility::UserVisible,
                    status: ThreadEpisodicItemStatus::Active,
                    text_hash: "a".repeat(64),
                    source_text_hash: "b".repeat(64),
                    projection_group_id: "projection_group_cleanup".to_owned(),
                    language_hint: None,
                    token_estimate: 1,
                    capsule_id: Some(old_capsule.id.clone()),
                    capsule_ref: Some(old_capsule.capsule_ref.clone()),
                    segment_index: Some(old_capsule.segment_index),
                    frame_id: Some(7),
                    frame_uri: Some("mv2://old/thread/frame".to_owned()),
                    indexed_at: Some(now_datetime()),
                    deleted_at: None,
                },
                1_700_000_000,
            )
            .await
            .expect("old item should insert");
        let job = crud_store
            .insert_thread_episodic_index_job_if_absent(
                NewThreadEpisodicIndexJobRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_capsule".to_owned(),
                    index_item_id: item.id.clone(),
                    capsule_id: Some(old_capsule.id.clone()),
                    capsule_ref: Some(old_capsule.capsule_ref.clone()),
                    segment_index: Some(old_capsule.segment_index),
                    frame_uri: Some("mv2://old/thread/frame".to_owned()),
                    status: ThreadEpisodicIndexJobStatus::Completed,
                    graph_enrichment_state: ThreadEpisodicGraphEnrichmentState::NotSupported,
                    next_run_at: now_datetime(),
                    last_error: None,
                },
                1_700_000_000,
            )
            .await
            .expect("old job should insert");
        crud_store
            .upsert_thread_episodic_thread_directory_entry(
                NewThreadEpisodicThreadDirectoryRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: "old_thread_capsule".to_owned(),
                    title: None,
                    summary_hash: None,
                    summary_ref: None,
                    thread_created_at: None,
                    thread_updated_at: Some(now_datetime()),
                    last_indexed_at: Some(now_datetime()),
                    indexed_item_count: 99,
                    task_affinity_json: None,
                    project_affinity_json: None,
                    visibility: ThreadEpisodicThreadDirectoryVisibility::Visible,
                    status: ThreadEpisodicThreadDirectoryStatus::Active,
                },
                1_700_000_000,
            )
            .await
            .expect("old directory entry should insert");

        mark_projection_reset(
            &crud_store.database_connection(),
            &workspace_id,
            PROJECTION_META_STATUS_PENDING,
            &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
        )
        .await
        .unwrap();
        let summary = cleanup_derived_artifacts(
            crud_store.as_ref(),
            1_700_000_010,
            temp_dir.path(),
            workspace_id.as_str(),
            pioneer_memory::lock_thread_episodic_workspace(&workspace_id).await,
        )
        .await
        .expect("cleanup should succeed");

        assert_eq!(summary.capsule_rows_deleted, 2);
        assert_eq!(summary.capsule_files_deleted, 1);
        assert_eq!(summary.capsule_files_missing, 1);
        assert_eq!(summary.item_rows_deleted, 0);
        assert_eq!(summary.exclusion_rows_deleted, 0);
        assert_eq!(summary.index_jobs_replaced, 1);
        assert_eq!(summary.thread_directory_rows_deleted, 1);
        assert!(!old_capsule_path.exists());
        assert!(orphan_capsule_path.exists());
        assert!(
            crud_store
                .list_all_thread_episodic_capsules()
                .await
                .expect("capsules should list")
                .is_empty()
        );
        assert!(
            crud_store
                .find_thread_episodic_item(item.id.as_str())
                .await
                .expect("item should load")
                .is_some_and(|item| item.status == ThreadEpisodicItemStatus::PendingIndex
                    && item.frame_id.is_none())
        );
        assert!(
            crud_store
                .find_thread_episodic_index_job_by_item(&item.id)
                .await
                .expect("job lookup succeeds")
                .is_some_and(|replacement| replacement.id != job.id
                    && replacement.status == ThreadEpisodicIndexJobStatus::Queued
                    && replacement.attempt_count == 0)
        );
        assert!(
            crud_store
                .find_thread_episodic_thread_directory_entry(
                    workspace_id.as_str(),
                    "old_thread_capsule"
                )
                .await
                .expect("directory lookup succeeds")
                .is_none()
        );

        let second = cleanup_derived_artifacts(
            crud_store.as_ref(),
            1_700_000_011,
            temp_dir.path(),
            workspace_id.as_str(),
            pioneer_memory::lock_thread_episodic_workspace(&workspace_id).await,
        )
        .await
        .expect("cleanup should be rerunnable");
        assert_eq!(second.capsule_rows_deleted, 0);
        assert_eq!(second.item_rows_deleted, 0);
        assert_eq!(second.exclusion_rows_deleted, 0);
        assert_eq!(second.index_jobs_replaced, 0);
        assert_eq!(second.thread_directory_rows_deleted, 0);
    }

    #[tokio::test]
    async fn thread_episodic_vector_disable_refill_cleans_stale_projection_without_deleting_history()
     {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_to_lexical_cleanup";
        let turn_id = "turn_vector_to_lexical_cleanup";
        let item_id = "item_vector_to_lexical_cleanup";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            turn_id,
            item_id,
            "vector to lexical cleanup keeps canonical history",
        )
        .await;
        let orphan_vector_path = temp_dir
            .path()
            .join("thread_episodic")
            .join("orphan_vector_projection")
            .join("segment.mv2");
        tokio::fs::create_dir_all(
            orphan_vector_path
                .parent()
                .expect("orphan vector file should have parent directory"),
        )
        .await
        .expect("orphan vector parent should be created");
        tokio::fs::write(&orphan_vector_path, b"stale vectorized mv2")
            .await
            .expect("orphan vector file should be created");
        let vector_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &GatewayThreadEpisodicVectorSearchConfig {
                    enabled: true,
                    provider: Some(GatewayThreadEpisodicVectorProviderConfig::OpenAi),
                    model: Some("text-embedding-3-small".to_owned()),
                    local_model: Some("bge-small-en-v1.5".to_owned()),
                    embedding_normalized: true,
                    use_search_instructions: false,
                },
            );
        mark_refill_marker_with_workspace_target(
            crud_store.as_ref(),
            workspace_id.as_str(),
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &vector_target,
        )
        .await;

        let disabled_config = GatewayThreadEpisodicVectorSearchConfig {
            enabled: false,
            ..GatewayThreadEpisodicVectorSearchConfig::default()
        };
        let stale_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        let summary = refill_once_for_vector_search_config(
            crud_store.clone(),
            temp_dir.path(),
            &disabled_config,
            Some(stale_provider.clone()),
        )
        .await
        .expect("disabled vector search should rebuild lexical projection");

        assert!(!summary.skipped);
        assert_eq!(summary.capsule_files_deleted, 0);
        assert_eq!(
            stale_provider.calls(),
            0,
            "disabled vector search must not call the stale embedding provider"
        );
        assert!(orphan_vector_path.exists());
        let items = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("rebuilt lexical items should list");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, ThreadEpisodicItemStatus::Active);
        assert!(items[0].frame_id.is_some());
        assert!(
            crud_store
                .get_turn_item(turn_id, item_id)
                .await
                .expect("canonical turn item lookup should succeed")
                .is_some(),
            "cleanup must not delete canonical turn history"
        );
        assert!(
            refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only()
            )
            .await
            .expect("lexical marker should be current after refill")
        );
        assert!(
            !refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &vector_target
            )
            .await
            .expect("old vector marker should no longer be current")
        );
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_indexes_items_with_embedding_identity() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill",
            "item_vector_refill",
            "vector refill should embed this workspace memory item",
        )
        .await;
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            embedding_provider.as_ref(),
        )
        .expect("test provider target");

        let summary = refill_once_with_projection(
            crud_store.clone(),
            temp_dir.path(),
            target.clone(),
            Some(embedding_provider.clone()),
        )
        .await
        .expect("vector refill should complete");

        assert!(!summary.skipped);
        assert_eq!(summary.refill_jobs_enqueued, 1);
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(embedding_provider.calls(), 1);
        assert!(
            refill_is_current_for_target(crud_store.as_ref(), &target)
                .await
                .expect("vector marker should be current")
        );

        let items = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("items should list");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].status, ThreadEpisodicItemStatus::Active);
        assert!(items[0].frame_id.is_some());
        assert!(
            items[0]
                .frame_uri
                .as_deref()
                .expect("frame uri should be persisted")
                .starts_with("mv2://workspace/")
        );
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_terminal_embedding_failure_does_not_complete_marker() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill_terminal";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_terminal",
            "item_vector_refill_terminal",
            "vector refill dimension mismatch should fail the projection",
        )
        .await;
        let embedding_provider = Arc::new(
            StaticThreadEpisodicEmbeddingProvider::with_declared_dimension(
                "openai",
                "text-embedding-3-small",
                3,
                vec![0.1, 0.2, 0.3, 0.4],
            ),
        );
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            embedding_provider.as_ref(),
        )
        .expect("test provider target");

        let error = refill_once_with_projection(
            crud_store.clone(),
            temp_dir.path(),
            target.clone(),
            Some(embedding_provider.clone()),
        )
        .await
        .expect_err("terminal embedding failure should fail refill");

        assert!(
            error
                .to_string()
                .contains("thread episodic workspace refill has terminally failed index jobs"),
            "unexpected error: {error:#}"
        );
        assert_eq!(embedding_provider.calls(), 1);
        assert!(
            !refill_is_current_for_target(crud_store.as_ref(), &target)
                .await
                .expect("failed vector marker should not be current")
        );
        let meta = find_projection_meta(
            &crud_store.database_connection(),
            refill_projection_key_for_workspace(workspace_id.as_str())
                .expect("workspace refill key should build")
                .as_str(),
        )
        .await
        .expect("meta query should succeed")
        .expect("meta exists");
        assert_eq!(meta.status, PROJECTION_META_STATUS_FAILED);

        let jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("jobs should list");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].status, ThreadEpisodicIndexJobStatus::Canceled);
        assert!(
            jobs[0]
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("embedding dimension mismatch"))
        );
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_retries_transient_embedding_failure_until_success() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill_retryable";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_retryable",
            "item_vector_refill_retryable",
            "vector refill provider failure should retry automatically",
        )
        .await;
        let embedding_provider = Arc::new(ScriptedThreadEpisodicEmbeddingProvider::new(vec![
            Err(ThreadEpisodicEmbeddingError::retryable_provider_failure(
                "openai",
                "text-embedding-3-small",
                "rate limited",
            )),
            Ok(vec![0.1, 0.2, 0.3]),
        ]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            embedding_provider.as_ref(),
        )
        .expect("test provider target");

        let summary = refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            embedding_provider.clone(),
            immediate_retry_config(5),
        )
        .await
        .expect("transient embedding failure should recover in the same refill");

        assert_eq!(embedding_provider.calls(), 2);
        assert_eq!(summary.failed_retryable_jobs, 1);
        assert_eq!(summary.completed_jobs, 1);
        assert!(
            refill_is_current_for_target(crud_store.as_ref(), &target)
                .await
                .expect("recovered vector marker should be current")
        );

        let jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("jobs should list");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].status, ThreadEpisodicIndexJobStatus::Completed);
        assert_eq!(jobs[0].attempt_count, 2);
        assert!(jobs[0].last_error.is_none());
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_stops_after_bounded_transient_retries() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill_retry_exhausted";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_retry_exhausted",
            "item_vector_refill_retry_exhausted",
            "vector refill should stop after its bounded retry budget",
        )
        .await;
        let embedding_provider =
            Arc::new(StaticThreadEpisodicEmbeddingProvider::retryable_failure());
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            embedding_provider.as_ref(),
        )
        .expect("test provider target");

        let error = refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            embedding_provider.clone(),
            immediate_retry_config(2),
        )
        .await
        .expect_err("exhausted transient failures should fail refill");

        assert!(
            error
                .to_string()
                .contains("thread episodic workspace refill has terminally failed index jobs"),
            "unexpected error: {error:#}"
        );
        assert_eq!(embedding_provider.calls(), 2);
        assert!(
            !refill_is_current_for_target(crud_store.as_ref(), &target)
                .await
                .expect("failed vector marker should not be current")
        );
        let jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("jobs should list");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].status, ThreadEpisodicIndexJobStatus::Canceled);
        assert_eq!(jobs[0].attempt_count, 2);
        assert!(
            jobs[0]
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("rate limited"))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn thread_episodic_vector_refill_reconciles_source_changed_during_provider_failure() {
        let (crud_store, temp_dir, workspace_id) = setup_concurrent_store().await;
        let thread_id = "thread_vector_provider_source_change";
        let turn_id = "turn_vector_provider_source_change";
        let item_id = "item_vector_provider_source_change";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            turn_id,
            item_id,
            "provider version A",
        )
        .await;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let embedding_provider = Arc::new(BlockingFirstFailureEmbeddingProvider {
            started: started_tx,
            release: Mutex::new(release_rx),
            calls: AtomicUsize::new(0),
        });
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            embedding_provider.as_ref(),
        )
        .expect("test provider target");
        let refill_store = crud_store.clone();
        let refill_workspace_id = workspace_id.clone();
        let refill_root = temp_dir.path().to_path_buf();
        let refill_target = target.clone();
        let refill_provider = embedding_provider.clone();
        let refill = tokio::spawn(async move {
            refill_once_with_test_executor_config(
                refill_store,
                refill_root.as_path(),
                refill_workspace_id.as_str(),
                refill_target,
                refill_provider,
                immediate_retry_config(5),
            )
            .await
        });

        tokio::task::spawn_blocking(move || started_rx.recv())
            .await
            .expect("provider-start waiter should join")
            .expect("provider should start");
        crud_store
            .materialize_item_snapshot_updated(
                ItemUpdatedNotification {
                    workspace_id: workspace_id.clone(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::UserMessage {
                        id: item_id.to_owned(),
                        text: "provider version B".to_owned(),
                        attachments: Vec::new(),
                    },
                },
                1_700_020_100,
            )
            .await
            .expect("canonical source should change during embedding");
        release_tx
            .send(())
            .expect("provider failure should be released");

        let summary = refill
            .await
            .expect("refill task should join")
            .expect("refill should reconcile and finish");
        assert_eq!(embedding_provider.calls(), 2);
        assert_eq!(summary.failed_terminal_jobs, 0);
        assert_eq!(summary.completed_jobs, 1);
        let items = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("source versions should list");
        assert_eq!(
            items
                .iter()
                .filter(|item| item.status == ThreadEpisodicItemStatus::Active)
                .count(),
            1
        );
        let active = items
            .iter()
            .find(|item| item.status == ThreadEpisodicItemStatus::Active)
            .expect("current source should be active");
        assert_eq!(
            active.source_text_hash,
            crate::thread_episodic::source_text_hash("provider version B")
        );
        assert!(active.embedding_artifact_id.is_some());
        assert!(
            refill_is_current_for_target(crud_store.as_ref(), &target)
                .await
                .expect("reconciled vector marker should be current")
        );
    }

    #[tokio::test]
    async fn thread_episodic_refill_success_commit_rechecks_source_after_resolution() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_refill_atomic_source_check";
        let turn_id = "turn_refill_atomic_source_check";
        let item_id = "item_refill_atomic_source_check";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            turn_id,
            item_id,
            "refill resolved version A",
        )
        .await;
        let job = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id.as_str(),
                chrono::Utc::now().timestamp().saturating_add(10),
                1,
                5,
            )
            .await
            .expect("refill job should claim")
            .assert_no_failures_for_test()
            .pop()
            .expect("refill job should exist");
        let payload_provider = StoreThreadEpisodicIndexPayloadProvider::new(
            crud_store.clone(),
            thread_episodic_storage_uri_from_path(temp_dir.path()),
        );
        let resolved = payload_provider
            .resolve_index_request(&job)
            .await
            .expect("version A should resolve");
        crud_store
            .materialize_item_snapshot_updated(
                ItemUpdatedNotification {
                    workspace_id: workspace_id.clone(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::UserMessage {
                        id: item_id.to_owned(),
                        text: "refill current version B".to_owned(),
                        attachments: Vec::new(),
                    },
                },
                1_700_020_201,
            )
            .await
            .expect("canonical source should change after resolution");
        let outcome = persist_successful_refill_item(
            crud_store.as_ref(),
            &job,
            resolved.clone(),
            ThreadEpisodicMemvidIndexOutput {
                frame_id: 501,
                frame_uri: resolved.request.frame_uri.clone(),
                embedding_identity: None,
                stats: ThreadEpisodicMemvidStats::default(),
            },
            1_700_020_202,
            Instant::now(),
        )
        .await
        .expect("stale refill success should be rejected cleanly");
        assert_eq!(outcome, ThreadEpisodicIndexAttemptOutcome::StaleAttempt);
        let stale = crud_store
            .find_thread_episodic_item(job.index_item_id.as_str())
            .await
            .expect("stale item lookup should succeed")
            .expect("stale item should remain");
        assert_ne!(stale.status, ThreadEpisodicItemStatus::Active);
        assert!(stale.frame_id.is_none());

        reconcile_and_release_refill_claim(
            crud_store.clone(),
            &job,
            1_700_020_203,
            immediate_retry_config(5),
        )
        .await
        .expect("current source should reconcile");
        let versions = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("source versions should list");
        assert!(versions.iter().any(|item| {
            item.status == ThreadEpisodicItemStatus::PendingIndex
                && item.source_text_hash
                    == crate::thread_episodic::source_text_hash("refill current version B")
        }));
    }

    #[tokio::test]
    async fn thread_episodic_refill_reconciliation_persistence_recovery_is_bounded() {
        let config = ThreadEpisodicIndexExecutorConfig {
            retry_base_delay_secs: 11,
            retry_max_delay_secs: 60,
            max_attempts: 3,
            ..ThreadEpisodicIndexExecutorConfig::default()
        };

        let (retry_store, _retry_temp, retry_workspace) = setup_store().await;
        ingest_materialized_user_item(
            retry_store.clone(),
            retry_workspace.as_str(),
            "thread_refill_reconcile_retry",
            "turn_refill_reconcile_retry",
            "item_refill_reconcile_retry",
            "retry reconciliation persistence",
        )
        .await;
        let retry_now = chrono::Utc::now().timestamp().saturating_add(60);
        let retry_job = retry_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                retry_workspace.as_str(),
                retry_now,
                1,
                5,
            )
            .await
            .expect("retry job should claim")
            .assert_no_failures_for_test()
            .pop()
            .expect("retry job should exist");
        let retry_outcome = persist_refill_reconciliation_error(
            retry_store.clone(),
            &retry_job,
            retry_now,
            config,
            anyhow::anyhow!("controlled reconciliation failure"),
            Some("controlled primary persistence failure"),
            None,
        )
        .await
        .expect("fallback should durably record the retryable reconciliation failure");
        assert_eq!(retry_outcome, ThreadEpisodicIndexAttemptOutcome::Applied);
        let stored_retry = retry_store
            .find_thread_episodic_index_job(retry_job.id.as_str())
            .await
            .expect("retry job lookup should succeed")
            .expect("retry job should remain");
        assert_eq!(stored_retry.status, ThreadEpisodicIndexJobStatus::Failed);
        assert_eq!(stored_retry.attempt_count, retry_job.attempt_count);
        assert_eq!(
            stored_retry.next_run_at,
            fixed_datetime_from_unix(next_refill_retry_at(&retry_job, retry_now, config))
        );
        assert!(
            stored_retry
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("controlled reconciliation failure")
                    && error.contains("controlled primary persistence failure"))
        );

        let (terminal_store, _terminal_temp, terminal_workspace) = setup_store().await;
        ingest_materialized_user_item(
            terminal_store.clone(),
            terminal_workspace.as_str(),
            "thread_refill_reconcile_terminal",
            "turn_refill_reconcile_terminal",
            "item_refill_reconcile_terminal",
            "terminal reconciliation persistence",
        )
        .await;
        let terminal_now = chrono::Utc::now().timestamp().saturating_add(60);
        let terminal_job = terminal_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                terminal_workspace.as_str(),
                terminal_now,
                1,
                5,
            )
            .await
            .expect("terminal job should claim")
            .assert_no_failures_for_test()
            .pop()
            .expect("terminal job should exist");
        let terminal_config = ThreadEpisodicIndexExecutorConfig {
            max_attempts: terminal_job.attempt_count,
            ..config
        };
        let terminal_outcome = persist_refill_reconciliation_error(
            terminal_store.clone(),
            &terminal_job,
            terminal_now,
            terminal_config,
            anyhow::anyhow!("terminal reconciliation failure"),
            Some("terminal primary persistence failure"),
            None,
        )
        .await
        .expect("fallback should durably record the terminal reconciliation failure");
        assert_eq!(terminal_outcome, ThreadEpisodicIndexAttemptOutcome::Applied);
        let stored_terminal = terminal_store
            .find_thread_episodic_index_job(terminal_job.id.as_str())
            .await
            .expect("terminal job lookup should succeed")
            .expect("terminal job should remain");
        assert_eq!(
            stored_terminal.status,
            ThreadEpisodicIndexJobStatus::Canceled
        );
        assert!(stored_terminal.next_run_at <= fixed_datetime_from_unix(terminal_now));

        let (failed_store, _failed_temp, failed_workspace) = setup_store().await;
        ingest_materialized_user_item(
            failed_store.clone(),
            failed_workspace.as_str(),
            "thread_refill_reconcile_unpersisted",
            "turn_refill_reconcile_unpersisted",
            "item_refill_reconcile_unpersisted",
            "unpersisted reconciliation failure",
        )
        .await;
        let failed_now = chrono::Utc::now().timestamp().saturating_add(60);
        let failed_job = failed_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                failed_workspace.as_str(),
                failed_now,
                1,
                5,
            )
            .await
            .expect("unpersisted job should claim")
            .assert_no_failures_for_test()
            .pop()
            .expect("unpersisted job should exist");
        let error = persist_refill_reconciliation_error(
            failed_store.clone(),
            &failed_job,
            failed_now,
            config,
            anyhow::anyhow!("unpersisted reconciliation failure"),
            Some("unavailable primary persistence"),
            Some("unavailable fallback persistence"),
        )
        .await
        .expect_err("failure of both persistence paths must surface");
        let rendered = format!("{error:#}");
        assert!(rendered.contains("unpersisted reconciliation failure"));
        assert!(rendered.contains("unavailable primary persistence"));
        assert!(rendered.contains("unavailable fallback persistence"));
        let still_running = failed_store
            .find_thread_episodic_index_job(failed_job.id.as_str())
            .await
            .expect("unpersisted job lookup should succeed")
            .expect("unpersisted job should remain");
        assert_eq!(still_running.status, ThreadEpisodicIndexJobStatus::Running);

        failed_store
            .requeue_thread_episodic_index_attempt(
                failed_job.id.as_str(),
                failed_job.attempt_count,
                failed_now.saturating_add(1),
                None,
            )
            .await
            .expect("old claim should requeue for stale-attempt coverage");
        let new_claim = failed_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                failed_workspace.as_str(),
                failed_now.saturating_add(2),
                1,
                5,
            )
            .await
            .expect("new attempt should claim")
            .assert_no_failures_for_test()
            .pop()
            .expect("new attempt should exist");
        assert!(new_claim.attempt_count > failed_job.attempt_count);
        let stale_outcome = persist_refill_reconciliation_error(
            failed_store.clone(),
            &failed_job,
            failed_now.saturating_add(3),
            config,
            anyhow::anyhow!("late old reconciliation failure"),
            Some("late primary persistence failure"),
            None,
        )
        .await
        .expect("old attempt should be rejected without changing the new claim");
        assert_eq!(
            stale_outcome,
            ThreadEpisodicIndexAttemptOutcome::StaleAttempt
        );
        let preserved_claim = failed_store
            .find_thread_episodic_index_job(failed_job.id.as_str())
            .await
            .expect("new claim lookup should succeed")
            .expect("new claim should remain");
        assert_eq!(
            preserved_claim.status,
            ThreadEpisodicIndexJobStatus::Running
        );
        assert_eq!(preserved_claim.attempt_count, new_claim.attempt_count);
    }

    struct PanickingRefillPreparation {
        inner: StoreThreadEpisodicIndexPayloadProvider,
        poison_id: String,
    }
    #[async_trait::async_trait]
    impl ThreadEpisodicIndexPayloadProvider for PanickingRefillPreparation {
        async fn resolve_index_request(
            &self,
            job: &ThreadEpisodicIndexJobRecord,
        ) -> std::result::Result<
            crate::thread_episodic::ThreadEpisodicResolvedIndexRequest,
            crate::thread_episodic::ThreadEpisodicIndexResolutionError,
        > {
            if job.id == self.poison_id {
                panic!("controlled refill candidate unwind");
            }
            self.inner.resolve_index_request(job).await
        }
    }

    #[tokio::test]
    async fn refill_candidate_unwind_keeps_a_c_success_and_b_durable_running() {
        let (store, root, workspace) = setup_store().await;
        for suffix in ["a", "b", "c"] {
            ingest_materialized_user_item(
                store.clone(),
                &workspace,
                &format!("unwind_refill_{suffix}"),
                &format!("unwind_refill_turn_{suffix}"),
                &format!("unwind_refill_item_{suffix}"),
                &format!("refill source {suffix}"),
            )
            .await;
        }
        let ownership = lock_thread_episodic_workspace(&workspace).await;
        let now = chrono::Utc::now().timestamp() + 60;
        let jobs = store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace, now, 3, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test();
        assert_eq!(jobs.len(), 3);
        let poison = jobs
            .iter()
            .find(|job| job.thread_id == "unwind_refill_b")
            .unwrap()
            .clone();
        let backend =
            MemvidThreadEpisodicBackend::new().with_workspace_ownership(ownership.clone());
        let provider = PanickingRefillPreparation {
            inner: StoreThreadEpisodicIndexPayloadProvider::new(
                store.clone(),
                thread_episodic_storage_uri_from_path(root.path()),
            ),
            poison_id: poison.id.clone(),
        };
        let mut summary = ThreadEpisodicWorkspaceCapsuleRefillSummary::default();
        assert!(
            execute_claimed_refill_batch(
                store.clone(),
                &backend,
                &provider,
                jobs.clone(),
                now,
                ThreadEpisodicIndexExecutorConfig::default(),
                &mut summary,
                None,
            )
            .await
            .is_err()
        );
        assert_eq!(summary.completed_jobs, 2);
        for job in &jobs {
            let current = store
                .find_thread_episodic_index_job(&job.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.attempt_count, 1);
            assert_eq!(
                current.status,
                if job.id == poison.id {
                    ThreadEpisodicIndexJobStatus::Running
                } else {
                    ThreadEpisodicIndexJobStatus::Completed
                }
            );
        }
        assert!(
            store
                .find_thread_episodic_item(&poison.index_item_id)
                .await
                .unwrap()
                .unwrap()
                .frame_id
                .is_none()
        );
        store
            .database_connection()
            .begin()
            .await
            .unwrap()
            .rollback()
            .await
            .unwrap();
        drop(backend);
        drop(ownership);
        let ownership = lock_thread_episodic_workspace(&workspace).await;
        assert_eq!(
            store
                .requeue_running_thread_episodic_index_jobs_for_workspace(&workspace, now, 5)
                .await
                .unwrap(),
            1
        );
        let retry = store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace, now, 3, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].id, poison.id);
        assert_eq!(retry[0].attempt_count, 2);
        let provider = StoreThreadEpisodicIndexPayloadProvider::new(
            store.clone(),
            thread_episodic_storage_uri_from_path(root.path()),
        );
        let backend = MemvidThreadEpisodicBackend::new().with_workspace_ownership(ownership);
        execute_claimed_refill_batch(
            store.clone(),
            &backend,
            &provider,
            retry,
            now,
            ThreadEpisodicIndexExecutorConfig::default(),
            &mut summary,
            None,
        )
        .await
        .unwrap();
        assert_eq!(summary.completed_jobs, 3);
    }

    #[tokio::test]
    async fn thread_episodic_refill_finishes_claimed_batch_before_returning_error() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        for suffix in ["a", "b"] {
            let thread_id = format!("thread_refill_claimed_batch_{suffix}");
            let turn_id = format!("turn_refill_claimed_batch_{suffix}");
            let item_id = format!("item_refill_claimed_batch_{suffix}");
            let text = format!("claimed batch source {suffix}");
            ingest_materialized_user_item(
                crud_store.clone(),
                workspace_id.as_str(),
                thread_id.as_str(),
                turn_id.as_str(),
                item_id.as_str(),
                text.as_str(),
            )
            .await;
        }
        let now_unix = chrono::Utc::now().timestamp().saturating_add(60);
        let jobs = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id.as_str(),
                now_unix,
                2,
                5,
            )
            .await
            .expect("two refill jobs should claim")
            .assert_no_failures_for_test();
        assert_eq!(jobs.len(), 2);
        let failed_claim = jobs[0].clone();
        let completed_claim = jobs[1].clone();
        let backend = MemvidThreadEpisodicBackend::new();
        let payload_provider = StoreThreadEpisodicIndexPayloadProvider::new(
            crud_store.clone(),
            thread_episodic_storage_uri_from_path(temp_dir.path()),
        );
        let mut summary = ThreadEpisodicWorkspaceCapsuleRefillSummary::default();
        let error = execute_claimed_refill_batch(
            crud_store.clone(),
            &backend,
            &payload_provider,
            jobs,
            now_unix,
            ThreadEpisodicIndexExecutorConfig::default(),
            &mut summary,
            Some(failed_claim.id.as_str()),
        )
        .await
        .expect_err("the injected unpersisted transition must fail the refill batch");
        let rendered = format!("{error:#}");
        assert!(rendered.contains(failed_claim.id.as_str()));
        assert!(rendered.contains("injected refill source reconciliation failure"));
        assert!(rendered.contains("injected primary reconciliation persistence failure"));
        assert!(rendered.contains("injected fallback reconciliation persistence failure"));
        assert_eq!(summary.completed_jobs, 1);

        let first = crud_store
            .find_thread_episodic_index_job(failed_claim.id.as_str())
            .await
            .expect("failed claim lookup should succeed")
            .expect("failed claim should remain");
        assert_eq!(first.status, ThreadEpisodicIndexJobStatus::Running);
        assert_eq!(first.attempt_count, failed_claim.attempt_count);
        let second = crud_store
            .find_thread_episodic_index_job(completed_claim.id.as_str())
            .await
            .expect("completed claim lookup should succeed")
            .expect("completed claim should remain");
        assert_eq!(second.status, ThreadEpisodicIndexJobStatus::Completed);
        let second_item = crud_store
            .find_thread_episodic_item(completed_claim.index_item_id.as_str())
            .await
            .expect("completed item lookup should succeed")
            .expect("completed item should remain");
        assert_eq!(second_item.status, ThreadEpisodicItemStatus::Active);

        assert_eq!(
            crud_store
                .requeue_thread_episodic_index_attempt(
                    failed_claim.id.as_str(),
                    failed_claim.attempt_count,
                    now_unix.saturating_add(1),
                    None
                )
                .await
                .expect("owned failed claim should requeue"),
            ThreadEpisodicIndexAttemptOutcome::Applied
        );
        let new_claim = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id.as_str(),
                now_unix.saturating_add(2),
                1,
                5,
            )
            .await
            .expect("requeued job should claim again")
            .assert_no_failures_for_test()
            .pop()
            .expect("new claim should exist");
        assert!(new_claim.attempt_count > failed_claim.attempt_count);
        assert_eq!(
            crud_store
                .cancel_thread_episodic_index_attempt(
                    failed_claim.id.as_str(),
                    failed_claim.attempt_count,
                    "late cleanup from obsolete refill batch",
                    now_unix.saturating_add(3),
                )
                .await
                .expect("late cleanup should be rejected cleanly"),
            ThreadEpisodicIndexAttemptOutcome::StaleAttempt
        );
        let preserved = crud_store
            .find_thread_episodic_index_job(new_claim.id.as_str())
            .await
            .expect("new claim lookup should succeed")
            .expect("new claim should remain");
        assert_eq!(preserved.status, ThreadEpisodicIndexJobStatus::Running);
        assert_eq!(preserved.attempt_count, new_claim.attempt_count);
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_does_not_reopen_exhausted_chunked_failure() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            "thread_vector_refill_chunked_terminal",
            "turn_vector_refill_chunked_terminal",
            "item_vector_refill_chunked_terminal",
            "bounded input still receives a terminal retry budget",
        )
        .await;
        let failed_provider = Arc::new(ScriptedThreadEpisodicEmbeddingProvider::new(vec![Err(
            ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                "openrouter",
                "qwen/qwen3-embedding-8b",
                format!(
                    "{CHUNKED_EMBEDDING_INPUT_ERROR_MARKER}: error decoding response body: missing field `data`"
                ),
            ),
        )]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            failed_provider.as_ref(),
        )
        .expect("test provider target");

        refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            failed_provider,
            immediate_retry_config(1),
        )
        .await
        .expect_err("chunked provider failure should exhaust its retry budget");

        let next_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        refill_once_with_test_executor_config(
            crud_store,
            temp_dir.path(),
            workspace_id.as_str(),
            target,
            next_provider.clone(),
            immediate_retry_config(1),
        )
        .await
        .expect_err("restart must not reset a chunked failure's bounded retry budget");

        assert_eq!(next_provider.calls(), 0);
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_restart_finalizes_completed_projection_without_rebuild()
    {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill_completed_before_marker";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_completed_before_marker",
            "item_vector_refill_completed_before_marker",
            "completed vector data must survive a final marker failure",
        )
        .await;
        let initial_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            initial_provider.as_ref(),
        )
        .expect("test provider target");

        refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            initial_provider.clone(),
            immediate_retry_config(5),
        )
        .await
        .expect("initial refill should complete");
        assert_eq!(initial_provider.calls(), 1);
        let before_item = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("item should list before marker failure")
            .into_iter()
            .next()
            .expect("indexed item should exist");
        let before_frame_uri = before_item
            .frame_uri
            .clone()
            .expect("indexed item should have a frame URI");

        let projection_key = refill_projection_key_for_workspace(workspace_id.as_str())
            .expect("workspace refill key should build");
        assert!(
            update_projection_meta_status(
                &crud_store.database_connection(),
                projection_key.as_str(),
                PROJECTION_META_STATUS_FAILED,
                Some("simulated final marker failure"),
                now_datetime(),
            )
            .await
            .expect("marker status should update")
        );

        let resume_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        let summary = refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            resume_provider.clone(),
            immediate_retry_config(5),
        )
        .await
        .expect("completed projection should finalize without rebuilding");

        assert!(summary.resumed);
        assert_eq!(summary.completed_jobs, 0);
        assert_eq!(summary.source_turn_item_count, 1);
        assert_eq!(summary.capsule_files_deleted, 0);
        assert_eq!(summary.capsule_rows_deleted, 0);
        assert_eq!(summary.item_rows_deleted, 0);
        assert_eq!(summary.index_jobs_replaced, 0);
        assert_eq!(resume_provider.calls(), 0);
        let after_item = crud_store
            .find_thread_episodic_item(before_item.id.as_str())
            .await
            .expect("retained item lookup should succeed")
            .expect("retained item should still exist");
        assert_eq!(
            after_item.frame_uri.as_deref(),
            Some(before_frame_uri.as_str())
        );
        assert!(
            refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &target,
            )
            .await
            .expect("finalized marker should be current")
        );
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_restart_requeues_interrupted_running_job() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill_interrupted";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_interrupted",
            "item_vector_refill_interrupted",
            "an interrupted refill job should resume after gateway restart",
        )
        .await;
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            embedding_provider.as_ref(),
        )
        .expect("test provider target");
        mark_refill_marker_with_workspace_target(
            crud_store.as_ref(),
            workspace_id.as_str(),
            PROJECTION_META_STATUS_BACKFILLING,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &target,
        )
        .await;
        let claimed = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id.as_str(),
                chrono::Utc::now().timestamp().saturating_add(1),
                1,
                5,
            )
            .await
            .expect("job should be claimed before simulated restart")
            .assert_no_failures_for_test();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].status, ThreadEpisodicIndexJobStatus::Running);

        assert_eq!(
            super::super::recover_inherited_thread_episodic_jobs(&crud_store, 5)
                .await
                .unwrap(),
            1
        );
        let summary = refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            embedding_provider.clone(),
            immediate_retry_config(5),
        )
        .await
        .expect("interrupted running job should resume");

        assert!(summary.resumed);
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(summary.item_rows_deleted, 0);
        assert_eq!(embedding_provider.calls(), 1);
        let jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("jobs should list after interrupted refill resumes");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].status, ThreadEpisodicIndexJobStatus::Completed);
        assert_eq!(jobs[0].attempt_count, 2);
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_does_not_requeue_current_process_running_job() {
        let (crud_store, _temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill_current_process";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_current_process",
            "item_vector_refill_current_process",
            "a current process job must keep its execution lease",
        )
        .await;
        let claimed_at = chrono::Utc::now().timestamp();
        let claimed = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id.as_str(),
                claimed_at,
                1,
                5,
            )
            .await
            .expect("current process job should be claimed")
            .assert_no_failures_for_test();
        assert_eq!(claimed.len(), 1);

        assert!(prepare_resumed_refill(None).resumed);

        let jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("jobs should list after resume preparation");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].status, ThreadEpisodicIndexJobStatus::Running);
        assert_eq!(jobs[0].attempt_count, 1);
    }

    #[tokio::test]
    async fn thread_episodic_vector_model_change_rebuilds_stale_projection() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_model_change";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_model_change",
            "item_vector_model_change",
            "vector model change should rebuild derived workspace capsules",
        )
        .await;
        let old_config = vector_search_config(
            GatewayThreadEpisodicVectorProviderConfig::OpenAi,
            "text-embedding-3-large",
            3072,
        );
        let old_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &old_config,
            );
        mark_refill_marker_with_workspace_target(
            crud_store.as_ref(),
            workspace_id.as_str(),
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &old_target,
        )
        .await;
        let stale_segment_path = temp_dir
            .path()
            .join("thread_episodic")
            .join("stale_model")
            .join("segment.mv2");
        tokio::fs::create_dir_all(
            stale_segment_path
                .parent()
                .expect("stale segment should have parent directory"),
        )
        .await
        .expect("stale segment parent should be created");
        tokio::fs::write(&stale_segment_path, b"old vector segment")
            .await
            .expect("stale segment should be written");

        let new_config = vector_search_config(
            GatewayThreadEpisodicVectorProviderConfig::OpenAi,
            "text-embedding-3-small",
            3,
        );
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        let new_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                embedding_provider.as_ref(),
            )
            .expect("test provider target");

        let summary = refill_once_for_vector_search_config(
            crud_store.clone(),
            temp_dir.path(),
            &new_config,
            Some(embedding_provider.clone()),
        )
        .await
        .expect("changed model should rebuild vector projection");

        assert!(!summary.skipped);
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(embedding_provider.calls(), 1);
        assert_eq!(summary.capsule_files_deleted, 0);
        assert!(stale_segment_path.exists());
        assert!(
            refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &new_target
            )
            .await
            .expect("new target current check")
        );
        assert!(
            !refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &old_target
            )
            .await
            .expect("old target current check")
        );
    }

    #[tokio::test]
    async fn thread_episodic_vector_model_same_config_does_not_rebuild() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let config = vector_search_config(
            GatewayThreadEpisodicVectorProviderConfig::OpenAi,
            "text-embedding-3-small",
            1536,
        );
        let embedding_provider =
            Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![0.1; 1536]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            embedding_provider.as_ref(),
        )
        .expect("test provider target");
        mark_refill_marker_with_target(
            crud_store.as_ref(),
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &target,
        )
        .await;
        let marker_before =
            find_refill_projection_meta_for_workspace(crud_store.as_ref(), workspace_id.as_str())
                .await
                .expect("refill marker snapshot should succeed");
        let capsules_before = crud_store
            .list_all_thread_episodic_capsules_for_workspace(workspace_id.as_str())
            .await
            .expect("capsule snapshot should succeed");
        let incomplete_jobs_before = crud_store
            .count_incomplete_thread_episodic_index_jobs_for_workspace(workspace_id.as_str())
            .await
            .expect("incomplete job snapshot should succeed");
        let canceled_jobs_before = crud_store
            .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id.as_str())
            .await
            .expect("canceled job snapshot should succeed");
        let resolver = Arc::new(FixedThreadEpisodicIndexEmbeddingProviderResolver::new(
            Some(embedding_provider.clone()),
        )) as Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>;
        let (status_sender, mut status_receiver) = tokio::sync::broadcast::channel(4);

        let summary = refill_once_with_projection_resolver(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &config,
            ),
            Some(resolver),
            Some(&status_sender),
        )
        .await
        .expect("same vector config should skip");

        assert!(summary.skipped);
        assert_eq!(embedding_provider.calls(), 0);
        assert!(matches!(
            status_receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        assert_eq!(
            find_refill_projection_meta_for_workspace(crud_store.as_ref(), workspace_id.as_str(),)
                .await
                .expect("refill marker verification should succeed"),
            marker_before
        );
        assert_eq!(
            crud_store
                .list_all_thread_episodic_capsules_for_workspace(workspace_id.as_str())
                .await
                .expect("capsule verification should succeed"),
            capsules_before
        );
        assert_eq!(
            crud_store
                .count_incomplete_thread_episodic_index_jobs_for_workspace(workspace_id.as_str())
                .await
                .expect("incomplete job verification should succeed"),
            incomplete_jobs_before
        );
        assert_eq!(
            crud_store
                .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id.as_str())
                .await
                .expect("canceled job verification should succeed"),
            canceled_jobs_before
        );
    }

    #[tokio::test]
    async fn thread_episodic_vector_model_change_openai_to_openrouter_rebuilds_projection() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            "thread_vector_provider_change",
            "turn_vector_provider_change",
            "item_vector_provider_change",
            "provider change should rebuild vector projection",
        )
        .await;
        let openai_config = vector_search_config(
            GatewayThreadEpisodicVectorProviderConfig::OpenAi,
            "text-embedding-3-small",
            3,
        );
        let openai_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &openai_config,
            );
        mark_refill_marker_with_workspace_target(
            crud_store.as_ref(),
            workspace_id.as_str(),
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &openai_target,
        )
        .await;
        let openrouter_config = vector_search_config(
            GatewayThreadEpisodicVectorProviderConfig::OpenRouter,
            "vendor/custom-embed",
            3,
        );
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "vendor/custom-embed",
            vec![0.1, 0.2, 0.3],
        ));
        let openrouter_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                embedding_provider.as_ref(),
            )
            .expect("openrouter test provider target");

        let summary = refill_once_for_vector_search_config(
            crud_store.clone(),
            temp_dir.path(),
            &openrouter_config,
            Some(embedding_provider),
        )
        .await
        .expect("provider change should rebuild vector projection");

        assert!(!summary.skipped);
        assert_eq!(summary.completed_jobs, 1);
        assert!(
            refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &openrouter_target
            )
            .await
            .expect("openrouter target current check")
        );
        assert!(
            !refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &openai_target
            )
            .await
            .expect("openai target current check")
        );
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_rejects_provider_identity_mismatch() {
        let (crud_store, temp_dir, _workspace_id) = setup_store().await;
        let config = vector_search_config(
            GatewayThreadEpisodicVectorProviderConfig::OpenRouter,
            "openai/text-embedding-3-small",
            3,
        );
        let wrong_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));

        let error = refill_once_for_vector_search_config(
            crud_store.clone(),
            temp_dir.path(),
            &config,
            Some(wrong_provider),
        )
        .await
        .expect_err("mismatched provider identity should fail before refill");

        assert!(
            error
                .to_string()
                .contains("provider identity does not match projection target")
        );
        assert!(
            find_projection_meta(
                &crud_store.database_connection(),
                THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY,
            )
            .await
            .expect("meta query should succeed")
            .is_none(),
            "provider mismatch should fail before writing a refill marker"
        );
    }

    #[tokio::test]
    #[ignore = "manual smoke: requires copied production DB and storage paths in env"]
    async fn thread_episodic_vector_refill_smoke_on_copied_production_db() {
        let Some(db_path) =
            std::env::var_os("PIONEER_THREAD_EPISODIC_VECTOR_REFILL_SMOKE_DB").map(PathBuf::from)
        else {
            eprintln!("skipping smoke: PIONEER_THREAD_EPISODIC_VECTOR_REFILL_SMOKE_DB is not set");
            return;
        };
        let Some(storage_root) =
            std::env::var_os("PIONEER_THREAD_EPISODIC_VECTOR_REFILL_SMOKE_STORAGE_ROOT")
                .map(PathBuf::from)
        else {
            eprintln!(
                "skipping smoke: PIONEER_THREAD_EPISODIC_VECTOR_REFILL_SMOKE_STORAGE_ROOT is not set"
            );
            return;
        };
        assert_manual_smoke_path_is_not_production(&db_path, "smoke DB");
        assert_manual_smoke_path_is_not_production(&storage_root, "smoke storage root");
        assert!(
            db_path.exists(),
            "smoke DB must exist: {}",
            db_path.display()
        );
        std::fs::create_dir_all(&storage_root).expect("smoke storage root should be creatable");

        pioneer_sqlite::zstd::register_auto_extension_once()
            .expect("sqlite-zstd extension should register");
        let database_url = pioneer_sqlite::sqlite_connection_url(db_path.as_path());
        let connection = Database::connect(database_url.as_str())
            .await
            .expect("smoke DB should connect");
        Migrator::up(&connection, None)
            .await
            .expect("smoke DB migrations should apply");
        let quick_check_statement =
            Statement::from_string(DatabaseBackend::Sqlite, "PRAGMA quick_check;".to_owned());
        let quick_check = connection
            .query_one_raw(quick_check_statement)
            .await
            .expect("quick_check query should succeed")
            .expect("quick_check should return a row")
            .try_get_by_index::<String>(0)
            .expect("quick_check row should decode");
        assert_eq!(quick_check, "ok");
        let unsafe_storage_uri_statement = Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) FROM thread_episodic_capsules WHERE storage_uri LIKE 'file://%/.pioneer/memory/%';"
                .to_owned(),
        );
        let unsafe_storage_uri_count = connection
            .query_one_raw(unsafe_storage_uri_statement)
            .await
            .expect("unsafe storage uri query should succeed")
            .expect("unsafe storage uri query should return a row")
            .try_get_by_index::<i64>(0)
            .expect("unsafe storage uri count should decode");
        assert_eq!(
            unsafe_storage_uri_count, 0,
            "smoke DB still points at production memory; rewrite copied storage_uri rows to a scratch storage root before running refill"
        );

        let crud_store = Arc::new(CrudStore::new(connection));
        let workspace_ids = crud_store
            .list_thread_episodic_refill_workspace_ids()
            .await
            .expect("smoke workspace ids should list");
        assert!(
            !workspace_ids.is_empty(),
            "copied production DB should contain refill workspaces"
        );

        let vector_config = GatewayThreadEpisodicVectorSearchConfig {
            enabled: true,
            provider: Some(GatewayThreadEpisodicVectorProviderConfig::OpenRouter),
            model: Some("smoke/test-embedding".to_owned()),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            embedding_normalized: true,
            use_search_instructions: false,
        };
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "smoke/test-embedding",
            vec![0.57735026, 0.57735026, 0.57735026],
        ));
        let projection_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                embedding_provider.as_ref(),
            )
            .expect("smoke provider target");

        let summary = refill_once_for_vector_search_config(
            crud_store.clone(),
            storage_root.as_path(),
            &vector_config,
            Some(embedding_provider),
        )
        .await
        .expect("copied production DB vector refill should complete");

        assert!(!summary.lock_contended);
        assert_eq!(summary.source_threads_failed, 0);
        assert_eq!(summary.failed_retryable_jobs, 0);
        assert_eq!(summary.failed_terminal_jobs, 0);
        assert!(!summary.has_incomplete_jobs);
        assert_eq!(
            crud_store
                .count_incomplete_thread_episodic_index_jobs()
                .await
                .expect("incomplete jobs should count"),
            0
        );
        assert!(
            refill_is_current_for_target(crud_store.as_ref(), &projection_target)
                .await
                .expect("vector projection current check should succeed")
        );
        let meta = find_projection_meta(
            &crud_store.database_connection(),
            &refill_projection_key_for_workspace(workspace_ids.first().unwrap()).unwrap(),
        )
        .await
        .expect("projection meta query should succeed")
        .expect("projection meta should exist");
        assert_eq!(
            meta.projection_config_hash.as_deref(),
            Some(projection_target.config_hash.as_str())
        );

        for workspace_id in workspace_ids {
            let capsules = crud_store
                .list_thread_episodic_workspace_capsules(workspace_id.as_str(), 100)
                .await
                .expect("workspace capsules should list");
            assert!(
                capsules
                    .iter()
                    .any(|capsule| capsule.status == ThreadEpisodicCapsuleStatus::Active),
                "workspace {workspace_id} should have active vector-refilled capsules"
            );
            for capsule in capsules {
                if let (Some(size_bytes), Some(capacity_bytes)) =
                    (capsule.size_bytes, capsule.capacity_bytes)
                {
                    assert!(
                        size_bytes <= capacity_bytes,
                        "capsule {} exceeds configured capacity: {size_bytes} > {capacity_bytes}",
                        capsule.id
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_rebuilds_workspace_capsule_from_database() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_a = "thread_refill_a";
        let thread_b = "thread_refill_b";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_a,
            "turn_refill_a",
            "item_refill_a",
            "workspace refill should index thread A database memory",
        )
        .await;
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_b,
            "turn_refill_b",
            "item_refill_b",
            "workspace refill should index thread B database memory",
        )
        .await;

        let summary = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("workspace refill should complete");

        assert!(!summary.skipped);
        assert_eq!(summary.workspace_count, 1);
        assert_eq!(summary.source_thread_count, 2);
        assert_eq!(summary.source_turn_count, 2);
        assert_eq!(summary.source_turn_item_count, 2);
        assert_eq!(summary.refill_jobs_enqueued, 2);
        assert_eq!(summary.completed_jobs, 2);
        let meta = find_projection_meta(
            &crud_store.database_connection(),
            refill_projection_key_for_workspace(workspace_id.as_str())
                .expect("workspace refill key should build")
                .as_str(),
        )
        .await
        .expect("meta query should succeed")
        .expect("meta exists");
        assert_eq!(meta.status, PROJECTION_META_STATUS_COMPLETE);
        assert_eq!(meta.source_thread_count, 2);
        assert_eq!(meta.source_turn_count, 2);
        assert_eq!(meta.source_turn_item_count, 2);
        assert_eq!(meta.source_turn_event_count, 2);
        let capsules = crud_store
            .list_thread_episodic_workspace_capsules(workspace_id.as_str(), 10)
            .await
            .expect("workspace capsules should list");
        assert_eq!(capsules.len(), 1);
        assert_eq!(
            capsules[0].thread_id,
            pioneer_crud::THREAD_EPISODIC_WORKSPACE_CAPSULE_THREAD_ID
        );
        assert!(PathBuf::from(capsules[0].storage_uri.trim_start_matches("file://")).exists());
        for thread_id in [thread_a, thread_b] {
            let items = crud_store
                .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
                .await
                .expect("items should list");
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].status, ThreadEpisodicItemStatus::Active);
            assert_eq!(
                items[0].capsule_id.as_deref(),
                Some(capsules[0].id.as_str())
            );
            assert!(
                items[0]
                    .frame_uri
                    .as_deref()
                    .expect("frame uri")
                    .starts_with("mv2://workspace/")
            );
        }
    }

    #[tokio::test]
    async fn thread_episodic_fresh_refill_indexes_all_seven_canonical_task_summaries() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_refill_seven_tasks";
        let mut expected = std::collections::BTreeMap::new();
        for index in 0..7 {
            let turn_id = format!("turn_refill_task_{index}");
            let item_id = format!("item_refill_task_{index}");
            let title = format!("Refill task title {index}");
            let preview = format!("Refill task preview {index}");
            let task = |status| TurnItem::Task {
                item: TaskTurnItem {
                    id: item_id.clone(),
                    task_id: format!("task_refill_{index}"),
                    created_by_turn_id: None,
                    run_id: Some(format!("run_refill_{index}")),
                    parent_task_id: None,
                    root_task_id: None,
                    title: title.clone(),
                    status,
                    attachment: pioneer_protocol::TaskAttachmentMode::Attached,
                    trigger_kind: TaskTriggerKind::Immediate,
                    executor_kind: TaskExecutorKind::Agent,
                    child_thread_id: None,
                    child_turn_id: None,
                    agent_role: None,
                    depth: 0,
                    max_depth: 3,
                    next_fire_at: None,
                    progress_preview: None,
                    result_preview: Some(preview.clone()),
                    error_preview: None,
                    started_at: Some(1_700_030_000),
                    created_at: 1_700_030_000,
                    updated_at: 1_700_030_001,
                },
            };
            materialize_thread_with_item(
                crud_store.as_ref(),
                workspace_id.as_str(),
                thread_id,
                turn_id.as_str(),
                task(if index == 6 {
                    TaskStatus::Scheduled
                } else {
                    TaskStatus::Running
                }),
                1_700_030_000 + index,
            )
            .await;
            crud_store
                .materialize_item_snapshot_updated(
                    ItemUpdatedNotification {
                        workspace_id: workspace_id.clone(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.clone(),
                        item: task(TaskStatus::Completed),
                    },
                    1_700_030_100 + index,
                )
                .await
                .expect("canonical completed task should update");
            expected.insert(item_id, format!("{title}: {preview} (completed)"));
        }

        let summary = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("fresh seven-task refill should complete");
        assert_eq!(summary.completed_jobs, 7);
        let items = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 20)
            .await
            .expect("task items should list");
        assert_eq!(items.len(), 7);
        let jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 20)
            .await
            .expect("task jobs should list");
        assert_eq!(jobs.len(), 7);
        assert!(
            jobs.iter()
                .all(|job| job.status == ThreadEpisodicIndexJobStatus::Completed)
        );
        for item in items {
            let text = expected
                .get(item.item_id.as_str())
                .expect("expected task text");
            assert_eq!(item.status, ThreadEpisodicItemStatus::Active);
            assert_eq!(
                item.source_text_hash,
                crate::thread_episodic::source_text_hash(text)
            );
            assert!(item.frame_id.is_some());
            assert!(item.frame_uri.is_some());
        }
        let meta = find_projection_meta(
            &crud_store.database_connection(),
            refill_projection_key_for_workspace(workspace_id.as_str())
                .unwrap()
                .as_str(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(meta.status, PROJECTION_META_STATUS_COMPLETE);
    }

    #[tokio::test]
    async fn thread_episodic_fresh_and_resume_preserve_exclusion_and_user_tombstone() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_refill_preserved_controls";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_refill_excluded",
            "item_refill_excluded",
            "excluded source",
        )
        .await;
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_refill_deleted",
            "item_refill_deleted",
            "deleted source",
        )
        .await;
        let items = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("control items should list");
        let excluded = items
            .iter()
            .find(|item| item.item_id == "item_refill_excluded")
            .unwrap();
        crud_store
            .exclude_thread_episodic_item(
                NewThreadEpisodicExclusionRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: thread_id.to_owned(),
                    index_item_id: excluded.id.clone(),
                    reason: ThreadEpisodicExclusionReason::UserRequested,
                    created_by: "refill-control-test".to_owned(),
                },
                1_700_045_000,
            )
            .await
            .expect("exclusion should insert");
        crud_store
            .tombstone_thread_episodic_items_for_item(
                workspace_id.as_str(),
                thread_id,
                "turn_refill_deleted",
                "item_refill_deleted",
                1_700_045_001,
            )
            .await
            .expect("user deletion should persist");

        refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("fresh refill should preserve control-plane decisions");
        mark_existing_refill_failed(crud_store.as_ref(), workspace_id.as_str()).await;
        refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("resume should preserve control-plane decisions");

        let preserved = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("preserved items should list");
        assert_eq!(preserved.len(), 2);
        let deleted = preserved
            .iter()
            .find(|item| item.item_id == "item_refill_deleted")
            .unwrap();
        assert_eq!(deleted.status, ThreadEpisodicItemStatus::Deleted);
        assert!(
            crud_store
                .find_thread_episodic_exclusion_by_item(
                    workspace_id.as_str(),
                    thread_id,
                    excluded.id.as_str(),
                )
                .await
                .expect("exclusion lookup should succeed")
                .is_some()
        );
        let jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(&workspace_id, thread_id, 10)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 2);
        assert!(
            jobs.iter()
                .all(|job| job.status == ThreadEpisodicIndexJobStatus::Canceled)
        );
        assert_eq!(
            jobs.iter()
                .find(|job| job.index_item_id == excluded.id)
                .unwrap()
                .last_error
                .as_deref(),
            Some(THREAD_EPISODIC_USER_EXCLUDED_ERROR)
        );
        assert_eq!(
            jobs.iter()
                .find(|job| job.index_item_id == deleted.id)
                .unwrap()
                .last_error
                .as_deref(),
            Some(THREAD_EPISODIC_USER_DELETED_ERROR)
        );
        assert!(
            crud_store
                .claim_due_thread_episodic_index_jobs_for_workspace(
                    &workspace_id,
                    chrono::Utc::now().timestamp(),
                    10,
                    5
                )
                .await
                .unwrap()
                .assert_no_failures_for_test()
                .is_empty()
        );
        refill_once(crud_store.clone(), temp_dir.path())
            .await
            .unwrap();
        assert_eq!(
            crud_store
                .list_thread_episodic_index_jobs_for_thread(&workspace_id, thread_id, 10)
                .await
                .unwrap(),
            jobs
        );

        // A genuine model replacement must also preserve user lifecycle
        // decisions, including the existing canceled execution identities.
        let provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "vendor/controls-model-b",
            vec![0.2; 4],
        ));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            provider.as_ref(),
        )
        .unwrap();
        for _ in 0..2 {
            let summary = refill_once_with_test_executor_config(
                crud_store.clone(),
                temp_dir.path(),
                &workspace_id,
                target.clone(),
                provider.clone(),
                immediate_retry_config(1),
            )
            .await
            .unwrap();
            assert_eq!(summary.completed_jobs, 0);
            assert_eq!(provider.calls(), 0);
            assert_eq!(
                crud_store
                    .list_thread_episodic_index_jobs_for_thread(&workspace_id, thread_id, 10)
                    .await
                    .unwrap(),
                jobs
            );
            assert_eq!(
                crud_store
                    .list_thread_episodic_items_for_thread(&workspace_id, thread_id, 10)
                    .await
                    .unwrap(),
                preserved
            );
            assert!(
                crud_store
                    .claim_due_thread_episodic_index_jobs_for_workspace(
                        &workspace_id,
                        chrono::Utc::now().timestamp(),
                        10,
                        5,
                    )
                    .await
                    .unwrap()
                    .assert_no_failures_for_test()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn thread_episodic_resume_ignores_preexisting_canceled_error_after_user_exclusion() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_resume_excluded_canceled";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_resume_excluded_canceled",
            "item_resume_excluded_canceled",
            "excluded source with an old canceled job",
        )
        .await;
        let item = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("item should list")
            .pop()
            .expect("item should exist");
        let claim_time = chrono::Utc::now().timestamp().saturating_add(60);
        let claimed = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id.as_str(),
                claim_time,
                1,
                5,
            )
            .await
            .expect("old job should claim")
            .assert_no_failures_for_test()
            .pop()
            .expect("old job should exist");
        crud_store
            .fail_thread_episodic_index_attempt_without_source_validation(
                claimed.id.as_str(),
                claimed.attempt_count,
                ThreadEpisodicIndexJobFailureUpdate {
                    retryable: false,
                    next_run_at_unix: None,
                    last_error: Some(
                        pioneer_crud::THREAD_EPISODIC_LEGACY_SOURCE_HASH_MISMATCH_ERROR.to_owned(),
                    ),
                    capacity_error: false,
                    last_attempt_latency_ms: None,
                },
                claim_time.saturating_add(1),
            )
            .await
            .expect("old mismatch should persist");
        crud_store
            .exclude_thread_episodic_item(
                NewThreadEpisodicExclusionRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: thread_id.to_owned(),
                    index_item_id: item.id.clone(),
                    reason: ThreadEpisodicExclusionReason::UserRequested,
                    created_by: "user".to_owned(),
                },
                claim_time.saturating_add(2),
            )
            .await
            .expect("production exclusion save must reconcile its canceled job");
        mark_refill_marker_with_workspace_target(
            crud_store.as_ref(),
            workspace_id.as_str(),
            PROJECTION_META_STATUS_FAILED,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
        )
        .await;

        refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("resume should not treat an explicitly excluded occurrence as failed work");

        let stored_item = crud_store
            .find_thread_episodic_item(item.id.as_str())
            .await
            .expect("excluded item lookup should succeed")
            .expect("excluded item should remain");
        assert_eq!(stored_item.status, ThreadEpisodicItemStatus::Excluded);
        assert!(stored_item.frame_id.is_none());
        let stored_job = crud_store
            .find_thread_episodic_index_job(claimed.id.as_str())
            .await
            .expect("excluded job lookup should succeed")
            .expect("excluded job should remain");
        assert_eq!(stored_job.status, ThreadEpisodicIndexJobStatus::Canceled);
        assert_eq!(stored_job.attempt_count, claimed.attempt_count);
        assert_eq!(
            stored_job.last_error.as_deref(),
            Some(pioneer_crud::THREAD_EPISODIC_USER_EXCLUDED_ERROR)
        );
        assert_eq!(
            crud_store
                .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id.as_str())
                .await
                .expect("excluded canceled job should not block refill"),
            0
        );

        mark_existing_refill_failed(crud_store.as_ref(), workspace_id.as_str()).await;
        let repeated = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("reconciled legacy exclusion should remain idempotent");
        assert_eq!(repeated.refill_jobs_enqueued, 0);

        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            "thread_resume_independent_terminal",
            "turn_resume_independent_terminal",
            "item_resume_independent_terminal",
            "independent source with a real terminal error",
        )
        .await;
        let independent_claim = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id.as_str(),
                chrono::Utc::now().timestamp().saturating_add(60),
                1,
                5,
            )
            .await
            .expect("independent job should claim")
            .assert_no_failures_for_test()
            .pop()
            .expect("independent job should exist");
        crud_store
            .fail_thread_episodic_index_attempt_without_source_validation(
                independent_claim.id.as_str(),
                independent_claim.attempt_count,
                ThreadEpisodicIndexJobFailureUpdate {
                    retryable: false,
                    next_run_at_unix: None,
                    last_error: Some("independent terminal provider failure".to_owned()),
                    capacity_error: false,
                    last_attempt_latency_ms: None,
                },
                chrono::Utc::now().timestamp().saturating_add(61),
            )
            .await
            .expect("independent terminal error should persist");
        mark_existing_refill_failed(crud_store.as_ref(), workspace_id.as_str()).await;
        refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect_err("an unrelated genuine terminal error must still fail resume");
    }

    #[tokio::test]
    async fn resumed_refill_executes_one_failed_job_without_history_access() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        for index in 0..64 {
            ingest_materialized_user_item(
                crud_store.clone(),
                &workspace_id,
                "ready_history",
                &format!("ready_turn_{index}"),
                &format!("ready_item_{index}"),
                &format!("ready content {index}"),
            )
            .await;
        }
        let provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            provider.as_ref(),
        )
        .unwrap();
        refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            &workspace_id,
            target.clone(),
            provider,
            immediate_retry_config(5),
        )
        .await
        .unwrap();
        let ready_jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(&workspace_id, "ready_history", 100)
            .await
            .unwrap();
        assert_eq!(ready_jobs.len(), 64);
        assert!(
            ready_jobs
                .iter()
                .all(|job| job.status == ThreadEpisodicIndexJobStatus::Completed)
        );
        ingest_materialized_user_item(
            crud_store.clone(),
            &workspace_id,
            "unfinished",
            "unfinished_turn",
            "unfinished_item",
            "continue only this job",
        )
        .await;
        let now = chrono::Utc::now().timestamp();
        let claim = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace_id, now, 1, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test()
            .pop()
            .unwrap();
        crud_store
            .fail_thread_episodic_index_attempt_without_source_validation(
                &claim.id,
                claim.attempt_count,
                ThreadEpisodicIndexJobFailureUpdate {
                    retryable: true,
                    next_run_at_unix: Some(now),
                    last_error: Some("provider temporarily unavailable".to_owned()),
                    capacity_error: false,
                    last_attempt_latency_ms: None,
                },
                now,
            )
            .await
            .unwrap();
        mark_existing_refill_failed(crud_store.as_ref(), &workspace_id).await;
        let db = crud_store.database_connection();
        let marker = find_projection_meta(
            &db,
            &refill_projection_key_for_workspace(&workspace_id).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        // Any history DTO load/reindex now fails. Canonical reads for the one
        // current job remain available, as required by source freshness guards.
        make_refill_history_unavailable(&crud_store).await;
        let resumed_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        let summary = refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            &workspace_id,
            target,
            resumed_provider.clone(),
            immediate_retry_config(5),
        )
        .await
        .unwrap();
        assert!(summary.resumed);
        assert_eq!(summary.source_threads_reindexed, 0);
        assert_eq!(summary.refill_jobs_enqueued, 0);
        assert_eq!(
            summary.source_turn_item_count,
            marker.source_turn_item_count
        );
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(resumed_provider.calls(), 1);
        assert_eq!(
            crud_store
                .list_thread_episodic_index_jobs_for_thread(&workspace_id, "ready_history", 100)
                .await
                .unwrap(),
            ready_jobs
        );
    }

    #[tokio::test]
    async fn complete_marker_executes_later_interrupted_job_without_history() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only();
        mark_refill_marker_with_workspace_target(
            &crud_store,
            &workspace_id,
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &target,
        )
        .await;
        let marker = find_refill_projection_meta_for_workspace(&crud_store, &workspace_id)
            .await
            .unwrap();
        ingest_materialized_user_item(
            crud_store.clone(),
            &workspace_id,
            "later_job_thread",
            "later_job_turn",
            "later_job_item",
            "created after refill completed",
        )
        .await;
        let now = chrono::Utc::now().timestamp();
        let old = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace_id, now, 1, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test()
            .pop()
            .unwrap();
        assert_eq!(
            super::super::recover_inherited_thread_episodic_jobs(&crud_store, 5)
                .await
                .unwrap(),
            1
        );
        make_refill_history_unavailable(&crud_store).await;
        // Existing lexical work needs no provider and must not rediscover history.
        let summary = refill_once_with_projection_resolver(
            crud_store.clone(),
            temp_dir.path(),
            &workspace_id,
            target,
            None,
            None,
        )
        .await
        .unwrap();
        assert!(summary.resumed);
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(summary.source_threads_reindexed, 0);
        let completed = crud_store
            .find_thread_episodic_index_job(&old.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.status, ThreadEpisodicIndexJobStatus::Completed);
        assert_eq!(completed.attempt_count, old.attempt_count + 1);
        let current_marker = find_refill_projection_meta_for_workspace(&crud_store, &workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            current_marker.source_turn_item_count,
            marker.unwrap().source_turn_item_count
        );
        assert_eq!(
            crud_store
                .fail_thread_episodic_index_attempt_without_source_validation(
                    &old.id,
                    old.attempt_count,
                    ThreadEpisodicIndexJobFailureUpdate {
                        retryable: true,
                        next_run_at_unix: Some(now + 10),
                        last_error: Some("obsolete attempt".to_owned()),
                        capacity_error: false,
                        last_attempt_latency_ms: None,
                    },
                    now + 1
                )
                .await
                .unwrap(),
            ThreadEpisodicIndexAttemptOutcome::StaleAttempt
        );
    }

    #[tokio::test]
    async fn pending_preparation_is_not_proven_by_one_existing_job() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        ingest_materialized_user_item(
            crud_store.clone(),
            &workspace_id,
            "prepared_prefix",
            "prepared_turn",
            "prepared_item",
            "already prepared prefix",
        )
        .await;
        materialize_thread_with_item(
            crud_store.as_ref(),
            &workspace_id,
            "unprepared_suffix",
            "unprepared_turn",
            TurnItem::UserMessage {
                id: "unprepared_item".to_owned(),
                text: "preparation must include this suffix".to_owned(),
                attachments: vec![],
            },
            1_700_000_000,
        )
        .await;
        let original = crud_store
            .list_thread_episodic_index_jobs_for_thread(&workspace_id, "prepared_prefix", 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only();
        mark_refill_preparing(&crud_store.database_connection(), &workspace_id, &target)
            .await
            .unwrap();
        let summary = refill_once_with_workspace_projection(
            crud_store.clone(),
            temp_dir.path(),
            &workspace_id,
            target,
            None,
        )
        .await
        .unwrap();
        assert!(!summary.resumed);
        assert_eq!(summary.source_threads_reindexed, 2);
        assert_eq!(summary.completed_jobs, 2);
        assert_eq!(
            crud_store
                .find_thread_episodic_index_job(&original.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ThreadEpisodicIndexJobStatus::Completed
        );
    }

    #[tokio::test]
    async fn interrupted_model_cleanup_restarts_even_when_config_reverts_to_complete_target() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        ingest_materialized_user_item(
            crud_store.clone(),
            &workspace_id,
            "reset_crash_thread",
            "reset_crash_turn",
            "reset_crash_item",
            "preserved source",
        )
        .await;
        refill_once(crud_store.clone(), temp_dir.path())
            .await
            .unwrap();
        let before = crud_store
            .list_thread_episodic_index_jobs_for_thread(&workspace_id, "reset_crash_thread", 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(before.status, ThreadEpisodicIndexJobStatus::Completed);
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only();
        let db = crud_store.database_connection();
        mark_projection_reset(&db, &workspace_id, PROJECTION_META_STATUS_PENDING, &target)
            .await
            .unwrap();
        // Simulate a crash after artifact deletion, before all jobs were reset.
        crud_store
            .delete_thread_episodic_capsules_for_workspace(&workspace_id)
            .await
            .unwrap();
        assert!(
            !refill_is_current_for_workspace_target(&crud_store, &workspace_id, &target)
                .await
                .unwrap()
        );
        assert!(
            !refill_is_current_for_target(&crud_store, &target)
                .await
                .unwrap()
        );
        assert_eq!(
            refill_status_for_workspace_target(&crud_store, &workspace_id, &target)
                .await
                .unwrap(),
            GatewayThreadEpisodicVectorRefillStatus::Required
        );
        assert_eq!(
            find_refill_projection_meta_for_workspace(&crud_store, &workspace_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            PROJECTION_META_STATUS_COMPLETE
        );
        // The marker still matches the reverted config, but cannot skip cleanup.
        let summary = refill_once_with_workspace_projection(
            crud_store.clone(),
            temp_dir.path(),
            &workspace_id,
            target,
            None,
        )
        .await
        .unwrap();
        assert!(!summary.skipped);
        assert!(!summary.resumed);
        assert_eq!(summary.completed_jobs, 1);
        assert!(
            !projection_reset_is_pending(&crud_store, &workspace_id)
                .await
                .unwrap()
        );
        let rebuilt = crud_store
            .find_thread_episodic_index_job_by_item(&before.index_item_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rebuilt.index_item_id, before.index_item_id);
        assert_eq!(rebuilt.status, ThreadEpisodicIndexJobStatus::Completed);
        assert_ne!(rebuilt.id, before.id);
        assert_eq!(rebuilt.attempt_count, 1);
    }

    #[tokio::test]
    async fn model_replacement_fences_old_identity_and_reset_resume_preserves_backoff() {
        let (crud_store, _, workspace_id) = setup_store().await;
        ingest_materialized_user_item(
            crud_store.clone(),
            &workspace_id,
            "model_claim_thread",
            "model_claim_turn",
            "model_claim_item",
            "current source",
        )
        .await;
        let now = chrono::Utc::now().timestamp();
        let old = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace_id, now, 1, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test()
            .pop()
            .unwrap();
        mark_projection_reset(
            &crud_store.database_connection(),
            &workspace_id,
            PROJECTION_META_STATUS_PENDING,
            &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
        )
        .await
        .unwrap();
        crud_store
            .reset_thread_episodic_projection(&workspace_id, now + 1)
            .await
            .unwrap();
        let reset = crud_store
            .find_thread_episodic_index_job_by_item(&old.index_item_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reset.status, ThreadEpisodicIndexJobStatus::Queued);
        assert_ne!(reset.id, old.id);
        assert_eq!(reset.attempt_count, 0);
        let failure = ThreadEpisodicIndexJobFailureUpdate {
            retryable: true,
            next_run_at_unix: Some(now + 600),
            last_error: Some("provider backoff".to_owned()),
            capacity_error: false,
            last_attempt_latency_ms: None,
        };
        assert_eq!(
            crud_store
                .fail_thread_episodic_index_attempt_without_source_validation(
                    &old.id,
                    old.attempt_count,
                    failure.clone(),
                    now + 1
                )
                .await
                .unwrap(),
            ThreadEpisodicIndexAttemptOutcome::StaleAttempt
        );
        let current = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace_id, now + 1, 1, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test()
            .pop()
            .unwrap();
        assert_ne!(current.id, old.id);
        assert_eq!(current.attempt_count, 1);
        crud_store
            .fail_thread_episodic_index_attempt_without_source_validation(
                &current.id,
                current.attempt_count,
                failure,
                now + 1,
            )
            .await
            .unwrap();
        let deferred = crud_store
            .find_thread_episodic_index_job(&current.id)
            .await
            .unwrap()
            .unwrap();
        crud_store
            .reset_thread_episodic_projection(&workspace_id, now + 2)
            .await
            .unwrap();
        assert_eq!(
            crud_store
                .find_thread_episodic_index_job(&current.id)
                .await
                .unwrap()
                .unwrap(),
            deferred
        );
        assert_eq!(deferred.status, ThreadEpisodicIndexJobStatus::Failed);
        assert_eq!(deferred.next_run_at.timestamp(), now + 600);
    }

    #[tokio::test]
    async fn workspace_due_jobs_preserve_backoff_and_terminal_states() {
        let (crud_store, _, workspace_id) = setup_store().await;
        for index in 0..3 {
            ingest_materialized_user_item(
                crud_store.clone(),
                &workspace_id,
                "due_jobs",
                &format!("due_turn_{index}"),
                &format!("due_item_{index}"),
                &format!("due text {index}"),
            )
            .await;
        }
        let now = chrono::Utc::now().timestamp();
        let claims = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace_id, now, 3, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test();
        assert_eq!(claims.len(), 3);
        crud_store
            .fail_thread_episodic_index_attempt_without_source_validation(
                &claims[0].id,
                claims[0].attempt_count,
                ThreadEpisodicIndexJobFailureUpdate {
                    retryable: true,
                    next_run_at_unix: Some(now + 600),
                    last_error: Some("backoff".to_owned()),
                    capacity_error: false,
                    last_attempt_latency_ms: None,
                },
                now,
            )
            .await
            .unwrap();
        crud_store
            .cancel_thread_episodic_index_attempt(
                &claims[1].id,
                claims[1].attempt_count,
                "terminal",
                now,
            )
            .await
            .unwrap();
        assert!(
            crud_store
                .claim_due_thread_episodic_index_jobs_for_workspace(&workspace_id, now, 10, 5)
                .await
                .unwrap()
                .assert_no_failures_for_test()
                .is_empty()
        );
        assert_eq!(
            super::super::recover_inherited_thread_episodic_jobs(&crud_store, 5)
                .await
                .unwrap(),
            1
        );
        let recovered = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace_id, now + 1, 10, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, claims[2].id);
        assert_eq!(recovered[0].attempt_count, claims[2].attempt_count + 1);
        assert_eq!(
            crud_store
                .fail_thread_episodic_index_attempt_without_source_validation(
                    &claims[2].id,
                    claims[2].attempt_count,
                    ThreadEpisodicIndexJobFailureUpdate {
                        retryable: false,
                        next_run_at_unix: None,
                        last_error: Some("late attempt".to_owned()),
                        capacity_error: false,
                        last_attempt_latency_ms: None
                    },
                    now + 2
                )
                .await
                .unwrap(),
            ThreadEpisodicIndexAttemptOutcome::StaleAttempt
        );
        assert_eq!(
            crud_store
                .find_thread_episodic_index_job(&claims[0].id)
                .await
                .unwrap()
                .unwrap()
                .next_run_at
                .timestamp(),
            now + 600
        );
        assert_eq!(
            crud_store
                .find_thread_episodic_index_job(&claims[1].id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ThreadEpisodicIndexJobStatus::Canceled
        );
    }

    async fn setup_concurrent_store() -> (Arc<CrudStore>, TempDir, String) {
        let temp_dir = TempDir::new().expect("temp dir");
        let database_path = temp_dir.path().join("gateway.sqlite");
        let mut writer_options =
            ConnectOptions::new(format!("sqlite://{}?mode=rwc", database_path.display()));
        writer_options.max_connections(1).sqlx_logging(false);
        let writer = Database::connect(writer_options)
            .await
            .expect("must connect test writer");
        Migrator::up(&writer, None)
            .await
            .expect("migrations must succeed");
        bootstrap(&writer)
            .await
            .expect("gateway bootstrap should create default workspace");
        writer
            .execute_unprepared("PRAGMA journal_mode=WAL")
            .await
            .expect("test database should enable WAL");
        pioneer_entity::workspace::Entity::delete_many()
            .exec(&writer)
            .await
            .unwrap();
        let workspace_id = WorkspaceManager::new(writer.clone())
            .create_workspace(
                &pioneer_protocol::generate_id(),
                Some("Concurrent refill test"),
            )
            .await
            .expect("isolated workspace should exist")
            .id;
        let mut reader_options =
            ConnectOptions::new(format!("sqlite://{}?mode=ro", database_path.display()));
        reader_options
            .max_connections(4)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
        let reader = Database::connect(reader_options)
            .await
            .expect("must connect test reader");
        (
            Arc::new(CrudStore::new(SqliteDatabase::new(reader, writer))),
            temp_dir,
            workspace_id,
        )
    }

    async fn mark_existing_refill_failed(crud_store: &CrudStore, workspace_id: &str) {
        let projection_key = refill_projection_key_for_workspace(workspace_id)
            .expect("workspace refill key should build");
        assert!(
            update_projection_meta_status(
                &crud_store.database_connection(),
                projection_key.as_str(),
                PROJECTION_META_STATUS_FAILED,
                Some("injected interruption after durable refill state"),
                now_datetime(),
            )
            .await
            .expect("existing refill marker status should update"),
            "existing refill marker should remain available for resume"
        );
    }

    #[tokio::test]
    async fn unknown_custom_dimension_is_resolved_before_resume_or_replacement() {
        for (dimension, version) in [
            (3, THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION),
            (4, THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION),
            (3, THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION - 1),
        ] {
            let (store, root, workspace) = setup_store().await;
            ingest_materialized_user_item(
                store.clone(),
                &workspace,
                "custom_ready",
                "custom_ready_turn",
                "custom_ready_item",
                "old ready frame",
            )
            .await;
            let first = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
                "openrouter",
                "vendor/custom-embed",
                vec![0.1; 3],
            ));
            let first_target =
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                    first.as_ref(),
                )
                .unwrap();
            refill_once_with_test_executor_config(
                store.clone(),
                root.path(),
                &workspace,
                first_target.clone(),
                first,
                immediate_retry_config(2),
            )
            .await
            .unwrap();
            let before = store
                .list_thread_episodic_items_for_thread(&workspace, "custom_ready", 10)
                .await
                .unwrap()
                .pop()
                .unwrap();
            let old_job = store
                .find_thread_episodic_index_job_by_item(&before.id)
                .await
                .unwrap()
                .unwrap();
            ingest_materialized_user_item(
                store.clone(),
                &workspace,
                "custom_extra",
                "custom_extra_turn",
                "custom_extra_item",
                "remaining prepared source",
            )
            .await;
            mark_refill_marker_with_workspace_target(
                &store,
                &workspace,
                PROJECTION_META_STATUS_BACKFILLING,
                version,
                &first_target,
            )
            .await;
            let configured =
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                    &vector_search_config(
                        GatewayThreadEpisodicVectorProviderConfig::OpenRouter,
                        "vendor/custom-embed",
                        0,
                    ),
                );
            assert_eq!(configured.payload.dimension, None);
            let replacement =
                dimension != 3 || version != THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION;
            if !replacement {
                // Any history read is an error; both sources already have jobs.
                make_refill_history_unavailable(&store).await;
            }
            let provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
                "openrouter",
                "vendor/custom-embed",
                vec![0.1; dimension],
            ));
            let final_target =
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                    provider.as_ref(),
                )
                .unwrap();
            let summary = refill_once_with_test_executor_config(
                store.clone(),
                root.path(),
                &workspace,
                configured,
                provider.clone(),
                immediate_retry_config(2),
            )
            .await
            .unwrap();
            assert_eq!(summary.resumed, !replacement);
            let after = store
                .find_thread_episodic_item(&before.id)
                .await
                .unwrap()
                .unwrap();
            let after_job = store
                .find_thread_episodic_index_job_by_item(&before.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(after.status, ThreadEpisodicItemStatus::Active);
            assert_eq!(after_job.status, ThreadEpisodicIndexJobStatus::Completed);
            if replacement {
                assert!(summary.source_threads_reindexed > 0);
                assert_eq!(summary.capsule_files_deleted, 1);
                assert_eq!(summary.capsule_rows_deleted, 1);
                assert_eq!(summary.index_jobs_replaced, 2);
                assert_ne!(after_job.id, old_job.id);
                assert_eq!(after_job.attempt_count, 1);
                assert!(
                    store
                        .find_thread_episodic_index_job(&old_job.id)
                        .await
                        .unwrap()
                        .is_none()
                );
            } else {
                assert_eq!(summary.source_threads_reindexed, 0);
                assert_eq!(after, before);
                assert_eq!(after_job, old_job);
                assert_eq!(provider.calls(), 1);
            }
            let artifact = store
                .find_thread_episodic_embedding_artifact(
                    after.embedding_artifact_id.as_deref().unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(artifact.dimension, dimension);
            let extra = store
                .list_thread_episodic_items_for_thread(&workspace, "custom_extra", 10)
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(extra.status, ThreadEpisodicItemStatus::Active);
            let extra_artifact = store
                .find_thread_episodic_embedding_artifact(
                    extra.embedding_artifact_id.as_deref().unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(extra_artifact.dimension, dimension);
            assert_capsule_contains_payload(&store, &after, "old ready frame").await;
            assert_capsule_contains_payload(&store, &extra, "remaining prepared source").await;
            assert!(
                refill_is_current_for_workspace_target(&store, &workspace, &final_target)
                    .await
                    .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn exhausted_model_a_gets_fresh_model_b_work_and_old_callback_is_fenced() {
        let (store, root, workspace) = setup_store().await;
        ingest_materialized_user_item(
            store.clone(),
            &workspace,
            "budget_thread",
            "budget_turn",
            "budget_item",
            "source survives old model exhaustion",
        )
        .await;
        let model_a = Arc::new(StaticThreadEpisodicEmbeddingProvider::retryable_failure());
        let target_a =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                model_a.as_ref(),
            )
            .unwrap();
        assert!(
            refill_once_with_test_executor_config(
                store.clone(),
                root.path(),
                &workspace,
                target_a.clone(),
                model_a,
                immediate_retry_config(1)
            )
            .await
            .is_err()
        );
        let source = store
            .list_thread_episodic_items_for_thread(&workspace, "budget_thread", 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let exhausted = store
            .find_thread_episodic_index_job_by_item(&source.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exhausted.status, ThreadEpisodicIndexJobStatus::Canceled);
        assert_eq!(exhausted.attempt_count, 1);
        let retry_a = Arc::new(StaticThreadEpisodicEmbeddingProvider::retryable_failure());
        assert!(
            refill_once_with_test_executor_config(
                store.clone(),
                root.path(),
                &workspace,
                target_a,
                retry_a.clone(),
                immediate_retry_config(1)
            )
            .await
            .is_err()
        );
        assert_eq!(retry_a.calls(), 0);
        assert_eq!(
            store
                .find_thread_episodic_index_job(&exhausted.id)
                .await
                .unwrap()
                .unwrap(),
            exhausted
        );
        let model_b = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "vendor/model-b",
            vec![0.2; 4],
        ));
        let target_b =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                model_b.as_ref(),
            )
            .unwrap();
        let result = refill_once_with_test_executor_config(
            store.clone(),
            root.path(),
            &workspace,
            target_b.clone(),
            model_b.clone(),
            immediate_retry_config(1),
        )
        .await
        .unwrap();
        assert!(!result.resumed);
        assert_eq!(result.completed_jobs, 1);
        assert_eq!(model_b.calls(), 1);
        let completed = store
            .find_thread_episodic_index_job_by_item(&source.id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(completed.id, exhausted.id);
        assert_eq!(completed.attempt_count, 1);
        assert_eq!(completed.status, ThreadEpisodicIndexJobStatus::Completed);
        assert!(
            store
                .find_thread_episodic_index_job(&exhausted.id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .fail_thread_episodic_index_attempt_without_source_validation(
                    &exhausted.id,
                    exhausted.attempt_count,
                    ThreadEpisodicIndexJobFailureUpdate {
                        retryable: false,
                        next_run_at_unix: None,
                        last_error: Some("late model A callback".to_owned()),
                        capacity_error: false,
                        last_attempt_latency_ms: None
                    },
                    chrono::Utc::now().timestamp()
                )
                .await
                .unwrap(),
            ThreadEpisodicIndexAttemptOutcome::StaleAttempt
        );
        let active = store
            .find_thread_episodic_item(&source.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.source_text_hash, source.source_text_hash);
        assert_eq!(active.status, ThreadEpisodicItemStatus::Active);
        let artifact = store
            .find_thread_episodic_embedding_artifact(
                active.embedding_artifact_id.as_deref().unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(artifact.model, "vendor/model-b");
        assert_eq!(artifact.dimension, 4);
        assert!(
            refill_is_current_for_workspace_target(&store, &workspace, &target_b)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn interrupted_model_reset_does_not_renew_exhausted_new_model_budget() {
        let (store, root, workspace) = setup_store().await;
        ingest_materialized_user_item(
            store.clone(),
            &workspace,
            "reset_budget",
            "reset_budget_turn",
            "reset_budget_item",
            "restart-safe execution identity",
        )
        .await;
        let model_a = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![0.1; 3]));
        let target_a =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                model_a.as_ref(),
            )
            .unwrap();
        refill_once_with_test_executor_config(
            store.clone(),
            root.path(),
            &workspace,
            target_a,
            model_a,
            immediate_retry_config(1),
        )
        .await
        .unwrap();
        let source = store
            .list_thread_episodic_items_for_thread(&workspace, "reset_budget", 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let old = store
            .find_thread_episodic_index_job_by_item(&source.id)
            .await
            .unwrap()
            .unwrap();
        let model_b = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "vendor/model-b",
            vec![0.1; 4],
        ));
        let target_b =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                model_b.as_ref(),
            )
            .unwrap();
        mark_projection_reset(
            &store.database_connection(),
            &workspace,
            PROJECTION_META_STATUS_PENDING,
            &target_b,
        )
        .await
        .unwrap();
        cleanup_derived_artifacts(
            &store,
            chrono::Utc::now().timestamp(),
            root.path(),
            &workspace,
            pioneer_memory::lock_thread_episodic_workspace(&workspace).await,
        )
        .await
        .unwrap();
        let now = chrono::Utc::now().timestamp();
        let claim = store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace, now, 1, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test()
            .pop()
            .unwrap();
        assert_ne!(claim.id, old.id);
        assert_eq!(claim.attempt_count, 1);
        store
            .fail_thread_episodic_index_attempt_without_source_validation(
                &claim.id,
                claim.attempt_count,
                ThreadEpisodicIndexJobFailureUpdate {
                    retryable: false,
                    next_run_at_unix: None,
                    last_error: Some("model B terminal provider failure".to_owned()),
                    capacity_error: false,
                    last_attempt_latency_ms: None,
                },
                now,
            )
            .await
            .unwrap();
        let exhausted = store
            .find_thread_episodic_index_job(&claim.id)
            .await
            .unwrap()
            .unwrap();
        // Simulate restart after committed replacement/execution, before the
        // new pending target marker and checkpoint completion were written.
        assert!(
            refill_once_with_test_executor_config(
                store.clone(),
                root.path(),
                &workspace,
                target_b.clone(),
                model_b.clone(),
                immediate_retry_config(1)
            )
            .await
            .is_err()
        );
        assert!(
            refill_once_with_test_executor_config(
                store.clone(),
                root.path(),
                &workspace,
                target_b,
                model_b.clone(),
                immediate_retry_config(1)
            )
            .await
            .is_err()
        );
        assert_eq!(model_b.calls(), 0);
        assert_eq!(
            store
                .find_thread_episodic_index_job(&claim.id)
                .await
                .unwrap()
                .unwrap(),
            exhausted
        );
        assert!(
            store
                .find_thread_episodic_index_job(&old.id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .list_thread_episodic_index_jobs_for_thread(&workspace, "reset_budget", 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn unresolved_provider_failure_preserves_partial_reset_identity_and_prefix_budget() {
        let (store, root, workspace) = setup_store().await;
        for index in 0..34 {
            ingest_materialized_user_item(
                store.clone(),
                &workspace,
                "checkpoint_thread",
                &format!("reset_turn_{index:02}"),
                &format!("reset_item_{index:02}"),
                &format!("checkpoint source {index}"),
            )
            .await;
        }
        let model_a = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![0.1; 3]));
        let target_a =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                model_a.as_ref(),
            )
            .unwrap();
        refill_once_with_test_executor_config(
            store.clone(),
            root.path(),
            &workspace,
            target_a,
            model_a,
            immediate_retry_config(1),
        )
        .await
        .unwrap();
        let model_b = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "vendor/model-b",
            vec![0.1; 4],
        ));
        let target_b =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                model_b.as_ref(),
            )
            .unwrap();
        mark_projection_reset(
            &store.database_connection(),
            &workspace,
            PROJECTION_META_STATUS_PENDING,
            &target_b,
        )
        .await
        .unwrap();
        // Production cleanup commits its first bounded reset page, then the
        // fixture rejects page two. No test SQL is executed outside this test.
        store.database_connection().execute_unprepared("CREATE TRIGGER interrupt_reset_suffix BEFORE UPDATE ON thread_episodic_items WHEN OLD.turn_id = 'reset_turn_32' BEGIN SELECT RAISE(ABORT, 'controlled reset interruption'); END").await.unwrap();
        assert!(
            cleanup_derived_artifacts(
                &store,
                chrono::Utc::now().timestamp(),
                root.path(),
                &workspace,
                pioneer_memory::lock_thread_episodic_workspace(&workspace).await
            )
            .await
            .is_err()
        );
        let key = projection_reset_checkpoint_key(&workspace).unwrap();
        let checkpoint = find_projection_meta(&store.database_connection(), &key)
            .await
            .unwrap()
            .unwrap();
        let progress: pioneer_crud::ThreadEpisodicProjectionResetProgress =
            serde_json::from_str(checkpoint.projection_config_json.as_deref().unwrap()).unwrap();
        assert!(progress.files_cleaned);
        assert_eq!(progress.after_source.as_ref().unwrap()[1], "reset_turn_31");
        let now = chrono::Utc::now().timestamp();
        let mut suffix = Vec::new();
        for source in store
            .list_thread_episodic_items_for_thread(&workspace, "checkpoint_thread", 100)
            .await
            .unwrap()
        {
            if source.turn_id.as_str() >= "reset_turn_32" {
                suffix.push(
                    store
                        .find_thread_episodic_index_job_by_item(&source.id)
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }
        }
        assert_eq!(suffix.len(), 2);
        let claims = store
            .claim_due_thread_episodic_index_jobs_for_workspace(&workspace, now, 34, 5)
            .await
            .unwrap()
            .assert_no_failures_for_test();
        assert_eq!(claims.len(), 32);
        let mut prefix = Vec::new();
        for claim in claims {
            let source = store
                .find_thread_episodic_item(&claim.index_item_id)
                .await
                .unwrap()
                .unwrap();
            if source.turn_id.as_str() < "reset_turn_32" {
                let terminal = source.turn_id == "reset_turn_00";
                store
                    .fail_thread_episodic_index_attempt_without_source_validation(
                        &claim.id,
                        claim.attempt_count,
                        ThreadEpisodicIndexJobFailureUpdate {
                            retryable: !terminal,
                            next_run_at_unix: (!terminal).then_some(now + 3600),
                            last_error: Some("used model B budget".to_owned()),
                            capacity_error: false,
                            last_attempt_latency_ms: None,
                        },
                        now,
                    )
                    .await
                    .unwrap();
                prefix.push(
                    store
                        .find_thread_episodic_index_job(&claim.id)
                        .await
                        .unwrap()
                        .unwrap(),
                );
            }
        }
        assert_eq!(prefix.len(), 32);
        let provisional =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &vector_search_config(
                    GatewayThreadEpisodicVectorProviderConfig::OpenRouter,
                    "vendor/model-b",
                    4,
                ),
            );
        assert_eq!(provisional.payload.dimension, None);
        assert!(
            refill_once_with_projection_resolver(
                store.clone(),
                root.path(),
                &workspace,
                provisional.clone(),
                None,
                None
            )
            .await
            .is_err()
        );
        let failed_checkpoint = find_projection_meta(&store.database_connection(), &key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            failed_checkpoint.projection_config_hash,
            checkpoint.projection_config_hash
        );
        assert_eq!(
            failed_checkpoint.projection_config_json,
            checkpoint.projection_config_json
        );
        assert_eq!(failed_checkpoint.status, PROJECTION_META_STATUS_PENDING);
        assert!(failed_checkpoint.last_error.is_some());
        store
            .database_connection()
            .execute_unprepared("DROP TRIGGER interrupt_reset_suffix")
            .await
            .unwrap();
        // Final resolution B resumes only the suffix. Its exhausted prefix
        // makes readiness fail, without renewing IDs, attempts or retry delay.
        assert!(
            refill_once_with_test_executor_config(
                store.clone(),
                root.path(),
                &workspace,
                provisional,
                model_b.clone(),
                immediate_retry_config(1)
            )
            .await
            .is_err()
        );
        assert_eq!(model_b.calls(), 2);
        for job in &prefix {
            assert_eq!(
                store
                    .find_thread_episodic_index_job(&job.id)
                    .await
                    .unwrap()
                    .unwrap(),
                *job
            );
        }
        for old in &suffix {
            assert!(
                store
                    .find_thread_episodic_index_job(&old.id)
                    .await
                    .unwrap()
                    .is_none()
            );
            let current = store
                .find_thread_episodic_index_job_by_item(&old.index_item_id)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(current.id, old.id);
            assert_eq!(current.status, ThreadEpisodicIndexJobStatus::Completed);
            assert_eq!(current.attempt_count, 1);
        }
        let resumed_checkpoint = find_projection_meta(&store.database_connection(), &key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            resumed_checkpoint.projection_config_hash,
            checkpoint.projection_config_hash
        );
        assert_eq!(resumed_checkpoint.status, PROJECTION_META_STATUS_COMPLETE);
        let final_progress: pioneer_crud::ThreadEpisodicProjectionResetProgress =
            serde_json::from_str(
                resumed_checkpoint
                    .projection_config_json
                    .as_deref()
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            final_progress.after_source.as_ref().unwrap()[1],
            "reset_turn_33"
        );
        // A genuinely different resolved identity gets a new reset/budget.
        let model_c = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "vendor/model-c",
            vec![0.1; 5],
        ));
        let target_c =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                model_c.as_ref(),
            )
            .unwrap();
        let complete = refill_once_with_test_executor_config(
            store.clone(),
            root.path(),
            &workspace,
            target_c,
            model_c.clone(),
            immediate_retry_config(1),
        )
        .await
        .unwrap();
        assert_eq!(complete.completed_jobs, 34);
        assert_eq!(model_c.calls(), 34);
        for old in prefix {
            assert!(
                store
                    .find_thread_episodic_index_job(&old.id)
                    .await
                    .unwrap()
                    .is_none()
            );
            let current = store
                .find_thread_episodic_index_job_by_item(&old.index_item_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(current.attempt_count, 1);
            assert_eq!(current.status, ThreadEpisodicIndexJobStatus::Completed);
        }
    }

    #[tokio::test]
    async fn failed_selection_preserves_ready_a_and_provider_free_return_without_history() {
        let (store, root, workspace) = setup_store().await;
        ingest_materialized_user_item(
            store.clone(),
            &workspace,
            "return_a_thread",
            "return_a_turn",
            "return_a_item",
            "ready A frame",
        )
        .await;
        let provider_a = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "vendor/a",
            vec![0.1; 3],
        ));
        let target_a =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                provider_a.as_ref(),
            )
            .unwrap();
        refill_once_with_workspace_projection(
            store.clone(),
            root.path(),
            &workspace,
            target_a.clone(),
            Some(provider_a.clone()),
        )
        .await
        .unwrap();
        let items = store
            .list_thread_episodic_items_for_thread(&workspace, "return_a_thread", 10)
            .await
            .unwrap();
        let jobs = store
            .list_thread_episodic_index_jobs_for_thread(&workspace, "return_a_thread", 10)
            .await
            .unwrap();
        let capsules = store
            .list_all_thread_episodic_capsules_for_workspace(&workspace)
            .await
            .unwrap();
        let file = capsules[0].storage_uri.strip_prefix("file://").unwrap();
        let bytes = std::fs::read(file).unwrap();
        let provider_b = StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "vendor/b",
            vec![0.2; 3],
        );
        let target_b =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                &provider_b,
            )
            .unwrap();
        assert!(
            refill_once_with_workspace_projection(
                store.clone(),
                root.path(),
                &workspace,
                target_b.clone(),
                None
            )
            .await
            .is_err()
        );
        assert!(
            !projection_reset_is_pending(&store, &workspace)
                .await
                .unwrap()
        );
        let marker = find_refill_projection_meta_for_workspace(&store, &workspace)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.status, PROJECTION_META_STATUS_COMPLETE);
        assert!(marker.last_error.is_some());
        assert!(
            refill_is_current_for_workspace_target(&store, &workspace, &target_a)
                .await
                .unwrap()
        );
        assert!(
            !refill_is_current_for_workspace_target(&store, &workspace, &target_b)
                .await
                .unwrap()
        );
        // The actual history view is unavailable; idle A must not resolve a
        // provider, prepare history, rewrite sources/jobs or touch the capsule.
        make_refill_history_unavailable(&store).await;
        let returned = refill_once_with_workspace_projection(
            store.clone(),
            root.path(),
            &workspace,
            target_a,
            None,
        )
        .await
        .unwrap();
        assert!(returned.skipped);
        assert_eq!(provider_a.calls(), 1);
        assert_eq!(
            store
                .list_thread_episodic_items_for_thread(&workspace, "return_a_thread", 10)
                .await
                .unwrap(),
            items
        );
        assert_eq!(
            store
                .list_thread_episodic_index_jobs_for_thread(&workspace, "return_a_thread", 10)
                .await
                .unwrap(),
            jobs
        );
        assert_eq!(
            store
                .list_all_thread_episodic_capsules_for_workspace(&workspace)
                .await
                .unwrap(),
            capsules
        );
        assert_eq!(std::fs::read(file).unwrap(), bytes);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_and_settings_completion_join_real_blocking_capsule_write() {
        use futures_util::poll;
        for entry in ["startup", "global_settings", "workspace_settings"] {
            let (store, root, workspace) = setup_store().await;
            ingest_materialized_user_item(
                store.clone(),
                &workspace,
                "wrapper_thread",
                "wrapper_turn",
                "wrapper_item",
                "wrapper source A",
            )
            .await;
            let supervisor =
                Arc::new(super::super::ThreadEpisodicWorkspaceRefillSupervisor::default());
            let registry = Arc::new(ProviderRegistry::new(|_| String::new()));
            let mut config = GatewayThreadEpisodicVectorSearchConfig::default();
            config.enabled = false;
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            REFILL_BLOCKING_TEST_GATES
                .lock()
                .unwrap()
                .insert(root.path().to_owned(), (started_tx, release_rx));
            let startup_cancellation = CancellationToken::new();
            let startup = if entry == "startup" {
                let store = store.clone();
                let root_path = root.path().to_owned();
                let registry = registry.clone();
                let supervisor = supervisor.clone();
                let config = config.clone();
                let cancellation = startup_cancellation.clone();
                Some(tokio::spawn(async move {
                    let result = super::super::run(
                        store,
                        true,
                        ThreadEpisodicIndexExecutorConfig::default(),
                        root_path.clone(),
                        config,
                        BTreeMap::new(),
                        registry,
                        root_path,
                        None,
                        supervisor,
                        cancellation.clone(),
                    )
                    .await;
                    assert!(cancellation.is_cancelled());
                    assert!(super::super::is_maintenance_cancelled(&result.unwrap_err()));
                }))
            } else {
                if entry == "global_settings" {
                    super::super::spawn_thread_episodic_workspace_capsule_refill(
                        store.clone(),
                        true,
                        ThreadEpisodicIndexExecutorConfig::default(),
                        root.path().to_owned(),
                        config.clone(),
                        BTreeMap::new(),
                        registry.clone(),
                        root.path().to_owned(),
                        None,
                        supervisor.clone(),
                    )
                    .await;
                } else {
                    super::super::spawn_thread_episodic_workspace_capsule_refill_for_workspace(
                        store.clone(),
                        true,
                        ThreadEpisodicIndexExecutorConfig::default(),
                        root.path().to_owned(),
                        workspace.clone(),
                        config.clone(),
                        config.clone(),
                        BTreeMap::new(),
                        registry.clone(),
                        root.path().to_owned(),
                        None,
                        supervisor.clone(),
                    )
                    .await;
                }
                None
            };
            started_rx.await.unwrap();
            let completed = supervisor
                .active
                .lock()
                .await
                .get(&workspace)
                .unwrap()
                .completed
                .clone();
            let old_job = store
                .list_due_thread_episodic_index_jobs_after(
                    chrono::Utc::now().timestamp(),
                    None,
                    None,
                    1,
                )
                .await
                .unwrap()
                .pop()
                .unwrap();
            assert_eq!(
                old_job.status,
                pioneer_crud::ThreadEpisodicIndexJobStatus::Running
            );
            assert!(!*completed.borrow());
            if entry == "workspace_settings" {
                // The next real generation cannot acquire its lease (and thus
                // cannot reset) while the old Native write is still running.
                let replacement = supervisor.begin_settings(&workspace);
                tokio::pin!(replacement);
                assert!(poll!(replacement.as_mut()).is_pending());
                tokio::task::yield_now().await;
                assert!(poll!(replacement.as_mut()).is_pending());
                assert!(!*completed.borrow());
                assert!(
                    pioneer_memory::try_lock_thread_episodic_workspace(&workspace)
                        .await
                        .is_none()
                );
                release_tx.send(()).unwrap();
                let lease = replacement.await;
                assert!(*completed.borrow());
                // A genuine replacement under this admitted lease gets a new
                // job identity; no old A write can run after the new capsule B.
                let provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
                    "openrouter",
                    "vendor/wrapper-B",
                    vec![0.1; 3],
                ));
                let target =
                    ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                        provider.as_ref(),
                    )
                    .unwrap();
                refill_once_with_workspace_projection(
                    store.clone(),
                    root.path(),
                    &workspace,
                    target,
                    Some(provider),
                )
                .await
                .unwrap();
                let replacement_job = store
                    .find_thread_episodic_index_job_by_item(&old_job.index_item_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_ne!(replacement_job.id, old_job.id);
                assert_eq!(
                    replacement_job.status,
                    pioneer_crud::ThreadEpisodicIndexJobStatus::Completed
                );
                drop(lease);
                supervisor.shutdown().await;
            } else {
                startup_cancellation.cancel();
                let shutdown = supervisor.shutdown();
                tokio::pin!(shutdown);
                assert!(poll!(shutdown.as_mut()).is_pending());
                tokio::task::yield_now().await;
                assert!(poll!(shutdown.as_mut()).is_pending());
                assert!(!*completed.borrow());
                assert!(
                    pioneer_memory::try_lock_thread_episodic_workspace(&workspace)
                        .await
                        .is_none()
                );
                release_tx.send(()).unwrap();
                shutdown.await;
                assert!(*completed.borrow());
            }
            if let Some(startup) = startup {
                startup.await.unwrap();
            }
            assert!(
                pioneer_memory::try_lock_thread_episodic_workspace(&workspace)
                    .await
                    .is_some()
            );
            assert!(
                REFILL_BLOCKING_TEST_GATES
                    .lock()
                    .unwrap()
                    .get(root.path())
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn workspace_wrapper_cancel_before_ownership_does_not_join_foreign_write_or_settle() {
        let (store, root, workspace) = setup_store().await;
        ingest_materialized_user_item(
            store.clone(),
            &workspace,
            "before_owner",
            "before_owner_turn",
            "before_owner_item",
            "before owner source",
        )
        .await;
        let owner = lock_thread_episodic_workspace(&workspace).await;
        let before = store
            .list_due_thread_episodic_index_jobs_after(
                chrono::Utc::now().timestamp(),
                None,
                None,
                1,
            )
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        let mut config = GatewayThreadEpisodicVectorSearchConfig::default();
        config.enabled = false;
        let work = run_workspace(
            store.clone(),
            true,
            ThreadEpisodicIndexExecutorConfig::default(),
            root.path().to_owned(),
            workspace.clone(),
            config.clone(),
            config,
            BTreeMap::new(),
            Arc::new(ProviderRegistry::new(|_| String::new())),
            root.path().to_owned(),
            None,
            cancellation.clone(),
        );
        tokio::pin!(work);
        assert!(futures_util::poll!(work.as_mut()).is_pending());
        cancellation.cancel();
        work.await;
        // Neither foreign ownership release nor a settlement write was needed.
        assert_eq!(
            store
                .list_due_thread_episodic_index_jobs_after(
                    chrono::Utc::now().timestamp(),
                    None,
                    None,
                    1
                )
                .await
                .unwrap(),
            before
        );
        assert_eq!(
            std::fs::read_dir(root.path())
                .unwrap()
                .filter(|entry| entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "mv2"))
                .count(),
            0
        );
        drop(owner);
    }

    #[tokio::test]
    async fn workspace_wrapper_cancel_drops_writer_admission_without_new_settlement() {
        let (store, _database_root, workspace) = setup_concurrent_store().await;
        let root = TempDir::new().unwrap();
        ingest_materialized_user_item(
            store.clone(),
            &workspace,
            "writer_cancel",
            "writer_cancel_turn",
            "writer_cancel_item",
            "writer cancel source",
        )
        .await;
        let jobs = store
            .list_due_thread_episodic_index_jobs_after(
                chrono::Utc::now().timestamp(),
                None,
                None,
                1,
            )
            .await
            .unwrap();
        let writer = store.database_connection().begin().await.unwrap();
        let (owned_tx, owned_rx) = tokio::sync::oneshot::channel();
        REFILL_OWNERSHIP_TEST_NOTICE
            .lock()
            .unwrap()
            .insert((root.path().to_owned(), true), owned_tx);
        let cancellation = CancellationToken::new();
        let mut config = GatewayThreadEpisodicVectorSearchConfig::default();
        config.enabled = false;
        let task = tokio::spawn(run_workspace(
            store.clone(),
            true,
            ThreadEpisodicIndexExecutorConfig::default(),
            root.path().to_owned(),
            workspace.clone(),
            config.clone(),
            config,
            BTreeMap::new(),
            Arc::new(ProviderRegistry::new(|_| String::new())),
            root.path().to_owned(),
            None,
            cancellation.clone(),
        ));
        owned_rx.await.unwrap();
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        cancellation.cancel();
        // The writer remains occupied until this join has returned. Mandatory
        // FS completion is already satisfied; no new DB cleanup can be admitted.
        task.await.unwrap();
        assert_eq!(
            store
                .list_due_thread_episodic_index_jobs_after(
                    chrono::Utc::now().timestamp(),
                    None,
                    None,
                    1
                )
                .await
                .unwrap(),
            jobs
        );
        assert!(
            pioneer_memory::try_lock_thread_episodic_workspace(&workspace)
                .await
                .is_some()
        );
        writer.rollback().await.unwrap();
        store
            .database_connection()
            .begin()
            .await
            .unwrap()
            .rollback()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn exhausted_running_is_terminal_for_startup_settings_and_ordinary_resume() {
        for entry in ["startup", "settings", "ordinary"] {
            let (store, root, workspace) = setup_store().await;
            ingest_materialized_user_item(
                store.clone(),
                &workspace,
                "ready_budget",
                "ready_budget_turn",
                "ready_budget_item",
                "ready budget A",
            )
            .await;
            refill_once_with_workspace_projection(
                store.clone(),
                root.path(),
                &workspace,
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
                None,
            )
            .await
            .unwrap();
            let capsule = store
                .list_thread_episodic_workspace_capsules(&workspace, 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
            let path = PathBuf::from(capsule.storage_uri.strip_prefix("file://").unwrap());
            let original_bytes = std::fs::read(&path).unwrap();
            ingest_materialized_user_item(
                store.clone(),
                &workspace,
                "exhausted_budget",
                "exhausted_budget_turn",
                "exhausted_budget_item",
                "exhausted source",
            )
            .await;
            let original = store
                .list_thread_episodic_index_jobs_for_thread(&workspace, "exhausted_budget", 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
            let now = chrono::Utc::now().timestamp() + 60;
            let max = 2;
            let policy = ThreadEpisodicIndexExecutorConfig {
                max_attempts: max,
                ..Default::default()
            };
            for attempt in 1..=max {
                let claimed = store
                    .claim_thread_episodic_index_job_if_due(&original.id, now, max)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(claimed.attempt_count, attempt);
                if attempt < max {
                    store
                        .requeue_thread_episodic_index_attempt(&claimed.id, attempt, now, None)
                        .await
                        .unwrap();
                }
            }
            let last = store
                .find_thread_episodic_index_job(&original.id)
                .await
                .unwrap()
                .unwrap();
            if entry == "ordinary" {
                let executor = crate::thread_episodic::ThreadEpisodicIndexExecutor::new(
                    store.clone(),
                    Arc::new(MemvidThreadEpisodicBackend::new()),
                    Arc::new(StoreThreadEpisodicIndexPayloadProvider::new(
                        store.clone(),
                        thread_episodic_storage_uri_from_path(root.path()),
                    )),
                );
                executor.apply_config(policy);
                let summary = executor.run_once(now).await.unwrap();
                assert_eq!(summary.claimed, 0);
                assert_eq!(summary.settled, 1);
                executor.shutdown().await;
            } else {
                let mut config = GatewayThreadEpisodicVectorSearchConfig::default();
                config.enabled = false;
                if entry == "startup" {
                    assert_eq!(
                        super::super::recover_inherited_thread_episodic_jobs(&store, max)
                            .await
                            .unwrap(),
                        1
                    );
                    run(
                        store.clone(),
                        true,
                        policy,
                        root.path().to_owned(),
                        config,
                        BTreeMap::new(),
                        Arc::new(ProviderRegistry::new(|_| String::new())),
                        root.path().to_owned(),
                        None,
                        Arc::new(super::super::ThreadEpisodicWorkspaceRefillSupervisor::default()),
                        super::super::RefillOwner::Startup,
                        CancellationToken::new(),
                    )
                    .await;
                } else {
                    run_workspace(
                        store.clone(),
                        true,
                        policy,
                        root.path().to_owned(),
                        workspace.clone(),
                        config.clone(),
                        config,
                        BTreeMap::new(),
                        Arc::new(ProviderRegistry::new(|_| String::new())),
                        root.path().to_owned(),
                        None,
                        CancellationToken::new(),
                    )
                    .await;
                }
            }
            let terminal = store
                .find_thread_episodic_index_job(&original.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(terminal.id, original.id);
            assert_eq!(terminal.attempt_count, max);
            assert_eq!(
                terminal.status,
                pioneer_crud::ThreadEpisodicIndexJobStatus::Canceled
            );
            assert_eq!(
                store
                    .find_thread_episodic_item(&original.index_item_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_crud::ThreadEpisodicItemStatus::Failed
            );
            assert_eq!(std::fs::read(&path).unwrap(), original_bytes);
            assert!(
                refill_is_current_for_workspace_target(
                    &store,
                    &workspace,
                    &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only()
                )
                .await
                .unwrap()
            );
            // Reopen/cancel/recovery of this same identity cannot renew its budget.
            assert_eq!(
                store
                    .requeue_running_thread_episodic_index_jobs_for_workspace(&workspace, now, max)
                    .await
                    .unwrap(),
                0
            );
            assert!(
                store
                    .claim_thread_episodic_index_job_if_due(&original.id, now, max)
                    .await
                    .unwrap()
                    .is_none()
            );
            let failure = ThreadEpisodicIndexJobFailureUpdate {
                retryable: true,
                next_run_at_unix: Some(now),
                last_error: Some("old callback".to_owned()),
                capacity_error: false,
                last_attempt_latency_ms: None,
            };
            assert_eq!(
                store
                    .fail_thread_episodic_index_attempt_without_source_validation(
                        &last.id,
                        last.attempt_count,
                        failure.clone(),
                        now
                    )
                    .await
                    .unwrap(),
                ThreadEpisodicIndexAttemptOutcome::StaleAttempt
            );
            let model_b = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
                "openrouter",
                "vendor/new-budget-B",
                vec![0.1; 3],
            ));
            let target_b =
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
                    model_b.as_ref(),
                )
                .unwrap();
            refill_once_with_workspace_projection(
                store.clone(),
                root.path(),
                &workspace,
                target_b,
                Some(model_b.clone()),
            )
            .await
            .unwrap();
            let replacement = store
                .find_thread_episodic_index_job_by_item(&original.index_item_id)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(replacement.id, original.id);
            assert_eq!(replacement.attempt_count, 1);
            assert_eq!(
                replacement.status,
                pioneer_crud::ThreadEpisodicIndexJobStatus::Completed
            );
            assert_eq!(model_b.calls(), 2);
            assert_eq!(
                store
                    .fail_thread_episodic_index_attempt_without_source_validation(
                        &last.id,
                        last.attempt_count,
                        failure,
                        now
                    )
                    .await
                    .unwrap(),
                ThreadEpisodicIndexAttemptOutcome::StaleAttempt
            );
            assert_eq!(
                store
                    .find_thread_episodic_index_job(&replacement.id)
                    .await
                    .unwrap()
                    .unwrap(),
                replacement
            );
        }
    }

    #[tokio::test]
    async fn refill_cancellation_reopen_spends_only_remaining_budget_and_confirms_durable_result() {
        use sea_orm::{ActiveModelTrait, EntityTrait, IntoActiveModel, Set};
        let (store, root, workspace) = setup_store().await;
        ingest_materialized_user_item(
            store.clone(),
            &workspace,
            "reopen_budget",
            "reopen_budget_turn",
            "reopen_budget_item",
            "reopen source",
        )
        .await;
        mark_refill_marker(
            &store,
            PROJECTION_META_STATUS_BACKFILLING,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
        )
        .await;
        let job = store
            .list_thread_episodic_index_jobs_for_thread(&workspace, "reopen_budget", 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let config = ThreadEpisodicIndexExecutorConfig {
            max_attempts: 2,
            ..Default::default()
        };
        for expected in 1..=2 {
            let (claimed_tx, claimed_rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn({
                let store = store.clone();
                let path = root.path().to_owned();
                let workspace = workspace.clone();
                async move {
                    refill_once_with_projection_resolver_and_config(
                        store,
                        &path,
                        &workspace,
                        ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
                        None,
                        None,
                        config,
                        None,
                        Some(claimed_tx),
                    )
                    .await
                }
            });
            claimed_rx.await.unwrap();
            let claimed = store
                .find_thread_episodic_index_job(&job.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(claimed.attempt_count, expected);
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
        assert!(
            refill_once_with_projection_resolver_and_config(
                store.clone(),
                root.path(),
                &workspace,
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
                None,
                None,
                config,
                None,
                None
            )
            .await
            .is_err()
        );
        let terminal = store
            .find_thread_episodic_index_job(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(terminal.attempt_count, 2);
        assert_eq!(
            terminal.status,
            pioneer_crud::ThreadEpisodicIndexJobStatus::Canceled
        );
        assert!(
            store
                .find_thread_episodic_item(&job.index_item_id)
                .await
                .unwrap()
                .unwrap()
                .frame_id
                .is_none()
        );

        // A separate same-identity source with one remaining attempt finishes
        // with its original ID. Its already persisted mapping is confirmed even
        // when a recovered Queued row has no remaining execution budget.
        let (store, root, workspace) = setup_store().await;
        ingest_materialized_user_item(
            store.clone(),
            &workspace,
            "remaining_budget",
            "remaining_turn",
            "remaining_item",
            "remaining source",
        )
        .await;
        mark_refill_marker(
            &store,
            PROJECTION_META_STATUS_BACKFILLING,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
        )
        .await;
        let job = store
            .list_thread_episodic_index_jobs_for_thread(&workspace, "remaining_budget", 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let now = chrono::Utc::now().timestamp() + 60;
        assert_eq!(
            store
                .claim_thread_episodic_index_job_if_due(&job.id, now, 2)
                .await
                .unwrap()
                .unwrap()
                .attempt_count,
            1
        );
        let finished = refill_once_with_projection_resolver_and_config(
            store.clone(),
            root.path(),
            &workspace,
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
            None,
            None,
            config,
            None,
            None,
        )
        .await
        .unwrap();
        assert!(finished.resumed);
        assert_eq!(finished.source_threads_reindexed, 0);
        let complete = store
            .find_thread_episodic_index_job(&job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(complete.id, job.id);
        assert_eq!(complete.attempt_count, 2);
        assert_eq!(
            complete.status,
            pioneer_crud::ThreadEpisodicIndexJobStatus::Completed
        );
        let item = store
            .find_thread_episodic_item(&job.index_item_id)
            .await
            .unwrap()
            .unwrap();
        let capsule = store
            .list_thread_episodic_workspace_capsules(&workspace, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let path = PathBuf::from(capsule.storage_uri.strip_prefix("file://").unwrap());
        let bytes = std::fs::read(&path).unwrap();
        for status in ["queued", "running"] {
            let mut row = pioneer_entity::thread_episodic_index_jobs::Entity::find_by_id(&job.id)
                .one(&store.database_connection())
                .await
                .unwrap()
                .unwrap()
                .into_active_model();
            row.status = Set(status.to_owned());
            row.update(&store.database_connection()).await.unwrap();
            if status == "queued" {
                assert!(
                    store
                        .claim_thread_episodic_index_job_if_due(&job.id, now, 2)
                        .await
                        .unwrap()
                        .is_none()
                );
            } else {
                let executor = crate::thread_episodic::ThreadEpisodicIndexExecutor::new(
                    store.clone(),
                    Arc::new(MemvidThreadEpisodicBackend::new()),
                    Arc::new(StoreThreadEpisodicIndexPayloadProvider::new(
                        store.clone(),
                        thread_episodic_storage_uri_from_path(root.path()),
                    )),
                );
                executor.apply_config(config);
                let summary = executor.run_once(now).await.unwrap();
                assert_eq!(summary.claimed, 0);
                assert_eq!(summary.settled, 1);
                executor.shutdown().await;
            }
            let confirmed = store
                .find_thread_episodic_index_job(&job.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                confirmed.status,
                pioneer_crud::ThreadEpisodicIndexJobStatus::Completed
            );
            assert_eq!(confirmed.attempt_count, 2);
            assert_eq!(
                store
                    .find_thread_episodic_item(&job.index_item_id)
                    .await
                    .unwrap()
                    .unwrap(),
                item
            );
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
        }
    }

    #[tokio::test]
    async fn refill_claim_partial_success_survives_poison_and_unknown_commit_without_replay() {
        use pioneer_crud::{ThreadEpisodicIndexJobStatus, ThreadEpisodicItemStatus};
        use sea_orm::{ActiveModelTrait, EntityTrait, IntoActiveModel, Set};
        for fault in [
            "storage",
            "lock",
            "panic_create",
            "panic_poll",
            "panic_commit",
            "commit_unknown",
            "retry_unknown",
            "exhaustion_rollback",
            "settlement_error",
        ] {
            let (store, root, workspace) = setup_store().await;
            let mut jobs = Vec::new();
            for name in ["claim_a", "claim_b", "claim_c"] {
                ingest_materialized_user_item(
                    store.clone(),
                    &workspace,
                    name,
                    &format!("{name}_turn"),
                    &format!("{name}_item"),
                    &format!("real refill source {name}"),
                )
                .await;
                jobs.push(
                    store
                        .list_thread_episodic_index_jobs_for_thread(&workspace, name, 1)
                        .await
                        .unwrap()
                        .pop()
                        .unwrap(),
                );
            }
            let now = chrono::Utc::now().timestamp();
            for (index, job) in jobs.iter_mut().enumerate() {
                let mut row =
                    pioneer_entity::thread_episodic_index_jobs::Entity::find_by_id(&job.id)
                        .one(&store.database_connection())
                        .await
                        .unwrap()
                        .unwrap()
                        .into_active_model();
                row.next_run_at = Set(fixed_datetime_from_unix(now - 3 + index as i64));
                row.update(&store.database_connection()).await.unwrap();
                *job = store
                    .find_thread_episodic_index_job(&job.id)
                    .await
                    .unwrap()
                    .unwrap();
            }
            let poison = &jobs[1];
            if fault == "exhaustion_rollback" {
                let claim = store
                    .claim_thread_episodic_index_job_if_due(&poison.id, now, 1)
                    .await
                    .unwrap()
                    .unwrap();
                store
                    .requeue_thread_episodic_index_attempt(&claim.id, 1, now - 2, None)
                    .await
                    .unwrap();
                store.database_connection().execute_unprepared(&format!(
                    "CREATE TRIGGER reject_exhausted_refill_source BEFORE UPDATE ON thread_episodic_items WHEN OLD.id = '{}' AND NEW.status = 'failed' BEGIN SELECT RAISE(ABORT, 'controlled exhaustion write-set failure'); END", poison.index_item_id,
                )).await.unwrap();
            } else {
                store.inject_episodic_claim_fault_for_test(
                    &poison.id,
                    if matches!(fault, "settlement_error" | "retry_unknown") {
                        "commit_unknown"
                    } else {
                        fault
                    },
                );
                if fault == "settlement_error" {
                    store.database_connection().execute_unprepared(&format!(
                        "CREATE TRIGGER reject_refill_poison_settlement BEFORE UPDATE ON thread_episodic_index_jobs WHEN OLD.id = '{}' AND NEW.status != 'running' BEGIN SELECT RAISE(ABORT, 'controlled fenced settlement failure'); END", poison.id,
                    )).await.unwrap();
                }
            }
            let poison_before = store
                .find_thread_episodic_index_job(&poison.id)
                .await
                .unwrap()
                .unwrap();
            let source_before = store
                .find_thread_episodic_item(&poison.index_item_id)
                .await
                .unwrap()
                .unwrap();
            mark_refill_marker(
                &store,
                PROJECTION_META_STATUS_BACKFILLING,
                THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            )
            .await;
            // Prepared resume must not turn a local claim error into discovery
            // of history or recreate the already confirmed A attempt.
            make_refill_history_unavailable(&store).await;
            let discoveries = store.episodic_claim_discoveries_for_test();
            let result = refill_once_with_projection_resolver_and_config(
                store.clone(),
                root.path(),
                &workspace,
                ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
                None,
                None,
                ThreadEpisodicIndexExecutorConfig {
                    max_attempts: if fault == "retry_unknown" { 2 } else { 1 },
                    ..Default::default()
                },
                None,
                None,
            )
            .await;
            assert!(result.is_err(), "poison claim must remain visible");
            assert_eq!(
                store.episodic_claim_discoveries_for_test() - discoveries,
                1,
                "the selected portion must never replay, including a lock error"
            );
            for job in [&jobs[0], &jobs[2]] {
                let current = store
                    .find_thread_episodic_index_job(&job.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(current.id, job.id);
                assert_eq!(current.status, ThreadEpisodicIndexJobStatus::Completed);
                assert_eq!(current.attempt_count, 1);
                assert_eq!(
                    store
                        .find_thread_episodic_item(&job.index_item_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    ThreadEpisodicItemStatus::Active
                );
            }
            let capsule_row = store
                .list_thread_episodic_workspace_capsules(&workspace, 1)
                .await
                .unwrap()
                .pop()
                .unwrap();
            let path = PathBuf::from(capsule_row.storage_uri.strip_prefix("file://").unwrap());
            let bytes = std::fs::read(&path).unwrap();
            let capsule = memvid_core::Memvid::open_read_only(&path).unwrap();
            for job in [&jobs[0], &jobs[2]] {
                let source = store
                    .find_thread_episodic_item(&job.index_item_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    capsule
                        .frame_by_uri(source.frame_uri.as_deref().unwrap())
                        .is_ok()
                );
            }
            let poison_uri = pioneer_crud::thread_episodic_frame_uri(
                &capsule_row.capsule_ref,
                &poison.index_item_id,
            )
            .unwrap();
            assert!(
                capsule.frame_by_uri(&poison_uri).is_err(),
                "unknown B commit is never a dispatch receipt"
            );
            drop(capsule);
            let current = store
                .find_thread_episodic_index_job(&poison.id)
                .await
                .unwrap()
                .unwrap();
            let mut deferred_input = poison_before.clone();
            deferred_input.next_run_at = current.next_run_at;
            deferred_input.updated_at = current.updated_at;
            deferred_input.last_error = current.last_error.clone();
            if fault == "exhaustion_rollback" {
                assert_eq!(current, deferred_input);
                assert_eq!(current.status, ThreadEpisodicIndexJobStatus::Queued);
                assert_eq!(current.attempt_count, 1);
                assert_eq!(current.last_error.as_deref(), Some("claim_storage"));
                assert_eq!(
                    current.next_run_at.timestamp(),
                    current.updated_at.timestamp() + 60
                );
                assert_eq!(
                    store
                        .find_thread_episodic_item(&poison.index_item_id)
                        .await
                        .unwrap()
                        .unwrap(),
                    source_before
                );
                store
                    .database_connection()
                    .execute_unprepared("DROP TRIGGER reject_exhausted_refill_source")
                    .await
                    .unwrap();
            } else if fault == "settlement_error" {
                // Keep the failure installed until independent A/C progress and
                // capacity have been checked. Only B remains an owned obligation.
                assert_eq!(current.status, ThreadEpisodicIndexJobStatus::Running);
                assert_eq!(current.attempt_count, 1);
                store
                    .database_connection()
                    .begin()
                    .await
                    .unwrap()
                    .rollback()
                    .await
                    .unwrap();
                store
                    .database_connection()
                    .execute_unprepared("DROP TRIGGER reject_refill_poison_settlement")
                    .await
                    .unwrap();
            } else if fault == "retry_unknown" {
                assert_eq!(current.status, ThreadEpisodicIndexJobStatus::Failed);
                assert_eq!(current.attempt_count, 1);
                assert_eq!(
                    current.next_run_at.timestamp(),
                    current.updated_at.timestamp() + 30
                );
            } else if matches!(fault, "commit_unknown" | "panic_commit") {
                assert_eq!(current.status, ThreadEpisodicIndexJobStatus::Canceled);
                assert_eq!(current.attempt_count, 1);
            } else {
                assert_eq!(current, deferred_input);
                assert_eq!(current.attempt_count, 0);
                assert_eq!(current.status, ThreadEpisodicIndexJobStatus::Queued);
                assert_eq!(
                    current.next_run_at.timestamp(),
                    current.updated_at.timestamp() + 30
                );
                assert_eq!(
                    current.last_error.as_deref(),
                    Some(if fault.starts_with("panic") {
                        "claim_unwind"
                    } else {
                        "claim_storage"
                    })
                );
                assert_eq!(
                    store
                        .find_thread_episodic_item(&poison.index_item_id)
                        .await
                        .unwrap()
                        .unwrap(),
                    source_before
                );
            }
            if matches!(fault, "exhaustion_rollback" | "settlement_error") {
                // Make the saved deadline due for the next controlled quantum;
                // no sleep or new execution budget is used to organize recovery.
                if fault == "exhaustion_rollback" {
                    let mut row =
                        pioneer_entity::thread_episodic_index_jobs::Entity::find_by_id(&poison.id)
                            .one(&store.database_connection())
                            .await
                            .unwrap()
                            .unwrap()
                            .into_active_model();
                    row.next_run_at = Set(fixed_datetime_from_unix(chrono::Utc::now().timestamp()));
                    row.update(&store.database_connection()).await.unwrap();
                }
                assert!(
                    refill_once_with_projection_resolver_and_config(
                        store.clone(),
                        root.path(),
                        &workspace,
                        ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
                        None,
                        None,
                        ThreadEpisodicIndexExecutorConfig {
                            max_attempts: 1,
                            ..Default::default()
                        },
                        None,
                        None,
                    )
                    .await
                    .is_err()
                );
                let terminal = store
                    .find_thread_episodic_index_job(&poison.id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(terminal.status, ThreadEpisodicIndexJobStatus::Canceled);
                assert_eq!(terminal.attempt_count, 1);
                assert_eq!(std::fs::read(&path).unwrap(), bytes);
                for job in [&jobs[0], &jobs[2]] {
                    assert_eq!(
                        store
                            .find_thread_episodic_index_job(&job.id)
                            .await
                            .unwrap()
                            .unwrap()
                            .attempt_count,
                        1
                    );
                }
            }
            assert!(
                pioneer_memory::try_lock_thread_episodic_workspace(&workspace)
                    .await
                    .is_some()
            );
        }
    }

    async fn setup_store() -> (Arc<CrudStore>, TempDir, String) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        bootstrap(&connection)
            .await
            .expect("gateway bootstrap should create default workspace");
        // Ownership is process-wide, so independent databases need independent
        // workspace identities too. No source rows exist at this point.
        pioneer_entity::workspace::Entity::delete_many()
            .exec(&connection)
            .await
            .unwrap();
        let workspace_manager = WorkspaceManager::new(connection.clone());
        let workspace_id = workspace_manager
            .create_workspace(&pioneer_protocol::generate_id(), Some("Refill test"))
            .await
            .expect("isolated workspace should exist")
            .id;
        (
            Arc::new(CrudStore::new(connection)),
            TempDir::new().expect("temp dir"),
            workspace_id,
        )
    }

    pub(crate) async fn make_refill_history_unavailable(store: &CrudStore) {
        let db = store.database_connection();
        let object = db
            .query_one_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT type FROM sqlite_schema WHERE name = ?",
                ["turn_event".into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "type")
            .unwrap();
        // Compression exposes a view; an ordinary in-memory schema has a
        // table. Keep its rows and FK targets while denying history queries.
        let statement = match object.as_str() {
            "table" => "ALTER TABLE turn_event RENAME TO forbidden_refill_history",
            "view" => "DROP VIEW turn_event",
            other => panic!("unexpected history object: {other}"),
        };
        db.execute_unprepared(statement).await.unwrap();
    }

    async fn mark_refill_marker(crud_store: &CrudStore, status: &str, version: i64) {
        mark_refill_marker_with_target(
            crud_store,
            status,
            version,
            &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
        )
        .await;
    }

    async fn mark_refill_marker_with_target(
        crud_store: &CrudStore,
        status: &str,
        version: i64,
        projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    ) {
        let workspace_id = crud_store
            .list_thread_episodic_refill_workspace_ids()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        mark_refill_marker_with_projection_key(
            crud_store,
            refill_projection_key_for_workspace(&workspace_id).unwrap(),
            status,
            version,
            projection_target,
        )
        .await;
    }

    async fn mark_refill_marker_with_workspace_target(
        crud_store: &CrudStore,
        workspace_id: &str,
        status: &str,
        version: i64,
        projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    ) {
        mark_refill_marker_with_projection_key(
            crud_store,
            refill_projection_key_for_workspace(workspace_id)
                .expect("workspace refill key should build"),
            status,
            version,
            projection_target,
        )
        .await;
    }

    async fn mark_refill_marker_with_projection_key(
        crud_store: &CrudStore,
        projection_key: String,
        status: &str,
        version: i64,
        projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    ) {
        let now = now_datetime();
        upsert_projection_meta_with_config(
            &crud_store.database_connection(),
            ProjectionMetaRecord {
                projection_key,
                projection_version: version,
                status: status.to_owned(),
                source_thread_count: 0,
                source_turn_count: 0,
                source_turn_item_count: 0,
                source_turn_event_count: 0,
                last_error: None,
                backfill_started_at: Some(now),
                backfilled_at: (status == PROJECTION_META_STATUS_COMPLETE).then_some(now),
                created_at: now,
                updated_at: now,
            },
            projection_target.meta_config_record(),
        )
        .await
        .expect("marker should upsert");
    }

    fn assert_manual_smoke_path_is_not_production(path: &Path, label: &str) {
        if std::env::var_os("PIONEER_THREAD_EPISODIC_VECTOR_REFILL_SMOKE_ALLOW_ANY_PATH").is_some()
        {
            return;
        }

        let normalized = path.to_string_lossy().replace('\\', "/");
        assert!(
            !normalized.ends_with("/.pioneer/gateway.db")
                && !normalized.contains("/.pioneer/memory"),
            "{label} must point to an isolated copy, not production: {}",
            path.display()
        );
        assert!(
            normalized.contains("/.worktrees/")
                || normalized.contains("/.scratch/")
                || normalized.contains("/tmp/")
                || normalized.contains("/var/folders/"),
            "{label} must live in a worktree/scratch/temp path unless PIONEER_THREAD_EPISODIC_VECTOR_REFILL_SMOKE_ALLOW_ANY_PATH=1 is set: {}",
            path.display()
        );
    }

    fn vector_search_config(
        provider: GatewayThreadEpisodicVectorProviderConfig,
        model: &str,
        _legacy_dimension: u32,
    ) -> GatewayThreadEpisodicVectorSearchConfig {
        GatewayThreadEpisodicVectorSearchConfig {
            enabled: true,
            provider: Some(provider),
            model: Some(model.to_owned()),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            embedding_normalized: true,
            use_search_instructions: false,
        }
    }

    fn expected_source_text_hash(text: &str) -> String {
        hex::encode(Sha256::digest(text.trim().as_bytes()))
    }

    async fn assert_capsule_contains_payload(
        crud_store: &CrudStore,
        item: &pioneer_crud::ThreadEpisodicItemRecord,
        expected_text: &str,
    ) {
        let capsule_id = item
            .capsule_id
            .as_deref()
            .expect("indexed item should carry capsule id");
        let capsule = crud_store
            .find_thread_episodic_capsule(capsule_id)
            .await
            .expect("capsule lookup should succeed")
            .expect("indexed capsule should exist");
        let output = MemvidThreadEpisodicBackend::new()
            .search(ThreadEpisodicMemvidSearchRequest {
                workspace_id: item.workspace_id.clone(),
                thread_id: item.thread_id.clone(),
                query: expected_text.to_owned(),
                scope: None,
                profile: ThreadEpisodicSearchProfile::for_kind(
                    ThreadEpisodicSearchProfileKind::ExactReference,
                ),
                segments: vec![ThreadEpisodicMemvidSearchSegment {
                    capsule_id: capsule.id,
                    capsule_ref: capsule.capsule_ref,
                    storage_uri: capsule.storage_uri,
                    segment_index: capsule.segment_index,
                }],
                exact_source: Some(ThreadEpisodicExactSourceTarget {
                    turn_id: Some(item.turn_id.clone()),
                    item_id: Some(item.item_id.clone()),
                    index_item_id: Some(item.id.clone()),
                }),
            })
            .await
            .expect("capsule payload should be searchable");
        let hit = output
            .hits
            .iter()
            .find(|hit| hit.hit.index_item_id == item.id)
            .expect("capsule should contain the repaired item");
        let rendered_metadata = hit
            .hit
            .text
            .strip_prefix(expected_text)
            .expect("capsule document should start with the canonical source payload");
        assert!(
            rendered_metadata.starts_with("\ntitle: "),
            "capsule source payload should be followed by rendered frame metadata"
        );
    }

    async fn ingest_materialized_user_item(
        crud_store: Arc<CrudStore>,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        text: &str,
    ) {
        let item = TurnItem::UserMessage {
            id: item_id.to_owned(),
            text: text.to_owned(),
            attachments: Vec::new(),
        };
        materialize_thread_with_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            item.clone(),
            1_700_000_000,
        )
        .await;
        let ingestor = StoreThreadEpisodicIngestor::new(crud_store);
        let outcome = ingestor
            .ingest_committed_item(ThreadEpisodicCommittedItem {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item_id: item_id.to_owned(),
                item_type: TurnItemType::UserMessage,
                source_actor_role: Some(ProtocolThreadEpisodicSourceActorRole::User),
                source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
                item,
            })
            .await
            .expect("ingestion should succeed");
        assert!(matches!(
            outcome,
            crate::thread_episodic::ThreadEpisodicIngestionOutcome::Accepted
        ));
    }

    async fn materialize_thread_with_item(
        crud_store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item: TurnItem,
        timestamp: i64,
    ) {
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "test-model".to_owned(),
            model_provider: "test-provider".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::Completed,
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
        crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &Vec::<UserInput>::new(),
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("turn start should materialize");
        crud_store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item,
                },
                timestamp + 1,
            )
            .await
            .expect("item completed should materialize");
    }
}
