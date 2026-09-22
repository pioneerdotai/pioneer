use crate::thread_episodic::{
    ConfigBackedThreadEpisodicIndexEmbeddingProviderResolver,
    RuntimeVectorThreadEpisodicIndexPayloadProvider, StoreThreadEpisodicIndexPayloadProvider,
    StoreThreadEpisodicIngestor, ThreadEpisodicIndexEmbeddingProviderResolver,
    ThreadEpisodicIndexExecutorConfig, ThreadEpisodicIndexPayloadProvider,
    ThreadEpisodicIndexResolutionError, ThreadEpisodicIndexResolutionFailureKind,
    ThreadEpisodicResolvedIndexRequest, ThreadEpisodicThreadReindexRequest,
    memvid_stats_reach_capacity_threshold,
};
use crate::thread_episodic_embedding::{
    CHUNKED_EMBEDDING_INPUT_ERROR_MARKER, provider_embedding_error_message_is_retryable,
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
    ThreadEpisodicItemIndexedUpdate, find_projection_meta, list_projection_meta_by_key_prefix,
    update_projection_meta_status, upsert_projection_meta_with_config,
};
use pioneer_memory::{
    MemvidThreadEpisodicBackend, ThreadEpisodicEmbeddingProvider, ThreadEpisodicMemvidBackend,
    ThreadEpisodicMemvidFailureKind, ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidStats,
    thread_episodic_storage_uri_from_path,
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
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub(crate) const THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY: &str =
    "thread_episodic_workspace_capsule_refill";
pub(crate) const THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION: i64 = 1;
const THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_CONFIG_VERSION: u32 = 1;

const REFILL_ENQUEUE_BATCH_SIZE: u64 = 1024;
const REFILL_EXECUTOR_MAX_BATCHES: u64 = 100_000;
const REFILL_JOB_CLAIM_LIMIT: u64 = 1;
const REFILL_RECOVERY_SCAN_LIMIT: u64 = 100_000;
const REFILL_LOCK_FILE_NAME: &str = ".thread_episodic_workspace_capsule_refill.lock";
const REFILL_INDEX_ERROR_MAX_CHARS: usize = 512;
const LEGACY_REFILL_WORKSPACE_ID: &str = "__default__";
const LEGACY_INVALID_SKETCH_TRACK_ERROR: &str =
    "Sketch track is invalid: Invalid sketch track magic";

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
        let payload = ThreadEpisodicWorkspaceCapsuleRefillProjectionPayload {
            schema_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_CONFIG_VERSION,
            vector_search_enabled,
            provider,
            model,
            dimension,
            normalized,
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
    pub(crate) index_jobs_deleted: u64,
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
    pub(crate) incomplete_jobs: u64,
    pub(crate) interrupted_jobs_requeued: u64,
    pub(crate) legacy_retryable_jobs_requeued: usize,
}

pub(super) async fn run(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    workspace_vector_search_configs: BTreeMap<String, GatewayThreadEpisodicVectorSearchConfig>,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    refill_supervisor: Arc<super::ThreadEpisodicWorkspaceRefillSupervisor>,
    interrupted_before_unix: Option<i64>,
    owner: super::RefillOwner,
    supervisor_cancellation: CancellationToken,
) {
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
            thread_episodic_storage_root.clone(),
            workspace_id,
            workspace_vector_search_config,
            vector_search_config.clone(),
            workspace_vector_search_configs.clone(),
            provider_registry.clone(),
            runtime_home.clone(),
            refill_status_sender.clone(),
            interrupted_before_unix,
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
    thread_episodic_storage_root: PathBuf,
    workspace_id: String,
    workspace_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    default_vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    workspace_vector_search_configs: BTreeMap<String, GatewayThreadEpisodicVectorSearchConfig>,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    interrupted_before_unix: Option<i64>,
    cancellation: CancellationToken,
) {
    let crud_store = Arc::new(crud_store.with_maintenance_access());
    if cancellation.is_cancelled() {
        return;
    }
    if workspace_vector_search_config.enabled
        && !workspace_vector_search_config.has_selected_embedding_model()
    {
        return;
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
    let refill = tokio::select! {
        _ = cancellation.cancelled() => return,
        refill = refill_once_with_projection_resolver(
            crud_store,
            thread_episodic_storage_root.as_path(),
            workspace_id.as_str(),
            projection_target,
            embedding_provider_resolver,
            refill_status_sender.as_ref(),
            interrupted_before_unix,
        ) => refill,
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
                index_jobs_deleted = summary.index_jobs_deleted,
                thread_directory_rows_deleted = summary.thread_directory_rows_deleted,
                workspace_id = %workspace_id,
                workspace_count = summary.workspace_count,
                source_threads_reindexed = summary.source_threads_reindexed,
                source_threads_failed = summary.source_threads_failed,
                refill_jobs_enqueued = summary.refill_jobs_enqueued,
                executor_batches = summary.executor_batches,
                completed_jobs = summary.completed_jobs,
                interrupted_jobs_requeued = summary.interrupted_jobs_requeued,
                legacy_retryable_jobs_requeued = summary.legacy_retryable_jobs_requeued,
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
        Some(chrono::Utc::now().timestamp()),
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
    interrupted_before_unix: Option<i64>,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    refill_once_with_projection_resolver_and_config(
        crud_store,
        thread_episodic_storage_root,
        workspace_id,
        projection_target,
        embedding_provider_resolver,
        refill_status_sender,
        ThreadEpisodicIndexExecutorConfig::default(),
        interrupted_before_unix,
    )
    .await
}

async fn refill_once_with_projection_resolver_and_config(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    workspace_id: &str,
    mut projection_target: ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
    mut embedding_provider_resolver: Option<Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>>,
    refill_status_sender: Option<&ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
    executor_config: ThreadEpisodicIndexExecutorConfig,
    interrupted_before_unix: Option<i64>,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let db = crud_store.database_connection();
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

    if refill_is_current_for_workspace_target(crud_store.as_ref(), workspace_id, &projection_target)
        .await?
    {
        return Ok(ThreadEpisodicWorkspaceCapsuleRefillSummary {
            skipped: true,
            ..Default::default()
        });
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

    if refill_is_current_for_workspace_target(crud_store.as_ref(), workspace_id, &projection_target)
        .await?
    {
        return Ok(ThreadEpisodicWorkspaceCapsuleRefillSummary {
            skipped: true,
            ..Default::default()
        });
    }

    let existing_meta =
        find_refill_projection_meta_for_workspace(crud_store.as_ref(), workspace_id).await?;
    let resume_existing = refill_can_resume_existing_projection(
        crud_store.as_ref(),
        workspace_id,
        existing_meta.as_ref(),
        &projection_target,
    )
    .await?;

    notify_refill_status(
        refill_status_sender,
        workspace_id,
        GatewayThreadEpisodicVectorRefillStatus::Running,
    );

    if let Err(error) = preflight_refill_embedding_resolver(
        workspace_id,
        &projection_target,
        embedding_provider_resolver.as_ref(),
    )
    .await
    {
        if resume_existing {
            mark_refill_failed(&db, workspace_id, &error, &projection_target).await?;
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
        prepare_resumed_refill(
            crud_store.clone(),
            workspace_id,
            existing_meta.as_ref(),
            executor_config,
            interrupted_before_unix,
        )
        .await
    } else {
        mark_refill_preparing(&db, workspace_id, &projection_target).await?;
        prepare_fresh_refill(
            crud_store.clone(),
            thread_episodic_storage_root,
            workspace_id,
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

    mark_refill_backfilling(&db, workspace_id, &summary, &projection_target).await?;
    let result = execute_refill_jobs(
        crud_store,
        thread_episodic_storage_root,
        workspace_id,
        embedding_provider_resolver,
        executor_config,
        &mut summary,
    )
    .await;

    match result {
        Ok(()) => {
            mark_refill_complete(&db, workspace_id, &summary, &projection_target).await?;
            notify_refill_status(
                refill_status_sender,
                workspace_id,
                GatewayThreadEpisodicVectorRefillStatus::Complete,
            );
            Ok(summary)
        }
        Err(error) => {
            mark_refill_failed(&db, workspace_id, &error, &projection_target).await?;
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
            return Ok(true);
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

    Ok(projection_meta_is_current_for_target(
        &meta,
        projection_target,
    ))
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

    if meta.projection_version != THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION {
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
    if workspace_id == LEGACY_REFILL_WORKSPACE_ID {
        return Ok(None);
    }
    find_projection_meta(&db, THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY).await
}

async fn refill_can_resume_existing_projection(
    crud_store: &CrudStore,
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
            PROJECTION_META_STATUS_BACKFILLING | PROJECTION_META_STATUS_FAILED
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

    let incomplete = crud_store
        .count_incomplete_thread_episodic_index_jobs_for_workspace(workspace_id)
        .await
        .context("failed to count resumable thread episodic refill jobs")?;
    if incomplete > 0 {
        return Ok(true);
    }
    let canceled = crud_store
        .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id)
        .await
        .context("failed to count terminal thread episodic refill jobs")?;
    if canceled > 0 || meta.source_turn_item_count > 0 {
        return Ok(true);
    }
    crud_store
        .thread_episodic_index_job_exists_for_workspace(workspace_id)
        .await
        .context("failed to check durable thread episodic refill jobs")
}

async fn prepare_fresh_refill(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    workspace_id: &str,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let now_unix = chrono::Utc::now().timestamp();
    let mut summary = cleanup_derived_artifacts(
        crud_store.as_ref(),
        now_unix,
        thread_episodic_storage_root,
        workspace_id,
    )
    .await?;

    rebuild_refill_items_from_history(crud_store.clone(), workspace_id, now_unix, &mut summary)
        .await?;
    populate_refill_source_counts(crud_store.as_ref(), workspace_id, &mut summary).await?;
    enqueue_refill_jobs(crud_store.as_ref(), workspace_id, now_unix, &mut summary).await?;
    Ok(summary)
}

async fn prepare_resumed_refill(
    crud_store: Arc<CrudStore>,
    workspace_id: &str,
    existing_meta: Option<&pioneer_entity::thread_timeline_projection_meta::Model>,
    config: ThreadEpisodicIndexExecutorConfig,
    interrupted_before_unix: Option<i64>,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let now_unix = chrono::Utc::now().timestamp();
    let mut summary = ThreadEpisodicWorkspaceCapsuleRefillSummary {
        resumed: true,
        ..Default::default()
    };
    rebuild_refill_items_from_history(crud_store.clone(), workspace_id, now_unix, &mut summary)
        .await?;
    populate_refill_source_counts(crud_store.as_ref(), workspace_id, &mut summary).await?;
    if let Some(existing_meta) = existing_meta {
        summary.source_thread_count = summary
            .source_thread_count
            .max(existing_meta.source_thread_count);
        summary.source_turn_count = summary
            .source_turn_count
            .max(existing_meta.source_turn_count);
        summary.source_turn_item_count = summary
            .source_turn_item_count
            .max(existing_meta.source_turn_item_count);
    }
    if let Some(interrupted_before_unix) = interrupted_before_unix {
        summary.interrupted_jobs_requeued = crud_store
            .requeue_running_thread_episodic_index_jobs_for_workspace(
                workspace_id,
                interrupted_before_unix,
                now_unix,
            )
            .await
            .context("failed to requeue interrupted thread episodic refill jobs")?;
    }
    summary.legacy_retryable_jobs_requeued =
        recover_misclassified_retryable_jobs(crud_store.as_ref(), workspace_id, now_unix, config)
            .await?;
    enqueue_refill_jobs(crud_store.as_ref(), workspace_id, now_unix, &mut summary).await?;
    Ok(summary)
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

async fn recover_misclassified_retryable_jobs(
    crud_store: &CrudStore,
    workspace_id: &str,
    now_unix: i64,
    config: ThreadEpisodicIndexExecutorConfig,
) -> Result<usize> {
    let jobs = crud_store
        .list_canceled_thread_episodic_index_jobs_for_workspace(
            workspace_id,
            REFILL_RECOVERY_SCAN_LIMIT,
        )
        .await
        .context("failed to list canceled thread episodic refill jobs")?;
    let mut recovered = 0usize;
    for job in jobs {
        let last_error = job.last_error.as_deref().unwrap_or_default();
        let legacy_invalid_sketch_track = job.attempt_count < config.max_attempts
            && last_error == LEGACY_INVALID_SKETCH_TRACK_ERROR;
        let exhausted_before_bounded_input_support = job.attempt_count == config.max_attempts
            && last_error.contains("missing field `data`")
            && !last_error.contains(CHUNKED_EMBEDDING_INPUT_ERROR_MARKER);
        let legacy_provider_failure = (job.attempt_count < config.max_attempts
            || exhausted_before_bounded_input_support)
            && provider_embedding_error_message_is_retryable(last_error);
        let retryable = legacy_invalid_sketch_track || legacy_provider_failure;
        if !retryable {
            continue;
        }
        if crud_store
            .requeue_canceled_thread_episodic_index_job(job.id.as_str(), now_unix)
            .await
            .with_context(|| {
                format!(
                    "failed to recover misclassified thread episodic refill job `{}`",
                    job.id
                )
            })?
            .is_some()
        {
            recovered = recovered.saturating_add(1);
        }
    }
    Ok(recovered)
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
    _now_unix: i64,
    _thread_episodic_storage_root: &Path,
    workspace_id: &str,
) -> Result<ThreadEpisodicWorkspaceCapsuleRefillSummary> {
    let capsules = crud_store
        .list_all_thread_episodic_capsules_for_workspace(workspace_id)
        .await
        .context("failed to list thread episodic capsules before workspace refill")?;
    let mut summary = ThreadEpisodicWorkspaceCapsuleRefillSummary::default();
    for capsule in capsules {
        match delete_capsule_file(capsule.storage_uri.as_str()).await? {
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
    summary.item_rows_deleted = crud_store
        .delete_rebuildable_thread_episodic_items_for_workspace(workspace_id)
        .await
        .context("failed to delete rebuildable thread episodic item rows")?;
    summary.index_jobs_deleted = crud_store
        .delete_thread_episodic_index_jobs_for_workspace(workspace_id)
        .await
        .context("failed to delete stale thread episodic index jobs")?;
    summary.thread_directory_rows_deleted = crud_store
        .delete_thread_episodic_thread_directory_entries_for_workspace(workspace_id)
        .await
        .context("failed to delete stale thread episodic thread directory rows")?;
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
    let ingestor = StoreThreadEpisodicIngestor::with_config(crud_store, true);
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
                warn!(
                    error = %format!("{error:#}"),
                    "thread episodic workspace refill skipped one source thread"
                );
            }
        }
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

async fn execute_refill_jobs(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    workspace_id: &str,
    embedding_provider_resolver: Option<Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>>,
    config: ThreadEpisodicIndexExecutorConfig,
    summary: &mut ThreadEpisodicWorkspaceCapsuleRefillSummary,
) -> Result<()> {
    let storage_uri_root = thread_episodic_storage_uri_from_path(thread_episodic_storage_root);
    let backend = MemvidThreadEpisodicBackend::new();
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
        let jobs = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id,
                now_unix,
                REFILL_JOB_CLAIM_LIMIT,
            )
            .await
            .context("failed to claim thread episodic workspace refill index jobs")?;
        if jobs.is_empty() {
            let canceled_jobs = crud_store
                .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id)
                .await
                .context("failed to count terminal thread episodic refill jobs")?;
            if canceled_jobs > 0 {
                summary.failed_terminal_jobs = usize::try_from(canceled_jobs).unwrap_or(usize::MAX);
                bail!(
                    "thread episodic workspace refill failed {} index jobs terminally",
                    canceled_jobs
                );
            }

            summary.incomplete_jobs = crud_store
                .count_incomplete_thread_episodic_index_jobs_for_workspace(workspace_id)
                .await
                .context("failed to count incomplete thread episodic index jobs")?;
            if summary.incomplete_jobs == 0 {
                return Ok(());
            }

            let next_run_at = crud_store
                .next_scheduled_thread_episodic_index_job_at_for_workspace(workspace_id)
                .await
                .context("failed to find the next scheduled thread episodic refill job")?;
            let Some(next_run_at) = next_run_at else {
                bail!(
                    "thread episodic workspace refill stalled with {} incomplete index jobs",
                    summary.incomplete_jobs
                );
            };
            let delay_secs = next_run_at.saturating_sub(chrono::Utc::now().timestamp());
            if delay_secs > 0 {
                tokio::time::sleep(Duration::from_secs(delay_secs as u64)).await;
            } else {
                tokio::task::yield_now().await;
            }
            continue;
        }

        summary.executor_batches = summary.executor_batches.saturating_add(1);
        execute_claimed_refill_batch(
            crud_store.clone(),
            &backend,
            payload_provider.as_ref(),
            jobs,
            now_unix,
            config,
            summary,
            #[cfg(test)]
            None,
        )
        .await?;
    }

    summary.incomplete_jobs = crud_store
        .count_incomplete_thread_episodic_index_jobs_for_workspace(workspace_id)
        .await
        .context("failed to count incomplete thread episodic index jobs")?;
    let canceled_jobs = crud_store
        .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id)
        .await
        .context("failed to count terminal thread episodic refill jobs")?;
    if canceled_jobs > 0 {
        summary.failed_terminal_jobs = usize::try_from(canceled_jobs).unwrap_or(usize::MAX);
        bail!(
            "thread episodic workspace refill failed {} index jobs terminally",
            canceled_jobs
        );
    }
    if summary.incomplete_jobs > 0 {
        bail!(
            "thread episodic workspace refill reached its executor limit with {} incomplete index jobs",
            summary.incomplete_jobs
        );
    }

    Ok(())
}

async fn execute_claimed_refill_batch(
    crud_store: Arc<CrudStore>,
    backend: &MemvidThreadEpisodicBackend,
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
    backend: &MemvidThreadEpisodicBackend,
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
    backend: &MemvidThreadEpisodicBackend,
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

async fn delete_capsule_file(storage_uri: &str) -> Result<CapsuleFileDeleteOutcome> {
    let Some(path) = storage_uri.strip_prefix("file://") else {
        return Ok(CapsuleFileDeleteOutcome::NonFileUri);
    };
    match tokio::fs::remove_file(path).await {
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
        ThreadEpisodicThreadReindexRequest,
    };
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
    use pioneer_entity::{
        thread_episodic_exclusions, thread_episodic_index_jobs, thread_episodic_items,
    };
    use pioneer_memory::ThreadEpisodicEmbeddingError;
    use pioneer_protocol::{
        ItemCompletedNotification, ItemUpdatedNotification, SandboxMode, TaskExecutorKind,
        TaskStatus, TaskTriggerKind, TaskTurnItem, Thread,
        ThreadEpisodicSourceActorRole as ProtocolThreadEpisodicSourceActorRole,
        ThreadEpisodicSourceContext, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, Turn, TurnItem, TurnItemType, TurnKind, TurnOrigin, TurnStatus, UserInput,
    };
    use pioneer_sqlite::SqliteDatabase;
    use sea_orm::{
        ActiveModelTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend, EntityTrait,
        IntoActiveModel, Set, Statement,
    };
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

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
            Some(chrono::Utc::now().timestamp().saturating_add(2)),
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

        let summary = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("contended refill should skip without error");

        assert!(summary.skipped);
        assert!(summary.lock_contended);
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
            let (crud_store, temp_dir, _workspace_id) = setup_store().await;
            mark_refill_marker(crud_store.as_ref(), status, version).await;

            let summary = refill_once(crud_store.clone(), temp_dir.path())
                .await
                .expect("non-current marker should retry");

            assert!(!summary.skipped, "status={status} version={version}");
            let meta = find_projection_meta(
                &crud_store.database_connection(),
                THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY,
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

        let summary = cleanup_derived_artifacts(
            crud_store.as_ref(),
            1_700_000_010,
            temp_dir.path(),
            workspace_id.as_str(),
        )
        .await
        .expect("cleanup should succeed");

        assert_eq!(summary.capsule_rows_deleted, 2);
        assert_eq!(summary.capsule_files_deleted, 1);
        assert_eq!(summary.capsule_files_missing, 1);
        assert_eq!(summary.item_rows_deleted, 1);
        assert_eq!(summary.exclusion_rows_deleted, 0);
        assert_eq!(summary.index_jobs_deleted, 1);
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
                .is_none()
        );
        assert!(
            crud_store
                .find_thread_episodic_index_job(job.id.as_str())
                .await
                .expect("job lookup succeeds")
                .is_none()
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
        )
        .await
        .expect("cleanup should be rerunnable");
        assert_eq!(second.capsule_rows_deleted, 0);
        assert_eq!(second.item_rows_deleted, 0);
        assert_eq!(second.exclusion_rows_deleted, 0);
        assert_eq!(second.index_jobs_deleted, 0);
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
                .contains("thread episodic workspace refill failed 1 index jobs terminally"),
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
                .contains("thread episodic workspace refill failed 1 index jobs terminally"),
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
            )
            .await
            .expect("refill job should claim")
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
        assert_eq!(outcome, ThreadEpisodicIndexAttemptOutcome::SourceChanged);
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
            )
            .await
            .expect("retry job should claim")
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
            )
            .await
            .expect("terminal job should claim")
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
            )
            .await
            .expect("unpersisted job should claim")
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
            )
            .await
            .expect("old claim should requeue for stale-attempt coverage");
        let new_claim = failed_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                failed_workspace.as_str(),
                failed_now.saturating_add(2),
                1,
            )
            .await
            .expect("new attempt should claim")
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
            .claim_due_thread_episodic_index_jobs_for_workspace(workspace_id.as_str(), now_unix, 2)
            .await
            .expect("two refill jobs should claim");
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
            )
            .await
            .expect("requeued job should claim again")
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
    async fn thread_episodic_vector_refill_restart_resumes_legacy_transient_failures() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill_resume";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_resume_a",
            "item_vector_refill_resume_a",
            "first vector should remain intact across refill restart",
        )
        .await;
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_resume_b",
            "item_vector_refill_resume_b",
            "second vector should resume after a transient response failure",
        )
        .await;

        let legacy_provider = Arc::new(ScriptedThreadEpisodicEmbeddingProvider::new(vec![
            Ok(vec![0.1, 0.2, 0.3]),
            Err(
                ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                    "openai",
                    "text-embedding-3-small",
                    "error decoding response body: request or response body error: operation timed out",
                ),
            ),
        ]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            legacy_provider.as_ref(),
        )
        .expect("test provider target");

        refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            legacy_provider.clone(),
            immediate_retry_config(5),
        )
        .await
        .expect_err("legacy transient classification should leave a failed marker");
        assert_eq!(legacy_provider.calls(), 2);

        let before_items = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("items should list before resume");
        assert_eq!(before_items.len(), 2);
        let retained_item = before_items
            .iter()
            .find(|item| item.status == ThreadEpisodicItemStatus::Active)
            .expect("one item should already be indexed");
        let retained_item_id = retained_item.id.clone();
        let retained_frame_uri = retained_item
            .frame_uri
            .clone()
            .expect("completed item should retain its frame URI");
        assert_eq!(
            before_items
                .iter()
                .filter(|item| item.status == ThreadEpisodicItemStatus::Failed)
                .count(),
            1
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
        .expect("same projection should resume the failed refill");

        assert!(summary.resumed);
        assert_eq!(summary.legacy_retryable_jobs_requeued, 1);
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(summary.capsule_files_deleted, 0);
        assert_eq!(summary.capsule_rows_deleted, 0);
        assert_eq!(summary.item_rows_deleted, 0);
        assert_eq!(summary.index_jobs_deleted, 0);
        assert_eq!(resume_provider.calls(), 1);

        let retained_item = crud_store
            .find_thread_episodic_item(retained_item_id.as_str())
            .await
            .expect("retained item lookup should succeed")
            .expect("retained item should still exist");
        assert_eq!(
            retained_item.frame_uri.as_deref(),
            Some(retained_frame_uri.as_str())
        );
        let jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("jobs should list after resume");
        assert_eq!(jobs.len(), 2);
        assert!(
            jobs.iter()
                .all(|job| job.status == ThreadEpisodicIndexJobStatus::Completed)
        );
        assert!(
            refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &target
            )
            .await
            .expect("resumed marker should be current")
        );
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_restart_recovers_legacy_invalid_sketch_track() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill_legacy_sketch";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_legacy_sketch_a",
            "item_vector_refill_legacy_sketch_a",
            "completed frame must survive legacy sketch recovery",
        )
        .await;
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_legacy_sketch_b",
            "item_vector_refill_legacy_sketch_b",
            "legacy sketch failure should resume without rebuild",
        )
        .await;

        let legacy_provider = Arc::new(ScriptedThreadEpisodicEmbeddingProvider::new(vec![
            Ok(vec![0.1, 0.2, 0.3]),
            Err(
                ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                    "openai",
                    "text-embedding-3-small",
                    LEGACY_INVALID_SKETCH_TRACK_ERROR,
                ),
            ),
        ]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            legacy_provider.as_ref(),
        )
        .expect("test provider target");

        refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            legacy_provider,
            immediate_retry_config(5),
        )
        .await
        .expect_err("legacy sketch classification should leave a failed marker");

        let retained_item = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("items should list before recovery")
            .into_iter()
            .find(|item| item.status == ThreadEpisodicItemStatus::Active)
            .expect("one item should already be indexed");
        let retained_item_id = retained_item.id;
        let retained_frame_uri = retained_item
            .frame_uri
            .expect("completed item should retain its frame URI");

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
        .expect("legacy sketch failure should resume");

        assert!(summary.resumed);
        assert_eq!(summary.legacy_retryable_jobs_requeued, 1);
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(summary.capsule_files_deleted, 0);
        assert_eq!(summary.capsule_rows_deleted, 0);
        assert_eq!(summary.item_rows_deleted, 0);
        assert_eq!(summary.index_jobs_deleted, 0);
        assert_eq!(resume_provider.calls(), 1);

        let retained_item = crud_store
            .find_thread_episodic_item(retained_item_id.as_str())
            .await
            .expect("retained item lookup should succeed")
            .expect("retained item should still exist");
        assert_eq!(
            retained_item.frame_uri.as_deref(),
            Some(retained_frame_uri.as_str())
        );
        let jobs = crud_store
            .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("jobs should list after recovery");
        assert_eq!(jobs.len(), 2);
        assert!(
            jobs.iter()
                .all(|job| job.status == ThreadEpisodicIndexJobStatus::Completed)
        );
        assert!(
            refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &target
            )
            .await
            .expect("recovered marker should be current")
        );
    }

    #[tokio::test]
    async fn thread_episodic_vector_refill_recovers_exhausted_pre_chunking_response_failure_once() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            "thread_vector_refill_pre_chunking",
            "turn_vector_refill_pre_chunking",
            "item_vector_refill_pre_chunking",
            "oversized source retained in full while its bounded embedding is retried",
        )
        .await;
        let legacy_provider = Arc::new(ScriptedThreadEpisodicEmbeddingProvider::new(vec![Err(
            ThreadEpisodicEmbeddingError::non_retryable_provider_failure(
                "openrouter",
                "qwen/qwen3-embedding-8b",
                "error decoding response body: missing field `data`",
            ),
        )]));
        let target = ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_embedding_provider(
            legacy_provider.as_ref(),
        )
        .expect("test provider target");

        refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            legacy_provider,
            immediate_retry_config(1),
        )
        .await
        .expect_err("pre-chunking response failure should exhaust the old retry budget");

        let resume_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
        let summary = refill_once_with_test_executor_config(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            target.clone(),
            resume_provider.clone(),
            immediate_retry_config(1),
        )
        .await
        .expect("bounded-input release should get one recovery attempt");

        assert!(summary.resumed);
        assert_eq!(summary.legacy_retryable_jobs_requeued, 1);
        assert_eq!(summary.completed_jobs, 1);
        assert_eq!(resume_provider.calls(), 1);
        assert!(
            refill_is_current_for_workspace_target(
                crud_store.as_ref(),
                workspace_id.as_str(),
                &target
            )
            .await
            .expect("recovered marker should be current")
        );
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
        assert_eq!(summary.index_jobs_deleted, 0);
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
            )
            .await
            .expect("job should be claimed before simulated restart");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].status, ThreadEpisodicIndexJobStatus::Running);

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
        assert_eq!(summary.interrupted_jobs_requeued, 1);
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
        let startup_cutoff = chrono::Utc::now().timestamp();
        let claimed_at = startup_cutoff.saturating_add(1);
        let claimed = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id.as_str(),
                claimed_at,
                1,
            )
            .await
            .expect("current process job should be claimed");
        assert_eq!(claimed.len(), 1);

        let summary = prepare_resumed_refill(
            crud_store.clone(),
            workspace_id.as_str(),
            None,
            immediate_retry_config(5),
            Some(startup_cutoff),
        )
        .await
        .expect("resume preparation should preserve a current process lease");

        assert_eq!(summary.interrupted_jobs_requeued, 0);
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
        let (crud_store, temp_dir, _workspace_id) = setup_store().await;
        let config = vector_search_config(
            GatewayThreadEpisodicVectorProviderConfig::OpenAi,
            "text-embedding-3-small",
            3,
        );
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));
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

        let summary = refill_once_for_vector_search_config(
            crud_store,
            temp_dir.path(),
            &config,
            Some(embedding_provider.clone()),
        )
        .await
        .expect("same vector config should skip");

        assert!(summary.skipped);
        assert_eq!(embedding_provider.calls(), 0);
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
        assert_eq!(summary.incomplete_jobs, 0);
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
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY,
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
    async fn thread_episodic_failed_refill_resumes_seven_hash_mismatches_idempotently() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_resume_seven_hash_mismatches";
        let mut canonical_updates = Vec::new();
        for index in 0..7 {
            let turn_id = format!("turn_resume_task_{index}");
            let item_id = format!("item_resume_task_{index}");
            let make_item = |status| TurnItem::Task {
                item: TaskTurnItem {
                    id: item_id.clone(),
                    task_id: format!("task_resume_{index}"),
                    created_by_turn_id: None,
                    run_id: Some(format!("run_resume_{index}")),
                    parent_task_id: None,
                    root_task_id: None,
                    title: format!("Resume task {index}"),
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
                    result_preview: Some(format!("Resume preview {index}")),
                    error_preview: None,
                    started_at: Some(1_700_040_000),
                    created_at: 1_700_040_000,
                    updated_at: 1_700_040_001,
                },
            };
            materialize_thread_with_item(
                crud_store.as_ref(),
                workspace_id.as_str(),
                thread_id,
                turn_id.as_str(),
                make_item(if index == 6 {
                    TaskStatus::Scheduled
                } else {
                    TaskStatus::Running
                }),
                1_700_040_000 + index,
            )
            .await;
            canonical_updates.push((turn_id, make_item(TaskStatus::Completed)));
        }
        StoreThreadEpisodicIngestor::new(crud_store.clone())
            .reindex_thread_from_history(ThreadEpisodicThreadReindexRequest {
                workspace_id: workspace_id.clone(),
                thread_id: thread_id.to_owned(),
                history_event_limit: None,
                item_scan_limit: 100,
                now_unix: 1_700_040_020,
            })
            .await
            .expect("historical projections should prepare");
        for (index, (turn_id, item)) in canonical_updates.into_iter().enumerate() {
            crud_store
                .materialize_item_snapshot_updated(
                    ItemUpdatedNotification {
                        workspace_id: workspace_id.clone(),
                        thread_id: thread_id.to_owned(),
                        turn_id,
                        item,
                    },
                    1_700_040_100 + index as i64,
                )
                .await
                .expect("canonical completed task should update");
        }
        let claimed = crud_store
            .claim_due_thread_episodic_index_jobs_for_workspace(
                workspace_id.as_str(),
                1_700_040_200,
                20,
            )
            .await
            .expect("stale jobs should claim");
        assert_eq!(claimed.len(), 7);
        for job in &claimed {
            let outcome = crud_store
                .fail_thread_episodic_index_attempt_without_source_validation(
                    job.id.as_str(),
                    job.attempt_count,
                    ThreadEpisodicIndexJobFailureUpdate {
                        retryable: false,
                        next_run_at_unix: None,
                        last_error: Some(
                            "thread episodic source text hash changed before indexing".to_owned(),
                        ),
                        capacity_error: false,
                        last_attempt_latency_ms: Some(1),
                    },
                    1_700_040_201,
                )
                .await
                .expect("terminal hash mismatch should persist");
            assert_eq!(outcome, ThreadEpisodicIndexAttemptOutcome::Applied);
        }
        mark_refill_marker_with_workspace_target(
            crud_store.as_ref(),
            workspace_id.as_str(),
            PROJECTION_META_STATUS_FAILED,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
        )
        .await;

        let resumed = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("failed refill should resume without row cleanup");
        assert!(resumed.resumed);
        let jobs_after_resume = crud_store
            .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 30)
            .await
            .expect("jobs should list after resume");
        assert_eq!(
            jobs_after_resume
                .iter()
                .filter(|job| job.status == ThreadEpisodicIndexJobStatus::Completed)
                .count(),
            7
        );
        assert_eq!(
            crud_store
                .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id.as_str())
                .await
                .expect("only blocking canceled jobs should count"),
            0
        );
        let job_count = jobs_after_resume.len();
        let repeated = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("successful refill should be idempotent");
        assert!(repeated.skipped);
        assert_eq!(
            crud_store
                .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 30,)
                .await
                .expect("jobs should list after repeat")
                .len(),
            job_count
        );
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
        assert!(
            crud_store
                .list_thread_episodic_index_jobs_for_thread(workspace_id.as_str(), thread_id, 10,)
                .await
                .expect("control jobs should list")
                .is_empty()
        );
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
            )
            .await
            .expect("old job should claim")
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
        let fixture_db = crud_store.database_connection();
        thread_episodic_exclusions::ActiveModel {
            id: Set("legacy_refill_exclusion".to_owned()),
            workspace_id: Set(workspace_id.clone()),
            thread_id: Set(thread_id.to_owned()),
            index_item_id: Set(item.id.clone()),
            reason: Set("user_requested".to_owned()),
            created_by: Set("legacy-fixture".to_owned()),
            created_at: Set(fixed_datetime_from_unix(claim_time.saturating_add(2))),
        }
        .insert(&fixture_db)
        .await
        .expect(
            "legacy exclusion row should be inserted without applying current lifecycle repair",
        );
        assert_eq!(
            crud_store
                .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id.as_str())
                .await
                .expect("legacy terminal count should succeed"),
            1,
            "the fixture must begin with an exclusion row and an unreconciled terminal reason"
        );
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
            )
            .await
            .expect("independent job should claim")
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
    async fn thread_episodic_resume_repairs_preexisting_legacy_deleted_job() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_resume_legacy_deleted";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_resume_legacy_deleted",
            "item_resume_legacy_deleted",
            "legacy user deletion remains authoritative",
        )
        .await;
        let item = crud_store
            .list_thread_episodic_items_for_thread(workspace_id.as_str(), thread_id, 10)
            .await
            .expect("legacy deleted item should list")
            .pop()
            .expect("legacy deleted item should exist");
        let job = crud_store
            .find_thread_episodic_index_job_by_item(item.id.as_str())
            .await
            .expect("legacy deleted job lookup should succeed")
            .expect("legacy deleted job should exist");
        let fixture_time = chrono::Utc::now().timestamp().saturating_add(20);
        let fixture_db = crud_store.database_connection();
        let item_row = thread_episodic_items::Entity::find_by_id(item.id.clone())
            .one(&fixture_db)
            .await
            .expect("legacy deleted item row lookup should succeed")
            .expect("legacy deleted item row should exist");
        let mut deleted_item = item_row.into_active_model();
        deleted_item.status = Set("deleted".to_owned());
        deleted_item.deleted_at = Set(Some(fixed_datetime_from_unix(fixture_time)));
        deleted_item.updated_at = Set(fixed_datetime_from_unix(fixture_time));
        deleted_item
            .update(&fixture_db)
            .await
            .expect("legacy deleted item state should be seeded directly");
        let job_row = thread_episodic_index_jobs::Entity::find_by_id(job.id.clone())
            .one(&fixture_db)
            .await
            .expect("legacy deleted job row lookup should succeed")
            .expect("legacy deleted job row should exist");
        let mut legacy_job = job_row.into_active_model();
        legacy_job.status = Set("canceled".to_owned());
        legacy_job.last_error = Set(Some(
            "legacy provider failure before tombstone repair".to_owned(),
        ));
        legacy_job.completed_at = Set(Some(fixed_datetime_from_unix(fixture_time)));
        legacy_job.updated_at = Set(fixed_datetime_from_unix(fixture_time));
        legacy_job
            .update(&fixture_db)
            .await
            .expect("legacy deleted job state should be seeded directly");
        assert_eq!(
            crud_store
                .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id.as_str())
                .await
                .expect("legacy deletion terminal count should succeed"),
            1
        );
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
            .expect("resume should repair a durable legacy user-deletion outcome");
        let repaired_item = crud_store
            .find_thread_episodic_item(item.id.as_str())
            .await
            .expect("repaired deleted item lookup should succeed")
            .expect("repaired deleted item should remain");
        assert_eq!(repaired_item.status, ThreadEpisodicItemStatus::Deleted);
        let repaired_job = crud_store
            .find_thread_episodic_index_job(job.id.as_str())
            .await
            .expect("repaired deleted job lookup should succeed")
            .expect("repaired deleted job should remain");
        assert_eq!(repaired_job.status, ThreadEpisodicIndexJobStatus::Canceled);
        assert_eq!(
            repaired_job.last_error.as_deref(),
            Some(pioneer_crud::THREAD_EPISODIC_USER_DELETED_ERROR)
        );
        assert_eq!(
            crud_store
                .count_canceled_thread_episodic_index_jobs_for_workspace(workspace_id.as_str())
                .await
                .expect("repaired deletion should not block refill"),
            0
        );

        mark_existing_refill_failed(crud_store.as_ref(), workspace_id.as_str()).await;
        let repeated = refill_once(crud_store.clone(), temp_dir.path())
            .await
            .expect("repaired legacy deletion should remain idempotent");
        assert_eq!(repeated.refill_jobs_enqueued, 0);
        let repeated_job = crud_store
            .find_thread_episodic_index_job(job.id.as_str())
            .await
            .expect("repeated deleted job lookup should succeed")
            .expect("repeated deleted job should remain");
        assert_eq!(repeated_job.attempt_count, repaired_job.attempt_count);
    }

    #[tokio::test]
    async fn thread_episodic_workspace_refill_deletes_orphan_derived_items_before_rebuild() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let orphan_item = crud_store
            .upsert_thread_episodic_item(
                NewThreadEpisodicItemRecord {
                    id: None,
                    workspace_id: workspace_id.clone(),
                    thread_id: "thread_missing_source".to_owned(),
                    turn_id: "turn_missing_source".to_owned(),
                    item_id: "item_missing_source".to_owned(),
                    source_actor_role: ThreadEpisodicSourceActorRole::User,
                    source_runtime_kind: ThreadEpisodicSourceRuntimeKind::UserTurn,
                    source_context: ThreadEpisodicSourceContext::UserVisibleThreadItem,
                    visibility: ThreadEpisodicItemVisibility::UserVisible,
                    status: ThreadEpisodicItemStatus::PendingIndex,
                    text_hash: "a".repeat(64),
                    source_text_hash: "b".repeat(64),
                    projection_group_id: "projection_group_orphan".to_owned(),
                    language_hint: None,
                    token_estimate: 1,
                    capsule_id: None,
                    capsule_ref: None,
                    segment_index: None,
                    frame_id: None,
                    frame_uri: None,
                    indexed_at: None,
                    deleted_at: None,
                },
                1_700_000_000,
            )
            .await
            .expect("item should insert");

        let summary = refill_once_with_workspace_projection(
            crud_store.clone(),
            temp_dir.path(),
            workspace_id.as_str(),
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::lexical_only(),
            None,
        )
        .await
        .expect("orphan derived item should be deleted before refill");
        assert_eq!(summary.item_rows_deleted, 1);
        assert_eq!(summary.refill_jobs_enqueued, 0);
        assert!(
            crud_store
                .find_thread_episodic_item(orphan_item.id.as_str())
                .await
                .expect("orphan item lookup should succeed")
                .is_none()
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
        assert_eq!(meta.status, PROJECTION_META_STATUS_COMPLETE);
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
        let workspace_manager = WorkspaceManager::new(connection.clone());
        let workspace_id = workspace_manager
            .list_workspaces()
            .await
            .expect("workspace list should succeed")
            .into_iter()
            .find(|workspace| workspace.is_active && workspace.is_current)
            .expect("current workspace should exist")
            .id;
        (
            Arc::new(CrudStore::new(connection)),
            TempDir::new().expect("temp dir"),
            workspace_id,
        )
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
        let workspace_id = WorkspaceManager::new(writer.clone())
            .list_workspaces()
            .await
            .expect("workspace list should succeed")
            .into_iter()
            .find(|workspace| workspace.is_active && workspace.is_current)
            .expect("current workspace should exist")
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
        mark_refill_marker_with_projection_key(
            crud_store,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_KEY.to_owned(),
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
