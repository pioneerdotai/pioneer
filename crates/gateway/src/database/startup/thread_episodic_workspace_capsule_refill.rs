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
    CrudStore, NewThreadEpisodicThreadDirectoryRecord, PROJECTION_META_STATUS_BACKFILLING,
    PROJECTION_META_STATUS_COMPLETE, PROJECTION_META_STATUS_FAILED, ProjectionMetaConfigRecord,
    ProjectionMetaRecord, ThreadEpisodicCapsuleCapacityUpdate, ThreadEpisodicCapsuleWriteState,
    ThreadEpisodicIndexJobCompletionUpdate, ThreadEpisodicIndexJobFailureUpdate,
    ThreadEpisodicIndexJobRecord, ThreadEpisodicItemIndexedUpdate,
    ThreadEpisodicThreadDirectoryStatus, ThreadEpisodicThreadDirectoryVisibility,
    find_projection_meta, upsert_projection_meta_with_config,
};
use pioneer_memory::{
    MemvidThreadEpisodicBackend, ThreadEpisodicEmbeddingProvider, ThreadEpisodicMemvidBackend,
    ThreadEpisodicMemvidFailureKind, ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidStats,
    thread_episodic_storage_uri_from_path,
};
use pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus;
use pioneer_provider::ProviderRegistry;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
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
        let config_hash = crate::settings::thread_episodic_vector_projection_identity_hash(config);
        let payload = ThreadEpisodicWorkspaceCapsuleRefillProjectionPayload {
            schema_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_CONFIG_VERSION,
            vector_search_enabled: config.enabled,
            provider: config.enabled.then(|| {
                config
                    .provider
                    .map(crate::settings::vector_provider_identity_name)
                    .unwrap_or("missing")
                    .to_owned()
            }),
            model: config.enabled.then(|| projection_embedding_model(config)),
            dimension: config
                .enabled
                .then(|| crate::settings::resolved_vector_embedding_dimension(config))
                .flatten(),
            normalized: config.enabled.then_some(config.embedding_normalized),
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

    fn requires_embedding_provider(&self) -> bool {
        self.payload.vector_search_enabled
    }

    fn matches_embedding_provider(&self, provider: &dyn ThreadEpisodicEmbeddingProvider) -> bool {
        if !self.payload.vector_search_enabled {
            return true;
        }
        self.payload.provider.as_deref() == Some(provider.provider_id())
            && self.payload.model.as_deref() == Some(provider.model())
            && self
                .payload
                .dimension
                .is_some_and(|dimension| usize::try_from(dimension) == Ok(provider.dimension()))
            && self.payload.normalized == Some(provider.normalized())
    }
}

struct ProjectionMetaRecordLike<'a> {
    projection_config_hash: Option<&'a str>,
    projection_config_json: Option<&'a str>,
}

fn projection_embedding_model(config: &GatewayThreadEpisodicVectorSearchConfig) -> String {
    match config.provider {
        Some(GatewayThreadEpisodicVectorProviderConfig::Local) => config
            .model
            .as_deref()
            .or(config.local_model.as_deref())
            .map(str::trim)
            .unwrap_or("")
            .to_owned(),
        Some(
            GatewayThreadEpisodicVectorProviderConfig::OpenAi
            | GatewayThreadEpisodicVectorProviderConfig::OpenRouter,
        ) => config
            .model
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .to_owned(),
        None => String::new(),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub(crate) struct ThreadEpisodicWorkspaceCapsuleRefillSummary {
    pub(crate) skipped: bool,
    pub(crate) lock_contended: bool,
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
}

pub(super) async fn run(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: PathBuf,
    vector_search_config: GatewayThreadEpisodicVectorSearchConfig,
    workspace_vector_search_configs: BTreeMap<String, GatewayThreadEpisodicVectorSearchConfig>,
    provider_registry: Arc<ProviderRegistry>,
    runtime_home: PathBuf,
    refill_status_sender: Option<ThreadEpisodicWorkspaceCapsuleRefillStatusSender>,
) {
    let workspace_ids = match crud_store.list_thread_episodic_refill_workspace_ids().await {
        Ok(workspace_ids) => workspace_ids,
        Err(error) => {
            warn!(
                error = %format!("{error:#}"),
                "thread episodic workspace capsule refill failed to list source workspaces"
            );
            return;
        }
    };
    for workspace_id in workspace_ids {
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
        )
        .await;
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
) {
    if !ensure_local_embedding_model_ready_for_workspace_refill(
        runtime_home.as_path(),
        workspace_id.as_str(),
        &workspace_vector_search_config,
    )
    .await
    {
        return;
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
    match refill_once_with_projection_resolver(
        crud_store,
        thread_episodic_storage_root.as_path(),
        workspace_id.as_str(),
        projection_target,
        embedding_provider_resolver,
        refill_status_sender.as_ref(),
    )
    .await
    {
        Ok(summary) if summary.skipped && summary.lock_contended => {
            info!(
                "thread episodic workspace capsule refill skipped because another process holds the refill lock"
            );
        }
        Ok(summary) if summary.skipped => {}
        Ok(summary) => {
            info!(
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
) -> bool {
    match crate::thread_episodic_embedding::ensure_local_embedding_model_downloaded_if_needed(
        runtime_home,
        config,
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
    let db = crud_store.database_connection();
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

    let started_at = now_datetime();
    let projection_key = refill_projection_key_for_workspace(workspace_id)?;

    upsert_projection_meta_with_config(
        &db,
        ProjectionMetaRecord {
            projection_key: projection_key.clone(),
            projection_version: THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            status: PROJECTION_META_STATUS_BACKFILLING.to_owned(),
            source_thread_count: 0,
            source_turn_count: 0,
            source_turn_item_count: 0,
            source_turn_event_count: 0,
            last_error: None,
            backfill_started_at: Some(started_at),
            backfilled_at: None,
            created_at: started_at,
            updated_at: started_at,
        },
        projection_target.meta_config_record(),
    )
    .await?;

    notify_refill_status(
        refill_status_sender,
        workspace_id,
        GatewayThreadEpisodicVectorRefillStatus::Running,
    );

    let result = async {
        preflight_refill_embedding_resolver(
            workspace_id,
            &projection_target,
            embedding_provider_resolver.as_ref(),
        )
        .await?;

        refill_after_marker(
            crud_store.clone(),
            thread_episodic_storage_root,
            workspace_id,
            embedding_provider_resolver,
        )
        .await
    }
    .await;

    match result {
        Ok(summary) => {
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
    if refill_is_current_for_workspace_target(
        crud_store,
        LEGACY_REFILL_WORKSPACE_ID,
        projection_target,
    )
    .await?
    {
        return Ok(true);
    }
    for workspace_id in crud_store
        .list_thread_episodic_refill_workspace_ids()
        .await?
    {
        if refill_is_current_for_workspace_target(
            crud_store,
            workspace_id.as_str(),
            projection_target,
        )
        .await?
        {
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
    let db = crud_store.database_connection();
    let projection_key = refill_projection_key_for_workspace(workspace_id)?;
    let Some(meta) = find_projection_meta(&db, projection_key.as_str()).await? else {
        return Ok(false);
    };

    let marker = ProjectionMetaRecordLike {
        projection_config_hash: meta.projection_config_hash.as_deref(),
        projection_config_json: meta.projection_config_json.as_deref(),
    };
    Ok(
        meta.projection_version == THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION
            && meta.status == PROJECTION_META_STATUS_COMPLETE
            && projection_target.matches_projection_meta(&marker),
    )
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
    let db = crud_store.database_connection();
    let projection_key = refill_projection_key_for_workspace(workspace_id)?;
    let Some(meta) = find_projection_meta(&db, projection_key.as_str()).await? else {
        return Ok(pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required);
    };

    if meta.projection_version != THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION {
        return Ok(pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required);
    }

    let marker = ProjectionMetaRecordLike {
        projection_config_hash: meta.projection_config_hash.as_deref(),
        projection_config_json: meta.projection_config_json.as_deref(),
    };
    if !projection_target.matches_projection_meta(&marker) {
        return Ok(pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Required);
    }

    let status = match meta.status.as_str() {
        PROJECTION_META_STATUS_COMPLETE => {
            pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Complete
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

async fn refill_after_marker(
    crud_store: Arc<CrudStore>,
    thread_episodic_storage_root: &Path,
    workspace_id: &str,
    embedding_provider_resolver: Option<Arc<dyn ThreadEpisodicIndexEmbeddingProviderResolver>>,
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
    summary.workspace_count = 1;
    let source_counts = crud_store
        .count_thread_episodic_refill_sources_for_workspace(workspace_id)
        .await
        .context("failed to count thread episodic refill sources")?;
    summary.source_thread_count = source_counts.source_thread_count;
    summary.source_turn_count = source_counts.source_turn_count;
    summary.source_turn_item_count = source_counts.source_turn_item_count;

    enqueue_refill_jobs(crud_store.as_ref(), workspace_id, now_unix, &mut summary).await?;
    execute_refill_jobs(
        crud_store,
        thread_episodic_storage_root,
        workspace_id,
        embedding_provider_resolver,
        &mut summary,
    )
    .await?;
    Ok(summary)
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
    summary.exclusion_rows_deleted = crud_store
        .delete_thread_episodic_exclusions_for_workspace(workspace_id)
        .await
        .context("failed to delete thread episodic exclusion rows")?;
    summary.item_rows_deleted = crud_store
        .delete_thread_episodic_items_for_workspace(workspace_id)
        .await
        .context("failed to delete thread episodic item rows")?;
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
            ))
        } else {
            base_payload_provider
        };
    let config = ThreadEpisodicIndexExecutorConfig::default();

    for _ in 0..REFILL_EXECUTOR_MAX_BATCHES {
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
            break;
        }

        summary.executor_batches = summary.executor_batches.saturating_add(1);
        for job in jobs {
            let attempt_started_at = Instant::now();
            match payload_provider.resolve_index_request(&job).await {
                Ok(resolved) => {
                    execute_resolved_refill_job(
                        crud_store.as_ref(),
                        &backend,
                        job,
                        resolved,
                        now_unix,
                        config,
                        attempt_started_at,
                        summary,
                    )
                    .await?;
                }
                Err(error) => {
                    let retryable = matches!(
                        error.kind,
                        ThreadEpisodicIndexResolutionFailureKind::Retryable
                    ) && job.attempt_count < config.max_attempts;
                    persist_refill_job_failure(
                        crud_store.as_ref(),
                        &job,
                        retryable,
                        false,
                        Some(error.message),
                        now_unix,
                        attempt_started_at,
                        config,
                    )
                    .await?;
                    if retryable {
                        summary.failed_retryable_jobs =
                            summary.failed_retryable_jobs.saturating_add(1);
                    } else {
                        summary.failed_terminal_jobs =
                            summary.failed_terminal_jobs.saturating_add(1);
                    }
                }
            }
        }
    }

    summary.incomplete_jobs = crud_store
        .count_incomplete_thread_episodic_index_jobs_for_workspace(workspace_id)
        .await
        .context("failed to count incomplete thread episodic index jobs")?;
    if summary.failed_terminal_jobs > 0 {
        bail!(
            "thread episodic workspace refill failed {} index jobs terminally",
            summary.failed_terminal_jobs
        );
    }
    if summary.incomplete_jobs > 0 {
        bail!(
            "thread episodic workspace refill left {} incomplete index jobs",
            summary.incomplete_jobs
        );
    }

    Ok(())
}

async fn execute_resolved_refill_job(
    crud_store: &CrudStore,
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
            persist_successful_refill_item(
                crud_store,
                &job,
                resolved,
                output,
                now_unix,
                attempt_started_at,
            )
            .await?;
            update_refill_capsule_capacity(
                crud_store,
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
                    crud_store,
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
            if capacity_error {
                update_refill_capsule_capacity(
                    crud_store,
                    resolved.request.capsule_id.as_str(),
                    &ThreadEpisodicMemvidStats::default(),
                    Some(error.message.clone()),
                    true,
                    now_unix,
                    config,
                )
                .await;
                rotate_refill_capsule_after_capacity_event(
                    crud_store,
                    resolved.request.capsule_id.as_str(),
                    now_unix,
                    "capacity_exceeded",
                )
                .await;
            }
            persist_refill_job_failure(
                crud_store,
                &job,
                retryable,
                capacity_error,
                Some(error.message),
                now_unix,
                attempt_started_at,
                config,
            )
            .await?;
            if retryable {
                summary.failed_retryable_jobs = summary.failed_retryable_jobs.saturating_add(1);
            } else {
                summary.failed_terminal_jobs = summary.failed_terminal_jobs.saturating_add(1);
            }
        }
    }

    Ok(())
}

async fn persist_successful_refill_item(
    crud_store: &CrudStore,
    job: &ThreadEpisodicIndexJobRecord,
    resolved: ThreadEpisodicResolvedIndexRequest,
    output: ThreadEpisodicMemvidIndexOutput,
    now_unix: i64,
    attempt_started_at: Instant,
) -> Result<()> {
    crud_store
        .mark_thread_episodic_item_indexed(
            job.index_item_id.as_str(),
            ThreadEpisodicItemIndexedUpdate {
                capsule_id: resolved.request.capsule_id.clone(),
                capsule_ref: resolved.request.capsule_ref.clone(),
                segment_index: resolved.segment_index,
                frame_id: output.frame_id,
                frame_uri: output.frame_uri.clone(),
            },
            now_unix,
        )
        .await
        .with_context(|| {
            format!(
                "failed to persist thread episodic item `{}` frame mapping",
                job.index_item_id
            )
        })?;
    crud_store
        .complete_thread_episodic_index_job(
            job.id.as_str(),
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
                "failed to mark thread episodic refill job `{}` completed",
                job.id
            )
        })?;
    refresh_refill_thread_directory(
        crud_store,
        job.workspace_id.as_str(),
        job.thread_id.as_str(),
        now_unix,
    )
    .await?;

    Ok(())
}

async fn persist_refill_job_failure(
    crud_store: &CrudStore,
    job: &ThreadEpisodicIndexJobRecord,
    retryable: bool,
    capacity_error: bool,
    error_message: Option<String>,
    now_unix: i64,
    attempt_started_at: Instant,
    config: ThreadEpisodicIndexExecutorConfig,
) -> Result<()> {
    let next_run_at_unix = if retryable && capacity_error {
        Some(now_unix)
    } else {
        retryable.then(|| next_refill_retry_at(job, now_unix, config))
    };
    let sanitized_error =
        error_message.map(|message| sanitize_refill_index_error(message.as_str()));
    crud_store
        .fail_thread_episodic_index_job(
            job.id.as_str(),
            ThreadEpisodicIndexJobFailureUpdate {
                retryable,
                next_run_at_unix,
                last_error: sanitized_error,
                capacity_error,
                last_attempt_latency_ms: Some(elapsed_ms(attempt_started_at)),
            },
            now_unix,
        )
        .await
        .with_context(|| {
            format!(
                "failed to persist thread episodic refill job `{}` failure",
                job.id
            )
        })?;

    if !retryable {
        crud_store
            .mark_thread_episodic_item_failed(job.index_item_id.as_str(), now_unix)
            .await
            .with_context(|| {
                format!(
                    "failed to mark thread episodic refill item `{}` failed",
                    job.index_item_id
                )
            })?;
    }

    Ok(())
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

async fn refresh_refill_thread_directory(
    crud_store: &CrudStore,
    workspace_id: &str,
    thread_id: &str,
    now_unix: i64,
) -> Result<()> {
    let indexed_item_count = crud_store
        .count_active_thread_episodic_items_for_thread(workspace_id, thread_id)
        .await
        .with_context(|| {
            format!("failed to count thread episodic items for refill thread `{thread_id}`")
        })?;
    crud_store
        .upsert_thread_episodic_thread_directory_entry(
            NewThreadEpisodicThreadDirectoryRecord {
                id: None,
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                title: None,
                summary_hash: None,
                summary_ref: None,
                thread_created_at: None,
                thread_updated_at: Some(fixed_datetime_from_unix(now_unix)),
                last_indexed_at: Some(fixed_datetime_from_unix(now_unix)),
                indexed_item_count,
                task_affinity_json: None,
                project_affinity_json: None,
                visibility: ThreadEpisodicThreadDirectoryVisibility::Visible,
                status: ThreadEpisodicThreadDirectoryStatus::Active,
            },
            now_unix,
        )
        .await
        .with_context(|| {
            format!("failed to refresh thread episodic refill directory for thread `{thread_id}`")
        })?;
    Ok(())
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

async fn mark_refill_complete(
    db: &sea_orm::DatabaseConnection,
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

async fn mark_refill_failed(
    db: &sea_orm::DatabaseConnection,
    workspace_id: &str,
    error: &anyhow::Error,
    projection_target: &ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget,
) -> Result<()> {
    let now = now_datetime();
    let projection_key = refill_projection_key_for_workspace(workspace_id)?;
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
            last_error: Some(format!("{error:#}")),
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
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::bootstrap;
    use crate::thread_episodic::{
        StoreThreadEpisodicIngestor, ThreadEpisodicCommittedItem, ThreadEpisodicIngestor,
    };
    use crate::workspace::WorkspaceManager;
    use migration::{Migrator, MigratorTrait};
    use pioneer_crud::{
        NewThreadEpisodicIndexJobRecord, NewThreadEpisodicItemRecord,
        ThreadEpisodicActiveWriteSegmentRequest, ThreadEpisodicCapsuleStatus,
        ThreadEpisodicGraphEnrichmentState, ThreadEpisodicIndexJobStatus, ThreadEpisodicItemStatus,
        ThreadEpisodicItemVisibility, ThreadEpisodicSourceActorRole,
        ThreadEpisodicSourceRuntimeKind,
    };
    use pioneer_memory::ThreadEpisodicEmbeddingError;
    use pioneer_protocol::{
        ItemCompletedNotification, SandboxMode, Thread,
        ThreadEpisodicSourceActorRole as ProtocolThreadEpisodicSourceActorRole,
        ThreadEpisodicSourceContext, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
        ThreadStatus, Turn, TurnItem, TurnItemType, TurnKind, TurnOrigin, TurnStatus, UserInput,
    };
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

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
                    embedding_dimension: Some(1536),
                    embedding_normalized: true,
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
            embedding_dimension: Some(1536),
            embedding_normalized: true,
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
                embedding_dimension: Some(3072),
                ..base_config.clone()
            },
            GatewayThreadEpisodicVectorSearchConfig {
                embedding_dimension: Some(768),
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
                    embedding_dimension: Some(1536),
                    embedding_normalized: true,
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
        let target = vector_projection_target(3);
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));

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
        let target = vector_projection_target(3);
        let embedding_provider = Arc::new(
            StaticThreadEpisodicEmbeddingProvider::with_declared_dimension(
                "openai",
                "text-embedding-3-small",
                3,
                vec![0.1, 0.2, 0.3, 0.4],
            ),
        );

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
    async fn thread_episodic_vector_refill_retryable_embedding_failure_does_not_complete_marker() {
        let (crud_store, temp_dir, workspace_id) = setup_store().await;
        let thread_id = "thread_vector_refill_retryable";
        ingest_materialized_user_item(
            crud_store.clone(),
            workspace_id.as_str(),
            thread_id,
            "turn_vector_refill_retryable",
            "item_vector_refill_retryable",
            "vector refill provider failure should leave refill incomplete",
        )
        .await;
        let target = vector_projection_target(3);
        let embedding_provider =
            Arc::new(StaticThreadEpisodicEmbeddingProvider::retryable_failure());

        let error = refill_once_with_projection(
            crud_store.clone(),
            temp_dir.path(),
            target.clone(),
            Some(embedding_provider.clone()),
        )
        .await
        .expect_err("retryable embedding failure should fail refill");

        assert!(
            error
                .to_string()
                .contains("thread episodic workspace refill left"),
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
        assert_eq!(jobs[0].status, ThreadEpisodicIndexJobStatus::Failed);
        assert!(jobs[0].attempt_count >= 1);
        assert!(
            jobs[0]
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("rate limited"))
        );
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
        let new_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &new_config,
            );
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));

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
        let target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &config,
            );
        mark_refill_marker_with_target(
            crud_store.as_ref(),
            PROJECTION_META_STATUS_COMPLETE,
            THREAD_EPISODIC_WORKSPACE_CAPSULE_REFILL_VERSION,
            &target,
        )
        .await;
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::new(vec![
            0.1, 0.2, 0.3,
        ]));

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
            "openai/text-embedding-3-small",
            3,
        );
        let openrouter_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &openrouter_config,
            );
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "openai/text-embedding-3-small",
            vec![0.1, 0.2, 0.3],
        ));

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
            embedding_dimension: Some(3),
            embedding_normalized: true,
        };
        let projection_target =
            ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
                &vector_config,
            );
        let embedding_provider = Arc::new(StaticThreadEpisodicEmbeddingProvider::with_identity(
            "openrouter",
            "smoke/test-embedding",
            vec![0.57735026, 0.57735026, 0.57735026],
        ));

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

    fn vector_projection_target(
        dimension: u32,
    ) -> ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget {
        ThreadEpisodicWorkspaceCapsuleRefillProjectionTarget::from_vector_search_config(
            &vector_search_config(
                GatewayThreadEpisodicVectorProviderConfig::OpenAi,
                "text-embedding-3-small",
                dimension,
            ),
        )
    }

    fn vector_search_config(
        provider: GatewayThreadEpisodicVectorProviderConfig,
        model: &str,
        dimension: u32,
    ) -> GatewayThreadEpisodicVectorSearchConfig {
        GatewayThreadEpisodicVectorSearchConfig {
            enabled: true,
            provider: Some(provider),
            model: Some(model.to_owned()),
            local_model: Some("bge-small-en-v1.5".to_owned()),
            embedding_dimension: Some(dimension),
            embedding_normalized: true,
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
            turns: Vec::new(),
        };
        let turn = Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::Completed,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
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
