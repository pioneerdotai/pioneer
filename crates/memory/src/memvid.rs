use crate::backend::{
    BackendDeleteRequest, BackendDeleteResult, BackendGetRequest, BackendPayload,
    BackendPutRequest, BackendPutResult, BackendSearchHit, BackendSearchRequest,
    BackendSearchScope, MemoryBackend,
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use memvid_core::{
    AclEnforcementMode, DocMetadata, FrameStatus, Memvid, PutOptions, SearchHit, SearchRequest,
};
use pioneer_crud::{
    AgentMemoryCapsuleRecord, CrudStore, MemoryScopeResolution, NewAgentMemoryRepairJob,
};
use pioneer_protocol::{
    MemoryActorKind, MemoryCategory, MemoryScope, MemoryScopeKind, MemorySensitivity,
    MemorySourceKind, MemoryStatus,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

const CAPSULE_SLOT_PRIMARY: &str = "primary";
const CAPSULE_BACKEND: &str = "memvid";
const CAPSULE_FORMAT: &str = "mv2";
const CAPSULE_STATUS_ACTIVE: &str = "active";
const REPAIR_STATUS_OK: &str = "ok";
const REPAIR_STATUS_REPAIR_NEEDED: &str = "repair_needed";
const REPAIR_JOB_MEMVID_REGISTRY_WRITE_FAILED: &str = "memvid_registry_write_failed";
const REPAIR_JOB_MEMVID_CAPSULE_MISSING: &str = "memvid_capsule_missing";
const REPAIR_JOB_MEMVID_FRAME_MISSING: &str = "memvid_frame_missing";
const REPAIR_JOB_MEMVID_FRAME_METADATA_INVALID: &str = "memvid_frame_metadata_invalid";
const REPAIR_JOB_MEMVID_DELETE_FAILED: &str = "memvid_delete_failed";
const REPAIR_JOB_MEMVID_SEARCH_FAILED: &str = "memvid_search_failed";
const REPAIR_PRIORITY_DEFAULT: i64 = 10;
const REPAIR_MAX_ATTEMPTS_DEFAULT: i32 = 3;
const MEMORY_SCHEMA_VERSION: &str = "1";
const DEFAULT_SNIPPET_CHARS: usize = 240;
const DEFAULT_TOP_K_MULTIPLIER: u32 = 4;
const PAYLOAD_MAGIC: &[u8] = b"\xFFPIONEER_MEMORY_PAYLOAD_V1\0";

#[derive(Debug, Clone)]
pub struct MemvidMemoryBackendConfig {
    pub capsules_root: PathBuf,
    pub backend_name: String,
    pub format: String,
    pub encrypted: bool,
    pub default_top_k_multiplier: u32,
}

impl MemvidMemoryBackendConfig {
    pub fn new(capsules_root: impl Into<PathBuf>) -> Self {
        Self {
            capsules_root: capsules_root.into(),
            backend_name: CAPSULE_BACKEND.to_owned(),
            format: CAPSULE_FORMAT.to_owned(),
            encrypted: false,
            default_top_k_multiplier: DEFAULT_TOP_K_MULTIPLIER,
        }
    }
}

pub struct MemvidMemoryBackend {
    store: Arc<CrudStore>,
    config: MemvidMemoryBackendConfig,
    locks: Mutex<BTreeMap<PathBuf, Arc<Mutex<()>>>>,
    delete_error: RwLock<Option<String>>,
}

impl MemvidMemoryBackend {
    pub fn new(store: Arc<CrudStore>, config: MemvidMemoryBackendConfig) -> Self {
        Self {
            store,
            config,
            locks: Mutex::new(BTreeMap::new()),
            delete_error: RwLock::new(None),
        }
    }

    pub fn with_capsules_root(store: Arc<CrudStore>, capsules_root: impl Into<PathBuf>) -> Self {
        Self::new(store, MemvidMemoryBackendConfig::new(capsules_root))
    }

    pub async fn set_delete_error(&self, error: Option<String>) {
        *self.delete_error.write().await = error;
    }

    async fn capsule_for_write(
        &self,
        scope: MemoryScope,
    ) -> Result<(MemoryScopeResolution, AgentMemoryCapsuleRecord, PathBuf)> {
        let resolved = self
            .store
            .resolve_memory_scope(scope.clone())
            .await
            .with_context(|| {
                format!(
                    "failed to resolve memvid memory scope `{:?}/{}`",
                    scope.kind, scope.key
                )
            })?;

        let capsule = match self.store.find_primary_agent_memory_capsule(scope).await? {
            Some(capsule) => capsule,
            None => self.create_capsule_registry(&resolved).await?,
        };
        let path = path_from_storage_uri(capsule.storage_uri.as_str())?;
        Ok((resolved, capsule, path))
    }

    async fn capsule_for_read(
        &self,
        request: &BackendGetRequest,
    ) -> Result<Option<(AgentMemoryCapsuleRecord, PathBuf)>> {
        let capsule = if let Some(capsule_ref) = request.capsule_ref.as_deref() {
            self.store
                .find_agent_memory_capsule_by_ref(capsule_ref)
                .await?
        } else {
            self.store
                .find_primary_agent_memory_capsule(request.scope.clone())
                .await?
        };

        let Some(capsule) = capsule else {
            return Ok(None);
        };
        let path = path_from_storage_uri(capsule.storage_uri.as_str())?;
        Ok(Some((capsule, path)))
    }

    async fn capsule_for_delete(
        &self,
        request: &BackendDeleteRequest,
    ) -> Result<Option<(AgentMemoryCapsuleRecord, PathBuf)>> {
        let capsule = if let Some(capsule_ref) = request.capsule_ref.as_deref() {
            self.store
                .find_agent_memory_capsule_by_ref(capsule_ref)
                .await?
        } else {
            self.store
                .find_primary_agent_memory_capsule(request.scope.clone())
                .await?
        };

        let Some(capsule) = capsule else {
            return Ok(None);
        };
        let path = path_from_storage_uri(capsule.storage_uri.as_str())?;
        Ok(Some((capsule, path)))
    }

    async fn create_capsule_registry(
        &self,
        resolved: &MemoryScopeResolution,
    ) -> Result<AgentMemoryCapsuleRecord> {
        let scope_kind = scope_kind_to_db(resolved.scope.kind);
        let capsule_id = deterministic_capsule_id(scope_kind, resolved.scope_key_hash.as_str());
        let capsule_ref = capsule_ref(scope_kind, resolved.scope_key_hash.as_str(), &capsule_id);
        let path = absolute_path(capsule_path(
            self.config.capsules_root.as_path(),
            scope_kind,
            resolved.scope_key_hash.as_str(),
            &capsule_id,
        ));
        let metadata_json = serde_json::json!({
            "schema_version": MEMORY_SCHEMA_VERSION,
            "path_policy": "scope_key_hash_only",
            "scope_slot": CAPSULE_SLOT_PRIMARY,
        })
        .to_string();

        let result = self
            .store
            .upsert_agent_memory_capsule(
                AgentMemoryCapsuleRecord {
                    id: Some(capsule_id.clone()),
                    scope: resolved.scope.clone(),
                    scope_key_hash: Some(resolved.scope_key_hash.clone()),
                    workspace_id: resolved.workspace_id.clone(),
                    scope_slot: Some(CAPSULE_SLOT_PRIMARY.to_owned()),
                    capsule_ref: capsule_ref.clone(),
                    storage_uri: storage_uri_from_path(path.as_path()),
                    backend: self.config.backend_name.clone(),
                    format: self.config.format.clone(),
                    encrypted: self.config.encrypted,
                    status: CAPSULE_STATUS_ACTIVE.to_owned(),
                    repair_status: REPAIR_STATUS_OK.to_owned(),
                    content_hash: None,
                    active_record_count: 0,
                    metadata_json: Some(metadata_json),
                    created_at_unix: None,
                    updated_at_unix: None,
                    last_error: None,
                },
                current_unix(),
            )
            .await;
        match result {
            Ok(capsule) => Ok(capsule),
            Err(error) => {
                let now = current_unix();
                let _ = self
                    .store
                    .enqueue_agent_memory_repair_job(
                        NewAgentMemoryRepairJob {
                            job_kind: REPAIR_JOB_MEMVID_REGISTRY_WRITE_FAILED.to_owned(),
                            workspace_id: resolved.workspace_id.clone(),
                            scope_kind: Some(resolved.scope.kind),
                            scope_key_hash: Some(resolved.scope_key_hash.clone()),
                            memory_id: None,
                            capsule_id: Some(capsule_id),
                            priority: REPAIR_PRIORITY_DEFAULT,
                            max_attempts: REPAIR_MAX_ATTEMPTS_DEFAULT,
                            scheduled_at_unix: now,
                            payload_json: Some(
                                serde_json::json!({
                                    "operation": "create_capsule_registry",
                                    "capsule_ref": capsule_ref,
                                    "storage_uri": storage_uri_from_path(path.as_path()),
                                    "error": error.to_string(),
                                })
                                .to_string(),
                            ),
                        },
                        now,
                    )
                    .await;
                Err(error)
            }
        }
    }

    async fn lock_for_path(&self, path: &Path) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().await;
        locks
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn mark_capsule_missing(
        &self,
        capsule: &AgentMemoryCapsuleRecord,
        memory_id: Option<&str>,
        frame_uri: Option<&str>,
        job_kind: &str,
        error: String,
    ) -> Result<()> {
        let now = current_unix();
        if let Some(capsule_id) = capsule.id.as_deref() {
            self.store
                .mark_agent_memory_capsule_repair_status(
                    capsule_id,
                    REPAIR_STATUS_REPAIR_NEEDED,
                    Some(error.clone()),
                    now,
                )
                .await?;
            self.store
                .enqueue_agent_memory_repair_job(
                    NewAgentMemoryRepairJob {
                        job_kind: job_kind.to_owned(),
                        workspace_id: capsule.workspace_id.clone(),
                        scope_kind: Some(capsule.scope.kind),
                        scope_key_hash: capsule.scope_key_hash.clone(),
                        memory_id: memory_id.map(str::to_owned),
                        capsule_id: Some(capsule_id.to_owned()),
                        priority: REPAIR_PRIORITY_DEFAULT,
                        max_attempts: REPAIR_MAX_ATTEMPTS_DEFAULT,
                        scheduled_at_unix: now,
                        payload_json: Some(
                            serde_json::json!({
                                "memory_id": memory_id,
                                "capsule_ref": capsule.capsule_ref,
                                "storage_uri": capsule.storage_uri,
                                "frame_uri": frame_uri,
                                "error": error,
                            })
                            .to_string(),
                        ),
                    },
                    now,
                )
                .await?;
        }
        Ok(())
    }

    async fn refresh_capsule_count(
        &self,
        capsule: AgentMemoryCapsuleRecord,
        active_record_count: i64,
    ) -> Result<AgentMemoryCapsuleRecord> {
        self.store
            .upsert_agent_memory_capsule(
                AgentMemoryCapsuleRecord {
                    active_record_count,
                    repair_status: REPAIR_STATUS_OK.to_owned(),
                    last_error: None,
                    ..capsule
                },
                current_unix(),
            )
            .await
    }
}

#[async_trait]
impl MemoryBackend for MemvidMemoryBackend {
    async fn put(&self, request: BackendPutRequest) -> Result<BackendPutResult> {
        let (resolved, capsule, path) = self.capsule_for_write(request.scope.clone()).await?;
        tokio::fs::create_dir_all(
            path.parent().with_context(|| {
                format!("memvid capsule path `{}` has no parent", path.display())
            })?,
        )
        .await
        .with_context(|| format!("failed to create memvid capsule dir `{}`", path.display()))?;

        let lock = self.lock_for_path(path.as_path()).await;
        let _guard = lock.lock().await;
        let frame_uri = frame_uri(
            scope_kind_to_db(resolved.scope.kind),
            resolved.scope_key_hash.as_str(),
            request.memory_id.as_str(),
        );
        let content = encode_payload(&request)?;
        let options = put_options(&request, &resolved, frame_uri.as_str());
        let path_for_task = path.clone();
        let frame_uri_for_task = frame_uri.clone();
        let outcome = tokio::task::spawn_blocking(move || -> Result<MemvidPutOutcome> {
            let mut memvid = open_or_create_memvid(path_for_task.as_path())?;
            match memvid.frame_by_uri(frame_uri_for_task.as_str()) {
                Ok(frame) if frame.status == FrameStatus::Active => {
                    memvid.update_frame(frame.id, Some(content), options, None)?;
                }
                _ => {
                    memvid.put_bytes_with_options(content.as_slice(), options)?;
                }
            }
            memvid.commit()?;
            let frame = memvid.frame_by_uri(frame_uri_for_task.as_str())?;
            let stats = memvid.stats()?;
            Ok(MemvidPutOutcome {
                frame_id: frame.id,
                active_frame_count: stats.active_frame_count,
            })
        })
        .await
        .context("memvid put task failed")??;

        let capsule = self
            .refresh_capsule_count(
                capsule,
                i64::try_from(outcome.active_frame_count)
                    .context("memvid active frame count does not fit i64")?,
            )
            .await?;

        Ok(BackendPutResult {
            capsule_id: capsule.id,
            capsule_ref: Some(capsule.capsule_ref),
            frame_id: Some(
                i64::try_from(outcome.frame_id).context("memvid frame id does not fit i64")?,
            ),
            frame_uri: Some(frame_uri),
            frame_version: i64::try_from(outcome.frame_id)
                .context("memvid frame id does not fit i64")?,
            backend_metadata_json: None,
        })
    }

    async fn get(&self, request: BackendGetRequest) -> Result<Option<BackendPayload>> {
        let Some((capsule, path)) = self.capsule_for_read(&request).await? else {
            return Ok(None);
        };
        if !path.exists() {
            self.mark_capsule_missing(
                &capsule,
                Some(request.memory_id.as_str()),
                request.frame_uri.as_deref(),
                REPAIR_JOB_MEMVID_CAPSULE_MISSING,
                format!("memvid capsule file `{}` is missing", path.display()),
            )
            .await?;
            return Ok(None);
        }

        let lock = self.lock_for_path(path.as_path()).await;
        let _guard = lock.lock().await;
        let frame_uri = request
            .frame_uri
            .clone()
            .or_else(|| {
                frame_uri_from_scope_hash(
                    &request.scope,
                    request.scope_key_hash.as_deref(),
                    request.memory_id.as_str(),
                )
            })
            .or_else(|| frame_uri_from_capsule(&capsule, request.memory_id.as_str()));
        let Some(frame_uri) = frame_uri else {
            return Ok(None);
        };
        let path_for_task = path.clone();
        let frame_uri_for_task = frame_uri.clone();
        let loaded = tokio::task::spawn_blocking(move || -> Result<Option<MemvidGetOutcome>> {
            let mut memvid = open_read_only_memvid(path_for_task.as_path())?;
            let frame = match memvid.frame_by_uri(frame_uri_for_task.as_str()) {
                Ok(frame) if frame.status == FrameStatus::Active => frame,
                Ok(_) => return Ok(None),
                Err(_) => return Ok(None),
            };
            let payload = memvid.frame_canonical_payload(frame.id)?;
            let content = decode_payload(payload.as_slice())?;
            Ok(Some(MemvidGetOutcome {
                content,
                frame_id: frame.id,
                metadata: frame.extra_metadata,
            }))
        })
        .await
        .context("memvid get task failed")??;

        let Some(loaded) = loaded else {
            self.mark_capsule_missing(
                &capsule,
                Some(request.memory_id.as_str()),
                Some(frame_uri.as_str()),
                REPAIR_JOB_MEMVID_FRAME_MISSING,
                format!("memvid frame `{frame_uri}` is missing"),
            )
            .await?;
            return Ok(None);
        };

        if loaded
            .metadata
            .get("pioneer.memory_id")
            .is_some_and(|memory_id| memory_id != &request.memory_id)
        {
            self.mark_capsule_missing(
                &capsule,
                Some(request.memory_id.as_str()),
                Some(frame_uri.as_str()),
                REPAIR_JOB_MEMVID_FRAME_METADATA_INVALID,
                format!(
                    "memvid frame `{}` belongs to another memory id",
                    loaded.frame_id
                ),
            )
            .await?;
            return Ok(None);
        }

        Ok(Some(BackendPayload {
            memory_id: request.memory_id,
            content: loaded.content,
            snippet: None,
            metadata_json: None,
        }))
    }

    async fn search(&self, request: BackendSearchRequest) -> Result<Vec<BackendSearchHit>> {
        let resolved_scopes = self.search_scopes(&request).await?;
        if resolved_scopes.is_empty() {
            return Ok(Vec::new());
        }

        let mut hits = Vec::new();
        let per_capsule_limit = request
            .limit
            .max(1)
            .saturating_mul(self.config.default_top_k_multiplier.max(1));
        for resolved in resolved_scopes {
            let capsule = if let Some(capsule_ref) = resolved.capsule_ref.as_deref() {
                self.store
                    .find_agent_memory_capsule_by_ref(capsule_ref)
                    .await?
            } else {
                self.store
                    .find_primary_agent_memory_capsule(resolved.scope.clone())
                    .await?
            };
            let Some(capsule) = capsule else {
                continue;
            };
            if capsule.repair_status != REPAIR_STATUS_OK {
                continue;
            }
            let path = path_from_storage_uri(capsule.storage_uri.as_str())?;
            if !path.exists() {
                self.mark_capsule_missing(
                    &capsule,
                    None,
                    None,
                    REPAIR_JOB_MEMVID_CAPSULE_MISSING,
                    format!("memvid capsule file `{}` is missing", path.display()),
                )
                .await?;
                continue;
            }

            let lock = self.lock_for_path(path.as_path()).await;
            let _guard = lock.lock().await;
            let path_for_task = path.clone();
            let query = request.query.clone();
            let search_request =
                memvid_search_request(query, per_capsule_limit as usize, DEFAULT_SNIPPET_CHARS);
            let scope_key_hash = resolved.scope_key_hash.clone();
            let scope_kind = scope_kind_to_db(resolved.scope.kind).to_owned();
            let capsule_hits_result =
                tokio::task::spawn_blocking(move || -> Result<Vec<BackendSearchHit>> {
                    let mut memvid = open_read_only_memvid(path_for_task.as_path())?;
                    let response = memvid.search(search_request)?;
                    Ok(response
                        .hits
                        .into_iter()
                        .filter_map(|hit| {
                            search_hit_to_backend_hit(
                                hit,
                                scope_kind.as_str(),
                                scope_key_hash.as_str(),
                            )
                        })
                        .collect())
                })
                .await
                .context("memvid search task failed")?;
            let capsule_hits = match capsule_hits_result {
                Ok(capsule_hits) => capsule_hits,
                Err(error) => {
                    self.mark_capsule_missing(
                        &capsule,
                        None,
                        None,
                        REPAIR_JOB_MEMVID_SEARCH_FAILED,
                        error.to_string(),
                    )
                    .await?;
                    continue;
                }
            };
            hits.extend(capsule_hits);
        }

        let mut deduped = BTreeMap::<String, BackendSearchHit>::new();
        for hit in hits {
            match deduped.get(hit.memory_id.as_str()) {
                Some(existing) if score_value(existing.score) >= score_value(hit.score) => {}
                _ => {
                    deduped.insert(hit.memory_id.clone(), hit);
                }
            }
        }
        let mut hits = deduped.into_values().collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.memory_id.cmp(&right.memory_id))
        });
        hits.truncate(request.limit as usize);
        Ok(hits)
    }

    async fn delete(&self, request: BackendDeleteRequest) -> Result<BackendDeleteResult> {
        if let Some(error) = self.delete_error.read().await.clone() {
            let Some((capsule, _path)) = self.capsule_for_delete(&request).await? else {
                bail!(error);
            };
            self.mark_capsule_missing(
                &capsule,
                Some(request.memory_id.as_str()),
                request.frame_uri.as_deref(),
                REPAIR_JOB_MEMVID_DELETE_FAILED,
                error.clone(),
            )
            .await?;
            bail!(error);
        }

        let Some((capsule, path)) = self.capsule_for_delete(&request).await? else {
            return Ok(BackendDeleteResult { deleted: false });
        };
        if !path.exists() {
            self.mark_capsule_missing(
                &capsule,
                Some(request.memory_id.as_str()),
                request.frame_uri.as_deref(),
                REPAIR_JOB_MEMVID_CAPSULE_MISSING,
                format!("memvid capsule file `{}` is missing", path.display()),
            )
            .await?;
            return Ok(BackendDeleteResult { deleted: false });
        }

        let lock = self.lock_for_path(path.as_path()).await;
        let _guard = lock.lock().await;
        let frame_uri = request
            .frame_uri
            .clone()
            .or_else(|| {
                frame_uri_from_scope_hash(
                    &request.scope,
                    request.scope_key_hash.as_deref(),
                    request.memory_id.as_str(),
                )
            })
            .or_else(|| frame_uri_from_capsule(&capsule, request.memory_id.as_str()));
        let Some(frame_uri) = frame_uri else {
            return Ok(BackendDeleteResult { deleted: false });
        };
        let path_for_task = path.clone();
        let frame_id = request.frame_id;
        let frame_uri_for_task = frame_uri.clone();
        let outcome = tokio::task::spawn_blocking(move || -> Result<MemvidDeleteOutcome> {
            let mut memvid = Memvid::open(path_for_task.as_path())?;
            let frame = if let Some(frame_id) = frame_id {
                let frame = memvid
                    .frame_by_id(u64::try_from(frame_id).context("negative memvid frame id")?)?;
                if frame.uri.as_deref() == Some(frame_uri_for_task.as_str())
                    && frame.status == FrameStatus::Active
                {
                    Some(frame)
                } else {
                    memvid
                        .frame_by_uri(frame_uri_for_task.as_str())
                        .ok()
                        .filter(|candidate| candidate.status == FrameStatus::Active)
                }
            } else {
                memvid
                    .frame_by_uri(frame_uri_for_task.as_str())
                    .ok()
                    .filter(|candidate| candidate.status == FrameStatus::Active)
            };

            let Some(frame) = frame else {
                return Ok(MemvidDeleteOutcome {
                    deleted: false,
                    active_frame_count: memvid.stats()?.active_frame_count,
                });
            };
            memvid.delete_frame(frame.id)?;
            memvid.commit()?;
            Ok(MemvidDeleteOutcome {
                deleted: true,
                active_frame_count: memvid.stats()?.active_frame_count,
            })
        })
        .await
        .context("memvid delete task failed")??;

        self.refresh_capsule_count(
            capsule,
            i64::try_from(outcome.active_frame_count)
                .context("memvid active frame count does not fit i64")?,
        )
        .await?;
        Ok(BackendDeleteResult {
            deleted: outcome.deleted,
        })
    }
}

impl MemvidMemoryBackend {
    async fn search_scopes(
        &self,
        request: &BackendSearchRequest,
    ) -> Result<Vec<BackendSearchScope>> {
        if !request.resolved_scopes.is_empty() {
            return Ok(request.resolved_scopes.clone());
        }

        self.store
            .resolve_memory_scopes(request.scopes.clone())
            .await
            .map(|resolved| {
                resolved
                    .into_iter()
                    .map(|scope| BackendSearchScope {
                        scope: scope.scope,
                        scope_key_hash: scope.scope_key_hash,
                        workspace_id: scope.workspace_id,
                        capsule_ref: None,
                    })
                    .collect()
            })
    }
}

#[must_use]
pub fn memvid_search_request(
    query: impl Into<String>,
    top_k: usize,
    snippet_chars: usize,
) -> SearchRequest {
    SearchRequest {
        query: query.into(),
        top_k,
        snippet_chars,
        uri: None,
        scope: None,
        cursor: None,
        temporal: None,
        as_of_frame: None,
        as_of_ts: None,
        no_sketch: true,
        acl_context: None,
        acl_enforcement_mode: AclEnforcementMode::default(),
    }
}

#[derive(Debug)]
struct MemvidPutOutcome {
    frame_id: u64,
    active_frame_count: u64,
}

#[derive(Debug)]
struct MemvidGetOutcome {
    content: String,
    frame_id: u64,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug)]
struct MemvidDeleteOutcome {
    deleted: bool,
    active_frame_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredMemvidPayload {
    schema_version: u32,
    memory_id: String,
    content: String,
}

fn encode_payload(request: &BackendPutRequest) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(&StoredMemvidPayload {
        schema_version: 1,
        memory_id: request.memory_id.clone(),
        content: request.content.clone(),
    })
    .context("failed to encode memvid memory payload")?;
    let mut payload = Vec::with_capacity(PAYLOAD_MAGIC.len() + json.len());
    payload.extend_from_slice(PAYLOAD_MAGIC);
    payload.extend_from_slice(json.as_slice());
    Ok(payload)
}

fn decode_payload(payload: &[u8]) -> Result<String> {
    if let Some(json) = payload.strip_prefix(PAYLOAD_MAGIC) {
        let stored = serde_json::from_slice::<StoredMemvidPayload>(json)
            .context("failed to decode memvid memory payload")?;
        return Ok(stored.content);
    }
    if let Ok(stored) = serde_json::from_slice::<StoredMemvidPayload>(payload) {
        return Ok(stored.content);
    }
    String::from_utf8(payload.to_vec()).context("memvid memory payload is not valid UTF-8")
}

fn open_or_create_memvid(path: &Path) -> Result<Memvid> {
    if path.exists() {
        Memvid::open(path).map_err(Into::into)
    } else {
        Memvid::create(path).map_err(Into::into)
    }
}

fn open_read_only_memvid(path: &Path) -> Result<Memvid> {
    match Memvid::open_read_only(path) {
        Ok(memvid) => Ok(memvid),
        Err(first_error) => {
            std::thread::sleep(std::time::Duration::from_millis(20));
            Memvid::open_read_only(path).with_context(|| {
                format!(
                    "failed to open memvid capsule `{}` read-only after one retry; first error: {first_error}",
                    path.display()
                )
            })
        }
    }
}

fn put_options(
    request: &BackendPutRequest,
    resolved: &MemoryScopeResolution,
    frame_uri: &str,
) -> PutOptions {
    let scope_kind = scope_kind_to_db(resolved.scope.kind);
    let namespace = request.namespace.as_deref().unwrap_or("default");
    let category = category_to_db(request.category);
    let key = request.key.as_deref().unwrap_or("");
    let sensitivity = sensitivity_to_db(request.sensitivity);
    let source_kind = source_kind_to_db(request.source_kind);
    let status = status_to_db(request.status);

    let mut builder = PutOptions::builder()
        .uri(frame_uri)
        .title(request.key.as_deref().unwrap_or(request.memory_id.as_str()))
        .track("pioneer_agent_memory")
        .kind("pioneer.memory.fact")
        .tag("pioneer.memory_id", request.memory_id.as_str())
        .tag("pioneer.scope_kind", scope_kind)
        .tag("pioneer.scope_key_hash", resolved.scope_key_hash.as_str())
        .tag("pioneer.namespace", namespace)
        .tag("pioneer.category", category)
        .tag("pioneer.key", key)
        .tag("pioneer.sensitivity", sensitivity)
        .tag("pioneer.source_kind", source_kind)
        .tag(
            "pioneer.source_thread_id",
            request.source_thread_id.as_deref().unwrap_or(""),
        )
        .tag(
            "pioneer.source_turn_id",
            request.source_turn_id.as_deref().unwrap_or(""),
        )
        .tag(
            "pioneer.source_item_id",
            request.source_item_id.as_deref().unwrap_or(""),
        )
        .tag("pioneer.policy_version", request.policy_version.as_str())
        .tag("pioneer.status", status)
        .tag("pioneer.schema_version", MEMORY_SCHEMA_VERSION)
        .metadata(DocMetadata {
            mime: Some("application/octet-stream".to_owned()),
            ..DocMetadata::default()
        })
        .search_text(request.content.as_str())
        .auto_tag(false)
        .extract_dates(false)
        .extract_triplets(false)
        .instant_index(false)
        .extraction_budget_ms(0);

    if let Some(workspace_id) = resolved.workspace_id.as_deref() {
        builder = builder.tag("pioneer.workspace_id", workspace_id);
    }
    if let Some(kind) = request.created_by_kind {
        builder = builder.tag("pioneer.created_by_kind", actor_kind_to_db(kind));
    }
    if let Some(created_by_id) = request.created_by_id.as_deref() {
        builder = builder.tag("pioneer.created_by_id", created_by_id);
    }
    if let Some(idempotency_key) = request.idempotency_key.as_deref() {
        builder = builder.tag("pioneer.idempotency_key", idempotency_key);
    }
    if let Some(metadata_json) = request.metadata_json.as_deref() {
        builder = builder.tag("pioneer.metadata_json_sha256", sha256_hex(metadata_json));
    }

    builder.build()
}

fn search_hit_to_backend_hit(
    hit: SearchHit,
    scope_kind: &str,
    scope_key_hash: &str,
) -> Option<BackendSearchHit> {
    let metadata = hit.metadata.as_ref()?.extra_metadata.clone();
    let memory_id = metadata.get("pioneer.memory_id")?.clone();
    if metadata
        .get("pioneer.scope_kind")
        .is_some_and(|candidate| candidate != scope_kind)
        || metadata
            .get("pioneer.scope_key_hash")
            .is_some_and(|candidate| candidate != scope_key_hash)
    {
        return None;
    }

    Some(BackendSearchHit {
        memory_id,
        score: hit.score,
        snippet: Some(hit.text),
        matched_terms: Vec::new(),
    })
}

fn score_value(score: Option<f32>) -> f32 {
    score.unwrap_or(0.0)
}

fn frame_uri_from_scope_hash(
    scope: &MemoryScope,
    scope_key_hash: Option<&str>,
    memory_id: &str,
) -> Option<String> {
    Some(frame_uri(
        scope_kind_to_db(scope.kind),
        scope_key_hash?,
        memory_id,
    ))
}

fn frame_uri_from_capsule(capsule: &AgentMemoryCapsuleRecord, memory_id: &str) -> Option<String> {
    Some(frame_uri(
        scope_kind_to_db(capsule.scope.kind),
        capsule.scope_key_hash.as_deref()?,
        memory_id,
    ))
}

fn deterministic_capsule_id(scope_kind: &str, scope_key_hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"agent_memory_capsule\0");
    hasher.update(scope_kind.as_bytes());
    hasher.update(b"\0");
    hasher.update(scope_key_hash.as_bytes());
    hasher.update(b"\0primary");
    let hash = hex::encode(hasher.finalize());
    hash.chars().take(21).collect()
}

fn capsule_ref(scope_kind: &str, scope_key_hash: &str, capsule_id: &str) -> String {
    format!("mv2://pioneer/{scope_kind}/{scope_key_hash}/capsules/{capsule_id}")
}

fn frame_uri(scope_kind: &str, scope_key_hash: &str, memory_id: &str) -> String {
    format!("mv2://pioneer/{scope_kind}/{scope_key_hash}/memory/{memory_id}")
}

fn capsule_path(root: &Path, scope_kind: &str, scope_key_hash: &str, capsule_id: &str) -> PathBuf {
    root.join(scope_kind)
        .join(scope_key_hash)
        .join(format!("{capsule_id}.mv2"))
}

fn absolute_path(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(&path))
            .unwrap_or(path)
    }
}

fn storage_uri_from_path(path: &Path) -> String {
    format!("file://{}", path.display())
}

fn path_from_storage_uri(storage_uri: &str) -> Result<PathBuf> {
    let path = storage_uri
        .strip_prefix("file://")
        .ok_or_else(|| anyhow!("unsupported memvid storage URI `{storage_uri}`"))?;
    Ok(PathBuf::from(path))
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn scope_kind_to_db(kind: MemoryScopeKind) -> &'static str {
    match kind {
        MemoryScopeKind::User => "user",
        MemoryScopeKind::Workspace => "workspace",
        MemoryScopeKind::Thread => "thread",
        MemoryScopeKind::Agent => "agent",
        MemoryScopeKind::Task => "task",
    }
}

fn category_to_db(category: MemoryCategory) -> &'static str {
    match category {
        MemoryCategory::Identity => "identity",
        MemoryCategory::Preference => "preference",
        MemoryCategory::Biography => "biography",
        MemoryCategory::Relationship => "relationship",
        MemoryCategory::ProjectFact => "project_fact",
        MemoryCategory::ProjectDecision => "project_decision",
        MemoryCategory::Procedure => "procedure",
        MemoryCategory::Todo => "todo",
        MemoryCategory::Constraint => "constraint",
        MemoryCategory::CommunicationStyle => "communication_style",
        MemoryCategory::Custom => "custom",
    }
}

fn sensitivity_to_db(sensitivity: MemorySensitivity) -> &'static str {
    match sensitivity {
        MemorySensitivity::Normal => "normal",
        MemorySensitivity::Personal => "personal",
        MemorySensitivity::SecretLike => "secret_like",
        MemorySensitivity::Regulated => "regulated",
    }
}

fn source_kind_to_db(source_kind: MemorySourceKind) -> &'static str {
    match source_kind {
        MemorySourceKind::ExplicitUserRequest => "explicit_user_request",
        MemorySourceKind::UserCorrection => "user_correction",
        MemorySourceKind::AssistantInference => "assistant_inference",
        MemorySourceKind::BackgroundExtractor => "background_extractor",
        MemorySourceKind::ToolObservation => "tool_observation",
        MemorySourceKind::Import => "import",
        MemorySourceKind::System => "system",
    }
}

fn actor_kind_to_db(actor_kind: MemoryActorKind) -> &'static str {
    match actor_kind {
        MemoryActorKind::User => "user",
        MemoryActorKind::Assistant => "assistant",
        MemoryActorKind::Extractor => "extractor",
        MemoryActorKind::System => "system",
        MemoryActorKind::Tool => "tool",
    }
}

fn status_to_db(status: MemoryStatus) -> &'static str {
    match status {
        MemoryStatus::Active => "active",
        MemoryStatus::Superseded => "superseded",
        MemoryStatus::Deleted => "deleted",
        MemoryStatus::Expired => "expired",
    }
}

fn current_unix() -> i64 {
    chrono::Utc::now().timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_request_helper_sets_all_fields_explicitly() {
        let request = memvid_search_request("birthday", 7, 120);
        assert_eq!(request.query, "birthday");
        assert_eq!(request.top_k, 7);
        assert_eq!(request.snippet_chars, 120);
        assert!(request.uri.is_none());
        assert!(request.scope.is_none());
        assert!(request.cursor.is_none());
        assert!(request.temporal.is_none());
        assert!(request.as_of_frame.is_none());
        assert!(request.as_of_ts.is_none());
        assert!(request.no_sketch);
        assert!(request.acl_context.is_none());
    }

    #[test]
    fn capsule_id_and_ref_do_not_include_raw_scope_key() {
        let scope_key_hash = sha256_hex("raw-user-key");
        let id = deterministic_capsule_id("user", scope_key_hash.as_str());
        let reference = capsule_ref("user", scope_key_hash.as_str(), id.as_str());
        assert_eq!(id.len(), 21);
        assert!(!reference.contains("raw-user-key"));
        assert!(reference.contains(scope_key_hash.as_str()));
    }
}
