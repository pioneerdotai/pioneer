use pioneer_crud::CrudStore;
use serde::Serialize;

use crate::blob_store::ArtifactBlobStore;
use crate::error::ArtifactResult;
use crate::output_dir::{ArtifactOutputDirGcCandidate, execute_output_dir_gc, plan_output_dir_gc};

const DEFAULT_GC_GRACE_SECS: u64 = 24 * 60 * 60;
const DEFAULT_OUTPUT_DIR_TTL_SECS: u64 = 24 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ArtifactGcPolicy {
    pub grace_secs: u64,
    pub output_dir_ttl_secs: u64,
}

impl Default for ArtifactGcPolicy {
    fn default() -> Self {
        Self {
            grace_secs: DEFAULT_GC_GRACE_SECS,
            output_dir_ttl_secs: DEFAULT_OUTPUT_DIR_TTL_SECS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct ArtifactGcPlan {
    pub workspace_id: String,
    pub temp_uploads: u64,
    pub expired_download_cache_entries: u64,
    pub orphan_blobs: Vec<ArtifactGcBlobCandidate>,
    pub expired_output_dirs: Vec<ArtifactOutputDirGcCandidate>,
    pub stale_projections: Vec<String>,
    pub expired_external_refs: u64,
    pub estimated_bytes_reclaimable: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactGcBlobCandidate {
    pub blob_id: String,
    pub storage_key: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactGcReport {
    pub plan: ArtifactGcPlan,
    pub deleted_blobs: u64,
    pub deleted_output_dirs: u64,
    pub deleted_projections: u64,
    pub pruned_external_refs: u64,
}

pub async fn plan_gc(
    store: &CrudStore,
    workspace_id: &str,
    now_unix_ms: i64,
) -> ArtifactResult<ArtifactGcPlan> {
    plan_gc_with_policy(
        store,
        None,
        workspace_id,
        now_unix_ms,
        ArtifactGcPolicy::default(),
    )
    .await
}

pub async fn plan_gc_with_policy(
    store: &CrudStore,
    blob_store: Option<&dyn ArtifactBlobStore>,
    workspace_id: &str,
    now_unix_ms: i64,
    policy: ArtifactGcPolicy,
) -> ArtifactResult<ArtifactGcPlan> {
    let record = store
        .plan_artifact_gc(workspace_id, now_unix_ms, policy.grace_secs)
        .await?;
    let output_dir_plan = match blob_store.and_then(|store| store.local_artifact_root()) {
        Some(artifact_root) => {
            plan_output_dir_gc(
                artifact_root.as_path(),
                workspace_id,
                now_unix_ms,
                policy.output_dir_ttl_secs,
            )
            .await?
        }
        None => Default::default(),
    };
    Ok(ArtifactGcPlan {
        workspace_id: record.workspace_id,
        temp_uploads: 0,
        expired_download_cache_entries: 0,
        estimated_bytes_reclaimable: record
            .estimated_bytes_reclaimable
            .saturating_add(output_dir_plan.estimated_bytes_reclaimable),
        orphan_blobs: record
            .orphan_blobs
            .into_iter()
            .map(|blob| ArtifactGcBlobCandidate {
                blob_id: blob.blob_id,
                storage_key: blob.storage_key,
                size_bytes: blob.size_bytes,
            })
            .collect(),
        expired_output_dirs: output_dir_plan.candidates,
        stale_projections: record.stale_projections,
        expired_external_refs: record.expired_external_refs,
    })
}

pub async fn execute_gc(
    store: &CrudStore,
    blob_store: &dyn ArtifactBlobStore,
    workspace_id: &str,
    now_unix_ms: i64,
) -> ArtifactResult<ArtifactGcReport> {
    execute_gc_with_policy(
        store,
        blob_store,
        workspace_id,
        now_unix_ms,
        ArtifactGcPolicy::default(),
    )
    .await
}

pub async fn execute_gc_with_policy(
    store: &CrudStore,
    blob_store: &dyn ArtifactBlobStore,
    workspace_id: &str,
    now_unix_ms: i64,
    policy: ArtifactGcPolicy,
) -> ArtifactResult<ArtifactGcReport> {
    let plan =
        plan_gc_with_policy(store, Some(blob_store), workspace_id, now_unix_ms, policy).await?;
    let mut deleted_blobs = 0;
    for blob in &plan.orphan_blobs {
        blob_store
            .delete_unreferenced(workspace_id, blob.storage_key.as_str())
            .await?;
        deleted_blobs += store
            .delete_artifact_blob_row(workspace_id, &blob.blob_id)
            .await?;
    }
    let deleted_output_dirs = match blob_store.local_artifact_root() {
        Some(artifact_root) => {
            execute_output_dir_gc(
                artifact_root.as_path(),
                workspace_id,
                now_unix_ms,
                policy.output_dir_ttl_secs,
            )
            .await?
            .deleted_dirs
        }
        None => 0,
    };

    let mut deleted_projections = 0;
    for projection_id in &plan.stale_projections {
        deleted_projections += store
            .delete_artifact_projection_row(workspace_id, projection_id)
            .await?;
    }

    let pruned_external_refs = store
        .prune_expired_artifact_external_refs(workspace_id, now_unix_ms)
        .await?;
    Ok(ArtifactGcReport {
        plan,
        deleted_blobs,
        deleted_output_dirs,
        deleted_projections,
        pruned_external_refs,
    })
}
