use std::collections::BTreeMap;

use pioneer_protocol::{ArtifactCreatedByKind, generate_id};
use sea_orm::{DatabaseTransaction, entity::prelude::DateTimeWithTimeZone};

use crate::artifact::{
    ArtifactBindingTargetRecord, ArtifactCrudError, ArtifactCrudResult, ArtifactRepository,
    IngestArtifactMetadataRecord, IngestedArtifactRecord, NewArtifactBlobRecord,
    checked_size_bytes, metadata_to_db,
};
use crate::html_preview_io::{
    HtmlPreviewIoFamily, HtmlPreviewIoLocator, into_database_error, request_prepared,
};

const ID_LENGTH: usize = 21;

#[derive(Clone)]
pub struct PreparedArtifactIngest {
    blob: NewArtifactBlobRecord,
    artifact: IngestArtifactMetadataRecord,
    binding: Option<ArtifactBindingTargetRecord>,
    identity: Option<String>,
    limits: Option<(u64, u64)>,
    blob_id: String,
    artifact_id: String,
    version_id: String,
    binding_id: String,
    at: DateTimeWithTimeZone,
    blob_size_bytes: i64,
    blob_metadata_json: String,
    artifact_metadata_json: String,
    version_metadata_json: String,
    binding_metadata_json: String,
    released_copy_locator: Option<HtmlPreviewIoLocator>,
}

impl PreparedArtifactIngest {
    pub fn new(
        blob: NewArtifactBlobRecord,
        artifact: IngestArtifactMetadataRecord,
        binding: Option<ArtifactBindingTargetRecord>,
        version_metadata: BTreeMap<String, serde_json::Value>,
        identity: Option<String>,
        limits: Option<(u64, u64)>,
    ) -> ArtifactCrudResult<Self> {
        let at = chrono::Utc::now().fixed_offset();
        let artifact_id = identity.clone().unwrap_or_else(|| generate_id(ID_LENGTH));
        let released_copy_locator = (artifact.created_by_kind == ArtifactCreatedByKind::Tool
            && artifact.created_by_actor_id.as_deref() == Some("html_preview")
            && artifact.metadata.get("htmlTool").is_some_and(|tool| {
                tool["retention"] == "released"
                    && tool["releasedCopies"]
                        .as_array()
                        .is_some_and(|paths| !paths.is_empty())
            }))
        .then(|| HtmlPreviewIoLocator::new(&artifact_id));

        Ok(Self {
            blob_size_bytes: checked_size_bytes(blob.size_bytes)?,
            blob_metadata_json: metadata_to_db(&blob.metadata)?,
            artifact_metadata_json: metadata_to_db(&artifact.metadata)?,
            version_metadata_json: metadata_to_db(&version_metadata)?,
            binding_metadata_json: metadata_to_db(&BTreeMap::new())?,
            blob,
            artifact,
            binding,
            identity,
            limits,
            blob_id: generate_id(ID_LENGTH),
            artifact_id,
            version_id: generate_id(ID_LENGTH),
            binding_id: generate_id(ID_LENGTH),
            at,
            released_copy_locator,
        })
    }

    pub fn publication_target(&self) -> Option<&ArtifactBindingTargetRecord> {
        if self.identity.is_some()
            && self.artifact.created_by_actor_id.as_deref() == Some("html_render")
        {
            self.binding.as_ref()
        } else {
            None
        }
    }

    pub fn prepared_at(&self) -> DateTimeWithTimeZone {
        self.at
    }

    pub fn has_released_copies(&self) -> bool {
        self.released_copy_locator.is_some()
    }

    pub async fn apply(
        mut self,
        tx: &DatabaseTransaction,
    ) -> ArtifactCrudResult<IngestedArtifactRecord> {
        let repository = ArtifactRepository::new();
        let publication = self.publication_target().is_some();
        if self.identity.is_some() {
            let target =
                self.binding
                    .as_mut()
                    .ok_or_else(|| ArtifactCrudError::InvalidRequest {
                        message: "tool artifact requires owner binding".into(),
                    })?;
            repository
                .lock_tool_ingest_owner(tx, &self.artifact.workspace_id, target)
                .await?;
            if let Some(existing) = repository
                .find_idempotent_ingest(
                    tx,
                    &self.artifact_id,
                    &self.blob,
                    &self.artifact_metadata_json,
                )
                .await?
            {
                return Ok(existing);
            }
            let (bytes, files) = self
                .limits
                .ok_or_else(|| ArtifactCrudError::InvalidRequest {
                    message: "tool artifact requires quotas".into(),
                })?;
            target.item_index = Some(
                repository
                    .reserve_tool_artifact_index(
                        tx,
                        &self.artifact.workspace_id,
                        target,
                        self.blob.size_bytes,
                        bytes,
                        files,
                        publication,
                    )
                    .await?,
            );
        }
        let released_copies = self.has_released_copies();
        let blob = repository
            .find_or_create_blob_prepared(
                tx,
                self.blob,
                self.blob_id,
                self.at,
                self.blob_size_bytes,
                self.blob_metadata_json,
            )
            .await?;
        let artifact = repository
            .create_artifact_prepared(
                tx,
                &self.artifact,
                self.artifact_id,
                self.at,
                self.artifact_metadata_json,
                released_copies,
            )
            .await?;
        if let Some(locator) = &self.released_copy_locator {
            request_prepared(
                tx,
                HtmlPreviewIoFamily::Copies,
                locator,
                0,
                &artifact.workspace_id,
            )
            .await
            .map_err(|error| ArtifactCrudError::Database {
                message: "failed to enqueue released copies".into(),
                source: into_database_error(error),
            })?;
        }
        let version = repository
            .create_version_prepared(
                tx,
                &artifact,
                &blob,
                self.binding.as_ref(),
                self.version_id,
                self.at,
                self.version_metadata_json,
            )
            .await?;
        let artifact = if released_copies {
            artifact
        } else {
            repository
                .update_current_version(tx, artifact, &version.id)
                .await?
        };
        if let Some(binding) = &self.binding {
            repository
                .create_binding_prepared(
                    tx,
                    &artifact.workspace_id,
                    &artifact.id,
                    Some(&version.id),
                    binding,
                    self.binding_id,
                    self.at,
                    self.binding_metadata_json,
                )
                .await?;
            if publication {
                repository
                    .touch_html_turn_projection(
                        tx,
                        &artifact.workspace_id,
                        binding.thread_id.as_deref().unwrap_or_default(),
                        binding.turn_id.as_deref().unwrap_or_default(),
                        self.at,
                    )
                    .await?;
            }
        }
        Ok(IngestedArtifactRecord {
            artifact,
            version,
            blob,
        })
    }
}

pub type ArtifactIngestGuard = (u64, tokio_util::sync::CancellationToken);

/// Validate caller authorization before the Artifact write set uses the transaction.
pub async fn authorize_artifact_ingest(
    transaction: pioneer_postgres_pools::PostgresTransaction,
    commit_guard: Option<&ArtifactIngestGuard>,
) -> anyhow::Result<pioneer_postgres_pools::PostgresTransaction> {
    use pioneer_crud_access::policy_generation::current_policy_generation_on;
    if let Some((generation, cancel)) = commit_guard {
        if cancel.is_cancelled() {
            transaction.rollback().await?;
            return Err(ArtifactCrudError::InvalidRequest {
                message: "HTML invocation cancelled before commit".into(),
            }
            .into());
        }
        let current = current_policy_generation_on(&transaction)
            .await
            .map_err(|error| ArtifactCrudError::InvalidRequest {
                message: format!("HTML authorization unavailable: {error}"),
            })?;
        if current.get() != *generation {
            transaction.rollback().await?;
            return Err(ArtifactCrudError::InvalidRequest {
                message: "HTML authorization changed before commit; retry under current policy"
                    .into(),
            }
            .into());
        }
    }
    Ok(transaction)
}

pub struct ArtifactIngestStore<'a> {
    connection: &'a pioneer_postgres_pools::PostgresDatabase,
    io_wake: &'a tokio::sync::Notify,
}

impl<'a> ArtifactIngestStore<'a> {
    pub fn new(
        connection: &'a pioneer_postgres_pools::PostgresDatabase,
        io_wake: &'a tokio::sync::Notify,
    ) -> Self {
        Self {
            connection,
            io_wake,
        }
    }

    pub async fn commit_without_publication(
        &self,
        prepared: PreparedArtifactIngest,
        commit_guard: Option<ArtifactIngestGuard>,
    ) -> anyhow::Result<IngestedArtifactRecord> {
        use anyhow::{Context, ensure};
        use sea_orm::TransactionTrait;
        ensure!(
            prepared.publication_target().is_none(),
            "HTML publication requires a timeline commit"
        );
        let maintenance = self.connection.work_signal("maintenance.html").changing();
        let queue = crate::html_preview_io::HtmlPreviewIoQueue::new(self.connection);
        let result = queue
            .retry_write_on_conflict(|| {
                let prepared = prepared.clone();
                let commit_guard = commit_guard.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin artifact ingest transaction")?;
                    let transaction =
                        authorize_artifact_ingest(transaction, commit_guard.as_ref()).await?;
                    let record = match prepared.apply(&transaction).await {
                        Ok(record) => record,
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            return Err(error.into());
                        }
                    };
                    if commit_guard
                        .as_ref()
                        .is_some_and(|(_, cancel)| cancel.is_cancelled())
                    {
                        transaction.rollback().await?;
                        anyhow::bail!("HTML invocation cancelled before commit");
                    }
                    transaction
                        .commit()
                        .await
                        .context("failed to commit artifact ingest transaction")?;
                    Ok(record)
                }
            })
            .await;
        if result.is_ok() {
            if prepared.has_released_copies() {
                self.io_wake.notify_one();
                self.connection.work_signal("maintenance.html").notify();
            }
            maintenance.completed(true);
        }
        result
    }
}
