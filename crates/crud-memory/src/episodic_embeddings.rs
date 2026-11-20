//! Immutable pgvector embedding artifacts and cache usage metadata.

use anyhow::{Context, Result};
use pioneer_entity::thread_episodic_embedding_artifacts;
use sea_orm::entity::prelude::{DateTimeWithTimeZone, PgVector};
use sea_orm::sea_query::OnConflict;
use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, IntoActiveModel, Set};

#[derive(Debug, Clone, PartialEq)]
pub struct ThreadEpisodicEmbeddingArtifactRecord {
    pub id: String,
    pub workspace_id: String,
    pub pipeline_identity_hash: String,
    pub input_hash: String,
    pub provider_id: String,
    pub model: String,
    pub dimension: usize,
    pub normalized: bool,
    pub vector: Vec<f32>,
    pub created_at: DateTimeWithTimeZone,
    pub last_used_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewThreadEpisodicEmbeddingArtifactRecord {
    pub id: String,
    pub workspace_id: String,
    pub pipeline_identity_hash: String,
    pub input_hash: String,
    pub provider_id: String,
    pub model: String,
    pub dimension: usize,
    pub normalized: bool,
    pub vector: Vec<f32>,
}

pub async fn find_embedding_artifact_by_id<C: ConnectionTrait>(
    db: &C,
    artifact_id: &str,
) -> Result<Option<thread_episodic_embedding_artifacts::Model>> {
    thread_episodic_embedding_artifacts::Entity::find_by_id(artifact_id.to_owned())
        .one(db)
        .await
        .with_context(|| {
            format!("failed to find thread episodic embedding artifact `{artifact_id}`")
        })
}

#[derive(Clone)]
pub struct PreparedEmbeddingArtifact {
    id: String,
    model: thread_episodic_embedding_artifacts::ActiveModel,
    identity: (String, String, String, String, String, i64, bool),
}

/// Validate and build a PostgreSQL vector before writer admission. The
/// identity is immutable; a conflicting insert is checked against the durable
/// row without accepting a different workspace/model under the same cache key.
pub fn prepare_embedding_artifact(
    artifact: NewThreadEpisodicEmbeddingArtifactRecord,
    now: DateTimeWithTimeZone,
) -> Result<PreparedEmbeddingArtifact> {
    anyhow::ensure!(
        artifact.dimension > 0
            && artifact.vector.len() == artifact.dimension
            && artifact.vector.iter().all(|value| value.is_finite())
            && artifact.vector.iter().any(|value| *value != 0.0),
        "invalid embedding artifact vector"
    );
    let dimension = i64::try_from(artifact.dimension)
        .context("thread episodic embedding dimension exceeds i64")?;
    let embedding = PgVector::from(artifact.vector);
    let identity = (
        artifact.workspace_id.clone(),
        artifact.pipeline_identity_hash.clone(),
        artifact.input_hash.clone(),
        artifact.provider_id.clone(),
        artifact.model.clone(),
        dimension,
        artifact.normalized,
    );
    let id = artifact.id.clone();
    let model = thread_episodic_embedding_artifacts::ActiveModel {
        id: Set(artifact.id),
        workspace_id: Set(artifact.workspace_id),
        pipeline_identity_hash: Set(artifact.pipeline_identity_hash),
        input_hash: Set(artifact.input_hash),
        provider_id: Set(artifact.provider_id),
        model: Set(artifact.model),
        dimension: Set(dimension),
        normalized: Set(artifact.normalized),
        embedding: Set(embedding),
        created_at: Set(now),
        last_used_at: Set(now),
    };
    Ok(PreparedEmbeddingArtifact {
        id,
        model,
        identity,
    })
}

pub async fn insert_embedding_artifact_if_absent<C: ConnectionTrait>(
    db: &C,
    artifact: PreparedEmbeddingArtifact,
) -> Result<thread_episodic_embedding_artifacts::Model> {
    let PreparedEmbeddingArtifact {
        id,
        model,
        identity,
    } = artifact;
    thread_episodic_embedding_artifacts::Entity::insert(model)
        .on_conflict(
            OnConflict::column(thread_episodic_embedding_artifacts::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        .try_insert()
        .exec(db)
        .await
        .context("failed to insert thread episodic embedding artifact")?;
    let row = find_embedding_artifact_by_id(db, &id)
        .await?
        .context("thread episodic embedding artifact missing after insert")?;
    anyhow::ensure!(
        (
            &row.workspace_id,
            &row.pipeline_identity_hash,
            &row.input_hash,
            &row.provider_id,
            &row.model,
            row.dimension,
            row.normalized
        ) == (
            &identity.0,
            &identity.1,
            &identity.2,
            &identity.3,
            &identity.4,
            identity.5,
            identity.6
        ),
        "embedding artifact identity conflict"
    );
    Ok(row)
}

pub async fn touch_embedding_artifact<C: ConnectionTrait>(
    db: &C,
    artifact_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<thread_episodic_embedding_artifacts::Model>> {
    let Some(row) = find_embedding_artifact_by_id(db, artifact_id).await? else {
        return Ok(None);
    };
    let mut active = row.into_active_model();
    active.last_used_at = Set(now);
    active
        .update(db)
        .await
        .with_context(|| {
            format!("failed to touch thread episodic embedding artifact `{artifact_id}`")
        })
        .map(Some)
}

pub fn thread_episodic_embedding_artifact_record_from_model(
    model: pioneer_entity::thread_episodic_embedding_artifacts::Model,
) -> Result<ThreadEpisodicEmbeddingArtifactRecord> {
    let dimension = usize::try_from(model.dimension)
        .map_err(|_| anyhow::anyhow!("thread episodic embedding dimension is negative"))?;
    let vector = model.embedding.to_vec();
    anyhow::ensure!(
        dimension > 0
            && vector.len() == dimension
            && vector.iter().all(|value| value.is_finite())
            && vector.iter().any(|value| *value != 0.0),
        "invalid stored embedding artifact"
    );
    Ok(ThreadEpisodicEmbeddingArtifactRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        pipeline_identity_hash: model.pipeline_identity_hash,
        input_hash: model.input_hash,
        provider_id: model.provider_id,
        model: model.model,
        dimension,
        normalized: model.normalized,
        vector,
        created_at: model.created_at,
        last_used_at: model.last_used_at,
    })
}
