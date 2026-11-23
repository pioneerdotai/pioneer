use anyhow::{Context, Result};
use pioneer_crud_memory::episodic_embeddings::{
    NewThreadEpisodicEmbeddingArtifactRecord, find_embedding_artifact_by_id,
    insert_embedding_artifact_if_absent, prepare_embedding_artifact,
    thread_episodic_embedding_artifact_record_from_model, touch_embedding_artifact,
};
use pioneer_entity::thread_episodic_embedding_artifacts;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, EntityTrait, PaginatorTrait};

#[tokio::test]
async fn embedding_artifacts_reject_invalid_vectors_and_reuse_only_the_same_identity() -> Result<()>
{
    with_database(async |fixture| {
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('workspace','Workspace'),('other-workspace','Other')"
        ).await?;
        let now = "2026-11-20T01:00:00Z".parse()?;
        let artifact = NewThreadEpisodicEmbeddingArtifactRecord {
            id: "artifact".into(),
            workspace_id: "workspace".into(),
            pipeline_identity_hash: "pipeline".into(),
            input_hash: "input".into(),
            provider_id: "test".into(),
            model: "test-model".into(),
            dimension: 3,
            normalized: false,
            vector: vec![2.0, 0.0, 1.0],
        };
        for (dimension, vector) in [
            (3, vec![1.0]), (3, vec![f32::NAN, 0.0, 1.0]),
            (3, vec![f32::INFINITY, 0.0, 1.0]), (3, vec![0.0; 3]), (0, Vec::new()),
        ] {
            assert!(prepare_embedding_artifact(
                NewThreadEpisodicEmbeddingArtifactRecord { dimension, vector, ..artifact.clone() }, now,
            ).is_err());
            assert!(find_embedding_artifact_by_id(db, "artifact").await?.is_none());
        }

        let saved = insert_embedding_artifact_if_absent(db, prepare_embedding_artifact(artifact.clone(), now)?).await?;
        let later = "2026-11-20T01:00:01Z".parse()?;
        let reused = insert_embedding_artifact_if_absent(db, prepare_embedding_artifact(artifact.clone(), later)?).await?;
        assert_eq!(reused, saved);
        assert_eq!(thread_episodic_embedding_artifact_record_from_model(reused)?.vector, [2.0, 0.0, 1.0]);
        assert_eq!(thread_episodic_embedding_artifacts::Entity::find().count(db).await?, 1);
        let conflicting = NewThreadEpisodicEmbeddingArtifactRecord { workspace_id: "other-workspace".into(), ..artifact };
        assert!(insert_embedding_artifact_if_absent(db, prepare_embedding_artifact(conflicting, later)?).await.is_err());

        let touched = touch_embedding_artifact(db, "artifact", later).await?.context("saved artifact")?;
        let found = find_embedding_artifact_by_id(db, "artifact").await?.context("retained artifact")?;
        assert_eq!(touched, found);
        assert_eq!((found.workspace_id.as_str(), found.created_at, found.last_used_at), ("workspace", now, later));
        Ok(())
    }).await
}
