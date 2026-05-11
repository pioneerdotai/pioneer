use anyhow::Result;
use async_trait::async_trait;
use pioneer_crud::{ArtifactExternalRefKey, CrudStore, UpsertArtifactExternalRefRequest};
use pioneer_provider::{
    ArtifactExternalRefCacheBackend, ArtifactExternalRefLookupRequest,
    ArtifactExternalRefStoreRequest, AttachmentArtifactContext,
};
use serde_json::json;
use std::sync::Arc;

pub struct CrudArtifactExternalRefCacheBackend {
    store: Arc<CrudStore>,
}

impl CrudArtifactExternalRefCacheBackend {
    pub fn new(store: Arc<CrudStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ArtifactExternalRefCacheBackend for CrudArtifactExternalRefCacheBackend {
    async fn lookup_uploaded_reference(
        &self,
        request: ArtifactExternalRefLookupRequest,
    ) -> Result<Option<String>> {
        let Some(artifact) = &request.artifact else {
            return Ok(None);
        };

        self.store
            .prune_expired_artifact_external_refs(
                artifact.workspace_id.as_str(),
                request.now_unix_ms,
            )
            .await?;
        Ok(self
            .store
            .find_active_artifact_external_ref(
                &external_ref_key(&request, artifact),
                request.now_unix_ms,
            )
            .await?
            .map(|found| found.external_id))
    }

    async fn store_uploaded_reference(
        &self,
        request: ArtifactExternalRefStoreRequest,
    ) -> Result<()> {
        let Some(artifact) = request.artifact.clone() else {
            return Ok(());
        };

        self.store
            .prune_expired_artifact_external_refs(
                artifact.workspace_id.as_str(),
                request.uploaded_at_unix_ms,
            )
            .await?;
        self.store
            .upsert_artifact_external_ref(UpsertArtifactExternalRefRequest {
                key: ArtifactExternalRefKey {
                    workspace_id: artifact.workspace_id,
                    artifact_id: artifact.artifact_id,
                    artifact_version_id: artifact.artifact_version_id,
                    provider: request.provider,
                    model_family: Some(request.model_family),
                    transport_kind: request.transport_kind.as_str().to_owned(),
                },
                external_id: request.provider_file_id,
                external_uri: None,
                expires_at_unix_ms: Some(request.expires_at_unix_ms),
                metadata: std::collections::BTreeMap::from([
                    ("registry_key".to_owned(), json!(request.registry_key)),
                    ("sha256".to_owned(), json!(request.sha256)),
                    ("mime_type".to_owned(), json!(request.mime_type)),
                    ("size_bytes".to_owned(), json!(request.size_bytes)),
                    ("file_name".to_owned(), json!(request.file_name)),
                    (
                        "uploaded_at_unix_ms".to_owned(),
                        json!(request.uploaded_at_unix_ms),
                    ),
                    ("ttl_secs".to_owned(), json!(request.ttl_secs)),
                ]),
            })
            .await?;
        Ok(())
    }
}

fn external_ref_key(
    request: &ArtifactExternalRefLookupRequest,
    artifact: &AttachmentArtifactContext,
) -> ArtifactExternalRefKey {
    ArtifactExternalRefKey {
        workspace_id: artifact.workspace_id.clone(),
        artifact_id: artifact.artifact_id.clone(),
        artifact_version_id: artifact.artifact_version_id.clone(),
        provider: request.provider.clone(),
        model_family: Some(request.model_family.clone()),
        transport_kind: request.transport_kind.as_str().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_provider::{AttachmentTransportKind, attachments::upload_registry_key};
    use sea_orm::Database;

    async fn setup_backend() -> CrudArtifactExternalRefCacheBackend {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite in-memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must run");
        let store = Arc::new(CrudStore::new(connection));
        CrudArtifactExternalRefCacheBackend::new(store)
    }

    fn artifact_context(workspace_id: &str) -> AttachmentArtifactContext {
        AttachmentArtifactContext {
            workspace_id: workspace_id.to_owned(),
            artifact_id: "artifact_a".to_owned(),
            artifact_version_id: Some("version_a".to_owned()),
        }
    }

    #[tokio::test]
    async fn non_artifact_uploads_are_not_cached() {
        let backend = setup_backend().await;
        let key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "aabbccdd",
        );

        backend
            .store_uploaded_reference(ArtifactExternalRefStoreRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "aabbccdd".to_owned(),
                registry_key: key.clone(),
                provider_file_id: "file-1".to_owned(),
                uploaded_at_unix_ms: 210,
                ttl_secs: 300,
                expires_at_unix_ms: 510,
                mime_type: "application/pdf".to_owned(),
                size_bytes: 123,
                file_name: "doc.pdf".to_owned(),
                artifact: None,
            })
            .await
            .expect("store should be a no-op");

        let hit = backend
            .lookup_uploaded_reference(ArtifactExternalRefLookupRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "aabbccdd".to_owned(),
                registry_key: key.clone(),
                now_unix_ms: 200,
                artifact: None,
            })
            .await
            .expect("lookup should succeed");
        assert!(hit.is_none());
    }

    #[tokio::test]
    async fn expired_non_artifact_uploads_are_not_cached() {
        let backend = setup_backend().await;
        let key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "expired",
        );

        backend
            .store_uploaded_reference(ArtifactExternalRefStoreRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "expired".to_owned(),
                registry_key: key.clone(),
                provider_file_id: "file-old".to_owned(),
                uploaded_at_unix_ms: 10,
                ttl_secs: 10,
                expires_at_unix_ms: 20,
                mime_type: "application/pdf".to_owned(),
                size_bytes: 100,
                file_name: "old.pdf".to_owned(),
                artifact: None,
            })
            .await
            .expect("store should be a no-op");

        let hit = backend
            .lookup_uploaded_reference(ArtifactExternalRefLookupRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "expired".to_owned(),
                registry_key: key,
                now_unix_ms: 25,
                artifact: None,
            })
            .await
            .expect("lookup should succeed");
        assert!(hit.is_none());
    }

    #[tokio::test]
    async fn artifact_upload_stores_external_ref_and_reuses_it() {
        let backend = setup_backend().await;
        let key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "artifactsha",
        );

        backend
            .store_uploaded_reference(ArtifactExternalRefStoreRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "artifactsha".to_owned(),
                registry_key: key,
                provider_file_id: "file-artifact".to_owned(),
                uploaded_at_unix_ms: 100,
                ttl_secs: 300,
                expires_at_unix_ms: 400,
                mime_type: "application/pdf".to_owned(),
                size_bytes: 100,
                file_name: "artifact.pdf".to_owned(),
                artifact: Some(artifact_context("ws_a")),
            })
            .await
            .expect("store artifact-backed reference");

        let different_sha_key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "different-sha",
        );
        let hit = backend
            .lookup_uploaded_reference(ArtifactExternalRefLookupRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "different-sha".to_owned(),
                registry_key: different_sha_key,
                now_unix_ms: 150,
                artifact: Some(artifact_context("ws_a")),
            })
            .await
            .expect("lookup artifact-backed reference");
        assert_eq!(hit.as_deref(), Some("file-artifact"));
    }

    #[tokio::test]
    async fn expired_artifact_external_ref_is_ignored() {
        let backend = setup_backend().await;
        let key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "expired-artifact",
        );

        backend
            .store_uploaded_reference(ArtifactExternalRefStoreRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "expired-artifact".to_owned(),
                registry_key: key,
                provider_file_id: "file-expired-artifact".to_owned(),
                uploaded_at_unix_ms: 10,
                ttl_secs: 10,
                expires_at_unix_ms: 20,
                mime_type: "application/pdf".to_owned(),
                size_bytes: 100,
                file_name: "expired.pdf".to_owned(),
                artifact: Some(artifact_context("ws_a")),
            })
            .await
            .expect("store artifact-backed reference");

        let missing_registry_key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "missing-registry",
        );
        let hit = backend
            .lookup_uploaded_reference(ArtifactExternalRefLookupRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "missing-registry".to_owned(),
                registry_key: missing_registry_key,
                now_unix_ms: 25,
                artifact: Some(artifact_context("ws_a")),
            })
            .await
            .expect("lookup expired artifact-backed reference");
        assert!(hit.is_none());
    }

    #[tokio::test]
    async fn artifact_external_ref_is_workspace_scoped() {
        let backend = setup_backend().await;
        let key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "workspace-a",
        );

        backend
            .store_uploaded_reference(ArtifactExternalRefStoreRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "workspace-a".to_owned(),
                registry_key: key,
                provider_file_id: "file-workspace-a".to_owned(),
                uploaded_at_unix_ms: 100,
                ttl_secs: 300,
                expires_at_unix_ms: 400,
                mime_type: "application/pdf".to_owned(),
                size_bytes: 100,
                file_name: "workspace.pdf".to_owned(),
                artifact: Some(artifact_context("ws_a")),
            })
            .await
            .expect("store workspace A artifact-backed reference");

        let missing_registry_key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "workspace-b",
        );
        let hit = backend
            .lookup_uploaded_reference(ArtifactExternalRefLookupRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "workspace-b".to_owned(),
                registry_key: missing_registry_key,
                now_unix_ms: 150,
                artifact: Some(artifact_context("ws_b")),
            })
            .await
            .expect("lookup workspace B artifact-backed reference");
        assert!(hit.is_none());
    }
}
