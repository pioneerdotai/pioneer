use anyhow::Result;
use async_trait::async_trait;
use pioneer_crud::{AttachmentUploadRegistryRecord, CrudStore};
use pioneer_provider::{
    AttachmentUploadRegistryBackend, UploadRegistryLookupRequest, UploadRegistryStoreRequest,
};
use std::sync::Arc;

pub struct CrudAttachmentUploadRegistryBackend {
    store: Arc<CrudStore>,
}

impl CrudAttachmentUploadRegistryBackend {
    pub fn new(store: Arc<CrudStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl AttachmentUploadRegistryBackend for CrudAttachmentUploadRegistryBackend {
    async fn lookup_uploaded_reference(
        &self,
        request: UploadRegistryLookupRequest,
    ) -> Result<Option<String>> {
        self.store
            .lookup_attachment_upload_reference(request.registry_key.as_str(), request.now_unix_ms)
            .await
    }

    async fn store_uploaded_reference(&self, request: UploadRegistryStoreRequest) -> Result<()> {
        self.store
            .upsert_attachment_upload_registry_record(&AttachmentUploadRegistryRecord {
                registry_key: request.registry_key,
                provider: request.provider,
                model_family: request.model_family,
                transport_kind: request.transport_kind.as_str().to_owned(),
                sha256: request.sha256,
                provider_file_id: request.provider_file_id,
                uploaded_at_unix_ms: request.uploaded_at_unix_ms,
                ttl_secs: request.ttl_secs,
                expires_at_unix_ms: request.expires_at_unix_ms,
                mime_type: request.mime_type,
                size_bytes: request.size_bytes,
                file_name: request.file_name,
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_provider::{AttachmentTransportKind, attachments::upload_registry_key};
    use sea_orm::Database;

    async fn setup_backend() -> CrudAttachmentUploadRegistryBackend {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite in-memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must run");
        let store = Arc::new(CrudStore::new(connection));
        CrudAttachmentUploadRegistryBackend::new(store)
    }

    #[tokio::test]
    async fn sqlite_registry_roundtrip() {
        let backend = setup_backend().await;
        let key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "aabbccdd",
        );

        let miss = backend
            .lookup_uploaded_reference(UploadRegistryLookupRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "aabbccdd".to_owned(),
                registry_key: key.clone(),
                now_unix_ms: 200,
            })
            .await
            .expect("lookup should succeed");
        assert!(miss.is_none());

        backend
            .store_uploaded_reference(UploadRegistryStoreRequest {
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
            })
            .await
            .expect("store should succeed");

        let hit = backend
            .lookup_uploaded_reference(UploadRegistryLookupRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "aabbccdd".to_owned(),
                registry_key: key,
                now_unix_ms: 220,
            })
            .await
            .expect("lookup should succeed");
        assert_eq!(hit.as_deref(), Some("file-1"));
    }

    #[tokio::test]
    async fn sqlite_registry_prunes_expired() {
        let backend = setup_backend().await;
        let key = upload_registry_key(
            "openai",
            "gpt-4.1-mini",
            AttachmentTransportKind::Upload,
            "expired",
        );

        backend
            .store_uploaded_reference(UploadRegistryStoreRequest {
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
            })
            .await
            .expect("store should succeed");

        let hit = backend
            .lookup_uploaded_reference(UploadRegistryLookupRequest {
                provider: "openai".to_owned(),
                model_family: "gpt-4.1-mini".to_owned(),
                transport_kind: AttachmentTransportKind::Upload,
                sha256: "expired".to_owned(),
                registry_key: key,
                now_unix_ms: 25,
            })
            .await
            .expect("lookup should succeed");
        assert!(hit.is_none());
    }
}
