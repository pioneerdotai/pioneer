use crate::attachments::observability;
use crate::attachments::types::{
    AttachmentPipelineConfig, AttachmentTransportKind, PreparedAttachment,
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::sync::{Arc, OnceLock, RwLock};

#[derive(Debug, Clone)]
pub struct UploadRegistryLookupRequest {
    pub provider: String,
    pub model_family: String,
    pub transport_kind: AttachmentTransportKind,
    pub sha256: String,
    pub registry_key: String,
    pub now_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub struct UploadRegistryStoreRequest {
    pub provider: String,
    pub model_family: String,
    pub transport_kind: AttachmentTransportKind,
    pub sha256: String,
    pub registry_key: String,
    pub provider_file_id: String,
    pub uploaded_at_unix_ms: i64,
    pub ttl_secs: u64,
    pub expires_at_unix_ms: i64,
    pub mime_type: String,
    pub size_bytes: usize,
    pub file_name: String,
}

#[async_trait]
pub trait AttachmentUploadRegistryBackend: Send + Sync {
    async fn lookup_uploaded_reference(
        &self,
        request: UploadRegistryLookupRequest,
    ) -> Result<Option<String>>;

    async fn store_uploaded_reference(&self, request: UploadRegistryStoreRequest) -> Result<()>;
}

static UPLOAD_REGISTRY_BACKEND: OnceLock<RwLock<Option<Arc<dyn AttachmentUploadRegistryBackend>>>> =
    OnceLock::new();

fn backend_store() -> &'static RwLock<Option<Arc<dyn AttachmentUploadRegistryBackend>>> {
    UPLOAD_REGISTRY_BACKEND.get_or_init(|| RwLock::new(None))
}

fn upload_registry_backend() -> Result<Arc<dyn AttachmentUploadRegistryBackend>> {
    backend_store()
        .read()
        .expect("attachment upload registry backend lock poisoned")
        .clone()
        .ok_or_else(|| {
            anyhow!(
                "attachment upload registry backend is not initialized; call set_attachment_upload_registry_backend(...) during gateway startup or disable upload_registry in provider attachments config"
            )
        })
}

pub fn set_attachment_upload_registry_backend(backend: Arc<dyn AttachmentUploadRegistryBackend>) {
    let mut guard = backend_store()
        .write()
        .expect("attachment upload registry backend lock poisoned");
    *guard = Some(backend);
}

fn now_unix_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn expires_at(uploaded_at_unix_ms: i64, ttl_secs: u64) -> i64 {
    uploaded_at_unix_ms.saturating_add(
        i64::try_from(ttl_secs.saturating_mul(1000)).unwrap_or(i64::MAX.saturating_sub(1)),
    )
}

pub fn model_family_for_model(model: &str) -> String {
    let trimmed = model.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return "default".to_owned();
    }

    let without_revision = trimmed
        .split('@')
        .next()
        .map(str::trim)
        .unwrap_or(trimmed.as_str());
    let without_provider_prefix = without_revision
        .rsplit('/')
        .next()
        .map(str::trim)
        .unwrap_or(without_revision);
    without_provider_prefix.to_owned()
}

pub fn upload_registry_key(
    provider: &str,
    model_family: &str,
    transport_kind: AttachmentTransportKind,
    sha256: &str,
) -> String {
    format!(
        "{}|{}|{}|{}",
        provider.trim().to_ascii_lowercase(),
        model_family.trim().to_ascii_lowercase(),
        transport_kind.as_str(),
        sha256.trim().to_ascii_lowercase(),
    )
}

pub async fn lookup_uploaded_reference(
    config: &AttachmentPipelineConfig,
    provider: &str,
    model_family: &str,
    transport_kind: AttachmentTransportKind,
    sha256: &str,
) -> Result<Option<String>> {
    if !config.upload_registry.enabled {
        return Ok(None);
    }

    let key = upload_registry_key(provider, model_family, transport_kind, sha256);
    let result = upload_registry_backend()?
        .lookup_uploaded_reference(UploadRegistryLookupRequest {
            provider: provider.trim().to_ascii_lowercase(),
            model_family: model_family.trim().to_ascii_lowercase(),
            transport_kind,
            sha256: sha256.trim().to_ascii_lowercase(),
            registry_key: key.clone(),
            now_unix_ms: now_unix_ms(),
        })
        .await?;

    if result.is_some() {
        observability::emit_upload_registry_hit(provider, key.as_str());
    } else {
        observability::emit_upload_registry_miss(provider, key.as_str());
    }

    Ok(result)
}

pub async fn store_uploaded_reference(
    config: &AttachmentPipelineConfig,
    provider: &str,
    model_family: &str,
    transport_kind: AttachmentTransportKind,
    attachment: &PreparedAttachment,
    provider_file_id: &str,
) -> Result<()> {
    if !config.upload_registry.enabled {
        return Ok(());
    }

    let trimmed_id = provider_file_id.trim();
    if trimmed_id.is_empty() {
        return Ok(());
    }

    let ttl_secs = config.upload_registry.ttl_secs.max(60);
    let uploaded_at_unix_ms = now_unix_ms();
    let expires_at_unix_ms = expires_at(uploaded_at_unix_ms, ttl_secs);
    let key = upload_registry_key(
        provider,
        model_family,
        transport_kind,
        attachment.sha256.as_str(),
    );

    upload_registry_backend()?
        .store_uploaded_reference(UploadRegistryStoreRequest {
            provider: provider.trim().to_ascii_lowercase(),
            model_family: model_family.trim().to_ascii_lowercase(),
            transport_kind,
            sha256: attachment.sha256.to_ascii_lowercase(),
            registry_key: key.clone(),
            provider_file_id: trimmed_id.to_owned(),
            uploaded_at_unix_ms,
            ttl_secs,
            expires_at_unix_ms,
            mime_type: attachment.mime_type.clone(),
            size_bytes: attachment.size_bytes,
            file_name: attachment.name.clone(),
        })
        .await?;
    observability::emit_upload_registry_write(provider, key.as_str());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::InputContentType;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct InMemoryRegistryBackend {
        entries: Mutex<HashMap<String, UploadRegistryStoreRequest>>,
    }

    #[async_trait]
    impl AttachmentUploadRegistryBackend for InMemoryRegistryBackend {
        async fn lookup_uploaded_reference(
            &self,
            request: UploadRegistryLookupRequest,
        ) -> Result<Option<String>> {
            let mut guard = self.entries.lock().expect("entries lock poisoned");
            guard.retain(|_, entry| entry.expires_at_unix_ms > request.now_unix_ms);
            Ok(guard
                .get(request.registry_key.as_str())
                .map(|entry| entry.provider_file_id.clone()))
        }

        async fn store_uploaded_reference(
            &self,
            request: UploadRegistryStoreRequest,
        ) -> Result<()> {
            self.entries
                .lock()
                .expect("entries lock poisoned")
                .insert(request.registry_key.clone(), request);
            Ok(())
        }
    }

    fn test_attachment() -> PreparedAttachment {
        PreparedAttachment {
            message_index: 0,
            part_index: 0,
            kind: InputContentType::File,
            mime_type: "application/pdf".to_owned(),
            name: "doc.pdf".to_owned(),
            size_bytes: 12,
            sha256: "aabbccddeeff00112233445566778899".to_owned(),
            source: crate::attachments::types::PreparedAttachmentSource::Bytes,
            bytes: Some(vec![1, 2, 3]),
            transport_plan: crate::attachments::types::AttachmentTransportPlan {
                kind: AttachmentTransportKind::Upload,
                reason: "test".to_owned(),
            },
        }
    }

    #[tokio::test]
    async fn registry_roundtrip_store_and_lookup() {
        set_attachment_upload_registry_backend(Arc::new(InMemoryRegistryBackend::default()));

        let config = AttachmentPipelineConfig::default();
        let attachment = test_attachment();
        let model_family = model_family_for_model("gpt-4.1-mini");

        let miss = lookup_uploaded_reference(
            &config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            attachment.sha256.as_str(),
        )
        .await
        .expect("lookup should succeed");
        assert!(miss.is_none());

        store_uploaded_reference(
            &config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            &attachment,
            "file-123",
        )
        .await
        .expect("store should succeed");

        let hit = lookup_uploaded_reference(
            &config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            attachment.sha256.as_str(),
        )
        .await
        .expect("lookup should succeed");
        assert_eq!(hit.as_deref(), Some("file-123"));
    }

    #[test]
    fn upload_registry_key_is_stable() {
        let key = upload_registry_key(
            "OpenAI",
            "GPT-4.1-Mini",
            AttachmentTransportKind::Upload,
            "AABBCC",
        );
        assert_eq!(key, "openai|gpt-4.1-mini|upload|aabbcc");
    }
}
