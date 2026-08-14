use crate::attachments::observability;
use crate::attachments::types::{
    AttachmentPipelineConfig, AttachmentTransportKind, PreparedAttachment,
};
use crate::types::AttachmentArtifactContext;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::sync::{Arc, OnceLock, RwLock};

#[derive(Debug, Clone)]
pub struct ArtifactExternalRefLookupRequest {
    pub provider: String,
    pub authority_fingerprint: String,
    pub model_family: String,
    pub transport_kind: AttachmentTransportKind,
    pub sha256: String,
    pub registry_key: String,
    pub now_unix_ms: i64,
    pub artifact: Option<AttachmentArtifactContext>,
}

#[derive(Debug, Clone)]
pub struct ArtifactExternalRefStoreRequest {
    pub provider: String,
    pub authority_fingerprint: String,
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
    pub artifact: Option<AttachmentArtifactContext>,
}

#[async_trait]
pub trait ArtifactExternalRefCacheBackend: Send + Sync {
    async fn lookup_uploaded_reference(
        &self,
        request: ArtifactExternalRefLookupRequest,
    ) -> Result<Option<String>>;

    async fn store_uploaded_reference(
        &self,
        request: ArtifactExternalRefStoreRequest,
    ) -> Result<()>;
}

static ARTIFACT_EXTERNAL_REF_CACHE_BACKEND: OnceLock<
    RwLock<Option<Arc<dyn ArtifactExternalRefCacheBackend>>>,
> = OnceLock::new();

fn backend_store() -> &'static RwLock<Option<Arc<dyn ArtifactExternalRefCacheBackend>>> {
    ARTIFACT_EXTERNAL_REF_CACHE_BACKEND.get_or_init(|| RwLock::new(None))
}

fn artifact_external_ref_cache_backend() -> Result<Arc<dyn ArtifactExternalRefCacheBackend>> {
    backend_store()
        .read()
        .expect("artifact external ref cache backend lock poisoned")
        .clone()
        .ok_or_else(|| {
            anyhow!(
                "artifact external ref cache backend is not initialized; call set_artifact_external_ref_cache_backend(...) during gateway startup or disable upload_registry in provider attachments config"
            )
        })
}

pub fn set_artifact_external_ref_cache_backend(backend: Arc<dyn ArtifactExternalRefCacheBackend>) {
    let mut guard = backend_store()
        .write()
        .expect("artifact external ref cache backend lock poisoned");
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

pub fn upload_registry_key_for_authority(
    provider: &str,
    model_family: &str,
    transport_kind: AttachmentTransportKind,
    sha256: &str,
    authority_fingerprint: &str,
) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        provider.trim().to_ascii_lowercase(),
        authority_fingerprint.trim().to_ascii_lowercase(),
        model_family.trim().to_ascii_lowercase(),
        transport_kind.as_str(),
        sha256.trim().to_ascii_lowercase(),
    )
}

pub async fn lookup_uploaded_reference_with_artifact_for_authority(
    config: &AttachmentPipelineConfig,
    provider: &str,
    model_family: &str,
    transport_kind: AttachmentTransportKind,
    sha256: &str,
    authority_fingerprint: &str,
    artifact: Option<&AttachmentArtifactContext>,
) -> Result<Option<String>> {
    if !config.upload_registry.enabled {
        return Ok(None);
    }
    let Some(artifact) = artifact else {
        return Ok(None);
    };

    let key = upload_registry_key_for_authority(
        provider,
        model_family,
        transport_kind,
        sha256,
        authority_fingerprint,
    );
    let result = artifact_external_ref_cache_backend()?
        .lookup_uploaded_reference(ArtifactExternalRefLookupRequest {
            provider: provider.trim().to_ascii_lowercase(),
            authority_fingerprint: authority_fingerprint.trim().to_ascii_lowercase(),
            model_family: model_family.trim().to_ascii_lowercase(),
            transport_kind,
            sha256: sha256.trim().to_ascii_lowercase(),
            registry_key: key.clone(),
            now_unix_ms: now_unix_ms(),
            artifact: Some(artifact.clone()),
        })
        .await?;

    if result.is_some() {
        observability::emit_upload_registry_hit(provider, key.as_str());
    } else {
        observability::emit_upload_registry_miss(provider, key.as_str());
    }

    Ok(result)
}

pub async fn store_uploaded_reference_for_authority(
    config: &AttachmentPipelineConfig,
    provider: &str,
    model_family: &str,
    transport_kind: AttachmentTransportKind,
    attachment: &PreparedAttachment,
    provider_file_id: &str,
    authority_fingerprint: &str,
) -> Result<()> {
    if !config.upload_registry.enabled {
        return Ok(());
    }
    if attachment.artifact.is_none() {
        return Ok(());
    }

    let trimmed_id = provider_file_id.trim();
    if trimmed_id.is_empty() {
        return Ok(());
    }

    let ttl_secs = config.upload_registry.ttl_secs.max(60);
    let uploaded_at_unix_ms = now_unix_ms();
    let expires_at_unix_ms = expires_at(uploaded_at_unix_ms, ttl_secs);
    let key = upload_registry_key_for_authority(
        provider,
        model_family,
        transport_kind,
        attachment.sha256.as_str(),
        authority_fingerprint,
    );

    artifact_external_ref_cache_backend()?
        .store_uploaded_reference(ArtifactExternalRefStoreRequest {
            provider: provider.trim().to_ascii_lowercase(),
            authority_fingerprint: authority_fingerprint.trim().to_ascii_lowercase(),
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
            artifact: attachment.artifact.clone(),
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

    const TEST_AUTHORITY_FINGERPRINT: &str = "test-authority-generation-a";

    #[derive(Default)]
    struct InMemoryRegistryBackend {
        entries: Mutex<HashMap<String, ArtifactExternalRefStoreRequest>>,
    }

    #[async_trait]
    impl ArtifactExternalRefCacheBackend for InMemoryRegistryBackend {
        async fn lookup_uploaded_reference(
            &self,
            request: ArtifactExternalRefLookupRequest,
        ) -> Result<Option<String>> {
            let mut guard = self.entries.lock().expect("entries lock poisoned");
            guard.retain(|_, entry| entry.expires_at_unix_ms > request.now_unix_ms);
            Ok(guard
                .get(request.registry_key.as_str())
                .map(|entry| entry.provider_file_id.clone()))
        }

        async fn store_uploaded_reference(
            &self,
            request: ArtifactExternalRefStoreRequest,
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
            artifact: None,
        }
    }

    #[tokio::test]
    async fn registry_roundtrip_store_and_lookup_for_artifact_attachment() {
        set_artifact_external_ref_cache_backend(Arc::new(InMemoryRegistryBackend::default()));

        let config = AttachmentPipelineConfig::default();
        let artifact = AttachmentArtifactContext {
            workspace_id: "ws_a".to_owned(),
            artifact_id: "artifact_a".to_owned(),
            artifact_version_id: Some("version_a".to_owned()),
        };
        let mut attachment = test_attachment();
        attachment.artifact = Some(artifact.clone());
        let model_family = model_family_for_model("gpt-4.1-mini");

        let miss = lookup_uploaded_reference_with_artifact_for_authority(
            &config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            attachment.sha256.as_str(),
            TEST_AUTHORITY_FINGERPRINT,
            Some(&artifact),
        )
        .await
        .expect("lookup should succeed");
        assert!(miss.is_none());

        store_uploaded_reference_for_authority(
            &config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            &attachment,
            "file-123",
            TEST_AUTHORITY_FINGERPRINT,
        )
        .await
        .expect("store should succeed");

        let hit = lookup_uploaded_reference_with_artifact_for_authority(
            &config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            attachment.sha256.as_str(),
            TEST_AUTHORITY_FINGERPRINT,
            Some(&artifact),
        )
        .await
        .expect("lookup should succeed");
        assert_eq!(hit.as_deref(), Some("file-123"));
    }

    #[tokio::test]
    async fn registry_ignores_non_artifact_attachments() {
        set_artifact_external_ref_cache_backend(Arc::new(InMemoryRegistryBackend::default()));

        let config = AttachmentPipelineConfig::default();
        let attachment = test_attachment();
        let model_family = model_family_for_model("gpt-4.1-mini");

        store_uploaded_reference_for_authority(
            &config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            &attachment,
            "file-legacy",
            TEST_AUTHORITY_FINGERPRINT,
        )
        .await
        .expect("store should be a no-op");

        let hit = lookup_uploaded_reference_with_artifact_for_authority(
            &config,
            "openai",
            model_family.as_str(),
            AttachmentTransportKind::Upload,
            attachment.sha256.as_str(),
            TEST_AUTHORITY_FINGERPRINT,
            None,
        )
        .await
        .expect("lookup should be a no-op");
        assert!(hit.is_none());
    }

    #[test]
    fn upload_registry_key_is_stable() {
        let key = upload_registry_key_for_authority(
            "OpenAI",
            "GPT-4.1-Mini",
            AttachmentTransportKind::Upload,
            "AABBCC",
            TEST_AUTHORITY_FINGERPRINT,
        );
        assert_eq!(
            key,
            "openai|test-authority-generation-a|gpt-4.1-mini|upload|aabbcc"
        );
    }
}
