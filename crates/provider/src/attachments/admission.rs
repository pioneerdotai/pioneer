//! Input evidence owned by one existing provider authority instance. Discovery
//! replaces its snapshot; pin receipts witness ingress already checked locally.
use super::{PreparedAttachment, PreparedAttachmentSource};
use crate::{InputContentType, catalog::CatalogModel};
use pioneer_protocol::ProviderModelInfo;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, RwLock},
};

pub(crate) struct AdmissionState {
    discovery: RwLock<Arc<Vec<ProviderModelInfo>>>,
    pins: RwLock<HashMap<(String, String), HashSet<&'static str>>>,
    #[cfg(test)]
    pub(crate) catalog: Option<Arc<crate::catalog::ModelCatalog>>,
    #[cfg(test)]
    pub(crate) pipeline_config: Option<super::AttachmentPipelineConfig>,
    #[cfg(test)]
    pub(crate) url_fixtures: RwLock<HashMap<String, Vec<u8>>>,
    #[cfg(test)]
    pub(crate) url_reads: std::sync::atomic::AtomicUsize,
}
impl Default for AdmissionState {
    fn default() -> Self {
        Self {
            discovery: RwLock::new(Arc::new(Vec::new())),
            pins: RwLock::new(HashMap::new()),
            #[cfg(test)]
            catalog: None,
            #[cfg(test)]
            pipeline_config: None,
            #[cfg(test)]
            url_fixtures: RwLock::new(HashMap::new()),
            #[cfg(test)]
            url_reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}
impl AdmissionState {
    pub(crate) fn replace_discovery(&self, models: Vec<ProviderModelInfo>) {
        *self.discovery.write().expect("input discovery lock") = Arc::new(models);
    }
    pub(crate) fn model(&self, id: &str) -> Option<ProviderModelInfo> {
        self.discovery
            .read()
            .expect("input discovery lock")
            .iter()
            .find(|m| m.id == id)
            .cloned()
    }
    pub(crate) fn pin(&self, attachment: &PreparedAttachment) -> anyhow::Result<()> {
        let ingress = match attachment.source {
            PreparedAttachmentSource::Path { .. } => "path",
            PreparedAttachmentSource::Url { .. } => "url",
            PreparedAttachmentSource::Bytes | PreparedAttachmentSource::Reference { .. } => {
                return Ok(());
            }
        };
        let key = (
            attachment.sha256.clone(),
            format!("{:?}:{}", attachment.kind, attachment.mime_type),
        );
        let mut pins = self.pins.write().expect("input pin lock");
        // Bounded lifetime by provider cache ownership; eviction fails closed.
        if pins.len() >= 4096 && !pins.contains_key(&key) {
            return Err(super::MediaInputRejection(
                "authority media ingress receipt capacity exceeded; input was not prepared",
            )
            .into());
        }
        pins.entry(key).or_default().insert(ingress);
        Ok(())
    }
    pub(crate) fn permits_ingress(
        &self,
        attachment: &PreparedAttachment,
        allowed: &[serde_json::Value],
    ) -> bool {
        self.pins
            .read()
            .expect("input pin lock")
            .get(&(
                attachment.sha256.clone(),
                format!("{:?}:{}", attachment.kind, attachment.mime_type),
            ))
            .is_some_and(|sources| {
                allowed
                    .iter()
                    .any(|v| v.as_str().is_some_and(|s| sources.contains(s)))
            })
    }
    #[cfg(test)]
    pub(crate) fn fixture_url(&self, url: &str) -> Option<Vec<u8>> {
        let bytes = self.url_fixtures.read().unwrap().get(url).cloned()?;
        self.url_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some(bytes)
    }
    #[cfg(test)]
    pub(crate) fn for_test(catalog: Arc<crate::catalog::ModelCatalog>) -> Self {
        Self {
            catalog: Some(catalog),
            ..Default::default()
        }
    }
}

tokio::task_local! { static STATE: Arc<AdmissionState>; }
thread_local! { static BLOCKING_STATE: std::cell::RefCell<Option<Arc<AdmissionState>>> = const { std::cell::RefCell::new(None) }; }
pub(crate) async fn scope<T>(state: Arc<AdmissionState>, future: impl Future<Output = T>) -> T {
    STATE.scope(state, future).await
}
pub(crate) fn current() -> Option<Arc<AdmissionState>> {
    STATE
        .try_with(Clone::clone)
        .ok()
        .or_else(|| BLOCKING_STATE.with(|s| s.borrow().clone()))
}
pub(super) fn blocking_scope<T>(
    state: Option<Arc<AdmissionState>>,
    operation: impl FnOnce() -> T,
) -> T {
    struct Restore(Option<Arc<AdmissionState>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            BLOCKING_STATE.with(|s| {
                s.replace(self.0.take());
            });
        }
    }
    let _restore = Restore(BLOCKING_STATE.with(|s| s.replace(state)));
    operation()
}

pub(super) fn effective_model(
    provider: &str,
    id: &str,
    catalog: &crate::catalog::ModelCatalog,
) -> Option<CatalogModel> {
    let discovery = current().and_then(|s| s.model(id));
    crate::catalog::effective_input_model(
        provider,
        id,
        catalog.model(provider, id),
        discovery.as_ref(),
    )
}

pub(super) fn source_name(source: &PreparedAttachmentSource) -> &'static str {
    match source {
        PreparedAttachmentSource::Bytes => "bytes",
        PreparedAttachmentSource::Path { .. } => "path",
        PreparedAttachmentSource::Url { .. } => "url",
        PreparedAttachmentSource::Reference { .. } => "reference",
    }
}

pub(crate) fn input_key(kind: InputContentType) -> &'static str {
    match kind {
        InputContentType::Text => "text",
        InputContentType::Image => "image",
        InputContentType::File => "file",
        InputContentType::Audio => "audio",
        InputContentType::Video => "video",
    }
}
