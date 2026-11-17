//! Session-fenced image fetches and immutable authenticated cache publications.
use super::*;
use crate::core::{ClientCore, ClientMutationAuthority, ClientScope};
use serde::{Deserialize, Serialize};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImagePublication {
    #[serde(rename = "principal_id")]
    resource_id: String,
    #[serde(rename = "avatar_revision")]
    revision: String,
    local_path: Option<ClientPath>,
    media_type: Option<ImageMediaType>,
    error: Option<ImageCacheError>,
    source: Option<ImageCacheSource>,
}
impl ImagePublication {
    pub fn resource_id(&self) -> &str {
        &self.resource_id
    }
    pub fn revision(&self) -> &str {
        &self.revision
    }
    pub fn local_path(&self) -> Option<&ClientPath> {
        self.local_path.as_ref()
    }
    pub fn media_type(&self) -> Option<ImageMediaType> {
        self.media_type
    }
    pub fn source(&self) -> Option<ImageCacheSource> {
        self.source
    }
    pub fn error(&self) -> Option<ImageCacheError> {
        self.error
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct FetchIdentity {
    gateway: GatewayId,
    session: AuthSessionId,
    authorization: u64,
    resource: String,
    revision: String,
    generation: u64,
    epoch: u64,
}
struct Fetch {
    identity: FetchIdentity,
    cancellation: CancellationToken,
}
#[derive(Default)]
pub(crate) struct ImageStore {
    epoch: u64,
    generation: u64,
    requests: HashMap<String, Fetch>,
    publications: std::collections::HashSet<String>,
    subscriptions: HashMap<String, usize>,
    cache_roots: std::collections::HashSet<(PathBuf, GatewayId)>,
}
impl ImageStore {
    pub(crate) fn invalidate(&mut self) {
        self.epoch += 1;
        self.publications.clear();
        for (_, request) in self.requests.drain() {
            request.cancellation.cancel();
        }
    }
    pub(crate) fn invalidate_access(&mut self) {
        self.invalidate();
        // Roots are registered only by authenticated Core fetches. Cancellation
        // also fences the service's final commit, so an old fetch cannot restore bytes.
        for (runtime_home, gateway_id) in self.cache_roots.drain() {
            let _ = remove_owned_directory(
                &runtime_home,
                &image_cache_root(&runtime_home).join(gateway_id.as_str()),
            );
        }
    }
    fn begin(
        &mut self,
        service: &ImageCacheService,
        authorization: u64,
        resource: String,
        revision: String,
        cancellation: CancellationToken,
    ) -> Result<FetchIdentity, ImageCacheError> {
        let key = image_identity_key(&resource, &revision);
        if let Some(previous) = self.requests.remove(&key) {
            previous.cancellation.cancel();
        }
        if !self.publications.contains(&key) && self.publications.len() >= IMAGE_CACHE_MAX_FILES {
            return Err(ImageCacheError::Cancelled);
        }
        self.publications.insert(key.clone());
        self.generation += 1;
        let identity = FetchIdentity {
            gateway: service.gateway_id.clone(),
            session: service.session_id.clone(),
            authorization,
            resource: resource.clone(),
            revision,
            generation: self.generation,
            epoch: self.epoch,
        };
        self.requests.insert(
            key,
            Fetch {
                identity: identity.clone(),
                cancellation,
            },
        );
        Ok(identity)
    }
    fn finish(&mut self, identity: &FetchIdentity) -> bool {
        if self.epoch != identity.epoch
            || self
                .requests
                .get(&image_identity_key(&identity.resource, &identity.revision))
                .is_none_or(|request| {
                    request.identity != *identity || request.cancellation.is_cancelled()
                })
        {
            return false;
        }
        self.requests
            .remove(&image_identity_key(&identity.resource, &identity.revision));
        true
    }
}
impl Drop for ImageStore {
    fn drop(&mut self) {
        self.invalidate();
    }
}
impl ClientCore {
    /// Constructor wiring for process-local consumers of the shared image owner.
    pub fn image_cache_service(
        &self,
        runtime_home: PathBuf,
    ) -> Result<ImageCacheService, ImageCacheError> {
        let sender = self.transport_runtime().ws_command_sender();
        let access = sender
            .current_gateway_http_access()
            .map_err(|_| ImageCacheError::Offline)?;
        let http = crate::transport::http::GatewayHttpSession::from_access(
            &access,
            Arc::new(crate::transport::http_authority::GatewayWsHttpAuthority { sender }),
        )
        .map_err(|_| ImageCacheError::Offline)?;
        Ok(ImageCacheService::new(
            http,
            runtime_home,
            access.gateway_id,
            access.session_id,
        ))
    }

    pub async fn resolve_image(
        &self,
        service: &ImageCacheService,
        request: ImageCacheRequest,
        cancellation: CancellationToken,
    ) -> Result<ImageCacheResult, ImageCacheError> {
        let identity = self.begin_image_fetch(
            service,
            request.resource_id.clone(),
            request.revision.clone(),
            cancellation.clone(),
        )?;
        let result = service.resolve_image(request, cancellation).await;
        let publication = match &result {
            Ok(result) => ImagePublication {
                resource_id: identity.resource.clone(),
                revision: identity.revision.clone(),
                local_path: Some(result.local_path.clone()),
                media_type: Some(result.media_type),
                error: None,
                source: Some(result.source),
            },
            Err(error) => ImagePublication {
                resource_id: identity.resource.clone(),
                revision: identity.revision.clone(),
                local_path: None,
                media_type: None,
                error: Some(*error),
                source: None,
            },
        };
        self.finish_image_fetch(&identity, publication)?;
        result
    }
    fn begin_image_fetch(
        &self,
        service: &ImageCacheService,
        resource: String,
        revision: String,
        cancellation: CancellationToken,
    ) -> Result<FetchIdentity, ImageCacheError> {
        if self.is_stopped() {
            return Err(ImageCacheError::Cancelled);
        }
        let access = self
            .transport_runtime()
            .ws_command_sender()
            .current_gateway_http_access()
            .map_err(|_| ImageCacheError::Authentication)?;
        if access.gateway_id != service.gateway_id || access.session_id != service.session_id {
            return Err(ImageCacheError::Authentication);
        }
        let authorization = self.authorization_connection_generation();
        let mut store = self.image_store.lock().expect("image store poisoned");
        store
            .cache_roots
            .insert((service.runtime_home.clone(), service.gateway_id.clone()));
        store.begin(service, authorization, resource, revision, cancellation)
    }
    fn finish_image_fetch(
        &self,
        identity: &FetchIdentity,
        publication: ImagePublication,
    ) -> Result<(), ImageCacheError> {
        let access = self
            .transport_runtime()
            .ws_command_sender()
            .current_gateway_http_access()
            .map_err(|_| ImageCacheError::Authentication)?;
        if self.is_stopped()
            || access.gateway_id != identity.gateway
            || access.session_id != identity.session
            || self.authorization_connection_generation() != identity.authorization
        {
            return Err(ImageCacheError::Cancelled);
        }
        let mut store = self.image_store.lock().expect("image store poisoned");
        if !store.finish(identity) {
            return Err(ImageCacheError::Cancelled);
        }
        let scope = ClientScope::Image {
            key: image_identity_key(&identity.resource, &identity.revision),
        };
        let previous = self.snapshot(&scope);
        if previous
            .as_ref()
            .and_then(|p| p.snapshot().payload::<ImagePublication>())
            .is_some_and(|p| p.as_ref() == &publication)
        {
            return Ok(());
        }
        let revision = previous.map_or(1, |p| p.revisions().scoped().get() + 1);
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            Arc::new(publication),
            vec![],
        );
        Ok(())
    }
}

/// Stable scope key for concurrently displayed current and historical revisions.
pub fn image_identity_key(resource: &str, revision: &str) -> String {
    format!("{resource}:{revision}")
}
impl ClientCore {
    pub(crate) fn image_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::Image { key: resource_id } = scope else {
            return;
        };
        let mut store = self.image_store.lock().expect("image store poisoned");
        let count = store.subscriptions.entry(resource_id.clone()).or_default();
        *count = if added {
            count.saturating_add(1)
        } else {
            count.saturating_sub(1)
        };
        let suspended = *count == 0;
        if suspended {
            store.subscriptions.remove(resource_id);
        }
        drop(store);
        if suspended {
            self.image_demand_changed(scope, crate::core::ClientDemand::Suspended);
        }
    }
    pub(crate) fn image_demand_changed(
        &self,
        scope: &ClientScope,
        demand: crate::core::ClientDemand,
    ) {
        let ClientScope::Image { key: resource_id } = scope else {
            return;
        };
        if demand != crate::core::ClientDemand::Suspended {
            return;
        }
        let mut store = self.image_store.lock().expect("image store poisoned");
        if let Some(request) = store.requests.remove(resource_id) {
            request.cancellation.cancel();
        }
        if store.publications.remove(resource_id) {
            let revision = self
                .snapshot(scope)
                .map_or(1, |p| p.revisions().scoped().get() + 1);
            self.publish(
                &ClientMutationAuthority { _private: () },
                scope.clone(),
                crate::threads::registry::revisions(revision),
                Arc::new(()),
                vec![],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct NoFetch;
    #[async_trait]
    impl ImageHttp for NoFetch {
        async fn execute(
            &self,
            _: GatewayHttpRequest,
            _: CancellationToken,
        ) -> Result<GatewayHttpResponse, GatewayHttpError> {
            panic!("routing tests must not perform HTTP");
        }
    }
    fn service() -> ImageCacheService {
        ImageCacheService {
            http: Arc::new(NoFetch),
            runtime_home: PathBuf::from("/synthetic-avatar-root"),
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
        }
    }
    #[test]
    fn live_identity_bound_and_last_subscriber_release_are_exact() {
        let core = Arc::new(ClientCore::new());
        let scope = ClientScope::Image {
            key: image_identity_key("p", "r"),
        };
        let first = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(2).unwrap());
        let second = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(2).unwrap());
        let token = CancellationToken::new();
        core.image_store
            .lock()
            .unwrap()
            .begin(&service(), 1, "p".into(), "r".into(), token.clone())
            .unwrap();
        drop(first);
        assert!(!token.is_cancelled());
        drop(second);
        assert!(token.is_cancelled());
        assert!(core.image_store.lock().unwrap().publications.is_empty());
        let mut store = ImageStore::default();
        for i in 0..IMAGE_CACHE_MAX_FILES {
            store
                .begin(
                    &service(),
                    1,
                    format!("p{i}"),
                    "r".into(),
                    CancellationToken::new(),
                )
                .unwrap();
        }
        assert_eq!(
            store.begin(
                &service(),
                1,
                "overflow".into(),
                "r".into(),
                CancellationToken::new()
            ),
            Err(ImageCacheError::Cancelled)
        );
        store.invalidate();
        assert!(store.requests.is_empty());
    }
    #[test]
    fn revisions_coexist_and_old_completion_cannot_replace_new_request() {
        let mut store = ImageStore::default();
        let service = service();
        let old_token = CancellationToken::new();
        let old = store
            .begin(
                &service,
                1,
                "principal".into(),
                "current".into(),
                old_token.clone(),
            )
            .unwrap();
        let current = store
            .begin(
                &service,
                1,
                "principal".into(),
                "current".into(),
                CancellationToken::new(),
            )
            .unwrap();
        let historical = store
            .begin(
                &service,
                1,
                "principal".into(),
                "history".into(),
                CancellationToken::new(),
            )
            .unwrap();
        assert!(old_token.is_cancelled());
        assert!(!store.finish(&old));
        assert!(store.finish(&historical));
        assert!(store.finish(&current));
        assert!(!store.finish(&current));
    }
    #[test]
    fn epoch_invalidation_cancels_and_clears_before_delivery() {
        let mut store = ImageStore::default();
        let token = CancellationToken::new();
        let old = store
            .begin(
                &service(),
                1,
                "principal".into(),
                "revision".into(),
                token.clone(),
            )
            .unwrap();
        store.invalidate();
        assert!(token.is_cancelled());
        assert!(store.publications.is_empty());
        assert!(!store.finish(&old));
        let current = store
            .begin(
                &service(),
                2,
                "principal".into(),
                "revision".into(),
                CancellationToken::new(),
            )
            .unwrap();
        assert_ne!(current.epoch, old.epoch);
        assert!(!store.finish(&old));
        assert!(store.finish(&current));
    }
}
