//! Session-fenced avatar fetches and immutable authenticated cache publications.
use super::*;
use crate::core::{ClientCore, ClientMutationAuthority, ClientScope};
use serde::{Deserialize, Serialize};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AvatarPublication {
    principal_id: String,
    avatar_revision: String,
    local_path: Option<ClientPath>,
    media_type: Option<ProfileAvatarMediaType>,
    error: Option<AvatarCacheError>,
    source: Option<AvatarCacheSource>,
}
impl AvatarPublication {
    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }
    pub fn avatar_revision(&self) -> &str {
        &self.avatar_revision
    }
    pub fn local_path(&self) -> Option<&ClientPath> {
        self.local_path.as_ref()
    }
    pub fn media_type(&self) -> Option<ProfileAvatarMediaType> {
        self.media_type
    }
    pub fn source(&self) -> Option<AvatarCacheSource> {
        self.source
    }
    pub fn error(&self) -> Option<AvatarCacheError> {
        self.error
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct FetchIdentity {
    gateway: GatewayId,
    session: AuthSessionId,
    authorization: u64,
    principal: String,
    revision: String,
    generation: u64,
    epoch: u64,
}
struct Fetch {
    identity: FetchIdentity,
    cancellation: CancellationToken,
}
#[derive(Default)]
pub struct AvatarStore {
    epoch: u64,
    generation: u64,
    requests: HashMap<String, Fetch>,
    publications: std::collections::HashSet<String>,
    subscriptions: HashMap<String, usize>,
    cache_roots: std::collections::HashSet<(PathBuf, GatewayId)>,
}
impl AvatarStore {
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
                &avatar_cache_root(&runtime_home).join(gateway_id.as_str()),
            );
        }
    }
    fn begin(
        &mut self,
        service: &AvatarCacheService,
        authorization: u64,
        principal: String,
        revision: String,
        cancellation: CancellationToken,
    ) -> Result<FetchIdentity, AvatarCacheError> {
        let key = avatar_identity_key(&principal, &revision);
        if let Some(previous) = self.requests.remove(&key) {
            previous.cancellation.cancel();
        }
        if !self.publications.contains(&key) && self.publications.len() >= AVATAR_CACHE_MAX_FILES {
            return Err(AvatarCacheError::Cancelled);
        }
        self.publications.insert(key.clone());
        self.generation += 1;
        let identity = FetchIdentity {
            gateway: service.gateway_id.clone(),
            session: service.session_id.clone(),
            authorization,
            principal: principal.clone(),
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
                .get(&avatar_identity_key(
                    &identity.principal,
                    &identity.revision,
                ))
                .is_none_or(|request| {
                    request.identity != *identity || request.cancellation.is_cancelled()
                })
        {
            return false;
        }
        self.requests.remove(&avatar_identity_key(
            &identity.principal,
            &identity.revision,
        ));
        true
    }
}
impl Drop for AvatarStore {
    fn drop(&mut self) {
        self.invalidate();
    }
}
impl ClientCore {
    pub async fn resolve_member_avatar(
        &self,
        service: &AvatarCacheService,
        request: AvatarCacheRequest,
        cancellation: CancellationToken,
    ) -> Result<AvatarCacheResult, AvatarCacheError> {
        let identity = self.begin_avatar_fetch(
            service,
            request.principal_id.to_string(),
            request.avatar_revision.clone(),
            cancellation.clone(),
        )?;
        let result = service.resolve(request, cancellation).await;
        let publication = match &result {
            Ok(result) => AvatarPublication {
                principal_id: identity.principal.clone(),
                avatar_revision: identity.revision.clone(),
                local_path: Some(result.local_path.clone()),
                media_type: Some(result.media_type),
                error: None,
                source: Some(result.source),
            },
            Err(error) => AvatarPublication {
                principal_id: identity.principal.clone(),
                avatar_revision: identity.revision.clone(),
                local_path: None,
                media_type: None,
                error: Some(*error),
                source: None,
            },
        };
        self.finish_avatar_fetch(&identity, publication)?;
        result
    }
    pub async fn resolve_agent_avatar(
        &self,
        service: &AvatarCacheService,
        revision: String,
        cancellation: CancellationToken,
    ) -> Result<AgentAvatarCacheResult, AvatarCacheError> {
        let identity = self.begin_avatar_fetch(
            service,
            format!("agent:{revision}"),
            revision.clone(),
            cancellation.clone(),
        )?;
        let result = service.resolve_agent_avatar(revision, cancellation).await;
        let publication = match &result {
            Ok(result) => AvatarPublication {
                principal_id: identity.principal.clone(),
                avatar_revision: identity.revision.clone(),
                local_path: Some(result.local_path.clone()),
                media_type: Some(result.media_type),
                error: None,
                source: Some(result.source),
            },
            Err(error) => AvatarPublication {
                principal_id: identity.principal.clone(),
                avatar_revision: identity.revision.clone(),
                local_path: None,
                media_type: None,
                error: Some(*error),
                source: None,
            },
        };
        self.finish_avatar_fetch(&identity, publication)?;
        result
    }
    fn begin_avatar_fetch(
        &self,
        service: &AvatarCacheService,
        principal: String,
        revision: String,
        cancellation: CancellationToken,
    ) -> Result<FetchIdentity, AvatarCacheError> {
        if self.is_stopped() {
            return Err(AvatarCacheError::Cancelled);
        }
        let access = self
            .compatibility_runtime()
            .ws_command_sender()
            .current_gateway_http_access()
            .map_err(|_| AvatarCacheError::Authentication)?;
        if access.gateway_id != service.gateway_id || access.session_id != service.session_id {
            return Err(AvatarCacheError::Authentication);
        }
        let authorization = self.authorization_connection_generation();
        let mut store = self.avatar_store.lock().expect("avatar store poisoned");
        store
            .cache_roots
            .insert((service.runtime_home.clone(), service.gateway_id.clone()));
        store.begin(service, authorization, principal, revision, cancellation)
    }
    fn finish_avatar_fetch(
        &self,
        identity: &FetchIdentity,
        publication: AvatarPublication,
    ) -> Result<(), AvatarCacheError> {
        let access = self
            .compatibility_runtime()
            .ws_command_sender()
            .current_gateway_http_access()
            .map_err(|_| AvatarCacheError::Authentication)?;
        if self.is_stopped()
            || access.gateway_id != identity.gateway
            || access.session_id != identity.session
            || self.authorization_connection_generation() != identity.authorization
        {
            return Err(AvatarCacheError::Cancelled);
        }
        let mut store = self.avatar_store.lock().expect("avatar store poisoned");
        if !store.finish(identity) {
            return Err(AvatarCacheError::Cancelled);
        }
        let scope = ClientScope::Avatar {
            principal_id: avatar_identity_key(&identity.principal, &identity.revision),
        };
        let previous = self.snapshot(&scope);
        if previous
            .as_ref()
            .and_then(|p| p.snapshot().payload::<AvatarPublication>())
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
pub fn avatar_identity_key(principal: &str, revision: &str) -> String {
    format!("{principal}:{revision}")
}
impl ClientCore {
    pub(crate) fn avatar_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::Avatar { principal_id } = scope else {
            return;
        };
        let mut store = self.avatar_store.lock().expect("avatar store poisoned");
        let count = store.subscriptions.entry(principal_id.clone()).or_default();
        *count = if added {
            count.saturating_add(1)
        } else {
            count.saturating_sub(1)
        };
        let suspended = *count == 0;
        if suspended {
            store.subscriptions.remove(principal_id);
        }
        drop(store);
        if suspended {
            self.avatar_demand_changed(scope, crate::core::ClientDemand::Suspended);
        }
    }
    pub(crate) fn avatar_demand_changed(
        &self,
        scope: &ClientScope,
        demand: crate::core::ClientDemand,
    ) {
        let ClientScope::Avatar { principal_id } = scope else {
            return;
        };
        if demand != crate::core::ClientDemand::Suspended {
            return;
        }
        let mut store = self.avatar_store.lock().expect("avatar store poisoned");
        if let Some(request) = store.requests.remove(principal_id) {
            request.cancellation.cancel();
        }
        if store.publications.remove(principal_id) {
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
    impl AvatarHttp for NoFetch {
        async fn execute(
            &self,
            _: GatewayHttpRequest,
            _: CancellationToken,
        ) -> Result<GatewayHttpResponse, GatewayHttpError> {
            panic!("routing tests must not perform HTTP");
        }
    }
    fn service() -> AvatarCacheService {
        AvatarCacheService {
            http: Arc::new(NoFetch),
            runtime_home: PathBuf::from("/synthetic-avatar-root"),
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
        }
    }
    #[test]
    fn live_identity_bound_and_last_subscriber_release_are_exact() {
        let core = Arc::new(ClientCore::new());
        let scope = ClientScope::Avatar {
            principal_id: avatar_identity_key("p", "r"),
        };
        let first = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(2).unwrap());
        let second = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(2).unwrap());
        let token = CancellationToken::new();
        core.avatar_store
            .lock()
            .unwrap()
            .begin(&service(), 1, "p".into(), "r".into(), token.clone())
            .unwrap();
        drop(first);
        assert!(!token.is_cancelled());
        drop(second);
        assert!(token.is_cancelled());
        assert!(core.avatar_store.lock().unwrap().publications.is_empty());
        let mut store = AvatarStore::default();
        for i in 0..AVATAR_CACHE_MAX_FILES {
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
            Err(AvatarCacheError::Cancelled)
        );
        store.invalidate();
        assert!(store.requests.is_empty());
    }
    #[test]
    fn revisions_coexist_and_old_completion_cannot_replace_new_request() {
        let mut store = AvatarStore::default();
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
        let mut store = AvatarStore::default();
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
