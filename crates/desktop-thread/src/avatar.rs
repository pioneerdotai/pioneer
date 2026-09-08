//! Desktop member-directory avatar presentation state.
//!
//! HTTP, credentials and cache ownership stay in `pioneer-client`. This state
//! only reconciles already-visible member-directory rows with secret-free
//! native cache references.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use pioneer_client::avatars::{
    AgentAvatarCacheResult, AvatarCacheError, AvatarCacheRequest, AvatarCacheResult,
    AvatarCacheSource, AvatarPublication,
};
use pioneer_client::avatars::{MemberSummary, PrincipalId, ProfileAvatarMediaType};
use tokio_util::sync::CancellationToken;

use crate::screen::ThreadScreenView;
pub(crate) struct ThreadAvatarChanged;
impl gpui_kit::EventEmitter<ThreadAvatarChanged> for ThreadScreenView {}
use gpui_kit::{AppContext as _, AsyncApp, Context, WeakEntity};
use pioneer_client::core::{ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, sync::Arc};

struct AvatarBinding {
    publications: RefCell<HashMap<String, ClientPublicationReference>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl Default for AvatarBinding {
    fn default() -> Self {
        Self {
            publications: RefCell::default(),
            changed: tokio::sync::watch::channel(0).0,
        }
    }
}
impl ClientPublicationSink for AvatarBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        let ClientScope::Avatar { principal_id } = publication.scope() else {
            return;
        };
        let mut inputs = self.publications.borrow_mut();
        if inputs.get(principal_id).is_some_and(|previous| {
            previous.revisions().scoped() >= publication.revisions().scoped()
        }) {
            return;
        }
        inputs.insert(principal_id.clone(), publication);
        self.changed.send_modify(|serial| *serial += 1);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DesktopMemberAvatarStatus {
    Placeholder,
    Loading,
    Ready,
    Offline,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DesktopMemberAvatarPresentation {
    pub principal_id: PrincipalId,
    pub avatar_revision: Option<String>,
    pub cached_image_path: Option<PathBuf>,
    pub media_type: Option<ProfileAvatarMediaType>,
    pub status: DesktopMemberAvatarStatus,
}

pub(crate) struct DesktopMemberAvatarState {
    visible: HashMap<PrincipalId, DesktopMemberAvatarPresentation>,
    historical: HashMap<(PrincipalId, String), DesktopMemberAvatarPresentation>,
    agent_cached_image_paths: HashMap<String, PathBuf>,
    agent_loading: HashSet<String>,
    attempted: HashSet<String>,
    agent_request_generation: u64,
    requests: HashMap<String, (CancellationToken, gpui_kit::Task<()>)>,
    binding: Arc<AvatarBinding>,
    client: Option<Arc<pioneer_client::core::ClientCore>>,
    registrar: Option<Arc<dyn ClientBindingRegistrar>>,
    registrations: HashMap<String, ClientBindingRegistration>,
    publications_task: Option<gpui_kit::Task<()>>,
}

impl Default for DesktopMemberAvatarState {
    fn default() -> Self {
        Self {
            visible: HashMap::new(),
            historical: HashMap::new(),
            agent_cached_image_paths: HashMap::new(),
            agent_loading: HashSet::new(),
            attempted: HashSet::new(),
            agent_request_generation: 0,
            requests: HashMap::new(),
            binding: Arc::default(),
            client: None,
            registrar: None,
            registrations: HashMap::new(),
            publications_task: None,
        }
    }
}

impl DesktopMemberAvatarState {
    pub(crate) fn new(
        client: Arc<pioneer_client::core::ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        cx: &mut Context<ThreadScreenView>,
    ) -> Self {
        let mut state = Self::default();
        state.registrar = Some(registrar);
        state.client = Some(client);
        let mut changes = state.binding.changed.subscribe();
        state.publications_task = Some(cx.spawn(async move |view, cx| {
            while changes.changed().await.is_ok() {
                if view
                    .update(cx, |view, cx| {
                        if view.member_avatar_state.apply_publications() {
                            cx.emit(ThreadAvatarChanged);
                            cx.notify();
                        }
                    })
                    .is_err()
                {
                    break;
                }
            }
        }));
        state
    }
    fn register(&mut self, key: &str) {
        if self.registrations.contains_key(key) {
            return;
        }
        let Some(registrar) = &self.registrar else {
            return;
        };
        let sink: Arc<dyn ClientPublicationSink> = self.binding.clone();
        self.registrations.insert(
            key.to_owned(),
            registrar.register(
                ClientScope::Avatar {
                    principal_id: key.to_owned(),
                },
                Arc::downgrade(&sink),
            ),
        );
    }
    fn apply_publications(&mut self) -> bool {
        let inputs = self.binding.publications.borrow().clone();
        let mut changed = false;
        for (key, input) in inputs {
            if !self.registrations.contains_key(&key) {
                continue;
            }
            let publication = input.snapshot().payload::<AvatarPublication>();
            for entry in self
                .visible
                .values_mut()
                .chain(self.historical.values_mut())
            {
                let Some(revision) = &entry.avatar_revision else {
                    continue;
                };
                if pioneer_client::avatars::avatar_identity_key(
                    entry.principal_id.as_str(),
                    revision,
                ) != key
                {
                    continue;
                }
                let before = entry.clone();
                if let Some(publication) = &publication {
                    entry.cached_image_path = publication
                        .local_path()
                        .map(|path| path.as_path().to_path_buf());
                    entry.media_type = publication.media_type();
                    entry.status = if entry.cached_image_path.is_none() {
                        DesktopMemberAvatarStatus::Placeholder
                    } else if publication.source() == Some(AvatarCacheSource::OfflineCache) {
                        DesktopMemberAvatarStatus::Offline
                    } else {
                        DesktopMemberAvatarStatus::Ready
                    };
                } else {
                    entry.cached_image_path = None;
                    entry.media_type = None;
                    entry.status = DesktopMemberAvatarStatus::Placeholder;
                }
                changed |= before != *entry;
            }
            if let Some(publication) = publication {
                if let Some(revision) = publication.principal_id().strip_prefix("agent:") {
                    let previous = self.agent_cached_image_paths.remove(revision);
                    let next = publication
                        .local_path()
                        .map(|path| path.as_path().to_path_buf());
                    changed |= previous != next;
                    if let Some(path) = next {
                        self.agent_cached_image_paths
                            .insert(revision.to_owned(), path);
                    }
                }
            } else {
                let revisions = self
                    .agent_cached_image_paths
                    .keys()
                    .filter(|revision| {
                        pioneer_client::avatars::avatar_identity_key(
                            &format!("agent:{revision}"),
                            revision,
                        ) == key
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                for revision in revisions {
                    changed |= self.agent_cached_image_paths.remove(&revision).is_some();
                }
            }
        }
        changed
    }
    fn prune_resources(&mut self) {
        let mut keys = self
            .visible
            .values()
            .chain(self.historical.values())
            .filter_map(|entry| {
                entry.avatar_revision.as_ref().map(|revision| {
                    pioneer_client::avatars::avatar_identity_key(
                        entry.principal_id.as_str(),
                        revision,
                    )
                })
            })
            .collect::<HashSet<_>>();
        for revision in self
            .agent_loading
            .iter()
            .chain(self.agent_cached_image_paths.keys())
        {
            keys.insert(pioneer_client::avatars::avatar_identity_key(
                &format!("agent:{revision}"),
                revision,
            ));
        }
        self.attempted.retain(|key| keys.contains(key));
        self.registrations.retain(|key, _| keys.contains(key));
        self.binding
            .publications
            .borrow_mut()
            .retain(|key, _| keys.contains(key));
        self.requests.retain(|key, (cancellation, _)| {
            if keys.contains(key) {
                true
            } else {
                cancellation.cancel();
                false
            }
        });
    }
    pub(crate) fn close(&mut self) {
        self.publications_task.take();
        self.clear();
    }
    pub(crate) fn clear(&mut self) {
        for (_, (cancellation, _)) in self.requests.drain() {
            cancellation.cancel();
        }
        self.registrations.clear();
        self.binding.publications.borrow_mut().clear();
        self.visible.clear();
        self.historical.clear();
        self.agent_cached_image_paths.clear();
        self.agent_loading.clear();
        self.attempted.clear();
        self.agent_request_generation = self.agent_request_generation.wrapping_add(1);
    }

    pub(crate) fn reconcile_visible_members(
        &mut self,
        members: &[MemberSummary],
    ) -> Vec<AvatarCacheRequest> {
        let visible_ids = members
            .iter()
            .map(|member| member.principal_id.clone())
            .collect::<HashSet<_>>();
        self.visible
            .retain(|principal_id, _| visible_ids.contains(principal_id));

        members
            .iter()
            .filter_map(|member| {
                self.reconcile_principal(&member.principal_id, member.avatar_revision.as_deref())
            })
            .collect()
    }

    pub(crate) fn reconcile_principal(
        &mut self,
        principal_id: &PrincipalId,
        avatar_revision: Option<&str>,
    ) -> Option<AvatarCacheRequest> {
        let Some(revision) = avatar_revision else {
            self.visible.insert(
                principal_id.clone(),
                DesktopMemberAvatarPresentation {
                    principal_id: principal_id.clone(),
                    avatar_revision: None,
                    cached_image_path: None,
                    media_type: None,
                    status: DesktopMemberAvatarStatus::Placeholder,
                },
            );
            return None;
        };

        let entry = self.visible.entry(principal_id.clone()).or_insert_with(|| {
            DesktopMemberAvatarPresentation {
                principal_id: principal_id.clone(),
                avatar_revision: Some(revision.to_owned()),
                cached_image_path: None,
                media_type: None,
                status: DesktopMemberAvatarStatus::Placeholder,
            }
        });
        let should_resolve = if entry.avatar_revision.as_deref() != Some(revision) {
            entry.avatar_revision = Some(revision.to_owned());
            entry.cached_image_path = None;
            entry.media_type = None;
            true
        } else {
            entry.cached_image_path.is_none() && entry.status != DesktopMemberAvatarStatus::Loading
        };
        if !should_resolve {
            return None;
        }
        if !self
            .attempted
            .insert(pioneer_client::avatars::avatar_identity_key(
                principal_id.as_str(),
                revision,
            ))
        {
            return None;
        }
        entry.status = DesktopMemberAvatarStatus::Loading;
        Some(AvatarCacheRequest {
            principal_id: principal_id.clone(),
            avatar_revision: revision.to_owned(),
        })
    }

    #[cfg(test)]
    pub(crate) fn apply_result(&mut self, result: AvatarCacheResult) -> bool {
        let status = if result.source == AvatarCacheSource::OfflineCache {
            DesktopMemberAvatarStatus::Offline
        } else {
            DesktopMemberAvatarStatus::Ready
        };
        let path = result.local_path.into_path_buf();
        let mut applied = false;
        if let Some(entry) = self.visible.get_mut(&result.principal_id)
            && entry.avatar_revision.as_deref() == Some(result.avatar_revision.as_str())
        {
            apply_presentation_result(entry, path.clone(), result.media_type, status);
            applied = true;
        }
        if let Some(entry) = self
            .historical
            .get_mut(&(result.principal_id, result.avatar_revision))
        {
            apply_presentation_result(entry, path, result.media_type, status);
            applied = true;
        }
        applied
    }

    pub(crate) fn apply_error(
        &mut self,
        principal_id: &PrincipalId,
        avatar_revision: &str,
        error: AvatarCacheError,
    ) -> bool {
        let mut applied = false;
        if let Some(entry) = self.visible.get_mut(principal_id)
            && entry.avatar_revision.as_deref() == Some(avatar_revision)
        {
            apply_presentation_error(entry, error);
            applied = true;
        }
        if let Some(entry) = self
            .historical
            .get_mut(&(principal_id.clone(), avatar_revision.to_owned()))
        {
            apply_presentation_error(entry, error);
            applied = true;
        }
        applied
    }

    pub(crate) fn presentation(
        &self,
        principal_id: &PrincipalId,
    ) -> Option<&DesktopMemberAvatarPresentation> {
        self.visible
            .get(principal_id)
            .filter(|entry| self.protected_path_is_current(entry))
    }

    pub(crate) fn reconcile_historical_revisions(
        &mut self,
        revisions: &[(PrincipalId, String)],
    ) -> Vec<AvatarCacheRequest> {
        let active = revisions.iter().cloned().collect::<HashSet<_>>();
        self.historical.retain(|key, _| active.contains(key));
        active
            .into_iter()
            .filter_map(|(principal_id, revision)| {
                if self.visible.get(&principal_id).is_some_and(|entry| {
                    entry.avatar_revision.as_deref() == Some(revision.as_str())
                }) {
                    return None;
                }
                let entry = self
                    .historical
                    .entry((principal_id.clone(), revision.clone()))
                    .or_insert_with(|| DesktopMemberAvatarPresentation {
                        principal_id: principal_id.clone(),
                        avatar_revision: Some(revision.clone()),
                        cached_image_path: None,
                        media_type: None,
                        status: DesktopMemberAvatarStatus::Placeholder,
                    });
                if entry.cached_image_path.is_some()
                    || entry.status == DesktopMemberAvatarStatus::Loading
                {
                    return None;
                }
                if !self
                    .attempted
                    .insert(pioneer_client::avatars::avatar_identity_key(
                        principal_id.as_str(),
                        &revision,
                    ))
                {
                    return None;
                }
                entry.status = DesktopMemberAvatarStatus::Loading;
                Some(AvatarCacheRequest {
                    principal_id,
                    avatar_revision: revision,
                })
            })
            .collect()
    }

    pub(crate) fn presentation_for_revision(
        &self,
        principal_id: &PrincipalId,
        avatar_revision: &str,
    ) -> Option<&DesktopMemberAvatarPresentation> {
        self.visible
            .get(principal_id)
            .filter(|entry| entry.avatar_revision.as_deref() == Some(avatar_revision))
            .or_else(|| {
                self.historical
                    .get(&(principal_id.clone(), avatar_revision.to_owned()))
            })
            .filter(|entry| self.protected_path_is_current(entry))
    }

    pub(crate) fn begin_agent_loading(&mut self, avatar_revision: &str) -> Option<u64> {
        if self.agent_loading.contains(avatar_revision)
            || self.agent_cached_image_paths.contains_key(avatar_revision)
        {
            return None;
        }
        if !self
            .attempted
            .insert(pioneer_client::avatars::avatar_identity_key(
                &format!("agent:{avatar_revision}"),
                avatar_revision,
            ))
        {
            return None;
        }
        self.agent_loading.insert(avatar_revision.to_owned());
        Some(self.agent_request_generation)
    }

    #[cfg(test)]
    pub(crate) fn apply_agent_result(&mut self, generation: u64, result: AgentAvatarCacheResult) {
        if self.agent_request_generation != generation
            || !self.agent_loading.remove(result.avatar_revision.as_str())
        {
            return;
        }
        self.agent_cached_image_paths
            .insert(result.avatar_revision, result.local_path.into_path_buf());
    }

    #[cfg(test)]
    pub(crate) fn apply_agent_error(&mut self, generation: u64, avatar_revision: &str) {
        if self.agent_request_generation != generation {
            return;
        }
        self.agent_loading.remove(avatar_revision);
    }

    fn protected_path_is_current(&self, entry: &DesktopMemberAvatarPresentation) -> bool {
        let (Some(revision), Some(path)) = (&entry.avatar_revision, &entry.cached_image_path)
        else {
            return true;
        };
        self.path_is_current(
            &pioneer_client::avatars::avatar_identity_key(entry.principal_id.as_str(), revision),
            path,
        )
    }
    fn path_is_current(&self, key: &str, path: &Path) -> bool {
        let Some(client) = &self.client else {
            return cfg!(test);
        };
        client
            .snapshot(&ClientScope::Avatar {
                principal_id: key.to_owned(),
            })
            .and_then(|snapshot| snapshot.snapshot().payload::<AvatarPublication>())
            .is_some_and(|publication| {
                publication
                    .local_path()
                    .is_some_and(|current| current.as_path() == path)
            })
    }
    pub(crate) fn agent_cached_image_path(&self, avatar_revision: &str) -> Option<&Path> {
        self.agent_cached_image_paths
            .get(avatar_revision)
            .filter(|path| {
                self.path_is_current(
                    &pioneer_client::avatars::avatar_identity_key(
                        &format!("agent:{avatar_revision}"),
                        avatar_revision,
                    ),
                    path,
                )
            })
            .map(PathBuf::as_path)
    }
}

#[cfg(test)]
fn apply_presentation_result(
    entry: &mut DesktopMemberAvatarPresentation,
    path: PathBuf,
    media_type: ProfileAvatarMediaType,
    status: DesktopMemberAvatarStatus,
) {
    entry.cached_image_path = Some(path);
    entry.media_type = Some(media_type);
    entry.status = status;
}

fn apply_presentation_error(entry: &mut DesktopMemberAvatarPresentation, error: AvatarCacheError) {
    let _ = error;
    entry.cached_image_path = None;
    entry.media_type = None;
    entry.status = DesktopMemberAvatarStatus::Placeholder;
}

impl ThreadScreenView {
    pub(crate) fn resolve_member_avatar_requests(
        &mut self,
        requests: Vec<AvatarCacheRequest>,
        cx: &mut Context<Self>,
    ) {
        self.member_avatar_state.prune_resources();
        if requests.is_empty() {
            return;
        }
        let Ok(client) = self.avatar_client() else {
            for request in requests {
                self.member_avatar_state.apply_error(
                    &request.principal_id,
                    request.avatar_revision.as_str(),
                    AvatarCacheError::Offline,
                );
            }
            cx.emit(ThreadAvatarChanged);
            cx.notify();
            return;
        };
        for request in requests {
            let key = pioneer_client::avatars::avatar_identity_key(
                request.principal_id.as_str(),
                &request.avatar_revision,
            );
            self.member_avatar_state.register(&key);
            let completion_key = key.clone();
            let cancellation = CancellationToken::new();
            let task_cancellation = cancellation.clone();
            let generation = self.member_avatar_state.agent_request_generation;
            let client = client.clone();
            let task = cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                let mut cx = cx.clone();
                async move {
                    let _ = cx
                        .background_spawn(async move {
                            client.resolve_member_avatar(request, task_cancellation)
                        })
                        .await;
                    let _ = this.update(&mut cx, |view, _| {
                        if generation == view.member_avatar_state.agent_request_generation {
                            view.member_avatar_state.requests.remove(&completion_key);
                        }
                    });
                }
            });
            if let Some((previous, _)) = self
                .member_avatar_state
                .requests
                .insert(key, (cancellation, task))
            {
                previous.cancel();
            }
        }
    }

    pub(crate) fn sync_timeline_avatar_demand(
        &mut self,
        members: Vec<(PrincipalId, String)>,
        agents: Vec<String>,
        cx: &mut Context<Self>,
    ) {
        let requests = self
            .member_avatar_state
            .reconcile_historical_revisions(&members);
        self.member_avatar_state
            .agent_loading
            .retain(|revision| agents.contains(revision));
        self.member_avatar_state
            .agent_cached_image_paths
            .retain(|revision, _| agents.contains(revision));
        self.resolve_member_avatar_requests(requests, cx);
        self.resolve_agent_avatar_revisions(&agents, cx);
    }
    fn resolve_agent_avatar_revisions(&mut self, revisions: &[String], cx: &mut Context<Self>) {
        let Ok(client) = self.avatar_client() else {
            return;
        };
        for avatar_revision in revisions {
            let Some(generation) = self
                .member_avatar_state
                .begin_agent_loading(avatar_revision)
            else {
                continue;
            };
            let client = client.clone();
            let avatar_revision = avatar_revision.to_owned();
            let key = pioneer_client::avatars::avatar_identity_key(
                &format!("agent:{avatar_revision}"),
                &avatar_revision,
            );
            self.member_avatar_state.register(&key);
            let completion_key = key.clone();
            let cancellation = CancellationToken::new();
            let task_cancellation = cancellation.clone();
            let task = cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                let mut cx = cx.clone();
                async move {
                    let _ = cx
                        .background_spawn(async move {
                            client.resolve_agent_avatar(avatar_revision, task_cancellation)
                        })
                        .await;
                    let _ = this.update(&mut cx, |view, _| {
                        if generation == view.member_avatar_state.agent_request_generation {
                            view.member_avatar_state.requests.remove(&completion_key);
                        }
                    });
                }
            });
            if let Some((previous, _)) = self
                .member_avatar_state
                .requests
                .insert(key, (cancellation, task))
            {
                previous.cancel();
            }
        }
    }
}
impl Drop for DesktopMemberAvatarState {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Uses the existing Client avatar service and publication owner.
#[derive(Clone)]
pub(crate) struct ThreadAvatarClient {
    client: Arc<pioneer_client::core::ClientCore>,
    service: pioneer_client::avatars::AvatarCacheService,
    runtime: Arc<tokio::runtime::Runtime>,
}
impl ThreadAvatarClient {
    pub(crate) fn new(
        client: Arc<pioneer_client::core::ClientCore>,
        runtime_home: PathBuf,
    ) -> Result<Self, AvatarCacheError> {
        let service = client.avatar_cache_service(runtime_home)?;
        let runtime =
            Arc::new(tokio::runtime::Runtime::new().map_err(|_| AvatarCacheError::Offline)?);
        Ok(Self {
            client,
            service,
            runtime,
        })
    }
    fn resolve_member_avatar(
        &self,
        request: AvatarCacheRequest,
        cancellation: CancellationToken,
    ) -> Result<AvatarCacheResult, AvatarCacheError> {
        self.runtime.block_on(self.client.resolve_member_avatar(
            &self.service,
            request,
            cancellation,
        ))
    }
    fn resolve_agent_avatar(
        &self,
        revision: String,
        cancellation: CancellationToken,
    ) -> Result<AgentAvatarCacheResult, AvatarCacheError> {
        self.runtime.block_on(self.client.resolve_agent_avatar(
            &self.service,
            revision,
            cancellation,
        ))
    }
}
impl ThreadScreenView {
    fn avatar_client(&self) -> Result<ThreadAvatarClient, AvatarCacheError> {
        self.avatar_http.clone().ok_or(AvatarCacheError::Offline)
    }
}
