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
use pioneer_protocol::{MemberSummary, PrincipalId, ProfileAvatarMediaType};
use tokio_util::sync::CancellationToken;

use crate::app::root::PioneerDesktop;
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
pub(super) enum DesktopMemberAvatarStatus {
    Placeholder,
    Loading,
    Ready,
    Offline,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DesktopMemberAvatarPresentation {
    pub principal_id: PrincipalId,
    pub avatar_revision: Option<String>,
    pub cached_image_path: Option<PathBuf>,
    pub media_type: Option<ProfileAvatarMediaType>,
    pub status: DesktopMemberAvatarStatus,
}

pub(super) struct DesktopMemberAvatarState {
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
    pub(super) fn new(
        client: Arc<pioneer_client::core::ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        cx: &mut Context<PioneerDesktop>,
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
    pub(super) fn close(&mut self) {
        self.publications_task.take();
        self.clear();
    }
    pub(super) fn clear(&mut self) {
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

    pub(super) fn reconcile_visible_members(
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

    pub(super) fn reconcile_principal(
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
    pub(super) fn apply_result(&mut self, result: AvatarCacheResult) -> bool {
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

    pub(super) fn apply_error(
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

    pub(super) fn presentation(
        &self,
        principal_id: &PrincipalId,
    ) -> Option<&DesktopMemberAvatarPresentation> {
        self.visible
            .get(principal_id)
            .filter(|entry| self.protected_path_is_current(entry))
    }

    pub(super) fn reconcile_historical_revisions(
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

    pub(super) fn presentation_for_revision(
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

    pub(super) fn begin_agent_loading(&mut self, avatar_revision: &str) -> Option<u64> {
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
    pub(super) fn apply_agent_result(&mut self, generation: u64, result: AgentAvatarCacheResult) {
        if self.agent_request_generation != generation
            || !self.agent_loading.remove(result.avatar_revision.as_str())
        {
            return;
        }
        self.agent_cached_image_paths
            .insert(result.avatar_revision, result.local_path.into_path_buf());
    }

    #[cfg(test)]
    pub(super) fn apply_agent_error(&mut self, generation: u64, avatar_revision: &str) {
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
    pub(super) fn agent_cached_image_path(&self, avatar_revision: &str) -> Option<&Path> {
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

impl PioneerDesktop {
    pub(super) fn resolve_current_principal_avatar(&mut self, cx: &mut Context<Self>) {
        let Some(auth) = self.gateway.current_auth.as_ref() else {
            return;
        };
        let Some(request) = self.member_avatar_state.reconcile_principal(
            &auth.principal.id,
            auth.principal.avatar_revision.as_deref(),
        ) else {
            return;
        };
        self.resolve_member_avatar_requests(vec![request], cx);
    }

    pub(super) fn resolve_member_avatar_requests(
        &mut self,
        requests: Vec<AvatarCacheRequest>,
        cx: &mut Context<Self>,
    ) {
        self.member_avatar_state.prune_resources();
        if requests.is_empty() {
            return;
        }
        let Ok(client) = self.active_gateway_http_client() else {
            for request in requests {
                self.member_avatar_state.apply_error(
                    &request.principal_id,
                    request.avatar_revision.as_str(),
                    AvatarCacheError::Offline,
                );
            }
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

    pub(super) fn sync_timeline_avatar_demand(
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
        let Ok(client) = self.active_gateway_http_client() else {
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use pioneer_client::platform::ClientPath;
    use pioneer_protocol::{PrincipalKind, PrincipalStatus};

    use super::*;

    fn member(id: &str, revision: Option<&str>) -> MemberSummary {
        MemberSummary {
            principal_id: PrincipalId::new(id).unwrap(),
            kind: PrincipalKind::User,
            display_name: "Member".to_owned(),
            nickname: "member".to_owned(),
            role_key: Some(pioneer_protocol::RoleKey::member()),
            role: pioneer_protocol::AuthorizationRolePresentation {
                key: "member".to_owned(),
                display_name: "Member".to_owned(),
                description: "Workspace collaborator".to_owned(),
                built_in: true,
            },
            lifecycle_managed: true,
            status: PrincipalStatus::Active,
            avatar_revision: revision.map(str::to_owned),
        }
    }

    fn result(member: &MemberSummary, path: &str, source: AvatarCacheSource) -> AvatarCacheResult {
        AvatarCacheResult {
            local_path: ClientPath::new(PathBuf::from(path)),
            principal_id: member.principal_id.clone(),
            avatar_revision: member.avatar_revision.clone().unwrap(),
            media_type: ProfileAvatarMediaType::Png,
            source,
        }
    }

    #[::core::prelude::v1::test]
    fn visible_rows_plan_revision_requests_and_hidden_rows_drop_references() {
        let mut state = DesktopMemberAvatarState::default();
        let first = member("P0000000000000000000A", Some(&"a".repeat(64)));
        let second = member("P0000000000000000000B", None);
        let requests = state.reconcile_visible_members(&[first.clone(), second.clone()]);
        assert_eq!(requests.len(), 1);
        assert_eq!(
            state.presentation(&first.principal_id).unwrap().status,
            DesktopMemberAvatarStatus::Loading
        );
        assert_eq!(
            state.presentation(&second.principal_id).unwrap().status,
            DesktopMemberAvatarStatus::Placeholder
        );

        assert!(state.apply_result(result(
            &first,
            "/owned/cache/avatar",
            AvatarCacheSource::Revalidated,
        )));
        assert_eq!(
            state.presentation(&first.principal_id).unwrap().status,
            DesktopMemberAvatarStatus::Ready
        );
        state.reconcile_visible_members(&[second]);
        assert!(state.presentation(&first.principal_id).is_none());
    }

    #[::core::prelude::v1::test]
    fn revision_change_and_stale_completion_cannot_reuse_old_reference() {
        let mut state = DesktopMemberAvatarState::default();
        let old = member("P0000000000000000000A", Some(&"a".repeat(64)));
        state.reconcile_visible_members(std::slice::from_ref(&old));
        assert!(state.apply_result(result(
            &old,
            "/owned/cache/old",
            AvatarCacheSource::Downloaded,
        )));

        let changed = member("P0000000000000000000A", Some(&"b".repeat(64)));
        state.reconcile_visible_members(std::slice::from_ref(&changed));
        let presentation = state.presentation(&changed.principal_id).unwrap();
        assert_eq!(presentation.status, DesktopMemberAvatarStatus::Loading);
        assert!(presentation.cached_image_path.is_none());
        assert!(!state.apply_result(result(
            &old,
            "/owned/cache/stale",
            AvatarCacheSource::Downloaded,
        )));
    }

    #[::core::prelude::v1::test]
    fn current_principal_avatar_can_be_reconciled_without_a_directory_row() {
        let mut state = DesktopMemberAvatarState::default();
        let principal_id = PrincipalId::new("P0000000000000000000A").unwrap();
        let revision = "a".repeat(64);
        let request = state
            .reconcile_principal(&principal_id, Some(revision.as_str()))
            .expect("current principal avatar request");
        assert_eq!(request.principal_id, principal_id);
        assert_eq!(request.avatar_revision, revision);
        assert!(
            state
                .reconcile_principal(&principal_id, Some(request.avatar_revision.as_str()))
                .is_none(),
            "an in-flight immutable revision must not be requested twice"
        );
    }

    #[::core::prelude::v1::test]
    fn historical_and_current_revisions_can_coexist_for_one_principal() {
        let mut state = DesktopMemberAvatarState::default();
        let current = member("P0000000000000000000A", Some(&"b".repeat(64)));
        state.reconcile_visible_members(std::slice::from_ref(&current));
        assert!(state.apply_result(result(
            &current,
            "/owned/cache/current",
            AvatarCacheSource::Downloaded,
        )));

        let historical_revision = "a".repeat(64);
        let historical_requests = state.reconcile_historical_revisions(&[(
            current.principal_id.clone(),
            historical_revision.clone(),
        )]);
        assert_eq!(historical_requests.len(), 1);
        let historical = member("P0000000000000000000A", Some(historical_revision.as_str()));
        assert!(state.apply_result(result(
            &historical,
            "/owned/cache/historical",
            AvatarCacheSource::Downloaded,
        )));

        assert_eq!(
            state
                .presentation(&current.principal_id)
                .and_then(|avatar| avatar.cached_image_path.as_deref()),
            Some(Path::new("/owned/cache/current"))
        );
        assert_eq!(
            state
                .presentation_for_revision(&current.principal_id, &historical_revision)
                .and_then(|avatar| avatar.cached_image_path.as_deref()),
            Some(Path::new("/owned/cache/historical"))
        );
    }

    #[::core::prelude::v1::test]
    fn failed_visible_demand_does_not_retry_on_repaint_and_is_released_when_hidden() {
        let mut state = DesktopMemberAvatarState::default();
        let principal = PrincipalId::new("P0000000000000000000A").unwrap();
        let revision = "a".repeat(64);
        let visible = vec![(principal.clone(), revision.clone())];
        assert_eq!(state.reconcile_historical_revisions(&visible).len(), 1);
        state.apply_error(&principal, &revision, AvatarCacheError::Offline);
        for _ in 0..10 {
            assert!(state.reconcile_historical_revisions(&visible).is_empty());
        }
        state.reconcile_historical_revisions(&[]);
        state.prune_resources();
        assert!(state.attempted.is_empty());
        assert_eq!(state.reconcile_historical_revisions(&visible).len(), 1);
    }
    #[::core::prelude::v1::test]
    fn offline_and_hidden_failures_preserve_non_oracular_placeholder_behavior() {
        let mut state = DesktopMemberAvatarState::default();
        let member = member("P0000000000000000000A", Some(&"a".repeat(64)));
        state.reconcile_visible_members(std::slice::from_ref(&member));
        let revision = member.avatar_revision.as_deref().unwrap();
        assert!(state.apply_error(&member.principal_id, revision, AvatarCacheError::Offline));
        assert_eq!(
            state.presentation(&member.principal_id).unwrap().status,
            DesktopMemberAvatarStatus::Placeholder
        );
        assert!(state.apply_error(
            &member.principal_id,
            revision,
            AvatarCacheError::HiddenOrMissing,
        ));
        let presentation = state.presentation(&member.principal_id).unwrap();
        assert_eq!(presentation.status, DesktopMemberAvatarStatus::Placeholder);
        assert!(presentation.cached_image_path.is_none());
    }

    #[::core::prelude::v1::test]
    fn stale_agent_avatar_completion_cannot_cross_a_session_reset() {
        let mut state = DesktopMemberAvatarState::default();
        let revision = pioneer_protocol::PIONEER_AGENT_AVATAR_REVISION;
        let stale_generation = state.begin_agent_loading(revision).unwrap();
        state.clear();
        state.apply_agent_result(
            stale_generation,
            AgentAvatarCacheResult {
                local_path: ClientPath::new(PathBuf::from("/owned/cache/stale-agent")),
                avatar_revision: pioneer_protocol::PIONEER_AGENT_AVATAR_REVISION.to_owned(),
                media_type: ProfileAvatarMediaType::Jpeg,
                source: AvatarCacheSource::Downloaded,
            },
        );
        assert!(state.agent_cached_image_path(revision).is_none());

        let current_generation = state.begin_agent_loading(revision).unwrap();
        state.apply_agent_result(
            current_generation,
            AgentAvatarCacheResult {
                local_path: ClientPath::new(PathBuf::from("/owned/cache/current-agent")),
                avatar_revision: pioneer_protocol::PIONEER_AGENT_AVATAR_REVISION.to_owned(),
                media_type: ProfileAvatarMediaType::Jpeg,
                source: AvatarCacheSource::Downloaded,
            },
        );
        assert_eq!(
            state.agent_cached_image_path(revision),
            Some(Path::new("/owned/cache/current-agent"))
        );
    }

    #[::core::prelude::v1::test]
    fn desktop_avatar_source_has_no_legacy_rpc_or_data_url_path() {
        let source = include_str!("member_avatars.rs");
        let production_source = source
            .split_once("#[cfg(test)]")
            .map_or(source, |(production, _)| production);
        assert!(!production_source.contains(&["member", "_avatar_get"].concat()));
        assert!(!production_source.contains(&["content", "_base64"].concat()));
        assert!(!production_source.contains(&["data", ":image"].concat()));
        assert!(!production_source.contains(&["Author", "ization"].concat()));
    }
}
