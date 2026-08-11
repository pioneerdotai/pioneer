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
    AvatarCacheSource,
};
use pioneer_protocol::{MemberSummary, PrincipalId, ProfileAvatarMediaType};
use tokio_util::sync::CancellationToken;

use crate::app::root::PioneerDesktop;
use gpui::{AppContext as _, AsyncApp, Context, WeakEntity};

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
    agent_cached_image_path: Option<PathBuf>,
    agent_loading: bool,
    agent_request_generation: u64,
}

impl Default for DesktopMemberAvatarState {
    fn default() -> Self {
        Self {
            visible: HashMap::new(),
            historical: HashMap::new(),
            agent_cached_image_path: None,
            agent_loading: false,
            agent_request_generation: 0,
        }
    }
}

impl DesktopMemberAvatarState {
    pub(super) fn clear(&mut self) {
        self.visible.clear();
        self.historical.clear();
        self.agent_cached_image_path = None;
        self.agent_loading = false;
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
        entry.status = DesktopMemberAvatarStatus::Loading;
        Some(AvatarCacheRequest {
            principal_id: principal_id.clone(),
            avatar_revision: revision.to_owned(),
        })
    }

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
        self.visible.get(principal_id)
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
    }

    pub(super) fn begin_agent_loading(&mut self) -> Option<u64> {
        if self.agent_loading || self.agent_cached_image_path.is_some() {
            return None;
        }
        self.agent_loading = true;
        self.agent_request_generation = self.agent_request_generation.wrapping_add(1);
        Some(self.agent_request_generation)
    }

    pub(super) fn apply_agent_result(&mut self, generation: u64, result: AgentAvatarCacheResult) {
        if !self.agent_loading || self.agent_request_generation != generation {
            return;
        }
        self.agent_loading = false;
        self.agent_cached_image_path = Some(result.local_path.into_path_buf());
    }

    pub(super) fn apply_agent_error(&mut self, generation: u64) {
        if self.agent_request_generation != generation {
            return;
        }
        self.agent_loading = false;
    }

    pub(super) fn agent_cached_image_path(&self) -> Option<&Path> {
        self.agent_cached_image_path.as_deref()
    }
}

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
    match error {
        AvatarCacheError::Offline if entry.cached_image_path.is_some() => {
            entry.status = DesktopMemberAvatarStatus::Offline;
        }
        _ => {
            entry.cached_image_path = None;
            entry.media_type = None;
            entry.status = DesktopMemberAvatarStatus::Placeholder;
        }
    }
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
            let client = client.clone();
            cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
                let mut cx = cx.clone();
                async move {
                    let request_for_error = request.clone();
                    let result = cx
                        .background_spawn(async move {
                            client.resolve_member_avatar(request, CancellationToken::new())
                        })
                        .await;
                    let _ = this.update(&mut cx, |view, cx| {
                        match result {
                            Ok(result) => {
                                view.member_avatar_state.apply_result(result);
                            }
                            Err(error) => {
                                view.member_avatar_state.apply_error(
                                    &request_for_error.principal_id,
                                    request_for_error.avatar_revision.as_str(),
                                    error,
                                );
                            }
                        }
                        cx.notify();
                    });
                }
            })
            .detach();
        }
    }

    pub(super) fn resolve_agent_avatar(&mut self, cx: &mut Context<Self>) {
        let Some(generation) = self.member_avatar_state.begin_agent_loading() else {
            return;
        };
        let Ok(client) = self.active_gateway_http_client() else {
            self.member_avatar_state.apply_agent_error(generation);
            return;
        };
        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        client.resolve_agent_avatar(CancellationToken::new())
                    })
                    .await;
                let _ = this.update(&mut cx, |view, cx| {
                    match result {
                        Ok(result) => view
                            .member_avatar_state
                            .apply_agent_result(generation, result),
                        Err(_) => view.member_avatar_state.apply_agent_error(generation),
                    }
                    cx.notify();
                });
            }
        })
        .detach();
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
        let stale_generation = state.begin_agent_loading().unwrap();
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
        assert!(state.agent_cached_image_path().is_none());

        let current_generation = state.begin_agent_loading().unwrap();
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
            state.agent_cached_image_path(),
            Some(Path::new("/owned/cache/current-agent"))
        );
    }

    #[::core::prelude::v1::test]
    fn desktop_avatar_source_has_no_legacy_rpc_or_data_url_path() {
        let source = include_str!("member_avatars.rs");
        assert!(!source.contains(&["member", "_avatar_get"].concat()));
        assert!(!source.contains(&["content", "_base64"].concat()));
        assert!(!source.contains(&["data", ":image"].concat()));
        assert!(!source.contains(&["Author", "ization"].concat()));
    }
}
