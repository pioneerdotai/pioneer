//! Desktop member-directory avatar presentation state.
//!
//! HTTP, credentials and cache ownership stay in `pioneer-client`. This state
//! only reconciles already-visible member-directory rows with secret-free
//! native cache references.

use std::collections::{HashMap, HashSet};

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
    pub cached_image_path: Option<String>,
    pub media_type: Option<ProfileAvatarMediaType>,
    pub status: DesktopMemberAvatarStatus,
}

pub(super) struct DesktopMemberAvatarState {
    visible: HashMap<PrincipalId, DesktopMemberAvatarPresentation>,
    agent_cached_image_path: Option<String>,
    agent_loading: bool,
    agent_request_generation: u64,
}

impl Default for DesktopMemberAvatarState {
    fn default() -> Self {
        Self {
            visible: HashMap::new(),
            agent_cached_image_path: None,
            agent_loading: false,
            agent_request_generation: 0,
        }
    }
}

impl DesktopMemberAvatarState {
    pub(super) fn clear(&mut self) {
        self.visible.clear();
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

        let mut requests = Vec::new();
        for member in members {
            match member.avatar_revision.as_deref() {
                None => {
                    self.visible.insert(
                        member.principal_id.clone(),
                        DesktopMemberAvatarPresentation {
                            principal_id: member.principal_id.clone(),
                            avatar_revision: None,
                            cached_image_path: None,
                            media_type: None,
                            status: DesktopMemberAvatarStatus::Placeholder,
                        },
                    );
                }
                Some(revision) => {
                    let entry = self
                        .visible
                        .entry(member.principal_id.clone())
                        .or_insert_with(|| DesktopMemberAvatarPresentation {
                            principal_id: member.principal_id.clone(),
                            avatar_revision: Some(revision.to_owned()),
                            cached_image_path: None,
                            media_type: None,
                            status: DesktopMemberAvatarStatus::Loading,
                        });
                    if entry.avatar_revision.as_deref() != Some(revision) {
                        entry.avatar_revision = Some(revision.to_owned());
                        entry.cached_image_path = None;
                        entry.media_type = None;
                        entry.status = DesktopMemberAvatarStatus::Loading;
                    } else if entry.cached_image_path.is_none() {
                        entry.status = DesktopMemberAvatarStatus::Loading;
                    }
                    requests.push(AvatarCacheRequest {
                        principal_id: member.principal_id.clone(),
                        avatar_revision: revision.to_owned(),
                    });
                }
            }
        }
        requests
    }

    pub(super) fn apply_result(&mut self, result: AvatarCacheResult) -> bool {
        let Some(entry) = self.visible.get_mut(&result.principal_id) else {
            return false;
        };
        if entry.avatar_revision.as_deref() != Some(result.avatar_revision.as_str()) {
            return false;
        }
        entry.cached_image_path = result
            .local_path
            .as_path()
            .to_str()
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        entry.media_type = Some(result.media_type);
        entry.status = if result.source == AvatarCacheSource::OfflineCache {
            DesktopMemberAvatarStatus::Offline
        } else {
            DesktopMemberAvatarStatus::Ready
        };
        true
    }

    pub(super) fn apply_error(
        &mut self,
        principal_id: &PrincipalId,
        avatar_revision: &str,
        error: AvatarCacheError,
    ) -> bool {
        let Some(entry) = self.visible.get_mut(principal_id) else {
            return false;
        };
        if entry.avatar_revision.as_deref() != Some(avatar_revision) {
            return false;
        }
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
        true
    }

    pub(super) fn presentation(
        &self,
        principal_id: &PrincipalId,
    ) -> Option<&DesktopMemberAvatarPresentation> {
        self.visible.get(principal_id)
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
        self.agent_cached_image_path = result
            .local_path
            .as_path()
            .to_str()
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
    }

    pub(super) fn apply_agent_error(&mut self, generation: u64) {
        if self.agent_request_generation != generation {
            return;
        }
        self.agent_loading = false;
    }

    pub(super) fn agent_cached_image_path(&self) -> Option<&str> {
        self.agent_cached_image_path.as_deref()
    }
}

impl PioneerDesktop {
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
            Some("/owned/cache/current-agent")
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
