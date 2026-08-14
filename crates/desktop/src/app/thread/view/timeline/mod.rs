mod avatar_rail;
mod code_highlighting;
mod items;
mod layout;
mod markdown;
pub(crate) mod model;
mod running_indicator;
mod scroll;
mod semantic_adapter;
mod semantic_requests;
mod view;

use self::layout::{TIMELINE_CONTENT_MAX_WIDTH, TIMELINE_ROW_MEASUREMENT_GUARD};
pub(crate) use self::layout::{
    TimelineAvatarGroupKind, TimelineGrouping, TimelineLayoutIndex, TimelineRowLayout,
    TimelineRowTopSpacing,
};
use self::model::{TimelineRow, TimelineRowKind};
use crate::app::{
    conversation::{ConversationViewState, ItemView},
    root::{
        CachedTimelineEntryLayout, PendingRequest, PioneerDesktop, ThreadTimelineViewState,
        TimelineScrollAnchor,
    },
};
use gpui::{prelude::*, *};
use pioneer_client::timeline::rows::UserMessagePresentation;
use pioneer_protocol::{
    MemberSummary, PersistedActorRef, PrincipalId, TurnAuthorSnapshot, WorkspaceId,
};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    rc::Rc,
};

#[derive(Clone)]
pub(crate) struct TimelinePendingRequestRow {
    pub key: String,
    pub request: PendingRequest,
}

#[derive(Clone)]
pub(crate) enum TimelineRenderRow {
    Timeline(TimelineRow),
    PendingRequest(TimelinePendingRequestRow),
}

impl TimelineRenderRow {
    pub(super) fn key(&self) -> &str {
        match self {
            TimelineRenderRow::Timeline(row) => row.key.as_str(),
            TimelineRenderRow::PendingRequest(row) => row.key.as_str(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TimelinePresentationContext {
    pub(crate) task_child_thread: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TimelineAuthorPresentation {
    principal_id: Option<PrincipalId>,
    display_name: String,
    nickname: String,
    avatar_revision: Option<String>,
}

fn resolve_timeline_author_presentation(
    author: Option<&TurnAuthorSnapshot>,
    current_member: Option<&MemberSummary>,
) -> TimelineAuthorPresentation {
    let fallback = || TimelineAuthorPresentation {
        principal_id: author.and_then(|author| match &author.actor {
            PersistedActorRef::Principal(principal_id) => Some(principal_id.clone()),
            PersistedActorRef::System => None,
        }),
        display_name: author
            .map(|author| author.display_name.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "?".to_owned()),
        nickname: author
            .map(|author| author.nickname.trim().to_owned())
            .unwrap_or_default(),
        avatar_revision: author.and_then(|author| author.avatar_revision.clone()),
    };

    let Some(author) = author else {
        return fallback();
    };
    let PersistedActorRef::Principal(principal_id) = &author.actor else {
        return fallback();
    };
    let Some(member) = current_member.filter(|member| member.principal_id == *principal_id) else {
        return fallback();
    };

    TimelineAuthorPresentation {
        principal_id: Some(principal_id.clone()),
        display_name: member.display_name.trim().to_owned(),
        nickname: member.nickname.trim().to_owned(),
        avatar_revision: member.avatar_revision.clone(),
    }
}

fn user_message_uses_current_principal_alignment(
    presentation: Option<&UserMessagePresentation>,
    current_principal_id: Option<&str>,
    presentation_context: TimelinePresentationContext,
) -> bool {
    let Some(presentation) = presentation else {
        return true;
    };

    match presentation.author.as_ref().map(|author| &author.actor) {
        Some(PersistedActorRef::Principal(principal_id)) => {
            current_principal_id == Some(principal_id.as_str())
        }
        Some(PersistedActorRef::System) => presentation_context.task_child_thread,
        None => {
            presentation_context.task_child_thread
                || presentation.item_id == format!("user_{}", presentation.turn_id)
                || presentation.item_id == format!("turn:{}:user", presentation.turn_id)
                || presentation.block_id == format!("turn:{}:user", presentation.turn_id)
        }
    }
}

fn is_current_principal_user_message(
    row: &TimelineRenderRow,
    current_principal_id: Option<&str>,
    presentation_context: TimelinePresentationContext,
) -> bool {
    let TimelineRenderRow::Timeline(TimelineRow {
        kind: TimelineRowKind::UserMessage { presentation, .. },
        ..
    }) = row
    else {
        return false;
    };

    user_message_uses_current_principal_alignment(
        Some(presentation),
        current_principal_id,
        presentation_context,
    )
}

#[derive(Clone)]
pub(crate) struct TimelineRenderModel {
    pub projection: Rc<ConversationViewState>,
    pub rows: Rc<Vec<TimelineRenderRow>>,
    pub row_render_fingerprints: Rc<HashMap<String, u64>>,
    pub semantic_row_ids:
        Rc<HashMap<String, pioneer_client::timeline::semantic::SemanticTimelineRowId>>,
    pub semantic_rows: Rc<pioneer_client::timeline::semantic::SemanticTimelineRows>,
}

impl TimelineRenderModel {
    pub(crate) fn empty() -> Self {
        Self {
            projection: Rc::new(ConversationViewState::default()),
            rows: Rc::new(Vec::new()),
            row_render_fingerprints: Rc::new(HashMap::new()),
            semantic_row_ids: Rc::new(HashMap::new()),
            semantic_rows: Rc::new(
                pioneer_client::timeline::semantic::SemanticTimelineRows::default(),
            ),
        }
    }
}

impl PioneerDesktop {
    fn current_timeline_author_presentation(
        &self,
        author: Option<&TurnAuthorSnapshot>,
    ) -> TimelineAuthorPresentation {
        let Some(PersistedActorRef::Principal(principal_id)) = author.map(|author| &author.actor)
        else {
            return resolve_timeline_author_presentation(author, None);
        };

        if let Some(auth) = self
            .gateway
            .current_auth
            .as_ref()
            .filter(|auth| auth.principal.id == *principal_id)
        {
            return TimelineAuthorPresentation {
                principal_id: Some(principal_id.clone()),
                display_name: auth.principal.display_name.trim().to_owned(),
                nickname: auth.principal.nickname.trim().to_owned(),
                avatar_revision: auth.principal.avatar_revision.clone(),
            };
        }

        let directory_member = self
            .administration
            .members()
            .find(|member| member.principal_id == *principal_id);
        let workspace_member = self
            .current_active_thread_id()
            .and_then(|thread_id| self.thread_workspace_id(thread_id))
            .and_then(|workspace_id| WorkspaceId::new(workspace_id.to_owned()).ok())
            .and_then(|workspace_id| self.administration.workspace_members(&workspace_id))
            .and_then(|members| {
                members
                    .iter()
                    .find(|member| member.principal_id == *principal_id)
            });

        resolve_timeline_author_presentation(author, directory_member.or(workspace_member))
    }

    fn sync_timeline_layout_width(&self, cx: &mut Context<Self>) {
        let measured_width = self.thread_timeline_scroll_handle.bounds().size.width;
        if measured_width > px(1.) {
            self.update_timeline_layout_width(measured_width);
            return;
        }

        let mut state = self.thread_timeline_view_state.borrow_mut();
        if state.measured_list_width <= px(1.) && state.width_probe_attempts < 12 {
            state.width_probe_attempts = state.width_probe_attempts.saturating_add(1);
            state.pending_width_probe = true;
            drop(state);
            cx.notify();
        }
    }

    pub(super) fn update_timeline_layout_width(&self, measured_width: Pixels) -> bool {
        if measured_width <= px(1.) {
            return false;
        }

        let mut state = self.thread_timeline_view_state.borrow_mut();
        state.pending_width_probe = false;
        state.width_probe_attempts = 0;
        if (state.measured_list_width - measured_width).abs() <= px(1.) {
            return false;
        }

        let previous_content_width = state
            .measured_list_width
            .max(px(1.))
            .min(TIMELINE_CONTENT_MAX_WIDTH);
        let next_content_width = measured_width.max(px(1.)).min(TIMELINE_CONTENT_MAX_WIDTH);
        state.measured_list_width = measured_width;

        let content_width_changed = (previous_content_width - next_content_width).abs() > px(1.);
        if content_width_changed {
            state.entry_layout_cache.clear();
            state.cached_item_sizes = None;
            state.cached_timeline_layout_index = None;
        }
        content_width_changed
    }

    fn timeline_entry_text(item_view: &ItemView) -> &str {
        pioneer_client::timeline::labels::timeline_entry_text(item_view)
    }

    fn timeline_row_render_fingerprint(
        &self,
        projection: &ConversationViewState,
        row: &TimelineRenderRow,
        row_render_fingerprints: &HashMap<String, u64>,
        expanded: &HashSet<String>,
    ) -> u64 {
        match row {
            TimelineRenderRow::Timeline(row) => {
                model::timeline_row_render_fingerprint_from_content(
                    row_render_fingerprints
                        .get(row.key.as_str())
                        .copied()
                        .unwrap_or_else(|| {
                            model::timeline_row_content_fingerprint(projection, row)
                        }),
                    projection,
                    row,
                    expanded,
                )
            }
            TimelineRenderRow::PendingRequest(row) => {
                timeline_pending_request_render_fingerprint(row)
            }
        }
    }

    fn timeline_content_width(&self, window: &Window) -> Pixels {
        let measured_width = self.thread_timeline_scroll_handle.bounds().size.width;
        if measured_width > px(1.) {
            return measured_width.max(px(280.));
        }

        let cached_width = self.thread_timeline_view_state.borrow().measured_list_width;
        if cached_width > px(1.) {
            return cached_width.max(px(280.));
        }

        let fallback_window_width = match window.window_bounds() {
            WindowBounds::Windowed(bounds)
            | WindowBounds::Maximized(bounds)
            | WindowBounds::Fullscreen(bounds) => bounds.size.width,
        };
        if fallback_window_width > px(1.) {
            return fallback_window_width.max(px(280.));
        }

        px(320.)
    }

    fn timeline_entry_content_width(&self, list_width: Pixels) -> Pixels {
        list_width.max(px(1.)).min(TIMELINE_CONTENT_MAX_WIDTH)
    }

    fn measure_timeline_row_size(
        &self,
        projection: &ConversationViewState,
        row: &TimelineRenderRow,
        is_last_row: bool,
        row_layout: TimelineRowLayout,
        row_width: Pixels,
        content_width: Pixels,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Size<Pixels> {
        let mut row_element =
            self.render_timeline_row(projection, row, is_last_row, row_layout, content_width, cx);
        let measured = row_element.layout_as_root(
            size(
                AvailableSpace::Definite(row_width),
                AvailableSpace::MaxContent,
            ),
            window,
            cx,
        );

        size(
            px(0.),
            (measured.height + TIMELINE_ROW_MEASUREMENT_GUARD).max(px(1.)),
        )
    }

    fn cached_or_measure_timeline_row_size(
        &self,
        state: &mut ThreadTimelineViewState,
        projection: &ConversationViewState,
        row: &TimelineRenderRow,
        is_last_row: bool,
        row_layout: TimelineRowLayout,
        row_width: Pixels,
        content_width: Pixels,
        row_render_fingerprints: &HashMap<String, u64>,
        expanded: &HashSet<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Size<Pixels> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.timeline_row_render_fingerprint(projection, row, row_render_fingerprints, expanded)
            .hash(&mut hasher);
        is_last_row.hash(&mut hasher);
        row_layout.hash(&mut hasher);
        let render_fingerprint = hasher.finish();

        if let Some(cached) = state.entry_layout_cache.get(row.key())
            && cached.render_fingerprint == render_fingerprint
        {
            return size(px(0.), cached.height.max(px(1.)));
        }

        let measured = self.measure_timeline_row_size(
            projection,
            row,
            is_last_row,
            row_layout,
            row_width,
            content_width,
            window,
            cx,
        );
        state.entry_layout_cache.insert(
            row.key().to_owned(),
            CachedTimelineEntryLayout {
                render_fingerprint,
                height: measured.height,
            },
        );
        measured
    }

    fn compute_timeline_item_sizes(
        &self,
        state: &mut ThreadTimelineViewState,
        projection: &ConversationViewState,
        rows: &[TimelineRenderRow],
        grouping: &TimelineGrouping,
        row_width: Pixels,
        content_width: Pixels,
        row_render_fingerprints: &HashMap<String, u64>,
        expanded: &HashSet<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Rc<Vec<Size<Pixels>>> {
        let row_len = rows.len();
        Rc::new(
            rows.iter()
                .enumerate()
                .map(|(ix, row)| {
                    self.cached_or_measure_timeline_row_size(
                        state,
                        projection,
                        row,
                        ix + 1 == row_len,
                        grouping.row_layout(ix),
                        row_width,
                        content_width,
                        row_render_fingerprints,
                        expanded,
                        window,
                        cx,
                    )
                })
                .collect::<Vec<_>>(),
        )
    }

    fn timeline_render_row_text_len(
        projection: &ConversationViewState,
        row: &TimelineRenderRow,
    ) -> usize {
        match row {
            TimelineRenderRow::Timeline(row) => model::timeline_row_text_len(projection, row),
            TimelineRenderRow::PendingRequest(row) => {
                row.request.title.as_deref().unwrap_or_default().len()
                    + row.request.message.as_deref().unwrap_or_default().len()
                    + row.request.request_id.len()
            }
        }
    }

    fn timeline_render_row_toggle_key(row: &TimelineRenderRow) -> Option<&str> {
        match row {
            TimelineRenderRow::Timeline(row) => model::timeline_row_toggle_key(row),
            TimelineRenderRow::PendingRequest(_) => None,
        }
    }
}

fn timeline_pending_request_render_fingerprint(row: &TimelinePendingRequestRow) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    row.request.workspace_id.hash(&mut hasher);
    row.request.request_id.hash(&mut hasher);
    row.request.thread_id.hash(&mut hasher);
    row.request.turn_id.hash(&mut hasher);
    row.request.item_id.hash(&mut hasher);
    format!("{:?}", row.request.origin).hash(&mut hasher);
    format!("{:?}", row.request.kind).hash(&mut hasher);
    row.request.title.hash(&mut hasher);
    row.request.message.hash(&mut hasher);
    format!("{:?}", row.request.payload).hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod author_presentation_tests {
    use super::*;
    use pioneer_protocol::{PrincipalKind, PrincipalStatus, RoleKey};

    fn principal(value: &str) -> PrincipalId {
        PrincipalId::new(value).expect("valid principal id")
    }

    fn snapshot(principal_id: &PrincipalId) -> TurnAuthorSnapshot {
        TurnAuthorSnapshot {
            actor: PersistedActorRef::Principal(principal_id.clone()),
            display_name: "Historical Name".to_owned(),
            nickname: "historical".to_owned(),
            avatar_revision: Some("historical-avatar".to_owned()),
        }
    }

    fn member(principal_id: &PrincipalId) -> MemberSummary {
        MemberSummary {
            principal_id: principal_id.clone(),
            kind: PrincipalKind::User,
            display_name: "Current Name".to_owned(),
            nickname: "current".to_owned(),
            role_key: Some(RoleKey::member()),
            role: pioneer_protocol::AuthorizationRolePresentation {
                key: "member".to_owned(),
                display_name: "Member".to_owned(),
                description: "Workspace collaborator".to_owned(),
                built_in: true,
            },
            lifecycle_managed: true,
            status: PrincipalStatus::Active,
            avatar_revision: Some("current-avatar".to_owned()),
        }
    }

    #[::core::prelude::v1::test]
    fn current_member_profile_overlays_the_persisted_author_snapshot() {
        let principal_id = principal("P0000000000000000000A");
        let author = snapshot(&principal_id);
        let member = member(&principal_id);

        let presentation = resolve_timeline_author_presentation(Some(&author), Some(&member));

        assert_eq!(presentation.principal_id.as_ref(), Some(&principal_id));
        assert_eq!(presentation.display_name, "Current Name");
        assert_eq!(presentation.nickname, "current");
        assert_eq!(
            presentation.avatar_revision.as_deref(),
            Some("current-avatar")
        );
    }

    #[::core::prelude::v1::test]
    fn persisted_author_snapshot_remains_the_fallback_without_a_visible_member() {
        let principal_id = principal("P0000000000000000000A");
        let author = snapshot(&principal_id);

        let presentation = resolve_timeline_author_presentation(Some(&author), None);

        assert_eq!(presentation.display_name, "Historical Name");
        assert_eq!(presentation.nickname, "historical");
        assert_eq!(
            presentation.avatar_revision.as_deref(),
            Some("historical-avatar")
        );
    }
}
