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
use pioneer_protocol::{PersistedActorRef, PrincipalId, TurnAuthorSnapshot};
use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    rc::Rc,
};

#[derive(Clone)]
pub(crate) struct TimelinePendingRequestRow {
    pub key: String,
    pub request: PendingRequest,
    pub author: Option<TurnAuthorSnapshot>,
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
) -> TimelineAuthorPresentation {
    TimelineAuthorPresentation {
        principal_id: author.and_then(|author| match &author.actor {
            PersistedActorRef::Principal(principal_id) => Some(principal_id.clone()),
            PersistedActorRef::AgentExecution(_) => None,
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
    }
}

fn timeline_agent_label(author: Option<&TurnAuthorSnapshot>) -> Option<String> {
    let author = timeline_agent_execution_author(author)?;
    let agent = timeline_agent_presentation(Some(author))?;
    let display_name = agent.display_name.trim();
    let nickname = agent.nickname.trim();
    match (display_name.is_empty(), nickname.is_empty()) {
        (true, true) => None,
        (true, false) => Some(format!("@{nickname}")),
        (false, true) => Some(display_name.to_owned()),
        (false, false) => Some(format!("{display_name} · @{nickname}")),
    }
}

pub(super) fn timeline_agent_execution_author(
    author: Option<&TurnAuthorSnapshot>,
) -> Option<&TurnAuthorSnapshot> {
    author.filter(|author| matches!(&author.actor, PersistedActorRef::AgentExecution(_)))
}

pub(super) fn timeline_agent_presentation(
    author: Option<&TurnAuthorSnapshot>,
) -> Option<&pioneer_protocol::AgentPresentationSnapshot> {
    let author = author?;
    let PersistedActorRef::AgentExecution(execution_id) = &author.actor else {
        return None;
    };
    author
        .agent
        .as_ref()
        .filter(|agent| &agent.agent_execution_id == execution_id)
}

fn user_message_uses_current_principal_alignment(
    presentation: Option<&UserMessagePresentation>,
    author: Option<&TurnAuthorSnapshot>,
    current_principal_id: Option<&str>,
) -> bool {
    let Some(presentation) = presentation else {
        return true;
    };

    match author.map(|author| &author.actor) {
        Some(PersistedActorRef::Principal(principal_id)) => {
            current_principal_id == Some(principal_id.as_str())
        }
        Some(PersistedActorRef::AgentExecution(_)) | Some(PersistedActorRef::System) => false,
        None => {
            presentation.item_id == format!("user_{}", presentation.turn_id)
                || presentation.item_id == format!("turn:{}:user", presentation.turn_id)
                || presentation.block_id == format!("turn:{}:user", presentation.turn_id)
        }
    }
}

fn is_current_principal_user_message(
    row: &TimelineRenderRow,
    current_principal_id: Option<&str>,
) -> bool {
    let TimelineRenderRow::Timeline(TimelineRow {
        author,
        kind: TimelineRowKind::UserMessage { presentation, .. },
        ..
    }) = row
    else {
        return false;
    };

    user_message_uses_current_principal_alignment(
        Some(presentation),
        author.as_ref(),
        current_principal_id,
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
        resolve_timeline_author_presentation(author)
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
        agent_group_author: Option<&TurnAuthorSnapshot>,
        row_width: Pixels,
        content_width: Pixels,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Size<Pixels> {
        let mut row_element = self.render_timeline_row(
            projection,
            row,
            is_last_row,
            row_layout,
            agent_group_author,
            content_width,
            cx,
        );
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
        agent_group_author: Option<&TurnAuthorSnapshot>,
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
        if row_layout.starts_avatar_group {
            timeline_agent_label(agent_group_author).hash(&mut hasher);
        }
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
            agent_group_author,
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
                        grouping.agent_author_for_group_start(ix),
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
    serde_json::to_vec(&row.author)
        .expect("timeline author snapshot must serialize")
        .hash(&mut hasher);
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
    use pioneer_protocol::{AgentExecutionId, AgentIdentityId, AgentIdentitySourceKind};

    fn principal(value: &str) -> PrincipalId {
        PrincipalId::new(value).expect("valid principal id")
    }

    fn snapshot(principal_id: &PrincipalId) -> TurnAuthorSnapshot {
        TurnAuthorSnapshot {
            actor: PersistedActorRef::Principal(principal_id.clone()),
            display_name: "Historical Name".to_owned(),
            nickname: "historical".to_owned(),
            avatar_revision: Some("historical-avatar".to_owned()),
            agent: None,
        }
    }

    #[::core::prelude::v1::test]
    fn persisted_author_snapshot_is_the_timeline_presentation() {
        let principal_id = principal("P0000000000000000000A");
        let author = snapshot(&principal_id);

        let presentation = resolve_timeline_author_presentation(Some(&author));

        assert_eq!(presentation.display_name, "Historical Name");
        assert_eq!(presentation.nickname, "historical");
        assert_eq!(
            presentation.avatar_revision.as_deref(),
            Some("historical-avatar")
        );
    }

    #[::core::prelude::v1::test]
    fn agent_group_label_requires_an_exact_agent_execution() {
        let principal_id = principal("P0000000000000000000A");
        assert_eq!(timeline_agent_label(Some(&snapshot(&principal_id))), None);
        assert_eq!(timeline_agent_label(None), None);

        let execution_id =
            AgentExecutionId::new("E0000000000000000000A").expect("agent execution id");
        let mut author = TurnAuthorSnapshot {
            actor: PersistedActorRef::AgentExecution(execution_id.clone()),
            display_name: "Codex CLI".to_owned(),
            nickname: "codex".to_owned(),
            avatar_revision: None,
            agent: None,
        };
        assert_eq!(timeline_agent_label(Some(&author)), None);

        author.agent = Some(pioneer_protocol::AgentPresentationSnapshot {
            agent_identity_id: AgentIdentityId::new("A0000000000000000000A")
                .expect("agent identity id"),
            agent_execution_id: execution_id,
            identity_source_kind: AgentIdentitySourceKind::CliRuntime,
            identity_source_revision: 1,
            display_name: "Codex CLI".to_owned(),
            nickname: "codex".to_owned(),
            avatar_revision: None,
            role_label: Some("codex".to_owned()),
        });
        assert_eq!(
            timeline_agent_label(Some(&author)),
            Some("Codex CLI · @codex".to_owned())
        );
    }
}
