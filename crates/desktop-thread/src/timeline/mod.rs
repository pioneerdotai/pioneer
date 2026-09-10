mod avatar_rail;
pub(crate) mod code_highlighting;
pub(crate) mod controller;
mod items;
mod layout;
pub(crate) mod layout_index;
pub(crate) mod layout_store;
mod markdown;
pub(crate) mod model;
mod row_presentation;
pub(crate) mod row_registry;
mod row_view;
mod running_indicator;
mod scroll;
mod semantic_adapter;
mod semantic_requests;
pub(crate) mod state;
pub(crate) mod terminal_registry;
mod view;

use self::layout::TIMELINE_CONTENT_MAX_WIDTH;
use self::layout::TIMELINE_ROW_MEASUREMENT_GUARD;
pub(crate) use self::layout::TimelineAvatarGroupKind;
pub(crate) use self::layout::TimelineGrouping;
pub(crate) use self::layout::TimelineLayoutIndex;
pub(crate) use self::layout::TimelineRowLayout;
pub(crate) use self::layout::TimelineRowTopSpacing;
use self::model::TimelineRow;
use self::model::TimelineRowKind;
pub(crate) use self::running_indicator::TimelineAvatarActivities;
pub(crate) use self::scroll::TimelineScrollState;
use crate::screen::TimelineView;
use gpui_kit::prelude::*;
use gpui_kit::*;
use pioneer_client::conversation::reducer::ConversationViewState;
use pioneer_client::conversation::reducer::ItemView;
use pioneer_client::timeline::rows::UserMessagePresentation;
use pioneer_client::timeline::types::MemberSummary;
use pioneer_client::timeline::types::PersistedActorRef;
use pioneer_client::timeline::types::PrincipalId;
use pioneer_client::timeline::types::TurnAuthorSnapshot;
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

// Prepared in update, measured during stock draw, committed by the layout store.
pub(super) struct TimelineLayoutMeasurement {
    entries: Vec<(layout_store::RowMeasurementKey, AnyElement)>,
    row_width: Pixels,
    bodies: Vec<(
        pioneer_client::timeline::presentation::RowId,
        Pixels,
        AnyElement,
    )>,
    row_inputs: Vec<(layout_store::RowMeasurementKey, Option<TurnAuthorSnapshot>)>,
}
impl TimelineLayoutMeasurement {
    fn measure(
        self,
        window: &mut Window,
        cx: &mut App,
    ) -> (
        Vec<(layout_store::RowMeasurementKey, Pixels)>,
        Vec<(pioneer_client::timeline::presentation::RowId, Pixels)>,
    ) {
        let bodies = self
            .bodies
            .into_iter()
            .map(|(id, width, mut element)| {
                let bounds = element.layout_as_root(
                    size(AvailableSpace::Definite(width), AvailableSpace::MaxContent),
                    window,
                    cx,
                );
                (id, bounds.height)
            })
            .collect();
        let rows = self
            .entries
            .into_iter()
            .map(|(key, mut element)| {
                let measured = element.layout_as_root(
                    size(
                        AvailableSpace::Definite(self.row_width),
                        AvailableSpace::MaxContent,
                    ),
                    window,
                    cx,
                );
                (
                    key,
                    (measured.height + TIMELINE_ROW_MEASUREMENT_GUARD).max(px(1.)),
                )
            })
            .collect();
        (rows, bodies)
    }
}

pub(crate) use pioneer_client::timeline::presentation::TimelineRenderRow;

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

fn timeline_agent_label(author: Option<&TurnAuthorSnapshot>) -> Option<String> {
    let author = timeline_agent_execution_author(author)?;
    timeline_agent_presentation(Some(author))?;
    let display_name = author.display_name.trim();
    let nickname = author.nickname.trim();
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
) -> Option<&pioneer_client::timeline::types::AgentPresentationSnapshot> {
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

pub(super) type TimelineItemPresentations =
    HashMap<String, std::sync::Arc<pioneer_client::timeline::presentation::TimelineRowSnapshot>>;

#[derive(Clone)]
pub(crate) struct TimelineRenderModel {
    pub snapshot: Option<std::sync::Arc<pioneer_client::timeline::presentation::TimelineSnapshot>>,
    pub revision: u64,
    pub source_revision: u64,
    pub(crate) item_presentations: std::sync::Arc<
        HashMap<
            String,
            std::sync::Arc<pioneer_client::timeline::presentation::TimelineRowSnapshot>,
        >,
    >,
    pub groups: std::sync::Arc<Vec<pioneer_client::timeline::presentation::TimelineGroup>>,
    pub projection: std::sync::Arc<ConversationViewState>,
    pub rows: std::sync::Arc<Vec<TimelineRenderRow>>,
}

impl TimelineRenderModel {
    pub(crate) fn empty() -> Self {
        Self {
            snapshot: None,
            revision: 0,
            source_revision: 0,
            item_presentations: Default::default(),
            groups: Default::default(),
            projection: std::sync::Arc::new(ConversationViewState::default()),
            rows: std::sync::Arc::new(Vec::new()),
        }
    }
}

impl TimelineView {
    fn current_timeline_author_presentation(
        &self,
        author: Option<&TurnAuthorSnapshot>,
    ) -> TimelineAuthorPresentation {
        let Some(PersistedActorRef::Principal(principal_id)) = author.map(|author| &author.actor)
        else {
            return resolve_timeline_author_presentation(author, None);
        };

        if let Some(auth) = self
            .identity_input
            .as_ref()
            .and_then(|input| input.current_auth.as_ref())
            .filter(|auth| auth.principal.id == *principal_id)
        {
            return TimelineAuthorPresentation {
                principal_id: Some(principal_id.clone()),
                display_name: auth.principal.display_name.trim().to_owned(),
                nickname: auth.principal.nickname.trim().to_owned(),
                avatar_revision: auth.principal.avatar_revision.clone(),
            };
        }

        let directory_member = self.thread_member_input.as_ref().and_then(|input| {
            input
                .member_directory
                .iter()
                .find(|member| member.principal_id == *principal_id)
        });
        let workspace_member = self.thread_member_input.as_ref().and_then(|input| {
            input
                .workspace_members
                .iter()
                .find(|member| member.principal_id == *principal_id)
        });

        resolve_timeline_author_presentation(author, directory_member.or(workspace_member))
    }

    pub(super) fn update_timeline_layout_width(&self, measured_width: Pixels) -> bool {
        if measured_width <= px(1.) {
            return false;
        }

        let mut state = self.thread_timeline_view_state.borrow_mut();
        if state.measured_list_width == measured_width {
            return false;
        }

        let previous_content_width = state
            .measured_list_width
            .max(px(1.))
            .min(TIMELINE_CONTENT_MAX_WIDTH);
        let next_content_width = measured_width.max(px(1.)).min(TIMELINE_CONTENT_MAX_WIDTH);
        state.measured_list_width = measured_width;

        let content_width_changed = previous_content_width != next_content_width;
        content_width_changed
    }

    fn timeline_entry_text(item_view: &ItemView) -> &str {
        pioneer_client::timeline::labels::timeline_entry_text(item_view)
    }

    fn timeline_content_width(&self, window: &Window) -> Pixels {
        let measured_width = self
            .thread_timeline_view_state
            .scroll_handle
            .bounds()
            .size
            .width;
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

    fn prepare_timeline_item_sizes(
        &self,
        model: &TimelineRenderModel,
        grouping: &TimelineGrouping,
        row_width: Pixels,
        content_width: Pixels,
        window: &Window,
        cx: &mut Context<Self>,
    ) -> TimelineLayoutMeasurement {
        let mut entries = Vec::new();
        let mut row_inputs = Vec::new();
        let mut bodies = Vec::new();
        let expanded = self.thread_timeline_view_state.expanded.borrow();
        let principal = self
            .identity_input
            .as_ref()
            .and_then(|input| input.current_auth.as_ref())
            .map(|auth| auth.principal.id.to_string());
        if let Some(snapshot) = &model.snapshot {
            for (ix, row) in snapshot.rows().iter().enumerate() {
                let layout = grouping.row_layout(ix);
                let author = grouping.agent_author_for_group_start(ix);
                let key = layout_store::RowMeasurementKey {
                    id: row.id().clone(),
                    dependencies_revision: row_view::row_dependency_revision(self, row, cx),
                    layout_revision: row.revision(),
                    content_revision: row.content_revision(),
                    presentation_revision: row.metadata_revision(),
                    content_width,
                    rem: window.rem_size(),
                    text_style: self.thread_timeline_view_state.layout_text_style.clone(),
                    theme_revision: self.layout_store.theme_revision,
                    locale: rust_i18n::locale().to_string(),
                    expanded: expanded.contains(row.id().as_str()),
                    grouping: layout,
                    last: ix + 1 == snapshot.rows().len(),
                    author_label: layout
                        .starts_avatar_group
                        .then(|| timeline_agent_label(author))
                        .flatten(),
                    principal: principal.clone(),
                    task_child: self.active_task_thread_navigation().is_some(),
                };
                row_inputs.push((key.clone(), author.cloned()));
                if self.layout_store.height(&key).is_none() {
                    let slot = self
                        .row_registry
                        .get(row.id())
                        .expect("live published row slot");
                    let presentation = row_view::RowPresentation::new(
                        slot.clone(),
                        key.clone(),
                        author.cloned(),
                        self,
                        cx,
                    );
                    if let Some(body) = presentation.body_measurement(cx) {
                        bodies.push(body);
                    }
                    entries.push((key, presentation.render(cx)));
                }
            }
        }
        TimelineLayoutMeasurement {
            bodies,
            entries,
            row_width,
            row_inputs,
        }
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

#[cfg(test)]
mod author_presentation_tests {
    use super::*;
    use pioneer_client::timeline::types::AgentExecutionId;
    use pioneer_client::timeline::types::AgentIdentityId;
    use pioneer_client::timeline::types::AgentIdentitySourceKind;
    use pioneer_client::timeline::types::PrincipalKind;
    use pioneer_client::timeline::types::PrincipalStatus;
    use pioneer_client::timeline::types::RoleKey;

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

    fn member(principal_id: &PrincipalId) -> MemberSummary {
        MemberSummary {
            principal_id: principal_id.clone(),
            kind: PrincipalKind::User,
            display_name: "Current Name".to_owned(),
            nickname: "current".to_owned(),
            role_key: Some(RoleKey::member()),
            role: pioneer_client::timeline::types::AuthorizationRolePresentation {
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

        author.agent = Some(pioneer_client::timeline::types::AgentPresentationSnapshot {
            agent_identity_id: AgentIdentityId::new("A0000000000000000000A")
                .expect("agent identity id"),
            agent_execution_id: execution_id,
            identity_source_kind: AgentIdentitySourceKind::CliRuntimeInstance,
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

        author.display_name = "Renamed Codex".to_owned();
        author.nickname = "renamed-codex".to_owned();
        assert_eq!(
            timeline_agent_label(Some(&author)),
            Some("Renamed Codex · @renamed-codex".to_owned())
        );
        assert_eq!(
            author
                .agent
                .as_ref()
                .map(|agent| agent.display_name.as_str()),
            Some("Codex CLI")
        );
    }
}
