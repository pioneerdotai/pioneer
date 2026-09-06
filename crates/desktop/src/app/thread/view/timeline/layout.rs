use super::{TimelinePresentationContext, TimelineRenderRow};
use crate::app::conversation::ConversationViewState;
use gpui_kit::{Pixels, Size, px};
use pioneer_client::timeline::{
    labels::is_task_timeline_agent_message,
    rows::{TimelineRow, TimelineRowKind},
};
use pioneer_protocol::{AgentMessagePhase, PersistedActorRef, TurnAuthorSnapshot, TurnItem};
use std::rc::Rc;

pub(crate) const TIMELINE_AVATAR_RAIL_WIDTH: Pixels = px(40.);
pub(crate) const TIMELINE_AVATAR_SIZE: Pixels = px(32.);
pub(crate) const TIMELINE_AVATAR_STICKY_BOTTOM: Pixels = px(8.);
pub(crate) const TIMELINE_CONTENT_HORIZONTAL_PADDING: Pixels = px(24.);
pub(crate) const TIMELINE_CONTENT_MAX_WIDTH: Pixels = px(800.);
pub(crate) const TIMELINE_EDGE_PADDING: Pixels = px(40.);
pub(crate) const TIMELINE_GROUP_MESSAGE_GAP: Pixels = px(4.);
pub(crate) const TIMELINE_MESSAGE_END_BOTTOM_SPACING: Pixels = TIMELINE_EDGE_PADDING;
pub(crate) const TIMELINE_ITEM_BOTTOM_SPACING: Pixels = px(10.);
pub(crate) const TIMELINE_END_BOTTOM_SPACING: Pixels = TIMELINE_EDGE_PADDING;
pub(crate) const TIMELINE_ROW_MEASUREMENT_GUARD: Pixels = px(1.);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TimelineRowTopSpacing {
    TimelineStart,
    GroupStart,
    GroupMessage,
    Compact,
    Standard,
}

impl TimelineRowTopSpacing {
    pub(crate) const fn pixels(self) -> Pixels {
        match self {
            Self::TimelineStart => TIMELINE_EDGE_PADDING,
            Self::GroupStart => px(30.),
            Self::GroupMessage => TIMELINE_GROUP_MESSAGE_GAP,
            Self::Compact => px(0.),
            Self::Standard => px(10.),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TimelineAvatarGroupKind {
    HistoricalUser,
    Agent,
}

#[derive(Clone, Debug)]
pub(crate) enum TimelineAvatarSource {
    HistoricalUser {
        author: Option<TurnAuthorSnapshot>,
    },
    Agent {
        author: Option<TurnAuthorSnapshot>,
        shows_running_dino: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TimelineRowLayout {
    pub(crate) top_spacing: TimelineRowTopSpacing,
    pub(crate) avatar_group_kind: Option<TimelineAvatarGroupKind>,
    pub(crate) starts_avatar_group: bool,
}

impl Default for TimelineRowLayout {
    fn default() -> Self {
        Self {
            top_spacing: TimelineRowTopSpacing::Standard,
            avatar_group_kind: None,
            starts_avatar_group: false,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TimelineAvatarGroup {
    pub(crate) first_row_index: usize,
    pub(crate) last_row_index: usize,
    pub(crate) source: TimelineAvatarSource,
    bottom_inset: Pixels,
}

#[derive(Clone, Debug)]
pub(crate) struct TimelineGrouping {
    row_layouts: Vec<TimelineRowLayout>,
    avatar_groups: Vec<TimelineAvatarGroup>,
}

impl TimelineGrouping {
    #[cfg(test)]
    fn build(
        rows: &[TimelineRenderRow],
        projection: &ConversationViewState,
        current_principal_id: Option<&str>,
        presentation_context: TimelinePresentationContext,
        message_text_bottom_inset: Pixels,
    ) -> Rc<Self> {
        let groups = pioneer_client::timeline::presentation::project_timeline_groups(
            rows,
            projection,
            current_principal_id,
        );
        Self::from_snapshot(
            rows,
            &groups,
            projection,
            current_principal_id,
            presentation_context,
            message_text_bottom_inset,
        )
    }

    pub(crate) fn from_snapshot(
        rows: &[TimelineRenderRow],
        groups: &[pioneer_client::timeline::presentation::TimelineGroup],
        projection: &ConversationViewState,
        _current_principal_id: Option<&str>,
        presentation_context: TimelinePresentationContext,
        message_text_bottom_inset: Pixels,
    ) -> Rc<Self> {
        let mut row_layouts = vec![TimelineRowLayout::default(); rows.len()];
        let mut avatar_groups = Vec::new();
        for group in groups {
            let index = group.first_row;
            let end = group.last_row;
            if index > end || end >= rows.len() {
                continue;
            }
            let own_message = group.current_principal;
            let kind = if own_message {
                None
            } else if group.user_message {
                Some(TimelineAvatarGroupKind::HistoricalUser)
            } else {
                Some(TimelineAvatarGroupKind::Agent)
            };
            for (offset, layout) in row_layouts[index..=end].iter_mut().enumerate() {
                layout.top_spacing = if offset > 0 {
                    TimelineRowTopSpacing::GroupMessage
                } else if index == 0 {
                    TimelineRowTopSpacing::TimelineStart
                } else {
                    TimelineRowTopSpacing::GroupStart
                };
                layout.avatar_group_kind = kind;
                layout.starts_avatar_group = offset == 0 && kind.is_some();
            }
            let source = match kind {
                Some(TimelineAvatarGroupKind::HistoricalUser) => {
                    Some(TimelineAvatarSource::HistoricalUser {
                        author: group.author.clone(),
                    })
                }
                Some(TimelineAvatarGroupKind::Agent) => Some(TimelineAvatarSource::Agent {
                    author: group.author.clone(),
                    shows_running_dino: presentation_context.task_child_thread && group.has_running,
                }),
                None => None,
            };
            if let Some(source) = source {
                avatar_groups.push(TimelineAvatarGroup {
                    first_row_index: index,
                    last_row_index: end,
                    source,
                    bottom_inset: avatar_group_bottom_inset(
                        &rows[end],
                        projection,
                        end + 1 == rows.len(),
                        message_text_bottom_inset,
                    ),
                });
            }
        }

        Rc::new(Self {
            row_layouts,
            avatar_groups,
        })
    }

    pub(crate) fn row_layout(&self, index: usize) -> TimelineRowLayout {
        self.row_layouts.get(index).copied().unwrap_or_default()
    }

    pub(crate) fn avatar_groups(&self) -> &[TimelineAvatarGroup] {
        self.avatar_groups.as_slice()
    }

    pub(crate) fn agent_author_for_group_start(
        &self,
        row_index: usize,
    ) -> Option<&TurnAuthorSnapshot> {
        self.avatar_groups
            .iter()
            .find(|group| group.first_row_index == row_index)
            .and_then(|group| match &group.source {
                TimelineAvatarSource::Agent { author, .. } => author.as_ref(),
                TimelineAvatarSource::HistoricalUser { .. } => None,
            })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TimelineLayoutIndex {
    grouping: Rc<TimelineGrouping>,
    item_sizes: Rc<Vec<Size<Pixels>>>,
    item_origins: Vec<Pixels>,
}

impl TimelineLayoutIndex {
    /// Mirrors the timeline's `v_virtual_list` contract: the list receives these exact
    /// item sizes and is explicitly rendered without padding or inter-item gap.
    pub(crate) fn new(
        grouping: Rc<TimelineGrouping>,
        item_sizes: Rc<Vec<Size<Pixels>>>,
    ) -> Rc<Self> {
        let item_origins = item_sizes
            .iter()
            .scan(px(0.), |top, item_size| {
                let origin = *top;
                *top += item_size.height;
                Some(origin)
            })
            .collect();

        Rc::new(Self {
            grouping,
            item_sizes,
            item_origins,
        })
    }

    pub(crate) fn grouping(&self) -> &TimelineGrouping {
        self.grouping.as_ref()
    }

    pub(crate) fn grouping_rc(&self) -> Rc<TimelineGrouping> {
        self.grouping.clone()
    }

    pub(crate) fn avatar_group_bounds(
        &self,
        group: &TimelineAvatarGroup,
    ) -> Option<(Pixels, Pixels)> {
        let first_origin = *self.item_origins.get(group.first_row_index)?;
        let last_origin = *self.item_origins.get(group.last_row_index)?;
        let last_size = self.item_sizes.get(group.last_row_index)?;
        let anchor = first_origin
            + self
                .grouping
                .row_layout(group.first_row_index)
                .top_spacing
                .pixels();
        Some((anchor, last_origin + last_size.height - group.bottom_inset))
    }
}

fn avatar_group_bottom_inset(
    row: &TimelineRenderRow,
    projection: &ConversationViewState,
    is_timeline_end: bool,
    message_text_bottom_inset: Pixels,
) -> Pixels {
    let message_end_spacing = message_text_bottom_inset
        + if is_timeline_end {
            TIMELINE_MESSAGE_END_BOTTOM_SPACING
        } else {
            px(0.)
        };
    let item_end_spacing = if is_timeline_end {
        TIMELINE_END_BOTTOM_SPACING
    } else {
        TIMELINE_ITEM_BOTTOM_SPACING
    };
    match row {
        TimelineRenderRow::Timeline(TimelineRow {
            kind: TimelineRowKind::UserMessage { .. },
            ..
        }) => message_end_spacing,
        TimelineRenderRow::Timeline(TimelineRow {
            kind: TimelineRowKind::Item { timeline_index },
            ..
        }) => {
            let Some(item_view) = projection
                .timeline
                .get(*timeline_index)
                .and_then(|entry| projection.item_for_timeline_entry(entry))
            else {
                return px(0.);
            };

            match &item_view.item {
                TurnItem::UserMessage { .. } => message_end_spacing,
                TurnItem::AgentMessage { .. } if is_task_timeline_agent_message(item_view) => {
                    item_end_spacing
                }
                TurnItem::AgentMessage {
                    phase: AgentMessagePhase::Commentary,
                    ..
                } => message_end_spacing,
                TurnItem::AgentMessage { .. } => message_end_spacing,
                _ => item_end_spacing,
            }
        }
        TimelineRenderRow::Timeline(TimelineRow {
            kind:
                TimelineRowKind::TurnWorkToggle(_)
                | TimelineRowKind::CoalescedTools(_)
                | TimelineRowKind::RunningTurn(_),
            ..
        })
        | TimelineRenderRow::PendingRequest(_) => item_end_spacing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::conversation::TimelineEntry;
    use pioneer_client::conversation::reducer::{TurnPhase, TurnView};
    use pioneer_client::timeline::rows::UserMessagePresentation;
    use pioneer_client::timeline::semantic_render::SEMANTIC_TURN_WORK_GROUP_PREFIX;
    use pioneer_protocol::{AgentExecutionId, PrincipalId, ThreadMode};

    fn principal(value: &str) -> PrincipalId {
        PrincipalId::new(value).expect("valid principal id")
    }

    fn left_message(key: &str, author_id: &str) -> TimelineRenderRow {
        left_message_with_profile(key, author_id, author_id, author_id, None)
    }

    fn left_message_with_profile(
        key: &str,
        author_id: &str,
        display_name: &str,
        nickname: &str,
        avatar_revision: Option<&str>,
    ) -> TimelineRenderRow {
        let author = TurnAuthorSnapshot {
            actor: PersistedActorRef::Principal(principal(author_id)),
            display_name: display_name.to_owned(),
            nickname: nickname.to_owned(),
            avatar_revision: avatar_revision.map(str::to_owned),
            agent: None,
        };
        TimelineRenderRow::Timeline(TimelineRow {
            key: key.to_owned(),
            author: Some(author.clone()),
            kind: TimelineRowKind::UserMessage {
                timeline_index: 0,
                presentation: UserMessagePresentation {
                    workspace_id: "workspace".to_owned(),
                    thread_id: "thread".to_owned(),
                    block_id: key.to_owned(),
                    turn_id: key.to_owned(),
                    item_id: key.to_owned(),
                    mode: ThreadMode::Message,
                    author: Some(author),
                    route: None,
                    reply: None,
                    reply_state: None,
                    mentions: Vec::new(),
                    attachments: Vec::new(),
                    revision: 0,
                    edited: false,
                    deleted: false,
                },
            },
        })
    }

    fn system_message(key: &str) -> TimelineRenderRow {
        let author = TurnAuthorSnapshot {
            actor: PersistedActorRef::System,
            display_name: "System".to_owned(),
            nickname: "system".to_owned(),
            avatar_revision: None,
            agent: None,
        };
        TimelineRenderRow::Timeline(TimelineRow {
            key: key.to_owned(),
            author: Some(author.clone()),
            kind: TimelineRowKind::UserMessage {
                timeline_index: 0,
                presentation: UserMessagePresentation {
                    workspace_id: "workspace".to_owned(),
                    thread_id: "child-thread".to_owned(),
                    block_id: format!("turn:{key}:user"),
                    turn_id: key.to_owned(),
                    item_id: format!("turn:{key}:user"),
                    mode: ThreadMode::Agent,
                    author: Some(author),
                    route: None,
                    reply: None,
                    reply_state: None,
                    mentions: Vec::new(),
                    attachments: Vec::new(),
                    revision: 0,
                    edited: false,
                    deleted: false,
                },
            },
        })
    }

    fn agent_author() -> TurnAuthorSnapshot {
        TurnAuthorSnapshot {
            actor: PersistedActorRef::AgentExecution(
                AgentExecutionId::new("EAAAAAAAAAAAAAAAAAAAA").unwrap(),
            ),
            display_name: "Codex CLI".to_owned(),
            nickname: "codex".to_owned(),
            avatar_revision: Some("agent-avatar".to_owned()),
            agent: None,
        }
    }

    #[test]
    fn consecutive_historical_messages_share_one_avatar_group() {
        let rows = vec![
            left_message("one", "PAAAAAAAAAAAAAAAAAAAA"),
            left_message("two", "PAAAAAAAAAAAAAAAAAAAA"),
            left_message("three", "PBBBBBBBBBBBBBBBBBBBB"),
        ];
        let grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext::default(),
            px(0.),
        );

        assert_eq!(grouping.avatar_groups.len(), 2);
        assert_eq!(grouping.avatar_groups[0].first_row_index, 0);
        assert_eq!(grouping.avatar_groups[0].last_row_index, 1);
        assert_eq!(
            grouping.row_layout(1).top_spacing,
            TimelineRowTopSpacing::GroupMessage
        );
        assert!(grouping.row_layout(0).starts_avatar_group);
        assert!(!grouping.row_layout(1).starts_avatar_group);
    }

    #[test]
    fn profile_change_keeps_messages_in_the_same_principal_group() {
        let rows = vec![
            left_message_with_profile(
                "before",
                "PAAAAAAAAAAAAAAAAAAAA",
                "Alice",
                "alice",
                Some("old-avatar"),
            ),
            left_message_with_profile(
                "after",
                "PAAAAAAAAAAAAAAAAAAAA",
                "Alicia",
                "alicia",
                Some("new-avatar"),
            ),
        ];
        let grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext::default(),
            px(0.),
        );

        assert_eq!(grouping.avatar_groups.len(), 1);
        assert_eq!(grouping.avatar_groups[0].first_row_index, 0);
        assert_eq!(grouping.avatar_groups[0].last_row_index, 1);
        assert!(!grouping.row_layout(1).starts_avatar_group);
        assert_eq!(
            grouping.row_layout(1).top_spacing,
            TimelineRowTopSpacing::GroupMessage
        );
        let TimelineAvatarSource::HistoricalUser {
            author: Some(author),
        } = &grouping.avatar_groups[0].source
        else {
            panic!("first snapshot should remain the persisted fallback for the group");
        };
        assert_eq!(author.display_name, "Alice");
        assert_eq!(author.nickname, "alice");
        assert_eq!(author.avatar_revision.as_deref(), Some("old-avatar"));
    }

    #[test]
    fn task_child_system_input_keeps_its_persisted_actor() {
        let rows = vec![system_message("child-input")];

        let standard_grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext::default(),
            px(0.),
        );
        let child_grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext {
                task_child_thread: true,
            },
            px(0.),
        );

        assert_eq!(standard_grouping.avatar_groups.len(), 1);
        assert_eq!(child_grouping.avatar_groups.len(), 1);
        assert_eq!(
            child_grouping.row_layout(0).avatar_group_kind,
            Some(TimelineAvatarGroupKind::HistoricalUser)
        );
    }

    #[test]
    fn optimistic_local_message_keeps_the_old_right_alignment_without_unknown_author() {
        let mut row = system_message("optimistic");
        let TimelineRenderRow::Timeline(TimelineRow {
            author,
            kind: TimelineRowKind::UserMessage { presentation, .. },
            ..
        }) = &mut row
        else {
            unreachable!();
        };
        *author = None;
        presentation.author = None;

        let grouping = TimelineGrouping::build(
            &[row],
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext::default(),
            px(0.),
        );

        assert!(grouping.avatar_groups.is_empty());
        assert_eq!(grouping.row_layout(0).avatar_group_kind, None);
    }

    #[test]
    fn task_child_agent_input_keeps_its_persisted_actor() {
        let mut row = system_message("child-agent-input");
        let TimelineRenderRow::Timeline(TimelineRow {
            author,
            kind: TimelineRowKind::UserMessage { presentation, .. },
            ..
        }) = &mut row
        else {
            unreachable!();
        };
        let agent_author = agent_author();
        *author = Some(agent_author.clone());
        presentation.author = Some(agent_author);

        let rows = [row];
        let standard_grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext::default(),
            px(0.),
        );
        let child_grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext {
                task_child_thread: true,
            },
            px(0.),
        );

        assert_eq!(standard_grouping.avatar_groups.len(), 1);
        assert_eq!(
            standard_grouping.row_layout(0).avatar_group_kind,
            Some(TimelineAvatarGroupKind::HistoricalUser)
        );
        assert_eq!(child_grouping.avatar_groups.len(), 1);
        assert_eq!(
            child_grouping.row_layout(0).avatar_group_kind,
            Some(TimelineAvatarGroupKind::HistoricalUser)
        );
        let TimelineAvatarSource::HistoricalUser {
            author: Some(author),
        } = &child_grouping.avatar_groups[0].source
        else {
            panic!("agent-authored input must preserve its exact author snapshot");
        };
        assert!(matches!(
            &author.actor,
            PersistedActorRef::AgentExecution(_)
        ));
    }

    #[test]
    fn consecutive_agent_rows_from_one_turn_share_one_avatar_group() {
        let mut projection = ConversationViewState::default();
        let author = agent_author();
        projection.turns.push(TurnView {
            id: "turn".to_owned(),
            phase: TurnPhase::Completed,
            started_at_unix_ms: None,
            completed_at_unix_ms: None,
            error: None,
            permission_profile: None,
            security_summary: None,
            resume: None,
        });
        projection.timeline = vec![
            TimelineEntry {
                id: "tool".to_owned(),
                turn_id: "turn".to_owned(),
                item_id: "tool".to_owned(),
                item_index: 0,
            },
            TimelineEntry {
                id: "answer".to_owned(),
                turn_id: "turn".to_owned(),
                item_id: "answer".to_owned(),
                item_index: 1,
            },
        ];
        let rows = vec![
            TimelineRenderRow::Timeline(TimelineRow {
                key: "tool".to_owned(),
                author: Some(author.clone()),
                kind: TimelineRowKind::Item { timeline_index: 0 },
            }),
            TimelineRenderRow::Timeline(TimelineRow {
                key: "answer".to_owned(),
                author: Some(author.clone()),
                kind: TimelineRowKind::Item { timeline_index: 1 },
            }),
        ];

        let grouping = TimelineGrouping::build(
            &rows,
            &projection,
            None,
            TimelinePresentationContext::default(),
            px(0.),
        );

        assert_eq!(grouping.avatar_groups.len(), 1);
        assert!(matches!(
            &grouping.avatar_groups[0].source,
            TimelineAvatarSource::Agent { .. }
        ));
        assert_eq!(grouping.avatar_groups[0].first_row_index, 0);
        assert_eq!(grouping.avatar_groups[0].last_row_index, 1);
        assert_eq!(grouping.agent_author_for_group_start(0), Some(&author));
        assert_eq!(
            grouping.row_layout(1).top_spacing,
            TimelineRowTopSpacing::GroupMessage
        );
    }

    #[test]
    fn technical_turn_rows_join_the_agent_group() {
        let rows = vec![
            TimelineRenderRow::Timeline(TimelineRow {
                key: "work".to_owned(),
                author: None,
                kind: TimelineRowKind::TurnWorkToggle(
                    pioneer_client::timeline::rows::TurnWorkGroupRow {
                        toggle_key: format!("{SEMANTIC_TURN_WORK_GROUP_PREFIX}turn"),
                        anchor_entry_id: "work".to_owned(),
                        elapsed_ms: None,
                        is_open: false,
                        state: None,
                    },
                ),
            }),
            TimelineRenderRow::Timeline(TimelineRow {
                key: "running".to_owned(),
                author: None,
                kind: TimelineRowKind::RunningTurn(
                    pioneer_client::timeline::labels::RunningTurnDisplay {
                        turn_id: "turn".to_owned(),
                        started_at_unix_ms: None,
                        state: None,
                        message: None,
                        route: None,
                        agent_work_graph: None,
                        permission_profile: None,
                        security_summary: None,
                    },
                ),
            }),
        ];

        let grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext::default(),
            px(0.),
        );

        assert_eq!(grouping.avatar_groups.len(), 1);
        assert_eq!(grouping.avatar_groups[0].first_row_index, 0);
        assert_eq!(grouping.avatar_groups[0].last_row_index, 1);
    }

    #[test]
    fn task_card_and_running_state_keep_one_turn_group_with_exact_agent() {
        let author = agent_author();
        let rows = vec![
            TimelineRenderRow::Timeline(TimelineRow {
                key: "task-card".to_owned(),
                author: Some(author.clone()),
                kind: TimelineRowKind::TurnWorkToggle(
                    pioneer_client::timeline::rows::TurnWorkGroupRow {
                        toggle_key: format!("{SEMANTIC_TURN_WORK_GROUP_PREFIX}turn"),
                        anchor_entry_id: "task-card".to_owned(),
                        elapsed_ms: None,
                        is_open: false,
                        state: None,
                    },
                ),
            }),
            TimelineRenderRow::Timeline(TimelineRow {
                key: "running".to_owned(),
                author: Some(author.clone()),
                kind: TimelineRowKind::RunningTurn(
                    pioneer_client::timeline::labels::RunningTurnDisplay {
                        turn_id: "turn".to_owned(),
                        started_at_unix_ms: None,
                        state: Some(pioneer_protocol::TurnWorkState::Running),
                        message: None,
                        route: None,
                        agent_work_graph: None,
                        permission_profile: None,
                        security_summary: None,
                    },
                ),
            }),
        ];

        let grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext::default(),
            px(0.),
        );

        assert_eq!(grouping.avatar_groups.len(), 1);
        assert_eq!(grouping.avatar_groups[0].first_row_index, 0);
        assert_eq!(grouping.avatar_groups[0].last_row_index, 1);
        assert_eq!(grouping.agent_author_for_group_start(0), Some(&author));
    }

    #[test]
    fn running_child_turn_keeps_the_old_dino_and_the_exact_agent_identity() {
        let author = agent_author();
        let rows = vec![TimelineRenderRow::Timeline(TimelineRow {
            key: "running".to_owned(),
            author: Some(author.clone()),
            kind: TimelineRowKind::RunningTurn(
                pioneer_client::timeline::labels::RunningTurnDisplay {
                    turn_id: "turn".to_owned(),
                    started_at_unix_ms: None,
                    state: None,
                    message: None,
                    route: None,
                    agent_work_graph: None,
                    permission_profile: None,
                    security_summary: None,
                },
            ),
        })];

        let root_grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext::default(),
            px(0.),
        );
        let child_grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext {
                task_child_thread: true,
            },
            px(0.),
        );

        let TimelineAvatarSource::Agent {
            author: root_author,
            shows_running_dino: root_shows_running_dino,
        } = &root_grouping.avatar_groups[0].source
        else {
            panic!("root running row must use an agent avatar");
        };
        let TimelineAvatarSource::Agent {
            author: child_author,
            shows_running_dino: child_shows_running_dino,
        } = &child_grouping.avatar_groups[0].source
        else {
            panic!("child running row must use an agent avatar");
        };
        assert_eq!(root_author.as_ref(), Some(&author));
        assert_eq!(child_author.as_ref(), Some(&author));
        assert!(!root_shows_running_dino);
        assert!(*child_shows_running_dino);
    }

    #[test]
    fn layout_index_uses_the_same_item_sizes_as_the_virtual_list() {
        let rows = vec![
            left_message("one", "PAAAAAAAAAAAAAAAAAAAA"),
            left_message("two", "PAAAAAAAAAAAAAAAAAAAA"),
        ];
        let grouping = TimelineGrouping::build(
            &rows,
            &ConversationViewState::default(),
            None,
            TimelinePresentationContext::default(),
            px(4.),
        );
        let index = TimelineLayoutIndex::new(
            grouping.clone(),
            Rc::new(vec![
                gpui_kit::size(px(100.), px(70.)),
                gpui_kit::size(px(100.), px(50.)),
            ]),
        );

        assert_eq!(
            index.avatar_group_bounds(&grouping.avatar_groups[0]),
            Some((px(40.), px(76.)))
        );
    }
}
