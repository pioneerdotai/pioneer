use super::{TimelinePresentationContext, TimelineRenderRow, is_current_principal_user_message};
use crate::app::conversation::ConversationViewState;
use gpui::{Pixels, Size, px};
use pioneer_client::timeline::{
    labels::is_task_timeline_agent_message,
    rows::{TimelineRow, TimelineRowKind},
    semantic_render::SEMANTIC_TURN_WORK_GROUP_PREFIX,
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
    HistoricalUser { author: Option<TurnAuthorSnapshot> },
    Agent { shows_running_dino: bool },
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
    pub(crate) fn build(
        rows: &[TimelineRenderRow],
        projection: &ConversationViewState,
        current_principal_id: Option<&str>,
        presentation_context: TimelinePresentationContext,
        message_text_bottom_inset: Pixels,
    ) -> Rc<Self> {
        let descriptors = rows
            .iter()
            .map(|row| {
                row_cluster_descriptor(row, projection, current_principal_id, presentation_context)
            })
            .collect::<Vec<_>>();
        let mut row_layouts = vec![TimelineRowLayout::default(); rows.len()];
        let mut avatar_groups = Vec::new();
        let mut index = 0;

        while index < rows.len() {
            let Some(descriptor) = descriptors[index].as_ref() else {
                row_layouts[index].top_spacing = if index == 0 {
                    TimelineRowTopSpacing::TimelineStart
                } else {
                    TimelineRowTopSpacing::Standard
                };
                index += 1;
                continue;
            };

            let mut end = index;
            while end + 1 < rows.len()
                && descriptors[end + 1]
                    .as_ref()
                    .is_some_and(|next| next.key.eq(&descriptor.key))
            {
                end += 1;
            }

            let top_spacing = if index == 0 {
                TimelineRowTopSpacing::TimelineStart
            } else {
                TimelineRowTopSpacing::GroupStart
            };
            for (offset, layout) in row_layouts[index..=end].iter_mut().enumerate() {
                layout.top_spacing = if offset == 0 {
                    top_spacing
                } else {
                    TimelineRowTopSpacing::GroupMessage
                };
                layout.avatar_group_kind = descriptor.avatar_kind;
                layout.starts_avatar_group = offset == 0 && descriptor.avatar_kind.is_some();
            }

            if let Some(source) = descriptor.avatar_source.clone() {
                let source = match source {
                    TimelineAvatarSource::Agent { .. } => TimelineAvatarSource::Agent {
                        shows_running_dino: presentation_context.task_child_thread
                            && rows[index..=end].iter().any(|row| {
                                matches!(
                                    row,
                                    TimelineRenderRow::Timeline(TimelineRow {
                                        kind: TimelineRowKind::RunningTurn(_),
                                        ..
                                    })
                                )
                            }),
                    },
                    source => source,
                };
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

            index = end + 1;
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum TimelineClusterKey {
    CurrentPrincipal,
    HistoricalUser(PersistedActorRef),
    HistoricalUnknown(String),
    Agent(String),
}

#[derive(Clone, Debug)]
struct TimelineClusterDescriptor {
    key: TimelineClusterKey,
    avatar_kind: Option<TimelineAvatarGroupKind>,
    avatar_source: Option<TimelineAvatarSource>,
}

fn row_cluster_descriptor(
    row: &TimelineRenderRow,
    projection: &ConversationViewState,
    current_principal_id: Option<&str>,
    presentation_context: TimelinePresentationContext,
) -> Option<TimelineClusterDescriptor> {
    if let TimelineRenderRow::Timeline(TimelineRow {
        kind: TimelineRowKind::UserMessage { presentation, .. },
        ..
    }) = row
    {
        if is_current_principal_user_message(row, current_principal_id, presentation_context) {
            return Some(TimelineClusterDescriptor {
                key: TimelineClusterKey::CurrentPrincipal,
                avatar_kind: None,
                avatar_source: None,
            });
        }

        let author = presentation.author.clone();
        let key = author
            .as_ref()
            .map(|author| TimelineClusterKey::HistoricalUser(author.actor.clone()))
            .unwrap_or_else(|| TimelineClusterKey::HistoricalUnknown(row.key().to_owned()));
        return Some(TimelineClusterDescriptor {
            key,
            avatar_kind: Some(TimelineAvatarGroupKind::HistoricalUser),
            avatar_source: Some(TimelineAvatarSource::HistoricalUser { author }),
        });
    }

    if let TimelineRenderRow::Timeline(TimelineRow {
        kind: TimelineRowKind::Item { timeline_index },
        ..
    }) = row
        && projection
            .timeline
            .get(*timeline_index)
            .and_then(|entry| projection.item_for_timeline_entry(entry))
            .is_some_and(|item| matches!(&item.item, TurnItem::UserMessage { .. }))
    {
        return Some(TimelineClusterDescriptor {
            key: TimelineClusterKey::CurrentPrincipal,
            avatar_kind: None,
            avatar_source: None,
        });
    }

    let turn_id = timeline_render_row_turn_id(row, projection)
        .unwrap_or_else(|| format!("standalone::{}", row.key()));
    Some(TimelineClusterDescriptor {
        key: TimelineClusterKey::Agent(turn_id),
        avatar_kind: Some(TimelineAvatarGroupKind::Agent),
        avatar_source: Some(TimelineAvatarSource::Agent {
            shows_running_dino: false,
        }),
    })
}

fn timeline_render_row_turn_id(
    row: &TimelineRenderRow,
    projection: &ConversationViewState,
) -> Option<String> {
    match row {
        TimelineRenderRow::Timeline(TimelineRow {
            kind: TimelineRowKind::Item { timeline_index },
            ..
        }) => projection
            .timeline
            .get(*timeline_index)
            .map(|entry| entry.turn_id.clone()),
        TimelineRenderRow::Timeline(TimelineRow {
            kind: TimelineRowKind::UserMessage { .. },
            ..
        }) => None,
        TimelineRenderRow::Timeline(TimelineRow {
            kind: TimelineRowKind::TurnWorkToggle(group),
            ..
        }) => projection
            .timeline
            .iter()
            .find(|entry| {
                entry.id == group.anchor_entry_id || entry.item_id == group.anchor_entry_id
            })
            .map(|entry| entry.turn_id.clone())
            .or_else(|| turn_id_from_toggle_key(group.toggle_key.as_str())),
        TimelineRenderRow::Timeline(TimelineRow {
            kind: TimelineRowKind::CoalescedTools(group),
            ..
        }) => turn_id_from_toggle_key(group.toggle_key.as_str()),
        TimelineRenderRow::Timeline(TimelineRow {
            kind: TimelineRowKind::RunningTurn(running_turn),
            ..
        }) => Some(running_turn.turn_id.clone()),
        TimelineRenderRow::PendingRequest(row) => row.request.turn_id.clone(),
    }
}

fn turn_id_from_toggle_key(toggle_key: &str) -> Option<String> {
    toggle_key
        .strip_prefix(SEMANTIC_TURN_WORK_GROUP_PREFIX)
        .or_else(|| toggle_key.strip_prefix("turn-work-group::"))
        .filter(|turn_id| !turn_id.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::conversation::TimelineEntry;
    use pioneer_client::timeline::rows::UserMessagePresentation;
    use pioneer_protocol::{PrincipalId, ThreadMode};

    fn principal(value: &str) -> PrincipalId {
        PrincipalId::new(value).expect("valid principal id")
    }

    fn left_message(key: &str, author_id: &str) -> TimelineRenderRow {
        TimelineRenderRow::Timeline(TimelineRow {
            key: key.to_owned(),
            kind: TimelineRowKind::UserMessage {
                timeline_index: 0,
                presentation: UserMessagePresentation {
                    workspace_id: "workspace".to_owned(),
                    thread_id: "thread".to_owned(),
                    block_id: key.to_owned(),
                    turn_id: key.to_owned(),
                    item_id: key.to_owned(),
                    mode: ThreadMode::Message,
                    author: Some(TurnAuthorSnapshot {
                        actor: PersistedActorRef::Principal(principal(author_id)),
                        display_name: author_id.to_owned(),
                        nickname: author_id.to_owned(),
                        avatar_revision: None,
                    }),
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
        TimelineRenderRow::Timeline(TimelineRow {
            key: key.to_owned(),
            kind: TimelineRowKind::UserMessage {
                timeline_index: 0,
                presentation: UserMessagePresentation {
                    workspace_id: "workspace".to_owned(),
                    thread_id: "child-thread".to_owned(),
                    block_id: format!("turn:{key}:user"),
                    turn_id: key.to_owned(),
                    item_id: format!("turn:{key}:user"),
                    mode: ThreadMode::Agent,
                    author: Some(TurnAuthorSnapshot {
                        actor: PersistedActorRef::System,
                        display_name: "System".to_owned(),
                        nickname: "system".to_owned(),
                        avatar_revision: None,
                    }),
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
    fn task_child_input_uses_current_principal_alignment_without_an_avatar() {
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
        assert!(child_grouping.avatar_groups.is_empty());
        assert_eq!(child_grouping.row_layout(0).avatar_group_kind, None,);
    }

    #[test]
    fn consecutive_agent_rows_from_one_turn_share_one_avatar_group() {
        let mut projection = ConversationViewState::default();
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
                kind: TimelineRowKind::Item { timeline_index: 0 },
            }),
            TimelineRenderRow::Timeline(TimelineRow {
                key: "answer".to_owned(),
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
                kind: TimelineRowKind::TurnWorkToggle(
                    pioneer_client::timeline::rows::TurnWorkGroupRow {
                        toggle_key: format!("{SEMANTIC_TURN_WORK_GROUP_PREFIX}turn"),
                        anchor_entry_id: "work".to_owned(),
                        elapsed_ms: None,
                        is_open: false,
                    },
                ),
            }),
            TimelineRenderRow::Timeline(TimelineRow {
                key: "running".to_owned(),
                kind: TimelineRowKind::RunningTurn(
                    pioneer_client::timeline::labels::RunningTurnDisplay {
                        turn_id: "turn".to_owned(),
                        started_at_unix_ms: None,
                        state: None,
                        message: None,
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
    fn running_child_turn_replaces_only_its_agent_avatar_with_the_dino() {
        let rows = vec![TimelineRenderRow::Timeline(TimelineRow {
            key: "running".to_owned(),
            kind: TimelineRowKind::RunningTurn(
                pioneer_client::timeline::labels::RunningTurnDisplay {
                    turn_id: "turn".to_owned(),
                    started_at_unix_ms: None,
                    state: None,
                    message: None,
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

        assert!(matches!(
            &root_grouping.avatar_groups[0].source,
            TimelineAvatarSource::Agent {
                shows_running_dino: false
            }
        ));
        assert!(matches!(
            &child_grouping.avatar_groups[0].source,
            TimelineAvatarSource::Agent {
                shows_running_dino: true
            }
        ));
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
                gpui::size(px(100.), px(70.)),
                gpui::size(px(100.), px(50.)),
            ]),
        );

        assert_eq!(
            index.avatar_group_bounds(&grouping.avatar_groups[0]),
            Some((px(40.), px(76.)))
        );
    }
}
