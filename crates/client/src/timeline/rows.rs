//! Timeline row models used by platform renderers.

use crate::timeline::labels::RunningTurnDisplay;
use pioneer_protocol::{
    PersistedActorRef, PrincipalId, ThreadMode, TimelineReplySummary, TurnAuthorSnapshot,
    TurnMention, TurnMessageRevisionChangeKind, TurnMessageRevisionsPageResponse, UserInput,
    UserMessageAttachment,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimelineReplyState {
    Available,
    Deleted,
    Unavailable,
}

pub fn timeline_reply_state(reply: &TimelineReplySummary) -> TimelineReplyState {
    if reply.deleted {
        TimelineReplyState::Deleted
    } else if reply.text.is_some() || reply.author.is_some() {
        TimelineReplyState::Available
    } else {
        TimelineReplyState::Unavailable
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UserMessageAlignment {
    CurrentPrincipal,
    Other,
}

/// Alignment is a current-session rendering fact. The historical author
/// snapshot remains untouched even if the member was renamed or removed.
pub fn user_message_alignment(
    presentation: &UserMessagePresentation,
    current_principal_id: &PrincipalId,
) -> UserMessageAlignment {
    match presentation.author.as_ref().map(|author| &author.actor) {
        Some(PersistedActorRef::Principal(author_id)) if author_id == current_principal_id => {
            UserMessageAlignment::CurrentPrincipal
        }
        _ => UserMessageAlignment::Other,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserMessageMutationAvailability {
    pub can_edit: bool,
    pub can_delete: bool,
}

/// Shell actions are offered only for the current principal's live explicit
/// Message. Gateway authorization and expected revision remain authoritative.
pub fn user_message_mutation_availability(
    presentation: &UserMessagePresentation,
    current_principal_id: &PrincipalId,
) -> UserMessageMutationAvailability {
    let owns_message = matches!(
        presentation.author.as_ref().map(|author| &author.actor),
        Some(PersistedActorRef::Principal(author_id)) if author_id == current_principal_id
    );
    let mutable = owns_message && presentation.mode == ThreadMode::Message && !presentation.deleted;

    UserMessageMutationAvailability {
        can_edit: mutable,
        can_delete: mutable,
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MessageRevisionPresentation {
    pub revision: u64,
    pub change_kind: TurnMessageRevisionChangeKind,
    pub changed_by: PersistedActorRef,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<TurnMention>,
    pub content_redacted: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MessageRevisionPagePresentation {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub revisions: Vec<MessageRevisionPresentation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Projects only display-safe revision content. Attachment inputs remain
/// represented by the existing authenticated artifact/file plane and are not
/// copied into presentation schemas as URLs or bytes.
pub fn project_message_revision_page(
    response: TurnMessageRevisionsPageResponse,
) -> MessageRevisionPagePresentation {
    MessageRevisionPagePresentation {
        workspace_id: response.workspace_id,
        thread_id: response.thread_id,
        turn_id: response.turn_id,
        revisions: response
            .revisions
            .into_iter()
            .map(|revision| {
                let content_redacted = revision.input.is_none();
                let text = revision.input.as_ref().map(|inputs| {
                    inputs
                        .iter()
                        .filter_map(|input| match input {
                            UserInput::Text { text, .. } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                });
                MessageRevisionPresentation {
                    revision: revision.revision,
                    change_kind: revision.change_kind,
                    changed_by: revision.changed_by,
                    created_at: revision.created_at,
                    text,
                    mentions: if content_redacted {
                        Vec::new()
                    } else {
                        revision.mentions
                    },
                    content_redacted,
                }
            })
            .collect(),
        next_cursor: response.next_cursor,
    }
}

/// Authoritative collaboration metadata attached to a rendered user-message
/// row. It mirrors disclosed server fields; shells must not reconstruct it by
/// parsing text or by joining a mutable member directory.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UserMessagePresentation {
    pub workspace_id: String,
    pub thread_id: String,
    pub block_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub mode: ThreadMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<TurnAuthorSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<pioneer_protocol::SafeRouteProvenance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply: Option<TimelineReplySummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_state: Option<TimelineReplyState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<TurnMention>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<UserMessageAttachment>,
    pub revision: u64,
    pub edited: bool,
    pub deleted: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TurnWorkGroupRow {
    pub toggle_key: String,
    pub anchor_entry_id: String,
    pub elapsed_ms: Option<u64>,
    pub is_open: bool,
    /// Server-owned lifecycle state; clients must not infer it from row order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<pioneer_protocol::TurnWorkState>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum TimelineCoalescedToolsKind {
    CompletedTaskTools,
    RepeatedTaskWait,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineCoalescedToolsRow {
    pub toggle_key: String,
    pub count: usize,
    pub is_open: bool,
    pub kind: TimelineCoalescedToolsKind,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum TimelineRowKind {
    Item {
        timeline_index: usize,
    },
    UserMessage {
        timeline_index: usize,
        presentation: UserMessagePresentation,
    },
    TurnWorkToggle(TurnWorkGroupRow),
    CoalescedTools(TimelineCoalescedToolsRow),
    RunningTurn(RunningTurnDisplay),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TimelineRow {
    pub key: String,
    pub kind: TimelineRowKind,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        TurnMessageRevision, TurnMessageRevisionChangeKind, TurnMessageRevisionsPageResponse,
    };

    fn principal(value: &str) -> PrincipalId {
        PrincipalId::new(value).expect("valid principal id")
    }

    fn presentation(author: Option<TurnAuthorSnapshot>) -> UserMessagePresentation {
        UserMessagePresentation {
            workspace_id: "workspace".to_owned(),
            thread_id: "thread".to_owned(),
            block_id: "block".to_owned(),
            turn_id: "turn".to_owned(),
            item_id: "item".to_owned(),
            mode: ThreadMode::Message,
            author,
            route: None,
            reply: None,
            reply_state: None,
            mentions: Vec::new(),
            attachments: Vec::new(),
            revision: 0,
            edited: false,
            deleted: false,
        }
    }

    #[test]
    fn mutation_actions_require_owned_explicit_live_message() {
        let current = principal("PCCCCCCCCCCCCCCCCCCCC");
        let author = TurnAuthorSnapshot {
            actor: PersistedActorRef::Principal(current.clone()),
            display_name: "Current".to_owned(),
            nickname: "current".to_owned(),
            avatar_revision: None,
            agent: None,
        };
        let mut message = presentation(Some(author));

        assert_eq!(
            user_message_mutation_availability(&message, &current),
            UserMessageMutationAvailability {
                can_edit: true,
                can_delete: true,
            }
        );

        message.mode = ThreadMode::Chat;
        assert!(!user_message_mutation_availability(&message, &current).can_edit);
        message.mode = ThreadMode::Message;
        message.deleted = true;
        assert!(!user_message_mutation_availability(&message, &current).can_delete);
        message.deleted = false;
        message.author = None;
        assert!(!user_message_mutation_availability(&message, &current).can_edit);
        message.author = Some(TurnAuthorSnapshot {
            actor: PersistedActorRef::Principal(principal("PDDDDDDDDDDDDDDDDDDDD")),
            display_name: "Other".to_owned(),
            nickname: "other".to_owned(),
            avatar_revision: None,
            agent: None,
        });
        assert!(!user_message_mutation_availability(&message, &current).can_delete);
    }

    #[test]
    fn current_alignment_uses_only_exact_principal_identity() {
        let alice = principal("PAAAAAAAAAAAAAAAAAAAA");
        let bob = principal("PBBBBBBBBBBBBBBBBBBBB");
        let historical = TurnAuthorSnapshot {
            actor: PersistedActorRef::Principal(alice.clone()),
            display_name: "Removed Alice".to_owned(),
            nickname: "alice-old".to_owned(),
            avatar_revision: Some("old-avatar".to_owned()),
            agent: None,
        };
        let row = presentation(Some(historical.clone()));

        assert_eq!(
            user_message_alignment(&row, &alice),
            UserMessageAlignment::CurrentPrincipal
        );
        assert_eq!(
            user_message_alignment(&row, &bob),
            UserMessageAlignment::Other
        );
        assert_eq!(row.author, Some(historical));
        assert_eq!(
            user_message_alignment(&presentation(None), &alice),
            UserMessageAlignment::Other
        );
    }

    #[test]
    fn reply_state_has_safe_deleted_and_off_page_fallbacks() {
        let unavailable = TimelineReplySummary {
            turn_id: "off-page".to_owned(),
            author: None,
            text: None,
            deleted: false,
        };
        let deleted = TimelineReplySummary {
            deleted: true,
            ..unavailable.clone()
        };
        let available = TimelineReplySummary {
            text: Some("hello".to_owned()),
            ..unavailable.clone()
        };

        assert_eq!(
            timeline_reply_state(&unavailable),
            TimelineReplyState::Unavailable
        );
        assert_eq!(timeline_reply_state(&deleted), TimelineReplyState::Deleted);
        assert_eq!(
            timeline_reply_state(&available),
            TimelineReplyState::Available
        );
    }

    #[test]
    fn revision_page_preserves_pagination_and_redacts_deleted_content() {
        let alice = principal("PAAAAAAAAAAAAAAAAAAAA");
        let page = project_message_revision_page(TurnMessageRevisionsPageResponse {
            workspace_id: "workspace".to_owned(),
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            revisions: vec![
                TurnMessageRevision {
                    turn_id: "turn".to_owned(),
                    revision: 1,
                    change_kind: TurnMessageRevisionChangeKind::Edit,
                    changed_by: PersistedActorRef::Principal(alice),
                    created_at: 10,
                    input: Some(vec![
                        UserInput::Text {
                            text: "first".to_owned(),
                            text_elements: Vec::new(),
                        },
                        UserInput::File {
                            url: "authenticated://must-not-leak".to_owned(),
                        },
                        UserInput::Text {
                            text: "second".to_owned(),
                            text_elements: Vec::new(),
                        },
                    ]),
                    mentions: Vec::new(),
                },
                TurnMessageRevision {
                    turn_id: "turn".to_owned(),
                    revision: 2,
                    change_kind: TurnMessageRevisionChangeKind::Delete,
                    changed_by: PersistedActorRef::System,
                    created_at: 20,
                    input: None,
                    mentions: vec![TurnMention {
                        principal_id: principal("PBBBBBBBBBBBBBBBBBBBB"),
                        nickname: "must-redact".to_owned(),
                    }],
                },
            ],
            next_cursor: Some("next".to_owned()),
        });

        assert_eq!(page.next_cursor.as_deref(), Some("next"));
        assert_eq!(page.revisions[0].text.as_deref(), Some("first\nsecond"));
        assert!(!page.revisions[0].content_redacted);
        assert_eq!(page.revisions[1].text, None);
        assert!(page.revisions[1].mentions.is_empty());
        assert!(page.revisions[1].content_redacted);
    }
}
