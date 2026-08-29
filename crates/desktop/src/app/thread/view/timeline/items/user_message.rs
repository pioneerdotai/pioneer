use super::super::markdown::CodeHighlightPolicy;
use super::super::{
    TimelineRowTopSpacing,
    layout::{
        TIMELINE_AVATAR_RAIL_WIDTH, TIMELINE_CONTENT_HORIZONTAL_PADDING,
        TIMELINE_MESSAGE_END_BOTTOM_SPACING,
    },
};
use crate::app::{
    conversation::{ItemView, TimelineEntry},
    root::PioneerDesktop,
};
use crate::assets::PioneerIconName;
use chrono::{Local, TimeZone};
use gpui::{prelude::*, *};
use gpui_component::{
    Icon, StyledExt, h_flex,
    menu::{ContextMenuExt, PopupMenuItem},
    theme::ActiveTheme,
    v_flex,
};
use pioneer_client::composer::state_machine::{
    ComposerDomainAction, composer_reply_target_from_visible_message,
};
use pioneer_client::timeline::labels::{
    ParsedUserAttachment, ParsedUserAttachmentKind, parse_user_attachments,
    stable_user_message_attachment_chip_id,
};
use pioneer_client::timeline::rows::{
    TimelineReplyState, UserMessageAlignment, UserMessagePresentation,
    user_message_mutation_availability,
};
use pioneer_protocol::{TurnAuthorSnapshot, TurnItem};
use std::path::PathBuf;

impl PioneerDesktop {
    pub(in crate::app) fn render_item_user_message(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        presentation: Option<&UserMessagePresentation>,
        exact_author: Option<&TurnAuthorSnapshot>,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (raw_text, attachments) = match item {
            TurnItem::UserMessage {
                text, attachments, ..
            } => (text.as_str(), parse_user_attachments(attachments)),
            _ => (Self::timeline_entry_text(item_view), Vec::new()),
        };

        let timestamp_text = item_view
            .started_at_unix_ms
            .or(item_view.updated_at_unix_ms)
            .or(item_view.completed_at_unix_ms)
            .and_then(|ts| Local.timestamp_millis_opt(ts).single())
            .map(|dt| dt.format("%d.%m.%Y %H:%M").to_string())
            .unwrap_or_default();
        let last_edited_timestamp_text = item_view
            .updated_at_unix_ms
            .or(item_view.started_at_unix_ms)
            .or(item_view.completed_at_unix_ms)
            .and_then(|ts| Local.timestamp_millis_opt(ts).single())
            .map(|dt| dt.format("%d.%m.%Y %H:%M").to_string())
            .unwrap_or_default();

        let copy_text = raw_text.to_owned();
        let current_principal_id = self
            .gateway
            .current_auth
            .as_ref()
            .map(|auth| &auth.principal.id);
        let alignment = if super::super::user_message_uses_current_principal_alignment(
            presentation,
            exact_author,
            current_principal_id.map(|principal_id| principal_id.as_str()),
        ) {
            UserMessageAlignment::CurrentPrincipal
        } else {
            UserMessageAlignment::Other
        };
        let mutation_availability =
            presentation
                .zip(current_principal_id)
                .map(|(presentation, principal_id)| {
                    user_message_mutation_availability(presentation, principal_id)
                });
        let editable_message = mutation_availability
            .is_some_and(|value| value.can_edit)
            .then(|| presentation.cloned())
            .flatten();
        let deletable_message = mutation_availability
            .is_some_and(|value| value.can_delete)
            .then(|| presentation.cloned())
            .flatten();
        let author = self.current_timeline_author_presentation(exact_author);
        let author_label = super::super::timeline_agent_label(exact_author).unwrap_or_else(|| {
            if super::super::timeline_agent_execution_author(exact_author).is_some() {
                t!("chat.composer.mode.agent_label").to_string()
            } else if author.display_name == "?" {
                t!("timeline.message.unknown_author").to_string()
            } else if author.nickname.is_empty() {
                author.display_name
            } else {
                format!("{} · @{}", author.display_name, author.nickname)
            }
        });
        let active_workspace_id = self
            .current_active_thread_id()
            .and_then(|thread_id| self.thread_workspace_id(thread_id))
            .map(str::to_owned);
        let reply_target = presentation.and_then(|presentation| {
            composer_reply_target_from_visible_message(presentation, raw_text)
        });
        let editable_artifacts = attachments
            .iter()
            .filter_map(|attachment| attachment.artifact.clone())
            .collect::<Vec<_>>();

        let context_reply_target = reply_target.clone();
        let context_editable_message = editable_message.clone();
        let context_deletable_message = deletable_message.clone();
        let context_can_copy = !presentation.is_some_and(|presentation| presentation.deleted);
        let context_last_edited = presentation
            .filter(|presentation| presentation.edited)
            .map(|presentation| (presentation.thread_id.clone(), presentation.turn_id.clone()));
        let context_timestamp = timestamp_text;
        let context_last_edited_timestamp = last_edited_timestamp_text;
        let context_copy_text = copy_text.clone();
        let context_edit_text = raw_text.to_owned();
        let desktop_entity = cx.entity().clone();
        let mut row = div().flex().w_full().justify_center();

        row = row.pt(top_spacing.pixels());

        if is_last_row {
            row = row.pb(TIMELINE_MESSAGE_END_BOTTOM_SPACING);
        }

        row.child(
            v_flex()
                .id(("timeline-user-message", entry.item_index))
                .w(content_width)
                .px(TIMELINE_CONTENT_HORIZONTAL_PADDING)
                .when(
                    alignment == UserMessageAlignment::CurrentPrincipal,
                    |this| this.items_end(),
                )
                .when(alignment == UserMessageAlignment::Other, |this| {
                    this.items_start()
                })
                .group(format!("user-message-{}", item_view.id))
                .context_menu(move |menu, _, _| {
                    let mut menu = menu;
                    if let Some(reply_target) = context_reply_target.clone() {
                        let desktop_entity = desktop_entity.clone();
                        menu = menu.item(
                            PopupMenuItem::new(t!("timeline.message.reply_action").to_string())
                                .icon(PioneerIconName::Reply)
                                .on_click(move |_, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        if view.composer_edit_target.is_some() {
                                            view.cancel_composer_message_edit(window, cx);
                                        }
                                        view.reduce_composer_domain(
                                            ComposerDomainAction::SetReplyTarget {
                                                target: reply_target.clone(),
                                            },
                                        );
                                        view.composer_state.update(cx, |state, cx| {
                                            state.focus(window, cx);
                                        });
                                        cx.notify();
                                    });
                                }),
                        );
                    }
                    if context_can_copy {
                        let copy_text = context_copy_text.clone();
                        let desktop_entity = desktop_entity.clone();
                        menu = menu.item(
                            PopupMenuItem::new(t!("timeline.message.copy_action").to_string())
                                .icon(PioneerIconName::Copy)
                                .on_click(move |_, _, cx| {
                                    let _ = desktop_entity.update(cx, |_, cx| {
                                        cx.write_to_clipboard(ClipboardItem::new_string(
                                            copy_text.clone(),
                                        ));
                                    });
                                }),
                        );
                    }
                    if let Some(editable_message) = context_editable_message.clone() {
                        let text = context_edit_text.clone();
                        let artifacts = editable_artifacts.clone();
                        let desktop_entity = desktop_entity.clone();
                        menu = menu.item(
                            PopupMenuItem::new(t!("timeline.message.edit_action").to_string())
                                .icon(PioneerIconName::SquarePen)
                                .on_click(move |_, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.start_composer_message_edit(
                                            editable_message.clone(),
                                            text.clone(),
                                            artifacts.clone(),
                                            window,
                                            cx,
                                        );
                                    });
                                }),
                        );
                    }
                    if let Some(deletable_message) = context_deletable_message.clone() {
                        let desktop_entity = desktop_entity.clone();
                        menu = menu.item(
                            PopupMenuItem::new(t!("timeline.message.delete_action").to_string())
                                .icon(PioneerIconName::Trash)
                                .on_click(move |_, window, cx| {
                                    let _ = desktop_entity.update(cx, |view, cx| {
                                        view.confirm_delete_message(
                                            deletable_message.clone(),
                                            window,
                                            cx,
                                        );
                                    });
                                }),
                        );
                    }
                    if !context_timestamp.is_empty() || context_last_edited.is_some() {
                        menu = menu.separator();
                    }
                    if let Some((thread_id, turn_id)) = context_last_edited.clone() {
                        let desktop_entity = desktop_entity.clone();
                        let label = SharedString::from(format!(
                            "{} · {}",
                            t!("timeline.message.last_edited"),
                            context_last_edited_timestamp
                        ));
                        menu = menu.item(
                            PopupMenuItem::element(move |_, _| {
                                div().text_xs().child(label.clone())
                            })
                            .icon(PioneerIconName::RotateCcwClock)
                            .on_click(move |_, window, cx| {
                                let _ = desktop_entity.update(cx, |view, cx| {
                                    view.open_message_revision_history(
                                        thread_id.clone(),
                                        turn_id.clone(),
                                        window,
                                        cx,
                                    );
                                });
                            }),
                        );
                    }
                    if !context_timestamp.is_empty() {
                        let label = SharedString::from(context_timestamp.clone());
                        menu = menu.item(
                            PopupMenuItem::element(move |_, _| {
                                div().text_xs().child(label.clone())
                            })
                            .icon(PioneerIconName::Clock),
                        );
                    }
                    menu
                })
                .when(
                    alignment == UserMessageAlignment::Other
                        && !matches!(
                            top_spacing,
                            TimelineRowTopSpacing::Compact | TimelineRowTopSpacing::GroupMessage
                        ),
                    |this| {
                        this.child(
                            h_flex()
                                .ml(TIMELINE_AVATAR_RAIL_WIDTH)
                                .h(px(32.))
                                .max_w_3_4()
                                .min_w_0()
                                .items_center()
                                .child(
                                    div()
                                        .min_w_0()
                                        .text_sm()
                                        .font_semibold()
                                        .child(author_label),
                                ),
                        )
                    },
                )
                .when_some(
                    presentation.and_then(|value| value.reply.as_ref()),
                    |this, reply| {
                        let label = match presentation.and_then(|value| value.reply_state) {
                            Some(TimelineReplyState::Deleted) => {
                                t!("timeline.message.reply_deleted").to_string()
                            }
                            Some(TimelineReplyState::Unavailable) => {
                                t!("timeline.message.reply_unavailable").to_string()
                            }
                            _ => reply.text.clone().unwrap_or_else(|| {
                                t!("timeline.message.reply_unavailable").to_string()
                            }),
                        };
                        this.child(
                            div()
                                .id(SharedString::from(format!(
                                    "timeline-reply-target-{}",
                                    reply.turn_id
                                )))
                                .max_w_3_4()
                                .min_w_0()
                                .when(alignment == UserMessageAlignment::Other, |this| {
                                    this.ml(TIMELINE_AVATAR_RAIL_WIDTH)
                                })
                                .mt_2()
                                .px_3()
                                .py_2()
                                .text_xs()
                                .opacity(0.75)
                                .child(label),
                        )
                    },
                )
                .when(
                    !presentation.is_some_and(|value| value.deleted) && !attachments.is_empty(),
                    |this| {
                        this.child(self.render_user_message_attachment_badges(
                            item_view.id.as_str(),
                            attachments.clone(),
                            active_workspace_id.clone(),
                            alignment == UserMessageAlignment::CurrentPrincipal,
                            cx,
                        ))
                    },
                )
                .child(
                    div()
                        .min_w_0()
                        .overflow_hidden()
                        .when(
                            alignment == UserMessageAlignment::CurrentPrincipal,
                            |this| this.max_w_3_4().bg(cx.theme().muted).rounded_2xl().p_4(),
                        )
                        .when(alignment == UserMessageAlignment::Other, |this| {
                            this.w_full().pl(TIMELINE_AVATAR_RAIL_WIDTH)
                        })
                        .child(
                            v_flex()
                                .when(presentation.is_some_and(|value| value.deleted), |this| {
                                    this.text_sm()
                                        .line_height(relative(1.65))
                                        .italic()
                                        .text_color(cx.theme().muted_foreground)
                                        .child(t!("timeline.message.deleted").to_string())
                                })
                                .when(
                                    !presentation.is_some_and(|value| value.deleted)
                                        && !raw_text.trim().is_empty(),
                                    |this| {
                                        this.child(self.render_markdown_auto(
                                            item_view.id.as_str(),
                                            raw_text,
                                            item_view.partial_markdown.as_ref(),
                                            CodeHighlightPolicy::Disabled,
                                            cx,
                                        ))
                                    },
                                ),
                        ),
                ),
        )
        .into_any_element()
    }

    fn render_user_message_attachment_badges(
        &self,
        item_id: &str,
        attachments: Vec<ParsedUserAttachment>,
        workspace_id: Option<String>,
        align_end: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let item_id = item_id.to_owned();
        let can_read_artifacts = self.active_artifact_presentation_policy().can_open;
        h_flex()
            .w_full()
            .min_w_0()
            .when(align_end, |this| this.justify_end())
            .when(!align_end, |this| {
                this.pl(TIMELINE_AVATAR_RAIL_WIDTH).justify_start()
            })
            .items_center()
            .flex_wrap()
            .gap_1p5()
            .pb_2()
            .children(
                attachments
                    .into_iter()
                    .enumerate()
                    .map(|(chip_index, attachment)| {
                        let chip_id =
                            stable_user_message_attachment_chip_id(item_id.as_str(), chip_index);
                        let artifact = attachment.artifact.clone();
                        let preview_image_path = artifact.as_ref().and_then(|artifact| {
                            if !can_read_artifacts {
                                return None;
                            }
                            if let Some(workspace_id) = workspace_id.as_deref() {
                                self.request_thread_artifact_preview_load(
                                    workspace_id,
                                    artifact,
                                    cx,
                                );
                            }
                            self.thread_artifacts
                                .preview_square_image_path(artifact)
                                .map(PathBuf::from)
                        });
                        let artifact_id = artifact
                            .as_ref()
                            .filter(|_| can_read_artifacts)
                            .map(|artifact| artifact.artifact_id.clone());

                        h_flex()
                            .id(("timeline-user-attachment-chip", chip_id))
                            .h(px(32.))
                            .max_w(px(196.))
                            .min_w_0()
                            .flex_initial()
                            .pl_1()
                            .pr_2()
                            .rounded_full()
                            .border_1()
                            .border_color(cx.theme().border)
                            .items_center()
                            .gap_2()
                            .child(self.render_user_message_attachment_preview(
                                preview_image_path,
                                attachment.kind,
                                cx,
                            ))
                            .child(
                                div()
                                    .flex_1()
                                    .min_w_0()
                                    .overflow_hidden()
                                    .whitespace_nowrap()
                                    .text_ellipsis()
                                    .text_xs()
                                    .child(attachment.display_name),
                            )
                            .when_some(artifact_id, |this, artifact_id| {
                                this.hover(|this| this.opacity(0.8)).on_click(cx.listener(
                                    move |view, _, _, cx| {
                                        view.open_thread_artifact_in_sidebar(
                                            artifact_id.clone(),
                                            cx,
                                        );
                                    },
                                ))
                            })
                    }),
            )
            .into_any_element()
    }

    fn render_user_message_attachment_preview(
        &self,
        preview_image_path: Option<PathBuf>,
        kind: ParsedUserAttachmentKind,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        if matches!(
            kind,
            ParsedUserAttachmentKind::Skill | ParsedUserAttachmentKind::Mcp
        ) {
            return attachment_capability_icon(kind, cx).into_any_element();
        }

        if let Some(image_path) = preview_image_path {
            div()
                .size(px(22.))
                .flex_none()
                .relative()
                .overflow_hidden()
                .rounded_full()
                .bg(cx.theme().muted)
                .child(
                    img(image_path)
                        .absolute()
                        .top_0()
                        .left_0()
                        .right_0()
                        .bottom_0()
                        .w_full()
                        .h_full()
                        .rounded_full()
                        .object_fit(ObjectFit::Fill)
                        .with_fallback(move || attachment_file_icon(false).into_any_element()),
                )
                .into_any_element()
        } else {
            attachment_file_icon(true).into_any_element()
        }
    }
}

fn attachment_file_icon(flex_none: bool) -> gpui::Div {
    let mut container = div().size(px(22.)).flex().items_center().justify_center();
    if flex_none {
        container = container.flex_none();
    }

    container.child(
        Icon::new(gpui_component::IconName::File)
            .size_3()
            .opacity(0.8),
    )
}

fn attachment_capability_icon(
    kind: ParsedUserAttachmentKind,
    cx: &mut Context<PioneerDesktop>,
) -> gpui::Div {
    let icon = match kind {
        ParsedUserAttachmentKind::Skill => PioneerIconName::Zap,
        ParsedUserAttachmentKind::Mcp => PioneerIconName::Mcp,
        ParsedUserAttachmentKind::File => PioneerIconName::Paperclip,
    };

    div()
        .flex_none()
        .size(px(20.))
        .rounded_full()
        .flex()
        .items_center()
        .justify_center()
        .child(
            Icon::new(icon)
                .size_3()
                .opacity(0.8)
                .text_color(cx.theme().foreground),
        )
}

#[cfg(test)]
mod tests {
    #[test]
    fn collaboration_actions_are_not_hover_only() {
        let source = include_str!("user_message.rs");
        let hover_only_marker = format!(".group_{}(", "hover");
        assert!(!source.contains(&hover_only_marker));
        for action in ["reply", "copy", "edit", "delete"] {
            let localization_key = format!("timeline.message.{action}_action");
            assert!(
                source.contains(&localization_key),
                "missing context-menu action {action}"
            );
        }
    }

    use super::{ParsedUserAttachmentKind, parse_user_attachments};
    use pioneer_protocol::{
        SkillId, SkillPackId, TurnSkillCapabilitySummary, TurnSkillPackCapabilitySummary,
        TurnSkillPackPresentationSummary, UserMessageAttachment,
    };

    #[test]
    fn desktop_user_message_badges_use_snapshot_pack_labels() {
        let pack_id = SkillPackId::new("P".repeat(21)).expect("pack id");
        let parsed = parse_user_attachments(&[
            UserMessageAttachment::SkillPack {
                capability: TurnSkillPackCapabilitySummary {
                    pack_id: pack_id.clone(),
                    label: "Research Pack".to_owned(),
                },
            },
            UserMessageAttachment::Skill {
                capability: TurnSkillCapabilitySummary {
                    skill_id: SkillId::new("S".repeat(21)).expect("skill id"),
                    label: "Search".to_owned(),
                    owner: None,
                    slug: "search".to_owned(),
                    source_kind: "user".to_owned(),
                    pack: Some(TurnSkillPackPresentationSummary {
                        pack_id,
                        label: "Research Pack".to_owned(),
                    }),
                },
            },
            UserMessageAttachment::Skill {
                capability: TurnSkillCapabilitySummary {
                    skill_id: SkillId::new("D".repeat(21)).expect("skill id"),
                    label: "Docs".to_owned(),
                    owner: None,
                    slug: "docs".to_owned(),
                    source_kind: "user".to_owned(),
                    pack: None,
                },
            },
        ]);

        assert_eq!(
            parsed
                .iter()
                .map(|attachment| attachment.display_name.as_str())
                .collect::<Vec<_>>(),
            vec!["Research Pack", "Research Pack / Search", "Docs"]
        );
        assert!(
            parsed
                .iter()
                .all(|attachment| attachment.kind == ParsedUserAttachmentKind::Skill)
        );
    }

    #[test]
    fn authored_row_uses_shared_identity_and_safe_collaboration_fields() {
        let source = include_str!("user_message.rs");
        for required in [
            "user_message_alignment",
            "author.display_name",
            "author.nickname",
            "author.avatar_revision",
            "reply_state",
            "value.deleted",
            "value.edited",
            "open_message_revision_history",
            "user_message_mutation_availability",
            "start_composer_message_edit",
            "confirm_delete_message",
        ] {
            assert!(source.contains(required), "missing `{required}`");
        }
        assert!(!source.contains(&["conversation", "_message"].concat()));
    }
}
