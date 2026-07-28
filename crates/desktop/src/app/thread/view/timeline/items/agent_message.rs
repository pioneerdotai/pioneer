use super::super::markdown::CodeHighlightPolicy;
use crate::{
    app::{
        conversation::{ItemView, TimelineEntry},
        root::PioneerDesktop,
    },
    assets::PioneerIconName,
};
use chrono::{Local, TimeZone};
use gpui::{prelude::*, *};
use gpui_component::{
    Icon, IconName, clipboard::Clipboard, collapsible::Collapsible, h_flex, v_flex,
};
use pioneer_client::timeline::labels::is_task_timeline_agent_message;
use pioneer_protocol::{AgentMessagePhase, TurnItem};
use std::hash::{Hash, Hasher};

impl PioneerDesktop {
    pub(super) fn render_item_agent_message(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (text, markdown) = match item {
            TurnItem::AgentMessage { markdown, .. } => (
                Self::timeline_entry_text(item_view),
                item_view
                    .final_markdown
                    .as_ref()
                    .or(item_view.partial_markdown.as_ref())
                    .or(markdown.as_ref()),
            ),
            _ => (
                Self::timeline_entry_text(item_view),
                item_view.partial_markdown.as_ref(),
            ),
        };

        let timestamp_text = item_view
            .started_at_unix_ms
            .or(item_view.updated_at_unix_ms)
            .or(item_view.completed_at_unix_ms)
            .and_then(|ts| Local.timestamp_millis_opt(ts).single())
            .map(|dt| dt.format("%d.%m.%Y %H:%M").to_string())
            .unwrap_or_default();

        let copy_text = text.to_owned();
        let code_highlight_policy = CodeHighlightPolicy::for_timeline_status(item_view.status);

        let is_commentary = matches!(
            item,
            TurnItem::AgentMessage {
                phase: AgentMessagePhase::Commentary,
                ..
            }
        );

        if is_task_timeline_agent_message(item_view) {
            let body_element =
                if let Some(document) = markdown.or(item_view.partial_markdown.as_ref()) {
                    self.render_markdown_document(document, code_highlight_policy, cx)
                } else {
                    self.render_markdown_auto(text, None, CodeHighlightPolicy::Disabled, cx)
                };
            let open = self
                .thread_timeline_item_expanded
                .borrow()
                .contains(entry.id.as_str());
            let entry_id = entry.id.clone();

            let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
            entry.id.hash(&mut toggle_id_hasher);
            let toggle_id = toggle_id_hasher.finish();

            let content = Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("subagent-answer-toggle", toggle_id))
                        .w_full()
                        .flex()
                        .items_center()
                        .opacity(0.68)
                        .hover(|this| this.opacity(0.88))
                        .child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_3()
                                .text_sm()
                                .child(
                                    h_flex()
                                        .flex_1()
                                        .min_w_0()
                                        .items_center()
                                        .gap_2()
                                        .child(
                                            Icon::new(PioneerIconName::MessageCircle)
                                                .size_4()
                                                .opacity(0.8),
                                        )
                                        .child(
                                            div()
                                                .min_w_0()
                                                .overflow_hidden()
                                                .text_ellipsis()
                                                .child(
                                                    t!("timeline.agent_message.subagent_completed")
                                                        .to_string(),
                                                ),
                                        ),
                                )
                                .child(
                                    h_flex()
                                        .flex_none()
                                        .items_center()
                                        .gap_2()
                                        .child(
                                            Clipboard::new((
                                                "copy-subagent-message",
                                                entry.item_index,
                                            ))
                                            .value(copy_text.clone()),
                                        )
                                        .child(
                                            Icon::new(if open {
                                                IconName::ChevronUp
                                            } else {
                                                IconName::ChevronDown
                                            })
                                            .size_4(),
                                        ),
                                ),
                        )
                        .on_click({
                            let entry_id = entry_id.clone();
                            cx.listener(move |this, _, _, cx| {
                                this.toggle_timeline_item_expanded(entry_id.as_str(), cx);
                            })
                        }),
                )
                .content(body_element)
                .into_any_element();

            return self.render_item_row(is_first_row, is_last_row, content_width, content);
        }

        let mut row = div().flex().w_full().justify_center();

        if is_first_row {
            row = row.pt(px(40.));
        } else {
            row = row.pt(px(10.));
        }

        if is_last_row {
            row = row.pb(px(10.));
        }

        row.child(
            v_flex()
                .w(content_width)
                .px_6()
                .group(format!("agent-message-{}", item_view.id))
                .child(div().w_full().overflow_hidden().child(
                    if let Some(document) = markdown.or(item_view.partial_markdown.as_ref()) {
                        self.render_markdown_document(document, code_highlight_policy, cx)
                    } else {
                        self.render_markdown_auto(text, None, CodeHighlightPolicy::Disabled, cx)
                    },
                ))
                .when(!is_commentary, |this| {
                    this.child(
                        h_flex()
                            .h(px(30.))
                            .justify_start()
                            .items_center()
                            .gap_2()
                            .text_xs()
                            .opacity(0.0)
                            .group_hover(format!("agent-message-{}", item_view.id), |this| {
                                this.opacity(0.6)
                            })
                            .child(timestamp_text)
                            .child(
                                Clipboard::new(("copy-agent-message", entry.item_index))
                                    .value(copy_text),
                            ),
                    )
                }),
        )
        .into_any_element()
    }
}
