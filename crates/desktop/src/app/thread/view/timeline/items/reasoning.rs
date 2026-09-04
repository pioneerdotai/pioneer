use super::super::TimelineRowTopSpacing;
use super::super::markdown::CodeHighlightPolicy;
use super::format_running_elapsed;
use crate::{
    app::{
        conversation::{ItemView, TimelineEntry, TimelineEntryStatus},
        root::PioneerDesktop,
    },
    assets::PioneerIconName,
};
use gpui_kit::component::{collapsible::Collapsible, h_flex, spinner::Spinner, *};
use gpui_kit::{prelude::*, *};
use pioneer_client::timeline::labels::reasoning_text;
use pioneer_protocol::TurnItem;
use std::hash::{Hash, Hasher};

impl PioneerDesktop {
    pub(super) fn render_item_reasoning(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let body = match item {
            TurnItem::Reasoning {
                summary, content, ..
            } => reasoning_text(summary, content, Self::timeline_entry_text(item_view)),
            _ => Self::timeline_entry_text(item_view).to_owned(),
        };

        let has_body = !body.trim().is_empty();

        let code_highlight_policy = CodeHighlightPolicy::for_timeline_status(item_view.status);
        let body_element = self.render_markdown_auto(
            item_view.id.as_str(),
            body.as_str(),
            item_view.partial_markdown.as_ref(),
            code_highlight_policy,
            cx,
        );

        let running_elapsed_label = format_running_elapsed(item_view);

        let open = self
            .thread_timeline_item_expanded
            .borrow()
            .contains(entry.id.as_str());

        let entry_id = entry.id.clone();

        let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
        entry.id.hash(&mut toggle_id_hasher);
        let toggle_id = toggle_id_hasher.finish();

        let content = if item_view.status == TimelineEntryStatus::Running {
            if has_body {
                Collapsible::new()
                    .gap_2()
                    .open(open)
                    .child(
                        div()
                            .id(("reasoning-toggle", toggle_id))
                            .w_full()
                            .flex()
                            .items_center()
                            .hover(|this| this.opacity(0.8))
                            .child(
                                h_flex()
                                    .w_full()
                                    .items_center()
                                    .justify_between()
                                    .text_sm()
                                    .font_semibold()
                                    .child(
                                        h_flex()
                                            .items_center()
                                            .gap_2()
                                            .child(Spinner::new().icon(IconName::Loader))
                                            .child(t!("timeline.reasoning.running").to_string()),
                                    )
                                    .child(
                                        h_flex()
                                            .items_center()
                                            .gap_2()
                                            .when_some(running_elapsed_label, |this, elapsed| {
                                                this.child(elapsed)
                                            })
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
                    .into_any_element()
            } else {
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .text_sm()
                    .font_semibold()
                    .child(
                        h_flex()
                            .items_center()
                            .gap_2()
                            .child(Spinner::new().icon(IconName::Loader))
                            .child(t!("timeline.reasoning.running").to_string()),
                    )
                    .child(
                        h_flex()
                            .items_center()
                            .gap_2()
                            .when_some(running_elapsed_label, |this, elapsed| this.child(elapsed)),
                    )
                    .into_any_element()
            }
        } else if has_body {
            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("reasoning-toggle", toggle_id))
                        .w_full()
                        .flex()
                        .items_center()
                        .opacity(0.6)
                        .hover(|this| this.opacity(0.8))
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
                                            Icon::new(PioneerIconName::Lightbulb)
                                                .size_4()
                                                .opacity(0.8),
                                        )
                                        .child(
                                            div()
                                                .min_w_0()
                                                .overflow_hidden()
                                                .text_ellipsis()
                                                .child(
                                                    t!("timeline.reasoning.completed").to_string(),
                                                ),
                                        ),
                                )
                                .child(
                                    h_flex().flex_none().items_center().gap_2().child(
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
                .into_any_element()
        } else {
            div()
                .w_full()
                .flex()
                .items_center()
                .opacity(0.6)
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
                                .child(Icon::new(PioneerIconName::Lightbulb).size_4().opacity(0.8))
                                .child(
                                    div()
                                        .min_w_0()
                                        .overflow_hidden()
                                        .text_ellipsis()
                                        .child(t!("timeline.reasoning.completed").to_string()),
                                ),
                        ),
                )
                .into_any_element()
        };

        self.render_item_row(top_spacing, is_last_row, content_width, content)
    }
}
