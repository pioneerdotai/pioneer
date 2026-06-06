use super::{format_elapsed, format_elapsed_ms, now_unix_ms};
use crate::app::{
    conversation::{ItemView, TimelineEntry, TimelineEntryStatus},
    root::PioneerDesktop,
};
use gpui::{prelude::*, *};
use gpui_component::{collapsible::Collapsible, h_flex, spinner::Spinner, v_flex, *};
use pioneer_client::timeline::labels::{
    TimelineFinalStatusKind, file_change_display_text, final_file_change_status,
};
use pioneer_protocol::TurnItem;
use std::hash::{Hash, Hasher};

fn changed_files_label(count: usize) -> String {
    match count {
        0 => t!("timeline.file_change.no_files").to_string(),
        1 => t!("timeline.file_change.one_file").to_string(),
        value => t!("timeline.file_change.files_count", count = value).to_string(),
    }
}

impl PioneerDesktop {
    pub(super) fn render_item_file_change(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let (tool_name, changed_files, exit_code, output, success) = match item {
            TurnItem::FileChange {
                tool_name,
                changed_files,
                exit_code,
                stdout,
                stderr,
                success,
                ..
            } => (
                tool_name.clone(),
                changed_files.clone(),
                *exit_code,
                file_change_display_text(stdout.as_deref(), stderr.as_deref(), None),
                *success,
            ),
            _ => (
                "apply_patch".to_owned(),
                Vec::new(),
                None,
                file_change_display_text(None, None, Some(Self::timeline_entry_text(item_view))),
                None,
            ),
        };

        let file_count = changed_files.len();
        let headline = if file_count == 0 {
            tool_name.clone()
        } else {
            changed_files_label(file_count)
        };

        let summary_row = || {
            h_flex()
                .flex_1()
                .min_w_0()
                .items_center()
                .gap_2()
                .child(Icon::new(IconName::File).size_4().opacity(0.8))
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .overflow_hidden()
                        .text_ellipsis()
                        .text_sm()
                        .line_height(relative(1.45))
                        .child(headline.clone()),
                )
                .into_any_element()
        };

        let elapsed_label = format_elapsed(item_view);
        let running_elapsed_label = item_view
            .started_at_unix_ms
            .map(|started| format_elapsed_ms(now_unix_ms().saturating_sub(started) as u64));

        let open = self
            .thread_timeline_item_expanded
            .borrow()
            .contains(entry.id.as_str());

        let entry_id = entry.id.clone();
        let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
        entry.id.hash(&mut toggle_id_hasher);
        let toggle_id = toggle_id_hasher.finish();

        let status = final_file_change_status(item_view.status, success, exit_code);
        let final_status = file_change_status_label(status.kind);
        let is_successful = status.successful;

        let content = if item_view.status == TimelineEntryStatus::Running {
            v_flex()
                .w_full()
                .gap_3()
                .child(summary_row())
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
                                .child(t!("timeline.file_change.running").to_string()),
                        )
                        .child(
                            h_flex()
                                .items_center()
                                .gap_2()
                                .when_some(running_elapsed_label, |this, elapsed| {
                                    this.child(elapsed)
                                }),
                        ),
                )
                .into_any_element()
        } else {
            let details = self.file_change_details(
                changed_files.as_slice(),
                exit_code,
                output.as_deref(),
                cx,
            );

            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("file-change-toggle", toggle_id))
                        .w_full()
                        .flex()
                        .items_center()
                        .opacity(0.7)
                        .hover(|this| this.opacity(0.9))
                        .child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_3()
                                .child(summary_row())
                                .child(
                                    h_flex()
                                        .flex_none()
                                        .max_w(px(300.0))
                                        .items_center()
                                        .gap_2()
                                        .text_sm()
                                        .child(
                                            Icon::new(if is_successful {
                                                IconName::Check
                                            } else {
                                                IconName::TriangleAlert
                                            })
                                            .size_3p5(),
                                        )
                                        .child(
                                            div()
                                                .min_w_0()
                                                .overflow_hidden()
                                                .text_ellipsis()
                                                .child(final_status),
                                        )
                                        .when_some(elapsed_label, |this, elapsed| {
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
                .content(details)
                .into_any_element()
        };

        self.render_item_row(is_first_row, is_last_row, content_width, content)
    }

    fn file_change_details(
        &self,
        changed_files: &[String],
        exit_code: Option<i32>,
        output: Option<&str>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let mut details = v_flex().w_full().gap_2().pt_1();
        let mut has_details = false;

        if let Some(exit_code) = exit_code {
            has_details = true;
            details = details.child(
                h_flex()
                    .items_center()
                    .gap_2()
                    .text_sm()
                    .opacity(0.8)
                    .child(t!("timeline.file_change.exit_code").to_string())
                    .child(exit_code.to_string()),
            );
        }

        if !changed_files.is_empty() {
            has_details = true;
            let mut list = v_flex()
                .w_full()
                .gap_1()
                .rounded_lg()
                .border_1()
                .border_color(cx.theme().border)
                .p_1();

            for (index, path) in changed_files.iter().take(40).enumerate() {
                list = list.child(
                    h_flex()
                        .id(("file-change-path", index))
                        .w_full()
                        .items_center()
                        .gap_2()
                        .px_2()
                        .py_1()
                        .rounded_md()
                        .child(Icon::new(IconName::File).size_3().opacity(0.65))
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .overflow_hidden()
                                .text_ellipsis()
                                .text_sm()
                                .line_height(relative(1.35))
                                .child(path.clone()),
                        ),
                );
            }

            if changed_files.len() > 40 {
                list = list.child(
                    div().px_2().py_1().text_sm().opacity(0.65).child(
                        t!(
                            "timeline.file_change.more_files",
                            count = changed_files.len() - 40
                        )
                        .to_string(),
                    ),
                );
            }

            details = details.child(list);
        }

        if let Some(output) = output.filter(|value| !value.trim().is_empty()) {
            has_details = true;
            details = details
                .child(self.file_change_output_block(Self::truncate_for_card(output, 4_000), cx));
        }

        if !has_details {
            details = details.child(
                div()
                    .text_sm()
                    .opacity(0.75)
                    .child(t!("timeline.common.no_details").to_string()),
            );
        }

        details.into_any_element()
    }

    fn file_change_output_block(&self, text: String, cx: &mut Context<Self>) -> AnyElement {
        div()
            .w_full()
            .overflow_hidden()
            .rounded_lg()
            .border_1()
            .border_color(cx.theme().border)
            .p_3()
            .child(
                v_flex()
                    .w_full()
                    .gap_2()
                    .child(
                        div()
                            .text_xs()
                            .opacity(0.65)
                            .child(t!("timeline.file_change.output").to_string()),
                    )
                    .child(
                        div()
                            .w_full()
                            .whitespace_normal()
                            .text_sm()
                            .line_height(relative(1.45))
                            .font_family("monospace")
                            .child(text),
                    ),
            )
            .into_any_element()
    }
}

fn file_change_status_label(kind: TimelineFinalStatusKind) -> String {
    match kind {
        TimelineFinalStatusKind::Cancelled => t!("timeline.file_change.cancelled").to_string(),
        TimelineFinalStatusKind::Failed => t!("timeline.file_change.failed").to_string(),
        TimelineFinalStatusKind::Running => t!("timeline.file_change.running").to_string(),
        TimelineFinalStatusKind::Completed => t!("timeline.file_change.completed").to_string(),
    }
}
