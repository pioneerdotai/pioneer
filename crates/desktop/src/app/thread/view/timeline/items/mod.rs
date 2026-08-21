mod agent_message;
mod command_execution;
mod download;
mod dynamic_tool_call;
mod file_change;
mod reasoning;
mod system_event;
mod user_message;
mod web_fetch;
mod web_search;

use super::{
    TimelineRowTopSpacing,
    layout::{
        TIMELINE_CONTENT_HORIZONTAL_PADDING, TIMELINE_END_BOTTOM_SPACING,
        TIMELINE_ITEM_BOTTOM_SPACING,
    },
};
use crate::app::{
    conversation::{ItemView, TimelineEntry, TimelineEntryStatus},
    root::PioneerDesktop,
};
use crate::assets::PioneerIconName;
use gpui::{prelude::*, *};
use gpui_component::{
    Icon, IconName, StyledExt, h_flex, separator::Separator, theme::ActiveTheme, v_flex,
};
use pioneer_client::{
    security::{
        ClientSecurityEnforcementStatus, ClientSecurityFilesystemAccess, ClientTurnSecuritySummary,
        security_diagnostic_rows, security_summary_label,
    },
    timeline::labels as timeline_labels,
};
use pioneer_protocol::{TaskAttachmentMode, TaskStatus, TurnItem};
use std::hash::{Hash, Hasher};

pub(super) fn format_elapsed_ms(elapsed_ms: u64) -> String {
    let total_seconds = elapsed_ms / 1_000;
    let hours = total_seconds / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;

    if hours > 0 {
        t!(
            "timeline.elapsed.hours_minutes",
            hours = hours,
            minutes = format!("{minutes:02}")
        )
        .to_string()
    } else if minutes > 0 {
        t!(
            "timeline.elapsed.minutes_seconds",
            minutes = minutes,
            seconds = format!("{seconds:02}")
        )
        .to_string()
    } else {
        t!("timeline.elapsed.seconds", seconds = seconds).to_string()
    }
}

pub(super) fn now_unix_ms() -> i64 {
    timeline_labels::now_unix_ms()
}

pub(super) fn format_running_elapsed(item_view: &ItemView) -> Option<String> {
    running_elapsed_ms(
        item_view.status,
        item_view.started_at_unix_ms,
        now_unix_ms(),
    )
    .map(format_elapsed_ms)
}

fn running_elapsed_ms(
    status: TimelineEntryStatus,
    started_at_unix_ms: Option<i64>,
    now_unix_ms: i64,
) -> Option<u64> {
    if status != TimelineEntryStatus::Running {
        return None;
    }
    let started_at_unix_ms = started_at_unix_ms?;
    Some(now_unix_ms.saturating_sub(started_at_unix_ms).max(0) as u64)
}

pub(super) fn host_from_url(url: &str) -> Option<String> {
    timeline_labels::host_from_url(url)
}

fn task_status_label(status: TaskStatus) -> String {
    match status {
        TaskStatus::Draft => t!("timeline.task.status.draft").to_string(),
        TaskStatus::Scheduled => t!("timeline.task.status.scheduled").to_string(),
        TaskStatus::Queued => t!("timeline.task.status.queued").to_string(),
        TaskStatus::Running => t!("timeline.task.status.running").to_string(),
        TaskStatus::Waiting => t!("timeline.task.status.waiting").to_string(),
        TaskStatus::WaitingReview => t!("timeline.task.status.waiting_review").to_string(),
        TaskStatus::Completed => t!("timeline.task.status.completed").to_string(),
        TaskStatus::Blocked => t!("timeline.task.status.blocked").to_string(),
        TaskStatus::Failed => t!("timeline.task.status.failed").to_string(),
        TaskStatus::Cancelled => t!("timeline.task.status.cancelled").to_string(),
    }
}

impl PioneerDesktop {
    pub(super) fn turn_security_icon(summary: &ClientTurnSecuritySummary) -> PioneerIconName {
        match summary.enforcement {
            ClientSecurityEnforcementStatus::Unavailable => PioneerIconName::ShieldX,
            ClientSecurityEnforcementStatus::Degraded => PioneerIconName::ShieldAlert,
            ClientSecurityEnforcementStatus::Active => match summary.filesystem_access {
                ClientSecurityFilesystemAccess::Unrestricted => PioneerIconName::ShieldCheck,
                ClientSecurityFilesystemAccess::ReadOnly => PioneerIconName::ShieldX,
                ClientSecurityFilesystemAccess::WorkspaceWrite => PioneerIconName::ShieldAlert,
            },
        }
    }

    pub(super) fn render_turn_security_badge(
        &self,
        summary: &ClientTurnSecuritySummary,
        _: &mut Context<Self>,
    ) -> AnyElement {
        h_flex()
            .items_center()
            .gap_1()
            .text_xs()
            .opacity(0.72)
            .child(Icon::new(Self::turn_security_icon(summary)).size_3())
            .child(
                div()
                    .min_w_0()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(security_summary_label(summary)),
            )
            .into_any_element()
    }

    pub(super) fn render_turn_security_summary(
        &self,
        summary: &ClientTurnSecuritySummary,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let diagnostic_rows = security_diagnostic_rows(summary);

        v_flex()
            .gap_1()
            .child(self.render_turn_security_badge(summary, cx))
            .children(diagnostic_rows.into_iter().take(2).map(|row| {
                h_flex()
                    .items_start()
                    .gap_1()
                    .text_xs()
                    .opacity(0.62)
                    .child(div().font_medium().child(row.label))
                    .child(div().child(row.message))
                    .into_any_element()
            }))
            .into_any_element()
    }

    pub(super) fn timeline_favicon_url(
        &self,
        primary: Option<String>,
        page_url: &str,
    ) -> Option<String> {
        timeline_labels::timeline_favicon_url(primary, page_url)
    }

    pub(super) fn timeline_favicon_icon(
        &self,
        favicon_url: Option<String>,
        size: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        favicon_url
            .map(|url| {
                div()
                    .size(size)
                    .overflow_hidden()
                    .rounded_sm()
                    .bg(cx.theme().muted.opacity(0.5))
                    .child(img(url).w_full().h_full().with_fallback(move || {
                        Icon::new(IconName::Globe)
                            .size(size)
                            .opacity(0.75)
                            .into_any_element()
                    }))
                    .into_any_element()
            })
            .unwrap_or_else(|| {
                Icon::new(IconName::Globe)
                    .size(size)
                    .opacity(0.75)
                    .into_any_element()
            })
    }

    pub(super) fn timeline_host_with_favicon(
        &self,
        host_label: &str,
        favicon_url: Option<String>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        h_flex()
            .w_full()
            .items_center()
            .gap_2()
            .child(self.timeline_favicon_icon(favicon_url, px(16.0), cx))
            .child(
                div()
                    .text_sm()
                    .opacity(0.9)
                    .line_height(relative(1.45))
                    .child(Self::truncate_for_card(host_label, 160)),
            )
            .into_any_element()
    }

    pub(super) fn toggle_timeline_item_expanded(&mut self, entry_id: &str, cx: &mut Context<Self>) {
        let mut expanded = self.thread_timeline_item_expanded.borrow_mut();
        if !expanded.remove(entry_id) {
            expanded.insert(entry_id.to_owned());
        }
        drop(expanded);

        let mut state = self.thread_timeline_view_state.borrow_mut();
        state.expanded_revision = state.expanded_revision.saturating_add(1);
        state.entry_layout_cache.remove(entry_id);
        state.cached_item_sizes = None;
        cx.notify();
    }

    pub(super) fn render_turn_item_entry(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match item {
            TurnItem::UserMessage { .. } => self.render_item_user_message(
                entry,
                item_view,
                item,
                None,
                None,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::AgentMessage { .. } => self.render_item_agent_message(
                entry,
                item_view,
                item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::Reasoning { .. } => self.render_item_reasoning(
                entry,
                item_view,
                item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::SystemEvent { .. } => self.render_item_system_event(
                entry,
                item_view,
                item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::Task { item: task_item } => self.render_item_task(
                item_view,
                task_item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::CommandExecution { .. } => self.render_item_command_execution(
                entry,
                item_view,
                item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::FileChange { .. } => self.render_item_file_change(
                entry,
                item_view,
                item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::WebSearch { .. } => self.render_item_web_search(
                entry,
                item_view,
                item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::WebFetch { .. } => self.render_item_web_fetch(
                entry,
                item_view,
                item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::Download { .. } => self.render_item_download(
                entry,
                item_view,
                item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::DynamicToolCall { .. } => self.render_item_dynamic_tool_call(
                entry,
                item_view,
                item,
                top_spacing,
                is_last_row,
                content_width,
                cx,
            ),
        }
    }

    fn render_item_task(
        &self,
        item_view: &ItemView,
        task_item: &pioneer_protocol::TaskTurnItem,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let status = task_status_label(task_item.status);
        let is_running = task_item.status == TaskStatus::Running;
        let uses_card_shell =
            task_uses_stable_card_shell(task_item.attachment, task_item.child_thread_id.as_deref());
        let detail = task_item
            .error_preview
            .as_ref()
            .or(task_item.progress_preview.as_ref())
            .cloned();
        let running_activity = is_running.then(|| {
            self.render_running_activity_content(
                format!("task:{}", task_item.id),
                task_item
                    .started_at
                    .map(|started_at| started_at.saturating_mul(1_000))
                    .or(item_view.started_at_unix_ms)
                    .or(Some(task_item.created_at.saturating_mul(1_000))),
                Some(pioneer_protocol::TurnWorkState::Running),
                None,
                true,
                cx,
            )
        });
        let child_thread_id = task_item.child_thread_id.clone();
        let title = task_item.title.clone();
        let mut row_id_hasher = std::collections::hash_map::DefaultHasher::new();
        task_item.id.hash(&mut row_id_hasher);
        let row_id = row_id_hasher.finish();

        let content =
            h_flex().w_full().child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .gap_2()
                    .child(
                        h_flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .text_sm()
                                    .overflow_hidden()
                                    .text_ellipsis()
                                    .child(task_item.title.clone()),
                            )
                            .when(uses_card_shell, |this| {
                                this.child(Icon::new(IconName::ChevronRight).size_4().opacity(0.6))
                            }),
                    )
                    .child(Separator::horizontal())
                    .child(
                        div()
                            .when(!is_running, |this| {
                                this.child(
                                    v_flex()
                                        .py_2()
                                        .child(div().text_sm().font_semibold().child(status))
                                        .when_some(detail, |this, detail| {
                                            this.child(div().text_xs().opacity(0.6).child(
                                                Self::truncate_for_card(detail.as_str(), 160),
                                            ))
                                        }),
                                )
                            })
                            .when_some(running_activity, |this, running_activity| {
                                this.child(div().py_2().child(running_activity))
                            }),
                    ),
            );

        let content = if uses_card_shell {
            let card = content
                .id(("task-anchor-card", row_id))
                .rounded_2xl()
                .px_4()
                .py_2p5()
                .border_1()
                .border_color(cx.theme().border);
            if let Some(child_thread_id) = child_thread_id {
                card.hover(|this| this.bg(cx.theme().muted.opacity(0.6)))
                    .on_click(cx.listener(move |view, _, window, cx| {
                        view.open_task_child_thread(
                            child_thread_id.clone(),
                            title.clone(),
                            window,
                            cx,
                        );
                    }))
                    .into_any_element()
            } else {
                card.into_any_element()
            }
        } else {
            content.into_any_element()
        };

        self.render_item_row(top_spacing, is_last_row, content_width, content)
    }

    pub(super) fn render_item_row(
        &self,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        content: AnyElement,
    ) -> AnyElement {
        let mut row = div().flex().w_full().justify_center();

        row = row.pt(top_spacing.pixels());

        if is_last_row {
            row = row.pb(TIMELINE_END_BOTTOM_SPACING);
        } else {
            row = row.pb(TIMELINE_ITEM_BOTTOM_SPACING);
        }

        row.child(
            v_flex()
                .w(content_width)
                .px(TIMELINE_CONTENT_HORIZONTAL_PADDING)
                .child(content),
        )
        .into_any_element()
    }

    pub(super) fn truncate_for_card(text: &str, max_chars: usize) -> String {
        let mut chars = text.char_indices();

        for _ in 0..max_chars {
            if chars.next().is_none() {
                return text.to_owned();
            }
        }

        let Some((boundary, _)) = chars.next() else {
            return text.to_owned();
        };

        let mut result = String::with_capacity(boundary.saturating_add(16));
        result.push_str(&text[..boundary]);
        result.push('\n');
        result.push_str(&t!("timeline.common.truncated").to_string());
        result
    }
}

fn task_uses_stable_card_shell(
    attachment: TaskAttachmentMode,
    child_thread_id: Option<&str>,
) -> bool {
    attachment == TaskAttachmentMode::Detached || child_thread_id.is_some()
}

#[cfg(test)]
mod tests {
    use super::{running_elapsed_ms, task_uses_stable_card_shell};
    use crate::app::conversation::TimelineEntryStatus;
    use pioneer_protocol::TaskAttachmentMode;

    #[test]
    fn detached_task_card_shell_does_not_depend_on_child_binding() {
        assert!(task_uses_stable_card_shell(
            TaskAttachmentMode::Detached,
            None
        ));
        assert!(task_uses_stable_card_shell(
            TaskAttachmentMode::Detached,
            Some("child")
        ));
        assert!(!task_uses_stable_card_shell(
            TaskAttachmentMode::Attached,
            None
        ));
    }

    #[test]
    fn work_item_elapsed_is_visible_only_while_running() {
        assert_eq!(
            running_elapsed_ms(TimelineEntryStatus::Running, Some(1_000), 3_500),
            Some(2_500)
        );

        for status in [
            TimelineEntryStatus::Completed,
            TimelineEntryStatus::Blocked,
            TimelineEntryStatus::Failed,
            TimelineEntryStatus::Cancelled,
        ] {
            assert_eq!(running_elapsed_ms(status, Some(1_000), 3_500), None);
        }
    }
}
