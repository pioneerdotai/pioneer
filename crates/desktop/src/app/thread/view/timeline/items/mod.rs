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

use crate::app::{
    conversation::{ItemView, TimelineEntry},
    root::PioneerDesktop,
};
use crate::assets::PioneerIconName;
use gpui::{prelude::*, *};
use gpui_component::{Icon, IconName, h_flex, theme::ActiveTheme, v_flex};
use pioneer_client::timeline::labels as timeline_labels;
use pioneer_protocol::{TaskStatus, TurnItem, TurnPermissionMode, TurnPermissionProfileSnapshot};

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

pub(super) fn format_elapsed(item_view: &ItemView) -> Option<String> {
    let started = item_view.started_at_unix_ms?;
    let ended = item_view
        .completed_at_unix_ms
        .or(item_view.updated_at_unix_ms)
        .unwrap_or(started);

    Some(format_elapsed_ms(ended.saturating_sub(started) as u64))
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
    pub(super) fn turn_permission_icon(mode: TurnPermissionMode) -> PioneerIconName {
        match mode {
            TurnPermissionMode::Supervised => PioneerIconName::Lock,
            TurnPermissionMode::AutoAcceptEdits => PioneerIconName::Pencil,
            TurnPermissionMode::FullAccess => PioneerIconName::Unlock,
        }
    }

    pub(super) fn render_turn_permission_badge(
        &self,
        permission_profile: &TurnPermissionProfileSnapshot,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let display = timeline_labels::turn_permission_profile_display(permission_profile);
        let mode = display.mode;

        h_flex()
            .h(px(22.))
            .max_w(px(180.))
            .items_center()
            .gap_1()
            .rounded_full()
            .bg(cx.theme().muted.opacity(0.72))
            .px_2()
            .text_xs()
            .opacity(0.72)
            .child(Icon::new(Self::turn_permission_icon(mode)).size_3())
            .child(
                div()
                    .min_w_0()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(display.label),
            )
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
        permission_profile: Option<&TurnPermissionProfileSnapshot>,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match item {
            TurnItem::UserMessage { .. } => self.render_item_user_message(
                entry,
                item_view,
                item,
                permission_profile,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::AgentMessage { .. } => self.render_item_agent_message(
                entry,
                item_view,
                item,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::Reasoning { .. } => self.render_item_reasoning(
                entry,
                item_view,
                item,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::SystemEvent { .. } => self.render_item_system_event(
                entry,
                item_view,
                item,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::Task { item: task_item } => {
                self.render_item_task(task_item, is_first_row, is_last_row, content_width)
            }
            TurnItem::CommandExecution { .. } => self.render_item_command_execution(
                entry,
                item_view,
                item,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::FileChange { .. } => self.render_item_file_change(
                entry,
                item_view,
                item,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::WebSearch { .. } => self.render_item_web_search(
                entry,
                item_view,
                item,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::WebFetch { .. } => self.render_item_web_fetch(
                entry,
                item_view,
                item,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::Download { .. } => self.render_item_download(
                entry,
                item_view,
                item,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
            TurnItem::DynamicToolCall { .. } => self.render_item_dynamic_tool_call(
                entry,
                item_view,
                item,
                is_first_row,
                is_last_row,
                content_width,
                cx,
            ),
        }
    }

    fn render_item_task(
        &self,
        task_item: &pioneer_protocol::TaskTurnItem,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
    ) -> AnyElement {
        let status = task_status_label(task_item.status);
        let content = h_flex()
            .w_full()
            .items_center()
            .gap_2()
            .child(Icon::new(IconName::Info).size_4().opacity(0.8))
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .child(
                        div()
                            .text_sm()
                            .line_height(relative(1.45))
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(task_item.title.clone()),
                    )
                    .child(div().text_xs().opacity(0.7).child(status)),
            )
            .into_any_element();

        self.render_item_row(is_first_row, is_last_row, content_width, content)
    }

    pub(super) fn render_item_row(
        &self,
        is_first_row: bool,
        is_last_row: bool,
        content_width: Pixels,
        content: AnyElement,
    ) -> AnyElement {
        let mut row = div().flex().w_full().justify_center();

        if is_first_row {
            row = row.pt(px(40.));
        } else {
            row = row.pt(px(10.));
        }

        if is_last_row {
            row = row.pb(px(40.));
        } else {
            row = row.pb(px(10.));
        }

        row.child(v_flex().w(content_width).px_6().child(content))
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
