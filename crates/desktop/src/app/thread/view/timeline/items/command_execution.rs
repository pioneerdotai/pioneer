use super::super::TimelineRowTopSpacing;
use super::format_running_elapsed;
use crate::{
    app::{
        conversation::{ItemView, TimelineEntry, TimelineEntryStatus},
        root::{CachedTimelineTerminal, PioneerDesktop},
    },
    assets::PioneerIconName,
};
use gpui::{prelude::*, *};
use gpui_component::{collapsible::Collapsible, h_flex, spinner::Spinner, *};
use pioneer_client::timeline::labels::{
    command_execution_display_command, command_execution_terminal_text,
};
use pioneer_protocol::TurnItem;
use std::{
    hash::{Hash, Hasher},
    io::Cursor,
};
use terminal::{ColorPalette, TerminalConfig, TerminalView};

impl PioneerDesktop {
    fn estimate_terminal_cols(content_width: Pixels) -> usize {
        let horizontal_padding = px(16.0);
        let approx_cell_width = px(6.7);
        let available_width = (content_width - horizontal_padding).max(px(200.0));
        ((available_width / approx_cell_width) as usize).clamp(40, 260)
    }

    fn estimate_visual_lines(text: &str, cols: usize) -> usize {
        let cols = cols.max(1);
        let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
        normalized
            .lines()
            .map(|line| line.chars().count().max(1).div_ceil(cols))
            .sum::<usize>()
            .max(1)
    }

    fn command_execution_terminal_view(
        &self,
        entry: &TimelineEntry,
        terminal_text: &str,
        content_width: Pixels,
        terminal_height: Pixels,
        desired_rows: usize,
        cx: &mut Context<Self>,
    ) -> Entity<TerminalView> {
        let horizontal_padding = px(16.0);
        let vertical_padding = px(16.0);
        let available_width = (content_width - horizontal_padding).max(px(200.0));
        let available_height = (terminal_height - vertical_padding).max(px(80.0));
        let approx_cell_width = px(6.7);
        let approx_cell_height = px(13.0);
        let cols = ((available_width / approx_cell_width) as usize).clamp(40, 260);
        let fallback_rows = ((available_height / approx_cell_height) as usize).clamp(8, 80);
        let rows = desired_rows.max(fallback_rows).clamp(8, 1600);

        let mut content_hasher = std::collections::hash_map::DefaultHasher::new();
        terminal_text.hash(&mut content_hasher);
        cols.hash(&mut content_hasher);
        rows.hash(&mut content_hasher);
        let content_hash = content_hasher.finish();

        if let Some(cached) = self
            .thread_timeline_terminal_item
            .borrow()
            .get(entry.id.as_str())
            && cached.content_hash == content_hash
        {
            return cached.view.clone();
        }

        let config = TerminalConfig {
            font_family: "Menlo".to_owned(),
            font_size: px(11.0),
            cols,
            rows,
            line_height_multiplier: 1.0,
            scrollback: 1000,
            padding: gpui::Edges::all(px(16.0)),
            colors: ColorPalette::builder().background(0x1e, 0x1e, 0x1e).build(),
            ..TerminalConfig::default()
        };
        let terminal_bytes = terminal_text.as_bytes().to_vec();
        let terminal = cx
            .new(|cx| TerminalView::new(std::io::sink(), Cursor::new(terminal_bytes), config, cx));

        self.thread_timeline_terminal_item.borrow_mut().insert(
            entry.id.clone(),
            CachedTimelineTerminal {
                content_hash,
                view: terminal.clone(),
            },
        );

        terminal
    }

    pub(super) fn render_item_command_execution(
        &self,
        entry: &TimelineEntry,
        item_view: &ItemView,
        item: &TurnItem,
        top_spacing: TimelineRowTopSpacing,
        is_last_row: bool,
        content_width: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let command_label = match item {
            TurnItem::CommandExecution {
                arguments,
                command,
                tool_name,
                ..
            } => command_execution_display_command(command, arguments)
                .unwrap_or_else(|| tool_name.clone()),
            _ => t!("timeline.command.running").to_string(),
        };

        let terminal_text =
            command_execution_terminal_text(item, Self::timeline_entry_text(item_view), |output| {
                Self::truncate_for_card(output, 24_000)
            });

        let cols = Self::estimate_terminal_cols(content_width);
        let line_count = Self::estimate_visual_lines(terminal_text.as_str(), cols);
        let desired_rows = line_count.saturating_add(2).clamp(8, 1600);
        let terminal_height = px(((desired_rows.saturating_mul(13)).saturating_add(24)) as f32)
            .max(px(140.0))
            .min(px(360.0));

        let terminal = self.command_execution_terminal_view(
            entry,
            terminal_text.as_str(),
            content_width,
            terminal_height,
            desired_rows,
            cx,
        );

        let terminal_block = div()
            .w_full()
            .h(terminal_height)
            .child(terminal)
            .into_any_element();

        let running_elapsed_label = format_running_elapsed(item_view);

        let open = self
            .thread_timeline_item_expanded
            .borrow()
            .contains(entry.id.as_str());

        let entry_id = entry.id.clone();
        let mut toggle_id_hasher = std::collections::hash_map::DefaultHasher::new();
        entry.id.hash(&mut toggle_id_hasher);
        let toggle_id = toggle_id_hasher.finish();
        let is_running = item_view.status == TimelineEntryStatus::Running;
        let command_row = || {
            h_flex()
                .flex_1()
                .min_w_0()
                .items_center()
                .gap_2()
                .when(is_running, |this| {
                    this.child(Spinner::new().icon(IconName::Loader))
                })
                .when(!is_running, |this| {
                    this.child(Icon::new(PioneerIconName::Terminal).size_4().opacity(0.8))
                })
                .child(
                    div()
                        .flex_1()
                        .min_w_0()
                        .overflow_hidden()
                        .text_ellipsis()
                        .line_height(relative(1.45))
                        .child(command_label.clone()),
                )
                .into_any_element()
        };

        let content = if is_running {
            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("command-execution-toggle", toggle_id))
                        .w_full()
                        .flex()
                        .items_center()
                        .hover(|this| this.opacity(0.8))
                        .child(
                            h_flex()
                                .w_full()
                                .items_center()
                                .justify_between()
                                .gap_3()
                                .text_sm()
                                .font_semibold()
                                .child(command_row())
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
                .content(terminal_block)
                .into_any_element()
        } else {
            Collapsible::new()
                .gap_2()
                .open(open)
                .child(
                    div()
                        .id(("command-execution-toggle", toggle_id))
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
                                .child(command_row())
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
                .content(terminal_block)
                .into_any_element()
        };

        self.render_item_row(top_spacing, is_last_row, content_width, content)
    }
}
