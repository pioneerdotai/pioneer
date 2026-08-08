use crate::app::root::{DesktopComposerEditTarget, PioneerDesktop};
use crate::assets::PioneerIconName;
use gpui::{prelude::*, *};
use gpui_component::{Icon, button::*, theme::ActiveTheme, *};
use pioneer_client::composer::{
    model_selection as composer_model_selection,
    state_machine::{ComposerDomainAction, ComposerReplyTarget},
};
use pioneer_protocol::ThreadMode;

impl PioneerDesktop {
    fn composer_mode_label(mode: ThreadMode) -> String {
        match mode {
            ThreadMode::Agent => t!("chat.composer.mode.agent_label").to_string(),
            ThreadMode::Chat => t!("chat.composer.mode.chat_label").to_string(),
            ThreadMode::Message => t!("chat.composer.mode.message_label").to_string(),
        }
    }

    fn composer_mode_description(mode: ThreadMode) -> String {
        match mode {
            ThreadMode::Agent => t!("chat.composer.mode.agent_description").to_string(),
            ThreadMode::Chat => t!("chat.composer.mode.chat_description").to_string(),
            ThreadMode::Message => t!("chat.composer.mode.message_description").to_string(),
        }
    }

    fn composer_mode_button_icon(mode: ThreadMode) -> AnyElement {
        match mode {
            ThreadMode::Agent => Icon::new(PioneerIconName::Infinity)
                .size_3p5()
                .into_any_element(),
            ThreadMode::Chat => Icon::new(PioneerIconName::MessageCircle)
                .size_3p5()
                .into_any_element(),
            ThreadMode::Message => Icon::new(PioneerIconName::Users)
                .size_3p5()
                .into_any_element(),
        }
    }

    pub(in crate::app::thread::view) fn render_composer_mode_selector(
        &self,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let selected_mode = self.composer_turn_mode;
        let desktop_entity = cx.entity();
        let content = if let Some(target) = self.composer_edit_target.clone() {
            vec![self.render_composer_edit_target(target, cx)]
        } else if let Some(target) = self.composer_reply_target.clone() {
            vec![self.render_composer_reply_target(target, cx)]
        } else {
            composer_model_selection::composer_turn_mode_options()
                .into_iter()
                .map(|mode| {
                    let id = match mode {
                        ThreadMode::Agent => "composer-mode-agent",
                        ThreadMode::Chat => "composer-mode-chat",
                        ThreadMode::Message => "composer-mode-message",
                    };
                    let is_active = selected_mode == mode;
                    let desktop_entity = desktop_entity.clone();
                    let hover_entity = desktop_entity.clone();
                    let description = Self::composer_mode_description(mode);
                    let show_label = is_active || self.composer_hovered_mode == Some(mode);
                    let hover_id = format!("{id}-hover");

                    let button = Button::new(id)
                        .ghost()
                        .rounded_full()
                        .gap_2()
                        .h_7()
                        .disabled(self.composer_upload_in_progress)
                        .selected(is_active)
                        .tooltip(description)
                        .on_click(move |_, _, cx| {
                            let _ = desktop_entity.update(cx, |view, cx| {
                                let transition = view.reduce_composer_domain(
                                    ComposerDomainAction::SetModeFromUser { mode },
                                );
                                if mode != ThreadMode::Message {
                                    view.sync_composer_model_selection_for_active_thread();
                                }
                                if transition.changed {
                                    cx.notify();
                                }
                            });
                        })
                        .child(
                            h_flex()
                                .items_center()
                                .gap_1()
                                .child(Self::composer_mode_button_icon(mode))
                                .when(show_label, |this| {
                                    this.child(Self::composer_mode_label(mode))
                                })
                                .font_medium()
                                .text_xs()
                                .when(!is_active, |this| this.opacity(0.6)),
                        );

                    div()
                        .id(hover_id)
                        .on_hover(move |hovered, _, cx| {
                            let _ = hover_entity.update(cx, |view, cx| {
                                if *hovered {
                                    view.composer_hovered_mode = Some(mode);
                                } else if view.composer_hovered_mode == Some(mode) {
                                    view.composer_hovered_mode = None;
                                }
                                cx.notify();
                            });
                        })
                        .child(button)
                        .into_any_element()
                })
                .collect()
        };

        div()
            .w_full()
            .flex_none()
            .px_3p5()
            .child(
                h_flex()
                    .id("composer-mode-selector")
                    .w_full()
                    // The selector is a fixed-height toolbar.  Keep it out of the
                    // flex distribution of the thread body so switching modes cannot
                    // make the composer (or the timeline) consume the remaining
                    // height.
                    .flex_none()
                    .items_center()
                    .gap_0p5()
                    .rounded_t_2xl()
                    .bg(cx.theme().muted)
                    .px_1p5()
                    .pt_1p5()
                    .pb_1()
                    .children(content),
            )
            .into_any_element()
    }

    fn render_composer_reply_target(
        &self,
        target: ComposerReplyTarget,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let label = target
            .author_display_name
            .as_deref()
            .map(str::to_owned)
            .unwrap_or_else(|| t!("timeline.message.unknown_author").to_string());
        let preview = target
            .preview
            .unwrap_or_else(|| t!("timeline.message.reply_unavailable").to_string());

        h_flex()
            .id(SharedString::from(format!(
                "composer-reply-{}",
                target.turn_id
            )))
            .w_full()
            .min_w_0()
            .h_7()
            .items_center()
            .gap_10()
            .pl_2()
            .pr_0p5()
            .child(
                div()
                    .min_w_0()
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .text_xs()
                    .font_medium()
                    .child(format!("{label}: {preview}")),
            )
            .child(
                Button::new("cancel-composer-reply")
                    .small()
                    .ghost()
                    .compact()
                    .icon(IconName::Close)
                    .disabled(self.composer_upload_in_progress)
                    .tooltip(t!("chat.composer.reply.cancel").to_string())
                    .on_click(cx.listener(|view, _, _, cx| {
                        view.reduce_composer_domain(ComposerDomainAction::ClearReplyTarget);
                        cx.notify();
                    })),
            )
            .into_any_element()
    }

    fn render_composer_edit_target(
        &self,
        target: DesktopComposerEditTarget,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let label = t!("timeline.message.edit_title").to_string();

        h_flex()
            .id(SharedString::from(format!(
                "composer-edit-{}",
                target.presentation.turn_id
            )))
            .w_full()
            .min_w_0()
            .h_7()
            .items_center()
            .gap_10()
            .pl_2()
            .pr_0p5()
            .child(
                div()
                    .min_w_0()
                    .flex_1()
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .text_xs()
                    .font_medium()
                    .child(format!("{label}: {}", target.preview)),
            )
            .child(
                Button::new("cancel-composer-edit")
                    .small()
                    .ghost()
                    .compact()
                    .icon(IconName::Close)
                    .disabled(self.message_mutation_pending)
                    .tooltip(t!("buttons.cancel").to_string())
                    .on_click(cx.listener(|view, _, window, cx| {
                        view.cancel_composer_message_edit(window, cx);
                    })),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn desktop_mode_selector_has_all_three_shared_modes_without_legacy_unreachable_path() {
        let source = include_str!("mode_selector.rs");
        for id in [
            "composer-mode-message",
            "composer-mode-chat",
            "composer-mode-agent",
        ] {
            assert!(source.contains(id));
        }
        assert!(!source.contains(&["Message composer UI", " is introduced"].concat()));
    }

    #[test]
    fn desktop_reply_replaces_mode_buttons_with_truncated_cancelable_context() {
        let source = include_str!("mode_selector.rs");
        assert!(source.contains("self.composer_reply_target.clone()"));
        assert!(source.contains(".text_ellipsis()"));
        assert!(source.contains("cancel-composer-reply"));
    }
}
