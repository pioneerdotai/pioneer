use crate::{
    app::root::PioneerDesktop,
    components::model_selector::{ModelSelectorDialogOptions, ModelSelectorSelection},
};
use gpui::{prelude::*, *};
use gpui_component::{
    Icon,
    button::{Button, ButtonVariants},
    *,
};
use std::rc::Rc;

impl PioneerDesktop {
    pub(super) fn render_composer_model_selector(&self, cx: &mut Context<Self>) -> AnyElement {
        let display_label = if let (Some(provider), Some(model)) = (
            &self.composer_selected_provider,
            &self.composer_selected_model,
        ) {
            format!("{provider}/{model}")
        } else {
            t!("chat.composer.model.select_label").to_string()
        };

        Button::new("composer-model-trigger")
            .small()
            .ghost()
            .compact()
            .child(
                h_flex()
                    .items_center()
                    .gap_1()
                    .opacity(0.6)
                    .child(
                        div()
                            .text_ellipsis()
                            .max_w(px(350.))
                            .overflow_hidden()
                            .child(display_label),
                    )
                    .child(Icon::new(IconName::ChevronDown).size_3())
                    .font_medium(),
            )
            .on_click(cx.listener(|view, _, window, cx| {
                view.open_composer_model_selector_dialog(window, cx);
            }))
            .into_any_element()
    }

    fn open_composer_model_selector_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let workspace_id = self.model_selector_workspace_id();
        self.open_model_selector_dialog(
            ModelSelectorDialogOptions {
                title: t!("chat.composer.model.dialog_title").to_string(),
                selected_provider: self.composer_selected_provider.clone(),
                selected_model: self.composer_selected_model.clone(),
                workspace_id,
                ws_sender: self.gateway.ws_command_sender.clone(),
                on_save: Rc::new(
                    |view: &mut PioneerDesktop, selection: ModelSelectorSelection, _cx| {
                        view.set_composer_model_selection_from_user(
                            selection.provider,
                            selection.model,
                        );
                        true
                    },
                ),
            },
            window,
            cx,
        );
    }
}
