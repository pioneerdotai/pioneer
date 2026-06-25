use crate::{
    app::root::PioneerDesktop,
    components::model_selector::{ModelSelectorDialogOptions, ModelSelectorSelection},
};
use gpui::{prelude::*, *};
use gpui_component::{
    Icon,
    button::{Button, ButtonVariants},
    spinner::Spinner,
    *,
};
use pioneer_client::providers::presentation::{
    self as provider_presentation, ProviderModelDisplayState,
};
use std::rc::Rc;

impl PioneerDesktop {
    pub(super) fn render_composer_model_selector(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let display_state = self.composer_model_display_state(cx);
        let display_label = match &display_state {
            ProviderModelDisplayState::Label(label) => label.clone(),
            ProviderModelDisplayState::Loading => String::new(),
            ProviderModelDisplayState::Missing => {
                t!("chat.composer.model.select_label").to_string()
            }
        };
        let effort_label = match &display_state {
            ProviderModelDisplayState::Label(_) => self
                .composer_selected_reasoning_effort
                .as_deref()
                .and_then(pioneer_protocol::ReasoningEffort::canonical_value)
                .map(provider_presentation::reasoning_effort_display_label)
                .filter(|label| !label.is_empty()),
            ProviderModelDisplayState::Loading | ProviderModelDisplayState::Missing => None,
        };
        let loading = matches!(display_state, ProviderModelDisplayState::Loading);

        Button::new("composer-model-trigger")
            .small()
            .ghost()
            .compact()
            .child(
                h_flex()
                    .items_center()
                    .gap_1()
                    .opacity(0.6)
                    .when(loading, |this| {
                        this.child(
                            Spinner::new()
                                .with_size(gpui_component::Size::Small)
                                .color(cx.theme().muted_foreground),
                        )
                    })
                    .when(!loading, |this| {
                        this.child(
                            h_flex()
                                .min_w_0()
                                .max_w(px(350.))
                                .gap_1()
                                .child(
                                    div()
                                        .min_w_0()
                                        .text_ellipsis()
                                        .overflow_hidden()
                                        .child(display_label),
                                )
                                .when_some(effort_label, |row, effort_label| {
                                    row.child(div().flex_none().opacity(0.6).child(effort_label))
                                }),
                        )
                    })
                    .child(
                        div()
                            .flex_none()
                            .child(Icon::new(IconName::ChevronDown).size_3()),
                    )
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
                selected_reasoning_effort: self.composer_selected_reasoning_effort.clone(),
                workspace_id,
                ws_sender: self.gateway.ws_command_sender.clone(),
                on_save: Rc::new(
                    |view: &mut PioneerDesktop, selection: ModelSelectorSelection, _cx| {
                        view.set_composer_model_selection_from_user(
                            selection.provider,
                            selection.model,
                        );
                        view.set_composer_reasoning_effort_from_user(
                            selection.selected_reasoning_effort,
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
