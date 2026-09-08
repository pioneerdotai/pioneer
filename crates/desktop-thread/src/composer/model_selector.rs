use crate::composer::ComposerView;
use gpui_kit::component::Icon;
use gpui_kit::component::button::Button;
use gpui_kit::component::button::ButtonVariants;
use gpui_kit::component::*;
use gpui_kit::prelude::*;
use gpui_kit::*;
use pioneer_client::providers::presentation as provider_presentation;
use pioneer_client::providers::presentation::ProviderModelDisplayState;

impl ComposerView {
    pub(super) fn render_composer_model_selector(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let display_state = self.composer_model_display_state();
        let display_label = match &display_state {
            ProviderModelDisplayState::Label(label) => label.clone(),
            ProviderModelDisplayState::Loading => String::new(),
            ProviderModelDisplayState::Missing => {
                t!("chat.composer.model.select_label").to_string()
            }
        };
        let effort_label = match &display_state {
            ProviderModelDisplayState::Label(_) => self
                .composer_domain()
                .selected_reasoning_effort
                .as_deref()
                .and_then(provider_presentation::normalize_reasoning_effort)
                .map(|effort| {
                    provider_presentation::reasoning_effort_display_label(effort.as_str())
                })
                .filter(|label| !label.is_empty()),
            ProviderModelDisplayState::Loading | ProviderModelDisplayState::Missing => None,
        };
        let loading = matches!(display_state, ProviderModelDisplayState::Loading);
        let capabilities = self.principal_presentation_capabilities();
        let model_selection_available = (capabilities.can_use_providers
            || capabilities.can_use_cli_runtimes)
            && self.can_start_active_thread_agent_presentation();

        Button::new("composer-model-trigger")
            .small()
            .ghost()
            .compact()
            .disabled(self.desktop_voice_context_locked() || !model_selection_available)
            .child(
                h_flex()
                    .items_center()
                    .gap_1()
                    .opacity(0.6)
                    .when(loading, |this| {
                        this.child(
                            crate::qualification_diagnostics::spinner!(
                                pioneer_client::timeline::diagnostics::AnimationSourceId::ComposerModelSelector,
                            )
                            .with_size(gpui_kit::component::Size::Small)
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
        let capabilities = self.principal_presentation_capabilities();
        if (!capabilities.can_use_providers && !capabilities.can_use_cli_runtimes)
            || !self.can_start_active_thread_agent_presentation()
        {
            return;
        }
        let Some(draft) = self.composer_input.as_ref() else {
            return;
        };
        let client = self.client.clone();
        use pioneer_client::composer::model_picker::ComposerModelPickerIntent;
        let thread_id = draft.thread_id().to_owned();
        let result = client.composer_model_picker_intent(ComposerModelPickerIntent::Open {
            thread_id: thread_id.clone(),
            draft_id: draft.draft_id(),
            deferred: true,
        });
        if result.outcome() != pioneer_client::core::ClientTransitionOutcome::Changed {
            return;
        }
        let Some(input) = client.composer_model_picker_snapshot(&thread_id) else {
            return;
        };
        let registrar = self.thread_bindings.registrar();
        let view = cx.new(|cx| {
            crate::model_picker::ComposerModelPickerView::new(client, registrar, input, window, cx)
        });
        view.update(cx, |view, cx| view.open(window, cx));
        self.composer_model_picker = Some(view);
    }
}
