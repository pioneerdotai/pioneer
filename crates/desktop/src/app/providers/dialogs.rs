use crate::app::root::PioneerDesktop;
use crate::components::buttonts::{default_outline_button, default_primary_button};
use gpui::{prelude::*, *};
use gpui_component::{
    WindowExt,
    button::*,
    form::{field, v_form},
    input::{Input, InputState},
    theme::ActiveTheme,
    *,
};
use std::rc::Rc;

impl PioneerDesktop {
    pub(super) fn open_provider_configuration_dialog(
        &mut self,
        provider_id: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let provider_id = Self::canonical_provider_id(provider_id.as_str());
        let Some(provider) = Self::provider_catalog_entry(provider_id.as_str()) else {
            return;
        };

        let provider_title = provider.title();
        let provider_description = provider.description();
        let is_configured = self.providers.is_configured(provider.id);
        let api_key_input_state = cx.new(|cx| {
            InputState::new(window, cx)
                .multi_line(true)
                .auto_grow(2, 7)
                .placeholder(t!("providers.dialog.api_key_placeholder").to_string())
        });
        let desktop_entity = cx.entity().clone();

        let save_api_key: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let desktop_entity = desktop_entity.clone();
            let provider_id = provider.id.to_owned();
            let api_key_input_state = api_key_input_state.clone();
            move |cx| {
                let api_key = api_key_input_state.read(cx).value().trim().to_owned();
                if api_key.is_empty() {
                    return false;
                }

                let _ = desktop_entity.update(cx, |view, cx| {
                    view.set_provider_api_key(provider_id.clone(), api_key.clone(), cx);
                    cx.notify();
                });

                true
            }
        });

        let delete_provider_id = is_configured.then(|| provider.id.to_owned());

        window.open_dialog(cx, move |dialog, window, cx| {
            api_key_input_state.update(cx, |state, cx| state.focus(window, cx));

            dialog
                .gap_1()
                .rounded_2xl()
                .title(
                    h_flex()
                        .items_center()
                        .gap_2()
                        .child(Self::render_provider_logo(
                            provider.id,
                            provider.logo_path,
                            px(20.),
                            cx.theme().mode.is_dark(),
                        ))
                        .child(
                            div().text_base().font_semibold().child(
                                t!("providers.dialog.title", provider = provider_title.as_str())
                                    .to_string(),
                            ),
                        ),
                )
                .on_ok({
                    let save_api_key = save_api_key.clone();
                    move |_, _, cx| save_api_key(cx)
                })
                .footer({
                    let save_api_key = save_api_key.clone();
                    let desktop_entity = desktop_entity.clone();
                    let delete_provider_id = delete_provider_id.clone();

                    move |_, _, _, _| {
                        let mut actions = vec![
                            default_outline_button("provider-dialog-cancel")
                                .label(t!("buttons.cancel").to_string())
                                .outline()
                                .on_click(|_, window, cx| {
                                    window.close_dialog(cx);
                                })
                                .into_any_element(),
                            default_primary_button("provider-dialog-save")
                                .label(t!("providers.button.submit").to_string())
                                .on_click({
                                    let save_api_key = save_api_key.clone();
                                    move |_, window, cx| {
                                        if save_api_key(cx) {
                                            window.close_dialog(cx);
                                        }
                                    }
                                })
                                .into_any_element(),
                        ];

                        if let Some(provider_id) = delete_provider_id.clone() {
                            actions.insert(
                                1,
                                default_outline_button("provider-dialog-delete")
                                    .label(t!("providers.button.remove_key").to_string())
                                    .danger()
                                    .on_click({
                                        let desktop_entity = desktop_entity.clone();
                                        let provider_id = provider_id.clone();
                                        move |_, window, cx| {
                                            let _ = desktop_entity.update(cx, |view, cx| {
                                                view.delete_provider_api_key(
                                                    provider_id.clone(),
                                                    cx,
                                                );
                                                cx.notify();
                                            });
                                            window.close_dialog(cx);
                                        }
                                    })
                                    .into_any_element(),
                            );
                        }

                        actions
                    }
                })
                .child(
                    v_flex()
                        .w_full()
                        .pb_5()
                        .gap_4()
                        .child(
                            div()
                                .text_sm()
                                .opacity(0.6)
                                .line_height(relative(1.35))
                                .child(provider_description.clone()),
                        )
                        .child(
                            v_form()
                                .child(
                                    field()
                                        .label(t!("providers.dialog.api_key_label").to_string())
                                        .child(Input::new(&api_key_input_state).min_w_0()),
                                )
                                .when(is_configured, |this| {
                                    this.child(
                                        field().label_indent(false).child(
                                            div()
                                                .text_xs()
                                                .line_height(relative(1.3))
                                                .opacity(0.6)
                                                .child(
                                                    t!(
                                                        "providers.dialog.replace_hint",
                                                        provider = provider_title.as_str()
                                                    )
                                                    .to_string(),
                                                ),
                                        ),
                                    )
                                }),
                        ),
                )
        });
    }
}
