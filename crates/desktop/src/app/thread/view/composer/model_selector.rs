use crate::{
    app::root::PioneerDesktop,
    components::buttonts::{default_outline_button, default_primary_button},
    gateway::GatewayWsCommandSender,
};
use gpui::{prelude::*, *};
use gpui_component::{
    Icon,
    button::*,
    divider::Divider,
    input::{Input, InputState},
    label::Label,
    popover::{Popover, PopoverState},
    scroll::Scrollbar,
    theme::ActiveTheme,
    *,
};
use pioneer_protocol::{ProviderListModelsParams, ProviderModelInfo, ProviderSummary};
use std::{
    cell::RefCell,
    collections::HashMap,
    hash::{Hash, Hasher},
    rc::Rc,
};

/// Minimum height of a model row in the virtual list (in pixels).
const MODEL_ROW_MIN_HEIGHT: f32 = 32.0;
/// Maximum visible height for the model virtual list.
const MODEL_LIST_MAX_HEIGHT: f32 = 260.0;
/// Fallback width of selector popovers before trigger width is measured.
const SELECTOR_POPOVER_FALLBACK_WIDTH: f32 = 380.0;

#[derive(Clone)]
struct ModelSelectorDialogState {
    desktop_entity: Entity<PioneerDesktop>,
    ws_sender: GatewayWsCommandSender,
    providers: Rc<RefCell<Vec<ProviderSummary>>>,
    models: Rc<RefCell<Vec<ProviderModelInfo>>>,
    selected_provider: Rc<RefCell<Option<String>>>,
    selected_model: Rc<RefCell<Option<String>>>,
    loading_providers: Rc<RefCell<bool>>,
    loading_models: Rc<RefCell<bool>>,
    error_message: Rc<RefCell<Option<String>>>,
    provider_search_input: Entity<InputState>,
    model_search_input: Entity<InputState>,
    provider_scroll_handle: ScrollHandle,
    model_scroll_handle: VirtualListScrollHandle,
    provider_trigger_width_px: Rc<RefCell<f32>>,
    model_trigger_width_px: Rc<RefCell<f32>>,
    model_row_layout_cache: Rc<RefCell<HashMap<String, CachedModelRowLayout>>>,
}

#[derive(Clone, Copy)]
struct CachedModelRowLayout {
    layout_hash: u64,
    height_px: f32,
}

#[derive(IntoElement)]
struct SelectorPopoverTrigger {
    id: ElementId,
    label: SharedString,
    icon: IconName,
    selected: bool,
}

impl SelectorPopoverTrigger {
    fn new(id: impl Into<ElementId>, label: impl Into<SharedString>, icon: IconName) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            icon,
            selected: false,
        }
    }
}

impl Selectable for SelectorPopoverTrigger {
    fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    fn is_selected(&self) -> bool {
        self.selected
    }
}

impl RenderOnce for SelectorPopoverTrigger {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();

        let border_color = if self.selected {
            theme.primary.opacity(0.8)
        } else {
            theme.border
        };

        let bg_color = if self.selected {
            theme.secondary.opacity(0.25)
        } else {
            theme.background
        };

        div()
            .id(self.id)
            .w_full()
            .h_8()
            .px_2()
            .flex()
            .items_center()
            .rounded(theme.radius)
            .border_1()
            .border_color(border_color)
            .bg(bg_color)
            .text_color(theme.foreground)
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .child(
                        div()
                            .flex_1()
                            .text_sm()
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(self.label),
                    )
                    .child(Icon::new(self.icon).size_3p5()),
            )
    }
}

impl PioneerDesktop {
    pub(super) fn render_composer_model_selector(&self, cx: &mut Context<Self>) -> AnyElement {
        let display_label = if let (Some(provider), Some(model)) = (
            &self.composer_selected_provider,
            &self.composer_selected_model,
        ) {
            format!("{}/{}", provider, model)
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
                view.open_model_selector_dialog(window, cx);
            }))
            .into_any_element()
    }

    fn open_model_selector_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let desktop_entity = cx.entity().clone();
        let ws_sender = self.gateway.ws_command_sender.clone();

        // Shared dialog state via Rc<RefCell<..>>
        let providers: Rc<RefCell<Vec<ProviderSummary>>> = Rc::new(RefCell::new(Vec::new()));
        let models: Rc<RefCell<Vec<ProviderModelInfo>>> = Rc::new(RefCell::new(Vec::new()));
        let selected_provider: Rc<RefCell<Option<String>>> =
            Rc::new(RefCell::new(self.composer_selected_provider.clone()));
        let selected_model: Rc<RefCell<Option<String>>> =
            Rc::new(RefCell::new(self.composer_selected_model.clone()));
        let loading_providers: Rc<RefCell<bool>> = Rc::new(RefCell::new(true));
        let loading_models: Rc<RefCell<bool>> = Rc::new(RefCell::new(false));
        let error_message: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));

        let provider_search_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(t!("chat.composer.model.provider_placeholder").to_string())
        });
        let model_search_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(t!("chat.composer.model.model_placeholder").to_string())
        });
        let provider_scroll_handle = ScrollHandle::new();
        let model_scroll_handle = VirtualListScrollHandle::new();
        let provider_trigger_width_px = Rc::new(RefCell::new(SELECTOR_POPOVER_FALLBACK_WIDTH));
        let model_trigger_width_px = Rc::new(RefCell::new(SELECTOR_POPOVER_FALLBACK_WIDTH));
        let model_row_layout_cache = Rc::new(RefCell::new(HashMap::new()));

        let state = ModelSelectorDialogState {
            desktop_entity,
            ws_sender,
            providers,
            models,
            selected_provider,
            selected_model,
            loading_providers,
            loading_models,
            error_message,
            provider_search_input,
            model_search_input,
            provider_scroll_handle,
            model_scroll_handle,
            provider_trigger_width_px,
            model_trigger_width_px,
            model_row_layout_cache,
        };

        Self::load_providers_async(cx, &state);
        Self::preload_selected_provider_models_async(
            cx,
            &state,
            self.composer_selected_provider.clone(),
        );
        Self::show_model_selector_dialog(window, cx, state);
    }

    fn load_providers_async(cx: &mut Context<Self>, state: &ModelSelectorDialogState) {
        let providers = state.providers.clone();
        let loading_providers = state.loading_providers.clone();
        let error_message = state.error_message.clone();
        let ws_sender = state.ws_sender.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move { ws_sender.provider_list() })
                    .await;
                let _ = this.update(&mut cx, |_view, cx| {
                    match result {
                        Ok(response) => {
                            *providers.borrow_mut() = response.providers;
                        }
                        Err(error) => {
                            *error_message.borrow_mut() = Some(format!("{error:#}"));
                        }
                    }
                    *loading_providers.borrow_mut() = false;
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn preload_selected_provider_models_async(
        cx: &mut Context<Self>,
        state: &ModelSelectorDialogState,
        selected_provider_name: Option<String>,
    ) {
        if let Some(provider_name) = selected_provider_name {
            *state.loading_models.borrow_mut() = true;
            Self::spawn_fetch_models_for_provider(cx, state.clone(), provider_name);
        }
    }

    fn spawn_fetch_models_for_provider(
        cx: &mut App,
        state: ModelSelectorDialogState,
        provider_name: String,
    ) {
        let models = state.models.clone();
        let loading_models = state.loading_models.clone();
        let error_message = state.error_message.clone();
        let model_row_layout_cache = state.model_row_layout_cache.clone();
        let ws_sender = state.ws_sender.clone();
        let desktop_entity = state.desktop_entity.clone();

        cx.spawn(move |cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.provider_list_models(ProviderListModelsParams {
                            provider: provider_name,
                        })
                    })
                    .await;
                let _ = desktop_entity.update(&mut cx, |_view, cx| {
                    match result {
                        Ok(response) => {
                            *models.borrow_mut() = response.models;
                            model_row_layout_cache.borrow_mut().clear();
                        }
                        Err(error) => {
                            *error_message.borrow_mut() = Some(format!("{error:#}"));
                        }
                    }
                    *loading_models.borrow_mut() = false;
                    cx.notify();
                });
            }
        })
        .detach();
    }

    fn show_model_selector_dialog(
        window: &mut Window,
        cx: &mut Context<Self>,
        state: ModelSelectorDialogState,
    ) {
        window.open_dialog(cx, move |dialog, _window, _cx| {
            let save_selection = Self::save_model_selector_selection(state.clone());
            let provider_trigger_label = Self::provider_trigger_label(&state);
            let model_trigger_label = Self::model_trigger_label(&state);

            dialog
                .gap_1()
                .rounded_2xl()
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("chat.composer.model.dialog_title").to_string()),
                )
                .on_ok({
                    let save_selection = save_selection.clone();
                    move |_, _, cx| save_selection(cx)
                })
                .footer({
                    let save_selection = save_selection.clone();
                    move |_, _, _, _| {
                        vec![
                            default_outline_button("model-selector-cancel")
                                .label(t!("buttons.cancel").to_string())
                                .outline()
                                .on_click(|_, window, cx| {
                                    window.close_dialog(cx);
                                })
                                .into_any_element(),
                            default_primary_button("model-selector-save")
                                .label(t!("buttons.save").to_string())
                                .on_click({
                                    let save_selection = save_selection.clone();
                                    move |_, window, cx| {
                                        if save_selection(cx) {
                                            window.close_dialog(cx);
                                        }
                                    }
                                })
                                .into_any_element(),
                        ]
                    }
                })
                .child(
                    v_flex()
                        .w_full()
                        .pt_4()
                        .pb_5()
                        .gap_4()
                        .child(Self::render_provider_selector_section(
                            state.clone(),
                            provider_trigger_label,
                        ))
                        .child(Self::render_model_selector_section(
                            state.clone(),
                            model_trigger_label,
                        )),
                )
        });
    }

    fn save_model_selector_selection(
        state: ModelSelectorDialogState,
    ) -> Rc<dyn Fn(&mut App) -> bool> {
        Rc::new(move |cx| {
            let provider = state.selected_provider.borrow().clone();
            let model = state.selected_model.borrow().clone();
            let _ = state.desktop_entity.update(cx, |view, cx| {
                view.composer_selected_provider = provider;
                view.composer_selected_model = model;
                cx.notify();
            });
            true
        })
    }

    fn provider_trigger_label(state: &ModelSelectorDialogState) -> String {
        state
            .selected_provider
            .borrow()
            .clone()
            .unwrap_or_else(|| t!("chat.composer.model.provider_placeholder").to_string())
    }

    fn model_trigger_label(state: &ModelSelectorDialogState) -> String {
        state
            .selected_model
            .borrow()
            .clone()
            .unwrap_or_else(|| t!("chat.composer.model.model_placeholder").to_string())
    }

    fn render_provider_selector_section(
        state: ModelSelectorDialogState,
        provider_trigger_label: String,
    ) -> AnyElement {
        let provider_trigger_width_px = state.provider_trigger_width_px.clone();
        let desktop_entity = state.desktop_entity.clone();
        v_flex()
            .w_full()
            .gap_1()
            .child(Label::new(t!("chat.composer.model.provider_label")).text_xs())
            .child(
                div()
                    .w_full()
                    .relative()
                    .child(
                        Popover::new("model-provider-popover")
                            .anchor(Corner::TopLeft)
                            .p_0()
                            .trigger(SelectorPopoverTrigger::new(
                                "model-provider-trigger",
                                provider_trigger_label,
                                IconName::ChevronsUpDown,
                            ))
                            .content(move |_, _, popover_cx| {
                                Self::render_provider_popover_content(state.clone(), popover_cx)
                            }),
                    )
                    .child(
                        canvas(
                            move |bounds, _, cx| {
                                let measured_width = bounds.size.width.max(px(1.)).as_f32();
                                let mut cached_width = provider_trigger_width_px.borrow_mut();
                                if (measured_width - *cached_width).abs() > 1.0 {
                                    *cached_width = measured_width;
                                    let _ = desktop_entity.update(cx, |_view, cx| {
                                        cx.notify();
                                    });
                                }
                            },
                            |_, _, _, _| {},
                        )
                        .absolute()
                        .size_full(),
                    ),
            )
            .into_any_element()
    }

    fn render_provider_popover_content(
        state: ModelSelectorDialogState,
        popover_cx: &mut Context<PopoverState>,
    ) -> AnyElement {
        let popover_entity: Entity<PopoverState> = popover_cx.entity();
        let theme = popover_cx.theme();
        let muted_fg = theme.muted_foreground;
        let muted_bg = theme.muted;
        let foreground = theme.foreground;
        let ghost_hover = if theme.mode.is_dark() {
            theme.secondary.lighten(0.2).opacity(0.8)
        } else {
            theme.secondary.darken(0.1).opacity(0.8)
        };
        let ghost_active = if theme.mode.is_dark() {
            theme.secondary.lighten(0.3).opacity(0.8)
        } else {
            theme.secondary.darken(0.2).opacity(0.8)
        };
        let popover_width =
            px((*state.provider_trigger_width_px.borrow()).max(SELECTOR_POPOVER_FALLBACK_WIDTH));

        let provider_list = state.providers.borrow().clone();
        let is_loading = *state.loading_providers.borrow();
        let current_selected = state.selected_provider.borrow().clone();
        let search_text = state
            .provider_search_input
            .read(popover_cx)
            .value()
            .to_lowercase();

        let filtered: Vec<_> = provider_list
            .iter()
            .filter(|p| search_text.is_empty() || p.name.to_lowercase().contains(&search_text))
            .cloned()
            .collect();

        let mut content = v_flex()
            .w(popover_width)
            .child(
                div().child(
                    Input::new(&state.provider_search_input)
                        .appearance(false)
                        .px_2(),
                ),
            )
            .child(Divider::horizontal());

        let mut list = v_flex()
            .id("provider-popover-list")
            .max_h(px(200.))
            .overflow_y_scroll()
            .track_scroll(&state.provider_scroll_handle);

        if is_loading {
            list = list.child(
                div()
                    .p_4()
                    .text_sm()
                    .text_color(muted_fg)
                    .child(t!("chat.composer.model.loading_providers").to_string()),
            );
        } else if filtered.is_empty() {
            list = list.child(
                div()
                    .p_4()
                    .text_sm()
                    .text_color(muted_fg)
                    .child(t!("chat.composer.model.no_providers").to_string()),
            );
        } else {
            for provider in filtered {
                let is_active = current_selected.as_deref() == Some(provider.name.as_str());
                let provider_name = provider.name.clone();
                let row_state = state.clone();
                let popover_entity = popover_entity.clone();
                let id: SharedString = format!("provider-opt-{}", provider.name).into();

                list = list.child(
                    div()
                        .id(id)
                        .w_full()
                        .cursor_pointer()
                        .px_2()
                        .py_1p5()
                        .text_sm()
                        .text_color(foreground)
                        .when(is_active, |d| d.bg(muted_bg))
                        .hover(move |d| d.bg(ghost_hover))
                        .active(move |d| d.bg(ghost_active))
                        .on_mouse_down(gpui::MouseButton::Left, |_, window, _| {
                            window.prevent_default();
                        })
                        .on_click(move |_, window, cx| {
                            Self::on_provider_selected(
                                row_state.clone(),
                                popover_entity.clone(),
                                provider_name.clone(),
                                window,
                                cx,
                            );
                        })
                        .child(provider.name),
                );
            }
        }

        content = content.child(
            div().relative().child(list).child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .child(Scrollbar::vertical(&state.provider_scroll_handle)),
            ),
        );
        content.into_any_element()
    }

    fn on_provider_selected(
        state: ModelSelectorDialogState,
        popover_entity: Entity<PopoverState>,
        provider_name: String,
        window: &mut Window,
        cx: &mut App,
    ) {
        *state.selected_provider.borrow_mut() = Some(provider_name.clone());
        *state.selected_model.borrow_mut() = None;
        *state.models.borrow_mut() = Vec::new();
        state.model_row_layout_cache.borrow_mut().clear();
        *state.loading_models.borrow_mut() = true;

        let _ = popover_entity.update(cx, |state, cx| {
            state.dismiss(window, cx);
        });

        Self::spawn_fetch_models_for_provider(cx, state.clone(), provider_name);

        let _ = state.desktop_entity.update(cx, |_, cx| cx.notify());
    }

    fn render_model_selector_section(
        state: ModelSelectorDialogState,
        model_trigger_label: String,
    ) -> AnyElement {
        let model_trigger_width_px = state.model_trigger_width_px.clone();
        let desktop_entity = state.desktop_entity.clone();
        v_flex()
            .w_full()
            .gap_1()
            .child(Label::new(t!("chat.composer.model.model_label")).text_xs())
            .child(
                div()
                    .w_full()
                    .relative()
                    .child(
                        Popover::new("model-model-popover")
                            .anchor(Corner::TopLeft)
                            .p_0()
                            .trigger(SelectorPopoverTrigger::new(
                                "model-model-trigger",
                                model_trigger_label,
                                IconName::ChevronsUpDown,
                            ))
                            .content(move |_, window, popover_cx| {
                                Self::render_model_popover_content(
                                    state.clone(),
                                    window,
                                    popover_cx,
                                )
                            }),
                    )
                    .child(
                        canvas(
                            move |bounds, _, cx| {
                                let measured_width = bounds.size.width.max(px(1.)).as_f32();
                                let mut cached_width = model_trigger_width_px.borrow_mut();
                                if (measured_width - *cached_width).abs() > 1.0 {
                                    *cached_width = measured_width;
                                    let _ = desktop_entity.update(cx, |_view, cx| {
                                        cx.notify();
                                    });
                                }
                            },
                            |_, _, _, _| {},
                        )
                        .absolute()
                        .size_full(),
                    ),
            )
            .into_any_element()
    }

    fn render_model_popover_content(
        state: ModelSelectorDialogState,
        window: &mut Window,
        popover_cx: &mut Context<PopoverState>,
    ) -> AnyElement {
        let popover_entity: Entity<PopoverState> = popover_cx.entity();
        let theme = popover_cx.theme();
        let muted_fg = theme.muted_foreground;
        let foreground = theme.foreground;
        let muted_bg = theme.muted;
        let ghost_hover = if theme.mode.is_dark() {
            theme.secondary.lighten(0.2).opacity(0.8)
        } else {
            theme.secondary.darken(0.1).opacity(0.8)
        };
        let ghost_active = if theme.mode.is_dark() {
            theme.secondary.lighten(0.3).opacity(0.8)
        } else {
            theme.secondary.darken(0.2).opacity(0.8)
        };
        let popover_width =
            px((*state.model_trigger_width_px.borrow()).max(SELECTOR_POPOVER_FALLBACK_WIDTH));

        let model_list = state.models.borrow().clone();
        let is_loading = *state.loading_models.borrow();
        let search_text = state
            .model_search_input
            .read(popover_cx)
            .value()
            .to_lowercase();
        let has_error = state.error_message.borrow().is_some();

        let filtered: Vec<ProviderModelInfo> = model_list
            .into_iter()
            .filter(|m| {
                if search_text.is_empty() {
                    return true;
                }
                let name_match = m
                    .name
                    .as_deref()
                    .map(|n| n.to_lowercase().contains(&search_text))
                    .unwrap_or(false);
                let id_match = m.id.to_lowercase().contains(&search_text);
                name_match || id_match
            })
            .collect();
        let filtered = Rc::new(filtered);

        let mut content = v_flex()
            .w(popover_width)
            .child(
                div().child(
                    Input::new(&state.model_search_input)
                        .appearance(false)
                        .px_2(),
                ),
            )
            .child(Divider::horizontal());

        if is_loading {
            content = content.child(
                div()
                    .p_4()
                    .text_sm()
                    .text_color(muted_fg)
                    .child(t!("chat.composer.model.loading_models").to_string()),
            );
        } else if has_error {
            let err_text = state.error_message.borrow().clone().unwrap_or_default();
            content = content.child(
                div()
                    .p_4()
                    .text_sm()
                    .text_color(muted_fg)
                    .child(t!("chat.composer.model.load_error", error = err_text).to_string()),
            );
        } else if filtered.is_empty() {
            content = content.child(
                div()
                    .p_4()
                    .text_sm()
                    .text_color(muted_fg)
                    .child(t!("chat.composer.model.no_models").to_string()),
            );
        } else {
            content = content.child(Self::render_model_virtual_list(
                state.clone(),
                filtered,
                popover_entity,
                foreground,
                muted_bg,
                ghost_hover,
                ghost_active,
                popover_width,
                window,
                popover_cx,
            ));
        }

        content.into_any_element()
    }

    fn render_model_virtual_list(
        state: ModelSelectorDialogState,
        filtered_models: Rc<Vec<ProviderModelInfo>>,
        popover_entity: Entity<PopoverState>,
        foreground: Hsla,
        muted_bg: Hsla,
        ghost_hover: Hsla,
        ghost_active: Hsla,
        row_width: Pixels,
        window: &mut Window,
        popover_cx: &mut Context<PopoverState>,
    ) -> AnyElement {
        let item_sizes = Self::measure_model_virtual_list_item_sizes(
            state.clone(),
            filtered_models.as_ref(),
            popover_entity.clone(),
            foreground,
            muted_bg,
            ghost_hover,
            ghost_active,
            row_width,
            window,
            popover_cx,
        );
        let visible_height = item_sizes
            .iter()
            .map(|item_size| item_size.height.as_f32())
            .sum::<f32>()
            .min(MODEL_LIST_MAX_HEIGHT);
        let scroll_handle = state.model_scroll_handle.clone();

        div()
            .min_h(px(visible_height))
            .relative()
            .overflow_hidden()
            .child(
                v_virtual_list(
                    state.desktop_entity.clone(),
                    "model-virtual-list",
                    item_sizes,
                    move |_view, visible_range, _window, _cx| {
                        visible_range
                            .filter_map(|ix| {
                                filtered_models.get(ix).map(|model| {
                                    Self::render_model_virtual_list_row(
                                        state.clone(),
                                        popover_entity.clone(),
                                        ix,
                                        model,
                                        foreground,
                                        muted_bg,
                                        ghost_hover,
                                        ghost_active,
                                    )
                                })
                            })
                            .collect::<Vec<_>>()
                    },
                )
                .with_sizing_behavior(ListSizingBehavior::Auto)
                .track_scroll(&scroll_handle),
            )
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .child(Scrollbar::vertical(&scroll_handle)),
            )
            .into_any_element()
    }

    #[allow(clippy::too_many_arguments)]
    fn measure_model_virtual_list_item_sizes(
        state: ModelSelectorDialogState,
        filtered_models: &[ProviderModelInfo],
        popover_entity: Entity<PopoverState>,
        foreground: Hsla,
        muted_bg: Hsla,
        ghost_hover: Hsla,
        ghost_active: Hsla,
        row_width: Pixels,
        window: &mut Window,
        popover_cx: &mut Context<PopoverState>,
    ) -> Rc<Vec<gpui::Size<Pixels>>> {
        Rc::new(
            filtered_models
                .iter()
                .enumerate()
                .map(|(ix, model)| {
                    let layout_hash = Self::model_row_layout_hash(model, row_width);
                    if let Some(cached_height_px) = {
                        let cache = state.model_row_layout_cache.borrow();
                        cache.get(model.id.as_str()).and_then(|cached| {
                            (cached.layout_hash == layout_hash).then_some(cached.height_px)
                        })
                    } {
                        return gpui::size(
                            px(0.),
                            px(cached_height_px).max(px(MODEL_ROW_MIN_HEIGHT)),
                        );
                    }

                    let mut row = Self::render_model_virtual_list_row(
                        state.clone(),
                        popover_entity.clone(),
                        ix,
                        model,
                        foreground,
                        muted_bg,
                        ghost_hover,
                        ghost_active,
                    );
                    let measured = row.layout_as_root(
                        size(
                            AvailableSpace::Definite(row_width),
                            AvailableSpace::MaxContent,
                        ),
                        window,
                        popover_cx,
                    );
                    let measured_height = measured.height.max(px(MODEL_ROW_MIN_HEIGHT));
                    state.model_row_layout_cache.borrow_mut().insert(
                        model.id.clone(),
                        CachedModelRowLayout {
                            layout_hash,
                            height_px: measured_height.as_f32(),
                        },
                    );

                    gpui::size(px(0.), measured_height)
                })
                .collect::<Vec<_>>(),
        )
    }

    fn model_row_layout_hash(model: &ProviderModelInfo, row_width: Pixels) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        model.id.hash(&mut hasher);
        model.name.hash(&mut hasher);
        row_width.as_f32().to_bits().hash(&mut hasher);
        hasher.finish()
    }

    #[allow(clippy::too_many_arguments)]
    fn render_model_virtual_list_row(
        state: ModelSelectorDialogState,
        popover_entity: Entity<PopoverState>,
        ix: usize,
        model: &ProviderModelInfo,
        foreground: Hsla,
        muted_bg: Hsla,
        ghost_hover: Hsla,
        ghost_active: Hsla,
    ) -> AnyElement {
        let model_id = model.id.clone();
        let display_name = model.name.clone().unwrap_or_else(|| model.id.clone());
        let is_active = state.selected_model.borrow().as_deref() == Some(model_id.as_str());
        let has_name = model.name.is_some();
        let raw_id = model.id.clone();
        let id: SharedString = format!("model-vl-{ix}").into();

        div()
            .id(id)
            .w_full()
            .min_h(px(MODEL_ROW_MIN_HEIGHT))
            .cursor_pointer()
            .py_1p5()
            .px_2()
            .text_color(foreground)
            .when(is_active, |d| d.bg(muted_bg))
            .hover(move |d| d.bg(ghost_hover))
            .active(move |d| d.bg(ghost_active))
            .on_mouse_down(gpui::MouseButton::Left, |_, window, _| {
                window.prevent_default();
            })
            .on_click(move |_, window, cx| {
                *state.selected_model.borrow_mut() = Some(model_id.clone());
                let _ = popover_entity.update(cx, |popover, cx| {
                    popover.dismiss(window, cx);
                });
            })
            .child(
                v_flex()
                    .w_full()
                    .min_w_0()
                    .child(div().text_sm().whitespace_normal().child(display_name))
                    .when(has_name, |d| {
                        d.child(
                            div()
                                .text_xs()
                                .whitespace_normal()
                                .line_height(relative(1.3))
                                .opacity(0.6)
                                .child(raw_id),
                        )
                    }),
            )
            .into_any_element()
    }
}
