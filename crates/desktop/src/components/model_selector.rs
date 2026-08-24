use crate::{
    app::PioneerDesktop,
    components::buttonts::{default_outline_button, default_primary_button},
    gateway::GatewayWsCommandSender,
};
use gpui::{prelude::*, *};
use gpui_component::{
    Icon,
    dialog::DialogFooter,
    form::{Field, field, v_form},
    input::{Input, InputState},
    popover::{Popover, PopoverState},
    scroll::Scrollbar,
    separator::Separator,
    spinner::Spinner,
    theme::ActiveTheme,
    *,
};
pub(crate) use pioneer_client::composer::model_selection::ModelSelectorSelection;
use pioneer_client::providers::list::{
    self as provider_list, ProviderModelSelectorMode, ProviderModelSelectorState,
};
use pioneer_client::providers::presentation as provider_presentation;
use pioneer_protocol::{ProviderModelInfo, RuntimeSummary};
use std::{
    cell::RefCell,
    collections::HashMap,
    hash::{Hash, Hasher},
    rc::{Rc, Weak},
};

/// Minimum height of a model row in the virtual list (in pixels).
const MODEL_ROW_MIN_HEIGHT: f32 = 32.0;
/// Maximum visible height for the model virtual list.
const MODEL_LIST_MAX_HEIGHT: f32 = 260.0;
/// Fallback width of selector popovers before trigger width is measured.
const SELECTOR_POPOVER_FALLBACK_WIDTH: f32 = 380.0;

type ModelSelectorSaveCallback =
    Rc<dyn Fn(&mut PioneerDesktop, ModelSelectorSelection, &mut Context<PioneerDesktop>) -> bool>;

pub(crate) struct ModelSelectorDialogOptions {
    pub(crate) title: String,
    pub(crate) selected_provider: Option<String>,
    pub(crate) selected_model: Option<String>,
    pub(crate) selected_reasoning_effort: Option<String>,
    pub(crate) mode: ProviderModelSelectorMode,
    pub(crate) workspace_id: String,
    pub(crate) ws_sender: GatewayWsCommandSender,
    pub(crate) on_save: ModelSelectorSaveCallback,
}

#[derive(Clone)]
struct ModelSelectorDialogState {
    title: String,
    desktop_entity: Entity<PioneerDesktop>,
    ws_sender: GatewayWsCommandSender,
    workspace_id: String,
    on_save: ModelSelectorSaveCallback,
    selector: Rc<RefCell<ProviderModelSelectorState>>,
    mode: ProviderModelSelectorMode,
    selected_reasoning_effort: Rc<RefCell<Option<String>>>,
    provider_search_input: Entity<InputState>,
    model_search_input: Entity<InputState>,
    provider_scroll_handle: ScrollHandle,
    reasoning_scroll_handle: ScrollHandle,
    model_scroll_handle: VirtualListScrollHandle,
    provider_trigger_width_px: Rc<RefCell<f32>>,
    model_trigger_width_px: Rc<RefCell<f32>>,
    reasoning_trigger_width_px: Rc<RefCell<f32>>,
    model_row_layout_cache: Rc<RefCell<HashMap<String, CachedModelRowLayout>>>,
}

#[derive(Clone)]
pub(crate) struct OpenModelSelectorCliRuntimeBinding {
    workspace_id: String,
    selector: Weak<RefCell<ProviderModelSelectorState>>,
}

impl OpenModelSelectorCliRuntimeBinding {
    fn new(workspace_id: String, selector: &Rc<RefCell<ProviderModelSelectorState>>) -> Self {
        Self {
            workspace_id,
            selector: Rc::downgrade(selector),
        }
    }

    fn sync(&self, active_workspace_id: Option<&str>, runtimes: &[RuntimeSummary]) -> bool {
        let Some(selector) = self.selector.upgrade() else {
            return false;
        };
        let runtimes = if active_workspace_id == Some(self.workspace_id.as_str()) {
            runtimes.to_vec()
        } else {
            Vec::new()
        };
        selector.borrow_mut().sync_cli_runtime_snapshot(runtimes);
        true
    }
}

#[derive(Clone, Copy)]
struct CachedModelRowLayout {
    layout_hash: u64,
    height_px: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ApiProviderModelListKind {
    Chat,
    Embeddings,
    Transcription,
}

fn api_provider_model_list_kind(mode: ProviderModelSelectorMode) -> ApiProviderModelListKind {
    match mode {
        ProviderModelSelectorMode::Chat | ProviderModelSelectorMode::SelfImprovement => {
            ApiProviderModelListKind::Chat
        }
        ProviderModelSelectorMode::Embeddings => ApiProviderModelListKind::Embeddings,
        ProviderModelSelectorMode::Transcription => ApiProviderModelListKind::Transcription,
    }
}

#[derive(IntoElement)]
struct SelectorPopoverTrigger {
    id: ElementId,
    label: SharedString,
    icon: IconName,
    selected: bool,
    loading: bool,
}

impl SelectorPopoverTrigger {
    fn new(id: impl Into<ElementId>, label: impl Into<SharedString>, icon: IconName) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            icon,
            selected: false,
            loading: false,
        }
    }

    fn loading(mut self, loading: bool) -> Self {
        self.loading = loading;
        self
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
                    .child(div().flex_1().child(if self.loading {
                        Spinner::new()
                            .with_size(gpui_component::Size::Small)
                            .color(theme.muted_foreground)
                            .into_any_element()
                    } else {
                        div()
                            .text_sm()
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(self.label)
                            .into_any_element()
                    }))
                    .child(Icon::new(self.icon).size_3p5()),
            )
    }
}

impl PioneerDesktop {
    pub(crate) fn open_model_selector_dialog(
        &mut self,
        options: ModelSelectorDialogOptions,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let desktop_entity = cx.entity().clone();

        let selector = Rc::new(RefCell::new(ProviderModelSelectorState::new_with_mode(
            options.selected_provider.clone(),
            options.selected_model.clone(),
            options.mode,
        )));
        selector.borrow_mut().mark_providers_loading();
        if options.mode == ProviderModelSelectorMode::Chat {
            selector
                .borrow_mut()
                .sync_cli_runtime_snapshot(self.model_selector_cli_runtimes().to_vec());
        }
        self.open_model_selector_cli_runtime_binding =
            (options.mode == ProviderModelSelectorMode::Chat).then(|| {
                OpenModelSelectorCliRuntimeBinding::new(options.workspace_id.clone(), &selector)
            });

        let provider_search_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(t!("chat.composer.model.provider_placeholder").to_string())
        });
        let model_search_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(t!("chat.composer.model.model_placeholder").to_string())
        });
        let provider_scroll_handle = ScrollHandle::new();
        let reasoning_scroll_handle = ScrollHandle::new();
        let model_scroll_handle = VirtualListScrollHandle::new();
        let provider_trigger_width_px = Rc::new(RefCell::new(SELECTOR_POPOVER_FALLBACK_WIDTH));
        let model_trigger_width_px = Rc::new(RefCell::new(SELECTOR_POPOVER_FALLBACK_WIDTH));
        let reasoning_trigger_width_px = Rc::new(RefCell::new(SELECTOR_POPOVER_FALLBACK_WIDTH));
        let model_row_layout_cache = Rc::new(RefCell::new(HashMap::new()));

        let state = ModelSelectorDialogState {
            title: options.title,
            desktop_entity,
            ws_sender: options.ws_sender,
            workspace_id: options.workspace_id,
            on_save: options.on_save,
            selector,
            mode: options.mode,
            selected_reasoning_effort: Rc::new(RefCell::new(options.selected_reasoning_effort)),
            provider_search_input,
            model_search_input,
            provider_scroll_handle,
            reasoning_scroll_handle,
            model_scroll_handle,
            provider_trigger_width_px,
            model_trigger_width_px,
            reasoning_trigger_width_px,
            model_row_layout_cache,
        };

        Self::load_providers_async(cx, &state);
        Self::preload_selected_provider_models_async(cx, &state, options.selected_provider);
        Self::show_model_selector_dialog(window, cx, state);
    }

    fn load_providers_async(cx: &mut Context<Self>, state: &ModelSelectorDialogState) {
        let selector = state.selector.clone();
        let ws_sender = state.ws_sender.clone();
        let workspace_id = state.workspace_id.clone();

        cx.spawn(move |this: WeakEntity<Self>, cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        ws_sender.provider_list(provider_list::provider_list_params(workspace_id))
                    })
                    .await;
                let _ = this.update(&mut cx, |_view, cx| {
                    match result {
                        Ok(response) => {
                            selector.borrow_mut().apply_provider_list_success(response);
                        }
                        Err(error) => {
                            selector
                                .borrow_mut()
                                .apply_provider_list_error(format!("{error:#}"));
                        }
                    }
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
        let provider_name = selected_provider_name.and_then(|_| {
            state
                .selector
                .borrow_mut()
                .preload_selected_provider_models()
        });
        if let Some(provider_name) = provider_name {
            Self::spawn_fetch_models_for_provider(cx, state.clone(), provider_name);
        }
    }

    fn spawn_fetch_models_for_provider(
        cx: &mut App,
        state: ModelSelectorDialogState,
        provider_name: String,
    ) {
        let selector = state.selector.clone();
        let model_row_layout_cache = state.model_row_layout_cache.clone();
        let dialog_state = state.clone();
        let ws_sender = state.ws_sender.clone();
        let desktop_entity = state.desktop_entity.clone();
        let workspace_id = state.workspace_id.clone();
        let mode = state.mode;
        let provider_name_for_error = provider_name.clone();

        cx.spawn(move |cx: &mut AsyncApp| {
            let mut cx = cx.clone();
            async move {
                let result = cx
                    .background_spawn(async move {
                        if let Some(runtime_id) =
                            provider_list::runtime_id_from_cli_runtime_provider_key(
                                provider_name.as_str(),
                            )
                        {
                            ws_sender
                                .cli_runtime_list_models(provider_list::cli_runtime_list_models_params(
                                    workspace_id,
                                    runtime_id.to_owned(),
                                ))
                                .map(|response| {
                                    provider_list::provider_models_response_from_cli_runtime_models_response(
                                        provider_name,
                                        response,
                                    )
                                })
                        } else {
                            match api_provider_model_list_kind(mode) {
                                ApiProviderModelListKind::Chat => ws_sender.provider_list_models(
                                    provider_list::provider_list_models_params(
                                        workspace_id,
                                        provider_name,
                                    ),
                                ),
                                ApiProviderModelListKind::Embeddings => ws_sender
                                    .provider_list_embedding_models(
                                        provider_list::provider_list_embedding_models_params(
                                            workspace_id,
                                            provider_name,
                                        ),
                                    ),
                                ApiProviderModelListKind::Transcription => ws_sender
                                    .provider_list_transcription_models(
                                        provider_list::provider_list_transcription_models_params(
                                            workspace_id,
                                            provider_name,
                                        ),
                                    ),
                            }
                        }
                    })
                    .await;
                let _ = desktop_entity.update(&mut cx, |_view, cx| {
                    match result {
                        Ok(response) => {
                            if selector
                                .borrow_mut()
                                .apply_provider_models_success(response)
                            {
                                model_row_layout_cache.borrow_mut().clear();
                                Self::clear_invalid_dialog_reasoning_effort(&dialog_state);
                            }
                        }
                        Err(error) => {
                            selector.borrow_mut().apply_provider_models_error(
                                provider_name_for_error.as_str(),
                                format!("{error:#}"),
                            );
                        }
                    }
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
            let model_trigger_loading = Self::model_trigger_loading(&state);

            dialog
                .gap_1()
                .rounded_2xl()
                .title(div().text_base().font_semibold().child(state.title.clone()))
                .on_ok({
                    let save_selection = save_selection.clone();
                    move |_, _, cx| save_selection(cx)
                })
                .footer(DialogFooter::new().children({
                    let save_selection = save_selection.clone();
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
                }))
                .child(v_flex().w_full().pt_4().pb_5().child({
                    let form = v_form()
                        .child(Self::render_provider_selector_section(
                            state.clone(),
                            provider_trigger_label,
                        ))
                        .child(Self::render_model_selector_section(
                            state.clone(),
                            model_trigger_label,
                            model_trigger_loading,
                        ));

                    if let Some(reasoning_section) =
                        Self::render_reasoning_effort_selector_section(state.clone())
                    {
                        form.child(reasoning_section)
                    } else {
                        form
                    }
                }))
        });
    }

    pub(crate) fn sync_open_model_selector_cli_runtime_snapshot(&mut self) {
        let Some(binding) = self.open_model_selector_cli_runtime_binding.clone() else {
            return;
        };
        let runtimes = self.model_selector_cli_runtimes().to_vec();
        if !binding.sync(self.active_workspace_id(), runtimes.as_slice()) {
            self.open_model_selector_cli_runtime_binding = None;
        }
    }

    fn save_model_selector_selection(
        state: ModelSelectorDialogState,
    ) -> Rc<dyn Fn(&mut App) -> bool> {
        Rc::new(move |cx| {
            let (provider, model) = state.selector.borrow().selection_parts();
            let selected_reasoning_effort = state.selected_reasoning_effort.borrow().clone();
            state.desktop_entity.update(cx, |view, cx| {
                let saved = (state.on_save)(
                    view,
                    ModelSelectorSelection {
                        provider,
                        model,
                        selected_reasoning_effort,
                    },
                    cx,
                );
                cx.notify();
                saved
            })
        })
    }

    fn provider_trigger_label(state: &ModelSelectorDialogState) -> String {
        state
            .selector
            .borrow()
            .selected_provider_label()
            .unwrap_or_else(|| t!("chat.composer.model.provider_placeholder").to_string())
    }

    fn model_trigger_label(state: &ModelSelectorDialogState) -> String {
        let selector = state.selector.borrow();
        match provider_presentation::model_selector_selected_model_display_state(
            selector.selected_model(),
            selector.models(),
            selector.loading_models(),
        ) {
            provider_presentation::ProviderModelDisplayState::Label(label) => label,
            provider_presentation::ProviderModelDisplayState::Loading
            | provider_presentation::ProviderModelDisplayState::Missing => {
                t!("chat.composer.model.model_placeholder").to_string()
            }
        }
    }

    fn model_trigger_loading(state: &ModelSelectorDialogState) -> bool {
        let selector = state.selector.borrow();
        matches!(
            provider_presentation::model_selector_selected_model_display_state(
                selector.selected_model(),
                selector.models(),
                selector.loading_models(),
            ),
            provider_presentation::ProviderModelDisplayState::Loading
        )
    }

    fn reasoning_effort_rows(
        state: &ModelSelectorDialogState,
    ) -> Vec<provider_presentation::ReasoningEffortRow> {
        if matches!(
            state.mode,
            ProviderModelSelectorMode::Transcription | ProviderModelSelectorMode::SelfImprovement
        ) {
            return Vec::new();
        }
        let selector = state.selector.borrow();
        let Some(selected_model) = selector.selected_model() else {
            return Vec::new();
        };
        let Some(model) = selector
            .models()
            .iter()
            .find(|model| model.id.as_str() == selected_model)
        else {
            return Vec::new();
        };
        let selected_effort = state.selected_reasoning_effort.borrow();
        provider_presentation::reasoning_effort_rows_for_model(model, selected_effort.as_deref())
    }

    fn reasoning_effort_trigger_label(
        rows: &[provider_presentation::ReasoningEffortRow],
    ) -> String {
        rows.iter()
            .find(|row| row.selected)
            .map(|row| row.label.clone())
            .unwrap_or_else(|| t!("chat.composer.model.reasoning_default").to_string())
    }

    fn clear_dialog_reasoning_effort(state: &ModelSelectorDialogState) {
        *state.selected_reasoning_effort.borrow_mut() = None;
    }

    fn clear_invalid_dialog_reasoning_effort(state: &ModelSelectorDialogState) {
        if state.selected_reasoning_effort.borrow().is_none() {
            return;
        }

        if !Self::reasoning_effort_rows(state)
            .iter()
            .any(|row| row.selected)
        {
            Self::clear_dialog_reasoning_effort(state);
        }
    }

    fn render_provider_selector_section(
        state: ModelSelectorDialogState,
        provider_trigger_label: String,
    ) -> Field {
        let provider_trigger_width_px = state.provider_trigger_width_px.clone();
        let desktop_entity = state.desktop_entity.clone();
        field()
            .label(t!("chat.composer.model.provider_label").to_string())
            .child(
                div()
                    .w_full()
                    .relative()
                    .child(
                        Popover::new("model-provider-popover")
                            .anchor(Anchor::TopLeft)
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

        let (provider_rows, is_loading, current_selected) = {
            let selector = state.selector.borrow();
            (
                selector.provider_rows(),
                selector.loading_providers(),
                selector.selected_provider().map(str::to_owned),
            )
        };
        let search_text = state
            .provider_search_input
            .read(popover_cx)
            .value()
            .to_owned();

        let search = search_text.trim().to_ascii_lowercase();
        let filtered = provider_rows
            .into_iter()
            .filter(|provider| {
                search.is_empty()
                    || provider.id.to_ascii_lowercase().contains(search.as_str())
                    || provider
                        .label
                        .to_ascii_lowercase()
                        .contains(search.as_str())
            })
            .collect::<Vec<_>>();

        let mut content = v_flex()
            .w(popover_width)
            .child(render_selector_filter_form(&state.provider_search_input))
            .child(Separator::horizontal());

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
                let is_active = current_selected.as_deref() == Some(provider.id.as_str());
                let provider_id = provider.id.clone();
                let provider_label = provider.label.clone();
                let row_state = state.clone();
                let popover_entity = popover_entity.clone();
                let id: SharedString = format!("provider-opt-{}", provider.id).into();

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
                                provider_id.clone(),
                                window,
                                cx,
                            );
                        })
                        .child(provider_label),
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
        let provider_name = state.selector.borrow_mut().select_provider(provider_name);
        Self::clear_dialog_reasoning_effort(&state);
        state.model_row_layout_cache.borrow_mut().clear();

        let _ = popover_entity.update(cx, |state, cx| {
            state.dismiss(window, cx);
        });

        Self::spawn_fetch_models_for_provider(cx, state.clone(), provider_name);

        let _ = state.desktop_entity.update(cx, |_, cx| cx.notify());
    }

    fn render_model_selector_section(
        state: ModelSelectorDialogState,
        model_trigger_label: String,
        model_trigger_loading: bool,
    ) -> Field {
        let model_trigger_width_px = state.model_trigger_width_px.clone();
        let desktop_entity = state.desktop_entity.clone();
        field()
            .label(t!("chat.composer.model.model_label").to_string())
            .child(
                div()
                    .w_full()
                    .relative()
                    .child(
                        Popover::new("model-model-popover")
                            .anchor(Anchor::TopLeft)
                            .p_0()
                            .trigger(
                                SelectorPopoverTrigger::new(
                                    "model-model-trigger",
                                    model_trigger_label,
                                    IconName::ChevronsUpDown,
                                )
                                .loading(model_trigger_loading),
                            )
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
    }

    fn render_reasoning_effort_selector_section(state: ModelSelectorDialogState) -> Option<Field> {
        let rows = Self::reasoning_effort_rows(&state);
        if rows.is_empty() {
            return None;
        }

        let reasoning_trigger_width_px = state.reasoning_trigger_width_px.clone();
        let desktop_entity = state.desktop_entity.clone();
        let trigger_label = Self::reasoning_effort_trigger_label(rows.as_slice());

        Some(
            field()
                .label(t!("chat.composer.model.reasoning_label").to_string())
                .child(
                    div()
                        .w_full()
                        .relative()
                        .child(
                            Popover::new("model-reasoning-effort-popover")
                                .anchor(Anchor::TopLeft)
                                .p_0()
                                .trigger(SelectorPopoverTrigger::new(
                                    "model-reasoning-effort-trigger",
                                    trigger_label,
                                    IconName::ChevronsUpDown,
                                ))
                                .content(move |_, _, popover_cx| {
                                    Self::render_reasoning_effort_popover_content(
                                        state.clone(),
                                        popover_cx,
                                    )
                                }),
                        )
                        .child(
                            canvas(
                                move |bounds, _, cx| {
                                    let measured_width = bounds.size.width.max(px(1.)).as_f32();
                                    let mut cached_width = reasoning_trigger_width_px.borrow_mut();
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
                ),
        )
    }

    fn render_reasoning_effort_popover_content(
        state: ModelSelectorDialogState,
        popover_cx: &mut Context<PopoverState>,
    ) -> AnyElement {
        let popover_entity: Entity<PopoverState> = popover_cx.entity();
        let theme = popover_cx.theme();
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
            px((*state.reasoning_trigger_width_px.borrow()).max(SELECTOR_POPOVER_FALLBACK_WIDTH));
        let rows = Self::reasoning_effort_rows(&state);

        let mut list = v_flex()
            .id("reasoning-effort-popover-list")
            .max_h(px(200.))
            .overflow_y_scroll()
            .track_scroll(&state.reasoning_scroll_handle);
        let default_selected = state.selected_reasoning_effort.borrow().is_none();
        let default_state = state.clone();
        let default_popover_entity = popover_entity.clone();

        list = list.child(
            div()
                .id("reasoning-effort-opt-default")
                .w_full()
                .cursor_pointer()
                .px_2()
                .py_1p5()
                .text_sm()
                .text_color(foreground)
                .when(default_selected, |d| d.bg(muted_bg))
                .hover(move |d| d.bg(ghost_hover))
                .active(move |d| d.bg(ghost_active))
                .on_mouse_down(gpui::MouseButton::Left, |_, window, _| {
                    window.prevent_default();
                })
                .on_click(move |_, window, cx| {
                    Self::clear_dialog_reasoning_effort(&default_state);
                    let _ = default_popover_entity.update(cx, |popover, cx| {
                        popover.dismiss(window, cx);
                    });
                    let _ = default_state.desktop_entity.update(cx, |_, cx| cx.notify());
                })
                .child(t!("chat.composer.model.reasoning_default").to_string()),
        );

        for row in rows {
            let row_state = state.clone();
            let popover_entity = popover_entity.clone();
            let effort = row.effort.clone();
            let id: SharedString = format!("reasoning-effort-opt-{}", row.effort).into();

            list = list.child(
                div()
                    .id(id)
                    .w_full()
                    .cursor_pointer()
                    .px_2()
                    .py_1p5()
                    .text_sm()
                    .text_color(foreground)
                    .when(row.selected, |d| d.bg(muted_bg))
                    .hover(move |d| d.bg(ghost_hover))
                    .active(move |d| d.bg(ghost_active))
                    .on_mouse_down(gpui::MouseButton::Left, |_, window, _| {
                        window.prevent_default();
                    })
                    .on_click(move |_, window, cx| {
                        if Self::reasoning_effort_rows(&row_state)
                            .iter()
                            .any(|row| row.effort == effort)
                        {
                            *row_state.selected_reasoning_effort.borrow_mut() =
                                Some(effort.clone());
                        }
                        let _ = popover_entity.update(cx, |popover, cx| {
                            popover.dismiss(window, cx);
                        });
                        let _ = row_state.desktop_entity.update(cx, |_, cx| cx.notify());
                    })
                    .child(row.label),
            );
        }

        v_flex()
            .w(popover_width)
            .child(
                div().relative().child(list).child(
                    div()
                        .absolute()
                        .top_0()
                        .left_0()
                        .right_0()
                        .bottom_0()
                        .child(Scrollbar::vertical(&state.reasoning_scroll_handle)),
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

        let (model_list, is_loading, error_text) = {
            let selector = state.selector.borrow();
            (
                selector.models().to_vec(),
                selector.loading_models(),
                selector.error().map(str::to_owned),
            )
        };
        let search_text = state.model_search_input.read(popover_cx).value().to_owned();
        let has_error = error_text.is_some();

        let filtered: Vec<ProviderModelInfo> =
            provider_presentation::filter_model_selector_models(&model_list, &search_text);
        let filtered = Rc::new(filtered);

        let mut content = v_flex()
            .w(popover_width)
            .child(render_selector_filter_form(&state.model_search_input))
            .child(Separator::horizontal());

        if is_loading {
            content = content.child(
                div()
                    .p_4()
                    .text_sm()
                    .text_color(muted_fg)
                    .child(t!("chat.composer.model.loading_models").to_string()),
            );
        } else if has_error {
            let err_text = error_text.unwrap_or_default();
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
        model.description.hash(&mut hasher);
        if let Some(metadata) = model.transcription.as_ref() {
            metadata.engine.hash(&mut hasher);
            metadata.download_size_mb.hash(&mut hasher);
            metadata.supported_languages.hash(&mut hasher);
            metadata.recommended.hash(&mut hasher);
        }
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
        let display_name = provider_presentation::model_selector_model_display_name(model);
        let secondary_text = provider_presentation::model_selector_model_secondary_text(model);
        let transcription = (state.mode == ProviderModelSelectorMode::Transcription)
            .then(|| provider_presentation::transcription_model_selector_presentation(model))
            .flatten();
        let is_active = state.selector.borrow().selected_model() == Some(model_id.as_str());
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
                let model_changed =
                    state.selector.borrow().selected_model() != Some(model_id.as_str());
                state
                    .selector
                    .borrow_mut()
                    .set_selected_model(model_id.clone());
                if model_changed {
                    Self::clear_dialog_reasoning_effort(&state);
                }
                let _ = popover_entity.update(cx, |popover, cx| {
                    popover.dismiss(window, cx);
                });
                let _ = state.desktop_entity.update(cx, |_, cx| cx.notify());
            })
            .child(
                v_flex()
                    .w_full()
                    .min_w_0()
                    .child(
                        h_flex()
                            .w_full()
                            .min_w_0()
                            .gap_2()
                            .child(
                                div()
                                    .min_w_0()
                                    .text_sm()
                                    .whitespace_normal()
                                    .child(display_name),
                            )
                            .when(
                                transcription
                                    .as_ref()
                                    .is_some_and(|details| details.recommended),
                                |row| {
                                    row.child(
                                        div()
                                            .flex_none()
                                            .px_1()
                                            .rounded_md()
                                            .border_1()
                                            .border_color(foreground.opacity(0.5))
                                            .text_size(px(10.))
                                            .font_medium()
                                            .child(
                                                t!("settings.voice_input.recommended").to_string(),
                                            ),
                                    )
                                },
                            ),
                    )
                    .when_some(secondary_text, |d, secondary_text| {
                        d.child(
                            div()
                                .text_xs()
                                .whitespace_normal()
                                .line_height(relative(1.3))
                                .opacity(0.6)
                                .child(secondary_text),
                        )
                    })
                    .when_some(transcription, |d, details| {
                        d.child(
                            div()
                                .text_xs()
                                .whitespace_normal()
                                .line_height(relative(1.3))
                                .opacity(0.75)
                                .child(format!(
                                    "{} | {} | {}",
                                    details.engine, details.download_size, details.language_summary
                                )),
                        )
                    }),
            )
            .into_any_element()
    }
}

fn render_selector_filter_form(search: &Entity<InputState>) -> AnyElement {
    v_form()
        .child(
            field()
                .label_indent(false)
                .child(Input::new(search).appearance(false).px_2().min_w_0()),
        )
        .into_any_element()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[::core::prelude::v1::test]
    fn model_selector_transcription_uses_dedicated_transport_kind() {
        assert_eq!(
            api_provider_model_list_kind(ProviderModelSelectorMode::Transcription),
            ApiProviderModelListKind::Transcription
        );
        assert_eq!(
            api_provider_model_list_kind(ProviderModelSelectorMode::Chat),
            ApiProviderModelListKind::Chat
        );
        assert_eq!(
            api_provider_model_list_kind(ProviderModelSelectorMode::SelfImprovement),
            ApiProviderModelListKind::Chat
        );
        assert_eq!(
            api_provider_model_list_kind(ProviderModelSelectorMode::Embeddings),
            ApiProviderModelListKind::Embeddings
        );
    }

    #[::core::prelude::v1::test]
    fn model_selector_transcription_recommended_badge_is_localized() {
        let source = include_str!("model_selector.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source exists");

        assert!(source.contains("settings.voice_input.recommended"));
        assert!(!source.contains(".child(\"Recommended\")"));
    }

    #[::core::prelude::v1::test]
    fn model_selector_dialog_render_never_reenters_the_desktop_entity() {
        let source = include_str!("model_selector.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source exists");
        let render = source
            .split("fn show_model_selector_dialog")
            .nth(1)
            .expect("dialog render exists")
            .split("pub(crate) fn sync_open_model_selector_cli_runtime_snapshot")
            .next()
            .expect("dialog render has a boundary");

        assert!(!render.contains("desktop_entity"));
        assert!(!render.contains(".read(cx)"));
    }

    #[::core::prelude::v1::test]
    fn model_selector_cli_runtime_binding_does_not_retain_a_closed_dialog() {
        let selector = Rc::new(RefCell::new(ProviderModelSelectorState::new_with_mode(
            None,
            None,
            ProviderModelSelectorMode::Chat,
        )));
        let binding = OpenModelSelectorCliRuntimeBinding::new("workspace".to_owned(), &selector);

        assert!(binding.sync(Some("workspace"), &[]));
        drop(selector);
        assert!(!binding.sync(Some("workspace"), &[]));
    }
}
