use crate::{
    app::PioneerDesktop,
    components::buttonts::{default_outline_button, default_primary_button},
};
use gpui_kit::component::{
    Icon,
    dialog::DialogFooter,
    form::{Field, field, v_form},
    input::{Input, InputState},
    popover::{Popover, PopoverState},
    scroll::Scrollbar,
    separator::Separator,
    theme::ActiveTheme,
    *,
};
use gpui_kit::{prelude::*, *};
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
    pub(crate) client: std::sync::Arc<pioneer_client::core::ClientCore>,
    pub(crate) on_save: ModelSelectorSaveCallback,
}

#[derive(Clone)]
struct ModelSelectorDialogState {
    title: String,
    desktop_entity: WeakEntity<PioneerDesktop>,
    owner: Entity<ModelSelectorOwner>,
    client: std::sync::Arc<pioneer_client::core::ClientCore>,
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

struct ModelSelectorOwner {
    client: std::sync::Weak<pioneer_client::core::ClientCore>, workspace: String,
    selector: Weak<RefCell<ProviderModelSelectorState>>, runtime: bool,
    subscription: Option<pioneer_client::core::ClientSubscription>,
    navigation: pioneer_client::core::ClientSubscription,
    window: AnyWindowHandle, task: Option<Task<()>>, providers: Option<Task<()>>, models: Option<Task<()>>,
    demand: std::sync::Arc<()>, model_demand: Option<std::sync::Arc<()>>, closed: bool,
}
impl ModelSelectorOwner {
    fn new(client: &std::sync::Arc<pioneer_client::core::ClientCore>, workspace: String, selector: &Rc<RefCell<ProviderModelSelectorState>>, runtime: bool, window: &mut Window, cx: &mut App) -> Entity<Self> {
        use pioneer_client::{core::ClientScope, providers::runtime::ProviderRuntimeIntent};
        let mut changed = client.watch_publications();
        let subscription = runtime.then(|| client.subscribe(ClientScope::ProviderRuntime { workspace_id: workspace.clone() }, std::num::NonZeroUsize::new(8).unwrap()));
        let navigation = client.subscribe(ClientScope::Navigation, std::num::NonZeroUsize::new(8).unwrap());
        if runtime { client.provider_runtime_intent(ProviderRuntimeIntent::Observe { workspace_id: workspace.clone() }); }
        let handle = window.window_handle(); let selector = Rc::downgrade(selector); let client = std::sync::Arc::downgrade(client);
        cx.new(|cx| {
            let task = cx.spawn(async move |owner: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if handle.update(cx, |_, window, cx| owner.update(cx, |owner, cx| {
                        let mut changed = false;
                        if let Some(subscription) = &owner.subscription { while subscription.try_next().is_some() { changed = true; } }
                        while owner.navigation.try_next().is_some() { changed = true; }
                        if !changed { return; }
                        let Some(client) = owner.client.upgrade() else { return; };
                        let Some(selector) = owner.selector.upgrade() else { return; };
                        let runtimes = (client.navigation_snapshot().workspace_id() == Some(owner.workspace.as_str())).then(|| client.provider_runtime_snapshot(&owner.workspace)).flatten().map(|p| p.runtimes().iter().map(|row| row.runtime().clone()).collect()).unwrap_or_default();
                        if selector.borrow().cli_runtimes() != &runtimes { selector.borrow_mut().sync_cli_runtime_snapshot(runtimes); gpui_kit::component::Root::update(window, cx, |_, _, cx| cx.notify()); }
                    })).is_err() { break; }
                }
            });
            Self { client, workspace, selector, runtime, subscription, navigation, window: handle, task: Some(task), providers: None, models: None, demand: std::sync::Arc::new(()), model_demand: None, closed: false }
        })
    }
    fn refresh(&self, cx: &mut App) { let _ = self.window.update(cx, |_, window, cx| gpui_kit::component::Root::update(window, cx, |_, _, cx| cx.notify())); }
    fn close(&mut self) { self.closed = true; self.demand = std::sync::Arc::new(()); self.model_demand = None; self.providers = None; self.models = None; self.task = None; self.subscription = None; if self.runtime { self.runtime = false; if let Some(client) = self.client.upgrade() { client.provider_runtime_intent(pioneer_client::providers::runtime::ProviderRuntimeIntent::Release { workspace_id: self.workspace.clone() }); } } }
}
impl Render for ModelSelectorOwner { fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement { div() } }
impl Drop for ModelSelectorOwner { fn drop(&mut self) { self.close(); } }

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
                        crate::qualification_diagnostics::spinner!(
                            pioneer_observability::AnimationSourceId::SharedModelSelector,
                        )
                        .with_size(gpui_kit::component::Size::Small)
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
        let desktop_entity = cx.weak_entity();

        let selector = Rc::new(RefCell::new(ProviderModelSelectorState::new_with_mode(
            options.selected_provider.clone(),
            options.selected_model.clone(),
            options.mode,
        )));
        selector.borrow_mut().mark_providers_loading();
        if options.mode == ProviderModelSelectorMode::Chat {
            selector
                .borrow_mut()
                .sync_cli_runtime_snapshot(options.client.provider_runtime_snapshot(&options.workspace_id).map(|p| p.runtimes().iter().map(|row| row.runtime().clone()).collect()).unwrap_or_default());
        }
        let owner = ModelSelectorOwner::new(&options.client, options.workspace_id.clone(), &selector, options.mode == ProviderModelSelectorMode::Chat, window, cx);

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
            owner,
            title: options.title,
            desktop_entity,
            client: options.client,
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
        let client = state.client.clone(); let workspace = state.workspace_id.clone();
        state.owner.update(cx, |owner, cx| {
            if owner.closed { return; }
            let demand = std::sync::Arc::downgrade(&owner.demand);
            owner.providers = Some(cx.spawn(async move |owner: WeakEntity<ModelSelectorOwner>, cx| {
                let result = cx.background_spawn(async move {
                    let read = client.read_provider_collection(pioneer_client::providers::store::ProviderCollectionKey::catalog(workspace), true); drop(client);
                    read?.wait_while(|| demand.strong_count() > 0)?.catalog_response()
                }).await;
                let _ = owner.update(cx, |owner, cx| {
                    if owner.closed { return; }
                    let Some(selector) = owner.selector.upgrade() else { return; };
                    match result { Ok(response) => selector.borrow_mut().apply_provider_list_success(response), Err(error) => selector.borrow_mut().apply_provider_list_error(format!("{error:#}")) }
                    owner.refresh(cx);
                });
            }));
        });
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

    fn spawn_fetch_models_for_provider(cx: &mut App, state: ModelSelectorDialogState, provider_name: String) {
        let client = state.client.clone(); let workspace = state.workspace_id.clone(); let mode = state.mode;
        let effort = state.selected_reasoning_effort.clone(); let layouts = state.model_row_layout_cache.clone();
        state.owner.update(cx, |owner, cx| {
            if owner.closed { return; }
            let token = std::sync::Arc::new(()); let demand = std::sync::Arc::downgrade(&token); owner.model_demand = Some(token);
            owner.models = Some(cx.spawn(async move |owner: WeakEntity<ModelSelectorOwner>, cx| {
                let provider = provider_name.clone();
                let result = cx.background_spawn(async move {
                    use pioneer_client::providers::store::{ProviderCollectionKey, ProviderModelKind};
                    let purpose = match api_provider_model_list_kind(mode) { ApiProviderModelListKind::Chat => ProviderModelKind::Chat, ApiProviderModelListKind::Embeddings => ProviderModelKind::Embeddings, ApiProviderModelListKind::Transcription => ProviderModelKind::Transcription };
                    let read = client.read_provider_collection(ProviderCollectionKey::models(workspace, provider, purpose), true); drop(client);
                    read?.wait_while(|| demand.strong_count() > 0)?.models_response()
                }).await;
                let _ = owner.update(cx, |owner, cx| {
                    if owner.closed { return; }
                    let Some(selector) = owner.selector.upgrade() else { return; };
                    match result {
                        Ok(response) => { if selector.borrow_mut().apply_provider_models_success(response) {
                            layouts.borrow_mut().clear();
                            let selector = selector.borrow();
                            let selected = selector.models().iter().find(|model| Some(model.id.as_str()) == selector.selected_model());
                            if !selected.is_some_and(|model| provider_presentation::reasoning_effort_rows_for_model(model, effort.borrow().as_deref()).iter().any(|row| row.selected)) { *effort.borrow_mut() = None; }
                        } },
                        Err(error) => { selector.borrow_mut().apply_provider_models_error(&provider_name, format!("{error:#}")); }
                    }
                    owner.refresh(cx);
                });
            }));
        });
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
                .on_close({ let owner = state.owner.clone(); move |_, _, cx| owner.update(cx, |owner, _| owner.close()) })
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

    fn save_model_selector_selection(
        state: ModelSelectorDialogState,
    ) -> Rc<dyn Fn(&mut App) -> bool> {
        Rc::new(move |cx| {
            let (provider, model) = state.selector.borrow().selection_parts();
            let selected_reasoning_effort = state.selected_reasoning_effort.borrow().clone();
            if state.client.navigation_snapshot().workspace_id() != Some(state.workspace_id.as_str()) { return false; }
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
            }).unwrap_or(false)
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
        if matches!(state.mode, ProviderModelSelectorMode::Transcription) {
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
                        .on_mouse_down(gpui_kit::MouseButton::Left, |_, window, _| {
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

        state.owner.update(cx, |owner, cx| owner.refresh(cx));
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
                .on_mouse_down(gpui_kit::MouseButton::Left, |_, window, _| {
                    window.prevent_default();
                })
                .on_click(move |_, window, cx| {
                    Self::clear_dialog_reasoning_effort(&default_state);
                    let _ = default_popover_entity.update(cx, |popover, cx| {
                        popover.dismiss(window, cx);
                    });
                    default_state.owner.update(cx, |owner, cx| owner.refresh(cx));
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
                    .on_mouse_down(gpui_kit::MouseButton::Left, |_, window, _| {
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
                        row_state.owner.update(cx, |owner, cx| owner.refresh(cx));
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
                    state.owner.clone(),
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
    ) -> Rc<Vec<gpui_kit::Size<Pixels>>> {
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
                        return gpui_kit::size(
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

                    gpui_kit::size(px(0.), measured_height)
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
        let id: SharedString = format!("model-selector:{}:{}:{}:{model_id}:row", state.owner.entity_id(), state.workspace_id, model.provider).into();

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
            .on_mouse_down(gpui_kit::MouseButton::Left, |_, window, _| {
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
                state.owner.update(cx, |owner, cx| owner.refresh(cx));
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
            .split("fn save_model_selector_selection")
            .next()
            .expect("dialog render has a boundary");

        assert!(!render.contains("desktop_entity"));
        assert!(!render.contains(".read(cx)"));
    }


}
