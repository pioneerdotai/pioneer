use crate::buttons::{default_outline_button, default_primary_button};
use gpui_kit::component::{
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
use pioneer_client::composer::model_picker::ProviderModelInfo;
use pioneer_client::{
    composer::model_picker::{ComposerModelPickerIntent, ComposerModelPickerPublication},
    composer::store::ComposerOperationIdentity,
    core::{ClientCore, ClientPublicationReference, ClientScope},
    providers::{list::ProviderModelSelectorMode, presentation as provider_presentation},
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};
const MODEL_ROW_MIN_HEIGHT: f32 = 32.0;
const MODEL_LIST_MAX_HEIGHT: f32 = 260.0;
const SELECTOR_POPOVER_FALLBACK_WIDTH: f32 = 380.0;
struct ModelPickerBinding {
    scope: ClientScope,
    sequence: Cell<u64>,
    input: RefCell<Option<Arc<ComposerModelPickerPublication>>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl ClientPublicationSink for ModelPickerBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        if publication.scope() != &self.scope
            || publication.snapshot().sequence().get() <= self.sequence.get()
        {
            return;
        }
        self.sequence.set(publication.snapshot().sequence().get());
        let next = publication
            .typed::<ComposerModelPickerPublication>()
            .map(|p| p.payload());
        if self
            .input
            .borrow()
            .as_ref()
            .zip(next.as_ref())
            .is_some_and(|(old, next)| old.revision > next.revision)
        {
            return;
        }
        *self.input.borrow_mut() = next;
        self.changed.send_modify(|v| *v = v.saturating_add(1));
    }
}
#[derive(Clone)]
struct ModelSelectorDialogState {
    owner: WeakEntity<ComposerModelPickerView>,
    input: Arc<ComposerModelPickerPublication>,
    mode: ProviderModelSelectorMode,
    provider_search_input: Entity<InputState>,
    model_search_input: Entity<InputState>,
    provider_scroll_handle: ScrollHandle,
    model_scroll_handle: VirtualListScrollHandle,
    provider_trigger_width_px: Rc<RefCell<f32>>,
    model_trigger_width_px: Rc<RefCell<f32>>,
    reasoning_popover: ChoicePopover,
    service_popover: ChoicePopover,
    context_popover: ChoicePopover,
}
impl ModelSelectorDialogState {
    fn send(&self, intent: ComposerModelPickerIntent, cx: &mut App) {
        let _ = self.owner.update(cx, |view, cx| view.send(intent, cx));
    }
}
pub(crate) struct ComposerModelPickerView {
    client: Arc<ClientCore>,
    identity: ComposerOperationIdentity,
    binding: Arc<ModelPickerBinding>,
    registration: Option<ClientBindingRegistration>,
    task: Option<Task<()>>,
    _release: Subscription,
    dialog_open: bool,
    state: ModelSelectorDialogState,
}
impl ComposerModelPickerView {
    pub(crate) fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        input: Arc<ComposerModelPickerPublication>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let identity = input.identity.clone();
        let scope = ClientScope::ComposerModelPicker {
            thread_id: identity.thread_id.clone(),
        };
        let binding = Arc::new(ModelPickerBinding {
            scope: scope.clone(),
            sequence: Cell::new(0),
            input: RefCell::new(Some(input.clone())),
            changed: tokio::sync::watch::channel(0).0,
        });
        let mut changes = binding.changed.subscribe();
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        let registration = registrar.register(scope, Arc::downgrade(&sink));
        let task = cx.spawn_in(window, async move |view, cx| {
            while changes.changed().await.is_ok() {
                let _ = *changes.borrow_and_update();
                if view
                    .update_in(cx, |view, window, cx| {
                        let input = view.binding.input.borrow().clone();
                        match input {
                            Some(input) if input.identity == view.identity && !input.closed => {
                                view.state.input = input;
                            }
                            _ => view.close(window, cx),
                        }
                        cx.notify();
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let release = cx.on_release_in(window, |view, window, cx| view.close(window, cx));
        let state = ModelSelectorDialogState {
            owner: cx.weak_entity(),
            input,
            mode: ProviderModelSelectorMode::Chat,
            provider_search_input: cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(t!("chat.composer.model.provider_placeholder").to_string())
            }),
            model_search_input: cx.new(|cx| {
                InputState::new(window, cx)
                    .placeholder(t!("chat.composer.model.model_placeholder").to_string())
            }),
            provider_scroll_handle: ScrollHandle::new(),
            model_scroll_handle: VirtualListScrollHandle::new(),
            provider_trigger_width_px: Rc::new(RefCell::new(SELECTOR_POPOVER_FALLBACK_WIDTH)),
            model_trigger_width_px: Rc::new(RefCell::new(SELECTOR_POPOVER_FALLBACK_WIDTH)),
            reasoning_popover: ChoicePopover::new("reasoning-effort"),
            service_popover: ChoicePopover::new("service-tier"),
            context_popover: ChoicePopover::new("context-window"),
        };
        Self {
            client,
            identity,
            binding,
            registration: Some(registration),
            task: Some(task),
            _release: release,
            dialog_open: false,
            state,
        }
    }
    fn send(&mut self, intent: ComposerModelPickerIntent, cx: &mut Context<Self>) {
        self.client.composer_model_picker_intent(intent);
        if let Some(input) = self
            .client
            .composer_model_picker_snapshot(&self.identity.thread_id)
            .filter(|p| p.identity == self.identity)
        {
            *self.binding.input.borrow_mut() = Some(input.clone());
            self.state.input = input;
        }
        cx.notify();
    }
    fn retire(&mut self) {
        self.client
            .composer_model_picker_intent(ComposerModelPickerIntent::Close {
                identity: self.identity.clone(),
            });
        self.registration.take();
        self.task.take();
    }
    pub(crate) fn close(&mut self, window: &mut Window, cx: &mut App) {
        self.retire();
        if std::mem::take(&mut self.dialog_open) {
            window.close_dialog(cx);
        }
    }
    fn commit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.send(
            ComposerModelPickerIntent::Commit {
                identity: self.identity.clone(),
            },
            cx,
        );
        self.close(window, cx);
    }
    pub(crate) fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.dialog_open = true;
        let owner = cx.weak_entity();
        window.open_dialog(cx, move |dialog, _, cx| {
            let Some(view) = owner.upgrade() else {
                return dialog;
            };
            let state = view.read(cx).state.clone();
            let cancel = owner.clone();
            let save = owner.clone();
            let ok = owner.clone();
            let closed = owner.clone();
            let provider_trigger_label = Self::provider_trigger_label(&state);
            let model_trigger_label = Self::model_trigger_label(&state);
            let model_trigger_loading = Self::model_trigger_loading(&state);
            dialog
                .gap_1()
                .rounded_2xl()
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("chat.composer.model.dialog_title").to_string()),
                )
                .on_close(move |_, _, cx| {
                    let _ = closed.update(cx, |view, _| {
                        view.dialog_open = false;
                        view.retire();
                    });
                })
                .on_ok(move |_, _, cx| {
                    let _ = ok.update(cx, |view, cx| {
                        view.send(
                            ComposerModelPickerIntent::Commit {
                                identity: view.identity.clone(),
                            },
                            cx,
                        );
                        view.dialog_open = false;
                        view.retire();
                    });
                    true
                })
                .footer(DialogFooter::new().children(vec![
                        default_outline_button("model-selector-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .outline()
                            .on_click(move |_, window, cx| {
                                let _ = cancel.update(cx, |view, cx| view.close(window, cx));
                            })
                            .into_any_element(),
                        default_primary_button("model-selector-save")
                            .label(t!("buttons.save").to_string())
                            .on_click(move |_, window, cx| {
                                let _ = save.update(cx, |view, cx| view.commit(window, cx));
                            })
                            .into_any_element(),
                    ]))
                .child(v_flex().w_full().pt_4().pb_5().child({
                    let mut form = v_form()
                        .child(Self::render_provider_selector_section(
                            state.clone(),
                            provider_trigger_label,
                        ))
                        .child(Self::render_model_selector_section(
                            state.clone(),
                            model_trigger_label,
                            model_trigger_loading,
                        ));
                    if let Some(reasoning) = Self::render_reasoning_effort_selector_section(&state)
                    {
                        form = form.child(reasoning);
                    }
                    if let Some(service) = Self::render_service_selector_section(&state) {
                        form = form.child(service);
                    }
                    if let Some(context) = Self::render_context_selector_section(&state) {
                        form = form.child(context);
                    }
                    form
                }))
        });
    }
    fn provider_trigger_label(state: &ModelSelectorDialogState) -> String {
        state
            .input
            .selector
            .selected_provider_label()
            .unwrap_or_else(|| t!("chat.composer.model.provider_placeholder").to_string())
    }

    fn model_trigger_label(state: &ModelSelectorDialogState) -> String {
        let selector = &state.input.selector;
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
        let selector = &state.input.selector;
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
        state.input.reasoning_rows.clone()
    }

    fn render_provider_selector_section(
        state: ModelSelectorDialogState,
        provider_trigger_label: String,
    ) -> Field {
        let provider_trigger_width_px = state.provider_trigger_width_px.clone();
        let owner = state.owner.clone();
        field()
            .label(t!("chat.composer.model.provider_label").to_string())
            .child(
                div()
                    .w_full()
                    .relative()
                    .child(
                        Popover::new("model-provider-popover")
                            .trigger_style(StyleRefinement::default().w_full())
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
                                    let _ = owner.update(cx, |_view, cx| {
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
            let selector = &state.input.selector;
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
        state.send(
            ComposerModelPickerIntent::SelectProvider {
                identity: state.input.identity.clone(),
                provider: provider_name,
            },
            cx,
        );
        popover_entity.update(cx, |state, cx| state.dismiss(window, cx));
    }

    fn render_model_selector_section(
        state: ModelSelectorDialogState,
        model_trigger_label: String,
        model_trigger_loading: bool,
    ) -> Field {
        let model_trigger_width_px = state.model_trigger_width_px.clone();
        let owner = state.owner.clone();
        field()
            .label(t!("chat.composer.model.model_label").to_string())
            .child(
                div()
                    .w_full()
                    .relative()
                    .child(
                        Popover::new("model-model-popover")
                            .trigger_style(StyleRefinement::default().w_full())
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
                                    let _ = owner.update(cx, |_view, cx| {
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

    fn render_service_selector_section(state: &ModelSelectorDialogState) -> Option<Field> {
        let rows = &state.input.service_rows;
        if rows.is_empty() {
            return None;
        }
        let configured = state.input.selected_service_mode.as_ref();
        let invalid = (configured.is_some() && !rows.iter().any(|row| row.selected))
            || rows.iter().any(|row| row.selected && !row.available);
        let choices = rows
            .iter()
            .map(|row| ChoicePopoverRow {
                id: format!("service-mode-{}", row.id).into(),
                label: if row.id == "standard" {
                    t!("chat.composer.model.service_standard").to_string()
                } else {
                    row.label.clone()
                },
                selected: row.selected,
                available: row.available,
                intent: ComposerModelPickerIntent::SelectServiceMode {
                    identity: state.input.identity.clone(),
                    id: row.id.clone(),
                },
            })
            .collect();
        Some(
            field()
                .label(t!("chat.composer.model.service_label").to_string())
                .child(state.service_popover.render(
                    state.owner.clone(),
                    configured.map(|mode| mode.id.clone()),
                    choices,
                ))
                .when(invalid, |field| {
                    field.child(
                        div()
                            .text_sm()
                            .child(t!("chat.composer.model.service_invalid").to_string()),
                    )
                }),
        )
    }

    fn render_context_selector_section(state: &ModelSelectorDialogState) -> Option<Field> {
        let rows = &state.input.context_rows;
        if rows.is_empty() {
            return None;
        }
        let configured = state.input.selected_context_window;
        let invalid = configured.is_some() && !rows.iter().any(|row| row.selected);
        let choices = rows
            .iter()
            .map(|row| ChoicePopoverRow {
                id: format!(
                    "context-window-{}",
                    row.window.map_or_else(|| "auto".into(), |v| v.to_string())
                )
                .into(),
                label: Self::context_window_label(row.window.is_none(), row.tokens),
                selected: row.selected,
                available: true,
                intent: ComposerModelPickerIntent::SelectContextWindow {
                    identity: state.input.identity.clone(),
                    window: row.window,
                },
            })
            .collect();
        Some(
            field()
                .label(t!("chat.composer.model.context_label").to_string())
                .child(state.context_popover.render(
                    state.owner.clone(),
                    configured.map(|tokens| Self::context_window_label(false, tokens)),
                    choices,
                ))
                .when(invalid, |field| {
                    field.child(
                        div()
                            .text_sm()
                            .child(t!("chat.composer.model.context_invalid").to_string()),
                    )
                }),
        )
    }

    fn context_window_label(auto: bool, tokens: u64) -> String {
        if auto {
            t!(
                "chat.composer.model.context_auto",
                tokens = tokens.to_string()
            )
            .to_string()
        } else {
            t!(
                "chat.composer.model.context_up_to",
                tokens = tokens.to_string()
            )
            .to_string()
        }
    }

    fn render_reasoning_effort_selector_section(state: &ModelSelectorDialogState) -> Option<Field> {
        if state.input.default_reasoning_only {
            let owner = state.owner.clone();
            let identity = state.input.identity.clone();
            return Some(
                field()
                    .label(t!("chat.composer.model.reasoning_label").to_string())
                    .child(
                        default_outline_button("service-default-reasoning")
                            .label(t!("chat.composer.model.reasoning_default").to_string())
                            .selected(state.input.selected_reasoning_effort.is_none())
                            .on_click(move |_, _, cx| {
                                let _ = owner.update(cx, |view, cx| {
                                    view.send(
                                        ComposerModelPickerIntent::SelectReasoningEffort {
                                            identity: identity.clone(),
                                            effort: None,
                                        },
                                        cx,
                                    );
                                });
                            }),
                    ),
            );
        }
        let rows = Self::reasoning_effort_rows(state);
        if rows.is_empty() {
            return None;
        }

        let choices = std::iter::once(ChoicePopoverRow {
            id: "reasoning-effort-opt-default".into(),
            label: t!("chat.composer.model.reasoning_default").to_string(),
            selected: state.input.selected_reasoning_effort.is_none(),
            available: true,
            intent: ComposerModelPickerIntent::SelectReasoningEffort {
                identity: state.input.identity.clone(),
                effort: None,
            },
        })
        .chain(rows.into_iter().map(|row| ChoicePopoverRow {
            id: format!("reasoning-effort-opt-{}", row.effort).into(),
            label: row.label,
            selected: row.selected,
            available: true,
            intent: ComposerModelPickerIntent::SelectReasoningEffort {
                identity: state.input.identity.clone(),
                effort: Some(row.effort),
            },
        }))
        .collect();

        Some(
            field()
                .label(t!("chat.composer.model.reasoning_label").to_string())
                .child(state.reasoning_popover.render(
                    state.owner.clone(),
                    state.input.selected_reasoning_effort.clone(),
                    choices,
                )),
        )
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
            let selector = &state.input.selector;
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
        let Some(owner) = state.owner.upgrade() else {
            return div().into_any_element();
        };

        div()
            .h(px(visible_height))
            .relative()
            .overflow_hidden()
            .child(
                v_virtual_list(
                    owner,
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
                    gpui_kit::size(px(0.), measured_height)
                })
                .collect::<Vec<_>>(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn render_model_virtual_list_row(
        state: ModelSelectorDialogState,
        popover_entity: Entity<PopoverState>,
        _ix: usize,
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
        let is_active = state.input.selector.selected_model() == Some(model_id.as_str());
        let id: SharedString = format!("model-vl-{}:{}", model.provider, model.id).into();

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
                state.send(
                    ComposerModelPickerIntent::SelectModel {
                        identity: state.input.identity.clone(),
                        model: model_id.clone(),
                    },
                    cx,
                );
                let _ = popover_entity.update(cx, |popover, cx| {
                    popover.dismiss(window, cx);
                });
                let _ = state.owner.update(cx, |_, cx| cx.notify());
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

impl Drop for ComposerModelPickerView {
    fn drop(&mut self) {
        self.retire();
    }
}
struct ChoicePopoverRow {
    id: SharedString,
    label: String,
    selected: bool,
    available: bool,
    intent: ComposerModelPickerIntent,
}

#[derive(Clone)]
struct ChoicePopover {
    id: &'static str,
    trigger_width_px: Rc<RefCell<f32>>,
    scroll_handle: ScrollHandle,
}

impl ChoicePopover {
    fn new(id: &'static str) -> Self {
        Self {
            id,
            trigger_width_px: Rc::new(RefCell::new(SELECTOR_POPOVER_FALLBACK_WIDTH)),
            scroll_handle: ScrollHandle::new(),
        }
    }

    fn render(
        &self,
        owner: WeakEntity<ComposerModelPickerView>,
        configured_label: Option<String>,
        rows: Vec<ChoicePopoverRow>,
    ) -> Div {
        let trigger_label = rows
            .iter()
            .find(|row| row.selected)
            .map(|row| row.label.clone())
            .or(configured_label)
            .or_else(|| rows.first().map(|row| row.label.clone()))
            .unwrap_or_default();
        let id = self.id;
        let layout = self.clone();
        let trigger_width_px = self.trigger_width_px.clone();
        let content_owner = owner.clone();

        div()
            .w_full()
            .relative()
            .child(
                Popover::new(SharedString::from(format!("model-{id}-popover")))
                    .trigger_style(StyleRefinement::default().w_full())
                    .anchor(Anchor::TopLeft)
                    .p_0()
                    .trigger(SelectorPopoverTrigger::new(
                        SharedString::from(format!("model-{id}-trigger")),
                        trigger_label,
                        IconName::ChevronsUpDown,
                    ))
                    .content(move |_, _, cx| layout.render_content(&content_owner, &rows, cx)),
            )
            .child(
                canvas(
                    move |bounds, _, cx| {
                        let measured_width = bounds.size.width.max(px(1.)).as_f32();
                        let mut cached_width = trigger_width_px.borrow_mut();
                        if (measured_width - *cached_width).abs() > 1.0 {
                            *cached_width = measured_width;
                            let _ = owner.update(cx, |_, cx| cx.notify());
                        }
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full(),
            )
    }

    fn render_content(
        &self,
        owner: &WeakEntity<ComposerModelPickerView>,
        rows: &[ChoicePopoverRow],
        cx: &mut Context<PopoverState>,
    ) -> AnyElement {
        let popover = cx.entity();
        let theme = cx.theme();
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

        let list = v_flex()
            .id(SharedString::from(format!("{}-popover-list", self.id)))
            .max_h(px(200.))
            .overflow_y_scroll()
            .track_scroll(&self.scroll_handle)
            .children(rows.iter().map(|row| {
                div()
                    .id(row.id.clone())
                    .w_full()
                    .px_2()
                    .py_1p5()
                    .text_sm()
                    .text_color(if row.available {
                        theme.foreground
                    } else {
                        theme.muted_foreground
                    })
                    .when(row.selected, |d| d.bg(theme.muted))
                    .when(row.available, |d| {
                        let owner = owner.clone();
                        let popover = popover.clone();
                        let intent = row.intent.clone();
                        d.cursor_pointer()
                            .hover(move |d| d.bg(ghost_hover))
                            .active(move |d| d.bg(ghost_active))
                            .on_mouse_down(gpui_kit::MouseButton::Left, |_, window, _| {
                                window.prevent_default();
                            })
                            .on_click(move |_, window, cx| {
                                let _ = owner.update(cx, |view, cx| view.send(intent.clone(), cx));
                                popover.update(cx, |popover, cx| popover.dismiss(window, cx));
                            })
                    })
                    .child(row.label.clone())
            }));

        v_flex()
            .w(px(*self.trigger_width_px.borrow()))
            .child(
                div().relative().child(list).child(
                    div()
                        .absolute()
                        .top_0()
                        .left_0()
                        .right_0()
                        .bottom_0()
                        .child(Scrollbar::vertical(&self.scroll_handle)),
                ),
            )
            .into_any_element()
    }
}

#[derive(gpui_kit::IntoElement)]
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
                            pioneer_client::timeline::diagnostics::AnimationSourceId::SharedModelSelector,
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

impl Render for ComposerModelPickerView {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div()
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::ComposerModelPickerView;
    use gpui_kit::component::Root;
    use gpui_kit::{
        AppContext, Context, FocusHandle, InteractiveElement, IntoElement, Render, TestAppContext,
        Window, div,
    };
    use pioneer_client::{
        composer::{
            model_picker::ComposerModelPickerPublication,
            store::{ComposerIntent, ComposerOperationIdentity},
        },
        core::ClientCore,
        providers::list::ProviderModelSelectorState,
    };
    use std::sync::Arc;
    struct Host {
        focus: FocusHandle,
    }
    impl Render for Host {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().track_focus(&self.focus)
        }
    }
    #[gpui_kit::test]
    fn model_picker_drop_releases_controls_binding_and_dialog_and_restores_focus(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let (root, cx) = cx.add_window_view(|window, cx| {
            let host = cx.new(|cx| Host {
                focus: cx.focus_handle(),
            });
            Root::new(host, window, cx)
        });
        let core = Arc::new(ClientCore::new());
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: Default::default(),
        });
        let draft = core.composer_snapshot("a").unwrap();
        let (registrar, deliver) = crate::test_support::binding_router(core.clone());
        let (weak, provider_input, model_input, trigger) = cx.update(|window, cx| {
            let trigger = root
                .read(cx)
                .view()
                .clone()
                .downcast::<Host>()
                .unwrap()
                .read(cx)
                .focus
                .clone();
            trigger.focus(window, cx);
            let input = Arc::new(ComposerModelPickerPublication {
                context_rows: vec![],
                service_rows: vec![],
                default_reasoning_only: false,
                identity: ComposerOperationIdentity {
                    thread_id: "a".into(),
                    draft_id: draft.draft_id(),
                    generation: 1,
                },
                revision: 1,
                selector: ProviderModelSelectorState::new(None, None),
                selected_reasoning_effort: None,
                selected_context_window: None,
                selected_service_mode: None,
                selected_context_authority: None,
                deferred: true,
                closed: false,
                providers_request: Default::default(),
                models_request: Default::default(),
                provider_rows: vec![],
                reasoning_rows: vec![],
                selected_provider_ready: true,
            });
            let picker = cx
                .new(|cx| ComposerModelPickerView::new(core.clone(), registrar, input, window, cx));
            let weak = picker.downgrade();
            let (provider_input, model_input) = picker.update(cx, |view, cx| {
                view.open(window, cx);
                view.state.provider_search_input.update(cx, |input, cx| {
                    input.set_value("filter", window, cx);
                    input.focus(window, cx);
                });
                (
                    view.state.provider_search_input.downgrade(),
                    view.state.model_search_input.downgrade(),
                )
            });
            deliver();
            assert!(Arc::ptr_eq(&draft, &core.composer_snapshot("a").unwrap()));
            drop(picker);
            (weak, provider_input, model_input, trigger)
        });
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
        assert!(provider_input.upgrade().is_none());
        assert!(model_input.upgrade().is_none());
        cx.update(|window, cx| assert_eq!(window.focused(cx), Some(trigger)));
    }
}
