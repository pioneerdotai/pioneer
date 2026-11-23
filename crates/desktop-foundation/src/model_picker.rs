//! Shared controls. Catalog sessions and persistence belong to the caller.
use gpui_kit::component::{
    form::{field, v_form},
    input::{Input, InputState},
    popover::{Popover, PopoverState},
    scroll::Scrollbar,
    separator::Separator,
    spinner::Spinner,
    theme::ActiveTheme,
    *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    composer::model_picker::ProviderModelInfo,
    providers::{
        list::{
            ProviderModelSelectorMode, ProviderModelSelectorState,
            runtime_id_from_cli_runtime_provider_key,
        },
        presentation as present,
    },
};
use std::{cell::Cell, rc::Rc};

const FALLBACK_WIDTH: f32 = 380.0;
const MODEL_ROW_HEIGHT: f32 = 32.0;
const MODEL_LIST_HEIGHT: f32 = 260.0;

pub type ApiProviderTitle = fn(&str) -> Option<String>;
type Translate = fn(&str) -> String;
type OnChange = Rc<dyn Fn(ModelPickerChange, &mut App)>;

#[derive(Clone)]
pub enum ModelPickerChange {
    Provider(String),
    Model(String),
    ReasoningEffort(Option<String>),
    ServiceMode(String),
    ContextWindow(Option<u64>),
}

pub struct ModelPickerInput {
    pub selector: ProviderModelSelectorState,
    pub reasoning_rows: Vec<present::ReasoningEffortRow>,
    pub service_rows: Vec<present::ServiceModeRow>,
    pub context_rows: Vec<present::ContextWindowRow>,
    pub selected_reasoning_effort: Option<String>,
    pub selected_service_mode_id: Option<String>,
    pub selected_context_window: Option<u64>,
    pub default_reasoning_only: bool,
}

#[derive(Clone)]
pub struct ModelPicker {
    mode: ProviderModelSelectorMode,
    fixed_provider: bool,
    api_title: Option<ApiProviderTitle>,
    text: Translate,
    provider_search: Entity<InputState>,
    model_search: Entity<InputState>,
    provider: ChoicePopover,
    model: ChoicePopover,
    reasoning: ChoicePopover,
    service: ChoicePopover,
    context: ChoicePopover,
    model_scroll: VirtualListScrollHandle,
}

impl ModelPicker {
    pub fn new(
        mode: ProviderModelSelectorMode,
        fixed_provider: bool,
        api_title: Option<ApiProviderTitle>,
        text: Translate,
        window: &mut Window,
        cx: &mut App,
    ) -> Self {
        Self {
            mode,
            fixed_provider,
            api_title,
            text,
            provider_search: cx
                .new(|cx| InputState::new(window, cx).placeholder(text("provider_placeholder"))),
            model_search: cx
                .new(|cx| InputState::new(window, cx).placeholder(text("model_placeholder"))),
            provider: ChoicePopover::new("provider"),
            model: ChoicePopover::new("model"),
            reasoning: ChoicePopover::new("reasoning-effort"),
            service: ChoicePopover::new("service-tier"),
            context: ChoicePopover::new("context-window"),
            model_scroll: VirtualListScrollHandle::new(),
        }
    }

    pub fn search_inputs(&self) -> (&Entity<InputState>, &Entity<InputState>) {
        (&self.provider_search, &self.model_search)
    }

    pub fn render(
        &self,
        input: ModelPickerInput,
        on_change: impl Fn(ModelPickerChange, &mut App) + 'static,
    ) -> Div {
        let on_change: OnChange = Rc::new(on_change);
        let selector = &input.selector;
        let text = self.text;
        let provider = self.provider_control(selector, on_change.clone());

        let display = present::model_selector_selected_model_display_state(
            selector.selected_model(),
            selector.models(),
            selector.loading_models(),
        );
        let loading = matches!(&display, present::ProviderModelDisplayState::Loading);
        let model_title = match display {
            present::ProviderModelDisplayState::Label(label) => label,
            _ => text("model_placeholder"),
        };
        let change = on_change.clone();
        let controls = self.clone();
        let selected = selector.selected_model().map(str::to_owned);
        let models = selector.models().to_vec();
        let loading_models = selector.loading_models();
        let error = selector.error().map(str::to_owned);
        let model = self.model.render(model_title, loading, move |window, cx| {
            let search = &controls.model_search;
            let query = search.read(cx).value().to_owned();
            let rows = present::filter_model_selector_models(&models, &query);
            search_panel(search)
                .child(if loading_models {
                    message(text("loading_models"), cx)
                } else if let Some(error) = &error {
                    message(text("load_error").replace("%{error}", error), cx)
                } else if rows.is_empty() {
                    message(text("no_models"), cx)
                } else {
                    controls.model_rows(rows, selected.clone(), change.clone(), window, cx)
                })
                .into_any_element()
        });

        let mut form = v_form()
            .child(field().label(text("provider_label")).child(provider))
            .child(field().label(text("model_label")).child(model));

        if input.default_reasoning_only || !input.reasoning_rows.is_empty() {
            let default = text("reasoning_default");
            let mut rows = vec![Choice {
                label: default,
                selected: input.selected_reasoning_effort.is_none(),
                available: true,
                change: ModelPickerChange::ReasoningEffort(None),
            }];
            if !input.default_reasoning_only {
                rows.extend(input.reasoning_rows.into_iter().map(|row| Choice {
                    label: row.label,
                    selected: row.selected,
                    available: true,
                    change: ModelPickerChange::ReasoningEffort(Some(row.effort)),
                }));
            }
            form = form.child(
                field()
                    .label(text("reasoning_label"))
                    .child(self.reasoning.choices(
                        rows,
                        input.selected_reasoning_effort,
                        on_change.clone(),
                    )),
            );
        }
        if !input.service_rows.is_empty() {
            let configured = input.selected_service_mode_id;
            let invalid = (configured.is_some()
                && !input.service_rows.iter().any(|row| row.selected))
                || input
                    .service_rows
                    .iter()
                    .any(|row| row.selected && !row.available);
            let rows = input
                .service_rows
                .into_iter()
                .map(|row| Choice {
                    label: if row.id == "standard" {
                        text("service_standard")
                    } else {
                        row.label
                    },
                    selected: row.selected,
                    available: row.available,
                    change: ModelPickerChange::ServiceMode(row.id),
                })
                .collect();
            form = form.child(
                field()
                    .label(text("service_label"))
                    .child(self.service.choices(rows, configured, on_change.clone()))
                    .when(invalid, |field| {
                        field.child(div().text_sm().child(text("service_invalid")))
                    }),
            );
        }
        if !input.context_rows.is_empty() {
            let configured = input.selected_context_window;
            let invalid =
                configured.is_some() && !input.context_rows.iter().any(|row| row.selected);
            let label = |auto, tokens: u64| {
                text(if auto {
                    "context_auto"
                } else {
                    "context_up_to"
                })
                .replace("%{tokens}", &tokens.to_string())
            };
            let rows = input
                .context_rows
                .into_iter()
                .map(|row| Choice {
                    label: label(row.window.is_none(), row.tokens),
                    selected: row.selected,
                    available: true,
                    change: ModelPickerChange::ContextWindow(row.window),
                })
                .collect();
            form = form.child(
                field()
                    .label(text("context_label"))
                    .child(self.context.choices(
                        rows,
                        configured.map(|tokens| label(false, tokens)),
                        on_change,
                    ))
                    .when(invalid, |field| {
                        field.child(div().text_sm().child(text("context_invalid")))
                    }),
            );
        }
        v_flex().w_full().pt_4().pb_5().child(form)
    }

    pub fn render_provider(
        &self,
        selector: &ProviderModelSelectorState,
        on_change: impl Fn(ModelPickerChange, &mut App) + 'static,
    ) -> Div {
        v_flex().w_full().pt_4().pb_5().gap_4().child(
            v_form().child(
                field()
                    .label((self.text)("provider_label"))
                    .child(self.provider_control(selector, Rc::new(on_change))),
            ),
        )
    }

    fn provider_control(
        &self,
        selector: &ProviderModelSelectorState,
        on_change: OnChange,
    ) -> AnyElement {
        let text = self.text;
        let provider_title = selector
            .selected_provider()
            .and_then(|id| {
                api_title(self.api_title, id).or_else(|| selector.selected_provider_label())
            })
            .unwrap_or_else(|| text("provider_placeholder"));
        if self.fixed_provider {
            div().text_sm().child(provider_title).into_any_element()
        } else {
            let selected = selector.selected_provider().map(str::to_owned);
            let mut rows = selector.provider_rows();
            for row in &mut rows {
                if let Some(title) = api_title(self.api_title, &row.id) {
                    row.label = title;
                }
            }
            let loading = selector.loading_providers();
            let search = self.provider_search.clone();
            let list = self.provider.clone();
            let change = on_change.clone();
            self.provider
                .render(provider_title, false, move |_, cx| {
                    let query = search.read(cx).value().trim().to_lowercase();
                    let choices = rows
                        .iter()
                        .filter(|row| {
                            row.id.to_lowercase().contains(query.as_str())
                                || row.label.to_lowercase().contains(query.as_str())
                        })
                        .map(|row| Choice {
                            label: row.label.clone(),
                            selected: selected.as_deref() == Some(row.id.as_str()),
                            available: true,
                            change: ModelPickerChange::Provider(row.id.clone()),
                        })
                        .collect();
                    search_panel(&search)
                        .child(if loading {
                            message(text("loading_providers"), cx)
                        } else {
                            list.rows(choices, Some(text("no_providers")), &change, cx)
                        })
                        .into_any_element()
                })
                .into_any_element()
        }
    }

    fn model_rows(
        &self,
        models: Vec<ProviderModelInfo>,
        selected: Option<String>,
        on_change: OnChange,
        window: &mut Window,
        cx: &mut Context<PopoverState>,
    ) -> AnyElement {
        let popover = cx.entity();
        let colors = RowColors::new(cx);
        let text = self.text;
        let transcription = self.mode == ProviderModelSelectorMode::Transcription;
        let row = move |index, model: &ProviderModelInfo| {
            let from_model = transcription
                .then(|| present::transcription_model_selector_presentation(model))
                .flatten();
            let id = format!("model-option-{}-{}-{index}", model.provider, model.id);
            option_row(
                id,
                selected.as_deref() == Some(model.id.as_str()),
                Some(ModelPickerChange::Model(model.id.clone())),
                on_change.clone(),
                popover.clone(),
                colors,
            )
            .child(
                v_flex()
                    .w_full()
                    .min_w_0()
                    .child(
                        h_flex()
                            .gap_2()
                            .min_w_0()
                            .child(
                                div()
                                    .min_w_0()
                                    .whitespace_normal()
                                    .child(present::model_selector_model_display_name(model)),
                            )
                            .when(
                                from_model.as_ref().is_some_and(|model| model.recommended),
                                |row| {
                                    row.child(
                                        div()
                                            .flex_none()
                                            .px_1()
                                            .rounded_md()
                                            .border_1()
                                            .text_xs()
                                            .child(text("recommended")),
                                    )
                                },
                            ),
                    )
                    .when_some(
                        present::model_selector_model_secondary_text(model),
                        |parent, details| {
                            parent.child(
                                div()
                                    .text_xs()
                                    .whitespace_normal()
                                    .opacity(0.6)
                                    .child(details),
                            )
                        },
                    )
                    .when_some(from_model, |parent, details| {
                        parent.child(div().text_xs().whitespace_normal().opacity(0.75).child(
                            format!(
                                "{} | {} | {}",
                                details.engine, details.download_size, details.language_summary
                            ),
                        ))
                    }),
            )
            .into_any_element()
        };
        let width = self.model.width.get();
        let sizes = Rc::new(
            models
                .iter()
                .enumerate()
                .map(|(index, model)| {
                    let height = row(index, model)
                        .layout_as_root(
                            size(
                                AvailableSpace::Definite(px(width)),
                                AvailableSpace::MaxContent,
                            ),
                            window,
                            cx,
                        )
                        .height
                        .max(px(MODEL_ROW_HEIGHT));
                    size(px(0.), height)
                })
                .collect::<Vec<_>>(),
        );
        let height = sizes
            .iter()
            .map(|size| size.height.as_f32())
            .sum::<f32>()
            .min(MODEL_LIST_HEIGHT);
        div()
            .h(px(height))
            .relative()
            .overflow_hidden()
            .child(
                v_virtual_list(
                    self.model_search.clone(),
                    "model-virtual-list",
                    sizes,
                    move |_, range, _, _| {
                        range
                            .filter_map(|index| models.get(index).map(|model| row(index, model)))
                            .collect::<Vec<_>>()
                    },
                )
                .with_sizing_behavior(ListSizingBehavior::Auto)
                .track_scroll(&self.model_scroll),
            )
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .child(Scrollbar::vertical(&self.model_scroll)),
            )
            .into_any_element()
    }
}

fn api_title(lookup: Option<ApiProviderTitle>, id: &str) -> Option<String> {
    if runtime_id_from_cli_runtime_provider_key(id).is_some() {
        return None;
    }
    lookup?(id)
}

struct Choice {
    label: String,
    selected: bool,
    available: bool,
    change: ModelPickerChange,
}

#[derive(Clone)]
struct ChoicePopover {
    id: &'static str,
    width: Rc<Cell<f32>>,
    scroll: ScrollHandle,
}

impl ChoicePopover {
    fn new(id: &'static str) -> Self {
        Self {
            id,
            width: Rc::new(Cell::new(FALLBACK_WIDTH)),
            scroll: ScrollHandle::new(),
        }
    }

    fn choices(&self, rows: Vec<Choice>, configured: Option<String>, change: OnChange) -> Div {
        let label = rows
            .iter()
            .find(|row| row.selected)
            .map(|row| row.label.clone())
            .or(configured)
            .or_else(|| rows.first().map(|row| row.label.clone()))
            .unwrap_or_default();
        let list = self.clone();
        let rows = Rc::new(rows);
        self.render(label, false, move |_, cx| {
            list.rows(
                rows.as_ref()
                    .iter()
                    .map(|row| Choice {
                        label: row.label.clone(),
                        selected: row.selected,
                        available: row.available,
                        change: row.change.clone(),
                    })
                    .collect(),
                None,
                &change,
                cx,
            )
        })
    }

    fn render(
        &self,
        label: String,
        loading: bool,
        content: impl Fn(&mut Window, &mut Context<PopoverState>) -> AnyElement + 'static,
    ) -> Div {
        let width = self.width.clone();
        let measured = width.clone();
        div()
            .w_full()
            .relative()
            .child(
                Popover::new(SharedString::from(format!("picker-{}", self.id)))
                    .trigger_style(StyleRefinement::default().w_full())
                    .anchor(Anchor::TopLeft)
                    .p_0()
                    .trigger(PickerTrigger {
                        label,
                        loading,
                        selected: false,
                    })
                    .content(move |_, window, cx| {
                        div().w(px(width.get())).child(content(window, cx))
                    }),
            )
            .child(
                canvas(
                    move |bounds, window, _| {
                        let width = bounds.size.width.max(px(1.)).as_f32();
                        if (width - measured.get()).abs() > 1.0 {
                            measured.set(width);
                            window.refresh();
                        }
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full(),
            )
    }

    fn rows(
        &self,
        rows: Vec<Choice>,
        empty: Option<String>,
        change: &OnChange,
        cx: &mut Context<PopoverState>,
    ) -> AnyElement {
        if let Some(empty) = empty.filter(|_| rows.is_empty()) {
            return message(empty, cx);
        }
        let colors = RowColors::new(cx);
        let popover = cx.entity();
        div()
            .relative()
            .child(
                v_flex()
                    .id(SharedString::from(format!("picker-{}-rows", self.id)))
                    .max_h(px(200.))
                    .overflow_y_scroll()
                    .track_scroll(&self.scroll)
                    .children(rows.into_iter().enumerate().map(|(index, row)| {
                        option_row(
                            format!("picker-choice-{index}"),
                            row.selected,
                            row.available.then_some(row.change),
                            change.clone(),
                            popover.clone(),
                            colors,
                        )
                        .child(row.label)
                    })),
            )
            .child(
                div()
                    .absolute()
                    .top_0()
                    .left_0()
                    .right_0()
                    .bottom_0()
                    .child(Scrollbar::vertical(&self.scroll)),
            )
            .into_any_element()
    }
}

fn search_panel(search: &Entity<InputState>) -> Div {
    v_flex()
        .w_full()
        .child(
            v_form().child(
                field()
                    .label_indent(false)
                    .child(Input::new(search).appearance(false).px_2().min_w_0()),
            ),
        )
        .child(Separator::horizontal())
}

fn message(label: String, cx: &App) -> AnyElement {
    div()
        .p_4()
        .text_sm()
        .text_color(cx.theme().muted_foreground)
        .child(label)
        .into_any_element()
}

#[derive(Clone, Copy)]
struct RowColors {
    foreground: Hsla,
    selected: Hsla,
    hover: Hsla,
    active: Hsla,
}
impl RowColors {
    fn new(cx: &App) -> Self {
        let theme = cx.theme();
        let secondary = theme.secondary;
        let (hover, active) = if theme.mode.is_dark() {
            (secondary.lighten(0.2), secondary.lighten(0.3))
        } else {
            (secondary.darken(0.1), secondary.darken(0.2))
        };
        Self {
            foreground: theme.foreground,
            selected: theme.muted,
            hover: hover.opacity(0.8),
            active: active.opacity(0.8),
        }
    }
}

fn option_row(
    id: String,
    selected: bool,
    change: Option<ModelPickerChange>,
    on_change: OnChange,
    popover: Entity<PopoverState>,
    colors: RowColors,
) -> Stateful<Div> {
    div()
        .id(SharedString::from(id))
        .w_full()
        .min_h(px(MODEL_ROW_HEIGHT))
        .py_1p5()
        .px_2()
        .text_sm()
        .text_color(colors.foreground)
        .when(selected, |row| row.bg(colors.selected))
        .when(change.is_none(), |row| row.opacity(0.5))
        .when_some(change, |row, change| {
            row.cursor_pointer()
                .hover(move |row| row.bg(colors.hover))
                .active(move |row| row.bg(colors.active))
                .on_mouse_down(gpui_kit::MouseButton::Left, |_, window, _| {
                    window.prevent_default()
                })
                .on_click(move |_, window, cx| {
                    on_change(change.clone(), cx);
                    popover.update(cx, |state, cx| state.dismiss(window, cx));
                    window.refresh();
                })
        })
}

#[derive(gpui_kit::IntoElement)]
struct PickerTrigger {
    label: String,
    loading: bool,
    selected: bool,
}
impl Selectable for PickerTrigger {
    fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }
    fn is_selected(&self) -> bool {
        self.selected
    }
}
impl RenderOnce for PickerTrigger {
    fn render(self, _: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        h_flex()
            .w_full()
            .h_8()
            .px_2()
            .items_center()
            .justify_between()
            .gap_2()
            .rounded(theme.radius)
            .border_1()
            .border_color(if self.selected {
                theme.primary.opacity(0.8)
            } else {
                theme.border
            })
            .bg(if self.selected {
                theme.secondary.opacity(0.25)
            } else {
                theme.background
            })
            .text_color(theme.foreground)
            .child(if self.loading {
                Spinner::new()
                    .with_size(gpui_kit::component::Size::Small)
                    .color(theme.muted_foreground)
                    .into_any_element()
            } else {
                div()
                    .flex_1()
                    .min_w_0()
                    .text_sm()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(self.label)
                    .into_any_element()
            })
            .child(Icon::new(IconName::ChevronsUpDown).size_3p5())
    }
}

#[cfg(test)]
mod tests {
    use super::api_title;
    use pioneer_client::providers::list::cli_runtime_provider_key;

    #[test]
    fn provider_title_lookup_never_relabels_cli_runtimes() {
        let lookup = Some((|id: &str| Some(format!("title for {id}"))) as super::ApiProviderTitle);
        assert_eq!(
            api_title(lookup, "local").as_deref(),
            Some("title for local")
        );
        assert_eq!(api_title(lookup, &cli_runtime_provider_key("codex")), None);
        assert_eq!(api_title(None, "local"), None);
    }
}
