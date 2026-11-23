use crate::{
    binding::SettingsBinding,
    buttons::{default_outline_button, default_primary_button},
    screen::SettingsScreenView,
};
use gpui_kit::{
    component::{Root, StyledExt, WindowExt, dialog::DialogFooter},
    prelude::*,
    *,
};
pub(crate) use pioneer_client::composer::model_selection::ModelSelectorSelection;
use pioneer_client::{
    core::{ClientCore, ClientScope},
    providers::{list::ProviderModelSelectorMode, presentation},
    settings::model_picker::{SettingsModelPickerIntent, SettingsModelPickerPublication},
};
use pioneer_desktop_foundation::{
    ApiProviderTitle, ClientBindingRegistrar, ModelPicker, ModelPickerChange, ModelPickerInput,
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

type ModelSelectorSaveCallback = Rc<
    dyn Fn(
        &mut SettingsScreenView,
        ModelSelectorSelection,
        &mut Context<SettingsScreenView>,
    ) -> bool,
>;
type SaveSelection = Rc<dyn Fn(ModelSelectorSelection, &mut App) -> bool>;
type PickerPublication = Rc<RefCell<Arc<SettingsModelPickerPublication>>>;

pub(crate) struct ModelSelectorDialogOptions {
    pub(crate) title: String,
    pub(crate) selected_provider: Option<String>,
    pub(crate) selected_model: Option<String>,
    pub(crate) selected_reasoning_effort: Option<String>,
    pub(crate) selected_context_window: Option<u64>,
    pub(crate) selected_service_mode: Option<pioneer_protocol::NativeServiceModeSelection>,
    pub(crate) selected_context_authority: Option<String>,
    pub(crate) mode: ProviderModelSelectorMode,
    pub(crate) workspace_id: String,
    pub(crate) client: Arc<ClientCore>,
    pub(crate) on_save: ModelSelectorSaveCallback,
}

/// Settings-backed picker session. The caller owns persistence of the selection.
pub struct SharedModelSelectorOptions {
    title: String,
    workspace_id: String,
    client: Arc<ClientCore>,
    bindings: Arc<dyn ClientBindingRegistrar>,
    on_save: SaveSelection,
    on_refresh: Rc<dyn Fn(&mut App)>,
    selection: ModelSelectorSelection,
    mode: ProviderModelSelectorMode,
    fixed_provider: bool,
    api_provider_title: Option<ApiProviderTitle>,
}

impl SharedModelSelectorOptions {
    pub fn new(
        title: String,
        workspace_id: String,
        client: Arc<ClientCore>,
        bindings: Arc<dyn ClientBindingRegistrar>,
        on_save: SaveSelection,
    ) -> Self {
        Self {
            title,
            workspace_id,
            client,
            bindings,
            on_save,
            on_refresh: Rc::new(|_| {}),
            selection: ModelSelectorSelection::default(),
            mode: ProviderModelSelectorMode::Chat,
            fixed_provider: false,
            api_provider_title: None,
        }
    }
    pub fn selection(mut self, selection: ModelSelectorSelection) -> Self {
        self.selection = selection;
        self
    }
    pub fn mode(mut self, mode: ProviderModelSelectorMode) -> Self {
        self.mode = mode;
        self
    }
    pub fn fixed_provider(mut self) -> Self {
        self.fixed_provider = true;
        self
    }
    pub fn on_refresh(mut self, callback: Rc<dyn Fn(&mut App)>) -> Self {
        self.on_refresh = callback;
        self
    }
    pub fn with_api_provider_title(mut self, title: Option<ApiProviderTitle>) -> Self {
        self.api_provider_title = title;
        self
    }
}

impl SettingsScreenView {
    pub(crate) fn open_model_selector_dialog(
        &mut self,
        options: ModelSelectorDialogOptions,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let save_owner = cx.weak_entity();
        let refresh_owner = save_owner.clone();
        let on_save = options.on_save;
        let shared = SharedModelSelectorOptions::new(
            options.title,
            options.workspace_id,
            options.client,
            self.config.bindings.clone(),
            Rc::new(move |selection, cx| {
                save_owner
                    .update(cx, |view, cx| {
                        let saved = on_save(view, selection, cx);
                        cx.notify();
                        saved
                    })
                    .unwrap_or(false)
            }),
        )
        .selection(ModelSelectorSelection {
            provider: options.selected_provider,
            model: options.selected_model,
            selected_reasoning_effort: options.selected_reasoning_effort,
            selected_context_window: options.selected_context_window,
            selected_service_mode: options.selected_service_mode,
            selected_context_authority: options.selected_context_authority,
        })
        .mode(options.mode)
        .with_api_provider_title(self.config.platform.api_provider_title())
        .on_refresh(Rc::new(move |cx| {
            let _ = refresh_owner.update(cx, |_, cx| cx.notify());
        }));
        open_shared_model_selector(shared, window, cx);
    }
}

struct ModelSelectorOwner {
    client: Weak<ClientCore>,
    picker_id: String,
    generation: u64,
    value: PickerPublication,
    window: AnyWindowHandle,
    task: Option<Task<()>>,
    binding: Option<Arc<SettingsBinding>>,
    closed: Rc<Cell<bool>>,
}

impl ModelSelectorOwner {
    fn new(
        client: &Arc<ClientCore>,
        bindings: &Arc<dyn ClientBindingRegistrar>,
        value: PickerPublication,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        let picker_id = value.borrow().picker_id.clone();
        let generation = value.borrow().owner_generation;
        let binding = SettingsBinding::new(
            vec![ClientScope::SettingsModelPicker {
                picker_id: picker_id.clone(),
            }],
            bindings,
        );
        let mut changed = binding.changed.subscribe();
        let handle = window.window_handle();
        let client = Arc::downgrade(client);
        cx.new(|cx| {
            let task = cx.spawn(async move |owner: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if handle
                        .update(cx, |_, window, cx| {
                            owner.update(cx, |owner, cx| {
                                owner.pull();
                                Root::update(window, cx, |_, _, cx| cx.notify());
                            })
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            Self {
                client,
                picker_id,
                generation,
                value,
                window: handle,
                task: Some(task),
                binding: Some(binding),
                closed: Rc::new(Cell::new(false)),
            }
        })
    }

    fn pull(&mut self) {
        let next = self
            .client
            .upgrade()
            .and_then(|client| client.settings_model_picker(&self.picker_id));
        match next.filter(|value| value.owner_generation == self.generation && !value.closed) {
            Some(next) => *self.value.borrow_mut() = next,
            None => self.close(),
        }
    }

    fn change(&mut self, change: ModelPickerChange, cx: &mut App) {
        if self.closed.get() {
            return;
        }
        let Some(client) = self.client.upgrade() else {
            return;
        };
        use SettingsModelPickerIntent as Intent;
        let picker_id = self.picker_id.clone();
        let expected_owner = self.generation;
        client.settings_model_picker_intent(match change {
            ModelPickerChange::Provider(provider) => Intent::SelectProvider {
                picker_id,
                expected_owner,
                provider,
            },
            ModelPickerChange::Model(model) => Intent::SelectModel {
                picker_id,
                expected_owner,
                model,
            },
            ModelPickerChange::ReasoningEffort(effort) => Intent::SelectReasoningEffort {
                picker_id,
                expected_owner,
                effort,
            },
            ModelPickerChange::ServiceMode(id) => Intent::SelectServiceMode {
                picker_id,
                expected_owner,
                id,
            },
            ModelPickerChange::ContextWindow(window) => Intent::SelectContextWindow {
                picker_id,
                expected_owner,
                window,
            },
        });
        self.pull();
        let _ = self.window.update(cx, |_, window, cx| {
            Root::update(window, cx, |_, _, cx| cx.notify());
        });
    }

    fn close(&mut self) {
        if self.closed.replace(true) {
            return;
        }
        self.task.take();
        self.binding.take();
        if let Some(client) = self.client.upgrade() {
            client.settings_model_picker_intent(SettingsModelPickerIntent::Close {
                picker_id: self.picker_id.clone(),
                expected_owner: self.generation,
            });
        }
    }
}

impl Drop for ModelSelectorOwner {
    fn drop(&mut self) {
        self.close();
    }
}

impl Render for ModelSelectorOwner {
    fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
        div()
    }
}

pub fn open_shared_model_selector(
    options: SharedModelSelectorOptions,
    window: &mut Window,
    cx: &mut App,
) {
    static NEXT_PICKER: AtomicU64 = AtomicU64::new(1);
    let id = NEXT_PICKER
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .expect("model picker identity exhausted");
    let picker_id = format!("model-selector:{id}");
    let fixed_provider = options
        .fixed_provider
        .then(|| options.selection.provider.clone())
        .flatten();
    options
        .client
        .settings_model_picker_intent(SettingsModelPickerIntent::Open {
            picker_id: picker_id.clone(),
            workspace_id: options.workspace_id,
            mode: options.mode,
            selection: options.selection,
        });
    let Some(publication) = options
        .client
        .settings_model_picker(&picker_id)
        .filter(|value| !value.closed)
    else {
        return;
    };
    let generation = publication.owner_generation;
    let value = Rc::new(RefCell::new(publication));
    let owner = ModelSelectorOwner::new(
        &options.client,
        &options.bindings,
        value.clone(),
        window,
        cx,
    );
    let closed = owner.read(cx).closed.clone();
    let picker = ModelPicker::new(
        options.mode,
        fixed_provider.is_some(),
        options.api_provider_title,
        picker_text,
        window,
        cx,
    );
    let mode = options.mode;
    let title = options.title;
    let on_refresh = options.on_refresh;
    let save: Rc<dyn Fn(&mut App) -> bool> = Rc::new(move |cx| {
        let Some(selection) = options
            .client
            .settings_model_picker_selection(&picker_id, generation)
        else {
            return false;
        };
        if fixed_provider
            .as_ref()
            .is_some_and(|expected| selection.provider.as_ref() != Some(expected))
        {
            return false;
        }
        (options.on_save)(selection, cx)
    });
    window.open_dialog(cx, move |dialog, _, _| {
        if closed.get() {
            return dialog
                .title(title.clone())
                .child(t!("settings.unavailable").to_string())
                .footer(
                    DialogFooter::new().child(
                        default_outline_button("model-selector-cancel")
                            .label(t!("buttons.cancel").to_string())
                            .on_click(|_, window, cx| window.close_dialog(cx)),
                    ),
                );
        }
        let on_change = owner.clone();
        let on_close = owner.clone();
        let refresh = on_refresh.clone();
        let ok = save.clone();
        let save = save.clone();
        let controls = picker.render(picker_input(&value.borrow(), mode), move |action, cx| {
            on_change.update(cx, |owner, cx| owner.change(action, cx));
            refresh(cx);
        });
        dialog
            .gap_1()
            .rounded_2xl()
            .title(div().text_base().font_semibold().child(title.clone()))
            .on_close(move |_, _, cx| on_close.update(cx, |owner, _| owner.close()))
            .on_ok(move |_, _, cx| ok(cx))
            .footer(DialogFooter::new().children(vec![
                    default_outline_button("model-selector-cancel")
                        .label(t!("buttons.cancel").to_string())
                        .outline()
                        .on_click(|_, window, cx| window.close_dialog(cx))
                        .into_any_element(),
                    default_primary_button("model-selector-save")
                        .label(t!("buttons.save").to_string())
                        .on_click(move |_, window, cx| {
                            if save(cx) {
                                window.close_dialog(cx);
                            }
                        })
                        .into_any_element(),
                ]))
            .child(controls)
    });
}

fn picker_input(
    input: &SettingsModelPickerPublication,
    mode: ProviderModelSelectorMode,
) -> ModelPickerInput {
    let service_mode = input
        .selected_service_mode
        .as_ref()
        .map(|mode| mode.id.clone());
    let mut reasoning_rows = if mode == ProviderModelSelectorMode::Transcription {
        Vec::new()
    } else {
        input
            .selector
            .selected_model()
            .and_then(|selected| {
                input
                    .selector
                    .models()
                    .iter()
                    .find(|model| model.id == selected)
            })
            .map(|model| {
                presentation::reasoning_effort_rows_for_model(
                    model,
                    input.selected_reasoning_effort.as_deref(),
                )
            })
            .unwrap_or_default()
    };
    if service_mode.as_deref() == Some("pro") {
        reasoning_rows.retain(|row| !matches!(row.effort.as_str(), "none" | "off"));
    }
    ModelPickerInput {
        selector: input.selector.clone(),
        reasoning_rows,
        service_rows: input.service_rows.clone(),
        context_rows: input.context_rows.clone(),
        selected_reasoning_effort: input.selected_reasoning_effort.clone(),
        selected_service_mode_id: service_mode,
        selected_context_window: input.selected_context_window,
        default_reasoning_only: input.default_reasoning_only,
    }
}

fn picker_text(key: &str) -> String {
    if key == "recommended" {
        t!("settings.voice_input.recommended").to_string()
    } else {
        t!(format!("chat.composer.model.{key}")).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ModelSelectorOwner, ModelSelectorSelection, ProviderModelSelectorMode,
        SettingsModelPickerIntent,
    };
    use gpui_kit::Entity;
    use std::{cell::RefCell, rc::Rc};

    struct Registrar(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl pioneer_desktop_foundation::ClientBindingRegistrar for Registrar {
        fn register(
            &self,
            _: pioneer_client::core::ClientScope,
            _: std::sync::Weak<dyn pioneer_desktop_foundation::ClientPublicationSink>,
        ) -> pioneer_desktop_foundation::ClientBindingRegistration {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let count = self.0.clone();
            pioneer_desktop_foundation::ClientBindingRegistration::new(move || {
                count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            })
        }
    }

    #[gpui_kit::test]
    fn picker_retains_one_binding_and_explicit_close_fences_reopened_owner(
        cx: &mut gpui_kit::TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let client = pioneer_client::catalog_test_support::settings_model_picker_client();
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let registrar: std::sync::Arc<dyn pioneer_desktop_foundation::ClientBindingRegistrar> =
            std::sync::Arc::new(Registrar(count.clone()));
        let open = || {
            client.settings_model_picker_intent(SettingsModelPickerIntent::Open {
                picker_id: "dialog".into(),
                workspace_id: "workspace".into(),
                mode: ProviderModelSelectorMode::Embeddings,
                selection: ModelSelectorSelection::default(),
            })
        };
        open();
        let value = Rc::new(RefCell::new(
            client.settings_model_picker("dialog").unwrap(),
        ));
        let (root, cx) = cx.add_window_view(|window, cx| {
            let owner = ModelSelectorOwner::new(&client, &registrar, value.clone(), window, cx);
            gpui_kit::component::Root::new(owner, window, cx)
        });
        let owner: Entity<ModelSelectorOwner> =
            root.read_with(cx, |root, _| root.view().clone().downcast().unwrap());
        cx.run_until_parked();
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
        cx.update(|_, cx| owner.update(cx, |owner, _| owner.close()));
        assert!(client.settings_model_picker("dialog").unwrap().closed);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
        open();
        let newer = client.settings_model_picker("dialog").unwrap();
        assert!(newer.owner_generation > value.borrow().owner_generation);
        cx.update(|_, cx| owner.update(cx, |owner, _| owner.close()));
        assert!(!client.settings_model_picker("dialog").unwrap().closed);
    }

    #[::core::prelude::v1::test]
    fn model_selector_transcription_recommended_badge_is_localized() {
        assert_eq!(
            super::picker_text("recommended"),
            t!("settings.voice_input.recommended").as_ref(),
        );
    }

    #[::core::prelude::v1::test]
    fn model_selector_dialog_render_never_reenters_the_desktop_entity() {
        let render = include_str!("model_selector.rs")
            .split_once("window.open_dialog(cx, move |dialog, _, _|")
            .expect("dialog render exists")
            .1
            .split_once("fn picker_input(")
            .expect("dialog render has a boundary")
            .0;

        assert!(!render.contains("desktop_entity"));
        assert!(!render.contains(".read(cx)"));
    }
}
