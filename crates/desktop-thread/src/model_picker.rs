use crate::buttons::{default_outline_button, default_primary_button};
use gpui_kit::{component::dialog::DialogFooter, prelude::*, *};
use pioneer_client::{
    composer::model_picker::{ComposerModelPickerIntent, ComposerModelPickerPublication},
    composer::store::ComposerOperationIdentity,
    core::{ClientCore, ClientPublicationReference, ClientScope},
    providers::list::ProviderModelSelectorMode,
};
use pioneer_desktop_foundation::{
    ApiProviderTitle, ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
    ModelPicker, ModelPickerChange, ModelPickerInput,
};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

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

pub(crate) struct ComposerModelPickerView {
    client: Arc<ClientCore>,
    identity: ComposerOperationIdentity,
    binding: Arc<ModelPickerBinding>,
    registration: Option<ClientBindingRegistration>,
    task: Option<Task<()>>,
    _release: Subscription,
    dialog_open: bool,
    input: Arc<ComposerModelPickerPublication>,
    picker: ModelPicker,
}

impl ComposerModelPickerView {
    pub(crate) fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        input: Arc<ComposerModelPickerPublication>,
        api_provider_title: Option<ApiProviderTitle>,
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
                                view.input = input
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
        let picker = ModelPicker::new(
            ProviderModelSelectorMode::Chat,
            false,
            api_provider_title,
            picker_text,
            window,
            cx,
        );
        Self {
            client,
            identity,
            binding,
            registration: Some(registration),
            task: Some(task),
            _release: release,
            dialog_open: false,
            input,
            picker,
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
            self.input = input;
        }
        cx.notify();
    }

    fn change(&mut self, change: ModelPickerChange, cx: &mut Context<Self>) {
        use ComposerModelPickerIntent as Intent;
        let identity = self.identity.clone();
        self.send(
            match change {
                ModelPickerChange::Provider(provider) => {
                    Intent::SelectProvider { identity, provider }
                }
                ModelPickerChange::Model(model) => Intent::SelectModel { identity, model },
                ModelPickerChange::ReasoningEffort(effort) => {
                    Intent::SelectReasoningEffort { identity, effort }
                }
                ModelPickerChange::ServiceMode(id) => Intent::SelectServiceMode { identity, id },
                ModelPickerChange::ContextWindow(window) => {
                    Intent::SelectContextWindow { identity, window }
                }
            },
            cx,
        );
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

    fn commit(&mut self, cx: &mut Context<Self>) {
        self.send(
            ComposerModelPickerIntent::Commit {
                identity: self.identity.clone(),
            },
            cx,
        );
    }

    pub(crate) fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.dialog_open = true;
        let owner = cx.weak_entity();
        window.open_dialog(cx, move |dialog, _, cx| {
            let Some(view) = owner.upgrade() else {
                return dialog;
            };
            let view = view.read(cx);
            let input = &view.input;
            let change = owner.clone();
            let controls = view.picker.render(
                ModelPickerInput {
                    selector: input.selector.clone(),
                    reasoning_rows: input.reasoning_rows.clone(),
                    service_rows: input.service_rows.clone(),
                    context_rows: input.context_rows.clone(),
                    selected_reasoning_effort: input.selected_reasoning_effort.clone(),
                    selected_service_mode_id: input
                        .selected_service_mode
                        .as_ref()
                        .map(|mode| mode.id.clone()),
                    selected_context_window: input.selected_context_window,
                    default_reasoning_only: input.default_reasoning_only,
                },
                move |action, cx| {
                    let _ = change.update(cx, |view, cx| view.change(action, cx));
                },
            );
            let cancel = owner.clone();
            let save = owner.clone();
            let ok = owner.clone();
            let closed = owner.clone();
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
                        view.commit(cx);
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
                                let _ = save.update(cx, |view, cx| {
                                    view.commit(cx);
                                    view.close(window, cx);
                                });
                            })
                            .into_any_element(),
                    ]))
                .child(controls)
        });
    }
}

fn picker_text(key: &str) -> String {
    t!(format!("chat.composer.model.{key}")).to_string()
}

impl Drop for ComposerModelPickerView {
    fn drop(&mut self) {
        self.retire();
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
            let picker = cx.new(|cx| {
                ComposerModelPickerView::new(core.clone(), registrar, input, None, window, cx)
            });
            let weak = picker.downgrade();
            let (provider_input, model_input) = picker.update(cx, |view, cx| {
                view.open(window, cx);
                let (provider, model) = view.picker.search_inputs();
                provider.update(cx, |input, cx| {
                    input.set_value("filter", window, cx);
                    input.focus(window, cx);
                });
                (provider.downgrade(), model.downgrade())
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
