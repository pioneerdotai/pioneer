use gpui_kit::component::input::{InputEvent, InputState};
use gpui_kit::{prelude::*, *};
use pioneer_client::{core::ClientCore, providers::credentials::*};
use std::sync::Arc;

/// The form alone retains the unsanitized proxy while it is being edited.
pub(crate) struct ProxyForm {
    original: Option<String>,
    edited: bool,
    closed: bool,
    input: Entity<InputState>,
    _lease: Option<ProviderCredentialLease>,
    _task: Option<Task<()>>,
    _subscription: Subscription,
}
impl ProxyForm {
    pub(crate) fn original(&self) -> Option<&str> {
        self.original.as_deref()
    }
    pub(crate) fn accept_submission(&mut self, value: String) {
        self.original = (!value.is_empty()).then_some(value);
        self.edited = false;
        self._lease = None;
        self._task = None;
    }
    pub(crate) fn clear(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.closed = true;
        self.original = None;
        self._lease = None;
        self._task = None;
        if !self.input.read(cx).value().is_empty() {
            self.input
                .update(cx, |input, cx| input.set_value("", window, cx));
        }
    }
    pub(crate) fn new(
        input: Entity<InputState>,
        client: &Arc<ClientCore>,
        workspace: String,
        target: ProviderCredentialTarget,
        original: Option<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        let pending = client.read_provider_credential(workspace, target).ok();
        let handle = window.window_handle();
        cx.new(|cx| {
            let subscription = cx.subscribe(&input, |form: &mut Self, _, event: &InputEvent, _| {
                if matches!(event, InputEvent::Change) {
                    form.edited = true;
                }
            });
            let (lease, task) = match pending {
                Some((lease, read)) => {
                    let task = cx.spawn(async move |form: WeakEntity<Self>, cx| {
                        let result = cx.background_spawn(async move { read.wait() }).await;
                        if let Ok(value) = result {
                            let _ = handle.update(cx, |_, window, cx| {
                                let _ = form.update(cx, |form, cx| {
                                    if form.closed {
                                        return;
                                    }
                                    if !form.edited {
                                        let value_text = value.as_deref().unwrap_or_default();
                                        if form.input.read(cx).value().as_ref() != value_text {
                                            form.input.update(cx, |input, cx| {
                                                input.set_value(value_text.to_owned(), window, cx)
                                            });
                                        }
                                    }
                                    form.original = value;
                                });
                            });
                        }
                    });
                    (Some(lease), Some(task))
                }
                None => (None, None),
            };
            Self {
                original,
                edited: false,
                closed: false,
                input,
                _lease: lease,
                _task: task,
                _subscription: subscription,
            }
        })
    }
}
