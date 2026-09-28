use gpui_kit::component::input::{InputEvent, InputState};
use gpui_kit::{prelude::*, *};
use pioneer_client::{core::ClientCore, providers::credentials::*};
use std::sync::Arc;

/// The form alone retains an unsanitized proxy or API base URL while editing.
pub(crate) struct ProxyForm {
    state: CredentialFieldState,
    input: Entity<InputState>,
    _lease: Option<ProviderCredentialLease>,
    _task: Option<Task<()>>,
    _subscription: Subscription,
}

struct CredentialFieldState {
    original: Option<String>,
    edited: bool,
    closed: bool,
}

impl CredentialFieldState {
    fn loaded(&mut self, value: Option<String>) -> Option<String> {
        if self.closed {
            return None;
        }
        let replacement = (!self.edited).then(|| value.clone().unwrap_or_default());
        self.original = value;
        replacement
    }

    fn clear(&mut self) {
        self.closed = true;
        self.original = None;
    }
}

impl ProxyForm {
    fn apply_loaded(&mut self, value: Option<String>, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(value_text) = self.state.loaded(value) {
            if self.input.read(cx).value().as_ref() != value_text {
                self.input
                    .update(cx, |input, cx| input.set_value(value_text, window, cx));
            }
        }
    }
    pub(crate) fn original(&self) -> Option<&str> {
        self.state.original.as_deref()
    }
    pub(crate) fn mutation(&self, cx: &App) -> (Option<String>, bool) {
        let value = self.input.read(cx).value().trim().to_owned();
        let empty = value.is_empty();
        let changed = self.original() != Some(value.as_str());
        let replacement = (changed && !empty).then_some(value);
        // A published marker is an empty string while the credential read is
        // pending. An explicit edit back to empty still means reset.
        let clear = replacement.is_none()
            && self.original().is_some()
            && (changed || self.state.edited && empty);
        (replacement, clear)
    }
    #[cfg(test)]
    pub(crate) fn apply_loaded_for_test(
        &mut self,
        value: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.apply_loaded(value, window, cx);
    }
    pub(crate) fn accept_submission(&mut self, value: String) {
        self.state.original = (!value.is_empty()).then_some(value);
        self.state.edited = false;
        self._lease = None;
        self._task = None;
    }
    pub(crate) fn clear(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.state.clear();
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
        Self::new_with_pending(input, pending, original, window, cx)
    }
    #[cfg(test)]
    pub(crate) fn new_with_pending_for_test(
        input: Entity<InputState>,
        pending: (ProviderCredentialLease, ProviderCredentialRead),
        original: Option<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        Self::new_with_pending(input, Some(pending), original, window, cx)
    }
    fn new_with_pending(
        input: Entity<InputState>,
        pending: Option<(ProviderCredentialLease, ProviderCredentialRead)>,
        original: Option<String>,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        let handle = window.window_handle();
        cx.new(|cx| {
            let subscription = cx.subscribe(&input, |form: &mut Self, _, event: &InputEvent, _| {
                if matches!(event, InputEvent::Change) {
                    form.state.edited = true;
                }
            });
            let (lease, task) = match pending {
                Some((lease, read)) => {
                    let task = cx.spawn(async move |form: WeakEntity<Self>, cx| {
                        let result = cx.background_spawn(async move { read.wait() }).await;
                        if let Ok(value) = result {
                            let _ = handle.update(cx, |_, window, cx| {
                                let _ = form.update(cx, |form, cx| {
                                    form.apply_loaded(value, window, cx);
                                });
                            });
                        }
                    });
                    (Some(lease), Some(task))
                }
                None => (None, None),
            };
            Self {
                state: CredentialFieldState {
                    original,
                    edited: false,
                    closed: false,
                },
                input,
                _lease: lease,
                _task: task,
                _subscription: subscription,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CredentialFieldState;

    #[test]
    fn late_base_url_read_does_not_replace_an_edit_or_reopen_closed_form() {
        let mut state = CredentialFieldState {
            original: Some(String::new()),
            edited: true,
            closed: false,
        };
        assert_eq!(
            state.loaded(Some("https://old.example.test/private".into())),
            None
        );
        assert_eq!(
            state.original.as_deref(),
            Some("https://old.example.test/private")
        );
        state.clear();
        assert_eq!(
            state.loaded(Some("https://late.example.test/private".into())),
            None
        );
        assert!(state.original.is_none());
        assert!(state.closed);
    }
}
