use gpui_kit::component::WindowExt;
use gpui_kit::{prelude::*, *};

/// Owns only the form lifetime. Root remains the sole focus and dismissal owner.
pub(crate) struct DialogLifetime {
    open: bool,
    valid: bool,
    focus: Option<FocusHandle>,
    clear: Box<dyn Fn(&mut Window, &mut App)>,
    _focus: Option<Subscription>,
    _forms: Vec<Subscription>,
}
impl DialogLifetime {
    pub(crate) fn new(
        clear: impl Fn(&mut Window, &mut App) + 'static,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|_| Self {
            open: true,
            valid: true,
            focus: None,
            clear: Box::new(clear),
            _focus: None,
            _forms: Vec::new(),
        })
    }
    pub(crate) fn track_form<T: 'static>(&mut self, form: &Entity<T>, cx: &mut Context<Self>) {
        self._forms.push(cx.observe_release(form, |owner, _, _| {
            owner.open = false;
            owner.valid = false;
        }));
    }
    pub(crate) fn valid(&self) -> bool {
        self.open && self.valid
    }
    pub(crate) fn open(&self) -> bool {
        self.open
    }
    pub(crate) fn attach(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(focus) = window.focused(cx) {
            self._focus = Some(cx.on_focus_in(&focus, window, |owner, window, cx| {
                if !owner.valid {
                    owner.close_if_focused(window, cx);
                }
            }));
            self.focus = Some(focus);
        }
    }
    pub(crate) fn dismissed(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.open {
            return;
        }
        self.open = false;
        if self.valid {
            self.valid = false;
            (self.clear)(window, cx);
        }
    }
    pub(crate) fn invalidate(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.valid {
            self.valid = false;
            (self.clear)(window, cx);
        }
        self.close_if_focused(window, cx);
    }
    fn close_if_focused(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.open
            && self
                .focus
                .as_ref()
                .is_some_and(|focus| focus.is_focused(window) || focus.contains_focused(window, cx))
        {
            self.open = false;
            window.close_dialog(cx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DialogLifetime;
    use gpui_kit::component::WindowExt;
    use gpui_kit::component::{
        Root,
        input::{Input, InputEvent, InputState},
    };
    use gpui_kit::{
        AppContext, Context, Entity, Focusable, IntoElement, ParentElement, Render, TestAppContext,
        Window,
    };
    use pioneer_client::providers::credentials::provider_credential_read_for_test;
    use std::{cell::Cell, rc::Rc};
    struct Host {
        input: Entity<InputState>,
    }
    impl Render for Host {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            gpui_kit::div().child(Input::new(&self.input))
        }
    }
    #[gpui_kit::test]
    fn scope_or_permission_invalidation_clears_open_form_once(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let (_, cx) = cx.add_window_view(|window, cx| {
            let input = cx.new(|cx| InputState::new(window, cx));
            let host = cx.new(|_| Host { input });
            Root::new(host, window, cx)
        });
        let clears = Rc::new(Cell::new(0));
        let count = clears.clone();
        let guard =
            cx.update(|_, cx| DialogLifetime::new(move |_, _| count.set(count.get() + 1), cx));
        cx.update(|window, cx| {
            guard.update(cx, |guard, cx| {
                guard.invalidate(window, cx);
                guard.invalidate(window, cx);
                assert!(!guard.valid());
            });
        });
        assert_eq!(clears.get(), 1);
    }
    #[gpui_kit::test]
    fn base_url_input_ignores_late_read_and_builds_user_mutation(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let (lease, read, sender) = provider_credential_read_for_test();
        let (root, cx) = cx.add_window_view(|window, cx| {
            let input = cx.new(|cx| InputState::new(window, cx));
            let host = cx.new(|_| Host { input });
            Root::new(host, window, cx)
        });
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<Host>().unwrap()
        });
        let input = host.read_with(cx, |host, _| host.input.clone());
        let (form, guard) = cx.update(|window, cx| {
            let form = crate::credential_form::ProxyForm::new_with_pending_for_test(
                input.clone(),
                (lease, read),
                Some(String::new()),
                window,
                cx,
            );
            let weak = form.downgrade();
            let guard = DialogLifetime::new(
                move |window, cx| {
                    let _ = weak.update(cx, |form, cx| form.clear(window, cx));
                },
                cx,
            );
            (form, guard)
        });
        // Saving an API key while the URL lease is pending must leave the
        // existing override untouched; the publication contains only a marker.
        assert_eq!(
            form.read_with(cx, |form, cx| form.mutation(cx)),
            (None, false)
        );
        // replace_all emits InputEvent::Change without waiting for the pending lease task.
        cx.update(|window, cx| {
            input.update(cx, |input, cx| {
                input.replace_all("https://new.example/v1", window, cx)
            });
        });
        sender
            .send(Ok(Some("https://old.example/private".into())))
            .unwrap();
        cx.run_until_parked();
        cx.update(|window, cx| {
            assert_eq!(input.read(cx).value().as_ref(), "https://new.example/v1");
            assert_eq!(
                form.read(cx).mutation(cx),
                (Some("https://new.example/v1".into()), false)
            );
            input.update(cx, |input, cx| input.replace_all("", window, cx));
            assert_eq!(form.read(cx).mutation(cx), (None, true));
            guard.update(cx, |guard, cx| guard.invalidate(window, cx));
            form.update(cx, |form, cx| {
                form.apply_loaded_for_test(Some("https://late.example/private".into()), window, cx)
            });
            assert!(input.read(cx).value().is_empty());
            assert_eq!(form.read(cx).mutation(cx), (None, false));
        });
    }
    #[gpui_kit::test]
    fn clearing_base_url_before_lease_reply_remains_a_reset(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let (lease, read, sender) = provider_credential_read_for_test();
        let (root, cx) = cx.add_window_view(|window, cx| {
            let input = cx.new(|cx| InputState::new(window, cx));
            Root::new(cx.new(|_| Host { input }), window, cx)
        });
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<Host>().unwrap()
        });
        let input = host.read_with(cx, |host, _| host.input.clone());
        let form = cx.update(|window, cx| {
            crate::credential_form::ProxyForm::new_with_pending_for_test(
                input.clone(),
                (lease, read),
                Some(String::new()),
                window,
                cx,
            )
        });
        cx.update(|window, cx| {
            input.update(cx, |input, cx| input.replace_all("x", window, cx));
            input.update(cx, |input, cx| input.replace_all("", window, cx));
        });
        assert_eq!(
            form.read_with(cx, |form, cx| form.mutation(cx)),
            (None, true)
        );
        sender
            .send(Ok(Some("https://old.example/private".into())))
            .unwrap();
        cx.run_until_parked();
        assert!(input.read_with(cx, |input, _| input.value().is_empty()));
        assert_eq!(
            form.read_with(cx, |form, cx| form.mutation(cx)),
            (None, true)
        );
    }
    #[gpui_kit::test]
    fn stock_dialog_dismissal_restores_trigger_and_invalidates_form_once(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let (root, cx) = cx.add_window_view(|window, cx| {
            let input = cx.new(|cx| InputState::new(window, cx));
            let host = cx.new(|_| Host { input });
            Root::new(host, window, cx)
        });
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<Host>().unwrap()
        });
        let input = host.read_with(cx, |host, _| host.input.clone());
        let clears = Rc::new(Cell::new(0));
        let count = clears.clone();
        let guard = cx.update(|window, cx| {
            input.update(cx, |input, cx| input.focus(window, cx));
            let guard = DialogLifetime::new(move |_, _| count.set(count.get() + 1), cx);
            let close = guard.downgrade();
            window.open_dialog(cx, move |dialog, _, _| {
                let close = close.clone();
                dialog
                    .title("Synthetic object")
                    .on_close(move |_, window, cx| {
                        let _ = close.update(cx, |guard, cx| guard.dismissed(window, cx));
                    })
            });
            guard.update(cx, |guard, cx| guard.attach(window, cx));
            guard
        });
        cx.run_until_parked();
        cx.simulate_keystrokes("escape");
        cx.run_until_parked();
        cx.executor()
            .advance_clock(std::time::Duration::from_secs(1));
        cx.run_until_parked();
        assert_eq!(clears.get(), 1);
        cx.update(|window, cx| {
            guard.update(cx, |guard, cx| guard.invalidate(window, cx));
            assert!(input.read(cx).focus_handle(cx).is_focused(window));
        });
        assert_eq!(clears.get(), 1);
    }
    #[gpui_kit::test]
    fn host_input_sync_has_no_user_echo_and_released_form_retires_its_guard(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let (_, cx) = cx.add_window_view(|window, cx| {
            let input = cx.new(|cx| InputState::new(window, cx));
            let host = cx.new(|_| Host { input });
            Root::new(host, window, cx)
        });
        let changes = Rc::new(Cell::new(0));
        let count = changes.clone();
        let (input, subscription, guard) = cx.update(|window, cx| {
            let input = cx.new(|cx| InputState::new(window, cx));
            let subscription = cx.subscribe(&input, move |_, event: &InputEvent, _| {
                if matches!(event, InputEvent::Change) {
                    count.set(count.get() + 1);
                }
            });
            let guard = DialogLifetime::new(|_, _| {}, cx);
            guard.update(cx, |guard, cx| guard.track_form(&input, cx));
            input.update(cx, |input, cx| input.set_value("host", window, cx));
            (input, subscription, guard)
        });
        cx.run_until_parked();
        assert_eq!(changes.get(), 0);
        drop(input);
        drop(subscription);
        cx.update(|_, _| {});
        cx.run_until_parked();
        assert!(!guard.read_with(cx, |guard, _| guard.open()));
    }
}
