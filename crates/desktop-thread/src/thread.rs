use crate::{
    artifacts::ThreadArtifactsView,
    binding::ThreadBindings,
    composer::ComposerView,
    footer::ThreadFooterView,
    header::{HeaderBack, ThreadHeaderView},
    members::ThreadMembersView,
    panel_layout::ThreadPanelLayoutStore,
    panels::ThreadSidePanelHostView,
    ports::*,
    screen::{ThreadScreenEvent, ThreadScreenView},
};
use gpui_kit::{
    component::{
        resizable::{h_resizable, resizable_panel},
        theme::ActiveTheme,
        v_flex,
    },
    prelude::*,
    *,
};
use pioneer_client::core::{ClientCore, ClientPublicationReference};
use pioneer_desktop_foundation::ClientBindingRegistrar;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub struct ThreadViewConfig {
    client: Arc<ClientCore>,
    thread_id: String,
    initial: Vec<ClientPublicationReference>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    files: Arc<dyn ThreadFilePort>,
    audio: Arc<dyn ThreadAudioPort>,
    external: Arc<dyn ThreadExternalNavigationPort>,
}
impl ThreadViewConfig {
    pub fn new(
        client: Arc<ClientCore>,
        thread_id: String,
        registrar: Arc<dyn ClientBindingRegistrar>,
        files: Arc<dyn ThreadFilePort>,
        audio: Arc<dyn ThreadAudioPort>,
        external: Arc<dyn ThreadExternalNavigationPort>,
    ) -> Self {
        Self {
            client,
            thread_id,
            registrar,
            files,
            audio,
            external,
            initial: Vec::new(),
        }
    }
    pub fn with_initial_publications(mut self, initial: Vec<ClientPublicationReference>) -> Self {
        self.initial = initial;
        self
    }
}

/// Only semantic navigation crosses the feature boundary.
pub enum ThreadNavigationEvent {
    OpenTaskThread {
        parent_thread_id: String,
        child_thread_id: String,
        title: String,
    },
    CloseTaskThread,
    OpenMcpServer {
        server_id: String,
    },
}

/// One opaque capability root per mounted thread. Domain publications terminate
/// in its private owning children; only window layout changes notify this root.
pub struct ThreadView {
    thread_id: String,
    screen: Entity<ThreadScreenView>,
    header: Entity<ThreadHeaderView>,
    composer: Entity<ComposerView>,
    panels: Entity<ThreadSidePanelHostView>,
    footer: Entity<ThreadFooterView>,
    layout: Entity<ThreadPanelLayoutStore>,
    files: Arc<dyn ThreadFilePort>,
    external: Arc<dyn ThreadExternalNavigationPort>,
    mount: u64,
    visible: bool,
    client: Arc<ClientCore>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    message_history: Option<Entity<crate::message_history::MessageRevisionView>>,
    _subscriptions: Vec<Subscription>,
}
impl EventEmitter<ThreadNavigationEvent> for ThreadView {}
static NEXT_MOUNT: AtomicU64 = AtomicU64::new(1);
impl ThreadView {
    pub fn set_visible(&mut self, visible: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.visible == visible {
            return;
        }
        self.visible = visible;
        self.screen
            .update(cx, |view, cx| view.set_visible(visible, window, cx));
        self.composer
            .update(cx, |view, cx| view.set_visible(visible, window, cx));
        self.header.read(cx).set_visible(visible);
        self.footer.read(cx).set_visible(visible);
        self.panels
            .update(cx, |view, cx| view.set_route_visible(visible, cx));
        if !visible {
            self.message_history.take();
        }
        self.set_window_active(window.is_window_active(), cx);
    }
    pub fn set_window_active(&mut self, active: bool, cx: &mut Context<Self>) {
        let active = active && self.visible;
        self.screen.update(cx, |screen, cx| {
            screen
                .running_indicator_views
                .borrow_mut()
                .set_active(active, cx);
        });
        self.composer
            .update(cx, |composer, cx| composer.set_window_active(active, cx));
    }
    pub fn new(config: ThreadViewConfig, window: &mut Window, cx: &mut App) -> Entity<Self> {
        let mount = NEXT_MOUNT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |mount| {
                mount.checked_add(3)
            })
            .expect("thread mount identity exhausted");
        config
            .client
            .composer_intent(pioneer_client::composer::store::ComposerIntent::Activate {
                thread_id: config.thread_id.clone(),
            });
        let layout = ThreadPanelLayoutStore::for_window(window, cx);
        let screen_bindings = ThreadBindings::new(
            config.registrar.clone(),
            &config.thread_id,
            config.initial.clone(),
        );
        let composer_bindings = ThreadBindings::scoped(
            config.registrar.clone(),
            vec![
                pioneer_client::core::ClientScope::Composer {
                    thread_id: config.thread_id.clone(),
                },
                pioneer_client::core::ClientScope::ComposerCatalog {
                    thread_id: config.thread_id.clone(),
                },
                pioneer_client::core::ClientScope::Thread {
                    thread_id: config.thread_id.clone(),
                },
                pioneer_client::core::ClientScope::ThreadCapability {
                    thread_id: config.thread_id.clone(),
                },
                pioneer_client::core::ClientScope::ThreadMember {
                    thread_id: config.thread_id.clone(),
                },
                pioneer_client::core::ClientScope::TurnCancellation {
                    thread_id: config.thread_id.clone(),
                },
                pioneer_client::core::ClientScope::Administration { workspace_id: None },
                pioneer_client::core::ClientScope::Session,
            ],
            config.initial,
        );
        let screen = ThreadScreenView::new(
            config.client.clone(),
            config.thread_id.clone(),
            screen_bindings,
            config.files.clone(),
            config.external.clone(),
            mount,
            window,
            cx,
        );
        let composer = ComposerView::new(
            config.client.clone(),
            config.thread_id.clone(),
            composer_bindings,
            screen.downgrade(),
            config.files.clone(),
            config.audio,
            mount + 1,
            window,
            cx,
        );
        let header = ThreadHeaderView::new(
            config.client.clone(),
            config.thread_id.clone(),
            config.registrar.clone(),
            layout.clone(),
            config.files.clone(),
            mount,
            window,
            cx,
        );
        let members = ThreadMembersView::new(
            config.client.clone(),
            config.thread_id.clone(),
            config.registrar.clone(),
            screen.downgrade(),
            window,
            cx,
        );
        let artifacts = ThreadArtifactsView::new(
            config.client.clone(),
            config.thread_id.clone(),
            config.registrar.clone(),
            config.files.clone(),
            config.external.clone(),
            mount + 2,
            cx,
        );
        let panels =
            cx.new(|cx| ThreadSidePanelHostView::new(layout.clone(), artifacts, members, cx));
        let footer = cx.new(|cx| {
            ThreadFooterView::new(
                config.client.clone(),
                config.thread_id.clone(),
                config.registrar.clone(),
                layout.clone(),
                cx,
            )
        });
        cx.new(|cx: &mut Context<Self>| {
            let subscriptions = vec![
                cx.observe(&layout, |_, _, cx| cx.notify()),
                cx.subscribe(&header, |_, _, _: &HeaderBack, cx| {
                    cx.emit(ThreadNavigationEvent::CloseTaskThread)
                }),
                cx.subscribe_in(
                    &screen,
                    window,
                    |view, _, event: &ThreadScreenEvent, window, cx| match event {
                        ThreadScreenEvent::FocusComposer => {
                            view.composer.update(cx, |view, cx| view.focus(window, cx))
                        }
                        ThreadScreenEvent::ComposerDomain(action) => {
                            view.composer.update(cx, |view, cx| {
                                if view.composer_domain_intent(action.clone()) {
                                    cx.notify();
                                }
                            });
                        }
                        ThreadScreenEvent::EditMessage(message) => {
                            view.composer.update(cx, |view, cx| {
                                view.start_composer_message_edit(message.clone(), window, cx)
                            })
                        }
                        ThreadScreenEvent::CancelMessageEdit => view
                            .composer
                            .update(cx, |view, cx| view.cancel_composer_message_edit(window, cx)),
                        ThreadScreenEvent::OpenArtifact(artifact) => view
                            .panels
                            .update(cx, |view, cx| view.open_artifact(artifact.clone(), cx)),
                        ThreadScreenEvent::OpenTaskThread {
                            child_thread_id,
                            title,
                        } => cx.emit(ThreadNavigationEvent::OpenTaskThread {
                            parent_thread_id: view.thread_id.clone(),
                            child_thread_id: child_thread_id.clone(),
                            title: title.clone(),
                        }),
                        ThreadScreenEvent::OpenMcpServer(server_id) => {
                            cx.emit(ThreadNavigationEvent::OpenMcpServer {
                                server_id: server_id.clone(),
                            })
                        }
                        ThreadScreenEvent::MessageHistory { thread_id, turn_id } => {
                            view.message_history =
                                Some(crate::message_history::MessageRevisionView::new(
                                    view.client.clone(),
                                    thread_id.clone(),
                                    turn_id.clone(),
                                    view.registrar.clone(),
                                    window,
                                    cx,
                                ))
                        }
                    },
                ),
            ];
            Self {
                thread_id: config.thread_id,
                screen,
                header,
                composer,
                panels,
                footer,
                layout,
                files: config.files,
                external: config.external,
                mount,
                visible: true,
                client: config.client,
                registrar: config.registrar,
                message_history: None,
                _subscriptions: subscriptions,
            }
        })
    }
}
impl Render for ThreadView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let layout = self.layout.read(cx);
        let open = layout.is_open();
        let width = layout.width();
        let layout = self.layout.downgrade();
        let body = v_flex()
            .size_full()
            .min_w_0()
            .min_h_0()
            .bg(cx.theme().background)
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .min_h_0()
                    .overflow_hidden()
                    .child(div().flex_1().min_h_0().child(self.screen.clone()))
                    .child(self.composer.clone()),
            );
        let split = h_resizable("thread-artifacts-layout")
            .on_resize(move |state, _, cx| {
                if let Some(width) = state.read(cx).sizes().get(1).copied() {
                    let _ = layout.update(cx, |layout, cx| layout.resize(width, cx));
                }
            })
            .child(
                resizable_panel()
                    .size_range(px(360.)..Pixels::MAX)
                    .child(body),
            )
            .child(
                resizable_panel()
                    .visible(open)
                    .size(width)
                    .size_range(px(320.)..px(640.))
                    .child(self.panels.clone()),
            );
        v_flex()
            .size_full()
            .min_w_0()
            .min_h_0()
            .bg(cx.theme().background)
            .child(self.header.clone())
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .w_full()
                    .overflow_hidden()
                    .child(split),
            )
            .child(self.footer.clone())
    }
}
impl Drop for ThreadView {
    fn drop(&mut self) {
        self.files.retire_mount(&self.thread_id, self.mount);
        self.external.retire_mount(&self.thread_id, self.mount);
    }
}

#[cfg(test)]
mod tests {
    use super::{ThreadView, ThreadViewConfig};
    use crate::{
        panel_layout::{ThreadPanelKind, ThreadPanelLayoutStore},
        test_support::*,
    };
    use gpui_kit::{
        AppContext, Context, Entity, IntoElement, ParentElement, Render, Styled, TestAppContext,
        Window, component::Root, div, px,
    };
    use pioneer_client::core::ClientCore;
    use std::sync::Arc;
    struct Host(Option<Entity<ThreadView>>);
    impl Render for Host {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().size_full().children(self.0.clone())
        }
    }
    #[gpui_kit::test]
    fn mounted_root_keeps_window_layout_and_replaces_all_thread_content(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "a", "A text");
        install_thread_timeline(&client, "b", "B text");
        let (registrar, deliver) = binding_router(client.clone());
        let make_config = |thread: &str| {
            ThreadViewConfig::new(
                client.clone(),
                thread.into(),
                registrar.clone(),
                Arc::new(ThreadPorts),
                Arc::new(ThreadPorts),
                Arc::new(ThreadPorts),
            )
        };
        let (root, cx) = cx.add_window_view(|window, cx| {
            let thread = ThreadView::new(make_config("a"), window, cx);
            let host = cx.new(|_| Host(Some(thread)));
            Root::new(host, window, cx)
        });
        deliver();
        cx.run_until_parked();
        let host = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<Host>().unwrap()
        });
        let a = host.read_with(cx, |host, _| host.0.clone().unwrap());
        let (weak_root, weak_screen, weak_composer, weak_panels, weak_header, weak_footer) = a
            .read_with(cx, |view, cx| {
                assert_eq!(view.screen.read(cx).thread_id, "a");
                let model = view
                    .screen
                    .read(cx)
                    .semantic_timeline_render_model(Some("a"));
                assert_eq!(
                    model.item_presentations.values().next().unwrap().text,
                    "A text"
                );
                (
                    a.downgrade(),
                    view.screen.downgrade(),
                    view.composer.downgrade(),
                    view.panels.downgrade(),
                    view.header.downgrade(),
                    view.footer.downgrade(),
                )
            });
        cx.update(|window, cx| {
            a.read(cx)
                .screen
                .read(cx)
                .thread_timeline_view_state
                .borrow_mut()
                .autoscroll_paused_by_user = true;
            a.update(cx, |view, cx| view.set_visible(false, window, cx));
            host.update(cx, |host, cx| {
                host.0 = None;
                cx.notify();
            });
        });
        deliver();
        cx.run_until_parked();
        assert!(weak_root.upgrade().is_some());
        assert!(weak_screen.upgrade().is_some());
        cx.update(|window, cx| {
            assert!(
                a.read(cx)
                    .screen
                    .read(cx)
                    .thread_timeline_view_state
                    .borrow()
                    .autoscroll_paused_by_user
            );
            a.update(cx, |view, cx| view.set_visible(true, window, cx));
            host.update(cx, |host, cx| {
                host.0 = Some(a.clone());
                cx.notify();
            });
        });
        deliver();
        cx.run_until_parked();
        let notifications = std::rc::Rc::new(std::cell::Cell::new((0, 0)));
        let observers = cx.update(|_, cx| {
            let screen = a.read(cx).screen.clone();
            let root_notifications = notifications.clone();
            let screen_notifications = notifications.clone();
            [
                cx.observe(&a, move |_, _| {
                    let (root, screen) = root_notifications.get();
                    root_notifications.set((root + 1, screen));
                }),
                cx.observe(&screen, move |_, _| {
                    let (root, screen) = screen_notifications.get();
                    screen_notifications.set((root, screen + 1));
                }),
            ]
        });
        let input = client.composer_snapshot("a").unwrap();
        client.composer_intent(pioneer_client::composer::store::ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id: input.draft_id(),
            text: "typed text".into(),
        });
        deliver();
        cx.run_until_parked();
        assert_eq!(
            notifications.get(),
            (0, 0),
            "controlled composer text notified the root or timeline"
        );
        drop(observers);
        cx.update(|window, cx| {
            let layout = ThreadPanelLayoutStore::for_window(window, cx);
            layout.update(cx, |layout, cx| {
                layout.open(ThreadPanelKind::Members, cx);
                layout.resize(px(412.), cx);
            });
            let b = ThreadView::new(make_config("b"), window, cx);
            host.update(cx, |host, cx| {
                host.0 = Some(b);
                cx.notify();
            });
        });
        drop(a);
        deliver();
        cx.run_until_parked();
        cx.update(|window, cx| {
            window.draw(cx).clear(cx);
        });
        cx.run_until_parked();
        assert!(weak_root.upgrade().is_none());
        assert!(weak_screen.upgrade().is_none());
        assert!(weak_composer.upgrade().is_none());
        assert!(weak_panels.upgrade().is_none());
        assert!(weak_header.upgrade().is_none());
        assert!(weak_footer.upgrade().is_none());
        host.read_with(cx, |host, cx| {
            let b = host.0.as_ref().unwrap().read(cx);
            assert_eq!(b.thread_id, "b");
            assert_eq!(b.screen.read(cx).thread_id, "b");
            assert_eq!(b.layout.read(cx).width(), px(412.));
            assert!(b.layout.read(cx).is_visible(ThreadPanelKind::Members));
            assert!(
                b.screen
                    .read(cx)
                    .thread_bindings
                    .timeline_model(Some("a"))
                    .is_none()
            );
            assert_eq!(
                b.screen
                    .read(cx)
                    .semantic_timeline_render_model(Some("b"))
                    .item_presentations
                    .values()
                    .next()
                    .unwrap()
                    .text,
                "B text"
            );
        });
        host.update(cx, |host, cx| {
            host.0 = None;
            cx.notify();
        });
        cx.run_until_parked();
    }
}
