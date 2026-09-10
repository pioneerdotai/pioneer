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
    screen::{ThreadScreenEvent, TimelineView},
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
    screen: Entity<TimelineView>,
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
            if !active {
                screen.set_row_activities_visible(&std::collections::HashSet::new(), cx);
            }
            screen.avatar_activities.borrow_mut().set_active(active, cx);
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
        let screen = TimelineView::new(
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
                cx.observe_in(&layout, window, |view, _, window, cx| {
                    view.screen.update(cx, |timeline, cx| {
                        crate::timeline::controller::DesktopTimelineController::schedule(
                            timeline, window, cx,
                        )
                    });
                    cx.notify();
                }),
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
    fn new_thread_draws_empty_timeline_and_editable_composer_before_bootstrap(
        cx: &mut TestAppContext,
    ) {
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        assert!(client.navigation_snapshot().active_thread_id().is_none());
        let id = client.prepare_thread_draft().unwrap();
        let (registrar, deliver) = binding_router(client.clone());
        let (root, cx) = cx.add_window_view(|window, cx| {
            let thread = ThreadView::new(
                ThreadViewConfig::new(
                    client.clone(),
                    id.clone(),
                    registrar,
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                ),
                window,
                cx,
            );
            Root::new(thread, window, cx)
        });
        deliver();
        cx.run_until_parked();
        cx.update(|window, cx| window.draw(cx).clear(cx));
        for selector in ["thread-empty-timeline", "thread-composer"] {
            let bounds = cx
                .debug_bounds(selector)
                .expect("new-thread surface must be drawn");
            assert!(
                bounds.size.width > px(0.) && bounds.size.height > px(0.),
                "{selector}"
            );
        }
        let thread = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<ThreadView>().unwrap()
        });
        // Connecting the selected Gateway starts an authorization epoch and
        // clears pre-session composer publications while this draft stays mounted.
        client.begin_authorization_epoch(Some(("synthetic-endpoint".into(), 1)));
        deliver();
        cx.run_until_parked();
        assert!(client.composer_snapshot(&id).is_some());
        cx.update(|window, cx| {
            let composer = thread.read(cx).composer.clone();
            composer.update(cx, |composer, cx| composer.focus(window, cx));
        });
        cx.simulate_input("Before bootstrap");
        deliver();
        cx.run_until_parked();
        assert_eq!(
            client.composer_snapshot(&id).unwrap().draft().text,
            "Before bootstrap"
        );
        client.activate_thread(None, Some("workspace"));
        deliver();
        cx.run_until_parked();
        cx.update(|window, cx| window.draw(cx).clear(cx));
        assert!(cx.debug_bounds("thread-composer").is_some());
        assert_eq!(
            client.composer_snapshot(&id).unwrap().draft().text,
            "Before bootstrap"
        );
        assert!(client.navigation_snapshot().active_thread_id().is_none());
        assert!(!client.thread_start_snapshot().in_progress);
        assert!(client.thread_coordinator_snapshot(&id).is_none());
    }

    #[gpui_kit::test]
    fn command_publications_publish_row_owned_terminal_through_completion_and_retirement(
        cx: &mut TestAppContext,
    ) {
        use pioneer_client::timeline::semantic::{TopLevelPageMergeMode, WorkPageMergeMode};
        // The public TerminalView seam reads this synthetic stream on an OS thread.
        cx.background_executor.allow_parking();
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "a", "message");
        let work = serde_json::json!({
            "turnId":"turn", "presentation":"expanded_terminal_no_final", "state":"completed",
            "workCount":1,"visibleWorkCount":1,"hiddenWorkCount":0,
            "hasMoreBefore":false,"hasMoreAfter":false
        });
        client.apply_thread_timeline_page(
            serde_json::from_value(serde_json::json!({
                "workspaceId":"workspace","threadId":"a","projectionVersion":1,
                "blocks":[{"workspaceId":"workspace","threadId":"a","blockId":"work-block",
                    "turnId":"turn","sortKey":"1","kind":{"kind":"turn_work","work":work}}],
                "page":{"hasMoreBefore":false,"hasMoreAfter":false}
            }))
            .unwrap(),
            TopLevelPageMergeMode::Reset,
        );
        let publish = |revision: i64, output: &str, active: bool| {
            client.apply_turn_work_page(serde_json::from_value(serde_json::json!({
                "workspaceId":"workspace","threadId":"a","turnId":"turn","projectionVersion":1,
                "sourceHighWatermark":revision,"projectionUpdatedAtUnixMicros":revision,"work":work,
                "items":[{"workItemId":"work-id","itemId":"command-id","turnId":"turn","orderKey":"1",
                    "sourceSequence":revision,"sourceUpdatedAtUnixMicros":revision,
                    "itemType":"command_execution","status":if active {"running"} else {"completed"},
                    "item":{"type":"commandExecution","id":"command-id","toolName":"exec_command",
                        "arguments":{},"status":if active {"in_progress"} else {"completed"},"command":["synthetic"],
                        "outputPolicy":{"llm":{"mode":"summary_only"},"llmRetention":{"mode":"do_not_retain"},
                            "timeline":{"mode":"full","max_bytes":24000},"storage":{"mode":"none"},
                            "recovery":{"mode":"none"},"deltas":{"mode":"disabled"}},
                        "display":{"kind":"shell","stdout":output,"truncated":false},"storage":{"kind":"none"}}
                }],"page":{"hasMoreBefore":false,"hasMoreAfter":false}
            })).unwrap(), WorkPageMergeMode::Reset);
            // Flush the copied publication without starting a Client worker.
            let _flush = client.subscribe(
                pioneer_client::core::ClientScope::Timeline {
                    thread_id: "a".into(),
                },
                std::num::NonZeroUsize::new(8).unwrap(),
            );
        };
        publish(1, "streaming", true);
        client.set_thread_turn_work_expanded("a", "turn", true);
        cx.update(gpui_kit::init);
        let (registrar, deliver) = binding_router(client.clone());
        let (root, cx) = cx.add_window_view(|window, cx| {
            let thread = ThreadView::new(
                ThreadViewConfig::new(
                    client.clone(),
                    "a".into(),
                    registrar,
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                ),
                window,
                cx,
            );
            Root::new(thread, window, cx)
        });
        deliver();
        cx.run_until_parked();
        let thread = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<ThreadView>().unwrap()
        });
        let timeline = thread.read_with(cx, |thread, _| thread.screen.clone());
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                crate::timeline::controller::DesktopTimelineController::dispatch(
                    view,
                    &crate::timeline::controller::TimelineAction::Expand {
                        entry_id: "work-id".into(),
                    },
                    window,
                    cx,
                );
            })
        });
        for _ in 0..3 {
            cx.update(|window, cx| window.draw(cx).clear(cx));
            cx.run_until_parked();
        }
        let row_terminal = |cx: &mut gpui_kit::VisualTestContext| {
            // This fixture drives publications without an authenticated shell;
            // provide the viewport membership explicitly.
            timeline.update(cx, |view, cx| {
                view.set_row_terminals_visible(&["work-id".to_owned()].into_iter().collect(), cx);
            });
            timeline.read_with(cx, |view, cx| {
                let slot = view
                    .thread_timeline_view_state
                    .prepared
                    .as_ref()
                    .unwrap()
                    .slots
                    .iter()
                    .find(|slot| slot.snapshot().id().as_str() == "work-id")
                    .unwrap_or_else(|| {
                        panic!(
                            "semantic command row missing: {:?}",
                            view.thread_timeline_view_state
                                .prepared
                                .as_ref()
                                .unwrap()
                                .model
                                .rows
                        )
                    });
                let terminal = slot.terminal_for_test(cx).expect("visible terminal child");
                let registered = view.row_registry.get(slot.snapshot().id()).unwrap();
                if Arc::ptr_eq(registered.snapshot(), slot.snapshot()) {
                    assert_eq!(
                        registered.terminal_for_test(cx).unwrap().entity_id(),
                        terminal.entity_id()
                    );
                }
                terminal.clone()
            })
        };
        let active = row_terminal(cx);
        timeline.read_with(cx, |view, _| {
            assert!(
                view.thread_timeline_terminal_item
                    .borrow()
                    .entries
                    .contains_key("work-id")
            )
        });
        publish(2, "final", false);
        deliver();
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                crate::timeline::controller::DesktopTimelineController::reconcile_publication(
                    view, window, cx,
                );
                assert!(view.measurement_coordinator.draw.is_some());
                let slot = view
                    .thread_timeline_view_state
                    .prepared
                    .as_ref()
                    .unwrap()
                    .slots
                    .iter()
                    .find(|slot| slot.snapshot().id().as_str() == "work-id")
                    .unwrap();
                assert_eq!(
                    slot.terminal_for_test(cx).unwrap().entity_id(),
                    active.entity_id()
                );
            });
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        let completed = row_terminal(cx);
        assert_eq!(completed.entity_id(), active.entity_id());
        timeline.read_with(cx, |view, _| {
            assert!(
                view.thread_timeline_terminal_item
                    .borrow()
                    .entries
                    .is_empty()
            )
        });
        publish(3, "late final", false);
        deliver();
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                crate::timeline::controller::DesktopTimelineController::reconcile_publication(
                    view, window, cx,
                );
                assert!(view.measurement_coordinator.draw.is_some());
                let slot = view
                    .thread_timeline_view_state
                    .prepared
                    .as_ref()
                    .unwrap()
                    .slots
                    .iter()
                    .find(|slot| slot.snapshot().id().as_str() == "work-id")
                    .unwrap();
                assert_eq!(
                    slot.terminal_for_test(cx).unwrap().entity_id(),
                    completed.entity_id()
                );
            });
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        let replaced = row_terminal(cx);
        assert_ne!(replaced.entity_id(), completed.entity_id());
        publish(4, "late final", false);
        deliver();
        cx.run_until_parked();
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        assert_eq!(row_terminal(cx).entity_id(), replaced.entity_id());
        let weak = replaced.downgrade();
        drop(replaced);
        timeline.update(cx, |view, cx| view.retire_timeline_rows(cx));
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
    }
    #[gpui_kit::test]
    fn timeline_reconciles_publications_before_composition_and_equal_input_is_quiet(
        cx: &mut TestAppContext,
    ) {
        use std::{cell::Cell, rc::Rc};
        cx.update(gpui_kit::init);
        let client = Arc::new(ClientCore::new());
        install_thread_timeline(&client, "a", "A text");
        let (registrar, deliver) = binding_router(client.clone());
        let (root, cx) = cx.add_window_view(|window, cx| {
            let thread = ThreadView::new(
                ThreadViewConfig::new(
                    client.clone(),
                    "a".into(),
                    registrar,
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                    Arc::new(ThreadPorts),
                ),
                window,
                cx,
            );
            Root::new(thread, window, cx)
        });
        deliver();
        cx.run_until_parked();
        let thread = root.read_with(cx, |root, _| {
            root.view().clone().downcast::<ThreadView>().unwrap()
        });
        let timeline = thread.read_with(cx, |thread, _| thread.screen.clone());
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        let notifications = Rc::new(Cell::new(0));
        let subscription = cx.update(|_, cx| {
            let notifications = notifications.clone();
            cx.observe(&timeline, move |_, _| {
                notifications.set(notifications.get() + 1)
            })
        });
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                assert!(
                    !crate::timeline::controller::DesktopTimelineController::reconcile_publication(
                        view, window, cx
                    )
                );
                let revision = view.thread_timeline_view_state.model.revision;
                let viewport_revision = view.thread_timeline_view_state.viewport_revision;
                let prepared = view
                    .thread_timeline_view_state
                    .prepared
                    .as_ref()
                    .unwrap()
                    .item_sizes
                    .clone();
                drop(view.render(window, cx));
                assert_eq!(view.thread_timeline_view_state.model.revision, revision);
                assert_eq!(
                    view.thread_timeline_view_state.viewport_revision,
                    viewport_revision
                );
                assert!(Rc::ptr_eq(
                    &prepared,
                    &view
                        .thread_timeline_view_state
                        .prepared
                        .as_ref()
                        .unwrap()
                        .item_sizes
                ));
            })
        });
        cx.run_until_parked();
        assert_eq!(notifications.get(), 0);
        drop(subscription);

        // A completed draw can be superseded before its deferred measurement
        // commits. Only the newest layout generation may replace the snapshot.
        let previous_sizes = timeline.read_with(cx, |view, _| {
            view.thread_timeline_view_state
                .prepared
                .as_ref()
                .unwrap()
                .item_sizes
                .clone()
        });
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                {
                    let mut state = view.thread_timeline_view_state.borrow_mut();

                    // A follow request from the preceding content publication.
                    state.pending_follow_bottom = true;
                }
                view.layout_store.theme_revision += 1;
                view.layout_store.context_changed();
                view.reconcile_timeline(window, cx);
                cx.notify();
            });
            window.draw(cx).clear(cx);
            timeline.update(cx, |view, cx| {
                view.reconcile_timeline(window, cx);
            });
        });
        cx.run_until_parked();
        timeline.read_with(cx, |view, _| {
            assert!(Rc::ptr_eq(
                &previous_sizes,
                &view
                    .thread_timeline_view_state
                    .prepared
                    .as_ref()
                    .unwrap()
                    .item_sizes
            ));
            assert!(view.measurement_coordinator.draw.is_some());
            assert!(
                view.thread_timeline_view_state
                    .borrow()
                    .pending_follow_bottom
            );
        });
        // A gesture between measure and commit wins over the earlier follow.
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                crate::timeline::controller::DesktopTimelineController::dispatch(
                    view,
                    &crate::timeline::controller::TimelineAction::Scroll { delta_y: px(12.) },
                    window,
                    cx,
                );
                assert!(
                    !view
                        .thread_timeline_view_state
                        .borrow()
                        .pending_follow_bottom
                );
            })
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        timeline.read_with(cx, |view, _| {
            assert!(view.measurement_coordinator.draw.is_none());
            let current = &view
                .thread_timeline_view_state
                .prepared
                .as_ref()
                .unwrap()
                .item_sizes;
            assert!(!Rc::ptr_eq(&previous_sizes, current));
            assert_eq!(previous_sizes.as_ref(), current.as_ref());
        });

        // Transient expansion must not paint with the preceding collapsed layout.
        let row_id = timeline.read_with(cx, |view, _| {
            view.thread_timeline_view_state
                .prepared
                .as_ref()
                .unwrap()
                .slots[0]
                .snapshot()
                .id()
                .as_str()
                .to_owned()
        });
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                assert!(
                    !view
                        .thread_timeline_view_state
                        .prepared
                        .as_ref()
                        .unwrap()
                        .row_inputs[0]
                        .0
                        .expanded
                );
                crate::timeline::controller::DesktopTimelineController::expand(
                    view, &row_id, window, cx,
                );
                assert!(view.measurement_coordinator.draw.is_some());
                assert!(
                    !view
                        .thread_timeline_view_state
                        .prepared
                        .as_ref()
                        .unwrap()
                        .row_inputs[0]
                        .0
                        .expanded
                );
            })
        });
        cx.update(|window, cx| window.draw(cx).clear(cx));
        cx.run_until_parked();
        timeline.read_with(cx, |view, _| {
            assert!(view.measurement_coordinator.draw.is_none());
            assert!(
                view.thread_timeline_view_state
                    .prepared
                    .as_ref()
                    .unwrap()
                    .row_inputs[0]
                    .0
                    .expanded
            );
        });

        // Revocation between draw and deferred commit retires the complete row closure.
        cx.update(|window, cx| {
            timeline.update(cx, |view, cx| {
                crate::timeline::controller::DesktopTimelineController::expand(
                    view, &row_id, window, cx,
                )
            });
            window.draw(cx).clear(cx);
            timeline.update(cx, |view, cx| view.retire_timeline_rows(cx));
        });
        cx.run_until_parked();
        timeline.read_with(cx, |view, _| {
            assert!(view.thread_timeline_view_state.prepared.is_none());
            assert!(view.row_registry.slots().is_empty());
            assert_eq!(view.layout_store.index.borrow().len(), 0);
            assert!(view.markdown_highlights.borrow().is_empty());
            assert!(
                view.thread_timeline_terminal_item
                    .borrow()
                    .entries
                    .is_empty()
            );
            assert!(view.measurement_coordinator.draw.is_none());
        });
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
                    .thread_timeline_view_state
                    .model
                    .clone();
                assert_eq!(
                    model
                        .item_presentations
                        .values()
                        .next()
                        .unwrap()
                        .content()
                        .unwrap()
                        .text,
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
        // Complete the remount draw transaction before measuring unrelated publications.
        cx.update(|window, cx| window.draw(cx).clear(cx));
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
                    .thread_timeline_view_state
                    .model
                    .clone()
                    .item_presentations
                    .values()
                    .next()
                    .unwrap()
                    .content()
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

#[cfg(test)]
impl ThreadView {
    pub(crate) fn timeline_for_test(&self) -> Entity<crate::screen::TimelineView> {
        self.screen.clone()
    }
}
