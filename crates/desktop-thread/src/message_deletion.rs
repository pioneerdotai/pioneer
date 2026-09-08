use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::{ClientCore, ClientPublicationReference, ClientScope},
    threads::message_deletion::{
        MessageDeletionIntent, MessageDeletionPlan, MessageDeletionPublication,
        MessageDeletionState,
    },
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{
    cell::{Cell, RefCell},
    sync::Arc,
};

struct DeletionBinding {
    scope: ClientScope,
    sequence: Cell<u64>,
    input: RefCell<Option<Arc<MessageDeletionPublication>>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl ClientPublicationSink for DeletionBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        if publication.scope() != &self.scope
            || publication.snapshot().sequence().get() <= self.sequence.get()
        {
            return;
        }
        self.sequence.set(publication.snapshot().sequence().get());
        *self.input.borrow_mut() = publication
            .typed::<MessageDeletionPublication>()
            .map(|p| p.payload());
        self.changed.send_modify(|v| *v = v.saturating_add(1));
    }
}

pub(crate) struct MessageDeletionView {
    client: Arc<ClientCore>,
    plan: MessageDeletionPlan,
    binding: Arc<DeletionBinding>,
    _registration: ClientBindingRegistration,
    _changes: Task<()>,
    prompt: Option<Task<()>>,
    presented_failure: Option<u64>,
}
impl MessageDeletionView {
    pub(crate) fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        plan: MessageDeletionPlan,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx: &mut Context<Self>| {
            let scope = ClientScope::MessageDeletion {
                thread_id: plan.identity.thread_id.clone(),
            };
            let binding = Arc::new(DeletionBinding {
                scope: scope.clone(),
                sequence: Cell::new(0),
                input: RefCell::new(client.message_deletion_snapshot(&plan.identity.thread_id)),
                changed: tokio::sync::watch::channel(0).0,
            });
            let mut changes = binding.changed.subscribe();
            let sink: Arc<dyn ClientPublicationSink> = binding.clone();
            let registration = registrar.register(scope, Arc::downgrade(&sink));
            let task = cx.spawn_in(window, async move |view, cx| {
                while changes.changed().await.is_ok() {
                    let _ = *changes.borrow_and_update();
                    if view
                        .update_in(cx, |view, window, cx| view.reconcile(window, cx))
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let answer = window.prompt(
                PromptLevel::Warning,
                t!("timeline.message.delete_title").to_string().as_str(),
                Some(
                    t!("timeline.message.delete_description")
                        .to_string()
                        .as_str(),
                ),
                &[
                    PromptButton::new(t!("timeline.message.delete_action").to_string()),
                    PromptButton::cancel(t!("buttons.cancel").to_string()),
                ],
                cx,
            );
            let prompt = cx.spawn_in(window, async move |view, cx| {
                let accepted = answer.await == Ok(0);
                let _ = view.update_in(cx, |view, window, cx| {
                    let intent = if accepted {
                        MessageDeletionIntent::Confirm {
                            identity: view.plan.identity.clone(),
                        }
                    } else {
                        MessageDeletionIntent::Cancel {
                            identity: view.plan.identity.clone(),
                        }
                    };
                    view.client.message_deletion_intent(intent);
                    *view.binding.input.borrow_mut() = view
                        .client
                        .message_deletion_snapshot(&view.plan.identity.thread_id);
                    view.reconcile(window, cx);
                });
            });
            Self {
                client,
                plan,
                binding,
                _registration: registration,
                _changes: task,
                prompt: Some(prompt),
                presented_failure: None,
            }
        })
    }
    fn reconcile(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let input = self.binding.input.borrow().clone();
        let Some(input) = input.filter(|p| p.plan.identity == self.plan.identity) else {
            self.prompt.take();
            cx.notify();
            return;
        };
        if input.state == MessageDeletionState::Cancelled {
            self.prompt.take();
        }
        if let MessageDeletionState::Failed { conflicted } = input.state {
            if self.presented_failure != Some(input.request_generation) {
                self.presented_failure = Some(input.request_generation);
                let message = if conflicted {
                    t!("timeline.message.delete_conflict").to_string()
                } else {
                    t!("timeline.message.delete_failed").to_string()
                };
                let _ = window.prompt(
                    PromptLevel::Warning,
                    t!("timeline.message.delete_title").to_string().as_str(),
                    Some(message.as_str()),
                    &[PromptButton::ok(t!("buttons.ok").to_string())],
                    cx,
                );
            }
        }
        cx.notify();
    }
}
impl Drop for MessageDeletionView {
    fn drop(&mut self) {
        self.prompt.take();
        self.client
            .message_deletion_intent(MessageDeletionIntent::Cancel {
                identity: self.plan.identity.clone(),
            });
    }
}

#[cfg(test)]
mod tests {
    use super::MessageDeletionView;
    use gpui_kit::{AppContext, Context, IntoElement, Render, TestAppContext, Window, div};
    struct Host;
    impl Render for Host {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div()
        }
    }
    use pioneer_client::{
        core::ClientCore,
        threads::message_deletion::{MessageDeletionIdentity, MessageDeletionPlan},
    };
    use std::sync::Arc;

    #[gpui_kit::test]
    fn dropping_prompt_owner_releases_binding_and_ignores_a_late_native_answer(
        cx: &mut TestAppContext,
    ) {
        let (_, window) = cx.add_window_view(|_, _| Host);
        let core = Arc::new(ClientCore::new());
        let (registrar, deliver) = crate::test_support::binding_router(core.clone());
        let weak = window.update(|window, cx| {
            let view = MessageDeletionView::new(
                core.clone(),
                registrar,
                MessageDeletionPlan {
                    identity: MessageDeletionIdentity {
                        thread_id: "a".into(),
                        generation: 7,
                    },
                    workspace_id: "workspace".into(),
                    turn_id: "turn".into(),
                    expected_revision: 3,
                },
                window,
                cx,
            );
            deliver();
            let weak = view.downgrade();
            drop(view);
            weak
        });
        window.run_until_parked();
        assert!(weak.upgrade().is_none());
        // TestPlatform owns the synthetic prompt; answering it has no native effect.
        window.simulate_prompt_answer(&t!("timeline.message.delete_action").to_string());
        window.run_until_parked();
        assert!(core.message_deletion_snapshot("a").is_none());
    }
}
