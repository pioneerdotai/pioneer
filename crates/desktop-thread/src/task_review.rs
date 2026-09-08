use gpui_kit::component::{
    button::{Button, ButtonVariants},
    dialog::DialogFooter,
    form::{field, v_form},
    h_flex,
    input::{Textarea, TextareaState},
    v_flex, *,
};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    core::{ClientCore, ClientPublicationReference, ClientScope},
    tasks::{
        review as task_review,
        review_controller::{
            TaskReviewFailure, TaskReviewIntent, TaskReviewPublication, TaskReviewRequestState,
        },
    },
    timeline::labels::{TaskWaitReviewDisplayItem, task_review_button_id},
};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};

actions!(
    thread_task_review,
    [AcceptReview, RequestReviewRevision, CancelReview]
);

struct ReviewBinding {
    scope: ClientScope,
    sequence: Cell<u64>,
    input: RefCell<Option<Arc<TaskReviewPublication>>>,
    changed: tokio::sync::watch::Sender<u64>,
}
impl ClientPublicationSink for ReviewBinding {
    fn publish(&self, publication: ClientPublicationReference) {
        if publication.scope() != &self.scope
            || publication.snapshot().sequence().get() <= self.sequence.get()
        {
            return;
        }
        self.sequence.set(publication.snapshot().sequence().get());
        let Some(input) = publication
            .typed::<TaskReviewPublication>()
            .map(|p| p.payload())
        else {
            if self.input.borrow_mut().take().is_some() {
                self.changed.send_modify(|v| *v = v.saturating_add(1));
            }
            return;
        };
        if self
            .input
            .borrow()
            .as_ref()
            .is_some_and(|old| old.revision >= input.revision)
        {
            return;
        }
        *self.input.borrow_mut() = Some(input);
        self.changed.send_modify(|v| *v = v.saturating_add(1));
    }
}

pub(crate) struct TaskReviewActionView {
    client: Arc<ClientCore>,
    thread_id: String,
    candidate_id: String,
    candidate_label: String,
    binding: Arc<ReviewBinding>,
    _registration: ClientBindingRegistration,
    _binding_task: Task<()>,
    _release: Subscription,
    focus: FocusHandle,
    dialog_open: Rc<Cell<bool>>,
}
impl TaskReviewActionView {
    pub(crate) fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        thread_id: String,
        candidate_id: String,
        candidate_label: String,
        window: &mut Window,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx: &mut Context<Self>| {
            let scope = ClientScope::TaskReview {
                thread_id: thread_id.clone(),
                candidate_id: candidate_id.clone(),
            };
            client.task_review_intent(TaskReviewIntent::Observe {
                thread_id: thread_id.clone(),
                candidate_id: candidate_id.clone(),
            });
            let binding = Arc::new(ReviewBinding {
                scope: scope.clone(),
                sequence: Cell::new(0),
                input: RefCell::new(client.task_review_snapshot(&thread_id, &candidate_id)),
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
                            if view
                                .binding
                                .input
                                .borrow()
                                .as_ref()
                                .is_none_or(|p| p.visible_actions.is_empty())
                            {
                                view.dismiss_dialog(window, cx);
                            }
                            cx.notify();
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let release =
                cx.on_release_in(window, |view, window, cx| view.dismiss_dialog(window, cx));
            Self {
                client,
                thread_id,
                candidate_id,
                candidate_label,
                binding,
                _registration: registration,
                _binding_task: task,
                _release: release,
                focus: cx.focus_handle(),
                dialog_open: Rc::new(Cell::new(false)),
            }
        })
    }
    fn dismiss_dialog(&self, window: &mut Window, cx: &mut App) {
        if self.dialog_open.replace(false) {
            window.close_dialog(cx);
        }
    }
    pub(crate) fn observe(&self) {
        self.client.task_review_intent(TaskReviewIntent::Observe {
            thread_id: self.thread_id.clone(),
            candidate_id: self.candidate_id.clone(),
        });
    }
    fn perform(
        &self,
        action: task_review::TaskReviewAction,
        feedback: Option<String>,
        reason: Option<String>,
    ) {
        self.client.task_review_intent(TaskReviewIntent::Perform {
            thread_id: self.thread_id.clone(),
            candidate_id: self.candidate_id.clone(),
            action,
            feedback,
            reason,
        });
    }
    fn accept(&self) {
        self.perform(
            task_review::TaskReviewAction::Accept,
            None,
            Some("Accepted in desktop".into()),
        );
    }
    fn cancel(&self) {
        self.perform(
            task_review::TaskReviewAction::Cancel,
            None,
            Some("Cancelled during result review".into()),
        );
    }
    fn revise(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.dialog_open.get() {
            return;
        }
        let item = self
            .binding
            .input
            .borrow()
            .as_ref()
            .filter(|p| {
                !p.pending()
                    && p.allowed_actions
                        .contains(&task_review::TaskReviewAction::Revise)
            })
            .and_then(|p| p.item.clone());
        if let Some(item) = item {
            self.open_task_review_revise_dialog(item, window, cx);
        }
    }
    fn task_review_plan_error_message(error: task_review::TaskReviewPlanError) -> String {
        match error {
            task_review::TaskReviewPlanError::BlankFeedback => {
                t!("timeline.task_review.error.feedback_required").to_string()
            }
            task_review::TaskReviewPlanError::MissingRunId
            | task_review::TaskReviewPlanError::MissingTaskId
            | task_review::TaskReviewPlanError::MissingCandidateId => {
                t!("timeline.task_review.error.target_incomplete").to_string()
            }
            task_review::TaskReviewPlanError::UserControlsNotAllowed
            | task_review::TaskReviewPlanError::ActionNotAllowed { .. } => {
                t!("timeline.task_review.error.action_unavailable").to_string()
            }
        }
    }

    fn open_task_review_revise_dialog(
        &mut self,
        item: TaskWaitReviewDisplayItem,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if item.run_id.is_none() {
            return;
        }

        let feedback_state = cx.new(|cx| {
            TextareaState::new(window, cx)
                .auto_grow(3, 8)
                .placeholder(t!("timeline.task_review.revision_feedback_placeholder").to_string())
        });
        let field_error: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
        let owner = cx.weak_entity();
        self.dialog_open.set(true);
        let dialog_open = self.dialog_open.clone();

        let submit_revision: Rc<dyn Fn(&mut App) -> bool> = Rc::new({
            let feedback_state = feedback_state.clone();
            let field_error = field_error.clone();
            move |cx| {
                let feedback = match task_review::validate_revision_feedback(
                    feedback_state.read(cx).value().as_str(),
                ) {
                    Ok(feedback) => feedback,
                    Err(error) => {
                        *field_error.borrow_mut() =
                            Some(Self::task_review_plan_error_message(error));
                        return false;
                    }
                };
                *field_error.borrow_mut() = None;
                owner
                    .update(cx, |view, _| {
                        view.perform(task_review::TaskReviewAction::Revise, Some(feedback), None);
                    })
                    .is_ok()
            }
        });

        window.open_dialog(cx, move |dialog, window, cx| {
            feedback_state.update(cx, |state, cx| state.focus(window, cx));
            let error = field_error.borrow().clone();
            let can_submit =
                task_review::validate_revision_feedback(feedback_state.read(cx).value().as_str())
                    .is_ok();
            dialog
                .w(px(420.))
                .gap_1()
                .rounded_2xl()
                .close_button(true)
                .overlay_closable(true)
                .keyboard(true)
                .title(
                    div()
                        .text_base()
                        .font_semibold()
                        .child(t!("timeline.task_review.request_revision").to_string()),
                )
                .on_close({
                    let dialog_open = dialog_open.clone();
                    move |_, _, _| dialog_open.set(false)
                })
                .on_ok({
                    let submit_revision = submit_revision.clone();
                    move |_, _, cx| submit_revision(cx)
                })
                .footer(DialogFooter::new().children({
                    let submit_revision = submit_revision.clone();
                    vec![
                        Button::new("task-review-revise-cancel")
                            .small()
                            .outline()
                            .label(t!("buttons.cancel").to_string())
                            .on_click({
                                let dialog_open = dialog_open.clone();
                                move |_, window, cx| {
                                    dialog_open.set(false);
                                    window.close_dialog(cx);
                                }
                            })
                            .into_any_element(),
                        Button::new("task-review-revise-submit")
                            .small()
                            .primary()
                            .label(t!("timeline.task_review.request_revision").to_string())
                            .disabled(!can_submit)
                            .on_click({
                                let submit_revision = submit_revision.clone();
                                let dialog_open = dialog_open.clone();
                                move |_, window, cx| {
                                    if submit_revision(cx) {
                                        dialog_open.set(false);
                                        window.close_dialog(cx);
                                    }
                                }
                            })
                            .into_any_element(),
                    ]
                }))
                .child(
                    v_form()
                        .child(
                            field()
                                .label(t!("timeline.task_review.feedback_label").to_string())
                                .child(Textarea::new(&feedback_state).min_w_0()),
                        )
                        .when_some(error, |this, error| {
                            this.child(
                                field().label_indent(false).child(
                                    div()
                                        .text_sm()
                                        .line_height(relative(1.35))
                                        .text_color(cx.theme().danger)
                                        .whitespace_normal()
                                        .child(error),
                                ),
                            )
                        }),
                )
        });
    }
}
impl Render for TaskReviewActionView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let input = self.binding.input.borrow().clone();
        let Some(input) = input.filter(|p| !p.visible_actions.is_empty()) else {
            return div().into_any_element();
        };
        let enabled = |action| !input.pending() && input.allowed_actions.contains(&action);
        let error = match &input.request {
            TaskReviewRequestState::Failed {
                error: TaskReviewFailure::Plan { error },
            } => Some(Self::task_review_plan_error_message(*error)),
            TaskReviewRequestState::Failed {
                error: TaskReviewFailure::Transport { message },
            } => Some(message.clone()),
            TaskReviewRequestState::Failed {
                error: TaskReviewFailure::Unavailable,
            } => Some(t!("timeline.task_review.error.action_unavailable").to_string()),
            _ => None,
        };
        div()
            .w_full()
            .overflow_hidden()
            .rounded_lg()
            .bg(cx.theme().muted)
            .p_3()
            .key_context("ThreadTaskReview")
            .track_focus(&self.focus)
            .on_action(cx.listener(|view, _: &AcceptReview, _, _| view.accept()))
            .on_action(cx.listener(|view, _: &CancelReview, _, _| view.cancel()))
            .on_action(
                cx.listener(|view, _: &RequestReviewRevision, window, cx| view.revise(window, cx)),
            )
            .child(
                v_flex()
                    .w_full()
                    .gap_2()
                    .child(
                        div()
                            .text_xs()
                            .opacity(0.6)
                            .child(self.candidate_label.clone()),
                    )
                    .child(
                        h_flex()
                            .w_full()
                            .flex_wrap()
                            .gap_2()
                            .child(
                                Button::new(task_review_button_id(
                                    &self.candidate_id,
                                    "task-review-accept",
                                ))
                                .small()
                                .primary()
                                .label(t!("timeline.task_review.accept_result").to_string())
                                .disabled(!enabled(task_review::TaskReviewAction::Accept))
                                .on_click(cx.listener(|view, _, _, _| view.accept())),
                            )
                            .child(
                                Button::new(task_review_button_id(
                                    &self.candidate_id,
                                    "task-review-revise",
                                ))
                                .small()
                                .outline()
                                .label(t!("timeline.task_review.request_revision").to_string())
                                .disabled(!enabled(task_review::TaskReviewAction::Revise))
                                .on_click(
                                    cx.listener(|view, _, window, cx| view.revise(window, cx)),
                                ),
                            )
                            .child(
                                Button::new(task_review_button_id(
                                    &self.candidate_id,
                                    "task-review-cancel",
                                ))
                                .small()
                                .danger()
                                .label(t!("timeline.task_review.cancel_task").to_string())
                                .disabled(!enabled(task_review::TaskReviewAction::Cancel))
                                .on_click(cx.listener(|view, _, _, _| view.cancel())),
                            ),
                    )
                    .when_some(error, |this, error| {
                        this.child(
                            div()
                                .text_xs()
                                .line_height(relative(1.35))
                                .text_color(cx.theme().danger)
                                .whitespace_normal()
                                .child(error),
                        )
                    }),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{ReviewBinding, TaskReviewActionView, TaskReviewIntent, TaskWaitReviewDisplayItem};
    use gpui_kit::component::Root;
    use gpui_kit::{
        AppContext, Context, FocusHandle, InteractiveElement, IntoElement, Render, TestAppContext,
        Window, div,
    };
    use pioneer_client::core::{ClientCore, ClientScope};
    use pioneer_desktop_foundation::ClientPublicationSink;
    use std::{
        cell::{Cell, RefCell},
        sync::Arc,
    };
    #[::core::prelude::v1::test]
    fn binding_rejects_wrong_candidate_and_late_protected_output_after_clear() {
        let core = ClientCore::new();
        let scope = ClientScope::TaskReview {
            thread_id: "a".into(),
            candidate_id: "candidate".into(),
        };
        let binding = ReviewBinding {
            scope: scope.clone(),
            sequence: Cell::new(0),
            input: RefCell::new(None),
            changed: tokio::sync::watch::channel(0).0,
        };
        core.task_review_intent(TaskReviewIntent::Observe {
            thread_id: "a".into(),
            candidate_id: "candidate".into(),
        });
        let old = core.snapshot(&scope).unwrap();
        binding.publish(old.clone());
        assert!(binding.input.borrow().is_some());
        core.task_review_intent(TaskReviewIntent::Observe {
            thread_id: "b".into(),
            candidate_id: "candidate".into(),
        });
        binding.publish(
            core.snapshot(&ClientScope::TaskReview {
                thread_id: "b".into(),
                candidate_id: "candidate".into(),
            })
            .unwrap(),
        );
        assert_eq!(binding.input.borrow().as_ref().unwrap().thread_id, "a");
        core.clear_authorization_projections();
        binding.publish(core.snapshot(&scope).unwrap());
        assert!(binding.input.borrow().is_none());
        binding.publish(old);
        assert!(binding.input.borrow().is_none());
    }

    struct Host {
        focus: FocusHandle,
    }
    impl Render for Host {
        fn render(&mut self, _: &mut Window, _: &mut Context<Self>) -> impl IntoElement {
            div().track_focus(&self.focus)
        }
    }
    #[gpui_kit::test]
    fn candidate_drop_releases_binding_dialog_and_weak_callbacks(cx: &mut TestAppContext) {
        cx.update(gpui_kit::init);
        let (root, cx) = cx.add_window_view(|window, cx| {
            let host = cx.new(|cx| Host {
                focus: cx.focus_handle(),
            });
            Root::new(host, window, cx)
        });
        let core = Arc::new(ClientCore::new());
        let (registrar, deliver) = crate::test_support::binding_router(core.clone());
        let (weak, trigger) = cx.update(|window, cx| {
            let host = root.read(cx).view().clone().downcast::<Host>().unwrap();
            let trigger = host.read(cx).focus.clone();
            trigger.focus(window, cx);
            let view = TaskReviewActionView::new(core.clone(), registrar, "a".into(), "candidate".into(), "Candidate".into(), window, cx);
            deliver();
            let item: TaskWaitReviewDisplayItem = serde_json::from_value(serde_json::json!({
                "task_id":"task", "run_id":"run", "candidate_id":"candidate", "review_mode":"user_approval", "user_approval_required":true,
                "diagnostics":[], "allowed_actions":["task_revise"]
            })).unwrap();
            view.update(cx, |view, cx| view.open_task_review_revise_dialog(item, window, cx));
            assert_ne!(window.focused(cx), Some(trigger.clone()));
            let weak = view.downgrade();
            drop(view);
            (weak, trigger)
        });
        cx.run_until_parked();
        assert!(weak.upgrade().is_none());
        assert!(core.task_review_snapshot("a", "candidate").is_none());
        cx.update(|window, cx| assert_eq!(window.focused(cx), Some(trigger)));
    }
}
