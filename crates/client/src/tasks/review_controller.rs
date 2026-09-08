//! Candidate-scoped review requests and their lifecycle.

use super::review::{
    self, TaskReviewAction, TaskReviewActionState, TaskReviewPlanError,
    TaskReviewPresentationCapabilities,
};
use crate::{
    core::{ClientCore, ClientIntent, ClientMutationAuthority, ClientScope, ClientTransition},
    timeline::{labels::TaskWaitReviewDisplayItem, presentation::TimelineSnapshot},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, mpsc},
    thread::JoinHandle,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskReviewIntent {
    Observe {
        thread_id: String,
        candidate_id: String,
    },
    Perform {
        thread_id: String,
        candidate_id: String,
        action: TaskReviewAction,
        feedback: Option<String>,
        reason: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    fn scope(thread: &str, candidate: &str) -> ClientScope {
        ClientScope::TaskReview {
            thread_id: thread.into(),
            candidate_id: candidate.into(),
        }
    }
    fn pending(core: &ClientCore, thread: &str, candidate: &str) -> ReviewRequest {
        let mut owner = core.task_reviews.lock().unwrap();
        let generation = owner.next_generation();
        core.publish_task_review(
            &mut owner,
            TaskReviewPublication {
                thread_id: thread.into(),
                candidate_id: candidate.into(),
                revision: 0,
                generation,
                item: None,
                visible_actions: vec![TaskReviewAction::Cancel],
                allowed_actions: vec![TaskReviewAction::Cancel],
                request: TaskReviewRequestState::Pending {
                    action: TaskReviewAction::Cancel,
                },
            },
        );
        ReviewRequest {
            thread_id: thread.into(),
            candidate_id: candidate.into(),
            generation,
            action: TaskReviewAction::Cancel,
            params: ReviewRequestParams::Cancel(pioneer_protocol::TaskCancelParams {
                task_id: "task".into(),
                reason: None,
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            }),
        }
    }

    #[test]
    fn completion_is_scoped_to_the_matching_thread_candidate_and_generation() {
        let core = Arc::new(ClientCore::new());
        let capacity = NonZeroUsize::new(16).unwrap();
        let root = core.subscribe(
            ClientScope::Thread {
                thread_id: "a".into(),
            },
            capacity,
        );
        let candidate = core.subscribe(scope("a", "candidate"), capacity);
        let other = core.subscribe(scope("a", "other"), capacity);
        let other_thread = core.subscribe(scope("b", "candidate"), capacity);
        let request = pending(&core, "a", "candidate");
        pending(&core, "a", "other");
        pending(&core, "b", "candidate");
        for subscription in [&root, &candidate, &other, &other_thread] {
            while subscription.try_next().is_some() {}
        }
        let before = core.task_review_snapshot("a", "candidate").unwrap();
        let mut stale = request.clone();
        stale.generation += 100;
        core.complete_task_review(stale, Ok(()));
        assert!(Arc::ptr_eq(
            &before,
            &core.task_review_snapshot("a", "candidate").unwrap()
        ));
        core.complete_task_review(request.clone(), Err("synthetic failure".into()));
        assert!(candidate.try_next().is_some());
        assert!(other.try_next().is_none());
        assert!(other_thread.try_next().is_none());
        assert!(root.try_next().is_none());
        let failed = core.task_review_snapshot("a", "candidate").unwrap();
        assert!(matches!(
            failed.request,
            TaskReviewRequestState::Failed { .. }
        ));
        core.complete_task_review(request, Ok(()));
        assert!(Arc::ptr_eq(
            &failed,
            &core.task_review_snapshot("a", "candidate").unwrap()
        ));
    }

    #[test]
    fn releasing_last_binding_retires_request_and_late_completion_cannot_restore_it() {
        let core = Arc::new(ClientCore::new());
        let subscription = core.subscribe(scope("a", "candidate"), NonZeroUsize::new(4).unwrap());
        let request = pending(&core, "a", "candidate");
        drop(subscription);
        assert!(core.task_review_snapshot("a", "candidate").is_none());
        core.complete_task_review(request, Ok(()));
        assert!(core.task_review_snapshot("a", "candidate").is_none());
        let retired = core
            .snapshot(&scope("a", "candidate"))
            .unwrap()
            .typed::<TaskReviewPublication>()
            .unwrap();
        assert!(retired.payload().item.is_none());
        assert_eq!(retired.payload().request, TaskReviewRequestState::Cancelled);
    }

    #[test]
    fn authorization_fence_cancels_candidate_and_composer_before_late_callbacks() {
        use crate::composer::{state_machine::ComposerDomainState, store::*};
        let core = Arc::new(ClientCore::new());
        let candidate = pending(&core, "a", "candidate");
        core.composer_intent(ComposerIntent::Open {
            thread_id: "a".into(),
            defaults: ComposerDomainState::default(),
        });
        let draft_id = core.composer_snapshot("a").unwrap().draft_id();
        core.composer_intent(ComposerIntent::EditText {
            thread_id: "a".into(),
            draft_id,
            text: "unsent draft".into(),
        });
        core.composer_intent(ComposerIntent::BeginOperation {
            thread_id: "a".into(),
            draft_id,
            operation: ComposerOperationKind::PickFiles,
        });
        let operation = core
            .composer_snapshot("a")
            .unwrap()
            .operation()
            .unwrap()
            .identity
            .clone();
        core.clear_authorization_projections();
        let before = core.task_review_snapshot("a", "candidate").unwrap();
        assert_eq!(before.request, TaskReviewRequestState::Cancelled);
        assert!(before.item.is_none());
        core.complete_task_review(candidate, Ok(()));
        assert!(Arc::ptr_eq(
            &before,
            &core.task_review_snapshot("a", "candidate").unwrap()
        ));
        assert!(core.composer_snapshot("a").is_none());
        core.composer_intent(ComposerIntent::Activate {
            thread_id: "a".into(),
        });
        let replacement = core.composer_snapshot("a").unwrap();
        assert_ne!(replacement.draft_id(), draft_id);
        core.composer_intent(ComposerIntent::CompleteOperation {
            identity: operation,
            completion: ComposerOperationCompletion::FilesSelected {
                attachments: vec![],
            },
        });
        assert!(Arc::ptr_eq(
            &replacement,
            &core.composer_snapshot("a").unwrap()
        ));
        assert!(replacement.operation().is_none());
        assert!(replacement.draft().text.is_empty());
    }

    #[test]
    fn unavailable_candidate_has_bounded_failure_and_observation_does_not_retry() {
        let core = ClientCore::new();
        core.task_review_intent(TaskReviewIntent::Perform {
            thread_id: "a".into(),
            candidate_id: "candidate".into(),
            action: TaskReviewAction::Accept,
            feedback: None,
            reason: None,
        });
        let failed = core.task_review_snapshot("a", "candidate").unwrap();
        for _ in 0..5 {
            core.task_review_intent(TaskReviewIntent::Observe {
                thread_id: "a".into(),
                candidate_id: "candidate".into(),
            });
        }
        assert!(Arc::ptr_eq(
            &failed,
            &core.task_review_snapshot("a", "candidate").unwrap()
        ));
        assert_eq!(
            failed.request,
            TaskReviewRequestState::Failed {
                error: TaskReviewFailure::Unavailable
            }
        );
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskReviewFailure {
    Plan { error: TaskReviewPlanError },
    Transport { message: String },
    Unavailable,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskReviewRequestState {
    Idle,
    Pending { action: TaskReviewAction },
    Succeeded { action: TaskReviewAction },
    Failed { error: TaskReviewFailure },
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TaskReviewPublication {
    pub thread_id: String,
    pub candidate_id: String,
    pub revision: u64,
    pub generation: u64,
    pub item: Option<TaskWaitReviewDisplayItem>,
    pub visible_actions: Vec<TaskReviewAction>,
    pub allowed_actions: Vec<TaskReviewAction>,
    pub request: TaskReviewRequestState,
}
impl TaskReviewPublication {
    pub fn pending(&self) -> bool {
        matches!(self.request, TaskReviewRequestState::Pending { .. })
    }
}

#[derive(Clone)]
enum ReviewRequestParams {
    Accept(pioneer_protocol::TaskAcceptParams),
    Revise(pioneer_protocol::TaskReviseParams),
    Cancel(pioneer_protocol::TaskCancelParams),
}
#[derive(Clone)]
struct ReviewRequest {
    thread_id: String,
    candidate_id: String,
    generation: u64,
    action: TaskReviewAction,
    params: ReviewRequestParams,
}

#[derive(Default)]
pub(crate) struct TaskReviewActionController {
    publications: BTreeMap<(String, String), Arc<TaskReviewPublication>>,
    subscriptions: BTreeMap<(String, String), usize>,
    generation: u64,
    sender: Option<mpsc::SyncSender<ReviewRequest>>,
    task: Option<JoinHandle<()>>,
}
impl TaskReviewActionController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
        self.subscriptions.clear();
    }
    fn next_generation(&mut self) -> u64 {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("task review generation exhausted");
        self.generation
    }
}
impl Drop for TaskReviewActionController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}

impl ClientCore {
    pub(crate) fn task_review_demand_changed(
        &self,
        scope: &ClientScope,
        demand: crate::core::ClientDemand,
    ) {
        let ClientScope::TaskReview {
            thread_id,
            candidate_id,
        } = scope
        else {
            return;
        };
        if demand != crate::core::ClientDemand::Suspended {
            self.task_review_intent(TaskReviewIntent::Observe {
                thread_id: thread_id.clone(),
                candidate_id: candidate_id.clone(),
            });
            return;
        }
        let mut owner = self
            .task_reviews
            .lock()
            .expect("task review owner poisoned");
        let key = (thread_id.clone(), candidate_id.clone());
        if let Some(current) = owner.publications.get(&key) {
            let mut next = (**current).clone();
            next.generation = owner.next_generation();
            next.item = None;
            next.visible_actions.clear();
            next.allowed_actions.clear();
            next.request = TaskReviewRequestState::Cancelled;
            self.publish_task_review(&mut owner, next);
            owner.publications.remove(&key);
        }
    }
    pub(crate) fn invalidate_task_reviews(&self, thread_id: Option<&str>) {
        let mut owner = self
            .task_reviews
            .lock()
            .expect("task review owner poisoned");
        let current = owner
            .publications
            .values()
            .filter(|p| thread_id.is_none_or(|thread| p.thread_id == thread))
            .cloned()
            .collect::<Vec<_>>();
        for input in current {
            let mut next = (*input).clone();
            next.generation = owner.next_generation();
            next.item = None;
            next.visible_actions.clear();
            next.allowed_actions.clear();
            next.request = TaskReviewRequestState::Cancelled;
            self.publish_task_review(&mut owner, next);
        }
    }
    pub fn task_review_snapshot(
        &self,
        thread_id: &str,
        candidate_id: &str,
    ) -> Option<Arc<TaskReviewPublication>> {
        self.task_reviews
            .lock()
            .expect("task review owner poisoned")
            .publications
            .get(&(thread_id.into(), candidate_id.into()))
            .cloned()
    }

    fn task_review_input(
        &self,
        thread_id: &str,
        candidate_id: &str,
    ) -> (
        Option<TaskWaitReviewDisplayItem>,
        Vec<TaskReviewAction>,
        Vec<TaskReviewAction>,
    ) {
        let item = self
            .snapshot(&ClientScope::Timeline {
                thread_id: thread_id.into(),
            })
            .and_then(|publication| publication.typed::<TimelineSnapshot>())
            .and_then(|publication| {
                publication
                    .payload()
                    .rows()
                    .iter()
                    .filter_map(|row| row.content()?.tool.as_ref()?.task_review.as_ref())
                    .flat_map(|review| &review.items)
                    .find(|item| item.candidate_id == candidate_id)
                    .cloned()
            });
        let capabilities = self
            .thread_coordinator_snapshot(thread_id)
            .and_then(|thread| {
                self.authorization_snapshot(Some(&thread.workspace_id), Some(thread_id))
            })
            .and_then(|snapshot| snapshot.thread)
            .map_or_else(TaskReviewPresentationCapabilities::default, |thread| {
                TaskReviewPresentationCapabilities {
                    can_review: thread.capabilities.can_review_tasks,
                    can_cancel: thread.capabilities.can_cancel_tasks,
                }
            });
        let visible_actions = [
            TaskReviewAction::Accept,
            TaskReviewAction::Revise,
            TaskReviewAction::Cancel,
        ]
        .into_iter()
        .filter(|action| {
            item.as_ref().is_some_and(|item| {
                item.user_controls_allowed()
                    && item.allows_action(action.protocol_action())
                    && match action {
                        TaskReviewAction::Accept | TaskReviewAction::Revise => {
                            capabilities.can_review
                        }
                        TaskReviewAction::Cancel => capabilities.can_cancel,
                    }
            })
        })
        .collect();
        let actions = [
            TaskReviewAction::Accept,
            TaskReviewAction::Revise,
            TaskReviewAction::Cancel,
        ]
        .into_iter()
        .filter(|action| {
            item.as_ref().is_some_and(|item| {
                review::task_review_action_authorized_and_enabled(
                    item,
                    *action,
                    capabilities,
                    &TaskReviewActionState::default(),
                )
            })
        })
        .collect();
        (item, visible_actions, actions)
    }

    fn publish_task_review(
        &self,
        owner: &mut TaskReviewActionController,
        mut next: TaskReviewPublication,
    ) -> ClientTransition {
        let key = (next.thread_id.clone(), next.candidate_id.clone());
        next.revision = owner
            .publications
            .get(&key)
            .map_or_else(
                || {
                    self.snapshot(&ClientScope::TaskReview {
                        thread_id: key.0.clone(),
                        candidate_id: key.1.clone(),
                    })
                    .map_or(0, |p| p.revisions().scoped().get())
                },
                |p| p.revision,
            )
            .checked_add(1)
            .expect("task review revision exhausted");
        let revision = next.revision;
        let next = Arc::new(next);
        owner.publications.insert(key.clone(), next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            ClientScope::TaskReview {
                thread_id: key.0,
                candidate_id: key.1,
            },
            crate::threads::registry::revisions(revision),
            next,
            vec![],
        )
    }

    pub fn task_review_intent(&self, intent: TaskReviewIntent) -> ClientTransition {
        let (thread_id, candidate_id) = match &intent {
            TaskReviewIntent::Observe {
                thread_id,
                candidate_id,
            }
            | TaskReviewIntent::Perform {
                thread_id,
                candidate_id,
                ..
            } => (thread_id.clone(), candidate_id.clone()),
        };
        if self.is_stopped() || thread_id.is_empty() || candidate_id.is_empty() {
            return self.reject_intent();
        }
        let (item, visible_actions, allowed_actions) =
            self.task_review_input(&thread_id, &candidate_id);
        let mut owner = self
            .task_reviews
            .lock()
            .expect("task review owner poisoned");
        let key = (thread_id.clone(), candidate_id.clone());
        let mut next = owner
            .publications
            .get(&key)
            .map(|p| (**p).clone())
            .unwrap_or_else(|| TaskReviewPublication {
                thread_id,
                candidate_id,
                revision: 0,
                generation: 0,
                item: None,
                visible_actions: vec![],
                allowed_actions: vec![],
                request: TaskReviewRequestState::Idle,
            });
        let changed = next.item != item
            || next.visible_actions != visible_actions
            || next.allowed_actions != allowed_actions;
        if changed {
            let pending = next.pending();
            next.item = item;
            next.visible_actions = visible_actions;
            next.allowed_actions = allowed_actions;
            next.generation = owner.next_generation();
            next.request = if pending {
                TaskReviewRequestState::Cancelled
            } else {
                TaskReviewRequestState::Idle
            };
        }
        match intent {
            TaskReviewIntent::Observe { .. } => {
                if !changed && owner.publications.contains_key(&key) {
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                }
            }
            TaskReviewIntent::Perform {
                action,
                feedback,
                reason,
                ..
            } => {
                if next.pending() {
                    return self.transition(
                        &ClientMutationAuthority { _private: () },
                        vec![],
                        vec![],
                    );
                }
                next.generation = owner.next_generation();
                let request = (|| -> Result<ReviewRequestParams, TaskReviewFailure> {
                    let item = next
                        .item
                        .as_ref()
                        .filter(|_| next.allowed_actions.contains(&action))
                        .ok_or(TaskReviewFailure::Unavailable)?;
                    let mut state = TaskReviewActionState::default();
                    let result = match action {
                        TaskReviewAction::Accept => {
                            review::plan_task_review_accept(item, reason, &mut state)
                                .map(|r| r.map(|r| ReviewRequestParams::Accept(r.params)))
                        }
                        TaskReviewAction::Revise => review::plan_task_review_revise(
                            item,
                            feedback.unwrap_or_default(),
                            &mut state,
                        )
                        .map(|r| r.map(|r| ReviewRequestParams::Revise(r.params))),
                        TaskReviewAction::Cancel => {
                            review::plan_task_review_cancel(item, reason, &mut state)
                                .map(|r| r.map(|r| ReviewRequestParams::Cancel(r.params)))
                        }
                    };
                    result
                        .map_err(|error| TaskReviewFailure::Plan { error })?
                        .ok_or(TaskReviewFailure::Unavailable)
                })();
                match request {
                    Ok(params) => {
                        let request = ReviewRequest {
                            thread_id: key.0,
                            candidate_id: key.1,
                            generation: next.generation,
                            action,
                            params,
                        };
                        next.request = if owner
                            .sender
                            .as_ref()
                            .is_some_and(|sender| sender.try_send(request).is_ok())
                        {
                            TaskReviewRequestState::Pending { action }
                        } else {
                            TaskReviewRequestState::Failed {
                                error: TaskReviewFailure::Unavailable,
                            }
                        };
                    }
                    Err(error) => next.request = TaskReviewRequestState::Failed { error },
                }
            }
        }
        self.publish_task_review(&mut owner, next)
    }

    fn task_review_request_is_current(&self, request: &ReviewRequest) -> bool {
        !self.is_stopped()
            && self
                .task_review_snapshot(&request.thread_id, &request.candidate_id)
                .is_some_and(|p| {
                    p.generation == request.generation
                        && p.request
                            == TaskReviewRequestState::Pending {
                                action: request.action,
                            }
                })
    }
    fn complete_task_review(&self, request: ReviewRequest, result: Result<(), String>) {
        let mut owner = self
            .task_reviews
            .lock()
            .expect("task review owner poisoned");
        let Some(current) = owner
            .publications
            .get(&(request.thread_id.clone(), request.candidate_id))
        else {
            return;
        };
        if self.is_stopped()
            || current.generation != request.generation
            || current.request
                != (TaskReviewRequestState::Pending {
                    action: request.action,
                })
        {
            return;
        }
        let mut next = (**current).clone();
        let succeeded = result.is_ok();
        next.request = match result {
            Ok(()) => TaskReviewRequestState::Succeeded {
                action: request.action,
            },
            Err(message) => TaskReviewRequestState::Failed {
                error: TaskReviewFailure::Transport { message },
            },
        };
        self.publish_task_review(&mut owner, next);
        drop(owner);
        if succeeded {
            self.refresh_thread_timeline(&request.thread_id);
        }
    }

    pub(crate) fn start_task_review_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<ReviewRequest>(64);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-task-review".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.task_review_request_is_current(&request) {
                        continue;
                    }
                    core.task_review_intent(TaskReviewIntent::Observe {
                        thread_id: request.thread_id.clone(),
                        candidate_id: request.candidate_id.clone(),
                    });
                    if !core.task_review_request_is_current(&request) {
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let result = match request.params.clone() {
                        ReviewRequestParams::Accept(params) => {
                            sender.task_accept(params).map(|_| ())
                        }
                        ReviewRequestParams::Revise(params) => {
                            sender.task_revise(params).map(|_| ())
                        }
                        ReviewRequestParams::Cancel(params) => {
                            sender.task_cancel(params).map(|_| ())
                        }
                    };
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.task_review_request_is_current(&request) {
                        continue;
                    }
                    core.task_review_intent(TaskReviewIntent::Observe {
                        thread_id: request.thread_id.clone(),
                        candidate_id: request.candidate_id.clone(),
                    });
                    core.complete_task_review(
                        request,
                        result.map_err(|error| format!("{error:#}")),
                    );
                }
            })
            .expect("task review worker could not start");
        let mut owner = self
            .task_reviews
            .lock()
            .expect("task review owner poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }

    pub(crate) fn task_review_subscription_changed(&self, scope: &ClientScope, added: bool) {
        let ClientScope::TaskReview {
            thread_id,
            candidate_id,
        } = scope
        else {
            return;
        };
        if added {
            *self
                .task_reviews
                .lock()
                .expect("task review owner poisoned")
                .subscriptions
                .entry((thread_id.clone(), candidate_id.clone()))
                .or_default() += 1;
            self.dispatch(ClientIntent::TaskReview {
                intent: TaskReviewIntent::Observe {
                    thread_id: thread_id.clone(),
                    candidate_id: candidate_id.clone(),
                },
            });
        } else {
            let mut owner = self
                .task_reviews
                .lock()
                .expect("task review owner poisoned");
            let key = (thread_id.clone(), candidate_id.clone());
            let count = owner.subscriptions.entry(key.clone()).or_default();
            *count = count.saturating_sub(1);
            if *count == 0 {
                owner.subscriptions.remove(&key);
                drop(owner);
                self.task_review_demand_changed(scope, crate::core::ClientDemand::Suspended);
            }
        }
    }
}
