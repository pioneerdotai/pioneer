//! Foreground API-provider startup ownership. No event bodies are queued here:
//! durable deliveries retain their existing outbox and progress remains lossy.
use super::*;
use std::sync::{Mutex as SyncMutex, Weak};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const MAX_STARTS: usize = 32;
const MAX_START_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
struct AdmissionState {
    closed: bool,
    entries: HashMap<String, Weak<StartObligation>>,
}

#[derive(Default)]
pub(crate) struct ForegroundStartOwners {
    state: SyncMutex<AdmissionState>,
    tasks: TaskTracker,
    shutdown: CancellationToken,
    #[cfg(test)]
    pub(crate) snapshot_waiting: tokio::sync::Notify,
}

/// Dropping the acceptance after the reply enqueue attempt releases publication.
/// Dropping a disconnected handler releases it too; execution is already owned.
pub(crate) struct ForegroundAcceptance {
    pub(crate) response: pioneer_protocol::TurnStartResponse,
    _release: Option<tokio::sync::oneshot::Sender<()>>,
}

pub(crate) struct StartObligation {
    thread_id: String,
    pending: AtomicBool,
    publisher_started: AtomicBool,
    pub(crate) execution: SyncMutex<Option<pioneer_crud::TurnExecutionRecord>>,
    pub(crate) actor_generation: SyncMutex<Option<u64>>,
    activation: SyncMutex<Option<Result<(), String>>>,
    activated: tokio::sync::Notify,
    mandatory_ready: AtomicBool,
    published: tokio::sync::Notify,
    #[cfg(test)]
    pub(crate) publication_wait_joined: tokio::sync::Notify,
}

tokio::task_local! {
    static PREPARING_START: Arc<StartObligation>;
}

#[cfg(test)]
tokio::task_local! { pub(crate) static FOREGROUND_UNREAD_EXTRAS: (); }

/// Called by the existing preparation owner only after its durable New commit.
/// The task-local receipt prevents a conflicting/replayed request (including a
/// voice preparation on another task) from acquiring this failure obligation.
pub(super) fn note_committed_start(execution: pioneer_crud::TurnExecutionRecord) {
    let _ = PREPARING_START.try_with(|entry| {
        *entry.execution.lock().unwrap_or_else(|e| e.into_inner()) = Some(execution);
    });
}

pub(super) fn preparing_foreground_start() -> bool {
    PREPARING_START.try_with(|_| ()).is_ok()
}

pub(super) fn note_start_actor(generation: u64) {
    let _ = PREPARING_START.try_with(|entry| {
        *entry
            .actor_generation
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(generation);
    });
}

impl StartObligation {
    pub(crate) fn activation_result(&self, result: Result<(), String>) {
        *self.activation.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
        self.activated.notify_waiters();
    }

    fn finish_publication(&self) {
        self.pending.store(false, Ordering::Release);
        self.published.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            let changed = self.published.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.pending.load(Ordering::Acquire) {
                return;
            }
            #[cfg(test)]
            self.publication_wait_joined.notify_one();
            changed.await;
        }
    }
}

struct PreparationGuard(Arc<StartObligation>);
impl Drop for PreparationGuard {
    fn drop(&mut self) {
        if !self.0.publisher_started.load(Ordering::Acquire) {
            self.0.finish_publication();
        }
    }
}

/// A cancelled/panicking publisher cannot leave an open gate or a future
/// delayed start: its publication future is dropped before this guard releases.
struct PublicationGuard(Arc<StartObligation>);
impl Drop for PublicationGuard {
    fn drop(&mut self) {
        self.0.finish_publication();
    }
}

impl ForegroundStartOwners {
    pub(crate) fn pending(&self, thread_id: &str, turn_id: &str) -> bool {
        self.entry(thread_id, turn_id)
            .is_some_and(|entry| entry.pending.load(Ordering::Acquire))
    }

    pub(crate) fn pending_thread(&self, thread_id: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .values()
            .filter_map(Weak::upgrade)
            .any(|entry| entry.thread_id == thread_id && entry.pending.load(Ordering::Acquire))
    }

    pub(crate) async fn before_turn_snapshot(&self, thread_id: &str, turn_id: &str) {
        if let Some(entry) = self.entry(thread_id, turn_id) {
            #[cfg(test)]
            if entry.pending.load(Ordering::Acquire) {
                self.snapshot_waiting.notify_one();
            }
            entry.wait().await;
        }
    }

    pub(crate) async fn before_thread_snapshot(&self, thread_id: &str) {
        let entries: Vec<_> = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .values()
            .filter_map(Weak::upgrade)
            .filter(|entry| entry.thread_id == thread_id)
            .collect();
        for entry in entries {
            entry.wait().await;
        }
    }

    pub(crate) fn entry(&self, thread_id: &str, turn_id: &str) -> Option<Arc<StartObligation>> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .get(turn_id)
            .and_then(Weak::upgrade)
            .filter(|entry| entry.thread_id == thread_id)
    }

    pub(crate) fn admit(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Arc<StartObligation>, TurnStartFailure> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.entries.retain(|_, entry| entry.strong_count() != 0);
        if state.closed || state.entries.len() >= MAX_STARTS {
            return Err(TurnStartFailure::unavailable(
                "foreground startup capacity unavailable",
            ));
        }
        if state.entries.contains_key(turn_id) {
            return Err(TurnStartFailure::conflict(
                "foreground turn startup is already owned",
            ));
        }
        let entry = Arc::new(StartObligation {
            thread_id: thread_id.to_owned(),
            pending: AtomicBool::new(true),
            publisher_started: AtomicBool::new(false),
            execution: SyncMutex::new(None),
            actor_generation: SyncMutex::new(None),
            activation: SyncMutex::new(None),
            activated: tokio::sync::Notify::new(),
            mandatory_ready: AtomicBool::new(false),
            published: tokio::sync::Notify::new(),
            #[cfg(test)]
            publication_wait_joined: tokio::sync::Notify::new(),
        });
        state
            .entries
            .insert(turn_id.to_owned(), Arc::downgrade(&entry));
        Ok(entry)
    }

    pub(crate) async fn shutdown(&self) {
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.closed = true;
            self.shutdown.cancel();
            self.tasks.close();
        }
        // Mandatory preparation/handoff is drained, never aborted with a socket.
        self.tasks.wait().await;
    }
}

/// Count encoded retained input without allocating a second copy. This ceiling
/// is independent of outbound admission and is not a process-RSS limit.
fn bounded_payload(value: &impl Serialize) -> Result<(), TurnStartFailure> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|n| *n <= MAX_START_BYTES)
                .ok_or_else(|| {
                    std::io::Error::other("foreground startup payload exceeds budget")
                })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter(0), value)
        .map_err(|_| TurnStartFailure::unavailable("foreground startup payload exceeds budget"))
}

impl MessageProcessor {
    pub(crate) async fn accept_foreground_api_provider_start(
        &self,
        connection_id: ConnectionId,
        request_actor: pioneer_protocol::PersistedActorRef,
        scoped_principal_id: Option<pioneer_protocol::PrincipalId>,
        params: TurnStartParams,
        reasoning_effort: Option<String>,
        execution_admission: ExecutionAuthorizationAdmission,
    ) -> Result<ForegroundAcceptance, TurnStartFailure> {
        // Replay continues to validate the canonical request digest through the
        // ordinary admission path. It must not race a delayed initial start.
        if self
            .crud_store
            .get_turn(params.thread_id.trim(), params.turn_id.trim())
            .await
            .map_err(|error| {
                TurnStartFailure::unavailable(format!("failed to inspect turn: {error:#}"))
            })?
            .is_some()
        {
            let entry = self
                .foreground_starts
                .entry(params.thread_id.trim(), params.turn_id.trim());
            let replay = self
                .prepare_api_provider_turn_start(
                    connection_id,
                    request_actor,
                    scoped_principal_id,
                    params,
                    reasoning_effort.as_deref(),
                    crate::authorization::ExecutionAdmissionEntryPoint::InteractiveTurn,
                    crate::message::turn_handlers::ApiProviderTurnPreparation::ReplayOnly,
                    execution_admission,
                )
                .await?;
            if let Some(entry) = entry {
                entry.wait().await;
            }
            return match replay {
                ApiProviderTurnAdmission::Replay(response) => Ok(ForegroundAcceptance {
                    response,
                    _release: None,
                }),
                ApiProviderTurnAdmission::New(_) => Err(TurnStartFailure::internal(
                    "existing turn lost its admission identity",
                )),
            };
        }
        bounded_payload(&params)?;
        let entry = self
            .foreground_starts
            .admit(params.thread_id.trim(), params.turn_id.trim())?;
        let ownership_guard = PreparationGuard(entry.clone());
        let processor = self.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        // Admission and task registration share the shutdown lock. No await or
        // I/O while held. A closed tracker cannot strand an admitted obligation.
        {
            let state = self
                .foreground_starts
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.closed {
                return Err(TurnStartFailure::unavailable("gateway is shutting down"));
            }
            self.foreground_starts.tasks.spawn(async move {
                let _guard = ownership_guard;
                let result = AssertUnwindSafe(PREPARING_START.scope(
                    entry.clone(),
                    processor.prepare_and_dispatch_foreground_start(
                        connection_id,
                        request_actor,
                        scoped_principal_id,
                        params,
                        reasoning_effort,
                        execution_admission,
                        entry,
                    ),
                ))
                .catch_unwind()
                .await;
                let result = match result {
                    Ok(result) => result,
                    Err(_) => Err(TurnStartFailure::internal(
                        "foreground startup owner panicked",
                    )),
                };
                if result.is_err() {
                    let execution = _guard
                        .0
                        .execution
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone();
                    if let Some(execution) = execution {
                        processor
                            .settle_foreground_start_failure(
                                &execution,
                                _guard.0.mandatory_ready.load(Ordering::Acquire),
                            )
                            .await;
                    }
                }
                let _ = ready_tx.send(result);
            });
        }
        ready_rx
            .await
            .map_err(|_| TurnStartFailure::unavailable("foreground startup owner stopped"))?
    }

    async fn settle_foreground_start_failure(
        &self,
        expected: &pioneer_crud::TurnExecutionRecord,
        mandatory_ready: bool,
    ) {
        // Advisory fast exit only. The write below MUST repeat this predicate
        // atomically; replacement may win after this read.
        if let Ok(Some(current)) = self.crud_store.get_turn_execution(&expected.turn_id).await {
            if current.owner_id != expected.owner_id
                || current.owner_generation != expected.owner_generation
            {
                return;
            }
        }
        #[cfg(test)]
        self.completed_history_preparation_barrier
            .wait_if_armed(
                "__foreground_failure_before_settlement__",
                &self.foreground_starts.shutdown,
            )
            .await;
        loop {
            let settled = async {
                if mandatory_ready {
                    // The existing recovery scanner discovers our Recovering
                    // receipts too. No unfenced recovery job or actor mutation.
                    self.crud_store
                        .settle_foreground_start_for_recovery(expected)
                        .await?;
                } else {
                    let Some((workspace_id, mut turn)) = self
                        .crud_store
                        .get_turn(&expected.thread_id, &expected.turn_id)
                        .await?
                    else {
                        return Ok::<_, anyhow::Error>(());
                    };
                    if turn.status != TurnStatus::InProgress {
                        return self.mirror_foreground_refusal(expected).await;
                    }
                    turn.status = TurnStatus::Blocked;
                    turn.error =
                        Some("mandatory foreground startup preparation did not complete".into());
                    let event = pioneer_crud::CanonicalTurnEventPayload::TurnBlocked(
                        pioneer_protocol::TurnBlockedNotification {
                            workspace_id,
                            thread_id: expected.thread_id.clone(),
                            turn: turn.clone(),
                            resume: None,
                        },
                    );
                    match self
                        .crud_store
                        .materialize_unactivated_turn_refusal_owned(
                            event,
                            now_timestamp_secs(),
                            expected.clone(),
                        )
                        .await
                    {
                        Ok(()) => {
                            self.mirror_foreground_refusal(expected).await?;
                            self.kick_native_turn_event_deliveries();
                        }
                        Err(error)
                            if error
                                .downcast_ref::<pioneer_crud::UnactivatedTurnPhaseChanged>()
                                .is_some() => {}
                        Err(error) => return Err(error),
                    }
                }
                Ok(())
            }
            .await;
            if settled.is_ok() {
                return;
            }
            tokio::select! {
                _ = self.foreground_starts.shutdown.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {},
            }
        }
    }

    async fn mirror_foreground_refusal(
        &self,
        expected: &pioneer_crud::TurnExecutionRecord,
    ) -> anyhow::Result<()> {
        let Some((workspace, terminal)) = self
            .crud_store
            .get_turn(&expected.thread_id, &expected.turn_id)
            .await?
        else {
            return Ok(());
        };
        if workspace != expected.workspace_id || terminal.status != TurnStatus::Blocked {
            return Ok(());
        }
        // Terminal acceptance fences claims in the writer. Revalidate its exact
        // owner and projection under the existing memory transition lock too.
        self.thread_manager
            .commit_terminal_turn_if_current(
                &expected.workspace_id,
                &expected.thread_id,
                &terminal,
                async {
                    let current = self
                        .crud_store
                        .get_turn_execution(&expected.turn_id)
                        .await?;
                    let projected = self
                        .crud_store
                        .get_turn(&expected.thread_id, &expected.turn_id)
                        .await?;
                    let owned = current.is_some_and(|current| {
                        current.owner_id == expected.owner_id
                            && current.owner_generation == expected.owner_generation
                            && current.status == pioneer_crud::TurnExecutionStatus::Blocked
                    }) && projected.is_some_and(|(_, projected)| projected == terminal);
                    if owned {
                        // No actor/progress was activated before mandatory-ready.
                        // Resume shares this memory lock; remove only this proven
                        // terminal's preparation lease before it may reopen.
                        self.execution_leases.finish(&expected.turn_id).await;
                    }
                    Ok(owned)
                },
            )
            .await?;
        Ok(())
    }

    async fn prepare_and_dispatch_foreground_start(
        &self,
        connection_id: ConnectionId,
        request_actor: pioneer_protocol::PersistedActorRef,
        scoped_principal_id: Option<pioneer_protocol::PrincipalId>,
        params: TurnStartParams,
        reasoning_effort: Option<String>,
        execution_admission: ExecutionAuthorizationAdmission,
        entry: Arc<StartObligation>,
    ) -> Result<ForegroundAcceptance, TurnStartFailure> {
        #[cfg(test)]
        self.completed_history_preparation_barrier
            .wait_if_armed(
                "__foreground_start_before_prepare__",
                &self.foreground_starts.shutdown,
            )
            .await;
        let admission = self
            .prepare_api_provider_turn_start(
                connection_id,
                request_actor,
                scoped_principal_id,
                params,
                reasoning_effort.as_deref(),
                crate::authorization::ExecutionAdmissionEntryPoint::InteractiveTurn,
                crate::message::turn_handlers::ApiProviderTurnPreparation::AllowNew,
                execution_admission,
            )
            .await?;
        let ApiProviderTurnAdmission::New(prepared) = admission else {
            let ApiProviderTurnAdmission::Replay(response) = admission else {
                unreachable!()
            };
            return Ok(ForegroundAcceptance {
                response,
                _release: None,
            });
        };
        // No prepared payload is retained for later handoff above this ceiling.
        bounded_payload(&(
            &prepared.outcome.response,
            &prepared.outcome.started_notification,
            &prepared.outcome.started_notification_connection_ids,
            &prepared.outcome.materialization.thread,
            &prepared.outcome.materialization.turn,
            &prepared.outcome.materialization.input,
            &prepared.outcome.materialization.capabilities,
            (
                &prepared.user_message_capability_attachments,
                &prepared.workspace_skill_policies,
                &prepared.skill_catalog,
                &prepared.agent_skill_overlay,
                &prepared.resolved_artifacts,
                &prepared.runtime_environment,
                &prepared.history,
                &prepared.effective_reasoning_effort,
                &prepared.permission_profile,
                &prepared.execution_security_snapshot,
            ),
            (
                &prepared.outcome.rollback_context.previous_preview,
                &prepared.outcome.rollback_context.previous_preview_author,
                &prepared.outcome.rollback_context.previous_model,
                &prepared.outcome.rollback_context.previous_model_provider,
                &prepared.outcome.rollback_context.previous_reasoning_effort,
            ),
        ))?;
        self.emit_user_message_item_lifecycle(
            &prepared.outcome.started_notification.workspace_id,
            &prepared.outcome.started_notification.thread_id,
            &prepared.outcome.started_notification.turn.id,
            &prepared.outcome.materialization.input,
            &prepared.user_message_capability_attachments,
        )
        .await
        .map_err(|error| {
            TurnStartFailure::internal(format!(
                "failed to commit native turn start lifecycle: {error:#}"
            ))
        })?;
        let execution = entry
            .execution
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| TurnStartFailure::internal("original startup execution is missing"))?;
        let actor_generation = *entry
            .actor_generation
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let actor_generation = actor_generation
            .ok_or_else(|| TurnStartFailure::internal("startup actor is missing"))?;
        entry.mandatory_ready.store(true, Ordering::Release);
        let response = prepared.outcome.response.clone();
        let (release, after_reply) = tokio::sync::oneshot::channel();
        self.spawn_foreground_start_publication(&prepared.outcome, entry.clone(), after_reply);
        #[cfg(test)]
        self.completed_history_preparation_barrier
            .wait_if_armed(
                "__foreground_start_before_dispatch__",
                &self.foreground_starts.shutdown,
            )
            .await;
        let receipt = self
            .dispatch_prepared_api_provider_turn_start_current(
                prepared,
                Some((&execution, actor_generation)),
            )
            .await
            .map_err(|error| {
                TurnStartFailure::unavailable(format!(
                    "native startup handoff not confirmed: {error}"
                ))
            })?;
        if let Some(receipt) = receipt {
            if let Err(error) = self
                .await_foreground_activation(&entry, &execution, &receipt)
                .await
            {
                let _ = self
                    .agent_manager
                    .cancel_started_turn(
                        &execution.thread_id,
                        &receipt,
                        "initial activation was not confirmed",
                        Some((&execution.owner_id, execution.owner_generation)),
                    )
                    .await;
                return Err(error);
            }
        }
        Ok(ForegroundAcceptance {
            response,
            _release: Some(release),
        })
    }

    async fn await_foreground_activation(
        &self,
        entry: &StartObligation,
        expected: &pioneer_crud::TurnExecutionRecord,
        receipt: &pioneer_agent::AgentStartReceipt,
    ) -> Result<(), TurnStartFailure> {
        loop {
            let changed = entry.activated.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let activation = entry
                .activation
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if matches!(activation, Some(Ok(()))) {
                return Ok(());
            }
            if self
                .crud_store
                .native_turn_has_accepted_terminal(&expected.turn_id)
                .await
                .map_err(|e| TurnStartFailure::internal(e.to_string()))?
            {
                return Ok(());
            }
            if let Some(Err(reason)) = activation {
                return Err(TurnStartFailure::unavailable(reason));
            }
            if !self
                .agent_manager
                .start_receipt_is_current(&expected.thread_id, receipt)
                .await
            {
                return Err(TurnStartFailure::unavailable(
                    "startup actor retired before activation",
                ));
            }
            tokio::select! {
                _ = self.foreground_starts.shutdown.cancelled() => return Err(TurnStartFailure::unavailable("startup shutdown before activation")),
                _ = changed => {},
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
        }
    }

    fn spawn_foreground_start_publication(
        &self,
        outcome: &crate::thread::TurnStartOutcome,
        entry: Arc<StartObligation>,
        after_reply: tokio::sync::oneshot::Receiver<()>,
    ) {
        let notification = outcome.started_notification.clone();
        let recipients = outcome.started_notification_connection_ids.clone();
        let sidebar_visible = outcome.materialization.thread.sidebar_visibility
            == pioneer_protocol::ThreadSidebarVisibility::Visible;
        let title = (outcome.materialization.thread.name.is_none()
            && matches!(
                outcome.materialization.thread.origin_kind,
                pioneer_protocol::ThreadOriginKind::User
                    | pioneer_protocol::ThreadOriginKind::Collaborative
                    | pioneer_protocol::ThreadOriginKind::DirectMessage
            ))
        .then(|| first_user_text(&outcome.materialization.input));
        let processor = self.clone();
        let state = self
            .foreground_starts
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.closed {
            entry.finish_publication();
            return;
        }
        let guard = PublicationGuard(entry);
        guard.0.publisher_started.store(true, Ordering::Release);
        self.foreground_starts.tasks.spawn(async move {
            let publish = async {
                let _ = after_reply.await;
                #[cfg(test)]
                processor
                    .completed_history_preparation_barrier
                    .wait_if_armed(
                        "__foreground_start_before_publication__",
                        &processor.foreground_starts.shutdown,
                    )
                    .await;
                processor
                    .send_notification_to_authorized_thread_connections(
                        &notification.thread_id,
                        events::TURN_STARTED,
                        &notification,
                        recipients,
                    )
                    .await;
            };
            tokio::select! {
                _ = processor.foreground_starts.shutdown.cancelled() => {},
                _ = publish => {},
            }
            let capacity = guard.0.clone();
            drop(guard);
            processor.kick_native_turn_event_deliveries();
            // No dependent live delivery waits for unread or title preparation.
            let extras = async {
                if sidebar_visible {
                    processor.dirty_push(
                        &notification.workspace_id,
                        Some(&notification.thread_id),
                        None,
                        None,
                        false,
                        None,
                    );
                    processor
                        .notify_thread_unread_legacy(
                            &notification.workspace_id,
                            &notification.thread_id,
                        )
                        .await;
                }
                if let Some(title) = title {
                    processor.spawn_initial_thread_title_task(notification.thread_id, title);
                }
            };
            #[cfg(test)]
            let extras = FOREGROUND_UNREAD_EXTRAS.scope((), extras);
            tokio::select! {
                _ = processor.foreground_starts.shutdown.cancelled() => {},
                _ = extras => {},
            }
            drop(capacity);
        });
    }
}
