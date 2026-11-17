//! One bounded sequential audio worker per physical connection. JoinSet aborts on Drop.
use crate::voice::{ingress::VoiceIngressTicket, session_store::GatewayVoiceSession};
use crate::{message::MessageProcessor, session::SessionOutbound};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;

const MAX_FRAMES: usize = 128;
const MAX_BYTES: usize = 4 * 1024 * 1024;
struct Item {
    payload: axum::body::Bytes,
    ticket: VoiceIngressTicket,
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}
pub(super) struct VoiceIngressOwner {
    processor: Arc<MessageProcessor>,
    sender: mpsc::Sender<Item>,
    failures: Arc<std::sync::Mutex<Option<GatewayVoiceSession>>>,
    failure_ready: Arc<tokio::sync::Notify>,
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    worker: JoinSet<()>,
    #[cfg(test)]
    pause: Arc<
        std::sync::Mutex<
            Option<(
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            )>,
        >,
    >,
}
impl VoiceIngressOwner {
    pub(super) fn new(
        processor: Arc<MessageProcessor>,
        sessions: Arc<crate::session::SessionManager>,
        auth: Arc<crate::auth::GatewayAuthService>,
        connection_id: crate::session::ConnectionId,
        _outbound: SessionOutbound,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Item>(MAX_FRAMES);
        // Only one failed recording may remain registered until its outcome is
        // admitted. Cancel may remove it sooner; a newer failure then replaces
        // that obsolete mailbox entry (Cancel already supplied its outcome).
        let failures = Arc::new(std::sync::Mutex::new(None::<GatewayVoiceSession>));
        let failure_ready = Arc::new(tokio::sync::Notify::new());
        let failed = failures.clone();
        let failed_ready = failure_ready.clone();
        let mut worker = JoinSet::new();
        let processing = processor.clone();
        #[cfg(test)]
        let pause = Arc::new(std::sync::Mutex::new(
            None::<(
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            )>,
        ));
        #[cfg(test)]
        let worker_pause = pause.clone();
        worker.spawn(async move {
            let mut retained = std::collections::VecDeque::<Item>::new();
            loop {
                tokio::select! {
                    biased;
                    session = async {
                        loop {
                            let ready = failed_ready.notified();
                            tokio::pin!(ready);
                            ready.as_mut().enable();
                            let session = failed.lock().unwrap().take();
                            if let Some(session) = session { break session; }
                            ready.await;
                        }
                    } => {
                        retained.retain(|item| !item.ticket.0.cancelled.is_cancelled());
                        while let Ok(item) = receiver.try_recv() {
                            if !item.ticket.0.cancelled.is_cancelled() { retained.push_back(item); }
                        }
                        let notification = pioneer_protocol::VoiceSessionResultNotification {
                            session_id: session.session_id.clone(),
                            outcome: pioneer_protocol::VoiceSessionOutcome::Failed,
                            turn_id: Some(session.turn_id.clone()),
                            error: Some(pioneer_protocol::VoiceError {
                                kind: pioneer_protocol::VoiceErrorKind::GatewayBusy,
                                message: "The operation exceeds its resource budget.".into(),
                                public_error: Some(crate::public_error::build_public_error(
                                    pioneer_protocol::PublicErrorCode::ResourceExhausted,
                                    pioneer_protocol::PublicErrorStage::Admission)),
                            }),
                        };
                        if let Ok(text) = pioneer_protocol::JsonRpcNotification::from_params(
                            pioneer_protocol::constants::events::VOICE_SESSION_RESULT, &notification)
                            .and_then(|v| serde_json::to_string(&v)) {
                            let _ = sessions.send_voice_text(connection_id, text, true).await;
                        }
                        processing.retire_failed_voice_ingress(&session);
                    }
                    Some(item) = async {
                        if let Some(item) = retained.pop_front() { Some(item) } else { receiver.recv().await }
                    } => {
                        if item.ticket.0.cancelled.is_cancelled() { continue; }
                        tokio::select! {
                            biased;
                            _ = item.ticket.0.cancelled.cancelled() => {},
                            _ = async {
                                #[cfg(test)]
                                {
                                    let gate = worker_pause.lock().unwrap().take();
                                    if let Some((entered, release)) = gate {
                                        let _ = entered.send(());
                                        release.await.expect("synthetic audio ingress gate owner dropped");
                                    }
                                }
                                let Ok(context) = sessions.connection_context(connection_id).await else { return; };
                                if auth.validate_session_lease(context.principal()).await.is_err() { return; }
                                processing.process_binary_frame(&context, &item.payload).await;
                            } => {},
                        }
                    }
                    else => return,
                }
            }
        });
        Self {
            processor,
            sender,
            failures,
            failure_ready,
            count: Arc::new(Semaphore::new(MAX_FRAMES)),
            bytes: Arc::new(Semaphore::new(MAX_BYTES)),
            worker,
            #[cfg(test)]
            pause,
        }
    }
    #[cfg(test)]
    pub(super) fn pause_once(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
    ) {
        *self.pause.lock().unwrap() = Some((entered, release));
    }
    pub(super) fn admit(
        &self,
        connection: &crate::request_context::ConnectionContext,
        payload: &axum::body::Bytes,
    ) -> bool {
        if !payload.starts_with(pioneer_protocol::VOICE_CHUNK_FRAME_MAGIC) {
            return false;
        }
        // Decode/size/ownership validation precedes retention. Unknown sessions
        // get no queue space; classification is never an authorization grant.
        let Some(session) = self.processor.voice_ingress_session(connection, payload) else {
            return true;
        };
        let Some(ticket) = session.ingress.admit() else {
            return true;
        };
        let permits = self.count.clone().try_acquire_owned().ok().zip(
            self.bytes
                .clone()
                .try_acquire_many_owned(payload.len() as u32)
                .ok(),
        );
        if let Some((count, bytes)) = permits {
            let item = Item {
                // A Bytes slice may retain a much larger WebSocket read buffer.
                // Own exactly this validated frame under its byte reservation.
                payload: axum::body::Bytes::copy_from_slice(payload),
                ticket,
                _count: count,
                _bytes: bytes,
            };
            if self.sender.try_send(item).is_ok() {
                return true;
            }
        }
        self.processor.fail_voice_ingress(&session);
        *self.failures.lock().unwrap() = Some(session);
        self.failure_ready.notify_one();
        true
    }
    pub(super) fn seal(&self, connection: &crate::request_context::ConnectionContext, text: &str) {
        self.processor.seal_voice_ingress(connection, text);
    }
    pub(super) fn reap(&mut self) -> anyhow::Result<()> {
        if let Some(result) = self.worker.try_join_next() {
            result?;
            anyhow::bail!("audio worker stopped");
        }
        Ok(())
    }
    pub(super) async fn next(&mut self) -> anyhow::Result<()> {
        if let Some(result) = self.worker.join_next().await {
            result?;
        }
        anyhow::bail!("audio worker stopped")
    }
    pub(super) async fn shutdown(&mut self) {
        self.count.close();
        self.bytes.close();
        self.worker.abort_all();
        while self.worker.join_next().await.is_some() {}
    }
}
