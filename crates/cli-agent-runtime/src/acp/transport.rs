//! Bounded JSONL transport. One ingress orders notifications and responses.

use super::schema;
use crate::{BoundedNativeEventCodec, NativeEventBudget, SpooledNativeFrame};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    Number(i64),
    String(String),
}

#[derive(Clone, Copy, Debug)]
pub struct TransportLimits {
    pub frame_bytes: usize,
    pub queued_frames: usize,
    pub in_flight: usize,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 1024 * 1024,
            queued_frames: 32,
            in_flight: 32,
        }
    }
}

impl TransportLimits {
    pub fn validate(self) -> schema::Result<Self> {
        if self.frame_bytes == 0 || self.queued_frames == 0 || self.in_flight == 0 {
            return Err(schema::Error::invalid_params());
        }
        Ok(self)
    }
}

/// Wire ingress only, never a second canonical event model or history store.
pub enum Incoming {
    Request {
        id: RequestId,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
        params: Value,
    },
    Response {
        id: RequestId,
        result: schema::Result<Value>,
    },
    InvalidResponse,
    Invalid {
        id: Value,
        error: schema::Error,
    },
}

struct Pending {
    method: String,
    params: Value,
    sender: oneshot::Sender<schema::Result<Value>>,
}

struct State {
    next_id: i64,
    pending: HashMap<RequestId, Pending>,
    closed: bool,
}

struct Shared {
    limits: TransportLimits,
    state: Mutex<State>,
    outgoing: mpsc::Sender<Vec<u8>>,
    cancelled: CancellationToken,
    callbacks: Mutex<HashMap<RequestId, CallbackControl>>,
    cancelled_sessions: Mutex<HashSet<String>>,
}

pub(crate) struct CallbackControl {
    pub session: Option<String>,
    pub permission: bool,
    pub token: CancellationToken,
    pub parent_request: Option<RequestId>,
}

impl Shared {
    fn close(&self) {
        let pending = {
            let mut state = self.state.lock().expect("ACP transport state poisoned");
            state.closed = true;
            std::mem::take(&mut state.pending)
        };
        self.cancelled.cancel();
        for (_, pending) in pending {
            let _ = pending.sender.send(Err(schema::Error::new(
                -32603,
                "ACP connection closed; dispatched requests have unknown outcomes",
            )));
        }
    }
}

#[derive(Clone)]
pub struct Sender(Arc<Shared>);

/// A request is registered before its frame can be written. Dropping it only
/// cancels this request; it never resubmits work or substitutes session/cancel.
pub struct PendingRequest {
    id: RequestId,
    receiver: oneshot::Receiver<schema::Result<Value>>,
    sender: Sender,
    complete: bool,
}

impl PendingRequest {
    pub fn id(&self) -> &RequestId {
        &self.id
    }

    pub async fn receive(mut self) -> schema::Result<Value> {
        let result = (&mut self.receiver)
            .await
            .unwrap_or_else(|_| Err(schema::Error::internal_error()));
        self.complete = true;
        result
    }
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let exists = self
            .sender
            .0
            .state
            .lock()
            .expect("ACP transport state poisoned")
            .pending
            .contains_key(&self.id);
        if exists {
            self.sender.cancel_nested(&self.id);
            // A full cancellation queue is a connection failure, never an
            // excuse to leave an unowned request executing indefinitely.
            if self
                .sender
                .enqueue(&json!({"jsonrpc":"2.0","method":"$/cancel_request",
                "params":{"requestId":self.id}}))
                .is_err()
            {
                self.sender.close();
            }
        }
    }
}

impl Sender {
    fn enqueue(&self, frame: &impl Serialize) -> schema::Result<()> {
        if self.0.cancelled.is_cancelled() {
            return Err(schema::Error::internal_error());
        }
        let bytes = encode(frame, self.0.limits.frame_bytes)?;
        self.0
            .outgoing
            .try_send(bytes)
            .map_err(|_| schema::Error::new(-32603, "ACP output queue unavailable"))
    }

    /// Production preflight of the exact JSON-RPC envelope without enqueueing.
    pub fn validate_request<P: Serialize>(&self, method: &str, params: &P) -> schema::Result<()> {
        // Reserve the largest numeric request id so later allocations cannot
        // make an admitted frame larger than this validated representation.
        encode(
            &RequestFrame {
                jsonrpc: "2.0",
                id: &RequestId::Number(i64::MAX),
                method,
                params,
            },
            self.0.limits.frame_bytes,
        )
        .map(|_| ())
    }

    pub fn request<P: Serialize>(
        &self,
        method: &str,
        params: &P,
    ) -> schema::Result<PendingRequest> {
        let (tx, rx) = oneshot::channel();
        let mut state = self.0.state.lock().expect("ACP transport state poisoned");
        if state.closed
            || self.0.cancelled.is_cancelled()
            || state.pending.len() >= self.0.limits.in_flight
        {
            return Err(schema::Error::new(
                -32603,
                "ACP request capacity unavailable",
            ));
        }
        let id = RequestId::Number(state.next_id);
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or_else(schema::Error::internal_error)?;
        // Serialize inside the registration boundary so a failed enqueue has
        // no observable pending entry. Payload serialization is byte bounded.
        let bytes = encode(
            &RequestFrame {
                jsonrpc: "2.0",
                id: &id,
                method,
                params,
            },
            self.0.limits.frame_bytes,
        )?;
        let mut frame: Value =
            serde_json::from_slice(&bytes).map_err(|_| schema::Error::internal_error())?;
        let pending_params = frame
            .get_mut("params")
            .map(Value::take)
            .unwrap_or(Value::Null);
        self.0
            .outgoing
            .try_send(bytes)
            .map_err(|_| schema::Error::new(-32603, "ACP output queue unavailable"))?;
        state.pending.insert(
            id.clone(),
            Pending {
                method: method.to_owned(),
                params: pending_params,
                sender: tx,
            },
        );
        Ok(PendingRequest {
            id,
            receiver: rx,
            sender: self.clone(),
            complete: false,
        })
    }

    pub fn notify<P: Serialize>(&self, method: &str, params: &P) -> schema::Result<()> {
        self.enqueue(&NotificationFrame {
            jsonrpc: "2.0",
            method,
            params,
        })
    }

    pub fn respond(&self, id: &RequestId, result: schema::Result<Value>) -> schema::Result<()> {
        match &result {
            Ok(result) => self.enqueue(&ResponseFrame {
                jsonrpc: "2.0",
                id,
                result: Some(result),
                error: None,
            }),
            Err(error) => self.enqueue(&ResponseFrame {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(error),
            }),
        }
    }

    pub(crate) fn invalid(&self, id: Value, error: schema::Error) -> schema::Result<()> {
        let id = if serde_json::from_value::<RequestId>(id.clone()).is_ok() {
            id
        } else {
            Value::Null
        };
        self.enqueue(&json!({"jsonrpc":"2.0","id":id,"error":error}))
    }

    pub fn pending_method(&self, id: &RequestId) -> Option<String> {
        self.0
            .state
            .lock()
            .expect("ACP transport state poisoned")
            .pending
            .get(id)
            .map(|p| p.method.clone())
    }

    pub(crate) fn pending_context(&self, id: &RequestId) -> Option<(String, Value)> {
        self.0
            .state
            .lock()
            .expect("ACP transport state poisoned")
            .pending
            .get(id)
            .map(|p| (p.method.clone(), p.params.clone()))
    }

    /// Called by the single ordered dispatcher AFTER projecting preceding
    /// notifications and this response. A waiter cannot race ahead of output.
    pub(crate) fn complete(&self, id: &RequestId, result: schema::Result<Value>) {
        let pending = self
            .0
            .state
            .lock()
            .expect("ACP transport state poisoned")
            .pending
            .remove(id);
        self.cancel_nested(id);
        if let Some(pending) = pending {
            let _ = pending.sender.send(result);
        }
    }

    fn cancel_nested(&self, id: &RequestId) {
        for control in self
            .0
            .callbacks
            .lock()
            .expect("ACP callback state poisoned")
            .values()
        {
            if control.parent_request.as_ref() == Some(id) {
                control.token.cancel();
            }
        }
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.0.cancelled.clone()
    }
    pub fn close(&self) {
        self.0.close();
    }

    pub(crate) fn register_callback(
        &self,
        id: RequestId,
        control: CallbackControl,
    ) -> schema::Result<()> {
        let state = self.0.state.lock().expect("ACP transport state poisoned");
        if control
            .parent_request
            .as_ref()
            .is_some_and(|id| !state.pending.contains_key(id))
        {
            control.token.cancel();
        }
        let cancelled = self
            .0
            .cancelled_sessions
            .lock()
            .expect("ACP cancellation state poisoned");
        let mut callbacks = self
            .0
            .callbacks
            .lock()
            .expect("ACP callback state poisoned");
        if callbacks.contains_key(&id) {
            return Err(schema::Error::invalid_request());
        }
        if control.permission
            && control
                .session
                .as_ref()
                .is_some_and(|s| cancelled.contains(s))
        {
            control.token.cancel();
        }
        callbacks.insert(id, control);
        Ok(())
    }

    pub(crate) fn respond_callback(
        &self,
        id: &RequestId,
        result: schema::Result<Value>,
        cancelled_result: schema::Result<Value>,
    ) -> schema::Result<bool> {
        // Shares the cancellation linearization boundary with session/cancel
        // and $/cancel_request. A completed host future is still pending until
        // its response has entered the bounded writer queue.
        let cancelled_sessions = self
            .0
            .cancelled_sessions
            .lock()
            .expect("ACP cancellation state poisoned");
        let mut callbacks = self
            .0
            .callbacks
            .lock()
            .expect("ACP callback state poisoned");
        let control = callbacks
            .get(id)
            .ok_or_else(schema::Error::request_cancelled)?;
        let cancelled = control.token.is_cancelled()
            || (control.permission
                && control
                    .session
                    .as_ref()
                    .is_some_and(|s| cancelled_sessions.contains(s)));
        let result = if cancelled { cancelled_result } else { result };
        let accepted = result
            .as_ref()
            .is_ok_and(|r| r.get("action").and_then(Value::as_str) == Some("accept"));
        self.respond(id, result)?;
        callbacks.remove(id);
        Ok(accepted)
    }

    pub(crate) fn cancel_callback(&self, id: &RequestId) {
        if let Some(control) = self
            .0
            .callbacks
            .lock()
            .expect("ACP callback state poisoned")
            .get(id)
        {
            control.token.cancel();
        }
    }

    pub(crate) fn cancel_permissions(&self, session_id: &str) {
        self.0
            .cancelled_sessions
            .lock()
            .expect("ACP cancellation state poisoned")
            .insert(session_id.to_owned());
        for control in self
            .0
            .callbacks
            .lock()
            .expect("ACP callback state poisoned")
            .values()
        {
            if control.permission && control.session.as_deref() == Some(session_id) {
                control.token.cancel();
            }
        }
    }

    pub(crate) fn cancel_session(&self, session_id: &str) {
        self.0
            .cancelled_sessions
            .lock()
            .expect("ACP cancellation state poisoned")
            .remove(session_id);
        for control in self
            .0
            .callbacks
            .lock()
            .expect("ACP callback state poisoned")
            .values()
        {
            if control.session.as_deref() == Some(session_id) {
                control.token.cancel();
            }
        }
    }

    pub(crate) fn begin_prompt(&self, session_id: &str) {
        self.0
            .cancelled_sessions
            .lock()
            .expect("ACP cancellation state poisoned")
            .remove(session_id);
    }
}

pub struct Transport {
    pub sender: Sender,
    pub incoming: mpsc::Receiver<Incoming>,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
}

impl Transport {
    pub fn connect<R, W>(reader: R, mut writer: W, limits: TransportLimits) -> schema::Result<Self>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let limits = limits.validate()?;
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(limits.queued_frames);
        let (ingress, incoming) = mpsc::channel(limits.queued_frames);
        let shared = Arc::new(Shared {
            limits,
            state: Mutex::new(State {
                next_id: 1,
                pending: HashMap::new(),
                closed: false,
            }),
            outgoing: tx,
            cancelled: CancellationToken::new(),
            callbacks: Mutex::new(HashMap::new()),
            cancelled_sessions: Mutex::new(HashSet::new()),
        });
        let sender = Sender(shared.clone());
        let reader_shared = shared.clone();
        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(reader);
            let codec = BoundedNativeEventCodec::new(NativeEventBudget {
                profile_version: 2,
                max_frame_bytes: limits.frame_bytes,
                max_recovery_frame_bytes: limits.frame_bytes,
            });
            loop {
                let read = tokio::select! {
                    biased;
                    _ = reader_shared.cancelled.cancelled() => break,
                    read = codec.read_frame_or_spool(&mut reader) => read,
                };
                let Ok(Some(SpooledNativeFrame::Complete(bytes))) = read else {
                    break;
                };
                let frame = decode(&bytes);
                let sent = tokio::select! {
                    biased;
                    _ = reader_shared.cancelled.cancelled() => break,
                    sent = ingress.send(frame) => sent,
                };
                if sent.is_err() {
                    break;
                }
            }
            // EOF is represented by closing ingress. The ordered dispatcher
            // drains it before failing requests, preserving a buffered terminal
            // response immediately followed by EOF.
        });
        let writer_shared = shared;
        let writer = tokio::spawn(async move {
            loop {
                let bytes = tokio::select! {
                    biased;
                    _ = writer_shared.cancelled.cancelled() => break,
                    bytes = rx.recv() => match bytes { Some(bytes) => bytes, None => break },
                };
                let result = tokio::select! {
                    biased;
                    _ = writer_shared.cancelled.cancelled() => break,
                    result = writer.write_all(&bytes) => result,
                };
                if result.is_err() {
                    writer_shared.close();
                    break;
                }
            }
        });
        Ok(Self {
            sender,
            incoming,
            reader,
            writer,
        })
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.sender.close();
        self.reader.abort();
        self.writer.abort();
    }
}

fn decode(bytes: &[u8]) -> Incoming {
    let frame: Value = match serde_json::from_slice(bytes) {
        Ok(value) => value,
        Err(_) => {
            return Incoming::Invalid {
                id: Value::Null,
                error: schema::Error::parse_error(),
            };
        }
    };
    let id = frame.get("id").cloned().unwrap_or(Value::Null);
    if !frame.is_object() || frame.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Incoming::Invalid {
            id: Value::Null,
            error: schema::Error::invalid_request(),
        };
    }
    if let Some(method) = frame.get("method") {
        let Some(method) = method.as_str() else {
            return Incoming::Invalid {
                id,
                error: schema::Error::invalid_request(),
            };
        };
        if frame.get("result").is_some() || frame.get("error").is_some() {
            return Incoming::Invalid {
                id,
                error: schema::Error::invalid_request(),
            };
        }
        let params = frame.get("params").cloned().unwrap_or(json!({}));
        if frame.get("id").is_none() {
            return Incoming::Notification {
                method: method.to_owned(),
                params,
            };
        }
        return match serde_json::from_value(id) {
            Ok(id) => Incoming::Request {
                id,
                method: method.to_owned(),
                params,
            },
            Err(_) => Incoming::Invalid {
                id: Value::Null,
                error: schema::Error::invalid_request(),
            },
        };
    }
    let Ok(id) = serde_json::from_value(id) else {
        return Incoming::InvalidResponse;
    };
    let result = match (frame.get("result"), frame.get("error")) {
        (Some(result), None) => Ok(result.clone()),
        (None, Some(error)) => match serde_json::from_value::<schema::Error>(error.clone()) {
            Ok(error) => Err(error),
            Err(_) => return Incoming::InvalidResponse,
        },
        _ => return Incoming::InvalidResponse,
    };
    Incoming::Response { id, result }
}

struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("ACP output frame too large"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Serialize)]
struct RequestFrame<'a, P> {
    jsonrpc: &'static str,
    id: &'a RequestId,
    method: &'a str,
    params: &'a P,
}

#[derive(Serialize)]
struct NotificationFrame<'a, P> {
    jsonrpc: &'static str,
    method: &'a str,
    params: &'a P,
}

#[derive(Serialize)]
struct ResponseFrame<'a> {
    jsonrpc: &'static str,
    id: &'a RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a schema::Error>,
}

fn encode(value: &impl Serialize, limit: usize) -> schema::Result<Vec<u8>> {
    let mut writer = LimitedBuffer {
        bytes: Vec::new(),
        limit: limit.saturating_sub(1),
    };
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| schema::Error::new(-32603, "ACP output frame exceeds budget"))?;
    writer.bytes.push(b'\n');
    Ok(writer.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_typed_and_null_request_is_invalid() {
        let Incoming::Request { id, .. } = decode(br#"{"jsonrpc":"2.0","id":"7","method":"test"}"#)
        else {
            panic!()
        };
        assert_eq!(id, RequestId::String("7".into()));
        assert!(matches!(
            decode(br#"{"jsonrpc":"2.0","id":null,"method":"test"}"#),
            Incoming::Invalid { .. }
        ));
        assert!(matches!(decode(b"[]"), Incoming::Invalid { .. }));
        assert!(matches!(
            decode(br#"{"jsonrpc":"2.0","id":1,"result":{},"error":{}}"#),
            Incoming::InvalidResponse
        ));
    }

    #[test]
    fn output_budget_counts_utf8_and_json_escaping() {
        assert!(encode(&json!("я\nя"), 5).is_err());
        let bytes = encode(&json!("я"), 6).unwrap();
        assert_eq!(bytes, "\"я\"\n".as_bytes());
    }

    #[tokio::test]
    async fn buffered_updates_and_terminal_response_precede_eof() {
        let (client_read, mut agent_write) = tokio::io::duplex(4096);
        let (agent_read, client_write) = tokio::io::duplex(4096);
        let mut transport =
            Transport::connect(client_read, client_write, TransportLimits::default()).unwrap();
        let mut pending = transport
            .sender
            .request("session/prompt", &json!({"sessionId":"s","prompt":[]}))
            .unwrap();
        agent_write.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{}}\n{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"stopReason\":\"end_turn\"}}\n").await.unwrap();
        drop(agent_write);
        assert!(matches!(
            transport.incoming.recv().await,
            Some(Incoming::Notification { .. })
        ));
        let Some(Incoming::Response { id, result }) = transport.incoming.recv().await else {
            panic!()
        };
        // Receiving the wire response alone cannot release the request waiter.
        assert!(pending.receiver.try_recv().is_err());
        transport.sender.complete(&id, result);
        assert_eq!(pending.receive().await.unwrap()["stopReason"], "end_turn");
        assert!(transport.incoming.recv().await.is_none());
        drop(agent_read);
    }

    #[tokio::test]
    async fn dropped_request_keeps_correlation_and_capacity_until_outcome() {
        let (client_read, _agent_write) = tokio::io::duplex(4096);
        let (_agent_read, client_write) = tokio::io::duplex(4096);
        let transport = Transport::connect(
            client_read,
            client_write,
            TransportLimits {
                in_flight: 1,
                ..Default::default()
            },
        )
        .unwrap();
        let pending = transport
            .sender
            .request("session/prompt", &json!({"sessionId":"s","prompt":[]}))
            .unwrap();
        let id = pending.id().clone();
        drop(pending);
        assert_eq!(
            transport.sender.pending_method(&id).as_deref(),
            Some("session/prompt")
        );
        assert!(transport.sender.request("session/new", &json!({})).is_err());
        transport
            .sender
            .complete(&id, Ok(json!({"stopReason":"cancelled"})));
        assert!(transport.sender.request("session/new", &json!({})).is_ok());
    }

    #[tokio::test]
    async fn oversized_frame_ends_ingress_without_spooling() {
        let (client_read, mut agent_write) = tokio::io::duplex(4096);
        let (_agent_read, client_write) = tokio::io::duplex(4096);
        let mut transport = Transport::connect(
            client_read,
            client_write,
            TransportLimits {
                frame_bytes: 128,
                ..Default::default()
            },
        )
        .unwrap();
        agent_write.write_all(&[b'x'; 129]).await.unwrap();
        assert!(transport.incoming.recv().await.is_none());
    }

    #[tokio::test]
    async fn cancel_overrides_permission_result_before_it_enters_writer_queue() {
        use tokio::io::AsyncBufReadExt;
        let (client_read, _agent_write) = tokio::io::duplex(4096);
        let (agent_read, client_write) = tokio::io::duplex(4096);
        let transport =
            Transport::connect(client_read, client_write, TransportLimits::default()).unwrap();
        let id = RequestId::String("permission".into());
        transport
            .sender
            .register_callback(
                id.clone(),
                CallbackControl {
                    session: Some("s".into()),
                    permission: true,
                    token: CancellationToken::new(),
                    parent_request: None,
                },
            )
            .unwrap();
        transport.sender.cancel_permissions("s");
        transport
            .sender
            .respond_callback(
                &id,
                Ok(json!({"outcome":{"outcome":"selected","optionId":"allow"}})),
                Ok(json!({"outcome":{"outcome":"cancelled"}})),
            )
            .unwrap();
        let mut reader = BufReader::new(agent_read);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let frame: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(frame["result"]["outcome"]["outcome"], "cancelled");
    }
}
