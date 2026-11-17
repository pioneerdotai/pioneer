//! Connection-local, cursor-based PTY streams. Credit bounds unread socket data;
//! the execution owner retains the replay ring across connection replacement.
use super::{ExecutionAuthority, pty};
use anyhow::{Context as _, Result, bail};
use pioneer_protocol::JsonRpcNotification;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

const WINDOW: u64 = 64 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Subscribe {
    session_id: String,
    stream_id: String,
    after: u64,
}

struct Subscription {
    stream_id: String,
    sent: u64,
    acknowledged: u64,
    frames: VecDeque<u64>,
    last_state: Option<(Option<u64>, bool)>,
}

#[derive(Default)]
pub(super) struct Streams {
    entries: BTreeMap<String, Subscription>,
    last_session: Option<String>,
    streaming: bool,
}

impl Streams {
    pub(super) fn is_streaming(&self) -> bool {
        self.streaming
    }

    pub(super) async fn dispatch(
        &mut self,
        scope: &Arc<ExecutionAuthority>,
        method: &str,
        params: Value,
    ) -> Result<Value> {
        match method {
            "host.pty.subscribe" => {
                let p: Subscribe = serde_json::from_value(params).context("host.invalid_params")?;
                if p.stream_id.is_empty() || p.stream_id.len() > 128 {
                    bail!("host.invalid_params");
                }
                if !self.entries.contains_key(&p.session_id)
                    && self.entries.len() >= pty::MAX_SESSIONS
                {
                    bail!("host.capacity");
                }
                // Validate the retained cursor under the same resource admission
                // as ordinary calls, before replacing this connection's stream.
                operate(
                    scope,
                    "host.pty.read",
                    json!({"session_id":p.session_id,"after":p.after}),
                )
                .await?;
                self.streaming = true;
                self.entries.insert(
                    p.session_id,
                    Subscription {
                        stream_id: p.stream_id,
                        sent: p.after,
                        acknowledged: p.after,
                        frames: VecDeque::new(),
                        last_state: None,
                    },
                );
                Ok(json!({"subscribed":true,"window":WINDOW}))
            }
            "host.pty.ack" => {
                let p: Subscribe = serde_json::from_value(params).context("host.invalid_params")?;
                operate(
                    scope,
                    "host.pty.keepalive",
                    json!({"session_id":p.session_id}),
                )
                .await?;
                let Some(stream) = self
                    .entries
                    .get_mut(&p.session_id)
                    .filter(|s| s.stream_id == p.stream_id)
                else {
                    // A retry from a superseded connection grants no credit to
                    // the replacement subscription.
                    return Ok(json!({"acknowledged":false}));
                };
                if p.after > stream.sent {
                    bail!("host.invalid_cursor");
                }
                stream.acknowledged = stream.acknowledged.max(p.after);
                while stream
                    .frames
                    .front()
                    .is_some_and(|end| *end <= stream.acknowledged)
                {
                    stream.frames.pop_front();
                }
                Ok(json!({"acknowledged":true}))
            }
            _ => {
                let session = params
                    .get("session_id")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let result = operate(scope, method, params).await?;
                if method == "host.pty.close" {
                    if let Some(session) = session {
                        self.entries.remove(&session);
                    }
                }
                Ok(result)
            }
        }
    }

    pub(super) fn next_event(&mut self, scope: &ExecutionAuthority) -> Option<String> {
        let mut sessions: Vec<_> = self.entries.keys().cloned().collect();
        // Round robin across sessions, including when one PTY never becomes idle.
        if let Some(last) = &self.last_session {
            let pivot = sessions.partition_point(|id| id <= last);
            sessions.rotate_left(pivot);
        }
        let mut owner = scope.ptys.lock().unwrap_or_else(|e| e.into_inner());
        if scope.cancel.is_cancelled() {
            return None;
        }
        for session in sessions {
            let stream = self.entries.get_mut(&session)?;
            let capacity = if stream.frames.len() >= 64 {
                0
            } else {
                WINDOW.saturating_sub(stream.sent - stream.acknowledged) as usize
            };
            let result = owner.read_output(&session, stream.sent, capacity);
            let params = match result {
                Ok(mut output) => {
                    let next = output["next"].as_u64()?;
                    let end = output["end"].as_u64()?;
                    let state = (output["exit_status"].as_u64(), output["failed"].as_bool()?);
                    // A full window sleeps until ACK, even if the producer is
                    // still appending. Failure is never hidden by backpressure.
                    if capacity == 0 && !state.1 && !(state.0.is_some() && end == next) {
                        continue;
                    }
                    if next == stream.sent && stream.last_state == Some(state) {
                        continue;
                    }
                    let after = stream.sent;
                    if next > after {
                        stream.frames.push_back(next);
                    }
                    stream.sent = next;
                    stream.last_state = Some(state);
                    output["session_id"] = json!(session);
                    output["stream_id"] = json!(stream.stream_id);
                    output["after"] = json!(after);
                    output
                }
                Err(error) => {
                    let code = match error.to_string().as_str() {
                        "host.output_cursor_expired" => "host.output_cursor_expired",
                        "host.session_expired" => "host.session_expired",
                        "host.session_unavailable" => "host.session_unavailable",
                        _ => "host.operation_failed",
                    };
                    let params =
                        json!({"session_id":session,"stream_id":stream.stream_id,"error":code});
                    self.entries.remove(&session);
                    params
                }
            };
            self.last_session = Some(session);
            return JsonRpcNotification::from_params("host.pty.output", &params)
                .and_then(|event| serde_json::to_string(&event))
                .ok();
        }
        None
    }
}

async fn operate(scope: &Arc<ExecutionAuthority>, method: &str, params: Value) -> Result<Value> {
    let scope = scope.clone();
    let method = method.to_owned();
    tokio::task::spawn_blocking(move || scope.operate(&method, params))
        .await
        .context("host.operation_failed")?
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::host_api::HostApi;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn pushed_output_obeys_cumulative_credit_and_revocation() {
        let api = HostApi::default();
        let (_, authority) = api.issue(
            "app".into(),
            "workspace".into(),
            "execution".into(),
            CancellationToken::new(),
        );
        let created = operate(
            &authority,
            "host.pty.create",
            json!({
                "key":"output", "command":["/bin/sh","-c",
                    "i=0; while [ \"$i\" -lt 20 ]; do printf '%s' \"$1\"; i=$((i+1)); done",
                    "sh", "x".repeat(8192)],
                "dimensions":{"rows":24,"cols":80}
            }),
        )
        .await
        .unwrap();
        let session = created["session_id"].as_str().unwrap();
        let mut streams = Streams::default();
        streams
            .dispatch(
                &authority,
                "host.pty.subscribe",
                json!({"session_id":session,"stream_id":"stream","after":0}),
            )
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut delivered = Vec::new();
        let collect = |streams: &mut Streams, delivered: &mut Vec<u8>| {
            while let Some(event) = streams.next_event(&authority) {
                let event: Value = serde_json::from_str(&event).unwrap();
                let output = &event["params"];
                assert!(
                    output.get("error").is_none(),
                    "unexpected output failure: {output}"
                );
                assert_eq!(output["after"].as_u64().unwrap(), delivered.len() as u64);
                delivered.extend(
                    output["bytes"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|byte| byte.as_u64().unwrap() as u8),
                );
                assert_eq!(output["next"].as_u64().unwrap(), delivered.len() as u64);
            }
        };
        while delivered.len() < 64 * 1024 {
            collect(&mut streams, &mut delivered);
            assert!(
                tokio::time::Instant::now() < deadline,
                "PTY did not fill the stream window"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(delivered, vec![b'x'; 64 * 1024]);
        assert!(
            streams.next_event(&authority).is_none(),
            "unacknowledged output exceeded its credit"
        );
        streams
            .dispatch(
                &authority,
                "host.pty.ack",
                json!({"session_id":session,"stream_id":"stream","after":32*1024}),
            )
            .await
            .unwrap();
        while delivered.len() < 96 * 1024 {
            collect(&mut streams, &mut delivered);
            assert!(
                tokio::time::Instant::now() < deadline,
                "ACK did not release output credit"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(delivered, vec![b'x'; 96 * 1024]);
        for stream in ["stream", "superseded-stream"] {
            streams
                .dispatch(
                    &authority,
                    "host.pty.ack",
                    json!({"session_id":session,"stream_id":stream,"after":32*1024}),
                )
                .await
                .unwrap();
            assert!(
                streams.next_event(&authority).is_none(),
                "duplicate or stale ACK minted more credit"
            );
        }
        assert!(
            streams
                .dispatch(
                    &authority,
                    "host.pty.ack",
                    json!({"session_id":session,"stream_id":"stream","after":96*1024+1})
                )
                .await
                .is_err()
        );
        api.fence_app("app");
        assert!(streams.next_event(&authority).is_none());
        assert!(
            streams
                .dispatch(
                    &authority,
                    "host.pty.keepalive",
                    json!({"session_id":session})
                )
                .await
                .is_err()
        );
        api.revoke_execution("execution");
    }
}
