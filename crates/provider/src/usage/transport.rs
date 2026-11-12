//! Request-owned transport observation. Dispatch admission is acknowledged;
//! source-owned post-send facts use a bounded mailbox across decoder tasks and
//! auxiliary open/drain scopes. No sender/ACK is captured by a body producer.
use super::*;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{Notify, mpsc, oneshot};

pub(super) struct BodyObservations {
    pending: Mutex<VecDeque<Value>>,
    ready: Notify,
    losses: Arc<AtomicU64>,
    eof_observed: AtomicU64,
    eof_delivered: AtomicU64,
    eof_lost: AtomicU64,
}
impl BodyObservations {
    pub(super) fn new(losses: Arc<AtomicU64>) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(VecDeque::new()),
            ready: Notify::new(),
            losses,
            eof_observed: AtomicU64::new(0),
            eof_delivered: AtomicU64::new(0),
            eof_lost: AtomicU64::new(0),
        })
    }
    fn lost(&self, eof: bool) {
        self.losses.fetch_add(1, Ordering::Relaxed);
        if eof {
            self.eof_lost.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn enqueue(&self, fact: Value) {
        let eof = fact["phase"] == "provider_body_finished";
        if eof {
            self.eof_observed.fetch_add(1, Ordering::Relaxed);
        }
        let mut pending = self.pending.lock().expect("transport observations");
        if pending.len() == 4 {
            drop(pending);
            self.lost(eof);
            return;
        }
        pending.push_back(fact);
        drop(pending);
        self.ready.notify_one();
    }
    async fn drain<H, HF>(&self, handler: &mut H)
    where
        H: FnMut(Value) -> HF,
        HF: std::future::Future<Output = anyhow::Result<()>>,
    {
        for _ in 0..4 {
            // No lock/DB capacity across owner work, and cancellation accounts
            // for the in-flight fact while leaving the rest in the mailbox.
            let fact = self
                .pending
                .lock()
                .expect("transport observations")
                .pop_front();
            let Some(fact) = fact else {
                break;
            };
            let eof = fact["phase"] == "provider_body_finished";
            let mut delivery = Delivery {
                body: self,
                eof,
                settled: false,
            };
            if handler(fact).await.is_err() {
                self.lost(eof);
            } else if eof {
                self.eof_delivered.fetch_add(1, Ordering::Relaxed);
            }
            delivery.settled = true;
        }
    }
    pub(super) fn coverage(&self) -> Value {
        let pending = self.pending.lock().expect("transport observations");
        json!({"basis":"actual_http_body_polling_only_not_protocol_or_normalized_terminal",
            "body_eof_observed":self.eof_observed.load(Ordering::Relaxed),
            "body_eof_persisted":self.eof_delivered.load(Ordering::Relaxed),
            "body_eof_lost":self.eof_lost.load(Ordering::Relaxed),
            "pending_details":pending.len(),
            "body_eof_coverage":if self.eof_observed.load(Ordering::Relaxed)==0{"unobserved"}else if self.eof_lost.load(Ordering::Relaxed)>0{"detail_loss"}else if !pending.is_empty(){"pending_owner_drain"}else{"source_boundary_delivered"}})
    }
}
struct Delivery<'a> {
    body: &'a BodyObservations,
    eof: bool,
    settled: bool,
}
impl Drop for Delivery<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.body.lost(self.eof);
        }
    }
}
impl Drop for BodyObservations {
    fn drop(&mut self) {
        // Cancellation has no detached persistence work. Any queued facts that
        // outlive their last request owner are explicitly counted as lost.
        let pending = self.pending.get_mut().expect("transport observations");
        self.losses
            .fetch_add(pending.len() as u64, Ordering::Relaxed);
        self.eof_lost.fetch_add(
            pending
                .iter()
                .filter(|v| v["phase"] == "provider_body_finished")
                .count() as u64,
            Ordering::Relaxed,
        );
    }
}
#[derive(Clone)]
struct Sink {
    tx: mpsc::Sender<Packet>,
    body: Weak<BodyObservations>,
    losses: Arc<AtomicU64>,
}
struct Packet {
    fact: Value,
    ack: oneshot::Sender<bool>,
}
#[derive(Clone)]
pub(crate) struct DispatchObservation {
    dispatch_id: String,
    physical_attempt_id: String,
    body: Weak<BodyObservations>,
    losses: Arc<AtomicU64>,
}
impl DispatchObservation {
    fn capture() -> anyhow::Result<Self> {
        let sink = SINK.try_with(Clone::clone)?;
        Ok(Self {
            dispatch_id: pioneer_protocol::generate_id(21),
            physical_attempt_id: ATTEMPT.try_with(Clone::clone)?,
            body: sink.body,
            losses: sink.losses,
        })
    }
    fn record(&self, mut fact: Value) {
        fact["dispatch_id"] = self.dispatch_id.clone().into();
        fact["physical_attempt_id"] = self.physical_attempt_id.clone().into();
        fact.as_object_mut().expect("transport fact").extend(
            super::observation_point()
                .as_object()
                .expect("source point")
                .clone(),
        );
        if let Some(body) = self.body.upgrade() {
            body.enqueue(fact);
        } else {
            self.losses.fetch_add(1, Ordering::Relaxed);
        }
    }
}
tokio::task_local! {static SINK:Sink;static ATTEMPT:String;}
pub(super) async fn attempt_scope<F: std::future::Future>(id: String, future: F) -> F::Output {
    ATTEMPT.scope(id, future).await
}
pub(super) async fn observe_scope<F, H, HF>(
    future: F,
    mut handler: H,
    body: Arc<BodyObservations>,
) -> anyhow::Result<F::Output>
where
    F: std::future::Future,
    H: FnMut(Value) -> HF,
    HF: std::future::Future<Output = anyhow::Result<()>>,
{
    let (tx, mut rx) = mpsc::channel::<Packet>(4);
    let sink = Sink {
        tx,
        body: Arc::downgrade(&body),
        losses: body.losses.clone(),
    };
    let future = SINK.scope(sink, future);
    tokio::pin!(future);
    let mut observations_open = true;
    loop {
        body.drain(&mut handler).await;
        tokio::select! {
            result=&mut future=>{body.drain(&mut handler).await;return Ok(result);},
            packet=rx.recv(),if observations_open=>{
                let Some(packet)=packet else {observations_open=false;continue;};
                let result=handler(packet.fact).await;
                let _=packet.ack.send(result.is_ok());
                result?; // This lane contains only the pre-dispatch admission.
            },
            _=body.ready.notified()=>{},
        }
    }
}
async fn emit(mut fact: Value) -> anyhow::Result<()> {
    let sink = SINK.try_with(Clone::clone)?;
    fact["physical_attempt_id"] = ATTEMPT.try_with(Clone::clone)?.into();
    fact.as_object_mut().expect("transport fact").extend(
        super::observation_point()
            .as_object()
            .expect("source point")
            .clone(),
    );
    let (ack, receive) = oneshot::channel();
    sink.tx
        .send(Packet { fact, ack })
        .await
        .map_err(|_| anyhow::anyhow!("request observation owner unavailable"))?;
    anyhow::ensure!(
        receive.await.unwrap_or(false),
        "request observation commit unconfirmed"
    );
    Ok(())
}

fn retain_conditions(value: Value) -> Value {
    let _ = ADAPTER_CONDITIONS
        .try_with(|slot| *slot.lock().expect("adapter conditions") = value.clone());
    value
}

fn request_conditions(body: &Value) -> Value {
    // Only closed condition values, never cache keys, headers or private routes.
    fn label(value: &Value, allowed: &[&str]) -> Option<String> {
        value.as_str().map(|s| {
            if allowed.contains(&s) {
                s.to_owned()
            } else {
                "unrecognized".into()
            }
        })
    }
    fn cache_ttls(value: &Value, ttls: &mut std::collections::BTreeSet<String>) {
        match value {
            Value::Object(fields) => {
                if let Some(control) = fields.get("cache_control") {
                    if let Some(ttl) = label(&control["ttl"], &["5m", "1h"]) {
                        ttls.insert(ttl);
                    }
                }
                for child in fields.values() {
                    cache_ttls(child, ttls);
                }
            }
            Value::Array(values) => {
                for child in values {
                    cache_ttls(child, ttls);
                }
            }
            _ => {}
        }
    }
    let mut ttls = std::collections::BTreeSet::new();
    cache_ttls(body, &mut ttls);
    json!({"version":1,"coverage":"parsed_adapter_body_closed_allowlist",
        "requested_service_tier":label(&body["service_tier"], &["auto","default","standard","flex","priority"]),
        "prompt_cache_retention":label(&body["prompt_cache_retention"], &["in-memory","24h","12h"]),
        "cache_control_ttls":ttls,"requested_region_present":body.get("inference_geo").is_some() || body.get("region").is_some()})
}

pub(crate) async fn before_send(
    builder: &reqwest::RequestBuilder,
) -> anyhow::Result<Option<DispatchObservation>> {
    if SINK.try_with(|_| ()).is_err() {
        return Ok(None);
    }
    let observation = DispatchObservation::capture()?;
    let dispatch_id = &observation.dispatch_id;
    let request = builder.try_clone().and_then(|b| b.build().ok());
    let Some(request) = request else {
        let conditions = retain_conditions(json!({"coverage":"uninspectable_adapter_body"}));
        emit(json!({"dispatch_id":dispatch_id,"phase":"dispatch_intent","context_manifest":null,"request_conditions":conditions,"coverage":"uncloneable_body","send_state":"send_unknown"})).await?;
        return Ok(Some(observation));
    };
    if request.method() != reqwest::Method::POST {
        return Ok(None);
    }
    let path = request.url().path();
    let route_kind = if path.ends_with("/api/show") {
        "provider_preparation"
    } else if path.ends_with("/embeddings") {
        "embedding_inference"
    } else if path.ends_with("/files") {
        "attachment_transfer"
    } else if [
        "/responses",
        "/chat/completions",
        "/messages",
        "/converse",
        "/converse-stream",
        "/api/chat",
        ":generateContent",
        ":streamGenerateContent",
    ]
    .iter()
    .any(|suffix| path.ends_with(suffix))
    {
        "model_inference"
    } else {
        "unclassified_http"
    };
    let mut conditions = json!({"coverage":"uninspectable_adapter_body"});
    let manifest = if let Some(bytes) = request.body().and_then(|b| b.as_bytes()) {
        let mut manifest = json!({"version":1,"layer":"adapter_serialized_json","render_version":"reqwest_json_v1",
            "serialized_bytes":bytes.len(),"fingerprint":hex::encode(Sha256::digest(bytes)),
            "estimated_tokens":null,"entries":[],"coverage":"body_size_only","framing_residual_bytes":null});
        // Bound diagnostic parsing independently of a request's allowed body size.
        if bytes.len() <= 1024 * 1024 {
            if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
                conditions = request_conditions(&value);
                let mut entries = Vec::new();
                let mut total_entries = 0usize;
                for (field, category) in [
                    ("system", "base_system"),
                    ("instructions", "base_system"),
                    ("systemInstruction", "base_system"),
                    ("tools", "tool_definitions"),
                    ("messages", "messages"),
                    ("input", "input"),
                    ("contents", "messages"),
                ] {
                    if let Some(component) = value.get(field) {
                        let components = component
                            .as_array()
                            .map(Vec::as_slice)
                            .unwrap_or(std::slice::from_ref(component));
                        total_entries = total_entries.saturating_add(components.len());
                        let remaining = 64usize.saturating_sub(entries.len());
                        for (ordinal, entry) in components.iter().enumerate().take(remaining) {
                            let encoded = serde_json::to_vec(entry)?;
                            entries.push(json!({"field":field,"category":category,"ordinal":ordinal,
                                "serialized_bytes":encoded.len(),"fingerprint":hex::encode(Sha256::digest(&encoded)),
                                "role":entry.get("role").and_then(Value::as_str).filter(|r|matches!(*r,"system"|"developer"|"user"|"assistant"|"tool"|"model")),
                                "estimated_tokens":null,"inclusion":"serialized","source_reconstruction":"unavailable"}));
                        }
                    }
                }
                manifest["total_component_entries"] = total_entries.into();
                manifest["entries"] = entries.into();
                manifest["coverage"] = "partial_components_no_billed_token_claim".into();
                if let Some(tools) = value.get("tools") {
                    manifest["rendered_definitions_hash"] =
                        hex::encode(Sha256::digest(serde_json::to_vec(tools)?)).into();
                }
            }
        }
        while serde_json::to_vec(&manifest)?.len() > 12 * 1024 - 128 {
            if manifest["entries"]
                .as_array_mut()
                .is_none_or(|entries| entries.pop().is_none())
            {
                break;
            }
        }
        let retained = manifest["entries"].as_array().map_or(0, Vec::len);
        let component_bytes = manifest["entries"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|entry| entry["serialized_bytes"].as_u64())
            .sum::<u64>();
        manifest["omitted_component_entries"] = manifest["total_component_entries"]
            .as_u64()
            .map(|n| n.saturating_sub(retained as u64))
            .into();
        manifest["framing_residual_bytes"] =
            ((bytes.len() as u64).saturating_sub(component_bytes)).into();
        manifest["residual_basis"] =
            "serialized_body_minus_disjoint_retained_root_components".into();
        Some(manifest)
    } else {
        None
    };
    let conditions = retain_conditions(conditions);
    emit(
        json!({"dispatch_id":dispatch_id,"route_kind":route_kind,"phase":"dispatch_intent","context_manifest":manifest,
        "request_conditions":conditions,
        "send_state":"send_unknown","render_complete":true,"render_completion_basis":"serialized_adapter_body_pre_dispatch","coverage":"adapter_boundary"}),
    )
    .await?;
    Ok(Some(observation))
}
pub(crate) fn headers_received(
    observation: &DispatchObservation,
    elapsed: std::time::Duration,
    dispatch_point: Value,
) {
    observation.record(json!({"dispatch_start":dispatch_point,"dispatch_start_basis":"reqwest_send_future_poll_boundary_confirmed_by_response_headers",
        "phase":"headers_received","request_sent":true,"elapsed_ns":elapsed.as_nanos().min(u64::MAX as u128) as u64,
        "timing_basis":"monotonic_send_to_headers"}));
}
pub(crate) fn body_finished(observation: &DispatchObservation, elapsed: std::time::Duration) {
    observation.record(json!({"phase":"provider_body_finished","elapsed_ns":elapsed.as_nanos().min(u64::MAX as u128) as u64,
        "timing_basis":"transport_dispatch_to_complete_body_monotonic","request_sent":true}));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    #[tokio::test]
    async fn durable_intent_failure_stops_dispatch_but_post_send_diagnostics_preserve_result() {
        let dispatched = Arc::new(AtomicBool::new(false));
        let sent = dispatched.clone();
        let losses = Arc::new(AtomicU64::new(0));
        let work = async move {
            let builder = reqwest::Client::new()
                .post("https://example.invalid/v1/chat/completions")
                .json(&json!({"messages":[{"role":"user","content":"PRIVATE_PAYLOAD"}]}));
            before_send(&builder).await?;
            sent.store(true, Ordering::SeqCst);
            anyhow::Ok(())
        };
        let result = observe_scope(
            attempt_scope("attempt_before_send".into(), work),
            |_| async { anyhow::bail!("fixture commit failure") },
            BodyObservations::new(losses.clone()),
        )
        .await;
        assert!(result.is_err());
        assert!(!dispatched.load(Ordering::SeqCst));
        assert_eq!(losses.load(Ordering::SeqCst), 0);

        let facts = Arc::new(Mutex::new(Vec::new()));
        let observed = facts.clone();
        let result = observe_scope(attempt_scope("attempt_after_send".into(), async {
            let builder=reqwest::Client::new().post("https://example.invalid/v1/responses")
                .json(&json!({"instructions":"RENDERED_PRIVATE","input":[{"role":"user","content":"VISIBLE_PRIVATE"}],"service_tier":"priority","tools":[{"type":"function","name":"fixture"}]}));
            let dispatch=before_send(&builder).await?.unwrap();
            headers_received(&dispatch,std::time::Duration::from_millis(2),observation_point());
            body_finished(&dispatch,std::time::Duration::from_millis(5));
            anyhow::Ok(7)
        }),move |fact|{
            observed.lock().unwrap().push(fact.clone());
            async move { if fact["phase"]!="dispatch_intent" {anyhow::bail!("diagnostic overload");} Ok(()) }
        },BodyObservations::new(losses.clone())).await.unwrap().unwrap();
        assert_eq!(result, 7);
        assert_eq!(losses.load(Ordering::SeqCst), 2);
        let facts = facts.lock().unwrap();
        assert_eq!(facts.len(), 3);
        assert_eq!(facts[0]["send_state"], "send_unknown");
        assert_eq!(facts[0]["physical_attempt_id"], "attempt_after_send");
        assert_eq!(
            facts[0]["request_conditions"]["requested_service_tier"],
            "priority"
        );
        assert_eq!(
            facts[0]["context_manifest"]["layer"],
            "adapter_serialized_json"
        );
        assert!(facts[0]["context_manifest"]["rendered_definitions_hash"].is_string());
        assert_eq!(facts[1]["request_sent"], true);
        assert!(!serde_json::to_string(&*facts).unwrap().contains("PRIVATE"));
    }

    #[tokio::test]
    async fn spawned_body_poller_survives_auxiliary_open_drain_without_false_eof() {
        use futures_util::StreamExt;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for mode in [
            "drain",
            "diagnostic_failure",
            "early_terminal",
            "cancel",
            "owner_dropped",
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!(
                "http://{}/v1/chat/completions",
                listener.local_addr().unwrap()
            );
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = [0u8; 4096];
                let _ = socket.read(&mut bytes).await.unwrap();
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nchunk",
                    )
                    .await
                    .unwrap();
            });
            let provider = crate::providers::OpenAiProvider::new("fixture");
            let context = UsageContext::capture(&provider, "fixture");
            let identity = context.initial_usage().physical_attempt_id.unwrap();
            let losses = context.diagnostic_losses.clone();
            let facts = Arc::new(Mutex::new(Vec::new()));
            let opened = facts.clone();
            let response = context
                .scope_with_observations(
                    async {
                        crate::http::send_stream_request(
                            reqwest::Client::new()
                                .post(&url)
                                .json(&json!({"messages":[]})),
                            Default::default(),
                        )
                        .await
                    },
                    move |fact| {
                        opened.lock().unwrap().push(fact);
                        async { Ok(()) }
                    },
                )
                .await
                .unwrap()
                .unwrap();
            // The open receiver has completed. The stream captures a weak body
            // binding, never that receiver's sender or an ACK wait.
            let mut bytes = crate::http::bounded_response_stream(response, 1024, "fixture_body");
            let (ready, seen) = oneshot::channel();
            let decoder = tokio::spawn(async move {
                assert!(bytes.next().await.unwrap().is_ok());
                let _ = ready.send(());
                if mode == "cancel" {
                    std::future::pending::<()>().await;
                }
                if matches!(mode, "drain" | "diagnostic_failure" | "owner_dropped") {
                    while let Some(chunk) = bytes.next().await {
                        chunk.unwrap();
                    }
                }
                7
            });
            seen.await.unwrap();
            if mode == "cancel" {
                decoder.abort();
            }
            if mode == "owner_dropped" {
                drop(context);
                assert_eq!(decoder.await.unwrap(), 7);
                assert_eq!(losses.load(Ordering::Relaxed), 1);
            } else {
                let observed = facts.clone();
                let outcome = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    context.observe_transport(async { decoder.await }, move |fact| {
                        let fail = mode == "diagnostic_failure"
                            && fact["phase"] == "provider_body_finished";
                        observed.lock().unwrap().push(fact);
                        async move {
                            if fail {
                                anyhow::bail!("fixture diagnostic failure");
                            }
                            Ok(())
                        }
                    }),
                )
                .await
                .unwrap()
                .unwrap();
                if mode == "cancel" {
                    assert!(outcome.unwrap_err().is_cancelled());
                } else {
                    assert_eq!(outcome.unwrap(), 7);
                }
                let mut usage = TokenUsage::default();
                context.finalize_usage(&mut usage);
                let coverage =
                    &usage.accounting.as_ref().unwrap()["transport_observation_coverage"];
                if matches!(mode, "drain" | "diagnostic_failure") {
                    assert_eq!(coverage["body_eof_observed"], 1);
                    assert_eq!(coverage["body_eof_persisted"], u64::from(mode == "drain"));
                    assert_eq!(
                        coverage["body_eof_lost"],
                        u64::from(mode == "diagnostic_failure")
                    );
                } else {
                    assert_eq!(coverage["body_eof_coverage"], "unobserved");
                }
            }
            server.await.unwrap();
            let facts = facts.lock().unwrap();
            let eof = facts
                .iter()
                .filter(|v| v["phase"] == "provider_body_finished")
                .collect::<Vec<_>>();
            assert_eq!(
                eof.len(),
                usize::from(matches!(mode, "drain" | "diagnostic_failure"))
            );
            if let Some(eof) = eof.first() {
                assert_eq!(eof["physical_attempt_id"], identity);
                assert_eq!(eof["dispatch_id"], facts[0]["dispatch_id"]);
            }
        }
    }
}
