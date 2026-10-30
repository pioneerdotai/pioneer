//! A single reducer for JSON and SSE. Native correlation never crosses Provider.
use super::{SafeError, replay};
use crate::{
    ChatResponse, ProviderMessageBoundary as Boundary, ProviderMessageCollector, ProviderMessageId,
    ProviderMessageMetadata, ProviderReplayState, ProviderResponseLimits, ProviderTermination,
    ProviderToolCall, StreamChunk, TokenUsage,
};
use anyhow::{Result, bail, ensure};
use pioneer_protocol::AgentMessagePhase;
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Default)]
struct Part {
    kind: String,
    text: String,
    sealed: bool,
    emitted: usize,
}
#[derive(Default)]
struct Item {
    raw: Value,
    raw_bytes: usize,
    phase: Option<Option<AgentMessagePhase>>,
    phase_resolved: bool,
    provisional: bool,
    done: bool,
    parts: BTreeMap<usize, Part>,
    summary: BTreeMap<usize, Part>,
    reasoning: BTreeMap<usize, Part>,
    args: String,
    args_sealed: bool,
    local_id: Option<ProviderMessageId>,
    started: bool,
    ended: bool,
}

pub(super) struct Reducer {
    provider: String,
    model: String,
    authority: String,
    items: BTreeMap<usize, Item>,
    collector: ProviderMessageCollector,
    next_message_id: u32,
    reasoning: String,
    pub(super) usage: TokenUsage,
    calls: Vec<ProviderToolCall>,
    replay: Option<ProviderReplayState>,
    terminal: Option<ProviderTermination>,
    staging_bytes: usize,
    parallel: Option<bool>,
    allowed_tools: Option<std::collections::BTreeSet<String>>,
    choice: Option<crate::ToolChoice>,
    provisional_messages: bool,
}
impl Reducer {
    pub(super) fn new(
        provider: &str,
        model: &str,
        authority: &str,
        parallel: Option<bool>,
        request_id: Option<&str>,
    ) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            authority: authority.into(),
            items: BTreeMap::new(),
            collector: Default::default(),
            next_message_id: 0,
            reasoning: String::new(),
            usage: TokenUsage::default().with_request_id(request_id),
            calls: Vec::new(),
            replay: None,
            terminal: None,
            staging_bytes: 0,
            parallel,
            allowed_tools: None,
            choice: None,
            provisional_messages: false,
        }
    }
    pub(super) fn with_provisional_messages(mut self, enabled: bool) -> Self {
        self.provisional_messages = enabled;
        self
    }
    pub(super) fn bind_request(&mut self, request: &crate::ChatRequest) {
        self.allowed_tools = Some(
            request
                .tools
                .iter()
                .flatten()
                .map(|tool| tool.name.clone())
                .collect(),
        );
        self.choice = request.tool_choice.clone();
    }
    fn index(v: &Value, key: &str) -> Result<usize> {
        let n = v[key]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("missing Responses event index"))?;
        ensure!(n < 256, "Responses index limit");
        Ok(n as usize)
    }
    fn correlated(&mut self, v: &Value) -> Result<&mut Item> {
        let index = Self::index(v, "output_index")?;
        let item = self
            .items
            .get_mut(&index)
            .ok_or_else(|| anyhow::anyhow!("Responses delta before item"))?;
        ensure!(
            v["item_id"].as_str().is_some_and(|id| item.raw["id"] == id),
            "Responses item correlation conflict"
        );
        ensure!(!item.done, "Responses delta after item completion");
        Ok(item)
    }
    fn observe(&mut self, response: &Value) -> Result<()> {
        let mut snapshot = match response.get("usage").filter(|u| !u.is_null()) {
            Some(v) => crate::usage::normalize_responses(v)?,
            None => TokenUsage::default(),
        }
        .with_native_id(response["id"].as_str())
        .with_reported_model(response["model"].as_str())
        .with_service_tier(response["service_tier"].as_str());
        snapshot.api = Some("responses".into());
        self.usage.update(&snapshot);
        Ok(())
    }
    fn part(
        parts: &mut BTreeMap<usize, Part>,
        index: usize,
        kind: &str,
        text: &str,
        done: bool,
    ) -> Result<()> {
        let part = parts.entry(index).or_default();
        ensure!(
            part.kind.is_empty() || part.kind == kind,
            "Responses content type conflict"
        );
        ensure!(
            (!part.sealed && text.starts_with(&part.text)) || text == part.text,
            "Responses content snapshot conflict"
        );
        part.kind = kind.into();
        part.text = text.into();
        part.sealed |= done;
        Ok(())
    }
    fn parts(item: &mut Item, value: &Value, done: bool) -> Result<()> {
        if item.raw["type"] == "message" {
            if let Some(parts) = value.get("content").and_then(Value::as_array) {
                ensure!(parts.len() <= 128, "Responses content part limit");
                ensure!(
                    !done || item.parts.keys().all(|i| *i < parts.len()),
                    "Responses snapshot lost content"
                );
                for (index, part) in parts.iter().enumerate() {
                    Self::message_part(item, index, part, done)?;
                }
            } else {
                ensure!(!done, "Responses message has no content");
            }
        } else if item.raw["type"] == "reasoning" {
            for (key, target, kind) in [
                ("summary", &mut item.summary, "summary_text"),
                ("content", &mut item.reasoning, "reasoning_text"),
            ] {
                if let Some(parts) = value.get(key).and_then(Value::as_array) {
                    ensure!(parts.len() <= 128, "Responses reasoning part limit");
                    ensure!(
                        !done || target.keys().all(|i| *i < parts.len()),
                        "Responses snapshot lost reasoning"
                    );
                    for (index, p) in parts.iter().enumerate() {
                        ensure!(p["type"] == kind, "unsupported Responses reasoning part");
                        let text = p["text"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("invalid Responses reasoning text"))?;
                        Self::part(target, index, kind, text, done)?;
                    }
                }
            }
        } else {
            if let Some(args) = value.get("arguments").and_then(Value::as_str) {
                ensure!(
                    (!item.args_sealed && args.starts_with(&item.args)) || args == item.args,
                    "Responses arguments snapshot conflict"
                );
                item.args = args.into();
                item.args_sealed |= done;
            }
        }
        Ok(())
    }
    fn message_part(item: &mut Item, index: usize, value: &Value, done: bool) -> Result<()> {
        let (kind, key) = match value["type"].as_str() {
            Some("output_text") => ("output_text", "text"),
            Some("refusal") => ("refusal", "refusal"),
            _ => bail!("unsupported Responses message part"),
        };
        let text = value[key]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("invalid Responses text part"))?;
        Self::part(&mut item.parts, index, kind, text, done)
    }
    fn merge_item(
        &mut self,
        index: usize,
        value: &Value,
        done: bool,
        terminal: bool,
    ) -> Result<()> {
        ensure!(index < 256, "Responses item limit");
        let kind = value["type"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing Responses item type"))?;
        ensure!(
            matches!(kind, "message" | "reasoning" | "function_call"),
            "unsupported Responses output item"
        );
        if kind == "function_call" {
            replay::validate_function_profile(value)?;
        }
        let id = value.get("id");
        ensure!(
            id.is_none_or(|id| id.as_str().is_some_and(|s| !s.is_empty())),
            "invalid Responses item ID"
        );
        if let Some(id) = id {
            ensure!(
                !self
                    .items
                    .iter()
                    .any(|(i, item)| *i != index && item.raw.get("id") == Some(id)),
                "duplicate Responses item ID"
            );
        }
        let item = self.items.entry(index).or_default();
        if !item.raw.is_null() {
            ensure!(
                item.raw["type"] == kind
                    && id.is_none_or(|id| item.raw.get("id").is_none_or(|old| old == id)),
                "Responses item snapshot conflict"
            );
            for key in ["call_id", "name", "role"] {
                if let (Some(old), Some(new)) = (item.raw.get(key), value.get(key)) {
                    ensure!(old == new, "Responses item identity conflict");
                }
            }
        } else {
            item.raw = json!({"type":kind});
        }
        if kind == "message" {
            ensure!(
                value["role"] == "assistant",
                "unsupported Responses message role"
            );
            replay::validate_phase(value)?;
            if let Some(phase) = value.get("phase") {
                let phase = match phase.as_str() {
                    Some("commentary") => Some(AgentMessagePhase::Commentary),
                    Some("final_answer") => Some(AgentMessagePhase::FinalAnswer),
                    _ => None,
                };
                ensure!(
                    item.phase
                        .as_ref()
                        .is_none_or(|old| old.is_none() || *old == phase),
                    "Responses phase conflict"
                );
                item.phase = Some(phase);
            }
            // Explicit null is known absence, but still buffered until terminal.
            item.phase_resolved |= item.phase.as_ref().is_some_and(Option::is_some) || terminal;
        }
        if item.done {
            for (key, parts) in [("content", &item.parts), ("summary", &item.summary)] {
                if kind == "message" && key == "content" || kind == "reasoning" && key == "summary"
                {
                    ensure!(
                        value
                            .get(key)
                            .and_then(Value::as_array)
                            .is_none_or(|v| v.len() == parts.len()),
                        "Responses completed item part conflict"
                    );
                }
            }
            if kind == "reasoning" {
                ensure!(
                    value
                        .get("content")
                        .and_then(Value::as_array)
                        .is_none_or(|v| v.len() == item.reasoning.len()),
                    "Responses completed reasoning part conflict"
                );
                if !terminal {
                    if let (Some(completed), Some(snapshot)) = (
                        item.raw.get("encrypted_content"),
                        value.get("encrypted_content"),
                    ) {
                        ensure!(
                            completed == snapshot,
                            "Responses completed reasoning state conflict"
                        );
                    }
                }
            }
        }
        Self::parts(item, value, done)?;
        // Preserve original explicit phase if later snapshots omit it.
        let old_phase = item.raw.get("phase").cloned();
        let mut merged = std::mem::take(&mut item.raw);
        // An added snapshot may contain only a ciphertext prefix. Only the
        // first completed snapshot establishes the replay value.
        if kind == "reasoning" && done && !item.done && value.get("encrypted_content").is_none() {
            merged
                .as_object_mut()
                .expect("item object")
                .remove("encrypted_content");
        }
        for (key, value) in value
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("invalid Responses item snapshot"))?
        {
            // Streaming replay uses the completed reasoning item from
            // response.output_item.done. A terminal response can carry a
            // different ciphertext for the same item; keep the completed one.
            // https://developers.openai.com/api/reference/resources/responses/streaming-events#response.output_item.added
            if key == "encrypted_content"
                && kind == "reasoning"
                && terminal
                && item.done
                && merged.get(key).is_some()
            {
                continue;
            }
            merged[key] = value.clone();
        }
        if merged.get("phase").is_none() {
            if let Some(phase) = old_phase {
                merged["phase"] = phase;
            }
        }
        if kind == "reasoning" && merged.get("content").is_none() && !item.reasoning.is_empty() {
            merged["content"] = json!(
                item.reasoning
                    .values()
                    .map(|part| json!({"type":"reasoning_text","text":part.text}))
                    .collect::<Vec<_>>()
            );
        }
        if terminal && value.get("status").is_none() {
            merged
                .as_object_mut()
                .expect("item object")
                .remove("status");
        }
        item.raw_bytes = super::wire::retained_bytes(&merged);
        item.raw = merged;
        item.done |= done;
        Ok(())
    }
    fn bounds(&self) -> Result<()> {
        let limits = ProviderResponseLimits::default();
        let mut text = 0usize;
        let mut reasoning = 0usize;
        let mut args = 0usize;
        let mut calls = 0usize;
        let mut messages = 0usize;
        for item in self.items.values() {
            text = text.saturating_add(item.parts.values().map(|p| p.text.len()).sum::<usize>());
            reasoning = reasoning.saturating_add(
                item.summary
                    .values()
                    .chain(item.reasoning.values())
                    .map(|p| p.text.len())
                    .sum::<usize>(),
            );
            args = args.saturating_add(item.args.len());

            calls += usize::from(item.raw["type"] == "function_call");
            messages += usize::from(item.raw["type"] == "message");
        }
        ensure!(
            messages <= ProviderMessageCollector::MAX_MESSAGES,
            "Responses message count limit"
        );
        limits.validate_text_bytes(text, "Responses text")?;
        limits.validate_reasoning_bytes(reasoning)?;
        if calls > limits.max_tool_calls || args > limits.max_tool_arguments_bytes {
            bail!("Responses function limit");
        }
        limits.validate_retained_bytes(self.retained_bytes().saturating_add(self.staging_bytes))?;
        Ok(())
    }
    fn retained_bytes(&self) -> usize {
        let mut total = 0usize;
        for item in self.items.values() {
            total = total
                .saturating_add(item.raw_bytes)
                .saturating_add(std::mem::size_of::<Item>())
                .saturating_add(item.args.len());
            for part in item
                .parts
                .values()
                .chain(item.summary.values())
                .chain(item.reasoning.values())
            {
                total = total
                    .saturating_add(part.text.len())
                    .saturating_add(std::mem::size_of::<Part>());
            }
        }
        let calls = self
            .calls
            .iter()
            .map(|call| call.id.len() + call.name.len() + call.arguments.len())
            .sum::<usize>();
        let replay = self
            .replay
            .as_ref()
            .map_or(0, |state| super::wire::retained_bytes(&state.payload));
        // Reserve one queued copy of each generated payload. The transport's
        // bounded channel can retain no more than the request's total payload.
        total
            .saturating_add(self.collector.retained_bytes())
            .saturating_add(self.reasoning.len())
            .saturating_add(calls)
            .saturating_add(replay)
            .saturating_add(
                self.items
                    .values()
                    .map(|item| {
                        item.args.len()
                            + item
                                .parts
                                .values()
                                .chain(item.summary.values())
                                .chain(item.reasoning.values())
                                .map(|part| part.text.len())
                                .sum::<usize>()
                    })
                    .sum::<usize>(),
            )
            .saturating_add(calls)
            .saturating_add(replay)
    }
    pub(super) fn parse_budget(&self, buffers: usize) -> Result<usize> {
        let limits = ProviderResponseLimits::default();
        let retained = self.retained_bytes().saturating_add(buffers);
        limits.validate_retained_bytes(retained)?;
        Ok(limits.max_retained_bytes - retained)
    }
    pub(super) fn stage(&mut self, bytes: usize) -> Result<()> {
        self.staging_bytes = bytes;
        self.bounds()
    }
    fn ordered_delta(parts: &mut BTreeMap<usize, Part>) -> String {
        let mut text = String::new();
        for (expected, (index, part)) in parts.iter_mut().enumerate() {
            if *index != expected {
                break;
            }
            text.push_str(&part.text[part.emitted..]);
            part.emitted = part.text.len();
            if !part.sealed {
                break;
            }
        }
        text
    }
    fn publish(&mut self) -> Result<Vec<StreamChunk>> {
        let mut chunks = Vec::new();
        let mut blocked = false;
        let mut reasoning_blocked = false;
        let mut expected_index = 0;
        for (index, item) in &mut self.items {
            if *index != expected_index {
                blocked = true;
                reasoning_blocked = true;
            }
            expected_index = *index + 1;
            if item.raw["type"] == "message" {
                if blocked {
                    continue;
                }
                if !item.phase_resolved && !self.provisional_messages {
                    blocked = true;
                    continue;
                }
                let phase = item.phase.clone().flatten();
                if item.local_id.is_none() {
                    item.local_id = Some(ProviderMessageId(self.next_message_id));
                    self.next_message_id += 1;
                }
                let local_id = item.local_id.expect("resolved message identity");
                let mut emit = |boundary, delta: String| {
                    let mut chunk = StreamChunk::delta(delta);
                    chunk.message = Some(ProviderMessageMetadata {
                        id: local_id,
                        phase: phase.clone(),
                        boundary,
                    });
                    chunks.push(chunk);
                };
                if !item.started {
                    item.provisional = !item.phase_resolved;
                    emit(
                        if item.provisional {
                            Boundary::StartProvisional
                        } else {
                            Boundary::Start
                        },
                        String::new(),
                    );
                    item.started = true;
                } else if item.provisional && item.phase_resolved {
                    emit(Boundary::Resolve, String::new());
                    item.provisional = false;
                }
                let delta = Self::ordered_delta(&mut item.parts);
                if !delta.is_empty() {
                    emit(Boundary::Delta, delta);
                }
                if item.done && item.phase_resolved && !item.ended {
                    emit(Boundary::End, String::new());
                    item.ended = true;
                }
                // Wire order and semantic resolution are different gates. A
                // sealed provisional item must not stall later real text while
                // its phase still waits for the terminal snapshot. The collector
                // keeps the accepted result cursor behind unresolved messages.
                blocked |= if self.provisional_messages {
                    !item.done
                } else {
                    !item.ended
                };
            } else if item.raw["type"] == "reasoning" {
                if reasoning_blocked {
                    continue;
                }
                reasoning_blocked |= !item.done;
                let mut delta = Self::ordered_delta(&mut item.summary);
                if item.done {
                    delta.push_str(&Self::ordered_delta(&mut item.reasoning));
                }
                if !delta.is_empty() {
                    self.reasoning.push_str(&delta);
                    chunks.push(StreamChunk::reasoning(delta));
                }
            }
        }
        for chunk in &chunks {
            self.collector.push(chunk)?;
        }
        self.bounds()?;
        Ok(chunks)
    }
    pub(super) fn snapshot(&mut self, response: &Value) -> Result<Vec<StreamChunk>> {
        ensure!(self.terminal.is_none(), "Responses duplicate terminal");
        self.observe(response)?;
        let status = response["status"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing Responses status"))?;
        if status == "failed" {
            return Err(SafeError::native(&response["error"], None).into());
        }
        let mut outcome = match status {
            "completed" => ProviderTermination::Complete,
            "incomplete" => match response["incomplete_details"]["reason"].as_str() {
                Some("max_output_tokens") => ProviderTermination::Length,
                Some("content_filter") => ProviderTermination::ContentFiltered,
                _ => ProviderTermination::Unknown("incomplete".into()),
            },
            "cancelled" => ProviderTermination::Cancelled,
            _ => bail!("unsupported Responses terminal status"),
        };
        let output = response["output"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("missing Responses output"))?;
        ensure!(
            output.len() <= 256 && self.items.keys().all(|i| *i < output.len()),
            "Responses terminal item conflict"
        );
        // Classify known incomplete before executable arguments validation.
        for (index, item) in output.iter().enumerate() {
            self.merge_item(index, item, true, true)?;
        }
        let refusal = self
            .items
            .values()
            .any(|i| i.parts.values().any(|p| p.kind == "refusal"));
        if refusal {
            outcome = ProviderTermination::Safety;
        }
        if outcome.permits_success() {
            let normalized = output
                .iter()
                .enumerate()
                .map(|(i, _)| self.items[&i].raw.clone())
                .collect::<Vec<_>>();
            replay::input_items(&normalized)?;
            let mut ids = std::collections::BTreeSet::new();
            for item in &normalized {
                ensure!(
                    item.get("status")
                        .is_none_or(|s| s == "completed" || s.is_null()),
                    "Responses completed item is unfinished"
                );
                if item["type"] == "function_call" {
                    let id = replay::required_string(item, "call_id")?;
                    ensure!(ids.insert(id), "duplicate Responses call_id");
                    self.calls.push(ProviderToolCall {
                        id: id.into(),
                        name: replay::required_string(item, "name")?.into(),
                        arguments: replay::required_string(item, "arguments")?.into(),
                    });
                }
            }
            ensure!(
                self.parallel != Some(false) || self.calls.len() <= 1,
                "Responses parallel policy violated"
            );
            if let Some(allowed) = &self.allowed_tools {
                ensure!(
                    self.calls.iter().all(|call| allowed.contains(&call.name)),
                    "Responses returned an unadvertised function"
                );
                ensure!(
                    !matches!(self.choice, Some(crate::ToolChoice::None)) || self.calls.is_empty(),
                    "Responses violated disabled tools"
                );
                if let Some(crate::ToolChoice::Tool { name }) = &self.choice {
                    ensure!(
                        !self.calls.is_empty() && self.calls.iter().all(|call| call.name == *name),
                        "Responses violated named tool choice"
                    );
                }
                ensure!(
                    !matches!(self.choice, Some(crate::ToolChoice::Required))
                        || !self.calls.is_empty(),
                    "Responses omitted required function call"
                );
            }
            if !self.calls.is_empty() {
                outcome = ProviderTermination::ToolCalls;
            }
            let state = ProviderReplayState {
                provider: self.provider.clone(),
                model: Some(self.model.clone()),
                payload: json!({"schema_version":1,"wire_api":replay::DIALECT,"authority_scope":self.authority,"response_id":response["id"],"output":normalized}),
            };
            ProviderResponseLimits::default().validate_replay_state(&state)?;
            self.replay = Some(state);
        }
        self.bounds()?;
        let mut chunks = self.publish()?;
        self.collector.finish()?;
        if !self.calls.is_empty() {
            chunks.push(StreamChunk::tool_calls(self.calls.clone()));
        }
        if let Some(state) = &self.replay {
            chunks.push(StreamChunk::provider_replay_state(state.clone()));
        }
        chunks.push(StreamChunk::usage(self.usage.clone()));
        chunks.push(StreamChunk::final_chunk_with(outcome.clone()));
        self.terminal = Some(outcome);
        Ok(chunks)
    }
    pub(super) fn event(&mut self, outer: Option<&str>, v: &Value) -> Result<Vec<StreamChunk>> {
        let kind = v["type"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("missing Responses event type"))?;
        ensure!(
            outer.is_none_or(|s| s == kind),
            "Responses outer event/type conflict"
        );
        ensure!(self.terminal.is_none(), "Responses event after terminal");
        match kind {
            "response.created" | "response.in_progress" | "response.queued" => {
                self.observe(&v["response"])?;
                return Ok(Vec::new());
            }
            "response.completed" | "response.incomplete" | "response.failed" => {
                let expected = kind.strip_prefix("response.").expect("terminal type");
                ensure!(
                    v["response"]["status"] == expected,
                    "Responses terminal event/status conflict"
                );
                return self.snapshot(&v["response"]);
            }
            "error" => return Err(SafeError::native(v, None).into()),
            "response.output_item.added" | "response.output_item.done" => {
                self.merge_item(
                    Self::index(v, "output_index")?,
                    &v["item"],
                    kind.ends_with(".done"),
                    false,
                )?;
            }
            "response.content_part.added" | "response.content_part.done" => {
                let index = Self::index(v, "content_index")?;
                let item = self.correlated(v)?;
                ensure!(
                    item.raw["type"] == "message",
                    "Responses content item type conflict"
                );
                Self::message_part(item, index, &v["part"], kind.ends_with(".done"))?;
            }
            "response.output_text.delta"
            | "response.output_text.done"
            | "response.refusal.delta"
            | "response.refusal.done" => {
                let index = Self::index(v, "content_index")?;
                let item = self.correlated(v)?;
                ensure!(
                    item.raw["type"] == "message",
                    "Responses text item type conflict"
                );
                let refusal = kind.starts_with("response.refusal");
                let part_kind = if refusal { "refusal" } else { "output_text" };
                if kind.ends_with(".delta") {
                    let part = item.parts.entry(index).or_default();
                    ensure!(
                        !part.sealed && (part.kind.is_empty() || part.kind == part_kind),
                        "Responses text delta lifecycle conflict"
                    );
                    part.kind = part_kind.into();
                    part.text.push_str(
                        v["delta"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("invalid Responses delta"))?,
                    );
                } else {
                    Self::part(
                        &mut item.parts,
                        index,
                        part_kind,
                        v[if refusal { "refusal" } else { "text" }]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("invalid Responses text snapshot"))?,
                        true,
                    )?;
                }
            }
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
                let item = self.correlated(v)?;
                ensure!(
                    item.raw["type"] == "function_call" && !item.args_sealed,
                    "Responses arguments lifecycle conflict"
                );
                if kind.ends_with(".delta") {
                    item.args.push_str(
                        v["delta"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("invalid Responses argument delta"))?,
                    );
                } else {
                    let args = v["arguments"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("invalid Responses arguments snapshot"))?;
                    ensure!(args.starts_with(&item.args), "Responses arguments conflict");
                    item.args = args.into();
                    item.args_sealed = true;
                }
            }
            "response.reasoning_summary_part.added" | "response.reasoning_summary_part.done" => {
                let index = Self::index(v, "summary_index")?;
                let item = self.correlated(v)?;
                ensure!(
                    item.raw["type"] == "reasoning" && v["part"]["type"] == "summary_text",
                    "unsupported Responses reasoning part"
                );
                Self::part(
                    &mut item.summary,
                    index,
                    "summary_text",
                    v["part"]["text"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("invalid Responses summary"))?,
                    kind.ends_with(".done"),
                )?;
            }
            "response.reasoning_summary_text.delta"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.delta"
            | "response.reasoning_text.done" => {
                let summary = kind.starts_with("response.reasoning_summary");
                let index = Self::index(
                    v,
                    if summary {
                        "summary_index"
                    } else {
                        "content_index"
                    },
                )?;
                let item = self.correlated(v)?;
                ensure!(
                    item.raw["type"] == "reasoning",
                    "Responses reasoning item type conflict"
                );
                let parts = if summary {
                    &mut item.summary
                } else {
                    &mut item.reasoning
                };
                let part_kind = if summary {
                    "summary_text"
                } else {
                    "reasoning_text"
                };
                if kind.ends_with(".delta") {
                    let part = parts.entry(index).or_default();
                    ensure!(!part.sealed, "Responses reasoning after done");
                    part.kind = part_kind.into();
                    part.text.push_str(
                        v["delta"]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("invalid Responses reasoning delta"))?,
                    );
                } else {
                    Self::part(
                        parts,
                        index,
                        part_kind,
                        v["text"].as_str().ok_or_else(|| {
                            anyhow::anyhow!("invalid Responses reasoning snapshot")
                        })?,
                        true,
                    )?;
                }
            }
            "response.output_text.annotation.added" => {
                return Ok(Vec::new());
            }
            _ => bail!("unsupported Responses event"),
        }
        self.bounds()?;
        self.publish()
    }
    pub(super) fn is_terminal(&self) -> bool {
        self.terminal.is_some()
    }
    pub(super) fn response(self) -> Result<ChatResponse> {
        let response = ChatResponse {
            text: self.collector.text(),
            messages: self.collector.into_messages(),
            reasoning_content: (!self.reasoning.is_empty()).then_some(self.reasoning),
            tool_calls: self.calls,
            usage: Some(self.usage),
            provider_replay_state: self.replay,
            termination: self
                .terminal
                .ok_or_else(|| anyhow::anyhow!("Responses missing terminal"))?,
        };
        ProviderMessageCollector::validate_response(&response)?;
        Ok(response)
    }
}
