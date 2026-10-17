//! A bounded, transient ACP projection into Pioneer's existing event contracts.
//! Durable state, replay deduplication and finalization remain Gateway-owned.

use super::{schema, value};
use pioneer_protocol::{
    AgentDurableEvent, AgentMessagePhase, AgentProgressEvent, ItemCompletedNotification,
    ItemDeltaNotification, ItemDeltaStream, ItemStartedNotification, ItemUpdatedNotification,
    StorageOutputPolicy, SystemEventLevel, TimelineOutputPolicy, ToolCallStatus,
    ToolDisplayPayload, ToolMetadata, ToolOutputPolicySnapshot, ToolOutputSummary,
    ToolStoragePayload, TurnItem, TurnItemExecutionClass,
};
use pioneer_runtime_events::ExecutionSnapshotEvent;
use schema::{ContentBlock, SessionUpdate};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// A batch for the existing durable/snapshot/progress hub. Live notices and
/// session descriptors are deliberately separate from conversation history.
pub struct MediaOutput {
    pub item_id: String,
    pub content: ContentBlock,
}

#[derive(Default)]
pub struct Projection {
    pub media: Vec<MediaOutput>,
    pub durable: Vec<AgentDurableEvent>,
    pub snapshots: Vec<ExecutionSnapshotEvent>,
    pub progress: Vec<AgentProgressEvent>,
    pub notice: Option<schema::Notice>,
    pub session_state: Option<SessionUpdate>,
}

impl Projection {
    /// Feed the existing runtime ingress/journal. The Gateway projector binds
    /// workspace/thread/turn from its admitted execution, rather than trusting
    /// scope fields arriving from a protocol peer. Canonical TurnItems retain
    /// their client-selected output policy through this typed boundary.
    pub fn runtime_events(
        &self,
        native_thread_id: &str,
        native_turn_id: &str,
    ) -> schema::Result<Vec<crate::event::RuntimeEvent>> {
        use crate::event::{
            RuntimeAgentMessagePhase, RuntimeEvent, RuntimeItemCompleted, RuntimeItemDelta,
            RuntimeItemDeltaKind, RuntimeItemStarted, RuntimeItemUpdated,
        };
        let mut events =
            Vec::with_capacity(self.durable.len() + self.snapshots.len() + self.progress.len());
        for event in &self.durable {
            events.push(match event {
                AgentDurableEvent::ItemStarted { notification } => {
                    RuntimeEvent::ItemStarted(RuntimeItemStarted {
                        item: Some(notification.item.clone()),
                        native_thread_id: Some(native_thread_id.to_owned()),
                        native_turn_id: native_turn_id.to_owned(),
                        native_item_id: notification.item.item_id().to_owned(),
                        item_kind: "acp".to_owned(),
                        title: None,
                        phase: RuntimeAgentMessagePhase::Commentary,
                        metadata: None,
                        native_item_redacted: None,
                        native: None,
                    })
                }
                AgentDurableEvent::ItemCompleted { notification } => {
                    RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                        item: Some(notification.item.clone()),
                        native_thread_id: Some(native_thread_id.to_owned()),
                        native_turn_id: native_turn_id.to_owned(),
                        native_item_id: notification.item.item_id().to_owned(),
                        item_kind: "acp".to_owned(),
                        text: None,
                        summary: Vec::new(),
                        content: Vec::new(),
                        phase: RuntimeAgentMessagePhase::Commentary,
                        metadata: None,
                        native_item_redacted: None,
                        native: None,
                    })
                }
                _ => return Err(schema::Error::internal_error()),
            });
        }
        for event in &self.progress {
            let AgentProgressEvent::ItemDelta { notification } = event else {
                return Err(schema::Error::internal_error());
            };
            events.push(RuntimeEvent::ItemDelta(RuntimeItemDelta {
                native_thread_id: Some(native_thread_id.to_owned()),
                native_turn_id: native_turn_id.to_owned(),
                native_item_id: notification.item_id.clone(),
                item_kind: "acp".to_owned(),
                delta_kind: if notification.stream == Some(ItemDeltaStream::AgentMessage) {
                    RuntimeItemDeltaKind::AgentMessage
                } else {
                    RuntimeItemDeltaKind::Generic
                },
                delta: notification.delta.clone(),
                metadata: notification.payload.clone(),
                native: None,
            }));
        }
        for event in &self.snapshots {
            let ExecutionSnapshotEvent::ItemUpdated { notification } = event;
            events.push(RuntimeEvent::ItemUpdated(RuntimeItemUpdated {
                item: Some(notification.item.clone()),
                native_thread_id: Some(native_thread_id.to_owned()),
                native_turn_id: native_turn_id.to_owned(),
                native_item_id: notification.item.item_id().to_owned(),
                item_kind: "acp".to_owned(),
                native: None,
            }));
        }
        Ok(events)
    }
}

#[derive(Clone, Debug)]
pub struct Context {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub session_id: schema::SessionId,
    /// Server-owned namespace covering configured instance, process generation
    /// and session. A peer's sessionId alone is not globally unique.
    pub session_scope: String,
}

struct Message {
    id: String,
    role: &'static str,
    text: String,
}

pub struct Reducer {
    context: Context,
    sequence: u64,
    /// Only protocol projection state; not a history store. Clear at the
    /// admitted prompt boundary, never implicitly on load replay.
    messages: HashMap<String, Message>,
    active_message: Option<(String, &'static str, Option<String>)>,
    tools: HashMap<String, Value>,
    terminal_outputs: HashMap<String, String>,
    compactions: HashMap<String, Value>,
    compaction_origins: HashMap<String, Context>,
    compaction_item_ids: HashMap<String, String>,
    plan_seen: bool,
    last_answer: Option<String>,
    deferred_answer: Option<String>,
    completed_tools: std::collections::HashSet<String>,
    max_entities: usize,
    max_bytes: usize,
    retained_bytes: usize,
}

impl Reducer {
    pub fn new(context: Context, max_entities: usize, max_bytes: usize) -> schema::Result<Self> {
        if max_entities == 0
            || max_bytes == 0
            || context.session_scope.is_empty()
            || context.session_scope.len() > 4096
        {
            return Err(schema::Error::invalid_params());
        }
        Ok(Self {
            context,
            sequence: 0,
            messages: HashMap::new(),
            active_message: None,
            tools: HashMap::new(),
            terminal_outputs: HashMap::new(),
            compactions: HashMap::new(),
            compaction_origins: HashMap::new(),
            compaction_item_ids: HashMap::new(),
            plan_seen: false,
            last_answer: None,
            deferred_answer: None,
            completed_tools: Default::default(),
            max_entities,
            max_bytes,
            retained_bytes: 0,
        })
    }

    /// Restore only existing canonical presentation, never user messages or
    /// execution state. This emits nothing and preserves the original row.
    pub fn hydrate_compaction(
        &mut self,
        notification: ItemUpdatedNotification,
    ) -> schema::Result<()> {
        if notification.workspace_id != self.context.workspace_id {
            return Err(schema::Error::invalid_params());
        }
        let TurnItem::SystemEvent {
            id: item_id,
            code: Some(code),
            details: Some(mut data),
            ..
        } = notification.item
        else {
            return Err(schema::Error::invalid_params());
        };
        if code != "agent_context_compaction" || !item_id.starts_with("acp_") {
            return Err(schema::Error::invalid_params());
        }
        let id = data
            .get("compactionId")
            .and_then(Value::as_str)
            .ok_or_else(schema::Error::invalid_params)?
            .to_owned();
        if let Some(existing) = self.compaction_item_ids.get(&id) {
            let origin = &self.compaction_origins[&id];
            return if existing == &item_id
                && origin.thread_id == notification.thread_id
                && origin.turn_id == notification.turn_id
            {
                Ok(())
            } else {
                Err(schema::Error::new(
                    -32603,
                    "Conflicting canonical ACP compaction identity",
                ))
            };
        }
        let status = data
            .get("acpStatus")
            .cloned()
            .ok_or_else(schema::Error::invalid_params)?;
        data["status"] = status;
        data.as_object_mut()
            .ok_or_else(schema::Error::invalid_params)?
            .remove("acpStatus");
        let mut origin = self.context.clone();
        origin.thread_id = notification.thread_id;
        origin.turn_id = notification.turn_id;
        self.reserve(
            data.to_string().len() + context_bytes(&origin) + item_id.len(),
            true,
        )?;
        self.compaction_item_ids.insert(id.clone(), item_id);
        self.compaction_origins.insert(id.clone(), origin);
        self.compactions.insert(id, data);
        Ok(())
    }

    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub fn entity_count(&self) -> usize {
        self.messages.len()
            + self.tools.len()
            + self.compactions.len()
            + usize::from(self.plan_seen)
    }
    pub fn limit_retention(&mut self, entities: usize, bytes: usize) -> schema::Result<()> {
        if self.entity_count() > entities || self.retained_bytes > bytes {
            return Err(schema::Error::new(
                -32603,
                "ACP session projection budget exceeded",
            ));
        }
        self.max_entities = entities;
        self.max_bytes = bytes;
        Ok(())
    }

    pub fn retire_prompt(&mut self) -> schema::Result<()> {
        self.begin_prompt(self.context.clone())
    }

    /// Start another admitted prompt in this exact process/session, retaining
    /// session-scoped compaction identities and their original canonical row.
    pub fn begin_prompt(&mut self, context: Context) -> schema::Result<()> {
        if context.session_scope != self.context.session_scope
            || context.session_id != self.context.session_id
        {
            return Err(schema::Error::invalid_params());
        }
        self.context = context;
        self.sequence = 0;
        self.messages.clear();
        self.active_message = None;
        self.tools.clear();
        self.terminal_outputs.clear();
        self.completed_tools.clear();
        self.plan_seen = false;
        self.last_answer = None;
        self.deferred_answer = None;
        self.retained_bytes = self
            .compactions
            .values()
            .map(|v| v.to_string().len())
            .sum::<usize>()
            + self
                .compaction_origins
                .values()
                .map(context_bytes)
                .sum::<usize>()
            + self
                .compaction_item_ids
                .values()
                .map(String::len)
                .sum::<usize>();
        Ok(())
    }

    fn id(&mut self, kind: &str) -> schema::Result<String> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(schema::Error::internal_error)?;
        Ok(format!(
            "acp:{}:{kind}:{}",
            self.context.turn_id, self.sequence
        ))
    }

    fn reserve(&mut self, bytes: usize, new_entity: bool) -> schema::Result<()> {
        if bytes > self.max_bytes.saturating_sub(self.retained_bytes)
            || (new_entity
                && self.messages.len()
                    + self.tools.len()
                    + self.compactions.len()
                    + usize::from(self.plan_seen)
                    >= self.max_entities)
        {
            return Err(schema::Error::new(-32603, "ACP projection budget exceeded"));
        }
        self.retained_bytes += bytes;
        Ok(())
    }

    fn started(&self, item: TurnItem) -> AgentDurableEvent {
        AgentDurableEvent::ItemStarted {
            notification: ItemStartedNotification {
                workspace_id: self.context.workspace_id.clone(),
                thread_id: self.context.thread_id.clone(),
                turn_id: self.context.turn_id.clone(),
                item,
            },
        }
    }

    fn completed(&self, item: TurnItem) -> AgentDurableEvent {
        AgentDurableEvent::ItemCompleted {
            notification: self.completed_notification(item),
        }
    }

    pub fn completed_notification(&self, item: TurnItem) -> ItemCompletedNotification {
        ItemCompletedNotification {
            workspace_id: self.context.workspace_id.clone(),
            thread_id: self.context.thread_id.clone(),
            turn_id: self.context.turn_id.clone(),
            item,
        }
    }

    fn snapshot(&self, item: TurnItem) -> ExecutionSnapshotEvent {
        ExecutionSnapshotEvent::ItemUpdated {
            notification: ItemUpdatedNotification {
                workspace_id: self.context.workspace_id.clone(),
                thread_id: self.context.thread_id.clone(),
                turn_id: self.context.turn_id.clone(),
                item,
            },
        }
    }

    fn message_item(message: &Message, final_answer: bool) -> TurnItem {
        if message.role == "thought" {
            TurnItem::Reasoning {
                id: message.id.clone(),
                summary: Vec::new(),
                content: vec![message.text.clone()],
            }
        } else {
            TurnItem::AgentMessage {
                id: message.id.clone(),
                text: message.text.clone(),
                phase: if final_answer {
                    AgentMessagePhase::FinalAnswer
                } else {
                    AgentMessagePhase::Commentary
                },
                markdown: None,
                markdown_version: None,
            }
        }
    }

    fn flush_deferred(&mut self, projection: &mut Projection) {
        if let Some(id) = self.deferred_answer.take()
            && let Some(message) = self.messages.get(&id)
        {
            projection
                .durable
                .push(self.completed(Self::message_item(message, false)));
        }
    }
    fn end_message(&mut self, projection: &mut Projection) {
        if let Some((id, role, _)) = self.active_message.take() {
            if role == "agent" {
                self.flush_deferred(projection);
                self.deferred_answer = Some(id);
            } else if let Some(message) = self.messages.get(&id) {
                projection
                    .durable
                    .push(self.completed(Self::message_item(message, false)));
            }
        }
    }

    fn chunk(
        &mut self,
        role: &'static str,
        chunk: schema::ContentChunk,
        projection: &mut Projection,
    ) -> schema::Result<()> {
        // User echo/replay is already represented by accepted Pioneer input.
        if role == "user" {
            self.end_message(projection);
            self.flush_deferred(projection);
            self.last_answer = None;
            return Ok(());
        }
        let wire_id = chunk.message_id.map(|id| id.to_string());
        let same_segment =
            self.active_message
                .as_ref()
                .is_some_and(|(_, active_role, active_id)| {
                    *active_role == role && *active_id == wire_id
                });
        if !same_segment {
            self.end_message(projection);
            if role == "agent" {
                self.flush_deferred(projection);
            }
            let id = if let Some(id) = &wire_id {
                scoped_id(&self.context.session_scope, &format!("message:{role}"), id)
            } else {
                self.id(role)?
            };
            let exists = self.messages.contains_key(&id);
            self.reserve(id.len(), !exists)?;
            if !exists {
                let message = Message {
                    id: id.clone(),
                    role,
                    text: String::new(),
                };
                projection
                    .durable
                    .push(self.started(Self::message_item(&message, false)));
                self.messages.insert(id.clone(), message);
            }
            self.active_message = Some((id.clone(), role, wire_id));
            if role == "agent" {
                self.last_answer = Some(id);
            }
        }
        let (text, payload) = match chunk.content {
            ContentBlock::Text(content) => (content.text, None::<Value>),
            ContentBlock::ResourceLink(link) => (
                format!("\n[Resource: {} — {}]\n", link.name, link.uri),
                None,
            ),
            content => {
                let data = value(&content)?;
                let label = data
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("content");
                if matches!(
                    content,
                    ContentBlock::Image(_) | ContentBlock::Audio(_) | ContentBlock::Resource(_)
                ) {
                    projection.media.push(MediaOutput {
                        item_id: self
                            .active_message
                            .as_ref()
                            .expect("active segment")
                            .0
                            .clone(),
                        content,
                    });
                }
                // Raw binary content is transient; only an authorized exact
                // artifact reference may cross the existing event boundary.
                (format!("\n[Agent {label}]\n"), None)
            }
        };
        self.reserve(
            text.len() + payload.as_ref().map_or(0, |p| p.to_string().len()),
            false,
        )?;
        let id = &self.active_message.as_ref().expect("active segment").0;
        self.messages
            .get_mut(id)
            .expect("active message")
            .text
            .push_str(&text);
        projection.progress.push(AgentProgressEvent::ItemDelta {
            notification: ItemDeltaNotification {
                workspace_id: self.context.workspace_id.clone(),
                thread_id: self.context.thread_id.clone(),
                turn_id: self.context.turn_id.clone(),
                item_id: id.clone(),
                delta: text,
                stream: Some(if role == "agent" {
                    ItemDeltaStream::AgentMessage
                } else {
                    ItemDeltaStream::Generic
                }),
                payload,
                markdown: None,
                markdown_version: None,
            },
        });
        Ok(())
    }

    /// An owned client terminal may keep producing output without the agent
    /// sending tool patches. Retain only its bounded display summary here.
    pub fn terminal_output(&mut self, id: &str, output: &str) -> schema::Result<Projection> {
        let summary = output
            .chars()
            .rev()
            .take(2000)
            .collect::<String>()
            .chars()
            .rev()
            .collect::<String>();
        if self.terminal_outputs.get(id) == Some(&summary) {
            return Ok(Projection::default());
        }
        if !self.terminal_outputs.contains_key(id) && self.terminal_outputs.len() >= 32 {
            return Err(schema::Error::invalid_params());
        }
        self.terminal_outputs.insert(id.into(), summary);
        let mut projection = Projection::default();
        for (tool_id, data) in &self.tools {
            if data
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|content| {
                    content
                        .iter()
                        .any(|block| block["type"] == "terminal" && block["terminalId"] == id)
                })
            {
                projection.snapshots.push(self.snapshot(tool_item(
                    &self.context.session_scope,
                    tool_id,
                    data,
                    &self.terminal_outputs,
                )));
            }
        }
        Ok(projection)
    }

    pub fn update(
        &mut self,
        notification: schema::SessionNotification,
    ) -> schema::Result<Projection> {
        if notification.session_id != self.context.session_id {
            return Err(schema::Error::invalid_params());
        }
        let mut projection = Projection::default();
        match notification.update {
            SessionUpdate::UserMessageChunk(chunk) => self.chunk("user", chunk, &mut projection)?,
            SessionUpdate::AgentMessageChunk(chunk) => {
                self.chunk("agent", chunk, &mut projection)?
            }
            SessionUpdate::AgentThoughtChunk(chunk) => {
                self.chunk("thought", chunk, &mut projection)?
            }
            SessionUpdate::ToolCall(call) => {
                let id = call.tool_call_id.to_string();
                if !self.tools.contains_key(&id) {
                    self.end_message(&mut projection);
                    self.flush_deferred(&mut projection);
                    self.last_answer = None;
                }
                let existed = self.tools.contains_key(&id);
                let data = value(call)?;
                let old_bytes = self.tools.get(&id).map_or(0, |old| old.to_string().len());
                let bytes = data.to_string().len();
                self.reserve(bytes.saturating_sub(old_bytes), !existed)?;
                self.retained_bytes = self
                    .retained_bytes
                    .saturating_sub(old_bytes.saturating_sub(bytes));
                collect_media(&self.context.session_scope, &id, &data, &mut projection)?;
                let item = tool_item(
                    &self.context.session_scope,
                    &id,
                    &data,
                    &self.terminal_outputs,
                );
                if !existed {
                    projection.durable.push(self.started(item.clone()));
                }
                if matches!(
                    data.get("status").and_then(Value::as_str),
                    Some("completed" | "failed")
                ) {
                    if self.completed_tools.insert(id.clone()) {
                        projection.durable.push(self.completed(item));
                    } else {
                        projection.snapshots.push(self.snapshot(item));
                    }
                } else if existed {
                    projection.snapshots.push(self.snapshot(item));
                }
                self.tools.insert(id, data);
            }
            SessionUpdate::ToolCallUpdate(update) => {
                let id = update.tool_call_id.to_string();
                if !self.tools.contains_key(&id) {
                    self.end_message(&mut projection);
                    self.flush_deferred(&mut projection);
                    self.last_answer = None;
                }
                let data = value(update)?;
                let previous = self.tools.get(&id);
                let existed = previous.is_some();
                let mut patched = previous.cloned().unwrap_or_else(
                    || json!({"toolCallId":id,"title":"Agent tool","status":"pending"}),
                );
                merge_present(&mut patched, &data);
                let old_bytes = previous.map_or(0, |value| value.to_string().len());
                let new_bytes = patched.to_string().len();
                self.reserve(new_bytes.saturating_sub(old_bytes), !existed)?;
                self.retained_bytes = self
                    .retained_bytes
                    .saturating_sub(old_bytes.saturating_sub(new_bytes));
                collect_media(&self.context.session_scope, &id, &data, &mut projection)?;
                let item = tool_item(
                    &self.context.session_scope,
                    &id,
                    &patched,
                    &self.terminal_outputs,
                );
                if !existed {
                    projection.durable.push(self.started(item.clone()));
                }
                if matches!(
                    patched.get("status").and_then(Value::as_str),
                    Some("completed" | "failed")
                ) {
                    if self.completed_tools.insert(id.clone()) {
                        projection.durable.push(self.completed(item));
                    } else {
                        projection.snapshots.push(self.snapshot(item));
                    }
                } else {
                    projection.snapshots.push(self.snapshot(item));
                }
                self.tools.insert(id, patched);
            }
            SessionUpdate::Plan(plan) => {
                let data = value(plan)?;
                let item = system_item(
                    scoped_id(&self.context.turn_id, "plan", "current"),
                    "Agent plan",
                    "agent_plan_updated",
                    data,
                );
                if !self.plan_seen {
                    self.reserve(0, true)?;
                    self.plan_seen = true;
                    projection.durable.push(self.started(item.clone()));
                }
                projection.snapshots.push(self.snapshot(item));
            }
            SessionUpdate::CompactionUpdate(update) => {
                // Serialize preserves MaybeUndefined: omitted retains, null
                // clears, concrete value replaces; replay does not reset.
                let id = update.compaction_id.to_string();
                if !self.compactions.contains_key(&id) {
                    self.end_message(&mut projection);
                    self.flush_deferred(&mut projection);
                    self.last_answer = None;
                }
                self.compaction(&id, value(update)?, None, &mut projection)?;
            }
            SessionUpdate::CompactionSummaryChunk(chunk) => {
                let id = chunk.compaction_id.to_string();
                if !self.compactions.contains_key(&id) {
                    self.end_message(&mut projection);
                    self.flush_deferred(&mut projection);
                    self.last_answer = None;
                }
                self.compaction(&id, json!({}), Some(value(chunk.content)?), &mut projection)?;
            }
            SessionUpdate::Notice(notice) => projection.notice = Some(notice),
            update @ (SessionUpdate::AvailableCommandsUpdate(_)
            | SessionUpdate::CurrentModeUpdate(_)
            | SessionUpdate::ConfigOptionUpdate(_)
            | SessionUpdate::SessionInfoUpdate(_)
            | SessionUpdate::UsageUpdate(_)) => projection.session_state = Some(update),
            _ => return Err(schema::Error::invalid_request()),
        }
        Ok(projection)
    }

    fn compaction(
        &mut self,
        id: &str,
        patch: Value,
        chunk: Option<Value>,
        projection: &mut Projection,
    ) -> schema::Result<()> {
        let item_id = self
            .compaction_item_ids
            .get(id)
            .cloned()
            .unwrap_or_else(|| scoped_id(&self.context.session_scope, "compaction", id));
        let mut patch = patch;
        if let Some(summary) = patch.get_mut("summary").and_then(Value::as_array_mut) {
            for content in summary {
                sanitize_compaction_media(content, &item_id, projection)?;
            }
        }
        let chunk = chunk
            .map(|mut content| {
                sanitize_compaction_media(&mut content, &item_id, projection)?;
                Ok::<_, schema::Error>(content)
            })
            .transpose()?;
        let existed = self.compactions.contains_key(id);
        let mut data = self
            .compactions
            .get(id)
            .cloned()
            .unwrap_or_else(|| json!({"compactionId":id,"status":"in_progress"}));
        let old_bytes = if existed { data.to_string().len() } else { 0 };
        merge_present(&mut data, &patch);
        if let Some(chunk) = chunk {
            if !data.get("summary").is_some_and(Value::is_array) {
                data["summary"] = json!([]);
            }
            data["summary"]
                .as_array_mut()
                .expect("summary array")
                .push(chunk);
        }
        let new_bytes = data.to_string().len();
        self.reserve(
            new_bytes.saturating_sub(old_bytes)
                + if existed {
                    0
                } else {
                    context_bytes(&self.context) + item_id.len()
                },
            !existed,
        )?;
        if !existed {
            self.compaction_origins
                .insert(id.into(), self.context.clone());
            self.compaction_item_ids.insert(id.into(), item_id.clone());
        }
        self.retained_bytes = self
            .retained_bytes
            .saturating_sub(old_bytes.saturating_sub(new_bytes));
        // Existing domain lifecycle uses started/running rather than ACP's
        // in_progress. Preserve the exact wire status alongside that mapping.
        let mut details = data.clone();
        if details["status"] == "in_progress" {
            details["status"] = json!("running");
        }
        details["acpStatus"] = data["status"].clone();
        let item = system_item(
            item_id,
            "Context compaction",
            "agent_context_compaction",
            details,
        );
        let origin = &self.compaction_origins[id];
        if !existed {
            projection.durable.push(AgentDurableEvent::ItemStarted {
                notification: ItemStartedNotification {
                    workspace_id: origin.workspace_id.clone(),
                    thread_id: origin.thread_id.clone(),
                    turn_id: origin.turn_id.clone(),
                    item: item.clone(),
                },
            });
        }
        projection
            .snapshots
            .push(ExecutionSnapshotEvent::ItemUpdated {
                notification: ItemUpdatedNotification {
                    workspace_id: origin.workspace_id.clone(),
                    thread_id: origin.thread_id.clone(),
                    turn_id: origin.turn_id.clone(),
                    item,
                },
            });
        self.compactions.insert(id.to_owned(), data);
        Ok(())
    }

    /// Finalization remains an existing Gateway transaction. An early message
    /// followed by tools is never promoted to the Task's final answer.
    pub fn final_answer(
        &mut self,
        response: &schema::PromptResponse,
    ) -> schema::Result<Option<ItemCompletedNotification>> {
        if !matches!(
            response.stop_reason,
            schema::StopReason::EndTurn
                | schema::StopReason::MaxTokens
                | schema::StopReason::MaxTurnRequests
                | schema::StopReason::Refusal
        ) {
            return Ok(None);
        }
        let Some(id) = &self.last_answer else {
            return Ok(None);
        };
        let message = self
            .messages
            .get(id)
            .ok_or_else(schema::Error::internal_error)?;
        if message.text.trim().is_empty() {
            return Ok(None);
        }
        Ok(Some(
            self.completed_notification(Self::message_item(message, true)),
        ))
    }

    /// Flushes the remaining live segment before Gateway prepares its existing
    /// finalization intent. The returned candidate is not a committed result.
    pub fn finish(
        &mut self,
        response: &schema::PromptResponse,
    ) -> schema::Result<(Projection, Option<ItemCompletedNotification>)> {
        let mut projection = Projection::default();
        if !matches!(
            response.stop_reason,
            schema::StopReason::EndTurn | schema::StopReason::Cancelled
        ) {
            let item = system_item(
                self.id("stop")?,
                "Agent stopped",
                "acp_stop_reason",
                json!({"stopReason":response.stop_reason}),
            );
            projection.durable.push(self.completed(item));
        }
        let candidate = self.final_answer(response)?;
        self.end_message(&mut projection);
        if self.deferred_answer.as_ref().is_some_and(|id| {
            candidate
                .as_ref()
                .is_some_and(|answer| answer.item.item_id() == id)
        }) {
            self.deferred_answer.take();
        } else {
            self.flush_deferred(&mut projection);
        }
        Ok((projection, candidate))
    }
}

fn context_bytes(context: &Context) -> usize {
    context.workspace_id.len()
        + context.thread_id.len()
        + context.turn_id.len()
        + context.session_scope.len()
        + context.session_id.to_string().len()
}

fn merge_present(target: &mut Value, patch: &Value) {
    if let (Some(target), Some(patch)) = (target.as_object_mut(), patch.as_object()) {
        for (key, value) in patch {
            if key != "sessionUpdate" {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

fn system_item(id: String, message: &str, code: &str, details: Value) -> TurnItem {
    TurnItem::SystemEvent {
        id,
        level: SystemEventLevel::Info,
        message: message.to_owned(),
        code: Some(code.to_owned()),
        details: Some(details),
    }
}

fn tool_item(
    session_id: &str,
    id: &str,
    data: &Value,
    terminal_outputs: &HashMap<String, String>,
) -> TurnItem {
    let status = match data.get("status").and_then(Value::as_str) {
        Some("completed") => ToolCallStatus::Completed,
        Some("failed") => ToolCallStatus::Failed,
        _ => ToolCallStatus::InProgress,
    };
    // Titles, names and rawInput are presentation data, never MCP identity or
    // invocation authority. Keep the wire toolCallId in a separate namespace.
    let name = data
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("acp_tool")
        .to_owned();
    let policy = ToolOutputPolicySnapshot::for_external_runtime_tool_name("acp_tool");
    let title = data
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("Agent tool");
    let limit = match policy.timeline {
        TimelineOutputPolicy::Summary { max_chars } => max_chars,
        _ => 2000,
    };
    let title_display = title.chars().take(limit).collect::<String>();
    // Metadata retention is not permission to persist arbitrary tool output.
    // Keep only bounded presentation, status and content fingerprints here.
    let content_bytes = data
        .get("content")
        .map(Value::to_string)
        .unwrap_or_default();
    let output_bytes = data
        .get("rawOutput")
        .map(Value::to_string)
        .unwrap_or_default();
    let metadata = json!({
        "acpToolCallId":id.chars().take(256).collect::<String>(),
        "acpToolCallIdSha256":hex::encode(Sha256::digest(id.as_bytes())),
        "agentToolName":name.chars().take(256).collect::<String>(),
        "kind":data.get("kind"), "status":data.get("status"),
        "contentSha256":hex::encode(Sha256::digest(content_bytes.as_bytes())), "contentBytes":content_bytes.len(),
        "outputSha256":hex::encode(Sha256::digest(output_bytes.as_bytes())), "outputBytes":output_bytes.len(),
    });
    let mut remaining = limit.saturating_sub(title_display.chars().count());
    let mut lines = Vec::new();
    let mut truncated = title.chars().count() > limit;
    let mut push = |text: &str| {
        let line = text.chars().take(remaining).collect::<String>();
        let count = line.chars().count();
        truncated |= count < text.chars().count();
        remaining = remaining.saturating_sub(count);
        if !line.is_empty() {
            lines.push(line);
        }
    };
    if let Some(content) = data.get("content").and_then(Value::as_array) {
        for block in content {
            match block.get("type").and_then(Value::as_str) {
                Some("content") => {
                    let content = &block["content"];
                    match content.get("type").and_then(Value::as_str) {
                        Some("text") => push(
                            content
                                .get("text")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                        ),
                        Some("image") => push("Image output"),
                        Some("audio") => push("Audio output"),
                        Some("resource") => push(
                            content
                                .pointer("/resource/text")
                                .and_then(Value::as_str)
                                .unwrap_or("Embedded resource"),
                        ),
                        Some("resource_link") => push(&format!(
                            "Resource: {} — {}",
                            content.get("name").and_then(Value::as_str).unwrap_or(""),
                            content.get("uri").and_then(Value::as_str).unwrap_or("")
                        )),
                        _ => {}
                    }
                }
                Some("diff") => {
                    push(&format!(
                        "Reported diff: {}",
                        block
                            .get("path")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                    ));
                    if let Some(old) = block.get("oldText").and_then(Value::as_str) {
                        push(&format!("- {old}"));
                    }
                    if let Some(new) = block.get("newText").and_then(Value::as_str) {
                        push(&format!("+ {new}"));
                    }
                }
                Some("terminal") => {
                    let terminal_id = block
                        .get("terminalId")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    push(
                        terminal_outputs
                            .get(terminal_id)
                            .map(String::as_str)
                            .unwrap_or("Terminal output unavailable"),
                    );
                }
                _ => {}
            }
        }
    }
    if !output_bytes.is_empty() {
        push(&output_bytes);
    }
    let summary = ToolOutputSummary {
        title: title_display,
        lines,
        metadata: ToolMetadata::from_json(metadata),
        truncated,
    };
    let display = match policy.timeline {
        TimelineOutputPolicy::Full { .. } | TimelineOutputPolicy::Summary { .. } => {
            ToolDisplayPayload::Summary(summary.clone())
        }
        TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden => {
            ToolDisplayPayload::Hidden
        }
    };
    let storage = match policy.storage {
        StorageOutputPolicy::Full { .. } | StorageOutputPolicy::Summary { .. } => {
            ToolStoragePayload::Summary(summary.clone())
        }
        StorageOutputPolicy::MetadataOnly => ToolStoragePayload::Metadata {
            metadata: summary.metadata.clone(),
        },
        StorageOutputPolicy::None => ToolStoragePayload::None,
    };
    TurnItem::DynamicToolCall {
        id: scoped_id(session_id, "tool", id),
        tool_name: "acp_tool".to_owned(),
        arguments: data.get("rawInput").cloned().unwrap_or(json!({})),
        status,
        execution_class: TurnItemExecutionClass::Standard,
        recovery_policy: None,
        output_policy: policy,
        display,
        storage,
        recovery: None,
        success: match status {
            ToolCallStatus::Completed => Some(true),
            ToolCallStatus::Failed => Some(false),
            _ => None,
        },
        outcome: None,
        observation: None,
    }
}

fn sanitize_compaction_media(
    content: &mut Value,
    item_id: &str,
    projection: &mut Projection,
) -> schema::Result<()> {
    if matches!(
        content.get("type").and_then(Value::as_str),
        Some("image" | "audio" | "resource")
    ) {
        let media: ContentBlock =
            serde_json::from_value(content.clone()).map_err(|_| schema::Error::invalid_params())?;
        projection.media.push(MediaOutput {
            item_id: item_id.into(),
            content: media,
        });
        *content = json!({"type":"text","text":"[Agent embedded content]"});
    }
    Ok(())
}

fn collect_media(
    scope: &str,
    id: &str,
    data: &Value,
    projection: &mut Projection,
) -> schema::Result<()> {
    for block in data
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if block.get("type").and_then(Value::as_str) != Some("content") {
            continue;
        }
        let Some(content) = block.get("content") else {
            continue;
        };
        let content: ContentBlock =
            serde_json::from_value(content.clone()).map_err(|_| schema::Error::invalid_params())?;
        if matches!(
            content,
            ContentBlock::Image(_) | ContentBlock::Audio(_) | ContentBlock::Resource(_)
        ) {
            projection.media.push(MediaOutput {
                item_id: scoped_id(scope, "tool", id),
                content,
            });
        }
    }
    Ok(())
}

fn scoped_id(scope: &str, kind: &str, opaque_id: &str) -> String {
    // Protocol IDs may contain any delimiter or be very long. Length-prefix
    // the tuple so model-controlled strings cannot alias another item kind.
    let mut digest = Sha256::new();
    for part in [scope, kind, opaque_id] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    format!("acp_{}", hex::encode(digest.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn reducer() -> Reducer {
        Reducer::new(
            Context {
                workspace_id: "w".into(),
                thread_id: "t".into(),
                turn_id: "turn".into(),
                session_id: schema::SessionId::new("s"),
                session_scope: "instance:boot:1:s".into(),
            },
            64,
            65536,
        )
        .unwrap()
    }
    fn notification(update: Value) -> schema::SessionNotification {
        serde_json::from_value(json!({"sessionId":"s", "update":update})).unwrap()
    }
    #[test]
    fn repeated_text_chunks_are_appended_and_tools_end_answer_candidate() {
        let mut reducer = reducer();
        for _ in 0..2 {
            reducer.update(notification(json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"same"}}))).unwrap();
        }
        let response = schema::PromptResponse::new(schema::StopReason::EndTurn);
        let final_item = reducer.final_answer(&response).unwrap().unwrap().item;
        assert!(matches!(final_item, TurnItem::AgentMessage { text, .. } if text == "samesame"));
        reducer.update(notification(json!({"sessionUpdate":"tool_call","toolCallId":"tool","title":"Run","status":"pending"}))).unwrap();
        assert!(reducer.final_answer(&response).unwrap().is_none());
        let patched = reducer.update(notification(json!({"sessionUpdate":"tool_call_update","toolCallId":"tool","status":"completed"}))).unwrap();
        assert_eq!(patched.durable.len(), 1);
        assert!(
            matches!(&patched.durable[0], AgentDurableEvent::ItemCompleted { notification } if matches!(notification.item, TurnItem::DynamicToolCall { .. }))
        );
    }

    #[test]
    fn prompt_completion_does_not_first_complete_the_final_segment_as_commentary() {
        let mut reducer = reducer();
        reducer.update(notification(json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Final response"}}))).unwrap();
        let (projection, candidate) = reducer
            .finish(&schema::PromptResponse::new(schema::StopReason::EndTurn))
            .unwrap();
        assert!(projection.durable.is_empty());
        assert!(matches!(
            candidate.unwrap().item,
            TurnItem::AgentMessage {
                phase: AgentMessagePhase::FinalAnswer,
                ..
            }
        ));
    }

    #[test]
    fn client_terminal_snapshot_updates_only_its_referencing_tool_and_keeps_generic_policy() {
        let mut reducer = reducer();
        reducer.update(notification(json!({"sessionUpdate":"tool_call","toolCallId":"opaque-tool","title":"exec_command","status":"in_progress","content":[{"type":"terminal","terminalId":"owned-terminal"}]}))).unwrap();
        assert!(
            reducer
                .terminal_output("unrelated", "untrusted")
                .unwrap()
                .snapshots
                .is_empty()
        );
        let projected = reducer
            .terminal_output("owned-terminal", "terminal output")
            .unwrap();
        let ExecutionSnapshotEvent::ItemUpdated { notification } = &projected.snapshots[0];
        let TurnItem::DynamicToolCall {
            tool_name,
            display,
            storage,
            ..
        } = &notification.item
        else {
            panic!("must remain generic")
        };
        assert_eq!(tool_name, "acp_tool");
        assert!(
            serde_json::to_string(display)
                .unwrap()
                .contains("terminal output")
        );
        assert!(
            !serde_json::to_string(storage)
                .unwrap()
                .contains("terminal output")
        );
        assert!(
            reducer
                .terminal_output("owned-terminal", "terminal output")
                .unwrap()
                .snapshots
                .is_empty()
        );
    }

    #[test]
    fn compaction_omission_null_and_replacement_remain_distinct() {
        let mut reducer = reducer();
        for update in [
            json!({"sessionUpdate":"compaction_summary_chunk","compactionId":"c","content":{"type":"text","text":"first"}}),
            json!({"sessionUpdate":"compaction_update","compactionId":"c","status":"completed"}),
        ] {
            reducer.update(notification(update)).unwrap();
        }
        assert_eq!(reducer.compactions["c"]["summary"][0]["text"], "first");
        reducer.update(notification(json!({"sessionUpdate":"compaction_update","compactionId":"c","status":"failed","summary":null}))).unwrap();
        assert!(reducer.compactions["c"]["summary"].is_null());
        reducer.update(notification(json!({"sessionUpdate":"compaction_summary_chunk","compactionId":"c","content":{"type":"text","text":"second"}}))).unwrap();
        assert_eq!(
            reducer.compactions["c"]["summary"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn thoughts_after_answer_do_not_commit_commentary_before_final_answer() {
        let mut reducer = reducer();
        let answer = reducer.update(notification(json!({"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"final"}}))).unwrap();
        assert_eq!(answer.durable.len(), 1);
        let thought = reducer.update(notification(json!({"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":"checking"}}))).unwrap();
        assert!(!thought.durable.iter().any(|event| matches!(event,AgentDurableEvent::ItemCompleted { notification } if matches!(notification.item,TurnItem::AgentMessage { .. }))));
        let (projection, answer) = reducer
            .finish(&schema::PromptResponse::new(schema::StopReason::EndTurn))
            .unwrap();
        assert!(!projection.durable.iter().any(|event| matches!(event,AgentDurableEvent::ItemCompleted { notification } if matches!(notification.item,TurnItem::AgentMessage { .. }))));
        assert!(matches!(
            answer.unwrap().item,
            TurnItem::AgentMessage {
                phase: AgentMessagePhase::FinalAnswer,
                ..
            }
        ));
    }
    #[test]
    fn late_entity_patches_keep_answer_but_new_activity_invalidates_it() {
        for first in [
            json!({"sessionUpdate":"tool_call","toolCallId":"old","title":"work"}),
            json!({"sessionUpdate":"compaction_update","compactionId":"old","status":"in_progress"}),
        ] {
            let mut reducer = reducer();
            let tool = first["sessionUpdate"] == "tool_call";
            reducer.update(notification(first)).unwrap();
            for (id, text) in [("a", "one"), ("b", "other"), ("a", "two")] {
                reducer.update(notification(json!({"sessionUpdate":"agent_message_chunk","messageId":id,"content":{"type":"text","text":text}}))).unwrap();
            }
            let patch = if tool {
                json!({"sessionUpdate":"tool_call_update","toolCallId":"old","status":"completed"})
            } else {
                json!({"sessionUpdate":"compaction_summary_chunk","compactionId":"old","content":{"type":"text","text":"summary"}})
            };
            reducer.update(notification(patch)).unwrap();
            let response = schema::PromptResponse::new(schema::StopReason::EndTurn);
            let answer = reducer.final_answer(&response).unwrap().unwrap();
            assert!(matches!(answer.item, TurnItem::AgentMessage { text, .. } if text == "onetwo"));
            reducer.update(notification(json!({"sessionUpdate":"tool_call_update","toolCallId":"new","status":"in_progress"}))).unwrap();
            assert!(reducer.final_answer(&response).unwrap().is_none());
        }
    }

    #[test]
    fn media_is_transient_until_gateway_authorizes_an_exact_artifact() {
        let mut reducer = reducer();
        let projection = reducer.update(notification(json!({"sessionUpdate":"agent_message_chunk","content":{"type":"image","mimeType":"image/png","data":"c2VjcmV0","uri":"file:///private/never-read"}}))).unwrap();
        assert_eq!(projection.media.len(), 1);
        let events =
            serde_json::to_string(&projection.runtime_events("session", "turn").unwrap()).unwrap();
        assert!(!events.contains("c2VjcmV0"));
        assert!(!events.contains("file:///private"));
    }
}
