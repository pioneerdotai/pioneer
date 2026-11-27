//! Host-owned positional declarations for the prepared Responses history.
//! Current request.tools remains authoritative for execution.
use crate::{ChatMessage, ToolDefinition};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const KEY: &str = "tool_declarations_v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolDeclarations {
    #[serde(skip)]
    pending: bool,
    prefix: Vec<String>,
    batches: Vec<Batch>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Batch {
    before_message: usize,
    tools: Vec<ToolDefinition>,
}

/// Identity used for a confirmed declaration position. Application-only
/// metadata is excluded in both admission and recovery.
pub fn message_hash(message: &ChatMessage) -> anyhow::Result<String> {
    let mut message = message.clone();
    message.application_context = None;
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&message)?)))
}

fn prefix(messages: &[ChatMessage]) -> anyhow::Result<Vec<String>> {
    messages.iter().map(message_hash).collect()
}

impl ToolDeclarations {
    /// Carry only the last confirmed positions through Gateway history preparation.
    pub fn pending(previous: Option<Self>) -> Self {
        let mut plan = previous.unwrap_or_else(Self::empty);
        plan.pending = true;
        plan
    }

    fn empty() -> Self {
        Self {
            pending: false,
            prefix: Vec::new(),
            batches: Vec::new(),
        }
    }

    pub(crate) fn select(request: &crate::ChatRequest) -> anyhow::Result<Option<Self>> {
        if request
            .messages
            .iter()
            .any(|message| !message.content_parts.is_empty())
        {
            return Ok(None);
        }
        match (&request.tool_declarations, request.tools.as_deref()) {
            (Some(plan), Some(tools)) if plan.pending && !tools.is_empty() => Ok(Some(
                Self::reconcile(Some(plan.clone()), &request.messages, tools)?,
            )),
            _ => Ok(Self::for_request(request)),
        }
    }

    /// Rebuild on schema change, removal, reorder or a replaced history prefix.
    /// Initial definitions are all in input, avoiding top-level override rules.
    pub fn reconcile(
        previous: Option<Self>,
        messages: &[ChatMessage],
        tools: &[ToolDefinition],
    ) -> anyhow::Result<Self> {
        let hashes = prefix(messages)?;
        let mut state = previous
            .filter(|state| state.valid_prefix(&hashes))
            .unwrap_or_else(Self::empty);
        state.pending = false;
        let old = state
            .batches
            .iter()
            .flat_map(|batch| batch.tools.iter())
            .collect::<Vec<_>>();
        let additive = old.len() <= tools.len()
            && old
                .iter()
                .zip(tools)
                .all(|(a, b)| wire_tool(a) == wire_tool(b));
        let start = if additive { old.len() } else { 0 };
        if !additive {
            state.batches.clear();
        }
        if start < tools.len() {
            state.batches.push(Batch {
                before_message: if state.batches.is_empty() {
                    0
                } else {
                    messages.len()
                },
                tools: tools[start..].to_vec(),
            });
        }
        state.prefix = hashes;
        Ok(state)
    }

    fn valid_prefix(&self, hashes: &[String]) -> bool {
        hashes.starts_with(&self.prefix)
            && self
                .batches
                .iter()
                .all(|batch| batch.before_message <= self.prefix.len())
            && self
                .batches
                .windows(2)
                .all(|pair| pair[0].before_message <= pair[1].before_message)
    }

    pub fn confirmed_message_prefix(&self) -> Option<&[String]> {
        (!self.pending && self.valid_prefix(&self.prefix)).then_some(self.prefix.as_slice())
    }

    pub fn from_metadata(metadata: &serde_json::Value) -> Option<Self> {
        serde_json::from_value(metadata.get(KEY)?.clone()).ok()
    }

    pub fn save(&self, metadata: &mut serde_json::Value) -> anyhow::Result<()> {
        anyhow::ensure!(!self.pending, "tool declarations have not been selected");
        anyhow::ensure!(
            metadata.is_object(),
            "tool declaration metadata must be an object"
        );
        metadata[KEY] = serde_json::to_value(self)?;
        Ok(())
    }

    /// Validate the selected positions and bind them to the complete request.
    /// An appended message can extend the proof without moving any batch.
    pub fn for_request(request: &crate::ChatRequest) -> Option<Self> {
        let plan = request.tool_declarations.as_ref()?;
        let tools = request.tools.as_deref()?;
        if plan.pending
            || tools.is_empty()
            || request
                .messages
                .iter()
                .any(|message| !message.content_parts.is_empty())
        {
            return None;
        }
        let hashes = prefix(&request.messages).ok()?;
        if !plan.valid_prefix(&hashes) {
            return None;
        }
        let declared = plan
            .batches
            .iter()
            .flat_map(|batch| batch.tools.iter())
            .collect::<Vec<_>>();
        (declared.len() == tools.len()
            && declared
                .iter()
                .zip(tools)
                .all(|(a, b)| wire_tool(a) == wire_tool(b)))
        .then(|| Self {
            prefix: hashes,
            ..plan.clone()
        })
    }

    /// Bind a fresh initial batch to the accepted replacement. Compaction
    /// sizes that batch before the summary text and provenance are available.
    pub fn bind_replacement(&mut self, messages: &[ChatMessage]) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.pending
                && self.prefix.is_empty()
                && self.batches.len() == 1
                && self.batches[0].before_message == 0,
            "replacement must start with one initial declaration batch"
        );
        self.prefix = prefix(messages)?;
        Ok(())
    }

    /// Calibration tracks declaration bytes/positions, not local prefix proofs.
    pub fn projection_identity(&self) -> serde_json::Value {
        serde_json::Value::Array(self.batches.iter().map(|batch| serde_json::json!({
            "before_message":batch.before_message, "tools":batch.tools.iter().map(wire_tool).collect::<Vec<_>>()
        })).collect())
    }

    /// Accounting projection: each current definition once, plus native framing.
    /// Fallback routes can conservatively retain this framing allowance.
    pub fn budget_tools(&self) -> serde_json::Value {
        serde_json::Value::Array(self.batches.iter().map(|batch| serde_json::json!({
            "type":"additional_tools", "role":"developer", "tools":batch.tools.iter().map(wire_tool).collect::<Vec<_>>()
        })).collect())
    }

    pub(crate) fn items_before(
        &self,
        index: usize,
    ) -> impl Iterator<Item = serde_json::Value> + '_ {
        self.batches.iter().filter(move |batch| batch.before_message == index).map(|batch| {
            serde_json::json!({"type":"additional_tools", "role":"developer", "tools":batch.tools.iter().map(wire_tool).collect::<Vec<_>>()})
        })
    }
}

pub(crate) fn wire_tool(tool: &ToolDefinition) -> serde_json::Value {
    serde_json::json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.parameters,"strict":false})
}
