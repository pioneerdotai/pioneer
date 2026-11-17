//! Host-owned positional declarations. These are proposals, never delivery
//! receipts or execution grants. Current request.tools remains authoritative.
use crate::{ChatMessage, ToolDefinition};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const KEY: &str = "tool_declarations_v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolDeclarations {
    prefix: Vec<String>,
    batches: Vec<Batch>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Batch {
    before_message: usize,
    tools: Vec<ToolDefinition>,
}

fn prefix(messages: &[ChatMessage]) -> anyhow::Result<Vec<String>> {
    messages
        .iter()
        .map(|message| {
            let mut message = message.clone();
            message.application_context = None;
            Ok(hex::encode(Sha256::digest(serde_json::to_vec(&message)?)))
        })
        .collect()
}

impl ToolDeclarations {
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
            .unwrap_or_else(|| Self {
                prefix: Vec::new(),
                batches: Vec::new(),
            });
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

    pub fn from_metadata(metadata: &serde_json::Value) -> Option<Self> {
        serde_json::from_value(metadata.get(KEY)?.clone()).ok()
    }

    pub fn save(&self, metadata: &mut serde_json::Value) -> anyhow::Result<()> {
        anyhow::ensure!(
            metadata.is_object(),
            "tool declaration metadata must be an object"
        );
        metadata[KEY] = serde_json::to_value(self)?;
        Ok(())
    }

    /// Select an applicable positional proposal after provider preflight. Never
    /// rebuild here: every consumer must describe the same final request/wire.
    pub fn for_request(request: &crate::ChatRequest) -> Option<Self> {
        let state = request.tool_declarations.clone()?;
        if !state.valid_prefix(&prefix(&request.messages).ok()?) {
            return None;
        }
        let tools = request.tools.as_deref()?;
        if tools.is_empty()
            || request
                .messages
                .iter()
                .any(|message| !message.content_parts.is_empty())
        {
            return None;
        }
        let declared = state
            .batches
            .iter()
            .flat_map(|batch| batch.tools.iter())
            .collect::<Vec<_>>();
        (declared.len() == tools.len()
            && declared
                .iter()
                .zip(tools)
                .all(|(a, b)| wire_tool(a) == wire_tool(b)))
        .then_some(state)
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
