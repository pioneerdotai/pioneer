//! Canonical provider transcript projection, independent of concrete provider APIs.
use crate::SourceRef;
use anyhow::{Result, ensure};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub struct CanonicalRecord {
    pub reference: SourceRef,
    pub source_type: String,
    pub item_id: Option<String>,
    pub payload: Value,
}
#[derive(Clone, Debug)]
pub struct CanonicalRound {
    pub sources: Vec<SourceRef>,
    pub messages: Vec<Value>,
    pub complete: bool,
}
struct Pending {
    source: SourceRef,
    message: Value,
    calls: Vec<(String, String, String)>,
    results: BTreeMap<String, (SourceRef, Value)>,
}
impl Pending {
    fn finish(self) -> CanonicalRound {
        let mut round = CanonicalRound {
            sources: vec![self.source],
            messages: vec![self.message],
            complete: true,
        };
        for (item, _, _) in self.calls {
            if let Some((source, value)) = self.results.get(&item) {
                round.sources.push(source.clone());
                round.messages.push(value.clone());
            } else {
                round.complete = false;
            }
        }
        round
    }
}

pub fn canonical_rounds(records: Vec<CanonicalRecord>) -> Result<Vec<CanonicalRound>> {
    let mut rounds = Vec::new();
    let mut pending: Option<Pending> = None;
    let mut seen = BTreeMap::new();
    for row in records {
        if let Some(previous) = seen.insert(row.reference.clone(), row.payload.clone()) {
            ensure!(
                previous == row.payload,
                "conflicting canonical source revision"
            );
            continue;
        }
        match row.source_type.as_str() {
            "assistant_round" if row.payload.get("version") == Some(&Value::from(1)) => {
                if let Some(round) = pending.take() {
                    rounds.push(round.finish());
                }
                let message = row
                    .payload
                    .get("message")
                    .ok_or_else(|| anyhow::anyhow!("missing canonical assistant"))?
                    .clone();
                ensure!(
                    message["role"] == "assistant",
                    "invalid canonical assistant role"
                );
                let tools = message["tool_calls"]
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("missing canonical tool calls"))?;
                let ids = row.payload["calls"]
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("missing canonical call identities"))?;
                ensure!(
                    tools.len() == ids.len() && !tools.is_empty(),
                    "incomplete call identities"
                );
                let mut items = BTreeSet::new();
                let mut providers = BTreeSet::new();
                let mut calls = Vec::new();
                for (ordinal, (tool, identity)) in tools.iter().zip(ids).enumerate() {
                    let item = identity["turn_item_id"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing item identity"))?;
                    let provider = identity["provider_call_id"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing call identity"))?;
                    let name = tool["name"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("missing tool name"))?;
                    ensure!(
                        !item.is_empty()
                            && !provider.is_empty()
                            && !name.is_empty()
                            && items.insert(item)
                            && providers.insert(provider)
                            && identity["ordinal"].as_u64() == Some(ordinal as u64)
                            && tool["id"] == provider,
                        "invalid canonical call mapping"
                    );
                    calls.push((item.into(), provider.into(), name.into()));
                }
                pending = Some(Pending {
                    source: row.reference,
                    message,
                    calls,
                    results: BTreeMap::new(),
                });
            }
            "tool_result_v2" => {
                let round = pending
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("tool result has no canonical round"))?;
                let item = row
                    .item_id
                    .ok_or_else(|| anyhow::anyhow!("tool result has no item identity"))?;
                let (_, provider, name) = round
                    .calls
                    .iter()
                    .find(|(id, _, _)| *id == item)
                    .ok_or_else(|| anyhow::anyhow!("tool result belongs to another round"))?;
                ensure!(
                    row.payload["truncated"] == false,
                    "truncated canonical result"
                );
                let message = row.payload["value"].clone();
                ensure!(
                    message["role"] == "tool"
                        && message["tool_call_id"] == *provider
                        && message["name"] == *name,
                    "canonical tool result identity mismatch"
                );
                ensure!(
                    !round.results.contains_key(&item),
                    "conflicting terminal tool results"
                );
                round.results.insert(item, (row.reference, message));
            }
            _ => {
                if let Some(round) = pending.take() {
                    rounds.push(round.finish());
                }
                // Legacy/CLI observations retain their complete available content and
                // provenance; they are never fabricated as a successful native tool round.
                rounds.push(CanonicalRound {
                    sources: vec![row.reference],
                    messages: vec![row.payload],
                    complete: false,
                });
            }
        }
    }
    if let Some(round) = pending {
        rounds.push(round.finish());
    }
    Ok(rounds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn record(id: &str, kind: &str, payload: Value) -> CanonicalRecord {
        CanonicalRecord {
            reference: SourceRef {
                scope: "t".into(),
                id: id.into(),
                version: "1".into(),
            },
            source_type: kind.into(),
            item_id: Some("item".into()),
            payload,
        }
    }
    fn assistant() -> CanonicalRecord {
        record(
            "assistant",
            "assistant_round",
            json!({"version":1,"message":{"role":"assistant","content":"", "reasoning_content":"known reasoning", "provider_replay_state":{"opaque":"keep"},"tool_calls":[{"id":"call","name":"read","arguments":"{}"}]},"calls":[{"provider_call_id":"call","turn_item_id":"item","ordinal":0}]}),
        )
    }
    #[test]
    fn canonical_round_is_atomic_and_preserves_reasoning_without_interpreting_opaque_state() {
        let result = record(
            "result",
            "tool_result_v2",
            json!({"truncated":false,"value":{"role":"tool","tool_call_id":"call","name":"read","content":"outcome"}}),
        );
        let rounds = canonical_rounds(vec![assistant(), result.clone(), result]).unwrap();
        assert_eq!(rounds.len(), 1);
        assert!(rounds[0].complete);
        assert_eq!(rounds[0].sources.len(), 2);
        assert_eq!(
            rounds[0].messages[0]["reasoning_content"],
            "known reasoning"
        );
        assert_eq!(
            rounds[0].messages[0]["provider_replay_state"]["opaque"],
            "keep"
        );
        assert!(!canonical_rounds(vec![assistant()]).unwrap()[0].complete);
    }
    #[test]
    fn foreign_result_is_not_invented_or_paired_by_text() {
        let result = record(
            "result",
            "tool_result_v2",
            json!({"truncated":false,"value":{"role":"tool","tool_call_id":"other","name":"read","content":"outcome"}}),
        );
        assert!(canonical_rounds(vec![assistant(), result]).is_err());
        assert_eq!(
            canonical_rounds(vec![
                record("a", "cli", json!("equal")),
                record("b", "cli", json!("equal"))
            ])
            .unwrap()
            .len(),
            2
        );
    }
}
