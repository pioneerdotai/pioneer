use crate::types::{ProviderResponseLimits, ProviderResponseTooLarge, ProviderToolCall};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct StreamToolCallDelta {
    #[serde(default)]
    pub(crate) index: Option<usize>,
    #[serde(default)]
    pub(crate) id: Option<String>,
    #[serde(default)]
    pub(crate) function: Option<StreamToolFunctionDelta>,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_argument_fragment")]
    pub(crate) arguments: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct StreamToolFunctionDelta {
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_argument_fragment")]
    pub(crate) arguments: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct StreamToolCallAccumulator {
    pending: BTreeMap<usize, PartialToolCall>,
    argument_bytes: usize,
    overflow: Option<ProviderResponseTooLarge>,
}

#[derive(Debug, Default)]
struct PartialToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

impl StreamToolCallAccumulator {
    pub(crate) fn ingest(&mut self, deltas: Vec<StreamToolCallDelta>) {
        for (fallback_index, delta) in deltas.into_iter().enumerate() {
            if self.overflow.is_some() {
                break;
            }
            let index = delta.index.unwrap_or(fallback_index);
            if !self.pending.contains_key(&index)
                && self.pending.len() >= ProviderResponseLimits::default().max_tool_calls
            {
                self.overflow = Some(ProviderResponseTooLarge::new(
                    "stream_tool_calls",
                    ProviderResponseLimits::default().max_tool_calls,
                    self.pending.len().saturating_add(1),
                ));
                break;
            }

            if let Some(id) = delta.id {
                if !id.is_empty() {
                    self.pending.entry(index).or_default().id = Some(id);
                }
            }

            let function_name = delta
                .function
                .as_ref()
                .and_then(|function| function.name.clone())
                .or(delta.name);
            if let Some(name) = function_name {
                if !name.is_empty() {
                    self.pending.entry(index).or_default().name = Some(name);
                }
            }

            let arguments_fragment = delta
                .function
                .and_then(|function| function.arguments)
                .or(delta.arguments);
            if let Some(arguments_fragment) = arguments_fragment {
                let next_bytes = self.argument_bytes.saturating_add(arguments_fragment.len());
                if next_bytes > ProviderResponseLimits::default().max_tool_arguments_bytes {
                    self.overflow = Some(ProviderResponseTooLarge::new(
                        "stream_tool_call_arguments",
                        ProviderResponseLimits::default().max_tool_arguments_bytes,
                        next_bytes,
                    ));
                    break;
                }
                self.pending
                    .entry(index)
                    .or_default()
                    .arguments
                    .push_str(arguments_fragment.as_str());
                self.argument_bytes = next_bytes;
            }
        }
    }

    pub(crate) fn take_tool_calls(&mut self) -> Result<Vec<ProviderToolCall>> {
        if let Some(error) = self.overflow.take() {
            self.pending.clear();
            return Err(error.into());
        }
        std::mem::take(&mut self.pending)
            .into_iter()
            .map(|(index, partial)| {
                let name = partial
                    .name
                    .filter(|name| !name.trim().is_empty())
                    .with_context(|| format!("provider tool call {index} is missing its name"))?;
                Ok(ProviderToolCall {
                    id: partial.id.unwrap_or_else(|| format!("call_{}", index + 1)),
                    name,
                    arguments: normalize_arguments(partial.arguments)?,
                })
            })
            .collect()
    }
}

fn normalize_arguments(arguments: String) -> Result<String> {
    if arguments.trim().is_empty() {
        bail!("provider tool call is missing arguments");
    }

    match serde_json::from_str::<Value>(arguments.as_str()) {
        Ok(value) => serde_json::to_string(&value)
            .context("provider tool call arguments cannot be serialized"),
        Err(error) => Err(error).context("provider tool call arguments are incomplete or invalid"),
    }
}

fn deserialize_argument_fragment<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.map(|value| match value {
        Value::String(text) => text,
        other => serde_json::to_string(&other).unwrap_or_else(|_| "{}".to_owned()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_incremental_tool_call_arguments() {
        let mut acc = StreamToolCallAccumulator::default();

        acc.ingest(vec![StreamToolCallDelta {
            index: Some(0),
            id: Some("call_1".to_owned()),
            function: Some(StreamToolFunctionDelta {
                name: Some("shell".to_owned()),
                arguments: Some("{\"command\":\"".to_owned()),
            }),
            name: None,
            arguments: None,
        }]);

        acc.ingest(vec![StreamToolCallDelta {
            index: Some(0),
            id: None,
            function: Some(StreamToolFunctionDelta {
                name: None,
                arguments: Some("pwd\"}".to_owned()),
            }),
            name: None,
            arguments: None,
        }]);

        let calls = acc.take_tool_calls().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[0].name, "shell");
        assert_eq!(calls[0].arguments, r#"{"command":"pwd"}"#);
    }

    #[test]
    fn incomplete_tool_call_fails_the_round() {
        let mut acc = StreamToolCallAccumulator::default();
        acc.ingest(vec![StreamToolCallDelta {
            index: Some(0),
            id: Some("call_1".to_owned()),
            function: Some(StreamToolFunctionDelta {
                name: Some("shell".to_owned()),
                arguments: Some("{".to_owned()),
            }),
            name: None,
            arguments: None,
        }]);
        assert!(acc.take_tool_calls().is_err());
    }

    #[test]
    fn oversized_stream_tool_arguments_are_rejected_before_accumulation() {
        let mut acc = StreamToolCallAccumulator::default();
        acc.ingest(vec![StreamToolCallDelta {
            index: Some(0),
            id: Some("call_1".to_owned()),
            function: Some(StreamToolFunctionDelta {
                name: Some("shell".to_owned()),
                arguments: Some(
                    "x".repeat(
                        ProviderResponseLimits::default()
                            .max_tool_arguments_bytes
                            .saturating_add(1),
                    ),
                ),
            }),
            name: None,
            arguments: None,
        }]);

        let error = acc
            .take_tool_calls()
            .expect_err("oversized stream arguments must be rejected");
        assert_eq!(
            error
                .downcast_ref::<ProviderResponseTooLarge>()
                .expect("typed response limit")
                .component,
            "stream_tool_call_arguments"
        );
    }
}
