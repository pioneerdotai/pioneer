//! Whole workspace model selections shared by General and compaction overrides.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelSelectionTransport {
    Api,
    Codex,
    Claude,
}

/// Inherit is a real absence of an override. It never stores a resolved fallback.
/// The tagged value also distinguishes an explicit reset from an omitted update.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum GatewayModelSelection {
    #[default]
    Inherit,
    Explicit {
        transport: ModelSelectionTransport,
        instance: String,
        model: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reasoning_effort: Option<String>,
    },
}
// A unit variant ignores extra fields in Serde's internally tagged representation.
// Use an empty struct variant at the decoding boundary to reject ambiguous resets.
impl<'de> Deserialize<'de> for GatewayModelSelection {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Inherit {},
            Explicit {
                transport: ModelSelectionTransport,
                instance: String,
                model: String,
                #[serde(default)]
                reasoning_effort: Option<String>,
            },
        }
        Ok(match Wire::deserialize(deserializer)? {
            Wire::Inherit {} => Self::Inherit,
            Wire::Explicit {
                transport,
                instance,
                model,
                reasoning_effort,
            } => Self::Explicit {
                transport,
                instance,
                model,
                reasoning_effort,
            },
        })
    }
}

impl GatewayModelSelection {
    pub fn normalized(self) -> Result<Self, String> {
        let Self::Explicit {
            transport,
            instance,
            model,
            reasoning_effort,
        } = self
        else {
            return Ok(Self::Inherit);
        };
        let instance = instance.trim();
        let model = model.trim();
        if instance.is_empty()
            || instance.len() > 256
            || model.is_empty()
            || model.len() > 512
            || instance.chars().any(char::is_control)
            || model.chars().any(char::is_control)
        {
            return Err("model selection requires a valid instance and model".into());
        }
        let reasoning_effort = reasoning_effort
            .map(|value| {
                crate::ReasoningEffort::canonical_value(&value)
                    .map(str::to_owned)
                    .ok_or_else(|| "invalid model reasoning effort".to_owned())
            })
            .transpose()?;
        Ok(Self::Explicit {
            transport,
            instance: instance.into(),
            model: model.into(),
            reasoning_effort,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_roundtrip_keeps_transport_instance_and_reasoning_as_one_value() {
        for transport in [
            ModelSelectionTransport::Api,
            ModelSelectionTransport::Codex,
            ModelSelectionTransport::Claude,
        ] {
            let selected = GatewayModelSelection::Explicit {
                transport,
                instance: "configured-instance".into(),
                model: "selected-model".into(),
                reasoning_effort: Some("high".into()),
            };
            let value = serde_json::to_value(&selected).unwrap();
            assert_eq!(
                serde_json::from_value::<GatewayModelSelection>(value)
                    .unwrap()
                    .normalized()
                    .unwrap(),
                selected
            );
        }
        assert_eq!(
            GatewayModelSelection::default(),
            GatewayModelSelection::Inherit
        );
        assert!(
            serde_json::from_str::<GatewayModelSelection>(
                r#"{"source":"explicit","transport":"codex","model":"m"}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<GatewayModelSelection>(
                r#"{"source":"inherit","model":"must-not-be-a-fallback"}"#
            )
            .is_err()
        );
    }
}
