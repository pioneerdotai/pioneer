//! Whole workspace model selections shared by General and compaction overrides.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelSelectionTransport {
    Api,
    Codex,
    Claude,
    #[serde(rename = "opencode")]
    OpenCode,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_window: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context_authority: Option<String>,
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
                #[serde(default)]
                context_window: Option<u64>,
                #[serde(default)]
                context_authority: Option<String>,
            },
        }
        Ok(match Wire::deserialize(deserializer)? {
            Wire::Inherit {} => Self::Inherit,
            Wire::Explicit {
                transport,
                instance,
                model,
                reasoning_effort,
                context_window,
                context_authority,
            } => Self::Explicit {
                transport,
                instance,
                model,
                reasoning_effort,
                context_window,
                context_authority,
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
            context_window,
            context_authority,
        } = self
        else {
            return Ok(Self::Inherit);
        };
        if context_window == Some(0)
            || (transport != ModelSelectionTransport::Api
                && (context_window.is_some() || context_authority.is_some()))
        {
            return Err("context selection is only available for Native/API chat models".into());
        }
        if context_window.is_some()
            && context_authority
                .as_ref()
                .is_none_or(|authority| authority.is_empty())
        {
            return Err(
                "context choice requires its provider authority; reopen the model picker".into(),
            );
        }
        let context_authority = if context_window.is_some() {
            context_authority
        } else {
            None
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
            context_window,
            context_authority,
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
            ModelSelectionTransport::OpenCode,
        ] {
            let selected = GatewayModelSelection::Explicit {
                context_authority: None,
                context_window: None,
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
    #[test]
    fn legacy_api_auto_explicit_reset_and_cli_wire_remain_distinct() {
        let old: GatewayModelSelection = serde_json::from_str(
            r#"{"source":"explicit","transport":"api","instance":"openai","model":"model"}"#,
        )
        .unwrap();
        assert!(matches!(
            old,
            GatewayModelSelection::Explicit {
                context_window: None,
                context_authority: None,
                ..
            }
        ));
        let manual = GatewayModelSelection::Explicit {
            transport: ModelSelectionTransport::Api,
            instance: "openai".into(),
            model: "model".into(),
            reasoning_effort: None,
            context_window: Some(1050000),
            context_authority: Some("account-a".into()),
        }
        .normalized()
        .unwrap();
        let restored: GatewayModelSelection =
            serde_json::from_str(&serde_json::to_string(&manual).unwrap()).unwrap();
        assert_eq!(restored, manual);
        assert_ne!(old, GatewayModelSelection::Inherit);
        let cli: GatewayModelSelection = serde_json::from_str(
            r#"{"source":"explicit","transport":"codex","instance":"codex","model":"m"}"#,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(cli).unwrap(),
            serde_json::json!({"source":"explicit","transport":"codex","instance":"codex","model":"m"})
        );
    }
}
