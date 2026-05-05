use crate::{
    HookContext, HookContribution, HookDiagnostic, HookId, HookMetadata, HookPhase, HookValue,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HookInputKind {
    TurnPrePolicy,
    TurnPrePromptContext,
    TurnPrePromptCompile,
    TurnPostPromptCompile,
    TurnPostTurn,
    TurnPreCompaction,
    Custom(String),
}

impl HookInputKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::TurnPrePolicy => "turn.pre_policy",
            Self::TurnPrePromptContext => "turn.pre_prompt_context",
            Self::TurnPrePromptCompile => "turn.pre_prompt_compile",
            Self::TurnPostPromptCompile => "turn.post_prompt_compile",
            Self::TurnPostTurn => "turn.post_turn",
            Self::TurnPreCompaction => "turn.pre_compaction",
            Self::Custom(kind) => kind.as_str(),
        }
    }
}

impl From<HookPhase> for HookInputKind {
    fn from(phase: HookPhase) -> Self {
        match phase {
            HookPhase::TurnPrePolicy => Self::TurnPrePolicy,
            HookPhase::TurnPrePromptContext => Self::TurnPrePromptContext,
            HookPhase::TurnPrePromptCompile => Self::TurnPrePromptCompile,
            HookPhase::TurnPostPromptCompile => Self::TurnPostPromptCompile,
            HookPhase::TurnPostTurn => Self::TurnPostTurn,
            HookPhase::TurnPreCompaction => Self::TurnPreCompaction,
        }
    }
}

impl From<&str> for HookInputKind {
    fn from(value: &str) -> Self {
        match value {
            "turn.pre_policy" => Self::TurnPrePolicy,
            "turn.pre_prompt_context" => Self::TurnPrePromptContext,
            "turn.pre_prompt_compile" => Self::TurnPrePromptCompile,
            "turn.post_prompt_compile" => Self::TurnPostPromptCompile,
            "turn.post_turn" => Self::TurnPostTurn,
            "turn.pre_compaction" => Self::TurnPreCompaction,
            other => Self::Custom(other.to_owned()),
        }
    }
}

impl From<String> for HookInputKind {
    fn from(value: String) -> Self {
        match value.as_str() {
            "turn.pre_policy" => Self::TurnPrePolicy,
            "turn.pre_prompt_context" => Self::TurnPrePromptContext,
            "turn.pre_prompt_compile" => Self::TurnPrePromptCompile,
            "turn.post_prompt_compile" => Self::TurnPostPromptCompile,
            "turn.post_turn" => Self::TurnPostTurn,
            "turn.pre_compaction" => Self::TurnPreCompaction,
            _ => Self::Custom(value),
        }
    }
}

impl fmt::Display for HookInputKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for HookInputKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HookInputKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(Self::from(value))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookInput {
    pub kind: HookInputKind,
    pub payload: HookValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookHandlerRequest {
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub context: HookContext,
    pub input: HookInput,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookHandlerResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contributions: Vec<HookContribution>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: HookMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HookMetadataKey, HookThreadId, HookTurnId};

    #[test]
    fn handler_request_response_roundtrip() {
        let request = HookHandlerRequest {
            hook_id: HookId::new("policy.turn_classifier").expect("valid id"),
            phase: HookPhase::TurnPrePolicy,
            context: HookContext {
                thread_id: Some(HookThreadId::new("thread-1").expect("valid thread id")),
                turn_id: Some(HookTurnId::new("turn-1").expect("valid turn id")),
                ..HookContext::default()
            },
            input: HookInput {
                kind: HookInputKind::TurnPrePolicy,
                payload: HookValue::Object(BTreeMap::from([
                    (
                        HookMetadataKey::new("await_policy").expect("valid metadata key"),
                        HookValue::Text("deadline".to_owned()),
                    ),
                    (
                        HookMetadataKey::new("timeout_ms").expect("valid metadata key"),
                        HookValue::I64(500),
                    ),
                ])),
            },
        };

        let value = serde_json::to_value(&request).expect("request should serialize");
        let decoded: HookHandlerRequest =
            serde_json::from_value(value).expect("request should deserialize");
        assert_eq!(decoded, request);

        let response = HookHandlerResponse::default();
        let value = serde_json::to_value(&response).expect("response should serialize");
        let decoded: HookHandlerResponse =
            serde_json::from_value(value).expect("response should deserialize");
        assert_eq!(decoded, response);
    }
}
