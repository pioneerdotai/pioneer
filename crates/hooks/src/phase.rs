use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HookPhase {
    TurnPrePolicy,
    TurnPrePromptContext,
    TurnPreToolMaterialization,
    TurnPrePromptCompile,
    TurnPostPromptCompile,
    RuntimeTurnPreContext,
    TurnPostTurn,
    TurnPreCompaction,
}

impl HookPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TurnPrePolicy => "turn.pre_policy",
            Self::TurnPrePromptContext => "turn.pre_prompt_context",
            Self::TurnPreToolMaterialization => "turn.pre_tool_materialization",
            Self::TurnPrePromptCompile => "turn.pre_prompt_compile",
            Self::TurnPostPromptCompile => "turn.post_prompt_compile",
            Self::RuntimeTurnPreContext => "runtime.turn_pre_context",
            Self::TurnPostTurn => "turn.post_turn",
            Self::TurnPreCompaction => "turn.pre_compaction",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseHookPhaseError {
    value: String,
}

impl fmt::Display for ParseHookPhaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown hook phase `{}`", self.value)
    }
}

impl std::error::Error for ParseHookPhaseError {}

impl FromStr for HookPhase {
    type Err = ParseHookPhaseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "turn.pre_policy" => Ok(Self::TurnPrePolicy),
            // Read legacy hook-run/audit metadata without restoring an executable phase.
            "turn.pre_prompt_context" | "turn.post_preflight_prompt_context" => {
                Ok(Self::TurnPrePromptContext)
            }
            "turn.pre_tool_materialization" => Ok(Self::TurnPreToolMaterialization),
            "turn.pre_prompt_compile" => Ok(Self::TurnPrePromptCompile),
            "turn.post_prompt_compile" => Ok(Self::TurnPostPromptCompile),
            "runtime.turn_pre_context" => Ok(Self::RuntimeTurnPreContext),
            "turn.post_turn" => Ok(Self::TurnPostTurn),
            "turn.pre_compaction" => Ok(Self::TurnPreCompaction),
            other => Err(ParseHookPhaseError {
                value: other.to_owned(),
            }),
        }
    }
}

impl fmt::Display for HookPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for HookPhase {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HookPhase {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(HookPhaseVisitor)
    }
}

struct HookPhaseVisitor;

impl Visitor<'_> for HookPhaseVisitor {
    type Value = HookPhase;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a stable dot-separated hook phase")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        HookPhase::from_str(value).map_err(E::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_phase_serializes_stable_dot_names() {
        assert_eq!(
            serde_json::to_value(HookPhase::TurnPrePolicy).expect("phase serializes"),
            serde_json::json!("turn.pre_policy")
        );
        assert_eq!(
            serde_json::to_value(HookPhase::TurnPostTurn).expect("phase serializes"),
            serde_json::json!("turn.post_turn")
        );
        assert_eq!(
            serde_json::to_value(HookPhase::TurnPreToolMaterialization).expect("phase serializes"),
            serde_json::json!("turn.pre_tool_materialization")
        );
        assert_eq!(
            serde_json::to_value(HookPhase::RuntimeTurnPreContext).expect("phase serializes"),
            serde_json::json!("runtime.turn_pre_context")
        );
    }

    #[test]
    fn hook_phase_deserializes_stable_dot_names() {
        let phase: HookPhase = serde_json::from_value(serde_json::json!("turn.pre_prompt_context"))
            .expect("phase deserializes");
        assert_eq!(phase, HookPhase::TurnPrePromptContext);

        let phase: HookPhase =
            serde_json::from_value(serde_json::json!("turn.pre_tool_materialization"))
                .expect("phase deserializes");
        assert_eq!(phase, HookPhase::TurnPreToolMaterialization);

        let phase: HookPhase =
            serde_json::from_value(serde_json::json!("runtime.turn_pre_context"))
                .expect("phase deserializes");
        assert_eq!(phase, HookPhase::RuntimeTurnPreContext);
    }

    #[test]
    fn legacy_post_preflight_phase_reads_and_serializes_as_prompt_context() {
        let legacy = "turn.post_preflight_prompt_context";
        // CRUD hook_run and hook_audit_event row decoders both use FromStr.
        let phase = HookPhase::from_str(legacy).expect("legacy stored phase reads");
        assert_eq!(phase, HookPhase::TurnPrePromptContext);
        let decoded: HookPhase = serde_json::from_value(serde_json::json!(legacy))
            .expect("legacy nested JSON phase reads");
        assert_eq!(decoded, phase);
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::json!("turn.pre_prompt_context")
        );
        assert!(HookPhase::from_str("turn.unknown_legacy_phase").is_err());
    }

    #[test]
    fn hook_phase_rejects_unknown_names() {
        let error =
            serde_json::from_value::<HookPhase>(serde_json::json!("domain.specific")).unwrap_err();
        assert!(error.to_string().contains("unknown hook phase"));
    }
}
