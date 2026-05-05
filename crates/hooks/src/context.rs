use crate::{
    HookActorId, HookAgentId, HookFeatureFlag, HookMetadata, HookTaskId, HookThreadId, HookTurnId,
    HookWorkspaceId,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HookActorKind {
    User,
    Agent,
    Task,
    System,
    Tool,
    Workspace,
    Thread,
    Automation,
    Service,
    Custom(String),
}

impl HookActorKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::User => "user",
            Self::Agent => "agent",
            Self::Task => "task",
            Self::System => "system",
            Self::Tool => "tool",
            Self::Workspace => "workspace",
            Self::Thread => "thread",
            Self::Automation => "automation",
            Self::Service => "service",
            Self::Custom(kind) => kind.as_str(),
        }
    }
}

impl From<&str> for HookActorKind {
    fn from(value: &str) -> Self {
        match value {
            "user" => Self::User,
            "agent" => Self::Agent,
            "task" => Self::Task,
            "system" => Self::System,
            "tool" => Self::Tool,
            "workspace" => Self::Workspace,
            "thread" => Self::Thread,
            "automation" => Self::Automation,
            "service" => Self::Service,
            other => Self::Custom(other.to_owned()),
        }
    }
}

impl From<String> for HookActorKind {
    fn from(value: String) -> Self {
        match value.as_str() {
            "user" => Self::User,
            "agent" => Self::Agent,
            "task" => Self::Task,
            "system" => Self::System,
            "tool" => Self::Tool,
            "workspace" => Self::Workspace,
            "thread" => Self::Thread,
            "automation" => Self::Automation,
            "service" => Self::Service,
            _ => Self::Custom(value),
        }
    }
}

impl fmt::Display for HookActorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for HookActorKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HookActorKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(Self::from(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HookContextMode {
    Chat,
    Agent,
    Task,
    System,
    Custom(String),
}

impl HookContextMode {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Chat => "chat",
            Self::Agent => "agent",
            Self::Task => "task",
            Self::System => "system",
            Self::Custom(mode) => mode.as_str(),
        }
    }
}

impl From<&str> for HookContextMode {
    fn from(value: &str) -> Self {
        match value {
            "chat" => Self::Chat,
            "agent" => Self::Agent,
            "task" => Self::Task,
            "system" => Self::System,
            other => Self::Custom(other.to_owned()),
        }
    }
}

impl From<String> for HookContextMode {
    fn from(value: String) -> Self {
        match value.as_str() {
            "chat" => Self::Chat,
            "agent" => Self::Agent,
            "task" => Self::Task,
            "system" => Self::System,
            _ => Self::Custom(value),
        }
    }
}

impl fmt::Display for HookContextMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for HookContextMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HookContextMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(Self::from(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookActor {
    pub kind: HookActorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<HookActorId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<HookWorkspaceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<HookThreadId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<HookTurnId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<HookTaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<HookAgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<HookContextMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<HookActor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_unix: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_home: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub feature_flags: BTreeMap<HookFeatureFlag, bool>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: HookMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_context_serializes_without_domain_dependencies() {
        let context = HookContext {
            workspace_id: Some(HookWorkspaceId::new("workspace-1").expect("valid workspace id")),
            thread_id: Some(HookThreadId::new("thread-1").expect("valid thread id")),
            turn_id: Some(HookTurnId::new("turn-1").expect("valid turn id")),
            mode: Some(HookContextMode::Agent),
            actor: Some(HookActor {
                kind: HookActorKind::User,
                id: Some(HookActorId::new("user-1").expect("valid actor id")),
            }),
            ..HookContext::default()
        };

        let value = serde_json::to_value(&context).expect("context should serialize");
        assert_eq!(value["workspace_id"], "workspace-1");
        assert_eq!(value["mode"], "agent");
        let decoded: HookContext =
            serde_json::from_value(value).expect("context should deserialize");
        assert_eq!(decoded, context);
    }

    #[test]
    fn hook_actor_kind_serializes_stable_names() {
        assert_eq!(
            serde_json::to_value(HookActorKind::User).expect("actor kind serializes"),
            serde_json::json!("user")
        );
        assert_eq!(
            serde_json::to_value(HookActorKind::Automation).expect("actor kind serializes"),
            serde_json::json!("automation")
        );
    }

    #[test]
    fn hook_actor_kind_deserializes_unknown_as_custom() {
        let kind: HookActorKind =
            serde_json::from_value(serde_json::json!("domain.worker")).expect("kind deserializes");

        assert_eq!(kind, HookActorKind::Custom("domain.worker".to_owned()));
        assert_eq!(kind.as_str(), "domain.worker");
    }

    #[test]
    fn hook_context_mode_deserializes_unknown_as_custom() {
        let mode: HookContextMode =
            serde_json::from_value(serde_json::json!("domain.mode")).expect("mode deserializes");

        assert_eq!(mode, HookContextMode::Custom("domain.mode".to_owned()));
        assert_eq!(mode.as_str(), "domain.mode");
    }
}
