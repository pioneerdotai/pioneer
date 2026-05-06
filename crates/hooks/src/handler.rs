use crate::{
    HookCapability, HookExecutionPolicy, HookFailurePolicy, HookHandlerRequest,
    HookHandlerResponse, HookId, HookKind, HookMetadata, HookPhase, HookResult,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookCapabilities {
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub names: BTreeSet<HookCapability>,
    #[serde(default, skip_serializing_if = "HookMetadata::is_empty")]
    pub metadata: HookMetadata,
}

impl HookCapabilities {
    pub fn new(names: impl IntoIterator<Item = HookCapability>) -> Self {
        Self {
            names: names.into_iter().collect(),
            metadata: HookMetadata::default(),
        }
    }

    pub fn contains(&self, capability: &HookCapability) -> bool {
        self.names.contains(capability)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookHandlerDescriptor {
    pub hook_id: HookId,
    pub hook_kind: HookKind,
    pub supported_phases: Vec<HookPhase>,
    pub version: u32,
    pub input_contract_version: u32,
    pub output_contract_version: u32,
    pub default_execution_policy: HookExecutionPolicy,
    pub default_failure_policy: HookFailurePolicy,
    pub capabilities: HookCapabilities,
}

impl HookHandlerDescriptor {
    pub fn from_handler(handler: &dyn HookHandler) -> Self {
        Self {
            hook_id: handler.id(),
            hook_kind: handler.kind(),
            supported_phases: normalized_phases(handler.supported_phases()),
            version: handler.version(),
            input_contract_version: handler.input_contract_version(),
            output_contract_version: handler.output_contract_version(),
            default_execution_policy: handler.default_execution_policy(),
            default_failure_policy: handler.default_failure_policy(),
            capabilities: handler.capabilities(),
        }
    }
}

#[async_trait]
pub trait HookHandler: Send + Sync {
    fn id(&self) -> HookId;

    fn kind(&self) -> HookKind;

    fn supported_phases(&self) -> Vec<HookPhase>;

    fn version(&self) -> u32 {
        1
    }

    fn input_contract_version(&self) -> u32 {
        1
    }

    fn output_contract_version(&self) -> u32 {
        1
    }

    fn default_execution_policy(&self) -> HookExecutionPolicy {
        HookExecutionPolicy::default()
    }

    fn default_failure_policy(&self) -> HookFailurePolicy {
        HookFailurePolicy::BestEffort
    }

    fn capabilities(&self) -> HookCapabilities {
        HookCapabilities::default()
    }

    async fn execute(&self, request: HookHandlerRequest) -> HookResult<HookHandlerResponse>;
}

pub(crate) fn normalized_phases(phases: Vec<HookPhase>) -> Vec<HookPhase> {
    phases
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HookError, HookValue};

    struct TestHookHandler;

    #[async_trait]
    impl HookHandler for TestHookHandler {
        fn id(&self) -> HookId {
            HookId::new("test.handler").expect("valid hook id")
        }

        fn kind(&self) -> HookKind {
            HookKind::new("test").expect("valid hook kind")
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            vec![HookPhase::TurnPostTurn, HookPhase::TurnPrePolicy]
        }

        fn capabilities(&self) -> HookCapabilities {
            let mut capabilities =
                HookCapabilities::new([HookCapability::new("audit").expect("valid capability")]);
            capabilities.metadata.insert(
                crate::HookMetadataKey::new("stable").expect("valid metadata key"),
                HookValue::Bool(true),
            );
            capabilities
        }

        async fn execute(&self, _request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
            Err(HookError::new(
                crate::HookDiagnosticCode::new("not.executed").expect("valid code"),
                crate::HookDiagnosticMessage::new("not executed").expect("valid message"),
            ))
        }
    }

    #[test]
    fn handler_descriptor_is_sorted_and_typed() {
        let descriptor = HookHandlerDescriptor::from_handler(&TestHookHandler);

        assert_eq!(descriptor.hook_id.as_str(), "test.handler");
        assert_eq!(descriptor.hook_kind.as_str(), "test");
        assert_eq!(
            descriptor.supported_phases,
            vec![HookPhase::TurnPrePolicy, HookPhase::TurnPostTurn]
        );
        assert_eq!(descriptor.version, 1);
        assert!(
            descriptor
                .capabilities
                .names
                .contains(&HookCapability::new("audit").expect("valid capability"))
        );
    }

    #[test]
    fn hook_capabilities_roundtrip() {
        let capabilities = HookCapabilities::new([
            HookCapability::new("policy").expect("valid capability"),
            HookCapability::new("audit").expect("valid capability"),
        ]);

        let value = serde_json::to_value(&capabilities).expect("capabilities serialize");
        let decoded: HookCapabilities =
            serde_json::from_value(value).expect("capabilities deserialize");

        assert_eq!(decoded, capabilities);
    }
}
