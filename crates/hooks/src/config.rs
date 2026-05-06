use crate::{
    HookAwaitPolicy, HookExecutionPolicy, HookFailurePolicy, HookPhase, HookSubscriptionId,
    HookSubscriptionVisibility,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookConfigLayerKind {
    GlobalDefaults,
    WorkspacePolicy,
    AgentModeDefaults,
    DomainSpecific,
    PerTurnPolicy,
    RuntimeOverride,
}

impl Default for HookConfigLayerKind {
    fn default() -> Self {
        Self::GlobalDefaults
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookConfigLayer {
    pub kind: HookConfigLayerKind,
    #[serde(default)]
    pub config: HookRuntimeConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookRuntimeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub phases: BTreeMap<HookPhase, HookPhaseConfig>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub subscriptions: BTreeMap<HookSubscriptionId, HookSubscriptionConfig>,
}

impl HookRuntimeConfig {
    pub fn merge_layers(layers: impl IntoIterator<Item = HookConfigLayer>) -> Self {
        let mut layers = layers.into_iter().collect::<Vec<_>>();
        layers.sort_by_key(|layer| layer.kind);
        layers
            .into_iter()
            .fold(Self::default(), |mut merged, layer| {
                merged.merge(layer.config);
                merged
            })
    }

    pub fn merge(&mut self, next: Self) {
        merge_option(&mut self.enabled, next.enabled);
        merge_option(&mut self.default_timeout_ms, next.default_timeout_ms);
        for (phase, config) in next.phases {
            self.phases.entry(phase).or_default().merge(config);
        }
        for (subscription_id, config) in next.subscriptions {
            self.subscriptions
                .entry(subscription_id)
                .or_default()
                .merge(config);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookPhaseConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallelism: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_await_policy: Option<HookAwaitPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_failure_policy: Option<HookFailurePolicy>,
}

impl HookPhaseConfig {
    pub fn merge(&mut self, next: Self) {
        merge_option(&mut self.max_parallelism, next.max_parallelism);
        merge_option(&mut self.default_await_policy, next.default_await_policy);
        merge_option(
            &mut self.default_failure_policy,
            next.default_failure_policy,
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookSubscriptionConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_policy: Option<HookExecutionPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_policy: Option<HookFailurePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visibility: Option<HookSubscriptionVisibility>,
}

impl HookSubscriptionConfig {
    pub fn merge(&mut self, next: Self) {
        merge_option(&mut self.enabled, next.enabled);
        merge_option(&mut self.execution_policy, next.execution_policy);
        merge_option(&mut self.failure_policy, next.failure_policy);
        merge_option(&mut self.visibility, next.visibility);
    }
}

fn merge_option<T>(current: &mut Option<T>, next: Option<T>) {
    if next.is_some() {
        *current = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HookSubscriptionId;

    #[test]
    fn layered_config_merges_in_precedence_order() {
        let subscription_id =
            HookSubscriptionId::new("sub.context").expect("valid subscription id");
        let merged = HookRuntimeConfig::merge_layers([
            HookConfigLayer {
                kind: HookConfigLayerKind::RuntimeOverride,
                config: HookRuntimeConfig {
                    default_timeout_ms: Some(2_000),
                    ..HookRuntimeConfig::default()
                },
            },
            HookConfigLayer {
                kind: HookConfigLayerKind::GlobalDefaults,
                config: HookRuntimeConfig {
                    enabled: Some(true),
                    default_timeout_ms: Some(1_000),
                    phases: BTreeMap::from([(
                        HookPhase::TurnPrePromptContext,
                        HookPhaseConfig {
                            max_parallelism: Some(4),
                            default_await_policy: Some(HookAwaitPolicy::Deadline),
                            default_failure_policy: None,
                        },
                    )]),
                    subscriptions: BTreeMap::from([(
                        subscription_id.clone(),
                        HookSubscriptionConfig {
                            enabled: Some(true),
                            execution_policy: None,
                            failure_policy: Some(HookFailurePolicy::BestEffort),
                            visibility: Some(HookSubscriptionVisibility::Internal),
                        },
                    )]),
                },
            },
            HookConfigLayer {
                kind: HookConfigLayerKind::WorkspacePolicy,
                config: HookRuntimeConfig {
                    subscriptions: BTreeMap::from([(
                        subscription_id.clone(),
                        HookSubscriptionConfig {
                            enabled: Some(false),
                            visibility: Some(HookSubscriptionVisibility::Debug),
                            ..HookSubscriptionConfig::default()
                        },
                    )]),
                    ..HookRuntimeConfig::default()
                },
            },
        ]);

        assert_eq!(merged.enabled, Some(true));
        assert_eq!(merged.default_timeout_ms, Some(2_000));
        assert_eq!(
            merged
                .phases
                .get(&HookPhase::TurnPrePromptContext)
                .and_then(|phase| phase.max_parallelism),
            Some(4)
        );
        let subscription = merged
            .subscriptions
            .get(&subscription_id)
            .expect("subscription config should merge");
        assert_eq!(subscription.enabled, Some(false));
        assert_eq!(
            subscription.failure_policy,
            Some(HookFailurePolicy::BestEffort)
        );
        assert_eq!(
            subscription.visibility,
            Some(HookSubscriptionVisibility::Debug)
        );
    }
}
