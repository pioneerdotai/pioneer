use crate::{
    HookContribution, HookExecutionPolicy, HookFailurePolicy, HookFilterKey, HookId, HookMetadata,
    HookPhase, HookRetryPolicy, HookSubscriptionId, HookValue,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub type HookFilterSet = BTreeMap<HookFilterKey, HookValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HookSubscriptionVisibility {
    #[default]
    Internal,
    Debug,
    UserAudit,
    UserVisible,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct HookSubscriptionDependencies {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<HookSubscriptionId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub before: Vec<HookSubscriptionId>,
}

impl HookSubscriptionDependencies {
    pub fn new(
        after: impl IntoIterator<Item = HookSubscriptionId>,
        before: impl IntoIterator<Item = HookSubscriptionId>,
    ) -> Self {
        Self {
            after: sorted_unique_ids(after),
            before: sorted_unique_ids(before),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookSubscription {
    pub subscription_id: HookSubscriptionId,
    pub hook_id: HookId,
    pub phase: HookPhase,
    pub enabled: bool,
    pub priority: i32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub filters: HookFilterSet,
    pub dependencies: HookSubscriptionDependencies,
    pub execution_policy: HookExecutionPolicy,
    pub failure_policy: HookFailurePolicy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fallback_contributions: Vec<HookContribution>,
    pub retry_policy: HookRetryPolicy,
    pub visibility: HookSubscriptionVisibility,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: HookMetadata,
}

impl HookSubscription {
    pub fn new(subscription_id: HookSubscriptionId, hook_id: HookId, phase: HookPhase) -> Self {
        Self {
            subscription_id,
            hook_id,
            phase,
            enabled: true,
            priority: 0,
            filters: BTreeMap::new(),
            dependencies: HookSubscriptionDependencies::default(),
            execution_policy: HookExecutionPolicy::default(),
            failure_policy: HookFailurePolicy::BestEffort,
            fallback_contributions: Vec::new(),
            retry_policy: HookRetryPolicy::default(),
            visibility: HookSubscriptionVisibility::Internal,
            metadata: HookMetadata::default(),
        }
    }

    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_filters(mut self, filters: HookFilterSet) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_dependencies(mut self, dependencies: HookSubscriptionDependencies) -> Self {
        self.dependencies = dependencies;
        self
    }

    pub fn with_execution_policy(mut self, execution_policy: HookExecutionPolicy) -> Self {
        self.execution_policy = execution_policy;
        self
    }

    pub fn with_failure_policy(mut self, failure_policy: HookFailurePolicy) -> Self {
        self.failure_policy = failure_policy;
        self
    }

    pub fn with_fallback_contributions(
        mut self,
        fallback_contributions: impl IntoIterator<Item = HookContribution>,
    ) -> Self {
        self.fallback_contributions = fallback_contributions.into_iter().collect();
        self
    }

    pub fn with_retry_policy(mut self, retry_policy: HookRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn with_visibility(mut self, visibility: HookSubscriptionVisibility) -> Self {
        self.visibility = visibility;
        self
    }

    pub fn with_metadata(mut self, metadata: HookMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

fn sorted_unique_ids(ids: impl IntoIterator<Item = HookSubscriptionId>) -> Vec<HookSubscriptionId> {
    ids.into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hook_id() -> HookId {
        HookId::new("test.handler").expect("valid hook id")
    }

    fn subscription_id(value: &str) -> HookSubscriptionId {
        HookSubscriptionId::new(value).expect("valid subscription id")
    }

    #[test]
    fn subscription_defaults_are_conservative() {
        let subscription = HookSubscription::new(
            subscription_id("sub.default"),
            hook_id(),
            HookPhase::TurnPrePolicy,
        );

        assert!(subscription.enabled);
        assert_eq!(subscription.priority, 0);
        assert!(subscription.filters.is_empty());
        assert!(subscription.fallback_contributions.is_empty());
        assert_eq!(subscription.failure_policy, HookFailurePolicy::BestEffort);
        assert_eq!(
            subscription.visibility,
            HookSubscriptionVisibility::Internal
        );
        assert_eq!(subscription.retry_policy, HookRetryPolicy::default());
    }

    #[test]
    fn subscription_visibility_serializes_stably() {
        assert_eq!(
            serde_json::to_value(HookSubscriptionVisibility::Debug).expect("visibility serializes"),
            serde_json::json!("debug")
        );
        assert_eq!(
            serde_json::to_value(HookSubscriptionVisibility::UserAudit)
                .expect("visibility serializes"),
            serde_json::json!("user_audit")
        );
        assert_eq!(
            serde_json::to_value(HookSubscriptionVisibility::UserVisible)
                .expect("visibility serializes"),
            serde_json::json!("user_visible")
        );
    }

    #[test]
    fn subscription_dependencies_roundtrip_and_sort() {
        let dependencies = HookSubscriptionDependencies::new(
            [
                subscription_id("sub.b"),
                subscription_id("sub.a"),
                subscription_id("sub.a"),
            ],
            [subscription_id("sub.c")],
        );

        assert_eq!(
            dependencies.after,
            vec![subscription_id("sub.a"), subscription_id("sub.b")]
        );

        let value = serde_json::to_value(&dependencies).expect("dependencies serialize");
        let decoded: HookSubscriptionDependencies =
            serde_json::from_value(value).expect("dependencies deserialize");
        assert_eq!(decoded, dependencies);
    }

    #[test]
    fn subscription_fallback_contributions_roundtrip() {
        let subscription = HookSubscription::new(
            subscription_id("sub.fallback"),
            hook_id(),
            HookPhase::TurnPrePolicy,
        )
        .with_failure_policy(HookFailurePolicy::Fallback)
        .with_fallback_contributions([HookContribution::Noop]);

        let value = serde_json::to_value(&subscription).expect("subscription serializes");
        let decoded: HookSubscription =
            serde_json::from_value(value).expect("subscription deserializes");
        assert_eq!(decoded.fallback_contributions, vec![HookContribution::Noop]);
    }
}
