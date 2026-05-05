use crate::handler::normalized_phases;
use crate::{
    HookHandler, HookHandlerDescriptor, HookId, HookPhase, HookRegistryError, HookSubscription,
    HookSubscriptionId,
};
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

#[derive(Default)]
pub struct HookRegistry {
    handlers: RwLock<BTreeMap<HookId, Arc<dyn HookHandler>>>,
}

impl HookRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_handler(&self, handler: Arc<dyn HookHandler>) -> Result<(), HookRegistryError> {
        let descriptor = HookHandlerDescriptor::from_handler(handler.as_ref());
        let hook_id = descriptor.hook_id.clone();
        if descriptor.supported_phases.is_empty() {
            return Err(HookRegistryError::EmptySupportedPhases(hook_id));
        }

        let mut handlers = self
            .handlers
            .write()
            .map_err(|_| HookRegistryError::LockPoisoned("hook_handlers"))?;
        if handlers.contains_key(&hook_id) {
            return Err(HookRegistryError::DuplicateHandlerId(hook_id));
        }
        handlers.insert(hook_id, handler);
        Ok(())
    }

    pub fn get_handler(
        &self,
        hook_id: &HookId,
    ) -> Result<Option<Arc<dyn HookHandler>>, HookRegistryError> {
        let handlers = self
            .handlers
            .read()
            .map_err(|_| HookRegistryError::LockPoisoned("hook_handlers"))?;
        Ok(handlers.get(hook_id).cloned())
    }

    pub fn contains_handler(&self, hook_id: &HookId) -> Result<bool, HookRegistryError> {
        let handlers = self
            .handlers
            .read()
            .map_err(|_| HookRegistryError::LockPoisoned("hook_handlers"))?;
        Ok(handlers.contains_key(hook_id))
    }

    pub fn descriptor(
        &self,
        hook_id: &HookId,
    ) -> Result<Option<HookHandlerDescriptor>, HookRegistryError> {
        let handlers = self
            .handlers
            .read()
            .map_err(|_| HookRegistryError::LockPoisoned("hook_handlers"))?;
        Ok(handlers
            .get(hook_id)
            .map(|handler| HookHandlerDescriptor::from_handler(handler.as_ref())))
    }

    pub fn descriptors(&self) -> Result<Vec<HookHandlerDescriptor>, HookRegistryError> {
        let handlers = self
            .handlers
            .read()
            .map_err(|_| HookRegistryError::LockPoisoned("hook_handlers"))?;
        Ok(handlers
            .values()
            .map(|handler| HookHandlerDescriptor::from_handler(handler.as_ref()))
            .collect())
    }
}

#[derive(Default)]
pub struct HookSubscriptionRegistry {
    subscriptions: RwLock<BTreeMap<HookSubscriptionId, HookSubscription>>,
}

impl HookSubscriptionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_subscription(
        &self,
        handlers: &HookRegistry,
        subscription: HookSubscription,
    ) -> Result<(), HookRegistryError> {
        let subscription_id = subscription.subscription_id.clone();
        {
            let subscriptions = self
                .subscriptions
                .read()
                .map_err(|_| HookRegistryError::LockPoisoned("hook_subscriptions"))?;
            if subscriptions.contains_key(&subscription_id) {
                return Err(HookRegistryError::DuplicateSubscriptionId(subscription_id));
            }
        }

        let handler = handlers
            .get_handler(&subscription.hook_id)?
            .ok_or_else(|| HookRegistryError::MissingHandler(subscription.hook_id.clone()))?;
        let supported_phases = normalized_phases(handler.supported_phases());
        if !supported_phases.contains(&subscription.phase) {
            return Err(HookRegistryError::UnsupportedPhase {
                hook_id: subscription.hook_id.clone(),
                phase: subscription.phase,
            });
        }

        let mut subscriptions = self
            .subscriptions
            .write()
            .map_err(|_| HookRegistryError::LockPoisoned("hook_subscriptions"))?;
        if subscriptions.contains_key(&subscription_id) {
            return Err(HookRegistryError::DuplicateSubscriptionId(subscription_id));
        }
        subscriptions.insert(subscription_id, subscription);
        Ok(())
    }

    pub fn disable_subscription(
        &self,
        subscription_id: &HookSubscriptionId,
    ) -> Result<Option<HookSubscription>, HookRegistryError> {
        let mut subscriptions = self
            .subscriptions
            .write()
            .map_err(|_| HookRegistryError::LockPoisoned("hook_subscriptions"))?;
        let Some(subscription) = subscriptions.get_mut(subscription_id) else {
            return Ok(None);
        };
        subscription.enabled = false;
        Ok(Some(subscription.clone()))
    }

    pub fn get_subscription(
        &self,
        subscription_id: &HookSubscriptionId,
    ) -> Result<Option<HookSubscription>, HookRegistryError> {
        let subscriptions = self
            .subscriptions
            .read()
            .map_err(|_| HookRegistryError::LockPoisoned("hook_subscriptions"))?;
        Ok(subscriptions.get(subscription_id).cloned())
    }

    pub fn subscriptions_for_phase(
        &self,
        phase: HookPhase,
    ) -> Result<Vec<HookSubscription>, HookRegistryError> {
        self.subscriptions_for_phase_inner(phase, false)
    }

    pub fn all_subscriptions_for_phase(
        &self,
        phase: HookPhase,
    ) -> Result<Vec<HookSubscription>, HookRegistryError> {
        self.subscriptions_for_phase_inner(phase, true)
    }

    fn subscriptions_for_phase_inner(
        &self,
        phase: HookPhase,
        include_disabled: bool,
    ) -> Result<Vec<HookSubscription>, HookRegistryError> {
        let subscriptions = self
            .subscriptions
            .read()
            .map_err(|_| HookRegistryError::LockPoisoned("hook_subscriptions"))?;
        let mut matching = subscriptions
            .values()
            .filter(|subscription| subscription.phase == phase)
            .filter(|subscription| include_disabled || subscription.enabled)
            .cloned()
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| {
            left.priority
                .cmp(&right.priority)
                .then_with(|| left.subscription_id.cmp(&right.subscription_id))
        });
        Ok(matching)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HookDiagnosticCode, HookDiagnosticMessage, HookError, HookHandlerRequest,
        HookHandlerResponse, HookKind, HookResult,
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestHookHandler {
        id: HookId,
        kind: HookKind,
        phases: Vec<HookPhase>,
        execute_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HookHandler for TestHookHandler {
        fn id(&self) -> HookId {
            self.id.clone()
        }

        fn kind(&self) -> HookKind {
            self.kind.clone()
        }

        fn supported_phases(&self) -> Vec<HookPhase> {
            self.phases.clone()
        }

        async fn execute(&self, _request: HookHandlerRequest) -> HookResult<HookHandlerResponse> {
            self.execute_count.fetch_add(1, Ordering::SeqCst);
            Err(HookError::new(
                HookDiagnosticCode::new("unexpected.execute").expect("valid code"),
                HookDiagnosticMessage::new("registry must not execute handlers")
                    .expect("valid message"),
            ))
        }
    }

    fn hook_id(value: &str) -> HookId {
        HookId::new(value).expect("valid hook id")
    }

    fn subscription_id(value: &str) -> HookSubscriptionId {
        HookSubscriptionId::new(value).expect("valid subscription id")
    }

    fn handler(
        id: &str,
        phases: Vec<HookPhase>,
        execute_count: Arc<AtomicUsize>,
    ) -> Arc<dyn HookHandler> {
        Arc::new(TestHookHandler {
            id: hook_id(id),
            kind: HookKind::new("test").expect("valid hook kind"),
            phases,
            execute_count,
        })
    }

    fn registry_with_handler(id: &str, phases: Vec<HookPhase>) -> (HookRegistry, Arc<AtomicUsize>) {
        let registry = HookRegistry::new();
        let execute_count = Arc::new(AtomicUsize::new(0));
        registry
            .register_handler(handler(id, phases, execute_count.clone()))
            .expect("handler registers");
        (registry, execute_count)
    }

    fn subscription(id: &str, hook_id: &str, phase: HookPhase) -> HookSubscription {
        HookSubscription::new(subscription_id(id), self::hook_id(hook_id), phase)
    }

    #[test]
    fn register_handler_stores_descriptor() {
        let registry = HookRegistry::new();
        registry
            .register_handler(handler(
                "test.handler",
                vec![HookPhase::TurnPrePolicy],
                Arc::new(AtomicUsize::new(0)),
            ))
            .expect("handler registers");

        let descriptor = registry
            .descriptor(&hook_id("test.handler"))
            .expect("descriptor lookup succeeds")
            .expect("descriptor exists");

        assert_eq!(descriptor.hook_id, hook_id("test.handler"));
        assert_eq!(
            descriptor.hook_kind,
            HookKind::new("test").expect("valid kind")
        );
        assert_eq!(descriptor.supported_phases, vec![HookPhase::TurnPrePolicy]);
        assert!(
            registry
                .contains_handler(&hook_id("test.handler"))
                .expect("contains succeeds")
        );
    }

    #[test]
    fn duplicate_handler_id_is_rejected() {
        let registry = HookRegistry::new();
        let execute_count = Arc::new(AtomicUsize::new(0));
        registry
            .register_handler(handler(
                "test.handler",
                vec![HookPhase::TurnPrePolicy],
                execute_count.clone(),
            ))
            .expect("handler registers");

        let error = registry
            .register_handler(handler(
                "test.handler",
                vec![HookPhase::TurnPostTurn],
                execute_count,
            ))
            .expect_err("duplicate handler rejected");

        assert!(
            matches!(error, HookRegistryError::DuplicateHandlerId(id) if id == hook_id("test.handler"))
        );
    }

    #[test]
    fn handler_with_empty_supported_phases_is_rejected() {
        let registry = HookRegistry::new();
        let error = registry
            .register_handler(handler(
                "test.empty",
                Vec::new(),
                Arc::new(AtomicUsize::new(0)),
            ))
            .expect_err("empty phases rejected");

        assert!(
            matches!(error, HookRegistryError::EmptySupportedPhases(id) if id == hook_id("test.empty"))
        );
    }

    #[test]
    fn register_subscription_stores_subscription() {
        let (handlers, _) = registry_with_handler("test.handler", vec![HookPhase::TurnPrePolicy]);
        let subscriptions = HookSubscriptionRegistry::new();
        let subscription = subscription("sub.one", "test.handler", HookPhase::TurnPrePolicy);

        subscriptions
            .register_subscription(&handlers, subscription.clone())
            .expect("subscription registers");

        assert_eq!(
            subscriptions
                .get_subscription(&subscription_id("sub.one"))
                .expect("subscription lookup succeeds"),
            Some(subscription)
        );
    }

    #[test]
    fn duplicate_subscription_id_is_rejected() {
        let (handlers, _) = registry_with_handler("test.handler", vec![HookPhase::TurnPrePolicy]);
        let subscriptions = HookSubscriptionRegistry::new();
        subscriptions
            .register_subscription(
                &handlers,
                subscription("sub.one", "test.handler", HookPhase::TurnPrePolicy),
            )
            .expect("subscription registers");

        let error = subscriptions
            .register_subscription(
                &handlers,
                subscription("sub.one", "test.handler", HookPhase::TurnPrePolicy),
            )
            .expect_err("duplicate subscription rejected");

        assert!(matches!(
            error,
            HookRegistryError::DuplicateSubscriptionId(id) if id == subscription_id("sub.one")
        ));
    }

    #[test]
    fn subscription_referencing_missing_handler_is_rejected() {
        let handlers = HookRegistry::new();
        let subscriptions = HookSubscriptionRegistry::new();
        let error = subscriptions
            .register_subscription(
                &handlers,
                subscription("sub.one", "missing.handler", HookPhase::TurnPrePolicy),
            )
            .expect_err("missing handler rejected");

        assert!(matches!(
            error,
            HookRegistryError::MissingHandler(id) if id == hook_id("missing.handler")
        ));
    }

    #[test]
    fn subscription_for_unsupported_phase_is_rejected() {
        let (handlers, _) = registry_with_handler("test.handler", vec![HookPhase::TurnPrePolicy]);
        let subscriptions = HookSubscriptionRegistry::new();
        let error = subscriptions
            .register_subscription(
                &handlers,
                subscription("sub.one", "test.handler", HookPhase::TurnPostTurn),
            )
            .expect_err("unsupported phase rejected");

        assert!(matches!(
            error,
            HookRegistryError::UnsupportedPhase { hook_id, phase }
                if hook_id == self::hook_id("test.handler") && phase == HookPhase::TurnPostTurn
        ));
    }

    #[test]
    fn disable_subscription_excludes_it_from_enabled_phase_lookup() {
        let (handlers, _) = registry_with_handler("test.handler", vec![HookPhase::TurnPrePolicy]);
        let subscriptions = HookSubscriptionRegistry::new();
        subscriptions
            .register_subscription(
                &handlers,
                subscription("sub.one", "test.handler", HookPhase::TurnPrePolicy),
            )
            .expect("subscription registers");

        let disabled = subscriptions
            .disable_subscription(&subscription_id("sub.one"))
            .expect("disable succeeds")
            .expect("subscription exists");

        assert!(!disabled.enabled);
        assert!(
            subscriptions
                .subscriptions_for_phase(HookPhase::TurnPrePolicy)
                .expect("phase lookup succeeds")
                .is_empty()
        );
    }

    #[test]
    fn disable_unknown_subscription_returns_none() {
        let subscriptions = HookSubscriptionRegistry::new();

        assert_eq!(
            subscriptions
                .disable_subscription(&subscription_id("missing.sub"))
                .expect("disable succeeds"),
            None
        );
    }

    #[test]
    fn subscriptions_for_phase_are_sorted_by_priority_then_id() {
        let (handlers, _) = registry_with_handler("test.handler", vec![HookPhase::TurnPrePolicy]);
        let subscriptions = HookSubscriptionRegistry::new();
        for subscription in [
            subscription("sub.c", "test.handler", HookPhase::TurnPrePolicy).with_priority(20),
            subscription("sub.b", "test.handler", HookPhase::TurnPrePolicy).with_priority(10),
            subscription("sub.a", "test.handler", HookPhase::TurnPrePolicy).with_priority(10),
        ] {
            subscriptions
                .register_subscription(&handlers, subscription)
                .expect("subscription registers");
        }

        let ids = subscriptions
            .subscriptions_for_phase(HookPhase::TurnPrePolicy)
            .expect("phase lookup succeeds")
            .into_iter()
            .map(|subscription| subscription.subscription_id)
            .collect::<Vec<_>>();

        assert_eq!(
            ids,
            vec![
                subscription_id("sub.a"),
                subscription_id("sub.b"),
                subscription_id("sub.c")
            ]
        );
    }

    #[test]
    fn all_subscriptions_for_phase_includes_disabled_subscriptions() {
        let (handlers, _) = registry_with_handler("test.handler", vec![HookPhase::TurnPrePolicy]);
        let subscriptions = HookSubscriptionRegistry::new();
        subscriptions
            .register_subscription(
                &handlers,
                subscription("sub.one", "test.handler", HookPhase::TurnPrePolicy),
            )
            .expect("subscription registers");
        subscriptions
            .disable_subscription(&subscription_id("sub.one"))
            .expect("disable succeeds");

        assert_eq!(
            subscriptions
                .all_subscriptions_for_phase(HookPhase::TurnPrePolicy)
                .expect("phase lookup succeeds")
                .len(),
            1
        );
    }

    #[test]
    fn registry_does_not_execute_handlers_during_registration_or_lookup() {
        let (handlers, execute_count) =
            registry_with_handler("test.handler", vec![HookPhase::TurnPrePolicy]);
        let subscriptions = HookSubscriptionRegistry::new();
        subscriptions
            .register_subscription(
                &handlers,
                subscription("sub.one", "test.handler", HookPhase::TurnPrePolicy),
            )
            .expect("subscription registers");
        let _ = handlers
            .descriptor(&hook_id("test.handler"))
            .expect("descriptor lookup succeeds");
        let _ = subscriptions
            .subscriptions_for_phase(HookPhase::TurnPrePolicy)
            .expect("phase lookup succeeds");

        assert_eq!(execute_count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn handler_descriptors_are_sorted_by_hook_id() {
        let registry = HookRegistry::new();
        for id in ["test.c", "test.a", "test.b"] {
            registry
                .register_handler(handler(
                    id,
                    vec![HookPhase::TurnPrePolicy],
                    Arc::new(AtomicUsize::new(0)),
                ))
                .expect("handler registers");
        }

        let ids = registry
            .descriptors()
            .expect("descriptors lookup succeeds")
            .into_iter()
            .map(|descriptor| descriptor.hook_id)
            .collect::<Vec<_>>();

        assert_eq!(
            ids,
            vec![hook_id("test.a"), hook_id("test.b"), hook_id("test.c")]
        );
    }
}
