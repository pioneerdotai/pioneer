use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pioneer_agent::AgentManagerHealthSnapshot;
use pioneer_protocol::{
    GatewayNativeLifecycleReadinessReport, GatewayReadinessComponent,
    GatewayReadinessComponentSnapshot, GatewayReadinessComponentState, GatewayReadinessStatus,
};

use super::MessageProcessor;

const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const STALE_RUNNING_TURN_SECS: u64 = 5 * 60;
const MAX_RECOVERY_BACKLOG: u64 = 100;
const MAX_RECOVERY_AGE_SECS: u64 = 5 * 60;
const MAX_TERMINAL_EFFECT_BACKLOG: u64 = 64;
const MAX_TERMINAL_EFFECT_AGE_SECS: u64 = 5 * 60;

#[derive(Debug)]
pub(super) struct NativeLifecycleReadinessState {
    generation: AtomicU64,
    signature: AtomicU64,
}

impl Default for NativeLifecycleReadinessState {
    fn default() -> Self {
        Self {
            generation: AtomicU64::new(1),
            signature: AtomicU64::new(u64::MAX),
        }
    }
}

impl NativeLifecycleReadinessState {
    fn observe(&self, signature: u64) -> u64 {
        let previous = self.signature.swap(signature, Ordering::AcqRel);
        if previous != signature {
            self.generation.fetch_add(1, Ordering::AcqRel) + 1
        } else {
            self.generation.load(Ordering::Acquire)
        }
    }
}

fn component(
    component: GatewayReadinessComponent,
    state: GatewayReadinessComponentState,
    generation: u64,
    active: u64,
    pending: u64,
    oldest_pending_age_secs: Option<u64>,
    reason_code: Option<&'static str>,
) -> GatewayReadinessComponentSnapshot {
    GatewayReadinessComponentSnapshot {
        component,
        state,
        generation,
        active,
        pending,
        oldest_pending_age_secs,
        reason_code: reason_code.map(str::to_owned),
    }
}

fn state_code(state: GatewayReadinessComponentState) -> u64 {
    match state {
        GatewayReadinessComponentState::Starting => 0,
        GatewayReadinessComponentState::Healthy => 1,
        GatewayReadinessComponentState::Degraded => 2,
        GatewayReadinessComponentState::Unhealthy => 3,
    }
}

fn metric_component(
    component: GatewayReadinessComponent,
) -> pioneer_observability::NativeReadinessComponent {
    match component {
        GatewayReadinessComponent::Database => {
            pioneer_observability::NativeReadinessComponent::Database
        }
        GatewayReadinessComponent::NativeAgentManager => {
            pioneer_observability::NativeReadinessComponent::NativeAgentManager
        }
        GatewayReadinessComponent::DurableListeners => {
            pioneer_observability::NativeReadinessComponent::DurableListeners
        }
        GatewayReadinessComponent::RecoveryCoordinator => {
            pioneer_observability::NativeReadinessComponent::RecoveryCoordinator
        }
        GatewayReadinessComponent::Terminalization => {
            pioneer_observability::NativeReadinessComponent::Terminalization
        }
        GatewayReadinessComponent::ProviderRegistry => {
            pioneer_observability::NativeReadinessComponent::ProviderRegistry
        }
    }
}

fn metric_state(
    state: GatewayReadinessComponentState,
) -> pioneer_observability::NativeReadinessState {
    match state {
        GatewayReadinessComponentState::Starting => {
            pioneer_observability::NativeReadinessState::Starting
        }
        GatewayReadinessComponentState::Healthy => {
            pioneer_observability::NativeReadinessState::Healthy
        }
        GatewayReadinessComponentState::Degraded => {
            pioneer_observability::NativeReadinessState::Degraded
        }
        GatewayReadinessComponentState::Unhealthy => {
            pioneer_observability::NativeReadinessState::Unhealthy
        }
    }
}

fn signature(components: &[GatewayReadinessComponentSnapshot]) -> u64 {
    components
        .iter()
        .enumerate()
        .fold(0xcbf2_9ce4_8422_2325_u64, |value, (index, component)| {
            let state = state_code(component.state);
            value
                .wrapping_mul(0x0000_0100_0000_01b3)
                .wrapping_add(u64::try_from(index).unwrap_or(u64::MAX))
                ^ state
                ^ component.generation.rotate_left(17)
        })
}

fn aggregate_native_lifecycle_readiness(
    base_status: GatewayReadinessStatus,
    generation: u64,
    checked_at_unix: i64,
    mut components: Vec<GatewayReadinessComponentSnapshot>,
) -> GatewayNativeLifecycleReadinessReport {
    let critical_failure = components.iter().any(|component| {
        component.component != GatewayReadinessComponent::ProviderRegistry
            && !matches!(component.state, GatewayReadinessComponentState::Healthy)
    });
    let any_degradation = components
        .iter()
        .any(|component| !matches!(component.state, GatewayReadinessComponentState::Healthy));
    let accepting_new_turns = base_status.accepts_sessions() && !critical_failure;
    let status = if matches!(base_status, GatewayReadinessStatus::Starting) {
        GatewayReadinessStatus::Starting
    } else if any_degradation {
        GatewayReadinessStatus::Degraded
    } else {
        base_status
    };
    for component in &mut components {
        if component.generation == 0 {
            component.generation = generation;
        }
    }
    GatewayNativeLifecycleReadinessReport {
        status,
        accepting_new_turns,
        generation,
        checked_at_unix,
        components,
    }
}

fn durable_listener_component(
    agent: AgentManagerHealthSnapshot,
    listener_count: u64,
    dead_listeners: u64,
) -> GatewayReadinessComponentSnapshot {
    let cardinality_mismatch = listener_count.abs_diff(agent.registered_actors);
    if agent.durable_listener_gaps > 0 || dead_listeners > 0 || cardinality_mismatch > 0 {
        let reason = if dead_listeners > 0 {
            "listener_stopped"
        } else if listener_count > agent.registered_actors {
            "stale_listener_generation"
        } else {
            "durable_lane_unclaimed"
        };
        return component(
            GatewayReadinessComponent::DurableListeners,
            GatewayReadinessComponentState::Unhealthy,
            agent.highest_actor_generation,
            listener_count.saturating_sub(dead_listeners),
            agent
                .durable_listener_gaps
                .max(dead_listeners)
                .max(cardinality_mismatch),
            None,
            Some(reason),
        );
    }
    component(
        GatewayReadinessComponent::DurableListeners,
        GatewayReadinessComponentState::Healthy,
        agent.highest_actor_generation,
        listener_count,
        0,
        None,
        None,
    )
}

fn terminalization_component(
    snapshot: pioneer_crud::NativeLifecycleDurableHealthSnapshot,
) -> GatewayReadinessComponentSnapshot {
    let effects = snapshot.terminal_effects;
    let pending = effects
        .prepared
        .saturating_add(effects.waiting_acceptance)
        .saturating_add(effects.ready)
        .saturating_add(effects.running)
        .saturating_add(effects.retry_wait);
    if effects.unresolved > 0 {
        component(
            GatewayReadinessComponent::Terminalization,
            GatewayReadinessComponentState::Unhealthy,
            0,
            effects.running,
            pending.saturating_add(effects.unresolved),
            snapshot.oldest_terminal_effect_age_secs,
            Some("terminal_effect_unresolved"),
        )
    } else if pending > MAX_TERMINAL_EFFECT_BACKLOG
        || snapshot
            .oldest_terminal_effect_age_secs
            .is_some_and(|age| age > MAX_TERMINAL_EFFECT_AGE_SECS)
    {
        component(
            GatewayReadinessComponent::Terminalization,
            GatewayReadinessComponentState::Degraded,
            0,
            effects.running,
            pending,
            snapshot.oldest_terminal_effect_age_secs,
            Some("terminal_effect_backlog"),
        )
    } else {
        component(
            GatewayReadinessComponent::Terminalization,
            GatewayReadinessComponentState::Healthy,
            0,
            effects.running,
            pending,
            snapshot.oldest_terminal_effect_age_secs,
            None,
        )
    }
}

impl MessageProcessor {
    pub(crate) async fn native_lifecycle_readiness_report(
        &self,
        base_status: GatewayReadinessStatus,
    ) -> GatewayNativeLifecycleReadinessReport {
        let checked_at_unix = chrono::Utc::now().timestamp();
        let (db_probe, durable, agent, listener_snapshot, resilience_worker) = tokio::join!(
            tokio::time::timeout(
                PROBE_TIMEOUT,
                self.crud_store.native_lifecycle_read_write_probe(),
            ),
            tokio::time::timeout(
                PROBE_TIMEOUT,
                self.crud_store.native_lifecycle_durable_health_snapshot(
                    checked_at_unix,
                    STALE_RUNNING_TURN_SECS,
                ),
            ),
            tokio::time::timeout(PROBE_TIMEOUT, self.agent_manager.health_snapshot()),
            tokio::time::timeout(PROBE_TIMEOUT, async {
                let listeners = self.agent_listener_tasks.lock().await;
                (
                    u64::try_from(listeners.len()).unwrap_or(u64::MAX),
                    u64::try_from(
                        listeners
                            .values()
                            .filter(|listener| listener.handle.is_finished())
                            .count(),
                    )
                    .unwrap_or(u64::MAX),
                )
            }),
            tokio::time::timeout(PROBE_TIMEOUT, async {
                let worker = self.resilience_worker.lock().await;
                worker.as_ref().map(|worker| worker.is_finished())
            }),
        );

        let mut components = Vec::with_capacity(6);
        let database_state = match (&db_probe, &durable) {
            (Ok(Ok(())), Ok(Ok(_))) => component(
                GatewayReadinessComponent::Database,
                GatewayReadinessComponentState::Healthy,
                0,
                1,
                0,
                None,
                None,
            ),
            (Err(_), _) | (_, Err(_)) => component(
                GatewayReadinessComponent::Database,
                GatewayReadinessComponentState::Unhealthy,
                0,
                0,
                1,
                None,
                Some("probe_timeout"),
            ),
            _ => component(
                GatewayReadinessComponent::Database,
                GatewayReadinessComponentState::Unhealthy,
                0,
                0,
                1,
                None,
                Some("probe_failed"),
            ),
        };
        components.push(database_state);

        let agent_snapshot = agent.ok();
        components.push(match agent_snapshot {
            Some(snapshot) if snapshot.dead_actors > 0 => component(
                GatewayReadinessComponent::NativeAgentManager,
                GatewayReadinessComponentState::Unhealthy,
                snapshot
                    .highest_actor_generation
                    .max(snapshot.runtime_generation),
                snapshot.active_turns,
                snapshot.dead_actors,
                None,
                Some("actor_dead"),
            ),
            Some(snapshot) => component(
                GatewayReadinessComponent::NativeAgentManager,
                GatewayReadinessComponentState::Healthy,
                snapshot
                    .highest_actor_generation
                    .max(snapshot.runtime_generation),
                snapshot.active_turns,
                0,
                None,
                None,
            ),
            None => component(
                GatewayReadinessComponent::NativeAgentManager,
                GatewayReadinessComponentState::Unhealthy,
                0,
                0,
                1,
                None,
                Some("supervisor_timeout"),
            ),
        });

        components.push(match (agent_snapshot, listener_snapshot) {
            (Some(agent), Ok((listener_count, dead_listeners))) => {
                durable_listener_component(agent, listener_count, dead_listeners)
            }
            _ => component(
                GatewayReadinessComponent::DurableListeners,
                GatewayReadinessComponentState::Unhealthy,
                0,
                0,
                1,
                None,
                Some("listener_probe_timeout"),
            ),
        });

        let durable_snapshot = durable.ok().and_then(Result::ok);
        let recovery = match (resilience_worker, durable_snapshot) {
            (Ok(Some(false)), Some(snapshot))
                if snapshot.active_recovery_jobs > MAX_RECOVERY_BACKLOG
                    || snapshot
                        .oldest_recovery_age_secs
                        .is_some_and(|age| age > MAX_RECOVERY_AGE_SECS) =>
            {
                component(
                    GatewayReadinessComponent::RecoveryCoordinator,
                    GatewayReadinessComponentState::Degraded,
                    0,
                    1,
                    snapshot.active_recovery_jobs,
                    snapshot.oldest_recovery_age_secs,
                    Some("recovery_backlog"),
                )
            }
            (Ok(Some(false)), Some(snapshot)) => component(
                GatewayReadinessComponent::RecoveryCoordinator,
                GatewayReadinessComponentState::Healthy,
                0,
                1,
                snapshot.active_recovery_jobs,
                snapshot.oldest_recovery_age_secs,
                None,
            ),
            (Err(_), _) => component(
                GatewayReadinessComponent::RecoveryCoordinator,
                GatewayReadinessComponentState::Unhealthy,
                0,
                0,
                1,
                None,
                Some("worker_probe_timeout"),
            ),
            _ => component(
                GatewayReadinessComponent::RecoveryCoordinator,
                GatewayReadinessComponentState::Unhealthy,
                0,
                0,
                1,
                None,
                Some("worker_stopped"),
            ),
        };
        components.push(recovery);

        components.push(match durable_snapshot {
            Some(snapshot) => terminalization_component(snapshot),
            None => component(
                GatewayReadinessComponent::Terminalization,
                GatewayReadinessComponentState::Unhealthy,
                0,
                0,
                1,
                None,
                Some("outbox_probe_failed"),
            ),
        });

        let registry = self.provider_registry.stats();
        let registry_capacity_exceeded = registry.cached_instances > registry.max_cached_instances
            || registry.injected_providers > registry.max_injected_providers;
        components.push(component(
            GatewayReadinessComponent::ProviderRegistry,
            if registry_capacity_exceeded {
                GatewayReadinessComponentState::Unhealthy
            } else {
                GatewayReadinessComponentState::Healthy
            },
            1,
            u64::try_from(
                registry
                    .cached_instances
                    .saturating_add(registry.injected_providers),
            )
            .unwrap_or(u64::MAX),
            0,
            None,
            registry_capacity_exceeded.then_some("registry_capacity_exceeded"),
        ));

        let observed_signature = signature(components.as_slice());
        let generation = self.native_lifecycle_readiness.observe(observed_signature);
        let report = aggregate_native_lifecycle_readiness(
            base_status,
            generation,
            checked_at_unix,
            components,
        );
        for component in &report.components {
            pioneer_observability::record_native_readiness_component(
                metric_component(component.component),
                metric_state(component.state),
            );
        }
        if let Some(snapshot) = durable_snapshot {
            use pioneer_observability::NativeLifecycleDepthKind;
            let effects = snapshot.terminal_effects;
            pioneer_observability::record_native_lifecycle_depth(
                NativeLifecycleDepthKind::ActiveTurns,
                snapshot.active_turns,
            );
            pioneer_observability::record_native_lifecycle_depth(
                NativeLifecycleDepthKind::StaleRunningTurns,
                snapshot.stale_running_turns,
            );
            pioneer_observability::record_native_lifecycle_depth(
                NativeLifecycleDepthKind::RecoveryBacklog,
                snapshot.active_recovery_jobs,
            );
            pioneer_observability::record_native_lifecycle_depth(
                NativeLifecycleDepthKind::TerminalBacklog,
                effects
                    .prepared
                    .saturating_add(effects.waiting_acceptance)
                    .saturating_add(effects.ready)
                    .saturating_add(effects.running)
                    .saturating_add(effects.retry_wait),
            );
            pioneer_observability::record_native_lifecycle_depth(
                NativeLifecycleDepthKind::UnresolvedTerminalEffects,
                effects.unresolved,
            );
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy(component_kind: GatewayReadinessComponent) -> GatewayReadinessComponentSnapshot {
        component(
            component_kind,
            GatewayReadinessComponentState::Healthy,
            1,
            0,
            0,
            None,
            None,
        )
    }

    fn healthy_components() -> Vec<GatewayReadinessComponentSnapshot> {
        vec![
            healthy(GatewayReadinessComponent::Database),
            healthy(GatewayReadinessComponent::NativeAgentManager),
            healthy(GatewayReadinessComponent::DurableListeners),
            healthy(GatewayReadinessComponent::RecoveryCoordinator),
            healthy(GatewayReadinessComponent::Terminalization),
            healthy(GatewayReadinessComponent::ProviderRegistry),
        ]
    }

    #[test]
    fn listener_generation_cardinality_mismatch_fails_readiness_closed() {
        let agent = AgentManagerHealthSnapshot {
            runtime_generation: 7,
            highest_actor_generation: 11,
            registered_actors: 1,
            active_turns: 0,
            dead_actors: 0,
            durable_listener_gaps: 0,
        };
        let stale = durable_listener_component(agent, 2, 0);
        assert_eq!(stale.state, GatewayReadinessComponentState::Unhealthy);
        assert_eq!(
            stale.reason_code.as_deref(),
            Some("stale_listener_generation")
        );
        let report = aggregate_native_lifecycle_readiness(
            GatewayReadinessStatus::Operational,
            1,
            1,
            vec![healthy(GatewayReadinessComponent::Database), stale],
        );
        assert!(!report.accepting_new_turns);

        let missing = durable_listener_component(agent, 0, 0);
        assert_eq!(missing.state, GatewayReadinessComponentState::Unhealthy);
        assert_eq!(
            missing.reason_code.as_deref(),
            Some("durable_lane_unclaimed")
        );
        assert_eq!(
            durable_listener_component(agent, 1, 0).state,
            GatewayReadinessComponentState::Healthy
        );
    }

    #[test]
    fn stale_running_turn_age_alone_does_not_fail_readiness() {
        let terminalization =
            terminalization_component(pioneer_crud::NativeLifecycleDurableHealthSnapshot {
                stale_running_turns: 1,
                ..Default::default()
            });
        assert_eq!(
            terminalization.state,
            GatewayReadinessComponentState::Healthy,
            "wall-clock age without an ownership or causal failure is an alert signal, not an admission fence"
        );
        let report = aggregate_native_lifecycle_readiness(
            GatewayReadinessStatus::Operational,
            1,
            1,
            vec![
                healthy(GatewayReadinessComponent::Database),
                terminalization,
            ],
        );
        assert!(report.accepting_new_turns);
    }

    #[test]
    fn critical_faults_fail_readiness_closed_and_repair_advances_generation() {
        let state = NativeLifecycleReadinessState::default();
        let healthy = healthy_components();
        let healthy_generation = state.observe(signature(healthy.as_slice()));
        let healthy_report = aggregate_native_lifecycle_readiness(
            GatewayReadinessStatus::Operational,
            healthy_generation,
            1,
            healthy.clone(),
        );
        assert!(healthy_report.accepting_new_turns);

        for (component_kind, reason) in [
            (GatewayReadinessComponent::Database, "probe_timeout"),
            (GatewayReadinessComponent::NativeAgentManager, "actor_dead"),
            (
                GatewayReadinessComponent::DurableListeners,
                "listener_stopped",
            ),
            (
                GatewayReadinessComponent::RecoveryCoordinator,
                "recovery_backlog",
            ),
            (
                GatewayReadinessComponent::Terminalization,
                "terminal_effect_unresolved",
            ),
        ] {
            let mut failed = healthy.clone();
            let target = failed
                .iter_mut()
                .find(|component| component.component == component_kind)
                .expect("component exists");
            target.state = GatewayReadinessComponentState::Unhealthy;
            target.reason_code = Some(reason.to_owned());
            let failed_generation = state.observe(signature(failed.as_slice()));
            let failed_report = aggregate_native_lifecycle_readiness(
                GatewayReadinessStatus::Operational,
                failed_generation,
                2,
                failed,
            );
            assert!(!failed_report.accepting_new_turns, "fault {reason}");
            assert_eq!(failed_report.status, GatewayReadinessStatus::Degraded);

            let repaired_generation = state.observe(signature(healthy.as_slice()));
            let repaired_report = aggregate_native_lifecycle_readiness(
                GatewayReadinessStatus::Operational,
                repaired_generation,
                3,
                healthy.clone(),
            );
            assert!(repaired_report.accepting_new_turns);
            assert!(repaired_generation > failed_generation);
        }
    }

    #[test]
    fn workspace_provider_outage_is_reported_without_cross_tenant_global_admission_failure() {
        let mut components = healthy_components();
        let provider = components
            .iter_mut()
            .find(|component| component.component == GatewayReadinessComponent::ProviderRegistry)
            .expect("provider component exists");
        provider.state = GatewayReadinessComponentState::Degraded;
        provider.reason_code = Some("workspace_provider_outage".to_owned());

        let report = aggregate_native_lifecycle_readiness(
            GatewayReadinessStatus::Operational,
            2,
            1,
            components,
        );
        assert_eq!(report.status, GatewayReadinessStatus::Degraded);
        assert!(report.accepting_new_turns);
    }

    #[test]
    fn replacement_actor_generation_invalidates_readiness_canary() {
        let state = NativeLifecycleReadinessState::default();
        let original = healthy_components();
        let original_generation = state.observe(signature(original.as_slice()));

        let mut replacement = original;
        replacement
            .iter_mut()
            .find(|component| component.component == GatewayReadinessComponent::NativeAgentManager)
            .expect("agent component exists")
            .generation = 2;
        let replacement_generation = state.observe(signature(replacement.as_slice()));

        assert!(replacement_generation > original_generation);
    }
}
