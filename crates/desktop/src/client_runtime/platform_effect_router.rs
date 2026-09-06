use std::sync::Arc;

use pioneer_client::{
    core::{
        ClientCore, ClientEffectCompletion, ClientEffectPlan, ClientEffectResult,
        ClientPlannedEffect,
    },
    gateway::session_refresh::{GatewaySessionStorage, GatewaySessionStorageEffect},
};

/// Dispatches native work and returns its typed completion to the originating core.
/// The process coordinator owns the adapter and its worker lifetime.
pub(super) struct DesktopPlatformEffectRouter;

impl DesktopPlatformEffectRouter {
    pub(super) fn dispatch(
        &self,
        core: &ClientCore,
        plan: ClientEffectPlan,
        storage: &dyn GatewaySessionStorage,
    ) {
        let result = match plan.effect() {
            ClientPlannedEffect::GatewaySessionStorage(
                GatewaySessionStorageEffect::ReadGatewaySession { endpoint },
            ) => storage
                .load(endpoint)
                .map(|envelope| ClientEffectResult::GatewaySessionEnvelopeLoaded { envelope }),
            ClientPlannedEffect::GatewaySessionStorage(
                GatewaySessionStorageEffect::PersistGatewaySession { endpoint, envelope },
            ) => storage
                .persist(endpoint, envelope)
                .map(|()| ClientEffectResult::Completed),
            _ => return,
        }
        .unwrap_or_else(|_| ClientEffectResult::Failed {
            code: "secure_storage_failed".into(),
        });
        core.complete_effect(ClientEffectCompletion::new(
            plan.operation_id().clone(),
            plan.generation(),
            result,
        ));
    }
}

pub(super) struct DesktopSessionStorageAdapter {
    worker: Option<std::thread::JoinHandle<()>>,
    core: std::sync::Weak<ClientCore>,
}

impl DesktopSessionStorageAdapter {
    pub(super) fn start(core: Arc<ClientCore>, router: &Arc<DesktopPlatformEffectRouter>) -> Self {
        let owner = Arc::downgrade(&core);
        let router = Arc::downgrade(router);
        let worker = std::thread::Builder::new()
            .name("desktop-session-storage".into())
            .spawn(move || {
                let mut sequence = Default::default();
                let mut secrets: Option<crate::gateway::DesktopSecrets> = None;
                while !core.is_stopped() {
                    let batch = core.wait_for_publications(sequence);
                    sequence = batch.sequence;
                    for plan in batch.effects {
                        if core.is_stopped() {
                            break;
                        }
                        let Some(router) = router.upgrade() else {
                            break;
                        };
                        if secrets.is_none() {
                            secrets = pioneer_config::AppConfig::load()
                                .map_err(anyhow::Error::from)
                                .and_then(|config| config.runtime_home_dir())
                                .and_then(|home| crate::gateway::DesktopSecrets::open(&home))
                                .ok();
                        }
                        match secrets.as_ref() {
                            Some(secrets) => router.dispatch(&core, plan, secrets),
                            None => {
                                core.complete_effect(ClientEffectCompletion::new(
                                    plan.operation_id().clone(),
                                    plan.generation(),
                                    ClientEffectResult::Failed {
                                        code: "secure_storage_failed".into(),
                                    },
                                ));
                            }
                        }
                    }
                }
            })
            .expect("desktop session storage worker could not be started");
        Self {
            worker: Some(worker),
            core: owner,
        }
    }

    pub(super) fn join(mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for DesktopSessionStorageAdapter {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            core.shutdown();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::{
        core::{ClientChangeSequence, ClientTransitionOutcome},
        gateway::{
            endpoint::GatewayBaseUrl,
            session_envelope::GatewaySessionEnvelope,
            session_refresh::GatewaySessionPlatformStorage,
            types::{GatewayEndpoint, GatewayEndpointKind},
        },
    };

    struct Storage(std::sync::atomic::AtomicUsize);
    impl GatewaySessionStorage for Storage {
        fn load(&self, _: &GatewayEndpoint) -> anyhow::Result<Option<GatewaySessionEnvelope>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(None)
        }
        fn persist(&self, _: &GatewayEndpoint, _: &GatewaySessionEnvelope) -> anyhow::Result<()> {
            panic!("read must not dispatch a write");
        }
    }

    #[test]
    fn platform_storage_completion_returns_to_originating_core_without_publication() {
        let core = Arc::new(ClientCore::new());
        let other_process = ClientCore::new();
        let worker_core = core.clone();
        let worker = std::thread::spawn(move || {
            GatewaySessionPlatformStorage(&worker_core).load(&GatewayEndpoint {
                id: "synthetic".into(),
                name: "Synthetic".into(),
                gateway_base_url: GatewayBaseUrl::parse_presentation("https://gateway.invalid")
                    .unwrap(),
                kind: GatewayEndpointKind::Remote,
                session_ref: Some("synthetic".into()),
                server_gateway_id: None,
                workspace_id: None,
                service_name: None,
            })
        });
        let plan = (0..20)
            .find_map(|_| {
                let batch = core.wait_for_publications(ClientChangeSequence::ZERO);
                assert!(batch.changes.is_empty());
                batch.effects.into_iter().next()
            })
            .expect("storage effect was not delivered");
        assert_eq!(
            other_process
                .complete_effect(ClientEffectCompletion::new(
                    plan.operation_id().clone(),
                    plan.generation(),
                    ClientEffectResult::GatewaySessionEnvelopeLoaded { envelope: None },
                ))
                .outcome(),
            ClientTransitionOutcome::Rejected
        );
        let storage = Storage(std::sync::atomic::AtomicUsize::new(0));
        DesktopPlatformEffectRouter.dispatch(&core, plan, &storage);
        assert!(worker.join().unwrap().unwrap().is_none());
        assert_eq!(storage.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        core.shutdown();
    }
}
