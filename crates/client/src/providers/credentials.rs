//! Transient credential reads belong to an explicitly retained form operation.
use crate::core::ClientCore;
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak, mpsc},
};
#[derive(Clone)]
pub enum ProviderCredentialTarget {
    Api(String),
    Runtime(String),
}
pub struct ProviderCredentialLease {
    _token: Arc<()>,
}
pub struct ProviderCredentialRead {
    receiver: mpsc::Receiver<anyhow::Result<Option<String>>>,
    demand: Weak<()>,
}
impl ProviderCredentialRead {
    pub fn wait(self) -> anyhow::Result<Option<String>> {
        loop {
            anyhow::ensure!(self.demand.strong_count() > 0, "provider_form_closed");
            match self
                .receiver
                .recv_timeout(std::time::Duration::from_millis(100))
            {
                Ok(value) => return value,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => anyhow::bail!("provider_credential_unavailable"),
            }
        }
    }
}
pub(super) struct CredentialRequest {
    identity: u64,
    epoch: (u64, u64, Option<u64>),
    workspace: String,
    target: ProviderCredentialTarget,
    reply: mpsc::SyncSender<anyhow::Result<Option<String>>>,
}
#[derive(Default)]
pub(super) struct CredentialRequests {
    generation: u64,
    active: BTreeMap<u64, Weak<()>>,
    sender: Option<mpsc::SyncSender<CredentialRequest>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl CredentialRequests {
    pub(super) fn invalidate(&mut self) {
        self.active.clear();
    }
    pub(super) fn stop(&mut self) {
        self.active.clear();
        self.sender.take();
    }
}
impl Drop for CredentialRequests {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
impl ClientCore {
    pub fn read_provider_credential(
        self: &Arc<Self>,
        workspace: String,
        target: ProviderCredentialTarget,
    ) -> anyhow::Result<(ProviderCredentialLease, ProviderCredentialRead)> {
        let allowed = self
            .authorization_snapshot(Some(&workspace), None)
            .or_else(|| self.authorization_snapshot(None, None))
            .as_ref()
            .is_some_and(|p| {
                crate::authorization::principal_presentation_capabilities(p).can_manage_capabilities
            });
        anyhow::ensure!(
            allowed && !self.is_stopped(),
            "provider_credential_forbidden"
        );
        let epoch = self.provider_runtime_epoch();
        let mut controller = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        let owner = &mut controller.credentials;
        owner.active.retain(|_, demand| demand.strong_count() > 0);
        anyhow::ensure!(owner.active.len() < 64, "provider_credential_overloaded");
        let token = Arc::new(());
        let demand = Arc::downgrade(&token);
        owner.generation = owner
            .generation
            .checked_add(1)
            .expect("provider credential identity exhausted");
        let identity = owner.generation;
        let (reply, receiver) = mpsc::sync_channel(1);
        owner.active.insert(identity, demand.clone());
        let request = CredentialRequest {
            identity,
            epoch,
            workspace,
            target,
            reply,
        };
        if !owner
            .sender
            .as_ref()
            .is_some_and(|sender| sender.try_send(request).is_ok())
        {
            owner.active.remove(&identity);
            anyhow::bail!("provider_credential_unavailable");
        }
        Ok((
            ProviderCredentialLease { _token: token },
            ProviderCredentialRead { receiver, demand },
        ))
    }
    fn provider_credential_current(&self, request: &CredentialRequest) -> bool {
        !self.is_stopped()
            && self.provider_runtime_epoch() == request.epoch
            && self
                .provider_controller
                .lock()
                .expect("provider controller poisoned")
                .credentials
                .active
                .get(&request.identity)
                .is_some_and(|token| token.strong_count() > 0)
    }
    pub(crate) fn start_provider_credential_controller(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let (sender, receiver) = mpsc::sync_channel::<CredentialRequest>(64);
        let task = std::thread::Builder::new()
            .name("client-provider-credentials".into())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if !core.provider_credential_current(&request) {
                        continue;
                    }
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    let result = match &request.target {
                        ProviderCredentialTarget::Api(provider) => sender
                            .provider_list(super::list::provider_list_params(&request.workspace))
                            .and_then(|r| {
                                r.providers
                                    .into_iter()
                                    .find(|p| &p.name == provider)
                                    .map(|p| p.proxy_url)
                                    .ok_or_else(|| {
                                        anyhow::anyhow!("provider_credential_unavailable")
                                    })
                            }),
                        ProviderCredentialTarget::Runtime(runtime) => sender
                            .cli_runtime_list(super::list::cli_runtime_list_params(
                                &request.workspace,
                            ))
                            .and_then(|r| {
                                r.runtimes
                                    .into_iter()
                                    .find(|p| &p.runtime_id == runtime)
                                    .map(|p| p.proxy_url)
                                    .ok_or_else(|| {
                                        anyhow::anyhow!("provider_credential_unavailable")
                                    })
                            }),
                    }
                    .map_err(|_| anyhow::anyhow!("provider_credential_unavailable"));
                    if let Some(core) = weak.upgrade() {
                        if core.provider_credential_current(&request) {
                            let _ = request.reply.try_send(result);
                        }
                        core.provider_controller
                            .lock()
                            .expect("provider controller poisoned")
                            .credentials
                            .active
                            .remove(&request.identity);
                    }
                }
            })
            .expect("provider credential worker");
        let mut owner = self
            .provider_controller
            .lock()
            .expect("provider controller poisoned");
        owner.credentials.sender = Some(sender);
        owner.credentials.task = Some(task);
    }
}
/// Preserve an ordinary proxy verbatim, but never publish URI credentials.
pub(super) fn public_proxy(value: Option<String>) -> Option<String> {
    value.map(|value| {
        let Ok(mut parsed) = url::Url::parse(&value) else {
            return String::new();
        };
        if parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.query().is_none()
            && parsed.fragment().is_none()
        {
            return value;
        }
        let _ = parsed.set_username("");
        let _ = parsed.set_password(None);
        parsed.set_query(None);
        parsed.set_fragment(None);
        parsed.to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proxy_publication_preserves_noncredential_configuration_and_removes_uri_secrets() {
        let ordinary = "http://127.0.0.1:8080";
        assert_eq!(
            public_proxy(Some(ordinary.into())).as_deref(),
            Some(ordinary)
        );
        assert_eq!(public_proxy(Some("http://synthetic-user:synthetic-password@127.0.0.1:8080/?token=synthetic-token#private".into())).as_deref(), Some("http://127.0.0.1:8080/"));
        assert_eq!(public_proxy(None), None);
        assert_eq!(
            public_proxy(Some("synthetic-invalid-credential".into())),
            Some(String::new())
        );
    }
    #[test]
    fn a_closed_form_does_not_wait_for_or_receive_a_credential_result() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let token = Arc::new(());
        let read = ProviderCredentialRead {
            receiver,
            demand: Arc::downgrade(&token),
        };
        sender
            .send(Ok(Some("synthetic-transient-input".into())))
            .unwrap();
        drop(token);
        assert!(read.wait().is_err());
    }
}
