use pioneer_client::core::{ClientPublicationReference, ClientScope};
use pioneer_desktop_foundation::{
    ClientBindingRegistrar, ClientBindingRegistration, ClientPublicationSink,
};
use std::{cell::RefCell, sync::Arc};
struct AvatarDemand {
    identity: (ClientScope, u64, u64),
    cancellation: tokio_util::sync::CancellationToken,
    _task: Option<
        gpui_kit::Task<
            Result<
                pioneer_client::avatars::AvatarCacheResult,
                pioneer_client::avatars::AvatarCacheError,
            >,
        >,
    >,
}
impl Drop for AvatarDemand {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
pub(crate) struct SettingsBinding {
    scopes: Vec<ClientScope>,
    avatar_scope: RefCell<Option<ClientScope>>,
    avatar_demand: RefCell<Option<AvatarDemand>>,
    avatar_registration: RefCell<Option<ClientBindingRegistration>>,
    revisions: RefCell<std::collections::HashMap<ClientScope, u64>>,
    registrations: RefCell<Vec<ClientBindingRegistration>>,
    pub changed: tokio::sync::watch::Sender<()>,
}
impl SettingsBinding {
    pub fn sync_avatar(
        self: &Arc<Self>,
        config: &crate::SettingsConfig,
        active: bool,
        cx: &mut gpui_kit::App,
    ) {
        self.reconcile_avatar(config, active, true, cx);
    }

    // The retained Account surface owns the fetch while its profile editor is open.
    // The editor observes the same result without superseding/cancelling that fetch.
    pub fn observe_avatar(
        self: &Arc<Self>,
        config: &crate::SettingsConfig,
        cx: &mut gpui_kit::App,
    ) {
        self.reconcile_avatar(config, true, false, cx);
    }

    fn reconcile_avatar(
        self: &Arc<Self>,
        config: &crate::SettingsConfig,
        active: bool,
        fetch: bool,
        cx: &mut gpui_kit::App,
    ) {
        use pioneer_client::{
            avatars::{AvatarCacheRequest, avatar_identity_key},
            gateway::identity_authorization::IdentityAuthorizationPublication,
        };
        let request = active
            .then(|| config.client.current_auth())
            .flatten()
            .and_then(|auth| {
                Some(AvatarCacheRequest {
                    principal_id: auth.principal.id,
                    avatar_revision: auth.principal.avatar_revision?,
                })
            });
        let scope = request.as_ref().map(|request| ClientScope::Avatar {
            principal_id: avatar_identity_key(
                request.principal_id.as_str(),
                &request.avatar_revision,
            ),
        });
        let authority = config
            .client
            .snapshot(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| p.typed::<IdentityAuthorizationPublication>())
            .map(|p| {
                (
                    p.payload().connection_generation,
                    p.payload().authorization_change_sequence,
                )
            })
            .unwrap_or_default();
        let identity = scope.clone().map(|scope| (scope, authority.0, authority.1));
        if self.avatar_demand.borrow().as_ref().map(|d| &d.identity) == identity.as_ref() {
            return;
        }
        self.avatar_demand.borrow_mut().take();
        if let Some(old_scope) = self.avatar_scope.borrow_mut().take() {
            self.revisions.borrow_mut().remove(&old_scope);
        }
        self.avatar_registration.borrow_mut().take();
        *self.avatar_scope.borrow_mut() = scope.clone();
        let sink: Arc<dyn ClientPublicationSink> = self.clone();
        *self.avatar_registration.borrow_mut() =
            scope.map(|scope| config.bindings.register(scope, Arc::downgrade(&sink)));
        if let Some((identity, request)) = identity.zip(request) {
            let cancellation = tokio_util::sync::CancellationToken::new();
            let task = (fetch && config.avatar_path(request.principal_id.as_str()).is_none())
                .then(|| config.avatars.resolve(request, cancellation.clone(), cx));
            *self.avatar_demand.borrow_mut() = Some(AvatarDemand {
                identity,
                cancellation,
                _task: task,
            });
        }
    }

    pub fn new(scopes: Vec<ClientScope>, registrar: &Arc<dyn ClientBindingRegistrar>) -> Arc<Self> {
        let binding = Arc::new(Self {
            scopes: scopes.clone(),
            avatar_scope: RefCell::default(),
            avatar_registration: RefCell::default(),
            avatar_demand: RefCell::default(),
            revisions: RefCell::default(),
            registrations: RefCell::default(),
            changed: tokio::sync::watch::channel(()).0,
        });
        let sink: Arc<dyn ClientPublicationSink> = binding.clone();
        *binding.registrations.borrow_mut() = scopes
            .into_iter()
            .map(|scope| registrar.register(scope, Arc::downgrade(&sink)))
            .collect();
        binding
    }
}
impl ClientPublicationSink for SettingsBinding {
    fn publish(&self, reference: ClientPublicationReference) {
        if !self.scopes.contains(reference.scope())
            && self.avatar_scope.borrow().as_ref() != Some(reference.scope())
        {
            return;
        }
        let revision = reference.revisions().scoped().get();
        let mut revisions = self.revisions.borrow_mut();
        if revisions
            .get(reference.scope())
            .is_some_and(|old| *old >= revision)
        {
            return;
        }
        revisions.insert(reference.scope().clone(), revision);
        self.changed.send_replace(());
    }
}
