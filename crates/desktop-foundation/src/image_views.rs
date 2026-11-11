//! Retained presentation leases for native imagery. Delivery, validation,
//! session fencing and disk cache remain owned by the shared Client image service.
use crate::{CatalogBinding, ClientBindingRegistrar, ImageSurface};
use gpui_kit::{
    component::{Icon, theme::ActiveTheme},
    prelude::*,
    *,
};
use pioneer_client::{
    core::{ClientCore, ClientScope},
    images::{ImageCacheRequest, ImagePublication, ImageReference, image_identity_key},
};
use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;

/// Shell adapter to the shared authenticated image resolver.
pub trait ImageViewPort {
    fn resolve(
        &self,
        icon: ImageReference,
        cancellation: CancellationToken,
        cx: &mut App,
    ) -> Task<()>;
}

/// Process wiring and weak view identities only; image bytes and caches live in Client/GPUI.
pub struct ImageViewConfig {
    client: Arc<ClientCore>,
    registrar: Arc<dyn ClientBindingRegistrar>,
    port: Arc<dyn ImageViewPort>,
    views: HashMap<String, WeakEntity<ImageViewState>>,
}
impl Global for ImageViewConfig {}
impl ImageViewConfig {
    pub fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        port: Arc<dyn ImageViewPort>,
    ) -> Self {
        Self {
            client,
            registrar,
            port,
            views: HashMap::new(),
        }
    }
    fn image(icon: &ImageReference, cx: &mut App) -> Option<Entity<ImageViewState>> {
        let request = ImageCacheRequest::new(icon.clone()).ok()?;
        let key = request.key();
        if !cx.has_global::<Self>() {
            return None;
        }
        cx.update_global::<Self, _>(|config, cx| {
            config.views.retain(|_, view| view.upgrade().is_some());
            if let Some(view) = config.views.get(&key).and_then(WeakEntity::upgrade) {
                return Some(view);
            }
            let view = ImageViewState::new(
                config.client.clone(),
                config.registrar.clone(),
                config.port.clone(),
                request,
                cx,
            );
            config.views.insert(key, view.downgrade());
            Some(view)
        })
    }
}

/// Caller-owned visible image views, reconciled with inputs outside render.
#[derive(Clone, Default)]
pub struct ImageViews {
    views: HashMap<String, Entity<ImageView>>,
    placeholder_path: Option<SharedString>,
}
impl ImageViews {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn placeholder(mut self, path: impl Into<SharedString>) -> Self {
        self.placeholder_path = Some(path.into());
        self
    }
    pub fn reconcile(&mut self, icons: impl IntoIterator<Item = ImageReference>, cx: &mut App) {
        let icons: Vec<_> = icons
            .into_iter()
            .map(|image| {
                let key = image_identity_key(&image.path, &image.revision);
                (key, image)
            })
            .collect();
        self.views
            .retain(|key, _| icons.iter().any(|(wanted, _)| wanted == key));
        for (key, image) in icons {
            if self.views.contains_key(&key) {
                continue;
            }
            if let Some(state) = ImageViewConfig::image(&image, cx) {
                let view = cx.new(|cx| {
                    let changed = cx.observe(&state, |_, _, cx| cx.notify());
                    ImageView {
                        state,
                        placeholder_path: self.placeholder_path.clone(),
                        _changed: changed,
                    }
                });
                self.views.insert(key, view);
            }
        }
    }
    pub fn element(&self, icon: Option<&ImageReference>) -> Div {
        div().size_4().flex_none().map(|surface| {
            match icon.and_then(|icon| {
                self.views
                    .get(&image_identity_key(&icon.path, &icon.revision))
            }) {
                Some(view) => surface.child(view.clone()),
                None => surface.when_some(self.placeholder_path.clone(), |surface, path| {
                    surface.child(Icon::default().path(path).size_full().opacity(0.8))
                }),
            }
        })
    }
}

type ImageEpoch = (u64, Option<u64>, u64, bool);
struct ImageViewState {
    client: Arc<ClientCore>,
    request_identity: ImageCacheRequest,
    image_key: String,
    _binding: Arc<CatalogBinding>,
    port: Arc<dyn ImageViewPort>,
    epoch: Option<ImageEpoch>,
    cancellation: CancellationToken,
    request: Option<Task<()>>,
    _publications: Task<()>,
}
impl ImageViewState {
    fn epoch(&self) -> ImageEpoch {
        let policy = self.client.snapshot(&ClientScope::Administration { workspace_id: None })
            .and_then(|publication| publication.snapshot().payload::<pioneer_client::gateway::identity_authorization::IdentityAuthorizationPublication>())
            .map_or(0, |identity| identity.authorization_change_sequence);
        let session = self.client.gateway_session();
        let connected = session.status.as_ref().is_some_and(|status| {
            status.connection_state
                == pioneer_client::state::client_state::GatewayConnectionState::Connected
        });
        (
            self.client.authorization_connection_generation(),
            session.startup.connection_id,
            policy,
            connected,
        )
    }
    fn new(
        client: Arc<ClientCore>,
        registrar: Arc<dyn ClientBindingRegistrar>,
        port: Arc<dyn ImageViewPort>,
        request_identity: ImageCacheRequest,
        cx: &mut App,
    ) -> Entity<Self> {
        cx.new(|cx| {
            let image_key = request_identity.key();
            let binding = CatalogBinding::new(registrar);
            let mut changed = binding.watch();
            binding.set_scopes(&[
                ClientScope::Image {
                    key: image_key.clone(),
                },
                ClientScope::Session,
                ClientScope::Administration { workspace_id: None },
            ]);
            let publications = cx.spawn(async move |view: WeakEntity<Self>, cx| {
                while changed.changed().await.is_ok() {
                    if view.update(cx, |view, cx| view.synchronize(cx)).is_err() {
                        break;
                    }
                }
            });
            let mut view = Self {
                client,
                request_identity,
                image_key,
                _binding: binding,
                port,
                epoch: None,
                cancellation: CancellationToken::new(),
                request: None,
                _publications: publications,
            };
            view.synchronize(cx);
            view
        })
    }
    fn synchronize(&mut self, cx: &mut Context<Self>) {
        let epoch = self.epoch();
        if self.epoch.as_ref() != Some(&epoch) {
            self.cancellation.cancel();
            self.request = None;
            self.epoch = Some(epoch);
            self.cancellation = CancellationToken::new();
            if epoch.3 {
                self.request = Some(self.port.resolve(
                    self.request_identity.reference(),
                    self.cancellation.clone(),
                    cx,
                ));
            }
        }
        cx.notify();
    }
}
impl Drop for ImageViewState {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
impl ImageViewState {
    fn source(&self) -> Option<std::path::PathBuf> {
        (self.epoch.as_ref() == Some(&self.epoch()))
            .then(|| {
                self.client
                    .snapshot(&ClientScope::Image {
                        key: self.image_key.clone(),
                    })
                    .and_then(|publication| publication.snapshot().payload::<ImagePublication>())
                    .filter(|publication| {
                        publication.revision() == self.request_identity.revision()
                            && publication.resource_id() == self.request_identity.resource_id()
                    })
                    .and_then(|publication| {
                        publication
                            .local_path()
                            .map(|path| path.as_path().to_owned())
                    })
            })
            .flatten()
    }
}

struct ImageView {
    state: Entity<ImageViewState>,
    placeholder_path: Option<SharedString>,
    _changed: Subscription,
}
impl Render for ImageView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let source = self.state.read(cx).source();
        ImageSurface::new()
            .size_full()
            .when_some(self.placeholder_path.clone(), |image, path| {
                image.placeholder(Icon::default().path(path).size_full().opacity(0.8))
            })
            .text_color(cx.theme().foreground)
            .when_some(source, |image, path| image.source(path))
    }
}
