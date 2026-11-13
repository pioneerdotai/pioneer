//! Uses the shared native image service as member and provider imagery.
use gpui_kit::{App, AppContext, Task};
use pioneer_client::{
    core::ClientCore,
    images::{ImageCacheRequest, ImageReference},
};
use pioneer_desktop_foundation::ImageViewPort;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) struct DesktopImageViews(pub(crate) Arc<ClientCore>);
impl ImageViewPort for DesktopImageViews {
    fn resolve(
        &self,
        icon: ImageReference,
        cancellation: CancellationToken,
        cx: &mut App,
    ) -> Task<()> {
        let client = self.0.clone();
        cx.background_spawn(async move {
            let Ok(home) = crate::state::runtime_home_dir() else {
                return;
            };
            let Ok(service) = client.image_cache_service(home) else {
                return;
            };
            let Ok(runtime) = tokio::runtime::Runtime::new() else {
                return;
            };
            let Ok(request) = ImageCacheRequest::new(icon) else {
                return;
            };
            let _ = runtime.block_on(client.resolve_image(&service, request, cancellation));
        })
    }
}
