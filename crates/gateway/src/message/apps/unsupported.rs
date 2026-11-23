//! Intel macOS has no local Microsandbox backend. Never start a worker or SDK setup.
use super::super::MessageProcessor;
use pioneer_protocol::{APPS_UNSUPPORTED_HOST_PLATFORM, AuthSecretString};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default, serde::Serialize)]
pub(crate) struct RuntimeStatus {
    pub state: String,
    pub failure: Option<String>,
    pub execution_id: Option<String>,
}

#[derive(Default)]
pub(crate) struct AppRuntime;

impl AppRuntime {
    pub(crate) async fn status(&self, _: &str) -> RuntimeStatus {
        RuntimeStatus {
            state: "unavailable".into(),
            failure: Some(APPS_UNSUPPORTED_HOST_PLATFORM.into()),
            execution_id: None,
        }
    }

    pub(crate) async fn logs(&self, _: &str) -> Vec<String> {
        Vec::new()
    }

    pub(crate) async fn upstream(
        &self,
        _: &str,
    ) -> Option<(std::net::SocketAddr, AuthSecretString, CancellationToken)> {
        None
    }
}

impl MessageProcessor {
    pub(crate) async fn start_app_runtimes(&self) {}
    pub(crate) async fn shutdown_app_runtimes(&self) {}

    pub(in crate::message) async fn stop_app_runtime(&self, app: &str) -> anyhow::Result<()> {
        self.stop_dev_view(app).await;
        self.host_api.fence_app(app);
        Ok(())
    }
}
