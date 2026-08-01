//! Process-free Desktop fixtures and Gateway contract checks.

use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use super::{VersionProbe, version_probe_from_args};

#[test]
fn version_probe_does_not_require_window_startup() {
    assert_eq!(
        version_probe_from_args(["--version".to_owned()]),
        Some(VersionProbe::Plain)
    );
    assert_eq!(
        version_probe_from_args(["-V".to_owned()]),
        Some(VersionProbe::Plain)
    );
    assert_eq!(
        version_probe_from_args(["--version".to_owned(), "--json".to_owned()]),
        Some(VersionProbe::Json)
    );
    assert_eq!(
        version_probe_from_args(["--json".to_owned(), "-V".to_owned()]),
        Some(VersionProbe::Json)
    );
    assert_eq!(version_probe_from_args(["--help".to_owned()]), None);
    assert_eq!(version_probe_from_args(Vec::<String>::new()), None);
}

#[derive(Debug, Default)]
pub(crate) struct FakeSystemViewerLauncher {
    opened_urls: Mutex<Vec<String>>,
    fail_next: AtomicBool,
}

impl FakeSystemViewerLauncher {
    pub(crate) fn fail_next_open(&self) {
        self.fail_next.store(true, Ordering::SeqCst);
    }

    pub(crate) fn open_url(&self, url: &str) -> anyhow::Result<()> {
        let url = url.trim();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            anyhow::bail!("system viewer fixture accepts only absolute HTTP(S) URLs");
        }
        if self.fail_next.swap(false, Ordering::SeqCst) {
            anyhow::bail!("injected system viewer failure");
        }
        self.opened_urls
            .lock()
            .expect("fake system viewer lock")
            .push(url.to_owned());
        Ok(())
    }

    pub(crate) fn opened_urls(&self) -> Vec<String> {
        self.opened_urls
            .lock()
            .expect("fake system viewer lock")
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use pioneer_client::{
        artifacts::http_download::ArtifactHttpDownloadRequest,
        avatars::AvatarCacheRequest,
        gateway::endpoint::{
            GatewayBaseUrl, PIONEER_PROTOCOL_VERSION_HEADER,
        },
        transport::{
            http::BrowserViewUrl,
            ws::rpc::build_ws_request,
        },
    };
    use pioneer_protocol::PrincipalId;

    use super::*;

    #[test]
    fn fake_viewer_records_without_starting_an_os_process() {
        let launcher = FakeSystemViewerLauncher::default();
        launcher
            .open_url("https://gateway.test/storage/views/redacted")
            .expect("record URL");
        assert_eq!(
            launcher.opened_urls(),
            vec!["https://gateway.test/storage/views/redacted".to_owned()]
        );

        launcher.fail_next_open();
        assert!(
            launcher
                .open_url("https://gateway.test/storage/views/redacted")
                .is_err()
        );
        assert!(launcher.open_url("file:///tmp/private").is_err());
    }

    #[test]
    fn updated_desktop_plan_is_root_storage_native_and_secret_free() {
        let base = GatewayBaseUrl::parse_presentation("https://gateway.test/pioneer/")
            .expect("canonical custom-prefix base");
        let websocket = build_ws_request(&base, Some("test-access-secret"))
            .expect("canonical WebSocket request");
        assert_eq!(websocket.uri().to_string(), "wss://gateway.test/pioneer/");
        assert_eq!(
            websocket
                .headers()
                .get(PIONEER_PROTOCOL_VERSION_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );

        let grant = "a".repeat(43);
        let view = BrowserViewUrl::resolve(&base, format!("/storage/views/{grant}").as_str())
            .expect("same-origin view");
        let launcher = FakeSystemViewerLauncher::default();
        launcher.open_url(view.expose_url()).expect("record view intent");
        assert_eq!(
            launcher.opened_urls(),
            vec![format!("https://gateway.test/pioneer/storage/views/{grant}")]
        );

        let download = ArtifactHttpDownloadRequest {
            gateway_profile_id: "gateway-test".to_owned(),
            workspace_id: "workspace-test".to_owned(),
            artifact_id: "artifact-test".to_owned(),
            version_id: "version-test".to_owned(),
            display_name: "report.txt".to_owned(),
            expected_size_bytes: 64,
            expected_sha256: "a".repeat(64),
        };
        let avatar = AvatarCacheRequest {
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            avatar_revision: "b".repeat(64),
        };
        let boundary = format!(
            "{} {}",
            serde_json::to_string(&download).unwrap(),
            serde_json::to_string(&avatar).unwrap()
        )
        .to_ascii_lowercase();
        for forbidden in ["authorization", "access_token", "content_base64", "data:image"] {
            assert!(!boundary.contains(forbidden));
        }
    }
}
