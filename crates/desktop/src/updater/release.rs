use super::{
    manifest::{
        DESKTOP_UPDATE_MANIFEST_FILE, DesktopManifestError, DesktopUpdateManifest,
        parse_desktop_update_manifest,
    },
    state::DesktopUpdateConfig,
};
use reqwest::blocking::Client;
use serde::Deserialize;
use std::{error::Error, fmt, thread, time::Duration};

pub(crate) const DESKTOP_UPDATER_USER_AGENT: &str = "pioneer-app-updater/1.0";
const MANIFEST_READY_ATTEMPTS: usize = 5;
const MANIFEST_READY_RETRY_DELAY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DesktopReleaseChannel {
    Stable,
    Beta,
    Canary,
}

impl DesktopReleaseChannel {
    fn parse(value: &str) -> Result<Self, DesktopReleaseError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "stable" => Ok(Self::Stable),
            "beta" => Ok(Self::Beta),
            "canary" => Ok(Self::Canary),
            other => Err(DesktopReleaseError::new(
                DesktopReleaseErrorCode::UnsupportedChannel,
                format!("unsupported desktop update channel: {other}"),
            )),
        }
    }

    fn tag_suffix(self) -> Option<&'static str> {
        match self {
            Self::Stable => None,
            Self::Beta => Some("-beta"),
            Self::Canary => Some("-canary"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FetchedDesktopManifest {
    pub(crate) tag: String,
    pub(crate) manifest_url: String,
    pub(crate) manifest: DesktopUpdateManifest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DesktopReleaseErrorCode {
    UnsupportedChannel,
    ReleaseRequest,
    ReleaseStatus,
    ReleaseJson,
    EmptyTag,
    ChannelTagNotFound,
    ManifestNotPublished,
    ManifestRequest,
    ManifestStatus,
    ManifestValidation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopReleaseError {
    code: DesktopReleaseErrorCode,
    message: String,
}

impl DesktopReleaseError {
    fn new(code: DesktopReleaseErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    #[cfg(test)]
    pub(crate) fn code(&self) -> DesktopReleaseErrorCode {
        self.code
    }

    fn is_manifest_readiness_error(&self) -> bool {
        matches!(
            self.code,
            DesktopReleaseErrorCode::ManifestNotPublished
                | DesktopReleaseErrorCode::ManifestRequest
                | DesktopReleaseErrorCode::ManifestStatus
        )
    }
}

impl fmt::Display for DesktopReleaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for DesktopReleaseError {}

#[derive(Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Option<Vec<GithubReleaseAsset>>,
}

#[derive(Debug, Deserialize)]
struct GithubReleaseAsset {
    name: String,
}

pub(crate) fn fetch_desktop_update_manifest_with_client(
    client: &Client,
    config: &DesktopUpdateConfig,
) -> Result<FetchedDesktopManifest, DesktopReleaseError> {
    let mut last_error = None;

    for attempt in 0..MANIFEST_READY_ATTEMPTS {
        match fetch_desktop_update_manifest_once(client, config) {
            Ok(fetched) => return Ok(fetched),
            Err(error)
                if error.is_manifest_readiness_error() && attempt + 1 < MANIFEST_READY_ATTEMPTS =>
            {
                last_error = Some(error);
                thread::sleep(MANIFEST_READY_RETRY_DELAY);
            }
            Err(error) => return Err(error),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        DesktopReleaseError::new(
            DesktopReleaseErrorCode::ManifestNotPublished,
            "desktop update manifest was not published before retry budget expired",
        )
    }))
}

fn fetch_desktop_update_manifest_once(
    client: &Client,
    config: &DesktopUpdateConfig,
) -> Result<FetchedDesktopManifest, DesktopReleaseError> {
    let tag = resolve_release_tag_with_client(client, config)?;
    let manifest_url = release_manifest_download_url(config, tag.as_str());
    let manifest_bytes = client
        .get(manifest_url.as_str())
        .send()
        .map_err(|error| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ManifestRequest,
                format!("failed to fetch desktop update manifest from `{manifest_url}`: {error}"),
            )
        })?
        .error_for_status()
        .map_err(|error| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ManifestStatus,
                format!("desktop update manifest request failed for `{manifest_url}`: {error}"),
            )
        })?
        .bytes()
        .map_err(|error| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ManifestRequest,
                format!("failed to read desktop update manifest from `{manifest_url}`: {error}"),
            )
        })?;

    let manifest = parse_desktop_update_manifest(manifest_bytes.as_ref()).map_err(
        |error: DesktopManifestError| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ManifestValidation,
                format!("desktop update manifest validation failed: {error}"),
            )
        },
    )?;

    Ok(FetchedDesktopManifest {
        tag,
        manifest_url,
        manifest,
    })
}

pub(crate) fn resolve_release_tag_with_client(
    client: &Client,
    config: &DesktopUpdateConfig,
) -> Result<String, DesktopReleaseError> {
    match DesktopReleaseChannel::parse(config.channel.as_str())? {
        DesktopReleaseChannel::Stable => fetch_latest_release_tag(client, config),
        channel @ (DesktopReleaseChannel::Beta | DesktopReleaseChannel::Canary) => {
            fetch_channel_release_tag(client, config, channel)
        }
    }
}

fn fetch_latest_release_tag(
    client: &Client,
    config: &DesktopUpdateConfig,
) -> Result<String, DesktopReleaseError> {
    let url = latest_release_url(config.release_api_base.as_str());
    let release = client
        .get(url.as_str())
        .send()
        .map_err(|error| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ReleaseRequest,
                format!("failed to fetch latest desktop release metadata from `{url}`: {error}"),
            )
        })?
        .error_for_status()
        .map_err(|error| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ReleaseStatus,
                format!("latest desktop release request failed for `{url}`: {error}"),
            )
        })?
        .json::<GithubRelease>()
        .map_err(|error| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ReleaseJson,
                format!("failed to parse latest desktop release metadata from `{url}`: {error}"),
            )
        })?;

    ensure_manifest_asset_is_listed(&release)?;
    normalize_non_empty_tag(release.tag_name)
}

fn fetch_channel_release_tag(
    client: &Client,
    config: &DesktopUpdateConfig,
    channel: DesktopReleaseChannel,
) -> Result<String, DesktopReleaseError> {
    let url = release_list_url(config.release_api_base.as_str());
    let releases = client
        .get(url.as_str())
        .send()
        .map_err(|error| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ReleaseRequest,
                format!("failed to fetch desktop release list from `{url}`: {error}"),
            )
        })?
        .error_for_status()
        .map_err(|error| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ReleaseStatus,
                format!("desktop release list request failed for `{url}`: {error}"),
            )
        })?
        .json::<Vec<GithubRelease>>()
        .map_err(|error| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ReleaseJson,
                format!("failed to parse desktop release list from `{url}`: {error}"),
            )
        })?;
    select_channel_release_tag(channel, releases.iter())
}

fn ensure_manifest_asset_is_listed(release: &GithubRelease) -> Result<(), DesktopReleaseError> {
    if release_may_have_manifest_asset(release) {
        return Ok(());
    }

    Err(DesktopReleaseError::new(
        DesktopReleaseErrorCode::ManifestNotPublished,
        format!(
            "desktop update manifest asset `{}` is not listed on release `{}` yet",
            DESKTOP_UPDATE_MANIFEST_FILE,
            release.tag_name.trim()
        ),
    ))
}

fn release_may_have_manifest_asset(release: &GithubRelease) -> bool {
    match &release.assets {
        Some(assets) => assets
            .iter()
            .any(|asset| asset.name == DESKTOP_UPDATE_MANIFEST_FILE),
        None => true,
    }
}

fn select_channel_release_tag<'a>(
    channel: DesktopReleaseChannel,
    releases: impl IntoIterator<Item = &'a GithubRelease>,
) -> Result<String, DesktopReleaseError> {
    let Some(suffix) = channel.tag_suffix() else {
        return Err(DesktopReleaseError::new(
            DesktopReleaseErrorCode::UnsupportedChannel,
            "stable release channel must use latest release metadata",
        ));
    };

    let mut matched_unpublished_release = false;
    for release in releases {
        let tag = release.tag_name.trim();
        if tag.is_empty() || !tag.contains(suffix) {
            continue;
        }
        if release_may_have_manifest_asset(release) {
            return Ok(tag.to_owned());
        }
        matched_unpublished_release = true;
    }

    if matched_unpublished_release {
        return Err(DesktopReleaseError::new(
            DesktopReleaseErrorCode::ManifestNotPublished,
            format!(
                "desktop update manifest asset `{DESKTOP_UPDATE_MANIFEST_FILE}` is not listed on latest `{suffix}` release yet"
            ),
        ));
    }

    Err(DesktopReleaseError::new(
        DesktopReleaseErrorCode::ChannelTagNotFound,
        format!("failed to find desktop release tag for channel `{suffix}`"),
    ))
}

#[cfg(test)]
pub(crate) fn release_by_tag_api_url(config: &DesktopUpdateConfig, tag: &str) -> String {
    format!(
        "{}/tags/{tag}",
        config.release_api_base.trim_end_matches('/')
    )
}

pub(crate) fn release_manifest_download_url(config: &DesktopUpdateConfig, tag: &str) -> String {
    release_asset_download_url(config, tag, DESKTOP_UPDATE_MANIFEST_FILE)
}

pub(crate) fn release_asset_download_url(
    config: &DesktopUpdateConfig,
    tag: &str,
    asset_name: &str,
) -> String {
    format!(
        "{}/{tag}/{asset_name}",
        config.release_download_base.trim_end_matches('/')
    )
}

pub(crate) fn latest_release_url(api_base: &str) -> String {
    format!("{}/latest", api_base.trim_end_matches('/'))
}

pub(crate) fn release_list_url(api_base: &str) -> String {
    format!("{}?per_page=100", api_base.trim_end_matches('/'))
}

#[cfg(test)]
fn select_channel_tag<'a>(
    channel: DesktopReleaseChannel,
    tags: impl IntoIterator<Item = &'a str>,
) -> Result<String, DesktopReleaseError> {
    let Some(suffix) = channel.tag_suffix() else {
        return Err(DesktopReleaseError::new(
            DesktopReleaseErrorCode::UnsupportedChannel,
            "stable release channel must use latest release metadata",
        ));
    };

    tags.into_iter()
        .map(str::trim)
        .find(|tag| !tag.is_empty() && tag.contains(suffix))
        .map(str::to_owned)
        .ok_or_else(|| {
            DesktopReleaseError::new(
                DesktopReleaseErrorCode::ChannelTagNotFound,
                format!("failed to find desktop release tag for channel `{suffix}`"),
            )
        })
}

fn normalize_non_empty_tag(tag: String) -> Result<String, DesktopReleaseError> {
    let tag = tag.trim().to_owned();
    if tag.is_empty() {
        return Err(DesktopReleaseError::new(
            DesktopReleaseErrorCode::EmptyTag,
            "desktop release metadata does not include a non-empty tag_name",
        ));
    }

    Ok(tag)
}

#[cfg(test)]
mod tests {
    use super::{
        DESKTOP_UPDATE_MANIFEST_FILE, DesktopReleaseChannel, DesktopReleaseErrorCode,
        GithubRelease, GithubReleaseAsset, ensure_manifest_asset_is_listed, latest_release_url,
        release_asset_download_url, release_by_tag_api_url, release_list_url,
        release_manifest_download_url, release_may_have_manifest_asset, select_channel_release_tag,
        select_channel_tag,
    };
    use crate::updater::state::DesktopUpdateConfig;

    #[test]
    fn builds_release_urls_from_overridden_bases() {
        let config = DesktopUpdateConfig {
            disabled: false,
            force_check: false,
            channel: "stable".to_owned(),
            release_repo: "local/repo".to_owned(),
            release_api_base: "http://localhost:1111/releases/".to_owned(),
            release_download_base: "http://localhost:2222/download/".to_owned(),
        };

        assert_eq!(
            latest_release_url(config.release_api_base.as_str()),
            "http://localhost:1111/releases/latest"
        );
        assert_eq!(
            release_list_url(config.release_api_base.as_str()),
            "http://localhost:1111/releases?per_page=100"
        );
        assert_eq!(
            release_by_tag_api_url(&config, "v1.2.3"),
            "http://localhost:1111/releases/tags/v1.2.3"
        );
        assert_eq!(
            release_manifest_download_url(&config, "v1.2.3"),
            "http://localhost:2222/download/v1.2.3/desktop-update-manifest.json"
        );
        assert_eq!(
            release_asset_download_url(&config, "v1.2.3", "Pioneer-aarch64.app.zip"),
            "http://localhost:2222/download/v1.2.3/Pioneer-aarch64.app.zip"
        );
    }

    #[test]
    fn selects_first_beta_tag_from_release_list_order() {
        let tags = [
            "v1.0.0",
            "v1.1.0-canary.1",
            "v1.1.0-beta.2",
            "v1.0.1-beta.1",
        ];

        let selected = select_channel_tag(DesktopReleaseChannel::Beta, tags).unwrap();

        assert_eq!(selected, "v1.1.0-beta.2");
    }

    #[test]
    fn selects_first_canary_tag_from_release_list_order() {
        let tags = ["v1.0.0", " v1.2.0-canary.3 ", "v1.1.0-canary.1"];

        let selected = select_channel_tag(DesktopReleaseChannel::Canary, tags).unwrap();

        assert_eq!(selected, "v1.2.0-canary.3");
    }

    #[test]
    fn returns_typed_error_when_channel_tag_is_missing() {
        let error = select_channel_tag(DesktopReleaseChannel::Beta, ["v1.0.0", "v1.1.0-canary.1"])
            .unwrap_err();

        assert_eq!(error.code(), DesktopReleaseErrorCode::ChannelTagNotFound);
    }

    #[test]
    fn release_without_assets_field_is_allowed_for_local_fixtures() {
        let release = release("v1.2.3", None);

        assert!(release_may_have_manifest_asset(&release));
        assert!(ensure_manifest_asset_is_listed(&release).is_ok());
    }

    #[test]
    fn release_with_assets_requires_manifest_asset() {
        let release = release("v1.2.3", Some(vec!["Pioneer-aarch64.app.zip"]));

        let error = ensure_manifest_asset_is_listed(&release).unwrap_err();

        assert_eq!(error.code(), DesktopReleaseErrorCode::ManifestNotPublished);
    }

    #[test]
    fn release_with_manifest_asset_is_ready() {
        let release = release(
            "v1.2.3",
            Some(vec![
                "Pioneer-aarch64.app.zip",
                DESKTOP_UPDATE_MANIFEST_FILE,
            ]),
        );

        assert!(release_may_have_manifest_asset(&release));
    }

    #[test]
    fn channel_release_without_manifest_is_retryable() {
        let releases = vec![release(
            "v1.2.3-beta.1",
            Some(vec!["Pioneer-aarch64.app.zip"]),
        )];

        let error =
            select_channel_release_tag(DesktopReleaseChannel::Beta, releases.iter()).unwrap_err();

        assert_eq!(error.code(), DesktopReleaseErrorCode::ManifestNotPublished);
    }

    #[test]
    fn channel_release_selects_first_ready_channel_release() {
        let releases = vec![
            release("v1.2.3-beta.2", Some(vec!["Pioneer-aarch64.app.zip"])),
            release(
                "v1.2.3-beta.1",
                Some(vec![
                    "Pioneer-aarch64.app.zip",
                    DESKTOP_UPDATE_MANIFEST_FILE,
                ]),
            ),
        ];

        let selected =
            select_channel_release_tag(DesktopReleaseChannel::Beta, releases.iter()).unwrap();

        assert_eq!(selected, "v1.2.3-beta.1");
    }

    fn release(tag_name: &str, assets: Option<Vec<&str>>) -> GithubRelease {
        GithubRelease {
            tag_name: tag_name.to_owned(),
            assets: assets.map(|assets| {
                assets
                    .into_iter()
                    .map(|name| GithubReleaseAsset {
                        name: name.to_owned(),
                    })
                    .collect()
            }),
        }
    }
}
