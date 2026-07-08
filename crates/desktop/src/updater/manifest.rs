use serde::Deserialize;
use std::{collections::HashSet, error::Error, fmt};

pub(crate) const DESKTOP_UPDATE_MANIFEST_FILE: &str = "desktop-update-manifest.json";
pub(crate) const DESKTOP_UPDATE_PRODUCT: &str = "pioneer-desktop";
pub(crate) const DESKTOP_UPDATE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopUpdateManifest {
    pub(crate) schema_version: u32,
    pub(crate) product: String,
    pub(crate) version: String,
    pub(crate) tag: String,
    pub(crate) channel: String,
    pub(crate) published_at: String,
    pub(crate) assets: Vec<DesktopUpdateManifestAsset>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopUpdateManifestAsset {
    pub(crate) os: String,
    pub(crate) arch: String,
    pub(crate) kind: String,
    pub(crate) name: String,
    pub(crate) sha256: String,
    pub(crate) size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DesktopManifestErrorCode {
    InvalidJson,
    MissingField,
    WrongSchemaVersion,
    WrongProduct,
    EmptyField,
    InvalidSha256,
    DuplicateAsset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DesktopManifestError {
    code: DesktopManifestErrorCode,
    message: String,
}

impl DesktopManifestError {
    fn new(code: DesktopManifestErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub(crate) fn code(&self) -> DesktopManifestErrorCode {
        self.code
    }
}

impl fmt::Display for DesktopManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for DesktopManifestError {}

pub(crate) fn parse_desktop_update_manifest(
    bytes: &[u8],
) -> Result<DesktopUpdateManifest, DesktopManifestError> {
    let raw: RawDesktopUpdateManifest = serde_json::from_slice(bytes).map_err(|error| {
        DesktopManifestError::new(
            DesktopManifestErrorCode::InvalidJson,
            format!("invalid desktop update manifest JSON: {error}"),
        )
    })?;

    DesktopUpdateManifest::try_from(raw)
}

impl TryFrom<RawDesktopUpdateManifest> for DesktopUpdateManifest {
    type Error = DesktopManifestError;

    fn try_from(raw: RawDesktopUpdateManifest) -> Result<Self, Self::Error> {
        let schema_version = required_u32("schema_version", raw.schema_version)?;
        if schema_version != DESKTOP_UPDATE_SCHEMA_VERSION {
            return Err(DesktopManifestError::new(
                DesktopManifestErrorCode::WrongSchemaVersion,
                format!("unsupported desktop update manifest schema_version: {schema_version}"),
            ));
        }

        let product = required_string("product", raw.product)?;
        if product != DESKTOP_UPDATE_PRODUCT {
            return Err(DesktopManifestError::new(
                DesktopManifestErrorCode::WrongProduct,
                format!("unexpected desktop update manifest product: {product}"),
            ));
        }

        let assets = required_vec("assets", raw.assets)?
            .into_iter()
            .enumerate()
            .map(|(index, asset)| DesktopUpdateManifestAsset::try_from_raw(index, asset))
            .collect::<Result<Vec<_>, _>>()?;

        reject_duplicate_assets(&assets)?;

        Ok(Self {
            schema_version,
            product,
            version: required_string("version", raw.version)?,
            tag: required_string("tag", raw.tag)?,
            channel: required_string("channel", raw.channel)?,
            published_at: required_string("published_at", raw.published_at)?,
            assets,
        })
    }
}

impl DesktopUpdateManifestAsset {
    fn try_from_raw(
        index: usize,
        raw: RawDesktopUpdateManifestAsset,
    ) -> Result<Self, DesktopManifestError> {
        let sha256 = required_string(asset_field(index, "sha256"), raw.sha256)?;
        if !is_lowercase_sha256_hex(&sha256) {
            return Err(DesktopManifestError::new(
                DesktopManifestErrorCode::InvalidSha256,
                format!("asset #{index} has invalid lowercase SHA256 hex"),
            ));
        }

        Ok(Self {
            os: required_string(asset_field(index, "os"), raw.os)?,
            arch: required_string(asset_field(index, "arch"), raw.arch)?,
            kind: required_string(asset_field(index, "kind"), raw.kind)?,
            name: required_string(asset_field(index, "name"), raw.name)?,
            sha256,
            size_bytes: required_u64(asset_field(index, "size_bytes"), raw.size_bytes)?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawDesktopUpdateManifest {
    schema_version: Option<u32>,
    product: Option<String>,
    version: Option<String>,
    tag: Option<String>,
    channel: Option<String>,
    published_at: Option<String>,
    assets: Option<Vec<RawDesktopUpdateManifestAsset>>,
}

#[derive(Debug, Deserialize)]
struct RawDesktopUpdateManifestAsset {
    os: Option<String>,
    arch: Option<String>,
    kind: Option<String>,
    name: Option<String>,
    sha256: Option<String>,
    size_bytes: Option<u64>,
}

fn reject_duplicate_assets(
    assets: &[DesktopUpdateManifestAsset],
) -> Result<(), DesktopManifestError> {
    let mut keys = HashSet::new();
    for asset in assets {
        let key = (asset.os.as_str(), asset.arch.as_str(), asset.kind.as_str());
        if !keys.insert(key) {
            return Err(DesktopManifestError::new(
                DesktopManifestErrorCode::DuplicateAsset,
                format!(
                    "duplicate desktop update asset entry for {}/{}/{}",
                    asset.os, asset.arch, asset.kind
                ),
            ));
        }
    }

    Ok(())
}

fn required_string(
    field: impl Into<String>,
    value: Option<String>,
) -> Result<String, DesktopManifestError> {
    let field = field.into();
    let value = value.ok_or_else(|| {
        DesktopManifestError::new(
            DesktopManifestErrorCode::MissingField,
            format!("desktop update manifest missing field: {field}"),
        )
    })?;

    if value.trim().is_empty() {
        return Err(DesktopManifestError::new(
            DesktopManifestErrorCode::EmptyField,
            format!("desktop update manifest field is empty: {field}"),
        ));
    }

    Ok(value)
}

fn required_u32(field: &str, value: Option<u32>) -> Result<u32, DesktopManifestError> {
    value.ok_or_else(|| {
        DesktopManifestError::new(
            DesktopManifestErrorCode::MissingField,
            format!("desktop update manifest missing field: {field}"),
        )
    })
}

fn required_u64(field: impl Into<String>, value: Option<u64>) -> Result<u64, DesktopManifestError> {
    let field = field.into();
    value.ok_or_else(|| {
        DesktopManifestError::new(
            DesktopManifestErrorCode::MissingField,
            format!("desktop update manifest missing field: {field}"),
        )
    })
}

fn required_vec<T>(field: &str, value: Option<Vec<T>>) -> Result<Vec<T>, DesktopManifestError> {
    value.ok_or_else(|| {
        DesktopManifestError::new(
            DesktopManifestErrorCode::MissingField,
            format!("desktop update manifest missing field: {field}"),
        )
    })
}

fn asset_field(index: usize, field: &str) -> String {
    format!("assets[{index}].{field}")
}

fn is_lowercase_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::{
        DESKTOP_UPDATE_PRODUCT, DESKTOP_UPDATE_SCHEMA_VERSION, DesktopManifestErrorCode,
        parse_desktop_update_manifest,
    };
    use serde_json::{Value, json};

    #[test]
    fn parses_valid_manifest() {
        let manifest = parse_manifest(valid_manifest());

        assert_eq!(manifest.schema_version, DESKTOP_UPDATE_SCHEMA_VERSION);
        assert_eq!(manifest.product, DESKTOP_UPDATE_PRODUCT);
        assert_eq!(manifest.version, "0.26.0");
        assert_eq!(manifest.tag, "v0.26.0");
        assert_eq!(manifest.channel, "stable");
        assert_eq!(manifest.assets.len(), 2);
        assert_eq!(manifest.assets[0].name, "Pioneer-aarch64.app.zip");
    }

    #[test]
    fn rejects_wrong_schema_version() {
        let mut value = valid_manifest();
        value["schema_version"] = json!(2);

        assert_error_code(value, DesktopManifestErrorCode::WrongSchemaVersion);
    }

    #[test]
    fn rejects_wrong_product() {
        let mut value = valid_manifest();
        value["product"] = json!("pioneer-gateway");

        assert_error_code(value, DesktopManifestErrorCode::WrongProduct);
    }

    #[test]
    fn rejects_missing_required_field() {
        let mut value = valid_manifest();
        value.as_object_mut().unwrap().remove("tag");

        assert_error_code(value, DesktopManifestErrorCode::MissingField);
    }

    #[test]
    fn rejects_empty_required_field() {
        let mut value = valid_manifest();
        value["assets"][0]["name"] = json!(" ");

        assert_error_code(value, DesktopManifestErrorCode::EmptyField);
    }

    #[test]
    fn rejects_invalid_sha256() {
        let mut value = valid_manifest();
        value["assets"][0]["sha256"] =
            json!("ABCDEFabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef");

        assert_error_code(value, DesktopManifestErrorCode::InvalidSha256);
    }

    #[test]
    fn rejects_duplicate_os_arch_kind_entries() {
        let mut value = valid_manifest();
        let duplicate = value["assets"][0].clone();
        value["assets"].as_array_mut().unwrap().push(duplicate);

        assert_error_code(value, DesktopManifestErrorCode::DuplicateAsset);
    }

    fn parse_manifest(value: Value) -> super::DesktopUpdateManifest {
        let bytes = serde_json::to_vec(&value).unwrap();
        parse_desktop_update_manifest(&bytes).unwrap()
    }

    fn assert_error_code(value: Value, expected_code: DesktopManifestErrorCode) {
        let bytes = serde_json::to_vec(&value).unwrap();
        let error = parse_desktop_update_manifest(&bytes).unwrap_err();
        assert_eq!(error.code(), expected_code);
    }

    fn valid_manifest() -> Value {
        json!({
            "schema_version": DESKTOP_UPDATE_SCHEMA_VERSION,
            "product": DESKTOP_UPDATE_PRODUCT,
            "version": "0.26.0",
            "tag": "v0.26.0",
            "channel": "stable",
            "published_at": "2026-07-08T00:00:00Z",
            "assets": [
                {
                    "os": "macos",
                    "arch": "aarch64",
                    "kind": "macos_app_zip",
                    "name": "Pioneer-aarch64.app.zip",
                    "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "size_bytes": 123
                },
                {
                    "os": "linux",
                    "arch": "x86_64",
                    "kind": "appimage",
                    "name": "pioneer-linux-x86_64.AppImage",
                    "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "size_bytes": 456
                }
            ]
        })
    }
}
