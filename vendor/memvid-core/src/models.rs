use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Component, Path, PathBuf};

#[cfg(feature = "vec")]
use std::time::Instant;

use serde::Deserialize;

use crate::error::{MemvidError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelVerificationStatus {
    Ok,
    Warn,
    Fail,
}

impl ModelVerificationStatus {
    fn elevate(&mut self, other: ModelVerificationStatus) {
        use ModelVerificationStatus::{Fail, Ok, Warn};
        match (*self, other) {
            (Fail, _) | (_, Fail) => *self = Fail,
            (Warn, _) | (_, Warn) => {
                if matches!(*self, Ok) {
                    *self = Warn;
                }
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModelVerification {
    pub digest: String,
    pub dims: Option<u32>,
    pub quant: Option<String>,
    pub context_length: Option<u32>,
    pub status: ModelVerificationStatus,
    pub load_latency_ms: Option<u128>,
    pub path: PathBuf,
    pub warnings: Vec<String>,
    pub errors: Vec<String>,
}

impl ModelVerification {
    fn from_error(path: PathBuf, err: MemvidError) -> Self {
        let digest = digest_from_dir_name(&path).unwrap_or_else(|| "sha256:unknown".to_string());
        Self {
            digest,
            dims: None,
            quant: None,
            context_length: None,
            status: ModelVerificationStatus::Fail,
            load_latency_ms: None,
            path,
            warnings: Vec::new(),
            errors: vec![err.to_string()],
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModelVerifyOptions {
    pub run_onnx_smoke: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
pub struct ModelManifest {
    pub schema_version: u32,
    pub digest: String,
    pub dims: u32,
    pub quant: Option<String>,
    pub context_length: Option<u32>,
    pub files: Vec<ModelManifestEntry>,
    pub metadata: serde_json::Value,
}

impl Default for ModelManifest {
    fn default() -> Self {
        Self {
            schema_version: 1,
            digest: String::new(),
            dims: 0,
            quant: None,
            context_length: None,
            files: Vec::new(),
            metadata: serde_json::Value::Null,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
#[derive(Default)]
pub struct ModelManifestEntry {
    pub path: String,
    pub sha256: String,
    pub optional: bool,
    pub roles: Vec<String>,
    pub kind: Option<String>,
}

pub fn verify_models(root: &Path, options: &ModelVerifyOptions) -> Result<Vec<ModelVerification>> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut dirs: Vec<PathBuf> = fs::read_dir(root)?
        .filter_map(std::result::Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            entry
                .file_type()
                .ok()
                .filter(std::fs::FileType::is_dir)
                .and_then(|_| digest_from_dir_name(&path).map(|_| path))
        })
        .collect();
    dirs.sort();

    let mut reports = Vec::with_capacity(dirs.len());
    for dir in dirs {
        match verify_model_dir(&dir, options) {
            Ok(report) => reports.push(report),
            Err(err) => reports.push(ModelVerification::from_error(dir, err)),
        }
    }

    reports.sort_by(|a, b| a.digest.cmp(&b.digest));
    Ok(reports)
}

pub fn verify_model_dir(dir: &Path, options: &ModelVerifyOptions) -> Result<ModelVerification> {
    let manifest_path = dir.join("manifest.json");
    if !manifest_path.exists() {
        return Err(MemvidError::ModelIntegrity {
            reason: format!("missing manifest.json in {}", dir.display()).into_boxed_str(),
        });
    }

    let manifest_data = fs::read_to_string(&manifest_path)?;
    let manifest: ModelManifest =
        serde_json::from_str(&manifest_data).map_err(|err| MemvidError::ModelManifestInvalid {
            reason: format!(
                "failed to parse manifest {}: {err}",
                manifest_path.display()
            )
            .into_boxed_str(),
        })?;

    if manifest.digest.trim().is_empty() {
        return Err(MemvidError::ModelManifestInvalid {
            reason: "manifest digest is empty".into(),
        });
    }

    if manifest.dims == 0 {
        return Err(MemvidError::ModelManifestInvalid {
            reason: "embedding dimensions must be > 0".into(),
        });
    }

    let manifest_digest_hex = normalize_sha256(&manifest.digest, "manifest digest")?;
    let dir_digest_hex = digest_from_dir_name(dir).ok_or_else(|| MemvidError::ModelIntegrity {
        reason: format!(
            "directory {} is not named as sha256-<digest>",
            dir.display()
        )
        .into_boxed_str(),
    })?;

    let dir_digest_hex = normalize_sha256(&dir_digest_hex, "directory digest")?;
    if manifest_digest_hex != dir_digest_hex {
        return Err(MemvidError::ModelIntegrity {
            reason: format!(
                "manifest digest sha256:{manifest_digest_hex} does not match directory sha256:{dir_digest_hex}"
            )
            .into_boxed_str(),
        });
    }

    let digest = format!("sha256:{manifest_digest_hex}");

    let mut status = ModelVerificationStatus::Ok;
    let mut warnings = Vec::new();
    let mut errors = Vec::new();
    let mut load_latency_ms = None;

    for entry in &manifest.files {
        validate_entry(entry)?;
        let expected_hex = normalize_sha256(&entry.sha256, &entry.path)?;
        let resolved_path = resolve_entry_path(dir, &entry.path)?;
        if !resolved_path.exists() {
            if entry.optional {
                warnings.push(format!("optional file missing: {}", entry.path));
                status.elevate(ModelVerificationStatus::Warn);
            } else {
                errors.push(format!("required file missing: {}", entry.path));
                status.elevate(ModelVerificationStatus::Fail);
            }
            continue;
        }

        let actual_hex = compute_sha256_hex(&resolved_path)?;
        if actual_hex != expected_hex {
            errors.push(format!(
                "checksum mismatch for {} (expected {}, got {})",
                entry.path, expected_hex, actual_hex
            ));
            status.elevate(ModelVerificationStatus::Fail);
        }
    }

    if status != ModelVerificationStatus::Fail && options.run_onnx_smoke {
        if let Some(weights_entry) = select_weights_entry(&manifest) {
            let weights_path = resolve_entry_path(dir, &weights_entry.path)?;
            if weights_path.exists() {
                match run_onnx_smoke_test(&weights_path) {
                    Ok(latency) => {
                        load_latency_ms = Some(latency.max(1));
                    }
                    Err(OnnxSmokeError::FeatureUnavailable(feature)) => {
                        warnings.push(format!(
                            "feature '{feature}' not enabled; skipping ONNX smoke test"
                        ));
                        status.elevate(ModelVerificationStatus::Warn);
                    }
                    Err(OnnxSmokeError::Engine(err)) => {
                        errors.push(format!("ONNX initialisation failed: {err}"));
                        status.elevate(ModelVerificationStatus::Fail);
                    }
                }
            }
        } else {
            warnings.push(
                "manifest does not declare a model .onnx file; skipping ONNX smoke test".into(),
            );
            status.elevate(ModelVerificationStatus::Warn);
        }
    }

    let resolved_dir = fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());

    Ok(ModelVerification {
        digest,
        dims: Some(manifest.dims),
        quant: manifest.quant.clone(),
        context_length: manifest.context_length,
        status,
        load_latency_ms,
        path: resolved_dir,
        warnings,
        errors,
    })
}

fn validate_entry(entry: &ModelManifestEntry) -> Result<()> {
    if entry.path.trim().is_empty() {
        return Err(MemvidError::ModelManifestInvalid {
            reason: "file entry path is empty".into(),
        });
    }
    if entry.path.contains('\\') {
        return Err(MemvidError::ModelManifestInvalid {
            reason: format!("file entry path must use forward slashes: {}", entry.path)
                .into_boxed_str(),
        });
    }
    if entry.sha256.trim().is_empty() {
        return Err(MemvidError::ModelManifestInvalid {
            reason: format!("file entry '{}' missing sha256", entry.path).into_boxed_str(),
        });
    }
    Ok(())
}

fn resolve_entry_path(base: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    if path.is_absolute() {
        return Err(MemvidError::ModelManifestInvalid {
            reason: format!("file entry '{relative}' must be relative").into_boxed_str(),
        });
    }

    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(MemvidError::ModelManifestInvalid {
                reason: format!("file entry '{relative}' attempts directory traversal")
                    .into_boxed_str(),
            });
        }
    }

    Ok(base.join(path))
}

fn normalize_sha256(value: &str, context: &str) -> Result<String> {
    let trimmed = value.trim();
    let trimmed = trimmed
        .strip_prefix("sha256:")
        .or_else(|| trimmed.strip_prefix("sha256-"))
        .unwrap_or(trimmed);
    if trimmed.len() != 64 || !trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(MemvidError::ModelManifestInvalid {
            reason: format!("invalid sha256 value for {context}").into_boxed_str(),
        });
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn digest_from_dir_name(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix("sha256-")
        .map(std::string::ToString::to_string)
}

fn compute_sha256_hex(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn select_weights_entry(manifest: &ModelManifest) -> Option<&ModelManifestEntry> {
    if let Some(quant) = manifest.quant.as_deref() {
        if let Some(entry) = manifest
            .files
            .iter()
            .find(|entry| entry.path.ends_with(".onnx") && entry.path.contains(quant))
        {
            return Some(entry);
        }
    }

    manifest
        .files
        .iter()
        .find(|entry| entry.roles.iter().any(|role| role == "weights"))
        .or_else(|| {
            manifest
                .files
                .iter()
                .find(|entry| entry.kind.as_deref() == Some("onnx"))
        })
        .or_else(|| {
            manifest
                .files
                .iter()
                .find(|entry| entry.path.ends_with(".onnx"))
        })
}

#[allow(dead_code)]
#[derive(Debug)]
enum OnnxSmokeError {
    FeatureUnavailable(&'static str),
    Engine(String),
}

#[cfg(feature = "vec")]
fn run_onnx_smoke_test(path: &Path) -> std::result::Result<u128, OnnxSmokeError> {
    use ort::session::Session;

    let mut builder = Session::builder().map_err(|err| OnnxSmokeError::Engine(err.to_string()))?;
    let start = Instant::now();
    let session = builder
        .commit_from_file(path)
        .map_err(|err| OnnxSmokeError::Engine(err.to_string()))?;
    drop(session);
    let elapsed = start.elapsed().as_millis();
    Ok(elapsed.max(1))
}

#[cfg(not(feature = "vec"))]
fn run_onnx_smoke_test(_path: &Path) -> std::result::Result<u128, OnnxSmokeError> {
    Err(OnnxSmokeError::FeatureUnavailable("vec"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    fn write_manifest(path: &Path, value: &serde_json::Value) -> Result<()> {
        let bytes =
            serde_json::to_vec_pretty(value).map_err(|err| MemvidError::ModelManifestInvalid {
                reason: format!("failed to encode manifest: {err}").into_boxed_str(),
            })?;
        fs::write(path, bytes)?;
        Ok(())
    }

    fn write_file(path: &Path, contents: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents)?;
        Ok(())
    }

    fn checksum_hex(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    #[test]
    fn verify_model_success() -> Result<()> {
        let temp = tempdir()?;
        let digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let model_dir = temp.path().join(format!("sha256-{digest}"));
        fs::create_dir_all(&model_dir)?;

        let model_bytes = b"ONNX";
        let tokenizer_bytes = b"{}";

        write_file(
            &model_dir.join("models/encoder/model_int8.onnx"),
            model_bytes,
        )?;
        write_file(
            &model_dir.join("models/encoder/tokenizer.json"),
            tokenizer_bytes,
        )?;

        let manifest = serde_json::json!({
            "digest": format!("sha256:{digest}"),
            "dims": 384,
            "quant": "int8",
            "files": [
                {
                    "path": "models/encoder/model_int8.onnx",
                    "sha256": checksum_hex(model_bytes),
                    "roles": ["weights"],
                },
                {
                    "path": "models/encoder/tokenizer.json",
                    "sha256": checksum_hex(tokenizer_bytes),
                }
            ]
        });
        write_manifest(&model_dir.join("manifest.json"), &manifest)?;

        let options = ModelVerifyOptions {
            run_onnx_smoke: false,
        };
        let report = verify_model_dir(&model_dir, &options)?;
        assert_eq!(report.digest, format!("sha256:{digest}"));
        assert_eq!(report.status, ModelVerificationStatus::Ok);
        assert_eq!(report.dims, Some(384));
        assert!(report.errors.is_empty());
        Ok(())
    }

    #[test]
    fn verify_model_missing_optional_warns() -> Result<()> {
        let temp = tempdir()?;
        let digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let model_dir = temp.path().join(format!("sha256-{digest}"));
        fs::create_dir_all(&model_dir)?;

        let model_bytes = b"ONNX";

        write_file(&model_dir.join("models/model.onnx"), model_bytes)?;

        let manifest = serde_json::json!({
            "digest": format!("sha256:{digest}"),
            "dims": 256,
            "files": [
                {
                    "path": "models/model.onnx",
                    "sha256": checksum_hex(model_bytes),
                    "roles": ["weights"],
                },
                {
                    "path": "models/tokenizer.json",
                    "sha256": checksum_hex(b"missing"),
                    "optional": true
                }
            ]
        });
        write_manifest(&model_dir.join("manifest.json"), &manifest)?;

        let options = ModelVerifyOptions {
            run_onnx_smoke: false,
        };
        let report = verify_model_dir(&model_dir, &options)?;
        assert_eq!(report.status, ModelVerificationStatus::Warn);
        assert!(report.errors.is_empty());
        assert_eq!(report.warnings.len(), 1);
        Ok(())
    }

    #[test]
    fn verify_models_directory_listing() -> Result<()> {
        let temp = tempdir()?;
        let digest = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let model_dir = temp.path().join(format!("sha256-{digest}"));
        fs::create_dir_all(&model_dir)?;

        let model_bytes = b"ONNX";
        fs::write(model_dir.join("model.onnx"), model_bytes)?;

        let manifest = serde_json::json!({
            "digest": format!("sha256:{digest}"),
            "dims": 128,
            "files": [
                {
                    "path": "model.onnx",
                    "sha256": checksum_hex(model_bytes),
                    "roles": ["weights"],
                }
            ]
        });
        write_manifest(&model_dir.join("manifest.json"), &manifest)?;

        let options = ModelVerifyOptions {
            run_onnx_smoke: false,
        };
        let reports = verify_models(temp.path(), &options)?;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].digest, format!("sha256:{digest}"));
        Ok(())
    }
}
