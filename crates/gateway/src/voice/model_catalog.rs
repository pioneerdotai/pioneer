use anyhow::Result;
use pioneer_config::AppConfig;
use std::path::{Path, PathBuf};

pub(crate) const PARAKEET_V3_INT8_MODEL_ID: &str = "parakeet-tdt-0.6b-v3";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceModelArchiveType {
    TarGz,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoiceModelCatalogEntry {
    pub(crate) id: &'static str,
    pub(crate) display_name: &'static str,
    pub(crate) version: &'static str,
    pub(crate) url: &'static str,
    pub(crate) sha256: &'static str,
    pub(crate) archive_type: VoiceModelArchiveType,
    pub(crate) archive_file_name: &'static str,
    pub(crate) install_dir_name: &'static str,
    pub(crate) model_data_dir_name: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoiceModelInstallLayout {
    pub(crate) models_root: PathBuf,
    pub(crate) downloads_dir: PathBuf,
    pub(crate) archive_path: PathBuf,
    pub(crate) partial_archive_path: PathBuf,
    pub(crate) install_dir: PathBuf,
    pub(crate) staging_dir: PathBuf,
    pub(crate) model_data_dir: PathBuf,
    pub(crate) ready_marker_path: PathBuf,
}

pub(crate) fn parakeet_v3_int8_catalog_entry() -> VoiceModelCatalogEntry {
    VoiceModelCatalogEntry {
        id: PARAKEET_V3_INT8_MODEL_ID,
        display_name: "Parakeet V3",
        version: "parakeet-tdt-0.6b-v3-int8",
        url: "https://blob.handy.computer/parakeet-v3-int8.tar.gz",
        sha256: "43d37191602727524a7d8c6da0eef11c4ba24320f5b4730f1a2497befc2efa77",
        archive_type: VoiceModelArchiveType::TarGz,
        archive_file_name: "parakeet-v3-int8.tar.gz",
        install_dir_name: "parakeet-tdt-0.6b-v3-int8",
        model_data_dir_name: "",
    }
}

pub(crate) fn voice_model_catalog() -> Vec<VoiceModelCatalogEntry> {
    vec![parakeet_v3_int8_catalog_entry()]
}

pub(crate) fn parakeet_v3_int8_install_layout(
    config: &AppConfig,
    runtime_home: &Path,
) -> Result<VoiceModelInstallLayout> {
    voice_model_install_layout(&parakeet_v3_int8_catalog_entry(), config, runtime_home)
}

pub(crate) fn voice_model_install_layout(
    entry: &VoiceModelCatalogEntry,
    config: &AppConfig,
    runtime_home: &Path,
) -> Result<VoiceModelInstallLayout> {
    let models_root = config.gateway.voice.resolve_models_root(runtime_home)?;
    let downloads_dir = models_root.join("downloads");
    let archive_path = downloads_dir.join(entry.archive_file_name);
    let partial_archive_path = downloads_dir.join(format!("{}.partial", entry.archive_file_name));
    let install_dir = models_root.join(entry.install_dir_name);
    let staging_dir = models_root.join(format!("{}.staging", entry.install_dir_name));
    let model_data_dir = if entry.model_data_dir_name.is_empty() {
        install_dir.clone()
    } else {
        install_dir.join(entry.model_data_dir_name)
    };
    let ready_marker_path = install_dir.join(".ready");

    Ok(VoiceModelInstallLayout {
        models_root,
        downloads_dir,
        archive_path,
        partial_archive_path,
        install_dir,
        staging_dir,
        model_data_dir,
        ready_marker_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_config::AppConfig;

    #[test]
    fn parakeet_catalog_entry_uses_handy_download_metadata() {
        let entry = parakeet_v3_int8_catalog_entry();

        assert_eq!(entry.id, "parakeet-tdt-0.6b-v3");
        assert_eq!(entry.archive_type, VoiceModelArchiveType::TarGz);
        assert_eq!(
            entry.url,
            "https://blob.handy.computer/parakeet-v3-int8.tar.gz"
        );
        assert_eq!(
            entry.sha256,
            "43d37191602727524a7d8c6da0eef11c4ba24320f5b4730f1a2497befc2efa77"
        );
        assert_eq!(entry.install_dir_name, "parakeet-tdt-0.6b-v3-int8");
        assert!(entry.model_data_dir_name.is_empty());
    }

    #[test]
    fn parakeet_layout_is_under_gateway_runtime_home() {
        let config = AppConfig::load().expect("load config");
        let runtime_home = PathBuf::from("/tmp/pioneer-runtime");
        let layout =
            parakeet_v3_int8_install_layout(&config, runtime_home.as_path()).expect("layout");

        assert_eq!(layout.models_root, runtime_home.join("models/voice"));
        assert_eq!(
            layout.archive_path,
            runtime_home.join("models/voice/downloads/parakeet-v3-int8.tar.gz")
        );
        assert_eq!(
            layout.partial_archive_path,
            runtime_home.join("models/voice/downloads/parakeet-v3-int8.tar.gz.partial")
        );
        assert_eq!(
            layout.install_dir,
            runtime_home.join("models/voice/parakeet-tdt-0.6b-v3-int8")
        );
        assert_eq!(
            layout.staging_dir,
            runtime_home.join("models/voice/parakeet-tdt-0.6b-v3-int8.staging")
        );
        assert_eq!(
            layout.model_data_dir,
            runtime_home.join("models/voice/parakeet-tdt-0.6b-v3-int8")
        );
        assert_eq!(
            layout.ready_marker_path,
            runtime_home.join("models/voice/parakeet-tdt-0.6b-v3-int8/.ready")
        );
    }
}
