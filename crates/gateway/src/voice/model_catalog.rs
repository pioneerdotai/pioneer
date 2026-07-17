#[cfg(test)]
use anyhow::Context;
use anyhow::{Result, bail};
use pioneer_config::AppConfig;
use pioneer_provider::providers::{
    LOCAL_TRANSCRIPTION_MODELS, LocalTranscriptionArtifactKind, LocalTranscriptionEngine,
    LocalTranscriptionModelInfo, local_transcription_model_info,
};
use std::path::{Component, Path, PathBuf};

#[cfg(test)]
pub(crate) const PARAKEET_V3_INT8_MODEL_ID: &str = "parakeet-tdt-0.6b-v3";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VoiceModelArchiveType {
    SingleFile,
    TarGzDirectory,
}

impl From<LocalTranscriptionArtifactKind> for VoiceModelArchiveType {
    fn from(value: LocalTranscriptionArtifactKind) -> Self {
        match value {
            LocalTranscriptionArtifactKind::SingleFile => Self::SingleFile,
            LocalTranscriptionArtifactKind::TarGzDirectory => Self::TarGzDirectory,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VoiceModelCatalogEntry {
    pub(crate) id: &'static str,
    pub(crate) display_name: &'static str,
    pub(crate) version: &'static str,
    pub(crate) engine: LocalTranscriptionEngine,
    pub(crate) url: &'static str,
    pub(crate) sha256: &'static str,
    pub(crate) download_size_mb: u64,
    pub(crate) archive_type: VoiceModelArchiveType,
    pub(crate) archive_file_name: &'static str,
    pub(crate) install_dir_name: &'static str,
    pub(crate) model_data_dir_name: &'static str,
}

impl From<&'static LocalTranscriptionModelInfo> for VoiceModelCatalogEntry {
    fn from(model: &'static LocalTranscriptionModelInfo) -> Self {
        Self {
            id: model.id,
            display_name: model.display_name,
            version: model.install_dir_name,
            engine: model.engine,
            url: model.url,
            sha256: model.sha256,
            download_size_mb: model.size_mb,
            archive_type: model.artifact_kind.into(),
            archive_file_name: model.artifact_file_name,
            install_dir_name: model.install_dir_name,
            model_data_dir_name: model.runtime_file_name.unwrap_or(""),
        }
    }
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

#[cfg(test)]
pub(crate) fn parakeet_v3_int8_catalog_entry() -> VoiceModelCatalogEntry {
    voice_model_catalog_entry(PARAKEET_V3_INT8_MODEL_ID)
        .expect("the pinned Parakeet V3 model must remain in the local transcription catalog")
}

pub(crate) fn voice_model_catalog() -> Vec<VoiceModelCatalogEntry> {
    LOCAL_TRANSCRIPTION_MODELS
        .iter()
        .map(VoiceModelCatalogEntry::from)
        .collect()
}

pub(crate) fn voice_model_catalog_entry(model_id: &str) -> Option<VoiceModelCatalogEntry> {
    local_transcription_model_info(model_id).map(VoiceModelCatalogEntry::from)
}

#[cfg(test)]
pub(crate) fn parakeet_v3_int8_install_layout(
    config: &AppConfig,
    runtime_home: &Path,
) -> Result<VoiceModelInstallLayout> {
    voice_model_install_layout(&parakeet_v3_int8_catalog_entry(), config, runtime_home)
}

#[cfg(test)]
pub(crate) fn voice_model_install_layout_for_id(
    model_id: &str,
    config: &AppConfig,
    runtime_home: &Path,
) -> Result<VoiceModelInstallLayout> {
    let entry = voice_model_catalog_entry(model_id)
        .with_context(|| format!("unknown local transcription model `{model_id}`"))?;
    voice_model_install_layout(&entry, config, runtime_home)
}

pub(crate) fn voice_model_install_layout(
    entry: &VoiceModelCatalogEntry,
    config: &AppConfig,
    runtime_home: &Path,
) -> Result<VoiceModelInstallLayout> {
    let models_root = normalize_path(config.gateway.voice.resolve_models_root(runtime_home)?)?;
    let downloads_dir = contained_join(&models_root, "downloads", "downloads directory")?;
    let archive_path = contained_join(
        &downloads_dir,
        entry.archive_file_name,
        "artifact file name",
    )?;
    let partial_archive_path = contained_join(
        &downloads_dir,
        format!("{}.partial", entry.archive_file_name).as_str(),
        "partial artifact file name",
    )?;
    let install_dir = contained_join(
        &models_root,
        entry.install_dir_name,
        "model install directory",
    )?;
    let staging_dir = contained_join(
        &models_root,
        format!("{}.staging", entry.install_dir_name).as_str(),
        "model staging directory",
    )?;
    let model_data_dir = match entry.archive_type {
        VoiceModelArchiveType::SingleFile => contained_join(
            &install_dir,
            entry.model_data_dir_name,
            "model runtime file",
        )?,
        VoiceModelArchiveType::TarGzDirectory => install_dir.clone(),
    };
    let ready_marker_path = contained_join(&install_dir, ".ready", "ready marker")?;

    for path in [
        &downloads_dir,
        &archive_path,
        &partial_archive_path,
        &install_dir,
        &staging_dir,
        &model_data_dir,
        &ready_marker_path,
    ] {
        if !path.starts_with(&models_root) {
            bail!(
                "voice model path {} escapes models root {}",
                path.display(),
                models_root.display()
            );
        }
    }

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

fn contained_join(root: &Path, child: &str, label: &str) -> Result<PathBuf> {
    if child.is_empty()
        || child == "."
        || child == ".."
        || child.contains('/')
        || child.contains('\\')
        || !Path::new(child)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        bail!("unsafe voice {label} `{child}`");
    }

    let joined = normalize_path(root.join(child))?;
    if !joined.starts_with(root) {
        bail!(
            "voice {label} {} escapes models root {}",
            joined.display(),
            root.display()
        );
    }
    Ok(joined)
}

fn normalize_path(path: PathBuf) -> Result<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    bail!("voice models root escapes through parent components");
                }
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_config::AppConfig;

    #[test]
    fn voice_model_catalog_is_a_complete_adapter_over_provider_metadata() {
        let catalog = voice_model_catalog();

        assert_eq!(catalog.len(), LOCAL_TRANSCRIPTION_MODELS.len());
        for (entry, source) in catalog.iter().zip(LOCAL_TRANSCRIPTION_MODELS) {
            assert_eq!(entry.id, source.id);
            assert_eq!(entry.display_name, source.display_name);
            assert_eq!(entry.engine, source.engine);
            assert_eq!(entry.url, source.url);
            assert_eq!(entry.sha256, source.sha256);
            assert_eq!(entry.download_size_mb, source.size_mb);
            assert_eq!(entry.archive_file_name, source.artifact_file_name);
            assert_eq!(entry.install_dir_name, source.install_dir_name);
            assert_eq!(
                entry.archive_type,
                VoiceModelArchiveType::from(source.artifact_kind)
            );
        }
    }

    #[test]
    fn voice_model_catalog_layouts_are_deterministic_and_contained() {
        let config = AppConfig::load().expect("load config");
        let runtime_home = PathBuf::from("/tmp/pioneer-runtime/./nested/..");

        for source in LOCAL_TRANSCRIPTION_MODELS {
            let entry = voice_model_catalog_entry(source.id).expect("catalog entry");
            let first =
                voice_model_install_layout_for_id(source.id, &config, runtime_home.as_path())
                    .expect("first layout");
            let second =
                voice_model_install_layout_for_id(source.id, &config, runtime_home.as_path())
                    .expect("second layout");

            assert_eq!(first, second);
            assert!(first.models_root.ends_with("pioneer-runtime/models/voice"));
            for path in [
                &first.downloads_dir,
                &first.archive_path,
                &first.partial_archive_path,
                &first.install_dir,
                &first.staging_dir,
                &first.model_data_dir,
                &first.ready_marker_path,
            ] {
                assert!(path.starts_with(&first.models_root), "{}", path.display());
            }
            assert_eq!(
                first.install_dir.file_name().unwrap(),
                source.install_dir_name
            );
            assert_eq!(
                first.archive_path.file_name().unwrap(),
                source.artifact_file_name
            );
            assert_eq!(first.ready_marker_path, first.install_dir.join(".ready"));

            match source.artifact_kind {
                LocalTranscriptionArtifactKind::SingleFile => assert_eq!(
                    first.model_data_dir,
                    first
                        .install_dir
                        .join(source.runtime_file_name.expect("single-file runtime file"))
                ),
                LocalTranscriptionArtifactKind::TarGzDirectory => {
                    assert_eq!(first.model_data_dir, first.install_dir)
                }
            }

            assert_eq!(entry.id, source.id);
        }
    }

    #[test]
    fn voice_model_catalog_rejects_unknown_ids_and_cross_platform_separators() {
        let config = AppConfig::load().expect("load config");
        let runtime_home = Path::new("/tmp/pioneer-runtime");

        assert!(
            voice_model_install_layout_for_id("custom/../model", &config, runtime_home).is_err()
        );
        assert!(
            voice_model_install_layout_for_id("custom\\..\\model", &config, runtime_home).is_err()
        );
        assert!(voice_model_install_layout_for_id("unknown", &config, runtime_home).is_err());

        for unsafe_child in ["../escape", "folder/file", "folder\\file", "/absolute", ""] {
            assert!(contained_join(runtime_home, unsafe_child, "test path").is_err());
        }
    }

    #[test]
    fn parakeet_compatibility_helpers_resolve_from_shared_catalog() {
        let config = AppConfig::load().expect("load config");
        let runtime_home = PathBuf::from("/tmp/pioneer-runtime");
        let entry = parakeet_v3_int8_catalog_entry();
        let layout =
            parakeet_v3_int8_install_layout(&config, runtime_home.as_path()).expect("layout");

        assert_eq!(entry.id, PARAKEET_V3_INT8_MODEL_ID);
        assert_eq!(entry.archive_type, VoiceModelArchiveType::TarGzDirectory);
        assert_eq!(
            layout.install_dir.file_name().unwrap(),
            entry.install_dir_name
        );
        assert_eq!(layout.model_data_dir, layout.install_dir);
    }
}
