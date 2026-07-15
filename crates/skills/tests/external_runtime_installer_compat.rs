use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use pioneer_skills::{
    ExternalRuntimeCopyResult, compute_skill_folder_hash, replace_external_runtime_skill,
    sanitize_name,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/external-runtime-installer")
}

#[cfg(unix)]
fn copy_fixture_tree(source: &Path, destination: &Path) {
    use std::os::unix::fs::symlink;

    fs::create_dir_all(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type().unwrap();
        if file_type.is_symlink() {
            symlink(fs::read_link(source_path).unwrap(), destination_path).unwrap();
        } else if file_type.is_dir() {
            copy_fixture_tree(&source_path, &destination_path);
        } else {
            fs::copy(source_path, destination_path).unwrap();
        }
    }
}

#[cfg(unix)]
fn materialize_fixture_source(root: &Path, manifest: &Value, scratch: &Path) -> PathBuf {
    use std::os::unix::fs::symlink;

    let source = scratch.join("source");
    copy_fixture_tree(&root.join("source"), &source);
    for generated in manifest["fixture_materialization"]["generated_files"]
        .as_array()
        .unwrap()
    {
        let relative = Path::new(generated["path"].as_str().unwrap());
        assert!(
            relative.is_relative()
                && relative
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_))),
            "generated fixture file path must be a contained relative path"
        );
        let file = source.join(relative);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, generated["contents"].as_str().unwrap()).unwrap();
    }
    for generated in manifest["fixture_materialization"]["generated_symlinks"]
        .as_array()
        .unwrap()
    {
        let relative = Path::new(generated["path"].as_str().unwrap());
        assert!(
            relative.is_relative()
                && relative
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_))),
            "generated fixture symlink path must be a contained relative path"
        );
        let link = source.join(relative);
        fs::create_dir_all(link.parent().unwrap()).unwrap();
        symlink(generated["target"].as_str().unwrap(), link).unwrap();
    }
    source
}

fn file_hashes(root: &Path, current: &Path, out: &mut BTreeMap<String, String>) {
    for entry in fs::read_dir(current).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if entry.file_type().unwrap().is_dir() {
            file_hashes(root, &path, out);
        } else {
            let relative = path
                .strip_prefix(root)
                .unwrap()
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            out.insert(
                relative,
                hex::encode(Sha256::digest(fs::read(path).unwrap())),
            );
        }
    }
}

#[test]
fn external_runtime_installer_compat_manifest_is_pinned_and_self_consistent() {
    let root = fixture_root();
    let manifest: Value =
        serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["upstream"]["version"], "1.5.15");
    assert_eq!(
        manifest["upstream"]["commit"],
        "4ce6d48ac44c8b637db87b2102fea3baca719df1"
    );
    let source = fs::read(root.join("source/SKILL.md")).unwrap();
    let expected = fs::read(root.join("expected/SKILL.md")).unwrap();
    assert_eq!(source, expected);
    assert_eq!(
        hex::encode(Sha256::digest(&source)),
        manifest["skill_md_sha256"]
    );
    assert!(
        manifest["hash_order"]
            .as_array()
            .unwrap()
            .iter()
            .any(|path| path == "order/日本語.txt")
    );
    assert_eq!(
        manifest["copy_selection"]["copied_real_directory"],
        "node_modules"
    );
    assert_eq!(manifest["hash_selection"]["includes_metadata_json"], true);
    assert_eq!(
        manifest["fixture_materialization"]["generated_files"][0]["path"],
        "__pycache__/cache.pyc"
    );
    assert_eq!(
        manifest["fixture_materialization"]["generated_symlinks"][0]["path"],
        "symlink-exclusions/.git"
    );
}

#[cfg(unix)]
#[test]
fn external_runtime_installer_compat_public_api_matches_oracle() {
    let root = fixture_root();
    let manifest: Value =
        serde_json::from_slice(&fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let source_temp = tempfile::tempdir().unwrap();
    let source = materialize_fixture_source(&root, &manifest, source_temp.path());
    for case in manifest["sanitize"].as_array().unwrap() {
        assert_eq!(
            sanitize_name(case["input"].as_str().unwrap()),
            case["output"]
        );
    }
    assert_eq!(
        compute_skill_folder_hash(&source).unwrap(),
        manifest["folder_hash"]
    );
    let temp = tempfile::tempdir().unwrap();
    let codex = temp.path().join("codex/skills/oracle-skill");
    let claude = temp.path().join("claude/skills/oracle-skill");
    assert_eq!(
        replace_external_runtime_skill(&source, &codex).unwrap(),
        ExternalRuntimeCopyResult::Changed
    );
    assert_eq!(
        replace_external_runtime_skill(&source, &claude).unwrap(),
        ExternalRuntimeCopyResult::Changed
    );
    assert_eq!(
        fs::read(codex.join("SKILL.md")).unwrap(),
        fs::read(claude.join("SKILL.md")).unwrap()
    );
    assert_eq!(
        fs::read(codex.join("SKILL.md")).unwrap(),
        fs::read(root.join("expected/SKILL.md")).unwrap()
    );
    let mut actual = BTreeMap::new();
    file_hashes(&codex, &codex, &mut actual);
    let expected = manifest["copy_file_hashes"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(codex.join("scripts/run.sh"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
    }
}
