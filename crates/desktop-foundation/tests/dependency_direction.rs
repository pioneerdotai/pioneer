use std::{fs, path::Path, path::PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("Foundation crate is nested under the workspace root")
        .to_path_buf()
}

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).expect("source directory must be readable") {
            let path = entry.expect("source entry must be readable").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
                sources.push(path);
            }
        }
    }
    sources.sort();
    sources
}

#[test]
fn desktop_depends_on_client_directly_and_not_on_mobile_boundary() {
    let root = workspace_root();
    let desktop_manifest = fs::read_to_string(root.join("crates/desktop/Cargo.toml"))
        .expect("Desktop manifest must be readable");
    assert!(desktop_manifest.contains("pioneer-client.workspace = true"));
    for forbidden in ["pioneer-client-ffi", "nitro", "react-native"] {
        assert!(
            !desktop_manifest.contains(forbidden),
            "Desktop manifest contains forbidden Mobile boundary: {forbidden}"
        );
    }

    for path in rust_sources(&root.join("crates/desktop/src")) {
        let source = fs::read_to_string(&path).expect("Desktop source must be readable");
        for forbidden in [
            "pioneer_client_ffi",
            "extern \"C\"",
            "HybridPioneerClient",
            "src/client/generated",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} contains forbidden Mobile boundary: {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn foundation_manifest_contains_only_approved_runtime_dependencies() {
    let manifest = fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .expect("Foundation manifest must be readable");
    let dependencies = manifest
        .split("[dependencies]")
        .nth(1)
        .and_then(|value| value.split("[dev-dependencies]").next())
        .expect("runtime dependency table");
    let keys = dependencies
        .lines()
        .filter_map(|line| line.split_once('=').map(|(key, _)| key.trim()))
        .filter(|key| !key.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(keys, ["gpui-kit", "pioneer-client.workspace"]);
}

#[test]
fn foundation_has_no_router_store_cache_or_custom_element_implementation() {
    let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut source_names = rust_sources(&source_root)
        .iter()
        .map(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .expect("UTF-8 Foundation source name")
                .to_owned()
        })
        .collect::<Vec<_>>();
    source_names.sort();
    assert_eq!(
        source_names,
        ["avatar.rs", "binding.rs", "identity.rs", "lib.rs"]
    );

    for entry in fs::read_dir(source_root).expect("Foundation source directory") {
        let path = entry.expect("Foundation source entry").path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("Foundation source file");
        for forbidden in [
            "struct DesktopClientBindingRouter",
            "impl Element for",
            "Entity<",
            "ClientEffectPlan",
            "ClientIntent",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} contains forbidden Foundation responsibility: {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn client_contract_remains_shell_neutral() {
    let root = workspace_root();
    let manifest = fs::read_to_string(root.join("crates/client/Cargo.toml"))
        .expect("Client manifest must be readable");
    let dependencies = manifest
        .split("[dependencies]")
        .nth(1)
        .and_then(|value| value.split("[target.").next())
        .expect("Client runtime dependency table");
    for forbidden in ["gpui", "gpui-kit", "react-native", "nitro"] {
        assert!(
            !dependencies
                .lines()
                .any(|line| line.trim_start().starts_with(forbidden)),
            "Client runtime dependency table contains shell framework: {forbidden}"
        );
    }
}

#[test]
fn foundation_is_not_consumed_by_a_feature_implementation() {
    let desktop_sources = workspace_root().join("crates/desktop/src");
    for path in rust_sources(&desktop_sources) {
        let source = fs::read_to_string(&path).expect("Desktop source must be readable");
        assert!(
            !source.contains("pioneer_desktop_foundation"),
            "{} consumes Foundation before feature extraction",
            path.display()
        );
    }
}

#[test]
fn cached_element_contract_is_explicit_without_adding_a_cache() {
    let source = fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"))
        .expect("Foundation root source");
    for required in [
        "definite bounds",
        "explicitly updates the element",
        "notifies its retained owner",
        "content mask",
        "TextStyle",
    ] {
        assert!(
            source.contains(required),
            "missing cache contract: {required}"
        );
    }
}
