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

// Foundation module inventory, closed retained-owner exceptions and Render
// purity have one primary owner: desktop/tests/desktop_architecture.rs and its
// handwritten contract. Keep cross-shell boundary checks here.

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
fn shell_uses_the_registrar_seam_without_exporting_its_concrete_router() {
    let root = workspace_root();
    let router =
        fs::read_to_string(root.join("crates/desktop/src/client_runtime/binding_router.rs"))
            .unwrap();
    assert!(router.contains("ClientBindingRegistrar"));
    let foundation = fs::read_to_string(root.join("crates/desktop-foundation/src/lib.rs")).unwrap();
    assert!(!foundation.contains("DesktopClientBindingRouter"));
}
