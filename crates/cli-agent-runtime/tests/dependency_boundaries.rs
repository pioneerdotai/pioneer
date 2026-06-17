use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate should live under workspace/crates")
        .to_path_buf()
}

fn read_manifest(relative_path: &str) -> String {
    let path = workspace_root().join(relative_path);
    fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read `{}`: {error}", path.display()))
}

#[test]
fn provider_does_not_depend_on_cli_agent_runtime() {
    let provider_manifest = read_manifest("crates/provider/Cargo.toml");

    assert!(
        !provider_manifest.contains("pioneer-cli-agent-runtime"),
        "pioneer-provider must remain API-only and cannot depend on pioneer-cli-agent-runtime"
    );
}

#[test]
fn cli_agent_runtime_does_not_depend_on_api_provider_or_desktop() {
    let runtime_manifest = read_manifest("crates/cli-agent-runtime/Cargo.toml");

    for forbidden in ["pioneer-provider", "pioneer-desktop", "gpui"] {
        assert!(
            !runtime_manifest.contains(forbidden),
            "pioneer-cli-agent-runtime must not depend on `{forbidden}`"
        );
    }
}
