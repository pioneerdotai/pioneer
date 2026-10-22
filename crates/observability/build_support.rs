//! Shared executable build metadata. This is provenance, NOT a post-link binary ID.
use sha2::{Digest, Sha256};
use std::{env, fs, path::PathBuf, process::Command};

pub fn manifest() {
    let crate_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let root = crate_dir.join("../..");
    let git = |args: &[&str]| {
        Command::new("git")
            .current_dir(&root)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| o.stdout)
    };
    let revision = git(&["rev-parse", "HEAD"])
        .and_then(|s| String::from_utf8(s).ok())
        .map(|s| s.trim().to_owned());
    // --porcelain includes untracked source; unknown is preserved outside git.
    let status = git(&["status", "--porcelain", "--untracked-files=normal"]);
    let dirty = status.as_ref().map(|s| !s.is_empty());
    let diff_digest = git(&["diff", "--binary", "HEAD"]).map(|s| hex::encode(Sha256::digest(s)));
    let lock = root.join("Cargo.lock");
    let dependency_revision = fs::read(&lock).ok().map(|s| hex::encode(Sha256::digest(s)));
    let mut features: Vec<_> = env::vars()
        .filter_map(|(k, _)| k.strip_prefix("CARGO_FEATURE_").map(str::to_owned))
        .collect();
    features.sort();
    let toolchain = Command::new(env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
        .arg("--version")
        .arg("--verbose")
        .output()
        .ok()
        .filter(|o| o.status.success() && o.stdout.len() <= 2048)
        .and_then(|o| String::from_utf8(o.stdout).ok());
    let flags_digest = env::var_os("CARGO_ENCODED_RUSTFLAGS")
        .map(|s| hex::encode(Sha256::digest(s.to_string_lossy().as_bytes())));
    let schema_revision = fs::read(root.join("crates/migration/src/lib.rs"))
        .ok()
        .map(|s| hex::encode(Sha256::digest(s)));
    let mut value = serde_json::json!({
        "schema_version": 1, "rust_toolchain":toolchain,"rustflags_sha256":flags_digest,"schema_registry_revision":schema_revision,"component": env::var("CARGO_PKG_NAME").ok(),
        "revision": revision, "dirty": dirty, "tracked_diff_sha256": diff_digest,
        "target": env::var("TARGET").ok(), "profile": env::var("PROFILE").ok(),
        "features": features, "target_features": env::var("CARGO_CFG_TARGET_FEATURE").ok(),
        "dependency_revision": dependency_revision,
        "provenance_completeness": "tracked-diff-and-lock; untracked-content-not-attested; post-link-ID-required"
    });
    let digest = hex::encode(Sha256::digest(serde_json::to_vec(&value).unwrap()));
    value["manifest_id"] = digest.into();
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let json = serde_json::to_vec(&value).unwrap();
    fs::write(out.join("pioneer-build.json"), &json).unwrap();
    let mut embedded = b"PIONEER_BUILD_V1\0".to_vec();
    embedded.extend_from_slice(&(json.len() as u32).to_le_bytes());
    embedded.extend_from_slice(&json);
    fs::write(out.join("pioneer-build-section.bin"), &embedded).unwrap();
    fs::write(out.join("pioneer-build-section.rs"), format!("#[used]\nstatic PIONEER_BUILD_SECTION: [u8; {}] = *include_bytes!(concat!(env!(\"OUT_DIR\"), \"/pioneer-build-section.bin\"));\n", embedded.len())).unwrap();
    println!("cargo:rerun-if-changed={}", lock.display());
    // Git resolves .git files for worktrees. Watching common-dir refs and the
    // index avoids assuming .git is a directory. Source archive stays unknown.
    for arg in ["HEAD", "index", "packed-refs", "refs"] {
        if let Some(path) =
            git(&["rev-parse", "--git-path", arg]).and_then(|s| String::from_utf8(s).ok())
        {
            println!(
                "cargo:rerun-if-changed={}",
                root.join(path.trim()).display()
            );
        }
    }
    println!("cargo:rerun-if-changed={}", root.join("crates").display());
    println!(
        "cargo:rerun-if-changed={}",
        root.join("Cargo.toml").display()
    );
}
