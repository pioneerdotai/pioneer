// Final-binary Swift inputs for the pinned cua-driver SDK (0.38.0).
// Adapted from trycua/cua 688c5fd62a9f13dddc309970c948df494e7de33f,
// libs/cua-driver/rust/crates/cua-driver/build.rs (MIT; notice in
// crates/tools/src/handlers/computer_use/LICENSE.cua).
// Transitive rustc-link-arg directives do not reach Pioneer executables.
pub fn macos_link_inputs() {
    // Each final binary must opt in explicitly; dependency features do not
    // propagate Cargo feature environment variables to its build script.
    if std::env::var_os("CARGO_FEATURE_COMPUTER_USE").is_none() {
        return;
    }
    let manifest = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    println!(
        "cargo:rerun-if-changed={}",
        manifest.join("../gateway/build_support.rs").display()
    );
    println!("cargo:rerun-if-env-changed=SDKROOT");
    println!("cargo:rerun-if-env-changed=DEVELOPER_DIR");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }
    emit_sdk_framework_search_path();
    emit_swift_runtime_link_args();
}

fn emit_sdk_framework_search_path() {
    if let Some(sdk_root) = active_macos_sdk_root() {
        println!(
            "cargo:rustc-link-search=framework={}",
            sdk_root.join("System/Library/Frameworks").display()
        );
    }
}

fn emit_swift_runtime_link_args() {
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    println!("cargo:rustc-link-arg=-Wl,-rpath,/usr/lib/swift");

    let mut dirs = BTreeSet::new();

    // `swiftc` links against the SDK's Swift TBDs on current Xcode releases.
    // The toolchain runtime directories below contain compatibility libraries,
    // but are not guaranteed to contain the full Swift runtime surface.
    if let Some(sdk_root) = active_macos_sdk_root() {
        let sdk_swift_dir = sdk_root.join("usr/lib/swift");
        if sdk_swift_dir.is_dir() {
            println!("cargo:rustc-link-search=native={}", sdk_swift_dir.display());
        }
    }

    if let Ok(out) = std::process::Command::new("xcode-select")
        .arg("-p")
        .output()
    {
        if out.status.success() {
            let xcode_path = String::from_utf8_lossy(&out.stdout).trim().to_string();
            let developer_dir = PathBuf::from(xcode_path);
            for sub in [
                "Toolchains/XcodeDefault.xctoolchain/usr/lib/swift/macosx",
                "Toolchains/XcodeDefault.xctoolchain/usr/lib/swift-5.5/macosx",
                "Toolchains/XcodeDefault.xctoolchain/usr/lib/swift-6.2/macosx",
                "usr/lib/swift/macosx",
                "usr/lib/swift-5.5/macosx",
                "usr/lib/swift-6.2/macosx",
            ] {
                dirs.insert(developer_dir.join(sub));
            }
        }
    }

    if let Ok(out) = Command::new("xcrun").args(["--find", "swiftc"]).output() {
        if out.status.success() {
            let swiftc = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim().to_string());
            if let Some(usr_dir) = swiftc.parent().and_then(Path::parent) {
                for sub in [
                    "lib/swift/macosx",
                    "lib/swift-5.5/macosx",
                    "lib/swift-6.2/macosx",
                ] {
                    dirs.insert(usr_dir.join(sub));
                }
            }
        }
    }

    for dir in dirs {
        if dir.is_dir() {
            println!("cargo:rustc-link-search=native={}", dir.display());
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", dir.display());
        }
    }
}

fn active_macos_sdk_root() -> Option<std::path::PathBuf> {
    std::env::var_os("SDKROOT")
        .map(std::path::PathBuf::from)
        .filter(|path| path.is_dir())
        .or_else(|| {
            let out = std::process::Command::new("xcrun")
                .args(["--sdk", "macosx", "--show-sdk-path"])
                .output()
                .ok()?;
            out.status
                .success()
                .then(|| std::path::PathBuf::from(String::from_utf8_lossy(&out.stdout).trim()))
                .filter(|path| path.is_dir())
        })
}
