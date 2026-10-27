#[path = "../observability/build_support.rs"]
mod build_support;
#[path = "../gateway/build_support.rs"]
mod cua_build_support;
fn main() {
    cua_build_support::macos_link_inputs();
    build_support::manifest();
    println!("cargo:rerun-if-env-changed=PIONEER_LEGACY_CU_PACKAGE");
    if std::env::var_os("CARGO_FEATURE_LEGACY_CU_BOOTSTRAP").is_some() {
        let package = std::path::PathBuf::from(
            std::env::var_os("PIONEER_LEGACY_CU_PACKAGE")
                .expect("legacy CU bridge requires the completed CU release package"),
        )
        .canonicalize()
        .expect("legacy CU package must exist");
        println!("cargo:rerun-if-changed={}", package.display());
        let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
        std::fs::write(
            output.join("pioneer-cu-update-package.rs"),
            format!("const PACKAGE: &[u8] = include_bytes!({:?});", package),
        )
        .expect("write legacy CU package binding");
    }
}
