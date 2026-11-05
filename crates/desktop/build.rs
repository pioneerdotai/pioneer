#[path = "../observability/build_support.rs"]
mod build_support;

fn main() {
    build_support::manifest();
    // Development images can exceed compact unwind's 24-bit offset range.
    // Apple's debug mode keeps unwinding and omits release-only advisories.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos")
        && std::env::var("PROFILE").as_deref() == Ok("debug")
    {
        println!("cargo:rustc-link-arg=-Wl,-debug_variant");
    }
    #[cfg(target_os = "windows")]
    {
        let manifest_dir = std::path::PathBuf::from(
            std::env::var("CARGO_MANIFEST_DIR")
                .expect("CARGO_MANIFEST_DIR is missing for desktop build script"),
        );

        let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        if target_os != "windows" {
            return;
        }

        let icon_path = manifest_dir.join("assets").join("app-icon.ico");
        println!("cargo:rerun-if-changed={}", icon_path.display());

        let icon_path = icon_path
            .to_str()
            .expect("desktop app icon path contains invalid UTF-8");

        let mut resource = winresource::WindowsResource::new();
        resource.set_icon(icon_path);
        resource
            .compile()
            .expect("failed to compile Windows icon resources");
    }
}
