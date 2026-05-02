fn main() {
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
