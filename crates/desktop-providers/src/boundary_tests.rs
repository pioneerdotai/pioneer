#[test]
fn feature_boundary_keeps_bindings_private_and_render_paths_free_of_new_primitives() {
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for entry in std::fs::read_dir(&source).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|extension| extension != "rs")
            || path.file_name().unwrap() == "boundary_tests.rs"
        {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        for forbidden in [
            "pioneer_client_ffi",
            "crate::app::",
            "pioneer_desktop_administration",
            ".cached(",
            "impl Element for",
            "pub mod binding",
            "pub use binding",
        ] {
            assert!(
                !text.contains(forbidden),
                "{} contains {}",
                path.display(),
                forbidden
            );
        }
    }
    let shell = source.join("../../desktop/src/desktop_shell.rs");
    let shell = std::fs::read_to_string(shell).unwrap();
    assert!(!shell.contains("ProviderListState"));
    assert!(!shell.contains("AdministrationCache"));
    assert!(!shell.contains("OpenModelSelectorCliRuntimeBinding"));
}
