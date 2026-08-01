//! Explicit development-only schema export through the allowed Cargo test path.

#![cfg(feature = "schema")]

#[test]
#[ignore = "writes authoritative client schemas to PIONEER_SCHEMA_OUTPUT"]
fn export_authoritative_client_schemas() {
    let output = std::env::var_os("PIONEER_SCHEMA_OUTPUT")
        .expect("PIONEER_SCHEMA_OUTPUT must name the checked-in schema directory");
    let output = std::path::PathBuf::from(output);
    assert!(
        output.ends_with("src/client/schema"),
        "schema output must end with src/client/schema"
    );
    if output.exists() {
        std::fs::remove_dir_all(&output).expect("replace schema output directory");
    }
    std::fs::create_dir_all(&output).expect("create schema output directory");
    pioneer_client::schema::write_client_schemas(&output).expect("write shared client schemas");
    pioneer_client_ffi::schema::write_client_ffi_schemas(&output)
        .expect("write client FFI schemas");
}
