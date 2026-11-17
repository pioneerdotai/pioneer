use pioneer_client::{ClientError, platform::ClientPath};

#[test]
fn client_error_display_is_stable_for_scaffold() {
    let error = ClientError::invalid_state("missing active gateway");

    assert_eq!(
        error.to_string(),
        "invalid client state: missing active gateway"
    );
}

#[test]
fn platform_path_round_trips_without_desktop_types() {
    let path = ClientPath::new("workspace/thread.txt");

    assert_eq!(path.as_path().to_string_lossy(), "workspace/thread.txt");
    assert_eq!(
        path.into_path_buf().to_string_lossy(),
        "workspace/thread.txt"
    );
}
