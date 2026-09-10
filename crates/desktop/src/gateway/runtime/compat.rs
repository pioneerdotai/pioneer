pub(super) fn local_gateway_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub(crate) fn is_same_gateway_version(installed: &str, expected: &str) -> bool {
    match (normalize_version(installed), normalize_version(expected)) {
        (Some(installed_version), Some(expected_version)) => installed_version == expected_version,
        _ => false,
    }
}

fn normalize_version(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_start_matches(['v', 'V']);
    if trimmed.is_empty() {
        return None;
    }

    Some(trimmed.to_owned())
}
