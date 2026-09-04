const FORBIDDEN_DEPENDENCIES: &[&str] = &[
    "gpui",
    "gpui-kit",
    "gpui-component",
    "terminal",
    "pioneer-gateway",
    "rust-i18n",
];

#[test]
fn manifest_declares_forbidden_dependency_guard() {
    let manifest = include_str!("../Cargo.toml");

    for dependency in FORBIDDEN_DEPENDENCIES {
        assert!(
            manifest.contains(&format!("\"{dependency}\"")),
            "`{dependency}` is missing from the forbidden dependency guard metadata"
        );
    }
}

#[test]
fn manifest_has_no_forbidden_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    let mut in_dependency_section = false;

    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_dependency_section = trimmed == "[dependencies]"
                || trimmed == "[dev-dependencies]"
                || trimmed == "[build-dependencies]"
                || trimmed.contains(".dependencies]");
            continue;
        }

        if !in_dependency_section || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let Some(dependency_name) = trimmed
            .split(|c: char| c == '=' || c.is_whitespace())
            .next()
        else {
            continue;
        };

        assert!(
            !FORBIDDEN_DEPENDENCIES.contains(&dependency_name),
            "`pioneer-client` must not depend on `{dependency_name}`"
        );
    }
}
