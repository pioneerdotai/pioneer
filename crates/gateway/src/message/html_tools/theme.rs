//! Projection of the same standard theme resource loaded by desktop. This is
//! only for preview; published documents receive the host's current palette.
use std::sync::LazyLock;

const STANDARD_THEME: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/themes/pioneer.json"
));

pub(super) fn standard_theme_css(dark: bool) -> String {
    static THEMES: LazyLock<serde_json::Value> = LazyLock::new(|| {
        serde_json::from_str(STANDARD_THEME).expect("bundled Pioneer theme must be valid JSON")
    });
    let mode = if dark { "dark" } else { "light" };
    let colors = &THEMES["themes"]
        .as_array()
        .expect("bundled Pioneer themes must be an array")
        .iter()
        .find(|theme| theme["mode"].as_str() == Some(mode))
        .expect("bundled Pioneer theme must define both modes")["colors"];
    let mut css = format!(":root{{color-scheme:{mode};");
    for (token, role) in [
        ("background", "background"),
        ("foreground", "foreground"),
        ("muted", "muted.foreground"),
        ("border", "border"),
        ("accent", "primary.background"),
        ("accent-foreground", "primary.foreground"),
    ] {
        let value = colors[role]
            .as_str()
            .expect("bundled Pioneer theme must define HTML color roles");
        css.push_str(&format!("--pioneer-{token}:{value};"));
    }
    css.push('}');
    css
}

#[cfg(test)]
mod tests {
    use super::standard_theme_css;

    #[test]
    fn standard_modes_project_pioneer_action_and_document_colors() {
        for (dark, mode, background, foreground, action, action_text) in [
            (false, "light", "#ffffff", "#0a0a0a", "#171717", "#fafafa"),
            (true, "dark", "#0a0a0a", "#fafafa", "#d4d4d4", "#171717"),
        ] {
            let css = standard_theme_css(dark);
            assert!(css.contains(&format!("color-scheme:{mode};")));
            for (role, value) in [
                ("background", background),
                ("foreground", foreground),
                ("accent", action),
                ("accent-foreground", action_text),
            ] {
                assert!(css.contains(&format!("--pioneer-{role}:{value};")));
            }
        }
    }
}
