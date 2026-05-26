use super::model::DesktopPreflightReport;
use serde_json::Value as JsonValue;

pub(crate) fn computer_use_preflight_payload(report: DesktopPreflightReport) -> JsonValue {
    let running_binary = running_binary_path();
    let permissions = permission_entries(report.platform.as_str(), running_binary.as_deref());
    serde_json::json!({
        "action": "preflight",
        "mode": "remote",
        "platform": report.platform,
        "desktop_environment": desktop_environment_payload(report.platform.as_str()),
        "status": report.status,
        "message": report.message,
        "running_binary": running_binary,
        "capabilities": report.capabilities,
        "permissions": permissions,
        "blocking_issues": report.blocking_issues,
        "warnings": report.warnings,
    })
}

fn permission_entries(platform: &str, running_binary: Option<&str>) -> Vec<JsonValue> {
    match platform {
        "macos" => vec![
            serde_json::json!({
                "name": "Accessibility",
                "status": "unknown",
                "required_for": ["accessibility_tree", "accessibility_actions"],
                "binary_path": running_binary,
                "instructions": macos_permission_instructions("Accessibility", running_binary)
            }),
            serde_json::json!({
                "name": "Screen Recording",
                "status": "unknown",
                "required_for": ["screenshot"],
                "binary_path": running_binary,
                "instructions": macos_permission_instructions("Screen Recording", running_binary)
            }),
            serde_json::json!({
                "name": "Input Monitoring",
                "status": "unknown",
                "required_for": ["input_simulation"],
                "binary_path": running_binary,
                "instructions": macos_permission_instructions("Input Monitoring", running_binary)
            }),
        ],
        "linux" => vec![
            serde_json::json!({
                "name": "AT-SPI accessibility bus",
                "status": "unknown",
                "required_for": ["accessibility_tree", "accessibility_actions"],
                "instructions": "Ensure the desktop session exposes AT-SPI and the target app has accessibility enabled."
            }),
            serde_json::json!({
                "name": "Screenshot backend",
                "status": "unknown",
                "required_for": ["screenshot"],
                "instructions": "Use X11 or a Wayland session with a working desktop portal grant for screenshots."
            }),
            serde_json::json!({
                "name": "Input simulation backend",
                "status": "unknown",
                "required_for": ["input_simulation"],
                "instructions": "Use X11/XTest or a Wayland portal/session that supports synthetic input."
            }),
        ],
        "windows" => vec![
            serde_json::json!({
                "name": "UI Automation",
                "status": "unknown",
                "required_for": ["accessibility_tree", "accessibility_actions"],
                "instructions": "Ensure the process can access Windows UI Automation for the target desktop session."
            }),
            serde_json::json!({
                "name": "Desktop screenshot/input APIs",
                "status": "unknown",
                "required_for": ["screenshot", "input_simulation"],
                "instructions": "Run in an interactive desktop session with screenshot and input APIs available."
            }),
        ],
        _ => vec![serde_json::json!({
            "name": "Desktop accessibility support",
            "status": "unknown",
            "required_for": ["accessibility_tree", "accessibility_actions", "screenshot", "input_simulation"],
            "instructions": "This platform must expose xa11y accessibility, screenshot, and input backends."
        })],
    }
}

pub(crate) fn macos_permission_guidance() -> String {
    let running_binary = running_binary_path();
    format!(
        "macOS permissions required: Accessibility for accessibility tree/actions, Screen Recording for screenshots, and Input Monitoring only for explicit input_* actions. Add the running gateway binary{} in System Settings > Privacy & Security, then restart the gateway.",
        running_binary
            .as_deref()
            .map(|path| format!(" `{path}`"))
            .unwrap_or_else(|| "".to_owned())
    )
}

fn macos_permission_instructions(permission: &str, running_binary: Option<&str>) -> String {
    format!(
        "System Settings > Privacy & Security > {permission}: allow the running gateway binary{}, then restart the gateway.",
        running_binary
            .map(|path| format!(" `{path}`"))
            .unwrap_or_else(|| "".to_owned())
    )
}

fn running_binary_path() -> Option<String> {
    std::env::current_exe()
        .ok()
        .map(|path| path.display().to_string())
}

fn desktop_environment_payload(platform: &str) -> JsonValue {
    match platform {
        "linux" => serde_json::json!({
            "session_type": std::env::var("XDG_SESSION_TYPE").ok(),
            "wayland_display": std::env::var("WAYLAND_DISPLAY").ok(),
            "display": std::env::var("DISPLAY").ok(),
            "note": "Linux computer_use behavior depends on AT-SPI plus X11/Wayland screenshot and input portal availability."
        }),
        _ => serde_json::json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::computer_use::model::DesktopPreflightCapabilities;

    #[test]
    fn preflight_payload_preserves_ready_status() {
        let payload = computer_use_preflight_payload(report("ready"));
        assert_eq!(payload["status"], "ready");
        assert_eq!(payload["capabilities"]["accessibility_tree"], "ok");
        assert!(payload.get("running_binary").is_some());
        assert!(
            payload["permissions"]
                .as_array()
                .is_some_and(|value| !value.is_empty())
        );
    }

    #[test]
    fn preflight_payload_preserves_degraded_status() {
        let mut report = report("degraded");
        report.capabilities.input_simulation = "blocked".to_owned();
        report.warnings.push("input blocked".to_owned());
        let payload = computer_use_preflight_payload(report);
        assert_eq!(payload["status"], "degraded");
        assert_eq!(payload["capabilities"]["input_simulation"], "blocked");
        assert_eq!(payload["warnings"].as_array().expect("warnings").len(), 1);
    }

    #[test]
    fn preflight_payload_preserves_blocked_status() {
        let mut report = report("blocked");
        report.capabilities.accessibility_tree = "blocked".to_owned();
        report.blocking_issues.push("tree blocked".to_owned());
        let payload = computer_use_preflight_payload(report);
        assert_eq!(payload["status"], "blocked");
        assert_eq!(
            payload["blocking_issues"].as_array().expect("issues").len(),
            1
        );
    }

    #[test]
    fn macos_permission_entries_name_binary_and_tcc_panes() {
        let entries = permission_entries("macos", Some("/tmp/pioneer-gateway"));
        assert_eq!(entries.len(), 3);
        let rendered = serde_json::to_string(&entries).expect("entries serialize");
        assert!(rendered.contains("/tmp/pioneer-gateway"));
        assert!(rendered.contains("Accessibility"));
        assert!(rendered.contains("Screen Recording"));
        assert!(rendered.contains("Input Monitoring"));
        assert!(rendered.contains("restart the gateway"));
    }

    #[test]
    fn linux_desktop_environment_payload_documents_x11_wayland_context() {
        let payload = desktop_environment_payload("linux");
        assert!(
            payload["note"]
                .as_str()
                .is_some_and(|note| note.contains("X11/Wayland"))
        );
        assert!(payload.get("session_type").is_some());
        assert!(payload.get("wayland_display").is_some());
        assert!(payload.get("display").is_some());
    }

    fn report(status: &str) -> DesktopPreflightReport {
        DesktopPreflightReport {
            platform: "macos".to_owned(),
            status: status.to_owned(),
            message: "test".to_owned(),
            capabilities: DesktopPreflightCapabilities {
                accessibility_tree: "ok".to_owned(),
                accessibility_actions: "ok".to_owned(),
                screenshot: "ok".to_owned(),
                input_simulation: "ok".to_owned(),
            },
            blocking_issues: Vec::new(),
            warnings: Vec::new(),
        }
    }
}
