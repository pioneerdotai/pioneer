use super::super::actions::parse_xa11y_key;
use super::super::model::{
    ActionExecution, AppHandle, AppHandleMeta, AppMeta, AppTarget, CapturedFrame,
    ComputerUseFailureClass, DesktopPreflightCapabilities, DesktopPreflightOptions,
    DesktopPreflightReport, DesktopTree, DisplayMeta, InputAction, InputActionKind,
    MouseButtonKind, OsAction, OsActionKind, ResolvedActionTarget, ResolvedInputActionTargets,
    SemanticAction, SemanticActionKind, SnapshotBudget, SnapshotTarget, derive_app_identity_key,
};
use super::super::permissions;
use super::super::platform;
use super::super::tree::{AccessibilityTreeBudget, compact_xa11y_app_tree};
use super::ComputerUseDesktopBackend;
use super::screenshots::{captured_frame_from_rgba_buffer, captured_frame_from_rgba_image};
use crate::error::ToolError;
use std::time::{Duration, Instant};
use xa11y::{
    Anchor, AppExt, ClickOptions, ClickTarget, DragOptions, InputSim, Key, MouseButton, Point,
    ScrollDelta,
};
// xa11y 0.8.1 does not expose public monitor enumeration. Keep xcap isolated here
// strictly for list_displays/display-target screenshots until multi-display xa11y API exists.
use xcap::Monitor;

#[derive(Default)]
pub(crate) struct Xa11yComputerUseBackend;

impl ComputerUseDesktopBackend for Xa11yComputerUseBackend {
    fn preflight(
        &self,
        options: DesktopPreflightOptions,
    ) -> Result<DesktopPreflightReport, ToolError> {
        let platform = std::env::consts::OS.to_owned();
        let mut blocking_issues = Vec::new();
        let mut warnings = Vec::new();

        let accessibility_tree = match xa11y::App::list() {
            Ok(_) => "ok".to_owned(),
            Err(error) => {
                blocking_issues.push(preflight_capability_error("accessibility tree", &error));
                "blocked".to_owned()
            }
        };
        let accessibility_actions = if accessibility_tree == "ok" {
            "ok".to_owned()
        } else {
            "blocked".to_owned()
        };
        let screenshot = if options.screenshot_probe_enabled {
            match xa11y::screenshot() {
                Ok(_) => "ok".to_owned(),
                Err(error) => {
                    blocking_issues.push(preflight_capability_error("screenshot", &error));
                    "blocked".to_owned()
                }
            }
        } else {
            "skipped".to_owned()
        };
        let input_simulation = if options.input_simulation_enabled {
            match xa11y::input_sim() {
                Ok(_) => "ok".to_owned(),
                Err(error) => {
                    warnings.push(preflight_capability_error("input simulation", &error));
                    "blocked".to_owned()
                }
            }
        } else {
            "disabled".to_owned()
        };

        let screenshot_ready = screenshot == "ok" || screenshot == "skipped";
        let status = if accessibility_tree == "ok" && screenshot_ready {
            if input_simulation == "ok" {
                "ready"
            } else {
                "degraded"
            }
        } else {
            "blocked"
        }
        .to_owned();

        let message = match status.as_str() {
            "ready" => "xa11y desktop-control capabilities are available",
            "degraded" => {
                "xa11y semantic control is available, but optional input simulation is blocked"
            }
            _ => "xa11y desktop-control preflight is blocked",
        }
        .to_owned();

        Ok(DesktopPreflightReport {
            platform,
            status,
            message,
            capabilities: DesktopPreflightCapabilities {
                accessibility_tree,
                accessibility_actions,
                screenshot,
                input_simulation,
            },
            blocking_issues,
            warnings,
        })
    }

    fn list_apps(&self) -> Result<Vec<AppMeta>, ToolError> {
        xa11y::App::list()
            .map_err(to_xa11y_error("app.list"))?
            .into_iter()
            .map(|app| {
                Ok(platform::enrich_app_identity(AppMeta {
                    identity_key: Some(derive_app_identity_key(
                        app.name.as_str(),
                        app.pid,
                        None,
                        None,
                    )),
                    name: app.name,
                    pid: app.pid,
                    role: None,
                    window_title: None,
                    bundle_id: None,
                    localized_name: None,
                    executable_path: None,
                    frontmost: None,
                }))
            })
            .collect()
    }

    fn frontmost_app(&self) -> Result<Option<AppMeta>, ToolError> {
        Ok(self
            .list_apps()?
            .into_iter()
            .find(|app| app.frontmost == Some(true)))
    }

    fn find_app(&self, target: &AppTarget, timeout: Duration) -> Result<AppHandle, ToolError> {
        if let Some(app) = self.find_app_from_inventory(target)? {
            return Ok(app);
        }

        let app = if let Some(pid) = target.pid {
            xa11y::App::by_pid(pid, timeout).map_err(to_xa11y_error("app.by_pid"))?
        } else if let Some(name) = target.name.as_deref() {
            xa11y::App::by_name(name, timeout).map_err(to_xa11y_error("app.by_name"))?
        } else if let Some(path) = target.executable_path.as_deref() {
            let name = platform::app_bundle_name_from_path(std::path::Path::new(path)).ok_or_else(
                || ToolError::invalid_arguments("executable_path target has no app name"),
            )?;
            xa11y::App::by_name(name.as_str(), timeout).map_err(to_xa11y_error("app.by_name"))?
        } else {
            return Err(ToolError::invalid_arguments(
                "app target requires name, pid, identity_key, bundle_id, or executable_path",
            ));
        };
        Ok(app_handle_from_meta(platform::enrich_app_identity(
            AppMeta {
                identity_key: Some(derive_app_identity_key(
                    app.name.as_str(),
                    app.pid,
                    None,
                    None,
                )),
                name: app.name,
                pid: app.pid,
                role: None,
                window_title: None,
                bundle_id: None,
                localized_name: None,
                executable_path: None,
                frontmost: None,
            },
        )))
    }

    fn launch_app(
        &self,
        target: &AppTarget,
        launch_command: Option<&str>,
    ) -> Result<(), ToolError> {
        platform::launch_app_target(target, launch_command)
    }

    fn activate_app(&self, app: &AppHandle) -> Result<(), ToolError> {
        platform::activate_app(app)
    }

    fn app_tree(
        &self,
        app: &AppHandle,
        budget: AccessibilityTreeBudget,
    ) -> Result<DesktopTree, ToolError> {
        let app = if let Some(pid) = app.pid {
            xa11y::App::by_pid(pid, Duration::from_millis(100))
                .map_err(to_xa11y_error("app.by_pid"))?
        } else {
            xa11y::App::by_name(app.name.as_str(), Duration::from_millis(100))
                .map_err(to_xa11y_error("app.by_name"))?
        };
        compact_xa11y_app_tree(&app, budget)
    }

    fn screenshot(
        &self,
        target: &SnapshotTarget,
        snapshot_budget: &SnapshotBudget,
    ) -> Result<CapturedFrame, ToolError> {
        match target {
            SnapshotTarget::PrimaryScreen => capture_xa11y_primary_screen(snapshot_budget),
            SnapshotTarget::Display { display_id } => {
                self.capture_display(*display_id, snapshot_budget)
            }
        }
    }

    fn perform_semantic_action(
        &self,
        app: &AppHandle,
        action: &SemanticAction,
        target: Option<&ResolvedActionTarget>,
    ) -> Result<ActionExecution, ToolError> {
        let started = Instant::now();
        let action_type = action.action_type.as_str().to_owned();
        let target_payload = target.cloned();
        let result = execute_semantic_action(app, action, target);
        let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        match result {
            Ok(message) => Ok(ActionExecution {
                status: "ok".to_owned(),
                message,
                action_type: Some(action_type),
                target: target_payload,
                duration_ms: Some(duration_ms),
                failure_class: None,
                app_before: None,
                app_after: None,
                details: None,
            }),
            Err(SemanticActionError::Invalid(message)) => {
                Err(ToolError::invalid_arguments(message))
            }
            Err(SemanticActionError::Xa11y(error)) => Ok(ActionExecution {
                status: "failed".to_owned(),
                message: error.to_string(),
                action_type: Some(action_type),
                target: target_payload,
                duration_ms: Some(duration_ms),
                failure_class: Some(xa11y_failure_class(&error).to_owned()),
                app_before: None,
                app_after: None,
                details: None,
            }),
        }
    }

    fn perform_input_action(
        &self,
        action: &InputAction,
        targets: &ResolvedInputActionTargets,
    ) -> Result<ActionExecution, ToolError> {
        let started = Instant::now();
        let action_type = action.action_type.as_str().to_owned();
        let target_payload = targets
            .target
            .clone()
            .or_else(|| targets.from.clone())
            .or_else(|| targets.to.clone());
        let result = execute_input_action(action, targets);
        let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        match result {
            Ok(message) => Ok(ActionExecution {
                status: "ok".to_owned(),
                message,
                action_type: Some(action_type),
                target: target_payload,
                duration_ms: Some(duration_ms),
                failure_class: None,
                app_before: None,
                app_after: None,
                details: None,
            }),
            Err(InputActionError::Invalid(message)) => Err(ToolError::invalid_arguments(message)),
            Err(InputActionError::Xa11y(error)) => Ok(ActionExecution {
                status: "failed".to_owned(),
                message: error.to_string(),
                action_type: Some(action_type),
                target: target_payload,
                duration_ms: Some(duration_ms),
                failure_class: Some(input_failure_class(&error).to_owned()),
                app_before: None,
                app_after: None,
                details: None,
            }),
        }
    }

    fn perform_os_action(&self, action: &OsAction) -> Result<ActionExecution, ToolError> {
        match action.action_type {
            OsActionKind::OpenApp | OsActionKind::ActivateApp | OsActionKind::FocusWindow => {
                self.perform_app_os_action(action)
            }
            OsActionKind::OpenPath | OsActionKind::RevealPath | OsActionKind::OpenUrl => {
                Ok(self.perform_external_os_action(action))
            }
            _ => Ok(unsupported_os_action(action)),
        }
    }

    fn list_displays(&self) -> Result<Vec<DisplayMeta>, ToolError> {
        let monitors = Monitor::all().map_err(|error| {
            ToolError::execution_failed(format!("failed to list monitors: {error}"))
        })?;
        let mut displays = Vec::with_capacity(monitors.len());
        for monitor in monitors {
            displays.push(DisplayMeta {
                display_id: monitor.id().map_err(to_tool_error("monitor.id"))?,
                width_px: monitor.width().map_err(to_tool_error("monitor.width"))?,
                height_px: monitor.height().map_err(to_tool_error("monitor.height"))?,
                scale_factor: monitor
                    .scale_factor()
                    .map_err(to_tool_error("monitor.scale_factor"))?,
                origin_x: monitor.x().map_err(to_tool_error("monitor.x"))?,
                origin_y: monitor.y().map_err(to_tool_error("monitor.y"))?,
                is_primary: monitor
                    .is_primary()
                    .map_err(to_tool_error("monitor.is_primary"))?,
            });
        }
        Ok(displays)
    }

    fn capture_display(
        &self,
        display_id: u32,
        snapshot_budget: &SnapshotBudget,
    ) -> Result<CapturedFrame, ToolError> {
        let monitor = Monitor::all()
            .map_err(|error| {
                ToolError::execution_failed(format!("failed to list monitors: {error}"))
            })?
            .into_iter()
            .find(|value| value.id().ok() == Some(display_id))
            .ok_or_else(|| ToolError::NotFound(format!("display {} not found", display_id)))?;

        let image = monitor.capture_image().map_err(|error| {
            ToolError::execution_failed(format!("capture_image failed: {error}"))
        })?;

        let scale_factor = monitor
            .scale_factor()
            .map_err(to_tool_error("monitor.scale_factor"))?;
        captured_frame_from_rgba_image(image, scale_factor, snapshot_budget)
    }
}

impl Xa11yComputerUseBackend {
    fn find_app_from_inventory(&self, target: &AppTarget) -> Result<Option<AppHandle>, ToolError> {
        if target.pid.is_none()
            && target.name.as_deref().is_none_or(str::is_empty)
            && target.identity_key.as_deref().is_none_or(str::is_empty)
            && target.bundle_id.as_deref().is_none_or(str::is_empty)
            && target.executable_path.as_deref().is_none_or(str::is_empty)
        {
            return Ok(None);
        }

        let apps = self.list_apps()?;
        Ok(apps
            .into_iter()
            .find(|app| app_matches_target(app, target))
            .map(app_handle_from_meta))
    }

    fn perform_app_os_action(&self, action: &OsAction) -> Result<ActionExecution, ToolError> {
        let started = Instant::now();
        let action_type = action.action_type.as_str().to_owned();
        let expected_after = app_action_expected_after(action);
        let app_name = action
            .app
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                ToolError::invalid_arguments(format!(
                    "{} requires non-empty app",
                    action.action_type.as_str()
                ))
            })?;
        let target = AppTarget {
            name: Some(app_name.to_owned()),
            pid: None,
            identity_key: None,
            bundle_id: None,
            executable_path: None,
        };
        let app_before = self
            .find_app(&target, Duration::from_millis(100))
            .ok()
            .map(AppHandleMeta::from);

        if action.action_type == OsActionKind::OpenApp {
            if let Err(error) = platform::launch_app_target(&target, None) {
                let failure_class = match &error {
                    ToolError::NotFound(_) => ComputerUseFailureClass::ActionNotSupported,
                    _ => ComputerUseFailureClass::RuntimeActionError,
                };
                return Ok(app_os_execution(
                    action_type,
                    format!("failed to open app `{app_name}`: {error}"),
                    "failed",
                    failure_class,
                    app_before,
                    None,
                    started,
                    expected_after.clone(),
                ));
            }
            std::thread::sleep(Duration::from_millis(250));
        }

        let app = match self.find_app(&target, Duration::from_millis(2_500)) {
            Ok(app) => app,
            Err(error) => {
                return Ok(app_os_execution(
                    action_type,
                    format!(
                        "app `{app_name}` was not found after {}: {error}",
                        action.action_type.as_str()
                    ),
                    "failed",
                    ComputerUseFailureClass::AppNotFound,
                    app_before,
                    None,
                    started,
                    expected_after.clone(),
                ));
            }
        };

        if action.action_type == OsActionKind::FocusWindow {
            if let (Some(title), Some(window_title)) =
                (action.title.as_deref(), app.window_title.as_deref())
            {
                if !contains_case_insensitive(window_title, title) {
                    return Ok(app_os_execution(
                        action_type,
                        format!(
                            "app `{}` is available, but no exposed window title matched `{title}`",
                            app.name
                        ),
                        "failed",
                        ComputerUseFailureClass::ElementNotFound,
                        app_before,
                        Some(AppHandleMeta::from(app)),
                        started,
                        expected_after,
                    ));
                }
            }
        }

        if let Err(error) = platform::activate_app(&app) {
            return Ok(app_os_execution(
                action_type,
                format!("failed to activate app `{}`: {error}", app.name),
                "failed",
                ComputerUseFailureClass::RuntimeActionError,
                app_before,
                Some(AppHandleMeta::from(app)),
                started,
                expected_after.clone(),
            ));
        }

        let app_after = AppHandleMeta::from(app);
        Ok(app_os_execution(
            action_type,
            format!(
                "{} app `{}`",
                match action.action_type {
                    OsActionKind::OpenApp => "Opened",
                    OsActionKind::FocusWindow => "Focused",
                    _ => "Activated",
                },
                app_after.name
            ),
            "ok",
            ComputerUseFailureClass::RuntimeActionError,
            app_before,
            Some(app_after),
            started,
            expected_after,
        ))
    }

    fn perform_external_os_action(&self, action: &OsAction) -> ActionExecution {
        let started = Instant::now();
        let action_type = action.action_type.as_str().to_owned();
        let result = match action.action_type {
            OsActionKind::OpenPath => action
                .path
                .as_deref()
                .ok_or_else(|| ToolError::invalid_arguments("open_path requires path"))
                .and_then(|path| platform::open_path(std::path::Path::new(path))),
            OsActionKind::RevealPath => action
                .path
                .as_deref()
                .ok_or_else(|| ToolError::invalid_arguments("reveal_path requires path"))
                .and_then(|path| platform::reveal_path(std::path::Path::new(path))),
            OsActionKind::OpenUrl => action
                .url
                .as_deref()
                .ok_or_else(|| ToolError::invalid_arguments("open_url requires url"))
                .and_then(|raw_url| {
                    platform::normalize_open_url(raw_url).and_then(|url| platform::open_url(&url))
                }),
            _ => Ok(()),
        };
        let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        match result {
            Ok(()) => ActionExecution {
                status: "ok".to_owned(),
                message: format!("Executed OS action {}", action.action_type.as_str()),
                action_type: Some(action_type),
                target: None,
                duration_ms: Some(duration_ms),
                failure_class: None,
                app_before: None,
                app_after: None,
                details: Some(external_os_action_details(action)),
            },
            Err(error) => ActionExecution {
                status: "failed".to_owned(),
                message: error.to_string(),
                action_type: Some(action_type),
                target: None,
                duration_ms: Some(duration_ms),
                failure_class: Some(
                    ComputerUseFailureClass::RuntimeActionError
                        .as_str()
                        .to_owned(),
                ),
                app_before: None,
                app_after: None,
                details: Some(external_os_action_details(action)),
            },
        }
    }
}

fn app_os_execution(
    action_type: String,
    message: String,
    status: &str,
    failure_class: ComputerUseFailureClass,
    app_before: Option<AppHandleMeta>,
    app_after: Option<AppHandleMeta>,
    started: Instant,
    expected_after: serde_json::Value,
) -> ActionExecution {
    let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    let details = serde_json::json!({
        "app_before": app_before.clone(),
        "app_after": app_after.clone(),
        "pid": app_after.as_ref().and_then(|app| app.pid),
        "resolved_name": app_after.as_ref().map(|app| app.name.clone()),
        "localized_name": app_after.as_ref().and_then(|app| app.localized_name.clone()),
        "expected_after": expected_after,
    });
    ActionExecution {
        status: status.to_owned(),
        message,
        action_type: Some(action_type),
        target: None,
        duration_ms: Some(duration_ms),
        failure_class: if status == "ok" {
            None
        } else {
            Some(failure_class.as_str().to_owned())
        },
        app_before,
        app_after,
        details: Some(details),
    }
}

fn app_handle_from_meta(app: AppMeta) -> AppHandle {
    AppHandle {
        identity_key: app.identity_key,
        name: app.name,
        pid: app.pid,
        role: app.role,
        window_title: app.window_title,
        bundle_id: app.bundle_id,
        localized_name: app.localized_name,
        executable_path: app.executable_path,
        frontmost: app.frontmost,
    }
}

fn app_matches_target(app: &AppMeta, target: &AppTarget) -> bool {
    if target.pid.is_some() && app.pid == target.pid {
        return true;
    }
    if non_empty_eq(app.identity_key.as_deref(), target.identity_key.as_deref()) {
        return true;
    }
    if non_empty_eq(app.bundle_id.as_deref(), target.bundle_id.as_deref()) {
        return true;
    }
    if non_empty_eq(
        app.executable_path.as_deref(),
        target.executable_path.as_deref(),
    ) {
        return true;
    }
    if let Some(name) = target
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return app.name == name || app.localized_name.as_deref() == Some(name);
    }
    false
}

fn non_empty_eq(left: Option<&str>, right: Option<&str>) -> bool {
    let Some(left) = left.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let Some(right) = right.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    left == right
}

fn unsupported_os_action(action: &OsAction) -> ActionExecution {
    ActionExecution {
        status: "failed".to_owned(),
        message: unsupported_os_action_message(action),
        action_type: Some(action.action_type.as_str().to_owned()),
        target: None,
        duration_ms: Some(0),
        failure_class: Some(
            ComputerUseFailureClass::ActionNotSupported
                .as_str()
                .to_owned(),
        ),
        app_before: None,
        app_after: None,
        details: Some(serde_json::json!({
            "expected_after": menu_window_expected_after(action),
            "platform_guidance": "This xa11y backend does not expose a stable cross-platform menu selection API yet. Use semantic accessibility actions from a fresh snapshot when available, or stop with action_not_supported."
        })),
    }
}

fn unsupported_os_action_message(action: &OsAction) -> String {
    match action.action_type {
        OsActionKind::SelectMenuItem => format!(
            "OS action select_menu_item is not supported by the xa11y backend yet for app `{}` and menu path {:?}",
            action.app.as_deref().unwrap_or(""),
            action.menu_path.as_ref().cloned().unwrap_or_default()
        ),
        _ => format!(
            "OS action {} is not implemented for xa11y backend yet",
            action.action_type.as_str()
        ),
    }
}

fn app_action_expected_after(action: &OsAction) -> serde_json::Value {
    serde_json::json!({
        "app": action.app.clone(),
        "title": action.title.clone(),
        "state": if action.action_type == OsActionKind::FocusWindow {
            "window_focused"
        } else {
            "app_active"
        }
    })
}

fn menu_window_expected_after(action: &OsAction) -> serde_json::Value {
    serde_json::json!({
        "app": action.app.clone(),
        "menu_path": action.menu_path.clone(),
        "title": action.title.clone(),
        "state": match action.action_type {
            OsActionKind::SelectMenuItem => "menu_item_selected",
            OsActionKind::FocusWindow => "window_focused",
            _ => "unsupported",
        }
    })
}

fn contains_case_insensitive(haystack: &str, needle: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .contains(needle.to_ascii_lowercase().as_str())
}

fn external_os_action_details(action: &OsAction) -> serde_json::Value {
    let expected_app = match action.action_type {
        OsActionKind::OpenPath | OsActionKind::RevealPath => "system_file_manager",
        OsActionKind::OpenUrl => "system_url_handler",
        _ => "unknown",
    };
    serde_json::json!({
        "path": action.path.clone(),
        "url": action.url.clone(),
        "expected_app": expected_app,
        "expected_window_title": action
            .path
            .as_deref()
            .and_then(|path| std::path::Path::new(path).file_name())
            .and_then(|name| name.to_str())
            .or(action.url.as_deref()),
        "verification_hint": "Take a fresh snapshot and verify the expected system handler, window, or URL content is visible.",
    })
}

#[derive(Debug)]
enum SemanticActionError {
    Invalid(String),
    Xa11y(xa11y::Error),
}

impl From<xa11y::Error> for SemanticActionError {
    fn from(value: xa11y::Error) -> Self {
        Self::Xa11y(value)
    }
}

fn execute_semantic_action(
    app: &AppHandle,
    action: &SemanticAction,
    target: Option<&ResolvedActionTarget>,
) -> Result<String, SemanticActionError> {
    let app = load_xa11y_app(app)?;
    let locator = semantic_locator(&app, target)?;
    match action.action_type {
        SemanticActionKind::Press => locator.press()?,
        SemanticActionKind::Focus => locator.focus()?,
        SemanticActionKind::Blur => locator.blur()?,
        SemanticActionKind::Toggle => locator.toggle()?,
        SemanticActionKind::Select => locator.select()?,
        SemanticActionKind::Expand => locator.expand()?,
        SemanticActionKind::Collapse => locator.collapse()?,
        SemanticActionKind::ShowMenu => locator.show_menu()?,
        SemanticActionKind::ScrollIntoView => locator.scroll_into_view()?,
        SemanticActionKind::SetValue => {
            let text = action.text.as_deref().ok_or_else(|| {
                SemanticActionError::Invalid("set_value requires text".to_owned())
            })?;
            locator.set_value(text)?;
        }
        SemanticActionKind::SetNumericValue => {
            let value = action.numeric_value.ok_or_else(|| {
                SemanticActionError::Invalid("set_numeric_value requires numeric_value".to_owned())
            })?;
            locator.set_numeric_value(value)?;
        }
        SemanticActionKind::TypeText => {
            let text = action.text.as_deref().ok_or_else(|| {
                SemanticActionError::Invalid("type_text requires text".to_owned())
            })?;
            locator.type_text(text)?;
        }
        SemanticActionKind::SelectText => {
            let text = action.text.as_deref().ok_or_else(|| {
                SemanticActionError::Invalid("select_text requires text range START,END".to_owned())
            })?;
            let (start, end) = parse_text_range(text)?;
            locator.select_text(start, end)?;
        }
        SemanticActionKind::PerformAction => {
            let action_name = action.action_name.as_deref().ok_or_else(|| {
                SemanticActionError::Invalid("perform_action requires action_name".to_owned())
            })?;
            locator.perform_action(action_name)?;
        }
        SemanticActionKind::WaitFor => {
            let condition = action.condition.as_deref().unwrap_or("visible");
            let timeout = Duration::from_millis(action.wait_ms.unwrap_or(5_000).clamp(1, 60_000));
            match condition {
                "visible" => {
                    locator.wait_visible(timeout)?;
                }
                "attached" | "exists" => {
                    locator.wait_attached(timeout)?;
                }
                "detached" | "removed" => {
                    locator.wait_detached(timeout)?;
                }
                "enabled" => {
                    locator.wait_enabled(timeout)?;
                }
                "disabled" => {
                    locator.wait_disabled(timeout)?;
                }
                "hidden" => {
                    locator.wait_hidden(timeout)?;
                }
                "focused" => {
                    locator.wait_focused(timeout)?;
                }
                "unfocused" => {
                    locator.wait_unfocused(timeout)?;
                }
                other => {
                    return Err(SemanticActionError::Invalid(format!(
                        "unsupported wait_for condition `{}`",
                        other
                    )));
                }
            }
        }
    }
    Ok(format!(
        "Executed semantic action {}",
        action.action_type.as_str()
    ))
}

fn load_xa11y_app(app: &AppHandle) -> Result<xa11y::App, xa11y::Error> {
    if let Some(pid) = app.pid {
        xa11y::App::by_pid(pid, Duration::from_millis(100))
    } else {
        xa11y::App::by_name(app.name.as_str(), Duration::from_millis(100))
    }
}

fn semantic_locator(
    app: &xa11y::App,
    target: Option<&ResolvedActionTarget>,
) -> Result<xa11y::Locator, SemanticActionError> {
    let target = target.ok_or_else(|| {
        SemanticActionError::Invalid("semantic action requires resolved target".to_owned())
    })?;
    let selector = target.selector.as_deref().ok_or_else(|| {
        SemanticActionError::Invalid(
            "semantic action target does not have a selector for xa11y locator re-resolution"
                .to_owned(),
        )
    })?;
    let mut locator = app.locator(selector);
    if let Some(nth) = target.nth {
        locator = locator.nth(nth);
    }
    Ok(locator)
}

fn parse_text_range(value: &str) -> Result<(u32, u32), SemanticActionError> {
    let (start, end) = value.split_once(',').ok_or_else(|| {
        SemanticActionError::Invalid("select_text text must be START,END".to_owned())
    })?;
    let start = start.trim().parse::<u32>().map_err(|_| {
        SemanticActionError::Invalid("select_text START must be an unsigned integer".to_owned())
    })?;
    let end = end.trim().parse::<u32>().map_err(|_| {
        SemanticActionError::Invalid("select_text END must be an unsigned integer".to_owned())
    })?;
    Ok((start, end))
}

fn xa11y_failure_class(error: &xa11y::Error) -> &'static str {
    match error {
        xa11y::Error::PermissionDenied { .. } => "permission_denied",
        xa11y::Error::AccessibilityNotEnabled { .. } => "accessibility_not_enabled",
        xa11y::Error::SelectorNotMatched { .. } => "element_not_found",
        xa11y::Error::ElementStale { .. } => "element_stale",
        xa11y::Error::ActionNotSupported { .. } | xa11y::Error::TextValueNotSupported => {
            "action_not_supported"
        }
        xa11y::Error::Timeout { .. } => "element_not_found",
        xa11y::Error::InvalidSelector { .. }
        | xa11y::Error::InvalidActionData { .. }
        | xa11y::Error::InvalidConfig { .. } => "runtime_action_error",
        xa11y::Error::NoElementBounds | xa11y::Error::Unsupported { .. } => {
            "accessibility_unavailable"
        }
        xa11y::Error::Platform { .. } => "runtime_action_error",
    }
}

fn preflight_capability_error(capability: &str, error: &xa11y::Error) -> String {
    let base = format!("{capability} unavailable: {error}");
    if cfg!(target_os = "macos") {
        format!("{}. {}", base, permissions::macos_permission_guidance())
    } else {
        base
    }
}

#[derive(Debug)]
enum InputActionError {
    Invalid(String),
    Xa11y(xa11y::Error),
}

impl From<xa11y::Error> for InputActionError {
    fn from(value: xa11y::Error) -> Self {
        Self::Xa11y(value)
    }
}

fn execute_input_action(
    action: &InputAction,
    targets: &ResolvedInputActionTargets,
) -> Result<String, InputActionError> {
    let input = init_xa11y_input_with_permission_guidance()?;
    match action.action_type {
        InputActionKind::InputClick
        | InputActionKind::InputDoubleClick
        | InputActionKind::InputRightClick => {
            let point = point_from_resolved_target(targets.target.as_ref(), "input click target")?;
            let (button, count) = match action.action_type {
                InputActionKind::InputDoubleClick => (MouseButton::Left, 2),
                InputActionKind::InputRightClick => (MouseButton::Right, 1),
                _ => (
                    to_xa11y_mouse_button(action.button.unwrap_or(MouseButtonKind::Left)),
                    1,
                ),
            };
            input.mouse().click_with(
                ClickTarget::Point(point),
                ClickOptions {
                    button,
                    count,
                    held: Vec::new(),
                    anchor: Anchor::Center,
                },
            )?;
        }
        InputActionKind::InputMove => {
            let point = point_from_resolved_target(targets.target.as_ref(), "input_move target")?;
            input.mouse().move_to(point)?;
        }
        InputActionKind::InputDrag => {
            let from = point_from_resolved_target(targets.from.as_ref(), "input_drag.from")?;
            let to = point_from_resolved_target(targets.to.as_ref(), "input_drag.to")?;
            input.mouse().drag_with(
                from,
                to,
                DragOptions {
                    button: to_xa11y_mouse_button(action.button.unwrap_or(MouseButtonKind::Left)),
                    held: Vec::new(),
                    duration: Duration::from_millis(action.wait_ms.unwrap_or(150).clamp(1, 10_000)),
                },
            )?;
        }
        InputActionKind::InputScroll => {
            let point = targets
                .target
                .as_ref()
                .map(|target| point_from_resolved_target(Some(target), "input_scroll target"))
                .transpose()?
                .unwrap_or_else(|| Point::new(0, 0));
            input.mouse().scroll(
                point,
                ScrollDelta::new(action.delta_x.unwrap_or(0), action.delta_y.unwrap_or(0)),
            )?;
        }
        InputActionKind::InputKey => {
            let key = parse_single_key(action)?;
            input.keyboard().press(key)?;
        }
        InputActionKind::InputChord => {
            let keys = parse_keys(action)?;
            let last_index = keys.len().saturating_sub(1);
            let held = keys[..last_index].to_vec();
            input
                .keyboard()
                .chord(keys[last_index].clone(), held.as_slice())?;
        }
        InputActionKind::InputTypeText => {
            let text = action.text.as_deref().ok_or_else(|| {
                InputActionError::Invalid("input_type_text requires text".to_owned())
            })?;
            input.keyboard().type_text(text)?;
        }
        InputActionKind::Wait => {}
    }
    Ok(format!(
        "Executed input action {}",
        action.action_type.as_str()
    ))
}

fn init_xa11y_input_with_permission_guidance() -> Result<InputSim, InputActionError> {
    xa11y::input_sim().map_err(InputActionError::Xa11y)
}

fn point_from_resolved_target(
    target: Option<&ResolvedActionTarget>,
    label: &str,
) -> Result<Point, InputActionError> {
    let target = target.ok_or_else(|| InputActionError::Invalid(format!("{label} is required")))?;
    if let Some(point) = target.point.as_ref() {
        return Ok(Point::new(point.x, point.y));
    }
    if let Some(bounds) = target.bounds.as_ref() {
        return Ok(Point::new(
            bounds.x.saturating_add((bounds.width as i32) / 2),
            bounds.y.saturating_add((bounds.height as i32) / 2),
        ));
    }
    Err(InputActionError::Invalid(format!(
        "{label} must resolve to point or bounded element"
    )))
}

fn parse_single_key(action: &InputAction) -> Result<Key, InputActionError> {
    let keys = action
        .keys
        .as_ref()
        .ok_or_else(|| InputActionError::Invalid("input_key requires keys".to_owned()))?;
    let key = keys
        .first()
        .ok_or_else(|| InputActionError::Invalid("input_key requires one key".to_owned()))?;
    parse_xa11y_key(key).map_err(|error| InputActionError::Invalid(error.to_string()))
}

fn parse_keys(action: &InputAction) -> Result<Vec<Key>, InputActionError> {
    let keys = action
        .keys
        .as_ref()
        .ok_or_else(|| InputActionError::Invalid("input_chord requires keys".to_owned()))?;
    keys.iter()
        .map(|key| {
            parse_xa11y_key(key).map_err(|error| InputActionError::Invalid(error.to_string()))
        })
        .collect()
}

fn to_xa11y_mouse_button(button: MouseButtonKind) -> MouseButton {
    match button {
        MouseButtonKind::Left => MouseButton::Left,
        MouseButtonKind::Right => MouseButton::Right,
        MouseButtonKind::Middle => MouseButton::Middle,
    }
}

fn input_failure_class(error: &xa11y::Error) -> &'static str {
    match error {
        xa11y::Error::PermissionDenied { .. }
        | xa11y::Error::Unsupported { .. }
        | xa11y::Error::AccessibilityNotEnabled { .. } => "input_simulation_unavailable",
        xa11y::Error::InvalidActionData { .. } => "runtime_action_error",
        xa11y::Error::Platform { .. } => "runtime_action_error",
        _ => "runtime_action_error",
    }
}

fn to_tool_error(op: &'static str) -> impl FnOnce(xcap::XCapError) -> ToolError {
    move |error| ToolError::execution_failed(format!("{op} failed: {error}"))
}

#[allow(dead_code)]
fn capture_xa11y_primary_screen(
    snapshot_budget: &SnapshotBudget,
) -> Result<CapturedFrame, ToolError> {
    let screenshot = xa11y::screenshot().map_err(to_xa11y_error("screenshot"))?;
    captured_frame_from_rgba_buffer(
        screenshot.width,
        screenshot.height,
        screenshot.scale,
        screenshot.pixels,
        snapshot_budget,
    )
}

fn to_xa11y_error(op: &'static str) -> impl FnOnce(xa11y::Error) -> ToolError {
    move |error| ToolError::execution_failed(format!("{op} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computer_use_semantic_action_maps_action_not_supported_failure() {
        let error = xa11y::Error::ActionNotSupported {
            action: "press".to_owned(),
            role: xa11y::Role::Button,
        };
        assert_eq!(xa11y_failure_class(&error), "action_not_supported");
    }

    #[test]
    fn computer_use_semantic_action_parses_select_text_range() {
        assert_eq!(parse_text_range("1,5").expect("range"), (1, 5));
        assert!(parse_text_range("5").is_err());
    }

    #[test]
    fn computer_use_input_action_maps_unsupported_to_unavailable() {
        let error = xa11y::Error::Unsupported {
            feature: "input simulation".to_owned(),
        };
        assert_eq!(input_failure_class(&error), "input_simulation_unavailable");
    }

    #[test]
    fn preflight_capability_error_is_actionable() {
        let error = xa11y::Error::Unsupported {
            feature: "screenshot".to_owned(),
        };
        let message = preflight_capability_error("screenshot", &error);
        assert!(message.contains("screenshot unavailable"));
        if cfg!(target_os = "macos") {
            assert!(message.contains("Accessibility"));
            assert!(message.contains("Screen Recording"));
            assert!(message.contains("restart the gateway"));
        }
    }

    #[test]
    fn menu_action_unsupported_is_structured() {
        let backend = Xa11yComputerUseBackend;
        let result = backend
            .perform_os_action(&OsAction {
                action_type: OsActionKind::SelectMenuItem,
                app: Some("ExampleApp".to_owned()),
                path: None,
                url: None,
                menu_path: Some(vec!["File".to_owned(), "New Window".to_owned()]),
                title: None,
            })
            .expect("menu action result");

        assert_eq!(result.status, "failed");
        assert_eq!(
            result.failure_class.as_deref(),
            Some(ComputerUseFailureClass::ActionNotSupported.as_str())
        );
        assert!(
            result
                .details
                .as_ref()
                .and_then(|details| details.pointer("/expected_after/state"))
                .and_then(serde_json::Value::as_str)
                == Some("menu_item_selected")
        );
    }
}
