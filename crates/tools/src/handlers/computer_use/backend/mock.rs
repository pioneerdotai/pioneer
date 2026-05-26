use super::super::model::{
    AccessibilityBounds, ActionExecution, AppHandle, AppHandleMeta, AppMeta, AppTarget,
    CapturedFrame, ComputerUseFailureClass, DesktopPreflightCapabilities, DesktopPreflightOptions,
    DesktopPreflightReport, DesktopTree, DisplayMeta, InputAction, InputActionKind, OsAction,
    OsActionKind, ResolvedActionTarget, ResolvedInputActionTargets, SemanticAction,
    SemanticActionKind, SnapshotBudget, SnapshotTarget, derive_app_identity_key,
};
use super::super::platform;
use super::super::tree::{AccessibilityTreeBudget, RawAccessibilityNode, compact_raw_tree};
use super::ComputerUseDesktopBackend;
use super::screenshots::captured_frame_from_rgba_image;
use crate::error::ToolError;
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub(crate) struct MockComputerUseBackend {
    action_count: AtomicUsize,
    preflight_status: String,
    launched_apps: Mutex<HashSet<String>>,
    os_actions: Mutex<Vec<String>>,
    apps: Mutex<Vec<AppMeta>>,
    unsupported_semantic_actions: HashSet<String>,
    extra_button: bool,
    scale_factor: f32,
}

impl MockComputerUseBackend {
    pub(crate) fn with_preflight_status(status: impl Into<String>) -> Self {
        Self {
            action_count: AtomicUsize::new(0),
            preflight_status: status.into(),
            launched_apps: Mutex::new(HashSet::new()),
            os_actions: Mutex::new(Vec::new()),
            apps: Mutex::new(vec![platform::enrich_app_identity(AppMeta {
                identity_key: Some(derive_app_identity_key(
                    "MockApp",
                    Some(42),
                    Some("com.pioneer.mockapp"),
                    None,
                )),
                name: "MockApp".to_owned(),
                pid: Some(42),
                role: Some("application".to_owned()),
                window_title: Some("Mock Window".to_owned()),
                bundle_id: Some("com.pioneer.mockapp".to_owned()),
                localized_name: None,
                executable_path: None,
                frontmost: Some(true),
            })]),
            unsupported_semantic_actions: HashSet::new(),
            extra_button: false,
            scale_factor: 2.0,
        }
    }

    pub(crate) fn with_scale_factor(mut self, scale_factor: f32) -> Self {
        self.scale_factor = scale_factor;
        self
    }

    pub(crate) fn with_unsupported_semantic_actions<I, S>(mut self, actions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.unsupported_semantic_actions = actions.into_iter().map(Into::into).collect();
        self
    }

    pub(crate) fn with_extra_button(mut self) -> Self {
        self.extra_button = true;
        self
    }

    pub(crate) fn with_apps(mut self, apps: Vec<AppMeta>) -> Self {
        self.apps = Mutex::new(
            apps.into_iter()
                .map(platform::enrich_app_identity)
                .collect(),
        );
        self
    }

    pub(crate) fn os_action_types(&self) -> Vec<String> {
        self.os_actions
            .lock()
            .map(|actions| actions.clone())
            .unwrap_or_default()
    }

    fn display_with_scale(scale_factor: f32) -> DisplayMeta {
        DisplayMeta {
            display_id: 1,
            width_px: 640,
            height_px: 360,
            scale_factor,
            origin_x: 0,
            origin_y: 0,
            is_primary: true,
        }
    }

    fn rgba_image(seed: u8) -> image::RgbaImage {
        image::RgbaImage::from_pixel(
            640,
            360,
            image::Rgba([seed, seed.saturating_add(1), seed.saturating_add(2), 255]),
        )
    }
}

fn mock_tree(action_count: usize, extra_button: bool) -> RawAccessibilityNode {
    let action_happened = action_count > 0;
    let mut button_states = vec![
        "visible".to_owned(),
        "enabled".to_owned(),
        "focusable".to_owned(),
    ];
    if action_happened {
        button_states.push("focused".to_owned());
        button_states.push("selected".to_owned());
    }
    let mut children = vec![RawAccessibilityNode {
        role: "button".to_owned(),
        name: Some("OK".to_owned()),
        value: action_happened.then(|| format!("action_count={action_count}")),
        description: Some("Submit".to_owned()),
        bounds: Some(AccessibilityBounds {
            x: 10,
            y: 20,
            width: 80,
            height: 32,
        }),
        states: button_states,
        actions: vec!["press".to_owned()],
        stable_id: Some("mock-ok-button".to_owned()),
        children: Vec::new(),
    }];
    if extra_button {
        children.push(RawAccessibilityNode {
            role: "button".to_owned(),
            name: Some("Cancel".to_owned()),
            value: None,
            description: Some("Cancel".to_owned()),
            bounds: Some(AccessibilityBounds {
                x: 120,
                y: 20,
                width: 90,
                height: 32,
            }),
            states: vec![
                "visible".to_owned(),
                "enabled".to_owned(),
                "focusable".to_owned(),
            ],
            actions: vec!["press".to_owned()],
            stable_id: Some("mock-cancel-button".to_owned()),
            children: Vec::new(),
        });
    }
    RawAccessibilityNode {
        role: "application".to_owned(),
        name: Some("MockApp".to_owned()),
        value: None,
        description: None,
        bounds: Some(AccessibilityBounds {
            x: 0,
            y: 0,
            width: 640,
            height: 360,
        }),
        states: vec!["visible".to_owned(), "enabled".to_owned()],
        actions: Vec::new(),
        stable_id: Some("mock-app".to_owned()),
        children,
    }
}

impl Default for MockComputerUseBackend {
    fn default() -> Self {
        Self::with_preflight_status("ready")
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

impl ComputerUseDesktopBackend for MockComputerUseBackend {
    fn preflight(
        &self,
        options: DesktopPreflightOptions,
    ) -> Result<DesktopPreflightReport, ToolError> {
        let mut capabilities = DesktopPreflightCapabilities {
            accessibility_tree: "ok".to_owned(),
            accessibility_actions: "ok".to_owned(),
            screenshot: if options.screenshot_probe_enabled {
                "ok".to_owned()
            } else {
                "skipped".to_owned()
            },
            input_simulation: if options.input_simulation_enabled {
                "ok".to_owned()
            } else {
                "disabled".to_owned()
            },
        };
        let mut blocking_issues = Vec::new();
        let mut warnings = Vec::new();
        match self.preflight_status.as_str() {
            "blocked" => {
                capabilities.accessibility_tree = "blocked".to_owned();
                capabilities.accessibility_actions = "blocked".to_owned();
                blocking_issues.push("mock accessibility is blocked".to_owned());
            }
            "degraded" => {
                if options.input_simulation_enabled {
                    capabilities.input_simulation = "blocked".to_owned();
                    warnings.push("mock input simulation is blocked".to_owned());
                }
            }
            _ => {}
        }
        Ok(DesktopPreflightReport {
            platform: "test".to_owned(),
            status: self.preflight_status.clone(),
            message: format!("mock backend {}", self.preflight_status),
            capabilities,
            blocking_issues,
            warnings,
        })
    }

    fn list_apps(&self) -> Result<Vec<AppMeta>, ToolError> {
        self.apps
            .lock()
            .map(|apps| apps.clone())
            .map_err(|_| ToolError::internal("mock apps lock poisoned"))
    }

    fn frontmost_app(&self) -> Result<Option<AppMeta>, ToolError> {
        let apps = self
            .apps
            .lock()
            .map_err(|_| ToolError::internal("mock apps lock poisoned"))?;
        Ok(apps.iter().find(|app| app.frontmost == Some(true)).cloned())
    }

    fn find_app(&self, target: &AppTarget, _timeout: Duration) -> Result<AppHandle, ToolError> {
        let launched = self
            .launched_apps
            .lock()
            .map_err(|_| ToolError::internal("mock launched_apps lock poisoned"))?;
        let missing_name =
            target.name.as_deref() == Some("MissingApp") && !launched.contains("MissingApp");
        drop(launched);
        if missing_name || target.pid == Some(404) {
            return Err(ToolError::NotFound("mock app not found".to_owned()));
        }
        let apps = self
            .apps
            .lock()
            .map_err(|_| ToolError::internal("mock apps lock poisoned"))?;
        if let Some(pid) = target.pid {
            if let Some(app) = apps.iter().find(|app| app.pid == Some(pid)) {
                return Ok(app_handle_from_meta(app.clone()));
            }
        }
        if let Some(name) = target.name.as_deref() {
            if let Some(app) = apps
                .iter()
                .find(|app| platform::app_identity_matches(app, name))
            {
                return Ok(app_handle_from_meta(app.clone()));
            }
        }
        Err(ToolError::NotFound("mock app not found".to_owned()))
    }

    fn launch_app(
        &self,
        target: &AppTarget,
        _launch_command: Option<&str>,
    ) -> Result<(), ToolError> {
        if let Some(name) = target.name.as_deref() {
            self.launched_apps
                .lock()
                .map_err(|_| ToolError::internal("mock launched_apps lock poisoned"))?
                .insert(name.to_owned());
            let mut apps = self
                .apps
                .lock()
                .map_err(|_| ToolError::internal("mock apps lock poisoned"))?;
            if apps
                .iter()
                .all(|app| !platform::app_identity_matches(app, name))
            {
                apps.push(platform::enrich_app_identity(AppMeta {
                    identity_key: Some(derive_app_identity_key(name, Some(43), None, None)),
                    name: name.to_owned(),
                    pid: Some(43),
                    role: Some("application".to_owned()),
                    window_title: Some("Mock Window".to_owned()),
                    bundle_id: None,
                    localized_name: None,
                    executable_path: None,
                    frontmost: Some(true),
                }));
            }
        }
        Ok(())
    }

    fn activate_app(&self, _app: &AppHandle) -> Result<(), ToolError> {
        Ok(())
    }

    fn app_tree(
        &self,
        _app: &AppHandle,
        budget: AccessibilityTreeBudget,
    ) -> Result<DesktopTree, ToolError> {
        compact_raw_tree(
            &mock_tree(self.action_count.load(Ordering::SeqCst), self.extra_button),
            budget,
        )
    }

    fn screenshot(
        &self,
        target: &SnapshotTarget,
        snapshot_budget: &SnapshotBudget,
    ) -> Result<CapturedFrame, ToolError> {
        let display_id = match target {
            SnapshotTarget::PrimaryScreen => 1,
            SnapshotTarget::Display { display_id } => *display_id,
        };
        self.capture_display(display_id, snapshot_budget)
    }

    fn perform_semantic_action(
        &self,
        _app: &AppHandle,
        action: &SemanticAction,
        _target: Option<&ResolvedActionTarget>,
    ) -> Result<ActionExecution, ToolError> {
        if self
            .unsupported_semantic_actions
            .contains(action.action_type.as_str())
        {
            return Ok(ActionExecution {
                status: "failed".to_owned(),
                message: format!(
                    "mock semantic action {} is unsupported",
                    action.action_type.as_str()
                ),
                action_type: Some(action.action_type.as_str().to_owned()),
                target: _target.cloned(),
                duration_ms: Some(0),
                failure_class: Some(
                    ComputerUseFailureClass::ActionNotSupported
                        .as_str()
                        .to_owned(),
                ),
                app_before: None,
                app_after: None,
                details: None,
            });
        }
        self.action_count.fetch_add(1, Ordering::SeqCst);
        Ok(ActionExecution {
            status: "ok".to_owned(),
            message: format!("mock semantic action {}", action.action_type.as_str()),
            action_type: Some(action.action_type.as_str().to_owned()),
            target: _target.cloned(),
            duration_ms: Some(0),
            failure_class: None,
            app_before: None,
            app_after: None,
            details: None,
        })
    }

    fn perform_input_action(
        &self,
        action: &InputAction,
        _targets: &ResolvedInputActionTargets,
    ) -> Result<ActionExecution, ToolError> {
        self.action_count.fetch_add(1, Ordering::SeqCst);
        Ok(ActionExecution {
            status: "ok".to_owned(),
            message: format!("mock input action {}", action.action_type.as_str()),
            action_type: Some(action.action_type.as_str().to_owned()),
            target: _targets.target.clone(),
            duration_ms: Some(0),
            failure_class: None,
            app_before: None,
            app_after: None,
            details: None,
        })
    }

    fn perform_os_action(&self, action: &OsAction) -> Result<ActionExecution, ToolError> {
        self.action_count.fetch_add(1, Ordering::SeqCst);
        let action_type = action.action_type.as_str().to_owned();
        self.os_actions
            .lock()
            .map_err(|_| ToolError::internal("mock os_actions lock poisoned"))?
            .push(action_type.clone());
        if action.app.as_deref() == Some("UnlaunchableApp") {
            return Ok(ActionExecution {
                status: "failed".to_owned(),
                message: "mock app not found after OS action".to_owned(),
                action_type: Some(action_type),
                target: None,
                duration_ms: Some(0),
                failure_class: Some(ComputerUseFailureClass::AppNotFound.as_str().to_owned()),
                app_before: None,
                app_after: None,
                details: Some(serde_json::json!({
                    "app_before": null,
                    "app_after": null,
                    "pid": null,
                    "resolved_name": null,
                    "localized_name": null,
                })),
            });
        }
        let app_after = matches!(
            action.action_type,
            OsActionKind::OpenApp | OsActionKind::ActivateApp | OsActionKind::FocusWindow
        )
        .then(|| AppHandleMeta {
            identity_key: Some(derive_app_identity_key(
                action.app.as_deref().unwrap_or("MockApp"),
                Some(42),
                None,
                None,
            )),
            name: action.app.clone().unwrap_or_else(|| "MockApp".to_owned()),
            pid: Some(42),
            role: Some("application".to_owned()),
            window_title: Some("Mock Window".to_owned()),
            bundle_id: None,
            localized_name: None,
            executable_path: None,
            frontmost: Some(true),
        });
        Ok(ActionExecution {
            status: "ok".to_owned(),
            message: format!("mock OS action {}", action.action_type.as_str()),
            action_type: Some(action_type),
            target: None,
            duration_ms: Some(0),
            failure_class: None,
            app_before: None,
            app_after: app_after.clone(),
            details: Some(if action.action_type == OsActionKind::SelectMenuItem {
                serde_json::json!({
                    "expected_after": {
                        "app": action.app.clone(),
                        "menu_path": action.menu_path.clone(),
                        "state": "menu_item_selected"
                    },
                    "verification_hint": "Take a fresh snapshot and verify the menu action effect is visible."
                })
            } else if matches!(
                action.action_type,
                OsActionKind::OpenPath | OsActionKind::RevealPath | OsActionKind::OpenUrl
            ) {
                serde_json::json!({
                    "path": action.path.clone(),
                    "url": action.url.clone(),
                    "expected_app": if action.action_type == OsActionKind::OpenUrl {
                        "system_url_handler"
                    } else {
                        "system_file_manager"
                    },
                    "verification_hint": "Take a fresh snapshot and verify the expected system handler, window, or URL content is visible."
                })
            } else {
                serde_json::json!({
                "app_before": null,
                "app_after": app_after.clone(),
                "pid": app_after.as_ref().and_then(|app| app.pid),
                "resolved_name": app_after.as_ref().map(|app| app.name.clone()),
                "localized_name": app_after.as_ref().and_then(|app| app.localized_name.clone()),
                "expected_after": {
                    "app": action.app.clone(),
                    "title": action.title.clone(),
                    "state": if action.action_type == OsActionKind::FocusWindow {
                        "window_focused"
                    } else {
                        "app_active"
                    }
                },
                })
            }),
        })
    }

    fn list_displays(&self) -> Result<Vec<DisplayMeta>, ToolError> {
        Ok(vec![Self::display_with_scale(self.scale_factor)])
    }

    fn capture_display(
        &self,
        _display_id: u32,
        snapshot_budget: &SnapshotBudget,
    ) -> Result<CapturedFrame, ToolError> {
        let seed = self.action_count.load(Ordering::SeqCst) as u8;
        captured_frame_from_rgba_image(Self::rgba_image(seed), self.scale_factor, snapshot_budget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_backend_covers_wp04_backend_surfaces() {
        let backend = MockComputerUseBackend::default();
        let preflight = backend
            .preflight(DesktopPreflightOptions {
                screenshot_probe_enabled: true,
                input_simulation_enabled: true,
            })
            .expect("preflight");
        assert_eq!(preflight.status, "ready");
        assert_eq!(backend.list_apps().expect("apps").len(), 1);
        let app = backend
            .find_app(
                &AppTarget {
                    name: Some("MockApp".to_owned()),
                    pid: None,
                    identity_key: None,
                    bundle_id: None,
                    executable_path: None,
                },
                Duration::ZERO,
            )
            .expect("app");
        assert_eq!(app.name, "MockApp");
        assert!(
            !backend
                .app_tree(&app, test_tree_budget())
                .expect("tree")
                .payload
                .nodes
                .is_empty()
        );
        let screenshot = backend
            .screenshot(&SnapshotTarget::PrimaryScreen, &test_snapshot_budget())
            .expect("screenshot");
        assert!(!screenshot.png_bytes.is_empty());
        assert_eq!(
            backend
                .perform_semantic_action(
                    &app,
                    &SemanticAction {
                        action_type: SemanticActionKind::Press,
                        target: None,
                        text: None,
                        numeric_value: None,
                        action_name: None,
                        condition: None,
                        wait_ms: None,
                    },
                    None
                )
                .expect("semantic")
                .status,
            "ok"
        );
        assert_eq!(
            backend
                .perform_input_action(
                    &InputAction {
                        action_type: InputActionKind::InputClick,
                        target: None,
                        from: None,
                        to: None,
                        button: None,
                        delta_x: None,
                        delta_y: None,
                        text: None,
                        keys: None,
                        wait_ms: None,
                    },
                    &ResolvedInputActionTargets::default(),
                )
                .expect("input")
                .status,
            "ok"
        );
        assert_eq!(
            backend
                .perform_os_action(&OsAction {
                    action_type: OsActionKind::OpenApp,
                    app: Some("MockApp".to_owned()),
                    path: None,
                    url: None,
                    menu_path: None,
                    title: None,
                })
                .expect("os")
                .status,
            "ok"
        );
        assert_eq!(backend.os_action_types(), vec!["open_app".to_owned()]);
    }

    fn test_snapshot_budget() -> SnapshotBudget {
        SnapshotBudget {
            provider_hint: None,
            model_hint: None,
            profile: "test".to_owned(),
            max_bytes: 8 * 1024 * 1024,
            max_side_px: 1280,
            min_side_px: 320,
            downscale_factor: 0.85,
        }
    }

    fn test_tree_budget() -> AccessibilityTreeBudget {
        AccessibilityTreeBudget {
            max_depth: 4,
            max_nodes: 20,
            max_serialized_bytes: 64 * 1024,
            text_max_chars: 160,
        }
    }
}
