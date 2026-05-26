use super::model::{
    ActionTarget, ComputerUseActArgs, ComputerUseAction, InputAction, InputActionKind,
    MAX_TEXT_CHARS, MouseButtonKind, OsAction, OsActionKind, SemanticAction, SemanticActionKind,
};
use super::platform;
use crate::error::ToolError;
use xa11y::Key;

pub(crate) fn parse_computer_use_action(
    act: ComputerUseActArgs,
) -> Result<ComputerUseAction, ToolError> {
    let kind = act.kind.trim().to_ascii_lowercase();
    if let Some(action_type) = parse_os_action_kind(kind.as_str()) {
        return parse_os_action(action_type, act).map(ComputerUseAction::Os);
    }
    if let Some(action_type) = parse_semantic_action_kind(kind.as_str()) {
        return parse_semantic_action(action_type, act).map(ComputerUseAction::Semantic);
    }
    if let Some(action_type) = parse_input_action_kind(kind.as_str()) {
        return parse_input_action(action_type, act).map(ComputerUseAction::Input);
    }
    Err(ToolError::invalid_arguments(format!(
        "unsupported act.type `{}`; supported OS actions: open_app, activate_app, open_path, reveal_path, open_url, select_menu_item, focus_window; supported semantic actions: press, focus, blur, toggle, select, expand, collapse, show_menu, scroll_into_view, set_value, set_numeric_value, type_text, select_text, perform_action, wait_for; explicit input actions: input_click, input_double_click, input_right_click, input_move, input_drag, input_scroll, input_key, input_chord, input_type_text, wait",
        kind
    )))
}

fn parse_os_action(
    action_type: OsActionKind,
    act: ComputerUseActArgs,
) -> Result<OsAction, ToolError> {
    reject_non_os_action_fields(action_type.as_str(), &act)?;
    let app = normalized_optional_string(act.app.as_deref());
    let raw_path = normalized_optional_string(act.path.as_deref());
    let raw_url = normalized_optional_string(act.url.as_deref());
    let title = normalized_optional_string(act.title.as_deref());
    let menu_path = normalize_menu_path(act.menu_path.as_ref())?;

    let (path, url) = match action_type {
        OsActionKind::OpenApp | OsActionKind::ActivateApp => {
            require_non_empty_os_field(action_type.as_str(), "app", app.as_deref())?;
            reject_unexpected_os_field(action_type.as_str(), "path", raw_path.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "url", raw_url.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "menu_path", menu_path.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "title", title.is_some())?;
            (None, None)
        }
        OsActionKind::OpenPath | OsActionKind::RevealPath => {
            let raw_path = raw_path.as_deref().ok_or_else(|| {
                ToolError::invalid_arguments(format!(
                    "{} requires non-empty path",
                    action_type.as_str()
                ))
            })?;
            reject_unexpected_os_field(action_type.as_str(), "app", app.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "url", raw_url.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "menu_path", menu_path.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "title", title.is_some())?;
            let path = platform::normalize_existing_path(raw_path)?;
            (Some(path.display().to_string()), None)
        }
        OsActionKind::OpenUrl => {
            let raw_url = raw_url.as_deref().ok_or_else(|| {
                ToolError::invalid_arguments(format!(
                    "{} requires non-empty url",
                    action_type.as_str()
                ))
            })?;
            reject_unexpected_os_field(action_type.as_str(), "app", app.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "path", raw_path.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "menu_path", menu_path.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "title", title.is_some())?;
            let url = platform::normalize_open_url(raw_url)?;
            (None, Some(url.to_string()))
        }
        OsActionKind::SelectMenuItem => {
            require_non_empty_os_field(action_type.as_str(), "app", app.as_deref())?;
            if menu_path.as_ref().map_or(true, Vec::is_empty) {
                return Err(ToolError::invalid_arguments(
                    "select_menu_item requires non-empty menu_path",
                ));
            }
            reject_unexpected_os_field(action_type.as_str(), "path", raw_path.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "url", raw_url.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "title", title.is_some())?;
            (None, None)
        }
        OsActionKind::FocusWindow => {
            require_non_empty_os_field(action_type.as_str(), "app", app.as_deref())?;
            reject_unexpected_os_field(action_type.as_str(), "path", raw_path.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "url", raw_url.is_some())?;
            reject_unexpected_os_field(action_type.as_str(), "menu_path", menu_path.is_some())?;
            (None, None)
        }
    };

    Ok(OsAction {
        action_type,
        app,
        path,
        url,
        menu_path,
        title,
    })
}

fn parse_semantic_action(
    action_type: SemanticActionKind,
    act: ComputerUseActArgs,
) -> Result<SemanticAction, ToolError> {
    reject_os_action_fields(action_type.as_str(), &act)?;
    if act
        .target
        .as_ref()
        .is_some_and(|target| target.point.is_some())
    {
        return Err(ToolError::invalid_arguments(
            "semantic actions do not accept target.point; use explicit input_* actions for coordinates",
        ));
    }

    match action_type {
        SemanticActionKind::WaitFor => {
            if normalized_optional_string(act.condition.as_deref()).is_none()
                && act.target.is_none()
            {
                return Err(ToolError::invalid_arguments(
                    "wait_for requires condition or target",
                ));
            }
        }
        _ => require_target(action_type.as_str(), act.target.as_ref())?,
    }

    let text = normalized_optional_string(act.text.as_deref());
    match action_type {
        SemanticActionKind::SetValue
        | SemanticActionKind::TypeText
        | SemanticActionKind::SelectText => {
            let text_value = text.as_deref().ok_or_else(|| {
                ToolError::invalid_arguments(format!(
                    "{} requires non-empty text",
                    action_type.as_str()
                ))
            })?;
            validate_text_length(text_value)?;
        }
        _ => {}
    }

    if action_type == SemanticActionKind::SetNumericValue {
        let value = act.numeric_value.ok_or_else(|| {
            ToolError::invalid_arguments("set_numeric_value requires numeric_value")
        })?;
        if !value.is_finite() {
            return Err(ToolError::invalid_arguments(
                "set_numeric_value numeric_value must be finite",
            ));
        }
    }

    let action_name = normalized_optional_string(act.action_name.as_deref());
    if action_type == SemanticActionKind::PerformAction && action_name.is_none() {
        return Err(ToolError::invalid_arguments(
            "perform_action requires action_name",
        ));
    }

    Ok(SemanticAction {
        action_type,
        target: act.target,
        text,
        numeric_value: act.numeric_value,
        action_name,
        condition: normalized_optional_string(act.condition.as_deref()),
        wait_ms: act.wait_ms.map(|value| value.clamp(1, 60_000)),
    })
}

fn parse_input_action(
    action_type: InputActionKind,
    act: ComputerUseActArgs,
) -> Result<InputAction, ToolError> {
    reject_os_action_fields(action_type.as_str(), &act)?;
    match action_type {
        InputActionKind::Wait => {}
        InputActionKind::InputDrag => {
            require_target("input_drag.from", act.from.as_ref())?;
            require_target("input_drag.to", act.to.as_ref())?;
        }
        InputActionKind::InputKey | InputActionKind::InputChord => {
            let keys = act.keys.as_ref().ok_or_else(|| {
                ToolError::invalid_arguments(format!("{} requires keys", action_type.as_str()))
            })?;
            if keys.is_empty() {
                return Err(ToolError::invalid_arguments(format!(
                    "{} requires at least one key",
                    action_type.as_str()
                )));
            }
            if action_type == InputActionKind::InputKey && keys.len() != 1 {
                return Err(ToolError::invalid_arguments(
                    "input_key requires exactly one key; use input_chord for multi-key hotkeys",
                ));
            }
            if keys.len() > 5 {
                return Err(ToolError::invalid_arguments(
                    "input_chord supports at most 5 keys; use input_key for a single key",
                ));
            }
        }
        InputActionKind::InputTypeText => {
            let text = normalized_optional_string(act.text.as_deref()).ok_or_else(|| {
                ToolError::invalid_arguments("input_type_text requires non-empty text")
            })?;
            validate_text_length(text.as_str())?;
        }
        InputActionKind::InputScroll => {
            let delta_x = act.delta_x.unwrap_or(0);
            let delta_y = act.delta_y.unwrap_or(0);
            if delta_x == 0 && delta_y == 0 {
                return Err(ToolError::invalid_arguments(
                    "input_scroll requires non-zero delta_x or delta_y",
                ));
            }
        }
        _ => require_target(action_type.as_str(), act.target.as_ref())?,
    }

    Ok(InputAction {
        action_type,
        target: act.target,
        from: act.from,
        to: act.to,
        button: parse_mouse_button(act.button.as_deref())?,
        delta_x: act.delta_x,
        delta_y: act.delta_y,
        text: normalized_optional_string(act.text.as_deref()),
        keys: act.keys,
        wait_ms: act.wait_ms.map(|value| value.clamp(1, 60_000)),
    })
}

fn parse_os_action_kind(value: &str) -> Option<OsActionKind> {
    Some(match value {
        "open_app" => OsActionKind::OpenApp,
        "activate_app" => OsActionKind::ActivateApp,
        "open_path" => OsActionKind::OpenPath,
        "reveal_path" => OsActionKind::RevealPath,
        "open_url" => OsActionKind::OpenUrl,
        "select_menu_item" => OsActionKind::SelectMenuItem,
        "focus_window" => OsActionKind::FocusWindow,
        _ => return None,
    })
}

fn parse_semantic_action_kind(value: &str) -> Option<SemanticActionKind> {
    Some(match value {
        "press" => SemanticActionKind::Press,
        "focus" => SemanticActionKind::Focus,
        "blur" => SemanticActionKind::Blur,
        "toggle" => SemanticActionKind::Toggle,
        "select" => SemanticActionKind::Select,
        "expand" => SemanticActionKind::Expand,
        "collapse" => SemanticActionKind::Collapse,
        "show_menu" => SemanticActionKind::ShowMenu,
        "scroll_into_view" => SemanticActionKind::ScrollIntoView,
        "set_value" => SemanticActionKind::SetValue,
        "set_numeric_value" => SemanticActionKind::SetNumericValue,
        "type_text" => SemanticActionKind::TypeText,
        "select_text" => SemanticActionKind::SelectText,
        "perform_action" => SemanticActionKind::PerformAction,
        "wait_for" => SemanticActionKind::WaitFor,
        _ => return None,
    })
}

fn parse_input_action_kind(value: &str) -> Option<InputActionKind> {
    Some(match value {
        "input_click" => InputActionKind::InputClick,
        "input_double_click" => InputActionKind::InputDoubleClick,
        "input_right_click" => InputActionKind::InputRightClick,
        "input_move" => InputActionKind::InputMove,
        "input_drag" => InputActionKind::InputDrag,
        "input_scroll" => InputActionKind::InputScroll,
        "input_key" => InputActionKind::InputKey,
        "input_chord" => InputActionKind::InputChord,
        "input_type_text" => InputActionKind::InputTypeText,
        "wait" => InputActionKind::Wait,
        _ => return None,
    })
}

fn require_non_empty_os_field(
    action_type: &str,
    field: &str,
    value: Option<&str>,
) -> Result<(), ToolError> {
    if value.is_some_and(|value| !value.trim().is_empty()) {
        return Ok(());
    }
    Err(ToolError::invalid_arguments(format!(
        "{action_type} requires non-empty {field}"
    )))
}

fn reject_os_action_fields(action_type: &str, act: &ComputerUseActArgs) -> Result<(), ToolError> {
    for (field, present) in [
        ("app", act.app.is_some()),
        ("path", act.path.is_some()),
        ("url", act.url.is_some()),
        ("menu_path", act.menu_path.is_some()),
        ("title", act.title.is_some()),
    ] {
        if present {
            return Err(ToolError::invalid_arguments(format!(
                "{action_type} does not accept {field}; use an OS act.type such as open_app, open_path, open_url, select_menu_item, or focus_window"
            )));
        }
    }
    Ok(())
}

fn reject_unexpected_os_field(
    action_type: &str,
    field: &str,
    present: bool,
) -> Result<(), ToolError> {
    if !present {
        return Ok(());
    }
    Err(ToolError::invalid_arguments(format!(
        "{action_type} does not accept {field}"
    )))
}

fn reject_non_os_action_fields(
    action_type: &str,
    act: &ComputerUseActArgs,
) -> Result<(), ToolError> {
    for (field, present) in [
        ("target", act.target.is_some()),
        ("from", act.from.is_some()),
        ("to", act.to.is_some()),
        ("button", act.button.is_some()),
        ("delta_x", act.delta_x.is_some()),
        ("delta_y", act.delta_y.is_some()),
        ("text", act.text.is_some()),
        ("keys", act.keys.is_some()),
        ("numeric_value", act.numeric_value.is_some()),
        ("action_name", act.action_name.is_some()),
        ("condition", act.condition.is_some()),
        ("wait_ms", act.wait_ms.is_some()),
    ] {
        if present {
            return Err(ToolError::invalid_arguments(format!(
                "{action_type} does not accept {field}; use only OS action fields app, path, url, menu_path, and title"
            )));
        }
    }
    Ok(())
}

fn normalize_menu_path(values: Option<&Vec<String>>) -> Result<Option<Vec<String>>, ToolError> {
    let Some(values) = values else {
        return Ok(None);
    };
    let mut normalized = Vec::with_capacity(values.len());
    for value in values {
        let Some(value) = normalized_optional_string(Some(value.as_str())) else {
            return Err(ToolError::invalid_arguments(
                "select_menu_item menu_path entries must be non-empty",
            ));
        };
        normalized.push(value);
    }
    Ok(Some(normalized))
}

fn require_target(action_type: &str, target: Option<&ActionTarget>) -> Result<(), ToolError> {
    let target = target
        .ok_or_else(|| ToolError::invalid_arguments(format!("{} requires target", action_type)))?;
    if target.node_id.is_none()
        && target.selector.is_none()
        && target.role.is_none()
        && target.name.is_none()
        && target.bounds_anchor.is_none()
        && target.point.is_none()
    {
        return Err(ToolError::invalid_arguments(format!(
            "{} target must include node_id, selector, role/name, bounds_anchor, or point",
            action_type
        )));
    }
    Ok(())
}

fn normalized_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn validate_text_length(text: &str) -> Result<(), ToolError> {
    if text.chars().count() > MAX_TEXT_CHARS {
        return Err(ToolError::invalid_arguments(format!(
            "text exceeds max length of {} characters",
            MAX_TEXT_CHARS
        )));
    }
    Ok(())
}

fn parse_mouse_button(button: Option<&str>) -> Result<Option<MouseButtonKind>, ToolError> {
    match button.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "left" => Ok(Some(MouseButtonKind::Left)),
            "right" => Ok(Some(MouseButtonKind::Right)),
            "middle" => Ok(Some(MouseButtonKind::Middle)),
            other => Err(ToolError::invalid_arguments(format!(
                "unsupported mouse button `{}`",
                other
            ))),
        },
    }
}

#[allow(dead_code)]
pub(crate) fn parse_xa11y_key(value: &str) -> Result<Key, ToolError> {
    let trimmed = value.trim();
    let normalized = normalize_hotkey_token(trimmed);
    let key = match normalized.as_str() {
        "ctrl" => Key::Ctrl,
        "shift" => Key::Shift,
        "alt" => Key::Alt,
        "meta" => Key::Meta,
        "enter" => Key::Enter,
        "tab" => Key::Tab,
        "space" => Key::Space,
        "esc" => Key::Escape,
        "backspace" => Key::Backspace,
        "delete" => Key::Delete,
        "up" => Key::ArrowUp,
        "down" => Key::ArrowDown,
        "left" => Key::ArrowLeft,
        "right" => Key::ArrowRight,
        "home" => Key::Home,
        "end" => Key::End,
        "pageup" => Key::PageUp,
        "pagedown" => Key::PageDown,
        function if function.starts_with('f') => {
            let number = function
                .trim_start_matches('f')
                .parse::<u8>()
                .map_err(|_| {
                    ToolError::invalid_arguments(format!("unsupported hotkey token `{}`", value))
                })?;
            if number == 0 {
                return Err(ToolError::invalid_arguments(format!(
                    "unsupported hotkey token `{}`",
                    value
                )));
            }
            Key::F(number)
        }
        _ if trimmed.chars().count() == 1 => {
            Key::Char(trimmed.chars().next().expect("single char"))
        }
        _ => {
            return Err(ToolError::invalid_arguments(format!(
                "unsupported hotkey token `{}`",
                value
            )));
        }
    };
    Ok(key)
}

#[allow(dead_code)]
fn normalize_hotkey_token(value: &str) -> String {
    match value.trim().to_lowercase().as_str() {
        "control" | "ctrl" => "ctrl".to_owned(),
        "option" | "alt" => "alt".to_owned(),
        "command" | "cmd" | "meta" | "super" | "win" | "windows" => "meta".to_owned(),
        "return" | "enter" => "enter".to_owned(),
        "escape" | "esc" => "esc".to_owned(),
        "del" | "delete" => "delete".to_owned(),
        "pgup" | "page_up" => "pageup".to_owned(),
        "pgdn" | "page_down" => "pagedown".to_owned(),
        other => other.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(value: serde_json::Value) -> Result<ComputerUseAction, ToolError> {
        let args: ComputerUseActArgs = serde_json::from_value(value).expect("valid JSON shape");
        parse_computer_use_action(args)
    }

    #[test]
    fn computer_use_action_schema_accepts_press_with_node_id() {
        let action = parse(serde_json::json!({
            "type": "press",
            "target": { "node_id": "n42" }
        }))
        .expect("press action");
        assert_eq!(action.action_type(), "press");
    }

    #[test]
    fn computer_use_os_action_accepts_open_app() {
        let action = parse(serde_json::json!({
            "type": "open_app",
            "app": "ExampleApp"
        }))
        .expect("open app action");
        assert_eq!(action.action_type(), "open_app");
    }

    #[test]
    fn computer_use_os_action_accepts_select_menu_item() {
        let action = parse(serde_json::json!({
            "type": "select_menu_item",
            "app": "ExampleApp",
            "menu_path": ["File", "New Window"]
        }))
        .expect("select menu item action");
        assert_eq!(action.action_type(), "select_menu_item");
    }

    #[test]
    fn computer_use_menu_action_rejects_missing_app() {
        let error = parse(serde_json::json!({
            "type": "select_menu_item",
            "menu_path": ["File", "New Window"]
        }))
        .expect_err("select_menu_item requires app");
        assert!(
            error
                .to_string()
                .contains("select_menu_item requires non-empty app")
        );
    }

    #[test]
    fn computer_use_os_action_accepts_open_path_with_absolute_existing_path() {
        let temp_dir = std::env::temp_dir();
        let action = parse(serde_json::json!({
            "type": "open_path",
            "path": temp_dir.display().to_string()
        }))
        .expect("open path action");
        assert_eq!(action.action_type(), "open_path");
    }

    #[test]
    fn computer_use_os_action_accepts_reveal_path_with_absolute_existing_path() {
        let temp_dir = std::env::temp_dir();
        let action = parse(serde_json::json!({
            "type": "reveal_path",
            "path": temp_dir.display().to_string()
        }))
        .expect("reveal path action");
        assert_eq!(action.action_type(), "reveal_path");
    }

    #[test]
    fn computer_use_os_action_accepts_open_url_with_http_url() {
        let action = parse(serde_json::json!({
            "type": "open_url",
            "url": "https://example.com"
        }))
        .expect("open url action");
        assert_eq!(action.action_type(), "open_url");
    }

    #[test]
    fn computer_use_os_action_rejects_invalid_open_url_before_execution() {
        let error = parse(serde_json::json!({
            "type": "open_url",
            "url": "ftp://example.com/file"
        }))
        .expect_err("ftp is unsupported");
        assert!(error.to_string().contains("unsupported open_url scheme"));
    }

    #[test]
    fn computer_use_os_action_rejects_missing_required_field() {
        let error = parse(serde_json::json!({
            "type": "open_path"
        }))
        .expect_err("open_path requires path");
        assert!(
            error
                .to_string()
                .contains("open_path requires non-empty path")
        );
    }

    #[test]
    fn computer_use_os_action_rejects_non_os_fields() {
        let error = parse(serde_json::json!({
            "type": "open_app",
            "app": "ExampleApp",
            "target": { "node_id": "n42" }
        }))
        .expect_err("OS actions must reject semantic target");
        assert!(
            error
                .to_string()
                .contains("open_app does not accept target")
        );
    }

    #[test]
    fn computer_use_action_schema_accepts_set_value_with_text() {
        let action = parse(serde_json::json!({
            "type": "set_value",
            "target": { "node_id": "n42" },
            "text": "hello"
        }))
        .expect("set_value action");
        assert_eq!(action.action_type(), "set_value");
    }

    #[test]
    fn computer_use_action_schema_rejects_set_value_without_target() {
        let error = parse(serde_json::json!({
            "type": "set_value",
            "text": "hello"
        }))
        .expect_err("target is required");
        assert!(error.to_string().contains("set_value requires target"));
    }

    #[test]
    fn computer_use_action_schema_rejects_unknown_action_type() {
        let error = parse(serde_json::json!({
            "type": "click",
            "target": { "point": { "x": 1, "y": 2 } }
        }))
        .expect_err("unknown action type");
        assert!(error.to_string().contains("unsupported act.type `click`"));
    }

    #[test]
    fn computer_use_action_schema_accepts_input_chord_with_multiple_keys() {
        let action = parse(serde_json::json!({
            "type": "input_chord",
            "keys": ["meta", "space"]
        }))
        .expect("input chord action");
        assert_eq!(action.action_type(), "input_chord");
    }

    #[test]
    fn computer_use_coordinate_space_accepts_explicit_point_space() {
        let action = parse(serde_json::json!({
            "type": "input_click",
            "target": {
                "point": {
                    "x": 1,
                    "y": 2,
                    "coordinate_space": "source_pixels"
                }
            }
        }))
        .expect("input click action");
        let ComputerUseAction::Input(action) = action else {
            panic!("expected input action");
        };
        assert_eq!(
            action
                .target
                .and_then(|target| target.point)
                .and_then(|point| point.coordinate_space),
            Some(crate::handlers::computer_use::model::CoordinateSpace::SourcePixels)
        );
    }

    #[test]
    fn computer_use_action_schema_rejects_input_key_with_multiple_keys() {
        let error = parse(serde_json::json!({
            "type": "input_key",
            "keys": ["meta", "space"]
        }))
        .expect_err("input_key is a single-key action");
        assert!(
            error
                .to_string()
                .contains("input_key requires exactly one key; use input_chord")
        );
    }

    #[test]
    fn computer_use_action_schema_rejects_semantic_point_target() {
        let error = parse(serde_json::json!({
            "type": "press",
            "target": { "point": { "x": 1, "y": 2 } }
        }))
        .expect_err("point is input only");
        assert!(
            error
                .to_string()
                .contains("semantic actions do not accept target.point")
        );
    }

    #[test]
    fn computer_use_input_action_preserves_uppercase_key_for_xa11y_validation() {
        let key = parse_xa11y_key("A").expect("key");
        assert!(matches!(key, Key::Char('A')));
    }
}
