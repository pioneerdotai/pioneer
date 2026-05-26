use super::model::{
    AccessibilityBounds, AccessibilityNodeRef, ActionTarget, CoordinateSpace, InputAction,
    InputPoint, PointTarget, ResolvedActionTarget, ResolvedActionTargetKind,
    ResolvedInputActionTargets, SemanticAction, SnapshotMeta,
};
use serde_json::Value as JsonValue;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TargetResolutionFailureClass {
    ElementNotFound,
    ElementStale,
    AmbiguousTarget,
    InvalidTarget,
}

impl TargetResolutionFailureClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ElementNotFound => "ElementNotFound",
            Self::ElementStale => "ElementStale",
            Self::AmbiguousTarget => "AmbiguousTarget",
            Self::InvalidTarget => "InvalidTarget",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TargetResolutionError {
    pub(crate) failure_class: TargetResolutionFailureClass,
    pub(crate) message: String,
    pub(crate) diagnostics: JsonValue,
}

impl TargetResolutionError {
    fn new(failure_class: TargetResolutionFailureClass, message: impl Into<String>) -> Self {
        Self {
            failure_class,
            message: message.into(),
            diagnostics: serde_json::json!({}),
        }
    }

    fn with_diagnostics(mut self, diagnostics: JsonValue) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

impl fmt::Display for TargetResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}: {}",
            self.failure_class.as_str(),
            self.message
        )
    }
}

pub(crate) fn resolve_semantic_action_target(
    action: &SemanticAction,
    node_refs: &[AccessibilityNodeRef],
    last_snapshot: Option<&SnapshotMeta>,
) -> Result<Option<ResolvedActionTarget>, TargetResolutionError> {
    let Some(target) = action.target.as_ref() else {
        return Ok(None);
    };
    if target.point.is_some() {
        return Err(TargetResolutionError::new(
            TargetResolutionFailureClass::InvalidTarget,
            "point targets are invalid for semantic actions; use explicit input_* actions",
        )
        .with_diagnostics(serde_json::json!({
            "attempted": target_diagnostics(target),
            "recommended_next_call": "act_with_input_action"
        })));
    }
    resolve_target(target, node_refs, TargetMode::Semantic, last_snapshot).map(Some)
}

pub(crate) fn resolve_input_action_targets(
    action: &InputAction,
    node_refs: &[AccessibilityNodeRef],
    last_snapshot: Option<&SnapshotMeta>,
) -> Result<ResolvedInputActionTargets, TargetResolutionError> {
    Ok(ResolvedInputActionTargets {
        target: action
            .target
            .as_ref()
            .map(|target| resolve_target(target, node_refs, TargetMode::Input, last_snapshot))
            .transpose()?,
        from: action
            .from
            .as_ref()
            .map(|target| resolve_target(target, node_refs, TargetMode::Input, last_snapshot))
            .transpose()?,
        to: action
            .to
            .as_ref()
            .map(|target| resolve_target(target, node_refs, TargetMode::Input, last_snapshot))
            .transpose()?,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetMode {
    Semantic,
    Input,
}

fn resolve_target(
    target: &ActionTarget,
    node_refs: &[AccessibilityNodeRef],
    mode: TargetMode,
    last_snapshot: Option<&SnapshotMeta>,
) -> Result<ResolvedActionTarget, TargetResolutionError> {
    if let Some(node_id) = target.node_id.as_deref().and_then(normalized) {
        validate_snapshot_id(
            target.snapshot_id.as_deref(),
            last_snapshot,
            "target.node_id",
        )?;
        return resolve_node_id_target(node_id, node_refs, mode);
    }
    if let Some(selector) = target.selector.as_deref().and_then(normalized) {
        return Ok(ResolvedActionTarget {
            kind: ResolvedActionTargetKind::Locator,
            node_id: None,
            selector: Some(selector.to_owned()),
            role: None,
            name: None,
            nth: target.nth,
            bounds: None,
            requested_point: None,
            point: None,
        });
    }
    if let Some(bounds_anchor) = target.bounds_anchor.as_ref() {
        validate_snapshot_id(
            bounds_anchor.snapshot_id.as_deref(),
            last_snapshot,
            "target.bounds_anchor.node_id",
        )?;
        let node = find_node_ref(bounds_anchor.node_id.as_str(), node_refs)?;
        return match mode {
            TargetMode::Semantic => Ok(locator_from_node_ref(node)),
            TargetMode::Input => Ok(point_from_node_ref(
                node,
                bounds_anchor.anchor.as_deref().unwrap_or("center"),
                last_snapshot,
            )?),
        };
    }
    if target
        .role
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || target
            .name
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
    {
        return resolve_role_name_target(target, node_refs, mode);
    }
    if let Some(point) = target.point.as_ref() {
        return match mode {
            TargetMode::Semantic => Err(TargetResolutionError::new(
                TargetResolutionFailureClass::InvalidTarget,
                "point targets are invalid for semantic actions; use explicit input_* actions",
            )
            .with_diagnostics(serde_json::json!({
                "attempted": target_diagnostics(target),
                "recommended_next_call": "act_with_input_action"
            }))),
            TargetMode::Input => resolved_direct_point(point, last_snapshot),
        };
    }
    Err(TargetResolutionError::new(
        TargetResolutionFailureClass::InvalidTarget,
        "target must include node_id, selector, role/name, bounds_anchor, or point",
    )
    .with_diagnostics(serde_json::json!({
        "attempted": target_diagnostics(target),
        "recommended_next_call": "snapshot"
    })))
}

fn resolve_node_id_target(
    node_id: &str,
    node_refs: &[AccessibilityNodeRef],
    mode: TargetMode,
) -> Result<ResolvedActionTarget, TargetResolutionError> {
    let node = find_node_ref(node_id, node_refs)?;
    match mode {
        TargetMode::Semantic => Ok(locator_from_node_ref(node)),
        TargetMode::Input => Ok(locator_from_node_ref(node)),
    }
}

fn resolve_role_name_target(
    target: &ActionTarget,
    node_refs: &[AccessibilityNodeRef],
    mode: TargetMode,
) -> Result<ResolvedActionTarget, TargetResolutionError> {
    let role = target.role.as_deref().and_then(normalized);
    let name = target.name.as_deref().and_then(normalized);
    let matches = node_refs
        .iter()
        .filter(|node| {
            role.is_none_or(|role| node.role.eq_ignore_ascii_case(role))
                && name.is_none_or(|name| node.name.as_deref() == Some(name))
        })
        .collect::<Vec<_>>();
    if matches.is_empty() {
        return Err(TargetResolutionError::new(
            TargetResolutionFailureClass::ElementNotFound,
            format!(
                "no node matched role={:?} name={:?}",
                role.unwrap_or("*"),
                name.unwrap_or("*")
            ),
        )
        .with_diagnostics(serde_json::json!({
            "attempted": target_diagnostics(target),
            "candidate_count": 0,
            "candidates": [],
            "recommended_next_call": "snapshot"
        })));
    }
    let selected = if let Some(nth) = target.nth {
        if nth == 0 {
            return Err(TargetResolutionError::new(
                TargetResolutionFailureClass::InvalidTarget,
                "nth is 1-based and must be greater than zero",
            )
            .with_diagnostics(serde_json::json!({
                "attempted": target_diagnostics(target),
                "candidate_count": matches.len(),
                "candidates": candidate_summaries(matches.as_slice()),
                "recommended_next_call": "act_with_valid_nth"
            })));
        }
        matches.get(nth - 1).copied().ok_or_else(|| {
            TargetResolutionError::new(
                TargetResolutionFailureClass::ElementNotFound,
                format!("nth={} is outside {} matching nodes", nth, matches.len()),
            )
            .with_diagnostics(serde_json::json!({
                "attempted": target_diagnostics(target),
                "candidate_count": matches.len(),
                "candidates": candidate_summaries(matches.as_slice()),
                "recommended_next_call": "act_with_valid_nth"
            }))
        })?
    } else if matches.len() == 1 {
        matches[0]
    } else {
        return Err(TargetResolutionError::new(
            TargetResolutionFailureClass::AmbiguousTarget,
            format!(
                "role/name target matched {} nodes; provide nth to disambiguate",
                matches.len()
            ),
        )
        .with_diagnostics(serde_json::json!({
            "attempted": target_diagnostics(target),
            "candidate_count": matches.len(),
            "candidates": candidate_summaries(matches.as_slice()),
            "recommended_next_call": "act_with_nth"
        })));
    };
    match mode {
        TargetMode::Semantic | TargetMode::Input => Ok(locator_from_node_ref(selected)),
    }
}

fn find_node_ref<'a>(
    node_id: &str,
    node_refs: &'a [AccessibilityNodeRef],
) -> Result<&'a AccessibilityNodeRef, TargetResolutionError> {
    node_refs
        .iter()
        .find(|node| node.id == node_id)
        .ok_or_else(|| {
            TargetResolutionError::new(
                TargetResolutionFailureClass::ElementStale,
                format!(
                    "node_id `{}` is not present in the latest snapshot",
                    node_id
                ),
            )
            .with_diagnostics(serde_json::json!({
                "node_id": node_id,
                "candidate_count": 0,
                "recommended_next_call": "snapshot"
            }))
        })
}

fn locator_from_node_ref(node: &AccessibilityNodeRef) -> ResolvedActionTarget {
    ResolvedActionTarget {
        kind: ResolvedActionTargetKind::Locator,
        node_id: Some(node.id.clone()),
        selector: node
            .selector_hints
            .iter()
            .find(|value| !value.trim().is_empty())
            .cloned(),
        role: Some(node.role.clone()),
        name: node.name.clone(),
        nth: None,
        bounds: node.bounds.clone(),
        requested_point: None,
        point: None,
    }
}

fn point_from_node_ref(
    node: &AccessibilityNodeRef,
    anchor: &str,
    last_snapshot: Option<&SnapshotMeta>,
) -> Result<ResolvedActionTarget, TargetResolutionError> {
    let bounds = node.bounds.clone().ok_or_else(|| {
        TargetResolutionError::new(
            TargetResolutionFailureClass::ElementNotFound,
            format!("node_id `{}` has no bounds for anchor target", node.id),
        )
        .with_diagnostics(serde_json::json!({
            "node_id": node.id.as_str(),
            "candidate": candidate_summary(node),
            "recommended_next_call": "act_with_semantic_target"
        }))
    })?;
    let requested_point = anchor_point(bounds.clone(), anchor)?;
    let point = convert_point_target(&requested_point, last_snapshot)?;
    Ok(ResolvedActionTarget {
        kind: ResolvedActionTargetKind::Point,
        node_id: Some(node.id.clone()),
        selector: node.selector_hints.first().cloned(),
        role: Some(node.role.clone()),
        name: node.name.clone(),
        nth: None,
        bounds: Some(bounds),
        requested_point: Some(requested_point),
        point: Some(point),
    })
}

fn anchor_point(
    bounds: AccessibilityBounds,
    anchor: &str,
) -> Result<InputPoint, TargetResolutionError> {
    let x0 = bounds.x;
    let y0 = bounds.y;
    let x1 = bounds.x.saturating_add(bounds.width as i32);
    let y1 = bounds.y.saturating_add(bounds.height as i32);
    let point = match anchor.trim().to_ascii_lowercase().as_str() {
        "" | "center" => InputPoint {
            x: x0.saturating_add((bounds.width as i32) / 2),
            y: y0.saturating_add((bounds.height as i32) / 2),
            coordinate_space: Some(CoordinateSpace::LogicalScreen),
        },
        "top_left" => InputPoint {
            x: x0,
            y: y0,
            coordinate_space: Some(CoordinateSpace::LogicalScreen),
        },
        "top_right" => InputPoint {
            x: x1,
            y: y0,
            coordinate_space: Some(CoordinateSpace::LogicalScreen),
        },
        "bottom_left" => InputPoint {
            x: x0,
            y: y1,
            coordinate_space: Some(CoordinateSpace::LogicalScreen),
        },
        "bottom_right" => InputPoint {
            x: x1,
            y: y1,
            coordinate_space: Some(CoordinateSpace::LogicalScreen),
        },
        other => {
            return Err(TargetResolutionError::new(
                TargetResolutionFailureClass::InvalidTarget,
                format!("unsupported bounds anchor `{}`", other),
            )
            .with_diagnostics(serde_json::json!({
                "anchor": other,
                "supported_anchors": ["center", "top_left", "top_right", "bottom_left", "bottom_right"],
                "recommended_next_call": "act_with_supported_anchor"
            })));
        }
    };
    Ok(point)
}

fn resolved_direct_point(
    point: &PointTarget,
    last_snapshot: Option<&SnapshotMeta>,
) -> Result<ResolvedActionTarget, TargetResolutionError> {
    let requested_point = normalize_direct_point_target(point, last_snapshot)?;
    let converted_point = convert_point_target(&requested_point, last_snapshot)?;
    Ok(ResolvedActionTarget {
        kind: ResolvedActionTargetKind::Point,
        node_id: None,
        selector: None,
        role: None,
        name: None,
        nth: None,
        bounds: None,
        requested_point: Some(requested_point),
        point: Some(converted_point),
    })
}

fn normalize_direct_point_target(
    point: &PointTarget,
    last_snapshot: Option<&SnapshotMeta>,
) -> Result<PointTarget, TargetResolutionError> {
    if point.coordinate_space.is_none()
        && last_snapshot
            .map(snapshot_requires_explicit_coordinate_space)
            .unwrap_or(true)
    {
        return Err(TargetResolutionError::new(
            TargetResolutionFailureClass::InvalidTarget,
            "coordinate_space_required: target.point must include coordinate_space when the latest snapshot was transformed for LLM transport",
        )
        .with_diagnostics(serde_json::json!({
            "point": point,
            "supported_coordinate_spaces": ["source_pixels", "transport_pixels", "logical_screen", "native_input"],
            "recommended_next_call": "act_with_coordinate_space"
        })));
    }
    Ok(PointTarget {
        x: point.x,
        y: point.y,
        coordinate_space: Some(
            point
                .coordinate_space
                .unwrap_or(CoordinateSpace::SourcePixels),
        ),
    })
}

fn snapshot_requires_explicit_coordinate_space(snapshot: &SnapshotMeta) -> bool {
    snapshot.resize_passes > 0
        || snapshot.transport_width_px != snapshot.width_px
        || snapshot.transport_height_px != snapshot.height_px
}

fn validate_snapshot_id(
    supplied: Option<&str>,
    last_snapshot: Option<&SnapshotMeta>,
    label: &str,
) -> Result<(), TargetResolutionError> {
    let latest = last_snapshot
        .map(|snapshot| snapshot.snapshot_id.as_str())
        .ok_or_else(|| {
            TargetResolutionError::new(
                TargetResolutionFailureClass::ElementStale,
                format!("{label} requires a fresh snapshot before node_id can be used"),
            )
            .with_diagnostics(serde_json::json!({
                "target_field": label,
                "recommended_next_call": "snapshot"
            }))
        })?;
    let supplied = supplied.and_then(normalized).ok_or_else(|| {
        TargetResolutionError::new(
            TargetResolutionFailureClass::ElementStale,
            format!(
                "snapshot_id_required: {label} must include snapshot_id from the latest snapshot ({latest})"
            ),
        )
        .with_diagnostics(serde_json::json!({
            "target_field": label,
            "latest_snapshot_id": latest,
            "recommended_next_call": "snapshot"
        }))
    })?;
    if supplied != latest {
        return Err(TargetResolutionError::new(
            TargetResolutionFailureClass::ElementStale,
            format!(
                "snapshot_id_stale: {label} snapshot_id `{supplied}` does not match latest `{latest}`; request a fresh snapshot and re-resolve the target"
            ),
        )
        .with_diagnostics(serde_json::json!({
            "target_field": label,
            "supplied_snapshot_id": supplied,
            "latest_snapshot_id": latest,
            "recommended_next_call": "snapshot"
        })));
    }
    Ok(())
}

fn convert_point_target(
    point: &PointTarget,
    last_snapshot: Option<&SnapshotMeta>,
) -> Result<PointTarget, TargetResolutionError> {
    let coordinate_space = point.coordinate_space.ok_or_else(|| {
        TargetResolutionError::new(
            TargetResolutionFailureClass::InvalidTarget,
            "coordinate_space_required: target.point must include coordinate_space",
        )
        .with_diagnostics(serde_json::json!({
            "point": point,
            "supported_coordinate_spaces": ["source_pixels", "transport_pixels", "logical_screen", "native_input"],
            "recommended_next_call": "act_with_coordinate_space"
        }))
    })?;
    match coordinate_space {
        CoordinateSpace::NativeInput => {
            validate_native_bounds(point.x, point.y, last_snapshot)?;
            Ok(PointTarget {
                x: point.x,
                y: point.y,
                coordinate_space: Some(CoordinateSpace::NativeInput),
            })
        }
        CoordinateSpace::LogicalScreen => {
            validate_native_bounds(point.x, point.y, last_snapshot)?;
            Ok(PointTarget {
                x: point.x,
                y: point.y,
                coordinate_space: Some(CoordinateSpace::NativeInput),
            })
        }
        CoordinateSpace::SourcePixels => {
            let snapshot = require_snapshot_for_space(coordinate_space, last_snapshot)?;
            validate_source_bounds(point.x, point.y, snapshot)?;
            let (x, y) = source_to_native(point.x, point.y, snapshot)?;
            validate_native_bounds(x, y, Some(snapshot))?;
            Ok(PointTarget {
                x,
                y,
                coordinate_space: Some(CoordinateSpace::NativeInput),
            })
        }
        CoordinateSpace::TransportPixels => {
            let snapshot = require_snapshot_for_space(coordinate_space, last_snapshot)?;
            validate_transport_bounds(point.x, point.y, snapshot)?;
            let (source_x, source_y) = transport_to_source(point.x, point.y, snapshot)?;
            validate_source_bounds(source_x, source_y, snapshot)?;
            let (x, y) = source_to_native(source_x, source_y, snapshot)?;
            validate_native_bounds(x, y, Some(snapshot))?;
            Ok(PointTarget {
                x,
                y,
                coordinate_space: Some(CoordinateSpace::NativeInput),
            })
        }
    }
}

fn require_snapshot_for_space(
    coordinate_space: CoordinateSpace,
    last_snapshot: Option<&SnapshotMeta>,
) -> Result<&SnapshotMeta, TargetResolutionError> {
    last_snapshot.ok_or_else(|| {
        TargetResolutionError::new(
            TargetResolutionFailureClass::InvalidTarget,
            format!(
                "coordinate_snapshot_required: coordinate_space={} requires latest snapshot metadata",
                coordinate_space.as_str()
            ),
        )
        .with_diagnostics(serde_json::json!({
            "coordinate_space": coordinate_space.as_str(),
            "recommended_next_call": "snapshot"
        }))
    })
}

fn validate_transport_bounds(
    x: i32,
    y: i32,
    snapshot: &SnapshotMeta,
) -> Result<(), TargetResolutionError> {
    validate_bounds(
        x,
        y,
        snapshot.transport_width_px,
        snapshot.transport_height_px,
        CoordinateSpace::TransportPixels,
    )
}

fn validate_source_bounds(
    x: i32,
    y: i32,
    snapshot: &SnapshotMeta,
) -> Result<(), TargetResolutionError> {
    validate_bounds(
        x,
        y,
        snapshot.width_px,
        snapshot.height_px,
        CoordinateSpace::SourcePixels,
    )
}

fn validate_native_bounds(
    x: i32,
    y: i32,
    snapshot: Option<&SnapshotMeta>,
) -> Result<(), TargetResolutionError> {
    let Some(snapshot) = snapshot else {
        return Ok(());
    };
    let (width, height) = native_bounds(snapshot);
    validate_bounds(x, y, width, height, CoordinateSpace::NativeInput)
}

fn validate_bounds(
    x: i32,
    y: i32,
    width: u32,
    height: u32,
    coordinate_space: CoordinateSpace,
) -> Result<(), TargetResolutionError> {
    if x >= 0 && y >= 0 && (x as u32) < width && (y as u32) < height {
        return Ok(());
    }
    Err(TargetResolutionError::new(
        TargetResolutionFailureClass::InvalidTarget,
        format!(
            "coordinate_out_of_bounds: point ({x},{y}) is outside {} bounds {}x{}",
            coordinate_space.as_str(),
            width,
            height
        ),
    )
    .with_diagnostics(serde_json::json!({
        "point": { "x": x, "y": y, "coordinate_space": coordinate_space.as_str() },
        "bounds": { "width": width, "height": height },
        "recommended_next_call": "snapshot"
    })))
}

fn transport_to_source(
    x: i32,
    y: i32,
    snapshot: &SnapshotMeta,
) -> Result<(i32, i32), TargetResolutionError> {
    if snapshot.transport_width_px == 0 || snapshot.transport_height_px == 0 {
        return Err(TargetResolutionError::new(
            TargetResolutionFailureClass::InvalidTarget,
            "coordinate_snapshot_invalid: transport dimensions must be positive",
        )
        .with_diagnostics(serde_json::json!({
            "recommended_next_call": "snapshot"
        })));
    }
    let source_x = (f64::from(x) * f64::from(snapshot.width_px)
        / f64::from(snapshot.transport_width_px))
    .floor() as i32;
    let source_y = (f64::from(y) * f64::from(snapshot.height_px)
        / f64::from(snapshot.transport_height_px))
    .floor() as i32;
    Ok((source_x, source_y))
}

fn source_to_native(
    x: i32,
    y: i32,
    snapshot: &SnapshotMeta,
) -> Result<(i32, i32), TargetResolutionError> {
    let scale = if snapshot.scale_factor.is_finite() && snapshot.scale_factor > 0.0 {
        f64::from(snapshot.scale_factor)
    } else {
        1.0
    };
    Ok((
        (f64::from(x) / scale).floor() as i32,
        (f64::from(y) / scale).floor() as i32,
    ))
}

fn native_bounds(snapshot: &SnapshotMeta) -> (u32, u32) {
    let scale = if snapshot.scale_factor.is_finite() && snapshot.scale_factor > 0.0 {
        f64::from(snapshot.scale_factor)
    } else {
        1.0
    };
    (
        (f64::from(snapshot.width_px) / scale).ceil() as u32,
        (f64::from(snapshot.height_px) / scale).ceil() as u32,
    )
}

fn target_diagnostics(target: &ActionTarget) -> JsonValue {
    serde_json::json!({
        "node_id": target.node_id.as_deref(),
        "snapshot_id": target.snapshot_id.as_deref(),
        "selector": target.selector.as_deref(),
        "role": target.role.as_deref(),
        "name": target.name.as_deref(),
        "nth": target.nth,
        "has_bounds_anchor": target.bounds_anchor.is_some(),
        "has_point": target.point.is_some(),
    })
}

fn candidate_summaries(candidates: &[&AccessibilityNodeRef]) -> Vec<JsonValue> {
    candidates
        .iter()
        .take(5)
        .map(|node| candidate_summary(node))
        .collect()
}

fn candidate_summary(node: &AccessibilityNodeRef) -> JsonValue {
    serde_json::json!({
        "node_id": node.id.as_str(),
        "role": node.role.as_str(),
        "name": node.name.as_deref(),
        "bounds": node.bounds.as_ref(),
        "supported_act_types": node.supported_act_types.as_slice(),
        "selector_hints": node.selector_hints.iter().take(2).collect::<Vec<_>>(),
    })
}

fn normalized(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() { None } else { Some(value) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::computer_use::model::{BoundsAnchorTarget, SemanticActionKind};

    fn node_refs() -> Vec<AccessibilityNodeRef> {
        vec![
            AccessibilityNodeRef {
                id: "n1".to_owned(),
                selector_hints: vec![r#"button[name="OK"]"#.to_owned()],
                stable_id: Some("ok-button".to_owned()),
                role: "button".to_owned(),
                name: Some("OK".to_owned()),
                bounds: Some(AccessibilityBounds {
                    x: 10,
                    y: 20,
                    width: 80,
                    height: 30,
                }),
                supported_act_types: vec!["press".to_owned(), "focus".to_owned()],
            },
            AccessibilityNodeRef {
                id: "n2".to_owned(),
                selector_hints: vec![r#"button[name="Cancel"]"#.to_owned()],
                stable_id: Some("cancel-button".to_owned()),
                role: "button".to_owned(),
                name: Some("Cancel".to_owned()),
                bounds: Some(AccessibilityBounds {
                    x: 100,
                    y: 20,
                    width: 80,
                    height: 30,
                }),
                supported_act_types: vec!["press".to_owned()],
            },
        ]
    }

    fn semantic(target: ActionTarget) -> SemanticAction {
        SemanticAction {
            action_type: SemanticActionKind::Press,
            target: Some(target),
            text: None,
            numeric_value: None,
            action_name: None,
            condition: None,
            wait_ms: None,
        }
    }

    fn snapshot(snapshot_id: &str) -> SnapshotMeta {
        SnapshotMeta {
            index: 1,
            snapshot_id: snapshot_id.to_owned(),
            path: "/tmp/snapshot.png".to_owned(),
            width_px: 640,
            height_px: 360,
            transport_width_px: 640,
            transport_height_px: 360,
            scale_factor: 1.0,
            size_bytes: 1,
            resize_passes: 0,
            captured_at_unix_ms: 0,
            state_hash: "state".to_owned(),
        }
    }

    #[test]
    fn computer_use_target_resolves_by_node_id() {
        let resolved = resolve_semantic_action_target(
            &semantic(ActionTarget {
                node_id: Some("n1".to_owned()),
                snapshot_id: Some("s1-1".to_owned()),
                selector: None,
                role: None,
                name: None,
                nth: None,
                bounds_anchor: None,
                point: None,
            }),
            &node_refs(),
            Some(&snapshot("s1-1")),
        )
        .expect("resolved")
        .expect("target");
        assert_eq!(resolved.selector.as_deref(), Some(r#"button[name="OK"]"#));
        assert_eq!(resolved.kind, ResolvedActionTargetKind::Locator);
    }

    #[test]
    fn computer_use_target_resolves_by_selector() {
        let resolved = resolve_semantic_action_target(
            &semantic(ActionTarget {
                node_id: None,
                snapshot_id: None,
                selector: Some(r#"button[name="OK"]"#.to_owned()),
                role: None,
                name: None,
                nth: Some(1),
                bounds_anchor: None,
                point: None,
            }),
            &node_refs(),
            None,
        )
        .expect("resolved")
        .expect("target");
        assert_eq!(resolved.selector.as_deref(), Some(r#"button[name="OK"]"#));
        assert_eq!(resolved.nth, Some(1));
    }

    #[test]
    fn computer_use_target_resolves_by_role_name_nth() {
        let resolved = resolve_semantic_action_target(
            &semantic(ActionTarget {
                node_id: None,
                snapshot_id: None,
                selector: None,
                role: Some("button".to_owned()),
                name: None,
                nth: Some(2),
                bounds_anchor: None,
                point: None,
            }),
            &node_refs(),
            None,
        )
        .expect("resolved")
        .expect("target");
        assert_eq!(resolved.node_id.as_deref(), Some("n2"));
    }

    #[test]
    fn computer_use_target_ambiguous_role_name_requires_nth() {
        let error = resolve_semantic_action_target(
            &semantic(ActionTarget {
                node_id: None,
                snapshot_id: None,
                selector: None,
                role: Some("button".to_owned()),
                name: None,
                nth: None,
                bounds_anchor: None,
                point: None,
            }),
            &node_refs(),
            None,
        )
        .expect_err("ambiguous");
        assert_eq!(
            error.failure_class,
            TargetResolutionFailureClass::AmbiguousTarget
        );
    }

    #[test]
    fn computer_use_target_rejects_point_for_semantic_action() {
        let error = resolve_semantic_action_target(
            &semantic(ActionTarget {
                node_id: None,
                snapshot_id: None,
                selector: None,
                role: None,
                name: None,
                nth: None,
                bounds_anchor: None,
                point: Some(InputPoint {
                    x: 1,
                    y: 2,
                    coordinate_space: Some(CoordinateSpace::SourcePixels),
                }),
            }),
            &node_refs(),
            None,
        )
        .expect_err("point rejected");
        assert_eq!(
            error.failure_class,
            TargetResolutionFailureClass::InvalidTarget
        );
    }

    #[test]
    fn computer_use_target_stale_node_maps_to_element_stale() {
        let error = resolve_semantic_action_target(
            &semantic(ActionTarget {
                node_id: Some("n999".to_owned()),
                snapshot_id: Some("s1-1".to_owned()),
                selector: None,
                role: None,
                name: None,
                nth: None,
                bounds_anchor: None,
                point: None,
            }),
            &node_refs(),
            Some(&snapshot("s1-1")),
        )
        .expect_err("stale");
        assert_eq!(
            error.failure_class,
            TargetResolutionFailureClass::ElementStale
        );
    }

    #[test]
    fn computer_use_target_resolves_bounds_anchor_to_input_point() {
        let action = InputAction {
            action_type: super::super::model::InputActionKind::InputClick,
            target: Some(ActionTarget {
                node_id: None,
                snapshot_id: None,
                selector: None,
                role: None,
                name: None,
                nth: None,
                bounds_anchor: Some(BoundsAnchorTarget {
                    node_id: "n1".to_owned(),
                    snapshot_id: Some("s1-1".to_owned()),
                    anchor: Some("center".to_owned()),
                }),
                point: None,
            }),
            from: None,
            to: None,
            button: None,
            delta_x: None,
            delta_y: None,
            text: None,
            keys: None,
            wait_ms: None,
        };
        let snapshot = snapshot("s1-1");
        let resolved =
            resolve_input_action_targets(&action, &node_refs(), Some(&snapshot)).expect("resolved");
        assert_eq!(
            resolved.target.and_then(|target| target.point),
            Some(InputPoint {
                x: 50,
                y: 35,
                coordinate_space: Some(CoordinateSpace::NativeInput),
            })
        );
    }
}
