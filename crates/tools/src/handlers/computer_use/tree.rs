use super::model::{
    AccessibilityBounds, AccessibilityNodeRef, AccessibilityTreePayload, CompactAccessibilityNode,
    DesktopTree,
};
use crate::error::ToolError;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccessibilityTreeBudget {
    pub(crate) max_depth: usize,
    pub(crate) max_nodes: usize,
    pub(crate) max_serialized_bytes: usize,
    pub(crate) text_max_chars: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct RawAccessibilityNode {
    pub(crate) role: String,
    pub(crate) name: Option<String>,
    pub(crate) value: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) bounds: Option<AccessibilityBounds>,
    pub(crate) states: Vec<String>,
    pub(crate) actions: Vec<String>,
    pub(crate) stable_id: Option<String>,
    pub(crate) children: Vec<RawAccessibilityNode>,
}

pub(crate) fn compact_raw_tree(
    root: &RawAccessibilityNode,
    budget: AccessibilityTreeBudget,
) -> Result<DesktopTree, ToolError> {
    let mut state = CompactState {
        budget,
        nodes: Vec::new(),
        node_refs: Vec::new(),
        next_id: 1,
        omitted_count: 0,
        truncated: false,
    };
    state.visit(root, None, 0);

    let mut payload = AccessibilityTreePayload {
        status: "ok".to_owned(),
        reason: None,
        nodes: state.nodes,
        truncated: state.truncated,
        omitted_count: state.omitted_count,
        max_depth: budget.max_depth,
        max_nodes: budget.max_nodes,
        serialized_bytes: 0,
    };

    payload.serialized_bytes = serialized_len(&payload)?;
    while payload.serialized_bytes > budget.max_serialized_bytes && !payload.nodes.is_empty() {
        payload.nodes.pop();
        payload.truncated = true;
        payload.omitted_count = payload.omitted_count.saturating_add(1);
        payload.serialized_bytes = serialized_len(&payload)?;
    }

    let allowed_ids = payload
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    state
        .node_refs
        .retain(|node_ref| allowed_ids.contains(node_ref.id.as_str()));

    Ok(DesktopTree {
        payload,
        node_refs: state.node_refs,
    })
}

pub(crate) fn absent_tree(reason: impl Into<String>) -> AccessibilityTreePayload {
    AccessibilityTreePayload {
        status: "absent".to_owned(),
        reason: Some(reason.into()),
        nodes: Vec::new(),
        truncated: false,
        omitted_count: 0,
        max_depth: 0,
        max_nodes: 0,
        serialized_bytes: 0,
    }
}

#[cfg(feature = "computer-use")]
pub(crate) fn compact_xa11y_app_tree(
    app: &xa11y::App,
    budget: AccessibilityTreeBudget,
) -> Result<DesktopTree, ToolError> {
    let root = raw_from_xa11y_element(&app.as_element(), budget.max_depth, 0)?;
    compact_raw_tree(&root, budget)
}

#[cfg(feature = "computer-use")]
fn raw_from_xa11y_element(
    element: &xa11y::Element,
    max_depth: usize,
    depth: usize,
) -> Result<RawAccessibilityNode, ToolError> {
    let data = element.data();
    let children = if depth < max_depth {
        element
            .children()
            .map_err(|error| {
                ToolError::execution_failed(format!("app.tree children failed: {error}"))
            })?
            .iter()
            .map(|child| raw_from_xa11y_element(child, max_depth, depth + 1))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        Vec::new()
    };

    Ok(RawAccessibilityNode {
        role: {
            let role: &'static str = data.role.into();
            role.to_owned()
        },
        name: data.name.clone(),
        value: data.value.clone(),
        description: data.description.clone(),
        bounds: data.bounds.map(|bounds| AccessibilityBounds {
            x: bounds.x,
            y: bounds.y,
            width: bounds.width,
            height: bounds.height,
        }),
        states: state_names(&data.states),
        actions: data.actions.clone(),
        stable_id: data.stable_id.clone(),
        children,
    })
}

#[cfg(feature = "computer-use")]
fn state_names(states: &xa11y::StateSet) -> Vec<String> {
    let mut out = Vec::new();
    if states.visible {
        out.push("visible".to_owned());
    }
    if states.enabled {
        out.push("enabled".to_owned());
    }
    if states.focused {
        out.push("focused".to_owned());
    }
    if states.selected {
        out.push("selected".to_owned());
    }
    if states.editable {
        out.push("editable".to_owned());
    }
    if states.focusable {
        out.push("focusable".to_owned());
    }
    if states.modal {
        out.push("modal".to_owned());
    }
    if states.required {
        out.push("required".to_owned());
    }
    if states.busy {
        out.push("busy".to_owned());
    }
    if let Some(checked) = states.checked {
        out.push(format!(
            "checked_{}",
            format!("{checked:?}").to_ascii_lowercase()
        ));
    }
    if let Some(expanded) = states.expanded {
        out.push(if expanded { "expanded" } else { "collapsed" }.to_owned());
    }
    out
}

struct CompactState {
    budget: AccessibilityTreeBudget,
    nodes: Vec<CompactAccessibilityNode>,
    node_refs: Vec<AccessibilityNodeRef>,
    next_id: usize,
    omitted_count: usize,
    truncated: bool,
}

impl CompactState {
    fn visit(&mut self, raw: &RawAccessibilityNode, parent_id: Option<String>, depth: usize) {
        if depth > self.budget.max_depth {
            self.truncated = true;
            self.omitted_count = self.omitted_count.saturating_add(count_raw_nodes(raw));
            return;
        }
        if self.nodes.len() >= self.budget.max_nodes {
            self.truncated = true;
            self.omitted_count = self.omitted_count.saturating_add(count_raw_nodes(raw));
            return;
        }
        if should_exclude(raw) {
            self.omitted_count = self.omitted_count.saturating_add(1);
            for child in &raw.children {
                self.visit(child, parent_id.clone(), depth + 1);
            }
            return;
        }

        let id = format!("n{}", self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        let selector_hints = selector_hints(raw);
        let supported_act_types = supported_act_types(raw.actions.as_slice());
        self.node_refs.push(AccessibilityNodeRef {
            id: id.clone(),
            selector_hints: selector_hints.clone(),
            stable_id: raw.stable_id.clone(),
            role: raw.role.clone(),
            name: truncate_optional(raw.name.as_deref(), self.budget.text_max_chars),
            bounds: raw.bounds.clone(),
            supported_act_types: supported_act_types.clone(),
        });
        self.nodes.push(CompactAccessibilityNode {
            id: id.clone(),
            parent_id: parent_id.clone(),
            depth,
            role: raw.role.clone(),
            name: truncate_optional(raw.name.as_deref(), self.budget.text_max_chars),
            value: truncate_optional(raw.value.as_deref(), self.budget.text_max_chars),
            description: truncate_optional(raw.description.as_deref(), self.budget.text_max_chars),
            bounds: raw.bounds.clone(),
            states: raw.states.clone(),
            supported_act_types,
            raw_actions: compact_raw_actions(raw.actions.as_slice()),
            selector_hints,
        });

        for child in &raw.children {
            self.visit(child, Some(id.clone()), depth + 1);
        }
    }
}

fn should_exclude(raw: &RawAccessibilityNode) -> bool {
    let visible = raw.states.iter().any(|state| state == "visible");
    let actionable = !raw.actions.is_empty();
    let informative = raw.name.is_some() || raw.value.is_some() || raw.description.is_some();
    !visible && !actionable && !informative
}

fn supported_act_types(raw_actions: &[String]) -> Vec<String> {
    let mut out = BTreeSet::new();
    for action in raw_actions {
        match normalized_action_name(action).as_str() {
            "press" | "axpress" | "click" | "tap" | "default" => {
                out.insert("press".to_owned());
            }
            "focus" | "axfocus" => {
                out.insert("focus".to_owned());
            }
            "blur" => {
                out.insert("blur".to_owned());
            }
            "toggle" | "axtoggle" => {
                out.insert("toggle".to_owned());
            }
            "select" | "axselect" => {
                out.insert("select".to_owned());
            }
            "expand" | "axexpand" => {
                out.insert("expand".to_owned());
            }
            "collapse" | "axcollapse" => {
                out.insert("collapse".to_owned());
            }
            "showmenu" | "show_menu" | "axshowmenu" => {
                out.insert("show_menu".to_owned());
            }
            "scrollintoview" | "scroll_into_view" | "axscrolltovisible" => {
                out.insert("scroll_into_view".to_owned());
            }
            "setvalue" | "set_value" | "axsetvalue" | "settext" | "set_text" => {
                out.insert("set_value".to_owned());
                out.insert("type_text".to_owned());
            }
            "setnumericvalue" | "set_numeric_value" | "increment" | "decrement" => {
                out.insert("set_numeric_value".to_owned());
            }
            "selecttext" | "select_text" => {
                out.insert("select_text".to_owned());
            }
            other if !other.is_empty() => {
                out.insert("perform_action".to_owned());
            }
            _ => {}
        }
    }
    out.into_iter().collect()
}

fn compact_raw_actions(raw_actions: &[String]) -> Vec<String> {
    if raw_actions.len() > 16 {
        return Vec::new();
    }
    let total_chars = raw_actions
        .iter()
        .map(|value| value.chars().count())
        .sum::<usize>();
    if total_chars > 256 {
        return Vec::new();
    }
    raw_actions.to_vec()
}

fn normalized_action_name(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect()
}

fn selector_hints(raw: &RawAccessibilityNode) -> Vec<String> {
    let mut hints = Vec::new();
    let role = raw.role.trim();
    if let Some(stable_id) = raw.stable_id.as_deref().filter(|value| !value.is_empty()) {
        hints.push(format!(
            r#"{role}[stable_id="{}"]"#,
            escape_selector_value(stable_id)
        ));
    }
    if let Some(name) = raw.name.as_deref().filter(|value| !value.is_empty()) {
        hints.push(format!(r#"{role}[name="{}"]"#, escape_selector_value(name)));
    }
    if hints.is_empty() && !role.is_empty() {
        hints.push(role.to_owned());
    }
    hints
}

fn escape_selector_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn truncate_optional(value: Option<&str>, max_chars: usize) -> Option<String> {
    let value = value?;
    let mut out = String::new();
    for (index, ch) in value.chars().enumerate() {
        if index >= max_chars {
            out.push_str("...");
            return Some(out);
        }
        out.push(ch);
    }
    Some(out)
}

fn count_raw_nodes(raw: &RawAccessibilityNode) -> usize {
    1 + raw.children.iter().map(count_raw_nodes).sum::<usize>()
}

fn serialized_len(payload: &AccessibilityTreePayload) -> Result<usize, ToolError> {
    serde_json::to_vec(payload)
        .map(|value| value.len())
        .map_err(|error| {
            ToolError::internal(format!("failed to serialize accessibility tree: {error}"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(max_depth: usize, max_nodes: usize, max_bytes: usize) -> AccessibilityTreeBudget {
        AccessibilityTreeBudget {
            max_depth,
            max_nodes,
            max_serialized_bytes: max_bytes,
            text_max_chars: 32,
        }
    }

    fn raw_node(
        role: &str,
        name: &str,
        children: Vec<RawAccessibilityNode>,
    ) -> RawAccessibilityNode {
        RawAccessibilityNode {
            role: role.to_owned(),
            name: Some(name.to_owned()),
            value: None,
            description: None,
            bounds: Some(AccessibilityBounds {
                x: 10,
                y: 20,
                width: 80,
                height: 32,
            }),
            states: vec!["visible".to_owned(), "enabled".to_owned()],
            actions: if role == "button" {
                vec!["press".to_owned()]
            } else {
                Vec::new()
            },
            stable_id: Some(format!("stable-{name}")),
            children,
        }
    }

    #[test]
    fn computer_use_tree_compacts_nodes_and_selector_hints() {
        let root = raw_node("window", "Main", vec![raw_node("button", "OK", Vec::new())]);
        let tree = compact_raw_tree(&root, budget(4, 10, 64 * 1024)).expect("tree");
        assert_eq!(tree.payload.status, "ok");
        assert_eq!(tree.payload.nodes.len(), 2);
        assert_eq!(tree.payload.nodes[1].parent_id.as_deref(), Some("n1"));
        assert!(
            tree.payload.nodes[1]
                .selector_hints
                .iter()
                .any(|hint| hint == "button[stable_id=\"stable-OK\"]")
        );
        assert_eq!(tree.node_refs.len(), 2);
    }

    #[test]
    fn computer_use_tree_truncates_by_node_count() {
        let root = raw_node(
            "window",
            "Main",
            vec![
                raw_node("button", "One", Vec::new()),
                raw_node("button", "Two", Vec::new()),
            ],
        );
        let tree = compact_raw_tree(&root, budget(4, 2, 64 * 1024)).expect("tree");
        assert!(tree.payload.truncated);
        assert_eq!(tree.payload.nodes.len(), 2);
        assert_eq!(tree.payload.omitted_count, 1);
    }

    #[test]
    fn computer_use_tree_truncates_by_depth() {
        let root = raw_node(
            "window",
            "Main",
            vec![raw_node(
                "group",
                "Group",
                vec![raw_node("button", "Deep", Vec::new())],
            )],
        );
        let tree = compact_raw_tree(&root, budget(1, 10, 64 * 1024)).expect("tree");
        assert!(tree.payload.truncated);
        assert_eq!(tree.payload.nodes.len(), 2);
        assert_eq!(tree.payload.omitted_count, 1);
    }

    #[test]
    fn computer_use_tree_truncates_by_serialized_bytes() {
        let root = raw_node("window", "Main", vec![raw_node("button", "OK", Vec::new())]);
        let tree = compact_raw_tree(&root, budget(4, 10, 220)).expect("tree");
        assert!(tree.payload.truncated);
        assert!(tree.payload.serialized_bytes <= 220);
    }

    #[test]
    fn computer_use_tree_excludes_raw_platform_data() {
        let root = raw_node("window", "Main", Vec::new());
        let tree = compact_raw_tree(&root, budget(4, 10, 64 * 1024)).expect("tree");
        let serialized = serde_json::to_string(&tree.payload).expect("json");
        assert!(!serialized.contains("raw"));
        assert!(!serialized.contains("platform"));
    }

    #[test]
    fn node_actionability_maps_button_raw_actions_to_supported_act_types() {
        let root = raw_node("window", "Main", vec![raw_node("button", "OK", Vec::new())]);
        let tree = compact_raw_tree(&root, budget(4, 10, 64 * 1024)).expect("tree");
        let button = &tree.payload.nodes[1];
        assert!(button.supported_act_types.contains(&"press".to_owned()));
        assert_eq!(button.raw_actions, vec!["press".to_owned()]);
    }

    #[test]
    fn node_actionability_maps_textfield_actions_without_press() {
        let mut textfield = raw_node("textfield", "Search", Vec::new());
        textfield.actions = vec!["focus".to_owned(), "set_value".to_owned()];
        let root = raw_node("window", "Main", vec![textfield]);
        let tree = compact_raw_tree(&root, budget(4, 10, 64 * 1024)).expect("tree");
        let field = &tree.payload.nodes[1];
        assert!(field.supported_act_types.contains(&"focus".to_owned()));
        assert!(field.supported_act_types.contains(&"set_value".to_owned()));
        assert!(field.supported_act_types.contains(&"type_text".to_owned()));
        assert!(!field.supported_act_types.contains(&"press".to_owned()));
    }

    #[test]
    fn node_actionability_maps_table_cell_select_and_omits_large_raw_actions() {
        let mut cell = raw_node("table_cell", "Row 1", Vec::new());
        cell.actions = vec!["select".to_owned(), "x".repeat(300)];
        let root = raw_node("window", "Main", vec![cell]);
        let tree = compact_raw_tree(&root, budget(4, 10, 64 * 1024)).expect("tree");
        let cell = &tree.payload.nodes[1];
        assert!(cell.supported_act_types.contains(&"select".to_owned()));
        assert!(
            cell.supported_act_types
                .contains(&"perform_action".to_owned())
        );
        assert!(
            cell.raw_actions.is_empty(),
            "oversized raw action list should not be exposed"
        );
    }
}
