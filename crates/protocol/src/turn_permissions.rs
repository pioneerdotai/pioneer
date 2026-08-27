use crate::turn::{
    PermissionBehavior, ToolPermissionPolicySnapshot, TurnPermissionMode, TurnPermissionProfileCap,
    TurnPermissionProfileSelection, TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
};

pub fn compile_turn_permission_profile(
    mode: TurnPermissionMode,
    source: TurnPermissionProfileSource,
) -> TurnPermissionProfileSnapshot {
    TurnPermissionProfileSnapshot {
        mode,
        source,
        effective_policy: permission_policy_for_mode(mode),
    }
}

pub fn default_turn_permission_profile_snapshot() -> TurnPermissionProfileSnapshot {
    compile_turn_permission_profile(
        TurnPermissionMode::FullAccess,
        TurnPermissionProfileSource::Defaulted,
    )
}

pub fn composer_turn_permission_profile_snapshot(
    selection: &TurnPermissionProfileSelection,
) -> TurnPermissionProfileSnapshot {
    compile_turn_permission_profile(selection.mode, TurnPermissionProfileSource::Composer)
}

pub fn inherited_turn_permission_profile_snapshot(
    mode: TurnPermissionMode,
) -> TurnPermissionProfileSnapshot {
    compile_turn_permission_profile(mode, TurnPermissionProfileSource::InheritedFromParentTurn)
}

pub fn system_turn_permission_profile_snapshot(
    mode: TurnPermissionMode,
) -> TurnPermissionProfileSnapshot {
    compile_turn_permission_profile(mode, TurnPermissionProfileSource::System)
}

pub fn task_permission_cap_for_mode(mode: TurnPermissionMode) -> TurnPermissionProfileCap {
    TurnPermissionProfileCap {
        mode,
        effective_policy: permission_policy_for_mode(mode),
    }
}

pub fn task_permission_cap_from_snapshot(
    profile: &TurnPermissionProfileSnapshot,
) -> TurnPermissionProfileCap {
    TurnPermissionProfileCap {
        mode: profile.mode,
        effective_policy: profile.effective_policy.clone(),
    }
}

pub fn task_permission_cap_snapshot(
    cap: &TurnPermissionProfileCap,
) -> TurnPermissionProfileSnapshot {
    TurnPermissionProfileSnapshot {
        mode: cap.mode,
        source: TurnPermissionProfileSource::TaskPermissionCap,
        effective_policy: cap.effective_policy.clone(),
    }
}

pub fn inherited_turn_permission_profile_from_snapshot(
    profile: &TurnPermissionProfileSnapshot,
) -> TurnPermissionProfileSnapshot {
    let mut inherited = profile.clone();
    inherited.source = TurnPermissionProfileSource::InheritedFromParentTurn;
    inherited
}

pub fn resolve_turn_permission_profile(
    selection: Option<&TurnPermissionProfileSelection>,
) -> TurnPermissionProfileSnapshot {
    match selection {
        Some(selection) => composer_turn_permission_profile_snapshot(selection),
        None => default_turn_permission_profile_snapshot(),
    }
}

pub fn permission_policy_for_mode(mode: TurnPermissionMode) -> ToolPermissionPolicySnapshot {
    match mode {
        TurnPermissionMode::FullAccess => {
            ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow)
        }
        TurnPermissionMode::AutoAcceptEdits => ToolPermissionPolicySnapshot {
            default_behavior: PermissionBehavior::Ask,
            file_read: PermissionBehavior::Allow,
            file_write: PermissionBehavior::Allow,
            shell_command: PermissionBehavior::Ask,
            network: PermissionBehavior::Ask,
            mcp_read: PermissionBehavior::Allow,
            mcp_write_or_unknown: PermissionBehavior::Ask,
            dynamic_skill_tool: PermissionBehavior::Ask,
            computer_use: PermissionBehavior::Ask,
            task_subagent: PermissionBehavior::Ask,
            memory_write: PermissionBehavior::Ask,
            agent_action: PermissionBehavior::Ask,
            allowed_tools_restricted: false,
            allowed_tools: Vec::new(),
            denied_tools: Vec::new(),
            allowed_paths_restricted: false,
            allowed_paths: Vec::new(),
        },
        TurnPermissionMode::Supervised => ToolPermissionPolicySnapshot {
            default_behavior: PermissionBehavior::Ask,
            file_read: PermissionBehavior::Allow,
            file_write: PermissionBehavior::Ask,
            shell_command: PermissionBehavior::Ask,
            network: PermissionBehavior::Ask,
            mcp_read: PermissionBehavior::Allow,
            mcp_write_or_unknown: PermissionBehavior::Ask,
            dynamic_skill_tool: PermissionBehavior::Ask,
            computer_use: PermissionBehavior::Ask,
            task_subagent: PermissionBehavior::Ask,
            memory_write: PermissionBehavior::Ask,
            agent_action: PermissionBehavior::Ask,
            allowed_tools_restricted: false,
            allowed_tools: Vec::new(),
            denied_tools: Vec::new(),
            allowed_paths_restricted: false,
            allowed_paths: Vec::new(),
        },
    }
}

pub fn most_restrictive_permission_behavior(
    left: PermissionBehavior,
    right: PermissionBehavior,
) -> PermissionBehavior {
    match (left, right) {
        (PermissionBehavior::Deny, _) | (_, PermissionBehavior::Deny) => PermissionBehavior::Deny,
        (PermissionBehavior::Ask, _) | (_, PermissionBehavior::Ask) => PermissionBehavior::Ask,
        (PermissionBehavior::Allow, PermissionBehavior::Allow) => PermissionBehavior::Allow,
    }
}

pub fn most_restrictive_turn_permission_mode(
    left: TurnPermissionMode,
    right: TurnPermissionMode,
) -> TurnPermissionMode {
    match (left, right) {
        (TurnPermissionMode::Supervised, _) | (_, TurnPermissionMode::Supervised) => {
            TurnPermissionMode::Supervised
        }
        (TurnPermissionMode::AutoAcceptEdits, _) | (_, TurnPermissionMode::AutoAcceptEdits) => {
            TurnPermissionMode::AutoAcceptEdits
        }
        (TurnPermissionMode::FullAccess, TurnPermissionMode::FullAccess) => {
            TurnPermissionMode::FullAccess
        }
    }
}

pub fn intersect_tool_permission_policies(
    left: &ToolPermissionPolicySnapshot,
    right: &ToolPermissionPolicySnapshot,
) -> ToolPermissionPolicySnapshot {
    let (allowed_tools, allowed_tools_restricted) = intersect_exact_allow_list(
        &left.allowed_tools,
        left.allowed_tools_restricted,
        &right.allowed_tools,
        right.allowed_tools_restricted,
    );
    let (allowed_paths, allowed_paths_restricted) = intersect_path_allow_list(
        &left.allowed_paths,
        left.allowed_paths_restricted,
        &right.allowed_paths,
        right.allowed_paths_restricted,
    );
    ToolPermissionPolicySnapshot {
        default_behavior: most_restrictive_permission_behavior(
            left.default_behavior,
            right.default_behavior,
        ),
        file_read: most_restrictive_permission_behavior(left.file_read, right.file_read),
        file_write: most_restrictive_permission_behavior(left.file_write, right.file_write),
        shell_command: most_restrictive_permission_behavior(
            left.shell_command,
            right.shell_command,
        ),
        network: most_restrictive_permission_behavior(left.network, right.network),
        mcp_read: most_restrictive_permission_behavior(left.mcp_read, right.mcp_read),
        mcp_write_or_unknown: most_restrictive_permission_behavior(
            left.mcp_write_or_unknown,
            right.mcp_write_or_unknown,
        ),
        dynamic_skill_tool: most_restrictive_permission_behavior(
            left.dynamic_skill_tool,
            right.dynamic_skill_tool,
        ),
        computer_use: most_restrictive_permission_behavior(left.computer_use, right.computer_use),
        task_subagent: most_restrictive_permission_behavior(
            left.task_subagent,
            right.task_subagent,
        ),
        memory_write: most_restrictive_permission_behavior(left.memory_write, right.memory_write),
        agent_action: most_restrictive_permission_behavior(left.agent_action, right.agent_action),
        allowed_tools_restricted,
        allowed_tools,
        denied_tools: union_unique(&left.denied_tools, &right.denied_tools),
        allowed_paths_restricted,
        allowed_paths,
    }
}

pub fn intersect_turn_permission_profiles(
    left: &TurnPermissionProfileSnapshot,
    right: &TurnPermissionProfileSnapshot,
    source: TurnPermissionProfileSource,
) -> TurnPermissionProfileSnapshot {
    TurnPermissionProfileSnapshot {
        mode: most_restrictive_turn_permission_mode(left.mode, right.mode),
        source,
        effective_policy: intersect_tool_permission_policies(
            &left.effective_policy,
            &right.effective_policy,
        ),
    }
}

fn intersect_exact_allow_list(
    left: &[String],
    left_restricted: bool,
    right: &[String],
    right_restricted: bool,
) -> (Vec<String>, bool) {
    let left_restricted = left_restricted || !left.is_empty();
    let right_restricted = right_restricted || !right.is_empty();
    match (left_restricted, right_restricted) {
        (false, false) => (Vec::new(), false),
        (true, false) => (normalize_unique(left), true),
        (false, true) => (normalize_unique(right), true),
        (true, true) => {
            let right_normalized = normalize_unique(right);
            let intersection = normalize_unique(left)
                .into_iter()
                .filter(|value| right_normalized.iter().any(|right| right == value))
                .collect();
            (intersection, true)
        }
    }
}

fn intersect_path_allow_list(
    left: &[String],
    left_restricted: bool,
    right: &[String],
    right_restricted: bool,
) -> (Vec<String>, bool) {
    let left_restricted = left_restricted || !left.is_empty();
    let right_restricted = right_restricted || !right.is_empty();
    match (left_restricted, right_restricted) {
        (false, false) => (Vec::new(), false),
        (true, false) => (normalize_unique_paths(left), true),
        (false, true) => (normalize_unique_paths(right), true),
        (true, true) => {
            let left = normalize_unique_paths(left);
            let right = normalize_unique_paths(right);
            let mut intersection = Vec::new();
            for left_path in &left {
                for right_path in &right {
                    let narrower = if path_within(left_path, right_path) {
                        Some(left_path)
                    } else if path_within(right_path, left_path) {
                        Some(right_path)
                    } else {
                        None
                    };
                    if let Some(narrower) = narrower
                        && !intersection.iter().any(|existing| existing == narrower)
                    {
                        intersection.push(narrower.clone());
                    }
                }
            }
            (intersection, true)
        }
    }
}

fn union_unique(left: &[String], right: &[String]) -> Vec<String> {
    let mut values = normalize_unique(left);
    for value in normalize_unique(right) {
        if !values.iter().any(|existing| existing == &value) {
            values.push(value);
        }
    }
    values
}

fn normalize_unique(values: &[String]) -> Vec<String> {
    let mut normalized = Vec::<String>::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !normalized.iter().any(|existing| existing == trimmed) {
            normalized.push(trimmed.to_owned());
        }
    }
    normalized
}

fn normalize_unique_paths(values: &[String]) -> Vec<String> {
    let mut normalized = Vec::<String>::new();
    for value in values {
        let value = normalize_policy_path(value);
        if value.is_empty() {
            continue;
        }
        if !normalized.iter().any(|existing| existing == &value) {
            normalized.push(value);
        }
    }
    normalized
}

fn normalize_policy_path(value: &str) -> String {
    use std::path::{Component, Path, PathBuf};

    let mut normalized = PathBuf::new();
    for component in Path::new(value.trim()).components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !matches!(
                    normalized.components().next_back(),
                    Some(Component::RootDir | Component::Prefix(_))
                ) {
                    normalized.pop();
                }
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized.to_string_lossy().into_owned()
}

fn path_within(path: &str, allowed: &str) -> bool {
    let path = std::path::Path::new(path);
    let allowed = std::path::Path::new(allowed);
    path == allowed || path.starts_with(allowed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disjoint_tool_allow_lists_intersect_to_explicit_deny_all() {
        let mut left = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        left.allowed_tools_restricted = true;
        left.allowed_tools = vec!["read_file".to_owned()];
        let mut right = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        right.allowed_tools_restricted = true;
        right.allowed_tools = vec!["list_dir".to_owned()];

        let intersection = intersect_tool_permission_policies(&left, &right);

        assert!(intersection.allowed_tools_restricted);
        assert!(intersection.allowed_tools.is_empty());
        let encoded = serde_json::to_value(&intersection).expect("intersection should encode");
        assert_eq!(encoded["allowed_tools_restricted"], true);
        let decoded: ToolPermissionPolicySnapshot =
            serde_json::from_value(encoded).expect("intersection should decode");
        assert!(decoded.allowed_tools_restricted);
        assert!(decoded.allowed_tools.is_empty());
    }

    #[test]
    fn nested_path_allow_lists_intersect_to_the_narrower_component_scope() {
        let mut left = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        left.allowed_paths_restricted = true;
        left.allowed_paths = vec!["/workspace/src".to_owned()];
        let mut right = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        right.allowed_paths_restricted = true;
        right.allowed_paths = vec!["/workspace/src/./generated/../lib".to_owned()];

        let intersection = intersect_tool_permission_policies(&left, &right);

        assert!(intersection.allowed_paths_restricted);
        assert_eq!(intersection.allowed_paths, vec!["/workspace/src/lib"]);
    }

    #[test]
    fn disjoint_path_allow_lists_intersect_to_explicit_deny_all() {
        let mut left = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        left.allowed_paths_restricted = true;
        left.allowed_paths = vec!["/workspace/src".to_owned()];
        let mut right = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
        right.allowed_paths_restricted = true;
        right.allowed_paths = vec!["/workspace/tests".to_owned()];

        let intersection = intersect_tool_permission_policies(&left, &right);

        assert!(intersection.allowed_paths_restricted);
        assert!(intersection.allowed_paths.is_empty());
    }

    #[test]
    fn legacy_nonempty_allow_lists_decode_as_restricted() {
        let encoded =
            serde_json::to_value(ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow))
                .expect("baseline policy should encode");
        let mut encoded = encoded.as_object().expect("policy object").clone();
        encoded.insert("allowed_tools".to_owned(), serde_json::json!(["read_file"]));
        encoded.insert(
            "allowed_paths".to_owned(),
            serde_json::json!(["/workspace/src"]),
        );

        let decoded: ToolPermissionPolicySnapshot =
            serde_json::from_value(serde_json::Value::Object(encoded))
                .expect("legacy policy should decode");

        assert!(decoded.allowed_tools_restricted);
        assert!(decoded.allowed_paths_restricted);
    }
}
