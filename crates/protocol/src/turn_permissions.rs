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
            allowed_tools: Vec::new(),
            denied_tools: Vec::new(),
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
            allowed_tools: Vec::new(),
            denied_tools: Vec::new(),
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
        allowed_tools: intersect_allowed_list(&left.allowed_tools, &right.allowed_tools),
        denied_tools: union_unique(&left.denied_tools, &right.denied_tools),
        allowed_paths: intersect_allowed_list(&left.allowed_paths, &right.allowed_paths),
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

fn intersect_allowed_list(left: &[String], right: &[String]) -> Vec<String> {
    if left.is_empty() {
        return normalize_unique(right);
    }
    if right.is_empty() {
        return normalize_unique(left);
    }
    let right_normalized = normalize_unique(right);
    normalize_unique(left)
        .into_iter()
        .filter(|value| right_normalized.iter().any(|right| right == value))
        .collect()
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
