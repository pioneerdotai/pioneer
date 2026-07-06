use pioneer_protocol::{
    PermissionBehavior, ToolPermissionPolicySnapshot, TurnPermissionMode,
    TurnPermissionProfileSnapshot,
};

pub fn current_permission_guidance(profile: &TurnPermissionProfileSnapshot) -> Option<String> {
    let policy = &profile.effective_policy;
    let narrowed = profile_has_narrowing(policy);
    if profile.mode == TurnPermissionMode::FullAccess && !narrowed {
        return None;
    }

    let mut lines = permission_profile_summary_lines(profile, PermissionSummaryStyle::Guidance);
    lines.push(
        "- Permission prompts are enforced by Pioneer; continue with allowed alternatives if an action is denied.".to_owned(),
    );

    Some(lines.join("\n"))
}

pub fn effective_permission_summary(profile: &TurnPermissionProfileSnapshot) -> Option<String> {
    let policy = &profile.effective_policy;
    if profile.mode == TurnPermissionMode::FullAccess && !profile_has_narrowing(policy) {
        return None;
    }

    Some(permission_profile_summary_lines(profile, PermissionSummaryStyle::Snapshot).join("\n"))
}

#[derive(Debug, Clone, Copy)]
enum PermissionSummaryStyle {
    Guidance,
    Snapshot,
}

fn permission_profile_summary_lines(
    profile: &TurnPermissionProfileSnapshot,
    style: PermissionSummaryStyle,
) -> Vec<String> {
    let policy = &profile.effective_policy;
    let mut lines = vec![format!("- mode: {}", profile.mode.as_str())];

    if matches!(style, PermissionSummaryStyle::Snapshot) {
        lines.push(format!("- source: {}", profile.source.as_str()));
    } else {
        match profile.mode {
            TurnPermissionMode::FullAccess => {
                lines.push(
                    "- This turn normally has full access, but the listed tool policy still applies."
                        .to_owned(),
                );
            }
            TurnPermissionMode::AutoAcceptEdits => {
                lines.push("- File edits may be allowed automatically.".to_owned());
                lines.push("- Shell commands, network access, task/subagent launches, and other external actions may require user approval.".to_owned());
            }
            TurnPermissionMode::Supervised => {
                lines.push("- Commands, file changes, network access, task/subagent launches, and other external actions may require user approval before execution.".to_owned());
            }
        }
    }

    match style {
        PermissionSummaryStyle::Guidance => {
            push_behavior_line_if_restricted(&mut lines, "file_read", policy.file_read);
            push_behavior_line_if_restricted(&mut lines, "file_write", policy.file_write);
            push_behavior_line_if_restricted(&mut lines, "shell_command", policy.shell_command);
            push_behavior_line_if_restricted(&mut lines, "network", policy.network);
            push_behavior_line_if_restricted(
                &mut lines,
                "mcp_write_or_unknown",
                policy.mcp_write_or_unknown,
            );
            push_behavior_line_if_restricted(
                &mut lines,
                "dynamic_skill_tool",
                policy.dynamic_skill_tool,
            );
            push_behavior_line_if_restricted(&mut lines, "task_subagent", policy.task_subagent);
        }
        PermissionSummaryStyle::Snapshot => {
            push_behavior_line(&mut lines, "file_read", policy.file_read);
            push_behavior_line(&mut lines, "file_write", policy.file_write);
            push_behavior_line(&mut lines, "shell_command", policy.shell_command);
            push_behavior_line(&mut lines, "network", policy.network);
            push_behavior_line(&mut lines, "mcp_read", policy.mcp_read);
            push_behavior_line(
                &mut lines,
                "mcp_write_or_unknown",
                policy.mcp_write_or_unknown,
            );
            push_behavior_line(&mut lines, "dynamic_skill_tool", policy.dynamic_skill_tool);
            push_behavior_line(&mut lines, "task_subagent", policy.task_subagent);
        }
    }
    push_optional_list(&mut lines, "allowed_tools", &policy.allowed_tools);
    push_optional_list(&mut lines, "denied_tools", &policy.denied_tools);
    push_optional_list(&mut lines, "allowed_paths", &policy.allowed_paths);

    lines
}

fn profile_has_narrowing(policy: &ToolPermissionPolicySnapshot) -> bool {
    !policy.allowed_tools.is_empty()
        || !policy.denied_tools.is_empty()
        || !policy.allowed_paths.is_empty()
        || matches!(
            policy.file_read,
            PermissionBehavior::Ask | PermissionBehavior::Deny
        )
        || matches!(
            policy.file_write,
            PermissionBehavior::Ask | PermissionBehavior::Deny
        )
        || matches!(
            policy.shell_command,
            PermissionBehavior::Ask | PermissionBehavior::Deny
        )
        || matches!(
            policy.network,
            PermissionBehavior::Ask | PermissionBehavior::Deny
        )
        || matches!(
            policy.mcp_read,
            PermissionBehavior::Ask | PermissionBehavior::Deny
        )
        || matches!(
            policy.mcp_write_or_unknown,
            PermissionBehavior::Ask | PermissionBehavior::Deny
        )
        || matches!(
            policy.dynamic_skill_tool,
            PermissionBehavior::Ask | PermissionBehavior::Deny
        )
        || matches!(
            policy.task_subagent,
            PermissionBehavior::Ask | PermissionBehavior::Deny
        )
}

fn push_behavior_line_if_restricted(
    lines: &mut Vec<String>,
    label: &str,
    behavior: PermissionBehavior,
) {
    if behavior != PermissionBehavior::Allow {
        push_behavior_line(lines, label, behavior);
    }
}

fn push_behavior_line(lines: &mut Vec<String>, label: &str, behavior: PermissionBehavior) {
    lines.push(format!("- {label}: {}", behavior.as_str()));
}

fn push_optional_list(lines: &mut Vec<String>, label: &str, values: &[String]) {
    if !values.is_empty() {
        lines.push(format!("- {label}: {}", values.join(", ")));
    }
}

#[cfg(test)]
mod tests {
    use super::{current_permission_guidance, effective_permission_summary};
    use pioneer_protocol::{
        TurnPermissionMode, TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
    };

    #[test]
    fn current_permission_guidance_omits_default_full_access() {
        let profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::FullAccess,
            TurnPermissionProfileSource::Defaulted,
        );

        assert!(current_permission_guidance(&profile).is_none());
    }

    #[test]
    fn current_permission_guidance_mentions_restricted_approval_boundary() {
        let profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::Supervised,
            TurnPermissionProfileSource::Composer,
        );

        let guidance = current_permission_guidance(&profile).expect("restricted guidance");

        assert_eq!(
            guidance,
            "- mode: supervised\n- Commands, file changes, network access, task/subagent launches, and other external actions may require user approval before execution.\n- file_write: ask\n- shell_command: ask\n- network: ask\n- mcp_write_or_unknown: ask\n- dynamic_skill_tool: ask\n- task_subagent: ask\n- Permission prompts are enforced by Pioneer; continue with allowed alternatives if an action is denied."
        );
        assert!(guidance.contains("may require user approval"));
        assert!(guidance.contains("enforced by Pioneer"));
        assert!(!guidance.contains("ToolPermissionPolicySnapshot"));
    }

    #[test]
    fn effective_permission_summary_includes_all_behavior_axes() {
        let profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::Supervised,
            TurnPermissionProfileSource::TaskPermissionCap,
        );

        let summary = effective_permission_summary(&profile).expect("restricted summary");

        assert!(summary.contains("- mode: supervised"));
        assert!(summary.contains("- source: task_permission_cap"));
        assert!(summary.contains("- file_read: allow"));
        assert!(summary.contains("- file_write: ask"));
        assert!(summary.contains("- shell_command: ask"));
        assert!(summary.contains("- network: ask"));
        assert!(summary.contains("- mcp_read: allow"));
        assert!(summary.contains("- mcp_write_or_unknown: ask"));
        assert!(summary.contains("- dynamic_skill_tool: ask"));
        assert!(summary.contains("- task_subagent: ask"));
    }
}
