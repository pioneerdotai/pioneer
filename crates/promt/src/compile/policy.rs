use crate::profile::PromptProfile;

pub fn include_workspace_context(profile: PromptProfile) -> bool {
    !matches!(profile, PromptProfile::AssistantNone)
}

pub fn include_safety(profile: PromptProfile) -> bool {
    !matches!(profile, PromptProfile::AssistantNone)
}

pub fn include_artifact_output_contract(profile: PromptProfile) -> bool {
    !matches!(profile, PromptProfile::AssistantNone)
}

pub fn include_tool_usage_policy(profile: PromptProfile) -> bool {
    !matches!(profile, PromptProfile::AssistantNone)
}

pub fn include_tool_recovery_policy(
    profile: PromptProfile,
    include_tool_recovery_policy: bool,
) -> bool {
    include_tool_recovery_policy && !matches!(profile, PromptProfile::AssistantNone)
}

pub fn include_identity_base(_profile: PromptProfile) -> bool {
    true
}
