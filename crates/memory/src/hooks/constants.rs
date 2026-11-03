pub const MEMORY_SEARCH_TOOL: &str = "memory_search";
pub const MEMORY_LIST_TOOL: &str = "memory_list";
pub const MEMORY_GET_TOOL: &str = "memory_get";
pub const MEMORY_REMEMBER_TOOL: &str = "memory_remember";
pub const MEMORY_FORGET_TOOL: &str = "memory_forget";

pub(super) const MEMORY_POLICY_DOMAIN: &str = "memory";
pub(super) const MEMORY_TURN_POLICY_KEY: &str = "turn_policy";
// Stable IDs are retained for persisted post-turn policy snapshots. The hook is local.
pub(super) const MEMORY_POLICY_HOOK_ID: &str = "memory.policy_classifier";
pub(super) const MEMORY_POLICY_SUBSCRIPTION_ID: &str = "memory.policy_classifier.default";
pub(super) const MEMORY_TURN_POLICY_OVERRIDE_METADATA_KEY: &str =
    "memory.policy_classifier.override";
pub(super) const MEMORY_TURN_POLICY_DEFAULT_METADATA_KEY: &str =
    "memory.policy_classifier.default_policy";
pub(super) const MEMORY_TOOL_BUNDLE_HOOK_ID: &str = "memory.tool_bundle";
pub(super) const MEMORY_TOOL_BUNDLE_SUBSCRIPTION_ID: &str = "memory.tool_bundle.default";
pub(super) const MEMORY_TOOL_BUNDLE_CONTRIBUTION_ID_PREFIX: &str =
    "memory.tool_bundle.contribution";
pub(super) const MEMORY_TOOL_BUNDLE_ID_PREFIX: &str = "memory.tool_bundle.bundle";
pub(super) const MEMORY_PROMPT_CONTRACT_HOOK_ID: &str = "memory.prompt_contract";
pub(super) const MEMORY_PROMPT_CONTRACT_SUBSCRIPTION_ID: &str = "memory.prompt_contract.default";
pub(super) const MEMORY_PROMPT_CONTRACT_CONTRIBUTION_ID: &str = "memory.prompt_contract.section";
pub(super) const MEMORY_PROMPT_CONTRACT_SECTION_ID: &str = "memory_recall";
pub(super) const MEMORY_POST_TURN_EXTRACTOR_HOOK_ID: &str = "memory.post_turn_extractor";
pub(super) const MEMORY_POST_TURN_EXTRACTOR_SUBSCRIPTION_ID: &str =
    "memory.post_turn_extractor.default";
pub(super) const MEMORY_POST_TURN_EXTRACTOR_VERSION: &str = "post_turn_semantic_v1";
pub const MEMORY_ACCEPTED_TASK_RESULT_POST_TURN_FEATURE_FLAG: &str =
    "memory.accepted_task_result_post_turn";
pub(super) const MEMORY_DEFAULT_USER_SCOPE_KEY: &str = "default";
