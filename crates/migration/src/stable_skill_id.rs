pub const LEGACY_SKILL_INSTALLATION_TABLE: &str = "_stable_skill_id_legacy_skill_installation";
pub const LEGACY_SKILL_WORKSPACE_POLICY_TABLE: &str =
    "_stable_skill_id_legacy_skill_workspace_policy";
pub const LEGACY_TURN_SKILL_BINDING_TABLE: &str = "_stable_skill_id_legacy_turn_skill_binding";
pub const LEGACY_SKILL_AUDIT_EVENT_TABLE: &str = "_stable_skill_id_legacy_skill_audit_event";
pub const LEGACY_SKILL_DEPENDENCY_SNAPSHOT_TABLE: &str =
    "_stable_skill_id_legacy_skill_dependency_snapshot";

pub const LEGACY_RELATION_TABLES: [(&str, &str); 5] = [
    ("skill_installation", LEGACY_SKILL_INSTALLATION_TABLE),
    (
        "skill_workspace_policy",
        LEGACY_SKILL_WORKSPACE_POLICY_TABLE,
    ),
    ("turn_skill_binding", LEGACY_TURN_SKILL_BINDING_TABLE),
    ("skill_audit_event", LEGACY_SKILL_AUDIT_EVENT_TABLE),
    (
        "skill_dependency_snapshot",
        LEGACY_SKILL_DEPENDENCY_SNAPSHOT_TABLE,
    ),
];
