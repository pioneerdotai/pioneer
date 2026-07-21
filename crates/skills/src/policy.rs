use crate::compile::{SkillDefinition, SkillImplicitInvocationPolicy};
use crate::contract::SkillSourceKind;
use pioneer_protocol::SkillId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SkillPolicyKey {
    pub skill_id: SkillId,
}

impl SkillPolicyKey {
    pub fn new(skill_id: SkillId) -> Self {
        Self { skill_id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SkillPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_implicit_invocation: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EffectiveSkillPolicy {
    pub enabled: bool,
    pub allow_implicit_invocation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SkillPolicySet {
    #[serde(default)]
    pub global_by_key: HashMap<SkillPolicyKey, SkillPolicy>,
    #[serde(default)]
    pub workspace_by_key: HashMap<SkillPolicyKey, SkillPolicy>,
}

fn apply(base: &mut EffectiveSkillPolicy, patch: &SkillPolicy) {
    if let Some(enabled) = patch.enabled {
        base.enabled = enabled;
    }
    if let Some(allow_implicit_invocation) = patch.allow_implicit_invocation {
        base.allow_implicit_invocation = allow_implicit_invocation;
    }
}

pub fn merge_policy(skill_id: &SkillId, set: &SkillPolicySet) -> EffectiveSkillPolicy {
    let mut effective = EffectiveSkillPolicy {
        enabled: true,
        allow_implicit_invocation: false,
    };
    let key = SkillPolicyKey::new(skill_id.clone());

    if let Some(global) = set.global_by_key.get(&key) {
        apply(&mut effective, global);
    }
    if let Some(workspace) = set.workspace_by_key.get(&key) {
        apply(&mut effective, workspace);
    }

    effective
}

pub fn skill_implicit_invocation_editable(skill: &SkillDefinition) -> bool {
    !(matches!(&skill.identity.source_kind, SkillSourceKind::System)
        && matches!(
            skill.policy_hints.implicit_invocation,
            SkillImplicitInvocationPolicy::Required
        ))
}

pub fn apply_skill_policy_constraints(
    skill: &SkillDefinition,
    effective: &mut EffectiveSkillPolicy,
) {
    if !skill_implicit_invocation_editable(skill) {
        effective.allow_implicit_invocation = true;
    }
}

pub fn effective_policy_for_skill(
    skill: &SkillDefinition,
    set: &SkillPolicySet,
) -> EffectiveSkillPolicy {
    let mut effective = merge_policy(&skill.identity.skill_id, set);
    apply_skill_policy_constraints(skill, &mut effective);
    effective
}

#[cfg(test)]
mod tests {
    use super::{SkillPolicy, SkillPolicyKey, SkillPolicySet, merge_policy};
    use pioneer_protocol::SkillId;

    #[test]
    fn workspace_overrides_global() {
        let mut set = SkillPolicySet::default();
        let skill_id = SkillId::new("AAAAAAAAAAAAAAAAAAAAA").unwrap();
        let key = SkillPolicyKey::new(skill_id.clone());
        set.global_by_key.insert(
            key.clone(),
            SkillPolicy {
                enabled: Some(true),
                allow_implicit_invocation: Some(false),
            },
        );
        set.workspace_by_key.insert(
            key,
            SkillPolicy {
                enabled: Some(false),
                allow_implicit_invocation: Some(true),
            },
        );

        let effective = merge_policy(&skill_id, &set);
        assert!(!effective.enabled);
        assert!(effective.allow_implicit_invocation);
    }
}
