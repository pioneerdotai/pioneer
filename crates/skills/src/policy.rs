use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SkillPolicyKey {
    pub slug: String,
    pub source_kind: String,
}

impl SkillPolicyKey {
    pub fn new(slug: impl Into<String>, source_kind: impl Into<String>) -> Self {
        Self {
            slug: slug.into(),
            source_kind: source_kind.into(),
        }
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

pub fn merge_policy(slug: &str, source_kind: &str, set: &SkillPolicySet) -> EffectiveSkillPolicy {
    let mut effective = EffectiveSkillPolicy {
        enabled: true,
        allow_implicit_invocation: false,
    };
    let key = SkillPolicyKey::new(slug.to_owned(), source_kind.to_owned());

    if let Some(global) = set.global_by_key.get(&key) {
        apply(&mut effective, global);
    }
    if let Some(workspace) = set.workspace_by_key.get(&key) {
        apply(&mut effective, workspace);
    }

    effective
}

#[cfg(test)]
mod tests {
    use super::{SkillPolicy, SkillPolicyKey, SkillPolicySet, merge_policy};

    #[test]
    fn workspace_overrides_global() {
        let mut set = SkillPolicySet::default();
        let key = SkillPolicyKey::new("pioneer/test", "workspace");
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

        let effective = merge_policy("pioneer/test", "workspace", &set);
        assert!(!effective.enabled);
        assert!(effective.allow_implicit_invocation);
    }
}
