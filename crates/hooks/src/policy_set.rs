use crate::{
    HookContribution, HookDiagnostic, HookDiagnosticCode, HookDiagnosticMessage,
    HookDiagnosticSeverity, HookDomain, HookMetadata, HookPolicyKey, HookValue, PolicyContribution,
};
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct HookPolicyKeyRef {
    pub domain: HookDomain,
    pub key: HookPolicyKey,
}

impl HookPolicyKeyRef {
    pub fn new(domain: HookDomain, key: HookPolicyKey) -> Self {
        Self { domain, key }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookPolicyEntry {
    pub domain: HookDomain,
    pub key: HookPolicyKey,
    pub value: HookValue,
    pub priority: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
}

impl From<PolicyContribution> for HookPolicyEntry {
    fn from(contribution: PolicyContribution) -> Self {
        Self {
            domain: contribution.domain,
            key: contribution.key,
            value: contribution.value,
            priority: contribution.priority,
            diagnostics: contribution.diagnostics,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookPolicySet {
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        serialize_with = "serialize_policy_entries",
        deserialize_with = "deserialize_policy_entries"
    )]
    pub entries: BTreeMap<HookPolicyKeyRef, HookPolicyEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
}

impl HookPolicySet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.diagnostics.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn get(&self, domain: &HookDomain, key: &HookPolicyKey) -> Option<&HookPolicyEntry> {
        self.entries
            .get(&HookPolicyKeyRef::new(domain.clone(), key.clone()))
    }

    pub fn entries(&self) -> impl Iterator<Item = &HookPolicyEntry> {
        self.entries.values()
    }

    pub fn merge_contributions(
        contributions: impl IntoIterator<Item = PolicyContribution>,
    ) -> Self {
        let mut entries = BTreeMap::new();
        let mut merge_diagnostics = Vec::new();

        for contribution in contributions {
            let incoming = HookPolicyEntry::from(contribution);
            let key_ref = HookPolicyKeyRef::new(incoming.domain.clone(), incoming.key.clone());
            let Some(existing) = entries.get_mut(&key_ref) else {
                entries.insert(key_ref, incoming);
                continue;
            };

            if incoming.priority > existing.priority {
                *existing = incoming;
                continue;
            }

            if incoming.priority < existing.priority {
                continue;
            }

            if incoming.value == existing.value {
                existing.diagnostics.extend(incoming.diagnostics);
                continue;
            }

            existing.diagnostics.extend(incoming.diagnostics);
            merge_diagnostics.push(policy_conflict_diagnostic());
        }

        let diagnostics = entries
            .values()
            .flat_map(|entry| entry.diagnostics.iter().cloned())
            .chain(merge_diagnostics)
            .collect();

        Self {
            entries,
            diagnostics,
        }
    }

    pub fn merge_hook_contributions(
        contributions: impl IntoIterator<Item = HookContribution>,
    ) -> Self {
        Self::merge_contributions(contributions.into_iter().filter_map(|contribution| {
            match contribution {
                HookContribution::Policy(policy) => Some(policy),
                HookContribution::PromptContext(_)
                | HookContribution::PromptSection(_)
                | HookContribution::ToolBundle(_)
                | HookContribution::PromptManifestDiagnostic(_)
                | HookContribution::Audit(_)
                | HookContribution::BackgroundJob(_)
                | HookContribution::Noop => None,
            }
        }))
    }
}

fn policy_conflict_diagnostic() -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new("policy.merge.conflict")
            .expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(
            "equal-priority policy contributions conflicted; first deterministic value kept",
        )
        .expect("static diagnostic message is valid"),
        severity: HookDiagnosticSeverity::Warning,
        safe_for_user: true,
        metadata: HookMetadata::default(),
    }
}

fn serialize_policy_entries<S>(
    entries: &BTreeMap<HookPolicyKeyRef, HookPolicyEntry>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut sequence = serializer.serialize_seq(Some(entries.len()))?;
    for entry in entries.values() {
        sequence.serialize_element(entry)?;
    }
    sequence.end()
}

fn deserialize_policy_entries<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<HookPolicyKeyRef, HookPolicyEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    struct PolicyEntriesVisitor;

    impl<'de> Visitor<'de> for PolicyEntriesVisitor {
        type Value = BTreeMap<HookPolicyKeyRef, HookPolicyEntry>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a list of hook policy entries")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut entries = BTreeMap::new();
            while let Some(entry) = sequence.next_element::<HookPolicyEntry>()? {
                entries.insert(
                    HookPolicyKeyRef::new(entry.domain.clone(), entry.key.clone()),
                    entry,
                );
            }
            Ok(entries)
        }
    }

    deserializer.deserialize_seq(PolicyEntriesVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AuditContribution, HookAuditEventKind, HookPromptContent, HookPromptSectionTitle,
        HookSectionId, PromptSectionContribution,
    };

    fn domain(value: &str) -> HookDomain {
        HookDomain::new(value).expect("valid domain")
    }

    fn key(value: &str) -> HookPolicyKey {
        HookPolicyKey::new(value).expect("valid policy key")
    }

    fn diagnostic(code: &str) -> HookDiagnostic {
        HookDiagnostic {
            code: HookDiagnosticCode::new(code).expect("valid code"),
            message: HookDiagnosticMessage::new("diagnostic").expect("valid message"),
            severity: HookDiagnosticSeverity::Info,
            safe_for_user: true,
            metadata: HookMetadata::default(),
        }
    }

    fn policy(
        domain_value: &str,
        key_value: &str,
        value: HookValue,
        priority: i32,
    ) -> PolicyContribution {
        PolicyContribution {
            domain: domain(domain_value),
            key: key(key_value),
            value,
            priority,
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn policy_set_empty_is_default() {
        assert_eq!(HookPolicySet::empty(), HookPolicySet::default());
        assert!(HookPolicySet::default().is_empty());
        assert_eq!(HookPolicySet::default().len(), 0);
    }

    #[test]
    fn policy_set_merges_single_contribution() {
        let set = HookPolicySet::merge_contributions([policy(
            "test",
            "mode",
            HookValue::Text("strict".to_owned()),
            10,
        )]);

        let entry = set
            .get(&domain("test"), &key("mode"))
            .expect("policy entry exists");
        assert_eq!(entry.value, HookValue::Text("strict".to_owned()));
        assert_eq!(entry.priority, 10);
    }

    #[test]
    fn higher_priority_policy_contribution_wins() {
        let set = HookPolicySet::merge_contributions([
            policy("test", "mode", HookValue::Text("weak".to_owned()), 0),
            policy("test", "mode", HookValue::Text("strong".to_owned()), 10),
        ]);

        let entry = set
            .get(&domain("test"), &key("mode"))
            .expect("policy entry exists");
        assert_eq!(entry.value, HookValue::Text("strong".to_owned()));
        assert_eq!(entry.priority, 10);
    }

    #[test]
    fn lower_priority_policy_contribution_does_not_override() {
        let set = HookPolicySet::merge_contributions([
            policy("test", "mode", HookValue::Text("strong".to_owned()), 10),
            policy("test", "mode", HookValue::Text("weak".to_owned()), 0),
        ]);

        let entry = set
            .get(&domain("test"), &key("mode"))
            .expect("policy entry exists");
        assert_eq!(entry.value, HookValue::Text("strong".to_owned()));
        assert_eq!(entry.priority, 10);
    }

    #[test]
    fn equal_priority_identical_values_merge_without_conflict() {
        let mut first = policy("test", "mode", HookValue::Text("strict".to_owned()), 10);
        first.diagnostics.push(diagnostic("test.first"));
        let mut second = policy("test", "mode", HookValue::Text("strict".to_owned()), 10);
        second.diagnostics.push(diagnostic("test.second"));

        let set = HookPolicySet::merge_contributions([first, second]);

        let entry = set
            .get(&domain("test"), &key("mode"))
            .expect("policy entry exists");
        assert_eq!(entry.value, HookValue::Text("strict".to_owned()));
        assert_eq!(entry.diagnostics.len(), 2);
        assert!(
            !set.diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code.as_str() == "policy.merge.conflict" })
        );
    }

    #[test]
    fn equal_priority_conflicting_values_keep_first_and_emit_diagnostic() {
        let set = HookPolicySet::merge_contributions([
            policy(
                "test",
                "mode",
                HookValue::Text("alpha-secret".to_owned()),
                10,
            ),
            policy(
                "test",
                "mode",
                HookValue::Text("beta-secret".to_owned()),
                10,
            ),
        ]);

        let entry = set
            .get(&domain("test"), &key("mode"))
            .expect("policy entry exists");
        assert_eq!(entry.value, HookValue::Text("alpha-secret".to_owned()));
        assert!(set.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "policy.merge.conflict"
                && diagnostic.safe_for_user
                && !diagnostic.message.as_str().contains("alpha-secret")
                && !diagnostic.message.as_str().contains("beta-secret")
        }));
    }

    #[test]
    fn policy_set_orders_entries_by_domain_and_key() {
        let set = HookPolicySet::merge_contributions([
            policy("test.z", "mode", HookValue::Bool(true), 0),
            policy("test.a", "zeta", HookValue::Bool(true), 0),
            policy("test.a", "alpha", HookValue::Bool(true), 0),
        ]);

        let keys = set
            .entries
            .keys()
            .map(|key| (key.domain.as_str().to_owned(), key.key.as_str().to_owned()))
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![
                ("test.a".to_owned(), "alpha".to_owned()),
                ("test.a".to_owned(), "zeta".to_owned()),
                ("test.z".to_owned(), "mode".to_owned()),
            ]
        );
    }

    #[test]
    fn policy_set_ignores_non_policy_contributions() {
        let set = HookPolicySet::merge_hook_contributions([
            HookContribution::Policy(policy("test", "mode", HookValue::Bool(true), 0)),
            HookContribution::PromptSection(PromptSectionContribution {
                contribution_id: crate::HookContributionId::new("test.section")
                    .expect("valid contribution id"),
                section_id: HookSectionId::new("test.section").expect("valid section id"),
                title: Some(HookPromptSectionTitle::new("Test").expect("valid title")),
                domain: domain("test"),
                priority: 100,
                content: HookPromptContent::new("ignored").expect("valid content"),
                max_chars: None,
                source_refs: Vec::new(),
                diagnostics: Vec::new(),
                truncated: false,
            }),
            HookContribution::Audit(AuditContribution {
                event_kind: HookAuditEventKind::new("test.audit").expect("valid audit event kind"),
                details: HookValue::Bool(true),
                safe_for_user: true,
            }),
            HookContribution::Noop,
        ]);

        assert_eq!(set.len(), 1);
        assert!(set.get(&domain("test"), &key("mode")).is_some());
    }

    #[test]
    fn policy_set_roundtrips_through_serde() {
        let set = HookPolicySet::merge_contributions([
            policy("test", "mode", HookValue::Text("strict".to_owned()), 10),
            policy("test", "allowed", HookValue::Bool(true), 5),
        ]);

        let value = serde_json::to_value(&set).expect("policy set serializes");
        assert!(value["entries"].is_array());
        let decoded: HookPolicySet =
            serde_json::from_value(value).expect("policy set deserializes");
        assert_eq!(decoded, set);
    }
}
