use crate::{
    HookContribution, HookContributionId, HookDiagnostic, HookDiagnosticCode,
    HookDiagnosticMessage, HookDiagnosticSeverity, HookDomain, HookMetadata, HookToolBundleId,
    HookToolName, ToolBundleContribution,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookToolBundleEntry {
    pub contribution_id: HookContributionId,
    pub bundle_id: HookToolBundleId,
    pub domain: HookDomain,
    pub priority: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_names: Vec<HookToolName>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
}

impl From<ToolBundleContribution> for HookToolBundleEntry {
    fn from(contribution: ToolBundleContribution) -> Self {
        Self {
            contribution_id: contribution.contribution_id,
            bundle_id: contribution.bundle_id,
            domain: contribution.domain,
            priority: contribution.priority,
            tool_names: contribution.tool_names,
            diagnostics: contribution.diagnostics,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookToolBundleSet {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<HookToolBundleEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
}

impl HookToolBundleSet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.diagnostics.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> impl Iterator<Item = &HookToolBundleEntry> {
        self.entries.iter()
    }

    pub fn aggregate_contributions(
        contributions: impl IntoIterator<Item = ToolBundleContribution>,
    ) -> Self {
        let mut indexed = contributions
            .into_iter()
            .enumerate()
            .map(|(index, contribution)| (index, HookToolBundleEntry::from(contribution)))
            .collect::<Vec<_>>();
        indexed.sort_by(|(left_index, left), (right_index, right)| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.domain.cmp(&right.domain))
                .then_with(|| left.bundle_id.cmp(&right.bundle_id))
                .then_with(|| left_index.cmp(right_index))
        });

        let mut entries = Vec::<HookToolBundleEntry>::new();
        let mut diagnostics = Vec::new();
        for (_, entry) in indexed {
            if entries
                .iter()
                .any(|existing| existing.bundle_id == entry.bundle_id)
            {
                diagnostics.push(duplicate_bundle_diagnostic());
                continue;
            }
            diagnostics.extend(entry.diagnostics.iter().cloned());
            entries.push(entry);
        }

        Self {
            entries,
            diagnostics,
        }
    }

    pub fn aggregate_hook_contributions(
        contributions: impl IntoIterator<Item = HookContribution>,
    ) -> Self {
        Self::aggregate_contributions(contributions.into_iter().filter_map(|contribution| {
            match contribution {
                HookContribution::ToolBundle(tool_bundle) => Some(tool_bundle),
                HookContribution::Policy(_)
                | HookContribution::PromptContext(_)
                | HookContribution::PromptSection(_)
                | HookContribution::PromptManifestDiagnostic(_)
                | HookContribution::Audit(_)
                | HookContribution::BackgroundJob(_)
                | HookContribution::Noop => None,
            }
        }))
    }
}

fn duplicate_bundle_diagnostic() -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new("tool_bundle.duplicate_bundle_id")
            .expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(
            "duplicate tool bundle contribution id was ignored after deterministic ordering",
        )
        .expect("static diagnostic message is valid"),
        severity: HookDiagnosticSeverity::Warning,
        safe_for_user: true,
        metadata: HookMetadata::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AuditContribution, HookAuditEventKind, HookPromptContent, HookPromptSectionTitle,
        HookSectionId, HookValue, PolicyContribution, PromptSectionContribution,
    };

    fn contribution(
        bundle_id: &str,
        domain: &str,
        priority: i32,
        tool_names: &[&str],
    ) -> ToolBundleContribution {
        ToolBundleContribution {
            contribution_id: HookContributionId::new(format!("{domain}.{bundle_id}"))
                .expect("valid contribution id"),
            bundle_id: HookToolBundleId::new(bundle_id).expect("valid bundle id"),
            domain: HookDomain::new(domain).expect("valid domain"),
            priority,
            tool_names: tool_names
                .iter()
                .map(|name| HookToolName::new(*name).expect("valid tool name"))
                .collect(),
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn tool_bundle_set_empty_is_default() {
        assert_eq!(HookToolBundleSet::empty(), HookToolBundleSet::default());
        assert!(HookToolBundleSet::default().is_empty());
        assert_eq!(HookToolBundleSet::default().len(), 0);
    }

    #[test]
    fn tool_bundles_order_deterministically() {
        let set = HookToolBundleSet::aggregate_contributions([
            contribution("test.low", "memory", 0, &["memory_search"]),
            contribution("test.same_b", "tasks.b", 5, &["task_spawn"]),
            contribution("test.high", "memory", 10, &["memory_remember"]),
            contribution("test.same_a", "tasks.a", 5, &["task_list"]),
        ]);

        let ids = set
            .entries()
            .map(|entry| entry.bundle_id.as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "test.high".to_owned(),
                "test.same_a".to_owned(),
                "test.same_b".to_owned(),
                "test.low".to_owned(),
            ]
        );
    }

    #[test]
    fn duplicate_bundle_ids_keep_first_after_ordering() {
        let set = HookToolBundleSet::aggregate_contributions([
            contribution("test.shared", "memory", 0, &["memory_search"]),
            contribution("test.shared", "memory", 10, &["memory_remember"]),
        ]);

        assert_eq!(set.len(), 1);
        assert_eq!(set.entries[0].priority, 10);
        assert_eq!(set.entries[0].tool_names[0].as_str(), "memory_remember");
        assert!(
            set.diagnostics.iter().any(|diagnostic| {
                diagnostic.code.as_str() == "tool_bundle.duplicate_bundle_id"
            })
        );
    }

    #[test]
    fn tool_bundle_diagnostics_are_preserved() {
        let mut contribution = contribution("test.bundle", "memory", 10, &["memory_search"]);
        contribution.diagnostics.push(HookDiagnostic {
            code: HookDiagnosticCode::new("test.tool_bundle").expect("valid code"),
            message: HookDiagnosticMessage::new("diagnostic").expect("valid message"),
            severity: HookDiagnosticSeverity::Info,
            safe_for_user: true,
            metadata: HookMetadata::default(),
        });

        let set = HookToolBundleSet::aggregate_contributions([contribution]);

        assert!(
            set.diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code.as_str() == "test.tool_bundle" })
        );
    }

    #[test]
    fn non_tool_bundle_contributions_are_ignored() {
        let set = HookToolBundleSet::aggregate_hook_contributions([
            HookContribution::Policy(PolicyContribution {
                domain: HookDomain::new("test").expect("valid domain"),
                key: crate::HookPolicyKey::new("key").expect("valid key"),
                value: HookValue::Bool(true),
                priority: 10,
                diagnostics: Vec::new(),
            }),
            HookContribution::PromptSection(PromptSectionContribution {
                contribution_id: HookContributionId::new("test.section")
                    .expect("valid contribution id"),
                section_id: HookSectionId::new("test.section").expect("valid section id"),
                title: Some(HookPromptSectionTitle::new("Test").expect("valid title")),
                domain: HookDomain::new("test").expect("valid domain"),
                priority: 100,
                content: HookPromptContent::new("ignored").expect("valid content"),
                max_chars: None,
                diagnostics: Vec::new(),
                truncated: false,
            }),
            HookContribution::Audit(AuditContribution {
                event_kind: HookAuditEventKind::new("test.audit").expect("valid audit"),
                details: HookValue::Bool(true),
                safe_for_user: true,
            }),
            HookContribution::Noop,
        ]);

        assert!(set.is_empty());
    }
}
