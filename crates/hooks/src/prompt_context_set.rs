use crate::{
    HookContribution, HookContributionId, HookDiagnostic, HookDiagnosticCode,
    HookDiagnosticMessage, HookDiagnosticSeverity, HookDomain, HookMetadata, HookPromptContent,
    HookSourceRef, PromptContextContribution,
};
use serde::{Deserialize, Serialize};

pub const DEFAULT_PROMPT_CONTEXT_MAX_ENTRIES: usize = 16;
pub const DEFAULT_PROMPT_CONTEXT_MAX_TOTAL_CHARS: usize = 12_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookPromptContextLimits {
    pub max_entries: usize,
    pub max_total_chars: usize,
}

impl Default for HookPromptContextLimits {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_PROMPT_CONTEXT_MAX_ENTRIES,
            max_total_chars: DEFAULT_PROMPT_CONTEXT_MAX_TOTAL_CHARS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookPromptContextEntry {
    pub contribution_id: HookContributionId,
    pub domain: HookDomain,
    pub priority: i32,
    pub content: HookPromptContent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chars: Option<usize>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<HookSourceRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookPromptContextSet {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<HookPromptContextEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default)]
    pub truncated: bool,
}

impl HookPromptContextSet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn runtime_failed() -> Self {
        Self {
            entries: Vec::new(),
            diagnostics: vec![runtime_failed_diagnostic()],
            truncated: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.diagnostics.is_empty() && !self.truncated
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> impl Iterator<Item = &HookPromptContextEntry> {
        self.entries.iter()
    }

    pub fn aggregate_contributions(
        contributions: impl IntoIterator<Item = PromptContextContribution>,
        limits: HookPromptContextLimits,
    ) -> Self {
        let mut indexed = contributions
            .into_iter()
            .enumerate()
            .collect::<Vec<(usize, PromptContextContribution)>>();
        indexed.sort_by(|(left_index, left), (right_index, right)| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.domain.cmp(&right.domain))
                .then_with(|| left.contribution_id.cmp(&right.contribution_id))
                .then_with(|| left_index.cmp(right_index))
        });

        let mut entries = Vec::new();
        let mut diagnostics = Vec::new();
        let mut truncated = false;
        let mut remaining_total_chars = limits.max_total_chars;

        for (_, contribution) in indexed {
            if entries.len() >= limits.max_entries {
                diagnostics.push(entry_omitted_diagnostic());
                truncated = true;
                continue;
            }

            let entry_limit = contribution
                .max_chars
                .unwrap_or(remaining_total_chars)
                .min(remaining_total_chars);

            if entry_limit == 0 {
                diagnostics.push(entry_omitted_diagnostic());
                truncated = true;
                continue;
            }

            let content = contribution.content.as_str();
            let content_chars = content.chars().count();
            let entry_was_truncated = contribution.truncated || content_chars > entry_limit;
            let content = if content_chars > entry_limit {
                let truncated_content = content.chars().take(entry_limit).collect::<String>();
                match HookPromptContent::new(truncated_content) {
                    Ok(content) => {
                        diagnostics.push(truncated_diagnostic());
                        truncated = true;
                        content
                    }
                    Err(_) => {
                        diagnostics.push(entry_omitted_diagnostic());
                        truncated = true;
                        continue;
                    }
                }
            } else {
                contribution.content.clone()
            };

            if contribution.truncated {
                truncated = true;
            }

            remaining_total_chars =
                remaining_total_chars.saturating_sub(content.as_str().chars().count());

            let mut entry_diagnostics = contribution.diagnostics.clone();
            if entry_was_truncated && content_chars > entry_limit {
                entry_diagnostics.push(truncated_diagnostic());
            }

            diagnostics.extend(contribution.diagnostics.iter().cloned());

            entries.push(HookPromptContextEntry {
                contribution_id: contribution.contribution_id,
                domain: contribution.domain,
                priority: contribution.priority,
                content,
                max_chars: contribution.max_chars,
                source_refs: contribution.source_refs,
                diagnostics: entry_diagnostics,
                truncated: entry_was_truncated,
            });
        }

        Self {
            entries,
            diagnostics,
            truncated,
        }
    }

    pub fn aggregate_hook_contributions(
        contributions: impl IntoIterator<Item = HookContribution>,
        limits: HookPromptContextLimits,
    ) -> Self {
        Self::aggregate_contributions(
            contributions
                .into_iter()
                .filter_map(|contribution| match contribution {
                    HookContribution::PromptContext(context) => Some(context),
                    HookContribution::Policy(_)
                    | HookContribution::PromptSection(_)
                    | HookContribution::ToolBundle(_)
                    | HookContribution::PromptManifestDiagnostic(_)
                    | HookContribution::Audit(_)
                    | HookContribution::BackgroundJob(_)
                    | HookContribution::Noop => None,
                }),
            limits,
        )
    }
}

fn truncated_diagnostic() -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new("prompt_context.truncated")
            .expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(
            "prompt context contribution was truncated to fit aggregation budget",
        )
        .expect("static diagnostic message is valid"),
        severity: HookDiagnosticSeverity::Warning,
        safe_for_user: true,
        metadata: HookMetadata::default(),
    }
}

fn entry_omitted_diagnostic() -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new("prompt_context.entry_omitted")
            .expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(
            "prompt context contribution was omitted because aggregation budget was exhausted",
        )
        .expect("static diagnostic message is valid"),
        severity: HookDiagnosticSeverity::Warning,
        safe_for_user: true,
        metadata: HookMetadata::default(),
    }
}

fn runtime_failed_diagnostic() -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new("prompt_context.runtime_failed")
            .expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(
            "prompt context hook phase failed; using empty prompt context",
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
        AuditContribution, HookAuditEventKind, HookPromptSectionTitle, HookSectionId, HookSourceId,
        HookSourceKind, HookSourceLabel, HookValue, PolicyContribution, PromptSectionContribution,
    };

    fn domain(value: &str) -> HookDomain {
        HookDomain::new(value).expect("valid domain")
    }

    fn contribution_id(value: &str) -> HookContributionId {
        HookContributionId::new(value).expect("valid contribution id")
    }

    fn source_ref(value: &str) -> HookSourceRef {
        HookSourceRef {
            kind: HookSourceKind::Thread,
            id: HookSourceId::new(value).expect("valid source id"),
            label: Some(HookSourceLabel::new("Source").expect("valid source label")),
        }
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

    fn context(
        id: &str,
        domain_value: &str,
        priority: i32,
        content: &str,
    ) -> PromptContextContribution {
        PromptContextContribution {
            contribution_id: contribution_id(id),
            domain: domain(domain_value),
            priority,
            content: HookPromptContent::new(content).expect("valid prompt content"),
            max_chars: None,
            source_refs: Vec::new(),
            diagnostics: Vec::new(),
            truncated: false,
        }
    }

    fn limits(max_entries: usize, max_total_chars: usize) -> HookPromptContextLimits {
        HookPromptContextLimits {
            max_entries,
            max_total_chars,
        }
    }

    #[test]
    fn prompt_context_set_empty_is_default() {
        assert_eq!(
            HookPromptContextSet::empty(),
            HookPromptContextSet::default()
        );
        assert!(HookPromptContextSet::default().is_empty());
        assert_eq!(HookPromptContextSet::default().len(), 0);
    }

    #[test]
    fn prompt_context_set_aggregates_single_contribution() {
        let set = HookPromptContextSet::aggregate_contributions(
            [context("test.context.one", "test", 10, "context one")],
            HookPromptContextLimits::default(),
        );

        assert_eq!(set.len(), 1);
        let entry = &set.entries[0];
        assert_eq!(entry.contribution_id, contribution_id("test.context.one"));
        assert_eq!(entry.domain, domain("test"));
        assert_eq!(entry.priority, 10);
        assert_eq!(entry.content.as_str(), "context one");
        assert!(!entry.truncated);
        assert!(!set.truncated);
    }

    #[test]
    fn prompt_context_set_orders_multiple_contributions_deterministically() {
        let set = HookPromptContextSet::aggregate_contributions(
            [
                context("test.context.low", "test", 0, "low"),
                context("test.context.same_b", "test.b", 5, "same b"),
                context("test.context.high", "test", 10, "high"),
                context("test.context.same_a", "test.a", 5, "same a"),
            ],
            HookPromptContextLimits::default(),
        );

        let ordered = set
            .entries
            .iter()
            .map(|entry| entry.contribution_id.as_str().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            ordered,
            vec![
                "test.context.high".to_owned(),
                "test.context.same_a".to_owned(),
                "test.context.same_b".to_owned(),
                "test.context.low".to_owned(),
            ]
        );
    }

    #[test]
    fn prompt_context_set_applies_per_contribution_max_chars() {
        let mut contribution = context("test.context.long", "test", 0, "0123456789abcdef");
        contribution.max_chars = Some(8);

        let set = HookPromptContextSet::aggregate_contributions(
            [contribution],
            HookPromptContextLimits::default(),
        );

        assert_eq!(set.entries[0].content.as_str(), "01234567");
        assert!(set.entries[0].truncated);
        assert!(set.truncated);
        assert!(set.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "prompt_context.truncated"
                && diagnostic.safe_for_user
                && !diagnostic.message.as_str().contains("0123456789abcdef")
        }));
    }

    #[test]
    fn prompt_context_set_applies_total_budget_predictably() {
        let set = HookPromptContextSet::aggregate_contributions(
            [
                context("test.context.one", "test", 10, "abcdef"),
                context("test.context.two", "test", 0, "ghijkl"),
            ],
            limits(16, 8),
        );

        assert_eq!(set.entries.len(), 2);
        assert_eq!(set.entries[0].content.as_str(), "abcdef");
        assert_eq!(set.entries[1].content.as_str(), "gh");
        assert!(set.entries[1].truncated);
        assert!(set.truncated);
    }

    #[test]
    fn prompt_context_set_marks_pretruncated_contribution() {
        let mut contribution = context("test.context.one", "test", 0, "already short");
        contribution.truncated = true;

        let set = HookPromptContextSet::aggregate_contributions(
            [contribution],
            HookPromptContextLimits::default(),
        );

        assert!(set.entries[0].truncated);
        assert!(set.truncated);
    }

    #[test]
    fn prompt_context_set_preserves_source_refs() {
        let mut contribution = context("test.context.one", "test", 0, "context");
        contribution.source_refs.push(source_ref("thread-1"));

        let set = HookPromptContextSet::aggregate_contributions(
            [contribution],
            HookPromptContextLimits::default(),
        );

        assert_eq!(set.entries[0].source_refs, vec![source_ref("thread-1")]);
    }

    #[test]
    fn prompt_context_set_preserves_contribution_diagnostics() {
        let mut contribution = context("test.context.one", "test", 0, "context");
        contribution
            .diagnostics
            .push(diagnostic("test.context_diag"));

        let set = HookPromptContextSet::aggregate_contributions(
            [contribution],
            HookPromptContextLimits::default(),
        );

        assert_eq!(set.entries[0].diagnostics.len(), 1);
        assert_eq!(set.diagnostics.len(), 1);
        assert_eq!(set.diagnostics[0].code.as_str(), "test.context_diag");
    }

    #[test]
    fn prompt_context_set_ignores_non_prompt_context_contributions() {
        let set = HookPromptContextSet::aggregate_hook_contributions(
            [
                HookContribution::PromptContext(context("test.context.one", "test", 0, "context")),
                HookContribution::Policy(PolicyContribution {
                    domain: domain("test"),
                    key: crate::HookPolicyKey::new("mode").expect("valid policy key"),
                    value: HookValue::Bool(true),
                    priority: 100,
                    diagnostics: Vec::new(),
                }),
                HookContribution::PromptSection(PromptSectionContribution {
                    contribution_id: crate::HookContributionId::new("test.section")
                        .expect("valid contribution id"),
                    section_id: HookSectionId::new("test.section").expect("valid section id"),
                    title: Some(HookPromptSectionTitle::new("Test").expect("valid title")),
                    domain: domain("test"),
                    priority: 100,
                    content: HookPromptContent::new("ignored").expect("valid content"),
                    max_chars: None,
                    diagnostics: Vec::new(),
                    truncated: false,
                }),
                HookContribution::Audit(AuditContribution {
                    event_kind: HookAuditEventKind::new("test.audit")
                        .expect("valid audit event kind"),
                    details: HookValue::Bool(true),
                    safe_for_user: true,
                }),
                HookContribution::Noop,
            ],
            HookPromptContextLimits::default(),
        );

        assert_eq!(set.len(), 1);
        assert_eq!(set.entries[0].content.as_str(), "context");
    }

    #[test]
    fn prompt_context_set_roundtrips_through_serde() {
        let set = HookPromptContextSet::aggregate_contributions(
            [
                context("test.context.one", "test", 10, "context one"),
                context("test.context.two", "test", 0, "context two"),
            ],
            HookPromptContextLimits::default(),
        );

        let value = serde_json::to_value(&set).expect("prompt context set serializes");
        let decoded: HookPromptContextSet =
            serde_json::from_value(value).expect("prompt context set deserializes");
        assert_eq!(decoded, set);
    }
}
