use crate::{
    HookContribution, HookDiagnostic, HookDiagnosticCode, HookDiagnosticMessage,
    HookDiagnosticSeverity, HookDomain, HookMetadata, HookPromptContent, HookPromptSectionTitle,
    HookSectionId, PromptSectionContribution,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const DEFAULT_PROMPT_SECTION_MAX_SECTIONS: usize = 16;
pub const DEFAULT_PROMPT_SECTION_MAX_CHARS_PER_SECTION: usize = 8_000;
pub const DEFAULT_PROMPT_SECTION_MAX_TOTAL_CHARS: usize = 16_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookPromptSectionLimits {
    pub max_sections: usize,
    pub max_chars_per_section: usize,
    pub max_total_chars: usize,
}

impl Default for HookPromptSectionLimits {
    fn default() -> Self {
        Self {
            max_sections: DEFAULT_PROMPT_SECTION_MAX_SECTIONS,
            max_chars_per_section: DEFAULT_PROMPT_SECTION_MAX_CHARS_PER_SECTION,
            max_total_chars: DEFAULT_PROMPT_SECTION_MAX_TOTAL_CHARS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HookPromptSectionEntry {
    pub section_id: HookSectionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<HookPromptSectionTitle>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domains: Vec<HookDomain>,
    pub priority: i32,
    pub content: HookPromptContent,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HookPromptSectionSet {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<HookPromptSectionEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<HookDiagnostic>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Debug, Clone)]
struct IndexedContribution {
    index: usize,
    contribution: PromptSectionContribution,
}

#[derive(Debug, Clone)]
struct MergedSection {
    first_index: usize,
    entry: HookPromptSectionEntry,
}

impl HookPromptSectionSet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.diagnostics.is_empty() && !self.truncated
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn entries(&self) -> impl Iterator<Item = &HookPromptSectionEntry> {
        self.entries.iter()
    }

    pub fn aggregate_contributions(
        contributions: impl IntoIterator<Item = PromptSectionContribution>,
        limits: HookPromptSectionLimits,
    ) -> Self {
        let mut indexed = contributions
            .into_iter()
            .enumerate()
            .map(|(index, contribution)| IndexedContribution {
                index,
                contribution,
            })
            .collect::<Vec<_>>();

        indexed.sort_by(|left, right| {
            right
                .contribution
                .priority
                .cmp(&left.contribution.priority)
                .then_with(|| left.contribution.domain.cmp(&right.contribution.domain))
                .then_with(|| {
                    left.contribution
                        .section_id
                        .cmp(&right.contribution.section_id)
                })
                .then_with(|| left.index.cmp(&right.index))
        });

        let mut grouped = Vec::<Vec<IndexedContribution>>::new();
        for contribution in indexed {
            if let Some(group) = grouped.iter_mut().find(|group| {
                group[0].contribution.section_id == contribution.contribution.section_id
            }) {
                group.push(contribution);
            } else {
                grouped.push(vec![contribution]);
            }
        }

        let mut merged = Vec::new();
        let mut diagnostics = Vec::new();
        let mut truncated = false;

        for group in grouped {
            match merge_section_group(group, limits.max_chars_per_section) {
                Some((section, mut section_diagnostics, section_truncated)) => {
                    diagnostics.append(&mut section_diagnostics);
                    truncated |= section_truncated;
                    merged.push(section);
                }
                None => {
                    diagnostics.push(section_omitted_diagnostic());
                    truncated = true;
                }
            }
        }

        merged.sort_by(|left, right| {
            right
                .entry
                .priority
                .cmp(&left.entry.priority)
                .then_with(|| left.entry.domains.first().cmp(&right.entry.domains.first()))
                .then_with(|| left.entry.section_id.cmp(&right.entry.section_id))
                .then_with(|| left.first_index.cmp(&right.first_index))
        });

        let mut entries = Vec::new();
        let mut remaining_total_chars = limits.max_total_chars;

        for section in merged {
            if entries.len() >= limits.max_sections {
                diagnostics.push(section_omitted_diagnostic());
                truncated = true;
                continue;
            }

            if remaining_total_chars == 0 {
                diagnostics.push(budget_exhausted_diagnostic());
                truncated = true;
                continue;
            }

            let mut entry = section.entry;
            let content_chars = entry.content.as_str().chars().count();
            if content_chars > remaining_total_chars {
                let truncated_content = entry
                    .content
                    .as_str()
                    .chars()
                    .take(remaining_total_chars)
                    .collect::<String>();
                match HookPromptContent::new(truncated_content) {
                    Ok(content) => {
                        let diagnostic = section_truncated_diagnostic();
                        entry.diagnostics.push(diagnostic.clone());
                        diagnostics.push(diagnostic);
                        entry.content = content;
                        entry.truncated = true;
                        truncated = true;
                    }
                    Err(_) => {
                        diagnostics.push(section_omitted_diagnostic());
                        truncated = true;
                        continue;
                    }
                }
            }

            remaining_total_chars =
                remaining_total_chars.saturating_sub(entry.content.as_str().chars().count());
            entries.push(entry);
        }

        Self {
            entries,
            diagnostics,
            truncated,
        }
    }

    pub fn aggregate_hook_contributions(
        contributions: impl IntoIterator<Item = HookContribution>,
        limits: HookPromptSectionLimits,
    ) -> Self {
        Self::aggregate_contributions(
            contributions
                .into_iter()
                .filter_map(|contribution| match contribution {
                    HookContribution::PromptSection(section) => Some(section),
                    HookContribution::Policy(_)
                    | HookContribution::PromptContext(_)
                    | HookContribution::PromptManifestDiagnostic(_)
                    | HookContribution::Audit(_)
                    | HookContribution::Noop => None,
                }),
            limits,
        )
    }
}

fn merge_section_group(
    mut group: Vec<IndexedContribution>,
    max_chars_per_section: usize,
) -> Option<(MergedSection, Vec<HookDiagnostic>, bool)> {
    group.sort_by(|left, right| {
        right
            .contribution
            .priority
            .cmp(&left.contribution.priority)
            .then_with(|| left.contribution.domain.cmp(&right.contribution.domain))
            .then_with(|| left.index.cmp(&right.index))
    });

    let section_id = group.first()?.contribution.section_id.clone();
    let priority = group
        .iter()
        .map(|contribution| contribution.contribution.priority)
        .max()
        .unwrap_or_default();
    let first_index = group
        .iter()
        .map(|contribution| contribution.index)
        .min()
        .unwrap_or_default();

    let mut domains = Vec::new();
    let mut seen_domains = BTreeSet::new();
    let mut title = None;
    let mut seen_title: Option<HookPromptSectionTitle> = None;
    let mut content_fragments = Vec::new();
    let mut entry_diagnostics = Vec::new();
    let mut set_diagnostics = Vec::new();
    let mut truncated = false;

    for indexed in group {
        let contribution = indexed.contribution;
        if seen_domains.insert(contribution.domain.clone()) {
            domains.push(contribution.domain.clone());
        }

        if let Some(contribution_title) = contribution.title.clone() {
            match &seen_title {
                None => {
                    seen_title = Some(contribution_title.clone());
                    title = Some(contribution_title);
                }
                Some(existing_title) if existing_title == &contribution_title => {}
                Some(_) => {
                    let diagnostic = title_conflict_diagnostic();
                    entry_diagnostics.push(diagnostic.clone());
                    set_diagnostics.push(diagnostic);
                }
            }
        }

        entry_diagnostics.extend(contribution.diagnostics.iter().cloned());
        set_diagnostics.extend(contribution.diagnostics.iter().cloned());
        truncated |= contribution.truncated;

        let fragment_limit = contribution.max_chars.unwrap_or(usize::MAX);
        if fragment_limit == 0 {
            let diagnostic = fragment_truncated_diagnostic();
            entry_diagnostics.push(diagnostic.clone());
            set_diagnostics.push(diagnostic);
            truncated = true;
            continue;
        }

        let content = contribution.content.as_str();
        let content_chars = content.chars().count();
        if content_chars > fragment_limit {
            let truncated_content = content.chars().take(fragment_limit).collect::<String>();
            match HookPromptContent::new(truncated_content) {
                Ok(content) => {
                    let diagnostic = fragment_truncated_diagnostic();
                    entry_diagnostics.push(diagnostic.clone());
                    set_diagnostics.push(diagnostic);
                    content_fragments.push(content.into_inner());
                    truncated = true;
                }
                Err(_) => {
                    let diagnostic = section_omitted_diagnostic();
                    entry_diagnostics.push(diagnostic.clone());
                    set_diagnostics.push(diagnostic);
                    truncated = true;
                }
            }
        } else {
            content_fragments.push(contribution.content.into_inner());
        }
    }

    let merged_content = content_fragments
        .into_iter()
        .map(|content| content.trim().to_owned())
        .filter(|content| !content.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");

    if merged_content.is_empty() {
        return None;
    }

    let merged_chars = merged_content.chars().count();
    let content = if merged_chars > max_chars_per_section {
        let truncated_content = merged_content
            .chars()
            .take(max_chars_per_section)
            .collect::<String>();
        match HookPromptContent::new(truncated_content) {
            Ok(content) => {
                let diagnostic = section_truncated_diagnostic();
                entry_diagnostics.push(diagnostic.clone());
                set_diagnostics.push(diagnostic);
                truncated = true;
                content
            }
            Err(_) => return None,
        }
    } else {
        HookPromptContent::new(merged_content).ok()?
    };

    Some((
        MergedSection {
            first_index,
            entry: HookPromptSectionEntry {
                section_id,
                title,
                domains,
                priority,
                content,
                diagnostics: entry_diagnostics,
                truncated,
            },
        },
        set_diagnostics,
        truncated,
    ))
}

fn fragment_truncated_diagnostic() -> HookDiagnostic {
    diagnostic(
        "prompt_section.fragment_truncated",
        "prompt section contribution fragment was truncated to fit aggregation budget",
    )
}

fn section_truncated_diagnostic() -> HookDiagnostic {
    diagnostic(
        "prompt_section.section_truncated",
        "prompt section was truncated to fit aggregation budget",
    )
}

fn section_omitted_diagnostic() -> HookDiagnostic {
    diagnostic(
        "prompt_section.section_omitted",
        "prompt section was omitted because aggregation budget was exhausted",
    )
}

fn title_conflict_diagnostic() -> HookDiagnostic {
    diagnostic(
        "prompt_section.title_conflict",
        "prompt section title conflict was resolved deterministically",
    )
}

fn budget_exhausted_diagnostic() -> HookDiagnostic {
    diagnostic(
        "prompt_section.budget_exhausted",
        "prompt section aggregation budget was exhausted",
    )
}

fn diagnostic(code: &str, message: &str) -> HookDiagnostic {
    HookDiagnostic {
        code: HookDiagnosticCode::new(code).expect("static diagnostic code is valid"),
        message: HookDiagnosticMessage::new(message).expect("static diagnostic message is valid"),
        severity: HookDiagnosticSeverity::Warning,
        safe_for_user: true,
        metadata: HookMetadata::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AuditContribution, HookAuditEventKind, HookContributionId, HookPolicyKey, HookValue,
        PolicyContribution, PromptContextContribution, PromptManifestDiagnosticContribution,
    };

    fn section_id(value: &str) -> HookSectionId {
        HookSectionId::new(value).expect("valid section id")
    }

    fn title(value: &str) -> HookPromptSectionTitle {
        HookPromptSectionTitle::new(value).expect("valid section title")
    }

    fn domain(value: &str) -> HookDomain {
        HookDomain::new(value).expect("valid domain")
    }

    fn content(value: &str) -> HookPromptContent {
        HookPromptContent::new(value).expect("valid prompt content")
    }

    fn diagnostic_for_test(code: &str) -> HookDiagnostic {
        HookDiagnostic {
            code: HookDiagnosticCode::new(code).expect("valid code"),
            message: HookDiagnosticMessage::new("diagnostic").expect("valid message"),
            severity: HookDiagnosticSeverity::Info,
            safe_for_user: true,
            metadata: HookMetadata::default(),
        }
    }

    fn contribution(
        section: &str,
        domain_value: &str,
        priority: i32,
        content_value: &str,
    ) -> PromptSectionContribution {
        PromptSectionContribution {
            section_id: section_id(section),
            title: Some(title("Section")),
            domain: domain(domain_value),
            priority,
            content: content(content_value),
            max_chars: None,
            diagnostics: Vec::new(),
            truncated: false,
        }
    }

    fn limits(
        max_sections: usize,
        max_chars_per_section: usize,
        max_total_chars: usize,
    ) -> HookPromptSectionLimits {
        HookPromptSectionLimits {
            max_sections,
            max_chars_per_section,
            max_total_chars,
        }
    }

    #[test]
    fn prompt_section_set_empty_is_default() {
        assert_eq!(
            HookPromptSectionSet::empty(),
            HookPromptSectionSet::default()
        );
        assert!(HookPromptSectionSet::default().is_empty());
        assert_eq!(HookPromptSectionSet::default().len(), 0);
    }

    #[test]
    fn one_prompt_section_contribution_is_retained() {
        let set = HookPromptSectionSet::aggregate_contributions(
            [contribution("test.section", "test", 10, "section content")],
            HookPromptSectionLimits::default(),
        );

        assert_eq!(set.len(), 1);
        let entry = &set.entries[0];
        assert_eq!(entry.section_id.as_str(), "test.section");
        assert_eq!(entry.domains[0].as_str(), "test");
        assert_eq!(entry.content.as_str(), "section content");
        assert!(!entry.truncated);
    }

    #[test]
    fn multiple_sections_order_deterministically() {
        let set = HookPromptSectionSet::aggregate_contributions(
            [
                contribution("test.low", "test", 0, "low"),
                contribution("test.same_b", "test.b", 5, "same b"),
                contribution("test.high", "test", 10, "high"),
                contribution("test.same_a", "test.a", 5, "same a"),
            ],
            HookPromptSectionLimits::default(),
        );

        let ids = set
            .entries()
            .map(|entry| entry.section_id.as_str().to_owned())
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
    fn same_section_id_merges_fragments_deterministically() {
        let set = HookPromptSectionSet::aggregate_contributions(
            [
                contribution("test.shared", "test.b", 5, "second"),
                contribution("test.shared", "test.a", 10, "first"),
            ],
            HookPromptSectionLimits::default(),
        );

        assert_eq!(set.len(), 1);
        let entry = &set.entries[0];
        assert_eq!(entry.priority, 10);
        assert_eq!(entry.content.as_str(), "first\n\nsecond");
        assert_eq!(
            entry
                .domains
                .iter()
                .map(|domain| domain.as_str().to_owned())
                .collect::<Vec<_>>(),
            vec!["test.a".to_owned(), "test.b".to_owned()]
        );
    }

    #[test]
    fn same_section_title_conflict_records_diagnostic() {
        let mut first = contribution("test.shared", "test.a", 10, "first");
        first.title = Some(title("First Title"));
        let mut second = contribution("test.shared", "test.b", 5, "second");
        second.title = Some(title("Second Title"));

        let set = HookPromptSectionSet::aggregate_contributions(
            [second, first],
            HookPromptSectionLimits::default(),
        );

        assert_eq!(
            set.entries[0].title.as_ref().map(|title| title.as_str()),
            Some("First Title")
        );
        assert!(set.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "prompt_section.title_conflict" && diagnostic.safe_for_user
        }));
    }

    #[test]
    fn section_count_limit_omits_later_sections() {
        let set = HookPromptSectionSet::aggregate_contributions(
            [
                contribution("test.keep", "test", 10, "keep"),
                contribution("test.omit", "test", 0, "omit"),
            ],
            limits(1, 100, 100),
        );

        assert_eq!(set.len(), 1);
        assert_eq!(set.entries[0].section_id.as_str(), "test.keep");
        assert!(set.truncated);
        assert!(
            set.diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code.as_str() == "prompt_section.section_omitted" })
        );
    }

    #[test]
    fn per_section_budget_truncates_content() {
        let set = HookPromptSectionSet::aggregate_contributions(
            [contribution("test.long", "test", 10, "0123456789")],
            limits(4, 5, 100),
        );

        assert_eq!(set.entries[0].content.as_str(), "01234");
        assert!(set.entries[0].truncated);
        assert!(set.truncated);
        assert!(set.diagnostics.iter().any(|diagnostic| {
            diagnostic.code.as_str() == "prompt_section.section_truncated"
                && !diagnostic.message.as_str().contains("0123456789")
        }));
    }

    #[test]
    fn total_budget_truncates_or_omits_later_sections() {
        let set = HookPromptSectionSet::aggregate_contributions(
            [
                contribution("test.first", "test", 10, "12345"),
                contribution("test.second", "test", 5, "67890"),
            ],
            limits(4, 10, 7),
        );

        assert_eq!(set.len(), 2);
        assert_eq!(set.entries[0].content.as_str(), "12345");
        assert_eq!(set.entries[1].content.as_str(), "67");
        assert!(set.entries[1].truncated);
        assert!(set.truncated);
    }

    #[test]
    fn non_section_contributions_are_ignored() {
        let set = HookPromptSectionSet::aggregate_hook_contributions(
            [
                HookContribution::Policy(PolicyContribution {
                    domain: domain("test"),
                    key: HookPolicyKey::new("key").expect("valid key"),
                    value: HookValue::Bool(true),
                    priority: 10,
                    diagnostics: Vec::new(),
                }),
                HookContribution::PromptContext(PromptContextContribution {
                    contribution_id: HookContributionId::new("test.context")
                        .expect("valid contribution id"),
                    domain: domain("test"),
                    priority: 10,
                    content: content("context"),
                    max_chars: None,
                    source_refs: Vec::new(),
                    diagnostics: Vec::new(),
                    truncated: false,
                }),
                HookContribution::PromptManifestDiagnostic(PromptManifestDiagnosticContribution {
                    code: HookDiagnosticCode::new("test.diagnostic").expect("valid code"),
                    message: HookDiagnosticMessage::new("diagnostic").expect("valid message"),
                    severity: HookDiagnosticSeverity::Info,
                    hook_id: None,
                    subscription_id: None,
                }),
                HookContribution::Audit(AuditContribution {
                    event_kind: HookAuditEventKind::new("test.audit").expect("valid audit"),
                    details: HookValue::Bool(true),
                    safe_for_user: true,
                }),
                HookContribution::Noop,
            ],
            HookPromptSectionLimits::default(),
        );

        assert!(set.is_empty());
    }

    #[test]
    fn contribution_diagnostics_are_preserved() {
        let mut contribution = contribution("test.section", "test", 10, "content");
        contribution
            .diagnostics
            .push(diagnostic_for_test("test.section_diagnostic"));

        let set = HookPromptSectionSet::aggregate_contributions(
            [contribution],
            HookPromptSectionLimits::default(),
        );

        assert!(
            set.diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code.as_str() == "test.section_diagnostic" })
        );
        assert!(
            set.entries[0]
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code.as_str() == "test.section_diagnostic" })
        );
    }

    #[test]
    fn pre_truncated_contribution_marks_set_truncated() {
        let mut contribution = contribution("test.section", "test", 10, "content");
        contribution.truncated = true;

        let set = HookPromptSectionSet::aggregate_contributions(
            [contribution],
            HookPromptSectionLimits::default(),
        );

        assert!(set.truncated);
        assert!(set.entries[0].truncated);
    }
}
