//! Static reasoning capability registry for providers that do not expose
//! effort metadata in their model list APIs.
//!
//! Maintenance rules:
//! - every production rule in `REASONING_MODEL_RULES` must carry an official
//!   HTTPS `source_url`;
//! - prefer exact model ids when effort values differ by model;
//! - use prefix rules only when provider docs describe a whole model family;
//! - unknown models intentionally return `None` so callers do not expose or
//!   serialize stale reasoning effort values.

use pioneer_protocol::{
    ProviderModelCapabilities, ProviderModelReasoningCapabilities, ReasoningCapabilitySource,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningModelMatcher {
    Exact(&'static str),
    Prefix(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReasoningModelRule {
    pub provider: &'static str,
    pub matcher: ReasoningModelMatcher,
    pub supported: bool,
    pub effort_options: &'static [&'static str],
    pub default_effort: Option<&'static str>,
    pub mandatory: Option<bool>,
    pub source_url: &'static str,
}

const OPENAI_GPT_5_REASONING_EFFORTS: &[&str] = &["none", "low", "medium", "high", "xhigh"];
const ANTHROPIC_EFFORTS_LOW_TO_MAX: &[&str] = &["low", "medium", "high", "max"];
const ANTHROPIC_EFFORTS_LOW_TO_XHIGH_MAX: &[&str] = &["low", "medium", "high", "xhigh", "max"];
const ANTHROPIC_EFFORTS_LOW_TO_HIGH: &[&str] = &["low", "medium", "high"];
const GEMINI_THINKING_MINIMAL_TO_HIGH: &[&str] = &["minimal", "low", "medium", "high"];
const GEMINI_THINKING_LOW_TO_HIGH: &[&str] = &["low", "medium", "high"];
const GEMINI_THINKING_LOW_HIGH: &[&str] = &["low", "high"];
const BEDROCK_CLAUDE_OPUS_4_5_EFFORTS: &[&str] = &["low", "medium", "high"];

pub const REASONING_MODEL_RULES: &[ReasoningModelRule] = &[
    ReasoningModelRule {
        provider: "openai",
        matcher: ReasoningModelMatcher::Exact("gpt-5.1"),
        supported: true,
        effort_options: &["none", "low", "medium", "high"],
        default_effort: None,
        mandatory: Some(false),
        source_url: "https://developers.openai.com/api/docs/models/gpt-5.1",
    },
    ReasoningModelRule {
        provider: "openai",
        matcher: ReasoningModelMatcher::Prefix("gpt-5.1-"),
        supported: true,
        effort_options: &["none", "low", "medium", "high"],
        default_effort: None,
        mandatory: Some(false),
        source_url: "https://developers.openai.com/api/docs/models/gpt-5.1",
    },
    ReasoningModelRule {
        provider: "openai",
        matcher: ReasoningModelMatcher::Exact("gpt-5.2"),
        supported: true,
        effort_options: OPENAI_GPT_5_REASONING_EFFORTS,
        default_effort: None,
        mandatory: Some(false),
        source_url: "https://developers.openai.com/api/docs/models/gpt-5.2",
    },
    ReasoningModelRule {
        provider: "openai",
        matcher: ReasoningModelMatcher::Prefix("gpt-5.2-"),
        supported: true,
        effort_options: OPENAI_GPT_5_REASONING_EFFORTS,
        default_effort: None,
        mandatory: Some(false),
        source_url: "https://developers.openai.com/api/docs/models/gpt-5.2",
    },
    ReasoningModelRule {
        provider: "openai",
        matcher: ReasoningModelMatcher::Exact("gpt-5.5"),
        supported: true,
        effort_options: OPENAI_GPT_5_REASONING_EFFORTS,
        default_effort: Some("medium"),
        mandatory: None,
        source_url: "https://developers.openai.com/api/docs/models",
    },
    ReasoningModelRule {
        provider: "openai",
        matcher: ReasoningModelMatcher::Exact("gpt-5.4"),
        supported: true,
        effort_options: OPENAI_GPT_5_REASONING_EFFORTS,
        default_effort: None,
        mandatory: None,
        source_url: "https://developers.openai.com/api/docs/models",
    },
    ReasoningModelRule {
        provider: "openai",
        matcher: ReasoningModelMatcher::Prefix("gpt-5.4-"),
        supported: true,
        effort_options: OPENAI_GPT_5_REASONING_EFFORTS,
        default_effort: None,
        mandatory: None,
        source_url: "https://developers.openai.com/api/docs/models",
    },
    ReasoningModelRule {
        provider: "anthropic",
        matcher: ReasoningModelMatcher::Exact("claude-fable-5"),
        supported: true,
        effort_options: ANTHROPIC_EFFORTS_LOW_TO_XHIGH_MAX,
        default_effort: Some("high"),
        mandatory: Some(true),
        source_url: "https://platform.claude.com/docs/en/build-with-claude/effort",
    },
    ReasoningModelRule {
        provider: "anthropic",
        matcher: ReasoningModelMatcher::Exact("claude-mythos-5"),
        supported: true,
        effort_options: ANTHROPIC_EFFORTS_LOW_TO_XHIGH_MAX,
        default_effort: Some("high"),
        mandatory: Some(true),
        source_url: "https://platform.claude.com/docs/en/build-with-claude/effort",
    },
    ReasoningModelRule {
        provider: "anthropic",
        matcher: ReasoningModelMatcher::Exact("claude-opus-4-8"),
        supported: true,
        effort_options: ANTHROPIC_EFFORTS_LOW_TO_XHIGH_MAX,
        default_effort: Some("high"),
        mandatory: None,
        source_url: "https://platform.claude.com/docs/en/build-with-claude/effort",
    },
    ReasoningModelRule {
        provider: "anthropic",
        matcher: ReasoningModelMatcher::Exact("claude-opus-4-7"),
        supported: true,
        effort_options: ANTHROPIC_EFFORTS_LOW_TO_XHIGH_MAX,
        default_effort: Some("high"),
        mandatory: None,
        source_url: "https://platform.claude.com/docs/en/build-with-claude/effort",
    },
    ReasoningModelRule {
        provider: "anthropic",
        matcher: ReasoningModelMatcher::Exact("claude-mythos-preview"),
        supported: true,
        effort_options: ANTHROPIC_EFFORTS_LOW_TO_MAX,
        default_effort: Some("high"),
        mandatory: Some(true),
        source_url: "https://platform.claude.com/docs/en/build-with-claude/effort",
    },
    ReasoningModelRule {
        provider: "anthropic",
        matcher: ReasoningModelMatcher::Exact("claude-opus-4-6"),
        supported: true,
        effort_options: ANTHROPIC_EFFORTS_LOW_TO_MAX,
        default_effort: Some("high"),
        mandatory: None,
        source_url: "https://platform.claude.com/docs/en/build-with-claude/effort",
    },
    ReasoningModelRule {
        provider: "anthropic",
        matcher: ReasoningModelMatcher::Exact("claude-sonnet-4-6"),
        supported: true,
        effort_options: ANTHROPIC_EFFORTS_LOW_TO_MAX,
        default_effort: Some("high"),
        mandatory: None,
        source_url: "https://platform.claude.com/docs/en/build-with-claude/effort",
    },
    ReasoningModelRule {
        provider: "anthropic",
        matcher: ReasoningModelMatcher::Exact("claude-opus-4-5"),
        supported: true,
        effort_options: ANTHROPIC_EFFORTS_LOW_TO_HIGH,
        default_effort: Some("high"),
        mandatory: None,
        source_url: "https://platform.claude.com/docs/en/build-with-claude/effort",
    },
    ReasoningModelRule {
        provider: "gemini",
        matcher: ReasoningModelMatcher::Exact("gemini-3.1-pro-preview"),
        supported: true,
        effort_options: GEMINI_THINKING_LOW_TO_HIGH,
        default_effort: Some("high"),
        mandatory: None,
        source_url: "https://ai.google.dev/gemini-api/docs/generate-content/thinking",
    },
    ReasoningModelRule {
        provider: "gemini",
        matcher: ReasoningModelMatcher::Exact("gemini-3-flash-preview"),
        supported: true,
        effort_options: GEMINI_THINKING_MINIMAL_TO_HIGH,
        default_effort: Some("high"),
        mandatory: None,
        source_url: "https://ai.google.dev/gemini-api/docs/generate-content/thinking",
    },
    ReasoningModelRule {
        provider: "gemini",
        matcher: ReasoningModelMatcher::Exact("gemini-3-pro-preview"),
        supported: true,
        effort_options: GEMINI_THINKING_LOW_HIGH,
        default_effort: Some("high"),
        mandatory: None,
        source_url: "https://ai.google.dev/gemini-api/docs/generate-content/thinking",
    },
    ReasoningModelRule {
        provider: "gemini",
        matcher: ReasoningModelMatcher::Exact("gemini-2.5-pro"),
        supported: true,
        effort_options: &[],
        default_effort: None,
        mandatory: Some(true),
        source_url: "https://ai.google.dev/api/generate-content#ThinkingConfig",
    },
    ReasoningModelRule {
        provider: "gemini",
        matcher: ReasoningModelMatcher::Exact("gemini-2.5-flash"),
        supported: true,
        effort_options: &["none"],
        default_effort: None,
        mandatory: Some(false),
        source_url: "https://ai.google.dev/api/generate-content#ThinkingConfig",
    },
    ReasoningModelRule {
        provider: "gemini",
        matcher: ReasoningModelMatcher::Exact("gemini-2.5-flash-lite"),
        supported: true,
        effort_options: &["none"],
        default_effort: None,
        mandatory: Some(false),
        source_url: "https://ai.google.dev/api/generate-content#ThinkingConfig",
    },
    ReasoningModelRule {
        provider: "bedrock",
        matcher: ReasoningModelMatcher::Exact("anthropic.claude-opus-4-5"),
        supported: true,
        effort_options: BEDROCK_CLAUDE_OPUS_4_5_EFFORTS,
        default_effort: Some("high"),
        mandatory: None,
        source_url: "https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-anthropic-claude-messages-request-response.html",
    },
];

pub fn reasoning_capabilities_for_model(
    provider: &str,
    model_id: &str,
) -> Option<ProviderModelReasoningCapabilities> {
    documented_native_profile(provider, model_id).or_else(|| {
        reasoning_capabilities_for_model_with_rules(REASONING_MODEL_RULES, provider, model_id)
    })
}

// Bounded identities shared with the native builders; original model IDs are
// never rewritten. These are protocol fallbacks, not a model/limit catalog.
// https://platform.claude.com/docs/en/build-with-claude/effort
// https://docs.aws.amazon.com/bedrock/latest/userguide/claude-messages-adaptive-thinking.html
// https://ai.google.dev/gemini-api/docs/generate-content/thinking
fn documented_native_profile(
    provider: &str,
    model_id: &str,
) -> Option<ProviderModelReasoningCapabilities> {
    let (efforts, default, mandatory) = if matches!(provider, "anthropic" | "bedrock") {
        let id = crate::generation::claude_id(provider, model_id)?.replace('.', "-");
        let family = [
            "claude-opus-4-5",
            "claude-opus-4-6",
            "claude-opus-4-7",
            "claude-opus-4-8",
            "claude-sonnet-4-6",
            "claude-fable-5",
            "claude-fable-5-1",
            "claude-mythos-5",
            "claude-mythos-5-1",
            "claude-mythos-preview",
            "claude-opus-5",
            "claude-opus-5-5",
            "claude-sonnet-5",
            "claude-sonnet-5-5",
        ]
        .into_iter()
        .find(|f| crate::generation::claude_family(&id, f))?;
        let efforts = if provider == "bedrock" {
            match family {
                "claude-opus-4-8" => return None,
                "claude-opus-4-6" if id == "claude-opus-4-6-v1" => {
                    ANTHROPIC_EFFORTS_LOW_TO_XHIGH_MAX
                }
                "claude-opus-5" => ANTHROPIC_EFFORTS_LOW_TO_XHIGH_MAX,
                "claude-sonnet-4-6" => ANTHROPIC_EFFORTS_LOW_TO_MAX,
                _ => ANTHROPIC_EFFORTS_LOW_TO_HIGH,
            }
        } else {
            match family {
                "claude-opus-4-5" => ANTHROPIC_EFFORTS_LOW_TO_HIGH,
                "claude-opus-4-6" | "claude-sonnet-4-6" | "claude-mythos-preview" => {
                    ANTHROPIC_EFFORTS_LOW_TO_MAX
                }
                _ => ANTHROPIC_EFFORTS_LOW_TO_XHIGH_MAX,
            }
        };
        (
            efforts,
            if provider == "anthropic" && family == "claude-opus-5-5" {
                "medium"
            } else {
                "high"
            },
            Some(crate::generation::protocol_mandatory(provider, model_id)),
        )
    } else if provider == "gemini" {
        let id = model_id.strip_suffix("-preview").unwrap_or(model_id);
        let (efforts, default) = match id {
            "gemini-3.1-pro" | "gemini-3.1-pro-preview-customtools" => {
                (GEMINI_THINKING_LOW_TO_HIGH, "high")
            }
            "gemini-3-pro" => (GEMINI_THINKING_LOW_HIGH, "high"),
            "gemini-3.7-flash" | "gemini-3.8-flash" => (GEMINI_THINKING_LOW_TO_HIGH, "medium"),
            "gemini-3.5-flash" | "gemini-3.6-flash" => (GEMINI_THINKING_MINIMAL_TO_HIGH, "medium"),
            "gemini-3.1-flash-lite" | "gemini-3.5-flash-lite" => {
                (GEMINI_THINKING_MINIMAL_TO_HIGH, "minimal")
            }
            "gemini-3.1-flash-lite-image" => (&["minimal", "high"][..], "minimal"),
            "gemini-3-flash" => (GEMINI_THINKING_MINIMAL_TO_HIGH, "high"),
            _ => return None,
        };
        (efforts, default, Some(true))
    } else {
        return None;
    };
    let mut controls: Vec<String> = efforts.iter().map(|e| (*e).into()).collect();
    // Product off is a thinking mode, never a Claude output_config effort.
    // Only the bounded optional-thinking Messages/Converse profiles above
    // document disabled; mandatory/between_tools profiles do not grant it.
    if matches!(provider, "anthropic" | "bedrock") && mandatory == Some(false) {
        controls.insert(0, "none".into());
    }
    Some(ProviderModelReasoningCapabilities {
        supported: Some(true),
        effort_options: controls,
        default_effort: Some(default.into()),
        mandatory,
        source: Some(ReasoningCapabilitySource::StaticRegistry),
        ..Default::default()
    })
}

/// Preserve native unknown/false/true for both aggregates and sublevels.
pub(crate) fn read_native_supports(
    value: &serde_json::Value,
    prefix: &str,
    facts: &mut std::collections::BTreeMap<String, Option<bool>>,
) {
    if value.is_null() {
        facts.insert(format!("{prefix}.supported"), None);
    }
    if let Some(object) = value.as_object() {
        for (key, value) in object {
            if key == "supported" {
                facts.insert(format!("{prefix}.supported"), value.as_bool());
            } else if let Some(supported) = value.get("supported") {
                facts.insert(format!("{prefix}.{key}"), supported.as_bool());
            }
        }
    }
}

pub(crate) fn apply_native_reasoning(
    capabilities: &mut ProviderModelCapabilities,
    facts: std::collections::BTreeMap<String, Option<bool>>,
) {
    if facts.is_empty() {
        return;
    }
    let had_fallback = capabilities.reasoning.is_some();
    let mut reasoning = capabilities.reasoning.take().unwrap_or_default();
    capabilities.thinking = facts
        .get("thinking.supported")
        .copied()
        .flatten()
        .or(capabilities.thinking);
    for (key, value) in &facts {
        let Some(level) = key
            .strip_prefix("effort.")
            .filter(|e| crate::ReasoningEffort::from_str(e).is_some())
        else {
            continue;
        };
        if let Some(supported) = value {
            reasoning.effort_options.retain(|e| e != level);
            if *supported {
                reasoning.effort_options.push(level.into());
            }
        }
    }
    if facts.get("effort.supported") == Some(&Some(false)) {
        reasoning.effort_options.retain(|e| e == "none");
    }
    if facts.get("thinking.supported") == Some(&Some(false))
        && facts.get("effort.supported") == Some(&Some(false))
    {
        reasoning.supported = Some(false);
    } else if facts.values().any(|v| *v == Some(true)) {
        reasoning.supported = Some(true);
    }
    reasoning.source = Some(if had_fallback {
        ReasoningCapabilitySource::Unknown
    } else {
        ReasoningCapabilitySource::ProviderMetadata
    });
    reasoning.native = facts;
    capabilities.reasoning = Some(reasoning);
}

pub fn apply_reasoning_capabilities(
    provider: &str,
    model_id: &str,
    capabilities: &mut ProviderModelCapabilities,
) {
    if let Some(reasoning) = reasoning_capabilities_for_model(provider, model_id) {
        capabilities.thinking = reasoning.supported;
        capabilities.reasoning = Some(reasoning);
    }
}

pub(crate) fn reasoning_capabilities_for_model_with_rules(
    rules: &[ReasoningModelRule],
    provider: &str,
    model_id: &str,
) -> Option<ProviderModelReasoningCapabilities> {
    matching_reasoning_rule(rules, provider, model_id)
        .map(reasoning_capabilities_from_registry_rule)
}

pub(crate) fn matching_reasoning_rule<'a>(
    rules: &'a [ReasoningModelRule],
    provider: &str,
    model_id: &str,
) -> Option<&'a ReasoningModelRule> {
    let provider = provider.trim();
    let model_id = model_id.trim();
    if provider.is_empty() || model_id.is_empty() {
        return None;
    }

    rules
        .iter()
        .filter(|rule| rule.provider.eq_ignore_ascii_case(provider))
        .filter(|rule| matches_model(rule.matcher, model_id))
        .max_by_key(|rule| matcher_precedence(rule.matcher))
}

pub(crate) fn reasoning_capabilities_from_registry_rule(
    rule: &ReasoningModelRule,
) -> ProviderModelReasoningCapabilities {
    ProviderModelReasoningCapabilities {
        native: Default::default(),
        supported: Some(rule.supported),
        effort_options: rule
            .effort_options
            .iter()
            .map(|effort| (*effort).to_owned())
            .collect(),
        default_effort: rule.default_effort.map(str::to_owned),
        mandatory: rule.mandatory,
        supports_token_budget: None,
        source: Some(ReasoningCapabilitySource::StaticRegistry),
    }
}

pub(crate) fn matches_model(matcher: ReasoningModelMatcher, model_id: &str) -> bool {
    match matcher {
        ReasoningModelMatcher::Exact(exact) => model_id.eq_ignore_ascii_case(exact),
        ReasoningModelMatcher::Prefix(prefix) => model_id
            .to_ascii_lowercase()
            .starts_with(prefix.to_ascii_lowercase().as_str()),
    }
}

fn matcher_precedence(matcher: ReasoningModelMatcher) -> (u8, usize) {
    match matcher {
        ReasoningModelMatcher::Exact(model_id) => (2, model_id.len()),
        ReasoningModelMatcher::Prefix(prefix) => (1, prefix.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const TEST_RULES: &[ReasoningModelRule] = &[
        ReasoningModelRule {
            provider: "test_provider",
            matcher: ReasoningModelMatcher::Prefix("family-"),
            supported: true,
            effort_options: &["low", "medium"],
            default_effort: Some("medium"),
            mandatory: Some(false),
            source_url: "https://example.com/family",
        },
        ReasoningModelRule {
            provider: "test_provider",
            matcher: ReasoningModelMatcher::Exact("family-exact"),
            supported: true,
            effort_options: &["high"],
            default_effort: Some("high"),
            mandatory: Some(true),
            source_url: "https://example.com/exact",
        },
        ReasoningModelRule {
            provider: "other_provider",
            matcher: ReasoningModelMatcher::Prefix("family-"),
            supported: false,
            effort_options: &[],
            default_effort: None,
            mandatory: None,
            source_url: "https://example.com/other",
        },
    ];

    #[test]
    fn exact_match_wins_over_prefix_match() {
        let capabilities = reasoning_capabilities_for_model_with_rules(
            TEST_RULES,
            "test_provider",
            "family-exact",
        )
        .expect("exact rule");

        assert_eq!(capabilities.supported, Some(true));
        assert_eq!(capabilities.effort_options, vec!["high".to_owned()]);
        assert_eq!(capabilities.default_effort.as_deref(), Some("high"));
        assert_eq!(capabilities.mandatory, Some(true));
        assert_eq!(
            capabilities.source,
            Some(ReasoningCapabilitySource::StaticRegistry)
        );
    }

    #[test]
    fn prefix_match_returns_registry_capabilities() {
        let capabilities = reasoning_capabilities_for_model_with_rules(
            TEST_RULES,
            "test_provider",
            "family-model",
        )
        .expect("prefix rule");

        assert_eq!(
            capabilities.effort_options,
            vec!["low".to_owned(), "medium".to_owned()]
        );
        assert_eq!(capabilities.default_effort.as_deref(), Some("medium"));
        assert_eq!(capabilities.mandatory, Some(false));
    }

    #[test]
    fn unmatched_model_returns_none() {
        assert!(
            reasoning_capabilities_for_model_with_rules(
                TEST_RULES,
                "test_provider",
                "unknown-model"
            )
            .is_none()
        );
    }

    #[test]
    fn provider_must_match_rule_provider() {
        let capabilities = reasoning_capabilities_for_model_with_rules(
            TEST_RULES,
            "other_provider",
            "family-model",
        )
        .expect("other provider rule");

        assert_eq!(capabilities.supported, Some(false));
        assert!(capabilities.effort_options.is_empty());
    }

    #[test]
    fn production_rules_all_have_source_urls() {
        for rule in REASONING_MODEL_RULES {
            assert!(
                rule.source_url.starts_with("https://"),
                "source URL for provider `{}` rule `{:?}` must be HTTPS: {}",
                rule.provider,
                rule.matcher,
                rule.source_url
            );
        }
    }

    #[test]
    fn production_rules_do_not_duplicate_exact_provider_model_pairs() {
        let mut seen = HashSet::new();
        for rule in REASONING_MODEL_RULES {
            let ReasoningModelMatcher::Exact(model_id) = rule.matcher else {
                continue;
            };
            let key = (
                rule.provider.to_ascii_lowercase(),
                model_id.to_ascii_lowercase(),
            );
            assert!(
                seen.insert(key),
                "duplicate exact reasoning registry rule for provider `{}` model `{}`",
                rule.provider,
                model_id
            );
        }
    }
}
