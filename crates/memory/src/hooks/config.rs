use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPostTurnExtractorConfig {
    pub enabled: bool,
    pub provider_enabled: bool,
    pub proactive_writes_enabled: bool,
    pub await_policy: HookAwaitPolicy,
    pub provider_name: Option<String>,
    pub model: Option<String>,
    pub timeout_ms: u64,
    pub max_facts_per_turn: usize,
    pub max_input_chars: usize,
    pub max_manifest_items: usize,
    pub max_fact_content_chars: usize,
    pub max_evidence_chars: usize,
    pub strict_debug: bool,
}

impl Default for MemoryPostTurnExtractorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            provider_enabled: true,
            proactive_writes_enabled: true,
            await_policy: HookAwaitPolicy::FireAndRecord,
            provider_name: None,
            model: None,
            timeout_ms: 180_000,
            max_facts_per_turn: 8,
            max_input_chars: 8_000,
            max_manifest_items: 24,
            max_fact_content_chars: 800,
            max_evidence_chars: 500,
            strict_debug: false,
        }
    }
}

impl MemoryPostTurnExtractorConfig {
    pub fn normalized(&self) -> Self {
        Self {
            enabled: self.enabled,
            provider_enabled: self.provider_enabled,
            proactive_writes_enabled: self.proactive_writes_enabled,
            await_policy: match self.await_policy {
                HookAwaitPolicy::Background | HookAwaitPolicy::FireAndRecord => self.await_policy,
                HookAwaitPolicy::Blocking | HookAwaitPolicy::Deadline => {
                    HookAwaitPolicy::FireAndRecord
                }
            },
            provider_name: normalized_optional_config_text(self.provider_name.as_deref(), 80),
            model: normalized_optional_config_text(self.model.as_deref(), 160),
            timeout_ms: self.timeout_ms.max(1),
            max_facts_per_turn: self.max_facts_per_turn.max(1),
            max_input_chars: self.max_input_chars.max(1),
            max_manifest_items: self.max_manifest_items.max(1),
            max_fact_content_chars: self.max_fact_content_chars.max(1),
            max_evidence_chars: self.max_evidence_chars.max(1),
            strict_debug: self.strict_debug,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryLoopConfig {
    pub tools_enabled: bool,
    pub post_turn_extractor: MemoryPostTurnExtractorConfig,
}

impl Default for MemoryLoopConfig {
    fn default() -> Self {
        Self {
            tools_enabled: true,
            post_turn_extractor: MemoryPostTurnExtractorConfig::default(),
        }
    }
}

impl MemoryLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            tools_enabled: self.tools_enabled,
            post_turn_extractor: self.post_turn_extractor.normalized(),
        }
    }
}

fn normalized_optional_config_text(value: Option<&str>, max_chars: usize) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.chars().take(max_chars).collect())
}
