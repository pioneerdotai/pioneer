use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryActiveRecallMode {
    Disabled,
    DeterministicOnly,
    Hybrid,
    StrictDebug,
}

impl Default for MemoryActiveRecallMode {
    fn default() -> Self {
        Self::Hybrid
    }
}

impl MemoryActiveRecallMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::DeterministicOnly => "deterministic_only",
            Self::Hybrid => "hybrid",
            Self::StrictDebug => "strict_debug",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryActiveRecallConfig {
    pub mode: MemoryActiveRecallMode,
    pub timeout_ms: u64,
    pub max_queries: usize,
    pub top_k_per_query: u32,
    pub max_prompt_chars: usize,
    pub deterministic_sufficient_min_items: usize,
    pub deterministic_sufficient_min_chars: usize,
}

impl Default for MemoryActiveRecallConfig {
    fn default() -> Self {
        Self {
            mode: MemoryActiveRecallMode::Hybrid,
            timeout_ms: 800,
            max_queries: 3,
            top_k_per_query: 5,
            max_prompt_chars: 1_500,
            deterministic_sufficient_min_items: 1,
            deterministic_sufficient_min_chars: 600,
        }
    }
}

impl MemoryActiveRecallConfig {
    pub fn normalized(&self) -> Self {
        Self {
            mode: self.mode,
            timeout_ms: self.timeout_ms.max(1),
            max_queries: self.max_queries.max(1),
            top_k_per_query: self.top_k_per_query.max(1),
            max_prompt_chars: self.max_prompt_chars.max(1),
            deterministic_sufficient_min_items: self.deterministic_sufficient_min_items.max(1),
            deterministic_sufficient_min_chars: self.deterministic_sufficient_min_chars.max(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryPostTurnExtractorConfig {
    pub enabled: bool,
    pub provider_enabled: bool,
    pub proactive_writes_enabled: bool,
    pub await_policy: HookAwaitPolicy,
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
            timeout_ms: 1_500,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemoryLoopConfig {
    pub active_recall: MemoryActiveRecallConfig,
    pub post_turn_extractor: MemoryPostTurnExtractorConfig,
}

impl MemoryLoopConfig {
    pub fn normalized(&self) -> Self {
        Self {
            active_recall: self.active_recall.normalized(),
            post_turn_extractor: self.post_turn_extractor.normalized(),
        }
    }
}
