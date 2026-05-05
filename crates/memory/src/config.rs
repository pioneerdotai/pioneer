use pioneer_protocol::MemorySensitivity;

#[derive(Debug, Clone)]
pub struct MemoryServiceConfig {
    pub default_limit: u32,
    pub max_limit: u32,
    pub max_content_bytes: usize,
    pub content_preview_chars: usize,
    pub policy_version: String,
    pub default_read_policy: MemoryReadPolicy,
    pub ranking: MemoryRankingConfig,
    pub recall: MemoryRecallConfig,
}

impl Default for MemoryServiceConfig {
    fn default() -> Self {
        Self {
            default_limit: 20,
            max_limit: 100,
            max_content_bytes: 64 * 1024,
            content_preview_chars: 240,
            policy_version: "memory_policy_v1".to_owned(),
            default_read_policy: MemoryReadPolicy::default(),
            ranking: MemoryRankingConfig::default(),
            recall: MemoryRecallConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryRankingConfig {
    pub backend_score_weight: f32,
    pub exact_key_boost: f32,
    pub category_match_boost: f32,
    pub primary_scope_boost: f32,
    pub scope_rank_boost: f32,
    pub recency_boost_max: f32,
    pub recency_half_life_secs: i64,
    pub importance_weight: f32,
    pub confidence_weight: f32,
    pub backend_candidate_multiplier: u32,
    pub max_backend_candidates: u32,
}

impl Default for MemoryRankingConfig {
    fn default() -> Self {
        Self {
            backend_score_weight: 1.0,
            exact_key_boost: 2.0,
            category_match_boost: 0.5,
            primary_scope_boost: 0.6,
            scope_rank_boost: 0.4,
            recency_boost_max: 0.4,
            recency_half_life_secs: 30 * 24 * 60 * 60,
            importance_weight: 0.8,
            confidence_weight: 0.2,
            backend_candidate_multiplier: 4,
            max_backend_candidates: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryRecallConfig {
    pub prompt_top_k: u32,
    pub tool_search_limit: u32,
    pub max_prompt_chars: usize,
    pub max_item_chars: usize,
}

impl Default for MemoryRecallConfig {
    fn default() -> Self {
        Self {
            prompt_top_k: 8,
            tool_search_limit: 20,
            max_prompt_chars: 4_000,
            max_item_chars: 500,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryReadPolicy {
    pub allow_normal: bool,
    pub allow_personal: bool,
    pub allow_secret_like: bool,
    pub allow_regulated: bool,
}

impl Default for MemoryReadPolicy {
    fn default() -> Self {
        Self {
            allow_normal: true,
            allow_personal: true,
            allow_secret_like: false,
            allow_regulated: false,
        }
    }
}

impl MemoryReadPolicy {
    pub fn allow_all() -> Self {
        Self {
            allow_normal: true,
            allow_personal: true,
            allow_secret_like: true,
            allow_regulated: true,
        }
    }

    pub fn allows(&self, sensitivity: MemorySensitivity) -> bool {
        match sensitivity {
            MemorySensitivity::Normal => self.allow_normal,
            MemorySensitivity::Personal => self.allow_personal,
            MemorySensitivity::SecretLike => self.allow_secret_like,
            MemorySensitivity::Regulated => self.allow_regulated,
        }
    }
}
