use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn is_false(value: &bool) -> bool {
    !*value
}

// --- Provider list ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListParams {
    pub workspace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListResponse {
    pub providers: Vec<ProviderSummary>,
    /// Gateway-owned endpoint definitions, including providers without credentials.
    #[serde(default)]
    pub definitions: Vec<ProviderDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProviderDefinition {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_base_url: Option<String>,
    #[serde(default)]
    pub supports_base_url_override: bool,
    /// Terminal product lifecycle diagnostic; old clients can ignore it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retirement_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderSummary {
    pub name: String,
    #[serde(default)]
    pub capabilities: ProviderSummaryCapabilities,
    #[serde(default, skip_serializing_if = "is_false")]
    pub api_key_configured: bool,
    /// The gateway can construct this provider in the selected workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProviderSummaryCapabilities {
    #[serde(default)]
    pub embeddings: bool,
    #[serde(default)]
    pub transcription: bool,
    /// Gateway-authoritative eligibility for the API-only self-improvement
    /// model selectors.
    #[serde(default)]
    pub self_improvement: bool,
}

// --- Provider list models ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListModelsParams {
    pub workspace_id: String,
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderListModelsResponse {
    pub provider: String,
    pub models: Vec<ProviderModelInfo>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ProviderModelPricing {
    pub input_token: Option<f64>,
    pub output_token: Option<f64>,
    pub image: Option<f64>,
    pub request: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ProviderModelLimits {
    pub max_input_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub context_window: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningCapabilitySource {
    ProviderMetadata,
    CliMetadata,
    ConfigOverride,
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProviderModelReasoningCapabilities {
    /// Raw Models API reasoning facts. Missing key means absent; null means
    /// unknown. Effort, thinking support and thinking types remain separate.
    #[serde(skip)]
    #[schemars(skip)]
    pub native: std::collections::BTreeMap<String, Option<bool>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supported: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub effort_options: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mandatory: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_token_budget: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<ReasoningCapabilitySource>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct ProviderModelCapabilities {
    /// Authority-native proof of conversational API support, consumed before
    /// publishing picker lists. This is not a wire capability or a runtime gate.
    #[serde(skip)]
    #[schemars(skip)]
    pub chat: Option<bool>,
    /// Native support for the adapter's chat operation; text modalities must
    /// still be established independently before offering conversational use.
    #[serde(skip)]
    #[schemars(skip)]
    pub chat_operation: Option<bool>,
    pub vision: Option<bool>,
    pub tool_calling: Option<bool>,
    pub json_output: Option<bool>,
    pub streaming: Option<bool>,
    pub embeddings: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcription: Option<bool>,
    pub thinking: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ProviderModelReasoningCapabilities>,
    pub fine_tuning: Option<bool>,
    pub input_modalities: Option<Vec<String>>,
    pub output_modalities: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ProviderTranscriptionModelMetadata {
    pub engine: String,
    pub download_size_mb: u64,
    pub accuracy_score: u8,
    pub speed_score: u8,
    pub supports_translation: bool,
    pub supported_languages: Vec<String>,
    pub supports_language_selection: bool,
    pub recommended: bool,
}

/// Non-secret proof binding a picker choice to one actual authority and model.
/// An instance's credential/endpoint change or a model switch invalidates it.
pub fn native_context_choice_authority(authority: &str, model: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"pioneer:native-context-choice:v1\0");
    digest.update((authority.len() as u64).to_le_bytes());
    digest.update(authority.as_bytes());
    digest.update(model.as_bytes());
    hex::encode(digest.finalize())
}

/// API summarizer selection and its resolved budget, captured with a Native launch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NativeSummaryContext {
    pub budget: NativeContextBudget,
    pub effort: Option<String>,
    pub inherits_working: bool,
}

/// Durable local authorization and budget for one Native/API provider authority/model.
/// Never sent to a provider or accepted as a child tool's requested variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NativeContextBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_mode: Option<crate::NativeServiceModePlan>,
    pub instance: String,
    pub authority: Option<String>,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_window: Option<u64>,
    pub context_window: u64,
    pub max_input: Option<u64>,
    pub max_output: Option<u64>,
}

impl NativeContextBudget {
    /// Project the admitted choice, retaining this authority/model scope.
    pub fn service_choice(&self) -> Option<crate::NativeServiceModeSelection> {
        let plan = self
            .service_mode
            .as_ref()
            .filter(|plan| plan.id != "standard")?;
        Some(crate::NativeServiceModeSelection {
            id: plan.id.clone(),
            authority: native_context_choice_authority(self.authority.as_deref()?, &self.model),
        })
    }
}

/// Authority-scoped, normalized context choices. Absence on a discovery DTO
/// means unavailable metadata, distinct from a confirmed model without tiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProviderContextTiers {
    NoTiers,
    Excluded,
    Invalid,
    Tiered {
        windows: Vec<u64>,
        price_order_confirmed: bool,
    },
}

impl ProviderContextTiers {
    /// Reachable tier endpoints under an actual discovery/technical limit.
    /// Preserve tariff boundaries and clamp only the final reachable endpoint.
    pub fn with_technical_limit(&self, technical_window: u64) -> Self {
        let Self::Tiered {
            windows,
            price_order_confirmed,
        } = self
        else {
            return self.clone();
        };
        if technical_window == 0
            || windows.first().is_none_or(|window| *window == 0)
            || windows.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Self::Invalid;
        }
        let cap = technical_window.min(*windows.last().expect("validated nonempty windows"));
        let mut windows: Vec<_> = windows
            .iter()
            .copied()
            .take_while(|window| *window < cap)
            .collect();
        windows.push(cap);
        Self::Tiered {
            windows,
            price_order_confirmed: *price_order_confirmed,
        }
    }

    /// Resolve Auto or an explicitly authorized manual window. Stale overrides
    /// fail explicitly; callers must never replace them with a wider choice.
    pub fn resolve(&self, technical_window: u64, manual: Option<u64>) -> Result<u64, &'static str> {
        match &self.with_technical_limit(technical_window) {
            Self::Invalid => Err("Context price metadata is invalid"),
            Self::NoTiers | Self::Excluded if manual.is_none() => Ok(technical_window),
            Self::NoTiers | Self::Excluded => Err("Context selection is no longer available"),
            Self::Tiered {
                windows,
                price_order_confirmed,
            } => {
                if technical_window == 0 || windows.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err("Context price metadata is invalid");
                }
                let default = windows
                    .first()
                    .copied()
                    .filter(|v| *v > 0)
                    .ok_or("Context price metadata is empty")?;
                let selected = manual.unwrap_or(default);
                if !windows.contains(&selected) || (manual.is_some() && selected > technical_window)
                {
                    return Err("Context selection is no longer available");
                }
                if manual.is_some() && selected > default && !price_order_confirmed {
                    return Err("Context price order is not confirmed");
                }
                Ok(technical_window.min(selected))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderModelInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_modes: Option<crate::SourceServiceModes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_authority: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_tiers: Option<ProviderContextTiers>,
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created: Option<i64>,
    pub provider: String,
    pub owned_by: Option<String>,
    pub limits: ProviderModelLimits,
    pub capabilities: ProviderModelCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcription: Option<ProviderTranscriptionModelMetadata>,
    pub pricing: Option<ProviderModelPricing>,
    pub active: Option<bool>,
    pub family: Option<String>,
    pub lifecycle_status: Option<String>,
}

// --- Provider API key management ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderConfigureParams {
    pub workspace_id: String,
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    #[serde(default)]
    pub clear_proxy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default)]
    pub clear_base_url: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderConfigureResponse {
    pub provider: String,
    #[serde(default)]
    pub api_key_updated: bool,
    #[serde(default)]
    pub proxy_updated: bool,
    #[serde(default)]
    pub proxy_deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    #[serde(default)]
    pub base_url_updated: bool,
    #[serde(default)]
    pub base_url_deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderSetApiKeyParams {
    pub workspace_id: String,
    pub provider: String,
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderSetApiKeyResponse {
    pub provider: String,
    pub updated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderDeleteApiKeyParams {
    pub workspace_id: String,
    pub provider: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProviderDeleteApiKeyResponse {
    pub provider: String,
    pub deleted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn provider_model_capabilities_round_trips_reasoning_metadata() {
        let capabilities: ProviderModelCapabilities = serde_json::from_value(json!({
            "thinking": true,
            "reasoning": {
                "supported": true,
                "effort_options": ["low", "high"],
                "default_effort": "medium",
                "mandatory": false,
                "supports_token_budget": true,
                "source": "provider_metadata"
            }
        }))
        .expect("capabilities should decode");

        assert_eq!(capabilities.thinking, Some(true));
        assert_eq!(
            capabilities.reasoning,
            Some(ProviderModelReasoningCapabilities {
                native: Default::default(),
                supported: Some(true),
                effort_options: vec!["low".to_owned(), "high".to_owned()],
                default_effort: Some("medium".to_owned()),
                mandatory: Some(false),
                supports_token_budget: Some(true),
                source: Some(ReasoningCapabilitySource::ProviderMetadata),
            })
        );

        let encoded = serde_json::to_value(capabilities).expect("capabilities should encode");
        assert_eq!(encoded["thinking"], json!(true));
        assert_eq!(
            encoded["reasoning"],
            json!({
                "supported": true,
                "effort_options": ["low", "high"],
                "default_effort": "medium",
                "mandatory": false,
                "supports_token_budget": true,
                "source": "provider_metadata"
            })
        );
    }

    #[test]
    fn provider_model_capabilities_decode_without_reasoning_metadata() {
        let capabilities: ProviderModelCapabilities = serde_json::from_value(json!({
            "thinking": false
        }))
        .expect("legacy capabilities should decode");

        assert_eq!(capabilities.thinking, Some(false));
        assert!(capabilities.reasoning.is_none());
        assert!(capabilities.transcription.is_none());
    }

    #[test]
    fn legacy_provider_payloads_decode_without_transcription_fields() {
        let summary: ProviderSummaryCapabilities =
            serde_json::from_value(json!({ "embeddings": true }))
                .expect("legacy provider summary capabilities should decode");
        assert!(summary.embeddings);
        assert!(!summary.transcription);

        let model: ProviderModelInfo = serde_json::from_value(json!({
            "id": "legacy-model",
            "name": "Legacy model",
            "description": null,
            "created": null,
            "provider": "legacy",
            "owned_by": null,
            "limits": {
                "max_input_tokens": null,
                "max_output_tokens": null,
                "context_window": null
            },
            "capabilities": {},
            "pricing": null,
            "active": true,
            "family": null,
            "lifecycle_status": null
        }))
        .expect("legacy provider model should decode");

        assert!(model.capabilities.transcription.is_none());
        assert!(model.transcription.is_none());

        let encoded = serde_json::to_value(model).expect("legacy provider model should encode");
        assert!(encoded.get("transcription").is_none());
        assert!(encoded["capabilities"].get("transcription").is_none());
    }

    #[test]
    fn provider_transcription_metadata_round_trips_without_trusted_fields() {
        let metadata = ProviderTranscriptionModelMetadata {
            engine: "parakeet".to_owned(),
            download_size_mb: 456,
            accuracy_score: 80,
            speed_score: 85,
            supports_translation: false,
            supported_languages: vec!["en".to_owned(), "ru".to_owned()],
            supports_language_selection: false,
            recommended: true,
        };

        let encoded = serde_json::to_value(&metadata).expect("metadata should encode");
        let decoded: ProviderTranscriptionModelMetadata =
            serde_json::from_value(encoded.clone()).expect("metadata should decode");

        assert_eq!(decoded, metadata);
        for trusted_field in [
            "url",
            "sha256",
            "artifact_file_name",
            "install_dir_name",
            "runtime_file_name",
        ] {
            assert!(
                encoded.get(trusted_field).is_none(),
                "trusted field leaked: {trusted_field}"
            );
        }

        let schema = schemars::schema_for!(ProviderTranscriptionModelMetadata);
        let schema_json = serde_json::to_string(&schema).expect("schema should encode");
        for trusted_field in [
            "url",
            "sha256",
            "artifact_file_name",
            "install_dir_name",
            "runtime_file_name",
        ] {
            assert!(
                !schema_json.contains(trusted_field),
                "trusted field leaked into schema: {trusted_field}"
            );
        }
    }

    #[test]
    fn provider_summary_and_configure_round_trip_base_url() {
        let summary: ProviderSummary = serde_json::from_value(json!({
            "name": "openai",
            "base_url": "https://api.example.com/v1"
        }))
        .expect("summary should decode");
        assert_eq!(
            summary.base_url.as_deref(),
            Some("https://api.example.com/v1")
        );
        assert_eq!(summary.available, None);
        let legacy_list: ProviderListResponse = serde_json::from_value(json!({
            "providers": [{"name": "openai", "api_key_configured": true}]
        }))
        .expect("old gateway list should decode");
        assert!(legacy_list.definitions.is_empty());
        assert_eq!(legacy_list.providers[0].available, None);

        let configure: ProviderConfigureParams = serde_json::from_value(json!({
            "workspace_id": "ws_default",
            "provider": "openai",
            "base_url": "https://api.example.com/v1",
            "clear_base_url": false
        }))
        .expect("configure params should decode");
        assert_eq!(
            configure.base_url.as_deref(),
            Some("https://api.example.com/v1")
        );
        assert!(!configure.clear_base_url);

        let legacy_configure: ProviderConfigureParams = serde_json::from_value(json!({
            "workspace_id": "ws_default",
            "provider": "openai"
        }))
        .expect("legacy configure params should decode");
        assert!(legacy_configure.base_url.is_none());
        assert!(!legacy_configure.clear_base_url);
    }
}

/// Totals from persisted provider usage items. This is observed consumption,
/// not a bill, a context-window measurement, or an estimate of missing usage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObservedProviderUsageTotals {
    pub attempts: usize,
    pub confirmed_physical_attempts: usize,
    pub unconfirmed_dispatches: usize,
    pub known_input_tokens: u64,
    pub known_output_tokens: u64,
    /// Cache totals are paired with full input totals; missing is never zero.
    pub known_cache_read_input_tokens: u64,
    pub cache_ratio_input_tokens: u64,
    pub missing_cache_ratio_attempts: usize,
    pub missing_input_attempts: usize,
    pub missing_output_attempts: usize,
    pub incomplete_attempts: usize,
    /// Native cost snapshots by their accounting scope. SDK estimates and
    /// unknown-origin runtime amounts are deliberately not a confirmed bill.
    pub cli_cost_observations: std::collections::BTreeMap<String, serde_json::Value>,
    /// Native session snapshots with unknown selected-range baseline/replay epoch.
    /// These are evidence, not consumption attributable to the supplied range.
    pub unattributed_session_observations: std::collections::BTreeMap<String, serde_json::Value>,
}

impl ObservedProviderUsageTotals {
    /// Weighted observed cache hit rate, available only with a complete denominator.
    pub fn cache_hit_rate(&self) -> Option<f64> {
        (self.unattributed_session_observations.is_empty()
            && self.missing_cache_ratio_attempts == 0
            && self.cache_ratio_input_tokens > 0)
            .then(|| {
                self.known_cache_read_input_tokens as f64 / self.cache_ratio_input_tokens as f64
            })
    }
}

/// Fold durable usage observations. Session totals with no attested range baseline
/// remain unattributed evidence, even when counters decrease or repeat. CLI
/// observations do not establish physical requests or billing.
pub fn observed_provider_usage_totals<'a>(
    items: impl IntoIterator<Item = &'a crate::TurnItem>,
) -> ObservedProviderUsageTotals {
    let mut observations = Vec::new();
    for item in items {
        if let crate::TurnItem::SystemEvent {
            id,
            code,
            details: Some(details),
            ..
        } = item
        {
            if code.as_deref() == Some("provider_usage")
                && details["schema_version"].as_u64() == Some(1)
            {
                observations.push((id, details.clone()));
            } else if code.as_deref() == Some("agent_runtime_item")
                && details["nativeItemKind"] == "tokenUsage"
                && details["semantics"] == "opencode_v2_exclusive_cache_separate_reasoning"
            {
                // Compatibility for already persisted OpenCode V2 observations.
                // These are turn aggregates, not confirmed physical requests.
                observations.push((
                    id,
                    serde_json::json!({
                        "usage": details, "complete": details["status"] == "completed"
                    }),
                ));
            }
        }
    }
    // Fold overlapping main-loop message/result evidence into one turn bucket.
    // Session-inclusive modelUsage never participates in these categories.
    let mut claude = std::collections::BTreeMap::<(String, String), Vec<serde_json::Value>>::new();
    observations.retain(|(_, details)| {
        let u = &details["usage"];
        if u["runtime"] == "claude" {
            if let (Some(thread), Some(turn)) =
                (u["native_thread_id"].as_str(), u["native_turn_id"].as_str())
            {
                claude
                    .entry((thread.into(), turn.into()))
                    .or_default()
                    .push(details.clone());
                return false;
            }
        }
        true
    });
    let mut folded = Vec::new();
    for ((thread, turn), snapshots) in claude {
        let fields = ["input_tokens", "output_tokens", "cache_read_input_tokens"];
        let mut terminal = [None::<u64>; 3];
        let mut messages = std::collections::BTreeMap::<String, [Option<u64>; 3]>::new();
        let mut expected = None;
        let mut result = serde_json::json!({"usage":{"runtime":"claude", "scope":"turn_snapshot", "native_thread_id":thread,"native_turn_id":turn}});
        let max = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        for d in snapshots {
            let u = &d["usage"];
            if u["scope"] == "turn_snapshot" {
                for (i, field) in fields.iter().enumerate() {
                    terminal[i] = max(terminal[i], u[*field].as_u64());
                }
                expected = u["num_turns"].as_u64().or(expected);
                for field in ["total_cost_usd", "cost_basis", "model_usage"] {
                    if !u[field].is_null() {
                        result["usage"][field] = u[field].clone();
                    }
                }
            } else if u["scope"] == "request_snapshot" {
                if let Some(id) = u["native_message_id"].as_str() {
                    let counters = messages.entry(id.into()).or_insert([None; 3]);
                    for (i, field) in fields.iter().enumerate() {
                        counters[i] = max(counters[i], u[*field].as_u64());
                    }
                }
            }
        }
        let coverage = !messages.is_empty() && expected.is_none_or(|n| n == messages.len() as u64);
        let fallback: [Option<u64>; 3] = std::array::from_fn(|i| {
            coverage
                .then(|| {
                    messages
                        .values()
                        .try_fold(0_u64, |sum, counters| sum.checked_add(counters[i]?))
                })
                .flatten()
        });
        for (i, field) in fields.iter().enumerate() {
            result["usage"][*field] = serde_json::json!(terminal[i].or(fallback[i]));
        }
        // Keep observed categories even when their scopes cannot form a ratio.
        let coherent =
            !(terminal[0].is_none() && terminal[2].is_some() && terminal[2] != fallback[2]
                || terminal[2].is_none() && terminal[0].is_some() && terminal[0] != fallback[0]);
        result["usage"]["cache_ratio_consistent"] = serde_json::json!(coherent);
        result["complete"] = serde_json::json!(terminal.iter().all(Option::is_some));
        folded.push((format!("claude:{thread}:{turn}"), result));
    }
    observations.extend(folded.iter().map(|(id, d)| (id, d.clone())));
    // Delivery replay can materialize the same physical attempt under another
    // item ID. Cumulative counts take the maximum; missing never erases known.
    let mut distinct = std::collections::BTreeMap::<
        String,
        (Option<u64>, Option<u64>, bool, bool, Option<u64>, bool),
    >::new();
    let mut costs = std::collections::BTreeMap::<String, serde_json::Value>::new();
    let mut session_evidence = std::collections::BTreeMap::new();
    for (id, details) in observations {
        let usage = &details["usage"];
        let native_thread = usage["native_thread_id"].as_str();
        let native_turn = usage["native_turn_id"].as_str();
        if usage["scope"] == "session_snapshot" {
            // No native sequence/epoch/baseline is available. A counter decrease
            // could also be late replay; numeric equality cannot identify replay.
            // Preserve every durable observation but exclude session history from
            // consumption of an arbitrary selected turn/range (including forks).
            session_evidence.insert(id.to_owned(), usage.clone());
            continue;
        }
        if let (Some(runtime), Some(thread)) = (usage["runtime"].as_str(), native_thread) {
            let (amount, scope, basis, key) = if runtime == "claude" {
                (
                    usage["total_cost_usd"].as_f64(),
                    "session_including_subagents",
                    "claude_sdk_estimate",
                    format!("{runtime}:{thread}"),
                )
            } else {
                (
                    usage["cost_usd"].as_f64(),
                    "turn",
                    "opencode_reported_unknown_origin",
                    format!("{runtime}:{thread}:{}", native_turn.unwrap_or(id)),
                )
            };
            if let Some(amount) = amount.filter(|n| n.is_finite() && *n >= 0.) {
                let record = costs.entry(key).or_insert(serde_json::json!({}));
                if record["amount_usd"]
                    .as_f64()
                    .is_none_or(|prior| amount >= prior)
                {
                    *record = serde_json::json!({"amount_usd":amount,"scope":scope,"basis":basis,"confirmed_billing":false});
                }
            }
        }
        let physical = details["physical_attempt_id"].as_str();
        let key = if let (Some(runtime), Some(thread), Some(turn)) =
            (usage["runtime"].as_str(), native_thread, native_turn)
        {
            if usage["scope"] == "request_snapshot" {
                format!(
                    "cli-message:{runtime}:{thread}:{}",
                    usage["native_message_id"].as_str().unwrap_or(id)
                )
            } else {
                format!("cli-turn:{runtime}:{thread}:{turn}")
            }
        } else {
            physical
                .map(|p| format!("physical:{p}"))
                .unwrap_or_else(|| format!("observation:{id}"))
        };
        let value =
            distinct
                .entry(key)
                .or_insert((None, None, false, physical.is_some(), None, true));
        let max = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        value.0 = max(value.0, details["usage"]["input_tokens"].as_u64());
        value.1 = max(value.1, details["usage"]["output_tokens"].as_u64());
        value.2 |= details["complete"].as_bool() == Some(true);
        value.5 &= usage["cache_ratio_consistent"] != false;
        value.4 = max(
            value.4,
            details["usage"]["cache_read_input_tokens"].as_u64(),
        );
    }
    let mut totals = ObservedProviderUsageTotals {
        cli_cost_observations: costs,
        unattributed_session_observations: session_evidence,
        ..Default::default()
    };
    for (input, output, complete, physical, cached, coherent) in distinct.values() {
        match (input, cached) {
            (Some(input), Some(cached)) if cached <= input && *coherent => {
                // If aggregation overflows, a measured ratio is unavailable.
                match (
                    totals.cache_ratio_input_tokens.checked_add(*input),
                    totals.known_cache_read_input_tokens.checked_add(*cached),
                ) {
                    (Some(input), Some(cached)) => {
                        totals.cache_ratio_input_tokens = input;
                        totals.known_cache_read_input_tokens = cached;
                    }
                    _ => totals.missing_cache_ratio_attempts += 1,
                }
            }
            _ => totals.missing_cache_ratio_attempts += 1,
        }
        totals.attempts += 1;
        if *physical {
            totals.confirmed_physical_attempts += 1;
        } else {
            totals.unconfirmed_dispatches += 1;
        }
        if !complete {
            totals.incomplete_attempts += 1;
        }
        match input {
            Some(count) => {
                totals.known_input_tokens = totals.known_input_tokens.saturating_add(*count)
            }
            None => totals.missing_input_attempts += 1,
        };
        match output {
            Some(count) => {
                totals.known_output_tokens = totals.known_output_tokens.saturating_add(*count)
            }
            None => totals.missing_output_attempts += 1,
        };
    }
    totals
}

#[cfg(test)]
mod usage_observation_tests {
    use super::*;
    fn item(id: &str, input: Option<u64>, output: Option<u64>, complete: bool) -> crate::TurnItem {
        crate::TurnItem::SystemEvent {
            id: id.into(),
            level: crate::SystemEventLevel::Info,
            message: "Provider usage observation".into(),
            code: Some("provider_usage".into()),
            details: Some(serde_json::json!({"schema_version":1,"complete":complete,
                "usage":{"input_tokens":input,"output_tokens":output}})),
        }
    }
    #[test]
    fn legacy_opencode_usage_and_weighted_cache_denominator_survive_replay() {
        let legacy = crate::TurnItem::SystemEvent {
            id: "native-turn:usage".into(),
            level: crate::SystemEventLevel::Info,
            message: "OpenCode usage".into(),
            code: Some("agent_runtime_item".into()),
            details: Some(serde_json::json!({
                "nativeItemKind":"tokenUsage", "status":"completed",
                "semantics":"opencode_v2_exclusive_cache_separate_reasoning",
                "input_tokens":100, "output_tokens":12, "cache_read_input_tokens":80
            })),
        };
        let mut small = item("codex-turn", Some(10), Some(2), false);
        if let crate::TurnItem::SystemEvent {
            details: Some(details),
            ..
        } = &mut small
        {
            details["usage"]["cache_read_input_tokens"] = serde_json::json!(0);
        }
        let totals = observed_provider_usage_totals([&legacy, &legacy, &small]);
        assert_eq!(totals.attempts, 2);
        assert_eq!(totals.confirmed_physical_attempts, 0);
        assert_eq!(totals.known_input_tokens, 110);
        assert_eq!(totals.known_output_tokens, 14);
        assert_eq!(totals.cache_hit_rate(), Some(80.0 / 110.0));
        let unknown = item("missing-cache", Some(1), None, false);
        assert_eq!(
            observed_provider_usage_totals([&legacy, &unknown]).cache_hit_rate(),
            None
        );
    }

    #[test]
    fn partial_snapshot_does_not_erase_known_zero_and_cache_ratio_rejects_overflow() {
        let first = item("turn", Some(0), Some(3), false);
        let later = item("turn", None, None, true);
        let totals = observed_provider_usage_totals([&first, &later]);
        assert_eq!(totals.attempts, 1);
        assert_eq!(totals.missing_input_attempts, 0);
        assert_eq!(totals.known_output_tokens, 3);
        assert_eq!(totals.incomplete_attempts, 0);
        let mut huge = item("huge", Some(u64::MAX), None, true);
        let mut extra = item("extra", Some(1), None, true);
        for item in [&mut huge, &mut extra] {
            if let crate::TurnItem::SystemEvent {
                details: Some(details),
                ..
            } = item
            {
                details["usage"]["cache_read_input_tokens"] = serde_json::json!(0);
            }
        }
        assert_eq!(
            observed_provider_usage_totals([&huge, &extra]).cache_hit_rate(),
            None
        );
    }

    #[test]
    fn claude_main_loop_aggregate_replaces_messages_but_never_adds_model_snapshots() {
        let make = |id: &str, scope: &str, turn: &str, input, output| {
            let mut item = item(id, input, output, true);
            if let crate::TurnItem::SystemEvent {
                details: Some(d), ..
            } = &mut item
            {
                d["usage"]["runtime"] = serde_json::json!("claude");
                d["usage"]["scope"] = serde_json::json!(scope);
                d["usage"]["native_thread_id"] = serde_json::json!("session");
                d["usage"]["native_turn_id"] = serde_json::json!(turn);
                d["usage"]["native_message_id"] = serde_json::json!("message");
                d["usage"]["cache_read_input_tokens"] = serde_json::json!(0);
                d["usage"]["model_usage"] = serde_json::json!({"model":{"inputTokens":99999}});
                if scope == "turn_snapshot" {
                    d["usage"]["total_cost_usd"] = serde_json::json!(0.25);
                }
            }
            item
        };
        let message = make(
            "message-snapshot",
            "request_snapshot",
            "t1",
            Some(10),
            Some(1),
        );
        let result = make("result", "turn_snapshot", "t1", Some(30), Some(5));
        let partial = make(
            "message-partial",
            "request_snapshot",
            "t2",
            Some(7),
            Some(2),
        );
        let missing = make("error-result", "turn_snapshot", "t2", None, None);
        let totals =
            observed_provider_usage_totals([&message, &result, &result, &partial, &missing]);
        assert_eq!(totals.known_input_tokens, 37);
        assert_eq!(totals.known_output_tokens, 7);
        assert_eq!(totals.cli_cost_observations.len(), 1);
        assert_eq!(
            totals.cli_cost_observations["claude:session"]["amount_usd"],
            0.25
        );
        assert_eq!(
            totals.cli_cost_observations["claude:session"]["confirmed_billing"],
            false
        );
        assert_eq!(totals.confirmed_physical_attempts, 0);
    }

    #[test]
    fn codex_session_snapshots_with_unknown_baseline_are_not_range_consumption() {
        let make = |id: &str, thread: &str, count: u64| {
            let mut item = item(id, Some(count), Some(0), false);
            if let crate::TurnItem::SystemEvent {
                details: Some(d), ..
            } = &mut item
            {
                d["usage"]["runtime"] = serde_json::json!("codex");
                d["usage"]["scope"] = serde_json::json!("session_snapshot");
                d["usage"]["native_thread_id"] = serde_json::json!(thread);
                d["usage"]["cache_read_input_tokens"] = serde_json::json!(0);
            }
            item
        };
        let before = make("before", "native", 100);
        let reset = make("reset", "native", 0);
        let after = make("after", "native", 20);
        let resumed = make("resumed-delivery", "native", 20);
        let recreated = make("recreated", "new-native", 5);
        let totals = observed_provider_usage_totals([
            &before, &reset, &after, &before, &resumed, &recreated,
        ]);
        assert_eq!(totals.known_input_tokens, 0);
        assert_eq!(totals.unattributed_session_observations.len(), 5);
        assert_eq!(totals.confirmed_physical_attempts, 0);
        assert_eq!(totals.cache_hit_rate(), None);
    }

    #[test]
    fn partial_claude_result_preserves_coherent_request_input_cache_pair() {
        let mut request = item("request", Some(100), Some(4), false);
        let mut result = item("result", None, Some(6), true);
        for (entry, scope) in [
            (&mut request, "request_snapshot"),
            (&mut result, "turn_snapshot"),
        ] {
            if let crate::TurnItem::SystemEvent {
                details: Some(d), ..
            } = entry
            {
                d["usage"]["runtime"] = serde_json::json!("claude");
                d["usage"]["scope"] = serde_json::json!(scope);
                d["usage"]["native_thread_id"] = serde_json::json!("session");
                d["usage"]["native_turn_id"] = serde_json::json!("turn");
                d["usage"]["native_message_id"] = serde_json::json!("message");
                d["usage"]["cache_read_input_tokens"] = serde_json::json!(80);
            }
        }
        let totals = observed_provider_usage_totals([&request, &result, &request]);
        assert_eq!(totals.attempts, 1);
        assert_eq!(totals.known_input_tokens, 100);
        assert_eq!(totals.known_output_tokens, 6);
        assert_eq!(totals.cache_hit_rate(), Some(0.8));
        assert_eq!(
            totals.incomplete_attempts, 1,
            "fallback is observed evidence, not a full turn assertion"
        );
        if let crate::TurnItem::SystemEvent {
            details: Some(d), ..
        } = &mut request
        {
            d["usage"]["cache_read_input_tokens"] = serde_json::json!(70);
        }
        let conflict = observed_provider_usage_totals([&request, &result]);
        assert_eq!(conflict.known_input_tokens, 100);
        assert_eq!(conflict.known_output_tokens, 6);
        assert_eq!(conflict.missing_input_attempts, 0);
        assert_eq!(conflict.incomplete_attempts, 1);
        assert_eq!(conflict.cache_hit_rate(), None);
        if let crate::TurnItem::SystemEvent {
            details: Some(d), ..
        } = &mut result
        {
            d["usage"]["num_turns"] = serde_json::json!(2);
        }
        let incomplete = observed_provider_usage_totals([&request, &result]);
        assert_eq!(incomplete.attempts, 1);
        assert_eq!(incomplete.missing_input_attempts, 1);
        assert_eq!(incomplete.cache_hit_rate(), None);
    }

    #[test]
    fn physical_attempt_identity_deduplicates_distinct_delivery_items() {
        let mut a = item("delivery-a", Some(10), Some(2), true);
        let mut b = item("delivery-b", Some(10), Some(2), true);
        for item in [&mut a, &mut b] {
            if let crate::TurnItem::SystemEvent {
                details: Some(details),
                ..
            } = item
            {
                details["physical_attempt_id"] = serde_json::json!("physical-1");
            }
        }
        let totals = observed_provider_usage_totals([&a, &b]);
        assert_eq!(totals.attempts, 1);
        assert_eq!(totals.confirmed_physical_attempts, 1);
        assert_eq!(totals.known_input_tokens, 10);
    }
    #[test]
    fn retries_partial_missing_zero_and_duplicate_snapshots_are_distinct() {
        let first = item("attempt-1", Some(10), Some(2), false);
        let second = item("attempt-2", Some(20), Some(0), true);
        let missing = item("attempt-3", None, None, false);
        let totals = observed_provider_usage_totals([&first, &second, &second, &missing]);
        assert_eq!(totals.attempts, 3);
        assert_eq!(totals.known_input_tokens, 30);
        assert_eq!(totals.known_output_tokens, 2);
        assert_eq!(totals.missing_output_attempts, 1);
        assert_eq!(totals.incomplete_attempts, 2);
        let encoded = serde_json::to_value(&second).unwrap();
        let decoded: crate::TurnItem = serde_json::from_value(encoded).unwrap();
        assert_eq!(observed_provider_usage_totals([&decoded]).attempts, 1);
    }
}

#[cfg(test)]
mod context_limit_tests {
    use super::ProviderContextTiers;

    #[test]
    fn bounded_endpoints_keep_multiple_tiers_and_reject_stale_or_unknown_expansion() {
        let tiers = ProviderContextTiers::Tiered {
            windows: vec![32000, 128000, 256000],
            price_order_confirmed: true,
        };
        assert_eq!(tiers.resolve(200000, None), Ok(32000));
        for window in [32000, 128000, 200000] {
            assert_eq!(tiers.resolve(200000, Some(window)), Ok(window));
        }
        assert!(tiers.resolve(200000, Some(160000)).is_err());
        assert!(tiers.resolve(200000, Some(256000)).is_err());
        assert_eq!(tiers.resolve(16000, None), Ok(16000));
        assert!(tiers.resolve(16000, Some(32000)).is_err());
        let unknown = ProviderContextTiers::Tiered {
            windows: vec![32000, 128000, 256000],
            price_order_confirmed: false,
        };
        assert!(unknown.resolve(200000, Some(200000)).is_err());
        assert_eq!(unknown.resolve(200000, None), Ok(32000));
        assert_eq!(
            ProviderContextTiers::NoTiers.resolve(800000, None),
            Ok(800000)
        );
        assert!(ProviderContextTiers::Invalid.resolve(800000, None).is_err());
    }
}
