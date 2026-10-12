use crate::{ToolErrorClass, ToolOutcomeStatus, ToolRetryObservation};
use pioneer_protocol::{
    ExecutionCheckpointToolNoProgressExactVariant, ExecutionCheckpointToolNoProgressState,
    ExecutionCheckpointToolNoProgressStrategy,
};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::Path;

const CONTROL_ARGUMENT_KEYS: &[&str] = &["timeout_ms", "yield_time_ms", "max_output_tokens"];
// Keep this optional checkpoint section well below the 128 KiB authoritative payload ceiling.
// The remaining budget is reserved for the request, provider and tool summaries.
const CHECKPOINT_STATE_MAX_BYTES: usize = 48 * 1024;
const CHECKPOINT_MAX_STRATEGIES: usize = 16;
const CHECKPOINT_MAX_EXACT_VARIANTS_PER_STRATEGY: usize = 8;
const CHECKPOINT_MAX_FEATURES_PER_STRATEGY: usize = 32;
const CHECKPOINT_MAX_TOOL_NAME_CHARS: usize = 128;
const CHECKPOINT_MAX_EXECUTABLE_CHARS: usize = 256;
const CHECKPOINT_MAX_HASH_CHARS: usize = 64;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolNoProgressMode {
    Disabled,
    Observe,
    #[default]
    Enforce,
}

impl std::str::FromStr for ToolNoProgressMode {
    type Err = &'static str;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "disabled" => Ok(Self::Disabled),
            "observe" => Ok(Self::Observe),
            "enforce" => Ok(Self::Enforce),
            _ => Err("no-progress mode must be disabled, observe, or enforce"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolNoProgressGuardConfig {
    /// A future invocation with the same normalized arguments is rejected after this many
    /// timeouts. Execution controls such as `timeout_ms` are excluded from normalization.
    pub exact_timeout_limit: u32,
    /// Advisory only; a similar invocation never inherits a hard denial.
    pub structural_timeout_limit: u32,
    pub warning_timeout_count: u32,
    /// Jaccard similarity threshold in thousandths.
    pub structural_similarity_millis: u16,
    pub max_strategies: usize,
    pub max_exact_variants_per_strategy: usize,
    pub max_features_per_strategy: usize,
}

impl Default for ToolNoProgressGuardConfig {
    fn default() -> Self {
        Self {
            exact_timeout_limit: 2,
            structural_timeout_limit: 3,
            warning_timeout_count: 2,
            structural_similarity_millis: 720,
            max_strategies: CHECKPOINT_MAX_STRATEGIES,
            max_exact_variants_per_strategy: CHECKPOINT_MAX_EXACT_VARIANTS_PER_STRATEGY,
            max_features_per_strategy: CHECKPOINT_MAX_FEATURES_PER_STRATEGY,
        }
    }
}

impl ToolNoProgressGuardConfig {
    fn normalized(&self) -> Self {
        Self {
            exact_timeout_limit: self.exact_timeout_limit.max(1),
            structural_timeout_limit: self.structural_timeout_limit.max(2),
            warning_timeout_count: self.warning_timeout_count.max(1),
            structural_similarity_millis: self.structural_similarity_millis.clamp(1, 1_000),
            max_strategies: self.max_strategies.clamp(1, CHECKPOINT_MAX_STRATEGIES),
            max_exact_variants_per_strategy: self
                .max_exact_variants_per_strategy
                .clamp(1, CHECKPOINT_MAX_EXACT_VARIANTS_PER_STRATEGY),
            max_features_per_strategy: self
                .max_features_per_strategy
                .clamp(8, CHECKPOINT_MAX_FEATURES_PER_STRATEGY),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolNoProgressPreflightDecision {
    Allow,
    Block {
        strategy_id: String,
        message: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolNoProgressFeedback {
    pub fact_lines: Vec<String>,
}

impl ToolNoProgressFeedback {
    pub fn is_empty(&self) -> bool {
        self.fact_lines.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct ToolNoProgressGuard {
    config: ToolNoProgressGuardConfig,
    mode: ToolNoProgressMode,
    state: ExecutionCheckpointToolNoProgressState,
}

impl Default for ToolNoProgressGuard {
    fn default() -> Self {
        Self::new(ToolNoProgressGuardConfig::default())
    }
}

impl ToolNoProgressGuard {
    pub fn exact_strategy_identity(tool_name: &str, arguments: &str) -> String {
        StrategyFingerprint::new(tool_name, arguments, 1).exact
    }

    pub fn new(config: ToolNoProgressGuardConfig) -> Self {
        Self::new_with_mode(config, ToolNoProgressMode::Enforce)
    }

    pub fn new_with_mode(config: ToolNoProgressGuardConfig, mode: ToolNoProgressMode) -> Self {
        Self {
            config: config.normalized(),
            mode,
            state: ExecutionCheckpointToolNoProgressState::default(),
        }
    }

    pub fn from_checkpoint(
        config: ToolNoProgressGuardConfig,
        state: ExecutionCheckpointToolNoProgressState,
    ) -> Self {
        Self::from_checkpoint_with_mode(config, state, ToolNoProgressMode::Enforce)
    }

    pub fn from_checkpoint_with_mode(
        config: ToolNoProgressGuardConfig,
        state: ExecutionCheckpointToolNoProgressState,
        mode: ToolNoProgressMode,
    ) -> Self {
        let mut guard = Self {
            config: config.normalized(),
            mode,
            state,
        };
        guard.bound_state();
        guard
    }

    pub fn checkpoint_state(&self) -> ExecutionCheckpointToolNoProgressState {
        self.state.clone()
    }

    pub fn preflight(&self, tool_name: &str, arguments: &str) -> ToolNoProgressPreflightDecision {
        if self.mode != ToolNoProgressMode::Enforce {
            return ToolNoProgressPreflightDecision::Allow;
        }
        let candidate =
            StrategyFingerprint::new(tool_name, arguments, self.config.max_features_per_strategy);
        let Some(index) = self.best_matching_strategy(&candidate) else {
            return ToolNoProgressPreflightDecision::Allow;
        };
        let strategy = &self.state.strategies[index];
        let exact_exhausted = strategy.exact_variants.iter().any(|variant| {
            variant.arguments_fingerprint == candidate.exact
                && variant.timeout_count >= self.config.exact_timeout_limit
        });
        if !exact_exhausted {
            return ToolNoProgressPreflightDecision::Allow;
        }

        let reason = "the same exact invocation exhausted its confirmed no-progress allowance";
        let message = serde_json::json!({
            "error": "repeated no-progress strategy blocked",
            "code": "tool_strategy_no_progress",
            "strategyId": strategy.strategy_id,
            "toolName": tool_name,
            "reason": reason,
            "timeoutCount": strategy.timeout_count,
            "cumulativeTimeoutMs": strategy.cumulative_timeout_ms,
            "guidance": "Change this exact strategy. A materially different resource, executable, interaction mode or argument remains eligible under the Turn budgets."
        })
        .to_string();
        ToolNoProgressPreflightDecision::Block {
            strategy_id: strategy.strategy_id.clone(),
            message,
        }
    }

    pub fn observe(&mut self, observations: &[ToolRetryObservation]) -> ToolNoProgressFeedback {
        let mut feedback = ToolNoProgressFeedback::default();
        if self.mode == ToolNoProgressMode::Disabled {
            return feedback;
        }
        for observation in observations {
            let fingerprint = StrategyFingerprint::new(
                observation.tool_name.as_str(),
                observation.arguments.as_str(),
                self.config.max_features_per_strategy,
            );
            if self.best_matching_strategy(&fingerprint).is_none()
                && self.state.strategies.iter().any(|strategy| {
                    strategy.tool_name == fingerprint.tool_name
                        && jaccard_millis(&strategy.structural_features, &fingerprint.features)
                            >= self.config.structural_similarity_millis
                })
            {
                feedback.fact_lines.push(format!(
                    "tool={} status=similar_strategy_advisory hard_denial=false",
                    observation.tool_name
                ));
            }
            if observation.success
                || matches!(
                    observation.outcome.status,
                    ToolOutcomeStatus::Ok | ToolOutcomeStatus::PartialSuccess
                )
            {
                if let Some(index) = self.best_matching_strategy(&fingerprint) {
                    self.state.strategies.remove(index);
                }
                continue;
            }
            if observation.outcome.error_class != Some(ToolErrorClass::Timeout) {
                continue;
            }

            // A hard timeout is not automatically evidence of zero progress. Some long-running
            // commands can produce useful bounded output before their caller-selected deadline.
            // This is deliberately structural: no language, keyword, or semantic interpretation
            // is used. A non-empty stdout is treated as observable progress and clears stale
            // no-progress evidence for the matching strategy.
            if observation_has_stdout_progress(observation) {
                if let Some(index) = self.best_matching_strategy(&fingerprint) {
                    self.state.strategies.remove(index);
                }
                feedback.fact_lines.push(format!(
                    "tool={} status=progress timeout_not_counted=true guidance=use_the_observed_output_or_change_execution_mode_if_more_time_is_needed",
                    observation.tool_name,
                ));
                continue;
            }

            let index = self
                .best_matching_strategy(&fingerprint)
                .unwrap_or_else(|| self.insert_strategy(&fingerprint));
            let strategy = &mut self.state.strategies[index];
            strategy.timeout_count = strategy.timeout_count.saturating_add(1);
            strategy.cumulative_timeout_ms = strategy
                .cumulative_timeout_ms
                .saturating_add(observed_timeout_ms(observation));

            let exact_timeout_count = match strategy
                .exact_variants
                .iter_mut()
                .find(|variant| variant.arguments_fingerprint == fingerprint.exact)
            {
                Some(variant) => {
                    variant.timeout_count = variant.timeout_count.saturating_add(1);
                    variant.timeout_count
                }
                None => {
                    if strategy.exact_variants.len() >= self.config.max_exact_variants_per_strategy
                    {
                        strategy.exact_variants.remove(0);
                        self.state.truncated = true;
                    }
                    strategy
                        .exact_variants
                        .push(ExecutionCheckpointToolNoProgressExactVariant {
                            arguments_fingerprint: fingerprint.exact.clone(),
                            timeout_count: 1,
                        });
                    1
                }
            };

            let exact_exhausted = exact_timeout_count >= self.config.exact_timeout_limit;
            // Accumulated time is diagnostic only. Absolute wall-clock thresholds cannot
            // distinguish a stuck command from a legitimately long build or test suite.
            let structural_exhausted =
                strategy.timeout_count >= self.config.structural_timeout_limit;
            strategy.exhausted |= structural_exhausted;

            if exact_exhausted || structural_exhausted {
                feedback.fact_lines.push(format!(
                    "strategy_id={} status=exhausted tool={} timeout_count={} cumulative_timeout_ms={} guidance=change_the_exact_strategy_materially",
                    strategy.strategy_id,
                    strategy.tool_name,
                    strategy.timeout_count,
                    strategy.cumulative_timeout_ms,
                ));
            } else if strategy.timeout_count >= self.config.warning_timeout_count
                && !strategy.warning_emitted
            {
                strategy.warning_emitted = true;
                feedback.fact_lines.push(format!(
                    "strategy_id={} status=warning tool={} timeout_count={} cumulative_timeout_ms={} guidance=another_confirmed_stall_of_this_exact_invocation_will_exhaust_it",
                    strategy.strategy_id,
                    strategy.tool_name,
                    strategy.timeout_count,
                    strategy.cumulative_timeout_ms,
                ));
            }
        }
        self.bound_state();
        feedback
    }

    fn insert_strategy(&mut self, fingerprint: &StrategyFingerprint) -> usize {
        if self.state.strategies.len() >= self.config.max_strategies {
            self.state.strategies.remove(0);
            self.state.truncated = true;
        }
        let index = self.state.strategies.len();
        self.state
            .strategies
            .push(ExecutionCheckpointToolNoProgressStrategy {
                strategy_id: fingerprint.exact[..16.min(fingerprint.exact.len())].to_owned(),
                tool_name: fingerprint.tool_name.clone(),
                executable: fingerprint.executable.clone(),
                structural_fingerprint: fingerprint.structural.clone(),
                structural_features: fingerprint.features.clone(),
                exact_variants: Vec::new(),
                timeout_count: 0,
                cumulative_timeout_ms: 0,
                warning_emitted: false,
                exhausted: false,
            });
        index
    }

    fn best_matching_strategy(&self, candidate: &StrategyFingerprint) -> Option<usize> {
        self.state
            .strategies
            .iter()
            .enumerate()
            .filter(|(_, strategy)| {
                strategy.tool_name == candidate.tool_name
                    && strategy.executable == candidate.executable
            })
            .filter_map(|(index, strategy)| {
                if strategy
                    .exact_variants
                    .iter()
                    .any(|variant| variant.arguments_fingerprint == candidate.exact)
                {
                    return Some((index, 1_000u16));
                }
                None
            })
            .max_by_key(|(_, similarity)| *similarity)
            .map(|(index, _)| index)
    }

    fn bound_state(&mut self) {
        if self.state.strategies.len() > self.config.max_strategies {
            let remove = self
                .state
                .strategies
                .len()
                .saturating_sub(self.config.max_strategies);
            self.state.strategies.drain(0..remove);
            self.state.truncated = true;
        }
        for strategy in &mut self.state.strategies {
            self.state.truncated |=
                truncate_chars(&mut strategy.strategy_id, CHECKPOINT_MAX_HASH_CHARS);
            self.state.truncated |=
                truncate_chars(&mut strategy.tool_name, CHECKPOINT_MAX_TOOL_NAME_CHARS);
            if let Some(executable) = &mut strategy.executable {
                self.state.truncated |= truncate_chars(executable, CHECKPOINT_MAX_EXECUTABLE_CHARS);
            }
            self.state.truncated |= truncate_chars(
                &mut strategy.structural_fingerprint,
                CHECKPOINT_MAX_HASH_CHARS,
            );
            for feature in &mut strategy.structural_features {
                self.state.truncated |= truncate_chars(feature, CHECKPOINT_MAX_HASH_CHARS);
            }
            for variant in &mut strategy.exact_variants {
                self.state.truncated |= truncate_chars(
                    &mut variant.arguments_fingerprint,
                    CHECKPOINT_MAX_HASH_CHARS,
                );
            }
            if strategy.structural_features.len() > self.config.max_features_per_strategy {
                strategy
                    .structural_features
                    .truncate(self.config.max_features_per_strategy);
                self.state.truncated = true;
            }
            if strategy.exact_variants.len() > self.config.max_exact_variants_per_strategy {
                let remove = strategy
                    .exact_variants
                    .len()
                    .saturating_sub(self.config.max_exact_variants_per_strategy);
                strategy.exact_variants.drain(0..remove);
                self.state.truncated = true;
            }
        }
        while serialized_checkpoint_state_len(&self.state) > CHECKPOINT_STATE_MAX_BYTES
            && !self.state.strategies.is_empty()
        {
            self.state.strategies.remove(0);
            self.state.truncated = true;
        }
    }
}

fn truncate_chars(value: &mut String, max_chars: usize) -> bool {
    let Some((byte_index, _)) = value.char_indices().nth(max_chars) else {
        return false;
    };
    value.truncate(byte_index);
    true
}

fn serialized_checkpoint_state_len(state: &ExecutionCheckpointToolNoProgressState) -> usize {
    serde_json::to_vec(state)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX)
}

#[derive(Debug, Clone)]
struct StrategyFingerprint {
    tool_name: String,
    executable: Option<String>,
    exact: String,
    structural: String,
    features: Vec<String>,
}

impl StrategyFingerprint {
    fn new(tool_name: &str, arguments: &str, max_features: usize) -> Self {
        let parsed = serde_json::from_str::<JsonValue>(arguments)
            .unwrap_or_else(|_| JsonValue::String(arguments.to_owned()));
        let mut normalized = canonical_exact_arguments(&parsed);
        // Only the documented root execution controls of these builtins are
        // nonsemantic. Dynamic/nested keys retain their original values.
        if matches!(tool_name, "exec_command" | "write_stdin")
            && let Some(map) = normalized.as_object_mut()
        {
            for key in CONTROL_ARGUMENT_KEYS {
                map.remove(*key);
            }
        }
        let exact = hex::encode(Sha256::digest(
            serde_json::json!([tool_name, normalized])
                .to_string()
                .as_bytes(),
        ));
        let mut bounded_tool_name = tool_name.to_owned();
        truncate_chars(&mut bounded_tool_name, CHECKPOINT_MAX_TOOL_NAME_CHARS);
        let executable = executable_identity(tool_name, &parsed);
        let mut raw_features = BTreeSet::new();
        raw_features.insert(format!("tool:{bounded_tool_name}"));
        if let Some(executable) = executable.as_deref() {
            raw_features.insert(format!("executable:{executable}"));
        }
        collect_structural_features("$", &normalized, &mut raw_features);
        let features = raw_features
            .into_iter()
            .map(|feature| short_hash(feature.as_bytes()))
            .take(max_features)
            .collect::<Vec<_>>();
        let structural = short_hash(features.join("|").as_bytes());
        Self {
            tool_name: bounded_tool_name,
            executable,
            exact,
            structural,
            features,
        }
    }
}

fn canonical_exact_arguments(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            JsonValue::Object(
                keys.into_iter()
                    .map(|key| (key.clone(), canonical_exact_arguments(&map[key])))
                    .collect(),
            )
        }
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(canonical_exact_arguments).collect())
        }
        value => value.clone(),
    }
}

fn executable_identity(tool_name: &str, arguments: &JsonValue) -> Option<String> {
    if tool_name != "exec_command" {
        return None;
    }
    arguments
        .get("command")
        .and_then(JsonValue::as_array)
        .and_then(|command| command.first())
        .and_then(JsonValue::as_str)
        .map(|value| {
            let mut executable = Path::new(value)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(value)
                .to_lowercase();
            truncate_chars(&mut executable, CHECKPOINT_MAX_EXECUTABLE_CHARS);
            executable
        })
}

fn collect_structural_features(path: &str, value: &JsonValue, output: &mut BTreeSet<String>) {
    match value {
        JsonValue::Null => {
            output.insert(format!("{path}:null"));
        }
        JsonValue::Bool(value) => {
            output.insert(format!("{path}:bool:{value}"));
        }
        JsonValue::Number(_) => {
            output.insert(format!("{path}:number"));
        }
        JsonValue::String(value) => collect_string_features(path, value, output),
        JsonValue::Array(items) => {
            output.insert(format!(
                "{path}:array:len_bucket={}",
                length_bucket(items.len())
            ));
            for (index, item) in items.iter().enumerate() {
                collect_structural_features(format!("{path}[{index}]").as_str(), item, output);
            }
        }
        JsonValue::Object(map) => {
            output.insert(format!("{path}:object"));
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                collect_structural_features(format!("{path}.{key}").as_str(), &map[key], output);
            }
        }
    }
}

fn collect_string_features(path: &str, value: &str, output: &mut BTreeSet<String>) {
    output.insert(format!(
        "{path}:string:len_bucket={}",
        length_bucket(value.chars().count())
    ));

    let mut token = String::new();
    let mut token_index = 0usize;
    let mut shape = String::new();
    let mut prior_shape = None;
    for character in value.chars() {
        if character.is_alphanumeric() {
            token.push(character);
            let class = if character.is_numeric() { 'N' } else { 'W' };
            if prior_shape != Some(class) {
                shape.push(class);
                prior_shape = Some(class);
            }
        } else {
            push_token_feature(path, token_index, token.as_str(), output);
            if !token.is_empty() {
                token_index = token_index.saturating_add(1);
                token.clear();
            }
            if !character.is_whitespace() {
                shape.push(character);
                prior_shape = None;
            }
        }
    }
    push_token_feature(path, token_index, token.as_str(), output);
    output.insert(format!("{path}:shape:{shape}"));
}

fn push_token_feature(path: &str, index: usize, token: &str, output: &mut BTreeSet<String>) {
    if token.is_empty() {
        return;
    }
    let normalized = if token.chars().all(char::is_numeric) {
        "$number".to_owned()
    } else {
        token.to_lowercase()
    };
    output.insert(format!("{path}:token:{index}:{normalized}"));
}

fn length_bucket(length: usize) -> usize {
    match length {
        0..=3 => length,
        4..=7 => 4,
        8..=15 => 8,
        16..=31 => 16,
        32..=63 => 32,
        64..=127 => 64,
        _ => 128,
    }
}

fn observed_timeout_ms(observation: &ToolRetryObservation) -> u64 {
    serde_json::from_str::<JsonValue>(observation.model_visible_text.as_str())
        .ok()
        .as_ref()
        .and_then(find_duration_ms)
        .or_else(|| {
            serde_json::from_str::<JsonValue>(observation.arguments.as_str())
                .ok()
                .and_then(|arguments| arguments.get("timeout_ms").and_then(JsonValue::as_u64))
        })
        .unwrap_or(60_000)
}

fn observation_has_stdout_progress(observation: &ToolRetryObservation) -> bool {
    serde_json::from_str::<JsonValue>(observation.model_visible_text.as_str())
        .ok()
        .is_some_and(|value| find_non_empty_string_field(&value, "stdout"))
}

fn find_non_empty_string_field(value: &JsonValue, field_name: &str) -> bool {
    match value {
        JsonValue::Object(map) => {
            map.get(field_name)
                .and_then(JsonValue::as_str)
                .is_some_and(|value| !value.trim().is_empty())
                || map
                    .values()
                    .any(|value| find_non_empty_string_field(value, field_name))
        }
        JsonValue::Array(items) => items
            .iter()
            .any(|value| find_non_empty_string_field(value, field_name)),
        _ => false,
    }
}

fn find_duration_ms(value: &JsonValue) -> Option<u64> {
    match value {
        JsonValue::Object(map) => map
            .get("duration_ms")
            .or_else(|| map.get("durationMs"))
            .and_then(JsonValue::as_u64)
            .or_else(|| map.values().find_map(find_duration_ms)),
        JsonValue::Array(items) => items.iter().find_map(find_duration_ms),
        _ => None,
    }
}

fn short_hash(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    hex::encode(&digest[..16])
}

fn jaccard_millis(left: &[String], right: &[String]) -> u16 {
    if left.is_empty() && right.is_empty() {
        return 1_000;
    }
    let left = left.iter().collect::<BTreeSet<_>>();
    let right = right.iter().collect::<BTreeSet<_>>();
    let intersection = left.intersection(&right).count();
    let union = left.union(&right).count();
    if union == 0 {
        0
    } else {
        u16::try_from(intersection.saturating_mul(1_000) / union).unwrap_or(1_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ToolOutcome;

    fn timeout_observation(arguments: &str) -> ToolRetryObservation {
        timeout_observation_with_output(arguments, 30_000, "")
    }

    fn timeout_observation_with_output(
        arguments: &str,
        duration_ms: u64,
        stdout: &str,
    ) -> ToolRetryObservation {
        let mut observation = ToolRetryObservation::from_tool_outcome(
            "item",
            "command_execution",
            1,
            "exec_command",
            arguments,
            false,
            ToolOutcome::recoverable(ToolErrorClass::Timeout, "timed out", false, None),
        );
        observation.model_visible_text = serde_json::json!({
            "duration_ms": duration_ms,
            "timed_out": true,
            "stdout": stdout,
            "stderr": ""
        })
        .to_string();
        observation
    }

    #[test]
    fn timeout_control_values_do_not_create_a_new_exact_strategy() {
        let first = StrategyFingerprint::new(
            "exec_command",
            r#"{"command":["tool","--date","2026-08-01"],"timeout_ms":10000}"#,
            128,
        );
        let second = StrategyFingerprint::new(
            "exec_command",
            r#"{"command":["tool","--date","2026-08-01"],"timeout_ms":120000}"#,
            128,
        );
        assert_eq!(first.exact, second.exact);
    }

    #[test]
    fn unicode_text_is_structurally_compared_without_language_rules() {
        let first = StrategyFingerprint::new(
            "exec_command",
            r#"{"command":["runner","--query","календарь 2026-08-01"]}"#,
            128,
        );
        let second = StrategyFingerprint::new(
            "exec_command",
            r#"{"command":["runner","--query","日历 2026-08-02"]}"#,
            128,
        );
        assert_eq!(first.executable, second.executable);
        assert!(
            jaccard_millis(&first.features, &second.features)
                >= ToolNoProgressGuardConfig::default().structural_similarity_millis
        );
    }

    #[test]
    fn exact_strategy_is_blocked_after_two_timeouts_even_when_timeout_is_increased() {
        let mut guard = ToolNoProgressGuard::default();
        let first = timeout_observation(
            r#"{"command":["runner","--date","2026-08-01"],"timeout_ms":10000}"#,
        );
        let second = timeout_observation(
            r#"{"command":["runner","--date","2026-08-01"],"timeout_ms":120000}"#,
        );
        guard.observe(&[first]);
        guard.observe(&[second]);
        assert!(matches!(
            guard.preflight(
                "exec_command",
                r#"{"command":["runner","--date","2026-08-01"],"timeout_ms":300000}"#
            ),
            ToolNoProgressPreflightDecision::Block { .. }
        ));
    }

    #[test]
    fn structurally_similar_resources_never_inherit_another_exact_denial() {
        let mut guard = ToolNoProgressGuard::default();
        for date in ["2026-08-01", "2026-08-02", "2026-08-03"] {
            let arguments = format!(
                r#"{{"command":["runner","--date","{date}","--format","json"],"timeout_ms":30000}}"#
            );
            guard.observe(&[timeout_observation(arguments.as_str())]);
        }
        assert!(matches!(
            guard.preflight(
                "exec_command",
                r#"{"command":["runner","--date","2026-08-04","--format","json"]}"#
            ),
            ToolNoProgressPreflightDecision::Allow
        ));
        assert_eq!(
            guard.preflight(
                "exec_command",
                r#"{"command":["different-runner","--date","2026-08-04","--format","json"]}"#
            ),
            ToolNoProgressPreflightDecision::Allow
        );
    }

    #[test]
    fn a_different_structurally_similar_resource_receives_advisory_only() {
        let mut guard = ToolNoProgressGuard::default();
        let first = timeout_observation(
            r#"{"command":["runner","--date","2026-08-01"],"timeout_ms":30000}"#,
        );
        let second = timeout_observation(
            r#"{"command":["runner","--date","2026-08-02"],"timeout_ms":30000}"#,
        );
        assert!(guard.observe(&[first]).is_empty());
        let feedback = guard.observe(&[second]);
        assert_eq!(feedback.fact_lines.len(), 1);
        assert!(feedback.fact_lines[0].contains("status=similar_strategy_advisory"));
        assert_eq!(
            guard.preflight(
                "exec_command",
                r#"{"command":["runner","--date","2026-08-03"]}"#
            ),
            ToolNoProgressPreflightDecision::Allow
        );
    }

    #[test]
    fn cumulative_duration_is_diagnostic_and_never_exhausts_a_strategy_by_itself() {
        let mut guard = ToolNoProgressGuard::default();
        for date in ["2026-08-01", "2026-08-02"] {
            let arguments =
                format!(r#"{{"command":["runner","--date","{date}"],"timeout_ms":3600000}}"#);
            guard.observe(&[timeout_observation_with_output(
                arguments.as_str(),
                3_600_000,
                "",
            )]);
        }

        let state = guard.checkpoint_state();
        assert_eq!(state.strategies.len(), 2);
        assert_eq!(
            state
                .strategies
                .iter()
                .map(|entry| entry.cumulative_timeout_ms)
                .sum::<u64>(),
            7_200_000
        );
        assert!(!state.strategies[0].exhausted);
        assert!(matches!(
            guard.preflight(
                "exec_command",
                r#"{"command":["runner","--date","2026-08-03"],"timeout_ms":3600000}"#
            ),
            ToolNoProgressPreflightDecision::Allow
        ));
    }

    #[test]
    fn timeout_with_stdout_is_progress_and_does_not_accumulate_no_progress_evidence() {
        let mut guard = ToolNoProgressGuard::default();
        let arguments = r#"{"command":["runner","test"],"timeout_ms":3600000}"#;
        let feedback = guard.observe(&[timeout_observation_with_output(
            arguments,
            3_600_000,
            "completed 812 of 1000 tests",
        )]);

        assert!(feedback.fact_lines[0].contains("status=progress"));
        assert!(guard.checkpoint_state().is_empty());
        assert_eq!(
            guard.preflight("exec_command", arguments),
            ToolNoProgressPreflightDecision::Allow
        );
    }

    #[test]
    fn a_successful_exact_call_resets_only_its_own_evidence() {
        let mut guard = ToolNoProgressGuard::default();
        let timed_out = timeout_observation(
            r#"{"command":["runner","--date","2026-08-01"],"timeout_ms":30000}"#,
        );
        guard.observe(&[timed_out.clone()]);
        guard.observe(&[timed_out]);

        let successful = ToolRetryObservation::from_tool_outcome(
            "success",
            "command_execution",
            1,
            "exec_command",
            r#"{"command":["runner","--date","2026-08-01"]}"#,
            true,
            ToolOutcome::ok(),
        );
        guard.observe(&[successful]);
        assert_eq!(
            guard.preflight(
                "exec_command",
                r#"{"command":["runner","--date","2026-08-01"]}"#
            ),
            ToolNoProgressPreflightDecision::Allow
        );
        assert!(guard.checkpoint_state().is_empty());
    }

    #[test]
    fn checkpoint_round_trip_preserves_exhausted_strategy() {
        let mut guard = ToolNoProgressGuard::default();
        let observation = timeout_observation(
            r#"{"command":["runner","--date","2026-08-01"],"timeout_ms":30000}"#,
        );
        guard.observe(&[observation.clone()]);
        guard.observe(&[observation]);
        let checkpoint_state = guard.checkpoint_state();
        let encoded = serde_json::to_vec(&checkpoint_state).expect("serialize checkpoint state");
        let checkpoint_state = serde_json::from_slice(&encoded).expect("restore checkpoint state");
        let restored = ToolNoProgressGuard::from_checkpoint(
            ToolNoProgressGuardConfig::default(),
            checkpoint_state,
        );
        assert!(matches!(
            restored.preflight(
                "exec_command",
                r#"{"command":["runner","--date","2026-08-01"]}"#
            ),
            ToolNoProgressPreflightDecision::Block { .. }
        ));
    }

    #[test]
    fn modes_preserve_exact_state_but_only_enforce_denies() {
        let args = r#"{"command":["/original/runner","same-resource"]}"#;
        for mode in [
            ToolNoProgressMode::Disabled,
            ToolNoProgressMode::Observe,
            ToolNoProgressMode::Enforce,
        ] {
            let mut guard =
                ToolNoProgressGuard::new_with_mode(ToolNoProgressGuardConfig::default(), mode);
            let observation = timeout_observation(args);
            guard.observe(&[observation.clone(), observation]);
            let saved = guard.checkpoint_state();
            assert_eq!(saved.is_empty(), mode == ToolNoProgressMode::Disabled);
            let restored = ToolNoProgressGuard::from_checkpoint_with_mode(
                ToolNoProgressGuardConfig::default(),
                saved,
                mode,
            );
            assert_eq!(
                matches!(
                    restored.preflight("exec_command", args),
                    ToolNoProgressPreflightDecision::Block { .. }
                ),
                mode == ToolNoProgressMode::Enforce
            );
            assert_eq!(
                restored.preflight(
                    "exec_command",
                    r#"{"command":["/other/runner","same-resource"]}"#
                ),
                ToolNoProgressPreflightDecision::Allow
            );
        }
        assert!("future".parse::<ToolNoProgressMode>().is_err());
    }

    #[test]
    fn exact_identity_retains_nested_controls_dynamic_literals_and_full_canonical_name() {
        let identity = |tool: &str, args: &str| StrategyFingerprint::new(tool, args, 32).exact;
        assert_ne!(
            identity("dynamic", r#"{"timeout_ms":1}"#),
            identity("dynamic", r#"{"timeout_ms":2}"#)
        );
        assert_ne!(
            identity("exec_command", r#"{"nested":{"timeout_ms":1}}"#),
            identity("exec_command", r#"{"nested":{"timeout_ms":2}}"#)
        );
        let prefix = "namespace".repeat(32);
        assert_ne!(
            identity(&format!("{prefix}one"), "{}"),
            identity(&format!("{prefix}two"), "{}")
        );
        assert_ne!(
            identity("read_file", r#"{"path":"данные/一"}"#),
            identity("read_file", r#"{"path":"данные/二"}"#)
        );
        assert_eq!(
            identity("read_file", r#"{"path":"a","start_line":1}"#),
            identity("read_file", r#"{"start_line":1,"path":"a"}"#)
        );
    }
    #[test]
    fn maximum_checkpoint_state_is_deterministically_compacted_below_its_wire_budget() {
        let oversized = ExecutionCheckpointToolNoProgressState {
            strategies: (0..64)
                .map(|index| ExecutionCheckpointToolNoProgressStrategy {
                    strategy_id: format!("strategy-{index}-{}", "s".repeat(512)),
                    tool_name: format!("tool-{index}-{}", "t".repeat(512)),
                    executable: Some(format!("runner-{index}-{}", "e".repeat(1_024))),
                    structural_fingerprint: "f".repeat(512),
                    structural_features: (0..256)
                        .map(|feature| format!("feature-{feature}-{}", "x".repeat(256)))
                        .collect(),
                    exact_variants: (0..32)
                        .map(|variant| ExecutionCheckpointToolNoProgressExactVariant {
                            arguments_fingerprint: format!("variant-{variant}-{}", "v".repeat(256)),
                            timeout_count: u32::MAX,
                        })
                        .collect(),
                    timeout_count: u32::MAX,
                    cumulative_timeout_ms: u64::MAX,
                    warning_emitted: true,
                    exhausted: true,
                })
                .collect(),
            truncated: false,
        };
        let guard = ToolNoProgressGuard::from_checkpoint(
            ToolNoProgressGuardConfig {
                max_strategies: usize::MAX,
                max_exact_variants_per_strategy: usize::MAX,
                max_features_per_strategy: usize::MAX,
                ..ToolNoProgressGuardConfig::default()
            },
            oversized,
        );
        let compacted = guard.checkpoint_state();

        assert!(compacted.truncated);
        assert!(compacted.strategies.len() <= CHECKPOINT_MAX_STRATEGIES);
        assert!(compacted.strategies.iter().all(|strategy| {
            strategy.structural_features.len() <= CHECKPOINT_MAX_FEATURES_PER_STRATEGY
                && strategy.exact_variants.len() <= CHECKPOINT_MAX_EXACT_VARIANTS_PER_STRATEGY
        }));
        assert!(serialized_checkpoint_state_len(&compacted) <= CHECKPOINT_STATE_MAX_BYTES);
        assert!(
            compacted
                .strategies
                .last()
                .is_some_and(|strategy| strategy.tool_name.starts_with("tool-63-")),
            "compaction must retain the newest evidence"
        );
    }
}
