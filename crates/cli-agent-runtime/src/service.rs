//! Bounded failure metadata shared by isolated CLI service adapters.
use pioneer_protocol::ProviderFailureClass;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceFailure {
    pub class: ProviderFailureClass,
    pub retry_after_ms: Option<u64>,
}
impl std::fmt::Display for ServiceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CLI service failure: {:?}", self.class)
    }
}
impl std::error::Error for ServiceFailure {}

/// Safe stage context survives anyhow propagation without persisting raw errors.
#[derive(Debug)]
pub struct ServiceStage(pub &'static str);
impl std::fmt::Display for ServiceStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ServiceStage {}

/// CLI-owned counters only; exclude transcripts, errors, native session state
/// and arbitrary identifiers from an otherwise much larger terminal result.
pub(crate) fn bounded_usage(value: &serde_json::Value) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    for key in [
        "input_tokens",
        "output_tokens",
        "cached_input_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
        "reasoning_tokens",
        "total_tokens",
    ] {
        if let Some(count) = value["usage"][key].as_u64() {
            result.insert(key.into(), count.into());
        }
    }
    if let Some(cost) = value["total_cost_usd"]
        .as_f64()
        .filter(|v| v.is_finite() && *v >= 0.)
    {
        result.insert("total_cost_usd".into(), serde_json::json!(cost));
    }
    serde_json::Value::Object(result)
}
#[cfg(test)]
mod usage_tests {
    #[test]
    fn cli_result_retains_only_reported_numeric_usage_and_usd_cost() {
        let raw = super::bounded_usage(
            &serde_json::json!({"result":"SECRET","session_id":"SECRET","usage":{"input_tokens":10,"cached_input_tokens":8,"output_tokens":0,"prompt":"SECRET"},"total_cost_usd":0.2}),
        );
        assert_eq!(raw["cached_input_tokens"], 8);
        assert_eq!(raw["output_tokens"], 0);
        assert_eq!(raw["total_cost_usd"], 0.2);
        assert!(!raw.to_string().contains("SECRET"));
        assert!(
            super::bounded_usage(&serde_json::json!({"usage":{}}))
                .as_object()
                .unwrap()
                .is_empty()
        );
    }
}

/// Sanitized numeric metadata attached to a terminal CLI error. It does not
/// change the existing failure classification or expose the terminal payload.
#[derive(Debug)]
pub struct ObservedServiceUsage(pub serde_json::Value);
impl std::fmt::Display for ObservedServiceUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CLI terminal usage available")
    }
}
impl std::error::Error for ObservedServiceUsage {}
