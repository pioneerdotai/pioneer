use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelBudget {
    pub context: u64,
    pub input_limit: Option<u64>,
    pub max_output: Option<u64>,
    /// Only APIs whose reasoning is outside their output cap add this reserve.
    pub separate_reasoning: u64,
}

impl ModelBudget {
    pub fn new(context: Option<u64>, input_limit: Option<u64>, max_output: Option<u64>) -> Self {
        Self {
            context: context.filter(|v| *v > 0).unwrap_or(128_000),
            input_limit,
            max_output,
            separate_reasoning: 0,
        }
    }

    pub fn output_reserve(&self, explicit: Option<u64>) -> anyhow::Result<u64> {
        if let Some(cap) = explicit {
            anyhow::ensure!(
                cap > 0 && self.max_output.is_none_or(|max| cap <= max),
                "explicit output limit is not supported by the selected model"
            );
            return Ok(cap);
        }
        let cap = 16_384
            .min(self.context / 4)
            .min(self.max_output.unwrap_or(u64::MAX));
        anyhow::ensure!(cap > 0, "model has no output capacity");
        Ok(cap)
    }

    pub fn summarizer_cap(&self, available: u64) -> anyhow::Result<u64> {
        Ok((self.output_reserve(None)? * 4 / 5)
            .min(13_107)
            .min(available))
    }

    pub fn fits(&self, input: u64, reserve: u64, recovery: bool) -> bool {
        let input = padded_input(input);
        let window = if recovery {
            (self.context as u128 * 9 / 10) as u64
        } else {
            self.context
        };
        self.input_limit.is_none_or(|limit| input <= limit)
            && input as u128 + reserve as u128 + self.separate_reasoning as u128 <= window as u128
    }
}

pub fn padded_input(input: u64) -> u64 {
    ((input as u128 * 105).div_ceil(100)).min(u64::MAX as u128) as u64
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestIdentity {
    pub provider_instance: String,
    pub model: String,
    pub api_format: String,
    pub instructions_hash: String,
    pub tools_hash: String,
    pub projection_version: u64,
    pub checkpoint: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InputMeasurement {
    pub identity: RequestIdentity,
    pub prefix: Vec<crate::SourceRef>,
    pub input_tokens: u64,
}

/// Sources must be the exact serialized prefix, not just the same message count.
pub fn estimate_input(
    local_complete: u64,
    identity: &RequestIdentity,
    sources: &[crate::SourceRef],
    measured: Option<&InputMeasurement>,
    appended_local: u64,
) -> u64 {
    measured
        .filter(|m| m.identity == *identity && sources.starts_with(&m.prefix))
        .map_or(local_complete, |m| {
            local_complete.max(m.input_tokens.saturating_add(appended_local))
        })
}

#[derive(Clone, Copy, Debug)]
pub enum CacheAccounting {
    Included,
    Separate,
}

/// Missing measurements stay missing. Output never participates in input accounting.
pub fn normalized_input(
    input: Option<u64>,
    cache_read: Option<u64>,
    cache_write: Option<u64>,
    accounting: CacheAccounting,
) -> Option<u64> {
    let input = input?;
    match accounting {
        CacheAccounting::Included => Some(input),
        CacheAccounting::Separate => input.checked_add(cache_read?)?.checked_add(cache_write?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reserve_and_recovery_obey_small_windows_and_explicit_caps() {
        let small = ModelBudget::new(Some(8_000), None, Some(4_000));
        assert_eq!(small.output_reserve(None).unwrap(), 2_000);
        assert_eq!(small.output_reserve(Some(3_000)).unwrap(), 3_000);
        assert!(small.output_reserve(Some(4_001)).is_err());
        assert!(small.fits(5_000, 2_000, false));
        assert!(!small.fits(5_000, 2_000, true));
        let normal = ModelBudget::new(None, Some(10), None);
        assert_eq!(normal.context, 128_000);
        assert_eq!(normal.output_reserve(None).unwrap(), 16_384);
        assert_eq!(normal.summarizer_cap(u64::MAX).unwrap(), 13_107);
        assert!(!normal.fits(10, 16_384, false));
        assert_eq!(padded_input(1), 2);
        assert!(!normal.fits(u64::MAX, u64::MAX, false));
    }
    #[test]
    fn separate_input_limit_triggers_before_combined_window_in_normal_and_recovery() {
        let combined = ModelBudget::new(Some(400_000), None, Some(128_000));
        let separate = ModelBudget::new(Some(400_000), Some(272_000), Some(128_000));
        let reserve = separate.output_reserve(None).unwrap();
        for recovery in [false, true] {
            assert!(combined.fits(280_000, reserve, recovery));
            assert!(!separate.fits(280_000, reserve, recovery));
            assert!(separate.fits(250_000, reserve, recovery));
        }
    }

    #[test]
    fn cache_and_unknown_usage_are_not_double_counted() {
        assert_eq!(
            normalized_input(Some(100), Some(40), None, CacheAccounting::Included),
            Some(100)
        );
        assert_eq!(
            normalized_input(Some(100), Some(40), Some(5), CacheAccounting::Separate),
            Some(145)
        );
        assert_eq!(
            normalized_input(Some(100), None, Some(5), CacheAccounting::Separate),
            None
        );
        assert_eq!(
            normalized_input(None, Some(40), Some(5), CacheAccounting::Included),
            None
        );
    }
    #[test]
    fn calibration_only_uses_compatible_unchanged_prefix() {
        let identity = RequestIdentity {
            provider_instance: "p".into(),
            model: "m".into(),
            api_format: "f".into(),
            instructions_hash: "i".into(),
            tools_hash: "t".into(),
            projection_version: 1,
            checkpoint: None,
        };
        let source = crate::SourceRef {
            scope: "turn".into(),
            id: "1".into(),
            version: "v1".into(),
        };
        let m = InputMeasurement {
            identity: identity.clone(),
            prefix: vec![source.clone()],
            input_tokens: 100,
        };
        assert_eq!(
            estimate_input(80, &identity, std::slice::from_ref(&source), Some(&m), 5),
            105
        );
        assert_eq!(
            estimate_input(200, &identity, std::slice::from_ref(&source), Some(&m), 5),
            200
        );
        let mut changed = identity.clone();
        changed.checkpoint = Some("new".into());
        assert_eq!(estimate_input(80, &changed, &[source], Some(&m), 5), 80);
        assert_eq!(estimate_input(80, &identity, &[], Some(&m), 5), 80);
    }
}
