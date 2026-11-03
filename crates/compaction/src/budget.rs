use serde::{Deserialize, Serialize};

/// Provider-normalized input plus locally estimated additions. The measurement
/// covers only the submitted prompt, never its response or later tool results.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InputEstimate {
    pub measured: u64,
    pub estimated: u64,
}
impl InputEstimate {
    pub fn local(estimated: u64) -> Self {
        Self {
            measured: 0,
            estimated,
        }
    }
    pub fn total(self) -> u64 {
        self.measured.saturating_add(self.estimated)
    }
    /// Only uncertain input receives OpenCode's 15% estimation allowance.
    pub fn padded(self) -> u64 {
        self.measured.saturating_add(padded_input(self.estimated))
    }
}

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

    /// Recovery uses the compaction buffer as a minimum room for output,
    /// replacing (rather than adding to) the ordinary output reserve.
    pub fn compaction_reserve(&self) -> u64 {
        let share = (self.context as u128 * 15 / 100) as u64;
        share.max(if self.context >= 32_000 { 16_000 } else { 0 })
    }

    pub fn fits(&self, input: u64, reserve: u64, recovery: bool) -> bool {
        self.fits_estimate(InputEstimate::local(input), reserve, recovery)
    }

    pub fn fits_estimate(&self, input: InputEstimate, reserve: u64, recovery: bool) -> bool {
        // A separate provider input limit excludes output. Context room includes
        // output, separate reasoning (only when outside output), and 1% framing
        // headroom. Unknown catalog limits retain the existing fallback contract.
        let input = input.padded();
        let window = (self.context as u128 * 99 / 100) as u64;
        let reserve = if recovery {
            reserve.max(self.compaction_reserve())
        } else {
            reserve
        };
        self.input_limit.is_none_or(|limit| input <= limit)
            && input as u128 + reserve as u128 + self.separate_reasoning as u128 <= window as u128
    }
}

/// Estimated input only; provider measurements must use InputEstimate::padded.
pub fn padded_input(input: u64) -> u64 {
    ((input as u128 * 115).div_ceil(100)).min(u64::MAX as u128) as u64
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
    estimate_input_parts(local_complete, identity, sources, measured, appended_local).total()
}

/// Replacing or shortening any measured source invalidates the anchor. No
/// arbitrary local maximum is mixed into a compatible provider measurement.
pub fn estimate_input_parts(
    local_complete: u64,
    identity: &RequestIdentity,
    sources: &[crate::SourceRef],
    measured: Option<&InputMeasurement>,
    appended_local: u64,
) -> InputEstimate {
    measured
        .filter(|m| m.identity == *identity && sources.starts_with(&m.prefix))
        .map_or(InputEstimate::local(local_complete), |m| InputEstimate {
            measured: m.input_tokens,
            estimated: appended_local,
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
    fn measured_input_is_not_padded_and_output_and_reasoning_are_reserved_once() {
        let mut small = ModelBudget::new(Some(8_000), Some(7_800), Some(2_000));
        let calibrated = InputEstimate {
            measured: 7_000,
            estimated: 100,
        };
        assert_eq!(calibrated.padded(), 7_115);
        assert!(small.fits_estimate(calibrated, 500, false));
        assert!(!small.fits(calibrated.total(), 500, false));
        small.separate_reasoning = 400;
        assert!(!small.fits_estimate(calibrated, 500, false));
        let large = ModelBudget::new(Some(128_000), None, Some(16_384));
        assert_eq!(large.compaction_reserve(), 19_200);
        assert!(large.fits_estimate(
            InputEstimate {
                measured: 107_000,
                estimated: 0
            },
            16_384,
            true
        ));
        assert!(!large.fits_estimate(
            InputEstimate {
                measured: 108_000,
                estimated: 0
            },
            16_384,
            true
        ));
        assert_eq!(small.compaction_reserve(), 1_200);
        assert!(!large.fits_estimate(
            InputEstimate {
                measured: u64::MAX,
                estimated: 1
            },
            1,
            false
        ));
    }

    #[test]
    fn reserve_and_recovery_obey_small_windows_and_explicit_caps() {
        let small = ModelBudget::new(Some(8_000), None, Some(4_000));
        assert_eq!(small.output_reserve(None).unwrap(), 2_000);
        assert_eq!(small.output_reserve(Some(3_000)).unwrap(), 3_000);
        assert!(small.output_reserve(Some(4_001)).is_err());
        for recovery in [false, true] {
            // The output reserve already exceeds this small window's buffer.
            assert!(small.fits(5_000, 2_000, recovery));
            assert!(!small.fits(5_200, 2_000, recovery));
        }
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
            assert!(separate.fits(230_000, reserve, recovery));
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
            105
        );
        let mut changed = identity.clone();
        changed.checkpoint = Some("new".into());
        assert_eq!(estimate_input(80, &changed, &[source], Some(&m), 5), 80);
        assert_eq!(estimate_input(80, &identity, &[], Some(&m), 5), 80);
    }
}
