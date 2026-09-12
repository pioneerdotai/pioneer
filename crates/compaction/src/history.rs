use crate::{ModelBudget, TAIL_TOKENS};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceRef {
    pub scope: String,
    pub id: String,
    pub version: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceRole {
    Own,
    Inherited,
    ReferenceOnly,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryUnit {
    pub sources: Vec<SourceRef>,
    pub role: SourceRole,
    pub tokens: u64,
    pub complete: bool,
    pub protected_input: bool,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompactionMode {
    Normal,
    Emergency,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompactionPlan {
    pub mode: CompactionMode,
    pub compact: Vec<usize>,
    pub retain: Vec<usize>,
    pub coverage: Vec<SourceRef>,
    pub fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanError {
    NoEligibleWork,
    ProtectedInputTooLarge,
    PendingRoundTooLarge,
    InvalidHistory,
}

/// Whole canonical rounds enter this planner. It never executes or reconciles tools.
#[allow(clippy::too_many_arguments)]
pub fn plan_compaction(
    units: &[HistoryUnit],
    model: &ModelBudget,
    reserve: u64,
    fixed_input: u64,
    summary_goal: u64,
    mode: CompactionMode,
    recovery: bool,
    basis: &str,
) -> Result<CompactionPlan, PlanError> {
    let mut seen = BTreeSet::new();
    for unit in units {
        if unit.sources.is_empty()
            || unit
                .sources
                .iter()
                .any(|s| !seen.insert((s.scope.clone(), s.id.clone())))
        {
            return Err(PlanError::InvalidHistory);
        }
    }
    let eligible = |u: &HistoryUnit| {
        u.complete
            && u.role == SourceRole::Own
            && (!u.protected_input || mode == CompactionMode::Emergency)
    };
    let mut retained: BTreeSet<usize> = units
        .iter()
        .enumerate()
        .filter_map(|(i, u)| (!eligible(u)).then_some(i))
        .collect();
    let mut tail = 0_u64;
    for (i, unit) in units.iter().enumerate().rev() {
        if !eligible(unit) {
            continue;
        }
        if tail >= TAIL_TOKENS {
            break;
        }
        retained.insert(i);
        tail = tail.saturating_add(unit.tokens);
    }
    let fits = |retained: &BTreeSet<usize>| {
        let input = retained
            .iter()
            .fold(fixed_input.saturating_add(summary_goal), |total, i| {
                total.saturating_add(units[*i].tokens)
            });
        model.fits(input, reserve, recovery)
    };
    while !fits(&retained) {
        if let Some(oldest) = retained.iter().copied().find(|i| eligible(&units[*i])) {
            retained.remove(&oldest);
        } else {
            return Err(if units.iter().any(|u| !u.complete) {
                PlanError::PendingRoundTooLarge
            } else {
                PlanError::ProtectedInputTooLarge
            });
        }
    }
    // A real provider overflow can disprove even an apparently fitting local
    // estimate. Recovery must change eligible input before its one main retry.
    if recovery
        && retained.len() == units.len()
        && let Some(oldest) = retained.iter().copied().find(|i| eligible(&units[*i]))
    {
        retained.remove(&oldest);
    }
    let compact: Vec<_> = (0..units.len()).filter(|i| !retained.contains(i)).collect();
    if compact.is_empty() {
        return Err(PlanError::NoEligibleWork);
    }
    let coverage = compact
        .iter()
        .flat_map(|i| units[*i].sources.clone())
        .collect();
    let fingerprint = hex::encode(Sha256::digest(
        serde_json::to_vec(&(
            basis,
            units,
            model,
            reserve,
            fixed_input,
            summary_goal,
            mode,
            recovery,
        ))
        .unwrap(),
    ));
    Ok(CompactionPlan {
        mode,
        compact,
        retain: retained.into_iter().collect(),
        coverage,
        fingerprint,
    })
}

/// Assemble overlapping delivery snapshots by identity. Equal text is irrelevant.
/// A conflicting version cannot be resolved by arbitrary iteration order.
pub fn union_sources(groups: &[Vec<SourceRef>]) -> anyhow::Result<Vec<SourceRef>> {
    let mut versions = BTreeMap::new();
    let mut result = Vec::new();
    for source in groups.iter().flatten() {
        let key = (&source.scope, &source.id);
        if let Some(previous) = versions.get(&key) {
            anyhow::ensure!(*previous == &source.version, "conflicting source revisions");
        } else {
            versions.insert(key, &source.version);
            result.push(source.clone());
        }
    }
    Ok(result)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub id: String,
    pub operation_id: String,
    pub format_version: u32,
    pub owner: String,
    pub previous: Option<String>,
    pub coverage: Vec<SourceRef>,
    pub summary: String,
    pub selection: crate::ModelSelection,
    pub projection_version: u64,
}

impl Checkpoint {
    pub fn compatible_with(&self, available: &BTreeSet<SourceRef>) -> bool {
        self.format_version == crate::FORMAT_VERSION
            && self.coverage.iter().all(|s| available.contains(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overflow_recovery_changes_an_eligible_prefix_even_when_local_estimate_fits() {
        let units = vec![unit("past", 100), unit("tail", 100)];
        let budget = ModelBudget::new(None, None, None);
        let plan = plan_compaction(
            &units,
            &budget,
            16384,
            0,
            100,
            CompactionMode::Normal,
            true,
            "overflow",
        )
        .unwrap();
        assert_eq!(plan.compact, vec![0]);
        assert_eq!(plan.retain, vec![1]);
        assert_eq!(plan.coverage, units[0].sources);
    }

    fn unit(id: &str, tokens: u64) -> HistoryUnit {
        HistoryUnit {
            sources: vec![SourceRef {
                scope: "t".into(),
                id: id.into(),
                version: "1".into(),
            }],
            role: SourceRole::Own,
            tokens,
            complete: true,
            protected_input: false,
        }
    }
    #[test]
    fn tail_rounds_up_and_then_shrinks_by_whole_rounds() {
        let units = vec![
            unit("old", 50_000),
            unit("7", 7_000),
            unit("9", 9_000),
            unit("8", 8_000),
        ];
        let roomy = ModelBudget::new(Some(50_000), None, None);
        let p = plan_compaction(
            &units,
            &roomy,
            10_000,
            0,
            1_000,
            CompactionMode::Normal,
            false,
            "b",
        )
        .unwrap();
        assert_eq!(p.retain, vec![1, 2, 3]);
        let tight = ModelBudget::new(Some(30_000), None, None);
        let p = plan_compaction(
            &units,
            &tight,
            10_000,
            0,
            1_000,
            CompactionMode::Normal,
            false,
            "b",
        )
        .unwrap();
        assert_eq!(p.retain, vec![2, 3]);
    }
    #[test]
    fn pending_and_accepted_steering_cannot_be_covered_by_normal_plan() {
        let mut units = vec![
            unit("old", 70_000),
            unit("steering", 1_000),
            unit("pending", 1_000),
        ];
        units[1].protected_input = true;
        units[2].complete = false;
        let budget = ModelBudget::new(Some(8_000), None, None);
        let p = plan_compaction(
            &units,
            &budget,
            2_000,
            100,
            100,
            CompactionMode::Normal,
            false,
            "b",
        )
        .unwrap();
        assert_eq!(p.compact, vec![0]);
        units[1].tokens = 30_000;
        assert!(
            plan_compaction(
                &units,
                &budget,
                2_000,
                100,
                100,
                CompactionMode::Normal,
                false,
                "b"
            )
            .is_err()
        );
        let p = plan_compaction(
            &units,
            &budget,
            2_000,
            100,
            100,
            CompactionMode::Emergency,
            true,
            "b",
        )
        .unwrap();
        assert_eq!(p.compact, vec![0, 1]);
        assert_eq!(p.retain, vec![2]);
    }
    #[test]
    fn independent_work_keeps_common_basis_once_and_conflicts_explicit() {
        let h = unit("h", 1).sources;
        let a = unit("a", 1).sources;
        let b = unit("b", 1).sources;
        assert_eq!(
            union_sources(&[h.clone(), a, h.clone(), b]).unwrap().len(),
            3
        );
        let mut changed = h.clone();
        changed[0].version = "2".into();
        assert!(union_sources(&[h, changed]).is_err());
    }
}
