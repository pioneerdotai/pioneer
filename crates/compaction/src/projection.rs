use crate::SourceRef;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryObservation {
    pub source: SourceRef,
    /// Only authoritative, persisted alias/revision links may populate this list.
    pub supersedes: Vec<SourceRef>,
    pub content: String,
}
#[derive(Clone, Debug)]
pub struct HistoryProjection {
    pub observations: Vec<HistoryObservation>,
    pub conflicts: Vec<(SourceRef, SourceRef)>,
}

/// Identity-based normalization. No similarity, text heuristics or invented results.
pub fn project_history(observations: Vec<HistoryObservation>) -> anyhow::Result<HistoryProjection> {
    let mut by_ref = BTreeMap::new();
    for observation in &observations {
        if let Some(previous) = by_ref.insert(&observation.source, observation) {
            anyhow::ensure!(
                previous.content == observation.content
                    && previous.supersedes == observation.supersedes,
                "same source revision has conflicting content"
            );
        }
        anyhow::ensure!(
            !observation.supersedes.contains(&observation.source),
            "source cannot supersede itself"
        );
    }
    let mut removed = BTreeSet::new();
    for observation in &observations {
        for reference in &observation.supersedes {
            // Follow only explicit links; cycles are malformed, not arbitrary winners.
            let mut queue = vec![reference];
            let mut visited = BTreeSet::new();
            while let Some(current) = queue.pop() {
                anyhow::ensure!(*current != observation.source, "cyclic source supersession");
                if !visited.insert(current) {
                    continue;
                }
                if let Some(source) = by_ref.get(current) {
                    queue.extend(source.supersedes.iter());
                }
            }
            removed.insert(reference.clone());
        }
    }
    let mut seen = BTreeSet::new();
    let observations: Vec<_> = observations
        .into_iter()
        .filter(|o| !removed.contains(&o.source) && seen.insert(o.source.clone()))
        .collect();
    let mut identities = BTreeMap::new();
    let mut conflicts = Vec::new();
    for observation in &observations {
        let key = (&observation.source.scope, &observation.source.id);
        if let Some(previous) = identities.insert(key, observation.source.clone()) {
            conflicts.push((previous, observation.source.clone()));
        }
    }
    Ok(HistoryProjection {
        observations,
        conflicts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn observation(id: &str) -> HistoryObservation {
        HistoryObservation {
            source: SourceRef {
                scope: "turn".into(),
                id: id.into(),
                version: "1".into(),
            },
            supersedes: vec![],
            content: "same text".into(),
        }
    }
    #[test]
    fn only_explicit_aliases_remove_ui_copies() {
        let ui = observation("ui");
        let mut canonical = observation("canonical");
        canonical.supersedes.push(ui.source.clone());
        let result = project_history(vec![
            ui,
            canonical.clone(),
            canonical,
            observation("independent"),
        ])
        .unwrap();
        assert_eq!(result.observations.len(), 2);
        assert!(result.conflicts.is_empty());
    }
    #[test]
    fn unresolved_revisions_are_retained_and_cycles_rejected() {
        let a = observation("a");
        let mut b = a.clone();
        b.source.version = "2".into();
        let result = project_history(vec![a.clone(), b.clone()]).unwrap();
        assert_eq!(result.observations.len(), 2);
        assert_eq!(result.conflicts.len(), 1);
        let mut a = a;
        a.supersedes.push(b.source.clone());
        b.supersedes.push(a.source.clone());
        assert!(project_history(vec![a, b]).is_err());
    }
}
