//! A frozen context owns ordered references, never a second transcript. A wire
//! digest verifies deterministic rematerialization; it is not a coverage guess.
use crate::SourceRef;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ScopedReplaySource {
    pub thread: String,
    pub source: SourceRef,
}

/// Shared publication/read policy for exact historical replay identities.
/// Conflicting Task-input claims leave the copy visible; tool replay remains
/// strict. Targets are checked against actual graph leaves, not made grants.
#[derive(Default)]
pub struct ReplayAliasGraph {
    resolved: BTreeMap<ScopedReplaySource, ScopedReplaySource>,
    ambiguous_inputs: BTreeSet<ScopedReplaySource>,
    targets: BTreeSet<ScopedReplaySource>,
    kinds: BTreeMap<ScopedReplaySource, bool>,
}

impl ReplayAliasGraph {
    pub fn insert(
        &mut self,
        replay: ScopedReplaySource,
        covered: ScopedReplaySource,
        tool_item_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let input_copy = tool_item_id.is_none()
            && replay.source.scope.starts_with("input:")
            && covered.source.scope.starts_with("input:");
        if let Some(previous_kind) = self.kinds.get(&replay) {
            anyhow::ensure!(
                *previous_kind == input_copy,
                "checkpoint replay alias mixes input and tool replay"
            );
        }
        if input_copy && replay == covered {
            self.resolved.remove(&replay);
            self.ambiguous_inputs.insert(replay.clone());
            self.kinds.insert(replay, true);
            return Ok(());
        }
        self.targets.insert(covered.clone());
        if let Some(previous_kind) = self.kinds.insert(replay.clone(), input_copy) {
            anyhow::ensure!(
                previous_kind == input_copy,
                "checkpoint replay alias mixes input and tool replay"
            );
        }
        if self.ambiguous_inputs.contains(&replay) {
            anyhow::ensure!(input_copy, "checkpoint replay alias is ambiguous");
            return Ok(());
        }
        if let Some(previous) = self.resolved.get(&replay) {
            if previous != &covered {
                anyhow::ensure!(input_copy, "checkpoint replay alias is ambiguous");
                self.resolved.remove(&replay);
                self.ambiguous_inputs.insert(replay);
            }
        } else {
            self.resolved.insert(replay, covered);
        }
        Ok(())
    }

    pub fn validate_targets(&self, leaves: &BTreeSet<ScopedReplaySource>) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.targets.is_subset(leaves),
            "checkpoint replay alias is outside historical coverage"
        );
        Ok(())
    }

    pub fn into_parts(
        self,
    ) -> (
        BTreeMap<ScopedReplaySource, ScopedReplaySource>,
        BTreeSet<ScopedReplaySource>,
        BTreeSet<ScopedReplaySource>,
    ) {
        let input_replays = self
            .kinds
            .into_iter()
            .filter_map(|(replay, input)| {
                (input && self.resolved.contains_key(&replay)).then_some(replay)
            })
            .collect();
        (self.resolved, self.ambiguous_inputs, input_replays)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenSourceAlias {
    pub represented_thread: String,
    pub represented_source: SourceRef,
    pub source_thread: String,
    pub source: SourceRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenInputAliasConflict {
    pub source_thread: String,
    pub source: SourceRef,
}

/// Only the evidence not already reachable through a checkpoint source in
/// this reference becomes a direct edge of the next checkpoint. The complete
/// evidence remains in `source_aliases` for literal frozen reconstruction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenPublicationAliases {
    pub source_aliases: Vec<FrozenSourceAlias>,
    pub ambiguous_input_aliases: Vec<FrozenInputAliasConflict>,
}

/// One direct replay edge of a selected frozen source. Ownership and source
/// selection are checked by the caller; this only defines the shared wire
/// transformation used by planning and publication.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FrozenReplayEdge {
    pub covered_thread: String,
    pub covered: SourceRef,
    pub replay_thread: String,
    pub replay: SourceRef,
    pub tool_item_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrozenEventInputRole {
    Authoritative,
    Deleted,
    InputCopy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenMessageRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_turn_id: Option<String>,
    pub source_thread: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_thread: Option<String>,
    pub unit_id: String,
    pub sources: Vec<SourceRef>,
    /// Exact event-input relationship captured while this source revision was
    /// still available. Unlike the mutable projection cache, this evidence is
    /// part of the immutable manifest reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_input_role: Option<FrozenEventInputRole>,
    /// Exact transport-copy inputs represented by `sources`. Aliases are
    /// historical suppression evidence, not payloads or access grants.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_aliases: Vec<FrozenSourceAlias>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ambiguous_input_aliases: Vec<FrozenInputAliasConflict>,
    /// None preserves the historical direct-edge policy of old manifests.
    /// Some, including empty lists, is the exact incremental publication set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_aliases: Option<FrozenPublicationAliases>,
    pub inherited: bool,
    pub complete: bool,
    pub protected_input: bool,
    pub wire_sha256: String,
    /// Exact bounded provider representation when the full original lives in a
    /// terminal tool item. The latter remains the actual coverage source.
    pub replay_source: Option<SourceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_item_id: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_name: Option<String>,
}
impl FrozenMessageRef {
    pub fn publication_edges_for(&self, source: &SourceRef) -> Vec<FrozenReplayEdge> {
        let mut edges = Vec::new();
        if let Some(replay) = &self.replay_source {
            edges.push(FrozenReplayEdge {
                covered_thread: self.source_thread.clone(),
                covered: source.clone(),
                replay_thread: self.source_thread.clone(),
                replay: replay.clone(),
                tool_item_id: self.tool_item_id.clone(),
            });
        }
        let (aliases, conflicts) = self
            .publication_aliases
            .as_ref()
            .map(|publication| {
                (
                    publication.source_aliases.as_slice(),
                    publication.ambiguous_input_aliases.as_slice(),
                )
            })
            .unwrap_or((&self.source_aliases, &self.ambiguous_input_aliases));
        for alias in aliases {
            if (alias.represented_thread == self.source_thread
                && alias.represented_source == *source)
                || (source.scope.starts_with("checkpoint:")
                    && alias.represented_source.scope.starts_with("input:"))
            {
                edges.push(FrozenReplayEdge {
                    covered_thread: alias.represented_thread.clone(),
                    covered: alias.represented_source.clone(),
                    replay_thread: alias.source_thread.clone(),
                    replay: alias.source.clone(),
                    tool_item_id: None,
                });
            }
        }
        for conflict in conflicts {
            edges.push(FrozenReplayEdge {
                covered_thread: conflict.source_thread.clone(),
                covered: conflict.source.clone(),
                replay_thread: conflict.source_thread.clone(),
                replay: conflict.source.clone(),
                tool_item_id: None,
            });
        }
        edges
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.logical_turn_id
                .as_ref()
                .is_none_or(|id| !id.is_empty()),
            "frozen logical turn identity is empty"
        );
        anyhow::ensure!(
            !self.source_thread.is_empty() && !self.unit_id.is_empty() && !self.sources.is_empty(),
            "frozen message identity is missing"
        );
        anyhow::ensure!(
            self.event_input_role.is_none()
                || (self.sources.len() == 1 && self.sources[0].scope.starts_with("event:")),
            "frozen event-input evidence does not name one event source"
        );
        anyhow::ensure!(
            self.wire_sha256.len() == 64 && self.wire_sha256.bytes().all(|c| c.is_ascii_hexdigit()),
            "frozen wire digest is invalid"
        );
        anyhow::ensure!(
            self.context_thread
                .as_ref()
                .is_none_or(|owner| !owner.is_empty()),
            "frozen context owner is missing"
        );
        anyhow::ensure!(
            self.tool_item_id.as_ref().is_none_or(|id| !id.is_empty()),
            "frozen tool item identity is empty"
        );
        for source in self
            .sources
            .iter()
            .chain(
                self.source_aliases
                    .iter()
                    .map(|alias| &alias.represented_source),
            )
            .chain(self.source_aliases.iter().map(|alias| &alias.source))
            .chain(
                self.ambiguous_input_aliases
                    .iter()
                    .map(|alias| &alias.source),
            )
            .chain(self.replay_source.iter())
        {
            anyhow::ensure!(
                !source.id.is_empty()
                    && !source.version.is_empty()
                    && source
                        .scope
                        .split_once(':')
                        .is_some_and(|(kind, owner)| !owner.is_empty()
                            && matches!(
                                kind,
                                "input"
                                    | "event"
                                    | "context"
                                    | "item"
                                    | "checkpoint"
                                    | "task-basis"
                            )),
                "frozen source is not canonical"
            );
        }
        anyhow::ensure!(
            self.source_aliases.iter().all(|alias| {
                !alias.represented_thread.is_empty()
                    && !alias.source_thread.is_empty()
                    && ((alias.represented_thread == self.source_thread
                        && self.sources.contains(&alias.represented_source))
                        || (self.sources.len() == 1
                            && self.sources[0].scope.starts_with("checkpoint:")
                            && alias.represented_source.scope.starts_with("input:")))
                    && alias.represented_source.scope.starts_with("input:")
                    && alias.source.scope.starts_with("input:")
                    && (alias.represented_thread != alias.source_thread
                        || alias.represented_source != alias.source)
            }),
            "frozen input alias identity is invalid"
        );
        anyhow::ensure!(
            self.ambiguous_input_aliases.iter().all(|alias| {
                !alias.source_thread.is_empty() && alias.source.scope.starts_with("input:")
            }),
            "frozen ambiguous input identity is invalid"
        );
        if let Some(publication) = &self.publication_aliases {
            anyhow::ensure!(
                self.sources.len() == 1 && self.sources[0].scope.starts_with("checkpoint:"),
                "incremental replay publication requires a checkpoint carrier"
            );
            anyhow::ensure!(
                publication
                    .source_aliases
                    .iter()
                    .all(|alias| self.source_aliases.contains(alias))
                    && publication
                        .ambiguous_input_aliases
                        .iter()
                        .all(|alias| self.ambiguous_input_aliases.contains(alias)),
                "incremental replay publication is not a subset of frozen evidence"
            );
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenHistoryRef {
    pub format: u32,
    pub manifest_id: String,
    pub messages: u64,
    pub identity_sha256: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    #[test]
    fn exact_input_conflict_is_conservative_but_tool_replay_is_strict() {
        let source = |thread: &str, id: &str, version: &str| ScopedReplaySource {
            thread: thread.into(),
            source: SourceRef {
                scope: format!("input:{thread}"),
                id: id.into(),
                version: version.into(),
            },
        };
        let a = source("parent", "A", "input-revision:1");
        let c = source("other", "C", "input-revision:1");
        let b_v1 = source("child", "B", "input-revision:1");
        let b_v2 = source("child", "B", "input-revision:2");
        for order in [[a.clone(), c.clone()], [c.clone(), a.clone()]] {
            let mut graph = ReplayAliasGraph::default();
            graph.insert(b_v1.clone(), order[0].clone(), None).unwrap();
            graph.insert(b_v1.clone(), order[1].clone(), None).unwrap();
            graph.insert(b_v2.clone(), a.clone(), None).unwrap();
            graph
                .validate_targets(&BTreeSet::from([a.clone(), c.clone()]))
                .unwrap();
            let (resolved, ambiguous, input_replays) = graph.into_parts();
            assert!(!resolved.contains_key(&b_v1));
            assert_eq!(resolved.get(&b_v2), Some(&a));
            assert!(ambiguous.contains(&b_v1));
            assert!(!ambiguous.contains(&b_v2));
            assert!(!input_replays.contains(&b_v1));
            assert!(input_replays.contains(&b_v2));
        }
        let mut tool = ReplayAliasGraph::default();
        tool.insert(b_v1.clone(), a, Some("tool-item")).unwrap();
        assert!(tool.insert(b_v1, c, Some("tool-item")).is_err());
    }

    #[test]
    fn publication_edges_distinguish_legacy_and_explicit_empty_direct_evidence() {
        let source = SourceRef {
            scope: "checkpoint:owner".into(),
            id: "summary".into(),
            version: "identity".into(),
        };
        let input = SourceRef {
            scope: "input:parent-turn".into(),
            id: "A".into(),
            version: "input-revision:1".into(),
        };
        let copy = SourceRef {
            scope: "input:child-turn".into(),
            id: "B".into(),
            version: "input-revision:1".into(),
        };
        let mut reference = FrozenMessageRef {
            logical_turn_id: None,
            source_thread: "parent".into(),
            context_thread: None,
            unit_id: "summary".into(),
            sources: vec![source.clone()],
            event_input_role: None,
            source_aliases: vec![FrozenSourceAlias {
                represented_thread: "parent".into(),
                represented_source: input.clone(),
                source_thread: "child".into(),
                source: copy.clone(),
            }],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: true,
            complete: true,
            protected_input: false,
            wire_sha256: "a".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        };
        reference.validate().unwrap();
        assert_eq!(reference.publication_edges_for(&source).len(), 1);
        reference.publication_aliases = Some(FrozenPublicationAliases {
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
        });
        reference.validate().unwrap();
        assert!(reference.publication_edges_for(&source).is_empty());
        assert_eq!(reference.source_aliases[0].source, copy);
        assert_eq!(reference.source_aliases[0].represented_source, input);
        reference.replay_source = Some(SourceRef {
            scope: "item:tool-turn".into(),
            id: "tool-output".into(),
            version: "item-revision:1".into(),
        });
        reference.tool_item_id = Some("tool-output".into());
        let edges = reference.publication_edges_for(&source);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].tool_item_id.as_deref(), Some("tool-output"));
    }
    #[test]
    fn logical_alias_extension_preserves_existing_frozen_digest_bytes() {
        let previous = format!(
            r#"{{"source_thread":"thread","unit_id":"unit","sources":[{{"scope":"event:turn","id":"event","version":"event-revision:1"}}],"inherited":false,"complete":true,"protected_input":false,"wire_sha256":"{}","replay_source":null,"tool_call_id":null,"tool_name":null}}"#,
            "a".repeat(64)
        );
        let mut value: FrozenMessageRef = serde_json::from_str(&previous).unwrap();
        assert!(value.logical_turn_id.is_none());
        assert!(value.event_input_role.is_none());
        value.validate().unwrap();
        assert_eq!(serde_json::to_string(&value).unwrap(), previous);
        value.logical_turn_id = Some("command-turn".into());
        let restored: FrozenMessageRef =
            serde_json::from_str(&serde_json::to_string(&value).unwrap()).unwrap();
        assert_eq!(restored, value);
        value.logical_turn_id = Some(String::new());
        assert!(value.validate().is_err());
    }

    #[test]
    fn input_copy_reference_roundtrips_without_changing_manifest_digest_bytes() {
        let persisted = format!(
            r#"{{"source_thread":"thread","unit_id":"unit","sources":[{{"scope":"event:turn","id":"event","version":"event-revision:1"}}],"event_input_role":"input_copy","inherited":false,"complete":true,"protected_input":false,"wire_sha256":"{}","replay_source":null,"tool_call_id":null,"tool_name":null}}"#,
            "a".repeat(64),
        );
        let before = Sha256::digest(persisted.as_bytes());
        let restored: FrozenMessageRef = serde_json::from_str(&persisted).unwrap();
        restored.validate().unwrap();
        assert_eq!(
            restored.event_input_role,
            Some(FrozenEventInputRole::InputCopy)
        );
        let encoded = serde_json::to_string(&restored).unwrap();
        assert_eq!(encoded, persisted);
        assert_eq!(Sha256::digest(encoded.as_bytes()), before);
        let legacy_with_explicit_null = persisted.replace(
            "\"tool_call_id\":null",
            "\"tool_item_id\":null,\"tool_call_id\":null",
        );
        let legacy: FrozenMessageRef = serde_json::from_str(&legacy_with_explicit_null).unwrap();
        assert_eq!(legacy, restored);
    }
}
