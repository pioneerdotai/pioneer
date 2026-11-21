//! Exact checkpoint ownership, historical evidence, and bounded graph validation.
use anyhow::{Result, ensure};
use pioneer_compaction::frozen::{FrozenEventInputRole, FrozenMessageRef};
use pioneer_compaction::{OperationSnapshot, REPLAY_ALIAS_LIMIT, SourceRef};
use pioneer_entity::compaction_checkpoint_import as import;
use sea_orm::sea_query::{Alias as SqlAlias, Expr, ExprTrait, Query};
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, Statement};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::SOURCE_PAGE_BYTES;
use HistoricalEventInputEvidenceRow as Event;
use HistoricalReplayAliasRow as Alias;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
pub struct CheckpointEdgesRow {
    pub id: String,
    pub proof_version: i64,
    /// Existing portion boundary; prevents a legacy repair mixing read pages.
    pub coverage_closed: bool,
    pub status: String,
    pub owner: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub identity_sha256: String,
    pub previous: Option<String>,
    pub format_version: i64,
    pub operation_id: String,
    pub manifest_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct HistoricalReplayAliasRow {
    pub covered_thread: String,
    pub replay_thread: String,
    pub covered_scope: String,
    pub covered_id: String,
    pub covered_version: String,
    pub replay_scope: String,
    pub replay_id: String,
    pub replay_version: String,
    pub tool_item_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct HistoricalEventInputEvidenceRow {
    pub source_thread: String,
    pub source_scope: String,
    pub source_id: String,
    pub source_version: String,
    pub role: String,
}

pub type Ownership = BTreeMap<SourceRef, BTreeSet<String>>;
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metadata {
    pub aliases: BTreeSet<Alias>,
    pub events: BTreeSet<Event>,
}

/// The single selection rule, shared by legacy reads and new preparation.
pub fn select_reference(
    reference: &FrozenMessageRef,
    ownership: &Ownership,
    metadata: &mut Metadata,
) -> Result<()> {
    for source in reference.sources.iter().filter(|source| {
        ownership
            .get(*source)
            .is_some_and(|threads| threads.len() == 1 && threads.contains(&reference.source_thread))
    }) {
        for edge in reference.publication_edges_for(source) {
            metadata.aliases.insert(Alias {
                covered_thread: edge.covered_thread,
                replay_thread: edge.replay_thread,
                covered_scope: edge.covered.scope,
                covered_id: edge.covered.id,
                covered_version: edge.covered.version,
                replay_scope: edge.replay.scope,
                replay_id: edge.replay.id,
                replay_version: edge.replay.version,
                tool_item_id: edge.tool_item_id,
            });
        }
    }
    if let [source] = reference.sources.as_slice()
        && let Some(role) = reference.event_input_role
        && ownership
            .get(source)
            .is_some_and(|threads| threads.len() == 1 && threads.contains(&reference.source_thread))
    {
        let event = Event {
            source_thread: reference.source_thread.clone(),
            source_scope: source.scope.clone(),
            source_id: source.id.clone(),
            source_version: source.version.clone(),
            role: match role {
                FrozenEventInputRole::Authoritative => "authoritative",
                FrozenEventInputRole::Deleted => "deleted",
                FrozenEventInputRole::InputCopy => "input_copy",
            }
            .into(),
        };
        ensure!(
            !metadata
                .events
                .iter()
                .any(|old| old.source_thread == event.source_thread
                    && old.source_scope == event.source_scope
                    && old.source_id == event.source_id
                    && old.source_version == event.source_version
                    && old.role != event.role),
            "conflicting historical event role"
        );
        metadata.events.insert(event);
    }
    ensure!(
        metadata.aliases.len() <= REPLAY_ALIAS_LIMIT && metadata.events.len() <= REPLAY_ALIAS_LIMIT,
        "checkpoint selected metadata exceeds supported quantum"
    );
    Ok(())
}

/// A missing frozen binding is legitimate only for the original raw assertion
/// admission, whose complete plan was durably saved (frozen admission saves an
/// empty compact/coverage plan). Historical ownership must still be exact.
pub fn raw_contract(
    snapshot: &OperationSnapshot,
    owner: &str,
    row: &CheckpointEdgesRow,
    ownership: &Ownership,
) -> Result<()> {
    let admitted = snapshot.plan.coverage.iter().collect::<BTreeSet<_>>();
    ensure!(
        owner == row.owner
            && snapshot.owner == row.owner
            && snapshot.id == row.operation_id
            && !snapshot.plan.compact.is_empty()
            && !snapshot.plan.coverage.is_empty()
            && ownership.keys().all(|source| admitted.contains(source)),
        "checkpoint lost expected frozen binding or original raw contract"
    );
    Ok(())
}
pub fn add_import_to_digest(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_le_bytes());
    digest.update(value);
}
pub fn import_identity(proof: &import::Model) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(
        proof.import_ordinal,
        proof.message_ordinal,
        &proof.target_source_thread,
        &proof.context_thread,
        &proof.target_checkpoint_json,
        &proof.proof_json,
        proof.bytes,
    ))?)
}
fn target_reference_statement(manifest: &str, ordinal: u64) -> Result<Statement> {
    Ok(DbBackend::Postgres.build(
        &Query::select()
            .columns([SqlAlias::new("reference_json"), SqlAlias::new("bytes")])
            .from(SqlAlias::new("compaction_frozen_message"))
            .and_where(Expr::col(SqlAlias::new("manifest_id")).eq(manifest))
            .and_where(Expr::col(SqlAlias::new("ordinal")).eq(i64::try_from(ordinal)?))
            .and_where(
                Expr::col(SqlAlias::new("bytes")).between(0, i64::try_from(SOURCE_PAGE_BYTES)?),
            )
            .and_where(Expr::cust("octet_length(reference_json::text)=bytes"))
            .limit(1)
            .to_owned(),
    ))
}
pub async fn target_reference(
    db: &impl ConnectionTrait,
    manifest: &str,
    ordinal: u64,
) -> Result<FrozenMessageRef> {
    // The logical view applies H's bounds to both direct and shared branches.
    // Understated retained-output bytes must not return a payload at all.
    let row = db
        .query_one_raw(target_reference_statement(manifest, ordinal)?)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("original import target missing, inconsistent or oversized")
        })?;
    let json: String = row.try_get("", "reference_json")?;
    let bytes: i64 = row.try_get("", "bytes")?;
    ensure!(
        json.len() == usize::try_from(bytes)?,
        "original import target size mismatch"
    );
    let target: FrozenMessageRef = serde_json::from_str(&json)?;
    target.validate()?;
    Ok(target)
}
pub fn validate_metadata(metadata: &Metadata, ownership: &Ownership) -> Result<()> {
    for alias in &metadata.aliases {
        ensure!(
            !alias.covered_thread.is_empty()
                && !alias.covered_scope.is_empty()
                && !alias.covered_id.is_empty()
                && !alias.covered_version.is_empty()
                && !alias.replay_thread.is_empty()
                && !alias.replay_scope.is_empty()
                && !alias.replay_id.is_empty()
                && !alias.replay_version.is_empty(),
            "malformed permanent replay identity"
        );
    }
    for event in &metadata.events {
        let source = SourceRef {
            scope: event.source_scope.clone(),
            id: event.source_id.clone(),
            version: event.source_version.clone(),
        };
        ensure!(
            source.scope.starts_with("event:")
                && ownership.get(&source).is_some_and(
                    |threads| threads.len() == 1 && threads.contains(&event.source_thread)
                )
                && matches!(
                    event.role.as_str(),
                    "authoritative" | "deleted" | "input_copy"
                ),
            "permanent event evidence is outside exact coverage"
        );
    }
    Ok(())
}
#[derive(Clone)]
pub struct GraphEvidence {
    pub row: CheckpointEdgesRow,
    pub ownership: Ownership,
    pub metadata: Metadata,
}
pub struct CachedGraphValidation {
    current: GraphEvidence,
    pending: Vec<(String, bool)>,
    active: BTreeSet<String>,
    done: BTreeSet<String>,
    leaves: BTreeSet<pioneer_compaction::frozen::ScopedReplaySource>,
    aliases: pioneer_compaction::frozen::ReplayAliasGraph,
    roles: BTreeMap<(String, String, String, String), String>,
    target_after: Option<pioneer_compaction::frozen::ScopedReplaySource>,
    finished: bool,
}
impl CachedGraphValidation {
    pub fn new(current: GraphEvidence) -> Self {
        Self {
            pending: vec![(current.row.id.clone(), false)],
            current,
            active: BTreeSet::new(),
            done: BTreeSet::new(),
            leaves: BTreeSet::new(),
            aliases: Default::default(),
            roles: BTreeMap::new(),
            target_after: None,
            finished: false,
        }
    }
    pub fn finished(&self) -> bool {
        self.finished
    }

    pub fn has_leaves(&self) -> bool {
        !self.leaves.is_empty()
    }

    pub fn step(&mut self, cache: &BTreeMap<String, GraphEvidence>) -> Result<()> {
        use pioneer_compaction::frozen::ScopedReplaySource;
        if self.finished {
            return Ok(());
        }
        if let Some((id, exit)) = self.pending.pop() {
            if exit {
                self.active.remove(&id);
                self.done.insert(id);
                return Ok(());
            }
            if self.done.contains(&id) {
                return Ok(());
            }
            ensure!(
                self.active.insert(id.clone()),
                "cyclic prepared checkpoint graph"
            );
            ensure!(
                self.active.len() + self.done.len() <= 65_536,
                "prepared checkpoint graph exceeds bound"
            );
            let node = if id == self.current.row.id {
                &self.current
            } else {
                cache
                    .get(&id)
                    .ok_or_else(|| anyhow::anyhow!("prepared dependency graph missing"))?
            };
            ensure!(
                node.row.workspace_id == self.current.row.workspace_id
                    && node.row.format_version == 1,
                "prepared graph scope/format mismatch"
            );
            validate_metadata(&node.metadata, &node.ownership)?;
            for alias in &node.metadata.aliases {
                self.aliases.insert(
                    ScopedReplaySource {
                        thread: alias.replay_thread.clone(),
                        source: SourceRef {
                            scope: alias.replay_scope.clone(),
                            id: alias.replay_id.clone(),
                            version: alias.replay_version.clone(),
                        },
                    },
                    ScopedReplaySource {
                        thread: alias.covered_thread.clone(),
                        source: SourceRef {
                            scope: alias.covered_scope.clone(),
                            id: alias.covered_id.clone(),
                            version: alias.covered_version.clone(),
                        },
                    },
                    alias.tool_item_id.as_deref(),
                )?;
            }
            for event in &node.metadata.events {
                let key = (
                    event.source_thread.clone(),
                    event.source_scope.clone(),
                    event.source_id.clone(),
                    event.source_version.clone(),
                );
                ensure!(
                    self.roles
                        .insert(key, event.role.clone())
                        .is_none_or(|old| old == event.role),
                    "conflicting prepared historical event roles"
                );
            }
            self.pending.push((id, true));
            if let Some(previous) = &node.row.previous {
                let previous_node = cache
                    .get(previous)
                    .ok_or_else(|| anyhow::anyhow!("prepared previous graph missing"))?;
                ensure!(
                    previous_node.row.owner == node.row.owner
                        && previous_node.row.thread_id == node.row.thread_id,
                    "prepared previous ownership mismatch"
                );
                self.pending.push((previous.clone(), false));
            }
            for (source, threads) in &node.ownership {
                let thread = threads
                    .iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("prepared ownership missing"))?;
                if source.scope.starts_with("checkpoint:") {
                    let foreign = cache
                        .get(&source.id)
                        .ok_or_else(|| anyhow::anyhow!("prepared foreign dependency missing"))?;
                    ensure!(
                        foreign.row.thread_id == *thread
                            && source.scope == format!("checkpoint:{}", foreign.row.owner)
                            && source.version == foreign.row.identity_sha256,
                        "prepared foreign identity mismatch"
                    );
                    self.pending.push((source.id.clone(), false));
                } else {
                    self.leaves.insert(ScopedReplaySource {
                        thread: thread.clone(),
                        source: source.clone(),
                    });
                }
            }
            return Ok(());
        }
        let (after, finished) = self
            .aliases
            .validate_targets_page(&self.leaves, self.target_after.as_ref())?;
        self.target_after = after;
        self.finished = finished;
        Ok(())
    }
}
#[cfg(test)]
fn validate_cached_graph(
    root: &str,
    current: &GraphEvidence,
    cache: &BTreeMap<String, GraphEvidence>,
) -> Result<bool> {
    ensure!(root == current.row.id, "wrong graph root");
    let mut validation = CachedGraphValidation::new(current.clone());
    while !validation.finished {
        validation.step(cache)?;
    }
    Ok(!validation.leaves.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::TransactionTrait;

    #[pioneer_postgres_test::postgres_test]
    async fn retained_target_payload_statement_rejects_understated_direct_and_shared_bytes() {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection();
        db.execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES('ws','fixture');
            INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
             VALUES('thread','ws','','agent','m','p','active');",
        )
        .await
        .unwrap();
        db.execute_unprepared("INSERT INTO compaction_frozen_history(id,workspace_id,owner_thread,identity_sha256,message_count,next_ordinal,ready) VALUES('output','ws','thread','output',1,1,1),('backing','ws','thread','backing',1,1,1)").await.unwrap();
        let oversized = "x".repeat(SOURCE_PAGE_BYTES + 1);
        // Model legacy corruption, then restore production CHECK enforcement
        // before either direct or shared payload SELECT is exercised.
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared("ALTER TABLE compaction_frozen_message_data DROP CONSTRAINT compaction_frozen_message_data_check")
            .await
            .unwrap();
        for id in ["output", "backing"] {
            tx.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
            "INSERT INTO compaction_frozen_message_data(manifest_id,ordinal,reference_json,bytes) VALUES($1,0,$2,1)", [id.into(), oversized.clone().into()])).await.unwrap();
        }
        tx.execute_unprepared("ALTER TABLE compaction_frozen_message_data ADD CONSTRAINT compaction_frozen_message_data_check CHECK (bytes BETWEEN 0 AND 262144 AND octet_length(reference_json) = bytes) NOT VALID")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        for shared in [false, true] {
            if shared {
                for sql in [
                    "INSERT INTO compaction_frozen_layout(manifest_id,kind,active,pending) VALUES('output',0,0,1)",
                    "INSERT INTO compaction_frozen_span(manifest_id,kind,start,\"end\",source_manifest) VALUES('output',0,0,1,'backing')",
                    "UPDATE compaction_frozen_layout SET active=1,pending=0,verified_span_revision=(SELECT revision FROM compaction_frozen_sweep_consumer WHERE manifest_id='output' AND kind=0) WHERE manifest_id='output' AND kind=0",
                ] {
                    db.execute_unprepared(sql).await.unwrap();
                }
            }
            assert!(
                db.query_one_raw(target_reference_statement("output", 0).unwrap())
                    .await
                    .unwrap()
                    .is_none(),
                "payload SQL must return no oversized JSON"
            );
            assert!(target_reference(db, "output", 0).await.is_err());
        }
        let n: i64 = db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT count(*) AS n FROM compaction_frozen_message_data",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "n")
            .unwrap();
        assert_eq!(n, 2, "bounded refusal preserves both backing rows");
    }

    #[test]
    fn selected_evidence_matches_original_extractor_exact_scoped_sets() {
        use pioneer_compaction::frozen::{
            FrozenInputAliasConflict, FrozenPublicationAliases, FrozenSourceAlias,
        };
        let source = |kind: &str, id: &str| SourceRef {
            scope: format!("{kind}:turn"),
            id: id.into(),
            version: format!("revision-{id}"),
        };
        let copied = source("input", "copy");
        let make = |id: &str, role: Option<FrozenEventInputRole>| FrozenMessageRef {
            logical_turn_id: None,
            source_thread: "thread".into(),
            context_thread: None,
            unit_id: id.into(),
            sources: vec![source("event", id)],
            event_input_role: role,
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
            publication_aliases: None,
            inherited: false,
            complete: true,
            protected_input: false,
            wire_sha256: "a".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
        };
        let mut authoritative = make("event", Some(FrozenEventInputRole::Authoritative));
        authoritative.replay_source = Some(source("item", "tool-replay"));
        authoritative.tool_item_id = Some("tool".into());
        authoritative.source_aliases.push(FrozenSourceAlias {
            represented_thread: "thread".into(),
            represented_source: source("input", "covered"),
            source_thread: "thread".into(),
            source: copied.clone(),
        });
        authoritative
            .ambiguous_input_aliases
            .push(FrozenInputAliasConflict {
                source_thread: "thread".into(),
                source: copied,
            });
        let mut input_carrier = make("input-carrier", None);
        input_carrier.sources = vec![source("input", "covered")];
        input_carrier.source_aliases = std::mem::take(&mut authoritative.source_aliases);
        let deleted = make("deleted", Some(FrozenEventInputRole::Deleted));
        let input_copy = make("input-copy", Some(FrozenEventInputRole::InputCopy));
        let reference_only = make("reference-only", Some(FrozenEventInputRole::Authoritative));
        let mut inherited = make("inherited", None);
        inherited.sources = vec![source("checkpoint", "inherited")];
        // An exact empty incremental set must suppress inherited aliases.
        inherited.source_aliases = input_carrier.source_aliases.clone();
        inherited.publication_aliases = Some(FrozenPublicationAliases {
            source_aliases: vec![],
            ambiguous_input_aliases: vec![],
        });
        let mut wrong_owner = make("event", Some(FrozenEventInputRole::Deleted));
        wrong_owner.source_thread = "other-thread".into();
        let ambiguous_owner = make("ambiguous-owner", Some(FrozenEventInputRole::Deleted));
        let refs = vec![
            authoritative,
            input_carrier,
            deleted,
            input_copy,
            reference_only,
            inherited,
            wrong_owner,
            ambiguous_owner,
        ];
        let mut ownership = Ownership::new();
        for id in ["event", "deleted", "input-copy"] {
            ownership.insert(source("event", id), BTreeSet::from(["thread".into()]));
        }
        ownership.insert(
            source("input", "covered"),
            BTreeSet::from(["thread".into()]),
        );
        ownership.insert(
            source("checkpoint", "inherited"),
            BTreeSet::from(["thread".into()]),
        );
        for reference in &refs {
            reference.validate().unwrap();
        }
        ownership.insert(
            source("event", "ambiguous-owner"),
            BTreeSet::from(["thread".into(), "other-thread".into()]),
        );
        // Oracle: the original selected extraction at the fixed base, before
        // this change; compare full identities, never just cardinalities.
        let mut original = Metadata::default();
        for reference in &refs {
            let selected = reference
                .sources
                .iter()
                .filter(|source| {
                    ownership.get(*source).is_some_and(|threads| {
                        threads.len() == 1 && threads.contains(&reference.source_thread)
                    })
                })
                .collect::<Vec<_>>();
            for source in selected {
                for edge in reference.publication_edges_for(source) {
                    original.aliases.insert(Alias {
                        covered_thread: edge.covered_thread,
                        replay_thread: edge.replay_thread,
                        covered_scope: edge.covered.scope,
                        covered_id: edge.covered.id,
                        covered_version: edge.covered.version,
                        replay_scope: edge.replay.scope,
                        replay_id: edge.replay.id,
                        replay_version: edge.replay.version,
                        tool_item_id: edge.tool_item_id,
                    });
                }
            }
            if reference.sources.len() == 1
                && let Some(role) = reference.event_input_role
            {
                let source = &reference.sources[0];
                if ownership.get(source).is_some_and(|threads| {
                    threads.len() == 1 && threads.contains(&reference.source_thread)
                }) {
                    original.events.insert(Event {
                        source_thread: reference.source_thread.clone(),
                        source_scope: source.scope.clone(),
                        source_id: source.id.clone(),
                        source_version: source.version.clone(),
                        role: match role {
                            FrozenEventInputRole::Authoritative => "authoritative",
                            FrozenEventInputRole::Deleted => "deleted",
                            FrozenEventInputRole::InputCopy => "input_copy",
                        }
                        .into(),
                    });
                }
            }
        }
        let mut selected = Metadata::default();
        for reference in &refs {
            select_reference(reference, &ownership, &mut selected).unwrap();
        }
        assert_eq!(selected, original);
        assert_eq!(
            selected
                .events
                .iter()
                .map(|e| (e.source_id.as_str(), e.role.as_str()))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                ("event", "authoritative"),
                ("deleted", "deleted"),
                ("input-copy", "input_copy")
            ])
        );
        assert!(
            selected
                .aliases
                .iter()
                .any(|a| a.tool_item_id.as_deref() == Some("tool"))
        );
        assert!(
            selected
                .aliases
                .iter()
                .any(|a| a.covered_id == "copy" && a.replay_id == "copy")
        );
        let contradictory = make("event", Some(FrozenEventInputRole::Deleted));
        assert!(select_reference(&contradictory, &ownership, &mut selected).is_err());
    }

    #[test]
    fn frozen_stream_hashes_keep_their_distinct_existing_length_protocols() {
        let value = b"selected reference";
        let mut messages = Sha256::new();
        messages.update((value.len() as u64).to_be_bytes());
        messages.update(value);
        let mut imports = Sha256::new();
        add_import_to_digest(&mut imports, value);
        assert_ne!(
            hex::encode(messages.finalize()),
            hex::encode(imports.finalize())
        );
        let mut expected = Sha256::new();
        expected.update((value.len() as u64).to_le_bytes());
        expected.update(value);
        let mut actual = Sha256::new();
        add_import_to_digest(&mut actual, value);
        assert_eq!(actual.finalize(), expected.finalize());
    }
    #[test]
    fn cached_full_graph_rejects_lost_ancestry_and_foreign_owner_even_for_empty_metadata() {
        let row = CheckpointEdgesRow {
            coverage_closed: true,
            id: "root".into(),
            proof_version: 0,
            status: "candidate".into(),
            owner: "owner".into(),
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            identity_sha256: "identity".into(),
            previous: Some("missing".into()),
            format_version: 1,
            operation_id: "operation".into(),
            manifest_id: None,
        };
        let mut graph = GraphEvidence {
            row,
            ownership: Ownership::new(),
            metadata: Metadata::default(),
        };
        assert!(validate_cached_graph("root", &graph, &BTreeMap::new()).is_err());
        graph.row.previous = None;
        let source = SourceRef {
            scope: "checkpoint:foreign-owner".into(),
            id: "foreign".into(),
            version: "foreign-identity".into(),
        };
        graph
            .ownership
            .insert(source, BTreeSet::from(["foreign-thread".into()]));
        let mut foreign = graph.clone();
        foreign.row.id = "foreign".into();
        foreign.row.owner = "wrong-owner".into();
        foreign.row.thread_id = "foreign-thread".into();
        foreign.row.identity_sha256 = "foreign-identity".into();
        foreign.ownership.clear();
        assert!(
            validate_cached_graph(
                "root",
                &graph,
                &BTreeMap::from([("foreign".into(), foreign)])
            )
            .is_err()
        );
    }
    #[test]
    fn cached_graph_traverses_deep_previous_and_rejects_cycle_after_valid_tip() {
        let mut cache = BTreeMap::new();
        for index in 0..2048 {
            let row = CheckpointEdgesRow {
                coverage_closed: true,
                id: format!("node-{index}"),
                proof_version: 1,
                status: "retained".into(),
                owner: "owner".into(),
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                identity_sha256: format!("identity-{index}"),
                previous: (index > 0).then(|| format!("node-{}", index - 1)),
                format_version: 1,
                operation_id: "operation".into(),
                manifest_id: None,
            };
            let ownership = if index == 0 {
                BTreeMap::from([(
                    SourceRef {
                        scope: "event:turn".into(),
                        id: "leaf".into(),
                        version: "event-revision:1".into(),
                    },
                    BTreeSet::from(["thread".into()]),
                )])
            } else {
                Ownership::new()
            };
            cache.insert(
                row.id.clone(),
                GraphEvidence {
                    row,
                    ownership,
                    metadata: Metadata::default(),
                },
            );
        }
        let root = cache.remove("node-2047").unwrap();
        assert!(validate_cached_graph(&root.row.id, &root, &cache).unwrap());
        cache.get_mut("node-0").unwrap().row.previous = Some(root.row.id.clone());
        assert!(validate_cached_graph(&root.row.id, &root, &cache).is_err());
        cache.get_mut("node-0").unwrap().row.previous = None;
        cache.remove("node-4");
        assert!(validate_cached_graph(&root.row.id, &root, &cache).is_err());
    }
}
