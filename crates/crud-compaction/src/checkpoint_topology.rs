//! Read historical coverage, exact ownership, and identity with bounded pages.
use anyhow::{Result, ensure};
use pioneer_compaction::{REPLAY_ALIAS_LIMIT as CHECKPOINT_SOURCE_LIMIT, SourceRef};
use pioneer_entity::compaction_checkpoint as checkpoint;
use sea_orm::sea_query::{Alias as SqlAlias, Expr, ExprTrait, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, DbBackend, Statement};

use crate::proof_graph::CheckpointEdgesRow;
use crate::proof_publication::{checkpoint_row, refresh_checkpoint_row};
use crate::runner_writes::checkpoint_identity;
use crate::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS};

/// One keyset page, with the byte gate in the payload statement. The cursor
/// includes the full coverage identity (scope/id/version). Ownership is grouped
/// by that same identity only after indexed extrema prove one exact thread.
/// A full checkpoint retains its historical domain bound, not a page byte cap.
pub struct CheckpointTopologyRead {
    pub row: CheckpointEdgesRow,
    coverage: std::collections::BTreeSet<SourceRef>,
    ownership: std::collections::BTreeMap<SourceRef, std::collections::BTreeSet<String>>,
    after: Option<SourceRef>,
    reading_ownership: bool,
    pub done: bool,
    // At most one open -> closed restart. Replayed pages must agree with
    // every previously observed key/owner before accepting the frozen set.
    observed: Option<Box<CheckpointTopologyRead>>,
}
impl CheckpointTopologyRead {
    pub async fn new<C: ConnectionTrait>(db: &C, id: &str) -> Result<Option<Self>> {
        Ok(checkpoint_row(db, id).await?.map(|row| Self {
            row,
            coverage: Default::default(),
            ownership: Default::default(),
            after: None,
            reading_ownership: false,
            done: false,
            observed: None,
        }))
    }
    pub async fn step<C: ConnectionTrait>(&mut self, db: &C) -> Result<()> {
        if self.done {
            return Ok(());
        }
        #[cfg(any(test, feature = "test-support"))]
        crate::proof_test_metrics::record_proof_preparation_check(&self.row.operation_id, true);
        if self.refresh(db).await? {
            return Ok(());
        }
        let rows = db.query_all_raw(self.page_statement()?).await?;
        // Close can occur during the payload statement, including its EOF.
        // Discard that page and restart before exposing a mixed result.
        if self.refresh(db).await? {
            return Ok(());
        }
        // The NULL row distinguishes an exhausted page from an oversized first
        // record without ever returning that record's strings to the process.
        let first = rows
            .first()
            .ok_or_else(|| anyhow::anyhow!("topology page missing size sentinel"))?;
        let count: i64 = first.try_get("", "n")?;
        ensure!(
            first.try_get::<i64>("", "invalid")? == 0,
            "checkpoint coverage lost or has ambiguous historical manifest ownership"
        );
        if first
            .try_get::<Option<String>>("", "source_scope")?
            .is_none()
        {
            ensure!(
                count == 0,
                "checkpoint single topology record exceeds byte quantum"
            );
            if let Some(observed) = &self.observed {
                ensure!(
                    if self.reading_ownership {
                        observed.ownership.is_empty()
                    } else {
                        observed.coverage.is_empty()
                    },
                    "checkpoint lost previously observed topology at close"
                );
            }
            if !self.reading_ownership {
                self.reading_ownership = true;
                self.after = None;
            } else {
                ensure!(
                    self.ownership.keys().eq(self.coverage.iter())
                        && self.ownership.values().all(|owners| owners.len() == 1),
                    "checkpoint coverage lost its historical manifest ownership"
                );
                self.done = true;
            }
            return Ok(());
        }
        let page_rows = rows.len();
        let mut loaded = 0_usize;
        for item in rows {
            let source = SourceRef {
                scope: item.try_get("", "source_scope")?,
                id: item.try_get("", "source_id")?,
                version: item.try_get("", "source_version")?,
            };
            let thread: String = item.try_get("", "source_thread")?;
            loaded += source.scope.len() + source.id.len() + source.version.len() + thread.len();
            ensure!(
                !source.scope.is_empty() && !source.id.is_empty() && !source.version.is_empty(),
                "invalid checkpoint coverage identity"
            );
            if let Some(observed) = &mut self.observed {
                if self.reading_ownership {
                    // Ownership observations precede the original full cursor.
                    if observed.after.as_ref().is_some_and(|last| &source <= last)
                        && observed.reading_ownership
                    {
                        ensure!(
                            observed.ownership.remove(&source)
                                == Some(std::collections::BTreeSet::from([thread.clone()])),
                            "checkpoint historical owner changed at close"
                        );
                    }
                } else if observed.reading_ownership
                    || observed.coverage.last().is_some_and(|last| &source <= last)
                {
                    ensure!(
                        observed.coverage.remove(&source),
                        "checkpoint coverage changed at close"
                    );
                }
            }
            if self.reading_ownership {
                ensure!(!thread.is_empty(), "invalid historical source thread");
                let owners = self.ownership.entry(source.clone()).or_default();
                owners.insert(thread.clone());
                ensure!(owners.len() == 1, "ambiguous historical manifest ownership");
            } else {
                ensure!(
                    self.coverage.insert(source.clone()),
                    "duplicate checkpoint coverage identity"
                );
            }
            ensure!(
                self.coverage.len() <= CHECKPOINT_SOURCE_LIMIT
                    && self.ownership.len() <= CHECKPOINT_SOURCE_LIMIT,
                "checkpoint exceeds supported source domain"
            );
            self.after = Some(source);
        }
        ensure!(
            page_rows <= SOURCE_PAGE_ROWS as usize && loaded <= SOURCE_PAGE_BYTES,
            "topology SQL page exceeded its payload quantum"
        );
        #[cfg(any(test, feature = "test-support"))]
        crate::proof_test_metrics::record_topology_page(&self.row.operation_id, page_rows, loaded);
        Ok(())
    }
    async fn refresh<C: ConnectionTrait>(&mut self, db: &C) -> Result<bool> {
        let current = refresh_checkpoint_row(db, &self.row).await?;
        if !self.row.coverage_closed && current.coverage_closed {
            ensure!(self.observed.is_none(), "repeated checkpoint close");
            let old = std::mem::replace(
                self,
                Self {
                    row: current,
                    coverage: Default::default(),
                    ownership: Default::default(),
                    after: None,
                    reading_ownership: false,
                    done: false,
                    observed: None,
                },
            );
            self.observed = Some(Box::new(old));
            return Ok(true);
        }
        self.row = current;
        Ok(false)
    }

    pub fn page_statement(&self) -> Result<Statement> {
        let (scope, id, version) = self
            .after
            .as_ref()
            .map(|s| {
                (
                    Some(s.scope.as_str()),
                    Some(s.id.as_str()),
                    Some(s.version.as_str()),
                )
            })
            .unwrap_or_default();
        // Seek coverage keys first, before ownership lookup. Two indexed
        // thread extrema prove the sole owner, including duplicates at other
        // ordinals, without a checkpoint-wide DISTINCT join or sort.
        let coverage = "SELECT source_scope,source_id,source_version FROM compaction_coverage
            WHERE checkpoint_id=$1 AND $2::text IS NOT NULL
              AND ($3::text IS NULL OR (source_scope,source_id,source_version)>($3,$4,$5))
            ORDER BY source_scope,source_id,source_version LIMIT $6";
        let owner = |descending: bool| {
            format!(
                "(SELECT m.source_thread FROM compaction_manifest m WHERE m.operation_id=$2 AND m.reference_only=0 AND m.source_scope=v.source_scope COLLATE \"default\" AND m.source_id=v.source_id COLLATE \"default\" AND m.source_version=v.source_version COLLATE \"default\" ORDER BY m.source_thread {} LIMIT 1)",
                if descending { "DESC" } else { "ASC" }
            )
        };
        let ownership = if self.reading_ownership {
            format!(
                "SELECT v.*,{0} AS source_thread,{1} AS last_thread FROM coverage_page v",
                owner(false),
                owner(true)
            )
        } else {
            "SELECT v.*,'' AS source_thread,'' AS last_thread FROM coverage_page v".into()
        };
        let sql = format!(
            "WITH coverage_page AS MATERIALIZED ({coverage}), owners AS MATERIALIZED ({ownership}), page AS MATERIALIZED (SELECT *,octet_length(source_scope)::bigint+octet_length(source_id)::bigint+octet_length(source_version)::bigint+octet_length(source_thread)::bigint AS bytes FROM owners), budget AS (SELECT *,sum(bytes) OVER (ORDER BY source_scope,source_id,source_version,source_thread ROWS UNBOUNDED PRECEDING) AS used FROM page), stats AS (SELECT count(*) AS n,count(*) FILTER (WHERE source_thread IS NULL OR last_thread IS NULL OR source_thread<>last_thread) AS invalid FROM page) SELECT b.source_scope,b.source_id,b.source_version,b.source_thread,stats.n,stats.invalid FROM stats LEFT JOIN budget b ON stats.invalid=0 AND b.used<=$7 ORDER BY b.source_scope,b.source_id,b.source_version,b.source_thread"
        );
        Ok(Statement::from_sql_and_values(
            DbBackend::Postgres,
            &sql,
            vec![
                self.row.id.clone().into(),
                self.row.operation_id.clone().into(),
                scope.into(),
                id.into(),
                version.into(),
                i64::try_from(SOURCE_PAGE_ROWS)?.into(),
                i64::try_from(SOURCE_PAGE_BYTES)?.into(),
            ],
        ))
    }
    pub fn finish(self) -> Result<CheckpointTopology> {
        ensure!(self.done, "checkpoint topology read incomplete");
        Ok(CheckpointTopology {
            row: self.row,
            ownership: self.ownership,
        })
    }
}

pub async fn checkpoint_coverage<C: ConnectionTrait>(db: &C, id: &str) -> Result<Vec<SourceRef>> {
    let mut read = CheckpointTopologyRead::new(db, id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint coverage identity missing"))?;
    // Coverage-only callers do not need manifest ownership. Each read releases
    // capacity; maintenance uses step directly instead of this foreground loop.
    while !read.reading_ownership {
        read.step(db).await?;
    }
    Ok(read.coverage.into_iter().collect())
}

/// Coverage discovery must not read summary text before source authorization.
#[derive(Clone)]
pub struct CheckpointTopology {
    pub row: CheckpointEdgesRow,
    pub ownership: std::collections::BTreeMap<SourceRef, std::collections::BTreeSet<String>>,
}

/// Historical coverage and ownership lookup does not read frozen input bytes.
pub async fn checkpoint_topology<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CheckpointTopology>> {
    let Some(mut read) = CheckpointTopologyRead::new(db, id).await? else {
        return Ok(None);
    };
    while !read.done {
        read.step(db).await?;
    }
    Ok(Some(read.finish()?))
}

pub struct CheckpointIdentityRead {
    topology: CheckpointTopology,
    selection: String,
    projection_version: i64,
    size: i64,
    bytes: Vec<u8>,
    done: bool,
}
impl CheckpointIdentityRead {
    pub fn verified_topology(&self) -> Option<&CheckpointTopology> {
        self.done.then_some(&self.topology)
    }

    pub async fn new<C: ConnectionTrait>(db: &C, topology: CheckpointTopology) -> Result<Self> {
        let meta = db
            .query_one(
                &Query::select()
                    .from(checkpoint::Entity)
                    .column(checkpoint::Column::ProjectionVersion)
                    .expr_as(
                        Expr::col(checkpoint::Column::Selection).cast_as("text"),
                        "selection",
                    )
                    .expr_as(
                        Expr::cust("octet_length(summary)::bigint"),
                        SqlAlias::new("size"),
                    )
                    .and_where(checkpoint::Column::Id.eq(&topology.row.id))
                    .and_where(
                        Expr::cust("octet_length(selection::text)::bigint")
                            .lte(i64::try_from(SOURCE_PAGE_BYTES)?),
                    )
                    .limit(1)
                    .to_owned(),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint identity metadata missing or oversized"))?;
        let size: i64 = meta.try_get("", "size")?;
        ensure!(
            (1..=13_107 * 128).contains(&size),
            "checkpoint summary size invalid"
        );
        Ok(Self {
            topology,
            selection: meta.try_get("", "selection")?,
            projection_version: meta.try_get("", "projection_version")?,
            size,
            bytes: Vec::new(),
            done: false,
        })
    }
    pub async fn step<C: ConnectionTrait>(&mut self, db: &C) -> Result<()> {
        if self.done {
            return Ok(());
        }
        let row = &self.topology.row;
        if (self.bytes.len() as i64) < self.size {
            // SeaQuery lacks a byte-range function for UTF-8 text. Filter
            // by the saved identity and byte length before returning a fragment.
            let page = db.query_one_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "SELECT substr(convert_to(summary, 'UTF8'), $2::bigint::int, $3::bigint::int) AS fragment
                 FROM compaction_checkpoint WHERE id=$1 AND identity_sha256=$4
                 AND octet_length(summary)=$5 LIMIT 1",
                [row.id.clone().into(), (self.bytes.len() as i64+1).into(),
                 i64::try_from(SOURCE_PAGE_BYTES)?.into(), row.identity_sha256.clone().into(), self.size.into()],
            )).await?.ok_or_else(|| anyhow::anyhow!("checkpoint summary identity changed"))?;
            let fragment: Vec<u8> = page.try_get("", "fragment")?;
            ensure!(
                !fragment.is_empty() && fragment.len() <= SOURCE_PAGE_BYTES,
                "checkpoint summary fragment missing"
            );
            self.bytes.extend(fragment);
            return Ok(());
        }
        ensure!(
            self.bytes.len() as i64 == self.size,
            "checkpoint summary size mismatch"
        );
        let checkpoint = pioneer_compaction::Checkpoint {
            id: row.id.clone(),
            operation_id: row.operation_id.clone(),
            owner: row.owner.clone(),
            previous: row.previous.clone(),
            summary: String::from_utf8(std::mem::take(&mut self.bytes))?,
            selection: serde_json::from_str(&self.selection)?,
            coverage: self.topology.ownership.keys().cloned().collect(),
            projection_version: u64::try_from(self.projection_version)?,
            format_version: u32::try_from(row.format_version)?,
        };
        ensure!(
            checkpoint_identity(&checkpoint)? == row.identity_sha256,
            "checkpoint exact coverage/identity mismatch"
        );
        self.done = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_test_support::explain;

    #[pioneer_postgres_test::postgres_test]
    async fn dense_selected_ownership_page_plan_seeks_thread_extrema_without_duplicate_scan() {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection();
        db.execute_unprepared(include_str!(
            "../tests/fixtures/checkpoint_topology_plan.sql"
        ))
        .await
        .unwrap();
        // Many duplicate ordinals must not make a single ownership lookup a full
        // range DISTINCT/sort. The sole thread is checked by indexed extrema.
        db.execute_unprepared("WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<2047) INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) SELECT 'topology-plan',1000+i,1000+i,0,'root-thread','event:root-turn','topology-plan-source-0','event-revision:1' FROM n").await.unwrap();
        db.execute_unprepared("ANALYZE compaction_manifest; ANALYZE compaction_coverage")
            .await
            .unwrap();
        let mut read = CheckpointTopologyRead::new(db, "topology-plan-checkpoint")
            .await
            .unwrap()
            .unwrap();
        // Three steps exhaust coverage and enter ownership while retaining progress.
        for _ in 0..3 {
            read.step(db).await.unwrap();
        }
        let plan = explain(db, read.page_statement().unwrap()).await;
        let nodes = plan.walk();
        let seeks = nodes
            .iter()
            .filter(|node| {
                node.index.as_deref() == Some("compaction_manifest_source")
                    && [
                        "operation_id",
                        "reference_only",
                        "source_scope",
                        "source_id",
                        "source_version",
                    ]
                    .into_iter()
                    .all(|column| node.has_index_constraint(column, "="))
            })
            .count();
        assert_eq!(
            seeks, 2,
            "both ascending and descending owner extrema need exact indexed bounds: {plan:#?}"
        );
        assert!(
            !nodes.iter().any(|node| {
                node.alias
                    .as_deref()
                    .is_some_and(|alias| alias == "m" || alias.starts_with("m_"))
                    && node.kind == "Seq Scan"
            }),
            "no duplicate-range scan: {plan:#?}"
        );
        while !read.done {
            read.step(db).await.unwrap();
        }
        assert_eq!(read.finish().unwrap().ownership.len(), 130);
    }
}
