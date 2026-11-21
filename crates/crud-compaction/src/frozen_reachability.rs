//! Bounded reverse traversal of published checkpoint dependencies.

use anyhow::{Result, ensure};
use pioneer_compaction::SourceRef;
use pioneer_entity::{
    compaction_checkpoint as checkpoint, compaction_context as context,
    compaction_coverage as coverage, compaction_operation as operation,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, JoinType, Order, Query};
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use std::collections::{BTreeSet, VecDeque};

use crate::{
    SOURCE_PAGE_BYTES, proof_graph::CheckpointEdgesRow, proof_publication::checkpoint_row,
};

/// Reverse traversal is paged separately from payload extraction. New
/// publication always seals its ancestry; an unsealed historical public root
/// cannot appear after this traversal via either production publication API.
pub struct Reachability {
    workspace: String,
    pending: VecDeque<(String, bool)>,
    seen: BTreeSet<String>,
    active: BTreeSet<String>,
    current: Option<(String, usize, String, BTreeSet<String>)>,
    reachable: bool,
}
impl Reachability {
    pub fn new(id: &str, workspace: &str) -> Self {
        Self {
            workspace: workspace.into(),
            pending: VecDeque::from([(id.into(), false)]),
            seen: BTreeSet::new(),
            active: BTreeSet::new(),
            current: None,
            reachable: false,
        }
    }
    pub fn done(&self) -> bool {
        self.reachable || (self.current.is_none() && self.pending.is_empty())
    }
    pub fn reachable(&self) -> bool {
        self.reachable
    }

    pub fn workspace(&self) -> &str {
        &self.workspace
    }

    pub async fn step(&mut self, db: &SqliteDatabase) -> Result<()> {
        if self.done() {
            return Ok(());
        }
        if self.current.is_none() {
            let (id, exit) = self.pending.pop_back().expect("reverse pending");
            if exit {
                self.active.remove(&id);
                self.seen.insert(id);
                return Ok(());
            }
            if self.seen.contains(&id) {
                return Ok(());
            }
            ensure!(
                self.active.insert(id.clone()),
                "cyclic reverse checkpoint graph"
            );
            ensure!(
                self.seen.len() + self.active.len() <= 65_536,
                "reverse graph exceeds supported bound"
            );
            let topology = checkpoint_row(db, &id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("reverse checkpoint missing"))?;
            ensure!(
                topology.workspace_id == self.workspace && topology.format_version == 1,
                "reverse graph scope corrupt"
            );
            let published = Query::select()
                .expr(Expr::val(1))
                .from(checkpoint::Entity)
                .join(
                    JoinType::InnerJoin,
                    operation::Entity,
                    Expr::col((operation::Entity, operation::Column::Id))
                        .eq(Expr::col((
                            checkpoint::Entity,
                            checkpoint::Column::OperationId,
                        )))
                        .and(
                            Expr::col((operation::Entity, operation::Column::Owner))
                                .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                        ),
                )
                .and_where(Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(&id))
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                        .eq("applied")
                        .or(Expr::col((checkpoint::Entity, checkpoint::Column::Status))
                            .eq("retained")
                            .and(
                                Expr::col((operation::Entity, operation::Column::Status))
                                    .eq("completed"),
                            )),
                )
                .limit(1)
                .to_owned();
            self.reachable = db.query_one(&published).await?.is_some();
            self.current = Some((id, 0, String::new(), BTreeSet::new()));
            return Ok(());
        }
        let (id, branch, after, children) = self.current.as_mut().expect("reverse current");
        let topology = checkpoint_row(db, id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("reverse checkpoint removed"))?;
        let row = &topology;
        let mut query = Query::select();
        if *branch == 0 {
            query
                .from(checkpoint::Entity)
                .columns(
                    [checkpoint::Column::Id, checkpoint::Column::Owner]
                        .map(|col| (checkpoint::Entity, col)),
                )
                .columns(
                    [context::Column::WorkspaceId, context::Column::ThreadId]
                        .map(|col| (context::Entity, col)),
                )
                .join(
                    JoinType::LeftJoin,
                    context::Entity,
                    Expr::col((context::Entity, context::Column::Owner))
                        .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                )
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Previous)).eq(id.clone()),
                )
                .and_where(
                    Expr::col((checkpoint::Entity, checkpoint::Column::Id)).gt(after.clone()),
                )
                .order_by((checkpoint::Entity, checkpoint::Column::Id), Order::Asc);
        } else {
            query
                .from(coverage::Entity)
                .expr_as(
                    Expr::col((coverage::Entity, coverage::Column::CheckpointId)),
                    Alias::new("id"),
                )
                .columns(
                    [
                        coverage::Column::SourceScope,
                        coverage::Column::SourceVersion,
                    ]
                    .map(|col| (coverage::Entity, col)),
                )
                .column((context::Entity, context::Column::WorkspaceId))
                .join(
                    JoinType::LeftJoin,
                    checkpoint::Entity,
                    Expr::col((checkpoint::Entity, checkpoint::Column::Id)).eq(Expr::col((
                        coverage::Entity,
                        coverage::Column::CheckpointId,
                    ))),
                )
                .join(
                    JoinType::LeftJoin,
                    context::Entity,
                    Expr::col((context::Entity, context::Column::Owner))
                        .eq(Expr::col((checkpoint::Entity, checkpoint::Column::Owner))),
                )
                .and_where(Expr::col((coverage::Entity, coverage::Column::SourceId)).eq(id.clone()))
                .and_where(
                    Expr::col((coverage::Entity, coverage::Column::SourceScope))
                        .like("checkpoint:%"),
                )
                .and_where(
                    Expr::col((coverage::Entity, coverage::Column::CheckpointId)).gt(after.clone()),
                )
                .order_by(
                    (coverage::Entity, coverage::Column::CheckpointId),
                    Order::Asc,
                );
        }
        query.limit(1);
        let page = db.query_all(&query).await?;
        for item in &page {
            let incoming: String = item.try_get("", "id")?;
            ensure!(
                item.try_get::<String>("", "workspace_id")? == self.workspace,
                "incoming checkpoint scope corrupt"
            );
            if *branch == 0 {
                ensure!(
                    item.try_get::<String>("", "owner")? == row.owner
                        && item.try_get::<String>("", "thread_id")? == row.thread_id,
                    "incoming previous owner corrupt"
                );
            } else {
                ensure!(
                    item.try_get::<String>("", "source_scope")?
                        == format!("checkpoint:{}", row.owner)
                        && item.try_get::<String>("", "source_version")? == row.identity_sha256,
                    "incoming foreign checkpoint identity corrupt"
                );
                // Verify selected historical ownership, not current raw lookup.
                // Metadata is bounded by the checkpoint's existing source cap.
                let incoming_topology = checkpoint_row(db, &incoming)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("incoming checkpoint missing"))?;
                let source = pioneer_compaction::SourceRef {
                    scope: format!("checkpoint:{}", row.owner),
                    id: id.clone(),
                    version: row.identity_sha256.clone(),
                };
                ensure!(
                    checkpoint_source_owner(db, &incoming_topology, &source).await?
                        == row.thread_id,
                    "incoming foreign checkpoint ownership corrupt"
                );
            }
            *after = incoming.clone();
            children.insert(incoming);
            ensure!(
                children.len() + self.pending.len() + self.active.len() + self.seen.len() <= 65_536,
                "reverse pending graph exceeds bound"
            );
        }
        if page.is_empty() {
            if *branch == 0 {
                *branch = 1;
                after.clear();
            } else {
                let id = id.clone();
                let children = std::mem::take(children);
                self.current = None;
                self.pending.push_back((id, true));
                self.pending
                    .extend(children.into_iter().map(|id| (id, false)));
            }
        }
        Ok(())
    }
}

/// Reverse readers need exactly one dependency's ownership, not every source.
pub async fn checkpoint_source_owner<C: ConnectionTrait>(
    db: &C,
    row: &CheckpointEdgesRow,
    source: &SourceRef,
) -> Result<String> {
    // SeaORM's Entity projection does not expose a materialized one-row CTE.
    // Two index-ordered scalar lookups avoid scanning every historical owner.
    let owner = |descending: bool| {
        format!(
            "(SELECT m.source_thread FROM compaction_manifest m WHERE m.operation_id=$2 AND m.reference_only=0 AND m.source_scope=$3 AND m.source_id=$4 AND m.source_version=$5 ORDER BY m.source_thread {} LIMIT 1)",
            if descending { "DESC" } else { "ASC" }
        )
    };
    let sql = format!(
        "WITH owners AS MATERIALIZED (SELECT {} AS first,{} AS last WHERE EXISTS(SELECT 1 FROM compaction_coverage WHERE checkpoint_id=$1 AND source_scope=$3 AND source_id=$4 AND source_version=$5)) SELECT first AS source_thread FROM owners WHERE first=last AND octet_length(first) BETWEEN 1 AND $6",
        owner(false),
        owner(true)
    );
    let item = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            &sql,
            [
                row.id.clone().into(),
                row.operation_id.clone().into(),
                source.scope.clone().into(),
                source.id.clone().into(),
                source.version.clone().into(),
                i64::try_from(SOURCE_PAGE_BYTES)?.into(),
            ],
        ))
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("missing, ambiguous or oversized historical source owner")
        })?;
    Ok(item.try_get("", "source_thread")?)
}
