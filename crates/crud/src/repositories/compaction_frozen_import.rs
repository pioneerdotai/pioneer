//! Immutable own-import evidence attached to the newly admitted frozen context.
//! Preparation decodes reference metadata outside database capacity. Publication
//! is fenced by the manifest's declared import count and independent digest.
use super::compaction::*;
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_compaction::frozen::FrozenMessageRef;
use pioneer_compaction::{FORMAT_VERSION, SourceRef};
use pioneer_entity::{
    compaction_delivery_output, compaction_event_revision, compaction_frozen_history,
    compaction_frozen_import, compaction_frozen_message, compaction_task_output, task_delivery,
    turn_event,
};
use sea_orm::sea_query::{Alias, BinOper, Expr, ExprTrait, JoinType, OnConflict};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use sea_orm::{ConnectionTrait, TransactionTrait};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub const EMPTY_FROZEN_IMPORT_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
// A proof compares two reference records, each individually <=256 KiB, plus
// fixed provenance fields. This is metadata, never a transcript payload.
pub const FROZEN_IMPORT_PAGE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FrozenImportRecord {
    pub message_ordinal: u64,
    pub source_thread: String,
    pub source: SourceRef,
    pub delivery_id: String,
    pub candidate_id: String,
    pub output_manifest: String,
    pub output_ordinal: u64,
    pub acknowledgement: SourceRef,
}

/// Fields are private: callers cannot mint an own claim from a delivery ID.
#[derive(Clone, Debug)]
pub struct PreparedFrozenImport {
    workspace: String,
    destination: String,
    output_digest: String,
    original_json: String,
    record: FrozenImportRecord,
}
impl PreparedFrozenImport {
    pub fn source(&self) -> &SourceRef {
        &self.record.source
    }
    pub fn source_thread(&self) -> &str {
        &self.record.source_thread
    }
    pub fn estimated_write_bytes(&self, target: &FrozenMessageRef) -> Result<usize> {
        Ok(self.original_json.len()
            + serde_json::to_vec(&self.record)?.len()
            + serde_json::to_vec(target)?.len()
            + 64)
    }
    pub(crate) fn record_at(&self, message: u64) -> FrozenImportRecord {
        let mut record = self.record.clone();
        record.message_ordinal = message;
        record
    }
}

pub fn frozen_import_identity(imports: &[(u64, PreparedFrozenImport)]) -> Result<String> {
    let mut digest = Sha256::new();
    for (message, prepared) in imports {
        let bytes = serde_json::to_vec(&prepared.record_at(*message))?;
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    Ok(hex::encode(digest.finalize()))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compaction_prepare_frozen_import(
    store: &CrudStore,
    workspace: &str,
    destination: &str,
    delivery: &str,
    acknowledgement: &SourceRef,
    output_ordinal: u64,
    source_thread: &str,
    source: &SourceRef,
) -> Result<PreparedFrozenImport> {
    let snapshot = store
        .compaction_delivery_output(workspace, delivery)
        .await?
        .ok_or_else(|| anyhow::anyhow!("accepted output binding is unavailable"))?;
    let acknowledged = task_delivery::Entity::find_by_id(delivery)
        .select_only()
        .column(task_delivery::Column::Id)
        .join(
            JoinType::InnerJoin,
            task_delivery::Entity::belongs_to(turn_event::Entity)
                .from(task_delivery::Column::DeliveredTurnId)
                .to(turn_event::Column::TurnId)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn_event::Entity::belongs_to(compaction_event_revision::Entity)
                .from(turn_event::Column::Id)
                .to(compaction_event_revision::Column::SourceId)
                .into(),
        )
        .filter(task_delivery::Column::WorkspaceId.eq(workspace))
        .filter(task_delivery::Column::TargetThreadId.eq(destination))
        .filter(task_delivery::Column::Status.eq("delivered"))
        .filter(
            Expr::col((turn_event::Entity, turn_event::Column::ThreadId)).eq(Expr::col((
                task_delivery::Entity,
                task_delivery::Column::TargetThreadId,
            ))),
        )
        .filter(
            Expr::col((
                compaction_event_revision::Entity,
                compaction_event_revision::Column::TurnId,
            ))
            .eq(Expr::col((turn_event::Entity, turn_event::Column::TurnId))),
        )
        .filter(compaction_event_revision::Column::Present.eq(1_i64))
        .filter(
            Expr::col((
                compaction_event_revision::Entity,
                compaction_event_revision::Column::ProjectionRevision,
            ))
            .eq(Expr::col((
                compaction_event_revision::Entity,
                compaction_event_revision::Column::Revision,
            ))),
        )
        .filter(turn_event::Column::Id.eq(acknowledgement.id.clone()))
        .filter(
            Expr::val("event:")
                .binary(
                    BinOper::Custom("||"),
                    Expr::col((turn_event::Entity, turn_event::Column::TurnId)),
                )
                .eq(acknowledgement.scope.clone()),
        )
        .filter(
            Expr::val("event-revision:")
                .binary(
                    BinOper::Custom("||"),
                    Expr::col((
                        compaction_event_revision::Entity,
                        compaction_event_revision::Column::Revision,
                    )),
                )
                .eq(acknowledgement.version.clone()),
        )
        .filter(
            turn_event::Column::EventType.eq(pioneer_protocol::constants::events::ITEM_COMPLETED),
        )
        .filter(
            compaction_event_revision::Column::ItemId
                .eq(pioneer_protocol::task_delivery_result_item_id(delivery)),
        )
        .into_tuple::<String>()
        .one(&store.connection)
        .await?
        .is_some();
    ensure!(
        acknowledged,
        "output has no exact acknowledged destination binding"
    );
    let original_json = compaction_frozen_message::Entity::find()
        .inner_join(compaction_frozen_history::Entity)
        .select_only()
        .column(compaction_frozen_message::Column::ReferenceJson)
        .filter(
            compaction_frozen_history::Column::Id.eq(snapshot.output.history.manifest_id.clone()),
        )
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(
            compaction_frozen_history::Column::OwnerThread
                .eq(snapshot.output.source_thread.clone()),
        )
        .filter(compaction_frozen_history::Column::Ready.eq(1_i64))
        .filter(
            compaction_frozen_history::Column::IdentitySha256.eq(snapshot
                .output
                .history
                .identity_sha256
                .clone()),
        )
        .filter(compaction_frozen_message::Column::Ordinal.eq(i64::try_from(output_ordinal)?))
        .filter(compaction_frozen_message::Column::Bytes.lte(SOURCE_PAGE_BYTES as i64))
        .into_tuple::<String>()
        .one(&store.connection)
        .await?
        .ok_or_else(|| anyhow::anyhow!("output origin reference is unavailable"))?;
    let original: FrozenMessageRef = serde_json::from_str(&original_json)?;
    original.validate()?;
    ensure!(
        !original.inherited
            && original.complete
            && !original.protected_input
            && original
                .context_thread
                .as_deref()
                .unwrap_or(&original.source_thread)
                == snapshot.output.source_thread,
        "inherited or unfinished work cannot become accepted own work"
    );
    // Prove membership through immutable checkpoint references, not summary
    // text. Each metadata query releases its reader before graph traversal.
    let mut pending = original.sources.clone();
    let mut visited = BTreeSet::new();
    let mut found = false;
    while let Some(reference) = pending.pop() {
        if !visited.insert(reference.clone()) {
            continue;
        }
        let thread = store
            .compaction_reference_thread(workspace, &reference)
            .await?
            .ok_or_else(|| anyhow::anyhow!("output coverage source changed"))?;
        if &reference == source && thread == source_thread {
            found = true;
            break;
        }
        if let Some(owner) = reference.scope.strip_prefix("checkpoint:") {
            let checkpoint = store
                .compaction_checkpoint_edges(&reference.id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("output coverage checkpoint disappeared"))?;
            ensure!(
                checkpoint.owner == owner && checkpoint.format_version == FORMAT_VERSION,
                "output checkpoint identity mismatch"
            );
            if let Some(previous) = checkpoint.previous {
                pending.push(
                    store
                        .compaction_checkpoint_source(workspace, &thread, &previous)
                        .await?
                        .ok_or_else(|| {
                            anyhow::anyhow!("output checkpoint ancestry is unavailable")
                        })?,
                );
            }
            pending.extend(checkpoint.coverage);
        }
    }
    ensure!(found, "source is outside the accepted own output coverage");
    Ok(PreparedFrozenImport {
        workspace: workspace.into(),
        destination: destination.into(),
        output_digest: snapshot.output.history.identity_sha256,
        original_json,
        record: FrozenImportRecord {
            message_ordinal: 0,
            source_thread: source_thread.into(),
            source: source.clone(),
            delivery_id: delivery.into(),
            candidate_id: snapshot.candidate_id,
            output_manifest: snapshot.output.history.manifest_id,
            output_ordinal,
            acknowledgement: acknowledgement.clone(),
        },
    })
}

pub(crate) async fn compaction_append_frozen_imports(
    store: &CrudStore,
    workspace: &str,
    owner: &str,
    manifest: &str,
    start: u64,
    imports: &[(u64, PreparedFrozenImport)],
) -> Result<()> {
    ensure!(
        imports.len() as u64 <= SOURCE_PAGE_ROWS,
        "import batch row limit"
    );
    let mut batch = Vec::new();
    let mut bytes = 0;
    for (index, (message, prepared)) in imports.iter().enumerate() {
        ensure!(
            prepared.workspace == workspace && prepared.destination == owner,
            "import preparation scope mismatch"
        );
        let target_json = compaction_frozen_message::Entity::find()
            .inner_join(compaction_frozen_history::Entity)
            .select_only()
            .column(compaction_frozen_message::Column::ReferenceJson)
            .filter(compaction_frozen_history::Column::Id.eq(manifest))
            .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
            .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
            .filter(compaction_frozen_message::Column::Ordinal.eq(i64::try_from(*message)?))
            .filter(compaction_frozen_message::Column::Bytes.lte(SOURCE_PAGE_BYTES as i64))
            .into_tuple::<String>()
            .one(&store.connection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("target reference is unavailable"))?;
        let target: FrozenMessageRef = serde_json::from_str(&target_json)?;
        target.validate()?;
        ensure!(
            !target.inherited
                && target.complete
                && !target.protected_input
                && target
                    .context_thread
                    .as_deref()
                    .unwrap_or(&target.source_thread)
                    == owner
                && target.source_thread == prepared.record.source_thread
                && target.sources.contains(&prepared.record.source),
            "target message is not this accepted own projection"
        );
        let record = prepared.record_at(*message);
        let json = serde_json::to_string(&record)?;
        bytes += target_json.len() + prepared.original_json.len() + json.len();
        ensure!(
            bytes <= FROZEN_IMPORT_PAGE_BYTES && json.len() <= SOURCE_PAGE_BYTES,
            "import batch byte limit"
        );
        batch.push((
            i64::try_from(
                start
                    .checked_add(index as u64)
                    .ok_or_else(|| anyhow::anyhow!("import ordinal overflow"))?,
            )?,
            record,
            prepared,
            target_json,
            json,
        ));
    }
    let tx = store.connection.begin().await?;
    let row = compaction_frozen_history::Entity::find_by_id(manifest)
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
        .one(&tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("import target manifest is unavailable"))?;
    let ready = row.ready != 0;
    let count = row.import_count;
    let next = row.next_import;
    let start = i64::try_from(start)?;
    let end = start
        .checked_add(i64::try_from(batch.len())?)
        .ok_or_else(|| anyhow::anyhow!("import ordinal overflow"))?;
    ensure!(
        start <= next && end <= count && (start == next || end <= next),
        "imports are not a sequential batch or exact retry"
    );
    for (ordinal, record, prepared, target_json, json) in &batch {
        if !ready && *ordinal >= next {
            // The existing transaction holds the validated dependency snapshot
            // through insertion; exact retries below retain their prior behavior.
            if compaction_frozen_history::Entity::find()
                .select_only()
                .join_as(
                    JoinType::InnerJoin,
                    compaction_frozen_history::Entity::belongs_to(
                        compaction_frozen_message::Entity,
                    )
                    .from(compaction_frozen_history::Column::Id)
                    .to(compaction_frozen_message::Column::ManifestId)
                    .into(),
                    Alias::new("target"),
                )
                .join(
                    JoinType::InnerJoin,
                    compaction_frozen_history::Entity::belongs_to(task_delivery::Entity)
                        .from(compaction_frozen_history::Column::WorkspaceId)
                        .to(task_delivery::Column::WorkspaceId)
                        .into(),
                )
                .join(
                    JoinType::InnerJoin,
                    task_delivery::Entity::belongs_to(compaction_delivery_output::Entity)
                        .from(task_delivery::Column::Id)
                        .to(compaction_delivery_output::Column::DeliveryId)
                        .into(),
                )
                .join(
                    JoinType::InnerJoin,
                    compaction_delivery_output::Entity::belongs_to(compaction_task_output::Entity)
                        .from(compaction_delivery_output::Column::TaskRunTurnId)
                        .to(compaction_task_output::Column::TaskRunTurnId)
                        .into(),
                )
                .join_as(
                    JoinType::InnerJoin,
                    compaction_task_output::Entity::belongs_to(compaction_frozen_history::Entity)
                        .from(compaction_task_output::Column::ManifestId)
                        .to(compaction_frozen_history::Column::Id)
                        .into(),
                    Alias::new("original"),
                )
                .join_as(
                    JoinType::InnerJoin,
                    sea_orm::RelationDef::from(
                        compaction_frozen_history::Entity::belongs_to(
                            compaction_frozen_message::Entity,
                        )
                        .from(compaction_frozen_history::Column::Id)
                        .to(compaction_frozen_message::Column::ManifestId),
                    )
                    .from_alias(Alias::new("original")),
                    Alias::new("origin"),
                )
                .join(
                    JoinType::InnerJoin,
                    task_delivery::Entity::belongs_to(turn_event::Entity)
                        .from(task_delivery::Column::TargetThreadId)
                        .to(turn_event::Column::ThreadId)
                        .into(),
                )
                .join(
                    JoinType::InnerJoin,
                    turn_event::Entity::belongs_to(compaction_event_revision::Entity)
                        .from(turn_event::Column::Id)
                        .to(compaction_event_revision::Column::SourceId)
                        .into(),
                )
                .join(
                    JoinType::InnerJoin,
                    compaction_live_sources::join(
                        compaction_frozen_history::Entity,
                        compaction_frozen_history::Column::WorkspaceId,
                        compaction_live_sources::Column::WorkspaceId,
                    ),
                )
                .filter(
                    Expr::col(("target", compaction_frozen_message::Column::Ordinal))
                        .eq(Expr::Value(i64::try_from(record.message_ordinal)?.into())),
                )
                .filter(
                    Expr::col(("target", compaction_frozen_message::Column::ReferenceJson))
                        .eq(Expr::Value(target_json.clone().into())),
                )
                .filter(
                    Expr::col((task_delivery::Entity, task_delivery::Column::Id))
                        .eq(Expr::Value(record.delivery_id.clone().into())),
                )
                .filter(
                    Expr::col((task_delivery::Entity, task_delivery::Column::TargetThreadId)).eq(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::OwnerThread,
                        )),
                    ),
                )
                .filter(
                    Expr::col((task_delivery::Entity, task_delivery::Column::Status))
                        .eq(Expr::val("delivered")),
                )
                .filter(
                    Expr::col((
                        compaction_delivery_output::Entity,
                        compaction_delivery_output::Column::CandidateId,
                    ))
                    .eq(Expr::Value(record.candidate_id.clone().into())),
                )
                .filter(
                    Expr::col((
                        compaction_task_output::Entity,
                        compaction_task_output::Column::TaskId,
                    ))
                    .eq(Expr::col((
                        task_delivery::Entity,
                        task_delivery::Column::TaskId,
                    ))),
                )
                .filter(
                    Expr::col((
                        compaction_task_output::Entity,
                        compaction_task_output::Column::RunId,
                    ))
                    .eq(Expr::col((
                        task_delivery::Entity,
                        task_delivery::Column::RunId,
                    ))),
                )
                .filter(
                    Expr::col((
                        compaction_task_output::Entity,
                        compaction_task_output::Column::WorkspaceId,
                    ))
                    .eq(Expr::col((
                        task_delivery::Entity,
                        task_delivery::Column::WorkspaceId,
                    ))),
                )
                .filter(
                    Expr::col((
                        compaction_task_output::Entity,
                        compaction_task_output::Column::ManifestId,
                    ))
                    .eq(Expr::Value(record.output_manifest.clone().into())),
                )
                .filter(
                    Expr::col(("original", compaction_frozen_history::Column::Ready))
                        .eq(Expr::val(1_i64)),
                )
                .filter(
                    Expr::col((
                        "original",
                        compaction_frozen_history::Column::IdentitySha256,
                    ))
                    .eq(Expr::Value(prepared.output_digest.clone().into())),
                )
                .filter(
                    Expr::col(("origin", compaction_frozen_message::Column::Ordinal))
                        .eq(Expr::Value(i64::try_from(record.output_ordinal)?.into())),
                )
                .filter(
                    Expr::col(("origin", compaction_frozen_message::Column::ReferenceJson))
                        .eq(Expr::Value(prepared.original_json.clone().into())),
                )
                .filter(
                    Expr::col((turn_event::Entity, turn_event::Column::Id))
                        .eq(Expr::Value(record.acknowledgement.id.clone().into())),
                )
                .filter(
                    Expr::col((turn_event::Entity, turn_event::Column::TurnId)).eq(Expr::col((
                        task_delivery::Entity,
                        task_delivery::Column::DeliveredTurnId,
                    ))),
                )
                .filter(
                    Expr::col((
                        compaction_event_revision::Entity,
                        compaction_event_revision::Column::TurnId,
                    ))
                    .eq(Expr::col((turn_event::Entity, turn_event::Column::TurnId))),
                )
                .filter(
                    Expr::col((
                        compaction_event_revision::Entity,
                        compaction_event_revision::Column::Present,
                    ))
                    .eq(Expr::val(1_i64)),
                )
                .filter(
                    Expr::col((
                        compaction_event_revision::Entity,
                        compaction_event_revision::Column::ProjectionRevision,
                    ))
                    .eq(Expr::col((
                        compaction_event_revision::Entity,
                        compaction_event_revision::Column::Revision,
                    ))),
                )
                .filter(
                    Expr::col((
                        compaction_live_sources::Column::Table,
                        compaction_live_sources::Column::SourceScope,
                    ))
                    .eq(Expr::Value(record.source.scope.clone().into())),
                )
                .filter(
                    Expr::col((
                        compaction_live_sources::Column::Table,
                        compaction_live_sources::Column::SourceId,
                    ))
                    .eq(Expr::Value(record.source.id.clone().into())),
                )
                .filter(
                    Expr::col((
                        compaction_live_sources::Column::Table,
                        compaction_live_sources::Column::SourceVersion,
                    ))
                    .eq(Expr::Value(record.source.version.clone().into())),
                )
                .filter(
                    Expr::col((
                        compaction_live_sources::Column::Table,
                        compaction_live_sources::Column::ThreadId,
                    ))
                    .eq(Expr::Value(record.source_thread.clone().into())),
                )
                .expr(Expr::col((
                    compaction_frozen_history::Entity,
                    compaction_frozen_history::Column::Id,
                )))
                .filter(
                    Expr::col((
                        compaction_frozen_history::Entity,
                        compaction_frozen_history::Column::Id,
                    ))
                    .eq(Expr::Value(manifest.into()))
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::WorkspaceId,
                        ))
                        .eq(Expr::Value(workspace.into())),
                    )
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::OwnerThread,
                        ))
                        .eq(Expr::Value(owner.into())),
                    )
                    .and(
                        Expr::col((
                            compaction_frozen_history::Entity,
                            compaction_frozen_history::Column::Ready,
                        ))
                        .eq(Expr::val(0_i64)),
                    )
                    .and(
                        Expr::val("event:")
                            .binary(
                                BinOper::Custom("||"),
                                Expr::col((turn_event::Entity, turn_event::Column::TurnId)),
                            )
                            .eq(Expr::Value(record.acknowledgement.scope.clone().into())),
                    )
                    .and(
                        Expr::val("event-revision:")
                            .binary(
                                BinOper::Custom("||"),
                                Expr::col((
                                    compaction_event_revision::Entity,
                                    compaction_event_revision::Column::Revision,
                                )),
                            )
                            .eq(Expr::Value(record.acknowledgement.version.clone().into())),
                    )
                    .and(
                        Expr::col((turn_event::Entity, turn_event::Column::EventType)).eq(
                            Expr::Value(pioneer_protocol::constants::events::ITEM_COMPLETED.into()),
                        ),
                    )
                    .and(
                        Expr::col((
                            compaction_event_revision::Entity,
                            compaction_event_revision::Column::ItemId,
                        ))
                        .eq(Expr::Value(
                            pioneer_protocol::task_delivery_result_item_id(&record.delivery_id)
                                .into(),
                        )),
                    ),
                )
                .into_tuple::<String>()
                .one(&tx)
                .await?
                .is_some()
            {
                let source =
                    super::compaction_frozen_storage::append_source(&tx, manifest, 1, *ordinal)
                        .await?;
                pioneer_entity::compaction_frozen_import_data::Entity::insert(
                    pioneer_entity::compaction_frozen_import_data::ActiveModel {
                        manifest_id: sea_orm::Set(source),
                        ordinal: sea_orm::Set(*ordinal),
                        message_ordinal: sea_orm::Set(i64::try_from(record.message_ordinal)?),
                        source_scope: sea_orm::Set(record.source.scope.clone()),
                        source_id: sea_orm::Set(record.source.id.clone()),
                        source_version: sea_orm::Set(record.source.version.clone()),
                        source_thread: sea_orm::Set(record.source_thread.clone()),
                        proof_json: sea_orm::Set(json.clone()),
                        bytes: sea_orm::Set(i64::try_from(json.len())?),
                    },
                )
                .on_conflict(
                    OnConflict::columns([
                        compaction_frozen_import::Column::ManifestId,
                        compaction_frozen_import::Column::Ordinal,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .exec_without_returning(&tx)
                .await?;
            }
        }
        ensure!(
            compaction_frozen_import::Entity::find()
                .select_only()
                .expr(Expr::val(1_i64))
                .filter(
                    Expr::col(compaction_frozen_import::Column::ManifestId)
                        .eq(Expr::Value(manifest.into()))
                        .and(
                            Expr::col(compaction_frozen_import::Column::Ordinal)
                                .eq(Expr::Value((*ordinal).into()))
                        )
                        .and(
                            Expr::col(compaction_frozen_import::Column::ProofJson)
                                .eq(Expr::Value(json.clone().into()))
                        )
                )
                .into_tuple::<i64>()
                .one(&tx)
                .await?
                .is_some(),
            "import binding changed or retry changed immutable metadata"
        );
    }
    if start == next && !ready {
        compaction_frozen_history::Entity::update_many()
            .col_expr(
                compaction_frozen_history::Column::NextImport,
                Expr::Value(end.into()),
            )
            .filter(
                Expr::col(compaction_frozen_history::Column::Id)
                    .eq(Expr::Value(manifest.into()))
                    .and(
                        Expr::col(compaction_frozen_history::Column::NextImport)
                            .eq(Expr::Value(next.into())),
                    )
                    .and(Expr::col(compaction_frozen_history::Column::Ready).eq(Expr::val(0_i64))),
            )
            .exec(&tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn compaction_frozen_import_state<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    owner: &str,
    manifest: &str,
) -> Result<Option<(u64, String)>> {
    use sea_orm::ColumnTrait;
    let row = compaction_frozen_history::Entity::find_by_id(manifest)
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
        .filter(compaction_frozen_history::Column::Ready.eq(1_i64))
        .filter(
            Expr::col(compaction_frozen_history::Column::ImportCount)
                .eq(Expr::col(compaction_frozen_history::Column::NextImport)),
        )
        .one(db)
        .await?;
    row.map(|row| Ok((u64::try_from(row.import_count)?, row.imports_sha256)))
        .transpose()
}

pub(crate) async fn compaction_frozen_import_page<C: ConnectionTrait>(
    db: &C,
    workspace: &str,
    owner: &str,
    manifest: &str,
    start: u64,
) -> Result<Vec<FrozenImportRecord>> {
    let scoped = compaction_frozen_import::Entity::find()
        .inner_join(compaction_frozen_history::Entity)
        .filter(compaction_frozen_history::Column::Id.eq(manifest))
        .filter(compaction_frozen_history::Column::WorkspaceId.eq(workspace))
        .filter(compaction_frozen_history::Column::OwnerThread.eq(owner))
        .filter(compaction_frozen_history::Column::Ready.eq(1_i64))
        .filter(compaction_frozen_import::Column::Ordinal.gte(i64::try_from(start)?));
    let rows = scoped
        .clone()
        .select_only()
        .column(compaction_frozen_import::Column::Ordinal)
        .column(compaction_frozen_import::Column::Bytes)
        .order_by_asc(compaction_frozen_import::Column::Ordinal)
        .limit(128)
        .into_tuple::<(i64, i64)>()
        .all(db)
        .await?;
    let mut end = start;
    let mut bytes = 0;
    for (ordinal, size) in rows {
        let size = usize::try_from(size)?;
        ensure!(size <= SOURCE_PAGE_BYTES, "invalid import metadata size");
        if bytes + size > SOURCE_PAGE_BYTES {
            break;
        }
        ensure!(u64::try_from(ordinal)? == end, "import ordinal gap");
        bytes += size;
        end += 1;
    }
    let rows = scoped
        .select_only()
        .column(compaction_frozen_import::Column::ProofJson)
        .filter(compaction_frozen_import::Column::Ordinal.lt(i64::try_from(end)?))
        .order_by_asc(compaction_frozen_import::Column::Ordinal)
        .into_tuple::<String>()
        .all(db)
        .await?;
    rows.into_iter()
        .map(|json| Ok(serde_json::from_str(&json)?))
        .collect()
}

use super::compaction_live_sources;
