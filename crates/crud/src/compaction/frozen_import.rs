//! Immutable own-import evidence attached to the newly admitted frozen context.
//! Preparation decodes reference metadata outside database capacity. Publication
//! is fenced by the manifest's declared import count and independent digest.
use super::*;
use pioneer_compaction::frozen::FrozenMessageRef;
use sea_orm::sea_query::{BinOper, Expr, ExprTrait, JoinType, OnConflict, Order, Query};
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
    fn record_at(&self, message: u64) -> FrozenImportRecord {
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

impl CrudStore {
    #[allow(clippy::too_many_arguments)]
    pub async fn compaction_prepare_frozen_import(
        &self,
        workspace: &str,
        destination: &str,
        delivery: &str,
        acknowledgement: &SourceRef,
        output_ordinal: u64,
        source_thread: &str,
        source: &SourceRef,
    ) -> Result<PreparedFrozenImport> {
        let snapshot = self
            .compaction_delivery_output(workspace, delivery)
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted output binding is unavailable"))?;
        let acknowledged = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr_as(Expr::val(1_i64), "found")
                    .from_as("task_delivery", "d")
                    .join_as(
                        JoinType::InnerJoin,
                        "turn_event",
                        "e",
                        Expr::col(("e", "turn_id"))
                            .eq(Expr::col(("d", "delivered_turn_id")))
                            .and(
                                Expr::col(("e", "thread_id"))
                                    .eq(Expr::col(("d", "target_thread_id"))),
                            ),
                    )
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_event_revision",
                        "r",
                        Expr::col(("r", "source_id"))
                            .eq(Expr::col(("e", "id")))
                            .and(Expr::col(("r", "turn_id")).eq(Expr::col(("e", "turn_id"))))
                            .and(Expr::col(("r", "present")).eq(Expr::val(1_i64)))
                            .and(
                                Expr::col(("r", "projection_revision"))
                                    .eq(Expr::col(("r", "revision"))),
                            ),
                    )
                    .and_where(
                        Expr::col(("d", "id"))
                            .eq(Expr::Value(delivery.into()))
                            .and(Expr::col(("d", "workspace_id")).eq(Expr::Value(workspace.into())))
                            .and(
                                Expr::col(("d", "target_thread_id"))
                                    .eq(Expr::Value(destination.into())),
                            )
                            .and(Expr::col(("d", "status")).eq(Expr::val("delivered")))
                            .and(
                                Expr::col(("e", "id"))
                                    .eq(Expr::Value(acknowledgement.id.clone().into())),
                            )
                            .and(
                                Expr::val("event:")
                                    .binary(BinOper::Custom("||"), Expr::col(("e", "turn_id")))
                                    .eq(Expr::Value(acknowledgement.scope.clone().into())),
                            )
                            .and(
                                Expr::val("event-revision:")
                                    .binary(BinOper::Custom("||"), Expr::col(("r", "revision")))
                                    .eq(Expr::Value(acknowledgement.version.clone().into())),
                            )
                            .and(Expr::col(("e", "event_type")).eq(Expr::Value(
                                pioneer_protocol::constants::events::ITEM_COMPLETED.into(),
                            )))
                            .and(Expr::col(("r", "item_id")).eq(Expr::Value(
                                pioneer_protocol::task_delivery_result_item_id(delivery).into(),
                            ))),
                    )
                    .to_owned(),
            ))
            .await?
            .is_some();
        ensure!(
            acknowledged,
            "output has no exact acknowledged destination binding"
        );
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col(("m", "reference_json")))
                    .from_as("compaction_frozen_message", "m")
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_frozen_history",
                        "h",
                        Expr::col(("h", "id")).eq(Expr::col(("m", "manifest_id"))),
                    )
                    .and_where(
                        Expr::col(("h", "id"))
                            .eq(Expr::Value(
                                snapshot.output.history.manifest_id.clone().into(),
                            ))
                            .and(Expr::col(("h", "workspace_id")).eq(Expr::Value(workspace.into())))
                            .and(
                                Expr::col(("h", "owner_thread"))
                                    .eq(Expr::Value(snapshot.output.source_thread.clone().into())),
                            )
                            .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64)))
                            .and(Expr::col(("h", "identity_sha256")).eq(Expr::Value(
                                snapshot.output.history.identity_sha256.clone().into(),
                            )))
                            .and(
                                Expr::col(("m", "ordinal"))
                                    .eq(Expr::Value(i64::try_from(output_ordinal)?.into())),
                            )
                            .and(Expr::col(("m", "bytes")).lte(Expr::val(262144_i64))),
                    )
                    .to_owned(),
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("output origin reference is unavailable"))?;
        let original_json: String = row.try_get("", "reference_json")?;
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
            let thread = self
                .compaction_reference_thread(workspace, &reference)
                .await?
                .ok_or_else(|| anyhow::anyhow!("output coverage source changed"))?;
            if &reference == source && thread == source_thread {
                found = true;
                break;
            }
            if let Some(owner) = reference.scope.strip_prefix("checkpoint:") {
                let checkpoint = self
                    .compaction_checkpoint_edges(&reference.id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("output coverage checkpoint disappeared"))?;
                ensure!(
                    checkpoint.owner == owner && checkpoint.format_version == FORMAT_VERSION,
                    "output checkpoint identity mismatch"
                );
                if let Some(previous) = checkpoint.previous {
                    pending.push(
                        self.compaction_checkpoint_source(workspace, &thread, &previous)
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

    pub async fn compaction_append_frozen_imports(
        &self,
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
            let row = self
                .connection
                .query_one_raw(statement(
                    &Query::select()
                        .expr(Expr::col(("m", "reference_json")))
                        .from_as("compaction_frozen_message", "m")
                        .join_as(
                            JoinType::InnerJoin,
                            "compaction_frozen_history",
                            "h",
                            Expr::col(("h", "id")).eq(Expr::col(("m", "manifest_id"))),
                        )
                        .and_where(
                            Expr::col(("h", "id"))
                                .eq(Expr::Value(manifest.into()))
                                .and(
                                    Expr::col(("h", "workspace_id"))
                                        .eq(Expr::Value(workspace.into())),
                                )
                                .and(Expr::col(("h", "owner_thread")).eq(Expr::Value(owner.into())))
                                .and(
                                    Expr::col(("m", "ordinal"))
                                        .eq(Expr::Value(i64::try_from(*message)?.into())),
                                )
                                .and(Expr::col(("m", "bytes")).lte(Expr::val(262144_i64))),
                        )
                        .to_owned(),
                ))
                .await?
                .ok_or_else(|| anyhow::anyhow!("target reference is unavailable"))?;
            let target_json: String = row.try_get("", "reference_json")?;
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
        let tx = self.connection.begin().await?;
        let row = tx
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("ready"))
                    .expr(Expr::col("import_count"))
                    .expr(Expr::col("next_import"))
                    .from("compaction_frozen_history")
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(manifest.into()))
                            .and(Expr::col("workspace_id").eq(Expr::Value(workspace.into())))
                            .and(Expr::col("owner_thread").eq(Expr::Value(owner.into()))),
                    )
                    .to_owned(),
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("import target manifest is unavailable"))?;
        let ready = row.try_get::<i64>("", "ready")? != 0;
        let count: i64 = row.try_get("", "import_count")?;
        let next: i64 = row.try_get("", "next_import")?;
        let start = i64::try_from(start)?;
        let end = start
            .checked_add(i64::try_from(batch.len())?)
            .ok_or_else(|| anyhow::anyhow!("import ordinal overflow"))?;
        ensure!(
            start <= next && end <= count && (start == next || end <= next),
            "imports are not a sequential batch or exact retry"
        );
        for (ordinal, record, prepared, target_json, json) in &batch {
            if !ready {
                tx.execute_raw(statement(
                    &Query::insert()
                        .into_table("compaction_frozen_import")
                        .columns([
                            "manifest_id",
                            "ordinal",
                            "message_ordinal",
                            "source_scope",
                            "source_id",
                            "source_version",
                            "source_thread",
                            "proof_json",
                            "bytes",
                        ])
                        .select_from(
                            Query::select()
                                .expr(Expr::col(("h", "id")))
                                .expr(Expr::Value((*ordinal).into()))
                                .expr(Expr::Value(i64::try_from(record.message_ordinal)?.into()))
                                .expr(Expr::Value(record.source.scope.clone().into()))
                                .expr(Expr::Value(record.source.id.clone().into()))
                                .expr(Expr::Value(record.source.version.clone().into()))
                                .expr(Expr::Value(record.source_thread.clone().into()))
                                .expr(Expr::Value(json.clone().into()))
                                .expr(Expr::Value(i64::try_from(json.len())?.into()))
                                .from_as("compaction_frozen_history", "h")
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_frozen_message",
                                    "target",
                                    Expr::col(("target", "manifest_id"))
                                        .eq(Expr::col(("h", "id")))
                                        .and(Expr::col(("target", "ordinal")).eq(Expr::Value(
                                            i64::try_from(record.message_ordinal)?.into(),
                                        )))
                                        .and(
                                            Expr::col(("target", "reference_json"))
                                                .eq(Expr::Value(target_json.clone().into())),
                                        ),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "task_delivery",
                                    "d",
                                    Expr::col(("d", "id"))
                                        .eq(Expr::Value(record.delivery_id.clone().into()))
                                        .and(
                                            Expr::col(("d", "workspace_id"))
                                                .eq(Expr::col(("h", "workspace_id"))),
                                        )
                                        .and(
                                            Expr::col(("d", "target_thread_id"))
                                                .eq(Expr::col(("h", "owner_thread"))),
                                        )
                                        .and(Expr::col(("d", "status")).eq(Expr::val("delivered"))),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_delivery_output",
                                    "b",
                                    Expr::col(("b", "delivery_id"))
                                        .eq(Expr::col(("d", "id")))
                                        .and(
                                            Expr::col(("b", "candidate_id")).eq(Expr::Value(
                                                record.candidate_id.clone().into(),
                                            )),
                                        ),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_task_output",
                                    "output",
                                    Expr::col(("output", "task_run_turn_id"))
                                        .eq(Expr::col(("b", "task_run_turn_id")))
                                        .and(
                                            Expr::col(("output", "task_id"))
                                                .eq(Expr::col(("d", "task_id"))),
                                        )
                                        .and(
                                            Expr::col(("output", "run_id"))
                                                .eq(Expr::col(("d", "run_id"))),
                                        )
                                        .and(
                                            Expr::col(("output", "workspace_id"))
                                                .eq(Expr::col(("d", "workspace_id"))),
                                        )
                                        .and(Expr::col(("output", "manifest_id")).eq(Expr::Value(
                                            record.output_manifest.clone().into(),
                                        ))),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_frozen_history",
                                    "original",
                                    Expr::col(("original", "id"))
                                        .eq(Expr::col(("output", "manifest_id")))
                                        .and(Expr::col(("original", "ready")).eq(Expr::val(1_i64)))
                                        .and(Expr::col(("original", "identity_sha256")).eq(
                                            Expr::Value(prepared.output_digest.clone().into()),
                                        )),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_frozen_message",
                                    "origin",
                                    Expr::col(("origin", "manifest_id"))
                                        .eq(Expr::col(("original", "id")))
                                        .and(Expr::col(("origin", "ordinal")).eq(Expr::Value(
                                            i64::try_from(record.output_ordinal)?.into(),
                                        )))
                                        .and(Expr::col(("origin", "reference_json")).eq(
                                            Expr::Value(prepared.original_json.clone().into()),
                                        )),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "turn_event",
                                    "e",
                                    Expr::col(("e", "id"))
                                        .eq(Expr::Value(record.acknowledgement.id.clone().into()))
                                        .and(
                                            Expr::col(("e", "thread_id"))
                                                .eq(Expr::col(("d", "target_thread_id"))),
                                        )
                                        .and(
                                            Expr::col(("e", "turn_id"))
                                                .eq(Expr::col(("d", "delivered_turn_id"))),
                                        ),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_event_revision",
                                    "r",
                                    Expr::col(("r", "source_id"))
                                        .eq(Expr::col(("e", "id")))
                                        .and(
                                            Expr::col(("r", "turn_id"))
                                                .eq(Expr::col(("e", "turn_id"))),
                                        )
                                        .and(Expr::col(("r", "present")).eq(Expr::val(1_i64)))
                                        .and(
                                            Expr::col(("r", "projection_revision"))
                                                .eq(Expr::col(("r", "revision"))),
                                        ),
                                )
                                .join_as(
                                    JoinType::InnerJoin,
                                    "compaction_live_sources",
                                    "s",
                                    Expr::col(("s", "source_scope"))
                                        .eq(Expr::Value(record.source.scope.clone().into()))
                                        .and(
                                            Expr::col(("s", "source_id"))
                                                .eq(Expr::Value(record.source.id.clone().into())),
                                        )
                                        .and(
                                            Expr::col(("s", "source_version")).eq(Expr::Value(
                                                record.source.version.clone().into(),
                                            )),
                                        )
                                        .and(
                                            Expr::col(("s", "thread_id")).eq(Expr::Value(
                                                record.source_thread.clone().into(),
                                            )),
                                        )
                                        .and(
                                            Expr::col(("s", "workspace_id"))
                                                .eq(Expr::col(("h", "workspace_id"))),
                                        ),
                                )
                                .and_where(
                                    Expr::col(("h", "id"))
                                        .eq(Expr::Value(manifest.into()))
                                        .and(
                                            Expr::col(("h", "workspace_id"))
                                                .eq(Expr::Value(workspace.into())),
                                        )
                                        .and(
                                            Expr::col(("h", "owner_thread"))
                                                .eq(Expr::Value(owner.into())),
                                        )
                                        .and(Expr::col(("h", "ready")).eq(Expr::val(0_i64)))
                                        .and(
                                            Expr::val("event:")
                                                .binary(
                                                    BinOper::Custom("||"),
                                                    Expr::col(("e", "turn_id")),
                                                )
                                                .eq(Expr::Value(
                                                    record.acknowledgement.scope.clone().into(),
                                                )),
                                        )
                                        .and(
                                            Expr::val("event-revision:")
                                                .binary(
                                                    BinOper::Custom("||"),
                                                    Expr::col(("r", "revision")),
                                                )
                                                .eq(Expr::Value(
                                                    record.acknowledgement.version.clone().into(),
                                                )),
                                        )
                                        .and(
                                            Expr::col(("e", "event_type")).eq(Expr::Value(
                                                pioneer_protocol::constants::events::ITEM_COMPLETED
                                                    .into(),
                                            )),
                                        )
                                        .and(
                                            Expr::col(("r", "item_id")).eq(Expr::Value(
                                                pioneer_protocol::task_delivery_result_item_id(
                                                    &record.delivery_id,
                                                )
                                                .into(),
                                            )),
                                        ),
                                )
                                .to_owned(),
                        )?
                        .on_conflict(
                            OnConflict::columns(["manifest_id", "ordinal"])
                                .do_nothing()
                                .to_owned(),
                        )
                        .to_owned(),
                ))
                .await?;
            }
            ensure!(
                tx.query_one_raw(statement(
                    &Query::select()
                        .expr_as(Expr::val(1_i64), "found")
                        .from("compaction_frozen_import")
                        .and_where(
                            Expr::col("manifest_id")
                                .eq(Expr::Value(manifest.into()))
                                .and(Expr::col("ordinal").eq(Expr::Value((*ordinal).into())))
                                .and(Expr::col("proof_json").eq(Expr::Value(json.clone().into())))
                        )
                        .to_owned()
                ))
                .await?
                .is_some(),
                "import binding changed or retry changed immutable metadata"
            );
        }
        if start == next && !ready {
            tx.execute_raw(statement(
                &Query::update()
                    .table("compaction_frozen_history")
                    .value("next_import", Expr::Value(end.into()))
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(manifest.into()))
                            .and(Expr::col("next_import").eq(Expr::Value(next.into())))
                            .and(Expr::col("ready").eq(Expr::val(0_i64))),
                    )
                    .to_owned(),
            ))
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn compaction_frozen_import_state(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
    ) -> Result<Option<(u64, String)>> {
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("import_count"))
                    .expr(Expr::col("imports_sha256"))
                    .from("compaction_frozen_history")
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(manifest.into()))
                            .and(Expr::col("workspace_id").eq(Expr::Value(workspace.into())))
                            .and(Expr::col("owner_thread").eq(Expr::Value(owner.into())))
                            .and(Expr::col("ready").eq(Expr::val(1_i64)))
                            .and(Expr::col("import_count").eq(Expr::col("next_import"))),
                    )
                    .to_owned(),
            ))
            .await?;
        row.map(|row| {
            Ok((
                u64::try_from(row.try_get::<i64>("", "import_count")?)?,
                row.try_get("", "imports_sha256")?,
            ))
        })
        .transpose()
    }

    pub async fn compaction_frozen_import_page(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
        start: u64,
    ) -> Result<Vec<FrozenImportRecord>> {
        let rows = self
            .connection
            .query_all_raw(statement(
                &Query::select()
                    .expr(Expr::col(("i", "ordinal")))
                    .expr(Expr::col(("i", "bytes")))
                    .from_as("compaction_frozen_import", "i")
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_frozen_history",
                        "h",
                        Expr::col(("h", "id")).eq(Expr::col(("i", "manifest_id"))),
                    )
                    .and_where(
                        Expr::col(("h", "id"))
                            .eq(Expr::Value(manifest.into()))
                            .and(Expr::col(("h", "workspace_id")).eq(Expr::Value(workspace.into())))
                            .and(Expr::col(("h", "owner_thread")).eq(Expr::Value(owner.into())))
                            .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64)))
                            .and(
                                Expr::col(("i", "ordinal"))
                                    .gte(Expr::Value(i64::try_from(start)?.into())),
                            ),
                    )
                    .order_by_expr(Expr::col(("i", "ordinal")), Order::Asc)
                    .limit(128)
                    .to_owned(),
            ))
            .await?;
        let mut end = start;
        let mut bytes = 0;
        for row in rows {
            let size = usize::try_from(row.try_get::<i64>("", "bytes")?)?;
            ensure!(size <= SOURCE_PAGE_BYTES, "invalid import metadata size");
            if bytes + size > SOURCE_PAGE_BYTES {
                break;
            }
            ensure!(
                u64::try_from(row.try_get::<i64>("", "ordinal")?)? == end,
                "import ordinal gap"
            );
            bytes += size;
            end += 1;
        }
        let rows = self
            .connection
            .query_all_raw(statement(
                &Query::select()
                    .expr(Expr::col(("i", "proof_json")))
                    .from_as("compaction_frozen_import", "i")
                    .join_as(
                        JoinType::InnerJoin,
                        "compaction_frozen_history",
                        "h",
                        Expr::col(("h", "id")).eq(Expr::col(("i", "manifest_id"))),
                    )
                    .and_where(
                        Expr::col(("h", "id"))
                            .eq(Expr::Value(manifest.into()))
                            .and(Expr::col(("h", "workspace_id")).eq(Expr::Value(workspace.into())))
                            .and(Expr::col(("h", "owner_thread")).eq(Expr::Value(owner.into())))
                            .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64)))
                            .and(
                                Expr::col(("i", "ordinal"))
                                    .gte(Expr::Value(i64::try_from(start)?.into())),
                            )
                            .and(
                                Expr::col(("i", "ordinal"))
                                    .lt(Expr::Value(i64::try_from(end)?.into())),
                            ),
                    )
                    .order_by_expr(Expr::col(("i", "ordinal")), Order::Asc)
                    .to_owned(),
            ))
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(serde_json::from_str(
                    &row.try_get::<String>("", "proof_json")?,
                )?)
            })
            .collect()
    }
}
