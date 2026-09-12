//! Frozen-history manifests are populated in bounded restart-safe quanta. An
//! incomplete manifest cannot be referenced by a started execution snapshot.
use super::*;
use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
use sea_orm::sea_query::{Expr, ExprTrait, JoinType, OnConflict, Order, Query};

impl CrudStore {
    pub async fn compaction_begin_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<()> {
        self.compaction_begin_frozen_history_with_imports(
            workspace,
            owner_thread,
            descriptor,
            0,
            EMPTY_FROZEN_IMPORT_SHA256,
        )
        .await
    }

    pub async fn compaction_begin_frozen_history_with_imports(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
        imports: u64,
        imports_sha256: &str,
    ) -> Result<()> {
        ensure!(
            descriptor.format == 1
                && !descriptor.manifest_id.is_empty()
                && descriptor.identity_sha256.len() == 64
                && descriptor
                    .identity_sha256
                    .bytes()
                    .all(|c| c.is_ascii_hexdigit()),
            "invalid frozen history descriptor"
        );
        ensure!(
            imports_sha256.len() == 64 && imports_sha256.bytes().all(|c| c.is_ascii_hexdigit()),
            "invalid import digest"
        );
        let messages = i64::try_from(descriptor.messages)?;
        self.connection
            .execute_raw(statement(
                &Query::insert()
                    .into_table("compaction_frozen_history")
                    .columns([
                        "id",
                        "workspace_id",
                        "owner_thread",
                        "identity_sha256",
                        "message_count",
                        "import_count",
                        "imports_sha256",
                    ])
                    .select_from(
                        Query::select()
                            .expr(Expr::Value(descriptor.manifest_id.clone().into()))
                            .expr(Expr::Value(workspace.into()))
                            .expr(Expr::Value(owner_thread.into()))
                            .expr(Expr::Value(descriptor.identity_sha256.clone().into()))
                            .expr(Expr::Value(messages.into()))
                            .expr(Expr::Value(i64::try_from(imports)?.into()))
                            .expr(Expr::Value(imports_sha256.into()))
                            .and_where(Expr::exists(
                                Query::select()
                                    .expr(Expr::val(1_i64))
                                    .from("thread")
                                    .and_where(
                                        Expr::col("id").eq(Expr::Value(owner_thread.into())).and(
                                            Expr::col("workspace_id")
                                                .eq(Expr::Value(workspace.into())),
                                        ),
                                    )
                                    .to_owned(),
                            ))
                            .to_owned(),
                    )?
                    .on_conflict(OnConflict::columns(["id"]).do_nothing().to_owned())
                    .to_owned(),
            ))
            .await?;
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("identity_sha256"))
                    .expr(Expr::col("message_count"))
                    .expr(Expr::col("import_count"))
                    .expr(Expr::col("imports_sha256"))
                    .from("compaction_frozen_history")
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(descriptor.manifest_id.clone().into()))
                            .and(Expr::col("workspace_id").eq(Expr::Value(workspace.into())))
                            .and(Expr::col("owner_thread").eq(Expr::Value(owner_thread.into()))),
                    )
                    .to_owned(),
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("frozen history scope is unavailable"))?;
        ensure!(
            row.try_get::<String>("", "identity_sha256")? == descriptor.identity_sha256
                && row.try_get::<i64>("", "message_count")? == messages
                && row.try_get::<i64>("", "import_count")? == i64::try_from(imports)?
                && row.try_get::<String>("", "imports_sha256")? == imports_sha256,
            "frozen history identity collision"
        );
        Ok(())
    }

    /// The prepared JSON contains typed references and hashes only. Before DB
    /// admission it is validated and bounded; the transaction revalidates the
    /// manifest owner/readiness and exact existing bytes on an idempotent retry.
    pub async fn compaction_append_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        manifest: &str,
        start: u64,
        messages: &[FrozenMessageRef],
    ) -> Result<()> {
        ensure!(
            messages.len() as u64 <= SOURCE_PAGE_ROWS,
            "frozen history batch row limit"
        );
        let mut batch = Vec::new();
        let mut total = 0;
        for (index, message) in messages.iter().enumerate() {
            message.validate()?;
            let json = serde_json::to_string(message)?;
            total += json.len();
            ensure!(
                total <= SOURCE_PAGE_BYTES,
                "frozen history batch byte limit"
            );
            batch.push((
                i64::try_from(
                    start
                        .checked_add(index as u64)
                        .ok_or_else(|| anyhow::anyhow!("frozen ordinal overflow"))?,
                )?,
                json,
            ));
        }
        let tx = self.connection.begin().await?;
        let row = tx
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("ready"))
                    .expr(Expr::col("message_count"))
                    .expr(Expr::col("next_ordinal"))
                    .from("compaction_frozen_history")
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(manifest.into()))
                            .and(Expr::col("workspace_id").eq(Expr::Value(workspace.into())))
                            .and(Expr::col("owner_thread").eq(Expr::Value(owner_thread.into()))),
                    )
                    .to_owned(),
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("frozen history owner is unavailable"))?;
        let ready = row.try_get::<i64>("", "ready")? != 0;
        let count = row.try_get::<i64>("", "message_count")?;
        let next = row.try_get::<i64>("", "next_ordinal")?;
        let start = i64::try_from(start)?;
        let end = start
            .checked_add(i64::try_from(batch.len())?)
            .ok_or_else(|| anyhow::anyhow!("frozen ordinal overflow"))?;
        ensure!(
            start <= next && end <= count && (start == next || end <= next),
            "frozen history append is not sequential or an exact retry"
        );
        for (ordinal, json) in &batch {
            ensure!(
                *ordinal < count,
                "frozen history ordinal exceeds declared count"
            );
            if !ready {
                tx.execute_raw(statement(
                    &Query::insert()
                        .into_table("compaction_frozen_message")
                        .columns(["manifest_id", "ordinal", "reference_json", "bytes"])
                        .values_panic([
                            Expr::Value(manifest.into()),
                            Expr::Value((*ordinal).into()),
                            Expr::Value(json.clone().into()),
                            Expr::Value((json.len() as i64).into()),
                        ])
                        .on_conflict(
                            OnConflict::columns(["manifest_id", "ordinal"])
                                .do_nothing()
                                .to_owned(),
                        )
                        .to_owned(),
                ))
                .await?;
            }
            let matches = tx
                .query_one_raw(statement(
                    &Query::select()
                        .expr_as(Expr::val(1_i64), "found")
                        .from("compaction_frozen_message")
                        .and_where(
                            Expr::col("manifest_id")
                                .eq(Expr::Value(manifest.into()))
                                .and(Expr::col("ordinal").eq(Expr::Value((*ordinal).into())))
                                .and(
                                    Expr::col("reference_json")
                                        .eq(Expr::Value(json.clone().into())),
                                ),
                        )
                        .to_owned(),
                ))
                .await?
                .is_some();
            ensure!(matches, "frozen history retry changed an immutable entry");
        }
        if start == next && !ready {
            tx.execute_raw(statement(
                &Query::update()
                    .table("compaction_frozen_history")
                    .value("next_ordinal", Expr::Value(end.into()))
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(manifest.into()))
                            .and(Expr::col("next_ordinal").eq(Expr::Value(next.into())))
                            .and(Expr::col("ready").eq(Expr::val(0_i64))),
                    )
                    .to_owned(),
            ))
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Ready is published only after the bounded writer has filled all exact
    /// ordinals. The content digest is checked by the caller before this CAS.
    pub async fn compaction_finish_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<bool> {
        // Constant-size publication: next_ordinal advances atomically with each
        // sequential batch, so finalization never scans the whole manifest.
        Ok(self
            .connection
            .execute_raw(statement(
                &Query::update()
                    .table("compaction_frozen_history")
                    .value("ready", Expr::val(1_i64))
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(descriptor.manifest_id.clone().into()))
                            .and(Expr::col("workspace_id").eq(Expr::Value(workspace.into())))
                            .and(Expr::col("owner_thread").eq(Expr::Value(owner_thread.into())))
                            .and(
                                Expr::col("identity_sha256")
                                    .eq(Expr::Value(descriptor.identity_sha256.clone().into())),
                            )
                            .and(
                                Expr::col("message_count")
                                    .eq(Expr::Value(i64::try_from(descriptor.messages)?.into())),
                            )
                            .and(Expr::col("message_count").eq(Expr::col("next_ordinal")))
                            .and(Expr::col("import_count").eq(Expr::col("next_import"))),
                    )
                    .to_owned(),
            ))
            .await?
            .rows_affected()
            == 1)
    }

    pub async fn compaction_frozen_history_owner(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<Option<String>> {
        ensure!(descriptor.format == 1, "unsupported frozen history format");
        let row = self
            .connection
            .query_one_raw(statement(
                &Query::select()
                    .expr(Expr::col("owner_thread"))
                    .from("compaction_frozen_history")
                    .and_where(
                        Expr::col("id")
                            .eq(Expr::Value(descriptor.manifest_id.clone().into()))
                            .and(Expr::col("workspace_id").eq(Expr::Value(workspace.into())))
                            .and(Expr::col("ready").eq(Expr::val(1_i64)))
                            .and(
                                Expr::col("identity_sha256")
                                    .eq(Expr::Value(descriptor.identity_sha256.clone().into())),
                            )
                            .and(
                                Expr::col("message_count")
                                    .eq(Expr::Value(i64::try_from(descriptor.messages)?.into())),
                            ),
                    )
                    .to_owned(),
            ))
            .await?;
        row.map(|row| Ok(row.try_get("", "owner_thread")?))
            .transpose()
    }

    pub async fn compaction_frozen_history_page(
        &self,
        workspace: &str,
        owner_thread: &str,
        manifest: &str,
        start: u64,
    ) -> Result<Vec<FrozenMessageRef>> {
        // Bound metadata discovery first, then the payload sum. Both reads use
        // the reader pool and release capacity before decoding reference JSON.
        let rows = self
            .connection
            .query_all_raw(statement(
                &Query::select()
                    .expr(Expr::col(("m", "ordinal")))
                    .expr(Expr::col(("m", "bytes")))
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
                            .and(Expr::col(("h", "workspace_id")).eq(Expr::Value(workspace.into())))
                            .and(
                                Expr::col(("h", "owner_thread"))
                                    .eq(Expr::Value(owner_thread.into())),
                            )
                            .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64)))
                            .and(
                                Expr::col(("m", "ordinal"))
                                    .gte(Expr::Value(i64::try_from(start)?.into())),
                            ),
                    )
                    .order_by_expr(Expr::col(("m", "ordinal")), Order::Asc)
                    .limit(128)
                    .to_owned(),
            ))
            .await?;
        let mut end = i64::try_from(start)?;
        let mut bytes = 0_i64;
        for row in rows {
            let size = row.try_get::<i64>("", "bytes")?;
            ensure!(
                (0..=SOURCE_PAGE_BYTES as i64).contains(&size),
                "invalid frozen reference size"
            );
            if bytes + size > SOURCE_PAGE_BYTES as i64 {
                break;
            }
            ensure!(
                row.try_get::<i64>("", "ordinal")? == end,
                "frozen history ordinal gap"
            );
            bytes += size;
            end += 1;
        }
        let rows = self
            .connection
            .query_all_raw(statement(
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
                            .and(Expr::col(("h", "workspace_id")).eq(Expr::Value(workspace.into())))
                            .and(
                                Expr::col(("h", "owner_thread"))
                                    .eq(Expr::Value(owner_thread.into())),
                            )
                            .and(Expr::col(("h", "ready")).eq(Expr::val(1_i64)))
                            .and(
                                Expr::col(("m", "ordinal"))
                                    .gte(Expr::Value(i64::try_from(start)?.into())),
                            )
                            .and(Expr::col(("m", "ordinal")).lt(Expr::Value(end.into()))),
                    )
                    .order_by_expr(Expr::col(("m", "ordinal")), Order::Asc)
                    .to_owned(),
            ))
            .await?;
        let mut result = Vec::new();
        for row in rows {
            let json: String = row.try_get("", "reference_json")?;
            let message: FrozenMessageRef = serde_json::from_str(&json)?;
            message.validate()?;
            result.push(message);
        }
        Ok(result)
    }
}
