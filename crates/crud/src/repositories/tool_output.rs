use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{thread, tool_output_chunk, turn, turn_item_attempt};
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ActiveValue::Set, JoinType, QueryOrder, QuerySelect, TransactionTrait, entity::prelude::*,
};

// Prepare bytes before writer admission. Scope and event identity are database
// state, so revalidate both inside the transaction that inserts the chunk.
pub(crate) async fn record_tool_output(
    store: &CrudStore,
    id: &str,
    n: &pioneer_protocol::ItemDeltaNotification,
) -> Result<()> {
    ensure!(
        n.delta.len() <= 128 * 1024,
        "tool output chunk exceeds frame limit"
    );
    let work_policy = prepare_work_policy(n)?;
    let stream = serde_json::to_string(&n.stream)?;
    let metadata = n.payload.as_ref().map(serde_json::to_string).transpose()?;
    let row = tool_output_chunk::ActiveModel {
        id: Set(id.into()),
        turn_id: Set(n.turn_id.clone()),
        item_id: Set(n.item_id.clone()),
        stream: Set(stream),
        text: Set(n.delta.clone()),
        metadata: Set(metadata),
        ..Default::default()
    };
    store
        .run_serialized_write(|| {
            let row = row.clone();
            let work_policy = work_policy.clone();
            async move {
                let tx = store.connection.begin().await?;
                ensure!(
                    thread::Entity::find_by_id(&n.thread_id)
                        .filter(thread::Column::WorkspaceId.eq(&n.workspace_id))
                        .select_only()
                        .column(thread::Column::Id)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                        .is_some(),
                    "tool output workspace mismatch"
                );
                ensure!(
                    turn::Entity::find_by_id(&n.turn_id)
                        .filter(turn::Column::ThreadId.eq(&n.thread_id))
                        .select_only()
                        .column(turn::Column::Id)
                        .into_tuple::<String>()
                        .one(&tx)
                        .await?
                        .is_some(),
                    "tool output turn mismatch"
                );
                if let Some(old) = tool_output_chunk::Entity::find()
                    .filter(tool_output_chunk::Column::Id.eq(id))
                    .one(&tx)
                    .await?
                {
                    ensure!(
                        old.turn_id == n.turn_id
                            && old.item_id == n.item_id
                            && old.text == n.delta
                            && old.stream == *row.stream.as_ref()
                            && old.metadata == *row.metadata.as_ref(),
                        "tool output identity conflict"
                    );
                } else {
                    if let Some((deadline, attempt_number, trace_id)) = &work_policy {
                        ensure!(
                            turn::Entity::find_by_id(&n.turn_id)
                                .filter(turn::Column::Status.eq("in_progress"))
                                .one(&tx)
                                .await?
                                .is_some(),
                            "tool work policy requires an active Turn"
                        );
                        ensure!(
                            tool_output_chunk::Entity::find()
                                .filter(tool_output_chunk::Column::TurnId.eq(&n.turn_id))
                                .filter(tool_output_chunk::Column::ItemId.eq(&n.item_id))
                                .filter(Expr::cust(
                                    "json_extract(metadata, '$.stage') = 'attempt_work_started'"
                                ))
                                .filter(Expr::cust_with_values(
                                    "json_extract(metadata, '$.observation.attempt_id') = ?",
                                    [*attempt_number]
                                ))
                                .select_only()
                                .column(tool_output_chunk::Column::Id)
                                .limit(1)
                                .into_tuple::<String>()
                                .one(&tx)
                                .await?
                                .is_none(),
                            "tool work policy already committed for this attempt"
                        );
                        let changed = turn_item_attempt::Entity::update_many()
                            .col_expr(
                                turn_item_attempt::Column::IdleDeadlineAt,
                                Expr::value(*deadline),
                            )
                            .col_expr(
                                turn_item_attempt::Column::HardDeadlineAt,
                                Expr::value(*deadline),
                            )
                            .filter(turn_item_attempt::Column::TurnId.eq(&n.turn_id))
                            .filter(turn_item_attempt::Column::ItemId.eq(&n.item_id))
                            .filter(turn_item_attempt::Column::Status.eq("running"))
                            .filter(turn_item_attempt::Column::AttemptNumber.eq(*attempt_number))
                            .filter(Expr::cust_with_values(
                                "json_extract(payload, '$.observation.traceId') = ?",
                                [trace_id.clone()],
                            ))
                            .exec(&tx)
                            .await?;
                        ensure!(
                            changed.rows_affected == 1,
                            "tool work policy owner mismatch"
                        );
                    }
                    tool_output_chunk::Entity::insert(row).exec(&tx).await?;
                }
                tx.commit().await?;
                Ok(())
            }
        })
        .await
}
/// Bounded ordered chunks; caller releases reader capacity before rendering.
pub(crate) async fn tool_output_page(
    store: &CrudStore,
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    after: i64,
) -> Result<Vec<(i64, tool_output_chunk::Model)>> {
    let rows = tool_output_chunk::Entity::find()
        .join(
            JoinType::InnerJoin,
            tool_output_chunk::Entity::belongs_to(turn::Entity)
                .from(tool_output_chunk::Column::TurnId)
                .to(turn::Column::Id)
                .into(),
        )
        .join(
            JoinType::InnerJoin,
            turn::Entity::belongs_to(thread::Entity)
                .from(turn::Column::ThreadId)
                .to(thread::Column::Id)
                .into(),
        )
        .filter(Expr::col((thread::Entity, thread::Column::WorkspaceId)).eq(workspace))
        .filter(Expr::col((turn::Entity, turn::Column::ThreadId)).eq(thread_id))
        .filter(tool_output_chunk::Column::TurnId.eq(turn_id))
        .filter(tool_output_chunk::Column::ItemId.eq(item_id))
        .filter(tool_output_chunk::Column::Ordinal.gt(after))
        .order_by_asc(tool_output_chunk::Column::Ordinal)
        .limit(128)
        .all(&store.connection)
        .await?;
    Ok(rows.into_iter().map(|row| (row.ordinal, row)).collect())
}

// New work policy is two fixed scalars in the existing durable progress receipt.
// Decode before writer admission; immutable observation binds its exact owner.
fn prepare_work_policy(
    n: &pioneer_protocol::ItemDeltaNotification,
) -> Result<Option<(chrono::DateTime<chrono::FixedOffset>, i64, String)>> {
    let Some(payload) = &n.payload else {
        return Ok(None);
    };
    if payload["stage"].as_str() != Some("attempt_work_started") {
        return Ok(None);
    }
    ensure!(
        n.stream == Some(pioneer_protocol::ItemDeltaStream::ToolProgress),
        "invalid tool work policy stream"
    );
    struct FrameLimit(usize);
    impl std::io::Write for FrameLimit {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|size| *size <= 4096)
                .ok_or_else(|| std::io::Error::other("tool work policy exceeds frame limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(FrameLimit(0), payload)?;
    let metadata: pioneer_protocol::ToolMetadata =
        serde_json::from_value(payload["metadata"].clone())?;
    let policy = metadata.to_json();
    ensure!(
        policy["schemaVersion"].as_u64() == Some(1)
            && policy.as_object().is_some_and(|p| p.len() == 2),
        "unsupported tool work policy"
    );
    let deadline = policy["deadlineAtUnixMs"]
        .as_i64()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .ok_or_else(|| anyhow::anyhow!("invalid tool work deadline"))?
        .fixed_offset();
    let observation = &payload["observation"];
    let attempt = observation["attempt_id"]
        .as_u64()
        .and_then(|v| i64::try_from(v).ok())
        .filter(|v| *v > 0)
        .ok_or_else(|| anyhow::anyhow!("missing tool work attempt"))?;
    let trace = observation["trace_id"]
        .as_str()
        .filter(|v| !v.is_empty() && v.len() <= 256)
        .ok_or_else(|| anyhow::anyhow!("missing tool work trace"))?
        .to_owned();
    ensure!(
        observation["turn_id"].as_str() == Some(n.turn_id.as_str())
            && observation["tool_call_id"].as_str() == Some(n.item_id.as_str()),
        "tool work observation scope mismatch"
    );
    Ok(Some((deadline, attempt, trace)))
}

pub(crate) async fn has_native_tool_work_policy(
    store: &CrudStore,
    turn_id: &str,
    item_id: &str,
) -> Result<bool> {
    use sea_orm::FromQueryResult;
    #[derive(FromQueryResult)]
    struct Receipt {
        metadata: String,
    }
    let receipt = tool_output_chunk::Entity::find().select_only()
        .column_as(Expr::cust("CASE WHEN length(CAST(metadata AS BLOB)) <= 4096 THEN metadata ELSE '' END"), "metadata")
        .filter(tool_output_chunk::Column::TurnId.eq(turn_id))
        .filter(tool_output_chunk::Column::ItemId.eq(item_id))
        .filter(Expr::cust("json_extract(metadata, '$.stage') = 'attempt_work_started'"))
        .filter(Expr::cust("EXISTS (SELECT 1 FROM turn_item_attempt a WHERE a.turn_id = tool_output_chunk.turn_id AND a.item_id = tool_output_chunk.item_id AND a.status = 'running' AND a.attempt_number = json_extract(tool_output_chunk.metadata, '$.observation.attempt_id') AND json_extract(a.payload, '$.observation.traceId') = json_extract(tool_output_chunk.metadata, '$.observation.trace_id'))"))
        .order_by_desc(tool_output_chunk::Column::Ordinal).limit(1)
        .into_model::<Receipt>().one(&store.connection).await?;
    let Some(receipt) = receipt else {
        return Ok(false);
    };
    let payload: serde_json::Value = serde_json::from_str(&receipt.metadata)?;
    let notification = pioneer_protocol::ItemDeltaNotification {
        workspace_id: String::new(),
        thread_id: String::new(),
        turn_id: turn_id.into(),
        item_id: item_id.into(),
        delta: String::new(),
        stream: Some(pioneer_protocol::ItemDeltaStream::ToolProgress),
        payload: Some(payload),
        markdown: None,
        markdown_version: None,
    };
    Ok(prepare_work_policy(&notification)?.is_some())
}
