use anyhow::{Context, Result};
use pioneer_crud_payloads::{
    json_fields::first_object_fields,
    storage::{self, CapturedPayload, Selection},
};
use pioneer_entity::turn_item as item;
use pioneer_protocol::{ToolErrorClass, ToolOutcomeStatus, TurnItemType};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    entity::prelude::DateTimeWithTimeZone,
    sea_query::{Expr, ExprTrait},
};
use std::time::Duration;

/// Tool facts for post-turn processing. Arguments and output are never exposed.
pub struct PostTurnToolSummary {
    pub item_id: String,
    pub item_type: TurnItemType,
    pub tool_name: String,
    pub attempt_number: u32,
    pub success: bool,
    pub outcome_status: Option<ToolOutcomeStatus>,
    pub error_class: Option<ToolErrorClass>,
}

type ToolCursor = (DateTimeWithTimeZone, String);

struct CapturedToolItem {
    cursor: ToolCursor,
    item_type: String,
    attempt: i64,
    payload: ToolPayload,
}

pub async fn list(
    db: &SqliteDatabase,
    turn_id: &str,
    limit: usize,
) -> Result<Vec<PostTurnToolSummary>> {
    let limit = limit.clamp(1, 129);
    let mut after = None;
    let mut result = Vec::new();
    while result.len() < limit {
        let candidate = retry_with_backoff(
            || async {
                let reader = db.begin_read().await?;
                let candidate = capture_next(&reader, turn_id, after.as_ref()).await?;
                reader.commit().await?;
                Ok(candidate)
            },
            is_retryable_database_conflict,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await?;
        let Some(candidate) = candidate else { break };
        after = Some(candidate.cursor.clone());
        if let Some(summary) = candidate.into_summary()? {
            result.push(summary);
        }
    }
    Ok(result)
}

enum ToolPayload {
    Inline(String),
    Portable(CapturedPayload),
}

/// Read one terminal item under the caller's snapshot. Advance by the raw row
/// even when the decoder later finds that the item has no tool result.
async fn capture_next<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    after: Option<&ToolCursor>,
) -> Result<Option<CapturedToolItem>> {
    use item::Column as Col;
    let mut query = item::Entity::find()
        .select_only()
        .columns([
            Col::CreatedAt,
            Col::ItemId,
            Col::ItemType,
            Col::ActiveAttemptNumber,
        ])
        .expr_as(
            Expr::col((item::Entity, Col::Payload)).cast_as("text"),
            "inline_payload",
        )
        .column(Col::PayloadPayloadId)
        .filter(Col::TurnId.eq(turn_id))
        .filter(Col::Status.is_in(["completed", "failed"]))
        .order_by_asc(Col::CreatedAt)
        .order_by_asc(Col::ItemId)
        .limit(1);
    if let Some((time, id)) = after {
        query = query.filter(
            Condition::any().add(Col::CreatedAt.gt(*time)).add(
                Condition::all()
                    .add(Col::CreatedAt.eq(*time))
                    .add(Col::ItemId.gt(id)),
            ),
        );
    }
    let Some((time, id, item_type, attempt, inline, portable)) = query
        .into_tuple::<(
            DateTimeWithTimeZone,
            String,
            String,
            i64,
            Option<String>,
            Option<String>,
        )>()
        .one(db)
        .await?
    else {
        return Ok(None);
    };
    let payload = match (inline, portable) {
        (Some(text), None) => ToolPayload::Inline(text),
        (None, Some(id)) => ToolPayload::Portable(
            storage::capture(db, &id, Selection::Full)
                .await?
                .context("turn item payload did not exist in the read snapshot")?,
        ),
        _ => anyhow::bail!("turn item payload has an invalid storage reference"),
    };
    Ok(Some(CapturedToolItem {
        cursor: (time, id),
        item_type,
        attempt,
        payload,
    }))
}

impl CapturedToolItem {
    fn into_summary(self) -> Result<Option<PostTurnToolSummary>> {
        let text = match self.payload {
            ToolPayload::Inline(text) => text,
            ToolPayload::Portable(payload) => payload.into_text()?,
        };
        let fields = first_object_fields(&text, &["toolName", "success", "outcome"])?;
        let Some(tool) = fields.get("toolName").and_then(|value| value.as_str()) else {
            return Ok(None);
        };
        let Some(success) = fields.get("success").and_then(|value| value.as_bool()) else {
            return Ok(None);
        };
        Ok(Some(PostTurnToolSummary {
            item_id: self.cursor.1,
            item_type: crate::turn_item::turn_item_type_from_db(&self.item_type)
                .context("unknown post-turn tool item type")?,
            tool_name: tool
                .split('\0')
                .next()
                .unwrap_or("")
                .chars()
                .take(160)
                .collect(),
            attempt_number: u32::try_from(self.attempt)?,
            success,
            outcome_status: fields
                .pointer("/outcome/status")
                .filter(|v| !v.is_null())
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()?,
            error_class: fields
                .pointer("/outcome/errorClass")
                .filter(|v| !v.is_null())
                .map(|v| serde_json::from_value(v.clone()))
                .transpose()?,
        }))
    }
}
