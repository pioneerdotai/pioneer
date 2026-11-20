use anyhow::{Context, Result, ensure};
use pioneer_entity::{thread, thread_cli_runtime_binding, thread_read_cursor};
use pioneer_protocol::{PrincipalId, ThreadReadCursor};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, LockType, OnConflict, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

pub async fn find_thread_read_cursor<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
    thread_id: &str,
) -> Result<Option<thread_read_cursor::Model>> {
    thread_read_cursor::Entity::find()
        .filter(thread_read_cursor::Column::PrincipalId.eq(principal_id.as_str()))
        .filter(thread_read_cursor::Column::ThreadId.eq(thread_id))
        .one(db)
        .await
        .context("failed to query thread read cursor")
}

pub fn thread_read_cursor_from_model(model: &thread_read_cursor::Model) -> ThreadReadCursor {
    ThreadReadCursor {
        through_turn_id: model.last_read_turn_id.clone(),
        sort_key: model.last_read_sort_key.clone(),
    }
}

/// Concurrent updates can advance a confirmed position only in sort-key order.
/// The caller validates the selected message in the same transaction.
pub async fn advance_thread_read_cursor<C: ConnectionTrait>(
    db: &C,
    principal_id: &PrincipalId,
    thread_id: &str,
    last_read_sort_key: &str,
    last_read_turn_id: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    ensure!(
        !last_read_sort_key.is_empty() && last_read_sort_key.len() <= 128,
        "thread read sort key must contain between 1 and 128 bytes"
    );
    use thread_read_cursor::Column as Field;
    let changed = thread_read_cursor::Entity::insert(thread_read_cursor::ActiveModel {
        principal_id: Set(principal_id.to_string()),
        thread_id: Set(thread_id.to_owned()),
        last_read_sort_key: Set(last_read_sort_key.to_owned()),
        last_read_turn_id: Set(last_read_turn_id.to_owned()),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::columns([Field::PrincipalId, Field::ThreadId])
            .update_columns([
                Field::LastReadSortKey,
                Field::LastReadTurnId,
                Field::UpdatedAt,
            ])
            .action_and_where(
                Expr::col(("excluded", Field::LastReadSortKey)).gt(Expr::col((
                    thread_read_cursor::Entity,
                    Field::LastReadSortKey,
                ))),
            )
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to advance thread read cursor")?;
    Ok(changed > 0)
}

pub struct NativeThreadNameFence<'a> {
    pub workspace_id: &'a str,
    pub runtime_id: &'a str,
    pub native_thread_id: &'a str,
}

pub async fn update_automatic_thread_name<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    name: &str,
    fallback: Option<&str>,
    binding: Option<NativeThreadNameFence<'_>>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    use thread::Column as Field;
    let mut update = thread::Entity::update_many()
        .col_expr(Field::Name, Expr::val(name))
        .col_expr(Field::UpdatedAt, Expr::val(updated_at))
        .filter(Field::Id.eq(thread_id))
        .filter(
            Condition::any()
                .add(Field::Name.is_null())
                .add(Field::Name.eq(""))
                .add_option(fallback.map(|fallback| Field::Name.eq(fallback))),
        )
        .filter(
            Condition::any()
                .add(Field::Name.is_null())
                .add(Field::Name.ne(name)),
        );

    if let Some(binding) = binding {
        use thread_cli_runtime_binding::Column as Binding;
        update = update
            .filter(Field::WorkspaceId.eq(binding.workspace_id))
            .filter(Expr::exists(
                Query::select()
                    .expr(Expr::val(1))
                    .from(thread_cli_runtime_binding::Entity)
                    .and_where(Binding::ThreadId.eq(thread_id))
                    .and_where(Binding::WorkspaceId.eq(binding.workspace_id))
                    .and_where(Binding::RuntimeId.eq(binding.runtime_id))
                    .and_where(Binding::RuntimeKind.eq("codex"))
                    .and_where(Binding::NativeThreadId.eq(binding.native_thread_id))
                    .and_where(Binding::Status.eq("active"))
                    .lock(LockType::Share)
                    .to_owned(),
            ));
    }
    let result = update
        .exec(db)
        .await
        .context("failed to accept automatic thread name")?;
    Ok(result.rows_affected > 0)
}

/// Exact timestamp and ID at the database's creation-order page boundary.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadReadPosition {
    pub created_at: String,
    pub id: String,
}

// Ordinary conversations are independent of archive state and read authorization.
fn list_conversation_condition() -> Condition {
    Condition::all()
        .add(thread::Column::SidebarVisibility.eq("visible"))
        .add(thread::Column::AccessClass.is_in(["private", "workspace"]))
        .add(thread::Column::OriginKind.is_not_in(["task_run", "taskRun", "system"]))
}

/// Revalidate list membership before disclosing a completed page.
pub async fn is_list_conversation<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
) -> Result<bool> {
    Ok(thread::Entity::find_by_id(thread_id)
        .select_only()
        .column(thread::Column::Id)
        .filter(thread::Column::WorkspaceId.eq(workspace_id))
        .filter(list_conversation_condition())
        .into_tuple::<String>()
        .one(db)
        .await
        .context("failed to revalidate thread list membership")?
        .is_some())
}

/// Advance by the last candidate, including rows that the caller's authorization
/// filter hides. A filtered empty page can still have a continuation.
pub async fn list_read_candidates_page<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    after: Option<&ThreadReadPosition>,
    limit: u64,
) -> Result<Vec<ThreadReadPosition>> {
    ensure!(
        (1..=200).contains(&limit),
        "thread candidate page limit must be 1..=200"
    );
    let mut query = thread::Entity::find()
        .select_only()
        .column(thread::Column::Id)
        .column(thread::Column::CreatedAt)
        .filter(thread::Column::WorkspaceId.eq(workspace_id))
        .filter(list_conversation_condition());
    if let Some(after) = after {
        let created_at = DateTimeWithTimeZone::parse_from_rfc3339(&after.created_at)
            .context("invalid thread creation cursor timestamp")?;
        query = query
            // Keep an explicit index range alongside the equal-timestamp ID boundary.
            .filter(thread::Column::CreatedAt.lte(created_at))
            .filter(
                Condition::any()
                    .add(thread::Column::CreatedAt.lt(created_at))
                    .add(
                        Condition::all()
                            .add(thread::Column::CreatedAt.eq(created_at))
                            .add(thread::Column::Id.lt(&after.id)),
                    ),
            );
    }
    let rows = query
        .order_by_desc(thread::Column::CreatedAt)
        .order_by_desc(thread::Column::Id)
        .limit(limit)
        .into_tuple::<(String, DateTimeWithTimeZone)>()
        .all(db)
        .await
        .context("failed to list thread read candidates")?;
    Ok(rows
        .into_iter()
        .map(|(id, created_at)| ThreadReadPosition {
            created_at: created_at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            id,
        })
        .collect())
}

pub async fn list_threads_by_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    limit: u64,
) -> Result<Vec<thread::Model>> {
    thread::Entity::find()
        .filter(thread::Column::WorkspaceId.eq(workspace_id))
        .filter(thread::Column::SidebarVisibility.eq("visible"))
        .order_by_desc(thread::Column::UpdatedAt)
        .order_by_asc(thread::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list threads by workspace")
}
