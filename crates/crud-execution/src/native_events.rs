//! Scoped native runtime journal rows and idempotent append.
use anyhow::{Context, Result};
use pioneer_entity::cli_runtime_native_event;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use serde::{Serialize, de::DeserializeOwned};

pub fn serialize_cli_runtime_json<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("failed to serialize CLI runtime JSON payload")
}

pub fn deserialize_cli_runtime_json<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).context("failed to deserialize CLI runtime JSON payload")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeNativeEventRecord {
    pub id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_method: String,
    pub payload_redacted_json: String,
    pub sequence: i64,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimeNativeEvent {
    pub id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_method: String,
    pub payload_redacted_json: String,
    pub sequence: i64,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CliRuntimeNativeEventListFilter {
    pub runtime_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_method: Option<String>,
    pub limit: Option<u64>,
}

pub async fn append_native_event<C: ConnectionTrait>(
    db: &C,
    event: NewCliRuntimeNativeEvent,
) -> Result<CliRuntimeNativeEventRecord> {
    cli_runtime_native_event::Entity::insert(active_native_event_from_new(event))
        .exec_with_returning(db)
        .await
        .context("failed to append CLI runtime native event")
        .and_then(native_event_record_from_model)
}

/// The Claude completion boundary uses a deterministic event ID. A replay
/// must recover the same row, while an accidental ID collision must fail.
pub async fn append_native_event_if_absent<C: ConnectionTrait>(
    db: &C,
    event: NewCliRuntimeNativeEvent,
) -> Result<CliRuntimeNativeEventRecord> {
    append_native_event_if_absent_with_outcome(db, event)
        .await
        .map(|(record, _)| record)
}

pub async fn append_native_event_if_absent_with_outcome<C: ConnectionTrait>(
    db: &C,
    event: NewCliRuntimeNativeEvent,
) -> Result<(CliRuntimeNativeEventRecord, bool)> {
    let id = event.id.clone();
    let inserted =
        cli_runtime_native_event::Entity::insert(active_native_event_from_new(event.clone()))
            .on_conflict(
                OnConflict::column(cli_runtime_native_event::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(db)
            .await
            .context("failed to persist idempotent CLI runtime native event")?;
    let existing = find_native_event(db, id.as_str())
        .await?
        .context("idempotent CLI runtime native event disappeared")?;
    anyhow::ensure!(
        existing.runtime_id == event.runtime_id
            && existing.runtime_kind == event.runtime_kind
            && existing.workspace_id == event.workspace_id
            && existing.thread_id == event.thread_id
            && existing.turn_id == event.turn_id
            && existing.native_thread_id == event.native_thread_id
            && existing.native_turn_id == event.native_turn_id
            && existing.native_method == event.native_method
            && existing.payload_redacted_json == event.payload_redacted_json,
        "idempotent CLI runtime native event conflicts with existing identity"
    );
    Ok((existing, inserted != 0))
}

pub async fn find_native_event<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CliRuntimeNativeEventRecord>> {
    cli_runtime_native_event::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime native event")?
        .map(native_event_record_from_model)
        .transpose()
}

pub async fn list_native_events<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimeNativeEventListFilter,
) -> Result<Vec<CliRuntimeNativeEventRecord>> {
    let query = filter_native_events(cli_runtime_native_event::Entity::find(), filter);
    query
        .order_by_asc(cli_runtime_native_event::Column::Sequence)
        .order_by_asc(cli_runtime_native_event::Column::Id)
        .all(db)
        .await
        .context("failed to list CLI runtime native events")?
        .into_iter()
        .map(native_event_record_from_model)
        .collect()
}

pub async fn latest_native_event<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimeNativeEventListFilter,
) -> Result<Option<CliRuntimeNativeEventRecord>> {
    filter_native_events(cli_runtime_native_event::Entity::find(), filter)
        .order_by_desc(cli_runtime_native_event::Column::Sequence)
        .order_by_desc(cli_runtime_native_event::Column::Id)
        .one(db)
        .await
        .context("failed to query latest CLI runtime native event")?
        .map(native_event_record_from_model)
        .transpose()
}

fn filter_native_events(
    mut query: sea_orm::Select<cli_runtime_native_event::Entity>,
    filter: CliRuntimeNativeEventListFilter,
) -> sea_orm::Select<cli_runtime_native_event::Entity> {
    if let Some(runtime_id) = filter.runtime_id {
        query = query.filter(cli_runtime_native_event::Column::RuntimeId.eq(runtime_id));
    }
    if let Some(thread_id) = filter.thread_id {
        query = query.filter(cli_runtime_native_event::Column::ThreadId.eq(thread_id));
    }
    if let Some(turn_id) = filter.turn_id {
        query = query.filter(cli_runtime_native_event::Column::TurnId.eq(turn_id));
    }
    if let Some(native_thread_id) = filter.native_thread_id {
        query = query.filter(cli_runtime_native_event::Column::NativeThreadId.eq(native_thread_id));
    }
    if let Some(native_turn_id) = filter.native_turn_id {
        query = query.filter(cli_runtime_native_event::Column::NativeTurnId.eq(native_turn_id));
    }
    if let Some(native_method) = filter.native_method {
        query = query.filter(cli_runtime_native_event::Column::NativeMethod.eq(native_method));
    }
    if let Some(limit) = filter.limit {
        query = query.limit(limit);
    }
    query
}

fn active_native_event_from_new(
    event: NewCliRuntimeNativeEvent,
) -> cli_runtime_native_event::ActiveModel {
    cli_runtime_native_event::ActiveModel {
        id: Set(event.id),
        runtime_id: Set(event.runtime_id),
        runtime_kind: Set(event.runtime_kind),
        workspace_id: Set(event.workspace_id),
        thread_id: Set(event.thread_id),
        turn_id: Set(event.turn_id),
        native_thread_id: Set(event.native_thread_id),
        native_turn_id: Set(event.native_turn_id),
        native_method: Set(event.native_method),
        payload_redacted_json: Set(event.payload_redacted_json),
        sequence: Set(event.sequence),
        created_at: Set(event.created_at),
    }
}

fn native_event_record_from_model(
    model: cli_runtime_native_event::Model,
) -> Result<CliRuntimeNativeEventRecord> {
    Ok(CliRuntimeNativeEventRecord {
        id: model.id,
        runtime_id: model.runtime_id,
        runtime_kind: model.runtime_kind,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        native_thread_id: model.native_thread_id,
        native_turn_id: model.native_turn_id,
        native_method: model.native_method,
        payload_redacted_json: model.payload_redacted_json,
        sequence: model.sequence,
        created_at: model.created_at,
    })
}

pub struct NativeEventRepository<'a> {
    connection: &'a pioneer_sqlite::SqliteDatabase,
}

impl<'a> NativeEventRepository<'a> {
    pub fn new(connection: &'a pioneer_sqlite::SqliteDatabase) -> Self {
        Self { connection }
    }

    async fn run_serialized_write<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        pioneer_sqlite::retry_with_backoff(
            || operation(),
            pioneer_sqlite::is_retryable_database_conflict,
            pioneer_sqlite::DEFAULT_LOCK_RETRY_ATTEMPTS,
            std::time::Duration::from_millis(pioneer_sqlite::DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }
}

use crate::gateway_control::{GatewayControlWake, control_work_change};

impl NativeEventRepository<'_> {
    pub async fn append_cli_runtime_native_event(
        &self,
        event: NewCliRuntimeNativeEvent,
    ) -> Result<CliRuntimeNativeEventRecord> {
        let control_wakes = event
            .turn_id
            .as_ref()
            .map(|id| vec![GatewayControlWake::Activity(id.clone())])
            .unwrap_or_default();
        let cleanup_turns = event.turn_id.iter().cloned().collect::<Vec<_>>();
        let cli_changes = [
            control_work_change(self.connection, "gateway.control", &control_wakes),
            control_work_change(self.connection, "gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            let cleanup_change = crate::terminal_cleanup::Change::native(self.connection);
            let maintenance_result = async {
                self.run_serialized_write(|| async {
                    append_native_event(self.connection, event.clone()).await
                })
                .await
            }
            .await;
            let maintenance_changed = maintenance_result.is_ok();
            if maintenance_result.is_ok() {
                cleanup_change
                    .complete(self.connection, maintenance_changed, &cleanup_turns)
                    .await;
            }
            maintenance_result
        }
        .await;
        if cli_result.is_ok() {
            for cli_change in cli_changes {
                cli_change.completed(cli_result.is_ok());
            }
        }
        cli_result
    }

    pub async fn append_cli_runtime_native_event_if_absent(
        &self,
        event: NewCliRuntimeNativeEvent,
    ) -> Result<CliRuntimeNativeEventRecord> {
        let control_wakes = event
            .turn_id
            .as_ref()
            .map(|id| vec![GatewayControlWake::Activity(id.clone())])
            .unwrap_or_default();
        let cleanup_turns = event.turn_id.iter().cloned().collect::<Vec<_>>();
        let cli_changes = [
            control_work_change(self.connection, "gateway.control", &control_wakes),
            control_work_change(self.connection, "gateway.cli", &control_wakes),
        ];
        let cli_result = async {
            let cleanup_change = crate::terminal_cleanup::Change::native(self.connection);
            let maintenance_result = async {
                self.run_serialized_write(|| async {
                    append_native_event_if_absent_with_outcome(self.connection, event.clone()).await
                })
                .await
            }
            .await;
            let maintenance_changed = maintenance_result
                .as_ref()
                .is_ok_and(|(_, changed)| *changed);
            if maintenance_result.is_ok() {
                cleanup_change
                    .complete(self.connection, maintenance_changed, &cleanup_turns)
                    .await;
            }
            maintenance_result
        }
        .await;
        if let Ok((_, changed)) = &cli_result {
            for cli_change in cli_changes {
                cli_change.completed(*changed);
            }
        }
        cli_result.map(|(record, _)| record)
    }

    pub async fn list_cli_runtime_native_events(
        &self,
        filter: CliRuntimeNativeEventListFilter,
    ) -> Result<Vec<CliRuntimeNativeEventRecord>> {
        list_native_events(self.connection, filter).await
    }

    pub async fn latest_cli_runtime_native_event(
        &self,
        filter: CliRuntimeNativeEventListFilter,
    ) -> Result<Option<CliRuntimeNativeEventRecord>> {
        latest_native_event(self.connection, filter).await
    }
}

#[cfg(test)]
#[path = "native_event_route_tests.rs"]
mod route_tests;
