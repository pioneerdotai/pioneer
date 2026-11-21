//! Authorization, validation, and monotonic advancement of a viewed position.
use anyhow::{Context, Result};
use pioneer_crud_access::authorization_scope::principal_current_thread_access_kind;
use pioneer_protocol::{PrincipalId, ThreadReadResponse};
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::TransactionTrait;
use std::{fmt, time::Duration};

use crate::{
    thread_metadata,
    thread_state::{
        advance_thread_read_cursor, find_thread_read_cursor, thread_read_cursor_from_model,
    },
    thread_timeline_projection::{
        BLOCK_KIND_ASSISTANT_MESSAGE, BLOCK_KIND_USER_MESSAGE, count_unread_message_blocks,
        find_thread_timeline_block_by_id, find_user_message_block_by_turn_id,
    },
    turn_reads,
    values::unix_to_datetime,
};

#[derive(Debug, Clone)]
pub struct MarkThreadReadRequest {
    pub through_block: Option<pioneer_protocol::ThreadReadPosition>,
    pub workspace_id: String,
    pub thread_id: String,
    pub principal_id: PrincipalId,
    pub through_turn_id: String,
    pub read_at_unix: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadReadFailure {
    InvalidTarget,
}

impl fmt::Display for ThreadReadFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("thread read target was not found")
    }
}

impl std::error::Error for ThreadReadFailure {}

pub async fn mark_thread_read(
    db: &SqliteDatabase,
    request: MarkThreadReadRequest,
) -> Result<ThreadReadResponse> {
    retry_with_backoff(
        || {
            let request = request.clone();
            async move { mark_thread_read_once(db, request).await }
        },
        is_retryable_database_conflict,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

async fn mark_thread_read_once(
    db: &SqliteDatabase,
    request: MarkThreadReadRequest,
) -> Result<ThreadReadResponse> {
    let principal_id = request.principal_id.to_string();
    let transaction = db
        .begin()
        .await
        .context("failed to begin thread read transaction")?;

    let outcome = async {
        if principal_current_thread_access_kind(
            &transaction,
            request.workspace_id.as_str(),
            request.thread_id.as_str(),
            &request.principal_id,
        )
        .await?
        .is_none()
        {
            return Err(anyhow::Error::new(ThreadReadFailure::InvalidTarget));
        }
        let thread_model =
            thread_metadata::find_thread_by_id(&transaction, request.thread_id.as_str())
                .await?
                .filter(|thread| thread.workspace_id == request.workspace_id)
                .ok_or_else(|| anyhow::Error::new(ThreadReadFailure::InvalidTarget))?;
        let block = if let Some(position) = &request.through_block {
            find_thread_timeline_block_by_id(
                &transaction,
                request.thread_id.as_str(),
                &position.block_id,
            )
            .await?
            .filter(|block| block.sort_key == position.sort_key)
        } else {
            find_user_message_block_by_turn_id(
                &transaction,
                request.thread_id.as_str(),
                request.through_turn_id.as_str(),
            )
            .await?
        }
        .filter(|block| {
            block.workspace_id == request.workspace_id
                && block.turn_id.as_deref() == Some(request.through_turn_id.as_str())
        })
        .ok_or_else(|| anyhow::Error::new(ThreadReadFailure::InvalidTarget))?;
        let turn_model = turn_reads::find_turn_by_thread_and_id(
            &transaction,
            request.thread_id.as_str(),
            request.through_turn_id.as_str(),
        )
        .await?
        .ok_or_else(|| anyhow::Error::new(ThreadReadFailure::InvalidTarget))?;
        let valid = match block.block_kind.as_str() {
            BLOCK_KIND_USER_MESSAGE => {
                // An exact canonical message is a viewed position even
                // when authored by an agent. Unread eligibility is separate.
                request.through_block.is_some()
                    || (turn_model.turn_kind == "conversation"
                        && turn_model.origin == "user"
                        && turn_model.initiated_by_actor_kind.as_deref() == Some("principal")
                        && turn_model.initiated_by_actor_id.is_some())
            }
            BLOCK_KIND_ASSISTANT_MESSAGE if block.source_kind.as_deref() == Some("turn_item") => {
                if let Some(item_id) = block.source_key.as_deref() {
                    turn_reads::is_completed_agent_message(&transaction, &turn_model.id, item_id)
                        .await?
                } else {
                    false
                }
            }
            _ => false,
        };
        if !valid {
            return Err(anyhow::Error::new(ThreadReadFailure::InvalidTarget));
        }

        advance_thread_read_cursor(
            &transaction,
            &request.principal_id,
            request.thread_id.as_str(),
            block.sort_key.as_str(),
            turn_model.id.as_str(),
            unix_to_datetime(request.read_at_unix),
        )
        .await?;
        let cursor_model = find_thread_read_cursor(
            &transaction,
            &request.principal_id,
            request.thread_id.as_str(),
        )
        .await?
        .context("thread read cursor is missing after monotonic upsert")?;
        let cursor = thread_read_cursor_from_model(&cursor_model);
        Ok((thread_model.workspace_id, cursor))
    }
    .await;

    match outcome {
        Ok((workspace_id, cursor)) => {
            transaction
                .commit()
                .await
                .context("failed to commit thread read transaction")?;
            // Counting can scan an arbitrarily long thread history. The
            // cursor mutation above is the only atomic write; perform the
            // derived response count through the reader after releasing
            // the physical writer.
            let unread_count = count_unread_message_blocks(
                db,
                request.thread_id.as_str(),
                principal_id.as_str(),
                cursor.sort_key.as_str(),
            )
            .await?;
            Ok(ThreadReadResponse {
                workspace_id,
                thread_id: request.thread_id,
                cursor,
                unread_count,
            })
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::insert_unread_test_turn;
    use crate::thread_mutations::upsert_thread_with_creator;
    use crate::thread_timeline_projection::count_unread_message_blocks_for_threads;
    use pioneer_crud_access::identity::{create_gateway_singleton, create_superuser};
    use pioneer_protocol::{
        GatewayId, PersistedActorRef, Thread, ThreadMode, ThreadOriginKind,
        ThreadSidebarVisibility, ThreadStatus,
    };
    use sea_orm::ConnectionTrait;

    #[tokio::test]
    async fn thread_read_cursor_is_monotonic_and_unread_is_authoritative() {
        pioneer_postgres_test::with_database(async |fixture| {
            let workspace_id = "ws_unread_authoritative";
            let thread_id = "T00000000000000000001";
            let gateway_id = GatewayId::new("G00000000000000000001").expect("gateway id");
            let reader_id = PrincipalId::new("P00000000000000000001").expect("principal id");
            let other_id = PrincipalId::new("P00000000000000000002").expect("principal id");
            fixture.connection().execute_unprepared(
                "INSERT INTO workspace (id,name) VALUES ('ws_unread_authoritative','Unread fixture')",
            ).await?;
            let db = SqliteDatabase::new(fixture.connect_read_only().await?, fixture.connection().clone());
            let now = unix_to_datetime(1_700_000_000);
            create_gateway_singleton(&db, &gateway_id, 0, now)
                .await
                .expect("gateway identity should persist");
            create_superuser(
                &db,
                &reader_id,
                &gateway_id,
                "Reader",
                "reader",
                "reader",
                now,
                0,
            )
            .await
            .expect("reader principal should persist");
            let thread = Thread {
                context_window: None,
                service_mode: None,
                context_authority: None,
                workspace_id: workspace_id.to_owned(),
                id: thread_id.to_owned(),
                name: None,
                preview: String::new(),
                preview_author: None,
                mode: ThreadMode::Message,
                model: String::new(),
                model_provider: String::new(),
                reasoning_effort: None,
                created_at: 1_700_000_000,
                updated_at: 1_700_000_000,
                status: ThreadStatus::Active,
                origin_kind: ThreadOriginKind::Collaborative,
                sidebar_visibility: ThreadSidebarVisibility::Visible,
                agent_nickname: None,
                agent_role: None,
                visibility: Some(pioneer_protocol::ThreadVisibility::Workspace),
                turns: Vec::new(),
            };
            let write = db.begin().await?;
            upsert_thread_with_creator(&write, &thread, &PersistedActorRef::Principal(reader_id.clone()), now, now)
                .await
                .expect("thread should persist");
            write.commit().await?;

            let own_turn_id = "O00000000000000000001";
            let message_turn_id = "M00000000000000000001";
            let chat_turn_id = "C00000000000000000001";
            let agent_turn_id = "A00000000000000000001";
            let system_turn_id = "S00000000000000000001";
            let deleted_turn_id = "D00000000000000000001";
            for (turn_id, sort_key, mode, actor_id, deleted) in [
                (
                    own_turn_id,
                    "001",
                    "message",
                    Some(reader_id.as_ref()),
                    false,
                ),
                (
                    message_turn_id,
                    "002",
                    "message",
                    Some(other_id.as_ref()),
                    false,
                ),
                (chat_turn_id, "003", "chat", Some(other_id.as_ref()), false),
                (
                    agent_turn_id,
                    "004",
                    "agent",
                    Some(other_id.as_ref()),
                    false,
                ),
                (system_turn_id, "005", "message", None, false),
                (
                    deleted_turn_id,
                    "006",
                    "message",
                    Some(other_id.as_ref()),
                    true,
                ),
            ] {
                insert_unread_test_turn(
                    &db,
                    workspace_id,
                    thread_id,
                    turn_id,
                    sort_key,
                    mode,
                    actor_id,
                    deleted,
                )
                .await;
            }

            let counts = count_unread_message_blocks_for_threads(
                &db, &[thread_id.to_owned()], reader_id.as_str(),
            )
                .await
                .expect("unread batch should succeed");
            assert_eq!(counts.get(thread_id), Some(&3));

            let read_chat = mark_thread_read(&db, MarkThreadReadRequest {
                    through_block: None,
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    principal_id: reader_id.clone(),
                    through_turn_id: chat_turn_id.to_owned(),
                    read_at_unix: 1_700_000_200,
                })
                .await
                .expect("chat cursor target should succeed");
            assert_eq!(read_chat.cursor.sort_key, "003");
            assert_eq!(read_chat.unread_count, 1);

            let stale = mark_thread_read(&db, MarkThreadReadRequest {
                    through_block: None,
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    principal_id: reader_id.clone(),
                    through_turn_id: message_turn_id.to_owned(),
                    read_at_unix: 1_700_000_201,
                })
                .await
                .expect("stale cursor target should be idempotent");
            assert_eq!(stale.cursor.sort_key, "003");
            assert_eq!(stale.unread_count, 1);

            let older = mark_thread_read(&db, MarkThreadReadRequest {
                through_block: None,
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                principal_id: reader_id.clone(),
                through_turn_id: message_turn_id.to_owned(),
                read_at_unix: 1_700_000_202,
            });
            let newer = mark_thread_read(&db, MarkThreadReadRequest {
                through_block: None,
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                principal_id: reader_id.clone(),
                through_turn_id: agent_turn_id.to_owned(),
                read_at_unix: 1_700_000_203,
            });
            let (older, newer) = tokio::join!(older, newer);
            older.expect("concurrent stale cursor should succeed");
            newer.expect("concurrent newer cursor should succeed");
            let cursor = find_thread_read_cursor(&db, &reader_id, thread_id)
                .await
                .expect("cursor query should succeed")
                .expect("cursor should exist");
            assert_eq!(cursor.last_read_sort_key, "004");

            let deleted = mark_thread_read(&db, MarkThreadReadRequest {
                    through_block: None,
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    principal_id: reader_id.clone(),
                    through_turn_id: deleted_turn_id.to_owned(),
                    read_at_unix: 1_700_000_204,
                })
                .await
                .expect("an accessible tombstone remains a valid cursor position");
            assert_eq!(deleted.cursor.sort_key, "006");
            assert_eq!(deleted.unread_count, 0);

            let error = mark_thread_read(&db, MarkThreadReadRequest {
                    through_block: None,
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    principal_id: reader_id,
                    through_turn_id: system_turn_id.to_owned(),
                    read_at_unix: 1_700_000_205,
                })
                .await
                .expect_err("system target must fail closed");
            assert_eq!(
                error.downcast_ref::<ThreadReadFailure>(),
                Some(&ThreadReadFailure::InvalidTarget)
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}
