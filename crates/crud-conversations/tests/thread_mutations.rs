use anyhow::{Context, Result};
use pioneer_crud_access::membership::PersistedThreadAccessClass;
use pioneer_crud_conversations::thread_metadata::find_thread_by_id;
use pioneer_crud_conversations::thread_mutations::upsert_agent_thread_with_creator;
use pioneer_postgres_test::with_database;
use pioneer_protocol::{
    PersistedActorRef, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
    ThreadVisibility,
};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};
use std::time::Duration;

#[tokio::test]
async fn concurrent_creation_cannot_overwrite_the_winning_thread_access_projection() -> Result<()> {
    for commit_first in [true, false] {
        with_database(async |fixture| {
            let db = fixture.connection();
            db.execute_unprepared("INSERT INTO workspace (id,name) VALUES ('ws','Workspace')")
                .await?;
            let now = "2026-11-19T10:00:00Z".parse::<chrono::DateTime<chrono::FixedOffset>>()?;
            let public = Thread {
                context_window: None,
                service_mode: None,
                context_authority: None,
                workspace_id: "ws".into(),
                id: "new-thread".into(),
                name: Some("Public".into()),
                preview: String::new(),
                preview_author: None,
                mode: ThreadMode::Agent,
                model: "fixture".into(),
                model_provider: "fixture".into(),
                reasoning_effort: None,
                created_at: now.timestamp(),
                updated_at: now.timestamp(),
                status: ThreadStatus::Active,
                origin_kind: ThreadOriginKind::User,
                sidebar_visibility: ThreadSidebarVisibility::Visible,
                agent_nickname: None,
                agent_role: None,
                visibility: Some(ThreadVisibility::Workspace),
                turns: Vec::new(),
            };
            let internal = Thread {
                name: Some("Internal".into()),
                sidebar_visibility: ThreadSidebarVisibility::Hidden,
                visibility: None,
                ..public.clone()
            };
            let first = db.begin().await?;
            assert!(
                upsert_agent_thread_with_creator(
                    &first,
                    &internal,
                    &PersistedActorRef::System,
                    PersistedThreadAccessClass::Internal,
                    now,
                    now,
                )
                .await?
            );

            let competing = fixture.connect().await?;
            let second = competing.begin().await?;
            let pid: i32 = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid()",
                ))
                .await?
                .context("missing competing backend PID")?
                .try_get_by_index(0)?;
            let writer = tokio::spawn(async move {
                let result = upsert_agent_thread_with_creator(
                    &second,
                    &public,
                    &PersistedActorRef::System,
                    PersistedThreadAccessClass::Workspace,
                    now,
                    now,
                )
                .await;
                if result.is_ok() {
                    second.commit().await?;
                } else {
                    second.rollback().await?;
                }
                Ok::<_, anyhow::Error>(result)
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked: bool = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0",
                            [pid.into()],
                        ))
                        .await?
                        .context("missing lock observation")?
                        .try_get_by_index(0)?;
                    if blocked {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("second writer did not reach the thread ID conflict")??;
            let (access, visibility, name) = if commit_first {
                first.commit().await?;
                assert!(writer.await??.is_err());
                ("internal", "hidden", "Internal")
            } else {
                first.rollback().await?;
                assert!(writer.await???);
                ("workspace", "visible", "Public")
            };
            let stored = find_thread_by_id(db, "new-thread")
                .await?
                .context("thread disappeared")?;
            assert_eq!(stored.access_class, access);
            assert_eq!(stored.sidebar_visibility, visibility);
            assert_eq!(stored.name.as_deref(), Some(name));
            competing.close().await?;
            Ok(())
        })
        .await?;
    }
    Ok(())
}
