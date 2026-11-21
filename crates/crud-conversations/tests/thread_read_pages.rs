use anyhow::{Context, Result};
use pioneer_crud_conversations::thread_lineage::list_children_page;
use pioneer_crud_conversations::thread_metadata::{find_thread_by_id, update_thread_status};
use pioneer_crud_conversations::thread_state::{
    is_list_conversation, list_read_candidates_page, list_threads_by_workspace,
};
use pioneer_entity::{thread, thread_lineage, workspace};
use pioneer_postgres_test::with_database;
use pioneer_protocol::ThreadStatus;
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DbBackend, EntityTrait, Set, Statement, TransactionTrait,
    entity::prelude::DateTimeWithTimeZone,
};
use std::time::Duration;

const WORKSPACE: &str = "read-pages";

async fn seed_workspace(db: &impl ConnectionTrait) -> Result<()> {
    workspace::Entity::insert(workspace::ActiveModel {
        id: Set(WORKSPACE.into()),
        name: Set("Read pages".into()),
        ..Default::default()
    })
    .exec_without_returning(db)
    .await?;
    Ok(())
}

fn conversation(id: &str, at: DateTimeWithTimeZone) -> thread::ActiveModel {
    thread::ActiveModel {
        id: Set(id.into()),
        workspace_id: Set(WORKSPACE.into()),
        preview: Set(String::new()),
        mode: Set("agent".into()),
        model: Set("fixture".into()),
        model_provider: Set("fixture".into()),
        status: Set("active".into()),
        updated_at: Set(at),
        created_at: Set(at),
        ..Default::default()
    }
}

#[tokio::test]
async fn conversation_pages_include_archived_and_keep_exact_position_after_updates() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        seed_workspace(db).await?;
        let index = db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT pg_get_indexdef('idx_thread_workspace_created_id'::regclass) AS definition",
            ))
            .await?
            .context("missing thread creation index")?
            .try_get::<String>("", "definition")?;
        assert!(index.ends_with("USING btree (workspace_id, created_at, id)"));

        // Dates differ at PostgreSQL's one-microsecond precision. Equal dates
        // must cross the page boundary by descending ID.
        for (id, date) in [
            ("z-old", "2026-01-01T00:00:00.123456Z"),
            ("a-new", "2026-01-01T00:00:01Z"),
            ("b-tie", "2026-01-01T00:00:00.123458Z"),
            ("c-tie", "2026-01-01T00:00:00.123458Z"),
            ("d-fraction", "2026-01-01T00:00:00.123457Z"),
        ] {
            let mut row = conversation(id, date.parse()?);
            if id == "b-tie" {
                row.status = Set("closed".into());
            }
            row.insert(db).await?;
        }
        let newer = "2026-01-01T00:00:02Z".parse()?;
        for (id, origin, visibility) in [
            ("hidden", "user", "hidden"),
            ("task", "task_run", "visible"),
            ("service", "system", "visible"),
        ] {
            let mut row = conversation(id, newer);
            row.origin_kind = Set(origin.into());
            row.sidebar_visibility = Set(visibility.into());
            row.insert(db).await?;
            assert!(!is_list_conversation(db, WORKSPACE, id).await?);
        }

        let first = list_read_candidates_page(db, WORKSPACE, None, 2).await?;
        assert_eq!(
            first.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["a-new", "c-tie"]
        );
        assert_eq!(first[1].created_at, "2026-01-01T00:00:00.123458Z");
        assert!(is_list_conversation(db, WORKSPACE, "b-tie").await?);

        thread::Entity::update(thread::ActiveModel {
            id: Set("a-new".into()),
            updated_at: Set(newer),
            ..Default::default()
        })
        .exec(db)
        .await?;
        thread::Entity::delete_by_id("c-tie").exec(db).await?;
        conversation("0-latest", "2026-01-01T00:00:03Z".parse()?)
            .insert(db)
            .await?;

        let next = list_read_candidates_page(db, WORKSPACE, first.last(), 2).await?;
        assert_eq!(
            next.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["b-tie", "d-fraction"]
        );
        let tail = list_read_candidates_page(db, WORKSPACE, next.last(), 2).await?;
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].id, "z-old");
        assert_eq!(
            list_read_candidates_page(db, WORKSPACE, None, 1).await?[0].id,
            "0-latest"
        );
        assert!(
            list_read_candidates_page(db, "unrelated", None, 2)
                .await?
                .is_empty()
        );
        assert!(
            list_read_candidates_page(db, WORKSPACE, tail.last(), 2)
                .await?
                .is_empty()
        );
        assert!(
            list_read_candidates_page(db, WORKSPACE, None, 201)
                .await
                .is_err()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn updated_thread_order_is_applied_before_the_workspace_limit() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        seed_workspace(db).await?;
        let same = "2026-01-01T00:00:01Z".parse()?;
        let early = "2026-01-01T00:00:00Z".parse()?;
        for (id, updated) in [("b", same), ("A", same), ("old", early), ("a", same)] {
            let mut row = conversation(id, early);
            row.updated_at = Set(updated);
            row.insert(db).await?;
        }
        let rows = list_threads_by_workspace(db, WORKSPACE, 2).await?;
        assert_eq!(
            rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["A", "a"]
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn child_pages_continue_beyond_128_and_are_scoped_to_actual_parent() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        let expected = (0..151)
            .map(|i| format!("child-{i:03}"))
            .collect::<Vec<_>>();
        let at: DateTimeWithTimeZone = "2026-01-01T00:00:00Z".parse()?;
        let lineage = |id: &str, parent: &str, depth| thread_lineage::ActiveModel {
            child_thread_id: Set(id.into()),
            parent_thread_id: Set(parent.into()),
            root_thread_id: Set("root".into()),
            depth: Set(depth),
            created_at: Set(at),
            ..Default::default()
        };
        let rows = expected
            .iter()
            .map(|id| lineage(id, "intermediate", 2))
            .chain([
                lineage("sibling", "root", 1),
                lineage("grandchild", "child-000", 3),
            ]);
        thread_lineage::Entity::insert_many(rows)
            .exec_without_returning(db)
            .await?;

        let mut actual = Vec::new();
        loop {
            let after = actual.last().map(String::as_str);
            let page = list_children_page(db, "intermediate", after, 17).await?;
            if page.is_empty() {
                break;
            }
            actual.extend(page.into_iter().map(|row| row.child_thread_id));
        }
        assert_eq!(actual, expected);
        let descendants = list_children_page(db, "child-000", None, 17).await?;
        assert_eq!(descendants.len(), 1);
        assert_eq!(descendants[0].child_thread_id, "grandchild");
        assert!(
            list_children_page(db, "intermediate", None, 0)
                .await
                .is_err()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_thread_status_updates_preserve_latest_activity_timestamp() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        seed_workspace(db).await?;
        let initial = "2026-01-01T00:00:00Z".parse()?;
        let earlier = "2026-01-01T00:00:01Z".parse()?;
        let later = "2026-01-01T00:00:02Z".parse()?;
        for (id, first_at, second_at) in [
            ("later-then-earlier", later, earlier),
            ("earlier-then-later", earlier, later),
        ] {
            conversation(id, initial).insert(db).await?;
            let first = db.begin().await?;
            update_thread_status(&first, id, ThreadStatus::Idle, first_at).await?;
            let second = db.begin().await?;
            let pid: i32 = second
                .query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT pg_backend_pid() AS pid",
                ))
                .await?
                .context("missing competing backend PID")?
                .try_get("", "pid")?;
            let writer = tokio::spawn(async move {
                update_thread_status(&second, id, ThreadStatus::Closed, second_at).await?;
                second.commit().await?;
                Ok::<_, anyhow::Error>(())
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let blocked: bool = db
                        .query_one_raw(Statement::from_sql_and_values(
                            DbBackend::Postgres,
                            "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                            [pid.into()],
                        ))
                        .await?
                        .context("missing lock observation")?
                        .try_get("", "blocked")?;
                    if blocked {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("second status update did not wait for the first")??;
            first.commit().await?;
            writer.await??;

            let committed = find_thread_by_id(db, id).await?.context("missing thread")?;
            assert_eq!(committed.status, "closed");
            assert_eq!(committed.updated_at, later);
        }
        Ok(())
    })
    .await
}
