use anyhow::Result;
use pioneer_crud_tasks::completion_inbox::{
    TaskCompletionInboxAccess, count_unread_task_completion_inbox,
    find_task_completion_inbox_entry, list_task_completion_inbox, mark_task_completion_inbox_read,
};
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};

const A: &str = "P0000000000000000000A";
const B: &str = "P0000000000000000000B";

async fn prepare(db: &DatabaseConnection) {
    db.execute_unprepared("UPDATE task_inbox_activation SET started_at = 1792108800 WHERE id = 1")
        .await
        .unwrap();
    // ACL membership tables enforce 21-character alphanumeric IDs. Keep the
    // fixture relationships valid without relaxing the migrated schema.
    for sql in [
        "INSERT INTO gateway_identity(id,singleton_key,identity_bootstrap_version,created_at,updated_at) VALUES('G00000000000000000001',1,1,'2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO gateway_principal(id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,created_at,updated_at) VALUES('P0000000000000000000A','G00000000000000000001','user','member','active','A','alice','alice','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('P0000000000000000000B','G00000000000000000001','user','member','active','B','bob','bob','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES('W0000000000000000000I','Inbox',true,false),('W0000000000000000000O','Other',true,false)",
        "INSERT INTO thread(id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('T0000000000000000000P','W0000000000000000000I','Public thread','','agent','m','p','active','user','workspace','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('T0000000000000000000V','W0000000000000000000I','Private thread','','agent','m','p','active','user','private','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('T0000000000000000000I','W0000000000000000000I',NULL,'','agent','m','p','active','task_run','internal','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO workspace_membership(principal_id,workspace_id,granted_by_actor_kind,created_at,updated_at) VALUES('P0000000000000000000A','W0000000000000000000I','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('P0000000000000000000B','W0000000000000000000I','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
        "INSERT INTO thread_membership(principal_id,thread_id,added_by_actor_kind,created_at,updated_at) VALUES('P0000000000000000000A','T0000000000000000000V','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00'),('P0000000000000000000B','T0000000000000000000V','system','2026-10-01 00:00:00 +00:00','2026-10-01 00:00:00 +00:00')",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
}

fn access(principal: &str) -> TaskCompletionInboxAccess<'_> {
    TaskCompletionInboxAccess {
        workspace_id: "W0000000000000000000I",
        principal_id: principal,
        absolute: false,
    }
}

async fn task(
    db: &DatabaseConnection,
    id: &str,
    thread: Option<&str>,
    root: Option<&str>,
    executor: &str,
    mode: &str,
    attachment: &str,
) {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,root_task_id,executor_kind,status,title,goal,priority,delivery_policy_json,lifecycle_policy_json,revision)
         VALUES($1,'W0000000000000000000I','workspace','W0000000000000000000I',$2,$3,$4,'completed','Task title','Goal',0,$5::text::json,$6::text::jsonb,1)",
        [
            id.into(),
            thread.into(),
            root.into(),
            executor.into(),
            serde_json::json!({"mode":mode,"includeResult":true,"format":"summary"}).to_string().into(),
            serde_json::json!({"attachment":attachment,"onParentCancel":"cancel","onParentFailure":"cancel","completion":"complete_on_terminal_run"}).to_string().into(),
        ],
    ))
    .await
    .unwrap();
}

async fn run(db: &DatabaseConnection, id: &str, task: &str, status: &str, number: i64) {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,result_json,updated_at)
         VALUES($1,$2,$1,1,$3,$4,(SELECT executor_kind FROM task WHERE id=$2),'2026-10-16 10:00:00 +00:00',$5,'2099-01-01 00:00:00 +00:00')",
        [
            id.into(),
            task.into(),
            number.into(),
            status.into(),
            serde_json::json!({"summary":"summary","data":{"html":"not for the list"},"artifacts":[]}).to_string().into(),
        ],
    ))
    .await
    .unwrap();
}

async fn unread(db: &DatabaseConnection, principal: &str) -> u64 {
    count_unread_task_completion_inbox(db, &access(principal))
        .await
        .unwrap()
}

async fn read(db: &DatabaseConnection, principal: &str, run: &str) {
    mark_task_completion_inbox_read(db, &access(principal), run, "2026-10-16 11:00:00 +00:00")
        .await
        .unwrap();
}

#[tokio::test]
async fn terminal_statuses_policies_executors_attachment_and_recurring_runs_share_one_source()
-> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        prepare(db).await;
        let mut expected = 0;
        for (ix, status) in [
            "succeeded",
            "failed",
            "blocked",
            "cancelled",
            "timed_out",
            "queued",
            "starting",
            "running",
            "waiting",
            "waiting_review",
        ]
        .iter()
        .enumerate()
        {
            for (jx, mode) in ["none", "thread", "user_notification", "webhook"]
                .iter()
                .enumerate()
            {
                let id = format!("task-{ix}-{jx}");
                task(
                    db,
                    &id,
                    Some("T0000000000000000000P"),
                    None,
                    ["agent", "tool", "workflow", "webhook", "system"][ix % 5],
                    mode,
                    if jx % 2 == 0 { "attached" } else { "detached" },
                )
                .await;
                run(db, &format!("run-{ix}-{jx}"), &id, status, 1).await;
                if ix < 5 {
                    expected += 1;
                }
            }
        }
        // A service System task has no accessible collaboration root. A System
        // task attached to a public root was included by the matrix above.
        task(db, "service", None, None, "system", "none", "detached").await;
        run(db, "service-run", "service", "succeeded", 1).await;
        run(db, "second-occurrence", "task-0-0", "succeeded", 2).await;
        expected += 1;
        assert_eq!(unread(db, A).await, expected);
        let page = list_task_completion_inbox(db, &access(A), None, 100)
            .await
            .unwrap();
        assert_eq!(page.len() as u64, expected);
        assert!(page.iter().all(|e| e.thread_id == "T0000000000000000000P"
            && e.summary.as_deref() == Some("summary")
            && e.completed_at == 1792144800));
        assert_eq!(page.iter().filter(|e| e.task_id == "task-0-0").count(), 2);
        let absolute = TaskCompletionInboxAccess {
            absolute: true,
            ..access(A)
        };
        assert_eq!(
            count_unread_task_completion_inbox(db, &absolute)
                .await
                .unwrap(),
            expected,
            "absolute access still excludes service tasks without a user thread"
        );
        // Replaying deliveries and re-reading events cannot insert inbox rows.
        let again = list_task_completion_inbox(db, &access(A), None, 100)
            .await
            .unwrap();
        assert_eq!(
            page.iter().map(|e| &e.run_id).collect::<Vec<_>>(),
            again.iter().map(|e| &e.run_id).collect::<Vec<_>>()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn time_and_id_seek_pages_are_stable_and_total_unread_is_not_page_size() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        prepare(db).await;
        task(
            db,
            "recurring",
            Some("T0000000000000000000P"),
            None,
            "tool",
            "none",
            "detached",
        )
        .await;
        for ix in 0..65 {
            run(
                db,
                &format!("run-{ix:03}"),
                "recurring",
                "succeeded",
                ix + 1,
            )
            .await;
        }
        assert_eq!(unread(db, A).await, 65);
        let first = list_task_completion_inbox(db, &access(A), None, 30)
            .await
            .unwrap();
        assert_eq!(first.len(), 30);
        assert_eq!(first[0].run_id, "run-064");
        let anchor = first.last().unwrap();
        read(db, A, &anchor.run_id).await;
        // A new occurrence with the same completion time cannot shift the seek.
        run(db, "run-999", "recurring", "succeeded", 66).await;
        let second = list_task_completion_inbox(
            db,
            &access(A),
            Some((&anchor.completed_key, &anchor.run_id)),
            30,
        )
        .await
        .unwrap();
        assert_eq!(second[0].run_id, "run-034");
        assert!(
            second
                .iter()
                .all(|e| !first.iter().any(|f| f.run_id == e.run_id))
        );
        let anchor = second.last().unwrap();
        let third = list_task_completion_inbox(
            db,
            &access(A),
            Some((&anchor.completed_key, &anchor.run_id)),
            30,
        )
        .await
        .unwrap();
        assert_eq!(third.len(), 5);
        assert_eq!(unread(db, A).await, 65);
        let foreign = TaskCompletionInboxAccess {
            workspace_id: "W0000000000000000000O",
            principal_id: A,
            absolute: true,
        };
        assert_eq!(
            count_unread_task_completion_inbox(db, &foreign)
                .await
                .unwrap(),
            0
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn an_unloaded_old_payload_is_not_parsed_to_return_a_metadata_page_or_total() -> Result<()> {
    with_database(async |fixture| {
        let writer = fixture.connection();
        prepare(writer).await;
        task(writer, "task", Some("T0000000000000000000P"), None, "agent", "none", "detached")
            .await;
        for ix in 0..31 {
            run(writer, &format!("run-{ix:03}"), "task", "succeeded", ix + 1).await;
        }
        // PostgreSQL accepts this lexical JSON in the text-backed result, but
        // fails to decode the isolated surrogate if the row enters projection.
        writer.execute_unprepared(
            r#"UPDATE task_run SET result_json='{"summary":"old","data":"\uD800"}' WHERE id='run-000'"#,
        ).await?;
        let reader = fixture.connect_read_only().await?;
        let first = list_task_completion_inbox(&reader, &access(A), None, 30).await?;
        assert_eq!(first.len(), 30);
        assert_eq!(first[29].run_id, "run-001");
        assert_eq!(unread(&reader, A).await, 31);
        Ok(())
    }).await
}

#[tokio::test]
async fn unread_filter_precedes_limit_and_keyset_cursor() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        prepare(db).await;
        task(
            db,
            "pages",
            Some("T0000000000000000000P"),
            None,
            "tool",
            "none",
            "detached",
        )
        .await;
        for ix in 0..80 {
            let id = format!("page-{ix:03}");
            run(db, &id, "pages", "succeeded", ix + 1).await;
            if ix >= 35 {
                read(db, A, &id).await;
            }
        }
        let first = list_task_completion_inbox(db, &access(A), None, 30)
            .await
            .unwrap();
        assert_eq!(first.len(), 30);
        assert_eq!(first[0].run_id, "page-034");
        let anchor = first.last().unwrap();
        let next = list_task_completion_inbox(
            db,
            &access(A),
            Some((&anchor.completed_key, &anchor.run_id)),
            30,
        )
        .await
        .unwrap();
        assert_eq!(next.len(), 5);
        assert_eq!(unread(db, A).await, 35);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn personal_reads_survive_reopen_and_acl_revocation_removes_list_count_get_and_write()
-> Result<()> {
    with_database(async |fixture| {
        let mut db = fixture.connection();
        prepare(db).await;
        task(
            db,
            "root",
            Some("T0000000000000000000V"),
            None,
            "agent",
            "none",
            "attached",
        )
        .await;
        task(
            db,
            "child",
            Some("T0000000000000000000P"),
            Some("root"),
            "system",
            "webhook",
            "detached",
        )
        .await;
        db
            .execute_unprepared(
                "UPDATE task SET owner_kind='user',owner_id='P0000000000000000000A' WHERE id='root'",
            )
            .await
            .unwrap();
        run(db, "run", "child", "blocked", 1).await;
        assert_eq!(unread(db, A).await, 1);
        assert_eq!(unread(db, B).await, 1);
        let scope = pioneer_crud_access::authorization_scope::resolve_task_authorization_scope(
            db,
            "child",
            Some("W0000000000000000000I"),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            scope.root_thread_id.as_deref(),
            Some("T0000000000000000000V")
        );
        read(db, A, "run").await;
        read(db, A, "run").await;
        assert_eq!(unread(db, A).await, 0);
        assert_eq!(unread(db, B).await, 1);
        assert!(
            find_task_completion_inbox_entry(db, &access(A), "run")
                .await
                .unwrap()
                .unwrap()
                .read_at
                .is_some()
        );
        let reopened = fixture.connect().await?;
        db = &reopened;
        assert_eq!(unread(db, A).await, 0);
        // Another device reloads the same durable receipt, not a local copy.
        assert!(
            find_task_completion_inbox_entry(db, &access(A), "run")
                .await
                .unwrap()
                .unwrap()
                .read_at
                .is_some()
        );
        db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000B' AND thread_id='T0000000000000000000V'").await.unwrap();
        assert_eq!(unread(db, B).await, 0);
        assert!(
            list_task_completion_inbox(db, &access(B), None, 30)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            find_task_completion_inbox_entry(db, &access(B), "run")
                .await
                .unwrap()
                .is_none()
        );
        read(db, B, "run").await;
        let receipts = db
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT COUNT(*) n FROM task_run_read WHERE principal_id='P0000000000000000000B'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipts.try_get::<i64>("", "n").unwrap(), 0);
        db
            .execute_unprepared(
                "DELETE FROM workspace_membership WHERE principal_id='P0000000000000000000A'",
            )
            .await
            .unwrap();
        assert!(
            find_task_completion_inbox_entry(db, &access(A), "run")
                .await
                .unwrap()
                .is_none()
        );
        Ok(())
    }).await
}

#[tokio::test]
async fn scheduled_and_recurring_trigger_history_does_not_depend_on_current_task_state()
-> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        prepare(db).await;
        task(
            db,
            "recurring",
            Some("T0000000000000000000P"),
            None,
            "agent",
            "none",
            "detached",
        )
        .await;
        for (ix, kind) in [
            "immediate",
            "scheduled_at",
            "interval",
            "cron",
            "manual",
            "external",
            "dependency",
        ]
        .iter()
        .enumerate()
        {
            let trigger = format!("trigger-{ix}");
            let run_id = format!("run-{ix}");
            // The list deliberately does not parse trigger configuration or join
            // an execution/timeline subscription to discover saved completions.
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,"INSERT INTO task_trigger(id,task_id,kind,status,spec_json,created_at,updated_at) VALUES($1,'recurring',$2,'paused','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",[trigger.clone().into(),(*kind).into()])).await.unwrap();
            run(db, &run_id, "recurring", "succeeded", ix as i64 + 1).await;
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE task_run SET trigger_id=$1 WHERE id=$2",
                [trigger.into(), run_id.into()],
            ))
            .await
            .unwrap();
        }
        db.execute_unprepared(
            "UPDATE task SET status='scheduled',completed_at=NULL WHERE id='recurring'",
        )
        .await
        .unwrap();
        assert_eq!(unread(db, A).await, 7);
        assert_eq!(
            list_task_completion_inbox(db, &access(A), None, 30)
                .await
                .unwrap()
                .len(),
            7
        );
        Ok(())
    }).await
}

#[tokio::test]
async fn same_run_updates_preserve_personal_read_and_list_projection_is_bounded() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        prepare(db).await;
        task(
            db,
            "task",
            Some("T0000000000000000000P"),
            None,
            "agent",
            "none",
            "attached",
        )
        .await;
        run(db, "run", "task", "blocked", 1).await;
        read(db, A, "run").await;
        let result=serde_json::json!({"summary":"界".repeat(2048),"data":{"html":"x".repeat(65536)},"artifacts":[]}).to_string();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE task_run SET status='succeeded',result_json=$1,error_json=NULL WHERE id='run'",
            [result.into()],
        ))
        .await
        .unwrap();
        let rows = list_task_completion_inbox(db, &access(A), None, 30)
            .await
            .unwrap();
        assert!(rows.is_empty());
        let row = find_task_completion_inbox_entry(db, &access(A), "run")
            .await
            .unwrap()
            .unwrap();
        assert!(row.read_at.is_some());
        assert_eq!(row.summary.as_ref().unwrap().chars().count(), 1024);
        assert_eq!(unread(db, A).await, 0);
        let failure=serde_json::json!({"code":"provider-error","message":"m".repeat(4096),"class":"provider","details":{"secret":"not for the list"}}).to_string();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE task_run SET status='failed',result_json=NULL,error_json=$1 WHERE id='run'",
            [failure.into()],
        ))
        .await
        .unwrap();
        let row = find_task_completion_inbox_entry(db, &access(A), "run")
            .await
            .unwrap()
            .unwrap();
        let error = row.error_json.unwrap();
        assert!(error.len() < 1536);
        assert!(!error.contains("not for the list"));
        assert!(row.read_at.is_some());
        // A reopened blocked run is no longer a completed entry despite its stale
        // saved completion timestamp. The later terminal state reuses its run ID.
        db.execute_unprepared("UPDATE task_run SET status='running' WHERE id='run'")
            .await
            .unwrap();
        assert!(
            find_task_completion_inbox_entry(db, &access(A), "run")
                .await
                .unwrap()
                .is_none()
        );
        db.execute_unprepared("UPDATE task_run SET status='succeeded',error_json=NULL WHERE id='run'")
            .await
            .unwrap();
        // Reopening the same run does not revoke its personal read receipt.
        assert!(
            list_task_completion_inbox(db, &access(A), None, 30)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(unread(db, A).await, 0);
        assert!(
            find_task_completion_inbox_entry(db, &access(A), "run")
                .await
                .unwrap()
                .unwrap()
                .read_at
                .is_some()
        );
        Ok(())
    }).await
}

#[tokio::test]
async fn empty_and_small_workspaces_page_independently_of_large_foreign_history() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        prepare(db).await;
        db.execute_unprepared("INSERT INTO task(id,workspace_id,owner_kind,owner_id,executor_kind,status,title,goal,priority,revision,created_at,updated_at) VALUES('foreign-task','W0000000000000000000O','workspace','W0000000000000000000O','system','completed','Foreign','Goal',0,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
        db.execute_unprepared(r#"INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind,completed_at,result_json,created_at,updated_at) SELECT 'foreign-' || lpad(n::text, 5, '0'),'foreign-task','foreign-' || lpad(n::text, 5, '0'),1,n,'succeeded','system','2099-01-01 00:00:00 +00:00','{"summary":"foreign payload must not be decoded","data":"\uD800"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM generate_series(1, 4096) AS numbers(n)"#).await.unwrap();
        // No tasks in the requested workspace: the outer workspace index seek
        // yields no run lookups, regardless of newer completions elsewhere.
        assert!(
            list_task_completion_inbox(db, &access(A), None, 2)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            count_unread_task_completion_inbox(db, &access(A))
                .await
                .unwrap(),
            0
        );
        task(
            db,
            "local",
            Some("T0000000000000000000P"),
            None,
            "tool",
            "none",
            "attached",
        )
        .await;
        run(db, "local-pending", "local", "running", 6).await;
        assert!(
            list_task_completion_inbox(db, &access(A), None, 2)
                .await
                .unwrap()
                .is_empty()
        );
        for ix in 0..5 {
            run(db, &format!("local-{ix}"), "local", "succeeded", ix + 1).await;
        }
        let mut cursor: Option<(String, String)> = None;
        let mut ids = Vec::new();
        loop {
            let page = list_task_completion_inbox(
                db,
                &access(A),
                cursor.as_ref().map(|(at, id)| (at.as_str(), id.as_str())),
                2,
            )
            .await
            .unwrap();
            assert_eq!(
                count_unread_task_completion_inbox(db, &access(A))
                    .await
                    .unwrap(),
                5
            );
            assert!(
                page.iter()
                    .all(|row| row.task_id == "local" && row.summary.as_deref() == Some("summary"))
            );
            if page.is_empty() {
                break;
            }
            let last = page.last().unwrap();
            cursor = Some((last.completed_key.clone(), last.run_id.clone()));
            ids.extend(page.into_iter().map(|row| row.run_id));
        }
        assert_eq!(ids, ["local-4", "local-3", "local-2", "local-1", "local-0"]);
        // Replaying the loaded prefix after an ACL loss and a resumed run must
        // remove both rows. Continuation starts after the new, authorized page.
        task(
            db,
            "hidden",
            Some("T0000000000000000000V"),
            None,
            "agent",
            "thread",
            "detached",
        )
        .await;
        run(db, "private-run", "hidden", "succeeded", 1).await;
        read(db, A, "local-2").await;
        db.execute_unprepared("DELETE FROM thread_membership WHERE principal_id='P0000000000000000000A' AND thread_id='T0000000000000000000V'").await.unwrap();
        db.execute_unprepared("UPDATE task_run SET status='running' WHERE id='local-3'")
            .await
            .unwrap();
        let page = list_task_completion_inbox(db, &access(A), None, 3)
            .await
            .unwrap();
        assert_eq!(
            page.iter()
                .map(|row| row.run_id.as_str())
                .collect::<Vec<_>>(),
            ["local-4", "local-1", "local-0"]
        );
        assert!(page.iter().all(|row| row.read_at.is_none()));
        assert_eq!(
            count_unread_task_completion_inbox(db, &access(A))
                .await
                .unwrap(),
            3
        );
        let last = page.last().unwrap();
        let next = list_task_completion_inbox(
            db,
            &access(A),
            Some((&last.completed_key, &last.run_id)),
            3,
        )
        .await
        .unwrap();
        assert_eq!(
            next.iter()
                .map(|row| row.run_id.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new()
        );
        Ok(())
    }).await
}
