use anyhow::{Context, Result};
use pioneer_crud_execution::recovery_diagnostic;
use pioneer_postgres_test::with_database;
use sea_orm::{ConnectionTrait, DbBackend, Statement};

#[tokio::test]
async fn terminal_diagnostics_follow_the_current_turn_and_delivered_recovery_receipt() -> Result<()>
{
    with_database(async |fixture| {
        let writer = fixture.connection();
        writer.execute_unprepared(
            r#"INSERT INTO workspace(id,name) VALUES ('workspace','Workspace');
               INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
               VALUES ('thread','workspace','','agent','model','provider','active');
               INSERT INTO turn(id,thread_id,status,error)
               VALUES ('turn','thread','failed','public failure');
               INSERT INTO recovery_job(id,turn_id,item_id,item_type,status,trigger,action,diagnostic)
               VALUES ('a','turn','item','command_execution','failed','timeout','retry','{"ordinal":1}'),
                      ('b','turn','item','command_execution','exhausted','timeout','retry','{ "ordinal": 2 }'),
                      ('z','turn','item','command_execution','running','timeout','retry','{"ordinal":3}');
               INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
               VALUES ('event','thread','turn',1,'turn/blocked','{}');"#,
        ).await?;
        let reader = fixture.connect_read_only().await?;
        assert_eq!(
            recovery_diagnostic::failed(&reader, "turn").await?,
            Some((Some(r#"{ "ordinal": 2 }"#.to_owned()), Some("public failure".to_owned()))),
        );
        let event = writer.query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT portable_identity, portable_revision FROM turn_event WHERE id='event'",
        )).await?.context("the blocked event was inserted")?;
        let identity: String = event.try_get("", "portable_identity")?;
        let revision: i64 = event.try_get("", "portable_revision")?;
        let source = Some(("event", identity.as_str(), revision));

        writer.execute_unprepared(
            "UPDATE turn SET status='blocked', error='public blocked' WHERE id='turn';
             UPDATE recovery_job SET status='blocked', last_error='worker blocked' WHERE id='b';
             INSERT INTO recovery_terminalization_outbox(
                 recovery_job_id,turn_id,item_id,item_type,recovery_status,attempt_number,
                 error_message,status,next_run_at
             ) VALUES ('b','turn','item','command_execution','blocked',1,'worker blocked','pending',CURRENT_TIMESTAMP);",
        ).await?;
        assert!(recovery_diagnostic::failed(&reader, "turn").await?.is_none());
        assert!(recovery_diagnostic::blocked(&reader, "turn", Some("b"), source).await?.is_none());

        writer.execute_unprepared(
            "UPDATE recovery_terminalization_outbox SET status='delivered' WHERE recovery_job_id='b'",
        ).await?;
        assert_eq!(
            recovery_diagnostic::blocked(&reader, "turn", Some("b"), source).await?,
            Some((Some(r#"{ "ordinal": 2 }"#.to_owned()), Some("public blocked".to_owned()))),
        );
        assert!(recovery_diagnostic::blocked(
            &reader, "turn", Some("b"), Some(("event", &identity, revision + 1)),
        ).await?.is_none());
        writer.execute_unprepared(
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
             VALUES ('newer','thread','turn',2,'turn/started','{}')",
        ).await?;
        assert!(recovery_diagnostic::blocked(&reader, "turn", Some("b"), source).await?.is_none());
        Ok(())
    }).await
}
