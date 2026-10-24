//! Durable user activity and publication-triggered optional root preparation.
use sea_orm::ConnectionTrait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

// SQLite strftime reads the durable original timestamp, including its offset.
fn millis(column: &str) -> String {
    format!(
        "(CAST(strftime('%s',{column}) AS INTEGER)*1000 + CAST(substr(strftime('%f',{column}),4,3) AS INTEGER))"
    )
}

fn enqueue(selection: &str) -> String {
    format!(
        r#"INSERT INTO compaction_history_check(turn_id,state,managed,purpose)
        {selection}
        ON CONFLICT(turn_id) DO UPDATE SET state='pending',outcome=NULL,
          revision=compaction_history_check.revision+1,failures=0,next_attempt_ms=0,
          attempt_deadline_ms=NULL,diagnostic=NULL,config_hash=NULL,managed=1,purpose='root_conversation';"#
    )
}

// A delivery locator is not authority: require the exact immutable binding,
// accepted candidate and the result's already materialized acknowledgement.
const DELIVERIES: &str = r#"
 FROM task_delivery d
 JOIN thread h ON h.id=d.target_thread_id AND h.workspace_id=d.workspace_id
 JOIN compaction_delivery_output b ON b.delivery_id=d.id
 JOIN compaction_task_output s ON s.task_run_turn_id=b.task_run_turn_id
   AND s.task_id=d.task_id AND s.run_id=d.run_id AND s.workspace_id=d.workspace_id
 JOIN task_result_candidate c ON c.id=b.candidate_id AND c.status='accepted'
   AND c.task_run_turn_id=s.task_run_turn_id AND c.task_id=s.task_id AND c.run_id=s.run_id
   AND c.thread_id=s.source_thread AND c.turn_id=s.source_turn
 JOIN task k ON k.id=d.task_id AND k.workspace_id=d.workspace_id
 JOIN turn_event acknowledgement ON acknowledgement.turn_id=d.delivered_turn_id
   AND acknowledgement.thread_id=h.id AND acknowledgement.event_type='item/completed'
 JOIN compaction_event_revision receipt ON receipt.source_id=acknowledgement.id
   AND receipt.turn_id=acknowledgement.turn_id AND receipt.present=1
   AND receipt.projection_revision=receipt.revision
   AND receipt.item_id='task_delivery_result_'||d.id
 JOIN turn published ON published.id=d.delivered_turn_id
   AND published.thread_id=h.id
 WHERE d.mode='thread' AND d.status='delivered' AND d.delivered_at IS NOT NULL
   AND d.error_snapshot_json IS NULL
   AND h.origin_kind IN ('user','collaborative','direct_message')
   AND NOT EXISTS(SELECT 1 FROM thread_lineage l WHERE l.child_thread_id=h.id)
   AND EXISTS(SELECT 1 FROM turn_item i WHERE i.turn_id=published.id
       AND i.item_id='task_delivery_result_'||d.id AND i.status='completed')
"#;

const COMPOSER: &str = r#"
 json_valid(k.metadata_json)
 AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.version')=1
 AND json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.launch.thread_id')=h.id
 AND EXISTS(SELECT 1 FROM turn human
   WHERE human.id=json_extract(CASE WHEN json_valid(k.metadata_json) THEN k.metadata_json ELSE '{}' END,'$.composerWork.launch.turn_id')
     AND human.thread_id=h.id AND human.origin='user'
     AND human.turn_kind='conversation' AND human.initiated_by_actor_kind='principal'
     AND human.work_owner='detached_task'
     AND k.created_by_turn_id=human.id AND k.created_by_thread_id=h.id)
"#;

// Publication performs one ordered index seek, never historical aggregates.
// Older ready responses (if any) are recovered by the existing Maintenance poll
// in bounded pages. Its cursor and the queue debt commit with publication.
fn seed_activity(destination: &str, publication: &str) -> String {
    let input_ms = millis("human.created_at");
    let candidate_ms = millis("candidate.created_at");
    format!(
        r#"INSERT INTO compaction_root_activity(thread_id,user_turn_id,last_user_activity_ms,recovery_pending)
      SELECT root.id,human.id,{input_ms},1
      FROM thread root CROSS JOIN turn human
      WHERE root.id={destination}
        AND root.origin_kind IN ('user','collaborative','direct_message')
        AND NOT EXISTS(SELECT 1 FROM thread_lineage WHERE child_thread_id=root.id)
        AND NOT EXISTS(SELECT 1 FROM compaction_root_activity a WHERE a.thread_id=root.id)
        AND ({publication})
        AND human.id=(SELECT candidate.id FROM turn candidate
          WHERE candidate.thread_id=root.id AND candidate.origin='user'
            AND candidate.turn_kind='conversation' AND candidate.initiated_by_actor_kind='principal'
          ORDER BY {candidate_ms} DESC,candidate.id DESC LIMIT 1)
      ON CONFLICT(thread_id) DO NOTHING;"#
    )
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        // Schema and completion marker share one transaction. There is no
        // history inventory/backfill under the bootstrap writer reservation.
        Some(true)
    }

    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        if !manager
            .has_column("compaction_history_check", "purpose")
            .await?
        {
            db.execute_unprepared("ALTER TABLE compaction_history_check ADD COLUMN purpose TEXT NOT NULL DEFAULT 'execution'").await?;
        }
        db.execute_unprepared("CREATE INDEX IF NOT EXISTS compaction_root_history_due ON compaction_history_check(next_attempt_ms,turn_id) WHERE purpose='root_conversation' AND state='pending' AND managed=1").await?;
        let task_storage = if manager.has_table("_task_zstd").await? {
            "_task_zstd"
        } else {
            "task"
        };
        // The metadata guard is a cheap superset of Composer work. The worker
        // verifies composerWork after reading only live rows in this root.
        db.execute_unprepared(&format!("CREATE INDEX IF NOT EXISTS compaction_root_live_composer ON {task_storage}(workspace_id,created_by_thread_id,status) WHERE status NOT IN ('completed','failed','blocked','cancelled') AND metadata_json IS NOT NULL")).await?;
        for name in [
            "compaction_root_human_input",
            "compaction_root_answer_completed",
            "compaction_root_task_delivered",
        ] {
            db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS {name}"))
                .await?;
        }
        db.execute_unprepared(
            r#"CREATE TABLE IF NOT EXISTS compaction_root_activity(
          thread_id TEXT PRIMARY KEY NOT NULL REFERENCES thread(id) ON DELETE CASCADE,
          user_turn_id TEXT NOT NULL REFERENCES turn(id) ON DELETE CASCADE,
          last_user_activity_ms INTEGER NOT NULL CHECK(last_user_activity_ms>=0),
          recovery_pending INTEGER NOT NULL DEFAULT 0 CHECK(recovery_pending IN (0,1)),
          recovery_before_ms INTEGER NOT NULL DEFAULT 9223372036854775807,
          recovery_before_id TEXT NOT NULL DEFAULT '~~~~~~~~~~~~~~~~~~~~~')"#,
        )
        .await?;
        db.execute_unprepared("CREATE UNIQUE INDEX IF NOT EXISTS compaction_root_activity_turn ON compaction_root_activity(user_turn_id)").await?;
        db.execute_unprepared("DROP TRIGGER IF EXISTS compaction_history_cli_completed")
            .await?;
        let turn_storage = if manager.has_table("_turn_zstd").await? {
            "_turn_zstd"
        } else {
            "turn"
        };
        let delivery_storage = if manager.has_table("_task_delivery_zstd").await? {
            "_task_delivery_zstd"
        } else {
            "task_delivery"
        };
        // Each range/order below uses the identical deterministic expression in
        // its index. No computed ORDER BY over an unindexed historical set.
        let event_storage = if manager.has_table("_turn_event_zstd").await? {
            "_turn_event_zstd"
        } else {
            "turn_event"
        };
        let item_storage = if manager.has_table("_turn_item_zstd").await? {
            "_turn_item_zstd"
        } else {
            "turn_item"
        };
        let human_ms = millis("created_at");
        db.execute_unprepared(&format!("CREATE INDEX IF NOT EXISTS compaction_root_human_turn ON {turn_storage}(thread_id,{human_ms} DESC,id DESC) WHERE origin='user' AND turn_kind='conversation' AND initiated_by_actor_kind='principal'")).await?;
        db.execute_unprepared(&format!("CREATE INDEX IF NOT EXISTS compaction_root_ready_event ON {event_storage}(thread_id,{human_ms} DESC,id DESC) WHERE event_type IN ('turn/completed','item/completed')")).await?;
        db.execute_unprepared(&format!("CREATE INDEX IF NOT EXISTS compaction_root_completion_event ON {event_storage}(turn_id,event_type,{human_ms} DESC,id DESC)")).await?;
        db.execute_unprepared(&format!("CREATE INDEX IF NOT EXISTS compaction_root_ready_item ON {item_storage}(turn_id,item_type,status)")).await?;
        db.execute_unprepared("CREATE INDEX IF NOT EXISTS compaction_root_activity_recovery ON compaction_root_activity(thread_id) WHERE recovery_pending=1").await?;
        // The current delivery joins its exact materialized receipt, rather
        // than searching completed items across the destination's history.
        db.execute_unprepared("CREATE INDEX IF NOT EXISTS compaction_root_delivery_receipt ON compaction_event_revision(turn_id,item_id,source_id) WHERE present=1 AND projection_revision=revision").await?;
        let input_ms = millis("NEW.created_at");
        db.execute_unprepared(&format!(r#"CREATE TRIGGER IF NOT EXISTS compaction_root_human_input
          AFTER INSERT ON {turn_storage}
          WHEN NEW.origin='user' AND NEW.turn_kind='conversation'
            AND NEW.initiated_by_actor_kind='principal'
            AND EXISTS(SELECT 1 FROM thread h WHERE h.id=NEW.thread_id AND h.origin_kind IN ('user','collaborative','direct_message'))
            AND NOT EXISTS(SELECT 1 FROM thread_lineage WHERE child_thread_id=NEW.thread_id)
          BEGIN
            -- Establish the owner even if the older worker has not admitted
            -- its first operation yet. The following fence must beat admission.
            INSERT INTO compaction_context(owner,workspace_id,thread_id)
              SELECT 'native:'||length(CAST(h.workspace_id AS BLOB))||':'||h.workspace_id||':'||length(CAST(h.id AS BLOB))||':'||h.id,
                     h.workspace_id,h.id
              FROM thread h JOIN compaction_root_activity a ON a.thread_id=h.id
              WHERE h.id=NEW.thread_id
              ON CONFLICT(owner) DO NOTHING;
            INSERT INTO compaction_execution_stop(owner,turn_id)
              SELECT c.owner,a.user_turn_id FROM compaction_root_activity a
              JOIN compaction_context c ON c.thread_id=a.thread_id
              JOIN thread h ON h.id=a.thread_id AND c.workspace_id=h.workspace_id
              WHERE a.thread_id=NEW.thread_id AND a.user_turn_id<>NEW.id
                AND c.owner='native:'||length(CAST(h.workspace_id AS BLOB))||':'||h.workspace_id||':'||length(CAST(h.id AS BLOB))||':'||h.id
              ON CONFLICT(owner,turn_id) DO NOTHING;
            UPDATE compaction_history_check SET state='finished',outcome='cancelled',
                revision=revision+1,attempt_deadline_ms=NULL
              WHERE turn_id=(SELECT user_turn_id FROM compaction_root_activity WHERE thread_id=NEW.thread_id)
                AND turn_id<>NEW.id AND purpose='root_conversation' AND state='pending';
            INSERT INTO compaction_root_activity(thread_id,user_turn_id,last_user_activity_ms)
              VALUES(NEW.thread_id,NEW.id,{input_ms})
              ON CONFLICT(thread_id) DO UPDATE SET
                user_turn_id=NEW.id,
                last_user_activity_ms=MAX(compaction_root_activity.last_user_activity_ms,{input_ms});
          END"#)).await?;
        let completion_ms = millis("e.created_at");
        let direct_seed = seed_activity("NEW.thread_id", "1");
        let direct_enqueue = enqueue(
            "SELECT a.user_turn_id,'pending',1,'root_conversation' FROM compaction_root_activity a WHERE a.thread_id=NEW.thread_id",
        );
        db.execute_unprepared(&format!(r#"CREATE TRIGGER IF NOT EXISTS compaction_root_answer_completed
          AFTER UPDATE OF status ON {turn_storage}
          WHEN NEW.status='completed' AND OLD.status<>'completed'
            AND NEW.origin='user' AND NEW.turn_kind='conversation'
            AND NEW.initiated_by_actor_kind='principal' AND NEW.work_owner='turn'
            AND EXISTS(SELECT 1 FROM turn_item i WHERE i.turn_id=NEW.id
                AND i.item_type='agent_message' AND i.status='completed')
            AND EXISTS(SELECT 1 FROM turn_event e WHERE e.turn_id=NEW.id AND e.event_type='turn/completed')
          BEGIN
            {direct_seed}
            UPDATE compaction_root_activity SET last_user_activity_ms=MAX(last_user_activity_ms,
              (SELECT {completion_ms} FROM turn_event e WHERE e.turn_id=NEW.id AND e.event_type='turn/completed' ORDER BY {completion_ms} DESC,e.id DESC LIMIT 1))
              WHERE thread_id=NEW.thread_id;
            {direct_enqueue}
          END"#)).await?;
        let delivery_ms = millis("acknowledgement.created_at");
        let delivery_seed = seed_activity(
            "NEW.target_thread_id",
            &format!("EXISTS(SELECT 1 {DELIVERIES} AND d.id=NEW.id)"),
        );
        let delivery_enqueue = enqueue(&format!(
            "SELECT a.user_turn_id,'pending',1,'root_conversation' FROM compaction_root_activity a WHERE a.thread_id=NEW.target_thread_id AND EXISTS(SELECT 1 {DELIVERIES} AND d.id=NEW.id)"
        ));
        db.execute_unprepared(&format!(
            r#"CREATE TRIGGER IF NOT EXISTS compaction_root_task_delivered
          AFTER UPDATE OF status ON {delivery_storage}
          WHEN NEW.status='delivered' AND OLD.status<>'delivered'
          BEGIN
            {delivery_seed}
            UPDATE compaction_root_activity SET last_user_activity_ms=MAX(last_user_activity_ms,
              (SELECT MAX({delivery_ms}) {DELIVERIES} AND d.id=NEW.id AND {COMPOSER}))
              WHERE thread_id=NEW.target_thread_id
                AND EXISTS(SELECT 1 {DELIVERIES} AND d.id=NEW.id AND {COMPOSER});
            {delivery_enqueue}
          END"#
        ))
        .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        for name in [
            "compaction_root_human_input",
            "compaction_root_answer_completed",
            "compaction_root_task_delivered",
        ] {
            db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS {name}"))
                .await?;
        }
        db.execute_unprepared("DROP TABLE IF EXISTS compaction_root_activity")
            .await?;
        for name in [
            "compaction_root_ready_event",
            "compaction_root_completion_event",
            "compaction_root_ready_item",
            "compaction_root_delivery_receipt",
        ] {
            db.execute_unprepared(&format!("DROP INDEX IF EXISTS {name}"))
                .await?;
        }
        db.execute_unprepared("DROP INDEX IF EXISTS compaction_root_human_turn")
            .await?;
        db.execute_unprepared("DROP INDEX IF EXISTS compaction_root_history_due")
            .await?;
        db.execute_unprepared("DROP INDEX IF EXISTS compaction_root_live_composer")
            .await?;
        if manager
            .has_column("compaction_history_check", "purpose")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(Alias::new("compaction_history_check"))
                        .drop_column(Alias::new("purpose"))
                        .to_owned(),
                )
                .await?;
        }
        // Restore the previous policy only on explicit schema rollback.
        db.execute_unprepared("CREATE TRIGGER IF NOT EXISTS compaction_history_cli_completed AFTER UPDATE OF status ON turn WHEN NEW.status='completed' AND OLD.status!='completed' AND EXISTS (SELECT 1 FROM turn_cli_runtime_binding b WHERE b.turn_id=NEW.id) BEGIN INSERT INTO compaction_history_check(turn_id) VALUES(NEW.id) ON CONFLICT(turn_id) DO NOTHING; END").await?;
        Ok(())
    }
}
