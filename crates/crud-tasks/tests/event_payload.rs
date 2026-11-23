use anyhow::{Context, Result};
use pioneer_crud_tasks::event_log::{self, EventPayload, NewEvent};
use pioneer_entity::{task_event, task_event_fanout_pending};
use pioneer_postgres_test::with_database;
use sea_orm::{EntityTrait, QuerySelect, TransactionTrait};

#[tokio::test]
async fn task_event_reads_original_size_without_fetching_the_payload() -> Result<()> {
    with_database(async |fixture| {
        let db = fixture.connection();
        let task_id = "observed-task";
        let short = "{ \"n\": 1 }".to_owned();
        let large = format!(r#"{{"data":"{}\u0000"}}"#, "Привет".repeat(24_000));
        let now = chrono::Utc::now().fixed_offset();

        for (id, body, codec) in [("raw", short, 0_i16), ("compressed", large, 1_i16)] {
            let payload = EventPayload::prepare(&body)?;
            let tx = db.begin().await?;
            event_log::lock_task(&tx, task_id).await?;
            let sequence = event_log::next_sequence(&tx, task_id).await?;
            event_log::append(
                &tx,
                NewEvent {
                    id,
                    task_id,
                    run_id: None,
                    thread_id: None,
                    turn_id: None,
                    event_type: "task/probe",
                    idempotency_key: Some(id),
                    sequence,
                    created_at: now,
                },
                payload,
            )
            .await?;
            tx.commit().await?;

            let lengths = task_event::Entity::find_by_id(id)
                .select_only()
                .column(task_event::Column::PayloadJsonBytes)
                .column(task_event::Column::PayloadJsonCodec)
                .into_tuple::<(i64, i16)>()
                .one(db)
                .await?
                .context("task event metadata disappeared")?;
            assert_eq!(lengths, (i64::try_from(body.len())?, codec));
            let mut saved = task_event::Entity::find_by_id(id)
                .one(db)
                .await?
                .context("task event disappeared")?;
            assert_eq!(EventPayload::read(&mut saved)?.as_bytes(), body.as_bytes());
            let pending = task_event_fanout_pending::Entity::find_by_id(task_id)
                .one(db)
                .await?
                .context("task event did not create fanout work")?;
            assert_eq!(pending.newest_sequence, sequence);
        }
        Ok(())
    })
    .await
}
