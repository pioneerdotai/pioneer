use pioneer_crud_conversations::{
    events::{CanonicalTurnEventPayload, CanonicalTurnStartedEventPayload},
    thread_metadata::{find_thread_by_id, prepare_preview_author_json},
    thread_mutations::upsert_projected_thread,
    turn_writes,
};
use pioneer_crud_execution::{
    event_append::{PreparedTurnEvent, append_prepared_event},
    time::unix_to_datetime,
};
use pioneer_crud_self_improvement::{
    source_turn::{
        apply_prepared_source_turn, prepare_completed_collaborative_source_exchange,
        prepare_completed_source_turn,
    },
    store::SelfImprovementStore,
};
use pioneer_protocol::{
    ItemCompletedNotification, ItemStartedNotification, PersistedActorRef, SandboxMode, Thread,
    Turn, TurnCompletedNotification, TurnFailedNotification, TurnKind, UserInput,
};
use sea_orm::{DatabaseConnection, TransactionTrait};

// Publish the canonical events consumed by the history reader. The task
// linkage for collaborative exchanges is seeded by each causal fixture.
pub struct HistoryFixture {
    pub writer: DatabaseConnection,
    pub history: SelfImprovementStore,
}

impl HistoryFixture {
    pub async fn seed_turn_start(
        &self,
        thread: &Thread,
        sandbox_mode: SandboxMode,
        turn: &Turn,
        input: &[UserInput],
        actor: PersistedActorRef,
    ) -> anyhow::Result<()> {
        let created_at = unix_to_datetime(thread.created_at);
        let updated_at = unix_to_datetime(thread.updated_at);
        let tx = self.writer.begin().await?;
        let existing = find_thread_by_id(&tx, &thread.id).await?.is_some();
        if !existing || turn.turn_kind != TurnKind::TaskRun {
            upsert_projected_thread(
                &tx,
                thread,
                (!existing).then_some(&actor),
                prepare_preview_author_json(thread.preview_author.as_ref())?,
                created_at,
                updated_at,
            )
            .await?;
        }
        let row = turn_writes::prepare_projected_turn_upsert(
            &tx,
            &turn.id,
            &thread.id,
            turn,
            None,
            None,
            Some(turn.work_owner),
            Some(&actor),
            true,
            updated_at,
            updated_at,
        )
        .await?;
        turn_writes::apply_prepared_turn_upsert(&tx, row).await?;
        let event = CanonicalTurnEventPayload::TurnStarted(CanonicalTurnStartedEventPayload {
            thread: thread.clone(),
            sandbox_mode,
            turn: turn.clone(),
            input: input.to_vec(),
            actor: Some(actor),
            reasoning_effort: None,
            work_owner: turn.work_owner,
        });
        append_prepared_event(
            &tx,
            PreparedTurnEvent::prepare_for_storage(event)?,
            updated_at,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn seed_turn_completed(
        &self,
        notification: TurnCompletedNotification,
        at: i64,
    ) -> anyhow::Result<()> {
        self.seed_terminal(CanonicalTurnEventPayload::TurnCompleted(notification), at)
            .await
    }

    pub async fn seed_turn_failed(
        &self,
        notification: TurnFailedNotification,
        at: i64,
    ) -> anyhow::Result<()> {
        self.seed_terminal(CanonicalTurnEventPayload::TurnFailed(notification), at)
            .await
    }

    async fn seed_terminal(&self, event: CanonicalTurnEventPayload, at: i64) -> anyhow::Result<()> {
        let turn = match &event {
            CanonicalTurnEventPayload::TurnCompleted(notification) => &notification.turn,
            CanonicalTurnEventPayload::TurnFailed(notification) => &notification.turn,
            _ => unreachable!("terminal fixture requires a terminal event"),
        };
        let at = unix_to_datetime(at);
        let tx = self.writer.begin().await?;
        turn_writes::update_turn_status(
            &tx,
            event.thread_id(),
            &turn.id,
            turn.status,
            turn.error.as_deref(),
            at,
        )
        .await?;
        let prepared = PreparedTurnEvent::prepare_for_storage(event.clone())?;
        let appended = append_prepared_event(&tx, prepared, at).await?;
        if let CanonicalTurnEventPayload::TurnCompleted(notification) = &event
            && let Some(source) =
                prepare_completed_source_turn(&tx, &appended.id, appended.created_at, notification)
                    .await?
        {
            apply_prepared_source_turn(&tx, source).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn seed_item_started(
        &self,
        notification: ItemStartedNotification,
        at: i64,
    ) -> anyhow::Result<()> {
        self.seed_item_event(CanonicalTurnEventPayload::ItemStarted(notification), at)
            .await
    }

    pub async fn seed_item_completed(
        &self,
        notification: ItemCompletedNotification,
        at: i64,
    ) -> anyhow::Result<()> {
        self.seed_item_event(CanonicalTurnEventPayload::ItemCompleted(notification), at)
            .await
    }

    async fn seed_item_event(
        &self,
        event: CanonicalTurnEventPayload,
        at: i64,
    ) -> anyhow::Result<()> {
        let at = unix_to_datetime(at);
        let tx = self.writer.begin().await?;
        let appended = append_prepared_event(
            &tx,
            PreparedTurnEvent::prepare_for_storage(event.clone())?,
            at,
        )
        .await?;
        if let CanonicalTurnEventPayload::ItemCompleted(notification) = &event
            && let Some(source) = prepare_completed_collaborative_source_exchange(
                &tx,
                &appended.id,
                appended.created_at,
                notification,
            )
            .await?
        {
            apply_prepared_source_turn(&tx, source).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
