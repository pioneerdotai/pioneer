//! Transient write conflicts recognized by Task repositories.
pub fn is_retryable(error: &anyhow::Error) -> bool {
    pioneer_sqlite::is_retryable_database_conflict(error)
        || crate::task_event::is_idempotency_preflight_race(error)
        || error
            .downcast_ref::<crate::task_occurrence_producers::OccurrencePublicationRace>()
            .is_some()
        || error
            .downcast_ref::<pioneer_crud_conversations::turn_payload::PreflightRace>()
            .is_some()
}
