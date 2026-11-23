use pioneer_protocol::{Thread, TurnKind};
use sea_orm::Statement;

pub fn assert_thread_turn_snapshot_statements(statements: &[Statement], threads: &[Thread]) {
    // The only other statement is the original thread-list selector. Any
    // per-thread selector, history/event load or transaction is a failure.
    let mut statements = statements.iter();
    assert!(statements.next().unwrap().sql.contains("thread"));
    for batch in
        threads.chunks(pioneer_crud_conversations::turn_order::THREAD_TURN_SNAPSHOT_BATCH_SIZE)
    {
        let latest = statements.next().expect("one latest-Turn batch query");
        assert_thread_turn_snapshot_statement(latest, false);
        assert_eq!(
            latest.values.as_ref().unwrap().0,
            batch
                .iter()
                .map(|thread| thread.id.clone().into())
                .collect::<Vec<sea_orm::Value>>()
        );
        let fallback_ids = batch
            .iter()
            .filter(|thread| {
                thread
                    .turns
                    .first()
                    .is_some_and(|turn| turn.turn_kind != TurnKind::Conversation)
            })
            .map(|thread| thread.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if !fallback_ids.is_empty() {
            let fallback = statements
                .next()
                .expect("one Conversation fallback batch query");
            assert_thread_turn_snapshot_statement(fallback, true);
            let values = &fallback.values.as_ref().unwrap().0;
            assert_eq!(values.len(), fallback_ids.len());
            // SQL result order is deliberately irrelevant to list order.
            for id in fallback_ids {
                assert!(values.contains(&id.into()));
            }
        }
    }
    assert!(
        statements.next().is_none(),
        "no individual Turn or extra history queries"
    );
}

pub(crate) fn assert_thread_turn_snapshot_statement(
    statement: &Statement,
    conversation_only: bool,
) {
    let sql = &statement.sql;
    assert!(sql.starts_with("WITH selected(thread_id) AS (VALUES "));
    assert!(sql.contains("SELECT latest.* FROM selected CROSS JOIN LATERAL"));
    assert!(sql.contains("candidate.thread_id = selected.thread_id"));
    assert!(sql.contains("ORDER BY candidate.created_at DESC, candidate.id DESC LIMIT 1"));
    assert_eq!(
        sql.contains("AND candidate.turn_kind = 'conversation'"),
        conversation_only
    );
    assert!(!sql.contains("reasoning_effort IS NOT NULL"));
    let id_count = statement.values.as_ref().unwrap().0.len();
    assert!((1..=128).contains(&id_count));
    assert_eq!(sql.matches("::text)").count(), id_count);
}
