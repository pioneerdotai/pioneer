use super::*;
use crate::message::reconciliation_diagnostics::{Operation, Reporter};
use sentry::SentryFutureExt;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;

fn local_capture() -> (
    Arc<sentry::Hub>,
    Arc<sentry::test::TestTransport>,
    tracing::Dispatch,
) {
    let transport = sentry::test::TestTransport::new();
    let options = sentry::ClientOptions::new()
        .dsn("https://public@sentry.invalid/1")
        .transport(transport.clone());
    let hub = Arc::new(sentry::Hub::new(
        Some(Arc::new(options.into())),
        Arc::new(Default::default()),
    ));
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(pioneer_observability::sentry_tracing_layer()),
    );
    (hub, transport, dispatch)
}

#[tokio::test]
async fn reconciliation_reporting_is_retained_in_both_real_worker_loops() {
    let (processor, _, _, _, _, _) = setup_workspace_message_processor().await;
    let processor = Arc::new(processor);
    // Closed pools supply a real typed storage failure without fixture hooks.
    processor
        .crud_store
        .database_connection()
        .close()
        .await
        .unwrap();
    // SQLite pool setup uses a real worker thread. Pause only after the fixture
    // is ready and closed, so Tokio cannot advance its acquisition timeout while
    // that thread is still opening the connection.
    tokio::time::pause();
    let (hub, transport, dispatch) = local_capture();
    let projection = tokio::spawn(
        MessageProcessor::run_projection_delivery_resilience_worker(Arc::downgrade(&processor))
            .with_subscriber(dispatch.clone())
            .bind_hub(hub.clone()),
    );
    let lifecycle = tokio::spawn(
        MessageProcessor::run_task_lifecycle_resilience_worker(Arc::downgrade(&processor))
            .with_subscriber(dispatch)
            .bind_hub(hub),
    );
    let mut events = Vec::new();
    // End-of-pass neighboring events provide a completion marker for each real
    // worker. Advance Tokio's clock only after both reached their existing sleep.
    for pass in 1..=3 {
        let mut completed = false;
        for _ in 0..1024 {
            tokio::task::yield_now().await;
            events.extend(transport.fetch_and_clear_events());
            let projection_passes = events
                .iter()
                .filter(|event| {
                    event.message.as_deref()
                        == Some("native turn-event optional delivery worker failed")
                })
                .count();
            let lifecycle_passes = events
                .iter()
                .filter(|event| event.message.as_deref() == Some("task delivery worker failed"))
                .count();
            if projection_passes >= pass && lifecycle_passes >= pass {
                completed = true;
                break;
            }
            // Existing retry policy also retries the ConnectionAcquire wrapper;
            // drive its backoff without changing production timing or sleeping.
            tokio::time::advance(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            completed,
            "both workers must finish their real reconciliation pass"
        );
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
    }
    projection.abort();
    lifecycle.abort();
    let _ = projection.await;
    let _ = lifecycle.await;
    events.extend(transport.fetch_and_clear_events());
    let diagnostics: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event.message.as_deref(),
                Some(
                    "native Turn finalization reconciler failed"
                        | "task parent occurrence reconciler failed"
                )
            )
        })
        .collect();
    assert_eq!(
        diagnostics.len(),
        2,
        "each operation emits once across repeated passes"
    );
    for operation in ["native_turn_finalization", "task_run_occurrence"] {
        assert_eq!(
            diagnostics
                .iter()
                .filter(|event| {
                    let sentry::protocol::Context::Other(fields) =
                        &event.contexts["Rust Tracing Fields"]
                    else {
                        panic!()
                    };
                    fields["operation"] == operation && fields["cause"] == "pool_closed"
                })
                .count(),
            1
        );
    }
    assert!(
        events
            .iter()
            .filter(|event| {
                event.message.as_deref() == Some("task child terminal reconciler failed")
            })
            .count()
            >= 2,
        "neighbor ERROR is still emitted each pass"
    );
}

#[tokio::test]
async fn real_complete_reconciliation_results_recover_only_their_reporter() {
    let (processor, _, _, _, _, _) = setup_workspace_message_processor().await;
    let processor = Arc::new(processor);
    let background = processor.for_background_reconciliation();
    let mut finalization = Reporter::new(Operation::NativeFinalization);
    let mut occurrence = Reporter::new(Operation::TaskRunOccurrence);
    let (hub, transport, dispatch) = local_capture();
    async {
        let now = std::time::Instant::now();
        finalization.observe::<usize>(&Err(anyhow::anyhow!("fixture failure")), now);
        occurrence.observe::<usize>(&Err(anyhow::anyhow!("fixture failure")), now);
        let result = background
            .reconcile_prepared_native_turn_finalizations(now_timestamp_secs(), 64)
            .await;
        assert!(result.is_ok());
        finalization.observe(&result, now);
        tracing::error!("after finalization success");
        let result = background
            .reconcile_terminal_task_run_occurrence_turns(64)
            .await;
        assert_eq!(result.as_ref().unwrap(), &0);
        occurrence.observe(&result, now);
        occurrence.observe(&result, now);
        tracing::error!("after occurrence success");
    }
    .with_subscriber(dispatch)
    .bind_hub(hub)
    .await;
    let events = transport.fetch_and_clear_events();
    let recovered_operations = |event: &sentry::protocol::Event<'_>| {
        event
            .breadcrumbs
            .iter()
            .filter(|crumb| {
                crumb.message.as_deref() == Some("reconciliation recovered after observed failures")
            })
            .map(|crumb| crumb.data["operation"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        recovered_operations(&events[2]),
        ["native_turn_finalization"]
    );
    assert_eq!(
        recovered_operations(&events[3]),
        ["native_turn_finalization", "task_run_occurrence"]
    );
}

#[tokio::test]
async fn partial_durable_progress_followed_by_real_error_does_not_recover() {
    let (processor, store, workspace_id) =
        setup_execution_window_terminal_turn("diagnostic_thread", "diagnostic_turn").await;
    let processor = Arc::new(processor);
    store
        .prepare_turn_finalization(
            &ItemCompletedNotification {
                workspace_id,
                thread_id: "diagnostic_thread".to_owned(),
                turn_id: "diagnostic_turn".to_owned(),
                item: TurnItem::AgentMessage {
                    id: "diagnostic_item".to_owned(),
                    text: "prepared".to_owned(),
                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
            None,
            1_700_000_100,
        )
        .await
        .unwrap();
    // Real durable progress followed by a real synchronization error: the
    // loaded Turn disagrees with the prepared durable Completed transition.
    let (_, mut loaded) = processor
        .thread_manager
        .turn_get("diagnostic_thread", "diagnostic_turn")
        .await
        .unwrap();
    loaded.status = TurnStatus::Failed;
    processor
        .thread_manager
        .commit_terminal_turn("diagnostic_thread", &loaded)
        .await
        .unwrap();
    let (hub, transport, dispatch) = local_capture();
    async {
        let mut reporting = Reporter::new(Operation::NativeFinalization);
        let result = processor
            .for_background_reconciliation()
            .reconcile_prepared_native_turn_finalizations(1_700_000_101, 64)
            .await;
        assert!(result.is_err());
        let (_, durable) = store
            .get_turn("diagnostic_thread", "diagnostic_turn")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            durable.status,
            TurnStatus::Completed,
            "partial durable progress preceded Err"
        );
        reporting.observe(&result, std::time::Instant::now());
        tracing::error!("after partial reconciliation failure");
    }
    .with_subscriber(dispatch)
    .bind_hub(hub)
    .await;
    let events = transport.fetch_and_clear_events();
    assert_eq!(events.len(), 2);
    assert!(events[1].breadcrumbs.iter().all(|crumb| {
        crumb.message.as_deref() != Some("reconciliation recovered after observed failures")
    }));
}
