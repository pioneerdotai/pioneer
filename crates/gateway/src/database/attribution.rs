use pioneer_observability::{DatabaseOperationSite, DatabaseWorkload, DatabaseWorkloadTrace};
use std::future::Future;
use std::pin::Pin;

pub(crate) fn current_database_workload() -> Option<DatabaseWorkload> {
    pioneer_observability::work::current_database().map(|context| context.workload())
}

pub(crate) fn record_database_query(
    fingerprint: u64,
    kind: pioneer_observability::DatabaseQueryKind,
    elapsed: std::time::Duration,
) {
    if let Some(context) = pioneer_observability::work::current_database() {
        context.record_query(fingerprint, kind, elapsed);
        pioneer_observability::live::progress(pioneer_observability::live::Stage::Database);
    }
}

pub(crate) fn scope_database_workload<'a, F>(
    workload: DatabaseWorkload,
    site: DatabaseOperationSite,
    future: F,
) -> Pin<Box<dyn Future<Output = F::Output> + Send + 'a>>
where
    F: Future + Send + 'a,
{
    Box::pin(async move {
        if !pioneer_observability::telemetry_enabled() {
            return future.await;
        }
        let trace = DatabaseWorkloadTrace::start_at(workload, site);
        let carrier = pioneer_observability::work::capture().with_database(trace.context());
        let output = carrier.scope(future).await;
        trace.finish_success();
        output
    })
}

pub(crate) fn scope_database_workload_result<'a, F, T, E>(
    workload: DatabaseWorkload,
    site: DatabaseOperationSite,
    future: F,
) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>
where
    F: Future<Output = Result<T, E>> + Send + 'a,
{
    Box::pin(async move {
        if !pioneer_observability::telemetry_enabled() {
            return future.await;
        }
        let trace = DatabaseWorkloadTrace::start_at(workload, site);
        let carrier = pioneer_observability::work::capture().with_database(trace.context());
        let output = carrier.scope_result(future).await;
        if output.is_ok() {
            trace.finish_success();
        } else {
            trace.finish_error();
        }
        output
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn nested_workload_scope_restores_the_outer_context() {
        scope_database_workload(
            DatabaseWorkload::ThreadTreeLoad,
            DatabaseOperationSite::ThreadTreeLoad,
            async {
                assert_eq!(
                    current_database_workload(),
                    Some(DatabaseWorkload::ThreadTreeLoad)
                );
                assert_eq!(
                    pioneer_observability::work::current_database()
                        .unwrap()
                        .site(),
                    DatabaseOperationSite::ThreadTreeLoad
                );
                scope_database_workload(
                    DatabaseWorkload::TimelinePage,
                    DatabaseOperationSite::TimelinePage,
                    async {
                        assert_eq!(
                            current_database_workload(),
                            Some(DatabaseWorkload::TimelinePage)
                        );
                        assert_eq!(
                            pioneer_observability::work::current_database()
                                .unwrap()
                                .site(),
                            DatabaseOperationSite::TimelinePage
                        );
                    },
                )
                .await;
                assert_eq!(
                    pioneer_observability::work::current_database()
                        .unwrap()
                        .site(),
                    DatabaseOperationSite::ThreadTreeLoad
                );
                assert_eq!(
                    current_database_workload(),
                    Some(DatabaseWorkload::ThreadTreeLoad)
                );
            },
        )
        .await;
        assert!(current_database_workload().is_none());
    }

    #[tokio::test]
    async fn result_scope_preserves_success_and_failure() {
        let success = scope_database_workload_result(
            DatabaseWorkload::ProjectionRecovery,
            DatabaseOperationSite::Unknown,
            async { Ok::<_, &'static str>(7) },
        )
        .await;
        assert_eq!(success, Ok(7));

        let failure = scope_database_workload_result(
            DatabaseWorkload::ProjectionRecovery,
            DatabaseOperationSite::Unknown,
            async { Err::<(), _>("failed") },
        )
        .await;
        assert_eq!(failure, Err("failed"));
    }
}

#[cfg(test)]
mod propagation_tests {
    use super::*;
    #[tokio::test]
    async fn actual_query_callback_keeps_nearest_owner_across_fresh_task() {
        scope_database_workload(
            DatabaseWorkload::ThreadTreeLoad,
            DatabaseOperationSite::ThreadTreeLoad,
            async {
                crate::message::message_fresh_task(async {
                    assert_eq!(
                        current_database_workload(),
                        Some(DatabaseWorkload::ThreadTreeLoad)
                    );
                    scope_database_workload(
                        DatabaseWorkload::TimelinePage,
                        DatabaseOperationSite::Unknown,
                        async {
                            // This is the same callback target called by connection.rs, with a
                            // synthetic observation: no SQL or database connection is executed.
                            record_database_query(
                                17,
                                pioneer_observability::DatabaseQueryKind::Read,
                                std::time::Duration::from_millis(3),
                            );
                            assert_eq!(
                                current_database_workload(),
                                Some(DatabaseWorkload::TimelinePage)
                            );
                        },
                    )
                    .await;
                    assert_eq!(
                        current_database_workload(),
                        Some(DatabaseWorkload::ThreadTreeLoad)
                    );
                })
                .await
                .unwrap();
            },
        )
        .await;
        assert_eq!(current_database_workload(), None);
    }
}
