use pioneer_observability::{DatabaseWorkload, DatabaseWorkloadContext, DatabaseWorkloadTrace};
use std::future::Future;
use std::pin::Pin;

tokio::task_local! {
    static CURRENT_DATABASE_WORKLOAD: DatabaseWorkloadContext;
}

pub(crate) fn current_database_workload() -> Option<DatabaseWorkload> {
    CURRENT_DATABASE_WORKLOAD
        .try_with(DatabaseWorkloadContext::workload)
        .ok()
}

pub(crate) fn record_database_query(
    fingerprint: u64,
    kind: pioneer_observability::DatabaseQueryKind,
    elapsed: std::time::Duration,
) {
    let _ = CURRENT_DATABASE_WORKLOAD
        .try_with(|context| context.record_query(fingerprint, kind, elapsed));
}

pub(crate) fn scope_database_workload<'a, F>(
    workload: DatabaseWorkload,
    future: F,
) -> Pin<Box<dyn Future<Output = F::Output> + Send + 'a>>
where
    F: Future + Send + 'a,
{
    Box::pin(async move {
        let trace = DatabaseWorkloadTrace::start(workload);
        let output = CURRENT_DATABASE_WORKLOAD
            .scope(trace.context(), future)
            .await;
        trace.finish_success();
        output
    })
}

pub(crate) fn scope_database_workload_result<'a, F, T, E>(
    workload: DatabaseWorkload,
    future: F,
) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>
where
    F: Future<Output = Result<T, E>> + Send + 'a,
{
    Box::pin(async move {
        let trace = DatabaseWorkloadTrace::start(workload);
        let output = CURRENT_DATABASE_WORKLOAD
            .scope(trace.context(), future)
            .await;
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
        scope_database_workload(DatabaseWorkload::ThreadTreeLoad, async {
            assert_eq!(
                current_database_workload(),
                Some(DatabaseWorkload::ThreadTreeLoad)
            );
            scope_database_workload(DatabaseWorkload::TimelinePage, async {
                assert_eq!(
                    current_database_workload(),
                    Some(DatabaseWorkload::TimelinePage)
                );
            })
            .await;
            assert_eq!(
                current_database_workload(),
                Some(DatabaseWorkload::ThreadTreeLoad)
            );
        })
        .await;
        assert!(current_database_workload().is_none());
    }

    #[tokio::test]
    async fn result_scope_preserves_success_and_failure() {
        let success = scope_database_workload_result(DatabaseWorkload::ProjectionRecovery, async {
            Ok::<_, &'static str>(7)
        })
        .await;
        assert_eq!(success, Ok(7));

        let failure = scope_database_workload_result(DatabaseWorkload::ProjectionRecovery, async {
            Err::<(), _>("failed")
        })
        .await;
        assert_eq!(failure, Err("failed"));
    }
}
