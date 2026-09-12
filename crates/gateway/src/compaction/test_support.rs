//! Convenience setup used only by compaction and message tests.
use super::*;
use pioneer_agent::compaction::controller::{
    NativeContext, NativePreparedRequest, NativeUsageMeasurement,
};
use pioneer_compaction::CompactionSettings;
use pioneer_provider::{ChatRequest, ProviderRegistry};

/// Freeze one accepted parent line at admission. Source revisions are checked
/// during rendering and restoration; the epoch check also detects edits to rows
/// omitted by a concurrent edit/delete during discovery. Appends beyond the
/// fence are intentionally excluded and do not invalidate the snapshot.
pub(crate) async fn capture_line_json(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
) -> Result<String> {
    capture_selected_line_json(store, workspace, thread, excluded_turn, None).await
}

/// Select the admitted Task basis before persisting its reference manifest.
/// Composer passes no policy and retains its separately accepted launch scope.
/// An ordinary Task passes its policy (or its explicit default); restoration
/// never reevaluates that policy against a later parent history.
pub(crate) async fn capture_selected_line_json(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    excluded_turn: Option<&str>,
    policy: Option<&pioneer_protocol::TaskAgentContextPolicy>,
) -> Result<String> {
    super::frozen::capture_execution_basis_json(
        store,
        workspace,
        thread,
        excluded_turn,
        excluded_turn,
        policy,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_native_request(
    store: &CrudStore,
    providers: &ProviderRegistry,
    settings: &CompactionSettings,
    context: &NativeContext,
    request: ChatRequest,
    measured: Option<NativeUsageMeasurement>,
    recovery: bool,
    observer: Arc<dyn CompactionObserver>,
    clock: Arc<dyn CompactionClock>,
) -> Result<NativePreparedRequest> {
    super::native::prepare_native_projection(
        store, providers, settings, context, request, measured, recovery, observer, clock, None,
        None,
    )
    .await
}
