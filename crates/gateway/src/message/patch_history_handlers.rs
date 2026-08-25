use super::*;
use crate::authorization::{AuthorizationExternalError, AuthorizedThread, AuthorizedTurn};
use anyhow::{Result, anyhow, bail};
use pioneer_protocol::{
    PatchAppliedStep, PatchDiffSelection, PatchFileHistoryCursor, PatchFileHistoryEntry,
    PatchHistoryAuthorityView, PatchHistoryChange, PatchHistoryChangeKind,
    PatchHistoryCoverageView, PatchHistoryErrorCode, PatchHistoryLineEnding,
    PatchHistoryLineEndingMetadata, PatchHistoryProvenanceView, PatchHistoryQueryCoverage,
    PatchHistoryRecord, PatchHistoryRecordOutcome, PatchHistorySideEffects,
    PatchHistorySnapshotRef, PatchHistoryStage, PatchHistoryTextEncoding, PatchRecordExactnessView,
    PatchRecordSelector, PatchThreadHistoryCursor, ThreadFilePatchHistoryPageParams,
    ThreadFilePatchHistoryPageResponse, ThreadPatchStepsPageParams, ThreadPatchStepsPageResponse,
    TurnDiffExactnessView, TurnPatchDiffGetParams, TurnPatchDiffGetResponse,
    TurnPatchRecordGetParams, TurnPatchRecordGetResponse, TurnPatchStepsPageParams,
    TurnPatchStepsPageResponse,
};
use pioneer_tools::apply_patch::history::{
    CommitOrdinal, FileHistoryCursor, HistoryCoverage, HistoryQueryLimits, InvocationIdentity,
    SqliteAppliedPatchStore, SqliteCodexAggregateStore, StoredPatchRecord, ThreadHistoryCursor,
};

const DEFAULT_HISTORY_PAGE_RECORDS: usize = 100;
const MAX_HISTORY_PAGE_RECORDS: usize = 100;
const MAX_HISTORY_PAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_HISTORY_PAGE_DECOMPRESSED_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_HISTORY_DIFF_BYTES: usize = 4 * 1024 * 1024;
const MAX_HISTORY_DIFF_BYTES: usize = 16 * 1024 * 1024;
const MAX_HISTORY_SELECTOR_BYTES: usize = 4096;

enum PatchRecordSelectionError {
    Invalid(&'static str),
    Internal(anyhow::Error),
}

fn history_limits(limit: Option<u32>) -> Result<HistoryQueryLimits> {
    let limit = limit
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))
        .unwrap_or(DEFAULT_HISTORY_PAGE_RECORDS);
    if !(1..=MAX_HISTORY_PAGE_RECORDS).contains(&limit) {
        bail!("history page limit must be between 1 and {MAX_HISTORY_PAGE_RECORDS}");
    }
    Ok(HistoryQueryLimits {
        max_page_records: limit,
        max_page_bytes: MAX_HISTORY_PAGE_BYTES,
        max_decompressed_bytes: MAX_HISTORY_PAGE_DECOMPRESSED_BYTES,
    })
}

fn history_diff_limit(limit: Option<u32>) -> Result<usize> {
    let limit = limit
        .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))
        .unwrap_or(DEFAULT_HISTORY_DIFF_BYTES);
    if !(1..=MAX_HISTORY_DIFF_BYTES).contains(&limit) {
        bail!("history diff maxBytes must be between 1 and {MAX_HISTORY_DIFF_BYTES}");
    }
    Ok(limit)
}

impl MessageProcessor {
    async fn send_patch_history_result<T: serde::Serialize>(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        result: &T,
    ) {
        match JsonRpcResponse::from_result(request_id, result) {
            Ok(response) => {
                if let Err(error) = self.send_json(connection_id, &response).await {
                    warn!(error = %format!("{error:#}"), "failed to send patch history response");
                }
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    patch_history_error(None, INVALID_REQUEST_CODE, error),
                )
                .await;
            }
        }
    }

    pub(super) async fn turn_patch_steps_page(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnPatchStepsPageParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim()
            || authorization.turn_id() != params.turn_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let limits = match history_limits(params.limit) {
            Ok(limits) => limits,
            Err(error) => {
                self.send_error(
                    connection_id,
                    patch_history_error(Some(request_id), INVALID_PARAMS_CODE, error),
                )
                .await;
                return;
            }
        };
        let database = self.crud_store.database_connection();
        let records = SqliteAppliedPatchStore::new(database.clone());
        let codex = SqliteCodexAggregateStore::new(database);
        let codex_state = match codex
            .get(params.thread_id.trim(), params.turn_id.trim())
            .await
        {
            Ok(state) => state,
            Err(error) => {
                self.send_patch_history_internal_error(connection_id, request_id, error)
                    .await;
                return;
            }
        };
        if let Some(state) = codex_state {
            let coverage = query_coverage_from_codex(&state);
            self.send_patch_history_result(
                connection_id,
                request_id,
                &TurnPatchStepsPageResponse {
                    thread_id: params.thread_id,
                    turn_id: params.turn_id,
                    items: Vec::new(),
                    next_cursor: None,
                    coverage,
                },
            )
            .await;
            return;
        }
        let cursor = params.after_ordinal.map(CommitOrdinal);
        match records
            .query_turn_steps(
                params.thread_id.trim(),
                params.turn_id.trim(),
                cursor,
                limits,
            )
            .await
        {
            Ok(page) => {
                let coverage = query_coverage(&page.coverage);
                let items = page
                    .items
                    .iter()
                    .map(|step| PatchAppliedStep {
                        record: record_view(&step.record),
                        coverage: coverage.clone(),
                    })
                    .collect();
                self.send_patch_history_result(
                    connection_id,
                    request_id,
                    &TurnPatchStepsPageResponse {
                        thread_id: params.thread_id,
                        turn_id: params.turn_id,
                        items,
                        next_cursor: page.next_cursor.map(|cursor| cursor.0),
                        coverage,
                    },
                )
                .await;
            }
            Err(error) => {
                self.send_patch_history_query_error(connection_id, request_id, error)
                    .await;
            }
        }
    }

    pub(super) async fn thread_patch_steps_page(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedThread,
        request_id: RequestId,
        params: ThreadPatchStepsPageParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let limits = match history_limits(params.limit) {
            Ok(limits) => limits,
            Err(error) => {
                self.send_error(
                    connection_id,
                    patch_history_error(Some(request_id), INVALID_PARAMS_CODE, error),
                )
                .await;
                return;
            }
        };
        let cursor = params.cursor.as_ref().map(|cursor| ThreadHistoryCursor {
            turn_id: cursor.turn_id.clone(),
            ordinal: CommitOrdinal(cursor.ordinal),
        });
        let database = self.crud_store.database_connection();
        let records = SqliteAppliedPatchStore::new(database.clone());
        let page = match records
            .query_thread_steps(params.thread_id.trim(), cursor, limits)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                self.send_patch_history_query_error(connection_id, request_id, error)
                    .await;
                return;
            }
        };
        let coverage = match thread_query_coverage(
            &records,
            &SqliteCodexAggregateStore::new(database),
            params.thread_id.trim(),
            &page.coverage,
        )
        .await
        {
            Ok(coverage) => coverage,
            Err(error) => {
                self.send_patch_history_internal_error(connection_id, request_id, error)
                    .await;
                return;
            }
        };
        let items = page
            .items
            .iter()
            .map(|step| PatchAppliedStep {
                record: record_view(&step.record),
                coverage: coverage.clone(),
            })
            .collect();
        self.send_patch_history_result(
            connection_id,
            request_id,
            &ThreadPatchStepsPageResponse {
                thread_id: params.thread_id,
                items,
                next_cursor: page
                    .next_thread_cursor
                    .map(|cursor| PatchThreadHistoryCursor {
                        turn_id: cursor.turn_id,
                        ordinal: cursor.ordinal.0,
                    }),
                coverage,
            },
        )
        .await;
    }

    pub(super) async fn thread_file_patch_history_page(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedThread,
        request_id: RequestId,
        params: ThreadFilePatchHistoryPageParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim() {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let limits = match history_limits(params.limit) {
            Ok(limits) => limits,
            Err(error) => {
                self.send_error(
                    connection_id,
                    patch_history_error(Some(request_id), INVALID_PARAMS_CODE, error),
                )
                .await;
                return;
            }
        };
        let cursor = params.cursor.as_ref().map(|cursor| FileHistoryCursor {
            environment_id: cursor.environment_id.clone(),
            turn_id: cursor.turn_id.clone(),
            ordinal: CommitOrdinal(cursor.ordinal),
            sequence: cursor.sequence,
        });
        let database = self.crud_store.database_connection();
        let records = SqliteAppliedPatchStore::new(database.clone());
        let page = match records
            .query_file_history(params.thread_id.trim(), params.path.trim(), cursor, limits)
            .await
        {
            Ok(page) => page,
            Err(error) => {
                self.send_patch_history_query_error(connection_id, request_id, error)
                    .await;
                return;
            }
        };
        let coverage = match thread_query_coverage(
            &records,
            &SqliteCodexAggregateStore::new(database),
            params.thread_id.trim(),
            &page.coverage,
        )
        .await
        {
            Ok(coverage) => coverage,
            Err(error) => {
                self.send_patch_history_internal_error(connection_id, request_id, error)
                    .await;
                return;
            }
        };
        let items = page
            .items
            .iter()
            .map(|entry| PatchFileHistoryEntry {
                environment_id: entry.environment_id.clone(),
                turn_id: entry.turn_id.clone(),
                ordinal: entry.ordinal.0,
                invocation_id: entry.invocation_id.clone(),
                change: change_view(&entry.change),
            })
            .collect();
        self.send_patch_history_result(
            connection_id,
            request_id,
            &ThreadFilePatchHistoryPageResponse {
                thread_id: params.thread_id,
                path: params.path,
                items,
                next_cursor: page.next_file_cursor.map(|cursor| PatchFileHistoryCursor {
                    environment_id: cursor.environment_id,
                    turn_id: cursor.turn_id,
                    ordinal: cursor.ordinal.0,
                    sequence: cursor.sequence,
                }),
                coverage,
            },
        )
        .await;
    }

    pub(super) async fn turn_patch_record_get(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnPatchRecordGetParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim()
            || authorization.turn_id() != params.turn_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let records = SqliteAppliedPatchStore::new(self.crud_store.database_connection());
        match selected_record(
            &records,
            params.thread_id.trim(),
            params.turn_id.trim(),
            &params.selector,
        )
        .await
        {
            Ok(Some(record)) => {
                self.send_patch_history_result(
                    connection_id,
                    request_id,
                    &TurnPatchRecordGetResponse {
                        record: record_view(&record),
                    },
                )
                .await;
            }
            Ok(None) => {
                self.send_error(
                    connection_id,
                    AuthorizationExternalError::NotFound.response(request_id),
                )
                .await;
            }
            Err(PatchRecordSelectionError::Invalid(message)) => {
                self.send_error(
                    connection_id,
                    patch_history_error(Some(request_id), INVALID_PARAMS_CODE, message),
                )
                .await;
            }
            Err(PatchRecordSelectionError::Internal(error)) => {
                self.send_patch_history_internal_error(connection_id, request_id, error)
                    .await;
            }
        }
    }

    pub(super) async fn turn_patch_diff_get(
        &self,
        request_context: &RequestContext,
        authorization: &AuthorizedTurn,
        request_id: RequestId,
        params: TurnPatchDiffGetParams,
    ) {
        let connection_id = request_context.connection_id();
        if authorization.thread_id() != params.thread_id.trim()
            || authorization.turn_id() != params.turn_id.trim()
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }
        let max_bytes = match history_diff_limit(params.max_bytes) {
            Ok(limit) => limit,
            Err(error) => {
                self.send_error(
                    connection_id,
                    patch_history_error(Some(request_id), INVALID_PARAMS_CODE, error),
                )
                .await;
                return;
            }
        };
        let database = self.crud_store.database_connection();
        let records = SqliteAppliedPatchStore::new(database.clone());
        let rendered = match &params.selection {
            PatchDiffSelection::Record { selector } => {
                match selected_record(
                    &records,
                    params.thread_id.trim(),
                    params.turn_id.trim(),
                    selector,
                )
                .await
                {
                    Ok(Some(record)) => records.render_record_diff(&record, max_bytes).await,
                    Ok(None) => {
                        self.send_error(
                            connection_id,
                            AuthorizationExternalError::NotFound.response(request_id),
                        )
                        .await;
                        return;
                    }
                    Err(PatchRecordSelectionError::Invalid(message)) => {
                        self.send_error(
                            connection_id,
                            patch_history_error(Some(request_id), INVALID_PARAMS_CODE, message),
                        )
                        .await;
                        return;
                    }
                    Err(PatchRecordSelectionError::Internal(error)) => Err(error),
                }
            }
            PatchDiffSelection::Boundary {
                after_ordinal,
                through_ordinal,
            } => {
                let codex = SqliteCodexAggregateStore::new(database);
                match codex
                    .get(params.thread_id.trim(), params.turn_id.trim())
                    .await
                {
                    Ok(Some(state)) if after_ordinal.is_none() && through_ordinal.is_none() => {
                        if state.diff.len() > max_bytes {
                            self.send_error(
                                connection_id,
                                patch_history_error(
                                    Some(request_id),
                                    INVALID_PARAMS_CODE,
                                    "provider aggregate diff exceeds the requested output limit",
                                ),
                            )
                            .await;
                            return;
                        }
                        self.send_patch_history_result(
                            connection_id,
                            request_id,
                            &TurnPatchDiffGetResponse {
                                thread_id: params.thread_id,
                                turn_id: params.turn_id,
                                exactness: exactness_view(&state.exactness),
                                coverage: coverage_view(&state.coverage),
                                unified_patch: state.diff,
                                records_rendered: 0,
                                after_ordinal: None,
                                through_ordinal: None,
                            },
                        )
                        .await;
                        return;
                    }
                    Ok(Some(_)) => {
                        self.send_error(
                            connection_id,
                            patch_history_error(
                                Some(request_id),
                                INVALID_PARAMS_CODE,
                                "aggregate-only provider history has no step boundaries",
                            ),
                        )
                        .await;
                        return;
                    }
                    Ok(None) => {
                        records
                            .render_turn_diff_between(
                                params.thread_id.trim(),
                                params.turn_id.trim(),
                                after_ordinal.map(CommitOrdinal),
                                through_ordinal.map(CommitOrdinal),
                                max_bytes,
                            )
                            .await
                    }
                    Err(error) => Err(error),
                }
            }
        };
        match rendered {
            Ok(rendered) => {
                self.send_patch_history_result(
                    connection_id,
                    request_id,
                    &TurnPatchDiffGetResponse {
                        thread_id: params.thread_id,
                        turn_id: params.turn_id,
                        exactness: exactness_view(&rendered.exactness),
                        coverage: coverage_view(&rendered.coverage),
                        unified_patch: rendered.unified_patch,
                        records_rendered: rendered.records_rendered,
                        after_ordinal: rendered.after_ordinal.map(|ordinal| ordinal.0),
                        through_ordinal: rendered.through_ordinal.map(|ordinal| ordinal.0),
                    },
                )
                .await;
            }
            Err(error) => {
                self.send_patch_history_internal_error(connection_id, request_id, error)
                    .await;
            }
        }
    }

    async fn send_patch_history_query_error(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        error: pioneer_tools::apply_patch::history::HistoryQueryError,
    ) {
        match error {
            pioneer_tools::apply_patch::history::HistoryQueryError::InvalidLimit => {
                self.send_error(
                    connection_id,
                    patch_history_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        "history query limit is invalid",
                    ),
                )
                .await;
            }
            pioneer_tools::apply_patch::history::HistoryQueryError::InvalidArgument => {
                self.send_error(
                    connection_id,
                    patch_history_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        "history query argument is invalid",
                    ),
                )
                .await;
            }
            pioneer_tools::apply_patch::history::HistoryQueryError::PageTooLarge => {
                self.send_error(
                    connection_id,
                    patch_history_error(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        "history page exceeds the requested output limit",
                    ),
                )
                .await;
            }
            pioneer_tools::apply_patch::history::HistoryQueryError::Store(message) => {
                self.send_patch_history_internal_error(
                    connection_id,
                    request_id,
                    anyhow!("history store query failed: {message}"),
                )
                .await;
            }
        }
    }

    async fn send_patch_history_internal_error(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        error: anyhow::Error,
    ) {
        warn!(error = %format!("{error:#}"), "patch history query failed");
        self.send_error(
            connection_id,
            patch_history_error(
                Some(request_id),
                INVALID_REQUEST_CODE,
                "patch history is temporarily unavailable",
            ),
        )
        .await;
    }
}

fn patch_history_error(
    request_id: Option<RequestId>,
    jsonrpc_code: i64,
    diagnostic: impl std::fmt::Display,
) -> JsonRpcErrorResponse {
    crate::public_error::agent_rpc_error(
        request_id,
        jsonrpc_code,
        if jsonrpc_code == INVALID_PARAMS_CODE {
            pioneer_protocol::PublicErrorCode::InvalidInput
        } else {
            pioneer_protocol::PublicErrorCode::Internal
        },
        pioneer_protocol::PublicErrorStage::Observation,
        diagnostic,
    )
}

async fn selected_record(
    records: &SqliteAppliedPatchStore,
    thread_id: &str,
    turn_id: &str,
    selector: &PatchRecordSelector,
) -> std::result::Result<Option<StoredPatchRecord>, PatchRecordSelectionError> {
    match selector {
        PatchRecordSelector::RecordId { record_id } => {
            let record_id = record_id.trim();
            if record_id.is_empty() || record_id.len() > MAX_HISTORY_SELECTOR_BYTES {
                return Err(PatchRecordSelectionError::Invalid(
                    "patch record selector is invalid",
                ));
            }
            records
                .get_by_record_id(thread_id, turn_id, record_id)
                .await
                .map_err(PatchRecordSelectionError::Internal)
        }
        PatchRecordSelector::Invocation { invocation_id } => {
            let identity = InvocationIdentity::new(thread_id, turn_id, invocation_id.trim())
                .map_err(|_| {
                    PatchRecordSelectionError::Invalid("patch invocation selector is invalid")
                })?;
            records
                .get(&identity)
                .await
                .map_err(PatchRecordSelectionError::Internal)
        }
    }
}

async fn thread_query_coverage(
    records: &SqliteAppliedPatchStore,
    codex: &SqliteCodexAggregateStore,
    thread_id: &str,
    base: &HistoryCoverage,
) -> Result<PatchHistoryQueryCoverage> {
    let Some(codex_state) = codex.first_for_thread(thread_id).await? else {
        return Ok(query_coverage(base));
    };
    if records.record_count_for_thread(thread_id).await? == 0 {
        return Ok(query_coverage_from_codex(&codex_state));
    }
    let coverage = pioneer_tools::apply_patch::history::PatchHistoryCoverage::Incomplete {
        reason: "thread contains both engine step history and aggregate-only provider history"
            .to_owned(),
    };
    Ok(PatchHistoryQueryCoverage {
        exactness: exactness_view(
            &pioneer_tools::apply_patch::history::TurnDiffExactness::from_coverage(
                false, &coverage,
            ),
        ),
        exact: false,
        coverage: coverage_view(&coverage),
        first_missing_ordinal: base.first_missing_ordinal.map(|ordinal| ordinal.0),
    })
}

fn query_coverage(coverage: &HistoryCoverage) -> PatchHistoryQueryCoverage {
    PatchHistoryQueryCoverage {
        exactness: exactness_view(
            &pioneer_tools::apply_patch::history::TurnDiffExactness::from_coverage(
                coverage.exact,
                &coverage.coverage,
            ),
        ),
        exact: coverage.exact,
        coverage: coverage_view(&coverage.coverage),
        first_missing_ordinal: coverage.first_missing_ordinal.map(|ordinal| ordinal.0),
    }
}

fn query_coverage_from_codex(
    state: &pioneer_tools::apply_patch::history::CodexAggregateState,
) -> PatchHistoryQueryCoverage {
    PatchHistoryQueryCoverage {
        exactness: exactness_view(&state.exactness),
        exact: state.exact,
        coverage: coverage_view(&state.coverage),
        first_missing_ordinal: None,
    }
}

fn record_view(stored: &StoredPatchRecord) -> PatchHistoryRecord {
    let record = &stored.record;
    PatchHistoryRecord {
        schema_version: record.schema_version,
        record_id: pioneer_tools::apply_patch::history::applied_patch_record_id(
            &record.identity,
            record.commit_ordinal.0,
        ),
        thread_id: record.identity.thread_id.clone(),
        turn_id: record.identity.turn_id.clone(),
        invocation_id: record.identity.invocation_id.clone(),
        environment_id: record.environment_id.clone(),
        commit_ordinal: record.commit_ordinal.0,
        authority: match record.authority {
            pioneer_tools::apply_patch::history::TurnDiffAuthority::NativePatchEngine => {
                PatchHistoryAuthorityView::NativePatchEngine
            }
            pioneer_tools::apply_patch::history::TurnDiffAuthority::CodexAggregateEvent => {
                PatchHistoryAuthorityView::CodexAggregateEvent
            }
            pioneer_tools::apply_patch::history::TurnDiffAuthority::ManagedClaudePatchEngine => {
                PatchHistoryAuthorityView::ManagedClaudePatchEngine
            }
            pioneer_tools::apply_patch::history::TurnDiffAuthority::Unsupported => {
                PatchHistoryAuthorityView::Unsupported
            }
        },
        provenance: match record.provenance {
            pioneer_tools::apply_patch::history::PatchHistoryProvenance::NativeEngine => {
                PatchHistoryProvenanceView::NativeEngine
            }
            pioneer_tools::apply_patch::history::PatchHistoryProvenance::ManagedClaude => {
                PatchHistoryProvenanceView::ManagedClaude
            }
            pioneer_tools::apply_patch::history::PatchHistoryProvenance::Recovery => {
                PatchHistoryProvenanceView::Recovery
            }
            pioneer_tools::apply_patch::history::PatchHistoryProvenance::ProviderAggregate => {
                PatchHistoryProvenanceView::ProviderAggregate
            }
            pioneer_tools::apply_patch::history::PatchHistoryProvenance::Unknown => {
                PatchHistoryProvenanceView::Unknown
            }
        },
        exactness: match record.exactness {
            pioneer_tools::apply_patch::history::PatchRecordExactness::Exact => PatchRecordExactnessView::Exact,
            pioneer_tools::apply_patch::history::PatchRecordExactness::Partial => {
                PatchRecordExactnessView::Partial
            }
            pioneer_tools::apply_patch::history::PatchRecordExactness::Uncertain => {
                PatchRecordExactnessView::Uncertain
            }
        },
        committed_at_unix_ms: record.committed_at_unix_ms,
        outcome: match &record.outcome {
            pioneer_tools::apply_patch::history::AppliedPatchRecordOutcome::Applied => {
                PatchHistoryRecordOutcome::Applied
            }
            pioneer_tools::apply_patch::history::AppliedPatchRecordOutcome::Partial {
                failed_stage,
                error_code,
            } => PatchHistoryRecordOutcome::Partial {
                failed_stage: stage_view(*failed_stage),
                error_code: error_code_view(*error_code),
            },
            pioneer_tools::apply_patch::history::AppliedPatchRecordOutcome::CommitStateUncertain => {
                PatchHistoryRecordOutcome::CommitStateUncertain
            }
            pioneer_tools::apply_patch::history::AppliedPatchRecordOutcome::Gap { reason } => {
                PatchHistoryRecordOutcome::Gap {
                    reason: reason.clone(),
                }
            }
        },
        changes: record.changes.iter().map(change_view).collect(),
        side_effects: side_effects_view(&record.side_effects),
    }
}

fn change_view(
    change: &pioneer_tools::apply_patch::history::DurablePatchChange,
) -> PatchHistoryChange {
    PatchHistoryChange {
        operation_index: change.operation_index,
        commit_step: change.commit_step,
        sequence: change.sequence,
        kind: match change.kind {
            pioneer_tools::apply_patch::history::ChangeKind::Add => PatchHistoryChangeKind::Add,
            pioneer_tools::apply_patch::history::ChangeKind::Replace => {
                PatchHistoryChangeKind::Replace
            }
            pioneer_tools::apply_patch::history::ChangeKind::Update => {
                PatchHistoryChangeKind::Update
            }
            pioneer_tools::apply_patch::history::ChangeKind::Delete => {
                PatchHistoryChangeKind::Delete
            }
            pioneer_tools::apply_patch::history::ChangeKind::Move => PatchHistoryChangeKind::Move,
        },
        source_path: change.source_path.clone(),
        destination_path: change.destination_path.clone(),
        before: change.before.as_ref().map(snapshot_view),
        after: change.after.as_ref().map(snapshot_view),
        overwritten_destination: change.overwritten_destination.as_ref().map(snapshot_view),
        side_effects: side_effects_view(&change.side_effects),
    }
}

fn side_effects_view(
    side_effects: &pioneer_tools::apply_patch::history::PatchSideEffects,
) -> PatchHistorySideEffects {
    PatchHistorySideEffects {
        created_directories: side_effects.created_directories.clone(),
        residual_directories: side_effects.residual_directories.clone(),
        metadata_warnings: side_effects.metadata_warnings.clone(),
        exact: side_effects.exact,
    }
}

fn snapshot_view(
    reference: &pioneer_tools::apply_patch::history::TextSnapshotRef,
) -> PatchHistorySnapshotRef {
    PatchHistorySnapshotRef {
        schema_version: reference.schema_version,
        content_hash: hex::encode(reference.content_hash),
        byte_len: reference.byte_len,
        encoding: match reference.encoding {
            pioneer_tools::apply_patch::history::TextEncoding::Utf8 => {
                PatchHistoryTextEncoding::Utf8
            }
            pioneer_tools::apply_patch::history::TextEncoding::Utf8Bom => {
                PatchHistoryTextEncoding::Utf8Bom
            }
        },
        line_endings: PatchHistoryLineEndingMetadata {
            dominant: match reference.line_endings.dominant {
                pioneer_tools::apply_patch::history::LineEnding::Lf => PatchHistoryLineEnding::Lf,
                pioneer_tools::apply_patch::history::LineEnding::Crlf => {
                    PatchHistoryLineEnding::Crlf
                }
                pioneer_tools::apply_patch::history::LineEnding::Mixed => {
                    PatchHistoryLineEnding::Mixed
                }
                pioneer_tools::apply_patch::history::LineEnding::None => {
                    PatchHistoryLineEnding::None
                }
            },
            mixed: reference.line_endings.mixed,
            final_newline: reference.line_endings.final_newline,
        },
    }
}

fn coverage_view(
    coverage: &pioneer_tools::apply_patch::history::PatchHistoryCoverage,
) -> PatchHistoryCoverageView {
    match coverage {
        pioneer_tools::apply_patch::history::PatchHistoryCoverage::EngineVerifiedSteps => {
            PatchHistoryCoverageView::EngineVerifiedSteps
        }
        pioneer_tools::apply_patch::history::PatchHistoryCoverage::ProviderReportedSteps {
            provider,
            protocol,
        } => PatchHistoryCoverageView::ProviderReportedSteps {
            provider: provider.clone(),
            protocol: protocol.clone(),
        },
        pioneer_tools::apply_patch::history::PatchHistoryCoverage::AggregateOnly {
            provider,
            protocol,
        } => PatchHistoryCoverageView::AggregateOnly {
            provider: provider.clone(),
            protocol: protocol.clone(),
        },
        pioneer_tools::apply_patch::history::PatchHistoryCoverage::Incomplete { reason } => {
            PatchHistoryCoverageView::Incomplete {
                reason: reason.clone(),
            }
        }
        pioneer_tools::apply_patch::history::PatchHistoryCoverage::Untracked { reason } => {
            PatchHistoryCoverageView::Untracked {
                reason: reason.clone(),
            }
        }
    }
}

fn exactness_view(
    exactness: &pioneer_tools::apply_patch::history::TurnDiffExactness,
) -> TurnDiffExactnessView {
    match exactness {
        pioneer_tools::apply_patch::history::TurnDiffExactness::EngineVerified => {
            TurnDiffExactnessView::EngineVerified
        }
        pioneer_tools::apply_patch::history::TurnDiffExactness::ProviderReported {
            provider,
            protocol,
        } => TurnDiffExactnessView::ProviderReported {
            provider: provider.clone(),
            protocol: protocol.clone(),
        },
        pioneer_tools::apply_patch::history::TurnDiffExactness::Incomplete { reason } => {
            TurnDiffExactnessView::Incomplete {
                reason: reason.clone(),
            }
        }
    }
}

fn stage_view(stage: pioneer_tools::apply_patch::file_mutation::PatchStage) -> PatchHistoryStage {
    match stage {
        pioneer_tools::apply_patch::file_mutation::PatchStage::Normalize => {
            PatchHistoryStage::Normalize
        }
        pioneer_tools::apply_patch::file_mutation::PatchStage::Parse => PatchHistoryStage::Parse,
        pioneer_tools::apply_patch::file_mutation::PatchStage::Resolve => {
            PatchHistoryStage::Resolve
        }
        pioneer_tools::apply_patch::file_mutation::PatchStage::Authorize => {
            PatchHistoryStage::Authorize
        }
        pioneer_tools::apply_patch::file_mutation::PatchStage::Prepare => {
            PatchHistoryStage::Prepare
        }
        pioneer_tools::apply_patch::file_mutation::PatchStage::Lock => PatchHistoryStage::Lock,
        pioneer_tools::apply_patch::file_mutation::PatchStage::Stage => PatchHistoryStage::Stage,
        pioneer_tools::apply_patch::file_mutation::PatchStage::Commit => PatchHistoryStage::Commit,
        pioneer_tools::apply_patch::file_mutation::PatchStage::Record => PatchHistoryStage::Record,
        pioneer_tools::apply_patch::file_mutation::PatchStage::Recover => {
            PatchHistoryStage::Recover
        }
    }
}

fn error_code_view(
    code: pioneer_tools::apply_patch::file_mutation::PatchErrorCode,
) -> PatchHistoryErrorCode {
    match code {
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::InvalidLimits => {
            PatchHistoryErrorCode::InvalidLimits
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::InvalidPayload => {
            PatchHistoryErrorCode::InvalidPayload
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::PatchSyntaxError => {
            PatchHistoryErrorCode::PatchSyntaxError
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::PatchEmpty => {
            PatchHistoryErrorCode::PatchEmpty
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::InputTooLarge => {
            PatchHistoryErrorCode::InputTooLarge
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::InvalidVersionToken => {
            PatchHistoryErrorCode::InvalidVersionToken
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::InvalidRequest => {
            PatchHistoryErrorCode::InvalidRequest
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::TooManyOperations => {
            PatchHistoryErrorCode::TooManyOperations
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::TooManyFiles => {
            PatchHistoryErrorCode::TooManyFiles
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::TooManyHunks => {
            PatchHistoryErrorCode::TooManyHunks
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::InvalidPath => {
            PatchHistoryErrorCode::InvalidPath
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::PathOutsideAllowedRoot => {
            PatchHistoryErrorCode::PathOutsideAllowedRoot
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::UnauthorizedPath => {
            PatchHistoryErrorCode::UnauthorizedPath
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::PermissionDenied => {
            PatchHistoryErrorCode::PermissionDenied
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::ContextNotFound => {
            PatchHistoryErrorCode::ContextNotFound
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::AmbiguousContext => {
            PatchHistoryErrorCode::AmbiguousContext
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::PreconditionRequired => {
            PatchHistoryErrorCode::PreconditionRequired
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::SourceMissing => {
            PatchHistoryErrorCode::SourceMissing
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::DestinationExists => {
            PatchHistoryErrorCode::DestinationExists
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::DestinationMissing => {
            PatchHistoryErrorCode::DestinationMissing
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::StaleFile => {
            PatchHistoryErrorCode::StaleFile
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::CrossDeviceMove => {
            PatchHistoryErrorCode::CrossDeviceMove
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::UnsupportedContent => {
            PatchHistoryErrorCode::UnsupportedContent
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::UnsupportedFileType => {
            PatchHistoryErrorCode::UnsupportedFileType
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::InvalidUtf8 => {
            PatchHistoryErrorCode::InvalidUtf8
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::FileTooLarge => {
            PatchHistoryErrorCode::FileTooLarge
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::LockTimeout => {
            PatchHistoryErrorCode::LockTimeout
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::IoCreateFailed => {
            PatchHistoryErrorCode::IoCreateFailed
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::IoWriteFailed => {
            PatchHistoryErrorCode::IoWriteFailed
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::IoSyncFailed => {
            PatchHistoryErrorCode::IoSyncFailed
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::IoRenameFailed => {
            PatchHistoryErrorCode::IoRenameFailed
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::IoDeleteFailed => {
            PatchHistoryErrorCode::IoDeleteFailed
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::Io => PatchHistoryErrorCode::Io,
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::PartialCommit => {
            PatchHistoryErrorCode::PartialCommit
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::CommitStateUncertain => {
            PatchHistoryErrorCode::CommitStateUncertain
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::TrackerPublishFailed => {
            PatchHistoryErrorCode::TrackerPublishFailed
        }
        pioneer_tools::apply_patch::file_mutation::PatchErrorCode::HistoryCapacity => {
            PatchHistoryErrorCode::HistoryCapacity
        }
    }
}
