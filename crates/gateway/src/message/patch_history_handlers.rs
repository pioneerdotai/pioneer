use super::*;
use crate::authorization::{AuthorizationExternalError, AuthorizedThread, AuthorizedTurn};
use anyhow::{Result, anyhow, bail};
use pioneer_protocol::{
    PatchAppliedStep, PatchDiffSelection, PatchFileHistoryCursor, PatchFileHistoryEntry,
    PatchFilesystemMutationSourceView, PatchHistoryAuthorityView, PatchHistoryChange,
    PatchHistoryChangeKind, PatchHistoryCoverageView, PatchHistoryErrorCode,
    PatchHistoryExecutionContext, PatchHistoryLineEnding, PatchHistoryLineEndingMetadata,
    PatchHistoryPathView, PatchHistoryProvenanceView, PatchHistoryQueryCoverage,
    PatchHistoryRecord, PatchHistoryRecordOutcome, PatchHistorySideEffects,
    PatchHistorySnapshotRef, PatchHistoryStage, PatchHistoryTextEncoding, PatchHistoryViewKind,
    PatchRecordExactnessView, PatchRecordSelector, PatchThreadHistoryCursor,
    ThreadFilePatchHistoryPageParams, ThreadFilePatchHistoryPageResponse,
    ThreadPatchStepsPageParams, ThreadPatchStepsPageResponse, TurnDiffExactnessView,
    TurnFilesystemCoverageView, TurnPatchDiffGetParams, TurnPatchDiffGetResponse,
    TurnPatchRecordGetParams, TurnPatchRecordGetResponse, TurnPatchStepsPageParams,
    TurnPatchStepsPageResponse,
};
use pioneer_tools::apply_patch::history::{
    CommitOrdinal, ExecutionHistoryCursor, FileHistoryCursor, HistoryCoverage, HistoryQueryLimits,
    InvocationIdentity, SqliteAppliedPatchStore, SqliteCodexAggregateStore, SqliteTurnDiffStore,
    StoredPatchRecord, TurnFilesystemCoverage,
};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

const DEFAULT_HISTORY_PAGE_RECORDS: usize = 100;
const MAX_HISTORY_PAGE_RECORDS: usize = 100;
const MAX_HISTORY_PAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_HISTORY_PAGE_DECOMPRESSED_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_HISTORY_DIFF_BYTES: usize = 4 * 1024 * 1024;
const MAX_HISTORY_DIFF_BYTES: usize = 16 * 1024 * 1024;
const MAX_HISTORY_SELECTOR_BYTES: usize = 4096;
const MAX_HISTORY_EXECUTION_THREADS: u64 = 256;

#[derive(Clone, Debug)]
struct PatchHistoryExecutionScope {
    thread_id: String,
    execution_id: Option<String>,
    run_id: Option<String>,
    presented_thread_id: String,
}

#[derive(Clone, Debug)]
struct PatchHistoryPathContext {
    workspace_root: PathBuf,
    allowed_roots: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
struct ScopedFileHistoryEntry {
    scope: PatchHistoryExecutionScope,
    entry: pioneer_tools::apply_patch::history::FileHistoryEntry,
    record: StoredPatchRecord,
}

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
                    view: PatchHistoryViewKind::Timeline,
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
                let filesystem = match turn_filesystem_coverage(
                    &SqliteTurnDiffStore::new(self.crud_store.database_connection()),
                    params.thread_id.trim(),
                    params.turn_id.trim(),
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
                let coverage = query_coverage(&page.coverage, filesystem);
                let path_context = match patch_history_path_context(
                    self.crud_store.as_ref(),
                    params.turn_id.trim(),
                )
                .await
                {
                    Ok(context) => context,
                    Err(error) => {
                        self.send_patch_history_internal_error(connection_id, request_id, error)
                            .await;
                        return;
                    }
                };
                let execution =
                    direct_execution_context(params.thread_id.trim(), params.turn_id.trim());
                let items = page
                    .items
                    .iter()
                    .map(|step| PatchAppliedStep {
                        view: PatchHistoryViewKind::Timeline,
                        execution: execution.clone(),
                        record: record_view(&step.record, &path_context),
                        coverage: coverage.clone(),
                    })
                    .collect();
                self.send_patch_history_result(
                    connection_id,
                    request_id,
                    &TurnPatchStepsPageResponse {
                        view: PatchHistoryViewKind::Timeline,
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
        let scope =
            match patch_history_execution_scope(self.crud_store.as_ref(), params.thread_id.trim())
                .await
            {
                Ok(scope) => scope,
                Err(error) => {
                    self.send_patch_history_internal_error(connection_id, request_id, error)
                        .await;
                    return;
                }
            };
        let cursor = match params.cursor.as_ref() {
            Some(cursor) => match (
                cursor.committed_at_unix_ms,
                cursor.source_thread_id.as_deref(),
            ) {
                (Some(committed_at_unix_ms), Some(source_thread_id)) => {
                    Some(ExecutionHistoryCursor {
                        committed_at_unix_ms,
                        thread_id: source_thread_id.to_owned(),
                        turn_id: cursor.turn_id.clone(),
                        ordinal: CommitOrdinal(cursor.ordinal),
                    })
                }
                _ => {
                    self.send_error(
                        connection_id,
                        patch_history_error(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            "history cursor is missing sourceThreadId or committedAtUnixMs; restart pagination without a cursor",
                        ),
                    )
                    .await;
                    return;
                }
            },
            None => None,
        };
        let database = self.crud_store.database_connection();
        let records = SqliteAppliedPatchStore::new(database.clone());
        let thread_ids = scope
            .iter()
            .map(|entry| entry.thread_id.clone())
            .collect::<Vec<_>>();
        let mut stored = match records
            .records_for_threads_page(
                thread_ids.as_slice(),
                cursor.as_ref(),
                limits.max_page_records.saturating_add(1),
            )
            .await
        {
            Ok(records) => records,
            Err(error) => {
                self.send_patch_history_internal_error(connection_id, request_id, error)
                    .await;
                return;
            }
        };
        let has_more = stored.len() > limits.max_page_records;
        stored.truncate(limits.max_page_records);
        let base_coverage = match records.coverage_for_threads(thread_ids.as_slice()).await {
            Ok(coverage) => coverage,
            Err(error) => {
                self.send_patch_history_query_error(connection_id, request_id, error)
                    .await;
                return;
            }
        };
        let filesystem = match filesystem_coverage_for_threads(
            &SqliteTurnDiffStore::new(database.clone()),
            thread_ids.as_slice(),
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
        let coverage = match thread_query_coverage_for_scope(
            &records,
            &SqliteCodexAggregateStore::new(database),
            thread_ids.as_slice(),
            &base_coverage,
            filesystem,
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
        let scope_by_thread = scope
            .into_iter()
            .map(|entry| (entry.thread_id.clone(), entry))
            .collect::<HashMap<_, _>>();
        let mut path_contexts = HashMap::new();
        for record in &stored {
            let turn_id = record.record.identity.turn_id.as_str();
            if !path_contexts.contains_key(turn_id) {
                let context = match patch_history_path_context(self.crud_store.as_ref(), turn_id)
                    .await
                {
                    Ok(context) => context,
                    Err(error) => {
                        self.send_patch_history_internal_error(connection_id, request_id, error)
                            .await;
                        return;
                    }
                };
                path_contexts.insert(turn_id.to_owned(), context);
            }
        }
        let items = stored
            .iter()
            .map(|record| {
                let identity = &record.record.identity;
                let scope = scope_by_thread
                    .get(identity.thread_id.as_str())
                    .expect("queried patch record must belong to execution scope");
                PatchAppliedStep {
                    view: PatchHistoryViewKind::Timeline,
                    execution: execution_context(scope, identity.turn_id.as_str()),
                    record: record_view(
                        record,
                        path_contexts
                            .get(identity.turn_id.as_str())
                            .expect("path context must be loaded for patch record"),
                    ),
                    coverage: coverage.clone(),
                }
            })
            .collect();
        let next_cursor = has_more
            .then(|| {
                stored.last().map(|record| PatchThreadHistoryCursor {
                    turn_id: record.record.identity.turn_id.clone(),
                    ordinal: record.record.commit_ordinal.0,
                    source_thread_id: Some(record.record.identity.thread_id.clone()),
                    committed_at_unix_ms: Some(record.record.committed_at_unix_ms),
                })
            })
            .flatten();
        self.send_patch_history_result(
            connection_id,
            request_id,
            &ThreadPatchStepsPageResponse {
                view: PatchHistoryViewKind::Timeline,
                thread_id: params.thread_id.clone(),
                items,
                next_cursor,
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
        if params.cursor.as_ref().is_some_and(|cursor| {
            cursor.source_thread_id.is_none() || cursor.committed_at_unix_ms.is_none()
        }) {
            self.send_error(
                connection_id,
                patch_history_error(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    "history cursor is missing sourceThreadId or committedAtUnixMs; restart pagination without a cursor",
                ),
            )
            .await;
            return;
        }
        let scope =
            match patch_history_execution_scope(self.crud_store.as_ref(), params.thread_id.trim())
                .await
            {
                Ok(scope) => scope,
                Err(error) => {
                    self.send_patch_history_internal_error(connection_id, request_id, error)
                        .await;
                    return;
                }
            };
        let database = self.crud_store.database_connection();
        let records = SqliteAppliedPatchStore::new(database.clone());
        let mut scoped_items = Vec::new();
        for scope_entry in &scope {
            let local_cursor = params.cursor.as_ref().and_then(|cursor| {
                (cursor.source_thread_id.as_deref() == Some(scope_entry.thread_id.as_str())).then(
                    || FileHistoryCursor {
                        environment_id: cursor.environment_id.clone(),
                        turn_id: cursor.turn_id.clone(),
                        ordinal: CommitOrdinal(cursor.ordinal),
                        sequence: cursor.sequence,
                    },
                )
            });
            let page = match records
                .query_file_history(
                    scope_entry.thread_id.as_str(),
                    params.path.trim(),
                    local_cursor,
                    limits,
                )
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    self.send_patch_history_query_error(connection_id, request_id, error)
                        .await;
                    return;
                }
            };
            for entry in page.items {
                let identity = match InvocationIdentity::new(
                    scope_entry.thread_id.clone(),
                    entry.turn_id.clone(),
                    entry.invocation_id.clone(),
                ) {
                    Ok(identity) => identity,
                    Err(error) => {
                        self.send_patch_history_internal_error(
                            connection_id,
                            request_id,
                            anyhow!("invalid indexed patch identity: {error}"),
                        )
                        .await;
                        return;
                    }
                };
                let record = match records.get(&identity).await {
                    Ok(Some(record)) => record,
                    Ok(None) => {
                        self.send_patch_history_internal_error(
                            connection_id,
                            request_id,
                            anyhow!("indexed patch history record is missing"),
                        )
                        .await;
                        return;
                    }
                    Err(error) => {
                        self.send_patch_history_internal_error(connection_id, request_id, error)
                            .await;
                        return;
                    }
                };
                scoped_items.push(ScopedFileHistoryEntry {
                    scope: scope_entry.clone(),
                    entry,
                    record,
                });
            }
        }
        scoped_items.sort_by(|left, right| {
            left.record
                .record
                .committed_at_unix_ms
                .cmp(&right.record.record.committed_at_unix_ms)
                .then_with(|| left.scope.thread_id.cmp(&right.scope.thread_id))
                .then_with(|| left.entry.turn_id.cmp(&right.entry.turn_id))
                .then_with(|| left.entry.ordinal.cmp(&right.entry.ordinal))
                .then_with(|| left.entry.change.sequence.cmp(&right.entry.change.sequence))
        });
        if let Some(cursor) = params.cursor.as_ref()
            && let (Some(committed_at), Some(source_thread_id)) = (
                cursor.committed_at_unix_ms,
                cursor.source_thread_id.as_deref(),
            )
        {
            scoped_items.retain(|item| {
                (
                    item.record.record.committed_at_unix_ms,
                    item.scope.thread_id.as_str(),
                    item.entry.turn_id.as_str(),
                    item.entry.ordinal,
                    item.entry.change.sequence,
                ) > (
                    committed_at,
                    source_thread_id,
                    cursor.turn_id.as_str(),
                    CommitOrdinal(cursor.ordinal),
                    cursor.sequence,
                )
            });
        }
        let has_more = scoped_items.len() > limits.max_page_records;
        scoped_items.truncate(limits.max_page_records);
        let thread_ids = scope
            .iter()
            .map(|entry| entry.thread_id.clone())
            .collect::<Vec<_>>();
        let base_coverage = match records.coverage_for_threads(thread_ids.as_slice()).await {
            Ok(coverage) => coverage,
            Err(error) => {
                self.send_patch_history_query_error(connection_id, request_id, error)
                    .await;
                return;
            }
        };
        let filesystem = match filesystem_coverage_for_threads(
            &SqliteTurnDiffStore::new(database.clone()),
            thread_ids.as_slice(),
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
        let coverage = match thread_query_coverage_for_scope(
            &records,
            &SqliteCodexAggregateStore::new(database),
            thread_ids.as_slice(),
            &base_coverage,
            filesystem,
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
        let mut path_contexts = HashMap::new();
        for item in &scoped_items {
            let entry = &item.entry;
            if !path_contexts.contains_key(entry.turn_id.as_str()) {
                let context = match patch_history_path_context(
                    self.crud_store.as_ref(),
                    entry.turn_id.as_str(),
                )
                .await
                {
                    Ok(context) => context,
                    Err(error) => {
                        self.send_patch_history_internal_error(connection_id, request_id, error)
                            .await;
                        return;
                    }
                };
                path_contexts.insert(entry.turn_id.clone(), context);
            }
        }
        let items = scoped_items
            .iter()
            .map(|item| {
                let entry = &item.entry;
                PatchFileHistoryEntry {
                    view: PatchHistoryViewKind::Timeline,
                    execution: execution_context(&item.scope, entry.turn_id.as_str()),
                    environment_id: entry.environment_id.clone(),
                    turn_id: entry.turn_id.clone(),
                    ordinal: entry.ordinal.0,
                    invocation_id: entry.invocation_id.clone(),
                    change: change_view(
                        &entry.change,
                        path_contexts
                            .get(entry.turn_id.as_str())
                            .expect("path context must be loaded for file history"),
                    ),
                }
            })
            .collect();
        let next_cursor = has_more
            .then(|| {
                scoped_items.last().map(|item| PatchFileHistoryCursor {
                    environment_id: item.entry.environment_id.clone(),
                    turn_id: item.entry.turn_id.clone(),
                    ordinal: item.entry.ordinal.0,
                    sequence: item.entry.change.sequence,
                    source_thread_id: Some(item.scope.thread_id.clone()),
                    committed_at_unix_ms: Some(item.record.record.committed_at_unix_ms),
                })
            })
            .flatten();
        self.send_patch_history_result(
            connection_id,
            request_id,
            &ThreadFilePatchHistoryPageResponse {
                view: PatchHistoryViewKind::Timeline,
                thread_id: params.thread_id.clone(),
                path: params.path,
                items,
                next_cursor,
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
                let path_context = match patch_history_path_context(
                    self.crud_store.as_ref(),
                    params.turn_id.trim(),
                )
                .await
                {
                    Ok(context) => context,
                    Err(error) => {
                        self.send_patch_history_internal_error(connection_id, request_id, error)
                            .await;
                        return;
                    }
                };
                self.send_patch_history_result(
                    connection_id,
                    request_id,
                    &TurnPatchRecordGetResponse {
                        record: record_view(&record, &path_context),
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
                                view: PatchHistoryViewKind::TurnAggregate,
                                thread_id: params.thread_id,
                                turn_id: params.turn_id,
                                exactness: exactness_view(&state.exactness),
                                coverage: coverage_view(&state.coverage),
                                filesystem: TurnFilesystemCoverageView::Incomplete {
                                    reason: "provider aggregate history does not prove turn-wide filesystem completeness".to_owned(),
                                    sources: Vec::new(),
                                },
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
                let filesystem = match turn_filesystem_coverage(
                    &SqliteTurnDiffStore::new(self.crud_store.database_connection()),
                    params.thread_id.trim(),
                    params.turn_id.trim(),
                )
                .await
                {
                    Ok(coverage) => filesystem_coverage_view(&coverage),
                    Err(error) => {
                        self.send_patch_history_internal_error(connection_id, request_id, error)
                            .await;
                        return;
                    }
                };
                self.send_patch_history_result(
                    connection_id,
                    request_id,
                    &TurnPatchDiffGetResponse {
                        view: match params.selection {
                            PatchDiffSelection::Record { .. } => PatchHistoryViewKind::RecordDiff,
                            PatchDiffSelection::Boundary { .. } => {
                                PatchHistoryViewKind::TurnAggregate
                            }
                        },
                        thread_id: params.thread_id,
                        turn_id: params.turn_id,
                        exactness: exactness_view(&rendered.exactness),
                        coverage: coverage_view(&rendered.coverage),
                        filesystem,
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

async fn thread_query_coverage_for_scope(
    records: &SqliteAppliedPatchStore,
    codex: &SqliteCodexAggregateStore,
    thread_ids: &[String],
    base: &HistoryCoverage,
    filesystem: TurnFilesystemCoverage,
) -> Result<PatchHistoryQueryCoverage> {
    let mut codex_threads = 0_u64;
    let mut record_count = 0_u64;
    for thread_id in thread_ids {
        if codex.first_for_thread(thread_id).await?.is_some() {
            codex_threads = codex_threads.saturating_add(1);
        }
        record_count =
            record_count.saturating_add(records.record_count_for_thread(thread_id).await?);
    }
    if codex_threads == 0 {
        return Ok(query_coverage(base, filesystem));
    }
    if record_count == 0 && thread_ids.len() == 1 {
        if let Some(state) = codex.first_for_thread(thread_ids[0].as_str()).await? {
            return Ok(query_coverage_from_codex(&state));
        }
    }
    let coverage = pioneer_tools::apply_patch::history::PatchHistoryCoverage::Incomplete {
        reason:
            "execution scope contains both engine step history and aggregate-only provider history"
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
        filesystem: filesystem_coverage_view(&filesystem),
        first_missing_ordinal: base.first_missing_ordinal.map(|ordinal| ordinal.0),
    })
}

fn query_coverage(
    coverage: &HistoryCoverage,
    filesystem: TurnFilesystemCoverage,
) -> PatchHistoryQueryCoverage {
    PatchHistoryQueryCoverage {
        exactness: exactness_view(
            &pioneer_tools::apply_patch::history::TurnDiffExactness::from_coverage(
                coverage.exact,
                &coverage.coverage,
            ),
        ),
        exact: coverage.exact,
        coverage: coverage_view(&coverage.coverage),
        filesystem: filesystem_coverage_view(&filesystem),
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
        filesystem: TurnFilesystemCoverageView::Incomplete {
            reason: "provider aggregate history does not prove turn-wide filesystem completeness"
                .to_owned(),
            sources: Vec::new(),
        },
        first_missing_ordinal: None,
    }
}

async fn patch_history_execution_scope(
    crud_store: &pioneer_crud::CrudStore,
    presented_thread_id: &str,
) -> Result<Vec<PatchHistoryExecutionScope>> {
    let mut thread_ids = vec![presented_thread_id.to_owned()];
    let mut cursor = 0_usize;
    while cursor < thread_ids.len() {
        let parent = thread_ids[cursor].clone();
        cursor += 1;
        for lineage in crud_store
            .list_task_thread_lineage_for_parent(parent.as_str())
            .await?
        {
            if !thread_ids.contains(&lineage.child_thread_id) {
                if thread_ids.len() >= usize::try_from(MAX_HISTORY_EXECUTION_THREADS).unwrap_or(256)
                {
                    bail!("patch history execution lineage exceeds its bounded scope");
                }
                thread_ids.push(lineage.child_thread_id);
            }
        }
    }

    let mut scope = Vec::with_capacity(thread_ids.len());
    for thread_id in thread_ids {
        let binding = crud_store
            .get_task_run_thread_binding_by_thread(thread_id.as_str())
            .await?;
        scope.push(PatchHistoryExecutionScope {
            thread_id,
            execution_id: binding
                .as_ref()
                .and_then(|binding| binding.execution_id.clone()),
            run_id: binding.map(|binding| binding.run_id),
            presented_thread_id: presented_thread_id.to_owned(),
        });
    }
    Ok(scope)
}

fn direct_execution_context(thread_id: &str, turn_id: &str) -> PatchHistoryExecutionContext {
    PatchHistoryExecutionContext {
        source_thread_id: thread_id.to_owned(),
        source_turn_id: turn_id.to_owned(),
        execution_id: None,
        run_id: None,
        presented_thread_id: thread_id.to_owned(),
    }
}

fn execution_context(
    scope: &PatchHistoryExecutionScope,
    turn_id: &str,
) -> PatchHistoryExecutionContext {
    PatchHistoryExecutionContext {
        source_thread_id: scope.thread_id.clone(),
        source_turn_id: turn_id.to_owned(),
        execution_id: scope.execution_id.clone(),
        run_id: scope.run_id.clone(),
        presented_thread_id: scope.presented_thread_id.clone(),
    }
}

async fn turn_filesystem_coverage(
    store: &SqliteTurnDiffStore,
    thread_id: &str,
    turn_id: &str,
) -> Result<TurnFilesystemCoverage> {
    Ok(store
        .get(thread_id, turn_id)
        .await?
        .map(|state| state.filesystem_coverage)
        .unwrap_or_default())
}

async fn filesystem_coverage_for_threads(
    store: &SqliteTurnDiffStore,
    thread_ids: &[String],
) -> Result<TurnFilesystemCoverage> {
    let states = store.list_for_threads(thread_ids).await?;
    if states.is_empty() {
        return Ok(TurnFilesystemCoverage::default());
    }
    let mut pending = false;
    let mut incomplete = false;
    let mut sources = Vec::new();
    for state in states {
        match state.filesystem_coverage {
            TurnFilesystemCoverage::Pending => pending = true,
            TurnFilesystemCoverage::Complete => {}
            TurnFilesystemCoverage::Incomplete {
                sources: turn_sources,
                ..
            } => {
                incomplete = true;
                sources.extend(turn_sources);
            }
        }
    }
    if incomplete {
        sources.sort_by(|left, right| {
            left.item_id
                .cmp(&right.item_id)
                .then_with(|| left.tool_name.cmp(&right.tool_name))
        });
        sources.dedup_by(|left, right| {
            left.item_id == right.item_id && left.tool_name == right.tool_name
        });
        sources.truncate(256);
        Ok(TurnFilesystemCoverage::Incomplete {
            reason: "one or more execution turns used filesystem-capable tools outside Apply Patch history"
                .to_owned(),
            sources,
        })
    } else if pending {
        Ok(TurnFilesystemCoverage::Pending)
    } else {
        Ok(TurnFilesystemCoverage::Complete)
    }
}

fn filesystem_coverage_view(coverage: &TurnFilesystemCoverage) -> TurnFilesystemCoverageView {
    match coverage {
        TurnFilesystemCoverage::Pending => TurnFilesystemCoverageView::Pending,
        TurnFilesystemCoverage::Complete => TurnFilesystemCoverageView::Complete,
        TurnFilesystemCoverage::Incomplete { reason, sources } => {
            TurnFilesystemCoverageView::Incomplete {
                reason: reason.clone(),
                sources: sources
                    .iter()
                    .map(|source| PatchFilesystemMutationSourceView {
                        item_id: source.item_id.clone(),
                        tool_name: source.tool_name.clone(),
                        reason: source.reason.clone(),
                    })
                    .collect(),
            }
        }
    }
}

async fn patch_history_path_context(
    crud_store: &pioneer_crud::CrudStore,
    turn_id: &str,
) -> Result<PatchHistoryPathContext> {
    let snapshot = crud_store
        .get_turn_execution_security_snapshot(turn_id)
        .await?;
    let Some(snapshot) = snapshot else {
        return Ok(PatchHistoryPathContext {
            workspace_root: PathBuf::from("/"),
            allowed_roots: vec![PathBuf::from("/")],
        });
    };
    let cwd = PathBuf::from(snapshot.snapshot.sandbox.cwd.as_str());
    let mut allowed_roots = snapshot
        .snapshot
        .sandbox
        .filesystem
        .entries
        .iter()
        .filter_map(|entry| entry.resolved_path.as_deref())
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if !allowed_roots.contains(&cwd) {
        allowed_roots.push(cwd.clone());
    }
    allowed_roots.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    allowed_roots.dedup();
    Ok(PatchHistoryPathContext {
        workspace_root: cwd,
        allowed_roots,
    })
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn path_view(path: &str, context: &PatchHistoryPathContext) -> PatchHistoryPathView {
    let stored = Path::new(path);
    let absolute = if stored.is_absolute() {
        normalize_lexical(stored)
    } else {
        normalize_lexical(context.workspace_root.join(stored).as_path())
    };
    let workspace_root = context
        .allowed_roots
        .iter()
        .find(|root| absolute.starts_with(root))
        .cloned()
        .unwrap_or_else(|| context.workspace_root.clone());
    let relative = absolute
        .strip_prefix(workspace_root.as_path())
        .map(Path::to_path_buf)
        .unwrap_or_else(|_| stored.to_path_buf());
    PatchHistoryPathView {
        relative_path: relative.to_string_lossy().into_owned(),
        workspace_root: workspace_root.to_string_lossy().into_owned(),
        absolute_path: absolute.to_string_lossy().into_owned(),
    }
}

fn record_view(
    stored: &StoredPatchRecord,
    path_context: &PatchHistoryPathContext,
) -> PatchHistoryRecord {
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
        changes: record
            .changes
            .iter()
            .map(|change| change_view(change, path_context))
            .collect(),
        side_effects: side_effects_view(&record.side_effects),
    }
}

fn change_view(
    change: &pioneer_tools::apply_patch::history::DurablePatchChange,
    path_context: &PatchHistoryPathContext,
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
        source_location: path_view(change.source_path.as_str(), path_context),
        destination_path: change.destination_path.clone(),
        destination_location: change
            .destination_path
            .as_deref()
            .map(|path| path_view(path, path_context)),
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
