use super::*;
use crate::authorization::AuthorizedThread;
use pioneer_protocol::{
    ExecutionObservabilityParams as Params, ExecutionObservabilityResponse as Response,
    ExecutionObservationRecord as Record, ExecutionObservationView as View,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

// A read may replace a retained interpretation only with a newer versioned
// source. Fingerprints identify unversioned evidence for explicit reconciliation;
// a changed hash does not establish time order. All hashing is outside DB capacity.
fn overlay_live_usage(facts: &Value, usage: &Value) -> bool {
    let included = facts["usage_snapshot_revision"]
        .as_i64()
        .or_else(|| facts["usage"]["_pioneer_observation"]["revision"].as_i64());
    let incoming = usage["_pioneer_observation"]["revision"].as_i64();
    match (included, incoming) {
        (old, Some(new)) => old.is_none_or(|old| new > old),
        _ => false,
    }
}
fn source_fingerprint(json: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(json.as_bytes()))
}

fn matches(row: &pioneer_entity::execution_observation::Model, p: &Params) -> bool {
    p.turn_id
        .as_ref()
        .is_none_or(|v| row.turn_id.as_ref() == Some(v))
        && p.task_id
            .as_ref()
            .is_none_or(|v| row.task_id.as_ref() == Some(v))
        && p.execution_id
            .as_ref()
            .is_none_or(|v| row.execution_id.as_ref() == Some(v))
        && p.operation_id.as_ref().is_none_or(|v| {
            &row.operation_id == v
                || (matches!(p.view, View::Contexts) && row.parent_operation_id.as_ref() == Some(v))
        })
        && p.parent_operation_id
            .as_ref()
            .is_none_or(|v| row.parent_operation_id.as_ref() == Some(v))
        && p.provider
            .as_ref()
            .is_none_or(|v| row.provider.as_ref() == Some(v))
        && p.model
            .as_ref()
            .is_none_or(|v| row.model.as_ref() == Some(v))
        && p.purpose.as_ref().is_none_or(|v| &row.purpose == v)
        && p.from_wall_ms
            .is_none_or(|from| row.start_wall_ms.is_some_and(|at| at >= from))
        && p.before_wall_ms
            .is_none_or(|before| row.start_wall_ms.is_some_and(|at| at < before))
}
struct ObservedSpan {
    parent: Option<String>,
    process: Option<String>,
    start: i64,
    end: i64,
}
#[derive(Default)]
struct Summary {
    attempts: u64,
    external_snapshots: u64,
    tool_invocations: u64,
    unmetered_tool_invocations: u64,
    usage_states: BTreeMap<String, u64>,
    unknown: u64,
    partial: u64,
    amount_groups_overflow: u64,
    totals: BTreeMap<(String, String), String>,
    breakdown: BTreeMap<(String, String, String, String, String), String>,
    tokens: BTreeMap<&'static str, (u128, u64)>,
    cache_read: u128,
    corresponding_input: u128,
    cache_known_attempts: u64,
    summed_observation_ms: u128,
    timed: u64,
    busy: BTreeMap<i64, i64>,
    union_overflow: bool,
    clock_anomalies: u64,
    running_or_untimed: u64,
    measured_work_ns: u128,
    live_revision: sha2::Sha256,
    spans: BTreeMap<String, ObservedSpan>,
    spans_overflow: bool,
    phase_work_ns: BTreeMap<String, u128>,
    omitted_phase_groups: u64,
}
impl Summary {
    fn observe(&mut self, row: &pioneer_entity::execution_observation::Model, facts: &Value) {
        use sha2::Digest;
        for phase in facts["phase_intervals"].as_array().into_iter().flatten() {
            if let (Some(kind), Some(elapsed)) =
                (phase["phase"].as_str(), phase["elapsed_ns"].as_u64())
            {
                if self.phase_work_ns.len() < 128 || self.phase_work_ns.contains_key(kind) {
                    *self.phase_work_ns.entry(kind.to_owned()).or_default() += u128::from(elapsed);
                } else {
                    self.omitted_phase_groups += 1;
                }
            }
        }
        if let Some(revision) = facts["live_usage_revision"].as_i64() {
            self.live_revision
                .update((row.operation_id.len() as u64).to_le_bytes());
            self.live_revision.update(row.operation_id.as_bytes());
            self.live_revision.update(revision.to_le_bytes());
        }
        if matches!(
            row.kind.as_str(),
            "provider_attempt" | "tool_invocation" | "context_preparation"
        ) {
            if let Some(elapsed) = row.elapsed_ns.filter(|n| *n >= 0) {
                self.measured_work_ns += elapsed as u128;
            }
            match (row.start_wall_ms, row.end_wall_ms) {
                (Some(mut start), Some(mut end)) if end >= start => {
                    if self.spans.len() < 4096 {
                        self.spans.insert(
                            row.operation_id.clone(),
                            ObservedSpan {
                                parent: row.parent_operation_id.clone(),
                                process: (row.start_process_run_id == row.process_run_id)
                                    .then(|| row.process_run_id.clone())
                                    .flatten(),
                                start,
                                end,
                            },
                        );
                    } else {
                        self.spans_overflow = true;
                    }

                    if !self.union_overflow {
                        let overlapping = self
                            .busy
                            .range(..=end)
                            .filter(|(_, b)| **b >= start)
                            .map(|(a, b)| (*a, *b))
                            .collect::<Vec<_>>();
                        for (a, b) in overlapping {
                            self.busy.remove(&a);
                            start = start.min(a);
                            end = end.max(b);
                        }
                        self.busy.insert(start, end);
                        if self.busy.len() > 4096 {
                            self.union_overflow = true;
                            self.busy.clear();
                        }
                    }
                }
                (Some(_), Some(_)) => self.clock_anomalies += 1,
                _ => self.running_or_untimed += 1,
            }
        }
        if row.kind == "tool_invocation" {
            self.tool_invocations += 1;
            if facts["direct_external_charge"]["status"] == "unobserved" {
                self.unmetered_tool_invocations += 1;
            }
        }
        if row.kind == "external_usage_snapshot" {
            self.external_snapshots += 1;
        }
        if row.kind != "provider_attempt" {
            return;
        }
        self.attempts += 1;
        let usage = &facts["usage"];
        let usage_state = usage["accounting"]["usage_coverage"]["status"]
            .as_str()
            .filter(|state| matches!(*state, "missing" | "partial" | "complete" | "inconsistent"))
            .unwrap_or("legacy_or_unclassified");
        *self.usage_states.entry(usage_state.into()).or_default() += 1;
        for meter in [
            "input_tokens",
            "output_tokens",
            "uncached_input_tokens",
            "cache_read_input_tokens",
            "cache_write_input_tokens",
            "reasoning_tokens",
        ] {
            if let Some(quantity) = usage[meter].as_u64() {
                let entry = self.tokens.entry(meter).or_default();
                entry.0 += u128::from(quantity);
                entry.1 += 1;
            }
        }
        if let (Some(read), Some(input)) = (
            usage["cache_read_input_tokens"].as_u64(),
            usage["input_tokens"].as_u64(),
        ) {
            if read <= input {
                self.cache_read += u128::from(read);
                self.corresponding_input += u128::from(input);
                self.cache_known_attempts += 1;
            }
        }
        if let Some(ms) = facts["timing"]["elapsed_ms"].as_u64() {
            self.summed_observation_ms += u128::from(ms);
            self.timed += 1;
        }
        let accounting = &usage["accounting"];
        // Reported charge and catalog estimate are separate bases. No implicit
        // USD conversion and no addition of reported + estimate.
        let reported = &accounting["reported_cost"];
        let estimate = &accounting["estimated_cost"];
        let (currency, basis, amount) =
            if let Some(amount) = reported.get("amount").filter(|v| v.is_number()) {
                let currency = reported["currency"].as_str().unwrap_or("unknown");
                if currency == "unknown" {
                    self.unknown += 1;
                    // Unknown monetary units cannot be added to each other.
                    // The exact reported amount remains in scoped leaf detail.
                    return;
                }
                (
                    currency,
                    "provider_reported",
                    reported["amount_decimal"]
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| amount.to_string()),
                )
            } else if let Some(amount) = estimate["amount_decimal"].as_str() {
                ("USD", "catalog_estimate", amount.to_owned())
            } else if let Some(amount) =
                accounting["cost_resolution"]["known_subtotal_decimal"].as_str()
            {
                self.partial += 1;
                ("USD", "partial_catalog_estimate", amount.to_owned())
            } else {
                self.unknown += 1;
                return;
            };
        let scoped_basis = if currency == "credits" {
            format!(
                "{basis}:{}",
                row.provider.as_deref().unwrap_or("unknown_provider")
            )
        } else {
            basis.to_owned()
        };
        let basis = scoped_basis.as_str();
        let total_key = (currency.to_owned(), basis.to_owned());
        if self.totals.len() >= 256 && !self.totals.contains_key(&total_key) {
            // Keep the underlying monetary fact intact. A bounded read summary
            // reports the omitted group rather than merging incompatible units.
            self.amount_groups_overflow += 1;
            self.unknown += 1;
            return;
        }
        let group = (
            row.provider.clone().unwrap_or_else(|| "unknown".into()),
            row.model.clone().unwrap_or_else(|| "unknown".into()),
            row.purpose.clone(),
            currency.to_owned(),
            basis.to_owned(),
        );
        let group = if self.breakdown.len() < 4096 || self.breakdown.contains_key(&group) {
            group
        } else {
            (
                "unclassified_overflow".into(),
                "unknown".into(),
                "unknown".into(),
                currency.into(),
                basis.into(),
            )
        };
        if let Some(sum) = pioneer_provider::usage::add_decimal_amounts(
            self.breakdown
                .get(&group)
                .map(String::as_str)
                .unwrap_or("0"),
            &amount,
        ) {
            self.breakdown.insert(group, sum);
        }
        let key = (currency.to_owned(), basis.to_owned());
        let previous = self.totals.get(&key).map(String::as_str).unwrap_or("0");
        if let Some(sum) = pioneer_provider::usage::add_decimal_amounts(previous, &amount) {
            self.totals.insert(key, sum);
        } else {
            self.unknown += 1;
        }
    }
    fn value(self) -> Value {
        use sha2::Digest;
        let union_ms = if self.union_overflow {
            None
        } else {
            Some(
                self.busy
                    .iter()
                    .map(|(a, b)| i128::from(*b) - i128::from(*a))
                    .sum::<i128>()
                    .to_string(),
            )
        };
        let mut exclusive = 0i128;
        let mut missing_parent = 0u64;
        let mut cross_process = 0u64;
        let mut containment_anomalies = 0u64;
        let mut children = BTreeMap::<&str, Vec<&ObservedSpan>>::new();
        for span in self.spans.values() {
            if let Some(parent) = &span.parent {
                children.entry(parent).or_default().push(span);
            }
        }
        for (id, span) in &self.spans {
            if span
                .parent
                .as_ref()
                .is_some_and(|id| !self.spans.contains_key(id))
            {
                missing_parent += 1;
            }
            let mut intervals = Vec::new();
            for child in children.get(id.as_str()).into_iter().flatten() {
                if span.process.is_none() || span.process != child.process {
                    cross_process += 1;
                    continue;
                }
                if child.start < span.start || child.end > span.end {
                    containment_anomalies += 1;
                }
                let a = child.start.max(span.start);
                let b = child.end.min(span.end);
                if b >= a {
                    intervals.push((a, b));
                }
            }
            intervals.sort_unstable();
            let mut union = 0i128;
            let mut current: Option<(i64, i64)> = None;
            for (a, b) in intervals {
                if let Some((start, end)) = current {
                    if a <= end {
                        current = Some((start, end.max(b)));
                    } else {
                        union += i128::from(end) - i128::from(start);
                        current = Some((a, b));
                    }
                } else {
                    current = Some((a, b));
                }
            }
            if let Some((a, b)) = current {
                union += i128::from(b) - i128::from(a);
            }
            exclusive += i128::from(span.end) - i128::from(span.start) - union;
        }
        let live_revision = hex::encode(self.live_revision.finalize());
        let observed_envelope = self
            .spans
            .values()
            .map(|s| s.start)
            .min()
            .zip(self.spans.values().map(|s| s.end).max())
            .map(|(a, b)| (i128::from(b) - i128::from(a)).to_string());
        json!({"version":1,
            "observed_financial_coverage":{"status":if self.attempts==0 && self.external_snapshots==0{"missing"}else if self.unknown>0 || self.partial>0 || self.external_snapshots>0{"partial"}else{"complete"},
                "basis":"observed_unique_attempts_only_legacy_backfill_and_external_executor_coverage_separate"},
            "token_quantities":self.tokens.into_iter().map(|(meter,(known_sum,known_attempts))|json!({"meter":meter,"known_sum":known_sum.to_string(),"known_attempts":known_attempts,"total_attempts":self.attempts,"unit":"tokens"})).collect::<Vec<_>>(),
            "breakdown":self.breakdown.into_iter().map(|((provider,model,purpose,currency,basis),amount)|json!({"provider":provider,"model":model,"purpose":purpose,"currency":currency,"basis":basis,"amount_decimal":amount})).collect::<Vec<_>>(),
            "usage_coverage_counts":self.usage_states,
            "external_tool_metering":{"tool_invocations":self.tool_invocations,"unobserved_direct_charge_operations":self.unmetered_tool_invocations,
                "basis":"direct_service_evidence_only_local_duration_and_footprint_are_not_charges","included_in_provider_financial_coverage":false},
            "model_breakdown_basis":"requested_model_resolved_billing_identity_available_in_attempt_detail",
            "external_statistics":{"snapshots":self.external_snapshots,"additive":false,"coverage":"external_baseline_not_assumed"},"attempts":self.attempts,"unknown_attempts":self.unknown,
            "partial_attempts":self.partial,"amount_group_limit_exceeded_operations":self.amount_groups_overflow,"own_amounts":self.totals.into_iter().map(|((currency,basis),amount)|
                json!({"currency":currency,"basis":basis,"amount_decimal":amount})).collect::<Vec<_>>(),
            "inclusive_amounts":null,"inclusive_coverage":"descendant_traversal_unavailable",
            "cache_read_tokens_known":self.cache_read.to_string(),"corresponding_input_tokens_known":self.corresponding_input.to_string(),
            "cache_ratio_known_attempts":self.cache_known_attempts,"cache_ratio_basis":"sum_known_read_over_sum_corresponding_input",
            "time":{"summed_observation_ms":self.summed_observation_ms.to_string(),"timed_attempts":self.timed,
                "wall_ms":null,"observed_union_busy_ms":union_ms,
                "observed_envelope_wall_ms":observed_envelope,"envelope_basis":"earliest_observed_start_to_latest_observed_end_including_gaps_not_active_inference",
                "summed_observed_exclusive_wall_ms":(!self.spans_overflow).then(||exclusive.to_string()),
                "exclusive_basis":"parent_wall_minus_union_of_observed_same_process_children_partial_graph",
                "missing_parents":missing_parent,"cross_process_children_unsubtracted":cross_process,"containment_anomalies":containment_anomalies,"span_limit_exceeded":self.spans_overflow,
                "measured_summed_work_ns":self.measured_work_ns.to_string(),"clock_anomalies":self.clock_anomalies,
                "phase_inclusive_work_ns":self.phase_work_ns.into_iter().map(|(phase,ns)|json!({"phase":phase,"elapsed_ns":ns.to_string(),"additive_across_phases":false})).collect::<Vec<_>>(),
                "omitted_phase_group_observations":self.omitted_phase_groups,
                "running_or_untimed_operations":self.running_or_untimed,"interval_union_limit_exceeded":self.union_overflow,
                "basis":"observed_wall_union_and_source_monotonic_work_not_inference_time"},
            "live_usage_revision_fingerprint":live_revision,"inference_time_ms":null,
            "allocation":{"method":"none","version":1,"basis":"no_source_token_or_cache_offsets","all_cost_unallocated":true}})
    }
}
impl MessageProcessor {
    // Return the heap boundary from the handler itself. Boxing an async fn at
    // the dispatch call site still constructs and moves its full state machine
    // in the shared dispatch poll frame, including for unrelated task RPCs.
    pub(super) fn workspace_execution_observability<'a>(
        &'a self,
        context: &'a RequestContext,
        proof: &'a crate::authorization::AuthorizedWorkspace,
        id: RequestId,
        p: pioneer_protocol::WorkspaceExecutionObservabilityParams,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            let result:anyhow::Result<pioneer_protocol::WorkspaceExecutionObservabilityResponse>=async {
            anyhow::ensure!(p.workspace_id==proof.workspace_id(),"workspace scope mismatch");
            for value in [&p.operation_id,&p.parent_operation_id].into_iter().flatten() {
                anyhow::ensure!(!value.is_empty() && value.len()<=256,"invalid scope filter");
            }
            let limit=p.limit.unwrap_or(50);anyhow::ensure!((1..=100).contains(&limit),"invalid limit");
            let maximum=self.crud_store.execution_workspace_frontier(proof.workspace_id()).await?;
            let through=p.through.unwrap_or(maximum);let mut after=p.after.unwrap_or(0);
            anyhow::ensure!(after>=0 && through>=after && through<=maximum,"invalid frontier");
            let summary_view=matches!(p.view,View::Summary);
            anyhow::ensure!(!summary_view || after==0,"summary requires initial cursor");
            anyhow::ensure!(!matches!(p.view,View::Attempt) || p.operation_id.is_some(),"operation id required");
            let mut summary=Summary::default();let mut records=Vec::new();let mut next_after=None;
            loop {
                let rows=if matches!(p.view,View::Attempt) {
                    self.crud_store.execution_operation_at_frontier(proof.workspace_id(),None,p.operation_id.as_deref().expect("validated operation"),through).await?.into_iter().filter(|r|r.sequence>after).collect()
                } else {self.crud_store.workspace_owned_execution_observation_page(proof.workspace_id(),after,through,limit).await?};
                let count=rows.len();
                for (row,mut facts) in self.observation_facts(proof.workspace_id(),"",rows).await? {
                    after=row.sequence;
                    if p.operation_id.as_ref().is_some_and(|op|op!=&row.operation_id && !(matches!(p.view,View::Contexts) && row.parent_operation_id.as_ref()==Some(op))){continue;}
                    if p.parent_operation_id.as_ref().is_some_and(|op|row.parent_operation_id.as_ref()!=Some(op)){continue;}
                    if summary_view {summary.observe(&row,&facts);continue;}
                    if matches!(p.view,View::Contexts) {
                        let Some(manifest)=facts.get("context_manifest").filter(|v|!v.is_null())else{continue;};
                        facts=json!({"context_manifest":manifest,"breakdown":context_breakdown(manifest,facts["usage"]["input_tokens"].as_u64())});
                    }
                    let tariff=if matches!(p.view,View::Attempt) {
                        if let Some(hash)=&row.tariff_hash {self.crud_store.execution_tariff(proof.workspace_id(),hash).await?
                            .map(|r|serde_json::from_str(&r.evidence_json)).transpose()?}else{None}
                    }else{None};
                    if matches!(p.view,View::Attempt){facts=self.attempt_children(proof.workspace_id(),None,&row.operation_id,through,facts).await?;}
                    records.push(observation_record(row,facts,tariff));
                }
                if count<limit as usize || after>=through || matches!(p.view,View::Attempt) {break;}
                if !summary_view {next_after=Some(after);break;}
                tokio::task::yield_now().await;
            }
            let legacy=self.crud_store.execution_observation_backfill_progress().await?;
            let auxiliary=self.crud_store.execution_aux_observation_backfill_progress().await?;
            Ok(pioneer_protocol::WorkspaceExecutionObservabilityResponse {workspace_id:proof.workspace_id().into(),through,next_after,records,
                summary:summary_view.then(||summary.value()),coverage:json!({"scope":"verified_workspace_owned_operations_only_no_thread_data",
                    "legacy_backfill":{"complete":legacy.complete && auxiliary.complete,"main_complete":legacy.complete,"auxiliary_complete":auxiliary.complete,"basis":"global_migration_status_no_cross_scope_counts"},
                    "frontier_basis":"immutable_metadata_revisions_live_usage_explicitly_revisioned","full_prompts_exported":false,
                    "query_scope":{"view":p.view,"operation_id":p.operation_id,"parent_operation_id":p.parent_operation_id},
                    "interpretation_versions":{"calculator":1,"normalizer":1,"resolver":1,"manifest":1}})})
        }.await;
            match result {
                Ok(value) => match JsonRpcResponse::from_result(id.clone(), &value) {
                    Ok(response) => {
                        let _ = self.send_json(context.connection_id(), &response).await;
                    }
                    Err(_) => {
                        self.send_error(
                            context.connection_id(),
                            JsonRpcErrorResponse::new(
                                Some(id),
                                INVALID_PARAMS_CODE,
                                "workspace observation serialization unavailable",
                            ),
                        )
                        .await
                    }
                },
                Err(_) => {
                    self.send_error(
                        context.connection_id(),
                        JsonRpcErrorResponse::new(
                            Some(id),
                            INVALID_PARAMS_CODE,
                            "workspace observation unavailable",
                        ),
                    )
                    .await
                }
            }
        })
    }
    pub(super) fn execution_observability<'a>(
        &'a self,
        context: &'a RequestContext,
        proof: &'a AuthorizedThread,
        id: RequestId,
        params: Params,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            if params.thread_id.trim() != proof.thread_id() {
                self.send_error(
                    context.connection_id(),
                    crate::authorization::AuthorizationExternalError::NotFound.response(id),
                )
                .await;
                return;
            }
            let result = self
                .read_execution_observability(context, proof, &params)
                .await;
            match result.and_then(|value| {
                JsonRpcResponse::from_result(id.clone(), &value).map_err(Into::into)
            }) {
                Ok(response) => {
                    let _ = self.send_json(context.connection_id(), &response).await;
                }
                Err(_) => {
                    self.send_error(
                        context.connection_id(),
                        crate::public_error::agent_rpc_error(
                            Some(id),
                            INVALID_PARAMS_CODE,
                            pioneer_protocol::PublicErrorCode::InvalidInput,
                            pioneer_protocol::PublicErrorStage::Observation,
                            "execution observability read unavailable",
                        ),
                    )
                    .await
                }
            }
        })
    }
    pub(super) fn execution_reconcile<'a>(
        &'a self,
        context: &'a RequestContext,
        proof: &'a AuthorizedThread,
        id: RequestId,
        p: pioneer_protocol::ExecutionReconcileParams,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            if p.thread_id.trim() != proof.thread_id() {
                self.send_error(
                    context.connection_id(),
                    crate::authorization::AuthorizationExternalError::NotFound.response(id),
                )
                .await;
                return;
            }
            let result = self
                .historical_reconcile(
                    proof.workspace_id(),
                    Some(proof.thread_id()),
                    &p.operation_id,
                    p.expected_sequence,
                )
                .await;
            match result
                .and_then(|v| JsonRpcResponse::from_result(id.clone(), &v).map_err(Into::into))
            {
                Ok(response) => {
                    let _ = self.send_json(context.connection_id(), &response).await;
                }
                Err(_) => {
                    self.send_error(
                        context.connection_id(),
                        crate::public_error::agent_rpc_error(
                            Some(id),
                            INVALID_PARAMS_CODE,
                            pioneer_protocol::PublicErrorCode::InvalidInput,
                            pioneer_protocol::PublicErrorStage::Observation,
                            "historical reconciliation unavailable or source changed",
                        ),
                    )
                    .await
                }
            }
        })
    }
    pub(super) fn workspace_execution_reconcile<'a>(
        &'a self,
        context: &'a RequestContext,
        proof: &'a crate::authorization::AuthorizedWorkspace,
        id: RequestId,
        p: pioneer_protocol::WorkspaceExecutionReconcileParams,
    ) -> MessageFuture<'a, ()> {
        message_future(async move {
            let result = if p.workspace_id == proof.workspace_id() {
                self.historical_reconcile(
                    proof.workspace_id(),
                    None,
                    &p.operation_id,
                    p.expected_sequence,
                )
                .await
            } else {
                Err(anyhow::anyhow!("workspace scope mismatch"))
            };
            match result.and_then(|value| {
                JsonRpcResponse::from_result(id.clone(), &value).map_err(Into::into)
            }) {
                Ok(response) => {
                    let _ = self.send_json(context.connection_id(), &response).await;
                }
                Err(_) => {
                    self.send_error(
                        context.connection_id(),
                        JsonRpcErrorResponse::new(
                            Some(id),
                            INVALID_PARAMS_CODE,
                            "workspace reconciliation unavailable or source changed",
                        ),
                    )
                    .await
                }
            }
        })
    }
    async fn historical_reconcile(
        &self,
        workspace: &str,
        thread: Option<&str>,
        operation: &str,
        expected_sequence: i64,
    ) -> anyhow::Result<Value> {
        let store = self.crud_store.with_maintenance_access();
        let row = if let Some(thread) = thread {
            store
                .execution_observation_latest(workspace, thread, operation)
                .await?
        } else {
            store
                .workspace_owned_execution_observation_latest(workspace, operation)
                .await?
        }
        .ok_or_else(|| anyhow::anyhow!("operation unavailable"))?;
        anyhow::ensure!(
            row.sequence == expected_sequence && row.kind == "provider_attempt",
            "reconciliation source changed"
        );
        let mut facts: Value = serde_json::from_str(&row.facts_json)?;
        let mut expected_live = None;
        let ledger_id = row
            .operation_id
            .strip_prefix("provider:journal:")
            .map(str::to_owned)
            .or_else(|| {
                row.turn_id
                    .as_ref()
                    .and_then(|turn| row.operation_id.strip_prefix(&format!("provider:{turn}:")))
                    .map(str::to_owned)
            });
        if let Some(ledger_id) = &ledger_id {
            let live = store
                // IDs come exclusively from this authorized immutable fact.
                // A nested call may retain a workspace journal owner while its
                // verified causal parent attributes it to this thread.
                .execution_scoped_live_usage(workspace, None, vec![ledger_id.clone()])
                .await?;
            if let Some(current) = live.into_iter().next() {
                expected_live = Some(current.usage_json.clone());
                let latest: Value = serde_json::from_str(&current.usage_json)?;
                let included = facts["usage_snapshot_revision"]
                    .as_i64()
                    .or_else(|| facts["usage"]["_pioneer_observation"]["revision"].as_i64());
                let hash = source_fingerprint(&current.usage_json);
                let incoming = latest["_pioneer_observation"]["revision"].as_i64();
                // An explicit reconciliation may bind an older unversioned fact
                // to the exact current journal. This is a CAS source selection,
                // not a read-time claim that unknown revision means newer.
                if overlay_live_usage(&facts, &latest)
                    || (included.is_none()
                        && incoming.is_none()
                        && facts["usage_source_fingerprint"].as_str() != Some(hash.as_str()))
                {
                    facts["usage_snapshot_revision"] =
                        latest["_pioneer_observation"]["revision"].clone();
                    let mut latest = latest;
                    if incoming.is_none() {
                        if let Some(accounting) =
                            latest.get_mut("accounting").and_then(Value::as_object_mut)
                        {
                            accounting.remove("pricing");
                        }
                    }
                    facts["usage"] = latest;
                    facts["usage_source_fingerprint"] = hash.clone().into();
                } else if included == incoming {
                    facts["usage_source_fingerprint"] = hash.into();
                }
            }
        }
        let hash = facts["usage"]["accounting"]["pricing"]["retained_evidence_hash"]
            .as_str()
            .or(row.tariff_hash.as_deref())
            .ok_or_else(|| anyhow::anyhow!("historical tariff unavailable"))?
            .to_owned();
        let tariff = store
            .execution_tariff(workspace, &hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("historical tariff unavailable"))?;
        let evidence: Value = serde_json::from_str(&tariff.evidence_json)?;
        let usage: pioneer_provider::TokenUsage = serde_json::from_value(facts["usage"].clone())?;
        let resolution = pioneer_provider::usage::resolve_retained_tariff(&usage, &evidence);
        let original: Value = serde_json::from_str(&row.facts_json)?;
        if original["usage"] == facts["usage"]
            && original["usage_source_fingerprint"] == facts["usage_source_fingerprint"]
            && facts["usage"]["accounting"]["cost_resolution"] == resolution
            && facts["usage"]["accounting"]["calculator_version"] == 1
        {
            return Ok(
                json!({"operationId":operation,"changed":false,"sourceSequence":row.sequence,"resolutionVersion":1}),
            );
        }
        facts["usage"]["accounting"]["cost_resolution"] = resolution.clone();
        facts["usage"]["accounting"]["unresolved_reasons"] = resolution["reasons"].clone();
        facts["usage"]["accounting"]["calculator_version"] = 1.into();
        facts["usage"]["accounting"]["estimated_cost"] = if resolution["status"] == "complete" {
            json!({"amount_decimal":resolution["known_subtotal_decimal"],"currency":"USD","provenance":"catalog_estimate"})
        } else {
            Value::Null
        };
        let selected = pioneer_provider::usage::select_cost_basis(
            facts["usage"]["accounting"].get("reported_cost"),
            facts["usage"]["accounting"].get("estimated_cost"),
        );
        facts["usage"]["accounting"]["selected_cost"] = serde_json::to_value(selected)?;
        facts["usage"]["accounting"]["price_status"] = if resolution["status"] == "complete" {
            "catalog_estimate"
        } else {
            "unknown_or_conditional_price"
        }
        .into();
        facts["historical_reconciliation"] = json!({"version":1,"source_sequence":row.sequence,"basis":"retained_tariff_only",
                "usage_source_fingerprint":facts["usage_source_fingerprint"],"source_revision":facts["usage_snapshot_revision"],
                "source_selection":"exact_journal_cas_or_retained_versioned_source_no_wall_clock_order"});
        let prepared = pioneer_crud::ExecutionObservationInput {
            workspace_id: row.workspace_id,
            operation_id: row.operation_id,
            revision: row
                .revision
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("resolution revision exhausted"))?,
            thread_id: row.thread_id,
            turn_id: row.turn_id,
            owner_kind: row.owner_kind,
            owner_id: row.owner_id,
            parent_operation_id: row.parent_operation_id,
            kind: row.kind,
            purpose: row.purpose,
            provider: row.provider,
            model: row.model,
            status: row.status,
            recorded_at_ms: chrono::Utc::now().timestamp_millis(),
            facts,
            tariff_evidence: Some(json!({"retained_evidence_hash":hash})),
        }
        .prepare()?
        .expecting_previous_sequence(row.sequence);
        let prepared = if let Some(ledger_id) = ledger_id {
            prepared.expecting_live_usage(ledger_id, expected_live)
        } else {
            prepared
        };
        store.record_execution_observation(prepared).await?;
        Ok(
            json!({"operationId":operation,"changed":true,"sourceSequence":row.sequence,"resolutionVersion":1}),
        )
    }
    async fn attempt_children(
        &self,
        workspace: &str,
        thread: Option<&str>,
        operation: &str,
        through: i64,
        mut facts: Value,
    ) -> anyhow::Result<Value> {
        let rows = self
            .crud_store
            .execution_children_page(workspace, thread, operation, 0, through, 32)
            .await?;
        let count = rows.len();
        let next = rows.last().map(|row| row.sequence);
        let mut sent = false;
        let mut intent = false;
        let mut children = Vec::new();
        for row in rows {
            let detail: Value = serde_json::from_str(&row.facts_json)?;
            sent |= detail["request_sent"] == true;
            intent |= detail["phase"] == "dispatch_intent";
            children.push(observation_record(row, detail, None));
        }
        facts["dispatch_coverage"] = json!({"state":if sent{"response_confirms_dispatch"}else if intent{"send_unknown"}else{"unobserved_or_preparing"},
            "detail_limit":32,"children_may_have_more":count==32,"children_after":(count==32).then_some(next).flatten(),"parent_operation_id":operation,"continuation_view":"timeline_with_parent_operation_id_filter","through":through});
        facts["children"] = serde_json::to_value(children)?;
        Ok(facts)
    }
    async fn observation_facts(
        &self,
        workspace: &str,
        _thread: &str,
        rows: Vec<pioneer_entity::execution_observation::Model>,
    ) -> anyhow::Result<Vec<(pioneer_entity::execution_observation::Model, Value)>> {
        fn journal_id(row: &pioneer_entity::execution_observation::Model) -> Option<String> {
            if row.kind != "provider_attempt" {
                return None;
            }
            row.operation_id
                .strip_prefix("provider:journal:")
                .map(str::to_owned)
                .or_else(|| {
                    row.turn_id
                        .as_ref()
                        .and_then(|turn| {
                            row.operation_id.strip_prefix(&format!("provider:{turn}:"))
                        })
                        .map(str::to_owned)
                })
        }
        let ids = rows.iter().filter_map(journal_id).collect();
        let live = self
            .crud_store
            .execution_scoped_live_usage(workspace, None, ids)
            .await?;
        let mut latest = BTreeMap::new();
        for row in live {
            latest.insert(
                row.id,
                (
                    serde_json::from_str::<Value>(&row.usage_json)?,
                    row.updated_at,
                    source_fingerprint(&row.usage_json),
                ),
            );
        }
        let mut out = Vec::with_capacity(rows.len());
        for mut row in rows {
            let mut facts: Value = serde_json::from_str(&row.facts_json)?;
            if row.kind == "tool_invocation" {
                facts["phase_intervals"] = tool_phase_intervals(&facts["observation"]);
            }
            if matches!(row.status.as_str(), "started" | "running")
                && row.process_run_id.as_ref().is_some_and(|run| {
                    run != &pioneer_observability::identity::run_identity().run_id
                })
            {
                facts["recovery"] = json!({"source_status":row.status,"state":"previous_process_unconfirmed","terminal_confirmed":false,"send_state":"send_unknown_without_dispatch_evidence"});
            }

            if let Some((usage, updated, hash)) = journal_id(&row).and_then(|id| latest.get(&id)) {
                if !overlay_live_usage(&facts, usage) {
                    facts["usage_overlay_coverage"] =
                        if facts["usage_source_fingerprint"].as_str() == Some(hash.as_str()) {
                            "retained_interpretation_same_exact_source"
                        } else if usage["_pioneer_observation"]["revision"].is_null()
                            && facts["usage_source_fingerprint"].is_string()
                        {
                            facts["changed_unversioned_source_fingerprint"] = hash.clone().into();
                            "unversioned_source_changed_reconciliation_required_order_unconfirmed"
                        } else {
                            "retained_interpretation_no_proven_newer_source"
                        }
                        .into();
                    out.push((row, facts));
                    continue;
                }
                facts["initial_tariff_hash"] = serde_json::json!(row.tariff_hash);
                if let Some(hash) =
                    usage["accounting"]["pricing"]["retained_evidence_hash"].as_str()
                {
                    row.tariff_hash = Some(hash.into());
                }
                facts["usage"] = usage.clone();
                facts["live_usage_source_fingerprint"] = hash.clone().into();
                facts["live_usage_revision"] = usage["_pioneer_observation"]["revision"].clone();
                facts["live_usage_updated_at_unix_seconds"] = serde_json::json!(updated);
                facts["usage_read_basis"] =
                    "latest_durable_cumulative_not_frozen_at_frontier".into();
            }
            out.push((row, facts));
        }
        Ok(out)
    }
    async fn read_execution_observability(
        &self,
        context: &RequestContext,
        proof: &AuthorizedThread,
        p: &Params,
    ) -> anyhow::Result<Response> {
        let limit = p.limit.unwrap_or(50);
        for value in [
            &p.turn_id,
            &p.task_id,
            &p.execution_id,
            &p.operation_id,
            &p.parent_operation_id,
            &p.provider,
            &p.model,
            &p.purpose,
        ]
        .into_iter()
        .flatten()
        {
            anyhow::ensure!(
                !value.is_empty() && value.len() <= 256,
                "invalid scope filter"
            );
        }
        anyhow::ensure!((1..=100).contains(&limit), "invalid limit");
        let maximum = self
            .crud_store
            .execution_workspace_frontier(proof.workspace_id())
            .await?;
        let through = p.through.unwrap_or(maximum);
        let mut after = p.after.unwrap_or(0);
        anyhow::ensure!(
            after >= 0 && through >= after && through <= maximum,
            "invalid frontier"
        );
        anyhow::ensure!(
            p.from_wall_ms
                .zip(p.before_wall_ms)
                .is_none_or(|(a, b)| a < b),
            "invalid period"
        );
        let summary_view = matches!(p.view, View::Summary);
        anyhow::ensure!(
            !summary_view || after == 0,
            "summary requires initial cursor"
        );
        if matches!(p.view, View::Attempt) {
            anyhow::ensure!(p.operation_id.is_some(), "operation id required");
        }
        let mut summary = Summary::default();
        let mut records = Vec::new();
        let mut next_after = None;
        let mut owner_turns = std::collections::BTreeSet::new();
        let mut owner_turns_omitted = false;
        let mut unplaced_period_operations = 0u64;
        loop {
            let rows = if matches!(p.view, View::Attempt) {
                self.crud_store
                    .execution_operation_at_frontier(
                        proof.workspace_id(),
                        Some(proof.thread_id()),
                        p.operation_id.as_deref().expect("validated operation"),
                        through,
                    )
                    .await?
                    .into_iter()
                    .filter(|r| r.sequence > after)
                    .collect()
            } else {
                self.crud_store
                    .execution_observation_page(
                        proof.workspace_id(),
                        proof.thread_id(),
                        after,
                        through,
                        limit,
                        !p.include_revisions.unwrap_or(false) || summary_view,
                    )
                    .await?
            };
            let count = rows.len();
            for (row, facts) in self
                .observation_facts(proof.workspace_id(), proof.thread_id(), rows)
                .await?
            {
                after = row.sequence;
                if row.start_wall_ms.is_none()
                    && (p.from_wall_ms.is_some() || p.before_wall_ms.is_some())
                {
                    let mut without_period = p.clone();
                    without_period.from_wall_ms = None;
                    without_period.before_wall_ms = None;
                    if matches(&row, &without_period) {
                        unplaced_period_operations += 1;
                    }
                }
                if !matches(&row, p) {
                    continue;
                }
                if let Some(turn) = &row.turn_id {
                    if owner_turns.len() < 100 || owner_turns.contains(turn) {
                        owner_turns.insert(turn.clone());
                    } else {
                        owner_turns_omitted = true;
                    }
                }
                if summary_view {
                    summary.observe(&row, &facts);
                    continue;
                }
                let mut facts = if matches!(p.view, View::Contexts) {
                    let manifest = facts.get("context_manifest").filter(|v| !v.is_null());
                    let Some(manifest) = manifest else {
                        continue;
                    };
                    json!({"context_manifest":manifest,"breakdown":context_breakdown(manifest,facts["usage"]["input_tokens"].as_u64())})
                } else {
                    facts
                };
                // Tariffs only through an authorized operation reference. No
                // unscoped hash lookup RPC exposes another thread's evidence.
                let tariff_evidence = if matches!(p.view, View::Attempt) {
                    if let Some(hash) = &row.tariff_hash {
                        self.crud_store
                            .execution_tariff(proof.workspace_id(), hash)
                            .await?
                            .map(|v| serde_json::from_str(&v.evidence_json))
                            .transpose()?
                    } else {
                        None
                    }
                } else {
                    None
                };
                if matches!(p.view, View::Attempt) {
                    facts = self
                        .attempt_children(
                            proof.workspace_id(),
                            Some(proof.thread_id()),
                            &row.operation_id,
                            through,
                            facts,
                        )
                        .await?;
                }
                records.push(observation_record(row, facts, tariff_evidence));
            }
            if count < limit as usize || after >= through || matches!(p.view, View::Attempt) {
                break;
            }
            if !summary_view {
                next_after = Some(after);
                break;
            }
            // Bounded read released before JSON parsing, aggregation or yielding.
            tokio::task::yield_now().await;
        }
        let mut summary_value = summary_view.then(|| summary.value());
        if summary_view && p.include_descendants.unwrap_or(false) {
            use crate::authorization::{
                AuthorizationResolver, AuthorizationService, ProofResolution, ResourceAction,
            };
            let action = ResourceAction::ThreadRead;
            let gate = AuthorizationService::new().authorize_action(
                context.principal().kind,
                context.principal().role_key.as_ref(),
                action,
            );
            let resolver = AuthorizationResolver::new(self.crud_store.as_ref().clone());
            let mut pending = std::collections::VecDeque::from([proof.thread_id().to_owned()]);
            let mut visited = std::collections::HashSet::from([proof.thread_id().to_owned()]);
            let mut inclusive = Summary::default();
            let mut graph_partial = false;
            let mut task_ancestry = BTreeMap::<String, Option<bool>>::new();
            let mut descendant_unplaced_period_operations = 0u64;
            let mut descendant_unknown_task_operations = 0u64;

            while let Some(parent) = pending.pop_front() {
                let mut child_after = None;
                loop {
                    let edges = self
                        .crud_store
                        .list_thread_child_lineage_page(&parent, child_after.as_deref(), 100)
                        .await?;
                    let count = edges.len();
                    for edge in edges {
                        child_after = Some(edge.child_thread_id.clone());
                        if parent == proof.thread_id()
                            && p.turn_id
                                .as_ref()
                                .is_some_and(|turn| edge.created_by_turn_id.as_ref() != Some(turn))
                        {
                            continue;
                        }
                        if parent == proof.thread_id() {
                            if let Some(execution) = &p.execution_id {
                                let Some(turn) = &edge.created_by_turn_id else {
                                    graph_partial = true;
                                    continue;
                                };
                                if !self
                                    .crud_store
                                    .execution_turn_owned_by(
                                        proof.workspace_id(),
                                        proof.thread_id(),
                                        turn,
                                        execution,
                                    )
                                    .await?
                                {
                                    continue;
                                }
                            }
                        }
                        if visited.len() >= 1024 {
                            graph_partial = true;
                            continue;
                        }
                        if !visited.insert(edge.child_thread_id.clone()) {
                            continue;
                        }
                        let child = &edge.child_thread_id;
                        let mut allowed = resolver
                            .authorize_thread(
                                context.principal(),
                                &gate,
                                action,
                                child,
                                Some(proof.workspace_id()),
                            )
                            .await?;
                        if !matches!(allowed, ProofResolution::Authorized(_)) {
                            allowed = resolver
                                .authorize_internal_thread_via_root(
                                    context.principal(),
                                    &gate,
                                    action,
                                    child,
                                    Some(proof.workspace_id()),
                                )
                                .await?;
                        }
                        if !matches!(allowed, ProofResolution::Authorized(_)) {
                            graph_partial = true;
                            continue;
                        }
                        pending.push_back(child.clone());
                        let mut cursor = 0;
                        loop {
                            let rows = self
                                .crud_store
                                .execution_observation_page(
                                    proof.workspace_id(),
                                    child,
                                    cursor,
                                    through,
                                    100,
                                    true,
                                )
                                .await?;
                            let count = rows.len();
                            for (row, facts) in self
                                .observation_facts(proof.workspace_id(), child, rows)
                                .await?
                            {
                                cursor = row.sequence;
                                let mut child_params = p.clone();
                                child_params.turn_id = None;
                                child_params.execution_id = None;
                                if let Some(task) = &p.task_id {
                                    let Some(candidate) = &row.task_id else {
                                        descendant_unknown_task_operations += 1;
                                        graph_partial = true;
                                        continue;
                                    };
                                    let membership =
                                        if let Some(value) = task_ancestry.get(candidate) {
                                            *value
                                        } else {
                                            let value = self
                                                .crud_store
                                                .execution_task_ancestry_matches(
                                                    proof.workspace_id(),
                                                    candidate,
                                                    task,
                                                )
                                                .await?;
                                            if task_ancestry.len() < 4096 {
                                                task_ancestry.insert(candidate.clone(), value);
                                            } else {
                                                graph_partial = true;
                                            }
                                            value
                                        };
                                    if membership != Some(true) {
                                        graph_partial |= membership.is_none();
                                        continue;
                                    }
                                    child_params.task_id = None;
                                }
                                if row.start_wall_ms.is_none()
                                    && (child_params.from_wall_ms.is_some()
                                        || child_params.before_wall_ms.is_some())
                                {
                                    let mut without_period = child_params.clone();
                                    without_period.from_wall_ms = None;
                                    without_period.before_wall_ms = None;
                                    if matches(&row, &without_period) {
                                        descendant_unplaced_period_operations += 1;
                                    }
                                }
                                if matches(&row, &child_params) {
                                    inclusive.observe(&row, &facts);
                                }
                            }
                            if count < 100 || cursor >= through {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                    if count < 100 {
                        break;
                    }
                }
            }
            let child_value = inclusive.value();
            if let Some(value) = summary_value.as_mut() {
                let mut merged = BTreeMap::<(String, String), String>::new();
                for array in [&value["own_amounts"], &child_value["own_amounts"]] {
                    for amount in array.as_array().into_iter().flatten() {
                        let key = (
                            amount["currency"].as_str().unwrap_or("unknown").to_owned(),
                            amount["basis"].as_str().unwrap_or("unknown").to_owned(),
                        );
                        let sum = pioneer_provider::usage::add_decimal_amounts(
                            merged.get(&key).map(String::as_str).unwrap_or("0"),
                            amount["amount_decimal"].as_str().unwrap_or(""),
                        );
                        if let Some(sum) = sum {
                            merged.insert(key, sum);
                        } else {
                            graph_partial = true;
                        }
                    }
                }
                value["inclusive_amounts"]=merged.into_iter().map(|((currency,basis),amount)|json!({"currency":currency,"basis":basis,"amount_decimal":amount})).collect::<Vec<_>>().into();
                value["inclusive_coverage"] = if graph_partial {
                    "partial_authorized_descendants"
                } else {
                    "authorized_causal_descendants_at_live_edges"
                }
                .into();
                value["descendant_summary"] = child_value;
                value["descendant_period_unplaced_operations"] =
                    descendant_unplaced_period_operations.into();
                value["descendant_unknown_task_operations"] =
                    descendant_unknown_task_operations.into();
            }
        }
        if let Some(turn) = &p.turn_id {
            owner_turns.insert(turn.clone());
        } else if owner_turns.is_empty() {
            owner_turns.extend(
                self.crud_store
                    .execution_recent_owner_turns(
                        proof.workspace_id(),
                        proof.thread_id(),
                        p.task_id.as_deref(),
                        p.execution_id.as_deref(),
                    )
                    .await?,
            );
        }
        let owners = self
            .crud_store
            .execution_scope_owners(
                proof.workspace_id(),
                proof.thread_id(),
                owner_turns.into_iter().take(100).collect(),
            )
            .await?;
        let backfill = self
            .crud_store
            .execution_observation_backfill_progress()
            .await?;
        let auxiliary_backfill = self
            .crud_store
            .execution_aux_observation_backfill_progress()
            .await?;
        Ok(Response {
            workspace_id: proof.workspace_id().into(),
            thread_id: proof.thread_id().into(),
            through,
            next_after,
            records,
            summary: summary_value,
            owners,
            coverage: json!({"version":1,"frontier_basis":"immutable_metadata_revisions_live_usage_explicitly_revisioned",
                "legacy_backfill":{"complete":backfill.complete && auxiliary_backfill.complete,"main_complete":backfill.complete,"auxiliary_complete":auxiliary_backfill.complete,"basis":"global_migration_status_no_cross_scope_counts"},"running":"latest_observed_not_confirmed_alive",
                "scope":"authorized_thread","diagnostics":"bounded_milestones_with_explicit_loss_and_legacy_gaps","financial":"native_and_gateway_auxiliary_attempts_external_cli_statistics_nonadditive",
                "period_basis":"half_open_source_start_wall_ms_untimed_excluded_no_recorded_time_substitution",
                "period_unplaced_operations_in_scanned_scope":unplaced_period_operations,
                "comparison_basis":"bounded_export_with_scope_frontier_currency_and_basis_labels",
                "query_scope":{"view":p.view,"turn_id":p.turn_id,"task_id":p.task_id,"execution_id":p.execution_id,"operation_id":p.operation_id,"parent_operation_id":p.parent_operation_id,
                    "provider":p.provider,"model":p.model,"purpose":p.purpose,"from_wall_ms":p.from_wall_ms,"before_wall_ms":p.before_wall_ms,"include_descendants":p.include_descendants.unwrap_or(false)},
                "interpretation_versions":{"usage":"api_specific_retained_semantics","tariff_normalizer":1,"resolver":1,"calculator":1,"manifest":1},
                "owners_coverage":"at_most_100_referenced_or_recent_turns_and_100_owners_per_kind_live_domain_projection",
                "referenced_turn_limit_exceeded":owner_turns_omitted,
                "full_prompts_exported":false}),
        })
    }
}

fn observation_record(
    row: pioneer_entity::execution_observation::Model,
    facts: Value,
    tariff_evidence: Option<Value>,
) -> Record {
    let lane = if row.kind == "provider_phase" {
        row.parent_operation_id
            .clone()
            .unwrap_or_else(|| row.operation_id.clone())
    } else {
        row.operation_id.clone()
    };
    let causal_links = row
        .execution_id
        .iter()
        .chain(row.window_id.iter())
        .cloned()
        .collect();
    Record {
        lane,
        causal_links,
        sequence: row.sequence,
        operation_id: row.operation_id,
        revision: row.revision,
        source_revision: row.source_revision,
        resolution_origin: row.resolution_origin,
        turn_id: row.turn_id,
        root_thread_id: row.root_thread_id,
        task_id: row.task_id,
        task_run_id: row.task_run_id,
        task_execution_id: row.task_execution_id,
        execution_id: row.execution_id,
        owner_kind: row.owner_kind,
        owner_id: row.owner_id,
        parent_operation_id: row.parent_operation_id,
        kind: row.kind,
        purpose: row.purpose,
        provider: row.provider,
        model: row.model,
        status: row.status,
        recorded_at_ms: row.recorded_at_ms,
        tariff_hash: row.tariff_hash,
        timing: json!({"start_wall_ms":row.start_wall_ms,"end_wall_ms":row.end_wall_ms,"elapsed_ns":row.elapsed_ns,
                        "process_run_id":row.process_run_id,"start_process_run_id":row.start_process_run_id,
                        "missing_duration_reason":if row.elapsed_ns.is_some(){None}else if row.start_process_run_id.is_some() && row.process_run_id.is_some() && row.start_process_run_id!=row.process_run_id{Some("process_incarnation_changed")}else if row.end_wall_ms.is_none(){Some("terminal_boundary_unconfirmed")}else{Some("monotonic_measurement_unavailable")},
                        "local_sequence":facts["source_observation"]["local_sequence"].as_u64().or_else(||facts["local_sequence"].as_u64()).or_else(||facts["observation"]["eventSeq"].as_u64()),
                        "clock_domain":facts["timing"].get("clock_domain").or_else(||facts.get("clock_domain")),
                        "wall_clock_alignment":"unverified_across_processes","basis":"source_observed","coverage":if row.elapsed_ns.is_some(){"measured_monotonic"}else{"partial_or_untimed"}}),
        window_id: row.window_id,
        facts,
        tariff_evidence,
    }
}

fn context_breakdown(manifest: &Value, provider_input: Option<u64>) -> Value {
    #[derive(Default)]
    struct Footprint {
        text_bytes: u128,
        serialized_bytes: u128,
        estimated_tokens: u128,
        entries: u64,
        known_estimates: u64,
    }
    let mut categories = BTreeMap::<String, Footprint>::new();
    let mut tool_entries = Vec::new();
    let mut sources = BTreeMap::<String, u64>::new();
    let mut estimates = 0u128;
    let mut observe = |entry: &Value, tool: bool| {
        if entry["inclusion"] == "replaced_by_compiled_prompt" {
            return;
        }
        let category = if tool {
            "tool_definitions"
        } else {
            entry["category"].as_str().unwrap_or("unclassified")
        };
        let value = categories.entry(category.into()).or_default();
        value.entries += 1;
        value.text_bytes += entry["text_bytes"]
            .as_u64()
            .or_else(|| entry["rendered_bytes"].as_u64())
            .unwrap_or(0) as u128;
        value.serialized_bytes += entry["serialized_bytes"]
            .as_u64()
            .or_else(|| entry["serialized_canonical_bytes"].as_u64())
            .unwrap_or(0) as u128;
        if let Some(tokens) = entry["estimated_text_tokens"].as_u64() {
            value.estimated_tokens += tokens as u128;
            value.known_estimates += 1;
            estimates += tokens as u128;
        }
        // Association counts are non-additive footprint references. A source
        // reused across requests has a separate occurrence, never another charge.
        for source in entry["source_occurrences"].as_array().into_iter().flatten() {
            if let Some(id) = source["source_id_fingerprint"].as_str() {
                if sources.len() < 1024 || sources.contains_key(id) {
                    *sources.entry(id.into()).or_default() += 1;
                }
            }
        }
    };
    for entry in manifest["entries"].as_array().into_iter().flatten() {
        observe(entry, false);
    }
    let mut section_references = 0u64;
    for entry in manifest["compiled_sections"]["sections"]
        .as_array()
        .into_iter()
        .flatten()
    {
        if entry["included_in_provider_instructions"] == true {
            observe(entry, false);
        } else {
            section_references += 1;
        }
    }
    for entry in manifest["tool_entries"].as_array().into_iter().flatten() {
        observe(entry, true);
        tool_entries.push(json!({"tool_name":entry["tool_name"],"kind":entry["kind"],"serialized_canonical_bytes":entry["serialized_canonical_bytes"],"estimated_text_tokens":entry["estimated_text_tokens"],"origin":entry["origin"]}));
    }
    drop(observe);
    let residual = provider_input.map(|input| (i128::from(input) - estimates as i128).to_string());
    json!({"version":1,"layer":manifest["layer"],"categories":categories.into_iter().map(|(category,value)|json!({"category":category,"text_bytes_utf8_known":value.text_bytes.to_string(),"serialized_bytes_known":value.serialized_bytes.to_string(),"estimated_text_tokens_known":value.estimated_tokens.to_string(),"entries":value.entries,"entries_with_estimate":value.known_estimates})).collect::<Vec<_>>(),
        "tools":tool_entries,"sources":sources.into_iter().map(|(id,occurrences)|json!({"source_id_fingerprint":id,"occurrences":occurrences,"footprint_allocation":"association_only_shared_sources_not_summed"})).collect::<Vec<_>>(),
        "nonadditive_compiled_section_references":section_references,
        "compiled_section_basis":"only_confirmed_provider_instructions_additive_turn_context_measured_in_messages",
        "provider_input_tokens":provider_input,"provider_minus_estimated_text_residual":residual,
        "residual_basis":"diagnostic_comparison_mixed_coverage_not_an_exact_token_partition",
        "omitted_message_entries":manifest["omitted_message_entries"],"omitted_tool_entries":manifest["omitted_tool_entries"],"omitted_components":manifest["omitted_component_entries"],
        "monetary_allocation":"none","media_and_opaque_tokens":"unallocated_without_api_measurement"})
}

/// Pair only source-owned boundaries from the same terminal observation/run.
/// These inclusive phases may overlap; absent endings stay open, never zero.
fn tool_phase_intervals(observation: &Value) -> Value {
    let Some(run) = observation["processRunId"].as_str() else {
        return json!([]);
    };
    let mut pending = BTreeMap::<(u64, String), Vec<&Value>>::new();
    let mut intervals = Vec::new();
    for boundary in observation["phaseBoundaries"]
        .as_array()
        .into_iter()
        .flatten()
        .take(48)
    {
        let Some(label) = boundary["phase"].as_str() else {
            continue;
        };
        let Some((phase, state)) = label.rsplit_once('.') else {
            continue;
        };
        let attempt = boundary["attemptId"].as_u64().unwrap_or(0);
        let key = (attempt, phase.to_owned());
        if state == "started" {
            pending.entry(key).or_default().push(boundary);
        } else if matches!(state, "completed" | "failed") {
            let start = pending.get_mut(&key).and_then(Vec::pop);
            let elapsed = start
                .and_then(|s| s["monoNs"].as_u64())
                .zip(boundary["monoNs"].as_u64())
                .and_then(|(a, b)| b.checked_sub(a));
            intervals.push(json!({"phase":phase,"attempt_id":attempt,"status":state,"process_run_id":run,
                "start_wall_ms":start.map(|s|&s["tsUnixMs"]),"end_wall_ms":boundary["tsUnixMs"],"elapsed_ns":elapsed,
                "start_sequence":start.map(|s|&s["localSequence"]),"end_sequence":boundary["localSequence"],
                "basis":"paired_tool_source_monotonic_inclusive","coverage":if elapsed.is_some(){"measured"}else{"missing_start_or_clock_anomaly"}}));
        }
    }
    for ((attempt, phase), starts) in pending {
        for start in starts {
            intervals.push(json!({"phase":phase,"attempt_id":attempt,"status":"unconfirmed","process_run_id":run,
                "start_wall_ms":start["tsUnixMs"],"end_wall_ms":null,"elapsed_ns":null,"start_sequence":start["localSequence"],
                "basis":"source_start_without_end","coverage":"incomplete_or_bounded_detail_loss"}));
        }
    }
    intervals.into()
}
