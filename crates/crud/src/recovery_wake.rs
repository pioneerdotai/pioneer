use anyhow::{Context, Result};
use pioneer_entity::{
    agent_work_queue as q, recovery_job as j, recovery_terminalization_outbox as o,
    turn_execution as e,
};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, Condition, EntityTrait, ExprTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait,
};

/// Advisory observations only. Each admitted handler retains its authoritative
/// transactional predicates. Errors must never be interpreted as an empty set.
#[derive(Debug, FromQueryResult)]
pub struct RecoveryAdmission {
    pub orphan: bool,
    pub backfill: bool,
    pub active: bool,
    pub terminal_repair: bool,
    pub terminal_audit: bool,
    pub terminal_delivery: bool,
    pub queue: bool,
    pub action_outbox: bool,
    pub queue_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    pub terminal_repair_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    pub orphan_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    pub terminal_delivery_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    pub action_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
}

impl RecoveryAdmission {
    pub fn unknown() -> Self {
        Self {
            orphan: true,
            backfill: true,
            active: true,
            terminal_repair: true,
            terminal_audit: true,
            terminal_delivery: true,
            queue: true,
            action_outbox: true,
            queue_next: None,
            terminal_repair_next: None,
            orphan_next: None,
            terminal_delivery_next: None,
            action_next: None,
        }
    }
    pub fn control_runnable(&self) -> bool {
        self.backfill
            || self.terminal_repair
            || self.terminal_audit
            || self.terminal_delivery
            || (self.queue && self.queue_next.is_none_or(|at| at <= crate::utc_now()))
            || self.action_outbox
    }
    pub fn control_deadline(&self) -> Option<sea_orm::prelude::DateTimeWithTimeZone> {
        let ceil_second = |at: sea_orm::prelude::DateTimeWithTimeZone| {
            crate::unix_to_datetime(
                at.timestamp()
                    .saturating_add(i64::from(at.timestamp_subsec_nanos() != 0)),
            )
        };
        self.queue_next
            .into_iter()
            .chain(self.action_next)
            .chain(self.orphan_next.map(ceil_second))
            .chain(self.terminal_delivery_next.map(ceil_second))
            .chain(self.terminal_repair_next.map(ceil_second))
            .min()
    }
}

impl crate::CrudStore {
    pub async fn recovery_admission(
        &self,
        owner: &str,
        now_unix: i64,
    ) -> Result<RecoveryAdmission> {
        let mut pass = RecoveryClockPass::default();
        while !self.recovery_clock_quantum(&mut pass, true).await? {}
        let mut result = self.recovery_admission_core(owner, now_unix).await?;
        // Explicit compatibility observation only. The live owner uses its
        // retained finite orphan pass instead of folding this entire source.
        let upper = e::Entity::find()
            .select_only()
            .column(e::Column::TurnId)
            .order_by_desc(e::Column::TurnId)
            .limit(1)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?;
        if let Some(upper) = upper {
            let mut after = String::new();
            loop {
                let ids = e::Entity::find()
                    .select_only()
                    .column(e::Column::TurnId)
                    .filter(e::Column::TurnId.gt(after.clone()))
                    .filter(e::Column::TurnId.lte(upper.clone()))
                    .order_by_asc(e::Column::TurnId)
                    .limit(64)
                    .into_tuple::<String>()
                    .all(&self.connection)
                    .await?;
                let done = ids.len() < 64;
                for id in ids {
                    after = id.clone();
                    let row = e::Entity::find_by_id(id)
                        .select_only()
                        .columns([e::Column::OwnerId, e::Column::Status, e::Column::LeaseUntil])
                        .into_tuple::<(String, String, sea_orm::prelude::DateTimeWithTimeZone)>()
                        .one(&self.connection)
                        .await?;
                    if let Some((current, status, lease)) = row {
                        if current == owner {
                            result.orphan |= status == "recovering";
                        } else if matches!(
                            status.as_str(),
                            "queued" | "starting" | "running" | "recovering"
                        ) {
                            result.orphan |= lease <= crate::unix_to_datetime(now_unix);
                            retain_clock(&mut result.orphan_next, Some(lease));
                        }
                    }
                }
                if done {
                    break;
                }
            }
        }
        result.terminal_audit = pass.terminal_audit;
        result.backfill = pass.backfill;
        let mut admission = ControlAdmissionPass::default();
        while !self.control_admission_quantum(&mut admission).await? {}
        admission.apply(&mut result, now_unix);
        result.terminal_repair_next = pass.repair_next;
        result.terminal_delivery_next = pass.terminal_next;
        result.terminal_repair = pass
            .repair_next
            .is_some_and(|at| at <= crate::unix_to_datetime(now_unix));
        result.terminal_delivery = pass
            .terminal_next
            .is_some_and(|at| at <= crate::unix_to_datetime(now_unix));
        Ok(result)
    }
    /// Remaining grouped heads only. Orphan and terminal-audit admission are
    /// supplied by finite passes in the caller, never population EXISTS/MIN.
    pub async fn recovery_admission_core(
        &self,
        _owner: &str,
        _now_unix: i64,
    ) -> Result<RecoveryAdmission> {
        let mut observation = j::Entity::find()
            .select_only()
            .expr_as(Expr::val(false), "orphan")
            .expr_as(Expr::val(false), "backfill")
            .expr_as(
                Expr::exists(
                    j::Entity::find()
                        .filter(Expr::col(j::Column::Status).eq(Expr::Constant("active".into())))
                        .into_query(),
                ),
                "active",
            )
            .expr_as(Expr::val(false), "terminal_repair")
            .expr_as(Expr::val(false), "terminal_audit")
            .expr_as(Expr::val(false), "terminal_delivery")
            .expr_as(Expr::val(false), "queue")
            .expr_as(Expr::val(false), "action_outbox");
        observation = observation
            .expr_as(
                Expr::val(None::<sea_orm::prelude::DateTimeWithTimeZone>),
                "queue_next",
            )
            .expr_as(
                Expr::val(None::<sea_orm::prelude::DateTimeWithTimeZone>),
                "terminal_repair_next",
            )
            .expr_as(
                Expr::val(None::<sea_orm::prelude::DateTimeWithTimeZone>),
                "orphan_next",
            )
            .expr_as(
                Expr::val(None::<sea_orm::prelude::DateTimeWithTimeZone>),
                "terminal_delivery_next",
            )
            .expr_as(
                Expr::val(None::<sea_orm::prelude::DateTimeWithTimeZone>),
                "action_next",
            );
        // Keep execution in the Entity/typed projection path, with a scalar
        // SELECT so an empty database still returns a confirmed observation.
        QueryTrait::query(&mut observation).from_clear();
        observation
            .into_model::<RecoveryAdmission>()
            .one(&self.connection)
            .await?
            .context("recovery admission returned no observation")
    }
}

/// A raw bounded active-attempt page. Individual malformed records retain
/// their ID and error, so a healthy tail can still consume its quota.
#[derive(Debug)]
pub struct TimeoutSupervisionPage {
    pub after: Option<String>,
    pub rows: Vec<Result<TimeoutSupervisionRow>>,
}
#[derive(Debug)]
pub struct TimeoutSupervisionRow {
    pub id: String,
    pub next_at_unix: Option<i64>,
    pub candidate: Option<crate::TimeoutCandidate>,
}
#[derive(FromQueryResult)]
struct TimeoutDeadlineRecord {
    id: String,
    turn_id: String,
    item_id: String,
    item_type: String,
    execution_class: String,
    attempt_number: i64,
    started_at: sea_orm::prelude::DateTimeWithTimeZone,
    started_event_sequence: Option<i64>,
    last_heartbeat_at: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    lease_expires_at: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    idle_deadline_at: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    hard_deadline_at: Option<sea_orm::prelude::DateTimeWithTimeZone>,
}
fn timeout_source_query() -> sea_orm::Select<pioneer_entity::turn_item_attempt::Entity> {
    use pioneer_entity::turn_item_attempt as attempt;
    attempt::Entity::find()
        .filter(attempt::Column::Status.eq("running"))
        .filter(attempt::Column::ExecutionClass.is_not_null())
}
impl crate::CrudStore {
    pub async fn timeout_supervision_upper(&self) -> Result<Option<String>> {
        use pioneer_entity::turn_item_attempt as attempt;
        Ok(attempt::Entity::find()
            .select_only()
            .column(attempt::Column::Id)
            .order_by_desc(attempt::Column::Id)
            .limit(1)
            .into_tuple::<String>()
            .one(&self.connection)
            .await?)
    }
    pub async fn timeout_supervision_page(
        &self,
        after: Option<&str>,
        upper: &str,
        now_unix: i64,
    ) -> Result<TimeoutSupervisionPage> {
        use pioneer_entity::turn_item_attempt as attempt;
        use sea_orm::QueryOrder;
        // Only the PK range precedes LIMIT. Status/class filtering and decoding
        // are point observations, so excluded history advances raw progress.
        let ids = attempt::Entity::find()
            .select_only()
            .column(attempt::Column::Id)
            .filter(attempt::Column::Id.gt(after.unwrap_or("")))
            .filter(attempt::Column::Id.lte(upper))
            .order_by_asc(attempt::Column::Id)
            .limit(64)
            .into_tuple::<String>()
            .all(&self.connection)
            .await?;
        let after = ids.last().cloned();
        let mut rows = Vec::with_capacity(ids.len());
        for id in ids {
            match self.timeout_supervision_row(&id, now_unix).await {
                Ok(Some(row)) => rows.push(Ok(row)),
                Ok(None) => {}
                Err(error) => rows.push(Err(error)),
            }
        }
        Ok(TimeoutSupervisionPage { after, rows })
    }
    pub async fn timeout_supervision_row(
        &self,
        id: &str,
        now_unix: i64,
    ) -> Result<Option<TimeoutSupervisionRow>> {
        let Some(row) = self.timeout_deadline_record(id).await? else {
            return Ok(None);
        };
        let at = [
            row.lease_expires_at,
            row.idle_deadline_at,
            row.hard_deadline_at,
        ]
        .into_iter()
        .flatten()
        .min();
        let next_at_unix = at.map(|at| {
            at.timestamp()
                .saturating_add(i64::from(at.timestamp_subsec_nanos() != 0))
        });
        let candidate = if at.is_some_and(|at| at <= crate::unix_to_datetime(now_unix)) {
            Some(crate::TimeoutCandidate {
                attempt_id: row.id.clone(),
                turn_id: row.turn_id,
                item_id: row.item_id,
                item_type: crate::convention::turn_item_type_from_db(&row.item_type)
                    .unwrap_or(pioneer_protocol::TurnItemType::DynamicToolCall),
                execution_class: crate::convention::turn_item_execution_class_from_db(
                    &row.execution_class,
                )
                .context("active timeout attempt has invalid execution class")?,
                attempt_number: row.attempt_number,
                timeout_reason: crate::infer_timeout_reason(
                    row.lease_expires_at,
                    row.idle_deadline_at,
                    row.hard_deadline_at,
                    now_unix,
                ),
                started_at_unix: row.started_at.timestamp(),
                started_event_sequence: row.started_event_sequence,
                last_heartbeat_at_unix: row.last_heartbeat_at.map(|at| at.timestamp()),
                lease_expires_at_unix: row.lease_expires_at.map(|at| at.timestamp()),
                idle_deadline_at_unix: row.idle_deadline_at.map(|at| at.timestamp()),
                hard_deadline_at_unix: row.hard_deadline_at.map(|at| at.timestamp()),
            })
        } else {
            None
        };
        Ok(Some(TimeoutSupervisionRow {
            id: row.id,
            next_at_unix,
            candidate,
        }))
    }
    async fn timeout_deadline_record(&self, id: &str) -> Result<Option<TimeoutDeadlineRecord>> {
        use pioneer_entity::turn_item_attempt::Column as A;
        timeout_source_query()
            .select_only()
            .columns([
                A::Id,
                A::TurnId,
                A::ItemId,
                A::ItemType,
                A::ExecutionClass,
                A::AttemptNumber,
                A::StartedAt,
                A::StartedEventSequence,
                A::LastHeartbeatAt,
                A::LeaseExpiresAt,
                A::IdleDeadlineAt,
                A::HardDeadlineAt,
            ])
            .filter(A::Id.eq(id))
            .into_model::<TimeoutDeadlineRecord>()
            .one(&self.connection)
            .await
            .map_err(Into::into)
    }
    pub async fn command_supervision_upper(&self) -> Result<Option<String>> {
        // Both owners traverse the same unfiltered immutable PK source.
        self.timeout_supervision_upper().await
    }
    pub async fn command_supervision_page(
        &self,
        after: Option<&str>,
        upper: &str,
    ) -> Result<(Option<String>, Vec<Result<crate::RunningTurnItemAttempt>>)> {
        use pioneer_entity::turn_item_attempt as attempt;
        use sea_orm::QueryOrder;
        let ids = attempt::Entity::find()
            .select_only()
            .column(attempt::Column::Id)
            .filter(attempt::Column::Id.gt(after.unwrap_or("")))
            .filter(attempt::Column::Id.lte(upper))
            .order_by_asc(attempt::Column::Id)
            .limit(64)
            .into_tuple::<String>()
            .all(&self.connection)
            .await?;
        let after = ids.last().cloned();
        let mut rows = Vec::with_capacity(ids.len());
        for id in ids {
            let result = async {
                let Some(row) = self.timeout_deadline_record(&id).await? else {
                    return Ok(None);
                };
                // Revalidate type as well as status after the key discovery.
                if row.item_type != "command_execution" {
                    return Ok(None);
                }
                Ok::<_, anyhow::Error>(Some(crate::RunningTurnItemAttempt {
                    turn_id: row.turn_id,
                    item_id: row.item_id,
                    item_type: pioneer_protocol::TurnItemType::CommandExecution,
                    execution_class: crate::convention::turn_item_execution_class_from_db(
                        &row.execution_class,
                    )
                    .context("command supervision has invalid execution class")?,
                    started_at_unix: row.started_at.timestamp(),
                    last_heartbeat_at_unix: row.last_heartbeat_at.map(|at| at.timestamp()),
                }))
            }
            .await;
            match result {
                Ok(Some(row)) => rows.push(Ok(row)),
                Ok(None) => {}
                Err(error) => rows.push(Err(error)),
            }
        }
        Ok((after, rows))
    }
    /// Used only after authoritative liveness classified a real human wait.
    /// Existing open-status/kind predicates are retained; no payload is read.
    pub async fn timeout_human_wait_deadlines(
        &self,
        turn_id: &str,
        native_runtime: &str,
    ) -> Result<(
        Option<sea_orm::prelude::DateTimeWithTimeZone>,
        Option<sea_orm::prelude::DateTimeWithTimeZone>,
    )> {
        use pioneer_entity::cli_runtime_pending_request as request;
        let open = request::Entity::find()
            .select_only()
            .filter(request::Column::TurnId.eq(turn_id))
            .filter(request::Column::Status.is_in([
                "pending",
                "response_accepted",
                "delivering",
                "delivery_failed",
            ]))
            .filter(
                Condition::any()
                    .add(request::Column::RuntimeId.eq(native_runtime))
                    .add(request::Column::RequestKind.is_in([
                        "command_approval",
                        "file_change_approval",
                        "permission_approval",
                        "user_input",
                    ])),
            );
        let row = open
            .expr_as(Expr::col(request::Column::CreatedAt).min(), "created")
            .expr_as(
                Expr::expr(
                    Expr::case(
                        request::Column::Status.is_in(["response_accepted", "delivering"]),
                        Expr::col(request::Column::UpdatedAt),
                    )
                    .finally(Expr::val(None::<sea_orm::prelude::DateTimeWithTimeZone>)),
                )
                .min(),
                "delivery",
            )
            .into_tuple::<(
                Option<sea_orm::prelude::DateTimeWithTimeZone>,
                Option<sea_orm::prelude::DateTimeWithTimeZone>,
            )>()
            .one(&self.connection)
            .await?;
        Ok(row.unwrap_or((None, None)))
    }
}

/// Process-local reconstruction state for the two existing recovery sources.
/// Keys, not eligible matches, determine the finite upper bound and progress.
#[derive(Default)]
pub struct RecoveryClockPass {
    /// Raw IDs from this quantum only; never authority or a persistent queue.
    pub job_candidates: Vec<String>,
    pub backfill_candidates: Vec<String>,
    pub backfill: bool,
    attempt_upper: Option<String>,
    attempt_after: Option<String>,
    attempt_done: bool,
    /// Missing-outbox locators from this quantum only. The latch survives later
    /// pages and own enqueue until a successful verification pass clears it.
    pub terminal_audit_candidates: Vec<String>,
    pub terminal_audit: bool,
    job_upper: Option<String>,
    job_after: Option<String>,
    job_done: bool,
    terminal_upper: Option<String>,
    terminal_after: Option<String>,
    terminal_done: bool,
    pub job_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    pub repair_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    pub terminal_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    failure: Option<anyhow::Error>,
}
fn retain_clock(
    next: &mut Option<sea_orm::prelude::DateTimeWithTimeZone>,
    at: Option<sea_orm::prelude::DateTimeWithTimeZone>,
) {
    *next = next.iter().copied().chain(at).min();
}
fn recovery_ceil(
    at: sea_orm::prelude::DateTimeWithTimeZone,
) -> Result<sea_orm::prelude::DateTimeWithTimeZone> {
    let nanos = at.timestamp_subsec_nanos();
    if nanos == 0 {
        return Ok(at);
    }
    at.checked_add_signed(chrono::Duration::nanoseconds(i64::from(
        1_000_000_000 - nanos,
    )))
    .context("recovery deadline overflow")
}
impl crate::CrudStore {
    /// At most64 raw job keys and, for control,64 terminal and64 attempt keys,
    /// each before eligibility. ORM all() releases its reader before point work.
    pub async fn recovery_clock_quantum(
        &self,
        pass: &mut RecoveryClockPass,
        terminal: bool,
    ) -> Result<bool> {
        pass.job_candidates.clear();
        pass.terminal_audit_candidates.clear();
        pass.backfill_candidates.clear();
        if terminal && !pass.attempt_done {
            use pioneer_entity::turn_item_attempt as attempt;
            if pass.attempt_upper.is_none() {
                pass.attempt_upper = attempt::Entity::find()
                    .select_only()
                    .column(attempt::Column::Id)
                    .order_by_desc(attempt::Column::Id)
                    .limit(1)
                    .into_tuple::<String>()
                    .one(&self.connection)
                    .await?;
            }
            if let Some(upper) = &pass.attempt_upper {
                let ids = attempt::Entity::find()
                    .select_only()
                    .column(attempt::Column::Id)
                    .filter(attempt::Column::Id.gt(pass.attempt_after.as_deref().unwrap_or("")))
                    .filter(attempt::Column::Id.lte(upper.clone()))
                    .order_by_asc(attempt::Column::Id)
                    .limit(64)
                    .into_tuple::<String>()
                    .all(&self.connection)
                    .await?;
                pass.attempt_done = ids.len() < 64;
                for id in ids {
                    let point = crate::repositories::turn_item_attempt::unqueued_timeout_query()
                        .filter(attempt::Column::Id.eq(id.clone()))
                        .select_only()
                        .column(attempt::Column::Id)
                        .into_tuple::<String>()
                        .one(&self.connection)
                        .await;
                    match point {
                        Ok(Some(_)) => {
                            pass.backfill = true;
                            pass.backfill_candidates.push(id.clone());
                        }
                        Ok(None) => {}
                        Err(error) => {
                            pass.failure.get_or_insert(error.into());
                        }
                    }
                    pass.attempt_after = Some(id);
                }
            } else {
                pass.attempt_done = true;
            }
        }
        if !pass.job_done {
            if pass.job_upper.is_none() {
                pass.job_upper = j::Entity::find()
                    .select_only()
                    .column(j::Column::Id)
                    .order_by_desc(j::Column::Id)
                    .limit(1)
                    .into_tuple::<String>()
                    .one(&self.connection)
                    .await?;
                pass.job_done = pass.job_upper.is_none();
            }
            if let Some(upper) = pass.job_upper.as_ref() {
                let ids = j::Entity::find()
                    .select_only()
                    .column(j::Column::Id)
                    .filter(j::Column::Id.gt(pass.job_after.as_deref().unwrap_or("")))
                    .filter(j::Column::Id.lte(upper.clone()))
                    .order_by_asc(j::Column::Id)
                    .limit(64)
                    .into_tuple::<String>()
                    .all(&self.connection)
                    .await?;
                pass.job_done = ids.len() < 64;
                pass.job_candidates.clone_from(&ids);
                for id in ids {
                    pass.job_after = Some(id.clone());
                    // The projection has no policy/diagnostic payloads.
                    let row = j::Entity::find_by_id(id.clone())
                        .select_only()
                        .columns([
                            j::Column::Status,
                            j::Column::Action,
                            j::Column::TurnId,
                            j::Column::NextRunAt,
                            j::Column::ClaimExpiresAt,
                            j::Column::ResolutionPending,
                        ])
                        .into_tuple::<(
                            String,
                            String,
                            String,
                            sea_orm::prelude::DateTimeWithTimeZone,
                            Option<sea_orm::prelude::DateTimeWithTimeZone>,
                            bool,
                        )>()
                        .one(&self.connection)
                        .await;
                    match row {
                        Ok(Some((status, action, turn, next, claim, _))) if status == "pending" => {
                            let at = match recovery_ceil(
                                claim.map_or(next, |claim| std::cmp::max(claim, next)),
                            ) {
                                Ok(at) => at,
                                Err(error) => {
                                    pass.failure.get_or_insert(error);
                                    continue;
                                }
                            };
                            retain_clock(&mut pass.job_next, Some(at));
                            if terminal && action == "mark_failed" {
                                // uq_recovery_job_active_turn bounds this status/Turn lookup to one row.
                                match j::Entity::find()
                                    .select_only()
                                    .column(j::Column::Id)
                                    .filter(
                                        Expr::col(j::Column::Status)
                                            .eq(Expr::Constant("active".into())),
                                    )
                                    .filter(j::Column::TurnId.eq(turn))
                                    .limit(1)
                                    .into_tuple::<String>()
                                    .one(&self.connection)
                                    .await
                                {
                                    Ok(None) => retain_clock(&mut pass.repair_next, Some(at)),
                                    Ok(Some(_)) => {}
                                    Err(error) => {
                                        pass.failure.get_or_insert(error.into());
                                    }
                                }
                            }
                        }
                        Ok(Some((status, _, _, _, _, true)))
                            if terminal
                                && matches!(
                                    status.as_str(),
                                    "failed" | "exhausted" | "blocked"
                                ) =>
                        {
                            // At most one PK probe per selected raw job. Existing
                            // future/leased outboxes consume input, not a global anti-join.
                            match o::Entity::find_by_id(id.clone())
                                .select_only()
                                .column(o::Column::RecoveryJobId)
                                .into_tuple::<String>()
                                .one(&self.connection)
                                .await
                            {
                                Ok(None) => {
                                    pass.terminal_audit = true;
                                    pass.terminal_audit_candidates.push(id.clone());
                                }
                                Ok(Some(_)) => {}
                                Err(error) => {
                                    pass.failure.get_or_insert(error.into());
                                }
                            }
                        }
                        Err(error) => {
                            pass.failure.get_or_insert(error.into());
                        }
                        _ => {}
                    }
                    pass.job_after = Some(id);
                }
            }
        }
        if terminal && !pass.terminal_done {
            if pass.terminal_upper.is_none() {
                pass.terminal_upper = o::Entity::find()
                    .select_only()
                    .column(o::Column::RecoveryJobId)
                    .order_by_desc(o::Column::RecoveryJobId)
                    .limit(1)
                    .into_tuple::<String>()
                    .one(&self.connection)
                    .await?;
                pass.terminal_done = pass.terminal_upper.is_none();
            }
            if let Some(upper) = pass.terminal_upper.as_ref() {
                let ids = o::Entity::find()
                    .select_only()
                    .column(o::Column::RecoveryJobId)
                    .filter(
                        o::Column::RecoveryJobId.gt(pass.terminal_after.as_deref().unwrap_or("")),
                    )
                    .filter(o::Column::RecoveryJobId.lte(upper.clone()))
                    .order_by_asc(o::Column::RecoveryJobId)
                    .limit(64)
                    .into_tuple::<String>()
                    .all(&self.connection)
                    .await?;
                pass.terminal_done = ids.len() < 64;
                for id in ids {
                    let row = o::Entity::find_by_id(id.clone())
                        .select_only()
                        .columns([
                            o::Column::Status,
                            o::Column::NextRunAt,
                            o::Column::ClaimExpiresAt,
                        ])
                        .into_tuple::<(
                            String,
                            sea_orm::prelude::DateTimeWithTimeZone,
                            Option<sea_orm::prelude::DateTimeWithTimeZone>,
                        )>()
                        .one(&self.connection)
                        .await;
                    match row {
                        Ok(Some((status, next, claim))) => {
                            let at = match status.as_str() {
                                "pending" | "failed" => Some(next),
                                "delivering" => claim,
                                _ => None,
                            };
                            match at.map(recovery_ceil).transpose() {
                                Ok(at) => retain_clock(&mut pass.terminal_next, at),
                                Err(error) => {
                                    pass.failure.get_or_insert(error);
                                }
                            }
                        }
                        Err(error) => {
                            pass.failure.get_or_insert(error.into());
                        }
                        _ => {}
                    }
                    pass.terminal_after = Some(id);
                }
            }
        }
        let complete = pass.job_done && (!terminal || (pass.terminal_done && pass.attempt_done));
        if complete {
            if let Some(error) = pass.failure.take() {
                return Err(error);
            }
        }
        Ok(complete)
    }
}

/// Queue and action discovery use immutable PKs before any policy. Each call
/// selects <=1 distinct queue root, <=64 queue keys, <=64 permit keys and <=64 action keys. Cursors survive
/// empty matching pages. Capacity is advisory; promotion revalidates atomically.
#[derive(Default)]
pub struct ControlAdmissionPass {
    root_upper: Option<String>,
    root_after: Option<String>,
    current_root: Option<String>,
    queue_upper: Option<String>,
    queue_after: Option<String>,
    queue_done: bool,
    action_upper: Option<String>,
    action_after: Option<String>,
    action_done: bool,
    capacity: Option<QueueCapacityPass>,
    cached_capacity: Option<(String, bool)>,
    pub queue_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    pub queue_immediate: bool,
    pub action_next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    action_immediate: bool,
    failure: Option<anyhow::Error>,
}
struct QueueCapacityPass {
    root: String,
    upper: Option<String>,
    after: Option<String>,
    held: i64,
}
impl ControlAdmissionPass {
    /// Legacy whole-observation callers use a truncated seconds clock.
    /// Keep their conservative admission, including the original fractional oracle.
    pub fn apply(&self, admission: &mut RecoveryAdmission, now_unix: i64) {
        self.apply_at(admission, crate::utc_now());
        admission.queue = self.queue_immediate
            || self
                .queue_next
                .is_some_and(|at| at < crate::unix_to_datetime(now_unix.saturating_add(1)));
    }
    /// Retained owner compares the same full-resolution wall source used by
    /// promotion. Future fractions park at the real deadline, never Runnable.
    pub fn apply_at(
        &self,
        admission: &mut RecoveryAdmission,
        now: sea_orm::prelude::DateTimeWithTimeZone,
    ) {
        admission.queue = self.queue_immediate || self.queue_next.is_some_and(|at| at <= now);
        admission.queue_next = if self.queue_immediate {
            None
        } else {
            self.queue_next
        };
        admission.action_next = self.action_next;
        admission.action_outbox =
            self.action_immediate || self.action_next.is_some_and(|at| at <= now);
    }
}
impl crate::CrudStore {
    pub async fn control_admission_quantum(&self, pass: &mut ControlAdmissionPass) -> Result<bool> {
        use pioneer_entity::{
            agent_action_outbox as action, agent_running_permit as permit,
            agent_work_branch_schedule as branch, agent_work_resource_scope as scope,
        };
        if !pass.queue_done {
            // idx_agent_work_queue_root_state begins with RootExecutionId.
            // No state/policy predicate, DISTINCT, or historical scope traversal:
            // strict root seek skips every duplicate group in the index.
            if pass.root_upper.is_none() {
                pass.root_upper = q::Entity::find()
                    .select_only()
                    .column(q::Column::RootExecutionId)
                    .order_by_desc(q::Column::RootExecutionId)
                    .limit(1)
                    .into_tuple::<String>()
                    .one(&self.connection)
                    .await?;
            }
            if pass.current_root.is_none() {
                pass.current_root = if let Some(upper) = &pass.root_upper {
                    q::Entity::find()
                        .select_only()
                        .column(q::Column::RootExecutionId)
                        .filter(
                            q::Column::RootExecutionId.gt(pass.root_after.as_deref().unwrap_or("")),
                        )
                        .filter(q::Column::RootExecutionId.lte(upper.clone()))
                        .order_by_asc(q::Column::RootExecutionId)
                        .limit(1)
                        .into_tuple::<String>()
                        .one(&self.connection)
                        .await?
                } else {
                    None
                };
            }
            let root = pass.current_root.clone();
            if let Some(root) = root {
                let result: Result<bool> = async {
                    let Some((status, max_concurrency)) = scope::Entity::find_by_id(root.clone())
                        .select_only()
                        .columns([scope::Column::Status, scope::Column::MaxConcurrency])
                        .into_tuple::<(String, i64)>()
                        .one(&self.connection)
                        .await?
                    else {
                        return Ok(true);
                    };
                    if status != "active" {
                        return Ok(true);
                    }
                    if pass
                        .cached_capacity
                        .as_ref()
                        .is_none_or(|(key, _)| key != &root)
                    {
                        if pass.capacity.is_none() {
                            let upper = permit::Entity::find()
                                .select_only()
                                .column(permit::Column::Id)
                                .order_by_desc(permit::Column::Id)
                                .limit(1)
                                .into_tuple::<String>()
                                .one(&self.connection)
                                .await?;
                            pass.capacity = Some(QueueCapacityPass {
                                root: root.clone(),
                                upper,
                                after: None,
                                held: 0,
                            });
                        }
                        let capacity = pass.capacity.as_mut().unwrap();
                        let rows = if let Some(upper) = &capacity.upper {
                            permit::Entity::find()
                                .select_only()
                                .columns([
                                    permit::Column::Id,
                                    permit::Column::RootExecutionId,
                                    permit::Column::Status,
                                ])
                                .filter(
                                    permit::Column::Id.gt(capacity.after.as_deref().unwrap_or("")),
                                )
                                .filter(permit::Column::Id.lte(upper.clone()))
                                .order_by_asc(permit::Column::Id)
                                .limit(64)
                                .into_tuple::<(String, String, String)>()
                                .all(&self.connection)
                                .await?
                        } else {
                            Vec::new()
                        };
                        let done = rows.len() < 64;
                        for (id, key, status) in rows {
                            capacity.after = Some(id);
                            if key == capacity.root && status == "held" {
                                capacity.held += 1;
                            }
                        }
                        if !done && capacity.held < max_concurrency {
                            return Ok(false);
                        }
                        pass.cached_capacity =
                            Some((root.clone(), capacity.held < max_concurrency));
                        pass.capacity = None;
                    }
                    if !pass.cached_capacity.as_ref().unwrap().1 {
                        return Ok(true);
                    }
                    if pass.queue_upper.is_none() {
                        pass.queue_upper = q::Entity::find()
                            .select_only()
                            .column(q::Column::Id)
                            .order_by_desc(q::Column::Id)
                            .limit(1)
                            .into_tuple::<String>()
                            .one(&self.connection)
                            .await?;
                    }
                    let ids = if let Some(upper) = &pass.queue_upper {
                        q::Entity::find()
                            .select_only()
                            .column(q::Column::Id)
                            .filter(q::Column::Id.gt(pass.queue_after.as_deref().unwrap_or("")))
                            .filter(q::Column::Id.lte(upper.clone()))
                            .order_by_asc(q::Column::Id)
                            .limit(64)
                            .into_tuple::<String>()
                            .all(&self.connection)
                            .await?
                    } else {
                        Vec::new()
                    };
                    let done = ids.len() < 64;
                    for id in ids {
                        pass.queue_after = Some(id.clone());
                        let point: Result<()> = async {
                            let Some(row) = q::Entity::find_by_id(id).one(&self.connection).await?
                            else {
                                return Ok(());
                            };
                            if row.state != "queued" || row.root_execution_id != root {
                                return Ok(());
                            }
                            if branch::Entity::find_by_id((root.clone(), row.branch_key))
                                .select_only()
                                .column(branch::Column::BranchKey)
                                .into_tuple::<String>()
                                .one(&self.connection)
                                .await?
                                .is_none()
                            {
                                return Ok(());
                            }
                            if row.eligible_at.is_none() {
                                pass.queue_immediate = true;
                            }
                            retain_clock(&mut pass.queue_next, row.eligible_at);
                            Ok(())
                        }
                        .await;
                        if let Err(error) = point {
                            pass.failure.get_or_insert(error);
                        }
                    }
                    Ok(done)
                }
                .await;
                let advance = match result {
                    Ok(done) => done,
                    Err(error) => {
                        pass.failure.get_or_insert(error);
                        true
                    }
                };
                if advance {
                    pass.root_after = Some(root);
                    pass.current_root = None;
                    pass.capacity = None;
                    pass.cached_capacity = None;
                    pass.queue_upper = None;
                    pass.queue_after = None;
                }
            } else {
                pass.queue_done = true;
            }
        }
        if !pass.action_done {
            if pass.action_upper.is_none() {
                pass.action_upper = action::Entity::find()
                    .select_only()
                    .column(action::Column::Id)
                    .order_by_desc(action::Column::Id)
                    .limit(1)
                    .into_tuple::<String>()
                    .one(&self.connection)
                    .await?;
            }
            if let Some(upper) = &pass.action_upper {
                let ids = action::Entity::find()
                    .select_only()
                    .column(action::Column::Id)
                    .filter(action::Column::Id.gt(pass.action_after.as_deref().unwrap_or("")))
                    .filter(action::Column::Id.lte(upper.clone()))
                    .order_by_asc(action::Column::Id)
                    .limit(64)
                    .into_tuple::<String>()
                    .all(&self.connection)
                    .await?;
                pass.action_done = ids.len() < 64;
                for id in ids {
                    // PK equality bounds the existing three-range policy and
                    // guarded JSON/action provenance checks to this one input.
                    let at = crate::repositories::agent_domain::action_outbox_candidate_query(None)
                        .select_only()
                        .column(action::Column::NextAttemptAt)
                        .filter(action::Column::Id.eq(id.clone()))
                        .into_tuple::<Option<sea_orm::prelude::DateTimeWithTimeZone>>()
                        .one(&self.connection)
                        .await;
                    match at {
                        Ok(Some(Some(at))) => retain_clock(&mut pass.action_next, Some(at)),
                        Ok(Some(None)) => {
                            pass.action_immediate = true;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            pass.failure.get_or_insert(error.into());
                        }
                    }
                    pass.action_after = Some(id);
                }
            } else {
                pass.action_done = true;
            }
        }
        let done = pass.queue_done && pass.action_done;
        if done && let Some(error) = pass.failure.take() {
            return Err(error);
        }
        Ok(done)
    }
}

/// Existing retry owner reconstruction, not claim authority. One raw PK page,
/// then <=32 released policy points and narrowly guarded conditional writes.
#[derive(Default)]
pub struct EffectRetryPass {
    upper: Option<String>,
    after: Option<String>,
    pub next: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    failure: Option<anyhow::Error>,
}
impl crate::CrudStore {
    pub async fn native_terminal_effect_retry_quantum(
        &self,
        pass: &mut EffectRetryPass,
        now_unix: i64,
        limit: u64,
    ) -> Result<(bool, u64)> {
        use crate::repositories::native_terminal_effect_outbox as repository;
        use pioneer_entity::native_terminal_effect_outbox as effect;
        let limit = std::cmp::min(limit, 32);
        if limit == 0 {
            return Ok((true, 0));
        }
        if pass.upper.is_none() {
            pass.upper = effect::Entity::find()
                .select_only()
                .column(effect::Column::EffectId)
                .order_by_desc(effect::Column::EffectId)
                .limit(1)
                .into_tuple::<String>()
                .one(&self.connection)
                .await?;
        }
        let Some(upper) = &pass.upper else {
            return Ok((true, 0));
        };
        let ids = effect::Entity::find()
            .select_only()
            .column(effect::Column::EffectId)
            .filter(effect::Column::EffectId.gt(pass.after.as_deref().unwrap_or("")))
            .filter(effect::Column::EffectId.lte(upper.clone()))
            .order_by_asc(effect::Column::EffectId)
            .limit(limit)
            .into_tuple::<String>()
            .all(&self.connection)
            .await?;
        let done = ids.len() < limit as usize;
        let now = crate::unix_to_datetime(now_unix);
        let mut changed = 0;
        for id in ids {
            pass.after = Some(id.clone());
            let result: Result<()> = async {
                let Some(completed) =
                    repository::retry_clock_by_id(&self.connection, &id, now).await?
                else {
                    return Ok(());
                };
                let due = recovery_ceil(completed + chrono::Duration::hours(1))?;
                if due > now {
                    retain_clock(&mut pass.next, Some(due));
                    return Ok(());
                }
                let ids = vec![id.clone()];
                changed += self
                    .background_changes(
                        &[
                            "recovery.effects",
                            "recovery.effects.retry",
                            "recovery.effects.retention",
                        ],
                        async {
                            self.run_serialized_write(|| {
                                repository::requeue_retryable_unresolved(
                                    &self.connection,
                                    now,
                                    now - chrono::Duration::hours(1),
                                    now - chrono::Duration::hours(24),
                                    1,
                                    &ids,
                                )
                            })
                            .await
                        },
                        |changed| *changed > 0,
                    )
                    .await?;
                Ok(())
            }
            .await;
            if let Err(error) = result {
                pass.failure.get_or_insert(error);
            }
        }
        if done && let Some(error) = pass.failure.take() {
            return Err(error);
        }
        Ok((done, changed))
    }
}
