use anyhow::{Context, Result, anyhow};
use pioneer_entity::cli_runtime_pending_request;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbBackend, EntityTrait, ExprTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, Set, Statement,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliRuntimePendingRequestStatus {
    Pending,
    ResponseAccepted,
    Delivering,
    DeliveryFailed,
    Answered,
    Resolved,
    Cancelled,
    Expired,
}

impl CliRuntimePendingRequestStatus {
    pub const fn is_open(self) -> bool {
        matches!(
            self,
            Self::Pending | Self::ResponseAccepted | Self::Delivering | Self::DeliveryFailed
        )
    }

    pub const fn is_terminal(self) -> bool {
        !self.is_open()
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::ResponseAccepted => "response_accepted",
            Self::Delivering => "delivering",
            Self::DeliveryFailed => "delivery_failed",
            Self::Answered => "answered",
            Self::Resolved => "resolved",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "response_accepted" => Ok(Self::ResponseAccepted),
            "delivering" => Ok(Self::Delivering),
            "delivery_failed" => Ok(Self::DeliveryFailed),
            "answered" => Ok(Self::Answered),
            "resolved" => Ok(Self::Resolved),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            other => Err(anyhow!(
                "unknown CLI runtime pending request status `{other}`"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimePendingRequestRecord {
    pub request_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_item_id: Option<String>,
    pub request_kind: String,
    pub payload_json: String,
    pub status: CliRuntimePendingRequestStatus,
    pub response_json: Option<String>,
    pub authorization_binding: Option<CliRuntimeRequestAuthorizationBinding>,
    pub responding_principal_id: Option<String>,
    pub responding_session_id: Option<String>,
    pub response_authorization_revision: Option<i64>,
    pub delivery_attempts: i64,
    pub delivery_error: Option<String>,
    pub response_contains_secret: bool,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub resolved_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeRequestAuthorizationBinding {
    pub initiating_principal_id: String,
    pub initiating_session_id: String,
    pub initiating_session_generation: i64,
    pub authorization_context_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimePendingRequest {
    pub request_id: String,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub native_thread_id: Option<String>,
    pub native_turn_id: Option<String>,
    pub native_item_id: Option<String>,
    pub request_kind: String,
    pub payload_json: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveCliRuntimePendingRequest {
    pub request_id: String,
    pub status: CliRuntimePendingRequestStatus,
    pub response_json: Option<String>,
    pub updated_at: DateTimeWithTimeZone,
    pub resolved_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptCliRuntimePendingRequestResponse {
    pub request_id: String,
    pub response_json: Option<String>,
    pub responding_principal_id: String,
    pub responding_session_id: String,
    pub response_authorization_revision: i64,
    pub response_contains_secret: bool,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionCliRuntimePendingRequestDelivery {
    pub request_id: String,
    pub expected_status: CliRuntimePendingRequestStatus,
    pub status: CliRuntimePendingRequestStatus,
    pub delivery_error: Option<String>,
    pub updated_at: DateTimeWithTimeZone,
    pub resolved_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CliRuntimePendingRequestListFilter {
    pub workspace_id: Option<String>,
    pub runtime_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub status: Option<CliRuntimePendingRequestStatus>,
    pub open_only: bool,
    pub initiating_principal_id: Option<String>,
    pub initiating_session_id: Option<String>,
    /// Exclusive stable keyset cursor `(created_at, request_id)`.
    pub after: Option<(DateTimeWithTimeZone, String)>,
    /// Inclusive frontier captured once before a finite background pass.
    pub through: Option<(DateTimeWithTimeZone, String)>,
    pub limit: Option<u64>,
}

/// Repository-level safety ceiling. Callers that need a complete scan must
/// advance `after`; omitting `limit` never permits an unbounded allocation.
pub const CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX: u64 = 256;

const fn bounded_pending_request_page_limit(requested: Option<u64>) -> u64 {
    let requested = match requested {
        Some(requested) => requested,
        None => CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX,
    };
    if requested < CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX {
        requested
    } else {
        CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX
    }
}

pub async fn create_pending_request<C: ConnectionTrait>(
    db: &C,
    request: NewCliRuntimePendingRequest,
) -> Result<CliRuntimePendingRequestRecord> {
    if let Some(existing) = find_pending_request(db, &request.request_id).await?
        && existing.status != CliRuntimePendingRequestStatus::Pending
    {
        return Ok(existing);
    }

    use cli_runtime_pending_request::Column;
    let request_id = request.request_id.clone();
    cli_runtime_pending_request::Entity::insert(active_pending_request_from_new(request)?)
        .on_conflict(
            OnConflict::column(Column::RequestId)
                .update_columns([
                    Column::RuntimeId,
                    Column::RuntimeKind,
                    Column::WorkspaceId,
                    Column::ThreadId,
                    Column::TurnId,
                    Column::NativeThreadId,
                    Column::NativeTurnId,
                    Column::NativeItemId,
                    Column::RequestKind,
                    Column::PayloadJson,
                    Column::ResponseJson,
                    Column::RespondingPrincipalId,
                    Column::RespondingSessionId,
                    Column::ResponseAuthorizationRevision,
                    Column::DeliveryAttempts,
                    Column::DeliveryError,
                    Column::ResponseContainsSecret,
                    Column::UpdatedAt,
                    Column::ResolvedAt,
                ])
                .action_and_where(
                    Expr::col((cli_runtime_pending_request::Entity, Column::Status))
                        .eq(CliRuntimePendingRequestStatus::Pending.as_str()),
                )
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context("failed to store CLI runtime pending request")?;

    find_pending_request(db, request_id.as_str())
        .await?
        .context("inserted CLI runtime pending request is missing")
}

pub async fn open_pending_request<C: ConnectionTrait>(
    db: &C,
    request: NewCliRuntimePendingRequest,
) -> Result<CliRuntimePendingRequestRecord> {
    create_pending_request(db, request).await
}

/// Check capacity inside the transaction that opens the request.
pub async fn ensure_pending_request_capacity<C: ConnectionTrait>(
    db: &C,
    request_id: &str,
    turn_id: &str,
    max_pending_requests: usize,
) -> Result<()> {
    if max_pending_requests == 0 {
        return Err(anyhow!(
            "execution human-interaction capacity must be greater than zero"
        ));
    }
    // SeaORM has no transaction-scoped advisory lock API. All authorized
    // admissions for this turn hold the same lock until publication commits.
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pg_advisory_xact_lock(hashtextextended($1, $2))",
        [turn_id.into(), 0x7069_6f6e_7065_6e64_i64.into()],
    ))
    .await
    .context("failed to lock CLI runtime pending request capacity")?;
    if find_pending_request(db, request_id).await?.is_some() {
        // Idempotent replay does not consume another slot and must remain
        // repairable when the execution is already at its boundary.
        return Ok(());
    }
    let max_pending_requests = u64::try_from(max_pending_requests)
        .context("execution human-interaction capacity exceeds the durable range")?;
    let open_requests = cli_runtime_pending_request::Entity::find()
        .filter(cli_runtime_pending_request::Column::TurnId.eq(turn_id.to_owned()))
        .filter(cli_runtime_pending_request::Column::Status.is_in([
            CliRuntimePendingRequestStatus::Pending.as_str(),
            CliRuntimePendingRequestStatus::ResponseAccepted.as_str(),
            CliRuntimePendingRequestStatus::Delivering.as_str(),
            CliRuntimePendingRequestStatus::DeliveryFailed.as_str(),
        ]))
        .count(db)
        .await
        .context("failed to count open CLI runtime pending requests")?;
    if open_requests >= max_pending_requests {
        return Err(anyhow!(
            "execution already has the maximum {max_pending_requests} pending human interactions"
        ));
    }
    Ok(())
}

pub async fn find_pending_request<C: ConnectionTrait>(
    db: &C,
    request_id: &str,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    cli_runtime_pending_request::Entity::find_by_id(request_id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime pending request")?
        .map(pending_request_record_from_model)
        .transpose()
}

pub async fn pending_request_frontier_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<(DateTimeWithTimeZone, String)>> {
    use cli_runtime_pending_request::Column;
    Ok(cli_runtime_pending_request::Entity::find()
        .filter(Column::TurnId.eq(turn_id))
        .order_by_desc(Column::CreatedAt)
        .order_by_desc(Column::RequestId)
        .limit(1)
        .one(db)
        .await?
        .map(|row| (row.created_at, row.request_id)))
}

pub async fn list_pending_requests<C: ConnectionTrait>(
    db: &C,
    filter: CliRuntimePendingRequestListFilter,
) -> Result<Vec<CliRuntimePendingRequestRecord>> {
    let mut query = cli_runtime_pending_request::Entity::find();
    if let Some(workspace_id) = filter.workspace_id {
        query = query.filter(cli_runtime_pending_request::Column::WorkspaceId.eq(workspace_id));
    }
    if let Some(runtime_id) = filter.runtime_id {
        query = query.filter(cli_runtime_pending_request::Column::RuntimeId.eq(runtime_id));
    }
    if let Some(thread_id) = filter.thread_id {
        query = query.filter(cli_runtime_pending_request::Column::ThreadId.eq(thread_id));
    }
    if let Some(turn_id) = filter.turn_id {
        query = query.filter(cli_runtime_pending_request::Column::TurnId.eq(turn_id));
    }
    if let Some(status) = filter.status {
        query = query.filter(cli_runtime_pending_request::Column::Status.eq(status.as_str()));
    }
    if filter.open_only {
        query = query.filter(cli_runtime_pending_request::Column::Status.is_in([
            CliRuntimePendingRequestStatus::Pending.as_str(),
            CliRuntimePendingRequestStatus::ResponseAccepted.as_str(),
            CliRuntimePendingRequestStatus::Delivering.as_str(),
            CliRuntimePendingRequestStatus::DeliveryFailed.as_str(),
        ]));
    }
    if let Some(initiating_principal_id) = filter.initiating_principal_id {
        query = query.filter(
            cli_runtime_pending_request::Column::InitiatingPrincipalId.eq(initiating_principal_id),
        );
    }
    if let Some(initiating_session_id) = filter.initiating_session_id {
        query = query.filter(
            cli_runtime_pending_request::Column::InitiatingSessionId.eq(initiating_session_id),
        );
    }
    if let Some((created_at, request_id)) = filter.after {
        query = query.filter(
            Condition::any()
                .add(cli_runtime_pending_request::Column::CreatedAt.gt(created_at.clone()))
                .add(
                    Condition::all()
                        .add(cli_runtime_pending_request::Column::CreatedAt.eq(created_at))
                        .add(cli_runtime_pending_request::Column::RequestId.gt(request_id)),
                ),
        );
    }

    if let Some((created_at, request_id)) = filter.through {
        use cli_runtime_pending_request::Column;
        query = query.filter(
            Expr::tuple([Expr::col(Column::CreatedAt), Expr::col(Column::RequestId)]).lte(
                Expr::tuple([Expr::value(created_at), Expr::value(request_id)]),
            ),
        );
    }

    let limit = bounded_pending_request_page_limit(filter.limit);

    query
        .order_by_asc(cli_runtime_pending_request::Column::CreatedAt)
        .order_by_asc(cli_runtime_pending_request::Column::RequestId)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list CLI runtime pending requests")?
        .into_iter()
        .map(pending_request_record_from_model)
        .collect()
}

pub async fn list_open_pending_requests_for_descendant_threads<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
) -> Result<Vec<CliRuntimePendingRequestRecord>> {
    let statuses = [
        CliRuntimePendingRequestStatus::Pending,
        CliRuntimePendingRequestStatus::ResponseAccepted,
        CliRuntimePendingRequestStatus::Delivering,
        CliRuntimePendingRequestStatus::DeliveryFailed,
    ]
    .map(|status| status.as_str());
    list_open_descendants(
        db,
        workspace_id,
        thread_id,
        statuses,
        CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX,
    )
    .await?
    .into_iter()
    .map(pending_request_record_from_model)
    .collect()
}

pub async fn resolve_pending_request<C: ConnectionTrait>(
    db: &C,
    resolution: ResolveCliRuntimePendingRequest,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    if resolution.status == CliRuntimePendingRequestStatus::Pending {
        return Err(anyhow!(
            "CLI runtime pending request resolution cannot keep pending status"
        ));
    }

    use cli_runtime_pending_request::Column;
    let updated = cli_runtime_pending_request::Entity::update_many()
        .col_expr(Column::Status, Expr::value(resolution.status.as_str()))
        .col_expr(Column::ResponseJson, Expr::value(resolution.response_json))
        .col_expr(Column::UpdatedAt, Expr::value(resolution.updated_at))
        .col_expr(
            Column::ResolvedAt,
            Expr::value(Some(resolution.resolved_at)),
        )
        .filter(Column::RequestId.eq(resolution.request_id.clone()))
        .filter(Column::Status.eq(CliRuntimePendingRequestStatus::Pending.as_str()))
        .exec(db)
        .await
        .context("failed to resolve CLI runtime pending request")?;

    if updated.rows_affected == 0 {
        if let Some(existing) = find_pending_request(db, &resolution.request_id).await? {
            return Err(anyhow!(
                "CLI runtime pending request `{}` is already `{}`",
                existing.request_id,
                existing.status.as_str()
            ));
        }
        return Ok(None);
    }
    find_pending_request(db, resolution.request_id.as_str()).await
}

pub async fn accept_pending_request_response<C: ConnectionTrait>(
    db: &C,
    response: AcceptCliRuntimePendingRequestResponse,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    if response.responding_principal_id.trim().is_empty()
        || response.responding_session_id.trim().is_empty()
        || response.response_authorization_revision < 0
    {
        return Err(anyhow!(
            "CLI runtime pending request responder provenance is invalid"
        ));
    }
    let result = cli_runtime_pending_request::Entity::update_many()
        .col_expr(
            cli_runtime_pending_request::Column::Status,
            sea_orm::sea_query::Expr::value(
                CliRuntimePendingRequestStatus::ResponseAccepted
                    .as_str()
                    .to_owned(),
            ),
        )
        .col_expr(
            cli_runtime_pending_request::Column::ResponseJson,
            sea_orm::sea_query::Expr::value(response.response_json),
        )
        .col_expr(
            cli_runtime_pending_request::Column::RespondingPrincipalId,
            sea_orm::sea_query::Expr::value(Some(response.responding_principal_id)),
        )
        .col_expr(
            cli_runtime_pending_request::Column::RespondingSessionId,
            sea_orm::sea_query::Expr::value(Some(response.responding_session_id)),
        )
        .col_expr(
            cli_runtime_pending_request::Column::ResponseAuthorizationRevision,
            sea_orm::sea_query::Expr::value(Some(response.response_authorization_revision)),
        )
        .col_expr(
            cli_runtime_pending_request::Column::ResponseContainsSecret,
            sea_orm::sea_query::Expr::value(response.response_contains_secret),
        )
        .col_expr(
            cli_runtime_pending_request::Column::DeliveryError,
            sea_orm::sea_query::Expr::value(Option::<String>::None),
        )
        .col_expr(
            cli_runtime_pending_request::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(response.updated_at),
        )
        .filter(cli_runtime_pending_request::Column::RequestId.eq(response.request_id.clone()))
        .filter(
            cli_runtime_pending_request::Column::Status
                .eq(CliRuntimePendingRequestStatus::Pending.as_str()),
        )
        .exec(db)
        .await
        .context("failed to accept CLI runtime pending request response")?;
    if result.rows_affected != 1 {
        return Ok(None);
    }
    find_pending_request(db, response.request_id.as_str()).await
}

pub async fn transition_pending_request_delivery<C: ConnectionTrait>(
    db: &C,
    transition: TransitionCliRuntimePendingRequestDelivery,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    let valid = matches!(
        (transition.expected_status, transition.status),
        (
            CliRuntimePendingRequestStatus::ResponseAccepted
                | CliRuntimePendingRequestStatus::DeliveryFailed,
            CliRuntimePendingRequestStatus::Delivering,
        ) | (
            CliRuntimePendingRequestStatus::ResponseAccepted,
            CliRuntimePendingRequestStatus::DeliveryFailed
                | CliRuntimePendingRequestStatus::Pending,
        ) | (
            CliRuntimePendingRequestStatus::Delivering,
            CliRuntimePendingRequestStatus::DeliveryFailed
                | CliRuntimePendingRequestStatus::Answered
                | CliRuntimePendingRequestStatus::Resolved
                | CliRuntimePendingRequestStatus::Cancelled
                | CliRuntimePendingRequestStatus::Expired
                | CliRuntimePendingRequestStatus::Pending,
        )
    );
    if !valid {
        return Err(anyhow!(
            "invalid CLI runtime request delivery transition `{}` -> `{}`",
            transition.expected_status.as_str(),
            transition.status.as_str()
        ));
    }

    let mut update = cli_runtime_pending_request::Entity::update_many()
        .col_expr(
            cli_runtime_pending_request::Column::Status,
            sea_orm::sea_query::Expr::value(transition.status.as_str().to_owned()),
        )
        .col_expr(
            cli_runtime_pending_request::Column::DeliveryError,
            sea_orm::sea_query::Expr::value(transition.delivery_error.clone()),
        )
        .col_expr(
            cli_runtime_pending_request::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(transition.updated_at),
        )
        .col_expr(
            cli_runtime_pending_request::Column::ResolvedAt,
            sea_orm::sea_query::Expr::value(transition.resolved_at),
        );
    if transition.status == CliRuntimePendingRequestStatus::Delivering {
        update = update.col_expr(
            cli_runtime_pending_request::Column::DeliveryAttempts,
            sea_orm::sea_query::Expr::col(cli_runtime_pending_request::Column::DeliveryAttempts)
                .add(1),
        );
    }
    if transition.status == CliRuntimePendingRequestStatus::Pending {
        update = update
            .col_expr(
                cli_runtime_pending_request::Column::ResponseJson,
                sea_orm::sea_query::Expr::value(Option::<String>::None),
            )
            .col_expr(
                cli_runtime_pending_request::Column::RespondingPrincipalId,
                sea_orm::sea_query::Expr::value(Option::<String>::None),
            )
            .col_expr(
                cli_runtime_pending_request::Column::RespondingSessionId,
                sea_orm::sea_query::Expr::value(Option::<String>::None),
            )
            .col_expr(
                cli_runtime_pending_request::Column::ResponseAuthorizationRevision,
                sea_orm::sea_query::Expr::value(Option::<i64>::None),
            )
            .col_expr(
                cli_runtime_pending_request::Column::ResponseContainsSecret,
                sea_orm::sea_query::Expr::value(false),
            );
    }
    let result = update
        .filter(cli_runtime_pending_request::Column::RequestId.eq(transition.request_id.clone()))
        .filter(cli_runtime_pending_request::Column::Status.eq(transition.expected_status.as_str()))
        .exec(db)
        .await
        .context("failed to transition CLI runtime pending request delivery")?;
    if result.rows_affected != 1 {
        return Ok(None);
    }
    find_pending_request(db, transition.request_id.as_str()).await
}

pub async fn cancel_pending_request<C: ConnectionTrait>(
    db: &C,
    request_id: String,
    response_json: Option<String>,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    resolve_pending_request(
        db,
        ResolveCliRuntimePendingRequest {
            request_id,
            status: CliRuntimePendingRequestStatus::Cancelled,
            response_json,
            updated_at,
            resolved_at: updated_at,
        },
    )
    .await
}

pub async fn expire_pending_request<C: ConnectionTrait>(
    db: &C,
    request_id: String,
    response_json: Option<String>,
    updated_at: DateTimeWithTimeZone,
) -> Result<Option<CliRuntimePendingRequestRecord>> {
    resolve_pending_request(
        db,
        ResolveCliRuntimePendingRequest {
            request_id,
            status: CliRuntimePendingRequestStatus::Expired,
            response_json,
            updated_at,
            resolved_at: updated_at,
        },
    )
    .await
}

fn active_pending_request_from_new(
    request: NewCliRuntimePendingRequest,
) -> Result<cli_runtime_pending_request::ActiveModel> {
    Ok(cli_runtime_pending_request::ActiveModel {
        request_id: Set(request.request_id),
        runtime_id: Set(request.runtime_id),
        runtime_kind: Set(request.runtime_kind),
        workspace_id: Set(request.workspace_id),
        thread_id: Set(request.thread_id),
        turn_id: Set(request.turn_id),
        native_thread_id: Set(request.native_thread_id),
        native_turn_id: Set(request.native_turn_id),
        native_item_id: Set(request.native_item_id),
        request_kind: Set(request.request_kind),
        payload_json: Set(serde_json::from_str(&request.payload_json)?),
        status: Set(CliRuntimePendingRequestStatus::Pending.as_str().to_owned()),
        response_json: Set(None),
        initiating_principal_id: Set(None),
        initiating_session_id: Set(None),
        initiating_session_generation: Set(None),
        authorization_context_fingerprint: Set(None),
        responding_principal_id: Set(None),
        responding_session_id: Set(None),
        response_authorization_revision: Set(None),
        delivery_attempts: Set(0),
        delivery_error: Set(None),
        response_contains_secret: Set(false),
        created_at: Set(request.created_at),
        updated_at: Set(request.updated_at),
        resolved_at: Set(None),
    })
}

pub async fn bind_pending_request_authorization<C: ConnectionTrait>(
    db: &C,
    request_id: &str,
    binding: &CliRuntimeRequestAuthorizationBinding,
) -> Result<CliRuntimePendingRequestRecord> {
    if binding.initiating_principal_id.trim().is_empty()
        || binding.initiating_session_id.trim().is_empty()
        || binding.initiating_session_generation < 0
        || binding.authorization_context_fingerprint.len() != 64
        || !binding
            .authorization_context_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(anyhow!(
            "CLI runtime pending request authorization binding is invalid"
        ));
    }
    let current = find_pending_request(db, request_id)
        .await?
        .context("CLI runtime pending request to authorize is missing")?;
    if current.status != CliRuntimePendingRequestStatus::Pending {
        return Err(anyhow!(
            "CLI runtime pending request authorization cannot bind terminal request"
        ));
    }
    if let Some(existing) = current.authorization_binding {
        if existing == *binding {
            return find_pending_request(db, request_id)
                .await?
                .context("authorized CLI runtime pending request is missing");
        }
        return Err(anyhow!(
            "CLI runtime pending request is bound to different initiating authority"
        ));
    }
    cli_runtime_pending_request::Entity::update(cli_runtime_pending_request::ActiveModel {
        request_id: Set(request_id.to_owned()),
        initiating_principal_id: Set(Some(binding.initiating_principal_id.clone())),
        initiating_session_id: Set(Some(binding.initiating_session_id.clone())),
        initiating_session_generation: Set(Some(binding.initiating_session_generation)),
        authorization_context_fingerprint: Set(Some(
            binding.authorization_context_fingerprint.clone(),
        )),
        ..Default::default()
    })
    .exec(db)
    .await
    .context("failed to bind CLI runtime pending request authorization")?;
    find_pending_request(db, request_id)
        .await?
        .context("authorized CLI runtime pending request is missing")
}

fn pending_request_record_from_model(
    model: cli_runtime_pending_request::Model,
) -> Result<CliRuntimePendingRequestRecord> {
    let authorization_binding = match (
        model.initiating_principal_id,
        model.initiating_session_id,
        model.initiating_session_generation,
        model.authorization_context_fingerprint,
    ) {
        (None, None, None, None) => None,
        (
            Some(initiating_principal_id),
            Some(initiating_session_id),
            Some(initiating_session_generation),
            Some(authorization_context_fingerprint),
        ) if initiating_session_generation >= 0
            && authorization_context_fingerprint.len() == 64
            && authorization_context_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()) =>
        {
            Some(CliRuntimeRequestAuthorizationBinding {
                initiating_principal_id,
                initiating_session_id,
                initiating_session_generation,
                authorization_context_fingerprint,
            })
        }
        _ => {
            return Err(anyhow!(
                "CLI runtime pending request authorization binding is incomplete or invalid"
            ));
        }
    };
    Ok(CliRuntimePendingRequestRecord {
        request_id: model.request_id,
        runtime_id: model.runtime_id,
        runtime_kind: model.runtime_kind,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        native_thread_id: model.native_thread_id,
        native_turn_id: model.native_turn_id,
        native_item_id: model.native_item_id,
        request_kind: model.request_kind,
        payload_json: model.payload_json.to_string(),
        status: CliRuntimePendingRequestStatus::from_db(model.status.as_str())?,
        response_json: model.response_json,
        authorization_binding,
        responding_principal_id: model.responding_principal_id,
        responding_session_id: model.responding_session_id,
        response_authorization_revision: model.response_authorization_revision,
        delivery_attempts: model.delivery_attempts,
        delivery_error: model.delivery_error,
        response_contains_secret: model.response_contains_secret,
        created_at: model.created_at,
        updated_at: model.updated_at,
        resolved_at: model.resolved_at,
    })
}

async fn list_open_descendants<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    statuses: [&str; 4],
    limit: u64,
) -> Result<Vec<cli_runtime_pending_request::Model>> {
    let limit = i64::try_from(limit).context("pending request page limit exceeds integer range")?;
    // SeaORM has no recursive union on Entity::find. Keep subtree traversal in
    // a single indexed query and decode through the generated Entity.
    let statement = Statement::from_sql_and_values(
        DbBackend::Postgres,
        r#"
WITH RECURSIVE descendant_threads(thread_id) AS (
    SELECT child_thread_id FROM thread_lineage WHERE parent_thread_id = $1
    UNION
    SELECT lineage.child_thread_id
    FROM thread_lineage AS lineage
    JOIN descendant_threads AS descendant ON lineage.parent_thread_id = descendant.thread_id
)
SELECT pending.*
FROM cli_runtime_pending_request AS pending
JOIN descendant_threads AS descendant ON descendant.thread_id = pending.thread_id
WHERE pending.workspace_id = $2 AND pending.status IN ($3, $4, $5, $6)
ORDER BY pending.created_at ASC, pending.request_id ASC
LIMIT $7
"#,
        [
            thread_id.into(),
            workspace_id.into(),
            statuses[0].into(),
            statuses[1].into(),
            statuses[2].into(),
            statuses[3].into(),
            limit.into(),
        ],
    );
    cli_runtime_pending_request::Entity::find()
        .from_raw_sql(statement)
        .all(db)
        .await
        .context("failed to list open CLI runtime pending requests for descendant threads")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_request_repository_limit_is_always_server_bounded() {
        assert_eq!(
            bounded_pending_request_page_limit(None),
            CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX
        );
        assert_eq!(bounded_pending_request_page_limit(Some(7)), 7);
        assert_eq!(
            bounded_pending_request_page_limit(Some(
                CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX.saturating_add(1)
            )),
            CLI_RUNTIME_PENDING_REQUEST_PAGE_MAX
        );
    }
}
