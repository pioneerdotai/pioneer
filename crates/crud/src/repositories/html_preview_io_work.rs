//! Durable locators only. No payloads, database permits or domain ownership leases.
//! due_at is Unix milliseconds: eligibility for ready/timed, expiry for running.
//! Ordinals and seed_version are nonnegative i64; revisions/incarnations/tokens
//! are opaque tokens (runtime UUIDs), never arithmetic generations. Empty object IDs are reserved
//! for the two global seed families; workspace resync IDs live in those families.
use crate::CrudStore;
use anyhow::{Result, ensure};
use pioneer_entity::{
    artifact, compaction_frozen_history as history, html_preview_frozen_result as result_row,
    html_preview_io_work as work,
};
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    Set, TransactionTrait,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HtmlPreviewIoFamily {
    Witness,
    Copies,
    SeedWitness,
    SeedCopies,
}
impl HtmlPreviewIoFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Witness => "witness",
            Self::Copies => "copies",
            Self::SeedWitness => "seed_witness",
            Self::SeedCopies => "seed_copies",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HtmlPreviewIoState {
    Ready,
    Timed,
    Running,
    Blocked,
    Done,
    Quarantine,
}
impl HtmlPreviewIoState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Timed => "timed",
            Self::Running => "running",
            Self::Blocked => "blocked",
            Self::Done => "done",
            Self::Quarantine => "quarantine",
        }
    }
}
impl TryFrom<&str> for HtmlPreviewIoState {
    type Error = anyhow::Error;
    fn try_from(value: &str) -> Result<Self> {
        Ok(match value {
            "ready" => Self::Ready,
            "timed" => Self::Timed,
            "running" => Self::Running,
            "blocked" => Self::Blocked,
            "done" => Self::Done,
            "quarantine" => Self::Quarantine,
            _ => anyhow::bail!("invalid HTML IO state"),
        })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HtmlPreviewIoErrorClass {
    Transient,
    LocalHold,
    InvalidAuthority,
}
impl HtmlPreviewIoErrorClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::LocalHold => "local_hold",
            Self::InvalidAuthority => "invalid_authority",
        }
    }
}

impl TryFrom<&str> for HtmlPreviewIoErrorClass {
    type Error = anyhow::Error;
    fn try_from(value: &str) -> Result<Self> {
        Ok(match value {
            "transient" => Self::Transient,
            "local_hold" => Self::LocalHold,
            "invalid_authority" => Self::InvalidAuthority,
            _ => anyhow::bail!("invalid HTML IO error class"),
        })
    }
}
/// Prepared outside database capacity; full source IDs are never truncated.
#[derive(Clone, Debug)]
pub struct HtmlPreviewIoLocator {
    pub(crate) source_id: String,
    pub(crate) key: String,
}
impl HtmlPreviewIoLocator {
    pub fn new(id: &str) -> Self {
        use sha2::{Digest, Sha256};
        Self {
            source_id: id.into(),
            key: hex::encode(Sha256::digest(id.as_bytes())),
        }
    }
}
#[derive(Clone, Debug)]
pub struct HtmlPreviewIoClaim {
    pub row: work::Model,
}
impl HtmlPreviewIoClaim {
    pub fn source_id(&self) -> &str {
        if self.row.family == "witness" || self.row.family == "copies" {
            self.row.cursor_id.as_deref().unwrap_or_default()
        } else {
            &self.row.object_id
        }
    }

    pub(crate) fn has_valid_point_locator(&self) -> bool {
        !self.source_id().is_empty()
            && HtmlPreviewIoLocator::new(self.source_id()).key == self.row.object_id
            && self.row.ordinal >= 0
    }
}
#[derive(Clone, Copy, Debug)]
pub enum HtmlPreviewIoOutcome {
    Retry,
    Blocked,
    Quarantine,
    Done,
}
pub enum HtmlPreviewIoSource<T> {
    Ready(T),
    Retry,
    Obsolete,
    Quarantine,
}
#[derive(Debug)]
pub(crate) struct CasRace;
impl std::fmt::Display for CasRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HTML IO transaction CAS conflict")
    }
}
impl std::error::Error for CasRace {}

// ArtifactCrudError retains a DbErr rather than an anyhow source. Preserve this
// one retry class across that existing repository error boundary; other errors
// (including an unknown commit outcome) must not become automatic CAS retries.
const CAS_DATABASE_CLASS: &str = "html_preview_io_cas_conflict";
pub(crate) fn into_database_error(error: anyhow::Error) -> sea_orm::DbErr {
    if is_conflict(&error) {
        sea_orm::DbErr::Custom(CAS_DATABASE_CLASS.into())
    } else {
        sea_orm::DbErr::Custom(error.to_string())
    }
}
pub(crate) fn is_conflict(error: &anyhow::Error) -> bool {
    error.downcast_ref::<CasRace>().is_some()
        || error.chain().any(|cause| {
            matches!(
                cause.downcast_ref::<sea_orm::DbErr>(),
                Some(sea_orm::DbErr::Custom(class)) if class == CAS_DATABASE_CLASS
            )
        })
}
fn token() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn key(row: &work::Model) -> Condition {
    Condition::all()
        .add(work::Column::Family.eq(&row.family))
        .add(work::Column::ObjectId.eq(&row.object_id))
        .add(work::Column::Ordinal.eq(row.ordinal))
}
pub(crate) fn fence(row: &work::Model) -> Condition {
    key(row)
        .add(work::Column::Incarnation.eq(&row.incarnation))
        .add(work::Column::ClaimToken.eq(row.claim_token.clone()))
        .add(work::Column::PassRevision.eq(&row.pass_revision))
        .add(work::Column::State.eq("running"))
}
/// Must be called inside the authoritative business transaction. A fresh random
/// revision prevents ACK ABA; an active pass, lease and cursor remain untouched.
pub(crate) async fn request_prepared<C: sea_orm::ConnectionTrait>(
    db: &C,
    family: HtmlPreviewIoFamily,
    locator: &HtmlPreviewIoLocator,
    ordinal: i64,
    workspace: &str,
) -> Result<()> {
    request_at(
        db,
        family,
        locator,
        ordinal,
        workspace,
        chrono::Utc::now().timestamp_millis(),
    )
    .await
}

pub(crate) async fn request_at<C: sea_orm::ConnectionTrait>(
    db: &C,
    family: HtmlPreviewIoFamily,
    locator: &HtmlPreviewIoLocator,
    ordinal: i64,
    workspace: &str,
    now_ms: i64,
) -> Result<()> {
    let id = locator.source_id.as_str();
    let service = matches!(
        family,
        HtmlPreviewIoFamily::SeedWitness | HtmlPreviewIoFamily::SeedCopies
    );
    ensure!(service || !id.is_empty(), "empty domain locator");
    ensure!(ordinal >= 0, "negative HTML IO ordinal");

    let name = family.as_str();
    let queue_id = if service && id.is_empty() {
        ""
    } else {
        locator.key.as_str()
    };
    let existing = work::Entity::find_by_id((name.to_owned(), queue_id.to_owned(), ordinal))
        .one(db)
        .await?;
    if let Some(row) = existing {
        ensure!(
            if service {
                row.workspace_id == workspace
            } else {
                row.cursor_id.as_deref() == Some(id)
            },
            "HTML IO locator digest collision"
        );
        let mut update = work::Entity::update_many()
            .filter(key(&row))
            .filter(work::Column::Incarnation.eq(&row.incarnation))
            .filter(work::Column::RequestedRevision.eq(&row.requested_revision))
            .filter(work::Column::State.eq(&row.state))
            .col_expr(work::Column::RequestedRevision, Expr::val(token()))
            .col_expr(work::Column::WorkspaceId, Expr::val(workspace));
        if row.state != "running" {
            update = update
                .col_expr(
                    work::Column::State,
                    Expr::val(HtmlPreviewIoState::Ready.as_str()),
                )
                .col_expr(
                    work::Column::DueAt,
                    Expr::val(if row.state == "ready" {
                        row.due_at.unwrap_or(now_ms).min(now_ms)
                    } else {
                        now_ms
                    }),
                )
                .col_expr(work::Column::ErrorClass, Expr::val(None::<String>));
        }
        if service && (row.state == "done" || row.state == "quarantine") {
            update = update
                .col_expr(work::Column::CursorId, Expr::val(None::<String>))
                .col_expr(work::Column::CursorOrdinal, Expr::val(None::<i64>))
                .col_expr(work::Column::UpperId, Expr::val(None::<String>))
                .col_expr(work::Column::UpperOrdinal, Expr::val(None::<i64>));
        }
        if update.exec(db).await?.rows_affected != 1 {
            return Err(CasRace.into());
        }
    } else {
        work::Entity::insert(work::ActiveModel {
            family: Set(name.into()),
            object_id: Set(queue_id.into()),
            ordinal: Set(ordinal),
            workspace_id: Set(workspace.into()),
            incarnation: Set(token()),
            requested_revision: Set(token()),
            pass_revision: Set(String::new()),
            state: Set(HtmlPreviewIoState::Ready.as_str().into()),
            due_at: Set(Some(now_ms)),
            claim_token: Set(None),
            cursor_id: Set(if service { None } else { Some(id.into()) }),
            cursor_ordinal: Set(None),
            upper_id: Set(None),
            upper_ordinal: Set(None),
            seed_version: Set(0),
            error_class: Set(None),
        })
        .exec_without_returning(db)
        .await
        .map_err(|error| -> anyhow::Error {
            if matches!(
                error.sql_err(),
                Some(sea_orm::SqlErr::UniqueConstraintViolation(_))
            ) {
                CasRace.into()
            } else {
                error.into()
            }
        })?;
    }
    Ok(())
}
// A conditional ACK lost after a matching read can be an RR snapshot race.
// Retry the owning short transaction, releasing its snapshot first.
fn checked_ack(rows: u64) -> Result<bool> {
    if rows == 1 {
        Ok(true)
    } else {
        Err(CasRace.into())
    }
}
pub(crate) async fn ack<C: sea_orm::ConnectionTrait>(
    db: &C,
    claim: &HtmlPreviewIoClaim,
) -> Result<bool> {
    let row = &claim.row;
    let current =
        work::Entity::find_by_id((row.family.clone(), row.object_id.clone(), row.ordinal))
            .one(db)
            .await?;
    let Some(current) = current else {
        return Ok(false);
    };
    if current.incarnation != row.incarnation
        || current.claim_token != row.claim_token
        || current.pass_revision != row.pass_revision
        || current.state != "running"
    {
        return Ok(false);
    }
    if current.requested_revision != row.pass_revision {
        return checked_ack(
            work::Entity::update_many()
                .filter(fence(row))
                .filter(work::Column::RequestedRevision.eq(&current.requested_revision))
                .col_expr(
                    work::Column::State,
                    Expr::val(HtmlPreviewIoState::Ready.as_str()),
                )
                .col_expr(
                    work::Column::DueAt,
                    Expr::val(chrono::Utc::now().timestamp_millis()),
                )
                .col_expr(work::Column::ClaimToken, Expr::val(None::<String>))
                .col_expr(
                    work::Column::CursorId,
                    Expr::val(if row.family.starts_with("seed_") {
                        None
                    } else {
                        row.cursor_id.clone()
                    }),
                )
                .col_expr(work::Column::CursorOrdinal, Expr::val(None::<i64>))
                .col_expr(work::Column::UpperId, Expr::val(None::<String>))
                .col_expr(work::Column::UpperOrdinal, Expr::val(None::<i64>))
                .exec(db)
                .await?
                .rows_affected,
        );
    }
    if row.family.starts_with("seed_") {
        checked_ack(
            work::Entity::update_many()
                .filter(fence(row))
                .filter(work::Column::RequestedRevision.eq(&row.pass_revision))
                .col_expr(
                    work::Column::State,
                    Expr::val(HtmlPreviewIoState::Done.as_str()),
                )
                .col_expr(work::Column::DueAt, Expr::val(None::<i64>))
                .col_expr(work::Column::SeedVersion, Expr::val(1_i64))
                .exec(db)
                .await?
                .rows_affected,
        )
    } else {
        checked_ack(
            work::Entity::delete_many()
                .filter(fence(row))
                .filter(work::Column::RequestedRevision.eq(&row.pass_revision))
                .exec(db)
                .await?
                .rows_affected,
        )
    }
}
impl CrudStore {
    pub async fn claim_html_preview_io_work(
        &self,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<HtmlPreviewIoClaim>> {
        self.claim_html_preview_io_work_kind(now_ms, lease_ms, false)
            .await
    }
    async fn claim_html_preview_io_work_kind(
        &self,
        now_ms: i64,
        lease_ms: i64,
        seed_only: bool,
    ) -> Result<Option<HtmlPreviewIoClaim>> {
        ensure!(lease_ms > 0, "invalid HTML IO lease");
        let Some(candidate) = work::Entity::find()
            .filter(work::Column::DueAt.lte(now_ms))
            .filter(work::Column::State.is_in(["ready", "timed", "running"]))
            .filter(if seed_only {
                Condition::any()
                    .add(work::Column::Family.eq("seed_witness"))
                    .add(work::Column::Family.eq("seed_copies"))
            } else {
                Condition::all()
            })
            .order_by_asc(work::Column::DueAt)
            .order_by_asc(work::Column::Family)
            .order_by_asc(work::Column::ObjectId)
            .order_by_asc(work::Column::Ordinal)
            .one(&self.connection)
            .await?
        else {
            return Ok(None);
        };
        HtmlPreviewIoState::try_from(candidate.state.as_str())?;
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let mut update = work::Entity::update_many()
                .filter(key(&candidate))
                .filter(work::Column::Incarnation.eq(&candidate.incarnation))
                .filter(work::Column::RequestedRevision.eq(&candidate.requested_revision))
                .filter(work::Column::State.eq(&candidate.state))
                .filter(work::Column::DueAt.lte(now_ms))
                .col_expr(
                    work::Column::State,
                    Expr::val(HtmlPreviewIoState::Running.as_str()),
                )
                .col_expr(work::Column::ClaimToken, Expr::val(token()))
                .col_expr(
                    work::Column::DueAt,
                    Expr::val(now_ms.saturating_add(lease_ms)),
                );
            // A reclaimed in-progress pass preserves its immutable frontier.
            if !candidate.family.starts_with("seed_") {
                update = update.col_expr(
                    work::Column::PassRevision,
                    Expr::val(candidate.requested_revision.clone()),
                );
            }
            if update.exec(&tx).await?.rows_affected != 1 {
                tx.rollback().await?;
                anyhow::bail!("HTML IO claim contention requires fresh discovery");
            }
            let row = work::Entity::find_by_id((
                candidate.family.clone(),
                candidate.object_id.clone(),
                candidate.ordinal,
            ))
            .one(&tx)
            .await?
            .expect("claimed row");
            tx.commit().await?;
            Ok(Some(HtmlPreviewIoClaim { row }))
        })
        .await
    }
    pub async fn html_preview_io_seed_startup_quantum(&self) -> Result<bool> {
        if self.html_preview_io_seed_complete().await? {
            return Ok(false);
        }
        if let Some(claim) = self
            .claim_html_preview_io_work_kind(chrono::Utc::now().timestamp_millis(), 300_000, true)
            .await?
        {
            return self.html_preview_io_seed_quantum(&claim).await;
        }
        Ok(false)
    }
    pub async fn defer_html_preview_io_work(
        &self,
        claim: &HtmlPreviewIoClaim,
        outcome: HtmlPreviewIoOutcome,
        now_ms: i64,
    ) -> Result<bool> {
        let obsolete = if matches!(outcome, HtmlPreviewIoOutcome::Done) {
            Some(self.prepare_html_preview_io_obsolete(claim).await?)
        } else {
            None
        };
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            if matches!(outcome, HtmlPreviewIoOutcome::Done) {
                if !obsolete
                    .as_ref()
                    .expect("prepared proof")
                    .revalidate(&tx)
                    .await?
                {
                    tx.rollback().await?;
                    return Ok(false);
                }
                let changed = ack(&tx, claim).await?;
                tx.commit().await?;
                return Ok(changed);
            }
            let (state, due, error) = match outcome {
                HtmlPreviewIoOutcome::Retry => (
                    HtmlPreviewIoState::Timed.as_str(),
                    Some(now_ms.saturating_add(60_000)),
                    Some(HtmlPreviewIoErrorClass::Transient.as_str()),
                ),
                HtmlPreviewIoOutcome::Blocked => (
                    HtmlPreviewIoState::Blocked.as_str(),
                    None,
                    Some(HtmlPreviewIoErrorClass::LocalHold.as_str()),
                ),
                HtmlPreviewIoOutcome::Quarantine => (
                    HtmlPreviewIoState::Quarantine.as_str(),
                    None,
                    Some(HtmlPreviewIoErrorClass::InvalidAuthority.as_str()),
                ),
                HtmlPreviewIoOutcome::Done => unreachable!(),
            };
            let changed = work::Entity::update_many()
                .filter(fence(&claim.row))
                .filter(work::Column::RequestedRevision.eq(&claim.row.requested_revision))
                .col_expr(work::Column::State, Expr::val(state))
                .col_expr(work::Column::DueAt, Expr::val(due))
                .col_expr(work::Column::ErrorClass, Expr::val(error))
                .exec(&tx)
                .await?
                .rows_affected
                == 1;
            // A producer racing a failed preparation must remain immediately due.
            if !changed {
                work::Entity::update_many()
                    .filter(fence(&claim.row))
                    .col_expr(
                        work::Column::State,
                        Expr::val(HtmlPreviewIoState::Ready.as_str()),
                    )
                    .col_expr(
                        work::Column::DueAt,
                        Expr::val(chrono::Utc::now().timestamp_millis()),
                    )
                    .exec(&tx)
                    .await?;
            }
            tx.commit().await?;
            Ok(changed)
        })
        .await
    }
    /// Typed repair request belongs in the repair's transaction, before commit.
    pub async fn request_html_preview_io_resync<C: sea_orm::ConnectionTrait>(
        &self,
        db: &C,
        workspace: &HtmlPreviewIoLocator,
        families: &[HtmlPreviewIoFamily],
    ) -> Result<()> {
        for family in families {
            let seed = match family {
                HtmlPreviewIoFamily::Witness => HtmlPreviewIoFamily::SeedWitness,
                HtmlPreviewIoFamily::Copies => HtmlPreviewIoFamily::SeedCopies,
                _ => anyhow::bail!("expected point family"),
            };
            ensure!(!workspace.source_id.is_empty(), "empty repair workspace");
            request_prepared(db, seed, workspace, 0, &workspace.source_id).await?;
        }
        Ok(())
    }
    pub async fn html_preview_io_seed_complete(&self) -> Result<bool> {
        for family in ["seed_witness", "seed_copies"] {
            let marker = work::Entity::find_by_id((family.to_owned(), String::new(), 0_i64))
                .select_only()
                .column(work::Column::SeedVersion)
                .column(work::Column::State)
                .into_tuple::<(i64, String)>()
                .one(&self.connection)
                .await?;
            if marker.is_none_or(|(version, state)| version != 1 || state != "done") {
                return Ok(false);
            }
        }
        Ok(true)
    }
    /// Bounded traversal of the queue, including quarantined/terminal records,
    /// avoids an unindexed search through an arbitrarily large parked set.
    pub async fn recover_html_preview_io_blocked(
        &self,
        after: &(String, String, i64),
    ) -> Result<Option<(String, String, i64)>> {
        // Full point locators/workspaces are TEXT; the recovery sweep needs
        // only fixed-width queue keys and fences, never those long values.
        let rows = work::Entity::find()
            .select_only()
            .column(work::Column::Family)
            .column(work::Column::ObjectId)
            .column(work::Column::Ordinal)
            .column(work::Column::State)
            .column(work::Column::Incarnation)
            .filter(after_key(after))
            .order_by_asc(work::Column::Family)
            .order_by_asc(work::Column::ObjectId)
            .order_by_asc(work::Column::Ordinal)
            .limit(16)
            .into_tuple::<(String, String, i64, String, String)>()
            .all(&self.connection)
            .await?;
        let next = rows
            .last()
            .map(|(family, id, ordinal, _, _)| (family.clone(), id.clone(), *ordinal));
        for (family, id, ordinal, state, incarnation) in rows {
            if state != "blocked" {
                continue;
            }
            work::Entity::update_many()
                .filter(work::Column::Family.eq(family))
                .filter(work::Column::ObjectId.eq(id))
                .filter(work::Column::Ordinal.eq(ordinal))
                .filter(work::Column::Incarnation.eq(incarnation))
                .filter(work::Column::State.eq(HtmlPreviewIoState::Blocked.as_str()))
                .col_expr(
                    work::Column::State,
                    Expr::val(HtmlPreviewIoState::Ready.as_str()),
                )
                .col_expr(
                    work::Column::DueAt,
                    Expr::val(chrono::Utc::now().timestamp_millis()),
                )
                .exec(&self.connection)
                .await?;
        }
        Ok(next)
    }
}
fn after_key(after: &(String, String, i64)) -> Condition {
    Condition::any()
        .add(work::Column::Family.gt(&after.0))
        .add(
            Condition::all()
                .add(work::Column::Family.eq(&after.0))
                .add(work::Column::ObjectId.gt(&after.1)),
        )
        .add(
            Condition::all()
                .add(work::Column::Family.eq(&after.0))
                .add(work::Column::ObjectId.eq(&after.1))
                .add(work::Column::Ordinal.gt(after.2)),
        )
}
impl CrudStore {
    /// One source record, at most 1 MiB metadata plus a lossless legacy key.
    /// A long TEXT key is handled alone; no byte filter skips source IDs.
    /// Metadata preparation happens on the reader before the fenced writer.
    pub async fn html_preview_io_seed_quantum(&self, claim: &HtmlPreviewIoClaim) -> Result<bool> {
        let witness = claim.row.family == "seed_witness";
        ensure!(witness || claim.row.family == "seed_copies", "not a seed");
        let mut row = claim.row.clone();
        if row.upper_id.is_none() {
            let initialized = self
                .run_serialized_write(|| async {
                    let tx = self.connection.begin().await?;
                    let Some(current) = work::Entity::find()
                        .filter(fence(&row))
                        .filter(work::Column::UpperId.is_null())
                        .lock_exclusive()
                        .one(&tx)
                        .await?
                    else {
                        tx.rollback().await?;
                        return Ok(None);
                    };
                    let upper = if witness {
                        result_row::Entity::find()
                            .select_only()
                            .column(result_row::Column::ManifestId)
                            .column(result_row::Column::Ordinal)
                            .order_by_desc(result_row::Column::ManifestId)
                            .order_by_desc(result_row::Column::Ordinal)
                            .into_tuple::<(String, i64)>()
                            .one(&tx)
                            .await?
                            .unwrap_or_default()
                    } else {
                        (
                            artifact::Entity::find()
                                .select_only()
                                .column(artifact::Column::Id)
                                .order_by_desc(artifact::Column::Id)
                                .into_tuple::<String>()
                                .one(&tx)
                                .await?
                                .unwrap_or_default(),
                            0,
                        )
                    };
                    if work::Entity::update_many()
                        .filter(fence(&row))
                        .filter(work::Column::UpperId.is_null())
                        .filter(work::Column::RequestedRevision.eq(&current.requested_revision))
                        .col_expr(
                            work::Column::PassRevision,
                            Expr::val(current.requested_revision.clone()),
                        )
                        .col_expr(work::Column::UpperId, Expr::val(upper.0.clone()))
                        .col_expr(work::Column::UpperOrdinal, Expr::val(upper.1))
                        .exec(&tx)
                        .await?
                        .rows_affected
                        != 1
                    {
                        tx.rollback().await?;
                        return Ok(None);
                    }
                    tx.commit().await?;
                    Ok(Some((upper, current.requested_revision)))
                })
                .await?;
            let Some(((id, ordinal), revision)) = initialized else {
                return Ok(false);
            };
            row.pass_revision = revision.clone();
            row.requested_revision = revision;
            row.upper_id = Some(id);
            row.upper_ordinal = Some(ordinal);
        }
        let after = row.cursor_id.clone().unwrap_or_default();
        let ordinal = row.cursor_ordinal.unwrap_or(-1);
        let upper = row.upper_id.as_deref().expect("frontier");
        let mut source = None;
        if witness {
            let key = result_row::Entity::find()
                .select_only()
                .column(result_row::Column::ManifestId)
                .column(result_row::Column::Ordinal)
                .filter(
                    Condition::any()
                        .add(result_row::Column::ManifestId.gt(&after))
                        .add(
                            Condition::all()
                                .add(result_row::Column::ManifestId.eq(&after))
                                .add(result_row::Column::Ordinal.gt(ordinal)),
                        ),
                )
                .filter(
                    Condition::any()
                        .add(result_row::Column::ManifestId.lt(upper))
                        .add(
                            Condition::all()
                                .add(result_row::Column::ManifestId.eq(upper))
                                .add(
                                    result_row::Column::Ordinal.lte(row.upper_ordinal.unwrap_or(0)),
                                ),
                        ),
                )
                .order_by_asc(result_row::Column::ManifestId)
                .order_by_asc(result_row::Column::Ordinal)
                .limit(1)
                .into_tuple::<(String, i64)>()
                .one(&self.connection)
                .await?;
            if let Some((id, n)) = key {
                let header = history::Entity::find_by_id(&id)
                    .select_only()
                    .column(history::Column::WorkspaceId)
                    .column(history::Column::Expired)
                    .into_tuple::<(String, i64)>()
                    .one(&self.connection)
                    .await?;
                let digest = result_row::Entity::find_by_id((id.clone(), n))
                    .select_only()
                    .column(result_row::Column::ArchiveWireSha256)
                    .into_tuple::<Option<String>>()
                    .one(&self.connection)
                    .await?;
                let pending = digest.is_some_and(|hash| hash.is_none())
                    && header.as_ref().is_none_or(|(_, expired)| *expired == 0);
                source = Some(SeedSource {
                    id,
                    ordinal: n,
                    workspace: header.map(|(ws, _)| ws).unwrap_or_default(),
                    metadata: None,
                    obligation: pending,
                });
            }
        } else {
            // Traverse PKs without a workspace/metadata search hidden before LIMIT.
            let id = artifact::Entity::find()
                .select_only()
                .column(artifact::Column::Id)
                .filter(artifact::Column::Id.gt(&after))
                .filter(artifact::Column::Id.lte(upper))
                .order_by_asc(artifact::Column::Id)
                .limit(1)
                .into_tuple::<String>()
                .one(&self.connection)
                .await?;
            if let Some(id) = id {
                let prepared = artifact::Entity::find_by_id(&id)
                    .select_only()
                    .column(artifact::Column::WorkspaceId)
                    .column(artifact::Column::MetadataJson)
                    .filter(artifact::Column::CreatedByKind.eq("tool"))
                    .filter(artifact::Column::CreatedByActorId.eq("html_preview"))
                    .filter(artifact::Column::CurrentVersionId.is_null())
                    .filter(
                        io_bytes(
                            self.connection.get_database_backend(),
                            Expr::col(artifact::Column::MetadataJson),
                        )
                        .lte(1048576),
                    )
                    .into_tuple::<(String, String)>()
                    .one(&self.connection)
                    .await?;
                let (workspace, metadata, obligation) = if let Some((ws, metadata)) = prepared {
                    let obligation = serde_json::from_str::<serde_json::Value>(&metadata)
                        .ok()
                        .is_some_and(|m| {
                            m["htmlTool"]["retention"] == "released"
                                && m["htmlTool"]["releasedCopies"]
                                    .as_array()
                                    .is_some_and(|a| !a.is_empty())
                        });
                    (ws, Some(metadata), obligation)
                } else {
                    (String::new(), None, false)
                };
                source = Some(SeedSource {
                    id,
                    ordinal: 0,
                    workspace,
                    metadata,
                    obligation,
                });
            }
        }
        let locator = source.as_ref().map(|s| HtmlPreviewIoLocator::new(&s.id));
        self.run_serialized_write(|| async {
            let tx = self.connection.begin().await?;
            let mut cursor_fence = fence(&row);
            cursor_fence = cursor_fence.add(if let Some(id) = &row.cursor_id {
                work::Column::CursorId.eq(id)
            } else {
                work::Column::CursorId.is_null()
            });
            cursor_fence = cursor_fence.add(work::Column::UpperId.eq(upper));
            cursor_fence = cursor_fence.add(if let Some(ordinal) = row.cursor_ordinal {
                work::Column::CursorOrdinal.eq(ordinal)
            } else {
                work::Column::CursorOrdinal.is_null()
            });
            cursor_fence = cursor_fence.add(work::Column::UpperOrdinal.eq(row.upper_ordinal));

            ensure!(
                work::Entity::find()
                    .filter(cursor_fence.clone())
                    .lock_exclusive()
                    .one(&tx)
                    .await?
                    .is_some(),
                "seed cursor fence lost"
            );
            if let Some(source) = &source {
                if source.obligation
                    && (row.workspace_id.is_empty() || row.workspace_id == source.workspace)
                {
                    if let Some(metadata) = &source.metadata {
                        ensure!(
                            artifact::Entity::find_by_id(&source.id)
                                .select_only()
                                .column(artifact::Column::Id)
                                .filter(artifact::Column::WorkspaceId.eq(&source.workspace))
                                .filter(artifact::Column::MetadataJson.eq(metadata))
                                .filter(artifact::Column::CurrentVersionId.is_null())
                                .lock_exclusive()
                                .into_tuple::<String>()
                                .one(&tx)
                                .await?
                                .is_some(),
                            "seed source changed; retry preparation"
                        );
                    } else {
                        let current = history::Entity::find_by_id(&source.id)
                            .select_only()
                            .column(history::Column::WorkspaceId)
                            .lock_exclusive()
                            .into_tuple::<String>()
                            .one(&tx)
                            .await?
                            .unwrap_or_default();
                        ensure!(current == source.workspace, "seed witness identity changed");
                    }
                    let still_pending = if witness {
                        result_row::Entity::find_by_id((source.id.clone(), source.ordinal))
                            .select_only()
                            .column(result_row::Column::ManifestId)
                            .filter(result_row::Column::ArchiveWireSha256.is_null())
                            .lock_exclusive()
                            .into_tuple::<String>()
                            .one(&tx)
                            .await?
                            .is_some()
                            && history::Entity::find_by_id(&source.id)
                                .select_only()
                                .column(history::Column::Expired)
                                .into_tuple::<i64>()
                                .one(&tx)
                                .await?
                                .is_none_or(|expired| expired == 0)
                    } else {
                        true
                    };
                    if still_pending {
                        request_prepared(
                            &tx,
                            if witness {
                                HtmlPreviewIoFamily::Witness
                            } else {
                                HtmlPreviewIoFamily::Copies
                            },
                            locator.as_ref().expect("prepared locator"),
                            source.ordinal,
                            &source.workspace,
                        )
                        .await?;
                    }
                }
                ensure!(
                    work::Entity::update_many()
                        .filter(cursor_fence)
                        .col_expr(work::Column::CursorId, Expr::val(source.id.clone()))
                        .col_expr(work::Column::CursorOrdinal, Expr::val(source.ordinal))
                        .col_expr(
                            work::Column::State,
                            Expr::val(HtmlPreviewIoState::Ready.as_str())
                        )
                        .col_expr(
                            work::Column::DueAt,
                            Expr::val(chrono::Utc::now().timestamp_millis())
                        )
                        .exec(&tx)
                        .await?
                        .rows_affected
                        == 1,
                    "seed cursor CAS lost"
                );
            } else {
                ensure!(
                    ack(&tx, &HtmlPreviewIoClaim { row: row.clone() }).await?,
                    "seed EOF fence lost"
                );
            }
            tx.commit().await?;
            Ok(true)
        })
        .await
    }
}
struct SeedSource {
    id: String,
    ordinal: i64,
    workspace: String,
    metadata: Option<String>,
    obligation: bool,
}

/// Advisory preparation is revalidated while holding the domain row lock on
/// transactional backends. SQLite's existing serialized writer supplies it.
pub(crate) enum ObsoleteProof {
    MissingWitness(String, i64),
    Witness(String, i64, String),
    Expired(history::Model),
    MissingCopies(String),
    Copies(String, String, Option<String>, String),
}
impl ObsoleteProof {
    async fn revalidate<C: sea_orm::ConnectionTrait>(&self, db: &C) -> Result<bool> {
        Ok(match self {
            Self::MissingWitness(id, ordinal) => {
                result_row::Entity::find_by_id((id.clone(), *ordinal))
                    .one(db)
                    .await?
                    .is_none()
            }
            Self::Witness(id, ordinal, hash) => {
                result_row::Entity::find_by_id((id.clone(), *ordinal))
                    .select_only()
                    .column(result_row::Column::ManifestId)
                    .filter(result_row::Column::ArchiveWireSha256.eq(hash))
                    .lock_exclusive()
                    .into_tuple::<String>()
                    .one(db)
                    .await?
                    .is_some()
            }
            Self::Expired(row) => history::Entity::find_by_id(&row.id)
                .filter(history::Column::WorkspaceId.eq(&row.workspace_id))
                .filter(history::Column::Expired.eq(1))
                .lock_exclusive()
                .one(db)
                .await?
                .is_some(),
            Self::MissingCopies(id) => artifact::Entity::find_by_id(id).one(db).await?.is_none(),
            Self::Copies(id, workspace, current, metadata) => {
                let query = artifact::Entity::find_by_id(id)
                    .filter(artifact::Column::WorkspaceId.eq(workspace))
                    .filter(artifact::Column::MetadataJson.eq(metadata));
                let query = if let Some(version) = current {
                    query.filter(artifact::Column::CurrentVersionId.eq(version))
                } else {
                    query.filter(artifact::Column::CurrentVersionId.is_null())
                };
                query.lock_exclusive().one(db).await?.is_some()
            }
        })
    }
}
impl CrudStore {
    async fn prepare_html_preview_io_obsolete(
        &self,
        claim: &HtmlPreviewIoClaim,
    ) -> Result<ObsoleteProof> {
        let row = &claim.row;
        if row.family == "witness" {
            let source =
                result_row::Entity::find_by_id((claim.source_id().to_owned(), row.ordinal))
                    .select_only()
                    .column(result_row::Column::ArchiveWireSha256)
                    .into_tuple::<Option<String>>()
                    .one(&self.connection)
                    .await?;
            let Some(hash) = source else {
                return Ok(ObsoleteProof::MissingWitness(
                    claim.source_id().to_owned(),
                    row.ordinal,
                ));
            };
            let header = history::Entity::find_by_id(claim.source_id())
                .one(&self.connection)
                .await?;
            ensure!(
                header
                    .as_ref()
                    .is_some_and(|h| h.workspace_id == row.workspace_id),
                "witness ownership changed"
            );
            let header = header.expect("validated header");
            if header.expired == 1 {
                return Ok(ObsoleteProof::Expired(header));
            }
            let Some(hash) = hash else {
                anyhow::bail!("witness still needs IO")
            };
            return Ok(ObsoleteProof::Witness(
                claim.source_id().to_owned(),
                row.ordinal,
                hash,
            ));
        }
        ensure!(row.family == "copies", "only point work can be obsolete");
        // The point loader already bounded this metadata. Repeat the byte guard
        // here because it could have changed since that advisory load.
        let source = artifact::Entity::find_by_id(claim.source_id())
            .select_only()
            .column(artifact::Column::WorkspaceId)
            .column(artifact::Column::CurrentVersionId)
            .column(artifact::Column::MetadataJson)
            .filter(
                io_bytes(
                    self.connection.get_database_backend(),
                    Expr::col(artifact::Column::MetadataJson),
                )
                .lte(1048576),
            )
            .into_tuple::<(String, Option<String>, String)>()
            .one(&self.connection)
            .await?;
        let Some((workspace, current, metadata)) = source else {
            ensure!(
                artifact::Entity::find_by_id(claim.source_id())
                    .select_only()
                    .column(artifact::Column::Id)
                    .into_tuple::<String>()
                    .one(&self.connection)
                    .await?
                    .is_none(),
                "oversized copy metadata"
            );
            return Ok(ObsoleteProof::MissingCopies(claim.source_id().to_owned()));
        };
        ensure!(workspace == row.workspace_id, "copies ownership changed");
        let parsed: serde_json::Value = serde_json::from_str(&metadata)?;
        ensure!(
            current.is_some()
                || parsed["htmlTool"]["retention"] != "released"
                || parsed["htmlTool"]["releasedCopies"]
                    .as_array()
                    .is_none_or(|a| a.is_empty()),
            "copies still need IO"
        );
        Ok(ObsoleteProof::Copies(
            claim.source_id().to_owned(),
            workspace,
            current,
            metadata,
        ))
    }
}

/// SeaQuery has no portable octet-length builder in the installed API.
/// SQLite renders its existing byte-length expression; remote SQL uses the
/// standard OCTET_LENGTH function. No raw statement or bound value is emitted.
pub(super) fn io_bytes(backend: sea_orm::DbBackend, value: Expr) -> Expr {
    if backend == sea_orm::DbBackend::Sqlite {
        super::html_preview_retention::queries::bytes(value)
    } else {
        let length: Expr =
            sea_orm::sea_query::Func::cust(sea_orm::sea_query::Alias::new("octet_length"))
                .args([value])
                .into();
        if backend == sea_orm::DbBackend::Postgres {
            length.cast_as(sea_orm::sea_query::Alias::new("bigint"))
        } else {
            length.cast_as(sea_orm::sea_query::Alias::new("signed"))
        }
    }
}

impl CrudStore {
    /// Retained coalesced hint; callers of transactional repair helpers invoke
    /// notify only after their business transaction has committed and dropped.
    pub fn html_preview_io_work_notification(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.html_preview_io_work.clone()
    }
    pub fn notify_html_preview_io_work(&self) {
        self.html_preview_io_work.notify_one();
    }
}

pub(crate) async fn callback_ticket<C: ConnectionTrait>(
    db: &C,
    family: &str,
    locator: &HtmlPreviewIoLocator,
    ordinal: i64,
) -> Result<Option<work::Model>> {
    let row = work::Entity::find_by_id((family.to_owned(), locator.key.clone(), ordinal))
        .one(db)
        .await?;
    ensure!(
        row.as_ref()
            .is_none_or(|r| r.cursor_id.as_deref() == Some(&locator.source_id)),
        "HTML IO locator digest collision"
    );
    Ok(row)
}
pub(crate) async fn callback_ack<C: ConnectionTrait>(db: &C, row: &work::Model) -> Result<()> {
    // Source CAS has just proved the obligation satisfied under the domain
    // writer. Conditional revision/incarnation deletion rejects a later request.
    let deleted = work::Entity::delete_many()
        .filter(key(row))
        .filter(work::Column::Incarnation.eq(&row.incarnation))
        .filter(work::Column::RequestedRevision.eq(&row.requested_revision))
        .exec(db)
        .await?
        .rows_affected
        == 1;
    if !deleted {
        return Err(CasRace.into());
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn request<C: ConnectionTrait>(
    db: &C,
    family: HtmlPreviewIoFamily,
    id: &str,
    ordinal: i64,
    workspace: &str,
) -> Result<()> {
    request_at(
        db,
        family,
        &HtmlPreviewIoLocator::new(id),
        ordinal,
        workspace,
        0,
    )
    .await
}
