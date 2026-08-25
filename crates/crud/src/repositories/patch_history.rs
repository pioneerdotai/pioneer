use anyhow::{Context, Result};
use chrono::Utc;
use pioneer_entity::{
    applied_patch_change_index, applied_patch_record, codex_aggregate_state, patch_commit_intent,
    patch_commit_terminal, patch_snapshot, patch_snapshot_reservation, turn_diff_state,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{
    Alias, ColumnDef, ExplainStatement, Expr, ExprTrait, Func, Index, OnConflict, Order, Query,
    SqliteQueryBuilder, Table,
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DbBackend, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, QueryTrait, Select, Set, Statement, StatementBuilder,
};

pub type TurnDiffStateRow = turn_diff_state::Model;
pub type CodexAggregateStateRow = codex_aggregate_state::Model;
pub type AppliedPatchRecordRow = applied_patch_record::Model;
pub type AppliedPatchChangeIndexRow = applied_patch_change_index::Model;
pub type PatchCommitIntentRow = patch_commit_intent::Model;
pub type PatchCommitTerminalRow = patch_commit_terminal::Model;
pub type PatchSnapshotRow = patch_snapshot::Model;
pub type PatchSnapshotReservationRow = patch_snapshot_reservation::Model;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnDiffStateWrite {
    pub thread_id: String,
    pub turn_id: String,
    pub revision: i64,
    pub authority: String,
    pub exact: i64,
    pub coverage_json: String,
    pub applied_through_ordinal: Option<i64>,
    pub record_count: i64,
    pub final_state: i64,
    pub state_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodexAggregateStateWrite {
    pub thread_id: String,
    pub turn_id: String,
    pub revision: i64,
    pub final_state: i64,
    pub state_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedPatchRecordWrite {
    pub id: String,
    pub schema_version: i64,
    pub thread_id: String,
    pub turn_id: String,
    pub invocation_id: String,
    pub environment_id: String,
    pub commit_ordinal: i64,
    pub authority: String,
    pub provenance: String,
    pub exactness: String,
    pub committed_at_unix_ms: i64,
    pub plan_fingerprint: Vec<u8>,
    pub outcome_json: String,
    pub changes_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppliedPatchChangeIndexWrite {
    pub record_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub invocation_id: String,
    pub environment_id: String,
    pub commit_ordinal: i64,
    pub sequence: i64,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub change_json: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchChangeIndexCursor {
    pub environment_id: String,
    pub turn_id: String,
    pub commit_ordinal: i64,
    pub sequence: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchCommitIntentWrite {
    pub thread_id: String,
    pub turn_id: String,
    pub invocation_id: String,
    pub commit_ordinal: i64,
    pub plan_fingerprint: Vec<u8>,
    pub operations_json: String,
    pub recovery_json: String,
    pub progress_json: String,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchCommitTerminalWrite {
    pub thread_id: String,
    pub turn_id: String,
    pub invocation_id: String,
    pub commit_ordinal: i64,
    pub plan_fingerprint: Vec<u8>,
    pub operations_json: String,
    pub authority: String,
    pub status: String,
    pub record_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingPatchIntentCursor {
    pub updated_at: DateTimeWithTimeZone,
    pub thread_id: String,
    pub turn_id: String,
    pub invocation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchOrdinalStatusRow {
    pub turn_id: String,
    pub commit_ordinal: i64,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchSnapshotWrite {
    pub domain_id: String,
    pub content_hash: Vec<u8>,
    pub byte_len: i64,
    pub encoding: String,
    pub line_endings_json: String,
    pub compressed_bytes: Vec<u8>,
    pub raw_byte_len: i64,
    pub ref_count: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PatchSnapshotSizes {
    pub compressed_bytes: i64,
    pub encoding_bytes: i64,
    pub line_endings_bytes: i64,
    pub raw_byte_len: i64,
    pub ref_count: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PatchSnapshotTotals {
    pub logical_bytes: i64,
    pub physical_bytes: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PatchSnapshotMetricsRow {
    pub blobs: u64,
    pub logical_bytes: i64,
    pub physical_bytes: i64,
    pub references: i64,
    pub referenced_logical_bytes: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchSnapshotKeyRow {
    pub domain_id: String,
    pub content_hash: Vec<u8>,
    pub byte_len: i64,
    pub ref_count: i64,
    pub physical_bytes: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchSnapshotKeyCursor {
    pub domain_id: String,
    pub content_hash: Vec<u8>,
    pub byte_len: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PatchSnapshotReservationCursor {
    pub thread_id: String,
    pub turn_id: String,
    pub invocation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpectedPatchSnapshotReferenceRow {
    pub domain_id: String,
    pub content_hash: Vec<u8>,
    pub byte_len: i64,
    pub ref_count: i64,
}

const SNAPSHOT_EXPECTED_TABLE: &str = "patch_snapshot_expected_reference";
const SNAPSHOT_EXPECTED_DOMAIN_ID: &str = "domain_id";
const SNAPSHOT_EXPECTED_CONTENT_HASH: &str = "content_hash";
const SNAPSHOT_EXPECTED_BYTE_LEN: &str = "byte_len";
const SNAPSHOT_EXPECTED_REF_COUNT: &str = "ref_count";

pub async fn find_patch_snapshot_reservation<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    invocation_id: &str,
) -> Result<Option<PatchSnapshotReservationRow>> {
    patch_snapshot_reservation::Entity::find_by_id((
        thread_id.to_owned(),
        turn_id.to_owned(),
        invocation_id.to_owned(),
    ))
    .one(db)
    .await
    .context("failed to load patch snapshot reservation")
}

pub async fn insert_patch_snapshot_reservation<C: ConnectionTrait>(
    db: &C,
    thread_id: String,
    turn_id: String,
    invocation_id: String,
    logical_bytes: i64,
    physical_bytes: i64,
) -> Result<()> {
    patch_snapshot_reservation::Entity::insert(patch_snapshot_reservation::ActiveModel {
        thread_id: Set(thread_id),
        turn_id: Set(turn_id),
        invocation_id: Set(invocation_id),
        logical_bytes: Set(logical_bytes),
        physical_bytes: Set(physical_bytes),
        created_at: Set(Utc::now().fixed_offset()),
    })
    .exec_without_returning(db)
    .await
    .context("failed to insert patch snapshot reservation")?;
    Ok(())
}

pub async fn find_patch_snapshot_sizes<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8],
    byte_len: i64,
) -> Result<Option<PatchSnapshotSizes>> {
    let row =
        patch_snapshot::Entity::find_by_id((domain_id.to_owned(), content_hash.to_vec(), byte_len))
            .select_only()
            .expr_as(
                sqlite_byte_length(patch_snapshot::Column::CompressedBytes),
                "compressed_bytes",
            )
            .expr_as(
                sqlite_byte_length(patch_snapshot::Column::Encoding),
                "encoding_bytes",
            )
            .expr_as(
                sqlite_byte_length(patch_snapshot::Column::LineEndingsJson),
                "line_endings_bytes",
            )
            .column(patch_snapshot::Column::RawByteLen)
            .column(patch_snapshot::Column::RefCount)
            .into_tuple::<(i64, i64, i64, i64, i64)>()
            .one(db)
            .await
            .context("failed to load patch snapshot sizes")?;
    Ok(row.map(
        |(compressed_bytes, encoding_bytes, line_endings_bytes, raw_byte_len, ref_count)| {
            PatchSnapshotSizes {
                compressed_bytes,
                encoding_bytes,
                line_endings_bytes,
                raw_byte_len,
                ref_count,
            }
        },
    ))
}

pub async fn find_patch_snapshot<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8],
    byte_len: i64,
) -> Result<Option<PatchSnapshotRow>> {
    patch_snapshot::Entity::find_by_id((domain_id.to_owned(), content_hash.to_vec(), byte_len))
        .one(db)
        .await
        .context("failed to load patch snapshot")
}

pub async fn find_patch_snapshot_with_bounds<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8],
    byte_len: i64,
    max_physical_bytes: i64,
    max_metadata_bytes: i64,
) -> Result<Option<PatchSnapshotRow>> {
    patch_snapshot::Entity::find_by_id((domain_id.to_owned(), content_hash.to_vec(), byte_len))
        .filter(sqlite_byte_length(patch_snapshot::Column::CompressedBytes).lte(max_physical_bytes))
        .filter(sqlite_byte_length(patch_snapshot::Column::Encoding).lte(max_metadata_bytes))
        .filter(sqlite_byte_length(patch_snapshot::Column::LineEndingsJson).lte(max_metadata_bytes))
        .one(db)
        .await
        .context("failed to load bounded patch snapshot")
}

pub async fn insert_patch_snapshot<C: ConnectionTrait>(
    db: &C,
    row: PatchSnapshotWrite,
) -> Result<()> {
    patch_snapshot::Entity::insert(patch_snapshot::ActiveModel {
        domain_id: Set(row.domain_id),
        content_hash: Set(row.content_hash),
        byte_len: Set(row.byte_len),
        encoding: Set(row.encoding),
        line_endings_json: Set(row.line_endings_json),
        compressed_bytes: Set(row.compressed_bytes),
        raw_byte_len: Set(row.raw_byte_len),
        ref_count: Set(row.ref_count),
        created_at: Set(Utc::now().fixed_offset()),
    })
    .exec_without_returning(db)
    .await
    .context("failed to insert patch snapshot")?;
    Ok(())
}

pub async fn replace_patch_snapshot<C: ConnectionTrait>(
    db: &C,
    row: PatchSnapshotWrite,
    expected_ref_count: i64,
) -> Result<u64> {
    Ok(patch_snapshot::Entity::update_many()
        .col_expr(patch_snapshot::Column::Encoding, Expr::value(row.encoding))
        .col_expr(
            patch_snapshot::Column::LineEndingsJson,
            Expr::value(row.line_endings_json),
        )
        .col_expr(
            patch_snapshot::Column::CompressedBytes,
            Expr::value(row.compressed_bytes),
        )
        .col_expr(
            patch_snapshot::Column::RawByteLen,
            Expr::value(row.raw_byte_len),
        )
        .col_expr(patch_snapshot::Column::RefCount, Expr::value(row.ref_count))
        .filter(patch_snapshot::Column::DomainId.eq(row.domain_id))
        .filter(patch_snapshot::Column::ContentHash.eq(row.content_hash))
        .filter(patch_snapshot::Column::ByteLen.eq(row.byte_len))
        .filter(patch_snapshot::Column::RefCount.eq(expected_ref_count))
        .exec(db)
        .await
        .context("failed to replace patch snapshot")?
        .rows_affected)
}

pub async fn set_patch_snapshot_reference_count<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8],
    byte_len: i64,
    expected_ref_count: i64,
    ref_count: i64,
) -> Result<u64> {
    Ok(patch_snapshot::Entity::update_many()
        .col_expr(patch_snapshot::Column::RefCount, Expr::value(ref_count))
        .filter(patch_snapshot::Column::DomainId.eq(domain_id.to_owned()))
        .filter(patch_snapshot::Column::ContentHash.eq(content_hash.to_vec()))
        .filter(patch_snapshot::Column::ByteLen.eq(byte_len))
        .filter(patch_snapshot::Column::RefCount.eq(expected_ref_count))
        .exec(db)
        .await
        .context("failed to set patch snapshot reference count")?
        .rows_affected)
}

pub async fn patch_snapshot_totals<C: ConnectionTrait>(db: &C) -> Result<PatchSnapshotTotals> {
    Ok(PatchSnapshotTotals {
        logical_bytes: sum_i64_column::<C, patch_snapshot::Entity>(
            db,
            patch_snapshot::Column::ByteLen,
        )
        .await?,
        physical_bytes: sum_patch_snapshot_physical_bytes(db).await?,
    })
}

pub async fn patch_snapshot_reservation_totals_excluding<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    invocation_id: &str,
) -> Result<PatchSnapshotTotals> {
    let outside_identity = Condition::any()
        .add(patch_snapshot_reservation::Column::ThreadId.ne(thread_id.to_owned()))
        .add(patch_snapshot_reservation::Column::TurnId.ne(turn_id.to_owned()))
        .add(patch_snapshot_reservation::Column::InvocationId.ne(invocation_id.to_owned()));
    let logical_bytes = patch_snapshot_reservation::Entity::find()
        .filter(outside_identity.clone())
        .select_only()
        .column_as(
            patch_snapshot_reservation::Column::LogicalBytes.sum(),
            "sum_value",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to sum logical patch snapshot reservations")?
        .flatten()
        .unwrap_or(0);
    let physical_bytes = patch_snapshot_reservation::Entity::find()
        .filter(outside_identity)
        .select_only()
        .column_as(
            patch_snapshot_reservation::Column::PhysicalBytes.sum(),
            "sum_value",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to sum physical patch snapshot reservations")?
        .flatten()
        .unwrap_or(0);
    Ok(PatchSnapshotTotals {
        logical_bytes,
        physical_bytes,
    })
}

pub async fn patch_snapshot_reservation_totals<C: ConnectionTrait>(
    db: &C,
) -> Result<PatchSnapshotTotals> {
    let logical_bytes = patch_snapshot_reservation::Entity::find()
        .select_only()
        .column_as(
            patch_snapshot_reservation::Column::LogicalBytes.sum(),
            "sum_value",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to sum logical patch snapshot reservations")?
        .flatten()
        .unwrap_or(0);
    let physical_bytes = patch_snapshot_reservation::Entity::find()
        .select_only()
        .column_as(
            patch_snapshot_reservation::Column::PhysicalBytes.sum(),
            "sum_value",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to sum physical patch snapshot reservations")?
        .flatten()
        .unwrap_or(0);
    Ok(PatchSnapshotTotals {
        logical_bytes,
        physical_bytes,
    })
}

pub async fn patch_snapshot_metrics<C: ConnectionTrait>(db: &C) -> Result<PatchSnapshotMetricsRow> {
    let blobs = patch_snapshot::Entity::find()
        .count(db)
        .await
        .context("failed to count patch snapshots")?;
    let logical_bytes =
        sum_i64_column::<C, patch_snapshot::Entity>(db, patch_snapshot::Column::RawByteLen).await?;
    let physical_bytes = sum_patch_snapshot_physical_bytes(db).await?;
    let references =
        sum_i64_column::<C, patch_snapshot::Entity>(db, patch_snapshot::Column::RefCount).await?;
    let referenced_logical_bytes = patch_snapshot::Entity::find()
        .select_only()
        .expr_as(
            Expr::col(patch_snapshot::Column::RawByteLen)
                .mul(Expr::col(patch_snapshot::Column::RefCount))
                .sum(),
            "sum_value",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to sum referenced logical patch snapshot bytes")?
        .flatten()
        .unwrap_or(0);
    Ok(PatchSnapshotMetricsRow {
        blobs,
        logical_bytes,
        physical_bytes,
        references,
        referenced_logical_bytes,
    })
}

async fn sum_patch_snapshot_physical_bytes<C: ConnectionTrait>(db: &C) -> Result<i64> {
    patch_snapshot::Entity::find()
        .select_only()
        .expr_as(
            sqlite_byte_length(patch_snapshot::Column::CompressedBytes).sum(),
            "sum_value",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to sum physical patch snapshot bytes")
        .map(Option::flatten)
        .map(|value| value.unwrap_or(0))
}

async fn sum_i64_column<C, E>(db: &C, column: E::Column) -> Result<i64>
where
    C: ConnectionTrait,
    E: EntityTrait,
    E::Column: ColumnTrait,
{
    E::find()
        .select_only()
        .column_as(column.sum(), "sum_value")
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to sum persistence column")
        .map(Option::flatten)
        .map(|value| value.unwrap_or(0))
}

fn sqlite_byte_length(column: patch_snapshot::Column) -> sea_orm::sea_query::Expr {
    Func::cust(Alias::new("length"))
        .arg(Expr::col(column).cast_as(Alias::new("BLOB")))
        .into()
}

pub async fn list_patch_snapshot_keys<C: ConnectionTrait>(
    db: &C,
    after: Option<&PatchSnapshotKeyCursor>,
    limit: u64,
) -> Result<Vec<PatchSnapshotKeyRow>> {
    let mut query = patch_snapshot::Entity::find();
    if let Some(after) = after {
        query = query.filter(
            Condition::any()
                .add(patch_snapshot::Column::DomainId.gt(after.domain_id.clone()))
                .add(
                    Condition::all()
                        .add(patch_snapshot::Column::DomainId.eq(after.domain_id.clone()))
                        .add(patch_snapshot::Column::ContentHash.gt(after.content_hash.clone())),
                )
                .add(
                    Condition::all()
                        .add(patch_snapshot::Column::DomainId.eq(after.domain_id.clone()))
                        .add(patch_snapshot::Column::ContentHash.eq(after.content_hash.clone()))
                        .add(patch_snapshot::Column::ByteLen.gt(after.byte_len)),
                ),
        );
    }
    let rows = query
        .select_only()
        .column(patch_snapshot::Column::DomainId)
        .column(patch_snapshot::Column::ContentHash)
        .column(patch_snapshot::Column::ByteLen)
        .column(patch_snapshot::Column::RefCount)
        .expr_as(
            sqlite_byte_length(patch_snapshot::Column::CompressedBytes),
            "physical_bytes",
        )
        .order_by_asc(patch_snapshot::Column::DomainId)
        .order_by_asc(patch_snapshot::Column::ContentHash)
        .order_by_asc(patch_snapshot::Column::ByteLen)
        .limit(limit)
        .into_tuple::<(String, Vec<u8>, i64, i64, i64)>()
        .all(db)
        .await
        .context("failed to list patch snapshot keys")?;
    Ok(rows
        .into_iter()
        .map(
            |(domain_id, content_hash, byte_len, ref_count, physical_bytes)| PatchSnapshotKeyRow {
                domain_id,
                content_hash,
                byte_len,
                ref_count,
                physical_bytes,
            },
        )
        .collect())
}

pub async fn delete_patch_snapshot<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8],
    byte_len: i64,
) -> Result<u64> {
    Ok(patch_snapshot::Entity::delete_by_id((
        domain_id.to_owned(),
        content_hash.to_vec(),
        byte_len,
    ))
    .exec(db)
    .await
    .context("failed to delete patch snapshot")?
    .rows_affected)
}

pub async fn list_patch_snapshot_reservations<C: ConnectionTrait>(
    db: &C,
    after: Option<&PatchSnapshotReservationCursor>,
    limit: u64,
) -> Result<Vec<PatchSnapshotReservationRow>> {
    let mut query = patch_snapshot_reservation::Entity::find();
    if let Some(after) = after {
        query = query.filter(
            Condition::any()
                .add(patch_snapshot_reservation::Column::ThreadId.gt(after.thread_id.clone()))
                .add(
                    Condition::all()
                        .add(
                            patch_snapshot_reservation::Column::ThreadId
                                .eq(after.thread_id.clone()),
                        )
                        .add(patch_snapshot_reservation::Column::TurnId.gt(after.turn_id.clone())),
                )
                .add(
                    Condition::all()
                        .add(
                            patch_snapshot_reservation::Column::ThreadId
                                .eq(after.thread_id.clone()),
                        )
                        .add(patch_snapshot_reservation::Column::TurnId.eq(after.turn_id.clone()))
                        .add(
                            patch_snapshot_reservation::Column::InvocationId
                                .gt(after.invocation_id.clone()),
                        ),
                ),
        );
    }
    query
        .order_by_asc(patch_snapshot_reservation::Column::ThreadId)
        .order_by_asc(patch_snapshot_reservation::Column::TurnId)
        .order_by_asc(patch_snapshot_reservation::Column::InvocationId)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list patch snapshot reservations")
}

pub async fn prepare_expected_patch_snapshot_references<C: ConnectionTrait>(db: &C) -> Result<()> {
    let drop_statement = Table::drop()
        .table(Alias::new(SNAPSHOT_EXPECTED_TABLE))
        .if_exists()
        .to_owned();
    db.execute(&drop_statement)
        .await
        .context("failed to reset expected patch snapshot references")?;
    let create_statement = Table::create()
        .table(Alias::new(SNAPSHOT_EXPECTED_TABLE))
        .temporary()
        .col(
            ColumnDef::new(Alias::new(SNAPSHOT_EXPECTED_DOMAIN_ID))
                .text()
                .not_null(),
        )
        .col(
            ColumnDef::new(Alias::new(SNAPSHOT_EXPECTED_CONTENT_HASH))
                .binary()
                .not_null(),
        )
        .col(
            ColumnDef::new(Alias::new(SNAPSHOT_EXPECTED_BYTE_LEN))
                .big_integer()
                .not_null(),
        )
        .col(
            ColumnDef::new(Alias::new(SNAPSHOT_EXPECTED_REF_COUNT))
                .big_integer()
                .not_null(),
        )
        .primary_key(
            Index::create()
                .col(Alias::new(SNAPSHOT_EXPECTED_DOMAIN_ID))
                .col(Alias::new(SNAPSHOT_EXPECTED_CONTENT_HASH))
                .col(Alias::new(SNAPSHOT_EXPECTED_BYTE_LEN)),
        )
        .to_owned();
    db.execute(&create_statement)
        .await
        .context("failed to create expected patch snapshot references")?;
    Ok(())
}

pub async fn increment_expected_patch_snapshot_reference<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8],
    byte_len: i64,
) -> Result<()> {
    let table = Alias::new(SNAPSHOT_EXPECTED_TABLE);
    let ref_count = Alias::new(SNAPSHOT_EXPECTED_REF_COUNT);
    let insert_statement = Query::insert()
        .into_table(table.clone())
        .columns([
            Alias::new(SNAPSHOT_EXPECTED_DOMAIN_ID),
            Alias::new(SNAPSHOT_EXPECTED_CONTENT_HASH),
            Alias::new(SNAPSHOT_EXPECTED_BYTE_LEN),
            ref_count.clone(),
        ])
        .values_panic([
            domain_id.to_owned().into(),
            content_hash.to_vec().into(),
            byte_len.into(),
            1_i64.into(),
        ])
        .on_conflict(
            OnConflict::columns([
                Alias::new(SNAPSHOT_EXPECTED_DOMAIN_ID),
                Alias::new(SNAPSHOT_EXPECTED_CONTENT_HASH),
                Alias::new(SNAPSHOT_EXPECTED_BYTE_LEN),
            ])
            .value(ref_count.clone(), Expr::col((table, ref_count)).add(1_i64))
            .to_owned(),
        )
        .to_owned();
    db.execute(&insert_statement)
        .await
        .context("failed to increment expected patch snapshot reference")?;
    Ok(())
}

pub async fn list_expected_patch_snapshot_references<C: ConnectionTrait>(
    db: &C,
    after: Option<&PatchSnapshotKeyCursor>,
    limit: u64,
) -> Result<Vec<ExpectedPatchSnapshotReferenceRow>> {
    let table = Alias::new(SNAPSHOT_EXPECTED_TABLE);
    let domain_id = Alias::new(SNAPSHOT_EXPECTED_DOMAIN_ID);
    let content_hash = Alias::new(SNAPSHOT_EXPECTED_CONTENT_HASH);
    let byte_len = Alias::new(SNAPSHOT_EXPECTED_BYTE_LEN);
    let ref_count = Alias::new(SNAPSHOT_EXPECTED_REF_COUNT);
    let mut query = Query::select();
    query
        .columns([
            domain_id.clone(),
            content_hash.clone(),
            byte_len.clone(),
            ref_count,
        ])
        .from(table.clone());
    if let Some(after) = after {
        query.cond_where(
            Condition::any()
                .add(Expr::col((table.clone(), domain_id.clone())).gt(after.domain_id.clone()))
                .add(
                    Condition::all()
                        .add(
                            Expr::col((table.clone(), domain_id.clone()))
                                .eq(after.domain_id.clone()),
                        )
                        .add(
                            Expr::col((table.clone(), content_hash.clone()))
                                .gt(after.content_hash.clone()),
                        ),
                )
                .add(
                    Condition::all()
                        .add(
                            Expr::col((table.clone(), domain_id.clone()))
                                .eq(after.domain_id.clone()),
                        )
                        .add(
                            Expr::col((table.clone(), content_hash.clone()))
                                .eq(after.content_hash.clone()),
                        )
                        .add(Expr::col((table.clone(), byte_len.clone())).gt(after.byte_len)),
                ),
        );
    }
    query
        .order_by(domain_id.clone(), Order::Asc)
        .order_by(content_hash.clone(), Order::Asc)
        .order_by(byte_len.clone(), Order::Asc)
        .limit(limit);
    let rows = db
        .query_all(&query)
        .await
        .context("failed to list expected patch snapshot references")?;
    rows.into_iter()
        .map(|row| {
            Ok(ExpectedPatchSnapshotReferenceRow {
                domain_id: row.try_get("", SNAPSHOT_EXPECTED_DOMAIN_ID)?,
                content_hash: row.try_get("", SNAPSHOT_EXPECTED_CONTENT_HASH)?,
                byte_len: row.try_get("", SNAPSHOT_EXPECTED_BYTE_LEN)?,
                ref_count: row.try_get("", SNAPSHOT_EXPECTED_REF_COUNT)?,
            })
        })
        .collect()
}

pub async fn expected_patch_snapshot_reference_exists<C: ConnectionTrait>(
    db: &C,
    domain_id_value: &str,
    content_hash_value: &[u8],
    byte_len_value: i64,
) -> Result<bool> {
    let table = Alias::new(SNAPSHOT_EXPECTED_TABLE);
    let mut query = Query::select();
    query
        .expr(Expr::val(1_i64))
        .from(table.clone())
        .and_where(
            Expr::col((table.clone(), Alias::new(SNAPSHOT_EXPECTED_DOMAIN_ID)))
                .eq(domain_id_value.to_owned()),
        )
        .and_where(
            Expr::col((table.clone(), Alias::new(SNAPSHOT_EXPECTED_CONTENT_HASH)))
                .eq(content_hash_value.to_vec()),
        )
        .and_where(Expr::col((table, Alias::new(SNAPSHOT_EXPECTED_BYTE_LEN))).eq(byte_len_value))
        .limit(1);
    Ok(db
        .query_one(&query)
        .await
        .context("failed to check expected patch snapshot reference")?
        .is_some())
}

pub async fn drop_expected_patch_snapshot_references<C: ConnectionTrait>(db: &C) -> Result<()> {
    let statement = Table::drop()
        .table(Alias::new(SNAPSHOT_EXPECTED_TABLE))
        .if_exists()
        .to_owned();
    db.execute(&statement)
        .await
        .context("failed to drop expected patch snapshot references")?;
    Ok(())
}

pub async fn find_patch_commit_intent<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    invocation_id: &str,
) -> Result<Option<PatchCommitIntentRow>> {
    patch_commit_intent::Entity::find_by_id((
        thread_id.to_owned(),
        turn_id.to_owned(),
        invocation_id.to_owned(),
    ))
    .one(db)
    .await
    .context("failed to load patch commit intent")
}

pub async fn find_patch_commit_terminal<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    invocation_id: &str,
) -> Result<Option<PatchCommitTerminalRow>> {
    patch_commit_terminal::Entity::find_by_id((
        thread_id.to_owned(),
        turn_id.to_owned(),
        invocation_id.to_owned(),
    ))
    .one(db)
    .await
    .context("failed to load patch commit terminal")
}

pub async fn next_patch_commit_ordinal<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<i64> {
    let record_max = max_applied_patch_record_ordinal(db, thread_id, turn_id).await?;
    let intent_max = max_patch_commit_intent_ordinal(db, thread_id, turn_id).await?;
    let terminal_max = max_patch_commit_terminal_ordinal(db, thread_id, turn_id).await?;
    let maximum = record_max
        .into_iter()
        .chain(intent_max)
        .chain(terminal_max)
        .max();
    match maximum {
        None => Ok(0),
        Some(ordinal) if ordinal < 0 => Ok(0),
        Some(ordinal) => ordinal
            .checked_add(1)
            .context("patch commit ordinal overflow"),
    }
}

pub async fn insert_patch_commit_intent<C: ConnectionTrait>(
    db: &C,
    row: PatchCommitIntentWrite,
) -> Result<()> {
    patch_commit_intent::Entity::insert(patch_commit_intent::ActiveModel {
        thread_id: Set(row.thread_id),
        turn_id: Set(row.turn_id),
        invocation_id: Set(row.invocation_id),
        commit_ordinal: Set(row.commit_ordinal),
        plan_fingerprint: Set(row.plan_fingerprint),
        operations_json: Set(row.operations_json),
        recovery_json: Set(row.recovery_json),
        progress_json: Set(row.progress_json),
        status: Set(row.status),
        updated_at: Set(Utc::now().fixed_offset()),
    })
    .exec_without_returning(db)
    .await
    .context("failed to insert patch commit intent")?;
    Ok(())
}

pub async fn update_patch_commit_intent_progress<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    invocation_id: &str,
    commit_ordinal: i64,
    expected_status: &str,
    progress_json: String,
    status: String,
) -> Result<u64> {
    Ok(patch_commit_intent::Entity::update_many()
        .col_expr(
            patch_commit_intent::Column::ProgressJson,
            Expr::value(progress_json),
        )
        .col_expr(patch_commit_intent::Column::Status, Expr::value(status))
        .col_expr(
            patch_commit_intent::Column::UpdatedAt,
            Expr::value(Utc::now().fixed_offset()),
        )
        .filter(patch_commit_intent::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_intent::Column::TurnId.eq(turn_id.to_owned()))
        .filter(patch_commit_intent::Column::InvocationId.eq(invocation_id.to_owned()))
        .filter(patch_commit_intent::Column::CommitOrdinal.eq(commit_ordinal))
        .filter(patch_commit_intent::Column::Status.eq(expected_status.to_owned()))
        .exec(db)
        .await
        .context("failed to update patch commit intent progress")?
        .rows_affected)
}

pub async fn list_pending_patch_commit_intents<C: ConnectionTrait>(
    db: &C,
    cursor: Option<&PendingPatchIntentCursor>,
    limit: u64,
) -> Result<Vec<PatchCommitIntentRow>> {
    let mut query = patch_commit_intent::Entity::find()
        .filter(patch_commit_intent::Column::Status.eq("pending".to_owned()));
    if let Some(cursor) = cursor {
        query = query.filter(
            Condition::any()
                .add(patch_commit_intent::Column::UpdatedAt.gt(cursor.updated_at))
                .add(
                    Condition::all()
                        .add(patch_commit_intent::Column::UpdatedAt.eq(cursor.updated_at))
                        .add(patch_commit_intent_identity_after(cursor)),
                ),
        );
    }
    query
        .order_by_asc(patch_commit_intent::Column::UpdatedAt)
        .order_by_asc(patch_commit_intent::Column::ThreadId)
        .order_by_asc(patch_commit_intent::Column::TurnId)
        .order_by_asc(patch_commit_intent::Column::InvocationId)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list pending patch commit intents")
}

fn patch_commit_intent_identity_after(cursor: &PendingPatchIntentCursor) -> Condition {
    Condition::any()
        .add(patch_commit_intent::Column::ThreadId.gt(cursor.thread_id.clone()))
        .add(
            Condition::all()
                .add(patch_commit_intent::Column::ThreadId.eq(cursor.thread_id.clone()))
                .add(patch_commit_intent::Column::TurnId.gt(cursor.turn_id.clone())),
        )
        .add(
            Condition::all()
                .add(patch_commit_intent::Column::ThreadId.eq(cursor.thread_id.clone()))
                .add(patch_commit_intent::Column::TurnId.eq(cursor.turn_id.clone()))
                .add(patch_commit_intent::Column::InvocationId.gt(cursor.invocation_id.clone())),
        )
}

pub async fn list_patch_commit_intents_for_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    after_ordinal: Option<i64>,
    limit: u64,
) -> Result<Vec<PatchCommitIntentRow>> {
    let mut query = patch_commit_intent::Entity::find()
        .filter(patch_commit_intent::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_intent::Column::TurnId.eq(turn_id.to_owned()));
    if let Some(after_ordinal) = after_ordinal {
        query = query.filter(patch_commit_intent::Column::CommitOrdinal.gt(after_ordinal));
    }
    query
        .order_by_asc(patch_commit_intent::Column::CommitOrdinal)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list patch commit intents for turn")
}

pub async fn distinct_applied_patch_authorities_for_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Vec<String>> {
    applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
        .select_only()
        .column(applied_patch_record::Column::Authority)
        .distinct()
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to list distinct applied patch authorities")
}

pub async fn distinct_patch_terminal_authorities_for_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Vec<String>> {
    patch_commit_terminal::Entity::find()
        .filter(patch_commit_terminal::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_terminal::Column::TurnId.eq(turn_id.to_owned()))
        .select_only()
        .column(patch_commit_terminal::Column::Authority)
        .distinct()
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to list distinct patch terminal authorities")
}

pub async fn upsert_patch_commit_terminal<C: ConnectionTrait>(
    db: &C,
    row: PatchCommitTerminalWrite,
) -> Result<()> {
    patch_commit_terminal::Entity::insert(patch_commit_terminal::ActiveModel {
        thread_id: Set(row.thread_id),
        turn_id: Set(row.turn_id),
        invocation_id: Set(row.invocation_id),
        commit_ordinal: Set(row.commit_ordinal),
        plan_fingerprint: Set(row.plan_fingerprint),
        operations_json: Set(row.operations_json),
        authority: Set(row.authority),
        status: Set(row.status),
        record_id: Set(row.record_id),
        created_at: Set(Utc::now().fixed_offset()),
    })
    .on_conflict(
        OnConflict::columns([
            patch_commit_terminal::Column::ThreadId,
            patch_commit_terminal::Column::TurnId,
            patch_commit_terminal::Column::InvocationId,
        ])
        .update_columns([
            patch_commit_terminal::Column::CommitOrdinal,
            patch_commit_terminal::Column::PlanFingerprint,
            patch_commit_terminal::Column::OperationsJson,
            patch_commit_terminal::Column::Authority,
            patch_commit_terminal::Column::Status,
            patch_commit_terminal::Column::RecordId,
        ])
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to upsert patch commit terminal")?;
    Ok(())
}

pub async fn delete_patch_commit_intent<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    invocation_id: &str,
    commit_ordinal: i64,
) -> Result<u64> {
    Ok(patch_commit_intent::Entity::delete_many()
        .filter(patch_commit_intent::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_intent::Column::TurnId.eq(turn_id.to_owned()))
        .filter(patch_commit_intent::Column::InvocationId.eq(invocation_id.to_owned()))
        .filter(patch_commit_intent::Column::CommitOrdinal.eq(commit_ordinal))
        .exec(db)
        .await
        .context("failed to delete patch commit intent")?
        .rows_affected)
}

pub async fn delete_patch_snapshot_reservation<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    invocation_id: &str,
) -> Result<u64> {
    Ok(patch_snapshot_reservation::Entity::delete_by_id((
        thread_id.to_owned(),
        turn_id.to_owned(),
        invocation_id.to_owned(),
    ))
    .exec(db)
    .await
    .context("failed to delete patch snapshot reservation")?
    .rows_affected)
}

pub async fn list_non_promoted_patch_statuses_for_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    after_ordinal: Option<i64>,
    limit: u64,
) -> Result<Vec<PatchOrdinalStatusRow>> {
    let mut intent_query = patch_commit_intent::Entity::find()
        .filter(patch_commit_intent::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_intent::Column::TurnId.eq(turn_id.to_owned()))
        .filter(patch_commit_intent::Column::Status.ne("promoted".to_owned()));
    let mut terminal_query = patch_commit_terminal::Entity::find()
        .filter(patch_commit_terminal::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_terminal::Column::TurnId.eq(turn_id.to_owned()))
        .filter(patch_commit_terminal::Column::Status.ne("promoted".to_owned()));
    if let Some(after_ordinal) = after_ordinal {
        intent_query =
            intent_query.filter(patch_commit_intent::Column::CommitOrdinal.gt(after_ordinal));
        terminal_query =
            terminal_query.filter(patch_commit_terminal::Column::CommitOrdinal.gt(after_ordinal));
    }
    let intents = intent_query
        .order_by_asc(patch_commit_intent::Column::CommitOrdinal)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list non-promoted patch intent statuses")?;
    let terminals = terminal_query
        .order_by_asc(patch_commit_terminal::Column::CommitOrdinal)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list non-promoted patch terminal statuses")?;
    Ok(merge_patch_status_rows(
        intents.into_iter().map(|row| PatchOrdinalStatusRow {
            turn_id: row.turn_id,
            commit_ordinal: row.commit_ordinal,
            status: row.status,
        }),
        terminals.into_iter().map(|row| PatchOrdinalStatusRow {
            turn_id: row.turn_id,
            commit_ordinal: row.commit_ordinal,
            status: row.status,
        }),
        limit,
    ))
}

pub async fn list_non_promoted_patch_statuses_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    after: Option<(&str, i64)>,
    limit: u64,
) -> Result<Vec<PatchOrdinalStatusRow>> {
    let mut intent_query = patch_commit_intent::Entity::find()
        .filter(patch_commit_intent::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_intent::Column::Status.ne("promoted".to_owned()));
    let mut terminal_query = patch_commit_terminal::Entity::find()
        .filter(patch_commit_terminal::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_terminal::Column::Status.ne("promoted".to_owned()));
    if let Some((turn_id, commit_ordinal)) = after {
        intent_query = intent_query.filter(turn_ordinal_after_intent(turn_id, commit_ordinal));
        terminal_query =
            terminal_query.filter(turn_ordinal_after_terminal(turn_id, commit_ordinal));
    }
    let intents = intent_query
        .order_by_asc(patch_commit_intent::Column::TurnId)
        .order_by_asc(patch_commit_intent::Column::CommitOrdinal)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list thread patch intent statuses")?;
    let terminals = terminal_query
        .order_by_asc(patch_commit_terminal::Column::TurnId)
        .order_by_asc(patch_commit_terminal::Column::CommitOrdinal)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list thread patch terminal statuses")?;
    Ok(merge_patch_status_rows(
        intents.into_iter().map(|row| PatchOrdinalStatusRow {
            turn_id: row.turn_id,
            commit_ordinal: row.commit_ordinal,
            status: row.status,
        }),
        terminals.into_iter().map(|row| PatchOrdinalStatusRow {
            turn_id: row.turn_id,
            commit_ordinal: row.commit_ordinal,
            status: row.status,
        }),
        limit,
    ))
}

fn turn_ordinal_after_intent(turn_id: &str, commit_ordinal: i64) -> Condition {
    Condition::any()
        .add(patch_commit_intent::Column::TurnId.gt(turn_id.to_owned()))
        .add(
            Condition::all()
                .add(patch_commit_intent::Column::TurnId.eq(turn_id.to_owned()))
                .add(patch_commit_intent::Column::CommitOrdinal.gt(commit_ordinal)),
        )
}

fn turn_ordinal_after_terminal(turn_id: &str, commit_ordinal: i64) -> Condition {
    Condition::any()
        .add(patch_commit_terminal::Column::TurnId.gt(turn_id.to_owned()))
        .add(
            Condition::all()
                .add(patch_commit_terminal::Column::TurnId.eq(turn_id.to_owned()))
                .add(patch_commit_terminal::Column::CommitOrdinal.gt(commit_ordinal)),
        )
}

fn merge_patch_status_rows(
    intents: impl Iterator<Item = PatchOrdinalStatusRow>,
    terminals: impl Iterator<Item = PatchOrdinalStatusRow>,
    limit: u64,
) -> Vec<PatchOrdinalStatusRow> {
    let mut rows = intents.chain(terminals).collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        (&left.turn_id, left.commit_ordinal, &left.status).cmp(&(
            &right.turn_id,
            right.commit_ordinal,
            &right.status,
        ))
    });
    rows.dedup();
    rows.truncate(limit as usize);
    rows
}

pub async fn max_patch_commit_state_ordinal<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<i64>> {
    Ok(max_patch_commit_intent_ordinal(db, thread_id, turn_id)
        .await?
        .into_iter()
        .chain(max_patch_commit_terminal_ordinal(db, thread_id, turn_id).await?)
        .max())
}

async fn max_applied_patch_record_ordinal<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<i64>> {
    applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
        .select_only()
        .column_as(
            applied_patch_record::Column::CommitOrdinal.max(),
            "max_ordinal",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to query maximum applied patch ordinal")
        .map(Option::flatten)
}

async fn max_patch_commit_intent_ordinal<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<i64>> {
    patch_commit_intent::Entity::find()
        .filter(patch_commit_intent::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_intent::Column::TurnId.eq(turn_id.to_owned()))
        .select_only()
        .column_as(
            patch_commit_intent::Column::CommitOrdinal.max(),
            "max_ordinal",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to query maximum patch intent ordinal")
        .map(Option::flatten)
}

async fn max_patch_commit_terminal_ordinal<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<i64>> {
    patch_commit_terminal::Entity::find()
        .filter(patch_commit_terminal::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(patch_commit_terminal::Column::TurnId.eq(turn_id.to_owned()))
        .select_only()
        .column_as(
            patch_commit_terminal::Column::CommitOrdinal.max(),
            "max_ordinal",
        )
        .into_tuple::<Option<i64>>()
        .one(db)
        .await
        .context("failed to query maximum patch terminal ordinal")
        .map(Option::flatten)
}

pub async fn find_applied_patch_record_by_invocation<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    invocation_id: &str,
) -> Result<Option<AppliedPatchRecordRow>> {
    applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
        .filter(applied_patch_record::Column::InvocationId.eq(invocation_id.to_owned()))
        .one(db)
        .await
        .context("failed to load applied patch record by invocation")
}

pub async fn find_applied_patch_record_by_scoped_id<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    record_id: &str,
) -> Result<Option<AppliedPatchRecordRow>> {
    applied_patch_record::Entity::find_by_id(record_id.to_owned())
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to load applied patch record by scoped id")
}

pub async fn applied_patch_ordinal_exists<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    commit_ordinal: i64,
) -> Result<bool> {
    Ok(applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
        .filter(applied_patch_record::Column::CommitOrdinal.eq(commit_ordinal))
        .one(db)
        .await
        .context("failed to check applied patch ordinal")?
        .is_some())
}

pub async fn insert_applied_patch_record<C: ConnectionTrait>(
    db: &C,
    row: AppliedPatchRecordWrite,
) -> Result<()> {
    applied_patch_record::Entity::insert(applied_patch_record::ActiveModel {
        id: Set(row.id),
        schema_version: Set(row.schema_version),
        thread_id: Set(row.thread_id),
        turn_id: Set(row.turn_id),
        invocation_id: Set(row.invocation_id),
        environment_id: Set(row.environment_id),
        commit_ordinal: Set(row.commit_ordinal),
        authority: Set(row.authority),
        provenance: Set(row.provenance),
        exactness: Set(row.exactness),
        committed_at_unix_ms: Set(row.committed_at_unix_ms),
        plan_fingerprint: Set(row.plan_fingerprint),
        outcome_json: Set(row.outcome_json),
        changes_json: Set(row.changes_json),
        created_at: Set(Utc::now().fixed_offset()),
    })
    .exec_without_returning(db)
    .await
    .context("failed to insert applied patch record")?;
    Ok(())
}

pub async fn list_applied_patch_records_for_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    after_ordinal: Option<i64>,
    limit: u64,
) -> Result<Vec<AppliedPatchRecordRow>> {
    let mut query = applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()));
    if let Some(after_ordinal) = after_ordinal {
        query = query.filter(applied_patch_record::Column::CommitOrdinal.gt(after_ordinal));
    }
    query
        .order_by_asc(applied_patch_record::Column::CommitOrdinal)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list applied patch records for turn")
}

pub async fn list_applied_patch_records_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    after: Option<(&str, i64)>,
    limit: u64,
) -> Result<Vec<AppliedPatchRecordRow>> {
    let mut query = applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()));
    if let Some((turn_id, commit_ordinal)) = after {
        query = query.filter(
            Condition::any()
                .add(applied_patch_record::Column::TurnId.gt(turn_id.to_owned()))
                .add(
                    Condition::all()
                        .add(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
                        .add(applied_patch_record::Column::CommitOrdinal.gt(commit_ordinal)),
                ),
        );
    }
    query
        .order_by_asc(applied_patch_record::Column::TurnId)
        .order_by_asc(applied_patch_record::Column::CommitOrdinal)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list applied patch records for thread")
}

/// Lists immutable patch records across an authorized execution-thread scope
/// in true commit order. The owning thread remains part of the keyset so a
/// visible parent can present child task-run history without copying rows.
pub async fn list_applied_patch_records_for_threads<C: ConnectionTrait>(
    db: &C,
    thread_ids: &[String],
    after: Option<(i64, &str, &str, i64)>,
    limit: u64,
) -> Result<Vec<AppliedPatchRecordRow>> {
    if thread_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.is_in(thread_ids.iter().cloned()));
    if let Some((committed_at, thread_id, turn_id, commit_ordinal)) = after {
        query = query.filter(
            Condition::any()
                .add(applied_patch_record::Column::CommittedAtUnixMs.gt(committed_at))
                .add(
                    Condition::all()
                        .add(applied_patch_record::Column::CommittedAtUnixMs.eq(committed_at))
                        .add(applied_patch_record::Column::ThreadId.gt(thread_id.to_owned())),
                )
                .add(
                    Condition::all()
                        .add(applied_patch_record::Column::CommittedAtUnixMs.eq(committed_at))
                        .add(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
                        .add(applied_patch_record::Column::TurnId.gt(turn_id.to_owned())),
                )
                .add(
                    Condition::all()
                        .add(applied_patch_record::Column::CommittedAtUnixMs.eq(committed_at))
                        .add(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
                        .add(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
                        .add(applied_patch_record::Column::CommitOrdinal.gt(commit_ordinal)),
                ),
        );
    }
    query
        .order_by_asc(applied_patch_record::Column::CommittedAtUnixMs)
        .order_by_asc(applied_patch_record::Column::ThreadId)
        .order_by_asc(applied_patch_record::Column::TurnId)
        .order_by_asc(applied_patch_record::Column::CommitOrdinal)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list applied patch records for execution threads")
}

pub async fn list_applied_patch_records_after_id<C: ConnectionTrait>(
    db: &C,
    after_id: &str,
    limit: u64,
) -> Result<Vec<AppliedPatchRecordRow>> {
    applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::Id.gt(after_id.to_owned()))
        .order_by_asc(applied_patch_record::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list applied patch records after id")
}

pub async fn summarize_applied_patch_records_for_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<(u64, Option<i64>)> {
    let base = || {
        applied_patch_record::Entity::find()
            .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
            .filter(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
    };
    let count = base()
        .count(db)
        .await
        .context("failed to count applied patch records for turn")?;
    let max_ordinal = base()
        .order_by_desc(applied_patch_record::Column::CommitOrdinal)
        .one(db)
        .await
        .context("failed to load latest applied patch record for turn")?
        .map(|row| row.commit_ordinal);
    Ok((count, max_ordinal))
}

pub async fn count_applied_patch_records_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<u64> {
    applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .count(db)
        .await
        .context("failed to count applied patch records for thread")
}

pub async fn has_applied_patch_records_for_turn_after<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    commit_ordinal: i64,
) -> Result<bool> {
    Ok(applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
        .filter(applied_patch_record::Column::CommitOrdinal.gt(commit_ordinal))
        .one(db)
        .await
        .context("failed to check later applied patch records for turn")?
        .is_some())
}

pub async fn has_applied_patch_records_for_thread_after<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    commit_ordinal: i64,
) -> Result<bool> {
    Ok(applied_patch_record::Entity::find()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(
            Condition::any()
                .add(applied_patch_record::Column::TurnId.gt(turn_id.to_owned()))
                .add(
                    Condition::all()
                        .add(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
                        .add(applied_patch_record::Column::CommitOrdinal.gt(commit_ordinal)),
                ),
        )
        .one(db)
        .await
        .context("failed to check later applied patch records for thread")?
        .is_some())
}

pub async fn list_applied_patch_turn_keys<C: ConnectionTrait>(
    db: &C,
    after: Option<(&str, &str)>,
    limit: u64,
) -> Result<Vec<(String, String)>> {
    let mut query = applied_patch_record::Entity::find();
    if let Some((thread_id, turn_id)) = after {
        query = query.filter(
            Condition::any()
                .add(applied_patch_record::Column::ThreadId.gt(thread_id.to_owned()))
                .add(
                    Condition::all()
                        .add(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
                        .add(applied_patch_record::Column::TurnId.gt(turn_id.to_owned())),
                ),
        );
    }
    query
        .select_only()
        .column(applied_patch_record::Column::ThreadId)
        .column(applied_patch_record::Column::TurnId)
        .group_by(applied_patch_record::Column::ThreadId)
        .group_by(applied_patch_record::Column::TurnId)
        .order_by_asc(applied_patch_record::Column::ThreadId)
        .order_by_asc(applied_patch_record::Column::TurnId)
        .limit(limit)
        .into_tuple::<(String, String)>()
        .all(db)
        .await
        .context("failed to list applied patch turn keys")
}

pub async fn delete_applied_patch_record_by_ordinal<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    commit_ordinal: i64,
) -> Result<u64> {
    Ok(applied_patch_record::Entity::delete_many()
        .filter(applied_patch_record::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(applied_patch_record::Column::TurnId.eq(turn_id.to_owned()))
        .filter(applied_patch_record::Column::CommitOrdinal.eq(commit_ordinal))
        .exec(db)
        .await
        .context("failed to delete applied patch record")?
        .rows_affected)
}

pub async fn delete_applied_patch_change_index_by_ordinal<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    commit_ordinal: i64,
) -> Result<u64> {
    Ok(applied_patch_change_index::Entity::delete_many()
        .filter(applied_patch_change_index::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(applied_patch_change_index::Column::TurnId.eq(turn_id.to_owned()))
        .filter(applied_patch_change_index::Column::CommitOrdinal.eq(commit_ordinal))
        .exec(db)
        .await
        .context("failed to delete applied patch change index")?
        .rows_affected)
}

pub async fn replace_applied_patch_change_index<C: ConnectionTrait>(
    db: &C,
    record_id: &str,
    rows: Vec<AppliedPatchChangeIndexWrite>,
) -> Result<()> {
    applied_patch_change_index::Entity::delete_many()
        .filter(applied_patch_change_index::Column::RecordId.eq(record_id.to_owned()))
        .exec(db)
        .await
        .context("failed to reset applied patch change index")?;
    if rows.is_empty() {
        return Ok(());
    }
    let models = rows
        .into_iter()
        .map(|row| applied_patch_change_index::ActiveModel {
            record_id: Set(row.record_id),
            thread_id: Set(row.thread_id),
            turn_id: Set(row.turn_id),
            invocation_id: Set(row.invocation_id),
            environment_id: Set(row.environment_id),
            commit_ordinal: Set(row.commit_ordinal),
            sequence: Set(row.sequence),
            source_path: Set(row.source_path),
            destination_path: Set(row.destination_path),
            change_json: Set(row.change_json),
        });
    applied_patch_change_index::Entity::insert_many(models)
        .exec_without_returning(db)
        .await
        .context("failed to insert applied patch change index")?;
    Ok(())
}

pub async fn list_patch_change_index_by_source_paths<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    paths: &[String],
    cursor: Option<&PatchChangeIndexCursor>,
    limit: u64,
    descending: bool,
) -> Result<Vec<AppliedPatchChangeIndexRow>> {
    list_patch_change_index(db, thread_id, paths, cursor, limit, descending, true).await
}

pub async fn list_patch_change_index_by_destination_paths<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    paths: &[String],
    cursor: Option<&PatchChangeIndexCursor>,
    limit: u64,
    descending: bool,
) -> Result<Vec<AppliedPatchChangeIndexRow>> {
    list_patch_change_index(db, thread_id, paths, cursor, limit, descending, false).await
}

async fn list_patch_change_index<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    paths: &[String],
    cursor: Option<&PatchChangeIndexCursor>,
    limit: u64,
    descending: bool,
    source_paths: bool,
) -> Result<Vec<AppliedPatchChangeIndexRow>> {
    patch_change_index_query(thread_id, paths, cursor, limit, descending, source_paths)
        .all(db)
        .await
        .context("failed to list applied patch change index")
}

fn patch_change_index_query(
    thread_id: &str,
    paths: &[String],
    cursor: Option<&PatchChangeIndexCursor>,
    limit: u64,
    descending: bool,
    source_paths: bool,
) -> Select<applied_patch_change_index::Entity> {
    let mut query = applied_patch_change_index::Entity::find()
        .filter(applied_patch_change_index::Column::ThreadId.eq(thread_id.to_owned()));
    query = if source_paths {
        query.filter(applied_patch_change_index::Column::SourcePath.is_in(paths.iter().cloned()))
    } else {
        query.filter(
            applied_patch_change_index::Column::DestinationPath.is_in(paths.iter().cloned()),
        )
    };
    if let Some(cursor) = cursor {
        query = query.filter(patch_change_cursor_condition(cursor, descending));
    }
    query = if descending {
        query
            .order_by_desc(applied_patch_change_index::Column::EnvironmentId)
            .order_by_desc(applied_patch_change_index::Column::TurnId)
            .order_by_desc(applied_patch_change_index::Column::CommitOrdinal)
            .order_by_desc(applied_patch_change_index::Column::Sequence)
    } else {
        query
            .order_by_asc(applied_patch_change_index::Column::EnvironmentId)
            .order_by_asc(applied_patch_change_index::Column::TurnId)
            .order_by_asc(applied_patch_change_index::Column::CommitOrdinal)
            .order_by_asc(applied_patch_change_index::Column::Sequence)
    };
    query.limit(limit)
}

/// Returns SQLite's query-plan details for the same typed query used by the
/// per-file history path lookup. This is a qualification hook: both the query
/// and its EXPLAIN wrapper are generated by SeaQuery, with no hand-written SQL.
#[doc(hidden)]
pub async fn explain_patch_change_index_path_query<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    paths: &[String],
    source_paths: bool,
) -> Result<Vec<String>> {
    let query = patch_change_index_query(thread_id, paths, None, 1, false, source_paths);
    let explain = ExplainStatement::new()
        .query_plan()
        .statement(query.as_query().to_owned());
    let rows = db
        .query_all(&SqliteQueryPlanStatement(explain))
        .await
        .context("failed to explain applied patch path query")?;
    rows.into_iter()
        .map(|row| {
            row.try_get("", "detail")
                .context("failed to decode applied patch path query plan")
        })
        .collect()
}

struct SqliteQueryPlanStatement(ExplainStatement);

impl StatementBuilder for SqliteQueryPlanStatement {
    fn build(&self, db_backend: &DbBackend) -> Statement {
        assert_eq!(
            *db_backend,
            DbBackend::Sqlite,
            "patch-history query-plan qualification requires SQLite"
        );
        let (sql, values) = self.0.build(SqliteQueryBuilder);
        Statement {
            sql,
            values: Some(values),
            db_backend: *db_backend,
        }
    }
}

fn patch_change_cursor_condition(cursor: &PatchChangeIndexCursor, descending: bool) -> Condition {
    let environment = if descending {
        applied_patch_change_index::Column::EnvironmentId.lt(cursor.environment_id.clone())
    } else {
        applied_patch_change_index::Column::EnvironmentId.gt(cursor.environment_id.clone())
    };
    let turn = if descending {
        applied_patch_change_index::Column::TurnId.lt(cursor.turn_id.clone())
    } else {
        applied_patch_change_index::Column::TurnId.gt(cursor.turn_id.clone())
    };
    let ordinal = if descending {
        applied_patch_change_index::Column::CommitOrdinal.lt(cursor.commit_ordinal)
    } else {
        applied_patch_change_index::Column::CommitOrdinal.gt(cursor.commit_ordinal)
    };
    let sequence = if descending {
        applied_patch_change_index::Column::Sequence.lt(cursor.sequence)
    } else {
        applied_patch_change_index::Column::Sequence.gt(cursor.sequence)
    };
    Condition::any()
        .add(environment)
        .add(
            Condition::all()
                .add(
                    applied_patch_change_index::Column::EnvironmentId
                        .eq(cursor.environment_id.clone()),
                )
                .add(turn),
        )
        .add(
            Condition::all()
                .add(
                    applied_patch_change_index::Column::EnvironmentId
                        .eq(cursor.environment_id.clone()),
                )
                .add(applied_patch_change_index::Column::TurnId.eq(cursor.turn_id.clone()))
                .add(ordinal),
        )
        .add(
            Condition::all()
                .add(
                    applied_patch_change_index::Column::EnvironmentId
                        .eq(cursor.environment_id.clone()),
                )
                .add(applied_patch_change_index::Column::TurnId.eq(cursor.turn_id.clone()))
                .add(applied_patch_change_index::Column::CommitOrdinal.eq(cursor.commit_ordinal))
                .add(sequence),
        )
}

pub async fn delete_patch_history_auxiliary_rows_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<()> {
    patch_commit_intent::Entity::delete_many()
        .filter(patch_commit_intent::Column::ThreadId.eq(thread_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete patch intents for thread")?;
    patch_commit_terminal::Entity::delete_many()
        .filter(patch_commit_terminal::Column::ThreadId.eq(thread_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete patch terminals for thread")?;
    patch_snapshot_reservation::Entity::delete_many()
        .filter(patch_snapshot_reservation::Column::ThreadId.eq(thread_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete patch snapshot reservations for thread")?;
    turn_diff_state::Entity::delete_many()
        .filter(turn_diff_state::Column::ThreadId.eq(thread_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete turn diff states for thread")?;
    codex_aggregate_state::Entity::delete_many()
        .filter(codex_aggregate_state::Column::ThreadId.eq(thread_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete Codex aggregate states for thread")?;
    Ok(())
}

pub async fn ensure_patch_snapshot_reference_count<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8],
    byte_len: i64,
    minimum_ref_count: i64,
) -> Result<u64> {
    Ok(patch_snapshot::Entity::update_many()
        .col_expr(
            patch_snapshot::Column::RefCount,
            Expr::value(minimum_ref_count),
        )
        .filter(patch_snapshot::Column::DomainId.eq(domain_id.to_owned()))
        .filter(patch_snapshot::Column::ContentHash.eq(content_hash.to_vec()))
        .filter(patch_snapshot::Column::ByteLen.eq(byte_len))
        .filter(patch_snapshot::Column::RefCount.lt(minimum_ref_count))
        .exec(db)
        .await
        .context("failed to ensure patch snapshot reference count")?
        .rows_affected)
}

pub async fn decrement_patch_snapshot_reference<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8],
    byte_len: i64,
) -> Result<u64> {
    Ok(patch_snapshot::Entity::update_many()
        .col_expr(
            patch_snapshot::Column::RefCount,
            Expr::col(patch_snapshot::Column::RefCount).sub(1_i64),
        )
        .filter(patch_snapshot::Column::DomainId.eq(domain_id.to_owned()))
        .filter(patch_snapshot::Column::ContentHash.eq(content_hash.to_vec()))
        .filter(patch_snapshot::Column::ByteLen.eq(byte_len))
        .filter(patch_snapshot::Column::RefCount.gte(1_i64))
        .exec(db)
        .await
        .context("failed to decrement patch snapshot reference")?
        .rows_affected)
}

pub async fn delete_unreferenced_patch_snapshot<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8],
    byte_len: i64,
) -> Result<u64> {
    Ok(patch_snapshot::Entity::delete_many()
        .filter(patch_snapshot::Column::DomainId.eq(domain_id.to_owned()))
        .filter(patch_snapshot::Column::ContentHash.eq(content_hash.to_vec()))
        .filter(patch_snapshot::Column::ByteLen.eq(byte_len))
        .filter(patch_snapshot::Column::RefCount.eq(0_i64))
        .exec(db)
        .await
        .context("failed to delete unreferenced patch snapshot")?
        .rows_affected)
}

pub async fn find_turn_diff_state<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<TurnDiffStateRow>> {
    turn_diff_state::Entity::find_by_id((thread_id.to_owned(), turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to load turn diff state")
}

pub async fn list_turn_diff_states_for_threads<C: ConnectionTrait>(
    db: &C,
    thread_ids: &[String],
) -> Result<Vec<TurnDiffStateRow>> {
    if thread_ids.is_empty() {
        return Ok(Vec::new());
    }
    turn_diff_state::Entity::find()
        .filter(turn_diff_state::Column::ThreadId.is_in(thread_ids.iter().cloned()))
        .order_by_asc(turn_diff_state::Column::ThreadId)
        .order_by_asc(turn_diff_state::Column::TurnId)
        .all(db)
        .await
        .context("failed to list turn diff states for execution threads")
}

pub async fn delete_turn_diff_state<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<u64> {
    Ok(
        turn_diff_state::Entity::delete_by_id((thread_id.to_owned(), turn_id.to_owned()))
            .exec(db)
            .await
            .context("failed to delete turn diff state")?
            .rows_affected,
    )
}

pub async fn upsert_turn_diff_state<C: ConnectionTrait>(
    db: &C,
    row: TurnDiffStateWrite,
) -> Result<()> {
    let now = Utc::now().fixed_offset();
    turn_diff_state::Entity::insert(turn_diff_state::ActiveModel {
        thread_id: Set(row.thread_id),
        turn_id: Set(row.turn_id),
        revision: Set(row.revision),
        authority: Set(row.authority),
        exact: Set(row.exact),
        coverage_json: Set(row.coverage_json),
        applied_through_ordinal: Set(row.applied_through_ordinal),
        record_count: Set(row.record_count),
        final_state: Set(row.final_state),
        state_json: Set(row.state_json),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            turn_diff_state::Column::ThreadId,
            turn_diff_state::Column::TurnId,
        ])
        .update_columns([
            turn_diff_state::Column::Revision,
            turn_diff_state::Column::Authority,
            turn_diff_state::Column::Exact,
            turn_diff_state::Column::CoverageJson,
            turn_diff_state::Column::AppliedThroughOrdinal,
            turn_diff_state::Column::RecordCount,
            turn_diff_state::Column::FinalState,
            turn_diff_state::Column::StateJson,
            turn_diff_state::Column::UpdatedAt,
        ])
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to upsert turn diff state")?;
    Ok(())
}

pub async fn find_codex_aggregate_state<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<CodexAggregateStateRow>> {
    codex_aggregate_state::Entity::find_by_id((thread_id.to_owned(), turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to load Codex aggregate state")
}

pub async fn find_first_codex_aggregate_state_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<CodexAggregateStateRow>> {
    codex_aggregate_state::Entity::find()
        .filter(codex_aggregate_state::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_asc(codex_aggregate_state::Column::TurnId)
        .limit(1)
        .one(db)
        .await
        .context("failed to load first Codex aggregate state for thread")
}

pub async fn upsert_codex_aggregate_state<C: ConnectionTrait>(
    db: &C,
    row: CodexAggregateStateWrite,
) -> Result<()> {
    let now = Utc::now().fixed_offset();
    codex_aggregate_state::Entity::insert(codex_aggregate_state::ActiveModel {
        thread_id: Set(row.thread_id),
        turn_id: Set(row.turn_id),
        revision: Set(row.revision),
        final_state: Set(row.final_state),
        state_json: Set(row.state_json),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            codex_aggregate_state::Column::ThreadId,
            codex_aggregate_state::Column::TurnId,
        ])
        .update_columns([
            codex_aggregate_state::Column::Revision,
            codex_aggregate_state::Column::FinalState,
            codex_aggregate_state::Column::StateJson,
            codex_aggregate_state::Column::UpdatedAt,
        ])
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to upsert Codex aggregate state")?;
    Ok(())
}

pub async fn finalize_codex_aggregate_state<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    revision: i64,
    state_json: String,
) -> Result<bool> {
    let now = Utc::now().fixed_offset();
    let result = codex_aggregate_state::Entity::update_many()
        .col_expr(
            codex_aggregate_state::Column::FinalState,
            Expr::value(1_i64),
        )
        .col_expr(
            codex_aggregate_state::Column::StateJson,
            Expr::value(state_json),
        )
        .col_expr(codex_aggregate_state::Column::UpdatedAt, Expr::value(now))
        .filter(codex_aggregate_state::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(codex_aggregate_state::Column::TurnId.eq(turn_id.to_owned()))
        .filter(codex_aggregate_state::Column::Revision.eq(revision))
        .filter(codex_aggregate_state::Column::FinalState.eq(0_i64))
        .exec(db)
        .await
        .context("failed to finalize Codex aggregate state")?;
    Ok(result.rows_affected != 0)
}
