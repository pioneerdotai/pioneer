use rusqlite::types::Value as NativeValue;
use sea_orm::{DbErr, Statement, Value};

/// Fixed schema coordinates for a legacy TEXT/BLOB range. The caller's guarded
/// Entity SELECT supplies its current rowid; rowid is never a durable identity.
#[derive(Clone, Copy, Debug)]
pub struct SqliteBlobSource {
    pub table: &'static str,
    pub column: &'static str,
}

pub(crate) fn error(message: &'static str) -> DbErr {
    DbErr::Custom(message.to_owned())
}

pub(crate) fn native_error(error: rusqlite::Error) -> DbErr {
    match error.sqlite_error_code() {
        Some(rusqlite::ErrorCode::DatabaseBusy) => {
            self::error("SQLite database busy during blob range read")
        }
        Some(rusqlite::ErrorCode::DatabaseLocked) => {
            self::error("SQLite database locked during blob range read")
        }
        _ => self::error("SQLite blob range read failed"),
    }
}

// Convert before admission. In particular, date formatting must not happen
// while holding the reader. Reject unsupported variants rather than silently
// changing SQLx's bind semantics. These cover the bounded owner guards.
pub(crate) fn bindings(statement: &mut Statement) -> Result<Vec<NativeValue>, DbErr> {
    statement
        .values
        .take()
        .map(|values| values.0)
        .unwrap_or_default()
        .into_iter()
        .map(|value| {
            Ok(match value {
                Value::String(value) => value.map_or(NativeValue::Null, NativeValue::Text),
                Value::BigInt(value) => value.map_or(NativeValue::Null, NativeValue::Integer),
                Value::Int(value) => {
                    value.map_or(NativeValue::Null, |v| NativeValue::Integer(v.into()))
                }
                Value::Bool(value) => {
                    value.map_or(NativeValue::Null, |v| NativeValue::Integer(v.into()))
                }
                Value::ChronoDateTimeWithTimeZone(value) => {
                    value.map_or(NativeValue::Null, |v| NativeValue::Text(v.to_rfc3339()))
                }
                _ => return Err(error("unsupported SQLite blob guard binding")),
            })
        })
        .collect()
}

/// The SQLx handle lock is held by the caller throughout this synchronous
/// operation. The wrapper borrows that handle and never opens/closes a physical
/// connection. A guard positioned on SQLITE_ROW pins the snapshot until after
/// the read-only incremental handle is closed, including every error path.
pub(crate) fn read(
    connection: &rusqlite::Connection,
    source: SqliteBlobSource,
    guard: &str,
    bindings: &[NativeValue],
    offset: u64,
    count: usize,
    expected_bytes: u64,
) -> Result<Option<Vec<u8>>, DbErr> {
    // Incremental reads return physical bytes. UTF-16 storage cannot satisfy
    // the caller's original UTF-8 byte offsets; do not convert or change it.
    let encoding: String = connection
        .query_row("PRAGMA encoding", [], |row| row.get(0))
        .map_err(native_error)?;
    if encoding != "UTF-8" {
        return Err(error("SQLite blob range requires UTF-8 storage"));
    }
    let mut statement = connection.prepare(guard).map_err(native_error)?;
    if !statement.readonly() || statement.column_count() != 1 {
        return Err(error("SQLite blob guard must select only rowid"));
    }
    if statement.parameter_count() != bindings.len() {
        return Err(error("SQLite blob guard binding count mismatch"));
    }
    let mut rows = statement
        .query(rusqlite::params_from_iter(bindings))
        .map_err(native_error)?;
    let Some(row) = rows.next().map_err(native_error)? else {
        return Ok(None);
    };
    let rowid: i64 = row.get(0).map_err(native_error)?;
    // SQLite accepts both TEXT and BLOB with read_only=true, including indexed
    // TEXT columns. It rejects NULL, numeric, virtual and WITHOUT ROWID targets.
    let blob = connection
        .blob_open(rusqlite::MAIN_DB, source.table, source.column, rowid, true)
        .map_err(native_error)?;
    if blob.len() as u64 != expected_bytes {
        return Err(error("SQLite blob physical length changed"));
    }
    let mut bytes = vec![0; count];
    let offset = usize::try_from(offset).map_err(|_| error("SQLite blob range offset overflow"))?;
    blob.read_at_exact(&mut bytes, offset)
        .map_err(native_error)?;
    blob.close().map_err(native_error)?;
    // rows/statement are still alive here. They and the borrowed connection
    // are released before the pool connection/maintenance permit can be reused.
    Ok(Some(bytes))
}
