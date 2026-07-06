#![warn(clippy::print_stdout)]

use rusqlite::Connection;
use std::sync::OnceLock;
use util::init_logging;

mod add_functions;
mod basic;
mod dict_management;
mod dict_training;
mod transparent;
mod util;

pub use log::LevelFilter as LogLevel;

/// Loads the sqlite extension with the default log level (INFO)
pub fn load(connection: &Connection) -> anyhow::Result<()> {
    load_with_loglevel(connection, LogLevel::Info)
}

/// Loads the sqlite extension with the given log level
pub fn load_with_loglevel(
    connection: &Connection,
    default_log_level: LogLevel,
) -> anyhow::Result<()> {
    init_logging(default_log_level);
    load_functions(connection)
}

fn load_functions(connection: &Connection) -> anyhow::Result<()> {
    self::dict_management::invalidate_caches(connection);
    self::add_functions::add_functions(connection)
}

/// Registers sqlite-zstd functions for every SQLite connection opened after this call.
///
/// Pioneer uses SQLx/SeaORM pools, while upstream sqlite-zstd exposes a rusqlite loader.
/// SQLite auto-extensions are the narrow bridge: register once before opening the pool,
/// and SQLite invokes the callback for each new connection.
pub fn register_auto_extension_once() -> anyhow::Result<()> {
    static REGISTRATION: OnceLock<Result<(), String>> = OnceLock::new();

    match REGISTRATION.get_or_init(|| {
        unsafe { rusqlite::auto_extension::register_auto_extension(sqlite_zstd_auto_extension) }
            .map_err(|error| error.to_string())
    }) {
        Ok(()) => Ok(()),
        Err(error) => anyhow::bail!("failed to register sqlite-zstd auto-extension: {error}"),
    }
}

unsafe extern "C" fn sqlite_zstd_auto_extension(
    db: *mut rusqlite::ffi::sqlite3,
    pz_err_msg: *mut *mut std::os::raw::c_char,
    _api: *const rusqlite::ffi::sqlite3_api_routines,
) -> std::os::raw::c_int {
    unsafe {
        rusqlite::auto_extension::init_auto_extension(db, pz_err_msg, |connection| {
            load_functions(&connection)
                .map_err(|error| rusqlite::Error::ModuleError(format!("{error:#}")))
        })
    }
}
