//! SQLite-specific search representation and extension initialization.
use std::sync::OnceLock;
mod literal;

/// Register before pools open. SQLite calls this extension on each physical
/// connection, including pool replacements; no loadable-library setting exists.
pub fn register_auto_extension_once() -> anyhow::Result<()> {
    static REGISTRATION: OnceLock<Result<(), i32>> = OnceLock::new();
    match REGISTRATION.get_or_init(|| {
        // sqlite-vec's crate declares the C entry point without its arguments.
        // sqlite3_auto_extension requires the canonical three-argument ABI.
        let entry = unsafe {
            std::mem::transmute::<
                unsafe extern "C" fn(),
                unsafe extern "C" fn(
                    *mut rusqlite::ffi::sqlite3,
                    *mut *mut std::os::raw::c_char,
                    *const rusqlite::ffi::sqlite3_api_routines,
                ) -> std::os::raw::c_int,
            >(sqlite_vec::sqlite3_vec_init)
        };
        let code = unsafe { rusqlite::ffi::sqlite3_auto_extension(Some(entry)) };
        if code == rusqlite::ffi::SQLITE_OK {
            unsafe { rusqlite::auto_extension::register_auto_extension(literal_auto_extension) }
                .map_err(|_| rusqlite::ffi::SQLITE_ERROR)
        } else {
            Err(code)
        }
    }) {
        Ok(()) => Ok(()),
        Err(code) => anyhow::bail!("failed to register SQLite vector extension (code {code})"),
    }
}

unsafe extern "C" fn literal_auto_extension(
    db: *mut rusqlite::ffi::sqlite3,
    error: *mut *mut std::os::raw::c_char,
    _api: *const rusqlite::ffi::sqlite3_api_routines,
) -> std::os::raw::c_int {
    unsafe {
        rusqlite::auto_extension::init_auto_extension(db, error, |connection| {
            use rusqlite::functions::FunctionFlags;
            connection.create_scalar_function(
                "pioneer_fts_literal",
                1,
                FunctionFlags::SQLITE_UTF8
                    | FunctionFlags::SQLITE_DETERMINISTIC
                    | FunctionFlags::SQLITE_INNOCUOUS,
                |context| {
                    let text = context.get::<String>(0)?;
                    // The borrowed connection cannot escape this callback.
                    let connection = context.get_connection()?;
                    literal::render(&connection, &text)
                },
            )
        })
    }
}
