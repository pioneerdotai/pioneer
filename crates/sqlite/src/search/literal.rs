//! Render ordinary text using precisely the FTS index's built-in unicode61
//! tokenizer. No user syntax, language dictionaries or intent classification.
use rusqlite::{Connection, Error, Result, ffi};
use std::{ffi::c_void, ptr};

pub(super) fn render(connection: &Connection, text: &str) -> Result<String> {
    // A query, unlike indexed content, has a bounded parser budget. Fail rather
    // than silently losing terms. Nothing derived from its text enters errors.
    if text.len() > 64 * 1024 {
        return Err(Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_TOOBIG),
            None,
        ));
    }
    // This borrowed handle is used synchronously by the scalar callback on the
    // same physical connection. It is never retained or sent to another thread.
    let db = unsafe { connection.handle() };
    let mut api: *mut ffi::fts5_api = ptr::null_mut();
    let mut statement = ptr::null_mut();
    check(unsafe {
        ffi::sqlite3_prepare_v2(
            db,
            c"SELECT fts5(?1)".as_ptr(),
            -1,
            &mut statement,
            ptr::null_mut(),
        )
    })?;
    let statement = Statement(statement);
    check(unsafe {
        ffi::sqlite3_bind_pointer(
            statement.0,
            1,
            (&mut api as *mut *mut ffi::fts5_api).cast(),
            c"fts5_api_ptr".as_ptr(),
            None,
        )
    })?;
    match unsafe { ffi::sqlite3_step(statement.0) } {
        ffi::SQLITE_ROW | ffi::SQLITE_DONE => {}
        code => {
            check(code)?;
        }
    }
    drop(statement);
    let api = unsafe { api.as_ref() }
        .ok_or_else(|| Error::ModuleError("FTS5 tokenizer API unavailable".into()))?;
    let find = api
        .xFindTokenizer
        .ok_or_else(|| Error::ModuleError("FTS5 tokenizer lookup unavailable".into()))?;
    let mut context = ptr::null_mut();
    // A zero function-pointer Option is None; FTS fills the struct on success.
    let mut tokenizer: ffi::fts5_tokenizer = unsafe { std::mem::zeroed() };
    check(unsafe {
        find(
            api as *const _ as *mut _,
            c"unicode61".as_ptr(),
            &mut context,
            &mut tokenizer,
        )
    })?;
    let create = tokenizer
        .xCreate
        .ok_or_else(|| Error::ModuleError("FTS5 tokenizer creation unavailable".into()))?;
    let delete = tokenizer
        .xDelete
        .ok_or_else(|| Error::ModuleError("FTS5 tokenizer deletion unavailable".into()))?;
    let tokenize = tokenizer
        .xTokenize
        .ok_or_else(|| Error::ModuleError("FTS5 tokenization unavailable".into()))?;
    let mut instance = ptr::null_mut();
    check(unsafe { create(context, ptr::null_mut(), 0, &mut instance) })?;
    let instance = Tokenizer { instance, delete };
    let mut terms = Terms {
        query: String::new(),
        count: 0,
    };
    check(unsafe {
        tokenize(
            instance.instance,
            (&mut terms as *mut Terms).cast(),
            ffi::FTS5_TOKENIZE_QUERY,
            text.as_ptr().cast(),
            i32::try_from(text.len()).map_err(|_| Error::ModuleError("query too large".into()))?,
            Some(collect),
        )
    })?;
    // Empty quoted phrase is valid FTS5 syntax and has no matches. Avoid passing
    // an empty MATCH expression, which is a parser error.
    if terms.count == 0 {
        Ok("\"\"".to_owned())
    } else {
        Ok(terms.query)
    }
}

fn check(code: i32) -> Result<()> {
    if code == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(Error::SqliteFailure(ffi::Error::new(code), None))
    }
}

struct Statement(*mut ffi::sqlite3_stmt);
impl Drop for Statement {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_finalize(self.0);
        }
    }
}
struct Tokenizer {
    instance: *mut ffi::Fts5Tokenizer,
    delete: unsafe extern "C" fn(*mut ffi::Fts5Tokenizer),
}
impl Drop for Tokenizer {
    fn drop(&mut self) {
        unsafe { (self.delete)(self.instance) }
    }
}
struct Terms {
    query: String,
    count: usize,
}

unsafe extern "C" fn collect(
    context: *mut c_void,
    _flags: i32,
    token: *const std::ffi::c_char,
    length: i32,
    _start: i32,
    _end: i32,
) -> i32 {
    // No panic may cross SQLite's C callback boundary.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || token.is_null() || length < 0 {
            return ffi::SQLITE_ERROR;
        }
        let terms = unsafe { &mut *context.cast::<Terms>() };
        if terms.count == 512 {
            return ffi::SQLITE_TOOBIG;
        }
        let bytes = unsafe { std::slice::from_raw_parts(token.cast::<u8>(), length as usize) };
        let Ok(token) = std::str::from_utf8(bytes) else {
            return ffi::SQLITE_ERROR;
        };
        if terms.count > 0 {
            terms.query.push_str(" OR ");
        }
        terms.query.push('"');
        for ch in token.chars() {
            terms.query.push(ch);
            if ch == '"' {
                terms.query.push('"');
            }
        }
        terms.query.push('"');
        terms.count += 1;
        ffi::SQLITE_OK
    }))
    .unwrap_or(ffi::SQLITE_NOMEM)
}
