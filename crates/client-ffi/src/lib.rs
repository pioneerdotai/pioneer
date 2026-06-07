//! C ABI boundary for native Pioneer client shells.
//!
//! This crate intentionally owns only ABI/runtime glue. Client domain logic
//! remains in `pioneer-client` and should be exposed here as explicit methods
//! only after desktop and mobile can share the same Rust API.

use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    ffi::{CStr, CString, c_char},
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::Mutex,
};

const FFI_VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct PioneerClientFfi {
    runtime: ClientFfiRuntime,
}

#[derive(Default)]
struct ClientFfiRuntime {
    config: Mutex<Option<ClientFfiConfig>>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientFfiConfig {
    pub app_data_dir: Option<String>,
    pub locale: Option<String>,
    pub platform: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientFfiInitializeResult {
    pub initialized: bool,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum FfiResponse<T> {
    Ok {
        value: T,
    },
    Error {
        message: String,
        code: Option<String>,
    },
}

impl PioneerClientFfi {
    fn new() -> Self {
        Self {
            runtime: ClientFfiRuntime::default(),
        }
    }
}

impl ClientFfiRuntime {
    fn initialize(&self, config_json: &str) -> Result<ClientFfiInitializeResult, String> {
        let config = if config_json.trim().is_empty() {
            ClientFfiConfig::default()
        } else {
            serde_json::from_str::<ClientFfiConfig>(config_json)
                .map_err(|error| format!("invalid client ffi config: {error}"))?
        };

        let _client_contract_count = pioneer_client::schema::public_client_schema_contracts().len();
        *self
            .config
            .lock()
            .map_err(|_| "client ffi config lock is poisoned".to_owned())? = Some(config);

        Ok(ClientFfiInitializeResult { initialized: true })
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pioneer_client_ffi_version() -> *mut c_char {
    into_ffi_response(|| Ok(FFI_VERSION))
}

#[unsafe(no_mangle)]
pub extern "C" fn pioneer_client_ffi_client_create() -> *mut PioneerClientFfi {
    catch_unwind(AssertUnwindSafe(|| {
        Box::into_raw(Box::new(PioneerClientFfi::new()))
    }))
    .unwrap_or(ptr::null_mut())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_client_destroy(ptr: *mut PioneerClientFfi) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if ptr.is_null() {
            return;
        }

        // SAFETY: ownership is transferred back from the raw pointer exactly once
        // by the native wrapper when its Nitro object is deallocated.
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }));
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_client_initialize(
    ptr: *mut PioneerClientFfi,
    config_json: *const c_char,
) -> *mut c_char {
    into_ffi_response(|| {
        let client = unsafe { ffi_ref(ptr)? };
        let config_json = unsafe { read_c_string(config_json)? };
        client.runtime.initialize(config_json.as_str())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_string_destroy(value: *mut c_char) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if value.is_null() {
            return;
        }

        // SAFETY: strings returned by this crate are allocated with
        // `CString::into_raw`; this reclaims and drops that allocation.
        unsafe {
            drop(CString::from_raw(value));
        }
    }));
}

unsafe fn ffi_ref<'a>(ptr: *mut PioneerClientFfi) -> Result<&'a PioneerClientFfi, String> {
    if ptr.is_null() {
        return Err("received null client pointer".to_owned());
    }

    // SAFETY: the pointer is created by `pioneer_client_ffi_client_create` and
    // remains valid until `pioneer_client_ffi_client_destroy`.
    unsafe { ptr.as_ref() }.ok_or_else(|| "received invalid client pointer".to_owned())
}

unsafe fn read_c_string(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Err("received null string pointer".to_owned());
    }

    // SAFETY: callers pass a valid, NUL-terminated string pointer owned by the
    // native bridge for the duration of this call.
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(str::to_owned)
        .map_err(|error| format!("received non-utf8 string: {error}"))
}

fn to_json_response<T: Serialize>(result: Result<T, String>) -> String {
    let response = match result {
        Ok(value) => serde_json::to_string(&FfiResponse::Ok { value }),
        Err(message) => serde_json::to_string(&FfiResponse::<()>::Error {
            message,
            code: Some("pioneer_client_ffi_error".to_owned()),
        }),
    };

    response.unwrap_or_else(|error| {
        format!(
            r#"{{"status":"error","message":"failed to serialize ffi response: {}","code":"pioneer_client_ffi_serialize_error"}}"#,
            sanitize_c_string(error.to_string())
        )
    })
}

fn into_ffi_response<T, F>(operation: F) -> *mut c_char
where
    T: Serialize,
    F: FnOnce() -> Result<T, String>,
{
    into_c_string(ffi_response_json(operation))
}

fn ffi_response_json<T, F>(operation: F) -> String
where
    T: Serialize,
    F: FnOnce() -> Result<T, String>,
{
    catch_unwind(AssertUnwindSafe(|| to_json_response(operation()))).unwrap_or_else(|payload| {
        to_json_response::<()>(Err(format!(
            "panic in pioneer client ffi: {}",
            panic_message(payload)
        )))
    })
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        sanitize_c_string((*message).to_owned())
    } else if let Some(message) = payload.downcast_ref::<String>() {
        sanitize_c_string(message.clone())
    } else {
        "unknown panic payload".to_owned()
    }
}

fn sanitize_c_string(value: String) -> String {
    value.replace('\0', "\\u0000")
}

fn into_c_string(value: String) -> *mut c_char {
    match CString::new(value) {
        Ok(value) => value.into_raw(),
        Err(error) => {
            let sanitized =
                sanitize_c_string(String::from_utf8_lossy(&error.into_vec()).into_owned());
            CString::new(sanitized)
                .map(CString::into_raw)
                .unwrap_or(ptr::null_mut())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_response<T: for<'de> Deserialize<'de>>(json: &str) -> T {
        #[derive(Deserialize)]
        #[serde(tag = "status", rename_all = "lowercase")]
        enum TestResponse<T> {
            Ok {
                value: T,
            },
            Error {
                message: String,
                code: Option<String>,
            },
        }

        match serde_json::from_str::<TestResponse<T>>(json).expect("response json") {
            TestResponse::Ok { value } => value,
            TestResponse::Error { message, code } => {
                panic!("unexpected ffi error: {message} {code:?}")
            }
        }
    }

    #[test]
    fn initialize_accepts_shell_config() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .initialize(r#"{"platform":"ios","locale":"en","app_data_dir":"/tmp/pioneer"}"#)
            .expect("initialize");

        assert!(result.initialized);
    }

    #[test]
    fn ffi_response_is_tagged_json() {
        let response = to_json_response::<serde_json::Value>(Ok(serde_json::json!({"value": 1})));
        let value: serde_json::Value = decode_response(response.as_str());

        assert_eq!(value["value"], 1);
    }

    #[test]
    fn ffi_boundary_converts_panic_to_error_response() {
        let response = ffi_response_json::<(), _>(|| panic!("boom"));
        let error = serde_json::from_str::<serde_json::Value>(response.as_str()).expect("json");

        assert_eq!(error["status"], "error");
        assert_eq!(error["code"], "pioneer_client_ffi_error");
        assert!(error["message"].as_str().unwrap().contains("boom"));
    }
}
