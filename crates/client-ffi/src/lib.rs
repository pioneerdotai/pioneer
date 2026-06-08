//! C ABI boundary for native Pioneer client shells.
//!
//! This crate intentionally owns only ABI/runtime glue. Client domain logic
//! remains in `pioneer-client` and should be exposed here as explicit methods
//! only after desktop and mobile can share the same Rust API.

use pioneer_client::{
    contracts::{ClientGatewayConnectRequest, ClientGatewayConnectResult},
    gateway::{
        runtime::{self as client_gateway_runtime, GatewayProfileError},
        secrets::GatewayAuthTokenRef,
        setup::{
            AddAndActivateRemoteGatewayRegistryPlan, AddRemoteGatewayPlan,
            PlanAddRemoteGatewayRequest, RemoteGatewayValidation, RemoteGatewayValidationRequest,
            plan_add_and_activate_remote_gateway_registry_request, plan_add_remote_gateway_request,
            validate_remote_gateway_request,
        },
    },
    runtime::ClientRuntime,
    transport::ws::GatewayWsEvent,
};
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
    client_runtime: ClientRuntime,
    active_connection_id: Mutex<Option<u64>>,
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

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ClientFfiGatewayDisconnectResult {
    pub disconnected: bool,
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

    fn gateway_validate_remote(&self, input_json: &str) -> Result<RemoteGatewayValidation, String> {
        let request = serde_json::from_str::<RemoteGatewayValidationRequest>(input_json)
            .map_err(|error| format!("invalid gateway validation request: {error}"))?;

        validate_remote_gateway_request(&request).map_err(|error| error.to_string())
    }

    fn gateway_plan_add_remote(&self, input_json: &str) -> Result<AddRemoteGatewayPlan, String> {
        let request = serde_json::from_str::<PlanAddRemoteGatewayRequest>(input_json)
            .map_err(|error| format!("invalid gateway add remote planning request: {error}"))?;

        plan_add_remote_gateway_request(request, gateway_auth_token_ref_for_endpoint)
            .map_err(|error| error.to_string())
    }

    fn gateway_plan_add_and_activate_remote_registry(
        &self,
        input_json: &str,
    ) -> Result<AddAndActivateRemoteGatewayRegistryPlan, String> {
        let request = serde_json::from_str::<PlanAddRemoteGatewayRequest>(input_json)
            .map_err(|error| format!("invalid gateway add remote registry request: {error}"))?;

        plan_add_and_activate_remote_gateway_registry_request(
            request,
            gateway_auth_token_ref_for_endpoint,
        )
        .map_err(|error| error.to_string())
    }

    fn gateway_connect(&self, input_json: &str) -> Result<ClientGatewayConnectResult, String> {
        let request = serde_json::from_str::<ClientGatewayConnectRequest>(input_json)
            .map_err(|error| format!("invalid gateway connect request: {error}"))?;

        let timings = request
            .timings
            .to_gateway_ws_timings()
            .map_err(|error| error.to_string())?;

        let plan = client_gateway_runtime::plan_gateway_connect_spec(
            &request.endpoint,
            request.auth_token,
            timings,
        );

        let connection_id = self
            .client_runtime
            .ws_command_sender()
            .connect_with_retry(plan.into())
            .map_err(|error| format!("{error:#}"))?;

        *self
            .active_connection_id
            .lock()
            .map_err(|_| "client ffi connection lock is poisoned".to_owned())? =
            Some(connection_id);

        Ok(ClientGatewayConnectResult { connection_id })
    }

    fn gateway_next_events(&self) -> Result<Vec<GatewayWsEvent>, String> {
        loop {
            let active_connection_id = *self
                .active_connection_id
                .lock()
                .map_err(|_| "client ffi connection lock is poisoned".to_owned())?;

            if active_connection_id.is_none() {
                return Ok(Vec::new());
            }

            let Some(first_event) = self.client_runtime.recv_ws_event() else {
                return Ok(Vec::new());
            };

            let active_connection_id = *self
                .active_connection_id
                .lock()
                .map_err(|_| "client ffi connection lock is poisoned".to_owned())?;

            let events = self
                .client_runtime
                .drain_applicable_ws_events(active_connection_id, Some(first_event));

            if !events.is_empty() {
                return Ok(events);
            }
        }
    }

    fn gateway_disconnect(&self) -> Result<ClientFfiGatewayDisconnectResult, String> {
        self.client_runtime
            .ws_command_sender()
            .disconnect()
            .map_err(|error| format!("{error:#}"))?;
        *self
            .active_connection_id
            .lock()
            .map_err(|_| "client ffi connection lock is poisoned".to_owned())? = None;
        Ok(ClientFfiGatewayDisconnectResult { disconnected: true })
    }
}

fn ffi_client_json_response<T, F>(
    ptr: *mut PioneerClientFfi,
    input_json: *const c_char,
    operation: F,
) -> *mut c_char
where
    T: Serialize,
    F: FnOnce(&ClientFfiRuntime, &str) -> Result<T, String>,
{
    into_ffi_response(|| {
        let client = unsafe { ffi_ref(ptr)? };
        let input_json = unsafe { read_c_string(input_json)? };
        operation(&client.runtime, input_json.as_str())
    })
}

fn ffi_client_response<T, F>(ptr: *mut PioneerClientFfi, operation: F) -> *mut c_char
where
    T: Serialize,
    F: FnOnce(&ClientFfiRuntime) -> Result<T, String>,
{
    into_ffi_response(|| {
        let client = unsafe { ffi_ref(ptr)? };
        operation(&client.runtime)
    })
}

macro_rules! ffi_client_json_method {
    ($export_name:ident, $runtime_method:ident) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $export_name(
            ptr: *mut PioneerClientFfi,
            input_json: *const c_char,
        ) -> *mut c_char {
            ffi_client_json_response(ptr, input_json, |runtime, input_json| {
                runtime.$runtime_method(input_json)
            })
        }
    };
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

ffi_client_json_method!(pioneer_client_ffi_client_initialize, initialize);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_validate_remote,
    gateway_validate_remote
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_plan_add_remote,
    gateway_plan_add_remote
);
ffi_client_json_method!(
    pioneer_client_ffi_gateway_plan_add_and_activate_remote_registry,
    gateway_plan_add_and_activate_remote_registry
);
ffi_client_json_method!(pioneer_client_ffi_gateway_connect, gateway_connect);

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_gateway_next_events(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, |runtime| runtime.gateway_next_events())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pioneer_client_ffi_gateway_disconnect(
    ptr: *mut PioneerClientFfi,
) -> *mut c_char {
    ffi_client_response(ptr, |runtime| runtime.gateway_disconnect())
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

fn gateway_auth_token_ref_for_endpoint(endpoint_id: &str) -> Result<String, GatewayProfileError> {
    GatewayAuthTokenRef::for_endpoint_id(endpoint_id)
        .map(GatewayAuthTokenRef::into_string)
        .map_err(|error| GatewayProfileError::InvalidAuthTokenRef {
            endpoint_id: endpoint_id.to_owned(),
            reason: error.to_string(),
        })
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
    fn gateway_validation_uses_shared_request_contract() {
        let runtime = ClientFfiRuntime::default();
        let error = runtime
            .gateway_validate_remote(r#"{"address":"127.0.0.1:23000","timeout_ms":0}"#)
            .expect_err("zero timeout should fail");

        assert!(error.contains("timeout must be positive"));
    }

    #[test]
    fn gateway_add_remote_planning_returns_shared_plan_without_persistence() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_add_remote(
                serde_json::json!({
                    "registry": {
                        "version": 1,
                        "active_gateway_id": null,
                        "local": {
                            "id": "local",
                            "name": "Local",
                            "address": "127.0.0.1:17878",
                            "kind": "local",
                            "auth_token_ref": null,
                            "workspace_id": null,
                            "service_name": null
                        },
                        "remotes": []
                    },
                    "name": " Remote ",
                    "address": "127.0.0.1:23000",
                    "auth_token": " token ",
                    "new_endpoint_id": "remote-one",
                    "default_remote_name": "Remote 1"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan add remote");

        assert_eq!(result.endpoint.id, "remote-one");
        assert_eq!(
            result.endpoint.auth_token_ref.as_deref(),
            Some("remote-one")
        );
        assert_eq!(
            result
                .token_write
                .as_ref()
                .map(|write| write.token.as_str()),
            Some("token")
        );
    }

    #[test]
    fn gateway_add_and_activate_remote_registry_plan_returns_shared_next_registry() {
        let runtime = ClientFfiRuntime::default();
        let result = runtime
            .gateway_plan_add_and_activate_remote_registry(
                serde_json::json!({
                    "registry": {
                        "version": 1,
                        "active_gateway_id": null,
                        "remotes": []
                    },
                    "name": " Remote ",
                    "address": "127.0.0.1:23000",
                    "auth_token": " token ",
                    "new_endpoint_id": "remote-one",
                    "default_remote_name": "Remote 1"
                })
                .to_string()
                .as_str(),
            )
            .expect("plan add remote registry");

        assert_eq!(result.endpoint.id, "remote-one");
        assert_eq!(
            result.registry.active_gateway_id.as_deref(),
            Some("remote-one")
        );
        assert!(result.registry.local.is_none());
        assert_eq!(result.registry.remotes.len(), 1);
        assert_eq!(
            result
                .token_write
                .as_ref()
                .map(|write| write.token.as_str()),
            Some("token")
        );
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
