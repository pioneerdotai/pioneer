use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_DIAGNOSTICS: usize = 64;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClientDiagnosticLevel {
    Breadcrumb,
    Error,
}

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClientDiagnosticEvent {
    pub sequence: u64,
    pub unix_ms: u64,
    pub level: ClientDiagnosticLevel,
    pub operation: String,
    pub message: String,
    pub code: Option<String>,
}

#[derive(Default)]
pub struct ClientFfiDiagnostics {
    inner: Mutex<ClientFfiDiagnosticsInner>,
}

#[derive(Default)]
struct ClientFfiDiagnosticsInner {
    next_sequence: u64,
    events: VecDeque<ClientDiagnosticEvent>,
}

impl ClientFfiDiagnostics {
    pub fn record_error(&self, operation: &'static str, message: String, code: &'static str) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };

        let sequence = inner.next_sequence;
        inner.next_sequence = inner.next_sequence.wrapping_add(1);

        if inner.events.len() == MAX_DIAGNOSTICS {
            inner.events.pop_front();
        }

        inner.events.push_back(ClientDiagnosticEvent {
            sequence,
            unix_ms: unix_timestamp_ms(),
            level: ClientDiagnosticLevel::Error,
            operation: operation.to_owned(),
            message,
            code: Some(code.to_owned()),
        });
    }

    pub fn drain(&self) -> Result<Vec<ClientDiagnosticEvent>, String> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| "client ffi diagnostics lock is poisoned".to_owned())?;

        Ok(inner.events.drain(..).collect())
    }
}

fn unix_timestamp_ms() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };

    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
