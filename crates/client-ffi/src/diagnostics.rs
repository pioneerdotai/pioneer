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
            message: redact_diagnostic_message(message.as_str()),
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

fn redact_diagnostic_message(message: &str) -> String {
    let message = redact_bearer_values(message);
    let message = redact_jwt_values(message.as_str());
    redact_named_secret_values(message.as_str())
}

fn redact_bearer_values(message: &str) -> String {
    redact_ascii_matches(message, |bytes, cursor| {
        let marker = b"bearer";
        if !ascii_eq_ignore_case(bytes.get(cursor..cursor + marker.len()), marker)
            || cursor
                .checked_sub(1)
                .and_then(|index| bytes.get(index))
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            return None;
        }
        let mut value_start = cursor + marker.len();
        if !bytes
            .get(value_start)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            return None;
        }
        while bytes
            .get(value_start)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            value_start += 1;
        }
        let mut value_end = value_start;
        while bytes
            .get(value_end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || b"._~+/=-".contains(byte))
        {
            value_end += 1;
        }
        (value_end > value_start).then_some((value_start, value_end, "[redacted]"))
    })
}

fn redact_jwt_values(message: &str) -> String {
    redact_ascii_matches(message, |bytes, cursor| {
        if bytes.get(cursor..cursor + 3)? != b"eyJ"
            || cursor
                .checked_sub(1)
                .and_then(|index| bytes.get(index))
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(byte))
        {
            return None;
        }
        let mut value_end = cursor;
        while bytes
            .get(value_end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(byte))
        {
            value_end += 1;
        }
        let candidate = std::str::from_utf8(&bytes[cursor..value_end]).ok()?;
        let mut segments = candidate.split('.');
        let valid = segments.next().is_some_and(|value| !value.is_empty())
            && segments.next().is_some_and(|value| !value.is_empty())
            && segments.next().is_some_and(|value| !value.is_empty())
            && segments.next().is_none();
        valid.then_some((cursor, value_end, "[redacted-jwt]"))
    })
}

fn redact_named_secret_values(message: &str) -> String {
    redact_ascii_matches(message, |bytes, cursor| {
        let key_quote = bytes
            .get(cursor)
            .copied()
            .filter(|byte| matches!(byte, b'\'' | b'"'));
        let key_start = cursor + usize::from(key_quote.is_some());
        if key_quote.is_none()
            && cursor
                .checked_sub(1)
                .and_then(|index| bytes.get(index))
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            return None;
        }
        let mut key_end = key_start;
        while bytes
            .get(key_end)
            .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            key_end += 1;
        }
        let normalized_key = bytes
            .get(key_start..key_end)?
            .iter()
            .filter(|byte| byte.is_ascii_alphanumeric())
            .map(|byte| byte.to_ascii_lowercase())
            .collect::<Vec<_>>();
        if !is_sensitive_diagnostic_key(normalized_key.as_slice()) {
            return None;
        }

        let mut separator = if let Some(key_quote) = key_quote {
            if bytes.get(key_end) != Some(&key_quote) {
                return None;
            }
            key_end + 1
        } else {
            key_end
        };
        while bytes
            .get(separator)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            separator += 1;
        }
        if !bytes
            .get(separator)
            .is_some_and(|byte| matches!(byte, b'=' | b':'))
        {
            return None;
        }
        let mut value_start = separator + 1;
        while bytes
            .get(value_start)
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            value_start += 1;
        }
        let quote = bytes
            .get(value_start)
            .copied()
            .filter(|byte| matches!(byte, b'\'' | b'"'));
        if quote.is_some() {
            value_start += 1;
        }
        let value_end = diagnostic_value_end(bytes, value_start, quote);
        (value_end > value_start).then_some((value_start, value_end, "[redacted]"))
    })
}

fn diagnostic_value_end(bytes: &[u8], value_start: usize, quote: Option<u8>) -> usize {
    if let Some(quote) = quote {
        return quoted_value_end(bytes, value_start, quote);
    }
    if bytes
        .get(value_start)
        .is_some_and(|byte| matches!(byte, b'{' | b'['))
    {
        return structured_value_end(bytes, value_start);
    }

    let mut value_end = value_start;
    while bytes.get(value_end).is_some_and(|byte| {
        !byte.is_ascii_whitespace() && !matches!(byte, b'&' | b',' | b'}' | b']')
    }) {
        value_end += 1;
    }
    value_end
}

fn quoted_value_end(bytes: &[u8], value_start: usize, quote: u8) -> usize {
    let mut value_end = value_start;
    let mut escaped = false;
    while let Some(byte) = bytes.get(value_end) {
        if !escaped && *byte == quote {
            break;
        }
        escaped = !escaped && *byte == b'\\';
        if *byte != b'\\' {
            escaped = false;
        }
        value_end += 1;
    }
    value_end
}

fn structured_value_end(bytes: &[u8], value_start: usize) -> usize {
    let mut value_end = value_start;
    let mut depth = 0_usize;
    let mut quote = None;
    let mut escaped = false;
    while let Some(byte) = bytes.get(value_end) {
        if let Some(active_quote) = quote {
            if !escaped && *byte == active_quote {
                quote = None;
            }
            escaped = !escaped && *byte == b'\\';
            if *byte != b'\\' {
                escaped = false;
            }
        } else {
            match byte {
                b'\'' | b'"' => quote = Some(*byte),
                b'{' | b'[' => depth += 1,
                b'}' | b']' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return value_end + 1;
                    }
                }
                _ => {}
            }
        }
        value_end += 1;
    }
    value_end
}

fn is_sensitive_diagnostic_key(normalized_key: &[u8]) -> bool {
    matches!(
        normalized_key,
        b"authorization"
            | b"signingsecret"
            | b"authtoken"
            | b"accesstoken"
            | b"refreshtoken"
            | b"bearertoken"
            | b"apikey"
            | b"clientsecret"
            | b"privatekey"
            | b"jwtmaterial"
            | b"jwtsecret"
            | b"credential"
            | b"credentials"
            | b"password"
            | b"token"
            | b"secret"
            | b"key"
            | b"displayname"
            | b"nickname"
            | b"prompt"
            | b"params"
            | b"payload"
            | b"filepayload"
    ) || [
        b"apikey".as_slice(),
        b"authorization".as_slice(),
        b"token".as_slice(),
        b"secret".as_slice(),
        b"password".as_slice(),
        b"privatekey".as_slice(),
        b"credential".as_slice(),
        b"credentials".as_slice(),
        b"principalid".as_slice(),
        b"gatewayid".as_slice(),
        b"actorid".as_slice(),
        b"sessionid".as_slice(),
        b"connectionid".as_slice(),
        b"userid".as_slice(),
        b"displayname".as_slice(),
        b"nickname".as_slice(),
        b"prompt".as_slice(),
        b"params".as_slice(),
        b"payload".as_slice(),
    ]
    .iter()
    .any(|suffix| normalized_key.ends_with(suffix))
}

fn redact_ascii_matches(
    message: &str,
    mut find_match: impl FnMut(&[u8], usize) -> Option<(usize, usize, &'static str)>,
) -> String {
    let bytes = message.as_bytes();
    let mut output = String::with_capacity(message.len());
    let mut copied_until = 0;
    let mut cursor = 0;
    while cursor < bytes.len() {
        let Some((start, end, replacement)) = find_match(bytes, cursor) else {
            cursor += 1;
            continue;
        };
        output.push_str(&message[copied_until..start]);
        output.push_str(replacement);
        copied_until = end;
        cursor = end;
    }
    output.push_str(&message[copied_until..]);
    output
}

fn ascii_eq_ignore_case(candidate: Option<&[u8]>, expected: &[u8]) -> bool {
    candidate.is_some_and(|candidate| candidate.eq_ignore_ascii_case(expected))
}

fn unix_timestamp_ms() -> u64 {
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };

    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::ClientFfiDiagnostics;

    #[test]
    fn diagnostics_redact_bearer_jwt_and_named_secret_values_before_mobile_drain() {
        let diagnostics = ClientFfiDiagnostics::default();
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzdXBlcnVzZXIifQ.signature";
        diagnostics.record_error(
            "gateway_connect",
            format!(
                "Authorization: Bearer raw-access-token token=raw-token password='raw-password' \
                 signing_secret=\"raw-signing\" apiKey=raw-api-key \
                 access-token=raw-access-token-2 client_secret=raw-client-secret \
                 jwt_material=raw-jwt-material provider_api_key=prefixed-api-key \
                 owner_principal_id=private-principal displayName='Private Name' \
                 profile_display_name='Prefixed Private Name' \
                 nickname=private-nickname request_prompt='private prompt' \
                 params='private params' file_payload='private payload' jwt={jwt} \
                 json={{\"auth_token\":\"json-token\",\"display_name\":\"Json Private\",\
                 \"params\":{{\"prompt\":\"json prompt\",\"note\":\"nested private note\"}}}}"
            ),
            "gateway_connect_failed",
        );

        let events = diagnostics.drain().expect("diagnostics drain");
        assert_eq!(events.len(), 1);
        let message = events[0].message.as_str();
        for forbidden in [
            "raw-access-token",
            "raw-token",
            "raw-password",
            "raw-signing",
            "raw-api-key",
            "raw-access-token-2",
            "raw-client-secret",
            "raw-jwt-material",
            "prefixed-api-key",
            "private-principal",
            "Private Name",
            "Prefixed Private Name",
            "private-nickname",
            "private prompt",
            "private params",
            "private payload",
            "json-token",
            "Json Private",
            "json prompt",
            "nested private note",
            jwt,
        ] {
            assert!(
                !message.contains(forbidden),
                "mobile diagnostic retained forbidden value `{forbidden}`: {message}"
            );
        }
        assert!(message.contains("Authorization: [redacted]"));
        assert!(message.contains("[redacted]"));
        assert!(message.contains("token=[redacted]"));
        assert!(message.contains("password='[redacted]'"));
        assert!(message.contains("signing_secret=\"[redacted]\""));
        assert!(message.contains("jwt=[redacted-jwt]"));
    }
}
