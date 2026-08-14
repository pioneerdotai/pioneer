use crate::runtime::McpToolCallResult;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::fmt;
use std::io::{self, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpInvocationBudget {
    pub max_arguments_bytes: usize,
    pub max_arguments_depth: usize,
    pub max_result_wire_bytes: usize,
    pub max_result_decoded_bytes: usize,
    pub max_result_depth: usize,
    pub max_result_tokens: usize,
    pub max_result_media: usize,
}

impl Default for McpInvocationBudget {
    fn default() -> Self {
        Self {
            max_arguments_bytes: 128 * 1024,
            max_arguments_depth: 32,
            max_result_wire_bytes: 1024 * 1024,
            max_result_decoded_bytes: 1024 * 1024,
            max_result_depth: 32,
            max_result_tokens: 64 * 1024,
            max_result_media: 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInvocationBudgetError {
    ArgumentsTooLarge,
    ArgumentsTooDeep,
    ResultTooLarge,
    ResultTooDeep,
    ResultTokenLimitExceeded,
    ResultMediaLimitExceeded,
    Serialization,
}

impl McpInvocationBudgetError {
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::ArgumentsTooLarge => "arguments_too_large",
            Self::ArgumentsTooDeep => "arguments_too_deep",
            Self::ResultTooLarge => "result_too_large",
            Self::ResultTooDeep => "result_too_deep",
            Self::ResultTokenLimitExceeded => "result_token_limit_exceeded",
            Self::ResultMediaLimitExceeded => "result_media_limit_exceeded",
            Self::Serialization => "payload_serialization_failed",
        }
    }
}

impl fmt::Display for McpInvocationBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ArgumentsTooLarge => "MCP arguments exceed the byte budget",
            Self::ArgumentsTooDeep => "MCP arguments exceed the JSON depth budget",
            Self::ResultTooLarge => "MCP result exceeds the wire or decoded byte budget",
            Self::ResultTooDeep => "MCP result exceeds the JSON depth budget",
            Self::ResultTokenLimitExceeded => "MCP result exceeds the token estimate budget",
            Self::ResultMediaLimitExceeded => "MCP result exceeds the media count budget",
            Self::Serialization => "MCP payload could not be measured",
        })
    }
}

impl std::error::Error for McpInvocationBudgetError {}

pub fn validate_mcp_arguments(
    arguments: &JsonValue,
    budget: McpInvocationBudget,
) -> Result<(), McpInvocationBudgetError> {
    measure_serialized(arguments, budget.max_arguments_bytes).map_err(|error| match error {
        MeasureError::Limit => McpInvocationBudgetError::ArgumentsTooLarge,
        MeasureError::Serialization => McpInvocationBudgetError::Serialization,
    })?;
    validate_depth(arguments, budget.max_arguments_depth)
        .then_some(())
        .ok_or(McpInvocationBudgetError::ArgumentsTooDeep)
}

pub fn validate_mcp_result(
    result: &McpToolCallResult,
    budget: McpInvocationBudget,
) -> Result<(), McpInvocationBudgetError> {
    validate_mcp_result_parts(
        &result.content,
        result.structured_content.as_ref(),
        result.is_error,
        result.duration_ms,
        result.meta.as_ref(),
        budget,
    )
}

pub fn validate_mcp_result_parts(
    content: &JsonValue,
    structured_content: Option<&JsonValue>,
    is_error: bool,
    duration_ms: u64,
    meta: Option<&JsonValue>,
    budget: McpInvocationBudget,
) -> Result<(), McpInvocationBudgetError> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ResultEnvelope<'a> {
        content: &'a JsonValue,
        #[serde(skip_serializing_if = "Option::is_none")]
        structured_content: Option<&'a JsonValue>,
        is_error: bool,
        duration_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        meta: Option<&'a JsonValue>,
    }
    let envelope = ResultEnvelope {
        content,
        structured_content,
        is_error,
        duration_ms,
        meta,
    };
    let wire_bytes = measure_serialized(&envelope, budget.max_result_wire_bytes).map_err(
        |error| match error {
            MeasureError::Limit => McpInvocationBudgetError::ResultTooLarge,
            MeasureError::Serialization => McpInvocationBudgetError::Serialization,
        },
    )?;
    if wire_bytes.div_ceil(4) > budget.max_result_tokens {
        return Err(McpInvocationBudgetError::ResultTokenLimitExceeded);
    }

    let mut decoded_bytes = 0usize;
    let mut media_count = 0usize;
    for value in std::iter::once(content)
        .chain(structured_content)
        .chain(meta)
    {
        if !validate_depth(value, budget.max_result_depth) {
            return Err(McpInvocationBudgetError::ResultTooDeep);
        }
        let (value_bytes, value_media) = decoded_size_and_media(value);
        decoded_bytes = decoded_bytes.saturating_add(value_bytes);
        media_count = media_count.saturating_add(value_media);
        if decoded_bytes > budget.max_result_decoded_bytes {
            return Err(McpInvocationBudgetError::ResultTooLarge);
        }
        if media_count > budget.max_result_media {
            return Err(McpInvocationBudgetError::ResultMediaLimitExceeded);
        }
    }
    Ok(())
}

fn validate_depth(value: &JsonValue, maximum: usize) -> bool {
    let mut stack = vec![(value, 1usize)];
    while let Some((value, depth)) = stack.pop() {
        if depth > maximum {
            return false;
        }
        match value {
            JsonValue::Array(values) => {
                stack.extend(values.iter().map(|value| (value, depth + 1)));
            }
            JsonValue::Object(values) => {
                stack.extend(values.values().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    true
}

fn decoded_size_and_media(value: &JsonValue) -> (usize, usize) {
    let mut bytes = 0usize;
    let mut media = 0usize;
    let mut stack = vec![value];
    while let Some(value) = stack.pop() {
        match value {
            JsonValue::Null => {}
            JsonValue::Bool(_) | JsonValue::Number(_) => bytes = bytes.saturating_add(16),
            JsonValue::String(value) => bytes = bytes.saturating_add(value.len()),
            JsonValue::Array(values) => stack.extend(values),
            JsonValue::Object(values) => {
                bytes = bytes.saturating_add(values.keys().map(String::len).sum::<usize>());
                if values
                    .get("type")
                    .and_then(JsonValue::as_str)
                    .is_some_and(|kind| matches!(kind, "image" | "audio" | "video" | "resource"))
                {
                    media = media.saturating_add(1);
                }
                stack.extend(values.values());
            }
        }
    }
    (bytes, media)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MeasureError {
    Limit,
    Serialization,
}

fn measure_serialized<T: Serialize>(value: &T, limit: usize) -> Result<usize, MeasureError> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl Write for Counter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            let next = self.bytes.checked_add(buffer.len()).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "MCP payload byte count overflow",
                )
            })?;
            if next > self.limit {
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    "MCP payload exceeds byte budget",
                ));
            }
            self.bytes = next;
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value).map_err(|error| {
        if error.io_error_kind() == Some(io::ErrorKind::FileTooLarge) {
            MeasureError::Limit
        } else {
            MeasureError::Serialization
        }
    })?;
    Ok(counter.bytes)
}
