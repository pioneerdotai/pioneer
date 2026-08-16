use serde::Serialize;
use serde_json::Value as JsonValue;
use std::fmt;
use std::io::{self, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpInvocationBudget {
    pub max_arguments_bytes: usize,
}

impl Default for McpInvocationBudget {
    fn default() -> Self {
        Self {
            max_arguments_bytes: 128 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpInvocationBudgetError {
    ArgumentsTooLarge,
    Serialization,
}

impl McpInvocationBudgetError {
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::ArgumentsTooLarge => "arguments_too_large",
            Self::Serialization => "payload_serialization_failed",
        }
    }
}

impl fmt::Display for McpInvocationBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ArgumentsTooLarge => "MCP arguments exceed the byte budget",
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
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MeasureError {
    Limit,
    Serialization,
}

fn measure_serialized<T: Serialize>(value: &T, limit: usize) -> Result<(), MeasureError> {
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
    Ok(())
}
