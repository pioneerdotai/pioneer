use std::{error::Error, fmt};

pub type ClientResult<T> = Result<T, ClientError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    InvalidState(String),
    Protocol(String),
    Platform(String),
}

impl ClientError {
    pub fn invalid_state(message: impl Into<String>) -> Self {
        Self::InvalidState(message.into())
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    pub fn platform(message: impl Into<String>) -> Self {
        Self::Platform(message.into())
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState(message) => write!(f, "invalid client state: {message}"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
            Self::Platform(message) => write!(f, "platform error: {message}"),
        }
    }
}

impl Error for ClientError {}
