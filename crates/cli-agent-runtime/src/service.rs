//! Bounded failure metadata shared by isolated CLI service adapters.
use pioneer_protocol::ProviderFailureClass;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceFailure {
    pub class: ProviderFailureClass,
    pub retry_after_ms: Option<u64>,
}
impl std::fmt::Display for ServiceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CLI service failure: {:?}", self.class)
    }
}
impl std::error::Error for ServiceFailure {}
