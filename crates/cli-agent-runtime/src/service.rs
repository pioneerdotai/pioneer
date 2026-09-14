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

/// Safe stage context survives anyhow propagation without persisting raw errors.
#[derive(Debug)]
pub struct ServiceStage(pub &'static str);
impl std::fmt::Display for ServiceStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ServiceStage {}
