use std::time::SystemTime;

/// Flow deadlines also use wall time: some platforms' monotonic clocks pause
/// during system sleep. The SDK remains responsible for absolute token expiry.
pub trait OAuthClock: Send + Sync {
    fn now(&self) -> SystemTime;
}
pub(crate) struct SystemOAuthClock;
impl OAuthClock for SystemOAuthClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}
