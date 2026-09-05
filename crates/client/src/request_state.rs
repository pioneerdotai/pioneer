//! Generation-safe request lifecycle shared by client capabilities.

use serde::{Deserialize, Serialize};

use crate::core::ClientGeneration;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientRequestFailure {
    code: String,
}

impl ClientRequestFailure {
    pub fn new(code: impl Into<String>) -> Option<Self> {
        let code = code.into();
        (!code.is_empty()).then_some(Self { code })
    }

    pub fn code(&self) -> &str {
        self.code.as_str()
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RequestStatus<T> {
    Idle,
    Pending {
        generation: ClientGeneration,
        attempt: u32,
    },
    Ready {
        generation: ClientGeneration,
        value: T,
    },
    Failed {
        generation: ClientGeneration,
        attempt: u32,
        error: ClientRequestFailure,
    },
    Cancelled {
        generation: ClientGeneration,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestUpdateOutcome {
    Changed,
    Noop,
    Stale,
    Rejected,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestState<T> {
    status: RequestStatus<T>,
    latest_generation: ClientGeneration,
}

impl<T> Default for RequestState<T> {
    fn default() -> Self {
        Self {
            status: RequestStatus::Idle,
            latest_generation: ClientGeneration::ZERO,
        }
    }
}

impl<T> RequestState<T> {
    pub const fn status(&self) -> &RequestStatus<T> {
        &self.status
    }

    pub const fn latest_generation(&self) -> ClientGeneration {
        self.latest_generation
    }

    pub fn start(&mut self) -> ClientGeneration {
        let next = ClientGeneration::new(
            self.latest_generation
                .get()
                .checked_add(1)
                .expect("Client request generation exhausted"),
        );
        self.latest_generation = next;
        self.status = RequestStatus::Pending {
            generation: next,
            attempt: 1,
        };
        next
    }

    pub fn retry(&mut self) -> Result<ClientGeneration, RequestUpdateOutcome> {
        let attempt = match &self.status {
            RequestStatus::Failed { attempt, .. } => attempt
                .checked_add(1)
                .expect("Client request attempt exhausted"),
            RequestStatus::Cancelled { .. } => 1,
            RequestStatus::Idle | RequestStatus::Pending { .. } | RequestStatus::Ready { .. } => {
                return Err(RequestUpdateOutcome::Rejected);
            }
        };
        let next = ClientGeneration::new(
            self.latest_generation
                .get()
                .checked_add(1)
                .expect("Client request generation exhausted"),
        );
        self.latest_generation = next;
        self.status = RequestStatus::Pending {
            generation: next,
            attempt,
        };
        Ok(next)
    }

    pub fn cancel(&mut self, generation: ClientGeneration) -> RequestUpdateOutcome {
        match &self.status {
            RequestStatus::Pending {
                generation: current,
                ..
            } if *current == generation => {
                self.status = RequestStatus::Cancelled { generation };
                RequestUpdateOutcome::Changed
            }
            RequestStatus::Cancelled {
                generation: current,
            } if *current == generation => RequestUpdateOutcome::Noop,
            _ if generation < self.latest_generation => RequestUpdateOutcome::Stale,
            _ => RequestUpdateOutcome::Rejected,
        }
    }

    pub fn succeed(&mut self, generation: ClientGeneration, value: T) -> RequestUpdateOutcome {
        if !self.is_current_pending(generation) {
            return self.non_pending_outcome(generation);
        }
        self.status = RequestStatus::Ready { generation, value };
        RequestUpdateOutcome::Changed
    }

    pub fn fail(
        &mut self,
        generation: ClientGeneration,
        error: ClientRequestFailure,
    ) -> RequestUpdateOutcome {
        let RequestStatus::Pending {
            generation: current,
            attempt,
        } = &self.status
        else {
            return self.non_pending_outcome(generation);
        };
        if *current != generation {
            return self.non_pending_outcome(generation);
        }
        self.status = RequestStatus::Failed {
            generation,
            attempt: *attempt,
            error,
        };
        RequestUpdateOutcome::Changed
    }

    fn is_current_pending(&self, generation: ClientGeneration) -> bool {
        matches!(
            self.status,
            RequestStatus::Pending {
                generation: current,
                ..
            } if current == generation
        )
    }

    fn non_pending_outcome(&self, generation: ClientGeneration) -> RequestUpdateOutcome {
        if generation < self.latest_generation {
            RequestUpdateOutcome::Stale
        } else if generation == self.latest_generation {
            RequestUpdateOutcome::Noop
        } else {
            RequestUpdateOutcome::Rejected
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_results_cannot_replace_a_newer_generation() {
        let mut request = RequestState::<String>::default();
        let first = request.start();
        assert_eq!(
            request.fail(
                first,
                ClientRequestFailure::new("offline").expect("failure code"),
            ),
            RequestUpdateOutcome::Changed
        );
        let second = request.retry().expect("failed requests are retryable");
        assert!(second > first);
        assert_eq!(
            request.succeed(first, "stale".to_owned()),
            RequestUpdateOutcome::Stale
        );
        assert_eq!(
            request.succeed(second, "fresh".to_owned()),
            RequestUpdateOutcome::Changed
        );
        assert_eq!(
            request.succeed(second, "duplicate".to_owned()),
            RequestUpdateOutcome::Noop
        );
    }

    #[test]
    fn cancellation_is_idempotent_and_late_completion_is_ignored() {
        let mut request = RequestState::<u64>::default();
        let generation = request.start();
        assert_eq!(request.cancel(generation), RequestUpdateOutcome::Changed);
        assert_eq!(request.cancel(generation), RequestUpdateOutcome::Noop);
        assert_eq!(request.succeed(generation, 7), RequestUpdateOutcome::Noop);
        let retry = request.retry().expect("cancelled requests are retryable");
        assert_eq!(
            request.fail(
                generation,
                ClientRequestFailure::new("late").expect("failure code"),
            ),
            RequestUpdateOutcome::Stale
        );
        assert_eq!(
            request.fail(
                retry,
                ClientRequestFailure::new("current").expect("failure code"),
            ),
            RequestUpdateOutcome::Changed
        );
    }

    #[test]
    fn request_failure_requires_a_non_empty_stable_code() {
        assert!(ClientRequestFailure::new("").is_none());
        assert_eq!(
            ClientRequestFailure::new("offline").unwrap().code(),
            "offline"
        );
    }
}
