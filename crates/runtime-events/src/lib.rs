//! Runtime-neutral execution event ingress.
//!
//! Runtime adapters publish lifecycle events to the lossless durable lane and
//! high-frequency updates to the coalesced progress lane. A durable publish
//! flushes causally related progress first, so consumers never have to invent
//! provider-specific ordering rules.

mod hub;
mod observation;
mod ordered_ingress;
mod progress;
mod snapshot;

pub use hub::{DurableEventReceiver, ExecutionEventHub, ExecutionEventHubError};
pub use observation::{ExecutionTurnObservation, ExecutionTurnStatus};
pub use ordered_ingress::{
    OrderedEventIngress, OrderedIngressClass, OrderedIngressConfig, OrderedIngressEvent,
    OrderedIngressOffer,
};
pub use progress::{ProgressCoalescer, ProgressCoalescerConfig};
pub use snapshot::{ExecutionSnapshotEvent, SnapshotEventReceiver};

pub const DEFAULT_LIVE_EVENT_CHANNEL_CAPACITY: usize = 1024;
pub const DEFAULT_DURABLE_EVENT_CHANNEL_CAPACITY: usize = 1024;

pub type AgentEventHub = ExecutionEventHub;
pub type AgentEventHubError = ExecutionEventHubError;

#[cfg(test)]
mod tests;
