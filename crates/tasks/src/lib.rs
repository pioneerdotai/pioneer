mod error;
mod event_bus;
mod executor;
mod invariant;
mod notifications;
mod policy;
mod ports;
mod projector;
mod reconciliation;
mod scheduler;
mod service;
mod trigger;
mod wait;

#[cfg(test)]
mod tests;

pub use error::{TaskRuntimeError, TaskRuntimeResult};
pub use event_bus::{
    TaskEventBus, TaskEventFilter, TaskEventSubscription, TaskEventWake, TaskEventWakeDelivery,
};
pub use executor::{
    TaskExecutionContext, TaskExecutionHandle, TaskExecutor, TaskExecutorRecoveryOutcome,
    TaskExecutorRegistry, TaskExecutorStartOutcome,
};
pub use invariant::{
    TaskRuntimeChildLinkRef, TaskRuntimeInvariantReport, TaskRuntimeInvariantScanner,
    TaskRuntimeInvariantSeverity, TaskRuntimeInvariantViolation, TaskRuntimeInvariantViolationKind,
};
pub use notifications::TaskNotificationMapper;
pub use policy::{TaskCreateContext, TaskMutationContext, TaskWaitContext};
pub use projector::TaskProjector;
pub use reconciliation::{ReconciliationReport, TaskStartupReconciler};
pub use scheduler::{TASK_EXECUTION_LEASE_SECONDS, TaskScheduler, TaskSchedulerHandle};
pub use service::{TaskRuntime, TaskService, WriteLockDecision};
pub use trigger::TaskTriggerCalculator;
