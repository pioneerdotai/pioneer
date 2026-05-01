mod error;
mod event_bus;
mod executor;
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
pub use event_bus::{TaskEventBus, TaskEventFilter, TaskEventSubscription};
pub use executor::{
    TaskExecutionContext, TaskExecutionHandle, TaskExecutor, TaskExecutorRecoveryOutcome,
    TaskExecutorRegistry, TaskExecutorStartOutcome,
};
pub use notifications::TaskNotificationMapper;
pub use policy::{TaskCreateContext, TaskMutationContext, TaskWaitContext};
pub use projector::TaskProjector;
pub use reconciliation::{ReconciliationReport, TaskStartupReconciler};
pub use scheduler::{TaskScheduler, TaskSchedulerHandle};
pub use service::{TaskRuntime, TaskService, WriteLockDecision};
pub use trigger::TaskTriggerCalculator;
