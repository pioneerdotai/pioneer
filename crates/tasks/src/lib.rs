mod actor_contract;
mod admission;
mod error;
mod event_bus;
mod executor;
mod invariant;
mod notifications;
mod policy;
mod ports;
mod projector;
mod reconciliation;
mod review;
mod scheduler;
mod service;
mod task_boundary;
mod trigger;
mod wait;

#[cfg(test)]
mod tests;

pub use actor_contract::{build_occurrence_contract, build_task_actor_contract};
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
pub use policy::{
    TaskAgentAuthorizationGrantSeed, TaskCreateContext, TaskExecutionAdmissionSeed,
    TaskMutationContext, TaskRunConversationSnapshotSeed, TaskWaitActivityObserver,
    TaskWaitContext, default_delivery_policy, default_lifecycle_policy,
};
pub use projector::TaskProjector;
pub use reconciliation::{ReconciliationReport, TaskStartupReconciler};
pub use review::{
    CreateTaskResultReviewerContextParams, RecordTaskResultReviewEventParams,
    RecordUserTaskResultReviewEventParams, TaskResultReviewActor, TaskResultReviewBlockReason,
    TaskResultReviewCandidateResolution, TaskResultReviewFinalActor,
    TaskResultReviewRecordResponse, TaskResultReviewResolutionState, TaskResultReviewerContext,
    evaluate_task_result_review_resolution, stable_review_thread_id, stable_review_turn_id,
    task_result_reviewer_spec_key,
};
pub use scheduler::{TASK_EXECUTION_LEASE_SECONDS, TaskScheduler, TaskSchedulerHandle};
pub use service::{
    TaskReviewRuntimeConfig, TaskRuntime, TaskRuntimeConfig, TaskService, WriteLockDecision,
};
pub use trigger::TaskTriggerCalculator;
