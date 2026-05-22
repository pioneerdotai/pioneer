mod active_recall;
mod artifact_store;
mod capabilities;
mod config;
mod constants;
mod context;
mod diagnostics;
mod handlers;
mod inputs;
mod labels;
mod package;
mod policy;
mod policy_codec;
mod post_turn;
mod post_turn_eligibility;
mod prompt_context;
mod providers;
mod recall;
mod state;
mod synthesis;
mod tools;

#[cfg(test)]
mod tests;

use crate::{MemoryModeRecallParams, MemoryRecallMode, MemoryRecallTarget};
use pioneer_hooks::HookHandler;
use pioneer_hooks::{
    AuditContribution, HookActorKind, HookAuditEventKind, HookAwaitPolicy, HookCapabilities,
    HookCapability, HookContextMode, HookContribution, HookContributionId, HookDefinition,
    HookDiagnostic, HookDiagnosticCode, HookDiagnosticMessage, HookDiagnosticSeverity, HookDomain,
    HookError, HookExecutionPolicy, HookFailurePolicy, HookHandlerRequest, HookHandlerResponse,
    HookId, HookInputPayload, HookKind, HookMetadata, HookMetadataKey, HookPackage, HookPhase,
    HookPolicyKey, HookPolicySet, HookPromptContent, HookRegistryError, HookResult,
    HookRetryBackoff, HookRetryPolicy, HookSectionId, HookSourceId, HookSourceKind, HookSourceRef,
    HookSubscription, HookSubscriptionDependencies, HookSubscriptionId, HookSubscriptionVisibility,
    HookToolBundleId, HookToolName, HookValue, PolicyContribution, PromptContextContribution,
    PromptSectionContribution, ToolBundleContribution, TurnPostPreflightPromptContextHookInput,
    TurnPostTurnDomain, TurnPostTurnHookInput, TurnPostTurnStatus, TurnPrePolicyHookInput,
    TurnPrePromptCompileHookInput, TurnPrePromptContextHookInput,
};
#[cfg(test)]
use pioneer_promt::MemoryRecallPromptItem;
use pioneer_promt::{
    MemoryActiveRecallPlannerPromptInput, MemoryPostTurnExtractorPromptInput,
    MemoryRecallPromptContextBlock, MemoryRecallPromptInput, MemoryRecallPromptPolicy,
    render_memory_active_recall_planner_prompt, render_memory_post_turn_extractor_prompt,
    render_memory_recall_prompt,
};
use pioneer_protocol::{
    MemoryActor, MemoryActorKind, MemoryAttribute, MemoryCandidateStatus, MemoryCategory,
    MemoryDurability, MemoryExplicitness, MemoryFactClass, MemoryIntent, MemoryProvenance,
    MemoryScope, MemoryScopeHint, MemoryScopeKind, MemorySemanticFields,
    MemorySemanticWriteDisposition, MemorySemanticWriteParams, MemorySemanticWriteResponse,
    MemorySensitivityHint, MemorySourceContextKind, MemoryStatus, MemorySubject,
    MemoryWriteEvidence, MemoryWriteRelation, ThreadMode,
};
use pioneer_tools::ToolExtensionBundle;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use active_recall::*;
pub use active_recall::{
    ActiveMemoryDecision, ActiveMemoryDecisionReasonCode, ActiveMemoryDecisionReasonCodeJson,
    ActiveMemoryDecisionStatus, ActiveRecallMode, ActiveRecallPlan, ActiveRecallPlanJson,
    ActiveRecallPlanJsonStatus, ActiveRecallTarget, active_recall_planned_query_count,
    normalize_active_recall_plan, parse_active_memory_decision_json,
};
pub use artifact_store::*;
use capabilities::*;
pub use config::*;
pub use constants::*;
pub use context::*;
use diagnostics::*;
use handlers::*;
use inputs::*;
use labels::*;
pub use package::*;
pub use policy::*;
pub use policy_codec::memory_turn_policy_from_hook_policy_set;
use policy_codec::*;
use post_turn::*;
use post_turn_eligibility::*;
use prompt_context::*;
pub use providers::*;
use recall::*;
pub use recall::{
    DeterministicRecallContextSummary, MemoryActiveRecallLocalPlan,
    MemoryActiveRecallProviderFallbackContext, active_recall_preflight_provider_fallback,
    build_active_recall_local_preflight_plan, deterministic_recall_context_summary,
};
use state::*;
use synthesis::*;
use tools::*;
