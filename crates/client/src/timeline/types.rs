//! Domain values carried by timeline rows, actions and author presentation.
pub use pioneer_protocol::{
    AgentExecutionId, AgentIdentityId, AgentIdentitySourceKind, AgentMessagePhase,
    AgentPresentationSnapshot, AuthorizationRolePresentation, CLAUDE_AGENT_AVATAR_REVISION,
    CODEX_AGENT_AVATAR_REVISION, MarkdownBlock, MarkdownDocument, MarkdownInline, MarkdownList,
    MarkdownMark, MarkdownMarkKind, MemberSummary, PIONEER_AGENT_AVATAR_REVISION,
    PersistedActorRef, PrincipalId, PrincipalKind, PrincipalStatus, RoleKey, SkillId, SkillPackId,
    SystemEventLevel, TaskAttachmentMode, TaskStatus, TaskTurnItem, ThreadMode,
    ThreadTimelinePageParams, TimelinePageAnchor, TurnAuthorSnapshot, TurnItem,
    TurnSkillCapabilitySummary, TurnSkillPackCapabilitySummary, TurnSkillPackPresentationSummary,
    TurnWorkState, UserMessageAttachment, WebSearchResultItem, WorkspaceId,
};
