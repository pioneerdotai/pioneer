//! Canonical persisted actor projection for an admitted Agent execution.

use pioneer_protocol::{AgentExecutionId, PersistedActorRef};

pub(crate) fn exact_agent_actor(execution_id: AgentExecutionId) -> PersistedActorRef {
    PersistedActorRef::AgentExecution(execution_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        AgentActionId, AgentAuthoredInput, AgentAuthoredTurnProjection, AgentIdentityId,
        AgentIdentitySourceKind, AgentPresentationSnapshot, PrincipalId, ThreadMode,
    };

    fn snapshot() -> AgentPresentationSnapshot {
        AgentPresentationSnapshot {
            agent_identity_id: AgentIdentityId::new("A12345678901234567890").unwrap(),
            agent_execution_id: AgentExecutionId::new("E12345678901234567890").unwrap(),
            identity_source_kind: AgentIdentitySourceKind::NativeAgent,
            identity_source_revision: 1,
            display_name: "Worker".to_owned(),
            nickname: "worker".to_owned(),
            avatar_revision: None,
            role_label: None,
        }
    }

    #[test]
    fn controller_and_visible_author_are_distinct() {
        let controller = PrincipalId::new("P12345678901234567890").unwrap();
        let projection = AgentAuthoredTurnProjection::new(
            AgentActionId::new("X12345678901234567890").unwrap(),
            &snapshot(),
            ThreadMode::Chat,
            AgentAuthoredInput::default(),
            Some(controller.clone()),
        )
        .unwrap();
        assert_eq!(
            projection.author.actor,
            PersistedActorRef::AgentExecution(projection.execution_id.clone())
        );
        assert_ne!(
            projection.author.actor,
            PersistedActorRef::Principal(controller)
        );
    }

    #[test]
    fn lifecycle_uses_system_only_for_runtime_events() {
        assert_eq!(
            exact_agent_actor(snapshot().agent_execution_id.clone()),
            PersistedActorRef::AgentExecution(snapshot().agent_execution_id)
        );
    }
}
