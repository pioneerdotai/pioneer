//! Authority checks for creating an agent-owned thread.
use crate::agent_delegation_route::AgentDelegationRouteInput;
use anyhow::{Context, Result, bail};
use pioneer_crud_access::membership::PersistedThreadAccessClass;
use pioneer_protocol::{AgentExecutionId, Thread};
use sea_orm::DatabaseTransaction;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

#[derive(Debug, Clone)]
pub struct AgentThreadCreationCommitInput<'a> {
    pub thread: &'a Thread,
    pub execution_id: AgentExecutionId,
    pub route: Option<AgentDelegationRouteInput>,
    pub lineage: Option<pioneer_protocol::TaskThreadLineage>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

pub async fn validate_agent_thread_creation(
    db: &DatabaseTransaction,
    input: &AgentThreadCreationCommitInput<'_>,
) -> Result<PersistedThreadAccessClass> {
    let execution =
        crate::agent_execution::load_agent_execution(db, &input.execution_id.to_string())
            .await
            .context("failed to validate agent Thread creator execution")?
            .context("agent Thread creator execution is missing")?;
    if execution.workspace_id != input.thread.workspace_id {
        bail!("agent Thread creator execution belongs to another workspace");
    }
    match (&input.lineage, &input.route) {
        (Some(lineage), None) => {
            if lineage.child_thread_id != input.thread.id
                || lineage.root_thread_id != execution.home_root_thread_id
                || lineage.created_by_turn_id.is_none()
                || lineage.created_by_thread_id.as_deref()
                    != Some(lineage.parent_thread_id.as_str())
                || lineage.depth < 1
            {
                bail!("agent internal Thread lineage differs from its exact creator capsule");
            }
            let parent = pioneer_crud_conversations::thread_metadata::find_thread_by_id(
                db,
                &lineage.parent_thread_id,
            )
            .await
            .context("failed to validate agent internal Thread parent")?
            .context("agent internal Thread parent is missing")?;
            if parent.workspace_id != execution.workspace_id {
                bail!("agent internal Thread parent belongs to another workspace");
            }
            if lineage.parent_thread_id == execution.home_root_thread_id {
                if lineage.depth != 1 {
                    bail!("agent internal Thread root-child depth is invalid");
                }
            } else {
                let parent_lineage =
                    pioneer_crud_conversations::thread_lineage::find_lineage_by_child_thread(
                        db,
                        &lineage.parent_thread_id,
                    )
                    .await
                    .context("failed to validate agent internal Thread parent lineage")?
                    .context("agent internal Thread parent has no durable lineage")?;
                if parent_lineage.root_thread_id != execution.home_root_thread_id
                    || parent_lineage.depth.checked_add(1) != Some(lineage.depth)
                {
                    bail!("agent internal Thread parent lineage is outside its capsule");
                }
            }
        }
        (None, Some(route))
            if route.destination_thread_id == input.thread.id
                && route.source_execution_id == execution.id
                && route.source_capsule_id.as_deref()
                    == Some(execution.home_root_thread_id.as_str())
                && route.destination_capsule_id.as_deref() == Some(input.thread.id.as_str()) => {}
        _ => bail!("agent Thread creation must persist exact internal lineage or a new-root route"),
    }

    Ok(if input.lineage.is_some() {
        PersistedThreadAccessClass::Internal
    } else {
        PersistedThreadAccessClass::Workspace
    })
}
