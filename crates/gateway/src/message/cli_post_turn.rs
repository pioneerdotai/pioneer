//! CLI completion adapter for the shared durable post-turn hook mechanism.

use anyhow::{Context, Result, bail};
use pioneer_agent::post_turn::{post_turn_tool_error_class, post_turn_tool_outcome_status};
use pioneer_agent::{AgentTurnHookRuntimeContext, post_turn::CompletedTurnHookInput};
use pioneer_crud::CliRuntimeTurnBindingRecord;
use pioneer_hooks::{
    HookActorKind, TurnPostTurnHookInput, TurnPostTurnHookInputLimits, TurnPostTurnStatus,
};
use pioneer_hooks::{
    TurnPostTurnDomain, TurnPostTurnDomainEventSummary, TurnPostTurnToolEventSummary,
    TurnPostTurnToolStatus,
};
use pioneer_protocol::{AgentMessagePhase, TaskAttachmentMode, TurnItem};

use crate::message::MessageProcessor;

impl MessageProcessor {
    pub(crate) async fn prepare_cli_post_turn_hook(
        &self,
        binding: &CliRuntimeTurnBindingRecord,
        attempt_index: i64,
        final_item: Option<&TurnItem>,
    ) -> Result<()> {
        let final_id = final_item
            .map(TurnItem::item_id)
            .unwrap_or("no-final-message");
        let batch_id = format!(
            "{}:cli-post-turn:{}:{final_id}",
            binding.turn_id,
            attempt_index.max(1)
        );
        if self
            .crud_store
            .post_turn_batch_exists(&binding.turn_id, &batch_id)
            .await?
        {
            return Ok(());
        }
        let Some(runtime) = self
            .agent_manager
            .capture_post_turn_runtime()
            .await
            .map_err(anyhow::Error::msg)?
        else {
            return Ok(());
        };
        let authority = self
            .load_turn_execution_authorization_context(&binding.turn_id)
            .await?;
        if authority.workspace_id() != binding.workspace_id {
            bail!("CLI post-turn authority workspace differs from turn binding");
        }
        let runtime_context = if let Some(task_turn) = self
            .crud_store
            .get_task_run_turn_by_turn(&binding.thread_id, &binding.turn_id)
            .await?
        {
            let task = self
                .crud_store
                .get_task_record(&task_turn.task_id)
                .await?
                .context("CLI post-turn task is missing")?;
            if task.workspace_id != binding.workspace_id {
                bail!("CLI post-turn task workspace mismatch");
            }
            // The CLI binding freezes the actual execution destination at
            // admission; the task's creator/current routing is not its substitute.
            AgentTurnHookRuntimeContext::for_task_turn(
                &task.id,
                task.lifecycle_policy
                    .as_ref()
                    .map(|p| p.attachment)
                    .unwrap_or(TaskAttachmentMode::Detached),
                task_turn.kind,
                &binding.continuation_thread_id,
            )
        } else {
            AgentTurnHookRuntimeContext {
                actor_kind: HookActorKind::User,
                actor_id: Some(authority.initiating_principal_id().to_string()),
                conversation_thread_id: Some(authority.root_thread_id().to_owned()),
                ..AgentTurnHookRuntimeContext::default()
            }
        };
        let limits = TurnPostTurnHookInputLimits::default();
        let (user_text, user_text_truncated) = self
            .crud_store
            .post_turn_user_text(&binding.turn_id, limits.user_text_preview_max_chars)
            .await?;
        let assistant_text = match final_item {
            Some(TurnItem::AgentMessage {
                text,
                phase: AgentMessagePhase::FinalAnswer,
                ..
            }) => Some(text.as_str()),
            None => None,
            _ => bail!("CLI post-turn final item is not a final answer"),
        };
        let summaries = self
            .crud_store
            .post_turn_tool_summaries(&binding.turn_id, limits.tool_event_max_count + 1)
            .await?;
        let tool_events: Vec<_> = summaries
            .into_iter()
            .map(|s| TurnPostTurnToolEventSummary {
                item_id: s.item_id,
                item_type: format!("{:?}", s.item_type),
                tool_name: s.tool_name,
                attempt_number: s.attempt_number,
                status: if s.success {
                    TurnPostTurnToolStatus::Succeeded
                } else {
                    TurnPostTurnToolStatus::Failed
                },
                outcome_status: s.outcome_status.map(post_turn_tool_outcome_status),
                error_class: s.error_class.map(post_turn_tool_error_class),
            })
            .collect();
        let domain_events = tool_events
            .iter()
            .map(|s| TurnPostTurnDomainEventSummary {
                domain: TurnPostTurnDomain::Tool,
                code: Some(
                    if s.status == TurnPostTurnToolStatus::Succeeded {
                        "tool.succeeded"
                    } else {
                        "tool.failed"
                    }
                    .to_owned(),
                ),
                item_id: Some(s.item_id.clone()),
                message: None,
            })
            .collect();
        // CLI model names are not ProviderRegistry models. The extractor uses
        // its configured API provider/model; never pass a `cli:*` provider as
        // the native thread-model fallback.
        let mut input = TurnPostTurnHookInput::from_parts(
            TurnPostTurnStatus::Succeeded,
            Some(user_text),
            assistant_text,
            None::<String>,
            tool_events,
            domain_events,
            limits,
        );
        if let Some(text) = input.user_text.as_mut() {
            text.truncated |= user_text_truncated;
        }
        if let Some(mut preparation) = runtime
            .prepare_completed_turn_hook(CompletedTurnHookInput {
                workspace_id: binding.workspace_id.clone(),
                thread_id: binding.thread_id.clone(),
                turn_id: binding.turn_id.clone(),
                runtime_context,
                input,
            })
            .await
            .map_err(anyhow::Error::msg)?
        {
            preparation.batch_id = batch_id;
            self.crud_store
                .prepare_cli_post_turn_once(
                    preparation,
                    attempt_index.max(1),
                    chrono::Utc::now().timestamp(),
                )
                .await?;
        }
        Ok(())
    }
}
