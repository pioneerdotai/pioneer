//! Read-only Thread views and their latest canonical and Composer snapshots.
use anyhow::{Context, Result, bail};
use pioneer_crud_conversations::{
    protocol_projection::{attach_thread_composer_snapshot, thread_from_db_model},
    thread_metadata, thread_state, turn_order, turn_reads,
    values::turn_kind_to_db,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{PrincipalId, Thread, TurnKind};
use std::collections::HashMap;

pub struct ThreadDisplayStore<'a> {
    connection: &'a PostgresDatabase,
}

impl<'a> ThreadDisplayStore<'a> {
    pub fn new(connection: &'a PostgresDatabase) -> Self {
        Self { connection }
    }

    pub async fn get_thread_model(&self, thread_id: &str) -> Result<Option<Thread>> {
        let Some(model) = thread_metadata::find_thread_by_id(self.connection, thread_id).await?
        else {
            return Ok(None);
        };
        let Some(mut thread) = thread_from_db_model(model)? else {
            return Ok(None);
        };

        self.attach_latest_thread_turn_snapshot(&mut thread).await?;

        Ok(Some(thread))
    }

    pub async fn list_accessible_threads_for_principal(
        &self,
        principal_id: &PrincipalId,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<Thread>> {
        let models = pioneer_crud_access::membership::list_accessible_threads_for_principal(
            self.connection,
            principal_id,
            workspace_id,
            limit,
        )
        .await?;
        let mut threads = Vec::with_capacity(models.len());
        let mut creator_executions = HashMap::new();
        for model in models {
            if model.created_by_actor_kind.as_deref() == Some("agent_execution") {
                creator_executions.insert(
                    model.id.clone(),
                    model
                        .created_by_actor_id
                        .clone()
                        .context("agent-created thread has no creator execution")?,
                );
            }
            let Some(thread) = thread_from_db_model(model)? else {
                continue;
            };
            threads.push(thread);
        }
        self.attach_latest_thread_turn_snapshots(&mut threads, &creator_executions)
            .await?;
        Ok(threads)
    }

    pub async fn list_threads_for_workspace(
        &self,
        workspace_id: &str,
        limit: u64,
    ) -> Result<Vec<Thread>> {
        let models =
            thread_state::list_threads_by_workspace(self.connection, workspace_id, limit).await?;
        let mut threads = Vec::with_capacity(models.len());
        let mut creator_executions = HashMap::new();

        for model in models {
            if model.created_by_actor_kind.as_deref() == Some("agent_execution") {
                creator_executions.insert(
                    model.id.clone(),
                    model
                        .created_by_actor_id
                        .clone()
                        .context("agent-created thread has no creator execution")?,
                );
            }
            let Some(thread) = thread_from_db_model(model)? else {
                continue;
            };
            threads.push(thread);
        }

        self.attach_latest_thread_turn_snapshots(&mut threads, &creator_executions)
            .await?;
        Ok(threads)
    }

    pub async fn attach_latest_thread_turn_snapshots(
        &self,
        threads: &mut [Thread],
        creator_executions: &HashMap<String, String>,
    ) -> Result<()> {
        for batch in threads.chunks_mut(turn_order::THREAD_TURN_SNAPSHOT_BATCH_SIZE) {
            let thread_ids = batch
                .iter()
                .map(|thread| thread.id.as_str())
                .collect::<Vec<_>>();
            let latest =
                turn_order::find_latest_turns_for_threads(self.connection, &thread_ids, false)
                    .await?;
            // Only non-Conversation history markers need a Composer fallback.
            // Keep NULL from the newest Conversation; never seek a non-NULL value.
            let conversation_ids = latest
                .iter()
                .filter(|model| model.turn_kind != turn_kind_to_db(TurnKind::Conversation))
                .map(|model| model.thread_id.as_str())
                .collect::<Vec<_>>();
            let mut conversations =
                turn_order::find_latest_turns_for_threads(self.connection, &conversation_ids, true)
                    .await?
                    .into_iter()
                    .map(|model| (model.thread_id.clone(), model))
                    .collect::<HashMap<_, _>>();
            let mut latest = latest
                .into_iter()
                .map(|model| (model.thread_id.clone(), model))
                .collect::<HashMap<_, _>>();
            let empty_creator_ids = batch
                .iter()
                .filter(|thread| !latest.contains_key(&thread.id))
                .filter_map(|thread| creator_executions.get(&thread.id).map(String::as_str))
                .collect::<Vec<_>>();
            let creator_reasoning =
                pioneer_crud_execution::agent_graph_projection::agent_thread_creator_reasoning(
                    self.connection,
                    &empty_creator_ids,
                )
                .await?;

            // All DB capacity has been released. Attach in original list order
            // using the same snapshot conversion as the single-thread caller.
            for thread in batch {
                let Some(model) = latest.remove(thread.id.as_str()) else {
                    if let Some((workspace, reasoning)) = creator_executions
                        .get(&thread.id)
                        .and_then(|id| creator_reasoning.get(id))
                    {
                        if workspace != &thread.workspace_id {
                            bail!("thread creator execution workspace differs");
                        }
                        thread.reasoning_effort = reasoning.clone();
                    }
                    continue;
                };
                let conversation = if model.turn_kind == turn_kind_to_db(TurnKind::Conversation) {
                    Some(model.clone())
                } else {
                    conversations.remove(thread.id.as_str())
                };
                attach_thread_composer_snapshot(thread, model, conversation.as_ref())?;
            }
        }
        Ok(())
    }

    async fn attach_latest_thread_turn_snapshot(&self, thread: &mut Thread) -> Result<()> {
        let Some(turn_model) =
            turn_reads::find_latest_turn_for_thread(self.connection, thread.id.as_str()).await?
        else {
            thread.reasoning_effort =
                pioneer_crud_execution::agent_graph_projection::empty_agent_thread_reasoning(
                    self.connection,
                    &thread.id,
                )
                .await?;
            return Ok(());
        };

        // TaskRun occurrence turns are timeline/lifecycle markers, not Composer
        // selections. In collaborative threads they are newer than the source
        // user Conversation turn and intentionally carry no reasoning effort.
        // Keep the newest turn as the status marker, but restore Composer
        // metadata from the newest Conversation turn. A null value there is
        // meaningful: it explicitly restores the provider default instead of
        // leaking an older non-default effort into the thread.
        let conversation = if turn_model.turn_kind == turn_kind_to_db(TurnKind::Conversation) {
            Some(turn_model.clone())
        } else {
            turn_reads::find_latest_conversation_turn_for_thread(
                self.connection,
                thread.id.as_str(),
            )
            .await?
        };
        attach_thread_composer_snapshot(thread, turn_model, conversation.as_ref())?;

        Ok(())
    }
}

#[cfg(test)]
#[path = "thread_display_tests.rs"]
mod tests;
