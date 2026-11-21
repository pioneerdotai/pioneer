//! Read models for assistant output and provider-facing conversation history.
use anyhow::{Context, Result};
use pioneer_crud_artifacts::artifact::{
    ArtifactRepository, ConversationArtifactRef, ConversationArtifactRefLimits,
    ConversationTurnArtifactRefs,
};
use pioneer_crud_conversations::{
    collaboration, thread_metadata as thread, turn_finalization, turn_payload, turn_reads as turn,
};
use pioneer_crud_tasks::{
    delivery as task_delivery, status::task_status_from_db, task as task_repository, task_run,
};
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactRole, ArtifactStatus, TaskStatus,
    TurnItem, UserInput,
};
use pioneer_sqlite::SqliteDatabase;
use std::collections::{BTreeMap, HashMap, HashSet};

pub struct ConversationDisplayStore<'a> {
    connection: &'a SqliteDatabase,
}

fn terminal_task_history_marker(status: TaskStatus) -> Option<&'static str> {
    match status {
        TaskStatus::Cancelled => Some("[Task was cancelled by the user before completion.]"),
        TaskStatus::Failed => Some("[Task failed before producing a complete response.]"),
        TaskStatus::Blocked => Some("[Task was blocked before producing a complete response.]"),
        TaskStatus::Draft
        | TaskStatus::Scheduled
        | TaskStatus::Queued
        | TaskStatus::Running
        | TaskStatus::Waiting
        | TaskStatus::WaitingReview
        | TaskStatus::Completed => None,
    }
}

fn conversation_entry_has_assistant_content(entry: &ConversationEntry) -> bool {
    entry
        .assistant_text
        .as_deref()
        .is_some_and(|text| !text.trim().is_empty())
        || !entry.assistant_artifacts.is_empty()
}

fn conversation_entry_has_user_content(entry: &ConversationEntry) -> bool {
    entry
        .user_text
        .as_deref()
        .is_some_and(|text| !text.trim().is_empty())
        || !entry.user_artifacts.is_empty()
}

/// A single turn's conversation content: user input + assistant reply.
#[derive(Debug, Clone)]
pub struct ConversationEntry {
    pub turn_id: String,
    pub user_text: Option<String>,
    pub assistant_text: Option<String>,
    pub user_artifacts: Vec<ConversationArtifactRef>,
    pub assistant_artifacts: Vec<ConversationArtifactRef>,
}

impl<'a> ConversationDisplayStore<'a> {
    pub fn new(connection: &'a SqliteDatabase) -> Self {
        Self { connection }
    }

    async fn list_exact_artifact_refs(
        &self,
        workspace_id: &str,
        requested: &[(String, String)],
    ) -> Result<HashMap<(String, String), pioneer_protocol::ArtifactRef>> {
        ArtifactRepository::new()
            .list_exact_artifact_refs(self.connection, workspace_id, requested)
            .await
            .map_err(Into::into)
    }

    async fn list_conversation_artifact_refs(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_ids: &[String],
        limits: ConversationArtifactRefLimits,
    ) -> Result<BTreeMap<String, ConversationTurnArtifactRefs>> {
        ArtifactRepository::new()
            .list_conversation_artifact_refs(
                self.connection,
                workspace_id,
                thread_id,
                turn_ids,
                limits,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn get_final_agent_message_keys(
        &self,
        workspace: &str,
        targets: &[(String, String, String)],
    ) -> Result<HashSet<(String, String, String)>> {
        let targets = targets
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let rows = turn::exact_agent_messages(self.connection, workspace, &targets).await?;
        let mut keys = HashSet::new();
        for row in rows {
            let item: TurnItem = serde_json::from_str(&row.payload)
                .context("failed to decode exact HTML response source")?;
            anyhow::ensure!(
                item.item_id() == row.item_id,
                "HTML response source identity mismatch"
            );
            if matches!(
                item,
                TurnItem::AgentMessage {
                    phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                    ..
                }
            ) {
                keys.insert((row.thread_id, row.turn_id, row.item_id));
            }
        }
        Ok(keys)
    }

    pub async fn get_timeline_assistant_items(
        &self,
        workspace_id: &str,
        thread_id: &str,
        targets: &[(String, String)],
    ) -> Result<HashMap<(String, String), (TurnItem, Option<String>, bool)>> {
        let targets = targets
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let rows =
            turn::timeline_assistant_items(self.connection, workspace_id, thread_id, &targets)
                .await?;
        let mut result = HashMap::new();
        for row in rows {
            let item = serde_json::from_str::<TurnItem>(&row.payload).with_context(|| {
                format!(
                    "failed to decode timeline item `{}/{}`",
                    row.turn_id, row.item_id
                )
            })?;
            anyhow::ensure!(
                row.item_type == "agent_message"
                    && item.item_id() == row.item_id
                    && matches!(&item, TurnItem::AgentMessage { .. }),
                "timeline assistant item identity/type mismatch"
            );
            let is_last_final = row.is_last_final
                && matches!(
                    &item,
                    TurnItem::AgentMessage {
                        phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                        ..
                    }
                );
            result.insert(
                (row.turn_id, row.item_id),
                (item, row.status, is_last_final),
            );
        }
        Ok(result)
    }

    pub async fn get_turn_items_by_ids(
        &self,
        turn_id: &str,
        item_ids: &[String],
    ) -> Result<HashMap<String, pioneer_protocol::TurnItem>> {
        let models = turn::list_turn_items_by_ids(self.connection, turn_id, item_ids).await?;
        models
            .into_iter()
            .map(|model| {
                let item_id = model.item_id.clone();
                let item = turn_payload::decode_item(&model).with_context(|| {
                    format!(
                        "failed to decode turn_item payload for turn `{turn_id}` item `{item_id}`"
                    )
                })?;
                Ok((item_id, item))
            })
            .collect()
    }

    pub async fn list_turn_items_by_type_for_turns(
        &self,
        turn_ids: &[String],
        item_type: &str,
    ) -> Result<HashMap<String, Vec<TurnItem>>> {
        Ok(self
            .list_turn_items_with_status_by_type_for_turns(turn_ids, item_type)
            .await?
            .into_iter()
            .map(|(turn, items)| (turn, items.into_iter().map(|(item, _)| item).collect()))
            .collect())
    }

    pub async fn list_turn_items_with_status_by_type_for_turns(
        &self,
        turn_ids: &[String],
        item_type: &str,
    ) -> Result<HashMap<String, Vec<(TurnItem, Option<String>)>>> {
        let rows =
            turn::list_turn_items_by_type_for_turns(self.connection, turn_ids, item_type).await?;
        let mut items_by_turn = turn_ids
            .iter()
            .cloned()
            .map(|turn_id| (turn_id, Vec::new()))
            .collect::<HashMap<_, _>>();
        for model in rows {
            let turn_id = model.turn_id.clone();
            let item = turn_payload::decode_item(&model).with_context(|| {
                format!(
                    "failed to decode turn item payload for turn `{turn_id}` item `{}`",
                    model.item_id
                )
            })?;
            items_by_turn
                .entry(turn_id)
                .or_default()
                .push((item, model.status));
        }
        Ok(items_by_turn)
    }

    pub async fn list_completed_agent_messages(&self, turn_id: &str) -> Result<Vec<TurnItem>> {
        let rows = turn::find_completed_turn_items(self.connection, turn_id).await?;
        rows.into_iter()
            .map(|model| {
                turn_payload::decode_item(&model).with_context(|| {
                    format!(
                        "failed to decode completed agent message for turn `{turn_id}` item `{}`",
                        model.item_id
                    )
                })
            })
            .collect()
    }

    pub async fn completed_turn_final_answer(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TurnItem>> {
        let Some(completed_turn) =
            turn::find_turn_by_thread_and_id(self.connection, thread_id, turn_id).await?
        else {
            return Ok(None);
        };
        let Some(owner) = thread::find_thread_by_id(self.connection, thread_id).await? else {
            return Ok(None);
        };
        if completed_turn.status != "completed" || owner.workspace_id != workspace_id {
            return Ok(None);
        }
        if let Some(intent) = turn_finalization::find_by_turn_id(self.connection, turn_id).await? {
            if intent.status != turn_finalization::STATUS_COMMITTED
                || intent.committed_at.is_none()
                || intent.turn_id != turn_id
                || intent.thread_id != thread_id
                || intent.workspace_id != workspace_id
            {
                return Ok(None);
            }
            let notification = turn_finalization::notification_from_model(&intent)?;
            return Ok(matches!(
                &notification.item,
                TurnItem::AgentMessage { id, phase: pioneer_protocol::AgentMessagePhase::FinalAnswer, .. }
                    if id == &intent.item_id
            )
            .then_some(notification.item));
        }
        // Expand/migrate/contract compatibility with older workers. Neither
        // created_at ties nor item-id sorting establish which of several finals
        // was accepted, so ambiguity retains the existing missing-result path.
        let mut finals = self
            .list_completed_agent_messages(turn_id)
            .await?
            .into_iter()
            .filter(|item| {
                matches!(
                    item,
                    TurnItem::AgentMessage {
                        phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
                        ..
                    }
                )
            });
        let answer = finals.next();
        Ok(if finals.next().is_none() {
            answer
        } else {
            None
        })
    }

    pub async fn get_thread_conversation_history(
        &self,
        thread_id: &str,
        max_turns: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.get_thread_conversation_history_inner(None, thread_id, max_turns)
            .await
    }

    pub async fn get_thread_conversation_history_with_artifacts(
        &self,
        workspace_id: &str,
        thread_id: &str,
        max_turns: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.get_thread_conversation_history_inner(Some(workspace_id), thread_id, max_turns)
            .await
    }

    pub async fn get_thread_causally_closed_conversation_history(
        &self,
        thread_id: &str,
        max_entries: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.get_thread_causally_closed_conversation_history_inner(None, thread_id, max_entries)
            .await
    }

    pub async fn get_thread_causally_closed_conversation_history_with_artifacts(
        &self,
        workspace_id: &str,
        thread_id: &str,
        max_entries: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.get_thread_causally_closed_conversation_history_inner(
            Some(workspace_id),
            thread_id,
            max_entries,
        )
        .await
    }

    async fn get_thread_causally_closed_conversation_history_inner(
        &self,
        workspace_id: Option<&str>,
        thread_id: &str,
        max_entries: usize,
    ) -> Result<Vec<ConversationEntry>> {
        if max_entries == 0 {
            return Ok(Vec::new());
        }
        // A detached exchange occupies two durable turns before causal pairing:
        // the source Conversation turn and the delivered TaskRun turn.
        let raw_limit = max_entries.saturating_mul(2);
        let entries = self
            .get_thread_conversation_history_inner(workspace_id, thread_id, raw_limit)
            .await?;
        let mut entries = self
            .project_causally_closed_conversation_entries(thread_id, entries)
            .await?;
        if entries.len() > max_entries {
            entries.drain(..entries.len().saturating_sub(max_entries));
        }
        Ok(entries)
    }

    async fn get_thread_conversation_history_inner(
        &self,
        workspace_id: Option<&str>,
        thread_id: &str,
        max_turns: usize,
    ) -> Result<Vec<ConversationEntry>> {
        let turns =
            turn::find_terminal_turns_for_thread(self.connection, thread_id, max_turns as u64)
                .await?;
        let artifact_refs = if let Some(workspace_id) = workspace_id {
            let turn_ids = turns
                .iter()
                .map(|turn_model| turn_model.id.clone())
                .collect::<Vec<_>>();
            self.list_conversation_artifact_refs(
                workspace_id,
                thread_id,
                &turn_ids,
                ConversationArtifactRefLimits::default(),
            )
            .await
            .unwrap_or_default()
        } else {
            BTreeMap::new()
        };

        self.build_ordered_conversation_entries(&turns, &artifact_refs, workspace_id)
            .await
    }

    async fn project_causally_closed_conversation_entries(
        &self,
        thread_id: &str,
        entries: Vec<ConversationEntry>,
    ) -> Result<Vec<ConversationEntry>> {
        let entry_turn_ids = entries
            .iter()
            .map(|entry| entry.turn_id.clone())
            .collect::<Vec<_>>();
        // Task occurrences and delivery turns are transport records, not standalone
        // conversation turns. Recognize them from the whole window before looking
        // for their source command: a long-running Task can finish after its source
        // has already fallen outside the selected history window.
        let occurrence_turn_ids =
            task_run::list_runs_by_ids(self.connection, entry_turn_ids.as_slice())
                .await?
                .into_iter()
                .map(|run| run.id)
                .collect::<HashSet<_>>();
        let delivery_turn_ids = task_delivery::list_thread_deliveries_by_delivered_turns(
            self.connection,
            thread_id,
            entry_turn_ids.as_slice(),
        )
        .await?
        .into_iter()
        .filter_map(|delivery| delivery.delivered_turn_id)
        .collect::<HashSet<_>>();

        let user_only_turn_ids = entries
            .iter()
            .filter(|entry| {
                conversation_entry_has_user_content(entry)
                    && !conversation_entry_has_assistant_content(entry)
            })
            .map(|entry| entry.turn_id.clone())
            .collect::<Vec<_>>();
        if user_only_turn_ids.is_empty() {
            return Ok(entries
                .into_iter()
                .filter(|entry| {
                    !occurrence_turn_ids.contains(entry.turn_id.as_str())
                        && !delivery_turn_ids.contains(entry.turn_id.as_str())
                })
                .collect());
        }

        let tasks = task_repository::list_tasks_by_creator_turns(
            self.connection,
            thread_id,
            user_only_turn_ids.as_slice(),
        )
        .await?;
        if tasks.is_empty() {
            return Ok(entries
                .into_iter()
                .filter(|entry| {
                    !occurrence_turn_ids.contains(entry.turn_id.as_str())
                        && !delivery_turn_ids.contains(entry.turn_id.as_str())
                })
                .collect());
        }

        let task_source_turns = tasks
            .iter()
            .filter_map(|task| {
                task.created_by_turn_id
                    .as_ref()
                    .map(|turn_id| (task.id.clone(), turn_id.clone()))
            })
            .collect::<HashMap<_, _>>();
        let linked_source_turn_ids = task_source_turns.values().cloned().collect::<HashSet<_>>();
        let task_ids = task_source_turns.keys().cloned().collect::<Vec<_>>();
        let deliveries = task_delivery::list_delivered_thread_deliveries_for_tasks(
            self.connection,
            task_ids.as_slice(),
            thread_id,
            entry_turn_ids.as_slice(),
        )
        .await?;
        let entries_by_turn_id = entries
            .iter()
            .map(|entry| (entry.turn_id.clone(), entry.clone()))
            .collect::<HashMap<_, _>>();
        let mut delivered_turn_ids_by_source = HashMap::<String, Vec<String>>::new();
        let mut delivered_task_ids = HashSet::new();
        let mut consumed_delivery_turn_ids = HashSet::new();
        for delivery in deliveries {
            let Some(source_turn_id) = task_source_turns.get(delivery.task_id.as_str()) else {
                continue;
            };
            let Some(delivered_turn_id) = delivery.delivered_turn_id else {
                continue;
            };
            let Some(delivered_entry) = entries_by_turn_id.get(delivered_turn_id.as_str()) else {
                continue;
            };
            // Delivery status can race slightly ahead of the terminal item projection.
            // The source is closed only after the complete assistant payload is queryable.
            if !conversation_entry_has_assistant_content(delivered_entry) {
                continue;
            }
            delivered_turn_ids_by_source
                .entry(source_turn_id.clone())
                .or_default()
                .push(delivered_turn_id.clone());
            delivered_task_ids.insert(delivery.task_id);
            consumed_delivery_turn_ids.insert(delivered_turn_id);
        }
        let mut terminal_outcomes_by_source = HashMap::<String, Vec<String>>::new();
        for task in &tasks {
            if delivered_task_ids.contains(task.id.as_str()) {
                continue;
            }
            let Some(status) = task_status_from_db(task.status.as_str()) else {
                continue;
            };
            let Some(marker) = terminal_task_history_marker(status) else {
                continue;
            };
            let Some(source_turn_id) = task_source_turns.get(task.id.as_str()) else {
                continue;
            };
            terminal_outcomes_by_source
                .entry(source_turn_id.clone())
                .or_default()
                .push(marker.to_owned());
        }

        let mut projected = Vec::with_capacity(entries.len());
        for entry in entries {
            if delivery_turn_ids.contains(entry.turn_id.as_str())
                || consumed_delivery_turn_ids.contains(entry.turn_id.as_str())
                || occurrence_turn_ids.contains(entry.turn_id.as_str())
            {
                continue;
            }
            let is_user_only = conversation_entry_has_user_content(&entry)
                && !conversation_entry_has_assistant_content(&entry);
            if !is_user_only || !linked_source_turn_ids.contains(entry.turn_id.as_str()) {
                projected.push(entry);
                continue;
            }
            let delivered_turn_ids = delivered_turn_ids_by_source.get(entry.turn_id.as_str());
            let terminal_outcomes = terminal_outcomes_by_source.get(entry.turn_id.as_str());
            if delivered_turn_ids.is_none() && terminal_outcomes.is_none() {
                // An unfinished sibling Task is not ordinary conversation history.
                continue;
            }

            let mut closed_entry = entry;
            let mut assistant_text = Vec::new();
            let mut assistant_artifacts = Vec::new();
            if let Some(delivered_turn_ids) = delivered_turn_ids {
                for delivered_turn_id in delivered_turn_ids {
                    let Some(delivered_entry) = entries_by_turn_id.get(delivered_turn_id.as_str())
                    else {
                        continue;
                    };
                    if let Some(text) = delivered_entry
                        .assistant_text
                        .as_deref()
                        .map(str::trim)
                        .filter(|text| !text.is_empty())
                    {
                        assistant_text.push(text.to_owned());
                    }
                    assistant_artifacts.extend(delivered_entry.assistant_artifacts.iter().cloned());
                }
            }
            if let Some(terminal_outcomes) = terminal_outcomes {
                assistant_text.extend(terminal_outcomes.iter().cloned());
            }
            if !assistant_text.is_empty() {
                closed_entry.assistant_text = Some(assistant_text.join("\n\n"));
            }
            closed_entry.assistant_artifacts = assistant_artifacts;
            projected.push(closed_entry);
        }
        Ok(projected)
    }

    async fn build_ordered_conversation_entries(
        &self,
        turns: &[pioneer_entity::turn::Model],
        artifact_refs: &BTreeMap<String, ConversationTurnArtifactRefs>,
        workspace_id: Option<&str>,
    ) -> Result<Vec<ConversationEntry>> {
        let mut ordered_entries = Vec::with_capacity(turns.len());

        for turn_model in turns {
            let collaboration = collaboration::collaboration_from_model(turn_model)?;
            let is_message = collaboration.mode.message_mutation_eligible;
            let inputs = if is_message && collaboration.message_deleted {
                Vec::new()
            } else {
                turn::find_turn_inputs(self.connection, &turn_model.id).await?
            };
            let user_text: String = inputs
                .iter()
                .filter(|i| i.input_type == "text")
                .filter_map(|i| i.text.as_deref())
                .collect::<Vec<_>>()
                .join("\n");

            let items = turn::find_completed_turn_items(self.connection, &turn_model.id).await?;
            let assistant_text: String = items
                .iter()
                .filter_map(|item| turn_payload::decode_item(&item).ok())
                .filter_map(|item| match item {
                    pioneer_protocol::TurnItem::AgentMessage { text, .. } => Some(text),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("");
            let mut refs = artifact_refs
                .get(&turn_model.id)
                .cloned()
                .unwrap_or_default();
            if is_message {
                // Message is the only mutable send mode. Its context must use
                // the exact refs in the current turn_input projection rather
                // than immutable upload/execution bindings from initial send.
                refs.assistant.clear();
                refs.user.clear();
                if !collaboration.message_deleted
                    && let Some(workspace_id) = workspace_id
                {
                    let requested = inputs
                        .iter()
                        .filter_map(|input| serde_json::from_str::<UserInput>(&input.payload).ok())
                        .filter_map(|input| match input {
                            UserInput::Artifact {
                                artifact_id,
                                version_id: Some(version_id),
                            } => Some((artifact_id, version_id)),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    let exact = self
                        .list_exact_artifact_refs(workspace_id, requested.as_slice())
                        .await?;
                    for (index, key) in requested.iter().enumerate() {
                        let Some(artifact) = exact.get(key) else {
                            continue;
                        };
                        if artifact.status != ArtifactStatus::Ready {
                            continue;
                        }
                        refs.user.push(ConversationArtifactRef {
                            artifact_id: artifact.artifact_id.clone(),
                            version_id: artifact.version_id.clone(),
                            display_name: artifact.display_name.clone(),
                            kind: artifact.kind,
                            mime_type: artifact.mime_type.clone(),
                            size_bytes: artifact.size_bytes,
                            sha256: artifact.sha256.clone(),
                            binding_kind: ArtifactBindingKind::UserInput,
                            direction: ArtifactBindingDirection::Input,
                            role: Some(ArtifactRole::User),
                            turn_id: Some(turn_model.id.clone()),
                            message_id: None,
                            turn_item_id: None,
                            item_index: i64::try_from(index).ok(),
                        });
                    }
                }
            }

            let has_user_message = !user_text.is_empty() || !refs.user.is_empty();
            let has_assistant_message = !assistant_text.is_empty() || !refs.assistant.is_empty();
            let effective_timestamp_millis = if has_user_message {
                turn_model.created_at.timestamp_millis()
            } else if has_assistant_message {
                items
                    .first()
                    .map(|item| item.created_at.timestamp_millis())
                    .unwrap_or_else(|| turn_model.created_at.timestamp_millis())
            } else {
                turn_model.created_at.timestamp_millis()
            };
            let message_rank = if has_user_message {
                0_u8
            } else if has_assistant_message {
                1_u8
            } else {
                2_u8
            };

            ordered_entries.push((
                effective_timestamp_millis,
                message_rank,
                turn_model.id.clone(),
                ConversationEntry {
                    turn_id: turn_model.id.clone(),
                    user_text: if has_user_message {
                        Some(user_text)
                    } else {
                        None
                    },
                    assistant_text: if has_assistant_message {
                        Some(assistant_text)
                    } else {
                        None
                    },
                    user_artifacts: refs.user,
                    assistant_artifacts: refs.assistant,
                },
            ));
        }

        // A foreground turn stores its user input and assistant response together, so ordering
        // turns by `turn.created_at` used to be sufficient. Detached Composer work deliberately
        // splits that pair into two parent turns: a message-only Conversation turn and an
        // assistant-only TaskRun occurrence. Both are admitted in the same second, while the
        // delivered AgentMessage is created later. Order assistant-only entries by the message's
        // own delivery timestamp so provider history follows the same chronology as the durable
        // conversation instead of reversing equal-timestamp request/result pairs.
        ordered_entries.sort_by(|left, right| {
            (left.0, left.1, left.2.as_str()).cmp(&(right.0, right.1, right.2.as_str()))
        });

        Ok(ordered_entries
            .into_iter()
            .map(|(_, _, _, entry)| entry)
            .collect())
    }

    pub async fn get_first_thread_user_text(&self, thread_id: &str) -> Result<Option<String>> {
        let Some(turn_model) =
            turn::find_oldest_user_conversation_turn(self.connection, thread_id).await?
        else {
            return Ok(None);
        };

        let inputs = turn::find_turn_inputs(self.connection, &turn_model.id).await?;
        for input in inputs {
            if input.input_type != "text" {
                continue;
            }
            if let Some(text) = input.text {
                let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
                if !normalized.is_empty() {
                    return Ok(Some(normalized));
                }
            }
        }

        Ok(None)
    }
}
