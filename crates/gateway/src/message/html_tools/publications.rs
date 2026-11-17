//! One enrichment pass for a timeline page, shared by CLI and API Turns.
use super::MessageProcessor;
use pioneer_protocol::{
    ArtifactBindingDirection, ArtifactBindingKind, ArtifactRole, HtmlPublication, TaskRunTurnKind,
    TaskValue,
};
use std::collections::{BTreeSet, HashMap};

type Source = (String, String);
type Response = (String, String);
type PublicationBatch = HashMap<Response, Vec<HtmlPublication>>;

impl MessageProcessor {
    pub(in crate::message) async fn html_publications_for_response_turn(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> anyhow::Result<Vec<HtmlPublication>> {
        Ok(self
            .html_publications_for_sources(workspace, &[(thread.into(), turn.into())])
            .await?
            .remove(&(thread.into(), turn.into()))
            .unwrap_or_default())
    }

    /// Standalone notifications/task result extraction use the same batched
    /// implementation; the timeline supplies its already-loaded response body.
    pub(in crate::message) async fn html_publications_for_agent_response(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> anyhow::Result<Vec<HtmlPublication>> {
        let final_response =
            if pioneer_protocol::task_delivery_id_from_result_item_id(item).is_some() {
                self.crud_store
                    .get_final_agent_message_keys(
                        workspace,
                        &[(thread.into(), turn.into(), item.into())],
                    )
                    .await?
                    .contains(&(thread.into(), turn.into(), item.into()))
            } else {
                true
            };
        Ok(self
            .html_publications_for_agent_responses(
                workspace,
                thread,
                &[(turn.into(), item.into(), final_response)],
            )
            .await?
            .remove(&(turn.into(), item.into()))
            .unwrap_or_default())
    }

    /// The bool is the canonical response's FinalAnswer phase, supplied by the
    /// page body read. A delivery record alone is never publication authority.
    pub(in crate::message) async fn html_publications_for_agent_responses(
        &self,
        workspace: &str,
        thread: &str,
        responses: &[(String, String, bool)],
    ) -> anyhow::Result<PublicationBatch> {
        let delivery_ids = responses
            .iter()
            .filter(|(_, _, final_response)| *final_response)
            .filter_map(|(_, item, _)| {
                pioneer_protocol::task_delivery_id_from_result_item_id(item).map(str::to_owned)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let deliveries = self
            .crud_store
            .get_task_deliveries_by_ids(workspace, thread, &delivery_ids)
            .await?;
        let mut sources_by_response = HashMap::<Response, Vec<Source>>::new();
        let mut legacy_sources = HashMap::<Response, (String, String, String)>::new();
        for (turn, item, final_response) in responses {
            let key = (turn.clone(), item.clone());
            let Some(id) = pioneer_protocol::task_delivery_id_from_result_item_id(item) else {
                sources_by_response.insert(key, vec![(thread.into(), turn.clone())]);
                continue;
            };
            if !final_response {
                continue;
            }
            let Some(delivery) = deliveries.get(id) else {
                continue;
            };
            if delivery.error_snapshot.is_some()
                || delivery
                    .delivered_turn_id
                    .as_deref()
                    .is_some_and(|id| id != turn)
            {
                continue;
            }
            let Some(result) = &delivery.result_snapshot else {
                continue;
            };
            let mut sources = BTreeSet::new();
            for artifact in &result.artifacts {
                if let Some(TaskValue::Object(metadata)) = &artifact.metadata {
                    if let (Some(TaskValue::String(thread)), Some(TaskValue::String(turn))) = (
                        metadata.get("htmlSourceThreadId"),
                        metadata.get("htmlSourceTurnId"),
                    ) {
                        sources.insert((thread.clone(), turn.clone()));
                    }
                }
            }
            if result.artifacts.is_empty() {
                if let Some(TaskValue::Object(data)) = &result.data {
                    if data.get("fallbackUsed") == Some(&TaskValue::Bool(true))
                        && data.get("schemaValid") == Some(&TaskValue::Bool(false))
                    {
                        if let (
                            Some(TaskValue::String(thread)),
                            Some(TaskValue::String(turn)),
                            Some(TaskValue::String(item)),
                        ) = (
                            data.get("sourceThreadId"),
                            data.get("sourceTurnId"),
                            data.get("sourceItemId"),
                        ) {
                            legacy_sources
                                .insert(key.clone(), (thread.clone(), turn.clone(), item.clone()));
                        }
                    }
                }
            }
            sources_by_response.insert(key, sources.into_iter().collect());
        }
        let legacy_targets = legacy_sources.values().cloned().collect::<Vec<_>>();
        let committed_sources = self
            .crud_store
            .get_final_agent_message_keys(workspace, &legacy_targets)
            .await?;
        for (response, source) in &legacy_sources {
            if committed_sources.contains(source) {
                sources_by_response
                    .entry(response.clone())
                    .or_default()
                    .push((source.0.clone(), source.1.clone()));
            }
        }
        let delivery_sources = sources_by_response
            .iter()
            .filter(|((_, item), _)| {
                pioneer_protocol::task_delivery_id_from_result_item_id(item).is_some()
            })
            .flat_map(|(_, sources)| sources.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let executions = self
            .crud_store
            .get_task_run_turns_by_sources(workspace, &delivery_sources)
            .await?;
        let legacy_execution_ids = legacy_sources
            .values()
            .filter_map(|s| executions.get(&(s.0.clone(), s.1.clone())))
            .map(|e| e.id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let candidates = self
            .crud_store
            .get_task_result_candidates_by_turns(&legacy_execution_ids)
            .await?;
        for ((_, item), sources) in &mut sources_by_response {
            let Some(id) = pioneer_protocol::task_delivery_id_from_result_item_id(item) else {
                continue;
            };
            let delivery = &deliveries[id];
            let result = delivery
                .result_snapshot
                .as_ref()
                .expect("selected delivery result");
            sources.retain(|source| {
                let Some(execution) = executions.get(source) else {
                    return false;
                };
                if execution.task_id != delivery.task_id
                    || execution.run_id != delivery.run_id
                    || execution.kind == TaskRunTurnKind::Review
                {
                    return false;
                }
                !result.artifacts.is_empty()
                    || candidates.get(&execution.id).is_some_and(|candidate| {
                        candidate.task_id == delivery.task_id
                            && candidate.run_id == delivery.run_id
                            && candidate.thread_id == source.0
                            && candidate.turn_id == source.1
                            && candidate.result.as_ref() == Some(result)
                    })
            });
        }
        let sources = sources_by_response
            .values()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let publications = self
            .html_publications_for_sources(workspace, &sources)
            .await?;
        let mut batch = HashMap::new();
        for (response, sources) in sources_by_response {
            let result = pioneer_protocol::task_delivery_id_from_result_item_id(&response.1)
                .and_then(|id| deliveries.get(id))
                .and_then(|d| d.result_snapshot.as_ref());
            let mut selected = Vec::new();
            for source in sources {
                for publication in publications.get(&source).into_iter().flatten() {
                    if result.is_none_or(|result| {
                        result.artifacts.is_empty()
                            || result.artifacts.iter().any(|artifact| {
                                artifact.artifact_id.as_ref()
                                    == Some(&publication.artifact.artifact_id)
                                    && artifact.version_id == publication.artifact.version_id
                            })
                    }) {
                        selected.push(publication.clone());
                    }
                }
            }
            batch.insert(response, selected);
        }
        Ok(batch)
    }

    async fn html_publications_for_sources(
        &self,
        workspace: &str,
        sources: &[Source],
    ) -> anyhow::Result<HashMap<Source, Vec<HtmlPublication>>> {
        let summaries = self
            .crud_store
            .html_publication_summaries(workspace, sources)
            .await?;
        let mut batch = HashMap::new();
        for ((thread, turn), summaries) in summaries {
            let mut publications = Vec::new();
            for summary in summaries {
                let Some(publication) = HtmlPublication::from_summary(&summary, &thread, &turn)
                else {
                    continue;
                };
                let position = summary
                    .bindings
                    .iter()
                    .filter(|b| {
                        b.thread_id.as_deref() == Some(thread.as_str())
                            && b.turn_id.as_deref() == Some(turn.as_str())
                            && b.tool_call_id.as_deref() == Some(publication.call_id.as_str())
                            && b.version_id == publication.artifact.version_id
                            && b.binding_kind == ArtifactBindingKind::ToolOutput
                            && b.direction == ArtifactBindingDirection::Output
                            && b.role == Some(ArtifactRole::Tool)
                    })
                    .filter_map(|b| b.item_index)
                    .min()
                    .unwrap_or(i64::MAX);
                publications.push((
                    (
                        position,
                        summary.created_at,
                        publication.call_id.clone(),
                        publication.artifact.artifact_id.clone(),
                    ),
                    publication,
                ));
            }
            publications.sort_by(|(a, _), (b, _)| a.cmp(b));
            batch.insert(
                (thread, turn),
                publications.into_iter().map(|(_, p)| p).collect(),
            );
        }
        Ok(batch)
    }
}
