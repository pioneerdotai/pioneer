//! Read-only projection of saved conversation records. The caller owns source
//! authorization and sealing cursor state before it crosses the tool boundary.
use super::MessageProcessor;
use anyhow::{Result, bail, ensure};
use pioneer_crud::{ProjectionVisibility, SavedHistoryKey, SavedHistoryRow, SavedHistorySnapshot};
use pioneer_protocol::{ThreadHistoryAnchor, ThreadHistoryDirection, TurnItem};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::future::Future;

const PAGE_BYTES: usize = 28 * 1024;
// Leave room for authenticated cursor framing within the 4096-token policy.
const PAGE_TOKENS: u64 = 2048;
const SCAN_QUANTUM: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCursor {
    version: u32,
    workspace_id: String,
    thread_id: String,
    limit: u32,
    boundary: SavedHistoryKey,
    snapshot: SavedHistorySnapshot,
    revision: String,
    direction: ThreadHistoryDirection,
    position: Option<SavedHistoryKey>,
    inclusive: bool,
    fragment: Option<FragmentCursor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FragmentCursor {
    key: SavedHistoryKey,
    digest: String,
    offset: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SavedHistoryPage {
    pub events: Vec<Value>,
    /// Internal state only. The tool handler MUST seal this before disclosure.
    pub next_cursor: Option<Value>,
    pub eof: bool,
    pub older_cursor: Option<Value>,
    pub newer_cursor: Option<Value>,
}

impl MessageProcessor {
    #[cfg(test)]
    pub(super) async fn read_saved_thread_history<F, Fut>(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u32,
        cursor: Option<Value>,
        authorize_child: F,
    ) -> Result<SavedHistoryPage>
    where
        F: FnMut(String) -> Fut,
        Fut: Future<Output = Result<bool>>,
    {
        self.read_saved_thread_history_near(
            workspace_id,
            thread_id,
            limit,
            cursor,
            None,
            ThreadHistoryDirection::Older,
            authorize_child,
        )
        .await
    }

    pub(super) async fn read_saved_thread_history_near<F, Fut>(
        &self,
        workspace_id: &str,
        thread_id: &str,
        limit: u32,
        cursor: Option<Value>,
        anchor: Option<ThreadHistoryAnchor>,
        direction: ThreadHistoryDirection,
        mut authorize_child: F,
    ) -> Result<SavedHistoryPage>
    where
        F: FnMut(String) -> Fut,
        Fut: Future<Output = Result<bool>>,
    {
        ensure!(
            (1..=100).contains(&limit),
            "limit must be between 1 and 100"
        );
        ensure!(
            cursor.is_none() || anchor.is_none(),
            "invalid history parameters"
        );
        ensure!(
            cursor.is_some() || anchor.is_some() || direction == ThreadHistoryDirection::Older,
            "invalid history parameters"
        );
        let mut state: HistoryCursor = if let Some(cursor) = cursor {
            let state: HistoryCursor = serde_json::from_value(cursor)
                .map_err(|_| anyhow::anyhow!("invalid history cursor"))?;
            ensure!(
                state.version == 2
                    && state.workspace_id == workspace_id
                    && state.thread_id == thread_id
                    && state.limit == limit
                    && state.direction == direction,
                "history cursor scope or parameters do not match"
            );
            ensure!(
                self.crud_store
                    .saved_history_revision_at_snapshot(thread_id, &state.boundary, &state.snapshot)
                    .await?
                    == state.revision,
                "history cursor is stale; restart reading"
            );
            state
        } else {
            let snapshot = self.crud_store.saved_history_snapshot(thread_id).await?;
            let newest = self
                .crud_store
                .saved_history_page_at_snapshot(thread_id, None, None, 1, &snapshot)
                .await?;
            let Some(newest) = newest.first() else {
                ensure!(anchor.is_none(), "history source unavailable");
                return Ok(SavedHistoryPage {
                    events: Vec::new(),
                    next_cursor: None,
                    eof: true,
                    older_cursor: None,
                    newer_cursor: None,
                });
            };
            // Capture the logical revision before resolving/reading the anchor.
            let revision = self
                .crud_store
                .saved_history_revision_at_snapshot(thread_id, &newest.key, &snapshot)
                .await?;
            let mut position = None;
            if let Some(anchor) = anchor {
                let row = self
                    .crud_store
                    .saved_history_anchor(
                        thread_id,
                        &anchor.turn_id,
                        anchor.item_id.as_deref(),
                        &snapshot,
                    )
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("history source unavailable"))?;
                ensure!(
                    self.saved_history_record(workspace_id, thread_id, &row, &mut authorize_child)
                        .await?
                        .is_some(),
                    "history source unavailable"
                );
                // A bounded key-only query chooses the near edge. No walk from
                // newest history and no payload fetch for the neighboring rows.
                let neighbors = self
                    .crud_store
                    .saved_history_page_direction_at_snapshot(
                        thread_id,
                        Some(&newest.key),
                        Some(&row.key),
                        false,
                        direction == ThreadHistoryDirection::Older,
                        (limit / 2).max(1) as u64,
                        &snapshot,
                    )
                    .await?;
                position = Some(if limit > 1 {
                    neighbors.last().map_or(row.key.clone(), |n| n.key.clone())
                } else {
                    row.key
                });
            }
            HistoryCursor {
                version: 2,
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                limit,
                boundary: newest.key.clone(),
                snapshot,
                revision,
                direction,
                position,
                inclusive: true,
                fragment: None,
            }
        };
        let rows = self
            .crud_store
            .saved_history_page_direction_at_snapshot(
                thread_id,
                Some(&state.boundary),
                state.position.as_ref(),
                state.inclusive,
                direction == ThreadHistoryDirection::Newer,
                SCAN_QUANTUM as u64,
                &state.snapshot,
            )
            .await?;
        let mut reverse_edge = None;
        let mut cursor_chars: usize = 0;
        let mut events = Vec::new();
        let mut exhausted = rows.len() < SCAN_QUANTUM;
        for (index, row) in rows.iter().enumerate() {
            if events.len() == limit as usize {
                exhausted = false;
                break;
            }
            if let Some(fragment) = &state.fragment {
                ensure!(
                    fragment.key == row.key,
                    "history cursor is stale; restart reading"
                );
            }
            reverse_edge.get_or_insert_with(|| row.key.clone());
            let record = self
                .saved_history_record(workspace_id, thread_id, row, &mut authorize_child)
                .await?;
            let Some(record) = record else {
                ensure!(
                    state.fragment.is_none(),
                    "history cursor is stale; restart reading"
                );
                state.position = Some(row.key.clone());
                state.inclusive = false;
                // A filtered final candidate still exhausts a short scan. A
                // preceding visible row must not leave EOF stuck at false.
                exhausted = index + 1 == rows.len() && rows.len() < SCAN_QUANTUM;
                continue;
            };
            let encoded = serde_json::to_string(&record)?;
            let digest = hex::encode(Sha256::digest(encoded.as_bytes()));
            let offset = state.fragment.as_ref().map_or(0, |f| f.offset);
            if let Some(fragment) = &state.fragment {
                ensure!(
                    fragment.digest == digest
                        && offset < encoded.len()
                        && encoded.is_char_boundary(offset),
                    "history cursor is stale; restart reading"
                );
            }
            let source_reference = record.pointer("/content/sourceThreadReference").cloned();
            // Opaque base64 ciphertext can tokenize poorly. Reserve a
            // conservative token per character for both public cursor
            // fields, including the largest fragment continuation state.
            let mut budget_state = state.clone();
            budget_state.position = Some(row.key.clone());
            budget_state.fragment = Some(FragmentCursor {
                key: row.key.clone(),
                digest: digest.clone(),
                offset: encoded.len(),
            });
            cursor_chars = cursor_chars
                .max(2 * ((serde_json::to_vec(&budget_state)?.len() + 29).div_ceil(3) * 4));
            let mut event = record;
            let mut proposed = events.clone();
            proposed.push(event.clone());
            if offset > 0 || !history_events_fit(&proposed, cursor_chars)? {
                // Resume an oversize canonical record without dropping text or
                // arguments. Every fragment's JSON envelope participates in the
                // byte/token limits; UTF-8 boundaries are preserved.
                if !events.is_empty() {
                    exhausted = false;
                    break;
                }
                let mut end = (offset + 4096).min(encoded.len());
                loop {
                    while !encoded.is_char_boundary(end) {
                        end -= 1;
                    }
                    ensure!(end > offset, "history record metadata exceeds page budget");
                    event = json!({"eventId":format!("{}:{}",row.key.kind,row.key.id), "kind":row.key.kind, "turnId":row.turn_id,
                        "createdAt":row.key.created_at, "fragment":{"encoding":"json","version":digest,
                        "offset":offset,"nextOffset":end,"eof":end == encoded.len(),"text":&encoded[offset..end]}});
                    if let Some(reference) = &source_reference {
                        event["sourceThreadReference"] = reference.clone();
                    }
                    if row.key.kind == "child" {
                        event["childReference"] =
                            json!({"threadId":row.key.id,"parentThreadId":thread_id});
                    }
                    if history_events_fit(std::slice::from_ref(&event), cursor_chars)? {
                        break;
                    }
                    end = offset + (end - offset) / 2;
                }
                if end < encoded.len() {
                    state.position = Some(row.key.clone());
                    state.inclusive = true;
                    state.fragment = Some(FragmentCursor {
                        key: row.key.clone(),
                        digest,
                        offset: end,
                    });
                    events.push(event);
                    exhausted = false;
                    break;
                }
            }
            events.push(event);
            state.fragment = None;
            state.position = Some(row.key.clone());
            state.inclusive = false;
            if index + 1 < rows.len() {
                exhausted = false;
            } else {
                exhausted = rows.len() < SCAN_QUANTUM;
            }
        }
        // Recheck after payload loading. Concurrent edits must never produce a
        // page mixed with the revision encoded by the continuation.
        ensure!(
            self.crud_store
                .saved_history_revision_at_snapshot(thread_id, &state.boundary, &state.snapshot)
                .await?
                == state.revision,
            "history cursor is stale; restart reading"
        );
        let opposite = reverse_edge
            .map(|key| {
                let mut reverse = state.clone();
                reverse.direction = if direction == ThreadHistoryDirection::Older {
                    ThreadHistoryDirection::Newer
                } else {
                    ThreadHistoryDirection::Older
                };
                reverse.position = Some(key);
                reverse.inclusive = false;
                reverse.fragment = None;
                serde_json::to_value(reverse)
            })
            .transpose()?;
        let next = if exhausted {
            None
        } else {
            Some(serde_json::to_value(state)?)
        };
        let (older_cursor, newer_cursor) = if direction == ThreadHistoryDirection::Older {
            (next.clone(), opposite)
        } else {
            // Pages keep the same descending presentation in both directions.
            events.reverse();
            (opposite, next.clone())
        };
        Ok(SavedHistoryPage {
            events,
            next_cursor: next,
            eof: exhausted,
            older_cursor,
            newer_cursor,
        })
    }

    async fn saved_history_record<F, Fut>(
        &self,
        workspace_id: &str,
        thread_id: &str,
        row: &SavedHistoryRow,
        authorize_child: &mut F,
    ) -> Result<Option<Value>>
    where
        F: FnMut(String) -> Fut,
        Fut: Future<Output = Result<bool>>,
    {
        if row.key.kind == "child" {
            if !authorize_child(row.key.id.clone()).await? {
                return Ok(None);
            }
            let Some(lineage) = self.crud_store.get_task_thread_lineage(&row.key.id).await? else {
                bail!("history source changed; restart reading");
            };
            ensure!(
                lineage.parent_thread_id == thread_id,
                "history source changed; restart reading"
            );
            let Some(child) = self.crud_store.get_thread_by_id(&row.key.id).await? else {
                return Ok(None);
            };
            if child.workspace_id != workspace_id {
                return Ok(None);
            }
            return Ok(Some(
                json!({"eventId":format!("child:{}", child.id), "kind":"childReference",
                "createdAt":row.key.created_at, "sourceTurnId": if lineage.created_by_thread_id.as_deref() == Some(thread_id) { lineage.created_by_turn_id } else { None },
                "childReference":{"threadId":child.id,"parentThreadId":thread_id,"title":child.name,"status":child.status,"originKind":lineage.origin_kind}}),
            ));
        }
        let turns = self
            .crud_store
            .get_turns_by_thread_and_ids(thread_id, &[row.turn_id.clone()])
            .await?;
        let Some(turn) = turns.get(&row.turn_id) else {
            bail!("history source unavailable");
        };
        let content = match row.key.kind.as_str() {
            "turn" => json!({"status":turn.status,"author":turn.author,"origin":turn.origin,
                "messageRevision":turn.message_revision,"messageDeleted":turn.message_deleted,
                "error":turn.error}),
            "input" if turn.message_deleted => return Ok(None),
            "input" => {
                let Some(input) = self
                    .crud_store
                    .saved_history_input(&row.turn_id, &row.key.id)
                    .await?
                else {
                    bail!("history source changed; restart reading");
                };
                json!({"input":input,"author":turn.author})
            }
            "item" if turn.message_deleted => return Ok(None),
            "item" => {
                let item_id = row
                    .item_id
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("history item reference unavailable"))?;
                let Some(item) = self.crud_store.get_turn_item(&row.turn_id, item_id).await? else {
                    bail!("history source changed; restart reading");
                };
                let classified = pioneer_crud::classify_turn_item_with_db_status(&item, None);
                if classified.visibility != ProjectionVisibility::Visible {
                    return Ok(None);
                }
                // Task records contain runtime receipts/child metadata. Their
                // real lineage references are projected separately after ACL.
                if let TurnItem::Task { .. } = &item {
                    json!({"itemId":item_id,"type":"task","status":classified.status})
                } else if item.is_tool_item() {
                    let value = serde_json::to_value(&item)?;
                    let name = value["toolName"].as_str().unwrap_or_default();
                    let observer = super::thread_read_tools::THREAD_READ_TOOL_NAMES.contains(&name)
                        || name == super::thread_search_tools::SEARCH_TOOL
                        || name == super::thread_tool_results::RESULT_READ_TOOL;
                    let target = value["arguments"]["threadId"]
                        .as_str()
                        .or_else(|| value["arguments"]["thread_id"].as_str());
                    let allowed = if observer {
                        match target {
                            Some(target) => authorize_child(target.to_owned()).await?,
                            None => true,
                        }
                    } else {
                        true
                    };
                    let mut projected = if allowed {
                        project_tool_record(workspace_id, thread_id, &row.turn_id, &item)?
                    } else {
                        json!({"id":item_id,"toolName":name,"status":classified.status,"sourceUnavailable":true})
                    };
                    if observer && allowed {
                        if let Some(target) = target {
                            projected["sourceThreadReference"] = json!({"threadId":target});
                        }
                    }
                    projected["author"] = serde_json::to_value(
                        self.timeline_agent_author_for_turn(thread_id, &row.turn_id)
                            .await?,
                    )?;
                    projected
                } else {
                    let author = if matches!(&item, TurnItem::UserMessage { .. }) {
                        turn.author.clone()
                    } else {
                        self.timeline_agent_author_for_turn(thread_id, &row.turn_id)
                            .await?
                    };
                    json!({"item":item,"author":author})
                }
            }
            _ => bail!("invalid history source kind"),
        };
        Ok(Some(
            json!({"eventId":format!("{}:{}", row.key.kind,row.key.id),"kind":row.key.kind,
            "turnId":row.turn_id,"itemId":row.item_id,"createdAt":row.key.created_at,"content":content}),
        ))
    }
}

fn history_events_fit(events: &[Value], cursor_chars: usize) -> Result<bool> {
    let encoded = serde_json::to_string(events)?;
    Ok(encoded.len() + 128 <= PAGE_BYTES
        && pioneer_compaction::text_tokens(&encoded) <= PAGE_TOKENS
        && encoded.len() + cursor_chars + 256 <= 32768
        && pioneer_compaction::text_tokens(&encoded) + cursor_chars as u64 + 256 <= 4096)
}

fn project_tool_record(
    workspace: &str,
    thread: &str,
    turn: &str,
    item: &TurnItem,
) -> Result<Value> {
    let value = serde_json::to_value(item)?;
    let mut projected = serde_json::Map::new();
    // Preserve arguments in full (including fragments when large), while full
    // results are read using the existing policy-aware result reader.
    for key in [
        "type",
        "id",
        "toolName",
        "arguments",
        "status",
        "success",
        "outcome",
        "command",
        "cwd",
    ] {
        if let Some(value) = value.get(key) {
            projected.insert(key.to_owned(), value.clone());
        }
    }
    // Typed summaries/metadata are already disclosure-safe project contracts;
    // retain them even when no full provider result was ever saved.
    let read_tool = value["toolName"].as_str().is_some_and(|name| {
        super::thread_read_tools::THREAD_READ_TOOL_NAMES.contains(&name)
            || name == super::thread_search_tools::SEARCH_TOOL
            || name == super::thread_tool_results::RESULT_READ_TOOL
    });
    for key in ["display", "storage"].into_iter().filter(|_| !read_tool) {
        if let Some(output) = value.get(key) {
            if matches!(
                output.get("kind").and_then(Value::as_str),
                Some("summary" | "metadata" | "progress")
            ) {
                projected.insert(key.to_owned(), output.clone());
            }
        }
    }
    let reference = json!({"workspace_id":workspace,"thread_id":thread,"turn_id":turn,"item_id":item.item_id()});
    projected.insert("toolResultReference".into(), reference.clone());
    let mut log = reference;
    log["output_log"] = Value::Bool(true);
    projected.insert("outputLogReference".into(), log);
    Ok(Value::Object(projected))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        ToolCallStatus, ToolDisplayPayload, ToolOutputPolicySnapshot, ToolStoragePayload,
        TurnItemExecutionClass,
    };

    #[test]
    fn history_preserves_tool_arguments_and_routes_full_results_to_existing_reader() {
        let arguments = json!({"text":"😀\\\"\n".repeat(5000),"exact":123456789012345_u64});
        let item = TurnItem::DynamicToolCall {
            id: "item".into(),
            tool_name: "example".into(),
            arguments: arguments.clone(),
            status: ToolCallStatus::Completed,
            execution_class: TurnItemExecutionClass::Standard,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("example"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Shell {
                stdout: Some("saved-output".into()),
                stderr: None,
                aggregated_output: None,
                exit_code: Some(0),
                duration_ms: None,
                timed_out: None,
                truncated: false,
            },
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let projected = project_tool_record("workspace", "thread", "turn", &item).unwrap();
        assert_eq!(projected["arguments"], arguments);
        assert_eq!(
            projected["toolResultReference"],
            json!({"workspace_id":"workspace","thread_id":"thread","turn_id":"turn","item_id":"item"})
        );
        assert_eq!(projected["outputLogReference"]["output_log"], true);
        assert!(projected.get("storage").is_none());
        assert!(projected.get("recovery").is_none());
        assert!(
            !serde_json::to_string(&projected)
                .unwrap()
                .contains("saved-output")
        );
    }
}
