use super::view::merge_pending_timeline_render_rows;
use super::{TimelineRenderModel, TimelineRenderRow};
use crate::app::{conversation::ConversationViewState, root::PioneerDesktop};
use pioneer_client::{
    cli_runtime::approvals::{CLIRuntimePendingRequestEntry, PendingRequest},
    timeline::{
        semantic::{self, SemanticTimelineRow, SemanticTimelineRowKind},
        semantic_render::render_semantic_timeline_rows,
    },
};
use pioneer_protocol::{CLIRuntimePendingRequestStatus, TimelineBlock, TimelineBlockKind};
use std::rc::Rc;

pub(in crate::app::thread::view::timeline) const SEMANTIC_TURN_WORK_GROUP_PREFIX: &str =
    "semantic-turn-work-group::";

impl PioneerDesktop {
    pub(crate) fn semantic_timeline_render_model(
        &self,
        active_thread_id: Option<&str>,
    ) -> TimelineRenderModel {
        let Some(active_thread_id) = active_thread_id else {
            return TimelineRenderModel::empty();
        };

        {
            let state = self.thread_timeline_view_state.borrow();
            if state.cached_semantic_model_active_thread_id.as_deref() == Some(active_thread_id)
                && state.cached_semantic_model_revision == self.semantic_timeline_revision
                && let Some(model) = state.cached_semantic_model.as_ref()
            {
                return model.clone();
            }
        }

        let Some(flattened) =
            semantic::flatten_semantic_timeline(&self.semantic_timelines, active_thread_id)
        else {
            return TimelineRenderModel::empty();
        };
        let semantic_rows = Rc::new(flattened);

        let mut projection = ConversationViewState::default();
        self.merge_turn_metadata_for_timeline(active_thread_id, &mut projection);
        let render_model = render_semantic_timeline_rows(semantic_rows.rows.as_slice(), projection);

        let mut projection = render_model.projection;
        projection.revision = self.semantic_timeline_revision;
        let rows = render_model
            .rows
            .into_iter()
            .map(TimelineRenderRow::Timeline)
            .collect();
        let rows = merge_pending_timeline_render_rows(
            Rc::new(rows),
            pending_requests_from_semantic_rows(semantic_rows.rows.as_slice()),
        );

        let model = TimelineRenderModel {
            projection: Rc::new(projection),
            rows: Rc::new(rows),
            semantic_row_ids: Rc::new(render_model.semantic_row_ids),
            semantic_rows,
        };

        {
            let mut state = self.thread_timeline_view_state.borrow_mut();
            state.cached_semantic_model_active_thread_id = Some(active_thread_id.to_owned());
            state.cached_semantic_model_revision = self.semantic_timeline_revision;
            state.cached_semantic_model = Some(model.clone());
        }

        model
    }

    fn merge_turn_metadata_for_timeline(
        &self,
        thread_id: &str,
        projection: &mut ConversationViewState,
    ) {
        let Some(coordinator) = self.thread_coordinator(thread_id) else {
            return;
        };

        projection
            .turns
            .extend(coordinator.conversation.projection().turns.iter().cloned());

        if let Some(thread) = coordinator.thread() {
            for turn in &thread.turns {
                projection.upsert_turn_snapshot_metadata(turn);
            }
        }
    }
}

fn pending_requests_from_semantic_rows(rows: &[SemanticTimelineRow]) -> Vec<PendingRequest> {
    rows.iter()
        .filter_map(|row| match &row.kind {
            SemanticTimelineRowKind::PendingRequest { block } => {
                pending_request_from_semantic_block(block)
            }
            _ => None,
        })
        .collect()
}

fn pending_request_from_semantic_block(block: &TimelineBlock) -> Option<PendingRequest> {
    let TimelineBlockKind::PendingRequest {
        runtime_id,
        request_id,
        status,
        item_id,
        request,
    } = &block.kind
    else {
        return None;
    };
    if *status != CLIRuntimePendingRequestStatus::Pending {
        return None;
    }

    Some(
        CLIRuntimePendingRequestEntry {
            workspace_id: block.workspace_id.clone(),
            runtime_id: runtime_id.clone(),
            request_id: request_id.clone(),
            thread_id: Some(block.thread_id.clone()),
            turn_id: block.turn_id.clone(),
            item_id: item_id.clone(),
            request: request.clone(),
        }
        .into_pending_request(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::timeline::{
        labels::RunningTurnDisplay,
        rows::{TimelineRow, TimelineRowKind},
        semantic::SemanticTimelineRowId,
    };
    use pioneer_protocol::{
        CLIRuntimePendingRequest, CLIRuntimeRequestKind, TurnPermissionApprovalRequest,
    };

    #[test]
    fn merge_pending_timeline_render_rows_uses_one_row_type_for_cli_and_native_requests() {
        let rows = Rc::new(vec![
            timeline_row("item-1", TimelineRowKind::Item { timeline_index: 0 }),
            timeline_row(
                "running",
                TimelineRowKind::RunningTurn(RunningTurnDisplay {
                    turn_id: "turn_a".to_owned(),
                    started_at_unix_ms: None,
                    state: Some(pioneer_protocol::TurnWorkState::Running),
                    message: None,
                    permission_profile: None,
                    security_summary: None,
                }),
            ),
        ]);

        let merged = merge_pending_timeline_render_rows(
            rows,
            vec![
                native_pending_request("native_req"),
                cli_pending_request("cli_req"),
            ],
        );

        assert_eq!(merged.len(), 4);
        assert_eq!(merged[0].key(), "item-1");
        assert_eq!(merged[3].key(), "running");
        assert!(matches!(merged[1], TimelineRenderRow::PendingRequest(_)));
        assert!(matches!(merged[2], TimelineRenderRow::PendingRequest(_)));
        assert_eq!(merged[1].key(), "timeline-pending-request::native_req");
        assert_eq!(merged[2].key(), "timeline-pending-request::cli_req");
    }

    #[test]
    fn semantic_pending_requests_preserve_child_thread_scope() {
        let pending_requests = pending_requests_from_semantic_rows(&[SemanticTimelineRow {
            id: SemanticTimelineRowId::TopLevelBlock {
                block_id: "approval_block".to_owned(),
            },
            kind: SemanticTimelineRowKind::PendingRequest {
                block: TimelineBlock {
                    workspace_id: "workspace_a".to_owned(),
                    thread_id: "child_thread".to_owned(),
                    block_id: "approval_block".to_owned(),
                    turn_id: Some("child_turn".to_owned()),
                    sort_key: "0001".to_owned(),
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(2),
                    kind: TimelineBlockKind::PendingRequest {
                        runtime_id: "codex".to_owned(),
                        request_id: "child_req".to_owned(),
                        status: CLIRuntimePendingRequestStatus::Pending,
                        item_id: Some("child_item".to_owned()),
                        request: CLIRuntimePendingRequest {
                            kind: CLIRuntimeRequestKind::CommandApproval,
                            title: Some("Run command".to_owned()),
                            message: None,
                            native_request_id: None,
                            payload: None,
                        },
                    },
                },
            },
        }]);

        assert_eq!(pending_requests.len(), 1);
        assert_eq!(pending_requests[0].request_id, "child_req");
        assert_eq!(
            pending_requests[0].thread_id.as_deref(),
            Some("child_thread")
        );
        assert_eq!(pending_requests[0].turn_id.as_deref(), Some("child_turn"));
        assert_eq!(pending_requests[0].item_id.as_deref(), Some("child_item"));
    }

    fn timeline_row(key: &str, kind: TimelineRowKind) -> TimelineRenderRow {
        TimelineRenderRow::Timeline(TimelineRow {
            key: key.to_owned(),
            kind,
        })
    }

    fn native_pending_request(request_id: &str) -> PendingRequest {
        PendingRequest::from_native_permission_request(TurnPermissionApprovalRequest {
            request_id: request_id.to_owned(),
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            visible_thread_ids: Vec::new(),
            tool_name: "shell".to_owned(),
            action: pioneer_protocol::TurnPermissionActionKind::ShellCommand,
            scope_hash: "scope_a".to_owned(),
            reason: pioneer_protocol::TurnPermissionDecisionReason::PolicyRequiresApproval,
            summary: Some("cargo check".to_owned()),
            details: Vec::new(),
        })
    }

    fn cli_pending_request(request_id: &str) -> PendingRequest {
        CLIRuntimePendingRequestEntry {
            workspace_id: "workspace_a".to_owned(),
            runtime_id: "codex".to_owned(),
            request_id: request_id.to_owned(),
            thread_id: Some("thread_a".to_owned()),
            turn_id: Some("turn_a".to_owned()),
            item_id: None,
            request: CLIRuntimePendingRequest {
                kind: CLIRuntimeRequestKind::CommandApproval,
                title: Some("Run command".to_owned()),
                message: None,
                native_request_id: None,
                payload: None,
            },
        }
        .into_pending_request()
    }
}
