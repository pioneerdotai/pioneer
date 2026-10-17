//! SeaQuery ownership predicates. All joins are explicit: Gateway writers use
//! foreign_keys=OFF, so a missing related row must never imply safe retirement.
use pioneer_entity::{
    agent_execution, agent_execution_resource_state, agent_turn_response_execution, artifact,
    artifact_binding, artifact_blob, artifact_projection, artifact_version,
    compaction_frozen_history, compaction_source_revision, compaction_task_output,
    html_preview_frozen_index, html_preview_frozen_result, html_preview_frozen_turn,
    html_preview_index_backfill, native_terminal_effect_outbox, recovery_job,
    recovery_terminalization_outbox, task, task_delivery, task_run, task_run_conversation_snapshot,
    task_run_turn, thread, thread_cli_runtime_binding, turn, turn_cli_runtime_binding,
    turn_event_delivery, turn_finalization, turn_llm_context, turn_runtime_snapshot,
};
use sea_orm::sea_query::{
    Alias, BinOper, Expr, ExprTrait, Func, IntoIden, IntoTableRef, JoinType, Query, SelectStatement,
};

pub(in crate::repositories) fn col<C: IntoIden>(alias: &'static str, column: C) -> Expr {
    Expr::col((alias, column))
}
fn one(table: impl IntoTableRef, alias: &'static str) -> SelectStatement {
    Query::select()
        .expr(Expr::val(1))
        .from_as(table, alias)
        .to_owned()
}
fn exists(query: SelectStatement) -> Expr {
    Expr::exists(query)
}
fn absent(query: SelectStatement) -> Expr {
    exists(query).not()
}
fn call(name: &'static str, args: impl IntoIterator<Item = Expr>) -> Expr {
    Func::cust(Alias::new(name)).args(args).into()
}
pub(super) fn safe_json(value: Expr) -> Expr {
    Expr::case(call("json_valid", [value.clone()]), value)
        .finally("{}")
        .into()
}
pub(super) fn json(value: Expr, path: &'static str) -> Expr {
    call("json_extract", [safe_json(value), Expr::val(path)])
}
pub(in crate::repositories) fn bytes(value: Expr) -> Expr {
    Func::char_length(value.cast_as("BLOB")).into()
}
pub(in crate::repositories) fn revision(value: Expr) -> Expr {
    Expr::val("revision:").binary(BinOper::Custom("||"), value)
}
fn is(left: Expr, right: Expr) -> Expr {
    left.binary(BinOper::Is, right)
}

pub(super) fn temporary_preview(a: &'static str) -> Expr {
    let metadata = col(a, artifact::Column::MetadataJson);
    let origin = |path| json(metadata.clone(), path);
    let binding = one(artifact_binding::Entity, "adopted")
        .and_where(
            col("adopted", artifact_binding::Column::ArtifactId).eq(col(a, artifact::Column::Id)),
        )
        .and_where(
            col("adopted", artifact_binding::Column::WorkspaceId)
                .eq(col(a, artifact::Column::WorkspaceId)),
        )
        .and_where(
            col("adopted", artifact_binding::Column::BindingKind)
                .eq("tool_output")
                .and(col("adopted", artifact_binding::Column::Direction).eq("output"))
                .and(is(
                    col("adopted", artifact_binding::Column::Role),
                    Expr::val("tool"),
                ))
                .and(is(
                    col("adopted", artifact_binding::Column::TurnId),
                    origin("$.htmlTool.turnId"),
                ))
                .and(is(
                    col("adopted", artifact_binding::Column::ToolCallId),
                    origin("$.htmlTool.callId"),
                ))
                .and(is(
                    col("adopted", artifact_binding::Column::ThreadId),
                    col(a, artifact::Column::PrimaryThreadId),
                ))
                .and(col("adopted", artifact_binding::Column::TaskId).is_null())
                .and(col("adopted", artifact_binding::Column::TaskRunId).is_null())
                .and(col("adopted", artifact_binding::Column::MessageId).is_null())
                .not(),
        )
        .to_owned();
    let kind = |path| call("json_type", [safe_json(metadata.clone()), Expr::val(path)]);
    let predicate = col(a, artifact::Column::CreatedByKind)
        .eq("tool")
        .and(col(a, artifact::Column::CreatedByActorId).eq("html_preview"))
        .and(kind("$.htmlTool.version").eq("integer"))
        .and(kind("$.htmlTool.publication").eq("false"))
        .and(origin("$.htmlTool.version").eq(1))
        .and(origin("$.htmlTool.publication").eq(0))
        .and(kind("$.htmlTool.turnId").eq("text"))
        .and(kind("$.htmlTool.callId").eq("text"))
        .and(absent(binding));
    Func::coalesce([predicate, Expr::val(0)]).into()
}

pub(in crate::repositories) fn released_marker(workspace: &str) -> SelectStatement {
    released_marker_for_turn(workspace, col("c", turn_llm_context::Column::TurnId))
}
fn released_marker_for_turn(workspace: &str, turn: Expr) -> SelectStatement {
    one(artifact::Entity, "a")
        .and_where(col("a", artifact::Column::WorkspaceId).eq(workspace))
        .and_where(col("a", artifact::Column::CreatedByKind).eq("tool"))
        .and_where(col("a", artifact::Column::CreatedByActorId).eq("html_preview"))
        .and_where(
            json(
                col("a", artifact::Column::MetadataJson),
                "$.htmlTool.retention",
            )
            .eq("released"),
        )
        .and_where(
            json(
                col("a", artifact::Column::MetadataJson),
                "$.htmlTool.turnId",
            )
            .eq(turn),
        )
        .and_where(
            json(
                col("a", artifact::Column::MetadataJson),
                "$.htmlTool.callId",
            )
            .eq(col("c", turn_llm_context::Column::ItemId)),
        )
        .to_owned()
}

/// Same canonical joins for current witness preparation and publication. No
/// FK/relationship inference: workspace, turn and source presence are checked.
pub(in crate::repositories) fn canonical_preview() -> SelectStatement {
    one(turn_llm_context::Entity, "c")
        .join_as(
            JoinType::InnerJoin,
            compaction_source_revision::Entity,
            "v",
            col("v", compaction_source_revision::Column::SourceId)
                .eq(col("c", turn_llm_context::Column::Id)),
        )
        .join_as(
            JoinType::InnerJoin,
            turn::Entity,
            "t",
            col("t", turn::Column::Id).eq(col("c", turn_llm_context::Column::TurnId)),
        )
        .join_as(
            JoinType::InnerJoin,
            thread::Entity,
            "th",
            col("th", thread::Column::Id).eq(col("t", turn::Column::ThreadId)),
        )
        .and_where(col("c", turn_llm_context::Column::Source).eq("tool_result_v2"))
        .and_where(col("c", turn_llm_context::Column::ToolName).eq("html_preview"))
        .and_where(col("v", compaction_source_revision::Column::Present).eq(1))
        .to_owned()
}

pub(super) fn unowned(
    workspace: &str,
    thread_id: &str,
    turn_id: &str,
    root_execution: &str,
) -> SelectStatement {
    let terminal = ["completed", "failed", "interrupted"];
    let scoped_turn = || {
        one(turn::Entity, "t")
            .join_as(
                JoinType::InnerJoin,
                thread::Entity,
                "th",
                col("th", thread::Column::Id).eq(col("t", turn::Column::ThreadId)),
            )
            .and_where(col("th", thread::Column::WorkspaceId).eq(workspace))
            .to_owned()
    };
    let own_turn = scoped_turn()
        .and_where(col("t", turn::Column::Id).eq(turn_id))
        .and_where(col("t", turn::Column::ThreadId).eq(thread_id))
        .and_where(col("t", turn::Column::Status).is_in(terminal))
        .to_owned();
    let active_turn = scoped_turn()
        .and_where(col("t", turn::Column::Status).is_not_in(terminal))
        .to_owned();
    let runtime = one(turn_runtime_snapshot::Entity, "s")
        .and_where(
            col("s", turn_runtime_snapshot::Column::WorkspaceId)
                .eq(workspace)
                .or(col("s", turn_runtime_snapshot::Column::WorkspaceId).eq(""))
                .or(col("s", turn_runtime_snapshot::Column::TurnId).eq(turn_id)),
        )
        .to_owned();
    let transient = one(turn_llm_context::Entity, "c")
        .and_where(col("c", turn_llm_context::Column::TurnId).eq(turn_id))
        .and_where(
            col("c", turn_llm_context::Column::Source)
                .is_not_in(["assistant_round", "tool_result_v2"]),
        )
        .to_owned();
    let recovery = one(recovery_job::Entity, "r")
        .and_where(col("r", recovery_job::Column::TurnId).eq(turn_id))
        .and_where(
            col("r", recovery_job::Column::Status)
                .is_not_in(["succeeded", "failed", "exhausted", "cancelled"])
                .or(col("r", recovery_job::Column::ResolutionPending).eq(1)),
        )
        .to_owned();
    let recovery_delivery = one(recovery_terminalization_outbox::Entity, "r")
        .and_where(col("r", recovery_terminalization_outbox::Column::TurnId).eq(turn_id))
        .and_where(
            col("r", recovery_terminalization_outbox::Column::Status)
                .is_not_in(["delivered", "cancelled"]),
        )
        .to_owned();
    let finalization = one(turn_finalization::Entity, "f")
        .and_where(col("f", turn_finalization::Column::TurnId).eq(turn_id))
        .and_where(col("f", turn_finalization::Column::Status).eq("prepared"))
        .to_owned();
    let terminal_effect = one(native_terminal_effect_outbox::Entity, "e")
        .and_where(col("e", native_terminal_effect_outbox::Column::TurnId).eq(turn_id))
        .and_where(
            col("e", native_terminal_effect_outbox::Column::Status).is_not_in([
                "succeeded",
                "discarded",
                "superseded",
            ]),
        )
        .to_owned();
    let event_delivery = one(turn_event_delivery::Entity, "d")
        .and_where(col("d", turn_event_delivery::Column::TurnId).eq(turn_id))
        .and_where(
            col("d", turn_event_delivery::Column::Status).is_not_in(["delivered", "exhausted"]),
        )
        .to_owned();
    let task_owner = one(task_run_turn::Entity, "r")
        .join_as(
            JoinType::LeftJoin,
            task_run::Entity,
            "tr",
            col("tr", task_run::Column::Id).eq(col("r", task_run_turn::Column::RunId)),
        )
        .join_as(
            JoinType::LeftJoin,
            task::Entity,
            "t",
            col("t", task::Column::Id).eq(col("tr", task_run::Column::TaskId)),
        )
        .and_where(col("r", task_run_turn::Column::TurnId).eq(turn_id))
        .and_where(
            col("tr", task_run::Column::Id)
                .is_null()
                .or(col("t", task::Column::Id).is_null())
                .or(col("t", task::Column::Status).ne("completed")),
        )
        .to_owned();
    let task_delivery = one(task_run_turn::Entity, "r")
        .join_as(
            JoinType::InnerJoin,
            task_delivery::Entity,
            "d",
            col("d", task_delivery::Column::TaskId)
                .eq(col("r", task_run_turn::Column::TaskId))
                .and(
                    col("d", task_delivery::Column::RunId)
                        .eq(col("r", task_run_turn::Column::RunId)),
                ),
        )
        .and_where(col("r", task_run_turn::Column::TurnId).eq(turn_id))
        .and_where(col("d", task_delivery::Column::Status).is_not_in([
            "delivered",
            "failed",
            "cancelled",
        ]))
        .to_owned();
    let missing_execution = one(agent_turn_response_execution::Entity, "r")
        .join_as(
            JoinType::LeftJoin,
            agent_execution::Entity,
            "e",
            col("e", agent_execution::Column::Id)
                .eq(col("r", agent_turn_response_execution::Column::ExecutionId)),
        )
        .and_where(col("r", agent_turn_response_execution::Column::TurnId).eq(turn_id))
        .and_where(col("e", agent_execution::Column::Id).is_null())
        .to_owned();
    let response = |execution| {
        one(agent_turn_response_execution::Entity, "r")
            .and_where(col("r", agent_turn_response_execution::Column::TurnId).eq(turn_id))
            .and_where(col("r", agent_turn_response_execution::Column::ExecutionId).eq(execution))
            .to_owned()
    };
    let execution = one(agent_execution::Entity, "e")
        .and_where(
            col("e", agent_execution::Column::Id)
                .eq(root_execution)
                .or(exists(response(col("e", agent_execution::Column::Id)))),
        )
        .and_where(col("e", agent_execution::Column::Status).is_not_in([
            "completed",
            "succeeded",
            "failed",
            "cancelled",
            "timed_out",
        ]))
        .to_owned();
    let resources = one(agent_execution_resource_state::Entity, "s")
        .and_where(
            col("s", agent_execution_resource_state::Column::ExecutionId)
                .eq(root_execution)
                .or(exists(response(col(
                    "s",
                    agent_execution_resource_state::Column::ExecutionId,
                )))),
        )
        .and_where(
            col("s", agent_execution_resource_state::Column::Status).is_not_in([
                "completed",
                "failed",
                "cancelled",
            ]),
        )
        .to_owned();
    let opaque_task = one(task_run_conversation_snapshot::Entity, "s")
        .join_as(
            JoinType::LeftJoin,
            task::Entity,
            "t",
            col("t", task::Column::Id).eq(col("s", task_run_conversation_snapshot::Column::TaskId)),
        )
        .and_where(
            col("s", task_run_conversation_snapshot::Column::WorkspaceId)
                .eq(workspace)
                .or(col("s", task_run_conversation_snapshot::Column::WorkspaceId).eq(""))
                .or(col("t", task::Column::WorkspaceId).eq(workspace)),
        )
        .and_where(
            col(
                "s",
                task_run_conversation_snapshot::Column::FrozenManifestId,
            )
            .eq("")
            .or(col(
                "s",
                task_run_conversation_snapshot::Column::FrozenManifestId,
            )
            .is_null()
            .and(col("s", task_run_conversation_snapshot::Column::HistoryJson).ne("[]"))),
        )
        .and_where(
            col("t", task::Column::Id)
                .is_null()
                .or(col("t", task::Column::Status).ne("completed")),
        )
        .to_owned();
    let opaque_history = |value: Expr, path| {
        Expr::case(
            call("json_valid", [value.clone()]),
            Func::coalesce([
                call("json_extract", [value, Expr::val(path)]),
                Expr::val("[]"),
            ])
            .ne("[]"),
        )
        .finally(1)
    };
    let opaque_thread = one(thread_cli_runtime_binding::Entity, "s")
        .and_where(
            col("s", thread_cli_runtime_binding::Column::WorkspaceId)
                .eq(workspace)
                .or(col("s", thread_cli_runtime_binding::Column::WorkspaceId).eq("")),
        )
        .and_where(
            col("s", thread_cli_runtime_binding::Column::FrozenManifestId)
                .eq("")
                .or(
                    col("s", thread_cli_runtime_binding::Column::FrozenManifestId)
                        .is_null()
                        .and(opaque_history(
                            col("s", thread_cli_runtime_binding::Column::ResumeCursorJson),
                            "$.pioneerContext.contextHistoryJson",
                        )),
                ),
        )
        .to_owned();
    let opaque_turn = one(turn_cli_runtime_binding::Entity, "s")
        .and_where(
            col("s", turn_cli_runtime_binding::Column::WorkspaceId)
                .eq(workspace)
                .or(col("s", turn_cli_runtime_binding::Column::WorkspaceId).eq("")),
        )
        .and_where(
            col("s", turn_cli_runtime_binding::Column::FrozenManifestId)
                .eq("")
                .or(col("s", turn_cli_runtime_binding::Column::FrozenManifestId)
                    .is_null()
                    .and(opaque_history(
                        col("s", turn_cli_runtime_binding::Column::InputMappingJson),
                        "$.pioneerContextBasis.historyJson",
                    ))),
        )
        .to_owned();
    let backfill = one(html_preview_index_backfill::Entity, "b")
        .and_where(col("b", html_preview_index_backfill::Column::Singleton).eq(1))
        .and_where(col("b", html_preview_index_backfill::Column::Complete).eq(1))
        .and_where(col("b", html_preview_index_backfill::Column::InputsComplete).eq(1))
        .to_owned();
    let unknown = one(html_preview_frozen_index::Entity, "i")
        .and_where(col("i", html_preview_frozen_index::Column::WorkspaceId).eq(workspace))
        .and_where(col("i", html_preview_frozen_index::Column::Expired).eq(0))
        .and_where(
            col("i", html_preview_frozen_index::Column::Ready)
                .ne(1)
                .or(col("i", html_preview_frozen_index::Column::Complete).ne(1))
                .or(col("i", html_preview_frozen_index::Column::Unknown).ne(0)),
        )
        .to_owned();
    let frozen = frozen_media_hold(workspace, turn_id);
    Query::select()
        .expr_as(Expr::val(1), "safe")
        .and_where(exists(own_turn))
        .and_where(exists(backfill))
        .and_where(absent(active_turn))
        .and_where(absent(runtime))
        .and_where(absent(transient))
        .and_where(absent(recovery))
        .and_where(absent(recovery_delivery))
        .and_where(absent(finalization))
        .and_where(absent(terminal_effect))
        .and_where(absent(event_delivery))
        .and_where(absent(task_owner))
        .and_where(absent(task_delivery))
        .and_where(absent(missing_execution))
        .and_where(absent(execution))
        .and_where(absent(resources))
        .and_where(absent(opaque_task))
        .and_where(absent(opaque_thread))
        .and_where(absent(opaque_turn))
        .and_where(absent(unknown))
        .and_where(absent(frozen))
        .to_owned()
}

fn frozen_media_hold(workspace: &str, turn_id: &str) -> SelectStatement {
    let completed_output = one(compaction_task_output::Entity, "o")
        .join_as(
            JoinType::InnerJoin,
            task::Entity,
            "t",
            col("t", task::Column::Id).eq(col("o", compaction_task_output::Column::TaskId)),
        )
        .and_where(
            col("o", compaction_task_output::Column::ManifestId)
                .eq(col("h", compaction_frozen_history::Column::Id)),
        )
        .and_where(col("t", task::Column::Status).eq("completed"))
        .to_owned();
    let accepted = one(task_run_conversation_snapshot::Entity, "s")
        .join_as(
            JoinType::LeftJoin,
            task::Entity,
            "t",
            col("t", task::Column::Id).eq(col("s", task_run_conversation_snapshot::Column::TaskId)),
        )
        .and_where(
            col(
                "s",
                task_run_conversation_snapshot::Column::FrozenManifestId,
            )
            .eq(col("h", compaction_frozen_history::Column::Id)),
        )
        .and_where(
            col("t", task::Column::Id)
                .is_null()
                .or(col("t", task::Column::Status).ne("completed")),
        )
        .to_owned();
    let runtime = one(turn_runtime_snapshot::Entity, "s")
        .and_where(
            col("s", turn_runtime_snapshot::Column::FrozenManifestId)
                .eq(col("h", compaction_frozen_history::Column::Id)),
        )
        .to_owned();
    let thread_binding = one(thread_cli_runtime_binding::Entity, "s")
        .and_where(
            col("s", thread_cli_runtime_binding::Column::FrozenManifestId)
                .eq(col("h", compaction_frozen_history::Column::Id)),
        )
        .to_owned();
    let turn_binding = one(turn_cli_runtime_binding::Entity, "s")
        .and_where(
            col("s", turn_cli_runtime_binding::Column::FrozenManifestId)
                .eq(col("h", compaction_frozen_history::Column::Id)),
        )
        .to_owned();
    let result = || {
        one(html_preview_frozen_result::Entity, "r")
            .and_where(
                col("r", html_preview_frozen_result::Column::ManifestId)
                    .eq(col("h", compaction_frozen_history::Column::Id)),
            )
            .and_where(col("r", html_preview_frozen_result::Column::TurnId).eq(turn_id))
            .to_owned()
    };
    let marker = released_marker_for_turn(
        workspace,
        col("r", html_preview_frozen_result::Column::TurnId),
    );
    let invalid_result = result()
        .join_as(
            JoinType::LeftJoin,
            compaction_source_revision::Entity,
            "v",
            col("v", compaction_source_revision::Column::SourceId)
                .eq(col("r", html_preview_frozen_result::Column::SourceId)),
        )
        .join_as(
            JoinType::LeftJoin,
            turn_llm_context::Entity,
            "c",
            col("c", turn_llm_context::Column::Id)
                .eq(col("r", html_preview_frozen_result::Column::SourceId)),
        )
        .and_where(
            col("r", html_preview_frozen_result::Column::ArchiveWireSha256)
                .is_null()
                .or(col("v", compaction_source_revision::Column::Present)
                    .binary(BinOper::IsNot, Expr::val(1)))
                .or(col("r", html_preview_frozen_result::Column::SourceVersion)
                    .eq(revision(col(
                        "v",
                        compaction_source_revision::Column::Revision,
                    )))
                    .or(col("r", html_preview_frozen_result::Column::SourceVersion)
                        .eq(revision(
                            col("v", compaction_source_revision::Column::Revision).sub(1),
                        ))
                        .and(exists(marker)))
                    .not()),
        )
        .to_owned();
    one(html_preview_frozen_turn::Entity, "p")
        .join_as(
            JoinType::InnerJoin,
            compaction_frozen_history::Entity,
            "h",
            col("h", compaction_frozen_history::Column::Id)
                .eq(col("p", html_preview_frozen_turn::Column::ManifestId)),
        )
        .and_where(col("p", html_preview_frozen_turn::Column::TurnId).eq(turn_id))
        .and_where(col("h", compaction_frozen_history::Column::WorkspaceId).eq(workspace))
        .and_where(col("h", compaction_frozen_history::Column::Expired).eq(0))
        .and_where(
            absent(completed_output)
                .or(exists(accepted))
                .or(exists(runtime))
                .or(exists(thread_binding))
                .or(exists(turn_binding))
                .or(absent(result()))
                .or(exists(invalid_result)),
        )
        .to_owned()
}

pub(super) fn blob_has_owners(alias: &'static str) -> Expr {
    exists(
        one(artifact_version::Entity, "version_owner")
            .and_where(
                col("version_owner", artifact_version::Column::BlobId)
                    .eq(col(alias, artifact_blob::Column::Id)),
            )
            .to_owned(),
    )
    .or(exists(
        one(artifact_projection::Entity, "projection_owner")
            .and_where(
                col("projection_owner", artifact_projection::Column::BlobId)
                    .eq(col(alias, artifact_blob::Column::Id)),
            )
            .to_owned(),
    ))
}
pub(super) fn mark_temporary_blob() -> Expr {
    call(
        "json_set",
        [
            safe_json(Expr::col(artifact_blob::Column::MetadataJson)),
            Expr::val("$.htmlToolTemporary"),
            Expr::val(1),
        ],
    )
}

/// Receipt and immutable output must describe the same exact manifest member.
/// Callers add either historical equality or current canonical body guards.
pub(in crate::repositories) fn archive_receipt(
    workspace: &str,
    owner: &str,
    manifest: &str,
    ordinal: i64,
) -> SelectStatement {
    use super::super::compaction_frozen_views::message;
    one(html_preview_frozen_result::Entity, "r")
        .join_as(
            JoinType::InnerJoin,
            compaction_frozen_history::Entity,
            "h",
            col("h", compaction_frozen_history::Column::Id)
                .eq(col("r", html_preview_frozen_result::Column::ManifestId)),
        )
        .join_as(
            JoinType::InnerJoin,
            message::Entity,
            "m",
            col("m", message::Column::ManifestId)
                .eq(col("r", html_preview_frozen_result::Column::ManifestId))
                .and(
                    col("m", message::Column::Ordinal)
                        .eq(col("r", html_preview_frozen_result::Column::Ordinal)),
                ),
        )
        .and_where(col("r", html_preview_frozen_result::Column::ManifestId).eq(manifest))
        .and_where(col("r", html_preview_frozen_result::Column::Ordinal).eq(ordinal))
        .and_where(col("h", compaction_frozen_history::Column::WorkspaceId).eq(workspace))
        .and_where(col("h", compaction_frozen_history::Column::OwnerThread).eq(owner))
        .and_where(col("h", compaction_frozen_history::Column::Ready).eq(1))
        .and_where(col("h", compaction_frozen_history::Column::Expired).eq(0))
        .to_owned()
}
