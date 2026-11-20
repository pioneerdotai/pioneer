use pioneer_entity::{artifact, artifact_binding};
use sea_orm::sea_query::{Expr, ExprTrait, Func, Query};

fn equal_nullable(left: Expr, right: Expr) -> Expr {
    // SeaQuery does not expose PostgreSQL's null-safe equality operator.
    Expr::cust_with_exprs("$1 IS NOT DISTINCT FROM $2", [left, right])
}

/// The original preview is disposable until it has an independent adoption
/// binding. Other first-party HTML publications retain their own lifecycle.
pub fn temporary_preview_predicate(alias: &'static str) -> Expr {
    use artifact_binding::Column as B;
    let metadata = Expr::col((alias, artifact::Column::MetadataJson)).cast_as("json");
    let field = |key: &'static str| {
        Expr::cust_with_exprs(
            "json_extract_path_text($1, 'htmlTool', $2)",
            [metadata.clone(), Expr::val(key)],
        )
    };
    let kind = |key: &'static str| {
        Expr::cust_with_exprs(
            "json_typeof(json_extract_path($1, 'htmlTool', $2))",
            [metadata.clone(), Expr::val(key)],
        )
    };
    let col = |column: B| Expr::col(("adopted", column));
    let adopted = Query::select()
        .expr(Expr::val(1))
        .from_as(artifact_binding::Entity, "adopted")
        .and_where(col(B::ArtifactId).eq(Expr::col((alias, artifact::Column::Id))))
        .and_where(col(B::WorkspaceId).eq(Expr::col((alias, artifact::Column::WorkspaceId))))
        .and_where(
            col(B::BindingKind)
                .eq("tool_output")
                .and(col(B::Direction).eq("output"))
                .and(equal_nullable(col(B::Role), Expr::val("tool")))
                .and(equal_nullable(col(B::TurnId), field("turnId")))
                .and(equal_nullable(col(B::ToolCallId), field("callId")))
                .and(equal_nullable(
                    col(B::ThreadId),
                    Expr::col((alias, artifact::Column::PrimaryThreadId)),
                ))
                .and(col(B::TaskId).is_null())
                .and(col(B::TaskRunId).is_null())
                .and(col(B::MessageId).is_null())
                .not(),
        )
        .to_owned();
    let preview = Expr::col((alias, artifact::Column::CreatedByKind))
        .eq("tool")
        .and(Expr::col((alias, artifact::Column::CreatedByActorId)).eq("html_preview"));
    let shape = kind("version")
        .eq("number")
        .and(field("version").is_in(["1", "2"]))
        .and(kind("publication").eq("boolean"))
        .and(field("publication").eq("false"))
        .and(kind("turnId").eq("string"))
        .and(kind("callId").eq("string"))
        .and(Expr::exists(adopted).not());
    Expr::case(preview, Func::coalesce([shape, Expr::val(false)]))
        .finally(false)
        .into()
}
