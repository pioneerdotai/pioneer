use pioneer_entity::{task, task_execution_admission, thread, thread_lineage, turn, workspace};
use sea_orm::{Condition, EntityName, sea_query::Expr};

/// Actor IDs use the same 21 ASCII alphanumeric bytes as protocol PrincipalId.
pub fn is_canonical_actor_id(id: Expr) -> Expr {
    // SeaQuery's PostgreSQL expressions do not expose the regex operator.
    Expr::cust_with_expr(r#"$1 COLLATE "C" ~ '^[A-Za-z0-9]{21}$'"#, id)
}

// Canonical Task scope shared by exact TaskRead resolution and inbox set queries.
// Every reference uses the same typed aliases so a child's owner/provenance
// cannot replace its root Task's ACL.
#[derive(Clone, Copy, sea_orm::DeriveIden)]
pub enum TaskScopeAlias {
    #[sea_orm(iden = "t")]
    Task,
    #[sea_orm(iden = "scope_root")]
    Root,
    #[sea_orm(iden = "scope_workspace")]
    Workspace,
    #[sea_orm(iden = "scope_turn")]
    Turn,
    #[sea_orm(iden = "scope_origin")]
    Origin,
    #[sea_orm(iden = "scope_lineage")]
    Lineage,
    #[sea_orm(iden = "scope_thread")]
    Thread,
    #[sea_orm(iden = "scope_admission")]
    Admission,
    #[sea_orm(iden = "turn_thread")]
    TurnThread,
}

fn root_owner_thread() -> sea_orm::sea_query::Expr {
    use sea_orm::sea_query::{Expr, ExprTrait};
    Expr::case(
        Expr::col((TaskScopeAlias::Root, task::Column::OwnerKind)).eq("thread"),
        Expr::col((TaskScopeAlias::Root, task::Column::OwnerId)),
    )
    .finally(Expr::val(None::<String>))
    .into()
}

fn explicit_origin_thread() -> sea_orm::sea_query::Expr {
    use sea_orm::sea_query::{Expr, Func};
    Func::coalesce([
        Expr::col((TaskScopeAlias::Root, task::Column::CreatedByThreadId)),
        root_owner_thread(),
    ])
    .into()
}

fn origin_thread() -> sea_orm::sea_query::Expr {
    use sea_orm::sea_query::{Expr, Func};
    Func::coalesce([
        Expr::col((TaskScopeAlias::Root, task::Column::CreatedByThreadId)),
        root_owner_thread(),
        Expr::col((TaskScopeAlias::Turn, turn::Column::ThreadId)),
    ])
    .into()
}

fn valid_actor_scope(alias: TaskScopeAlias) -> Condition {
    use sea_orm::sea_query::{Expr, ExprTrait};
    let kind = Expr::col((alias, thread::Column::CreatedByActorKind));
    let id = Expr::col((alias, thread::Column::CreatedByActorId));
    Condition::any()
        .add(
            Condition::all()
                .add(kind.clone().is_null())
                .add(id.clone().is_null()),
        )
        .add(
            Condition::all()
                .add(kind.clone().eq("system"))
                .add(id.clone().is_null()),
        )
        .add(
            Condition::all()
                .add(kind.is_in(["principal", "agent_execution"]))
                .add(is_canonical_actor_id(id)),
        )
}

pub fn task_scope_query(
    mut query: sea_orm::sea_query::SelectStatement,
) -> sea_orm::sea_query::SelectStatement {
    use TaskScopeAlias as S;
    use sea_orm::sea_query::{Expr, ExprTrait, Func, JoinType, Query};
    // SeaQuery intentionally ignores ON for CROSS JOIN, so its typed equality
    // predicates belong in WHERE.
    query
        .cross_join(task::Entity.table_ref().alias(S::Root))
        .and_where(Expr::col((S::Root, task::Column::Id)).eq(Func::coalesce([
            Expr::col((S::Task, task::Column::RootTaskId)),
            Expr::col((S::Task, task::Column::Id)),
        ])))
        .and_where(
            Expr::col((S::Root, task::Column::WorkspaceId))
                .equals((S::Task, task::Column::WorkspaceId)),
        )
        .and_where(Expr::col((S::Root, task::Column::RootTaskId)).is_null())
        .cross_join(workspace::Entity.table_ref().alias(S::Workspace))
        .and_where(
            Expr::col((S::Workspace, workspace::Column::Id))
                .equals((S::Task, task::Column::WorkspaceId)),
        )
        .join_as(
            JoinType::LeftJoin,
            turn::Entity,
            S::Turn,
            Expr::col((S::Turn, turn::Column::Id)).equals((S::Root, task::Column::CreatedByTurnId)),
        )
        .join_as(
            JoinType::LeftJoin,
            thread::Entity,
            S::Origin,
            Condition::all()
                .add(Expr::col((S::Origin, thread::Column::Id)).eq(origin_thread()))
                .add(
                    Expr::col((S::Origin, thread::Column::WorkspaceId))
                        .equals((S::Task, task::Column::WorkspaceId)),
                ),
        )
        .join_as(
            JoinType::LeftJoin,
            thread_lineage::Entity,
            S::Lineage,
            Expr::col((S::Lineage, thread_lineage::Column::ChildThreadId))
                .equals((S::Origin, thread::Column::Id)),
        )
        .join_as(
            JoinType::LeftJoin,
            thread::Entity,
            S::Thread,
            Condition::all()
                .add(
                    Expr::col((S::Thread, thread::Column::Id)).eq(Expr::case(
                        Expr::col((S::Origin, thread::Column::AccessClass)).eq("internal"),
                        Expr::col((S::Lineage, thread_lineage::Column::RootThreadId)),
                    )
                    .finally(Expr::col((S::Origin, thread::Column::Id)))),
                )
                .add(
                    Expr::col((S::Thread, thread::Column::WorkspaceId))
                        .equals((S::Task, task::Column::WorkspaceId)),
                ),
        )
        .join_as(
            JoinType::LeftJoin,
            task_execution_admission::Entity,
            S::Admission,
            Expr::col((S::Admission, task_execution_admission::Column::TaskId))
                .equals((S::Task, task::Column::Id)),
        );
    let creator_turn_thread = Query::select()
        .expr(Expr::val(1))
        .from_as(thread::Entity, S::TurnThread)
        .and_where(
            Expr::col((S::TurnThread, thread::Column::Id))
                .equals((S::Turn, turn::Column::ThreadId)),
        )
        .and_where(
            Expr::col((S::TurnThread, thread::Column::WorkspaceId))
                .equals((S::Task, task::Column::WorkspaceId)),
        )
        .to_owned();
    query
        .cond_where(
            Condition::any()
                .add(Expr::col((S::Root, task::Column::CreatedByTurnId)).is_null())
                .add(
                    Condition::all()
                        .add(Expr::col((S::Turn, turn::Column::Id)).is_not_null())
                        .add(Expr::exists(creator_turn_thread))
                        .add(
                            Condition::any()
                                .add(explicit_origin_thread().is_null())
                                .add(
                                    Expr::col((S::Turn, turn::Column::ThreadId))
                                        .eq(explicit_origin_thread()),
                                ),
                        ),
                ),
        )
        .cond_where(
            Condition::any().add(origin_thread().is_null()).add(
                Condition::all()
                    .add(Expr::col((S::Origin, thread::Column::Id)).is_not_null())
                    .add(Expr::col((S::Origin, thread::Column::AccessClass)).is_in([
                        "private",
                        "workspace",
                        "internal",
                    ]))
                    .add(Expr::col((S::Thread, thread::Column::Id)).is_not_null())
                    .add(
                        Expr::col((S::Thread, thread::Column::AccessClass))
                            .is_in(["private", "workspace"]),
                    )
                    .add(valid_actor_scope(S::Origin))
                    .add(valid_actor_scope(S::Thread)),
            ),
        )
        .cond_where(
            Condition::any()
                .add(Expr::col((S::Admission, task_execution_admission::Column::TaskId)).is_null())
                .add(
                    Expr::col((S::Admission, task_execution_admission::Column::RootThreadId))
                        .equals((S::Thread, thread::Column::Id)),
                ),
        )
        .cond_where(
            Condition::any()
                .add(Expr::col((S::Root, task::Column::OwnerKind)).ne("user"))
                .add(is_canonical_actor_id(Expr::col((
                    S::Root,
                    task::Column::OwnerId,
                )))),
        );
    query
}
