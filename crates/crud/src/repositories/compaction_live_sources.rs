//! Read-only SQL view projections. This view is not a generated SeaORM entity.
use sea_orm::sea_query::{ConditionType, IntoIden, IntoTableRef};
use sea_orm::{DeriveIden, FromQueryResult, RelationDef, RelationType};

#[derive(Clone, Copy, Debug, DeriveIden)]
pub(super) enum Column {
    #[sea_orm(iden = "compaction_live_sources")]
    Table,
    SourceScope,
    SourceId,
    SourceVersion,
    ThreadId,
    WorkspaceId,
}

#[derive(FromQueryResult)]
pub(super) struct SourceRow {
    pub source_scope: String,
    pub source_id: String,
    pub source_version: String,
}

#[derive(FromQueryResult)]
pub(super) struct ThreadRow {
    pub thread_id: String,
}

/// Join an entity query to the view without declaring an entity or foreign key.
pub(super) fn join(
    table: impl IntoTableRef,
    column: impl IntoIden,
    view_column: Column,
) -> RelationDef {
    RelationDef {
        rel_type: RelationType::HasOne,
        from_tbl: table.into_table_ref(),
        to_tbl: Column::Table.into_table_ref(),
        from_col: sea_orm::Identity::Unary(column.into_iden()),
        to_col: sea_orm::Identity::Unary(view_column.into_iden()),
        is_owner: false,
        skip_fk: true,
        on_delete: None,
        on_update: None,
        on_condition: None,
        fk_name: None,
        condition_type: ConditionType::All,
    }
}
