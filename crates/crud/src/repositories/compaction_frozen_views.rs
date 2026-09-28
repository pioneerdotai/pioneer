//! Query mappings for the logical views created by
//! `m20260914_000003_shared_frozen_ranges`. Entity generation discovers the
//! physical `_data` tables, so these view mappings belong in CRUD.
//!
//! Reads must use the views to include shared ranges under the logical manifest
//! ID. Writes continue to use the generated `_data` entities.

pub(super) mod message {
    use pioneer_entity::compaction_frozen_history as history;
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "compaction_frozen_message")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
        pub manifest_id: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub ordinal: i64,
        #[sea_orm(column_type = "Text")]
        pub reference_json: String,
        pub bytes: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {
        #[sea_orm(
            belongs_to = "history::Entity",
            from = "Column::ManifestId",
            to = "history::Column::Id"
        )]
        History,
    }

    impl Related<history::Entity> for Entity {
        fn to() -> RelationDef {
            Relation::History.def()
        }
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub(super) mod import {
    use pioneer_entity::compaction_frozen_history as history;
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "compaction_frozen_import")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
        pub manifest_id: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub ordinal: i64,
        pub message_ordinal: i64,
        #[sea_orm(column_type = "Text")]
        pub source_scope: String,
        #[sea_orm(column_type = "Text")]
        pub source_id: String,
        #[sea_orm(column_type = "Text")]
        pub source_version: String,
        #[sea_orm(column_type = "Text")]
        pub source_thread: String,
        #[sea_orm(column_type = "Text")]
        pub proof_json: String,
        pub bytes: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {
        #[sea_orm(
            belongs_to = "history::Entity",
            from = "Column::ManifestId",
            to = "history::Column::Id"
        )]
        History,
    }

    impl Related<history::Entity> for Entity {
        fn to() -> RelationDef {
            Relation::History.def()
        }
    }

    impl ActiveModelBehavior for ActiveModel {}
}
