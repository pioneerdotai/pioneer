//! Read-only row type: nullable tool identity is enforced by the schema
//! expression UNIQUE key. Repository writes compare the full exact identity.
use sea_orm::FromQueryResult;

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
pub struct Model {
    pub checkpoint_id: String,
    pub covered_thread: String,
    pub covered_scope: String,
    pub covered_id: String,
    pub covered_version: String,
    pub replay_thread: String,
    pub replay_scope: String,
    pub replay_id: String,
    pub replay_version: String,
    pub tool_item_id: Option<String>,
}
