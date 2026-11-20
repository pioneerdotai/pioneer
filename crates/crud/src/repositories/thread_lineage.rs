use anyhow::Result;
use pioneer_protocol::TaskThreadLineage;
use sea_orm::ConnectionTrait;

pub use pioneer_crud_conversations::thread_lineage::{
    find_lineage_by_child_thread, list_children_for_parent_thread, list_children_page,
    list_lineage_by_root_thread, list_lineage_by_root_thread_page,
};

pub async fn upsert_lineage<C: ConnectionTrait>(db: &C, lineage: &TaskThreadLineage) -> Result<()> {
    if pioneer_crud_conversations::thread_lineage::upsert_lineage(db, lineage).await? {
        super::task_child_producers::lineage(db, &lineage.child_thread_id).await?;
    }
    Ok(())
}
