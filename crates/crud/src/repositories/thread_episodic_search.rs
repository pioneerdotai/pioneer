use crate::thread_episodic::{
    ThreadEpisodicSearchQuery, ThreadEpisodicSearchResult, ThreadEpisodicSearchRow,
    thread_episodic_item_record_from_model, thread_episodic_refill_projection_key,
};
use anyhow::Result;
use sea_orm::ConnectionTrait;

pub async fn search<C: ConnectionTrait>(
    db: &C,
    request: &ThreadEpisodicSearchQuery,
) -> Result<ThreadEpisodicSearchResult> {
    let page = pioneer_crud_memory::episodic_search::search(db, request).await?;
    let rows = page
        .rows
        .into_iter()
        .map(|row| {
            let text = row.item.search_text.clone().unwrap_or_default();
            Ok(ThreadEpisodicSearchRow {
                item: thread_episodic_item_record_from_model(row.item)?,
                text,
                lexical_rank: row.lexical_rank,
                semantic_rank: row.semantic_rank,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let projection_key = thread_episodic_refill_projection_key(&request.workspace_id)?;
    let marker = crate::find_projection_meta(db, &projection_key).await?;
    let coverage_incomplete = page.incomplete_sources
        || !marker.is_some_and(|marker| marker.status == crate::PROJECTION_META_STATUS_COMPLETE);
    Ok(ThreadEpisodicSearchResult {
        rows,
        coverage_incomplete,
    })
}
