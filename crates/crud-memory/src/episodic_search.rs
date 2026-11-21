use crate::thread_episodic::{
    ThreadEpisodicSearchResult, ThreadEpisodicSearchRow, thread_episodic_item_record_from_model,
    thread_episodic_refill_projection_key,
};
use anyhow::{Result, ensure};
use pioneer_crud_conversations::thread_timeline_projection::{
    PROJECTION_META_STATUS_COMPLETE, find_projection_meta,
};
use pioneer_entity::thread_episodic_items;
use sea_orm::entity::prelude::PgVector;
use sea_orm::{ConnectionTrait, DbBackend, FromQueryResult, Statement};
use serde::{Deserialize, Serialize};

/// Conditions supplied by the product's authorization boundary. A missing
/// principal restricts no ACL only when the caller has an absolute read grant.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ThreadEpisodicSearchAccess {
    pub principal_id: Option<String>,
    pub root_thread_id: Option<String>,
    pub include_internal: bool,
    pub current_thread_id: Option<String>,
}

/// Equal-weight reciprocal-rank policy, independent of the storage engine.
/// Its top score is 1 when both branches rank a result first. A single first
/// rank scores 0.5; this scale is suitable for the existing 0..1 thresholds.
pub const THREAD_SEARCH_RRF_K: f64 = 10.0;
/// Invalidates continuation positions when ranking/grouping changes.
pub const THREAD_SEARCH_RANKING_VERSION: u32 = 3;
pub fn thread_search_rank_score(lexical_rank: Option<i64>, semantic_rank: Option<i64>) -> f32 {
    let contribution = |rank: Option<i64>| {
        rank.filter(|rank| *rank > 0).map_or(0.0, |rank| {
            (THREAD_SEARCH_RRF_K + 1.0) / (THREAD_SEARCH_RRF_K + rank as f64)
        })
    };
    let score = (contribution(lexical_rank) + contribution(semantic_rank)) / 2.0;
    // Monotonic calibration preserves SQL order and the existing recall scale:
    // a lone rank r still scores 61/(2*(60+r)), including 0.25 at rank 62.
    // Above the single-branch ceiling, retain the 0.5..1 agreement range.
    // This avoids silently dropping lexical/semantic-only recall candidates
    // when sharpening fusion, without changing any caller's cutoff settings.
    if score <= 0.5 {
        const SINGLE_BRANCH_SCORE_K: f64 = 60.0;
        ((SINGLE_BRANCH_SCORE_K + 1.0) * score
            / (2.0 * (SINGLE_BRANCH_SCORE_K - THREAD_SEARCH_RRF_K) * score
                + THREAD_SEARCH_RRF_K
                + 1.0)) as f32
    } else {
        score as f32
    }
}

#[derive(Debug, Clone)]
pub struct ThreadEpisodicSearchQuery {
    pub workspace_id: String,
    pub thread_id: Option<String>,
    pub access: ThreadEpisodicSearchAccess,
    pub query: String,
    pub lexical: bool,
    pub embedding: Option<ThreadEpisodicSearchEmbedding>,
    pub created_after_unix: Option<i64>,
    pub offset: u64,
    pub limit: u32,
    pub snippet_chars: u32,
}

#[derive(Debug, Clone)]
pub struct ThreadEpisodicSearchEmbedding {
    pub provider_id: String,
    pub model: String,
    pub dimension: usize,
    pub normalized: bool,
    pub vector: Vec<f32>,
}

/// Rank and hydrate only the page selected by the database.
pub struct RankedItem {
    pub item: thread_episodic_items::Model,
    pub lexical_rank: Option<i64>,
    pub semantic_rank: Option<i64>,
}

pub struct SearchPage {
    pub rows: Vec<RankedItem>,
    pub incomplete_sources: bool,
}

pub async fn search<C: ConnectionTrait>(
    db: &C,
    request: &ThreadEpisodicSearchQuery,
) -> Result<SearchPage> {
    ensure!(
        (1..=257).contains(&request.limit),
        "invalid search page size"
    );
    ensure!(
        (1..=4000).contains(&request.snippet_chars),
        "invalid snippet size"
    );
    ensure!(
        request.query.len() <= 64 * 1024,
        "episodic search query exceeds byte limit"
    );

    let (provider, model, dimension, normalized, vector) =
        if let Some(embedding) = &request.embedding {
            ensure!(
                embedding.dimension > 0
                    && embedding.vector.len() == embedding.dimension
                    && embedding.vector.iter().all(|value| value.is_finite())
                    && embedding.vector.iter().any(|value| *value != 0.0),
                "invalid query embedding"
            );
            (
                Some(embedding.provider_id.as_str()),
                Some(embedding.model.as_str()),
                Some(i64::try_from(embedding.dimension)?),
                Some(embedding.normalized),
                Some(PgVector::from(embedding.vector.clone())),
            )
        } else {
            (None, None, None, None, None)
        };

    // SeaORM's typed selects cannot express this combination of PostgreSQL's
    // array overlap, CTE-backed rank fusion, and typed projection with one
    // coverage row even when the page is empty.
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            include_str!("episodic_search.sql"),
            [
                request.workspace_id.as_str().into(),
                request.thread_id.as_deref().into(),
                request.access.root_thread_id.as_deref().into(),
                request.access.include_internal.into(),
                request.access.current_thread_id.as_deref().into(),
                request.access.principal_id.as_deref().into(),
                request.created_after_unix.into(),
                request.lexical.into(),
                request.query.as_str().into(),
                provider.into(),
                model.into(),
                dimension.into(),
                normalized.into(),
                vector.into(),
                THREAD_SEARCH_RRF_K.into(),
                i64::from(request.limit).into(),
                i64::try_from(request.offset)?.into(),
                i32::try_from(request.snippet_chars)?.into(),
            ],
        ))
        .await?;
    let incomplete_sources = rows
        .first()
        .ok_or_else(|| anyhow::anyhow!("search coverage row missing"))?
        .try_get("", "incomplete")?;
    let mut page = Vec::with_capacity(rows.len());
    for row in rows {
        if row.try_get::<Option<String>>("", "id")?.is_some() {
            page.push(RankedItem {
                item: thread_episodic_items::Model::from_query_result(&row, "")?,
                lexical_rank: row.try_get("", "lexical_rank")?,
                semantic_rank: row.try_get("", "semantic_rank")?,
            });
        }
    }
    Ok(SearchPage {
        rows: page,
        incomplete_sources,
    })
}

pub async fn search_projection<C: ConnectionTrait>(
    db: &C,
    request: &ThreadEpisodicSearchQuery,
) -> Result<ThreadEpisodicSearchResult> {
    let page = search(db, request).await?;
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
    let marker = find_projection_meta(db, &projection_key).await?;
    let coverage_incomplete = page.incomplete_sources
        || !marker.is_some_and(|marker| marker.status == PROJECTION_META_STATUS_COMPLETE);
    Ok(ThreadEpisodicSearchResult {
        rows,
        coverage_incomplete,
    })
}
