#![cfg(feature = "parallel_segments")]

use super::builder::BuildOpts;
use crate::types::FrameId;

/// High-level planner responsible for turning extracted documents into `SegmentPlan`s.
pub struct SegmentPlanner {
    opts: BuildOpts,
}

impl SegmentPlanner {
    pub fn new(opts: BuildOpts) -> Self {
        Self { opts }
    }

    /// Groups frame-backed chunks into segment-sized plans based on configured limits.
    pub fn plan_from_chunks(&self, mut chunks: Vec<SegmentChunkPlan>) -> Vec<SegmentPlan> {
        let mut plans = Vec::new();
        if chunks.is_empty() {
            return plans;
        }

        chunks.sort_by_key(|chunk| (chunk.frame_id, chunk.chunk_index));

        let mut current_chunks = Vec::new();
        let mut acc_tokens = 0usize;
        let mut acc_pages = 0usize;
        let mut running_tokens = 0usize;

        for mut chunk in chunks.into_iter() {
            let chunk_tokens = chunk.token_estimate.max(1);
            let chunk_pages = chunk.page_span().max(1);
            chunk.token_start = running_tokens;
            chunk.token_end = running_tokens + chunk_tokens;
            running_tokens += chunk_tokens;

            if !current_chunks.is_empty()
                && (acc_tokens + chunk_tokens > self.opts.segment_tokens
                    || acc_pages + chunk_pages > self.opts.segment_pages)
            {
                let last_token_end = current_chunks
                    .last()
                    .map(|c: &SegmentChunkPlan| c.token_end)
                    .unwrap_or(0);
                plans.push(SegmentPlan::new(
                    std::mem::take(&mut current_chunks),
                    acc_tokens,
                    acc_pages,
                    last_token_end,
                ));
                acc_tokens = 0;
                acc_pages = 0;
            }

            acc_tokens += chunk_tokens;
            acc_pages += chunk_pages;
            current_chunks.push(chunk);
        }

        if !current_chunks.is_empty() {
            let last_token_end = current_chunks
                .last()
                .map(|c: &SegmentChunkPlan| c.token_end)
                .unwrap_or(running_tokens);
            plans.push(SegmentPlan::new(
                current_chunks,
                acc_tokens,
                acc_pages,
                last_token_end,
            ));
        }
        plans
    }
}

/// Describes the work required to build a segment (lex/vec/time bundles).
#[derive(Debug, Clone)]
pub struct SegmentPlan {
    pub estimated_tokens: usize,
    pub estimated_pages: usize,
    pub token_start: usize,
    pub token_end: usize,
    pub chunk_count: usize,
    pub chunks: Vec<SegmentChunkPlan>,
}

impl SegmentPlan {
    fn new(
        chunks: Vec<SegmentChunkPlan>,
        estimated_tokens: usize,
        estimated_pages: usize,
        token_end: usize,
    ) -> Self {
        let chunk_count = chunks.len();
        let token_start = chunks.first().map(|c| c.token_start).unwrap_or(0);
        Self {
            estimated_tokens,
            estimated_pages,
            token_start,
            token_end,
            chunk_count,
            chunks,
        }
    }
}

/// Individual chunk of text slated for indexing.
#[derive(Debug, Clone)]
pub struct SegmentChunkPlan {
    pub text: String,
    pub frame_id: FrameId,
    pub timestamp: i64,
    pub chunk_index: usize,
    pub chunk_count: usize,
    pub token_estimate: usize,
    pub token_start: usize,
    pub token_end: usize,
    pub page_start: usize,
    pub page_end: usize,
    pub embedding: Option<Vec<f32>>,
}

impl SegmentChunkPlan {
    pub fn page_span(&self) -> usize {
        if self.page_end >= self.page_start {
            self.page_end - self.page_start + 1
        } else {
            1
        }
    }
}

/// Planner-produced work messages consumed by workers.
#[derive(Debug)]
pub enum PlannerMessage {
    Plan {
        plan_index: usize,
        plan: SegmentPlan,
    },
    Shutdown,
}
