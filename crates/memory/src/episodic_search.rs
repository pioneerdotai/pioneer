//! Pioneer retrieval contract. Providers return ordered source references, never
//! trusted content. Consumers validate canonical sources before rank fusion.
use crate::ThreadEpisodicEmbeddingIdentity;
use async_trait::async_trait;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct EpisodicSearchScope {
    pub workspace_id: String,
    /// None means workspace candidate discovery, subject to the caller's read authority.
    pub thread_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EpisodicQueryEmbedding {
    pub identity: ThreadEpisodicEmbeddingIdentity,
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpisodicSourceCandidate {
    pub source_id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub source_version: String,
}

#[derive(Debug, Clone, Default)]
pub struct EpisodicChannelCandidates {
    /// Independent ordered partitions; raw scores are intentionally absent.
    pub partitions: Vec<Vec<EpisodicSourceCandidate>>,
    pub coverage_limited: bool,
    pub unavailable: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct EpisodicSearchWork {
    pub max_partitions: u32,
    pub candidates_per_partition: u32,
}

#[async_trait]
pub trait EpisodicCandidateProvider: Send + Sync {
    async fn lexical(
        &self,
        query: &str,
        scope: &EpisodicSearchScope,
        work: EpisodicSearchWork,
    ) -> EpisodicChannelCandidates;
    async fn semantic(
        &self,
        query: &str,
        scope: &EpisodicSearchScope,
        embedding: &EpisodicQueryEmbedding,
        work: EpisodicSearchWork,
    ) -> EpisodicChannelCandidates;
}

pub use pioneer_protocol::ThreadsSearchLimitations as EpisodicSearchLimitations;

/// Each source contributes at most once per channel, even if replicated in
/// several partitions. Ranks are computed after canonical-source deduplication.
/// No raw scores or old Memvid relevancy thresholds participate.
pub fn fuse_episodic_ranks(channels: &[Vec<Vec<String>>]) -> Vec<(String, f32)> {
    let mut fused = BTreeMap::<String, f32>::new();
    for partitions in channels {
        let mut channel = BTreeMap::<String, f32>::new();
        for partition in partitions {
            let mut seen = std::collections::BTreeSet::new();
            let mut rank = 0;
            for source in partition {
                if !seen.insert(source) {
                    continue;
                }
                rank += 1;
                let score = 1.0 / (60.0 + rank as f32);
                channel
                    .entry(source.clone())
                    .and_modify(|v| *v = v.max(score))
                    .or_insert(score);
            }
        }
        // Convert partition rank contributions to a single channel ranking.
        let mut ordered = channel.into_iter().collect::<Vec<_>>();
        ordered.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        for (rank, (id, _)) in ordered.into_iter().enumerate() {
            *fused.entry(id).or_default() += 1.0 / (61.0 + rank as f32);
        }
    }
    let mut ordered = fused.into_iter().collect::<Vec<_>>();
    ordered.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ordered
}

/// Turn-local identity for reusing an already contributed current-thread search.
pub fn episodic_query_fingerprint(
    workspace: &str,
    thread: &str,
    turn: &str,
    query: &str,
) -> String {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(&(workspace, thread, turn, query)).expect("string tuple");
    format!("{:x}", Sha256::digest(bytes))
}

pub fn episodic_source_identity(id: &str) -> &str {
    if id.starts_with("thread:") {
        id.rsplit_once('/').map(|(source, _)| source).unwrap_or(id)
    } else {
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fusion_uses_channel_ranks_and_does_not_reward_replicated_sources() {
        let list = |ids: &[&str]| ids.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let channels = vec![
            vec![list(&["b", "b", "a"]), list(&["b", "b", "a"])],
            vec![list(&["a", "b"])],
        ];
        let result = fuse_episodic_ranks(&channels);
        assert_eq!(
            result.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert!((result[0].1 - (1.0 / 61.0 + 1.0 / 62.0)).abs() < 0.00001);
        assert_eq!(
            result,
            fuse_episodic_ranks(&[vec![list(&["b", "a"])], vec![list(&["a", "b"])]])
        );
    }
}
