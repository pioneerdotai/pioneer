#![cfg(feature = "parallel_segments")]

use std::{
    any::Any,
    collections::HashMap,
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Instant,
};

use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::debug;

use super::{
    builder::BuildOpts,
    planner::{PlannerMessage, SegmentPlan},
    segments::{LexSegmentArtifact, TimeSegmentArtifact, VecSegmentArtifact},
};
use crate::{
    MemvidError, Result, TimeIndexEntry, time_index_append,
    types::{SegmentSpan, SegmentStats},
};

/// Minimum number of vectors required to use Product Quantization.
/// Below this threshold, we fall back to uncompressed vectors.
/// PQ requires training k-means on many vectors to learn good codebooks.
const MIN_VECTORS_FOR_PQ: usize = 100;

/// Drives segment-building work by fanning `SegmentPlan`s across worker threads.
pub(crate) struct SegmentWorkerPool {
    threads: usize,
    queue_depth: usize,
    opts: BuildOpts,
}

#[derive(Debug)]
pub(crate) struct SegmentArtifact<T> {
    pub artifact: T,
    pub stats: SegmentStats,
}

#[derive(Debug)]
pub(crate) struct SegmentResult {
    pub plan_index: usize,
    pub span: Option<SegmentSpan>,
    pub lex: Option<SegmentArtifact<LexSegmentArtifact>>,
    pub vec: Option<SegmentArtifact<VecSegmentArtifact>>,
    pub time: Option<SegmentArtifact<TimeSegmentArtifact>>,
}

enum WorkerMessage {
    Result(SegmentResult),
    Error(MemvidError),
}

impl SegmentWorkerPool {
    pub fn new(opts: &BuildOpts) -> Self {
        Self {
            threads: opts.threads.max(1),
            queue_depth: opts.queue_depth.max(1),
            opts: opts.clone(),
        }
    }

    pub fn execute(&self, plans: Vec<SegmentPlan>) -> Result<Vec<SegmentResult>> {
        let plan_count = plans.len();
        if plan_count == 0 {
            return Ok(Vec::new());
        }

        let (plan_tx, plan_rx) = bounded(self.queue_depth);
        let (result_tx, result_rx) = bounded(self.queue_depth);
        let cancel_flag = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::with_capacity(self.threads);
        for worker_id in 0..self.threads {
            let rx = plan_rx.clone();
            let tx = result_tx.clone();
            let cancel = cancel_flag.clone();
            let opts = self.opts.clone();
            handles.push(thread::spawn(move || {
                worker_loop(worker_id, rx, tx, cancel, opts)
            }));
        }
        drop(result_tx);

        for (plan_index, plan) in plans.into_iter().enumerate() {
            if cancel_flag.load(Ordering::SeqCst) {
                break;
            }
            if plan_tx
                .send(PlannerMessage::Plan { plan_index, plan })
                .is_err()
            {
                cancel_flag.store(true, Ordering::SeqCst);
                break;
            }
        }
        for _ in 0..self.threads {
            let _ = plan_tx.send(PlannerMessage::Shutdown);
        }
        drop(plan_tx);

        let mut results = Vec::with_capacity(plan_count);
        let mut worker_error: Option<MemvidError> = None;
        while results.len() < plan_count {
            match result_rx.recv() {
                Ok(WorkerMessage::Result(result)) => {
                    results.push(result);
                }
                Ok(WorkerMessage::Error(err)) => {
                    worker_error = Some(err);
                    cancel_flag.store(true, Ordering::SeqCst);
                    break;
                }
                Err(_) => {
                    if worker_error.is_none() {
                        worker_error = Some(MemvidError::CheckpointFailed {
                            reason: "worker channel closed unexpectedly".into(),
                        });
                    }
                    break;
                }
            }
        }

        for handle in handles {
            if let Err(panic) = handle.join() {
                if worker_error.is_none() {
                    worker_error = Some(MemvidError::CheckpointFailed {
                        reason: format!(
                            "parallel segment worker panicked: {}",
                            panic_payload(&panic)
                        ),
                    });
                }
            }
        }

        if worker_error.is_none() && results.len() != plan_count {
            worker_error = Some(MemvidError::CheckpointFailed {
                reason: format!(
                    "expected {plan_count} segment results, received {}",
                    results.len()
                ),
            });
        }

        if let Some(err) = worker_error {
            return Err(err);
        }

        results.sort_by_key(|result| result.plan_index);
        Ok(results)
    }
}

fn worker_loop(
    worker_id: usize,
    plan_rx: Receiver<PlannerMessage>,
    result_tx: Sender<WorkerMessage>,
    cancel: Arc<AtomicBool>,
    opts: BuildOpts,
) {
    while !cancel.load(Ordering::SeqCst) {
        match plan_rx.recv() {
            Ok(PlannerMessage::Plan { plan_index, plan }) => {
                debug!(
                    worker_id,
                    plan_index,
                    chunks = plan.chunks.len(),
                    tokens = plan.estimated_tokens,
                    pages = plan.estimated_pages,
                    "segment worker building artifacts"
                );
                match build_segment(plan_index, plan, &opts) {
                    Ok(result) => {
                        if result_tx.send(WorkerMessage::Result(result)).is_err() {
                            cancel.store(true, Ordering::SeqCst);
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = result_tx.send(WorkerMessage::Error(err));
                        cancel.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            }
            Ok(PlannerMessage::Shutdown) | Err(_) => break,
        }
    }
}

fn build_segment(plan_index: usize, plan: SegmentPlan, opts: &BuildOpts) -> Result<SegmentResult> {
    let span = span_from_plan(plan_index, &plan);
    let lex = build_lex_artifact(plan_index, &plan)?;
    let vec = build_vec_artifact(plan_index, &plan, opts)?;
    let time = build_time_artifact(plan_index, &plan)?;
    Ok(SegmentResult {
        plan_index,
        span,
        lex,
        vec,
        time,
    })
}

fn build_lex_artifact(
    _plan_index: usize,
    plan: &SegmentPlan,
) -> Result<Option<SegmentArtifact<LexSegmentArtifact>>> {
    if plan.chunks.is_empty() {
        return Ok(None);
    }
    let mut builder = crate::lex::LexIndexBuilder::new();
    let mut docs_added = 0usize;
    let tags = HashMap::new();
    let start = Instant::now();
    for chunk in &plan.chunks {
        if chunk.text.trim().is_empty() {
            continue;
        }
        docs_added += 1;
        let uri = format!("memvid://frame/{}", chunk.frame_id);
        builder.add_document(chunk.frame_id, &uri, None, &chunk.text, &tags);
    }
    if docs_added == 0 {
        return Ok(None);
    }
    let artifact = builder.finish()?;
    if artifact.doc_count == 0 {
        return Ok(None);
    }
    let artifact = LexSegmentArtifact {
        bytes: artifact.bytes,
        doc_count: artifact.doc_count,
        checksum: artifact.checksum,
    };
    let stats = SegmentStats {
        doc_count: artifact.doc_count,
        vector_count: 0,
        time_entries: 0,
        bytes_uncompressed: artifact.bytes.len() as u64,
        build_micros: start.elapsed().as_micros() as u64,
    };
    Ok(Some(SegmentArtifact { artifact, stats }))
}

fn build_vec_artifact(
    _plan_index: usize,
    plan: &SegmentPlan,
    opts: &BuildOpts,
) -> Result<Option<SegmentArtifact<VecSegmentArtifact>>> {
    use crate::types::VectorCompression;
    use tracing::info;

    if plan.chunks.is_empty() {
        info!("build_vec_artifact: plan.chunks is empty, returning None");
        return Ok(None);
    }

    let start = Instant::now();

    // Count non-empty vectors
    let non_empty_count = plan
        .chunks
        .iter()
        .filter(|chunk| chunk.embedding.as_ref().map_or(false, |e| !e.is_empty()))
        .count();

    info!(
        chunks = plan.chunks.len(),
        non_empty_count, "build_vec_artifact: checking embeddings in plan"
    );

    // Determine effective compression: use uncompressed if below PQ threshold
    let effective_compression = match &opts.vec_compression {
        VectorCompression::Pq96 if non_empty_count < MIN_VECTORS_FOR_PQ => {
            // Fall back to uncompressed for small vector counts
            VectorCompression::None
        }
        other => other.clone(),
    };

    match effective_compression {
        VectorCompression::None => {
            // Uncompressed path - use regular VecIndexBuilder
            let mut builder = crate::vec::VecIndexBuilder::new();
            let mut vectors = 0usize;
            let mut dimension = 0u32;

            for chunk in &plan.chunks {
                let Some(embedding) = chunk.embedding.as_ref() else {
                    continue;
                };
                if embedding.is_empty() {
                    continue;
                }
                dimension = dimension.max(embedding.len() as u32);
                vectors += 1;
                builder.add_document(chunk.frame_id, embedding.clone());
            }

            if vectors == 0 {
                return Ok(None);
            }

            let artifact = builder.finish()?;
            if artifact.vector_count == 0 {
                return Ok(None);
            }

            let final_dimension = if artifact.dimension == 0 {
                dimension
            } else {
                artifact.dimension
            };

            let artifact = VecSegmentArtifact {
                bytes: artifact.bytes,
                vector_count: artifact.vector_count,
                dimension: final_dimension,
                checksum: artifact.checksum,
                compression: VectorCompression::None,
                #[cfg(feature = "parallel_segments")]
                bytes_uncompressed: artifact.bytes_uncompressed,
            };
            let stats = SegmentStats {
                doc_count: 0,
                vector_count: artifact.vector_count,
                time_entries: 0,
                bytes_uncompressed: artifact.bytes_uncompressed,
                build_micros: start.elapsed().as_micros() as u64,
            };
            Ok(Some(SegmentArtifact { artifact, stats }))
        }
        VectorCompression::Pq96 => {
            // Compressed path - use QuantizedVecIndexBuilder
            let mut builder = crate::vec_pq::QuantizedVecIndexBuilder::new();
            let mut dimension = 0u32;

            // Collect all vectors for training
            let mut training_vectors = Vec::new();
            for chunk in &plan.chunks {
                let Some(embedding) = chunk.embedding.as_ref() else {
                    continue;
                };
                if embedding.is_empty() {
                    continue;
                }
                dimension = dimension.max(embedding.len() as u32);
                training_vectors.push(embedding.clone());
            }

            if training_vectors.is_empty() {
                return Ok(None);
            }

            // Train the quantizer on all vectors
            builder.train_quantizer(&training_vectors, dimension)?;

            // Add all documents
            for chunk in &plan.chunks {
                let Some(embedding) = chunk.embedding.as_ref() else {
                    continue;
                };
                if embedding.is_empty() {
                    continue;
                }
                builder.add_document(chunk.frame_id, embedding.clone())?;
            }

            let artifact = builder.finish()?;
            if artifact.vector_count == 0 {
                return Ok(None);
            }

            let final_dimension = if artifact.dimension == 0 {
                dimension
            } else {
                artifact.dimension
            };

            let bytes_len = artifact.bytes.len() as u64;

            let artifact = VecSegmentArtifact {
                bytes: artifact.bytes,
                vector_count: artifact.vector_count,
                dimension: final_dimension,
                checksum: artifact.checksum,
                compression: VectorCompression::Pq96,
                #[cfg(feature = "parallel_segments")]
                bytes_uncompressed: bytes_len,
            };
            let stats = SegmentStats {
                doc_count: 0,
                vector_count: artifact.vector_count,
                time_entries: 0,
                bytes_uncompressed: bytes_len,
                build_micros: start.elapsed().as_micros() as u64,
            };
            Ok(Some(SegmentArtifact { artifact, stats }))
        }
    }
}

fn build_time_artifact(
    _plan_index: usize,
    plan: &SegmentPlan,
) -> Result<Option<SegmentArtifact<TimeSegmentArtifact>>> {
    if plan.chunks.is_empty() {
        return Ok(None);
    }
    let mut entries = Vec::with_capacity(plan.chunks.len());
    for chunk in &plan.chunks {
        entries.push(TimeIndexEntry::new(chunk.timestamp, chunk.frame_id));
    }
    if entries.is_empty() {
        return Ok(None);
    }
    let start = Instant::now();
    let mut cursor = Cursor::new(Vec::new());
    let (_, _length, checksum) = time_index_append(&mut cursor, &mut entries)?;
    let bytes = cursor.into_inner();
    if bytes.is_empty() {
        return Ok(None);
    }
    let artifact = TimeSegmentArtifact {
        bytes,
        entry_count: entries.len() as u64,
        checksum,
    };
    let stats = SegmentStats {
        doc_count: 0,
        vector_count: 0,
        time_entries: entries.len() as u64,
        bytes_uncompressed: artifact.bytes.len() as u64,
        build_micros: start.elapsed().as_micros() as u64,
    };
    Ok(Some(SegmentArtifact { artifact, stats }))
}

fn span_from_plan(_plan_index: usize, plan: &SegmentPlan) -> Option<SegmentSpan> {
    let first = plan.chunks.first()?;
    let last = plan.chunks.last()?;
    Some(SegmentSpan {
        frame_start: first.frame_id,
        frame_end: last.frame_id,
        page_start: first.page_start as u32,
        page_end: last.page_end as u32,
        token_start: plan.token_start as u64,
        token_end: plan.token_end as u64,
    })
}

fn panic_payload(payload: &Box<dyn Any + Send + 'static>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown".to_string()
    }
}
