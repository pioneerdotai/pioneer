use super::query;
use super::schema::{build_schema, initialise_tokenizer};
use super::util::to_search_value;
use crate::search::parser::ParsedQuery;
use crate::types::{Frame, FrameId};
use crate::{MemvidError, Result};
use blake3::{Hasher, hash};
use tantivy::ReloadPolicy;
use tantivy::collector::TopDocs;
use tantivy::indexer::IndexWriter;
use tantivy::schema::{Field, OwnedValue, Schema, TantivyDocument};
use tantivy::{Index, IndexReader, Term, doc};
use tempfile::TempDir;

/// Tantivy-backed search index used when the `lex` feature is enabled.
///
/// Field order is load-bearing for `Drop`: Rust drops struct fields in
/// declaration order, so `work_dir` (the temporary directory backing the
/// index) **must remain the last field**. The `index`, `reader`, and
/// `index_writer` all keep file handles and lock files open inside that
/// directory; if the `TempDir` were dropped first, its directory removal
/// would fail on platforms that refuse to delete files with open handles
/// (notably Windows), silently leaking one working directory per discarded
/// engine. See https://github.com/memvid/memvid/issues/215.
pub struct TantivyEngine {
    pub(super) index: Index,
    pub(super) _schema: Schema,
    pub(super) content: Field,
    pub(super) tags: Field,
    pub(super) labels: Field,
    pub(super) track: Field,
    pub(super) timestamp: Field,
    pub(super) uri: Field,
    pub(super) frame_id: Field,
    pub(super) index_writer: Option<IndexWriter>,
    pub(super) reader: IndexReader,
    pub(super) tokenizer: Option<String>,
    // MUST be the last field — see the type-level comment above.
    pub(super) work_dir: TempDir,
}

impl Drop for TantivyEngine {
    fn drop(&mut self) {
        // Release the exclusive writer (and its `.tantivy-writer.lock`) before
        // the `work_dir` TempDir is removed during normal field drop. Without
        // this the writer lock can still be held when the directory removal
        // runs, leaking the working directory on Windows. See issue #215.
        if let Some(writer) = self.index_writer.take() {
            drop(writer);
        }
    }
}

/// Search hit returned from Tantivy queries.
pub struct TantivyDocHit {
    pub frame_id: u64,
    pub score: f32,
    #[allow(dead_code)] // Content preserved for debugging; evaluation uses frame metadata
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct TantivySnapshot {
    pub doc_count: u64,
    pub checksum: [u8; 32],
    pub segments: Vec<TantivySegmentBlob>,
}

#[derive(Debug, Clone)]
pub struct TantivySegmentBlob {
    pub path: String,
    pub bytes: Vec<u8>,
    pub checksum: [u8; 32],
}

impl TantivyEngine {
    pub fn create() -> Result<Self> {
        let dir = TempDir::new().map_err(|err| MemvidError::Tantivy {
            reason: format!("failed to allocate Tantivy work directory: {err}"),
        })?;
        let schema = build_schema();
        let index = Index::create_in_dir(dir.path(), schema.clone()).map_err(|err| {
            MemvidError::Tantivy {
                reason: err.to_string(),
            }
        })?;
        initialise_tokenizer(&index);
        Self::from_parts(dir, index, schema)
    }

    pub fn open_from_dir(dir: TempDir) -> Result<Self> {
        let index = Index::open_in_dir(dir.path()).map_err(|err| MemvidError::Tantivy {
            reason: err.to_string(),
        })?;
        initialise_tokenizer(&index);
        let schema = index.schema();
        Self::from_parts(dir, index, schema)
    }

    fn from_parts(dir: TempDir, index: Index, schema: Schema) -> Result<Self> {
        let content = schema
            .get_field("content")
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        let tags = schema
            .get_field("tags")
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        let labels = schema
            .get_field("labels")
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        let track = schema
            .get_field("track")
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        let timestamp = schema
            .get_field("timestamp")
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        let uri = schema
            .get_field("uri")
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        let frame_id = schema
            .get_field("frame_id")
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;

        let writer = index
            .writer(50_000_000)
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        // Commits below reload the reader synchronously before returning. Keeping
        // Tantivy's default commit watcher enabled would trigger a second,
        // concurrent reload against the same temporary directory. On macOS that
        // can race on `.tantivy-meta.lock` and fail with `EINVAL`, even though the
        // synchronous reload has already made the commit searchable.
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|err: tantivy::TantivyError| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;

        Ok(Self {
            work_dir: dir,
            index,
            _schema: schema,
            content,
            tags,
            labels,
            track,
            timestamp,
            uri,
            frame_id,
            index_writer: Some(writer),
            reader,
            tokenizer: Some("memvid_default".to_string()),
        })
    }

    fn take_writer(&mut self) -> Result<IndexWriter> {
        self.index_writer.take().ok_or(MemvidError::Tantivy {
            reason: "tantivy index writer unavailable".into(),
        })
    }

    fn writer_mut(&mut self) -> Result<&mut IndexWriter> {
        self.index_writer.as_mut().ok_or(MemvidError::Tantivy {
            reason: "tantivy index writer unavailable".into(),
        })
    }

    fn create_writer(&self) -> Result<IndexWriter> {
        // Use single thread for deterministic index generation
        self.index
            .writer_with_num_threads(1, 50_000_000)
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })
    }

    pub fn add_frame(&mut self, frame: &Frame, content: &str) -> Result<()> {
        if content.trim().is_empty() {
            return Ok(());
        }
        let mut document = doc!(
            self.content => content,
            self.timestamp => frame.timestamp,
            self.frame_id => frame.id,
        );
        for tag in &frame.tags {
            document.add_text(self.tags, to_search_value(tag));
        }
        for label in &frame.labels {
            document.add_text(self.labels, to_search_value(label));
        }
        if let Some(track) = &frame.track {
            document.add_text(self.track, to_search_value(track));
        }
        if let Some(uri) = &frame.uri {
            document.add_text(self.uri, to_search_value(uri));
        }
        self.writer_mut()?
            .add_document(document)
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        Ok(())
    }

    pub fn delete_frame(&mut self, frame_id: FrameId) -> Result<()> {
        let term = Term::from_field_u64(self.frame_id, frame_id);
        if let Some(writer) = self.index_writer.as_mut() {
            writer.delete_term(term);
        }
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        let mut writer = self.take_writer()?;
        writer.commit().map_err(|err| MemvidError::Tantivy {
            reason: err.to_string(),
        })?;
        writer
            .wait_merging_threads()
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        self.index_writer = Some(self.create_writer()?);
        self.reader.reload().map_err(|err| MemvidError::Tantivy {
            reason: err.to_string(),
        })?;
        Ok(())
    }

    /// Soft commit that makes documents searchable immediately without waiting for merge.
    /// Used for instant indexing during progressive ingestion (Phase 1).
    /// This is faster than full `commit()` but leaves segments unmerged.
    pub fn soft_commit(&mut self) -> Result<()> {
        let writer = self.writer_mut()?;
        writer.commit().map_err(|err| MemvidError::Tantivy {
            reason: err.to_string(),
        })?;
        // Don't wait for merge threads - let them run in background
        // Reload reader to make new documents searchable immediately
        self.reader.reload().map_err(|err| MemvidError::Tantivy {
            reason: err.to_string(),
        })?;
        Ok(())
    }

    /// Add frame and make it searchable immediately via soft commit.
    /// Returns Ok(true) if the frame was indexed, Ok(false) if skipped (empty content).
    #[allow(dead_code)]
    pub fn add_frame_immediate(&mut self, frame: &Frame, content: &str) -> Result<bool> {
        if content.trim().is_empty() {
            return Ok(false);
        }
        self.add_frame(frame, content)?;
        self.soft_commit()?;
        Ok(true)
    }

    pub fn reset(&mut self) -> Result<()> {
        let mut writer = self.take_writer()?;
        writer
            .delete_all_documents()
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        writer.commit().map_err(|err| MemvidError::Tantivy {
            reason: err.to_string(),
        })?;
        writer
            .wait_merging_threads()
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        self.index_writer = Some(self.create_writer()?);
        self.reader.reload().map_err(|err| MemvidError::Tantivy {
            reason: err.to_string(),
        })?;
        Ok(())
    }

    pub fn search_documents(
        &self,
        parsed: &ParsedQuery,
        uri_filter: Option<&str>,
        scope_filter: Option<&str>,
        frame_filter: Option<&[u64]>,
        limit: usize,
    ) -> Result<Vec<TantivyDocHit>> {
        if let Some(ids) = frame_filter {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
        }

        let query = query::build_root_query(self, parsed, uri_filter, scope_filter, frame_filter)?;
        let doc_limit = limit.max(1);
        let searcher = self.reader.searcher();
        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(doc_limit))
            .map_err(|err| MemvidError::Tantivy {
                reason: err.to_string(),
            })?;
        let mut results = Vec::new();
        for (score, address) in top_docs {
            let document: TantivyDocument =
                searcher.doc(address).map_err(|err| MemvidError::Tantivy {
                    reason: err.to_string(),
                })?;
            let frame_id = match document.get_first(self.frame_id) {
                Some(value) => match OwnedValue::from(value) {
                    OwnedValue::U64(id) => id,
                    _ => {
                        return Err(MemvidError::Tantivy {
                            reason: "tantivy doc missing frame_id".into(),
                        });
                    }
                },
                None => {
                    return Err(MemvidError::Tantivy {
                        reason: "tantivy doc missing frame_id".into(),
                    });
                }
            };
            let content = match document.get_first(self.content) {
                Some(value) => match OwnedValue::from(value) {
                    OwnedValue::Str(text) => text,
                    _ => String::new(),
                },
                None => String::new(),
            };
            results.push(TantivyDocHit {
                frame_id,
                score,
                content,
            });
        }
        Ok(results)
    }

    pub fn snapshot_segments(&self) -> Result<TantivySnapshot> {
        let entries =
            std::fs::read_dir(self.work_dir.path()).map_err(|err| MemvidError::Tantivy {
                reason: format!(
                    "failed to read Tantivy index directory {}: {}",
                    self.work_dir.path().display(),
                    err
                ),
            })?;
        let mut file_names: Vec<String> = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|err| MemvidError::Tantivy {
                reason: format!(
                    "failed to iterate Tantivy index directory {}: {}",
                    self.work_dir.path().display(),
                    err
                ),
            })?;
            let file_type = entry.file_type().map_err(|err| MemvidError::Tantivy {
                reason: format!(
                    "failed to inspect Tantivy index entry {}: {}",
                    entry.path().display(),
                    err
                ),
            })?;
            if file_type.is_file() {
                let name = entry.file_name().to_string_lossy().into_owned();
                // Skip Tantivy lock files - they're held open and cause Windows errors
                if name.starts_with(".tantivy-") {
                    continue;
                }
                file_names.push(name);
            }
        }
        file_names.sort();

        let mut segments = Vec::with_capacity(file_names.len());
        let mut index_hasher = Hasher::new();

        for name in file_names {
            let path = self.work_dir.path().join(&name);
            let bytes = std::fs::read(&path).map_err(|err| MemvidError::Tantivy {
                reason: format!("failed to read Tantivy segment {}: {}", path.display(), err),
            })?;
            let checksum = *hash(&bytes).as_bytes();
            index_hasher.update(&checksum);
            index_hasher.update(name.as_bytes());
            segments.push(TantivySegmentBlob {
                path: name,
                bytes,
                checksum,
            });
        }

        let checksum = *index_hasher.finalize().as_bytes();
        Ok(TantivySnapshot {
            doc_count: self.reader.searcher().num_docs(),
            checksum,
            segments,
        })
    }

    pub(crate) fn analyse_text(&self, text: &str) -> Vec<String> {
        if let Some(name) = &self.tokenizer {
            if let Some(mut analyzer) = self.index.tokenizers().get(name) {
                let mut stream = analyzer.token_stream(text);
                let mut tokens = Vec::new();
                while stream.advance() {
                    tokens.push(stream.token().text.to_string());
                }
                return tokens;
            }
        }
        if text.trim().is_empty() {
            Vec::new()
        } else {
            vec![text.to_ascii_lowercase()]
        }
    }

    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_test_document(engine: &mut TantivyEngine, frame_id: u64, content: &str) {
        let mut document = TantivyDocument::default();
        document.add_u64(engine.frame_id, frame_id);
        document.add_text(engine.content, content);
        engine
            .writer_mut()
            .expect("writer is available")
            .add_document(document)
            .expect("test document is accepted");
    }

    #[test]
    fn manual_reader_reload_keeps_commit_paths_searchable() {
        let mut engine = TantivyEngine::create().expect("engine is created");

        add_test_document(&mut engine, 1, "first document");
        engine.soft_commit().expect("soft commit succeeds");
        assert_eq!(engine.num_docs(), 1);

        add_test_document(&mut engine, 2, "second document");
        engine.commit().expect("full commit succeeds");
        assert_eq!(engine.num_docs(), 2);

        engine.reset().expect("reset succeeds");
        assert_eq!(engine.num_docs(), 0);
    }
}
