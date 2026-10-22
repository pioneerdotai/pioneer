//! Replay session operations for Memvid.
//!
//! This module provides session management for time-travel replay functionality,
//! enabling recording and replaying of agent sessions.

use crate::error::Result;
use crate::memvid::lifecycle::Memvid;
use crate::replay::{
    ActionType, ActiveSession, ReplayAction, ReplayConfig, ReplayManifest, ReplaySession,
    SessionSummary, StateSnapshot,
};
use uuid::Uuid;

impl Memvid {
    /// Start recording a new replay session.
    ///
    /// Returns the session ID for the newly created session.
    /// Only one session can be active at a time.
    ///
    /// # Arguments
    /// * `name` - Optional human-readable name for the session
    /// * `config` - Configuration for the recording session
    #[cfg(feature = "replay")]
    pub fn start_session(
        &mut self,
        name: Option<String>,
        config: Option<ReplayConfig>,
    ) -> Result<Uuid> {
        if self.active_session.is_some() {
            return Err(crate::MemvidError::InvalidQuery {
                reason: "A session is already active. End it before starting a new one.".into(),
            });
        }

        let cfg = config.unwrap_or_default();
        let session = ActiveSession::new(name, cfg);
        let session_id = session.session_id();

        self.active_session = Some(session);

        tracing::info!("Started replay session: {}", session_id);
        Ok(session_id)
    }

    /// End the current recording session.
    ///
    /// Returns the completed session with all recorded actions.
    #[cfg(feature = "replay")]
    pub fn end_session(&mut self) -> Result<ReplaySession> {
        let session =
            self.active_session
                .take()
                .ok_or_else(|| crate::MemvidError::InvalidQuery {
                    reason: "No active session to end".into(),
                })?;

        let completed = session.end();
        let session_id = completed.session_id;

        // Store the session in memory for later persistence
        self.completed_sessions.push(completed.clone());

        tracing::info!("Ended replay session: {}", session_id);
        Ok(completed)
    }

    /// Get the ID of the currently active session, if any.
    #[cfg(feature = "replay")]
    pub fn active_session_id(&self) -> Option<Uuid> {
        self.active_session.as_ref().map(|s| s.session_id())
    }

    /// Check if a recording session is currently active.
    #[cfg(feature = "replay")]
    pub fn is_recording(&self) -> bool {
        self.active_session.is_some()
    }

    /// Create a checkpoint in the current session.
    ///
    /// Checkpoints capture the complete state at a point in time,
    /// enabling replay to start from that point rather than the beginning.
    #[cfg(feature = "replay")]
    pub fn create_checkpoint(&mut self) -> Result<u64> {
        let session =
            self.active_session
                .as_mut()
                .ok_or_else(|| crate::MemvidError::InvalidQuery {
                    reason: "No active session for checkpoint".into(),
                })?;

        // Create a state snapshot
        let snapshot = StateSnapshot {
            frame_count: self.toc.frames.len(),
            frame_ids: self.toc.frames.iter().map(|f| f.id).collect(),
            lex_index_hash: self.toc.indexes.lex.as_ref().map(|m| m.checksum),
            vec_index_hash: self.toc.indexes.vec.as_ref().map(|m| m.checksum),
            wal_sequence: self.header.wal_sequence,
            generation: self.generation,
        };

        let checkpoint = session.create_checkpoint(snapshot);
        let checkpoint_id = checkpoint.id;

        // Record the checkpoint action
        session.record_action(ReplayAction::new(
            session.session.next_sequence(),
            ActionType::Checkpoint {
                checkpoint_id: checkpoint.id,
            },
        ));

        tracing::debug!("Created checkpoint {} in session", checkpoint_id);
        Ok(checkpoint_id)
    }

    /// Record a Put action in the current session.
    #[cfg(feature = "replay")]
    pub fn record_put_action(&mut self, frame_id: u64, input: &[u8]) {
        if let Some(session) = self.active_session.as_mut() {
            let action = ReplayAction::new(
                session.session.next_sequence(),
                ActionType::Put { frame_id },
            )
            .with_input(input)
            .with_affected_frames(vec![frame_id]);

            session.record_action(action);

            // Check if auto-checkpoint is due
            if session.should_checkpoint() {
                let _ = self.create_checkpoint();
            }
        }
    }

    /// Record a Find action in the current session.
    #[cfg(feature = "replay")]
    pub fn record_find_action(
        &mut self,
        query: &str,
        mode: &str,
        result_count: usize,
        result_frames: Vec<u64>,
    ) {
        if let Some(session) = self.active_session.as_mut() {
            let action = ReplayAction::new(
                session.session.next_sequence(),
                ActionType::Find {
                    query: query.to_string(),
                    mode: mode.to_string(),
                    result_count,
                },
            )
            .with_input(query.as_bytes())
            .with_affected_frames(result_frames);

            session.record_action(action);
        }
    }

    /// Record an Ask action in the current session.
    ///
    /// # Arguments
    /// * `query` - The question asked
    /// * `provider` - The LLM provider (e.g., "openai", "anthropic")
    /// * `model` - The model name (e.g., "gpt-4", "claude-3")
    /// * `response` - The raw response bytes
    /// * `duration_ms` - How long the request took
    /// * `retrieved_frames` - Frame IDs that were retrieved as context
    #[cfg(feature = "replay")]
    pub fn record_ask_action(
        &mut self,
        query: &str,
        provider: &str,
        model: &str,
        response: &[u8],
        duration_ms: u64,
        retrieved_frames: Vec<u64>,
    ) {
        if let Some(session) = self.active_session.as_mut() {
            let action = ReplayAction::new(
                session.session.next_sequence(),
                ActionType::Ask {
                    query: query.to_string(),
                    provider: provider.to_string(),
                    model: model.to_string(),
                },
            )
            .with_input(query.as_bytes())
            .with_output(response)
            .with_duration_ms(duration_ms)
            .with_affected_frames(retrieved_frames);

            session.record_action(action);
        }
    }

    /// List all completed sessions (in memory).
    #[cfg(feature = "replay")]
    pub fn list_sessions(&self) -> Vec<SessionSummary> {
        self.completed_sessions
            .iter()
            .map(SessionSummary::from)
            .collect()
    }

    /// Get a completed session by ID.
    #[cfg(feature = "replay")]
    pub fn get_session(&self, session_id: Uuid) -> Option<&ReplaySession> {
        self.completed_sessions
            .iter()
            .find(|s| s.session_id == session_id)
    }

    /// Delete a completed session by ID.
    #[cfg(feature = "replay")]
    pub fn delete_session(&mut self, session_id: Uuid) -> Result<()> {
        let pos = self
            .completed_sessions
            .iter()
            .position(|s| s.session_id == session_id)
            .ok_or_else(|| crate::MemvidError::InvalidQuery {
                reason: format!("Session {} not found", session_id).into(),
            })?;

        self.completed_sessions.remove(pos);
        tracing::info!("Deleted session {}", session_id);
        Ok(())
    }

    /// Save all sessions to the replay segment.
    ///
    /// This persists all completed sessions to the .mv2 file.
    #[cfg(feature = "replay")]
    pub fn save_replay_sessions(&mut self) -> Result<()> {
        use crate::replay::storage;
        use std::io::{Seek, SeekFrom, Write};

        self.ensure_writable()?;

        if self.completed_sessions.is_empty() {
            // Clear the replay manifest when all sessions are deleted
            if self.toc.replay_manifest.is_some() {
                self.toc.replay_manifest = None;
                self.dirty = true;
                tracing::info!("Cleared replay manifest (no sessions remaining)");
            }
            return Ok(());
        }

        // Build the replay segment
        let segment_data = storage::build_segment(&self.completed_sessions)?;
        let segment_size = segment_data.len() as u64;

        // Debug: show first 32 bytes being written
        let preview_len = segment_data.len().min(32);
        let hex_preview: Vec<String> = segment_data[..preview_len]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        tracing::debug!(
            "Writing segment with first {} bytes: {}",
            preview_len,
            hex_preview.join(" ")
        );
        tracing::debug!(
            "Segment magic should be: {:?}",
            String::from_utf8_lossy(&segment_data[..8.min(segment_data.len())])
        );

        // IMPORTANT: The replay segment must never be written at `data_end`.
        //
        // `data_end` can intentionally lag behind embedded index bytes (e.g. Tantivy segments)
        // so that the next commit can overwrite them. Writing replay at `data_end` can therefore
        // overwrite indexes and (worse) move `footer_offset` backwards, corrupting the TOC.
        //
        // Instead, append replay at the current footer boundary, which is always after any
        // embedded index / metadata bytes for the current generation.
        let segment_offset = self.header.footer_offset.max(self.data_end);
        tracing::debug!(
            "Writing replay segment: offset={}, size={}, footer_offset={}, data_end={}",
            segment_offset,
            segment_size,
            self.header.footer_offset,
            self.data_end
        );
        self.file.seek(SeekFrom::Start(segment_offset))?;
        self.file.write_all(&segment_data)?;
        self.file.sync_all()?; // Ensure data is flushed to disk

        // Update the replay manifest in TOC
        self.toc.replay_manifest = Some(ReplayManifest {
            segment_offset,
            segment_size,
            session_count: self.completed_sessions.len() as u32,
            total_actions: self
                .completed_sessions
                .iter()
                .map(|s| s.actions.len() as u64)
                .sum(),
            version: crate::replay::REPLAY_SEGMENT_VERSION,
        });

        // Advance footer_offset so the next TOC write lands after the replay segment.
        // Never move footer_offset backwards.
        let new_end = segment_offset + segment_size;
        self.data_end = self.data_end.max(new_end);
        self.header.footer_offset = self.header.footer_offset.max(new_end);
        self.dirty = true;

        tracing::info!(
            "Saved {} replay sessions ({} bytes) at offset {}",
            self.completed_sessions.len(),
            segment_size,
            segment_offset
        );
        Ok(())
    }

    /// Load replay sessions from the file.
    #[cfg(feature = "replay")]
    pub fn load_replay_sessions(&mut self) -> Result<()> {
        use crate::replay::storage;
        use std::io::{Read, Seek, SeekFrom};

        let manifest = match &self.toc.replay_manifest {
            Some(m) if m.session_count > 0 => {
                tracing::debug!(
                    "Found replay manifest: session_count={}, segment_offset={}, segment_size={}",
                    m.session_count,
                    m.segment_offset,
                    m.segment_size
                );
                m.clone()
            }
            Some(_) => {
                tracing::debug!("Replay manifest has 0 sessions, skipping load");
                return Ok(());
            }
            _ => {
                tracing::debug!("No replay manifest in TOC, skipping load");
                return Ok(());
            }
        };

        // Read the segment data
        tracing::debug!("Allocating buffer of {} bytes", manifest.segment_size);
        let mut buf = vec![0u8; manifest.segment_size as usize];

        tracing::debug!("Seeking to offset {}", manifest.segment_offset);
        self.file
            .seek(SeekFrom::Start(manifest.segment_offset))
            .map_err(|e| {
                tracing::error!(
                    "Failed to seek to replay segment at offset {}: {}",
                    manifest.segment_offset,
                    e
                );
                e
            })?;

        tracing::debug!("Reading {} bytes from file", manifest.segment_size);
        self.file.read_exact(&mut buf).map_err(|e| {
            tracing::error!(
                "Failed to read replay segment ({} bytes): {}",
                manifest.segment_size,
                e
            );
            e
        })?;

        // Debug: show first 32 bytes as hex
        let preview_len = buf.len().min(32);
        let hex_preview: Vec<String> = buf[..preview_len]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        tracing::debug!(
            "First {} bytes of segment: {}",
            preview_len,
            hex_preview.join(" ")
        );

        // Show expected magic vs actual
        let expected_magic = b"MV2RPLY!";
        let actual_magic = &buf[..8.min(buf.len())];
        tracing::debug!(
            "Expected magic: {:?}, Actual magic: {:?} (as string: {:?})",
            expected_magic,
            actual_magic,
            String::from_utf8_lossy(actual_magic)
        );

        // Parse sessions
        tracing::debug!("Parsing replay segment data");
        self.completed_sessions = storage::read_segment(&buf).map_err(|e| {
            tracing::error!("Failed to parse replay segment: {}", e);
            e
        })?;

        tracing::info!(
            "Loaded {} replay sessions from file",
            self.completed_sessions.len()
        );
        Ok(())
    }

    /// Get the path to the active session sidecar file.
    #[cfg(feature = "replay")]
    fn active_session_path(&self) -> std::path::PathBuf {
        let mut path = self.path.clone();
        let mut filename = path.file_name().unwrap_or_default().to_os_string();
        filename.push(".session");
        path.set_file_name(filename);
        path
    }

    /// Save the active session to a sidecar file.
    /// This allows the session to persist across CLI invocations.
    #[cfg(feature = "replay")]
    pub fn save_active_session(&self) -> Result<()> {
        use crate::replay::storage;
        use std::io::Write;

        self.ensure_not_terminal()?;

        let session = match &self.active_session {
            Some(s) => s,
            None => {
                // No active session, remove sidecar file if it exists
                let path = self.active_session_path();
                if path.exists() {
                    let _ = std::fs::remove_file(&path);
                }
                return Ok(());
            }
        };

        let data = storage::serialize_active_session(session)?;
        let path = self.active_session_path();
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&data)?;
        file.sync_all()?;

        tracing::debug!("Saved active session to {:?}", path);
        Ok(())
    }

    /// Load the active session from a sidecar file.
    #[cfg(feature = "replay")]
    pub fn load_active_session(&mut self) -> Result<bool> {
        use crate::replay::storage;

        let path = self.active_session_path();
        if !path.exists() {
            return Ok(false);
        }

        let data = std::fs::read(&path)?;
        match storage::deserialize_active_session(&data) {
            Ok(session) => {
                tracing::info!(
                    "Loaded active session {} from {:?}",
                    session.session_id(),
                    path
                );
                self.active_session = Some(session);
                Ok(true)
            }
            Err(e) => {
                self.ensure_not_terminal()?;
                tracing::warn!("Failed to load active session: {}, removing stale file", e);
                let _ = std::fs::remove_file(&path);
                Ok(false)
            }
        }
    }

    /// Clear the active session sidecar file.
    #[cfg(feature = "replay")]
    pub fn clear_active_session_file(&self) -> Result<()> {
        self.ensure_not_terminal()?;
        let path = self.active_session_path();
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "replay"))]
mod active_session_locking_tests {
    use super::*;
    use crate::lock::LockMode;

    #[test]
    fn terminal_handle_cannot_delete_or_replace_current_active_session() {
        crate::run_serial_test(|| {
            let dir = tempfile::tempdir().expect("temp dir");
            let path = dir.path().join("active-session-terminal.mv2");
            let mut seed = Memvid::create(&path).expect("create");
            seed.put_bytes(b"seed").expect("put seed");
            seed.commit().expect("commit seed");
            drop(seed);

            let mut stale = Memvid::open_read_only(&path).expect("stale candidate");
            let reader = Memvid::open_read_only(&path).expect("competing reader");
            crate::lock::set_lock_max_attempts(Some(0));
            let upgrade = stale.commit();
            crate::lock::set_lock_max_attempts(None);
            upgrade.expect_err("upgrade must fail");
            assert_eq!(stale.lock.mode(), LockMode::None);
            drop(reader);

            let mut current = Memvid::open(&path).expect("current writer");
            let current_session_id = current
                .start_session(Some("current writer".into()), None)
                .expect("start current session");
            current.save_active_session().expect("save current session");
            let sidecar = current.active_session_path();
            let expected = std::fs::read(&sidecar).expect("saved session bytes");

            assert!(stale.clear_active_session_file().is_err());
            assert_eq!(std::fs::read(&sidecar).unwrap(), expected);
            assert!(
                stale.save_active_session().is_err(),
                "None branch is guarded"
            );
            assert_eq!(std::fs::read(&sidecar).unwrap(), expected);
            stale
                .start_session(Some("stale writer".into()), None)
                .unwrap();
            assert!(
                stale.save_active_session().is_err(),
                "Some branch is guarded"
            );
            assert_eq!(std::fs::read(&sidecar).unwrap(), expected);

            assert!(
                current
                    .load_active_session()
                    .expect("reload current session")
            );
            assert_eq!(current.active_session_id(), Some(current_session_id));

            std::fs::write(&sidecar, b"invalid active session").expect("inject invalid sidecar");
            let invalid = std::fs::read(&sidecar).unwrap();
            assert!(
                stale.load_active_session().is_err(),
                "terminal handle must reject the decode-error deletion branch"
            );
            assert_eq!(std::fs::read(&sidecar).unwrap(), invalid);

            current
                .save_active_session()
                .expect("current writer restores session");
            assert!(
                current
                    .load_active_session()
                    .expect("current writer reloads")
            );
            assert_eq!(current.active_session_id(), Some(current_session_id));
        });
    }
}
