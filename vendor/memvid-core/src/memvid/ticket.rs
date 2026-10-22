use crate::constants::MEMVID_TICKET_PUBKEY;
use crate::error::{MemvidError, Result};
use crate::memvid::lifecycle::Memvid;
use crate::signature::{parse_ed25519_public_key_base64, verify_ticket_signature};
use crate::types::{FrameStatus, SignedTicket, Stats, Ticket, TicketRef};

impl Memvid {
    /// Committed capsule generation used to reject stale search continuations.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn stats(&self) -> Result<Stats> {
        let metadata = self.file.metadata()?;
        let mut payload_bytes = 0u64;
        let mut logical_bytes = 0u64;
        let mut active_frames = 0u64;

        for frame in self
            .toc
            .frames
            .iter()
            .filter(|frame| frame.status == FrameStatus::Active)
        {
            active_frames = active_frames.saturating_add(1);
            let stored = frame.payload_length;
            payload_bytes = payload_bytes.saturating_add(stored);
            if stored > 0 {
                let logical = frame.canonical_length.unwrap_or(stored);
                logical_bytes = logical_bytes.saturating_add(logical);
            }
        }

        let saved_bytes = logical_bytes.saturating_sub(payload_bytes);
        let round2 = |value: f64| (value * 100.0).round() / 100.0;
        let compression_ratio_percent = if logical_bytes > 0 {
            round2((payload_bytes as f64 / logical_bytes as f64) * 100.0)
        } else {
            100.0
        };
        let savings_percent = if logical_bytes > 0 {
            round2((saved_bytes as f64 / logical_bytes as f64) * 100.0)
        } else {
            0.0
        };
        let storage_utilisation_percent = if self.capacity_limit() > 0 {
            round2((metadata.len() as f64 / self.capacity_limit() as f64) * 100.0)
        } else {
            0.0
        };
        let remaining_capacity_bytes = self.capacity_limit().saturating_sub(metadata.len());
        let average_payload = if active_frames > 0 {
            payload_bytes / active_frames
        } else {
            0
        };
        let average_logical = if active_frames > 0 {
            logical_bytes / active_frames
        } else {
            0
        };

        // PHASE 2: Calculate detailed overhead breakdown for observability
        let wal_bytes = self.header.wal_size;

        let mut lex_index_bytes = 0u64;
        if let Some(ref lex) = self.toc.indexes.lex {
            lex_index_bytes = lex_index_bytes.saturating_add(lex.bytes_length);
        }
        for seg in &self.toc.indexes.lex_segments {
            lex_index_bytes = lex_index_bytes.saturating_add(seg.bytes_length);
        }

        let mut vec_index_bytes = 0u64;
        let mut vector_count = 0u64;
        if let Some(ref vec) = self.toc.indexes.vec {
            vec_index_bytes = vec_index_bytes.saturating_add(vec.bytes_length);
            vector_count = vector_count.saturating_add(vec.vector_count);
        }
        for seg in &self.toc.segment_catalog.vec_segments {
            vec_index_bytes = vec_index_bytes.saturating_add(seg.common.bytes_length);
            vector_count = vector_count.saturating_add(seg.vector_count);
        }

        let mut time_index_bytes = 0u64;
        if let Some(ref time) = self.toc.time_index {
            time_index_bytes = time_index_bytes.saturating_add(time.bytes_length);
        }
        for seg in &self.toc.segment_catalog.time_segments {
            time_index_bytes = time_index_bytes.saturating_add(seg.common.bytes_length);
        }

        // CLIP image count from clip index manifest
        let clip_image_count = self.toc.indexes.clip.as_ref().map_or(0, |c| c.vector_count);

        Ok(Stats {
            frame_count: self.toc.frames.len() as u64,
            size_bytes: metadata.len(),
            tier: self.tier(),
            // Use consolidated helper for consistent lex index detection
            has_lex_index: crate::memvid::lifecycle::has_lex_index(&self.toc),
            has_vec_index: self.toc.indexes.vec.is_some()
                || !self.toc.segment_catalog.vec_segments.is_empty(),
            has_clip_index: self.toc.indexes.clip.is_some(),
            has_time_index: self.toc.time_index.is_some()
                || !self.toc.segment_catalog.time_segments.is_empty(),
            seq_no: (self.toc.ticket_ref.seq_no != 0).then_some(self.toc.ticket_ref.seq_no),
            capacity_bytes: self.capacity_limit(),
            active_frame_count: active_frames,
            payload_bytes,
            logical_bytes,
            saved_bytes,
            compression_ratio_percent,
            savings_percent,
            storage_utilisation_percent,
            remaining_capacity_bytes,
            average_frame_payload_bytes: average_payload,
            average_frame_logical_bytes: average_logical,
            wal_bytes,
            lex_index_bytes,
            vec_index_bytes,
            time_index_bytes,
            vector_count,
            clip_image_count,
            lex_enabled: self.lex_enabled,
            vec_enabled: self.vec_enabled,
        })
    }

    /// Applies an unsigned ticket to this memory.
    ///
    /// # Deprecation
    /// This method is deprecated and will be removed in a future release.
    /// Use [`apply_signed_ticket`](Self::apply_signed_ticket) instead, which
    /// verifies the ticket signature against the Memvid public key.
    #[deprecated(
        since = "0.3.0",
        note = "Use apply_signed_ticket() for cryptographically verified tickets"
    )]
    pub fn apply_ticket(&mut self, ticket: Ticket) -> Result<()> {
        self.ensure_writable()?;
        let current_seq = self.toc.ticket_ref.seq_no;
        if ticket.seq_no <= current_seq {
            return Err(MemvidError::TicketSequence {
                expected: current_seq + 1,
                actual: ticket.seq_no,
            });
        }

        self.toc.ticket_ref.capacity_bytes = ticket.capacity_bytes.unwrap_or(0);
        self.toc.ticket_ref.issuer = ticket.issuer;
        self.toc.ticket_ref.seq_no = ticket.seq_no;
        self.toc.ticket_ref.expires_in_secs = ticket.expires_in_secs;
        self.toc.ticket_ref.verified = false; // Unsigned tickets are not verified

        self.generation = self.generation.wrapping_add(1);
        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Applies a cryptographically signed ticket to this memory.
    ///
    /// This method verifies the ticket signature against the embedded Memvid
    /// public key before applying. Only tickets signed by the official Memvid
    /// control plane will be accepted.
    ///
    /// # Arguments
    /// * `ticket` - A signed ticket obtained from the Memvid API
    ///
    /// # Errors
    /// Returns an error if:
    /// - The signature verification fails
    /// - The memory ID doesn't match this memory
    /// - The sequence number is not greater than the current one
    ///
    /// # Example
    /// ```ignore
    /// use memvid_core::types::SignedTicket;
    ///
    /// let ticket = SignedTicket::new(
    ///     "memvid.com",
    ///     1,
    ///     86400,
    ///     Some(100 * 1024 * 1024),
    ///     memory_id,
    ///     signature_bytes,
    /// );
    /// memory.apply_signed_ticket(ticket)?;
    /// ```
    pub fn apply_signed_ticket(&mut self, ticket: SignedTicket) -> Result<()> {
        self.ensure_writable()?;

        // 1. Parse the embedded public key
        let verifying_key = parse_ed25519_public_key_base64(MEMVID_TICKET_PUBKEY)?;

        // 2. Verify the memory is bound and get its memory_id
        let binding = self.toc.memory_binding.as_ref().ok_or_else(|| {
            MemvidError::TicketSignatureInvalid {
                reason: "cannot apply signed ticket: memory is not bound to the Memvid API".into(),
            }
        })?;

        // 3. Verify the memory ID matches
        if ticket.memory_id != binding.memory_id {
            return Err(MemvidError::TicketSignatureInvalid {
                reason: format!(
                    "ticket memory_id {} does not match this memory {}",
                    ticket.memory_id, binding.memory_id
                )
                .into_boxed_str(),
            });
        }

        // 4. Verify the signature
        verify_ticket_signature(
            &verifying_key,
            &ticket.memory_id,
            &ticket.issuer,
            ticket.seq_no,
            ticket.expires_in_secs,
            ticket.capacity_bytes,
            &ticket.signature,
        )?;

        // 5. Check sequence number (replay protection)
        let current_seq = self.toc.ticket_ref.seq_no;
        if ticket.seq_no <= current_seq {
            return Err(MemvidError::TicketSequence {
                expected: current_seq + 1,
                actual: ticket.seq_no,
            });
        }

        // 6. Apply the verified ticket
        self.toc.ticket_ref.capacity_bytes = ticket.capacity_bytes.unwrap_or(0);
        self.toc.ticket_ref.issuer = ticket.issuer;
        self.toc.ticket_ref.seq_no = ticket.seq_no;
        self.toc.ticket_ref.expires_in_secs = ticket.expires_in_secs;
        self.toc.ticket_ref.verified = true; // Mark as cryptographically verified

        self.generation = self.generation.wrapping_add(1);
        self.rewrite_toc_footer()?;
        self.header.toc_checksum = self.toc.toc_checksum;
        crate::persist_header(&mut self.file, &self.header)?;
        self.file.sync_all()?;
        Ok(())
    }

    #[must_use]
    pub fn current_ticket(&self) -> TicketRef {
        self.toc.ticket_ref.clone()
    }

    /// Returns a reference to the Logic-Mesh manifest, if present.
    #[must_use]
    pub fn logic_mesh_manifest(&self) -> Option<&crate::types::LogicMeshManifest> {
        self.toc.logic_mesh.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pubkey_parses() {
        // Ensure the embedded public key is valid base64 and parses correctly
        let result = parse_ed25519_public_key_base64(MEMVID_TICKET_PUBKEY);
        assert!(result.is_ok(), "Failed to parse embedded public key");
    }

    #[test]
    fn test_signed_ticket_struct() {
        let memory_id = uuid::Uuid::new_v4();
        let signature = vec![0u8; 64];

        let ticket = SignedTicket::new(
            "memvid.com",
            1,
            86400,
            Some(100 * 1024 * 1024),
            memory_id,
            signature.clone(),
        );

        assert_eq!(ticket.issuer, "memvid.com");
        assert_eq!(ticket.seq_no, 1);
        assert_eq!(ticket.expires_in_secs, 86400);
        assert_eq!(ticket.capacity_bytes, Some(100 * 1024 * 1024));
        assert_eq!(ticket.memory_id, memory_id);
        assert_eq!(ticket.signature, signature);
    }

    #[test]
    fn test_signed_ticket_serialization() {
        let memory_id = uuid::Uuid::nil();
        let signature = vec![1u8; 64];

        let ticket = SignedTicket::new("test", 5, 3600, None, memory_id, signature);

        // Should serialize to JSON without errors
        let json = serde_json::to_string(&ticket).expect("serialization failed");
        assert!(json.contains("\"issuer\":\"test\""));
        assert!(json.contains("\"seq_no\":5"));

        // Should deserialize back
        let parsed: SignedTicket = serde_json::from_str(&json).expect("deserialization failed");
        assert_eq!(parsed.issuer, ticket.issuer);
        assert_eq!(parsed.seq_no, ticket.seq_no);
        assert_eq!(parsed.memory_id, ticket.memory_id);
    }

    #[test]
    fn test_ticket_ref_verified_default() {
        // New TicketRef should default verified to false
        let ticket_ref: TicketRef = serde_json::from_str(
            r#"{"issuer":"test","seq_no":1,"expires_in_secs":0,"capacity_bytes":0}"#,
        )
        .expect("deserialization failed");

        assert!(!ticket_ref.verified, "verified should default to false");
    }

    #[test]
    fn test_invalid_signature_rejected() {
        // Create a ticket with an invalid signature (all zeros)
        let memory_id = uuid::Uuid::new_v4();
        let invalid_signature = vec![0u8; 64];

        let ticket = SignedTicket::new(
            "memvid.com",
            1,
            86400,
            Some(100 * 1024 * 1024),
            memory_id,
            invalid_signature,
        );

        // Verify the signature fails
        let verifying_key = parse_ed25519_public_key_base64(MEMVID_TICKET_PUBKEY).unwrap();
        let result = verify_ticket_signature(
            &verifying_key,
            &ticket.memory_id,
            &ticket.issuer,
            ticket.seq_no,
            ticket.expires_in_secs,
            ticket.capacity_bytes,
            &ticket.signature,
        );

        assert!(result.is_err(), "Invalid signature should be rejected");
        if let Err(MemvidError::TicketSignatureInvalid { reason }) = result {
            assert!(
                reason.contains("signature"),
                "Error should mention signature"
            );
        }
    }
}
