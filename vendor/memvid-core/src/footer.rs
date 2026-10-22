use std::convert::TryInto;

use blake3::Hasher;
use memchr::memrchr;

/// Magic trailer marker appended to every committed TOC.
pub const FOOTER_MAGIC: &[u8; 8] = b"MV2FOOT!";

/// Total size of a commit footer in bytes.
pub const FOOTER_SIZE: usize = FOOTER_MAGIC.len() + 8 + 32 + 8;

/// Parsed representation of the footer trailer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitFooter {
    pub toc_len: u64,
    pub toc_hash: [u8; 32],
    pub generation: u64,
}

impl CommitFooter {
    /// Serialises the footer into a fixed-size byte array.
    #[must_use]
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[..FOOTER_MAGIC.len()].copy_from_slice(FOOTER_MAGIC);
        buf[FOOTER_MAGIC.len()..FOOTER_MAGIC.len() + 8]
            .copy_from_slice(&self.toc_len.to_le_bytes());
        buf[FOOTER_MAGIC.len() + 8..FOOTER_MAGIC.len() + 40].copy_from_slice(&self.toc_hash);
        buf[FOOTER_MAGIC.len() + 40..].copy_from_slice(&self.generation.to_le_bytes());
        buf
    }

    /// Attempts to decode a footer from a byte slice.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != FOOTER_SIZE {
            return None;
        }
        if &bytes[..FOOTER_MAGIC.len()] != FOOTER_MAGIC {
            return None;
        }
        let toc_len = u64::from_le_bytes(
            bytes[FOOTER_MAGIC.len()..FOOTER_MAGIC.len() + 8]
                .try_into()
                .ok()?,
        );
        let mut toc_hash = [0u8; 32];
        toc_hash.copy_from_slice(&bytes[FOOTER_MAGIC.len() + 8..FOOTER_MAGIC.len() + 40]);
        let generation = u64::from_le_bytes(bytes[FOOTER_MAGIC.len() + 40..].try_into().ok()?);
        Some(Self {
            toc_len,
            toc_hash,
            generation,
        })
    }

    #[must_use]
    pub fn hash_matches(&self, toc_bytes: &[u8]) -> bool {
        let mut hasher = Hasher::new();
        hasher.update(toc_bytes);
        hasher.finalize().as_bytes() == &self.toc_hash
    }
}

/// Result of scanning a file for the last valid commit footer.
#[derive(Debug)]
pub struct FooterSlice<'a> {
    pub footer_offset: usize,
    pub toc_offset: usize,
    pub footer: CommitFooter,
    pub toc_bytes: &'a [u8],
}

/// Scan the provided bytes backwards to locate the most recent valid footer.
#[must_use]
pub fn find_last_valid_footer(bytes: &[u8]) -> Option<FooterSlice<'_>> {
    match find_last_valid_footer_with_charge(bytes, |_| Ok::<(), std::convert::Infallible>(())) {
        Ok(found) => found,
        Err(never) => match never {},
    }
}

/// Recovery-only variant which charges every byte submitted to BLAKE3 before hashing it.
///
/// The public footer API remains unbounded for compatibility; recovery supplies one shared
/// budget covering footer search and its later fallback phases.
pub(crate) fn find_last_valid_footer_with_charge<'a, F, E>(
    bytes: &'a [u8],
    mut charge_hash: F,
) -> std::result::Result<Option<FooterSlice<'a>>, E>
where
    F: FnMut(usize) -> std::result::Result<(), E>,
{
    if bytes.len() < FOOTER_SIZE {
        return Ok(None);
    }

    let total_len = bytes.len();
    let mut search_end = bytes.len();
    while let Some(pos) = memrchr(FOOTER_MAGIC[0], &bytes[..search_end]) {
        if pos + FOOTER_SIZE > total_len {
            if pos == 0 {
                break;
            }
            search_end = pos;
            continue;
        }
        let candidate = &bytes[pos..pos + FOOTER_SIZE];
        if let Some(footer) = CommitFooter::decode(candidate) {
            let toc_end = pos;
            let toc_len = usize::try_from(footer.toc_len).unwrap_or(0);
            if toc_len == 0 || toc_len > toc_end {
                search_end = pos;
                continue;
            }
            let toc_offset = toc_end - toc_len;
            let toc_bytes = &bytes[toc_offset..toc_end];
            charge_hash(toc_bytes.len())?;
            if !footer.hash_matches(toc_bytes) {
                search_end = pos;
                continue;
            }
            return Ok(Some(FooterSlice {
                footer_offset: pos,
                toc_offset,
                footer,
                toc_bytes,
            }));
        }
        if pos == 0 {
            break;
        }
        search_end = pos;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_sample_bytes(generation: u64, toc: &[u8]) -> Vec<u8> {
        let mut footer_hash = Hasher::new();
        footer_hash.update(toc);
        let mut buffer = Vec::new();
        buffer.extend_from_slice(toc);
        let footer = CommitFooter {
            toc_len: toc.len() as u64,
            toc_hash: *footer_hash.finalize().as_bytes(),
            generation,
        };
        buffer.extend_from_slice(&footer.encode());
        buffer
    }

    #[test]
    fn encode_decode_roundtrip() {
        let footer = CommitFooter {
            toc_len: 123,
            toc_hash: [0xAB; 32],
            generation: 99,
        };
        let encoded = footer.encode();
        let decoded = CommitFooter::decode(&encoded).expect("decode");
        assert_eq!(footer, decoded);
    }

    #[test]
    fn scan_finds_footer() {
        let toc = vec![0xAA, 0xBB, 0xCC];
        let bytes = build_sample_bytes(7, &toc);
        let slice = find_last_valid_footer(&bytes).expect("footer present");
        assert_eq!(slice.footer.generation, 7);
        assert_eq!(slice.toc_bytes, toc);
        assert_eq!(
            &bytes[slice.footer_offset..slice.footer_offset + FOOTER_SIZE],
            &slice.footer.encode()
        );
    }

    #[test]
    fn scan_skips_corrupt_footer() {
        let toc = vec![1u8, 2, 3, 4];
        let mut bytes = build_sample_bytes(1, &toc);
        // Corrupt the hash of the first footer.
        let idx = bytes.len() - FOOTER_SIZE + FOOTER_MAGIC.len() + 12;
        bytes[idx] ^= 0xFF;
        // Append a valid second footer.
        let mut extra_toc = vec![9u8; 10];
        extra_toc.push(42);
        let appended = build_sample_bytes(2, &extra_toc);
        bytes.extend_from_slice(&appended);
        let slice = find_last_valid_footer(&bytes).expect("footer present");
        assert_eq!(slice.footer.generation, 2);
        assert_eq!(slice.toc_bytes, &extra_toc);
    }
}
