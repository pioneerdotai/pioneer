//! Physical encoding of portable UTF-8 payloads.

pub mod json_projection;
pub mod storage;

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};

pub const BLOCK_BYTES: usize = 64 * 1024;
const ZSTD_LEVEL: i32 = 3;

// PostgreSQL keeps these discriminants in a separate SMALLINT column. Raw
// bytes are written unchanged. A Zstd body is a single dictionary-free frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i16)]
pub enum Encoding {
    Raw = 0,
    Zstd = 1,
}

impl TryFrom<i16> for Encoding {
    type Error = anyhow::Error;

    fn try_from(value: i16) -> Result<Self> {
        match value {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Zstd),
            _ => bail!("unknown portable payload encoding"),
        }
    }
}

#[derive(Clone)]
pub struct Frame {
    pub encoding: Encoding,
    pub bytes: Vec<u8>,
    pub original_bytes: usize,
    pub original_chars: usize,
    pub sha256: [u8; 32],
}

impl Frame {
    pub fn encode(text: &str) -> Result<Self> {
        let original = text.as_bytes();
        let compressed =
            zstd::bulk::compress(original, ZSTD_LEVEL).context("compress portable payload")?;
        // Reserve the codec field's two service bytes when testing the gain.
        let (encoding, bytes) = if compressed.len().saturating_add(2) < original.len() {
            (Encoding::Zstd, compressed)
        } else {
            (Encoding::Raw, original.to_vec())
        };
        Ok(Self {
            encoding,
            bytes,
            original_bytes: original.len(),
            original_chars: text.chars().count(),
            sha256: Sha256::digest(original).into(),
        })
    }

    /// Decode a frame using lengths from its persisted row. The caller sets
    /// the family limit before allocating output.
    pub fn decode(self, max_bytes: usize) -> Result<String> {
        ensure!(
            self.original_bytes <= max_bytes,
            "portable payload exceeds its byte limit"
        );
        let bytes = match self.encoding {
            Encoding::Raw => self.bytes,
            Encoding::Zstd => {
                ensure!(
                    self.bytes.len().saturating_add(2) < self.original_bytes,
                    "portable payload frame does not fit its original length"
                );
                ensure!(
                    self.bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]),
                    "invalid portable payload Zstd frame"
                );
                let size = zstd::zstd_safe::find_frame_compressed_size(&self.bytes)
                    .map_err(|_| anyhow::anyhow!("invalid portable payload Zstd frame"))?;
                ensure!(
                    size == self.bytes.len(),
                    "portable payload has trailing frame data"
                );
                let declared_size = zstd::zstd_safe::get_frame_content_size(&self.bytes)
                    .map_err(|_| anyhow::anyhow!("invalid portable payload frame length"))?;
                let expected_size = u64::try_from(self.original_bytes)?;
                ensure!(
                    declared_size == Some(expected_size),
                    "portable payload frame length mismatch"
                );
                ensure!(
                    zstd::zstd_safe::get_dict_id_from_frame(&self.bytes).is_none(),
                    "portable payload frame requires a dictionary"
                );
                // A single-segment window already equals the declared length.
                // Otherwise the window descriptor can request a larger window.
                if self.bytes[4] & 0x20 == 0 {
                    let descriptor = *self.bytes.get(5).context("missing Zstd window")?;
                    let base = 1_u64 << (10 + (descriptor >> 3));
                    let window = base + (base / 8) * u64::from(descriptor & 7);
                    ensure!(
                        window <= expected_size,
                        "portable payload Zstd window exceeds its original length"
                    );
                }
                zstd::bulk::decompress(&self.bytes, self.original_bytes)
                    .context("decompress portable payload")?
            }
        };
        ensure!(
            bytes.len() == self.original_bytes,
            "portable payload byte length mismatch"
        );
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        ensure!(actual == self.sha256, "portable payload checksum mismatch");
        let text = String::from_utf8(bytes).context("invalid UTF-8 in portable payload")?;
        ensure!(
            text.chars().count() == self.original_chars,
            "portable payload character length mismatch"
        );
        Ok(text)
    }
}

pub struct Block {
    pub byte_offset: usize,
    pub char_offset: usize,
    pub frame: Frame,
}

pub enum BlockPayload {
    Single(Frame),
    Split {
        original_bytes: usize,
        original_chars: usize,
        sha256: [u8; 32],
        blocks: Vec<Block>,
    },
}

impl BlockPayload {
    /// Prepare storage for a field that supports indexed range reads. Every
    /// block starts and ends at a UTF-8 boundary.
    pub fn encode(text: &str) -> Result<Self> {
        if text.len() <= BLOCK_BYTES {
            return Frame::encode(text).map(Self::Single);
        }
        let mut blocks = Vec::new();
        let mut byte_offset = 0;
        let mut char_offset = 0;
        while byte_offset < text.len() {
            let mut end = byte_offset.saturating_add(BLOCK_BYTES).min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            let frame = Frame::encode(&text[byte_offset..end])?;
            let chars = frame.original_chars;
            blocks.push(Block {
                byte_offset,
                char_offset,
                frame,
            });
            byte_offset = end;
            char_offset += chars;
        }
        Ok(Self::Split {
            original_bytes: text.len(),
            original_chars: char_offset,
            sha256: Sha256::digest(text.as_bytes()).into(),
            blocks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whole_value_is_byte_exact_beyond_128_kib() -> Result<()> {
        let json = format!(r#"{{ "value": "{}\u0000" }}"#, "🧪".repeat(38_000));
        let frame = Frame::encode(&json)?;
        assert_eq!(frame.encoding, Encoding::Zstd);
        assert_eq!(frame.original_bytes, json.len());
        assert_eq!(frame.decode(json.len())?.as_bytes(), json.as_bytes());
        Ok(())
    }

    #[test]
    fn raw_preserves_empty_and_short_values() -> Result<()> {
        for text in ["", "🧪", "raw\0value"] {
            let frame = Frame::encode(text)?;
            assert_eq!(frame.encoding, Encoding::Raw);
            assert_eq!(frame.bytes, text.as_bytes());
            assert_eq!(frame.decode(text.len())?, text);
        }
        Ok(())
    }

    #[test]
    fn split_starts_only_above_64_kib_and_checks_each_utf8_block() -> Result<()> {
        let boundary = "a".repeat(BLOCK_BYTES);
        assert!(matches!(
            BlockPayload::encode(&boundary)?,
            BlockPayload::Single(_)
        ));
        let text = format!("{}🧪ё", &boundary[..BLOCK_BYTES - 1]);
        let BlockPayload::Split {
            original_bytes,
            original_chars,
            sha256,
            mut blocks,
        } = BlockPayload::encode(&text)?
        else {
            panic!("expected separate UTF-8 blocks");
        };
        assert_eq!(original_bytes, text.len());
        assert_eq!(original_chars, text.chars().count());
        assert_eq!(
            sha256.as_slice(),
            Sha256::digest(text.as_bytes()).as_slice()
        );
        assert_eq!(blocks.len(), 2);
        let tail = blocks.pop().unwrap();
        assert_eq!(tail.byte_offset, BLOCK_BYTES - 1);
        assert_eq!(tail.char_offset, BLOCK_BYTES - 1);
        assert_eq!(tail.frame.decode(BLOCK_BYTES)?, "🧪ё");
        let mut head = blocks.pop().unwrap().frame;
        head.sha256[0] ^= 1;
        assert!(head.decode(BLOCK_BYTES).is_err());
        Ok(())
    }

    #[test]
    fn codec_rejects_trailing_frame_and_too_small_limit() -> Result<()> {
        let text = "compressible".repeat(1000);
        assert!(Frame::encode(&text)?.decode(text.len() - 1).is_err());
        let mut frame = Frame::encode(&text)?;
        frame.original_bytes += 1;
        assert!(frame.clone().decode(text.len() + 1).is_err());
        frame.original_bytes -= 1;
        frame
            .bytes
            .extend(zstd::bulk::compress(b"trailing", ZSTD_LEVEL)?);
        assert!(frame.decode(text.len()).is_err());
        Ok(())
    }
}

pub mod json_fields;
