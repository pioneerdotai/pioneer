//! Portable byte-block storage, independent of database adapters.
//!
//! Version 1 is exactly: `PPAY` (4 bytes), version `1` (u8), codec (u8:
//! `0` = raw, `1` = Zstd encoded at level 6), original length (u64 little
//! endian), dictionary SHA-256 (32 bytes; all zero for raw), original-byte
//! SHA-256 (32 bytes), then the body. There are no native-size integers,
//! implicit dictionary IDs, SQLite metadata, or string-prefix heuristics.
//! Zstd bodies contain one ordinary frame, including magic and checksum.
//! The decoder also accepts ordinary frames without an inner checksum because
//! the envelope always verifies the original-byte SHA-256.
//!
//! An envelope represents a present byte slice, including an empty one. NULL
//! and legacy/plain storage are adapter states outside this format: adapters
//! must select the codec using explicit storage metadata, never inspect text
//! to guess whether it is an envelope. Raw is a completed encoding outcome.
//! Callers split larger values into blocks and publish dictionaries before
//! references. They must release database capacity before calling this module.

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    fmt,
    io::Read,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

pub const MAX_BLOCK_BYTES: usize = 65_536;
pub const MAX_DICTIONARY_BYTES: usize = 32_768;
const MAX_TRAINING_BYTES: usize = 1024 * 1024;
const MAX_TRAINING_SAMPLES: usize = 1024;
const LEVEL: i32 = 6;
const WINDOW_LOG: u32 = 16;
const CACHE_ENTRIES: usize = 16;
const CACHE_BYTES: usize = 16 * 1024 * 1024;
const MAGIC: &[u8; 4] = b"PPAY";
const VERSION: u8 = 1;
const RAW: u8 = 0;
const ZSTD: u8 = 1;
const HEADER_BYTES: usize = 78;
// A little above compressBound(64 KiB), including frame overhead. This is a
// storage-format bound, not a request for an allocation from untrusted input.
const MAX_BODY_BYTES: usize = MAX_BLOCK_BYTES + 1024;
const ZERO_DIGEST: [u8; 32] = [0; 32];

/// Optional adapter notification at actual native CPU entry points. This
/// byte codec does not know telemetry, database owners or scheduling classes.
#[derive(Clone, Copy)]
pub(crate) enum CodecStage {
    Train,
    Prepare,
    Compress,
    Decompress,
}

/// Caller-owned, fixed-size observations. All durations are elapsed wall time,
/// not measured CPU. Phases do not overlap each other, but are nested within
/// the caller's encode/decode/train/from-bytes duration. No content is retained.
/// Unobserved public entry points do not read the clock.
#[derive(Default)]
pub(crate) struct CodecDiagnostics {
    pub(crate) on_stage: Option<fn(CodecStage)>,
    /// Both cache critical sections, including mutex wait and byte comparison.
    pub(crate) lookup_elapsed: Duration,
    /// Native encoder/decoder dictionary construction on a cache miss.
    pub(crate) prepare_elapsed: Duration,
    /// Actual native training attempts, including unsuccessful fallback attempts.
    pub(crate) train_elapsed: Duration,
    /// Compressor creation/configuration, compression and compressor destruction.
    pub(crate) compress_elapsed: Duration,
    /// Decoder creation/configuration, bounded decode/frame check and destruction.
    pub(crate) decompress_elapsed: Duration,
    /// Input/header/hash validation, raw copy, envelope and training sample assembly.
    /// Adapter UTF-8/assembly is outside this module and must be measured there.
    pub(crate) verify_elapsed: Duration,
    /// One initial cache probe per request; the insertion recheck is not another hit/miss.
    /// A probe returning an error (poison/collision) has elapsed time but no hit/miss.
    pub(crate) cache_hits: u64,
    pub(crate) cache_misses: u64,
    /// Attempts, incremented before native work; failure is not a successful prepare/train.
    pub(crate) preparations: u64,
    pub(crate) trainings: u64,
    /// Native training errors handled by the existing raw-content fallback.
    pub(crate) training_fallbacks: u64,
}

/// Keeps partial elapsed time on early Result return and unwind. No clock read
/// or diagnostic state is needed for existing, unobserved codec consumers.
struct Elapsed<'a> {
    started: Option<Instant>,
    elapsed: Option<&'a mut Duration>,
}
impl<'a> Elapsed<'a> {
    fn new(elapsed: Option<&'a mut Duration>) -> Self {
        Self {
            started: elapsed.as_ref().map(|_| Instant::now()),
            elapsed,
        }
    }
}
impl Drop for Elapsed<'_> {
    fn drop(&mut self) {
        if let (Some(started), Some(elapsed)) = (self.started, self.elapsed.as_deref_mut()) {
            *elapsed = elapsed.saturating_add(started.elapsed());
        }
    }
}

/// Immutable dictionary content; identity is its actual bytes, not a DB ID.
#[derive(Clone)]
pub struct Dictionary {
    digest: [u8; 32],
    bytes: Arc<[u8]>,
}

impl fmt::Debug for Dictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A Debug dump must never disclose dictionary/sample contents.
        f.debug_struct("Dictionary")
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl Dictionary {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Self::from_bytes_impl(bytes, None)
    }

    pub(crate) fn from_bytes_observed(
        bytes: Vec<u8>,
        diagnostics: &mut CodecDiagnostics,
    ) -> Result<Self> {
        Self::from_bytes_impl(bytes, Some(diagnostics))
    }

    fn from_bytes_impl(
        bytes: Vec<u8>,
        mut diagnostics: Option<&mut CodecDiagnostics>,
    ) -> Result<Self> {
        let dictionary = {
            let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
            ensure!(
                !bytes.is_empty() && bytes.len() <= MAX_DICTIONARY_BYTES,
                "portable payload dictionary size is invalid"
            );
            let digest = Sha256::digest(&bytes).into();
            ensure!(digest != ZERO_DIGEST, "reserved dictionary digest");
            Self {
                digest,
                bytes: bytes.into(),
            }
        };
        // Reject malformed trained dictionaries here, before an adapter can
        // publish a reference to unusable bytes. Raw-content dictionaries are
        // supported by Zstd as well. Preparation is shared with encode/decode.
        prepared_dictionary(&dictionary, diagnostics)?;
        Ok(dictionary)
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

struct PreparedDictionary {
    digest: [u8; 32],
    level: i32,
    bytes: Arc<[u8]>,
    encoder: zstd::dict::EncoderDictionary<'static>,
    decoder: zstd::dict::DecoderDictionary<'static>,
    memory: usize,
}

type DictionaryCache = VecDeque<Arc<PreparedDictionary>>;
static CACHE: OnceLock<Mutex<DictionaryCache>> = OnceLock::new();

fn cached_dictionary(
    cache: &mut DictionaryCache,
    dictionary: &Dictionary,
) -> Result<Option<Arc<PreparedDictionary>>> {
    let Some(index) = cache
        .iter()
        .position(|entry| entry.digest == dictionary.digest && entry.level == LEVEL)
    else {
        return Ok(None);
    };
    // SHA-256 is the persistent identity, but do not silently reuse a prepared
    // dictionary even in the event of a content-digest collision.
    ensure!(
        cache[index].bytes.as_ref() == dictionary.bytes(),
        "portable payload dictionary digest collision"
    );
    let entry = cache.remove(index).expect("located cache entry");
    cache.push_back(entry.clone());
    Ok(Some(entry))
}

fn prepared_dictionary(
    dictionary: &Dictionary,
    mut diagnostics: Option<&mut CodecDiagnostics>,
) -> Result<Arc<PreparedDictionary>> {
    let cache = CACHE.get_or_init(|| Mutex::new(VecDeque::new()));
    let cached = {
        let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.lookup_elapsed));
        let mut entries = cache
            .lock()
            .map_err(|_| anyhow::anyhow!("portable payload dictionary cache unavailable"))?;
        cached_dictionary(&mut entries, dictionary)?
    };
    if let Some(diagnostics) = diagnostics.as_deref_mut() {
        if cached.is_some() {
            diagnostics.cache_hits = diagnostics.cache_hits.saturating_add(1);
        } else {
            diagnostics.cache_misses = diagnostics.cache_misses.saturating_add(1);
        }
    }
    if let Some(prepared) = cached {
        return Ok(prepared);
    }
    // Native preparation and allocation occur without holding the cache lock.
    // Concurrent misses can prepare the same key, then converge below.
    if let Some(diagnostics) = diagnostics.as_deref_mut() {
        diagnostics.preparations = diagnostics.preparations.saturating_add(1);
    }
    if let Some(on_stage) = diagnostics.as_ref().and_then(|d| d.on_stage) {
        on_stage(CodecStage::Prepare);
    }
    let (prepared, memory) = {
        let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.prepare_elapsed));
        let encoder = zstd::dict::EncoderDictionary::try_copy(dictionary.bytes(), LEVEL)
            .context("invalid portable payload encoder dictionary")?;
        let decoder = zstd::dict::DecoderDictionary::try_copy(dictionary.bytes())
            .context("invalid portable payload decoder dictionary")?;
        let memory =
            encoder.as_cdict().sizeof() + decoder.as_ddict().sizeof() + dictionary.bytes.len();
        let prepared = Arc::new(PreparedDictionary {
            digest: dictionary.digest,
            level: LEVEL,
            bytes: dictionary.bytes.clone(),
            encoder,
            decoder,
            memory,
        });
        (prepared, memory)
    };
    let mut evicted = Vec::new();
    let result = {
        let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.lookup_elapsed));
        let mut entries = cache
            .lock()
            .map_err(|_| anyhow::anyhow!("portable payload dictionary cache unavailable"))?;
        if let Some(existing) = cached_dictionary(&mut entries, dictionary)? {
            existing
        } else {
            // Bound both retained entry count and the actual native prepared
            // dictionary footprint. In-flight Arcs belong to bounded caller
            // operations; eviction cannot invalidate an active decode.
            if memory <= CACHE_BYTES {
                let mut retained: usize = entries.iter().map(|entry| entry.memory).sum();
                while entries.len() >= CACHE_ENTRIES || retained + memory > CACHE_BYTES {
                    let old = entries.pop_front().expect("nonempty full cache");
                    retained -= old.memory;
                    evicted.push(old);
                }
                entries.push_back(prepared.clone());
            }
            prepared
        }
    };
    // Potentially expensive native destruction also stays outside the lock.
    drop(evicted);
    Ok(result)
}

/// Train on a bounded prefix (at most 1024 samples and 1 MiB of input).
/// Tiny/unsuitable samples produce a valid raw-content dictionary instead of
/// an endlessly failing training job. No sample bytes are logged or persisted
/// here. Families and immutable dictionary publication are adapter concerns.
pub fn train(samples: &[&[u8]]) -> Result<Dictionary> {
    train_impl(samples, None)
}

pub(crate) fn train_observed(
    samples: &[&[u8]],
    diagnostics: &mut CodecDiagnostics,
) -> Result<Dictionary> {
    train_impl(samples, Some(diagnostics))
}

fn train_impl(
    samples: &[&[u8]],
    mut diagnostics: Option<&mut CodecDiagnostics>,
) -> Result<Dictionary> {
    let (data, sizes) = {
        let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
        let mut data = Vec::new();
        let mut sizes = Vec::new();
        for sample in samples.iter().take(MAX_TRAINING_SAMPLES) {
            let count = sample
                .len()
                .min(MAX_BLOCK_BYTES)
                .min(MAX_TRAINING_BYTES - data.len());
            if count != 0 {
                data.extend_from_slice(&sample[..count]);
                sizes.push(count);
            }
            if data.len() == MAX_TRAINING_BYTES {
                break;
            }
        }
        (data, sizes)
    };
    if sizes.len() >= 8 && data.len() >= 2048 {
        let capacity = (data.len() / 8).clamp(256, MAX_DICTIONARY_BYTES);
        if let Some(diagnostics) = diagnostics.as_deref_mut() {
            diagnostics.trainings = diagnostics.trainings.saturating_add(1);
        }
        if let Some(on_stage) = diagnostics.as_ref().and_then(|d| d.on_stage) {
            on_stage(CodecStage::Train);
        }
        let trained = {
            let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.train_elapsed));
            zstd::dict::from_continuous(&data, &sizes, capacity)
        };
        if let Ok(bytes) = trained {
            return Dictionary::from_bytes_impl(bytes, diagnostics);
        }
        if let Some(diagnostics) = diagnostics.as_deref_mut() {
            diagnostics.training_fallbacks = diagnostics.training_fallbacks.saturating_add(1);
        }
    }
    // A leading zero guarantees that arbitrary sample bytes cannot masquerade
    // as a malformed trained dictionary (whose magic starts with 0x37).
    let bytes = {
        let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
        let mut bytes = Vec::with_capacity((data.len() + 1).min(MAX_DICTIONARY_BYTES));
        bytes.push(0);
        bytes.extend_from_slice(&data[..data.len().min(MAX_DICTIONARY_BYTES - 1)]);
        bytes
    };
    Dictionary::from_bytes_impl(bytes, diagnostics)
}

/// Encode a block at Zstd level 6, or raw when tiny or unprofitable.
pub fn encode(input: &[u8], dictionary: &Dictionary) -> Result<Vec<u8>> {
    encode_impl(input, dictionary, None)
}

pub(crate) fn encode_observed(
    input: &[u8],
    dictionary: &Dictionary,
    diagnostics: &mut CodecDiagnostics,
) -> Result<Vec<u8>> {
    encode_impl(input, dictionary, Some(diagnostics))
}

fn encode_impl(
    input: &[u8],
    dictionary: &Dictionary,
    mut diagnostics: Option<&mut CodecDiagnostics>,
) -> Result<Vec<u8>> {
    let content_digest: [u8; 32] = {
        let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
        ensure!(
            input.len() <= MAX_BLOCK_BYTES,
            "portable payload block exceeds limit"
        );
        Sha256::digest(input).into()
    };
    if input.len() >= 64 {
        let prepared = prepared_dictionary(dictionary, diagnostics.as_deref_mut())?;
        if let Some(on_stage) = diagnostics.as_ref().and_then(|d| d.on_stage) {
            on_stage(CodecStage::Compress);
        }
        let compressed = {
            let _elapsed =
                Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.compress_elapsed));
            let mut compressor =
                zstd::bulk::Compressor::with_prepared_dictionary(&prepared.encoder)?;
            compressor.set_parameter(zstd::zstd_safe::CParameter::WindowLog(WINDOW_LOG))?;
            compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))?;
            compressor.set_parameter(zstd::zstd_safe::CParameter::ContentSizeFlag(true))?;
            compressor.compress(input)?
        };
        if compressed.len() < input.len() {
            let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
            return Ok(envelope(
                ZSTD,
                input.len(),
                dictionary.digest,
                content_digest,
                &compressed,
            ));
        }
    }
    let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
    Ok(envelope(
        RAW,
        input.len(),
        ZERO_DIGEST,
        content_digest,
        input,
    ))
}

fn envelope(
    codec: u8,
    original_len: usize,
    dictionary: [u8; 32],
    content: [u8; 32],
    body: &[u8],
) -> Vec<u8> {
    let mut result = Vec::with_capacity(HEADER_BYTES + body.len());
    result.extend_from_slice(MAGIC);
    result.push(VERSION);
    result.push(codec);
    result.extend_from_slice(&(original_len as u64).to_le_bytes());
    result.extend_from_slice(&dictionary);
    result.extend_from_slice(&content);
    result.extend_from_slice(body);
    result
}

struct Header<'a> {
    codec: u8,
    original_len: usize,
    dictionary: [u8; 32],
    content: [u8; 32],
    body: &'a [u8],
}

fn header(bytes: &[u8]) -> Result<Header<'_>> {
    ensure!(
        (HEADER_BYTES..=HEADER_BYTES + MAX_BODY_BYTES).contains(&bytes.len()),
        "portable payload envelope size is invalid"
    );
    ensure!(&bytes[..4] == MAGIC, "invalid portable payload magic");
    ensure!(bytes[4] == VERSION, "unsupported portable payload version");
    let codec = bytes[5];
    ensure!(
        codec == RAW || codec == ZSTD,
        "unsupported portable payload codec"
    );
    let original_len = u64::from_le_bytes(bytes[6..14].try_into()?);
    ensure!(
        original_len <= MAX_BLOCK_BYTES as u64,
        "portable payload decoded size exceeds limit"
    );
    let dictionary = bytes[14..46].try_into()?;
    let content = bytes[46..78].try_into()?;
    let body = &bytes[HEADER_BYTES..];
    if codec == RAW {
        ensure!(
            dictionary == ZERO_DIGEST,
            "raw portable payload references a dictionary"
        );
        ensure!(
            body.len() as u64 == original_len,
            "raw portable payload length mismatch"
        );
    } else {
        ensure!(
            dictionary != ZERO_DIGEST,
            "compressed portable payload has no dictionary identity"
        );
        ensure!(
            body.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]),
            "invalid portable payload Zstd frame"
        );
        let frame_len = zstd::zstd_safe::find_frame_compressed_size(body)
            .map_err(|_| anyhow::anyhow!("invalid portable payload Zstd frame"))?;
        ensure!(
            frame_len == body.len(),
            "portable payload has trailing frames or bytes"
        );
        let frame_size = zstd::zstd_safe::get_frame_content_size(body)
            .map_err(|_| anyhow::anyhow!("invalid portable payload frame content size"))?;
        if let Some(frame_size) = frame_size {
            ensure!(
                frame_size == original_len,
                "portable payload frame length mismatch"
            );
        }
        // Zstd's streaming implementation may take a one-shot fast path for
        // frames with a known content size before checking WindowLogMax.
        // Enforce the standard frame's declared window independently as well.
        // In single-segment mode the window is exactly the frame content size
        // (already bounded above); otherwise byte 5 is Window_Descriptor.
        if body[4] & 0x20 == 0 {
            let descriptor = *body.get(5).context("missing Zstd window descriptor")?;
            let base = 1_u64 << (10 + (descriptor >> 3));
            let window = base + (base / 8) * u64::from(descriptor & 7);
            ensure!(
                window <= MAX_BLOCK_BYTES as u64,
                "portable payload Zstd window exceeds limit"
            );
        } else {
            ensure!(
                frame_size.is_some(),
                "single-segment Zstd frame has no content size"
            );
        }
    }
    Ok(Header {
        codec,
        original_len: original_len as usize,
        dictionary,
        content,
        body,
    })
}

/// Inspect the explicit format and structural bounds. This does not verify the
/// original-byte checksum; use `decode` before accepting decoded content.
pub fn is_compressed(envelope: &[u8]) -> Result<bool> {
    Ok(header(envelope)?.codec == ZSTD)
}

/// Decode exactly one bounded block. No recovery to empty bytes on corruption.
pub fn decode(envelope: &[u8], dictionary: Option<&Dictionary>) -> Result<Vec<u8>> {
    decode_impl(envelope, dictionary, None)
}

pub(crate) fn decode_observed(
    envelope: &[u8],
    dictionary: Option<&Dictionary>,
    diagnostics: &mut CodecDiagnostics,
) -> Result<Vec<u8>> {
    decode_impl(envelope, dictionary, Some(diagnostics))
}

fn decode_impl(
    envelope: &[u8],
    dictionary: Option<&Dictionary>,
    mut diagnostics: Option<&mut CodecDiagnostics>,
) -> Result<Vec<u8>> {
    let header = {
        let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
        header(envelope)?
    };
    let result = if header.codec == RAW {
        let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
        header.body.to_vec()
    } else {
        let dictionary = {
            let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
            let dictionary = dictionary.context("portable payload dictionary is missing")?;
            ensure!(
                dictionary.digest == header.dictionary,
                "portable payload dictionary mismatch"
            );
            dictionary
        };
        let prepared = prepared_dictionary(dictionary, diagnostics.as_deref_mut())?;
        if let Some(on_stage) = diagnostics.as_ref().and_then(|d| d.on_stage) {
            on_stage(CodecStage::Decompress);
        }
        let _elapsed = Elapsed::new(
            diagnostics
                .as_deref_mut()
                .map(|d| &mut d.decompress_elapsed),
        );
        let mut decoder =
            zstd::stream::read::Decoder::with_prepared_dictionary(header.body, &prepared.decoder)?
                .single_frame();
        // Defense in depth after the explicit frame-window check above.
        // WindowLogMax only applies to the streaming allocation path.
        decoder.window_log_max(WINDOW_LOG)?;
        let mut result = vec![0; header.original_len];
        decoder
            .read_exact(&mut result)
            .context("corrupt or short portable payload Zstd body")?;
        // Force frame completion/checksum validation, and reject extra decoded
        // bytes without allowing read_to_end to grow a buffer beyond the cap.
        let mut extra = [0];
        ensure!(
            decoder
                .read(&mut extra)
                .context("corrupt portable payload Zstd body")?
                == 0,
            "portable payload decoded length mismatch"
        );
        result
    };
    let _elapsed = Elapsed::new(diagnostics.as_deref_mut().map(|d| &mut d.verify_elapsed));
    ensure!(
        result.len() == header.original_len,
        "portable payload decoded length mismatch"
    );
    let actual: [u8; 32] = Sha256::digest(&result).into();
    ensure!(
        actual == header.content,
        "portable payload content checksum mismatch"
    );
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{
        Dictionary, MAX_BLOCK_BYTES, MAX_DICTIONARY_BYTES, decode, encode, is_compressed, train,
    };

    // These wire fixtures do not use the encoder, private header constants, or
    // subject-generated digests. SHA-256("abc") and SHA-256("") are standard
    // known answers. The compressed fixture is a standard Zstd single-segment
    // frame with one final raw block, independently specified by the format.
    fn raw_abc() -> Vec<u8> {
        hex::decode(concat!(
            "5050415901000300000000000000",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "616263"
        ))
        .unwrap()
    }

    fn zstd_abc() -> Vec<u8> {
        hex::decode(concat!(
            "5050415901010300000000000000",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "28b52ffd2003190000616263"
        ))
        .unwrap()
    }

    fn abc_dictionary() -> Dictionary {
        Dictionary::from_bytes(b"abc".to_vec()).unwrap()
    }

    #[test]
    fn v1_raw_wire_bytes_and_present_empty_are_stable() {
        let dictionary = abc_dictionary();
        let expected = raw_abc();
        assert_eq!(encode(b"abc", &dictionary).unwrap(), expected);
        assert_eq!(decode(&expected, None).unwrap(), b"abc");
        assert!(!is_compressed(&expected).unwrap());

        // NULL is deliberately not a codec input. A present empty value has
        // its own complete envelope and cannot turn into absent storage.
        let empty = hex::decode(concat!(
            "5050415901000000000000000000",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ))
        .unwrap();
        assert_eq!(encode(b"", &dictionary).unwrap(), empty);
        assert_eq!(decode(&empty, None).unwrap(), b"");
    }

    #[test]
    fn standard_zstd_frame_decodes_with_explicit_dictionary_identity() {
        let fixture = zstd_abc();
        assert!(is_compressed(&fixture).unwrap());
        assert_eq!(decode(&fixture, Some(&abc_dictionary())).unwrap(), b"abc");
        assert!(
            decode(&fixture, None)
                .unwrap_err()
                .to_string()
                .contains("dictionary is missing")
        );
        let other = Dictionary::from_bytes(b"different database dictionary".to_vec()).unwrap();
        assert!(
            decode(&fixture, Some(&other))
                .unwrap_err()
                .to_string()
                .contains("dictionary mismatch")
        );
    }

    #[test]
    fn compression_preserves_multibyte_utf8_bytes_at_the_block_limit() {
        let dictionary = Dictionary::from_bytes(b"\0JSON text history metadata".to_vec()).unwrap();
        let input = "\u{1f642}".repeat(16_384).into_bytes();
        assert_eq!(input.len(), MAX_BLOCK_BYTES);
        let encoded = encode(&input, &dictionary).unwrap();
        assert!(is_compressed(&encoded).unwrap());
        let decoded = decode(&encoded, Some(&dictionary)).unwrap();
        assert_eq!(decoded, [0xf0, 0x9f, 0x99, 0x82].repeat(16_384));
        let incompressible: Vec<u8> = (0..=u8::MAX).collect();
        let raw = encode(&incompressible, &dictionary).unwrap();
        assert!(!is_compressed(&raw).unwrap());
        assert_eq!(decode(&raw, None).unwrap(), incompressible);
    }

    #[test]
    fn corruption_and_extra_frames_never_return_successful_data() {
        let dictionary = abc_dictionary();
        let fixture = zstd_abc();
        let mut changed = fixture.clone();
        *changed.last_mut().unwrap() ^= 1;
        assert!(
            decode(&changed, Some(&dictionary))
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
        for length in [0, 4, 77, fixture.len() - 1] {
            assert!(decode(&fixture[..length], Some(&dictionary)).is_err());
        }
        let mut trailing_byte = fixture.clone();
        trailing_byte.push(0);
        assert!(
            decode(&trailing_byte, Some(&dictionary))
                .unwrap_err()
                .to_string()
                .contains("trailing")
        );
        let mut concatenated = fixture.clone();
        concatenated.extend_from_slice(&fixture[78..]);
        assert!(
            decode(&concatenated, Some(&dictionary))
                .unwrap_err()
                .to_string()
                .contains("trailing")
        );
        let mut raw = raw_abc();
        *raw.last_mut().unwrap() ^= 1;
        assert!(
            decode(&raw, None)
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
    }

    #[test]
    fn unsupported_headers_and_size_or_window_limits_are_rejected() {
        let dictionary = abc_dictionary();
        let mut bad_magic = raw_abc();
        bad_magic[0] = b'X';
        assert!(
            decode(&bad_magic, None)
                .unwrap_err()
                .to_string()
                .contains("magic")
        );
        let mut raw_dictionary = raw_abc();
        raw_dictionary[14] = 1;
        assert!(
            decode(&raw_dictionary, None)
                .unwrap_err()
                .to_string()
                .contains("raw portable payload references a dictionary")
        );
        let mut version = raw_abc();
        version[4] = 2;
        assert!(
            decode(&version, None)
                .unwrap_err()
                .to_string()
                .contains("version")
        );
        let mut codec = raw_abc();
        codec[5] = 2;
        assert!(
            decode(&codec, None)
                .unwrap_err()
                .to_string()
                .contains("codec")
        );
        let mut oversized = raw_abc();
        oversized[6..14].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(
            decode(&oversized, None)
                .unwrap_err()
                .to_string()
                .contains("size exceeds limit")
        );
        assert!(encode(&vec![0; MAX_BLOCK_BYTES + 1], &dictionary).is_err());
        let mut wrong_length = zstd_abc();
        wrong_length[6] = 4;
        assert!(
            decode(&wrong_length, Some(&dictionary))
                .unwrap_err()
                .to_string()
                .contains("frame length mismatch")
        );
        let mut wider_window = zstd_abc();
        wider_window.truncate(78);
        // Non-single-segment frame, no content-size field, window descriptor
        // 0x38 => windowLog 17 (128 KiB), one final raw block "abc".
        wider_window.extend_from_slice(&[
            0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x38, 0x19, 0x00, 0x00, b'a', b'b', b'c',
        ]);
        assert!(
            decode(&wider_window, Some(&dictionary))
                .unwrap_err()
                .to_string()
                .contains("window exceeds limit")
        );
        // A known-size frame can take Zstd's one-shot decoder fast path;
        // window enforcement must also cover that path.
        wider_window.truncate(78);
        wider_window.extend_from_slice(&[
            0x28, 0xb5, 0x2f, 0xfd, 0x80, 0x38, 3, 0, 0, 0, 0x19, 0, 0, b'a', b'b', b'c',
        ]);
        assert!(
            decode(&wider_window, Some(&dictionary))
                .unwrap_err()
                .to_string()
                .contains("window exceeds limit")
        );
        // No content-size field: the envelope must still check actual output.
        let mut extra_output = zstd_abc();
        extra_output[6] = 2;
        extra_output.truncate(78);
        extra_output.extend_from_slice(&[
            0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x30, 0x19, 0, 0, b'a', b'b', b'c',
        ]);
        assert!(
            decode(&extra_output, Some(&dictionary))
                .unwrap_err()
                .to_string()
                .contains("decoded length mismatch")
        );
    }

    #[test]
    fn bounded_training_has_usable_fallback_and_rejects_invalid_dictionaries() {
        assert!(Dictionary::from_bytes(Vec::new()).is_err());
        assert!(Dictionary::from_bytes(vec![0; MAX_DICTIONARY_BYTES + 1]).is_err());
        assert!(Dictionary::from_bytes(vec![0x37, 0xa4, 0x30, 0xec, 0, 0, 0, 0]).is_err());
        for samples in [vec![], vec![&b"tiny"[..]]] {
            let dictionary = train(&samples).unwrap();
            assert!(!dictionary.bytes().is_empty());
            assert!(dictionary.bytes().len() <= MAX_DICTIONARY_BYTES);
            let bytes = b"sample content ".repeat(1024);
            let encoded = encode(&bytes, &dictionary).unwrap();
            assert_eq!(decode(&encoded, Some(&dictionary)).unwrap(), bytes);
        }
        let sample = vec![b'x'; 2 * 1024 * 1024];
        let dictionary = train(&[&sample]).unwrap();
        assert!(dictionary.bytes().len() <= MAX_DICTIONARY_BYTES);
        let input = b"large sample dictionary fallback ".repeat(64);
        let encoded = encode(&input, &dictionary).unwrap();
        assert_eq!(decode(&encoded, Some(&dictionary)).unwrap(), input);
    }
}
