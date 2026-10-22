/// File magic for `.mv2` memories.
pub const MAGIC: [u8; 4] = *b"MV2\0";
/// Logical header size (4 KiB) reserving space for future upgrades.
pub const HEADER_SIZE: usize = 4096;
/// Magic bytes for the time index track.
pub const TIME_INDEX_MAGIC: [u8; 4] = *b"MVTI";
#[cfg(feature = "temporal_track")]
/// Magic bytes for the temporal mentions track.
pub const TEMPORAL_TRACK_MAGIC: [u8; 4] = *b"MVTN";
#[cfg(feature = "temporal_track")]
/// Initial on-disk version for the temporal mentions track header.
pub const TEMPORAL_TRACK_VERSION: u16 = 1;
/// Specification major version.
pub const SPEC_MAJOR: u8 = 2;
/// Specification minor version.
pub const SPEC_MINOR: u8 = 1;
/// Combined two-byte specification version encoded in headers.
pub const SPEC_VERSION: u16 = ((SPEC_MAJOR as u16) << 8) | SPEC_MINOR as u16;
/// Binary format schema version.
pub const FORMAT_VERSION: u16 = 1;

/// Embedded WAL begins immediately after the fixed header.
pub const WAL_OFFSET: u64 = HEADER_SIZE as u64;
/// Minimal WAL size for empty/small memories (auto-grows on demand).
pub const WAL_SIZE_TINY: u64 = 64 * 1024;
/// WAL size tiers based on requested capacity (<100 MB).
pub const WAL_SIZE_SMALL: u64 = 1024 * 1024;
/// WAL size for memories under 1 GB.
pub const WAL_SIZE_MEDIUM: u64 = 4 * 1024 * 1024;
/// WAL size for memories under 10 GB.
pub const WAL_SIZE_LARGE: u64 = 16 * 1024 * 1024;
/// WAL size for larger memories.
pub const WAL_SIZE_XLARGE: u64 = 64 * 1024 * 1024;
/// Trigger checkpoints when the WAL exceeds 75 % occupancy.
pub const WAL_CHECKPOINT_THRESHOLD: f64 = 0.75;
/// Additional checkpoint every N transactions (PRD default).
pub const WAL_CHECKPOINT_PERIOD: u64 = 1_000;

/// Memvid's Ed25519 public key for verifying signed tickets.
/// This key is used to verify that tickets were issued by the official Memvid control plane.
/// The corresponding private key is held securely on the Memvid dashboard.
pub const MEMVID_TICKET_PUBKEY: &str = "DFKNhP/yO5i1b9aKL+aHeBaGunz9sMfOF736fzYws4Q=";
