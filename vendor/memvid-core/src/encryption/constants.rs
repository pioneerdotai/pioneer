//! Constants for the `.mv2e` encrypted capsule format.

/// Magic bytes identifying an encrypted capsule file.
pub const MV2E_MAGIC: [u8; 4] = *b"MV2E";

/// Current `.mv2e` format version.
pub const MV2E_VERSION: u16 = 1;

/// Fixed header size for `.mv2e`.
pub const MV2E_HEADER_SIZE: usize = 64;

/// KDF algorithm identifiers.
pub const KDF_ARGON2ID: u8 = 1;

/// Cipher algorithm identifiers.
pub const CIPHER_AES_256_GCM: u8 = 1;

/// Cryptographic parameter sizes.
pub const SALT_SIZE: usize = 32;
pub const NONCE_SIZE: usize = 12;
pub const TAG_SIZE: usize = 16;
pub const KEY_SIZE: usize = 32;

/// Argon2id parameters (OWASP 2024 recommendations).
pub const ARGON2_MEMORY_KIB: u32 = 64 * 1024; // 64 MiB
pub const ARGON2_ITERATIONS: u32 = 3;
pub const ARGON2_PARALLELISM: u32 = 4;
