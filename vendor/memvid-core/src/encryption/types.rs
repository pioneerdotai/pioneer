use crate::encryption::constants::{
    CIPHER_AES_256_GCM, KDF_ARGON2ID, MV2E_HEADER_SIZE, MV2E_MAGIC, MV2E_VERSION, NONCE_SIZE,
    SALT_SIZE,
};
use crate::encryption::error::EncryptionError;

/// MV2E file header (fixed-size, 64 bytes).
#[derive(Debug, Clone)]
pub struct Mv2eHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub kdf_algorithm: KdfAlgorithm,
    pub cipher_algorithm: CipherAlgorithm,
    pub salt: [u8; SALT_SIZE],
    pub nonce: [u8; NONCE_SIZE],
    pub original_size: u64,
    pub reserved: [u8; 4],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KdfAlgorithm {
    Argon2id = KDF_ARGON2ID,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CipherAlgorithm {
    Aes256Gcm = CIPHER_AES_256_GCM,
}

impl Mv2eHeader {
    pub const SIZE: usize = MV2E_HEADER_SIZE;

    #[must_use]
    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6] = self.kdf_algorithm as u8;
        buf[7] = self.cipher_algorithm as u8;
        buf[8..40].copy_from_slice(&self.salt);
        buf[40..52].copy_from_slice(&self.nonce);
        buf[52..60].copy_from_slice(&self.original_size.to_le_bytes());
        buf[60..64].copy_from_slice(&self.reserved);
        buf
    }

    pub fn decode(bytes: &[u8; Self::SIZE]) -> Result<Self, EncryptionError> {
        let magic = [bytes[0], bytes[1], bytes[2], bytes[3]];
        if magic != MV2E_MAGIC {
            return Err(EncryptionError::InvalidMagic {
                expected: MV2E_MAGIC,
                found: magic,
            });
        }

        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != MV2E_VERSION {
            return Err(EncryptionError::UnsupportedVersion { version });
        }

        let kdf_algorithm = match bytes[6] {
            KDF_ARGON2ID => KdfAlgorithm::Argon2id,
            other => return Err(EncryptionError::UnsupportedKdf { id: other }),
        };

        let cipher_algorithm = match bytes[7] {
            CIPHER_AES_256_GCM => CipherAlgorithm::Aes256Gcm,
            other => return Err(EncryptionError::UnsupportedCipher { id: other }),
        };

        let mut salt = [0u8; SALT_SIZE];
        salt.copy_from_slice(&bytes[8..40]);

        let mut nonce = [0u8; NONCE_SIZE];
        nonce.copy_from_slice(&bytes[40..52]);

        let original_size = u64::from_le_bytes([
            bytes[52], bytes[53], bytes[54], bytes[55], bytes[56], bytes[57], bytes[58], bytes[59],
        ]);

        let reserved = [bytes[60], bytes[61], bytes[62], bytes[63]];

        Ok(Self {
            magic,
            version,
            kdf_algorithm,
            cipher_algorithm,
            salt,
            nonce,
            original_size,
            reserved,
        })
    }
}
