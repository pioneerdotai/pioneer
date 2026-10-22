use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use argon2::{Algorithm, Argon2, Params, Version};

use crate::encryption::constants::{
    ARGON2_ITERATIONS, ARGON2_MEMORY_KIB, ARGON2_PARALLELISM, KEY_SIZE, NONCE_SIZE, SALT_SIZE,
};
use crate::encryption::error::EncryptionError;

pub fn derive_key(
    password: &[u8],
    salt: &[u8; SALT_SIZE],
) -> Result<[u8; KEY_SIZE], EncryptionError> {
    let params = Params::new(
        ARGON2_MEMORY_KIB,
        ARGON2_ITERATIONS,
        ARGON2_PARALLELISM,
        Some(KEY_SIZE),
    )
    .map_err(|e| EncryptionError::KeyDerivation {
        reason: e.to_string(),
    })?;

    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    let mut key = [0u8; KEY_SIZE];
    argon2
        .hash_password_into(password, salt, &mut key)
        .map_err(|e| EncryptionError::KeyDerivation {
            reason: e.to_string(),
        })?;

    Ok(key)
}

pub fn encrypt(
    plaintext: &[u8],
    key: &[u8; KEY_SIZE],
    nonce: &[u8; NONCE_SIZE],
) -> Result<Vec<u8>, EncryptionError> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| EncryptionError::CipherInit {
        reason: e.to_string(),
    })?;
    cipher
        .encrypt(Nonce::from_slice(nonce), plaintext)
        .map_err(|e| EncryptionError::Encryption {
            reason: e.to_string(),
        })
}

pub fn decrypt(
    ciphertext: &[u8],
    key: &[u8; KEY_SIZE],
    nonce: &[u8; NONCE_SIZE],
) -> Result<Vec<u8>, EncryptionError> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| EncryptionError::CipherInit {
        reason: e.to_string(),
    })?;
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|e| EncryptionError::Decryption {
            reason: e.to_string(),
        })
}
