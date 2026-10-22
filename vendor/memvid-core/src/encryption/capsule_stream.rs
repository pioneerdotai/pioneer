use rand::RngCore;
use rand::rngs::OsRng;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use zeroize::Zeroize;

use crate::encryption::capsule::{validate_mv2_file, write_atomic};
use crate::encryption::constants::{MV2E_MAGIC, MV2E_VERSION, NONCE_SIZE, SALT_SIZE};
use crate::encryption::crypto::{decrypt, derive_key, encrypt};
use crate::encryption::error::EncryptionError;
use crate::encryption::types::{CipherAlgorithm, KdfAlgorithm, Mv2eHeader};

const CHUNK_SIZE: usize = 1024 * 1024;

// format: [header][len0][chunk0][len1][chunk1]...
// reserved[0] == 0x01 => streaming framed format

pub fn lock_file_stream(
    input: impl AsRef<Path>,
    output: Option<&Path>,
    password: &[u8],
) -> Result<PathBuf, EncryptionError> {
    let input = input.as_ref();
    validate_mv2_file(input)?;

    let metadata = std::fs::metadata(input)?;

    let mut salt = [0u8; SALT_SIZE];
    let mut base_nonce = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut salt);
    OsRng.fill_bytes(&mut base_nonce);

    let mut key = derive_key(password, &salt)?;

    let header = Mv2eHeader {
        magic: MV2E_MAGIC,
        version: MV2E_VERSION,
        kdf_algorithm: KdfAlgorithm::Argon2id,
        cipher_algorithm: CipherAlgorithm::Aes256Gcm,
        salt,
        nonce: base_nonce,
        original_size: metadata.len(),
        reserved: [0x01, 0, 0, 0],
    };

    let output_path = output
        .map(PathBuf::from)
        .unwrap_or_else(|| input.with_extension("mv2e"));

    let input_file = File::open(input)?;
    let mut reader = BufReader::new(input_file);

    write_atomic(&output_path, |file| -> Result<(), EncryptionError> {
        let mut writer = BufWriter::new(file);
        writer.write_all(&header.encode())?;

        let mut buffer = vec![0u8; CHUNK_SIZE];
        let mut chunk_index: u64 = 0;

        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }

            let mut nonce = base_nonce;
            nonce[NONCE_SIZE - 8..].copy_from_slice(&chunk_index.to_be_bytes());

            let ciphertext = encrypt(&buffer[..n], &key, &nonce)?;

            let chunk_len = ciphertext.len() as u32;
            writer.write_all(&chunk_len.to_le_bytes())?;
            writer.write_all(&ciphertext)?;

            chunk_index += 1;
        }

        writer.flush()?;
        Ok(())
    })?;

    key.zeroize();
    Ok(output_path)
}

pub fn unlock_file_stream(
    input: impl AsRef<Path>,
    output: Option<&Path>,
    password: &[u8],
) -> Result<PathBuf, EncryptionError> {
    let input = input.as_ref();

    let input_file = File::open(input)?;
    let mut reader = BufReader::new(input_file);

    let mut header_bytes = [0u8; Mv2eHeader::SIZE];
    reader.read_exact(&mut header_bytes)?;

    let header = Mv2eHeader::decode(&header_bytes)?;
    let mut key = derive_key(password, &header.salt)?;

    let output_path = output
        .map(PathBuf::from)
        .unwrap_or_else(|| input.with_extension("mv2"));

    write_atomic(&output_path, |file| -> Result<(), EncryptionError> {
        let mut writer = BufWriter::new(file);
        let mut chunk_index: u64 = 0;

        loop {
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let chunk_len = u32::from_le_bytes(len_bytes) as usize;

            let mut ciphertext = vec![0u8; chunk_len];
            reader.read_exact(&mut ciphertext)?;

            let mut nonce = header.nonce;
            nonce[NONCE_SIZE - 8..].copy_from_slice(&chunk_index.to_be_bytes());

            let plaintext = decrypt(&ciphertext, &key, &nonce)?;
            writer.write_all(&plaintext)?;

            chunk_index += 1;
        }

        writer.flush()?;
        Ok(())
    })?;

    key.zeroize();
    Ok(output_path)
}
