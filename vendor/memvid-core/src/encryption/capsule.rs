use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use atomic_write_file::AtomicWriteFile;
use zeroize::Zeroize;

use crate::encryption::capsule_stream::{lock_file_stream, unlock_file_stream};
use crate::encryption::crypto::{decrypt, derive_key};
use crate::encryption::error::EncryptionError;
use crate::encryption::types::Mv2eHeader;

/// Lock (encrypt) an `.mv2` file into a `.mv2e` capsule.
pub fn lock_file(
    input: impl AsRef<Path>,
    output: Option<&Path>,
    password: &[u8],
) -> Result<PathBuf, EncryptionError> {
    lock_file_stream(input, output, password)
}

/// Unlock (decrypt) an `.mv2e` capsule into an `.mv2` file.
pub fn unlock_file(
    input: impl AsRef<Path>,
    output: Option<&Path>,
    password: &[u8],
) -> Result<PathBuf, EncryptionError> {
    let input = input.as_ref();

    let mut file = File::open(input).map_err(|source| EncryptionError::Io {
        source,
        path: Some(input.to_path_buf()),
    })?;

    let mut header_bytes = [0u8; Mv2eHeader::SIZE];
    file.read_exact(&mut header_bytes)
        .map_err(|source| EncryptionError::Io {
            source,
            path: Some(input.to_path_buf()),
        })?;
    let header = Mv2eHeader::decode(&header_bytes)?;

    if header.reserved[0] == 0x01 {
        unlock_file_stream(input, output, password)
    } else {
        unlock_file_oneshot(input, output, password, header)
    }
}

fn unlock_file_oneshot(
    input: &Path,
    output: Option<&Path>,
    password: &[u8],
    header: Mv2eHeader,
) -> Result<PathBuf, EncryptionError> {
    let mut file = File::open(input).map_err(|source| EncryptionError::Io {
        source,
        path: Some(input.to_path_buf()),
    })?;

    file.seek(SeekFrom::Start(Mv2eHeader::SIZE as u64))
        .map_err(|source| EncryptionError::Io {
            source,
            path: Some(input.to_path_buf()),
        })?;

    let mut ciphertext = Vec::new();
    file.read_to_end(&mut ciphertext)
        .map_err(|source| EncryptionError::Io {
            source,
            path: Some(input.to_path_buf()),
        })?;

    let mut key = derive_key(password, &header.salt)?;
    let plaintext = decrypt(&ciphertext, &key, &header.nonce)?;
    key.zeroize();

    if plaintext.len() as u64 != header.original_size {
        return Err(EncryptionError::SizeMismatch {
            expected: header.original_size,
            actual: plaintext.len() as u64,
        });
    }

    validate_mv2_bytes(&plaintext)?;

    let output_path = output
        .map(PathBuf::from)
        .unwrap_or_else(|| input.with_extension("mv2"));

    write_atomic(&output_path, |writer| -> Result<(), EncryptionError> {
        writer.write_all(&plaintext)?;
        Ok(())
    })?;

    Ok(output_path)
}

pub fn validate_mv2_file(path: &Path) -> Result<(), EncryptionError> {
    let mut file = File::open(path).map_err(|source| EncryptionError::Io {
        source,
        path: Some(path.to_path_buf()),
    })?;

    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)
        .map_err(|source| EncryptionError::Io {
            source,
            path: Some(path.to_path_buf()),
        })?;

    // Plain `.mv2` files start with "MV2\0".
    if magic != *b"MV2\0" {
        return Err(EncryptionError::NotMv2File {
            path: path.to_path_buf(),
        });
    }

    Ok(())
}

fn validate_mv2_bytes(bytes: &[u8]) -> Result<(), EncryptionError> {
    if bytes.len() < 4 || &bytes[0..4] != b"MV2\0" {
        return Err(EncryptionError::CorruptedDecryption);
    }
    Ok(())
}

pub fn write_atomic<F, E>(path: &Path, write_fn: F) -> Result<(), E>
where
    F: FnOnce(&mut File) -> Result<(), E>,
    E: From<std::io::Error>,
{
    let mut options = AtomicWriteFile::options();
    options.read(false);
    let mut atomic = options.open(path)?;

    let file = atomic.as_file_mut();
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    write_fn(file)?;
    file.flush()?;
    file.sync_all()?;
    atomic.commit()?;
    Ok(())
}
