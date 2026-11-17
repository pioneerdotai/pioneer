//! Post-link platform IDs. Revision and manifest digests never substitute for
//! UUID / ELF build-id / PE CodeView GUID+age.
use anyhow::{Context, Result, bail, ensure};
use object::Object;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    path::Path,
};
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModuleIdentity {
    pub format: String,
    pub module_id: String,
    pub binary_sha256: String,
    pub bytes: u64,
}
#[derive(Clone, Copy)]
struct ModuleRead<'a> {
    cache: &'a object::read::ReadCache<fs::File>,
    remaining: &'a std::cell::Cell<u64>,
}
impl<'a> object::read::ReadRef<'a> for ModuleRead<'a> {
    fn len(self) -> Result<u64, ()> {
        object::read::ReadRef::len(self.cache)
    }
    fn read_bytes_at(self, offset: u64, size: u64) -> Result<&'a [u8], ()> {
        let remaining = self.remaining.get().checked_sub(size).ok_or(())?;
        self.remaining.set(remaining);
        object::read::ReadRef::read_bytes_at(self.cache, offset, size)
    }
    fn read_bytes_at_until(
        self,
        range: std::ops::Range<u64>,
        delimiter: u8,
    ) -> Result<&'a [u8], ()> {
        let end = range.end.min(range.start.saturating_add(4096));
        let size = end.checked_sub(range.start).ok_or(())?;
        self.remaining
            .set(self.remaining.get().checked_sub(size).ok_or(())?);
        object::read::ReadRef::read_bytes_at_until(self.cache, range.start..end, delimiter)
    }
}
pub fn module_identity(path: &Path) -> Result<ModuleIdentity> {
    module_identity_guarded(path, || true)
}
pub(crate) fn module_identity_guarded(
    path: &Path,
    allowed: impl Fn() -> bool,
) -> Result<ModuleIdentity> {
    ensure!(allowed(), "module_identity_cancelled");
    let mut binary = fs::File::open(path)?;
    let metadata = binary.metadata()?;
    ensure!(
        metadata.len() <= 2 * 1024 * 1024 * 1024,
        "binary exceeds 2GiB identity-read limit"
    );
    let (format, module_id) = {
        let cache = object::read::ReadCache::new(binary.try_clone()?);
        let budget = std::cell::Cell::new(32 * 1024 * 1024);
        let file = object::File::parse(ModuleRead {
            cache: &cache,
            remaining: &budget,
        })
        .context("unsupported or oversized executable metadata")?;
        if let Some(uuid) = file.mach_uuid()? {
            ("macho_uuid", hex::encode(uuid))
        } else if let Some(id) = file.build_id()? {
            ("elf_build_id", hex::encode(id))
        } else if let Some(pdb) = file.pdb_info()? {
            (
                "pe_codeview_guid_age",
                format!("{}-{}", hex::encode(pdb.guid()), pdb.age()),
            )
        } else {
            bail!("executable has no supported post-link module ID");
        }
    };
    binary.seek(SeekFrom::Start(0))?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 65536];
    let mut bytes = 0_u64;
    let started = std::time::Instant::now();
    loop {
        ensure!(allowed(), "module_identity_cancelled");
        let n = binary.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        bytes = bytes.saturating_add(n as u64);
        ensure!(bytes <= metadata.len(), "executable grew during hashing");
        ensure!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "module checksum timeout"
        );
        hash.update(&buffer[..n]);
    }
    let after = binary.metadata()?;
    ensure!(
        bytes == metadata.len()
            && metadata.len() == after.len()
            && metadata.modified().ok() == after.modified().ok(),
        "executable changed during identity read"
    );
    Ok(ModuleIdentity {
        format: format.into(),
        module_id,
        binary_sha256: hex::encode(hash.finalize()),
        bytes,
    })
}

// The retained optimized Gateway inventory is 3,374,663,472 bytes (including
// a 2,969,275,740-byte DWARF). 4 GiB admits that exact artifact with finite
// headroom; buffers (64 KiB), file count (4096), manifest and metadata caps do
// not grow with this byte budget. The checksum wall budget scales from 30s
// for 2 GiB to 60s for 4 GiB; slow storage still fails explicitly.
const SYMBOL_INVENTORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const SYMBOL_CHECKSUM_TIME: std::time::Duration = std::time::Duration::from_secs(60);
fn admit_symbol_file(total: u64, actual: u64, expected: Option<u64>) -> Result<u64> {
    let next = total
        .checked_add(actual)
        .context("symbol inventory byte overflow")?;
    ensure!(
        next <= SYMBOL_INVENTORY_BYTES,
        "symbol inventory exceeds byte budget"
    );
    ensure!(expected == Some(actual), "symbol file size mismatch");
    Ok(next)
}
fn symbol_checksum(
    mut file: impl Read,
    expected: u64,
    started: std::time::Instant,
) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut bytes = 0_u64;
    loop {
        ensure!(
            started.elapsed() < SYMBOL_CHECKSUM_TIME,
            "symbol inventory checksum timeout"
        );
        let n = file.read(&mut buffer)?;
        bytes = bytes
            .checked_add(n as u64)
            .context("symbol byte overflow")?;
        ensure!(bytes <= expected, "symbol changed during hashing");
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    ensure!(bytes == expected, "symbol changed during hashing");
    Ok(hex::encode(hash.finalize()))
}

pub fn validate_symbols(target: &ModuleIdentity, symbol_manifest: &Path) -> Result<()> {
    let metadata = fs::metadata(symbol_manifest)?;
    ensure!(
        metadata.len() <= 2 * 1024 * 1024,
        "symbol manifest too large"
    );
    let mut bytes = Vec::new();
    fs::File::open(symbol_manifest)?
        .take(2 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 2 * 1024 * 1024,
        "symbol manifest grew beyond budget"
    );
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    ensure!(value["schema_version"] == 1, "unsupported symbol manifest");
    validate_id(target, &value)?;
    let packaged = value
        .get("packaged_binary_sha256s")
        .map(|v| v.as_array().context("invalid packaged checksum inventory"))
        .transpose()?;
    ensure!(
        packaged.is_none_or(|v| v.len() <= 16),
        "packaged checksum inventory overflow"
    );
    ensure!(
        [
            value["binary_sha256"].as_str(),
            value["packaged_binary_sha256"].as_str()
        ]
        .contains(&Some(target.binary_sha256.as_str()))
            || packaged.is_some_and(|v| v
                .iter()
                .any(|v| v.as_str() == Some(target.binary_sha256.as_str()))),
        "target checksum is not one of the retained exact artifacts"
    );
    // Signing/packaging may change checksum but must preserve platform module ID.
    ensure!(
        value["symbols_validated"].as_bool() == Some(true),
        "release symbols not validated"
    );
    let files = value["files"]
        .as_array()
        .context("symbol inventory missing")?;
    ensure!(
        !files.is_empty() && files.len() <= 4096,
        "symbol inventory empty or oversized"
    );
    let root = symbol_manifest
        .parent()
        .context("symbol manifest has no parent")?;
    let mut total = 0_u64;
    let started = std::time::Instant::now();
    for item in files {
        let relative = std::path::Path::new(
            item["filename"]
                .as_str()
                .context("symbol filename missing")?,
        );
        ensure!(
            !relative.is_absolute()
                && relative
                    .components()
                    .all(|c| matches!(c, std::path::Component::Normal(_))),
            "invalid symbol filename"
        );
        let mut checked = root.to_path_buf();
        for component in relative.components() {
            checked.push(component);
            ensure!(
                !fs::symlink_metadata(&checked)?.file_type().is_symlink(),
                "symbol symlink component rejected"
            );
        }
        let path = root.join(relative);
        let metadata = fs::symlink_metadata(&path)?;
        ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "symbol is not regular file"
        );
        total = admit_symbol_file(total, metadata.len(), item["bytes"].as_u64())?;
        let file = fs::File::open(path)?;
        let checksum = symbol_checksum(file, metadata.len(), started)?;
        ensure!(
            item["sha256"].as_str() == Some(checksum.as_str()),
            "symbol checksum mismatch"
        );
    }
    Ok(())
}
fn validate_id(target: &ModuleIdentity, value: &serde_json::Value) -> Result<()> {
    ensure!(
        value["module_id"].as_str() == Some(target.module_id.as_str())
            && value["format"].as_str() == Some(target.format.as_str()),
        "symbol module ID mismatch"
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mismatched_symbols_rejected() {
        let target = ModuleIdentity {
            format: "elf_build_id".into(),
            module_id: "aabb".into(),
            binary_sha256: "ff".into(),
            bytes: 1,
        };
        let bad = serde_json::json!({"format":"elf_build_id","module_id":"ccdd"});
        assert!(validate_id(&target, &bad).is_err());
        let right = serde_json::json!({"format":"elf_build_id","module_id":"aabb"});
        assert!(validate_id(&target, &right).is_ok());
    }
}

#[cfg(test)]
mod inventory_tests {
    use super::*;
    #[test]
    fn symbol_id_build_and_inventory_checksum_must_all_match() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        fs::write(root.join("exact.debug"), b"abc").unwrap();
        let target = ModuleIdentity {
            format: "elf".into(),
            module_id: "1234".into(),
            binary_sha256: "artifact".into(),
            bytes: 3,
        };
        let value = serde_json::json!({"schema_version":1,"module_id":"1234","format":"elf","symbols_validated":true,"binary_sha256":"artifact","files":[{"filename":"exact.debug","bytes":3,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}]});
        let manifest = root.join("manifest.json");
        fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(validate_symbols(&target, &manifest).is_ok());
        let mut packaged = value.clone();
        packaged["packaged_binary_sha256s"] =
            serde_json::json!(["archive-signature", "bootstrap-signature"]);
        fs::write(&manifest, serde_json::to_vec(&packaged).unwrap()).unwrap();
        for checksum in ["archive-signature", "bootstrap-signature"] {
            let packaged_target = ModuleIdentity {
                binary_sha256: checksum.into(),
                ..target.clone()
            };
            assert!(validate_symbols(&packaged_target, &manifest).is_ok());
        }
        let unlisted = ModuleIdentity {
            binary_sha256: "unlisted-signature".into(),
            ..target.clone()
        };
        assert!(validate_symbols(&unlisted, &manifest).is_err());
        packaged["packaged_binary_sha256s"] = serde_json::json!(vec!["archive-signature"; 17]);
        fs::write(&manifest, serde_json::to_vec(&packaged).unwrap()).unwrap();
        assert!(validate_symbols(&target, &manifest).is_err());
        fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        fs::write(root.join("exact.debug"), b"abd").unwrap();
        assert!(validate_symbols(&target, &manifest).is_err());
        let mut wrong = target;
        wrong.module_id = "5678".into();
        assert!(validate_symbols(&wrong, &manifest).is_err());
    }
}

#[cfg(test)]
mod cancellation_tests {
    #[test]
    fn disabled_or_cancelled_identity_read_does_not_open_the_file() {
        let error = super::module_identity_guarded(
            std::path::Path::new("/definitely-absent-pioneer-module"),
            || false,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "module_identity_cancelled");
    }
}

#[cfg(test)]
mod large_inventory_tests {
    use super::*;
    #[test]
    fn exact_large_gateway_metadata_fits_finite_inventory_limit() {
        let dwarf = admit_symbol_file(0, 2_969_275_740, Some(2_969_275_740)).unwrap();
        assert_eq!(
            admit_symbol_file(dwarf, 405_387_732, Some(405_387_732)).unwrap(),
            3_374_663_472
        );
        assert!(admit_symbol_file(0, 4_294_967_296, Some(4_294_967_296)).is_ok());
        assert!(admit_symbol_file(1, 4_294_967_296, Some(4_294_967_296)).is_err());
        assert!(admit_symbol_file(u64::MAX, 1, Some(1)).is_err());
        assert!(admit_symbol_file(0, 2_969_275_740, Some(2_969_275_739)).is_err());
    }
    #[test]
    fn bounded_stream_checks_exact_bytes_without_large_allocation() {
        struct Chunks {
            next: usize,
        }
        impl Read for Chunks {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                assert!(buffer.len() <= 65_536);
                if self.next == 3 {
                    return Ok(0);
                }
                buffer[0] = b"abc"[self.next];
                self.next += 1;
                Ok(1)
            }
        }
        let start = std::time::Instant::now();
        assert_eq!(
            symbol_checksum(Chunks { next: 0 }, 3, start).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        // A large admitted metadata size does not allow a truncated stream.
        assert!(symbol_checksum(Chunks { next: 0 }, 2_969_275_740, start).is_err());
        assert!(symbol_checksum(Chunks { next: 0 }, 2, start).is_err());
        assert!(symbol_checksum(Chunks { next: 0 }, 3, start - SYMBOL_CHECKSUM_TIME).is_err());
    }
    #[test]
    fn inventory_still_rejects_unsafe_paths_and_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let target = ModuleIdentity {
            format: "elf".into(),
            module_id: "id".into(),
            binary_sha256: "binary".into(),
            bytes: 3,
        };
        let manifest = temp.path().join("symbols.json");
        for name in [
            "../foreign.debug",
            "/absolute.debug",
            "a/../../foreign.debug",
        ] {
            let value = serde_json::json!({"schema_version":1,"format":"elf","module_id":"id","binary_sha256":"binary","symbols_validated":true,"files":[{"filename":name,"bytes":3,"sha256":"hash"}]});
            fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
            assert!(
                validate_symbols(&target, &manifest)
                    .unwrap_err()
                    .to_string()
                    .contains("invalid symbol filename")
            );
        }
        #[cfg(unix)]
        {
            fs::write(temp.path().join("exact.debug"), b"abc").unwrap();
            std::os::unix::fs::symlink("exact.debug", temp.path().join("link.debug")).unwrap();
            let value = serde_json::json!({"schema_version":1,"format":"elf","module_id":"id","binary_sha256":"binary","symbols_validated":true,"files":[{"filename":"link.debug","bytes":3,"sha256":"hash"}]});
            fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
            assert!(
                validate_symbols(&target, &manifest)
                    .unwrap_err()
                    .to_string()
                    .contains("symlink component")
            );
        }
    }
}
