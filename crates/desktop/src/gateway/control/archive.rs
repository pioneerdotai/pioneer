//! Operation-owned installer extracted from the same verified archive it installs.
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;
const MAX_PACKAGE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_METADATA_BYTES: u64 = 4 * 1024 * 1024;
#[cfg(target_os = "macos")]
const GATEWAY_BUNDLE: &str = "cua/Pioneer Gateway.app";

pub(super) struct ArchiveInstaller {
    pub binary_path: PathBuf,
    pub asset_path: PathBuf,
    pub checksums_path: PathBuf,
    // Retain every extracted dependency until the child installer has exited.
    _directory: TempDir,
}

impl ArchiveInstaller {
    pub fn prepare(directory: &Path, asset_name: &str) -> Result<Self> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("pioneer-installer-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(fs::Permissions::from_mode(0o700));
        }
        let temporary = builder.tempdir()?;
        let asset_path = temporary.path().join(asset_name);
        let checksums_path = temporary.path().join("SHA256SUMS");
        let provenance_path = temporary.path().join("provenance.json");
        // Snapshot all inputs in our private directory so verification, extraction,
        // and the child installer consume exactly the same archive bytes.
        copy_bounded(&directory.join(asset_name), &asset_path, MAX_PACKAGE_BYTES)?;
        copy_bounded(
            &directory.join("SHA256SUMS"),
            &checksums_path,
            MAX_METADATA_BYTES,
        )?;
        copy_bounded(
            &directory.join("provenance.json"),
            &provenance_path,
            MAX_METADATA_BYTES,
        )?;
        let mut asset = File::open(&asset_path)?;
        verify_package(&mut asset, &checksums_path, &provenance_path, asset_name)?;
        asset.rewind()?;
        let runtime = temporary.path().join("runtime");
        fs::create_dir(&runtime)?;
        extract_archive(asset, &runtime)?;
        let binary_path = runtime.join(installer_member());
        validate_runtime(&runtime, &binary_path)?;
        Ok(Self {
            binary_path,
            asset_path,
            checksums_path,
            _directory: temporary,
        })
    }
}

fn copy_bounded(source: &Path, destination: &Path, limit: u64) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("missing embedded Gateway input `{}`", source.display()))?;
    ensure!(
        metadata.is_file() && metadata.len() <= limit,
        "invalid embedded Gateway input"
    );
    let mut input = File::open(source)?.take(limit + 1);
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    ensure!(
        std::io::copy(&mut input, &mut output)? <= limit,
        "embedded Gateway input exceeds size budget"
    );
    Ok(())
}

fn verify_package(
    asset: &mut File,
    checksums: &Path,
    provenance: &Path,
    asset_name: &str,
) -> Result<()> {
    let actual = hash_reader(asset)?;
    let checksum_text = fs::read_to_string(checksums)?;
    // Embedded runtime has one archive. Reject ambiguous or unrelated records.
    let expected_checksum = format!("sha256:{actual} {asset_name}");
    ensure!(
        checksum_text
            .lines()
            .eq(std::iter::once(expected_checksum.as_str())),
        "embedded Gateway checksum mismatch"
    );
    let metadata: Value = serde_json::from_slice(&fs::read(provenance)?)?;
    let own_build: Value = serde_json::from_slice(include_bytes!(concat!(
        env!("OUT_DIR"),
        "/pioneer-build.json"
    )))?;
    let target = target_triple()?;
    ensure!(
        metadata["schema_version"] == 1
            && metadata["stage"] == "gateway-final"
            && metadata["variant"] == "computer-use"
            && metadata["profile"] == "release"
            && metadata["target"].as_str() == Some(target.as_str())
            && metadata["version"] == env!("CARGO_PKG_VERSION")
            && own_build["revision"].as_str().is_some()
            && metadata["commit"] == own_build["revision"]
            && metadata["gateway_asset"] == asset_name
            && metadata["gateway_sha256"].as_str() == Some(actual.as_str()),
        "embedded Gateway provenance mismatch"
    );
    let records = metadata["files"]
        .as_array()
        .context("missing Gateway input inventory")?;
    let archive_records: Vec<_> = records
        .iter()
        .filter(|row| row["path"] == asset_name)
        .collect();
    ensure!(
        archive_records.len() == 1
            && archive_records[0]["sha256"].as_str() == Some(actual.as_str())
            && archive_records[0]["bytes"].as_u64() == Some(asset.metadata()?.len()),
        "embedded Gateway archive is absent from provenance inventory"
    );
    Ok(())
}

fn extract_archive(input: File, runtime: &Path) -> Result<()> {
    let mut archive = zip::ZipArchive::new(input)?;
    ensure!(
        archive.len() <= 16384,
        "Gateway archive contains too many members"
    );
    let mut seen = BTreeSet::new();
    let mut total = 0u64;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index)?;
        let name = entry.name().to_owned();
        let normalized = name.strip_suffix('/').unwrap_or(&name);
        ensure!(
            !normalized.is_empty()
                && !normalized.contains(['\\', ':'])
                && !normalized.chars().any(char::is_control)
                && normalized.split('/').all(safe_component),
            "unsafe Gateway archive path"
        );
        let relative = entry
            .enclosed_name()
            .context("unsafe Gateway archive path")?;
        ensure!(
            seen.insert(normalized.to_lowercase()),
            "duplicate Gateway archive member"
        );
        let mode = entry
            .unix_mode()
            .unwrap_or(if entry.is_dir() { 0o755 } else { 0o644 });
        let kind = mode & 0o170000;
        ensure!(
            (kind == 0
                || if entry.is_dir() {
                    kind == 0o040000
                } else {
                    kind == 0o100000
                })
                && mode & 0o7000 == 0
                && (!cfg!(unix) || mode & 0o022 == 0),
            "unsafe Gateway archive member type or permissions"
        );
        // CU packages contain one platform-specific executable and its CUA tree.
        // macOS retains the entire signed bundle at its original relative paths.
        ensure!(
            normalized == "cua"
                || normalized.starts_with("cua/")
                || (!cfg!(target_os = "macos") && normalized == installer_member()),
            "unexpected Gateway archive member"
        );
        let destination = runtime.join(relative);
        if entry.is_dir() {
            ensure!(entry.size() == 0, "invalid Gateway archive directory");
            fs::create_dir_all(destination)?;
            continue;
        }
        total = total
            .checked_add(entry.size())
            .context("Gateway archive size overflow")?;
        ensure!(
            entry.size() <= MAX_FILE_BYTES && total <= MAX_PACKAGE_BYTES,
            "Gateway archive exceeds payload budget"
        );
        fs::create_dir_all(
            destination
                .parent()
                .context("archive member has no parent")?,
        )?;
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&destination)?;
        let size = entry.size();
        let copied = std::io::copy(&mut (&mut entry).take(size + 1), &mut output)?;
        ensure!(copied == size, "Gateway archive member length mismatch");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&destination, fs::Permissions::from_mode(mode & 0o777))?;
        }
    }
    Ok(())
}

fn safe_component(component: &str) -> bool {
    if component.is_empty() || component.ends_with(['.', ' ']) {
        return false;
    }
    // Windows normalizes trailing dots/spaces and treats device names specially,
    // even when they have extensions. Reject those aliases on every platform.
    let stem = component
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    !matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        && !(stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
}

fn installer_member() -> &'static str {
    if cfg!(target_os = "macos") {
        "cua/Pioneer Gateway.app/Contents/MacOS/pioneer"
    } else if cfg!(windows) {
        "pioneer.exe"
    } else {
        "pioneer"
    }
}

fn validate_runtime(runtime: &Path, binary: &Path) -> Result<()> {
    validate_architecture(binary)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            fs::metadata(binary)?.permissions().mode() & 0o100 != 0,
            "Gateway installer is not executable"
        );
    }
    let payload = runtime.join("cua");
    let daemon = if cfg!(target_os = "macos") {
        "Pioneer Gateway.app/Contents/Helpers/cua-driver"
    } else if cfg!(windows) {
        "cua-driver.exe"
    } else {
        "cua-driver"
    };
    let manifest_path = payload.join("manifest.json");
    ensure!(
        fs::metadata(&manifest_path)?.len() <= MAX_METADATA_BYTES,
        "Gateway CUA manifest exceeds size budget"
    );
    let manifest: Value = serde_json::from_slice(&fs::read(manifest_path)?)?;
    let daemon_path = payload.join(daemon);
    let daemon_hash = hash_reader(&mut File::open(&daemon_path)?)?;
    let target = target_triple()?;
    ensure!(
        manifest["schema_version"] == 1
            && manifest["repository"] == "https://github.com/trycua/cua"
            && manifest["target"].as_str() == Some(target.as_str())
            && manifest["daemon"] == daemon
            && manifest["permission_owner"] == "gateway"
            && manifest["sha256"].as_str() == Some(daemon_hash.as_str())
            && payload.join("LICENSE.cua-driver.md").is_file()
            && payload.join("THIRD_PARTY_NOTICES.md").is_file(),
        "embedded Gateway CUA identity or contents mismatch"
    );
    validate_architecture(&daemon_path)?;
    #[cfg(windows)]
    validate_architecture(&payload.join("cua-driver-uia.exe"))?;
    #[cfg(target_os = "macos")]
    {
        let bundle = runtime.join(GATEWAY_BUNDLE);
        ensure!(
            bundle.join("Contents/Info.plist").is_file(),
            "Gateway bundle metadata missing"
        );
        // Verification reads the signed bundle without changing its contents.
        let status = std::process::Command::new("/usr/bin/codesign")
            .args(["--verify", "--deep", "--strict"])
            .arg(&bundle)
            .status()
            .context("failed to verify extracted Gateway bundle signature")?;
        ensure!(
            status.success(),
            "extracted Gateway bundle signature is invalid"
        );
    }
    Ok(())
}

fn target_triple() -> Result<String> {
    let os = match std::env::consts::OS {
        "macos" => "apple-darwin",
        "windows" => "pc-windows-msvc",
        "linux" => "unknown-linux-gnu",
        _ => bail!("unsupported Gateway installer platform"),
    };
    ensure!(
        matches!(std::env::consts::ARCH, "x86_64" | "aarch64"),
        "unsupported Gateway installer architecture"
    );
    Ok(format!("{}-{os}", std::env::consts::ARCH))
}

fn hash_reader(reader: &mut impl Read) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let length = reader.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        hash.update(&buffer[..length]);
    }
    Ok(hex::encode(hash.finalize()))
}

fn validate_architecture(path: &Path) -> Result<()> {
    let mut input = File::open(path)?;
    let mut header = [0u8; 64];
    input.read_exact(&mut header)?;
    let arm = cfg!(target_arch = "aarch64");
    let valid = if cfg!(target_os = "macos") {
        let cpu = if arm { 0x0100000c } else { 0x01000007 };
        match &header[..4] {
            [0xcf, 0xfa, 0xed, 0xfe] => u32::from_le_bytes(header[4..8].try_into()?) == cpu,
            [0xfe, 0xed, 0xfa, 0xcf] => u32::from_be_bytes(header[4..8].try_into()?) == cpu,
            [0xca, 0xfe, 0xba, magic @ (0xbe | 0xbf)] => {
                let count = u32::from_be_bytes(header[4..8].try_into()?) as usize;
                ensure!(
                    count > 0 && count <= 16,
                    "invalid Gateway Mach-O architecture count"
                );
                let width = if *magic == 0xbf { 32 } else { 20 };
                let mut table = [0u8; 16 * 32];
                input.seek(std::io::SeekFrom::Start(8))?;
                input.read_exact(&mut table[..count * width])?;
                table[..count * width].chunks_exact(width).any(|entry| {
                    u32::from_be_bytes([entry[0], entry[1], entry[2], entry[3]]) == cpu
                })
            }
            _ => false,
        }
    } else if cfg!(target_os = "linux") {
        header[..6] == [0x7f, b'E', b'L', b'F', 2, 1]
            && u16::from_le_bytes(header[18..20].try_into()?) == if arm { 183 } else { 62 }
    } else if cfg!(windows) {
        let offset = u32::from_le_bytes(header[60..64].try_into()?);
        ensure!(
            offset as u64 <= MAX_FILE_BYTES - 6,
            "invalid Gateway PE header offset"
        );
        input.seek(std::io::SeekFrom::Start(offset as u64))?;
        let mut pe = [0u8; 6];
        input.read_exact(&mut pe)?;
        header[..2] == *b"MZ"
            && pe[..4] == [b'P', b'E', 0, 0]
            && u16::from_le_bytes(pe[4..6].try_into()?) == if arm { 0xaa64 } else { 0x8664 }
    } else {
        false
    };
    ensure!(
        valid,
        "Gateway executable architecture mismatch: {}",
        path.display()
    );
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::{ArchiveInstaller, extract_archive, installer_member};
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::Path;
    use zip::write::SimpleFileOptions;

    fn write_zip(path: &Path, entries: &[(&str, &[u8])]) {
        let mut archive = zip::ZipWriter::new(File::create(path).unwrap());
        for (name, bytes) in entries {
            archive
                .start_file(*name, SimpleFileOptions::default().unix_permissions(0o755))
                .unwrap();
            archive.write_all(bytes).unwrap();
        }
        archive.finish().unwrap();
    }

    #[test]
    fn extraction_preserves_executable_and_companion_bytes_in_package_layout() {
        let temporary = tempfile::tempdir().unwrap();
        let archive = temporary.path().join("gateway.zip");
        write_zip(
            &archive,
            &[
                (installer_member(), b"gateway bytes"),
                (
                    "cua/Pioneer Gateway.app/Contents/Frameworks/libmodel.dylib",
                    b"library bytes",
                ),
                ("cua/THIRD_PARTY_NOTICES.md", b"notice bytes"),
            ],
        );
        let runtime = temporary.path().join("runtime");
        fs::create_dir(&runtime).unwrap();
        extract_archive(File::open(archive).unwrap(), &runtime).unwrap();
        assert_eq!(
            fs::read(runtime.join(installer_member())).unwrap(),
            b"gateway bytes"
        );
        assert_eq!(
            fs::read(runtime.join("cua/Pioneer Gateway.app/Contents/Frameworks/libmodel.dylib"))
                .unwrap(),
            b"library bytes"
        );
        assert_eq!(
            fs::read(runtime.join("cua/THIRD_PARTY_NOTICES.md")).unwrap(),
            b"notice bytes"
        );
    }

    #[test]
    fn extraction_rejects_unsafe_paths_case_aliases_and_links() {
        for name in [
            "../escaped",
            "/escaped",
            "cua/../escaped",
            "cua/./escaped",
            "cua/a\\b",
            "cua/a:stream",
            "cua/CON.txt",
            "cua/file.",
            "cua/file ",
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let archive = temporary.path().join("gateway.zip");
            write_zip(&archive, &[(name, b"untrusted")]);
            let runtime = temporary.path().join("runtime");
            fs::create_dir(&runtime).unwrap();
            let error = extract_archive(File::open(&archive).unwrap(), &runtime).unwrap_err();
            assert!(
                error.to_string().contains("unsafe Gateway archive path"),
                "{name}: {error:#}"
            );
            assert!(!temporary.path().join("escaped").exists());
        }
        let temporary = tempfile::tempdir().unwrap();
        let archive = temporary.path().join("gateway.zip");
        write_zip(
            &archive,
            &[
                ("cua/manifest.json", b"first"),
                ("cua/MANIFEST.json", b"second"),
            ],
        );
        let runtime = temporary.path().join("runtime");
        fs::create_dir(&runtime).unwrap();
        let error = extract_archive(File::open(&archive).unwrap(), &runtime).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate Gateway archive member")
        );
        assert_eq!(
            fs::read(runtime.join("cua/manifest.json")).unwrap(),
            b"first"
        );

        let linked_archive = temporary.path().join("linked.zip");
        let mut writer = zip::ZipWriter::new(File::create(&linked_archive).unwrap());
        writer
            .add_symlink("cua/link", "../../escaped", SimpleFileOptions::default())
            .unwrap();
        writer.finish().unwrap();
        let error = extract_archive(File::open(linked_archive).unwrap(), &runtime).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsafe Gateway archive member type or permissions")
        );
        assert!(!runtime.join("cua/link").exists());
    }

    #[cfg(not(target_os = "macos"))]
    fn prepared_fixture(directory: &Path) -> String {
        use sha2::{Digest, Sha256};
        let asset_name = "gateway.zip";
        let mut native = vec![0u8; 128];
        let arm = cfg!(target_arch = "aarch64");
        if cfg!(windows) {
            native[..2].copy_from_slice(b"MZ");
            native[60..64].copy_from_slice(&64u32.to_le_bytes());
            native[64..68].copy_from_slice(b"PE\0\0");
            native[68..70]
                .copy_from_slice(&(if arm { 0xaa64u16 } else { 0x8664u16 }).to_le_bytes());
        } else {
            native[..6].copy_from_slice(&[0x7f, b'E', b'L', b'F', 2, 1]);
            native[18..20].copy_from_slice(&(if arm { 183u16 } else { 62u16 }).to_le_bytes());
        }
        let target = format!(
            "{}-{}",
            std::env::consts::ARCH,
            if cfg!(windows) {
                "pc-windows-msvc"
            } else {
                "unknown-linux-gnu"
            }
        );
        let daemon_name = if cfg!(windows) {
            "cua-driver.exe"
        } else {
            "cua-driver"
        };
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "repository": "https://github.com/trycua/cua",
            "target": target, "daemon": daemon_name, "permission_owner": "gateway",
            "sha256": hex::encode(Sha256::digest(&native))
        }))
        .unwrap();
        let daemon_path = format!("cua/{daemon_name}");
        let mut members = vec![
            (installer_member(), native.as_slice()),
            (daemon_path.as_str(), native.as_slice()),
            ("cua/manifest.json", manifest.as_slice()),
            ("cua/LICENSE.cua-driver.md", b"license".as_slice()),
            ("cua/THIRD_PARTY_NOTICES.md", b"notices".as_slice()),
        ];
        if cfg!(windows) {
            members.push(("cua/cua-driver-uia.exe", native.as_slice()));
        }
        let asset = directory.join(asset_name);
        write_zip(&asset, &members);
        let hash = hex::encode(Sha256::digest(fs::read(&asset).unwrap()));
        fs::write(
            directory.join("SHA256SUMS"),
            format!("sha256:{hash} {asset_name}\n"),
        )
        .unwrap();
        let build: serde_json::Value = serde_json::from_slice(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/pioneer-build.json"
        )))
        .unwrap();
        fs::write(directory.join("provenance.json"), serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "stage": "gateway-final", "variant": "computer-use", "profile": "release",
            "target": target, "version": env!("CARGO_PKG_VERSION"), "commit": build["revision"],
            "gateway_asset": asset_name, "gateway_sha256": hash,
            "files": [{ "path": asset_name, "sha256": hash, "bytes": fs::metadata(asset).unwrap().len() }]
        })).unwrap()).unwrap();
        asset_name.to_owned()
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn prepared_installer_accepts_crlf_and_owns_snapshot_until_drop() {
        let inputs = tempfile::tempdir().unwrap();
        let asset_name = prepared_fixture(inputs.path());
        let checksums = inputs.path().join("SHA256SUMS");
        fs::write(
            &checksums,
            fs::read_to_string(&checksums)
                .unwrap()
                .replace('\n', "\r\n"),
        )
        .unwrap();
        let installer = ArchiveInstaller::prepare(inputs.path(), &asset_name).unwrap();
        let snapshot = installer.asset_path.clone();
        let binary = installer.binary_path.clone();
        let original = fs::read(&snapshot).unwrap();
        fs::write(inputs.path().join(&asset_name), b"replaced package").unwrap();
        assert_eq!(fs::read(&snapshot).unwrap(), original);
        assert!(binary.is_file());
        drop(installer);
        assert!(!snapshot.exists());
        assert!(!binary.exists());
        let error = ArchiveInstaller::prepare(inputs.path(), &asset_name)
            .err()
            .expect("changed package must be rejected");
        assert!(error.to_string().contains("checksum mismatch"));
    }

    #[cfg(not(target_os = "macos"))]
    #[test]
    fn package_with_matching_checksum_but_different_provenance_is_rejected() {
        let inputs = tempfile::tempdir().unwrap();
        let asset_name = prepared_fixture(inputs.path());
        let metadata_path = inputs.path().join("provenance.json");
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        for (field, value) in [
            ("target", "other-target"),
            ("commit", "other-commit"),
            ("variant", "default"),
            ("gateway_sha256", "unrelated-digest"),
        ] {
            let original = metadata[field].clone();
            metadata[field] = value.into();
            fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap()).unwrap();
            let error = ArchiveInstaller::prepare(inputs.path(), &asset_name)
                .err()
                .expect("mismatched provenance must fail");
            assert!(
                error.to_string().contains("provenance mismatch"),
                "{field}: {error:#}"
            );
            metadata[field] = original;
        }
    }
}
