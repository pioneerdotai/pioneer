use crate::{
    plan::DesktopUpdatePlan,
    platform::{PlatformApplyOutcome, PlatformRelaunch},
};
use anyhow::{Context as _, Result, anyhow, bail};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    fs, io,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};
use zip::ZipArchive;

const MACOS_ASSET_KIND: &str = "macos_app_zip";
const APP_BUNDLE_NAME: &str = "Pioneer.app";
const EXTRACT_DIR_NAME: &str = "extract";

pub fn apply(plan: &DesktopUpdatePlan, plan_path: &Path) -> Result<PlatformApplyOutcome> {
    require_macos_plan(plan)?;

    let staging_dir = plan_path
        .parent()
        .ok_or_else(|| anyhow!("desktop update plan path has no parent"))?
        .join(EXTRACT_DIR_NAME);
    recreate_dir(staging_dir.as_path())?;

    let staged_app = extract_app_zip(plan.asset_path.as_path(), staging_dir.as_path())?;
    validate_staged_app(staged_app.as_path(), plan.target_version.as_str())?;

    let install_root = plan.install_root_path.as_path();
    let rollback_path = rollback_path_for_install_root(install_root);
    replace_app_bundle(install_root, staged_app.as_path(), rollback_path.as_path())?;

    let _ = fs::remove_dir_all(rollback_path.as_path());
    Ok(PlatformApplyOutcome {
        result_details: None,
        relaunch: Some(PlatformRelaunch::new(
            "open",
            [install_root.as_os_str().to_owned()],
        )),
    })
}

fn replace_app_bundle(install_root: &Path, staged_app: &Path, rollback_path: &Path) -> Result<()> {
    fs::rename(install_root, rollback_path).with_context(|| {
        format!(
            "failed to move current app `{}` to rollback path `{}`",
            install_root.display(),
            rollback_path.display()
        )
    })?;

    match fs::rename(staged_app, install_root) {
        Ok(()) => Ok(()),
        Err(error) => {
            let rollback_result = fs::rename(rollback_path, install_root);
            if let Err(rollback_error) = rollback_result {
                return Err(anyhow!(
                    "failed to move staged app into place: {error}; rollback also failed: {rollback_error}"
                ));
            }
            Err(anyhow!("failed to move staged app into place: {error}"))
        }
    }
}

fn require_macos_plan(plan: &DesktopUpdatePlan) -> Result<()> {
    if plan.asset_kind != MACOS_ASSET_KIND {
        bail!(
            "macOS desktop update requires asset kind `{MACOS_ASSET_KIND}`, got `{}`",
            plan.asset_kind
        );
    }
    validate_install_root(plan.install_root_path.as_path())
}

fn validate_install_root(install_root: &Path) -> Result<()> {
    if install_root
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == APP_BUNDLE_NAME)
    {
        return Ok(());
    }

    bail!("macOS desktop update install root must end in `{APP_BUNDLE_NAME}`")
}

fn extract_app_zip(asset_path: &Path, staging_dir: &Path) -> Result<PathBuf> {
    let file = fs::File::open(asset_path)
        .with_context(|| format!("failed to open macOS app zip `{}`", asset_path.display()))?;
    let mut archive = ZipArchive::new(file).context("failed to read macOS app zip")?;

    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .with_context(|| format!("failed to read macOS app zip entry {index}"))?;
        let enclosed_name = entry
            .enclosed_name()
            .ok_or_else(|| anyhow!("macOS app zip contains an unsafe path"))?;
        let out_path = staging_dir.join(enclosed_name);

        if entry.is_dir() {
            fs::create_dir_all(out_path.as_path()).with_context(|| {
                format!(
                    "failed to create macOS app zip directory `{}`",
                    out_path.display()
                )
            })?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create macOS app zip parent `{}`",
                    parent.display()
                )
            })?;
        }

        let mut out_file = fs::File::create(out_path.as_path()).with_context(|| {
            format!(
                "failed to create extracted macOS app file `{}`",
                out_path.display()
            )
        })?;
        io::copy(&mut entry, &mut out_file).with_context(|| {
            format!("failed to extract macOS app file `{}`", out_path.display())
        })?;
        apply_zip_entry_permissions(&entry, out_path.as_path())?;
    }

    let staged_app = staging_dir.join(APP_BUNDLE_NAME);
    if !staged_app.is_dir() {
        bail!("macOS app zip did not contain `{APP_BUNDLE_NAME}` at archive root");
    }
    Ok(staged_app)
}

fn apply_zip_entry_permissions(
    entry: &zip::read::ZipFile<'_, fs::File>,
    path: &Path,
) -> Result<()> {
    #[cfg(unix)]
    if let Some(mode) = entry.unix_mode() {
        fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o7777)).with_context(
            || {
                format!(
                    "failed to set extracted macOS app file permissions on `{}`",
                    path.display()
                )
            },
        )?;
    }

    #[cfg(not(unix))]
    let _ = (entry, path);

    Ok(())
}

fn validate_staged_app(staged_app: &Path, target_version: &str) -> Result<()> {
    if !staged_app.is_dir() {
        bail!("staged macOS app bundle is missing");
    }

    let info_plist = staged_app.join("Contents").join("Info.plist");
    let version = read_info_plist_short_version(info_plist.as_path()).with_context(|| {
        format!(
            "failed to validate staged macOS app version from `{}`",
            info_plist.display()
        )
    })?;

    if version != target_version {
        bail!(
            "staged macOS app version `{version}` does not match target version `{target_version}`"
        );
    }

    Ok(())
}

fn read_info_plist_short_version(path: &Path) -> Result<String> {
    if let Ok(output) = Command::new("plutil")
        .args(["-extract", "CFBundleShortVersionString", "raw", "-o", "-"])
        .arg(path)
        .output()
    {
        if output.status.success() {
            let version = String::from_utf8_lossy(output.stdout.as_slice())
                .trim()
                .to_owned();
            if !version.is_empty() {
                return Ok(version);
            }
        }
    }

    let content = fs::read_to_string(path)?;
    parse_info_plist_short_version_xml(content.as_str())
        .ok_or_else(|| anyhow!("CFBundleShortVersionString not found"))
}

fn parse_info_plist_short_version_xml(content: &str) -> Option<String> {
    let key_index = content.find("<key>CFBundleShortVersionString</key>")?;
    let after_key = &content[key_index..];
    let string_start = after_key.find("<string>")? + "<string>".len();
    let after_string_start = &after_key[string_start..];
    let string_end = after_string_start.find("</string>")?;
    Some(after_string_start[..string_end].trim().to_owned()).filter(|version| !version.is_empty())
}

fn recreate_dir(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to clear staging directory `{}`", path.display())
            });
        }
    }

    fs::create_dir_all(path)
        .with_context(|| format!("failed to create staging directory `{}`", path.display()))
}

fn rollback_path_for_install_root(install_root: &Path) -> PathBuf {
    let preferred = install_root.with_file_name(format!("{APP_BUNDLE_NAME}.previous"));
    if !preferred.exists() {
        return preferred;
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    install_root.with_file_name(format!(
        "{APP_BUNDLE_NAME}.previous-{}-{timestamp}",
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        APP_BUNDLE_NAME, extract_app_zip, parse_info_plist_short_version_xml, replace_app_bundle,
        validate_install_root, validate_staged_app,
    };
    use std::{fs, io::Write as _, os::unix::fs::PermissionsExt as _};
    use zip::{ZipWriter, write::SimpleFileOptions};

    #[test]
    fn macos_rejects_non_pioneer_install_root() {
        let error =
            validate_install_root(std::path::Path::new("/Applications/Other.app")).unwrap_err();

        assert!(error.to_string().contains(APP_BUNDLE_NAME));
    }

    #[test]
    fn macos_validates_staged_app_version() {
        let temp_dir = tempfile::tempdir().unwrap();
        let app = create_staged_app(temp_dir.path(), "0.26.0");

        validate_staged_app(app.as_path(), "0.26.0").unwrap();
    }

    #[test]
    fn macos_rejects_staged_version_mismatch() {
        let temp_dir = tempfile::tempdir().unwrap();
        let app = create_staged_app(temp_dir.path(), "0.25.0");

        let error = validate_staged_app(app.as_path(), "0.26.0").unwrap_err();

        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn macos_parses_info_plist_xml_version() {
        let content = r#"
            <plist>
              <dict>
                <key>CFBundleShortVersionString</key>
                <string>0.26.0</string>
              </dict>
            </plist>
        "#;

        assert_eq!(
            parse_info_plist_short_version_xml(content),
            Some("0.26.0".to_owned())
        );
    }

    #[test]
    fn macos_extract_preserves_executable_permissions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let zip_path = temp_dir.path().join("Pioneer-aarch64.app.zip");
        write_minimal_app_zip(zip_path.as_path());

        let staged_app = extract_app_zip(
            zip_path.as_path(),
            temp_dir.path().join("extract").as_path(),
        )
        .unwrap();
        let executable = staged_app
            .join("Contents")
            .join("MacOS")
            .join("pioneer-app");

        let mode = fs::metadata(executable).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
    }

    #[test]
    fn macos_rolls_back_when_staged_move_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let install_root = temp_dir.path().join(APP_BUNDLE_NAME);
        let rollback_path = temp_dir.path().join("Pioneer.app.previous");
        fs::create_dir_all(install_root.join("Contents")).unwrap();

        let error = replace_app_bundle(
            install_root.as_path(),
            temp_dir.path().join("missing-staged.app").as_path(),
            rollback_path.as_path(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("failed to move staged app"));
        assert!(install_root.is_dir());
        assert!(!rollback_path.exists());
    }

    fn write_minimal_app_zip(path: &std::path::Path) {
        let file = fs::File::create(path).unwrap();
        let mut zip = ZipWriter::new(file);
        let dir_options = SimpleFileOptions::default().unix_permissions(0o755);
        let file_options = SimpleFileOptions::default().unix_permissions(0o644);
        let executable_options = SimpleFileOptions::default().unix_permissions(0o755);

        zip.add_directory("Pioneer.app/", dir_options).unwrap();
        zip.add_directory("Pioneer.app/Contents/", dir_options)
            .unwrap();
        zip.add_directory("Pioneer.app/Contents/MacOS/", dir_options)
            .unwrap();
        zip.add_directory("Pioneer.app/Contents/Resources/", dir_options)
            .unwrap();
        zip.start_file("Pioneer.app/Contents/MacOS/pioneer-app", executable_options)
            .unwrap();
        zip.write_all(b"#!/bin/sh\n").unwrap();
        zip.start_file("Pioneer.app/Contents/Info.plist", file_options)
            .unwrap();
        zip.write_all(
            br#"
            <plist>
              <dict>
                <key>CFBundleShortVersionString</key>
                <string>0.26.0</string>
              </dict>
            </plist>
            "#,
        )
        .unwrap();
        zip.finish().unwrap();
    }

    fn create_staged_app(root: &std::path::Path, version: &str) -> std::path::PathBuf {
        let app = root.join(APP_BUNDLE_NAME);
        let contents = app.join("Contents");
        fs::create_dir_all(contents.as_path()).unwrap();
        fs::write(
            contents.join("Info.plist"),
            format!(
                r#"<plist><dict><key>CFBundleShortVersionString</key><string>{version}</string></dict></plist>"#
            ),
        )
        .unwrap();
        app
    }
}
