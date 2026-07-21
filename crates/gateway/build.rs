use pioneer_protocol::SkillId;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BundledSkillsManifest {
    version: u32,
    skills: Vec<BundledSkillsManifestEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BundledSkillsManifestEntry {
    skill_id: String,
    owner: String,
    slug: String,
    resource_path: String,
}

struct ValidatedBundledSkill {
    skill_id: String,
    owner: String,
    slug: String,
    resource_path: String,
    absolute_path: PathBuf,
    files: Vec<BundledFile>,
}

struct BundledFile {
    relative_path: String,
    absolute_path: String,
    unix_mode: u32,
}

fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is missing"));
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("gateway crate must live under <repo>/crates/gateway");
    let skills_root = repo_root.join("resources").join("skills");
    let bundled_manifest_path = skills_root.join("bundled-system-skills.toml");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is missing"));
    let generated_path = out_dir.join("bundled_system_skills.rs");

    println!("cargo:rerun-if-changed={}", skills_root.display());
    println!("cargo:rerun-if-changed={}", bundled_manifest_path.display());

    let manifest_bytes = fs::read(bundled_manifest_path.as_path()).unwrap_or_else(|error| {
        panic!(
            "failed to read bundled skills manifest {}: {error}",
            bundled_manifest_path.display()
        )
    });
    let manifest_text = std::str::from_utf8(manifest_bytes.as_slice()).unwrap_or_else(|error| {
        panic!(
            "bundled skills manifest {} is not UTF-8: {error}",
            bundled_manifest_path.display()
        )
    });
    let manifest: BundledSkillsManifest = toml::from_str(manifest_text).unwrap_or_else(|error| {
        panic!(
            "failed to parse bundled skills manifest {}: {error}",
            bundled_manifest_path.display()
        )
    });
    let skills = validate_manifest(manifest, skills_root.as_path()).unwrap_or_else(|error| {
        panic!(
            "invalid bundled skills manifest {}: {error}",
            bundled_manifest_path.display()
        )
    });

    for skill in &skills {
        println!("cargo:rerun-if-changed={}", skill.absolute_path.display());
        for file in &skill.files {
            println!("cargo:rerun-if-changed={}", file.absolute_path);
        }
    }

    let generated = generate_embedded_source(
        skills.as_slice(),
        bundled_manifest_path.as_path(),
        manifest_bytes.as_slice(),
    );
    fs::write(generated_path.as_path(), generated).unwrap_or_else(|error| {
        panic!(
            "failed to write bundled system skills source {}: {error}",
            generated_path.display()
        )
    });
}

fn validate_manifest(
    manifest: BundledSkillsManifest,
    skills_root: &Path,
) -> Result<Vec<ValidatedBundledSkill>, String> {
    if manifest.version != 1 {
        return Err(format!(
            "unsupported version {}; expected 1",
            manifest.version
        ));
    }
    if manifest.skills.is_empty() {
        return Err("manifest must contain at least one skill".to_owned());
    }

    let mut skill_ids = BTreeSet::new();
    let mut resource_paths = BTreeSet::new();
    let mut validated = Vec::with_capacity(manifest.skills.len());

    for entry in manifest.skills {
        let skill_id = SkillId::new(entry.skill_id.clone())
            .map_err(|error| format!("invalid skill_id `{}`: {error}", entry.skill_id))?;
        if !skill_ids.insert(skill_id.to_string()) {
            return Err(format!("duplicate skill_id `{skill_id}`"));
        }
        validate_manifest_segment("owner", entry.owner.as_str())?;
        validate_manifest_segment("slug", entry.slug.as_str())?;
        validate_resource_path(entry.resource_path.as_str())?;
        if !resource_paths.insert(entry.resource_path.clone()) {
            return Err(format!(
                "resource package `{}` is referenced more than once",
                entry.resource_path
            ));
        }

        let absolute_path = skills_root.join(entry.resource_path.as_str());
        if !absolute_path.is_dir() {
            return Err(format!(
                "resource package `{}` is missing or is not a directory",
                entry.resource_path
            ));
        }
        if !absolute_path.join("SKILL.md").is_file() {
            return Err(format!(
                "resource package `{}` does not contain SKILL.md",
                entry.resource_path
            ));
        }

        let mut files = Vec::new();
        collect_package_files(absolute_path.as_path(), absolute_path.as_path(), &mut files)?;
        files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        validated.push(ValidatedBundledSkill {
            skill_id: skill_id.to_string(),
            owner: entry.owner,
            slug: entry.slug,
            resource_path: entry.resource_path,
            absolute_path,
            files,
        });
    }

    let discovered = discover_bundled_packages(skills_root)?;
    if discovered != resource_paths {
        let missing = discovered
            .difference(&resource_paths)
            .cloned()
            .collect::<Vec<_>>();
        let unknown = resource_paths
            .difference(&discovered)
            .cloned()
            .collect::<Vec<_>>();
        return Err(format!(
            "manifest/package mismatch; missing entries: {missing:?}; unknown entries: {unknown:?}"
        ));
    }

    validated.sort_by(|left, right| left.resource_path.cmp(&right.resource_path));
    Ok(validated)
}

fn validate_manifest_segment(field: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.trim() != value {
        return Err(format!("{field} must be a non-empty trimmed path segment"));
    }
    if value.contains('/') || value.contains('\\') || matches!(value, "." | "..") {
        return Err(format!("invalid {field} path segment `{value}`"));
    }
    let mut components = Path::new(value).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err(format!("invalid {field} path segment `{value}`"));
    }
    Ok(())
}

fn validate_resource_path(value: &str) -> Result<(), String> {
    if value.is_empty() || value.trim() != value || value.contains('\\') {
        return Err(format!("invalid resource_path `{value}`"));
    }
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "resource_path `{value}` must be relative and cannot escape the skills root"
        ));
    }
    Ok(())
}

fn discover_bundled_packages(skills_root: &Path) -> Result<BTreeSet<String>, String> {
    let bundled_root = skills_root.join("pioneer");
    let entries = fs::read_dir(bundled_root.as_path()).map_err(|error| {
        format!(
            "failed to read bundled package root {}: {error}",
            bundled_root.display()
        )
    })?;
    let mut packages = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "failed to read entry in bundled package root {}: {error}",
                bundled_root.display()
            )
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to read file type for {}: {error}", path.display()))?;
        if file_type.is_dir() && path.join("SKILL.md").is_file() {
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| format!("non-UTF-8 bundled package path {}", path.display()))?;
            packages.insert(format!("pioneer/{name}"));
        }
    }
    Ok(packages)
}

fn collect_package_files(
    root: &Path,
    dir: &Path,
    files: &mut Vec<BundledFile>,
) -> Result<(), String> {
    let entries = fs::read_dir(dir)
        .map_err(|error| format!("failed to read directory {}: {error}", dir.display()))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| format!("failed to read entry in {}: {error}", dir.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to read file type for {}: {error}", path.display()))?;
        if file_type.is_dir() {
            collect_package_files(root, path.as_path(), files)?;
        } else if file_type.is_file() {
            let metadata = entry.metadata().map_err(|error| {
                format!("failed to read metadata for {}: {error}", path.display())
            })?;
            let relative_path = portable_relative_path(root, path.as_path())?;
            files.push(BundledFile {
                relative_path,
                absolute_path: path.display().to_string(),
                unix_mode: file_unix_mode(&metadata),
            });
        } else {
            return Err(format!(
                "bundled package contains unsupported filesystem entry {}",
                path.display()
            ));
        }
    }
    Ok(())
}

fn portable_relative_path(root: &Path, path: &Path) -> Result<String, String> {
    path.strip_prefix(root)
        .map_err(|error| {
            format!(
                "failed to compute bundled skill relative path for {}: {error}",
                path.display()
            )
        })?
        .components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("non-UTF-8 bundled file path {}", path.display()))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|components| components.join("/"))
}

fn generate_embedded_source(
    skills: &[ValidatedBundledSkill],
    manifest_path: &Path,
    manifest_bytes: &[u8],
) -> String {
    let mut generated = String::new();
    generated.push_str("const BUNDLED_SKILLS_MANIFEST_BYTES: &[u8] = include_bytes!(");
    generated.push_str(&rust_string_literal(
        manifest_path.display().to_string().as_str(),
    ));
    generated.push_str(");\n");
    generated.push_str("const BUNDLED_SYSTEM_SKILLS: &[BundledSystemSkillRecord] = &[\n");
    for skill in skills {
        generated.push_str("    BundledSystemSkillRecord {\n");
        generated.push_str(&format!(
            "        skill_id: {},\n",
            rust_string_literal(skill.skill_id.as_str())
        ));
        generated.push_str(&format!(
            "        owner: {},\n",
            rust_string_literal(skill.owner.as_str())
        ));
        generated.push_str(&format!(
            "        slug: {},\n",
            rust_string_literal(skill.slug.as_str())
        ));
        generated.push_str(&format!(
            "        resource_path: {},\n",
            rust_string_literal(skill.resource_path.as_str())
        ));
        generated.push_str("        files: &[\n");
        for file in &skill.files {
            generated.push_str("            BundledSystemSkillFile {\n");
            generated.push_str(&format!(
                "                relative_path: {},\n",
                rust_string_literal(file.relative_path.as_str())
            ));
            generated.push_str("                bytes: include_bytes!(");
            generated.push_str(&rust_string_literal(file.absolute_path.as_str()));
            generated.push_str("),\n");
            generated.push_str(&format!(
                "                unix_mode: {:#o},\n",
                file.unix_mode
            ));
            generated.push_str("            },\n");
        }
        generated.push_str("        ],\n");
        generated.push_str("    },\n");
    }
    generated.push_str("];\n");

    assert_eq!(
        manifest_bytes,
        fs::read(manifest_path).expect("manifest should remain readable during build")
    );
    generated
}

#[cfg(unix)]
fn file_unix_mode(metadata: &fs::Metadata) -> u32 {
    metadata.permissions().mode() & 0o777
}

#[cfg(not(unix))]
fn file_unix_mode(_metadata: &fs::Metadata) -> u32 {
    0o644
}

fn rust_string_literal(value: &str) -> String {
    format!("{value:?}")
}
