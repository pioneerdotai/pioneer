use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

fn main() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is missing"));
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("gateway crate must live under <repo>/crates/gateway");
    let skills_root = repo_root.join("resources").join("skills");
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is missing"));
    let manifest_path = out_dir.join("bundled_system_skills.rs");

    println!("cargo:rerun-if-changed={}", skills_root.display());

    let mut files = Vec::new();
    if skills_root.exists() {
        collect_files(skills_root.as_path(), skills_root.as_path(), &mut files);
    }
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));

    let mut generated =
        String::from("const BUNDLED_SYSTEM_SKILL_FILES: &[BundledSystemSkillFile] = &[\n");
    for file in &files {
        println!("cargo:rerun-if-changed={}", file.absolute_path);
        generated.push_str("    BundledSystemSkillFile {\n");
        generated.push_str("        relative_path: ");
        generated.push_str(&rust_string_literal(file.relative_path.as_str()));
        generated.push_str(",\n");
        generated.push_str("        bytes: include_bytes!(");
        generated.push_str(&rust_string_literal(file.absolute_path.as_str()));
        generated.push_str("),\n");
        generated.push_str("        unix_mode: ");
        generated.push_str(&format!("{:#o}", file.unix_mode));
        generated.push_str(",\n");
        generated.push_str("    },\n");
    }
    generated.push_str("];\n");

    fs::write(manifest_path.as_path(), generated).unwrap_or_else(|error| {
        panic!(
            "failed to write bundled system skills manifest {}: {error}",
            manifest_path.display()
        )
    });
}

struct BundledFile {
    relative_path: String,
    absolute_path: String,
    unix_mode: u32,
}

fn collect_files(root: &Path, dir: &Path, files: &mut Vec<BundledFile>) {
    println!("cargo:rerun-if-changed={}", dir.display());

    let entries = fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("failed to read directory {}: {error}", dir.display()));

    for entry in entries {
        let entry = entry
            .unwrap_or_else(|error| panic!("failed to read entry in {}: {error}", dir.display()));
        let path = entry.path();
        let file_type = entry.file_type().unwrap_or_else(|error| {
            panic!("failed to read file type for {}: {error}", path.display())
        });

        if file_type.is_dir() {
            collect_files(root, path.as_path(), files);
        } else if file_type.is_file() {
            let metadata = entry.metadata().unwrap_or_else(|error| {
                panic!("failed to read metadata for {}: {error}", path.display())
            });
            let relative_path = path
                .strip_prefix(root)
                .unwrap_or_else(|error| {
                    panic!(
                        "failed to compute bundled skill relative path for {}: {error}",
                        path.display()
                    )
                })
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            files.push(BundledFile {
                relative_path,
                absolute_path: path.display().to_string(),
                unix_mode: file_unix_mode(&metadata),
            });
        }
    }
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
