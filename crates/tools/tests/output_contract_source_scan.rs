use std::fs;
use std::path::{Path, PathBuf};

const SCAN_ROOTS: &[&str] = &[
    "crates/protocol/src",
    "crates/tools/src",
    "crates/agent/src",
    "crates/gateway/src",
    "crates/crud/src",
    "crates/desktop/src",
];

const RAW_BOUNDARY_ALLOWLIST: &[&str] = &[
    "crates/tools/src/context.rs",
    "crates/tools/src/runtime.rs",
    "crates/tools/src/orchestrator.rs",
    "crates/tools/src/classifier.rs",
    "crates/tools/src/output_projection.rs",
    "crates/tools/src/handlers/skill.rs",
];

const OBSOLETE_RAW_CARRIERS: &[&str] = &[
    "output_json",
    "outputJson",
    "output_text",
    "outputText",
    "meta[\"output_json\"]",
    "meta.get(\"output_json\")",
    "meta[\"outputText\"]",
    "meta.get(\"outputText\")",
    "meta[\"llmView\"]",
    "meta.get(\"llmView\")",
];

const FORBIDDEN_EVERYWHERE: &[&str] = &[
    "ConfiguredToolSpec::dynamic",
    "should_emit_output_delta",
    "ToolEvent { text",
    "text: Option<String>,\n    meta: Option",
];

const DOWNSTREAM_RAW_ACCESSORS: &[&str] = &["raw_output_json(", "raw_output_text("];

const RAW_METADATA_FIELDS: &[&str] = &[
    " metadata: JsonValue",
    " pub metadata: JsonValue",
    " metadata: serde_json::Value",
    " pub metadata: serde_json::Value",
];

#[test]
fn tool_output_contract_has_no_obsolete_raw_paths() {
    let root = workspace_root();
    let mut failures = Vec::new();

    for file in rust_files_under_scan_roots(&root) {
        let relative = relative_path(&root, &file);
        let source = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"));
        let production_source = strip_cfg_test_modules(&source);

        for forbidden in FORBIDDEN_EVERYWHERE {
            collect_hits(
                &mut failures,
                &relative,
                &production_source,
                forbidden,
                "delete obsolete helper or replace it with typed policy/projector API",
            );
        }

        if !is_raw_boundary_file(&relative) && !relative.starts_with("crates/protocol/src/") {
            for forbidden in OBSOLETE_RAW_CARRIERS {
                collect_hits(
                    &mut failures,
                    &relative,
                    &production_source,
                    forbidden,
                    "move raw output handling into crates/tools projection boundary",
                );
            }
        }

        if !relative.starts_with("crates/tools/src/") {
            for forbidden in DOWNSTREAM_RAW_ACCESSORS {
                collect_hits(
                    &mut failures,
                    &relative,
                    &production_source,
                    forbidden,
                    "downstream code must consume typed llm/display/storage/recovery projections",
                );
            }
        }

        for forbidden in RAW_METADATA_FIELDS {
            collect_hits(
                &mut failures,
                &relative,
                &production_source,
                forbidden,
                "tool output/replay metadata must use ToolMetadata",
            );
        }
    }

    assert!(
        failures.is_empty(),
        "obsolete tool-output paths found:\n{}",
        failures.join("\n")
    );
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("tools crate should be under workspace/crates/tools")
        .to_path_buf()
}

fn rust_files_under_scan_roots(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for scan_root in SCAN_ROOTS {
        collect_rust_files(root.join(scan_root), &mut files);
    }
    files.sort();
    files
}

fn collect_rust_files(path: PathBuf, files: &mut Vec<PathBuf>) {
    let Ok(metadata) = fs::metadata(&path) else {
        return;
    };

    if metadata.is_file() {
        if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
        return;
    }

    if !metadata.is_dir() {
        return;
    }

    for entry in fs::read_dir(path).expect("failed to read source directory") {
        let entry = entry.expect("failed to read source directory entry");
        collect_rust_files(entry.path(), files);
    }
}

fn relative_path(root: &Path, file: &Path) -> String {
    file.strip_prefix(root)
        .unwrap_or(file)
        .to_string_lossy()
        .replace('\\', "/")
}

fn is_raw_boundary_file(relative: &str) -> bool {
    RAW_BOUNDARY_ALLOWLIST
        .iter()
        .any(|allowed| relative == *allowed)
}

fn collect_hits(
    failures: &mut Vec<String>,
    relative: &str,
    source: &str,
    needle: &str,
    suggestion: &str,
) {
    for (index, line) in source.lines().enumerate() {
        if line.contains(needle) {
            failures.push(format!(
                "{relative}:{} contains `{needle}`; {suggestion}",
                index + 1
            ));
        }
    }
}

fn strip_cfg_test_modules(source: &str) -> String {
    let mut stripped = String::new();
    let mut pending_cfg_test = false;
    let mut skipping = false;
    let mut brace_depth = 0i32;

    for line in source.lines() {
        let trimmed = line.trim_start();

        if skipping {
            stripped.push('\n');
            brace_depth += brace_delta(line);
            if brace_depth <= 0 {
                skipping = false;
            }
            continue;
        }

        if pending_cfg_test && trimmed.starts_with("mod ") && trimmed.contains('{') {
            stripped.push('\n');
            pending_cfg_test = false;
            skipping = true;
            brace_depth = brace_delta(line);
            if brace_depth <= 0 {
                skipping = false;
            }
            continue;
        }

        if trimmed == "#[cfg(test)]" {
            stripped.push('\n');
            pending_cfg_test = true;
            continue;
        }

        if pending_cfg_test && !(trimmed.is_empty() || trimmed.starts_with("#[")) {
            pending_cfg_test = false;
        }

        stripped.push_str(line);
        stripped.push('\n');
    }

    stripped
}

fn brace_delta(line: &str) -> i32 {
    line.chars().fold(0, |delta, ch| match ch {
        '{' => delta + 1,
        '}' => delta - 1,
        _ => delta,
    })
}
