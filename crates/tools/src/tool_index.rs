pub const CORE_FILE_TOOL_NAMES: &[&str] = &["read_file", "list_dir", "grep_files", "apply_patch"];

pub const CORE_TOOL_NAMES: &[&str] = &[
    "exec_command",
    "write_stdin",
    "read_file",
    "list_dir",
    "grep_files",
    "apply_patch",
    "web_search",
    "web_fetch",
    "download_url",
    "read_skill",
    "request_tools",
];

/// Stable model-facing filesystem catalog used by prompt/evaluation snapshots.
/// Provider adapters may transform the wire schema, but they must preserve
/// this semantic hierarchy and its single general text mutator.
pub fn filesystem_catalog_snapshot() -> serde_json::Value {
    serde_json::json!({
        "read_file": "bounded_paginated_utf8_text",
        "list_dir": "directory_discovery",
        "grep_files": "scoped_text_search",
        "apply_patch": "add_update_move_delete_multi_file_utf8_text_mutation"
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tool_index_defines_core_file_tool_identity() {
        assert_eq!(
            CORE_FILE_TOOL_NAMES,
            ["read_file", "list_dir", "grep_files", "apply_patch",]
        );
    }

    #[test]
    fn filesystem_catalog_snapshot_is_semantic_and_single_writer() {
        let snapshot = filesystem_catalog_snapshot();
        let mut keys = snapshot
            .as_object()
            .expect("catalog object")
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["apply_patch", "grep_files", "list_dir", "read_file"]
        );
        assert_eq!(
            snapshot["apply_patch"],
            "add_update_move_delete_multi_file_utf8_text_mutation"
        );
    }
}
