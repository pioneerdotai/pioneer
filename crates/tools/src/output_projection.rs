use crate::context::{ToolErrorClass, ToolOutcome};
use crate::output_policy::{
    DiagnosticExcerptPolicy, LlmOutputPolicy, RecoveryOutputPolicy, StorageOutputPolicy,
    TimelineOutputPolicy, ToolDisplayPayload, ToolMetadata, ToolOutputPolicySnapshot,
    ToolOutputProjectionKind, ToolOutputSummary, ToolRecoveryView, ToolResultEnvelope,
    ToolResultView, ToolStoragePayload,
};
use crate::web::{DownloadModelPayload, WebFetchModelPayload, WebSearchModelPayload};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

const SUMMARY_MAX_CHARS: usize = 2_000;

pub struct ToolProjectionInput<'a> {
    pub call_id: &'a str,
    pub tool_name: &'a str,
    pub arguments: &'a JsonValue,
    pub raw_output_text: &'a str,
    pub raw_output_json: &'a JsonValue,
    pub success: bool,
    pub outcome: &'a ToolOutcome,
    pub output_policy: &'a ToolOutputPolicySnapshot,
    pub output_projection: &'a ToolOutputProjectionKind,
}

pub fn project_tool_result(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    match input.output_projection {
        ToolOutputProjectionKind::Builtin => project_builtin(input),
        ToolOutputProjectionKind::DynamicGeneric => project_dynamic_generic(input),
        ToolOutputProjectionKind::DynamicHttp => project_dynamic_http(input),
        ToolOutputProjectionKind::DynamicShell => project_dynamic_shell(input),
        ToolOutputProjectionKind::DynamicMcp => project_dynamic_mcp(input),
        ToolOutputProjectionKind::DynamicFunctionProxy {
            target_tool_name,
            target_policy,
            target_projection_kind,
        } => project_dynamic_function_proxy(
            input,
            target_tool_name,
            target_policy,
            target_projection_kind,
        ),
    }
}

fn project_builtin(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    match input.tool_name {
        "exec_command" | "write_stdin" => project_shell(input),
        "web_fetch" => project_web_fetch(input),
        "web_search" => project_web_search(input),
        "download_url" | "download" => project_download(input),
        "apply_patch" => project_file_change(input),
        "read_file" => project_read_file(input),
        "read_skill" => project_read_skill(input),
        "list_dir" => project_list_dir(input),
        "grep_files" => project_grep_files(input),
        "computer_use" => project_computer_use(input),
        _ => project_dynamic_generic(input),
    }
}

fn project_shell(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let stdout = input
        .raw_output_json
        .get("stdout")
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .filter(|value| !value.is_empty());
    let stderr = input
        .raw_output_json
        .get("stderr")
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .filter(|value| !value.is_empty());
    let aggregated_output = input
        .raw_output_json
        .get("aggregated_output")
        .and_then(JsonValue::as_str)
        .map(str::to_owned)
        .filter(|value| !value.is_empty())
        .or_else(|| (!input.raw_output_text.is_empty()).then(|| input.raw_output_text.to_owned()));
    let exit_code = input
        .raw_output_json
        .get("exit_code")
        .and_then(JsonValue::as_i64)
        .and_then(|value| i32::try_from(value).ok());
    let duration_ms = input
        .raw_output_json
        .get("duration_ms")
        .and_then(JsonValue::as_u64);
    let timed_out = input
        .raw_output_json
        .get("timed_out")
        .and_then(JsonValue::as_bool);
    let truncated = shell_truncated(input.raw_output_json);

    let display = ToolDisplayPayload::Shell {
        stdout: stdout.clone(),
        stderr: stderr.clone(),
        aggregated_output: aggregated_output.clone(),
        exit_code,
        duration_ms,
        timed_out,
        truncated,
    };
    let storage = ToolStoragePayload::Shell {
        stdout,
        stderr,
        aggregated_output,
        exit_code,
        duration_ms,
        timed_out,
        truncated,
    };

    envelope(
        &input,
        llm_view_for_policy(&input),
        display,
        storage,
        recovery_view(&input),
    )
}

fn shell_truncated(raw_output_json: &JsonValue) -> bool {
    let Some(value) = raw_output_json.get("truncated") else {
        return false;
    };

    if let Some(truncated) = value.as_bool() {
        return truncated;
    }

    value
        .get("stdout")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
        || value
            .get("stderr")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false)
        || value
            .get("aggregated_output")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false)
}

fn project_web_fetch(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let payload =
        serde_json::from_value::<WebFetchModelPayload>(input.raw_output_json.clone()).ok();
    let metadata = payload
        .as_ref()
        .map(|payload| {
            serde_json::json!({
                "url": &payload.url,
                "finalUrl": &payload.final_url,
                "favicon": &payload.favicon,
                "statusCode": payload.status_code,
                "success": payload.success,
                "contentType": &payload.content_type,
                "extractMode": &payload.extract_mode,
                "resolvedMode": &payload.resolved_mode,
                "extractor": &payload.extractor,
                "elapsedMs": payload.elapsed_ms,
                "bytesReceived": payload.bytes_received,
                "truncated": payload.truncated.network || payload.truncated.output,
                "title": &payload.title,
                "wordCount": payload.word_count,
                "links": &payload.links,
                "contentHash": sha256_hex(payload.content.as_bytes()),
            })
        })
        .unwrap_or_else(|| safe_metadata_from_unknown(&input));
    let truncated = metadata
        .get("truncated")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    let summary = summary(
        metadata
            .get("url")
            .and_then(JsonValue::as_str)
            .map(|url| format!("Fetched {url}"))
            .unwrap_or_else(|| "web_fetch completed".to_owned()),
        vec![
            metadata
                .get("statusCode")
                .and_then(JsonValue::as_u64)
                .map(|code| format!("HTTP {code}"))
                .unwrap_or_else(|| "HTTP status unavailable".to_owned()),
            metadata
                .get("contentType")
                .and_then(JsonValue::as_str)
                .unwrap_or("content type unavailable")
                .to_owned(),
        ],
        metadata.clone(),
        truncated,
    );

    envelope(
        &input,
        llm_view_for_policy(&input),
        display_for_policy(input.output_policy, summary),
        ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(metadata),
        },
        recovery_view(&input),
    )
}

fn project_web_search(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let payload =
        serde_json::from_value::<WebSearchModelPayload>(input.raw_output_json.clone()).ok();
    let metadata = payload
        .as_ref()
        .map(|payload| {
            serde_json::json!({
                "query": &payload.query,
                "provider": &payload.provider,
                "tookMs": payload.took_ms,
                "resultCount": payload.result_count,
                "truncated": payload.truncated,
                "results": &payload.results,
            })
        })
        .unwrap_or_else(|| safe_metadata_from_unknown(&input));
    let result_count = metadata
        .get("resultCount")
        .and_then(JsonValue::as_u64)
        .unwrap_or_else(|| {
            metadata
                .get("results")
                .and_then(JsonValue::as_array)
                .map(|items| items.len() as u64)
                .unwrap_or(0)
        });
    let query = metadata.get("query").and_then(JsonValue::as_str);
    let summary = summary(
        query
            .map(|query| format!("web_search found {result_count} result(s) for {query}"))
            .unwrap_or_else(|| format!("web_search found {result_count} result(s)")),
        Vec::new(),
        metadata,
        false,
    );

    envelope(
        &input,
        llm_view_for_policy(&input),
        ToolDisplayPayload::Summary(summary.clone()),
        ToolStoragePayload::Summary(summary),
        recovery_view(&input),
    )
}

fn project_download(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let payload =
        serde_json::from_value::<DownloadModelPayload>(input.raw_output_json.clone()).ok();
    let metadata = payload
        .as_ref()
        .map(|payload| {
            serde_json::json!({
                "url": &payload.url,
                "finalUrl": &payload.final_url,
                "favicon": &payload.favicon,
                "statusCode": payload.status_code,
                "success": payload.success,
                "path": &payload.path,
                "bytesWritten": payload.bytes_written,
                "sha256": &payload.sha256,
                "contentType": &payload.content_type,
                "elapsedMs": payload.elapsed_ms,
                "truncated": payload.truncated,
            })
        })
        .unwrap_or_else(|| safe_metadata_from_unknown(&input));
    let summary = summary(
        metadata
            .get("url")
            .and_then(JsonValue::as_str)
            .map(|url| format!("Downloaded {url}"))
            .unwrap_or_else(|| "download completed".to_owned()),
        metadata
            .get("path")
            .and_then(JsonValue::as_str)
            .map(|path| vec![path.to_owned()])
            .unwrap_or_default(),
        metadata.clone(),
        metadata
            .get("truncated")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    );

    envelope(
        &input,
        llm_view_for_policy(&input),
        display_for_policy(input.output_policy, summary),
        ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(metadata),
        },
        recovery_view(&input),
    )
}

fn project_file_change(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let changed_files = input
        .raw_output_json
        .get("changed_files")
        .and_then(JsonValue::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let metadata = serde_json::json!({
        "changedFiles": changed_files,
        "status": input.raw_output_json.get("status").cloned().unwrap_or(JsonValue::Null),
        "success": input.raw_output_json.get("success").cloned().unwrap_or(JsonValue::Null),
        "exact": input.raw_output_json.get("exact").cloned().unwrap_or(JsonValue::Null),
        "historyBearing": input.raw_output_json.get("history_bearing").cloned().unwrap_or(JsonValue::Null),
        "changes": input.raw_output_json.get("changes").cloned().unwrap_or_else(|| serde_json::json!([])),
        "sideEffects": input.raw_output_json.get("side_effects").cloned().unwrap_or_else(|| serde_json::json!({})),
        "failedStage": input.raw_output_json.get("failed_stage").cloned().unwrap_or(JsonValue::Null),
        "error": input.raw_output_json.get("error").cloned().unwrap_or(JsonValue::Null),
        "tracking": input.raw_output_json.get("tracking").cloned().unwrap_or_else(|| serde_json::json!({})),
        "outputHash": sha256_hex(input.raw_output_text.as_bytes()),
    });
    let file_count = metadata
        .get("changedFiles")
        .and_then(JsonValue::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let summary = summary(
        if file_count == 0 {
            format!("{} completed", input.tool_name)
        } else {
            file_change_title(input.tool_name, file_count)
        },
        metadata
            .get("changedFiles")
            .and_then(JsonValue::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(JsonValue::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        metadata.clone(),
        false,
    );
    // The native gateway needs the immutable record reference to finish its
    // durable projection even when the model/storage policy is summary-only.
    // This envelope contains hashes, paths and record identity only; exact
    // source bytes never cross the model/event storage boundary.  Keep this
    // small trusted control envelope in metadata for apply_patch, while all
    // other file tools continue to obey their normal storage policy.
    let storage =
        if input.tool_name == "apply_patch" && input.raw_output_json.get("history").is_some() {
            let mut metadata_with_history = metadata.clone();
            if let Some(history) = input.raw_output_json.get("history")
                && let Some(object) = metadata_with_history.as_object_mut()
            {
                object.insert("patchHistory".to_owned(), history.clone());
            }
            ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(metadata_with_history),
            }
        } else {
            storage_for_policy(input.output_policy, summary.clone(), metadata)
        };
    envelope(
        &input,
        file_change_llm_view(&input),
        display_for_policy(input.output_policy, summary.clone()),
        storage,
        recovery_view(&input),
    )
}

fn file_change_title(tool_name: &str, file_count: usize) -> String {
    format!("{tool_name} changed {file_count} file(s)")
}

fn file_change_llm_view(input: &ToolProjectionInput<'_>) -> ToolResultView {
    llm_view_for_policy(input)
}

fn project_read_file(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let path = input
        .raw_output_json
        .get("path")
        .cloned()
        .unwrap_or(JsonValue::Null);
    let content = input
        .raw_output_json
        .get("text")
        .or_else(|| input.raw_output_json.get("output"))
        .and_then(JsonValue::as_str)
        .unwrap_or(input.raw_output_text);
    let metadata = serde_json::json!({
        "path": path,
        "startLine": input.raw_output_json.get("start_line").cloned().unwrap_or(JsonValue::Null),
        "endLine": input.raw_output_json.get("end_line").cloned().unwrap_or(JsonValue::Null),
        "nextLine": input.raw_output_json.get("next_line").cloned().unwrap_or(JsonValue::Null),
        "cursor": input.raw_output_json.get("cursor").cloned().unwrap_or(JsonValue::Null),
        "continuation": input.raw_output_json.get("continuation").cloned().unwrap_or(JsonValue::Null),
        "range": input.raw_output_json.get("range").cloned().unwrap_or(JsonValue::Null),
        "maxBytes": input.raw_output_json.get("max_bytes").cloned().unwrap_or(JsonValue::Null),
        "truncated": input.raw_output_json.get("truncated").cloned().unwrap_or(JsonValue::Bool(false)),
        "version": input.raw_output_json.get("version").cloned().unwrap_or(JsonValue::Null),
        "contentHash": input.raw_output_json.get("version").cloned().unwrap_or(JsonValue::Null),
        "bytes": input.raw_output_json.get("bytes").cloned().unwrap_or_else(|| JsonValue::from(content.len())),
    });
    let summary = summary(
        metadata
            .get("path")
            .and_then(JsonValue::as_str)
            .map(|path| format!("Read {path}"))
            .unwrap_or_else(|| "read_file completed".to_owned()),
        vec![format!("{} bytes", content.len())],
        metadata.clone(),
        metadata
            .get("truncated")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    );
    envelope(
        &input,
        llm_view_for_policy(&input),
        ToolDisplayPayload::Summary(summary),
        ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(metadata),
        },
        recovery_view(&input),
    )
}

fn project_read_skill(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let body = input
        .raw_output_json
        .get("body")
        .and_then(JsonValue::as_str)
        .unwrap_or(input.raw_output_text);
    let metadata = serde_json::json!({
        "slug": input.raw_output_json.get("slug").cloned().unwrap_or(JsonValue::Null),
        "name": input.raw_output_json.get("name").cloned().unwrap_or(JsonValue::Null),
        "description": input.raw_output_json.get("description").cloned().unwrap_or(JsonValue::Null),
        "sourceKind": input.raw_output_json.get("source_kind").cloned().unwrap_or(JsonValue::Null),
        "fingerprint": input.raw_output_json.get("fingerprint").cloned().unwrap_or(JsonValue::Null),
        "truncated": input.raw_output_json.get("truncated").cloned().unwrap_or(JsonValue::Bool(false)),
        "bodyHash": sha256_hex(body.as_bytes()),
        "bytes": body.len(),
    });
    let summary = summary(
        metadata
            .get("slug")
            .and_then(JsonValue::as_str)
            .map(|slug| format!("Read skill {slug}"))
            .unwrap_or_else(|| "read_skill completed".to_owned()),
        vec![format!("{} bytes", body.len())],
        metadata.clone(),
        metadata
            .get("truncated")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    );
    envelope(
        &input,
        llm_view_for_policy(&input),
        ToolDisplayPayload::Summary(summary),
        ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(metadata),
        },
        recovery_view(&input),
    )
}

fn project_list_dir(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let entries = input
        .raw_output_json
        .get("entries")
        .and_then(JsonValue::as_array)
        .map(|items| items.len())
        .unwrap_or(0);
    let metadata = serde_json::json!({
        "root": input.raw_output_json.get("root").cloned().unwrap_or(JsonValue::Null),
        "entryCount": entries,
        "truncated": input.raw_output_json.get("truncated").cloned().unwrap_or(JsonValue::Bool(false)),
        "resultHash": sha256_hex(input.raw_output_text.as_bytes()),
    });
    let summary = summary(
        metadata
            .get("root")
            .and_then(JsonValue::as_str)
            .map(|root| format!("Listed {root}"))
            .unwrap_or_else(|| "list_dir completed".to_owned()),
        vec![format!("{entries} entries")],
        metadata.clone(),
        metadata
            .get("truncated")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    );
    envelope(
        &input,
        llm_view_for_policy(&input),
        ToolDisplayPayload::Summary(summary),
        ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(metadata),
        },
        recovery_view(&input),
    )
}

fn project_grep_files(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let stdout = input
        .raw_output_json
        .get("stdout")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let match_count = stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
    let metadata = serde_json::json!({
        "status": input.raw_output_json.get("status").cloned().unwrap_or(JsonValue::Null),
        "engine": input.raw_output_json.get("engine").cloned().unwrap_or(JsonValue::Null),
        "path": input.raw_output_json.get("path").cloned().unwrap_or(JsonValue::Null),
        "exitCode": input.raw_output_json.get("exit_code").cloned().unwrap_or(JsonValue::Null),
        "truncated": input.raw_output_json.get("truncated").cloned().unwrap_or(JsonValue::Bool(false)),
        "matchCount": match_count,
        "resultHash": sha256_hex(input.raw_output_text.as_bytes()),
    });
    let summary = summary(
        "grep_files completed",
        vec![format!("{match_count} match line(s)")],
        metadata.clone(),
        metadata
            .get("truncated")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    );
    envelope(
        &input,
        llm_view_for_policy(&input),
        ToolDisplayPayload::Summary(summary),
        ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(metadata),
        },
        recovery_view(&input),
    )
}

fn project_computer_use(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let metadata = compact_computer_use_metadata(input.raw_output_json);
    let lines = computer_use_summary_lines(input.raw_output_json);
    let summary = summary(
        input
            .raw_output_json
            .get("action")
            .and_then(JsonValue::as_str)
            .map(|action| format!("computer_use {action}"))
            .unwrap_or_else(|| "computer_use completed".to_owned()),
        lines,
        metadata.clone(),
        input.outcome.incomplete,
    );
    envelope(
        &input,
        llm_view_for_policy(&input),
        display_for_policy(input.output_policy, summary),
        ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(metadata),
        },
        recovery_view(&input),
    )
}

fn computer_use_summary_lines(value: &JsonValue) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(coordinate) = value.pointer("/result/coordinate_observability") {
        lines.extend(computer_use_coordinate_lines(coordinate));
    } else if let Some(coordinate) = value.get("coordinate_observability") {
        lines.extend(computer_use_coordinate_lines(coordinate));
    }
    lines
}

fn computer_use_coordinate_lines(coordinate: &JsonValue) -> Vec<String> {
    let Some(slots) = coordinate.get("slots").and_then(JsonValue::as_object) else {
        return Vec::new();
    };
    slots
        .iter()
        .filter_map(|(name, slot)| {
            let requested = slot.get("requested_point")?;
            let converted = slot.get("converted_point")?;
            let requested_space = slot
                .get("requested_space")
                .and_then(JsonValue::as_str)
                .unwrap_or("unknown");
            let converted_space = slot
                .get("converted_space")
                .and_then(JsonValue::as_str)
                .unwrap_or("unknown");
            let requested_x = requested.get("x").and_then(JsonValue::as_i64)?;
            let requested_y = requested.get("y").and_then(JsonValue::as_i64)?;
            let converted_x = converted.get("x").and_then(JsonValue::as_i64)?;
            let converted_y = converted.get("y").and_then(JsonValue::as_i64)?;
            let status = slot
                .get("validation_status")
                .and_then(JsonValue::as_str)
                .unwrap_or("unknown");
            Some(format!(
                "coordinate {name}: {requested_space}({requested_x},{requested_y}) -> {converted_space}({converted_x},{converted_y}), validation={status}"
            ))
        })
        .collect()
}

fn compact_computer_use_metadata(value: &JsonValue) -> JsonValue {
    let mut metadata = remove_raw_like_fields(value);
    compact_accessibility_payload(&mut metadata);
    metadata
}

fn compact_accessibility_payload(value: &mut JsonValue) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if let Some(tree) = object.get_mut("accessibility_tree") {
        compact_accessibility_tree_nodes(tree);
    }
    if let Some(accessibility) = object.get_mut("accessibility") {
        compact_accessibility_tree_nodes(accessibility);
    }
    if let Some(llm_context) = object
        .get_mut("llm_context")
        .and_then(JsonValue::as_object_mut)
    {
        if let Some(tree) = llm_context.get_mut("accessibility_tree") {
            compact_accessibility_tree_nodes(tree);
        }
        if let Some(accessibility) = llm_context.get_mut("accessibility") {
            compact_accessibility_tree_nodes(accessibility);
        }
    }
}

fn compact_accessibility_tree_nodes(value: &mut JsonValue) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if let Some(nodes) = object.remove("nodes") {
        let node_count = nodes.as_array().map_or(0, Vec::len);
        object.insert(
            "node_count".to_owned(),
            JsonValue::Number(serde_json::Number::from(node_count)),
        );
    }
}

fn project_dynamic_generic(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let metadata = safe_metadata_from_unknown(&input);
    let summary = summary(
        format!("{} completed", input.tool_name),
        Vec::new(),
        metadata.clone(),
        input.outcome.incomplete,
    );
    envelope(
        &input,
        llm_view_for_policy(&input),
        display_for_policy(input.output_policy, summary.clone()),
        storage_for_policy(input.output_policy, summary, metadata),
        recovery_view(&input),
    )
}

fn project_dynamic_http(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let body = input
        .raw_output_json
        .get("body")
        .and_then(JsonValue::as_str)
        .unwrap_or(input.raw_output_text);
    let metadata = serde_json::json!({
        "toolName": input.tool_name,
        "url": input.raw_output_json.get("url").cloned().unwrap_or(JsonValue::Null),
        "statusCode": input.raw_output_json.get("status_code").cloned()
            .or_else(|| input.raw_output_json.get("statusCode").cloned())
            .unwrap_or(JsonValue::Null),
        "success": input.success,
        "truncated": input.raw_output_json.get("truncated").cloned().unwrap_or(JsonValue::Bool(input.outcome.incomplete)),
        "bodyHash": sha256_hex(body.as_bytes()),
        "bodyBytes": body.len(),
    });
    let title = metadata
        .get("url")
        .and_then(JsonValue::as_str)
        .map(|url| format!("Fetched {url}"))
        .unwrap_or_else(|| format!("{} completed", input.tool_name));
    let lines = metadata
        .get("statusCode")
        .and_then(JsonValue::as_i64)
        .map(|code| vec![format!("HTTP {code}")])
        .unwrap_or_default();
    let summary = summary(
        title,
        lines,
        metadata.clone(),
        metadata
            .get("truncated")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    );
    envelope(
        &input,
        llm_view_for_policy(&input),
        display_for_policy(input.output_policy, summary.clone()),
        storage_for_policy(input.output_policy, summary, metadata),
        recovery_view(&input),
    )
}

fn project_dynamic_shell(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    if shell_full_projection_allowed(input.output_policy) {
        return project_shell(input);
    }

    let metadata = safe_shell_metadata(&input);
    let summary = summary(
        format!("{} completed", input.tool_name),
        shell_summary_lines(&metadata),
        metadata.clone(),
        metadata
            .get("truncated")
            .and_then(JsonValue::as_bool)
            .unwrap_or(input.outcome.incomplete),
    );
    envelope(
        &input,
        llm_view_for_policy(&input),
        display_for_policy(input.output_policy, summary.clone()),
        storage_for_policy(input.output_policy, summary, metadata),
        recovery_view(&input),
    )
}

fn project_dynamic_mcp(input: ToolProjectionInput<'_>) -> ToolResultEnvelope {
    let mcp = input
        .raw_output_json
        .get("mcp")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let content = input
        .raw_output_json
        .get("content")
        .cloned()
        .unwrap_or(JsonValue::Null);
    let structured = input.raw_output_json.get("structuredContent").cloned();
    let runtime_state = input
        .raw_output_json
        .pointer("/meta/pioneer/runtime_state")
        .or_else(|| input.raw_output_json.pointer("/meta/pioneer/runtimeState"))
        .cloned()
        .unwrap_or(JsonValue::Null);
    let mut mcp_metadata = match sanitize_known_metadata_value(&mcp, None) {
        JsonValue::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    copy_mcp_metadata_alias(&mut mcp_metadata, &mcp, "serverId", "server_id");
    copy_mcp_metadata_alias(&mut mcp_metadata, &mcp, "serverName", "server_name");
    copy_mcp_metadata_alias(&mut mcp_metadata, &mcp, "rawToolName", "raw_tool_name");
    copy_mcp_metadata_alias(&mut mcp_metadata, &mcp, "callableName", "callable_name");
    copy_mcp_metadata_alias(&mut mcp_metadata, &mcp, "catalogVersion", "catalog_version");
    copy_mcp_metadata_alias(
        &mut mcp_metadata,
        &mcp,
        "snapshotVersion",
        "snapshot_version",
    );
    mcp_metadata.insert("runtime_state".to_owned(), runtime_state);
    mcp_metadata.insert(
        "duration_ms".to_owned(),
        input
            .raw_output_json
            .get("durationMs")
            .cloned()
            .unwrap_or(JsonValue::Null),
    );
    mcp_metadata.insert(
        "result_truncated".to_owned(),
        JsonValue::Bool(input.outcome.incomplete),
    );
    let metadata = serde_json::json!({
        "source": "mcp",
        "toolName": input.tool_name,
        "mcp": mcp_metadata,
        "success": input.success,
        "isError": input.raw_output_json
            .get("isError")
            .and_then(JsonValue::as_bool)
            .unwrap_or(!input.success),
        "durationMs": input.raw_output_json.get("durationMs").cloned().unwrap_or(JsonValue::Null),
        "duration_ms": input.raw_output_json.get("durationMs").cloned().unwrap_or(JsonValue::Null),
        "contentHash": sha256_hex(input.raw_output_text.as_bytes()),
        "jsonHash": sha256_hex(input.raw_output_json.to_string().as_bytes()),
        "structured": structured.is_some(),
        "contentPreview": sanitize_dynamic_value(&content, Some("content")),
        "truncated": input.outcome.incomplete,
    });
    let server_name = mcp
        .get("serverName")
        .and_then(JsonValue::as_str)
        .unwrap_or("mcp");
    let raw_tool_name = mcp
        .get("rawToolName")
        .and_then(JsonValue::as_str)
        .unwrap_or(input.tool_name);
    let title = if input.success {
        format!("MCP {server_name}/{raw_tool_name} completed")
    } else {
        format!("MCP {server_name}/{raw_tool_name} returned an error")
    };
    let lines = metadata
        .get("durationMs")
        .and_then(JsonValue::as_u64)
        .map(|duration| vec![format!("{duration} ms")])
        .unwrap_or_default();
    let summary = summary(title, lines, metadata.clone(), input.outcome.incomplete);
    envelope(
        &input,
        llm_view_for_policy(&input),
        display_for_policy(input.output_policy, summary.clone()),
        storage_for_policy(input.output_policy, summary, metadata),
        recovery_view(&input),
    )
}

fn copy_mcp_metadata_alias(
    target: &mut serde_json::Map<String, JsonValue>,
    source: &JsonValue,
    source_key: &str,
    target_key: &str,
) {
    if let Some(value) = source.get(source_key).cloned() {
        target.insert(target_key.to_owned(), value);
    }
}

fn project_dynamic_function_proxy(
    input: ToolProjectionInput<'_>,
    target_tool_name: &str,
    target_policy: &ToolOutputPolicySnapshot,
    target_projection_kind: &ToolOutputProjectionKind,
) -> ToolResultEnvelope {
    let target_input = ToolProjectionInput {
        call_id: input.call_id,
        tool_name: target_tool_name,
        arguments: input.arguments,
        raw_output_text: input.raw_output_text,
        raw_output_json: input.raw_output_json,
        success: input.success,
        outcome: input.outcome,
        output_policy: target_policy,
        output_projection: target_projection_kind,
    };
    let target = project_tool_result(target_input);
    let metadata = safe_metadata_from_projection(&input, &target);
    let summary = summary(
        format!("{} proxied {target_tool_name}", input.tool_name),
        Vec::new(),
        metadata.clone(),
        target.outcome.incomplete,
    );
    let display = if shell_full_projection_allowed(input.output_policy)
        && let ToolDisplayPayload::Shell { .. } = &target.display
    {
        target.display.clone()
    } else if matches!(
        &target.display,
        ToolDisplayPayload::Summary(_) | ToolDisplayPayload::Progress { .. }
    ) && matches!(
        input.output_policy.timeline,
        TimelineOutputPolicy::Full { .. } | TimelineOutputPolicy::Summary { .. }
    ) {
        target.display.clone()
    } else {
        display_for_policy(input.output_policy, summary.clone())
    };
    let storage = if shell_full_projection_allowed(input.output_policy)
        && let ToolStoragePayload::Shell { .. } = &target.storage
    {
        target.storage.clone()
    } else if matches!(&target.storage, ToolStoragePayload::Summary(_))
        && matches!(
            input.output_policy.storage,
            StorageOutputPolicy::Full { .. } | StorageOutputPolicy::Summary { .. }
        )
    {
        target.storage.clone()
    } else {
        storage_for_policy(input.output_policy, summary, metadata)
    };

    ToolResultEnvelope {
        llm_view: target.llm_view,
        display,
        storage,
        recovery: recovery_view(&input),
        outcome: target.outcome,
        success: target.success,
        output_policy: input.output_policy.clone(),
    }
}

fn envelope(
    input: &ToolProjectionInput<'_>,
    llm_view: ToolResultView,
    display: ToolDisplayPayload,
    storage: ToolStoragePayload,
    recovery: Option<ToolRecoveryView>,
) -> ToolResultEnvelope {
    ToolResultEnvelope {
        llm_view,
        display,
        storage,
        recovery,
        outcome: input.outcome.clone(),
        success: input.success,
        output_policy: input.output_policy.clone(),
    }
}

fn llm_view_for_policy(input: &ToolProjectionInput<'_>) -> ToolResultView {
    match input.output_policy.llm {
        LlmOutputPolicy::Full { max_bytes } | LlmOutputPolicy::Structured { max_bytes } => {
            ToolResultView::Json {
                value: input.raw_output_json.clone(),
                truncated: false,
            }
            .bounded_to_bytes(max_bytes)
        }
        LlmOutputPolicy::SummaryOnly => ToolResultView::Json {
            value: safe_metadata_from_unknown(input),
            truncated: false,
        },
    }
}

fn summary(
    title: impl Into<String>,
    lines: Vec<String>,
    raw_metadata: JsonValue,
    truncated: bool,
) -> ToolOutputSummary {
    ToolOutputSummary {
        title: truncate_chars(title.into().as_str(), SUMMARY_MAX_CHARS),
        lines: lines
            .into_iter()
            .map(|line| truncate_chars(line.as_str(), SUMMARY_MAX_CHARS))
            .collect(),
        metadata: ToolMetadata::from_json(raw_metadata),
        truncated,
    }
}

fn display_for_policy(
    output_policy: &ToolOutputPolicySnapshot,
    summary: ToolOutputSummary,
) -> ToolDisplayPayload {
    match output_policy.timeline {
        TimelineOutputPolicy::Full { .. } => ToolDisplayPayload::Summary(summary),
        TimelineOutputPolicy::Summary { max_chars } => {
            ToolDisplayPayload::Summary(bounded_summary_for_chars(summary, max_chars))
        }
        TimelineOutputPolicy::MetadataOnly | TimelineOutputPolicy::Hidden => {
            ToolDisplayPayload::Hidden
        }
    }
}

fn storage_for_policy(
    output_policy: &ToolOutputPolicySnapshot,
    summary: ToolOutputSummary,
    raw_metadata: JsonValue,
) -> ToolStoragePayload {
    match output_policy.storage {
        StorageOutputPolicy::Full { .. } => ToolStoragePayload::Summary(summary),
        StorageOutputPolicy::Summary { max_chars } => {
            ToolStoragePayload::Summary(bounded_summary_for_chars(summary, max_chars))
        }
        StorageOutputPolicy::MetadataOnly => ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(raw_metadata),
        },
        StorageOutputPolicy::None => ToolStoragePayload::None,
    }
}

fn shell_full_projection_allowed(output_policy: &ToolOutputPolicySnapshot) -> bool {
    matches!(output_policy.timeline, TimelineOutputPolicy::Full { .. })
        && matches!(output_policy.storage, StorageOutputPolicy::Full { .. })
}

fn recovery_view(input: &ToolProjectionInput<'_>) -> Option<ToolRecoveryView> {
    if input.success && !input.outcome.incomplete && input.outcome.error_class.is_none() {
        return None;
    }

    match &input.output_policy.recovery {
        RecoveryOutputPolicy::None => None,
        RecoveryOutputPolicy::MetadataOnly => Some(ToolRecoveryView {
            error_class: input.outcome.error_class.map(error_class_name),
            retry_hint: input.outcome.retry_hint.clone(),
            incomplete_reason: input.outcome.incomplete_reason.clone(),
            diagnostic_summary: input.outcome.retry_hint.clone(),
            diagnostic_excerpt: None,
            output_fingerprint: None,
            content_fingerprint: None,
            was_truncated: recovery_was_truncated(input),
            continuation: None,
        }),
        RecoveryOutputPolicy::Evidence {
            include_exit_status,
            include_error_class,
            include_retry_hint,
            diagnostic_excerpt,
            include_fingerprints,
        } => Some(ToolRecoveryView {
            error_class: include_error_class
                .then(|| input.outcome.error_class.map(error_class_name))
                .flatten(),
            retry_hint: include_retry_hint
                .then(|| input.outcome.retry_hint.clone())
                .flatten(),
            incomplete_reason: input.outcome.incomplete_reason.clone(),
            diagnostic_summary: include_retry_hint
                .then(|| input.outcome.retry_hint.clone())
                .flatten()
                .or_else(|| input.outcome.incomplete_reason.clone()),
            diagnostic_excerpt: recovery_excerpt(input, diagnostic_excerpt),
            output_fingerprint: include_fingerprints
                .then(|| sha256_hex(input.raw_output_text.as_bytes())),
            content_fingerprint: include_fingerprints
                .then(|| sha256_hex(input.raw_output_json.to_string().as_bytes())),
            was_truncated: recovery_was_truncated(input),
            continuation: include_exit_status
                .then(|| recovery_exit_status(input))
                .flatten(),
        }),
    }
}

fn error_class_name(class: ToolErrorClass) -> String {
    format!("{class:?}")
}

fn recovery_excerpt(
    input: &ToolProjectionInput<'_>,
    policy: &DiagnosticExcerptPolicy,
) -> Option<String> {
    match policy {
        DiagnosticExcerptPolicy::Disabled => None,
        DiagnosticExcerptPolicy::ErrorsOnly { max_chars } => recovery_error_text(input)
            .filter(|text| !text.trim().is_empty())
            .map(|text| truncate_chars(text.as_str(), *max_chars)),
        DiagnosticExcerptPolicy::Output { max_chars } => {
            let source = if input.raw_output_text.is_empty() {
                input.raw_output_json.to_string()
            } else {
                input.raw_output_text.to_owned()
            };
            (!source.trim().is_empty()).then(|| truncate_chars(source.as_str(), *max_chars))
        }
    }
}

fn recovery_error_text(input: &ToolProjectionInput<'_>) -> Option<String> {
    input
        .raw_output_json
        .get("stderr")
        .and_then(JsonValue::as_str)
        .or_else(|| {
            input
                .raw_output_json
                .get("error")
                .and_then(JsonValue::as_str)
        })
        .or_else(|| {
            input
                .raw_output_json
                .get("message")
                .and_then(JsonValue::as_str)
        })
        .map(str::to_owned)
}

fn recovery_was_truncated(input: &ToolProjectionInput<'_>) -> bool {
    input.outcome.incomplete
        || input
            .raw_output_json
            .get("truncated")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false)
}

fn recovery_exit_status(input: &ToolProjectionInput<'_>) -> Option<JsonValue> {
    let mut map = serde_json::Map::new();
    if let Some(exit_code) = input.raw_output_json.get("exit_code").cloned() {
        map.insert("exitCode".to_owned(), exit_code);
    }
    if let Some(timed_out) = input.raw_output_json.get("timed_out").cloned() {
        map.insert("timedOut".to_owned(), timed_out);
    }
    (!map.is_empty()).then_some(JsonValue::Object(map))
}

fn safe_metadata_from_unknown(input: &ToolProjectionInput<'_>) -> JsonValue {
    serde_json::json!({
        "toolName": input.tool_name,
        "argumentKeys": input.arguments.as_object().map(|map| map.keys().cloned().collect::<Vec<_>>()).unwrap_or_default(),
        "success": input.success,
        "hasModelOutput": !input.raw_output_text.is_empty() || !input.raw_output_json.is_null(),
        "outputHash": sha256_hex(input.raw_output_text.as_bytes()),
        "jsonHash": sha256_hex(input.raw_output_json.to_string().as_bytes()),
        "sanitizedResult": sanitize_dynamic_value(input.raw_output_json, None),
    })
}

fn remove_raw_like_fields(value: &JsonValue) -> JsonValue {
    sanitize_known_metadata_value(value, None)
}

fn safe_metadata_from_projection(
    input: &ToolProjectionInput<'_>,
    target: &ToolResultEnvelope,
) -> JsonValue {
    serde_json::json!({
        "toolName": input.tool_name,
        "success": input.success,
        "targetStorage": sanitize_dynamic_value(
            &serde_json::to_value(&target.storage).unwrap_or_else(|_| serde_json::json!({})),
            None,
        ),
        "outputHash": sha256_hex(input.raw_output_text.as_bytes()),
        "jsonHash": sha256_hex(input.raw_output_json.to_string().as_bytes()),
    })
}

fn safe_shell_metadata(input: &ToolProjectionInput<'_>) -> JsonValue {
    let stdout = input
        .raw_output_json
        .get("stdout")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let stderr = input
        .raw_output_json
        .get("stderr")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    let aggregated = input
        .raw_output_json
        .get("aggregated_output")
        .and_then(JsonValue::as_str)
        .unwrap_or(input.raw_output_text);
    serde_json::json!({
        "toolName": input.tool_name,
        "command": input.raw_output_json.get("command").cloned().unwrap_or(JsonValue::Null),
        "exitCode": input.raw_output_json.get("exit_code").cloned().unwrap_or(JsonValue::Null),
        "timedOut": input.raw_output_json.get("timed_out").cloned().unwrap_or(JsonValue::Bool(false)),
        "durationMs": input.raw_output_json.get("duration_ms").cloned().unwrap_or(JsonValue::Null),
        "truncated": input.raw_output_json.get("truncated").cloned().unwrap_or(JsonValue::Bool(input.outcome.incomplete)),
        "stdoutHash": sha256_hex(stdout.as_bytes()),
        "stdoutBytes": stdout.len(),
        "stderrHash": sha256_hex(stderr.as_bytes()),
        "stderrBytes": stderr.len(),
        "aggregatedOutputHash": sha256_hex(aggregated.as_bytes()),
        "aggregatedOutputBytes": aggregated.len(),
    })
}

fn shell_summary_lines(metadata: &JsonValue) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(exit_code) = metadata.get("exitCode").and_then(JsonValue::as_i64) {
        lines.push(format!("exit code {exit_code}"));
    }
    if let Some(duration_ms) = metadata.get("durationMs").and_then(JsonValue::as_u64) {
        lines.push(format!("{duration_ms} ms"));
    }
    lines
}

fn sanitize_dynamic_value(value: &JsonValue, key_hint: Option<&str>) -> JsonValue {
    let raw_like_key = key_hint.is_some_and(is_raw_like_key);
    if raw_like_key {
        return hashed_value(value, key_hint.unwrap_or("value"));
    }

    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => value.clone(),
        JsonValue::String(text) => serde_json::json!({
            "valueHash": sha256_hex(text.as_bytes()),
            "valueBytes": text.len(),
            "valueKind": "string",
        }),
        JsonValue::Array(items) => JsonValue::Array(
            items
                .iter()
                .map(|item| sanitize_dynamic_value(item, None))
                .collect(),
        ),
        JsonValue::Object(map) => {
            let mut safe = serde_json::Map::new();
            for (key, value) in map {
                if is_raw_like_key(key) {
                    let hashed = hashed_value(value, key);
                    if let Some(object) = hashed.as_object() {
                        for (hashed_key, hashed_value) in object {
                            safe.insert(hashed_key.clone(), hashed_value.clone());
                        }
                    }
                } else {
                    safe.insert(key.clone(), sanitize_dynamic_value(value, Some(key)));
                }
            }
            JsonValue::Object(safe)
        }
    }
}

fn sanitize_known_metadata_value(value: &JsonValue, key_hint: Option<&str>) -> JsonValue {
    let raw_like_key = key_hint.is_some_and(is_raw_like_key);
    if raw_like_key {
        return hashed_value(value, key_hint.unwrap_or("value"));
    }

    match value {
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) => value.clone(),
        JsonValue::String(text) => {
            if text.chars().count() > SUMMARY_MAX_CHARS {
                serde_json::json!({
                    "valueHash": sha256_hex(text.as_bytes()),
                    "valueBytes": text.len(),
                    "valueKind": "string",
                    "truncated": true,
                })
            } else {
                value.clone()
            }
        }
        JsonValue::Array(items) => JsonValue::Array(
            items
                .iter()
                .map(|item| sanitize_known_metadata_value(item, None))
                .collect(),
        ),
        JsonValue::Object(map) => {
            let mut safe = serde_json::Map::new();
            for (key, value) in map {
                if is_raw_like_key(key) {
                    let hashed = hashed_value(value, key);
                    if let Some(object) = hashed.as_object() {
                        for (hashed_key, hashed_value) in object {
                            safe.insert(hashed_key.clone(), hashed_value.clone());
                        }
                    }
                } else {
                    safe.insert(key.clone(), sanitize_known_metadata_value(value, Some(key)));
                }
            }
            JsonValue::Object(safe)
        }
    }
}

fn hashed_value(value: &JsonValue, key: &str) -> JsonValue {
    let bytes = value_bytes(value);
    let normalized = normalize_key_prefix(key);
    let mut map = serde_json::Map::new();
    map.insert(
        format!("{normalized}Hash"),
        JsonValue::String(sha256_hex(bytes.as_slice())),
    );
    map.insert(
        format!("{normalized}Bytes"),
        JsonValue::Number(serde_json::Number::from(bytes.len())),
    );
    map.insert(
        format!("{normalized}Kind"),
        JsonValue::String(value_kind(value).to_owned()),
    );
    JsonValue::Object(map)
}

fn value_bytes(value: &JsonValue) -> Vec<u8> {
    match value {
        JsonValue::String(text) => text.as_bytes().to_vec(),
        _ => serde_json::to_vec(value).unwrap_or_default(),
    }
}

fn normalize_key_prefix(key: &str) -> String {
    let mut output = String::new();
    let mut uppercase_next = false;
    for ch in key.chars() {
        if ch == '_' || ch == '-' || ch == ' ' {
            uppercase_next = true;
            continue;
        }
        if output.is_empty() {
            output.push(ch.to_ascii_lowercase());
        } else if uppercase_next {
            output.push(ch.to_ascii_uppercase());
            uppercase_next = false;
        } else {
            output.push(ch);
        }
    }
    if output.is_empty() {
        "value".to_owned()
    } else {
        output
    }
}

fn is_raw_like_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "content"
            | "body"
            | "blob"
            | "base64"
            | "bytes"
            | "data"
            | "dataurl"
            | "data_url"
            | "html"
            | "image"
            | "output"
            | "outputjson"
            | "output_json"
            | "screenshot"
            | "stdout"
            | "stderr"
            | "text"
    )
}

fn value_kind(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "bool",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_owned();
    }
    text.chars().take(max_chars).collect()
}

fn bounded_summary_for_chars(summary: ToolOutputSummary, max_chars: usize) -> ToolOutputSummary {
    let mut remaining = max_chars;
    let mut truncated = summary.truncated;
    let (title, title_truncated) = take_chars_with_status(summary.title.as_str(), remaining);
    truncated |= title_truncated;
    remaining = remaining.saturating_sub(title.chars().count());

    let mut lines = Vec::new();
    for line in summary.lines {
        if remaining == 0 {
            truncated = true;
            break;
        }
        let (bounded, line_truncated) = take_chars_with_status(line.as_str(), remaining);
        remaining = remaining.saturating_sub(bounded.chars().count());
        lines.push(bounded);
        if line_truncated {
            truncated = true;
            break;
        }
    }

    ToolOutputSummary {
        title,
        lines,
        metadata: summary.metadata,
        truncated,
    }
}

fn take_chars_with_status(text: &str, max_chars: usize) -> (String, bool) {
    let count = text.chars().count();
    if count <= max_chars {
        return (text.to_owned(), false);
    }
    (text.chars().take(max_chars).collect(), true)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output_policy::{
        DeltaOutputPolicy, DiagnosticExcerptPolicy, LlmOutputPolicy, LlmRetentionPolicy,
        RecoveryOutputPolicy,
    };
    use crate::web::{WebFetchLink, WebFetchTruncation};

    fn ok_outcome() -> ToolOutcome {
        ToolOutcome::ok()
    }

    #[test]
    fn web_fetch_projection_keeps_body_only_in_llm_view() {
        let payload = serde_json::to_value(WebFetchModelPayload {
            url: "https://example.com".to_owned(),
            final_url: "https://example.com".to_owned(),
            favicon: None,
            status_code: 200,
            success: true,
            content_type: Some("text/html".to_owned()),
            extract_mode: "text".to_owned(),
            resolved_mode: "text".to_owned(),
            extractor: None,
            elapsed_ms: 10,
            bytes_received: 32,
            truncated: WebFetchTruncation {
                network: false,
                output: false,
            },
            title: Some("Example".to_owned()),
            word_count: Some(3),
            links: vec![WebFetchLink {
                index: 1,
                text: "link".to_owned(),
                href: "https://example.com/link".to_owned(),
            }],
            content: "SECRET_PAGE_BODY_SENTINEL".to_owned(),
        })
        .expect("payload should serialize");

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_web_fetch",
            tool_name: "web_fetch",
            arguments: &serde_json::json!({"url": "https://example.com"}),
            raw_output_text: "SECRET_PAGE_BODY_SENTINEL",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            output_projection: &ToolOutputProjectionKind::Builtin,
        });

        assert!(
            serde_json::to_string(&envelope.llm_view)
                .unwrap()
                .contains("SECRET_PAGE_BODY_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.display)
                .unwrap()
                .contains("SECRET_PAGE_BODY_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.storage)
                .unwrap()
                .contains("SECRET_PAGE_BODY_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.recovery)
                .unwrap()
                .contains("SECRET_PAGE_BODY_SENTINEL")
        );
    }

    #[test]
    fn computer_use_projection_compacts_accessibility_nodes_outside_llm_view() {
        let payload = serde_json::json!({
            "action": "snapshot",
            "session_id": 1,
            "snapshot": {
                "path": "/tmp/snap.png"
            },
            "accessibility_tree": {
                "status": "ok",
                "nodes": [
                    { "id": "n1", "role": "button", "name": "SECRET_TREE_NODE" }
                ],
                "truncated": false
            },
            "llm_context": {
                "attachment": {
                    "path": "/tmp/snap.png",
                    "mime_type": "image/png"
                },
                "accessibility_tree": {
                    "status": "ok",
                    "nodes": [
                        { "id": "n1", "role": "button", "name": "SECRET_TREE_NODE" }
                    ],
                    "truncated": false
                }
            }
        });
        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_computer_use",
            tool_name: "computer_use",
            arguments: &serde_json::json!({"action": "snapshot"}),
            raw_output_text: "",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &ToolOutputPolicySnapshot::for_tool_name("computer_use"),
            output_projection: &ToolOutputProjectionKind::Builtin,
        });

        assert!(
            serde_json::to_string(&envelope.llm_view)
                .unwrap()
                .contains("SECRET_TREE_NODE")
        );
        assert!(
            !serde_json::to_string(&envelope.display)
                .unwrap()
                .contains("SECRET_TREE_NODE")
        );
        assert!(
            serde_json::to_string(&envelope.display)
                .unwrap()
                .contains("node_count")
        );
        assert!(
            !serde_json::to_string(&envelope.storage)
                .unwrap()
                .contains("SECRET_TREE_NODE")
        );
    }

    #[test]
    fn computer_use_projection_bounds_display_summary_to_timeline_policy() {
        let slots = (0..80)
            .map(|index| {
                (
                    format!("slot_{index}"),
                    serde_json::json!({
                        "requested_point": { "x": 10, "y": 20 },
                        "converted_point": { "x": 10, "y": 20 },
                        "requested_space": "snapshot",
                        "converted_space": "screen",
                        "validation_status": "ok"
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let payload = serde_json::json!({
            "action": "act",
            "result": {
                "coordinate_observability": {
                    "slots": slots
                }
            }
        });
        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_computer_use",
            tool_name: "computer_use",
            arguments: &serde_json::json!({"action": "act"}),
            raw_output_text: "",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &ToolOutputPolicySnapshot::for_tool_name("computer_use"),
            output_projection: &ToolOutputProjectionKind::Builtin,
        });

        let ToolDisplayPayload::Summary(summary) = envelope.display else {
            panic!("computer_use should use summary display");
        };
        let visible_chars = summary.title.chars().count()
            + summary
                .lines
                .iter()
                .map(|line| line.chars().count())
                .sum::<usize>();
        assert!(visible_chars <= 2_000, "visible_chars={visible_chars}");
        assert!(summary.truncated);
    }

    #[test]
    fn computer_use_coordinate_observability_projection_is_compact() {
        let payload = serde_json::json!({
            "action": "act",
            "session_id": 1,
            "result": {
                "coordinate_observability": {
                    "validation_status": "ok",
                    "slots": {
                        "target": {
                            "requested_point": {
                                "x": 100,
                                "y": 50,
                                "coordinate_space": "transport_pixels"
                            },
                            "requested_space": "transport_pixels",
                            "converted_point": {
                                "x": 100,
                                "y": 50,
                                "coordinate_space": "native_input"
                            },
                            "converted_space": "native_input",
                            "validation_status": "ok"
                        }
                    },
                    "display_bounds": {
                        "native_input": { "width": 320, "height": 180 }
                    }
                }
            }
        });
        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_computer_use",
            tool_name: "computer_use",
            arguments: &serde_json::json!({"action": "act"}),
            raw_output_text: "",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &ToolOutputPolicySnapshot::for_tool_name("computer_use"),
            output_projection: &ToolOutputProjectionKind::Builtin,
        });

        let display = serde_json::to_string(&envelope.display).expect("display serializes");
        assert!(display.contains("transport_pixels(100,50)"));
        assert!(display.contains("native_input(100,50)"));
        assert!(display.contains("validation=ok"));
        assert!(
            serde_json::to_string(&envelope.storage)
                .unwrap()
                .contains("coordinate_observability")
        );
    }

    #[test]
    fn output_projection_preserves_computer_use_trace_metadata() {
        let payload = serde_json::json!({
            "action": "act",
            "session_id": 7,
            "trace": {
                "session_id": 7,
                "snapshot_id": "s7-1",
                "action_kind": "semantic",
                "action_type": "press",
                "execution_status": "failed",
                "failure_class": "element_stale",
                "suggested_fallbacks": [{ "type": "snapshot" }]
            },
            "result": {
                "trace": {
                    "session_id": 7,
                    "action_kind": "semantic",
                    "action_type": "press",
                    "failure_class": "element_stale"
                }
            }
        });
        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_computer_use",
            tool_name: "computer_use",
            arguments: &serde_json::json!({"action": "act"}),
            raw_output_text: "",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &ToolOutputPolicySnapshot::for_tool_name("computer_use"),
            output_projection: &ToolOutputProjectionKind::Builtin,
        });

        let storage = serde_json::to_string(&envelope.storage).expect("storage serializes");
        assert!(storage.contains("\"trace\""));
        assert!(storage.contains("element_stale"));
        assert!(storage.contains("suggested_fallbacks"));
    }

    #[test]
    fn dynamic_unknown_projection_is_metadata_only_for_storage() {
        let payload = serde_json::json!({
            "content": "SECRET_DYNAMIC_SENTINEL",
            "body": "SECRET_DYNAMIC_SENTINEL",
            "nested": {
                "html": "<html>SECRET_DYNAMIC_SENTINEL</html>",
                "items": [
                    { "base64": "SECRET_DYNAMIC_SENTINEL" }
                ]
            }
        });
        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_dynamic",
            tool_name: "dynamic_tool",
            arguments: &serde_json::json!({"x": 1}),
            raw_output_text: "SECRET_DYNAMIC_SENTINEL",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &ToolOutputPolicySnapshot::for_tool_name("dynamic_tool"),
            output_projection: &ToolOutputProjectionKind::DynamicGeneric,
        });

        assert!(
            serde_json::to_string(&envelope.llm_view)
                .unwrap()
                .contains("SECRET_DYNAMIC_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.storage)
                .unwrap()
                .contains("SECRET_DYNAMIC_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.display)
                .unwrap()
                .contains("SECRET_DYNAMIC_SENTINEL")
        );
        if let ToolStoragePayload::Metadata { metadata } = &envelope.storage {
            assert!(metadata.get("output").is_none());
            assert!(metadata.get("sanitizedResult").is_some());
        } else {
            panic!("dynamic unknown storage should be metadata-only");
        }
    }

    #[test]
    fn dynamic_http_projection_keeps_body_only_in_llm_view() {
        let payload = serde_json::json!({
            "status_code": 200,
            "success": true,
            "url": "https://example.com/data",
            "body": "<html>SECRET_DYNAMIC_HTTP_BODY_SENTINEL</html>",
            "truncated": false
        });
        let policy = ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::Structured {
                max_bytes: 2 * 1024 * 1024,
            },
            llm_retention: LlmRetentionPolicy::UntilTurnTerminal {
                max_bytes: 2 * 1024 * 1024,
            },
            timeline: TimelineOutputPolicy::Summary { max_chars: 2000 },
            storage: StorageOutputPolicy::MetadataOnly,
            recovery: RecoveryOutputPolicy::MetadataOnly,
            deltas: DeltaOutputPolicy::ProgressOnly,
        };

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_dynamic_http",
            tool_name: "skill.tests-my-skill.fetch-data",
            arguments: &serde_json::json!({}),
            raw_output_text: "SECRET_DYNAMIC_HTTP_BODY_SENTINEL",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &policy,
            output_projection: &ToolOutputProjectionKind::DynamicHttp,
        });

        assert!(
            serde_json::to_string(&envelope.llm_view)
                .unwrap()
                .contains("SECRET_DYNAMIC_HTTP_BODY_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.display)
                .unwrap()
                .contains("SECRET_DYNAMIC_HTTP_BODY_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.storage)
                .unwrap()
                .contains("SECRET_DYNAMIC_HTTP_BODY_SENTINEL")
        );
    }

    #[test]
    fn llm_projection_respects_policy_max_bytes() {
        let payload = serde_json::json!({
            "path": "/tmp/large.txt",
            "output": format!("SECRET_LARGE_LLM_SENTINEL{}", "x".repeat(5_000)),
            "truncated": false
        });
        let policy = ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::Full { max_bytes: 256 },
            llm_retention: LlmRetentionPolicy::UntilTurnTerminal { max_bytes: 256 },
            timeline: TimelineOutputPolicy::Hidden,
            storage: StorageOutputPolicy::MetadataOnly,
            recovery: RecoveryOutputPolicy::MetadataOnly,
            deltas: DeltaOutputPolicy::Disabled,
        };

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_large_llm",
            tool_name: "read_file",
            arguments: &serde_json::json!({"path": "/tmp/large.txt"}),
            raw_output_text: "SECRET_LARGE_LLM_SENTINEL",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &policy,
            output_projection: &ToolOutputProjectionKind::Builtin,
        });

        assert!(envelope.llm_view.serialized_size_bytes() <= 256);
        assert!(matches!(
            envelope.llm_view,
            ToolResultView::Json {
                truncated: true,
                ..
            }
        ));
    }

    #[test]
    fn recovery_projection_honors_none_policy() {
        let payload = serde_json::json!({
            "stderr": "SECRET_RECOVERY_ERROR",
            "exit_code": 1,
            "timed_out": false
        });
        let outcome = ToolOutcome::recoverable(
            ToolErrorClass::ExecutionFailed,
            "retry with corrected input",
            false,
            None,
        );
        let policy = ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::SummaryOnly,
            llm_retention: LlmRetentionPolicy::DoNotRetain,
            timeline: TimelineOutputPolicy::Hidden,
            storage: StorageOutputPolicy::MetadataOnly,
            recovery: RecoveryOutputPolicy::None,
            deltas: DeltaOutputPolicy::Disabled,
        };

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_no_recovery",
            tool_name: "dynamic_tool",
            arguments: &serde_json::json!({}),
            raw_output_text: "SECRET_RECOVERY_ERROR",
            raw_output_json: &payload,
            success: false,
            outcome: &outcome,
            output_policy: &policy,
            output_projection: &ToolOutputProjectionKind::DynamicGeneric,
        });

        assert!(envelope.recovery.is_none());
    }

    #[test]
    fn recovery_projection_honors_evidence_fields() {
        let payload = serde_json::json!({
            "stderr": "abcdef",
            "stdout": "SECRET_STDOUT_SHOULD_NOT_BE_EXCERPTED",
            "exit_code": 124,
            "timed_out": true,
            "truncated": true
        });
        let outcome = ToolOutcome::recoverable(
            ToolErrorClass::Timeout,
            "retry with a longer timeout",
            true,
            Some("timed out".to_owned()),
        );
        let policy = ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::SummaryOnly,
            llm_retention: LlmRetentionPolicy::DoNotRetain,
            timeline: TimelineOutputPolicy::Hidden,
            storage: StorageOutputPolicy::MetadataOnly,
            recovery: RecoveryOutputPolicy::Evidence {
                include_exit_status: true,
                include_error_class: false,
                include_retry_hint: false,
                diagnostic_excerpt: DiagnosticExcerptPolicy::ErrorsOnly { max_chars: 4 },
                include_fingerprints: false,
            },
            deltas: DeltaOutputPolicy::Disabled,
        };

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_recovery_evidence",
            tool_name: "exec_command",
            arguments: &serde_json::json!({}),
            raw_output_text: "SECRET_STDOUT_SHOULD_NOT_BE_EXCERPTED",
            raw_output_json: &payload,
            success: false,
            outcome: &outcome,
            output_policy: &policy,
            output_projection: &ToolOutputProjectionKind::DynamicShell,
        });

        let recovery = envelope
            .recovery
            .expect("failure with evidence policy should produce recovery view");
        assert_eq!(recovery.error_class, None);
        assert_eq!(recovery.retry_hint, None);
        assert_eq!(recovery.diagnostic_excerpt.as_deref(), Some("abcd"));
        assert_eq!(recovery.output_fingerprint, None);
        assert_eq!(recovery.content_fingerprint, None);
        assert_eq!(
            recovery.continuation,
            Some(serde_json::json!({
                "exitCode": 124,
                "timedOut": true
            }))
        );
        assert!(recovery.was_truncated);
    }

    #[test]
    fn dynamic_shell_full_policy_persists_bounded_shell_output() {
        let payload = serde_json::json!({
            "command": ["/bin/sh", "-c", "printf VISIBLE_DYNAMIC_SHELL_SENTINEL"],
            "exit_code": 0,
            "stdout": "VISIBLE_DYNAMIC_SHELL_SENTINEL",
            "stderr": "",
            "timed_out": false,
            "duration_ms": 4,
            "truncated": false
        });
        let policy = ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::Full {
                max_bytes: 2 * 1024 * 1024,
            },
            llm_retention: LlmRetentionPolicy::UntilTurnTerminal {
                max_bytes: 2 * 1024 * 1024,
            },
            timeline: TimelineOutputPolicy::Full {
                max_bytes: 1024 * 1024,
            },
            storage: StorageOutputPolicy::Full {
                max_bytes: 1024 * 1024,
            },
            recovery: RecoveryOutputPolicy::MetadataOnly,
            deltas: DeltaOutputPolicy::PersistAndDisplay {
                max_chunk_bytes: 64 * 1024,
                max_total_bytes: 1024 * 1024,
            },
        };

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_dynamic_shell",
            tool_name: "skill.tests-my-skill.echo-shell",
            arguments: &serde_json::json!({}),
            raw_output_text: "VISIBLE_DYNAMIC_SHELL_SENTINEL",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &policy,
            output_projection: &ToolOutputProjectionKind::DynamicShell,
        });

        assert!(
            serde_json::to_string(&envelope.display)
                .unwrap()
                .contains("VISIBLE_DYNAMIC_SHELL_SENTINEL")
        );
        assert!(
            serde_json::to_string(&envelope.storage)
                .unwrap()
                .contains("VISIBLE_DYNAMIC_SHELL_SENTINEL")
        );
    }

    #[test]
    fn dynamic_shell_without_full_policy_hashes_shell_output_for_storage() {
        let payload = serde_json::json!({
            "command": ["/bin/sh", "-c", "printf hidden-output"],
            "exit_code": 0,
            "stdout": "SECRET_DYNAMIC_SHELL_SENTINEL",
            "stderr": "",
            "timed_out": false,
            "duration_ms": 4,
            "truncated": false
        });
        let policy = ToolOutputPolicySnapshot::for_tool_name("dynamic_tool");

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_dynamic_shell",
            tool_name: "skill.tests-my-skill.echo-shell",
            arguments: &serde_json::json!({}),
            raw_output_text: "SECRET_DYNAMIC_SHELL_SENTINEL",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &policy,
            output_projection: &ToolOutputProjectionKind::DynamicShell,
        });

        assert!(
            serde_json::to_string(&envelope.llm_view)
                .unwrap()
                .contains("SECRET_DYNAMIC_SHELL_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.display)
                .unwrap()
                .contains("SECRET_DYNAMIC_SHELL_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.storage)
                .unwrap()
                .contains("SECRET_DYNAMIC_SHELL_SENTINEL")
        );
    }

    #[test]
    fn dynamic_function_proxy_to_model_only_target_does_not_persist_raw_output() {
        let payload = serde_json::json!({
            "path": "/tmp/secret.txt",
            "output": "SECRET_FUNCTION_PROXY_MODEL_ONLY_SENTINEL",
            "truncated": false
        });
        let target_policy = ToolOutputPolicySnapshot::for_tool_name("read_file");
        let outer_policy = ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::Full {
                max_bytes: 2 * 1024 * 1024,
            },
            llm_retention: LlmRetentionPolicy::UntilTurnTerminal {
                max_bytes: 2 * 1024 * 1024,
            },
            timeline: TimelineOutputPolicy::Summary { max_chars: 2000 },
            storage: StorageOutputPolicy::Summary { max_chars: 2000 },
            recovery: RecoveryOutputPolicy::MetadataOnly,
            deltas: DeltaOutputPolicy::ProgressOnly,
        };

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_proxy_read_file",
            tool_name: "skill.tests-my-skill.proxy-read-file",
            arguments: &serde_json::json!({}),
            raw_output_text: "SECRET_FUNCTION_PROXY_MODEL_ONLY_SENTINEL",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &outer_policy,
            output_projection: &ToolOutputProjectionKind::DynamicFunctionProxy {
                target_tool_name: "read_file".to_owned(),
                target_policy,
                target_projection_kind: Box::new(ToolOutputProjectionKind::Builtin),
            },
        });

        assert!(
            serde_json::to_string(&envelope.llm_view)
                .unwrap()
                .contains("SECRET_FUNCTION_PROXY_MODEL_ONLY_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.display)
                .unwrap()
                .contains("SECRET_FUNCTION_PROXY_MODEL_ONLY_SENTINEL")
        );
        assert!(
            !serde_json::to_string(&envelope.storage)
                .unwrap()
                .contains("SECRET_FUNCTION_PROXY_MODEL_ONLY_SENTINEL")
        );
    }

    #[test]
    fn dynamic_function_proxy_to_exec_command_can_preserve_bounded_shell_output() {
        let payload = serde_json::json!({
            "command": ["/bin/sh", "-c", "printf PROXY_SHELL_SENTINEL"],
            "exit_code": 0,
            "stdout": "PROXY_SHELL_SENTINEL",
            "stderr": "",
            "aggregated_output": "PROXY_SHELL_SENTINEL",
            "timed_out": false,
            "duration_ms": 7,
            "truncated": {
                "stdout": false,
                "stderr": false,
                "aggregated_output": false
            }
        });
        let shell_policy = ToolOutputPolicySnapshot::for_tool_name("exec_command");

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_proxy_shell",
            tool_name: "skill.tests-my-skill.proxy-shell",
            arguments: &serde_json::json!({}),
            raw_output_text: "PROXY_SHELL_SENTINEL",
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &shell_policy,
            output_projection: &ToolOutputProjectionKind::DynamicFunctionProxy {
                target_tool_name: "exec_command".to_owned(),
                target_policy: shell_policy.clone(),
                target_projection_kind: Box::new(ToolOutputProjectionKind::Builtin),
            },
        });

        assert!(
            matches!(envelope.storage, ToolStoragePayload::Shell { stdout: Some(stdout), .. } if stdout.contains("PROXY_SHELL_SENTINEL"))
        );
    }

    #[test]
    fn dynamic_generic_large_strings_are_hashed_before_storage_and_recovery() {
        let large_secret = format!("SECRET_LARGE_DYNAMIC_SENTINEL{}", "x".repeat(10_000));
        let payload = serde_json::json!({
            "title": large_secret,
            "nested": {
                "items": [
                    { "value": large_secret }
                ]
            }
        });
        let policy = ToolOutputPolicySnapshot {
            llm: LlmOutputPolicy::Full {
                max_bytes: 2 * 1024 * 1024,
            },
            llm_retention: LlmRetentionPolicy::UntilTurnTerminal {
                max_bytes: 2 * 1024 * 1024,
            },
            timeline: TimelineOutputPolicy::Summary { max_chars: 2000 },
            storage: StorageOutputPolicy::Summary { max_chars: 2000 },
            recovery: RecoveryOutputPolicy::Evidence {
                include_exit_status: true,
                include_error_class: true,
                include_retry_hint: true,
                diagnostic_excerpt: DiagnosticExcerptPolicy::Output { max_chars: 4000 },
                include_fingerprints: true,
            },
            deltas: DeltaOutputPolicy::ProgressOnly,
        };

        let envelope = project_tool_result(ToolProjectionInput {
            call_id: "call_dynamic_large",
            tool_name: "skill.tests-my-skill.large",
            arguments: &serde_json::json!({}),
            raw_output_text: large_secret.as_str(),
            raw_output_json: &payload,
            success: true,
            outcome: &ok_outcome(),
            output_policy: &policy,
            output_projection: &ToolOutputProjectionKind::DynamicGeneric,
        });

        let storage = serde_json::to_string(&envelope.storage).unwrap();
        assert!(!storage.contains("SECRET_LARGE_DYNAMIC_SENTINEL"));
        assert!(storage.contains("valueHash") || storage.contains("titleHash"));
        assert!(
            !serde_json::to_string(&envelope.recovery)
                .unwrap()
                .contains("SECRET_LARGE_DYNAMIC_SENTINEL")
        );
    }

    #[test]
    fn model_only_tool_storage_never_contains_raw_content() {
        for (tool_name, payload) in [
            (
                "read_file",
                serde_json::json!({
                    "path": "/tmp/secret.txt",
                    "output": "SECRET_MODEL_ONLY_SENTINEL",
                    "truncated": false
                }),
            ),
            (
                "read_skill",
                serde_json::json!({
                    "slug": "secret-skill",
                    "body": "SECRET_MODEL_ONLY_SENTINEL",
                    "truncated": false
                }),
            ),
            (
                "list_dir",
                serde_json::json!({
                    "root": "/tmp",
                    "entries": ["SECRET_MODEL_ONLY_SENTINEL"],
                    "truncated": false
                }),
            ),
            (
                "grep_files",
                serde_json::json!({
                    "stdout": "SECRET_MODEL_ONLY_SENTINEL",
                    "stderr": "SECRET_MODEL_ONLY_SENTINEL",
                    "truncated": false
                }),
            ),
        ] {
            let envelope = project_tool_result(ToolProjectionInput {
                call_id: "call_model_only",
                tool_name,
                arguments: &serde_json::json!({}),
                raw_output_text: "SECRET_MODEL_ONLY_SENTINEL",
                raw_output_json: &payload,
                success: true,
                outcome: &ok_outcome(),
                output_policy: &ToolOutputPolicySnapshot::for_tool_name(tool_name),
                output_projection: &ToolOutputProjectionKind::Builtin,
            });

            assert!(
                serde_json::to_string(&envelope.llm_view)
                    .unwrap()
                    .contains("SECRET_MODEL_ONLY_SENTINEL")
            );
            assert!(
                !serde_json::to_string(&envelope.display)
                    .unwrap()
                    .contains("SECRET_MODEL_ONLY_SENTINEL"),
                "{tool_name} display leaked raw content"
            );
            assert!(
                !serde_json::to_string(&envelope.storage)
                    .unwrap()
                    .contains("SECRET_MODEL_ONLY_SENTINEL"),
                "{tool_name} storage leaked raw content"
            );
        }
    }
}
