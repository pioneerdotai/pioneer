use super::super::handler::ComputerUseHandler;
use super::super::model::*;
use super::super::util::now_unix_ms;
use crate::context::FunctionToolOutput;
use crate::error::ToolError;
use crate::events::ToolEventTrace;
use serde_json::Value as JsonValue;

impl ComputerUseHandler {
    pub(crate) async fn handle_verify(
        &self,
        args: ComputerUseArgs,
        attempt_id: u32,
        trace: &ToolEventTrace,
    ) -> Result<FunctionToolOutput, ToolError> {
        let session_id = args.session_id.ok_or_else(|| {
            ToolError::invalid_arguments("computer_use verify requires session_id")
        })?;
        let expect = args.expect.ok_or_else(|| {
            ToolError::invalid_arguments("computer_use verify requires expect object")
        })?;

        let (session_status, goal, target, snapshot, tree, step_count) = {
            let manager = self.manager.lock().await;
            let session = manager.sessions.get(&session_id).ok_or_else(|| {
                ToolError::NotFound(format!("computer_use session {} not found", session_id))
            })?;
            (
                session.status,
                session.goal.clone(),
                session.target.clone(),
                session.last_snapshot.clone(),
                session.last_accessibility_tree.clone(),
                session.step_count,
            )
        };

        if snapshot.is_none() || tree.is_none() {
            let payload = serde_json::json!({
                "action": "verify",
                "mode": "remote",
                "session_id": session_id,
                "status": session_status.as_str(),
                "verification": {
                    "status": "needs_snapshot",
                    "evidence": [],
                    "reason": "latest snapshot/accessibility state is missing"
                },
                "next_call": {
                    "tool": "computer_use",
                    "arguments": {
                        "action": "snapshot",
                        "session_id": session_id
                    },
                    "reason": "verify_needs_latest_state"
                },
                "llm_context": {
                    "goal": goal,
                    "instruction": "Call computer_use snapshot first, then call verify again with the same expectation."
                }
            });
            trace.emit_stage(
                attempt_id,
                "computer_use.verify",
                Some("needs_snapshot".to_owned()),
                Some(serde_json::json!({"session_id": session_id, "status": "needs_snapshot"})),
            );
            return Ok(FunctionToolOutput::with_payload(
                format!("Verification needs a snapshot for session {session_id}"),
                true,
                payload,
            ));
        }

        let tree = tree.expect("checked above");
        let mut evidence = Vec::new();
        if let Some(expected_app) = normalized(expect.app.as_deref()) {
            let actual = target_app_name(&target);
            evidence.push(check_contains(
                "app",
                expected_app,
                actual.as_deref(),
                serde_json::json!({ "actual": actual }),
            ));
        }
        if let Some(expected_title) = normalized(expect.window_title.as_deref()) {
            let actual = target_window_title(&target);
            evidence.push(check_contains(
                "window_title",
                expected_title,
                actual.as_deref(),
                serde_json::json!({ "actual": actual }),
            ));
        }
        if let Some(expected_text) = normalized(expect.visible_text.as_deref()) {
            let matches = visible_text_matches(&tree, expected_text);
            evidence.push(serde_json::json!({
                "kind": "visible_text",
                "expected": expected_text,
                "passed": !matches.is_empty(),
                "matches": matches,
            }));
        }
        if let Some(expected_node) = expect.node.as_ref() {
            let matches = node_matches(&tree, expected_node);
            evidence.push(serde_json::json!({
                "kind": "node",
                "expected": {
                    "node_id": expected_node.node_id.clone(),
                    "selector": expected_node.selector.clone(),
                    "role": expected_node.role.clone(),
                    "name": expected_node.name.clone(),
                },
                "passed": !matches.is_empty(),
                "matches": matches,
            }));
        }
        if let Some(expected_hash_change) = expect.snapshot_hash_changed {
            evidence.push(serde_json::json!({
                "kind": "snapshot_hash_changed",
                "expected": expected_hash_change,
                "passed": JsonValue::Null,
                "inconclusive": true,
                "reason": "previous snapshot hash is not retained in session state yet"
            }));
        }

        if evidence.is_empty() {
            evidence.push(serde_json::json!({
                "kind": "expectation",
                "passed": JsonValue::Null,
                "inconclusive": true,
                "reason": "verify expect object did not include any supported expectation fields"
            }));
        }

        let status = verification_status(evidence.as_slice());
        let verified_at = now_unix_ms();
        let evidence_value = serde_json::json!(evidence);
        {
            let mut manager = self.manager.lock().await;
            if let Some(session) = manager.sessions.get_mut(&session_id) {
                session.updated_at_unix_ms = verified_at;
                session.previous_verification_status = session
                    .last_verification
                    .as_ref()
                    .map(|record| record.status.clone());
                session.last_verification = Some(VerifyRecord {
                    status: status.to_owned(),
                    evidence: evidence_value.clone(),
                    verified_at_unix_ms: verified_at,
                });
                if status == "passed" {
                    session.last_completion_evidence = Some(CompletionEvidence {
                        source: "verify".to_owned(),
                        strength: "strong".to_owned(),
                        summary: "verify passed against the latest snapshot/accessibility state"
                            .to_owned(),
                        evidence: evidence_value.clone(),
                        recorded_at_unix_ms: verified_at,
                        step_count: session.step_count,
                        snapshot_index: session
                            .last_snapshot
                            .as_ref()
                            .map(|snapshot| snapshot.index),
                    });
                    session.last_evidence_at_step = Some(session.step_count);
                } else {
                    session.last_completion_evidence = None;
                    session.last_evidence_at_step = None;
                }
            }
        }

        trace.emit_stage(
            attempt_id,
            "computer_use.verify",
            None,
            Some(serde_json::json!({"session_id": session_id, "status": status})),
        );

        Ok(FunctionToolOutput::with_payload(
            format!("Verification {status} for session {session_id}"),
            true,
            serde_json::json!({
                "action": "verify",
                "mode": "remote",
                "session_id": session_id,
                "status": session_status.as_str(),
                "verification": {
                    "status": status,
                    "evidence": evidence_value,
                    "verified_at_unix_ms": verified_at,
                },
                "completion_evidence": {
                    "accepted": status == "passed",
                    "source": if status == "passed" { serde_json::Value::String("verify".to_owned()) } else { serde_json::Value::Null },
                    "step_count": if status == "passed" { serde_json::Value::from(step_count) } else { serde_json::Value::Null }
                },
                "llm_context": {
                    "goal": goal,
                    "instruction": match status {
                        "passed" => "Verification passed and recorded completion evidence. You may now call computer_use stop with outcome=completed if the user goal is done.",
                        "failed" => "Verification failed. Request a fresh snapshot or choose another action before claiming completion.",
                        _ => "Verification was inconclusive. Request a fresh snapshot or use a more specific expectation.",
                    }
                }
            }),
        ))
    }
}

fn normalized(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn check_contains(kind: &str, expected: &str, actual: Option<&str>, extra: JsonValue) -> JsonValue {
    let passed = actual.is_some_and(|actual| contains_case_insensitive(actual, expected));
    let mut value = serde_json::json!({
        "kind": kind,
        "expected": expected,
        "passed": passed,
    });
    merge_object(&mut value, extra);
    value
}

fn merge_object(target: &mut JsonValue, extra: JsonValue) {
    if let (Some(target), Some(extra)) = (target.as_object_mut(), extra.as_object()) {
        for (key, value) in extra {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn verification_status(evidence: &[JsonValue]) -> &'static str {
    if evidence
        .iter()
        .any(|item| item.get("passed").and_then(JsonValue::as_bool) == Some(false))
    {
        return "failed";
    }
    if evidence.iter().any(|item| {
        item.get("inconclusive").and_then(JsonValue::as_bool) == Some(true)
            || item.get("passed").is_some_and(JsonValue::is_null)
    }) {
        return "inconclusive";
    }
    "passed"
}

fn target_app_name(target: &ComputerUseTarget) -> Option<String> {
    match target {
        ComputerUseTarget::Screen { .. } => None,
        ComputerUseTarget::App { app, .. } | ComputerUseTarget::ActiveApp { app, .. } => {
            Some(app.name.clone())
        }
    }
}

fn target_window_title(target: &ComputerUseTarget) -> Option<String> {
    match target {
        ComputerUseTarget::Screen { .. } => None,
        ComputerUseTarget::App { app, .. } | ComputerUseTarget::ActiveApp { app, .. } => {
            app.window_title.clone()
        }
    }
}

fn visible_text_matches(tree: &AccessibilityTreePayload, expected: &str) -> Vec<JsonValue> {
    tree.nodes
        .iter()
        .filter(|node| {
            [
                node.name.as_deref(),
                node.value.as_deref(),
                node.description.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|value| contains_case_insensitive(value, expected))
        })
        .map(|node| {
            serde_json::json!({
                "node_id": node.id,
                "role": node.role.clone(),
                "name": node.name.clone(),
                "value": node.value.clone(),
                "description": node.description.clone(),
            })
        })
        .collect()
}

fn node_matches(
    tree: &AccessibilityTreePayload,
    expected: &ComputerUseVerifyNodeArgs,
) -> Vec<JsonValue> {
    tree.nodes
        .iter()
        .filter(|node| {
            optional_match(expected.node_id.as_deref(), || node.id.as_str())
                && expected.selector.as_deref().map_or(true, |value| {
                    node.selector_hints.iter().any(|hint| hint == value)
                })
                && expected.role.as_deref().map_or(true, |value| {
                    contains_case_insensitive(node.role.as_str(), value)
                })
                && expected.name.as_deref().map_or(true, |value| {
                    node.name
                        .as_deref()
                        .is_some_and(|name| contains_case_insensitive(name, value))
                })
        })
        .map(|node| {
            serde_json::json!({
                "node_id": node.id.clone(),
                "role": node.role.clone(),
                "name": node.name.clone(),
                "selector_hints": node.selector_hints.clone(),
            })
        })
        .collect()
}

fn optional_match<'a>(expected: Option<&str>, actual: impl FnOnce() -> &'a str) -> bool {
    expected.map_or(true, |expected| actual() == expected)
}

fn contains_case_insensitive(haystack: &str, needle: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .contains(needle.to_ascii_lowercase().as_str())
}
