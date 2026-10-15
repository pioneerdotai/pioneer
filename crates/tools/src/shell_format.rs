use serde::{Deserialize, Serialize};

const TRUNCATION_SUFFIX: &str = "\n... [output truncated]";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecTruncation {
    pub stdout: bool,
    pub stderr: bool,
    pub aggregated_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecStreamOutputStats {
    pub bytes_seen: usize,
    pub bytes_retained: usize,
    pub bytes_dropped: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecOutputStats {
    pub stdout: ExecStreamOutputStats,
    pub stderr: ExecStreamOutputStats,
    pub truncation_method: String,
    pub full_output_available: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecModelPayload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub duration_ms: u64,
    pub stdout: String,
    pub stderr: String,
    pub aggregated_output: String,
    pub truncated: ExecTruncation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_stats: Option<ExecOutputStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<u64>,
    pub command: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ExecPayloadInput {
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub duration_ms: u64,
    pub stdout: String,
    pub stderr: String,
    pub session_id: Option<u64>,
    pub command: Vec<String>,
    pub max_output_tokens: Option<usize>,
    pub force_truncated_stdout: bool,
    pub force_truncated_stderr: bool,
}

pub fn build_exec_model_payload(input: ExecPayloadInput) -> ExecModelPayload {
    build_exec_model_payload_with_stats(input, None)
}

pub fn build_exec_model_payload_with_stats(
    input: ExecPayloadInput,
    output_stats: Option<ExecOutputStats>,
) -> ExecModelPayload {
    let aggregated_raw = aggregate_output(input.stdout.as_str(), input.stderr.as_str());

    let (stdout, stdout_truncated) =
        truncate_output(input.stdout.as_str(), input.max_output_tokens);
    let (stderr, stderr_truncated) =
        truncate_output(input.stderr.as_str(), input.max_output_tokens);
    let (aggregated_output, aggregated_truncated) =
        truncate_output(aggregated_raw.as_str(), input.max_output_tokens);

    ExecModelPayload {
        exit_code: input.exit_code,
        timed_out: input.timed_out,
        duration_ms: input.duration_ms,
        stdout,
        stderr,
        aggregated_output,
        truncated: ExecTruncation {
            stdout: stdout_truncated || input.force_truncated_stdout,
            stderr: stderr_truncated || input.force_truncated_stderr,
            aggregated_output: aggregated_truncated
                || input.force_truncated_stdout
                || input.force_truncated_stderr,
        },
        output_stats,
        session_id: input.session_id,
        command: input.command,
    }
}

pub fn render_exec_ui_text(payload: &ExecModelPayload) -> String {
    let mut sections = Vec::new();

    if let Some(session_id) = payload.session_id {
        sections.push(format!("Session ID: {session_id}"));
    }

    if !payload.command.is_empty() {
        sections.push(format!("Command: {}", payload.command.join(" ")));
    }

    if let Some(code) = payload.exit_code {
        sections.push(format!("Exit Code: {code}"));
    }

    sections.push(format!("Duration: {}ms", payload.duration_ms));

    if payload.timed_out {
        sections.push("Timed Out: true".to_owned());
    }

    sections.push(String::new());
    sections.push("stdout:".to_owned());
    sections.push(payload.stdout.clone());
    sections.push(String::new());
    sections.push("stderr:".to_owned());
    sections.push(payload.stderr.clone());

    sections.join("\n")
}

fn aggregate_output(stdout: &str, stderr: &str) -> String {
    if stdout.is_empty() {
        return stderr.to_owned();
    }
    if stderr.is_empty() {
        return stdout.to_owned();
    }
    format!("{stdout}\n{stderr}")
}

fn truncate_output(text: &str, max_output_tokens: Option<usize>) -> (String, bool) {
    let Some(max_tokens) = max_output_tokens else {
        return (text.to_owned(), false);
    };

    if max_tokens == 0 {
        return (String::new(), !text.is_empty());
    }

    let max_chars = max_tokens.saturating_mul(4);
    let total_chars = text.chars().count();
    if total_chars <= max_chars {
        return (text.to_owned(), false);
    }

    let mut truncated = String::new();
    for (idx, ch) in text.chars().enumerate() {
        if idx >= max_chars {
            break;
        }
        truncated.push(ch);
    }
    truncated.push_str(TRUNCATION_SUFFIX);
    (truncated, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload_json(input: ExecPayloadInput) -> String {
        let payload = build_exec_model_payload(input);
        serde_json::to_string_pretty(&payload).expect("payload should serialize")
    }

    fn assert_json_eq(actual: &str, expected: &str) {
        let actual_value: serde_json::Value =
            serde_json::from_str(actual).expect("actual payload should be valid json");
        let expected_value: serde_json::Value =
            serde_json::from_str(expected).expect("expected payload should be valid json");
        assert_eq!(actual_value, expected_value);
    }

    #[test]
    fn snapshot_success_payload() {
        let json = payload_json(ExecPayloadInput {
            exit_code: Some(0),
            timed_out: false,
            duration_ms: 12,
            stdout: "ok\n".to_owned(),
            stderr: String::new(),
            session_id: None,
            command: vec!["/bin/sh".to_owned(), "-c".to_owned(), "echo ok".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });
        assert_json_eq(
            json.as_str(),
            r#"{
            "exit_code": 0,
            "timed_out": false,
            "duration_ms": 12,
            "stdout": "ok\n",
            "stderr": "",
            "aggregated_output": "ok\n",
            "truncated": {
                "stdout": false,
                "stderr": false,
                "aggregated_output": false
            },
            "command": [
                "/bin/sh",
                "-c",
                "echo ok"
            ]
            }"#,
        );
    }

    #[test]
    fn snapshot_non_zero_payload() {
        let json = payload_json(ExecPayloadInput {
            exit_code: Some(2),
            timed_out: false,
            duration_ms: 44,
            stdout: String::new(),
            stderr: "boom".to_owned(),
            session_id: None,
            command: vec!["sh".to_owned(), "-c".to_owned(), "exit 2".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });
        assert_json_eq(
            json.as_str(),
            r#"{
            "exit_code": 2,
            "timed_out": false,
            "duration_ms": 44,
            "stdout": "",
            "stderr": "boom",
            "aggregated_output": "boom",
            "truncated": {
                "stdout": false,
                "stderr": false,
                "aggregated_output": false
            },
            "command": [
                "sh",
                "-c",
                "exit 2"
            ]
            }"#,
        );
    }

    #[test]
    fn snapshot_signal_payload() {
        let json = payload_json(ExecPayloadInput {
            exit_code: None,
            timed_out: false,
            duration_ms: 30,
            stdout: String::new(),
            stderr: "terminated by signal".to_owned(),
            session_id: None,
            command: vec!["sh".to_owned(), "-c".to_owned(), "kill -TERM $$".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });
        assert_json_eq(
            json.as_str(),
            r#"{
            "timed_out": false,
            "duration_ms": 30,
            "stdout": "",
            "stderr": "terminated by signal",
            "aggregated_output": "terminated by signal",
            "truncated": {
                "stdout": false,
                "stderr": false,
                "aggregated_output": false
            },
            "command": [
                "sh",
                "-c",
                "kill -TERM $$"
            ]
            }"#,
        );
    }

    #[test]
    fn snapshot_timeout_payload() {
        let json = payload_json(ExecPayloadInput {
            exit_code: None,
            timed_out: true,
            duration_ms: 1_000,
            stdout: String::new(),
            stderr: "command timed out".to_owned(),
            session_id: None,
            command: vec!["sh".to_owned(), "-c".to_owned(), "sleep 5".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });
        assert_json_eq(
            json.as_str(),
            r#"{
            "timed_out": true,
            "duration_ms": 1000,
            "stdout": "",
            "stderr": "command timed out",
            "aggregated_output": "command timed out",
            "truncated": {
                "stdout": false,
                "stderr": false,
                "aggregated_output": false
            },
            "command": [
                "sh",
                "-c",
                "sleep 5"
            ]
            }"#,
        );
    }

    #[test]
    fn snapshot_huge_output_payload() {
        let json = payload_json(ExecPayloadInput {
            exit_code: Some(0),
            timed_out: false,
            duration_ms: 9,
            stdout: "abcdefghijklmnopqrstuvwxyz".to_owned(),
            stderr: String::new(),
            session_id: None,
            command: vec!["echo".to_owned(), "long".to_owned()],
            max_output_tokens: Some(2),
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });
        assert_json_eq(
            json.as_str(),
            r#"{
            "exit_code": 0,
            "timed_out": false,
            "duration_ms": 9,
            "stdout": "abcdefgh\n... [output truncated]",
            "stderr": "",
            "aggregated_output": "abcdefgh\n... [output truncated]",
            "truncated": {
                "stdout": true,
                "stderr": false,
                "aggregated_output": true
            },
            "command": [
                "echo",
                "long"
            ]
            }"#,
        );
    }

    #[test]
    fn snapshot_tty_session_chunk_and_final_payload() {
        let chunk = payload_json(ExecPayloadInput {
            exit_code: None,
            timed_out: false,
            duration_ms: 120,
            stdout: "partial".to_owned(),
            stderr: String::new(),
            session_id: Some(7),
            command: vec!["python".to_owned(), "-i".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });
        assert_json_eq(
            chunk.as_str(),
            r#"{
            "timed_out": false,
            "duration_ms": 120,
            "stdout": "partial",
            "stderr": "",
            "aggregated_output": "partial",
            "truncated": {
                "stdout": false,
                "stderr": false,
                "aggregated_output": false
            },
            "session_id": 7,
            "command": [
                "python",
                "-i"
            ]
            }"#,
        );

        let final_payload = payload_json(ExecPayloadInput {
            exit_code: Some(0),
            timed_out: false,
            duration_ms: 240,
            stdout: "done".to_owned(),
            stderr: String::new(),
            session_id: Some(7),
            command: vec!["python".to_owned(), "-i".to_owned()],
            max_output_tokens: None,
            force_truncated_stdout: false,
            force_truncated_stderr: false,
        });
        assert_json_eq(
            final_payload.as_str(),
            r#"{
            "exit_code": 0,
            "timed_out": false,
            "duration_ms": 240,
            "stdout": "done",
            "stderr": "",
            "aggregated_output": "done",
            "truncated": {
                "stdout": false,
                "stderr": false,
                "aggregated_output": false
            },
            "session_id": 7,
            "command": [
                "python",
                "-i"
            ]
            }"#,
        );
    }
}
