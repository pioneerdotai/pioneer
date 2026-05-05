use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptDiagnosticCode {
    MissingFile,
    FileReadError,
    FileTruncated,
    TotalBudgetTruncated,
    FileFilteredByProfile,
    DynamicSectionTruncated,
    DynamicSectionOmitted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptDiagnostic {
    pub code: PromptDiagnosticCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_id: Option<String>,
}

impl PromptDiagnostic {
    pub fn missing_file(name: &str, file: String) -> Self {
        Self {
            code: PromptDiagnosticCode::MissingFile,
            message: format!("bootstrap file `{name}` is missing"),
            file: Some(file),
            section_id: None,
        }
    }

    pub fn file_read_error(name: &str, file: String, error: &str) -> Self {
        Self {
            code: PromptDiagnosticCode::FileReadError,
            message: format!("bootstrap file `{name}` could not be read: {error}"),
            file: Some(file),
            section_id: None,
        }
    }

    pub fn filtered_by_profile(name: &str, file: String) -> Self {
        Self {
            code: PromptDiagnosticCode::FileFilteredByProfile,
            message: format!("bootstrap file `{name}` filtered by profile"),
            file: Some(file),
            section_id: None,
        }
    }

    pub fn file_truncated(
        name: &str,
        file: String,
        original_chars: usize,
        kept_chars: usize,
    ) -> Self {
        Self {
            code: PromptDiagnosticCode::FileTruncated,
            message: format!(
                "bootstrap file `{name}` truncated by per-file limit: {original_chars} -> {kept_chars} chars"
            ),
            file: Some(file),
            section_id: None,
        }
    }

    pub fn total_budget_truncated(
        name: &str,
        file: String,
        original_chars: usize,
        kept_chars: usize,
    ) -> Self {
        Self {
            code: PromptDiagnosticCode::TotalBudgetTruncated,
            message: format!(
                "bootstrap file `{name}` truncated by total budget: {original_chars} -> {kept_chars} chars"
            ),
            file: Some(file),
            section_id: None,
        }
    }

    pub fn dynamic_section_truncated(
        section_id: &str,
        original_chars: usize,
        kept_chars: usize,
    ) -> Self {
        Self {
            code: PromptDiagnosticCode::DynamicSectionTruncated,
            message: format!(
                "dynamic prompt section `{section_id}` truncated: {original_chars} -> {kept_chars} chars"
            ),
            file: None,
            section_id: Some(section_id.to_owned()),
        }
    }

    pub fn dynamic_section_omitted(section_id: &str, reason: &str) -> Self {
        Self {
            code: PromptDiagnosticCode::DynamicSectionOmitted,
            message: format!("dynamic prompt section `{section_id}` omitted: {reason}"),
            file: None,
            section_id: Some(section_id.to_owned()),
        }
    }
}
