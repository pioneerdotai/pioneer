use crate::constants::files::BootstrapFileKind;
use crate::diagnostics::PromptDiagnostic;
use crate::sources::files::LoadedBootstrapFile;
use crate::sources::sanitize::sanitize_file_content;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetedBootstrapFile {
    pub kind: BootstrapFileKind,
    pub name: String,
    pub path: std::path::PathBuf,
    pub content: String,
}

fn floor_char_boundary(value: &str, index: usize) -> usize {
    if index >= value.len() {
        return value.len();
    }
    let mut position = index;
    while position > 0 && !value.is_char_boundary(position) {
        position -= 1;
    }
    position
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    let mut byte_index = 0usize;
    for (chars_seen, (idx, ch)) in value.char_indices().enumerate() {
        if chars_seen == max_chars {
            byte_index = idx;
            break;
        }
        byte_index = idx + ch.len_utf8();
    }
    let bounded = floor_char_boundary(value, byte_index);
    value[..bounded].to_owned()
}

pub fn apply_budgets(
    files: Vec<LoadedBootstrapFile>,
    max_chars_per_file: usize,
    max_chars_total: usize,
) -> (Vec<BudgetedBootstrapFile>, Vec<PromptDiagnostic>) {
    let mut diagnostics = Vec::new();
    let mut budgeted = Vec::with_capacity(files.len());

    let mut total_used = 0usize;

    for file in files {
        let sanitized = sanitize_file_content(file.content.as_str());
        let original_count = sanitized.chars().count();

        let mut content = if max_chars_per_file == 0 {
            String::new()
        } else {
            truncate_chars(sanitized.as_str(), max_chars_per_file)
        };

        let per_file_count = content.chars().count();
        if per_file_count < original_count {
            diagnostics.push(PromptDiagnostic::file_truncated(
                file.name.as_str(),
                file.path.display().to_string(),
                original_count,
                per_file_count,
            ));
        }

        let remaining = max_chars_total.saturating_sub(total_used);
        let remaining_before_total = remaining;
        let content_count = content.chars().count();

        if content_count > remaining {
            content = truncate_chars(content.as_str(), remaining);
            diagnostics.push(PromptDiagnostic::total_budget_truncated(
                file.name.as_str(),
                file.path.display().to_string(),
                content_count,
                remaining,
            ));
        }

        total_used += content.chars().count();

        let preserve_empty_identity_file = matches!(
            file.kind,
            BootstrapFileKind::Soul | BootstrapFileKind::Identity
        ) && original_count == 0;

        if remaining_before_total == 0 || (content.is_empty() && !preserve_empty_identity_file) {
            continue;
        }

        budgeted.push(BudgetedBootstrapFile {
            kind: file.kind,
            name: file.name,
            path: file.path,
            content,
        });
    }

    (budgeted, diagnostics)
}

#[cfg(test)]
mod tests {
    use super::apply_budgets;
    use crate::constants::files::BootstrapFileKind;
    use crate::sources::files::LoadedBootstrapFile;
    use std::path::PathBuf;

    fn file(name: &str, content: &str) -> LoadedBootstrapFile {
        LoadedBootstrapFile {
            kind: BootstrapFileKind::Soul,
            name: name.to_owned(),
            path: PathBuf::from(format!("/tmp/{name}")),
            content: content.to_owned(),
        }
    }

    #[test]
    fn per_file_truncation_applies() {
        let (files, diagnostics) = apply_budgets(vec![file("AGENTS.md", "abcdefgh")], 4, 100);
        assert_eq!(files[0].content, "abcd");
        assert!(diagnostics.iter().any(|d| d.message.contains("per-file")));
    }

    #[test]
    fn total_budget_truncation_applies_in_order() {
        let (files, diagnostics) =
            apply_budgets(vec![file("A.md", "12345"), file("B.md", "67890")], 10, 7);
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].content, "12345");
        assert_eq!(files[1].content, "67");
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("total budget"))
        );
    }
}
