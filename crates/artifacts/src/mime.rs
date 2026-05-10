use std::path::Path;

use pioneer_protocol::ArtifactKind;

pub const OCTET_STREAM: &str = "application/octet-stream";

pub fn infer_mime_from_path(path: &Path) -> String {
    mime_guess::from_path(path)
        .first()
        .map(|mime| mime.essence_str().to_owned())
        .unwrap_or_else(|| OCTET_STREAM.to_owned())
}

pub fn classify_kind(mime_type: Option<&str>, path: Option<&Path>) -> ArtifactKind {
    let mime_type = mime_type.unwrap_or(OCTET_STREAM).to_ascii_lowercase();
    if mime_type == "application/pdf" {
        return ArtifactKind::Pdf;
    }
    if mime_type == "application/json"
        || mime_type.ends_with("+json")
        || path_has_extension(path, &["json", "jsonl"])
    {
        return ArtifactKind::Json;
    }
    if mime_type.starts_with("image/") {
        return ArtifactKind::Image;
    }
    if mime_type.starts_with("audio/") {
        return ArtifactKind::Audio;
    }
    if mime_type.starts_with("video/") {
        return ArtifactKind::Video;
    }
    if mime_type.starts_with("text/") {
        if mime_type == "text/csv" || path_has_extension(path, &["csv", "tsv"]) {
            return ArtifactKind::Spreadsheet;
        }
        return ArtifactKind::Text;
    }
    if mime_type.contains("spreadsheet")
        || mime_type.contains("excel")
        || path_has_extension(path, &["xls", "xlsx", "ods"])
    {
        return ArtifactKind::Spreadsheet;
    }
    if mime_type.contains("zip")
        || mime_type.contains("tar")
        || mime_type.contains("gzip")
        || path_has_extension(path, &["zip", "tar", "gz", "tgz"])
    {
        return ArtifactKind::Archive;
    }

    ArtifactKind::File
}

pub fn sanitize_display_name(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_control() || ch == '/' || ch == '\\' {
                '_'
            } else {
                ch
            }
        })
        .collect::<String>();
    let trimmed = sanitized.trim();
    if trimmed.is_empty() {
        "artifact".to_owned()
    } else {
        trimmed.to_owned()
    }
}

pub fn is_safe_visible_name(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| !ch.is_control() && ch != '/' && ch != '\\')
}

fn path_has_extension(path: Option<&Path>, extensions: &[&str]) -> bool {
    path.and_then(Path::extension)
        .and_then(|value| value.to_str())
        .map(|extension| {
            extensions
                .iter()
                .any(|expected| extension.eq_ignore_ascii_case(expected))
        })
        .unwrap_or(false)
}
