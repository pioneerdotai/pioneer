use std::collections::BTreeMap;
use std::path::Path;

use pioneer_protocol::ArtifactKind;
use serde_json::{Value, json};

pub const OCTET_STREAM: &str = "application/octet-stream";

pub const MAX_MIME_SNIFF_BYTES: usize = 8192;

pub fn infer_mime_from_path(path: &Path) -> String {
    mime_guess::from_path(path)
        .first()
        .map(|mime| mime.essence_str().to_owned())
        .unwrap_or_else(|| OCTET_STREAM.to_owned())
}

pub fn detect_mime_from_bytes(bytes: &[u8], path_hint: Option<&Path>) -> String {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return "image/png".to_owned();
    }
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        return "image/jpeg".to_owned();
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return "image/gif".to_owned();
    }
    if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        return "image/webp".to_owned();
    }
    if bytes.starts_with(b"%PDF-") {
        return "application/pdf".to_owned();
    }
    if bytes.starts_with(b"PK\x03\x04") || bytes.starts_with(b"PK\x05\x06") {
        return "application/zip".to_owned();
    }
    if bytes.starts_with(&[0x1f, 0x8b]) {
        return "application/gzip".to_owned();
    }

    if is_likely_utf8_text(bytes) {
        let trimmed = std::str::from_utf8(&bytes[..bytes.len().min(MAX_MIME_SNIFF_BYTES)])
            .unwrap_or_default()
            .trim_start();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            return "application/json".to_owned();
        }
        if path_has_extension(path_hint, &["csv"]) {
            return "text/csv".to_owned();
        }
        if path_has_extension(path_hint, &["tsv"]) {
            return "text/tab-separated-values".to_owned();
        }
        return "text/plain".to_owned();
    }

    OCTET_STREAM.to_owned()
}

pub fn effective_mime_type(declared: Option<&str>, detected: &str) -> String {
    if detected != OCTET_STREAM {
        return detected.to_owned();
    }
    declared
        .map(normalize_mime_type)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| OCTET_STREAM.to_owned())
}

pub fn normalize_mime_type(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

pub fn record_mime_metadata(
    metadata: &mut BTreeMap<String, Value>,
    declared: Option<&str>,
    detected: &str,
    effective: &str,
) {
    if let Some(declared) = declared {
        metadata.insert("declared_mime_type".to_owned(), json!(declared));
    } else {
        metadata.remove("declared_mime_type");
    }
    metadata.insert("detected_mime_type".to_owned(), json!(detected));
    metadata.insert("effective_mime_type".to_owned(), json!(effective));
    if let Some(declared) = declared {
        metadata.insert(
            "declared_detected_mime_mismatch".to_owned(),
            json!(detected != OCTET_STREAM && declared != detected),
        );
    } else {
        metadata.remove("declared_detected_mime_mismatch");
    }
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

pub fn display_name_with_mime_extension(display_name: String, mime_type: Option<&str>) -> String {
    if display_name_has_known_extension(display_name.as_str()) {
        return display_name;
    }
    let Some(extension) = mime_type.and_then(preferred_extension_for_mime_type) else {
        return display_name;
    };
    format!("{display_name}.{extension}")
}

pub fn preferred_extension_for_mime_type(mime_type: &str) -> Option<&'static str> {
    let mime_type = normalize_mime_type(mime_type);
    if mime_type.is_empty() || mime_type == OCTET_STREAM {
        return None;
    }
    match mime_type.as_str() {
        "image/jpeg" => Some("jpg"),
        "image/svg+xml" => Some("svg"),
        "text/plain" => Some("txt"),
        "text/tab-separated-values" => Some("tsv"),
        _ => mime_guess::get_mime_extensions_str(mime_type.as_str())
            .and_then(|extensions| extensions.first().copied()),
    }
}

fn display_name_has_known_extension(display_name: &str) -> bool {
    Path::new(display_name)
        .extension()
        .and_then(|extension| extension.to_str())
        .filter(|extension| !extension.trim().is_empty())
        .is_some_and(|extension| mime_guess::from_ext(extension).first().is_some())
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

fn is_likely_utf8_text(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return true;
    }
    let sample = &bytes[..bytes.len().min(MAX_MIME_SNIFF_BYTES)];
    std::str::from_utf8(sample).is_ok()
        && !sample
            .iter()
            .any(|byte| *byte < 0x09 || (*byte > 0x0d && *byte < 0x20))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mime_detects_png_from_bytes() {
        let bytes = b"\x89PNG\r\n\x1a\nrest";
        assert_eq!(detect_mime_from_bytes(bytes, None), "image/png");
    }

    #[test]
    fn mime_detects_json_text_from_bytes() {
        assert_eq!(
            detect_mime_from_bytes(br#"{"ok":true}"#, Some(Path::new("data.txt"))),
            "application/json"
        );
    }

    #[test]
    fn mime_effective_uses_declared_only_when_detection_is_unknown() {
        assert_eq!(
            effective_mime_type(Some("Text/Markdown; charset=utf-8"), OCTET_STREAM),
            "text/markdown"
        );
        assert_eq!(
            effective_mime_type(Some("text/plain"), "image/png"),
            "image/png"
        );
    }

    #[test]
    fn mime_records_declared_detected_metadata() {
        let mut metadata = BTreeMap::new();

        record_mime_metadata(&mut metadata, Some("text/plain"), "image/png", "image/png");

        assert_eq!(
            metadata.get("declared_mime_type"),
            Some(&json!("text/plain"))
        );
        assert_eq!(
            metadata.get("detected_mime_type"),
            Some(&json!("image/png"))
        );
        assert_eq!(
            metadata.get("effective_mime_type"),
            Some(&json!("image/png"))
        );
        assert_eq!(
            metadata.get("declared_detected_mime_mismatch"),
            Some(&json!(true))
        );
    }

    #[test]
    fn display_name_extension_is_added_from_mime_only_when_missing() {
        assert_eq!(
            display_name_with_mime_extension("auto.ru_screenshot".to_owned(), Some("image/png")),
            "auto.ru_screenshot.png"
        );
        assert_eq!(
            display_name_with_mime_extension("photo.jpeg".to_owned(), Some("image/png")),
            "photo.jpeg"
        );
        assert_eq!(
            display_name_with_mime_extension("unknown".to_owned(), Some(OCTET_STREAM)),
            "unknown"
        );
    }
}
