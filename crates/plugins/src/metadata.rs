//! Best-effort authored presentation. Secondary manifests never affect execution.
use crate::Snapshot;
use pioneer_protocol::{PluginIcon, PluginMetadata};
use serde_json::Value;
use sha2::{Digest, Sha256};

const MANIFESTS: [&str; 4] = [
    "plugin.json",
    ".codex-plugin/plugin.json",
    ".claude-plugin/plugin.json",
    ".cursor-plugin/plugin.json",
];

fn text(value: &Value, pointers: &[&str], limit: usize) -> Option<String> {
    pointers.iter().find_map(|pointer| {
        let value = value.pointer(pointer)?.as_str()?.trim();
        let value: String = value
            .chars()
            .filter(|c| !c.is_control() || matches!(c, '\n' | '\t'))
            .take(limit)
            .collect();
        (!value.trim().is_empty()).then_some(value)
    })
}
fn fill(target: &mut Option<String>, candidate: Option<String>) {
    if target.is_none() {
        *target = candidate;
    }
}
fn strings(value: &Value, pointers: &[&str], count: usize, length: usize) -> Vec<String> {
    pointers
        .iter()
        .find_map(|pointer| {
            let list: Vec<_> = value
                .pointer(pointer)?
                .as_array()?
                .iter()
                .filter_map(|v| text(v, &[""], length))
                .take(count)
                .collect();
            (!list.is_empty()).then_some(list)
        })
        .unwrap_or_default()
}

pub(crate) fn resolve(snapshot: &Snapshot, name: &str) -> (PluginMetadata, Option<String>) {
    let mut metadata = PluginMetadata::default();
    let mut version = None;
    let mut logos = Vec::new();
    let mut composer_icons = Vec::new();
    for path in MANIFESTS {
        let Some(bytes) = snapshot
            .file(path)
            .filter(|bytes| bytes.len() <= 1024 * 1024)
        else {
            continue;
        };
        let Ok(document) = serde_json::from_slice::<Value>(bytes) else {
            continue;
        };
        if document.get("name").and_then(Value::as_str) != Some(name) {
            continue;
        }
        // Only the explicitly supported Pioneer namespace is interpreted in the portable manifest.
        let owned = if path == "plugin.json" {
            document
                .pointer("/extensions/com.pioneer/interface")
                .or_else(|| document.pointer("/extensions/com.pioneer"))
        } else {
            document.get("interface")
        };
        let core = if path == "plugin.json" {
            Value::Object(
                document
                    .as_object()
                    .into_iter()
                    .flat_map(|object| object.iter())
                    .filter(|(key, _)| {
                        matches!(
                            key.as_str(),
                            "name"
                                | "version"
                                | "description"
                                | "author"
                                | "homepage"
                                | "repository"
                                | "license"
                                | "keywords"
                        )
                    })
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            )
        } else {
            document.clone()
        };
        let candidates = owned.into_iter().chain(std::iter::once(&core));
        for value in candidates {
            fill(
                &mut metadata.display_name,
                text(value, &["/displayName"], 256),
            );
            fill(
                &mut metadata.short_description,
                text(value, &["/shortDescription"], 2048),
            );
            fill(
                &mut metadata.description,
                text(value, &["/description"], 2048),
            );
            fill(
                &mut metadata.long_description,
                text(value, &["/longDescription"], 16384),
            );
            fill(
                &mut metadata.developer_name,
                text(value, &["/developerName", "/author/name"], 256),
            );
            fill(
                &mut metadata.homepage,
                text(value, &["/websiteURL", "/homepage"], 2048),
            );
            fill(
                &mut metadata.repository,
                text(value, &["/repository"], 2048),
            );
            fill(
                &mut metadata.documentation_url,
                text(value, &["/documentationURL", "/documentationUrl"], 2048),
            );
            fill(
                &mut metadata.support_url,
                text(value, &["/supportURL", "/supportUrl"], 2048),
            );
            fill(
                &mut metadata.privacy_policy_url,
                text(value, &["/privacyPolicyURL", "/privacyPolicyUrl"], 2048),
            );
            fill(
                &mut metadata.terms_of_service_url,
                text(value, &["/termsOfServiceURL", "/termsOfServiceUrl"], 2048),
            );
            fill(&mut metadata.license, text(value, &["/license"], 256));
            fill(&mut metadata.category, text(value, &["/category"], 128));
            fill(&mut metadata.brand_color, text(value, &["/brandColor"], 64));
            fill(&mut version, text(value, &["/version"], 128));
            let author_name = text(value, &["/author/name"], 256);
            let same_author = metadata
                .author
                .name
                .as_ref()
                .zip(author_name.as_ref())
                .is_none_or(|(a, b)| a.eq_ignore_ascii_case(b));
            if same_author {
                fill(&mut metadata.author.name, author_name);
                fill(
                    &mut metadata.author.email,
                    text(value, &["/author/email"], 512),
                );
                fill(
                    &mut metadata.author.url,
                    text(value, &["/author/url"], 2048),
                );
            }
            if metadata.keywords.is_empty() {
                metadata.keywords = strings(value, &["/keywords"], 64, 128);
            }
            if metadata.default_prompts.is_empty() {
                metadata.default_prompts =
                    strings(value, &["/defaultPrompt", "/defaultPrompts"], 12, 1024);
                if metadata.default_prompts.is_empty() {
                    metadata
                        .default_prompts
                        .extend(text(value, &["/defaultPrompt"], 1024));
                }
            }
            for pointer in ["/logo", "/icon"] {
                if let Some(path) = text(value, &[pointer], 1024) {
                    logos.push(path);
                }
            }
            if let Some(path) = text(value, &["/composerIcon"], 1024) {
                composer_icons.push(path);
            }
        }
    }
    metadata.logo = logos.iter().find_map(|path| icon(snapshot, path));
    metadata.composer_icon = composer_icons
        .iter()
        .chain(logos.iter())
        .find_map(|path| icon(snapshot, path));
    if metadata.short_description.is_none() {
        metadata.short_description = metadata.description.clone();
    }
    if metadata.description.is_none() {
        metadata.description = metadata.short_description.clone();
    }
    if metadata.long_description.is_none() {
        metadata.long_description = metadata.description.clone();
    }
    if metadata.developer_name.is_none() {
        metadata.developer_name = metadata.author.name.clone();
    }
    (metadata, version)
}

fn icon(snapshot: &Snapshot, path: &str) -> Option<PluginIcon> {
    let path = path.strip_prefix("./").unwrap_or(path);
    // References may only name contained package files. Image delivery and validation
    // belong to the existing native avatar service, not to the metadata parser.
    if path.starts_with('/')
        || path.contains('\\')
        || path.contains(':')
        || path.split('/').any(|part| matches!(part, ".." | "." | ""))
    {
        return None;
    }
    let bytes = snapshot
        .file(path)
        .filter(|bytes| !bytes.is_empty() && bytes.len() <= pioneer_protocol::IMAGE_MAX_BYTES)?;
    let raster = bytes.starts_with(b"\x89PNG\r\n\x1a\n")
        || bytes.starts_with(&[0xff, 0xd8, 0xff])
        || (bytes.starts_with(b"RIFF") && bytes.get(8..12).is_some_and(|magic| magic == b"WEBP"));
    raster.then(|| PluginIcon {
        path: path.into(),
        revision: hex::encode(Sha256::digest(bytes)),
    })
}
