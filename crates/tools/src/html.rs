//! Shared first-party HTML contracts used by native tools and managed CLI facades.
use crate::{
    ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolIdempotencyMode, ToolOutputPolicySnapshot,
    ToolRecoveryMetadata, ToolRetryClass, ToolSpec,
};
use serde::Deserialize;
use serde_json::{Value, json};

pub const HTML_PREVIEW: &str = "html_preview";
pub const HTML_RENDER: &str = "html_render";
pub const HTML_SERVER_ID: &str = "pioneer-html-tools-v1";
pub const HTML_SERVER_NAME: &str = "pioneer_html";
pub const PREVIEW_WIDTHS: [u32; 3] = [360, 768, 1024];
pub const PREVIEW_FRAMES: [(&str, u32); 6] = [
    ("light", 360),
    ("light", 768),
    ("light", 1024),
    ("dark", 360),
    ("dark", 768),
    ("dark", 1024),
];

/// Unversioned, appearance-free three-frame results are the recorded v1 format.
/// A versioned result must always contain the complete ordered v2 set.
pub fn preview_image_layout(payload: &Value) -> Option<Vec<(Option<&'static str>, u32)>> {
    let images = payload.pointer("/llm_context/images")?.as_array()?;
    let layout = match payload.get("formatVersion") {
        None if images.len() == 3
            && images.iter().all(|image| image.get("appearance").is_none()) =>
        {
            PREVIEW_WIDTHS
                .iter()
                .map(|&width| (None, width))
                .collect::<Vec<_>>()
        }
        Some(version) if version == 2 && images.len() == PREVIEW_FRAMES.len() => PREVIEW_FRAMES
            .iter()
            .map(|&(appearance, width)| (Some(appearance), width))
            .collect(),
        _ => return None,
    };
    if images
        .iter()
        .zip(&layout)
        .any(|(image, (appearance, width))| {
            image["width"] != *width
                || appearance.is_some_and(|appearance| image["appearance"] != appearance)
        })
    {
        return None;
    }
    Some(layout)
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Appearance {
    #[default]
    Dark,
    Light,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreviewParams {
    pub html: String,
    // Accepted only to deserialize recorded v1 calls. New previews always capture both themes.
    #[serde(default)]
    pub appearance: Appearance,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenderParams {
    pub html: String,
    pub title: String,
}

pub fn validate_html(html: &str) -> Result<(), crate::ToolError> {
    if !(1..=512_000).contains(&html.chars().count()) {
        return Err(crate::ToolError::invalid_arguments(
            "html must contain 1 through 512000 characters",
        ));
    }
    Ok(())
}
pub fn validate_title(title: &str) -> Result<(), crate::ToolError> {
    if !(1..=200).contains(&title.chars().count()) {
        return Err(crate::ToolError::invalid_arguments(
            "title must contain 1 through 200 characters",
        ));
    }
    Ok(())
}
pub fn html_tool_specs() -> Vec<ConfiguredToolSpec> {
    definitions()
        .into_iter()
        .map(|definition| {
            let name = definition["name"].as_str().expect("static name");
            ConfiguredToolSpec::new(
                ToolSpec::new(
                    name,
                    definition["description"]
                        .as_str()
                        .expect("static description"),
                    definition["inputSchema"].clone(),
                    PayloadKind::Function,
                )
                .with_recovery(ToolRecoveryMetadata {
                    retry_class: ToolRetryClass::Never,
                    idempotency_mode: ToolIdempotencyMode::Safe,
                    max_attempts: 1,
                    can_resume: false,
                    max_wall_clock_secs: Some(120),
                }),
                ExecutionClass::Shared,
                ToolOutputPolicySnapshot::for_tool_name(name),
            )
        })
        .collect()
}
pub fn definition(name: &str) -> Option<Value> {
    definitions().into_iter().find(|d| d["name"] == name)
}
/// Exact recorded contract from c0f9f151ef43a3b57a6bec279a347bca9a8526e2.
/// Recovery only: never include this definition in a new turn's catalog.
pub fn legacy_preview_definition() -> Value {
    json!({
      "name": "html_preview",
      "description": "Render the same HTML page in Pioneer's headless browser at three viewport widths: 360 CSS pixels for mobile, 768 for tablet and 1024 for desktop. Return three separate PNG screenshots in that order, with content height and console output for each viewport, including uncaught exceptions. Use console.log to report your own checks. Check and iterate on the page before html_render. Submit a complete HTML document; include your CSS and JavaScript in it. Submit finished HTML and JavaScript, not TypeScript or TSX. Use a fluid width and let content set the page height. Avoid 100vh and height:100% on html or body. Do not add an outer card, border, banner title or horizontal padding. Use the host-provided --pioneer-* theme variables; the page follows the user interface theme.",
      "inputSchema": {
        "type": "object",
        "additionalProperties": false,
        "required": [
          "html"
        ],
        "properties": {
          "html": {
            "type": "string",
            "minLength": 1,
            "maxLength": 512000,
            "description": "A complete HTML document."
          },
          "appearance": {
            "type": "string",
            "enum": [
              "dark",
              "light"
            ],
            "default": "dark",
            "description": "Theme for all three previews. Defaults to dark."
          }
        }
      }
    })
}

fn definitions() -> Vec<Value> {
    vec![
        json!({
          "name": "html_preview",
          "description": "Render the same HTML page in Pioneer's headless browser in both standard Pioneer themes at three viewport widths: 360 CSS pixels for mobile, 768 for tablet and 1024 for desktop. Return six separate PNG screenshots in fixed order: light 360/768/1024, then dark 360/768/1024, with content height and console output for each theme and viewport, including uncaught exceptions. Use console.log to report your own checks. Check and iterate on the page before html_render. Submit a complete HTML document; include your CSS and JavaScript in it. Submit finished HTML and JavaScript, not TypeScript or TSX. Use a fluid width and let content set the page height. Avoid 100vh and height:100% on html or body. Do not add an outer card, border, banner title or horizontal padding. Use the host-provided --pioneer-* theme variables; the page follows the user interface theme.",
          "inputSchema": {
            "type": "object",
            "additionalProperties": false,
            "required": [
              "html"
            ],
            "properties": {
              "html": {
                "type": "string",
                "minLength": 1,
                "maxLength": 512000,
                "description": "A complete HTML document."
              }
            }
          }
        }),
        json!({
          "name": "html_render",
          "description": "Include a finished HTML page in your response, before its final text; call this tool before writing that text. The page follows the response's delivery and review rules. Add only what the page does not say. Preview with html_preview first when available. The page occupies its full content height at the reader's width and scrolls with the conversation. You do not need to supply a height. Submit a complete HTML document; include your CSS and JavaScript in it. Submit finished HTML and JavaScript, not TypeScript or TSX. Use a fluid width and let content set the page height. Avoid 100vh and height:100% on html or body. Do not add an outer card, border, banner title or horizontal padding. Use the host-provided --pioneer-* theme variables; the page follows the user interface theme.",
          "inputSchema": {
            "type": "object",
            "additionalProperties": false,
            "required": [
              "html",
              "title"
            ],
            "properties": {
              "html": {
                "type": "string",
                "minLength": 1,
                "maxLength": 512000,
                "description": "A complete HTML document."
              },
              "title": {
                "type": "string",
                "minLength": 1,
                "maxLength": 200,
                "description": "Short name for the page."
              }
            }
          }
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inputs_reject_host_geometry_and_accept_recorded_appearance() {
        let params: PreviewParams = serde_json::from_value(json!({"html":"<p>test</p>"})).unwrap();
        assert!(matches!(params.appearance, Appearance::Dark));
        for appearance in ["light", "dark"] {
            assert!(
                serde_json::from_value::<PreviewParams>(
                    json!({"html":"x","appearance":appearance})
                )
                .is_ok()
            );
        }
        assert!(
            definition(HTML_PREVIEW).unwrap()["inputSchema"]["properties"]
                .get("appearance")
                .is_none()
        );
        for field in ["width", "height", "delay", "capture_mode"] {
            let mut value = json!({"html":"<p>test</p>"});
            value[field] = 1.into();
            assert!(serde_json::from_value::<PreviewParams>(value).is_err());
        }
        assert!(
            serde_json::from_value::<RenderParams>(json!({"html":"x","title":"x","height":10}))
                .is_err()
        );
        assert!(serde_json::from_value::<RenderParams>(json!({"html":"x"})).is_err());
        assert!(validate_html(&"я".repeat(512_000)).is_ok());
        assert!(validate_html(&"я".repeat(512_001)).is_err());
    }
}
