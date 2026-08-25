//! Provider-native file-tool capability and wire-shape declarations.
//!
//! Tool-side normalization and result projection live with the Apply Patch
//! implementation in `pioneer-tools`.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const NATIVE_FILE_TOOL_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativePatchWireShape {
    /// The provider sends the patch document itself, without a JSON wrapper.
    Freeform,
    /// The provider sends an object containing exactly one patch string field.
    JsonFunction,
    /// The provider/model combination is not trusted to expose native file
    /// tools.  No fallback mutator is implied by this value.
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NativeFileToolCapability {
    pub schema_version: u16,
    pub provider: String,
    pub model: String,
    pub patch_shape: NativePatchWireShape,
    pub read_file: bool,
    pub apply_patch: bool,
    pub reason: Option<String>,
}

impl NativeFileToolCapability {
    pub fn is_supported(&self) -> bool {
        self.read_file && self.apply_patch && self.patch_shape != NativePatchWireShape::Unavailable
    }

    pub fn unavailable(
        provider: impl Into<String>,
        model: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: NATIVE_FILE_TOOL_SCHEMA_VERSION,
            provider: provider.into(),
            model: model.into(),
            patch_shape: NativePatchWireShape::Unavailable,
            read_file: false,
            apply_patch: false,
            reason: Some(reason.into()),
        }
    }

    pub fn prompt_capability_metadata(&self) -> JsonValue {
        serde_json::json!({
            "schemaVersion": self.schema_version,
            "provider": self.provider,
            "model": self.model,
            "readFile": self.read_file,
            "applyPatch": self.apply_patch,
            "patchWireShape": self.patch_shape,
            "reason": self.reason,
        })
    }
}

/// Select one immutable file-tool contract from trusted provider/model facts.
/// Unknown providers and obviously placeholder models fail closed.
pub fn select_native_file_tool_capability(provider: &str, model: &str) -> NativeFileToolCapability {
    let provider_key = provider.trim().to_ascii_lowercase();
    let model_value = model.trim();
    if model_value.is_empty()
        || model_value.eq_ignore_ascii_case("unknown")
        || model_value.eq_ignore_ascii_case("unsupported")
    {
        return NativeFileToolCapability::unavailable(
            provider_key,
            model_value,
            "model identity is missing or explicitly unsupported",
        );
    }

    // These are the API/provider families currently represented by Pioneer.
    // They all expose ordinary JSON function calls at the native transport
    // boundary.  A genuinely free-form provider must be added explicitly,
    // rather than inferred from an arbitrary provider name.
    const JSON_FUNCTION_PROVIDERS: &[&str] = &[
        "ai21",
        "ai21-labs",
        "anthropic",
        "anyscale",
        "astrai",
        "azure",
        "azure-openai",
        "azure_openai",
        "baichuan",
        "baseten",
        "bedrock",
        "bigmodel",
        "cerebras",
        "cohere",
        "copilot",
        "deep-infra",
        "deepinfra",
        "deepseek",
        "doubao",
        "fireworks",
        "fireworks-ai",
        "friendli",
        "friendliai",
        "glm",
        "glm-cn",
        "glm-global",
        "google",
        "google-gemini",
        "grok",
        "groq",
        "hf",
        "huggingface",
        "hunyuan",
        "lepton",
        "lepton-ai",
        "litellm",
        "lm-studio",
        "lmstudio",
        "mistral",
        "nebius",
        "nscale",
        "novita",
        "nvidia",
        "nvidia-nim",
        "ollama",
        "openai",
        "openrouter",
        "ovh",
        "ovhcloud",
        "perplexity",
        "qianfan",
        "reka",
        "sambanova",
        "sglang",
        "silicon-flow",
        "siliconflow",
        "step",
        "stepfun",
        "synthetic",
        "telnyx",
        "together",
        "together-ai",
        "tencent",
        "venice",
        "vllm",
        "volcengine",
        "xai",
        "yi",
        "zhipu",
        "zhipu-cn",
        "zhipu-global",
    ];

    if JSON_FUNCTION_PROVIDERS.contains(&provider_key.as_str()) {
        NativeFileToolCapability {
            schema_version: NATIVE_FILE_TOOL_SCHEMA_VERSION,
            provider: provider_key,
            model: model_value.to_owned(),
            patch_shape: NativePatchWireShape::JsonFunction,
            read_file: true,
            apply_patch: true,
            reason: None,
        }
    } else {
        NativeFileToolCapability::unavailable(
            provider_key,
            model_value,
            "provider family has no registered native file-tool wire contract",
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub enum NativePatchPayload<'a> {
    Freeform(&'a str),
    Json(&'a JsonValue),
}

pub fn read_file_tool_schema() -> JsonValue {
    serde_json::json!({
        "schemaVersion": NATIVE_FILE_TOOL_SCHEMA_VERSION,
        "input": {
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "Path relative to the turn cwd, or an authorized absolute file path." },
                "start_line": { "type": "integer", "minimum": 1 },
                "start_byte": { "type": "integer", "minimum": 0 },
                "max_lines": { "type": "integer", "minimum": 1 },
                "max_bytes": { "type": "integer", "minimum": 1 },
                "cursor": { "type": "string", "minLength": 1, "maxLength": 16384, "description": "Opaque continuation returned by the preceding page of this same file." }
            },
            "required": ["path"],
            "additionalProperties": false
        },
        "result": {
            "type": "object",
            "required": ["path", "text", "range", "truncated", "continuation", "version"],
            "properties": {
                "path": { "type": "string" },
                "text": { "type": "string" },
                "range": {
                    "type": "object",
                    "properties": {
                        "start": { "type": "integer", "minimum": 0 },
                        "end": { "type": "integer", "minimum": 0 },
                        "unit": { "const": "bytes" }
                    },
                    "required": ["start", "end", "unit"],
                    "additionalProperties": false
                },
                "continuation": { "type": ["string", "null"] },
                "version": { "type": "string", "pattern": "^sha256:[0-9a-f]{64}:[0-9]+$" },
                "output": { "type": "string" },
                "truncated": { "type": "boolean" }
            },
            "additionalProperties": true
        }
    })
}

pub fn apply_patch_tool_schema(shape: NativePatchWireShape) -> JsonValue {
    match shape {
        NativePatchWireShape::Freeform => serde_json::json!({
            "schemaVersion": NATIVE_FILE_TOOL_SCHEMA_VERSION,
            "type": "string",
            "description": "Pass the complete patch document directly as the tool input. Do not wrap it in JSON."
        }),
        NativePatchWireShape::JsonFunction => serde_json::json!({
            "schemaVersion": NATIVE_FILE_TOOL_SCHEMA_VERSION,
            "type": "object",
            "properties": {
                "patch": { "type": "string", "description": "The complete patch document in the syntax defined by the apply_patch tool description." }
            },
            "required": ["patch"],
            "additionalProperties": false
        }),
        NativePatchWireShape::Unavailable => JsonValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_provider_selects_one_json_shape() {
        let capability = select_native_file_tool_capability("openai", "gpt-5");
        assert_eq!(capability.patch_shape, NativePatchWireShape::JsonFunction);
        assert!(capability.is_supported());
    }

    #[test]
    fn unknown_provider_fails_closed_without_fallback_mutator() {
        let capability = select_native_file_tool_capability("made-up", "model");
        assert_eq!(capability.patch_shape, NativePatchWireShape::Unavailable);
        assert!(!capability.apply_patch);
    }

    #[test]
    fn apply_patch_schemas_describe_only_the_selected_input_transport() {
        let freeform = apply_patch_tool_schema(NativePatchWireShape::Freeform);
        assert_eq!(freeform["type"], serde_json::json!("string"));
        assert!(
            freeform["description"]
                .as_str()
                .expect("freeform description")
                .contains("directly as the tool input")
        );

        let json = apply_patch_tool_schema(NativePatchWireShape::JsonFunction);
        assert_eq!(json["type"], serde_json::json!("object"));
        assert_eq!(json["required"], serde_json::json!(["patch"]));
        assert_eq!(json["additionalProperties"], serde_json::json!(false));
        assert_eq!(
            json["properties"]["patch"]["type"],
            serde_json::json!("string")
        );

        let rendered = format!("{freeform}{json}").to_ascii_lowercase();
        assert!(!rendered.contains("codex"));
        assert!(!rendered.contains("proposal"));
    }
}
