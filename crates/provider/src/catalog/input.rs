use super::CatalogModel;
use crate::InputContentType;

/// Unknown is deliberately different from an authoritative negative. Both
/// fail closed at media preflight, but expose different remediation to callers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputCapabilityState {
    Unknown,
    Unsupported,
    Supported,
}

impl CatalogModel {
    pub fn input_is_known(&self) -> bool {
        if let Some(origin) = self.metadata.get("inputOrigin") {
            return matches!(origin["kind"].as_str(), Some("source" | "override"));
        }
        // Legacy catalog projections are positive evidence only.
        false
    }

    pub fn input_capability(&self, kind: InputContentType) -> InputCapabilityState {
        if let Some(state) = self
            .metadata
            .get("effectiveInput")
            .and_then(|v| v.get(crate::attachments::admission::input_key(kind)))
            .and_then(|v| v.as_str())
        {
            return match state {
                "supported" => InputCapabilityState::Supported,
                "unsupported" => InputCapabilityState::Unsupported,
                _ => InputCapabilityState::Unknown,
            };
        }
        let names: &[&str] = match kind {
            InputContentType::Text => &["text"],
            InputContentType::Image => &["image"],
            InputContentType::File => &["file", "pdf", "document"],
            InputContentType::Audio => &["audio"],
            InputContentType::Video => &["video"],
        };
        if self
            .input
            .iter()
            .any(|v| names.iter().any(|name| v.eq_ignore_ascii_case(name)))
        {
            InputCapabilityState::Supported
        } else if kind == InputContentType::File
            && self
                .metadata
                .get("sourceMetadata")
                .is_some_and(|s| s["attachment"] == false)
        {
            // The schema's file-attachment flag only addresses documents; an
            // explicit modality above takes precedence over this stale flag.
            InputCapabilityState::Unsupported
        } else if self.input_is_known() {
            InputCapabilityState::Unsupported
        } else {
            InputCapabilityState::Unknown
        }
    }
}

/// Discovery evidence is scoped to the actual adapter authority, never published
/// into the global catalog. Explicit narrower input wins; known catalog negatives
/// cannot be widened by discovery. Partial vision evidence only addresses images.
pub(crate) fn effective_input_model(
    provider: &str,
    id: &str,
    base: Option<&CatalogModel>,
    discovery: Option<&pioneer_protocol::ProviderModelInfo>,
) -> Option<CatalogModel> {
    use serde_json::json;
    let mut entry = if let Some(base) = base {
        base.clone()
    } else {
        let model = discovery?;
        let api = match provider {
            "ollama" => "ollama-chat",
            "anthropic" => "anthropic-messages",
            "gemini" => "google-generative-ai",
            "bedrock" => "bedrock-converse-stream",
            _ => "openai-completions",
        };
        CatalogModel {
            id: id.into(),
            name: model.name.clone().unwrap_or_else(|| id.into()),
            provider: provider.into(),
            api: api.into(),
            base_url: String::new(),
            context_window: model.limits.context_window.unwrap_or(0),
            max_tokens: model.limits.max_output_tokens.unwrap_or(0),
            reasoning: false,
            input: vec![],
            cost: json!({}),
            metadata: Default::default(),
        }
    };
    let Some(discovery) = discovery else {
        return Some(entry);
    };
    let mut states = serde_json::Map::new();
    for kind in [
        InputContentType::Text,
        InputContentType::Image,
        InputContentType::File,
        InputContentType::Audio,
        InputContentType::Video,
    ] {
        let baseline = entry.input_capability(kind);
        let discovered = if let Some(input) = &discovery.capabilities.input_modalities {
            let names: &[&str] = match kind {
                InputContentType::File => &["pdf", "file", "document"],
                InputContentType::Text => &["text"],
                InputContentType::Image => &["image"],
                InputContentType::Audio => &["audio"],
                InputContentType::Video => &["video"],
            };
            if input
                .iter()
                .any(|s| names.iter().any(|n| s.eq_ignore_ascii_case(n)))
            {
                InputCapabilityState::Supported
            } else {
                InputCapabilityState::Unsupported
            }
        } else if kind == InputContentType::Image {
            match discovery.capabilities.vision {
                Some(true) => InputCapabilityState::Supported,
                Some(false) => InputCapabilityState::Unsupported,
                None => InputCapabilityState::Unknown,
            }
        } else {
            InputCapabilityState::Unknown
        };
        let state = match (baseline, discovered) {
            (InputCapabilityState::Unsupported, _) | (_, InputCapabilityState::Unsupported) => {
                "unsupported"
            }
            (InputCapabilityState::Supported, _) | (_, InputCapabilityState::Supported) => {
                "supported"
            }
            _ => "unknown",
        };
        states.insert(
            crate::attachments::admission::input_key(kind).into(),
            json!(state),
        );
    }
    entry
        .metadata
        .insert("effectiveInput".into(), json!(states));
    entry.metadata.insert("discoveryInputEvidence".into(),json!({"input":discovery.capabilities.input_modalities,"vision":discovery.capabilities.vision,"scope":"provider authority instance"}));
    if let Some(context) = discovery.limits.context_window.filter(|v| *v > 0) {
        entry.context_window = if entry.context_window > 0 {
            entry.context_window.min(context)
        } else {
            context
        };
    }
    Some(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ModelCatalog;
    #[test]
    fn tri_state_and_pdf_alias_survive_discovery_and_protocol_serialization() {
        let catalog = ModelCatalog::parse(
            include_str!("../../tests/fixtures/capabilities/models.json"),
            include_str!("../../tests/fixtures/capabilities/provenance.json"),
        )
        .unwrap();
        assert_eq!(
            catalog
                .model("groq", "unknown")
                .unwrap()
                .input_capability(InputContentType::Audio),
            InputCapabilityState::Unknown
        );
        assert_eq!(
            catalog
                .model("groq", "text")
                .unwrap()
                .input_capability(InputContentType::Audio),
            InputCapabilityState::Unsupported
        );
        assert_eq!(
            catalog
                .model("google", "media")
                .unwrap()
                .input_capability(InputContentType::File),
            InputCapabilityState::Supported
        );
        let mut model: pioneer_protocol::ProviderModelInfo = serde_json::from_value(
            serde_json::json!({"id":"media","provider":"google","limits":{},"capabilities":{}}),
        )
        .unwrap();
        catalog.enrich("gemini", std::slice::from_mut(&mut model));
        let wire = serde_json::to_value(&model).unwrap();
        let decoded: pioneer_protocol::ProviderModelInfo = serde_json::from_value(wire).unwrap();
        assert_eq!(
            decoded.capabilities.input_modalities,
            Some(vec![
                "text".into(),
                "image".into(),
                "audio".into(),
                "video".into(),
                "pdf".into()
            ])
        );
        assert_eq!(
            decoded.capabilities.output_modalities,
            Some(vec!["text".into()])
        );
        // Explicit discovery data remains above catalog enrichment in priority.
        model.capabilities.input_modalities = Some(vec!["text".into()]);
        catalog.enrich("gemini", std::slice::from_mut(&mut model));
        assert_eq!(
            model.capabilities.input_modalities,
            Some(vec!["text".into()])
        );
    }
}

#[cfg(test)]
mod conflict_tests {
    use super::*;
    use serde_json::{Value, json};
    #[test]
    fn pinned_modalities_precede_file_flags_and_partial_evidence_has_no_negative_av() {
        let c = crate::catalog::ModelCatalog::parse(
            include_str!("../../tests/fixtures/capabilities/models.json"),
            include_str!("../../tests/fixtures/capabilities/provenance.json"),
        )
        .unwrap();
        let cases: Vec<Value> = serde_json::from_str(include_str!(
            "../../tests/fixtures/capabilities/attachment-conflicts.json"
        ))
        .unwrap();
        for case in cases {
            let mut entry = c.model("groq", "vision").unwrap().clone();
            entry.input =
                serde_json::from_value(case["source"]["modalities"]["input"].clone()).unwrap();
            entry
                .metadata
                .insert("sourceMetadata".into(), case["source"].clone());
            assert_eq!(case["source"]["attachment"], false);
            assert_eq!(
                entry.input_capability(crate::InputContentType::Image),
                InputCapabilityState::Supported,
                "{}",
                case["id"]
            );
        }
        let mut entry = c.model("groq", "vision").unwrap().clone();
        entry.metadata.insert(
            "inputOrigin".into(),
            json!({"kind":"partial","expression":"vercel positive vision tag"}),
        );
        assert_eq!(
            entry.input_capability(crate::InputContentType::Image),
            InputCapabilityState::Supported
        );
        for kind in [
            crate::InputContentType::Audio,
            crate::InputContentType::Video,
            crate::InputContentType::File,
        ] {
            assert_eq!(entry.input_capability(kind), InputCapabilityState::Unknown);
        }
        entry.metadata.remove("inputOrigin");
        assert_eq!(
            entry.input_capability(crate::InputContentType::Audio),
            InputCapabilityState::Unknown
        );
        entry
            .metadata
            .insert("sourceMetadata".into(), json!({"attachment":false}));
        assert_eq!(
            entry.input_capability(crate::InputContentType::File),
            InputCapabilityState::Unsupported
        );
    }
}

#[cfg(test)]
mod discovery_consistency_tests {
    use serde_json::json;
    #[test]
    fn narrower_raw_vision_and_input_remain_consistent_after_enrichment() {
        let c = crate::catalog::ModelCatalog::parse(
            include_str!("../../tests/fixtures/capabilities/models.json"),
            include_str!("../../tests/fixtures/capabilities/provenance.json"),
        )
        .unwrap();
        for caps in [
            json!({"vision":false}),
            json!({"vision":true,"input_modalities":["text"]}),
        ] {
            let mut m: pioneer_protocol::ProviderModelInfo = serde_json::from_value(
                json!({"id":"vision","provider":"groq","limits":{},"capabilities":caps}),
            )
            .unwrap();
            c.enrich("groq", std::slice::from_mut(&mut m));
            assert_eq!(m.capabilities.vision, Some(false));
            assert_eq!(m.capabilities.input_modalities, Some(vec!["text".into()]));
        }
    }
}
