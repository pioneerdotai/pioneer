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
        // Compatibility for existing saved catalogs. Refresh replaces their
        // truncated modality list with the complete source contract.
        !self.input.is_empty()
    }

    pub fn input_capability(&self, kind: InputContentType) -> InputCapabilityState {
        if !self.input_is_known() {
            return InputCapabilityState::Unknown;
        }
        if kind != InputContentType::Text
            && self
                .metadata
                .get("sourceMetadata")
                .is_some_and(|source| source["attachment"] == false)
        {
            return InputCapabilityState::Unsupported;
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
        } else {
            InputCapabilityState::Unsupported
        }
    }
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
