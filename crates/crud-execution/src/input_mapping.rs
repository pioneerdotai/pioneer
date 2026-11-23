use anyhow::{Context, Result, ensure};
use pioneer_crud_payloads::Frame;
use pioneer_entity::turn_cli_runtime_binding;
use sea_orm::Set;

const MAX_MAPPING_BYTES: usize = (1 << 30) - 1;

#[derive(Clone)]
pub struct InputMapping {
    frame: Frame,
    has_inline_history: bool,
}

impl std::fmt::Debug for InputMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InputMapping")
            .field("original_bytes", &self.frame.original_bytes)
            .finish_non_exhaustive()
    }
}

impl InputMapping {
    pub fn prepare(json: &str, frozen: bool) -> Result<Self> {
        ensure!(
            json.len() <= MAX_MAPPING_BYTES,
            "CLI input mapping exceeds its byte limit"
        );
        let mapping: serde_json::Value =
            serde_json::from_str(json).context("decode CLI mapping")?;
        ensure!(mapping.is_object(), "CLI mapping must be a JSON object");
        let has_inline_history = !frozen
            && mapping
                .get("pioneerContextBasis")
                .and_then(|basis| basis.get("historyJson"))
                .is_some_and(|value| !value.is_null() && value.as_str() != Some("[]"));
        Ok(Self {
            frame: Frame::encode(json)?,
            has_inline_history,
        })
    }

    pub fn has_inline_history(&self) -> bool {
        self.has_inline_history
    }

    pub fn set(self, model: &mut turn_cli_runtime_binding::ActiveModel) -> Result<()> {
        model.input_mapping_json = Set(self.frame.bytes);
        model.input_mapping_codec = Set(self.frame.encoding as i16);
        model.input_mapping_bytes = Set(self.frame.original_bytes.try_into()?);
        model.input_mapping_chars = Set(self.frame.original_chars.try_into()?);
        model.input_mapping_sha256 = Set(self.frame.sha256.to_vec());
        model.input_mapping_has_inline_history = Set(self.has_inline_history);
        Ok(())
    }

    pub fn take(model: &mut turn_cli_runtime_binding::Model) -> Result<Self> {
        Ok(Self {
            frame: Frame {
                bytes: std::mem::take(&mut model.input_mapping_json),
                encoding: model.input_mapping_codec.try_into()?,
                original_bytes: model.input_mapping_bytes.try_into()?,
                original_chars: model.input_mapping_chars.try_into()?,
                sha256: model.input_mapping_sha256.as_slice().try_into()?,
            },
            has_inline_history: model.input_mapping_has_inline_history,
        })
    }

    pub fn decode(self) -> Result<String> {
        self.frame
            .decode(MAX_MAPPING_BYTES)
            .context("decode stored CLI input mapping")
    }
}
