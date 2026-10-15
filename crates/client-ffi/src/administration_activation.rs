use crate::ClientFfiError;

#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdministrationActivationRequest {
    pub schema_version: u32,
    pub generation: u64,
}
impl AdministrationActivationRequest {
    pub(crate) fn validate(&self) -> Result<(), ClientFfiError> {
        if self.schema_version != 1 || self.generation == 0 {
            return Err(ClientFfiError::new(
                "unsupported administration activation request",
                "administration_activation_invalid_request",
            ));
        }
        Ok(())
    }
}
