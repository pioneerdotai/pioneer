//! Owned Azure connection inputs accepted before native execution. Credentials
//! and the mutable authorization ceiling remain outside this value.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureResolvedConnection {
    pub schema_version: u32,
    pub resource_name: String,
    pub configured_base_url: Option<String>,
    pub deployment_name: String,
    pub api_version: String,
}
impl std::fmt::Debug for AzureResolvedConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureResolvedConnection")
            .field("schema_version", &self.schema_version)
            .finish_non_exhaustive()
    }
}
impl AzureResolvedConnection {
    pub fn from_values(
        resource: String,
        deployment: String,
        version: String,
        base_url: Option<String>,
    ) -> Result<Self> {
        let connection = Self {
            schema_version: 1,
            resource_name: resource,
            configured_base_url: base_url.map(|mut value| {
                let len = value.trim_end_matches('/').len();
                value.truncate(len);
                value
            }),
            deployment_name: deployment,
            api_version: version,
        };
        connection.validate(true)?;
        Ok(connection)
    }
    pub(crate) fn from_environment(base_url: Option<&str>) -> Result<Self> {
        Self::from_values(
            std::env::var("AZURE_OPENAI_RESOURCE").unwrap_or_default(),
            std::env::var("AZURE_OPENAI_DEPLOYMENT").unwrap_or_default(),
            std::env::var("AZURE_OPENAI_API_VERSION")
                .unwrap_or_else(|_| crate::providers::AZURE_DEFAULT_API_VERSION.into()),
            base_url.map(str::to_owned),
        )
    }
    pub fn validate(&self, requires_deployment: bool) -> Result<()> {
        Self::validate_parts(
            self.schema_version,
            &self.resource_name,
            self.configured_base_url.as_deref(),
            &self.deployment_name,
            &self.api_version,
            requires_deployment,
        )
    }
    pub(crate) fn validate_parts(
        schema_version: u32,
        resource_name: &str,
        configured_base_url: Option<&str>,
        deployment_name: &str,
        api_version: &str,
        requires_deployment: bool,
    ) -> Result<()> {
        ensure!(
            schema_version == 1,
            "unsupported accepted Azure connection schema"
        );
        let total = resource_name
            .len()
            .checked_add(deployment_name.len())
            .and_then(|n| n.checked_add(api_version.len()))
            .and_then(|n| n.checked_add(configured_base_url.unwrap_or("").len()));
        ensure!(
            total.is_some_and(|n| n <= pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES),
            "accepted Azure connection exceeds execution byte limit"
        );
        if let Some(base) = configured_base_url {
            crate::definition::validate_provider_base_url("azure-openai", base)
                .map_err(|_| anyhow::anyhow!("accepted Azure gateway root is invalid"))?;
        }
        if configured_base_url.is_none()
            && (resource_name.is_empty()
                || !resource_name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-'))
        {
            anyhow::bail!(
                "Azure OpenAI requires AZURE_OPENAI_RESOURCE or an explicit resource/gateway root base URL"
            );
        }
        if requires_deployment && deployment_name.trim().is_empty() {
            anyhow::bail!(
                "Azure OpenAI requires AZURE_OPENAI_DEPLOYMENT (deployment name, not base model ID)"
            );
        }
        if api_version == "v1" {
            return Ok(());
        }
        ensure!(
            !configured_base_url.is_some_and(|base| base.ends_with("/openai/v1")),
            "Azure date-version Chat requires a resource/gateway root, not an openai/v1 prefix"
        );
        let version = api_version.strip_suffix("-preview").unwrap_or(api_version);
        let bytes = version.as_bytes();
        ensure!(
            bytes.len() == 10
                && bytes.iter().enumerate().all(|(i, c)| if i == 4 || i == 7 {
                    *c == b'-'
                } else {
                    c.is_ascii_digit()
                }),
            "Azure Chat requires v1 or a date API version (YYYY-MM-DD or YYYY-MM-DD-preview); Responses needs a separate transport"
        );
        Ok(())
    }
    pub(crate) fn hash_into(&self, digest: &mut Sha256) {
        digest.update(b"\0pioneer-azure-connection-v1");
        for value in [
            self.resource_name.as_str(),
            self.configured_base_url.as_deref().unwrap_or(""),
            &self.deployment_name,
            &self.api_version,
        ] {
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_azure_validation_preserves_existing_chat_contract_and_safe_errors() {
        let original = AzureResolvedConnection::from_values(
            "resource".into(),
            "deployment".into(),
            "v1".into(),
            None,
        )
        .unwrap();
        let encoded = serde_json::to_string(&original).unwrap();
        let decoded: AzureResolvedConnection = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, original);
        assert!(
            AzureResolvedConnection::from_values(
                String::new(),
                "deployment".into(),
                "v1".into(),
                None
            )
            .is_err()
        );
        assert!(
            AzureResolvedConnection::from_values(
                "resource".into(),
                String::new(),
                "v1".into(),
                None
            )
            .is_err()
        );
        assert!(
            AzureResolvedConnection::from_values(
                "resource".into(),
                "deployment".into(),
                "SECRET".into(),
                None
            )
            .unwrap_err()
            .to_string()
            .contains("API version")
        );
        assert!(
            !format!(
                "{:?}",
                AzureResolvedConnection {
                    deployment_name: "SECRET".into(),
                    ..original.clone()
                }
            )
            .contains("SECRET")
        );
        assert!(
            AzureResolvedConnection::from_values(
                String::new(),
                "deployment".into(),
                "2025-04-01-preview".into(),
                Some("https://gateway.invalid/openai/v1".into())
            )
            .is_err()
        );
        let gateway = AzureResolvedConnection::from_values(
            String::new(),
            "deployment".into(),
            "2025-04-01-preview".into(),
            Some("https://gateway.invalid/".into()),
        )
        .unwrap();
        assert_eq!(
            gateway.configured_base_url.as_deref(),
            Some("https://gateway.invalid")
        );
        let mut future = original.clone();
        future.schema_version = 2;
        assert!(future.validate(true).is_err());
        let mut huge = original;
        huge.resource_name =
            "SECRET".repeat(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES / 6 + 1);
        assert!(
            !huge
                .validate(true)
                .unwrap_err()
                .to_string()
                .contains("SECRET")
        );
    }
}
