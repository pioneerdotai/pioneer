//! Stable, namespaced identities shared by Client shell adapters.

use serde::{Deserialize, Serialize};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientFeature {
    Session,
    Navigation,
    WorkspaceTree,
    Task,
    Thread,
    Timeline,
    Composer,
    PendingRequest,
    Artifact,
    Avatar,
    Provider,
    Administration,
    Mcp,
    Skills,
    Settings,
    OnboardingInvitation,
    AgentsDocument,
    DesktopUpdate,
}

impl ClientFeature {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Navigation => "navigation",
            Self::WorkspaceTree => "workspace_tree",
            Self::Task => "task",
            Self::Thread => "thread",
            Self::Timeline => "timeline",
            Self::Composer => "composer",
            Self::PendingRequest => "pending_request",
            Self::Artifact => "artifact",
            Self::Avatar => "avatar",
            Self::Provider => "provider",
            Self::Administration => "administration",
            Self::Mcp => "mcp",
            Self::Skills => "skills",
            Self::Settings => "settings",
            Self::OnboardingInvitation => "onboarding_invitation",
            Self::AgentsDocument => "agents_document",
            Self::DesktopUpdate => "desktop_update",
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ClientIdentityNamespace {
    feature: ClientFeature,
    list: Option<String>,
}

impl ClientIdentityNamespace {
    pub const fn for_feature(feature: ClientFeature) -> Self {
        Self {
            feature,
            list: None,
        }
    }

    pub fn for_list(feature: ClientFeature, list: impl Into<String>) -> Option<Self> {
        let list = list.into();
        (!list.is_empty()).then_some(Self {
            feature,
            list: Some(list),
        })
    }

    pub const fn feature(&self) -> ClientFeature {
        self.feature
    }

    pub fn list(&self) -> Option<&str> {
        self.list.as_deref()
    }

    fn stable_key(&self) -> String {
        let list = self.list.as_ref().map_or_else(
            || "none".to_owned(),
            |list| format!("some:{}:{list}", list.len()),
        );
        format!("{}/{list}", self.feature.as_str())
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ClientDomainIdentity {
    ThreadId(String),
    RowId(String),
    WorkItemId(String),
    WorkspaceId(String),
    ProviderId(String),
    RuntimeId(String),
    ModelId(String),
    ServerId(String),
    SkillId(String),
    PrincipalId(String),
    RequestId(String),
    SessionId(String),
    InvitationId(String),
    ArtifactVersionId(String),
    ActivityId(String),
    NotificationId(String),
    GeneratedAttachmentId(String),
    MarkdownNodeId(String),
}

impl ClientDomainIdentity {
    pub fn as_str(&self) -> &str {
        match self {
            Self::ThreadId(value)
            | Self::RowId(value)
            | Self::WorkItemId(value)
            | Self::WorkspaceId(value)
            | Self::ProviderId(value)
            | Self::RuntimeId(value)
            | Self::ModelId(value)
            | Self::ServerId(value)
            | Self::SkillId(value)
            | Self::PrincipalId(value)
            | Self::RequestId(value)
            | Self::SessionId(value)
            | Self::InvitationId(value)
            | Self::ArtifactVersionId(value)
            | Self::ActivityId(value)
            | Self::NotificationId(value)
            | Self::GeneratedAttachmentId(value)
            | Self::MarkdownNodeId(value) => value.as_str(),
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::ThreadId(_) => "thread",
            Self::RowId(_) => "row",
            Self::WorkItemId(_) => "work_item",
            Self::WorkspaceId(_) => "workspace",
            Self::ProviderId(_) => "provider",
            Self::RuntimeId(_) => "runtime",
            Self::ModelId(_) => "model",
            Self::ServerId(_) => "server",
            Self::SkillId(_) => "skill",
            Self::PrincipalId(_) => "principal",
            Self::RequestId(_) => "request",
            Self::SessionId(_) => "session",
            Self::InvitationId(_) => "invitation",
            Self::ArtifactVersionId(_) => "artifact_version",
            Self::ActivityId(_) => "activity",
            Self::NotificationId(_) => "notification",
            Self::GeneratedAttachmentId(_) => "generated_attachment",
            Self::MarkdownNodeId(_) => "markdown_node",
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientControlRole {
    Surface,
    Row,
    Button,
    Input,
    MenuItem,
    Tab,
    Dialog,
    Activity,
    Attachment,
    MarkdownNode,
}

impl ClientControlRole {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Surface => "surface",
            Self::Row => "row",
            Self::Button => "button",
            Self::Input => "input",
            Self::MenuItem => "menu_item",
            Self::Tab => "tab",
            Self::Dialog => "dialog",
            Self::Activity => "activity",
            Self::Attachment => "attachment",
            Self::MarkdownNode => "markdown_node",
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ClientIdentity {
    namespace: ClientIdentityNamespace,
    parent: Option<ClientDomainIdentity>,
    domain: ClientDomainIdentity,
    role: ClientControlRole,
    occurrence: Option<u32>,
}

impl ClientIdentity {
    pub fn new(
        namespace: ClientIdentityNamespace,
        parent: Option<ClientDomainIdentity>,
        domain: ClientDomainIdentity,
        role: ClientControlRole,
        occurrence: Option<u32>,
    ) -> Self {
        Self {
            namespace,
            parent,
            domain,
            role,
            occurrence,
        }
    }

    pub const fn namespace(&self) -> &ClientIdentityNamespace {
        &self.namespace
    }

    pub const fn parent(&self) -> Option<&ClientDomainIdentity> {
        self.parent.as_ref()
    }

    pub const fn domain(&self) -> &ClientDomainIdentity {
        &self.domain
    }

    pub const fn role(&self) -> ClientControlRole {
        self.role
    }

    pub const fn occurrence(&self) -> Option<u32> {
        self.occurrence
    }

    pub fn stable_key(&self) -> String {
        fn encoded(identity: &ClientDomainIdentity) -> String {
            let value = identity.as_str();
            format!("{}:{}:{value}", identity.kind_name(), value.len())
        }

        let parent = self.parent.as_ref().map_or_else(
            || "none".to_owned(),
            |identity| format!("some:{}", encoded(identity)),
        );
        let occurrence = self
            .occurrence
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_owned());
        format!(
            "client_identity/{}/{parent}/{}/{}/{occurrence}",
            self.namespace.stable_key(),
            encoded(&self.domain),
            self.role.as_str(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_key_includes_namespace_parent_role_and_occurrence() {
        let base = ClientIdentity::new(
            ClientIdentityNamespace::for_list(ClientFeature::Timeline, "primary")
                .expect("non-empty list identity"),
            Some(ClientDomainIdentity::ThreadId("thread-a".to_owned())),
            ClientDomainIdentity::RowId("row-a".to_owned()),
            ClientControlRole::Row,
            None,
        );
        let repeated = ClientIdentity::new(
            ClientIdentityNamespace::for_list(ClientFeature::Timeline, "primary")
                .expect("non-empty list identity"),
            Some(ClientDomainIdentity::ThreadId("thread-a".to_owned())),
            ClientDomainIdentity::RowId("row-a".to_owned()),
            ClientControlRole::Row,
            Some(1),
        );
        assert_ne!(base.stable_key(), repeated.stable_key());
        assert_eq!(base.stable_key(), base.clone().stable_key());
        assert_eq!(
            base.stable_key(),
            "client_identity/timeline/some:7:primary/some:thread:8:thread-a/row:5:row-a/row/none"
        );
    }

    #[test]
    fn identity_key_length_prefixes_untrusted_domain_values() {
        let first = ClientIdentity::new(
            ClientIdentityNamespace::for_feature(ClientFeature::Timeline),
            Some(ClientDomainIdentity::ThreadId("a/row:1:b".to_owned())),
            ClientDomainIdentity::RowId("c".to_owned()),
            ClientControlRole::Row,
            None,
        );
        let second = ClientIdentity::new(
            ClientIdentityNamespace::for_feature(ClientFeature::Timeline),
            Some(ClientDomainIdentity::ThreadId("a".to_owned())),
            ClientDomainIdentity::RowId("1:b/row:1:c".to_owned()),
            ClientControlRole::Row,
            None,
        );
        assert_ne!(first.stable_key(), second.stable_key());
    }

    #[test]
    fn list_namespace_is_explicit_and_non_empty() {
        assert!(ClientIdentityNamespace::for_list(ClientFeature::Timeline, "").is_none());
        let namespace =
            ClientIdentityNamespace::for_list(ClientFeature::Timeline, "search-results")
                .expect("non-empty list identity");
        assert_eq!(namespace.feature(), ClientFeature::Timeline);
        assert_eq!(namespace.list(), Some("search-results"));
        assert_ne!(
            namespace,
            ClientIdentityNamespace::for_feature(ClientFeature::Timeline)
        );
    }
}
