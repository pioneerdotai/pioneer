use gpui_kit::ElementId;
use pioneer_client::ids::ClientIdentity;

#[derive(Clone, Copy)]
pub struct ClientIdentityRef<'a>(&'a ClientIdentity);

impl<'a> ClientIdentityRef<'a> {
    pub const fn new(identity: &'a ClientIdentity) -> Self {
        Self(identity)
    }

    pub const fn get(self) -> &'a ClientIdentity {
        self.0
    }
}

impl From<ClientIdentityRef<'_>> for ElementId {
    fn from(identity: ClientIdentityRef<'_>) -> Self {
        ElementId::from(identity.0.stable_key())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_client::ids::{
        ClientControlRole, ClientDomainIdentity, ClientFeature, ClientIdentityNamespace,
    };

    #[test]
    fn every_shared_domain_identity_converts_to_an_element_id() {
        let identities = [
            ClientDomainIdentity::ThreadId("thread".to_owned()),
            ClientDomainIdentity::RowId("row".to_owned()),
            ClientDomainIdentity::WorkItemId("work".to_owned()),
            ClientDomainIdentity::WorkspaceId("workspace".to_owned()),
            ClientDomainIdentity::ProviderId("provider".to_owned()),
            ClientDomainIdentity::RuntimeId("runtime".to_owned()),
            ClientDomainIdentity::ModelId("model".to_owned()),
            ClientDomainIdentity::ServerId("server".to_owned()),
            ClientDomainIdentity::SkillId("skill".to_owned()),
            ClientDomainIdentity::PrincipalId("principal".to_owned()),
            ClientDomainIdentity::RequestId("request".to_owned()),
            ClientDomainIdentity::SessionId("session".to_owned()),
            ClientDomainIdentity::InvitationId("invitation".to_owned()),
            ClientDomainIdentity::ArtifactVersionId("artifact".to_owned()),
            ClientDomainIdentity::ActivityId("activity".to_owned()),
            ClientDomainIdentity::NotificationId("notification".to_owned()),
            ClientDomainIdentity::GeneratedAttachmentId("attachment".to_owned()),
            ClientDomainIdentity::MarkdownNodeId("markdown".to_owned()),
        ];
        for domain in identities {
            let identity = ClientIdentity::new(
                ClientIdentityNamespace::for_feature(ClientFeature::Timeline),
                None,
                domain,
                ClientControlRole::Surface,
                None,
            );
            let _: ElementId = ClientIdentityRef::new(&identity).into();
        }
    }
}
