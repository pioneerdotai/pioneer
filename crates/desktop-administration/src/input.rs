use pioneer_client::{
    administration::{AdministrationPendingAction, operations::*, pages::*, types::*},
    core::ClientCore,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Default)]
pub(crate) struct AdministrationInput {
    pub(crate) members: Option<Arc<AdministrationPagePublication>>,
    pub(crate) invitations: Option<Arc<AdministrationPagePublication>>,
    pub(crate) workspaces: BTreeMap<WorkspaceId, Arc<AdministrationPagePublication>>,
    pub(crate) operation: Option<Arc<AdministrationOperationPublication>>,
    pending: AdministrationPendingAction,
}
impl AdministrationInput {
    pub(crate) fn read(client: &ClientCore) -> Self {
        let operation = client.administration_operation_snapshot();
        let pending = operation
            .as_ref()
            .filter(|op| op.request == AdministrationLoadState::Loading)
            .and_then(|op| op.action.clone())
            .map(|action| AdministrationPendingAction::Pending { action })
            .unwrap_or_default();
        Self {
            members: client.administration_page_snapshot(&AdministrationPage::Members),
            invitations: client.administration_page_snapshot(&AdministrationPage::Invitations),
            workspaces: client
                .administration_workspace_pages()
                .into_iter()
                .filter_map(|page| match &page.page {
                    AdministrationPage::WorkspaceMembers { workspace_id } => {
                        Some((workspace_id.clone(), page.clone()))
                    }
                    _ => None,
                })
                .collect(),
            operation,
            pending,
        }
    }
    pub(crate) fn members(&self) -> impl Iterator<Item = &MemberSummary> {
        self.members
            .iter()
            .flat_map(|page| page.members.iter().map(|row| &row.member))
    }
    pub(crate) fn invitations(&self) -> impl Iterator<Item = &InvitationSummary> {
        self.invitations
            .iter()
            .flat_map(|page| page.invitations.iter().map(|row| &row.invitation))
    }
    pub(crate) fn member_next_cursor(&self) -> Option<&str> {
        self.members.as_ref()?.next_cursor.as_deref()
    }
    pub(crate) fn invitation_next_cursor(&self) -> Option<&str> {
        self.invitations.as_ref()?.next_cursor.as_deref()
    }
    pub(crate) fn workspace_members(&self, id: &WorkspaceId) -> Option<Vec<&MemberSummary>> {
        let page = self.workspaces.get(id)?;
        (page.request == AdministrationLoadState::Ready && page.next_cursor.is_none())
            .then(|| page.members.iter().map(|row| &row.member).collect())
    }
    pub(crate) fn pending_action(&self) -> &AdministrationPendingAction {
        &self.pending
    }
}
