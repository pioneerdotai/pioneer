//! Typed adapters for callers that still use the existing list response shapes.
//! Opaque cursors identify an immutable Client page and cannot cross a query or revision.
use super::pages::*;
use crate::core::ClientCore;
use pioneer_protocol::*;
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PageCursor {
    version: u8,
    revision: u64,
    offset: usize,
    query: String,
}
fn slice<T: Clone>(
    rows: &[T],
    cursor: Option<&str>,
    limit: u32,
    revision: u64,
    query: String,
) -> anyhow::Result<(Vec<T>, Option<String>)> {
    let offset = if let Some(cursor) = cursor {
        let cursor: PageCursor = serde_json::from_str(cursor)
            .map_err(|_| anyhow::anyhow!("administration_cursor_stale"))?;
        anyhow::ensure!(
            cursor.version == 1
                && cursor.revision == revision
                && cursor.query == query
                && cursor.offset <= rows.len(),
            "administration_cursor_stale"
        );
        cursor.offset
    } else {
        0
    };
    let end = offset.saturating_add(limit as usize).min(rows.len());
    let next = (end < rows.len()).then(|| {
        serde_json::to_string(&PageCursor {
            version: 1,
            revision,
            offset: end,
            query,
        })
        .expect("administration cursor serialization")
    });
    Ok((rows[offset..end].to_vec(), next))
}
impl ClientCore {
    fn read_administration_complete(
        self: &Arc<Self>,
        page: AdministrationPage,
        refresh: bool,
    ) -> anyhow::Result<Arc<AdministrationPagePublication>> {
        let read = self.read_administration_page(page.clone(), refresh)?;
        loop {
            let publication = read.wait_while(|| true)?;
            if publication.next_cursor.is_none() {
                return Ok(publication);
            }
            self.administration_page_intent(AdministrationPageIntent::Next { page: page.clone() });
        }
    }
    pub fn read_administration_members(
        self: &Arc<Self>,
        params: MemberListParams,
    ) -> anyhow::Result<MemberListResponse> {
        let limit = params.validate()?;
        let page = self.read_administration_complete(
            AdministrationPage::MemberDirectory,
            params.cursor.is_none(),
        )?;
        let rows: Vec<_> = page.members.iter().map(|row| row.member.clone()).collect();
        let (members, next_cursor) = slice(
            &rows,
            params.cursor.as_deref(),
            limit,
            page.revision,
            "members".into(),
        )?;
        Ok(MemberListResponse {
            members,
            next_cursor,
        })
    }
    pub fn read_administration_invitations(
        self: &Arc<Self>,
        params: InvitationListParams,
    ) -> anyhow::Result<InvitationListResponse> {
        let limit = params.validate()?;
        let page = self.read_administration_complete(
            AdministrationPage::Invitations,
            params.cursor.is_none(),
        )?;
        let query =
            serde_json::to_string(&("invitations", &params.status, &params.creator_principal_id))?;
        let rows: Vec<_> = page
            .invitations
            .iter()
            .map(|row| &row.invitation)
            .filter(|row| {
                params
                    .status
                    .as_ref()
                    .is_none_or(|status| &row.status == status)
                    && params
                        .creator_principal_id
                        .as_ref()
                        .is_none_or(|creator| &row.inviter.principal_id == creator)
            })
            .cloned()
            .collect();
        let (invitations, next_cursor) =
            slice(&rows, params.cursor.as_deref(), limit, page.revision, query)?;
        Ok(InvitationListResponse {
            invitations,
            next_cursor,
        })
    }
    pub fn read_administration_workspace_members(
        self: &Arc<Self>,
        params: WorkspaceMemberListParams,
    ) -> anyhow::Result<WorkspaceMemberListResponse> {
        let limit = params.validate()?;
        let page = self.read_administration_complete(
            AdministrationPage::WorkspaceMembers {
                workspace_id: params.workspace_id.clone(),
            },
            params.cursor.is_none(),
        )?;
        let rows: Vec<_> = page.members.iter().map(|row| row.member.clone()).collect();
        let (members, next_cursor) = slice(
            &rows,
            params.cursor.as_deref(),
            limit,
            page.revision,
            format!("workspace:{}", params.workspace_id),
        )?;
        Ok(WorkspaceMemberListResponse {
            workspace_id: params.workspace_id,
            members,
            next_cursor,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn list_shape_adapter_preserves_limits_identity_and_rejects_stale_or_cross_query_cursors() {
        let rows = vec!["alice", "bob", "carol"];
        let (first, next) = slice(&rows, None, 2, 9, "members".into()).unwrap();
        assert_eq!(first, ["alice", "bob"]);
        let (last, end) = slice(&rows, next.as_deref(), 2, 9, "members".into()).unwrap();
        assert_eq!(last, ["carol"]);
        assert!(end.is_none());
        assert!(slice(&rows, next.as_deref(), 2, 10, "members".into()).is_err());
        assert!(slice(&rows, next.as_deref(), 2, 9, "workspace:other".into()).is_err());
        assert!(slice(&rows, Some("invalid"), 2, 9, "members".into()).is_err());
    }
}
