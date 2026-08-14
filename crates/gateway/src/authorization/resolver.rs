use anyhow::{Context, Result};
use pioneer_crud::{
    CrudStore, PersistedCapabilityScopeKind, PersistedThreadAccessClass,
    find_active_workspace_for_principal, find_thread_membership, find_workspace_membership,
    resolve_artifact_authorization_scope, resolve_persisted_capability_authorization_scope,
    resolve_runtime_draft_artifact_authorization_scope, resolve_session_authorization_scope,
    resolve_task_authorization_scope, resolve_thread_authorization_scope,
    resolve_turn_authorization_scope, resolve_workspace_authorization_scope,
};
use pioneer_protocol::{
    AuthSessionId, InvitationId, PrincipalId, PrincipalKind, PrincipalStatus, ThreadVisibility,
    WorkspaceId,
};
use sea_orm::ConnectionTrait;

use crate::auth::AuthenticatedSessionPrincipal;
use crate::thread::RuntimeDraftAccess;

use super::{
    ActionGateDecision, AgentsDocumentResourceId, ArtifactResourceId, AuthorizationDecision,
    AuthorizationResource, AuthorizationService, CapabilityKind, CapabilityResourceId, DenyReason,
    DisclosurePolicy, ResourceAction, TaskResourceId, ThreadAccessClass, ThreadAccessFacts,
    ThreadResourceClass, ThreadResourceId, TurnResourceId, WorkspaceAccessFacts,
    WorkspaceResourceId,
};
use super::{ResolvedResourceAccess, ResourceIdError};

#[derive(Clone, Debug)]
pub(crate) struct CapabilityThreadFacts {
    pub(crate) workspace_id: String,
    pub(crate) access: ThreadAccessFacts,
    pub(crate) internal_child: bool,
    pub(crate) parent_execution_actions: Option<Vec<String>>,
}

#[derive(Clone)]
pub(crate) struct AuthorizationResolver {
    store: CrudStore,
    service: AuthorizationService,
}

impl AuthorizationResolver {
    pub(crate) fn new(store: CrudStore) -> Self {
        Self {
            store,
            service: AuthorizationService::new(),
        }
    }

    /// Resolves exact server-owned workspace facts for a capability snapshot.
    /// These facts are not an authorization proof and must still be evaluated
    /// by `AuthorizationService` for every projected action.
    pub(crate) async fn capability_workspace_facts(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceAccessFacts>> {
        let Some(scope) =
            resolve_workspace_authorization_scope(&self.store.database_connection(), workspace_id)
                .await?
        else {
            return Ok(None);
        };
        self.workspace_facts(principal, scope.workspace_id.as_str(), scope.is_active)
            .await
            .map(Some)
    }

    /// Resolves exact server-owned thread facts for a capability snapshot.
    /// The caller must evaluate them through the canonical policy service.
    pub(crate) async fn capability_thread_facts(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        thread_id: &str,
        expected_workspace_id: Option<&str>,
    ) -> Result<Option<CapabilityThreadFacts>> {
        let Some(scope) = resolve_thread_authorization_scope(
            &self.store.database_connection(),
            thread_id,
            expected_workspace_id,
        )
        .await?
        else {
            return Ok(None);
        };
        let workspace_id = scope.workspace_id.clone();
        if self
            .service
            .runtime_principal_policy(principal.kind, principal.role_key.as_ref())
            == Some(super::RuntimePrincipalPolicy::ScopedCollaboration)
            && scope.access_class == PersistedThreadAccessClass::Internal
        {
            let Some(lineage) = self
                .store
                .get_task_thread_lineage(scope.thread_id.as_str())
                .await
                .context("failed to resolve capability snapshot thread lineage")?
            else {
                return Ok(None);
            };
            if lineage.child_thread_id != scope.thread_id
                || lineage.root_thread_id == scope.thread_id
            {
                return Ok(None);
            }
            let Some(root_scope) = resolve_thread_authorization_scope(
                &self.store.database_connection(),
                lineage.root_thread_id.as_str(),
                Some(workspace_id.as_str()),
            )
            .await?
            else {
                return Ok(None);
            };
            let Some(parent_turn_id) = lineage.created_by_turn_id.as_deref() else {
                return Ok(None);
            };
            if self
                .store
                .get_turn_execution_authorization_context(parent_turn_id)
                .await?
                .is_none()
            {
                return Ok(None);
            }
            let parent_execution_actions = Some(
                super::ExecutionAuthorizationContext::load_for_turn(&self.store, parent_turn_id)
                    .await?
                    .granted_action_names()
                    .to_vec(),
            );
            return self
                .thread_facts(principal, &root_scope)
                .await
                .map(|access| {
                    Some(CapabilityThreadFacts {
                        workspace_id,
                        access,
                        internal_child: true,
                        parent_execution_actions,
                    })
                });
        }
        self.thread_facts(principal, &scope).await.map(|access| {
            Some(CapabilityThreadFacts {
                workspace_id,
                access,
                internal_child: false,
                parent_execution_actions: None,
            })
        })
    }

    pub(crate) async fn authorize_agents_document_for_thread(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<ProofResolution<AuthorizedAgentsDocument>> {
        let Some(thread) = resolve_thread_authorization_scope(
            &self.store.database_connection(),
            thread_id,
            Some(workspace_id),
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        if thread.access_class == PersistedThreadAccessClass::Internal {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let folder_id = self
            .store
            .resolve_thread_agents_doc_folder_for_thread(workspace_id, thread_id)
            .await?;
        self.authorize_agents_document(
            principal,
            action_gate,
            action,
            workspace_id,
            folder_id.as_deref(),
            None,
        )
        .await
    }

    pub(crate) fn authorize_workspace_collection(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
    ) -> ProofResolution<AuthorizedWorkspaceCollection> {
        self.finish(
            principal,
            action_gate,
            action,
            AuthorizationResource::WorkspaceCollection(principal.gateway_id.clone()),
            ResolvedResourceAccess::WorkspaceCollection,
        )
        .map(AuthorizedWorkspaceCollection)
    }

    pub(crate) async fn authorize_invitation_grants<C: ConnectionTrait>(
        &self,
        db: &C,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        workspace_ids: &[WorkspaceId],
    ) -> Result<ProofResolution<AuthorizedInvitationGrants>> {
        let action = ResourceAction::InvitationCreate;
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        if !persisted_actor_is_current(db, principal).await? {
            return Ok(ProofResolution::Denied(inactive_principal()));
        }
        let mut canonical_workspace_ids = workspace_ids.to_vec();
        canonical_workspace_ids.sort();
        canonical_workspace_ids.dedup();
        let mut resources = Vec::with_capacity(canonical_workspace_ids.len());
        for workspace_id in &canonical_workspace_ids {
            let authorized = match action_gate {
                ActionGateDecision::AllowAbsolute => {
                    pioneer_crud::resolve_workspace_authorization_scope(db, workspace_id.as_str())
                        .await?
                        .is_some_and(|scope| scope.is_active)
                }
                ActionGateDecision::RequireResource { .. } => {
                    pioneer_crud::find_active_workspace_for_principal(
                        db,
                        &principal.principal_id,
                        workspace_id.as_str(),
                    )
                    .await?
                    .is_some()
                }
                ActionGateDecision::Deny { .. } => false,
            };
            if !authorized {
                return Ok(ProofResolution::Denied(missing_resource()));
            }
            let Some(resource_id) = resource_id(WorkspaceResourceId::new(workspace_id.to_string()))
            else {
                return Ok(ProofResolution::Denied(missing_resource()));
            };
            resources.push(resource_id);
        }
        if resources.is_empty() {
            return Ok(ProofResolution::Denied(AuthorizationDecision::Deny {
                reason: DenyReason::ResourceScopeMismatch,
                disclosure: DisclosurePolicy::Validation,
            }));
        }
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::InvitationGrantSet(resources),
                ResolvedResourceAccess::InvitationGrantSet {
                    all_active_and_authorized: true,
                },
            )
            .map(AuthorizedInvitationGrants))
    }

    pub(crate) fn authorize_invitation_collection(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
    ) -> ProofResolution<AuthorizedInvitationCollection> {
        self.finish(
            principal,
            action_gate,
            ResourceAction::InvitationList,
            AuthorizationResource::InvitationCollection(principal.gateway_id.clone()),
            ResolvedResourceAccess::InvitationCollection,
        )
        .map(AuthorizedInvitationCollection)
    }

    pub(crate) fn authorize_member_directory(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
    ) -> ProofResolution<AuthorizedMemberDirectory> {
        self.finish(
            principal,
            action_gate,
            ResourceAction::MemberDirectoryList,
            AuthorizationResource::MemberDirectory(principal.gateway_id.clone()),
            ResolvedResourceAccess::MemberDirectory,
        )
        .map(AuthorizedMemberDirectory)
    }

    pub(crate) async fn authorize_member_avatar<C: ConnectionTrait>(
        &self,
        db: &C,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        target_principal_id: &PrincipalId,
    ) -> Result<ProofResolution<AuthorizedMemberAvatar>> {
        let action = ResourceAction::MemberAvatarRead;
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(target) = pioneer_crud::load_principal_by_id(db, target_principal_id).await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        if target.gateway_id != principal.gateway_id {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let visible = match action_gate {
            ActionGateDecision::AllowAbsolute => true,
            ActionGateDecision::RequireResource { .. } => {
                target.id == principal.principal_id
                    || (target.kind == PrincipalKind::Superuser
                        && target.status == pioneer_protocol::PrincipalStatus::Active)
                    || pioneer_crud::find_shared_workspace_principal_for_principal(
                        db,
                        &principal.gateway_id,
                        &principal.principal_id,
                        target_principal_id,
                    )
                    .await?
                    .is_some()
            }
            ActionGateDecision::Deny { .. } => false,
        };
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::DirectoryPrincipal(target_principal_id.clone()),
                ResolvedResourceAccess::DirectoryPrincipal { visible },
            )
            .map(AuthorizedMemberAvatar))
    }

    pub(crate) async fn authorize_member_principal<C: ConnectionTrait>(
        &self,
        db: &C,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        target_principal_id: &PrincipalId,
    ) -> Result<ProofResolution<AuthorizedMemberPrincipal>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(target) = pioneer_crud::load_principal_by_id(db, target_principal_id).await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        if target.gateway_id != principal.gateway_id {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        // Resolution proves only that the requested administrative resource
        // belongs to this Gateway. Transaction-local lifecycle eligibility
        // (ordinary Member role and current status) is deliberately owned by
        // MemberService/AuthService so existing but invalid targets receive
        // the stable `invalid_target` management error instead of being
        // collapsed into an anti-IDOR miss before the transaction.
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::MemberPrincipal(target_principal_id.clone()),
                ResolvedResourceAccess::MemberPrincipal,
            )
            .map(AuthorizedMemberPrincipal))
    }

    pub(crate) async fn authorize_invitation<C: ConnectionTrait>(
        &self,
        db: &C,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        invitation_id: &InvitationId,
    ) -> Result<ProofResolution<AuthorizedInvitation>> {
        let action = ResourceAction::InvitationRevoke;
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        if !persisted_actor_is_current(db, principal).await? {
            return Ok(ProofResolution::Denied(inactive_principal()));
        }
        let Some(invitation) = pioneer_crud::load_invitation(db, invitation_id).await? else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        if invitation.gateway_id != principal.gateway_id.as_str() {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let actor_created = invitation.created_by_principal_id == principal.principal_id.as_str();
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::Invitation(invitation_id.clone()),
                ResolvedResourceAccess::Invitation { actor_created },
            )
            .map(AuthorizedInvitation))
    }

    pub(crate) async fn authorize_workspace(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        workspace_id: &str,
    ) -> Result<ProofResolution<AuthorizedWorkspace>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let database = self.store.database_connection();
        let scope = match action_gate {
            ActionGateDecision::AllowAbsolute => {
                resolve_workspace_authorization_scope(&database, workspace_id).await?
            }
            ActionGateDecision::RequireResource { .. } => find_active_workspace_for_principal(
                &database,
                &principal.principal_id,
                workspace_id,
            )
            .await?
            .map(|workspace| pioneer_crud::WorkspaceAuthorizationScope {
                workspace_id: workspace.id,
                is_active: workspace.is_active,
            }),
            ActionGateDecision::Deny { .. } => None,
        };
        let Some(scope) = scope else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(resource_id) = resource_id(WorkspaceResourceId::new(scope.workspace_id.clone()))
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let workspace = WorkspaceAccessFacts {
            workspace_active: scope.is_active,
            workspace_member: matches!(action_gate, ActionGateDecision::RequireResource { .. }),
        };
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::Workspace(resource_id),
                ResolvedResourceAccess::Workspace(workspace),
            )
            .map(AuthorizedWorkspace))
    }

    pub(crate) async fn authorize_thread(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        thread_id: &str,
        expected_workspace_id: Option<&str>,
    ) -> Result<ProofResolution<AuthorizedThread>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(scope) = resolve_thread_authorization_scope(
            &self.store.database_connection(),
            thread_id,
            expected_workspace_id,
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(resource) = thread_resource(scope.workspace_id.as_str(), scope.thread_id.as_str())
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let collaboration_root_thread_id = if scope.access_class
            == PersistedThreadAccessClass::Internal
        {
            let Some(lineage) = self
                .store
                .get_task_thread_lineage(scope.thread_id.as_str())
                .await
                .context("failed to resolve internal thread authorization lineage")?
            else {
                return Ok(ProofResolution::Denied(missing_resource()));
            };
            if lineage.child_thread_id != scope.thread_id
                || lineage.root_thread_id == scope.thread_id
            {
                return Ok(ProofResolution::Denied(missing_resource()));
            }
            let Some(root_id) = resource_id(ThreadResourceId::new(lineage.root_thread_id)) else {
                return Ok(ProofResolution::Denied(missing_resource()));
            };
            Some(root_id)
        } else {
            None
        };
        let access = self.thread_facts(principal, &scope).await?;
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                resource,
                ResolvedResourceAccess::Thread(access),
            )
            .map(|mut proof| {
                proof.collaboration_root_thread_id = collaboration_root_thread_id;
                AuthorizedThread(proof)
            }))
    }

    /// Authorizes an exact connection-owned draft without pretending that it
    /// already exists in persistence. `RuntimeDraftAccess` is the server-owned
    /// resource proof; persisted workspace state is still resolved here so a
    /// revoked Member cannot keep using a draft through an old connection.
    pub(crate) async fn authorize_runtime_draft(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        access: &RuntimeDraftAccess,
    ) -> Result<ProofResolution<AuthorizedThread>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        if access.owner().identity.principal_id != principal.principal_id
            || access.owner().identity.session_id != principal.session_id
        {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let Some(workspace_scope) = resolve_workspace_authorization_scope(
            &self.store.database_connection(),
            access.workspace_id(),
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let workspace = self
            .workspace_facts(
                principal,
                workspace_scope.workspace_id.as_str(),
                workspace_scope.is_active,
            )
            .await?;
        let Some(workspace_id) =
            resource_id(WorkspaceResourceId::new(access.workspace_id().to_owned()))
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(thread_id) = resource_id(ThreadResourceId::new(access.thread_id().to_owned()))
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let access_class = if access.visibility() == Some(ThreadVisibility::Workspace) {
            ThreadAccessClass::Workspace
        } else {
            ThreadAccessClass::Private
        };

        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::Thread {
                    workspace_id,
                    thread_id,
                },
                ResolvedResourceAccess::Thread(ThreadAccessFacts {
                    workspace,
                    access_class,
                    resource_class: ThreadResourceClass::Root,
                    thread_member: self
                        .service
                        .runtime_principal_policy(principal.kind, principal.role_key.as_ref())
                        == Some(super::RuntimePrincipalPolicy::ScopedCollaboration),
                    thread_creator: true,
                }),
            )
            .map(AuthorizedThread))
    }

    /// Authorizes a completed composer upload through the exact connection-
    /// owned runtime draft that accepted it. The artifact must still be an
    /// exclusive `draft_upload` created by the same principal.
    pub(crate) async fn authorize_runtime_draft_artifact(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        artifact_id: &str,
        access: &RuntimeDraftAccess,
    ) -> Result<ProofResolution<AuthorizedArtifact>> {
        let thread = self
            .authorize_runtime_draft(principal, action_gate, action, access)
            .await?;
        let ProofResolution::Authorized(thread) = thread else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(scope) = resolve_runtime_draft_artifact_authorization_scope(
            &self.store.database_connection(),
            artifact_id,
            access.workspace_id(),
            access.thread_id(),
            &principal.principal_id,
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(workspace_id) = resource_id(WorkspaceResourceId::new(scope.workspace_id)) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(thread_id) = resource_id(ThreadResourceId::new(scope.thread_id)) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(artifact_id) = resource_id(ArtifactResourceId::new(scope.artifact_id)) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let AuthorizationProofCore {
            principal_id,
            action,
            decision,
            collaboration_root_thread_id,
            thread_access,
            ..
        } = thread.0;
        Ok(ProofResolution::Authorized(AuthorizedArtifact(
            AuthorizationProofCore {
                principal_id,
                action,
                resource: AuthorizationResource::Artifact {
                    workspace_id,
                    thread_id: Some(thread_id),
                    artifact_id,
                },
                decision,
                collaboration_root_thread_id,
                thread_access,
            },
        )))
    }

    /// Resolves an internal child only through its persisted root lineage and
    /// the caller's current authorization for that root. Direct thread
    /// resolution deliberately continues to deny internal rows to Members.
    pub(crate) async fn authorize_internal_thread_via_root(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        child_thread_id: &str,
        expected_workspace_id: Option<&str>,
    ) -> Result<ProofResolution<AuthorizedThread>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(child_scope) = resolve_thread_authorization_scope(
            &self.store.database_connection(),
            child_thread_id,
            expected_workspace_id,
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        if child_scope.access_class != PersistedThreadAccessClass::Internal {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let Some(lineage) = self
            .store
            .get_task_thread_lineage(child_thread_id)
            .await
            .context("failed to resolve internal thread lineage")?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        if lineage.child_thread_id != child_thread_id || lineage.root_thread_id == child_thread_id {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let workspace_id = child_scope.workspace_id.clone();
        let root = self
            .authorize_thread(
                principal,
                action_gate,
                action,
                lineage.root_thread_id.as_str(),
                Some(workspace_id.as_str()),
            )
            .await?;
        let ProofResolution::Authorized(root) = root else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(child_action) = super::execution_child_policy_action(action) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let child_gate = self.service.authorize_action(
            principal.kind,
            principal.role_key.as_ref(),
            child_action,
        );
        if !child_gate.permits_resource_resolution() && !child_gate.is_final_allow() {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let Some(parent_turn_id) = lineage.created_by_turn_id.as_deref() else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        if self
            .store
            .get_turn_execution_authorization_context(parent_turn_id)
            .await?
            .is_none()
        {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let context =
            super::ExecutionAuthorizationContext::load_for_turn(&self.store, parent_turn_id)
                .await?;
        if context.workspace_id() != workspace_id
            || context.root_thread_id() != lineage.root_thread_id
            || !context.grants_action(action)
        {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let Some(resource) = thread_resource(workspace_id.as_str(), child_scope.thread_id.as_str())
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        Ok(ProofResolution::Authorized(AuthorizedThread(
            AuthorizationProofCore {
                principal_id: principal.principal_id.clone(),
                action,
                resource,
                decision: root.decision().clone(),
                collaboration_root_thread_id: resource_id(ThreadResourceId::new(
                    lineage.root_thread_id.clone(),
                )),
                thread_access: root.0.thread_access.map(|mut facts| {
                    facts.resource_class = ThreadResourceClass::InternalChild;
                    facts
                }),
            },
        )))
    }

    /// Resolves a turn in an internal child through the same persisted root
    /// lineage used for the child thread itself. The proof keeps the exact
    /// child and turn identity while inheriting only the root's current ACL.
    pub(crate) async fn authorize_internal_turn_via_root(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        turn_id: &str,
        expected_workspace_id: Option<&str>,
        expected_thread_id: Option<&str>,
    ) -> Result<ProofResolution<AuthorizedTurn>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(scope) = resolve_turn_authorization_scope(
            &self.store.database_connection(),
            turn_id,
            expected_workspace_id,
            expected_thread_id,
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        if scope.thread.access_class != PersistedThreadAccessClass::Internal {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let child = self
            .authorize_internal_thread_via_root(
                principal,
                action_gate,
                action,
                scope.thread_id.as_str(),
                Some(scope.workspace_id.as_str()),
            )
            .await?;
        let ProofResolution::Authorized(child) = child else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(workspace_id) = resource_id(WorkspaceResourceId::new(scope.workspace_id)) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(thread_id) = resource_id(ThreadResourceId::new(scope.thread_id)) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(turn_id) = resource_id(TurnResourceId::new(scope.turn_id)) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };

        Ok(ProofResolution::Authorized(AuthorizedTurn(
            AuthorizationProofCore {
                principal_id: principal.principal_id.clone(),
                action,
                resource: AuthorizationResource::Turn {
                    workspace_id,
                    thread_id,
                    turn_id,
                },
                decision: child.decision().clone(),
                collaboration_root_thread_id: None,
                thread_access: child.0.thread_access,
            },
        )))
    }

    pub(crate) async fn authorize_turn(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        turn_id: &str,
        expected_workspace_id: Option<&str>,
        expected_thread_id: Option<&str>,
    ) -> Result<ProofResolution<AuthorizedTurn>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(scope) = resolve_turn_authorization_scope(
            &self.store.database_connection(),
            turn_id,
            expected_workspace_id,
            expected_thread_id,
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(workspace_id) = resource_id(WorkspaceResourceId::new(scope.workspace_id.clone()))
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(thread_id) = resource_id(ThreadResourceId::new(scope.thread_id.clone())) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(turn_id) = resource_id(TurnResourceId::new(scope.turn_id.clone())) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let access = self.thread_facts(principal, &scope.thread).await?;
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::Turn {
                    workspace_id,
                    thread_id,
                    turn_id,
                },
                ResolvedResourceAccess::Turn(access),
            )
            .map(AuthorizedTurn))
    }

    pub(crate) async fn authorize_artifact(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        artifact_id: &str,
        expected_workspace_id: Option<&str>,
        expected_thread_id: Option<&str>,
    ) -> Result<ProofResolution<AuthorizedArtifact>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(scope) = resolve_artifact_authorization_scope(
            &self.store.database_connection(),
            artifact_id,
            expected_workspace_id,
            expected_thread_id,
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(workspace_id) = resource_id(WorkspaceResourceId::new(scope.workspace_id.clone()))
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let thread_id = match scope.thread_id.clone() {
            Some(thread_id) => {
                let Some(thread_id) = resource_id(ThreadResourceId::new(thread_id)) else {
                    return Ok(ProofResolution::Denied(missing_resource()));
                };
                Some(thread_id)
            }
            None => None,
        };
        let Some(artifact_id) = resource_id(ArtifactResourceId::new(scope.artifact_id)) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        // Artifacts produced by a task/subagent are bound to its internal
        // child thread. Members never receive direct ACL membership for those
        // implementation threads, so inherit the exact artifact operation
        // from the persisted root lineage just as thread/turn reads do.
        if self
            .service
            .runtime_principal_policy(principal.kind, principal.role_key.as_ref())
            == Some(super::RuntimePrincipalPolicy::ScopedCollaboration)
            && scope
                .thread
                .as_ref()
                .is_some_and(|thread| thread.access_class == PersistedThreadAccessClass::Internal)
        {
            let Some(child_thread_id) = scope.thread_id.as_deref() else {
                return Ok(ProofResolution::Denied(missing_resource()));
            };
            let child = self
                .authorize_internal_thread_via_root(
                    principal,
                    action_gate,
                    action,
                    child_thread_id,
                    Some(scope.workspace_id.as_str()),
                )
                .await?;
            let ProofResolution::Authorized(child) = child else {
                return Ok(ProofResolution::Denied(missing_resource()));
            };
            return Ok(ProofResolution::Authorized(AuthorizedArtifact(
                AuthorizationProofCore {
                    principal_id: principal.principal_id.clone(),
                    action,
                    resource: AuthorizationResource::Artifact {
                        workspace_id,
                        thread_id,
                        artifact_id,
                    },
                    decision: child.decision().clone(),
                    collaboration_root_thread_id: None,
                    thread_access: child.0.thread_access,
                },
            )));
        }
        let workspace = self
            .workspace_facts(
                principal,
                scope.workspace_id.as_str(),
                scope.workspace_is_active,
            )
            .await?;
        let thread = match scope.thread.as_ref() {
            Some(scope) => Some(self.thread_facts(principal, scope).await?),
            None => None,
        };
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::Artifact {
                    workspace_id,
                    thread_id,
                    artifact_id,
                },
                ResolvedResourceAccess::Artifact { workspace, thread },
            )
            .map(AuthorizedArtifact))
    }

    pub(crate) async fn authorize_task(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        task_id: &str,
        expected_workspace_id: Option<&str>,
        expected_root_thread_id: Option<&str>,
    ) -> Result<ProofResolution<AuthorizedTask>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(scope) = resolve_task_authorization_scope(
            &self.store.database_connection(),
            task_id,
            expected_workspace_id,
            expected_root_thread_id,
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(workspace_id) = resource_id(WorkspaceResourceId::new(scope.workspace_id.clone()))
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let root_thread_id = match scope.root_thread_id.clone() {
            Some(thread_id) => {
                let Some(thread_id) = resource_id(ThreadResourceId::new(thread_id)) else {
                    return Ok(ProofResolution::Denied(missing_resource()));
                };
                Some(thread_id)
            }
            None => None,
        };
        let Some(task_id) = resource_id(TaskResourceId::new(scope.task_id)) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let workspace = self
            .workspace_facts(
                principal,
                scope.workspace_id.as_str(),
                scope.workspace_is_active,
            )
            .await?;
        let root_thread = match scope.root_thread.as_ref() {
            Some(scope) => Some(self.thread_facts(principal, scope).await?),
            None => None,
        };
        let initiating_principal =
            scope.initiating_principal_id.as_ref() == Some(&principal.principal_id);
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::Task {
                    workspace_id,
                    root_thread_id,
                    task_id,
                },
                ResolvedResourceAccess::Task {
                    workspace,
                    root_thread,
                    initiating_principal,
                },
            )
            .map(AuthorizedTask))
    }

    pub(crate) async fn authorize_agents_document(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        workspace_id: &str,
        folder_id: Option<&str>,
        expected_revision: Option<i64>,
    ) -> Result<ProofResolution<AuthorizedAgentsDocument>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(workspace_scope) =
            resolve_workspace_authorization_scope(&self.store.database_connection(), workspace_id)
                .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(workspace_resource_id) = resource_id(WorkspaceResourceId::new(
            workspace_scope.workspace_id.clone(),
        )) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let folder_resource_id = match folder_id {
            Some(folder_id) => {
                let folder_id = folder_id.trim();
                if folder_id.is_empty()
                    || !self
                        .store
                        .list_thread_folders(workspace_scope.workspace_id.as_str())
                        .await?
                        .iter()
                        .any(|folder| folder.id == folder_id)
                {
                    return Ok(ProofResolution::Denied(missing_resource()));
                }
                let Some(folder_id) =
                    resource_id(AgentsDocumentResourceId::new(folder_id.to_owned()))
                else {
                    return Ok(ProofResolution::Denied(missing_resource()));
                };
                Some(folder_id)
            }
            None => None,
        };
        let current_revision = self
            .store
            .get_thread_agents_doc_explicit(workspace_scope.workspace_id.as_str(), folder_id)
            .await?
            .map(|doc| doc.version);
        if expected_revision.is_some() && expected_revision != current_revision {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let workspace = self
            .workspace_facts(
                principal,
                workspace_scope.workspace_id.as_str(),
                workspace_scope.is_active,
            )
            .await?;
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::AgentsDocument {
                    workspace_id: workspace_resource_id,
                    folder_id: folder_resource_id,
                    revision: current_revision,
                },
                ResolvedResourceAccess::AgentsDocument {
                    workspace,
                    scope_exists: true,
                },
            )
            .map(AuthorizedAgentsDocument))
    }

    pub(crate) async fn authorize_session(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        session_id: &AuthSessionId,
    ) -> Result<ProofResolution<AuthorizedSession>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let Some(scope) = resolve_session_authorization_scope(
            &self.store.database_connection(),
            session_id.as_str(),
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        if scope.gateway_id != principal.gateway_id.as_str() {
            return Ok(ProofResolution::Denied(missing_resource()));
        }
        let Ok(owner) = PrincipalId::new(scope.principal_id) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Ok(resolved_session_id) = AuthSessionId::new(scope.session_id) else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let owns_session = owner == principal.principal_id;
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::Session {
                    principal_id: owner,
                    session_id: resolved_session_id,
                },
                ResolvedResourceAccess::Session { owns_session },
            )
            .map(AuthorizedSession))
    }

    pub(crate) async fn authorize_persisted_capability(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        workspace_id: &str,
        kind: CapabilityKind,
        capability_id: &str,
    ) -> Result<ProofResolution<AuthorizedCapability>> {
        if let Some(denied) = self.preflight(action_gate, action) {
            return Ok(ProofResolution::Denied(denied));
        }
        let persisted_kind = match kind {
            CapabilityKind::Skill => PersistedCapabilityScopeKind::Skill,
            CapabilityKind::McpServer => PersistedCapabilityScopeKind::McpServer,
        };
        let Some(scope) = resolve_persisted_capability_authorization_scope(
            &self.store.database_connection(),
            persisted_kind,
            workspace_id,
            capability_id,
        )
        .await?
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let projected = match kind {
            CapabilityKind::Skill => self.service.skill_allowed(
                principal.kind,
                principal.role_key.as_ref(),
                scope.capability_id.as_str(),
            ),
            CapabilityKind::McpServer => self.service.mcp_server_allowed(
                principal.kind,
                principal.role_key.as_ref(),
                scope.capability_id.as_str(),
            ),
        };
        self.authorize_capability_scope(
            principal,
            action_gate,
            action,
            scope.workspace_id.as_str(),
            kind,
            scope.capability_id.as_str(),
            scope.workspace_is_active,
            scope.enabled && projected,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn authorize_capability_scope(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        workspace_id: &str,
        kind: CapabilityKind,
        capability_id: &str,
        workspace_is_active: bool,
        enabled: bool,
    ) -> Result<ProofResolution<AuthorizedCapability>> {
        let Some(workspace_id_value) =
            resource_id(WorkspaceResourceId::new(workspace_id.to_owned()))
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let Some(capability_id_value) =
            resource_id(CapabilityResourceId::new(capability_id.to_owned()))
        else {
            return Ok(ProofResolution::Denied(missing_resource()));
        };
        let workspace = self
            .workspace_facts(principal, workspace_id, workspace_is_active)
            .await?;
        Ok(self
            .finish(
                principal,
                action_gate,
                action,
                AuthorizationResource::Capability {
                    workspace_id: workspace_id_value,
                    kind,
                    id: capability_id_value,
                },
                ResolvedResourceAccess::Capability { workspace, enabled },
            )
            .map(AuthorizedCapability))
    }

    fn preflight(
        &self,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
    ) -> Option<AuthorizationDecision> {
        (!action_gate.permits_resource_resolution()).then(|| {
            self.service
                .authorize_resource(action_gate, action, ResolvedResourceAccess::Gateway)
        })
    }

    async fn workspace_facts(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        workspace_id: &str,
        workspace_active: bool,
    ) -> Result<WorkspaceAccessFacts> {
        let workspace_member = match self
            .service
            .runtime_principal_policy(principal.kind, principal.role_key.as_ref())
        {
            Some(super::RuntimePrincipalPolicy::Absolute) | None => false,
            Some(super::RuntimePrincipalPolicy::ScopedCollaboration) => find_workspace_membership(
                &self.store.database_connection(),
                &principal.principal_id,
                workspace_id,
            )
            .await
            .context("failed to resolve workspace authorization membership")?
            .is_some(),
        };
        Ok(WorkspaceAccessFacts {
            workspace_active,
            workspace_member,
        })
    }

    async fn thread_facts(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        scope: &pioneer_crud::ThreadAuthorizationScope,
    ) -> Result<ThreadAccessFacts> {
        let workspace = self
            .workspace_facts(
                principal,
                scope.workspace_id.as_str(),
                scope.workspace_is_active,
            )
            .await?;
        let thread_member = match self
            .service
            .runtime_principal_policy(principal.kind, principal.role_key.as_ref())
        {
            Some(super::RuntimePrincipalPolicy::Absolute) | None => false,
            Some(super::RuntimePrincipalPolicy::ScopedCollaboration) => find_thread_membership(
                &self.store.database_connection(),
                scope.thread_id.as_str(),
                &principal.principal_id,
            )
            .await
            .context("failed to resolve thread authorization membership")?
            .is_some(),
        };
        Ok(ThreadAccessFacts {
            workspace,
            access_class: match scope.access_class {
                PersistedThreadAccessClass::Private => ThreadAccessClass::Private,
                PersistedThreadAccessClass::Workspace => ThreadAccessClass::Workspace,
                PersistedThreadAccessClass::Internal => ThreadAccessClass::Internal,
            },
            resource_class: ThreadResourceClass::Root,
            thread_member,
            thread_creator: scope.creator_principal_id.as_ref() == Some(&principal.principal_id),
        })
    }

    fn finish(
        &self,
        principal: &AuthenticatedSessionPrincipal,
        action_gate: &ActionGateDecision,
        action: ResourceAction,
        resource: AuthorizationResource,
        access: ResolvedResourceAccess,
    ) -> ProofResolution<AuthorizationProofCore> {
        let thread_access = match access {
            ResolvedResourceAccess::Thread(facts) | ResolvedResourceAccess::Turn(facts) => {
                Some(facts)
            }
            _ => None,
        };
        let decision = self.service.authorize_resource(action_gate, action, access);
        if decision.is_allowed() {
            ProofResolution::Authorized(AuthorizationProofCore {
                principal_id: principal.principal_id.clone(),
                action,
                resource,
                decision,
                collaboration_root_thread_id: None,
                thread_access,
            })
        } else {
            ProofResolution::Denied(decision)
        }
    }
}

pub(crate) async fn persisted_actor_is_current<C: ConnectionTrait>(
    db: &C,
    principal: &AuthenticatedSessionPrincipal,
) -> Result<bool> {
    let Some(persisted_actor) =
        pioneer_crud::load_principal_by_id(db, &principal.principal_id).await?
    else {
        return Ok(false);
    };
    let Some(session) = pioneer_crud::load_session(db, &principal.session_id).await? else {
        return Ok(false);
    };
    let Some(device) = pioneer_crud::load_device(db, &principal.device_id).await? else {
        return Ok(false);
    };
    Ok(persisted_actor.gateway_id == principal.gateway_id
        && persisted_actor.kind == principal.kind
        && persisted_actor.status == PrincipalStatus::Active
        && persisted_actor.role_key.as_deref()
            == principal
                .role_key
                .as_ref()
                .map(pioneer_protocol::RoleKey::as_str)
        && session.gateway_id == principal.gateway_id.as_str()
        && session.principal_id == principal.principal_id.as_str()
        && session.device_id == principal.device_id.as_str()
        && session.status == "active"
        && device.gateway_id == principal.gateway_id.as_str()
        && device.principal_id == principal.principal_id.as_str()
        && device.status == "active")
}

#[derive(Debug, PartialEq, Eq)]
struct AuthorizationProofCore {
    principal_id: PrincipalId,
    action: ResourceAction,
    resource: AuthorizationResource,
    decision: AuthorizationDecision,
    collaboration_root_thread_id: Option<ThreadResourceId>,
    thread_access: Option<ThreadAccessFacts>,
}

impl AuthorizationProofCore {
    pub(crate) fn principal_id(&self) -> &PrincipalId {
        &self.principal_id
    }

    pub(crate) const fn action(&self) -> ResourceAction {
        self.action
    }

    pub(crate) fn resource(&self) -> &AuthorizationResource {
        &self.resource
    }

    pub(crate) fn decision(&self) -> &AuthorizationDecision {
        &self.decision
    }
}

macro_rules! authorized_proof_accessor {
    (principal_id) => {
        pub(crate) fn principal_id(&self) -> &PrincipalId {
            self.0.principal_id()
        }
    };
    (action) => {
        pub(crate) const fn action(&self) -> ResourceAction {
            self.0.action()
        }
    };
    (resource) => {
        pub(crate) fn resource(&self) -> &AuthorizationResource {
            self.0.resource()
        }
    };
    (decision) => {
        pub(crate) fn decision(&self) -> &AuthorizationDecision {
            self.0.decision()
        }
    };
}

macro_rules! authorized_proof {
    ($name:ident) => {
        #[derive(Debug, PartialEq, Eq)]
        pub(crate) struct $name(AuthorizationProofCore);
    };
    ($name:ident, $($accessor:ident),+ $(,)?) => {
        authorized_proof!($name);

        impl $name {
            $(authorized_proof_accessor!($accessor);)+
        }
    };
}

authorized_proof!(
    AuthorizedWorkspaceCollection,
    principal_id,
    action,
    decision,
);
authorized_proof!(
    AuthorizedWorkspace,
    principal_id,
    action,
    resource,
    decision,
);
authorized_proof!(AuthorizedThread, principal_id, action, resource, decision,);
authorized_proof!(AuthorizedTurn, principal_id, action, resource, decision);
authorized_proof!(AuthorizedArtifact, action, resource, decision);
authorized_proof!(AuthorizedTask, action, resource, decision);
authorized_proof!(AuthorizedSession, principal_id, action, resource, decision,);
authorized_proof!(AuthorizedCapability, decision);
authorized_proof!(AuthorizedAgentsDocument, action, resource, decision,);
authorized_proof!(
    AuthorizedInvitationGrants,
    principal_id,
    action,
    resource,
    decision,
);
authorized_proof!(
    AuthorizedInvitationCollection,
    principal_id,
    action,
    decision,
);
authorized_proof!(
    AuthorizedInvitation,
    principal_id,
    action,
    resource,
    decision,
);
authorized_proof!(AuthorizedMemberDirectory, principal_id, action, decision,);
authorized_proof!(AuthorizedMemberAvatar);
authorized_proof!(
    AuthorizedMemberPrincipal,
    principal_id,
    action,
    resource,
    decision,
);

impl AuthorizedWorkspace {
    pub(crate) fn workspace_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Workspace(workspace_id) => workspace_id.as_str(),
            _ => unreachable!("AuthorizedWorkspace always contains a workspace resource"),
        }
    }
}

impl AuthorizedInvitationGrants {
    pub(crate) fn workspace_ids(&self) -> Vec<&str> {
        match self.resource() {
            AuthorizationResource::InvitationGrantSet(workspace_ids) => workspace_ids
                .iter()
                .map(|workspace_id| workspace_id.as_str())
                .collect(),
            _ => unreachable!("AuthorizedInvitationGrants always contains an invitation grant set"),
        }
    }
}

impl AuthorizedInvitation {
    pub(crate) fn invitation_id(&self) -> &InvitationId {
        match self.resource() {
            AuthorizationResource::Invitation(invitation_id) => invitation_id,
            _ => unreachable!("AuthorizedInvitation always contains an invitation resource"),
        }
    }
}

impl AuthorizedMemberPrincipal {
    pub(crate) fn target_principal_id(&self) -> &PrincipalId {
        match self.resource() {
            AuthorizationResource::MemberPrincipal(principal_id) => principal_id,
            _ => unreachable!("AuthorizedMemberPrincipal always contains a principal resource"),
        }
    }
}

impl AuthorizedThread {
    pub(crate) fn workspace_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Thread { workspace_id, .. } => workspace_id.as_str(),
            _ => unreachable!("AuthorizedThread always contains a thread resource"),
        }
    }

    pub(crate) fn thread_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Thread { thread_id, .. } => thread_id.as_str(),
            _ => unreachable!("AuthorizedThread always contains a thread resource"),
        }
    }

    pub(crate) fn thread_access_class(&self) -> Option<ThreadAccessClass> {
        self.0.thread_access.map(|facts| facts.access_class)
    }

    /// Exact collaboration root carried by a thread proof. Root-thread proofs
    /// return their own id; internal-child proofs retain the root id resolved
    /// from durable lineage in `ThreadAccessFacts`.
    pub(crate) fn collaboration_root_thread_id(&self) -> &str {
        self.0
            .collaboration_root_thread_id
            .as_ref()
            .map_or_else(|| self.thread_id(), |thread_id| thread_id.as_str())
    }
}

impl AuthorizedAgentsDocument {
    pub(crate) fn workspace_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::AgentsDocument { workspace_id, .. } => workspace_id.as_str(),
            _ => unreachable!("AuthorizedAgentsDocument always contains an instruction resource"),
        }
    }

    pub(crate) fn folder_id(&self) -> Option<&str> {
        match self.resource() {
            AuthorizationResource::AgentsDocument { folder_id, .. } => {
                folder_id.as_ref().map(|folder_id| folder_id.as_str())
            }
            _ => unreachable!("AuthorizedAgentsDocument always contains an instruction resource"),
        }
    }
}

impl AuthorizedTurn {
    pub(crate) fn workspace_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Turn { workspace_id, .. } => workspace_id.as_str(),
            _ => unreachable!("AuthorizedTurn always contains a turn resource"),
        }
    }

    pub(crate) fn thread_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Turn { thread_id, .. } => thread_id.as_str(),
            _ => unreachable!("AuthorizedTurn always contains a turn resource"),
        }
    }

    pub(crate) fn turn_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Turn { turn_id, .. } => turn_id.as_str(),
            _ => unreachable!("AuthorizedTurn always contains a turn resource"),
        }
    }
}

impl AuthorizedArtifact {
    pub(crate) fn workspace_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Artifact { workspace_id, .. } => workspace_id.as_str(),
            _ => unreachable!("AuthorizedArtifact always contains an artifact resource"),
        }
    }

    pub(crate) fn thread_id(&self) -> Option<&str> {
        match self.resource() {
            AuthorizationResource::Artifact { thread_id, .. } => {
                thread_id.as_ref().map(|thread_id| thread_id.as_str())
            }
            _ => unreachable!("AuthorizedArtifact always contains an artifact resource"),
        }
    }

    pub(crate) fn artifact_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Artifact { artifact_id, .. } => artifact_id.as_str(),
            _ => unreachable!("AuthorizedArtifact always contains an artifact resource"),
        }
    }
}

impl AuthorizedTask {
    pub(crate) fn workspace_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Task { workspace_id, .. } => workspace_id.as_str(),
            _ => unreachable!("AuthorizedTask always contains a task resource"),
        }
    }

    #[cfg(test)]
    pub(crate) fn root_thread_id(&self) -> Option<&str> {
        match self.resource() {
            AuthorizationResource::Task { root_thread_id, .. } => {
                root_thread_id.as_ref().map(|thread_id| thread_id.as_str())
            }
            _ => unreachable!("AuthorizedTask always contains a task resource"),
        }
    }

    pub(crate) fn task_id(&self) -> &str {
        match self.resource() {
            AuthorizationResource::Task { task_id, .. } => task_id.as_str(),
            _ => unreachable!("AuthorizedTask always contains a task resource"),
        }
    }
}

#[cfg(test)]
pub(super) fn authorized_thread_for_test(
    principal_id: PrincipalId,
    action: ResourceAction,
    resource: AuthorizationResource,
    decision: AuthorizationDecision,
) -> AuthorizedThread {
    assert!(matches!(resource, AuthorizationResource::Thread { .. }));
    AuthorizedThread(AuthorizationProofCore {
        principal_id,
        action,
        resource,
        decision,
        collaboration_root_thread_id: None,
        thread_access: None,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ProofResolution<P> {
    Authorized(P),
    Denied(AuthorizationDecision),
}

impl<P> ProofResolution<P> {
    fn map<T>(self, map: impl FnOnce(P) -> T) -> ProofResolution<T> {
        match self {
            Self::Authorized(proof) => ProofResolution::Authorized(map(proof)),
            Self::Denied(decision) => ProofResolution::Denied(decision),
        }
    }

    #[cfg(test)]
    pub(crate) fn into_authorized(self) -> Option<P> {
        match self {
            Self::Authorized(proof) => Some(proof),
            Self::Denied(_) => None,
        }
    }

    pub(crate) fn denial(&self) -> Option<&AuthorizationDecision> {
        match self {
            Self::Authorized(_) => None,
            Self::Denied(decision) => Some(decision),
        }
    }
}

fn thread_resource(workspace_id: &str, thread_id: &str) -> Option<AuthorizationResource> {
    Some(AuthorizationResource::Thread {
        workspace_id: resource_id(WorkspaceResourceId::new(workspace_id.to_owned()))?,
        thread_id: resource_id(ThreadResourceId::new(thread_id.to_owned()))?,
    })
}

fn resource_id<T>(value: Result<T, ResourceIdError>) -> Option<T> {
    value.ok()
}

const fn missing_resource() -> AuthorizationDecision {
    AuthorizationDecision::Deny {
        reason: DenyReason::MissingAuthoritativeResource,
        disclosure: DisclosurePolicy::NotFound,
    }
}

const fn inactive_principal() -> AuthorizationDecision {
    AuthorizationDecision::Deny {
        reason: DenyReason::InactivePrincipal,
        disclosure: DisclosurePolicy::AuthenticationTerminal,
    }
}

#[cfg(test)]
mod tests {
    use pioneer_protocol::{
        AuthSessionId, DeviceId, GatewayId, PrincipalId, PrincipalKind, RoleKey,
    };
    use sea_orm::ConnectionTrait;

    use super::*;
    use crate::tests::authorization::{
        IsolatedEpic4Harness, MEMBER_A_ID, MEMBER_B_ID, THREAD_BLUE_PRIVATE_A_ID,
        THREAD_GREEN_PRIVATE_B_ID, THREAD_RED_INTERNAL_ID, THREAD_RED_PRIVATE_A_ID,
        THREAD_RED_PRIVATE_B_ID, THREAD_RED_WORKSPACE_ID, WORKSPACE_BLUE_ID, WORKSPACE_GREEN_ID,
        WORKSPACE_RED_ID,
    };

    const WORKSPACE_ID: &str = "W00000000000000000001";
    const PRIVATE_THREAD_ID: &str = "T0000000000000000000A";
    const WORKSPACE_THREAD_ID: &str = "T0000000000000000000W";
    const INTERNAL_THREAD_ID: &str = "T0000000000000000000I";
    const TURN_ID: &str = "U0000000000000000000A";
    const INTERNAL_TURN_ID: &str = "U0000000000000000000I";
    const AMBIGUOUS_ARTIFACT_ID: &str = "artifact-ambiguous";
    const INTERNAL_ARTIFACT_ID: &str = "artifact-internal";
    const UNBOUND_ARTIFACT_ID: &str = "artifact-unbound";
    const PRIVATE_ROOT_TASK_ID: &str = "K0000000000000000000A";
    const PRIVATE_CHILD_TASK_ID: &str = "K0000000000000000000B";
    const PRIVATE_GRANDCHILD_TASK_ID: &str = "K0000000000000000000C";
    const WORKSPACE_ROOT_TASK_ID: &str = "K0000000000000000000W";

    fn principal(id: &str, kind: PrincipalKind) -> AuthenticatedSessionPrincipal {
        AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new("G00000000000000000001").expect("gateway"),
            principal_id: PrincipalId::new(id).expect("principal"),
            kind,
            role_key: (kind == PrincipalKind::User).then(RoleKey::member),
            device_id: DeviceId::new("D0000000000000000000Z").expect("device"),
            session_id: AuthSessionId::new("S0000000000000000000Z").expect("session"),
            access_jti: "J0000000000000000000Z".to_owned(),
            access_expires_at_unix: u64::MAX,
        }
    }

    async fn resolver_fixture() -> (IsolatedEpic4Harness, AuthorizationResolver) {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated Epic 4 harness");
        harness
            .database
            .execute_unprepared(
                "INSERT INTO thread(\
                    id,workspace_id,name,preview,mode,model,model_provider,status,\
                    origin_kind,sidebar_visibility,access_class,created_at,updated_at,\
                    created_by_actor_kind,created_by_actor_id\
                 ) VALUES\
                    ('T0000000000000000000A','W00000000000000000001','Private A','',\
                     'chat','test','test','active','user','visible','private',\
                     CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'principal',\
                     'P0000000000000000000A'),\
                    ('T0000000000000000000W','W00000000000000000001','Workspace','',\
                     'chat','test','test','active','user','visible','workspace',\
                     CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'principal',\
                     'P0000000000000000000A'),\
                    ('T0000000000000000000I','W00000000000000000001','Internal','',\
                     'agent','test','test','active','task_run','hidden','internal',\
                     CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'system',NULL);\
                 INSERT INTO thread_membership(\
                    thread_id,principal_id,added_by_actor_kind,added_by_actor_id,\
                    created_at,updated_at\
                 ) VALUES(\
                    'T0000000000000000000A','P0000000000000000000A','system',NULL,\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
                 );\
                 INSERT INTO thread_lineage(\
                    child_thread_id,parent_thread_id,root_thread_id,depth,created_at,\
                    origin_kind,created_by_thread_id,created_by_turn_id\
                 ) VALUES(\
                    'T0000000000000000000I','T0000000000000000000A',\
                    'T0000000000000000000A',1,CURRENT_TIMESTAMP,'task_run',\
                    'T0000000000000000000A','U0000000000000000000A'\
                 );\
                 INSERT INTO turn(\
                    id,thread_id,status,prompt_manifest_json,created_at,updated_at,\
                    turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id\
                 ) VALUES(\
                    'U0000000000000000000A','T0000000000000000000A','completed','{}',\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'chat','user','principal',\
                    'P0000000000000000000A'),(\
                    'U0000000000000000000I','T0000000000000000000I','completed','{}',\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'agent','task','system',NULL\
                 );\
                 INSERT INTO artifact(\
                    id,workspace_id,primary_thread_id,display_name,kind,status,\
                    created_by_kind,created_by_actor_id,created_at,updated_at,metadata_json\
                 ) VALUES(\
                    'artifact-ambiguous','W00000000000000000001',\
                    'T0000000000000000000A','Ambiguous','file','ready','principal',\
                    'P0000000000000000000A',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}'),(\
                    'artifact-internal','W00000000000000000001',\
                    'T0000000000000000000I','Internal','file','ready','system',NULL,\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}'),(\
                    'artifact-unbound','W00000000000000000001',NULL,\
                    'Unbound','file','ready','system',NULL,\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}'\
                 );\
                 INSERT INTO artifact_binding(\
                    id,artifact_id,workspace_id,thread_id,binding_kind,direction,\
                    created_at,metadata_json\
                 ) VALUES(\
                    'binding-ambiguous','artifact-ambiguous','W00000000000000000001',\
                    'T0000000000000000000W','thread','output',CURRENT_TIMESTAMP,'{}'),(\
                    'binding-internal','artifact-internal','W00000000000000000001',\
                    'T0000000000000000000I','task_result','output',CURRENT_TIMESTAMP,'{}'\
                 );\
                 INSERT INTO task(\
                    id,workspace_id,owner_kind,owner_id,created_by_thread_id,\
                    root_task_id,parent_task_id,executor_kind,status,title,goal\
                 ) VALUES(\
                    'K0000000000000000000A','W00000000000000000001','user',\
                    'P0000000000000000000A','T0000000000000000000A',NULL,NULL,\
                    'agent','waiting','Private root','Private root'),(\
                    'K0000000000000000000B','W00000000000000000001','thread',\
                    'T0000000000000000000I','T0000000000000000000I',\
                    'K0000000000000000000A','K0000000000000000000A',\
                    'agent','waiting','Private child','Private child'),(\
                    'K0000000000000000000C','W00000000000000000001','thread',\
                    'T0000000000000000000I','T0000000000000000000I',\
                    'K0000000000000000000A','K0000000000000000000B',\
                    'agent','waiting','Private grandchild','Private grandchild'),(\
                    'K0000000000000000000W','W00000000000000000001','user',\
                    'P0000000000000000000A','T0000000000000000000W',NULL,NULL,\
                    'agent','waiting','Workspace root','Workspace root'\
                 );",
            )
            .await
            .expect("materialize resolver fixture");
        let store = CrudStore::new(harness.database.clone());
        let context = super::super::ExecutionAuthorizationContext::for_test(
            &principal(MEMBER_A_ID, PrincipalKind::User),
            WORKSPACE_ID,
            PRIVATE_THREAD_ID,
            &pioneer_protocol::default_turn_permission_profile_snapshot(),
            None,
        );
        store
            .set_turn_execution_authorization_context(
                TURN_ID,
                context
                    .to_persisted_json()
                    .expect("parent authority should encode")
                    .as_str(),
            )
            .await
            .expect("parent authority should persist");
        let resolver = AuthorizationResolver::new(store);
        (harness, resolver)
    }

    #[tokio::test]
    async fn workspace_resolution_uses_member_join_and_superuser_absolute_access() {
        let (_harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member = principal(MEMBER_A_ID, PrincipalKind::User);
        let superuser = principal("P00000000000000000001", PrincipalKind::Superuser);
        let member_gate = service.authorize_action(
            member.kind,
            member.role_key.as_ref(),
            ResourceAction::WorkspaceRead,
        );
        let superuser_gate = service.authorize_action(
            superuser.kind,
            superuser.role_key.as_ref(),
            ResourceAction::WorkspaceRead,
        );

        let member_proof = resolver
            .authorize_workspace(
                &member,
                &member_gate,
                ResourceAction::WorkspaceRead,
                WORKSPACE_ID,
            )
            .await
            .expect("resolve Member workspace")
            .into_authorized()
            .expect("Member has persisted workspace membership");
        assert_eq!(member_proof.workspace_id(), WORKSPACE_ID);

        assert!(
            resolver
                .authorize_workspace(
                    &member,
                    &member_gate,
                    ResourceAction::WorkspaceRead,
                    WORKSPACE_GREEN_ID,
                )
                .await
                .expect("resolve ungranted workspace")
                .into_authorized()
                .is_none(),
            "ordinary-principal resolution must use the membership-scoped SQL path"
        );
        assert!(
            resolver
                .authorize_workspace(
                    &superuser,
                    &superuser_gate,
                    ResourceAction::WorkspaceRead,
                    WORKSPACE_GREEN_ID,
                )
                .await
                .expect("resolve Superuser workspace")
                .into_authorized()
                .is_some(),
            "Superuser must not require membership rows"
        );
    }

    #[tokio::test]
    async fn member_mcp_capability_resolves_ui_id_and_execution_name_to_the_same_server() {
        let (harness, resolver) = resolver_fixture().await;
        harness
            .database
            .execute_unprepared(
                "INSERT INTO mcp_server_installation(\
                    id,scope_kind,scope_key,name,source_kind,source_ref,transport_kind,\
                    transport_json,auth_json,secret_refs_json,enabled,\
                    allow_implicit_invocation,required,fingerprint\
                 ) VALUES(\
                    'mcp-installation-member','workspace','W00000000000000000001',\
                    'member-tools','config','{}','stdio','{}','{}','[]',1,0,0,\
                    'mcp-member-fingerprint'\
                 )",
            )
            .await
            .expect("materialize enabled Member MCP capability");

        let service = AuthorizationService::new();
        let member = principal(MEMBER_A_ID, PrincipalKind::User);
        let gate = service.authorize_action(
            member.kind,
            member.role_key.as_ref(),
            ResourceAction::McpUse,
        );

        for capability_id in ["mcp-installation-member", "member-tools"] {
            let authorized = resolver
                .authorize_persisted_capability(
                    &member,
                    &gate,
                    ResourceAction::McpUse,
                    WORKSPACE_ID,
                    CapabilityKind::McpServer,
                    capability_id,
                )
                .await
                .expect("resolve Member MCP capability")
                .into_authorized();
            assert!(
                authorized.is_some(),
                "Member MCP capability should resolve through `{capability_id}`"
            );
        }

        harness
            .database
            .execute_unprepared(
                "UPDATE mcp_server_installation SET enabled=0 \
                 WHERE id='mcp-installation-member'",
            )
            .await
            .expect("disable Member MCP capability");
        assert!(
            resolver
                .authorize_persisted_capability(
                    &member,
                    &gate,
                    ResourceAction::McpUse,
                    WORKSPACE_ID,
                    CapabilityKind::McpServer,
                    "mcp-installation-member",
                )
                .await
                .expect("resolve disabled Member MCP capability")
                .into_authorized()
                .is_none(),
            "disabled MCP capabilities must remain unusable"
        );
    }

    #[tokio::test]
    async fn invitation_grants_reauthorize_the_actor_inside_the_transaction() {
        let (harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member = principal(MEMBER_A_ID, PrincipalKind::User);
        let gate = service.authorize_action(
            member.kind,
            member.role_key.as_ref(),
            ResourceAction::InvitationCreate,
        );

        harness
            .database
            .execute_unprepared(
                "UPDATE gateway_principal SET status='suspended' \
                 WHERE id='P0000000000000000000A'",
            )
            .await
            .expect("suspend invitation actor");

        assert_eq!(
            resolver
                .authorize_invitation_grants(
                    &harness.database,
                    &member,
                    &gate,
                    &[WorkspaceId::new(WORKSPACE_ID).expect("workspace")],
                )
                .await
                .expect("resolve invitation grants")
                .denial(),
            Some(&AuthorizationDecision::Deny {
                reason: DenyReason::InactivePrincipal,
                disclosure: DisclosurePolicy::AuthenticationTerminal,
            }),
            "the request principal snapshot must not authorize an actor that became inactive"
        );
    }

    #[tokio::test]
    async fn persisted_two_member_three_workspace_thread_matrix_is_exact_and_fail_closed() {
        let harness = IsolatedEpic4Harness::new()
            .await
            .expect("isolated Epic 4 harness");
        let resolver = AuthorizationResolver::new(CrudStore::new(harness.database.clone()));
        let service = AuthorizationService::new();
        let member_a = principal(MEMBER_A_ID, PrincipalKind::User);
        let member_b = principal(MEMBER_B_ID, PrincipalKind::User);
        let superuser = principal("P00000000000000000001", PrincipalKind::Superuser);

        for (principal, allowed, denied) in [
            (
                &member_a,
                [WORKSPACE_RED_ID, WORKSPACE_BLUE_ID],
                WORKSPACE_GREEN_ID,
            ),
            (
                &member_b,
                [WORKSPACE_RED_ID, WORKSPACE_GREEN_ID],
                WORKSPACE_BLUE_ID,
            ),
        ] {
            let gate = service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::WorkspaceRead,
            );
            for workspace_id in allowed {
                assert!(
                    resolver
                        .authorize_workspace(
                            principal,
                            &gate,
                            ResourceAction::WorkspaceRead,
                            workspace_id,
                        )
                        .await
                        .expect("resolve granted workspace")
                        .into_authorized()
                        .is_some()
                );
            }
            assert_eq!(
                resolver
                    .authorize_workspace(principal, &gate, ResourceAction::WorkspaceRead, denied,)
                    .await
                    .expect("resolve forbidden workspace")
                    .denial(),
                Some(&AuthorizationDecision::Deny {
                    reason: DenyReason::MissingAuthoritativeResource,
                    disclosure: DisclosurePolicy::NotFound,
                })
            );
        }

        let member_cases = [
            (
                &member_a,
                [
                    (THREAD_RED_PRIVATE_A_ID, WORKSPACE_RED_ID, true),
                    (THREAD_RED_PRIVATE_B_ID, WORKSPACE_RED_ID, false),
                    (THREAD_RED_WORKSPACE_ID, WORKSPACE_RED_ID, true),
                    (THREAD_RED_INTERNAL_ID, WORKSPACE_RED_ID, false),
                    (THREAD_BLUE_PRIVATE_A_ID, WORKSPACE_BLUE_ID, true),
                    (THREAD_GREEN_PRIVATE_B_ID, WORKSPACE_GREEN_ID, false),
                ],
            ),
            (
                &member_b,
                [
                    (THREAD_RED_PRIVATE_A_ID, WORKSPACE_RED_ID, false),
                    (THREAD_RED_PRIVATE_B_ID, WORKSPACE_RED_ID, true),
                    (THREAD_RED_WORKSPACE_ID, WORKSPACE_RED_ID, true),
                    (THREAD_RED_INTERNAL_ID, WORKSPACE_RED_ID, false),
                    (THREAD_BLUE_PRIVATE_A_ID, WORKSPACE_BLUE_ID, false),
                    (THREAD_GREEN_PRIVATE_B_ID, WORKSPACE_GREEN_ID, true),
                ],
            ),
        ];
        for (principal, cases) in member_cases {
            let gate = service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::ThreadRead,
            );
            for (thread_id, workspace_id, allowed) in cases {
                let resolution = resolver
                    .authorize_thread(
                        principal,
                        &gate,
                        ResourceAction::ThreadRead,
                        thread_id,
                        Some(workspace_id),
                    )
                    .await
                    .expect("resolve persisted thread matrix case");
                assert_eq!(
                    resolution.into_authorized().is_some(),
                    allowed,
                    "unexpected access for principal {} to thread {thread_id}",
                    principal.principal_id
                );
            }
        }

        let superuser_gate = service.authorize_action(
            superuser.kind,
            superuser.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );
        for (thread_id, workspace_id) in [
            (THREAD_RED_PRIVATE_A_ID, WORKSPACE_RED_ID),
            (THREAD_RED_PRIVATE_B_ID, WORKSPACE_RED_ID),
            (THREAD_RED_WORKSPACE_ID, WORKSPACE_RED_ID),
            (THREAD_RED_INTERNAL_ID, WORKSPACE_RED_ID),
            (THREAD_BLUE_PRIVATE_A_ID, WORKSPACE_BLUE_ID),
            (THREAD_GREEN_PRIVATE_B_ID, WORKSPACE_GREEN_ID),
        ] {
            assert!(
                resolver
                    .authorize_thread(
                        &superuser,
                        &superuser_gate,
                        ResourceAction::ThreadRead,
                        thread_id,
                        Some(workspace_id),
                    )
                    .await
                    .expect("resolve absolute Superuser thread access")
                    .into_authorized()
                    .is_some(),
                "Superuser must not need workspace or thread membership for {thread_id}"
            );
        }

        for principal in [&member_a, &superuser] {
            let gate = service.authorize_action(
                principal.kind,
                principal.role_key.as_ref(),
                ResourceAction::ThreadRead,
            );
            for (thread_id, workspace_id) in [
                (THREAD_RED_PRIVATE_A_ID, WORKSPACE_GREEN_ID),
                ("T00000000000000000099", WORKSPACE_RED_ID),
            ] {
                assert_eq!(
                    resolver
                        .authorize_thread(
                            principal,
                            &gate,
                            ResourceAction::ThreadRead,
                            thread_id,
                            Some(workspace_id),
                        )
                        .await
                        .expect("resolve missing or malformed parent-child pair")
                        .denial(),
                    Some(&AuthorizationDecision::Deny {
                        reason: DenyReason::MissingAuthoritativeResource,
                        disclosure: DisclosurePolicy::NotFound,
                    })
                );
            }
        }
    }

    #[tokio::test]
    async fn private_thread_proof_is_exact_and_peer_access_is_not_found() {
        let (_harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member_a = principal(MEMBER_A_ID, PrincipalKind::User);
        let member_b = principal(MEMBER_B_ID, PrincipalKind::User);
        let gate_a = service.authorize_action(
            member_a.kind,
            member_a.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );
        let gate_b = service.authorize_action(
            member_b.kind,
            member_b.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );

        let authorized = resolver
            .authorize_thread(
                &member_a,
                &gate_a,
                ResourceAction::ThreadRead,
                PRIVATE_THREAD_ID,
                Some(WORKSPACE_ID),
            )
            .await
            .expect("resolve private thread")
            .into_authorized()
            .expect("member A proof");
        assert_eq!(authorized.principal_id(), &member_a.principal_id);
        assert_eq!(authorized.action(), ResourceAction::ThreadRead);
        assert!(matches!(
            authorized.resource(),
            AuthorizationResource::Thread {
                workspace_id,
                thread_id,
            } if workspace_id.as_str() == WORKSPACE_ID
                && thread_id.as_str() == PRIVATE_THREAD_ID
        ));

        let denied = resolver
            .authorize_thread(
                &member_b,
                &gate_b,
                ResourceAction::ThreadRead,
                PRIVATE_THREAD_ID,
                Some(WORKSPACE_ID),
            )
            .await
            .expect("resolve peer private thread");
        assert_eq!(
            denied.denial(),
            Some(&AuthorizationDecision::Deny {
                reason: DenyReason::NoPrivateThreadMembership,
                disclosure: DisclosurePolicy::NotFound,
            })
        );
    }

    #[tokio::test]
    async fn overlapping_member_creator_participant_and_superuser_matrix_is_exact() {
        let (harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member_a = principal(MEMBER_A_ID, PrincipalKind::User);
        let member_b = principal(MEMBER_B_ID, PrincipalKind::User);
        let superuser = principal("P00000000000000000001", PrincipalKind::Superuser);

        let read_a = service.authorize_action(
            member_a.kind,
            member_a.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );
        let read_b = service.authorize_action(
            member_b.kind,
            member_b.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );
        let read_superuser = service.authorize_action(
            superuser.kind,
            superuser.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );

        assert!(
            resolver
                .authorize_thread(
                    &member_a,
                    &read_a,
                    ResourceAction::ThreadRead,
                    PRIVATE_THREAD_ID,
                    Some(WORKSPACE_ID),
                )
                .await
                .expect("resolve creator private access")
                .into_authorized()
                .is_some()
        );
        assert!(
            resolver
                .authorize_thread(
                    &member_b,
                    &read_b,
                    ResourceAction::ThreadRead,
                    PRIVATE_THREAD_ID,
                    Some(WORKSPACE_ID),
                )
                .await
                .expect("resolve peer private access")
                .into_authorized()
                .is_none()
        );
        assert!(
            resolver
                .authorize_thread(
                    &member_b,
                    &read_b,
                    ResourceAction::ThreadRead,
                    WORKSPACE_THREAD_ID,
                    Some(WORKSPACE_ID),
                )
                .await
                .expect("resolve overlapping workspace access")
                .into_authorized()
                .is_some()
        );
        assert!(
            resolver
                .authorize_thread(
                    &superuser,
                    &read_superuser,
                    ResourceAction::ThreadRead,
                    PRIVATE_THREAD_ID,
                    Some(WORKSPACE_ID),
                )
                .await
                .expect("resolve absolute Superuser access")
                .into_authorized()
                .is_some(),
            "Superuser must not require a thread membership"
        );

        harness
            .database
            .execute_unprepared(
                "INSERT INTO thread_membership(\
                    thread_id,principal_id,added_by_actor_kind,added_by_actor_id,\
                    created_at,updated_at\
                 ) VALUES(\
                    'T0000000000000000000A','P0000000000000000000B','principal',\
                    'P0000000000000000000A',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
                 )",
            )
            .await
            .expect("add explicit participant");
        assert!(
            resolver
                .authorize_thread(
                    &member_b,
                    &read_b,
                    ResourceAction::ThreadRead,
                    PRIVATE_THREAD_ID,
                    Some(WORKSPACE_ID),
                )
                .await
                .expect("resolve explicit participant")
                .into_authorized()
                .is_some()
        );

        let manage_a = service.authorize_action(
            member_a.kind,
            member_a.role_key.as_ref(),
            ResourceAction::ThreadManage,
        );
        let manage_b = service.authorize_action(
            member_b.kind,
            member_b.role_key.as_ref(),
            ResourceAction::ThreadManage,
        );
        assert!(
            resolver
                .authorize_thread(
                    &member_a,
                    &manage_a,
                    ResourceAction::ThreadManage,
                    PRIVATE_THREAD_ID,
                    Some(WORKSPACE_ID),
                )
                .await
                .expect("resolve creator management")
                .into_authorized()
                .is_some()
        );
        assert!(
            resolver
                .authorize_thread(
                    &member_b,
                    &manage_b,
                    ResourceAction::ThreadManage,
                    PRIVATE_THREAD_ID,
                    Some(WORKSPACE_ID),
                )
                .await
                .expect("resolve participant management")
                .into_authorized()
                .is_none(),
            "participant access must not transfer creator management"
        );
    }

    #[tokio::test]
    async fn parent_mismatch_denies_member_and_superuser_before_proof_construction() {
        let (_harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member = principal(MEMBER_A_ID, PrincipalKind::User);
        let superuser = principal("P00000000000000000001", PrincipalKind::Superuser);
        let member_gate = service.authorize_action(
            member.kind,
            member.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );
        let superuser_gate = service.authorize_action(
            superuser.kind,
            superuser.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );

        for (principal, gate) in [(&member, &member_gate), (&superuser, &superuser_gate)] {
            let denied = resolver
                .authorize_thread(
                    principal,
                    gate,
                    ResourceAction::ThreadRead,
                    PRIVATE_THREAD_ID,
                    Some("W00000000000000000002"),
                )
                .await
                .expect("resolve mismatched thread");
            assert_eq!(
                denied.denial(),
                Some(&AuthorizationDecision::Deny {
                    reason: DenyReason::MissingAuthoritativeResource,
                    disclosure: DisclosurePolicy::NotFound,
                })
            );
        }
    }

    #[tokio::test]
    async fn internal_thread_is_denied_directly_and_requires_authorized_root_lineage() {
        let (harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member = principal(MEMBER_A_ID, PrincipalKind::User);
        let gate = service.authorize_action(
            member.kind,
            member.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );

        let direct = resolver
            .authorize_thread(
                &member,
                &gate,
                ResourceAction::ThreadRead,
                INTERNAL_THREAD_ID,
                Some(WORKSPACE_ID),
            )
            .await
            .expect("resolve direct internal thread");
        assert_eq!(
            direct.denial(),
            Some(&AuthorizationDecision::Deny {
                reason: DenyReason::MissingAuthoritativeResource,
                disclosure: DisclosurePolicy::NotFound,
            })
        );

        let inherited = resolver
            .authorize_internal_thread_via_root(
                &member,
                &gate,
                ResourceAction::ThreadRead,
                INTERNAL_THREAD_ID,
                None,
            )
            .await
            .expect("resolve internal thread through root")
            .into_authorized()
            .expect("authorized root permits indirect internal projection");
        assert_eq!(inherited.thread_id(), INTERNAL_THREAD_ID);

        let peer = principal(MEMBER_B_ID, PrincipalKind::User);
        let peer_gate = service.authorize_action(
            peer.kind,
            peer.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );
        assert!(
            resolver
                .authorize_internal_thread_via_root(
                    &peer,
                    &peer_gate,
                    ResourceAction::ThreadRead,
                    INTERNAL_THREAD_ID,
                    Some(WORKSPACE_ID),
                )
                .await
                .expect("resolve peer internal projection")
                .into_authorized()
                .is_none(),
            "guessed child id must not bypass inaccessible root ACL"
        );

        harness
            .database
            .execute_unprepared(
                "UPDATE \"turn\" SET execution_authorization_context_json=NULL \
                 WHERE id='U0000000000000000000A'",
            )
            .await
            .expect("parent authority should be removable for fail-closed test");
        assert!(
            resolver
                .authorize_internal_thread_via_root(
                    &member,
                    &gate,
                    ResourceAction::ThreadRead,
                    INTERNAL_THREAD_ID,
                    Some(WORKSPACE_ID),
                )
                .await
                .expect("missing parent authority should resolve as deny")
                .into_authorized()
                .is_none(),
            "missing parent authority must never become unrestricted child access"
        );
    }

    #[tokio::test]
    async fn internal_turn_requires_and_inherits_authorized_root_lineage() {
        let (_harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member = principal(MEMBER_A_ID, PrincipalKind::User);
        let gate = service.authorize_action(
            member.kind,
            member.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );

        assert_eq!(
            resolver
                .authorize_turn(
                    &member,
                    &gate,
                    ResourceAction::ThreadRead,
                    INTERNAL_TURN_ID,
                    Some(WORKSPACE_ID),
                    Some(INTERNAL_THREAD_ID),
                )
                .await
                .expect("resolve direct internal turn")
                .denial(),
            Some(&AuthorizationDecision::Deny {
                reason: DenyReason::MissingAuthoritativeResource,
                disclosure: DisclosurePolicy::NotFound,
            })
        );

        let inherited = resolver
            .authorize_internal_turn_via_root(
                &member,
                &gate,
                ResourceAction::ThreadRead,
                INTERNAL_TURN_ID,
                None,
                Some(INTERNAL_THREAD_ID),
            )
            .await
            .expect("resolve internal turn through root")
            .into_authorized()
            .expect("authorized root permits internal turn projection");
        assert!(matches!(
            inherited.resource(),
            AuthorizationResource::Turn {
                workspace_id,
                thread_id,
                turn_id,
            } if workspace_id.as_str() == WORKSPACE_ID
                && thread_id.as_str() == INTERNAL_THREAD_ID
                && turn_id.as_str() == INTERNAL_TURN_ID
        ));

        let peer = principal(MEMBER_B_ID, PrincipalKind::User);
        let peer_gate = service.authorize_action(
            peer.kind,
            peer.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );
        assert!(
            resolver
                .authorize_internal_turn_via_root(
                    &peer,
                    &peer_gate,
                    ResourceAction::ThreadRead,
                    INTERNAL_TURN_ID,
                    Some(WORKSPACE_ID),
                    Some(INTERNAL_THREAD_ID),
                )
                .await
                .expect("resolve peer internal turn")
                .into_authorized()
                .is_none(),
            "guessed internal turn must not bypass inaccessible root ACL"
        );
    }

    #[tokio::test]
    async fn turn_proof_uses_persisted_thread_parent_and_rejects_forged_pair() {
        let (_harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member = principal(MEMBER_A_ID, PrincipalKind::User);
        let gate = service.authorize_action(
            member.kind,
            member.role_key.as_ref(),
            ResourceAction::ThreadRead,
        );

        let authorized = resolver
            .authorize_turn(
                &member,
                &gate,
                ResourceAction::ThreadRead,
                TURN_ID,
                Some(WORKSPACE_ID),
                Some(PRIVATE_THREAD_ID),
            )
            .await
            .expect("resolve turn")
            .into_authorized()
            .expect("turn proof");
        assert!(matches!(
            authorized.resource(),
            AuthorizationResource::Turn {
                workspace_id,
                thread_id,
                turn_id,
            } if workspace_id.as_str() == WORKSPACE_ID
                && thread_id.as_str() == PRIVATE_THREAD_ID
                && turn_id.as_str() == TURN_ID
        ));

        let denied = resolver
            .authorize_turn(
                &member,
                &gate,
                ResourceAction::ThreadRead,
                TURN_ID,
                Some(WORKSPACE_ID),
                Some(WORKSPACE_THREAD_ID),
            )
            .await
            .expect("resolve forged turn parent");
        assert!(denied.into_authorized().is_none());
    }

    #[tokio::test]
    async fn ambiguous_artifact_parent_fails_closed_even_for_superuser() {
        let (_harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let superuser = principal("P00000000000000000001", PrincipalKind::Superuser);
        let gate = service.authorize_action(
            superuser.kind,
            superuser.role_key.as_ref(),
            ResourceAction::ArtifactRead,
        );

        let denied = resolver
            .authorize_artifact(
                &superuser,
                &gate,
                ResourceAction::ArtifactRead,
                AMBIGUOUS_ARTIFACT_ID,
                Some(WORKSPACE_ID),
                None,
            )
            .await
            .expect("resolve ambiguous artifact");
        assert_eq!(
            denied.denial(),
            Some(&AuthorizationDecision::Deny {
                reason: DenyReason::MissingAuthoritativeResource,
                disclosure: DisclosurePolicy::NotFound,
            })
        );
    }

    #[tokio::test]
    async fn artifact_collection_uses_the_exact_parent_resolver_before_pagination() {
        let (harness, _resolver) = resolver_fixture().await;
        harness
            .database
            .execute_unprepared(
                "INSERT INTO artifact(\
                    id,workspace_id,primary_thread_id,display_name,kind,status,\
                    created_by_kind,created_by_actor_id,created_at,updated_at,metadata_json\
                 ) VALUES(\
                    'artifact-primary-only','W00000000000000000001',\
                    'T0000000000000000000A','Primary only','file','ready','principal',\
                    'P0000000000000000000A',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}'),(\
                    'artifact-rootless-task','W00000000000000000001',\
                    'T0000000000000000000A','Rootless task','file','ready','principal',\
                    'P0000000000000000000A',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}'\
                 );\
                 INSERT INTO task(\
                    id,workspace_id,owner_kind,owner_id,root_task_id,parent_task_id,\
                    executor_kind,status,title,goal\
                 ) VALUES(\
                    'K0000000000000000000R','W00000000000000000001','user',\
                    'P0000000000000000000A',NULL,NULL,'agent','waiting',\
                    'Rootless task','Rootless task'\
                 );\
                 INSERT INTO artifact_binding(\
                    id,artifact_id,workspace_id,task_id,binding_kind,direction,\
                    created_at,metadata_json\
                 ) VALUES(\
                    'binding-rootless-task','artifact-rootless-task',\
                    'W00000000000000000001','K0000000000000000000R',\
                    'task_result','output',CURRENT_TIMESTAMP,'{}'\
                 );",
            )
            .await
            .expect("materialize exact artifact collection cases");

        let store = CrudStore::new(harness.database.clone());
        let artifact_ids = store
            .list_artifact_ids_for_authorized_thread_roots(
                WORKSPACE_ID,
                &[
                    vec![PRIVATE_THREAD_ID.to_owned(), INTERNAL_THREAD_ID.to_owned()],
                    vec![WORKSPACE_THREAD_ID.to_owned()],
                ],
            )
            .await
            .expect("resolve authorized artifact collection");
        assert_eq!(
            artifact_ids,
            vec![
                INTERNAL_ARTIFACT_ID.to_owned(),
                "artifact-primary-only".to_owned()
            ],
            "primary-thread-only artifacts remain visible, while ambiguous and rootless-task \
             bindings fail closed even when every individual visible root is authorized"
        );
    }

    #[tokio::test]
    async fn artifact_acl_inherits_internal_lineage_and_tracks_root_visibility() {
        let (harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member_a = principal(MEMBER_A_ID, PrincipalKind::User);
        let member_b = principal(MEMBER_B_ID, PrincipalKind::User);
        let gate_a = service.authorize_action(
            member_a.kind,
            member_a.role_key.as_ref(),
            ResourceAction::ArtifactRead,
        );
        let gate_b = service.authorize_action(
            member_b.kind,
            member_b.role_key.as_ref(),
            ResourceAction::ArtifactRead,
        );

        let proof = resolver
            .authorize_artifact(
                &member_a,
                &gate_a,
                ResourceAction::ArtifactRead,
                INTERNAL_ARTIFACT_ID,
                Some(WORKSPACE_ID),
                None,
            )
            .await
            .expect("resolve internal artifact through root")
            .into_authorized()
            .expect("private root participant inherits artifact access");
        assert_eq!(proof.thread_id(), Some(PRIVATE_THREAD_ID));
        assert!(
            resolver
                .authorize_artifact(
                    &member_b,
                    &gate_b,
                    ResourceAction::ArtifactRead,
                    INTERNAL_ARTIFACT_ID,
                    Some(WORKSPACE_ID),
                    None,
                )
                .await
                .expect("resolve peer internal artifact")
                .into_authorized()
                .is_none()
        );

        harness
            .database
            .execute_unprepared(
                "UPDATE thread SET access_class='workspace' \
                 WHERE id='T0000000000000000000A'",
            )
            .await
            .expect("make root workspace-visible");
        assert!(
            resolver
                .authorize_artifact(
                    &member_b,
                    &gate_b,
                    ResourceAction::ArtifactRead,
                    INTERNAL_ARTIFACT_ID,
                    Some(WORKSPACE_ID),
                    None,
                )
                .await
                .expect("resolve artifact after visibility change")
                .into_authorized()
                .is_some(),
            "future artifact authorization must reflect current root visibility"
        );
    }

    #[tokio::test]
    async fn task_child_and_grandchild_ids_inherit_exact_root_acl() {
        let (harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member_a = principal(MEMBER_A_ID, PrincipalKind::User);
        let member_b = principal(MEMBER_B_ID, PrincipalKind::User);
        let read_a = service.authorize_action(
            member_a.kind,
            member_a.role_key.as_ref(),
            ResourceAction::TaskRead,
        );
        let read_b = service.authorize_action(
            member_b.kind,
            member_b.role_key.as_ref(),
            ResourceAction::TaskRead,
        );

        for task_id in [
            PRIVATE_ROOT_TASK_ID,
            PRIVATE_CHILD_TASK_ID,
            PRIVATE_GRANDCHILD_TASK_ID,
        ] {
            let proof = resolver
                .authorize_task(
                    &member_a,
                    &read_a,
                    ResourceAction::TaskRead,
                    task_id,
                    Some(WORKSPACE_ID),
                    Some(PRIVATE_THREAD_ID),
                )
                .await
                .expect("resolve private-root task")
                .into_authorized()
                .expect("root participant inherits descendant task access");
            assert_eq!(proof.task_id(), task_id);
            assert_eq!(proof.root_thread_id(), Some(PRIVATE_THREAD_ID));

            assert!(
                resolver
                    .authorize_task(
                        &member_b,
                        &read_b,
                        ResourceAction::TaskRead,
                        task_id,
                        Some(WORKSPACE_ID),
                        Some(PRIVATE_THREAD_ID),
                    )
                    .await
                    .expect("resolve guessed descendant task")
                    .into_authorized()
                    .is_none(),
                "guessed descendant id must not bypass private root ACL"
            );
        }

        harness
            .database
            .execute_unprepared(
                "DELETE FROM thread_membership \
                 WHERE thread_id='T0000000000000000000A' \
                   AND principal_id='P0000000000000000000A'",
            )
            .await
            .expect("revoke private root access");
        assert!(
            resolver
                .authorize_task(
                    &member_a,
                    &read_a,
                    ResourceAction::TaskRead,
                    PRIVATE_GRANDCHILD_TASK_ID,
                    Some(WORKSPACE_ID),
                    Some(PRIVATE_THREAD_ID),
                )
                .await
                .expect("resolve descendant after access loss")
                .into_authorized()
                .is_none(),
            "future task reads and mutations must re-evaluate current root ACL"
        );
    }

    #[tokio::test]
    async fn task_management_is_shared_by_authorized_root_collaborators() {
        let (_harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member_a = principal(MEMBER_A_ID, PrincipalKind::User);
        let member_b = principal(MEMBER_B_ID, PrincipalKind::User);
        let manage_a = service.authorize_action(
            member_a.kind,
            member_a.role_key.as_ref(),
            ResourceAction::TaskReview,
        );
        let manage_b = service.authorize_action(
            member_b.kind,
            member_b.role_key.as_ref(),
            ResourceAction::TaskReview,
        );

        assert!(
            resolver
                .authorize_task(
                    &member_a,
                    &manage_a,
                    ResourceAction::TaskReview,
                    WORKSPACE_ROOT_TASK_ID,
                    Some(WORKSPACE_ID),
                    Some(WORKSPACE_THREAD_ID),
                )
                .await
                .expect("resolve initiator management")
                .into_authorized()
                .is_some()
        );
        assert!(
            resolver
                .authorize_task(
                    &member_b,
                    &manage_b,
                    ResourceAction::TaskReview,
                    WORKSPACE_ROOT_TASK_ID,
                    Some(WORKSPACE_ID),
                    Some(WORKSPACE_THREAD_ID),
                )
                .await
                .expect("resolve peer management")
                .into_authorized()
                .is_some(),
            "root-thread collaboration authority must allow peer task management"
        );
    }

    #[tokio::test]
    async fn task_collection_applies_root_acl_before_limit() {
        let (harness, _resolver) = resolver_fixture().await;
        harness
            .database
            .execute_unprepared(
                "UPDATE task SET updated_at='2030-01-01T00:00:00Z' \
                 WHERE id IN (\
                    'K0000000000000000000A',\
                    'K0000000000000000000B',\
                    'K0000000000000000000C'\
                 );\
                 UPDATE task SET updated_at='2020-01-01T00:00:00Z' \
                 WHERE id='K0000000000000000000W';",
            )
            .await
            .expect("order inaccessible tasks before accessible task");
        let store = CrudStore::new(harness.database.clone());
        let tasks = store
            .list_tasks_scoped(
                pioneer_protocol::TaskListParams {
                    workspace_id: WORKSPACE_ID.to_owned(),
                    owner_kind: None,
                    owner_id: None,
                    parent_task_id: None,
                    root_task_id: None,
                    status: None,
                    cursor: None,
                    limit: Some(1),
                },
                Some(&pioneer_crud::TaskRootAccessFilter {
                    allowed_root_thread_ids: vec![WORKSPACE_THREAD_ID.to_owned()],
                }),
            )
            .await
            .expect("list root-authorized tasks");
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.id.as_str())
                .collect::<Vec<_>>(),
            vec![WORKSPACE_ROOT_TASK_ID],
            "inaccessible rows must be excluded before pagination"
        );
    }

    #[tokio::test]
    async fn unbound_artifact_is_member_hidden_but_superuser_visible() {
        let (_harness, resolver) = resolver_fixture().await;
        let service = AuthorizationService::new();
        let member = principal(MEMBER_A_ID, PrincipalKind::User);
        let superuser = principal("P00000000000000000001", PrincipalKind::Superuser);
        let member_gate = service.authorize_action(
            member.kind,
            member.role_key.as_ref(),
            ResourceAction::ArtifactRead,
        );
        let superuser_gate = service.authorize_action(
            superuser.kind,
            superuser.role_key.as_ref(),
            ResourceAction::ArtifactRead,
        );
        assert!(
            resolver
                .authorize_artifact(
                    &member,
                    &member_gate,
                    ResourceAction::ArtifactRead,
                    UNBOUND_ARTIFACT_ID,
                    Some(WORKSPACE_ID),
                    None,
                )
                .await
                .expect("resolve Member unbound artifact")
                .into_authorized()
                .is_none()
        );
        assert!(
            resolver
                .authorize_artifact(
                    &superuser,
                    &superuser_gate,
                    ResourceAction::ArtifactRead,
                    UNBOUND_ARTIFACT_ID,
                    Some(WORKSPACE_ID),
                    None,
                )
                .await
                .expect("resolve Superuser unbound artifact")
                .into_authorized()
                .is_some()
        );
    }
}
