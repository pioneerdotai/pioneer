use crate::MemoryReadPolicy;
use pioneer_crud::{
    MemoryWorkspaceGuard, global_agent_memory_scope_key, workspace_agent_memory_scope_key,
};
use pioneer_protocol::{MemoryActor, MemoryScope, MemoryScopeKind};
use std::collections::BTreeSet;

/// Server-derived mutation boundary for durable memory.
///
/// Read visibility and mutation authority are deliberately separate. A caller
/// may be allowed to recall workspace-visible knowledge while only being
/// allowed to create, update, or forget memory owned by one collaboration
/// capsule. Keeping this fact in the domain-service context makes direct RPC,
/// agent tools, and post-turn hooks share the same enforcement point.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum MemoryMutationBoundary {
    #[default]
    Unrestricted,
    ThreadCapsule {
        root_thread_id: String,
        task_id: Option<String>,
        authorized_source_thread_ids: BTreeSet<String>,
    },
}

impl MemoryMutationBoundary {
    pub fn thread_capsule(root_thread_id: impl Into<String>, task_id: Option<String>) -> Self {
        let root_thread_id = root_thread_id.into();
        let authorized_source_thread_ids = BTreeSet::from([root_thread_id.clone()]);
        Self::ThreadCapsule {
            root_thread_id,
            task_id,
            authorized_source_thread_ids,
        }
    }

    pub fn thread_capsule_with_sources(
        root_thread_id: impl Into<String>,
        task_id: Option<String>,
        source_thread_ids: impl IntoIterator<Item = String>,
    ) -> Self {
        let root_thread_id = root_thread_id.into();
        let mut authorized_source_thread_ids: BTreeSet<String> =
            source_thread_ids.into_iter().collect();
        authorized_source_thread_ids.insert(root_thread_id.clone());
        Self::ThreadCapsule {
            root_thread_id,
            task_id,
            authorized_source_thread_ids,
        }
    }

    pub fn validate_scope(
        &self,
        scope: &MemoryScope,
        source_thread_id: Option<&str>,
    ) -> anyhow::Result<()> {
        let Self::ThreadCapsule {
            authorized_source_thread_ids,
            ..
        } = self
        else {
            return Ok(());
        };

        if !source_thread_id
            .is_some_and(|source_thread_id| authorized_source_thread_ids.contains(source_thread_id))
        {
            anyhow::bail!("memory mutation provenance is outside the authorized thread capsule");
        }

        self.validate_owned_scope(scope)
    }

    /// Existing records belong to a stable scope. Their historical source
    /// thread is evidence, not the authority of the current mutator.
    pub fn validate_owned_scope(&self, scope: &MemoryScope) -> anyhow::Result<()> {
        let Self::ThreadCapsule {
            root_thread_id,
            task_id,
            ..
        } = self
        else {
            return Ok(());
        };
        match scope.kind {
            MemoryScopeKind::Thread if scope.key == *root_thread_id => Ok(()),
            MemoryScopeKind::Task
                if task_id
                    .as_deref()
                    .is_some_and(|authorized_task_id| scope.key == authorized_task_id) =>
            {
                Ok(())
            }
            MemoryScopeKind::Thread | MemoryScopeKind::Task => {
                anyhow::bail!("memory mutation scope is outside the authorized thread capsule")
            }
            MemoryScopeKind::User | MemoryScopeKind::Workspace | MemoryScopeKind::Agent => {
                anyhow::bail!(
                    "personal, workspace, and agent memory require a separate authorization scope"
                )
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemorySourceAccessPolicy {
    accessible_thread_ids: Option<BTreeSet<String>>,
    owned_scopes: Vec<MemoryScope>,
}

impl MemorySourceAccessPolicy {
    pub fn unrestricted() -> Self {
        Self {
            accessible_thread_ids: None,
            owned_scopes: Vec::new(),
        }
    }

    pub fn accessible_threads(thread_ids: impl IntoIterator<Item = String>) -> Self {
        Self {
            accessible_thread_ids: Some(thread_ids.into_iter().collect()),
            owned_scopes: Vec::new(),
        }
    }

    /// Server-derived scope grants. Only thread/task memory can be owned by a
    /// collaboration capsule; this cannot grant personal/global memory.
    pub fn with_owned_scopes(mut self, scopes: impl IntoIterator<Item = MemoryScope>) -> Self {
        self.owned_scopes = scopes
            .into_iter()
            .filter(|scope| matches!(scope.kind, MemoryScopeKind::Thread | MemoryScopeKind::Task))
            .collect();
        self
    }

    pub fn owned_scopes(&self) -> Vec<MemoryScope> {
        self.owned_scopes.clone()
    }

    pub fn allows_record(&self, scope: &MemoryScope, source_thread_id: Option<&str>) -> bool {
        self.owned_scopes.contains(scope) || self.allows_source_thread(source_thread_id)
    }

    pub fn requires_authoritative_provenance(&self) -> bool {
        self.accessible_thread_ids.is_some()
    }

    pub fn allows_source_thread(&self, source_thread_id: Option<&str>) -> bool {
        match &self.accessible_thread_ids {
            None => true,
            Some(accessible_thread_ids) => source_thread_id
                .filter(|thread_id| !thread_id.trim().is_empty())
                .is_some_and(|thread_id| accessible_thread_ids.contains(thread_id)),
        }
    }

    pub fn accessible_thread_ids(&self) -> Option<Vec<String>> {
        self.accessible_thread_ids
            .as_ref()
            .map(|thread_ids| thread_ids.iter().cloned().collect())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryOperationContext {
    pub workspace_id: Option<String>,
    pub thread_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
    pub actor: Option<MemoryActor>,
    pub now_unix: Option<i64>,
    pub allow_global_user: bool,
    pub allow_global_agent: bool,
    pub read_policy: Option<MemoryReadPolicy>,
    pub source_access: MemorySourceAccessPolicy,
    pub mutation_boundary: MemoryMutationBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryResolvedScopes {
    pub scopes: Vec<MemoryScope>,
    pub workspace_guard: Option<MemoryWorkspaceGuard>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryActiveScopes {
    pub scopes: Vec<MemoryScope>,
    pub primary_scope: Option<MemoryScope>,
    pub priorities: Vec<MemoryScopePriority>,
    pub explicit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryScopePriority {
    pub scope: MemoryScope,
    pub rank: u32,
}

impl MemoryActiveScopes {
    pub fn rank_for(&self, scope: &MemoryScope) -> Option<u32> {
        self.priorities
            .iter()
            .find(|priority| priority.scope == *scope)
            .map(|priority| priority.rank)
    }
}

impl MemoryOperationContext {
    pub fn now_or(&self, fallback: i64) -> i64 {
        self.now_unix.unwrap_or(fallback)
    }

    pub fn workspace_guard(&self) -> Option<MemoryWorkspaceGuard> {
        self.workspace_id
            .as_ref()
            .map(|workspace_id| MemoryWorkspaceGuard {
                workspace_id: workspace_id.clone(),
                allow_global_user: self.allow_global_user,
                allow_global_agent: self.allow_global_agent,
            })
    }

    pub fn allows_source_thread(&self, source_thread_id: Option<&str>) -> bool {
        self.source_access.allows_source_thread(source_thread_id)
    }

    pub fn allows_memory_record(
        &self,
        scope: &MemoryScope,
        source_thread_id: Option<&str>,
    ) -> bool {
        self.source_access.allows_record(scope, source_thread_id)
    }

    pub fn accessible_source_thread_ids(&self) -> Option<Vec<String>> {
        self.source_access.accessible_thread_ids()
    }

    pub fn effective_scopes(&self, explicit_scopes: &[MemoryScope]) -> Vec<MemoryScope> {
        if !explicit_scopes.is_empty() {
            return explicit_scopes.to_vec();
        }

        let mut scopes = vec![MemoryScope {
            kind: MemoryScopeKind::User,
            key: "default".to_owned(),
        }];

        if let Some(workspace_id) = &self.workspace_id {
            scopes.push(MemoryScope {
                kind: MemoryScopeKind::Workspace,
                key: workspace_id.clone(),
            });
        }

        if let Some(thread_id) = &self.thread_id {
            scopes.push(MemoryScope {
                kind: MemoryScopeKind::Thread,
                key: thread_id.clone(),
            });
        }

        if let Some(task_id) = &self.task_id {
            scopes.push(MemoryScope {
                kind: MemoryScopeKind::Task,
                key: task_id.clone(),
            });
        }

        if let Some(agent_id) = &self.agent_id {
            if let Some(workspace_id) = &self.workspace_id {
                scopes.push(MemoryScope {
                    kind: MemoryScopeKind::Agent,
                    key: workspace_agent_memory_scope_key(workspace_id, agent_id),
                });
            } else if self.allow_global_agent {
                scopes.push(MemoryScope {
                    kind: MemoryScopeKind::Agent,
                    key: global_agent_memory_scope_key(agent_id),
                });
            }
        }

        scopes
    }

    pub fn active_scopes(&self, explicit_scopes: &[MemoryScope]) -> MemoryActiveScopes {
        let scopes = self.effective_scopes(explicit_scopes);
        let explicit = !explicit_scopes.is_empty();
        let priority_scopes = if explicit {
            scopes.clone()
        } else {
            self.default_active_scope_priority(&scopes)
        };
        let primary_scope = priority_scopes.first().cloned();
        let priorities = priority_scopes
            .into_iter()
            .enumerate()
            .map(|(index, scope)| MemoryScopePriority {
                scope,
                rank: (index + 1) as u32,
            })
            .collect();

        MemoryActiveScopes {
            scopes,
            primary_scope,
            priorities,
            explicit,
        }
    }

    pub fn resolved_scopes(&self, explicit_scopes: &[MemoryScope]) -> MemoryResolvedScopes {
        MemoryResolvedScopes {
            scopes: self.effective_scopes(explicit_scopes),
            workspace_guard: self.workspace_guard(),
        }
    }

    fn default_active_scope_priority(&self, searchable_scopes: &[MemoryScope]) -> Vec<MemoryScope> {
        let mut priority_scopes = Vec::new();

        if let Some(thread_id) = &self.thread_id {
            push_scope_if_searchable(
                &mut priority_scopes,
                searchable_scopes,
                MemoryScope {
                    kind: MemoryScopeKind::Thread,
                    key: thread_id.clone(),
                },
            );
        }

        if let Some(task_id) = &self.task_id {
            push_scope_if_searchable(
                &mut priority_scopes,
                searchable_scopes,
                MemoryScope {
                    kind: MemoryScopeKind::Task,
                    key: task_id.clone(),
                },
            );
        }

        if let Some(agent_id) = &self.agent_id {
            if let Some(workspace_id) = &self.workspace_id {
                push_scope_if_searchable(
                    &mut priority_scopes,
                    searchable_scopes,
                    MemoryScope {
                        kind: MemoryScopeKind::Agent,
                        key: workspace_agent_memory_scope_key(workspace_id, agent_id),
                    },
                );
            }
        }

        if let Some(workspace_id) = &self.workspace_id {
            push_scope_if_searchable(
                &mut priority_scopes,
                searchable_scopes,
                MemoryScope {
                    kind: MemoryScopeKind::Workspace,
                    key: workspace_id.clone(),
                },
            );
        }

        push_scope_if_searchable(
            &mut priority_scopes,
            searchable_scopes,
            MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".to_owned(),
            },
        );

        if let Some(agent_id) = &self.agent_id
            && self.workspace_id.is_none()
            && self.allow_global_agent
        {
            push_scope_if_searchable(
                &mut priority_scopes,
                searchable_scopes,
                MemoryScope {
                    kind: MemoryScopeKind::Agent,
                    key: global_agent_memory_scope_key(agent_id),
                },
            );
        }

        for scope in searchable_scopes {
            push_unique_scope(&mut priority_scopes, scope.clone());
        }

        priority_scopes
    }
}

fn push_scope_if_searchable(
    priority_scopes: &mut Vec<MemoryScope>,
    searchable_scopes: &[MemoryScope],
    scope: MemoryScope,
) {
    if searchable_scopes
        .iter()
        .any(|candidate| candidate == &scope)
    {
        push_unique_scope(priority_scopes, scope);
    }
}

fn push_unique_scope(scopes: &mut Vec<MemoryScope>, scope: MemoryScope) {
    if !scopes.iter().any(|candidate| candidate == &scope) {
        scopes.push(scope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(kind: MemoryScopeKind, key: &str) -> MemoryScope {
        MemoryScope {
            kind,
            key: key.to_owned(),
        }
    }

    #[test]
    fn thread_capsule_mutation_boundary_is_exact_and_not_workspace_wide() {
        let boundary = MemoryMutationBoundary::thread_capsule("thread-root", None);

        assert!(
            boundary
                .validate_scope(
                    &scope(MemoryScopeKind::Thread, "thread-root"),
                    Some("thread-root")
                )
                .is_ok()
        );
        assert!(
            boundary
                .validate_scope(
                    &scope(MemoryScopeKind::Workspace, "workspace-a"),
                    Some("thread-root")
                )
                .is_err()
        );
        assert!(
            boundary
                .validate_scope(
                    &scope(MemoryScopeKind::Thread, "thread-other"),
                    Some("thread-other")
                )
                .is_err()
        );
    }

    #[test]
    fn thread_capsule_allows_only_its_active_task_scope() {
        let boundary =
            MemoryMutationBoundary::thread_capsule("thread-root", Some("task-current".to_owned()));

        assert!(
            boundary
                .validate_scope(
                    &scope(MemoryScopeKind::Task, "task-current"),
                    Some("thread-root")
                )
                .is_ok()
        );
        assert!(
            boundary
                .validate_scope(
                    &scope(MemoryScopeKind::Task, "task-other"),
                    Some("thread-root")
                )
                .is_err()
        );
        assert!(
            boundary
                .validate_scope(
                    &scope(MemoryScopeKind::Task, "task-current"),
                    Some("thread-other")
                )
                .is_err()
        );
    }
}
