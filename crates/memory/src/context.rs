use crate::MemoryReadPolicy;
use pioneer_crud::{
    MemoryWorkspaceGuard, global_agent_memory_scope_key, workspace_agent_memory_scope_key,
};
use pioneer_protocol::{MemoryActor, MemoryScope, MemoryScopeKind};

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryResolvedScopes {
    pub scopes: Vec<MemoryScope>,
    pub workspace_guard: Option<MemoryWorkspaceGuard>,
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

    pub fn resolved_scopes(&self, explicit_scopes: &[MemoryScope]) -> MemoryResolvedScopes {
        MemoryResolvedScopes {
            scopes: self.effective_scopes(explicit_scopes),
            workspace_guard: self.workspace_guard(),
        }
    }
}
