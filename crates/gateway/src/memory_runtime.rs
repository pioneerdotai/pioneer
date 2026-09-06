use anyhow::{Context, Result, bail};
use pioneer_config::GatewayMemoryConfig;
use pioneer_crud::CrudStore;
use pioneer_memory::hooks::MemoryTurnContext;
use pioneer_memory::{
    MemoryMutationBoundary, MemoryOperationContext, MemoryReadPolicy, MemoryService,
    MemoryServiceConfig, MemorySourceAccessPolicy, MemvidMemoryBackend,
};
use pioneer_protocol::{MemoryActor, MemoryScope, MemoryScopeKind};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GatewayMemoryContextDefaults {
    pub allow_global_user: bool,
    pub allow_global_agent: bool,
}

#[derive(Clone)]
pub(crate) struct GatewayMemoryRuntime {
    enabled: bool,
    service: Arc<MemoryService>,
    context_defaults: GatewayMemoryContextDefaults,
    #[cfg(test)]
    capsules_root: Option<PathBuf>,
}

impl GatewayMemoryRuntime {
    pub(crate) fn from_config(
        store: Arc<CrudStore>,
        runtime_home: &Path,
        config: &GatewayMemoryConfig,
    ) -> Result<Self> {
        let context_defaults = GatewayMemoryContextDefaults {
            allow_global_user: config.allow_global_user_by_default,
            allow_global_agent: config.allow_global_agent_by_default,
        };

        if !config.enabled {
            return Ok(Self {
                enabled: false,
                service: Arc::new(MemoryService::with_noop_backend(store)),
                context_defaults,
                #[cfg(test)]
                capsules_root: None,
            });
        }

        let capsules_root = config
            .resolve_capsules_root(runtime_home)
            .context("failed to resolve gateway memory capsule root")?;
        std::fs::create_dir_all(capsules_root.as_path()).with_context(|| {
            format!(
                "failed to create gateway memory capsule root `{}`",
                capsules_root.display()
            )
        })?;

        let backend = Arc::new(MemvidMemoryBackend::with_capsules_root(
            store.clone(),
            capsules_root.clone(),
        ));
        let service = Arc::new(MemoryService::new(
            store,
            backend,
            MemoryServiceConfig::default(),
        ));

        Ok(Self {
            enabled: true,
            service,
            context_defaults,
            #[cfg(test)]
            capsules_root: Some(capsules_root),
        })
    }

    pub(crate) fn ensure_enabled(&self) -> Result<()> {
        if self.enabled {
            Ok(())
        } else {
            bail!("memory runtime is disabled")
        }
    }

    pub(crate) fn service(&self) -> Arc<MemoryService> {
        self.service.clone()
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn operation_context(
        &self,
        workspace_id: Option<String>,
        actor: Option<MemoryActor>,
    ) -> MemoryOperationContext {
        MemoryOperationContext {
            workspace_id,
            actor,
            allow_global_user: self.context_defaults.allow_global_user,
            allow_global_agent: self.context_defaults.allow_global_agent,
            ..MemoryOperationContext::default()
        }
    }

    pub(crate) fn operation_context_for_authorized_turn(
        &self,
        turn: &MemoryTurnContext,
        actor: Option<MemoryActor>,
        scoped_collaboration: bool,
    ) -> MemoryOperationContext {
        let conversation_thread_id = turn.effective_conversation_thread_id().to_owned();
        let mut context = MemoryOperationContext {
            workspace_id: Some(turn.workspace_id.clone()),
            thread_id: Some(conversation_thread_id.clone()),
            task_id: turn.task_id.clone(),
            agent_id: turn.agent_id.clone(),
            actor,
            allow_global_user: self.context_defaults.allow_global_user,
            allow_global_agent: self.context_defaults.allow_global_agent,
            ..MemoryOperationContext::default()
        };
        if scoped_collaboration {
            context.allow_global_user = false;
            context.allow_global_agent = false;
            context.read_policy = Some(MemoryReadPolicy {
                allow_normal: true,
                allow_personal: false,
                allow_secret_like: false,
                allow_regulated: false,
            });
            let mut owned_scopes = vec![MemoryScope {
                kind: MemoryScopeKind::Thread,
                key: conversation_thread_id.clone(),
            }];
            if let Some(task_id) = turn.task_id.as_ref() {
                owned_scopes.push(MemoryScope {
                    kind: MemoryScopeKind::Task,
                    key: task_id.clone(),
                });
            }
            context.source_access = MemorySourceAccessPolicy::accessible_threads([
                conversation_thread_id,
                turn.thread_id.clone(),
            ])
            .with_owned_scopes(owned_scopes);
            context.mutation_boundary = MemoryMutationBoundary::thread_capsule_with_sources(
                turn.effective_conversation_thread_id(),
                turn.task_id.clone(),
                [turn.thread_id.clone()],
            );
        }
        context
    }
}

#[cfg(test)]
impl GatewayMemoryRuntime {
    pub(crate) fn disabled(store: Arc<CrudStore>) -> Self {
        Self {
            enabled: false,
            service: Arc::new(MemoryService::with_noop_backend(store)),
            context_defaults: GatewayMemoryContextDefaults {
                allow_global_user: true,
                allow_global_agent: false,
            },
            capsules_root: None,
        }
    }
    pub(crate) fn capsules_root(&self) -> Option<&Path> {
        self.capsules_root.as_deref()
    }
}

#[cfg(test)]
mod scope_contract_tests {
    use super::*;

    #[tokio::test]
    async fn scoped_turn_grants_stable_owners_but_not_global_or_foreign_memory() {
        let store = Arc::new(CrudStore::new(
            sea_orm::Database::connect("sqlite::memory:").await.unwrap(),
        ));
        let runtime = GatewayMemoryRuntime::disabled(store);
        let turn = MemoryTurnContext {
            workspace_id: "workspace".to_owned(),
            thread_id: "run_b".to_owned(),
            conversation_thread_id: Some("root".to_owned()),
            turn_id: "turn_b".to_owned(),
            mode: pioneer_protocol::ThreadMode::Agent,
            input_text: String::new(),
            task_id: Some("task".to_owned()),
            agent_id: None,
            principal_id: None,
        };
        let context = runtime.operation_context_for_authorized_turn(&turn, None, true);
        for (kind, key) in [
            (MemoryScopeKind::Thread, "root"),
            (MemoryScopeKind::Task, "task"),
        ] {
            let scope = MemoryScope {
                kind,
                key: key.to_owned(),
            };
            assert!(context.allows_memory_record(&scope, Some("run_a")));
            assert!(
                context
                    .mutation_boundary
                    .validate_owned_scope(&scope)
                    .is_ok()
            );
        }
        for (kind, key) in [
            (MemoryScopeKind::Thread, "other_root"),
            (MemoryScopeKind::Task, "other_task"),
            (MemoryScopeKind::User, "default"),
        ] {
            let scope = MemoryScope {
                kind,
                key: key.to_owned(),
            };
            assert!(!context.allows_memory_record(&scope, Some("run_a")));
            assert!(
                context
                    .mutation_boundary
                    .validate_owned_scope(&scope)
                    .is_err()
            );
        }
        assert!(!context.allow_global_user);
        assert!(
            context
                .tool_scope_contract(true)
                .contains("mutation scopes: thread, task.")
        );
        assert!(
            context
                .tool_scope_contract(false)
                .contains("read scopes: workspace, thread, task.")
        );
        assert!(
            context
                .tool_scope_contract(false)
                .contains("source-filtered")
        );
        assert_eq!(
            context.scope_read_denial(&MemoryScope {
                kind: MemoryScopeKind::User,
                key: "default".into(),
            }),
            Some("global_user_memory_unavailable")
        );
        assert!(
            !context.source_access.allows_source_thread(Some("run_a")),
            "scope grants must not authorize fabricated provenance"
        );
        let owner = runtime.operation_context_for_authorized_turn(&turn, None, false);
        assert!(owner.allow_global_user);
        assert!(
            owner
                .tool_scope_contract(false)
                .contains("read scopes: user, workspace, thread, task.")
        );
    }
}
