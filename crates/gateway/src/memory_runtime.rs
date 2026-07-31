use anyhow::{Context, Result, bail};
use pioneer_config::GatewayMemoryConfig;
use pioneer_crud::CrudStore;
use pioneer_memory::hooks::MemoryTurnContext;
use pioneer_memory::{
    MemoryOperationContext, MemoryReadPolicy, MemoryService, MemoryServiceConfig,
    MemorySourceAccessPolicy, MemvidMemoryBackend,
};
use pioneer_protocol::MemoryActor;
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
        member_restricted: bool,
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
        if member_restricted {
            context.allow_global_user = false;
            context.allow_global_agent = false;
            context.read_policy = Some(MemoryReadPolicy {
                allow_normal: true,
                allow_personal: false,
                allow_secret_like: false,
                allow_regulated: false,
            });
            context.source_access =
                MemorySourceAccessPolicy::accessible_threads([conversation_thread_id]);
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
