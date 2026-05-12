use async_trait::async_trait;
use pioneer_crud::CrudStore;
use pioneer_promt::{
    AgentsDocPromptResolver, ResolvedAgentsDocPrompt, agents_doc_package,
    hooks::AgentsDocPromptHookPackage,
};
use std::sync::Arc;

pub(crate) fn agents_doc_prompt_hook_package(
    crud_store: Arc<CrudStore>,
) -> AgentsDocPromptHookPackage {
    agents_doc_package(Arc::new(GatewayAgentsDocPromptResolver { crud_store }))
}

struct GatewayAgentsDocPromptResolver {
    crud_store: Arc<CrudStore>,
}

#[async_trait]
impl AgentsDocPromptResolver for GatewayAgentsDocPromptResolver {
    async fn resolve_agents_doc_prompt(
        &self,
        workspace_id: &str,
        thread_id: &str,
    ) -> Result<Option<ResolvedAgentsDocPrompt>, String> {
        self.crud_store
            .resolve_thread_agents_doc_for_thread(workspace_id, thread_id)
            .await
            .map(|resolved| {
                resolved.map(|record| ResolvedAgentsDocPrompt {
                    id: record.doc.id,
                    version: record.doc.version,
                    content: record.doc.content,
                    source_path: record.source_path,
                })
            })
            .map_err(|error| error.to_string())
    }
}
