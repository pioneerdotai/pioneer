//! The existing skill materializer, carried by the existing managed MCP facade.
//! Package and schema identities are captured at admission; invocation still
//! passes through GatewayTurnMcpInvoker and the ordinary skill continuation gate.
use crate::{
    mcp_service::McpService,
    turn_mcp::{
        McpProjectionLimits, McpSelectionReason, ResolvedMcpTurnProjection, ResolvedMcpTurnTool,
        invoker::{
            CurrentMcpToolIdentity, TurnMcpInvocationError, TurnMcpInvocationErrorCode,
            TurnMcpRuntimeView,
        },
    },
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) const SKILL_SERVER_ID: &str = "pioneer-skill-tools-v1";
pub(crate) const SKILL_SERVER_NAME: &str = "pioneer_skills";

pub(crate) struct AcpSkillTooling {
    pub(crate) skills: Vec<pioneer_skills::ResolvedSkill>,
    pub(crate) plan: pioneer_skills::SkillRuntimePlan,
    config: pioneer_agent::SkillsLoopConfig,
    materialized: pioneer_tools::SkillRuntimeToolMaterialization,
    packages: BTreeMap<String, String>,
    fingerprint: String,
}

impl std::fmt::Debug for AcpSkillTooling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcpSkillTooling")
            .field("fingerprint", &self.fingerprint)
            .finish_non_exhaustive()
    }
}

fn packages(skills: &[pioneer_skills::ResolvedSkill]) -> Result<BTreeMap<String, String>> {
    let mut captured = BTreeMap::new();
    for skill in skills {
        let files = pioneer_skills::snapshot_skill_package(
            std::path::Path::new(&skill.definition.identity.skill_dir),
            256,
            256 * 1024,
        )?;
        let mut digest = Sha256::new();
        for file in files {
            digest.update((file.relative_path.len() as u64).to_be_bytes());
            digest.update(file.relative_path.as_bytes());
            digest.update((file.bytes.len() as u64).to_be_bytes());
            digest.update(&file.bytes);
        }
        captured.insert(skill.skill_id.to_string(), hex::encode(digest.finalize()));
    }
    Ok(captured)
}

impl AcpSkillTooling {
    pub(crate) async fn prepare(
        skills: Vec<pioneer_skills::ResolvedSkill>,
        config: pioneer_agent::SkillsLoopConfig,
    ) -> Result<Arc<Self>> {
        let to_capture = skills.clone();
        let captured = tokio::task::spawn_blocking(move || packages(&to_capture)).await??;
        let plan = pioneer_agent::resolved_skill_runtime_plan(&skills, &config);
        let materialized = pioneer_agent::materialize_skill_tooling(&plan, &config);
        let fingerprint = hex::encode(Sha256::digest(serde_json::to_vec(&serde_json::json!({
            "packages":captured,"descriptors":plan.tools,
            "bindings":super::skills::cli_runtime_turn_skill_bindings(&skills),
        }))?));
        Ok(Arc::new(Self {
            skills,
            plan,
            config,
            materialized,
            packages: captured,
            fingerprint,
        }))
    }

    pub(crate) fn has_tools(&self) -> bool {
        self.materialized
            .bundles
            .iter()
            .any(|bundle| !bundle.specs.is_empty())
    }

    pub(crate) async fn revalidate_packages(self: &Arc<Self>) -> Result<()> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            ensure!(
                packages(&this.skills)? == this.packages,
                "Selected skill package changed after admission"
            );
            Ok(())
        })
        .await??;
        Ok(())
    }

    pub(crate) fn extend_projection(
        &self,
        projection: &mut ResolvedMcpTurnProjection,
        limits: McpProjectionLimits,
    ) -> Result<()> {
        for spec in self
            .materialized
            .bundles
            .iter()
            .flat_map(|bundle| &bundle.specs)
        {
            projection.tools.push(ResolvedMcpTurnTool {
                canonical_callable_name: spec.spec.name.clone(),
                workspace_id: projection.workspace_id.clone(),
                server_installation_id: SKILL_SERVER_ID.into(),
                server_name: SKILL_SERVER_NAME.into(),
                raw_tool_name: spec.spec.name.clone(),
                description: Some(spec.spec.description.clone()),
                input_schema: spec.spec.parameters.clone(),
                annotations: None,
                timeout_ms: 120_000,
                catalog_version: self.fingerprint.clone(),
                installation_fingerprint: self.fingerprint.clone(),
                schema_fingerprint: String::new(),
                runtime_generation: 1,
                selection_reason: McpSelectionReason::ExplicitTool,
                capability_id: None,
            });
        }
        // Skill tools consume the same bounded facade allowance as external
        // tools. Only the existing reserved HTML definitions have extra slots.
        let html_bytes = projection
            .tools
            .iter()
            .filter(|tool| tool.server_installation_id == pioneer_tools::html::HTML_SERVER_ID)
            .try_fold(0usize, |total, tool| -> Result<usize> {
                let (_, _, bytes) =
                    crate::turn_mcp::projection::canonical_schema_identity(&tool.input_schema)
                        .map_err(anyhow::Error::msg)?;
                Ok(total.saturating_add(bytes.len()))
            })?;
        let limits = McpProjectionLimits {
            max_tools: limits.max_tools + projection.html_tool_count(),
            max_total_schema_bytes: limits.max_total_schema_bytes.saturating_add(html_bytes),
            ..limits
        };
        projection.finalize_identity(limits)?;
        Ok(())
    }

    pub(crate) async fn build_runtime(
        self: &Arc<Self>,
        workspace: &str,
        thread: &str,
        turn: &str,
        security: &pioneer_protocol::TurnExecutionSecuritySnapshot,
        broker: Arc<dyn pioneer_tools::PermissionApprovalBroker>,
        mut extensions: Vec<pioneer_tools::ToolExtensionBundle>,
        service: Arc<McpService>,
    ) -> Result<pioneer_tools::ToolCallRuntime> {
        extensions.extend(self.materialized.bundles.clone());
        let tools = pioneer_tools::build_tools_with_environment_and_security_snapshot(
            &security.sandbox.cwd,
            turn,
            pioneer_tools::PermissionEvaluationContext::for_turn(
                workspace.to_owned(),
                thread.to_owned(),
                turn.to_owned(),
                security.permission_profile.clone(),
            ),
            Default::default(),
            Default::default(),
            extensions,
            BTreeMap::new(),
            Some(security.clone()),
        )
        .map_err(|error| anyhow::anyhow!("Cannot materialize selected skill tools: {error:?}"))?
        .with_permission_approval_broker(Arc::new(SkillApprovalGuard {
            delegate: broker,
            tooling: Arc::downgrade(self),
            service,
            thread: thread.into(),
            turn: turn.into(),
        }));
        // Function proxies bind to the same shared router/runtime as in the API
        // loop; otherwise an apparently offered proxy would be unusable.
        self.materialized
            .bind_function_proxy_runtime(tools.router.clone(), tools.runtime.clone())
            .await;
        Ok(tools.runtime)
    }

    pub(crate) async fn clear(&self) {
        self.materialized.clear_function_proxy_runtime().await;
    }

    pub(crate) fn recheck(&self, tool: &str) -> Result<()> {
        if let Some(descriptor) = self
            .plan
            .tools
            .iter()
            .find(|descriptor| descriptor.canonical_tool_name == tool)
        {
            let checked = pioneer_skills::recheck_runtime_tool_execution(
                descriptor,
                &pioneer_agent::skill_runtime_recheck_policy(&self.config),
            );
            ensure!(checked.allowed, "{}", checked.message);
        } else {
            ensure!(
                self.materialized
                    .bundles
                    .iter()
                    .flat_map(|bundle| &bundle.specs)
                    .any(|spec| spec.spec.name == tool),
                "Unbound skill callable"
            );
        }
        Ok(())
    }

    fn identity(
        &self,
        workspace: &str,
        binding: &pioneer_crud::TurnMcpBindingRecord,
    ) -> Result<CurrentMcpToolIdentity> {
        ensure!(
            binding.server_name == SKILL_SERVER_NAME
                && binding.fingerprint == self.fingerprint
                && binding.catalog_version == self.fingerprint,
            "Stale skill projection"
        );
        let spec = self
            .materialized
            .bundles
            .iter()
            .flat_map(|bundle| &bundle.specs)
            .find(|spec| spec.spec.name == binding.raw_tool_name)
            .context("Unbound skill callable")?;
        let (canonical_schema, canonical_schema_fingerprint, _) =
            crate::turn_mcp::projection::canonical_schema_identity(&spec.spec.parameters)
                .map_err(anyhow::Error::msg)?;
        let (annotations_json, annotations_digest) =
            crate::turn_mcp::projection::canonical_annotations_identity(&Default::default())
                .map_err(anyhow::Error::msg)?;
        ensure!(!workspace.is_empty(), "Missing workspace");
        Ok(CurrentMcpToolIdentity {
            server_installation_id: SKILL_SERVER_ID.into(),
            server_name: SKILL_SERVER_NAME.into(),
            raw_tool_name: spec.spec.name.clone(),
            description: Some(spec.spec.description.clone()),
            catalog_version: self.fingerprint.clone(),
            installation_fingerprint: self.fingerprint.clone(),
            canonical_schema_fingerprint,
            canonical_schema,
            annotations_json,
            annotations_digest,
            effective_timeout_ms: 120_000,
            runtime_generation: 1,
        })
    }
}

pub(crate) struct AcpSkillRuntimeView {
    pub(crate) tooling: Arc<AcpSkillTooling>,
    pub(crate) delegate: Arc<McpService>,
}
#[async_trait]
impl TurnMcpRuntimeView for AcpSkillRuntimeView {
    async fn current_tool_identity(
        &self,
        workspace: &str,
        binding: &pioneer_crud::TurnMcpBindingRecord,
    ) -> Result<CurrentMcpToolIdentity, TurnMcpInvocationError> {
        if binding.server_installation_id != SKILL_SERVER_ID {
            return self
                .delegate
                .current_tool_identity(workspace, binding)
                .await;
        }
        self.tooling.revalidate_packages().await.map_err(|error| {
            TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::ToolDrift, error.to_string())
        })?;
        self.tooling.identity(workspace, binding).map_err(|error| {
            TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::ToolDrift, error.to_string())
        })
    }
}

// A function proxy's MCP leaf still enters the very same invoker with its
// captured CLI origin. Tool names never substitute for the frozen binding.
struct NestedMcpExecutor {
    service: Arc<McpService>,
    instance: super::session_instance::CliSessionInstanceId,
    thread: String,
    turn: String,
}
#[async_trait]
impl pioneer_tools::McpToolExecutor for NestedMcpExecutor {
    async fn call_mcp_tool(
        &self,
        request: pioneer_tools::McpToolCallRequest,
        _trace: pioneer_tools::ToolEventTrace,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<pioneer_tools::McpToolCallOutput, pioneer_tools::ToolError> {
        use crate::turn_mcp::invoker::{
            TurnMcpInvocation, TurnMcpInvocationOrigin, TurnMcpInvoker,
        };
        if request.workspace_id != self.instance.key().workspace_id || request.turn_id != self.turn
        {
            return Err(pioneer_tools::ToolError::Rejected(
                "MCP proxy scope mismatch".into(),
            ));
        }
        let result = self
            .service
            .turn_mcp_invoker()
            .invoke(
                TurnMcpInvocation {
                    workspace_id: request.workspace_id,
                    thread_id: self.thread.clone(),
                    turn_id: self.turn.clone(),
                    runtime_id: Some(self.instance.key().runtime_id.clone()),
                    session_generation: Some(self.instance.generation()),
                    provider_call_id: request.call_id,
                    canonical_callable_name: request.callable_name,
                    arguments: request.arguments,
                    origin: TurnMcpInvocationOrigin::CliFacade,
                },
                cancellation,
            )
            .await
            .map_err(|error| pioneer_tools::ToolError::Rejected(error.to_string()))?;
        Ok(pioneer_tools::McpToolCallOutput {
            content: result.content,
            structured_content: result.structured_content,
            is_error: result.is_error,
            duration_ms: result.duration_ms,
            meta: result.meta,
        })
    }
}
impl McpService {
    pub(crate) async fn acp_skill_extensions(
        &self,
        instance: &super::session_instance::CliSessionInstanceId,
        thread: &str,
        turn: &str,
    ) -> Result<Vec<pioneer_tools::ToolExtensionBundle>> {
        let processor = self
            .inner
            .html_processor
            .read()
            .expect("processor bridge")
            .upgrade()
            .context("Gateway tool owner unavailable")?;
        let mut bundles = processor
            .materialize_acp_skill_extensions(pioneer_agent::TurnToolContext {
                workspace_id: instance.key().workspace_id.clone(),
                thread_id: thread.into(),
                turn_id: turn.into(),
            })
            .await?;
        let mut descriptors = Vec::new();
        for binding in self.inner.crud_store.list_turn_mcp_bindings(turn).await? {
            if binding.server_installation_id == SKILL_SERVER_ID
                || binding.server_installation_id == pioneer_tools::html::HTML_SERVER_ID
            {
                continue;
            }
            let identity = self
                .current_tool_identity(&instance.key().workspace_id, &binding)
                .await?;
            crate::turn_mcp::invoker::validate_frozen_identity(&binding, &identity)?;
            descriptors.push(pioneer_tools::McpDynamicToolDescriptor {
                callable_name: binding.canonical_callable_name,
                workspace_id: instance.key().workspace_id.clone(),
                server_id: binding.server_installation_id,
                server_name: binding.server_name,
                raw_tool_name: binding.raw_tool_name,
                catalog_version: binding.catalog_version,
                fingerprint: binding.fingerprint,
                snapshot_version: identity.runtime_generation,
                description: identity.description.unwrap_or_default(),
                parameters: identity.canonical_schema,
                annotations: serde_json::from_str(&binding.annotations_json)?,
                timeout_ms: Some(identity.effective_timeout_ms),
                max_arguments_bytes: 1024 * 1024,
                selection_reason: binding.selection_reason,
                capability_id: binding.capability_id,
            });
        }
        let materialized = pioneer_tools::materialize_mcp_runtime_tools(
            &descriptors,
            Arc::new(NestedMcpExecutor {
                service: Arc::new(self.clone()),
                instance: instance.clone(),
                thread: thread.into(),
                turn: turn.into(),
            }),
        );
        ensure!(
            materialized.excluded_tools.is_empty(),
            "Frozen MCP proxy tooling could not be materialized"
        );
        bundles.extend(materialized.bundles);
        Ok(bundles)
    }
}

/// Repeat package and live skill authority checks at the existing shared
/// pre-side-effect hook, including after consent and nested proxy dispatch.
struct SkillApprovalGuard {
    delegate: Arc<dyn pioneer_tools::PermissionApprovalBroker>,
    tooling: std::sync::Weak<AcpSkillTooling>,
    service: Arc<McpService>,
    thread: String,
    turn: String,
}
#[async_trait]
impl pioneer_tools::PermissionApprovalBroker for SkillApprovalGuard {
    async fn revalidate_permission_context(
        &self,
        context: &pioneer_tools::PermissionEvaluationContext,
        invocation: &pioneer_tools::ToolInvocation,
    ) -> Result<pioneer_tools::PermissionEvaluationContext, String> {
        let current = self
            .delegate
            .revalidate_permission_context(context, invocation)
            .await?;
        let tooling = self.tooling.upgrade().ok_or("ACP skill runtime retired")?;
        tooling
            .revalidate_packages()
            .await
            .map_err(|error| error.to_string())?;
        for descriptor in &tooling.plan.tools {
            tooling
                .recheck(&descriptor.canonical_tool_name)
                .map_err(|error| error.to_string())?;
        }
        let processor = self
            .service
            .inner
            .html_processor
            .read()
            .map_err(|_| "Tool owner unavailable")?
            .upgrade()
            .ok_or("Tool owner unavailable")?;
        processor
            .revalidate_persisted_turn_skill_projection(&self.thread, &self.turn)
            .await
            .map_err(|error| error.to_string())?;
        Ok(current)
    }
    async fn request_approval(
        &self,
        context: &pioneer_tools::PermissionEvaluationContext,
        invocation: &pioneer_tools::ToolInvocation,
        intent: &pioneer_tools::PermissionIntent,
        key: &pioneer_tools::PermissionRequestKey,
        reason: pioneer_tools::PermissionDecisionReason,
    ) -> pioneer_tools::PermissionApprovalResolution {
        self.delegate
            .request_approval(context, invocation, intent, key, reason)
            .await
    }
}
