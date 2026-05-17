use super::*;

impl MessageProcessor {
    pub(super) async fn provider_list(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ProviderListParams,
    ) {
        let Some(workspace_id) = self
            .validate_provider_workspace(
                connection_id,
                request_id.clone(),
                methods::PROVIDER_LIST,
                params.workspace_id,
            )
            .await
        else {
            return;
        };

        let provider_names = match self
            .gateway_secrets
            .list_configured_workspace_provider_names(workspace_id.as_str())
        {
            Ok(provider_names) => provider_names,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to list provider api keys: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        let result = ProviderListResponse {
            providers: provider_names
                .into_iter()
                .map(|name| ProviderSummary { name })
                .collect(),
        };

        let response = match JsonRpcResponse::from_result(request_id, &result) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send provider/list response"
            );
        }
    }

    pub(super) async fn provider_list_models(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ProviderListModelsParams,
    ) {
        let Some(workspace_id) = self
            .validate_provider_workspace(
                connection_id,
                request_id.clone(),
                methods::PROVIDER_MODELS_LIST,
                params.workspace_id.clone(),
            )
            .await
        else {
            return;
        };

        if params.provider.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `provider` is required",
                        methods::PROVIDER_MODELS_LIST
                    ),
                ),
            )
            .await;
            return;
        }

        let provider = match self
            .provider_registry
            .get_or_create_for_workspace(workspace_id.as_str(), &params.provider)
        {
            Ok(p) => p,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to create provider `{}`: {error:#}", params.provider),
                    ),
                )
                .await;
                return;
            }
        };

        match provider.list_models().await {
            Ok(models) => {
                let protocol_models = models
                    .into_iter()
                    .map(|m| ProviderModelInfo {
                        id: m.id,
                        name: m.name,
                        description: m.description,
                        created: m.created,
                        provider: m.provider,
                        owned_by: m.owned_by,
                        limits: ProviderModelLimits {
                            max_input_tokens: m.limits.max_input_tokens,
                            max_output_tokens: m.limits.max_output_tokens,
                            context_window: m.limits.context_window,
                        },
                        capabilities: ProviderModelCapabilities {
                            vision: m.capabilities.vision,
                            tool_calling: m.capabilities.tool_calling,
                            json_output: m.capabilities.json_output,
                            streaming: m.capabilities.streaming,
                            thinking: m.capabilities.thinking,
                            fine_tuning: m.capabilities.fine_tuning,
                            input_modalities: m.capabilities.input_modalities,
                            output_modalities: m.capabilities.output_modalities,
                        },
                        pricing: m.pricing.map(|p| ProviderModelPricing {
                            input_token: p.input_token,
                            output_token: p.output_token,
                            image: p.image,
                            request: p.request,
                        }),
                        active: m.active,
                        family: m.family,
                        lifecycle_status: m.lifecycle_status,
                    })
                    .collect();

                let result = ProviderListModelsResponse {
                    provider: params.provider,
                    models: protocol_models,
                };

                let response = match JsonRpcResponse::from_result(request_id, &result) {
                    Ok(response) => response,
                    Err(error) => {
                        self.send_error(
                            connection_id,
                            JsonRpcErrorResponse::new(
                                None,
                                INVALID_REQUEST_CODE,
                                format!("failed to encode response: {error}"),
                            ),
                        )
                        .await;
                        return;
                    }
                };

                if let Err(error) = self.send_json(connection_id, &response).await {
                    warn!(
                        connection_id,
                        error = %format!("{error:#}"),
                        "failed to send provider/list_models response"
                    );
                }
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!(
                            "failed to list models for provider `{}`: {error:#}",
                            params.provider
                        ),
                    ),
                )
                .await;
            }
        }
    }

    pub(super) async fn provider_set_api_key(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ProviderSetApiKeyParams,
    ) {
        let Some(workspace_id) = self
            .validate_provider_workspace(
                connection_id,
                request_id.clone(),
                methods::PROVIDER_SET_API_KEY,
                params.workspace_id.clone(),
            )
            .await
        else {
            return;
        };

        if params.provider.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `provider` is required",
                        methods::PROVIDER_SET_API_KEY
                    ),
                ),
            )
            .await;
            return;
        }

        if params.api_key.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `api_key` must not be empty",
                        methods::PROVIDER_SET_API_KEY
                    ),
                ),
            )
            .await;
            return;
        }

        let requested_provider = params.provider.as_str();
        if let Err(error) = self
            .gateway_secrets
            .normalize_provider_name(requested_provider)
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: invalid `provider`: {error:#}",
                        methods::PROVIDER_SET_API_KEY
                    ),
                ),
            )
            .await;
            return;
        }

        let raw_provider = params.provider;

        let normalized_provider = match self.gateway_secrets.set_workspace_provider_api_key(
            workspace_id.as_str(),
            &raw_provider,
            params.api_key.as_str(),
        ) {
            Ok(normalized_provider) => normalized_provider,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to save provider api key: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        self.provider_registry.invalidate(&raw_provider);
        if raw_provider != normalized_provider {
            self.provider_registry.invalidate(&normalized_provider);
        }

        let response = ProviderSetApiKeyResponse {
            provider: normalized_provider,
            updated: true,
        };
        let response = match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send provider/set_api_key response"
            );
        }
    }

    pub(super) async fn provider_delete_api_key(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        params: ProviderDeleteApiKeyParams,
    ) {
        let Some(workspace_id) = self
            .validate_provider_workspace(
                connection_id,
                request_id.clone(),
                methods::PROVIDER_DELETE_API_KEY,
                params.workspace_id.clone(),
            )
            .await
        else {
            return;
        };

        if params.provider.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `provider` is required",
                        methods::PROVIDER_DELETE_API_KEY
                    ),
                ),
            )
            .await;
            return;
        }

        if let Err(error) = self
            .gateway_secrets
            .normalize_provider_name(&params.provider)
        {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: invalid `provider`: {error:#}",
                        methods::PROVIDER_DELETE_API_KEY
                    ),
                ),
            )
            .await;
            return;
        }

        let raw_provider = params.provider;

        let (normalized_provider, deleted) = match self
            .gateway_secrets
            .delete_workspace_provider_api_key(workspace_id.as_str(), &raw_provider)
        {
            Ok(result) => result,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_REQUEST_CODE,
                        format!("failed to delete provider api key: {error:#}"),
                    ),
                )
                .await;
                return;
            }
        };

        if deleted {
            self.provider_registry.invalidate(&raw_provider);
            if raw_provider != normalized_provider {
                self.provider_registry.invalidate(&normalized_provider);
            }
        }

        let response = ProviderDeleteApiKeyResponse {
            provider: normalized_provider,
            deleted,
        };
        let response = match JsonRpcResponse::from_result(request_id, &response) {
            Ok(response) => response,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        format!("failed to encode response: {error}"),
                    ),
                )
                .await;
                return;
            }
        };

        if let Err(error) = self.send_json(connection_id, &response).await {
            warn!(
                connection_id,
                error = %format!("{error:#}"),
                "failed to send provider/delete_api_key response"
            );
        }
    }

    /// Load conversation history for a thread with progressive summarization.
    ///
    /// Strategy (ChatGPT-style):
    /// 1. Load all completed turns + existing summary
    /// 2. Count total tokens
    /// 3. If < 80% of budget — return everything as-is (maximum context fidelity)
    /// 4. If >= 80% — compress ALL turns into a ~10% summary via LLM, notify UI,
    ///    then return the compressed summary (conversation continues growing from there)
    pub(super) async fn load_conversation_history(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Vec<ChatMessage> {
        use crate::tokenizer::count_tokens;

        const MAX_TURNS: usize = 200;
        const MESSAGE_OVERHEAD: usize = 4;
        const COMPRESSION_THRESHOLD_BPS: u16 = 8_000;
        const COMPRESSION_TARGET_BPS: u16 = 1_000;
        const BPS_DENOMINATOR: usize = 10_000;

        let budget = self.context_budget.history_budget();

        let (existing_summary, existing_summary_turn_count) =
            match self.crud_store.get_thread_summary(thread_id).await {
                Ok(Some((text, turn_count))) => (Some(text), Some(turn_count)),
                Ok(None) => (None, None),
                Err(error) => {
                    warn!(
                        thread_id,
                        error = %format!("{error:#}"),
                        "failed to load existing thread summary"
                    );
                    (None, None)
                }
            };

        let entries = match self
            .crud_store
            .get_thread_conversation_history(thread_id, MAX_TURNS)
            .await
        {
            Ok(entries) => entries,
            Err(error) => {
                warn!(
                    thread_id,
                    error = %format!("{error:#}"),
                    "failed to load conversation history, proceeding without it"
                );
                return Vec::new();
            }
        };

        let mut total_tokens: usize = 0;

        if let Some(ref summary_text) = existing_summary {
            total_tokens +=
                count_tokens(&format!("Summary of earlier conversation:\n{summary_text}"))
                    + MESSAGE_OVERHEAD;
        }

        let entry_tokens: Vec<usize> = entries
            .iter()
            .map(|entry| {
                let user_t = entry
                    .user_text
                    .as_deref()
                    .map(|t| count_tokens(t) + MESSAGE_OVERHEAD)
                    .unwrap_or(0);
                let assistant_t = entry
                    .assistant_text
                    .as_deref()
                    .map(|t| count_tokens(t) + MESSAGE_OVERHEAD)
                    .unwrap_or(0);
                user_t + assistant_t
            })
            .collect();

        let turns_tokens: usize = entry_tokens.iter().sum();
        total_tokens += turns_tokens;

        let threshold =
            budget.saturating_mul(usize::from(COMPRESSION_THRESHOLD_BPS)) / BPS_DENOMINATOR;
        let target_tokens =
            budget.saturating_mul(usize::from(COMPRESSION_TARGET_BPS)) / BPS_DENOMINATOR;

        debug!(
            thread_id,
            total_tokens,
            budget,
            threshold,
            turn_count = entries.len(),
            "context token count"
        );

        if total_tokens < threshold {
            return self.build_messages_from_entries(existing_summary.as_deref(), &entries);
        }

        info!(
            thread_id,
            total_tokens, threshold, "context threshold reached, compressing conversation"
        );

        let hook_runtime = self.hook_runtime.read().await.clone();
        if let Some(runtime) = hook_runtime.as_ref() {
            let thread = match self.crud_store.get_thread_by_id(thread_id).await {
                Ok(Some(thread)) => Some(thread),
                Ok(None) => {
                    warn!(
                        thread_id,
                        "thread not found while preparing pre-compaction hook input"
                    );
                    None
                }
                Err(error) => {
                    warn!(
                        thread_id,
                        error = %format!("{error:#}"),
                        "failed to load thread while preparing pre-compaction hook input"
                    );
                    None
                }
            };

            if let Some(thread) = thread {
                let compaction_id = format!("cmp_{}", pioneer_protocol::generate_id(21));
                let dispatch = match hooks::build_pre_compaction_hook_dispatch(
                    hooks::PreCompactionHookInputParts {
                        workspace_id: thread.workspace_id.as_str(),
                        thread_id,
                        turn_id,
                        compaction_id,
                        loaded_completed_turn_count: entries.len(),
                        source_entry_count: entries.len(),
                        max_loaded_turns: MAX_TURNS,
                        existing_summary_turn_count,
                        max_context_tokens: self.context_budget.max_context_tokens,
                        response_reserve_tokens: self.context_budget.response_reserve_tokens,
                        history_budget_tokens: budget,
                        estimated_current_tokens: total_tokens,
                        compression_threshold_tokens: threshold,
                        target_summary_tokens: target_tokens,
                        compression_threshold_bps: COMPRESSION_THRESHOLD_BPS,
                        compression_target_bps: COMPRESSION_TARGET_BPS,
                        existing_summary: existing_summary.as_deref(),
                    },
                ) {
                    Ok(dispatch) => Some(dispatch),
                    Err(error) => {
                        warn!(
                            thread_id,
                            turn_id,
                            error = %error,
                            "failed to build typed pre-compaction hook context"
                        );
                        None
                    }
                };

                if let Some(dispatch) = dispatch {
                    match hooks::run_pre_compaction_hook_phase(Some(runtime), dispatch).await {
                        Ok(outcome) => {
                            if !outcome.diagnostics.is_empty() || !outcome.runs.is_empty() {
                                debug!(
                                    thread_id,
                                    turn_id,
                                    diagnostic_count = outcome.diagnostics.len(),
                                    run_count = outcome.runs.len(),
                                    "pre-compaction hook phase completed"
                                );
                            }
                        }
                        Err(error) => {
                            warn!(
                                thread_id,
                                turn_id,
                                error = %error.runtime_error,
                                message = error.safe_message.as_str(),
                                "pre-compaction hook phase blocked context compression"
                            );
                            return self.build_messages_truncated(
                                existing_summary.as_deref(),
                                &entries,
                                &entry_tokens,
                                budget,
                            );
                        }
                    }
                }
            }
        }

        let compressing_notification = ContextCompressingNotification {
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            message: "Compressing conversation history...".to_owned(),
        };
        self.send_notification_to_thread_subscribers(
            thread_id,
            events::CONTEXT_COMPRESSING,
            &compressing_notification,
        )
        .await;

        match summary::compress_context(
            &self.crud_store,
            &self.provider_registry,
            thread_id,
            &entries,
            existing_summary.as_deref(),
            target_tokens,
            &self.summary_config,
        )
        .await
        {
            Ok(compressed_summary) => {
                let compressed_tokens = count_tokens(&compressed_summary);

                let compressed_notification = ContextCompressedNotification {
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    compressed_tokens,
                };
                self.send_notification_to_thread_subscribers(
                    thread_id,
                    events::CONTEXT_COMPRESSED,
                    &compressed_notification,
                )
                .await;

                debug!(
                    thread_id,
                    compressed_tokens,
                    original_tokens = total_tokens,
                    "context compressed successfully"
                );

                // Return just the summary — conversation grows from here
                vec![ChatMessage::system(format!(
                    "Summary of earlier conversation:\n{compressed_summary}"
                ))]
            }
            Err(error) => {
                warn!(
                    thread_id,
                    error = %format!("{error:#}"),
                    "context compression failed, falling back to truncation"
                );

                // Fallback: return as many recent turns as fit in the budget
                self.build_messages_truncated(
                    existing_summary.as_deref(),
                    &entries,
                    &entry_tokens,
                    budget,
                )
            }
        }
    }

    /// Build ChatMessage list from summary + all entries (no truncation).
    pub(super) fn build_messages_from_entries(
        &self,
        existing_summary: Option<&str>,
        entries: &[ConversationEntry],
    ) -> Vec<ChatMessage> {
        let mut messages = Vec::with_capacity(1 + entries.len() * 2);

        if let Some(summary_text) = existing_summary {
            messages.push(ChatMessage::system(format!(
                "Summary of earlier conversation:\n{summary_text}"
            )));
        }

        for entry in entries {
            if let Some(user_text) = &entry.user_text {
                messages.push(ChatMessage::user(user_text.clone()));
            }
            if let Some(assistant_text) = &entry.assistant_text {
                messages.push(ChatMessage::assistant(assistant_text.clone()));
            }
        }

        messages
    }

    /// Fallback: fit as many recent turns as possible within the token budget.
    pub(super) fn build_messages_truncated(
        &self,
        existing_summary: Option<&str>,
        entries: &[ConversationEntry],
        entry_tokens: &[usize],
        budget: usize,
    ) -> Vec<ChatMessage> {
        const MESSAGE_OVERHEAD: usize = 4;

        let mut used_tokens: usize = 0;

        let summary_msg = if let Some(summary_text) = existing_summary {
            let text = format!("Summary of earlier conversation:\n{summary_text}");
            let tokens = count_tokens(&text) + MESSAGE_OVERHEAD;
            if tokens < budget {
                used_tokens += tokens;
                Some(ChatMessage::system(text))
            } else {
                None
            }
        } else {
            None
        };

        let mut selected_indices: Vec<usize> = Vec::new();
        for i in (0..entries.len()).rev() {
            if used_tokens + entry_tokens[i] <= budget {
                used_tokens += entry_tokens[i];
                selected_indices.push(i);
            } else {
                break;
            }
        }
        selected_indices.reverse();

        let mut messages = Vec::with_capacity(1 + selected_indices.len() * 2);
        if let Some(summary) = summary_msg {
            messages.push(summary);
        }
        for i in selected_indices {
            if let Some(user_text) = &entries[i].user_text {
                messages.push(ChatMessage::user(user_text.clone()));
            }
            if let Some(assistant_text) = &entries[i].assistant_text {
                messages.push(ChatMessage::assistant(assistant_text.clone()));
            }
        }

        messages
    }

    async fn validate_provider_workspace(
        &self,
        connection_id: ConnectionId,
        request_id: RequestId,
        method: &str,
        workspace_id: String,
    ) -> Option<String> {
        let workspace_id = match self
            .workspace_manager
            .validate_workspace_id(workspace_id.as_str())
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!("failed to validate workspace for `{method}`: {error}"),
                    ),
                )
                .await;
                return None;
            }
        };

        self.session_manager
            .set_connection_workspace(connection_id, Some(workspace_id.clone()))
            .await;
        Some(workspace_id)
    }
}
