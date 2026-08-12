use super::*;
use crate::authorization::AuthorizationExternalError;

impl MessageProcessor {
    pub(super) async fn provider_list(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ProviderListParams,
    ) {
        let connection_id = request_context.connection_id();
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
        let member = request_context.principal().kind == pioneer_protocol::PrincipalKind::User;

        let provider_names = match self
            .gateway_secrets
            .list_configured_workspace_provider_names(workspace_id.as_str())
        {
            Ok(provider_names) => provider_names,
            Err(error) => {
                let message = if member {
                    "provider catalog is unavailable".to_owned()
                } else {
                    format!("failed to list provider api keys: {error:#}")
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                )
                .await;
                return;
            }
        };
        let provider_proxies = match self
            .gateway_secrets
            .list_workspace_provider_proxies(workspace_id.as_str())
        {
            Ok(provider_proxies) => provider_proxies,
            Err(error) => {
                let message = if member {
                    "provider catalog is unavailable".to_owned()
                } else {
                    format!("failed to list provider proxies: {error:#}")
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                )
                .await;
                return;
            }
        };

        let mut provider_configs = std::collections::BTreeMap::new();
        for name in provider_names {
            // `api_key_configured` is the operational availability bit used by
            // every model selector. It does not contain the key itself and
            // must remain true for a Member, otherwise the shared clients
            // correctly filter the provider out as unusable. Secret-bearing
            // proxy configuration remains redacted below.
            provider_configs.insert(name, (true, None));
        }
        for (name, proxy_url) in provider_proxies {
            provider_configs
                .entry(name)
                .and_modify(|(_, existing_proxy)| *existing_proxy = Some(proxy_url.clone()))
                .or_insert((false, Some(proxy_url)));
        }

        // Local is built in, so there is no workspace secret or proxy from which to discover it.
        provider_configs
            .entry("local".to_owned())
            .or_insert((false, None));

        let providers = provider_configs
            .into_iter()
            .map(|(name, (api_key_configured, proxy_url))| {
                let operationally_configured =
                    api_key_configured || proxy_url.is_some() || name == "local";
                let capabilities = self
                    .provider_registry
                    .get_or_create_for_workspace(workspace_id.as_str(), name.as_str())
                    .map(|provider| {
                        let capabilities = provider.capabilities();
                        ProviderSummaryCapabilities {
                            embeddings: capabilities.embeddings,
                            transcription: capabilities.transcription,
                            self_improvement:
                                crate::self_improvement::settings::model_provider_is_eligible(
                                    name.as_str(),
                                    &capabilities,
                                ),
                        }
                    })
                    .unwrap_or_default();

                ProviderSummary {
                    name,
                    capabilities,
                    // For Members this is deliberately an availability bit:
                    // a workspace proxy can provide the credential even when
                    // no local API key exists. Management clients retain the
                    // literal API-key state and the configured proxy URL.
                    api_key_configured: if member {
                        operationally_configured
                    } else {
                        api_key_configured
                    },
                    proxy_url: if member { None } else { proxy_url },
                }
            })
            .collect::<Vec<_>>();

        let result = ProviderListResponse { providers };

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
        request_context: &RequestContext,
        request_id: RequestId,
        params: ProviderListModelsParams,
    ) {
        let connection_id = request_context.connection_id();
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
        let member = request_context.principal().kind == pioneer_protocol::PrincipalKind::User;

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
        if member
            && !self.member_provider_is_configured(workspace_id.as_str(), params.provider.as_str())
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
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
                let message = if member {
                    "provider model catalog is unavailable".to_owned()
                } else {
                    format!("failed to create provider `{}`: {error:#}", params.provider)
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                )
                .await;
                return;
            }
        };

        match provider.list_models().await {
            Ok(models) => {
                let protocol_models = models
                    .into_iter()
                    .map(provider_model_info_to_protocol)
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
                let message = if member {
                    "provider model catalog is unavailable".to_owned()
                } else {
                    format!(
                        "failed to list models for provider `{}`: {error:#}",
                        params.provider
                    )
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                )
                .await;
            }
        }
    }

    pub(super) async fn provider_list_embedding_models(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ProviderListModelsParams,
    ) {
        let connection_id = request_context.connection_id();
        let Some(workspace_id) = self
            .validate_provider_workspace(
                connection_id,
                request_id.clone(),
                methods::PROVIDER_EMBEDDING_MODELS_LIST,
                params.workspace_id.clone(),
            )
            .await
        else {
            return;
        };
        let member = request_context.principal().kind == pioneer_protocol::PrincipalKind::User;

        if params.provider.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `provider` is required",
                        methods::PROVIDER_EMBEDDING_MODELS_LIST
                    ),
                ),
            )
            .await;
            return;
        }
        if member
            && !self.member_provider_is_configured(workspace_id.as_str(), params.provider.as_str())
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
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
                let message = if member {
                    "provider model catalog is unavailable".to_owned()
                } else {
                    format!("failed to create provider `{}`: {error:#}", params.provider)
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                )
                .await;
                return;
            }
        };

        match provider.list_embedding_models().await {
            Ok(models) => {
                let result = ProviderListModelsResponse {
                    provider: params.provider,
                    models: models
                        .into_iter()
                        .map(provider_model_info_to_protocol)
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
                        "failed to send provider/list_embedding_models response"
                    );
                }
            }
            Err(error) => {
                let message = if member {
                    "provider model catalog is unavailable".to_owned()
                } else {
                    format!(
                        "failed to list embedding models for provider `{}`: {error:#}",
                        params.provider
                    )
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                )
                .await;
            }
        }
    }

    pub(super) async fn provider_list_transcription_models(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ProviderListModelsParams,
    ) {
        let connection_id = request_context.connection_id();
        let Some(workspace_id) = self
            .validate_provider_workspace(
                connection_id,
                request_id.clone(),
                methods::PROVIDER_TRANSCRIPTION_MODELS_LIST,
                params.workspace_id.clone(),
            )
            .await
        else {
            return;
        };
        let member = request_context.principal().kind == pioneer_protocol::PrincipalKind::User;

        if params.provider.trim().is_empty() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `provider` is required",
                        methods::PROVIDER_TRANSCRIPTION_MODELS_LIST
                    ),
                ),
            )
            .await;
            return;
        }
        if member
            && !self.member_provider_is_configured(workspace_id.as_str(), params.provider.as_str())
        {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
            )
            .await;
            return;
        }

        let provider = match self
            .provider_registry
            .get_or_create_for_workspace(workspace_id.as_str(), &params.provider)
        {
            Ok(provider) => provider,
            Err(error) => {
                let message = if member {
                    "provider model catalog is unavailable".to_owned()
                } else {
                    format!("failed to create provider `{}`: {error:#}", params.provider)
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                )
                .await;
                return;
            }
        };

        match provider.list_transcription_models().await {
            Ok(models) => {
                let result = ProviderListModelsResponse {
                    provider: params.provider,
                    models: models
                        .into_iter()
                        .map(provider_model_info_to_protocol)
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
                        "failed to send provider/list_transcription_models response"
                    );
                }
            }
            Err(error) => {
                let message = if member {
                    "provider model catalog is unavailable".to_owned()
                } else {
                    format!(
                        "failed to list transcription models for provider `{}`: {error:#}",
                        params.provider
                    )
                };
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(Some(request_id), INVALID_REQUEST_CODE, message),
                )
                .await;
            }
        }
    }

    pub(super) async fn provider_configure(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ProviderConfigureParams,
    ) {
        let connection_id = request_context.connection_id();
        let Some(workspace_id) = self
            .validate_provider_workspace(
                connection_id,
                request_id.clone(),
                methods::PROVIDER_CONFIGURE,
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
                        methods::PROVIDER_CONFIGURE
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
                        methods::PROVIDER_CONFIGURE
                    ),
                ),
            )
            .await;
            return;
        }

        if params.clear_proxy && params.proxy_url.is_some() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `proxy_url` and `clear_proxy` cannot both be set",
                        methods::PROVIDER_CONFIGURE
                    ),
                ),
            )
            .await;
            return;
        }

        let api_key = match params.api_key {
            Some(api_key) => {
                let trimmed = api_key.trim().to_owned();
                if trimmed.is_empty() {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            format!(
                                "invalid params for `{}`: `api_key` must not be empty when provided",
                                methods::PROVIDER_CONFIGURE
                            ),
                        ),
                    )
                    .await;
                    return;
                }
                Some(trimmed)
            }
            None => None,
        };
        let proxy_url = match params.proxy_url {
            Some(proxy_url) => match pioneer_provider::validate_proxy_url(proxy_url.as_str()) {
                Ok(proxy_url) => Some(proxy_url),
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            format!(
                                "invalid params for `{}`: {error:#}",
                                methods::PROVIDER_CONFIGURE
                            ),
                        ),
                    )
                    .await;
                    return;
                }
            },
            None => None,
        };

        let raw_provider = params.provider;
        let mut normalized_provider = match self
            .gateway_secrets
            .normalize_provider_name(raw_provider.as_str())
        {
            Ok(normalized_provider) => normalized_provider,
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        Some(request_id),
                        INVALID_PARAMS_CODE,
                        format!(
                            "invalid params for `{}`: invalid `provider`: {error:#}",
                            methods::PROVIDER_CONFIGURE
                        ),
                    ),
                )
                .await;
                return;
            }
        };
        let mut api_key_updated = false;
        if let Some(api_key) = api_key {
            match self.gateway_secrets.set_workspace_provider_api_key(
                workspace_id.as_str(),
                raw_provider.as_str(),
                api_key.as_str(),
            ) {
                Ok(provider) => {
                    normalized_provider = provider;
                    api_key_updated = true;
                }
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
            }
        }

        let mut proxy_updated = false;
        let mut proxy_deleted = false;
        let mut response_proxy_url = self
            .gateway_secrets
            .get_workspace_provider_proxy(workspace_id.as_str(), raw_provider.as_str())
            .ok()
            .flatten();
        if let Some(proxy_url) = proxy_url {
            match self.gateway_secrets.set_workspace_provider_proxy(
                workspace_id.as_str(),
                raw_provider.as_str(),
                proxy_url.as_str(),
            ) {
                Ok(provider) => {
                    normalized_provider = provider;
                    proxy_updated = true;
                    response_proxy_url = Some(proxy_url);
                }
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to save provider proxy: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            }
        } else if params.clear_proxy {
            match self
                .gateway_secrets
                .delete_workspace_provider_proxy(workspace_id.as_str(), raw_provider.as_str())
            {
                Ok((provider, deleted)) => {
                    normalized_provider = provider;
                    proxy_deleted = deleted;
                    response_proxy_url = None;
                }
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to delete provider proxy: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            }
        }

        if api_key_updated || proxy_updated || proxy_deleted {
            self.provider_registry.invalidate(raw_provider.as_str());
            if raw_provider != normalized_provider {
                self.provider_registry
                    .invalidate(normalized_provider.as_str());
            }
        }

        let response = ProviderConfigureResponse {
            provider: normalized_provider,
            api_key_updated,
            proxy_updated,
            proxy_deleted,
            proxy_url: response_proxy_url,
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
                "failed to send provider/configure response"
            );
        }
    }

    pub(super) async fn provider_set_api_key(
        &self,
        request_context: &RequestContext,
        request_id: RequestId,
        params: ProviderSetApiKeyParams,
    ) {
        let connection_id = request_context.connection_id();
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
        request_context: &RequestContext,
        request_id: RequestId,
        params: ProviderDeleteApiKeyParams,
    ) {
        let connection_id = request_context.connection_id();
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
    #[cfg(test)]
    pub(super) async fn load_conversation_history(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Vec<ChatMessage> {
        let workspace_id = match self.crud_store.get_thread_by_id(thread_id).await {
            Ok(Some(thread)) => Some(thread.workspace_id),
            Ok(None) => None,
            Err(error) => {
                warn!(
                    thread_id,
                    error = %format!("{error:#}"),
                    "failed to load thread workspace for conversation artifact refs"
                );
                None
            }
        };
        self.load_conversation_history_inner(
            workspace_id.as_deref(),
            thread_id,
            thread_id,
            turn_id,
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub(super) async fn load_conversation_history_for_workspace(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Vec<ChatMessage> {
        self.load_conversation_history_inner(
            Some(workspace_id),
            thread_id,
            thread_id,
            turn_id,
            None,
            None,
            None,
            false,
        )
        .await
    }

    pub(super) async fn load_conversation_history_for_workspace_in_execution_excluding_turn(
        &self,
        workspace_id: &str,
        conversation_thread_id: &str,
        execution_thread_id: &str,
        execution_turn_id: &str,
        excluded_conversation_turn_id: Option<&str>,
        fallback_model: Option<&str>,
        fallback_model_provider: Option<&str>,
    ) -> Vec<ChatMessage> {
        self.load_conversation_history_inner(
            Some(workspace_id),
            conversation_thread_id,
            execution_thread_id,
            execution_turn_id,
            fallback_model,
            fallback_model_provider,
            excluded_conversation_turn_id,
            true,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_conversation_history_inner(
        &self,
        workspace_id: Option<&str>,
        conversation_thread_id: &str,
        execution_thread_id: &str,
        execution_turn_id: &str,
        fallback_model: Option<&str>,
        fallback_model_provider: Option<&str>,
        excluded_conversation_turn_id: Option<&str>,
        causally_closed: bool,
    ) -> Vec<ChatMessage> {
        use crate::tokenizer::count_tokens;

        const MAX_TURNS: usize = 200;
        const MESSAGE_OVERHEAD: usize = 4;
        const COMPRESSION_THRESHOLD_BPS: u16 = 8_000;
        const COMPRESSION_TARGET_BPS: u16 = 1_000;
        const BPS_DENOMINATOR: usize = 10_000;

        let budget = self.context_budget.history_budget();

        let (existing_summary, existing_summary_turn_count) = match self
            .crud_store
            .get_thread_summary(conversation_thread_id)
            .await
        {
            Ok(Some((text, turn_count))) => (Some(text), Some(turn_count)),
            Ok(None) => (None, None),
            Err(error) => {
                warn!(
                    thread_id = conversation_thread_id,
                    error = %format!("{error:#}"),
                    "failed to load existing thread summary"
                );
                (None, None)
            }
        };

        let entries_result = if let Some(workspace_id) = workspace_id {
            if causally_closed {
                self.crud_store
                    .get_thread_causally_closed_conversation_history_with_artifacts(
                        workspace_id,
                        conversation_thread_id,
                        MAX_TURNS,
                    )
                    .await
            } else {
                self.crud_store
                    .get_thread_conversation_history_with_artifacts(
                        workspace_id,
                        conversation_thread_id,
                        MAX_TURNS,
                    )
                    .await
            }
        } else if causally_closed {
            self.crud_store
                .get_thread_causally_closed_conversation_history(conversation_thread_id, MAX_TURNS)
                .await
        } else {
            self.crud_store
                .get_thread_conversation_history(conversation_thread_id, MAX_TURNS)
                .await
        };

        let mut entries = match entries_result {
            Ok(entries) => entries,
            Err(error) => {
                warn!(
                    thread_id = conversation_thread_id,
                    error = %format!("{error:#}"),
                    "failed to load conversation history, proceeding without it"
                );
                return Vec::new();
            }
        };
        if let Some(excluded_turn_id) = excluded_conversation_turn_id {
            entries.retain(|entry| entry.turn_id != excluded_turn_id);
        }

        let mut total_tokens: usize = 0;

        if let Some(ref summary_text) = existing_summary {
            total_tokens +=
                count_tokens(&format!("Summary of earlier conversation:\n{summary_text}"))
                    + MESSAGE_OVERHEAD;
        }

        let entry_tokens: Vec<usize> = entries
            .iter()
            .map(|entry| {
                let user_t = rendered_user_history_text(entry)
                    .as_deref()
                    .map(|text| count_tokens(text) + MESSAGE_OVERHEAD)
                    .unwrap_or(0);
                let assistant_t = rendered_assistant_history_text(entry)
                    .as_deref()
                    .map(|text| count_tokens(text) + MESSAGE_OVERHEAD)
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
            thread_id = conversation_thread_id,
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
            thread_id = conversation_thread_id,
            total_tokens, threshold, "context threshold reached, compressing conversation"
        );

        let hook_runtime = self.hook_runtime.read().await.clone();
        if let Some(runtime) = hook_runtime.as_ref() {
            let thread = match self
                .crud_store
                .get_thread_by_id(conversation_thread_id)
                .await
            {
                Ok(Some(thread)) => Some(thread),
                Ok(None) => {
                    warn!(
                        thread_id = conversation_thread_id,
                        "thread not found while preparing pre-compaction hook input"
                    );
                    None
                }
                Err(error) => {
                    warn!(
                        thread_id = conversation_thread_id,
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
                        execution_thread_id,
                        conversation_thread_id,
                        turn_id: execution_turn_id,
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
                            thread_id = execution_thread_id,
                            turn_id = execution_turn_id,
                            conversation_thread_id,
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
                                    thread_id = execution_thread_id,
                                    turn_id = execution_turn_id,
                                    conversation_thread_id,
                                    diagnostic_count = outcome.diagnostics.len(),
                                    run_count = outcome.runs.len(),
                                    "pre-compaction hook phase completed"
                                );
                            }
                        }
                        Err(error) => {
                            warn!(
                                thread_id = execution_thread_id,
                                turn_id = execution_turn_id,
                                conversation_thread_id,
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
            thread_id: execution_thread_id.to_owned(),
            turn_id: execution_turn_id.to_owned(),
            message: "Compressing conversation history...".to_owned(),
        };
        self.send_notification_to_thread_subscribers(
            execution_thread_id,
            events::CONTEXT_COMPRESSING,
            &compressing_notification,
        )
        .await;

        match summary::compress_context(
            &self.crud_store,
            &self.provider_registry,
            conversation_thread_id,
            &entries,
            existing_summary.as_deref(),
            target_tokens,
            &self.summary_config,
            fallback_model,
            fallback_model_provider,
        )
        .await
        {
            Ok(compressed_summary) => {
                let compressed_tokens = count_tokens(&compressed_summary);

                let compressed_notification = ContextCompressedNotification {
                    thread_id: execution_thread_id.to_owned(),
                    turn_id: execution_turn_id.to_owned(),
                    compressed_tokens,
                };
                self.send_notification_to_thread_subscribers(
                    execution_thread_id,
                    events::CONTEXT_COMPRESSED,
                    &compressed_notification,
                )
                .await;

                debug!(
                    thread_id = conversation_thread_id,
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
                    thread_id = conversation_thread_id,
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
            if let Some(user_text) = rendered_user_history_text(entry) {
                messages.push(ChatMessage::user(user_text));
            }
            if let Some(assistant_text) = rendered_assistant_history_text(entry) {
                messages.push(ChatMessage::assistant(assistant_text));
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
            if let Some(user_text) = rendered_user_history_text(&entries[i]) {
                messages.push(ChatMessage::user(user_text));
            }
            if let Some(assistant_text) = rendered_assistant_history_text(&entries[i]) {
                messages.push(ChatMessage::assistant(assistant_text));
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

    fn member_provider_is_configured(&self, workspace_id: &str, provider: &str) -> bool {
        let Ok(provider) = self.gateway_secrets.normalize_provider_name(provider) else {
            return false;
        };
        if provider == "local" {
            return true;
        }
        self.gateway_secrets
            .list_configured_workspace_provider_names(workspace_id)
            .is_ok_and(|providers| providers.into_iter().any(|name| name == provider))
            || self
                .gateway_secrets
                .list_workspace_provider_proxies(workspace_id)
                .is_ok_and(|proxies| proxies.into_iter().any(|(name, _)| name == provider))
    }
}

fn provider_model_info_to_protocol(m: ProviderModelInfo) -> ProviderModelInfo {
    ProviderModelInfo {
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
            embeddings: m.capabilities.embeddings,
            transcription: m.capabilities.transcription,
            thinking: m.capabilities.thinking,
            reasoning: m.capabilities.reasoning,
            fine_tuning: m.capabilities.fine_tuning,
            input_modalities: m.capabilities.input_modalities,
            output_modalities: m.capabilities.output_modalities,
        },
        transcription: m.transcription,
        pricing: m.pricing.map(|p| ProviderModelPricing {
            input_token: p.input_token,
            output_token: p.output_token,
            image: p.image,
            request: p.request,
        }),
        active: m.active,
        family: m.family,
        lifecycle_status: m.lifecycle_status,
    }
}

pub(super) fn rendered_user_history_text(entry: &ConversationEntry) -> Option<String> {
    crate::artifact_prompt_refs::append_history_artifact_refs(
        entry.user_text.as_deref(),
        &entry.user_artifacts,
        crate::artifact_prompt_refs::HistoryArtifactRefRole::User,
    )
}

pub(super) fn rendered_assistant_history_text(entry: &ConversationEntry) -> Option<String> {
    crate::artifact_prompt_refs::append_history_artifact_refs(
        entry.assistant_text.as_deref(),
        &entry.assistant_artifacts,
        crate::artifact_prompt_refs::HistoryArtifactRefRole::Assistant,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_crud::ConversationArtifactRef;
    use pioneer_protocol::{
        ArtifactBindingDirection, ArtifactBindingKind, ArtifactKind, ArtifactRole,
    };

    fn history_artifact_ref(role: ArtifactRole) -> ConversationArtifactRef {
        ConversationArtifactRef {
            artifact_id: "art_car".to_owned(),
            version_id: Some("ver_car_1".to_owned()),
            display_name: "car.jpg".to_owned(),
            kind: ArtifactKind::Image,
            mime_type: Some("image/jpeg".to_owned()),
            size_bytes: Some(862_208),
            sha256: Some("sha".to_owned()),
            binding_kind: ArtifactBindingKind::UserInput,
            direction: ArtifactBindingDirection::Input,
            role: Some(role),
            turn_id: Some("turn_1".to_owned()),
            message_id: Some("msg_1".to_owned()),
            turn_item_id: Some("item_1".to_owned()),
            item_index: Some(0),
        }
    }

    #[test]
    fn history_rendering_appends_artifact_refs_to_matching_message_text() {
        let entry = ConversationEntry {
            turn_id: "turn_1".to_owned(),
            user_text: Some("Что за машина?".to_owned()),
            assistant_text: Some("Похоже на седан.".to_owned()),
            user_artifacts: vec![history_artifact_ref(ArtifactRole::User)],
            assistant_artifacts: vec![history_artifact_ref(ArtifactRole::Assistant)],
        };

        let user_text = rendered_user_history_text(&entry).expect("user text");
        assert!(user_text.starts_with("Что за машина?"));
        assert!(user_text.contains("Available artifacts from this user message:"));
        assert!(user_text.contains("artifactId=art_car"));
        assert!(!user_text.contains("Artifact References"));

        let assistant_text = rendered_assistant_history_text(&entry).expect("assistant text");
        assert!(assistant_text.starts_with("Похоже на седан."));
        assert!(assistant_text.contains("Available artifacts from this assistant message:"));
        assert!(assistant_text.contains("artifactId=art_car"));
        assert!(!assistant_text.contains("Artifact References"));
    }
}
