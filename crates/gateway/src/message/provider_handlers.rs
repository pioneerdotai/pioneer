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
        let member = crate::authorization::AuthorizationService::new().role_disclosure_policy(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
        ) == Some(crate::authorization::RoleDisclosurePolicy::Collaborator);

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
        let provider_base_urls = match self
            .gateway_secrets
            .list_workspace_provider_base_urls(workspace_id.as_str())
        {
            Ok(provider_base_urls) => provider_base_urls,
            Err(error) => {
                let message = if member {
                    "provider catalog is unavailable".to_owned()
                } else {
                    format!("failed to list provider base urls: {error:#}")
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
            provider_configs.insert(name, (true, None, None));
        }
        for (name, proxy_url) in provider_proxies {
            provider_configs
                .entry(name)
                .and_modify(|entry: &mut (bool, Option<String>, Option<String>)| {
                    entry.1 = Some(proxy_url.clone())
                })
                .or_insert((false, Some(proxy_url), None));
        }
        for (name, base_url) in provider_base_urls {
            provider_configs
                .entry(name)
                .and_modify(|entry: &mut (bool, Option<String>, Option<String>)| {
                    entry.2 = Some(base_url.clone())
                })
                .or_insert((false, None, Some(base_url)));
        }

        // Local is built in, so there is no workspace secret or proxy from which to discover it.
        provider_configs
            .entry("local".to_owned())
            .or_insert((false, None, None));

        let providers = provider_configs
            .into_iter()
            .filter(|(name, _)| {
                crate::authorization::AuthorizationService::new().provider_allowed(
                    request_context.principal().kind,
                    request_context.principal().role_key.as_ref(),
                    name.as_str(),
                )
            })
            .map(|(name, (api_key_configured, proxy_url, base_url))| {
                let operationally_configured = api_key_configured
                    || proxy_url.is_some()
                    || base_url.is_some()
                    || name == "local";
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
                    base_url: if member { None } else { base_url },
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
        let member = crate::authorization::AuthorizationService::new().role_disclosure_policy(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
        ) == Some(crate::authorization::RoleDisclosurePolicy::Collaborator);

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
        if !crate::authorization::AuthorizationService::new().provider_allowed(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
            params.provider.trim(),
        ) {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
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
                    .filter(|model| {
                        crate::authorization::AuthorizationService::new().provider_model_allowed(
                            request_context.principal().kind,
                            request_context.principal().role_key.as_ref(),
                            params.provider.as_str(),
                            model.id.as_str(),
                        )
                    })
                    .collect();

                let result = ProviderListModelsResponse {
                    provider: params.provider.clone(),
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
        let member = crate::authorization::AuthorizationService::new().role_disclosure_policy(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
        ) == Some(crate::authorization::RoleDisclosurePolicy::Collaborator);

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
        if !crate::authorization::AuthorizationService::new().provider_allowed(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
            params.provider.trim(),
        ) {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
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
                    provider: params.provider.clone(),
                    models: models
                        .into_iter()
                        .map(provider_model_info_to_protocol)
                        .filter(|model| {
                            crate::authorization::AuthorizationService::new()
                                .provider_model_allowed(
                                    request_context.principal().kind,
                                    request_context.principal().role_key.as_ref(),
                                    params.provider.as_str(),
                                    model.id.as_str(),
                                )
                        })
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
        let member = crate::authorization::AuthorizationService::new().role_disclosure_policy(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
        ) == Some(crate::authorization::RoleDisclosurePolicy::Collaborator);

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
        if !crate::authorization::AuthorizationService::new().provider_allowed(
            request_context.principal().kind,
            request_context.principal().role_key.as_ref(),
            params.provider.trim(),
        ) {
            self.send_error(
                connection_id,
                AuthorizationExternalError::NotFound.response(request_id),
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
                    provider: params.provider.clone(),
                    models: models
                        .into_iter()
                        .map(provider_model_info_to_protocol)
                        .filter(|model| {
                            crate::authorization::AuthorizationService::new()
                                .provider_model_allowed(
                                    request_context.principal().kind,
                                    request_context.principal().role_key.as_ref(),
                                    params.provider.as_str(),
                                    model.id.as_str(),
                                )
                        })
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

        if params.clear_base_url && params.base_url.is_some() {
            self.send_error(
                connection_id,
                JsonRpcErrorResponse::new(
                    Some(request_id),
                    INVALID_PARAMS_CODE,
                    format!(
                        "invalid params for `{}`: `base_url` and `clear_base_url` cannot both be set",
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
        let base_url = match params.base_url {
            Some(base_url) => {
                let trimmed = base_url.trim().to_owned();
                if trimmed.is_empty() {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_PARAMS_CODE,
                            format!(
                                "invalid params for `{}`: `base_url` must not be empty when provided",
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

        let mut base_url_updated = false;
        let mut base_url_deleted = false;
        let mut response_base_url = self
            .gateway_secrets
            .get_workspace_provider_base_url(workspace_id.as_str(), raw_provider.as_str())
            .ok()
            .flatten();
        if let Some(base_url) = base_url {
            match self.gateway_secrets.set_workspace_provider_base_url(
                workspace_id.as_str(),
                raw_provider.as_str(),
                base_url.as_str(),
            ) {
                Ok(provider) => {
                    normalized_provider = provider;
                    base_url_updated = true;
                    response_base_url = Some(base_url);
                }
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to save provider base url: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            }
        } else if params.clear_base_url {
            match self
                .gateway_secrets
                .delete_workspace_provider_base_url(workspace_id.as_str(), raw_provider.as_str())
            {
                Ok((provider, deleted)) => {
                    normalized_provider = provider;
                    base_url_deleted = deleted;
                    response_base_url = None;
                }
                Err(error) => {
                    self.send_error(
                        connection_id,
                        JsonRpcErrorResponse::new(
                            Some(request_id),
                            INVALID_REQUEST_CODE,
                            format!("failed to delete provider base url: {error:#}"),
                        ),
                    )
                    .await;
                    return;
                }
            }
        }

        if api_key_updated || proxy_updated || proxy_deleted || base_url_updated || base_url_deleted
        {
            self.provider_registry
                .invalidate_workspace_provider(workspace_id.as_str(), normalized_provider.as_str());
            self.request_api_provider_warmup(workspace_id.clone());
        }

        let response = ProviderConfigureResponse {
            provider: normalized_provider,
            api_key_updated,
            proxy_updated,
            proxy_deleted,
            proxy_url: response_proxy_url,
            base_url_updated,
            base_url_deleted,
            base_url: response_base_url,
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

        self.provider_registry
            .invalidate_workspace_provider(workspace_id.as_str(), &normalized_provider);
        self.request_api_provider_warmup(workspace_id.clone());

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
            self.provider_registry
                .invalidate_workspace_provider(workspace_id.as_str(), &normalized_provider);
            self.request_api_provider_warmup(workspace_id.clone());
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

    /// Restore canonical history without generating summaries or truncating sources.
    /// The native request controller owns admission and full-request budgeting.
    pub(super) async fn load_conversation_history_for_workspace(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> anyhow::Result<Vec<ChatMessage>> {
        let store = self.crud_store.as_ref();
        let prepared = self
            .capture_current_context_basis_prepared(
                store,
                workspace_id,
                thread_id,
                turn_id,
                Some(turn_id),
            )
            .await?;
        Ok(prepared.messages)
    }

    #[cfg(test)]
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
