use super::*;

impl MessageProcessor {
    pub(super) async fn agent_route_create(
        &self,
        context: &crate::request_context::RequestContext,
        request_id: RequestId,
        params: pioneer_protocol::AgentDelegationRouteCreateParams,
    ) {
        let service =
            crate::authorization::AgentRouteManagementService::new(self.crud_store.clone());
        match service.create(context, params).await {
            Ok(route) => {
                self.send_agent_route_response(context.connection_id(), request_id, &route)
                    .await
            }
            Err(error) => {
                self.send_agent_route_error(context.connection_id(), request_id, error)
                    .await
            }
        }
    }

    pub(super) async fn agent_route_list(
        &self,
        context: &crate::request_context::RequestContext,
        request_id: RequestId,
        params: pioneer_protocol::AgentDelegationRouteListParams,
    ) {
        let service =
            crate::authorization::AgentRouteManagementService::new(self.crud_store.clone());
        match service.list(context, params).await {
            Ok(routes) => {
                self.send_agent_route_response(context.connection_id(), request_id, &routes)
                    .await
            }
            Err(error) => {
                self.send_agent_route_error(context.connection_id(), request_id, error)
                    .await
            }
        }
    }

    pub(super) async fn agent_route_revoke(
        &self,
        context: &crate::request_context::RequestContext,
        request_id: RequestId,
        params: pioneer_protocol::AgentDelegationRouteRevokeParams,
    ) {
        let service =
            crate::authorization::AgentRouteManagementService::new(self.crud_store.clone());
        match service.revoke(context, params).await {
            Ok(route) => {
                self.send_agent_route_response(context.connection_id(), request_id, &route)
                    .await
            }
            Err(error) => {
                self.send_agent_route_error(context.connection_id(), request_id, error)
                    .await
            }
        }
    }

    async fn send_agent_route_response<T: serde::Serialize>(
        &self,
        connection_id: crate::session::ConnectionId,
        request_id: RequestId,
        result: &T,
    ) {
        match JsonRpcResponse::from_result(request_id, result) {
            Ok(response) => {
                if let Err(error) = self.send_json(connection_id, &response).await {
                    warn!(
                        failure_class = "agent_route_response_delivery_failed",
                        "failed to send Agent route response"
                    );
                    let _ = error;
                }
            }
            Err(error) => {
                self.send_error(
                    connection_id,
                    JsonRpcErrorResponse::new(
                        None,
                        INVALID_REQUEST_CODE,
                        "Agent route response could not be encoded",
                    ),
                )
                .await;
                let _ = error;
            }
        }
    }

    async fn send_agent_route_error(
        &self,
        connection_id: crate::session::ConnectionId,
        request_id: RequestId,
        error: anyhow::Error,
    ) {
        warn!(
            failure_class = "agent_route_operation_failed",
            "Agent route lifecycle operation failed"
        );
        let _ = error;
        self.send_error(
            connection_id,
            JsonRpcErrorResponse::new(
                Some(request_id),
                INVALID_REQUEST_CODE,
                "Agent route operation is not authorized or could not be completed",
            ),
        )
        .await;
    }
}
