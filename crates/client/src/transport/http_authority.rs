//! Adapts current process-local session access without a parallel refresh owner.
use super::{
    http::{GatewayHttpAccess, GatewayHttpAuthorityError, GatewayHttpSessionAuthority},
    ws::GatewayWsCommandSender,
};
use async_trait::async_trait;

pub(crate) struct GatewayWsHttpAuthority {
    pub(crate) sender: GatewayWsCommandSender,
}

#[async_trait]
impl GatewayHttpSessionAuthority for GatewayWsHttpAuthority {
    async fn current_access(&self) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError> {
        self.sender.current_gateway_http_access()
    }

    async fn coordinated_refresh(
        &self,
        rejected_generation: u64,
    ) -> Result<GatewayHttpAccess, GatewayHttpAuthorityError> {
        let current = self.sender.current_gateway_http_access()?;
        if current.generation != rejected_generation {
            Ok(current)
        } else {
            // Mobile refresh credentials remain owned by the existing session
            // coordinator. Native storage I/O never starts a parallel refresh.
            Err(GatewayHttpAuthorityError::TemporarilyUnavailable)
        }
    }
}
