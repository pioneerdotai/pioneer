use super::GatewayWsCommandSender;
use crate::thread_platform::DesktopClientFileSystem;
use anyhow::Result;
use pioneer_client::composer::turn_prepare::{
    PrepareComposerTurnRequest, PreparedComposerTurn, PreparedVoiceComposerSnapshot,
};

pub(crate) trait DesktopGatewayWsCommandSenderExt {
    fn prepare_composer_voice(
        &self,
        core: &pioneer_client::core::ClientCore,
        identity: &pioneer_client::composer::store::ComposerOperationIdentity,
        endpoint_kind: Option<pioneer_client::gateway::types::GatewayEndpointKind>,
    ) -> Result<PreparedVoiceComposerSnapshot>;
    fn submit_composer_send(
        &self,
        core: &std::sync::Arc<pioneer_client::core::ClientCore>,
        identity: pioneer_client::composer::store::ComposerOperationIdentity,
        context: pioneer_client::composer::workflow::ComposerSendContext,
    ) -> Result<pioneer_client::composer::workflow::ComposerSendResult>;

    fn prepare_composer_turn(
        &self,
        request: PrepareComposerTurnRequest,
    ) -> Result<PreparedComposerTurn>;
}

impl DesktopGatewayWsCommandSenderExt for GatewayWsCommandSender {
    fn prepare_composer_voice(
        &self,
        core: &pioneer_client::core::ClientCore,
        identity: &pioneer_client::composer::store::ComposerOperationIdentity,
        endpoint_kind: Option<pioneer_client::gateway::types::GatewayEndpointKind>,
    ) -> Result<PreparedVoiceComposerSnapshot> {
        core.prepare_composer_voice(identity, &DesktopClientFileSystem, endpoint_kind)
    }

    fn submit_composer_send(
        &self,
        core: &std::sync::Arc<pioneer_client::core::ClientCore>,
        identity: pioneer_client::composer::store::ComposerOperationIdentity,
        context: pioneer_client::composer::workflow::ComposerSendContext,
    ) -> Result<pioneer_client::composer::workflow::ComposerSendResult> {
        core.submit_composer_send(identity, &DesktopClientFileSystem, context)
    }

    fn prepare_composer_turn(
        &self,
        request: PrepareComposerTurnRequest,
    ) -> Result<PreparedComposerTurn> {
        self.prepare_composer_turn_with_file_system(&DesktopClientFileSystem, request)
    }
}
