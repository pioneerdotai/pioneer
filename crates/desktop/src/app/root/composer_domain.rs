use super::*;
use pioneer_client::composer::{state_machine::ComposerDomainState, store::*};

impl PioneerDesktop {
    fn composer_output(&self) -> Option<Arc<ComposerPublication>> {
        self.current_active_thread_id().and_then(|thread| {
            self.gateway
                .client_runtime
                .client_core()
                .composer_snapshot(thread)
        })
    }
    pub(in crate::app) fn composer_domain(&self) -> ComposerDomainState {
        self.composer_output()
            .map(|input| input.domain().clone())
            .unwrap_or_default()
    }
    pub(in crate::app) fn composer_domain_state(&self) -> ComposerDomainState {
        self.composer_domain()
    }
    pub(in crate::app) fn composer_authorization_fingerprint(&self) -> Option<String> {
        self.composer_output()
            .and_then(|input| input.authorization_fingerprint().map(str::to_owned))
    }
    pub(in crate::app) fn composer_upload_in_progress(&self) -> bool {
        self.composer_output()
            .and_then(|input| input.operation().cloned())
            .is_some_and(|operation| match operation.kind {
                ComposerOperationKind::Send => operation.pending(),
                ComposerOperationKind::Voice => matches!(
                    operation.status,
                    ComposerOperationStatus::Preparing | ComposerOperationStatus::Uploading
                ),
                _ => false,
            })
    }
    pub(in crate::app) fn desktop_voice_context_locked(&self) -> bool {
        self.composer_output()
            .and_then(|input| input.operation().cloned())
            .is_some_and(|operation| {
                operation.kind == ComposerOperationKind::Voice && operation.pending()
            })
    }
}
