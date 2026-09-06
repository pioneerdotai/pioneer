//! Authoritative Gateway settings and request generations for one Client.
use crate::core::{ClientCore, ClientGeneration, ClientScope};
use pioneer_protocol::{
    GatewaySettingsGetResponse, GatewaySettingsSnapshot, GatewaySettingsUpdate,
    GatewaySettingsUpdateResponse,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
pub struct GatewaySettingsStore {
    pub settings: Option<GatewaySettingsSnapshot>,
    pub voice_input: Option<pioneer_protocol::GatewayVoiceInputSettings>,
    pub vector_refill_refresh_requested: bool,
    pub loading: bool,
    pub saving: bool,
    pub error: Option<String>,
}

impl ClientCore {
    pub(crate) fn gateway_http_generation(&self) -> Option<u64> {
        self.compatibility_runtime()
            .ws_command_sender()
            .current_gateway_http_access()
            .ok()
            .map(|access| access.generation)
    }

    pub(crate) fn observe_gateway_settings(&self, event: &crate::transport::ws::GatewayWsEvent) {
        let crate::transport::ws::GatewayWsEvent::Notification { notification, .. } = event else {
            return;
        };
        if !self
            .compatibility_runtime()
            .ws_command_sender()
            .gateway_state_event_is_current(event)
        {
            return;
        }
        self.reduce_settings_notification(notification);
    }

    fn reduce_settings_notification(&self, notification: &pioneer_protocol::GatewayNotification) {
        use pioneer_protocol::GatewayNotification;
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped() {
            return;
        }
        let before = owner.settings.clone();
        match notification {
            GatewayNotification::GatewayRemoteAccessStatusChanged(change) => {
                owner.settings_notifications[0] = owner.settings_notifications[0]
                    .checked_add(1)
                    .expect("settings notification generation exhausted");
                if let Some(settings) = owner.settings.settings.as_mut() {
                    settings.remote_access.status = change.status.clone();
                }
            }
            GatewayNotification::GatewayThreadEpisodicVectorRefillStatusChanged(change) => {
                owner.settings_notifications[1] = owner.settings_notifications[1]
                    .checked_add(1)
                    .expect("settings notification generation exhausted");
                if let Some(settings) = owner.settings.settings.as_mut() {
                    let refresh = apply_vector_refill_notification(
                        &mut settings.thread_episodic.vector_search,
                        change,
                    );
                    owner.settings.vector_refill_refresh_requested |= refresh;
                }
            }
            GatewayNotification::GatewayVoiceInputStatusChanged(change) => {
                owner.settings_notifications[2] = owner.settings_notifications[2]
                    .checked_add(1)
                    .expect("settings notification generation exhausted");
                owner.settings.voice_input = Some(change.settings.clone());
                if let Some(settings) = owner.settings.settings.as_mut() {
                    settings.voice_input = change.settings.clone();
                }
                owner.settings.error = None;
            }
            _ => return,
        }
        if owner.settings != before {
            self.publish_gateway_settings(&owner.settings);
        }
    }

    pub fn gateway_settings(&self) -> GatewaySettingsStore {
        self.identity_authorization
            .lock()
            .expect("identity owner poisoned")
            .settings
            .clone()
    }

    pub fn request_gateway_settings(&self) -> anyhow::Result<ClientGeneration> {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(!self.is_stopped(), "Client runtime is stopped");
        owner.settings_request = owner
            .settings_request
            .checked_add(1)
            .expect("settings generation exhausted");
        owner.settings_request_connection = self.gateway_http_generation();
        owner.settings_request_notifications = owner.settings_notifications;
        owner.settings.vector_refill_refresh_requested = false;
        owner.settings.loading = true;
        owner.settings.error = None;
        self.publish_gateway_settings(&owner.settings);
        Ok(ClientGeneration::new(owner.settings_request))
    }

    fn finish_gateway_settings(
        &self,
        generation: ClientGeneration,
        result: &mut anyhow::Result<GatewaySettingsSnapshot>,
    ) -> bool {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        if self.is_stopped()
            || owner.settings_request != generation.get()
            || owner.settings_request_connection != self.gateway_http_generation()
        {
            return false;
        }
        owner.settings.loading = false;
        owner.settings.saving = false;
        match result {
            Ok(settings) => {
                let delivered = owner.settings_notifications;
                let requested = owner.settings_request_notifications;
                if let Some(current) = &owner.settings.settings {
                    if delivered[0] != requested[0] {
                        settings.remote_access.status = current.remote_access.status.clone();
                    }
                    if delivered[1] != requested[1] {
                        let latest = &current.thread_episodic.vector_search;
                        let target = &mut settings.thread_episodic.vector_search;
                        target.refill_status = latest.refill_status;
                        target.local_model_status = latest.local_model_status;
                        target.downloaded_bytes = latest.downloaded_bytes;
                        target.total_bytes = latest.total_bytes;
                    }
                }
                if delivered[2] != requested[2] {
                    if let Some(voice) = &owner.settings.voice_input {
                        settings.voice_input = voice.clone();
                    }
                }
                owner.settings.voice_input = Some(settings.voice_input.clone());
                owner.settings.settings = Some(settings.clone());
                owner.settings.error = None;
            }
            Err(error) => owner.settings.error = Some(format!("{error:#}")),
        }
        self.publish_gateway_settings(&owner.settings);
        true
    }

    pub fn refresh_gateway_settings(&self) -> anyhow::Result<GatewaySettingsGetResponse> {
        let generation = self.request_gateway_settings()?;
        self.load_gateway_settings(generation)
    }

    pub fn load_gateway_settings(
        &self,
        generation: ClientGeneration,
    ) -> anyhow::Result<GatewaySettingsGetResponse> {
        {
            let owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            anyhow::ensure!(
                !self.is_stopped()
                    && owner.settings_request == generation.get()
                    && owner.settings.loading,
                "Settings request is no longer pending"
            );
        }
        let mut result = self
            .compatibility_runtime()
            .ws_command_sender()
            .gateway_settings_get()
            .map(|response| response.settings);
        anyhow::ensure!(
            self.finish_gateway_settings(generation, &mut result),
            "Settings response belongs to a superseded authorization generation"
        );
        result.map(|settings| GatewaySettingsGetResponse { settings })
    }

    pub fn update_gateway_settings(
        &self,
        update: GatewaySettingsUpdate,
    ) -> anyhow::Result<GatewaySettingsUpdateResponse> {
        let generation = self.prepare_gateway_settings_update(None)?;
        self.execute_gateway_settings_update(generation, update)
    }

    pub fn prepare_gateway_settings_update(
        &self,
        optimistic: Option<GatewaySettingsSnapshot>,
    ) -> anyhow::Result<ClientGeneration> {
        let mut owner = self
            .identity_authorization
            .lock()
            .expect("identity owner poisoned");
        anyhow::ensure!(!self.is_stopped(), "Client runtime is stopped");
        owner.settings_request = owner
            .settings_request
            .checked_add(1)
            .expect("settings generation exhausted");
        owner.settings_request_connection = self.gateway_http_generation();
        owner.settings_request_notifications = owner.settings_notifications;
        owner.settings.vector_refill_refresh_requested = false;
        owner.settings.saving = true;
        owner.settings.error = None;
        if let Some(snapshot) = optimistic {
            owner.settings.settings = Some(snapshot);
        }
        self.publish_gateway_settings(&owner.settings);
        Ok(ClientGeneration::new(owner.settings_request))
    }

    pub fn execute_gateway_settings_update(
        &self,
        generation: ClientGeneration,
        update: GatewaySettingsUpdate,
    ) -> anyhow::Result<GatewaySettingsUpdateResponse> {
        {
            let owner = self
                .identity_authorization
                .lock()
                .expect("identity owner poisoned");
            anyhow::ensure!(
                !self.is_stopped()
                    && owner.settings_request == generation.get()
                    && owner.settings.saving,
                "Settings update is no longer pending"
            );
        }
        let mut result = self
            .compatibility_runtime()
            .ws_command_sender()
            .gateway_settings_update(update)
            .map(|response| response.settings);
        anyhow::ensure!(
            self.finish_gateway_settings(generation, &mut result),
            "Settings update belongs to a superseded authorization generation"
        );
        result.map(|settings| GatewaySettingsUpdateResponse { settings })
    }

    fn publish_gateway_settings(&self, settings: &GatewaySettingsStore) {
        use crate::core::{
            ClientMutationAuthority, ClientRevisions, ContentRevision, DomainRevision,
            PresentationRevision, ScopedRevision,
        };
        let revision = self.snapshot(&ClientScope::Settings).map_or(1, |snapshot| {
            snapshot
                .revisions()
                .scoped()
                .get()
                .checked_add(1)
                .expect("settings revision exhausted")
        });
        self.publish(
            &ClientMutationAuthority { _private: () },
            ClientScope::Settings,
            ClientRevisions::new(
                DomainRevision::new(revision),
                PresentationRevision::new(revision),
                ContentRevision::ZERO,
                ScopedRevision::new(revision),
            ),
            std::sync::Arc::new(settings.clone()),
            vec![],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot(keepawake: bool) -> GatewaySettingsSnapshot {
        GatewaySettingsSnapshot {
            general: pioneer_protocol::GatewayGeneralSettings {
                keepawake,
                ..Default::default()
            },
            memory: Default::default(),
            self_improvement: Default::default(),
            thread_episodic: Default::default(),
            cli_runtimes: Default::default(),
            remote_access: Default::default(),
            voice_input: Default::default(),
        }
    }
    #[test]
    fn voice_ingress_publishes_once_and_stale_or_stopped_ingress_cannot_restore_settings() {
        let core = ClientCore::new();
        let settings = pioneer_protocol::GatewayVoiceInputSettings {
            enabled: true,
            ..Default::default()
        };
        let notification = pioneer_protocol::GatewayNotification::GatewayVoiceInputStatusChanged(
            pioneer_protocol::GatewayVoiceInputStatusChangedNotification {
                settings: settings.clone(),
            },
        );
        core.reduce_settings_notification(&notification);
        let accepted = core.snapshot(&ClientScope::Settings).unwrap();
        assert_eq!(core.gateway_settings().voice_input, Some(settings));
        core.reduce_settings_notification(&notification);
        assert_eq!(
            accepted.revisions(),
            core.snapshot(&ClientScope::Settings).unwrap().revisions()
        );
        core.invalidate_authorization_revision(1);
        core.observe_gateway_settings(&crate::transport::ws::GatewayWsEvent::Notification {
            connection_id: 17,
            notification: notification.clone(),
        });
        assert_eq!(core.gateway_settings(), GatewaySettingsStore::default());
        core.shutdown();
        core.reduce_settings_notification(&notification);
        assert_eq!(core.gateway_settings(), GatewaySettingsStore::default());
        assert!(core.snapshot(&ClientScope::Settings).is_none());
    }

    #[test]
    fn delayed_settings_response_preserves_newer_voice_ingress_in_both_owner_and_result() {
        let core = ClientCore::new();
        let request = core.request_gateway_settings().unwrap();
        let voice = pioneer_protocol::GatewayVoiceInputSettings {
            enabled: true,
            ..Default::default()
        };
        core.reduce_settings_notification(
            &pioneer_protocol::GatewayNotification::GatewayVoiceInputStatusChanged(
                pioneer_protocol::GatewayVoiceInputStatusChangedNotification {
                    settings: voice.clone(),
                },
            ),
        );
        let mut response = Ok(snapshot(false));
        assert!(core.finish_gateway_settings(request, &mut response));
        assert_eq!(response.unwrap().voice_input, voice);
        assert_eq!(core.gateway_settings().settings.unwrap().voice_input, voice);
    }

    #[test]
    fn terminal_refill_during_a_settings_request_keeps_one_followup_refresh_pending() {
        let core = ClientCore::new();
        let initial = core.request_gateway_settings().unwrap();
        assert!(core.finish_gateway_settings(initial, &mut Ok(snapshot(false))));
        let pending = core.request_gateway_settings().unwrap();
        core.reduce_settings_notification(
            &pioneer_protocol::GatewayNotification::GatewayThreadEpisodicVectorRefillStatusChanged(
                pioneer_protocol::GatewayThreadEpisodicVectorRefillStatusChangedNotification {
                    workspace_id: "synthetic-workspace".into(),
                    status: pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Complete,
                    local_model_status: None,
                    downloaded_bytes: None,
                    total_bytes: None,
                },
            ),
        );
        assert!(core.gateway_settings().vector_refill_refresh_requested);
        assert!(core.finish_gateway_settings(pending, &mut Ok(snapshot(false))));
        assert!(core.gateway_settings().vector_refill_refresh_requested);
        let followup = core.request_gateway_settings().unwrap();
        assert!(!core.gateway_settings().vector_refill_refresh_requested);
        assert!(core.finish_gateway_settings(followup, &mut Ok(snapshot(false))));
        assert!(!core.gateway_settings().vector_refill_refresh_requested);
    }

    #[test]
    fn superseded_load_cannot_overwrite_an_optimistic_update_or_its_authoritative_result() {
        let core = ClientCore::new();
        let load = core.request_gateway_settings().unwrap();
        let save = core
            .prepare_gateway_settings_update(Some(snapshot(true)))
            .unwrap();
        assert!(!core.finish_gateway_settings(load, &mut Ok(snapshot(false))));
        assert!(core.gateway_settings().settings.unwrap().general.keepawake);
        assert!(core.finish_gateway_settings(save, &mut Ok(snapshot(true))));
        let before = core.snapshot(&ClientScope::Settings).unwrap();
        assert!(core.finish_gateway_settings(save, &mut Ok(snapshot(true))));
        assert!(std::sync::Arc::ptr_eq(
            &before.snapshot(),
            &core.snapshot(&ClientScope::Settings).unwrap().snapshot()
        ));
        assert!(!core.gateway_settings().saving);
    }

    #[test]
    fn authorization_fence_clears_loading_and_rejects_late_settings_failure() {
        let core = ClientCore::new();
        let request = core.request_gateway_settings().unwrap();
        assert!(core.gateway_settings().loading);
        core.invalidate_authorization_revision(3);
        assert_eq!(core.gateway_settings(), GatewaySettingsStore::default());
        assert!(
            !core.finish_gateway_settings(request, &mut Err(anyhow::anyhow!("synthetic failure")))
        );
        let publication = core
            .snapshot(&ClientScope::Settings)
            .unwrap()
            .typed::<GatewaySettingsStore>()
            .unwrap();
        assert_eq!(
            publication.payload().as_ref(),
            &GatewaySettingsStore::default()
        );
    }
}

pub fn apply_vector_refill_notification(
    vector_search: &mut pioneer_protocol::GatewayThreadEpisodicVectorSearchSettings,
    notification: &pioneer_protocol::GatewayThreadEpisodicVectorRefillStatusChangedNotification,
) -> bool {
    vector_search.refill_status = notification.status;
    if let Some(local_model_status) = notification.local_model_status {
        vector_search.local_model_status = local_model_status;
        if local_model_status
            == pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Downloading
        {
            vector_search.downloaded_bytes = notification.downloaded_bytes;
            vector_search.total_bytes = notification.total_bytes;
        } else {
            vector_search.downloaded_bytes = None;
            vector_search.total_bytes = None;
        }
    } else if notification.status
        == pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Running
        && vector_search.provider
            == Some(pioneer_protocol::GatewayThreadEpisodicVectorProvider::Local)
        && vector_search.local_model_status
            != pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Installed
    {
        vector_search.local_model_status =
            pioneer_protocol::GatewayThreadEpisodicVectorLocalModelStatus::Downloading;
    }

    let terminal = matches!(
        notification.status,
        pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Complete
            | pioneer_protocol::GatewayThreadEpisodicVectorRefillStatus::Failed
    );
    if terminal {
        vector_search.downloaded_bytes = None;
        vector_search.total_bytes = None;
    }
    terminal
}
