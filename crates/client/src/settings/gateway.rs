//! Gateway settings state.

use pioneer_protocol::{
    GatewayGeneralSettingsUpdate, GatewayMemoryModelSelection, GatewaySettingsGetParams,
    GatewaySettingsGetResponse, GatewaySettingsSnapshot, GatewaySettingsUpdate,
    GatewaySettingsUpdateParams, GatewaySettingsUpdateResponse,
    GatewayThreadEpisodicSettingsUpdate,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct GatewaySettingsState {
    pub settings: Option<GatewaySettingsSnapshot>,
    pub loading: bool,
    pub error: Option<String>,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct GatewaySettingsUpdatePlan {
    pub snapshot: GatewaySettingsSnapshot,
    pub update: GatewaySettingsUpdate,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum GatewaySettingsRefreshUnavailable {
    GatewayNotConnected,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct GatewaySettingsActionScope {
    pub connection_id: u64,
    pub connection_epoch: u64,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum GatewaySettingsRefreshPlan {
    Send(GatewaySettingsActionScope),
    SkipAlreadyLoading,
    Unavailable(GatewaySettingsRefreshUnavailable),
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ThreadEpisodicSettingToggle {
    Enabled,
}

pub fn gateway_settings_get_params() -> GatewaySettingsGetParams {
    GatewaySettingsGetParams::default()
}

pub fn gateway_settings_update_params(
    update: GatewaySettingsUpdate,
) -> GatewaySettingsUpdateParams {
    GatewaySettingsUpdateParams { update }
}

pub fn should_start_gateway_settings_refresh(loading: bool) -> bool {
    !loading
}

pub fn plan_gateway_settings_refresh(
    loading: bool,
    gateway_connected: bool,
    connection_id: Option<u64>,
    connection_epoch: u64,
) -> GatewaySettingsRefreshPlan {
    if !should_start_gateway_settings_refresh(loading) {
        return GatewaySettingsRefreshPlan::SkipAlreadyLoading;
    }
    let Some(connection_id) = gateway_connected.then_some(connection_id).flatten() else {
        return GatewaySettingsRefreshPlan::Unavailable(
            GatewaySettingsRefreshUnavailable::GatewayNotConnected,
        );
    };

    GatewaySettingsRefreshPlan::Send(GatewaySettingsActionScope {
        connection_id,
        connection_epoch,
    })
}

pub fn plan_gateway_settings_update_action(
    connection_id: Option<u64>,
    connection_epoch: u64,
) -> Option<GatewaySettingsActionScope> {
    Some(GatewaySettingsActionScope {
        connection_id: connection_id?,
        connection_epoch,
    })
}

pub fn settings_action_matches_connection(
    action_connection_id: u64,
    action_connection_epoch: u64,
    current_connection_id: Option<u64>,
    current_connection_epoch: u64,
) -> bool {
    current_connection_id == Some(action_connection_id)
        && current_connection_epoch == action_connection_epoch
}

pub fn apply_gateway_settings_unavailable(
    settings: &mut Option<GatewaySettingsSnapshot>,
    loading: &mut bool,
    error: &mut Option<String>,
    message: impl Into<String>,
) {
    *settings = None;
    *loading = false;
    *error = Some(message.into());
}

pub fn begin_gateway_settings_refresh(loading: &mut bool, error: &mut Option<String>) {
    *loading = true;
    *error = None;
}

pub fn apply_gateway_settings_get_response(
    settings: &mut Option<GatewaySettingsSnapshot>,
    loading: &mut bool,
    error: &mut Option<String>,
    response: GatewaySettingsGetResponse,
) {
    *settings = Some(response.settings);
    *loading = false;
    *error = None;
}

pub fn apply_gateway_settings_get_error(
    loading: &mut bool,
    error: &mut Option<String>,
    message: impl Into<String>,
) {
    *loading = false;
    *error = Some(message.into());
}

pub fn apply_optimistic_gateway_settings_update(
    settings: &mut Option<GatewaySettingsSnapshot>,
    error: &mut Option<String>,
    snapshot: GatewaySettingsSnapshot,
) {
    *settings = Some(snapshot);
    *error = None;
}

pub fn apply_gateway_settings_update_response(
    settings: &mut Option<GatewaySettingsSnapshot>,
    error: &mut Option<String>,
    response: GatewaySettingsUpdateResponse,
) {
    *settings = Some(response.settings);
    *error = None;
}

pub fn apply_gateway_settings_update_error(error: &mut Option<String>, message: impl Into<String>) {
    *error = Some(message.into());
}

pub fn gateway_settings_state_from_parts(
    settings: Option<GatewaySettingsSnapshot>,
    loading: bool,
    error: Option<String>,
) -> GatewaySettingsState {
    GatewaySettingsState {
        settings,
        loading,
        error,
    }
}

pub fn keepawake_update_plan(
    current: Option<&GatewaySettingsSnapshot>,
    enabled: bool,
) -> Option<GatewaySettingsUpdatePlan> {
    let mut snapshot = current.cloned()?;
    snapshot.general.keepawake = enabled;

    Some(GatewaySettingsUpdatePlan {
        snapshot,
        update: GatewaySettingsUpdate {
            general: Some(GatewayGeneralSettingsUpdate {
                keepawake: Some(enabled),
                preflight_model: None,
            }),
            memory: None,
            thread_episodic: None,
        },
    })
}

pub fn preflight_model_update_plan(
    current: Option<&GatewaySettingsSnapshot>,
    model_selection: GatewayMemoryModelSelection,
) -> Option<GatewaySettingsUpdatePlan> {
    let mut snapshot = current.cloned()?;
    snapshot.general.preflight_model = model_selection.clone();

    Some(GatewaySettingsUpdatePlan {
        snapshot,
        update: GatewaySettingsUpdate {
            general: Some(GatewayGeneralSettingsUpdate {
                keepawake: None,
                preflight_model: Some(model_selection),
            }),
            memory: None,
            thread_episodic: None,
        },
    })
}

pub fn thread_episodic_enabled_update_plan(
    current: Option<&GatewaySettingsSnapshot>,
    enabled: bool,
) -> Option<GatewaySettingsUpdatePlan> {
    let mut snapshot = current.cloned()?;
    snapshot.thread_episodic.enabled = enabled;

    Some(GatewaySettingsUpdatePlan {
        snapshot,
        update: GatewaySettingsUpdate {
            general: None,
            memory: None,
            thread_episodic: Some(GatewayThreadEpisodicSettingsUpdate::enabled(enabled)),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{GatewayGeneralSettings, GatewayMemorySettings};

    fn snapshot(keepawake: bool) -> GatewaySettingsSnapshot {
        GatewaySettingsSnapshot {
            general: GatewayGeneralSettings {
                keepawake,
                ..Default::default()
            },
            memory: GatewayMemorySettings::default(),
            thread_episodic: Default::default(),
        }
    }

    #[test]
    fn refresh_state_transitions_preserve_desktop_behavior() {
        let mut settings = Some(snapshot(false));
        let mut loading = false;
        let mut error = Some("old".to_owned());

        assert!(should_start_gateway_settings_refresh(loading));
        begin_gateway_settings_refresh(&mut loading, &mut error);
        assert!(loading);
        assert!(error.is_none());

        apply_gateway_settings_get_response(
            &mut settings,
            &mut loading,
            &mut error,
            GatewaySettingsGetResponse {
                settings: snapshot(true),
            },
        );
        assert!(!loading);
        assert!(error.is_none());
        assert_eq!(
            settings.as_ref().map(|settings| settings.general.keepawake),
            Some(true)
        );

        apply_gateway_settings_get_error(&mut loading, &mut error, "network failed");
        assert!(!loading);
        assert_eq!(error.as_deref(), Some("network failed"));
    }

    #[test]
    fn unavailable_state_clears_snapshot_and_records_error() {
        let mut settings = Some(snapshot(false));
        let mut loading = true;
        let mut error = None;

        apply_gateway_settings_unavailable(
            &mut settings,
            &mut loading,
            &mut error,
            "Gateway is not connected",
        );

        assert!(settings.is_none());
        assert!(!loading);
        assert_eq!(error.as_deref(), Some("Gateway is not connected"));
    }

    #[test]
    fn optimistic_update_and_response_reconcile_snapshot() {
        let mut settings = None;
        let mut error = Some("old".to_owned());

        apply_optimistic_gateway_settings_update(&mut settings, &mut error, snapshot(true));
        assert_eq!(
            settings.as_ref().map(|settings| settings.general.keepawake),
            Some(true)
        );
        assert!(error.is_none());

        apply_gateway_settings_update_response(
            &mut settings,
            &mut error,
            GatewaySettingsUpdateResponse {
                settings: snapshot(false),
            },
        );
        assert_eq!(
            settings.as_ref().map(|settings| settings.general.keepawake),
            Some(false)
        );
        assert!(error.is_none());

        apply_gateway_settings_update_error(&mut error, "update failed");
        assert_eq!(error.as_deref(), Some("update failed"));
    }

    #[test]
    fn request_params_and_connection_matching_are_deterministic() {
        let update = GatewaySettingsUpdate::default();
        let params = gateway_settings_update_params(update);
        assert!(params.update.general.is_none());
        assert!(params.update.memory.is_none());
        assert!(params.update.thread_episodic.is_none());
        let _params: GatewaySettingsGetParams = gateway_settings_get_params();

        assert!(settings_action_matches_connection(7, 3, Some(7), 3));
        assert!(!settings_action_matches_connection(7, 3, Some(8), 3));
        assert!(!settings_action_matches_connection(7, 3, Some(7), 4));
        assert!(!settings_action_matches_connection(7, 3, None, 3));
    }

    #[test]
    fn refresh_plan_requires_idle_connected_gateway() {
        assert_eq!(
            plan_gateway_settings_refresh(true, true, Some(7), 3),
            GatewaySettingsRefreshPlan::SkipAlreadyLoading
        );
        assert_eq!(
            plan_gateway_settings_refresh(false, false, Some(7), 3),
            GatewaySettingsRefreshPlan::Unavailable(
                GatewaySettingsRefreshUnavailable::GatewayNotConnected
            )
        );
        assert_eq!(
            plan_gateway_settings_refresh(false, true, None, 3),
            GatewaySettingsRefreshPlan::Unavailable(
                GatewaySettingsRefreshUnavailable::GatewayNotConnected
            )
        );
        assert_eq!(
            plan_gateway_settings_refresh(false, true, Some(7), 3),
            GatewaySettingsRefreshPlan::Send(GatewaySettingsActionScope {
                connection_id: 7,
                connection_epoch: 3,
            })
        );
    }

    #[test]
    fn update_action_plan_requires_connection_id() {
        assert_eq!(
            plan_gateway_settings_update_action(Some(7), 3),
            Some(GatewaySettingsActionScope {
                connection_id: 7,
                connection_epoch: 3,
            })
        );
        assert_eq!(plan_gateway_settings_update_action(None, 3), None);
    }

    #[test]
    fn keepawake_update_plan_updates_general_only() {
        let plan = keepawake_update_plan(Some(&snapshot(false)), true).expect("plan");

        assert!(plan.snapshot.general.keepawake);
        let general = plan.update.general.expect("general update");
        assert_eq!(general.keepawake, Some(true));
        assert!(general.preflight_model.is_none());
        assert!(plan.update.memory.is_none());
        assert!(plan.update.thread_episodic.is_none());
        assert!(keepawake_update_plan(None, true).is_none());
    }

    #[test]
    fn preflight_update_plan_updates_general_only() {
        let model_selection = GatewayMemoryModelSelection::custom("openai", "gpt-5.4");
        let plan = preflight_model_update_plan(Some(&snapshot(true)), model_selection.clone())
            .expect("plan");

        assert_eq!(plan.snapshot.general.preflight_model, model_selection);
        assert!(plan.snapshot.general.keepawake);
        let general = plan.update.general.expect("general update");
        assert!(general.keepawake.is_none());
        assert_eq!(general.preflight_model, Some(model_selection));
        assert!(plan.update.memory.is_none());
        assert!(plan.update.thread_episodic.is_none());
        assert!(preflight_model_update_plan(None, GatewayMemoryModelSelection::thread()).is_none());
    }

    #[test]
    fn thread_episodic_enabled_update_plan_does_not_touch_hidden_fields() {
        let mut current = snapshot(true);
        current.thread_episodic.indexing_enabled = false;
        current.thread_episodic.recall_enabled = false;

        let plan =
            thread_episodic_enabled_update_plan(Some(&current), false).expect("enabled plan");

        assert!(!plan.snapshot.thread_episodic.enabled);
        assert!(!plan.snapshot.thread_episodic.indexing_enabled);
        assert!(!plan.snapshot.thread_episodic.recall_enabled);
        assert_eq!(
            plan.update.thread_episodic,
            Some(GatewayThreadEpisodicSettingsUpdate::enabled(false))
        );
        assert!(thread_episodic_enabled_update_plan(None, true).is_none());
    }
}
