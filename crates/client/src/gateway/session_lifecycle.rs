//! Credential-free planning for a Gateway device session lifecycle.

use pioneer_protocol::{AuthSessionId, DeviceId, GatewayId};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GatewaySessionMetadata {
    pub gateway_id: GatewayId,
    pub device_id: DeviceId,
    pub session_id: AuthSessionId,
    pub refresh_generation: u64,
    pub refresh_expires_at_unix: u64,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTerminalReason {
    AuthenticationRequired,
    SessionRevoked,
    SessionExpired,
    SessionCompromised,
    GatewayIdentityMismatch,
    SecureStorageFailed,
    RefreshOutcomeUnknown,
    RefreshCredentialInvalid,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum SessionLifecycleState {
    NoSession,
    NeedsDeviceActivation,
    Refreshing {
        metadata: GatewaySessionMetadata,
        intent_id: u64,
        previous_connection_generation: Option<u64>,
    },
    AwaitingSecureStorage {
        metadata: GatewaySessionMetadata,
        intent_id: u64,
        candidate_connection_generation: u64,
        previous_connection_generation: Option<u64>,
        access_expires_at_unix: u64,
    },
    Connecting {
        metadata: GatewaySessionMetadata,
        connection_generation: u64,
        previous_connection_generation: Option<u64>,
        access_expires_at_unix: u64,
    },
    Active {
        metadata: GatewaySessionMetadata,
        connection_generation: u64,
        access_expires_at_unix: u64,
    },
    Terminal {
        metadata: Option<GatewaySessionMetadata>,
        reason: SessionTerminalReason,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum SessionLifecycleEvent {
    NoStoredSession,
    DeviceActivationRequired,
    StoredSessionLoaded(GatewaySessionMetadata),
    RefreshGrantReceived {
        intent_id: u64,
        metadata: GatewaySessionMetadata,
        access_expires_at_unix: u64,
    },
    SecureStorageCommitted {
        intent_id: u64,
    },
    SecureStorageFailed {
        intent_id: u64,
    },
    ConnectionEstablished {
        generation: u64,
    },
    ConnectionTransportFailed {
        generation: u64,
        now_unix: u64,
    },
    ConnectionEventObserved {
        generation: u64,
    },
    ClockAdvanced {
        now_unix: u64,
        refresh_leeway_seconds: u64,
    },
    RefreshTransportLost {
        intent_id: u64,
    },
    AuthFailed {
        reason: SessionTerminalReason,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum SessionLifecycleEffect {
    None,
    BeginDeviceActivation,
    BeginRefresh {
        session_id: AuthSessionId,
        intent_id: u64,
    },
    PersistRefreshBeforeAccess {
        intent_id: u64,
        candidate_connection_generation: u64,
    },
    ConnectWithEphemeralAccess {
        connection_generation: u64,
    },
    RetryConnection {
        connection_generation: u64,
    },
    SwitchConnection {
        active_connection_generation: u64,
        close_connection_generation: Option<u64>,
    },
    IgnoreStaleConnectionEvent {
        generation: u64,
    },
    Stop {
        reason: SessionTerminalReason,
    },
}

#[derive(Clone, Debug)]
pub struct SessionLifecycle {
    state: SessionLifecycleState,
    next_intent_id: u64,
    next_connection_generation: u64,
}

impl Default for SessionLifecycle {
    fn default() -> Self {
        Self {
            state: SessionLifecycleState::NoSession,
            next_intent_id: 1,
            next_connection_generation: 1,
        }
    }
}

impl SessionLifecycle {
    pub fn state(&self) -> &SessionLifecycleState {
        &self.state
    }

    pub fn reduce(&mut self, event: SessionLifecycleEvent) -> SessionLifecycleEffect {
        match event {
            SessionLifecycleEvent::NoStoredSession => {
                self.state = SessionLifecycleState::NoSession;
                SessionLifecycleEffect::None
            }
            SessionLifecycleEvent::DeviceActivationRequired => {
                self.state = SessionLifecycleState::NeedsDeviceActivation;
                SessionLifecycleEffect::BeginDeviceActivation
            }
            SessionLifecycleEvent::StoredSessionLoaded(metadata) => {
                self.begin_refresh(metadata, None)
            }
            SessionLifecycleEvent::RefreshGrantReceived {
                intent_id,
                metadata,
                access_expires_at_unix,
            } => {
                let SessionLifecycleState::Refreshing {
                    intent_id: active_intent,
                    previous_connection_generation,
                    ..
                } = &self.state
                else {
                    return SessionLifecycleEffect::None;
                };
                if *active_intent != intent_id {
                    return SessionLifecycleEffect::None;
                }
                let previous_connection_generation = *previous_connection_generation;
                let candidate = self.take_connection_generation();
                self.state = SessionLifecycleState::AwaitingSecureStorage {
                    metadata,
                    intent_id,
                    candidate_connection_generation: candidate,
                    previous_connection_generation,
                    access_expires_at_unix,
                };
                SessionLifecycleEffect::PersistRefreshBeforeAccess {
                    intent_id,
                    candidate_connection_generation: candidate,
                }
            }
            SessionLifecycleEvent::SecureStorageCommitted { intent_id } => {
                let SessionLifecycleState::AwaitingSecureStorage {
                    metadata,
                    intent_id: active_intent,
                    candidate_connection_generation,
                    previous_connection_generation,
                    access_expires_at_unix,
                } = &self.state
                else {
                    return SessionLifecycleEffect::None;
                };
                if *active_intent != intent_id {
                    return SessionLifecycleEffect::None;
                }
                let generation = *candidate_connection_generation;
                self.state = SessionLifecycleState::Connecting {
                    metadata: metadata.clone(),
                    connection_generation: generation,
                    previous_connection_generation: *previous_connection_generation,
                    access_expires_at_unix: *access_expires_at_unix,
                };
                SessionLifecycleEffect::ConnectWithEphemeralAccess {
                    connection_generation: generation,
                }
            }
            SessionLifecycleEvent::SecureStorageFailed { intent_id } => {
                if !self.matches_intent(intent_id) {
                    return SessionLifecycleEffect::None;
                }
                self.enter_terminal(SessionTerminalReason::SecureStorageFailed)
            }
            SessionLifecycleEvent::ConnectionEstablished { generation } => {
                if matches!(
                    &self.state,
                    SessionLifecycleState::Active {
                        connection_generation,
                        ..
                    } if *connection_generation == generation
                ) {
                    return SessionLifecycleEffect::SwitchConnection {
                        active_connection_generation: generation,
                        close_connection_generation: None,
                    };
                }
                let SessionLifecycleState::Connecting {
                    metadata,
                    connection_generation,
                    previous_connection_generation,
                    access_expires_at_unix,
                } = &self.state
                else {
                    return self.ignore_stale(generation);
                };
                if *connection_generation != generation {
                    return self.ignore_stale(generation);
                }
                let previous = *previous_connection_generation;
                self.state = SessionLifecycleState::Active {
                    metadata: metadata.clone(),
                    connection_generation: generation,
                    access_expires_at_unix: *access_expires_at_unix,
                };
                SessionLifecycleEffect::SwitchConnection {
                    active_connection_generation: generation,
                    close_connection_generation: previous,
                }
            }
            SessionLifecycleEvent::ConnectionTransportFailed {
                generation,
                now_unix,
            } => {
                let Some((active_generation, access_expires_at_unix)) = self.expected_connection()
                else {
                    return self.ignore_stale(generation);
                };
                if active_generation != generation {
                    return self.ignore_stale(generation);
                }
                if now_unix < access_expires_at_unix {
                    SessionLifecycleEffect::RetryConnection {
                        connection_generation: generation,
                    }
                } else {
                    let metadata = self.metadata().cloned();
                    match metadata {
                        Some(metadata) => self.begin_refresh(metadata, None),
                        None => SessionLifecycleEffect::None,
                    }
                }
            }
            SessionLifecycleEvent::ConnectionEventObserved { generation } => {
                if self
                    .expected_connection()
                    .is_some_and(|(expected, _)| expected == generation)
                {
                    SessionLifecycleEffect::None
                } else {
                    self.ignore_stale(generation)
                }
            }
            SessionLifecycleEvent::ClockAdvanced {
                now_unix,
                refresh_leeway_seconds,
            } => {
                let SessionLifecycleState::Active {
                    metadata,
                    connection_generation,
                    access_expires_at_unix,
                } = &self.state
                else {
                    return SessionLifecycleEffect::None;
                };
                if now_unix.saturating_add(refresh_leeway_seconds) < *access_expires_at_unix {
                    return SessionLifecycleEffect::None;
                }
                self.begin_refresh(metadata.clone(), Some(*connection_generation))
            }
            SessionLifecycleEvent::RefreshTransportLost { intent_id } => {
                let SessionLifecycleState::Refreshing {
                    metadata,
                    intent_id: active_intent,
                    ..
                } = &self.state
                else {
                    return SessionLifecycleEffect::None;
                };
                if *active_intent != intent_id {
                    return SessionLifecycleEffect::None;
                }
                SessionLifecycleEffect::BeginRefresh {
                    session_id: metadata.session_id.clone(),
                    intent_id,
                }
            }
            SessionLifecycleEvent::AuthFailed { reason } => self.enter_terminal(reason),
        }
    }

    fn begin_refresh(
        &mut self,
        metadata: GatewaySessionMetadata,
        previous_connection_generation: Option<u64>,
    ) -> SessionLifecycleEffect {
        if matches!(&self.state, SessionLifecycleState::Refreshing { .. }) {
            return SessionLifecycleEffect::None;
        }
        let intent_id = self.next_intent_id;
        self.next_intent_id = self.next_intent_id.saturating_add(1);
        let session_id = metadata.session_id.clone();
        self.state = SessionLifecycleState::Refreshing {
            metadata,
            intent_id,
            previous_connection_generation,
        };
        SessionLifecycleEffect::BeginRefresh {
            session_id,
            intent_id,
        }
    }

    fn take_connection_generation(&mut self) -> u64 {
        let generation = self.next_connection_generation;
        self.next_connection_generation = self.next_connection_generation.saturating_add(1);
        generation
    }

    fn matches_intent(&self, intent_id: u64) -> bool {
        match &self.state {
            SessionLifecycleState::Refreshing {
                intent_id: active, ..
            }
            | SessionLifecycleState::AwaitingSecureStorage {
                intent_id: active, ..
            } => *active == intent_id,
            _ => false,
        }
    }

    fn metadata(&self) -> Option<&GatewaySessionMetadata> {
        match &self.state {
            SessionLifecycleState::Refreshing { metadata, .. }
            | SessionLifecycleState::AwaitingSecureStorage { metadata, .. }
            | SessionLifecycleState::Connecting { metadata, .. }
            | SessionLifecycleState::Active { metadata, .. } => Some(metadata),
            SessionLifecycleState::Terminal { metadata, .. } => metadata.as_ref(),
            SessionLifecycleState::NoSession | SessionLifecycleState::NeedsDeviceActivation => None,
        }
    }

    fn expected_connection(&self) -> Option<(u64, u64)> {
        match &self.state {
            SessionLifecycleState::Connecting {
                connection_generation,
                access_expires_at_unix,
                ..
            }
            | SessionLifecycleState::Active {
                connection_generation,
                access_expires_at_unix,
                ..
            } => Some((*connection_generation, *access_expires_at_unix)),
            _ => None,
        }
    }

    fn ignore_stale(&self, generation: u64) -> SessionLifecycleEffect {
        SessionLifecycleEffect::IgnoreStaleConnectionEvent { generation }
    }

    fn enter_terminal(&mut self, reason: SessionTerminalReason) -> SessionLifecycleEffect {
        let metadata = self.metadata().cloned();
        self.state = SessionLifecycleState::Terminal { metadata, reason };
        SessionLifecycleEffect::Stop { reason }
    }
}

pub fn terminal_reason_from_auth_code(code: &str) -> Option<SessionTerminalReason> {
    match code {
        "authentication_terminal" => Some(SessionTerminalReason::AuthenticationRequired),
        "session_revoked" => Some(SessionTerminalReason::SessionRevoked),
        "session_expired" => Some(SessionTerminalReason::SessionExpired),
        "session_compromised" => Some(SessionTerminalReason::SessionCompromised),
        "gateway_identity_mismatch" => Some(SessionTerminalReason::GatewayIdentityMismatch),
        "secure_storage_failed" => Some(SessionTerminalReason::SecureStorageFailed),
        "invalid_credential" => Some(SessionTerminalReason::RefreshCredentialInvalid),
        _ => None,
    }
}

pub fn auth_code_requires_refresh(code: &str) -> bool {
    matches!(code, "access_expired" | "credential_expired")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(generation: u64) -> GatewaySessionMetadata {
        GatewaySessionMetadata {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            device_id: DeviceId::new("D00000000000000000001").unwrap(),
            session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
            refresh_generation: generation,
            refresh_expires_at_unix: 10_000,
        }
    }

    fn activate(lifecycle: &mut SessionLifecycle, access_expiry: u64) -> u64 {
        let SessionLifecycleEffect::BeginRefresh { intent_id, .. } =
            lifecycle.reduce(SessionLifecycleEvent::StoredSessionLoaded(metadata(0)))
        else {
            panic!("refresh expected")
        };
        let SessionLifecycleEffect::PersistRefreshBeforeAccess {
            candidate_connection_generation,
            ..
        } = lifecycle.reduce(SessionLifecycleEvent::RefreshGrantReceived {
            intent_id,
            metadata: metadata(1),
            access_expires_at_unix: access_expiry,
        })
        else {
            panic!("storage expected")
        };
        assert!(matches!(
            lifecycle.reduce(SessionLifecycleEvent::SecureStorageCommitted { intent_id }),
            SessionLifecycleEffect::ConnectWithEphemeralAccess { .. }
        ));
        assert!(matches!(
            lifecycle.reduce(SessionLifecycleEvent::ConnectionEstablished {
                generation: candidate_connection_generation,
            }),
            SessionLifecycleEffect::SwitchConnection { .. }
        ));
        candidate_connection_generation
    }

    #[test]
    fn refresh_leeway_boundary_is_exact_and_serialized() {
        let mut lifecycle = SessionLifecycle::default();
        let generation = activate(&mut lifecycle, 1_000);
        assert_eq!(
            lifecycle.reduce(SessionLifecycleEvent::ClockAdvanced {
                now_unix: 899,
                refresh_leeway_seconds: 100,
            }),
            SessionLifecycleEffect::None
        );
        let first = lifecycle.reduce(SessionLifecycleEvent::ClockAdvanced {
            now_unix: 900,
            refresh_leeway_seconds: 100,
        });
        assert!(matches!(first, SessionLifecycleEffect::BeginRefresh { .. }));
        assert_eq!(
            lifecycle.reduce(SessionLifecycleEvent::ClockAdvanced {
                now_unix: 901,
                refresh_leeway_seconds: 100,
            }),
            SessionLifecycleEffect::None
        );
        assert!(matches!(
            lifecycle.state(),
            SessionLifecycleState::Refreshing {
                previous_connection_generation: Some(value),
                ..
            } if *value == generation
        ));
    }

    #[test]
    fn storage_commit_precedes_access_activation_and_switch_closes_old() {
        let mut lifecycle = SessionLifecycle::default();
        let old = activate(&mut lifecycle, 1_000);
        let SessionLifecycleEffect::BeginRefresh { intent_id, .. } =
            lifecycle.reduce(SessionLifecycleEvent::ClockAdvanced {
                now_unix: 950,
                refresh_leeway_seconds: 100,
            })
        else {
            panic!("replacement refresh expected")
        };
        let persist = lifecycle.reduce(SessionLifecycleEvent::RefreshGrantReceived {
            intent_id,
            metadata: metadata(2),
            access_expires_at_unix: 2_000,
        });
        let SessionLifecycleEffect::PersistRefreshBeforeAccess {
            candidate_connection_generation: candidate,
            ..
        } = persist
        else {
            panic!("persist expected")
        };
        assert!(matches!(
            lifecycle.reduce(SessionLifecycleEvent::SecureStorageCommitted { intent_id }),
            SessionLifecycleEffect::ConnectWithEphemeralAccess {
                connection_generation
            } if connection_generation == candidate
        ));
        assert_eq!(
            lifecycle.reduce(SessionLifecycleEvent::ConnectionEstablished {
                generation: candidate,
            }),
            SessionLifecycleEffect::SwitchConnection {
                active_connection_generation: candidate,
                close_connection_generation: Some(old),
            }
        );
    }

    #[test]
    fn response_loss_retries_the_same_intent_while_terminal_auth_stops() {
        let mut lifecycle = SessionLifecycle::default();
        let SessionLifecycleEffect::BeginRefresh { intent_id, .. } =
            lifecycle.reduce(SessionLifecycleEvent::StoredSessionLoaded(metadata(0)))
        else {
            panic!("refresh expected")
        };
        assert_eq!(
            lifecycle.reduce(SessionLifecycleEvent::RefreshTransportLost { intent_id }),
            SessionLifecycleEffect::BeginRefresh {
                session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
                intent_id,
            }
        );
        assert!(matches!(
            lifecycle.state(),
            SessionLifecycleState::Refreshing {
                intent_id: active,
                ..
            } if *active == intent_id
        ));
        assert_eq!(
            lifecycle.reduce(SessionLifecycleEvent::AuthFailed {
                reason: SessionTerminalReason::SessionCompromised,
            }),
            SessionLifecycleEffect::Stop {
                reason: SessionTerminalReason::SessionCompromised,
            }
        );
    }

    #[test]
    fn stale_connection_events_cannot_replace_new_generation() {
        let mut lifecycle = SessionLifecycle::default();
        let current = activate(&mut lifecycle, 1_000);
        assert_eq!(
            lifecycle.reduce(SessionLifecycleEvent::ConnectionEventObserved {
                generation: current.saturating_sub(1),
            }),
            SessionLifecycleEffect::IgnoreStaleConnectionEvent {
                generation: current.saturating_sub(1),
            }
        );
        assert!(matches!(
            lifecycle.state(),
            SessionLifecycleState::Active {
                connection_generation,
                ..
            } if *connection_generation == current
        ));
    }

    #[test]
    fn reconnect_with_unexpired_access_keeps_active_generation() {
        let mut lifecycle = SessionLifecycle::default();
        let current = activate(&mut lifecycle, 1_000);
        assert_eq!(
            lifecycle.reduce(SessionLifecycleEvent::ConnectionTransportFailed {
                generation: current,
                now_unix: 900,
            }),
            SessionLifecycleEffect::RetryConnection {
                connection_generation: current,
            }
        );
        assert_eq!(
            lifecycle.reduce(SessionLifecycleEvent::ConnectionEstablished {
                generation: current,
            }),
            SessionLifecycleEffect::SwitchConnection {
                active_connection_generation: current,
                close_connection_generation: None,
            }
        );
    }

    #[test]
    fn connection_failure_after_access_expiry_starts_refresh() {
        let mut lifecycle = SessionLifecycle::default();
        let current = activate(&mut lifecycle, 1_000);

        let effect = lifecycle.reduce(SessionLifecycleEvent::ConnectionTransportFailed {
            generation: current,
            now_unix: 1_000,
        });

        assert!(matches!(
            effect,
            SessionLifecycleEffect::BeginRefresh { .. }
        ));
        assert!(matches!(
            lifecycle.state(),
            SessionLifecycleState::Refreshing { .. }
        ));
    }

    #[test]
    fn shared_lifecycle_debug_and_state_have_no_secret_fields() {
        let debug = format!("{:?}", SessionLifecycle::default());
        for forbidden in ["access_token", "refresh_token", "activation_code"] {
            assert!(!debug.contains(forbidden));
        }
    }

    #[test]
    fn access_expiry_requires_refresh_without_terminating_the_session() {
        for code in ["access_expired", "credential_expired"] {
            assert!(auth_code_requires_refresh(code));
            assert_eq!(terminal_reason_from_auth_code(code), None);
        }
        assert!(!auth_code_requires_refresh("session_expired"));
        assert_eq!(
            terminal_reason_from_auth_code("authentication_terminal"),
            Some(SessionTerminalReason::AuthenticationRequired)
        );
        assert_eq!(
            terminal_reason_from_auth_code("session_expired"),
            Some(SessionTerminalReason::SessionExpired)
        );
    }
}
