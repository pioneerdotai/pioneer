use crate::session::ConnectionId;
use pioneer_protocol::{VoiceAudioFormat, VoiceError, VoiceErrorKind, VoiceSessionStartContext};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GatewayVoiceSessionState {
    Created,
    Recording,
    Finalizing,
    Transcribing,
}

impl GatewayVoiceSessionState {
    pub(crate) fn is_terminal_for_chunk_ingest(self) -> bool {
        matches!(self, Self::Finalizing | Self::Transcribing)
    }

    fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Created, Self::Recording)
                | (Self::Created, Self::Finalizing)
                | (Self::Recording, Self::Finalizing)
                | (Self::Finalizing, Self::Transcribing)
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GatewayVoiceSession {
    pub(crate) session_id: String,
    pub(crate) connection_id: ConnectionId,
    pub(crate) workspace_id: String,
    pub(crate) thread_id: String,
    pub(crate) turn_id: String,
    pub(crate) state: GatewayVoiceSessionState,
    pub(crate) audio_format: VoiceAudioFormat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GatewayVoiceSessionErrorKind {
    UnknownSession,
    DuplicateSession,
    OwnershipMismatch,
    InvalidTransition,
    StoreUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GatewayVoiceSessionError {
    pub(crate) kind: GatewayVoiceSessionErrorKind,
    pub(crate) message: String,
}

impl GatewayVoiceSessionError {
    pub(crate) fn into_voice_error(self) -> VoiceError {
        let kind = match self.kind {
            GatewayVoiceSessionErrorKind::UnknownSession
            | GatewayVoiceSessionErrorKind::OwnershipMismatch => VoiceErrorKind::InvalidSession,
            GatewayVoiceSessionErrorKind::DuplicateSession
            | GatewayVoiceSessionErrorKind::InvalidTransition => VoiceErrorKind::InvalidSession,
            GatewayVoiceSessionErrorKind::StoreUnavailable => VoiceErrorKind::GatewayBusy,
        };
        VoiceError {
            kind,
            message: self.message,
        }
    }
}

impl std::fmt::Display for GatewayVoiceSessionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for GatewayVoiceSessionError {}

#[derive(Clone, Default)]
pub(crate) struct GatewayVoiceSessionStore {
    sessions: Arc<Mutex<HashMap<String, GatewayVoiceSession>>>,
}

impl GatewayVoiceSessionStore {
    pub(crate) fn create_session(
        &self,
        session_id: impl Into<String>,
        connection_id: ConnectionId,
        context: VoiceSessionStartContext,
        audio_format: VoiceAudioFormat,
    ) -> Result<GatewayVoiceSession, GatewayVoiceSessionError> {
        let session_id = session_id.into();
        let mut sessions = self.lock_sessions()?;
        if sessions.contains_key(session_id.as_str()) {
            return Err(session_error(
                GatewayVoiceSessionErrorKind::DuplicateSession,
                format!("voice session `{session_id}` already exists"),
            ));
        }

        let session = GatewayVoiceSession {
            session_id: session_id.clone(),
            connection_id,
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            state: GatewayVoiceSessionState::Created,
            audio_format,
        };
        sessions.insert(session_id, session.clone());
        Ok(session)
    }

    pub(crate) fn lookup_session(
        &self,
        session_id: &str,
        connection_id: ConnectionId,
    ) -> Result<GatewayVoiceSession, GatewayVoiceSessionError> {
        let sessions = self.lock_sessions()?;
        let session = sessions.get(session_id).ok_or_else(|| {
            session_error(
                GatewayVoiceSessionErrorKind::UnknownSession,
                format!("voice session `{session_id}` is not active"),
            )
        })?;
        ensure_session_owner(session, connection_id)?;
        Ok(session.clone())
    }

    pub(crate) fn active_session_for_connection(
        &self,
        connection_id: ConnectionId,
    ) -> Option<GatewayVoiceSession> {
        let sessions = self.lock_sessions().ok()?;
        sessions
            .values()
            .find(|session| session.connection_id == connection_id)
            .cloned()
    }

    pub(crate) fn mark_recording(
        &self,
        session_id: &str,
        connection_id: ConnectionId,
    ) -> Result<GatewayVoiceSession, GatewayVoiceSessionError> {
        self.transition_session(
            session_id,
            connection_id,
            GatewayVoiceSessionState::Recording,
        )
    }

    pub(crate) fn mark_finalizing(
        &self,
        session_id: &str,
        connection_id: ConnectionId,
    ) -> Result<GatewayVoiceSession, GatewayVoiceSessionError> {
        self.transition_session(
            session_id,
            connection_id,
            GatewayVoiceSessionState::Finalizing,
        )
    }

    pub(crate) fn mark_transcribing(
        &self,
        session_id: &str,
        connection_id: ConnectionId,
    ) -> Result<GatewayVoiceSession, GatewayVoiceSessionError> {
        self.transition_session(
            session_id,
            connection_id,
            GatewayVoiceSessionState::Transcribing,
        )
    }

    pub(crate) fn remove_session(
        &self,
        session_id: &str,
        connection_id: ConnectionId,
    ) -> Result<GatewayVoiceSession, GatewayVoiceSessionError> {
        let mut sessions = self.lock_sessions()?;
        let session = sessions.get(session_id).ok_or_else(|| {
            session_error(
                GatewayVoiceSessionErrorKind::UnknownSession,
                format!("voice session `{session_id}` is not active"),
            )
        })?;
        ensure_session_owner(session, connection_id)?;
        Ok(sessions
            .remove(session_id)
            .expect("session exists after lookup"))
    }

    pub(crate) fn cleanup_connection(
        &self,
        connection_id: ConnectionId,
    ) -> Vec<GatewayVoiceSession> {
        let Ok(mut sessions) = self.lock_sessions() else {
            return Vec::new();
        };
        let session_ids = sessions
            .iter()
            .filter_map(|(session_id, session)| {
                (session.connection_id == connection_id).then(|| session_id.clone())
            })
            .collect::<Vec<_>>();

        session_ids
            .into_iter()
            .filter_map(|session_id| sessions.remove(session_id.as_str()))
            .collect()
    }

    fn transition_session(
        &self,
        session_id: &str,
        connection_id: ConnectionId,
        next_state: GatewayVoiceSessionState,
    ) -> Result<GatewayVoiceSession, GatewayVoiceSessionError> {
        let mut sessions = self.lock_sessions()?;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            session_error(
                GatewayVoiceSessionErrorKind::UnknownSession,
                format!("voice session `{session_id}` is not active"),
            )
        })?;
        ensure_session_owner(session, connection_id)?;
        if !session.state.can_transition_to(next_state) {
            return Err(session_error(
                GatewayVoiceSessionErrorKind::InvalidTransition,
                format!(
                    "voice session `{session_id}` cannot transition from {:?} to {:?}",
                    session.state, next_state
                ),
            ));
        }

        session.state = next_state;
        Ok(session.clone())
    }

    fn lock_sessions(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, HashMap<String, GatewayVoiceSession>>,
        GatewayVoiceSessionError,
    > {
        self.sessions.lock().map_err(|_| {
            session_error(
                GatewayVoiceSessionErrorKind::StoreUnavailable,
                "voice session store is unavailable",
            )
        })
    }
}

fn ensure_session_owner(
    session: &GatewayVoiceSession,
    connection_id: ConnectionId,
) -> Result<(), GatewayVoiceSessionError> {
    if session.connection_id == connection_id {
        return Ok(());
    }

    Err(session_error(
        GatewayVoiceSessionErrorKind::OwnershipMismatch,
        format!(
            "voice session `{}` belongs to a different connection",
            session.session_id
        ),
    ))
}

fn session_error(
    kind: GatewayVoiceSessionErrorKind,
    message: impl Into<String>,
) -> GatewayVoiceSessionError {
    GatewayVoiceSessionError {
        kind,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{VoiceAudioEncoding, VoiceAudioFormat};

    #[test]
    fn create_lookup_and_transitions_preserve_context() {
        let store = GatewayVoiceSessionStore::default();
        let context = test_context();

        let created = store
            .create_session("voice_session_1", 7, context.clone(), target_format())
            .expect("create");
        let recording = store
            .mark_recording("voice_session_1", 7)
            .expect("recording");
        let finalizing = store
            .mark_finalizing("voice_session_1", 7)
            .expect("finalizing");
        let transcribing = store
            .mark_transcribing("voice_session_1", 7)
            .expect("transcribing");
        let lookup = store.lookup_session("voice_session_1", 7).expect("lookup");

        assert_eq!(created.state, GatewayVoiceSessionState::Created);
        assert_eq!(recording.state, GatewayVoiceSessionState::Recording);
        assert_eq!(finalizing.state, GatewayVoiceSessionState::Finalizing);
        assert_eq!(transcribing.state, GatewayVoiceSessionState::Transcribing);
        assert_eq!(lookup.workspace_id, "workspace_1");
        assert_eq!(lookup.thread_id, "thread_1");
        assert_eq!(lookup.turn_id, "turn_1");
    }

    #[test]
    fn unknown_duplicate_foreign_and_invalid_transitions_are_typed() {
        let store = GatewayVoiceSessionStore::default();
        store
            .create_session("voice_session_1", 7, test_context(), target_format())
            .expect("create");

        let duplicate = store
            .create_session("voice_session_1", 7, test_context(), target_format())
            .expect_err("duplicate");
        let foreign = store
            .lookup_session("voice_session_1", 8)
            .expect_err("foreign owner");
        let invalid = store
            .mark_transcribing("voice_session_1", 7)
            .expect_err("invalid transition");
        let unknown = store
            .lookup_session("missing_session", 7)
            .expect_err("unknown");

        assert_eq!(
            duplicate.kind,
            GatewayVoiceSessionErrorKind::DuplicateSession
        );
        assert_eq!(
            foreign.kind,
            GatewayVoiceSessionErrorKind::OwnershipMismatch
        );
        assert_eq!(
            invalid.kind,
            GatewayVoiceSessionErrorKind::InvalidTransition
        );
        assert_eq!(unknown.kind, GatewayVoiceSessionErrorKind::UnknownSession);
        assert_eq!(
            foreign.into_voice_error().kind,
            VoiceErrorKind::InvalidSession
        );
    }

    #[test]
    fn remove_and_cleanup_by_connection_release_sessions() {
        let store = GatewayVoiceSessionStore::default();
        store
            .create_session("voice_session_1", 7, test_context(), target_format())
            .expect("create 1");
        store
            .create_session("voice_session_2", 7, test_context(), target_format())
            .expect("create 2");
        store
            .create_session("voice_session_3", 8, test_context(), target_format())
            .expect("create 3");

        let removed = store
            .remove_session("voice_session_1", 7)
            .expect("remove explicit");
        let cleaned = store.cleanup_connection(7);

        assert_eq!(removed.session_id, "voice_session_1");
        assert_eq!(cleaned.len(), 1);
        assert_eq!(cleaned[0].session_id, "voice_session_2");
        assert!(store.lookup_session("voice_session_1", 7).is_err());
        assert!(store.lookup_session("voice_session_2", 7).is_err());
        assert!(store.lookup_session("voice_session_3", 8).is_ok());
    }

    fn test_context() -> VoiceSessionStartContext {
        VoiceSessionStartContext {
            workspace_id: "workspace_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            turn_id: "turn_1".to_owned(),
        }
    }

    fn target_format() -> VoiceAudioFormat {
        VoiceAudioFormat {
            sample_rate_hz: 16_000,
            channels: 1,
            encoding: VoiceAudioEncoding::PcmS16Le,
        }
    }
}
