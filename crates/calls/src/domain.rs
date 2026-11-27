use pioneer_protocol::{
    ThreadCallEndReason as End, ThreadCallParticipant, ThreadCallPhase as Phase,
    ThreadCallSnapshot, ThreadCallTransition as Transition,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

pub const RING_SECONDS: i64 = 35;
pub const RECONNECT_SECONDS: i64 = 120;
pub const MAX_PARTICIPANTS: usize = 64;

#[derive(Clone, Serialize, Deserialize)]
pub struct Call {
    pub snapshot: ThreadCallSnapshot,
    pub gateway_id: String,
    pub created_at: i64,
    pub media_sessions: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    Applied,
    Unchanged,
    Conflict,
    Forbidden,
}

impl Call {
    pub fn new(
        gateway_id: String,
        workspace_id: String,
        thread_id: String,
        call_id: String,
        initiator_id: String,
        invitee_id: Option<String>,
        now: i64,
    ) -> Self {
        let phase = if invitee_id.is_some() {
            Phase::Ringing
        } else {
            Phase::Active
        };
        Self {
            gateway_id,
            created_at: now,
            media_sessions: BTreeMap::new(),
            snapshot: ThreadCallSnapshot {
                workspace_id,
                thread_id,
                call_id,
                revision: 1,
                created_at_unix: now,
                phase,
                participants: vec![participant(&initiator_id)],
                initiator_id,
                ring_until_unix: invitee_id.as_ref().map(|_| now + RING_SECONDS),
                invitee_id,
                end_reason: None,
            },
        }
    }

    pub fn is_active(&self) -> bool {
        self.snapshot.phase != Phase::Ended
    }

    pub fn participant(&self, principal: &str) -> Option<&ThreadCallParticipant> {
        self.snapshot
            .participants
            .iter()
            .find(|member| member.principal_id == principal)
    }

    pub fn visible_to(&self, principal: &str) -> bool {
        self.snapshot
            .invitee_id
            .as_ref()
            .is_none_or(|invitee| principal == invitee || principal == self.snapshot.initiator_id)
    }

    pub fn expire_ring(&mut self, now: i64) -> bool {
        if self
            .snapshot
            .ring_until_unix
            .is_some_and(|deadline| now >= deadline)
        {
            self.end(End::Missed);
            return true;
        }
        false
    }

    pub fn end(&mut self, reason: End) {
        self.snapshot.phase = Phase::Ended;
        self.snapshot.end_reason = Some(reason);
        self.snapshot.ring_until_unix = None;
        self.snapshot.participants.clear();
        self.media_sessions.clear();
        self.snapshot.revision += 1;
    }

    pub fn change(&mut self, principal: &str, revision: i64, transition: Transition) -> Change {
        if !self.visible_to(principal) {
            return Change::Forbidden;
        }
        let existing = self
            .snapshot
            .participants
            .iter()
            .position(|m| m.principal_id == principal);
        let phase = self.snapshot.phase;
        let already_done = match transition {
            Transition::Join => {
                existing.is_some_and(|index| !self.snapshot.participants[index].held)
            }
            Transition::Leave => existing.is_none(),
            Transition::Hold => {
                existing.is_some_and(|index| self.snapshot.participants[index].held)
            }
            Transition::Resume => {
                existing.is_some_and(|index| !self.snapshot.participants[index].held)
            }
            Transition::Decline => {
                phase == Phase::Ended && self.snapshot.end_reason == Some(End::Declined)
            }
        };
        if already_done {
            return Change::Unchanged;
        }
        if self.snapshot.revision != revision || phase == Phase::Ended {
            return Change::Conflict;
        }

        match transition {
            Transition::Join
                if existing.is_none() && self.snapshot.participants.len() < MAX_PARTICIPANTS =>
            {
                self.snapshot.participants.push(participant(principal));
                self.snapshot.phase = Phase::Active;
                self.snapshot.ring_until_unix = None;
            }
            Transition::Decline
                if phase == Phase::Ringing
                    && self.snapshot.invitee_id.as_deref() == Some(principal) =>
            {
                self.end(End::Declined);
                return Change::Applied;
            }
            Transition::Leave if existing.is_some() => {
                self.snapshot
                    .participants
                    .remove(existing.expect("existing participant"));
                self.media_sessions.remove(principal);
                if self.snapshot.participants.is_empty() {
                    self.end(End::Left);
                    return Change::Applied;
                }
            }
            Transition::Hold if phase == Phase::Active && existing.is_some() => {
                self.snapshot.participants[existing.expect("existing participant")].held = true;
            }
            Transition::Resume if existing.is_some() => {
                self.snapshot.participants[existing.expect("existing participant")] =
                    participant(principal);
            }
            _ => return Change::Conflict,
        }
        self.snapshot.revision += 1;
        Change::Applied
    }
}

fn participant(principal: &str) -> ThreadCallParticipant {
    ThreadCallParticipant {
        principal_id: principal.to_owned(),
        media_identity: Uuid::new_v4().to_string(),
        held: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn direct(now: i64) -> Call {
        Call::new(
            "gateway".into(),
            "workspace".into(),
            "thread".into(),
            "call".into(),
            "alice".into(),
            Some("bob".into()),
            now,
        )
    }

    #[test]
    fn invite_is_private_and_timeout_is_terminal() {
        let mut call = direct(100);
        assert_eq!(
            call.change("someone_else", 1, Transition::Join),
            Change::Forbidden
        );
        assert!(call.expire_ring(100 + RING_SECONDS));
        assert_eq!(call.snapshot.end_reason, Some(End::Missed));
        assert_eq!(call.change("bob", 1, Transition::Join), Change::Conflict);
    }

    #[test]
    fn hold_rotates_admission_and_last_departure_ends_call() {
        let mut call = direct(100);
        assert_eq!(call.change("bob", 1, Transition::Join), Change::Applied);
        assert_eq!(call.change("bob", 1, Transition::Join), Change::Unchanged);
        let original = call.participant("bob").unwrap().media_identity.clone();
        assert_eq!(call.change("bob", 2, Transition::Hold), Change::Applied);
        assert_eq!(call.change("bob", 3, Transition::Resume), Change::Applied);
        assert_ne!(call.participant("bob").unwrap().media_identity, original);
        assert_eq!(call.change("alice", 4, Transition::Leave), Change::Applied);
        assert!(call.is_active());
        assert_eq!(call.change("bob", 5, Transition::Leave), Change::Applied);
        assert!(!call.is_active());
    }
}
