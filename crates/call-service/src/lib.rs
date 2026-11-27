use anyhow::{Result, anyhow};
use pioneer_calls::{Call, Change, RECONNECT_SECONDS, livekit::LiveKit};
use pioneer_crud::{CrudStore, ThreadCallScope};

use pioneer_postgres_pools::WorkSignal;
use pioneer_protocol::{
    ThreadCallAction, ThreadCallEndReason, ThreadCallHistory, ThreadCallPhase, ThreadCallResponse,
    ThreadCallTransition,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    time::Duration,
};
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const PRESENCE_INTERVAL: Duration = Duration::from_secs(3);

pub struct Calls {
    pub media: LiveKit,
    // Gateway owns the database and the local sidecar. Serialize signaling
    // admission and room changes so a hold cannot race a new RTC handshake.
    serial: Mutex<()>,
    presence: Mutex<Presence>,
    dirty: Mutex<HashSet<String>>,
    changed: WorkSignal,
    pub shutdown: CancellationToken,
}

pub enum CallError {
    Missing,
    Conflict,
    Invalid,
    Backend(anyhow::Error),
}

impl From<anyhow::Error> for CallError {
    fn from(error: anyhow::Error) -> Self {
        Self::Backend(error)
    }
}

impl Calls {
    pub fn new(media: LiveKit) -> Self {
        Self {
            media,
            serial: Mutex::new(()),
            presence: Mutex::new(Presence::default()),
            dirty: Mutex::default(),
            changed: WorkSignal::default(),
            shutdown: CancellationToken::new(),
        }
    }

    async fn save(
        &self,
        store: &CrudStore,
        call: &Call,
        revision: Option<i64>,
        actor: Option<&str>,
    ) -> Result<bool> {
        let new_hint = self
            .dirty
            .lock()
            .await
            .insert(call.snapshot.call_id.clone());
        let change = self
            .changed
            .changing()
            .with_keys(std::slice::from_ref(&call.snapshot.call_id));
        let saved = store.save_thread_call(call, revision, actor).await?;
        change.completed(saved);
        if !saved && new_hint {
            self.dirty.lock().await.remove(&call.snapshot.call_id);
        }
        Ok(saved)
    }

    pub async fn close_inherited(&self, store: &CrudStore) -> Result<()> {
        let store = store.with_maintenance_access();
        let mut cursor = None;
        loop {
            let page = store.active_thread_call_page(cursor.as_deref()).await?;
            if page.is_empty() {
                return Ok(());
            }
            for mut call in page {
                cursor = Some(call.snapshot.call_id.clone());
                let revision = call.snapshot.revision;
                if !call.expire_ring(now()) {
                    call.end(ThreadCallEndReason::GatewayRestarted);
                }
                self.save(&store, &call, Some(revision), None).await?;
            }
            tokio::task::yield_now().await;
        }
    }

    pub async fn process(
        &self,
        store: &CrudStore,
        scope: &ThreadCallScope<'_>,
        actor: &str,
        session_id: &str,
        action: ThreadCallAction,
        relay_url: Option<&str>,
    ) -> std::result::Result<ThreadCallResponse, CallError> {
        if scope.workspace.is_empty() || scope.thread.is_empty() || self.shutdown.is_cancelled() {
            return Err(CallError::Invalid);
        }
        let _serial = self.serial.lock().await;
        let reconciliation = matches!(
            action,
            ThreadCallAction::Restore { .. }
                | ThreadCallAction::Disconnect { .. }
                | ThreadCallAction::Resume { .. }
        );
        if !store.is_thread_call_member(scope, actor).await? {
            return if reconciliation {
                Ok(no_membership())
            } else {
                Err(CallError::Missing)
            };
        }
        let disconnect = matches!(action, ThreadCallAction::Disconnect { .. });
        let resume = matches!(action, ThreadCallAction::Resume { .. });
        let mut resumed_identity = None;
        let (action, resume_key) = match action {
            ThreadCallAction::Resume {
                call_id,
                media_identity,
                resume_key,
            } => (
                ThreadCallAction::Restore {
                    call_id,
                    media_identity,
                },
                Some(resume_key),
            ),
            ThreadCallAction::Disconnect {
                call_id,
                media_identity,
                resume_key,
            } => (
                ThreadCallAction::Restore {
                    call_id,
                    media_identity,
                },
                resume_key,
            ),
            action => (action, None),
        };
        let action = match action {
            ThreadCallAction::Restore {
                call_id,
                media_identity,
            } => {
                let successor = resume_key
                    .as_deref()
                    .map(|key| resume_media_identity(&media_identity, key))
                    .transpose()?;
                let call = match self.scoped_call(store, scope, actor, &call_id).await {
                    Ok(call) => call,
                    Err(CallError::Missing) => return Ok(no_membership()),
                    Err(error) => return Err(error),
                };
                let Some(member) = membership_for_reconciliation(
                    &call,
                    actor,
                    session_id,
                    &media_identity,
                    successor.as_deref(),
                ) else {
                    return Ok(no_membership());
                };
                if expired_ring(&call) {
                    return Ok(no_membership());
                }
                if !store
                    .can_access_thread_call_media(&call, actor, now())
                    .await?
                {
                    return Ok(no_membership());
                }
                let transition = if disconnect {
                    ThreadCallTransition::Leave
                } else if resume {
                    if successor.as_ref() == Some(&member.media_identity) {
                        return Ok(if member.held {
                            no_membership()
                        } else {
                            response(&call)
                        });
                    }
                    if !member.held
                        || self
                            .presence
                            .lock()
                            .await
                            .hold_expired(&member.media_identity, Instant::now())
                    {
                        return Ok(no_membership());
                    }
                    resumed_identity = successor;
                    ThreadCallTransition::Resume
                } else if member.held {
                    return if self
                        .presence
                        .lock()
                        .await
                        .renew_hold(&media_identity, Instant::now())
                    {
                        Ok(response(&call))
                    } else {
                        Ok(no_membership())
                    };
                } else {
                    return self
                        .connect(store, &call, actor, session_id, relay_url, true)
                        .await;
                };
                ThreadCallAction::Change {
                    call_id,
                    revision: call.snapshot.revision,
                    transition,
                }
            }
            action => action,
        };
        match action {
            ThreadCallAction::Restore { .. }
            | ThreadCallAction::Disconnect { .. }
            | ThreadCallAction::Resume { .. } => {
                unreachable!("membership reconciliation is normalized under the serial lock")
            }
            ThreadCallAction::Status { call_id } => {
                let call = if let Some(id) = call_id {
                    store.thread_call(&id).await?
                } else if let Some(call) = store.active_thread_call(scope.thread).await? {
                    Some(call)
                } else {
                    store.latest_thread_call(scope, actor).await?
                };
                Ok(ThreadCallResponse {
                    call: call
                        .filter(|call| matches_scope(call, scope) && call.visible_to(actor))
                        .map(|call| call.snapshot),
                    credentials: None,
                    history: None,
                })
            }
            ThreadCallAction::History { before_call_id } => {
                let before = if let Some(id) = before_call_id {
                    Some(self.scoped_call(store, scope, actor, &id).await?)
                } else {
                    None
                };
                let (calls, next_before_call_id) = store
                    .thread_call_history(scope, before.as_ref(), actor)
                    .await?;
                Ok(ThreadCallResponse {
                    call: None,
                    credentials: None,
                    history: Some(ThreadCallHistory {
                        calls: calls.into_iter().map(|call| call.snapshot).collect(),
                        next_before_call_id,
                    }),
                })
            }
            ThreadCallAction::Start { request_key } => {
                let request_key = Uuid::parse_str(&request_key).map_err(|_| CallError::Invalid)?;
                let id = hex::encode(Sha256::digest(
                    serde_json::to_vec(&(
                        scope.gateway,
                        scope.thread,
                        actor,
                        request_key.to_string(),
                    ))
                    .map_err(anyhow::Error::from)?,
                ));
                if let Some(call) = store.thread_call(&id).await? {
                    if matches_scope(&call, scope) && call.visible_to(actor) {
                        return Ok(response(&call));
                    }
                    return Err(CallError::Missing);
                }
                if store.active_thread_call(scope.thread).await?.is_some() {
                    return Err(CallError::Conflict);
                }
                self.check_other_calls(store, actor, &id, false).await?;
                let people = store.classify_thread_call(scope).await?;
                let invitee = match people.as_slice() {
                    [first, second] if first == actor => Some(second.clone()),
                    [first, second] if second == actor => Some(first.clone()),
                    [_, _, _] => None,
                    _ => return Err(CallError::Conflict),
                };
                let mut call = Call::new(
                    scope.gateway.into(),
                    scope.workspace.into(),
                    scope.thread.into(),
                    id,
                    actor.into(),
                    invitee,
                    now(),
                );
                call.created_at = chrono::Utc::now().timestamp_micros();
                call.media_sessions.insert(actor.into(), session_id.into());
                self.media.ensure_running().await?;
                self.media.create_room(&call.snapshot.call_id).await?;
                if !self.save(store, &call, None, Some(actor)).await? {
                    self.media.delete_room(&call.snapshot.call_id).await?;
                    return Err(CallError::Conflict);
                }
                Ok(response(&call))
            }
            ThreadCallAction::KeepHeld { call_id } => {
                let call = self.scoped_call(store, scope, actor, &call_id).await?;
                let member = call
                    .participant(actor)
                    .filter(|member| member.held)
                    .ok_or(CallError::Conflict)?;
                if call.media_sessions.get(actor).map(String::as_str) != Some(session_id)
                    || !store
                        .can_access_thread_call_media(&call, actor, now())
                        .await?
                {
                    return Err(CallError::Missing);
                }
                if !self
                    .presence
                    .lock()
                    .await
                    .renew_hold(&member.media_identity, Instant::now())
                {
                    return Err(CallError::Conflict);
                }
                Ok(response(&call))
            }
            ThreadCallAction::Connect { call_id } => {
                let call = self.scoped_call(store, scope, actor, &call_id).await?;
                self.connect(store, &call, actor, session_id, relay_url, false)
                    .await
            }
            ThreadCallAction::Change {
                call_id,
                revision,
                transition,
            } => {
                let mut call = self.scoped_call(store, scope, actor, &call_id).await?;
                if expired_ring(&call) {
                    return Err(CallError::Conflict);
                }
                let expected = call.snapshot.revision;
                let previous = call.participant(actor).cloned();
                if matches!(
                    transition,
                    ThreadCallTransition::Hold | ThreadCallTransition::Resume
                ) && let Some(member) = &previous
                {
                    if call.media_sessions.get(actor).map(String::as_str) != Some(session_id) {
                        return Err(CallError::Missing);
                    }
                    if member.held
                        && self
                            .presence
                            .lock()
                            .await
                            .hold_expired(&member.media_identity, Instant::now())
                    {
                        return Err(CallError::Conflict);
                    }
                }
                match call.change(actor, revision, transition) {
                    Change::Conflict => return Err(CallError::Conflict),
                    Change::Forbidden => return Err(CallError::Missing),
                    Change::Unchanged => return Ok(response(&call)),
                    Change::Applied => {}
                }
                if let Some(identity) = resumed_identity
                    && let Some(member) = call
                        .snapshot
                        .participants
                        .iter_mut()
                        .find(|member| member.principal_id == actor)
                {
                    member.media_identity = identity;
                }
                match transition {
                    ThreadCallTransition::Join | ThreadCallTransition::Resume => {
                        self.check_other_calls(store, actor, &call_id, false)
                            .await?;
                        call.media_sessions.insert(actor.into(), session_id.into());
                    }
                    ThreadCallTransition::Hold => {
                        self.check_other_calls(store, actor, &call_id, true).await?
                    }
                    _ => {}
                }
                // Do the media-side restriction before committing the change.
                // The serial signaling gate prevents re-admission until commit.
                if !call.is_active() {
                    if previous.as_ref().is_none_or(|member| !member.held) {
                        self.media.ensure_running().await?;
                        self.media.delete_room(&call_id).await?;
                    }
                } else if matches!(
                    transition,
                    ThreadCallTransition::Hold | ThreadCallTransition::Leave
                ) && let Some(previous) = &previous
                    && !previous.held
                {
                    self.media.ensure_running().await?;
                    self.media.eject(&call_id, &previous.media_identity).await?;
                }
                if transition == ThreadCallTransition::Hold
                    && let Some(member) = &previous
                {
                    // Seed before dispatch. If the commit succeeds after the
                    // request is cancelled, reconciliation still has the lease.
                    self.presence
                        .lock()
                        .await
                        .hold(&member.media_identity, Instant::now());
                }
                if !self.save(store, &call, Some(expected), Some(actor)).await? {
                    return Err(CallError::Conflict);
                }
                if let Some(previous) = previous
                    && call
                        .participant(actor)
                        .is_none_or(|member| member.media_identity != previous.media_identity)
                {
                    self.presence.lock().await.remove(&previous.media_identity);
                }
                Ok(response(&call))
            }
        }
    }

    async fn connect(
        &self,
        store: &CrudStore,
        call: &Call,
        actor: &str,
        session_id: &str,
        relay_url: Option<&str>,
        reconciliation: bool,
    ) -> std::result::Result<ThreadCallResponse, CallError> {
        let call_id = &call.snapshot.call_id;
        if !call.is_active() || expired_ring(call) {
            return Err(CallError::Conflict);
        }
        let member = call
            .participant(actor)
            .filter(|member| !member.held)
            .ok_or(CallError::Conflict)?;
        self.media.ensure_running().await?;
        self.media.create_room(call_id).await?;
        // Revalidate after awaits before disclosing the bearer token.
        if call.media_sessions.get(actor).map(String::as_str) != Some(session_id)
            || !store
                .can_access_thread_call_media(call, actor, now())
                .await?
        {
            return if reconciliation {
                Ok(no_membership())
            } else {
                Err(CallError::Missing)
            };
        }
        let mut response = response(call);
        response.credentials = Some(self.media.credentials_with_relay_url(
            call_id,
            &member.media_identity,
            now(),
            relay_url,
        )?);
        Ok(response)
    }

    async fn check_other_calls(
        &self,
        store: &CrudStore,
        actor: &str,
        call_id: &str,
        held: bool,
    ) -> std::result::Result<(), CallError> {
        let other = store.active_thread_calls_for_principal(actor).await?;
        if other.iter().any(|other| {
            other.snapshot.call_id != call_id
                && other
                    .participant(actor)
                    .is_some_and(|member| member.held == held)
        }) {
            return Err(CallError::Conflict);
        }
        Ok(())
    }

    async fn scoped_call(
        &self,
        store: &CrudStore,
        scope: &ThreadCallScope<'_>,
        actor: &str,
        call_id: &str,
    ) -> std::result::Result<Call, CallError> {
        store
            .thread_call(call_id)
            .await?
            .filter(|call| matches_scope(call, scope) && call.visible_to(actor))
            .ok_or(CallError::Missing)
    }

    pub async fn admit(
        &self,
        store: &CrudStore,
        token: &str,
    ) -> Result<tokio::sync::MutexGuard<'_, ()>> {
        let (room, identity) = self.media.admission(token)?;
        let permit = self.serial.lock().await;
        let call = store
            .thread_call(&room)
            .await?
            .ok_or_else(|| anyhow!("unknown call"))?;
        let member = call
            .snapshot
            .participants
            .iter()
            .find(|member| member.media_identity == identity && !member.held)
            .ok_or_else(|| anyhow!("inactive media admission"))?;
        anyhow::ensure!(
            call.is_active() && !expired_ring(&call) && !self.shutdown.is_cancelled(),
            "call ended"
        );
        anyhow::ensure!(
            store
                .can_access_thread_call_media(&call, &member.principal_id, now())
                .await?,
            "call membership expired"
        );
        self.media.ensure_running().await?;
        self.media.create_room(&room).await?;
        Ok(permit)
    }

    pub async fn run<F: Future<Output = Result<()>> + Send>(
        &self,
        store: &CrudStore,
        mut publish: impl FnMut(Call) -> F + Send,
    ) {
        let mut changes = self.changed.subscribe();
        let mut delivery = Delivery::default();
        let mut cleanup = RoomCleanup::default();
        loop {
            let work = async {
                {
                    // Take the journal of attempted writes in the same order as
                    // the signal's settlement. No newer write can enter between them.
                    let _serial = self.serial.lock().await;
                    let hints = changes
                        .begin_keyed_pass(&store.database_connection())
                        .await?;
                    delivery.pending.extend(self.dirty.lock().await.drain());
                    if let Some(hints) = hints {
                        delivery.pending.extend(hints);
                    } else {
                        delivery.pending.extend(delivery.advertised.keys().cloned());
                    }
                }
                self.deliver_pending(store, &mut delivery, &mut publish)
                    .await;
                cleanup
                    .retry(move |id| async move {
                        let _serial = self.serial.lock().await;
                        // A failed or ambiguous CAS may have left this call active.
                        if store
                            .thread_call(&id)
                            .await?
                            .is_none_or(|call| !call.is_active())
                        {
                            self.media.ensure_running().await?;
                            self.media.delete_room(&id).await?;
                        }
                        Ok(())
                    })
                    .await;
                self.reconcile_pass(store, &mut delivery, &mut cleanup, &mut publish)
                    .await
            };
            let busy = match tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => return,
                result = work => result,
            } {
                Ok(busy) => busy,
                Err(_) => {
                    tracing::warn!("thread call reconciliation failed");
                    true
                }
            };
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => return,
                _ = changes.wait(None) => {},
                _ = tokio::time::sleep(PRESENCE_INTERVAL), if busy || !delivery.pending.is_empty() || !cleanup.rooms.is_empty() => {},
            }
        }
    }

    async fn deliver_pending<F: Future<Output = Result<()>> + Send>(
        &self,
        store: &CrudStore,
        delivery: &mut Delivery,
        publish: &mut (impl FnMut(Call) -> F + Send),
    ) {
        let keys: Vec<_> = delivery.pending.iter().cloned().collect();
        for key in keys {
            let result = delivery
                .resolve(
                    &key,
                    async {
                        let _serial = self.serial.lock().await;
                        let call = store.thread_call(&key).await?;
                        // An advertised room may outlive a cascaded thread deletion.
                        if call.is_none() {
                            self.media.ensure_running().await?;
                            self.media.delete_room(&key).await?;
                        }
                        if call.as_ref().is_none_or(|call| !call.is_active()) {
                            self.presence.lock().await.forget_room(&key);
                        }
                        Ok(call)
                    },
                    &mut *publish,
                )
                .await;
            if result.is_err() {
                tracing::warn!("failed to deliver a pending thread call revision");
            }
        }
    }

    async fn reconcile_pass<F: Future<Output = Result<()>> + Send>(
        &self,
        store: &CrudStore,
        delivery: &mut Delivery,
        cleanup: &mut RoomCleanup,
        publish: &mut (impl FnMut(Call) -> F + Send),
    ) -> Result<bool> {
        let mut cursor = None;
        let mut active = false;
        let mut disappeared: HashSet<_> = delivery.advertised.keys().cloned().collect();
        loop {
            let page = store.active_thread_call_page(cursor.as_deref()).await?;
            if page.is_empty() {
                delivery.pending.extend(disappeared);
                self.presence.lock().await.prune(Instant::now());
                return Ok(active);
            }
            for call in page {
                let id = call.snapshot.call_id;
                cursor = Some(id.clone());
                disappeared.remove(&id);
                active = true;
                match self.reconcile_one(store, &id, cleanup).await {
                    Ok(Some(current)) if current.is_active() => {
                        if delivery.publish(&current, &mut *publish).await.is_err() {
                            delivery.pending.insert(id);
                        }
                    }
                    Ok(_) => {
                        delivery.pending.insert(id);
                    }
                    Err(_) => tracing::warn!("failed to reconcile an active thread call"),
                }
            }
            tokio::task::yield_now().await;
        }
    }

    async fn reconcile_one(
        &self,
        store: &CrudStore,
        id: &str,
        cleanup: &mut RoomCleanup,
    ) -> Result<Option<Call>> {
        let _serial = self.serial.lock().await;
        let Some(mut call) = store.thread_call(id).await? else {
            return Ok(None);
        };
        if !call.is_active() {
            return Ok(Some(call));
        }
        let expected = call.snapshot.revision;
        let members = call.snapshot.participants.clone();
        let has_media = members.iter().any(|member| !member.held);
        if call.expire_ring(now()) {
            // Commit Missed independently of the SFU. Cleanup uses the old
            // call ID and rechecks persistence after WorkSignal settles writes.
            cleanup.rooms.insert(id.to_owned());
        } else {
            let connected = if has_media {
                let media_status = async {
                    self.media.ensure_running().await?;
                    self.media.participants(id).await
                }
                .await;
                if media_status.is_err() {
                    tracing::warn!("could not reconcile LiveKit participants");
                }
                media_status.ok()
            } else {
                None
            };
            for member in &members {
                let eligible = store
                    .can_access_thread_call_media(&call, &member.principal_id, now())
                    .await?;
                let at = Instant::now();
                let mut presence = self.presence.lock().await;
                let expired = if member.held {
                    presence.hold_expired(&member.media_identity, at)
                } else {
                    connected.as_ref().is_some_and(|connected| {
                        presence.media_expired(
                            id,
                            &member.media_identity,
                            connected.contains(&member.media_identity),
                            at,
                        )
                    })
                };
                drop(presence);
                if !eligible || expired {
                    if !member.held {
                        // An unavailable SFU is not proof the media was revoked.
                        self.media.eject(id, &member.media_identity).await?;
                    }
                    call.change(
                        &member.principal_id,
                        call.snapshot.revision,
                        ThreadCallTransition::Leave,
                    );
                }
            }
            reconcile_invitation(&mut call, async |call, invitee| {
                store
                    .is_thread_call_member(&ThreadCallScope::from(call), invitee)
                    .await
            })
            .await?;
            if !call.is_active() && (has_media || connected.is_some()) {
                self.media.delete_room(id).await?;
            }
            // If everyone was held, Hold already removed their media. The
            // empty room expires on the SFU, even if its API is unavailable.
        }
        if call.snapshot.revision == expected {
            return Ok(Some(call));
        }
        if !self.save(store, &call, Some(expected), None).await? {
            return store.thread_call(id).await;
        }
        let mut presence = self.presence.lock().await;
        for member in members {
            if call.participant(&member.principal_id).is_none() {
                presence.remove(&member.media_identity);
            }
        }
        Ok(Some(call))
    }
}

async fn reconcile_invitation(
    call: &mut Call,
    is_member: impl AsyncFnOnce(&Call, &str) -> Result<bool>,
) -> Result<()> {
    if call.snapshot.phase == ThreadCallPhase::Ringing
        && let Some(invitee) = call.snapshot.invitee_id.as_deref()
        && !is_member(call, invitee).await?
    {
        call.end(ThreadCallEndReason::Left);
    }
    Ok(())
}

#[derive(Default)]
struct Presence {
    media: HashMap<String, (String, Instant)>,
    held: HashMap<String, Instant>,
}

const GRACE: Duration = Duration::from_secs(RECONNECT_SECONDS as u64);

impl Presence {
    fn hold(&mut self, id: &str, now: Instant) {
        self.held.insert(id.to_owned(), now);
    }

    fn renew_hold(&mut self, id: &str, now: Instant) -> bool {
        if self.hold_expired(id, now) {
            return false;
        }
        self.hold(id, now);
        true
    }

    fn hold_expired(&self, id: &str, now: Instant) -> bool {
        self.held
            .get(id)
            .is_none_or(|last| now.duration_since(*last) >= GRACE)
    }

    fn media_expired(&mut self, room: &str, id: &str, connected: bool, now: Instant) -> bool {
        let (_, last) = self
            .media
            .entry(id.to_owned())
            .or_insert_with(|| (room.to_owned(), now));
        if connected {
            *last = now;
        }
        now.duration_since(*last) >= GRACE
    }

    fn remove(&mut self, id: &str) {
        self.media.remove(id);
        self.held.remove(id);
    }

    fn forget_room(&mut self, room: &str) {
        self.media.retain(|_, (owner, _)| owner != room);
    }

    fn prune(&mut self, now: Instant) {
        self.held
            .retain(|_, last| now.duration_since(*last) < GRACE);
        // An expired media identity must stay expired until its removal
        // succeeds. Pruning it would start another reconnect grace.
    }
}

#[derive(Default)]
struct RoomCleanup {
    rooms: HashSet<String>,
}

impl RoomCleanup {
    async fn retry<F: Future<Output = Result<()>> + Send>(
        &mut self,
        mut remove: impl FnMut(String) -> F + Send,
    ) {
        let pending: Vec<_> = self.rooms.iter().cloned().collect();
        for id in pending {
            if remove(id.clone()).await.is_ok() {
                self.rooms.remove(&id);
            } else {
                tracing::warn!("failed to clean up an expired ringing room");
            }
        }
    }
}

#[derive(Default)]
struct Delivery {
    pending: HashSet<String>,
    advertised: HashMap<String, i64>,
}

impl Delivery {
    async fn resolve<F: Future<Output = Result<()>> + Send>(
        &mut self,
        id: &str,
        load: impl Future<Output = Result<Option<Call>>> + Send,
        publish: impl FnMut(Call) -> F + Send,
    ) -> Result<()> {
        if let Some(call) = load.await? {
            self.publish(&call, publish).await?;
        } else {
            self.advertised.remove(id);
        }
        self.pending.remove(id);
        Ok(())
    }

    async fn publish<F: Future<Output = Result<()>> + Send>(
        &mut self,
        call: &Call,
        mut publish: impl FnMut(Call) -> F + Send,
    ) -> Result<()> {
        let snapshot = &call.snapshot;
        let id = &snapshot.call_id;
        if self
            .advertised
            .get(id)
            .is_some_and(|seen| *seen >= snapshot.revision)
        {
            return Ok(());
        }
        publish(call.clone()).await?;
        if call.is_active() {
            self.advertised.insert(id.clone(), snapshot.revision);
        } else {
            self.advertised.remove(id);
        }
        Ok(())
    }
}

// A request can recover only its own successor. Including the previous identity
// prevents a reused request key from linking independent participation epochs.
fn resume_media_identity(previous: &str, key: &str) -> Result<String, CallError> {
    let previous = Uuid::parse_str(previous).map_err(|_| CallError::Invalid)?;
    let key = Uuid::parse_str(key).map_err(|_| CallError::Invalid)?;
    let digest = Sha256::new()
        .chain_update(b"pioneer/call-resume/v1")
        .chain_update(previous.as_bytes())
        .chain_update(key.as_bytes())
        .finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    Ok(Uuid::from_bytes(bytes).to_string())
}

fn membership_for_reconciliation<'a>(
    call: &'a Call,
    actor: &str,
    session_id: &str,
    previous: &str,
    successor: Option<&str>,
) -> Option<&'a pioneer_protocol::ThreadCallParticipant> {
    call.participant(actor).filter(|member| {
        call.is_active()
            && call.media_sessions.get(actor).map(String::as_str) == Some(session_id)
            && (member.media_identity == previous
                || successor == Some(member.media_identity.as_str()))
    })
}

fn no_membership() -> ThreadCallResponse {
    ThreadCallResponse {
        call: None,
        credentials: None,
        history: None,
    }
}

fn response(call: &Call) -> ThreadCallResponse {
    ThreadCallResponse {
        call: Some(call.snapshot.clone()),
        credentials: None,
        history: None,
    }
}

fn matches_scope(call: &Call, scope: &ThreadCallScope<'_>) -> bool {
    call.gateway_id == scope.gateway
        && call.snapshot.workspace_id == scope.workspace
        && call.snapshot.thread_id == scope.thread
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

fn expired_ring(call: &Call) -> bool {
    call.snapshot
        .ring_until_unix
        .is_some_and(|deadline| now() >= deadline)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call() -> Call {
        Call::new(
            "gateway".into(),
            "workspace".into(),
            "thread".into(),
            "call".into(),
            "owner".into(),
            None,
            0,
        )
    }

    #[test]
    fn resume_replay_and_offline_disconnect_are_bound_to_one_participation_and_session() {
        let actor = "owner";
        let started = 1;
        let mut call = Call::new(
            "gateway".into(),
            "workspace".into(),
            "thread".into(),
            "call".into(),
            actor.into(),
            None,
            started,
        );
        call.media_sessions.insert(actor.into(), "session".into());
        let old = call.participant(actor).unwrap().media_identity.clone();
        let key = Uuid::new_v4().to_string();
        let next = resume_media_identity(&old, &key).ok().unwrap();
        let unrelated = resume_media_identity(&old, &Uuid::new_v4().to_string())
            .ok()
            .unwrap();
        assert_ne!(old, next);
        assert_ne!(unrelated, next);
        assert_eq!(resume_media_identity(&old, &key).ok().as_ref(), Some(&next));
        assert!(
            membership_for_reconciliation(&call, actor, "session", &old, Some(&next)).is_some()
        );

        assert_eq!(
            call.change(actor, call.snapshot.revision, ThreadCallTransition::Hold),
            Change::Applied
        );
        assert_eq!(
            call.change(actor, call.snapshot.revision, ThreadCallTransition::Resume),
            Change::Applied
        );
        call.snapshot.participants[0].media_identity = next.clone();
        assert_eq!(
            membership_for_reconciliation(&call, actor, "session", &old, Some(&next))
                .map(|member| (member.media_identity.as_str(), member.held)),
            Some((next.as_str(), false)),
        );
        assert!(membership_for_reconciliation(&call, actor, "session", &old, None).is_none());
        assert!(
            membership_for_reconciliation(&call, actor, "session", &old, Some(&unrelated))
                .is_none()
        );
        assert!(
            membership_for_reconciliation(&call, actor, "different-session", &old, Some(&next))
                .is_none()
        );

        call.snapshot.participants[0].media_identity = Uuid::new_v4().to_string();
        assert!(
            membership_for_reconciliation(&call, actor, "session", &old, Some(&next)).is_none()
        );
    }

    #[tokio::test]
    async fn invitee_revocation_cancels_only_a_pending_invitation() {
        let direct_call = Call::new(
            "gateway".into(),
            "workspace".into(),
            "thread".into(),
            "call".into(),
            "owner".into(),
            Some("invitee".into()),
            0,
        );

        let mut answered = direct_call.clone();
        assert_eq!(
            answered.change("invitee", 1, ThreadCallTransition::Join),
            Change::Applied
        );
        // Reconciliation removes an ineligible participant after ejecting
        // that participant from LiveKit, before checking the invitation.
        assert_eq!(
            answered.change("invitee", 2, ThreadCallTransition::Leave),
            Change::Applied
        );
        assert_eq!(answered.snapshot.invitee_id.as_deref(), Some("invitee"));
        reconcile_invitation(&mut answered, async |_, _| {
            panic!("an answered call has no pending invitation to check");
        })
        .await
        .unwrap();
        assert_eq!(answered.snapshot.phase, ThreadCallPhase::Active);
        assert_eq!(answered.snapshot.end_reason, None);
        assert_eq!(answered.snapshot.participants.len(), 1);
        assert!(answered.participant("owner").is_some());
        assert_eq!(
            answered.change("owner", 3, ThreadCallTransition::Leave),
            Change::Applied
        );
        assert_eq!(answered.snapshot.phase, ThreadCallPhase::Ended);

        let mut ringing = direct_call;
        reconcile_invitation(&mut ringing, async |_, invitee| {
            assert_eq!(invitee, "invitee");
            Ok(false)
        })
        .await
        .unwrap();
        assert_eq!(ringing.snapshot.phase, ThreadCallPhase::Ended);
        assert_eq!(ringing.snapshot.end_reason, Some(ThreadCallEndReason::Left));
        assert!(ringing.snapshot.participants.is_empty());
    }

    #[tokio::test]
    async fn terminal_delivery_survives_read_failure_and_releases_tracking() {
        let mut delivery = Delivery::default();
        let mut call = call();
        let id = call.snapshot.call_id.clone();
        delivery.advertised.insert(id.clone(), 1);
        delivery.pending.insert(id.clone());
        call.end(ThreadCallEndReason::Left);

        let err = delivery
            .resolve(
                &id,
                async { Err(anyhow!("temporary reader failure")) },
                |_| async {
                    panic!("no snapshot on reader failure");
                },
            )
            .await;
        assert!(err.is_err());
        assert!(delivery.pending.contains(&id));
        assert_eq!(delivery.advertised.get(&id), Some(&1));

        // A retry without another signal must discover the terminal snapshot.
        let mut ended = None;
        let error = delivery
            .resolve(&id, async { Ok(Some(call.clone())) }, |_| async {
                Err(anyhow!("temporary publisher failure"))
            })
            .await;
        assert!(error.is_err());
        assert!(delivery.pending.contains(&id));
        delivery
            .resolve(&id, async { Ok(Some(call.clone())) }, |snapshot| {
                ended = snapshot.snapshot.end_reason;
                async { Ok(()) }
            })
            .await
            .unwrap();
        assert_eq!(ended, Some(ThreadCallEndReason::Left));
        assert!(delivery.advertised.is_empty());
        assert!(delivery.pending.is_empty());
    }

    #[tokio::test]
    async fn expired_room_cleanup_retries_only_the_original_call_id() {
        let mut cleanup = RoomCleanup::default();
        cleanup.rooms.insert("expired".into());
        cleanup
            .retry(|id| async move {
                assert_eq!(id, "expired");
                Err(anyhow!("sidecar unavailable"))
            })
            .await;
        assert!(cleanup.rooms.contains("expired"));
        let mut rooms = HashSet::from(["expired".to_owned(), "replacement".to_owned()]);
        cleanup
            .retry(|id| {
                assert!(rooms.remove(&id));
                async { Ok(()) }
            })
            .await;
        assert_eq!(rooms, HashSet::from(["replacement".to_owned()]));
        assert!(cleanup.rooms.is_empty());
    }

    #[test]
    fn held_presence_survives_reconnect_inside_grace_and_expires_after_abandonment() {
        let mut presence = Presence::default();
        let mut call = call();
        let id = call.participant("owner").unwrap().media_identity.clone();
        let start = Instant::now();
        assert_eq!(
            call.change("owner", 1, ThreadCallTransition::Hold),
            Change::Applied
        );
        presence.hold(&id, start);
        let mut now = start;
        for _ in 0..20 {
            now += GRACE - Duration::from_secs(1);
            assert!(presence.renew_hold(&id, now));
        }
        assert!(!presence.hold_expired(&id, now + GRACE - Duration::from_millis(1)));
        assert!(presence.hold_expired(&id, now + GRACE));
        assert!(!presence.renew_hold(&id, now + GRACE));
        assert_eq!(
            call.change("owner", 2, ThreadCallTransition::Leave),
            Change::Applied
        );
        assert!(!call.is_active());
    }
}
