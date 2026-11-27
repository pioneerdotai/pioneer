mod media;
mod speech;
mod window;

use self::media::{Device, Devices, Media};
use anyhow::{Context as _, Result};
use gpui_kit::{prelude::*, *};
use pioneer_client::{
    calls::{
        CallMembership, CallUpdates, ThreadCallAction, ThreadCallEndReason, ThreadCallParams,
        ThreadCallPhase, ThreadCallResponse, ThreadCallSnapshot, ThreadCallTransition,
        call_membership, change_call, settle_call_membership,
    },
    core::ClientCore,
    gateway::session_lifecycle::SessionLifecycleState,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub(crate) struct CallTarget {
    pub workspace_id: String,
    pub thread_id: String,
}

impl From<&ThreadCallSnapshot> for CallTarget {
    fn from(call: &ThreadCallSnapshot) -> Self {
        Self {
            workspace_id: call.workspace_id.clone(),
            thread_id: call.thread_id.clone(),
        }
    }
}

#[derive(Clone)]
struct Session {
    call: ThreadCallSnapshot,
    devices: Devices,
}

#[derive(Clone, PartialEq, Eq)]
struct CallOwner {
    endpoint: String,
    gateway: String,
    session: String,
    principal: String,
}

impl CallOwner {
    fn selected(core: &ClientCore) -> Option<(String, String, String)> {
        let endpoint = core.gateway_registry()?.active_gateway_id?;
        let publication = core.gateway_session();
        let metadata = match publication.session(&endpoint)? {
            SessionLifecycleState::Active { metadata, .. }
            | SessionLifecycleState::Connecting { metadata, .. }
            | SessionLifecycleState::Refreshing { metadata, .. }
            | SessionLifecycleState::AwaitingSecureStorage { metadata, .. }
            | SessionLifecycleState::Suspended {
                metadata: Some(metadata),
            } => metadata,
            _ => return None,
        };
        Some((
            endpoint,
            metadata.gateway_id.to_string(),
            metadata.session_id.to_string(),
        ))
    }

    fn matches(&self, core: &ClientCore) -> bool {
        Self::selected(core).is_some_and(|(endpoint, gateway, session)| {
            endpoint == self.endpoint && gateway == self.gateway && session == self.session
        })
    }
}

#[derive(Clone)]
struct Gateway {
    core: Arc<ClientCore>,
    connection_id: u64,
    principal: String,
    owner: CallOwner,
}

impl Gateway {
    fn current(core: &Arc<ClientCore>) -> Option<Self> {
        let connection_id = core.current_auth_ticket().1?;
        let auth = core.current_auth()?;
        let (endpoint, gateway, session) = CallOwner::selected(core)?;
        let publication = core.gateway_session();
        let verified = publication.connections.get(&endpoint)?.connected.as_ref()?;
        if verified.connection_id != connection_id
            || auth.gateway.id.as_str() != gateway
            || auth.session.id.as_str() != session
        {
            return None;
        }
        let principal = auth.principal.id.to_string();
        Some(Self {
            core: core.clone(),
            connection_id,
            owner: CallOwner {
                endpoint,
                gateway,
                session,
                principal: principal.clone(),
            },
            principal,
        })
    }

    fn media_identity(&self, call: &ThreadCallSnapshot) -> Result<String> {
        call.participants
            .iter()
            .find(|member| member.principal_id == self.principal)
            .map(|member| member.media_identity.clone())
            .context("Call membership is unavailable")
    }

    fn restore(&self, call: &ThreadCallSnapshot) -> Result<ThreadCallResponse> {
        self.request(
            call.into(),
            ThreadCallAction::Restore {
                call_id: call.call_id.clone(),
                media_identity: self.media_identity(call)?,
            },
        )
    }

    fn disconnect(&self, leave: &PendingLeave) -> Result<ThreadCallResponse> {
        self.request(
            (&leave.call).into(),
            ThreadCallAction::Disconnect {
                call_id: leave.call.call_id.clone(),
                media_identity: self.media_identity(&leave.call)?,
                resume_key: leave.resume_key.clone(),
            },
        )
    }

    fn resume(
        &self,
        call: &ThreadCallSnapshot,
        resume_key: String,
    ) -> Result<Option<ThreadCallSnapshot>> {
        Ok(self
            .request(
                call.into(),
                ThreadCallAction::Resume {
                    call_id: call.call_id.clone(),
                    media_identity: self.media_identity(call)?,
                    resume_key,
                },
            )?
            .call)
    }

    fn request(
        &self,
        target: CallTarget,
        action: ThreadCallAction,
    ) -> Result<pioneer_client::calls::ThreadCallResponse> {
        anyhow::ensure!(self.owner.matches(&self.core), "Call session changed");
        let response = self.core.request_thread_call(
            self.connection_id,
            &ThreadCallParams {
                workspace_id: target.workspace_id,
                thread_id: target.thread_id,
                action,
            },
        )?;
        anyhow::ensure!(self.owner.matches(&self.core), "Call session changed");
        Ok(response)
    }

    fn status(&self, call: &ThreadCallSnapshot) -> Result<Option<ThreadCallSnapshot>> {
        Ok(self
            .request(
                call.into(),
                ThreadCallAction::Status {
                    call_id: Some(call.call_id.clone()),
                },
            )?
            .call)
    }

    fn settle(
        &self,
        call: &ThreadCallSnapshot,
        transition: ThreadCallTransition,
    ) -> Result<Option<ThreadCallSnapshot>> {
        let mut before = Some(self.status(call)?);
        if Calls::same_membership(
            call,
            before.as_ref().and_then(Option::as_ref),
            &self.principal,
        ) == CallMembership::Absent
        {
            return Ok(None);
        }
        settle_call_membership(
            &self.principal,
            transition,
            || match before.take() {
                Some(status) => Ok(status),
                None => self.status(call),
            },
            |latest| {
                Ok(self
                    .request(latest.into(), change_call(latest, transition))?
                    .call)
            },
        )
    }

    fn change(
        &self,
        call: &ThreadCallSnapshot,
        transition: ThreadCallTransition,
    ) -> Result<ThreadCallSnapshot> {
        let target = CallTarget::from(call);
        let current = self
            .request(
                target.clone(),
                ThreadCallAction::Status {
                    call_id: Some(call.call_id.clone()),
                },
            )?
            .call
            .context("Call is unavailable")?;
        if current.phase == ThreadCallPhase::Ended {
            return Ok(current);
        }
        self.request(target, change_call(&current, transition))?
            .call
            .context("Call transition returned no call")
    }

    fn enter(&self, target: CallTarget) -> Result<ThreadCallSnapshot> {
        let previous = self
            .request(target.clone(), ThreadCallAction::Status { call_id: None })?
            .call;
        if let Some(call) = previous.filter(|call| call.phase != ThreadCallPhase::Ended) {
            if call.phase == ThreadCallPhase::Ringing
                || call
                    .participants
                    .iter()
                    .any(|participant| participant.principal_id == self.principal)
            {
                return Ok(call);
            }
            return self.change(&call, ThreadCallTransition::Join);
        }
        self.request(
            target,
            ThreadCallAction::Start {
                request_key: uuid::Uuid::new_v4().to_string(),
            },
        )?
        .call
        .context("Call start returned no call")
    }
}

#[derive(Clone)]
struct PendingLeave {
    call: ThreadCallSnapshot,
    resume_key: Option<String>,
}

pub(crate) struct Calls {
    core: Arc<ClientCore>,
    connection: Option<u64>,
    owner: Option<CallOwner>,
    leaving: Vec<PendingLeave>,
    resume_key: Option<String>,
    recovery: Option<Task<()>>,
    held_renewed: Option<Instant>,
    active: Option<Session>,
    incoming: Option<ThreadCallSnapshot>,
    held: Option<Session>,
    resume_armed: bool,
    calls_by_thread: HashMap<String, ThreadCallSnapshot>,
    window: Option<AnyWindowHandle>,
    media: Option<Media>,
    device_selection: media::DeviceSelection,
    command: Option<Task<()>>,
    media_task: Option<Task<()>>,
    _events: Task<()>,
    _identity: Task<()>,
    _retry: Task<()>,
    title: String,
    closing: bool,
    failed: bool,
    opening: bool,
}

struct CallsGlobal(Entity<Calls>);
impl Global for CallsGlobal {}

pub fn init_thread_calls(core: Arc<ClientCore>, cx: &mut App) {
    window::init(cx);
    if cx.has_global::<CallsGlobal>() {
        return;
    }
    let mut updates = core.thread_call_updates();
    let mut publications = core.watch_publications();
    let owner = cx.new(|cx: &mut Context<Calls>| {
        let events = cx.spawn(async move |owner, cx| {
            while updates.changed().await.is_ok() {
                let update = updates.borrow_and_update().clone();
                if owner
                    .update(cx, |owner, cx| {
                        let update = owner.core.current_thread_call_updates(&update);
                        owner.on_update(update, cx);
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let identity = cx.spawn(async move |owner, cx| {
            while publications.changed().await.is_ok() {
                if owner
                    .update(cx, |owner, cx| {
                        let gateway = Gateway::current(&owner.core);
                        if owner.sync_owner(gateway.as_ref()) {
                            owner.retry(cx);
                            cx.notify();
                        }
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        let timer = cx.background_executor().clone();
        let retry = cx.spawn(async move |owner, cx| {
            loop {
                timer.timer(Duration::from_secs(2)).await;
                if owner.update(cx, |owner, cx| owner.retry(cx)).is_err() {
                    break;
                }
            }
        });
        Calls {
            core,
            connection: None,
            owner: None,
            leaving: Vec::new(),
            resume_key: None,
            recovery: None,
            held_renewed: None,
            active: None,
            incoming: None,
            held: None,
            resume_armed: false,
            calls_by_thread: HashMap::new(),
            window: None,
            media: None,
            device_selection: media::DeviceSelection::default(),
            media_task: None,
            command: None,
            _events: events,
            _identity: identity,
            _retry: retry,
            title: String::new(),
            closing: false,
            failed: false,
            opening: false,
        }
    });
    cx.set_global(CallsGlobal(owner));
}

impl Calls {
    pub(crate) fn get(cx: &App) -> Option<Entity<Self>> {
        cx.try_global::<CallsGlobal>().map(|owner| owner.0.clone())
    }

    pub(crate) fn label(&self, thread: &str) -> String {
        match self.calls_by_thread.get(thread) {
            Some(call) if call.phase == ThreadCallPhase::Active => {
                t!("thread.call.join").to_string()
            }
            Some(call) if call.end_reason == Some(ThreadCallEndReason::Missed) => {
                t!("thread.call.again").to_string()
            }
            _ => t!("thread.call.start").to_string(),
        }
    }

    pub(crate) fn missed(&self, thread: &str) -> bool {
        self.calls_by_thread
            .get(thread)
            .is_some_and(|call| call.end_reason == Some(ThreadCallEndReason::Missed))
    }

    pub(crate) fn refresh(&self, target: CallTarget, cx: &mut Context<Self>) {
        let Some(gateway) = Gateway::current(&self.core) else {
            return;
        };
        let background = cx.background_spawn(async move {
            let response = gateway.request(target, ThreadCallAction::Status { call_id: None })?;
            Ok::<_, anyhow::Error>((gateway.connection_id, response.call))
        });
        cx.spawn(async move |owner, cx| {
            if let Ok((connection, Some(call))) = background.await {
                let _ = owner.update(cx, |owner, cx| {
                    if owner.core.current_auth_ticket().1 == Some(connection) {
                        owner.remember(call);
                        cx.notify();
                    }
                });
            }
        })
        .detach();
    }

    fn remember(&mut self, call: ThreadCallSnapshot) {
        if self.calls_by_thread.len() >= 128 && !self.calls_by_thread.contains_key(&call.thread_id)
        {
            if let Some(key) = self.calls_by_thread.keys().next().cloned() {
                self.calls_by_thread.remove(&key);
            }
        }
        let current = self.calls_by_thread.get(&call.thread_id);
        let replace = current.is_none_or(|current| {
            if current.call_id == call.call_id {
                call.revision > current.revision
            } else {
                (call.created_at_unix, call.phase != ThreadCallPhase::Ended)
                    >= (
                        current.created_at_unix,
                        current.phase != ThreadCallPhase::Ended,
                    )
            }
        });
        if replace {
            self.calls_by_thread.insert(call.thread_id.clone(), call);
        }
    }

    pub(crate) fn open(&mut self, target: CallTarget, title: String, cx: &mut Context<Self>) {
        let gateway = Gateway::current(&self.core);
        if self.sync_owner(gateway.as_ref()) {
            cx.notify();
        }
        if !self.leaving.is_empty()
            || self.active.is_some()
            || self.held.is_some()
            || self.incoming.is_some()
            || self.opening
        {
            self.show(cx);
            return;
        }
        let Some(gateway) = gateway else { return };
        self.title = title;
        self.opening = true;
        self.failed = false;
        self.show(cx);
        let principal = gateway.principal.clone();
        let connection = gateway.connection_id;
        let response = cx.background_spawn(async move { gateway.enter(target) });
        self.command = Some(cx.spawn(async move |owner, cx| {
            let result = response.await;
            let _ = owner.update(cx, |owner, cx| {
                owner.command.take();
                owner.opening = false;
                if Gateway::current(&owner.core)
                    .is_none_or(|gateway| gateway.connection_id != connection)
                {
                    cx.notify();
                    return;
                }
                match result {
                    Ok(call)
                        if call.phase == ThreadCallPhase::Ringing
                            && call.invitee_id.as_deref() == Some(&principal) =>
                    {
                        owner.remember(call.clone());
                        owner.incoming = Some(call);
                    }
                    Ok(call) => owner.start_session(call, Devices::default(), cx),
                    Err(_) => owner.failed = true,
                }
                cx.notify();
            });
        }));
    }

    fn stop_media(&mut self) {
        self.media_task.take();
        self.media.take();
    }

    fn sync_owner(&mut self, gateway: Option<&Gateway>) -> bool {
        let replaced = self.owner.as_ref().is_some_and(|owner| {
            !owner.matches(&self.core) || gateway.is_some_and(|gateway| gateway.owner != *owner)
        });
        let connection = gateway.map(|gateway| gateway.connection_id);
        if !replaced && self.connection == connection {
            return false;
        }
        self.command.take();
        self.recovery.take();
        self.stop_media();
        self.incoming.take();
        self.calls_by_thread.clear();
        self.opening = false;
        self.held_renewed = None;
        if replaced {
            self.active.take();
            self.held.take();
            self.leaving.clear();
            self.resume_key.take();
            self.owner.take();
            self.device_selection = media::DeviceSelection::default();
            self.resume_armed = false;
            self.closing = false;
            self.failed = false;
        }
        if let Some(gateway) = gateway {
            self.owner = Some(gateway.owner.clone());
        }
        self.connection = connection;
        true
    }

    fn on_update(&mut self, update: CallUpdates, cx: &mut Context<Self>) {
        let gateway = Gateway::current(&self.core);
        self.sync_owner(gateway.as_ref());
        let Some(gateway) =
            gateway.filter(|gateway| update.connection_id == Some(gateway.connection_id))
        else {
            self.retry(cx);
            cx.notify();
            return;
        };
        for call in update.calls {
            self.remember(call.clone());
            if let Some(active) = &mut self.active
                && active.call.call_id == call.call_id
                && active.call.revision <= call.revision
            {
                match Self::same_membership(&active.call, Some(&call), &gateway.principal) {
                    CallMembership::Active => active.call = call.clone(),
                    membership => {
                        if membership == CallMembership::Held {
                            self.held = Some(Session {
                                call: call.clone(),
                                devices: active.devices,
                            });
                            self.resume_armed = true;
                        }
                        self.active.take();
                        self.recovery.take();
                        self.stop_media();
                    }
                }
            }
            if let Some(held) = &mut self.held
                && held.call.call_id == call.call_id
                && held.call.revision <= call.revision
            {
                if Self::same_membership(&held.call, Some(&call), &gateway.principal)
                    == CallMembership::Absent
                    && self.command.is_none()
                    && self.resume_key.is_none()
                {
                    self.held.take();
                    self.resume_armed = false;
                } else if Self::same_membership(&held.call, Some(&call), &gateway.principal)
                    == CallMembership::Held
                {
                    held.call = call.clone();
                }
            }
            if self
                .incoming
                .as_ref()
                .is_some_and(|incoming| incoming.call_id == call.call_id)
                && call.phase != ThreadCallPhase::Ringing
            {
                self.incoming.take();
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            if call.phase == ThreadCallPhase::Ringing
                && call.invitee_id.as_deref() == Some(&gateway.principal)
                && call.ring_until_unix.is_some_and(|deadline| deadline > now)
                && self
                    .incoming
                    .as_ref()
                    .is_none_or(|incoming| incoming.call_id != call.call_id)
            {
                if self.active.is_none() {
                    self.title.clear();
                }
                self.incoming = Some(call);
                self.show(cx);
            }
        }
        self.retry(cx);
        cx.notify();
    }

    fn same_membership(
        previous: &ThreadCallSnapshot,
        current: Option<&ThreadCallSnapshot>,
        principal: &str,
    ) -> CallMembership {
        let media_identity = |call: &ThreadCallSnapshot| {
            call.participants
                .iter()
                .find(|member| member.principal_id == principal)
                .map(|member| member.media_identity.clone())
        };
        if current.is_none_or(|call| call.call_id != previous.call_id)
            || current.and_then(media_identity) != media_identity(previous)
        {
            return CallMembership::Absent;
        }
        call_membership(current, principal)
    }

    fn start_session(
        &mut self,
        call: ThreadCallSnapshot,
        devices: Devices,
        cx: &mut Context<Self>,
    ) {
        let Some(gateway) = Gateway::current(&self.core) else {
            return;
        };
        let current = self
            .calls_by_thread
            .get(&call.thread_id)
            .filter(|latest| latest.call_id == call.call_id && latest.revision > call.revision)
            .unwrap_or(&call);
        if Self::same_membership(&call, Some(current), &gateway.principal) != CallMembership::Active
        {
            return;
        }
        let call = current.clone();
        self.remember(call.clone());
        self.active = Some(Session { call, devices });
        self.recover_media(cx);
    }

    fn recover_media(&mut self, cx: &mut Context<Self>) {
        self.retry(cx);
    }

    fn retry(&mut self, cx: &mut Context<Self>) {
        let gateway = Gateway::current(&self.core);
        if self.sync_owner(gateway.as_ref()) {
            cx.notify();
        }
        let Some(gateway) = gateway else { return };
        if self.recovery.is_some() {
            return;
        }
        if self.resume_key.is_some() && self.held.is_some() {
            self.act(ThreadCallTransition::Resume, cx);
        } else {
            self.resume(cx);
        }
        let leaving = !self.leaving.is_empty();
        let can_restore = !leaving && self.command.is_none();
        let held = self.held.clone().filter(|_| {
            self.command.is_none()
                && self.resume_key.is_none()
                && self
                    .held_renewed
                    .is_none_or(|at| at.elapsed() >= Duration::from_secs(12))
        });
        let active = self
            .active
            .clone()
            .filter(|_| can_restore && self.media.is_none());
        let disconnect = self.leaving.first().cloned();
        if held.is_none() && active.is_none() && !leaving {
            return;
        }
        if held.is_some() {
            self.held_renewed = Some(Instant::now());
        }
        let connection = gateway.connection_id;
        let principal = gateway.principal.clone();
        let requests = cx.background_spawn(async move {
            let disconnected = disconnect.map(|leave| gateway.disconnect(&leave));
            let held = held.map(|session| {
                let response = gateway.restore(&session.call);
                (session, response)
            });
            let active = active.map(|session| {
                let response = gateway.restore(&session.call);
                (session, response)
            });
            (disconnected, held, active)
        });
        self.recovery = Some(cx.spawn(async move |owner, cx| {
            let (disconnected, held, active) = requests.await;
            let joined = owner.update(cx, |owner, cx| {
                if owner.connection != Some(connection)
                    || Gateway::current(&owner.core)
                        .is_none_or(|gateway| gateway.connection_id != connection)
                {
                    owner.recovery.take();
                    return None;
                }
                if let Some(Ok(response)) = disconnected {
                    owner.leaving.remove(0);
                    if let Some(call) = response.call {
                        owner.remember(call);
                    }
                }
                if let Some((session, Ok(response))) = held
                    && owner.resume_key.is_none()
                    && let Some(current) = &owner.held
                    && current.call.call_id == session.call.call_id
                {
                    match Self::same_membership(&current.call, response.call.as_ref(), &principal) {
                        CallMembership::Held => {
                            if let Some(call) = response.call
                                && call.revision >= current.call.revision
                            {
                                owner.held = Some(Session {
                                    call,
                                    devices: current.devices,
                                });
                            }
                        }
                        CallMembership::Absent => {
                            owner.held.take();
                            owner.resume_armed = false;
                        }
                        CallMembership::Active => {}
                    }
                }
                let mut joined = None;
                if let Some((session, result)) = active
                    && let Some(current) = owner.active.as_ref()
                    && current.call.call_id == session.call.call_id
                {
                    match result {
                        Ok(response) => {
                            let observed = response.call.clone();
                            match Self::same_membership(
                                &current.call,
                                response.call.as_ref(),
                                &principal,
                            ) {
                                CallMembership::Active => {
                                    if let Some(credentials) = response.credentials {
                                        let call = response.call.expect("confirmed membership");
                                        if call.revision >= current.call.revision {
                                            owner.active = Some(Session {
                                                call,
                                                devices: current.devices,
                                            });
                                        }
                                        let call_id = session.call.call_id;
                                        let devices = session.devices;
                                        let selection = owner.device_selection.clone();
                                        joined = Some((
                                            call_id,
                                            cx.background_spawn(async move {
                                                Media::connect(credentials, devices, selection)
                                                    .await
                                            }),
                                        ));
                                    }
                                }
                                CallMembership::Held => {
                                    owner.held = response.call.map(|call| Session {
                                        call,
                                        devices: current.devices,
                                    });
                                    owner.active.take();
                                    owner.resume_armed = true;
                                }
                                CallMembership::Absent => {
                                    owner.active.take();
                                    owner.failed = false;
                                }
                            }
                            if let Some(call) = observed {
                                owner.remember(call);
                            }
                        }
                        Err(_) => owner.failed = true,
                    }
                }
                if joined.is_none() {
                    owner.recovery.take();
                }
                owner.resume(cx);
                cx.notify();
                joined
            });
            if let Ok(Some((call_id, joined))) = joined {
                let media = joined.await;
                let _ = owner.update(cx, |owner, cx| {
                    owner.recovery.take();
                    if owner.connection == Some(connection)
                        && owner
                            .active
                            .as_ref()
                            .is_some_and(|session| session.call.call_id == call_id)
                        && Gateway::current(&owner.core)
                            .is_some_and(|gateway| gateway.connection_id == connection)
                    {
                        match media {
                            Ok((media, selection)) => {
                                owner.device_selection = selection;
                                owner.attach_media(media, cx);
                            }
                            Err(_) => owner.failed = true,
                        }
                    }
                    cx.notify();
                });
            }
        }));
    }

    fn attach_media(&mut self, media: Media, cx: &mut Context<Self>) {
        let mut updates = media.changes.clone();
        self.media = Some(media);
        self.failed = false;
        self.media_task = Some(cx.spawn(async move |owner, cx| {
            while updates.changed().await.is_ok() {
                if owner.update(cx, |_, cx| cx.notify()).is_err() {
                    return;
                }
            }
            let _ = owner.update(cx, |owner, cx| {
                owner.media.take();
                owner.failed = true;
                owner.retry(cx);
                cx.notify();
            });
        }));
    }

    fn leave(&mut self, cx: &mut Context<Self>) {
        let (session, resume_key) = if let Some(session) = self.active.take() {
            self.stop_media();
            (session, None)
        } else if let Some(session) = self.held.take() {
            self.resume_armed = false;
            (session, self.resume_key.take())
        } else {
            return;
        };
        // At most the active and held sessions can reach this list. Opening
        // and resuming stay blocked until their exact exits are confirmed.
        self.leaving.push(PendingLeave {
            call: session.call,
            resume_key,
        });
        self.recovery.take();
        self.failed = false;
        if self.closing
            && self.held.is_none()
            && self.incoming.is_none()
            && let Some(handle) = self.window.take()
        {
            self.closing = false;
            let _ = handle.update(cx, |_, window, _| window.remove_window());
        }
        self.retry(cx);
        cx.notify();
    }

    fn act(&mut self, transition: ThreadCallTransition, cx: &mut Context<Self>) {
        if self.command.is_some() {
            return;
        }
        if transition == ThreadCallTransition::Leave {
            self.leave(cx);
            return;
        }
        if !self.leaving.is_empty()
            || (transition == ThreadCallTransition::Join && self.resume_key.is_some())
        {
            return;
        }
        let Some(gateway) = Gateway::current(&self.core) else {
            return;
        };
        let session = match transition {
            ThreadCallTransition::Resume
                if self.active.is_none()
                    && (self.incoming.is_none() || self.resume_key.is_some()) =>
            {
                self.held.as_ref()
            }
            _ => None,
        };
        let call = match transition {
            ThreadCallTransition::Join | ThreadCallTransition::Decline => self.incoming.clone(),
            _ => session.map(|session| session.call.clone()),
        };
        let Some(call) = call else { return };
        let devices = session.map(|session| session.devices).unwrap_or_default();
        let active = if transition == ThreadCallTransition::Join {
            self.active.clone()
        } else {
            None
        };
        if active.is_some() && self.held.is_some() {
            return;
        }
        let resume_key = (transition == ThreadCallTransition::Resume).then(|| {
            self.resume_armed = false;
            self.resume_key
                .get_or_insert_with(|| uuid::Uuid::new_v4().to_string())
                .clone()
        });
        self.failed = false;
        let media = if active.is_some() {
            self.recovery.take();
            self.media_task.take();
            self.media.take()
        } else {
            None
        };
        let hold_requested = active.is_some();
        let connection = gateway.connection_id;
        let principal = gateway.principal.clone();
        let work = cx.background_spawn(async move {
            if let Some(media) = media {
                media.close().await;
            }
            let held = active
                .map(|session| {
                    let status = gateway.settle(&session.call, ThreadCallTransition::Hold)?;
                    Ok::<_, anyhow::Error>(
                        status
                            .filter(|call| {
                                call_membership(Some(call), &gateway.principal)
                                    == CallMembership::Held
                            })
                            .map(|call| Session {
                                call,
                                devices: session.devices,
                            }),
                    )
                })
                .transpose();
            let result = if held.is_ok() {
                match transition {
                    ThreadCallTransition::Join | ThreadCallTransition::Decline => {
                        gateway.change(&call, transition).map(Some)
                    }
                    ThreadCallTransition::Resume => {
                        gateway.resume(&call, resume_key.expect("resume key"))
                    }
                    _ => gateway.settle(&call, transition),
                }
            } else {
                Err(anyhow::anyhow!("Could not hold the active call"))
            };
            (held, result)
        });
        self.command = Some(cx.spawn(async move |owner, cx| {
            let (held, result) = work.await;
            let _ = owner.update(cx, |owner, cx| {
                owner.command.take();
                if Gateway::current(&owner.core)
                    .is_none_or(|gateway| gateway.connection_id != connection)
                {
                    cx.notify();
                    return;
                }
                if let Ok(Some(held)) = held {
                    owner.active.take();
                    owner.resume_armed = held.is_some();
                    owner.held = held;
                }
                match result {
                    Ok(result) => {
                        if let Some(call) = &result {
                            owner.remember(call.clone());
                        }
                        match transition {
                            ThreadCallTransition::Join => {
                                owner.incoming.take();
                                if let Some(call) = result {
                                    owner.start_session(call, Devices::default(), cx);
                                }
                            }
                            ThreadCallTransition::Decline => {
                                owner.incoming.take();
                            }
                            ThreadCallTransition::Resume => {
                                owner.resume_key.take();
                                owner.held.take();
                                if let Some(call) = result.filter(|call| {
                                    call_membership(Some(call), &principal)
                                        == CallMembership::Active
                                }) {
                                    owner.start_session(call, devices, cx);
                                }
                            }
                            ThreadCallTransition::Hold | ThreadCallTransition::Leave => {}
                        }
                    }
                    Err(_) => {
                        owner.failed = true;
                        // After holding successfully, a failed answer still leaves the first call
                        // recoverable once the incoming invitation is dismissed or expires.
                        if hold_requested && owner.held.is_none() && owner.active.is_some() {
                            owner.recover_media(cx);
                        }
                    }
                }
                if owner.closing && !owner.failed {
                    owner.closing = false;
                    if owner.active.is_none()
                        && owner.incoming.is_none()
                        && owner.held.is_none()
                        && let Some(handle) = owner.window.take()
                    {
                        let _ = handle.update(cx, |_, window, _| window.remove_window());
                    }
                }
                owner.resume(cx);
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn resume(&mut self, cx: &mut Context<Self>) {
        if self.resume_armed
            && self.active.is_none()
            && self.held.is_some()
            && self.incoming.is_none()
        {
            self.act(ThreadCallTransition::Resume, cx);
        }
    }

    fn select_call_device(&mut self, choice: media::DeviceChoice, cx: &mut Context<Self>) {
        let Some(media) = self.media.as_ref().filter(|_| self.command.is_none()) else {
            return;
        };
        let change = media.select_device(choice.clone());
        self.command = Some(cx.spawn(async move |owner, cx| {
            let result = change.await;
            let _ = owner.update(cx, |owner, cx| {
                owner.command.take();
                match result {
                    Ok(change) => {
                        if let Some(session) = &mut owner.active {
                            session.devices.camera = change.camera_enabled;
                        }
                        owner.failed = change.result.is_err();
                        if change.result.is_ok() {
                            owner.device_selection.set(choice);
                        }
                    }
                    Err(_) => owner.failed = true,
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn toggle(&mut self, device: Device, cx: &mut Context<Self>) {
        if self.command.is_some() {
            return;
        }
        let (Some(media), Some(session)) = (&self.media, &mut self.active) else {
            return;
        };
        let enabled = session.devices.begin_toggle(device);
        let change = media.set_device(device, enabled);
        self.command = Some(cx.spawn(async move |owner, cx| {
            let result = change.await;
            let _ = owner.update(cx, |owner, cx| {
                owner.command.take();
                owner.failed = result.is_err();
                if enabled
                    && result.is_ok()
                    && let Some(session) = &mut owner.active
                {
                    session.devices.confirm_enabled(device);
                } else if !enabled && result.is_err() {
                    owner.stop_media();
                    owner.retry(cx);
                }
                cx.notify();
            });
        }));
        cx.notify();
    }
}
