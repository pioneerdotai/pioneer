use anyhow::{Context, Result};
use futures_util::StreamExt;
use livekit::{
    options::TrackPublishOptions,
    prelude::*,
    webrtc::{
        native::yuv_helper::abgr_to_i420,
        prelude::*,
        video_frame::{BoxVideoFrame, VideoFormatType},
        video_source::native::NativeVideoSource,
        video_stream::native::NativeVideoStream,
    },
};
use nokhwa::{
    Camera,
    pixel_format::RgbAFormat,
    utils::{CameraIndex, RequestedFormat, RequestedFormatType},
};
use pioneer_client::calls::ThreadCallCredentials;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, oneshot, watch},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub(super) struct Devices {
    pub microphone: bool,
    pub camera: bool,
}

impl Devices {
    fn preference_mut(&mut self, device: Device) -> &mut bool {
        match device {
            Device::Microphone => &mut self.microphone,
            Device::Camera => &mut self.camera,
        }
    }

    pub(super) fn begin_toggle(&mut self, device: Device) -> bool {
        let preference = self.preference_mut(device);
        if *preference {
            // Restore must see an explicit off even if the SDK reply is lost.
            *preference = false;
            false
        } else {
            true
        }
    }

    pub(super) fn confirm_enabled(&mut self, device: Device) {
        *self.preference_mut(device) = true;
    }
}

impl Default for Devices {
    fn default() -> Self {
        Self {
            microphone: true,
            camera: false,
        }
    }
}

pub(super) struct Frame {
    pub width: u32,
    pub height: u32,
    pub bytes: Vec<u8>,
}

type Frames = Arc<Mutex<HashMap<String, Arc<Frame>>>>;

#[derive(Clone, Copy, Default, PartialEq)]
pub(super) struct ParticipantMedia {
    pub camera: bool,
    pub microphone: bool,
    pub audio_level: f32,
}

#[derive(Clone, Default)]
pub(super) struct MediaView {
    pub frames: Frames,
    pub participants: Arc<Mutex<HashMap<String, ParticipantMedia>>>,
    pub connected: Arc<AtomicBool>,
}

pub(super) struct Media {
    commands: mpsc::Sender<MediaCommand>,
    stop: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
    pub view: MediaView,
    pub changes: watch::Receiver<u64>,
}

#[derive(Clone, Copy)]
pub(super) enum Device {
    Microphone,
    Camera,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) enum DeviceChoice {
    Microphone(RecordingDeviceId),
    Speaker(PlayoutDeviceId),
    Camera(CameraIndex),
}

#[derive(Clone, Default)]
pub(super) struct DeviceSelection {
    pub microphone: Option<RecordingDeviceId>,
    pub speaker: Option<PlayoutDeviceId>,
    pub camera: Option<CameraIndex>,
}

impl DeviceSelection {
    pub fn contains(&self, choice: &DeviceChoice) -> bool {
        match choice {
            DeviceChoice::Microphone(id) => self.microphone.as_ref() == Some(id),
            DeviceChoice::Speaker(id) => self.speaker.as_ref() == Some(id),
            DeviceChoice::Camera(id) => self.camera.as_ref() == Some(id),
        }
    }

    pub fn set(&mut self, choice: DeviceChoice) {
        match choice {
            DeviceChoice::Microphone(id) => self.microphone = Some(id),
            DeviceChoice::Speaker(id) => self.speaker = Some(id),
            DeviceChoice::Camera(id) => self.camera = Some(id),
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct DeviceInventory {
    pub microphones: Vec<(String, DeviceChoice)>,
    pub speakers: Vec<(String, DeviceChoice)>,
    pub cameras: Vec<(String, DeviceChoice)>,
    pub camera_error: bool,
}

pub(super) struct DeviceChange {
    pub result: Result<()>,
    pub camera_enabled: bool,
}

enum MediaCommand {
    Toggle {
        device: Device,
        enabled: bool,
        done: oneshot::Sender<Result<()>>,
    },
    Inventory(oneshot::Sender<DeviceInventory>),
    Select {
        choice: DeviceChoice,
        done: oneshot::Sender<DeviceChange>,
    },
}

fn cameras() -> Result<Vec<(String, DeviceChoice)>> {
    Ok(nokhwa::query(nokhwa::utils::ApiBackend::Auto)?
        .into_iter()
        .map(|info| {
            let index = if cfg!(target_os = "macos") {
                // AVFoundation's unique ID keeps a hot-plug from shifting the selection.
                CameraIndex::String(info.misc())
            } else {
                info.index().clone()
            };
            (info.human_name(), DeviceChoice::Camera(index))
        })
        .collect())
}

fn runtime() -> Result<&'static Runtime> {
    static RUNTIME: OnceLock<std::io::Result<Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("pioneer-calls")
                .build()
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!("Could not start the media runtime: {error}"))
}

impl Media {
    pub async fn connect(
        credentials: ThreadCallCredentials,
        devices: Devices,
        selection: DeviceSelection,
    ) -> Result<(Self, DeviceSelection)> {
        let (commands, receiver) = mpsc::channel(4);
        let (changes, updates) = watch::channel(0);
        let (ready, joined) = oneshot::channel();
        let view = MediaView::default();
        let stop = CancellationToken::new();
        let task = runtime()?.spawn(run(
            credentials,
            devices,
            selection,
            receiver,
            ready,
            stop.clone(),
            view.clone(),
            changes,
        ));
        let session = Self {
            commands,
            stop,
            task: Some(task),
            view,
            changes: updates,
        };
        let selection = joined.await.context("LiveKit media session ended")??;
        Ok((session, selection))
    }

    pub fn set_device(
        &self,
        device: Device,
        enabled: bool,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let commands = self.commands.clone();
        async move {
            let (done, result) = oneshot::channel();
            commands
                .send(MediaCommand::Toggle {
                    device,
                    enabled,
                    done,
                })
                .await
                .map_err(|_| anyhow::anyhow!("Media session ended"))?;
            result.await.context("Media session ended")?
        }
    }

    pub fn inventory(&self) -> impl Future<Output = Result<DeviceInventory>> + Send + 'static {
        let commands = self.commands.clone();
        async move {
            let (done, result) = oneshot::channel();
            commands
                .send(MediaCommand::Inventory(done))
                .await
                .map_err(|_| anyhow::anyhow!("Media session ended"))?;
            result.await.context("Media session ended")
        }
    }

    pub fn select_device(
        &self,
        choice: DeviceChoice,
    ) -> impl Future<Output = Result<DeviceChange>> + Send + 'static {
        let commands = self.commands.clone();
        async move {
            let (done, result) = oneshot::channel();
            commands
                .send(MediaCommand::Select { choice, done })
                .await
                .map_err(|_| anyhow::anyhow!("Media session ended"))?;
            result.await.context("Media session ended")
        }
    }

    pub async fn close(mut self) {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for Media {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

struct CameraCapture {
    stop: CancellationToken,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for CameraCapture {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

impl CameraCapture {
    async fn start(source: NativeVideoSource, index: CameraIndex) -> Result<Self> {
        let (ready, opened) = oneshot::channel();
        let stop = CancellationToken::new();
        let capture_stop = stop.clone();
        let thread = std::thread::Builder::new()
            .name("pioneer-call-camera".into())
            .spawn(move || {
                let result = (|| -> Result<Camera> {
                    #[cfg(target_os = "macos")]
                    {
                        let (send, receive) = std::sync::mpsc::sync_channel(1);
                        nokhwa::nokhwa_initialize(move |allowed| {
                            let _ = send.try_send(allowed);
                        });
                        while !capture_stop.is_cancelled() {
                            match receive.recv_timeout(Duration::from_millis(200)) {
                                Ok(true) => break,
                                Ok(false) => anyhow::bail!("Camera permission denied"),
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                                Err(error) => return Err(error.into()),
                            }
                        }
                    }
                    anyhow::ensure!(!capture_stop.is_cancelled(), "Camera opening cancelled");
                    // AVFoundation cameras may offer only native YUV formats.
                    #[cfg(target_os = "macos")]
                    let requested = RequestedFormatType::None;
                    #[cfg(not(target_os = "macos"))]
                    let requested =
                        RequestedFormatType::Closest(nokhwa::utils::CameraFormat::new_from(
                            640,
                            480,
                            nokhwa::utils::FrameFormat::MJPEG,
                            30,
                        ));
                    let format = RequestedFormat::new::<RgbAFormat>(requested);
                    let mut camera = Camera::new(index, format)?;
                    camera.open_stream()?;
                    Ok(camera)
                })();
                let mut camera = match result {
                    Ok(camera) => camera,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(())).is_err() {
                    return;
                }
                while !capture_stop.is_cancelled() {
                    let Ok(frame) = camera.frame() else { break };
                    let Ok(rgba) = frame.decode_image::<RgbAFormat>() else {
                        continue;
                    };
                    let (width, height) = rgba.dimensions();
                    let mut buffer = I420Buffer::new(width, height);
                    let (stride_y, stride_u, stride_v) = buffer.strides();
                    let (y, u, v) = buffer.data_mut();
                    abgr_to_i420(
                        rgba.as_raw(),
                        width * 4,
                        y,
                        stride_y,
                        u,
                        stride_u,
                        v,
                        stride_v,
                        width as i32,
                        height as i32,
                    );
                    if !capture_stop.is_cancelled() {
                        source
                            .capture_frame(&VideoFrame::new(VideoRotation::VideoRotation0, buffer));
                    }
                }
                let _ = camera.stop_stream();
            })?;
        let capture = Self {
            stop,
            thread: Some(thread),
        };
        tokio::time::timeout(Duration::from_secs(30), opened)
            .await??
            .context("Could not open camera")?;
        Ok(capture)
    }

    async fn close(mut self) {
        self.stop.cancel();
        if let Some(thread) = self.thread.take() {
            let _ = tokio::task::spawn_blocking(move || thread.join()).await;
        }
    }
}

struct LocalCamera {
    track: LocalVideoTrack,
    source: NativeVideoSource,
    capture: Option<CameraCapture>,
}

struct RoomMedia {
    room: Room,
    audio: PlatformAudio,
    selection: DeviceSelection,
    microphone: Option<LocalAudioTrack>,
    camera: Option<LocalCamera>,
    streams: HashMap<String, (TrackSid, tokio::task::JoinHandle<()>)>,
    view: MediaView,
    speakers: HashMap<String, f32>,
    changes: watch::Sender<u64>,
}

impl RoomMedia {
    fn remove_video(&mut self, key: &str) {
        if let Some((_, task)) = self.streams.remove(key) {
            task.abort();
        }
        self.view.frames.lock().unwrap().remove(key);
        self.changes.send_modify(|n| *n += 1);
    }

    fn watch_video(&mut self, key: String, sid: TrackSid, track: RtcVideoTrack) {
        self.remove_video(&key);
        let frames = self.view.frames.clone();
        let changes = self.changes.clone();
        let peer = key.clone();
        let mut stream = NativeVideoStream::new(track);
        let task = tokio::spawn(async move {
            while let Some(frame) = stream.next().await {
                let frame = to_frame(frame);
                frames.lock().unwrap().insert(peer.clone(), Arc::new(frame));
                changes.send_modify(|n| *n += 1);
            }
        });
        self.streams.insert(key, (sid, task));
    }

    async fn set_microphone(&mut self, enabled: bool) -> Result<()> {
        if enabled {
            if let Some(track) = &self.microphone {
                self.audio.start_recording()?;
                track.unmute();
            } else {
                let track =
                    LocalAudioTrack::create_audio_track("microphone", self.audio.rtc_source());
                self.room
                    .local_participant()
                    .publish_track(
                        LocalTrack::Audio(track.clone()),
                        TrackPublishOptions {
                            source: TrackSource::Microphone,
                            ..Default::default()
                        },
                    )
                    .await?;
                self.microphone = Some(track);
            }
        } else {
            if let Some(track) = &self.microphone {
                track.mute();
            }
            self.audio.stop_recording()?;
        }
        Ok(())
    }

    async fn inventory(&self) -> DeviceInventory {
        let cameras = tokio::task::spawn_blocking(cameras).await;
        let (cameras, camera_error) = match cameras {
            Ok(Ok(found)) => (found, false),
            _ => (Vec::new(), true),
        };
        DeviceInventory {
            microphones: self
                .audio
                .recording_devices()
                .map(|d| (d.name, DeviceChoice::Microphone(d.id)))
                .collect(),
            speakers: self
                .audio
                .playout_devices()
                .map(|d| (d.name, DeviceChoice::Speaker(d.id)))
                .collect(),
            cameras,
            camera_error,
        }
    }

    async fn select(&mut self, choice: DeviceChoice, devices: &mut Devices) -> Result<()> {
        if self.selection.contains(&choice) {
            return Ok(());
        }
        match &choice {
            DeviceChoice::Microphone(id) => {
                if devices.microphone {
                    self.audio.switch_recording_device(id)?;
                } else {
                    self.audio.set_recording_device(id)?;
                }
            }
            DeviceChoice::Speaker(id) => {
                anyhow::ensure!(
                    self.audio.playout_devices().any(|d| d.id == *id),
                    "Speaker unavailable"
                );
                self.audio.switch_playout_device(id)?;
            }
            DeviceChoice::Camera(id) => {
                let id = id.clone();
                let listed = tokio::task::spawn_blocking(cameras).await??;
                anyhow::ensure!(
                    listed
                        .into_iter()
                        .any(|(_, choice)| choice == DeviceChoice::Camera(id.clone())),
                    "Camera unavailable"
                );
                if devices.camera {
                    let previous = self.selection.camera.clone();
                    self.set_camera(false).await?;
                    self.selection.camera = Some(id);
                    if let Err(error) = self.set_camera(true).await {
                        self.selection.camera = previous;
                        if let Err(restore) = self.set_camera(true).await {
                            devices.camera = false;
                            return Err(error.context(format!("Camera recovery failed: {restore}")));
                        }
                        return Err(error);
                    }
                }
            }
        }
        self.selection.set(choice);
        Ok(())
    }

    async fn set_camera(&mut self, enabled: bool) -> Result<()> {
        let participant = self.room.local_participant();
        let key = participant.identity().to_string();
        let index = self
            .selection
            .camera
            .clone()
            .unwrap_or(CameraIndex::Index(0));
        if !enabled {
            if let Some(camera) = &mut self.camera
                && let Some(capture) = camera.capture.take()
            {
                // The SDK can assign a new SID while republishing this track.
                // Mute the track and release the device regardless of its SID.
                camera.track.mute();
                self.remove_video(&key);
                capture.close().await;
            }
            return Ok(());
        }
        if let Some(camera) = &mut self.camera {
            if camera.capture.is_none() {
                camera.capture = Some(CameraCapture::start(camera.source.clone(), index).await?);
                camera.track.unmute();
                let sid = camera.track.sid();
                let rtc = camera.track.rtc_track();
                self.watch_video(key, sid, rtc);
            }
            return Ok(());
        }
        let source = NativeVideoSource::new(
            VideoResolution {
                width: 640,
                height: 480,
            },
            false,
        );
        let capture = CameraCapture::start(source.clone(), index).await?;
        let track =
            LocalVideoTrack::create_video_track("camera", RtcVideoSource::Native(source.clone()));
        let publication = participant
            .publish_track(
                LocalTrack::Video(track.clone()),
                TrackPublishOptions {
                    source: TrackSource::Camera,
                    ..Default::default()
                },
            )
            .await?;
        self.watch_video(key, publication.sid(), track.rtc_track());
        self.camera = Some(LocalCamera {
            track,
            source,
            capture: Some(capture),
        });
        Ok(())
    }

    fn refresh_participants(&self) {
        let local = Participant::Local(self.room.local_participant());
        let participants = self
            .room
            .remote_participants()
            .into_values()
            .map(Participant::Remote)
            .chain(std::iter::once(local));
        let next = participants
            .map(|participant| {
                let identity = participant.identity().to_string();
                let publications = participant.track_publications();
                let enabled = |source| {
                    publications
                        .values()
                        .any(|track| track.source() == source && !track.is_muted())
                };
                let microphone = enabled(TrackSource::Microphone);
                let state = ParticipantMedia {
                    camera: enabled(TrackSource::Camera),
                    microphone,
                    audio_level: if microphone {
                        self.speakers.get(&identity).copied().unwrap_or_default()
                    } else {
                        0.0
                    },
                };
                (identity, state)
            })
            .collect();
        let mut current = self.view.participants.lock().unwrap();
        if *current != next {
            *current = next;
            self.changes.send_modify(|n| *n += 1);
        }
    }

    fn set_connected(&self, connected: bool) {
        if self.view.connected.swap(connected, Ordering::Relaxed) != connected {
            self.changes.send_modify(|n| *n += 1);
        }
    }

    fn restored_local_tracks(&self, devices: Devices) -> bool {
        let publications = self.room.local_participant().track_publications();
        let published = |sid: &TrackSid| {
            publications.get(sid).and_then(|publication| {
                publication
                    .track()
                    .map(|_| (publication.source(), publication.is_muted()))
            })
        };
        local_track_is_restored(
            self.microphone.as_ref().map(LocalAudioTrack::sid),
            TrackSource::Microphone,
            devices.microphone,
            published,
        ) && local_track_is_restored(
            self.camera.as_ref().map(|camera| camera.track.sid()),
            TrackSource::Camera,
            devices.camera,
            published,
        )
    }

    fn observe(&mut self, event: RoomEvent, devices: Devices) -> bool {
        match event {
            RoomEvent::TrackSubscribed {
                track: RemoteTrack::Video(track),
                publication,
                participant,
            } if publication.source() == TrackSource::Camera && !publication.is_muted() => {
                self.watch_video(
                    participant.identity().to_string(),
                    publication.sid(),
                    track.rtc_track(),
                );
            }
            RoomEvent::TrackUnsubscribed {
                publication,
                participant,
                ..
            } => {
                let key = participant.identity().to_string();
                if self
                    .streams
                    .get(&key)
                    .is_some_and(|(sid, _)| sid == &publication.sid())
                {
                    self.remove_video(&key);
                }
            }
            RoomEvent::TrackMuted {
                publication: TrackPublication::Remote(publication),
                participant,
                ..
            } if publication.source() == TrackSource::Camera => {
                self.remove_video(&participant.identity().to_string());
            }
            RoomEvent::TrackUnmuted {
                publication: TrackPublication::Remote(publication),
                participant,
                ..
            } if publication.source() == TrackSource::Camera => {
                if let Some(RemoteTrack::Video(track)) = publication.track() {
                    self.watch_video(
                        participant.identity().to_string(),
                        publication.sid(),
                        track.rtc_track(),
                    );
                }
            }
            RoomEvent::ParticipantDisconnected(participant) => {
                let identity = participant.identity().to_string();
                self.speakers.remove(&identity);
                self.remove_video(&identity);
            }
            RoomEvent::ActiveSpeakersChanged { speakers } => {
                self.speakers = speakers
                    .iter()
                    .map(|participant| {
                        let level = participant.audio_level();
                        let level = if level.is_finite() {
                            level.clamp(0., 1.)
                        } else {
                            0.
                        };
                        (participant.identity().to_string(), level)
                    })
                    .collect();
            }
            RoomEvent::Reconnecting => {
                self.speakers.clear();
                self.set_connected(false);
            }
            // LiveKit can finish transport recovery even if a local track
            // failed to republish. Recreate the room connection in that case.
            RoomEvent::Reconnected => {
                if !self.restored_local_tracks(devices) {
                    return false;
                }
                self.set_connected(true);
            }
            RoomEvent::Disconnected { .. } => return false,
            _ => {}
        }
        self.refresh_participants();
        true
    }

    async fn close(mut self) {
        self.set_connected(false);
        for (_, (_, task)) in self.streams.drain() {
            task.abort();
        }
        if let Some(track) = self.microphone.take() {
            track.mute();
        }
        let _ = self.audio.stop_recording();
        if let Some(camera) = &mut self.camera {
            camera.track.mute();
            if let Some(capture) = camera.capture.take() {
                capture.close().await;
            }
        }
        let _ = self.room.close().await;
    }
}

fn local_track_is_restored(
    retained: Option<TrackSid>,
    source: TrackSource,
    enabled: bool,
    publication: impl FnOnce(&TrackSid) -> Option<(TrackSource, bool)>,
) -> bool {
    // A device that has never been started needs no publication. A retained
    // track must still be published, including when it is muted.
    match retained {
        Some(sid) => publication(&sid) == Some((source, !enabled)),
        None => !enabled,
    }
}

async fn run(
    credentials: ThreadCallCredentials,
    mut devices: Devices,
    mut selection: DeviceSelection,
    mut commands: mpsc::Receiver<MediaCommand>,
    ready: oneshot::Sender<Result<DeviceSelection>>,
    stop: CancellationToken,
    view: MediaView,
    changes: watch::Sender<u64>,
) {
    // Wait for capture and playback from the previous room to stop. A cancelled
    // join or a connection change can drop its UI owner before cleanup finishes.
    static DEVICES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _devices = tokio::select! {
        _ = stop.cancelled() => return,
        guard = DEVICES.lock() => guard,
    };
    let setup = async {
        let audio = PlatformAudio::new()?;
        if let Some(index) = selection.camera.clone() {
            let found = tokio::task::spawn_blocking(cameras).await;
            if !found.is_ok_and(|result| {
                result.is_ok_and(|found| {
                    found
                        .into_iter()
                        .any(|(_, device)| device == DeviceChoice::Camera(index.clone()))
                })
            }) {
                selection.camera = None;
            }
        }
        if let Some(id) = &selection.microphone
            && audio.set_recording_device(id).is_err()
        {
            selection.microphone = None;
        }
        if let Some(id) = &selection.speaker
            && audio.set_playout_device(id).is_err()
        {
            selection.speaker = None;
        }
        let (room, events) =
            Room::connect(&credentials.url, &credentials.token, RoomOptions::default()).await?;
        Ok::<_, anyhow::Error>((
            RoomMedia {
                room,
                audio,
                selection,
                microphone: None,
                camera: None,
                streams: HashMap::new(),
                view,
                speakers: HashMap::new(),
                changes,
            },
            events,
        ))
    };
    let (mut media, mut events) = tokio::select! {
        _ = stop.cancelled() => return,
        result = setup => match result {
            Ok(value) => value,
            Err(error) => { let _ = ready.send(Err(error)); return; }
        },
    };
    let opening = async {
        if devices.microphone {
            media.set_microphone(true).await?;
        }
        if devices.camera {
            media.set_camera(true).await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let result = tokio::select! {
        _ = stop.cancelled() => { media.close().await; return; }
        result = opening => result,
    };
    media.refresh_participants();
    let failed = result.is_err();
    media.set_connected(!failed);
    if ready.send(result.map(|_| media.selection.clone())).is_err() || failed {
        media.close().await;
        return;
    }
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            event = events.recv() => match event {
                Some(event) => if !media.observe(event, devices) { break; },
                None => break,
            },
            command = commands.recv() => {
                let (device, enabled, done) = match command {
                    Some(MediaCommand::Inventory(result)) => {
                        let inventory = media.inventory().await;
                        let _ = result.send(inventory);
                        continue;
                    }
                    Some(MediaCommand::Select { choice, done }) => {
                        let result = tokio::select! {
                            _ = stop.cancelled() => break,
                            outcome = media.select(choice, &mut devices) => outcome,
                        };
                        media.refresh_participants();
                        let _ = done.send(DeviceChange { result, camera_enabled: devices.camera });
                        continue;
                    }
                    Some(MediaCommand::Toggle { device, enabled, done }) => (device, enabled, done),
                    None => break,
                };
                // A command can reach the worker before its Reconnected event.
                // Close stale media instead of enabling an unpublished track.
                if enabled && !media.restored_local_tracks(devices) {
                    let _ = done.send(Err(anyhow::anyhow!("Local call publication was lost")));
                    break;
                }
                let change = async {
                    match device {
                        Device::Microphone => media.set_microphone(enabled).await,
                        Device::Camera => media.set_camera(enabled).await,
                    }
                };
                let outcome = tokio::select! {
                    _ = stop.cancelled() => break,
                    result = change => result,
                };
                if outcome.is_ok() {
                    *devices.preference_mut(device) = enabled;
                }
                let failed_off = !enabled && outcome.is_err();
                let _ = done.send(outcome);
                if failed_off {
                    break;
                }
            }
        }
    }
    media.close().await;
}

fn to_frame(frame: BoxVideoFrame) -> Frame {
    let width = frame.buffer.width();
    let height = frame.buffer.height();
    let mut bytes = vec![0; width as usize * height as usize * 4];
    frame.buffer.to_argb(
        VideoFormatType::ARGB,
        &mut bytes,
        width * 4,
        width as i32,
        height as i32,
    );
    let image = image::RgbaImage::from_raw(width, height, bytes).expect("packed video frame");
    let image = match frame.rotation {
        VideoRotation::VideoRotation0 => image,
        VideoRotation::VideoRotation90 => image::imageops::rotate90(&image),
        VideoRotation::VideoRotation180 => image::imageops::rotate180(&image),
        VideoRotation::VideoRotation270 => image::imageops::rotate270(&image),
    };
    Frame {
        width: image.width(),
        height: image.height(),
        bytes: image.into_raw(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_muted_publication_requires_restore_before_explicit_enable() {
        let retained_sid = TrackSid::try_from("TR_retained".to_owned()).unwrap();
        let old_sid = TrackSid::try_from("TR_stale".to_owned()).unwrap();
        for (device, source, other_source) in [
            (
                Device::Microphone,
                TrackSource::Microphone,
                TrackSource::Camera,
            ),
            (Device::Camera, TrackSource::Camera, TrackSource::Microphone),
        ] {
            let retained = || Some(retained_sid.clone());
            let stale = HashMap::from([(old_sid.clone(), (source, true))]);
            assert!(!local_track_is_restored(retained(), source, false, |_| {
                None
            }));
            assert!(!local_track_is_restored(retained(), source, false, |sid| {
                stale.get(sid).copied()
            }));
            assert!(!local_track_is_restored(retained(), source, false, |_| {
                Some((other_source, true))
            }));
            assert!(!local_track_is_restored(retained(), source, false, |_| {
                Some((source, false))
            }));
            assert!(local_track_is_restored(
                retained(),
                source,
                false,
                |_| Some((source, true))
            ));
            assert!(local_track_is_restored(retained(), source, true, |_| Some(
                (source, false)
            ),));
            // Restore opens the replacement room with this device off and no
            // retained track. Its next explicit enable takes the publish path.
            let mut restored = Devices {
                microphone: false,
                camera: false,
            };
            assert!(local_track_is_restored(None, source, false, |_| panic!(
                "off device has no lookup"
            )));
            assert!(restored.begin_toggle(device));
            assert!(!*restored.preference_mut(device));
            restored.confirm_enabled(device);
            assert!(*restored.preference_mut(device));
        }
    }

    #[test]
    fn restore_uses_off_even_when_the_device_command_never_replies() {
        for device in [Device::Microphone, Device::Camera] {
            let mut preferences = Devices {
                microphone: true,
                camera: true,
            };
            assert!(!preferences.begin_toggle(device));
            let mut restored = preferences;
            match device {
                Device::Microphone => assert!(!restored.microphone && restored.camera),
                Device::Camera => assert!(restored.microphone && !restored.camera),
            }
            assert!(restored.begin_toggle(device));
            assert!(!*restored.preference_mut(device));
            restored.confirm_enabled(device);
            assert!(restored.microphone && restored.camera);
        }
    }
}
