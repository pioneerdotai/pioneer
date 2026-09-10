//! Native capture handles for mounted thread features.
use super::{
    capture::{
        DesktopComposerVoiceGateway, DesktopVoiceCaptureConfig, DesktopVoiceCaptureError,
        DesktopVoiceCaptureErrorKind, DesktopVoiceCaptureFlow, PlatformDesktopAudioInputBackend,
    },
    microphone::{
        DesktopMicrophoneFormatRequest, PlatformDesktopMicrophoneDeviceProbe,
        verify_desktop_microphone_ready,
    },
};
use gpui_kit::{App, AppContext, Task};
use pioneer_client::{composer::turn_prepare::PreparedVoiceComposerSnapshot, core::ClientCore};
use pioneer_desktop_thread::{
    ThreadAudioCompletion, ThreadAudioError, ThreadAudioErrorKind, ThreadAudioPort,
    ThreadAudioRequest,
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

type CaptureFlow =
    DesktopVoiceCaptureFlow<PlatformDesktopAudioInputBackend, DesktopComposerVoiceGateway>;
type CaptureKey = (String, u64, u64);
#[derive(Default)]
struct CaptureSlot {
    cancelled: AtomicBool,
    flow: Mutex<Option<CaptureFlow>>,
}
impl CaptureSlot {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        let flow = self.flow.lock().expect("native capture poisoned").take();
        drop(flow);
    }
    fn take(&self) -> Option<CaptureFlow> {
        if self.cancelled.load(Ordering::Acquire) {
            return None;
        }
        self.flow.lock().expect("native capture poisoned").take()
    }
    fn keep(&self, flow: CaptureFlow) -> bool {
        let mut current = self.flow.lock().expect("native capture poisoned");
        if self.cancelled.load(Ordering::Acquire) {
            return false;
        }
        *current = Some(flow);
        true
    }
}
#[derive(Default)]
struct CaptureSlots(Mutex<HashMap<CaptureKey, Arc<CaptureSlot>>>);
impl CaptureSlots {
    fn retire(&self, thread: &str, mount: u64) {
        let retired = {
            let mut slots = self.0.lock().expect("native captures poisoned");
            let keys = slots
                .keys()
                .filter(|(id, owner, _)| id == thread && *owner == mount)
                .cloned()
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| slots.remove(&key))
                .collect::<Vec<_>>()
        };
        for slot in retired {
            slot.cancel();
        }
    }
}
impl Drop for CaptureSlots {
    fn drop(&mut self) {
        for (_, slot) in self.0.get_mut().expect("native captures poisoned").drain() {
            slot.cancel();
        }
    }
}

pub(crate) struct DesktopThreadAudioPort {
    client: Arc<ClientCore>,
    slots: Arc<CaptureSlots>,
}
impl DesktopThreadAudioPort {
    pub(crate) fn new(client: Arc<ClientCore>) -> Self {
        Self {
            client,
            slots: Arc::default(),
        }
    }
    fn key(request: &ThreadAudioRequest) -> CaptureKey {
        let operation = request.presentation();
        (
            operation.thread_id().into(),
            operation.mount(),
            operation.generation(),
        )
    }
    fn matches(&self, request: &ThreadAudioRequest) -> bool {
        request.presentation().thread_id() == request.plan().identity.thread_id
            && self
                .client
                .composer_operation_plan(&request.plan().identity)
                .as_ref()
                == Some(request.plan())
    }
    fn slot(&self, request: &ThreadAudioRequest) -> Option<Arc<CaptureSlot>> {
        self.slots
            .0
            .lock()
            .expect("native captures poisoned")
            .get(&Self::key(request))
            .cloned()
    }
}
impl ThreadAudioPort for DesktopThreadAudioPort {
    fn start_capture(
        &self,
        request: ThreadAudioRequest,
        cx: &mut App,
    ) -> Task<Result<ThreadAudioCompletion, ThreadAudioError>> {
        if !self.matches(&request) || self.slot(&request).is_some() {
            return Task::ready(Ok(ThreadAudioCompletion::Cancelled));
        }
        let Some(context) = request.plan().voice_start.clone() else {
            return Task::ready(Ok(ThreadAudioCompletion::Cancelled));
        };
        // A mounted composer can retain only its current native recording.
        self.slots.retire(
            request.presentation().thread_id(),
            request.presentation().mount(),
        );
        let slot = Arc::new(CaptureSlot::default());
        self.slots
            .0
            .lock()
            .expect("native captures poisoned")
            .insert(Self::key(&request), slot.clone());
        let client = self.client.clone();
        cx.background_spawn(async move {
            if slot.cancelled.load(Ordering::Acquire) {
                return Ok(ThreadAudioCompletion::Cancelled);
            }
            client
                .prepare_composer_voice_capture(request.plan().identity.clone())
                .map_err(|error| {
                    ThreadAudioError::new(
                        ThreadAudioErrorKind::GatewaySession,
                        format!("{error:#}"),
                    )
                })?;
            if slot.cancelled.load(Ordering::Acquire) {
                return Ok(ThreadAudioCompletion::Cancelled);
            }
            let gate = verify_desktop_microphone_ready(
                &PlatformDesktopMicrophoneDeviceProbe,
                DesktopMicrophoneFormatRequest::default(),
            );
            let gateway =
                DesktopComposerVoiceGateway::new(client.clone(), request.plan().identity.clone());
            let mut flow = DesktopVoiceCaptureFlow::new(PlatformDesktopAudioInputBackend, gateway);
            flow.start(&gate, DesktopVoiceCaptureConfig::default(), context)
                .map_err(capture_error)?;
            if client
                .composer_operation_plan(&request.plan().identity)
                .is_none()
                || !slot.keep(flow)
            {
                return Ok(ThreadAudioCompletion::Cancelled);
            }
            Ok(ThreadAudioCompletion::CaptureReady)
        })
    }
    fn stop_recording(
        &self,
        request: &ThreadAudioRequest,
    ) -> Result<ThreadAudioCompletion, ThreadAudioError> {
        if !self.matches(request) {
            self.cancel_capture(request);
            return Ok(ThreadAudioCompletion::Cancelled);
        }
        let Some(slot) = self.slot(request) else {
            return Ok(ThreadAudioCompletion::Cancelled);
        };
        let Some(mut flow) = slot.take() else {
            return Ok(ThreadAudioCompletion::Cancelled);
        };
        flow.stop_recording().map_err(capture_error)?;
        Ok(if slot.keep(flow) {
            ThreadAudioCompletion::RecordingStopped
        } else {
            ThreadAudioCompletion::Cancelled
        })
    }
    fn finalize_capture(
        &self,
        request: ThreadAudioRequest,
        prepared: PreparedVoiceComposerSnapshot,
        cx: &mut App,
    ) -> Task<Result<ThreadAudioCompletion, ThreadAudioError>> {
        if !self.matches(&request) {
            self.cancel_capture(&request);
            return Task::ready(Ok(ThreadAudioCompletion::Cancelled));
        }
        let Some(slot) = self.slot(&request) else {
            return Task::ready(Ok(ThreadAudioCompletion::Cancelled));
        };
        let Some(mut flow) = slot.take() else {
            return Task::ready(Ok(ThreadAudioCompletion::Cancelled));
        };
        let slots = Arc::downgrade(&self.slots);
        let key = Self::key(&request);
        cx.background_spawn(async move {
            if slot.cancelled.load(Ordering::Acquire) {
                return Ok(ThreadAudioCompletion::Cancelled);
            }
            let result = flow.finalize_send(prepared.context).map_err(capture_error);
            if let Some(slots) = slots.upgrade() {
                slots
                    .0
                    .lock()
                    .expect("native captures poisoned")
                    .remove(&key);
            }
            result.map(|_| {
                if slot.cancelled.load(Ordering::Acquire) {
                    ThreadAudioCompletion::Cancelled
                } else {
                    ThreadAudioCompletion::Finalized
                }
            })
        })
    }
    fn cancel_capture(&self, request: &ThreadAudioRequest) {
        let slot = self
            .slots
            .0
            .lock()
            .expect("native captures poisoned")
            .remove(&Self::key(request));
        if let Some(slot) = slot {
            slot.cancel();
        }
    }
    fn retire_mount(&self, thread_id: &str, mount: u64) {
        self.slots.retire(thread_id, mount);
    }
}

fn capture_error(error: DesktopVoiceCaptureError) -> ThreadAudioError {
    use DesktopVoiceCaptureErrorKind as Native;
    use ThreadAudioErrorKind as Port;
    let kind = match error.kind {
        Native::PermissionDenied => Port::PermissionDenied,
        Native::NoInputDevice => Port::NoInputDevice,
        Native::DeviceBusy => Port::DeviceBusy,
        Native::UnsupportedFormat => Port::UnsupportedFormat,
        Native::DeviceInterrupted => Port::DeviceInterrupted,
        Native::AlreadyCapturing => Port::AlreadyCapturing,
        Native::GatewaySession => Port::GatewaySession,
        Native::GatewayChunk => Port::GatewayChunk,
        Native::GatewayFinalize => Port::GatewayFinalize,
        #[cfg(test)]
        Native::NotCapturing => Port::GatewaySession,
    };
    ThreadAudioError::new(kind, error.message)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retiring_a_mount_cancels_pending_callbacks_without_retiring_other_mounts() {
        let slots = CaptureSlots::default();
        let a = Arc::new(CaptureSlot::default());
        let b = Arc::new(CaptureSlot::default());
        slots
            .0
            .lock()
            .unwrap()
            .insert(("thread".into(), 1, 1), a.clone());
        slots
            .0
            .lock()
            .unwrap()
            .insert(("thread".into(), 2, 1), b.clone());
        slots.retire("thread", 1);
        assert!(a.cancelled.load(Ordering::Acquire));
        assert!(!b.cancelled.load(Ordering::Acquire));
        assert_eq!(slots.0.lock().unwrap().len(), 1);
        drop(slots);
        assert!(b.cancelled.load(Ordering::Acquire));
    }
    #[test]
    fn native_error_copy_and_category_cross_the_port_unchanged() {
        let error = capture_error(DesktopVoiceCaptureError::new(
            DesktopVoiceCaptureErrorKind::GatewayFinalize,
            "synthetic localized copy",
        ));
        assert_eq!(error.kind(), ThreadAudioErrorKind::GatewayFinalize);
        assert_eq!(error.message(), "synthetic localized copy");
    }
}
