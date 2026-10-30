//! Voice-only capacity retained across the existing command FIFOs and socket write.
use std::sync::{Arc, Mutex};

const MAX_VOICE_BYTES: usize = 256 * 1024;
const MAX_VOICE_FRAMES: usize = 128;

#[derive(Default)]
pub(super) struct VoiceTransportBudget(Mutex<(usize, usize)>);

pub(super) struct VoiceReservation {
    budget: Arc<VoiceTransportBudget>,
    bytes: usize,
}

/// Owned voice command payload. Constructed only by the transport's admission budget.
pub struct QueuedVoiceAudio {
    pub(super) bytes: Vec<u8>,
    pub(super) reservation: VoiceReservation,
}

impl QueuedVoiceAudio {
    pub(super) fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl VoiceTransportBudget {
    pub(super) fn admit(self: &Arc<Self>, bytes: Vec<u8>) -> anyhow::Result<QueuedVoiceAudio> {
        let mut used = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("Voice transport capacity unavailable"))?;
        let next_bytes = used
            .0
            .checked_add(bytes.len())
            .filter(|next| *next <= MAX_VOICE_BYTES)
            .ok_or_else(|| anyhow::anyhow!("Voice transport byte capacity exceeded"))?;
        anyhow::ensure!(
            used.1 < MAX_VOICE_FRAMES,
            "Voice transport frame capacity exceeded"
        );
        *used = (next_bytes, used.1 + 1);
        Ok(QueuedVoiceAudio {
            reservation: VoiceReservation {
                budget: self.clone(),
                bytes: bytes.len(),
            },
            bytes,
        })
    }
}

impl Drop for VoiceReservation {
    fn drop(&mut self) {
        let mut used = self
            .budget
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        used.0 -= self.bytes;
        used.1 -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_and_frame_capacity_are_released_only_with_the_owned_payload() {
        let budget = Arc::new(VoiceTransportBudget::default());
        let full = budget.admit(vec![0; MAX_VOICE_BYTES]).unwrap();
        assert!(budget.admit(vec![0]).is_err());
        drop(full);
        let mut tiny = Vec::new();
        for _ in 0..MAX_VOICE_FRAMES {
            tiny.push(budget.admit(vec![0]).unwrap());
        }
        assert!(budget.admit(vec![0]).is_err());
        tiny.pop();
        assert!(budget.admit(vec![0]).is_ok());
        drop(tiny);
        assert!(budget.admit(vec![0; MAX_VOICE_BYTES]).is_ok());
    }
}
