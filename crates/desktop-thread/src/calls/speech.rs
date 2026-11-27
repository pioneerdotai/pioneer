use std::time::{Duration, Instant};

// videocall-rs bea64063, call.css audio-indicator and glow-overlay.
const ATTACK: Duration = Duration::from_millis(120);
const RELEASE: Duration = Duration::from_millis(1400);
const MIC_HOLD: Duration = Duration::from_secs(1);
const MIC_RELEASE: Duration = Duration::from_millis(4900);

struct Transition {
    from: f32,
    target: f32,
    start: Instant,
    duration: Duration,
}

impl Transition {
    fn new(now: Instant) -> Self {
        Self {
            from: 0.,
            target: 0.,
            start: now,
            duration: Duration::ZERO,
        }
    }

    fn running(&self, now: Instant) -> bool {
        self.from != self.target && now.saturating_duration_since(self.start) < self.duration
    }

    fn value(&self, now: Instant) -> f32 {
        if !self.running(now) {
            return self.target;
        }
        let t =
            now.saturating_duration_since(self.start).as_secs_f32() / self.duration.as_secs_f32();
        self.from + (self.target - self.from) * gpui_kit::ease_in_out(t)
    }

    fn retarget(&mut self, target: f32, duration: Duration, now: Instant) {
        if self.target != target || duration.is_zero() {
            *self = Self {
                from: self.value(now),
                target,
                start: now,
                duration,
            };
        }
    }
}

pub(super) struct Speech {
    level: Transition,
    ring: Transition,
    mic: Transition,
    hold_until: Instant,
    was_speaking: bool,
}

#[derive(Default)]
pub(super) struct SpeechFrame {
    pub level: f32,
    pub ring: f32,
    pub mic: f32,
    pub animate: bool,
    pub reduced_motion: bool,
}

impl Speech {
    pub fn new(now: Instant) -> Self {
        Self {
            level: Transition::new(now),
            ring: Transition::new(now),
            mic: Transition::new(now),
            hold_until: now,
            was_speaking: false,
        }
    }

    pub fn update(
        &mut self,
        level: f32,
        microphone: bool,
        reduce_motion: bool,
        now: Instant,
    ) -> SpeechFrame {
        let speaking = microphone && level > 0.;
        if !microphone {
            self.hold_until = now;
        } else if self.was_speaking && !speaking {
            self.hold_until = now + MIC_HOLD;
        }
        self.was_speaking = speaking;
        let held = !speaking && now < self.hold_until;
        let envelope = if speaking { ATTACK } else { RELEASE };
        let duration = |time| if reduce_motion { Duration::ZERO } else { time };
        self.level
            .retarget(if speaking { level } else { 0. }, duration(envelope), now);
        self.ring
            .retarget(if speaking { 1. } else { 0. }, duration(envelope), now);
        let green_mic = speaking || held;
        self.mic.retarget(
            if green_mic { 1. } else { 0. },
            duration(if !microphone {
                Duration::ZERO
            } else if green_mic {
                ATTACK
            } else {
                MIC_RELEASE
            }),
            now,
        );
        SpeechFrame {
            level: self.level.value(now),
            ring: self.ring.value(now),
            mic: self.mic.value(now),
            // One last frame after the hold expires starts the release even on an audio-only call.
            animate: held
                || self.level.running(now)
                || self.ring.running(now)
                || self.mic.running(now),
            reduced_motion: reduce_motion,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glow_follows_level_and_releases_before_the_mic_tint() {
        let now = Instant::now();
        let mut speech = Speech::new(now);
        let start = speech.update(0.8, true, false, now);
        assert!(start.animate);
        assert_eq!(start.ring, 0.);
        let full = speech.update(0.8, true, false, now + ATTACK);
        assert_eq!((full.ring, full.level, full.mic), (1., 0.8, 1.));
        assert!(!full.animate);
        let quieter = now + ATTACK;
        speech.update(0.3, true, false, quieter);
        assert_eq!(speech.update(0.3, true, false, quieter + ATTACK).level, 0.3);

        let silence = quieter + ATTACK;
        speech.update(0., true, false, silence);
        let fading = speech.update(0., true, false, silence + MIC_HOLD / 2);
        assert!(fading.ring > 0. && fading.ring < 1.);
        assert_eq!(fading.mic, 1.);
        let release_mic = silence + MIC_HOLD;
        speech.update(0., true, false, release_mic);
        let no_ring = speech.update(0., true, false, silence + RELEASE);
        assert_eq!(no_ring.ring, 0.);
        assert!(no_ring.mic > 0. && no_ring.mic < 1.);
        assert!(
            !speech
                .update(0., true, false, release_mic + MIC_RELEASE)
                .animate
        );
    }

    #[test]
    fn reduced_motion_is_steady_and_mute_clears_the_mic_tint() {
        let now = Instant::now();
        let mut speech = Speech::new(now);
        let steady = speech.update(0.2, true, true, now);
        assert!(steady.reduced_motion && !steady.animate);
        assert_eq!((steady.ring, steady.mic), (1., 1.));
        let muted = speech.update(0., false, true, now);
        assert_eq!((muted.ring, muted.mic), (0., 0.));
        assert!(!muted.animate);
    }
}
