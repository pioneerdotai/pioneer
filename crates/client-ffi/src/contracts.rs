#[cfg(test)]
mod tests {
    use crate::{ClientFfiVoiceAudioChunkParams, voice_capture::VoiceCaptureAction};

    #[test]
    fn mobile_nitro_voice_metadata_carries_actual_source_format_without_samples() {
        let params: ClientFfiVoiceAudioChunkParams = serde_json::from_value(serde_json::json!({
            "operation": { "thread_id": "thread", "draft_id": 1, "generation": 2 },
            "session_id": "voice_mobile_binary_1",
            "action": "process",
            "input_sample_rate_hz": 48000,
            "captured_at_unix_ms": 1_725_000_000_020_u64
        }))
        .expect("Pioneer App binary source metadata");
        assert_eq!(params.action, VoiceCaptureAction::Process);
        assert_eq!(params.input_sample_rate_hz, Some(48000));
        // Source samples exist only in the binary argument, never a JSON fallback.
        assert!(
            serde_json::from_value::<ClientFfiVoiceAudioChunkParams>(serde_json::json!({
                "operation": { "thread_id": "thread", "draft_id": 1, "generation": 2 },
                "session_id": "voice_mobile_binary_1", "action": "process", "samples": [0.5]
            }))
            .is_err()
        );
    }
}
