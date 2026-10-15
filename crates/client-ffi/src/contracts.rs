#[cfg(test)]
mod tests {
    use crate::ClientFfiVoiceAudioChunkParams;

    #[test]
    fn mobile_nitro_voice_array_buffer_uses_the_shared_binary_frame_contract() {
        let input = serde_json::json!({
            "operation": { "thread_id": "thread", "draft_id": 1, "generation": 2 },
            "session_id": "voice_mobile_binary_1",
            "sequence": 7,
            "audio_format": {
                "sample_rate_hz": 16000,
                "channels": 1,
                "encoding": "pcm_s16_le"
            },
            "captured_at_unix_ms": 1_725_000_000_020_u64,
            "duration_ms": 20
        });
        let params: ClientFfiVoiceAudioChunkParams =
            serde_json::from_value(input).expect("Pioneer App voice JSON contract");
        let array_buffer_bytes = [0x00, 0x80, 0xff, 0x7f];
        let frame = pioneer_client::transport::ws::frames::encode_voice_audio_chunk_frame(
            params.session_id,
            params.sequence,
            params.audio_format,
            params.captured_at_unix_ms,
            params.duration_ms,
            array_buffer_bytes.as_slice(),
        )
        .expect("Nitro ArrayBuffer bytes should enter the shared voice frame encoder");

        let decoded = pioneer_protocol::decode_voice_chunk_frame(frame.as_slice())
            .expect("Gateway-compatible voice frame");
        assert_eq!(decoded.header.session_id, "voice_mobile_binary_1");
        assert_eq!(decoded.header.sequence, 7);
        assert_eq!(decoded.audio_payload, array_buffer_bytes);
    }
}
