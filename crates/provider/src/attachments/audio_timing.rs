//! Confirmed elementary-stream counts; never Symphonia bitrate estimates.
//! Pinned primary contracts: symphonia-bundle-mp3 0.6.1 header.rs/demuxer.rs;
//! symphonia-codec-aac 0.6.1 adts.rs. No resync, decoder, inference, or I/O.
use anyhow::{Context, Result, ensure};
use symphonia::core::io::Monitor;
const MAX_FRAMES: usize = 1_000_000;

fn leading_id3(bytes: &[u8]) -> Result<usize> {
    if !bytes.starts_with(b"ID3") {
        return Ok(0);
    }
    ensure!(
        bytes.len() >= 10 && matches!(bytes[3], 2..=4),
        "unproven MP3 ID3 extent"
    );
    ensure!(
        bytes[6..10].iter().all(|b| b & 0x80 == 0),
        "invalid MP3 ID3 size"
    );
    let size = bytes[6..10]
        .iter()
        .fold(0usize, |n, b| (n << 7) | usize::from(*b));
    let end = 10usize
        .checked_add(size)
        .context("MP3 metadata extent overflow")?;
    let footer = bytes[3] == 4 && bytes[5] & 0x10 != 0;
    let end = if footer {
        ensure!(
            bytes.get(end..end + 3) == Some(b"3DI"),
            "invalid MP3 ID3 footer"
        );
        end.checked_add(10)
            .context("MP3 metadata extent overflow")?
    } else {
        end
    };
    ensure!(end <= bytes.len(), "truncated MP3 metadata");
    Ok(end)
}

pub(super) fn mp3_samples(bytes: &[u8]) -> Result<(u64, u32)> {
    let mut offset = leading_id3(bytes)?;
    let mut end = bytes.len();
    if end >= 128 && &bytes[end - 128..end - 125] == b"TAG" {
        end -= 128;
    }
    let mut frames = 0u64;
    let mut rate = None;
    let mut version = None;
    let mut declared = None;
    let mut xing = false;
    let mut trim = 0u64;
    let mut samples_per_frame = 0u64;
    while offset < end {
        ensure!(frames < MAX_FRAMES as u64, "MP3 frame quantum exceeded");
        let h = bytes
            .get(offset..offset + 4)
            .filter(|_| offset + 4 <= end)
            .context("truncated MP3 frame header")?;
        ensure!(
            h[0] == 0xff && h[1] & 0xe0 == 0xe0 && (h[1] >> 1) & 3 == 1,
            "unproven MP3 Layer III frame boundary"
        );
        let v = (h[1] >> 3) & 3;
        let frequency = (h[2] >> 2) & 3;
        let bitrate = h[2] >> 4;
        ensure!(
            v != 1 && frequency < 3 && (1..15).contains(&bitrate),
            "unsupported MP3 frame timing"
        );
        let r = [44100u32, 48000, 32000][usize::from(frequency)]
            / match v {
                3 => 1,
                2 => 2,
                _ => 4,
            };
        ensure!(
            rate.is_none_or(|old| old == r) && version.is_none_or(|old| old == v),
            "changing MP3 sample-rate/version timeline is not proven"
        );
        rate = Some(r);
        version = Some(v);
        let kbps = if v == 3 {
            [
                0u32, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
            ][usize::from(bitrate)]
        } else {
            [
                0u32, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160,
            ][usize::from(bitrate)]
        };
        let len = ((if v == 3 { 144 } else { 72 }) * kbps * 1000 / r + u32::from((h[2] >> 1) & 1))
            as usize;
        let frame_end = offset
            .checked_add(len)
            .context("MP3 frame extent overflow")?;
        ensure!(len >= 4 && frame_end <= end, "truncated MP3 frame payload");
        let frame = &bytes[offset..frame_end];
        let mono = h[3] >> 6 == 3;
        let side = match (v == 3, mono) {
            (true, true) => 17,
            (true, false) => 32,
            (false, true) => 9,
            _ => 17,
        };
        let frame_samples = if v == 3 { 1152u64 } else { 576 };
        let tag_at = 4 + side;
        // Pinned MpaReader candidate contract (demuxer.rs:940–967,1021–1045):
        // signature alone can be ordinary ancillary data. Prefix/extent decide
        // classification BEFORE placement/protection/count/trim policies.
        // header_size skips optional CRC; the tag offsets themselves use the
        // fixed four-byte MPEG header, as in the pinned contract.
        let header_size = 4 + if h[1] & 1 == 0 { 2 } else { 0 };
        let is_vbri = frame.len() >= 36 + 26
            && frame.get(36..40) == Some(b"VBRI")
            && frame[header_size..36].iter().all(|b| *b == 0);
        ensure!(!is_vbri, "MP3 VBRI priming/count contract is not proven");
        let is_xing = frame.len() >= tag_at + 8
            && matches!(frame.get(tag_at..tag_at + 4), Some(b"Xing" | b"Info"))
            && frame[header_size..tag_at].iter().all(|b| *b == 0);
        if is_xing {
            ensure!(
                frames == 0 && !xing && h[1] & 1 != 0,
                "MP3 embedded/protected info-tag timeline is not proven"
            );
            let flags = frame
                .get(tag_at + 4..tag_at + 8)
                .context("truncated MP3 info tag")?;
            let flags = u32::from_be_bytes(flags.try_into()?);
            ensure!(
                flags & !15 == 0 && flags & 1 != 0,
                "MP3 info counter provenance unavailable"
            );
            declared = Some(u32::from_be_bytes(
                frame
                    .get(tag_at + 8..tag_at + 12)
                    .context("truncated MP3 frame counter")?
                    .try_into()?,
            ));
            let fields = 8
                + 4
                + if flags & 2 != 0 { 4 } else { 0 }
                + if flags & 4 != 0 { 100 } else { 0 }
                + if flags & 8 != 0 { 4 } else { 0 };
            ensure!(tag_at + fields <= len, "truncated MP3 info fields");
            let extension_at = tag_at + fields;
            let extension = &frame[extension_at..];
            if extension.iter().any(|b| *b != 0) {
                // The pinned parser accepts absent/zero CRCs. Hard proof here
                // deliberately requires the full, nonzero, matching tag CRC.
                // Known encoder trims are sample counts, not packet-end bounds
                // rounded/clamped by the demuxer or a guessed bitrate.
                ensure!(
                    extension.len() >= 36 && matches!(&extension[..4], b"LAME" | b"Lavf" | b"Lavc"),
                    "MP3 encoder trim representation is not proven"
                );
                let expected = u16::from_be_bytes(extension[34..36].try_into()?);
                let mut crc = symphonia::core::checksum::Crc16AnsiLe::new(0);
                crc.process_buf_bytes(&frame[..extension_at + 34]);
                ensure!(
                    expected != 0 && expected == crc.crc(),
                    "MP3 encoder trim CRC is not proven"
                );
                let packed = (u32::from(extension[21]) << 16)
                    | (u32::from(extension[22]) << 8)
                    | u32::from(extension[23]);
                let delay = u64::from(packed >> 12);
                let padding = u64::from(packed & 4095);
                // Symphonia 0.6.1 demuxer.rs:848–855 explicitly applies
                // leading=529+delay, trailing=max(padding-529,0) to these
                // three labels. CRC proves bytes, not this semantic domain.
                // Reject the non-cancelling domain instead of treating the
                // raw sum as measured presentation samples.
                const DECODER_DELAY: u64 = 529;
                let trailing = padding
                    .checked_sub(DECODER_DELAY)
                    .context("MP3 encoder padding domain is not proven")?;
                if &extension[..4] == b"LAME" {
                    // Only the default completed LAME3.100 profile is proved:
                    // encoder.h ENCDELAY=576; initialization sets that delay;
                    // lame_encode_flush yields padding in
                    // [576, 576+samples_per_frame-1]. Other builds/versions
                    // need their own evidence, not an encoder label or CRC.
                    // https://github.com/lameproject/lame/blob/master/libmp3lame/lame.c
                    // https://github.com/lameproject/lame/blob/master/libmp3lame/encoder.h
                    ensure!(
                        &extension[..9] == b"LAME3.100"
                            && delay == 576
                            && (576..=frame_samples + 575).contains(&padding),
                        "MP3 completed LAME trim domain is not proven"
                    );
                }
                // Lavf/Lavc use the explicitly pinned decoder tag contract,
                // padding>=529, not LAME's encoder-specific minimum576.
                // No claim that the bytes were produced by a given encoder.
                trim = delay
                    .checked_add(DECODER_DELAY)
                    .and_then(|leading| leading.checked_add(trailing))
                    .context("MP3 trim overflow")?;
            }
            xing = true;
        } else {
            frames = frames.checked_add(1).context("MP3 frame count overflow")?;
        }
        samples_per_frame = frame_samples;
        offset = frame_end;
    }
    ensure!(frames > 0, "MP3 confirmed frames unavailable");
    let mut samples = frames
        .checked_mul(samples_per_frame)
        .context("MP3 sample count overflow")?;
    if xing {
        ensure!(
            declared.map(u64::from) == Some(frames),
            "MP3 declared/scanned frame count mismatch"
        );
        // No library estimated count or PacketBuilder::trimmed_dur is used.
        // Declared trims apply only after full scan/count/CRC AND domain proof.
        samples = samples
            .checked_sub(trim)
            .context("MP3 trim exceeds confirmed samples")?;
    }
    ensure!(samples > 0, "MP3 confirmed span unavailable");
    Ok((
        samples,
        rate.context("MP3 confirmed sample rate unavailable")?,
    ))
}

pub(super) fn adts_samples(bytes: &[u8]) -> Result<(u64, u32)> {
    let mut offset = 0usize;
    let mut frames = 0u64;
    let mut config = None;
    const RATES: [u32; 13] = [
        96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350,
    ];
    while offset < bytes.len() {
        ensure!(frames < MAX_FRAMES as u64, "ADTS frame quantum exceeded");
        let h = bytes
            .get(offset..offset + 7)
            .context("truncated ADTS header")?;
        ensure!(
            h[0] == 0xff && h[1] & 0xf6 == 0xf0,
            "unproven ADTS frame boundary"
        );
        let frequency = (h[2] >> 2) & 15;
        let channels = ((h[2] & 1) << 2) | (h[3] >> 6);
        ensure!(
            h[1] & 1 != 0 && h[2] >> 6 == 1 && frequency < 13 && channels > 0 && h[6] & 3 == 0,
            "ADTS timing requires unprotected AAC-LC single raw block with explicit channels"
        );
        let key = (frequency, channels, h[1] & 8);
        ensure!(
            config.is_none_or(|old| old == key),
            "changing ADTS configuration timeline is not proven"
        );
        config = Some(key);
        let len = (usize::from(h[3] & 3) << 11) | (usize::from(h[4]) << 3) | usize::from(h[5] >> 5);
        let end = offset
            .checked_add(len)
            .context("ADTS frame extent overflow")?;
        ensure!(
            len > 7 && end <= bytes.len(),
            "truncated ADTS frame payload"
        );
        frames = frames.checked_add(1).context("ADTS frame count overflow")?;
        offset = end;
    }
    ensure!(frames > 0, "ADTS confirmed frames unavailable");
    let samples = frames
        .checked_mul(1024)
        .context("ADTS sample count overflow")?;
    Ok((
        samples,
        RATES[usize::from(config.context("ADTS sample rate unavailable")?.0)],
    ))
}

#[cfg(test)]
mod ancillary_classification_regressions {
    use super::*;
    use crate::attachments::{
        input_estimate::duration_millis,
        media_fixtures::{
            ordinary_mp3_collision, rejected_mp3_tag_candidates, trimmed_xing_mp3, xing_mp3,
        },
    };
    #[test]
    fn ignored_private_bit_and_ancillary_magic_preserve_first_and_later_sample_clock() {
        for magic in [b"Info", b"Xing", b"VBRI"] {
            for index in [0, 37] {
                let bytes = ordinary_mp3_collision(100, index, magic);
                assert_eq!(mp3_samples(&bytes).unwrap(), (115200, 48000));
                assert_eq!(duration_millis(&bytes, "audio/mpeg").unwrap(), 2400);
                assert_eq!(
                    duration_millis(&ordinary_mp3_collision(101, index, magic), "audio/mpeg")
                        .unwrap(),
                    2424
                );
            }
        }
        // One ordinary MPEG1 frame has1152/48000=24ms independently.
        for magic in [b"Info", b"Xing", b"VBRI"] {
            assert_eq!(
                duration_millis(&ordinary_mp3_collision(1, 0, magic), "audio/mpeg").unwrap(),
                24
            );
        }
    }
    #[test]
    fn minimum_extent_and_version_layout_precede_tag_only_policies() {
        // Valid zero-side-data MPEG2/24kHz stereo 8kbps +padding =>25bytes.
        // Xing magic fits, full minimum eight-byte tag does not. 576 samples.
        let mut short_xing = vec![0; 25];
        short_xing[..4].copy_from_slice(&[0xff, 0xf3, 0x16, 0]);
        short_xing[21..25].copy_from_slice(b"Xing");
        assert_eq!(mp3_samples(&short_xing).unwrap(), (576, 24000));
        assert_eq!(duration_millis(&short_xing, "audio/mpeg").unwrap(), 24);
        // MPEG2/24kHz mono16kbps =>48bytes; VBRI magic fits but full26-byte
        // metadata extent after36 does not. Zero coded lengths, ordinary audio.
        let mut short_vbri = vec![0; 48];
        short_vbri[..4].copy_from_slice(&[0xff, 0xf3, 0x24, 0xc0]);
        short_vbri[36..40].copy_from_slice(b"VBRI");
        assert_eq!(mp3_samples(&short_vbri).unwrap(), (576, 24000));
        assert_eq!(duration_millis(&short_vbri, "audio/mpeg").unwrap(), 24);
    }
    #[test]
    fn actual_candidates_keep_count_placement_protection_extent_and_trim_guards() {
        for magic in [b"Xing", b"Info"] {
            let mut valid = xing_mp3(100, 100);
            valid[21..25].copy_from_slice(magic);
            assert_eq!(mp3_samples(&valid).unwrap(), (115200, 48000));
            // Source-backed completed LAME: 100*1152 -576 -576, independently.
            let trimmed = trimmed_xing_mp3(magic);
            assert_eq!(mp3_samples(&trimmed).unwrap(), (114048, 48000));
            assert_eq!(duration_millis(&trimmed, "audio/mpeg").unwrap(), 2376);
        }
        for bytes in rejected_mp3_tag_candidates() {
            assert!(mp3_samples(&bytes).is_err());
        }
        // Existing Lavc count/CRC trim proof remains exercised separately.
        assert!(mp3_samples(crate::attachments::regression::mp3()).is_ok());
        let mut crc = crate::attachments::regression::mp3().to_vec();
        crc[0xb9 + 5] ^= 1;
        assert!(mp3_samples(&crc).is_err());
    }
}

#[cfg(test)]
mod encoder_trim_domain_regressions {
    use super::*;
    use crate::attachments::{
        input_estimate::{duration_millis, native_duration},
        media_fixtures::{
            confirmed_mp3_trims, encoder_trim_mp3, encoder_trim_mp3_mpeg2, unproven_mp3_trims,
        },
    };
    #[test]
    fn supported_domains_have_independent_samples_and_exact_hard_bounds() {
        for magic in [b"Info", b"Xing"] {
            for (bytes, samples, rate, millis) in confirmed_mp3_trims(magic) {
                assert_eq!(mp3_samples(&bytes).unwrap(), (samples, rate));
                let duration = native_duration(&bytes, "audio/mpeg").unwrap();
                assert_eq!(duration_millis(&bytes, "audio/mpeg").unwrap(), millis);
                assert!(duration.within_millis(millis).unwrap());
                assert!(!duration.within_millis(millis - 1).unwrap());
            }
        }
    }
    #[test]
    fn crc_valid_bytes_do_not_prove_low_padding_or_completed_encoder_domain() {
        for magic in [b"Info", b"Xing"] {
            for bytes in unproven_mp3_trims(magic) {
                assert!(mp3_samples(&bytes).is_err());
            }
            // The former positive must reach the domain guard after valid CRC,
            // not fail a checksum/counter check that hides this regression.
            let former_positive = encoder_trim_mp3(magic, b"LAME3.100", 100, 100, 200);
            assert!(
                mp3_samples(&former_positive)
                    .unwrap_err()
                    .to_string()
                    .contains("padding domain")
            );
            for encoder in [b"LAME3.100", b"Lavf62.11", b"Lavc62.11"] {
                for padding in [0, 200, 528] {
                    let bytes = encoder_trim_mp3(magic, encoder, 100, 576, padding);
                    assert!(
                        mp3_samples(&bytes)
                            .unwrap_err()
                            .to_string()
                            .contains("padding domain")
                    );
                }
            }
            for padding in [529, 575, 1728] {
                let bytes = encoder_trim_mp3(magic, b"LAME3.100", 100, 576, padding);
                assert!(
                    mp3_samples(&bytes)
                        .unwrap_err()
                        .to_string()
                        .contains("completed LAME trim domain")
                );
            }
            assert!(mp3_samples(&encoder_trim_mp3_mpeg2(magic, b"LAME3.100", 576, 1152)).is_err());
            let underflow = encoder_trim_mp3(magic, b"LAME3.100", 1, 576, 1152);
            assert!(
                mp3_samples(&underflow)
                    .unwrap_err()
                    .to_string()
                    .contains("trim exceeds confirmed samples")
            );
        }
    }
}
