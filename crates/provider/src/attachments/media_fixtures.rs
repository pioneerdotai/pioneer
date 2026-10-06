//! Valid containers built only during future authorized tests; no codecs run.
#[cfg(test)]
pub(crate) fn webm(audio: bool, video: bool, doc: &str) -> Vec<u8> {
    webm_timeline(audio, video, doc, TimingFixture::default())
}
#[derive(Clone, Copy)]
pub(crate) struct TimingFixture {
    pub cluster_timestamp: u64,
    pub audio_start: i16,
    pub video_start: i16,
    pub video_duration: u64,
    pub declared_duration: Option<f64>,
    pub timestamp_scale: u32,
    pub track_scale: f64,
    pub codec_delay: u64,
}
impl Default for TimingFixture {
    fn default() -> Self {
        Self {
            cluster_timestamp: 0,
            audio_start: 0,
            video_start: 0,
            video_duration: 1000,
            declared_duration: None,
            timestamp_scale: 1_000_000,
            track_scale: 1.0,
            codec_delay: 0,
        }
    }
}
pub(crate) fn webm_timeline(audio: bool, video: bool, doc: &str, timing: TimingFixture) -> Vec<u8> {
    fn element(id: &[u8], data: &[u8]) -> Vec<u8> {
        assert!(data.len() < 16383);
        [id, &((data.len() as u16) | 0x4000).to_be_bytes(), data].concat()
    }
    fn number(id: &[u8], value: u32) -> Vec<u8> {
        element(id, &value.to_be_bytes())
    }
    fn number64(id: &[u8], value: u64) -> Vec<u8> {
        element(id, &value.to_be_bytes())
    }
    let mut header = element(&[0x42, 0x82], doc.as_bytes());
    header.extend(number(&[0x42, 0x86], 1));
    header.extend(number(&[0x42, 0xf7], 1));
    header.extend(number(&[0x42, 0xf2], 4));
    header.extend(number(&[0x42, 0xf3], 8));
    header.extend(number(&[0x42, 0x87], 4));
    header.extend(number(&[0x42, 0x85], 2));
    let mut tracks = vec![];
    let mut blocks = Vec::new();
    let mut block = |track: u8, pts: i16, duration: u64, frame: &[u8]| {
        let mut bytes = vec![0x80 | track];
        bytes.extend(pts.to_be_bytes());
        bytes.push(0);
        bytes.extend(frame);
        let mut group = element(&[0xa1], &bytes);
        group.extend(number64(&[0x9b], duration));
        blocks.push((pts, element(&[0xa0], &group)));
    };
    if audio {
        let mut t = number(&[0xd7], 1);
        t.extend(number(&[0x73, 0xc5], 1));
        t.extend(number(&[0x83], 2));
        t.extend(element(&[0x86], b"A_OPUS"));
        // RFC 7845 OpusHead: mono, pre-skip zero, 48 kHz, mapping family 0.
        t.extend(element(
            &[0x63, 0xa2],
            b"OpusHead\x01\x01\x00\x00\x80\xbb\x00\x00\x00\x00\x00",
        ));
        let mut a = element(&[0xb5], &48000f64.to_be_bytes());
        a.extend(number(&[0x9f], 1));
        t.extend(element(&[0xe1], &a));
        t.extend(number64(&[0x56, 0xaa], timing.codec_delay));
        tracks.extend(element(&[0xae], &t));
        // Fifty valid 20ms Opus silence packets, exactly one second.
        for n in 0..50 {
            block(
                1,
                timing.audio_start.checked_add(n * 20).unwrap(),
                20,
                &[0xf8, 0xff, 0xfe],
            );
        }
    }
    if video {
        // The pinned WebP is a lossy VP8 keyframe. Extract its VP8 chunk only
        // when tests are authorized; preserve encoded frame bytes unchanged.
        let webp = include_bytes!("../../tests/fixtures/capabilities/hermes-feature-connect.webp");
        assert_eq!(&webp[12..16], b"VP8 ");
        let len = u32::from_le_bytes(webp[16..20].try_into().unwrap()) as usize;
        let frame = &webp[20..20 + len];
        let mut t = number(&[0xd7], 2);
        t.extend(number(&[0x73, 0xc5], 2));
        t.extend(number(&[0x83], 1));
        t.extend(element(&[0x86], b"V_VP8"));
        let mut v = number(&[0xb0], 128);
        v.extend(number(&[0xba], 128));
        t.extend(element(&[0xe0], &v));
        t.extend(element(
            &[0x23, 0x31, 0x4f],
            &timing.track_scale.to_be_bytes(),
        ));
        tracks.extend(element(&[0xae], &t));
        block(2, timing.video_start, timing.video_duration, frame);
    }
    blocks.sort_by_key(|(pts, _)| *pts);
    let mut cluster = number64(&[0xe7], timing.cluster_timestamp);
    for (_, block) in blocks {
        cluster.extend(block);
    }
    let mut info = number(&[0x2a, 0xd7, 0xb1], timing.timestamp_scale);
    if let Some(d) = timing.declared_duration {
        info.extend(element(&[0x44, 0x89], &d.to_be_bytes()));
    }
    info.extend(element(&[0x4d, 0x80], b"Pioneer fixture"));
    info.extend(element(&[0x57, 0x41], b"Pioneer fixture"));
    let segment = [
        element(&[0x15, 0x49, 0xa9, 0x66], &info),
        element(&[0x16, 0x54, 0xae, 0x6b], &tracks),
        element(&[0x1f, 0x43, 0xb6, 0x75], &cluster),
    ]
    .concat();
    [
        element(&[0x1a, 0x45, 0xdf, 0xa3], &header),
        element(&[0x18, 0x53, 0x80, 0x67], &segment),
    ]
    .concat()
}

/// Layer III silence: main_data_begin=0, granule/channel lengths=0, no
/// reservoir or gapless tags. MPEG1/48kHz mono, each frame has 1152 samples.
/// First 20 frames are 320kbps/960 bytes, remaining are 32kbps/96 bytes.
/// The expected count comes from the caller, not a bitrate estimator.
pub(crate) fn vbr_mp3(frames: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for n in 0..frames {
        let long = n < 20;
        out.extend([0xff, 0xfb, if long { 0xe4 } else { 0x14 }, 0xc0]);
        out.resize(out.len() + if long { 956 } else { 92 }, 0);
    }
    out
}

fn aac_frames() -> (Vec<u8>, Vec<u8>) {
    let source = include_bytes!("../../tests/fixtures/capabilities/opencode-bip-bop-04.aac");
    fn take(bytes: &[u8]) -> &[u8] {
        assert_eq!(&bytes[..3], &[0xff, 0xf1, 0x50]); // AAC-LC, 44100Hz, unprotected
        assert_eq!(bytes[3] >> 6, 1); // mono
        assert_eq!(bytes[6] & 3, 0); // one raw block
        let len = (usize::from(bytes[3] & 3) << 11)
            | (usize::from(bytes[4]) << 3)
            | usize::from(bytes[5] >> 5);
        &bytes[..len]
    }
    let a = take(source);
    let b = take(&source[a.len()..]);
    assert_ne!(a.len(), b.len());
    if a.len() > b.len() {
        (a.to_vec(), b.to_vec())
    } else {
        (b.to_vec(), a.to_vec())
    }
}
/// Reuses unchanged valid AAC-LC encoded payloads from the pinned reference;
/// header rate/channel/frame configuration stays unchanged. No codec runs.
pub(crate) fn vbr_adts(frames: usize) -> Vec<u8> {
    let (long, short) = aac_frames();
    (0..frames)
        .flat_map(|n| if n < 20 { long.clone() } else { short.clone() })
        .collect()
}

#[derive(Clone, Copy)]
pub(crate) struct Mp4Edit {
    pub duration: u32,
    pub start: i32,
    pub rate: i32,
}
fn atom(id: &[u8], data: &[u8]) -> Vec<u8> {
    [((data.len() + 8) as u32).to_be_bytes().as_slice(), id, data].concat()
}
fn elst(edits: &[Mp4Edit]) -> Vec<u8> {
    let mut data = vec![0; 4];
    data.extend((edits.len() as u32).to_be_bytes());
    for e in edits {
        data.extend(e.duration.to_be_bytes());
        data.extend(e.start.to_be_bytes());
        data.extend(e.rate.to_be_bytes());
    }
    atom(b"edts", &atom(b"elst", &data))
}
/// Rebuild metadata only; move new moov to EOF and replace original with an
/// equally sized free box. All original mdat offsets/sample payloads stay valid.
fn edit_movie(source: &[u8], edits: Option<&[Mp4Edit]>) -> Vec<u8> {
    use super::mp4_timing::atoms;
    let mut budget = 100_000;
    let mut out = Vec::new();
    let mut new_movie = Vec::new();
    for top in atoms(source, &mut budget).unwrap() {
        if top.id != b"moov" {
            out.extend(atom(top.id, top.data));
            continue;
        }
        out.extend(atom(b"free", top.data));
        for field in atoms(top.data, &mut budget).unwrap() {
            if field.id == b"trak" {
                let mut track = Vec::new();
                for t in atoms(field.data, &mut budget).unwrap() {
                    track.extend(atom(if t.id == b"edts" { b"free" } else { t.id }, t.data));
                }
                if let Some(edits) = edits {
                    track.extend(elst(edits));
                }
                new_movie.extend(atom(b"trak", &track));
            } else if field.id == b"mvhd" && edits.is_some() {
                let mut data = field.data.to_vec();
                assert_eq!(data[0], 0);
                data[12..16].copy_from_slice(&1000u32.to_be_bytes());
                data[16..20].copy_from_slice(
                    &edits
                        .unwrap()
                        .iter()
                        .map(|e| e.duration)
                        .sum::<u32>()
                        .to_be_bytes(),
                );
                new_movie.extend(atom(field.id, &data));
            } else {
                new_movie.extend(atom(field.id, field.data));
            }
        }
    }
    assert_eq!(
        out.len(),
        source.len(),
        "fixture requires unchanged original box header extents"
    );
    out.extend(atom(b"moov", &new_movie));
    out
}
pub(crate) fn encoded_video_mp4(edits: Option<&[Mp4Edit]>) -> Vec<u8> {
    edit_movie(
        include_bytes!("../../tests/fixtures/capabilities/opencode-tabs.mp4"),
        edits,
    )
}
/// Encoded audio-only ISO BMFF: 441 valid AAC-LC mono samples from the pinned
/// asset. 441*1024 /44100 =10.24 seconds independently; MDHD and STTS agree.
pub(crate) fn encoded_audio_mp4(edits: Option<&[Mp4Edit]>) -> Vec<u8> {
    let (packet, _) = aac_frames();
    let payload = &packet[7..];
    let count = 441u32;
    let ftyp = atom(b"ftyp", b"isom\0\0\0\0isommp42");
    let mdat = atom(b"mdat", &payload.repeat(count as usize));
    let mut mvhd = vec![0; 12];
    mvhd.extend(1000u32.to_be_bytes());
    mvhd.extend(10240u32.to_be_bytes());
    mvhd.extend(0x10000u32.to_be_bytes());
    mvhd.extend(0x100u16.to_be_bytes());
    mvhd.extend([0; 10]);
    let matrix = [0x10000u32, 0, 0, 0, 0x10000, 0, 0, 0, 0x40000000]
        .into_iter()
        .flat_map(u32::to_be_bytes)
        .collect::<Vec<_>>();
    mvhd.extend(&matrix);
    mvhd.extend([0; 24]);
    mvhd.extend(2u32.to_be_bytes());
    let mut tkhd = vec![0, 0, 0, 3];
    tkhd.extend([0; 8]);
    tkhd.extend(1u32.to_be_bytes());
    tkhd.extend([0; 4]);
    tkhd.extend(10240u32.to_be_bytes());
    tkhd.extend([0; 12]);
    tkhd.extend(0x100u16.to_be_bytes());
    tkhd.extend([0; 2]);
    tkhd.extend(&matrix);
    tkhd.extend([0; 8]);
    let mut mdhd = vec![0; 12];
    mdhd.extend(44100u32.to_be_bytes());
    mdhd.extend((count * 1024).to_be_bytes());
    mdhd.extend([0; 4]);
    let mut hdlr = vec![0; 8];
    hdlr.extend(b"soun");
    hdlr.extend([0; 12]);
    hdlr.extend(b"Audio\0");
    let mut entry = vec![0; 6];
    entry.extend(1u16.to_be_bytes());
    entry.extend([0; 8]);
    entry.extend(1u16.to_be_bytes());
    entry.extend(16u16.to_be_bytes());
    entry.extend([0; 4]);
    entry.extend((44100u32 << 16).to_be_bytes());
    let esds = [
        0, 0, 0, 0, 3, 25, 0, 1, 0, 4, 17, 0x40, 0x15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 2, 0x12,
        0x08, 6, 1, 2,
    ];
    entry.extend(atom(b"esds", &esds));
    let full = |words: &[u32]| {
        words
            .iter()
            .flat_map(|v| v.to_be_bytes())
            .collect::<Vec<_>>()
    };
    let stsd = atom(b"stsd", &[full(&[0, 1]), atom(b"mp4a", &entry)].concat());
    let stbl = atom(
        b"stbl",
        &[
            stsd,
            atom(b"stts", &full(&[0, 1, count, 1024])),
            atom(b"stsc", &full(&[0, 1, 1, count, 1])),
            atom(b"stsz", &full(&[0, payload.len() as u32, count])),
            atom(b"stco", &full(&[0, 1, (ftyp.len() + 8) as u32])),
        ]
        .concat(),
    );
    let dref = atom(
        b"dref",
        &[full(&[0, 1]), atom(b"url ", &[0, 0, 0, 1])].concat(),
    );
    let minf = atom(
        b"minf",
        &[atom(b"smhd", &[0; 8]), atom(b"dinf", &dref), stbl].concat(),
    );
    let mdia = atom(
        b"mdia",
        &[atom(b"mdhd", &mdhd), atom(b"hdlr", &hdlr), minf].concat(),
    );
    let track = atom(b"trak", &[atom(b"tkhd", &tkhd), mdia].concat());
    let movie = atom(b"moov", &[atom(b"mvhd", &mvhd), track].concat());
    let raw = [ftyp, mdat, movie].concat();
    if edits.is_some() {
        edit_movie(&raw, edits)
    } else {
        raw
    }
}

/// Full zero-extension Xing frame followed by raw valid silence frames.
/// No implicit guessed priming/padding; the caller supplies header counter.
pub(crate) fn xing_mp3(frames: usize, declared: u32) -> Vec<u8> {
    let mut header = vbr_mp3(1);
    header[21..25].copy_from_slice(b"Xing");
    header[25..29].copy_from_slice(&1u32.to_be_bytes());
    header[29..33].copy_from_slice(&declared.to_be_bytes());
    header.extend(vbr_mp3(frames));
    header
}

/// Valid ordinary silence with a metadata-looking ancillary payload. The
/// MPEG1 mono ignored private bit (after 9-bit main_data_begin) is nonzero;
/// SCFSI and both granules' part2_3_length/coded side data stay zero. Nothing
/// references ancillary payload: main_data_begin and coded lengths remain zero.
pub(crate) fn ordinary_mp3_collision(frames: usize, at: usize, magic: &[u8; 4]) -> Vec<u8> {
    assert!(at < frames && matches!(magic, b"Info" | b"Xing" | b"VBRI"));
    let mut bytes = vbr_mp3(frames);
    let offset = if at < 20 {
        at * 960
    } else {
        20 * 960 + (at - 20) * 96
    };
    bytes[offset + 5] = 0x40;
    let ancillary = if magic == b"VBRI" { 36 } else { 21 };
    bytes[offset + ancillary..offset + ancillary + 4].copy_from_slice(magic);
    // Independent fixture shape, distinguishing ignored private bits from
    // coded fields. These checks run only in future authorized tests.
    assert_eq!(bytes[offset + 4], 0); // all 9 main_data_begin bits, including bit7 of byte5
    assert_eq!(bytes[offset + 5] & 0x83, 0); // main_data_begin/SCFSI bits
    assert!(bytes[offset + 6..offset + 21].iter().all(|b| *b == 0));
    bytes
}

/// Metadata candidates, deliberately outside the admitted contract; unlike
/// collision fixtures, these have complete extent and zero side/prefix.
pub(crate) fn rejected_mp3_tag_candidates() -> Vec<Vec<u8>> {
    let mut vbri = vbr_mp3(1);
    vbri[36..40].copy_from_slice(b"VBRI");
    vbri[40..42].copy_from_slice(&1u16.to_be_bytes()); // VBRI version
    vbri[46..50].copy_from_slice(&960u32.to_be_bytes());
    vbri[50..54].copy_from_slice(&1u32.to_be_bytes());
    let mut protected = xing_mp3(100, 100);
    protected[1] &= !1;
    protected[4..6].copy_from_slice(&[0x12, 0x34]); // candidate prefix excludes CRC bytes
    let embedded = [vbr_mp3(1), xing_mp3(100, 100)].concat();
    let mut fields = xing_mp3(100, 100);
    fields[25..29].copy_from_slice(&15u32.to_be_bytes()); // declared TOC/fields fit
    fields[149] = 1; // unsupported nonzero encoder extension, not an ordinary collision
    let mut short_fields = vbr_mp3(1);
    short_fields.truncate(96);
    short_fields[2] = 0x14; // valid short MPEG1/48kHz32kbps frame, exactly96bytes
    short_fields[21..25].copy_from_slice(b"Info");
    short_fields[25..29].copy_from_slice(&15u32.to_be_bytes());
    short_fields[29..33].copy_from_slice(&1u32.to_be_bytes()); // TOC no longer fits -> proven candidate, incomplete fields
    vec![
        vbri,
        protected,
        embedded,
        fields,
        short_fields,
        xing_mp3(100, 99),
    ]
}

/// Known LAME extension with independent declared sample trim. CRC construction
/// happens only in future authorized tests, never while preparing these edits.
pub(crate) fn trimmed_xing_mp3(magic: &[u8; 4]) -> Vec<u8> {
    use symphonia::core::io::Monitor;
    assert!(matches!(magic, b"Xing" | b"Info"));
    let mut bytes = xing_mp3(100, 100);
    bytes[21..25].copy_from_slice(magic);
    const EXTENSION: usize = 33; // flags=1, frame counter only
    bytes[EXTENSION..EXTENSION + 9].copy_from_slice(b"LAME3.100");
    // 100 delay +200 padding samples; this is explicit, not guessed priming.
    let trim = (100u32 << 12) | 200;
    bytes[EXTENSION + 21..EXTENSION + 24].copy_from_slice(&trim.to_be_bytes()[1..]);
    let mut crc = symphonia::core::checksum::Crc16AnsiLe::new(0);
    crc.process_buf_bytes(&bytes[..EXTENSION + 34]);
    assert_ne!(crc.crc(), 0); // supported contract requires nonzero matching CRC
    bytes[EXTENSION + 34..EXTENSION + 36].copy_from_slice(&crc.crc().to_be_bytes());
    bytes
}
