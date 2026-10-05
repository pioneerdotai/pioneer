//! Valid containers built only during future authorized tests; no codecs run.
#[cfg(test)]
pub(crate) fn webm(audio: bool, video: bool, doc: &str) -> Vec<u8> {
    fn element(id: &[u8], data: &[u8]) -> Vec<u8> {
        assert!(data.len() < 16383);
        [id, &((data.len() as u16) | 0x4000).to_be_bytes(), data].concat()
    }
    fn number(id: &[u8], value: u32) -> Vec<u8> {
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
    let mut block = |track: u8, pts: i16, duration: u32, frame: &[u8]| {
        let mut bytes = vec![0x80 | track];
        bytes.extend(pts.to_be_bytes());
        bytes.push(0);
        bytes.extend(frame);
        let mut group = element(&[0xa1], &bytes);
        group.extend(number(&[0x9b], duration));
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
        tracks.extend(element(&[0xae], &t));
        // Fifty valid 20ms Opus silence packets, exactly one second.
        for n in 0..50 {
            block(1, n * 20, 20, &[0xf8, 0xff, 0xfe]);
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
        tracks.extend(element(&[0xae], &t));
        block(2, 0, 1000, frame);
    }
    blocks.sort_by_key(|(pts, _)| *pts);
    let mut cluster = number(&[0xe7], 0);
    for (_, block) in blocks {
        cluster.extend(block);
    }
    let mut info = number(&[0x2a, 0xd7, 0xb1], 1_000_000);
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
