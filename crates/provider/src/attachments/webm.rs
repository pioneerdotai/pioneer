//! Bounded EBML metadata identity only, without decoding or transcoding.
//! https://www.webmproject.org/docs/container/ (DocType and TrackType).
use anyhow::{Result, ensure};

struct Element<'a> {
    id: u64,
    data: &'a [u8],
}
fn elements<'a>(
    mut bytes: &'a [u8],
    budget: &mut usize,
    segment: bool,
) -> Result<Vec<Element<'a>>> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        ensure!(*budget > 0, "WebM metadata element budget exhausted");
        *budget -= 1;
        let (id, id_len, _) = vint(bytes, true)?;
        bytes = &bytes[id_len..];
        let (size, size_len, unknown) = vint(bytes, false)?;
        bytes = &bytes[size_len..];
        if unknown {
            // Only the outer Segment may extend to EOF. An unknown Cluster
            // prevents proving that no later Tracks element changes identity.
            ensure!(
                segment && id == 0x18538067,
                "unbounded WebM metadata element"
            );
        }
        let size = if unknown {
            bytes.len()
        } else {
            usize::try_from(size)?
        };
        ensure!(size <= bytes.len(), "truncated WebM element");
        out.push(Element {
            id,
            data: &bytes[..size],
        });
        bytes = &bytes[size..];
    }
    Ok(out)
}
fn vint(bytes: &[u8], id: bool) -> Result<(u64, usize, bool)> {
    let first = *bytes
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing EBML VINT"))?;
    ensure!(first != 0, "invalid EBML VINT");
    let len = first.leading_zeros() as usize + 1;
    ensure!(
        len <= if id { 4 } else { 8 } && bytes.len() >= len,
        "invalid EBML VINT length"
    );
    let mut value = u64::from(if id { first } else { first & (0xff >> len) });
    for byte in &bytes[1..len] {
        value = (value << 8) | u64::from(*byte);
    }
    Ok((value, len, !id && value == ((1u64 << (7 * len)) - 1)))
}
fn uint(bytes: &[u8]) -> Result<u64> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= 8,
        "invalid WebM unsigned integer"
    );
    Ok(bytes.iter().fold(0, |n, b| (n << 8) | u64::from(*b)))
}
pub(super) fn actual_mime(bytes: &[u8]) -> Result<&'static str> {
    let mut budget = 100_000;
    let top = elements(bytes, &mut budget, true)?;
    ensure!(
        top.first().is_some_and(|e| e.id == 0x1a45dfa3),
        "WebM must begin with its EBML header"
    );
    let headers = top
        .iter()
        .filter(|e| e.id == 0x1a45dfa3)
        .collect::<Vec<_>>();
    let segments = top
        .iter()
        .filter(|e| e.id == 0x18538067)
        .collect::<Vec<_>>();
    ensure!(
        headers.len() == 1 && segments.len() == 1,
        "missing or duplicate WebM header/segment"
    );
    let header = elements(headers[0].data, &mut budget, false)?;
    let docs = header.iter().filter(|e| e.id == 0x4282).collect::<Vec<_>>();
    ensure!(
        docs.len() == 1 && docs[0].data == b"webm",
        "EBML DocType is not WebM"
    );
    let segment = elements(segments[0].data, &mut budget, false)?;
    let track_lists = segment
        .iter()
        .filter(|e| e.id == 0x1654ae6b)
        .collect::<Vec<_>>();
    ensure!(track_lists.len() == 1, "missing or duplicate WebM Tracks");
    let mut numbers = std::collections::BTreeSet::new();
    let mut audio = false;
    let mut video = false;
    for entry in elements(track_lists[0].data, &mut budget, false)?
        .into_iter()
        .filter(|e| e.id == 0xae)
    {
        let fields = elements(entry.data, &mut budget, false)?;
        let types = fields.iter().filter(|e| e.id == 0x83).collect::<Vec<_>>();
        let ids = fields.iter().filter(|e| e.id == 0xd7).collect::<Vec<_>>();
        let codecs = fields.iter().filter(|e| e.id == 0x86).collect::<Vec<_>>();
        ensure!(
            types.len() == 1 && ids.len() == 1 && codecs.len() == 1,
            "unreadable WebM track identity"
        );
        let number = uint(ids[0].data)?;
        ensure!(
            number > 0 && numbers.insert(number),
            "invalid/duplicate WebM track number"
        );
        // Finite WebM codec families: recognizing identity is not endpoint
        // certification for any particular model's codec/profile constraints.
        match uint(types[0].data)? {
            1 => {
                ensure!(
                    matches!(codecs[0].data, b"V_VP8" | b"V_VP9" | b"V_AV1"),
                    "unsupported WebM video codec identity"
                );
                video = true;
            }
            2 => {
                ensure!(
                    matches!(codecs[0].data, b"A_OPUS" | b"A_VORBIS"),
                    "unsupported WebM audio codec identity"
                );
                audio = true;
            }
            _ => anyhow::bail!("unsupported WebM TrackType"),
        }
    }
    ensure!(audio || video, "WebM has no identifiable media tracks");
    Ok(if video { "video/webm" } else { "audio/webm" })
}
