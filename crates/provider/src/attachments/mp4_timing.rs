//! Full raw edit evidence, before mp4parse's lossy Track projection is used.
//! Apple QuickTime edit_list_atom/edit_list_table: entry duration uses movie
//! scale, media_time uses media scale, rate is signed 16.16. Only no edits or
//! one exact identity entry are proven here; repeats/trims/rates fail closed.
use anyhow::{Context, Result, ensure};

#[derive(Clone, Copy)]
pub(super) struct Atom<'a> {
    pub id: &'a [u8],
    pub data: &'a [u8],
}
pub(super) fn atoms<'a>(mut bytes: &'a [u8], budget: &mut usize) -> Result<Vec<Atom<'a>>> {
    let mut result = Vec::new();
    while !bytes.is_empty() {
        *budget = budget
            .checked_sub(1)
            .context("MP4 metadata quantum exceeded")?;
        ensure!(bytes.len() >= 8, "truncated MP4 atom header");
        let short = u32::from_be_bytes(bytes[..4].try_into()?);
        let (size, header) = if short == 1 {
            ensure!(bytes.len() >= 16, "truncated extended MP4 atom");
            (
                usize::try_from(u64::from_be_bytes(bytes[8..16].try_into()?))?,
                16,
            )
        } else if short == 0 {
            (bytes.len(), 8)
        } else {
            (usize::try_from(short)?, 8)
        };
        ensure!(
            size >= header && size <= bytes.len(),
            "invalid MP4 atom extent"
        );
        result.push(Atom {
            id: &bytes[4..8],
            data: &bytes[header..size],
        });
        bytes = &bytes[size..];
    }
    Ok(result)
}
pub(super) fn identity_edits(bytes: &[u8]) -> Result<Vec<Option<u64>>> {
    let mut budget = 100_000;
    let top = atoms(bytes, &mut budget)?;
    ensure!(
        !top.iter().any(|a| a.id == b"moof"),
        "fragmented MP4 timeline is not proven"
    );
    let movies = top.iter().filter(|a| a.id == b"moov").collect::<Vec<_>>();
    ensure!(movies.len() == 1, "MP4 movie timing is not unique");
    let movie = atoms(movies[0].data, &mut budget)?;
    ensure!(
        !movie.iter().any(|a| a.id == b"mvex"),
        "fragmented MP4 timeline is not proven"
    );
    ensure!(
        movie.iter().filter(|a| a.id == b"mvhd").count() <= 1,
        "duplicate MP4 movie timebase"
    );
    let tracks = movie.iter().filter(|a| a.id == b"trak").collect::<Vec<_>>();
    let mut identities = Vec::new();
    for raw in tracks {
        let fields = atoms(raw.data, &mut budget)?;
        let media = fields
            .iter()
            .filter(|a| a.id == b"mdia")
            .collect::<Vec<_>>();
        ensure!(media.len() == 1, "MP4 track media timing is not unique");
        let media_fields = atoms(media[0].data, &mut budget)?;
        ensure!(
            media_fields.iter().filter(|a| a.id == b"mdhd").count() == 1,
            "MP4 track media timebase is not unique"
        );
        let edits = fields
            .iter()
            .filter(|a| a.id == b"edts")
            .collect::<Vec<_>>();
        ensure!(edits.len() <= 1, "duplicate MP4 edit container");
        if let Some(edits) = edits.first() {
            let lists = atoms(edits.data, &mut budget)?;
            ensure!(
                lists.len() == 1 && lists[0].id == b"elst",
                "MP4 edit timeline is not proven"
            );
            let list = lists[0].data;
            ensure!(
                list.len() >= 8 && list[1..4] == [0, 0, 0],
                "MP4 edit flags/timing unsupported"
            );
            let count = u32::from_be_bytes(list[4..8].try_into()?);
            ensure!(count == 1, "MP4 nonidentity edit timeline is not proven");
            let (duration, start, rate) = match list[0] {
                0 if list.len() == 20 => (
                    u64::from(u32::from_be_bytes(list[8..12].try_into()?)),
                    i64::from(i32::from_be_bytes(list[12..16].try_into()?)),
                    i32::from_be_bytes(list[16..20].try_into()?),
                ),
                1 if list.len() == 28 => (
                    u64::from_be_bytes(list[8..16].try_into()?),
                    i64::from_be_bytes(list[16..24].try_into()?),
                    i32::from_be_bytes(list[24..28].try_into()?),
                ),
                _ => anyhow::bail!("MP4 edit representation is not proven"),
            };
            ensure!(
                start == 0 && rate == 0x0001_0000,
                "MP4 nonidentity edit timeline is not proven"
            );
            identities.push(Some(duration));
        } else {
            identities.push(None);
        }
    }
    Ok(identities)
}
pub(super) fn validate_edits(
    edits: &[Option<u64>],
    context: &mp4parse::MediaContext,
) -> Result<()> {
    ensure!(
        edits.len() == context.tracks.len(),
        "MP4 raw/projected tracks mismatch"
    );
    for (edit, track) in edits.iter().zip(&context.tracks) {
        if let Some(duration) = edit {
            let movie_scale = context
                .timescale
                .context("MP4 edit movie timebase unavailable")?
                .0;
            let media_scale = track
                .timescale
                .context("MP4 edit media timebase unavailable")?
                .0;
            let media_duration = track
                .duration
                .context("MP4 edit media duration unavailable")?
                .0;
            // u64 × u64 fits u128. No rounding, raw tick addition or first-edit sum.
            ensure!(
                movie_scale > 0
                    && media_scale > 0
                    && u128::from(*duration) * u128::from(media_scale)
                        == u128::from(media_duration) * u128::from(movie_scale),
                "MP4 nonidentity edit timeline is not proven"
            );
        }
    }
    Ok(())
}
