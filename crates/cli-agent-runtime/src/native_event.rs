use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::File;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeEventBudget {
    pub profile_version: u32,
    /// In-memory frame threshold. Readers switch to a temporary file after
    /// this many bytes.
    pub max_frame_bytes: usize,
    /// Maximum size of one frame that may be spooled outside the in-memory
    /// ingress budget for bounded, streaming protocol recovery.
    pub max_recovery_frame_bytes: usize,
}

impl Default for NativeEventBudget {
    fn default() -> Self {
        Self {
            profile_version: 2,
            max_frame_bytes: 1024 * 1024,
            max_recovery_frame_bytes: default_max_recovery_frame_bytes(),
        }
    }
}

impl NativeEventBudget {
    pub const fn is_valid(self) -> bool {
        self.profile_version > 0
            && self.max_frame_bytes > 0
            && self.max_recovery_frame_bytes >= self.max_frame_bytes
    }

    /// Intersects an immutable admitted ceiling with the currently installed
    /// role ceiling. A policy update may narrow an existing execution, but it
    /// can never widen the process generation that was originally admitted.
    pub const fn intersect(self, current: Self) -> Option<Self> {
        if !self.is_valid()
            || !current.is_valid()
            || self.profile_version != current.profile_version
        {
            return None;
        }
        Some(Self {
            profile_version: self.profile_version,
            max_frame_bytes: min_usize(self.max_frame_bytes, current.max_frame_bytes),
            max_recovery_frame_bytes: min_usize(
                self.max_recovery_frame_bytes,
                current.max_recovery_frame_bytes,
            ),
        })
    }
}

const fn default_max_recovery_frame_bytes() -> usize {
    64 * 1024 * 1024
}

const fn min_usize(left: usize, right: usize) -> usize {
    if left < right { left } else { right }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeIngressErrorKind {
    FrameTooLarge,
    Io,
}

#[derive(Debug)]
pub struct NativeIngressError {
    pub kind: NativeIngressErrorKind,
    message: String,
}

impl NativeIngressError {
    fn new(kind: NativeIngressErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for NativeIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message.as_str())
    }
}

impl std::error::Error for NativeIngressError {}

#[derive(Debug, Clone, Copy)]
pub struct BoundedNativeEventCodec {
    budget: NativeEventBudget,
}

#[derive(Debug)]
pub enum SpooledNativeFrame {
    Complete(Vec<u8>),
    Oversized { file: File, total_bytes: usize },
}

impl BoundedNativeEventCodec {
    pub const fn new(budget: NativeEventBudget) -> Self {
        Self { budget }
    }

    pub const fn budget(self) -> NativeEventBudget {
        self.budget
    }

    /// Reads exactly one JSONL frame while retaining normal frames in memory
    /// and spooling oversized frames to an unnamed file.
    ///
    /// The secondary spool budget is finite. A frame that crosses it fails
    /// the owning runtime session instead of consuming unbounded disk or time.
    /// The returned file owns its storage and is deleted automatically when
    /// dropped.
    pub async fn read_frame_or_spool<R>(
        &self,
        reader: &mut R,
    ) -> Result<Option<SpooledNativeFrame>, NativeIngressError>
    where
        R: AsyncBufRead + Unpin,
    {
        let mut prefix = Vec::with_capacity(self.budget.max_frame_bytes.min(8 * 1024));
        let mut total_bytes = 0usize;
        let mut spool: Option<tokio::fs::File> = None;

        loop {
            let available = reader.fill_buf().await.map_err(|error| {
                NativeIngressError::new(NativeIngressErrorKind::Io, error.to_string())
            })?;
            if available.is_empty() {
                return if total_bytes == 0 {
                    Ok(None)
                } else if let Some(mut file) = spool {
                    file.flush().await.map_err(|error| {
                        NativeIngressError::new(NativeIngressErrorKind::Io, error.to_string())
                    })?;
                    Ok(Some(SpooledNativeFrame::Oversized {
                        file: file.into_std().await,
                        total_bytes,
                    }))
                } else {
                    Ok(Some(SpooledNativeFrame::Complete(prefix)))
                };
            }

            let consumed = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |index| index + 1);
            let chunk = &available[..consumed];
            total_bytes = total_bytes.saturating_add(consumed);

            let retained = if prefix.len() < self.budget.max_frame_bytes {
                let retained = (self.budget.max_frame_bytes - prefix.len()).min(chunk.len());
                prefix.extend_from_slice(&chunk[..retained]);
                retained
            } else {
                0
            };

            if spool.is_none() && total_bytes > self.budget.max_frame_bytes {
                if total_bytes > self.budget.max_recovery_frame_bytes {
                    return Err(NativeIngressError::new(
                        NativeIngressErrorKind::FrameTooLarge,
                        format!(
                            "native JSONL recovery frame exceeds {} bytes",
                            self.budget.max_recovery_frame_bytes
                        ),
                    ));
                }
                let std_file = tempfile::tempfile().map_err(|error| {
                    NativeIngressError::new(NativeIngressErrorKind::Io, error.to_string())
                })?;
                let mut file = tokio::fs::File::from_std(std_file);
                file.write_all(prefix.as_slice()).await.map_err(|error| {
                    NativeIngressError::new(NativeIngressErrorKind::Io, error.to_string())
                })?;
                if retained < chunk.len() {
                    file.write_all(&chunk[retained..]).await.map_err(|error| {
                        NativeIngressError::new(NativeIngressErrorKind::Io, error.to_string())
                    })?;
                }
                spool = Some(file);
            } else if let Some(file) = spool.as_mut() {
                if total_bytes > self.budget.max_recovery_frame_bytes {
                    return Err(NativeIngressError::new(
                        NativeIngressErrorKind::FrameTooLarge,
                        format!(
                            "native JSONL recovery frame exceeds {} bytes",
                            self.budget.max_recovery_frame_bytes
                        ),
                    ));
                }
                file.write_all(chunk).await.map_err(|error| {
                    NativeIngressError::new(NativeIngressErrorKind::Io, error.to_string())
                })?;
            }

            let ended = chunk.last() == Some(&b'\n');
            reader.consume(consumed);
            if ended {
                return if let Some(mut file) = spool {
                    file.flush().await.map_err(|error| {
                        NativeIngressError::new(NativeIngressErrorKind::Io, error.to_string())
                    })?;
                    Ok(Some(SpooledNativeFrame::Oversized {
                        file: file.into_std().await,
                        total_bytes,
                    }))
                } else {
                    Ok(Some(SpooledNativeFrame::Complete(prefix)))
                };
            }
        }
    }
}

impl Default for BoundedNativeEventCodec {
    fn default() -> Self {
        Self::new(NativeEventBudget::default())
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedNativeEventCodec, NativeEventBudget, SpooledNativeFrame};
    use std::io::{Read, Seek, SeekFrom};
    use tokio::io::BufReader;

    #[test]
    fn native_event_budget_intersection_never_widens_admitted_limits() {
        let admitted = NativeEventBudget::default();
        let current = NativeEventBudget {
            max_frame_bytes: 4_096,
            ..NativeEventBudget::default()
        };
        let effective = admitted
            .intersect(current)
            .expect("compatible policies intersect");
        assert_eq!(effective, current);
        assert_eq!(current.intersect(admitted), Some(current));
    }

    #[test]
    fn native_event_budget_rejects_incompatible_profiles() {
        let admitted = NativeEventBudget::default();
        let current = NativeEventBudget {
            profile_version: admitted.profile_version + 1,
            ..admitted
        };
        assert!(admitted.intersect(current).is_none());
    }

    #[tokio::test]
    async fn oversized_frame_is_spooled_with_finite_budget_and_alignment() {
        let budget = NativeEventBudget {
            max_frame_bytes: 16,
            max_recovery_frame_bytes: 128,
            ..NativeEventBudget::default()
        };
        let codec = BoundedNativeEventCodec::new(budget);
        let first = b"{\"id\":1,\"result\":{\"value\":\"oversized\"}}\n";
        let mut input = Vec::from(first.as_slice());
        input.extend_from_slice(b"{}\n");
        let mut reader = BufReader::new(input.as_slice());

        let frame = codec
            .read_frame_or_spool(&mut reader)
            .await
            .expect("oversized frame should spool")
            .expect("spooled frame should exist");
        let SpooledNativeFrame::Oversized {
            mut file,
            total_bytes,
            ..
        } = frame
        else {
            panic!("expected spooled oversized frame");
        };
        assert_eq!(total_bytes, first.len());
        file.seek(SeekFrom::Start(0)).expect("rewind spool");
        let mut recovered = Vec::new();
        file.read_to_end(&mut recovered).expect("read spool");
        assert_eq!(recovered, first);

        let next = codec
            .read_frame_or_spool(&mut reader)
            .await
            .expect("next frame should remain aligned")
            .expect("next frame should exist");
        match next {
            SpooledNativeFrame::Complete(frame) => assert_eq!(frame, b"{}\n"),
            SpooledNativeFrame::Oversized { .. } => panic!("next frame must remain complete"),
        }
    }
}
