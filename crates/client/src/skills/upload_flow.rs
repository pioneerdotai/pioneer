//! One upload state machine; shells consume progress and submit typed effect results.
use super::archive::SkillUploadArchive;
use pioneer_protocol::*;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillUploadState {
    Preparing,
    Starting,
    Uploading,
    Finishing,
    Applying,
    Succeeded,
    Failed,
    Cancelled,
}
impl SkillUploadState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SkillUploadPublication {
    pub operation_id: u64,
    pub generation: u64,
    pub revision: u64,
    pub state: SkillUploadState,
    pub sent_bytes: u64,
    pub total_bytes: u64,
    pub target: super::operations::SkillUploadTarget,
    pub pack: bool,
}
pub(crate) enum UploadEffect {
    Start(SkillsUploadStartParams),
    Chunk {
        upload_id: String,
        offset: u64,
        bytes: Vec<u8>,
    },
    Finish(SkillsUploadFinishParams),
    Apply {
        upload_id: String,
    },
}
pub(crate) enum UploadCompletion {
    Start(SkillsUploadStartResponse),
    Chunk(SkillsUploadChunkAckNotification),
    Finish(SkillsUploadFinishResponse),
    Applied,
}
pub(crate) struct SkillUploadFlow {
    pub publication: SkillUploadPublication,
    workspace: String,
    archive: Option<SkillUploadArchive>,
    upload_id: Option<String>,
    chunk_size: usize,
    expected_end: u64,
    in_flight: bool,
    effect_generation: u64,
    cleanup_upload: Option<String>,
}
impl SkillUploadFlow {
    pub fn new(operation: u64, workspace: String) -> Self {
        Self {
            publication: SkillUploadPublication {
                operation_id: operation,
                generation: operation,
                revision: 0,
                state: SkillUploadState::Preparing,
                sent_bytes: 0,
                total_bytes: 0,
                target: super::operations::SkillUploadTarget::Install,
                pack: false,
            },
            workspace,
            archive: None,
            upload_id: None,
            chunk_size: 0,
            expected_end: 0,
            in_flight: false,
            effect_generation: 0,
            cleanup_upload: None,
        }
    }
    pub fn prepare(&mut self, archive: SkillUploadArchive) -> bool {
        if self.publication.state != SkillUploadState::Preparing {
            return false;
        }
        if super::upload::skills_upload_start_params(&self.workspace, &archive).is_err() {
            self.terminate(SkillUploadState::Failed);
            return true;
        }
        self.publication.total_bytes = archive.bytes.len() as u64;
        self.archive = Some(archive);
        self.publication.state = SkillUploadState::Starting;
        true
    }
    pub fn effect(&mut self) -> Option<UploadEffect> {
        if self.in_flight || self.publication.state.is_terminal() {
            return None;
        }
        let archive = self.archive.as_ref()?;
        let effect = match self.publication.state {
            SkillUploadState::Starting => UploadEffect::Start(
                super::upload::skills_upload_start_params(&self.workspace, archive).ok()?,
            ),
            SkillUploadState::Uploading => {
                let chunk = super::upload::next_skill_upload_chunk(
                    &archive.bytes,
                    self.publication.sent_bytes as usize,
                    self.chunk_size,
                )
                .ok()??;
                self.expected_end = chunk.next_offset_bytes;
                UploadEffect::Chunk {
                    upload_id: self.upload_id.clone()?,
                    offset: chunk.offset_bytes,
                    bytes: chunk.bytes,
                }
            }
            SkillUploadState::Finishing => {
                UploadEffect::Finish(super::upload::skills_upload_finish_params(
                    &self.workspace,
                    self.upload_id.clone()?,
                ))
            }
            SkillUploadState::Applying => UploadEffect::Apply {
                upload_id: self.upload_id.clone()?,
            },
            _ => return None,
        };
        self.in_flight = true;
        self.effect_generation = self
            .effect_generation
            .checked_add(1)
            .expect("upload effect generation exhausted");
        Some(effect)
    }
    pub fn complete(&mut self, generation: u64, completion: UploadCompletion) -> bool {
        if generation != self.effect_generation
            || !self.in_flight
            || self.publication.state.is_terminal()
        {
            return false;
        }
        let valid = match (&completion, self.publication.state) {
            (UploadCompletion::Start(start), SkillUploadState::Starting) => {
                !start.upload_id.is_empty()
                    && start.max_chunk_size_bytes > 0
                    && start.recommended_chunk_size_bytes > 0
                    && self.publication.total_bytes <= start.max_compressed_size_bytes
                    && self.archive.as_ref().is_some_and(|a| {
                        a.uncompressed_size_bytes <= start.max_uncompressed_size_bytes
                    })
            }
            (UploadCompletion::Chunk(ack), SkillUploadState::Uploading) => {
                Some(&ack.upload_id) == self.upload_id.as_ref()
                    && ack.offset == self.publication.sent_bytes
                    && ack.len == self.expected_end - self.publication.sent_bytes
                    && ack.next_offset == self.expected_end
                    && ack.received_bytes == self.expected_end
            }
            (UploadCompletion::Finish(finish), SkillUploadState::Finishing) => {
                Some(&finish.upload_id) == self.upload_id.as_ref()
                    && finish.status == "finalized"
                    && finish.compressed_size_bytes == self.publication.total_bytes
                    && self
                        .archive
                        .as_ref()
                        .is_some_and(|a| a.sha256 == finish.sha256)
            }
            (UploadCompletion::Applied, SkillUploadState::Applying) => true,
            _ => return false,
        };
        if !valid {
            if let UploadCompletion::Start(start) = &completion
                && !start.upload_id.is_empty()
            {
                self.upload_id = Some(start.upload_id.clone());
            }
            self.terminate(SkillUploadState::Failed);
            return true;
        }
        self.in_flight = false;
        match completion {
            UploadCompletion::Start(start) => {
                self.chunk_size = start
                    .recommended_chunk_size_bytes
                    .min(start.max_chunk_size_bytes)
                    .min(usize::MAX as u64) as usize;
                self.upload_id = Some(start.upload_id);
                self.publication.state = if self.publication.total_bytes == 0 {
                    SkillUploadState::Finishing
                } else {
                    SkillUploadState::Uploading
                };
            }
            UploadCompletion::Chunk(_) => {
                self.publication.sent_bytes = self.expected_end;
                if self.publication.sent_bytes == self.publication.total_bytes {
                    self.publication.state = SkillUploadState::Finishing;
                }
            }
            UploadCompletion::Finish(_) => {
                self.publication.state = SkillUploadState::Applying;
            }
            UploadCompletion::Applied => {
                self.terminate(SkillUploadState::Succeeded);
            }
        }
        true
    }
    pub fn terminate(&mut self, state: SkillUploadState) -> Option<String> {
        if self.publication.state.is_terminal() {
            return None;
        }
        self.publication.state = state;
        self.in_flight = false;
        self.archive.take();
        let remote = self.upload_id.take();
        if state != SkillUploadState::Succeeded {
            self.cleanup_upload = remote.clone();
        }
        remote
    }
    pub fn accepts_effect(&self, generation: u64) -> bool {
        generation == self.effect_generation
            && self.in_flight
            && !self.publication.state.is_terminal()
    }
    pub fn effect_generation(&self) -> u64 {
        self.effect_generation
    }
    pub fn take_cleanup(&mut self) -> Option<String> {
        self.cleanup_upload.take()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn prepared() -> SkillUploadFlow {
        let mut f = SkillUploadFlow::new(7, "workspace".into());
        f.prepare(SkillUploadArchive {
            file_name: "skill.tar.gz".into(),
            bytes: vec![1, 2, 3],
            sha256: "digest".into(),
            uncompressed_size_bytes: 3,
        });
        f
    }
    fn start() -> UploadCompletion {
        UploadCompletion::Start(SkillsUploadStartResponse {
            upload_id: "upload".into(),
            recommended_chunk_size_bytes: 2,
            max_chunk_size_bytes: 2,
            max_compressed_size_bytes: 10,
            max_uncompressed_size_bytes: 10,
            expires_at_unix: 1,
        })
    }
    #[test]
    fn ordered_upload_validates_identity_offsets_finish_and_terminal_once() {
        let mut f = prepared();
        assert!(matches!(f.effect(), Some(UploadEffect::Start(_))));
        assert!(f.effect().is_none());
        assert!(!f.complete(8, start()));
        assert!(f.complete(1, start()));
        assert!(
            matches!(f.effect(),Some(UploadEffect::Chunk{offset:0,bytes,..}) if bytes==vec![1,2])
        );
        assert!(f.complete(
            2,
            UploadCompletion::Chunk(SkillsUploadChunkAckNotification {
                upload_id: "upload".into(),
                offset: 0,
                len: 2,
                received_bytes: 2,
                next_offset: 2
            })
        ));
        assert!(!f.complete(1, start()));
        assert!(
            matches!(f.effect(),Some(UploadEffect::Chunk{offset:2,bytes,..}) if bytes==vec![3])
        );
        assert!(!f.complete(
            2,
            UploadCompletion::Chunk(SkillsUploadChunkAckNotification {
                upload_id: "upload".into(),
                offset: 0,
                len: 2,
                received_bytes: 2,
                next_offset: 2
            })
        ));
        f.complete(
            3,
            UploadCompletion::Chunk(SkillsUploadChunkAckNotification {
                upload_id: "upload".into(),
                offset: 2,
                len: 1,
                received_bytes: 3,
                next_offset: 3,
            }),
        );
        assert!(matches!(f.effect(), Some(UploadEffect::Finish(_))));
        f.complete(
            4,
            UploadCompletion::Finish(SkillsUploadFinishResponse {
                upload_id: "upload".into(),
                status: "finalized".into(),
                sha256: "digest".into(),
                compressed_size_bytes: 3,
            }),
        );
        assert!(matches!(f.effect(), Some(UploadEffect::Apply { .. })));
        assert!(f.complete(5, UploadCompletion::Applied));
        assert_eq!(f.publication.state, SkillUploadState::Succeeded);
        assert!(f.archive.is_none());
        assert!(!f.complete(5, UploadCompletion::Applied));
    }
    #[test]
    fn cancellation_and_invalid_ack_release_bytes_and_ignore_late_results() {
        let mut f = prepared();
        f.effect();
        f.complete(1, start());
        f.effect();
        f.complete(
            2,
            UploadCompletion::Chunk(SkillsUploadChunkAckNotification {
                upload_id: "wrong".into(),
                offset: 0,
                len: 2,
                received_bytes: 2,
                next_offset: 2,
            }),
        );
        assert_eq!(f.publication.state, SkillUploadState::Failed);
        assert!(f.archive.is_none());
        let mut f = prepared();
        f.effect();
        f.terminate(SkillUploadState::Cancelled);
        assert!(!f.complete(1, start()));
        assert!(f.effect().is_none());
        assert!(f.archive.is_none());
    }
}
