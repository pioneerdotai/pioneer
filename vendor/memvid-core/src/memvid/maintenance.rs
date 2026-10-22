use std::path::Path;

use crate::Result;
use crate::io::time_index::read_track as time_index_read;
use crate::memvid::lifecycle::Memvid;
use crate::types::{
    DoctorOptions, DoctorPlan, DoctorReport, VerificationCheck, VerificationReport,
    VerificationStatus,
};

impl Memvid {
    pub fn verify<P: AsRef<Path>>(path: P, deep: bool) -> Result<VerificationReport> {
        let path_buf = path.as_ref().to_path_buf();
        let mut mem = Self::open_read_only(&path_buf)?;

        let mut checks = Vec::new();
        let mut overall = VerificationStatus::Passed;
        let mut push_check = |name: &str, status: VerificationStatus, details: Option<String>| {
            if status == VerificationStatus::Failed {
                overall = VerificationStatus::Failed;
            }
            checks.push(VerificationCheck {
                name: name.to_string(),
                status,
                details,
            });
        };

        // Time index integrity
        if let Some(manifest) = mem.toc.time_index.clone() {
            match time_index_read(&mut mem.file, manifest.bytes_offset, manifest.bytes_length) {
                Ok(entries) => {
                    if manifest.entry_count == entries.len() as u64 {
                        push_check("TimeIndexEntryCount", VerificationStatus::Passed, None);
                    } else {
                        push_check(
                            "TimeIndexEntryCount",
                            VerificationStatus::Failed,
                            Some(format!(
                                "expected {} entries, got {}",
                                manifest.entry_count,
                                entries.len()
                            )),
                        );
                    }

                    if deep {
                        let sorted = entries
                            .windows(2)
                            .all(|pair| pair[0].timestamp <= pair[1].timestamp);
                        if sorted {
                            push_check("TimeIndexSortOrder", VerificationStatus::Passed, None);
                        } else {
                            push_check(
                                "TimeIndexSortOrder",
                                VerificationStatus::Failed,
                                Some("timestamps are not sorted".into()),
                            );
                        }
                    }
                }
                Err(err) => {
                    push_check(
                        "TimeIndexRead",
                        VerificationStatus::Failed,
                        Some(err.to_string()),
                    );
                }
            }
        } else {
            push_check(
                "TimeIndexRead",
                VerificationStatus::Skipped,
                Some("time index disabled".into()),
            );
        }

        // Lexical index decode
        if mem.lex_enabled {
            match mem.ensure_lex_index() {
                Ok(()) => push_check("LexIndexDecode", VerificationStatus::Passed, None),
                Err(err) => push_check(
                    "LexIndexDecode",
                    VerificationStatus::Failed,
                    Some(err.to_string()),
                ),
            }
        } else {
            push_check(
                "LexIndexDecode",
                VerificationStatus::Skipped,
                Some("lex index disabled".into()),
            );
        }

        // Vector index decode
        if mem.vec_enabled {
            match mem.ensure_vec_index() {
                Ok(()) => push_check("VecIndexDecode", VerificationStatus::Passed, None),
                Err(err) => push_check(
                    "VecIndexDecode",
                    VerificationStatus::Failed,
                    Some(err.to_string()),
                ),
            }
        } else {
            push_check(
                "VecIndexDecode",
                VerificationStatus::Skipped,
                Some("vector index disabled".into()),
            );
        }

        // WAL pending entries check
        match mem.wal.pending_records() {
            Ok(records) => {
                if records.is_empty() {
                    push_check("WalPendingRecords", VerificationStatus::Passed, None);
                } else {
                    push_check(
                        "WalPendingRecords",
                        VerificationStatus::Failed,
                        Some(format!("{} pending records", records.len())),
                    );
                }
            }
            Err(err) => push_check(
                "WalPendingRecords",
                VerificationStatus::Failed,
                Some(err.to_string()),
            ),
        }

        // Frame count consistency
        match mem.stats() {
            Ok(stats) => {
                if stats.frame_count == mem.toc.frames.len() as u64 {
                    push_check("FrameCountConsistency", VerificationStatus::Passed, None);
                } else {
                    push_check(
                        "FrameCountConsistency",
                        VerificationStatus::Failed,
                        Some(format!(
                            "stats reports {}, toc has {}",
                            stats.frame_count,
                            mem.toc.frames.len()
                        )),
                    );
                }
            }
            Err(err) => push_check(
                "FrameCountConsistency",
                VerificationStatus::Failed,
                Some(err.to_string()),
            ),
        }

        Ok(VerificationReport {
            file_path: path_buf,
            checks,
            overall_status: overall,
        })
    }

    pub fn doctor<P: AsRef<Path>>(path: P, options: DoctorOptions) -> Result<DoctorReport> {
        crate::memvid::doctor::doctor_run(path.as_ref(), options)
    }

    pub fn doctor_plan<P: AsRef<Path>>(path: P, options: DoctorOptions) -> Result<DoctorPlan> {
        crate::memvid::doctor::doctor_plan(path.as_ref(), options)
    }

    pub fn doctor_apply<P: AsRef<Path>>(path: P, plan: DoctorPlan) -> Result<DoctorReport> {
        crate::memvid::doctor::doctor_apply(path.as_ref(), plan)
    }
}
