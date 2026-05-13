use super::*;
use anyhow::{Context, Result};
use pioneer_artifacts::{
    ArtifactCapturePolicy, ArtifactListFilter, ArtifactLocalPathPolicy, ArtifactSource,
    IngestArtifactSourceRequest,
};
use pioneer_protocol::{ArtifactCreatedByKind, ArtifactSummary};
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub(super) struct TurnFileCaptureSession {
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    policy: ArtifactCapturePolicy,
    output_roots: Vec<PathBuf>,
    baseline: HashMap<PathBuf, FileBaseline>,
    explicit_events: Vec<FileCaptureEvent>,
    skip_paths: HashSet<PathBuf>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct FileCaptureOutcome {
    pub artifacts: Vec<ArtifactSummary>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FileCaptureEvent {
    pub path: PathBuf,
    pub modified: bool,
    pub source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileBaseline {
    len: u64,
    modified: Option<SystemTime>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileCandidate {
    path: PathBuf,
    len: u64,
    fallback_scan: bool,
    capture_source: String,
}

impl TurnFileCaptureSession {
    pub(super) async fn start(
        workspace_id: String,
        thread_id: String,
        turn_id: String,
        policy: ArtifactCapturePolicy,
        default_root: PathBuf,
    ) -> Result<Self> {
        let output_roots = canonical_output_roots(policy.output_roots_or_default(default_root))?;
        let baseline = scan_baseline(&policy, output_roots.as_slice())?;
        Ok(Self {
            workspace_id,
            thread_id,
            turn_id,
            policy,
            output_roots,
            baseline,
            explicit_events: Vec::new(),
            skip_paths: HashSet::new(),
        })
    }

    pub(super) fn add_skip_paths(&mut self, paths: impl IntoIterator<Item = PathBuf>) {
        for path in paths {
            let canonical = fs::canonicalize(path.as_path()).unwrap_or(path);
            self.skip_paths.insert(canonical);
        }
    }

    pub(super) async fn finish(
        &self,
        artifact_service: &ArtifactService,
        skip_source_paths: &HashSet<PathBuf>,
    ) -> Result<FileCaptureOutcome> {
        let mut skip_paths = skip_source_paths.clone();
        skip_paths.extend(self.skip_paths.iter().cloned());
        let candidates = self.collect_candidates(&skip_paths)?;
        let mut artifacts = Vec::new();
        let mut diagnostics = Vec::new();
        let mut total_bytes = 0_u64;

        for candidate in candidates {
            if artifacts.len() >= self.policy.max_files_per_turn {
                diagnostics.push(format!(
                    "file capture max_files_per_turn={} reached",
                    self.policy.max_files_per_turn
                ));
                break;
            }
            if candidate.len > self.policy.max_bytes_per_file {
                diagnostics.push(format!(
                    "skipped {}: size {} exceeds max_bytes_per_file={}",
                    candidate.path.display(),
                    candidate.len,
                    self.policy.max_bytes_per_file
                ));
                continue;
            }
            if total_bytes.saturating_add(candidate.len) > self.policy.max_total_bytes_per_turn {
                diagnostics.push(format!(
                    "skipped {}: max_total_bytes_per_turn={} reached",
                    candidate.path.display(),
                    self.policy.max_total_bytes_per_turn
                ));
                break;
            }

            match self.ingest_candidate(artifact_service, &candidate).await {
                Ok(summary) => {
                    total_bytes = total_bytes.saturating_add(candidate.len);
                    artifacts.push(summary);
                }
                Err(error) => diagnostics.push(format!(
                    "failed to capture {}: {error:#}",
                    candidate.path.display()
                )),
            }
        }

        Ok(FileCaptureOutcome {
            artifacts,
            diagnostics,
        })
    }

    fn collect_candidates(
        &self,
        skip_source_paths: &HashSet<PathBuf>,
    ) -> Result<Vec<FileCandidate>> {
        let mut candidates = Vec::new();
        let mut seen = HashSet::new();

        for event in &self.explicit_events {
            if event.modified && !self.policy.capture_modified_workspace_files {
                continue;
            }
            let Some(candidate) = candidate_for_path(
                &self.policy,
                self.output_roots.as_slice(),
                event.path.as_path(),
                false,
                event.source.clone(),
                skip_source_paths,
            )?
            else {
                continue;
            };
            if seen.insert(candidate.path.clone()) {
                candidates.push(candidate);
            }
        }

        for root in &self.output_roots {
            for path in list_regular_files(root, &self.policy)? {
                if skip_source_paths.contains(path.as_path()) {
                    continue;
                }
                let metadata = fs::metadata(path.as_path())
                    .with_context(|| format!("failed to stat `{}`", path.display()))?;
                let baseline = self.baseline.get(path.as_path());
                let is_new = baseline.is_none();
                let is_modified = baseline.is_some_and(|baseline| {
                    baseline.len != metadata.len() || baseline.modified != metadata.modified().ok()
                });
                if (is_new && self.policy.capture_new_workspace_files)
                    || (is_modified && self.policy.capture_modified_workspace_files)
                {
                    let candidate = FileCandidate {
                        path: path.clone(),
                        len: metadata.len(),
                        fallback_scan: true,
                        capture_source: if is_new {
                            "fallback_scan_new".to_owned()
                        } else {
                            "fallback_scan_modified".to_owned()
                        },
                    };
                    if seen.insert(candidate.path.clone()) {
                        candidates.push(candidate);
                    }
                }
            }
        }

        candidates.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(candidates)
    }

    async fn ingest_candidate(
        &self,
        artifact_service: &ArtifactService,
        candidate: &FileCandidate,
    ) -> Result<ArtifactSummary> {
        let mut metadata = BTreeMap::new();
        metadata.insert("source_kind".to_owned(), json!("file_capture_session"));
        metadata.insert("capture_source".to_owned(), json!(candidate.capture_source));
        metadata.insert(
            "source_path".to_owned(),
            json!(candidate.path.display().to_string()),
        );
        metadata.insert("fallback_scan".to_owned(), json!(candidate.fallback_scan));

        artifact_service
            .ingest_source(IngestArtifactSourceRequest {
                workspace_id: self.workspace_id.clone(),
                primary_thread_id: Some(self.thread_id.clone()),
                source: ArtifactSource::LocalPath(candidate.path.clone()),
                display_name: candidate_display_name(candidate.path.as_path()),
                kind: None,
                mime_type: None,
                created_by_kind: ArtifactCreatedByKind::System,
                created_by_actor_id: Some("file_capture_session".to_owned()),
                binding: Some(ArtifactBindingTarget {
                    thread_id: Some(self.thread_id.clone()),
                    turn_id: Some(self.turn_id.clone()),
                    message_id: None,
                    turn_item_id: None,
                    tool_call_id: None,
                    task_id: None,
                    task_run_id: None,
                    binding_kind: ArtifactBindingKind::SystemCapture,
                    direction: ArtifactBindingDirection::Output,
                    role: Some(ArtifactRole::System),
                    item_index: None,
                }),
                metadata,
                local_path_policy: Some(ArtifactLocalPathPolicy {
                    allowed_roots: self.output_roots.clone(),
                    max_file_bytes: self.policy.max_bytes_per_file,
                    follow_symlinks: false,
                }),
            })
            .await
            .with_context(|| format!("failed to ingest `{}`", candidate.path.display()))
    }
}

impl MessageProcessor {
    pub(super) async fn start_file_capture_session(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) {
        let default_root = match std::env::current_dir() {
            Ok(path) => path,
            Err(error) => {
                warn!(workspace_id, thread_id, turn_id, error = %error, "failed to resolve file capture root");
                return;
            }
        };
        match TurnFileCaptureSession::start(
            workspace_id.to_owned(),
            thread_id.to_owned(),
            turn_id.to_owned(),
            (*self.artifact_capture_policy).clone(),
            default_root,
        )
        .await
        {
            Ok(session) => {
                self.file_capture_sessions
                    .lock()
                    .await
                    .insert(turn_id.to_owned(), session);
            }
            Err(error) => {
                warn!(
                    workspace_id,
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to start file capture session"
                );
            }
        }
    }

    pub(super) async fn finish_file_capture_session(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) {
        let Some(session) = self.file_capture_sessions.lock().await.remove(turn_id) else {
            return;
        };
        let skip_paths = self
            .captured_source_paths_for_turn(workspace_id, thread_id, turn_id)
            .await;
        match session.finish(&self.artifact_service, &skip_paths).await {
            Ok(outcome) => {
                let artifact_ids = outcome
                    .artifacts
                    .iter()
                    .map(|summary| summary.artifact.artifact_id.clone())
                    .collect::<Vec<_>>();
                for summary in outcome.artifacts {
                    self.send_notification_to_thread_subscribers(
                        thread_id,
                        events::ARTIFACT_CREATED,
                        &ArtifactCreatedNotification {
                            workspace_id: workspace_id.to_owned(),
                            artifact: summary,
                        },
                    )
                    .await;
                }
                if !artifact_ids.is_empty() {
                    self.send_notification_to_thread_subscribers(
                        thread_id,
                        events::THREAD_ARTIFACTS_CHANGED,
                        &ThreadArtifactsChangedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            artifact_ids,
                            reason: "file_capture_session".to_owned(),
                            generated_at: now_timestamp_secs(),
                        },
                    )
                    .await;
                }
                for diagnostic in outcome.diagnostics {
                    warn!(
                        workspace_id,
                        thread_id, turn_id, diagnostic, "file capture diagnostic"
                    );
                }
            }
            Err(error) => {
                warn!(
                    workspace_id,
                    thread_id,
                    turn_id,
                    error = %format!("{error:#}"),
                    "failed to finish file capture session"
                );
            }
        }
    }

    pub(super) async fn skip_resolved_artifact_inputs_for_file_capture(
        &self,
        turn_id: &str,
        resolved_artifacts: &[ResolvedArtifactInput],
    ) {
        let paths = resolved_artifacts
            .iter()
            .filter_map(|resolved| match &resolved.attachment.source {
                pioneer_provider::AttachmentDataSource::Path { path } => Some(PathBuf::from(path)),
                _ => None,
            })
            .collect::<Vec<_>>();

        if paths.is_empty() {
            return;
        }

        if let Some(session) = self.file_capture_sessions.lock().await.get_mut(turn_id) {
            session.add_skip_paths(paths);
        }
    }

    async fn captured_source_paths_for_turn(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> HashSet<PathBuf> {
        let page = match self
            .artifact_service
            .list_thread_artifacts(
                workspace_id,
                thread_id,
                ArtifactListFilter {
                    turn_id: Some(turn_id.to_owned()),
                    limit: Some(512),
                    ..ArtifactListFilter::default()
                },
            )
            .await
        {
            Ok(page) => page,
            Err(error) => {
                warn!(
                    workspace_id,
                    thread_id,
                    turn_id,
                    error = %error,
                    "failed to list existing turn artifacts before file capture"
                );
                return HashSet::new();
            }
        };
        page.items
            .into_iter()
            .filter_map(|summary| {
                summary
                    .metadata
                    .get("source_path")
                    .and_then(JsonValue::as_str)
                    .map(PathBuf::from)
            })
            .collect()
    }
}

fn scan_baseline(
    policy: &ArtifactCapturePolicy,
    roots: &[PathBuf],
) -> Result<HashMap<PathBuf, FileBaseline>> {
    let mut baseline = HashMap::new();
    for root in roots {
        for path in list_regular_files(root, policy)? {
            let metadata = fs::metadata(path.as_path())
                .with_context(|| format!("failed to stat `{}`", path.display()))?;
            baseline.insert(
                path,
                FileBaseline {
                    len: metadata.len(),
                    modified: metadata.modified().ok(),
                },
            );
        }
    }
    Ok(baseline)
}

fn candidate_for_path(
    policy: &ArtifactCapturePolicy,
    roots: &[PathBuf],
    path: &Path,
    fallback_scan: bool,
    capture_source: String,
    skip_source_paths: &HashSet<PathBuf>,
) -> Result<Option<FileCandidate>> {
    let canonical = match fs::canonicalize(path) {
        Ok(path) => path,
        Err(_) => return Ok(None),
    };
    if skip_source_paths.contains(canonical.as_path())
        || !roots.iter().any(|root| canonical.starts_with(root))
        || policy.ignores_path(canonical.as_path())
    {
        return Ok(None);
    }
    let metadata = fs::symlink_metadata(canonical.as_path())
        .with_context(|| format!("failed to stat `{}`", canonical.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Ok(None);
    }
    Ok(Some(FileCandidate {
        path: canonical,
        len: metadata.len(),
        fallback_scan,
        capture_source,
    }))
}

fn canonical_output_roots(roots: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    let mut output = Vec::new();
    for root in roots {
        let canonical = fs::canonicalize(root.as_path())
            .with_context(|| format!("failed to canonicalize output root `{}`", root.display()))?;
        let metadata = fs::metadata(canonical.as_path())
            .with_context(|| format!("failed to stat output root `{}`", canonical.display()))?;
        if metadata.is_dir() {
            output.push(canonical);
        }
    }
    Ok(output)
}

fn list_regular_files(root: &Path, policy: &ArtifactCapturePolicy) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut queue = VecDeque::from([root.to_path_buf()]);
    while let Some(dir) = queue.pop_front() {
        if policy.ignores_path(dir.as_path()) {
            continue;
        }
        let Ok(entries) = fs::read_dir(dir.as_path()) else {
            continue;
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if policy.ignores_path(path.as_path()) {
                continue;
            }
            let metadata = fs::symlink_metadata(path.as_path())?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                queue.push_back(path);
            } else if metadata.is_file() {
                files.push(fs::canonicalize(path.as_path())?);
            }
        }
    }
    Ok(files)
}

fn candidate_display_name(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_artifacts::LocalArtifactBlobStore;
    use pioneer_crud::CrudStore;
    use sea_orm::Database;
    use std::sync::Arc;

    #[tokio::test]
    async fn file_capture_fallback_scan_captures_new_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        fs::write(temp.path().join("before.txt"), b"before").expect("write baseline");
        let service = artifact_service(temp.path().join("runtime")).await;
        let session = TurnFileCaptureSession::start(
            "ws_file_capture".to_owned(),
            "thr_file_capture".to_owned(),
            "turn_file_capture".to_owned(),
            ArtifactCapturePolicy {
                output_roots: vec![temp.path().to_path_buf()],
                ..ArtifactCapturePolicy::default()
            },
            temp.path().to_path_buf(),
        )
        .await
        .expect("start session");
        fs::write(temp.path().join("created.txt"), b"created").expect("write created");

        let outcome = session
            .finish(&service, &HashSet::new())
            .await
            .expect("finish session");

        assert_eq!(outcome.artifacts.len(), 1);
        assert_eq!(outcome.artifacts[0].artifact.display_name, "created.txt");
        assert_eq!(
            outcome.artifacts[0].bindings[0].binding_kind,
            ArtifactBindingKind::SystemCapture
        );
    }

    #[tokio::test]
    async fn file_capture_ignores_modified_existing_by_default_and_ignored_dirs() {
        let temp = tempfile::tempdir().expect("tempdir");
        let existing = temp.path().join("existing.txt");
        fs::write(existing.as_path(), b"before").expect("write baseline");
        fs::create_dir_all(temp.path().join("target")).expect("mkdir target");
        let service = artifact_service(temp.path().join("runtime")).await;
        let session = TurnFileCaptureSession::start(
            "ws_file_capture".to_owned(),
            "thr_file_capture".to_owned(),
            "turn_file_capture".to_owned(),
            ArtifactCapturePolicy {
                output_roots: vec![temp.path().to_path_buf()],
                ..ArtifactCapturePolicy::default()
            },
            temp.path().to_path_buf(),
        )
        .await
        .expect("start session");
        fs::write(existing.as_path(), b"after").expect("modify existing");
        fs::write(temp.path().join("target").join("ignored.txt"), b"ignored")
            .expect("write ignored");

        let outcome = session
            .finish(&service, &HashSet::new())
            .await
            .expect("finish session");

        assert!(outcome.artifacts.is_empty());
    }

    #[tokio::test]
    async fn file_capture_skips_resolved_artifact_input_paths() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service = artifact_service(temp.path().join("runtime")).await;
        let mut session = TurnFileCaptureSession::start(
            "ws_file_capture".to_owned(),
            "thr_file_capture".to_owned(),
            "turn_file_capture".to_owned(),
            ArtifactCapturePolicy {
                output_roots: vec![temp.path().to_path_buf()],
                ..ArtifactCapturePolicy::default()
            },
            temp.path().to_path_buf(),
        )
        .await
        .expect("start session");
        let materialized_input = temp.path().join("materialized-input.webp");
        fs::write(materialized_input.as_path(), b"provider input").expect("write input");
        session.add_skip_paths(vec![materialized_input]);

        let outcome = session
            .finish(&service, &HashSet::new())
            .await
            .expect("finish session");

        assert!(outcome.artifacts.is_empty());
    }

    #[tokio::test]
    async fn file_capture_rejects_symlink_and_honors_limits() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service = artifact_service(temp.path().join("runtime")).await;
        let session = TurnFileCaptureSession::start(
            "ws_file_capture".to_owned(),
            "thr_file_capture".to_owned(),
            "turn_file_capture".to_owned(),
            ArtifactCapturePolicy {
                output_roots: vec![temp.path().to_path_buf()],
                max_files_per_turn: 1,
                max_bytes_per_file: 4,
                ..ArtifactCapturePolicy::default()
            },
            temp.path().to_path_buf(),
        )
        .await
        .expect("start session");
        fs::write(temp.path().join("large.txt"), b"12345").expect("write large");
        fs::write(temp.path().join("small.txt"), b"ok").expect("write small");
        #[cfg(unix)]
        std::os::unix::fs::symlink(temp.path().join("small.txt"), temp.path().join("link.txt"))
            .expect("symlink");

        let outcome = session
            .finish(&service, &HashSet::new())
            .await
            .expect("finish session");

        assert_eq!(outcome.artifacts.len(), 1);
        assert_eq!(outcome.artifacts[0].artifact.display_name, "small.txt");
        assert!(!outcome.diagnostics.is_empty());
    }

    async fn artifact_service(runtime_home: PathBuf) -> ArtifactService {
        let db = Database::connect("sqlite::memory:").await.expect("sqlite");
        Migrator::up(&db, None).await.expect("migrate");
        ArtifactService::new(
            Arc::new(CrudStore::new(db)),
            Arc::new(LocalArtifactBlobStore::new(runtime_home)),
        )
    }
}
