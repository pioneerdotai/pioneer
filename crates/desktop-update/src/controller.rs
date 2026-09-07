use crate::{check, snapshot::DesktopUpdateSnapshot, updater};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DesktopUpdateOperation {
    generation: u64,
    step: UpdateStep,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpdateStep {
    Check,
    Download,
    Stage,
    Apply,
}

/// The fields are private: only the controller can issue a validated operation.
pub struct DesktopUpdatePlan {
    operation: DesktopUpdateOperation,
    cancelled: Arc<AtomicBool>,
    work: Work,
}
enum Work {
    Check {
        runtime_home: PathBuf,
        config: updater::state::DesktopUpdateConfig,
    },
    Download(check::DesktopUpdateDownload),
    Stage {
        runtime_home: PathBuf,
        input: updater::plan::DesktopUpdatePlanInput,
    },
    Apply(updater::plan::PreparedDesktopUpdateApply),
}
impl DesktopUpdatePlan {
    pub fn operation(&self) -> DesktopUpdateOperation {
        self.operation
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}
pub struct DesktopUpdateCompletion {
    operation: DesktopUpdateOperation,
    result: Result<Outcome, String>,
}
enum Outcome {
    State(DesktopUpdateSnapshot),
    Download(check::DesktopUpdateDownload),
    Staged(updater::plan::PreparedDesktopUpdateApply),
    Applied,
}
impl DesktopUpdateCompletion {
    pub fn no_update(operation: DesktopUpdateOperation) -> Self {
        Self {
            operation,
            result: Ok(Outcome::State(DesktopUpdateSnapshot::Idle)),
        }
    }
    pub fn failed(operation: DesktopUpdateOperation, error: String) -> Self {
        Self {
            operation,
            result: Err(error),
        }
    }
}
pub trait DesktopUpdatePort: Send + Sync + 'static {
    fn execute(&self, plan: DesktopUpdatePlan) -> DesktopUpdateCompletion;
}
/// Construction is inert. Native effects exist only in the explicitly invoked port.
pub struct NativeDesktopUpdatePort;
impl DesktopUpdatePort for NativeDesktopUpdatePort {
    fn execute(&self, plan: DesktopUpdatePlan) -> DesktopUpdateCompletion {
        let operation = plan.operation;
        if plan.is_cancelled() {
            return DesktopUpdateCompletion::failed(operation, "cancelled".into());
        }
        let result = match plan.work {
            Work::Check {
                runtime_home,
                config,
            } => match check::run_desktop_update_check(config, runtime_home) {
                check::DesktopUpdateCheckResult::Done(state) => Ok(Outcome::State(state)),
                check::DesktopUpdateCheckResult::Download(download) => {
                    Ok(Outcome::Download(download))
                }
            },
            Work::Download(download) => {
                Ok(Outcome::State(check::run_desktop_update_download(download)))
            }
            Work::Stage {
                runtime_home,
                input,
            } => (|| {
                let prepared = updater::plan::prepare_desktop_update_apply(&runtime_home, input)
                    .map_err(|error| format!("{error:#}"))?;
                pioneer_app_updater::plan::read_and_validate_plan(&prepared.plan_path)
                    .map_err(|error| error.to_string())?;
                if plan.cancelled.load(Ordering::Acquire) {
                    return Err("cancelled".into());
                }
                Ok(Outcome::Staged(prepared))
            })(),
            Work::Apply(prepared) => (|| {
                // Revalidate at the operation boundary before invoking the unchanged helper.
                pioneer_app_updater::plan::read_and_validate_plan(&prepared.plan_path)
                    .map_err(|error| error.to_string())?;
                if plan.cancelled.load(Ordering::Acquire) {
                    return Err("cancelled".into());
                }
                std::process::Command::new(&prepared.helper_path)
                    .arg("apply")
                    .arg("--plan")
                    .arg(&prepared.plan_path)
                    .spawn()
                    .map_err(|error| error.to_string())?;
                Ok(Outcome::Applied)
            })(),
        };
        DesktopUpdateCompletion { operation, result }
    }
}

pub struct DesktopUpdateStore {
    snapshot: Arc<DesktopUpdateSnapshot>,
    revision: u64,
    generation: u64,
    active: Option<(DesktopUpdateOperation, Arc<AtomicBool>)>,
    error: Option<String>,
}
pub struct DesktopUpdateController {
    store: DesktopUpdateStore,
    runtime_home: PathBuf,
}
pub struct DesktopUpdateTransition {
    pub next: Option<DesktopUpdatePlan>,
    pub relaunch: bool,
    pub changed: bool,
}
impl DesktopUpdateController {
    pub fn new(runtime_home: PathBuf) -> Self {
        Self {
            runtime_home,
            store: DesktopUpdateStore {
                snapshot: Arc::new(DesktopUpdateSnapshot::initial()),
                revision: 0,
                generation: 0,
                active: None,
                error: None,
            },
        }
    }
    pub fn snapshot(&self) -> Arc<DesktopUpdateSnapshot> {
        self.store.snapshot.clone()
    }
    pub fn revision(&self) -> u64 {
        self.store.revision
    }
    pub fn error(&self) -> Option<&str> {
        self.store.error.as_deref()
    }
    fn state(&mut self, state: DesktopUpdateSnapshot) {
        if *self.store.snapshot != state {
            self.store.snapshot = Arc::new(state);
            self.store.revision += 1;
        }
    }
    fn issue(&mut self, step: UpdateStep, work: Work) -> DesktopUpdatePlan {
        self.store.generation += 1;
        let operation = DesktopUpdateOperation {
            generation: self.store.generation,
            step,
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        self.store.active = Some((operation, cancelled.clone()));
        DesktopUpdatePlan {
            operation,
            cancelled,
            work,
        }
    }
    pub fn check(&mut self) -> Option<DesktopUpdatePlan> {
        if self.store.active.is_some() || self.store.snapshot.is_style_preview() {
            return None;
        }
        let config = updater::desktop_update_config_from_env();
        if config.disabled || !self.runtime_home.is_absolute() {
            return None;
        }
        for value in [&config.release_api_base, &config.release_download_base] {
            let url = reqwest::Url::parse(value).ok()?;
            if !matches!(url.scheme(), "https" | "http") || url.host_str().is_none() {
                return None;
            }
        }
        self.store.error = None;
        self.state(DesktopUpdateSnapshot::Checking);
        Some(self.issue(
            UpdateStep::Check,
            Work::Check {
                runtime_home: self.runtime_home.clone(),
                config,
            },
        ))
    }
    pub fn apply(&mut self) -> Option<DesktopUpdatePlan> {
        if self.store.active.is_some() || self.store.snapshot.is_style_preview() {
            return None;
        }
        let DesktopUpdateSnapshot::Ready {
            version,
            current_version,
            tag,
            asset_path,
            asset_name,
            sha256,
            os,
            arch,
            kind,
            ..
        } = self.store.snapshot.as_ref()
        else {
            return None;
        };
        if !asset_path.is_absolute()
            || sha256.len() != 64
            || !sha256.bytes().all(|b| b.is_ascii_hexdigit())
            || semver::Version::parse(version).ok()?
                <= semver::Version::parse(current_version).ok()?
        {
            return None;
        }
        let input = updater::plan::DesktopUpdatePlanInput {
            target_version: version.clone(),
            current_version: current_version.clone(),
            tag: tag.clone(),
            asset_path: asset_path.clone(),
            asset_name: asset_name.clone(),
            asset_sha256: sha256.clone(),
            os: os.clone(),
            arch: arch.clone(),
            asset_kind: kind.clone(),
        };
        self.store.error = None;
        Some(self.issue(
            UpdateStep::Stage,
            Work::Stage {
                runtime_home: self.runtime_home.clone(),
                input,
            },
        ))
    }
    pub fn complete(&mut self, completion: DesktopUpdateCompletion) -> DesktopUpdateTransition {
        let mut transition = DesktopUpdateTransition {
            next: None,
            relaunch: false,
            changed: false,
        };
        if !self
            .store
            .active
            .as_ref()
            .is_some_and(|(operation, cancelled)| {
                *operation == completion.operation && !cancelled.load(Ordering::Acquire)
            })
        {
            return transition;
        }
        // Identity alone is insufficient: an apply completion cannot be used
        // as a check/download result, even by an incorrectly implemented port.
        let matching_result = match (&completion.result, completion.operation.step) {
            (Err(_), _) => true,
            (Ok(Outcome::Applied), UpdateStep::Apply) => true,
            (Ok(Outcome::Staged(_)), UpdateStep::Stage) => true,
            (Ok(Outcome::Download(_)), UpdateStep::Check) => true,
            (Ok(Outcome::State(_)), UpdateStep::Check | UpdateStep::Download) => true,
            _ => false,
        };
        if !matching_result {
            return transition;
        }
        self.store.active.take();
        let before = self.store.revision;
        match completion.result {
            Ok(Outcome::State(state)) => self.state(state),
            Ok(Outcome::Download(download)) => {
                self.state(DesktopUpdateSnapshot::Downloading {
                    style_preview: false,
                });
                transition.next = Some(self.issue(UpdateStep::Download, Work::Download(download)));
            }
            Ok(Outcome::Staged(prepared)) => {
                transition.next = Some(self.issue(UpdateStep::Apply, Work::Apply(prepared)));
            }
            Ok(Outcome::Applied) => {
                if let DesktopUpdateSnapshot::Ready { version, .. } = self.store.snapshot.as_ref() {
                    self.state(DesktopUpdateSnapshot::Applying {
                        version: version.clone(),
                    });
                    transition.relaunch = true;
                }
            }
            Err(error) => {
                self.store.error = Some(error);
                self.store.revision += 1;
                if !matches!(
                    completion.operation.step,
                    UpdateStep::Stage | UpdateStep::Apply
                ) {
                    self.state(DesktopUpdateSnapshot::FailedSilent {
                        checked_at_unix: 0,
                        error_code: "operation".into(),
                    });
                }
            }
        }
        transition.changed = before != self.store.revision;
        transition
    }
    pub fn cancel(&mut self) {
        if let Some((_, cancelled)) = self.store.active.take() {
            cancelled.store(true, Ordering::Release);
            self.state(DesktopUpdateSnapshot::Idle);
        }
    }
}
impl Drop for DesktopUpdateController {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct FakePort;
    impl DesktopUpdatePort for FakePort {
        fn execute(&self, plan: DesktopUpdatePlan) -> DesktopUpdateCompletion {
            DesktopUpdateCompletion::no_update(plan.operation())
        }
    }
    fn operation(controller: &mut DesktopUpdateController) -> DesktopUpdatePlan {
        controller.issue(
            UpdateStep::Check,
            Work::Check {
                runtime_home: PathBuf::from("/synthetic"),
                config: updater::state::DesktopUpdateConfig::from_lookup(|_| None),
            },
        )
    }
    #[test]
    fn matching_completion_only_and_cancel_fences_old_operation() {
        let mut controller = DesktopUpdateController::new(PathBuf::from("/synthetic"));
        let old = operation(&mut controller);
        let old_id = old.operation();
        controller.cancel();
        assert!(old.is_cancelled());
        let next = operation(&mut controller);
        let next_id = next.operation();
        assert!(
            !controller
                .complete(DesktopUpdateCompletion::failed(old_id, "old".into()))
                .changed
        );
        assert!(controller.error().is_none());
        controller.complete(FakePort.execute(next));
        assert!(
            !controller
                .complete(DesktopUpdateCompletion::failed(next_id, "duplicate".into()))
                .changed
        );
        assert!(controller.error().is_none());
    }
    #[test]
    fn wrong_operation_result_keeps_the_matching_operation_pending() {
        let mut controller = DesktopUpdateController::new(PathBuf::from("/synthetic"));
        let plan = operation(&mut controller);
        let identity = plan.operation();
        assert!(
            !controller
                .complete(DesktopUpdateCompletion {
                    operation: identity,
                    result: Ok(Outcome::Applied)
                })
                .changed
        );
        assert_eq!(
            controller.store.active.as_ref().map(|(id, _)| *id),
            Some(identity)
        );
        controller.complete(FakePort.execute(plan));
        assert!(controller.store.active.is_none());
    }
    #[test]
    fn drop_cancels_port_plan_without_native_effect() {
        let mut controller = DesktopUpdateController::new(PathBuf::from("/synthetic"));
        let plan = operation(&mut controller);
        drop(controller);
        assert!(plan.is_cancelled());
    }
}
