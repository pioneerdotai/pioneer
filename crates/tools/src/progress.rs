//! Runtime facts owned by real handlers, before any model output projection.
//! Observation advancement is evidence about resources, never goal acceptance.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const VERSION: u32 = 1;
const MAX_RESOURCES: usize = 64;
const MAX_ACTIVITIES: usize = 8;
const MAX_STALLS: usize = 16;
const MAX_FRONTIER_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCompletionEvidence {
    #[default]
    Unknown,
    Running,
    Completed,
    TimedOut,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolStreamActivity {
    pub bytes_observed: u64,
    pub digest: [u8; 32],
    pub complete: bool,
}

impl ToolStreamActivity {
    pub fn from_bytes(bytes: &[u8], complete: bool) -> Self {
        Self {
            bytes_observed: bytes.len() as u64,
            digest: Sha256::digest(bytes).into(),
            complete,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolSemanticEvidence {
    #[default]
    Unknown,
    ResourceState {
        resource: [u8; 32],
        revision: [u8; 32],
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolProgressEvidence {
    pub schema_version: u32,
    pub completion: ToolCompletionEvidence,
    pub stdout: ToolStreamActivity,
    pub stderr: ToolStreamActivity,
    pub duration_ms: Option<u64>,
    pub semantic: ToolSemanticEvidence,
}

impl Default for ToolProgressEvidence {
    fn default() -> Self {
        Self {
            schema_version: VERSION,
            completion: Default::default(),
            stdout: Default::default(),
            stderr: Default::default(),
            duration_ms: None,
            semantic: Default::default(),
        }
    }
}

impl ToolProgressEvidence {
    pub fn resource_state(resource: [u8; 32], revision: [u8; 32]) -> Self {
        Self {
            completion: ToolCompletionEvidence::Completed,
            semantic: ToolSemanticEvidence::ResourceState { resource, revision },
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolProgressDisposition {
    Advanced,
    Unchanged,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceBaseline {
    resource: [u8; 32],
    revision: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivityBaseline {
    strategy: [u8; 32],
    stdout: ToolStreamActivity,
    stderr: ToolStreamActivity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StallCertificate {
    strategy: [u8; 32],
    resource: [u8; 32],
    revision: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolProgressFrontier {
    pub schema_version: u32,
    pub advanced_observations: u64,
    pub unchanged_observations: u64,
    pub unknown_observations: u64,
    pub truncated: bool,
    resources: Vec<ResourceBaseline>,
    activities: Vec<ActivityBaseline>,
    stalls: Vec<StallCertificate>,
}

impl Default for ToolProgressFrontier {
    fn default() -> Self {
        Self {
            schema_version: VERSION,
            advanced_observations: 0,
            unchanged_observations: 0,
            unknown_observations: 0,
            truncated: false,
            resources: Vec::new(),
            activities: Vec::new(),
            stalls: Vec::new(),
        }
    }
}

impl ToolProgressFrontier {
    pub fn valid(&self) -> bool {
        self.schema_version == VERSION
            && self.resources.len() <= MAX_RESOURCES
            && self.activities.len() <= MAX_ACTIVITIES
            && self.stalls.len() <= MAX_STALLS
            && self.advanced_observations >= self.resources.len() as u64
            && self.unchanged_observations >= self.stalls.len() as u64
            && self.resources.iter().enumerate().all(|(i, resource)| {
                !self.resources[..i]
                    .iter()
                    .any(|prior| prior.resource == resource.resource)
            })
            && self.activities.iter().enumerate().all(|(i, activity)| {
                !self.activities[..i]
                    .iter()
                    .any(|prior| prior.strategy == activity.strategy)
            })
            && self.stalls.iter().enumerate().all(|(i, stall)| {
                !self.stalls[..i]
                    .iter()
                    .any(|prior| prior.strategy == stall.strategy)
                    && self
                        .resources
                        .iter()
                        .any(|resource| resource.resource == stall.resource)
            })
            && serde_json::to_vec(self).is_ok_and(|wire| wire.len() <= MAX_FRONTIER_BYTES)
    }

    pub(crate) fn stall_is_current(&self, exact_strategy: &str) -> bool {
        let strategy: [u8; 32] = Sha256::digest(exact_strategy.as_bytes()).into();
        self.stalls.iter().any(|stall| {
            stall.strategy == strategy
                && self.resources.iter().any(|resource| {
                    resource.resource == stall.resource && resource.revision == stall.revision
                })
        })
    }

    pub(crate) fn confirm_stall(
        &mut self,
        exact_strategy: &str,
        evidence: &ToolProgressEvidence,
    ) -> bool {
        let ToolSemanticEvidence::ResourceState { resource, revision } = evidence.semantic else {
            return false;
        };
        let strategy: [u8; 32] = Sha256::digest(exact_strategy.as_bytes()).into();
        if let Some(stall) = self
            .stalls
            .iter_mut()
            .find(|stall| stall.strategy == strategy)
        {
            *stall = StallCertificate {
                strategy,
                resource,
                revision,
            };
        } else if self.stalls.len() < MAX_STALLS {
            self.stalls.push(StallCertificate {
                strategy,
                resource,
                revision,
            });
        } else {
            self.truncated = true;
            return false;
        }
        true
    }

    pub(crate) fn observe(
        &mut self,
        exact_strategy: &str,
        evidence: &ToolProgressEvidence,
    ) -> (ToolProgressDisposition, bool) {
        if evidence.schema_version != VERSION {
            self.unknown_observations = self.unknown_observations.saturating_add(1);
            return (ToolProgressDisposition::Unknown, false);
        }
        let strategy = Sha256::digest(exact_strategy.as_bytes()).into();
        let has_activity = evidence.stdout.bytes_observed > 0 || evidence.stderr.bytes_observed > 0;
        let new_activity = if has_activity {
            if let Some(prior) = self
                .activities
                .iter_mut()
                .find(|prior| prior.strategy == strategy)
            {
                let changed = prior.stdout != evidence.stdout || prior.stderr != evidence.stderr;
                prior.stdout = evidence.stdout;
                prior.stderr = evidence.stderr;
                changed
            } else if self.activities.len() < MAX_ACTIVITIES {
                self.activities.push(ActivityBaseline {
                    strategy,
                    stdout: evidence.stdout,
                    stderr: evidence.stderr,
                });
                true
            } else {
                self.truncated = true;
                false
            }
        } else {
            false
        };
        let disposition = match evidence.semantic {
            ToolSemanticEvidence::Unknown => ToolProgressDisposition::Unknown,
            ToolSemanticEvidence::ResourceState { resource, revision } => {
                if let Some(prior) = self
                    .resources
                    .iter_mut()
                    .find(|prior| prior.resource == resource)
                {
                    if prior.revision == revision {
                        ToolProgressDisposition::Unchanged
                    } else {
                        prior.revision = revision;
                        ToolProgressDisposition::Advanced
                    }
                } else if self.resources.len() < MAX_RESOURCES {
                    self.resources.push(ResourceBaseline { resource, revision });
                    ToolProgressDisposition::Advanced
                } else {
                    self.truncated = true;
                    ToolProgressDisposition::Unknown
                }
            }
        };
        let counter = match disposition {
            ToolProgressDisposition::Advanced => &mut self.advanced_observations,
            ToolProgressDisposition::Unchanged => &mut self.unchanged_observations,
            ToolProgressDisposition::Unknown => &mut self.unknown_observations,
        };
        if let Some(next) = counter.checked_add(1) {
            *counter = next;
            (disposition, new_activity)
        } else {
            self.truncated = true;
            (ToolProgressDisposition::Unknown, new_activity)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_revisions_survive_restore_and_activity_is_separate() {
        let mut frontier = ToolProgressFrontier::default();
        let state = ToolProgressEvidence::resource_state([1; 32], [2; 32]);
        assert_eq!(
            frontier.observe("read", &state).0,
            ToolProgressDisposition::Advanced
        );
        let wire = serde_json::to_vec(&frontier).unwrap();
        let mut restored: ToolProgressFrontier = serde_json::from_slice(&wire).unwrap();
        assert!(restored.valid());
        assert_eq!(
            restored.observe("read", &state).0,
            ToolProgressDisposition::Unchanged
        );
        let stderr = ToolProgressEvidence {
            stderr: ToolStreamActivity::from_bytes(b"actual stderr", true),
            completion: ToolCompletionEvidence::Completed,
            ..Default::default()
        };
        assert_eq!(
            restored.observe("command", &stderr),
            (ToolProgressDisposition::Unknown, true)
        );
        assert_eq!(
            restored.observe("command", &stderr),
            (ToolProgressDisposition::Unknown, false)
        );
        assert_eq!(restored.advanced_observations, 1);
        assert_eq!(restored.unchanged_observations, 1);
        assert_eq!(restored.unknown_observations, 2);
        assert_eq!(
            restored
                .observe(
                    "read",
                    &ToolProgressEvidence::resource_state([1; 32], [3; 32])
                )
                .0,
            ToolProgressDisposition::Advanced
        );
        let unsupported = ToolProgressEvidence {
            schema_version: 99,
            ..state
        };
        assert_eq!(
            restored.observe("read", &unsupported).0,
            ToolProgressDisposition::Unknown
        );
    }

    #[test]
    fn full_frontier_never_evicts_and_fabricates_novelty() {
        let mut frontier = ToolProgressFrontier::default();
        for index in 0..MAX_RESOURCES {
            let mut resource = [255; 32];
            resource[0] = index as u8;
            frontier.observe(
                "read",
                &ToolProgressEvidence::resource_state(resource, [255; 32]),
            );
        }
        let extra = ToolProgressEvidence::resource_state([100; 32], [255; 32]);
        assert_eq!(
            frontier.observe("read", &extra).0,
            ToolProgressDisposition::Unknown
        );
        assert_eq!(
            frontier.observe("read", &extra).0,
            ToolProgressDisposition::Unknown
        );
        assert_eq!(frontier.resources.len(), MAX_RESOURCES);
        let resource = frontier.resources[0].resource;
        assert_eq!(
            frontier
                .observe(
                    "read",
                    &ToolProgressEvidence::resource_state(resource, [255; 32])
                )
                .0,
            ToolProgressDisposition::Unchanged
        );
        let evidence = ToolProgressEvidence {
            stdout: ToolStreamActivity {
                bytes_observed: u64::MAX,
                digest: [255; 32],
                complete: true,
            },
            stderr: ToolStreamActivity {
                bytes_observed: u64::MAX,
                digest: [255; 32],
                complete: true,
            },
            duration_ms: Some(u64::MAX),
            semantic: ToolSemanticEvidence::ResourceState {
                resource,
                revision: [255; 32],
            },
            ..Default::default()
        };
        for index in 0..MAX_STALLS {
            let strategy = format!("strategy-{index}");
            frontier.observe(&strategy, &evidence);
            assert!(frontier.confirm_stall(&strategy, &evidence));
        }
        assert_eq!(frontier.activities.len(), MAX_ACTIVITIES);
        assert_eq!(frontier.stalls.len(), MAX_STALLS);
        assert!(serde_json::to_vec(&evidence).unwrap().len() <= 1024);
        assert!(frontier.valid());
        assert!(serde_json::to_vec(&frontier).unwrap().len() <= MAX_FRONTIER_BYTES);
        let encoded = serde_json::to_vec(&frontier).unwrap();
        let mut cold: ToolProgressFrontier = serde_json::from_slice(&encoded).unwrap();
        assert!(cold.stall_is_current("strategy-0"));
        cold.observe(
            "mutation",
            &ToolProgressEvidence::resource_state(resource, [254; 32]),
        );
        assert!(!cold.stall_is_current("strategy-0"));
        let mut invalid = frontier.clone();
        invalid.resources.push(invalid.resources[0].clone());
        assert!(!invalid.valid());
        let mut invalid = frontier.clone();
        invalid.advanced_observations = 0;
        assert!(!invalid.valid());
        let mut invalid = frontier.clone();
        invalid.stalls[0].resource = [77; 32];
        assert!(!invalid.valid());
        let mut invalid = frontier;
        invalid.schema_version = 99;
        assert!(!invalid.valid());
    }

    #[test]
    fn observation_counter_overflow_is_unknown() {
        let mut frontier = ToolProgressFrontier::default();
        frontier.advanced_observations = u64::MAX;
        assert_eq!(
            frontier
                .observe(
                    "read",
                    &ToolProgressEvidence::resource_state([1; 32], [2; 32])
                )
                .0,
            ToolProgressDisposition::Unknown
        );
        assert!(frontier.truncated);
    }
}
