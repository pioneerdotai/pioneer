use crate::TaskRuntimeResult;
use anyhow::{anyhow, bail};
use pioneer_protocol::{
    Task, TaskAgentSpec, TaskAgentSpecInput, TaskCreateParams, TaskTrigger, TaskUpdateParams,
};
use serde::Serialize;
use std::io::{self, Write};

const MAX_TASK_REVIEWERS: usize = 128;
const MAX_DURABLE_TASK_CONTRACT_BYTES: usize = 512 * 1024;
const MAX_TASK_TITLE_BYTES: usize = 512;

pub(crate) struct TaskAdmissionNormalizer;

impl TaskAdmissionNormalizer {
    pub(crate) const fn new() -> Self {
        Self
    }

    pub(crate) fn validate_create(&self, params: &TaskCreateParams) -> TaskRuntimeResult<()> {
        self.validate_encoded(params, "task create request")?;
        self.validate_title(params.title.as_str())?;
        if let Some(spec) = params.agent_spec.as_ref() {
            self.validate_agent_spec_input(spec)?;
        }
        if let Some(metadata) = params.metadata.as_ref() {
            if let Some(work) = metadata.composer_work.as_ref() {
                pioneer_protocol::validate_turn_execution_envelope(&work.launch)
                    .map_err(|error| anyhow!("composer launch exceeds Turn budget: {error}"))?;
            }
        }
        Ok(())
    }

    pub(crate) fn validate_update(&self, params: &TaskUpdateParams) -> TaskRuntimeResult<()> {
        self.validate_encoded(params, "task update request")?;
        if let Some(title) = params.title.as_deref() {
            self.validate_title(title)?;
        }
        Ok(())
    }

    pub(crate) fn validate_durable(
        &self,
        task: &Task,
        trigger: Option<&TaskTrigger>,
        agent_spec: Option<&TaskAgentSpec>,
    ) -> TaskRuntimeResult<()> {
        self.validate_encoded(&(task, trigger, agent_spec), "durable task contract")?;
        self.validate_title(task.title.as_str())?;
        Ok(())
    }

    fn validate_agent_spec_input(&self, spec: &TaskAgentSpecInput) -> TaskRuntimeResult<()> {
        if let Some(review) = spec.review_policy.as_ref()
            && review.reviewers.len() > MAX_TASK_REVIEWERS
        {
            bail!(
                "review policy has {} reviewers; maximum is {MAX_TASK_REVIEWERS}",
                review.reviewers.len()
            );
        }
        Ok(())
    }

    fn validate_title(&self, title: &str) -> TaskRuntimeResult<()> {
        if title.len() > MAX_TASK_TITLE_BYTES {
            bail!("task title exceeds {MAX_TASK_TITLE_BYTES} bytes");
        }
        Ok(())
    }

    fn validate_encoded<T: Serialize>(&self, value: &T, field: &str) -> TaskRuntimeResult<()> {
        struct Counter {
            bytes: usize,
            limit: usize,
        }
        impl Write for Counter {
            fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
                self.bytes = self.bytes.checked_add(buffer.len()).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::FileTooLarge, "task byte count overflow")
                })?;
                if self.bytes > self.limit {
                    return Err(io::Error::new(
                        io::ErrorKind::FileTooLarge,
                        "durable task contract size limit exceeded",
                    ));
                }
                Ok(buffer.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut counter = Counter {
            bytes: 0,
            limit: MAX_DURABLE_TASK_CONTRACT_BYTES,
        };
        serde_json::to_writer(&mut counter, value)
            .map_err(|_| anyhow!("{field} exceeds or cannot satisfy the durable size limit"))?;
        Ok(())
    }
}
