use crate::TaskRuntimeResult;
use anyhow::{anyhow, bail};
use pioneer_protocol::{
    Task, TaskAgentContext, TaskAgentInput, TaskAgentPrompt, TaskAgentSpec, TaskAgentSpecInput,
    TaskCreateParams, TaskResourceBudget, TaskTrigger, TaskTriggerSpec, TaskUpdateParams,
    TaskValue,
};
use serde::Serialize;
use std::io::{self, Write};

pub(crate) struct TaskAdmissionNormalizer {
    budget: TaskResourceBudget,
}

impl TaskAdmissionNormalizer {
    pub(crate) const fn new(budget: TaskResourceBudget) -> Self {
        Self { budget }
    }

    pub(crate) fn validate_create(&self, params: &TaskCreateParams) -> TaskRuntimeResult<()> {
        self.validate_encoded(params, "task create request")?;
        self.validate_title(params.title.as_str())?;
        self.validate_string(params.goal.as_str(), "goal")?;
        self.validate_trigger(&params.trigger.spec)?;
        if let Some(spec) = params.agent_spec.as_ref() {
            self.validate_agent_spec_input(spec)?;
        }
        if let Some(metadata) = params.metadata.as_ref() {
            self.validate_count(metadata.labels.len(), "metadata labels")?;
            for label in &metadata.labels {
                self.validate_string(label, "metadata label")?;
            }
            if let Some(value) = metadata.data.as_ref() {
                self.validate_task_value(value, "metadata data")?;
            }
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
        if let Some(goal) = params.goal.as_deref() {
            self.validate_string(goal, "goal")?;
        }
        if let Some(trigger) = params.trigger.as_ref() {
            self.validate_trigger(&trigger.spec)?;
        }
        if let Some(instructions) = params.instructions.as_ref() {
            self.validate_count(instructions.len(), "instructions")?;
            for instruction in instructions {
                self.validate_string(instruction, "instruction")?;
            }
        }
        if let Some(text) = params.input_text.as_deref() {
            self.validate_string(text, "input text")?;
        }
        if let Some(input) = params.input.as_ref() {
            self.validate_input(input)?;
        }
        if let Some(text) = params.output_instructions.as_deref() {
            self.validate_string(text, "output instructions")?;
        }
        if let Some(context) = params
            .context_policy
            .as_ref()
            .and_then(|policy| policy.custom_context.as_ref())
        {
            self.validate_context(context)?;
        }
        if let Some(policy) = params.tool_policy.as_ref() {
            self.validate_count(policy.allowed_tools.len(), "allowed tools")?;
            self.validate_count(policy.denied_tools.len(), "denied tools")?;
            self.validate_count(policy.allowed_paths.len(), "allowed paths")?;
            for value in policy
                .allowed_tools
                .iter()
                .chain(policy.denied_tools.iter())
                .chain(policy.allowed_paths.iter())
            {
                self.validate_string(value, "tool policy entry")?;
            }
        }
        if let Some(metadata) = params.metadata.as_ref() {
            self.validate_count(metadata.labels.len(), "metadata labels")?;
            if let Some(value) = metadata.data.as_ref() {
                self.validate_task_value(value, "metadata data")?;
            }
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
        self.validate_string(task.goal.as_str(), "goal")?;
        if let Some(trigger) = trigger {
            self.validate_trigger(&trigger.spec)?;
        }
        if let Some(spec) = agent_spec {
            self.validate_prompt(&spec.prompt)?;
        }
        Ok(())
    }

    fn validate_agent_spec_input(&self, spec: &TaskAgentSpecInput) -> TaskRuntimeResult<()> {
        self.validate_prompt(&spec.prompt)?;
        if let Some(context) = spec
            .context_policy
            .as_ref()
            .and_then(|policy| policy.custom_context.as_ref())
        {
            self.validate_context(context)?;
        }
        if let Some(policy) = spec.tool_policy.as_ref() {
            self.validate_count(policy.allowed_tools.len(), "allowed tools")?;
            self.validate_count(policy.denied_tools.len(), "denied tools")?;
            self.validate_count(policy.allowed_paths.len(), "allowed paths")?;
        }
        if let Some(contract) = spec.result_contract.as_ref()
            && let Some(schema) = contract.schema.as_ref()
        {
            self.validate_task_value(&schema.schema, "result schema")?;
        }
        if let Some(review) = spec.review_policy.as_ref() {
            self.validate_count(review.reviewers.len(), "reviewers")?;
        }
        Ok(())
    }

    fn validate_prompt(&self, prompt: &TaskAgentPrompt) -> TaskRuntimeResult<()> {
        self.validate_string(prompt.goal.as_str(), "agent prompt goal")?;
        self.validate_count(prompt.instructions.len(), "instructions")?;
        for instruction in &prompt.instructions {
            self.validate_string(instruction, "instruction")?;
        }
        if let Some(input) = prompt.input.as_ref() {
            self.validate_input(input)?;
        }
        if let Some(value) = prompt.output_instructions.as_deref() {
            self.validate_string(value, "output instructions")?;
        }
        Ok(())
    }

    fn validate_input(&self, input: &TaskAgentInput) -> TaskRuntimeResult<()> {
        if let Some(text) = input.text.as_deref() {
            self.validate_string(text, "input text")?;
        }
        self.validate_count(input.variables.len(), "input variables")?;
        self.validate_count(input.attachments.len(), "input attachments")?;
        self.validate_count(input.references.len(), "input references")?;
        for variable in &input.variables {
            self.validate_string(variable.name.as_str(), "variable name")?;
            self.validate_task_value(&variable.value, "variable value")?;
        }
        Ok(())
    }

    fn validate_context(&self, context: &TaskAgentContext) -> TaskRuntimeResult<()> {
        if let Some(text) = context.text.as_deref() {
            self.validate_string(text, "custom context text")?;
        }
        self.validate_count(context.variables.len(), "context variables")?;
        self.validate_count(context.attachments.len(), "context attachments")?;
        self.validate_count(context.references.len(), "context references")?;
        for variable in &context.variables {
            self.validate_string(variable.name.as_str(), "context variable name")?;
            self.validate_task_value(&variable.value, "context variable value")?;
        }
        Ok(())
    }

    fn validate_trigger(&self, trigger: &TaskTriggerSpec) -> TaskRuntimeResult<()> {
        match trigger {
            TaskTriggerSpec::Interval {
                interval_seconds, ..
            } if *interval_seconds < self.budget.min_interval_seconds => {
                bail!(
                    "task interval {interval_seconds}s is below resource minimum {}s",
                    self.budget.min_interval_seconds
                );
            }
            TaskTriggerSpec::Dependency { policy } => {
                self.validate_count(policy.depends_on_task_ids.len(), "task dependencies")?;
            }
            TaskTriggerSpec::External {
                filter: Some(filter),
                ..
            } => {
                self.validate_count(filter.fields.len(), "external trigger fields")?;
                for value in filter.fields.values() {
                    self.validate_task_value(value, "external trigger field")?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_task_value(&self, root: &TaskValue, field: &str) -> TaskRuntimeResult<()> {
        let mut stack = vec![(root, 1usize)];
        let mut nodes = 0usize;
        while let Some((value, depth)) = stack.pop() {
            nodes = nodes.saturating_add(1);
            if nodes > self.budget.max_value_nodes {
                bail!("{field} exceeds task value node budget");
            }
            if depth > self.budget.max_value_depth {
                bail!("{field} exceeds task value depth budget");
            }
            match value {
                TaskValue::String(value) => self.validate_string(value, field)?,
                TaskValue::List(values) => {
                    self.validate_count(values.len(), field)?;
                    stack.extend(values.iter().map(|value| (value, depth + 1)));
                }
                TaskValue::Object(values) => {
                    self.validate_count(values.len(), field)?;
                    for (key, value) in values {
                        self.validate_string(key, field)?;
                        stack.push((value, depth + 1));
                    }
                }
                TaskValue::Null
                | TaskValue::Bool(_)
                | TaskValue::Integer(_)
                | TaskValue::Number(_) => {}
            }
        }
        Ok(())
    }

    fn validate_title(&self, title: &str) -> TaskRuntimeResult<()> {
        if title.len() > self.budget.max_title_bytes {
            bail!("task title exceeds {} bytes", self.budget.max_title_bytes);
        }
        Ok(())
    }

    fn validate_string(&self, value: &str, field: &str) -> TaskRuntimeResult<()> {
        if value.len() > self.budget.max_string_bytes {
            bail!("{field} exceeds {} bytes", self.budget.max_string_bytes);
        }
        Ok(())
    }

    fn validate_count(&self, count: usize, field: &str) -> TaskRuntimeResult<()> {
        if count > self.budget.max_collection_items {
            bail!(
                "{field} has {count} entries; maximum is {}",
                self.budget.max_collection_items
            );
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
                        "task byte budget exceeded",
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
            limit: self.budget.max_encoded_bytes,
        };
        serde_json::to_writer(&mut counter, value)
            .map_err(|_| anyhow!("{field} exceeds or cannot satisfy the encoded byte budget"))?;
        Ok(())
    }
}
