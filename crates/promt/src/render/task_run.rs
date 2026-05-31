use chrono::{DateTime, Utc};
use pioneer_protocol::{
    Task, TaskAgentInput, TaskAgentPrompt, TaskAgentSpec, TaskAgentToolPolicy, TaskRun,
    TaskTrigger, TaskTriggerSpec, TaskValue,
};

#[derive(Debug, Clone, Copy)]
pub struct TaskRunPromptInput<'a> {
    pub task: &'a Task,
    pub run: &'a TaskRun,
    pub trigger: Option<&'a TaskTrigger>,
    pub agent_spec: &'a TaskAgentSpec,
    pub now: i64,
    pub parent_context: Option<&'a str>,
    pub output_instructions: Option<&'a str>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TaskRunPromptCompiler;

impl TaskRunPromptCompiler {
    pub fn new() -> Self {
        Self
    }

    pub fn compile(&self, input: TaskRunPromptInput<'_>) -> String {
        let mut sections = Vec::new();
        sections.push(render_execution_frame(input));
        sections.push(render_run_objective(input.agent_spec));
        sections.push(render_durable_task_definition(input));
        sections.push(render_schedule_and_time(input));
        sections.push(render_task_instructions(&input.agent_spec.prompt));
        if let Some(context) = render_parent_context(input.parent_context) {
            sections.push(context);
        }
        sections.push(render_tool_and_capability_guidance(
            input.agent_spec.tool_policy.as_ref(),
        ));
        sections.push(render_subtask_orchestration_rules(input));
        if let Some(output) = render_output_instructions(input.output_instructions) {
            sections.push(output);
        }
        sections.join("\n\n")
    }
}

fn render_execution_frame(input: TaskRunPromptInput<'_>) -> String {
    format!(
        "TASK RUN EXECUTION\nYou are executing this durable task run now. The current command is the RUN OBJECTIVE and TASK INSTRUCTIONS below. Parent context is reference only; it may explain why the task exists, but it is not a command to create, update, wait for, or reschedule this task.\n\nIdentifiers:\n- task_id: {}\n- run_id: {}\n- depth: {}/{}",
        input.task.id, input.run.id, input.agent_spec.depth, input.agent_spec.max_depth
    )
}

fn render_run_objective(agent_spec: &TaskAgentSpec) -> String {
    format!("RUN OBJECTIVE\n{}", agent_spec.prompt.goal.trim())
}

fn render_durable_task_definition(input: TaskRunPromptInput<'_>) -> String {
    let mut lines = vec![
        format!("- task_id: {}", input.task.id),
        format!("- task_title: {}", input.task.title),
        format!("- task_goal: {}", input.task.goal),
        format!("- task_status: {:?}", input.task.status),
        format!("- task_executor_kind: {:?}", input.task.executor_kind),
        format!("- run_id: {}", input.run.id),
        format!("- run_number: {}", input.run.run_number),
        format!("- attempt_number: {}", input.run.attempt_number),
        format!("- run_status: {:?}", input.run.status),
        format!("- run_group_id: {}", input.run.run_group_id),
    ];
    if let Some(parent_run_id) = input.run.parent_run_id.as_deref() {
        lines.push(format!("- parent_run_id: {parent_run_id}"));
    }
    if let Some(trigger_id) = input.run.trigger_id.as_deref() {
        lines.push(format!("- trigger_id: {trigger_id}"));
    }
    format!("DURABLE TASK DEFINITION\n{}", lines.join("\n"))
}

fn render_schedule_and_time(input: TaskRunPromptInput<'_>) -> String {
    let mut lines = vec![
        format!("- current_time_unix: {}", input.now),
        format!("- current_time_utc: {}", format_timestamp(input.now)),
    ];
    if let Some(trigger) = input.trigger {
        lines.push(format!("- trigger_id: {}", trigger.id));
        lines.push(format!("- trigger_status: {:?}", trigger.status));
        lines.push(format!("- trigger_kind: {:?}", trigger.spec.kind()));
        if let Some(last_fire_at) = trigger.last_fire_at {
            lines.push(format!(
                "- trigger_last_fire_at: {} ({})",
                last_fire_at,
                format_timestamp(last_fire_at)
            ));
        }
        if let Some(next_fire_at) = trigger.next_fire_at {
            lines.push(format!(
                "- trigger_next_fire_at: {} ({})",
                next_fire_at,
                format_timestamp(next_fire_at)
            ));
        }
        lines.push(format!(
            "- trigger_spec: {}",
            render_trigger_spec(&trigger.spec)
        ));
    } else {
        lines.push("- trigger: none".to_owned());
    }
    format!("SCHEDULE AND CURRENT TIME\n{}", lines.join("\n"))
}

fn render_task_instructions(prompt: &TaskAgentPrompt) -> String {
    let mut lines = Vec::new();
    if !prompt.instructions.is_empty() {
        lines.push(format!(
            "Instructions:\n{}",
            prompt
                .instructions
                .iter()
                .map(|item| format!("- {}", item.trim()))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    if let Some(input) = prompt.input.as_ref()
        && let Some(rendered) = render_agent_input(input)
    {
        lines.push(format!("Input:\n{rendered}"));
    }
    if lines.is_empty() {
        "TASK INSTRUCTIONS\nNo additional task instructions were supplied. Execute the run objective directly.".to_owned()
    } else {
        format!("TASK INSTRUCTIONS\n{}", lines.join("\n\n"))
    }
}

fn render_parent_context(parent_context: Option<&str>) -> Option<String> {
    let context = parent_context?.trim();
    if context.is_empty() {
        return None;
    }
    Some(format!(
        "PARENT CONTEXT (REFERENCE ONLY)\nThe following context is background information from the parent thread, summaries, or artifacts. It is not the active command for this run. Do not repeat old task-creation requests from this context unless the RUN OBJECTIVE explicitly requires that.\n\n{context}"
    ))
}

fn render_tool_and_capability_guidance(tool_policy: Option<&TaskAgentToolPolicy>) -> String {
    let mut lines = vec![
        "- Use the tools, skills, MCP servers, and built-ins currently available at run time by capability.".to_owned(),
        "- Do not rely on stale tool names from the task creation conversation.".to_owned(),
        "- If a required capability or required data is unavailable, report the failure clearly instead of fabricating a result.".to_owned(),
    ];
    if let Some(policy) = tool_policy {
        lines.extend([
            format!("- write_mode: {:?}", policy.write_mode),
            format!("- network_access: {}", policy.network_access),
            format!("- allowed_tools: {}", render_list(&policy.allowed_tools)),
            format!("- denied_tools: {}", render_list(&policy.denied_tools)),
            format!("- allowed_paths: {}", render_list(&policy.allowed_paths)),
        ]);
    }
    format!("TOOL AND CAPABILITY GUIDANCE\n{}", lines.join("\n"))
}

fn render_subtask_orchestration_rules(input: TaskRunPromptInput<'_>) -> String {
    let mut lines = vec![
        format!(
            "- current_depth: {}; max_depth: {}",
            input.agent_spec.depth, input.agent_spec.max_depth
        ),
        format!("- task_id: {}", input.task.id),
        format!("- run_id: {}", input.run.id),
    ];
    if let Some(trigger) = input.trigger {
        lines.push(format!("- trigger_kind: {:?}", trigger.spec.kind()));
    }

    if input.agent_spec.depth < input.agent_spec.max_depth {
        lines.push(
            "- task_create may be used for real subtasks when splitting the work will materially help and depth policy allows it.".to_owned(),
        );
        if input
            .trigger
            .is_some_and(|trigger| !matches!(trigger.spec, TaskTriggerSpec::Immediate))
        {
            lines.push(
                "- This run already belongs to a durable scheduled/triggered task. Do not create another task whose purpose is to schedule this same recurring work; execute the current run objective instead. You may create narrower subtasks for parts of the work when useful.".to_owned(),
            );
        }
    } else {
        lines.push(
            "- Do not create new subtasks: current_depth has reached max_depth. Complete the work directly or report the blocker clearly.".to_owned(),
        );
    }
    format!("SUBTASK ORCHESTRATION RULES\n{}", lines.join("\n"))
}

fn render_output_instructions(output_instructions: Option<&str>) -> Option<String> {
    let output = output_instructions?.trim();
    if output.is_empty() {
        None
    } else {
        Some(format!("OUTPUT INSTRUCTIONS\n{output}"))
    }
}

fn render_agent_input(input: &TaskAgentInput) -> Option<String> {
    let mut lines = Vec::new();
    if let Some(text) = input.text.as_deref()
        && !text.trim().is_empty()
    {
        lines.push(text.to_owned());
    }
    for variable in &input.variables {
        lines.push(format!(
            "Variable {}: {}",
            variable.name,
            render_task_value(&variable.value)
        ));
    }
    for attachment in &input.attachments {
        lines.push(format!(
            "Attachment {:?}: {}",
            attachment.kind,
            attachment
                .name
                .as_deref()
                .or(attachment.path.as_deref())
                .or(attachment.url.as_deref())
                .or(attachment.artifact_id.as_deref())
                .unwrap_or("unnamed")
        ));
    }
    for reference in &input.references {
        lines.push(format!(
            "Reference {:?}: {}{}",
            reference.kind,
            reference.id,
            reference
                .label
                .as_ref()
                .map(|label| format!(" ({label})"))
                .unwrap_or_default()
        ));
    }
    (!lines.is_empty()).then(|| lines.join("\n"))
}

fn render_task_value(value: &TaskValue) -> String {
    match value {
        TaskValue::Null => "null".to_owned(),
        TaskValue::Bool(value) => value.to_string(),
        TaskValue::Integer(value) => value.to_string(),
        TaskValue::Number(value) => value.to_string(),
        TaskValue::String(value) => value.clone(),
        TaskValue::List(_) | TaskValue::Object(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "<unserializable>".to_owned())
        }
    }
}

fn render_trigger_spec(spec: &TaskTriggerSpec) -> String {
    serde_json::to_string(spec).unwrap_or_else(|_| format!("{spec:?}"))
}

fn render_list(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(", ")
    }
}

fn format_timestamp(timestamp: i64) -> String {
    DateTime::<Utc>::from_timestamp(timestamp, 0)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| format!("invalid-unix-timestamp:{timestamp}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        TaskAgentInputVariable, TaskAgentWriteMode, TaskExecutorKind, TaskOwnerKind, TaskRunStatus,
        TaskStatus, TaskTriggerStatus,
    };
    use std::collections::BTreeMap;

    const NOW: i64 = 1_704_067_200;

    fn sample_task() -> Task {
        Task {
            id: "task_weather_daily01".to_owned(),
            workspace_id: "workspace_000000000".to_owned(),
            owner_kind: TaskOwnerKind::Thread,
            owner_id: Some("thread_parent000001".to_owned()),
            created_by_thread_id: Some("thread_parent000001".to_owned()),
            created_by_turn_id: Some("turn_create00000001".to_owned()),
            root_task_id: None,
            parent_task_id: None,
            executor_kind: TaskExecutorKind::Agent,
            status: TaskStatus::Scheduled,
            title: "Daily weather report".to_owned(),
            goal: "Send a daily weather forecast for Moscow, Dubai, and San Francisco.".to_owned(),
            priority: 0,
            lifecycle_policy: None,
            delivery_policy: None,
            retry_policy: None,
            timeout_policy: None,
            concurrency_policy: None,
            metadata: None,
            result: None,
            error: None,
            revision: 1,
            created_at: NOW,
            updated_at: NOW,
            completed_at: None,
        }
    }

    fn sample_run() -> TaskRun {
        TaskRun {
            id: "run_weather_000000001".to_owned(),
            task_id: "task_weather_daily01".to_owned(),
            trigger_id: Some("trigger_cron0000001".to_owned()),
            parent_run_id: None,
            run_group_id: "run_group0000000001".to_owned(),
            attempt_number: 1,
            retry_of_run_id: None,
            ready_at: Some(NOW),
            run_number: 1,
            status: TaskRunStatus::Running,
            executor_kind: TaskExecutorKind::Agent,
            started_at: Some(NOW),
            completed_at: None,
            heartbeat_at: None,
            locked_by: None,
            lock_expires_at: None,
            result: None,
            error: None,
            created_at: NOW,
            updated_at: NOW,
        }
    }

    fn sample_trigger() -> TaskTrigger {
        TaskTrigger {
            id: "trigger_cron0000001".to_owned(),
            task_id: "task_weather_daily01".to_owned(),
            status: TaskTriggerStatus::Active,
            spec: TaskTriggerSpec::Cron {
                cron_expr: "0 7 * * *".to_owned(),
                timezone: "Europe/Moscow".to_owned(),
                catch_up_policy: None,
            },
            next_fire_at: Some(NOW + 86_400),
            last_fire_at: Some(NOW),
            created_at: NOW,
            updated_at: NOW,
        }
    }

    fn sample_agent_spec(depth: i64, max_depth: i64) -> TaskAgentSpec {
        TaskAgentSpec {
            id: "agent_spec000000001".to_owned(),
            task_id: "task_weather_daily01".to_owned(),
            run_id: Some("run_weather_000000001".to_owned()),
            agent_role: Some("weather reporter".to_owned()),
            agent_nickname: Some("Weather".to_owned()),
            model: Some("model".to_owned()),
            model_provider: Some("provider".to_owned()),
            prompt: TaskAgentPrompt {
                goal: "Prepare today's forecast for Moscow, Dubai, and San Francisco.".to_owned(),
                instructions: vec![
                    "Use currently available weather-capable tools by capability.".to_owned(),
                    "Include temperature, precipitation chance, and a concise summary.".to_owned(),
                ],
                input: Some(TaskAgentInput {
                    text: Some("Cities: Moscow, Dubai, San Francisco.".to_owned()),
                    variables: vec![TaskAgentInputVariable {
                        name: "language".to_owned(),
                        value: TaskValue::String("ru".to_owned()),
                    }],
                    attachments: Vec::new(),
                    references: Vec::new(),
                }),
                output_instructions: Some(
                    "Return concise markdown. If weather data is unavailable, say what failed."
                        .to_owned(),
                ),
            },
            context_policy: None,
            tool_policy: Some(TaskAgentToolPolicy {
                allowed_tools: Vec::new(),
                denied_tools: Vec::new(),
                write_mode: TaskAgentWriteMode::ReadOnly,
                allowed_paths: Vec::new(),
                network_access: true,
            }),
            result_contract: None,
            review_policy: None,
            depth,
            max_depth,
            created_at: NOW,
            updated_at: NOW,
        }
    }

    fn compile(
        task: &Task,
        run: &TaskRun,
        trigger: Option<&TaskTrigger>,
        agent_spec: &TaskAgentSpec,
        parent_context: Option<&str>,
    ) -> String {
        TaskRunPromptCompiler::new().compile(TaskRunPromptInput {
            task,
            run,
            trigger,
            agent_spec,
            now: NOW,
            parent_context,
            output_instructions: agent_spec.prompt.output_instructions.as_deref(),
        })
    }

    #[test]
    fn scheduled_weather_prompt_uses_run_objective_as_active_command() {
        let task = sample_task();
        let run = sample_run();
        let trigger = sample_trigger();
        let agent_spec = sample_agent_spec(0, 2);
        let prompt = compile(
            &task,
            &run,
            Some(&trigger),
            &agent_spec,
            Some("User: Create a task that sends weather every morning."),
        );

        assert!(prompt.contains("RUN OBJECTIVE\nPrepare today's forecast"));
        assert!(prompt.contains("SCHEDULE AND CURRENT TIME"));
        assert!(prompt.contains("trigger_kind: Cron"));
        assert!(prompt.contains("PARENT CONTEXT (REFERENCE ONLY)"));
        assert!(prompt.contains(
            "Do not create another task whose purpose is to schedule this same recurring work"
        ));
        let create_task_pos = prompt.find("Create a task").expect("parent context phrase");
        let parent_context_pos = prompt
            .find("PARENT CONTEXT (REFERENCE ONLY)")
            .expect("parent context section");
        assert!(create_task_pos > parent_context_pos);
        assert!(!prompt[..parent_context_pos].contains("Create a task"));
        assert_eq!(
            prompt,
            include_str!("../../tests/fixtures/prompt_compiler/daily_weather.golden")
                .trim_end_matches('\n')
        );
    }

    #[test]
    fn depth_below_max_allows_real_subtasks() {
        let task = sample_task();
        let run = sample_run();
        let trigger = sample_trigger();
        let agent_spec = sample_agent_spec(1, 3);
        let prompt = compile(&task, &run, Some(&trigger), &agent_spec, None);

        assert!(prompt.contains("current_depth: 1; max_depth: 3"));
        assert!(prompt.contains("task_create may be used for real subtasks"));
    }

    #[test]
    fn depth_at_max_rejects_new_subtasks_in_prompt() {
        let task = sample_task();
        let run = sample_run();
        let trigger = sample_trigger();
        let agent_spec = sample_agent_spec(2, 2);
        let prompt = compile(&task, &run, Some(&trigger), &agent_spec, None);

        assert!(prompt.contains("current_depth: 2; max_depth: 2"));
        assert!(prompt.contains("Do not create new subtasks"));
        assert!(!prompt.contains("task_create may be used for real subtasks"));
    }

    #[test]
    fn immediate_attached_subagent_golden_prompt_is_stable() {
        let mut task = sample_task();
        task.status = TaskStatus::Queued;
        task.title = "Inspect code".to_owned();
        task.goal = "Find where TaskService is implemented.".to_owned();
        let mut run = sample_run();
        run.trigger_id = Some("trigger_immediate001".to_owned());
        run.parent_run_id = Some("parent_run000000001".to_owned());
        let trigger = TaskTrigger {
            id: "trigger_immediate001".to_owned(),
            task_id: task.id.clone(),
            status: TaskTriggerStatus::Active,
            spec: TaskTriggerSpec::Immediate,
            next_fire_at: None,
            last_fire_at: Some(NOW),
            created_at: NOW,
            updated_at: NOW,
        };
        let mut agent_spec = sample_agent_spec(0, 3);
        agent_spec.prompt.goal = "Find TaskService and summarize file:line references.".to_owned();
        agent_spec.prompt.instructions = vec!["Use search/read tools before answering.".to_owned()];
        agent_spec.prompt.input = None;

        let prompt = compile(&task, &run, Some(&trigger), &agent_spec, None);

        assert_eq!(
            prompt,
            include_str!("../../tests/fixtures/prompt_compiler/immediate_attached_subagent.golden")
                .trim_end_matches('\n')
        );
    }

    #[test]
    fn nested_scheduled_run_golden_prompt_keeps_parent_context_as_reference() {
        let task = sample_task();
        let mut run = sample_run();
        run.parent_run_id = Some("run_parent000000001".to_owned());
        run.run_number = 2;
        let trigger = sample_trigger();
        let mut agent_spec = sample_agent_spec(1, 2);
        agent_spec.prompt.input = Some(TaskAgentInput {
            text: Some("Nested task input.".to_owned()),
            variables: vec![TaskAgentInputVariable {
                name: "cities".to_owned(),
                value: TaskValue::List(vec![
                    TaskValue::String("Moscow".to_owned()),
                    TaskValue::String("Dubai".to_owned()),
                ]),
            }],
            attachments: Vec::new(),
            references: Vec::new(),
        });

        let prompt = compile(
            &task,
            &run,
            Some(&trigger),
            &agent_spec,
            Some("Assistant: I created the recurring weather task yesterday."),
        );

        assert!(prompt.contains("parent_run_id: run_parent000000001"));
        assert!(prompt.contains("Variable cities: {\"kind\":\"list\""));
        assert!(prompt.contains("PARENT CONTEXT (REFERENCE ONLY)"));
        assert!(prompt.contains("Assistant: I created the recurring weather task yesterday."));
        assert!(prompt.contains(
            "Do not create another task whose purpose is to schedule this same recurring work"
        ));
        assert_eq!(
            prompt,
            include_str!("../../tests/fixtures/prompt_compiler/nested_scheduled_run.golden")
                .trim_end_matches('\n')
        );
    }

    #[test]
    fn object_variables_render_deterministically() {
        let value = TaskValue::Object(BTreeMap::from([(
            "city".to_owned(),
            TaskValue::String("Moscow".to_owned()),
        )]));
        assert_eq!(
            render_task_value(&value),
            "{\"kind\":\"object\",\"value\":{\"city\":{\"kind\":\"string\",\"value\":\"Moscow\"}}}"
        );
    }
}
