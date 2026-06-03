#[derive(Debug, Clone)]
pub struct ToolLoopBudgetConfig {
    pub max_agent_rounds_per_turn: u32,
    pub max_tool_calls_per_turn: u32,
}

impl Default for ToolLoopBudgetConfig {
    fn default() -> Self {
        Self {
            max_agent_rounds_per_turn: 512,
            max_tool_calls_per_turn: 2048,
        }
    }
}

impl ToolLoopBudgetConfig {
    pub fn normalized(&self) -> Self {
        Self {
            max_agent_rounds_per_turn: self.max_agent_rounds_per_turn.max(1),
            max_tool_calls_per_turn: self.max_tool_calls_per_turn.max(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionWindowBudgetConfig {
    pub max_agent_rounds_per_window: u32,
    pub max_tool_calls_per_window: u32,
    pub max_wall_clock_ms_per_window: Option<u64>,
    pub max_provider_tokens_per_window: Option<u64>,
}

impl Default for ExecutionWindowBudgetConfig {
    fn default() -> Self {
        Self {
            max_agent_rounds_per_window: 512,
            max_tool_calls_per_window: 2048,
            max_wall_clock_ms_per_window: None,
            max_provider_tokens_per_window: None,
        }
    }
}

impl ExecutionWindowBudgetConfig {
    pub fn normalized(&self) -> Self {
        Self {
            max_agent_rounds_per_window: self.max_agent_rounds_per_window.max(1),
            max_tool_calls_per_window: self.max_tool_calls_per_window.max(1),
            max_wall_clock_ms_per_window: self
                .max_wall_clock_ms_per_window
                .map(|value| value.max(1)),
            max_provider_tokens_per_window: self
                .max_provider_tokens_per_window
                .map(|value| value.max(1)),
        }
    }
}

impl From<ToolLoopBudgetConfig> for ExecutionWindowBudgetConfig {
    fn from(value: ToolLoopBudgetConfig) -> Self {
        Self {
            max_agent_rounds_per_window: value.max_agent_rounds_per_turn,
            max_tool_calls_per_window: value.max_tool_calls_per_turn,
            max_wall_clock_ms_per_window: None,
            max_provider_tokens_per_window: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionWindowTotalBudgetConfig {
    pub max_windows_per_turn: u32,
    pub max_tool_calls_per_turn: u32,
    pub max_wall_clock_ms_per_turn: Option<u64>,
    pub max_provider_tokens_per_turn: Option<u64>,
    pub max_consecutive_failed_windows: u32,
}

impl Default for ExecutionWindowTotalBudgetConfig {
    fn default() -> Self {
        Self {
            max_windows_per_turn: 16,
            max_tool_calls_per_turn: 4096,
            max_wall_clock_ms_per_turn: Some(86_400_000),
            max_provider_tokens_per_turn: None,
            max_consecutive_failed_windows: 3,
        }
    }
}

impl ExecutionWindowTotalBudgetConfig {
    pub fn normalized(&self) -> Self {
        Self {
            max_windows_per_turn: self.max_windows_per_turn.max(1),
            max_tool_calls_per_turn: self.max_tool_calls_per_turn.max(1),
            max_wall_clock_ms_per_turn: self.max_wall_clock_ms_per_turn.map(|value| value.max(1)),
            max_provider_tokens_per_turn: self
                .max_provider_tokens_per_turn
                .map(|value| value.max(1)),
            max_consecutive_failed_windows: self.max_consecutive_failed_windows.max(1),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExecutionWindowsConfig {
    pub window: ExecutionWindowBudgetConfig,
    pub total: ExecutionWindowTotalBudgetConfig,
}

impl ExecutionWindowsConfig {
    pub fn normalized(&self) -> Self {
        Self {
            window: self.window.normalized(),
            total: self.total.normalized(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLoopBudgetReason {
    AgentRoundsExceeded,
    ToolCallsExceeded,
    ProviderReturnedToolsAfterToolsDisabled,
}

impl ToolLoopBudgetReason {
    pub fn code(self) -> &'static str {
        match self {
            Self::AgentRoundsExceeded => "max_agent_rounds_per_window",
            Self::ToolCallsExceeded => "max_tool_calls_per_window",
            Self::ProviderReturnedToolsAfterToolsDisabled => {
                "provider_returned_tools_after_tools_disabled"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLoopRoundAction {
    StartProviderRound,
    RequestContinuation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolLoopRoundPlan {
    pub action: ToolLoopRoundAction,
    pub tools_enabled: bool,
    pub final_instruction: Option<String>,
    pub budget_exceeded: Option<ToolLoopBudgetExceeded>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolLoopGuardDecision {
    Continue,
    RequestContinuation {
        budget_exceeded: ToolLoopBudgetExceeded,
    },
    RequestFinalAnswer {
        instruction: String,
        budget_exceeded: ToolLoopBudgetExceeded,
    },
    FailTurn {
        message: String,
        budget_exceeded: ToolLoopBudgetExceeded,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLoopBudgetAction {
    ContinueInNextWindow,
    RequestFinalNoToolsRound,
    FailTurn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolLoopBudgetExceeded {
    pub reason: ToolLoopBudgetReason,
    pub limit: u32,
    pub observed: u32,
    pub action: ToolLoopBudgetAction,
}

#[derive(Debug)]
pub struct ToolLoopGuard {
    budget: ToolLoopBudgetConfig,
    tool_capable_rounds: u32,
    total_tool_calls: u32,
    final_no_tools_round_requested: bool,
    final_no_tools_round_started: bool,
    final_no_tools_instruction: String,
}

impl ToolLoopGuard {
    pub fn new(
        budget: ToolLoopBudgetConfig,
        final_no_tools_instruction: impl Into<String>,
    ) -> Self {
        Self {
            budget: budget.normalized(),
            tool_capable_rounds: 0,
            total_tool_calls: 0,
            final_no_tools_round_requested: false,
            final_no_tools_round_started: false,
            final_no_tools_instruction: final_no_tools_instruction.into(),
        }
    }

    pub fn begin_provider_round(&mut self) -> Result<ToolLoopRoundPlan, String> {
        if self.final_no_tools_round_requested {
            if self.final_no_tools_round_started {
                return Err(Self::terminal_message(
                    ToolLoopBudgetReason::AgentRoundsExceeded,
                    "final_no_tools_round_already_used",
                ));
            }

            self.final_no_tools_round_started = true;
            return Ok(ToolLoopRoundPlan {
                action: ToolLoopRoundAction::StartProviderRound,
                tools_enabled: false,
                final_instruction: Some(self.final_no_tools_instruction.clone()),
                budget_exceeded: None,
            });
        }

        if self.tool_capable_rounds >= self.budget.max_agent_rounds_per_turn {
            let budget_exceeded = ToolLoopBudgetExceeded {
                reason: ToolLoopBudgetReason::AgentRoundsExceeded,
                limit: self.budget.max_agent_rounds_per_turn,
                observed: self.tool_capable_rounds,
                action: ToolLoopBudgetAction::ContinueInNextWindow,
            };
            return Ok(ToolLoopRoundPlan {
                action: ToolLoopRoundAction::RequestContinuation,
                tools_enabled: false,
                final_instruction: None,
                budget_exceeded: Some(budget_exceeded),
            });
        }

        self.tool_capable_rounds = self.tool_capable_rounds.saturating_add(1);
        Ok(ToolLoopRoundPlan {
            action: ToolLoopRoundAction::StartProviderRound,
            tools_enabled: true,
            final_instruction: None,
            budget_exceeded: None,
        })
    }

    pub fn after_provider_round(
        &mut self,
        tools_enabled: bool,
        tool_call_count: usize,
    ) -> ToolLoopGuardDecision {
        if !tools_enabled {
            if tool_call_count > 0 {
                let observed = u32::try_from(tool_call_count).unwrap_or(u32::MAX);
                let budget_exceeded = ToolLoopBudgetExceeded {
                    reason: ToolLoopBudgetReason::ProviderReturnedToolsAfterToolsDisabled,
                    limit: 0,
                    observed,
                    action: ToolLoopBudgetAction::FailTurn,
                };
                return ToolLoopGuardDecision::FailTurn {
                    message: Self::terminal_message(
                        ToolLoopBudgetReason::ProviderReturnedToolsAfterToolsDisabled,
                        format!("tool_calls={tool_call_count}"),
                    ),
                    budget_exceeded,
                };
            }

            return ToolLoopGuardDecision::Continue;
        }

        let tool_call_count = u32::try_from(tool_call_count).unwrap_or(u32::MAX);
        let next_total = self.total_tool_calls.saturating_add(tool_call_count);
        if next_total > self.budget.max_tool_calls_per_turn {
            return ToolLoopGuardDecision::RequestContinuation {
                budget_exceeded: ToolLoopBudgetExceeded {
                    reason: ToolLoopBudgetReason::ToolCallsExceeded,
                    limit: self.budget.max_tool_calls_per_turn,
                    observed: next_total,
                    action: ToolLoopBudgetAction::ContinueInNextWindow,
                },
            };
        }

        self.total_tool_calls = next_total;
        ToolLoopGuardDecision::Continue
    }

    pub fn request_final_answer_with_instruction(
        &mut self,
        instruction: impl Into<String>,
    ) -> Result<String, String> {
        self.request_final_answer_override(instruction.into())
    }

    fn request_final_answer_override(&mut self, instruction: String) -> Result<String, String> {
        self.request_final_answer_override_for_reason(
            ToolLoopBudgetReason::AgentRoundsExceeded,
            instruction,
        )
    }

    fn request_final_answer_override_for_reason(
        &mut self,
        reason: ToolLoopBudgetReason,
        instruction: String,
    ) -> Result<String, String> {
        if self.final_no_tools_round_started {
            return Err(Self::terminal_message(
                reason,
                "final_no_tools_round_already_used",
            ));
        }

        self.final_no_tools_round_requested = true;
        self.final_no_tools_instruction = instruction.clone();
        Ok(instruction)
    }

    fn terminal_message(reason: ToolLoopBudgetReason, detail: impl AsRef<str>) -> String {
        let detail = detail.as_ref();
        if detail.is_empty() {
            format!("tool_loop_budget_exceeded: {}", reason.code())
        } else {
            format!("tool_loop_budget_exceeded: {} ({detail})", reason.code())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn guard(max_agent_rounds_per_turn: u32, max_tool_calls_per_turn: u32) -> ToolLoopGuard {
        ToolLoopGuard::new(
            ToolLoopBudgetConfig {
                max_agent_rounds_per_turn,
                max_tool_calls_per_turn,
            },
            "final answer only",
        )
    }

    #[test]
    fn budget_reason_codes_use_window_scoped_names() {
        assert_eq!(
            ToolLoopBudgetReason::AgentRoundsExceeded.code(),
            "max_agent_rounds_per_window"
        );
        assert_eq!(
            ToolLoopBudgetReason::ToolCallsExceeded.code(),
            "max_tool_calls_per_window"
        );
        assert_eq!(
            ToolLoopBudgetReason::ProviderReturnedToolsAfterToolsDisabled.code(),
            "provider_returned_tools_after_tools_disabled"
        );
    }

    #[test]
    fn agent_round_budget_requests_continuation_without_final_no_tools_round() {
        let mut guard = guard(1, 8);

        let first = guard.begin_provider_round().expect("first round starts");
        assert_eq!(first.action, ToolLoopRoundAction::StartProviderRound);
        assert!(first.tools_enabled);
        assert!(first.budget_exceeded.is_none());
        assert!(matches!(
            guard.after_provider_round(first.tools_enabled, 1),
            ToolLoopGuardDecision::Continue
        ));

        let continuation = guard
            .begin_provider_round()
            .expect("budget exhaustion should request continuation");
        assert_eq!(
            continuation.action,
            ToolLoopRoundAction::RequestContinuation
        );
        assert!(!continuation.tools_enabled);
        assert!(continuation.final_instruction.is_none());
        assert!(matches!(
            continuation.budget_exceeded,
            Some(ToolLoopBudgetExceeded {
                reason: ToolLoopBudgetReason::AgentRoundsExceeded,
                limit: 1,
                observed: 1,
                action: ToolLoopBudgetAction::ContinueInNextWindow,
            })
        ));

        let repeated = guard
            .begin_provider_round()
            .expect("budget continuation must not consume final no-tools state");
        assert_eq!(repeated.action, ToolLoopRoundAction::RequestContinuation);
        assert!(repeated.final_instruction.is_none());
    }

    #[test]
    fn tool_call_budget_requests_continuation_without_counting_excess_calls() {
        let mut guard = guard(8, 2);
        let first = guard.begin_provider_round().expect("first round starts");
        assert!(first.tools_enabled);

        let decision = guard.after_provider_round(first.tools_enabled, 3);
        assert!(matches!(
            decision,
            ToolLoopGuardDecision::RequestContinuation {
                budget_exceeded: ToolLoopBudgetExceeded {
                    reason: ToolLoopBudgetReason::ToolCallsExceeded,
                    limit: 2,
                    observed: 3,
                    action: ToolLoopBudgetAction::ContinueInNextWindow,
                }
            }
        ));

        let next_round = guard
            .begin_provider_round()
            .expect("excess unexecuted calls should not be counted into guard state");
        assert!(next_round.tools_enabled);
        assert!(matches!(
            guard.after_provider_round(next_round.tools_enabled, 2),
            ToolLoopGuardDecision::Continue
        ));
    }

    #[test]
    fn exact_tool_call_limit_is_allowed_before_continuation() {
        let mut guard = guard(8, 2);
        let first = guard.begin_provider_round().expect("first round starts");
        assert!(matches!(
            guard.after_provider_round(first.tools_enabled, 2),
            ToolLoopGuardDecision::Continue
        ));

        let second = guard.begin_provider_round().expect("second round starts");
        let decision = guard.after_provider_round(second.tools_enabled, 1);
        assert!(matches!(
            decision,
            ToolLoopGuardDecision::RequestContinuation {
                budget_exceeded: ToolLoopBudgetExceeded {
                    reason: ToolLoopBudgetReason::ToolCallsExceeded,
                    limit: 2,
                    observed: 3,
                    action: ToolLoopBudgetAction::ContinueInNextWindow,
                }
            }
        ));
    }

    #[test]
    fn explicit_final_no_tools_api_is_preserved_for_non_budget_finalization() {
        let mut guard = guard(8, 8);
        let instruction = guard
            .request_final_answer_with_instruction("wrap up without tools")
            .expect("explicit finalization should be accepted before final round starts");
        assert_eq!(instruction, "wrap up without tools");

        let final_round = guard
            .begin_provider_round()
            .expect("explicit final no-tools round should start");
        assert_eq!(final_round.action, ToolLoopRoundAction::StartProviderRound);
        assert!(!final_round.tools_enabled);
        assert_eq!(
            final_round.final_instruction.as_deref(),
            Some("wrap up without tools")
        );
        assert!(final_round.budget_exceeded.is_none());

        let err = guard
            .begin_provider_round()
            .expect_err("a second final no-tools round must fail");
        assert!(err.contains("tool_loop_budget_exceeded"));
        assert!(err.contains("max_agent_rounds_per_window"));
        assert!(err.contains("final_no_tools_round_already_used"));
    }

    #[test]
    fn provider_tools_after_tools_disabled_fails_deterministically() {
        let mut guard = guard(8, 8);
        guard
            .request_final_answer_with_instruction("wrap up without tools")
            .expect("explicit finalization should be accepted");
        let final_round = guard
            .begin_provider_round()
            .expect("final no-tools round starts");

        let decision = guard.after_provider_round(final_round.tools_enabled, 1);
        assert!(matches!(
            decision,
            ToolLoopGuardDecision::FailTurn {
                budget_exceeded: ToolLoopBudgetExceeded {
                    reason: ToolLoopBudgetReason::ProviderReturnedToolsAfterToolsDisabled,
                    limit: 0,
                    observed: 1,
                    action: ToolLoopBudgetAction::FailTurn,
                },
                ..
            }
        ));
        if let ToolLoopGuardDecision::FailTurn { message, .. } = decision {
            assert!(message.contains("tool_loop_budget_exceeded"));
            assert!(message.contains("provider_returned_tools_after_tools_disabled"));
        }
    }
}
