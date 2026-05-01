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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolLoopBudgetReason {
    AgentRoundsExceeded,
    ToolCallsExceeded,
    ProviderReturnedToolsAfterToolsDisabled,
}

impl ToolLoopBudgetReason {
    pub fn code(self) -> &'static str {
        match self {
            Self::AgentRoundsExceeded => "max_agent_rounds_per_turn",
            Self::ToolCallsExceeded => "max_tool_calls_per_turn",
            Self::ProviderReturnedToolsAfterToolsDisabled => {
                "provider_returned_tools_after_tools_disabled"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolLoopRoundPlan {
    pub tools_enabled: bool,
    pub final_instruction: Option<String>,
    pub budget_exceeded: Option<ToolLoopBudgetExceeded>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolLoopGuardDecision {
    Continue,
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
                tools_enabled: false,
                final_instruction: Some(self.final_no_tools_instruction.clone()),
                budget_exceeded: None,
            });
        }

        if self.tool_capable_rounds >= self.budget.max_agent_rounds_per_turn {
            let instruction =
                self.request_final_answer(ToolLoopBudgetReason::AgentRoundsExceeded)?;
            let budget_exceeded = ToolLoopBudgetExceeded {
                reason: ToolLoopBudgetReason::AgentRoundsExceeded,
                limit: self.budget.max_agent_rounds_per_turn,
                observed: self.tool_capable_rounds,
                action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
            };
            self.final_no_tools_round_started = true;
            return Ok(ToolLoopRoundPlan {
                tools_enabled: false,
                final_instruction: Some(instruction),
                budget_exceeded: Some(budget_exceeded),
            });
        }

        self.tool_capable_rounds = self.tool_capable_rounds.saturating_add(1);
        Ok(ToolLoopRoundPlan {
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
            return self.request_final_answer_decision(
                ToolLoopBudgetReason::ToolCallsExceeded,
                self.budget.max_tool_calls_per_turn,
                next_total,
                format!(
                    "limit={} attempted_total={next_total}",
                    self.budget.max_tool_calls_per_turn
                ),
            );
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

    fn request_final_answer_decision(
        &mut self,
        reason: ToolLoopBudgetReason,
        limit: u32,
        observed: u32,
        detail: impl Into<String>,
    ) -> ToolLoopGuardDecision {
        let budget_exceeded = ToolLoopBudgetExceeded {
            reason,
            limit,
            observed,
            action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
        };
        match self.request_final_answer(reason) {
            Ok(instruction) => ToolLoopGuardDecision::RequestFinalAnswer {
                instruction,
                budget_exceeded,
            },
            Err(message) => ToolLoopGuardDecision::FailTurn {
                message: format!("{message}; {}", detail.into()),
                budget_exceeded: ToolLoopBudgetExceeded {
                    action: ToolLoopBudgetAction::FailTurn,
                    ..budget_exceeded
                },
            },
        }
    }

    fn request_final_answer(&mut self, reason: ToolLoopBudgetReason) -> Result<String, String> {
        self.request_final_answer_override_for_reason(
            reason,
            self.final_no_tools_instruction.clone(),
        )
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
    fn agent_round_budget_allows_exactly_one_final_no_tools_round() {
        let mut guard = guard(1, 8);

        let first = guard.begin_provider_round().expect("first round starts");
        assert!(first.tools_enabled);
        assert!(first.budget_exceeded.is_none());
        assert!(matches!(
            guard.after_provider_round(first.tools_enabled, 1),
            ToolLoopGuardDecision::Continue
        ));

        let final_round = guard
            .begin_provider_round()
            .expect("budget exhaustion should request final round");
        assert!(!final_round.tools_enabled);
        assert_eq!(
            final_round.final_instruction.as_deref(),
            Some("final answer only")
        );
        assert!(matches!(
            final_round.budget_exceeded,
            Some(ToolLoopBudgetExceeded {
                reason: ToolLoopBudgetReason::AgentRoundsExceeded,
                limit: 1,
                observed: 1,
                action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
            })
        ));
        assert!(matches!(
            guard.after_provider_round(final_round.tools_enabled, 0),
            ToolLoopGuardDecision::Continue
        ));

        let err = guard
            .begin_provider_round()
            .expect_err("a second final no-tools round must fail");
        assert!(err.contains("tool_loop_budget_exceeded"));
        assert!(err.contains("final_no_tools_round_already_used"));
    }

    #[test]
    fn tool_call_budget_requests_final_round_without_counting_excess_calls() {
        let mut guard = guard(8, 2);
        let first = guard.begin_provider_round().expect("first round starts");
        assert!(first.tools_enabled);

        let decision = guard.after_provider_round(first.tools_enabled, 3);
        assert!(matches!(
            decision,
            ToolLoopGuardDecision::RequestFinalAnswer {
                budget_exceeded: ToolLoopBudgetExceeded {
                    reason: ToolLoopBudgetReason::ToolCallsExceeded,
                    limit: 2,
                    observed: 3,
                    action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
                },
                ..
            }
        ));

        let final_round = guard
            .begin_provider_round()
            .expect("tool-call exhaustion should request final round");
        assert!(!final_round.tools_enabled);
        assert!(final_round.budget_exceeded.is_none());
    }

    #[test]
    fn provider_tools_after_tools_disabled_fails_deterministically() {
        let mut guard = guard(1, 8);
        let first = guard.begin_provider_round().expect("first round starts");
        assert!(matches!(
            guard.after_provider_round(first.tools_enabled, 1),
            ToolLoopGuardDecision::Continue
        ));
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
