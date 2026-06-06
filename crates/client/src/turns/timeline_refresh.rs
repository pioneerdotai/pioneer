//! Turn timeline refresh planning.

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurnTimelineRefreshState {
    pub in_flight: bool,
    pub dirty: bool,
    pub next_generation: u64,
    pub in_flight_generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnTimelineRefreshTransitionEvent {
    Request,
    Complete { generation: u64 },
}

pub fn transition_turn_timeline_refresh_state(
    state: Option<TurnTimelineRefreshState>,
    event: TurnTimelineRefreshTransitionEvent,
) -> (Option<TurnTimelineRefreshState>, Option<u64>) {
    let mut state = state.unwrap_or_default();
    match event {
        TurnTimelineRefreshTransitionEvent::Request => {
            state.next_generation = state.next_generation.saturating_add(1);
            if state.in_flight {
                state.dirty = true;
                return (Some(state), None);
            }
            state.in_flight = true;
            state.dirty = false;
            state.in_flight_generation = state.next_generation;
            let in_flight_generation = state.in_flight_generation;
            (Some(state), Some(in_flight_generation))
        }
        TurnTimelineRefreshTransitionEvent::Complete { generation } => {
            if !state.in_flight {
                return (Some(state), None);
            }
            let should_rerun = state.dirty || state.next_generation > generation;
            state.in_flight = false;
            state.dirty = false;
            if should_rerun {
                state.in_flight = true;
                state.in_flight_generation = state.next_generation;
                let in_flight_generation = state.in_flight_generation;
                return (Some(state), Some(in_flight_generation));
            }
            (None, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_transition_coalesces_dirty_requests() {
        let (state, first_generation) = transition_turn_timeline_refresh_state(
            None,
            TurnTimelineRefreshTransitionEvent::Request,
        );
        assert_eq!(first_generation, Some(1));

        let (state, second_generation) = transition_turn_timeline_refresh_state(
            state,
            TurnTimelineRefreshTransitionEvent::Request,
        );
        assert_eq!(second_generation, None);

        let (state, rerun_generation) = transition_turn_timeline_refresh_state(
            state,
            TurnTimelineRefreshTransitionEvent::Complete { generation: 1 },
        );
        assert_eq!(rerun_generation, Some(2));

        let (state, next_generation) = transition_turn_timeline_refresh_state(
            state,
            TurnTimelineRefreshTransitionEvent::Complete { generation: 2 },
        );
        assert!(state.is_none());
        assert_eq!(next_generation, None);
    }

    #[test]
    fn refresh_transition_cleans_state_after_clean_complete() {
        let (state, first_generation) = transition_turn_timeline_refresh_state(
            None,
            TurnTimelineRefreshTransitionEvent::Request,
        );
        assert_eq!(first_generation, Some(1));

        let (state, queued_generation) = transition_turn_timeline_refresh_state(
            state,
            TurnTimelineRefreshTransitionEvent::Complete { generation: 1 },
        );
        assert!(state.is_none());
        assert_eq!(queued_generation, None);
    }
}
