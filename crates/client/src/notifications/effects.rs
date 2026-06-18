//! Notification-triggered refresh effect planning.

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum ClientEffect {
    RefreshWorkspaceList,
    RefreshGatewaySettings,
    RefreshProviderLists,
    QueueSkillsRefresh,
    EnqueueInFlightTurnsForResume,
    UnsubscribeThreads { thread_ids: Vec<String> },
}

pub trait ClientEffectSink {
    fn refresh_workspace_list(&mut self);
    fn refresh_gateway_settings(&mut self);
    fn refresh_provider_lists(&mut self);
    fn queue_skills_refresh(&mut self);
    fn enqueue_in_flight_turns_for_resume(&mut self);
    fn unsubscribe_threads(&mut self, thread_ids: Vec<String>);
}

pub fn execute_client_effects<Sink, Effects>(sink: &mut Sink, effects: Effects)
where
    Sink: ClientEffectSink,
    Effects: IntoIterator<Item = ClientEffect>,
{
    for effect in effects {
        execute_client_effect(sink, effect);
    }
}

pub fn execute_client_effect<Sink>(sink: &mut Sink, effect: ClientEffect)
where
    Sink: ClientEffectSink,
{
    match effect {
        ClientEffect::RefreshWorkspaceList => sink.refresh_workspace_list(),
        ClientEffect::RefreshGatewaySettings => sink.refresh_gateway_settings(),
        ClientEffect::RefreshProviderLists => sink.refresh_provider_lists(),
        ClientEffect::QueueSkillsRefresh => sink.queue_skills_refresh(),
        ClientEffect::EnqueueInFlightTurnsForResume => sink.enqueue_in_flight_turns_for_resume(),
        ClientEffect::UnsubscribeThreads { thread_ids } => sink.unsubscribe_threads(thread_ids),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingSink {
        calls: Vec<String>,
    }

    impl ClientEffectSink for RecordingSink {
        fn refresh_workspace_list(&mut self) {
            self.calls.push("refresh_workspace_list".to_owned());
        }

        fn refresh_gateway_settings(&mut self) {
            self.calls.push("refresh_gateway_settings".to_owned());
        }

        fn refresh_provider_lists(&mut self) {
            self.calls.push("refresh_provider_lists".to_owned());
        }

        fn queue_skills_refresh(&mut self) {
            self.calls.push("queue_skills_refresh".to_owned());
        }

        fn enqueue_in_flight_turns_for_resume(&mut self) {
            self.calls
                .push("enqueue_in_flight_turns_for_resume".to_owned());
        }

        fn unsubscribe_threads(&mut self, thread_ids: Vec<String>) {
            self.calls
                .push(format!("unsubscribe_threads:{}", thread_ids.join(",")));
        }
    }

    #[test]
    fn executes_effects_in_order_against_sink() {
        let mut sink = RecordingSink::default();

        execute_client_effects(
            &mut sink,
            vec![
                ClientEffect::RefreshWorkspaceList,
                ClientEffect::RefreshGatewaySettings,
                ClientEffect::RefreshProviderLists,
                ClientEffect::QueueSkillsRefresh,
                ClientEffect::EnqueueInFlightTurnsForResume,
                ClientEffect::UnsubscribeThreads {
                    thread_ids: vec!["thr_a".to_owned(), "thr_b".to_owned()],
                },
            ],
        );

        assert_eq!(
            sink.calls,
            vec![
                "refresh_workspace_list",
                "refresh_gateway_settings",
                "refresh_provider_lists",
                "queue_skills_refresh",
                "enqueue_in_flight_turns_for_resume",
                "unsubscribe_threads:thr_a,thr_b",
            ]
        );
    }
}
