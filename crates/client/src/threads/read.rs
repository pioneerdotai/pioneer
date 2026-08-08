//! Shell-neutral unread projection and conservative mark-read planning.
//!
//! Counts and cursors remain Gateway-owned. These helpers never infer unread
//! from locally loaded timeline rows and never advance beyond an explicitly
//! viewed Turn supplied by a shell.

use pioneer_protocol::{ThreadReadParams, ThreadUnreadSummary};
use std::collections::BTreeMap;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadUnreadPresentation {
    pub thread_id: String,
    pub unread_count: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MarkThreadReadContext<'a> {
    pub active_thread_id: Option<&'a str>,
    pub thread_id: &'a str,
    pub application_is_foreground: bool,
    pub thread_is_visible: bool,
    pub latest_known_user_turn_id: Option<&'a str>,
    pub viewed_through_turn_id: Option<&'a str>,
    pub last_requested_through_turn_id: Option<&'a str>,
}

pub fn project_thread_unread(summaries: &[ThreadUnreadSummary]) -> Vec<ThreadUnreadPresentation> {
    let mut by_thread = BTreeMap::<String, u64>::new();
    for summary in summaries {
        if !summary.thread_id.trim().is_empty() {
            by_thread.insert(summary.thread_id.clone(), summary.unread_count);
        }
    }
    by_thread
        .into_iter()
        .map(|(thread_id, unread_count)| ThreadUnreadPresentation {
            thread_id,
            unread_count,
        })
        .collect()
}

pub fn plan_mark_thread_read(context: MarkThreadReadContext<'_>) -> Option<ThreadReadParams> {
    let thread_id = context.thread_id.trim();
    let latest_known_user_turn_id = context.latest_known_user_turn_id?.trim();
    let through_turn_id = context.viewed_through_turn_id?.trim();
    if thread_id.is_empty()
        || latest_known_user_turn_id.is_empty()
        || through_turn_id.is_empty()
        || through_turn_id != latest_known_user_turn_id
        || context.active_thread_id != Some(thread_id)
        || !context.application_is_foreground
        || !context.thread_is_visible
        || context.last_requested_through_turn_id == Some(through_turn_id)
    {
        return None;
    }

    Some(ThreadReadParams {
        thread_id: thread_id.to_owned(),
        through_turn_id: through_turn_id.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unread_projection_uses_only_authoritative_exact_thread_summaries() {
        let projected = project_thread_unread(&[
            ThreadUnreadSummary {
                thread_id: "thread_b".to_owned(),
                unread_count: 3,
            },
            ThreadUnreadSummary {
                thread_id: "thread_a".to_owned(),
                unread_count: 2,
            },
            ThreadUnreadSummary {
                thread_id: "thread_b".to_owned(),
                unread_count: 4,
            },
        ]);

        assert_eq!(
            projected,
            vec![
                ThreadUnreadPresentation {
                    thread_id: "thread_a".to_owned(),
                    unread_count: 2,
                },
                ThreadUnreadPresentation {
                    thread_id: "thread_b".to_owned(),
                    unread_count: 4,
                },
            ]
        );
    }

    #[test]
    fn mark_read_requires_active_visible_foreground_view_through_and_skips_exact_retry() {
        let ready = MarkThreadReadContext {
            active_thread_id: Some("thread_a"),
            thread_id: "thread_a",
            application_is_foreground: true,
            thread_is_visible: true,
            latest_known_user_turn_id: Some("turn_7"),
            viewed_through_turn_id: Some("turn_7"),
            last_requested_through_turn_id: None,
        };
        assert_eq!(
            plan_mark_thread_read(ready),
            Some(ThreadReadParams {
                thread_id: "thread_a".to_owned(),
                through_turn_id: "turn_7".to_owned(),
            })
        );

        for blocked in [
            MarkThreadReadContext {
                application_is_foreground: false,
                ..ready
            },
            MarkThreadReadContext {
                thread_is_visible: false,
                ..ready
            },
            MarkThreadReadContext {
                active_thread_id: Some("thread_b"),
                ..ready
            },
            MarkThreadReadContext {
                latest_known_user_turn_id: Some("turn_8"),
                ..ready
            },
            MarkThreadReadContext {
                viewed_through_turn_id: None,
                ..ready
            },
            MarkThreadReadContext {
                last_requested_through_turn_id: Some("turn_7"),
                ..ready
            },
        ] {
            assert_eq!(plan_mark_thread_read(blocked), None);
        }
    }
}
