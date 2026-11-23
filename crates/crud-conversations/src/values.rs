//! Database tags shared by conversation and execution repositories.
use chrono::Utc;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

use pioneer_protocol::{
    ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, TurnStatus,
};

pub(crate) fn unix_to_datetime(value: i64) -> DateTimeWithTimeZone {
    chrono::DateTime::from_timestamp(value, 0)
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}

pub fn thread_mode_from_db(value: &str) -> Option<ThreadMode> {
    match value {
        "message" => Some(ThreadMode::Message),
        "chat" => Some(ThreadMode::Chat),
        "agent" => Some(ThreadMode::Agent),
        _ => None,
    }
}

pub fn thread_mode_to_db(mode: ThreadMode) -> &'static str {
    match mode {
        ThreadMode::Message => "message",
        ThreadMode::Chat => "chat",
        ThreadMode::Agent => "agent",
    }
}

pub fn turn_status_to_db(status: TurnStatus) -> &'static str {
    match status {
        TurnStatus::InProgress => "in_progress",
        TurnStatus::Completed => "completed",
        TurnStatus::Failed => "failed",
        TurnStatus::Interrupted => "interrupted",
        TurnStatus::Blocked => "blocked",
    }
}

pub fn thread_origin_kind_to_db(kind: ThreadOriginKind) -> &'static str {
    match kind {
        ThreadOriginKind::Collaborative => "collaborative",
        ThreadOriginKind::DirectMessage => "direct_message",
        ThreadOriginKind::User => "user",
        ThreadOriginKind::TaskRun => "task_run",
        ThreadOriginKind::System => "system",
    }
}

pub fn thread_origin_kind_from_db(value: &str) -> Option<ThreadOriginKind> {
    match value {
        "collaborative" => Some(ThreadOriginKind::Collaborative),
        "direct_message" => Some(ThreadOriginKind::DirectMessage),
        "user" => Some(ThreadOriginKind::User),
        "task_run" => Some(ThreadOriginKind::TaskRun),
        "system" => Some(ThreadOriginKind::System),
        _ => None,
    }
}

pub fn thread_sidebar_visibility_to_db(visibility: ThreadSidebarVisibility) -> &'static str {
    match visibility {
        ThreadSidebarVisibility::Visible => "visible",
        ThreadSidebarVisibility::Hidden => "hidden",
    }
}

pub fn thread_sidebar_visibility_from_db(value: &str) -> Option<ThreadSidebarVisibility> {
    match value {
        "visible" => Some(ThreadSidebarVisibility::Visible),
        "hidden" => Some(ThreadSidebarVisibility::Hidden),
        _ => None,
    }
}

pub fn turn_kind_to_db(kind: pioneer_protocol::TurnKind) -> &'static str {
    match kind {
        pioneer_protocol::TurnKind::Conversation => "conversation",
        pioneer_protocol::TurnKind::TaskRun => "task_run",
    }
}

pub fn turn_origin_to_db(origin: pioneer_protocol::TurnOrigin) -> &'static str {
    match origin {
        pioneer_protocol::TurnOrigin::User => "user",
        pioneer_protocol::TurnOrigin::ScheduledTask => "scheduled_task",
        pioneer_protocol::TurnOrigin::DetachedTask => "detached_task",
        pioneer_protocol::TurnOrigin::AttachedTask => "attached_task",
        pioneer_protocol::TurnOrigin::TaskDelivery => "task_delivery",
    }
}

pub fn thread_status_to_db(status: ThreadStatus) -> &'static str {
    match status {
        ThreadStatus::Active => "active",
        ThreadStatus::Idle => "idle",
        ThreadStatus::Closed => "closed",
    }
}

pub fn thread_status_from_db(value: &str) -> Option<ThreadStatus> {
    match value {
        "active" => Some(ThreadStatus::Active),
        "idle" => Some(ThreadStatus::Idle),
        "closed" => Some(ThreadStatus::Closed),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_db_values_are_snake_case() {
        assert_eq!(thread_mode_to_db(ThreadMode::Message), "message");
        assert_eq!(thread_mode_to_db(ThreadMode::Chat), "chat");
        assert_eq!(thread_mode_to_db(ThreadMode::Agent), "agent");
        assert_eq!(thread_status_to_db(ThreadStatus::Active), "active");
        assert_eq!(thread_status_to_db(ThreadStatus::Idle), "idle");
        assert_eq!(thread_status_to_db(ThreadStatus::Closed), "closed");
    }

    #[test]
    fn thread_mode_and_status_parser_accept_snake_case_only() {
        assert_eq!(thread_mode_from_db("message"), Some(ThreadMode::Message));
        assert_eq!(thread_mode_from_db("chat"), Some(ThreadMode::Chat));
        assert_eq!(thread_mode_from_db("agent"), Some(ThreadMode::Agent));
        assert_eq!(thread_mode_from_db("Chat"), None);

        assert_eq!(thread_status_from_db("active"), Some(ThreadStatus::Active));
        assert_eq!(thread_status_from_db("idle"), Some(ThreadStatus::Idle));
        assert_eq!(thread_status_from_db("closed"), Some(ThreadStatus::Closed));
        assert_eq!(thread_status_from_db("Active"), None);
    }
}
