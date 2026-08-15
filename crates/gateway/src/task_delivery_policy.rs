use anyhow::{Result, bail};
use pioneer_protocol::{TaskDeliveryMode, TaskDeliveryPolicy, TaskDeliveryThreadTarget};

/// Trusted thread lineage used to turn a semantic delivery target into the
/// exact thread id persisted with a Task. None of these ids come from model
/// input or an unverified client field.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TaskDeliveryThreadContext<'a> {
    pub current_thread_id: Option<&'a str>,
    pub origin_thread_id: Option<&'a str>,
    pub collaboration_root_thread_id: Option<&'a str>,
}

pub(crate) fn resolve_task_delivery_policy(
    policy: &mut TaskDeliveryPolicy,
    context: TaskDeliveryThreadContext<'_>,
) -> Result<()> {
    match policy.mode {
        TaskDeliveryMode::Thread => {
            let target = policy
                .thread_target
                .ok_or_else(|| anyhow::anyhow!("thread delivery requires threadTarget"))?;
            match target {
                TaskDeliveryThreadTarget::OriginThread => {
                    resolve_semantic_thread_id(policy, context.origin_thread_id, "origin_thread")?
                }
                TaskDeliveryThreadTarget::CurrentThread => {
                    resolve_semantic_thread_id(policy, context.current_thread_id, "current_thread")?
                }
                TaskDeliveryThreadTarget::CollaborationRoot => resolve_semantic_thread_id(
                    policy,
                    context.collaboration_root_thread_id,
                    "collaboration_root",
                )?,
                TaskDeliveryThreadTarget::ExactThread => {
                    let thread_id =
                        required_thread_id(policy.thread_id.as_deref(), "exact_thread")?;
                    policy.thread_id = Some(thread_id.to_owned());
                }
            }
        }
        TaskDeliveryMode::None | TaskDeliveryMode::UserNotification | TaskDeliveryMode::Webhook => {
            if policy.thread_target.is_some() || policy.thread_id.is_some() {
                bail!("non-thread delivery cannot carry a thread target");
            }
        }
    }
    Ok(())
}

fn resolve_semantic_thread_id(
    policy: &mut TaskDeliveryPolicy,
    authoritative_thread_id: Option<&str>,
    target_name: &str,
) -> Result<()> {
    let authoritative_thread_id = required_thread_id(authoritative_thread_id, target_name)?;
    if let Some(requested) = policy
        .thread_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        && requested != authoritative_thread_id
    {
        bail!("{target_name} delivery thread id differs from Gateway lineage");
    }
    policy.thread_id = Some(authoritative_thread_id.to_owned());
    Ok(())
}

fn required_thread_id<'a>(thread_id: Option<&'a str>, target_name: &str) -> Result<&'a str> {
    thread_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{target_name} delivery has no authoritative thread"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::TaskDeliveryFormat;

    fn policy(target: TaskDeliveryThreadTarget, thread_id: Option<&str>) -> TaskDeliveryPolicy {
        TaskDeliveryPolicy {
            mode: TaskDeliveryMode::Thread,
            thread_target: Some(target),
            thread_id: thread_id.map(str::to_owned),
            webhook_url: None,
            include_result: true,
            format: TaskDeliveryFormat::Summary,
        }
    }

    fn context() -> TaskDeliveryThreadContext<'static> {
        TaskDeliveryThreadContext {
            current_thread_id: Some("thread_current"),
            origin_thread_id: Some("thread_origin"),
            collaboration_root_thread_id: Some("thread_root"),
        }
    }

    #[test]
    fn gateway_resolves_all_semantic_thread_targets() {
        for (target, expected) in [
            (TaskDeliveryThreadTarget::OriginThread, "thread_origin"),
            (TaskDeliveryThreadTarget::CurrentThread, "thread_current"),
            (TaskDeliveryThreadTarget::CollaborationRoot, "thread_root"),
        ] {
            let mut policy = policy(target, None);
            resolve_task_delivery_policy(&mut policy, context()).expect("semantic resolution");
            assert_eq!(policy.thread_id.as_deref(), Some(expected));
        }
    }

    #[test]
    fn gateway_rejects_spoofed_semantic_thread_id() {
        let mut policy = policy(
            TaskDeliveryThreadTarget::OriginThread,
            Some("thread_attacker_selected"),
        );
        assert!(resolve_task_delivery_policy(&mut policy, context()).is_err());
    }

    #[test]
    fn exact_thread_requires_caller_supplied_id() {
        let mut missing = policy(TaskDeliveryThreadTarget::ExactThread, None);
        assert!(resolve_task_delivery_policy(&mut missing, context()).is_err());

        let mut exact = policy(
            TaskDeliveryThreadTarget::ExactThread,
            Some(" thread_exact "),
        );
        resolve_task_delivery_policy(&mut exact, context()).expect("exact resolution");
        assert_eq!(exact.thread_id.as_deref(), Some("thread_exact"));
    }

    #[test]
    fn thread_delivery_requires_explicit_target() {
        let mut policy = TaskDeliveryPolicy {
            mode: TaskDeliveryMode::Thread,
            thread_target: None,
            thread_id: Some("thread_exact".to_owned()),
            webhook_url: None,
            include_result: true,
            format: TaskDeliveryFormat::Summary,
        };
        assert!(resolve_task_delivery_policy(&mut policy, context()).is_err());
    }
}
