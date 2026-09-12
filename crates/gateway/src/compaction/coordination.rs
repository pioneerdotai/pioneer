//! One service operation per context. Foreground preparation can interrupt an
//! optional background check, then waits until that owner's cleanup has ended.
use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{OwnedMutexGuard, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContextWorkPriority {
    Foreground,
    Background,
}
#[derive(Clone)]
struct Active {
    priority: ContextWorkPriority,
    cancellation: CancellationToken,
}
struct Owner {
    gate: Arc<tokio::sync::Mutex<()>>,
    active: watch::Sender<Option<Active>>,
}
#[derive(Default)]
pub(crate) struct ContextCompactionCoordinator {
    owners: Arc<Mutex<HashMap<(String, String), Weak<Owner>>>>,
}
struct OwnerReference {
    owner: Arc<Owner>,
    registry: Weak<Mutex<HashMap<(String, String), Weak<Owner>>>>,
    key: (String, String),
}
impl Drop for OwnerReference {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            if let Ok(mut registry) = registry.lock() {
                // Acquisition upgrades the weak reference under this same lock.
                // Every waiter owns this guard, including cancelled waiters.
                if Arc::strong_count(&self.owner) == 1
                    && registry
                        .get(&self.key)
                        .is_some_and(|owner| owner.ptr_eq(&Arc::downgrade(&self.owner)))
                {
                    registry.remove(&self.key);
                }
            }
        }
    }
}
pub(crate) struct ContextCompactionLease {
    reference: OwnerReference,
    cancellation: CancellationToken,
    _guard: OwnedMutexGuard<()>,
}
impl ContextCompactionLease {
    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}
impl Drop for ContextCompactionLease {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.reference.owner.active.send_replace(None);
    }
}
impl ContextCompactionCoordinator {
    pub(crate) async fn acquire(
        &self,
        workspace: &str,
        thread: &str,
        priority: ContextWorkPriority,
        cancellation: &CancellationToken,
    ) -> Result<Option<ContextCompactionLease>> {
        let key = (workspace.to_owned(), thread.to_owned());
        let owner = {
            let mut registry = self
                .owners
                .lock()
                .map_err(|_| anyhow::anyhow!("context ownership unavailable"))?;
            match registry.get(&key).and_then(Weak::upgrade) {
                Some(owner) => owner,
                None => {
                    let owner = Arc::new(Owner {
                        gate: Arc::new(tokio::sync::Mutex::new(())),
                        active: watch::channel(None).0,
                    });
                    registry.insert(key.clone(), Arc::downgrade(&owner));
                    owner
                }
            }
        };
        let reference = OwnerReference {
            owner,
            registry: Arc::downgrade(&self.owners),
            key,
        };
        let owner = &reference.owner;
        anyhow::ensure!(
            !cancellation.is_cancelled(),
            "context preparation cancelled"
        );
        let guard = match priority {
            ContextWorkPriority::Background => match owner.gate.clone().try_lock_owned() {
                Ok(guard) => guard,
                Err(_) => return Ok(None),
            },
            ContextWorkPriority::Foreground => {
                let mut activity = owner.active.subscribe();
                let lock = owner.gate.clone().lock_owned();
                tokio::pin!(lock);
                loop {
                    if let Some(active) = activity.borrow_and_update().as_ref() {
                        if active.priority == ContextWorkPriority::Background {
                            active.cancellation.cancel();
                        }
                    }
                    tokio::select! { biased;
                        _ = cancellation.cancelled() => anyhow::bail!("context preparation cancelled while waiting"),
                        guard = &mut lock => break guard,
                        changed = activity.changed() => { changed.map_err(|_| anyhow::anyhow!("context owner closed"))?; }
                    }
                }
            }
        };
        let child = cancellation.child_token();
        owner.active.send_replace(Some(Active {
            priority,
            cancellation: child.clone(),
        }));
        Ok(Some(ContextCompactionLease {
            reference,
            cancellation: child,
            _guard: guard,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn foreground_waits_for_cancelled_background_cleanup_before_entering() {
        let coordinator = Arc::new(ContextCompactionCoordinator::default());
        let parent = CancellationToken::new();
        let background = coordinator
            .acquire("ws", "thread", ContextWorkPriority::Background, &parent)
            .await
            .unwrap()
            .unwrap();
        let cancellation = background.cancellation();
        let contender = coordinator.clone();
        let incoming = tokio::spawn(async move {
            contender
                .acquire(
                    "ws",
                    "thread",
                    ContextWorkPriority::Foreground,
                    &CancellationToken::new(),
                )
                .await
                .unwrap()
                .unwrap()
        });
        cancellation.cancelled().await;
        assert!(
            !parent.is_cancelled(),
            "preemption must not cancel the user's completed execution token"
        );
        assert!(
            !incoming.is_finished(),
            "cleanup owner still holds the boundary"
        );
        drop(background);
        let foreground = incoming.await.unwrap();
        assert!(!foreground.cancellation().is_cancelled());
        assert!(
            coordinator
                .acquire(
                    "ws",
                    "thread",
                    ContextWorkPriority::Background,
                    &CancellationToken::new()
                )
                .await
                .unwrap()
                .is_none()
        );
        drop(foreground);
        assert!(coordinator.owners.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn cancelled_waiter_does_not_cancel_foreground_or_keep_the_context_locked() {
        let coordinator = Arc::new(ContextCompactionCoordinator::default());
        let foreground = coordinator
            .acquire(
                "ws",
                "thread",
                ContextWorkPriority::Foreground,
                &CancellationToken::new(),
            )
            .await
            .unwrap()
            .unwrap();
        let token = CancellationToken::new();
        let waiting_token = token.clone();
        let waiting_owner = coordinator.clone();
        let waiting = tokio::spawn(async move {
            waiting_owner
                .acquire(
                    "ws",
                    "thread",
                    ContextWorkPriority::Foreground,
                    &waiting_token,
                )
                .await
        });
        tokio::task::yield_now().await;
        token.cancel();
        assert!(waiting.await.unwrap().is_err());
        assert!(!foreground.cancellation().is_cancelled());
        drop(foreground);
        let next = coordinator
            .acquire(
                "ws",
                "thread",
                ContextWorkPriority::Background,
                &CancellationToken::new(),
            )
            .await
            .unwrap()
            .unwrap();
        drop(next);
        assert!(coordinator.owners.lock().unwrap().is_empty());
    }
}
