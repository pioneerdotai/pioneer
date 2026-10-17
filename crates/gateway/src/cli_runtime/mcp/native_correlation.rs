//! Native item correlation shared by the Codex and OpenCode MCP adapters.
//! The facade execution ledger remains the sole execution/replay journal.
use crate::turn_mcp::invoker::TurnMcpInvoker;
use anyhow::{Result, bail};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeMcpItemBinding {
    pub(crate) native_thread_id: String,
    pub(crate) native_turn_id: String,
    pub(crate) native_item_id: String,
    pub(crate) canonical_callable_name: String,
    pub(crate) arguments_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NativeMcpItemKey {
    pub(crate) native_thread_id: String,
    pub(crate) native_turn_id: String,
    pub(crate) native_item_id: String,
}

struct NativeMcpItemCorrelation {
    canonical_callable_name: String,
    arguments_fingerprint: String,
    sequence: u64,
    facade_request_id: Option<String>,
}

#[derive(Default)]
pub(crate) struct NativeMcpCorrelationLedger {
    items: StdMutex<HashMap<NativeMcpItemKey, NativeMcpItemCorrelation>>,
    next_sequence: AtomicU64,
    // None preserves the existing Codex claim semantics. OpenCode explicitly
    // activates one native turn before sending its deferred prompt.
    scope: StdMutex<Option<Option<(String, String)>>>,
    pub(crate) changed: tokio::sync::Notify,
}

impl NativeMcpCorrelationLedger {
    pub(crate) fn scoped() -> Self {
        Self {
            scope: StdMutex::new(Some(None)),
            ..Self::default()
        }
    }

    pub(crate) fn activate_scope(&self, thread: &str, turn: &str) {
        *self.scope.lock().unwrap() = Some(Some((thread.into(), turn.into())));
        self.items.lock().unwrap().clear();
    }

    pub(crate) fn deactivate_scope(&self) {
        *self.scope.lock().unwrap() = Some(None);
    }

    pub(crate) fn register(&self, binding: NativeMcpItemBinding) -> Result<()> {
        let key = NativeMcpItemKey {
            native_thread_id: binding.native_thread_id,
            native_turn_id: binding.native_turn_id,
            native_item_id: binding.native_item_id,
        };
        let mut items = self
            .items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned");
        if let Some(existing) = items.get(&key) {
            if existing.canonical_callable_name != binding.canonical_callable_name
                || existing.arguments_fingerprint != binding.arguments_fingerprint
            {
                bail!("Codex native MCP item identity changed during replay")
            }
            return Ok(());
        }
        items.insert(
            key,
            NativeMcpItemCorrelation {
                canonical_callable_name: binding.canonical_callable_name,
                arguments_fingerprint: binding.arguments_fingerprint,
                sequence: self.next_sequence.fetch_add(1, AtomicOrdering::Relaxed),
                facade_request_id: None,
            },
        );
        drop(items);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn callable(&self, key: &NativeMcpItemKey) -> Option<String> {
        self.items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned")
            .get(key)
            .map(|item| item.canonical_callable_name.clone())
    }

    pub(crate) fn contains(&self, key: &NativeMcpItemKey) -> bool {
        self.items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned")
            .contains_key(key)
    }

    pub(crate) async fn claim(
        &self,
        canonical_callable_name: &str,
        arguments_fingerprint: &str,
        facade_request_id: &str,
    ) -> Option<NativeMcpItemKey> {
        let deadline = Instant::now() + Duration::from_millis(250);
        loop {
            // notify_waiters() reaches a Notified created before registration,
            // even if it has not been polled yet. Create it before checking.
            let notified = self.changed.notified();
            if let Some(key) = self.claim_now(
                canonical_callable_name,
                arguments_fingerprint,
                facade_request_id,
            ) {
                return Some(key);
            }
            // A ready notification must not keep us looping past the deadline.
            if Instant::now() >= deadline || timeout_at(deadline, notified).await.is_err() {
                return self.claim_now(
                    canonical_callable_name,
                    arguments_fingerprint,
                    facade_request_id,
                );
            }
        }
    }

    fn claim_now(
        &self,
        canonical_callable_name: &str,
        arguments_fingerprint: &str,
        facade_request_id: &str,
    ) -> Option<NativeMcpItemKey> {
        let scope = self.scope.lock().unwrap().clone();
        if scope == Some(None) {
            return None;
        }
        let mut items = self
            .items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned");
        let key = items
            .iter()
            .filter(|(key, item)| {
                scope
                    .as_ref()
                    .and_then(|scope| scope.as_ref())
                    .is_none_or(|(thread, turn)| {
                        &key.native_thread_id == thread && &key.native_turn_id == turn
                    })
                    && item.facade_request_id.is_none()
                    && item.canonical_callable_name == canonical_callable_name
                    && item.arguments_fingerprint == arguments_fingerprint
            })
            .min_by_key(|(_, item)| item.sequence)
            .map(|(key, _)| key.clone())?;
        items
            .get_mut(&key)
            .expect("selected Codex MCP correlation must exist")
            .facade_request_id = Some(facade_request_id.to_owned());
        Some(key)
    }

    pub(crate) fn clear_turn(&self, native_thread_id: Option<&str>, native_turn_id: Option<&str>) {
        let (Some(native_thread_id), Some(native_turn_id)) = (native_thread_id, native_turn_id)
        else {
            return;
        };
        self.items
            .lock()
            .expect("Codex native MCP correlation ledger should not be poisoned")
            .retain(|key, _| {
                key.native_thread_id != native_thread_id || key.native_turn_id != native_turn_id
            });
    }
}

pub(crate) struct CorrelatingTurnMcpInvoker {
    pub(crate) inner: Arc<dyn TurnMcpInvoker>,
    pub(crate) native_items: Arc<NativeMcpCorrelationLedger>,
}

#[async_trait]
impl TurnMcpInvoker for CorrelatingTurnMcpInvoker {
    async fn invoke(
        &self,
        mut invocation: crate::turn_mcp::invoker::TurnMcpInvocation,
        cancellation: CancellationToken,
    ) -> std::result::Result<
        crate::turn_mcp::result::CanonicalMcpToolResult,
        crate::turn_mcp::invoker::TurnMcpInvocationError,
    > {
        let arguments_fingerprint =
            crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&invocation.arguments)
                .map_err(|_| {
                    crate::turn_mcp::invoker::TurnMcpInvocationError::new(
                        crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::InvalidRequest,
                        "CLI MCP invocation arguments are not canonicalizable",
                    )
                })?;
        let facade_request_id = invocation.provider_call_id.clone();
        let native_item = tokio::select! {
            biased;
            () = cancellation.cancelled() => None,
            native_item = self.native_items.claim(
                invocation.canonical_callable_name.as_str(),
                arguments_fingerprint.as_str(),
                facade_request_id.as_str(),
            ) => native_item,
        }
        .ok_or_else(|| {
            crate::turn_mcp::invoker::TurnMcpInvocationError::new(
                crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::SessionBindingUnavailable,
                "CLI MCP facade call has no matching native lifecycle item",
            )
        })?;
        invocation.provider_call_id = native_item.native_item_id;
        self.inner.invoke(invocation, cancellation).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn_mcp::invoker::{
        TurnMcpInvocation, TurnMcpInvocationError, TurnMcpInvocationErrorCode,
        TurnMcpInvocationOrigin,
    };
    use crate::turn_mcp::result::CanonicalMcpToolResult;
    use futures_util::poll;
    use serde_json::json;
    use std::task::Poll;
    use tokio::time::advance;

    const CALLABLE: &str = "test.echo";

    fn fingerprint(value: &str) -> String {
        crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&json!({ "value": value }))
            .expect("canonical test arguments")
    }

    fn binding(item: &str, value: &str) -> NativeMcpItemBinding {
        NativeMcpItemBinding {
            native_thread_id: "native-thread".into(),
            native_turn_id: "native-turn".into(),
            native_item_id: item.into(),
            canonical_callable_name: CALLABLE.into(),
            arguments_fingerprint: fingerprint(value),
        }
    }

    fn key(item: &str) -> NativeMcpItemKey {
        NativeMcpItemKey {
            native_thread_id: "native-thread".into(),
            native_turn_id: "native-turn".into(),
            native_item_id: item.into(),
        }
    }

    fn ledger(scoped: bool) -> NativeMcpCorrelationLedger {
        if scoped {
            let ledger = NativeMcpCorrelationLedger::scoped();
            ledger.activate_scope("native-thread", "native-turn");
            ledger
        } else {
            NativeMcpCorrelationLedger::default()
        }
    }

    // Poll each future explicitly to establish the registration/wakeup order.
    // Paused time keeps pending assertions independent of scheduler timing.
    #[tokio::test(start_paused = true)]
    async fn three_claims_wait_for_their_own_separately_registered_items() {
        for scoped in [false, true] {
            let ledger = ledger(scoped);
            let [a, b, c] = [fingerprint("a"), fingerprint("b"), fingerprint("c")];
            let mut claim_a = Box::pin(ledger.claim(CALLABLE, &a, "request-a"));
            let mut claim_b = Box::pin(ledger.claim(CALLABLE, &b, "request-b"));
            let mut claim_c = Box::pin(ledger.claim(CALLABLE, &c, "request-c"));
            assert!(poll!(claim_a.as_mut()).is_pending());
            assert!(poll!(claim_b.as_mut()).is_pending());
            assert!(poll!(claim_c.as_mut()).is_pending());

            ledger.register(binding("item-a", "a")).unwrap();
            assert_eq!(poll!(claim_a.as_mut()), Poll::Ready(Some(key("item-a"))));
            assert!(poll!(claim_b.as_mut()).is_pending());
            assert!(poll!(claim_c.as_mut()).is_pending());

            ledger.register(binding("item-b", "b")).unwrap();
            assert_eq!(poll!(claim_b.as_mut()), Poll::Ready(Some(key("item-b"))));
            assert!(poll!(claim_c.as_mut()).is_pending());

            ledger.register(binding("item-c", "c")).unwrap();
            assert_eq!(poll!(claim_c.as_mut()), Poll::Ready(Some(key("item-c"))));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn multiple_foreign_notifications_do_not_finish_claim() {
        let ledger = ledger(false);
        let arguments = fingerprint("own");
        let mut claim = Box::pin(ledger.claim(CALLABLE, &arguments, "request"));
        assert!(poll!(claim.as_mut()).is_pending());

        for item in ["foreign-a", "foreign-b", "foreign-c"] {
            ledger.register(binding(item, "foreign")).unwrap();
            assert!(poll!(claim.as_mut()).is_pending());
        }

        ledger.register(binding("own", "own")).unwrap();
        assert_eq!(poll!(claim.as_mut()), Poll::Ready(Some(key("own"))));
    }

    #[tokio::test(start_paused = true)]
    async fn already_registered_item_is_claimed_immediately() {
        for scoped in [false, true] {
            let ledger = ledger(scoped);
            ledger.register(binding("own", "own")).unwrap();
            let arguments = fingerprint("own");
            let started = Instant::now();
            let mut claim = Box::pin(ledger.claim(CALLABLE, &arguments, "request"));

            assert_eq!(poll!(claim.as_mut()), Poll::Ready(Some(key("own"))));
            assert_eq!(Instant::now(), started);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn missing_item_waits_until_the_deadline() {
        let ledger = ledger(false);
        let arguments = fingerprint("missing");
        let mut claim = Box::pin(ledger.claim(CALLABLE, &arguments, "request"));
        assert!(poll!(claim.as_mut()).is_pending());

        advance(Duration::from_millis(249)).await;
        assert!(poll!(claim.as_mut()).is_pending());
        advance(Duration::from_millis(1)).await;
        assert_eq!(poll!(claim.as_mut()), Poll::Ready(None));
    }

    #[tokio::test(start_paused = true)]
    async fn foreign_notifications_do_not_restart_the_wait_budget() {
        let ledger = ledger(false);
        let arguments = fingerprint("missing");
        let started = Instant::now();
        let mut claim = Box::pin(ledger.claim(CALLABLE, &arguments, "request"));
        assert!(poll!(claim.as_mut()).is_pending());

        for (index, millis) in [50, 50, 50, 50, 49].into_iter().enumerate() {
            advance(Duration::from_millis(millis)).await;
            ledger
                .register(binding(&format!("foreign-{index}"), "foreign"))
                .unwrap();
            assert!(poll!(claim.as_mut()).is_pending());
        }

        advance(Duration::from_millis(1)).await;
        // Leave a ready notification at the deadline as well.
        ledger.register(binding("last-foreign", "foreign")).unwrap();
        assert_eq!(poll!(claim.as_mut()), Poll::Ready(None));
        assert_eq!(Instant::now() - started, Duration::from_millis(250));
    }

    #[tokio::test(start_paused = true)]
    async fn item_published_before_notification_is_found_when_wait_times_out() {
        let ledger = ledger(false);
        let arguments = fingerprint("own");
        let mut claim = Box::pin(ledger.claim(CALLABLE, &arguments, "request"));
        assert!(poll!(claim.as_mut()).is_pending());

        advance(Duration::from_millis(250)).await;
        // Model register() after inserting the item but before notify_waiters().
        // The notification remains pending, so the timeout's final lookup must
        // find the item rather than relying on a notification to retry.
        ledger.items.lock().unwrap().insert(
            key("own"),
            NativeMcpItemCorrelation {
                canonical_callable_name: CALLABLE.into(),
                arguments_fingerprint: arguments.clone(),
                sequence: 0,
                facade_request_id: None,
            },
        );
        assert_eq!(poll!(claim.as_mut()), Poll::Ready(Some(key("own"))));
    }

    #[tokio::test(start_paused = true)]
    async fn one_item_cannot_be_claimed_by_two_waiting_calls() {
        for scoped in [false, true] {
            let ledger = ledger(scoped);
            let arguments = fingerprint("same");
            let mut first = Box::pin(ledger.claim(CALLABLE, &arguments, "first"));
            let mut second = Box::pin(ledger.claim(CALLABLE, &arguments, "second"));
            assert!(poll!(first.as_mut()).is_pending());
            assert!(poll!(second.as_mut()).is_pending());

            let item = binding("shared", "same");
            ledger.register(item.clone()).unwrap();
            assert_eq!(poll!(first.as_mut()), Poll::Ready(Some(key("shared"))));
            assert!(poll!(second.as_mut()).is_pending());
            // Replaying the registration must not make a claimed item available.
            ledger.register(item).unwrap();
            advance(Duration::from_millis(250)).await;
            assert_eq!(poll!(second.as_mut()), Poll::Ready(None));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn claim_rejects_foreign_scope_callable_and_arguments() {
        let ledger = ledger(true);
        let arguments = fingerprint("own");
        let mut claim = Box::pin(ledger.claim(CALLABLE, &arguments, "request"));
        assert!(poll!(claim.as_mut()).is_pending());

        let mut wrong_thread = binding("wrong-thread", "own");
        wrong_thread.native_thread_id = "foreign-thread".into();
        let mut wrong_turn = binding("wrong-turn", "own");
        wrong_turn.native_turn_id = "foreign-turn".into();
        let mut wrong_callable = binding("wrong-callable", "own");
        wrong_callable.canonical_callable_name = "test.other".into();
        for item in [
            wrong_thread,
            wrong_turn,
            wrong_callable,
            binding("wrong-args", "foreign"),
        ] {
            ledger.register(item).unwrap();
            assert!(poll!(claim.as_mut()).is_pending());
        }

        ledger.register(binding("own", "own")).unwrap();
        assert_eq!(poll!(claim.as_mut()), Poll::Ready(Some(key("own"))));
    }

    #[tokio::test(start_paused = true)]
    async fn inactive_scope_cannot_claim_an_item() {
        let ledger = ledger(true);
        ledger.register(binding("own", "own")).unwrap();
        ledger.deactivate_scope();
        let arguments = fingerprint("own");
        let mut claim = Box::pin(ledger.claim(CALLABLE, &arguments, "request"));
        assert!(poll!(claim.as_mut()).is_pending());

        advance(Duration::from_millis(250)).await;
        assert_eq!(poll!(claim.as_mut()), Poll::Ready(None));
    }

    struct UnexpectedInvoker;

    #[async_trait]
    impl TurnMcpInvoker for UnexpectedInvoker {
        async fn invoke(
            &self,
            _invocation: TurnMcpInvocation,
            _cancellation: CancellationToken,
        ) -> std::result::Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
            panic!("cancelled correlation must not invoke the inner MCP invoker");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_drops_the_pending_invocation_without_claiming_a_later_item() {
        let native_items = Arc::new(ledger(true));
        let invoker = CorrelatingTurnMcpInvoker {
            inner: Arc::new(UnexpectedInvoker),
            native_items: native_items.clone(),
        };
        let cancellation = CancellationToken::new();
        let invocation = TurnMcpInvocation {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            runtime_id: Some("opencode".into()),
            session_generation: Some(1),
            provider_call_id: "cancelled-request".into(),
            canonical_callable_name: CALLABLE.into(),
            arguments: json!({ "value": "own" }),
            origin: TurnMcpInvocationOrigin::CliFacade,
        };
        let started = Instant::now();
        let mut pending = Box::pin(invoker.invoke(invocation, cancellation.clone()));
        assert!(poll!(pending.as_mut()).is_pending());
        native_items
            .register(binding("foreign", "foreign"))
            .unwrap();
        assert!(poll!(pending.as_mut()).is_pending());

        cancellation.cancel();
        assert!(matches!(
            poll!(pending.as_mut()),
            Poll::Ready(Err(TurnMcpInvocationError {
                code: TurnMcpInvocationErrorCode::SessionBindingUnavailable,
                ..
            }))
        ));
        assert_eq!(Instant::now(), started);
        drop(pending);

        native_items.register(binding("own", "own")).unwrap();
        advance(Duration::from_millis(250)).await;
        let arguments = fingerprint("own");
        let mut later = Box::pin(native_items.claim(CALLABLE, &arguments, "later-request"));
        assert_eq!(poll!(later.as_mut()), Poll::Ready(Some(key("own"))));
    }
}
