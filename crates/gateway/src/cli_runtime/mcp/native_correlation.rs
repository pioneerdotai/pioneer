//! Native item correlation shared by the Codex and OpenCode MCP adapters.
//! The facade execution ledger remains the sole execution/replay journal.
use crate::turn_mcp::invoker::TurnMcpInvoker;
use anyhow::{Result, bail};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::time::timeout as tokio_timeout;
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
        let notified = self.changed.notified();
        if let Some(key) = self.claim_now(
            canonical_callable_name,
            arguments_fingerprint,
            facade_request_id,
        ) {
            return Some(key);
        }
        let _ = tokio_timeout(Duration::from_millis(250), notified).await;
        self.claim_now(
            canonical_callable_name,
            arguments_fingerprint,
            facade_request_id,
        )
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
