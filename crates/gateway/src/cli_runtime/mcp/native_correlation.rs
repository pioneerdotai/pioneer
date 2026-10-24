//! Native item correlation shared by the Codex and OpenCode MCP adapters.
//! The facade execution ledger remains the sole execution/replay journal.
use super::coordinator::CliMcpCoordinator;
use super::grants::CliMcpBoundGrant;
use crate::turn_mcp::invoker::{
    TurnMcpInvocation, TurnMcpInvocationError, TurnMcpInvocationErrorCode, TurnMcpInvoker,
};
use anyhow::{Result, bail};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeMcpItemBinding {
    pub(crate) native_thread_id: String,
    pub(crate) native_turn_id: String,
    pub(crate) native_item_id: String,
    pub(crate) canonical_callable_name: String,
    pub(crate) arguments_fingerprint: String,
}

pub(crate) use crate::turn_mcp::invoker::NativeMcpCallIdentity as NativeMcpItemKey;

struct NativeMcpItemCorrelation {
    canonical_callable_name: String,
    arguments_fingerprint: String,
    facade_request_id: Option<String>,
}

#[derive(Default)]
pub(crate) struct NativeMcpCorrelationLedger {
    items: StdMutex<HashMap<NativeMcpItemKey, NativeMcpItemCorrelation>>,
    pub(crate) changed: tokio::sync::Notify,
}

impl NativeMcpCorrelationLedger {
    /// Drop previous-turn items when OpenCode activates its coordinator binding.
    pub(crate) fn clear(&self) {
        self.items.lock().unwrap().clear();
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

    /// OpenCode claims its exact item within the active coordinator binding.
    pub(crate) fn claim_exact(
        &self,
        invocation: &TurnMcpInvocation,
        fingerprint: &str,
        cancellation: &CancellationToken,
    ) -> std::result::Result<bool, TurnMcpInvocationError> {
        let identity = invocation.native_call.as_ref().ok_or_else(|| {
            TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::InvalidRequest,
                "OpenCode native identity is missing",
            )
        })?;
        let key = NativeMcpItemKey {
            native_thread_id: identity.native_thread_id.clone(),
            native_turn_id: identity.native_turn_id.clone(),
            native_item_id: identity.native_item_id.clone(),
        };
        let mut items = self
            .items
            .lock()
            .expect("native MCP ledger should not be poisoned");
        if cancellation.is_cancelled() {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::Cancelled,
                "OpenCode MCP admission cancelled",
            ));
        }
        if invocation
            .admission_deadline
            .is_none_or(|deadline| Instant::now() >= deadline)
        {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::TimedOut,
                "OpenCode MCP admission timed out",
            ));
        }
        let Some(item) = items.get_mut(&key) else {
            return Ok(false);
        };
        if item.canonical_callable_name != invocation.canonical_callable_name
            || item.arguments_fingerprint != fingerprint
            || item.facade_request_id.is_some()
        {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::InvalidRequest,
                "OpenCode native item mismatches this invocation or is already used",
            ));
        }
        item.facade_request_id = Some(invocation.provider_call_id.clone());
        Ok(true)
    }

    /// Exact Codex claim. The caller checks the existing coordinator binding
    /// before calling this, under the coordinator lock. Used marks live here,
    /// independently of the facade's bounded full-result cache.
    pub(crate) fn claim_codex_exact(
        &self,
        key: &NativeMcpItemKey,
        callable: &str,
        fingerprint: &str,
        request_id: &str,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> std::result::Result<bool, TurnMcpInvocationError> {
        let mut items = self
            .items
            .lock()
            .expect("native MCP ledger should not be poisoned");
        // Recheck after acquiring the item lock, immediately before admission.
        if cancellation.is_cancelled() {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::Cancelled,
                "Codex MCP admission cancelled",
            ));
        }
        if Instant::now() >= deadline {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::TimedOut,
                "Codex MCP admission timed out",
            ));
        }
        let Some(item) = items.get_mut(key) else {
            return Ok(false);
        };
        if item.canonical_callable_name != callable || item.arguments_fingerprint != fingerprint {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::InvalidRequest,
                "Codex MCP native item does not match the requested tool and arguments",
            ));
        }
        if item.facade_request_id.is_some() {
            return Err(TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::InvalidRequest,
                "Codex MCP native item has already been used",
            ));
        }
        item.facade_request_id = Some(request_id.to_owned());
        Ok(true)
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

/// References to the existing authorization owner; no separate native scope.
pub(crate) struct CodexMcpCorrelationBinding {
    pub(crate) coordinator: Arc<CliMcpCoordinator>,
    pub(crate) bound_grant: CliMcpBoundGrant,
}

pub(crate) struct CorrelatingTurnMcpInvoker {
    pub(crate) inner: Arc<dyn TurnMcpInvoker>,
    pub(crate) native_items: Arc<NativeMcpCorrelationLedger>,
    // Factory-owned binding references, independent of the instance's name.
    pub(crate) opencode_binding: Option<(Arc<CliMcpCoordinator>, CliMcpBoundGrant)>,
    pub(crate) codex_binding: Option<CodexMcpCorrelationBinding>,
}

impl CorrelatingTurnMcpInvoker {
    async fn claim_codex(
        &self,
        invocation: &TurnMcpInvocation,
        fingerprint: &str,
        cancellation: &CancellationToken,
    ) -> std::result::Result<NativeMcpItemKey, TurnMcpInvocationError> {
        let invalid = || {
            TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::InvalidRequest,
                "Codex MCP invocation is missing native admission metadata",
            )
        };
        let identity = invocation.native_call.as_ref().ok_or_else(invalid)?;
        let deadline = invocation.admission_deadline.ok_or_else(invalid)?;
        let binding = self.codex_binding.as_ref().ok_or_else(invalid)?;
        let key = identity.clone();
        loop {
            // Subscribe before inspecting either source; no lost wakeup between
            // durable retarget/native registration and the next wait.
            let item_changed = self.native_items.changed.notified();
            let turn_changed = binding.coordinator.native_turn_changed.notified();
            let claim = binding.coordinator.claim_codex_native_item(
                &binding.bound_grant,
                invocation,
                &key,
                &self.native_items,
                fingerprint,
                cancellation,
                deadline,
            );
            let claimed = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::Cancelled, "Codex MCP admission cancelled")),
                () = tokio::time::sleep_until(deadline) => return Err(TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::TimedOut, "Codex MCP native confirmation timed out")),
                claimed = claim => claimed?,
            };
            if claimed {
                return Ok(key);
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::Cancelled, "Codex MCP admission cancelled")),
                () = tokio::time::sleep_until(deadline) => return Err(TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::TimedOut, "Codex MCP native confirmation timed out")),
                () = item_changed => {},
                () = turn_changed => {},
            }
        }
    }

    async fn claim_opencode(
        &self,
        invocation: &TurnMcpInvocation,
        fingerprint: &str,
        cancellation: &CancellationToken,
    ) -> std::result::Result<(), TurnMcpInvocationError> {
        let (coordinator, bound) = self
            .opencode_binding
            .as_ref()
            .expect("OpenCode factory binding");
        let deadline = invocation.admission_deadline.ok_or_else(|| {
            TurnMcpInvocationError::new(
                TurnMcpInvocationErrorCode::InvalidRequest,
                "OpenCode admission deadline is missing",
            )
        })?;
        loop {
            let notified = self.native_items.changed.notified();
            let claim = coordinator.claim_correlated_call(bound, invocation, cancellation, || {
                self.native_items
                    .claim_exact(invocation, fingerprint, cancellation)
            });
            let claimed = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::Cancelled, "OpenCode MCP admission cancelled")),
                () = tokio::time::sleep_until(deadline) => return Err(TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::TimedOut, "OpenCode MCP confirmation timed out")),
                claimed = claim => claimed?,
            };
            if claimed {
                return Ok(());
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::Cancelled, "OpenCode MCP admission cancelled")),
                () = tokio::time::sleep_until(deadline) => return Err(TurnMcpInvocationError::new(TurnMcpInvocationErrorCode::TimedOut, "OpenCode MCP confirmation timed out")),
                () = notified => {},
            }
        }
    }
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
        if self.opencode_binding.is_some() {
            self.claim_opencode(&invocation, &arguments_fingerprint, &cancellation)
                .await?;
            invocation.provider_call_id = invocation
                .native_call
                .as_ref()
                .expect("claimed OpenCode identity")
                .native_item_id
                .clone();
            return self.inner.invoke(invocation, cancellation).await;
        }
        if self.codex_binding.is_some() {
            let item = self
                .claim_codex(&invocation, &arguments_fingerprint, &cancellation)
                .await?;
            invocation.provider_call_id = item.native_item_id;
            return self.inner.invoke(invocation, cancellation).await;
        }
        Err(TurnMcpInvocationError::new(
            TurnMcpInvocationErrorCode::SessionBindingUnavailable,
            "CLI MCP provider correlation binding is unavailable",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn_mcp::invoker::TurnMcpInvocationOrigin;
    use crate::turn_mcp::result::CanonicalMcpToolResult;
    use serde_json::json;
    use std::time::Duration;

    struct UnexpectedInvoker;

    #[async_trait]
    impl TurnMcpInvoker for UnexpectedInvoker {
        async fn invoke(
            &self,
            _invocation: TurnMcpInvocation,
            _cancellation: CancellationToken,
        ) -> std::result::Result<CanonicalMcpToolResult, TurnMcpInvocationError> {
            panic!("unbound correlation must not invoke the inner MCP invoker");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn missing_provider_binding_rejects_matching_item_without_execution_or_claim() {
        let native_items = Arc::new(NativeMcpCorrelationLedger::default());
        let arguments = json!({"value": "own"});
        let fingerprint =
            crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&arguments).unwrap();
        let key = NativeMcpItemKey {
            native_thread_id: "native-thread".into(),
            native_turn_id: "native-turn".into(),
            native_item_id: "native-item".into(),
        };
        native_items
            .register(NativeMcpItemBinding {
                native_thread_id: key.native_thread_id.clone(),
                native_turn_id: key.native_turn_id.clone(),
                native_item_id: key.native_item_id.clone(),
                canonical_callable_name: "test.echo".into(),
                arguments_fingerprint: fingerprint.clone(),
            })
            .unwrap();
        let invoker = CorrelatingTurnMcpInvoker {
            inner: Arc::new(UnexpectedInvoker),
            native_items: native_items.clone(),
            opencode_binding: None,
            codex_binding: None,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        let invocation = TurnMcpInvocation {
            workspace_id: "workspace".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            runtime_id: Some("codex".into()),
            session_generation: Some(1),
            provider_call_id: "request".into(),
            native_call: Some(key.clone()),
            admission_deadline: Some(deadline),
            canonical_callable_name: "test.echo".into(),
            arguments,
            origin: TurnMcpInvocationOrigin::CliFacade,
        };
        let cancellation = CancellationToken::new();
        let error = invoker
            .invoke(invocation, cancellation.clone())
            .await
            .unwrap_err();
        assert_eq!(
            error.code,
            TurnMcpInvocationErrorCode::SessionBindingUnavailable
        );
        assert!(
            native_items
                .claim_codex_exact(
                    &key,
                    "test.echo",
                    &fingerprint,
                    "later-request",
                    &cancellation,
                    deadline
                )
                .unwrap()
        );
    }
}
