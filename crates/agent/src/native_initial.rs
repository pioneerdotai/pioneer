//! Owned initial preparation state for one semantic native Turn.
//! Runtime handles stay in the process; only committed concrete outputs persist.
use crate::ToolLoopConfig;
use crate::hooks::{
    EffectiveTurnPolicySet, EffectiveTurnPromptContextSet, EffectiveTurnPromptSectionSet,
};
use anyhow::{Result, ensure};
use chrono::{DateTime, FixedOffset, Local};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcceptedNativeInitialOutputs {
    pub(crate) policy: EffectiveTurnPolicySet,
    pub(crate) prompt_context: EffectiveTurnPromptContextSet,
    pub(crate) prompt_sections: EffectiveTurnPromptSectionSet,
    pub(crate) instructions: pioneer_promt::CompiledInstructionDeliveryPlan,
    pub(crate) capability_diagnostics: Vec<pioneer_protocol::PromptManifestDiagnostic>,
    pub(crate) skill_plan: pioneer_skills::SkillRuntimePlan,
    pub(crate) tool_metadata: pioneer_hooks::HookToolBundleSet,
    pub(crate) extension_specs: Vec<Vec<pioneer_tools::ConfiguredToolSpec>>,
    pub(crate) provider_tool_calling: bool,
    pub(crate) initiating_thread_id: Option<String>,
    pub(crate) visible_tools: Vec<String>,
    pub(crate) core_tools: Vec<String>,
    pub(crate) selected_optional_tools: Vec<String>,
    pub(crate) requested_tools: Vec<String>,
    pub(crate) preflight_usage: crate::chat::preflight::PreflightBudgetUsage,
    pub(crate) config: ToolLoopConfig,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum InitialPhase {
    NotStarted,
    Preparing,
    Completed {
        outputs: Box<AcceptedNativeInitialOutputs>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InitialPhaseSnapshot {
    pub(crate) schema_version: u32,
    pub(crate) workspace_id: String,
    pub(crate) thread_id: String,
    pub(crate) turn_id: String,
    pub(crate) admission_clock: DateTime<FixedOffset>,
    pub(crate) state: InitialPhase,
}

/// The same owner survives warm windows. Cold restoration has no handler cache;
/// concrete tool owners must rebind the saved tool identities before dispatch.
#[derive(Clone)]
pub struct NativeInitialPreparation {
    execution_owner_generation: Option<u64>,
    pub(crate) inner: Arc<tokio::sync::Mutex<InitialPreparationState>>,
}

pub(crate) struct InitialPreparationState {
    pub(crate) snapshot: InitialPhaseSnapshot,
    pub(crate) encoded: String,
    pub(crate) byte_budget: usize,
    pub(crate) warm_tools: Option<crate::chat::WarmNativeInitialTools>,
}

impl std::fmt::Debug for NativeInitialPreparation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeInitialPreparation")
            .finish_non_exhaustive()
    }
}

impl NativeInitialPreparation {
    /// Called once by the immutable admission producer, before actor dispatch.
    pub fn new_admission_json(
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        byte_budget: usize,
    ) -> Result<String> {
        let scope_bytes = workspace_id
            .len()
            .checked_add(thread_id.len())
            .and_then(|n| n.checked_add(turn_id.len()));
        ensure!(
            scope_bytes.is_some_and(
                |n| n <= byte_budget.min(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES)
            ),
            "initial scope exceeds execution allowance"
        );
        encode(
            &InitialPhaseSnapshot {
                schema_version: 1,
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                admission_clock: Local::now().fixed_offset(),
                state: InitialPhase::NotStarted,
            },
            byte_budget,
        )
    }

    pub fn restore(
        encoded: &str,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        byte_budget: usize,
    ) -> Result<Self> {
        let snapshot = decode(encoded, byte_budget)?;
        ensure!(
            snapshot.workspace_id == workspace_id
                && snapshot.thread_id == thread_id
                && snapshot.turn_id == turn_id,
            "initial preparation scope differs"
        );
        Ok(Self {
            execution_owner_generation: None,
            inner: Arc::new(tokio::sync::Mutex::new(InitialPreparationState {
                snapshot,
                encoded: encoded.to_owned(),
                byte_budget,
                warm_tools: None,
            })),
        })
    }

    /// The dispatch owner captures the mutable execution epoch; it is never
    /// frozen in the original semantic outputs or adopted from a later event.
    pub fn with_execution_owner_generation(mut self, generation: u64) -> Self {
        self.execution_owner_generation = Some(generation);
        self
    }

    pub(crate) fn execution_owner_generation(&self) -> Option<u64> {
        self.execution_owner_generation
    }

    pub(crate) async fn frozen_config(&self) -> Option<ToolLoopConfig> {
        match &self.inner.lock().await.snapshot.state {
            InitialPhase::Completed { outputs } => Some(outputs.config.clone()),
            _ => None,
        }
    }

    pub fn validate_for_resume(encoded: &str, byte_budget: usize) -> Result<()> {
        let snapshot = decode(encoded, byte_budget)?;
        ensure!(
            !matches!(snapshot.state, InitialPhase::Preparing),
            "initial preparation outcome is unknown and cannot be rerun"
        );
        Ok(())
    }

    pub fn validate_transition(previous: &str, next: &str, byte_budget: usize) -> Result<()> {
        let previous = decode(previous, byte_budget)?;
        let next = decode(next, byte_budget)?;
        ensure!(
            previous.admission_clock == next.admission_clock
                && previous.workspace_id == next.workspace_id
                && previous.thread_id == next.thread_id
                && previous.turn_id == next.turn_id,
            "initial preparation clock changed"
        );
        ensure!(
            matches!(
                (&previous.state, &next.state),
                (InitialPhase::NotStarted, InitialPhase::Preparing)
                    | (InitialPhase::Preparing, InitialPhase::Completed { .. })
            ),
            "initial preparation transition is invalid"
        );
        Ok(())
    }
}

pub(crate) fn digest(encoded: &str) -> String {
    hex::encode(Sha256::digest(encoded.as_bytes()))
}

pub(crate) fn encode(snapshot: &InitialPhaseSnapshot, byte_budget: usize) -> Result<String> {
    bounded_json(snapshot, byte_budget)
}

/// Measure the borrowed catalog before retaining a second owned copy. The
/// existing native wire ceiling and shared envelope apply to real JSON bytes.
pub(crate) fn checked_extension_specs(
    bundles: &[pioneer_tools::ToolExtensionBundle],
) -> Result<Vec<Vec<pioneer_tools::ConfiguredToolSpec>>> {
    ensure!(
        bundles.len() <= 256,
        "initial tool bundle count exceeds native limit"
    );
    let count = bundles
        .iter()
        .try_fold(0usize, |n, b| n.checked_add(b.specs.len()));
    ensure!(
        count.is_some_and(|n| n <= 256),
        "initial tool count exceeds native limit"
    );
    let borrowed = bundles
        .iter()
        .map(|b| b.specs.as_slice())
        .collect::<Vec<_>>();
    bounded_json(
        &borrowed,
        pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES,
    )?;
    Ok(bundles.iter().map(|b| b.specs.clone()).collect())
}

fn bounded_json<T: Serialize + ?Sized>(snapshot: &T, byte_budget: usize) -> Result<String> {
    struct Writer {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl std::io::Write for Writer {
        fn write(&mut self, value: &[u8]) -> std::io::Result<usize> {
            let wanted = self
                .bytes
                .len()
                .checked_add(value.len())
                .filter(|size| *size <= self.limit)
                .ok_or_else(|| std::io::Error::other("initial preparation byte limit"))?;
            if wanted > self.bytes.capacity() {
                self.bytes.reserve_exact(wanted - self.bytes.len());
            }
            self.bytes.extend_from_slice(value);
            Ok(value.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        limit: byte_budget.min(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES),
    };
    serde_json::to_writer(&mut writer, snapshot)
        .map_err(|_| anyhow::anyhow!("initial preparation encoding unavailable or oversized"))?;
    let encoded = String::from_utf8(writer.bytes)
        .map_err(|_| anyhow::anyhow!("initial preparation encoding is invalid"))?;
    validate_shape(&encoded)?;
    Ok(encoded)
}

fn decode(encoded: &str, byte_budget: usize) -> Result<InitialPhaseSnapshot> {
    ensure!(
        encoded.len() <= byte_budget.min(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES),
        "initial preparation exceeds accepted execution byte limit"
    );
    validate_shape(encoded)?;
    let snapshot: InitialPhaseSnapshot = serde_json::from_str(encoded)
        .map_err(|_| anyhow::anyhow!("initial preparation is incompatible"))?;
    ensure!(
        snapshot.schema_version == 1,
        "initial preparation version unavailable"
    );
    if let InitialPhase::Completed { outputs } = &snapshot.state {
        pioneer_promt::validate_native_instruction_plan(&outputs.instructions)
            .map_err(|_| anyhow::anyhow!("initial instruction plan unavailable"))?;
        ensure!(
            outputs.extension_specs.len() <= 256
                && outputs.tool_metadata.entries.len() == outputs.extension_specs.len(),
            "initial tool bundle identities unavailable"
        );
        ensure!(
            outputs.provider_tool_calling || outputs.extension_specs.is_empty(),
            "accepted tools require native tool calling"
        );
        ensure!(
            [
                &outputs.visible_tools,
                &outputs.core_tools,
                &outputs.selected_optional_tools,
                &outputs.requested_tools
            ]
            .iter()
            .all(|names| names.len() <= 256),
            "initial visibility exceeds native tool limit"
        );
        let count = outputs
            .extension_specs
            .iter()
            .try_fold(0usize, |n, tools| n.checked_add(tools.len()));
        ensure!(
            count.is_some_and(|n| n <= 256),
            "initial tool count exceeds native wire limit"
        );
        for (entry, specs) in outputs
            .tool_metadata
            .entries
            .iter()
            .zip(&outputs.extension_specs)
        {
            let expected = entry
                .tool_names
                .iter()
                .map(|n| n.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            let actual = specs
                .iter()
                .map(|s| s.spec.name.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            ensure!(expected == actual, "initial tool bundle catalog differs");
        }
    }
    Ok(snapshot)
}

fn validate_shape(encoded: &str) -> Result<()> {
    use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
    struct Seed<'a> {
        nodes: &'a mut usize,
        depth: usize,
    }
    impl<'de> DeserializeSeed<'de> for Seed<'_> {
        type Value = ();
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            d: D,
        ) -> std::result::Result<(), D::Error> {
            *self.nodes += 1;
            // Up to256 rich tool specs plus hook manifests and skill plans.
            // Count keys as nodes; permit their existing depth32 schema inside
            // the concrete phase/spec envelopes, with a finite depth64 ceiling.
            if *self.nodes > 32768 || self.depth > 64 {
                return Err(serde::de::Error::custom("initial preparation shape limit"));
            }
            d.deserialize_any(self)
        }
    }
    impl<'de> Visitor<'de> for Seed<'_> {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("bounded initial preparation")
        }
        fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_bool<E: serde::de::Error>(self, _: bool) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_str<E: serde::de::Error>(self, _: &str) -> std::result::Result<(), E> {
            Ok(())
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> std::result::Result<(), M::Error> {
            while map
                .next_key_seed(Seed {
                    nodes: self.nodes,
                    depth: self.depth + 1,
                })?
                .is_some()
            {
                map.next_value_seed(Seed {
                    nodes: self.nodes,
                    depth: self.depth + 1,
                })?;
            }
            Ok(())
        }
        fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> std::result::Result<(), S::Error> {
            while seq
                .next_element_seed(Seed {
                    nodes: self.nodes,
                    depth: self.depth + 1,
                })?
                .is_some()
            {}
            Ok(())
        }
    }
    let mut nodes = 0;
    let mut decoder = serde_json::Deserializer::from_str(encoded);
    Seed {
        nodes: &mut nodes,
        depth: 0,
    }
    .deserialize(&mut decoder)
    .map_err(|_| anyhow::anyhow!("initial preparation shape unavailable"))?;
    decoder
        .end()
        .map_err(|_| anyhow::anyhow!("initial preparation trailing data"))
}

#[cfg(test)]
mod tests {
    use super::*;
    const LIMIT: usize = pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES;

    fn admitted() -> InitialPhaseSnapshot {
        decode(
            &NativeInitialPreparation::new_admission_json("workspace", "thread", "turn", LIMIT)
                .unwrap(),
            LIMIT,
        )
        .unwrap()
    }

    fn outputs() -> AcceptedNativeInitialOutputs {
        let root = tempfile::tempdir().unwrap();
        let bundle = pioneer_promt::compile_prompt(pioneer_promt::PromptCompileInput {
            workspace_root: root.path().to_owned(),
            profile: pioneer_promt::PromptProfile::AssistantFull,
            skills_prompt: Some("accepted skill bytes".into()),
            retry_instruction: None,
            include_tool_recovery_policy: true,
            include_task_orchestration_policy: false,
            continue_generation_hint: false,
            runtime_sections: Vec::new(),
            dynamic_sections: Vec::new(),
            dynamic_context: None,
            extra_system: Some("Current date/time: original admission clock".into()),
            limits: pioneer_promt::PromptLimits::default(),
        })
        .unwrap();
        AcceptedNativeInitialOutputs {
            policy: Default::default(),
            prompt_context: Default::default(),
            prompt_sections: Default::default(),
            instructions: pioneer_promt::compile_instruction_delivery_plan(bundle).unwrap(),
            capability_diagnostics: Vec::new(),
            skill_plan: pioneer_skills::SkillRuntimePlan {
                tools: Vec::new(),
                read_skill_index: Default::default(),
                excluded_tools: Vec::new(),
            },
            tool_metadata: Default::default(),
            extension_specs: Vec::new(),
            provider_tool_calling: false,
            initiating_thread_id: Some("parent".into()),
            visible_tools: Vec::new(),
            core_tools: Vec::new(),
            selected_optional_tools: Vec::new(),
            requested_tools: Vec::new(),
            preflight_usage: crate::chat::preflight::PreflightBudgetUsage {
                attempts: 2,
                known_tokens: 37,
                unknown: true,
            },
            config: crate::manager_tests::test_tool_loop_config(),
        }
    }

    #[test]
    fn phase_transition_keeps_scope_and_clock_and_rejects_ambiguous_rerun() {
        let snapshot = admitted();
        let original = encode(&snapshot, LIMIT).unwrap();
        let mut preparing = snapshot.clone();
        preparing.state = InitialPhase::Preparing;
        let encoded = encode(&preparing, LIMIT).unwrap();
        NativeInitialPreparation::validate_transition(&original, &encoded, LIMIT).unwrap();
        assert!(NativeInitialPreparation::validate_for_resume(&encoded, LIMIT).is_err());
        assert!(NativeInitialPreparation::validate_transition(&encoded, &original, LIMIT).is_err());
        assert!(
            NativeInitialPreparation::restore(&original, "other", "thread", "turn", LIMIT).is_err()
        );
        preparing.admission_clock += chrono::Duration::seconds(1);
        assert!(
            NativeInitialPreparation::validate_transition(
                &original,
                &encode(&preparing, LIMIT).unwrap(),
                LIMIT
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn completed_outputs_restore_original_prompt_config_and_usage_without_runtime_handles() {
        let mut preparing = admitted();
        preparing.state = InitialPhase::Preparing;
        let previous = encode(&preparing, LIMIT).unwrap();
        let mut completed = preparing.clone();
        let mut owned = outputs();
        owned
            .config
            .execution_windows
            .window
            .max_agent_rounds_per_window = 7;
        let expected_plan = serde_json::to_value(&owned.instructions).unwrap();
        completed.state = InitialPhase::Completed {
            outputs: Box::new(owned),
        };
        let encoded = encode(&completed, LIMIT).unwrap();
        NativeInitialPreparation::validate_transition(&previous, &encoded, LIMIT).unwrap();
        let restored =
            NativeInitialPreparation::restore(&encoded, "workspace", "thread", "turn", LIMIT)
                .unwrap();
        assert_eq!(
            restored
                .frozen_config()
                .await
                .unwrap()
                .execution_windows
                .window
                .max_agent_rounds_per_window,
            7
        );
        let state = restored.inner.lock().await;
        assert!(state.warm_tools.is_none());
        assert_eq!(state.snapshot.admission_clock, preparing.admission_clock);
        let InitialPhase::Completed { outputs } = &state.snapshot.state else {
            panic!("completed outputs lost")
        };
        assert_eq!(
            serde_json::to_value(&outputs.instructions).unwrap(),
            expected_plan
        );
        assert_eq!(outputs.preflight_usage.attempts, 2);
        assert_eq!(outputs.preflight_usage.known_tokens, 37);
        assert!(outputs.preflight_usage.unknown);
        assert!(NativeInitialPreparation::validate_transition(&encoded, &previous, LIMIT).is_err());
    }

    #[test]
    fn future_or_tampered_phase_is_unavailable_and_errors_do_not_echo_content() {
        let mut completed = admitted();
        completed.state = InitialPhase::Completed {
            outputs: Box::new(outputs()),
        };
        let mut value = serde_json::to_value(&completed).unwrap();
        value["state"]["outputs"]["config"]["unknown_contract"] = "PRIVATE_SENTINEL".into();
        let error = decode(&value.to_string(), LIMIT).err().unwrap();
        assert!(!format!("{error:#}").contains("PRIVATE_SENTINEL"));
        completed.schema_version = 2;
        assert!(decode(&encode(&completed, LIMIT).unwrap(), LIMIT).is_err());
        completed.schema_version = 1;
        if let InitialPhase::Completed { outputs } = &mut completed.state {
            outputs
                .instructions
                .bundle
                .full_system_text
                .push_str("tamper");
        }
        assert!(decode(&encode(&completed, LIMIT).unwrap(), LIMIT).is_err());
    }

    #[test]
    fn initial_encoding_obeys_actual_residual_bytes_and_bounded_shape_before_decode() {
        let snapshot = admitted();
        let encoded = encode(&snapshot, LIMIT).unwrap();
        assert!(encode(&snapshot, encoded.len() - 1).is_err());
        assert!(decode(&encoded, encoded.len() - 1).is_err());
        assert!(
            NativeInitialPreparation::new_admission_json(&"x".repeat(64), "thread", "turn", 32)
                .is_err()
        );
        let deep = format!("{}0{}", "[".repeat(65), "]".repeat(65));
        assert!(
            decode(&deep, LIMIT)
                .err()
                .unwrap()
                .to_string()
                .contains("shape")
        );
        let wide = format!("[{}]", vec!["0"; 32769].join(","));
        assert!(
            decode(&wide, LIMIT)
                .err()
                .unwrap()
                .to_string()
                .contains("shape")
        );
    }
}

#[cfg(test)]
mod borrowed_catalog_bounds_tests {
    #[test]
    fn initial_catalog_rejects_expanded_json_before_owned_spec_copy() {
        use pioneer_tools::{
            ConfiguredToolSpec, ExecutionClass, PayloadKind, ToolExtensionBundle, ToolSpec,
        };
        let bundle = ToolExtensionBundle {
            specs: vec![ConfiguredToolSpec::new(
                ToolSpec::new(
                    "bounded",
                    "x".repeat(pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES),
                    serde_json::json!({"type":"object"}),
                    PayloadKind::Function,
                ),
                ExecutionClass::Shared,
                pioneer_tools::dynamic_unknown_output_policy(),
            )],
            ..Default::default()
        };
        assert!(super::checked_extension_specs(&[bundle]).is_err());
    }
}
