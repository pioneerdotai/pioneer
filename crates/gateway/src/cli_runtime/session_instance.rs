use crate::cli_runtime::manager::CLIAgentRuntimeSessionKey;
use anyhow::{Result, bail};
use pioneer_cli_agent_runtime::session::CLIAgentProcessGenerationAllocator;
use rand::random;

/// Identifies the Gateway boot that allocated a native CLI process generation.
///
/// Generations are monotonic only within one Gateway boot. The boot id is
/// therefore part of identities that can be persisted into filesystem paths.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CliSessionBootId(String);

impl CliSessionBootId {
    fn generate() -> Self {
        Self(format!("{:032x}", random::<u128>()))
    }

    #[cfg(test)]
    fn unmanaged_test() -> Self {
        Self("ffffffffffffffffffffffffffffffff".to_owned())
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Immutable identity of one native CLI process within a Gateway boot.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CliSessionInstanceId {
    key: CLIAgentRuntimeSessionKey,
    boot_id: CliSessionBootId,
    generation: u64,
}

impl CliSessionInstanceId {
    fn new(
        key: CLIAgentRuntimeSessionKey,
        boot_id: CliSessionBootId,
        generation: u64,
    ) -> Result<Self> {
        if generation == 0 {
            bail!("CLI session generation must be greater than zero");
        }
        Ok(Self {
            key,
            boot_id,
            generation,
        })
    }

    pub(crate) fn key(&self) -> &CLIAgentRuntimeSessionKey {
        &self.key
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn boot_id(&self) -> &CliSessionBootId {
        &self.boot_id
    }

    #[cfg(test)]
    pub(crate) fn unmanaged_for_test(
        key: CLIAgentRuntimeSessionKey,
        generation: u64,
    ) -> Result<Self> {
        Self::new(key, CliSessionBootId::unmanaged_test(), generation)
    }
}

pub(crate) trait CliSessionInstanceOrigin {
    fn to_session_instance(&self) -> CliSessionInstanceId;
}

impl CliSessionInstanceOrigin for CliSessionInstanceId {
    fn to_session_instance(&self) -> CliSessionInstanceId {
        self.clone()
    }
}

#[cfg(test)]
impl CliSessionInstanceOrigin for CLIAgentRuntimeSessionKey {
    fn to_session_instance(&self) -> CliSessionInstanceId {
        CliSessionInstanceId::new(self.clone(), CliSessionBootId::unmanaged_test(), u64::MAX)
            .expect("test-only unmanaged CLI session instance should be valid")
    }
}

#[derive(Debug)]
pub(crate) struct CliSessionGenerationAllocator {
    inner: CLIAgentProcessGenerationAllocator,
    boot_id: CliSessionBootId,
}

impl Default for CliSessionGenerationAllocator {
    fn default() -> Self {
        Self {
            inner: CLIAgentProcessGenerationAllocator::default(),
            boot_id: CliSessionBootId::generate(),
        }
    }
}

impl CliSessionGenerationAllocator {
    pub(crate) fn allocate(&self, key: CLIAgentRuntimeSessionKey) -> Result<CliSessionInstanceId> {
        let generation = self
            .inner
            .allocate()
            .ok_or_else(|| anyhow::anyhow!("CLI session generation space is exhausted"))?
            .get();
        CliSessionInstanceId::new(key, self.boot_id.clone(), generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(thread_id: &str) -> CLIAgentRuntimeSessionKey {
        CLIAgentRuntimeSessionKey::new("workspace", "codex", thread_id).unwrap()
    }

    #[test]
    fn cli_runtime_session_generation_is_monotonic_across_logical_keys() {
        let allocator = CliSessionGenerationAllocator::default();
        let first = allocator.allocate(key("one")).unwrap();
        let second = allocator.allocate(key("two")).unwrap();
        let third = allocator.allocate(key("one")).unwrap();
        assert_eq!(first.generation(), 1);
        assert_eq!(second.generation(), 2);
        assert_eq!(third.generation(), 3);
        assert_ne!(first, third);
        assert_eq!(first.boot_id(), second.boot_id());
        assert_eq!(second.boot_id(), third.boot_id());
    }

    #[test]
    fn cli_runtime_session_boot_ids_are_unique_between_allocators() {
        let first = CliSessionGenerationAllocator::default()
            .allocate(key("one"))
            .unwrap();
        let second = CliSessionGenerationAllocator::default()
            .allocate(key("one"))
            .unwrap();
        assert_ne!(first.boot_id(), second.boot_id());
        assert_ne!(first, second);
    }
}
