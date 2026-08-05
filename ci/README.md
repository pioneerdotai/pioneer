# Native-agent lifecycle release gate

`native-agent-gate.json` is the versioned execution contract for the native
provider-based agent. Every required suite is enumerated before execution and
produces a JSON report containing the exact commit, clean-tree state, platform,
fault seed, commands, feature profile, discovered and actually executed test
names, ignored-test decisions, durations, outcomes and output digests.

The reusable workflow `.github/workflows/native-agent-gate.yml` runs the full
Linux lifecycle set plus macOS and Windows runtime subsets. Its final job merges
the reports and emits an exact-SHA attestation. CI, Gateway release and Desktop
release call that workflow directly; release build jobs cannot start unless the
attestation job succeeds.

The attestation records exact source SHA and cleanliness, deterministic fault
and property-test seeds, enumerated versus executed counts, the feature matrix,
explicitly allowlisted ignored tests, per-category test counts, case outcomes,
durations, SHA-256 output hashes and the exact required-test identity map.
Required categories are anchored to exact Rust test identities rather than
substring patterns, and every identity listed for a category must be present in
the executed set. Both virtual long-running
progress and persisted bounded no-progress are mandatory categories, so a
release cannot silently drop either side of that contract.

Ignored Rust tests are excluded from executed counts and category coverage.
They are fail-closed: a necessary ignore must be added to
`allowed_ignored_tests` with an exact suite/case/test identity and a non-empty
reason. A stale allowance also fails, preventing an obsolete exception from
silently remaining in the gate.

The case/package matrix is independently pinned by
`validate_native_agent_workflows.py`, including the `computer-use` production
feature profile on Linux, macOS and Windows. Removing a crate, platform,
feature case, required category or exact regression identity fails the contract
tests even if the manifest is edited at the same time. Release publisher jobs
are also checked to depend on their gated build/package jobs using active YAML
lines; commented-out wiring cannot satisfy the validator.

The default fault seed is the exact commit SHA. Native fault tests receive it as
`PIONEER_FAULT_SEED`; property-test frameworks receive a deterministic numeric
derivative through `PROPTEST_RNG_SEED` and `QUICKCHECK_SEED`. Reports preserve
both values so a failed fault schedule can be replayed.

Local contract-only verification (no Cargo invocation):

```text
python scripts/ci/native_agent_gate.py validate-manifest --manifest ci/native-agent-gate.json
python scripts/ci/validate_native_agent_workflows.py
python -m unittest discover -s scripts/ci/tests -p 'test_*.py' -v
```

The Cargo suites are intentionally run only by the track/release gate, not as a
substitute for completing an implementation unit.
