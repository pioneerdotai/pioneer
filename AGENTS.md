# AGENTS.md

## Repository instructions

Follow the nearest nested `AGENTS.md` for crate-specific rules. The release-gate
rules below apply repository-wide and cannot be weakened by a nested file.

## Native-agent release gate

Gateway and Desktop releases are protected by the versioned native-agent
lifecycle contract in `ci/native-agent-gate.json`. Read `ci/README.md` before
changing that manifest, the CI/release workflows, or any covered lifecycle.

Treat the gate as part of the product contract, not as a one-time Proposal 62
artifact. Reassess and, when needed, extend the gate whenever a change adds,
renames, removes, or materially changes any of the following:

- native agent/provider/tool execution or provider conformance;
- durable events, cancellation, recovery, restart, scheduling, or bounded
  long-running behavior;
- database migrations or previous-release restart compatibility;
- attachment/path/process security behavior;
- production feature profiles or macOS, Linux, or Windows support;
- CI, release, build, package, publish, or attestation workflows.

For a relevant change:

1. Update the manifest when workspace coverage, feature profiles, required
   categories, exact Rust test identities, platforms, or justified ignores
   change. Keep one default-workspace case and one explicit production
   `computer-use` case per supported OS. New critical behavior needs an exact
   required test identity; broad substring coverage is not acceptable.
2. Keep enumerated and actually executed tests distinct. An ignored test must
   never satisfy required coverage or executed-test counts. Duplicate required
   or ignored identities inside a workspace case are ambiguous and must fail
   closed.
3. Prefer deterministic automated coverage. If an external/manual smoke must be
   ignored, allowlist its exact suite/case/test identity with a concrete reason
   and retain deterministic coverage for the same contract. Remove stale
   allowances.
4. Update `scripts/ci/validate_native_agent_workflows.py` and its regression
   tests when the platform/case/feature matrix, workflow wiring, report schema,
   or publisher dependencies change.
5. Do not bypass `.github/workflows/native-agent-gate.yml`. Release build and
   package jobs must remain gate-dependent, and publisher jobs must remain
   dependent on their gated build/package jobs.
6. Do not call a release ready from a local platform-only result. The final
   release commit requires clean exact-SHA hosted evidence for Linux, macOS, and
   Windows.

Run the contract checks after every relevant change:

```bash
python3 scripts/ci/native_agent_gate.py validate-manifest --manifest ci/native-agent-gate.json
python3 scripts/ci/validate_native_agent_workflows.py
python3 -m unittest discover -s scripts/ci/tests -p 'test_*.py' -v
cargo fmt --all -- --check
```

Also run the affected locked Cargo tests. When producing release evidence, use
the runner described in `ci/README.md` against the exact clean commit; do not
reuse an attestation from another SHA or from a dirty tree.
