# CI production line coverage

CI measures **Linux production line coverage, excluding doctests**. The four existing
Linux nextest shards each execute once under cargo-llvm-cov. Shard 1 also runs fmt
and ordinary, uninstrumented doctests. Windows and macOS checks are unchanged.

Open the GitHub Actions run Summary for the combined result and crate breakdown.
Download `linux-production-coverage-<run_id>-<attempt>` from that run's Artifacts
section and open `html/index.html`. Retention is 90 days. A usable baseline requires
all four Linux jobs, the coverage artifact upload, and the final Linux check to
succeed. Failed/cancelled runs do not publish a baseline. An artifact that exists
without a successful final Linux check must not be used as a baseline.

The artifact contains:

- `production.lcov`: repository-relative `SF`, `DA`, `LF`, and `LH` records;
- `html/`: source lines with hit counts, red uncovered lines and green covered lines;
- `summary.json`: covered/measurable lines and percentages overall and by crate,
  test count, checkout SHA and completeness; a crate with no measurable lines has
  `percent: null`, never 100%;
- `metadata.json`: the common measurement settings and each shard's identity,
  required check outcomes, tool versions, report checksums and test count;
- `policy.json`, `exclusions.json`, `source-hashes.json`: the exact exclusion policy,
  applied source ranges and hashes of the tracked workspace Rust ownership inventory;
- `shards/`: original normalized LCOV, JUnit and metadata from all four shards.

Metadata schema 1 distinguishes the checkout SHA from PR head/base SHAs. For a
`pull_request` event, checkout is GitHub's synthetic merge commit. The identity
also includes repository, event, run ID and attempt. Exact artifact names prevent
mixing a previous attempt with a retry. Rerun all jobs to obtain a new complete
baseline; rerunning only failed jobs does not supply four reports for the new attempt. Instrumented dependencies use a separate
cache and target directory; cargo-llvm-cov cleans workspace artifacts and profiles
before every shard. Doctests use the ordinary target directory and do not inherit
instrumentation flags.

## Exclusion policy v1

The denominator is the union of LLVM-measurable production lines, not the number
of textual Rust lines and not the average of shard percentages. Hit counts are
summed for each `(repository-relative path, line)`; any positive sum means covered.
Zero-hit lines remain in the denominator even when no shard executes them.
Platform-disabled or never-instantiated code for which LLVM emits no line record
is outside this measurement. Each crate appears in the breakdown, including crates
with no measurable production lines.

The fixed policy in `policy.json` measures only tracked Rust files under workspace
members' `src/` directories. Source ownership is a separate inventory: tracked Rust
files anywhere inside those members, including explicitly declared Cargo target
roots outside `src/`. An explicit `[lib].path` replaces the default library root.
The walker follows external modules and literal includes from real Cargo roots,
including `#[path = "src/lib.rs"]` from `crates/entity/lib.rs`; children of a
`#[path]` module are resolved beside the referenced file. These ownership-only
files do not enter LCOV or the denominator. Metadata records `source_roots` and
`measurement_paths` separately, and source hashes cover the ownership inventory.
Build scripts, integration-test targets outside `src/`,
external dependencies and generated target output are outside the production source
scope. It excludes directory components and test/fixture filenames listed in that
file, plus files whose first nonblank line is a `//` comment with `@generated`.
Tests are still executed; none of these exclusions are nextest filters.

Tree-sitter Rust supplies syntax boundaries for inline modules, test functions,
attributes, strings, comments and macro token trees. `cfg(test)` and
`feature = "test-support"` are false for classification. Linux platform predicates
come from `rustc --print cfg --target x86_64-unknown-linux-gnu` during CI preparation;
actual per-package feature sets come from Cargo `compiler-artifact` JSON messages
forwarded by the existing nextest invocation, including fresh/cached artifacts.
No extra test run or compilation is introduced to obtain these facts. Build and
test diagnostics remain on stderr; stdout captures Cargo JSON. Packing requires a
successful `build-finished` message and feature facts for every workspace package.

Metadata `configuration` stores normalized `target_cfg` and `package_features`;
all four shards must agree. Each actual feature variant is classified separately,
so production availability in any variant retains a line, without inventing a
combined feature configuration. For example, `cfg(any(feature = "schema", test))`
is production when schema is enabled and test-only otherwise;
`cfg(any(not(unix), test))` is test-only on the measured Linux target.
`cfg(not(test))` stays production. Unknown custom cfgs remain conservative, and an
unresolved test-sensitive predicate is an explicit error. Only `cfg`, `cfg_attr`,
`path` and test attributes affect classification; ordinary attribute arguments
such as `schemars(length(min = 1, max = 128))` remain opaque, including inside nested
`cfg_attr`. Disabled cfg branches are excluded at report time, without filtering
test execution.
Literal external modules (`#[path]` or normal module paths) and literal `include!`
inherit test scope, including files with unconventional names. A file also reached
from production remains production, subject to its own inline exclusions.
Each inclusion retains its own child directory and inherited test scope. In
particular, normal `mod linux;` and `#[path = "linux.rs"] mod linux_cross_platform;`
can load the same file without sharing descendant-resolution state. Both graphs
are traversed, including excluded connections; a child reached only through a
test/disabled connection stays excluded. Literal `include!` resolves child modules
beside the included file. Ownership-only Cargo roots and the measured `src/` scope
remain separate.

This is a report filter, not a Rust expander. Parser errors, unresolved measured
source ownership, mixed test/production statements on one line, unresolved conditional
module paths and test-generating macro definitions outside test scopes fail the
coverage job instead of publishing an uncertain percentage. Dynamic `include!` is
supported only as `concat!(env!("OUT_DIR"), "/literal.rs")` for generated content,
which is outside source scope.
Production macro token trees may contain ordinary feature/platform cfgs, whether
those predicates are enabled or disabled. The macro guard separately checks test
attributes and test-sensitive cfgs; `cfg(not(test))` stays production. It also
inspects nested `cfg_attr` payloads for test generators even when a producer-side
predicate is false, since macro attributes expand at the call site. Test-generating
macros require an enclosing test scope, propagated through modules and includes.
No source files are rewritten, and no coverage attributes are added.

The policy fingerprint hashes the policy, both processing scripts, pinned parser
requirements, nextest configuration and the workflow. Metadata also records manifests, lockfile,
Cargo configuration and source inventory hashes. For a later local audit, use the
exact checkout SHA, tool versions, build settings, source hashes and policy from the
artifact. This change does not add a local audit runner or baseline comparison.

## Infrastructure maintenance

Dependencies are wheel-only installs from `requirements.txt`. The report uses only
line records; it makes no branch/function-coverage claim. JUnit from the existing
nextest run supplies counts and detects repeated tests across shards.

The regression tests in `test_coverage.py` cover union semantics, measured zero,
missing/corrupt/incompatible shards, repeated tests and Rust exclusion boundaries.
They also cover nonstandard Cargo roots and `#[path]` children without expanding the
measurement scope, opaque numeric schemars attributes, nested `cfg_attr`, mixed
feature/test and platform/test predicates, `cfg(not(test))`, multiple actual feature
variants, excluded external modules with literal includes, and missing or
incompatible configuration facts. Additional cases cover service-style normal and
`#[path]` connections in both declaration orders, distinct production descendants
for both inclusion directories, literal includes carrying test macros, ordinary
production macros with features on/off, and rejection of test generators inside
macro definitions/invocations and nested `cfg_attr`.
After external acceptance and explicit permission to execute checks, they can be run
with `python -m unittest discover -s scripts/coverage -p 'test_coverage.py'` in an
environment with the pinned parser wheels installed. They have not been executed
during implementation.

References: [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov),
[nextest coverage](https://nexte.st/docs/integrations/test-coverage/),
[nextest JUnit](https://nexte.st/docs/machine-readable/junit/),
[nextest Cargo messages](https://nexte.st/docs/running/#cargo-build-options),
[Cargo JSON messages](https://doc.rust-lang.org/cargo/reference/external-tools.html#json-messages),
[Rust cfg](https://doc.rust-lang.org/reference/conditional-compilation.html),
[Rust module paths](https://doc.rust-lang.org/reference/items/modules.html#the-path-attribute).
