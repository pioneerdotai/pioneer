# Exact release symbols and source lookup

Read before interpreting raw profile addresses. Symbols map machine addresses to
functions and source lines; they are build artifacts for agents/developers, not
files the user must generate or manage every time the app starts. They are not
needed to read process CPU/RSS, live work or queue snapshots.

## Identity comes before filenames

Use the capture's `module.format`, `module.module_id`, `module.binary_sha256`,
its target build scope and selected PID/start/run identity. The formats are:

| OS | Native identity | Artifact |
| --- | --- | --- |
| macOS | Mach-O UUID | `.dSYM` bundle |
| Linux | ELF GNU build ID | separate `.debug` file |
| Windows | PE CodeView GUID plus age | matching `.pdb` |

A source commit, semver, CPU architecture or build-manifest ID is not a substitute
for this post-link identity. Signing/packaging may change the binary checksum while
preserving the module ID. The symbol manifest explicitly retains allowed packaged
hashes. Rebuilding the same source can produce another native identity.

Ordinary `snapshot.module` can be null with `post_link_id_unavailable` module status; identity
is intentionally not scanned and hashed on each read. A native capture records
its actual executable identity when possible. Do not invent UUIDs from a build
manifest or assert a child's identity from its parent's build.

## Locate the producing release and symbol run

Production repository: `pioneerdotai/pioneer` on GitHub. Use existing authorized
GitHub CLI/API access or the connected GitHub tool. Private artifact access may
require signing in with an account allowed to read repository Actions. Do not ask
for tokens to be pasted into chat or send the bundle to a public symbol server.

The independent **Build Debug Symbols** workflow
(`.github/workflows/build-debug-symbols.yml`) prepares symbols and source bundles
and uploads them to the configured Sentry account. **Release** queues it once
the exact inputs exist, alongside publication. The release does not wait for
symbol production, and a published installer does not prove symbols succeeded.
Symbol jobs consume original release inputs instead of compiling another
application binary. Missing Sentry credentials skip Sentry upload; private
GitHub symbol artifacts are still retained.

Identify the exact Release **run ID and commit** (or Release Gateway / Release App
for older releases), target
triple, and Gateway feature variant (`default` or `computer-use`). Use installed
build provenance, the release tag, module ID and manifest hashes to disambiguate.
Do not just download the newest successful run. The same commit may have multiple
release/symbol attempts or different package variants.

Example read-only discovery:

```bash
diag_repo='pioneerdotai/pioneer'
gh run list --repo "$diag_repo" --workflow build-debug-symbols.yml --limit 20 \
  --json databaseId,displayTitle,headSha,status,conclusion,url
```

The run title contains the source release run ID. Verify that ID and the source
commit in the retained provenance before using symbols. A manually dispatched
repair may use a newer workflow revision; its workflow `headSha` need not equal
the producer commit. Symbol jobs still check out the exact producer commit.

If the release did not retain symbol inputs or they have expired, report that
limit. A missing standalone workflow on the producer revision does not prevent
using a newer workflow ref with available original inputs. The skill does not
itself upgrade the installation.
Some older releases have symbol jobs and artifacts inside **Release** itself;
inspect that release revision's workflow and verify the original producer ID.

## Download only the matching platform/variant

Choose the symbol workflow run for that exact producer and inspect its artifacts
before downloading (use the Release run ID for versions with inline symbol jobs):

```bash
diag_symbols_run='EXACT_SYMBOL_WORKFLOW_RUN_ID'
gh api --paginate "repos/$diag_repo/actions/runs/$diag_symbols_run/artifacts" \
  --jq '.artifacts[] | {id, name, size_in_bytes, expired}'
```

Output names are derived from the input artifact names:

- Gateway: `debug-symbol-inputs-gateway-TARGET-default` or
  `debug-symbol-inputs-gateway-TARGET-computer-use`.
- Desktop: `debug-symbol-inputs-desktop-TARGET`. This artifact includes the
  desktop executable, updater and bundled Gateway symbol inventories. Select
  the inventory for the actual process being analyzed.
- Targets: `x86_64-apple-darwin`, `aarch64-apple-darwin`,
  `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
  `x86_64-pc-windows-msvc`, `aarch64-pc-windows-msvc`.

A dSYM for ARM64 does not describe x86_64/Rosetta execution. Use actual module and
target identity rather than just the host's architecture.

Check free space for both download and expansion and keep 5 GiB free throughout.
Compressed ZIP size is not expanded disk cost. Only download the selected artifact
to a new private case subdirectory, not every matrix output:

```bash
diag_symbol_artifact='EXACT_ARTIFACT_NAME_FROM_THE_LIST'
gh run download "$diag_symbols_run" --repo "$diag_repo" \
  --name "$diag_symbol_artifact" --dir "$diag_case/symbols"
```

The artifact root contains `collection-result.json`, `release-inputs.json` when
processing completed, and per-executable directories with `manifest.json`, exact
original executables and symbols. macOS can also include dsymutil provenance.
Before using a directory, check:

1. `collection-result.json` has `status=completed`, the expected source release
   run ID and commit. Failed/in-progress uploads can exist and contain partial data.
2. `release-inputs.json` has the same producer `run_id`, `commit`, `target` and
   expected binary inventory. This establishes the producer association that
   filtering symbol runs by SHA alone does not establish.
3. The selected executable's manifest matches the captured module ID/format and
   build provenance, `source_run_id` and `target`; `symbols_validated` and
   `packaged_module_validated` are true. Do not confuse this with bundle manifests.
4. The executable checksum is the original hash or one of the manifest's retained
   packaged hashes. Verify the actual local/captured executable, not an arbitrary
   other executable shipped in the same desktop artifact.
5. Inventory files, byte sizes and checksums validate; retain the directory layout.

Desktop packaging reuses the `computer-use` Gateway from the same Release run.
Older releases can reference a separate Release Gateway run from the same commit.
In both cases, `release-inputs.json.reused_gateway` and
`gateway-release-inputs.json` identify its original producer. For its executable
manifest, `source_run_id` must match that Gateway run; `packaged_by_run_id` must
match the packaging run (both IDs are the same for the unified workflow).
Use the Desktop artifact's packaged hashes because
Desktop packaging may strip or sign the reused executable. The Desktop and
Gateway inventories must agree on commit, target, catalog hash and module identity.

Do not load an entire multi-GiB DWARF/PDB into a JSON/text tool or model context.
Use streaming checksums and bounded native lookups. Copying just a dSYM without
its sibling manifest/executable loses the automatic validation path.

## Validate using the installed diagnostics CLI

Use the per-executable `manifest.json`, not `collection-result.json` or a capture
bundle manifest. Supply absolute paths and quote paths with spaces:

```bash
"$diag_cli" diagnostics validate-symbols "$diag_exact_executable" \
  "$diag_symbol_manifest"
"$diag_cli" diagnostics validate-capture-symbols "$diag_capture_manifest" \
  "$diag_symbol_manifest"
```

The first reads the executable's native identity/hash; the second reads the native
capture manifest's recorded module. It expects a CaptureManifest, not the bundle
receipt. Both validate the retained symbol inventory and allowed binary hashes.
A null capture module cannot be attested by pretending the CLI found it elsewhere.
Use successful existing identity evidence if appropriate, and label the limitation.

Validation can stream several GiB and is not free. Run it once for the exact
artifact pair, save its result and file hashes, and reuse that evidence while the
files remain unchanged. Its inventory is bounded (4 GiB, 4,096 files and a checksum
budget); an explicit overflow/timeout does not permit skipping checks or raising
limits as part of ordinary diagnosis.

## Resolve native addresses

On macOS, confirm UUID and architecture with `xcrun dwarfdump --uuid` on the exact
executable and dSYM. Use the recorded process image load address from the **same
capture/run** and a runtime program counter belonging to that module:

```bash
diag_dwarf='/absolute/path/to/executable.dSYM/Contents/Resources/DWARF/executable'
diag_atos_arch='arm64' # x86_64 for an x86_64 image; derive it from the actual module.
xcrun atos -arch "$diag_atos_arch" -o "$diag_dwarf" \
  -l "$diag_image_load_address" "$diag_runtime_pc"
```

`-l` takes the actual runtime image load address, not a guessed ASLR slide, file
offset or main-binary base for a library PC. Obtain image mappings from the native
capture or a permitted read-only process mapping inspection. If the mapping is
missing, say lookup is incomplete. An address resolving to a plausible function
without matching module/base evidence is not proof. Confirm a few captured PCs
resolve into the expected functions and `.rs` file/line information; retain the
commands/results with module, architecture and base evidence.

For Linux, read build IDs using `readelf -n` and the capture's `perf buildid-list`.
Provide the exact executable and separate `.debug` file to perf's documented
symbol lookup configuration. Consult that installed perf version for cache/search
paths; do not overwrite another user's symbol cache. For an offline normalized
module address, `addr2line -e "$diag_debug_file" -f -C ADDRESS` can resolve source;
it does not accept an arbitrary PIE runtime PC without load-address normalization.

Windows PDB identity is GUID+age, including the format's byte representation.
Prefer the built-in validator over string-converting a displayed UUID by hand.
PDB availability does not add a Windows native capture adapter. Use matching PDBs
with the offline native evidence/readers available for the task, without pausing
the live host in a debugger.

Main executable symbols do not automatically cover SQLite/framework/system
libraries or plugins in a stack. Determine the owning module before resolving an
address. Use source matching recorded revision/dirty provenance; report source
mapping gaps rather than claiming the latest checkout supplies exact source lines.

## Missing or failed symbol production

Outputs retain privately for 90 days; raw release `symbol-inputs-*` and the macOS
tool artifact retain for 7 days. Artifacts can be manually removed or expire.
Check the actual repository/run rather than promising permanent availability.

Do not rebuild Gateway/Desktop or compile LLVM locally just because symbols are
missing. First inspect the exact producer and symbol run logs, artifact expiry,
`collection-result.json` and native verifier output. If another symbol generation
is actually needed and authorized, dispatch **Build Debug Symbols** with the exact
producer's `source_run_id`. Use a workflow ref containing the needed fix; the
workflow validates the source producer and checks out its exact commit to consume
the original inputs. Do not rerun the release or recreate its tag. This is an
external CI action, not an automatic side effect of reading a profile; obtain
authorization unless the user already requested it. It does not restart the app.

The macOS runner builds a standalone `dsymutil` from pinned LLVM revision
`5e8490a1cf89bd94194246d0569c232a923d0161` with the committed one-line
`scripts/packaging/dsymutil-undefined-symbols.patch`. It excludes undefined symbols
from an address map, preserving valid debug ranges for these builds. The runner
records tool/patch hashes and checks output DWARF and UUID. This is not a runtime
library dependency, a system Xcode replacement, or a file users build at startup.
The release job still compiles debug information and retains/uploads exact inputs;
heavy linking/tool compilation runs in the independent symbol workflow. Do not
promise zero release cost.

If original inputs have expired, explicitly report the limit. Never relabel a
fresh build's dSYM as the old release's symbols, silently accept verifier errors,
or start an open-ended series of rebuilds. A symbol-production repair is a
separate scoped task; preserve useful resource/work evidence from this incident.
