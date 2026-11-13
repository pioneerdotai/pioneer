# App runtime images

Build recipes for Pioneer's four managed App environments. This workflow builds
runtime images independently of Gateway and application builds. The public
runtime names in `app.json` remain
`node`, `python`, `native`, `wasm`.

| Runtime / Dockerfile target | Base |
| --- | --- |
| `node` | Distroless Node 26, Debian 13, glibc |
| `python` | Official Python 3.15.2 slim-trixie, glibc |
| `native` | Distroless CC Debian 13: glibc and basic C/C++ runtime libraries |
| `wasm` | Distroless CC Debian 13 + prebuilt Wasmtime 50.0.1 |

Exact upstream index digests and Wasmtime SHA-256 checksums are pinned in the
[Dockerfile](Dockerfile). They were retrieved from the official registries and
GitHub Release API on November 13, 2026. Updates require an explicit recipe edit.
The App format does not require every runtime to use the same distribution or
Debian 13 base.

Each OCI index includes both `linux/amd64` and `linux/arm64`. These are
Microsandbox guest platforms, independent of the Gateway host OS. Wasmtime is
downloaded as a prebuilt binary for the target architecture. This CI does not
compile Rust, Node, Python, SDKs, or applications.

## Workflow

[App runtime images](../../.github/workflows/app-runtimes.yml) runs only through
manual dispatch (`workflow_dispatch`) on `main`. It has no push, pull request,
or scheduled triggers.

- Every manual run builds and publishes four candidate images:
  `ghcr.io/<repository-owner>/runtime-node`, `runtime-python`,
  `runtime-native`, `runtime-wasm`. The current organization owner is `pioneerdotai`.
- Tags include the full Git SHA, run ID, and attempt number. The workflow does
  not publish `latest` or floating `node`/`26` tags. Runtime selection uses an
  image reference with a digest.
- Once all four builds succeed, the `runtime-catalog-candidate` artifact contains
  a combined `runtime-catalog.candidate.json` with the source revision, both
  platforms, exact digests, and published references. Only small JSON metadata
  files are stored in Actions artifacts, with a seven-day retention period.
  Image contents are stored in GHCR without a duplicate OCI archive in artifacts.

Publication uses the built-in `GITHUB_TOKEN` with `packages:write`; a separate
PAT is not required. The organization must allow package creation. If a package
already exists, this repository must have Actions access to it.
**Initial publication does not automatically make a package public:** select
Public in the settings of each of the four GHCR packages after creation.
Gateway can then download the images without a user's GitHub token.

Under GitHub's current billing policy, Container registry image storage and
bandwidth are free. Standard GitHub-hosted runners are also free for public
repositories; this workflow uses standard Ubuntu runners. Actions artifact
storage has separate plan allowances, so the workflow retains only small JSON
metadata files. GHCR limits individual layers to 10 GB and uploads to 10 minutes.
These terms were checked on November 13, 2026; GitHub states that any change to
the free Container registry policy will be announced at least one month ahead.

These images are candidates. The workflow does not add them to Gateway's catalog,
change installed Apps, or establish production readiness. Before integration,
separately verify Microsandbox startup on both architectures, networking and
ports, writable data, and application compatibility. Do not run this workflow
while the current restriction on builds and tests remains in effect.

## Launch contract

- Final images use UID/GID `65532:65532`, `HOME=/tmp`, and working directory
  `/app`. Gateway supplies the actual cwd, mounts, and complete argv from
  `app.json`.
- Upstream Entrypoint and CMD are cleared so that `node start.cjs` does not
  become `node node start.cjs`. PATH includes `/nodejs/bin` for Node. Python
  and Wasmtime are available through `/usr/local/bin`.
- Example argv: `node start.cjs`, `python3 main.py`, `./server`,
  `wasmtime run main.wasm`. Application developers prepare dependencies before
  installation and build native binaries/extensions for the selected base ABI.
- Gateway must provide writable `/tmp` and separate data storage accessible to
  the runtime user. Images contain no App data.
- The `native` runtime does not include arbitrary system libraries. Authors
  must bundle additional application dependencies compatible with the base.
- Gateway serves the Terminal's host shell through an authorized API. These
  images do not provide the Gateway host's shell. The Node/native/wasm Distroless
  images contain no shell or package manager; these are not separately removed
  from Python slim.

## Sources

- [Distroless](https://github.com/GoogleContainerTools/distroless)
- [Python official image](https://hub.docker.com/_/python)
- [Wasmtime 50.0.1](https://github.com/bytecodealliance/wasmtime/releases/tag/v50.0.1)
- [GHCR authentication and visibility](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
- [Docker build/push action](https://github.com/docker/build-push-action)
- [GitHub Packages billing](https://docs.github.com/en/billing/concepts/product-billing/github-packages)
- [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions)
