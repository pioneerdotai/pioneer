# Base index digests resolved from the official registries on 2026-11-13.
# Keep the readable tags; the digests, not moving tags, select the contents.
ARG NODE_IMAGE=gcr.io/distroless/nodejs26-debian13:nonroot@sha256:1d736cf8b5716c07a245301f92717143215d248fd55771ee1936ef4b3302e531
ARG PYTHON_IMAGE=docker.io/library/python:3.15.2-slim-trixie@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928
ARG NATIVE_IMAGE=gcr.io/distroless/cc-debian13:nonroot@sha256:cb87acc574abe719b6e5c547384defa2f7b1f24af9605d3f618cf852495d8be2

FROM ${NODE_IMAGE} AS node
ENV PATH="/nodejs/bin:/usr/local/bin:/usr/bin:/bin" HOME=/tmp NODE_ENV=production
USER 65532:65532
WORKDIR /app
# Gateway supplies the complete argv from app.json; do not prepend node twice.
ENTRYPOINT []
CMD []

FROM ${PYTHON_IMAGE} AS python
ENV HOME=/tmp PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 65532:65532
WORKDIR /app
ENTRYPOINT []
CMD []

FROM ${NATIVE_IMAGE} AS native
ENV HOME=/tmp
USER 65532:65532
WORKDIR /app
ENTRYPOINT []
CMD []

# This build-host stage only downloads/unpacks upstream release files. Python
# and its tools do not become part of the wasm runtime. No target code is run.
FROM --platform=$BUILDPLATFORM ${PYTHON_IMAGE} AS wasmtime-download
ARG TARGETARCH
RUN python3 - <<'PY'
import hashlib
import io
from pathlib import Path
import tarfile
import urllib.request
import os

version = "50.0.1"
architecture, checksum = {
    "amd64": ("x86_64", "685466455dd42a14774a86c36c8f2e45cbb9bd6d92a850139315df60a75498a5"),
    "arm64": ("aarch64", "d737e077416d78fbfb0a008229ec27d6a0fd5a4924874b8c38b293c850cf4094"),
}[os.environ["TARGETARCH"]]
name = f"wasmtime-v{version}-{architecture}-linux"
url = f"https://github.com/bytecodealliance/wasmtime/releases/download/v{version}/{name}.tar.xz"
with urllib.request.urlopen(url, timeout=120) as response:
    archive = response.read()
if hashlib.sha256(archive).hexdigest() != checksum:
    raise SystemExit("Wasmtime archive checksum mismatch")
Path("/out").mkdir()
with tarfile.open(fileobj=io.BytesIO(archive), mode="r:xz") as bundle:
    for filename in ("wasmtime", "LICENSE"):
        member = bundle.getmember(f"{name}/{filename}")
        if not member.isfile():
            raise SystemExit("Expected a regular Wasmtime release file")
        with bundle.extractfile(member) as source:
            Path(f"/out/{filename}").write_bytes(source.read())
Path("/out/wasmtime").chmod(0o755)
PY

FROM ${NATIVE_IMAGE} AS wasm
COPY --from=wasmtime-download /out/wasmtime /usr/local/bin/wasmtime
COPY --from=wasmtime-download /out/LICENSE /usr/share/licenses/wasmtime/LICENSE
ENV PATH="/usr/local/bin:/usr/bin:/bin" HOME=/tmp
USER 65532:65532
WORKDIR /app
ENTRYPOINT []
CMD []
