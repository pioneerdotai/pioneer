#!/usr/bin/env bash
set -euo pipefail

# Only the independent symbols workflow builds this tool.
commit=5e8490a1cf89bd94194246d0569c232a923d0161
destination="$1"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
patch_file="$script_dir/dsymutil-undefined-symbols.patch"
if [[ -x "$destination/dsymutil" && -f "$destination/tool.json" ]]; then
  python3 - "$destination" "$commit" "$patch_file" <<'PY'
import hashlib, json, pathlib, sys
out, commit, patch = pathlib.Path(sys.argv[1]), sys.argv[2], pathlib.Path(sys.argv[3])
value = json.loads((out / "tool.json").read_text())
assert value["llvm_commit"] == commit
assert value["patch_sha256"] == hashlib.sha256(patch.read_bytes()).hexdigest()
assert value["binary_sha256"] == hashlib.sha256((out / "dsymutil").read_bytes()).hexdigest()
PY
  exit 0
fi
scratch="$(mktemp -d "${RUNNER_TEMP:-/tmp}/pioneer-dsymutil.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
curl --fail --location --retry 3 \
  "https://codeload.github.com/llvm/llvm-project/tar.gz/$commit" -o "$scratch/source.tar.gz"
tar -xzf "$scratch/source.tar.gz" -C "$scratch"
source_dir="$scratch/llvm-project-$commit"
cpp="$source_dir/llvm/tools/dsymutil/MachODebugMapParser.cpp"
[[ "$(shasum -a 256 "$cpp" | awk '{print $1}')" == \
  6e0965b39d12da01237a00d79a70497e05f14fac1f7cedf8abb955a34a8e5b31 ]]
patch -d "$source_dir" -p1 < "$patch_file"
[[ "$(shasum -a 256 "$cpp" | awk '{print $1}')" == \
  5b3f90111e1f02503b1e06f0fa4451920b2f9333f583a3fe7474fb202471b902 ]]
cmake -S "$source_dir/llvm" -B "$scratch/build" \
  -DCMAKE_BUILD_TYPE=Release '-DLLVM_TARGETS_TO_BUILD=AArch64;X86' \
  -DLLVM_INCLUDE_TESTS=OFF -DLLVM_INCLUDE_EXAMPLES=OFF -DLLVM_INCLUDE_BENCHMARKS=OFF
cmake --build "$scratch/build" --target dsymutil --parallel 2
mkdir -p "$destination"
cp "$scratch/build/bin/dsymutil" "$destination/dsymutil"
python3 - "$destination" "$commit" "$patch_file" <<'PY'
import hashlib, json, pathlib, sys
out, commit, patch = pathlib.Path(sys.argv[1]), sys.argv[2], pathlib.Path(sys.argv[3])
(out / "tool.json").write_text(json.dumps({
    "llvm_commit": commit,
    "patch_sha256": hashlib.sha256(patch.read_bytes()).hexdigest(),
    "binary_sha256": hashlib.sha256((out / "dsymutil").read_bytes()).hexdigest(),
}, indent=2) + "\n")
PY
