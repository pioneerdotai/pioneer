#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUTPUT_DIR="${1:-$ROOT_DIR/schemas/client}"

cargo run -p pioneer-client --features schema --bin schema -- "$OUTPUT_DIR"
