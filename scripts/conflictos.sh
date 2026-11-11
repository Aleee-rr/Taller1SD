#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source config/peers.env
export MONGO_URI
exec .venv/bin/python -m scripts.conflictos "$@"
