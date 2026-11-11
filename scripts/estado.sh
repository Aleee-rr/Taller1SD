#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source config/peers.env
case "${1:-}" in
  node1) IP="$NODE1_IP" ;;
  node2) IP="$NODE2_IP" ;;
  node3) IP="$NODE3_IP" ;;
  *) echo 'Uso: bash scripts/estado.sh node1|node2|node3' >&2; exit 1 ;;
esac
[[ -x .venv/bin/python ]] || { echo 'Instala las dependencias primero.' >&2; exit 1; }
exec .venv/bin/python app/nodo.py status --target "${IP}:${APP_PORT}"
