#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source config/peers.env
case "$(hostname -s)" in
  sdm-node1) ID=1 ;;
  sdm-node2) ID=2 ;;
  sdm-node3) ID=3 ;;
  *) echo "ERROR: hostname no reconocido: $(hostname -s)" >&2; exit 1 ;;
esac
[[ -x .venv/bin/python ]] || { echo 'Ejecuta primero: bash scripts/instalar_dependencias.sh' >&2; exit 1; }
export MONGO_URI
PEERS="1=${NODE1_IP}:${APP_PORT},2=${NODE2_IP}:${APP_PORT},3=${NODE3_IP}:${APP_PORT}"
printf 'Iniciando Bully en nodo %s (puerto %s)\n' "$ID" "$APP_PORT"
exec .venv/bin/python -u app/nodo.py run --id "$ID" --peers "$PEERS"
