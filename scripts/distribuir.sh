#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck disable=SC1091
source config/peers.env
[[ "$(hostname -s)" == "sdm-node1" ]] || { echo 'Distribuir solo desde sdm-node1.' >&2; exit 1; }
case "${1:-}" in
  node2) IP="$NODE2_IP" ;;
  node3) IP="$NODE3_IP" ;;
  *) echo 'Uso: bash scripts/distribuir.sh node2|node3' >&2; exit 1 ;;
esac
KEY="$HOME/.ssh/id_ed25519_node1"
[[ -f "$KEY" ]] || { echo "No existe la clave privada de Node1: $KEY" >&2; exit 1; }
[[ -z "$(git status --porcelain)" ]] || {
  echo 'Hay cambios sin commit. Confirma primero la version a distribuir.' >&2
  git status --short
  exit 1
}
if git ls-tree -r --name-only HEAD | grep -Eq '(^|/)(\.ssh/|\.venv/|id_ed25519[^/]*$|id_rsa[^/]*$|\.git-respaldo-accidental)'; then
  echo 'ERROR: el commit incluye posibles claves o directorios privados; no distribuyo.' >&2
  exit 1
fi
printf 'Transfiriendo commit %s hacia %s (%s)...\n' "$(git rev-parse --short HEAD)" "${1}" "$IP"
git archive --format=tar HEAD | ssh -i "$KEY" -o IdentitiesOnly=yes -o ConnectTimeout=12 "student@$IP" '
  set -eu
  dest="$HOME/Taller1SD"
  if [ -L "$dest" ]; then
    echo "ERROR: el destino es un enlace simbolico. No se modifica." >&2
    exit 2
  fi
  if [ -e "$dest" ] && [ ! -f "$dest/.managed-by-node1" ]; then
    echo "ERROR: ~/Taller1SD ya existe y no fue desplegada por este script." >&2
    echo "Revisa su contenido o renombrala a ~/Taller1SD_anterior antes de continuar." >&2
    exit 2
  fi
  mkdir -p "$dest"
  tar -xf - -C "$dest"
  touch "$dest/.managed-by-node1"
  echo "Proyecto recibido en $dest"
'
printf 'Distribucion completada. En el destino: cd ~/Taller1SD && bash scripts/instalar_dependencias.sh\n'
